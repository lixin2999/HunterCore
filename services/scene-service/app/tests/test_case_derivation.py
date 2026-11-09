"""用例 / 测试集自动派生端点测试（计划 §5 / §8）。

覆盖：语义驱动用例内容（objective/preconditions/steps/platforms）、``case_key`` 幂等、
分区过滤、``dry_run`` 预览、未知类型拒绝、测试集按类型建集并关联用例与具体场景、
draft 幂等补齐、锁定后跳过。
"""
from __future__ import annotations

from typing import Any

from httpx import AsyncClient

from app.scene_engine.catalog import CATALOG_BY_ID
from app.scene_engine.semantics import semantics_for
from app.tests.conftest import ADMIN_HEADERS, OPERATOR_HEADERS, REVIEWER_HEADERS

SCENE_BASE = "/api/v1/scene"
RESPONSE_FIELDS = {"code", "message", "data", "request_id", "timestamp"}


def _data(response: Any, code: int = 0) -> dict[str, Any]:
    body = response.json()
    assert set(body) == RESPONSE_FIELDS
    assert body["code"] == code
    return body["data"]


async def _derive_cases(
    client: AsyncClient,
    *,
    scene_types: list[str],
    zones: list[str] | None = None,
    dry_run: bool = False,
) -> Any:
    payload: dict[str, Any] = {"scene_types": scene_types, "dry_run": dry_run}
    if zones is not None:
        payload["zones"] = zones
    return await client.post(
        f"{SCENE_BASE}/testcases/derive", headers=OPERATOR_HEADERS, json=payload
    )


async def _derive_suites(
    client: AsyncClient,
    *,
    scene_types: list[str],
    include_concrete: bool = True,
) -> Any:
    return await client.post(
        f"{SCENE_BASE}/suites/derive",
        headers=OPERATOR_HEADERS,
        json={"scene_types": scene_types, "include_concrete": include_concrete},
    )


async def _generate(client: AsyncClient, *, logical_id: str = "LS-BAS-001", count: int = 6) -> dict:
    """提交一次生成任务并等待 BackgroundTasks 结束。"""
    resp = await client.post(
        f"{SCENE_BASE}/generate",
        headers=OPERATOR_HEADERS,
        json={
            "logical_id": logical_id,
            "count": count,
            "strategy": "distribution",
            "profile": "exploration",
            "seed": 20261109,
        },
    )
    assert resp.status_code == 202
    task_id = _data(resp)["task_id"]
    return _data(
        await client.get(f"{SCENE_BASE}/generate/{task_id}", headers=ADMIN_HEADERS)
    )


# =====================================================================
# 一、用例派生
# =====================================================================
async def test_derive_testcases_from_semantics(client: AsyncClient) -> None:
    """按语义契约派生：编号/目的/判据/前置/步骤/平台全部来自事实来源。"""
    resp = await _derive_cases(
        client, scene_types=["occluded_pedestrian"], zones=["Z1", "Z2"]
    )
    assert resp.status_code == 200
    data = _data(resp)
    assert data["created"] == 2 and data["skipped"] == 0 and data["planned"] == 0
    assert [item["case_key"] for item in data["cases"]] == [
        "TC-LS-INT-003-Z1",
        "TC-LS-INT-003-Z2",
    ]
    assert {item["status"] for item in data["cases"]} == {"created"}

    ir = CATALOG_BY_ID["LS-INT-003"]
    semantics = semantics_for("occluded_pedestrian")
    detail = _data(
        await client.get(
            f"{SCENE_BASE}/testcases/{data['cases'][0]['test_case_id']}", headers=ADMIN_HEADERS
        )
    )
    assert detail["logical_id"] == "LS-INT-003"
    assert detail["logical_version"] == "1.1.0"
    assert detail["objective"] == semantics.objective
    assert detail["oracle_ref"] == ir.oracle_ref
    assert detail["status"] == "draft"
    assert detail["preconditions"]["zone"] == "Z1"
    assert detail["preconditions"]["scene_type"] == "occluded_pedestrian"
    assert detail["preconditions"]["parameters"]["d_occ"] == [1.0, 6.0]
    assert [step["action"] for step in detail["steps"]] == [
        "generate",
        "render",
        "dispatch",
        "assert",
        "archive",
    ]
    assert detail["platforms"] == ["sil", "hil"]
    assert detail["timeout_seconds"] >= 60
    assert detail["tags"] == ["derived", "occluded_pedestrian", "Z1"]


async def test_derive_testcases_is_idempotent(client: AsyncClient) -> None:
    """重复派生同一 case_key → 跳过并回报原因，不新建用例。"""
    await _derive_cases(client, scene_types=["child_chasing_ball"], zones=["Z3"])
    again = _data(
        await _derive_cases(client, scene_types=["child_chasing_ball"], zones=["Z3"])
    )
    assert again["created"] == 0 and again["skipped"] == 1
    assert again["cases"][0]["case_key"] == "TC-LS-CC-003-Z3"
    assert again["cases"][0]["status"] == "skipped"
    assert again["cases"][0]["reason"] == "case_key 已存在"

    listed = _data(
        await client.get(
            f"{SCENE_BASE}/testcases",
            headers=ADMIN_HEADERS,
            params={"logical_id": "LS-CC-003"},
        )
    )
    assert listed["total"] == 1


async def test_derive_testcases_dry_run_writes_nothing(client: AsyncClient) -> None:
    """dry_run 只返回 planned 预览。"""
    data = _data(
        await _derive_cases(
            client, scene_types=["minimal_risk_manoeuvre"], zones=["Z4"], dry_run=True
        )
    )
    assert data["created"] == 0 and data["planned"] == 1 and data["skipped"] == 0
    assert data["cases"][0]["status"] == "planned"
    assert data["cases"][0]["test_case_id"] is None

    listed = _data(
        await client.get(
            f"{SCENE_BASE}/testcases",
            headers=ADMIN_HEADERS,
            params={"logical_id": "LS-HSE-001"},
        )
    )
    assert listed["total"] == 0


async def test_derive_testcases_rejects_unknown_scene_type(client: AsyncClient) -> None:
    """目录外的场景类型 → 422 + 2001。"""
    resp = await _derive_cases(client, scene_types=["not_a_scene_type"])
    assert resp.status_code == 422
    _data(resp, code=2001)
    assert "not_a_scene_type" in resp.json()["message"]


# =====================================================================
# 二、测试集派生
# =====================================================================
async def test_derive_suites_links_cases_and_concretes(client: AsyncClient) -> None:
    """一类型一集：条目 = 4 条分区用例 + 每分区具体场景；draft 再次派生为 updated。"""
    task = await _generate(client)
    assert task["produced"] > 0

    await _derive_cases(client, scene_types=["straight_cruise"])
    first = _data(await _derive_suites(client, scene_types=["straight_cruise"]))
    assert first["created"] == 1 and first["skipped"] == 0
    suite = first["suites"][0]
    assert suite["name"] == "自动派生-straight_cruise"
    assert suite["status"] == "created"

    detail = _data(
        await client.get(f"{SCENE_BASE}/suites/{suite['suite_id']}", headers=ADMIN_HEADERS)
    )
    case_items = [item for item in detail["items"] if item["item_type"] == "test_case"]
    concrete_items = [
        item for item in detail["items"] if item["item_type"] == "concrete_scenario"
    ]
    assert len(case_items) == 4
    assert {item["ref_id"] for item in case_items} == {
        "TC-LS-BAS-001-Z1",
        "TC-LS-BAS-001-Z2",
        "TC-LS-BAS-001-Z3",
        "TC-LS-BAS-001-Z4",
    }
    assert concrete_items, "应纳入已生成的具体场景"
    assert detail["item_count"] == len(detail["items"])

    again = _data(await _derive_suites(client, scene_types=["straight_cruise"]))
    assert again["created"] == 0 and again["updated"] == 1 and again["skipped"] == 0
    assert again["suites"][0]["status"] == "updated"


async def test_derive_suites_skips_locked_suite(client: AsyncClient) -> None:
    """已锁定测试集条目冻结 → 跳过并回报原因。"""
    await _derive_cases(client, scene_types=["narrow_road_meeting"])
    created = _data(
        await _derive_suites(client, scene_types=["narrow_road_meeting"], include_concrete=False)
    )
    assert created["created"] == 1
    suite_id = created["suites"][0]["suite_id"]
    assert created["suites"][0]["item_count"] == 4

    locked = await client.post(
        f"{SCENE_BASE}/suites/{suite_id}/lock",
        headers=REVIEWER_HEADERS,
        json={"baseline_name": "derived-baseline"},
    )
    assert locked.status_code == 200
    assert _data(locked)["status"] == "locked"

    again = _data(
        await _derive_suites(client, scene_types=["narrow_road_meeting"], include_concrete=False)
    )
    assert again["created"] == 0 and again["updated"] == 0 and again["skipped"] == 1
    assert again["suites"][0]["status"] == "skipped"
    assert "locked" in again["suites"][0]["reason"]


async def test_derive_suites_requires_cases_first(client: AsyncClient) -> None:
    """未派生用例且无具体场景时跳过（不建空测试集）。"""
    data = _data(await _derive_suites(client, scene_types=["snowy"]))
    assert data["created"] == 0 and data["skipped"] == 1
    assert data["suites"][0]["status"] == "skipped"
    assert "无语例" in data["suites"][0]["reason"]

    listed = _data(await client.get(f"{SCENE_BASE}/suites", headers=ADMIN_HEADERS))
    assert listed["total"] == 0
