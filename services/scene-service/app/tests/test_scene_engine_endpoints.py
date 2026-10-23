"""场景引擎端点测试（设计文档 §4.1：路径 ↔ 实现 ↔ 错误码）。

覆盖：逻辑场景（列表/详情/版本/校验/干跑/同步/发布）、生成任务（202 → 执行 →
具体场景/渲染/批量复检/取消）、判据与测试用例、测试集（加条目/锁定/基线/差异）、
覆盖总览与盲区、RBAC 与统一响应格式。
"""
from __future__ import annotations

from typing import Any
from uuid import uuid4

from httpx import AsyncClient

from app.scene_engine.catalog import CATALOG, CATALOG_VERSION
from app.tests.conftest import ADMIN_HEADERS, OPERATOR_HEADERS, REVIEWER_HEADERS, VIEWER_HEADERS

RESPONSE_FIELDS = {"code", "message", "data", "request_id", "timestamp"}
SCENE_BASE = "/api/v1/scene"


def _assert_envelope(body: dict[str, Any], code: int = 0) -> dict[str, Any]:
    """统一响应格式校验（五字段 + 错误码）。"""
    assert set(body) == RESPONSE_FIELDS
    assert body["code"] == code
    assert body["request_id"]
    return body


def _data(response: Any, code: int = 0) -> dict[str, Any]:
    """断言统一响应体并取出 data。"""
    return _assert_envelope(response.json(), code=code)["data"]


def _testcase_payload(case_key: str = "TC-SAFE-001") -> dict[str, Any]:
    return {
        "case_key": case_key,
        "name": "前车切入-最小安全距离",
        "logical_id": "LS-INT-001",
        "objective": "验证切入工况下纵向最小安全距离不被突破",
        "preconditions": {"map_id": "Town01", "weather": "clear"},
        "steps": [{"index": 1, "action": "spawn", "note": "初始化场景"}],
        "platforms": ["simulation", "replay"],
        "oracle_ref": "oracle.safety.min_distance/v1",
        "timeout_seconds": 180,
        "tags": ["smoke", "cut-in"],
    }


async def _generate(client: AsyncClient, *, count: int = 6, seed: int = 20261023) -> dict[str, Any]:
    """提交一次生成任务并返回已结束的任务详情（BackgroundTasks 在响应前完成）。"""
    resp = await client.post(
        f"{SCENE_BASE}/generate",
        headers=OPERATOR_HEADERS,
        json={
            "logical_id": "LS-BAS-001",
            "count": count,
            "strategy": "distribution",
            "profile": "regression",
            "seed": seed,
        },
    )
    assert resp.status_code == 202
    accepted = _data(resp)
    detail = await client.get(f"{SCENE_BASE}/generate/{accepted['task_id']}", headers=ADMIN_HEADERS)
    return _data(detail)


# =====================================================================
# 一、逻辑场景
# =====================================================================
async def test_list_logical_returns_catalog(client: AsyncClient, scene_env: object) -> None:
    """未同步 DB 时目录兜底：17 个内置逻辑场景全部可见。"""
    resp = await client.get(f"{SCENE_BASE}/logical", headers=ADMIN_HEADERS)
    assert resp.status_code == 200
    data = _data(resp)
    assert data["total"] == len(CATALOG) == 17
    assert {item["logical_id"] for item in data["items"]} == {ir.logical_id for ir in CATALOG}
    assert all(item["origin"] == "catalog" for item in data["items"])
    assert all(item["version"] == CATALOG_VERSION for item in data["items"])


async def test_list_logical_filters_category_and_keyword(client: AsyncClient) -> None:
    """分类与关键字过滤（关键字命中 ID 或名称）。"""
    by_category = await client.get(
        f"{SCENE_BASE}/logical", headers=ADMIN_HEADERS, params={"category": "interactive"}
    )
    items = _data(by_category)["items"]
    assert items and all(item["category"] == "interactive" for item in items)

    by_keyword = await client.get(
        f"{SCENE_BASE}/logical", headers=ADMIN_HEADERS, params={"keyword": "INT-001"}
    )
    hits = _data(by_keyword)["items"]
    assert [item["logical_id"] for item in hits] == ["LS-INT-001"]


async def test_get_logical_detail_contains_ir(client: AsyncClient) -> None:
    """详情返回 IR 本体与参数/绑定/约束声明。"""
    resp = await client.get(f"{SCENE_BASE}/logical/LS-INT-001", headers=ADMIN_HEADERS)
    assert resp.status_code == 200
    data = _data(resp)
    assert data["logical_id"] == "LS-INT-001"
    assert data["ir_version"] == "hunter.ir/1.2"
    assert data["oracle_ref"] == "oracle.safety.min_distance/v1"
    assert data["ir"]["logical_id"] == "LS-INT-001"
    assert len(data["parameters"]) == len(data["ir"]["parameters"]) == 3
    assert data["bindings"] and data["constraints"] and data["correlations"]


async def test_get_logical_unknown_returns_404(client: AsyncClient) -> None:
    """未知逻辑场景 → 404 + code=3001。"""
    resp = await client.get(f"{SCENE_BASE}/logical/LS-ZZZ-999", headers=ADMIN_HEADERS)
    assert resp.status_code == 404
    _assert_envelope(resp.json(), code=3001)


async def test_list_logical_versions_includes_catalog(client: AsyncClient) -> None:
    """版本列表在未同步时回落到目录当前版本。"""
    resp = await client.get(f"{SCENE_BASE}/logical/LS-BAS-001/versions", headers=ADMIN_HEADERS)
    assert resp.status_code == 200
    data = _data(resp)
    assert data["logical_id"] == "LS-BAS-001"
    assert [item["version"] for item in data["items"]] == [CATALOG_VERSION]
    assert data["items"][0]["origin"] == "catalog"
    assert len(data["items"][0]["content_hash"]) == 64


async def test_validate_logical_runs_seven_gates(client: AsyncClient) -> None:
    """静态校验返回 G1-G7 明细与判据存在性。"""
    resp = await client.post(f"{SCENE_BASE}/logical/LS-INT-001/validate", headers=ADMIN_HEADERS)
    assert resp.status_code == 200
    data = _data(resp)
    assert data["passed"] is True, data["failures"]
    assert [gate["gate"] for gate in data["gates"]] == ["G1", "G2", "G3", "G4", "G5", "G6", "G7"]
    assert all(gate["passed"] for gate in data["gates"])
    assert data["oracle_found"] is True
    assert data["parameter_defaults"]["v0"] > 0


async def test_dryrun_applies_overrides_and_renders_config(client: AsyncClient) -> None:
    """干跑：显式参数覆盖缺省值，并返回可直接下发的 SceneConfig。"""
    resp = await client.post(
        f"{SCENE_BASE}/logical/LS-INT-001/dryrun",
        headers=ADMIN_HEADERS,
        json={"values": {"v0": 0.4}},
    )
    assert resp.status_code == 200
    data = _data(resp)
    assert data["values"]["v0"] == 0.4
    assert data["passed"] is True, data["failures"]
    assert {"map", "ego_vehicle", "actors"} <= set(data["scene_config"])
    assert len(data["gates"]) == 7


async def test_dryrun_rejects_unknown_parameter(client: AsyncClient) -> None:
    """未声明的参数名 → 422 + code=2001。"""
    resp = await client.post(
        f"{SCENE_BASE}/logical/LS-INT-001/dryrun",
        headers=ADMIN_HEADERS,
        json={"values": {"typo_speed": 1.0}},
    )
    assert resp.status_code == 422
    _assert_envelope(resp.json(), code=2001)


async def test_sync_catalog_persists_and_db_takes_priority(
    client: AsyncClient, scene_env: object
) -> None:
    """目录同步幂等落库；同步后列表来源切换为 database。"""
    first = await client.post(f"{SCENE_BASE}/logical/sync", headers=ADMIN_HEADERS)
    assert first.status_code == 200
    synced = _data(first)
    assert synced["synced"] == len(CATALOG)
    assert synced["version"] == CATALOG_VERSION
    assert len(scene_env.logical_repository.rows) == len(CATALOG)  # type: ignore[attr-defined]

    await client.post(f"{SCENE_BASE}/logical/sync", headers=ADMIN_HEADERS)
    assert len(scene_env.logical_repository.rows) == len(CATALOG)  # type: ignore[attr-defined]

    listed = _data(await client.get(f"{SCENE_BASE}/logical", headers=ADMIN_HEADERS))
    assert listed["total"] == len(CATALOG)
    assert all(item["origin"] == "database" for item in listed["items"])

    versions = _data(
        await client.get(f"{SCENE_BASE}/logical/LS-BAS-001/versions", headers=ADMIN_HEADERS)
    )
    assert [item["origin"] for item in versions["items"]] == ["database"]


async def test_publish_upserts_logical_scenario(client: AsyncClient, scene_env: object) -> None:
    """发布目录项：先落库再置 published。"""
    resp = await client.post(f"{SCENE_BASE}/logical/LS-BAS-002/publish", headers=ADMIN_HEADERS)
    assert resp.status_code == 200
    data = _data(resp)
    assert data == {
        "logical_id": "LS-BAS-002",
        "version": CATALOG_VERSION,
        "status": "published",
    }
    row = scene_env.logical_repository.rows[("LS-BAS-002", CATALOG_VERSION)]  # type: ignore[attr-defined]
    assert row.status == "published"
    assert row.created_by


# =====================================================================
# 二、生成任务 / 具体场景
# =====================================================================
async def test_generate_task_lifecycle(client: AsyncClient, scene_env: object) -> None:
    """202 受理 → 后台执行 succeeded → 具体场景可渲染、可复检、不可再取消。"""
    first = await client.post(
        f"{SCENE_BASE}/generate",
        headers=OPERATOR_HEADERS,
        json={
            "logical_id": "LS-BAS-001",
            "count": 6,
            "strategy": "distribution",
            "profile": "regression",
            "seed": 20261023,
        },
    )
    assert first.status_code == 202
    accepted = _data(first)
    task_id = accepted["task_id"]
    assert accepted["logical_id"] == "LS-BAS-001"
    assert accepted["count"] == 6 and accepted["seed"] == 20261023

    task = _data(await client.get(f"{SCENE_BASE}/generate/{task_id}", headers=ADMIN_HEADERS))
    assert task["status"] == "succeeded", task["error_message"]
    assert task["produced"] == 6
    assert task["progress"] == 1.0
    assert sum(task["zone_counts"].values()) == task["produced"]

    tasks = _data(await client.get(f"{SCENE_BASE}/generate", headers=ADMIN_HEADERS))
    assert tasks["total"] >= 1
    assert tasks["items"][0]["task_id"] == task_id

    listing = _data(
        await client.get(
            f"{SCENE_BASE}/concrete", headers=ADMIN_HEADERS, params={"logical_id": "LS-BAS-001"}
        )
    )
    assert listing["total"] == task["produced"]
    scenario_ids = [item["scenario_id"] for item in listing["items"]]

    rendered = _data(
        await client.post(
            f"{SCENE_BASE}/concrete/{scenario_ids[0]}/render", headers=OPERATOR_HEADERS
        )
    )
    assert rendered["passed"] is True
    assert {"map", "ego_vehicle"} <= set(rendered["scene_config"])
    assert len(rendered["gates"]) == 7

    detail = _data(await client.get(f"{SCENE_BASE}/concrete/{scenario_ids[0]}", headers=ADMIN_HEADERS))
    assert detail["param_values"]
    assert len(detail["param_vec"]) == len(detail["param_values"])
    assert detail["scene_config"]["ego_vehicle"]

    batch = await client.post(
        f"{SCENE_BASE}/concrete/batch-validate",
        headers=OPERATOR_HEADERS,
        json={"scenario_ids": scenario_ids},
    )
    assert batch.status_code == 200
    validated = _data(batch)
    assert validated["validated"] == len(scenario_ids)
    assert validated["quarantined"] == 0

    cancel = await client.post(
        f"{SCENE_BASE}/generate/{task_id}/cancel", headers=OPERATOR_HEADERS
    )
    assert cancel.status_code == 409
    _assert_envelope(cancel.json(), code=3003)


async def test_generate_is_deterministic_and_deduped(client: AsyncClient) -> None:
    """同 seed 重放：内容哈希一致 → 跨任务按 content_hash 幂等跳过。"""
    first = await _generate(client, count=4, seed=99)
    second = await _generate(client, count=4, seed=99)
    assert first["status"] == second["status"] == "succeeded"
    assert first["zone_counts"] == second["zone_counts"]
    assert first["produced"] == 4
    # 同 seed 生成同一批内容哈希 → 第二次全部被判重（G8/唯一约束）
    assert second["produced"] == 0

    rows = _data(
        await client.get(f"{SCENE_BASE}/concrete", headers=ADMIN_HEADERS, params={"page_size": 100})
    )["items"]
    hashes = [item["content_hash"] for item in rows]
    assert len(hashes) == len(set(hashes)) == 4


async def test_generate_rejects_unknown_logical(client: AsyncClient) -> None:
    """未知逻辑场景 → 404 + code=3001。"""
    resp = await client.post(
        f"{SCENE_BASE}/generate", headers=OPERATOR_HEADERS, json={"logical_id": "LS-ZZZ-999"}
    )
    assert resp.status_code == 404
    _assert_envelope(resp.json(), code=3001)


async def test_generate_rejects_replay_mutation_without_base(client: AsyncClient) -> None:
    """replay_mutation 缺少 overrides.base → 422 + code=2001。"""
    resp = await client.post(
        f"{SCENE_BASE}/generate",
        headers=OPERATOR_HEADERS,
        json={"logical_id": "LS-BAS-001", "count": 1, "strategy": "replay_mutation"},
    )
    assert resp.status_code == 422
    _assert_envelope(resp.json(), code=2001)


async def test_generate_rejects_unknown_overrides(client: AsyncClient) -> None:
    """overrides 仅接受白名单键 → 未知键 422 + code=2001。"""
    resp = await client.post(
        f"{SCENE_BASE}/generate",
        headers=OPERATOR_HEADERS,
        json={"logical_id": "LS-BAS-001", "count": 1, "overrides": {"typo_field": 1}},
    )
    assert resp.status_code == 422
    _assert_envelope(resp.json(), code=2001)


# =====================================================================
# 三、判据 / 测试用例
# =====================================================================
async def test_oracles_catalog(client: AsyncClient) -> None:
    """判据目录：代码即事实来源，返回版本化 ref。"""
    resp = await client.get(f"{SCENE_BASE}/oracles", headers=ADMIN_HEADERS)
    assert resp.status_code == 200
    data = _data(resp)
    refs = {item["oracle_ref"] for item in data["items"]}
    assert data["total"] == len(refs) == 7
    assert "oracle.safety.no_collision/v1" in refs
    assert "oracle.energy.soc_reserve/v1" in refs


async def test_create_testcase_and_get_detail(client: AsyncClient) -> None:
    """创建测试用例（201）→ 列表按 logical_id 过滤 → 详情字段完整。"""
    created = await client.post(
        f"{SCENE_BASE}/testcases", headers=OPERATOR_HEADERS, json=_testcase_payload()
    )
    assert created.status_code == 201
    data = _data(created)
    assert data["case_key"] == "TC-SAFE-001"
    assert data["status"] == "draft"
    assert data["logical_version"] == CATALOG_VERSION
    assert data["timeout_seconds"] == 180
    assert data["platforms"] == ["simulation", "replay"]

    listed = _data(
        await client.get(
            f"{SCENE_BASE}/testcases", headers=ADMIN_HEADERS, params={"logical_id": "LS-INT-001"}
        )
    )
    assert listed["total"] == 1

    detail = _data(
        await client.get(f"{SCENE_BASE}/testcases/{data['test_case_id']}", headers=ADMIN_HEADERS)
    )
    assert detail["test_case_id"] == data["test_case_id"]
    assert detail["objective"].startswith("验证切入工况")


async def test_create_testcase_rejects_duplicate_and_unknown_oracle(client: AsyncClient) -> None:
    """case_key 冲突 → 409 + code=3002；未知判据 → 422 + code=2001。"""
    await client.post(f"{SCENE_BASE}/testcases", headers=OPERATOR_HEADERS, json=_testcase_payload())

    duplicate = await client.post(
        f"{SCENE_BASE}/testcases", headers=OPERATOR_HEADERS, json=_testcase_payload()
    )
    assert duplicate.status_code == 409
    _assert_envelope(duplicate.json(), code=3002)

    unknown_oracle = await client.post(
        f"{SCENE_BASE}/testcases",
        headers=OPERATOR_HEADERS,
        json={**_testcase_payload("TC-SAFE-002"), "oracle_ref": "oracle.unknown/v1"},
    )
    assert unknown_oracle.status_code == 422
    _assert_envelope(unknown_oracle.json(), code=2001)


async def test_get_testcase_unknown_returns_404(client: AsyncClient) -> None:
    """未知用例 ID → 404 + code=3001。"""
    resp = await client.get(f"{SCENE_BASE}/testcases/{uuid4()}", headers=ADMIN_HEADERS)
    assert resp.status_code == 404
    _assert_envelope(resp.json(), code=3001)


# =====================================================================
# 四、测试集 / 基线
# =====================================================================
async def test_suite_lock_baseline_and_diff(client: AsyncClient) -> None:
    """建集 → 加条目 → 锁定（写哈希 + 基线）→ 差异一致 → 锁定后拒绝变更。"""
    created = await client.post(
        f"{SCENE_BASE}/testcases", headers=OPERATOR_HEADERS, json=_testcase_payload()
    )
    case_key = _data(created)["case_key"]

    suite_resp = await client.post(
        f"{SCENE_BASE}/suites",
        headers=OPERATOR_HEADERS,
        json={"name": "回归集-smoke", "description": "冒烟回归"},
    )
    assert suite_resp.status_code == 201
    suite = _data(suite_resp)
    suite_id = suite["suite_id"]
    assert suite["status"] == "draft" and suite["items"] == []

    added = _data(
        await client.post(
            f"{SCENE_BASE}/suites/{suite_id}/items",
            headers=OPERATOR_HEADERS,
            json={
                "items": [
                    {
                        "item_id": "item-1",
                        "item_type": "test_case",
                        "ref_id": case_key,
                        "weight": 2.0,
                    }
                ]
            },
        )
    )
    assert added["item_count"] == 1
    assert added["items"][0]["ref_id"] == case_key

    locked = _data(
        await client.post(
            f"{SCENE_BASE}/suites/{suite_id}/lock",
            headers=REVIEWER_HEADERS,
            json={"baseline_name": "baseline-2026-10", "metrics": {"pass_rate": 0.97}},
        )
    )
    assert locked["status"] == "locked"
    assert locked["item_count"] == 1
    assert len(locked["lock_hash"]) == 64
    assert locked["baseline_name"] == "baseline-2026-10"

    baselines = _data(
        await client.get(f"{SCENE_BASE}/suites/{suite_id}/baselines", headers=ADMIN_HEADERS)
    )
    assert len(baselines["items"]) == 1
    baseline = baselines["items"][0]
    assert baseline["suite_hash"] == locked["lock_hash"]
    assert baseline["metrics"]["pass_rate"] == 0.97
    assert baseline["metrics"]["items"][0]["item_id"] == "item-1"

    diff = _data(await client.get(f"{SCENE_BASE}/suites/{suite_id}/diff", headers=ADMIN_HEADERS))
    assert diff["identical"] is True
    assert diff["current_hash"] == diff["baseline_hash"] == locked["lock_hash"]
    assert diff["added"] == diff["removed"] == diff["changed"] == []

    relock = await client.post(
        f"{SCENE_BASE}/suites/{suite_id}/lock", headers=REVIEWER_HEADERS, json={}
    )
    assert relock.status_code == 409
    _assert_envelope(relock.json(), code=3003)

    mutate = await client.post(
        f"{SCENE_BASE}/suites/{suite_id}/items",
        headers=OPERATOR_HEADERS,
        json={
            "items": [
                {"item_id": "item-2", "item_type": "test_case", "ref_id": case_key}
            ]
        },
    )
    assert mutate.status_code == 409
    _assert_envelope(mutate.json(), code=3003)


async def test_suite_rejects_duplicate_name_and_unknown_ref(client: AsyncClient) -> None:
    """测试集名称唯一（409/3002）；条目引用必须存在（404/3001）。"""
    await client.post(f"{SCENE_BASE}/suites", headers=OPERATOR_HEADERS, json={"name": "集合-A"})
    duplicate = await client.post(
        f"{SCENE_BASE}/suites", headers=OPERATOR_HEADERS, json={"name": "集合-A"}
    )
    assert duplicate.status_code == 409
    _assert_envelope(duplicate.json(), code=3002)

    suite = _data(
        await client.post(f"{SCENE_BASE}/suites", headers=OPERATOR_HEADERS, json={"name": "集合-B"})
    )
    missing_ref = await client.post(
        f"{SCENE_BASE}/suites/{suite['suite_id']}/items",
        headers=OPERATOR_HEADERS,
        json={
            "items": [
                {"item_id": "item-1", "item_type": "test_case", "ref_id": "TC-NOT-FOUND"}
            ]
        },
    )
    assert missing_ref.status_code == 404
    _assert_envelope(missing_ref.json(), code=3001)


async def test_get_suite_unknown_returns_404(client: AsyncClient) -> None:
    """未知测试集 → 404 + code=3001。"""
    resp = await client.get(f"{SCENE_BASE}/suites/{uuid4()}", headers=ADMIN_HEADERS)
    assert resp.status_code == 404
    _assert_envelope(resp.json(), code=3001)


# =====================================================================
# 五、覆盖分析
# =====================================================================
async def test_coverage_overview_empty(client: AsyncClient) -> None:
    """无具体场景时覆盖总览为 0，矩阵标记为已应用。"""
    resp = await client.get(f"{SCENE_BASE}/coverage/overview", headers=ADMIN_HEADERS)
    assert resp.status_code == 200
    data = _data(resp)
    assert data["total"] == 0
    assert data["zone_counts"] == {}
    assert data["zone_ratio"] == {}
    assert data["risk_counts"] == {}
    assert data["risk_matrix_applied"] is True


async def test_coverage_gaps_returns_deficits(client: AsyncClient) -> None:
    """盲区按剖面配额计算缺口：Z3 目标 25 > Z4 目标 5，排序后 Z3 在前。"""
    resp = await client.get(
        f"{SCENE_BASE}/coverage/gaps", headers=ADMIN_HEADERS, params={"profile": "exploration"}
    )
    assert resp.status_code == 200
    data = _data(resp)
    assert data["profile"] == "exploration"
    assert len(data["items"]) == len(CATALOG) * 2 == 34
    first = data["items"][0]
    assert first["zone"] == "Z3"
    assert first["target"] == 25 and first["deficit"] == 25
    assert "/api/v1/scene/generate" in first["recommendation"]
    assert data["items"][-1]["zone"] == "Z4"


async def test_coverage_rejects_unknown_profile(client: AsyncClient) -> None:
    """未定义剖面 → 422 + code=2001。"""
    resp = await client.get(
        f"{SCENE_BASE}/coverage/gaps", headers=ADMIN_HEADERS, params={"profile": "unknown"}
    )
    assert resp.status_code == 422
    _assert_envelope(resp.json(), code=2001)


# =====================================================================
# 六、RBAC / 统一响应
# =====================================================================
async def test_engine_read_forbidden_for_viewer(client: AsyncClient) -> None:
    """角色不含 scene:read → 403 + code=1002。"""
    resp = await client.get(f"{SCENE_BASE}/logical", headers=VIEWER_HEADERS)
    assert resp.status_code == 403
    _assert_envelope(resp.json(), code=1002)


async def test_engine_execute_forbidden_for_viewer(client: AsyncClient) -> None:
    """角色不含 scene:execute → 生成提交 403 + code=1002。"""
    resp = await client.post(
        f"{SCENE_BASE}/generate", headers=VIEWER_HEADERS, json={"logical_id": "LS-BAS-001"}
    )
    assert resp.status_code == 403
    _assert_envelope(resp.json(), code=1002)


async def test_engine_requires_forwarded_identity(client: AsyncClient) -> None:
    """缺失网关注入身份头 → 401 + code=1001。"""
    resp = await client.get(f"{SCENE_BASE}/oracles")
    assert resp.status_code == 401
    _assert_envelope(resp.json(), code=1001)
