"""/api/v1/analytics/reports 端点测试（正常流 + 错误流，对照契约示例）。"""
from __future__ import annotations

import pytest

from app.core import dependencies as deps
from app.main import app
from app.repositories.reports import ReportRepository
from app.tests.conftest import ANALYST_HEADERS, OPERATOR_HEADERS, api
from app.tests.fakes import FakeVehicleClient, InMemoryObjectStorage

READY_REPORT_ID = "6f9619ff-8b86-d011-b42d-00c04fc964ff"


def _ready_sidecar() -> dict[str, object]:
    """ready 报告 sidecar（契约 components.schemas.ReportMeta 示例）。"""
    return {
        "report_id": READY_REPORT_ID,
        "report_type": "algorithm_eval",
        "status": "ready",
        "vehicle_id": None,
        "start_time": 1000.0,
        "end_time": 2000.0,
        "formats": ["html"],
        "created_at": 1500.0,
        "completed_at": 1600.0,
        "generated_by": "u-1",
        "trigger": "manual",
        "artifacts": [
            {"format": "html", "object_key": "reports/algorithm_eval/report.html", "size_bytes": 123}
        ],
        "summary": {"total_events": 3},
    }


def _seed_storage() -> None:
    """注入内存对象存储并预置 ready 报告。"""
    storage = InMemoryObjectStorage()
    app.state.__setattr__(deps.KEY_STORAGE, storage)
    app.state.__setattr__(
        deps.KEY_VEHICLE_CLIENT, FakeVehicleClient(exists={"HUNTER-001"}, total=5)
    )
    app.state.__setattr__(
        deps.KEY_REPORT_REPOSITORY,
        ReportRepository(storage, prefix="reports"),
    )


_GENERATE_BODY = {
    "report_type": "algorithm_eval",
    "start_time": 1000.0,
    "end_time": 2000.0,
    "formats": ["html", "json"],
}


@pytest.mark.asyncio
async def test_report_list_empty(clean_state: None) -> None:
    """/reports 空列表 → 200 code=0 total=0。"""
    _seed_storage()
    async with api() as client:
        resp = await client.get("/api/v1/analytics/reports", headers=ANALYST_HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    assert body["code"] == 0
    assert body["data"] == {"items": [], "total": 0, "page": 1, "page_size": 20}


@pytest.mark.asyncio
async def test_generate_and_detail_flow(clean_state: None) -> None:
    """提交报告 → 202 pending；列表可见；详情可查。"""
    _seed_storage()
    async with api() as client:
        resp = await client.post(
            "/api/v1/analytics/reports/generate", json=_GENERATE_BODY, headers=ANALYST_HEADERS
        )
        assert resp.status_code == 202
        body = resp.json()
        assert body["code"] == 0
        data = body["data"]
        assert data["status"] == "pending"
        assert data["report_type"] == "algorithm_eval"
        assert data["poll_url"] == f"/api/v1/analytics/reports/{data['report_id']}"

        listed = await client.get(
            "/api/v1/analytics/reports",
            params={"report_type": "algorithm_eval"},
            headers=ANALYST_HEADERS,
        )
        assert listed.json()["data"]["total"] == 1

        detail = await client.get(f"/api/v1/analytics/reports/{data['report_id']}", headers=ANALYST_HEADERS)
        assert detail.status_code == 200
        assert detail.json()["data"]["status"] == "pending"


@pytest.mark.asyncio
async def test_generate_duplicate_conflict(clean_state: None) -> None:
    """同参数重复提交 → 409 code=3003（防重复提交）。"""
    _seed_storage()
    async with api() as client:
        first = await client.post(
            "/api/v1/analytics/reports/generate", json=_GENERATE_BODY, headers=ANALYST_HEADERS
        )
        assert first.status_code == 202
        second = await client.post(
            "/api/v1/analytics/reports/generate", json=_GENERATE_BODY, headers=ANALYST_HEADERS
        )
    assert second.status_code == 409
    assert second.json()["code"] == 3003


@pytest.mark.asyncio
async def test_generate_concurrency_guard(clean_state: None) -> None:
    """并发上限（默认 2）：第 3 个生成中报告 → 409 code=3003。"""
    _seed_storage()
    bodies = [
        {"report_type": "algorithm_eval", "start_time": 1000.0, "end_time": 2000.0, "formats": ["html"]},
        {"report_type": "scene_test", "start_time": 1000.0, "end_time": 2000.0, "formats": ["html"]},
        {
            "report_type": "vehicle_daily",
            "vehicle_id": "HUNTER-001",
            "start_time": 1000.0,
            "end_time": 2000.0,
            "formats": ["html"],
        },
    ]
    async with api() as client:
        first = await client.post("/api/v1/analytics/reports/generate", json=bodies[0], headers=ANALYST_HEADERS)
        second = await client.post("/api/v1/analytics/reports/generate", json=bodies[1], headers=ANALYST_HEADERS)
        third = await client.post("/api/v1/analytics/reports/generate", json=bodies[2], headers=ANALYST_HEADERS)
    assert first.status_code == 202
    assert second.status_code == 202
    assert third.status_code == 409
    assert third.json()["code"] == 3003


@pytest.mark.asyncio
async def test_generate_vehicle_dimension_validation(clean_state: None) -> None:
    """车辆维度报告：缺 vehicle_id → 422/2001；车辆不存在 → 404/3001。"""
    _seed_storage()
    no_vehicle = {"report_type": "vehicle_daily", "start_time": 1000.0, "end_time": 2000.0, "formats": ["html"]}
    unknown_vehicle = {**no_vehicle, "vehicle_id": "HUNTER-999"}
    async with api() as client:
        missing = await client.post("/api/v1/analytics/reports/generate", json=no_vehicle, headers=ANALYST_HEADERS)
        not_found = await client.post(
            "/api/v1/analytics/reports/generate", json=unknown_vehicle, headers=ANALYST_HEADERS
        )
    assert missing.status_code == 422
    assert missing.json()["code"] == 2001
    assert not_found.status_code == 404
    assert not_found.json()["code"] == 3001


@pytest.mark.asyncio
async def test_generate_window_validation(clean_state: None) -> None:
    """窗口校验：end<=start → 2001；跨度 > 90 天 → 2001。"""
    _seed_storage()
    reversed_window = {"report_type": "scene_test", "start_time": 2000.0, "end_time": 1000.0, "formats": ["html"]}
    too_long = {"report_type": "scene_test", "start_time": 0.0, "end_time": 91 * 86400.0, "formats": ["html"]}
    async with api() as client:
        r1 = await client.post("/api/v1/analytics/reports/generate", json=reversed_window, headers=ANALYST_HEADERS)
        r2 = await client.post("/api/v1/analytics/reports/generate", json=too_long, headers=ANALYST_HEADERS)
    assert r1.status_code == 422 and r1.json()["code"] == 2001
    assert r2.status_code == 422 and r2.json()["code"] == 2001


@pytest.mark.asyncio
async def test_report_detail_not_found(clean_state: None) -> None:
    """详情不存在 → 404 code=3001。"""
    _seed_storage()
    async with api() as client:
        resp = await client.get(
            "/api/v1/analytics/reports/00000000-0000-0000-0000-000000000000", headers=ANALYST_HEADERS
        )
    assert resp.status_code == 404
    assert resp.json()["code"] == 3001


@pytest.mark.asyncio
async def test_ready_report_artifacts_presigned(clean_state: None) -> None:
    """ready 报告详情：产物附预签名 URL（15 分钟有效）。"""
    _seed_storage()
    storage = getattr(app.state, deps.KEY_STORAGE)
    assert isinstance(storage, InMemoryObjectStorage)
    await storage.put_json(f"reports/algorithm_eval/{READY_REPORT_ID}.json", _ready_sidecar())
    async with api() as client:
        resp = await client.get(f"/api/v1/analytics/reports/{READY_REPORT_ID}", headers=ANALYST_HEADERS)
    assert resp.status_code == 200
    artifact = resp.json()["data"]["artifacts"][0]
    assert artifact["download_url"].startswith("https://minio.test/reports/algorithm_eval/")
    assert artifact["expires_in"] == 900


@pytest.mark.asyncio
async def test_report_list_window_filter_validation(clean_state: None) -> None:
    """/reports 仅提供 start_time（不成对）→ 422/2001。"""
    _seed_storage()
    async with api() as client:
        resp = await client.get(
            "/api/v1/analytics/reports", params={"start_time": 1000.0}, headers=ANALYST_HEADERS
        )
    assert resp.status_code == 422
    assert resp.json()["code"] == 2001


@pytest.mark.asyncio
async def test_reports_require_authentication(clean_state: None) -> None:
    """缺 X-User-Id → 401 code=1001（api-gateway 直连保护）。"""
    _seed_storage()
    async with api() as client:
        resp = await client.get("/api/v1/analytics/reports")
    assert resp.status_code == 401
    assert resp.json()["code"] == 1001


@pytest.mark.asyncio
async def test_generate_requires_execute_role(clean_state: None) -> None:
    """operator 无 execute 权限 → 403 code=1002；operator 可读列表（read 集合包含 operator）。"""
    _seed_storage()
    async with api() as client:
        forbidden = await client.post(
            "/api/v1/analytics/reports/generate", json=_GENERATE_BODY, headers=OPERATOR_HEADERS
        )
        readable = await client.get("/api/v1/analytics/reports", headers=OPERATOR_HEADERS)
    assert forbidden.status_code == 403
    assert forbidden.json()["code"] == 1002
    assert readable.status_code == 200