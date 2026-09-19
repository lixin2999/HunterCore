"""升级记录端点测试（契约 records 组：任务监控明细 / 单车历史时间线）。"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from httpx import AsyncClient
from hunter_common.database.enums import OtaStatus
from hunter_common.database.models import OtaRecord

from app.tests.conftest import ADMIN_HEADERS, make_task, make_version


def _record(task_id: Any, vehicle_id: str, to_version: str, *, status: OtaStatus) -> OtaRecord:
    """构造 ota_records 行（start/end 成对，duration 可派生）。"""
    start = datetime(2026, 9, 18, 8, 0, 0, tzinfo=UTC)
    end = start if status is not OtaStatus.SUCCESS else datetime(
        2026, 9, 18, 8, 30, 0, tzinfo=UTC
    )
    return OtaRecord(
        record_id=1,
        task_id=task_id,
        vehicle_id=vehicle_id,
        from_version="V1.1.0",
        to_version=to_version,
        status=status,
        phase=status,
        progress=100,
        error_code="6001" if status is OtaStatus.FAILED else None,
        error_message="sha256 mismatch" if status is OtaStatus.FAILED else None,
        start_time=start,
        end_time=end if status in {OtaStatus.SUCCESS, OtaStatus.FAILED} else None,
    )


async def test_task_records_require_existing_task(client: AsyncClient) -> None:
    """任务不存在 → 404 + code=3001（契约 /tasks/{task_id}/records 404）。"""
    resp = await client.get(
        "/api/v1/ota/tasks/00000000-0000-4000-8000-000000000000/records",
        headers=ADMIN_HEADERS,
    )
    assert resp.status_code == 404
    assert resp.json()["code"] == 3001


async def test_task_records_with_summary(client: AsyncClient, ota_env: Any) -> None:
    """任务记录：含任务级 summary（OtaTaskProgress）与派生 duration_seconds。"""
    version = make_version()
    ota_env.versions.rows[version.version_id] = version
    task = make_task(target_version_id=version.version_id, target_vehicles=["HUNTER-001"])
    ota_env.tasks.rows[task.task_id] = task
    ota_env.records.rows[task.task_id] = [
        _record(task.task_id, "HUNTER-001", version.version_name, status=OtaStatus.SUCCESS)
    ]
    resp = await client.get(
        f"/api/v1/ota/tasks/{task.task_id}/records", headers=ADMIN_HEADERS
    )
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["total"] == 1
    assert data["summary"] is not None
    assert data["summary"]["succeeded"] == 1
    assert data["summary"]["success_rate"] == 1.0
    item = data["items"][0]
    assert item["duration_seconds"] == 1800.0
    assert item["status"] == "SUCCESS"
    assert item["error_code"] is None


async def test_task_records_status_filter(client: AsyncClient, ota_env: Any) -> None:
    """按升级状态过滤（车端 9 态取值）。"""
    version = make_version()
    ota_env.versions.rows[version.version_id] = version
    task = make_task(
        target_version_id=version.version_id, target_vehicles=["HUNTER-001", "HUNTER-002"]
    )
    ota_env.tasks.rows[task.task_id] = task
    ota_env.records.rows[task.task_id] = [
        _record(task.task_id, "HUNTER-001", version.version_name, status=OtaStatus.SUCCESS),
        _record(task.task_id, "HUNTER-002", version.version_name, status=OtaStatus.FAILED),
    ]
    resp = await client.get(
        f"/api/v1/ota/tasks/{task.task_id}/records?status=FAILED", headers=ADMIN_HEADERS
    )
    data = resp.json()["data"]
    assert data["total"] == 1
    assert data["items"][0]["vehicle_id"] == "HUNTER-002"
    assert data["items"][0]["error_code"] == "6001"


async def test_vehicle_records_empty_is_ok(client: AsyncClient) -> None:
    """单车历史：无记录返回空列表（契约：该端点无 404）。"""
    resp = await client.get(
        "/api/v1/ota/vehicles/HUNTER-042/records", headers=ADMIN_HEADERS
    )
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["total"] == 0
    assert data["items"] == []
    assert data["summary"] is None  # summary 仅任务级端点返回


async def test_vehicle_records_invalid_id_rejected(client: AsyncClient) -> None:
    """非法 vehicle_id（不符合 VehicleId pattern）→ 422 + code=2001。"""
    resp = await client.get(
        "/api/v1/ota/vehicles/无效车辆ID/records", headers=ADMIN_HEADERS
    )
    assert resp.status_code == 422
    assert resp.json()["code"] == 2001
