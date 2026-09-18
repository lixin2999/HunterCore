"""升级任务端点测试（契约 ota-service.yaml tasks 组：灰度/门禁/动作/回滚）。"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from httpx import AsyncClient
from hunter_common.database.enums import OtaStatus
from hunter_common.database.models import OtaRecord

from app.config import settings
from app.tests.conftest import (
    ADMIN_HEADERS,
    VIEWER_HEADERS,
    make_task,
    make_version,
)


def _task_payload(version_id: UUID, **overrides: Any) -> dict[str, Any]:
    """契约 OtaTaskCreateRequest.example 载荷（20 台车便于批次切分断言）。"""
    payload: dict[str, Any] = {
        "task_name": "V1.2.0 灰度升级（首批 5%）",
        "target_version_id": str(version_id),
        "target_vehicles": [f"HUNTER-{i:03d}" for i in range(1, 21)],
        "schedule": {"mode": "immediate"},
    }
    payload.update(overrides)
    return payload


def _seed_published_version(ota_env: Any, code: int = 10200):
    """写入已发布版本（发布门禁要求 target 版本 published）。"""
    row = make_version(code=code)
    ota_env.versions.rows[row.version_id] = row
    return row


async def test_create_task_freezes_canonical_strategy(
    client: AsyncClient, ota_env: Any
) -> None:
    """创建任务：status=created + 冻结规范灰度策略（5/20/50/100 + 24h + 0.95）。"""
    version = _seed_published_version(ota_env)
    resp = await client.post(
        "/api/v1/ota/tasks", json=_task_payload(version.version_id), headers=ADMIN_HEADERS
    )
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["status"] == "created"
    assert data["vehicle_count"] == 20
    batches = data["upgrade_strategy"]["batches"]
    assert [b["percent"] for b in batches] == [5, 20, 50, 100]
    assert all(b["observe_hours"] == 24 and b["success_rate_threshold"] == 0.95 for b in batches)
    assert data["upgrade_strategy"]["stage_gate"] is True
    assert data["target_vehicles"] is not None
    assert data["progress"]["current_batch"] == 0


async def test_create_task_strategy_mismatch_rejected(
    client: AsyncClient, ota_env: Any
) -> None:
    """显式灰度策略与规范序列不一致 → 422 + code=2001（灰度流程不可更改）。"""
    version = _seed_published_version(ota_env)
    payload = _task_payload(
        version.version_id,
        upgrade_strategy={
            "batches": [
                {"batch_no": 1, "percent": 10, "observe_hours": 24, "success_rate_threshold": 0.95},
                {"batch_no": 2, "percent": 20, "observe_hours": 24, "success_rate_threshold": 0.95},
                {"batch_no": 3, "percent": 50, "observe_hours": 24, "success_rate_threshold": 0.95},
                {"batch_no": 4, "percent": 100, "observe_hours": 24, "success_rate_threshold": 0.95},
            ],
            "stage_gate": True,
        },
    )
    resp = await client.post("/api/v1/ota/tasks", json=payload, headers=ADMIN_HEADERS)
    assert resp.status_code == 422
    assert resp.json()["code"] == 2001


async def test_create_task_version_not_found(client: AsyncClient) -> None:
    """目标版本不存在 → 404 + code=3001。"""
    payload = _task_payload("00000000-0000-4000-8000-000000000001")
    resp = await client.post("/api/v1/ota/tasks", json=payload, headers=ADMIN_HEADERS)
    assert resp.status_code == 404
    assert resp.json()["code"] == 3001


async def test_create_task_draft_version_conflict(
    client: AsyncClient, ota_env: Any
) -> None:
    """目标版本为 draft → 409 + code=3003（必须先发布）。"""
    row = make_version(status="draft")
    ota_env.versions.rows[row.version_id] = row
    resp = await client.post(
        "/api/v1/ota/tasks", json=_task_payload(row.version_id), headers=ADMIN_HEADERS
    )
    assert resp.status_code == 409
    assert resp.json()["code"] == 3003


async def test_create_task_vehicle_limit(
    client: AsyncClient, ota_env: Any, monkeypatch: Any
) -> None:
    """目标车辆数超上限 → 422 + code=2001（OTA_TASK_MAX_TARGET_VEHICLES）。"""
    monkeypatch.setattr(settings, "ota_task_max_target_vehicles", 3)
    version = _seed_published_version(ota_env)
    payload = _task_payload(version.version_id)
    resp = await client.post("/api/v1/ota/tasks", json=payload, headers=ADMIN_HEADERS)
    assert resp.status_code == 422
    assert resp.json()["code"] == 2001


async def test_list_tasks_omits_vehicle_list(client: AsyncClient, ota_env: Any) -> None:
    """任务列表 target_vehicles 为 null（仅计数，契约 OtaTaskItem.description）。"""
    version = _seed_published_version(ota_env)
    task = make_task(target_version_id=version.version_id, target_vehicles=["HUNTER-001"])
    ota_env.tasks.rows[task.task_id] = task
    resp = await client.get("/api/v1/ota/tasks", headers=ADMIN_HEADERS)
    assert resp.status_code == 200
    items = resp.json()["data"]["items"]
    assert items[0]["target_vehicles"] is None
    assert items[0]["vehicle_count"] == 1


async def test_get_task_detail_with_rollout(client: AsyncClient, ota_env: Any) -> None:
    """任务详情：target_version 快照 + rollout（四批 pending、next_action=observing）。"""
    version = _seed_published_version(ota_env)
    task = make_task(target_version_id=version.version_id, target_vehicles=["HUNTER-001"])
    ota_env.tasks.rows[task.task_id] = task
    resp = await client.get(f"/api/v1/ota/tasks/{task.task_id}", headers=ADMIN_HEADERS)
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["target_version"]["version_code"] == 10200
    assert data["rollout"]["current_batch"] == 0
    assert data["rollout"]["next_action"] == "observing"
    assert len(data["rollout"]["batches"]) == 4
    assert data["rollout"]["batches"][0]["status"] == "pending"


async def test_task_forbidden_for_viewer(client: AsyncClient) -> None:
    """viewer 无 ota:read → 403（RBAC 边界）。"""
    resp = await client.get("/api/v1/ota/tasks", headers=VIEWER_HEADERS)
    assert resp.status_code == 403


async def test_start_releases_first_batch_with_gates(
    client: AsyncClient, ota_env: Any
) -> None:
    """start：第 1 批（5%，20 台 → ceil=1 台）逐车门禁通过 → released=1，下发 ota_notify。"""
    version = _seed_published_version(ota_env)
    task = make_task(
        target_version_id=version.version_id,
        target_vehicles=[f"HUNTER-{i:03d}" for i in range(1, 21)],
    )
    ota_env.tasks.rows[task.task_id] = task
    for i in range(1, 21):
        ota_env.reader.seed(
            f"HUNTER-{i:03d}",
            battery_soc=80,
            gear="P",
            last_seen_seconds=3,
            free_storage_mb=8192,
        )
    resp = await client.post(
        f"/api/v1/ota/tasks/{task.task_id}/start", headers=ADMIN_HEADERS
    )
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["action"] == "start"
    assert data["status"] == "running"
    assert data["released_count"] == 1
    assert data["blocked_count"] == 0
    assert data["batch_no"] == 1
    assert data["released_vehicles"] == ["HUNTER-001"]
    # ota_notify 已下发（package_url = 发送时刻签发的 1 小时预签名地址）
    assert len(ota_env.notify.sent) == 1
    notify = ota_env.notify.sent[0]
    assert notify["vehicle_id"] == "HUNTER-001"
    assert notify["task_id"] == str(task.task_id)
    assert notify["package_url"].startswith("https://minio.test/")
    assert notify["preconditions"]["soc_min"] == 50


async def test_start_blocks_low_soc_vehicle(
    client: AsyncClient, ota_env: Any
) -> None:
    """门禁不满足（电量 42 < 50）→ blocked[] 给出 failed_conditions 与实测值。"""
    version = _seed_published_version(ota_env)
    task = make_task(target_version_id=version.version_id, target_vehicles=["HUNTER-001"])
    ota_env.tasks.rows[task.task_id] = task
    ota_env.reader.seed(
        "HUNTER-001", battery_soc=42, gear="P", last_seen_seconds=3, free_storage_mb=8192
    )
    resp = await client.post(
        f"/api/v1/ota/tasks/{task.task_id}/start", headers=ADMIN_HEADERS
    )
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["released_count"] == 0
    assert data["blocked_count"] == 1
    failure = data["blocked_vehicles"][0]
    assert failure["vehicle_id"] == "HUNTER-001"
    assert "battery_soc" in failure["failed_conditions"]
    assert failure["actual"]["battery_soc"] == 42


async def test_start_whole_batch_blocked_fails_task(
    client: AsyncClient, ota_env: Any
) -> None:
    """整批门禁全失败 → 任务转 failed（契约 start 描述第 4 步）。"""
    version = _seed_published_version(ota_env)
    task = make_task(target_version_id=version.version_id, target_vehicles=["HUNTER-001"])
    ota_env.tasks.rows[task.task_id] = task
    ota_env.reader.seed(
        "HUNTER-001", battery_soc=10, gear="D", last_seen_seconds=60, free_storage_mb=128
    )
    resp = await client.post(
        f"/api/v1/ota/tasks/{task.task_id}/start", headers=ADMIN_HEADERS
    )
    data = resp.json()["data"]
    assert data["status"] == "failed"
    assert ota_env.tasks.rows[task.task_id].status.value == "failed"
    assert len(ota_env.notify.sent) == 0


async def test_start_offline_vehicle_blocked(client: AsyncClient, ota_env: Any) -> None:
    """车辆不在线（读模型/在线集合缺失）→ blocked（vehicle_offline 语义）。"""
    version = _seed_published_version(ota_env)
    task = make_task(target_version_id=version.version_id, target_vehicles=["HUNTER-007"])
    ota_env.tasks.rows[task.task_id] = task
    resp = await client.post(
        f"/api/v1/ota/tasks/{task.task_id}/start", headers=ADMIN_HEADERS
    )
    data = resp.json()["data"]
    assert data["blocked_count"] == 1
    assert data["blocked_vehicles"][0]["actual"]["reason"] == "vehicle_offline"


async def test_start_is_idempotent_when_running(
    client: AsyncClient, ota_env: Any
) -> None:
    """重复 start（running）→ 200 幂等回执，不重复下发（released=0）。"""
    version = _seed_published_version(ota_env)
    task = make_task(target_version_id=version.version_id, target_vehicles=["HUNTER-001"])
    ota_env.tasks.rows[task.task_id] = task
    ota_env.reader.seed(
        "HUNTER-001", battery_soc=90, gear="P", last_seen_seconds=1, free_storage_mb=4096
    )
    first = await client.post(f"/api/v1/ota/tasks/{task.task_id}/start", headers=ADMIN_HEADERS)
    assert first.json()["data"]["released_count"] == 1
    second = await client.post(f"/api/v1/ota/tasks/{task.task_id}/start", headers=ADMIN_HEADERS)
    assert second.status_code == 200
    assert second.json()["data"]["released_count"] == 0
    assert second.json()["data"]["status"] == "running"
    assert len(ota_env.notify.sent) == 1  # 未重复下发


async def test_start_batch_no_skip_forbidden(
    client: AsyncClient, ota_env: Any
) -> None:
    """禁止跳批：batch_no != 当前批次 + 1 → 409 + code=3003（#3）。"""
    version = _seed_published_version(ota_env)
    task = make_task(target_version_id=version.version_id, target_vehicles=["HUNTER-001"])
    ota_env.tasks.rows[task.task_id] = task
    resp = await client.post(
        f"/api/v1/ota/tasks/{task.task_id}/start",
        json={"batch_no": 3},
        headers=ADMIN_HEADERS,
    )
    assert resp.status_code == 409
    assert resp.json()["code"] == 3003


async def test_pause_requires_running(client: AsyncClient, ota_env: Any) -> None:
    """非 running 状态调用 pause → 409 + code=3003（契约 pause 描述）。"""
    version = _seed_published_version(ota_env)
    task = make_task(target_version_id=version.version_id, target_vehicles=["HUNTER-001"])
    ota_env.tasks.rows[task.task_id] = task
    resp = await client.post(
        f"/api/v1/ota/tasks/{task.task_id}/pause",
        json={"reason": "成功率异常，人工介入排查"},
        headers=ADMIN_HEADERS,
    )
    assert resp.status_code == 409
    assert resp.json()["code"] == 3003


async def test_pause_resume_lifecycle(client: AsyncClient, ota_env: Any) -> None:
    """start → pause → resume 生命周期（resume 重新评估批次并回执 running）。"""
    version = _seed_published_version(ota_env)
    task = make_task(target_version_id=version.version_id, target_vehicles=["HUNTER-001"])
    ota_env.tasks.rows[task.task_id] = task
    ota_env.reader.seed(
        "HUNTER-001", battery_soc=90, gear="P", last_seen_seconds=1, free_storage_mb=4096
    )
    await client.post(f"/api/v1/ota/tasks/{task.task_id}/start", headers=ADMIN_HEADERS)
    paused = await client.post(
        f"/api/v1/ota/tasks/{task.task_id}/pause", json={"reason": "演练"}, headers=ADMIN_HEADERS
    )
    assert paused.json()["data"]["status"] == "paused"
    resumed = await client.post(
        f"/api/v1/ota/tasks/{task.task_id}/resume", headers=ADMIN_HEADERS
    )
    assert resumed.json()["data"]["status"] == "running"
    # 已下发的车辆不二次通知（幂等）
    assert len(ota_env.notify.sent) == 1


async def test_resume_rejected_when_halted(
    client: AsyncClient, ota_env: Any
) -> None:
    """批次成功率 < 门禁 → 拒绝恢复（409/3003 + data.halt_reason）。"""
    version = _seed_published_version(ota_env)
    task = make_task(
        target_version_id=version.version_id,
        target_vehicles=[f"HUNTER-{i:03d}" for i in range(1, 21)],
        status="paused",
    )
    ota_env.tasks.rows[task.task_id] = task
    # 批次 1（HUNTER-001）已下发且 FAILED → success_rate=0 < 0.95 → halted
    ota_env.records.rows[task.task_id] = [
        OtaRecord(
            record_id=1,
            task_id=task.task_id,
            vehicle_id="HUNTER-001",
            to_version=version.version_name,
            status=OtaStatus.FAILED,
            phase=OtaStatus.FAILED,
            progress=30,
            error_code="6001",
            error_message="sha256 mismatch",
            start_time=datetime.now(tz=timezone.utc),
            end_time=datetime.now(tz=timezone.utc),  # 终态必写 end_time（消费链路语义）
        )
    ]
    task.progress["current_batch"] = 1
    resp = await client.post(
        f"/api/v1/ota/tasks/{task.task_id}/resume", headers=ADMIN_HEADERS
    )
    assert resp.status_code == 409
    body = resp.json()
    assert body["code"] == 3003
    assert "halt_reason" in body["data"]


async def test_cancel_terminal_conflict(client: AsyncClient, ota_env: Any) -> None:
    """终态任务重复终止 → 409 + code=3003（契约 cancel 描述）。"""
    version = _seed_published_version(ota_env)
    task = make_task(
        target_version_id=version.version_id,
        target_vehicles=["HUNTER-001"],
        status="canceled",
    )
    ota_env.tasks.rows[task.task_id] = task
    resp = await client.post(
        f"/api/v1/ota/tasks/{task.task_id}/cancel", json={"reason": "演练"}, headers=ADMIN_HEADERS
    )
    assert resp.status_code == 409
    assert resp.json()["code"] == 3003


async def test_cancel_running_task(client: AsyncClient, ota_env: Any) -> None:
    """running → canceled（终态；reason 回显于回执）。"""
    version = _seed_published_version(ota_env)
    task = make_task(target_version_id=version.version_id, target_vehicles=["HUNTER-001"])
    ota_env.tasks.rows[task.task_id] = task
    ota_env.reader.seed(
        "HUNTER-001", battery_soc=90, gear="P", last_seen_seconds=1, free_storage_mb=4096
    )
    await client.post(f"/api/v1/ota/tasks/{task.task_id}/start", headers=ADMIN_HEADERS)
    resp = await client.post(
        f"/api/v1/ota/tasks/{task.task_id}/cancel", json={"reason": "业务调整"}, headers=ADMIN_HEADERS
    )
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["action"] == "cancel"
    assert data["status"] == "canceled"
    assert data["reason"] == "业务调整"


async def test_rollback_success_records_dispatch_command(
    client: AsyncClient, ota_env: Any
) -> None:
    """回滚：仅 SUCCESS 记录受理，逐车下发 ota_rollback 指令（topic=hunter.{id}.command）。"""
    version = _seed_published_version(ota_env)
    task = make_task(target_version_id=version.version_id, target_vehicles=["HUNTER-001"])
    ota_env.tasks.rows[task.task_id] = task
    ota_env.records.rows[task.task_id] = [
        OtaRecord(
            record_id=1,
            task_id=task.task_id,
            vehicle_id="HUNTER-001",
            to_version=version.version_name,
            status=OtaStatus.SUCCESS,
            phase=OtaStatus.SUCCESS,
            progress=100,
            start_time=datetime.now(tz=timezone.utc),
        ),
        OtaRecord(
            record_id=2,
            task_id=task.task_id,
            vehicle_id="HUNTER-002",
            to_version=version.version_name,
            status=OtaStatus.DOWNLOAD,
            phase=OtaStatus.DOWNLOAD,
            progress=40,
            start_time=datetime.now(tz=timezone.utc),
        ),
    ]
    ota_env.reader.seed(
        "HUNTER-001", battery_soc=90, gear="P", last_seen_seconds=1, free_storage_mb=4096
    )
    resp = await client.post(
        f"/api/v1/ota/tasks/{task.task_id}/rollback",
        json={
            "vehicle_ids": ["HUNTER-001", "HUNTER-002"],
            "reason": "灰度观察期内发现定位漂移，回滚上一分区",
        },
        headers=ADMIN_HEADERS,
    )
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["accepted_count"] == 1
    assert data["rejected_count"] == 1
    accepted = next(r for r in data["results"] if r["accepted"])
    assert accepted["vehicle_id"] == "HUNTER-001"
    assert accepted["topic"] == "hunter.HUNTER-001.command"
    rejected = next(r for r in data["results"] if not r["accepted"])
    assert rejected["reason"] == "status_not_allowed"
    assert rejected["current_status"] == "DOWNLOAD"
    command = ota_env.commands.sent[0]
    assert command["vehicle_id"] == "HUNTER-001"
    assert command["task_id"] == task.task_id
    assert command["target"] == "previous_slot"


async def test_rollback_offline_vehicle_rejected(
    client: AsyncClient, ota_env: Any
) -> None:
    """回滚门禁：车辆不在线 → rejected reason=vehicle_offline（契约 rollback 门禁）。"""
    version = _seed_published_version(ota_env)
    task = make_task(target_version_id=version.version_id, target_vehicles=["HUNTER-001"])
    ota_env.tasks.rows[task.task_id] = task
    ota_env.records.rows[task.task_id] = [
        OtaRecord(
            record_id=1,
            task_id=task.task_id,
            vehicle_id="HUNTER-001",
            to_version=version.version_name,
            status=OtaStatus.SUCCESS,
            phase=OtaStatus.SUCCESS,
            progress=100,
            start_time=datetime.now(tz=timezone.utc),
        )
    ]
    resp = await client.post(
        f"/api/v1/ota/tasks/{task.task_id}/rollback",
        json={"vehicle_ids": ["HUNTER-001"], "reason": "定位漂移"},
        headers=ADMIN_HEADERS,
    )
    data = resp.json()["data"]
    assert data["accepted_count"] == 0
    assert data["results"][0]["reason"] == "vehicle_offline"
    assert len(ota_env.commands.sent) == 0
