"""灰度自动调度器与批次批量取数测试（G-14：x-hunter-canary-rollout.scheduler）。

覆盖：
- ``derive_scheduler_action`` 纯函数决策（halt / succeed / advance / none）；
- ``check_batch`` 批量门禁（与逐车 ``check_vehicle`` 判定口径一致）；
- ``TaskService.scheduler_tick`` 端到端：到点启 scheduled、推进、暂停、完成、幂等、禁止跳批。
"""
from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from hunter_common.database.enums import OtaStatus, OtaTaskStatus
from hunter_common.database.models import OtaTask

from app.config import settings
from app.main import app
from app.repositories.records import RecordSnapshot
from app.schemas.common import OtaBatchStatus, OtaPreconditionName
from app.services.gates import check_batch
from app.services.rollout import (
    build_rollout_view,
    canonical_strategy,
    derive_scheduler_action,
)
from app.tests.conftest import make_task, make_version

#: 30 台车队 → 规范批次切分为 [2, 6, 15, 7]（ceil(5%/20%/50%) + 末批补足）
_FLEET = [f"HUNTER-{i:03d}" for i in range(1, 31)]
_BATCH1 = _FLEET[:2]
_BATCH4 = _FLEET[23:30]


def _seed_online(reader: Any, vehicle_ids: list[str]) -> None:
    """将车辆置为在线且门禁全通过（SOC 80/P 档/遥测 3s/存储 8192MB）。"""
    for vid in vehicle_ids:
        reader.seed(
            vid, battery_soc=80, gear="P", last_seen_seconds=3, free_storage_mb=8192
        )


async def _seed_records(
    env: Any, task_id: UUID, statuses: dict[str, OtaStatus], *, ended_seconds_ago: float | None
) -> None:
    """直写 ota_records（模拟车端回传终态），供调度器按快照评估批次。"""
    now = time.time()
    await env.records.insert_released(
        None,
        [
            {
                "task_id": task_id,
                "vehicle_id": vid,
                "from_version": None,
                "to_version": "V1.2.0",
                "status": OtaStatus.PENDING,
                "phase": OtaStatus.IDLE,
                "progress": 0,
            }
            for vid in statuses
        ],
    )
    for rec in env.records.rows[task_id]:
        status = statuses.get(rec.vehicle_id)
        if status is None:
            continue
        rec.status = status
        rec.progress = 100 if status == OtaStatus.SUCCESS else 40
        if ended_seconds_ago is not None:
            rec.end_time = datetime.fromtimestamp(now - ended_seconds_ago, tz=UTC)


def _register(env: Any, *, status: OtaTaskStatus = OtaTaskStatus.RUNNING) -> OtaTask:
    """登记已发布版本 + 一个任务行（默认 running），返回任务。"""
    version = make_version()
    env.versions.rows[version.version_id] = version
    task = make_task(target_version_id=version.version_id, target_vehicles=list(_FLEET), status=status)
    env.tasks.rows[task.task_id] = task
    return task


class TestDeriveSchedulerAction:
    """调度器单任务决策纯函数（基于 build_rollout_view 结果）。"""

    def _view(self, snapshots: dict[str, RecordSnapshot], allocation: list[list[str]], now: float):
        return build_rollout_view(
            strategy=canonical_strategy(settings),
            allocation=allocation,
            snapshots=snapshots,
            now=now,
        )

    def test_halt_on_low_success_rate(self) -> None:
        """首批成功率 0.5 < 0.95 → halt。"""
        view = self._view(
            {
                "A": RecordSnapshot("A", OtaStatus.SUCCESS, 100, 0.0, 100.0),
                "B": RecordSnapshot("B", OtaStatus.FAILED, 30, 0.0, 100.0),
            },
            [["A", "B"], ["C"], ["D"], []],
            now=0.0,
        )
        assert view.batches[0].status == OtaBatchStatus.HALTED
        assert derive_scheduler_action(view) == "halt"

    def test_advance_when_not_last_batch_passed(self) -> None:
        """首批已过观察窗且达标、非末批 → advance。"""
        snapshots = {"A": RecordSnapshot("A", OtaStatus.SUCCESS, 100, 0.0, 1000.0)}
        view = self._view(snapshots, [["A"], ["B"], ["C"], ["D"]], now=1000.0 + 24 * 3600 + 1)
        assert view.batches[0].status == OtaBatchStatus.PASSED
        assert derive_scheduler_action(view) == "advance"

    def test_succeed_on_last_batch_passed(self) -> None:
        """末批（batch 4）已过观察窗且达标 → succeed（不跳批，current_batch=4）。"""
        snapshots = {
            vid: RecordSnapshot(vid, OtaStatus.SUCCESS, 100, 0.0, 1000.0) for vid in ("W", "X", "Y")
        }
        view = self._view(snapshots, [[], [], [], ["W", "X", "Y"]], now=1000.0 + 24 * 3600 + 1)
        assert view.current_batch == 4
        assert view.batches[3].status == OtaBatchStatus.PASSED
        assert derive_scheduler_action(view) == "succeed"

    def test_none_when_in_progress(self) -> None:
        """首批下载中（无终态）→ none。"""
        snapshots = {"A": RecordSnapshot("A", OtaStatus.DOWNLOAD, 40, 1000.0, None)}
        view = self._view(snapshots, [["A"], ["B"], ["C"], ["D"]], now=1000.0 + 3600)
        assert derive_scheduler_action(view) == "none"

    def test_none_when_observing(self) -> None:
        """首批完成但观察窗未过 → none（observing）。"""
        snapshots = {"A": RecordSnapshot("A", OtaStatus.SUCCESS, 100, 0.0, 1000.0)}
        view = self._view(snapshots, [["A"], ["B"], ["C"], ["D"]], now=1000.0 + 3600)
        assert view.batches[0].status == OtaBatchStatus.OBSERVING
        assert derive_scheduler_action(view) == "none"


class TestCheckBatch:
    """批次批量门禁（is_online_many + get_status_many 两次批量读）。"""

    async def test_mixed_results_match_single_gate(self, ota_env: Any) -> None:
        """在线且达标放行；不在线 offline；在线但门禁不过 blocked。"""
        reader = ota_env.reader
        _seed_online(reader, ["OK"])
        reader.seed("LOW_SOC", battery_soc=10, gear="P", last_seen_seconds=3, free_storage_mb=8192)
        # OFFLINE 不 seed（不在在线集合）
        results = await check_batch(reader, ["OK", "LOW_SOC", "OFFLINE"], settings)
        by_id = {r.vehicle_id: r for r in results}
        assert by_id["OK"].released
        assert not by_id["LOW_SOC"].released
        assert OtaPreconditionName.BATTERY_SOC in by_id["LOW_SOC"].failed_conditions
        assert by_id["OFFLINE"].offline

    async def test_empty_input_returns_empty(self, ota_env: Any) -> None:
        assert await check_batch(ota_env.reader, [], settings) == []


class TestSchedulerTick:
    """scheduler_tick 端到端（到点启动 / 推进 / 暂停 / 完成 / 幂等 / 非候选）。"""

    async def test_starts_due_scheduled(self, ota_env: Any) -> None:
        """scheduled 到点 → 启首批（created → running，首批 2 台放行并下发通知）。"""
        _seed_online(ota_env.reader, _FLEET)
        task = _register(ota_env, status=OtaTaskStatus.CREATED)
        task.schedule = {"mode": "scheduled", "start_time": time.time() - 1}
        counters = await app.state.task_service.scheduler_tick()
        assert counters["start"] == 1
        assert task.status == OtaTaskStatus.RUNNING
        assert len(ota_env.records.rows[task.task_id]) == len(_BATCH1)
        assert len(ota_env.notify.sent) == len(_BATCH1)

    async def test_ignores_future_scheduled_and_immediate(self, ota_env: Any) -> None:
        """未到点的 scheduled 与 immediate created 均非候选（不自动启动）。"""
        version = make_version()
        ota_env.versions.rows[version.version_id] = version
        future = make_task(target_version_id=version.version_id, target_vehicles=list(_FLEET))
        future.schedule = {"mode": "scheduled", "start_time": time.time() + 3600}
        ota_env.tasks.rows[future.task_id] = future
        immediate = make_task(target_version_id=version.version_id, target_vehicles=list(_FLEET))
        ota_env.tasks.rows[immediate.task_id] = immediate
        counters = await app.state.task_service.scheduler_tick()
        assert sum(counters.values()) == 0
        assert future.status == OtaTaskStatus.CREATED
        assert immediate.status == OtaTaskStatus.CREATED

    async def test_advance_to_next_batch(self, ota_env: Any) -> None:
        """running 且首批已过观察窗达标 → 推进下批（禁止跳批：仅 current+1）。"""
        _seed_online(ota_env.reader, _FLEET)
        task = _register(ota_env)
        await _seed_records(
            ota_env, task.task_id, {vid: OtaStatus.SUCCESS for vid in _BATCH1},
            ended_seconds_ago=25 * 3600,
        )
        counters = await app.state.task_service.scheduler_tick()
        assert counters["advance"] == 1
        assert task.status == OtaTaskStatus.RUNNING
        assert task.progress["current_batch"] == 2
        # 累计记录 = 首批 2 + 第二批 6（跳批被禁止，仅推进到 current_batch+1）
        assert len(ota_env.records.rows[task.task_id]) == len(_BATCH1) + 6

    async def test_halt_sets_paused(self, ota_env: Any) -> None:
        """首批成功率不达标 → 任务 paused（CRITICAL 日志，不生产 alert_event）。"""
        task = _register(ota_env)
        await _seed_records(
            ota_env,
            task.task_id,
            {_BATCH1[0]: OtaStatus.SUCCESS, _BATCH1[1]: OtaStatus.FAILED},
            ended_seconds_ago=25 * 3600,
        )
        counters = await app.state.task_service.scheduler_tick()
        assert counters["halt"] == 1
        assert task.status == OtaTaskStatus.PAUSED

    async def test_succeed_on_last_batch(self, ota_env: Any) -> None:
        """末批全部达标且过观察窗 → succeeded。"""
        task = _register(ota_env)
        await _seed_records(
            ota_env,
            task.task_id,
            {vid: OtaStatus.SUCCESS for vid in _BATCH4},
            ended_seconds_ago=25 * 3600,
        )
        counters = await app.state.task_service.scheduler_tick()
        assert counters["succeed"] == 1
        assert task.status == OtaTaskStatus.SUCCEEDED

    async def test_tick_is_idempotent(self, ota_env: Any) -> None:
        """到点启动后再次 tick：首批仍下发中（无终态）→ 不重复启动、不重复推进。"""
        _seed_online(ota_env.reader, _FLEET)
        task = _register(ota_env, status=OtaTaskStatus.CREATED)
        task.schedule = {"mode": "scheduled", "start_time": time.time() - 1}
        svc = app.state.task_service
        await svc.scheduler_tick()
        records_after_first = len(ota_env.records.rows[task.task_id])
        notify_after_first = len(ota_env.notify.sent)
        counters2 = await svc.scheduler_tick()
        assert sum(counters2.values()) == 0  # 第二轮无动作（幂等）
        assert len(ota_env.records.rows[task.task_id]) == records_after_first
        assert len(ota_env.notify.sent) == notify_after_first
        assert task.status == OtaTaskStatus.RUNNING
