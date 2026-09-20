"""灰度发布推进算法（x-hunter-canary-rollout；纯函数，便于单元测试）。

契约要点（不可更改）：
- 四批 5% / 20% / 50% / 100%（本批覆盖目标车辆的比例，绝对值非累计）；
- 批次取整：ceil(percent × total)，每批至少 1 台，末批（100%）补足全部剩余车辆；
- 成功率 = 本批 SUCCESS / (本批 SUCCESS + FAILED + ROLLED_BACK)；无终态记录时为 null
  （insufficient_data：next_action=observing，不误判通过也不误判失败）；
- 推进条件：「观察窗口已结束」且「成功率 ≥ 阈值」同时满足；禁止人工跳批；
- 暂停（halt）：成功率 < 阈值 → 任务 paused + 告警 + 人工介入。
"""
from __future__ import annotations

import math
from typing import TYPE_CHECKING

from hunter_common.database.enums import (
    OTA_ACTIVE_STATUSES,
    OTA_TERMINAL_STATUSES,
    OtaStatus,
)
from hunter_common.exceptions import InvalidParameterError

from app.schemas.common import OtaBatchStatus, OtaNextAction
from app.schemas.tasks import (
    OtaBatchProgress,
    OtaCanaryBatch,
    OtaRolloutView,
    OtaTaskProgress,
    OtaUpgradeStrategy,
)

if TYPE_CHECKING:
    from app.config import Settings
    from app.repositories.records import RecordSnapshot

#: 车端进行中状态（PENDING/DOWNLOAD/INSTALL/TEST/ROLLBACK）
_ACTIVE_STATUSES: frozenset[OtaStatus] = OTA_ACTIVE_STATUSES


def canonical_strategy(settings: Settings) -> OtaUpgradeStrategy:
    """规范灰度序列（写入 ota_tasks.upgrade_strategy 的缺省值）。"""
    batches = [
        OtaCanaryBatch(
            batch_no=index + 1,
            percent=int(percent),
            observe_hours=settings.ota_canary_observe_hours,
            success_rate_threshold=settings.ota_canary_min_success_rate,
        )
        for index, percent in enumerate(settings.canonical_canary_stages)
    ]
    return OtaUpgradeStrategy(batches=batches, stage_gate=True)


def resolve_strategy(
    payload: OtaUpgradeStrategy | None, settings: Settings
) -> OtaUpgradeStrategy:
    """冻结灰度策略：省略时取规范序列；显式给出必须逐字段等于规范值（否则 2001）。"""
    canonical = canonical_strategy(settings)
    if payload is None:
        return canonical
    if payload.model_dump() != canonical.model_dump():
        raise InvalidParameterError(
            message=(
                "灰度策略与规范序列不一致（2001 参数错误；灰度流程不可更改，"
                "须为 4 批 5/20/50/100 + 观察 24h + 成功率门禁 0.95）"
            ),
            details={"field": "upgrade_strategy", "expected": canonical.model_dump()},
        )
    return payload


def allocate_batches(vehicles: list[str], percents: list[float]) -> list[list[str]]:
    """批次车辆分配（保序去重后按 ceil 取整；末批补足剩余）。

    - 前 3 批：max(1, ceil(percent × total))，且不超过剩余车辆；
    - 第 4 批（100%）：补足全部剩余车辆（小车队可能为 0 台，见契约 rounding 待确认 #14）。
    """
    remaining = list(vehicles)
    batches: list[list[str]] = []
    for percent in percents[:3]:
        wanted = max(1, math.ceil(percent / 100 * len(vehicles)))
        take = remaining[:wanted]
        batches.append(take)
        remaining = remaining[len(take) :]
    batches.append(remaining)  # 末批补足剩余车辆
    return batches


def _batch_status(
    *,
    released_count: int,
    terminal_total: int,
    success_rate: float | None,
    finished_at: float | None,
    observe_until: float | None,
    now: float,
    threshold: float,
) -> OtaBatchStatus:
    """批次状态派生（pending/in_progress/observing/passed/halted）。"""
    if released_count == 0:
        return OtaBatchStatus.PENDING
    if terminal_total < released_count or finished_at is None:
        return OtaBatchStatus.IN_PROGRESS
    if success_rate is not None and success_rate < threshold:
        return OtaBatchStatus.HALTED
    if observe_until is not None and now < observe_until:
        return OtaBatchStatus.OBSERVING
    return OtaBatchStatus.PASSED


def build_rollout_view(
    *,
    strategy: OtaUpgradeStrategy,
    allocation: list[list[str]],
    snapshots: dict[str, RecordSnapshot],
    now: float,
) -> OtaRolloutView:
    """灰度推进视图（任务详情 rollout 字段；数据来源 ota_records 聚合）。

    - ``current_batch`` = 存在下发记录的最大批次号（0=尚未开始）；
    - ``next_action``：当前批 halted → halt；passed 且非末批 → advance；其余 → observing。
    """
    batches: list[OtaBatchProgress] = []
    current_batch = 0
    halt_reason: str | None = None
    for index, plan in enumerate(strategy.batches):
        vehicles = allocation[index] if index < len(allocation) else []
        released = [snapshots[v] for v in vehicles if v in snapshots]
        terminal = [s for s in released if s.status in OTA_TERMINAL_STATUSES]
        success = sum(1 for s in terminal if s.status == OtaStatus.SUCCESS)
        failed = sum(1 for s in terminal if s.status == OtaStatus.FAILED)
        rolled_back = sum(1 for s in terminal if s.status == OtaStatus.ROLLED_BACK)
        in_progress = sum(1 for s in released if s.status in _ACTIVE_STATUSES)
        denominator = success + failed + rolled_back
        success_rate = success / denominator if denominator else None
        started_at = min((s.start_time for s in released if s.start_time is not None), default=None)
        end_times = [s.end_time for s in terminal if s.end_time is not None]
        finished_at = (
            max(end_times) if end_times and len(terminal) == len(released) else None
        )
        observe_until = finished_at + plan.observe_hours * 3600 if finished_at is not None else None
        status = _batch_status(
            released_count=len(released),
            terminal_total=len(terminal),
            success_rate=success_rate,
            finished_at=finished_at,
            observe_until=observe_until,
            now=now,
            threshold=plan.success_rate_threshold,
        )
        if released:
            current_batch = plan.batch_no
        if status == OtaBatchStatus.HALTED and success_rate is not None:
            halt_reason = f"success_rate {success_rate:.4f} < {plan.success_rate_threshold}"
        batches.append(
            OtaBatchProgress(
                batch_no=plan.batch_no,
                percent=plan.percent,
                status=status,
                target_count=len(vehicles),
                success_count=success,
                failed_count=failed,
                rolled_back_count=rolled_back,
                in_progress_count=in_progress,
                success_rate=success_rate,
                observe_until=observe_until,
                started_at=started_at,
                finished_at=finished_at,
            )
        )

    current = next((b for b in batches if b.batch_no == current_batch), None)
    next_action = OtaNextAction.OBSERVING
    if current is not None:
        if current.status == OtaBatchStatus.HALTED:
            next_action = OtaNextAction.HALT
        elif current.status == OtaBatchStatus.PASSED and current.batch_no < len(strategy.batches):
            next_action = OtaNextAction.ADVANCE
    return OtaRolloutView(
        current_batch=current_batch,
        total_batches=len(strategy.batches),
        observe_until=current.observe_until if current else None,
        next_action=next_action,
        halt_reason=halt_reason,
        batches=batches,
    )


def build_task_progress(
    *,
    total_vehicles: int,
    snapshots: dict[str, RecordSnapshot],
    current_batch: int,
) -> OtaTaskProgress:
    """任务聚合进度（写入 ota_tasks.progress JSONB；分母口径见契约 OtaTaskProgress）。"""
    terminal = [s for s in snapshots.values() if s.status in OTA_TERMINAL_STATUSES]
    success = sum(1 for s in terminal if s.status == OtaStatus.SUCCESS)
    failed = sum(1 for s in terminal if s.status == OtaStatus.FAILED)
    rolled_back = sum(1 for s in terminal if s.status == OtaStatus.ROLLED_BACK)
    in_progress = sum(1 for s in snapshots.values() if s.status in _ACTIVE_STATUSES)
    denominator = success + failed + rolled_back
    return OtaTaskProgress(
        total=total_vehicles,
        pending=total_vehicles - len(snapshots),
        in_progress=in_progress,
        succeeded=success,
        failed=failed,
        rolled_back=rolled_back,
        success_rate=success / denominator if denominator else None,
        current_batch=max(0, min(4, current_batch)),
    )


def derive_scheduler_action(rollout: OtaRolloutView) -> str:
    """灰度自动调度器单任务决策（纯函数，基于 build_rollout_view 结果）。

    返回：
    - ``halt``：当前批成功率 < 门禁 → 任务置 paused（人工介入）；
    - ``succeed``：末批（100%）已过观察窗且达标 → 任务置 succeeded；
    - ``advance``：当前批已过观察窗且达标且非末批 → 推进下一批；
    - ``none``：其余（进行中/观察中/未开始）→ 本轮不动作。
    """
    current = next((b for b in rollout.batches if b.batch_no == rollout.current_batch), None)
    if current is None:
        return "none"
    if current.status == OtaBatchStatus.HALTED:
        return "halt"
    if current.status == OtaBatchStatus.PASSED:
        if current.batch_no >= rollout.total_batches:
            return "succeed"
        return "advance"
    return "none"


__all__ = [
    "allocate_batches",
    "build_rollout_view",
    "build_task_progress",
    "canonical_strategy",
    "derive_scheduler_action",
    "resolve_strategy",
]
