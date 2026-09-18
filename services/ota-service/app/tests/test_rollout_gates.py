"""灰度推进与升级门禁纯逻辑测试（x-hunter-canary-rollout + 设计文档门禁阈值）。"""
from __future__ import annotations

from typing import Any

import pytest
from hunter_common.database.enums import OtaStatus
from hunter_common.exceptions import InvalidParameterError

from app.config import settings
from app.repositories.records import RecordSnapshot
from app.schemas.common import OtaBatchStatus, OtaNextAction, OtaPreconditionName
from app.schemas.tasks import OtaUpgradeStrategy
from app.services.gates import evaluate_gates
from app.services.rollout import (
    allocate_batches,
    build_rollout_view,
    build_task_progress,
    canonical_strategy,
    resolve_strategy,
)


def _snapshot(vehicle_id: str, status: OtaStatus) -> RecordSnapshot:
    return RecordSnapshot(
        vehicle_id=vehicle_id, status=status, progress=50, start_time=1000.0, end_time=None
    )


class TestAllocateBatches:
    """批次取整规则（契约 x-hunter-canary-rollout.rounding + 待确认 #14）。"""

    def test_hundred_vehicles(self) -> None:
        """100 台 → [5, 20, 50, 25]（末批补足剩余 25 台）。"""
        vehicles = [f"HUNTER-{i:03d}" for i in range(1, 101)]
        batches = allocate_batches(vehicles, [5.0, 20.0, 50.0, 100.0])
        assert [len(b) for b in batches] == [5, 20, 50, 25]
        assert sum(batches, []) == vehicles  # 不重不漏、保序

    def test_small_fleet_ceil_and_min_one(self) -> None:
        """7 台 → ceil 规则 [1, 2, 4, 0]（5%=0.35→1、20%=1.4→2、50%=3.5→4）。"""
        vehicles = [f"HUNTER-{i}" for i in range(7)]
        batches = allocate_batches(vehicles, [5.0, 20.0, 50.0, 100.0])
        assert [len(b) for b in batches] == [1, 2, 4, 0]

    def test_tiny_fleet(self) -> None:
        """3 台 → [1, 1, 1, 0]（每批 max(1, ceil) 且不超过剩余量；末批补足 = 0）。"""
        batches = allocate_batches(["A", "B", "C"], [5.0, 20.0, 50.0, 100.0])
        assert [len(b) for b in batches] == [1, 1, 1, 0]

    def test_last_batch_fills_remainder(self) -> None:
        """末批（100%）补足全部剩余车辆（契约 rounding）。"""
        vehicles = [f"V{i}" for i in range(40)]
        batches = allocate_batches(vehicles, [5.0, 20.0, 50.0, 100.0])
        assert [len(b) for b in batches] == [2, 8, 20, 10]


class TestResolveStrategy:
    """灰度策略冻结校验（显式给出必须逐字段等于规范值 → 否则 2001）。"""

    def test_none_returns_canonical(self) -> None:
        strategy = resolve_strategy(None, settings)
        assert [b.percent for b in strategy.batches] == [5, 20, 50, 100]
        assert strategy.stage_gate is True

    def test_equal_payload_accepted(self) -> None:
        canonical = canonical_strategy(settings)
        assert resolve_strategy(canonical, settings) is canonical

    def test_mismatch_rejected_with_2001(self) -> None:
        """schema 合法但与规范序列不一致（batch_no 逆序）→ 2001（灰度流程不可更改）。"""
        payload = OtaUpgradeStrategy.model_validate(
            {
                "batches": [
                    {"batch_no": n, "percent": p, "observe_hours": 24,
                     "success_rate_threshold": 0.95}
                    for n, p in ((4, 5), (3, 20), (2, 50), (1, 100))
                ],
                "stage_gate": True,
            }
        )
        with pytest.raises(InvalidParameterError):
            resolve_strategy(payload, settings)


class TestRolloutView:
    """灰度推进视图派生（next_action / halt_reason / 批次状态机）。"""

    def test_not_started(self) -> None:
        view = build_rollout_view(
            strategy=canonical_strategy(settings),
            allocation=[[], [], [], []],
            snapshots={},
            now=0.0,
        )
        assert view.current_batch == 0
        assert view.next_action == OtaNextAction.OBSERVING

    def test_batch_in_progress(self) -> None:
        view = build_rollout_view(
            strategy=canonical_strategy(settings),
            allocation=[["A"], ["B"], ["C"], ["D"]],
            snapshots={"A": _snapshot("A", OtaStatus.DOWNLOAD)},
            now=0.0,
        )
        assert view.current_batch == 1
        assert view.batches[0].status == OtaBatchStatus.IN_PROGRESS
        assert view.next_action == OtaNextAction.OBSERVING

    def test_batch_success_rate_and_halt(self) -> None:
        snapshots = {
            "A": RecordSnapshot("A", OtaStatus.SUCCESS, 100, 0.0, 100.0),
            "B": RecordSnapshot("B", OtaStatus.FAILED, 30, 0.0, 100.0),
        }
        view = build_rollout_view(
            strategy=canonical_strategy(settings),
            allocation=[["A", "B"], [], [], []],
            snapshots=snapshots,
            now=0.0,
        )
        assert view.batches[0].success_rate == pytest.approx(0.5)
        assert view.batches[0].status == OtaBatchStatus.HALTED
        assert view.next_action == OtaNextAction.HALT
        assert view.halt_reason is not None and "0.5" in view.halt_reason

    def test_passed_window_elapsed_advances(self) -> None:
        """批次 1 全部 SUCCESS 且观察窗口（完成 + 24h）已过 → advance。"""
        snapshots = {"A": RecordSnapshot("A", OtaStatus.SUCCESS, 100, 0.0, 1000.0)}
        view = build_rollout_view(
            strategy=canonical_strategy(settings),
            allocation=[["A"], ["B"], ["C"], ["D"]],
            snapshots=snapshots,
            now=1000.0 + 24 * 3600 + 1,
        )
        assert view.batches[0].status == OtaBatchStatus.PASSED
        assert view.next_action == OtaNextAction.ADVANCE

    def test_observing_window_active(self) -> None:
        """批次完成但观察窗口未结束 → observing（observe_until = 完成时间 + 24h）。"""
        snapshots = {"A": RecordSnapshot("A", OtaStatus.SUCCESS, 100, 0.0, 1000.0)}
        view = build_rollout_view(
            strategy=canonical_strategy(settings),
            allocation=[["A"], ["B"], ["C"], ["D"]],
            snapshots=snapshots,
            now=1000.0 + 3600,
        )
        assert view.batches[0].status == OtaBatchStatus.OBSERVING
        assert view.next_action == OtaNextAction.OBSERVING
        assert view.observe_until == 1000.0 + 24 * 3600


class TestTaskProgress:
    """任务聚合进度（分母 = succeeded + failed + rolled_back）。"""

    def test_counts_and_success_rate(self) -> None:
        snapshots = {
            "A": RecordSnapshot("A", OtaStatus.SUCCESS, 100, 0.0, 1.0),
            "B": RecordSnapshot("B", OtaStatus.FAILED, 40, 0.0, 1.0),
            "C": RecordSnapshot("C", OtaStatus.DOWNLOAD, 40, 0.0, None),
        }
        progress = build_task_progress(total_vehicles=5, snapshots=snapshots, current_batch=1)
        assert progress.total == 5
        assert progress.pending == 2
        assert progress.in_progress == 1
        assert progress.succeeded == 1
        assert progress.failed == 1
        assert progress.success_rate == pytest.approx(0.5)
        assert progress.current_batch == 1

    def test_no_terminal_records_rate_is_null(self) -> None:
        """无终态记录 → success_rate=null（insufficient_data，不误判，契约 insufficient_data）。"""
        progress = build_task_progress(total_vehicles=3, snapshots={}, current_batch=0)
        assert progress.success_rate is None
        assert progress.pending == 3


class TestEvaluateGates:
    """升级门禁评估（电量 ≥ 50% / P 档 / 网络 ≤ 10s / 存储 ≥ 2048MB）。"""

    def _evaluate(self, status: dict[str, Any] | None, **overrides: Any):
        kwargs: dict[str, Any] = dict(
            require_soc=True,
            require_parked=True,
            require_network=True,
            min_soc=50,
            min_storage_mb=2048,
            offline_threshold_seconds=10,
        )
        kwargs.update(overrides)
        return evaluate_gates("HUNTER-001", status, **kwargs)

    def test_all_pass(self) -> None:
        result = self._evaluate(
            {"battery_soc": 80, "gear": "P", "last_seen_seconds": 3, "free_storage_mb": 8192}
        )
        assert result.released
        assert result.failed_conditions == ()

    def test_low_soc_rejected(self) -> None:
        result = self._evaluate({"battery_soc": 42, "gear": "P"})
        assert not result.released
        assert OtaPreconditionName.BATTERY_SOC in result.failed_conditions

    def test_not_parked_rejected(self) -> None:
        result = self._evaluate({"battery_soc": 90, "gear": "D"})
        assert OtaPreconditionName.VEHICLE_PARKED in result.failed_conditions

    def test_slow_network_rejected(self) -> None:
        result = self._evaluate({"battery_soc": 90, "gear": "P", "last_seen_seconds": 11})
        assert OtaPreconditionName.NETWORK_STABLE in result.failed_conditions

    def test_low_storage_rejected(self) -> None:
        result = self._evaluate({"battery_soc": 90, "gear": "P", "free_storage_mb": 512})
        assert OtaPreconditionName.STORAGE in result.failed_conditions

    def test_missing_read_model_rejects_all(self) -> None:
        """读模型缺失（fallback=false 安全默认）→ 全部必检项拒绝（#19）。"""
        result = self._evaluate(None)
        assert not result.released
        assert len(result.failed_conditions) == 4

    def test_rollback_skips_soc_gate(self) -> None:
        """回滚门禁不查电量（仅静止 + P 档 + 网络 + 存储）。"""
        result = self._evaluate(
            {"gear": "P", "last_seen_seconds": 3, "free_storage_mb": 4096}, require_soc=False
        )
        assert result.released


class TestSettingsContractFixedValues:
    """契约固定值启动校验（OTA_CANARY_* 不可更改，见 app/config.py validators）。"""

    def test_canonical_stages(self) -> None:
        assert settings.canonical_canary_stages == [5.0, 20.0, 50.0, 100.0]
        assert settings.ota_canary_observe_hours == 24
        assert settings.ota_canary_min_success_rate == 0.95

    def test_strategy_jsonb_roundtrip(self) -> None:
        """规范策略可 JSON 序列化往返（写入 ota_tasks.upgrade_strategy JSONB）。"""
        dump = canonical_strategy(settings).model_dump()
        assert OtaUpgradeStrategy.model_validate(dump) == canonical_strategy(settings)
