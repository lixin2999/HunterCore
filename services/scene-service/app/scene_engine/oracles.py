"""判据（oracle）目录：把"如何判定通过"从场景参数中解耦（§2.10）。

判据是**可执行规则**而非用户数据，因此不做持久化：代码即事实来源，
``oracle_ref`` 以 ``oracle.<域>.<名称>/v<主版本>`` 形式被 IR / 测试用例引用；
版本升级必须新增 ref（既有 ref 语义不可变），保证历史用例可追溯。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class Oracle:
    """判据定义。"""

    ref: str
    name: str
    kind: str
    description: str
    metric: str
    operator: str
    threshold: float
    unit: str = ""
    platforms: tuple[str, ...] = ("simulation",)
    timeout_seconds: int = 120
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "oracle_ref": self.ref,
            "name": self.name,
            "kind": self.kind,
            "description": self.description,
            "metric": self.metric,
            "operator": self.operator,
            "threshold": self.threshold,
            "unit": self.unit,
            "platforms": list(self.platforms),
            "timeout_seconds": self.timeout_seconds,
            "extra": dict(self.extra),
        }


ORACLES: tuple[Oracle, ...] = (
    Oracle(
        ref="oracle.safety.no_collision/v1",
        name="全程无碰撞",
        kind="state_predicate",
        description="仿真全程碰撞次数为 0（车-车 / 车-人 / 车-物）。",
        metric="collision_count",
        operator="==",
        threshold=0.0,
        unit="次",
    ),
    Oracle(
        ref="oracle.safety.min_distance/v1",
        name="最小安全距离",
        kind="metric_threshold",
        description="与最近交通参与者的最小间距不低于场景 success_criteria.min_safe_distance。",
        metric="min_distance",
        operator=">=",
        threshold=1.0,
        unit="m",
    ),
    Oracle(
        ref="oracle.task.speed_tracking/v1",
        name="速度跟踪误差",
        kind="metric_threshold",
        description="实际车速与目标车速的偏差不超过 success_criteria.max_speed_deviation。",
        metric="max_speed_deviation",
        operator="<=",
        threshold=2.0,
        unit="m/s",
    ),
    Oracle(
        ref="oracle.comfort.accel/v1",
        name="纵向/横向加速度平顺性",
        kind="metric_threshold",
        description="纵向加速度 |a_x| ≤ 3.0 m/s²、横向加速度 |a_y| ≤ 2.5 m/s² 的时长占比 ≥ 95%。",
        metric="comfort_violation_ratio",
        operator="<=",
        threshold=0.05,
        extra={"ax_limit": 3.0, "ay_limit": 2.5},
    ),
    Oracle(
        ref="oracle.geometry.clearance/v1",
        name="几何间隙可行",
        kind="metric_threshold",
        description="HUNTER SE 车体包络与障碍/对向车的最小横向间隙 ≥ success_criteria.min_safe_distance。",
        metric="lateral_clearance",
        operator=">=",
        threshold=0.3,
        unit="m",
        extra={"vehicle_width": 1.8},
    ),
    Oracle(
        ref="oracle.energy.soc_reserve/v1",
        name="电量安全余量（SP-PWR-01）",
        kind="metric_threshold",
        description="任务结束时 SOC 不低于 5%，且未出现动力中断。",
        metric="soc_end",
        operator=">=",
        threshold=0.05,
        unit="1",
        extra={"max_power_cutoff": 0},
    ),
    Oracle(
        ref="oracle.localization.degraded/v1",
        name="定位降级可恢复",
        kind="temporal_logic",
        description="定位误差 > 0.5m 持续不超过 2s，且能回到定位正常态。",
        metric="localization_degraded_seconds",
        operator="<=",
        threshold=2.0,
        unit="s",
    ),
)

ORACLE_BY_REF: dict[str, Oracle] = {oracle.ref: oracle for oracle in ORACLES}


def get_oracle(ref: str) -> Oracle | None:
    """按 ref 取判据（未知 ref 返回 None，由调用方转 2001）。"""
    return ORACLE_BY_REF.get(ref)


def list_oracles() -> list[dict[str, Any]]:
    """判据目录（GET /api/v1/scene/oracles）。"""
    return [oracle.to_dict() for oracle in ORACLES]


__all__ = ["ORACLES", "ORACLE_BY_REF", "Oracle", "get_oracle", "list_oracles"]
