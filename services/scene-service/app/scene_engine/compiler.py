"""场景编译器：IR + 参数向量 → 可下发 ``SceneConfig``（含 G1-G7 闸门）。

与 ``ir.compile_parameters`` 的分工：
- ``ir`` 只做纯表达式求值（无业务校验，便于生成器高频调用）；
- 本模块负责"对外一次编译"的语义：缺省参数取分布中位数、执行闸门、
  失败时抛 ``GateFailure``（携带逐门明细，路由层转 2001）。
"""
from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Any

from app.scene_engine.esmini import EsminiRunner
from app.scene_engine.ir import SceneIR, compile_parameters
from app.scene_engine.stats import distribution_quantile, sample_distribution
from app.scene_engine.validators import ValidationReport, run_gates
from app.scene_engine.vehicle_params import VehicleParams


class GateFailure(ValueError):
    """参数/结构未通过校验门（G1-G7）。"""

    def __init__(self, report: ValidationReport) -> None:
        super().__init__("；".join(report.failures) or "未通过校验门")
        self.report = report


@dataclass(frozen=True, slots=True)
class Compilation:
    """一次编译的产物（配置 + 参数 + 闸门明细）。"""

    ir: SceneIR
    values: dict[str, float | str]
    config: dict[str, Any]
    report: ValidationReport

    @property
    def passed(self) -> bool:
        return self.report.passed


def default_values(ir: SceneIR) -> dict[str, float | str]:
    """各参数取边缘分布中位数（分类参数取首个取值），用于目录校验/展示。"""
    values: dict[str, float | str] = {}
    for parameter in ir.parameters:
        distribution = parameter.distribution
        if distribution.kind == "categorical":
            choices = distribution.params.get("choices") or []
            values[parameter.name] = str(choices[0]["value"]) if choices else ""
            continue
        quantile = distribution_quantile(distribution.kind, distribution.params, 0.5)
        if quantile is None:
            sampled = sample_distribution(distribution.kind, distribution.params, random.Random(0))
            quantile = float(sampled) if isinstance(sampled, (int, float)) else 0.0
        values[parameter.name] = float(quantile)
    return values


def compile_scene(
    ir: SceneIR,
    values: dict[str, float | str],
    *,
    max_duration: float,
    allowed_maps: frozenset[str] | None = None,
    vehicle: VehicleParams | None = None,
    bounds: tuple[float, float, float, float] | None = None,
    esmini: EsminiRunner | None = None,
) -> Compilation:
    """编译并执行 G1-G7；不抛异常，由调用方决定是否拒绝。"""
    config = compile_parameters(ir, values)
    report = run_gates(
        ir,
        values,
        config,
        max_duration=max_duration,
        allowed_maps=allowed_maps,
        vehicle=vehicle,
        bounds=bounds,
        esmini=esmini,
    )
    return Compilation(ir=ir, values=dict(values), config=config, report=report)


def render_scene(
    ir: SceneIR,
    values: dict[str, float | str],
    *,
    max_duration: float,
    allowed_maps: frozenset[str] | None = None,
    vehicle: VehicleParams | None = None,
    bounds: tuple[float, float, float, float] | None = None,
    esmini: EsminiRunner | None = None,
) -> Compilation:
    """编译并要求全部闸门通过（否则抛 ``GateFailure``）。"""
    compilation = compile_scene(
        ir,
        values,
        max_duration=max_duration,
        allowed_maps=allowed_maps,
        vehicle=vehicle,
        bounds=bounds,
        esmini=esmini,
    )
    if not compilation.passed:
        raise GateFailure(compilation.report)
    return compilation


__all__ = [
    "Compilation",
    "GateFailure",
    "compile_scene",
    "default_values",
    "render_scene",
]
