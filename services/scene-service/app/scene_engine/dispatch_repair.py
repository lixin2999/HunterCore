"""存量场景下发期 best-effort 修复（V1.27.0）。

背景（``docs/HunterSim场景语义对齐方案.md`` 改动 A + 遗留项「3 子场景仅映射 1 actor」）：

1. V1.27.0 之前 :func:`app.scene_engine.semantics.resolve_value` 不解析**裸参数名**，
   ``crossing_speed: animal_speed`` 以字符串落库并下发，仿真端只能按 ``unresolved_param``
   回落默认速度；
2. ``animal_crossing`` 的 animal 角色 ``count`` 旧为常量 ``"1"``，「群畜穿行（herd）」
   变体也被压成 1 头（契约 v1.2 起改为 ``round(n_animals)`` 且 herd force 3 头）。

库内 2490 条存量场景不会重新生成，因此**下发时**对 config 做两类修复（新场景在生成期
即正确，不经过这里；运行期 ``param_overrides`` 先于本修复合并，直接覆盖的数值不受影响）：

- **参数值回填**：``actors[].behavior.parameters`` / ``events[].trigger.parameters`` /
  ``events[].action.parameters`` 里值仍是参数名的字符串 → 按「语义均值 ∪ 变体 force」
  重建参数向量后回填数值。生成期未存储采样向量，这是最接近的可重建口径：被变体 force
  的参数是精确值（``approximate=False``），仅均值兜底的参数是近似值（``approximate=True``）。
- **角色实例补齐**：变体 force 提升了 count 表达式引用的参数、且按重建向量求得的实例数
  **同时高于**存量实例数与均值口径实例数时（即「契约把该子场景从单实例改为多实例」的明确
  信号——防止把「采样值恰好少」的合法单实例场景错误加演员），按语义表达式补齐缺失实例：
  出生点以存量首个实例为锚、按表达式相对间距铺开，**不整体重算存量位姿**。

两类修复都逐条返回记录（调用方写结构化日志），不做静默改写；无法重建的（无语义契约 /
无 ``sub:`` 标签 / 值不在参数向量内）保持原样并交由仿真端 ``unresolved_param`` 留痕。
"""
from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from hunter_common.scene_taxonomy import RoleSemantics, SceneSemantics, VariantSemantics

from app.scene_engine.ir import expression_names
from app.scene_engine.semantics import (
    MAX_ACTORS,
    MAX_ROLE_ACTORS,
    SemanticsError,
    actor_entry_for,
    evaluate,
    group_actors,
    resolve_parameters,
    semantics_for,
)

#: 场景 tags 里的子场景标记前缀（生成期 ``sub:<variant.key>``，见 batch_add_scenes.py）
SUB_TAG_PREFIX = "sub:"

#: 角度归一到 [-π, π]（与 semantics._wrap_angle 同口径）
_TWO_PI = 2.0 * math.pi
_PI = math.pi


def recover_variant(
    semantics: SceneSemantics, tags: Sequence[str] | None
) -> VariantSemantics | None:
    """从场景 tags 的 ``sub:<key>`` 恢复子场景声明（无标记/未命中返回 None）。"""
    keys = {
        str(tag)[len(SUB_TAG_PREFIX):]
        for tag in (tags or ())
        if str(tag).startswith(SUB_TAG_PREFIX)
    }
    for variant in semantics.variants:
        if variant.key in keys:
            return variant
    return None


@dataclass(frozen=True, slots=True)
class ParamRepair:
    """单处参数回填记录（``approximate`` 标记均值兜底的近似值）。"""

    where: str
    name: str
    raw: str
    value: float
    approximate: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "where": self.where,
            "name": self.name,
            "raw": self.raw,
            "value": self.value,
            "approximate": self.approximate,
        }


@dataclass(frozen=True, slots=True)
class ActorRepair:
    """单角色实例补齐记录。"""

    role: str
    actor_ids: tuple[str, ...]
    before: int
    after: int
    anchored_to: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "role": self.role,
            "actor_ids": list(self.actor_ids),
            "before": self.before,
            "after": self.after,
            "anchored_to": self.anchored_to,
        }


@dataclass(slots=True)
class RepairReport:
    """下发期修复结论（空报告 = 无需修复或无法修复）。"""

    param_repairs: list[ParamRepair] = field(default_factory=list)
    actor_repairs: list[ActorRepair] = field(default_factory=list)

    @property
    def empty(self) -> bool:
        return not self.param_repairs and not self.actor_repairs


def repair_dispatch_config(
    config: dict[str, Any], scene_type: str, tags: Sequence[str] | None
) -> RepairReport:
    """就地修复下发载荷（``config`` 应为下发副本，不回写库），返回逐条修复记录。

    无语义契约的类型（``custom``/实车回放等）原样返回——这类场景没有可重建的参数向量，
    硬修只会引入新的事实源。
    """
    semantics = semantics_for(scene_type)
    report = RepairReport()
    if semantics is None:
        return report
    variant = recover_variant(semantics, tags)
    vector = resolve_parameters(semantics, None, variant)
    _repair_param_strings(config, vector, set(variant.force) if variant else frozenset(), report)
    _repair_role_counts(config, semantics, vector, variant, report)
    return report


def _repair_param_strings(
    config: dict[str, Any],
    vector: Mapping[str, float],
    forced: frozenset[str],
    report: RepairReport,
) -> None:
    """把仍是参数名字符串的参数值回填为重建向量数值（仅裸名，表达式/字面量不动）。"""

    def resolve_map(parameters: dict[str, Any], where: str) -> None:
        for name, value in parameters.items():
            if not isinstance(value, str):
                continue
            text = value.strip()
            if text not in vector:
                continue
            resolved = round(float(vector[text]), 3)
            parameters[name] = resolved
            report.param_repairs.append(
                ParamRepair(
                    where=f"{where}.{name}",
                    name=name,
                    raw=text,
                    value=resolved,
                    approximate=text not in forced,
                )
            )

    for index, actor in enumerate(list(config.get("actors") or [])):
        behavior = actor.get("behavior") or {}
        parameters = behavior.get("parameters")
        if isinstance(parameters, dict):
            resolve_map(parameters, f"actors[{actor.get('actor_id', index)}].parameters")

    for index, event in enumerate(list(config.get("events") or [])):
        event_id = event.get("event_id") or f"event_{index}"
        trigger = event.get("trigger") or {}
        if isinstance(trigger.get("parameters"), dict):
            resolve_map(trigger["parameters"], f"events[{event_id}].trigger.parameters")
        action = event.get("action") or {}
        if isinstance(action.get("parameters"), dict):
            resolve_map(action["parameters"], f"events[{event_id}].action.parameters")


def _count_of(role: RoleSemantics, variables: Mapping[str, float]) -> int:
    """求角色 count 表达式（求值失败按 0——该角色不参与补齐判定）。"""
    try:
        return max(0, min(MAX_ROLE_ACTORS, round(evaluate(role.count, variables))))
    except SemanticsError:
        return 0


def _wrap_angle(value: float) -> float:
    wrapped = math.remainder(value, _TWO_PI)
    return max(-_PI, min(_PI, wrapped))


def _anchored_entry(
    role: RoleSemantics,
    index: int,
    variables: Mapping[str, float],
    anchor: Mapping[str, Any] | None,
    anchor_base: Mapping[str, Any],
) -> dict[str, Any]:
    """按语义装配第 ``index`` 个实例，并把出生点平移到「存量首实例」的采样基准上。

    语义表达式用的是重建参数向量（均值 ∪ force），而存量首实例的出生点来自生成期采样；
    两者对 ``i=0`` 的差值即采样偏移，把它加回补齐实例，等价于「以存量为锚、按表达式
    相对间距铺开」，不会把整场参与者重算到均值位姿。
    """
    entry = actor_entry_for(role, index, variables)
    spawn = entry["spawn_point"]
    if anchor is None:
        return entry
    for key in ("x", "y", "z"):
        if key in anchor and key in anchor_base:
            spawn[key] = round(spawn[key] + (float(anchor[key]) - float(anchor_base[key])), 2)
    if "yaw" in anchor and "yaw" in anchor_base:
        delta = _wrap_angle(float(anchor["yaw"]) - float(anchor_base["yaw"]))
        spawn["yaw"] = round(_wrap_angle(float(spawn["yaw"]) + delta), 4)
    return entry


def _repair_role_counts(
    config: dict[str, Any],
    semantics: SceneSemantics,
    vector: Mapping[str, float],
    variant: VariantSemantics | None,
    report: RepairReport,
) -> None:
    """补齐「变体 force 明确提升实例数」的角色（仅追加，绝不删减存量实例）。"""
    actors = list(config.get("actors") or [])
    if variant is None or not variant.force or len(actors) >= MAX_ACTORS:
        return
    mean_vector = resolve_parameters(semantics, None, None)
    groups, _unmatched = group_actors(semantics, actors)
    changed = False
    for role in semantics.roles:
        existing = groups.get(role.role) or []
        # 闸门 1：count 表达式引用的参数必须被该变体 force（提升须归因于变体，而非均值抖动）
        if not (expression_names(role.count) & set(variant.force)):
            continue
        expected = _count_of(role, vector)
        # 闸门 2：确实缺实例（多于存量）
        if expected <= len(existing):
            continue
        # 闸门 3：变体确实「提升」了实例数（高于均值口径；否则视为采样差异，不动）
        if expected <= _count_of(role, mean_vector):
            continue
        anchor = existing[0] if existing else None
        anchor_spawn = actor_entry_for(role, 0, {**vector, "i": 0.0})["spawn_point"]
        appended: list[str] = []
        for index in range(len(existing), expected):
            if len(actors) >= MAX_ACTORS:
                break
            entry = _anchored_entry(
                role,
                index,
                {**vector, "i": float(index)},
                (anchor or {}).get("spawn_point") if anchor else None,
                anchor_spawn,
            )
            actors.append(entry)
            appended.append(str(entry["actor_id"]))
        if appended:
            report.actor_repairs.append(
                ActorRepair(
                    role=role.role,
                    actor_ids=tuple(appended),
                    before=len(existing),
                    after=len(existing) + len(appended),
                    anchored_to=str(anchor.get("actor_id")) if anchor else None,
                )
            )
            changed = True
    if changed:
        config["actors"] = actors
