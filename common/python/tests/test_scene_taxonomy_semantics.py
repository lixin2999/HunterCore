"""场景语义契约（``scene_semantics``）完整性测试（V1.25.0 语义化重建）。

契约来源：``contracts/scene-taxonomy.yaml::scene_semantics``（46 类可合成场景）。
本文件只校验**共享字典层**（hunter_common 加载器解析后的只读视图），不依赖任何服务包；
服务侧闸门/G9 行为见 ``services/scene-service/app/tests/test_scene_semantics.py``。

为什么需要这条门禁
------------------
生成器、模板、场景引擎目录、用例派生全部以 ``scene_semantics`` 为单一事实来源：
一个角色写了字典外的 ``behavior_type``、一个事件引用了不存在的 ``condition_type``，
加载期虽会报错，但「语义内容是否可用」的约束（必需角色/变体数量/参数分布自洽）不会自动覆盖，
因此在此把可执行断言固化，避免 46 类语义在后续扩充中静默退化。
"""
from __future__ import annotations

import re

import pytest

from hunter_common.scene_taxonomy import (
    CONSTRAINT_OPS,
    ROLE_TYPE_VALUES,
    WEATHER_FIELDS,
    SceneTaxonomy,
    SceneTaxonomyError,
    get_scene_taxonomy,
)

#: 可合成（有语义契约）的场景类型数
SEMANTIC_TYPES = 46
#: 无合成语义的类型：人工自定义 + 实车回放（不得被生成器/模板合成）
NON_SYNTHETIC = frozenset({"custom", "real_vehicle_replay"})
#: 判据引用格式（与契约 oracle_ref 一致）
ORACLE_REF_RE = re.compile(r"^oracle\.[a-z0-9_.]+/v\d+$")
#: 参数名格式（与加载器校验同口径）
PARAM_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")


@pytest.fixture(scope="module")
def taxonomy() -> SceneTaxonomy:
    """进程内缓存的受控字典（从仓库根定位 ``contracts/scene-taxonomy.yaml``）。"""
    return get_scene_taxonomy()


def test_semantics_cover_all_synthetic_types(taxonomy: SceneTaxonomy) -> None:
    """46 类可合成类型语义齐全，且只有 custom/实车回放没有语义契约。"""
    assert taxonomy.version
    assert len(taxonomy.semantics) == SEMANTIC_TYPES
    assert set(taxonomy.semantics) <= set(taxonomy.scene_types)
    assert set(taxonomy.scene_types) - set(taxonomy.semantics) == NON_SYNTHETIC
    for scene_type, sem in taxonomy.semantics.items():
        assert sem.scene_type == scene_type
        assert sem.objective, scene_type
        assert ORACLE_REF_RE.match(sem.oracle_ref), (scene_type, sem.oracle_ref)
        assert sem.duration.strip() and sem.ego_steer.strip(), scene_type


def test_roles_use_controlled_types_and_behaviors(taxonomy: SceneTaxonomy) -> None:
    """角色类型/行为/命名合法；必需角色至少 1 个实例。"""
    for scene_type, sem in taxonomy.semantics.items():
        names = [role.role for role in sem.roles]
        assert len(names) == len(set(names)), f"{scene_type} 角色名重复: {names}"
        for role in sem.roles:
            assert role.actor_type in ROLE_TYPE_VALUES, (scene_type, role.role)
            assert role.behavior_type in taxonomy.behavior_types, (scene_type, role.role)
            assert role.count.strip(), (scene_type, role.role)
            if role.required:
                assert role.min_count >= 1, (scene_type, role.role)
        # 无角色的类型只能是「无可交互参与者」的最小风险策略
        if not sem.roles:
            assert scene_type == "minimal_risk_manoeuvre", scene_type


def test_events_use_controlled_conditions_and_actions(taxonomy: SceneTaxonomy) -> None:
    """事件类型/条件/动作全部落在受控字典内；声明必需事件就不能为空。"""
    for scene_type, sem in taxonomy.semantics.items():
        for event in sem.events:
            assert event.type in taxonomy.scene_event_types, (scene_type, event.type)
            assert event.condition_type in taxonomy.condition_types, (scene_type, event.type)
            assert event.action_type in taxonomy.action_types, (scene_type, event.type)
        if sem.required_events:
            assert sem.events, f"{scene_type} 声明 required_events 但未给出事件模板"


def test_params_and_constraints_are_self_consistent(taxonomy: SceneTaxonomy) -> None:
    """参数名/分布/可行性约束自洽（截断正态口径：low ≤ mean ≤ high）。"""
    for scene_type, sem in taxonomy.semantics.items():
        assert sem.params, scene_type
        for param in sem.params.values():
            assert PARAM_NAME_RE.match(param.name), (scene_type, param.name)
            assert param.low <= param.mean <= param.high, (scene_type, param.name)
            assert param.std >= 0, (scene_type, param.name)
            assert param.risk in {-1, 0, 1}, (scene_type, param.name)
        for constraint in sem.constraints:
            assert constraint.op in CONSTRAINT_OPS, (scene_type, constraint.op)
            assert constraint.left in sem.params, (scene_type, constraint.left)


def test_variants_are_distinguishable_and_reference_params(taxonomy: SceneTaxonomy) -> None:
    """子场景 ≥ 2 且互不重名；force/overrides 只能引用已声明参数（防场景雷同/悬空覆盖）。"""
    for scene_type, sem in taxonomy.semantics.items():
        keys = [variant.key for variant in sem.variants]
        assert len(keys) >= 2, f"{scene_type} 子场景不足 2 个: {keys}"
        assert len(keys) == len(set(keys)), f"{scene_type} 子场景键重复: {keys}"
        for variant in sem.variants:
            assert variant.label and variant.sub, (scene_type, variant.key)
            assert set(variant.force) <= set(sem.params), (scene_type, variant.key)
            assert set(variant.overrides) <= set(sem.params), (scene_type, variant.key)


def test_weather_expressions_use_contract_fields(taxonomy: SceneTaxonomy) -> None:
    """天气表达式只出现在 7 个契约字段上。"""
    for scene_type, sem in taxonomy.semantics.items():
        assert set(sem.weather) <= WEATHER_FIELDS, (scene_type, sorted(sem.weather))


def test_semantics_for_rejects_missing_contract(taxonomy: SceneTaxonomy) -> None:
    """缺失语义契约时报错而不是静默降级（生成器/模板的启动期断言依赖此行为）。"""
    with pytest.raises(SceneTaxonomyError):
        taxonomy.semantics_for("custom")
