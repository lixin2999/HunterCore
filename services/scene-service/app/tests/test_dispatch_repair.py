"""存量场景下发期修复回归（V1.27.0：裸参数名解析 + 群畜实例补齐 + 动作语义声明）。

对应 HunterSim 现场验证遗留的三件事：

1. **crossing_speed 下发变量名未解析**——生成期 ``resolve_value`` 现在解析裸参数名
   （方案文档改动 A），新场景不再落库字符串；
2. **3 子场景仅映射 1 actor**——契约 v1.2 起 ``animal_crossing.herd`` 按 ``n_animals``
   生成 3 头；存量 herd 场景在下发期补齐（锚定存量采样位姿、按语义间距铺开）；
3. **ev-1:brake 动作不驱动行为**——下发载荷显式声明 ``action_semantics=expectation_only``
   （事件动作是评估基线，不是指令；仿真端不执行，方案文档 §7 F）。

三类修复都**不静默**：下发期修复逐条返回记录并写结构化日志，无法重建的（无契约 /
无 ``sub:`` 标签 / 值不在参数向量内）保持原样交仿真端 ``unresolved_param`` 留痕。
"""
from __future__ import annotations

import copy
from typing import Any

import pytest
from httpx import AsyncClient
from hunter_common.database.enums import SceneStatus

from app.scene_engine.dispatch_repair import (
    recover_variant,
    repair_dispatch_config,
)
from app.scene_engine.semantics import (
    build_semantic_config,
    check_scene_semantics,
    require_semantics,
    resolve_value,
)
from app.tests.conftest import ADMIN_HEADERS
from app.tests.fakes import config_payload, make_scene_model

SCENE_BASE = "/api/v1/scene"
ANIMAL = "animal_crossing"


def _variant(key: str) -> Any:
    return next(v for v in require_semantics(ANIMAL).variants if v.key == key)


# =====================================================================
# 一、生成期：裸参数名解析 + herd 多实例（新场景不再产生遗留形态）
# =====================================================================


def test_resolve_value_resolves_bare_param_name() -> None:
    """裸参数名 → 参数数值；字面量（left/longitudinal/dog）必须保持字符串。"""
    variables = {"animal_speed": 5.0, "animal_kind": 2.0}
    assert resolve_value("animal_speed", variables) == 5.0
    assert resolve_value("left", variables) == "left"
    assert resolve_value("longitudinal", variables) == "longitudinal"
    assert resolve_value("dog", variables) == "dog"


def test_herd_variant_builds_three_animals_spread_by_index() -> None:
    """契约 v1.2：herd 按 n_animals=3 生成多头、出生点按 i 铺开、crossing_speed 数值化。"""
    config = build_semantic_config(
        require_semantics(ANIMAL),
        {"ego_speed": 10.0, "animal_x": 30.0, "animal_speed": 3.0,
         "animal_kind": 2.0, "n_animals": 3.0},
        _variant("herd"),
    )
    assert [a["actor_id"] for a in config["actors"]] == ["animal-1", "animal-2", "animal-3"]
    params = config["actors"][0]["behavior"]["parameters"]
    assert params["animal_kind"] == "cattle"
    assert params["crossing_speed"] == 2.0, "crossing_speed 必须解析为数值（herd force 2.0）"
    assert [a["spawn_point"]["x"] for a in config["actors"]] == [30.0, 31.2, 32.4]
    assert [a["spawn_point"]["y"] for a in config["actors"]] == [-4.5, -5.4, -6.3]
    assert check_scene_semantics(ANIMAL, config) == []


def test_single_animal_variants_stay_single() -> None:
    """dog_dash / deer_cross 仍为 1 头（子场景辨识度），crossing_speed 按 force 解析。"""
    dog = build_semantic_config(require_semantics(ANIMAL), {"animal_kind": 0.0}, _variant("dog_dash"))
    assert len(dog["actors"]) == 1
    assert dog["actors"][0]["behavior"]["parameters"]["crossing_speed"] == 5.0
    deer = build_semantic_config(require_semantics(ANIMAL), {"animal_kind": 1.0}, _variant("deer_cross"))
    assert len(deer["actors"]) == 1


# =====================================================================
# 二、下发期修复单元（存量 V1.26.x 形态：字符串参数值 + 单实例 herd）
# =====================================================================


def _legacy_config() -> dict[str, Any]:
    """V1.26.x 生成的 animal_crossing 存量形态：crossing_speed 是未解析的变量名字符串。"""
    payload = config_payload(ANIMAL)
    payload["actors"] = [
        {
            "actor_id": "animal-1",
            "type": "other",
            "spawn_point": {"x": 31.7, "y": -4.5, "z": 0.0, "yaw": 1.5708},
            "behavior": {
                "behavior_type": "sudden_cross",
                "parameters": {"animal_kind": "cattle", "crossing_speed": "animal_speed"},
            },
        }
    ]
    return payload


def test_variant_force_backfill_is_exact() -> None:
    """sub:dog_dash 场景：crossing_speed 回填变体 force 值 5.0（approximate=False）。"""
    config = _legacy_config()
    report = repair_dispatch_config(config, ANIMAL, ["sub:dog_dash"])
    assert [r.to_dict() for r in report.param_repairs] == [
        {
            "where": "actors[animal-1].parameters.crossing_speed",
            "name": "crossing_speed",
            "raw": "animal_speed",
            "value": 5.0,
            "approximate": False,
        }
    ]
    assert config["actors"][0]["behavior"]["parameters"]["crossing_speed"] == 5.0
    assert not report.actor_repairs, "dog_dash 期望 1 头，不得补实例"


def test_mean_fallback_is_marked_approximate() -> None:
    """无 sub: 标签：回填语义均值 3.5 并标记 approximate（采样向量未落库，只能近似）。"""
    config = _legacy_config()
    report = repair_dispatch_config(config, ANIMAL, ["src:synthetic"])
    assert report.param_repairs[0].value == 3.5
    assert report.param_repairs[0].approximate is True
    assert not report.actor_repairs


def test_herd_backfill_appends_anchored_animals() -> None:
    """sub:herd 存量场景：补齐 2 头，锚定存量采样位姿、按语义间距铺开、kind 为 cattle。"""
    config = _legacy_config()
    report = repair_dispatch_config(config, ANIMAL, ["sub:herd"])
    assert [r.to_dict() for r in report.actor_repairs] == [
        {
            "role": "animal",
            "actor_ids": ["animal-2", "animal-3"],
            "before": 1,
            "after": 3,
            "anchored_to": "animal-1",
        }
    ]
    actors = {a["actor_id"]: a for a in config["actors"]}
    # 存量 animal-1 位姿不动（采样值 31.7，不是均值 24）
    assert actors["animal-1"]["spawn_point"]["x"] == 31.7
    # 补齐实例 = 存量锚点 + 语义相对间距（i=1/2 → +1.2/+2.4）
    assert actors["animal-2"]["spawn_point"]["x"] == pytest.approx(32.9)
    assert actors["animal-3"]["spawn_point"]["x"] == pytest.approx(34.1)
    assert actors["animal-2"]["spawn_point"]["y"] == pytest.approx(-5.4)
    assert actors["animal-2"]["behavior"]["parameters"]["animal_kind"] == "cattle"
    assert actors["animal-2"]["behavior"]["parameters"]["crossing_speed"] == 2.0
    assert actors["animal-1"]["behavior"]["parameters"]["crossing_speed"] == 2.0
    # 修复后的载荷必须仍过语义闸门
    assert check_scene_semantics(ANIMAL, config) == []


def test_sampled_deficit_without_force_is_not_inflated() -> None:
    """采样值恰好少实例（pedestrian_crossing/child 无 n_ped force）不得被补齐。"""
    payload = config_payload("pedestrian_crossing")
    payload["actors"] = [
        {
            "actor_id": "crossing_pedestrian-1",
            "type": "pedestrian",
            "spawn_point": {"x": 16.0, "y": -6.0, "z": 0.0, "yaw": 1.5708},
            "behavior": {
                "behavior_type": "cross_road",
                "parameters": {"crossing_speed": "ped_speed"},
            },
        }
    ]
    report = repair_dispatch_config(payload, "pedestrian_crossing", ["sub:child"])
    assert not report.actor_repairs
    assert report.param_repairs[0].value == 2.4, "child force ped_speed=2.4"


def test_custom_and_literals_are_untouched() -> None:
    """无语义契约类型原样返回；字面量/表达式字符串不做回填（只修裸参数名缺陷）。"""
    config = {"actors": [{"behavior": {"parameters": {"crossing_speed": "animal_speed"}}}]}
    assert repair_dispatch_config(copy.deepcopy(config), "custom", ["sub:herd"]).empty

    legacy = _legacy_config()
    legacy["events"][0]["trigger"]["parameters"]["value"] = "animal_x - 6"
    legacy["actors"][0]["behavior"]["parameters"]["rule_like"] = "left"
    report = repair_dispatch_config(legacy, ANIMAL, ["sub:dog_dash"])
    assert legacy["events"][0]["trigger"]["parameters"]["value"] == "animal_x - 6"
    assert legacy["actors"][0]["behavior"]["parameters"]["rule_like"] == "left"
    assert len(report.param_repairs) == 1, "只回填裸参数名"


def test_recover_variant_matches_sub_tag() -> None:
    semantics = require_semantics(ANIMAL)
    assert recover_variant(semantics, ["sub:herd", "risk:L4"]).key == "herd"
    assert recover_variant(semantics, ["risk:L4"]) is None
    assert recover_variant(semantics, None) is None


# =====================================================================
# 三、下发链路集成：修复后的载荷 + 动作语义声明
# =====================================================================


@pytest.mark.asyncio
async def test_run_scene_repairs_legacy_herd_and_declares_semantics(
    client: AsyncClient, scene_env: Any
) -> None:
    """下发存量 herd 场景：载荷里参数已回填、实例已补齐、并带两个语义标记。"""
    model = make_scene_model(
        scene_name="存量群畜场景（V1.26.x 形态）",
        scene_type=ANIMAL,
        status=SceneStatus.PUBLISHED,
        config=_legacy_config(),
        tags=["sub:herd", "risk:L4"],
    )
    scene_env.repository.rows[model.scene_id] = model

    resp = await client.post(f"{SCENE_BASE}/{model.scene_id}/run", headers=ADMIN_HEADERS)
    assert resp.status_code == 200, resp.text

    dispatched = scene_env.carla.created[0]["scene_config"]
    # 项③：事件动作语义显式声明（期望留档，不是指令）
    assert dispatched["action_semantics"] == "expectation_only"
    assert dispatched["spawn_semantics"] == "relative_to_ego"
    # 项②：crossing_speed 不再是变量名字符串
    assert dispatched["actors"][0]["behavior"]["parameters"]["crossing_speed"] == 2.0
    # 项④：herd 补齐到 3 头
    assert [a["actor_id"] for a in dispatched["actors"]] == [
        "animal-1", "animal-2", "animal-3",
    ]
    # 存量场景本体不被改写（库里的 config 仍是 1 头 + 字符串）
    stored = scene_env.repository.rows[model.scene_id].config_json
    assert len(stored["actors"]) == 1
    assert stored["actors"][0]["behavior"]["parameters"]["crossing_speed"] == "animal_speed"


@pytest.mark.asyncio
async def test_run_scene_new_config_is_not_touched(client: AsyncClient, scene_env: Any) -> None:
    """新契约生成的场景：修复为空操作，载荷与生成期一致（只加两个语义标记）。"""
    config = build_semantic_config(
        require_semantics(ANIMAL),
        {"ego_speed": 10.0, "animal_x": 30.0, "animal_speed": 3.0,
         "animal_kind": 2.0, "n_animals": 3.0},
        _variant("herd"),
    )
    payload = config_payload(ANIMAL, **config)
    model = make_scene_model(
        scene_name="新契约群畜场景",
        scene_type=ANIMAL,
        status=SceneStatus.PUBLISHED,
        config=payload,
        tags=["sub:herd"],
    )
    scene_env.repository.rows[model.scene_id] = model

    resp = await client.post(f"{SCENE_BASE}/{model.scene_id}/run", headers=ADMIN_HEADERS)
    assert resp.status_code == 200, resp.text

    dispatched = scene_env.carla.created[0]["scene_config"]
    assert len(dispatched["actors"]) == 3, "已是 3 头，不得再补"
    for dispatched_actor, built_actor in zip(dispatched["actors"], payload["actors"]):
        assert dispatched_actor["actor_id"] == built_actor["actor_id"]
        assert dispatched_actor["spawn_point"]["x"] == built_actor["spawn_point"]["x"]
        assert dispatched_actor["spawn_point"]["y"] == built_actor["spawn_point"]["y"]
        assert dispatched_actor["spawn_point"]["yaw"] == built_actor["spawn_point"]["yaw"]
        assert dispatched_actor["behavior"]["parameters"] == built_actor["behavior"]["parameters"]
