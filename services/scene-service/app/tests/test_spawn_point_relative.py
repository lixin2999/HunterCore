"""出生点职责反转回归（V1.25.0：平台按下发原样场景，有效出生坐标由仿真端替换）。

覆盖三件事，缺一不可：

1. **原点不再被拦**：``map.spawn_point = (0,0,0)`` 的场景必须能正常下发（V1.24.8~V1.24.10
   在这里返 2001；那是把「只有实机 CARLA 才知道答案」的问题硬塞给平台）；
2. **偏移不折算**：``actors[].spawn_point`` 保持**相对自车出生点的偏移**原样下发，
   只打 ``spawn_semantics=relative_to_ego`` 标记——仿真端替换基准后必须把该偏移叠加上去；
3. **闸门不再提实机表**：G3 只做「地图非空 / 生成点数值有限 / 几何围栏 / 白名单」，
   不得再因为「表未校准」在闸门消息里出现出生点提示。

另附源码级钉桩，防止平台侧出生点表被重新引进（provider / 采集脚本 / 配置键 / 批量脚本强约束）。
"""
from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import pytest
from httpx import AsyncClient
from hunter_common.database.enums import SceneStatus

from app.config import settings
from app.scene_engine.catalog import CATALOG_BY_ID
from app.scene_engine.compiler import default_values
from app.scene_engine.ir import compile_parameters
from app.scene_engine.validators import run_gates
from app.tests.conftest import ADMIN_HEADERS
from app.tests.fakes import config_payload, make_scene_model

SCENE_BASE = "/api/v1/scene"
#: 本文件位于 ``services/scene-service/app/tests/``，上溯四级到仓库根
REPO_ROOT = Path(__file__).resolve().parents[4]
_ORIGIN = {"x": 0.0, "y": 0.0, "z": 0.0, "yaw": 0.0}

_IR = CATALOG_BY_ID["LS-BAS-001"]


def _actor_offset() -> dict[str, Any]:
    return {
        "actor_id": "lead",
        "type": "vehicle",
        "spawn_point": {"x": 18.0, "y": 0.0, "z": 0.0, "yaw": 0.0},
        "behavior": {"behavior_type": "lane_follow", "parameters": {"target_speed": 8.0}},
    }


# =====================================================================
# 一、下发链路：原样下发 + 相对语义标记
# =====================================================================


@pytest.mark.asyncio
async def test_origin_spawn_point_dispatches_without_being_blocked(
    client: AsyncClient, scene_env: Any
) -> None:
    """出生点为占位原点：下发必须成功，且载荷里原点保持不被改写（有效性交由仿真端处理）。"""
    model = make_scene_model(
        scene_name="原点出生点场景",
        status=SceneStatus.PUBLISHED,
        config=config_payload(
            map={"map_id": "Town01", "map_type": "carla_town", "spawn_point": dict(_ORIGIN)}
        ),
    )
    scene_env.repository.rows[model.scene_id] = model

    resp = await client.post(f"{SCENE_BASE}/{model.scene_id}/run", headers=ADMIN_HEADERS)
    assert resp.status_code == 200, resp.text

    dispatched = scene_env.carla.created[0]["scene_config"]
    assert dispatched["map"]["spawn_point"]["x"] == 0.0
    assert dispatched["map"]["spawn_point"]["y"] == 0.0


@pytest.mark.asyncio
async def test_actor_offsets_stay_relative_and_are_marked(
    client: AsyncClient, scene_env: Any
) -> None:
    """参与者偏移不得被折算成绝对坐标：值原样下发，语义靠 ``spawn_semantics`` 声明。"""
    offset = _actor_offset()
    model = make_scene_model(
        scene_name="相对偏移下发场景",
        status=SceneStatus.PUBLISHED,
        config=config_payload(actors=[offset]),
    )
    scene_env.repository.rows[model.scene_id] = model

    resp = await client.post(f"{SCENE_BASE}/{model.scene_id}/run", headers=ADMIN_HEADERS)
    assert resp.status_code == 200, resp.text

    dispatched = scene_env.carla.created[0]["scene_config"]
    assert dispatched["spawn_semantics"] == "relative_to_ego"
    base = dispatched["map"]["spawn_point"]
    actor = dispatched["actors"][0]["spawn_point"]
    # 折算的话这里会变成 base+18=138.5；相对语义下必须仍是 18.0
    assert actor["x"] == pytest.approx(18.0)
    assert base["x"] == pytest.approx(120.5)


# =====================================================================
# 二、校验门 G3：不再判定出生点有效性
# =====================================================================


def _gates(*, mutate: Any = None, **kwargs: Any) -> Any:
    values = dict(default_values(_IR))
    config = compile_parameters(_IR, values)
    if mutate is not None:
        mutate(config)
    return run_gates(
        _IR, values, config, max_duration=settings.scene_duration_max_seconds, **kwargs
    )


def _gate(report: Any, name: str) -> Any:
    return next(item for item in report.gates if item.gate == name)


def test_g3_accepts_origin_spawn_point_without_table_hint() -> None:
    def to_origin(config: dict[str, Any]) -> None:
        config["map"]["spawn_point"] = dict(_ORIGIN)

    g3 = _gate(_gates(mutate=to_origin), "G3")
    assert g3.passed is True, g3.message
    assert "出生点" not in g3.message, "G3 不应再提出生点表/校准状态"
    assert "未校准" not in g3.message


def test_g3_still_rejects_non_finite_spawn_point() -> None:
    def break_point(config: dict[str, Any]) -> None:
        config["map"]["spawn_point"]["x"] = math.nan

    assert _gate(_gates(mutate=break_point), "G3").passed is False


def test_g3_fence_treats_actor_offset_as_relative() -> None:
    """围栏仍按 base + offset 判定参与者落点——这正是相对语义的体现。"""

    def far_actor(config: dict[str, Any]) -> None:
        config["map"]["spawn_point"] = {"x": 50.0, "y": 50.0, "z": 0.0, "yaw": 0.0}
        config["actors"] = [
            {"actor_id": "lead", "type": "vehicle",
             "spawn_point": {"x": 400.0, "y": 0.0, "z": 0.0, "yaw": 0.0},
             "behavior": {"behavior_type": "lane_follow", "parameters": {}}}
        ]

    g3 = _gate(_gates(mutate=far_actor, bounds=(0.0, 0.0, 300.0, 300.0)), "G3")
    assert g3.passed is False
    assert "超出地图围栏" in g3.message


# =====================================================================
# 三、源码级钉桩：平台侧出生点表不得复活
# =====================================================================


def test_spawn_point_provider_and_artifacts_are_removed() -> None:
    assert not (REPO_ROOT / "services/scene-service/app/scene_engine/spawn_points.py").exists()
    assert not (REPO_ROOT / "services/scene-service/config/carla_spawn_points.json").exists()
    assert not (REPO_ROOT / "scripts/collect_carla_spawn_points.py").exists()
    assert not hasattr(settings, "scene_carla_spawn_points_path")


def test_engine_modules_do_not_reference_spawn_table() -> None:
    for relative in (
        "services/scene-service/app/scene_engine/validators.py",
        "services/scene-service/app/scene_engine/generator.py",
        "services/scene-service/app/scene_engine/compiler.py",
        "services/scene-service/app/services/simulation.py",
    ):
        source = (REPO_ROOT / relative).read_text(encoding="utf-8")
        assert "spawn_table" not in source, f"{relative} 仍带着平台侧出生点表参数"
        assert "to_absolute_spawn_offsets" not in source, f"{relative} 仍在做相对→绝对折算"


def test_batch_script_keeps_origin_placeholder() -> None:
    """批量脚本回到「写占位原点」，且不得因原点报错或要求出生点表。"""
    source = (REPO_ROOT / "scripts/batch_add_scenes.py").read_text(encoding="utf-8")
    assert '_spawn(0.0, 0.0, 0.0, lane_id="lane-1")' in source
    assert "app.scene_engine.spawn_points" not in source
    assert "check_spawn_points" not in source
