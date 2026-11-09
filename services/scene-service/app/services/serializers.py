"""场景导出序列化（4.3 节）：Carla ScenarioRunner XML 与 OpenSCENARIO 1.2。

2026-11 语义化改造：
- ``events[].action.action_type`` / ``trigger.condition_type`` 经
  ``app.scene_engine.carla_execution`` 映射为 OpenSCENARIO 1.2 标准元素；
- 无标准等价物的组合不伪造语义，以 ``x-hunter-*`` 注释保留原始自由结构，
  并在具体场景渲染结果 ``execution`` 中显式报告（见 ``check_executable``）；
- OpenSCENARIO 1.2 产物在根节点写入 ``x-scenario-version``（契约
  x-hunter-export.formats）；批量导出为「单文件集」（多场景合并入同一文件）。
"""
from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any
from xml.sax.saxutils import escape, quoteattr

from app.scene_engine.carla_execution import BEHAVIOR_MAP, action_lines, condition_lines
from app.schemas.scene import (
    EXPORT_EXTENSION_BY_FORMAT,
    Scene,
    SceneConfig,
    SceneExportFormat,
)

#: XML 声明（导出文件统一 UTF-8）
_XML_HEADER = '<?xml version="1.0" encoding="UTF-8"?>'


def file_name_for(scene: Scene, fmt: SceneExportFormat) -> str:
    """按契约命名规则生成文件名：``scene-{scene_id}-{version}.{ext}``。"""
    return f"scene-{scene.scene_id}-{scene.version}{EXPORT_EXTENSION_BY_FORMAT[fmt]}"


def render_export_document(
    scenes: Sequence[Scene],
    fmt: SceneExportFormat,
    *,
    generated_at: datetime | None = None,
) -> bytes:
    """渲染导出文件内容（确定性输出：字段顺序固定、数值统一 6 位有效数字）。"""
    stamp = (generated_at or datetime.now(UTC)).strftime("%Y-%m-%dT%H:%M:%SZ")
    renderer = (
        _render_carla_scenarios
        if fmt is SceneExportFormat.CARLA_SCENARIORUNNER_XML
        else _render_openscenario
    )
    body = renderer(scenes, stamp)
    return ("\n".join([_XML_HEADER, *body]) + "\n").encode("utf-8")


def _num(value: float) -> str:
    """数值序列化（消除浮点噪声，保证导出可复现）。"""
    return f"{round(float(value), 6):g}"


def _params(params: dict[str, object]) -> str:
    """参数结构序列化（JSON → XML 属性；sort_keys 保证确定性）。"""
    return quoteattr(json.dumps(params, ensure_ascii=False, sort_keys=True))


def _spawn_point_line(point: object, indent: str) -> str:
    """spawn_point 元素（位置 m + 朝向 rad）。"""
    lane = getattr(point, "lane_id", None)
    lane_attr = f" lane_id={quoteattr(str(lane))}" if lane else ""
    return (
        f'{indent}<spawn_point x="{_num(point.x)}" y="{_num(point.y)}" z="{_num(point.z)}" '
        f'yaw="{_num(point.yaw)}"{lane_attr}/>'
    )


def _weather_line(config: SceneConfig, indent: str) -> str:
    """weather 元素（7 字段全部输出，契约 4.2.2 不可缺省）。"""
    weather = config.weather
    return (
        f'{indent}<weather cloudiness="{_num(weather.cloudiness)}" rain="{_num(weather.rain)}" '
        f'wetness="{_num(weather.wetness)}" fog="{_num(weather.fog)}" wind="{_num(weather.wind)}" '
        f'sun_azimuth="{_num(weather.sun_azimuth)}" sun_altitude="{_num(weather.sun_altitude)}"/>'
    )


def _success_criteria_line(config: SceneConfig, indent: str) -> str:
    """success_criteria 元素。"""
    criteria = config.success_criteria
    no_collision = "true" if criteria.no_collision else "false"
    return (
        f'{indent}<success_criteria max_speed_deviation="{_num(criteria.max_speed_deviation)}" '
        f'no_collision="{no_collision}" min_safe_distance="{_num(criteria.min_safe_distance)}"/>'
    )


# =====================================================================
# Carla ScenarioRunner XML（.xml）
# =====================================================================
def _render_carla_scenarios(scenes: Sequence[Scene], stamp: str) -> list[str]:
    """渲染 Carla ScenarioRunner 场景文件（scenarios 根 + 逐场景 scenario 子节点）。"""
    lines = [f"<!-- HunterCore scene export generated_at={stamp} -->", "<scenarios>"]
    for scene in scenes:
        lines.extend(_carla_scenario(scene))
    lines.append("</scenarios>")
    return lines


def _carla_scenario(scene: Scene) -> list[str]:
    """单个场景节点（4.2.2 结构逐字段序列化）。"""
    config = scene.config
    lines = [
        (
            f"  <scenario name={quoteattr(scene.scene_name)} "
            f"type={quoteattr(str(scene.scene_type))} version={quoteattr(scene.version)} "
            f"uuid={quoteattr(str(scene.scene_id))}>"
        ),
        (
            f"    <map map_id={quoteattr(config.map.map_id)} "
            f"map_type={quoteattr(config.map.map_type.value)}>"
        ),
        _spawn_point_line(config.map.spawn_point, "      "),
        "    </map>",
        (
            f"    <ego_vehicle model={quoteattr(config.ego_vehicle.model)} "
            f'initial_speed="{_num(config.ego_vehicle.initial_speed)}" '
            f'initial_steer="{_num(config.ego_vehicle.initial_steer)}"/>'
        ),
        _weather_line(config, "    "),
        "    <actors>",
    ]
    for actor in config.actors:
        lines.append(
            f"      <actor actor_id={quoteattr(actor.actor_id)} type={quoteattr(actor.type.value)}>"
        )
        lines.append(_spawn_point_line(actor.spawn_point, "        "))
        lines.append(
            f"        <behavior behavior_type={quoteattr(actor.behavior.behavior_type)} "
            f"parameters={_params(actor.behavior.parameters)}/>"
        )
        lines.append("      </actor>")
    lines.extend(["    </actors>", "    <events>"])
    for event in config.events:
        lines.append(
            f"      <event event_id={quoteattr(event.event_id)} type={quoteattr(event.type.value)}>"
        )
        lines.append(
            f"        <trigger condition={quoteattr(event.trigger.condition)} "
            f"parameters={_params(event.trigger.parameters)}/>"
        )
        lines.append(
            f"        <action action_type={quoteattr(event.action.action_type)} "
            f"parameters={_params(event.action.parameters)}/>"
        )
        lines.append("      </event>")
    lines.extend(
        [
            "    </events>",
            _success_criteria_line(config, "    "),
            f"    <duration>{_num(config.duration)}</duration>",
            "  </scenario>",
        ]
    )
    return lines


# =====================================================================
# OpenSCENARIO 1.2（.xosc）
# =====================================================================
def _render_openscenario(scenes: Sequence[Scene], stamp: str) -> list[str]:
    """渲染 OpenSCENARIO 1.2（单场景为纯 OpenSCENARIO 根；批量导出为单文件集容器）。"""
    if len(scenes) == 1:
        return _osc_scenario(scenes[0], stamp, indent="")
    lines = [
        f"<!-- HunterCore OpenSCENARIO export set generated_at={stamp} -->",
        (
            f"<x-hunter-scenario-set x-scenario-version={quoteattr(scenes[0].version)} "
            f'count="{len(scenes)}">'
        ),
    ]
    for scene in scenes:
        lines.extend(_osc_scenario(scene, stamp, indent="  "))
    lines.append("</x-hunter-scenario-set>")
    return lines


def _osc_scenario(scene: Scene, stamp: str, *, indent: str) -> list[str]:
    """单个 OpenSCENARIO 文档（FileHeader / RoadNetwork / Entities / Storyboard）。"""
    config = scene.config
    pad = indent
    lines = [
        f"{pad}<OpenSCENARIO x-scenario-version={quoteattr(scene.version)}>",
        (
            f"{pad}  <FileHeader revMajor=\"1\" revMinor=\"2\" date={quoteattr(stamp)} "
            f"description={quoteattr(scene.scene_name)} author={quoteattr(str(scene.creator))}/>"
        ),
        f"{pad}  <ParameterDeclarations>",
        (
            f'{pad}    <ParameterDeclaration name="duration" parameterType="double" '
            f'value="{_num(config.duration)}"/>'
        ),
        f"{pad}  </ParameterDeclarations>",
        f"{pad}  <CatalogLocations/>",
        f"{pad}  <RoadNetwork>",
        f"{pad}    <LogicFile filepath={quoteattr(config.map.map_id)}/>",
        f"{pad}  </RoadNetwork>",
        f"{pad}  <Entities>",
    ]
    lines.extend(_osc_ego_entity(config, pad))
    for actor in config.actors:
        lines.extend(_osc_actor_entity(actor, pad))
    lines.append(f"{pad}  </Entities>")
    lines.extend(_osc_storyboard(scene, pad))
    lines.append(f"{pad}</OpenSCENARIO>")
    return lines


def _osc_ego_entity(config: SceneConfig, pad: str) -> list[str]:
    """自车实体（ScenarioObject + Vehicle + Controller）。"""
    point = config.map.spawn_point
    model = config.ego_vehicle.model
    return [
        f'{pad}    <ScenarioObject name="ego">',
        f"{pad}      <Vehicle name={quoteattr(model)} vehicleCategory=\"car\">",
        (
            f'{pad}        <BoundingBox><Center x="0" y="0" z="0"/>'
            f'<Dimensions width="1" length="2" height="1"/></BoundingBox>'
        ),
        f'{pad}        <Performance maxSpeed="0" maxAcceleration="0" maxDeceleration="0"/>',
        (
            f'{pad}        <Axles><FrontAxle maxSteering="0" wheelDiameter="0.5" '
            f'trackWidth="1" positionX="1" positionZ="{_num(point.z)}"/>'
            f'<RearAxle maxSteering="0" wheelDiameter="0.5" trackWidth="1" '
            f'positionX="0" positionZ="{_num(point.z)}"/></Axles>'
        ),
        f"{pad}      </Vehicle>",
        f'{pad}      <Controller name={quoteattr(model.lower() + "-controller")}/>',
        f"{pad}    </ScenarioObject>",
    ]


def _osc_actor_entity(actor: Any, pad: str) -> list[str]:
    """参与者实体（按 type 映射 Vehicle / Pedestrian / MiscObject，附行为控制器）。"""
    category_attr, tag = _osc_entity_tag(str(actor.type.value))
    behavior_type = str(actor.behavior.behavior_type)
    mapping = BEHAVIOR_MAP.get(behavior_type)
    controller = f"hunter-{behavior_type}-controller" if mapping else None
    lines = [
        f"{pad}    <ScenarioObject name={quoteattr(actor.actor_id)}>",
        (
            f"{pad}      <{tag} name={quoteattr(actor.actor_id)} "
            f'{category_attr}="{escape(str(actor.type.value))}">'
        ),
        (
            f'{pad}        <BoundingBox><Center x="0" y="0" z="0"/>'
            f'<Dimensions width="1" length="1" height="2"/></BoundingBox>'
        ),
        f"{pad}      </{tag}>",
    ]
    if controller:
        lines.append(f"{pad}      <Controller name={quoteattr(controller)}/>")
    lines.append(f"{pad}    </ScenarioObject>")
    return lines


def _osc_entity_tag(actor_type: str) -> tuple[str, str]:
    """参与者类型 → OpenSCENARIO 实体标签与分类属性名。"""
    if actor_type == "pedestrian":
        return "pedestrianCategory", "Pedestrian"
    if actor_type == "vehicle":
        return "vehicleCategory", "Vehicle"
    return "miscObjectCategory", "MiscObject"


def _osc_storyboard(scene: Scene, pad: str) -> list[str]:
    """Storyboard：Init 初始位姿/天气 + Story/Act/ManeuverGroup/Event + StopTrigger。

    事件触发与动作经 ``carla_execution`` 映射为 OpenSCENARIO 1.2 标准元素；
    自由结构以 ``x-hunter-*`` 注释保留，便于现场追溯。
    """
    config = scene.config
    point = config.map.spawn_point
    actor_refs = [actor.actor_id for actor in config.actors]
    lines = [
        f"{pad}  <Storyboard>",
        f"{pad}    <Init>",
        f"{pad}      <Actions>",
        *_osc_teleport("ego", point.x, point.y, point.z, point.yaw, pad, "        "),
    ]
    for actor in config.actors:
        spawn = actor.spawn_point
        lines.extend(
            _osc_teleport(
                actor.actor_id, spawn.x, spawn.y, spawn.z, spawn.yaw, pad, "        "
            )
        )
    weather = config.weather
    lines.extend(
        [
            f"{pad}        <GlobalAction>",
            f"{pad}          <EnvironmentAction>",
            f'{pad}            <Environment name="hunter-weather">',
            (
                f'{pad}              <TimeOfDay dateTime="2026-01-01T12:00:00" '
                'animation="false"/>'
            ),
            (
                f'{pad}              <Weather cloudiness="{_num(weather.cloudiness)}" '
                f'rainfall="{_num(weather.rain)}" fog="{_num(weather.fog)}" '
                f'snowfall="0.0" sunAzimuth="{_num(weather.sun_azimuth)}" '
                f'sunElevation="{_num(weather.sun_altitude)}" temperature="20.0"/>'
            ),
            (
                f"{pad}              <!-- x-hunter-weather wetness={_num(weather.wetness)} "
                f'wind={_num(weather.wind)} -->'
            ),
            f"{pad}            </Environment>",
            f"{pad}          </EnvironmentAction>",
            f"{pad}        </GlobalAction>",
            f"{pad}      </Actions>",
            f"{pad}    </Init>",
            f"{pad}    <Story name={quoteattr(scene.scene_name)}>",
            f'{pad}      <Act name="act-1">',
            f'{pad}        <ManeuverGroup name="group-1" maximumExecutionCount="1">',
            f'{pad}          <Actors selectTriggeringEntities="false">',
            f'{pad}            <EntityRef entityRef="ego"/>',
        ]
    )
    for actor in config.actors:
        lines.append(f"{pad}            <EntityRef entityRef={quoteattr(actor.actor_id)}/>")
    lines.extend(
        [
            f"{pad}          </Actors>",
            f'{pad}          <Maneuver name="maneuver-1">',
        ]
    )
    for event in config.events:
        lines.extend(_osc_event(event, pad, actor_refs=actor_refs))
    lines.extend(
        [
            f"{pad}          </Maneuver>",
            f"{pad}        </ManeuverGroup>",
            f"{pad}      </Act>",
            f"{pad}    </Story>",
            f"{pad}    <StopTrigger>",
            f"{pad}      <ConditionGroup>",
            f'{pad}        <Condition name="stop" delay="0" conditionEdge="none">',
            f"{pad}          <ByValueCondition>",
            (
                f'{pad}            <SimulationTimeCondition value="{_num(config.duration)}" '
                'rule="greaterThan"/>'
            ),
            f"{pad}          </ByValueCondition>",
            f"{pad}        </Condition>",
            f"{pad}      </ConditionGroup>",
            f"{pad}    </StopTrigger>",
            f"{pad}  </Storyboard>",
        ]
    )
    return lines


def _osc_teleport(
    entity: str, x: float, y: float, z: float, yaw: float, pad: str, action_pad: str
) -> list[str]:
    """Init 阶段的 TeleportAction（WorldPosition，含朝向 h）。"""
    return [
        f"{pad}{action_pad}<Private entityRef={quoteattr(entity)}>",
        f"{pad}{action_pad}  <PrivateAction>",
        f"{pad}{action_pad}    <TeleportAction>",
        f"{pad}{action_pad}      <Position>",
        (
            f'{pad}{action_pad}        <WorldPosition x="{_num(x)}" y="{_num(y)}" '
            f'z="{_num(z)}" h="{_num(yaw)}"/>'
        ),
        f"{pad}{action_pad}      </Position>",
        f"{pad}{action_pad}    </TeleportAction>",
        f"{pad}{action_pad}  </PrivateAction>",
        f"{pad}{action_pad}</Private>",
    ]


def _osc_event(event: Any, pad: str, *, actor_refs: Sequence[str] = ()) -> list[str]:
    """事件节点：真实 ``StartTrigger`` + 映射后的 Action（自由参数以注释保留）。"""
    event_pad = f"{pad}            "
    body_pad = f"{pad}              "
    lines = [
        f'{event_pad}<Event name={quoteattr(event.event_id)} priority="overwrite">',
        f'{event_pad}  <Action name={quoteattr(event.action.action_type)}>',
        f"{event_pad}    <PrivateAction>",
    ]
    for line in action_lines(event.action.action_type, event.action.parameters):
        lines.append(f"{body_pad}  {line}")
    lines.append(
        f"{body_pad}  <!-- x-hunter-action "
        f"action_type={quoteattr(event.action.action_type)} "
        f"parameters={_params(event.action.parameters)} -->"
    )
    lines.append(f"{event_pad}    </PrivateAction>")
    lines.append(f"{event_pad}  </Action>")
    lines.append(f"{event_pad}  <StartTrigger>")
    lines.append(f"{event_pad}    <ConditionGroup>")
    for line in condition_lines(
        condition_type=event.trigger.condition_type,
        parameters=event.trigger.parameters,
        name=f"{event.event_id}-trigger",
        actor_refs=list(actor_refs),
    ):
        lines.append(f"{body_pad}  {line}")
    lines.append(
        f"{body_pad}  <!-- x-hunter-trigger type={quoteattr(event.type.value)} "
        f"condition={quoteattr(event.trigger.condition)} "
        f"parameters={_params(event.trigger.parameters)} -->"
    )
    lines.append(f"{event_pad}    </ConditionGroup>")
    lines.append(f"{event_pad}  </StartTrigger>")
    lines.append(f"{event_pad}</Event>")
    return lines


__all__ = ["file_name_for", "render_export_document"]