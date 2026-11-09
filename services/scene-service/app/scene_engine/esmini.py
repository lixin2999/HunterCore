"""G5 干跑：把 ``SceneConfig`` 渲染为最小 OpenSCENARIO 并调用 esmini。

设计约束（§2.7 G5 / §10.2 P1）：
- 干跑用于验证场景能被仿真器加载并推进到结束，不校验判据通过与否；
- esmini 可执行文件由 ``SCENE_ESMINI_BIN`` 配置（支持命令前缀，如
  ``"esmini --window 0 --headless"``）；未部署时返回 ``skipped``，
  **不计入失败**，由校验报告显式标注，避免 CI 把"缺工具"误判为场景缺陷；
- 每次干跑在独立临时目录生成 ``scenario.xosc`` + 直线 ``road.xodr``，
  超时/崩溃返回失败，输出截断防日志膨胀。
"""
from __future__ import annotations

import shlex
import subprocess
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape, quoteattr

from app.scene_engine.carla_execution import BEHAVIOR_MAP, action_lines, condition_lines

#: esmini 输出保留的最大字符数（防止日志/接口响应膨胀）
OUTPUT_LIMIT = 600

#: 最小直线道路（OpenDRIVE），保证 xosc 自包含可加载
_ROAD_XODR = """<?xml version="1.0" standalone="yes"?>
<OpenDRIVE>
  <header revMajor="1" revMinor="4" name="scene-g5" version="1" date="{date}"
          north="0" south="0" east="0" west="0">
    <geoReference><![CDATA[+proj=tmerc +lat_0=0 +lon_0=0 +k=1 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs]]></geoReference>
  </header>
  <road name="road0" length="1000" id="0" junction="-1">
    <planView>
      <geometry s="0" x="0" y="0" hdg="0" length="1000"><line/></geometry>
    </planView>
    <elevationProfile><elevation s="0" a="0" b="0" c="0" d="0"/></elevationProfile>
    <lateralProfile/>
    <lanes>
      <laneSection s="0">
        <left><lane id="1" type="driving" level="false"><width sOffset="0" a="3.5" b="0" c="0" d="0"/></lane></left>
        <center><lane id="0" type="none" level="false"/></center>
        <right><lane id="-1" type="driving" level="false"><width sOffset="0" a="3.5" b="0" c="0" d="0"/></lane></right>
      </laneSection>
    </lanes>
  </road>
</OpenDRIVE>
"""


@dataclass(frozen=True, slots=True)
class EsminiResult:
    """单次干跑结论（``skipped`` 表示环境未部署，不参与成败判定）。"""

    skipped: bool
    passed: bool
    message: str


def _number(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _vehicle_entity(name: str, pedestrian: bool, controller: str | None = None) -> str:
    controller_xml = (
        f"\n        <Controller name={quoteattr(controller)}/>" if controller else ""
    )
    if pedestrian:
        return (
            f'<Pedestrian name={quoteattr(name)} model="walker" mass="60" '
            'pedestrianCategory="pedestrian">\n'
            '        <BoundingBox><Center x="0" y="0" z="0"/>'
            '<Dimensions width="0.5" length="0.4" height="1.7"/></BoundingBox>\n'
            "        <Properties/>"
            f"{controller_xml}\n"
            "      </Pedestrian>"
        )
    return (
        f'<Vehicle name={quoteattr(name)} vehicleCategory="car">\n'
        '        <BoundingBox><Center x="0" y="0" z="0"/>'
        '<Dimensions width="0.7" length="1.2" height="1.0"/></BoundingBox>\n'
        "        <Axles>\n"
        '          <FrontAxle maxSteering="0.384" wheelDiameter="0.3" trackWidth="0.6" '
        'positionX="0.23" positionZ="0.15"/>\n'
        '          <RearAxle maxSteering="0" wheelDiameter="0.3" trackWidth="0.6" '
        'positionX="-0.23" positionZ="0.15"/>\n'
        "        </Axles>\n"
        "        <Properties/>"
        f"{controller_xml}\n"
        "      </Vehicle>"
    )


def _world_position(x: float, y: float, z: float, yaw: float) -> str:
    return (
        "<WorldPosition "
        f'x="{x:.3f}" y="{y:.3f}" z="{z:.3f}" h="{yaw:.3f}" '
        'p="0.000" r="0.000"/>'
    )


def _spawn(config: Mapping[str, Any], section: Mapping[str, Any]) -> tuple[float, float, float, float]:
    map_section = config.get("map") or {}
    base = map_section.get("spawn_point") or {}
    spawn = section.get("spawn_point") or {}
    return (
        _number(base.get("x", 0.0)) + _number(spawn.get("x", 0.0)),
        _number(base.get("y", 0.0)) + _number(spawn.get("y", 0.0)),
        _number(base.get("z", 0.0)) + _number(spawn.get("z", 0.0)),
        _number(base.get("yaw", 0.0)) + _number(spawn.get("yaw", 0.0)),
    )


def _weather_block(weather: Mapping[str, Any], pad: str) -> list[str]:
    """天气 → OpenSCENARIO ``Environment``（雨/雾/云/日照直接映射，湿度与风写入注释）。"""
    if not weather:
        return []
    cloudiness = _number(weather.get("cloudiness"))
    rain = _number(weather.get("rain"))
    fog = _number(weather.get("fog"))
    azimuth = _number(weather.get("sun_azimuth"))
    altitude = _number(weather.get("sun_altitude"))
    wetness = _number(weather.get("wetness"))
    wind = _number(weather.get("wind"))
    return [
        f"{pad}<GlobalAction>",
        f"{pad}  <EnvironmentAction>",
        f"{pad}    <Environment name=\"hunter-weather\">",
        f'{pad}      <TimeOfDay dateTime="2026-01-01T12:00:00" animation="false"/>',
        (
            f'{pad}      <Weather cloudiness="{cloudiness:.1f}" rainfall="{rain:.1f}" '
            f'fog="{fog:.1f}" snowfall="0.0" sunAzimuth="{azimuth:.1f}" '
            f'sunElevation="{altitude:.1f}" temperature="20.0"/>'
        ),
        f"{pad}      <!-- x-hunter-weather wetness={wetness:.1f} wind={wind:.1f} -->",
        f"{pad}    </Environment>",
        f"{pad}  </EnvironmentAction>",
        f"{pad}</GlobalAction>",
    ]


def _initial_speed(actor: Mapping[str, Any]) -> float | None:
    """参与者初始速度（取行为参数中的速度键；无则不出 SpeedAction）。"""
    parameters = (actor.get("behavior") or {}).get("parameters") or {}
    for key in ("target_speed", "speed", "initial_speed", "crossing_speed"):
        if parameters.get(key) is not None:
            return max(_number(parameters[key]), 0.0)
    return None


def _event_block(
    event: Mapping[str, Any],
    *,
    index: int,
    ego_name: str,
    actor_refs: list[str],
    pad: str,
) -> list[str]:
    """单个事件：真实 ``StartTrigger`` + 映射后的 Action（未映射保留扩展注释）。"""
    event_id = str(event.get("event_id") or f"event_{index}")
    trigger = event.get("trigger") or {}
    action = event.get("action") or {}
    lines = [f'{pad}<Event name={quoteattr(event_id)} priority="overwrite">']
    lines.append(
        f"{pad}  <Action name={quoteattr(str(action.get('action_type') or 'action'))}>"
    )
    lines.append(f"{pad}    <PrivateAction>")
    action_body = action_lines(str(action.get("action_type") or ""), action.get("parameters") or {})
    lines.extend(f"{pad}      {line}" for line in action_body)
    lines.append(f"{pad}    </PrivateAction>")
    lines.append(f"{pad}  </Action>")
    lines.append(f"{pad}  <StartTrigger>")
    lines.append(f"{pad}    <ConditionGroup>")
    condition_body = condition_lines(
        condition_type=trigger.get("condition_type"),
        parameters=trigger.get("parameters") or {},
        name=f"{event_id}-trigger",
        ego_ref=ego_name,
        actor_refs=actor_refs,
    )
    lines.extend(f"{pad}      {line}" for line in condition_body)
    lines.append(f"{pad}    </ConditionGroup>")
    lines.append(f"{pad}  </StartTrigger>")
    lines.append(f"{pad}</Event>")
    return lines


def build_xosc(config: Mapping[str, Any], *, road_file: str = "road.xodr") -> str:
    """SceneConfig → OpenSCENARIO 1.2（初始位姿/速度/天气 + 事件触发与动作）。"""
    ego = config.get("ego_vehicle") or {}
    actors = list(config.get("actors") or [])
    events = list(config.get("events") or [])
    weather = config.get("weather") or {}
    duration = max(_number(config.get("duration"), 1.0), 0.1)

    entities: list[str] = []
    init_actions: list[str] = []
    ego_name = str(ego.get("actor_id") or "ego")
    ego_speed = max(_number(ego.get("initial_speed"), 0.0), 0.0)
    actor_refs: list[str] = []

    entities.append(_vehicle_entity(ego_name, False))
    ex, ey, ez, eyaw = _spawn(config, {})
    init_actions.append(
        f'<Private entityRef={quoteattr(ego_name)}>\n'
        "          <PrivateAction><TeleportAction>"
        f"{_world_position(ex, ey, ez, eyaw)}</TeleportAction></PrivateAction>\n"
        "        </Private>"
    )
    init_actions.append(
        f'<Private entityRef={quoteattr(ego_name)}>\n'
        "          <PrivateAction><LongitudinalAction><SpeedAction>\n"
        '            <SpeedActionDynamics dynamicsShape="step" value="0" '
        'dynamicsDimension="time"/>\n'
        "            <SpeedActionTarget>"
        f'<AbsoluteTargetSpeed value="{ego_speed:.3f}"/>'
        "</SpeedActionTarget>\n"
        "          </SpeedAction></LongitudinalAction></PrivateAction>\n"
        "        </Private>"
    )
    for index, actor in enumerate(actors):
        name = str(actor.get("actor_id") or f"actor_{index}")
        actor_type = str(actor.get("type") or actor.get("actor_type") or "").lower()
        pedestrian = "pedestrian" in actor_type or "vru" in actor_type
        behavior = actor.get("behavior") or {}
        behavior_type = str(behavior.get("behavior_type") or "")
        mapping = BEHAVIOR_MAP.get(behavior_type)
        controller = f"hunter-{behavior_type}-controller" if mapping else None
        ax, ay, az, ayaw = _spawn(config, actor)
        actor_refs.append(name)
        entities.append(_vehicle_entity(name, pedestrian, controller))
        init_actions.append(
            f'<Private entityRef={quoteattr(name)}>\n'
            "          <PrivateAction><TeleportAction>"
            f"{_world_position(ax, ay, az, ayaw)}</TeleportAction></PrivateAction>\n"
            "        </Private>"
        )
        speed = _initial_speed(actor)
        if speed is not None and not pedestrian:
            init_actions.append(
                f'<Private entityRef={quoteattr(name)}>\n'
                "          <PrivateAction><LongitudinalAction><SpeedAction>\n"
                '            <SpeedActionDynamics dynamicsShape="step" value="0" '
                'dynamicsDimension="time"/>\n'
                "            <SpeedActionTarget>"
                f'<AbsoluteTargetSpeed value="{speed:.3f}"/>'
                "</SpeedActionTarget>\n"
                "          </SpeedAction></LongitudinalAction></PrivateAction>\n"
                "        </Private>"
            )

    entity_xml = "\n      ".join(entities)
    init_xml = "\n        ".join(init_actions)
    init_weather = "\n        ".join(_weather_block(weather, "        "))
    event_xml = "\n".join(
        line
        for index, event in enumerate(events)
        for line in _event_block(
            event, index=index, ego_name=ego_name, actor_refs=actor_refs, pad="            "
        )
    )
    if not events:
        # 无事件时保留一个「保持初速」事件，保证 Maneuver 非空且干跑可推进
        event_xml = "\n".join(
            [
                '            <Event name="hold" priority="overwrite">',
                '              <Action name="speed">',
                "                <PrivateAction>",
                "                  <LongitudinalAction>",
                "                    <SpeedAction>",
                (
                    '                      <SpeedActionDynamics dynamicsShape="step" value="0" '
                    'dynamicsDimension="time"/>'
                ),
                "                      <SpeedActionTarget>",
                f'                        <AbsoluteTargetSpeed value="{ego_speed:.3f}"/>',
                "                      </SpeedActionTarget>",
                "                    </SpeedAction>",
                "                  </LongitudinalAction>",
                "                </PrivateAction>",
                "              </Action>",
                "              <StartTrigger>",
                "                <ConditionGroup>",
                '                  <Condition name="hold-start" delay="0" conditionEdge="none">',
                "                    <ByValueCondition>",
                '                      <SimulationTimeCondition value="0" rule="greaterThan"/>',
                "                    </ByValueCondition>",
                "                  </Condition>",
                "                </ConditionGroup>",
                "              </StartTrigger>",
                "            </Event>",
            ]
        )
    actor_ref_xml = "\n            ".join(
        f'<EntityRef entityRef={quoteattr(ref)}/>' for ref in [ego_name, *actor_refs]
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<OpenSCENARIO>
  <FileHeader revMajor="1" revMinor="2" date="{datetime.now(UTC).date().isoformat()}"
              description="HunterCore G5 dry run" author="scene-service"/>
  <ParameterDeclarations/>
  <CatalogLocations/>
  <RoadNetwork>
    <LogicFile filepath={quoteattr(road_file)}/>
  </RoadNetwork>
  <Entities>
      {entity_xml}
  </Entities>
  <Storyboard>
    <Init>
      <Actions>
        {init_xml}
        {init_weather}
      </Actions>
    </Init>
    <Story name="g5">
      <Act name="act">
        <ManeuverGroup maximumExecutionCount="1" name="group">
          <Actors selectTriggeringEntities="false">
            {actor_ref_xml}
          </Actors>
          <Maneuver name="maneuver">
{event_xml}
          </Maneuver>
        </ManeuverGroup>
        <StartTrigger>
          <ConditionGroup>
            <Condition name="start" delay="0" conditionEdge="none">
              <ByValueCondition>
                <SimulationTimeCondition value="0" rule="greaterThan"/>
              </ByValueCondition>
            </Condition>
          </ConditionGroup>
        </StartTrigger>
        <StopTrigger>
          <ConditionGroup>
            <Condition name="stop" delay="0" conditionEdge="none">
              <ByValueCondition>
                <SimulationTimeCondition value="{duration:.3f}" rule="greaterThan"/>
              </ByValueCondition>
            </Condition>
          </ConditionGroup>
        </StopTrigger>
      </Act>
    </Story>
  </Storyboard>
</OpenSCENARIO>
"""


def _command_parts(command: str) -> list[str]:
    parts = shlex.split(command, posix=False)
    return [part.strip('"') for part in parts if part.strip('"')]


class EsminiRunner:
    """esmini 干跑执行器（命令前缀 + 超时；未配置即 skipped）。"""

    def __init__(self, command: str, *, timeout_seconds: float = 10.0) -> None:
        self._command = command.strip()
        self._timeout = max(float(timeout_seconds), 0.1)
        self._parts = _command_parts(self._command) if self._command else []

    @property
    def available(self) -> bool:
        return bool(self._parts) and Path(self._parts[0]).exists()

    def run(self, config: Mapping[str, Any]) -> EsminiResult:
        if not self._parts:
            return EsminiResult(True, True, "esmini 未配置（SCENE_ESMINI_BIN 为空），G5 跳过实机干跑")
        if not Path(self._parts[0]).exists():
            return EsminiResult(
                True, True, f"esmini 可执行文件不存在（{self._parts[0]}），G5 跳过实机干跑"
            )
        with tempfile.TemporaryDirectory(prefix="scene-g5-") as workspace:
            root = Path(workspace)
            (root / "road.xodr").write_text(
                _ROAD_XODR.format(date=datetime.now(UTC).date().isoformat()), encoding="utf-8"
            )
            xosc_path = root / "scenario.xosc"
            xosc_path.write_text(build_xosc(config), encoding="utf-8")
            try:
                completed = subprocess.run(
                    [*self._parts, str(xosc_path)],
                    cwd=workspace,
                    capture_output=True,
                    text=True,
                    timeout=self._timeout,
                    check=False,
                )
            except subprocess.TimeoutExpired:
                return EsminiResult(
                    False, False, f"esmini 干跑超时（>{self._timeout:.0f}s），场景可能不可推进"
                )
            except OSError as exc:
                return EsminiResult(False, False, f"esmini 启动失败: {exc}")
            output = (completed.stderr or completed.stdout or "").strip()
            if len(output) > OUTPUT_LIMIT:
                output = output[-OUTPUT_LIMIT:]
            if completed.returncode == 0:
                return EsminiResult(False, True, "esmini 干跑通过")
            return EsminiResult(
                False,
                False,
                f"esmini 干跑失败（exit={completed.returncode}）: {escape(output)}",
            )


__all__ = ["EsminiResult", "EsminiRunner", "build_xosc"]
