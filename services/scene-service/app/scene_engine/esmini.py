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


def _vehicle_entity(name: str, pedestrian: bool) -> str:
    if pedestrian:
        return (
            f'<Pedestrian name={quoteattr(name)} model="walker" mass="60" '
            'pedestrianCategory="pedestrian">\n'
            '        <BoundingBox dimensions width="0.5" length="0.4" height="1.7"/>\n'
            "        <Properties/>\n"
            "      </Pedestrian>"
        )
    return (
        f'<Vehicle name={quoteattr(name)} vehicleCategory="car">\n'
        '        <BoundingBox dimensions width="0.7" length="1.2" height="1.0"/>\n'
        "        <Axles>\n"
        '          <FrontAxle maxSteering="0.384" wheelDiameter="0.3" trackWidth="0.6" '
        'positionX="0.23" positionZ="0.15"/>\n'
        '          <RearAxle maxSteering="0" wheelDiameter="0.3" trackWidth="0.6" '
        'positionX="-0.23" positionZ="0.15"/>\n'
        "        </Axles>\n"
        "        <Properties/>\n"
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


def build_xosc(config: Mapping[str, Any], *, road_file: str = "road.xodr") -> str:
    """SceneConfig → 最小合法 OpenSCENARIO 1.2（初始位姿 + 自车初速）。"""
    ego = config.get("ego_vehicle") or {}
    actors = list(config.get("actors") or [])
    duration = max(_number(config.get("duration"), 1.0), 0.1)

    entities: list[str] = []
    init_actions: list[str] = []
    ego_name = str(ego.get("actor_id") or "ego")
    ego_speed = max(_number(ego.get("initial_speed"), 0.0), 0.0)

    entities.append(_vehicle_entity(ego_name, False))
    ex, ey, ez, eyaw = _spawn(config, {})
    init_actions.append(
        f'<Private entityRef={quoteattr(ego_name)}>\n'
        "          <PrivateAction><TeleportAction>"
        f"{_world_position(ex, ey, ez, eyaw)}</TeleportAction></PrivateAction>\n"
        "        </Private>"
    )
    for index, actor in enumerate(actors):
        name = str(actor.get("actor_id") or f"actor_{index}")
        actor_type = str(actor.get("type") or actor.get("actor_type") or "").lower()
        pedestrian = "pedestrian" in actor_type or "vru" in actor_type
        ax, ay, az, ayaw = _spawn(config, actor)
        entities.append(_vehicle_entity(name, pedestrian))
        init_actions.append(
            f'<Private entityRef={quoteattr(name)}>\n'
            "          <PrivateAction><TeleportAction>"
            f"{_world_position(ax, ay, az, ayaw)}</TeleportAction></PrivateAction>\n"
            "        </Private>"
        )

    entity_xml = "\n      ".join(entities)
    init_xml = "\n        ".join(init_actions)
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
      </Actions>
    </Init>
    <Story name="g5">
      <Act name="act">
        <ManeuverGroup maximumExecutionCount="1" name="group">
          <Actors selectTriggeringEntities="false">
            <EntityRef entityRef={quoteattr(ego_name)}/>
          </Actors>
          <Maneuver name="maneuver">
            <Event name="run" priority="overwrite">
              <Action name="speed">
                <PrivateAction>
                  <LongitudinalAction>
                    <SpeedAction>
                      <SpeedActionDynamics dynamicsShape="step" value="0"
                                           dynamicsDimension="time"/>
                      <SpeedActionTarget>
                        <AbsoluteTargetSpeed value="{ego_speed:.3f}"/>
                      </SpeedActionTarget>
                    </SpeedAction>
                  </LongitudinalAction>
                </PrivateAction>
              </Action>
            </Event>
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
