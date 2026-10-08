#!/usr/bin/env python3
"""HunterCore 场景库批量添加脚本（自动驾驶训练场景数据集：300 个高频场景）。

用途
----
按系统「场景库规范」（设计文档 4.2.1 分类体系 + 4.2.2 场景配置结构，落库
``scene_svc.scenes.config_json``）批量生成 **300 个** 覆盖自动驾驶全功能与极端环境的
高频训练场景，并通过 scene-service ``POST /api/v1/scene`` 接口入库，供 Carla 仿真训练
（``POST /api/v1/scene/export`` 可导出 ScenarioRunner XML / OpenSCENARIO 1.2）。

设计要点
--------
1. **300 个高频场景**：按 ``SCENE_QUOTAS`` 分配到 4 大类 16 个叶子场景类型
   （basic / interactive / environment / corner_case），配额之和恒等于 300；
2. **满足 Carla 仿真训练**：``map_type=carla_town``（Town01~Town07）、自车基线车型
   ``HUNTER_SE``、天气段严格对齐 ``carla.WeatherParameters`` 语义（7 字段全必填）、
   参与者行为编排采用 ScenarioRunner 行为词汇（lane_follow / cut_in / cross_road /
   static / constant_velocity / forward ...）；
3. **覆盖全部功能与极端环境**：感知预测（切入/行人/多车混行/障碍突现）、决策规划
   （跟车/换道/路口会车/紧急制动/施工绕行）、控制（直线巡航/弯道）、鲁棒性（雨/雾/夜/
   逆光）、失效安全（传感器失效）；极端环境含暴雨/浓雾/黑夜/冬季暴雪/大风/强逆光等；
4. **严守场景库规范**：每条 config 在提交前以 ``SceneCreateRequest``（4.2.2 结构，
   ``extra="forbid"``）自校验；命名全局唯一（重名 → 3002 幂等跳过）；时长 ≤ 600s。

用法（仓库根目录；Linux 解释器为 ``python3``，需 3.11+ 方可启用契约 Schema 校验）::

    # 1) 仅生成 + 校验 + 落盘，不提交（推荐先跑，产出可读的数据集清单）
    #    目标目录不可写时自动回退临时目录；也可 --output ~/scene_batch_300.json 或 --output -
    python3 scripts/batch_add_scenes.py --dry-run

    # 2) 直连 scene-service 批量入库（开发/集群内，可选 GATEWAY_HMAC_SECRET 验签头）
    python3 scripts/batch_add_scenes.py --mode direct \
        --base-url http://localhost:8081 \
        --user-id 00000000-0000-0000-0000-000000000001 --roles admin

    # 3) 经 api-gateway（先登录换 JWT，再带 Bearer 提交）
    python3 scripts/batch_add_scenes.py --mode gateway \
        --base-url http://localhost:8080 --username admin --password 'Admin@12345' \
        --publish

环境变量（CLI 优先）：HUNTER_BATCH_MODE / HUNTER_BATCH_BASE_URL / HUNTER_BATCH_USER_ID /
HUNTER_BATCH_ROLES / HUNTER_BATCH_GATEWAY_HMAC_SECRET / HUNTER_BATCH_USERNAME /
HUNTER_BATCH_PASSWORD。

退出码：0 全部成功（或幂等跳过）；1 存在失败项；2 参数/环境错误。
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import sys
import tempfile
import uuid
from collections import OrderedDict
from collections.abc import Callable
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
#: 4.2.2 结构单一事实来源：scene-service schemas（缺失时降级为轻量内部校验）
sys.path.insert(0, str(ROOT / "common" / "python"))
sys.path.insert(0, str(ROOT / "services" / "scene-service"))

DEFAULT_OUTPUT = ROOT / "scripts" / "output" / "scene_batch_300.json"
#: 车端基线车型（见 config.SCENE_EXTRACTION_DEFAULT_EGO_MODEL）
EGO_MODEL = "HUNTER_SE"
#: 契约 SCENE_DURATION_MAX_SECONDS 默认上限
DURATION_MAX = 600.0
#: Carla 内置城镇（map_type=carla_town；⚠ 地图资源清单待确认，按内置镇轮换）
MAPS = ("Town01", "Town02", "Town03", "Town04", "Town05", "Town06", "Town07")
PI = math.pi


# =====================================================================
# 一、场景类型 → 配额（之和恒等于 300；覆盖 4 大类 16 个高频叶子场景）
# =====================================================================
SCENE_QUOTAS: OrderedDict[str, int] = OrderedDict(
    [
        # basic（基础行驶能力）
        ("straight_cruise", 12),
        ("curve_driving", 15),
        ("car_following", 22),
        ("lane_change", 22),
        # interactive（交互博弈）
        ("cut_in", 25),
        ("pedestrian_crossing", 28),
        ("intersection_meeting", 22),
        ("construction_detour", 14),
        # environment（环境鲁棒性，含极端天气）
        ("rainy", 22),
        ("night", 20),
        ("foggy", 14),
        ("backlight", 10),
        # corner_case（长尾 / 安全边界）
        ("emergency_braking", 24),
        ("obstacle_appearance", 20),
        ("sensor_failure", 12),
        ("multi_vehicle_mixed", 18),
    ]
)
assert sum(SCENE_QUOTAS.values()) == 300, "场景配额之和必须为 300"

#: 类型 → 分类（4.2.1 两级结构第一级；写入 tags 与描述）
CATEGORY_BY_TYPE: dict[str, str] = {
    "straight_cruise": "basic",
    "curve_driving": "basic",
    "car_following": "basic",
    "lane_change": "basic",
    "cut_in": "interactive",
    "pedestrian_crossing": "interactive",
    "intersection_meeting": "interactive",
    "construction_detour": "interactive",
    "rainy": "environment",
    "night": "environment",
    "foggy": "environment",
    "backlight": "environment",
    "emergency_braking": "corner_case",
    "obstacle_appearance": "corner_case",
    "sensor_failure": "corner_case",
    "multi_vehicle_mixed": "corner_case",
}

#: 类型 → 中文标签（契约 SCENE_TYPE_LABELS 同源）
LABEL_BY_TYPE: dict[str, str] = {
    "straight_cruise": "直线巡航",
    "curve_driving": "弯道行驶",
    "car_following": "跟车行驶",
    "lane_change": "变道行驶",
    "cut_in": "前车切入",
    "pedestrian_crossing": "行人横穿",
    "intersection_meeting": "路口会车",
    "construction_detour": "施工绕行",
    "rainy": "雨天",
    "night": "夜间",
    "foggy": "雾天",
    "backlight": "强光逆光",
    "emergency_braking": "紧急制动",
    "obstacle_appearance": "障碍物突现",
    "sensor_failure": "传感器失效",
    "multi_vehicle_mixed": "多车混行",
}

#: 类型 → 名称缩写（保证 scene_name 全局唯一且可读，≤128 字符）
ABBR_BY_TYPE: dict[str, str] = {
    "straight_cruise": "CRS",
    "curve_driving": "CRV",
    "car_following": "FOL",
    "lane_change": "LNC",
    "cut_in": "CUT",
    "pedestrian_crossing": "PED",
    "intersection_meeting": "INT",
    "construction_detour": "CST",
    "rainy": "RUN",
    "night": "NGT",
    "foggy": "FOG",
    "backlight": "GLR",
    "emergency_braking": "EBR",
    "obstacle_appearance": "OBS",
    "sensor_failure": "SNF",
    "multi_vehicle_mixed": "MVM",
}


# =====================================================================
# 二、天气段（对齐 carla.WeatherParameters 语义，7 字段全必填）
#    cloudiness/rain/wetness/fog/wind: 0..100；sun_azimuth: 0..360；sun_altitude: -90..90
# =====================================================================
WEATHER: dict[str, dict[str, float]] = {
    # —— 常规 ——
    "clear_day": {"cloudiness": 10, "rain": 0, "wetness": 0, "fog": 0, "wind": 5, "sun_azimuth": 0, "sun_altitude": 60},
    "cloudy": {"cloudiness": 70, "rain": 0, "wetness": 0, "fog": 5, "wind": 10, "sun_azimuth": 45, "sun_altitude": 40},
    "overcast": {"cloudiness": 85, "rain": 0, "wetness": 5, "fog": 8, "wind": 12, "sun_azimuth": 90, "sun_altitude": 30},
    # —— 雨天（含极端）——
    "light_rain": {"cloudiness": 70, "rain": 30, "wetness": 25, "fog": 8, "wind": 15, "sun_azimuth": 90, "sun_altitude": 35},
    "heavy_rain": {"cloudiness": 100, "rain": 90, "wetness": 90, "fog": 20, "wind": 45, "sun_azimuth": 120, "sun_altitude": 25},
    "torrential_rain": {"cloudiness": 100, "rain": 100, "wetness": 100, "fog": 35, "wind": 70, "sun_azimuth": 150, "sun_altitude": 12},
    # —— 雾天（含极端）——
    "mist": {"cloudiness": 50, "rain": 0, "wetness": 15, "fog": 30, "wind": 5, "sun_azimuth": 60, "sun_altitude": 28},
    "dense_fog": {"cloudiness": 80, "rain": 0, "wetness": 20, "fog": 70, "wind": 8, "sun_azimuth": 200, "sun_altitude": 10},
    "super_fog": {"cloudiness": 95, "rain": 5, "wetness": 40, "fog": 100, "wind": 6, "sun_azimuth": 210, "sun_altitude": 4},
    # —— 夜间 ——
    "night_clear": {"cloudiness": 20, "rain": 0, "wetness": 0, "fog": 0, "wind": 5, "sun_azimuth": 180, "sun_altitude": -15},
    "night_rain": {"cloudiness": 90, "rain": 60, "wetness": 75, "fog": 15, "wind": 25, "sun_azimuth": 200, "sun_altitude": -20},
    "dusk": {"cloudiness": 40, "rain": 0, "wetness": 0, "fog": 10, "wind": 10, "sun_azimuth": 270, "sun_altitude": 5},
    # —— 逆光 / 强光 ——
    "dawn_glare": {"cloudiness": 30, "rain": 0, "wetness": 5, "fog": 12, "wind": 8, "sun_azimuth": 90, "sun_altitude": 6},
    "sun_glare": {"cloudiness": 15, "rain": 0, "wetness": 0, "fog": 5, "wind": 5, "sun_azimuth": 0, "sun_altitude": 3},
    # —— 其他极端 ——
    "high_wind": {"cloudiness": 60, "rain": 10, "wetness": 20, "fog": 10, "wind": 90, "sun_azimuth": 100, "sun_altitude": 20},
    "winter_snow": {"cloudiness": 95, "rain": 0, "wetness": 60, "fog": 55, "wind": 50, "sun_azimuth": 220, "sun_altitude": -5},
}

#: 通用轮换天气序列（贯穿所有类型，保证环境多样性；含极端天气）
FULL_CYCLE: tuple[str, ...] = (
    "clear_day", "cloudy", "light_rain", "mist", "night_clear", "dusk",
    "heavy_rain", "dense_fog", "overcast", "dawn_glare", "high_wind", "night_rain",
    "torrential_rain", "super_fog", "winter_snow", "sun_glare",
)
#: 环境专项天气序列
WEATHER_CYCLE_BY_TYPE: dict[str, tuple[str, ...]] = {
    "rainy": ("light_rain", "heavy_rain", "torrential_rain", "night_rain"),
    "night": ("night_clear", "night_rain", "dusk"),
    "foggy": ("mist", "dense_fog", "super_fog"),
    "backlight": ("dawn_glare", "sun_glare", "dusk"),
}

#: 各类型默认难度标签（写入 tags，供场景库筛选）
FUNCTION_TAGS: dict[str, tuple[str, ...]] = {
    "straight_cruise": ("control", "cruise"),
    "curve_driving": ("control", "lateral"),
    "car_following": ("planning", "longitudinal"),
    "lane_change": ("planning", "lateral"),
    "cut_in": ("prediction", "interaction"),
    "pedestrian_crossing": ("perception", "vru"),
    "intersection_meeting": ("planning", "decision"),
    "construction_detour": ("perception", "static-avoid"),
    "rainy": ("robustness", "wet-road"),
    "night": ("robustness", "low-light"),
    "foggy": ("robustness", "low-visibility"),
    "backlight": ("robustness", "glare"),
    "emergency_braking": ("safety", "aeb"),
    "obstacle_appearance": ("safety", "corner-case"),
    "sensor_failure": ("safety", "fail-degraded"),
    "multi_vehicle_mixed": ("perception", "complex-traffic"),
}


# =====================================================================
# 三、4.2.2 结构构造helper（全部返回 dict，落库前经 SceneCreateRequest 校验）
# =====================================================================
def _spawn(x: float, y: float = 0.0, yaw: float = 0.0, z: float = 0.0, lane_id: str | None = None) -> dict[str, Any]:
    return {"x": round(x, 3), "y": round(y, 3), "z": round(z, 3), "yaw": round(yaw, 5), "lane_id": lane_id}


def _actor(aid: str, atype: str, spawn: dict[str, Any], behavior: str, **params: Any) -> dict[str, Any]:
    return {
        "actor_id": aid,
        "type": atype,
        "spawn_point": spawn,
        "behavior": {"behavior_type": behavior, "parameters": dict(params)},
    }


def _vehicle(aid: str, x: float, y: float, behavior: str, yaw: float = 0.0, **params: Any) -> dict[str, Any]:
    return _actor(aid, "vehicle", _spawn(x, y, yaw), behavior, **params)


def _pedestrian(aid: str, x: float, y: float, speed: float = 1.4, yaw: float = PI / 2) -> dict[str, Any]:
    return _actor(aid, "pedestrian", _spawn(x, y, yaw), "cross_road", crossing_speed=round(speed, 2))


def _static(aid: str, x: float, y: float, kind: str = "debris") -> dict[str, Any]:
    return _actor(aid, "other", _spawn(x, y, 0.0), "static", object_kind=kind)


def _event(eid: str, etype: str, condition: str, action: str, trigger: dict[str, Any] | None = None, **action_params: Any) -> dict[str, Any]:
    return {
        "event_id": eid,
        "type": etype,
        "trigger": {"condition": condition, "parameters": trigger or {"source": "batch"}},
        "action": {"action_type": action, "parameters": dict(action_params)},
    }


def _weather(name: str) -> dict[str, float]:
    cfg = WEATHER[name]
    # 复制并保证 7 字段齐全（校验阶段兜底，禁止漏字段）
    return {
        "cloudiness": cfg["cloudiness"],
        "rain": cfg["rain"],
        "wetness": cfg["wetness"],
        "fog": cfg["fog"],
        "wind": cfg["wind"],
        "sun_azimuth": cfg["sun_azimuth"],
        "sun_altitude": cfg["sun_altitude"],
    }


def _config(
    *,
    map_id: str,
    ego_speed: float,
    ego_steer: float,
    weather: str,
    actors: list[dict[str, Any]],
    events: list[dict[str, Any]],
    duration: float,
    spawn: dict[str, Any] | None = None,
    min_safe: float = 5.0,
    max_dev: float = 2.0,
    no_collision: bool = True,
) -> dict[str, Any]:
    return {
        "map": {
            "map_id": map_id,
            "map_type": "carla_town",
            "spawn_point": spawn or _spawn(0.0, 0.0, 0.0, lane_id="lane-1"),
        },
        "ego_vehicle": {
            "model": EGO_MODEL,
            "initial_speed": round(max(0.0, ego_speed), 3),
            "initial_steer": round(min(1.0, max(-1.0, ego_steer)), 3),
        },
        "weather": _weather(weather),
        "actors": actors,
        "events": events,
        "success_criteria": {
            "max_speed_deviation": round(max_dev, 3),
            "no_collision": no_collision,
            "min_safe_distance": round(max(0.0, min_safe), 3),
        },
        "duration": round(min(duration, DURATION_MAX), 1),
    }


def _cyclic(seq: tuple[str, ...] | list[str], i: int) -> str:
    return seq[i % len(seq)]


# =====================================================================
# 四、各类型场景生成器：builder(i, total) -> (name_token, config, extra_tags)
#     i 为该类型内序号（0 基）；通过轮换 map / weather / speed / actor 参数制造多样性
# =====================================================================
Builder = Callable[[int, int], tuple[str, dict[str, Any], list[str]]]

# 速度档（m/s）：低速城区 → 高速巡航，轮换使用
SPEED_CITY = (2.0, 4.0, 6.0, 8.0)
SPEED_HIGHWAY = (12.0, 16.0, 20.0, 24.0)


def _b_straight(i: int, total: int) -> tuple[str, dict[str, Any], list[str]]:
    highway = i % 2 == 0
    speed = _cyclic(SPEED_HIGHWAY if highway else SPEED_CITY, i)
    w = _cyclic(FULL_CYCLE, i)
    actors = []
    if i % 3 == 0:  # 远处背景车
        actors.append(_vehicle("bg-vehicle", 80.0, 3.5, "lane_follow", target_speed=round(speed * 0.9, 2)))
    events = []
    if i % 4 == 0:
        events.append(_event("ev-cruise-brake", "emergency_brake", "ego.distance.headway < 2.0", "hold_speed"))
    cfg = _config(
        map_id=MAPS[i % len(MAPS)], ego_speed=speed, ego_steer=0.0, weather=w,
        actors=actors, events=events, duration=60 + 10 * (i % 6),
        min_safe=3.0 if not highway else 8.0, max_dev=1.0,
    )
    return f"{'highway' if highway else 'city'}-{w}", cfg, ["carla"]


def _b_curve(i: int, total: int) -> tuple[str, dict[str, Any], list[str]]:
    curve = ("left", "right", "s-ramp", "hairpin")[i % 4]
    steer = {"left": -0.5, "right": 0.5, "s-ramp": 0.2, "hairpin": -0.9}[curve]
    speed = (4.0, 6.0, 8.0, 3.0)[i % 4]
    w = _cyclic(FULL_CYCLE, i + 1)
    actors = []
    if i % 2 == 0:
        actors.append(_vehicle("oncoming", 60.0, -3.5, "lane_follow", yaw=PI, target_speed=6.0))
    events = [_event("ev-curve-limit", "collision", "ego.lateral.accel > 5.0", "speed_limit", target_speed=round(speed, 2))]
    cfg = _config(
        map_id=MAPS[(i + 1) % len(MAPS)], ego_speed=speed, ego_steer=steer, weather=w,
        actors=actors, events=events, duration=50 + 10 * (i % 5), min_safe=4.0, max_dev=1.5,
    )
    return f"{curve}-{w}", cfg, ["carla"]


def _b_follow(i: int, total: int) -> tuple[str, dict[str, Any], list[str]]:
    speed = _cyclic(SPEED_HIGHWAY + SPEED_CITY, i)
    gap = (6.0, 10.0, 15.0, 20.0, 25.0)[i % 5]
    lead_speed = round(speed * (0.7 + 0.1 * (i % 3)), 2)
    w = _cyclic(FULL_CYCLE, i)
    actors = [_vehicle("lead-vehicle", gap, 0.0, "lane_follow", target_speed=lead_speed)]
    if i % 3 == 0:
        actors.append(_vehicle("adjacent", gap + 12.0, 3.5, "lane_follow", target_speed=round(speed * 0.95, 2)))
    events = []
    if i % 2 == 0:
        events.append(_event("ev-lead-brake", "emergency_brake", "ego.distance.lead < 8.0", "brake", deceleration=round(3.0 + 0.5 * (i % 4), 2)))
    cfg = _config(
        map_id=MAPS[i % len(MAPS)], ego_speed=speed, ego_steer=0.0, weather=w,
        actors=actors, events=events, duration=70 + 10 * (i % 6),
        min_safe=max(3.0, gap * 0.5), max_dev=1.5,
    )
    return f"gap{int(gap)}m-{w}", cfg, ["carla"]


def _b_lane_change(i: int, total: int) -> tuple[str, dict[str, Any], list[str]]:
    direction = ("left", "right")[i % 2]
    speed = _cyclic(SPEED_CITY + SPEED_HIGHWAY, i)
    w = _cyclic(FULL_CYCLE, i + 2)
    lat = -3.5 if direction == "left" else 3.5
    actors = [
        _vehicle("target-gap", 18.0, lat, "lane_follow", target_speed=round(speed * 1.05, 2)),
        _vehicle("behind", -6.0, lat, "constant_velocity", target_speed=round(speed * 1.1, 2)),
    ]
    if i % 3 == 0:
        actors.append(_vehicle("lead", 12.0, 0.0, "lane_follow", target_speed=round(speed * 0.9, 2)))
    events = [_event("ev-lane-change", "cut_in", f"ego.position.x > {10 + i}", "lane_change", target_lane=direction, complete_in=round(4.0 + (i % 4), 2))]
    cfg = _config(
        map_id=MAPS[(i + 2) % len(MAPS)], ego_speed=speed, ego_steer=0.0, weather=w,
        actors=actors, events=events, duration=50 + 10 * (i % 5), min_safe=4.0, max_dev=2.0,
    )
    return f"{direction}-{w}", cfg, ["carla"]


def _b_cut_in(i: int, total: int) -> tuple[str, dict[str, Any], list[str]]:
    speed = _cyclic(SPEED_HIGHWAY, i)
    side = -3.5 if i % 2 == 0 else 3.5
    aggressiveness = (0.8, 1.5, 2.5, 4.0)[i % 4]  # 切入横向速度 → 难度递增
    cut_gap = (20.0, 28.0, 35.0)[i % 3]
    w = _cyclic(FULL_CYCLE, i + 3)
    actors = [
        _vehicle("cut-in-vehicle", cut_gap, side, "cut_in", target_lane="ego_lane", cut_in_speed=aggressiveness),
        _vehicle("lead", cut_gap + 15.0, 0.0, "lane_follow", target_speed=round(speed * 0.85, 2)),
    ]
    events = [_event("ev-cut-in", "cut_in", f"ego.position.x > {cut_gap - 5}", "speed_change", target_speed=round(speed * 0.6, 2))]
    cfg = _config(
        map_id=MAPS[(i + 1) % len(MAPS)], ego_speed=speed, ego_steer=0.0, weather=w,
        actors=actors, events=events, duration=45 + 10 * (i % 5), min_safe=6.0, max_dev=2.0,
    )
    return f"agg{aggressiveness}-{w}", cfg, ["carla"]


def _b_pedestrian(i: int, total: int) -> tuple[str, dict[str, Any], list[str]]:
    speed = (3.0, 5.0, 7.0, 9.0)[i % 4]
    n_ped = 1 + (i % 4)
    peds = (6.0, 8.0, 10.0)[i % 3]
    w = _cyclic(FULL_CYCLE, i + 4)
    actors = []
    for k in range(n_ped):
        actors.append(_pedestrian(f"ped-{k + 1}", 22.0 + k * 1.5, -peds - k * 0.6, speed=round(1.2 + 0.4 * (k % 3), 2)))
    if i % 3 == 0:
        actors.append(_vehicle("cross-vehicle", 40.0, -3.5, "lane_follow", yaw=PI, target_speed=6.0))
    events = [_event("ev-ped-crossing", "pedestrian_crossing", "ego.position.x > 15", "stop", min_safe_distance=2.0)]
    cfg = _config(
        map_id=MAPS[(i + 3) % len(MAPS)], ego_speed=speed, ego_steer=0.0, weather=w,
        actors=actors, events=events, duration=40 + 10 * (i % 5), min_safe=2.0, max_dev=1.0,
    )
    return f"{n_ped}ped-{w}", cfg, ["carla", "vru"]


def _b_intersection(i: int, total: int) -> tuple[str, dict[str, Any], list[str]]:
    maneuver = ("straight", "left-turn", "right-turn", "uturn")[i % 4]
    speed = (4.0, 6.0, 8.0)[i % 3]
    w = _cyclic(FULL_CYCLE, i + 5)
    actors = [
        _vehicle("cross-vehicle-1", 30.0, -8.0, "forward", yaw=PI / 2, target_speed=7.0),
        _vehicle("cross-vehicle-2", 30.0, 8.0, "forward", yaw=-PI / 2, target_speed=6.0),
        _vehicle("opposite", 45.0, 0.0, "lane_follow", yaw=PI, target_speed=6.0),
    ]
    if i % 2 == 0:
        actors.append(_pedestrian("ped-zebra", 18.0, -6.0))
    events = [_event("ev-signal", "collision", "ego.position.x > 10", "traffic_light", phase=("green" if i % 2 else "yellow"))]
    cfg = _config(
        map_id=MAPS[(i + 4) % len(MAPS)], ego_speed=speed, ego_steer=0.0, weather=w,
        actors=actors, events=events, duration=55 + 10 * (i % 5), min_safe=4.0, max_dev=2.0,
    )
    return f"{maneuver}-{w}", cfg, ["carla"]


def _b_construction(i: int, total: int) -> tuple[str, dict[str, Any], list[str]]:
    speed = (4.0, 6.0, 8.0)[i % 3]
    n_bar = 2 + (i % 3)
    w = _cyclic(FULL_CYCLE, i + 6)
    actors = [_static(f"barrier-{k + 1}", 15.0 + k * 4.0, 1.8, kind="construction") for k in range(n_bar)]
    actors.append(_vehicle("worker-vehicle", 40.0, 3.5, "static"))
    events = [_event("ev-detour", "obstacle_appearance", "ego.position.x > 8", "lane_change", target_lane="left", complete_in=6.0)]
    cfg = _config(
        map_id=MAPS[(i + 5) % len(MAPS)], ego_speed=speed, ego_steer=0.0, weather=w,
        actors=actors, events=events, duration=50 + 10 * (i % 4), min_safe=3.0, max_dev=1.5,
    )
    return f"{n_bar}barrier-{w}", cfg, ["carla"]


def _b_rain(i: int, total: int) -> tuple[str, dict[str, Any], list[str]]:
    w = _cyclic(WEATHER_CYCLE_BY_TYPE["rainy"], i)
    speed = _cyclic(SPEED_CITY, i)
    actors = [_vehicle("lead-vehicle", 18.0, 0.0, "lane_follow", target_speed=round(speed * 0.8, 2))]
    events = [_event("ev-wet-brake", "emergency_brake", "ttc < 2.5", "brake", deceleration=3.0)]
    cfg = _config(
        map_id=MAPS[i % len(MAPS)], ego_speed=speed, ego_steer=0.0, weather=w,
        actors=actors, events=events, duration=60 + 10 * (i % 5), min_safe=6.0, max_dev=2.0,
    )
    return f"rain-{w}", cfg, ["carla", "extreme"]


def _b_night(i: int, total: int) -> tuple[str, dict[str, Any], list[str]]:
    w = _cyclic(WEATHER_CYCLE_BY_TYPE["night"], i)
    speed = _cyclic(SPEED_CITY, i)
    actors = [_vehicle("lead-vehicle", 22.0, 0.0, "lane_follow", target_speed=round(speed * 0.85, 2))]
    if i % 2 == 0:
        actors.append(_pedestrian("ped-dark", 26.0, -4.0))
    events = [_event("ev-night-detect", "obstacle_appearance", "ego.position.x > 12", "brake", deceleration=2.5)]
    cfg = _config(
        map_id=MAPS[(i + 2) % len(MAPS)], ego_speed=speed, ego_steer=0.0, weather=w,
        actors=actors, events=events, duration=60 + 10 * (i % 4), min_safe=5.0, max_dev=2.0,
    )
    return f"night-{w}", cfg, ["carla", "extreme"]


def _b_fog(i: int, total: int) -> tuple[str, dict[str, Any], list[str]]:
    w = _cyclic(WEATHER_CYCLE_BY_TYPE["foggy"], i)
    speed = (3.0, 4.0, 5.0)[i % 3]
    actors = [_vehicle("lead-emerge", 14.0 + i, 0.0, "lane_follow", target_speed=round(speed * 0.7, 2))]
    events = [_event("ev-fog-appear", "obstacle_appearance", "ego.position.x > 8", "emergency_stop")]
    cfg = _config(
        map_id=MAPS[(i + 3) % len(MAPS)], ego_speed=speed, ego_steer=0.0, weather=w,
        actors=actors, events=events, duration=55 + 10 * (i % 4), min_safe=4.0, max_dev=1.5,
    )
    return f"fog-{w}", cfg, ["carla", "extreme"]


def _b_backlight(i: int, total: int) -> tuple[str, dict[str, Any], list[str]]:
    w = _cyclic(WEATHER_CYCLE_BY_TYPE["backlight"], i)
    speed = _cyclic(SPEED_CITY, i)
    actors = [_vehicle("lead-vehicle", 20.0, 0.0, "lane_follow", target_speed=round(speed * 0.9, 2))]
    if i % 2 == 0:
        actors.append(_static("roadside", 24.0, 3.0, kind="sign"))
    events = [_event("ev-glare", "pedestrian_crossing", "ego.position.x > 10", "hold_speed")]
    cfg = _config(
        map_id=MAPS[(i + 4) % len(MAPS)], ego_speed=speed, ego_steer=0.0, weather=w,
        actors=actors, events=events, duration=50 + 10 * (i % 4), min_safe=4.0, max_dev=1.5,
    )
    return f"glare-{w}", cfg, ["carla", "extreme"]


def _b_emergency(i: int, total: int) -> tuple[str, dict[str, Any], list[str]]:
    speed = _cyclic(SPEED_HIGHWAY + SPEED_CITY, i)
    gap = (8.0, 12.0, 16.0, 20.0)[i % 4]
    decel = (4.0, 6.0, 8.0, 9.5)[i % 4]
    w = _cyclic(FULL_CYCLE, i + 7)
    actors = [_vehicle("lead-hard-brake", gap, 0.0, "lane_follow", target_speed=round(speed * 0.5, 2))]
    events = [_event("ev-aeb", "emergency_brake", f"ttc < {round(1.0 + 0.3 * (i % 3), 2)}", "emergency_stop", deceleration=decel)]
    cfg = _config(
        map_id=MAPS[(i + 1) % len(MAPS)], ego_speed=speed, ego_steer=0.0, weather=w,
        actors=actors, events=events, duration=40 + 10 * (i % 4), min_safe=2.0, max_dev=2.0, no_collision=True,
    )
    return f"decel{decel}-{w}", cfg, ["carla", "safety"]


def _b_obstacle(i: int, total: int) -> tuple[str, dict[str, Any], list[str]]:
    speed = _cyclic(SPEED_CITY + SPEED_HIGHWAY, i)
    kind = ("debris", "cone", "animal", "fallen-load", "tyre")[i % 5]
    appear_x = (10.0, 14.0, 18.0)[i % 3]
    w = _cyclic(FULL_CYCLE, i + 8)
    actors = [_static("sudden-obstacle", appear_x, 0.4, kind=kind)]
    events = [_event("ev-obstacle", "obstacle_appearance", f"ego.position.x > {appear_x - 4}", "emergency_stop")]
    cfg = _config(
        map_id=MAPS[(i + 2) % len(MAPS)], ego_speed=speed, ego_steer=0.0, weather=w,
        actors=actors, events=events, duration=40 + 10 * (i % 4), min_safe=2.0, max_dev=2.0,
    )
    return f"{kind}-{w}", cfg, ["carla", "corner-case"]


def _b_sensor(i: int, total: int) -> tuple[str, dict[str, Any], list[str]]:
    sensor = ("camera_front", "lidar_top", "radar_front", "camera_rear", "gnss")[i % 5]
    fault = ("black_frame", "point_dropout", "ghost_return", "freeze", "denied")[i % 5]
    speed = _cyclic(SPEED_CITY, i)
    w = _cyclic(FULL_CYCLE, i + 9)
    actors = [_vehicle("lead-vehicle", 20.0, 0.0, "lane_follow", target_speed=round(speed * 0.8, 2))]
    events = [_event("ev-sensor-fail", "sensor_failure", f"ego.position.x > {8 + 2 * (i % 3)}", "degrade_control", sensor=sensor, fault=fault, fallback="limp_home")]
    cfg = _config(
        map_id=MAPS[(i + 3) % len(MAPS)], ego_speed=speed, ego_steer=0.0, weather=w,
        actors=actors, events=events, duration=45 + 10 * (i % 4), min_safe=5.0, max_dev=3.0, no_collision=True,
    )
    return f"{sensor}-{fault}", cfg, ["carla", "fail-degraded"]


def _b_multi(i: int, total: int) -> tuple[str, dict[str, Any], list[str]]:
    n_v = 3 + (i % 4)
    n_p = 1 + (i % 3)
    speed = _cyclic(SPEED_CITY, i)
    w = _cyclic(FULL_CYCLE, i + 10)
    actors: list[dict[str, Any]] = []
    for k in range(n_v):
        lat = (3.5, -3.5)[k % 2]
        actors.append(_vehicle(f"veh-{k + 1}", 12.0 + k * 6.0, lat, ("lane_follow", "cut_in")[k % 2], target_speed=round(speed * (0.8 + 0.1 * k), 2)))
    for k in range(n_p):
        actors.append(_pedestrian(f"ped-{k + 1}", 24.0 + k * 2.0, -6.0 - k))
    actors.append(_static("roadside-obj", 30.0, 4.5, kind="bin"))
    events = [
        _event("ev-mix-cutin", "cut_in", f"ego.position.x > {16 + i}", "speed_change", target_speed=round(speed * 0.5, 2)),
        _event("ev-mix-ped", "pedestrian_crossing", "ego.position.x > 22", "yield"),
    ]
    cfg = _config(
        map_id=MAPS[(i + 4) % len(MAPS)], ego_speed=speed, ego_steer=0.0, weather=w,
        actors=actors, events=events, duration=70 + 10 * (i % 5), min_safe=3.0, max_dev=2.5,
    )
    return f"{n_v}v{n_p}p-{w}", cfg, ["carla", "complex"]


BUILDERS: dict[str, Builder] = {
    "straight_cruise": _b_straight,
    "curve_driving": _b_curve,
    "car_following": _b_follow,
    "lane_change": _b_lane_change,
    "cut_in": _b_cut_in,
    "pedestrian_crossing": _b_pedestrian,
    "intersection_meeting": _b_intersection,
    "construction_detour": _b_construction,
    "rainy": _b_rain,
    "night": _b_night,
    "foggy": _b_fog,
    "backlight": _b_backlight,
    "emergency_braking": _b_emergency,
    "obstacle_appearance": _b_obstacle,
    "sensor_failure": _b_sensor,
    "multi_vehicle_mixed": _b_multi,
}
assert set(BUILDERS) == set(SCENE_QUOTAS), "生成器必须覆盖全部配额场景类型"


# =====================================================================
# 五、组装 300 条场景（全局唯一命名 + tags 规范）
# =====================================================================
def _dedup_tags(tags: list[str], *, limit: int = 20, maxlen: int = 32) -> list[str]:
    out: list[str] = []
    for t in tags:
        t = str(t).strip()[:maxlen]
        if t and t not in out:
            out.append(t)
        if len(out) >= limit:
            break
    return out


def generate_scenes() -> list[dict[str, Any]]:
    """按配额生成 300 条场景载荷（dict，字段严格对齐 SceneCreateRequest）。"""
    scenes: list[dict[str, Any]] = []
    g = 0
    for stype, count in SCENE_QUOTAS.items():
        builder = BUILDERS[stype]
        abbr = ABBR_BY_TYPE[stype]
        label = LABEL_BY_TYPE[stype]
        category = CATEGORY_BY_TYPE[stype]
        for i in range(count):
            g += 1
            token, cfg, extra = builder(i, count)
            # SCN-<全局序号>-<类型缩写>-<类型内序号>-<关键参数>：全局唯一、可读、≤128
            name = f"SCN-{g:03d}-{abbr}-{i + 1:02d}-{token}"[:128]
            tags = _dedup_tags([
                category, stype, "ad-training", "carla",
                *FUNCTION_TAGS[stype], *extra,
            ])
            description = (
                f"[{label}] {category} 类高频训练场景 #{i + 1}/{count}"
                f"（地图 {cfg['map']['map_id']}，天气 {token.rsplit('-', 1)[-1]}，"
                f"自车初速 {cfg['ego_vehicle']['initial_speed']}m/s，时长 {cfg['duration']}s；"
                f"参与者 {len(cfg['actors'])}，事件 {len(cfg['events'])}）。"
            )[:1024]
            scenes.append({
                "scene_name": name,
                "scene_type": stype,
                "description": description,
                "tags": tags,
                "config": cfg,
            })
    return scenes


# =====================================================================
# 六、场景库规范校验（优先用 4.2.2 Schema；不可导入时降级为轻量内部校验）
# =====================================================================
VALID_SCENE_TYPES = set(SCENE_QUOTAS)
_SCENE_EVENT_TYPES = {"collision", "cut_in", "pedestrian_crossing", "emergency_brake",
                      "manual_takeover", "sensor_failure", "obstacle_appearance"}
_ACTOR_TYPES = {"vehicle", "pedestrian", "other"}
_WEATHER_KEYS = {"cloudiness", "rain", "wetness", "fog", "wind", "sun_azimuth", "sun_altitude"}


def _light_validate(payload: dict[str, Any]) -> None:
    """轻量结构校验（Schema 不可用时的兜底，覆盖枚举/范围/必填）。违规抛 ValueError。"""
    for key in ("scene_name", "scene_type", "config"):
        if not payload.get(key):
            raise ValueError(f"缺少必填字段: {key}")
    name = payload["scene_name"]
    if not 1 <= len(name) <= 128:
        raise ValueError("scene_name 长度越界")
    if payload["scene_type"] not in VALID_SCENE_TYPES:
        raise ValueError(f"非法 scene_type: {payload['scene_type']}")
    cfg = payload["config"]
    if cfg["map"]["map_type"] not in {"carla_town", "hd_map", "site_map"}:
        raise ValueError("非法 map_type")
    ego = cfg["ego_vehicle"]
    if ego["initial_speed"] < 0 or not -1 <= ego["initial_steer"] <= 1:
        raise ValueError("ego_vehicle 取值越界")
    w = cfg["weather"]
    if set(w) != _WEATHER_KEYS:
        raise ValueError("weather 必须恰含 7 字段")
    for k, v in w.items():
        lo, hi = (-90, 90) if k == "sun_altitude" else (0, 360 if k == "sun_azimuth" else 100)
        if not lo <= v <= hi:
            raise ValueError(f"weather.{k}={v} 越界[{lo},{hi}]")
    if len(cfg["actors"]) > 100 or len(cfg["events"]) > 100:
        raise ValueError("actors/events 超过 100")
    for a in cfg["actors"]:
        if a["type"] not in _ACTOR_TYPES:
            raise ValueError(f"非法 actor.type: {a['type']}")
        if not a["behavior"]["behavior_type"]:
            raise ValueError("actor.behavior.behavior_type 不能为空")
    for e in cfg["events"]:
        if e["type"] not in _SCENE_EVENT_TYPES:
            raise ValueError(f"非法 event.type: {e['type']}")
    dur = cfg["duration"]
    if not 0 < dur <= DURATION_MAX:
        raise ValueError(f"duration={dur} 越界(0,{DURATION_MAX}]")
    sc = cfg["success_criteria"]
    if sc["max_speed_deviation"] < 0 or sc["min_safe_distance"] < 0:
        raise ValueError("success_criteria 取值越界")


_SCHEMA_VALIDATOR: Any = None
_SCHEMA_VALIDATOR_LOADED = False


def load_schema_validator() -> Any:
    """尝试导入 4.2.2 契约 Schema（SceneCreateRequest）；失败返回 None（结果缓存，只告警一次）。"""
    global _SCHEMA_VALIDATOR, _SCHEMA_VALIDATOR_LOADED
    if _SCHEMA_VALIDATOR_LOADED:
        return _SCHEMA_VALIDATOR
    _SCHEMA_VALIDATOR_LOADED = True
    try:
        from app.schemas.scene import SceneCreateRequest  # type: ignore

        _SCHEMA_VALIDATOR = SceneCreateRequest
    except Exception as exc:  # noqa: BLE001
        print(
            f"[warn] 无法导入 scene-service Schema（降级轻量校验）：{exc}"
            "\n       （需 Python 3.11+；如用系统 python3.10 请改用项目 venv 或 3.11+ 解释器）",
            file=sys.stderr,
        )
        _SCHEMA_VALIDATOR = None
    return _SCHEMA_VALIDATOR


def validate_scenes(scenes: list[dict[str, Any]]) -> tuple[int, list[str]]:
    """逐条校验；返回 (通过数, 错误列表)。"""
    validator = load_schema_validator()
    errors: list[str] = []
    passed = 0
    for payload in scenes:
        try:
            if validator is not None:
                validator.model_validate(payload)  # extra="forbid"：字段名/结构严格校验
            else:
                _light_validate(payload)
            passed += 1
        except Exception as exc:  # noqa: BLE001
            msg = str(exc).splitlines()[0] if str(exc) else type(exc).__name__
            errors.append(f"{payload.get('scene_name')}: {msg}")
    return passed, errors


# =====================================================================
# 七、认证与批量提交（direct：X-User-Id/X-Roles[可选 HMAC 验签]；gateway：JWT Bearer）
# =====================================================================
def _make_header_factory(args: argparse.Namespace, token: str | None) -> Callable[[], dict[str, str]]:
    if args.mode == "gateway":
        assert token is not None
        bearer = {"Authorization": f"Bearer {token}"}
        return lambda: dict(bearer)
    if args.hmac_secret:
        try:
            from hunter_common.internal_auth import build_identity_headers
        except Exception as exc:  # noqa: BLE001
            raise SystemExit(f"[error] 需要 GATEWAY_HMAC_SECRET 验签但 hunter_common.internal_auth 不可用: {exc}")

        def factory() -> dict[str, str]:
            return build_identity_headers(
                args.hmac_secret, user_id=args.user_id, roles=args.roles, trace_id=str(uuid.uuid4())
            )

        return factory

    def plain() -> dict[str, str]:
        return {"X-User-Id": args.user_id, "X-Roles": args.roles, "X-Trace-Id": str(uuid.uuid4())}

    return plain


async def _login_gateway(client: Any, args: argparse.Namespace) -> str:
    resp = await client.post(f"{args.base_url}/api/v1/user/login",
                             json={"username": args.username, "password": args.password})
    body = _safe_json(resp)
    if resp.status_code != 200 or not body or body.get("code") != 0:
        raise SystemExit(f"[error] 网关登录失败: HTTP {resp.status_code} {body}")
    token = ((body.get("data") or {}).get("access_token")) or ""
    if not token:
        raise SystemExit("[error] 登录响应缺少 access_token")
    return str(token)


def _safe_json(resp: Any) -> dict[str, Any] | None:
    try:
        return resp.json()
    except Exception:  # noqa: BLE001
        return None


async def _post_scene(
    client: Any, sem: Any, payload: dict[str, Any], headers: Callable[[], dict[str, str]],
    args: argparse.Namespace, stats: dict[str, int], failures: list[str],
) -> None:
    url = f"{args.base_url}/api/v1/scene"
    last_err = ""
    for attempt in range(args.max_retries + 1):
        async with sem:
            try:
                resp = await client.post(url, json=payload, headers=headers(), timeout=args.timeout)
            except Exception as exc:  # noqa: BLE001 - 传输层异常统一重试
                last_err = f"transport: {exc}"
                await asyncio.sleep(0.5 * (attempt + 1))
                continue
        body = _safe_json(resp) or {}
        code = body.get("code")
        name = payload["scene_name"]
        if resp.status_code in (200, 201) and code == 0:
            stats["created"] += 1
            scene_id = (body.get("data") or {}).get("scene_id")
            if args.publish and scene_id:
                await _publish_scene(client, sem, headers, args, scene_id, name, stats, failures)
            return
        if code == 3002 or resp.status_code == 409:
            stats["existed"] += 1  # 幂等：名称已存在视为已入库
            return
        if resp.status_code in (429, 500, 502, 503, 504):
            last_err = f"HTTP {resp.status_code} code={code} {body.get('message')}"
            await asyncio.sleep(0.6 * (attempt + 1))
            continue
        failures.append(f"{name}: HTTP {resp.status_code} code={code} msg={body.get('message')}")
        stats["failed"] += 1
        return
    failures.append(f"{name}: 重试耗尽 last={last_err}")
    stats["failed"] += 1


async def _publish_scene(
    client: Any, sem: Any, headers: Callable[[], dict[str, str]], args: argparse.Namespace,
    scene_id: str, name: str, stats: dict[str, int], failures: list[str],
) -> None:
    async with sem:
        try:
            resp = await client.post(f"{args.base_url}/api/v1/scene/{scene_id}/publish",
                                     headers=headers(), timeout=args.timeout)
            body = _safe_json(resp) or {}
            if resp.status_code in (200, 201) and body.get("code") == 0:
                stats["published"] += 1
            else:
                failures.append(f"{name}(publish): HTTP {resp.status_code} code={body.get('code')}")
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{name}(publish): transport {exc}")


async def submit_scenes(scenes: list[dict[str, Any]], args: argparse.Namespace) -> dict[str, int]:
    import httpx

    stats = {"created": 0, "existed": 0, "failed": 0, "published": 0}
    failures: list[str] = []
    sem = asyncio.Semaphore(max(1, args.concurrency))
    async with httpx.AsyncClient() as client:
        if args.mode == "gateway":
            token = await _login_gateway(client, args)
            headers = _make_header_factory(args, token)
            print("[auth] gateway 登录成功，Bearer token 已就绪")
        else:
            headers = _make_header_factory(args, None)
            sign = "HMAC 验签头" if args.hmac_secret else "明文身份头"
            print(f"[auth] direct 模式（{sign}）→ {args.base_url}")
        tasks = [_post_scene(client, sem, p, headers, args, stats, failures) for p in scenes]
        total = len(tasks)
        for done, coro in enumerate(asyncio.as_completed(tasks), 1):
            await coro
            if done % 25 == 0 or done == total:
                print(f"[progress] {done}/{len(scenes)} 已处理 "
                      f"(created={stats['created']} existed={stats['existed']} failed={stats['failed']})")
    if failures:
        print("\n[failures]（前 30 条）", file=sys.stderr)
        for f in failures[:30]:
            print("  - " + f, file=sys.stderr)
    stats_with_fail = {**stats, "failed_total": len(failures)}
    return stats_with_fail


# =====================================================================
# 八、CLI
# =====================================================================
def _env(name: str, default: str | None = None) -> str | None:
    return os.environ.get(name) or default


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="批量添加 300 个自动驾驶训练场景到 HunterCore 场景库（Carla 仿真）",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--dry-run", action="store_true", help="仅生成 + 校验 + 落盘 JSON，不提交 API")
    p.add_argument("--mode", choices=("direct", "gateway"), default=_env("HUNTER_BATCH_MODE", "direct"),
                   help="direct=直连 scene-service(X-User-Id/X-Roles)；gateway=经 api-gateway(JWT)")
    p.add_argument("--base-url", default=_env("HUNTER_BATCH_BASE_URL", ""),
                   help="服务基址；direct 默认 http://localhost:8081，gateway 默认 http://localhost:8080")
    p.add_argument("--user-id", default=_env("HUNTER_BATCH_USER_ID", "00000000-0000-0000-0000-000000000001"),
                   help="direct 模式创建者 user_id（须为合法 UUID）")
    p.add_argument("--roles", default=_env("HUNTER_BATCH_ROLES", "admin"),
                   help="direct 模式角色（逗号分隔，写权限需 admin/operator）")
    p.add_argument("--hmac-secret", default=_env("HUNTER_BATCH_GATEWAY_HMAC_SECRET", _env("GATEWAY_HMAC_SECRET", "")),
                   help="direct 模式身份头 HMAC 验签密钥（服务配置了 GATEWAY_HMAC_SECRET 时必填）")
    p.add_argument("--username", default=_env("HUNTER_BATCH_USERNAME", ""), help="gateway 模式登录用户名")
    p.add_argument("--password", default=_env("HUNTER_BATCH_PASSWORD", ""), help="gateway 模式登录密码")
    p.add_argument("--publish", action="store_true", help="创建后立即发布（draft→published，需 execute 权限）")
    p.add_argument("--concurrency", type=int, default=5, help="并发提交数")
    p.add_argument("--timeout", type=float, default=15.0, help="单请求超时（秒）")
    p.add_argument("--max-retries", type=int, default=2, help="可重试错误(429/5xx/超时)最大重试次数")
    p.add_argument("--limit", type=int, default=0, help="仅提交前 N 条（0=全部；用于冒烟测试）")
    p.add_argument("--start", type=int, default=0, help="跳过前 N 条（断点续跑）")
    p.add_argument("--only-type", default="", help="仅生成指定场景类型（逗号分隔，如 rainy,night）")
    p.add_argument("--output", default=str(DEFAULT_OUTPUT), help="dry-run JSON 输出路径")
    return p


def _filter_types(scenes: list[dict[str, Any]], only: str) -> list[dict[str, Any]]:
    if not only:
        return scenes
    wanted = {t.strip() for t in only.split(",") if t.strip()}
    unknown = wanted - VALID_SCENE_TYPES
    if unknown:
        raise SystemExit(f"[error] 未知场景类型: {sorted(unknown)}；可选: {sorted(VALID_SCENE_TYPES)}")
    return [s for s in scenes if s["scene_type"] in wanted]


def _print_distribution(scenes: list[dict[str, Any]]) -> None:
    by_type: dict[str, int] = {}
    by_cat: dict[str, int] = {}
    extreme = 0
    for s in scenes:
        by_type[s["scene_type"]] = by_type.get(s["scene_type"], 0) + 1
        cat = CATEGORY_BY_TYPE[s["scene_type"]]
        by_cat[cat] = by_cat.get(cat, 0) + 1
        if "extreme" in s["tags"] or "safety" in s["tags"] or "fail-degraded" in s["tags"] or "corner-case" in s["tags"]:
            extreme += 1
    print("\n=== 场景数据集分布 ===")
    print(f"总计: {len(scenes)} 条；覆盖类型: {len(by_type)}/16；极端/安全长尾场景: {extreme} 条")
    print("按分类:")
    for cat in ("basic", "interactive", "environment", "corner_case"):
        if cat in by_cat:
            print(f"  - {cat:<12} {by_cat[cat]:>3} 条")
    print("按类型:")
    for stype in SCENE_QUOTAS:
        if stype in by_type:
            print(f"  - {LABEL_BY_TYPE[stype]:<6}({stype:<20}) {by_type[stype]:>3} 条")


def _dump_scenes(scenes: list[dict[str, Any]], args: argparse.Namespace) -> int:
    """写出 dry-run 产物：目标不可写时自动回退临时目录，仍失败则输出到 stdout（绝不崩溃）。"""
    data = json.dumps(scenes, ensure_ascii=False, indent=2)
    if args.output == "-":
        print(data)
        return 0
    out = Path(args.output)
    fallback = Path(tempfile.gettempdir()) / out.name
    last_err: OSError | None = None
    for cand in (out, fallback):
        try:
            cand.parent.mkdir(parents=True, exist_ok=True)
            cand.write_text(data, encoding="utf-8")
        except OSError as exc:
            last_err = exc
            continue
        print(f"\n[dry-run] 已写出 {len(scenes)} 条场景 → {cand}")
        if cand != out:
            print(
                f"[dry-run] 原目标不可写（{type(last_err).__name__}: {last_err}），已回退到临时目录",
                file=sys.stderr,
            )
            print(
                f"[hint] 若要写入原路径，先确保可写（任选其一）：\n"
                f"       ① sudo mkdir -p {out.parent} && sudo chown -R \"$(id -un)\":\"$(id -gn)\" {out.parent}\n"
                f"       ② 改用可写路径：--output ~/scene_batch_300.json\n"
                f"       ③ 直接输出到 stdout 并重定向：--output - > ~/scene_batch_300.json",
                file=sys.stderr,
            )
        return 0
    print(f"\n[dry-run] 无法写入 {out}（{type(last_err).__name__}: {last_err}）；改为输出到 stdout：", file=sys.stderr)
    print(data)
    return 0


def main(argv: list[str] | None = None) -> int:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            reconfigure(encoding="utf-8", errors="replace")

    args = build_parser().parse_args(argv)

    # 基址默认值（按模式）
    if not args.base_url:
        args.base_url = "http://localhost:8081" if args.mode == "direct" else "http://localhost:8080"
    args.base_url = args.base_url.rstrip("/")

    # gateway 模式校验凭据
    if args.mode == "gateway" and not args.dry_run and not (args.username and args.password):
        print("[error] gateway 模式需 --username/--password（或环境变量）登录", file=sys.stderr)
        return 2
    # direct 模式校验 UUID + 写角色
    if args.mode == "direct" and not args.dry_run:
        try:
            uuid.UUID(args.user_id)
        except ValueError:
            print(f"[error] --user-id 非法 UUID: {args.user_id}", file=sys.stderr)
            return 2
        if not ({"admin", "operator"} & {r.strip() for r in args.roles.split(",")}):
            print("[error] direct 模式 --roles 须包含 admin 或 operator（scene:create 权限）", file=sys.stderr)
            return 2

    scenes = generate_scenes()
    scenes = _filter_types(scenes, args.only_type)
    if args.start:
        scenes = scenes[args.start:]
    if args.limit:
        scenes = scenes[:args.limit]

    _print_distribution(scenes)

    # 规范校验
    passed, errors = validate_scenes(scenes)
    print(f"\n[validate] 通过 {passed}/{len(scenes)} 条（Schema={load_schema_validator() is not None}）")
    if errors:
        print("[validate] 校验失败样本（前 10）:", file=sys.stderr)
        for e in errors[:10]:
            print("  - " + e, file=sys.stderr)
        return 1

    if args.dry_run:
        return _dump_scenes(scenes, args)

    print(f"\n[submit] 开始提交 {len(scenes)} 条场景 → {args.base_url} (mode={args.mode}, concurrency={args.concurrency})")
    stats = asyncio.run(submit_scenes(scenes, args))
    print("\n=== 提交结果 ===")
    print(f"新建: {stats['created']}  幂等跳过(已存在): {stats['existed']}  "
          f"发布: {stats['published']}  失败: {stats['failed_total']}")
    return 1 if stats["failed_total"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
