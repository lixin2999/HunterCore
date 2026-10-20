#!/usr/bin/env python3
"""HunterCore 场景库批量生成脚本（高斯扰动场景生成引擎，重构版）。

设计依据
--------
《场景生成器与场景管理详细设计文档》V1.0（2026-10-20）：
- 二、场景生成器：以「高斯泼溅（Gaussian Perturbation）」为核心技术路线——以场景关键参数的
  统计分布为基底构建多维高斯模型，从分布中心高密度生成**常用场景**、向尾部定向采样生成
  **边缘/长尾场景**（§2.1、§2.3.1 参数化高斯扰动、§2.3.3 长尾定向生成）；
- §2.2.3 场景参数字典：每类场景定义可扰动参数及其取值范围与物理约束；
- §2.4 核心算法：分层采样引擎（§2.4.2）、场景有效性三层校验（§2.4.3）、场景多样性度量与
  去重（§2.4.4、§2.3.1 步骤 6）；
- §2.5 分类体系：功能一级标签（§2.5.1）+ 风险等级 L1~L4（§2.5.2）+ 数据来源（§2.5.3）；
- §5.1 场景元数据：category/sub_category/risk_level/distribution_zone/source 等分类维度。

与旧版（固定 ``i % n`` 轮换）的根本差异
--------------------------------------
旧版 300 个场景由确定性取模轮换拼装，同类型内参与者/事件/天气高度复制 → **场景详情内容雷同**。
本版对每条场景独立执行「按分布区间分层的高斯采样 → 物理可行性校验（拒绝采样）→ 参数向量
余弦相似度去重」，使每条场景的速度/间距/横向偏移/参与者数量/天气强度/事件阈值等**逐条不同**，
并按 60% 常用 / 30% 边缘 / 10% 长尾的比例覆盖风险梯度。

契约映射说明（不改动 DDL / OpenAPI 固定契约）
--------------------------------------------
设计文档 §5.1 的扩展元数据（category / risk_level / distribution_zone / source）落在现有
``scenes`` 已具备且被 GIN 索引、前端已展示的 **tags** 与 **description** 上（受控标签前缀
``cat:`` / ``risk:`` / ``zone:`` / ``src:``），场景仿真参数仍严格写入 ``config_json``（4.2.2 结构，
经 ``SceneCreateRequest`` ``extra="forbid"`` 自校验）。新增独立列需改契约与 ORM 映射，属后续演进项。

用法（仓库根目录；需 Python 3.11+ 启用契约 Schema 校验）::

    # 1) 生成 + 校验 + 落盘 + 多样性报告，不提交（先跑这个看效果）
    python3 scripts/batch_add_scenes.py --dry-run

    # 2) 直连 scene-service 批量入库（先跑 clear-scenes.sh 清空旧数据）
    python3 scripts/batch_add_scenes.py --mode direct \
        --base-url http://localhost:8081 \
        --user-id 00000000-0000-0000-0000-000000000001 --roles admin

    # 3) 经 api-gateway（登录换 JWT 后带 Bearer 提交），并固定随机种子以复现
    python3 scripts/batch_add_scenes.py --mode gateway \
        --base-url http://localhost:8080 --username admin --password 'Admin@12345' \
        --publish --seed 20261020

环境变量（CLI 优先）：HUNTER_BATCH_MODE / HUNTER_BATCH_BASE_URL / HUNTER_BATCH_USER_ID /
HUNTER_BATCH_ROLES / HUNTER_BATCH_GATEWAY_HMAC_SECRET / HUNTER_BATCH_USERNAME / HUNTER_BATCH_PASSWORD。

退出码：0 全部成功（或幂等跳过）；1 存在失败项；2 参数/环境错误。
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import random
import sys
import tempfile
import uuid
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
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
#: Carla 内置城镇（map_type=carla_town）
MAPS = ("Town01", "Town02", "Town03", "Town04", "Town05", "Town06", "Town07")
PI = math.pi

#: 生成分布比例（设计文档 §2.3.1：60% 常用 / 30% 边缘 / 10% 长尾）
ZONE_RATIO: dict[str, float] = {"common": 0.60, "edge": 0.30, "longtail": 0.10}
#: 去重阈值：参数向量余弦相似度 > 该值判为重复，重采样（设计文档 §2.3.1 步骤 6）
DEDUP_COSINE_THRESHOLD = 0.95
#: 单条场景「采样→可行性→去重」最大重试次数（拒绝采样，设计文档 §2.1.2）
MAX_SAMPLE_TRIES = 64


# =====================================================================
# 一、场景类型：配额 + 功能分类 + 风险/分布/来源标签（设计文档 §2.5）
# =====================================================================
SCENE_QUOTAS: OrderedDict[str, int] = OrderedDict(
    [
        ("straight_cruise", 12),
        ("curve_driving", 15),
        ("car_following", 22),
        ("lane_change", 22),
        ("cut_in", 25),
        ("pedestrian_crossing", 28),
        ("intersection_meeting", 22),
        ("construction_detour", 14),
        ("rainy", 22),
        ("night", 20),
        ("foggy", 14),
        ("backlight", 10),
        ("emergency_braking", 24),
        ("obstacle_appearance", 20),
        ("sensor_failure", 12),
        ("multi_vehicle_mixed", 18),
    ]
)
assert sum(SCENE_QUOTAS.values()) == 300, "场景配额之和必须为 300"

#: 类型 → 平台契约分类（4.2.1 两级结构第一级；写入 tags 与 config 校验）
CATEGORY_BY_TYPE: dict[str, str] = {
    "straight_cruise": "basic", "curve_driving": "basic", "car_following": "basic", "lane_change": "basic",
    "cut_in": "interactive", "pedestrian_crossing": "interactive", "intersection_meeting": "interactive",
    "construction_detour": "interactive",
    "rainy": "environment", "night": "environment", "foggy": "environment", "backlight": "environment",
    "emergency_braking": "corner_case", "obstacle_appearance": "corner_case", "sensor_failure": "corner_case",
    "multi_vehicle_mixed": "corner_case",
}

#: 类型 → 设计文档 §2.5.1 功能一级标签（cat: 标签，与契约分类互补）
FUNC_CATEGORY: dict[str, str] = {
    "car_following": "following", "cut_in": "following", "lane_change": "following", "emergency_braking": "following",
    "pedestrian_crossing": "avoidance", "obstacle_appearance": "avoidance", "construction_detour": "avoidance",
    "multi_vehicle_mixed": "avoidance",
    "straight_cruise": "road-structure", "curve_driving": "road-structure", "intersection_meeting": "road-structure",
    "rainy": "environment", "night": "environment", "foggy": "environment", "backlight": "environment",
    "sensor_failure": "anomaly",
}

#: 类型 → 中文标签（契约 SCENE_TYPE_LABELS 同源）
LABEL_BY_TYPE: dict[str, str] = {
    "straight_cruise": "直线巡航", "curve_driving": "弯道行驶", "car_following": "跟车行驶", "lane_change": "变道行驶",
    "cut_in": "前车切入", "pedestrian_crossing": "行人横穿", "intersection_meeting": "路口会车",
    "construction_detour": "施工绕行", "rainy": "雨天", "night": "夜间", "foggy": "雾天", "backlight": "强光逆光",
    "emergency_braking": "紧急制动", "obstacle_appearance": "障碍物突现", "sensor_failure": "传感器失效",
    "multi_vehicle_mixed": "多车混行",
}

#: 类型 → 名称缩写（保证 scene_name 全局唯一且可读）
ABBR_BY_TYPE: dict[str, str] = {
    "straight_cruise": "CRS", "curve_driving": "CRV", "car_following": "FOL", "lane_change": "LNC",
    "cut_in": "CUT", "pedestrian_crossing": "PED", "intersection_meeting": "INT", "construction_detour": "CST",
    "rainy": "RUN", "night": "NGT", "foggy": "FOG", "backlight": "GLR", "emergency_braking": "EBR",
    "obstacle_appearance": "OBS", "sensor_failure": "SNF", "multi_vehicle_mixed": "MVM",
}

#: 类型 → 功能能力标签（写入 tags，供场景库筛选）
FUNCTION_TAGS: dict[str, tuple[str, ...]] = {
    "straight_cruise": ("control", "cruise"), "curve_driving": ("control", "lateral"),
    "car_following": ("planning", "longitudinal"), "lane_change": ("planning", "lateral"),
    "cut_in": ("prediction", "interaction"), "pedestrian_crossing": ("perception", "vru"),
    "intersection_meeting": ("planning", "decision"), "construction_detour": ("perception", "static-avoid"),
    "rainy": ("robustness", "wet-road"), "night": ("robustness", "low-light"),
    "foggy": ("robustness", "low-visibility"), "backlight": ("robustness", "glare"),
    "emergency_braking": ("safety", "aeb"), "obstacle_appearance": ("safety", "corner-case"),
    "sensor_failure": ("safety", "fail-degraded"), "multi_vehicle_mixed": ("perception", "complex-traffic"),
}

#: 分布区间 → 风险等级（设计文档 §2.5.2：L1 常规 / L2 边缘 / L3 危险 / L4 极端）
RISK_BY_ZONE: dict[str, str] = {"common": "L1", "edge": "L2", "longtail": "L4"}


# =====================================================================
# 二、场景参数字典（设计文档 §2.2.3）：每参数 (mean, std, lo, hi, risk)
#     risk: +1 值越大越危险 / -1 值越小越危险 / 0 中性；长尾按风险方向做「极值组合」
# =====================================================================
@dataclass(frozen=True)
class Param:
    mean: float
    std: float
    lo: float
    hi: float
    risk: int = 0
    discrete: bool = False


#: 所有场景共享的天气/时长子向量（保证逐条天气多样——旧版雷同的直接修复点）
SHARED_PARAMS: dict[str, Param] = {
    "rain": Param(12, 26, 0, 100, +1),
    "fog": Param(8, 18, 0, 100, +1),
    "wind": Param(15, 25, 0, 100, +1),
    "sun_alt": Param(40, 35, -25, 90, -1),   # 越低越暗越危险
    "sun_az": Param(120, 100, 0, 360, 0),
    "duration": Param(80, 45, 30, 300, 0),
}

#: 类型专属几何/动力学参数（覆盖/追加共享段）
TYPE_PARAMS: dict[str, dict[str, Param]] = {
    "straight_cruise": {"ego_speed": Param(16, 7, 2, 24, +1)},
    "curve_driving": {"ego_speed": Param(6, 2.2, 2, 11, +1), "steer_mag": Param(0.5, 0.28, 0.1, 0.95, +1)},
    "car_following": {
        "ego_speed": Param(14, 6, 4, 24, +1), "gap": Param(16, 7, 6, 32, -1),
        "lead_ratio": Param(0.85, 0.12, 0.5, 1.05, 0),
    },
    "lane_change": {
        "ego_speed": Param(12, 6, 4, 22, +1), "lat": Param(3.5, 0.4, 3.0, 4.2, 0),
        "complete_in": Param(5, 1.6, 3, 9, -1), "target_ratio": Param(1.05, 0.12, 0.8, 1.3, 0),
    },
    "cut_in": {
        "ego_speed": Param(18, 5, 10, 24, +1), "cut_gap": Param(28, 8, 15, 42, -1),
        "lat": Param(3.5, 0.5, 2.5, 4.5, -1), "aggr": Param(2.0, 1.3, 0.5, 5.0, +1),
    },
    "pedestrian_crossing": {
        "ego_speed": Param(6, 2.6, 2, 11, +1), "n_ped": Param(2, 1.3, 1, 5, +1, discrete=True),
        "ped_speed": Param(1.6, 0.6, 0.8, 2.6, +1), "cross_dist": Param(22, 7, 10, 36, -1),
    },
    "intersection_meeting": {
        "ego_speed": Param(6, 2.2, 3, 11, +1), "cross_speed": Param(6.5, 2.4, 3, 11, +1),
        "cross_dist": Param(30, 9, 15, 52, -1),
    },
    "construction_detour": {
        "ego_speed": Param(6, 2.2, 3, 10, +1), "n_bar": Param(3, 1.4, 2, 6, +1, discrete=True),
        "bar_dist": Param(16, 6, 8, 30, -1),
    },
    "rainy": {
        "ego_speed": Param(6, 2.4, 2, 11, +1),
        "rain": Param(72, 24, 25, 100, +1), "fog": Param(18, 15, 0, 60, +1),
    },
    "night": {
        "ego_speed": Param(6, 2.4, 2, 11, +1), "sun_alt": Param(-20, 10, -45, 2, -1),
        "n_ped": Param(1, 1.0, 0, 3, +1, discrete=True),
    },
    "foggy": {
        "ego_speed": Param(4, 1.6, 2, 8, +1), "fog": Param(82, 16, 45, 100, +1),
        "vis_dist": Param(14, 6, 6, 32, -1),
    },
    "backlight": {
        "ego_speed": Param(6, 2.2, 2, 11, +1), "sun_alt": Param(5, 4, -2, 16, +1),
        "sun_az": Param(0, 30, 0, 360, 0), "rain": Param(4, 8, 0, 30, 0),
    },
    "emergency_braking": {
        "ego_speed": Param(16, 6, 8, 24, +1), "gap": Param(13, 5, 6, 24, -1),
        "decel": Param(6, 2.2, 3, 9.5, +1), "ttc": Param(1.6, 0.8, 0.8, 3.2, -1),
    },
    "obstacle_appearance": {
        "ego_speed": Param(9, 4, 3, 18, +1), "appear_x": Param(15, 5, 8, 24, -1),
        "kind_idx": Param(2, 1.6, 0, 4, 0, discrete=True),
    },
    "sensor_failure": {
        "ego_speed": Param(6, 2.4, 2, 11, +1), "fail_x": Param(11, 5, 4, 22, -1),
        "sensor_idx": Param(2, 1.6, 0, 4, 0, discrete=True), "fault_idx": Param(2, 1.6, 0, 4, 0, discrete=True),
    },
    "multi_vehicle_mixed": {
        "ego_speed": Param(6, 2.4, 2, 11, +1), "n_v": Param(5, 1.6, 3, 7, +1, discrete=True),
        "n_p": Param(2, 1.2, 1, 4, +1, discrete=True), "spread": Param(7, 3, 3, 14, +1),
    },
}

_OBSTACLE_KINDS = ("debris", "cone", "animal", "fallen-load", "tyre")
_SENSORS = ("camera_front", "lidar_top", "radar_front", "camera_rear", "gnss")
_FAULTS = ("black_frame", "point_dropout", "ghost_return", "freeze", "denied")
_CURVES = ("left", "right", "s-ramp", "hairpin")
_MANEUVERS = ("straight", "left-turn", "right-turn", "uturn")


# =====================================================================
# 三、高斯分层采样引擎（设计文档 §2.4.2 分层采样 + §2.3.3 长尾定向）
# =====================================================================
def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _zone_magnitude(rng: random.Random, zone: str) -> float:
    """按分布区间抽取 |z| 倍数（常用 ±1σ / 边缘 1~3σ / 长尾 >3σ，§2.1.1）。"""
    if zone == "common":
        return rng.uniform(0.0, 1.0)
    if zone == "edge":
        return rng.uniform(1.0, 3.0)
    # longtail：重要性采样向尾部偏移（3~4.5σ，再按物理边界裁剪）
    return rng.uniform(3.0, 4.5)


def _sample_param(rng: random.Random, name: str, p: Param, zone: str, coherent: int) -> float:
    """单参数高斯抽样；长尾沿风险方向做极值组合（§2.3.3），常用/边缘部分独立。"""
    mag = _zone_magnitude(rng, zone)
    if p.risk != 0 and zone == "longtail":
        sign = coherent * p.risk                 # 多参数同向推向极限
    elif p.risk != 0 and zone == "edge" and rng.random() < 0.5:
        sign = coherent * p.risk
    else:
        sign = rng.choice((-1, 1))
    val = _clamp(p.mean + sign * mag * p.std, p.lo, p.hi)
    if p.discrete:
        val = float(int(round(val)))
        val = _clamp(val, p.lo, p.hi)
    return val


def _sample_vector(rng: random.Random, spec: dict[str, Param], zone: str) -> dict[str, float]:
    coherent = rng.choice((-1, 1))
    return {name: _sample_param(rng, name, p, zone, coherent) for name, p in spec.items()}


def _normalize_vector(spec: dict[str, Param], params: dict[str, float]) -> list[float]:
    """参数向量归一化到 [0,1]（用于余弦相似度去重，§2.4.4）。"""
    vec: list[float] = []
    for name, p in spec.items():
        v = params[name]
        vec.append((v - p.lo) / (p.hi - p.lo) if p.hi > p.lo else 0.0)
    return vec


def _cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0 or nb == 0:
        return 1.0
    return dot / (na * nb)


def _feasible(stype: str, p: dict[str, float]) -> bool:
    """物理可行性/初始状态校验（§2.4.3 第 2/3 层）：核心是初始不碰撞、运动连续。"""
    speed = p.get("ego_speed", 0.0)
    stop_dist = speed * 0.6 + 3.0            # 简化制动距离
    if stype in {"car_following", "emergency_braking"} and p["gap"] < stop_dist:
        return False
    if stype == "cut_in" and p["cut_gap"] < stop_dist:
        return False
    if stype == "pedestrian_crossing" and p["cross_dist"] < stop_dist:
        return False
    if stype == "obstacle_appearance" and p["appear_x"] < stop_dist:
        return False
    if stype == "foggy" and p["vis_dist"] < stop_dist:
        return False
    if stype == "sensor_failure" and p["fail_x"] < stop_dist:
        return False
    return True


def _zone_counts(total: int, ratio: dict[str, float]) -> OrderedDict[str, int]:
    """按比例把类型配额拆到三个分布区间，舍入余数补给 common（和恒等于 total）。"""
    keys = ("common", "edge", "longtail")
    counts = {k: int(round(total * ratio.get(k, 0.0))) for k in keys}
    counts["common"] += total - sum(counts.values())   # 舍入余数归常用
    if counts["common"] < 0:                            # 极端舍入致常用为负时回补
        deficit = -counts["common"]
        counts["common"] = 0
        for k in ("edge", "longtail"):
            take = min(counts[k], deficit)
            counts[k] -= take
            deficit -= take
            if deficit <= 0:
                break
    return OrderedDict((k, counts[k]) for k in keys)


# =====================================================================
# 四、4.2.2 结构构造 helper（全部返回 dict，落库前经 SceneCreateRequest 校验）
# =====================================================================
def _spawn(x: float, y: float = 0.0, yaw: float = 0.0, z: float = 0.0, lane_id: str | None = None) -> dict[str, Any]:
    return {"x": round(x, 3), "y": round(y, 3), "z": round(z, 3), "yaw": round(yaw, 5), "lane_id": lane_id}


def _actor(aid: str, atype: str, spawn: dict[str, Any], behavior: str, **params: Any) -> dict[str, Any]:
    return {"actor_id": aid, "type": atype, "spawn_point": spawn,
            "behavior": {"behavior_type": behavior, "parameters": dict(params)}}


def _vehicle(aid: str, x: float, y: float, behavior: str, yaw: float = 0.0, **params: Any) -> dict[str, Any]:
    return _actor(aid, "vehicle", _spawn(x, y, yaw), behavior, **params)


def _pedestrian(aid: str, x: float, y: float, speed: float = 1.4, yaw: float = PI / 2) -> dict[str, Any]:
    return _actor(aid, "pedestrian", _spawn(x, y, yaw), "cross_road", crossing_speed=round(speed, 2))


def _static(aid: str, x: float, y: float, kind: str = "debris") -> dict[str, Any]:
    return _actor(aid, "other", _spawn(x, y, 0.0), "static", object_kind=kind)


def _event(eid: str, etype: str, condition: str, action: str, **action_params: Any) -> dict[str, Any]:
    return {"event_id": eid, "type": etype,
            "trigger": {"condition": condition, "parameters": {"source": "gaussian-batch"}},
            "action": {"action_type": action, "parameters": dict(action_params)}}


def _build_weather(p: dict[str, float]) -> dict[str, float]:
    """由采样参数装配 Carla 7 字段天气段（严格落范围）。"""
    rain = p["rain"]
    fog = p["fog"]
    cloud = _clamp(rain * 0.7 + fog * 0.4 + 10.0, 0.0, 100.0)
    return {
        "cloudiness": round(cloud, 1),
        "rain": round(rain, 1),
        "wetness": round(_clamp(rain * 0.9, 0.0, 100.0), 1),
        "fog": round(fog, 1),
        "wind": round(p["wind"], 1),
        "sun_azimuth": round(p["sun_az"], 1),
        "sun_altitude": round(_clamp(p["sun_alt"], -90.0, 90.0), 1),
    }


def _derive_criteria(stype: str, p: dict[str, float]) -> tuple[float, float]:
    """由速度/类型推导 (min_safe, max_dev)，随场景强度自适应。"""
    speed = p.get("ego_speed", 6.0)
    base = {"emergency_braking": 2.0, "obstacle_appearance": 2.5, "cut_in": 6.0, "car_following": 4.0}.get(stype, 3.5)
    min_safe = _clamp(base + speed * 0.25, 1.0, 25.0)
    max_dev = _clamp(1.0 + speed * 0.05, 0.5, 3.5)
    return round(min_safe, 2), round(max_dev, 2)


def _config(
    *, map_id: str, ego_speed: float, ego_steer: float, weather: dict[str, float],
    actors: list[dict[str, Any]], events: list[dict[str, Any]], duration: float,
    min_safe: float, max_dev: float, no_collision: bool = True,
) -> dict[str, Any]:
    return {
        "map": {"map_id": map_id, "map_type": "carla_town",
                "spawn_point": _spawn(0.0, 0.0, 0.0, lane_id="lane-1")},
        "ego_vehicle": {"model": EGO_MODEL, "initial_speed": round(max(0.0, ego_speed), 3),
                        "initial_steer": round(_clamp(ego_steer, -1.0, 1.0), 3)},
        "weather": weather,
        "actors": actors,
        "events": events,
        "success_criteria": {"max_speed_deviation": max_dev, "no_collision": no_collision,
                             "min_safe_distance": min_safe},
        "duration": round(min(duration, DURATION_MAX), 1),
    }


# =====================================================================
# 五、各类型装配器：assembler(p) -> (actors, events, ego_steer, token)
#     数值全部来自高斯采样参数 p，逐条不同；结构保持语义正确（参与者/事件类型合法）
# =====================================================================
Assembler = Callable[[dict[str, float]], tuple[list[dict[str, Any]], list[dict[str, Any]], float, str]]


def _a_straight(p):
    speed = p["ego_speed"]
    actors = [_vehicle("bg-vehicle", 60.0 + speed * 3, 3.5, "lane_follow", target_speed=round(speed * 0.9, 2))]
    events = [_event("ev-cruise", "emergency_brake", f"ego.distance.headway < {round(2.0 + speed * 0.05, 2)}", "hold_speed")]
    return actors, events, 0.0, f"v{speed:.1f}"


def _a_curve(p):
    speed, steer = p["ego_speed"], p["steer_mag"]
    curve = _CURVES[int(round((steer - 0.1) / 0.85 * 3)) % 4]
    signed = -steer if curve in ("left", "hairpin") else steer
    actors = [_vehicle("oncoming", 50.0 + speed * 3, -3.5, "lane_follow", yaw=PI, target_speed=round(speed * 0.8, 2))]
    events = [_event("ev-curve-limit", "collision", f"ego.lateral.accel > {round(3.0 + steer * 3, 2)}", "speed_limit", target_speed=round(speed, 2))]
    return actors, events, signed, f"{curve}-s{steer:.2f}"


def _a_follow(p):
    speed, gap, ratio = p["ego_speed"], p["gap"], p["lead_ratio"]
    actors = [_vehicle("lead-vehicle", gap, 0.0, "lane_follow", target_speed=round(speed * ratio, 2))]
    events = [_event("ev-lead-brake", "emergency_brake", f"ego.distance.lead < {round(gap * 0.5, 2)}", "brake", deceleration=round(3.0 + speed * 0.15, 2))]
    return actors, events, 0.0, f"g{gap:.1f}m-v{speed:.1f}"


def _a_lane_change(p):
    speed, lat, cin, tratio = p["ego_speed"], p["lat"], p["complete_in"], p["target_ratio"]
    direction = "left" if lat > 3.5 else "right"
    y = -lat if direction == "left" else lat
    actors = [
        _vehicle("target-gap", 18.0, y, "lane_follow", target_speed=round(speed * tratio, 2)),
        _vehicle("behind", -6.0, y, "constant_velocity", target_speed=round(speed * 1.1, 2)),
    ]
    events = [_event("ev-lane-change", "cut_in", f"ego.position.x > {round(10 + speed, 1)}", "lane_change", target_lane=direction, complete_in=round(cin, 2))]
    return actors, events, 0.0, f"{direction}-t{cin:.1f}s"


def _a_cut_in(p):
    speed, gap, lat, aggr = p["ego_speed"], p["cut_gap"], p["lat"], p["aggr"]
    side = -lat if int(gap) % 2 == 0 else lat
    actors = [
        _vehicle("cut-in-vehicle", gap, side, "cut_in", target_lane="ego_lane", cut_in_speed=round(aggr, 2)),
        _vehicle("lead", gap + 15.0, 0.0, "lane_follow", target_speed=round(speed * 0.85, 2)),
    ]
    events = [_event("ev-cut-in", "cut_in", f"ego.position.x > {round(gap - 5, 1)}", "speed_change", target_speed=round(speed * 0.6, 2))]
    return actors, events, 0.0, f"agg{aggr:.1f}-d{gap:.1f}"


def _a_pedestrian(p):
    speed, n, ps, dist = p["ego_speed"], int(p["n_ped"]), p["ped_speed"], p["cross_dist"]
    actors = [_pedestrian(f"ped-{k + 1}", dist + k * 1.5, -6.0 - k * 0.6, speed=round(ps + 0.1 * k, 2)) for k in range(n)]
    events = [_event("ev-ped-crossing", "pedestrian_crossing", f"ego.position.x > {round(dist - 5, 1)}", "stop", min_safe_distance=2.0)]
    return actors, events, 0.0, f"{n}ped-{ps:.1f}"


def _a_intersection(p):
    speed, cs, dist = p["ego_speed"], p["cross_speed"], p["cross_dist"]
    maneuver = _MANEUVERS[int(round(speed)) % 4]
    actors = [
        _vehicle("cross-vehicle-1", dist, -8.0, "forward", yaw=PI / 2, target_speed=round(cs, 2)),
        _vehicle("cross-vehicle-2", dist, 8.0, "forward", yaw=-PI / 2, target_speed=round(cs * 0.9, 2)),
        _vehicle("opposite", dist + 15.0, 0.0, "lane_follow", yaw=PI, target_speed=round(cs, 2)),
    ]
    events = [_event("ev-signal", "collision", f"ego.position.x > {round(dist * 0.4, 1)}", "traffic_light", phase=("green" if int(speed) % 2 else "yellow"))]
    return actors, events, 0.0, f"{maneuver}-cd{dist:.0f}"


def _a_construction(p):
    speed, n, dist = p["ego_speed"], int(p["n_bar"]), p["bar_dist"]
    actors = [_static(f"barrier-{k + 1}", dist + k * 4.0, 1.8, kind="construction") for k in range(n)]
    actors.append(_vehicle("worker-vehicle", dist + n * 4.0 + 8.0, 3.5, "static"))
    events = [_event("ev-detour", "obstacle_appearance", f"ego.position.x > {round(dist - 6, 1)}", "lane_change", target_lane="left", complete_in=6.0)]
    return actors, events, 0.0, f"{n}bar-d{dist:.0f}"


def _a_rain(p):
    speed = p["ego_speed"]
    actors = [_vehicle("lead-vehicle", 18.0, 0.0, "lane_follow", target_speed=round(speed * 0.8, 2))]
    events = [_event("ev-wet-brake", "emergency_brake", f"ttc < {round(2.5 - speed * 0.03, 2)}", "brake", deceleration=round(3.0 + p["rain"] * 0.02, 2))]
    return actors, events, 0.0, f"r{p['rain']:.0f}"


def _a_night(p):
    speed, n = p["ego_speed"], int(p["n_ped"])
    actors = [_vehicle("lead-vehicle", 22.0, 0.0, "lane_follow", target_speed=round(speed * 0.85, 2))]
    actors += [_pedestrian(f"ped-dark-{k + 1}", 26.0 + k * 2.0, -4.0) for k in range(n)]
    events = [_event("ev-night-detect", "obstacle_appearance", f"ego.position.x > {round(12 + speed, 1)}", "brake", deceleration=2.5)]
    return actors, events, 0.0, f"alt{p['sun_alt']:.0f}-{n}ped"


def _a_fog(p):
    speed, vis = p["ego_speed"], p["vis_dist"]
    actors = [_vehicle("lead-emerge", vis, 0.0, "lane_follow", target_speed=round(speed * 0.7, 2))]
    events = [_event("ev-fog-appear", "obstacle_appearance", f"ego.position.x > {round(vis - 6, 1)}", "emergency_stop")]
    return actors, events, 0.0, f"fog{p['fog']:.0f}-v{vis:.0f}"


def _a_backlight(p):
    speed = p["ego_speed"]
    actors = [_vehicle("lead-vehicle", 20.0, 0.0, "lane_follow", target_speed=round(speed * 0.9, 2))]
    actors.append(_static("roadside", 24.0, 3.0, kind="sign"))
    events = [_event("ev-glare", "pedestrian_crossing", f"ego.position.x > {round(10 + speed, 1)}", "hold_speed")]
    return actors, events, 0.0, f"alt{p['sun_alt']:.0f}-az{p['sun_az']:.0f}"


def _a_emergency(p):
    speed, gap, decel, ttc = p["ego_speed"], p["gap"], p["decel"], p["ttc"]
    actors = [_vehicle("lead-hard-brake", gap, 0.0, "lane_follow", target_speed=round(speed * 0.5, 2))]
    events = [_event("ev-aeb", "emergency_brake", f"ttc < {round(ttc, 2)}", "emergency_stop", deceleration=round(decel, 2))]
    return actors, events, 0.0, f"dec{decel:.1f}-ttc{ttc:.1f}"


def _a_obstacle(p):
    speed, ax = p["ego_speed"], p["appear_x"]
    kind = _OBSTACLE_KINDS[int(p["kind_idx"]) % len(_OBSTACLE_KINDS)]
    actors = [_static("sudden-obstacle", ax, 0.4, kind=kind)]
    events = [_event("ev-obstacle", "obstacle_appearance", f"ego.position.x > {round(ax - 4, 1)}", "emergency_stop")]
    return actors, events, 0.0, f"{kind}-x{ax:.0f}"


def _a_sensor(p):
    speed, fx = p["ego_speed"], p["fail_x"]
    sensor = _SENSORS[int(p["sensor_idx"]) % len(_SENSORS)]
    fault = _FAULTS[int(p["fault_idx"]) % len(_FAULTS)]
    actors = [_vehicle("lead-vehicle", 20.0, 0.0, "lane_follow", target_speed=round(speed * 0.8, 2))]
    events = [_event("ev-sensor-fail", "sensor_failure", f"ego.position.x > {round(fx, 1)}", "degrade_control", sensor=sensor, fault=fault, fallback="limp_home")]
    return actors, events, 0.0, f"{sensor}-{fault}"


def _a_multi(p):
    speed, nv, np_, spread = p["ego_speed"], int(p["n_v"]), int(p["n_p"]), p["spread"]
    actors: list[dict[str, Any]] = []
    for k in range(nv):
        lat = (3.5, -3.5)[k % 2]
        actors.append(_vehicle(f"veh-{k + 1}", spread + k * 6.0, lat, ("lane_follow", "cut_in")[k % 2], target_speed=round(speed * (0.8 + 0.1 * k), 2)))
    for k in range(np_):
        actors.append(_pedestrian(f"ped-{k + 1}", spread * 2 + k * 2.0, -6.0 - k))
    actors.append(_static("roadside-obj", spread * 3 + 10.0, 4.5, kind="bin"))
    events = [
        _event("ev-mix-cutin", "cut_in", f"ego.position.x > {round(spread * 2, 1)}", "speed_change", target_speed=round(speed * 0.5, 2)),
        _event("ev-mix-ped", "pedestrian_crossing", f"ego.position.x > {round(spread * 3 + 6, 1)}", "yield"),
    ]
    return actors, events, 0.0, f"{nv}v{np_}p-s{spread:.0f}"


ASSEMBLERS: dict[str, Assembler] = {
    "straight_cruise": _a_straight, "curve_driving": _a_curve, "car_following": _a_follow,
    "lane_change": _a_lane_change, "cut_in": _a_cut_in, "pedestrian_crossing": _a_pedestrian,
    "intersection_meeting": _a_intersection, "construction_detour": _a_construction, "rainy": _a_rain,
    "night": _a_night, "foggy": _a_fog, "backlight": _a_backlight, "emergency_braking": _a_emergency,
    "obstacle_appearance": _a_obstacle, "sensor_failure": _a_sensor, "multi_vehicle_mixed": _a_multi,
}
assert set(ASSEMBLERS) == set(SCENE_QUOTAS), "装配器必须覆盖全部配额场景类型"


# =====================================================================
# 六、组装 300 条场景（分层采样 + 可行性拒绝 + 余弦去重 + 唯一命名）
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


def _make_scene(
    stype: str, spec: dict[str, Param], zone: str, rng: random.Random, accepted: list[list[float]],
) -> tuple[dict[str, Any], dict[str, float], str, int]:
    """采样一条通过可行性 + 去重的场景；返回 (config, params, token, tries)。"""
    last: tuple[dict[str, float], bool] | None = None
    for attempt in range(1, MAX_SAMPLE_TRIES + 1):
        params = _sample_vector(rng, spec, zone)
        if not _feasible(stype, params):
            continue
        vec = _normalize_vector(spec, params)
        if any(_cosine(vec, a) > DEDUP_COSINE_THRESHOLD for a in accepted):
            last = (params, False)
            continue
        accepted.append(vec)
        cfg = _assemble_config(stype, params, zone)
        token = ASSEMBLERS[stype](params)[3]
        return cfg, params, token, attempt
    # 重试耗尽：接受最后一次可行样本（或强制放宽），保证数量恒等于配额
    params = last[0] if last and last[0] else _sample_vector(rng, spec, "common")
    cfg = _assemble_config(stype, params, zone)
    token = ASSEMBLERS[stype](params)[3]
    return cfg, params, token, MAX_SAMPLE_TRIES


def _assemble_config(stype: str, params: dict[str, float], zone: str) -> dict[str, Any]:
    actors, events, steer, _token = ASSEMBLERS[stype](params)
    min_safe, max_dev = _derive_criteria(stype, params)
    spec = {**SHARED_PARAMS, **TYPE_PARAMS[stype]}
    map_id = MAPS[hash(tuple(round(v, 2) for v in params.values())) % len(MAPS)]
    return _config(
        map_id=map_id, ego_speed=params.get("ego_speed", 6.0), ego_steer=steer,
        weather=_build_weather({**spec_default_weather(spec), **params}),
        actors=actors, events=events, duration=params["duration"],
        min_safe=min_safe, max_dev=max_dev, no_collision=True,
    )


def spec_default_weather(spec: dict[str, Param]) -> dict[str, float]:
    """补齐天气装配所需键（当某类型未显式采样该参数时用其均值兜底）。"""
    keys = ("rain", "fog", "wind", "sun_alt", "sun_az")
    return {k: spec[k].mean for k in keys if k in spec}


def generate_scenes(seed: int | None = None, ratio: dict[str, float] | None = None) -> list[dict[str, Any]]:
    """按配额 + 高斯分层采样生成 300 条场景载荷（字段严格对齐 SceneCreateRequest）。"""
    rng = random.Random(seed)
    zone_ratio = ratio or ZONE_RATIO
    scenes: list[dict[str, Any]] = []
    stats = {"tries_total": 0, "dedup_drop": 0}
    g = 0
    for stype, count in SCENE_QUOTAS.items():
        spec = {**SHARED_PARAMS, **TYPE_PARAMS[stype]}
        accepted: list[list[float]] = []
        zone_counts = _zone_counts(count, zone_ratio)
        per_type: list[dict[str, Any]] = []
        for zone, n in zone_counts.items():
            for _ in range(n):
                cfg, params, token, tries = _make_scene(stype, spec, zone, rng, accepted)
                stats["tries_total"] += tries
                stats["dedup_drop"] += max(0, tries - 1)
                g += 1
                risk = RISK_BY_ZONE[zone]
                name = f"SCN-{g:03d}-{ABBR_BY_TYPE[stype]}-{risk}-{token[:24]}"[:128]
                tags = _dedup_tags([
                    f"cat:{FUNC_CATEGORY[stype]}", CATEGORY_BY_TYPE[stype], stype,
                    f"risk:{risk}", f"zone:{zone}", "src:synthetic",
                    "ad-training", "carla", *FUNCTION_TAGS[stype],
                ])
                w = cfg["weather"]
                description = (
                    f"[{LABEL_BY_TYPE[stype]}] {FUNC_CATEGORY[stype]}/{CATEGORY_BY_TYPE[stype]} 类"
                    f"高斯扰动训练场景（风险 {risk}·{zone} 区）。"
                    f"地图 {cfg['map']['map_id']}，自车初速 {cfg['ego_vehicle']['initial_speed']}m/s，"
                    f"时长 {cfg['duration']}s；天气 雨{w['rain']}/雾{w['fog']}/风{w['wind']}"
                    f"/高度角{w['sun_altitude']}；参与者 {len(cfg['actors'])}，事件 {len(cfg['events'])}。"
                )[:1024]
                per_type.append({
                    "scene_name": name, "scene_type": stype, "description": description,
                    "tags": tags, "config": cfg,
                })
        scenes.extend(per_type)
    generate_scenes.stats = stats  # type: ignore[attr-defined]
    return scenes


# =====================================================================
# 七、场景库规范校验（优先用 4.2.2 Schema；不可导入时降级为轻量内部校验）
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
    if not 1 <= len(payload["scene_name"]) <= 128:
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
    if not 0 < cfg["duration"] <= DURATION_MAX:
        raise ValueError(f"duration={cfg['duration']} 越界(0,{DURATION_MAX}]")
    sc = cfg["success_criteria"]
    if sc["max_speed_deviation"] < 0 or sc["min_safe_distance"] < 0:
        raise ValueError("success_criteria 取值越界")


_SCHEMA_VALIDATOR: Any = None
_SCHEMA_VALIDATOR_LOADED = False


def load_schema_validator() -> Any:
    """尝试导入 4.2.2 契约 Schema（SceneCreateRequest）；失败返回 None（缓存，只告警一次）。"""
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


def diversity_report(scenes: list[dict[str, Any]]) -> None:
    """打印多样性摘要：唯一 config 指纹数 / 平均参与者 / 平均事件 / 采样重试。"""
    fp: set[str] = set()
    actor_ct: dict[str, int] = {}
    for s in scenes:
        fp.add(json.dumps(s["config"], sort_keys=True, ensure_ascii=False))
        actor_ct[s["scene_type"]] = actor_ct.get(s["scene_type"], 0) + len(s["config"]["actors"])
    stats = getattr(generate_scenes, "stats", {})
    print("\n=== 多样性摘要（重构核心指标）===")
    print(f"场景总数: {len(scenes)}；唯一 config 指纹: {len(fp)}（旧版固定轮换远低于此）")
    print(f"平均参与者/场景: {sum(actor_ct.values()) / max(1, len(scenes)):.2f}")
    print(f"采样总尝试: {stats.get('tries_total', 'NA')}；因去重/可行性重采样: {stats.get('dedup_drop', 'NA')} 次")


# =====================================================================
# 八、认证与批量提交（direct：X-User-Id/X-Roles[可选 HMAC 验签]；gateway：JWT Bearer）
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


class _RateLimiter:
    """全局请求节流：把发起速率压在网关每用户 QPS 阈值之下，避免 429。"""

    def __init__(self, qps: float) -> None:
        self._interval = (1.0 / qps) if qps and qps > 0 else 0.0
        self._lock = asyncio.Lock()
        self._next = 0.0

    async def acquire(self) -> None:
        if self._interval <= 0:
            return
        async with self._lock:
            now = asyncio.get_running_loop().time()
            wait = self._next - now
            if wait > 0:
                await asyncio.sleep(wait)
                now = self._next
            self._next = now + self._interval


def _retry_delay(resp: Any, attempt: int, args: argparse.Namespace) -> float:
    if resp is not None and getattr(resp, "status_code", None) == 429:
        ra = resp.headers.get("Retry-After")
        if ra:
            try:
                return min(float(ra), args.retry_max)
            except (TypeError, ValueError):
                pass
    return min(args.backoff * (2 ** attempt), args.retry_max)


async def _post_scene(
    client: Any, sem: Any, payload: dict[str, Any], headers: Callable[[], dict[str, str]],
    args: argparse.Namespace, stats: dict[str, int], failures: list[str], limiter: _RateLimiter,
) -> None:
    url = f"{args.base_url}/api/v1/scene"
    last_err = ""
    for attempt in range(args.max_retries + 1):
        await limiter.acquire()
        async with sem:
            try:
                resp = await client.post(url, json=payload, headers=headers(), timeout=args.timeout)
            except Exception as exc:  # noqa: BLE001 - 传输层异常统一重试
                last_err = f"transport: {exc}"
                await asyncio.sleep(_retry_delay(None, attempt, args))
                continue
        body = _safe_json(resp) or {}
        code = body.get("code")
        name = payload["scene_name"]
        if resp.status_code in (200, 201) and code == 0:
            stats["created"] += 1
            scene_id = (body.get("data") or {}).get("scene_id")
            if args.publish and scene_id:
                await _publish_scene(client, sem, headers, args, scene_id, name, stats, failures, limiter)
            return
        if code == 3002 or resp.status_code == 409:
            stats["existed"] += 1  # 幂等：名称已存在视为已入库
            return
        if resp.status_code in (429, 500, 502, 503, 504):
            last_err = f"HTTP {resp.status_code} code={code} {body.get('message')}"
            await asyncio.sleep(_retry_delay(resp, attempt, args))
            continue
        failures.append(f"{name}: HTTP {resp.status_code} code={code} msg={body.get('message')}")
        stats["failed"] += 1
        return
    failures.append(f"{name}: 重试耗尽 last={last_err}")
    stats["failed"] += 1


async def _publish_scene(
    client: Any, sem: Any, headers: Callable[[], dict[str, str]], args: argparse.Namespace,
    scene_id: str, name: str, stats: dict[str, int], failures: list[str], limiter: _RateLimiter,
) -> None:
    last_err = ""
    for attempt in range(args.max_retries + 1):
        await limiter.acquire()
        async with sem:
            try:
                resp = await client.post(f"{args.base_url}/api/v1/scene/{scene_id}/publish",
                                         headers=headers(), timeout=args.timeout)
            except Exception as exc:  # noqa: BLE001
                last_err = f"transport: {exc}"
                await asyncio.sleep(_retry_delay(None, attempt, args))
                continue
        body = _safe_json(resp) or {}
        if resp.status_code in (200, 201) and body.get("code") == 0:
            stats["published"] += 1
            return
        if resp.status_code in (429, 500, 502, 503, 504):
            last_err = f"HTTP {resp.status_code} code={body.get('code')}"
            await asyncio.sleep(_retry_delay(resp, attempt, args))
            continue
        failures.append(f"{name}(publish): HTTP {resp.status_code} code={body.get('code')}")
        return
    failures.append(f"{name}(publish): 重试耗尽 last={last_err}")


async def submit_scenes(scenes: list[dict[str, Any]], args: argparse.Namespace) -> dict[str, int]:
    import httpx

    stats = {"created": 0, "existed": 0, "failed": 0, "published": 0}
    failures: list[str] = []
    sem = asyncio.Semaphore(max(1, args.concurrency))
    limiter = _RateLimiter(args.qps)
    async with httpx.AsyncClient() as client:
        if args.mode == "gateway":
            token = await _login_gateway(client, args)
            headers = _make_header_factory(args, token)
            print("[auth] gateway 登录成功，Bearer token 已就绪")
        else:
            headers = _make_header_factory(args, None)
            sign = "HMAC 验签头" if args.hmac_secret else "明文身份头"
            print(f"[auth] direct 模式（{sign}）→ {args.base_url}")
        tasks = [_post_scene(client, sem, p, headers, args, stats, failures, limiter) for p in scenes]
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
    return {**stats, "failed_total": len(failures)}


# =====================================================================
# 九、CLI
# =====================================================================
def _env(name: str, default: str | None = None) -> str | None:
    return os.environ.get(name) or default


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="高斯扰动场景生成引擎：批量生成 300 个多样化自动驾驶训练场景（Carla 仿真）",
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
    p.add_argument("--seed", type=int, default=None, help="随机种子（复现任意一批生成结果，设计文档 §九.5）")
    p.add_argument("--ratio", default="", help="分布比例 common:edge:longtail，如 0.6:0.3:0.1（缺省内置）")
    p.add_argument("--concurrency", type=int, default=5, help="并发提交数（配合 --qps 使用）")
    p.add_argument("--qps", type=float, default=8.0, help="全局请求速率上限（须低于网关阈值；0=不限速）")
    p.add_argument("--timeout", type=float, default=15.0, help="单请求超时（秒）")
    p.add_argument("--max-retries", type=int, default=4, help="可重试错误(429/5xx/超时)最大重试次数")
    p.add_argument("--backoff", type=float, default=1.0, help="重试指数退避基数（秒）")
    p.add_argument("--retry-max", type=float, default=30.0, help="单次重试等待封顶（秒）")
    p.add_argument("--limit", type=int, default=0, help="仅提交前 N 条（0=全部；冒烟测试）")
    p.add_argument("--start", type=int, default=0, help="跳过前 N 条（断点续跑）")
    p.add_argument("--only-type", default="", help="仅生成指定场景类型（逗号分隔，如 rainy,night）")
    p.add_argument("--output", default=str(DEFAULT_OUTPUT), help="dry-run JSON 输出路径")
    return p


def _parse_ratio(raw: str) -> dict[str, float] | None:
    if not raw:
        return None
    parts = [x.strip() for x in raw.split(":") if x.strip()]
    if len(parts) != 3:
        raise SystemExit("[error] --ratio 需三段 common:edge:longtail，如 0.6:0.3:0.1")
    try:
        vals = [float(x) for x in parts]
    except ValueError:
        raise SystemExit("[error] --ratio 三段须为数字")
    total = sum(vals)
    if total <= 0:
        raise SystemExit("[error] --ratio 之和须 > 0")
    keys = ("common", "edge", "longtail")
    return dict(zip(keys, (v / total for v in vals)))


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
    by_risk: dict[str, int] = {}
    for s in scenes:
        by_type[s["scene_type"]] = by_type.get(s["scene_type"], 0) + 1
        by_cat[CATEGORY_BY_TYPE[s["scene_type"]]] = by_cat.get(CATEGORY_BY_TYPE[s["scene_type"]], 0) + 1
        for t in s["tags"]:
            if t.startswith("risk:"):
                by_risk[t] = by_risk.get(t, 0) + 1
    print("\n=== 场景数据集分布 ===")
    print(f"总计: {len(scenes)} 条；覆盖类型: {len(by_type)}/16")
    print("按契约分类:")
    for cat in ("basic", "interactive", "environment", "corner_case"):
        if cat in by_cat:
            print(f"  - {cat:<12} {by_cat[cat]:>3} 条")
    print("按风险梯度（设计文档 §2.5.2）:")
    for rk in ("risk:L1", "risk:L2", "risk:L4"):
        if rk in by_risk:
            print(f"  - {rk:<9} {by_risk[rk]:>3} 条")


def _dump_scenes(scenes: list[dict[str, Any]], args: argparse.Namespace) -> int:
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
            print(f"[dry-run] 原目标不可写（{type(last_err).__name__}: {last_err}），已回退到临时目录", file=sys.stderr)
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

    if not args.base_url:
        args.base_url = "http://localhost:8081" if args.mode == "direct" else "http://localhost:8080"
    args.base_url = args.base_url.rstrip("/")

    if args.mode == "gateway" and not args.dry_run and not (args.username and args.password):
        print("[error] gateway 模式需 --username/--password（或环境变量）登录", file=sys.stderr)
        return 2
    if args.mode == "direct" and not args.dry_run:
        try:
            uuid.UUID(args.user_id)
        except ValueError:
            print(f"[error] --user-id 非法 UUID: {args.user_id}", file=sys.stderr)
            return 2
        if not ({"admin", "operator"} & {r.strip() for r in args.roles.split(",")}):
            print("[error] direct 模式 --roles 须包含 admin 或 operator（scene:create 权限）", file=sys.stderr)
            return 2

    scenes = generate_scenes(seed=args.seed, ratio=_parse_ratio(args.ratio))
    scenes = _filter_types(scenes, args.only_type)
    if args.start:
        scenes = scenes[args.start:]
    if args.limit:
        scenes = scenes[:args.limit]

    _print_distribution(scenes)
    diversity_report(scenes)

    passed, errors = validate_scenes(scenes)
    print(f"\n[validate] 通过 {passed}/{len(scenes)} 条（Schema={load_schema_validator() is not None}）")
    if errors:
        print("[validate] 校验失败样本（前 10）:", file=sys.stderr)
        for e in errors[:10]:
            print("  - " + e, file=sys.stderr)
        return 1

    if args.dry_run:
        return _dump_scenes(scenes, args)

    print(f"\n[submit] 开始提交 {len(scenes)} 条场景 → {args.base_url} "
          f"(mode={args.mode}, concurrency={args.concurrency}, qps={args.qps})")
    stats = asyncio.run(submit_scenes(scenes, args))
    print("\n=== 提交结果 ===")
    print(f"新建: {stats['created']}  幂等跳过(已存在): {stats['existed']}  "
          f"发布: {stats['published']}  失败: {stats['failed_total']}")
    return 1 if stats["failed_total"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
