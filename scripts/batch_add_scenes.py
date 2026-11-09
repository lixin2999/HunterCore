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

覆盖面扩展（语义契约驱动：可合成类型 46）
-------------------------------------
``SceneType`` 不再是固定 18 值枚举，而是 ``contracts/scene-taxonomy.yaml`` 驱动的可扩展受控字典。
本脚本按字典顺序为**除 ``real_vehicle_replay``（实车自动提取产物）与 ``custom``（人工自定义）外**
的全部叶子类型批量合成场景，而**每个类型「需要哪些交通参与者、什么行为、什么事件、什么天气」
均由同一份 ``scene_semantics`` 契约声明**（与模板服务、场景引擎目录、用例派生共用单一事实来源）。
因此「编辑 YAML 新增类型 → 无需改本脚本即可批量生成」且不会静默退化成 1 辆前车。

每个类型内以**子场景（variant）层**枚举业界典型危险场景（鬼探头、儿童追球、雪天低能见度、
多传感器级联失效等），子场景通过 ``overrides``（调参数分布）与 ``force``（强制恶劣天气/夜间）
在契约层声明，代码侧仅负责分层采样。参与者/事件/天气/时长全部由 ``app.scene_engine.semantics``
的 ``build_actors/build_events/build_weather/build_duration`` 实例化，避免逐类型手写装配器。

去重与自检口径
--------------
- 参数层：同一类型内参数向量余弦相似度 > ``DEDUP_COSINE_THRESHOLD`` 判重、拒绝重采样；
- 语义层：同类型内至少 2 个可辨识语义指纹（角色×行为 + 事件类型 + 天气档位）；
  配额（单类型最高 120）远大于可用指纹组合数，因此逐条唯一以**参数**度量，
  子场景可辨识性以**每类型语义指纹 ≥ 2 且必需角色覆盖率 100%** 度量；
- 提交前逐条执行 ``check_scene_semantics``（与 G9 闸门同源），任一条违例即中止。

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

自车出生点（V1.25.0 口径）
--------------------------
``map.spawn_point`` 保持写 (0,0,0)——这是场景目录的**占位基准**，不是待仿真消费的绝对坐标。
平台不维护、也不猜测每个地图的有效车道坐标（那只能由实机
``world.get_map().get_spawn_points()`` 得到）；出生点有效性校验与“把占位原点换成有效出生
坐标”均在仿真端（HunterSim）完成，``actors[].spawn_point`` 始终为**相对自车出生点的偏移**。
参见 ``docs/HunterSim传感器挂载换算方案.md`` 第二部分。

退出码：0 全部成功（或幂等跳过）；1 存在失败项；2 参数/环境错误。
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import os
import random
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

from app.scene_engine.semantics import (
    audit_roles,
    build_actors,
    build_duration,
    build_ego,
    build_events,
    build_weather,
    check_scene_semantics,
    require_semantics,
    resolve_parameters,
    semantic_fingerprint,
)
from app.scene_engine.semantics import evaluate as evaluate_semantics
from hunter_common.scene_taxonomy import (
    ParamSemantics,
    SceneSemantics,
    VariantSemantics,
    get_scene_taxonomy,
)

#: 受控字典单一事实来源（contracts/scene-taxonomy.yaml；派生分类/标签/类型全集）
_TAXONOMY = get_scene_taxonomy()

DEFAULT_OUTPUT = ROOT / "scripts" / "output" / "scene_batch_generated.json"
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
# 场景类型配额（受控字典驱动：重点类型手工加权，其余按分类默认；TOTAL_SCENES 由配额之和导出）
#: 覆盖面靠「受控字典全类型 + 每类型内子场景（VARIANTS）」双重扩展，不再受限于固定 18 值。
#: 重点类型手工加权（对齐测试重要性）；其余受控类型由 DEFAULT_QUOTA_BY_CATEGORY 兜底。
CURATED_QUOTAS: dict[str, int] = {
    "straight_cruise": 40, "curve_driving": 60, "car_following": 90, "lane_change": 80,
    "cut_in": 100, "pedestrian_crossing": 120, "intersection_meeting": 90, "construction_detour": 60,
    "rainy": 80, "night": 80, "foggy": 60, "backlight": 50,
    "emergency_braking": 80, "obstacle_appearance": 70, "sensor_failure": 60, "multi_vehicle_mixed": 80,
}
#: 无专属加权时按契约分类的默认配额
DEFAULT_QUOTA_BY_CATEGORY: dict[str, int] = {
    "basic": 40, "interactive": 45, "environment": 40, "corner_case": 45,
    "custom": 0, "real_vehicle": 0,
}
#: 批量合成排除的类型：real_vehicle_replay 为实车自动提取产物、custom 为人工自定义，均不合成
BATCH_EXCLUDED_TYPES: frozenset[str] = frozenset({"real_vehicle_replay", "custom"})


def _build_quotas() -> OrderedDict[str, int]:
    """按受控字典声明顺序构建全类型配额（排除实车回放/自定义；重点类型取加权值）。"""
    quotas: OrderedDict[str, int] = OrderedDict()
    for value, spec in _TAXONOMY.scene_types.items():
        if value in BATCH_EXCLUDED_TYPES:
            continue
        quotas[value] = CURATED_QUOTAS.get(value, DEFAULT_QUOTA_BY_CATEGORY[spec.category])
    return quotas


SCENE_QUOTAS: OrderedDict[str, int] = _build_quotas()
assert set(CURATED_QUOTAS) <= set(SCENE_QUOTAS), "重点配额类型必须为纳入合成的受控类型"
TOTAL_SCENES = sum(SCENE_QUOTAS.values())
assert TOTAL_SCENES > 0, "场景配额之和须为正"

#: 类型 → 平台契约分类（受控字典派生，覆盖全部类型；写入 tags 与 config 校验）
CATEGORY_BY_TYPE: dict[str, str] = dict(_TAXONOMY.scene_type_to_category)

#: 类型 → 中文标签（受控字典 SCENE_TYPE_LABELS 同源，覆盖全部类型）
LABEL_BY_TYPE: dict[str, str] = dict(_TAXONOMY.scene_type_labels)

#: 类型 → 名称缩写（保证 scene_name 全局唯一且可读）
ABBR_BY_TYPE: dict[str, str] = {
    "straight_cruise": "CRS", "curve_driving": "CRV", "car_following": "FOL", "lane_change": "LNC",
    "cut_in": "CUT", "pedestrian_crossing": "PED", "intersection_meeting": "INT", "construction_detour": "CST",
    "rainy": "RUN", "night": "NGT", "foggy": "FOG", "backlight": "GLR", "emergency_braking": "EBR",
    "obstacle_appearance": "OBS", "sensor_failure": "SNF", "multi_vehicle_mixed": "MVM",
}

#: 分布区间 → 风险等级（设计文档 §2.5.2：L1 常规 / L2 边缘 / L3 危险 / L4 极端）
RISK_BY_ZONE: dict[str, str] = {"common": "L1", "edge": "L2", "longtail": "L4"}


# =====================================================================
# 二、场景语义契约（contracts/scene-taxonomy.yaml::scene_semantics）
#    参数分布 / 子场景 / 参与者角色 / 事件 / 天气表达式全部来自语义契约；
#    本脚本只保留「配额 + 分层采样 + 可行性 + 去重 + 提交」，不再逐类型写装配器，
#    从根上消除「未覆盖类型静默退化成 1 辆前车」的降级路径。
# =====================================================================
#: 类型 → 语义契约（BATCH_EXCLUDED_TYPES 之外的全部受控类型；缺失即启动失败）
SEMANTICS: dict[str, SceneSemantics] = {stype: require_semantics(stype) for stype in SCENE_QUOTAS}
if not SEMANTICS:
    raise SystemExit("[error] 受控字典未声明 scene_semantics，无法生成语义驱动场景")

#: 分类 → 功能一级标签 / 能力标签的默认值（逐类型标签改由角色/行为/天气派生）
_FUNC_CATEGORY_DEFAULT: dict[str, str] = {
    "basic": "road-structure", "interactive": "interaction", "environment": "environment",
    "corner_case": "safety", "custom": "custom", "real_vehicle": "replay",
}
_FUNCTION_TAGS_DEFAULT: dict[str, tuple[str, ...]] = {
    "basic": ("control", "planning"), "interactive": ("prediction", "interaction"),
    "environment": ("robustness",), "corner_case": ("safety", "corner-case"),
    "custom": ("custom",), "real_vehicle": ("replay",),
}


def _params_for(stype: str) -> dict[str, ParamSemantics]:
    """类型参数分布（含共享天气/时长参数：契约已合并 scene_semantics_defaults）。"""
    return SEMANTICS[stype].params


def _variants_for(stype: str) -> tuple[VariantSemantics, ...]:
    """类型子场景清单（契约保证 ≥2 且带 force/overrides，无通用兜底子场景）。"""
    return SEMANTICS[stype].variants


def _abbr_for(stype: str) -> str:
    if stype in ABBR_BY_TYPE:
        return ABBR_BY_TYPE[stype]
    tokens = stype.split("_")
    abbr = "".join(t[0] for t in tokens).upper() if len(tokens) > 1 else tokens[0][:3].upper()
    return abbr[:6]


def _func_category_for(stype: str) -> str:
    return _FUNC_CATEGORY_DEFAULT[CATEGORY_BY_TYPE[stype]]


def _func_tags_for(stype: str) -> tuple[str, ...]:
    return _FUNCTION_TAGS_DEFAULT[CATEGORY_BY_TYPE[stype]]


def _weather_tag(weather: dict[str, float]) -> str:
    """天气档位标签（与语义指纹同源头，供场景库筛选「雨/雾/夜间/黄昏」）。"""
    rain = float(weather.get("rain") or 0.0)
    fog = float(weather.get("fog") or 0.0)
    altitude = float(weather.get("sun_altitude") or 0.0)
    if rain >= 40:
        band = "rain-heavy"
    elif rain >= 1:
        band = "rain-light"
    elif fog >= 40:
        band = "fog-dense"
    elif fog >= 5:
        band = "fog-light"
    elif altitude < -6:
        band = "night"
    elif altitude < 15:
        band = "dusk"
    else:
        band = "day"
    return f"wx:{band}"


def _role_of(actor_id: str) -> str:
    """actor_id → 语义角色名（命名口径 ``{role}-{序号}``；角色名本身可含 '-'）。"""
    head, sep, tail = actor_id.rpartition("-")
    return head if sep and tail.isdigit() else actor_id


def _distribute(total: int, parts: int) -> list[int]:
    """把类型配额尽量均分到其子场景（余数给靠前的子场景，和恒为 total）。"""
    if parts <= 0:
        return []
    base, rem = divmod(total, parts)
    return [base + (1 if i < rem else 0) for i in range(parts)]


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


def _sample_param(
    rng: random.Random, name: str, p: ParamSemantics, zone: str, coherent: int
) -> float:
    """单参数高斯抽样；长尾沿风险方向做极值组合（§2.3.3），常用/边缘部分独立。"""
    mag = _zone_magnitude(rng, zone)
    if p.risk != 0 and zone == "longtail":
        sign = coherent * p.risk                 # 多参数同向推向极限
    elif p.risk != 0 and zone == "edge" and rng.random() < 0.5:
        sign = coherent * p.risk
    else:
        sign = rng.choice((-1, 1))
    val = _clamp(p.mean + sign * mag * p.std, p.low, p.high)
    if p.discrete:
        val = float(round(val))
        val = _clamp(val, p.low, p.high)
    return val


def _sample_vector(
    rng: random.Random, spec: dict[str, ParamSemantics], zone: str
) -> dict[str, float]:
    coherent = rng.choice((-1, 1))
    return {name: _sample_param(rng, name, p, zone, coherent) for name, p in spec.items()}


def _normalize_vector(
    spec: dict[str, ParamSemantics], params: dict[str, float]
) -> list[float]:
    """参数向量归一化到 [0,1]（用于余弦相似度去重，§2.4.4）。"""
    vec: list[float] = []
    for name, p in spec.items():
        v = params[name]
        span = p.high - p.low
        vec.append((v - p.low) / span if span > 0 else 0.0)
    return vec


def _cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0 or nb == 0:
        return 1.0
    return dot / (na * nb)


def _feasible(stype: str, p: dict[str, float]) -> bool:
    """初始状态可行性（§2.4.3 第 2/3 层）：语义 constraints（初始不碰撞等）。"""
    semantics = SEMANTICS[stype]
    variables = resolve_parameters(semantics, p)
    for constraint in semantics.constraints:
        left = variables.get(constraint.left)
        if left is None:
            continue
        right = evaluate_semantics(constraint.right, variables, where=constraint.left)
        satisfied = {
            "<=": left <= right + 1e-9,
            "<": left < right,
            ">=": left >= right - 1e-9,
            ">": left > right,
            "==": abs(left - right) <= 1e-9,
        }[constraint.op]
        if not satisfied:
            return False
    return True


def _zone_counts(total: int, ratio: dict[str, float]) -> OrderedDict[str, int]:
    """按比例把类型配额拆到三个分布区间，舍入余数补给 common（和恒等于 total）。"""
    keys = ("common", "edge", "longtail")
    counts = {k: round(total * ratio.get(k, 0.0)) for k in keys}
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
#     参与者/事件由语义契约生成（见 scene_engine.semantics），此处只保留地图与自车装配
# =====================================================================
def _spawn(x: float, y: float = 0.0, yaw: float = 0.0, z: float = 0.0, lane_id: str | None = None) -> dict[str, Any]:
    return {"x": round(x, 3), "y": round(y, 3), "z": round(z, 3), "yaw": round(yaw, 5), "lane_id": lane_id}


def _derive_criteria(stype: str, p: dict[str, float]) -> tuple[float, float]:
    """由速度/类型推导 (min_safe, max_dev)，随场景强度自适应。"""
    speed = p.get("ego_speed", 6.0)
    base = {"emergency_braking": 2.0, "obstacle_appearance": 2.5, "cut_in": 6.0, "car_following": 4.0}.get(stype, 3.5)
    min_safe = _clamp(base + speed * 0.25, 1.0, 25.0)
    max_dev = _clamp(1.0 + speed * 0.05, 0.5, 3.5)
    return round(min_safe, 2), round(max_dev, 2)


def _config(
    *, map_id: str, ego_speed: float, ego_steer: float,
    weather: dict[str, float],
    actors: list[dict[str, Any]], events: list[dict[str, Any]], duration: float,
    min_safe: float, max_dev: float, no_collision: bool = True,
) -> dict[str, Any]:
    return {
        # 出生点为目录占位基准 (0,0,0)：有效出生坐标的选取与替换由仿真端（HunterSim）完成
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
# 五、语义装配：config 的参与者/事件/天气/时长/自车全部由语义契约实例化
#     数值由高斯采样参数 p 决定（逐条不同），结构由 roles/events/weather 声明决定
# =====================================================================
def _map_id_for(stype: str, params: dict[str, float]) -> str:
    """稳定地图选型（sha256 而非内置 hash，避免 PYTHONHASHSEED 致重生成不可复现）。"""
    digest_key = json.dumps(
        {"stype": stype, "params": {k: round(v, 2) for k, v in params.items()}},
        sort_keys=True,
    )
    digest = int(hashlib.sha256(digest_key.encode("utf-8")).hexdigest()[:8], 16)
    return MAPS[digest % len(MAPS)]


def _assemble_config(stype: str, params: dict[str, float], zone: str) -> dict[str, Any]:
    """按语义契约装配 4.2.2 场景（参与者角色×数量 / 事件 / 天气 / 时长 / 自车）。"""
    semantics = SEMANTICS[stype]
    ego = build_ego(semantics, params, model=EGO_MODEL)
    min_safe, max_dev = _derive_criteria(stype, params)
    return _config(
        map_id=_map_id_for(stype, params),
        ego_speed=ego["initial_speed"],
        ego_steer=ego["initial_steer"],
        weather=build_weather(semantics, params),
        actors=build_actors(semantics, params),
        events=build_events(semantics, params),
        duration=build_duration(semantics, params),
        min_safe=min_safe,
        max_dev=max_dev,
        no_collision=True,
    )


def _scene_token(variant: VariantSemantics, config: dict[str, Any]) -> str:
    """名称短码：子场景 + 参与者/事件数 + 角色集合（人工可辨识）。"""
    roles = "+".join(sorted({_role_of(str(a["actor_id"])) for a in config["actors"]})) or "no-role"
    return f"{variant.sub}-{len(config['actors'])}a{len(config['events'])}e-{roles}"[:20]


def _scene_tags(
    stype: str, variant: VariantSemantics, zone: str, risk: str, config: dict[str, Any]
) -> list[str]:
    """标签：分类/子场景/风险/分区 + 角色 + 行为 + 天气档位（供场景库区分测试目的）。"""
    roles = sorted({_role_of(str(a["actor_id"])) for a in config["actors"]})
    acts = sorted({str(a["behavior"]["behavior_type"]) for a in config["actors"]})
    return _dedup_tags([
        f"cat:{_func_category_for(stype)}", CATEGORY_BY_TYPE[stype], stype,
        f"sub:{variant.key}", f"risk:{risk}", f"zone:{zone}", "src:synthetic", "ad-training", "carla",
        *[f"role:{role}" for role in roles],
        *[f"act:{act}" for act in acts],
        _weather_tag(config["weather"]),
        *_func_tags_for(stype),
    ])


def _scene_description(
    stype: str, variant: VariantSemantics, zone: str, risk: str, config: dict[str, Any]
) -> str:
    """描述：测试目的 + 参与者角色×数量/行为 + 触发条件 + 判据 + 天气/关键参数。"""
    semantics = SEMANTICS[stype]
    counts: dict[str, int] = {}
    for actor in config["actors"]:
        role = _role_of(str(actor["actor_id"]))
        counts[role] = counts.get(role, 0) + 1
    role_desc = "、".join(
        f"{role.role}×{counts[role.role]}({role.actor_type}/{role.behavior_type})"
        for role in semantics.roles
        if role.role in counts
    ) or "无交通参与者"
    event_desc = "、".join(
        f"{event.type}@{event.condition_type}" for event in semantics.events
    ) or "无事件"
    weather = config["weather"]
    return (
        f"[{LABEL_BY_TYPE[stype]}·{variant.label}] {semantics.objective}。"
        f"参与者：{role_desc}；触发：{event_desc}；判据：{semantics.oracle_ref}；"
        f"天气：雨{weather['rain']}/雾{weather['fog']}/风{weather['wind']}"
        f"/高度角{weather['sun_altitude']}（{_weather_tag(weather)}）；"
        f"地图 {config['map']['map_id']}，自车 {config['ego_vehicle']['initial_speed']}m/s，"
        f"时长 {config['duration']}s，风险 {risk}·{zone} 区，事件 {len(config['events'])}。"
    )[:1024]


# =====================================================================
# 六、组装全量场景（类型 → 子场景 → 分层采样 + 可行性拒绝 + 余弦去重 + 唯一命名）
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
    stype: str, spec: dict[str, ParamSemantics], zone: str, rng: random.Random,
    accepted: list[list[float]], variant: VariantSemantics,
) -> tuple[dict[str, Any], dict[str, float], str, int]:
    """采样一条通过可行性 + 余弦去重的场景；返回 (config, params, token, tries)。"""
    last: tuple[dict[str, float], bool] | None = None
    for attempt in range(1, MAX_SAMPLE_TRIES + 1):
        params = _sample_vector(rng, spec, zone)
        if variant.force:
            params.update(variant.force)
        if not _feasible(stype, params):
            continue
        vec = _normalize_vector(spec, params)
        if any(_cosine(vec, a) > DEDUP_COSINE_THRESHOLD for a in accepted):
            last = (params, False)
            continue
        accepted.append(vec)
        cfg = _assemble_config(stype, params, zone)
        return cfg, params, _scene_token(variant, cfg), attempt
    # 重试耗尽：接受最后一次可行样本（或强制放宽），保证数量恒等于配额
    params = last[0] if last and last[0] else _sample_vector(rng, spec, "common")
    if variant.force:
        params.update(variant.force)
    cfg = _assemble_config(stype, params, zone)
    return cfg, params, _scene_token(variant, cfg), MAX_SAMPLE_TRIES


def _enforce_unique_configs(scenes: list[dict[str, Any]]) -> int:
    """兜底：对完全重复的 config 施加最小抖动（自车初速 +0.01 m/s）以保证逐条唯一。

    长尾（L4）区的强约束子场景（如动物窜出 kind_idx 固定、障碍急刹参数贴极限）可能
    在 MAX_SAMPLE_TRIES 耗尽后放宽接受与已有样本重合的 config。此处仅对检出的重复项抖动，
    正常不重复的场景不受影响；0.01m/s 仍在契约速度域内且不改变场景语义。
    """
    seen: set[str] = set()
    fixed = 0
    for s in scenes:
        cfg = s["config"]
        key = json.dumps(cfg, sort_keys=True, ensure_ascii=False)
        if key not in seen:
            seen.add(key)
            continue
        ego = cfg["ego_vehicle"]
        for _ in range(10000):
            ego["initial_speed"] = round(ego["initial_speed"] + 0.01, 2)
            key = json.dumps(cfg, sort_keys=True, ensure_ascii=False)
            if key not in seen:
                break
        seen.add(key)
        fixed += 1
    return fixed


def generate_scenes(seed: int | None = None, ratio: dict[str, float] | None = None) -> list[dict[str, Any]]:
    """按 类型 → 子场景 → 分布区间 逐层采样生成 TOTAL_SCENES 条场景载荷。

    每条场景落库前立即执行语义自检（``check_scene_semantics``）：参与者角色/行为/事件
    与语义契约不一致的条目计数并打印，由 ``validate_scenes`` 汇总拦截（dry-run 也不会放行）。
    """
    rng = random.Random(seed)
    zone_ratio = ratio or ZONE_RATIO
    scenes: list[dict[str, Any]] = []
    stats: dict[str, Any] = {
        "tries_total": 0, "dedup_drop": 0, "semantic_problems": 0, "semantic_samples": []
    }
    fps_by_type: dict[str, set[str]] = {}
    g = 0
    for stype, count in SCENE_QUOTAS.items():
        variants = _variants_for(stype)
        per_variant = _distribute(count, len(variants))
        accepted: list[list[float]] = []          # 同一类型内跨子场景共享，保障类型内逐条唯一
        fingerprints: set[str] = set()
        for variant, vq in zip(variants, per_variant):
            spec = {**SEMANTICS[stype].params, **variant.overrides}
            zone_counts = _zone_counts(vq, zone_ratio)
            for zone, n in zone_counts.items():
                for _ in range(n):
                    g += 1
                    cfg, params, token, tries = _make_scene(
                        stype, spec, zone, rng, accepted, variant
                    )
                    stats["tries_total"] += tries
                    stats["dedup_drop"] += max(0, tries - 1)
                    problems = check_scene_semantics(stype, cfg, values=params)
                    if problems:
                        stats["semantic_problems"] += 1
                        if len(stats["semantic_samples"]) < 10:
                            stats["semantic_samples"].append(
                                f"{stype}/{variant.key}/{zone}: "
                                + "；".join(p.message for p in problems[:3])
                            )
                    fingerprints.add(semantic_fingerprint(stype, cfg))
                    risk = RISK_BY_ZONE[zone]
                    name = f"SCN-{g:04d}-{_abbr_for(stype)}-{variant.sub}-{risk}-{token[:20]}"[:128]
                    scenes.append({
                        "scene_name": name,
                        "scene_type": stype,
                        "description": _scene_description(stype, variant, zone, risk, cfg),
                        "tags": _scene_tags(stype, variant, zone, risk, cfg),
                        "config": cfg,
                    })
        fps_by_type[stype] = fingerprints
    generate_scenes.stats = stats  # type: ignore[attr-defined]
    generate_scenes.fingerprints = fps_by_type  # type: ignore[attr-defined]
    stats["dup_fixed"] = _enforce_unique_configs(scenes)
    return scenes


# =====================================================================
# 七、场景库规范校验（优先用 4.2.2 Schema；不可导入时降级为轻量内部校验）
# =====================================================================
VALID_SCENE_TYPES = set(_TAXONOMY.scene_type_values)
VALID_BEHAVIOR_TYPES = set(_TAXONOMY.behavior_type_values)
VALID_ACTION_TYPES = set(_TAXONOMY.action_type_values)
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
        bt = a["behavior"]["behavior_type"]
        if bt not in VALID_BEHAVIOR_TYPES:
            raise ValueError(f"非法 behavior_type: {bt}")
    for e in cfg["events"]:
        if e["type"] not in _SCENE_EVENT_TYPES:
            raise ValueError(f"非法 event.type: {e['type']}")
        at = e["action"]["action_type"]
        if at not in VALID_ACTION_TYPES:
            raise ValueError(f"非法 action_type: {at}")
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
    """提交前逐条校验：4.2.2 结构（Schema）+ 语义契约（角色/行为/事件）双闸门。

    语义违例（缺必需角色、角色类型/行为不符、必需事件为空）与结构违例同等对待，
    任一条存在即计入 errors，由 ``main`` 中止（dry-run 同样不放行），杜绝静默退化。
    """
    validator = load_schema_validator()
    errors: list[str] = []
    passed = 0
    for payload in scenes:
        problems: list[str] = []
        try:
            if validator is not None:
                validator.model_validate(payload)  # extra="forbid"：字段名/结构严格校验
            else:
                _light_validate(payload)
        except Exception as exc:  # noqa: BLE001
            msg = str(exc).splitlines()[0] if str(exc) else type(exc).__name__
            problems.append(msg)
        try:
            problems.extend(
                f"语义[{p.code}]{p.message}"
                for p in check_scene_semantics(
                    str(payload.get("scene_type") or ""), payload.get("config") or {}
                )
            )
        except Exception as exc:  # noqa: BLE001 - 语义求值异常同样视为违例
            problems.append(f"语义求值异常: {type(exc).__name__}: {exc}")
        if problems:
            errors.append(f"{payload.get('scene_name')}: " + "；".join(problems))
        else:
            passed += 1
    return passed, errors


def _type_semantic_stats(scenes: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """按类型汇总语义指标：场景数 / 语义指纹集 / 语义违例数 / 必需角色覆盖计数。"""
    rows: dict[str, dict[str, Any]] = {}
    for scene in scenes:
        stype = str(scene["scene_type"])
        row = rows.setdefault(stype, {"count": 0, "fps": set(), "violations": 0, "roles": {}})
        row["count"] += 1
        config = scene["config"]
        row["fps"].add(semantic_fingerprint(stype, config))
        if check_scene_semantics(stype, config):
            row["violations"] += 1
        for audit in audit_roles(stype, config):
            if not audit.required:
                continue
            row["roles"][audit.role] = row["roles"].get(audit.role, 0) + (1 if audit.actual else 0)
    return rows


def diversity_report(scenes: list[dict[str, Any]]) -> list[str]:
    """打印并校验多样性/语义覆盖摘要；返回不达标的明细（非空即中止提交）。

    达标口径（契约 ``variants`` 已保证每型 ≥2 个可辨识子场景）：
    - 每类型语义指纹数 ≥ ``min(2, 该类型场景数)``（过滤/限量时为子集放宽）；
    - 每类型必需角色覆盖率 = 100%（G9 违例数为 0）；
    - 每类型语义违例 0 条。
    """
    fp: set[str] = set()
    actor_ct: dict[str, int] = {}
    for s in scenes:
        fp.add(json.dumps(s["config"], sort_keys=True, ensure_ascii=False))
        actor_ct[s["scene_type"]] = actor_ct.get(s["scene_type"], 0) + len(s["config"]["actors"])
    stats = getattr(generate_scenes, "stats", {})
    rows = _type_semantic_stats(scenes)
    issues: list[str] = []
    print("\n=== 多样性摘要（重构核心指标）===")
    print(f"场景总数: {len(scenes)}；唯一 config 指纹: {len(fp)}（旧版固定轮换远低于此）")
    print(f"平均参与者/场景: {sum(actor_ct.values()) / max(1, len(scenes)):.2f}")
    print(f"采样总尝试: {stats.get('tries_total', 'NA')}；因去重/可行性重采样: {stats.get('dedup_drop', 'NA')} 次")
    print("\n=== 语义覆盖（每类型：场景数 / 语义指纹 / 必需角色覆盖 / 违例）===")
    total_fps = sum(len(row["fps"]) for row in rows.values())
    total_violations = sum(row["violations"] for row in rows.values())
    print(f"类型数: {len(rows)}；语义指纹合计: {total_fps}；语义违例: {total_violations} 条")
    for stype, row in rows.items():
        count = row["count"]
        role_cov = min((c / count for c in row["roles"].values()), default=1.0)
        print(
            f"  - {stype:<26} {count:>3} 条 / 指纹 {len(row['fps']):>2}"
            f" / 必需角色覆盖 {role_cov * 100:>5.1f}% / 违例 {row['violations']}"
        )
        need = min(2, count)
        if len(row["fps"]) < need:
            issues.append(f"{stype}: 语义指纹仅 {len(row['fps'])} 个（要求 ≥ {need}，子场景不可辨识）")
        if row["violations"]:
            issues.append(f"{stype}: {row['violations']} 条场景不满足语义契约")
        if role_cov < 1.0:
            lacks = [r for r, c in row["roles"].items() if c < count]
            issues.append(f"{stype}: 必需角色覆盖不全（缺 {','.join(lacks)}）")
    sample = stats.get("semantic_samples") or []
    if sample:
        print("\n语义违例样本（前 10）:")
        for line in sample:
            print("  - " + line)
    if issues:
        print("[diversity] 未达标明细（前 10）:")
        for line in issues[:10]:
            print("  - " + line)
    else:
        print("[diversity] 语义覆盖达标：每类型指纹 ≥2、必需角色覆盖率 100%、违例 0")
    return issues


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
        code = body.get("code")
        if resp.status_code in (200, 201) and code == 0:
            stats["published"] += 1
            return
        if code == 3003 or resp.status_code == 409:
            # 幂等：3003 = “仅 draft 可发布”，说明该场景已非 draft（已发布）
            # → 发布态已达成，视为幂等成功（类比 create 的 3002→existed），不计入失败
            stats["pub_existed"] += 1
            return
        if resp.status_code in (429, 500, 502, 503, 504):
            last_err = f"HTTP {resp.status_code} code={code}"
            await asyncio.sleep(_retry_delay(resp, attempt, args))
            continue
        failures.append(f"{name}(publish): HTTP {resp.status_code} code={body.get('code')}")
        return
    failures.append(f"{name}(publish): 重试耗尽 last={last_err}")


async def submit_scenes(scenes: list[dict[str, Any]], args: argparse.Namespace) -> dict[str, int]:
    import httpx

    stats = {"created": 0, "existed": 0, "failed": 0, "published": 0, "pub_existed": 0}
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


async def publish_drafts(args: argparse.Namespace) -> dict[str, int]:
    """分批修复通道：分页拉取全部 draft 场景并逐一发布（不新建）。

    背景：gateway 模式会话为 Redis ``session:{user_id}`` 单会话全等比较——任何同用户重登录/
    前端刷新都会覆盖会话使脚本 token 即刻 401/1001（典型时序：创建已全部成功、发布窗口内被覆盖）。
    重新登录后重跑本通道即可把存量 draft 全部闭环，无需重建场景。"""
    import httpx

    stats = {"published": 0, "already": 0, "failed": 0}
    failures: list[str] = []
    sem = asyncio.Semaphore(max(1, args.concurrency))
    limiter = _RateLimiter(args.qps)
    async with httpx.AsyncClient() as client:
        if args.mode == "gateway":
            token = await _login_gateway(client, args)
            headers = _make_header_factory(args, token)
        else:
            headers = _make_header_factory(args, None)
        drafts: list[dict[str, Any]] = []
        page = 1
        while True:
            await limiter.acquire()
            async with sem:
                resp = await client.get(f"{args.base_url}/api/v1/scene",
                                        params={"status": "draft", "page": page, "page_size": 200},
                                        headers=headers(), timeout=args.timeout)
            body = _safe_json(resp) or {}
            if resp.status_code != 200 or body.get("code") != 0:
                print(f"[error] 拉取 draft 列表失败：HTTP {resp.status_code} "
                      f"code={body.get('code')} msg={body.get('message')}", file=sys.stderr)
                return stats
            data = body.get("data") or {}
            drafts.extend(data.get("items") or [])
            if page * 200 >= int(data.get("total") or 0):
                break
            page += 1
        print(f"[publish-drafts] 命中 draft 场景 {len(drafts)} 条，开始发布（qps={args.qps}）")

        async def _pub(item: dict[str, Any]) -> None:
            scene_id = item.get("scene_id")
            name = item.get("scene_name") or str(scene_id)
            if not scene_id:
                stats["failed"] += 1
                failures.append(f"{name}: 列表项缺少 scene_id")
                return
            for attempt in range(args.max_retries + 1):
                await limiter.acquire()
                async with sem:
                    try:
                        resp = await client.post(f"{args.base_url}/api/v1/scene/{scene_id}/publish",
                                                 headers=headers(), timeout=args.timeout)
                    except Exception:  # noqa: BLE001 - 传输层异常统一重试
                        await asyncio.sleep(_retry_delay(None, attempt, args))
                        continue
                body = _safe_json(resp) or {}
                code = body.get("code")
                if resp.status_code in (200, 201) and code == 0:
                    stats["published"] += 1
                    return
                if code == 3003 or resp.status_code == 409:
                    stats["already"] += 1  # 幂等：已非 draft（已发布），发布态已达成
                    return
                if resp.status_code in (429, 500, 502, 503, 504):
                    await asyncio.sleep(_retry_delay(resp, attempt, args))
                    continue
                stats["failed"] += 1
                failures.append(f"{name}: HTTP {resp.status_code} code={body.get('code')} "
                                f"msg={body.get('message')}")
                return
            stats["failed"] += 1
            failures.append(f"{name}: 重试耗尽")

        await asyncio.gather(*(_pub(i) for i in drafts))
    if failures:
        print("\n[failures]（前 30 条）", file=sys.stderr)
        for f in failures[:30]:
            print("  - " + f, file=sys.stderr)
    return stats


# =====================================================================
# 九、CLI
# =====================================================================
def _env(name: str, default: str | None = None) -> str | None:
    return os.environ.get(name) or default


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="高斯扰动场景生成引擎：按受控字典批量生成多样化自动驾驶训练场景（覆盖全字典类型 × 子场景族，含常见/极端/组合多风险，Carla 仿真）",
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
    p.add_argument("--publish-drafts", action="store_true",
                   help="分批修复通道：仅列出并发布全部 draft 场景（不新建）——提交窗口内会话被覆盖"
                        "（gateway 单会话语义，旧 token 即刻 401/1001）导致部分未发布时重跑本项闭环")
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
    seen_subs: set[tuple[str, str]] = set()   # 子场景按 (类型, key) 计，同名子场景可跨类型复用
    for s in scenes:
        stype = s["scene_type"]
        by_type[stype] = by_type.get(stype, 0) + 1
        by_cat[CATEGORY_BY_TYPE[stype]] = by_cat.get(CATEGORY_BY_TYPE[stype], 0) + 1
        for t in s["tags"]:
            if t.startswith("risk:"):
                by_risk[t] = by_risk.get(t, 0) + 1
            elif t.startswith("sub:"):
                seen_subs.add((stype, t[4:]))
    total_variants = len({(st, v.key) for st in SCENE_QUOTAS for v in _variants_for(st)})
    print("\n=== 场景数据集分布 ===")
    print(
        f"总计: {len(scenes)} 条；覆盖类型: {len(by_type)}/{len(SCENE_QUOTAS)}；"
        f"覆盖子场景: {len(seen_subs)}/{total_variants}"
    )
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

    # 提交依赖前置检查（dry-run 不需要）：避免生成/校验跑完后才在 submit 深处报 ModuleNotFoundError
    if not args.dry_run or args.publish_drafts:
        try:
            import httpx  # noqa: F401
        except ImportError:
            print("[error] 缺少提交依赖 httpx。安装（任选其一）：\n"
                  "        pip3 install httpx -i https://pypi.tuna.tsinghua.edu.cn/simple\n"
                  "        sudo apt-get install -y python3-httpx\n"
                  "        （dry-run 不依赖 httpx，可先 --dry-run 验证生成与校验）", file=sys.stderr)
            return 2

    # 分批修复通道：不生成/不新建，直接把存量 draft 全部发布
    if args.publish_drafts:
        pd = asyncio.run(publish_drafts(args))
        print(f"\n=== 补发布结果 === 已发布: {pd['published']}（幂等已发布: {pd.get('already', 0)}）  失败: {pd['failed']}")
        return 1 if pd["failed"] else 0

    scenes = generate_scenes(seed=args.seed, ratio=_parse_ratio(args.ratio))
    scenes = _filter_types(scenes, args.only_type)
    if args.start:
        scenes = scenes[args.start:]
    if args.limit:
        scenes = scenes[:args.limit]

    _print_distribution(scenes)
    diversity_issues = diversity_report(scenes)

    passed, errors = validate_scenes(scenes)
    print(f"\n[validate] 通过 {passed}/{len(scenes)} 条（Schema={load_schema_validator() is not None}）")
    if errors:
        print("[validate] 校验失败样本（前 10）:", file=sys.stderr)
        for e in errors[:10]:
            print("  - " + e, file=sys.stderr)
        return 1
    if diversity_issues:
        print("[error] 语义覆盖/多样性未达标，已中止提交（详见上方 diversity 明细）", file=sys.stderr)
        return 1

    if args.dry_run:
        return _dump_scenes(scenes, args)

    print(f"\n[submit] 开始提交 {len(scenes)} 条场景 → {args.base_url} "
          f"(mode={args.mode}, concurrency={args.concurrency}, qps={args.qps})")
    stats = asyncio.run(submit_scenes(scenes, args))
    print("\n=== 提交结果 ===")
    print(f"新建: {stats['created']}  幂等跳过(已存在): {stats['existed']}  "
          f"发布: {stats['published']}（幂等已发布: {stats['pub_existed']}）  失败: {stats['failed_total']}")
    return 1 if stats["failed_total"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
