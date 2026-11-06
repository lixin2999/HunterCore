"""CARLA 地图出生点表（``carla_spawn_points``）单一事实来源。

为什么需要这张表
----------------
``map.spawn_point`` 是**自车在地图坐标系下的绝对出生点**，``actors[].spawn_point`` 是
**相对该出生点的偏移**（该语义由 ``scene_engine.esmini._spawn`` 与
``validators._out_of_bounds`` 的既有实现确立，本模块不改变它）。此前批量生成脚本与内置目录
把出生点一律写成 ``(0, 0, 0)``，而 CARLA 内置城镇的世界原点普遍**不是合法车道坐标**
（空地 / 水面 / 路网外）：自车落在原点即表现为仿真端「黑屏」或「只见路面」，且参与者相对
偏移也一起失真。

平台不猜测坐标：合法出生点只能来自实机 CARLA 的 ``world.get_map().get_spawn_points()``，
由 ``scripts/collect_carla_spawn_points.py`` 导出到本文件并置 ``calibrated=true``。表已校准后：

- 生成链路按表指派自车出生点（:func:`assign_ego_spawn_point`）；
- 校验门 G3 要求出生点必须是表内候选点（:func:`match_candidate`），原点和猜填坐标一律拦下。

表未校准（出厂默认状态）时，平台无从判定任意坐标是否有效，因此不靠 G3 硬拦（否则未接
实机的部署上整条生成链路会直接失效），而是把硬拦放在两个确定能判定的位置：

- 下发前置检查（``services.simulation``）：``carla_town`` 且出生点落在原点 → 2001，
  不创建仿真实例（避免黑屏空跑）；
- 批量生成脚本（``scripts/batch_add_scenes.py``）：表未或未覆盖目标城镇 → 退出码 2。

加载策略与 ``vehicle_params`` 一致：默认读包内 ``config/carla_spawn_points.json``，部署时可用
``SCENE_CARLA_SPAWN_POINTS_PATH`` 覆盖；每次加载计算文件内容 SHA-256（``source_hash``）留痕。
"""
from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

from app.scene_engine.ir import is_finite_number

#: 默认出生点表（仓库内 ``services/scene-service/config/carla_spawn_points.json``；
#: 容器内解析为 ``/app/config/carla_spawn_points.json``，由 Dockerfile 的 ``COPY config`` 内置）
DEFAULT_CARLA_SPAWN_POINTS_PATH = (
    Path(__file__).resolve().parents[2] / "config" / "carla_spawn_points.json"
)

#: 原点判定 / 表命中匹配的默认容差（米；可被表内 ``origin_tolerance_m`` 等字段覆盖）
DEFAULT_ORIGIN_TOLERANCE_M = 1.0
DEFAULT_MATCH_TOLERANCE_M = 2.0

#: 下发载荷的出生点语义标记（契约 openapi ``x-hunter-simulation-flow`` 登记；仿真端按绝对坐标解释）
SPAWN_SEMANTICS_ABSOLUTE = "absolute"

#: 出生点表缺数据时的说明（供校验消息与脚本提示复用）
UNCALIBRATED_HINT = (
    "出生点表未校准（config/carla_spawn_points.json 的 calibrated=false 或该地图无候选点），"
    "请在装有 CARLA PythonAPI 的机器上运行 scripts/collect_carla_spawn_points.py 导出实机出生点"
)


class SpawnPointsError(ValueError):
    """出生点表缺失/结构非法（加载即失败，禁止带病运行）。"""


@dataclass(frozen=True, slots=True)
class SpawnCandidate:
    """单个合法出生点（地图坐标系，yaw 为弧度且已归一到 [-pi, pi]）。"""

    x: float
    y: float
    z: float
    yaw: float

    def as_spawn_point(self) -> dict[str, Any]:
        """转为 4.2.2 结构 ``spawn_point`` 段（``lane_id`` 由地图数据决定，此处不臆造）。"""
        return {
            "x": round(self.x, 3),
            "y": round(self.y, 3),
            "z": round(self.z, 3),
            "yaw": round(self.yaw, 6),
            "lane_id": None,
        }


@dataclass(frozen=True, slots=True)
class SpawnPointTable:
    """出生点表（按 ``map_id`` 分组的实机合法出生点集合）。"""

    calibrated: bool
    maps: Mapping[str, tuple[SpawnCandidate, ...]] = field(default_factory=dict)
    origin_tolerance_m: float = DEFAULT_ORIGIN_TOLERANCE_M
    match_tolerance_m: float = DEFAULT_MATCH_TOLERANCE_M
    source: str = ""
    source_hash: str = ""
    carla_version: str | None = None
    exported_at: str | None = None

    def candidates(self, map_id: str) -> tuple[SpawnCandidate, ...]:
        """该地图的候选出生点（未登记/无数据时为空元组）。"""
        return self.maps.get(str(map_id or "").strip(), ())

    def usable(self, map_id: str) -> bool:
        """该地图能否直接指派实机出生点（表已校准且候选点非空）。"""
        return self.calibrated and bool(self.candidates(map_id))

    def to_dict(self) -> dict[str, Any]:
        """可序列化摘要（写入生成元数据 / 校验报告，用于追溯出生点来源）。"""
        return {
            "calibrated": self.calibrated,
            "maps": {map_id: len(points) for map_id, points in self.maps.items()},
            "origin_tolerance_m": round(self.origin_tolerance_m, 3),
            "match_tolerance_m": round(self.match_tolerance_m, 3),
            "source": self.source,
            "source_hash": self.source_hash,
            "carla_version": self.carla_version,
            "exported_at": self.exported_at,
        }


# =====================================================================
# 加载与解析
# =====================================================================


def _finite(payload: Mapping[str, Any], key: str, default: float | None = None) -> float:
    value = payload.get(key, default)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise SpawnPointsError(f"carla_spawn_points.{key} 必须是数值")
    number = float(value)
    if not is_finite_number(number):
        raise SpawnPointsError(f"carla_spawn_points.{key} 必须是有限数值")
    return number


def _tolerance(payload: Mapping[str, Any], key: str, default: float) -> float:
    if key not in payload:
        return default
    number = _finite(payload, key)
    if number <= 0:
        raise SpawnPointsError(f"carla_spawn_points.{key} 必须为正数")
    return number


def wrap_rad(angle: float) -> float:
    """把弧度归一到 ``[-pi, pi]``（契约 ``SpawnPoint.yaw`` 的取值区间）。"""
    wrapped = math.fmod(float(angle), 2.0 * math.pi)
    if wrapped > math.pi:
        wrapped -= 2.0 * math.pi
    elif wrapped < -math.pi:
        wrapped += 2.0 * math.pi
    return wrapped


def _parse_candidate(raw: Any, label: str) -> SpawnCandidate:
    if not isinstance(raw, Mapping):
        raise SpawnPointsError(f"{label} 必须是对象")
    return SpawnCandidate(
        x=_finite(raw, "x", 0.0),
        y=_finite(raw, "y", 0.0),
        z=_finite(raw, "z", 0.0),
        yaw=wrap_rad(_finite(raw, "yaw", 0.0)),
    )


def parse_spawn_point_table(payload: Mapping[str, Any], *, source: str, source_hash: str) -> SpawnPointTable:
    """解析并校验出生点表载荷（非法即抛 ``SpawnPointsError``）。"""
    if not isinstance(payload, Mapping):
        raise SpawnPointsError("carla_spawn_points 根节点必须是对象")
    calibrated = payload.get("calibrated")
    if not isinstance(calibrated, bool):
        raise SpawnPointsError("carla_spawn_points.calibrated 必须是布尔值")

    maps_raw = payload.get("maps") or {}
    if not isinstance(maps_raw, Mapping):
        raise SpawnPointsError("carla_spawn_points.maps 必须是对象")
    maps: dict[str, tuple[SpawnCandidate, ...]] = {}
    for map_id, section in maps_raw.items():
        points_raw = section.get("spawn_points") if isinstance(section, Mapping) else section
        if not isinstance(points_raw, list):
            raise SpawnPointsError(f"carla_spawn_points.maps.{map_id}.spawn_points 必须是数组")
        maps[str(map_id)] = tuple(
            _parse_candidate(item, f"carla_spawn_points.maps.{map_id}.spawn_points[{index}]")
            for index, item in enumerate(points_raw)
        )
    if calibrated and not any(maps.values()):
        raise SpawnPointsError("carla_spawn_points.calibrated=true 但所有地图均无候选出生点")

    return SpawnPointTable(
        calibrated=calibrated,
        maps=maps,
        origin_tolerance_m=_tolerance(
            payload, "origin_tolerance_m", DEFAULT_ORIGIN_TOLERANCE_M
        ),
        match_tolerance_m=_tolerance(payload, "match_tolerance_m", DEFAULT_MATCH_TOLERANCE_M),
        source=str(payload.get("source") or ""),
        source_hash=source_hash,
        carla_version=payload.get("carla_version"),
        exported_at=payload.get("exported_at"),
    )


def load_spawn_point_table(path: str | Path | None = None) -> SpawnPointTable:
    """读取并校验出生点表文件（缺失/非法即抛 ``SpawnPointsError``）。"""
    resolved = Path(path) if path is not None else resolve_spawn_point_table_path()
    try:
        raw = resolved.read_bytes()
    except OSError as exc:
        raise SpawnPointsError(f"出生点表文件不可读: {resolved}") from exc
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SpawnPointsError(f"出生点表文件不是合法 JSON: {resolved}") from exc
    if not isinstance(payload, dict):
        raise SpawnPointsError("carla_spawn_points 根节点必须是对象")
    return parse_spawn_point_table(
        payload, source=str(payload.get("source") or resolved), source_hash=hashlib.sha256(raw).hexdigest()
    )


def resolve_spawn_point_table_path() -> Path:
    """解析表文件路径（``SCENE_CARLA_SPAWN_POINTS_PATH`` 优先，缺省包内文件）。"""
    from app.config import settings

    configured = (settings.scene_carla_spawn_points_path or "").strip()
    path = Path(configured) if configured else DEFAULT_CARLA_SPAWN_POINTS_PATH
    if not path.is_file():
        raise SpawnPointsError(f"出生点表文件不存在: {path}")
    return path


@lru_cache(maxsize=8)
def _load_cached(path: str) -> SpawnPointTable:
    return load_spawn_point_table(path)


def get_spawn_point_table() -> SpawnPointTable:
    """进程内缓存的出生点表（部署变更后需重启或清缓存）。"""
    return _load_cached(str(resolve_spawn_point_table_path()))


def clear_spawn_point_table_cache() -> None:
    """清空缓存（测试或运行期热更新表文件后调用）。"""
    _load_cached.cache_clear()


# =====================================================================
# 判定 / 指派 / 折算
# =====================================================================


def is_origin_like(spawn: Mapping[str, Any], *, tolerance_m: float = DEFAULT_ORIGIN_TOLERANCE_M) -> bool:
    """出生点是否"落在世界原点附近"（|x|、|y| 均在容差内）。

    该判定不依赖地图数据：CARLA 内置城镇的原点普遍非有效车道坐标，故表未校准时也能识别
    出“批量场景全放原点”这一类缺陷（下发环节据此拦下）。
    """
    x = spawn.get("x")
    y = spawn.get("y")
    if not isinstance(x, (int, float)) or not isinstance(y, (int, float)):
        return False
    return abs(float(x)) <= tolerance_m and abs(float(y)) <= tolerance_m


def match_candidate(
    map_id: str, spawn: Mapping[str, Any], table: SpawnPointTable
) -> SpawnCandidate | None:
    """在表内查找与该出生点最近且在容差内的候选点；无匹配返回 ``None``。"""
    x = spawn.get("x")
    y = spawn.get("y")
    if not isinstance(x, (int, float)) or not isinstance(y, (int, float)):
        return None
    best: SpawnCandidate | None = None
    best_d2 = float("inf")
    for candidate in table.candidates(map_id):
        d2 = (candidate.x - float(x)) ** 2 + (candidate.y - float(y)) ** 2
        if d2 < best_d2:
            best, best_d2 = candidate, d2
    if best is None:
        return None
    return best if math.sqrt(best_d2) <= table.match_tolerance_m else None


def pick_candidate(
    map_id: str, seq: int, table: SpawnPointTable
) -> SpawnCandidate | None:
    """按序号轮换取该地图的候选出生点（同批场景分散到不同车道位置，避免互相占用）。

    表未校准或该地图无候选点时返回 ``None``：平台绝不凭猜测给出坐标。
    """
    if not table.usable(map_id):
        return None
    candidates = table.candidates(map_id)
    return candidates[int(seq) % len(candidates)]


def assign_ego_spawn_point(
    config: MutableMapping[str, Any],
    *,
    seq: int,
    table: SpawnPointTable | None = None,
) -> tuple[bool, str]:
    """为**原点出生点**的 carla_town 场景按表指派实机出生点（就地修改 ``config``）。

    仅在满足全部条件时覆写，否则原样保留并给出原因，交由 G3 校验拦截：

    1. ``map.map_type == "carla_town"``（高精地图/园区地图的坐标由部署方定义，不套用内置城镇表）；
    2. 当前出生点为原点样（人工填写的有效坐标一律尊重，不覆盖）；
    3. 表已校准且该地图有候选点。

    Returns:
        ``(是否覆写, 说明)``。
    """
    resolved = table if table is not None else get_spawn_point_table()
    map_section = config.get("map")
    if not isinstance(map_section, MutableMapping):
        return False, "缺少 map 段，未指派出生点"
    if str(map_section.get("map_type") or "") != "carla_town":
        return False, "非 carla_town 地图，出生点由部署方定义，不套用内置城镇表"
    spawn = map_section.get("spawn_point")
    if not isinstance(spawn, MutableMapping) or not is_origin_like(
        spawn, tolerance_m=resolved.origin_tolerance_m
    ):
        return False, "出生点非原点，按人工指定值保留"
    map_id = str(map_section.get("map_id") or "")
    candidate = pick_candidate(map_id, seq, resolved)
    if candidate is None:
        return False, f"地图 {map_id!r} 无可指派出生点：{UNCALIBRATED_HINT}"
    map_section["spawn_point"] = candidate.as_spawn_point()
    return True, f"已按出生点表指派 {map_id} 出生点 ({candidate.x:.1f}, {candidate.y:.1f})"


def to_absolute_spawn_offsets(config: Mapping[str, Any]) -> dict[str, Any]:
    """把「相对自车出生点」的参与者偏移折算为**绝对地图坐标**（下发前调用）。

    平台存储语义：``actors[].spawn_point`` 是相对 ``map.spawn_point`` 的偏移；
    仿真端（HunterSim）按绝对坐标直接使用参与者出生点、不叠加 map 基准。历史数据里基准恒为
    原点，两者恰好等价；基准一旦改成真实车道坐标，不折算就会让所有参与者脱离自车周边。
    故在下发载荷构造这一步统一折算，并打上 ``spawn_semantics=absolute`` 标记，
    使仿真端将来实现基准叠加时可据此跳过、避免二次相加。

    纯函数：不修改入参，返回深拷贝后的新载荷。
    """
    payload = json.loads(json.dumps(config, ensure_ascii=False, default=str))
    map_section = payload.get("map")
    if not isinstance(map_section, dict):
        return payload
    base = map_section.get("spawn_point")
    if not isinstance(base, dict):
        return payload
    base_x = _as_float(base.get("x"))
    base_y = _as_float(base.get("y"))
    base_z = _as_float(base.get("z"))
    base_yaw = _as_float(base.get("yaw"))
    actors = payload.get("actors")
    if isinstance(actors, list):
        for actor in actors:
            if not isinstance(actor, dict):
                continue
            spawn = actor.get("spawn_point")
            if not isinstance(spawn, dict):
                continue
            spawn["x"] = round(_as_float(spawn.get("x")) + base_x, 3)
            spawn["y"] = round(_as_float(spawn.get("y")) + base_y, 3)
            spawn["z"] = round(_as_float(spawn.get("z")) + base_z, 3)
            spawn["yaw"] = round(wrap_rad(_as_float(spawn.get("yaw")) + base_yaw), 6)
    payload["spawn_semantics"] = SPAWN_SEMANTICS_ABSOLUTE
    return payload


def _as_float(value: Any) -> float:
    """非法/缺失数值一律按 0.0（出生点段已由 SceneConfig 严格校验过，此处只做兜底）。"""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 0.0
    number = float(value)
    return number if is_finite_number(number) else 0.0


__all__ = [
    "DEFAULT_CARLA_SPAWN_POINTS_PATH",
    "SPAWN_SEMANTICS_ABSOLUTE",
    "UNCALIBRATED_HINT",
    "SpawnCandidate",
    "SpawnPointTable",
    "SpawnPointsError",
    "assign_ego_spawn_point",
    "clear_spawn_point_table_cache",
    "get_spawn_point_table",
    "is_origin_like",
    "load_spawn_point_table",
    "match_candidate",
    "parse_spawn_point_table",
    "pick_candidate",
    "resolve_spawn_point_table_path",
    "to_absolute_spawn_offsets",
    "wrap_rad",
]
