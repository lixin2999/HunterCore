"""地理围栏判定服务（G-11；契约 x-hunter-geofence）。

判定链路（POST /session 创建时，可控性判定之后、互斥锁获取之前）：
1. ``fence_json`` 未配置（NULL/车辆未登记）→ 直接放行（零额外开销，不读 Redis 定位）；
2. 围栏 JSON 结构非法 → 3003 ``fence_malformed``（配置错误不得默许放行）；
3. 定位缺失/非数/超值域/陈旧（``lat_lng_at`` 距现在 > RC_GEOFENCE_POSITION_MAX_AGE_S，
   与 data-collector VEHICLE_OFFLINE_THRESHOLD_SECONDS 同源）→ 3003 ``position_unavailable``
   （安全默认，同 G-08 OTA 门禁缺数据即拒绝）；
4. 位置在围栏外 → 3003 ``outside_geofence``（data 携带 distance_m/围栏摘要）。

⚠ 禁止新增错误码：全部拒绝路径复用 3003 资源状态冲突（409 语义）；
运行期越界由车端自动减速/停车兜底，平台侧定稿 pending #22 前禁止自动结束会话。
几何：circle 用 haversine 大圆距离（地球半径 6371008.8m）；polygon 用射线法。
"""
from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from hunter_common.exceptions import ResourceStateConflictError
from hunter_common.logging import get_logger
from hunter_common.redis import RedisManager

from app.config import Settings
from app.services.metrics import GEOFENCE_REJECTED_TOTAL
from app.services.vehicle_view import KEY_VEHICLE_STATUS

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = get_logger("app.services.geofence")

#: 地球半径（米；契约 x-hunter-geofence.geometry 固定值）
EARTH_RADIUS_M = 6371008.8

#: 拒绝原因（与契约 verdict_rules 一一对应；作为 3003 响应 data.reason 与 metrics 标签）
REASON_FENCE_MALFORMED = "fence_malformed"
REASON_POSITION_UNAVAILABLE = "position_unavailable"
REASON_OUTSIDE_GEOFENCE = "outside_geofence"


def haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """两点大圆距离（米）。"""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = phi2 - phi1
    d_lambda = math.radians(lng2 - lng1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(a))


def point_in_polygon(lat: float, lng: float, points: Sequence[tuple[float, float]]) -> bool:
    """射线法判定 (lat, lng) 是否在多边形内（点序列隐式闭合；自交多边形保守处理）。"""
    inside = False
    n = len(points)
    j = n - 1
    for i in range(n):
        lat_i, lng_i = points[i]
        lat_j, lng_j = points[j]
        # lng 为射线轴：跨越判定用 lat，交点插值用 lng
        if (lat_i > lat) != (lat_j > lat):
            cross_lng = (lng_j - lng_i) * (lat - lat_i) / (lat_j - lat_i) + lng_i
            if lng < cross_lng:
                inside = not inside
        j = i
    return inside


class FenceDefinitionError(ValueError):
    """围栏 JSON 结构非法（type 未知/字段缺失/值域越界）。"""


@dataclass(frozen=True)
class CircleFence:
    """圆形围栏：圆心 + 半径（米）。"""

    center_lat: float
    center_lng: float
    radius_m: float

    def contains(self, lat: float, lng: float) -> bool:
        return haversine_m(self.center_lat, self.center_lng, lat, lng) <= self.radius_m

    def violation_summary(self, lat: float, lng: float) -> dict[str, object]:
        over = haversine_m(self.center_lat, self.center_lng, lat, lng) - self.radius_m
        return {"fence_type": "circle", "distance_m": round(max(over, 0.0), 1)}


@dataclass(frozen=True)
class PolygonFence:
    """多边形围栏：顶点序列（隐式闭合）。"""

    points: tuple[tuple[float, float], ...]

    def contains(self, lat: float, lng: float) -> bool:
        return point_in_polygon(lat, lng, self.points)

    def violation_summary(self, lat: float, lng: float) -> dict[str, object]:
        # 多边形外点到边界的精确距离非门禁必需，摘要给顶点数（契约 data 围栏摘要）
        return {"fence_type": "polygon", "vertices": len(self.points)}


def parse_fence(raw: object) -> CircleFence | PolygonFence:
    """解析并校验 fence_json（契约 x-hunter-geofence.fence_schema；非法抛 FenceDefinitionError）。"""
    if not isinstance(raw, dict):
        raise FenceDefinitionError("围栏定义必须是 JSON 对象")
    fence_type = raw.get("type")
    if fence_type == "circle":
        center_lat = _require_number(raw.get("center_lat"), "center_lat", -90.0, 90.0)
        center_lng = _require_number(raw.get("center_lng"), "center_lng", -180.0, 180.0)
        radius_m = _require_number(raw.get("radius_m"), "radius_m", 0.0, 20_000_000.0)
        if radius_m <= 0:
            raise FenceDefinitionError("radius_m 必须 > 0")
        return CircleFence(center_lat=center_lat, center_lng=center_lng, radius_m=radius_m)
    if fence_type == "polygon":
        points_raw = raw.get("points")
        if not isinstance(points_raw, list) or len(points_raw) < 3:
            raise FenceDefinitionError("polygon 需要至少 3 个顶点的 points 数组")
        points: list[tuple[float, float]] = []
        for vertex in points_raw:
            if (
                not isinstance(vertex, (list, tuple))
                or len(vertex) != 2
            ):
                raise FenceDefinitionError("polygon 顶点必须为 [lat, lng] 二元组")
            lat = _require_number(vertex[0], "points[].lat", -90.0, 90.0)
            lng = _require_number(vertex[1], "points[].lng", -180.0, 180.0)
            points.append((lat, lng))
        return PolygonFence(points=tuple(points))
    raise FenceDefinitionError(f"未知围栏类型: {fence_type!r}（仅支持 circle/polygon）")


def _require_number(raw: object, name: str, low: float, high: float) -> float:
    """严格数值字段校验（bool/非数/NaN/Inf/越界均视为结构非法）。"""
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise FenceDefinitionError(f"围栏字段 {name} 必须为数值")
    value = float(raw)
    if not math.isfinite(value) or not low <= value <= high:
        raise FenceDefinitionError(f"围栏字段 {name} 超出值域 [{low}, {high}]: {value}")
    return value


class GeofenceRejectedError(ResourceStateConflictError):
    """围栏接管拒绝（3003/409；``details`` 经 error_handlers 专用处理器透传响应 data）。"""

    def __init__(self, message: str, *, reason: str, **context: object) -> None:
        super().__init__(message, details={"reason": reason, **context})


class FenceProvider(Protocol):
    """围栏配置读取接口（PG repo 实现；测试注入替身）。"""

    async def get_fence(self, vehicle_id: str) -> dict[str, object] | None:
        """读取 vehicle_svc.vehicles.fence_json（未配置/未登记 → None）。"""
        ...


class GeofenceChecker:
    """接管围栏门禁（只读：PG fence_json + Redis vehicle:status 定位字段）。"""

    def __init__(
        self, *, fence_repo: FenceProvider, redis: RedisManager, settings: Settings
    ) -> None:
        self._fence_repo = fence_repo
        self._redis = redis
        self._settings = settings

    async def check(self, vehicle_id: str, *, operator_id: str) -> None:
        """创建会话前的围栏校验；放行返回 None，拒绝抛 GeofenceRejectedError（3003）。"""
        raw = await self._fence_repo.get_fence(vehicle_id)
        if raw is None:
            return  # 未配置围栏 → 直接放行（行为与 G-11 前完全一致）
        try:
            fence = parse_fence(raw)
        except FenceDefinitionError as exc:
            self._reject(
                vehicle_id,
                operator_id,
                REASON_FENCE_MALFORMED,
                message=f"车辆 {vehicle_id} 围栏配置非法，暂时无法接管",
                error=str(exc),
            )
        lat_lng = await self._read_position(vehicle_id)
        if lat_lng is None:
            self._reject(
                vehicle_id,
                operator_id,
                REASON_POSITION_UNAVAILABLE,
                message=f"车辆 {vehicle_id} 无有效定位（车端 GPS 未上报或数据陈旧），围栏内不允许接管",
            )
        lat, lng = lat_lng
        if fence.contains(lat, lng):
            return
        # 位置在围栏外：data 携带 distance_m（circle）/围栏摘要（polygon）
        self._reject(
            vehicle_id,
            operator_id,
            REASON_OUTSIDE_GEOFENCE,
            message=f"车辆 {vehicle_id} 当前位置在电子围栏外，不允许接管",
            **fence.violation_summary(lat, lng),
        )

    async def _read_position(self, vehicle_id: str) -> tuple[float, float] | None:
        """读取并校验 GPS 定位（lat/lng 成对 + 值域 + lat_lng_at 新鲜度；任一不满足 → None）。"""
        mapping = await self._redis.client.hgetall(
            KEY_VEHICLE_STATUS.format(vehicle_id=vehicle_id)
        )
        if not mapping:
            return None
        lat = _to_float((mapping or {}).get("lat"))
        lng = _to_float((mapping or {}).get("lng"))
        if lat is None or lng is None or not -90.0 <= lat <= 90.0 or not -180.0 <= lng <= 180.0:
            return None
        fetched_at = _to_float((mapping or {}).get("lat_lng_at"))
        if fetched_at is None:
            return None
        max_age = float(self._settings.rc_geofence_position_max_age_s)
        # 新鲜度仅设上限不设下限：写入侧 f"{:.3f}" 舍入可能使 lat_lng_at 比读取时刻大
        # 亚毫秒级“未来值”（同进程/时钟回拨场景），不得据此误判定位不可信
        if time.time() - fetched_at > max_age:
            return None
        return lat, lng

    def _reject(
        self, vehicle_id: str, operator_id: str, reason: str, *, message: str, **context: object
    ) -> None:
        """计数 + 结构化审计日志 + 抛出 3003（永不静默放行）。"""
        GEOFENCE_REJECTED_TOTAL.labels(reason=reason).inc()
        logger.warning(
            "geofence_rejected",
            vehicle_id=vehicle_id,
            operator_id=operator_id,
            reason=reason,
            **context,
        )
        raise GeofenceRejectedError(message, reason=reason, **context)


def _to_float(raw: object) -> float | None:
    """Redis Hash 字段宽松浮点解析（缺失/脏数据 → None，不伪造定位）。"""
    if raw is None:
        return None
    try:
        value = float(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


__all__ = [
    "EARTH_RADIUS_M",
    "REASON_FENCE_MALFORMED",
    "REASON_OUTSIDE_GEOFENCE",
    "REASON_POSITION_UNAVAILABLE",
    "CircleFence",
    "FenceDefinitionError",
    "FenceProvider",
    "GeofenceChecker",
    "GeofenceRejectedError",
    "PolygonFence",
    "haversine_m",
    "parse_fence",
    "point_in_polygon",
]
