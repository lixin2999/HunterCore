"""G-11 地理围栏测试（几何纯函数 + parse_fence + GeofenceChecker 判定矩阵 + API 集成）。

契约依据：remote-control.yaml x-hunter-geofence（verdict_rules 5 条，全部拒绝复用 3003）。
"""
from __future__ import annotations

import math
import time
from typing import Any

import pytest
from httpx import AsyncClient
from hunter_common.exceptions import ServiceUnavailableError

from app.services.geofence import (
    REASON_FENCE_MALFORMED,
    REASON_OUTSIDE_GEOFENCE,
    REASON_POSITION_UNAVAILABLE,
    FenceDefinitionError,
    GeofenceRejectedError,
    haversine_m,
    parse_fence,
    point_in_polygon,
)
from app.tests.conftest import (
    OPERATOR_HEADERS,
    VEHICLE_ONLINE,
)

pytestmark = pytest.mark.asyncio

#: 上海人民广场附近基准点（circle 围栏中心）
CENTER_LAT, CENTER_LNG = 31.2304, 121.4737
CIRCLE_FENCE: dict[str, Any] = {
    "type": "circle",
    "center_lat": CENTER_LAT,
    "center_lng": CENTER_LNG,
    "radius_m": 1000,
}
POLYGON_FENCE: dict[str, Any] = {
    "type": "polygon",
    "points": [[31.0, 121.0], [31.0, 122.0], [32.0, 122.0], [32.0, 121.0]],
    "closed": True,
}


def set_position(
    rc_env: Any, vehicle_id: str, lat: object, lng: object, *, age_s: float = 0.0
) -> None:
    """写入 vehicle:status 定位字段（模拟 data-collector 健康消费链路）。"""
    hash_map = rc_env.redis.store[f"vehicle:status:{vehicle_id}"]
    if lat is None:
        hash_map.pop("lat", None)
    else:
        hash_map["lat"] = str(lat)
    if lng is None:
        hash_map.pop("lng", None)
    else:
        hash_map["lng"] = str(lng)
    if age_s < 0:
        hash_map.pop("lat_lng_at", None)
    else:
        hash_map["lat_lng_at"] = f"{time.time() - age_s:.3f}"


# ---------- 几何纯函数 ----------
async def test_haversine_zero_and_known_distance() -> None:
    assert haversine_m(31.2304, 121.4737, 31.2304, 121.4737) == pytest.approx(0.0, abs=1e-6)
    # 上海（31.2304,121.4737）→ 北京（39.9042,116.4074）大圆距离约 1067 km
    sh_to_bj = haversine_m(31.2304, 121.4737, 39.9042, 116.4074)
    assert 1_000_000 < sh_to_bj < 1_100_000


async def test_haversine_one_degree_latitude_approx_111km() -> None:
    d = haversine_m(0.0, 0.0, 1.0, 0.0)
    assert d == pytest.approx(math.pi * 6371008.8 / 180.0, rel=1e-9)


async def test_point_in_polygon_square() -> None:
    square = [(31.0, 121.0), (31.0, 122.0), (32.0, 122.0), (32.0, 121.0)]
    assert point_in_polygon(31.5, 121.5, square) is True
    assert point_in_polygon(30.5, 121.5, square) is False
    assert point_in_polygon(31.5, 122.5, square) is False


# ---------- parse_fence（契约 fence_schema；非法一律 FenceDefinitionError） ----------
async def test_parse_fence_circle_and_polygon() -> None:
    circle = parse_fence(CIRCLE_FENCE)
    assert circle.contains(CENTER_LAT, CENTER_LNG) is True
    polygon = parse_fence(POLYGON_FENCE)
    assert polygon.contains(31.5, 121.5) is True


@pytest.mark.parametrize(
    "raw",
    [
        None,
        "circle",
        {"type": "rect", "a": 1},  # 未知类型
        {"type": "circle"},  # 字段缺失
        {"type": "circle", "center_lat": 91.0, "center_lng": 0.0, "radius_m": 10},  # 值域
        {"type": "circle", "center_lat": 31.0, "center_lng": 121.0, "radius_m": 0},  # 半径非正
        {"type": "circle", "center_lat": True, "center_lng": 121.0, "radius_m": 10},  # bool
        {"type": "polygon", "points": [[0.0, 0.0], [1.0, 1.0]]},  # 顶点不足 3
        {"type": "polygon", "points": [[0.0, 0.0], [1.0, 1.0], [0.0, "x"]]},  # 非数值
        {"type": "polygon", "points": [[0.0, 0.0], [1.0, 1.0], 5]},  # 顶点非二元组
    ],
)
async def test_parse_fence_rejects_malformed(raw: object) -> None:
    with pytest.raises(FenceDefinitionError):
        parse_fence(raw)


async def test_parse_fence_ignores_optional_extras() -> None:
    """max_speed_mps 等可选字段仅作登记（限速执行在车端），不影响解析。"""
    fence = parse_fence({**CIRCLE_FENCE, "max_speed_mps": 2.0})
    assert fence.radius_m == 1000


# ---------- GeofenceChecker 判定矩阵（verdict_rules 5 条） ----------
async def test_check_no_fence_passes_without_reading_position(
    rc_env: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """未配置围栏 → 直接放行且不触碰定位读取（零额外开销）。"""
    calls: list[str] = []
    original = rc_env.geofence._read_position

    async def spy(vehicle_id: str) -> tuple[float, float] | None:
        calls.append(vehicle_id)
        return await original(vehicle_id)

    monkeypatch.setattr(rc_env.geofence, "_read_position", spy)
    await rc_env.geofence.check(VEHICLE_ONLINE, operator_id="op")
    assert calls == []


async def test_check_position_missing_or_stale_rejected(rc_env: Any) -> None:
    rc_env.fence_repo.fences[VEHICLE_ONLINE] = CIRCLE_FENCE
    # lat/lng 缺失（车端未上报 GPS；seed 默认无定位字段）
    with pytest.raises(GeofenceRejectedError) as exc:
        await rc_env.geofence.check(VEHICLE_ONLINE, operator_id="op")
    assert exc.value.code == 3003
    assert exc.value.details["reason"] == REASON_POSITION_UNAVAILABLE

    # lat_lng_at 缺失（成对写入被破坏 → 视为无定位）
    set_position(rc_env, VEHICLE_ONLINE, CENTER_LAT, CENTER_LNG, age_s=-1)
    with pytest.raises(GeofenceRejectedError) as exc:
        await rc_env.geofence.check(VEHICLE_ONLINE, operator_id="op")
    assert exc.value.details["reason"] == REASON_POSITION_UNAVAILABLE

    # 定位陈旧（超过 max_age）
    set_position(rc_env, VEHICLE_ONLINE, CENTER_LAT, CENTER_LNG, age_s=3600)
    with pytest.raises(GeofenceRejectedError) as exc:
        await rc_env.geofence.check(VEHICLE_ONLINE, operator_id="op")
    assert exc.value.details["reason"] == REASON_POSITION_UNAVAILABLE

    # 脏数据（非数/超值域）不伪造定位
    set_position(rc_env, VEHICLE_ONLINE, "not-a-number", CENTER_LNG)
    with pytest.raises(GeofenceRejectedError) as exc:
        await rc_env.geofence.check(VEHICLE_ONLINE, operator_id="op")
    assert exc.value.details["reason"] == REASON_POSITION_UNAVAILABLE
    set_position(rc_env, VEHICLE_ONLINE, 95.0, CENTER_LNG)
    with pytest.raises(GeofenceRejectedError) as exc:
        await rc_env.geofence.check(VEHICLE_ONLINE, operator_id="op")
    assert exc.value.details["reason"] == REASON_POSITION_UNAVAILABLE


async def test_check_fence_malformed_rejected(rc_env: Any) -> None:
    """配置错误不得默许放行（fail-closed，即使定位正常）。"""
    rc_env.fence_repo.fences[VEHICLE_ONLINE] = {"type": "rectangle", "w": 1}
    set_position(rc_env, VEHICLE_ONLINE, CENTER_LAT, CENTER_LNG)
    with pytest.raises(GeofenceRejectedError) as exc:
        await rc_env.geofence.check(VEHICLE_ONLINE, operator_id="op")
    assert exc.value.code == 3003
    assert exc.value.details["reason"] == REASON_FENCE_MALFORMED


async def test_check_circle_inside_passes_outside_rejects_with_distance(rc_env: Any) -> None:
    rc_env.fence_repo.fences[VEHICLE_ONLINE] = CIRCLE_FENCE
    set_position(rc_env, VEHICLE_ONLINE, CENTER_LAT + 0.0005, CENTER_LNG)  # ~55m
    await rc_env.geofence.check(VEHICLE_ONLINE, operator_id="op")  # 放行

    set_position(rc_env, VEHICLE_ONLINE, CENTER_LAT + 0.02, CENTER_LNG)  # ~2.2km
    with pytest.raises(GeofenceRejectedError) as exc:
        await rc_env.geofence.check(VEHICLE_ONLINE, operator_id="op")
    details = exc.value.details
    assert details["reason"] == REASON_OUTSIDE_GEOFENCE
    assert details["fence_type"] == "circle"
    assert details["distance_m"] > 1000


async def test_check_polygon_membership(rc_env: Any) -> None:
    rc_env.fence_repo.fences[VEHICLE_ONLINE] = POLYGON_FENCE
    set_position(rc_env, VEHICLE_ONLINE, 31.5, 121.5)
    await rc_env.geofence.check(VEHICLE_ONLINE, operator_id="op")

    set_position(rc_env, VEHICLE_ONLINE, 30.5, 121.5)
    with pytest.raises(GeofenceRejectedError) as exc:
        await rc_env.geofence.check(VEHICLE_ONLINE, operator_id="op")
    assert exc.value.details["reason"] == REASON_OUTSIDE_GEOFENCE
    assert exc.value.details["fence_type"] == "polygon"


async def test_check_repo_failure_is_service_unavailable(rc_env: Any) -> None:
    """DB 不可读 → 5001（fail-closed，不允许在围栏状态不可知时接管）。"""
    rc_env.fence_repo.fences[VEHICLE_ONLINE] = CIRCLE_FENCE
    rc_env.fence_repo.fail = True
    with pytest.raises(ServiceUnavailableError):
        await rc_env.geofence.check(VEHICLE_ONLINE, operator_id="op")


# ---------- API 集成（POST /session 挂点：可控性判定后、锁前；拒绝不留脏状态） ----------
async def test_api_no_fence_session_created(client: AsyncClient, rc_env: Any) -> None:
    """无围栏车辆行为与 G-11 前完全一致（放行且无定位要求）。"""
    response = await client.post(
        "/api/v1/remote/session",
        headers=OPERATOR_HEADERS,
        json={"vehicle_id": VEHICLE_ONLINE},
    )
    assert response.status_code == 200
    assert response.json()["data"]["session_id"]


async def test_api_outside_geofence_rejected_3003(client: AsyncClient, rc_env: Any) -> None:
    rc_env.fence_repo.fences[VEHICLE_ONLINE] = CIRCLE_FENCE
    set_position(rc_env, VEHICLE_ONLINE, CENTER_LAT + 0.05, CENTER_LNG)  # ~5.5km 外
    response = await client.post(
        "/api/v1/remote/session",
        headers=OPERATOR_HEADERS,
        json={"vehicle_id": VEHICLE_ONLINE},
    )
    assert response.status_code == 409
    body = response.json()
    assert body["code"] == 3003
    # 契约：details 透传 data（reason + distance_m 围栏摘要）
    assert body["data"]["reason"] == REASON_OUTSIDE_GEOFENCE
    assert body["data"]["distance_m"] > 1000
    # 拒绝发生在校验期：不写会话 Hash、不投 boot/信令帧
    assert f"rc:session:{VEHICLE_ONLINE}" not in rc_env.redis.store
    assert rc_env.frame.frames == []
    assert rc_env.command.commands == []


async def test_api_position_unavailable_rejected_3003(client: AsyncClient, rc_env: Any) -> None:
    rc_env.fence_repo.fences[VEHICLE_ONLINE] = CIRCLE_FENCE
    response = await client.post(
        "/api/v1/remote/session",
        headers=OPERATOR_HEADERS,
        json={"vehicle_id": VEHICLE_ONLINE},
    )
    assert response.status_code == 409
    assert response.json()["code"] == 3003
    assert response.json()["data"]["reason"] == REASON_POSITION_UNAVAILABLE


async def test_api_fence_inside_session_created(client: AsyncClient, rc_env: Any) -> None:
    rc_env.fence_repo.fences[VEHICLE_ONLINE] = CIRCLE_FENCE
    set_position(rc_env, VEHICLE_ONLINE, CENTER_LAT, CENTER_LNG + 0.001)
    response = await client.post(
        "/api/v1/remote/session",
        headers=OPERATOR_HEADERS,
        json={"vehicle_id": VEHICLE_ONLINE},
    )
    assert response.status_code == 200
    assert response.json()["data"]["session_id"]
