"""/api/v1/remote 路由 API 层测试（RBAC + 错误码 → HTTP 状态映射 + 端到端生命周期）。"""

from __future__ import annotations

from typing import Any

import pytest
from httpx import AsyncClient

from app.tests.conftest import (
    ADMIN_HEADERS,
    OPERATOR_HEADERS,
    OTHER_HEADERS,
    VEHICLE_BUSY,
    VEHICLE_OFFLINE,
    VEHICLE_ONLINE,
    VIEWER_HEADERS,
)

pytestmark = pytest.mark.asyncio


async def test_requires_forwarded_auth(client: AsyncClient) -> None:
    """缺失网关注入头 → 401 + code=1001（统一响应格式；契约 securitySchemes）。"""
    response = await client.get("/api/v1/remote/vehicles")
    assert response.status_code == 401
    body: dict[str, Any] = response.json()
    assert body["code"] == 1001 and body["data"] is None
    assert body["request_id"] and isinstance(body["timestamp"], int)


async def test_viewer_can_read_but_not_create(client: AsyncClient) -> None:
    """viewer：remote:read 放行查询，remote:create 缺失 → 403 + 1002（契约 RBAC）。"""
    read = await client.get("/api/v1/remote/vehicles", headers=VIEWER_HEADERS)
    assert read.status_code == 200
    assert read.json()["code"] == 0

    create = await client.post(
        "/api/v1/remote/session",
        headers=VIEWER_HEADERS,
        json={"vehicle_id": VEHICLE_ONLINE},
    )
    assert create.status_code == 403
    assert create.json()["code"] == 1002


async def test_create_session_blocked_reasons(client: AsyncClient) -> None:
    """创建：离线 → 409/4001；升级中 → 409/4002（契约 209 行映射）。"""
    offline = await client.post(
        "/api/v1/remote/session",
        headers=OPERATOR_HEADERS,
        json={"vehicle_id": VEHICLE_OFFLINE},
    )
    assert offline.status_code == 409 and offline.json()["code"] == 4001

    busy = await client.post(
        "/api/v1/remote/session",
        headers=OPERATOR_HEADERS,
        json={"vehicle_id": VEHICLE_BUSY},
    )
    assert busy.status_code == 409 and busy.json()["code"] == 4002


async def test_create_session_invalid_vehicle_id(client: AsyncClient) -> None:
    """车辆 ID 不满足契约 pattern → 422/2001（Pydantic 校验 → 统一响应）。"""
    response = await client.post(
        "/api/v1/remote/session",
        headers=OPERATOR_HEADERS,
        json={"vehicle_id": "XXX-999"},
    )
    assert response.status_code == 422
    assert response.json()["code"] == 2001


async def test_session_lifecycle_e2e(client: AsyncClient, rc_env) -> None:
    """端到端：创建 → 查详情 → 结束（sidecar_written）→ 历史可查 → 录像可访。"""
    create = await client.post(
        "/api/v1/remote/session",
        headers=OPERATOR_HEADERS,
        json={"vehicle_id": VEHICLE_ONLINE},
    )
    assert create.status_code == 200
    session_id = create.json()["data"]["session_id"]
    assert create.json()["data"]["status"] == "connecting"

    detail = await client.get(
        f"/api/v1/remote/session/{session_id}", headers=OPERATOR_HEADERS
    )
    assert (
        detail.status_code == 200 and detail.json()["data"]["control_stats"] is not None
    )

    ended = await client.delete(
        f"/api/v1/remote/session/{session_id}",
        headers=OPERATOR_HEADERS,
        params={"reason": "operator_end"},
    )
    assert ended.status_code == 200
    assert ended.json()["data"]["sidecar_written"] is True
    assert ended.json()["data"]["status"] == "ended"

    # 结束后查询活跃会话 → 3001（幂等，契约 238 行）
    gone = await client.get(
        f"/api/v1/remote/session/{session_id}", headers=OPERATOR_HEADERS
    )
    assert gone.status_code == 404 and gone.json()["code"] == 3001

    # 历史详情（sidecar 已归档）+ 录像对象此刻尚未封存 → video_available=false
    history = await client.get(
        f"/api/v1/remote/history/{session_id}", headers=OPERATOR_HEADERS
    )
    assert history.status_code == 200
    assert history.json()["data"]["video_available"] is False

    video = await client.get(
        f"/api/v1/remote/history/{session_id}/video", headers=OPERATOR_HEADERS
    )
    assert video.status_code == 404 and video.json()["code"] == 3001


async def test_end_session_foreign_operator_forbidden(client: AsyncClient) -> None:
    """他人结束会话 → 403 + 1002（契约 279 行越权保护）。"""
    create = await client.post(
        "/api/v1/remote/session",
        headers=OPERATOR_HEADERS,
        json={"vehicle_id": VEHICLE_ONLINE},
    )
    session_id = create.json()["data"]["session_id"]
    forbidden = await client.delete(
        f"/api/v1/remote/session/{session_id}", headers=OTHER_HEADERS
    )
    assert forbidden.status_code == 403 and forbidden.json()["code"] == 1002

    admin_end = await client.delete(
        f"/api/v1/remote/session/{session_id}", headers=ADMIN_HEADERS
    )
    assert admin_end.status_code == 200


async def test_list_sessions_viewer_forced_own(client: AsyncClient) -> None:
    """viewer 列表仅见自身会话（空）；admin 可见全量（契约 139 行）。"""
    await client.post(
        "/api/v1/remote/session",
        headers=OPERATOR_HEADERS,
        json={"vehicle_id": VEHICLE_ONLINE},
    )
    viewer_view = await client.get("/api/v1/remote/sessions", headers=VIEWER_HEADERS)
    assert viewer_view.status_code == 200 and viewer_view.json()["data"]["total"] == 0

    admin_view = await client.get("/api/v1/remote/sessions", headers=ADMIN_HEADERS)
    assert admin_view.status_code == 200 and admin_view.json()["data"]["total"] == 1


async def test_vehicles_list_filters(client: AsyncClient) -> None:
    """车辆列表：controllable_only 过滤 + vehicle_id 精确匹配（契约 90-99 行）。"""
    all_only = await client.get(
        "/api/v1/remote/vehicles",
        headers=OPERATOR_HEADERS,
        params={"controllable_only": "false"},
    )
    assert all_only.status_code == 200
    assert (
        all_only.json()["data"]["total"] == 2
    )  # 001 可控 + 002 升级中（003 离线不在读模型）

    single = await client.get(
        "/api/v1/remote/vehicles",
        headers=OPERATOR_HEADERS,
        params={"vehicle_id": VEHICLE_ONLINE},
    )
    items = single.json()["data"]["items"]
    assert len(items) == 1 and items[0]["controllable"] is True
    assert items[0]["model"] == "HUNTER_SE"


async def test_history_data_permission_forced_own(client: AsyncClient) -> None:
    """普通用户 history 查询强制 operator_id=自身（他人 sidecar 不可见；契约 324 行）。"""
    response = await client.get("/api/v1/remote/history", headers=OPERATOR_HEADERS)
    assert response.status_code == 200
    assert response.json()["data"]["total"] == 0
    assert response.json()["data"]["source"] == "minio_sidecar"


async def test_session_create_rate_limit(
    client: AsyncClient, rc_env, monkeypatch
) -> None:
    """附录 D：POST /remote/session 单用户 1 QPS → 第 2 次 429 + Retry-After。"""
    monkeypatch.setattr(
        __import__("app.config", fromlist=["settings"]).settings,
        "rc_session_create_rate_limit_per_min",
        1,
    )
    first = await client.post(
        "/api/v1/remote/session",
        headers=OTHER_HEADERS,
        json={"vehicle_id": VEHICLE_ONLINE},
    )
    assert first.status_code == 200
    # 释放车辆占用后再试（排除 7001 干扰；限流先于业务判定触发）
    session_id = first.json()["data"]["session_id"]
    await client.delete(f"/api/v1/remote/session/{session_id}", headers=OTHER_HEADERS)
    second = await client.post(
        "/api/v1/remote/session",
        headers=OTHER_HEADERS,
        json={"vehicle_id": VEHICLE_ONLINE},
    )
    assert second.status_code == 429
    assert second.headers.get("retry-after") is not None
