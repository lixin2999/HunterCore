"""反向代理转发测试（契约 x-hunter-gateway-routes / route_headers）。

覆盖：路径与查询透传、身份头注入与覆盖（防伪造）、Authorization 剥离、
未认证拒绝、未知前缀 404（3001）、pending 前缀 503（5001）、
后端连接失败 503（5001）、熔断开启、上游错误响应透传。
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
from httpx import AsyncClient

from app.config import settings
from app.main import app
from app.services.proxy_service import reset_circuit_breakers
from app.tests.conftest import FakeRedis, make_access_token, seed_session

pytestmark = pytest.mark.asyncio

TEST_USER = "22222222-2222-4222-8222-222222222222"
RESPONSE_FIELDS = {"code", "message", "data", "request_id", "timestamp"}


@pytest.fixture()
def mock_backend(fake_redis: FakeRedis) -> AsyncIterator[dict[str, Any]]:
    """伪后端（httpx.MockTransport）：记录收到的请求，返回回显 JSON。"""
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["query"] = request.url.query.decode()
        captured["headers"] = {k.lower(): v for k, v in request.headers.items()}
        captured["body"] = request.content.decode()
        captured["calls"] = captured.get("calls", 0) + 1
        return httpx.Response(200, json={"ok": True, "echo": request.url.path})

    app.state.http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), trust_env=False
    )
    try:
        yield captured
    finally:
        app.state.http_client = None
        reset_circuit_breakers()


async def _authorized_headers(fake_redis: FakeRedis) -> dict[str, str]:
    """构造已认证请求头（签名有效 + 会话存在）。"""
    token = make_access_token(user_id=TEST_USER, username="proxy_user", roles=["operator"])
    await seed_session(fake_redis, token, TEST_USER)
    return {"Authorization": f"Bearer {token}"}


async def test_forwards_path_query_and_identity_headers(
    client: AsyncClient, fake_redis: FakeRedis, mock_backend: dict[str, Any]
) -> None:
    """转发：完整路径（strip_prefix=false）+ 查询串 + 身份头注入（契约 route_headers）。"""
    headers = await _authorized_headers(fake_redis)
    headers["X-User-Id"] = "forged-user"      # 客户端伪造身份头必须被覆盖
    headers["X-Roles"] = "forged-role"
    headers["X-Trace-Id"] = "forged-trace"
    resp = await client.get("/api/v1/scene/scenes/abc?keyword=hunter&page=2", headers=headers)
    assert resp.status_code == 200
    assert mock_backend["method"] == "GET"
    assert mock_backend["path"] == "/api/v1/scene/scenes/abc"   # 全路径转发
    assert "keyword=hunter" in mock_backend["query"] and "page=2" in mock_backend["query"]
    sent = mock_backend["headers"]
    assert sent["x-user-id"] == TEST_USER       # 身份头以网关注入为准
    assert sent["x-roles"] == "operator"
    assert sent["x-trace-id"]                   # 链路 trace_id
    assert sent["x-request-id"] == sent["x-trace-id"]
    assert resp.headers["x-request-id"]         # 响应侧 trace 头保持


async def test_authorization_stripped_and_body_forwarded(
    client: AsyncClient, fake_redis: FakeRedis, mock_backend: dict[str, Any]
) -> None:
    """客户端 Authorization 不透传（后端信任网关注入）；POST 请求体转发。"""
    headers = await _authorized_headers(fake_redis)
    resp = await client.post(
        "/api/v1/scene/scenes", headers=headers, json={"scene_name": "demo"}
    )
    assert resp.status_code == 200
    sent = mock_backend["headers"]
    assert "authorization" not in sent           # 禁止透传客户端 Token
    assert mock_backend["body"]                  # JSON 体到达后端
    assert "scene_name" in mock_backend["body"]


async def test_forward_requires_auth(client: AsyncClient, mock_backend: dict[str, Any]) -> None:
    """未认证转发请求 → 401 + 1001；后端零调用（JWT 校验在网关完成）。"""
    resp = await client.get("/api/v1/scene/scenes")
    assert resp.status_code == 401
    assert resp.json()["code"] == 1001
    assert mock_backend.get("calls", 0) == 0


async def test_forward_garbage_token_401(
    client: AsyncClient, mock_backend: dict[str, Any]
) -> None:
    """垃圾 JWT → 401 + 1001（签名校验，后端零调用）。"""
    resp = await client.get(
        "/api/v1/scene/scenes", headers={"Authorization": "Bearer bad.token.sig"}
    )
    assert resp.status_code == 401
    assert resp.json()["code"] == 1001
    assert mock_backend.get("calls", 0) == 0


async def test_unknown_prefix_404(
    client: AsyncClient, mock_backend: dict[str, Any]
) -> None:
    """路由表外未知路径 → 404 + code=3001（统一响应体，禁止裸 404）。

    认证顺序：先路由表匹配、后认证 —— 无 Token 的未知路径也必须 404（不得
    401 拦截；L5 契约面 test_unknown_path_unified_error_body 强制校验）。
    """
    no_token = await client.get("/api/v1/unknown/resource")
    assert no_token.status_code == 404
    assert set(no_token.json()) == RESPONSE_FIELDS
    assert no_token.json()["code"] == 3001
    root_unknown = await client.get("/__l5_unknown__")
    assert root_unknown.status_code == 404
    assert root_unknown.json()["code"] == 3001
    assert mock_backend.get("calls", 0) == 0


async def test_pending_prefix_returns_503(
    client: AsyncClient, fake_redis: FakeRedis, mock_backend: dict[str, Any]
) -> None:
    """vehicle-service 归属待确认（契约 pending_confirmation，无后端 URL）→ 503 + 5001。"""
    assert settings.vehicle_service_url is None
    headers = await _authorized_headers(fake_redis)
    resp = await client.get("/api/v1/vehicle/list", headers=headers)
    assert resp.status_code == 503
    body = resp.json()
    assert body["code"] == 5001
    assert mock_backend.get("calls", 0) == 0


async def test_backend_failure_503_and_circuit_breaker(
    client: AsyncClient, fake_redis: FakeRedis, monkeypatch: pytest.MonkeyPatch
) -> None:
    """后端连接失败 → 503 + 5001；连续失败达到阈值 → 熔断（后端零调用）。"""
    monkeypatch.setattr(settings, "circuit_breaker_failure_threshold", 1)

    def failing_handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("backend down", request=request)

    app.state.http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(failing_handler), trust_env=False
    )
    try:
        headers = await _authorized_headers(fake_redis)
        first = await client.get("/api/v1/analytics/reports", headers=headers)
        assert first.status_code == 503
        assert first.json()["code"] == 5001
        second = await client.get("/api/v1/analytics/reports", headers=headers)
        assert second.status_code == 503       # 熔断开启（不再请求后端）
        assert second.json()["code"] == 5001
    finally:
        app.state.http_client = None
        reset_circuit_breakers()


async def test_upstream_error_response_passthrough(
    client: AsyncClient, fake_redis: FakeRedis
) -> None:
    """上游业务错误（后端统一响应 404）原样透传（状态码+五字段体），不二次包装。"""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            404,
            json={"code": 3001, "message": "资源不存在", "data": None,
                  "request_id": "upstream-rid", "timestamp": 1724035200},
        )

    app.state.http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), trust_env=False
    )
    try:
        headers = await _authorized_headers(fake_redis)
        resp = await client.get("/api/v1/ota/versions/999", headers=headers)
        assert resp.status_code == 404
        assert resp.json()["code"] == 3001
        assert resp.json()["request_id"] == "upstream-rid"  # 上游体原样
    finally:
        app.state.http_client = None
        reset_circuit_breakers()


async def test_identity_headers_signed_when_hmac_secret_configured(
    client: AsyncClient,
    fake_redis: FakeRedis,
    mock_backend: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """G-02：配置 GATEWAY_HMAC_SECRET 后转发头附 X-Internal-MAC + 时间戳，
    签名可被后端验证；客户端伪造的签名字段一律被覆盖。"""
    from hunter_common.internal_auth import verify_identity_headers

    secret = "gw-hmac-secret-32-bytes-long-xxxxx"
    monkeypatch.setattr(settings, "gateway_hmac_secret", secret)
    headers = await _authorized_headers(fake_redis)
    headers["X-Internal-MAC"] = "forged-mac"          # 伪造签名字段必须被覆盖
    headers["X-Identity-Timestamp"] = "9999999999"
    resp = await client.get("/api/v1/scene/scenes", headers=headers)
    assert resp.status_code == 200
    sent = mock_backend["headers"]
    assert sent["x-internal-mac"] != "forged-mac"
    assert sent["x-identity-timestamp"]
    assert verify_identity_headers(secret, sent)