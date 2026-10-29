"""服务令牌测试（契约 x-hunter-service-token / redis-keys.yaml 第 17 条；V1.23.6）。

覆盖核心诉求：**服务令牌与用户单会话完全隔离**——同一账号再次登录只覆盖
``session:{user_id}``，脚本/仿真持有的服务令牌（``session:service:{client_id}``）
继续可用，两个模块可并行运行。

覆盖范围：签发（TTL/角色/参数校验/非管理员 403）、并行（再次登录不掉线）、
撤销（即时失效 + 幂等）、轮换（同 client_id 覆盖）、能力面收窄（/me 拒绝）、
网关转发身份注入（X-User-Id=service:{client_id} / X-Roles=data-uploader）。
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
from httpx import AsyncClient

from app.main import app
from app.services.proxy_service import reset_circuit_breakers
from app.tests.conftest import FakeRedis, FakeUserRepository

pytestmark = pytest.mark.asyncio

LOGIN = "/api/v1/user/login"
ME = "/api/v1/user/me"
ISSUE = "/api/v1/user/service-token"
REVOKE = "/api/v1/user/service-token/{client_id}"
PRESIGN = "/api/v1/data/files/presign"
RESPONSE_FIELDS = {"code", "message", "data", "request_id", "timestamp"}

CLIENT_ID = "hunter-sim"
SERVICE_KEY = f"session:service:{CLIENT_ID}"


def service_key(client_id: str = CLIENT_ID) -> str:
    """服务令牌会话键（契约 redis-keys.yaml 第 17 条）。"""
    return f"session:service:{client_id}"


@pytest.fixture()
def mock_backend(fake_redis: FakeRedis) -> AsyncIterator[dict[str, Any]]:
    """伪后端（httpx.MockTransport）：校验转发链路的注入头与服务令牌可用性。"""
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["headers"] = {k.lower(): v for k, v in request.headers.items()}
        captured["calls"] = captured.get("calls", 0) + 1
        return httpx.Response(200, json={"code": 0, "message": "success"})

    app.state.http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), trust_env=False
    )
    try:
        yield captured
    finally:
        app.state.http_client = None
        reset_circuit_breakers()


async def _admin_headers(client: AsyncClient) -> dict[str, str]:
    """登录 admin 获取控制台 Access Token（签发服务令牌的前提）。"""
    resp = await client.post(LOGIN, json={"username": "admin", "password": "Admin@12345"})
    assert resp.status_code == 200
    return {"Authorization": f"Bearer {resp.json()['data']['access_token']}"}


async def _issue(
    client: AsyncClient,
    headers: dict[str, str],
    *,
    client_id: str = CLIENT_ID,
    ttl_seconds: int | None = None,
) -> dict[str, Any]:
    """签发服务令牌（返回 data 段）。"""
    payload: dict[str, Any] = {"client_id": client_id}
    if ttl_seconds is not None:
        payload["ttl_seconds"] = ttl_seconds
    resp = await client.post(ISSUE, headers=headers, json=payload)
    assert resp.status_code == 200, resp.text
    return resp.json()["data"]


def _bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


# =====================================================================
# 签发：载荷 / 会话键 / TTL（契约 ServiceToken + redis-keys 第 17 条）
# =====================================================================
async def test_issue_service_token_writes_isolated_session(
    client: AsyncClient, fake_redis: FakeRedis
) -> None:
    """签发成功：统一五字段 + 服务角色 + 会话键 session:service:{client_id}（不写用户会话）。"""
    resp = await client.post(
        ISSUE, headers=await _admin_headers(client), json={"client_id": CLIENT_ID}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert set(body) == RESPONSE_FIELDS
    assert body["code"] == 0
    data = body["data"]
    assert data["client_id"] == CLIENT_ID
    assert data["token_type"] == "Bearer"
    assert data["roles"] == ["data-uploader"]           # 服务角色（不在 data:read 白名单）
    assert data["expires_in"] == 604800                  # 默认 7 天
    # 服务令牌会话写入独立键，TTL 与返回一致
    assert await fake_redis.get(service_key()) == data["access_token"]
    assert fake_redis.ttls[service_key()] == 604800
    # 用户会话键未被触碰：服务令牌只存在于独立键（session:{user_id} 仍为登录 Token）
    holders = [key for key, value in fake_redis.store.items() if value == data["access_token"]]
    assert holders == [service_key()]


async def test_issue_ttl_seconds_respected(
    client: AsyncClient, fake_redis: FakeRedis
) -> None:
    """ttl_seconds 可缩短有效期（契约 300..2592000），会话 TTL 与返回值一致。"""
    data = await _issue(client, await _admin_headers(client), ttl_seconds=600)
    assert data["expires_in"] == 600
    assert fake_redis.ttls[service_key()] == 600


async def test_issue_validation_422(client: AsyncClient) -> None:
    """client_id 格式（小写 + 3..64）与 ttl_seconds 越界 → 422 + 2001。"""
    headers = await _admin_headers(client)
    for payload in (
        {"client_id": "ab"},                          # 过短（minLength=3）
        {"client_id": "Hunter_Sim"},                  # 大写不符 pattern
        {"client_id": "hunter sim"},                   # 空格不符 pattern
        {"client_id": CLIENT_ID, "ttl_seconds": 299},   # 低于下限 300
        {"client_id": CLIENT_ID, "ttl_seconds": 2592001},  # 超过上限 30 天
    ):
        resp = await client.post(ISSUE, headers=headers, json=payload)
        assert resp.status_code == 422, payload
        assert resp.json()["code"] == 2001


async def test_issue_requires_admin_role(
    client: AsyncClient, fake_users: FakeUserRepository
) -> None:
    """非 SERVICE_TOKEN_ISSUE_ROLES 角色签发 → 403 + 1002（默认仅 admin）。"""
    fake_users.add_user(username="operator1", password="Operator@123", roles=["operator"])
    login = await client.post(
        LOGIN, json={"username": "operator1", "password": "Operator@123"}
    )
    resp = await client.post(
        ISSUE,
        headers={"Authorization": f"Bearer {login.json()['data']['access_token']}"},
        json={"client_id": CLIENT_ID},
    )
    assert resp.status_code == 403
    assert resp.json()["code"] == 1002


async def test_issue_without_token_401(client: AsyncClient) -> None:
    """未携带 Bearer → 401 + 1001（服务令牌不能匿名签发）。"""
    resp = await client.post(ISSUE, json={"client_id": CLIENT_ID})
    assert resp.status_code == 401
    assert resp.json()["code"] == 1001


# =====================================================================
# 并行：再次登录不再踢掉服务令牌（核心诉求）
# =====================================================================
async def test_service_token_survives_user_relogin(
    client: AsyncClient, fake_redis: FakeRedis, mock_backend: dict[str, Any]
) -> None:
    """管理员签发服务令牌后再次登录：服务令牌继续可用，用户单会话语义保持不变。

    这正是 HunterSim 与 Web 前端并行运行所需的语义：重新登录只覆盖
    ``session:{user_id}``，不影响 ``session:service:{client_id}``。
    """
    admin_headers = await _admin_headers(client)
    data = await _issue(client, admin_headers)
    service_headers = _bearer(data["access_token"])

    first = await client.post(PRESIGN, headers=service_headers, json={"vehicle_id": "HUNTER-001"})
    assert first.status_code == 200

    # 管理员重新登录（覆盖 session:{user_id}）
    relogin = await client.post(LOGIN, json={"username": "admin", "password": "Admin@12345"})
    assert relogin.status_code == 200
    new_user_token = relogin.json()["data"]["access_token"]
    assert new_user_token != admin_headers["Authorization"].split(" ")[1]

    # 旧用户 Token 失效（单会话语义保留）
    stale = await client.get(ME, headers=admin_headers)
    assert stale.status_code == 401
    assert stale.json()["code"] == 1001
    # 新用户 Token 有效
    fresh = await client.get(ME, headers=_bearer(new_user_token))
    assert fresh.status_code == 200
    # 服务令牌 **不受影响**（独立会话键）
    again = await client.post(PRESIGN, headers=service_headers, json={"vehicle_id": "HUNTER-001"})
    assert again.status_code == 200
    assert await fake_redis.get(service_key()) == data["access_token"]


async def test_proxy_injects_service_identity_headers(
    client: AsyncClient, mock_backend: dict[str, Any]
) -> None:
    """转发：X-User-Id=service:{client_id} / X-Roles=data-uploader；Authorization 不透传。"""
    data = await _issue(client, await _admin_headers(client))
    resp = await client.post(
        PRESIGN, headers=_bearer(data["access_token"]), json={"vehicle_id": "HUNTER-001"}
    )
    assert resp.status_code == 200
    sent = mock_backend["headers"]
    assert sent["x-user-id"] == f"service:{CLIENT_ID}"
    assert sent["x-roles"] == "data-uploader"
    assert "authorization" not in sent
    assert mock_backend["path"] == PRESIGN


# =====================================================================
# 撤销 / 轮换：均可即时失效，且不影响用户会话
# =====================================================================
async def test_revoke_invalidates_immediately_and_idempotent(
    client: AsyncClient, fake_redis: FakeRedis, mock_backend: dict[str, Any]
) -> None:
    """DELETE 撤销 → 令牌立即 401；重复撤销仍 200 code=0（幂等，不泄露存在性）。"""
    admin_headers = await _admin_headers(client)
    data = await _issue(client, admin_headers)
    service_headers = _bearer(data["access_token"])
    assert (await client.post(PRESIGN, headers=service_headers, json={})).status_code == 200

    first = await client.delete(REVOKE.format(client_id=CLIENT_ID), headers=admin_headers)
    assert first.status_code == 200
    assert first.json()["code"] == 0
    assert first.json()["data"] is None
    assert await fake_redis.exists(service_key()) is False

    revoked = await client.post(PRESIGN, headers=service_headers, json={})
    assert revoked.status_code == 401
    assert revoked.json()["code"] == 1001

    again = await client.delete(REVOKE.format(client_id=CLIENT_ID), headers=admin_headers)
    assert again.status_code == 200
    assert again.json()["code"] == 0
    # 用户会话未受影响（撤销只 DEL 服务令牌键）
    assert (await client.get(ME, headers=admin_headers)).status_code == 200


async def test_reissue_rotates_old_token(
    client: AsyncClient, mock_backend: dict[str, Any]
) -> None:
    """同 client_id 重复签发即轮换：旧令牌 401，新令牌可用（会话覆盖语义）。"""
    admin_headers = await _admin_headers(client)
    old = await _issue(client, admin_headers)
    new = await _issue(client, admin_headers, ttl_seconds=600)
    assert new["access_token"] != old["access_token"]

    stale = await client.post(PRESIGN, headers=_bearer(old["access_token"]), json={})
    assert stale.status_code == 401
    assert stale.json()["code"] == 1001
    ok = await client.post(PRESIGN, headers=_bearer(new["access_token"]), json={})
    assert ok.status_code == 200


async def test_revoke_requires_admin_role(
    client: AsyncClient, fake_users: FakeUserRepository
) -> None:
    """撤销同样需要 SERVICE_TOKEN_ISSUE_ROLES → 非 admin 403 + 1002。"""
    fake_users.add_user(username="analyst1", password="Analyst@123", roles=["analyst"])
    login = await client.post(LOGIN, json={"username": "analyst1", "password": "Analyst@123"})
    resp = await client.delete(
        REVOKE.format(client_id=CLIENT_ID),
        headers={"Authorization": f"Bearer {login.json()['data']['access_token']}"},
    )
    assert resp.status_code == 403
    assert resp.json()["code"] == 1002


# =====================================================================
# 能力面收窄：服务令牌不得访问控制台端点
# =====================================================================
async def test_service_token_rejected_by_console_endpoints(client: AsyncClient) -> None:
    """服务令牌访问 /me（及再签发）→ 401 + 1001（decode_access_token 仅接受 typ=access）。"""
    data = await _issue(client, await _admin_headers(client))
    headers = _bearer(data["access_token"])

    me = await client.get(ME, headers=headers)
    assert me.status_code == 401
    assert me.json()["code"] == 1001

    escalate = await client.post(ISSUE, headers=headers, json={"client_id": "other-client"})
    assert escalate.status_code == 401
    assert escalate.json()["code"] == 1001

    logout = await client.post("/api/v1/user/logout", headers=headers)
    assert logout.status_code == 401
    assert logout.json()["code"] == 1001
