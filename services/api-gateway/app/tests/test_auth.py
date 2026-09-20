"""统一认证端点测试（基于契约 example 与 login/refresh/logout/me 流程）。

覆盖：登录成功/失败、参数校验（契约 minLength/maxLength/pattern）、
刷新轮换防重放、注销幂等、会话强依赖 503、/me 用户信息、限流 429。
"""
from __future__ import annotations

import pytest
from httpx import AsyncClient
from hunter_common.database.enums import UserStatus

from app.tests.conftest import FakeRedis, FakeUserRepository, seed_session

pytestmark = pytest.mark.asyncio

LOGIN = "/api/v1/user/login"
REFRESH = "/api/v1/user/refresh"
LOGOUT = "/api/v1/user/logout"
ME = "/api/v1/user/me"
CHANGE_PASSWORD = "/api/v1/user/change-password"
RESPONSE_FIELDS = {"code", "message", "data", "request_id", "timestamp"}


# =====================================================================
# 登录（契约 example：admin 正常流程）
# =====================================================================
async def test_login_success_returns_token_pair(
    client: AsyncClient, fake_users: FakeUserRepository
) -> None:
    """登录成功：统一五字段 + TokenPair 结构（契约 ApiResponseTokenPair/TokenPair）。"""
    resp = await client.post(LOGIN, json={"username": "admin", "password": "Admin@12345"})
    assert resp.status_code == 200
    body = resp.json()
    assert set(body) == RESPONSE_FIELDS
    assert body["code"] == 0
    data = body["data"]
    assert data["token_type"] == "Bearer"
    assert data["expires_in"] == 1800           # Access 30min（契约 example 1800，G-04①）
    assert data["refresh_expires_in"] == 604800  # Refresh 7d（契约 example 604800）
    user = data["user"]
    assert user["username"] == "admin"
    assert user["roles"] == ["admin"]
    assert user["permissions"] == ["scene:read", "ota:release"]
    assert user["real_name"] == "管理员"
    # 契约：成功后更新 last_login_time
    assert len(fake_users.last_login_touched) == 1


async def test_login_unknown_user_unified_401(client: AsyncClient) -> None:
    """用户名不存在：401 + code=1001（与密码错误同一响应，防用户名枚举）。"""
    resp = await client.post(LOGIN, json={"username": "ghost", "password": "Whatever@123"})
    assert resp.status_code == 401
    assert resp.json()["code"] == 1001


async def test_login_wrong_password_401_and_failures_locked(
    client: AsyncClient, fake_redis: FakeRedis
) -> None:
    """密码错误 401；连续失败触发 IP 锁定 429（设计文档 14.5 暴力破解防护）。

    语义：失败计数达到阈值即锁定 —— 锁定期间**任何**登录尝试（含正确密码）
    一律 429 + Retry-After，直至锁定窗口过期。
    """
    for _ in range(4):  # 未达 login_max_failures=5：正常 401
        resp = await client.post(LOGIN, json={"username": "admin", "password": "Wrong@12345"})
        assert resp.status_code == 401
        assert resp.json()["code"] == 1001
    # 第 5 次失败：计数达到阈值 → 锁定（429）
    fifth = await client.post(LOGIN, json={"username": "admin", "password": "Wrong@12345"})
    assert fifth.status_code == 429
    assert fifth.json()["code"] == 5001          # 附录 A 无专用错误码，复用 5001
    assert "retry-after" in fifth.headers
    # 锁定期间正确密码同样拒绝（IP 级封禁，fail fast 不做凭证校验）
    locked = await client.post(LOGIN, json={"username": "admin", "password": "Admin@12345"})
    assert locked.status_code == 429
    assert locked.json()["code"] == 5001


async def test_login_disabled_and_locked_user_401(client: AsyncClient) -> None:
    """disabled / locked 账号统一 401 + 1001（不泄露状态原因）。"""
    for username, password in (("disabled_user", "Disable@12345"), ("locked_user", "Locked@12345")):
        resp = await client.post(LOGIN, json={"username": username, "password": password})
        assert resp.status_code == 401
        assert resp.json()["code"] == 1001


async def test_login_validation_422(client: AsyncClient) -> None:
    """参数校验（契约：username ≤64、password 8..128、totp 6 位数字）→ 422 + 2001。"""
    short_password = await client.post(LOGIN, json={"username": "admin", "password": "short"})
    assert short_password.status_code == 422
    assert short_password.json()["code"] == 2001

    bad_totp = await client.post(
        LOGIN, json={"username": "admin", "password": "Admin@12345", "totp_code": "abc"}
    )
    assert bad_totp.status_code == 422
    assert bad_totp.json()["code"] == 2001

    missing = await client.post(LOGIN, json={"username": "admin"})
    assert missing.status_code == 422
    assert missing.json()["code"] == 2001


# =====================================================================
# 刷新（契约：失效/过期 → 1003；一次一换防重放）
# =====================================================================
async def test_refresh_rotates_and_rejects_replay(client: AsyncClient) -> None:
    """刷新成功返回新 Token 对；旧 Refresh Token 重放 → 401 + 1003（防重放）。"""
    login = await client.post(LOGIN, json={"username": "admin", "password": "Admin@12345"})
    first = login.json()["data"]
    refreshed = await client.post(REFRESH, json={"refresh_token": first["refresh_token"]})
    assert refreshed.status_code == 200
    second = refreshed.json()["data"]
    assert second["access_token"] != first["access_token"]
    assert second["refresh_token"] != first["refresh_token"]
    # 旧 Refresh Token 重放：会话已覆盖为新 jti → 1003
    replay = await client.post(REFRESH, json={"refresh_token": first["refresh_token"]})
    assert replay.status_code == 401
    assert replay.json()["code"] == 1003


# =====================================================================
# 注销（契约：幂等；data=null）
# =====================================================================
async def test_logout_revokes_session_and_idempotent(
    client: AsyncClient, fake_redis: FakeRedis
) -> None:
    """注销后原 Access Token 立即失效（/me → 1001）；重复注销仍 200 code=0。"""
    login = await client.post(LOGIN, json={"username": "admin", "password": "Admin@12345"})
    data = login.json()["data"]
    headers = {"Authorization": f"Bearer {data['access_token']}"}
    first = await client.post(LOGOUT, headers=headers)
    assert first.status_code == 200
    assert first.json()["code"] == 0
    assert first.json()["data"] is None
    # 会话已删除
    user_id = data["user"]["user_id"]
    assert await fake_redis.get(f"session:{user_id}") is None
    # 原 Access Token 即刻失效（无会话 → 1001）
    me_after = await client.get(ME, headers=headers)
    assert me_after.status_code == 401
    assert me_after.json()["code"] == 1001
    # 幂等：Token 已失效仍返回 code=0（不泄露 Token 状态）
    again = await client.post(LOGOUT, headers=headers)
    assert again.status_code == 200
    assert again.json()["code"] == 0


async def test_logout_without_token_401(client: AsyncClient) -> None:
    """未携带 Bearer → 401 + 1001（契约 security: bearerAuth）。"""
    resp = await client.post(LOGOUT)
    assert resp.status_code == 401
    assert resp.json()["code"] == 1001


# =====================================================================
# 当前用户（契约 /me：JWT 解析结果 + 用户资料）
# =====================================================================
async def test_me_returns_profile(client: AsyncClient) -> None:
    """登录后 /me 返回用户信息（roles/permissions 供前端 v-permission）。"""
    login = await client.post(LOGIN, json={"username": "admin", "password": "Admin@12345"})
    token = login.json()["data"]["access_token"]
    resp = await client.get(ME, headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["code"] == 0
    profile = body["data"]
    assert profile["username"] == "admin"
    assert profile["roles"] == ["admin"]
    assert profile["permissions"] == ["scene:read", "ota:release"]


async def test_me_without_token_401(client: AsyncClient) -> None:
    """未认证 → 401 + 1001。"""
    resp = await client.get(ME)
    assert resp.status_code == 401
    assert resp.json()["code"] == 1001


async def test_me_garbage_token_401(client: AsyncClient) -> None:
    """无效 JWT → 401 + 1001（Token 缺失或无效）。"""
    resp = await client.get(ME, headers={"Authorization": "Bearer garbage.token.here"})
    assert resp.status_code == 401
    assert resp.json()["code"] == 1001


# =====================================================================
# 会话强依赖（redis-keys.yaml：session 不可用 → 503 + 5001，禁止降级）
# =====================================================================
async def test_me_redis_down_503(client: AsyncClient, fake_redis: FakeRedis) -> None:
    """Redis 会话读取失败 → 503 + code=5001（强依赖，禁止降级放行）。"""
    login = await client.post(LOGIN, json={"username": "admin", "password": "Admin@12345"})
    token = login.json()["data"]["access_token"]

    async def broken_get(key: str) -> str | None:
        raise ConnectionError("redis down")

    app_redis = client._transport.app.state.redis  # type: ignore[attr-defined]
    original_get = app_redis.get
    app_redis.get = broken_get  # type: ignore[method-assign]
    try:
        resp = await client.get(ME, headers={"Authorization": f"Bearer {token}"})
    finally:
        app_redis.get = original_get  # type: ignore[method-assign]
    assert resp.status_code == 503
    assert resp.json()["code"] == 5001


# =====================================================================
# 单会话语义：两次登录互相覆盖会话（session:{user_id} 单值模型）
# =====================================================================
async def test_session_isolated_between_logins(
    client: AsyncClient, fake_redis: FakeRedis
) -> None:
    """两次登录互相覆盖会话（单会话语义）：先登录者 Token 失效。"""
    first = (await client.post(LOGIN, json={"username": "admin", "password": "Admin@12345"})).json()["data"]
    second = (await client.post(LOGIN, json={"username": "admin", "password": "Admin@12345"})).json()["data"]
    user_id = first["user"]["user_id"]
    # 后登录者会话有效
    ok = await client.get(ME, headers={"Authorization": f"Bearer {second['access_token']}"})
    assert ok.status_code == 200
    # 先登录者被新会话覆盖 → 1001（多终端并存需契约扩展，见 README 待确认）
    stale = await client.get(ME, headers={"Authorization": f"Bearer {first['access_token']}"})
    assert stale.status_code == 401
    assert stale.json()["code"] == 1001
    assert await fake_redis.get(f"session:{user_id}") == second["access_token"]


async def test_seed_session_helper(
    client: AsyncClient, fake_redis: FakeRedis, fake_users: FakeUserRepository
) -> None:
    """conftest.seed_session 可直接构造有效会话（转发测试基础设施验证）。"""
    user = fake_users.users["admin"]
    from app.config import settings as cfg
    from app.core.security import create_access_token

    access, _ = create_access_token(
        user_id=str(user["user_id"]),
        username="admin",
        roles=["admin"],
        permissions=[],
        settings=cfg,
    )
    await seed_session(fake_redis, access, str(user["user_id"]))
    resp = await client.get(ME, headers={"Authorization": f"Bearer {access}"})
    assert resp.status_code == 200
    assert resp.json()["data"]["user_id"] == str(user["user_id"])


@pytest.mark.parametrize(
    "username,status",
    [("locked_user", UserStatus.LOCKED), ("disabled_user", UserStatus.DISABLED)],
)
async def test_non_enabled_status_never_logs_in(
    client: AsyncClient, username: str, status: UserStatus
) -> None:
    """参数化：非 enabled 状态（locked/disabled）一律拒绝登录（1001）。"""
    resp = await client.post(LOGIN, json={"username": username, "password": "Admin@12345"})
    assert resp.status_code == 401
    assert resp.json()["code"] == 1001


async def test_refresh_invalid_token_401(client: AsyncClient) -> None:
    """伪造/垃圾 Refresh Token → 401 + 1003（契约统一需重新登录）。"""
    resp = await client.post(REFRESH, json={"refresh_token": "not-a-jwt"})
    assert resp.status_code == 401
    assert resp.json()["code"] == 1003


async def test_refresh_after_logout_rejected(client: AsyncClient) -> None:
    """登出后 Refresh Token 不可再用（会话撤销 → 1003 需重新登录）。"""
    login = await client.post(LOGIN, json={"username": "admin", "password": "Admin@12345"})
    data = login.json()["data"]
    await client.post(LOGOUT, headers={"Authorization": f"Bearer {data['access_token']}"})
    resp = await client.post(REFRESH, json={"refresh_token": data["refresh_token"]})
    assert resp.status_code == 401
    assert resp.json()["code"] == 1003


# =====================================================================
# G-06 首登强制改密（must_change_password 贯穿 + /user/change-password）
# =====================================================================
async def test_login_returns_must_change_password_flag(
    client: AsyncClient, fake_users: FakeUserRepository
) -> None:
    """初始化账号登录：TokenPair.user.must_change_password=true（前端据此引导改密）。"""
    fake_users.add_user(
        username="bootstrap_admin", password="Init@12345", roles=["admin"],
        must_change_password=True,
    )
    resp = await client.post(LOGIN, json={"username": "bootstrap_admin", "password": "Init@12345"})
    assert resp.status_code == 200
    user = resp.json()["data"]["user"]
    assert user["must_change_password"] is True
    # 存量账号默认 False（不影响普通用户）
    normal = await client.post(LOGIN, json={"username": "admin", "password": "Admin@12345"})
    assert normal.json()["data"]["user"]["must_change_password"] is False


async def test_me_returns_must_change_password(client: AsyncClient) -> None:
    """/me 从 DB 读标志（非 JWT 载荷；改密后复查可验证复位）。"""
    login = await client.post(LOGIN, json={"username": "admin", "password": "Admin@12345"})
    headers = {"Authorization": f"Bearer {login.json()['data']['access_token']}"}
    resp = await client.get(ME, headers=headers)
    assert resp.status_code == 200
    assert resp.json()["data"]["must_change_password"] is False


async def test_change_password_flow(client: AsyncClient, fake_users: FakeUserRepository) -> None:
    """首登改密全流程：旧口令错误 1001 → 强度不符 422 → 相同口令 422 → 成功后
    新口令可登录、标志复位、旧口令失效；当前会话不强制撤销。"""
    fake_users.add_user(
        username="bootstrap_admin", password="Init@12345", roles=["admin"],
        must_change_password=True,
    )
    login = await client.post(LOGIN, json={"username": "bootstrap_admin", "password": "Init@12345"})
    assert login.json()["data"]["user"]["must_change_password"] is True
    headers = {"Authorization": f"Bearer {login.json()['data']['access_token']}"}

    # 旧口令错误 → 1001（统一认证失败，不泄露原因）
    bad_old = await client.post(
        CHANGE_PASSWORD, headers=headers,
        json={"old_password": "Wrong@12345", "new_password": "NewPass@12345"},
    )
    assert bad_old.status_code == 401
    assert bad_old.json()["code"] == 1001

    # 新口令强度不符（无大写）→ 422 + 2001
    weak = await client.post(
        CHANGE_PASSWORD, headers=headers,
        json={"old_password": "Init@12345", "new_password": "alllowercase1"},
    )
    assert weak.status_code == 422
    assert weak.json()["code"] == 2001

    # 新旧相同 → 422 + 2001
    same = await client.post(
        CHANGE_PASSWORD, headers=headers,
        json={"old_password": "Init@12345", "new_password": "Init@12345"},
    )
    assert same.status_code == 422
    assert same.json()["code"] == 2001

    # 改密成功：data=null，五字段统一响应
    ok = await client.post(
        CHANGE_PASSWORD, headers=headers,
        json={"old_password": "Init@12345", "new_password": "NewPass@12345"},
    )
    assert ok.status_code == 200
    assert ok.json()["code"] == 0
    assert ok.json()["data"] is None

    # 当前会话未强制撤销（契约：改密不强制重登；/me 仍为改密后标志 False）
    me_after = await client.get(ME, headers=headers)
    assert me_after.status_code == 200
    assert me_after.json()["data"]["must_change_password"] is False

    # 新口令可登录且标志复位；旧口令失效（注意：单值会话下重新登录会覆盖旧 Token）
    relogin = await client.post(LOGIN, json={"username": "bootstrap_admin", "password": "NewPass@12345"})
    assert relogin.status_code == 200
    assert relogin.json()["data"]["user"]["must_change_password"] is False
    stale = await client.post(LOGIN, json={"username": "bootstrap_admin", "password": "Init@12345"})
    assert stale.status_code == 401


async def test_change_password_without_token_401(client: AsyncClient) -> None:
    """未携带 Bearer → 401 + 1001（契约 security: bearerAuth）。"""
    resp = await client.post(
        CHANGE_PASSWORD,
        json={"old_password": "Admin@12345", "new_password": "NewPass@12345"},
    )
    assert resp.status_code == 401
    assert resp.json()["code"] == 1001