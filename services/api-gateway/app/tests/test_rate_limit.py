"""限流中间件测试（设计文档附录 D；契约 x-hunter-rate-limits）。

覆盖：单 IP 全局限流、单用户全局限流、接口级限流（user 维度）、
ops 端点豁免、Redis 故障 fail-open、限流键模式与 429 响应头。
"""
from __future__ import annotations

import pytest
from httpx import AsyncClient

from app.config import settings
from app.tests.conftest import FakeRedis, make_access_token

pytestmark = pytest.mark.asyncio

FORWARDED_PATH = "/api/v1/scene/scenes"          # 契约转发前缀（scene-service）
ENDPOINT_LIMITED = "/api/v1/ota/versions"        # 附录 D：POST 5 QPS（user 维度）
RESPONSE_FIELDS = {"code", "message", "data", "request_id", "timestamp"}


async def test_per_ip_global_limit(client: AsyncClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """单 IP 200 QPS（附录 D；测试阈值收紧为 2）→ 第 3 次 429。"""
    monkeypatch.setattr(settings, "rate_limit_per_ip_qps", 2)
    for _ in range(2):
        resp = await client.get(FORWARDED_PATH)
        assert resp.status_code == 401  # 未认证（限流先计数，认证后拒绝）
    blocked = await client.get(FORWARDED_PATH)
    assert blocked.status_code == 429
    body = blocked.json()
    assert set(body) == RESPONSE_FIELDS
    assert body["code"] == 5001         # 附录 A 无专用错误码，复用 5001
    assert blocked.headers["retry-after"] == "60"
    assert blocked.headers["x-ratelimit-limit"] == "2"
    assert blocked.headers["x-ratelimit-remaining"] == "0"


async def test_per_user_global_limit(client: AsyncClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """单用户 100 QPS（附录 D；测试阈值收紧为 2）→ 第 3 次 429（无需有效会话）。"""
    monkeypatch.setattr(settings, "rate_limit_per_user_qps", 2)
    token = make_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    for _ in range(2):
        resp = await client.get(FORWARDED_PATH, headers=headers)
        assert resp.status_code == 401  # 无会话被认证拒绝，但限流已按用户计数
    blocked = await client.get(FORWARDED_PATH, headers=headers)
    assert blocked.status_code == 429
    assert blocked.json()["code"] == 5001


async def test_endpoint_limit_for_ota_upload(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """接口级限流：POST /api/v1/ota/versions 单用户 5 QPS（附录 D，不可更改）。"""
    monkeypatch.setattr(settings, "rate_limit_per_user_qps", 10000)
    token = make_access_token(roles=["admin"])
    headers = {"Authorization": f"Bearer {token}"}
    for _ in range(5):
        resp = await client.post(ENDPOINT_LIMITED, headers=headers, json={"version_name": "v"})
        assert resp.status_code == 401  # 无会话被认证拒绝，但接口级计数生效
    blocked = await client.post(ENDPOINT_LIMITED, headers=headers, json={"version_name": "v"})
    assert blocked.status_code == 429
    assert blocked.headers["x-ratelimit-limit"] == "5"
    assert blocked.headers["retry-after"]


async def test_ops_endpoints_exempt(client: AsyncClient) -> None:
    """/healthz /readyz /metrics 不经 /api/v1 前缀 → 不参与限流（K8s probe 语义）。"""
    for _ in range(5):
        resp = await client.get("/healthz")
        assert resp.status_code == 200


async def test_rate_limit_fail_open_when_redis_down(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Redis 故障 fail-open（rate_limit 非强依赖）：放行，由认证层决定 401。"""
    monkeypatch.setattr(settings, "rate_limit_per_ip_qps", 1)

    class BrokenRedis(FakeRedis):
        async def incr_with_expire(self, key: str, expire_seconds: int = 60) -> int:
            raise ConnectionError("redis down")

    app_redis = client._transport.app.state.redis  # type: ignore[attr-defined]
    original = app_redis.incr_with_expire
    app_redis.incr_with_expire = BrokenRedis.incr_with_expire  # type: ignore[method-assign]
    try:
        for _ in range(3):
            resp = await client.get(FORWARDED_PATH)
            assert resp.status_code == 401  # 放行（非 429），认证层统一拒绝
    finally:
        app_redis.incr_with_expire = original  # type: ignore[method-assign]


async def test_rate_limit_key_pattern(client: AsyncClient, fake_redis: FakeRedis) -> None:
    """限流键遵循契约模式 rate_limit:{ip}:{api}（redis-keys.yaml 第 4 条）。"""
    await client.get("/healthz")  # 豁免路径不计数
    await client.get(FORWARDED_PATH)  # 默认单 IP 200 QPS，1 次
    keys = [key for key in fake_redis.store if key.startswith("rate_limit:")]
    assert keys, "限流计数键必须写入"
    assert keys[0].endswith(":global")
    assert keys[0].count(":") == 2  # rate_limit:{identity}:{scope}


async def test_login_rate_limit_per_ip(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """登录接口频率限制（14.5 暴力破解防护；测试阈值 2/分钟）→ 第 3 次 429。"""
    monkeypatch.setattr(settings, "login_rate_limit_per_ip", 2)
    for _ in range(2):
        resp = await client.post(
            "/api/v1/user/login", json={"username": "ghost", "password": "Whatever@123"}
        )
        assert resp.status_code == 401
    blocked = await client.post(
        "/api/v1/user/login", json={"username": "admin", "password": "Admin@12345"}
    )
    assert blocked.status_code == 429
    assert blocked.json()["code"] == 5001
