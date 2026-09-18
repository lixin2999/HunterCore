"""认证依赖：Bearer JWT 校验 + Redis 会话强依赖校验。

契约依据：
- Redis 键模式 ``session:{user_id}``（redis-keys.yaml 第 1 条）：String 值为
  JWT Access Token 原样存储，TTL 7200s，owner/writer/reader = api-gateway
- 强依赖规则（redis-keys.yaml common_rules）：session 键不可用时必须返回
  HTTP 503 + code=5001，**禁止降级为无会话模式**
- 撤销语义（redis-keys.yaml pending #6 选项②）：不引入黑名单键，以
  「会话存在性 + 值与当前 Access Token 全等」替代黑名单——登录写入会话、
  刷新覆盖会话、登出 DEL 会话，旧 Access Token 即刻失效直至自然过期
"""
from __future__ import annotations

from typing import Any

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from hunter_common.exceptions import AuthenticationError, ServiceUnavailableError
from hunter_common.logging import get_logger

from app.config import settings
from app.core.security import decode_access_token

logger = get_logger("app.core.auth")

__all__ = [
    "authenticate_request",
    "get_current_user",
    "get_logout_principal",
    "get_redis_manager",
    "load_session_token",
    "session_key",
]

#: Bearer 方案（契约 securitySchemes.bearerAuth：HTTP Bearer, JWT）
_bearer_scheme = HTTPBearer(auto_error=False, scheme_name="bearerAuth")


def session_key(user_id: str) -> str:
    """构造会话键（契约模式 ``session:{user_id}``，禁止其他拼法）。"""
    return f"session:{user_id}"


def get_redis_manager(request: Request) -> Any:
    """会话存储依赖（RedisManager 或测试注入的兼容对象）。

    应用未完成初始化即视为依赖不可用 → 503（强依赖，禁止降级）。
    返回类型宽松（鸭子类型），以支持单测注入伪 Redis 实现。
    """
    manager = getattr(request.app.state, "redis", None)
    if manager is None:
        raise ServiceUnavailableError("服务不可用")
    return manager


async def load_session_token(redis: Any, user_id: str) -> str:
    """读取当前会话中的 Access Token（空字符串表示无会话）。

    Redis 不可用 → 503（redis-keys.yaml 强依赖规则，禁止降级放行）。
    """
    try:
        return (await redis.get(session_key(user_id))) or ""
    except Exception as exc:
        logger.error("session_backend_unavailable", user_id=user_id)
        raise ServiceUnavailableError("服务不可用") from exc


async def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
    redis: Any = Depends(get_redis_manager),
) -> dict[str, Any]:
    """严格认证依赖（转发请求与 GET /api/v1/user/me 使用）。

    校验链：Bearer 缺失 → 1001；JWT 签名/类型非法 → 1001；过期 → 1003；
    会话缺失或不匹配（已登出/已被新会话覆盖）→ 1001。返回 JWT 载荷。
    """
    if credentials is None or not credentials.credentials:
        raise AuthenticationError
    claims = decode_access_token(credentials.credentials, settings)
    stored = await load_session_token(redis, str(claims["sub"]))
    if stored != credentials.credentials:
        # 会话已注销（登出）或已被新会话覆盖（重新登录 / 刷新轮换）→ 旧 Token 即刻失效
        raise AuthenticationError
    return claims


async def authenticate_request(request: Request) -> dict[str, Any]:
    """在路由处理器内部显式执行的严格认证（与 ``get_current_user`` 校验链一致）。

    供 catch-all 代理使用：先做路由表匹配（路由表外路径 → 404 + code=3001，
    认证不适用于无资源路径），命中后再调用本函数完成认证，随后转发。
    Bearer 缺失 → 1001；签名/类型非法 → 1001；过期 → 1003；会话缺失/不一致 → 1001；
    Redis 不可用 → 503（强依赖）。
    """
    credentials = await _bearer_scheme(request)
    return await get_current_user(credentials=credentials, redis=get_redis_manager(request))


async def get_logout_principal(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
) -> dict[str, Any]:
    """注销专用主体校验：仅要求 JWT 有效（签名 + 有效期）。

    契约幂等要求：POST /api/v1/user/logout「Token 已失效仍返回 code=0」——
    重复登出（会话已删除）不得返回 401，故此处**不做**会话一致性校验；
    注销动作本身（DEL 会话）幂等且仅作用于 JWT `sub` 对应的自身会话。
    """
    if credentials is None or not credentials.credentials:
        raise AuthenticationError
    return decode_access_token(credentials.credentials, settings)