"""认证依赖：Bearer JWT 校验 + Redis 会话强依赖校验。

契约依据：
- Redis 键模式 ``session:{user_id}``（redis-keys.yaml 第 1 条）：String 值为
  JWT Access Token 原样存储，TTL 1800s（G-04① 收紧），owner/writer/reader = api-gateway
- 强依赖规则（redis-keys.yaml common_rules）：session 键不可用时必须返回
  HTTP 503 + code=5001，**禁止降级为无会话模式**
- 撤销语义（redis-keys.yaml pending #6 选项②）：不引入黑名单键，以
  「会话存在性 + 值与当前 Access Token 全等」替代黑名单——登录写入会话、
  刷新覆盖会话、登出 DEL 会话，旧 Access Token 即刻失效直至自然过期
- 服务令牌（V1.23.6）：``typ=service`` 使用独立会话键 ``session:service:{client_id}``
  （redis-keys.yaml 第 17 条），与单会话 ``session:{user_id}`` 完全隔离——同一账号
  再次登录只覆盖用户会话，不会使脚本/仿真持有的服务令牌失效（契约 x-hunter-service-token）
"""
from __future__ import annotations

from typing import Any

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from hunter_common.exceptions import AuthenticationError, ServiceUnavailableError
from hunter_common.logging import get_logger
from hunter_common.redis_keys import SERVICE_SESSION

from app.config import settings
from app.core.security import decode_access_token, decode_bearer_token

logger = get_logger("app.core.auth")

__all__ = [
    "authenticate_request",
    "get_current_user",
    "get_logout_principal",
    "get_redis_manager",
    "load_service_session_token",
    "load_session_token",
    "service_session_key",
    "session_key",
]

#: Bearer 方案（契约 securitySchemes.bearerAuth：HTTP Bearer, JWT）
_bearer_scheme = HTTPBearer(auto_error=False, scheme_name="bearerAuth")


def session_key(user_id: str) -> str:
    """构造会话键（契约模式 ``session:{user_id}``，禁止其他拼法）。"""
    return f"session:{user_id}"


def service_session_key(client_id: str) -> str:
    """构造服务令牌会话键（契约 ``session:service:{client_id}``，与用户会话隔离）。"""
    return SERVICE_SESSION.format(client_id=client_id)


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


async def load_service_session_token(redis: Any, client_id: str) -> str:
    """读取服务令牌会话中的令牌（空字符串表示已撤销/未签发）。

    Redis 不可用 → 503（redis-keys.yaml 强依赖规则，禁止降级放行）。
    """
    try:
        return (await redis.get(service_session_key(client_id))) or ""
    except Exception as exc:
        logger.error("service_session_backend_unavailable", client_id=client_id)
        raise ServiceUnavailableError("服务不可用") from exc


async def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
    redis: Any = Depends(get_redis_manager),
) -> dict[str, Any]:
    """严格认证依赖（控制台端点：GET /api/v1/user/me、change-password 等）。

    仅接受 Access Token（``typ=access``）；服务令牌（``typ=service``）→ 1001
    （服务令牌不得读取用户资料或改密）。校验链：Bearer 缺失 → 1001；JWT 签名/类型
    非法 → 1001；过期 → 1003；会话缺失或不匹配（已登出/已被新会话覆盖）→ 1001。
    """
    return await _authenticate(credentials, redis, allow_service=False)


async def authenticate_request(request: Request) -> dict[str, Any]:
    """在路由处理器内部显式执行的严格认证（catch-all 代理专用）。

    供 catch-all 代理使用：先做路由表匹配（路由表外路径 → 404 + code=3001，
    认证不适用于无资源路径），命中后再调用本函数完成认证，随后转发。

    与 ``get_current_user`` 的差异：额外接受服务令牌（``typ=service``，会话键
    ``session:service:{client_id}``），使脚本/仿真等非交互组件与用户登录并行运行
    （契约 x-hunter-service-token）。
    Bearer 缺失 → 1001；签名/类型非法 → 1001；过期 → 1003；会话缺失/不一致 → 1001；
    Redis 不可用 → 503（强依赖）。
    """
    credentials = await _bearer_scheme(request)
    return await _authenticate(credentials, get_redis_manager(request), allow_service=True)


async def _authenticate(
    credentials: HTTPAuthorizationCredentials | None,
    redis: Any,
    *,
    allow_service: bool,
) -> dict[str, Any]:
    """统一认证链：Bearer 解析 → 会话比对（服务令牌按 ``client_id`` 独立比对）。"""
    if credentials is None or not credentials.credentials:
        raise AuthenticationError
    token = credentials.credentials
    if allow_service:
        claims = decode_bearer_token(token, settings)
        if claims.get("typ") == "service":
            client_id = str(claims.get("client_id") or "")
            if not client_id or claims.get("sub") != f"service:{client_id}":
                # 载荷自洽性校验：sub 与 client_id 必须同源，防拼装越权
                raise AuthenticationError
            stored_service = await load_service_session_token(redis, client_id)
            if stored_service != token:
                # 已撤销（DELETE）或已轮换（同 client_id 重新签发）→ 旧令牌即刻失效
                raise AuthenticationError
            return claims
    else:
        claims = decode_access_token(token, settings)
    stored = await load_session_token(redis, str(claims["sub"]))
    if stored != token:
        # 会话已注销（登出）或已被新会话覆盖（重新登录 / 刷新轮换）→ 旧 Token 即刻失效
        raise AuthenticationError
    return claims


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