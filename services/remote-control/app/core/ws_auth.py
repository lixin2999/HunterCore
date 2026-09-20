"""WS 握手鉴权（G-09；契约 x-hunter-websocket-contract.handshake）。

契约检查序（任一失败 → 握手拒绝，不进入帧循环）：
1. Token 提取与 JWT 校验：缺失/无效 → 1001；过期 → 1003；
   传送方式（决策 G-24/#20①）：
   - 浏览器：``Sec-WebSocket-Protocol: hunter-jwt, <token>``（子协议列表双值形态，
     浏览器 WebSocket API 无法自定义握手头，故以子协议承载；服务端回显 ``hunter-jwt``）；
     兼容单值形态 ``hunter-jwt.<token>``（回显原值）；
   - 非浏览器客户端：``Authorization: Bearer <token>`` 头（契约本服务侧实现口径）；
   - **禁止查询串明文携带 Token**（日志泄漏防护，任何形态一律拒绝）；
2. 资源域 RBAC：control 通道 remote:execute / signal 通道 remote:create
   （角色集合与 REST 侧同一配置 rc_execute_roles / rc_create_roles；Token roles 声明）→ 1002；
3. Origin 白名单（CORS_ORIGINS）防跨站 WebSocket 劫持：携带 Origin 且不在白名单 → 1002；
   无 Origin（原生客户端/服务端互联）放行；配置含 "*" 时全放行；
4. 握手限流 1 QPS/用户（Redis 固定窗口，与 POST /session 同档）→ 超限 429 语义（1008 关闭）。

操作员身份取自 Token claims（sub/roles）——WS 由 nginx/Ingress 直达本服务
（不经 api-gateway REST 转发），无 X-User-Id 注入头，故与 REST 侧信任模型不同，
以共享 JWT_SECRET_KEY 验签为准（infra 部署已注入 business-env）。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import jwt
from fastapi import WebSocket
from hunter_common.exceptions import AuthenticationError, PermissionDeniedError, TokenExpiredError
from hunter_common.logging import get_logger

from app.config import Settings, settings
from app.core.dependencies import OperatorContext

logger = get_logger("app.core.ws_auth")


@dataclass(frozen=True)
class WsIdentity:
    """握手鉴权结果：操作员上下文 + Access Token 过期时间（连接期复检用）。"""

    operator: OperatorContext
    expires_at: float | None
    #: 服务端须回显的Sec-WebSocket-Protocol值（Authorization 头认证时为 None，不回显）
    echo_subprotocol: str | None


def extract_subprotocol_token(
    offered: list[str], subprotocol: str
) -> tuple[str | None, str | None]:
    """从客户端 Offer-Protocol 列表提取 Token（决策 #20① 子协议承载）。

    Returns:
        (token, echo)：无 hunter-jwt 子协议时 (None, None)；
        ``hunter-jwt, <token>`` 双值形态 echo=hunter-jwt；
        ``hunter-jwt.<token>`` 单值形态 echo=原值（浏览器要求服务端回显必须是客户端提供的取值）。
    """
    for index, value in enumerate(offered):
        if value == subprotocol:
            token = offered[index + 1] if index + 1 < len(offered) else None
            return token, subprotocol
        if value.startswith(f"{subprotocol}.") and len(value) > len(subprotocol) + 1:
            return value[len(subprotocol) + 1 :], value
    return None, None


def extract_bearer_token(authorization: str | None) -> str | None:
    """Authorization: Bearer 头提取（大小写不敏感 scheme；无头返回 None）。"""
    if not authorization:
        return None
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        return None
    return token.strip()


def decode_access_token(token: str, config: Settings) -> dict[str, Any]:
    """校验 Access Token（PyJWT HS256 白名单 + issuer；无效 → 1001，过期 → 1003）。"""
    try:
        claims: dict[str, Any] = jwt.decode(
            token,
            config.jwt_secret_key,
            algorithms=[config.jwt_algorithm],
            issuer=config.jwt_issuer,
        )
    except jwt.ExpiredSignatureError as exc:
        raise TokenExpiredError from exc
    except jwt.InvalidTokenError as exc:
        raise AuthenticationError from exc
    if claims.get("typ") != "access":
        # 仅 Access Token 可建立操控通道（Refresh Token 冒用视为无效）
        raise AuthenticationError(message="Token 类型无效（仅 Access Token 可建立 WS 通道）")
    return claims


def check_origin(origin: str | None, config: Settings) -> None:
    """Origin 白名单校验（契约 handshake.checks 第 5 条；缺失放行，非法 → 1002）。"""
    if origin is None:
        return
    allowed = config.cors_origins_list
    if "*" in allowed or origin in allowed:
        return
    raise PermissionDeniedError(message="Origin 不在白名单内（防跨站 WebSocket 劫持）")


def identity_from_claims(claims: dict[str, Any], *, role_set: set[str]) -> WsIdentity:
    """claims → WsIdentity（roles 交集判定资源域；与 REST 侧同配置源）。"""
    user_id = str(claims.get("sub") or "")
    if not user_id:
        raise AuthenticationError(message="Token 缺少 sub 声明")
    roles = frozenset(str(role) for role in (claims.get("roles") or []))
    if not roles & role_set:
        raise PermissionDeniedError(
            message=f"无权限：WS 通道需要角色 {sorted(role_set)}"
        )
    exp = claims.get("exp")
    return WsIdentity(
        operator=OperatorContext(user_id=user_id, roles=roles),
        expires_at=float(exp) if isinstance(exp, (int, float)) else None,
        echo_subprotocol=None,
    )


async def authenticate_ws_handshake(
    websocket: WebSocket, *, role_set: set[str]
) -> WsIdentity:
    """WS 握手鉴权入口（Token 提取 → 验签 → RBAC → Origin；不含会话归属校验，路由层做）。

    Raises:
        AuthenticationError / TokenExpiredError / PermissionDeniedError：
        由路由层捕获并以相应关闭码拒绝握手（不 accept）。
    """
    headers = websocket.headers
    config = settings
    token = extract_bearer_token(headers.get("authorization"))
    echo: str | None = None
    if token is None:
        offered = [
            item.strip()
            for value in headers.getlist("sec-websocket-protocol")
            for item in value.split(",")
            if item.strip()
        ]
        token, echo = extract_subprotocol_token(offered, config.ws_jwt_subprotocol)
    if not token:
        raise AuthenticationError(
            message="未认证：缺少 Authorization 头或 hunter-jwt 子协议携带的 Access Token"
        )
    claims = decode_access_token(token, config)
    identity = identity_from_claims(claims, role_set=role_set)
    check_origin(headers.get("origin"), config)
    if echo is not None:
        identity = WsIdentity(
            operator=identity.operator, expires_at=identity.expires_at, echo_subprotocol=echo
        )
    logger.info(
        "ws_handshake_authenticated",
        user_id=identity.operator.user_id,
        transport="subprotocol" if echo else "authorization",
    )
    return identity


__all__ = [
    "WsIdentity",
    "authenticate_ws_handshake",
    "check_origin",
    "decode_access_token",
    "extract_bearer_token",
    "extract_subprotocol_token",
    "identity_from_claims",
]
