"""认证安全原语：bcrypt 口令哈希 + JWT 签发/校验（设计文档 3.2.2 节 / 14.1 节）。

JWT 载荷契约（设计文档 3.2.2 节）：``sub``(user_id) / ``username`` / ``roles`` /
``permissions`` / ``iat`` / ``exp`` / ``iss="hunter-platform"`` / ``jti``。
Access Token 2h、Refresh Token 7d（安全机制；与 HunterBaseConfig 默认值一致）。

Refresh Token 轮换（防重放）：通过「Refresh Token 绑定其配对 Access Token 的 jti
（``at_jti``）+ Redis ``session:{user_id}`` 存储当前 Access Token（redis-keys.yaml
第 1 条）」实现——刷新成功后会话覆盖为新 Access Token，旧 Refresh Token 重放时
``at_jti`` 与会话中 jti 不再匹配 → 1003。该方案不新增任何 Redis 键模式
（redis-keys.yaml pending #6 选项②：删除会话 + jti 校验替代黑名单）。
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import bcrypt
import jwt
from hunter_common.exceptions import AuthenticationError, TokenExpiredError

from app.config import Settings

__all__ = [
    "create_access_token",
    "create_refresh_token",
    "decode_access_token",
    "decode_access_token_ignoring_expiry",
    "decode_refresh_token",
    "hash_password",
    "verify_password",
]


# =====================================================================
# 口令哈希（user_svc.users.password_hash：bcrypt，禁止明文/可逆加密）
# =====================================================================
def hash_password(password: str, *, rounds: int = 12) -> str:
    """生成 bcrypt 哈希（轮次经配置注入，禁止硬编码）。"""
    salt = bcrypt.gensalt(rounds=rounds)
    return bcrypt.hashpw(password.encode("utf-8"), salt).decode("ascii")


def verify_password(password: str, password_hash: str) -> bool:
    """校验口令；哈希格式非法一律返回 False（不向调用方泄露失败原因）。"""
    try:
        return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("ascii"))
    except (ValueError, TypeError):
        return False


# =====================================================================
# JWT 签发 / 校验（PyJWT；算法固定白名单，防 algorithm confusion）
# =====================================================================
def _encode(payload: dict[str, Any], settings: Settings) -> str:
    return jwt.encode(payload, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)


def _now() -> datetime:
    return datetime.now(UTC)


def create_access_token(
    *,
    user_id: str,
    username: str,
    roles: list[str],
    permissions: list[str],
    settings: Settings,
) -> tuple[str, str]:
    """签发 Access Token（2h）；返回 (token, jti)。载荷见设计文档 3.2.2 节。"""
    now = _now()
    ttl_seconds = settings.jwt_access_token_expire_minutes * 60
    jti = str(uuid.uuid4())
    token = _encode(
        {
            "sub": user_id,
            "username": username,
            "roles": roles,
            "permissions": permissions,
            "iat": now,
            "exp": now + timedelta(seconds=ttl_seconds),
            "iss": settings.jwt_issuer,
            "jti": jti,
            "typ": "access",
        },
        settings,
    )
    return token, jti


def create_refresh_token(
    *, user_id: str, access_jti: str, settings: Settings
) -> tuple[str, str]:
    """签发 Refresh Token（7d，单次使用轮换）；返回 (token, jti)。

    ``at_jti`` 绑定配对 Access Token 的 jti（轮换/防重放机制见模块 docstring）。
    """
    now = _now()
    ttl_seconds = settings.jwt_refresh_token_expire_days * 86400
    jti = str(uuid.uuid4())
    token = _encode(
        {
            "sub": user_id,
            "iat": now,
            "exp": now + timedelta(seconds=ttl_seconds),
            "iss": settings.jwt_issuer,
            "jti": jti,
            "typ": "refresh",
            "at_jti": access_jti,
        },
        settings,
    )
    return token, jti


def decode_access_token(token: str, settings: Settings) -> dict[str, Any]:
    """校验并解析 Access Token：过期 → 1003，无效 → 1001（附录 A 预定义错误码）。"""
    try:
        claims = jwt.decode(
            token,
            settings.jwt_secret_key,
            algorithms=[settings.jwt_algorithm],
            issuer=settings.jwt_issuer,
        )
    except jwt.ExpiredSignatureError as exc:
        raise TokenExpiredError from exc
    except jwt.InvalidTokenError as exc:
        raise AuthenticationError from exc
    if claims.get("typ") != "access":
        raise AuthenticationError
    return claims


def decode_access_token_ignoring_expiry(token: str, settings: Settings) -> dict[str, Any]:
    """校验签名（忽略有效期）解析 Access Token。

    用途：①限流身份识别（core/middleware，无效 Token 交给认证依赖统一拒绝）；
    ②提取 Redis 会话中 Access Token 的 jti（会话与 Token 同 TTL，无需再验 exp）。
    """
    try:
        return jwt.decode(
            token,
            settings.jwt_secret_key,
            algorithms=[settings.jwt_algorithm],
            issuer=settings.jwt_issuer,
            options={"verify_exp": False},
        )
    except jwt.InvalidTokenError as exc:
        raise AuthenticationError from exc


def decode_refresh_token(token: str, settings: Settings) -> dict[str, Any]:
    """校验并解析 Refresh Token；契约：失效/过期统一 → 1003（需重新登录）。"""
    try:
        claims = jwt.decode(
            token,
            settings.jwt_secret_key,
            algorithms=[settings.jwt_algorithm],
            issuer=settings.jwt_issuer,
        )
    except jwt.InvalidTokenError as exc:
        # 含 ExpiredSignatureError（jwt.InvalidTokenError 基类）；契约要求统一 1003
        raise TokenExpiredError from exc
    if claims.get("typ") != "refresh":
        raise TokenExpiredError
    return claims