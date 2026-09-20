"""网关 → 后端服务的身份头信任加固（G-02，设计文档 14.2「内部服务间 mTLS」的第一道替代防线）。

背景：后端服务此前仅信任网关注入的 ``X-User-Id`` / ``X-Roles`` 头，集群内任意 Pod
（NetworkPolicy 之前）可伪造身份绕过 JWT/RBAC。本模块用共享密钥（K8s Secret
``GATEWAY_HMAC_SECRET``）对身份头做 HMAC-SHA256 签名：

- 网关转发时追加 ``X-Identity-Timestamp`` + ``X-Internal-MAC``；
- 后端校验 MAC 与时间戳新鲜度（防重放窗口 ``gateway_identity_max_age_s``，默认 300s）；
- 密钥未配置（dev/test）时跳过校验，行为与历史一致；staging/prod 由
  :class:`hunter_common.config.HunterBaseConfig` 启动强校验保证密钥必填。

签名串（顺序固定，禁止改动）：``user_id|roles|trace_id|timestamp``
（roles 为逗号分隔小写、去空白后的原样字符串，与服务端解析口径一致）。
彻底消除身份头信任仍需网关↔后端 mTLS/Istio（NetworkPolicy 为纵深防御第一层）。
"""

from __future__ import annotations

import hashlib
import hmac
import time
from collections.abc import Mapping

HEADER_USER_ID = "X-User-Id"
HEADER_ROLES = "X-Roles"
HEADER_TRACE_ID = "X-Trace-Id"
HEADER_TIMESTAMP = "X-Identity-Timestamp"
HEADER_MAC = "X-Internal-MAC"

_ALGORITHM = hashlib.sha256


def canonical_identity(user_id: str, roles: str, trace_id: str, timestamp: str) -> str:
    """构造待签名规范串（字段以 ``|`` 连接，空值以空串参与）。"""
    return f"{user_id}|{roles}|{trace_id}|{timestamp}"


def sign_identity(secret: str, user_id: str, roles: str, trace_id: str, timestamp: str) -> str:
    """HMAC-SHA256 十六进制摘要（网关侧注入用）。"""
    message = canonical_identity(user_id, roles, trace_id, timestamp).encode("utf-8")
    return hmac.new(secret.encode("utf-8"), message, _ALGORITHM).hexdigest()


def build_identity_headers(
    secret: str, *, user_id: str, roles: str, trace_id: str
) -> dict[str, str]:
    """网关转发注入的全套身份头（含防伪造覆盖语义由调用方保证）。"""
    timestamp = str(int(time.time()))
    return {
        HEADER_USER_ID: user_id,
        HEADER_ROLES: roles,
        HEADER_TRACE_ID: trace_id,
        HEADER_TIMESTAMP: timestamp,
        HEADER_MAC: sign_identity(secret, user_id, roles, trace_id, timestamp),
    }


def verify_identity_headers(
    secret: str,
    headers: Mapping[str, str | None],
    *,
    max_age_seconds: int = 300,
    now: float | None = None,
) -> bool:
    """后端校验：MAC 恒定时间比较 + 时间戳新鲜度；任一缺失/过期/不匹配返回 False。

    ``headers`` 键大小写不敏感（Starlette Headers 原生不敏感；dict 需调用方小写或按常量传）。
    """

    def _get(name: str) -> str | None:
        if name in headers:
            return headers[name]
        lower = {k.lower(): v for k, v in headers.items() if k is not None}
        return lower.get(name.lower())

    user_id = _get(HEADER_USER_ID)
    roles = _get(HEADER_ROLES)
    trace_id = _get(HEADER_TRACE_ID) or ""
    timestamp = _get(HEADER_TIMESTAMP)
    mac = _get(HEADER_MAC)
    if not user_id or roles is None or not timestamp or not mac:
        return False
    try:
        ts = int(timestamp)
    except ValueError:
        return False
    current = time.time() if now is None else now
    if abs(current - ts) > max_age_seconds:
        return False
    expected = sign_identity(secret, user_id, roles, trace_id, timestamp)
    return hmac.compare_digest(expected, mac)
