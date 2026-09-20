"""核心依赖注入：RBAC（网关注入头）+ 请求 ID + 接口限流（附录 D）。

安全机制（契约 securitySchemes.bearerAuth + x-hunter-ota-security.authorization）：
- 认证由 api-gateway 完成（JWT 校验），本服务信任转发头 X-User-Id / X-Roles；
  头缺失 → AuthenticationError（1001，统一响应 401）；
- 授权：ota:read（版本/任务/记录查询）、ota:create（创建版本与任务）、
  ota:execute（发布/退役/启动/暂停/恢复/终止/回滚）；角色不足 → PermissionDeniedError（1002）；
- 限流：POST /versions 单用户 5 QPS（附录 D）；publish 建议值 2 QPS（x-hunter-rate-limits），
  超限 429 + Retry-After（响应体 code 复用 5001）。
"""
from __future__ import annotations

from typing import Annotated
from uuid import uuid4

from fastapi import Depends, Header, Request
from hunter_common.exceptions import AuthenticationError, PermissionDeniedError
from hunter_common.internal_auth import verify_identity_headers
from hunter_common.logging import get_trace_id

from app.config import settings
from app.core.rate_limit import enforce_user_rate_limit

#: 限流 API 标识（写入 rate_limit:{user_id}:{api} 的 api 段）
VERSION_CREATE_API = "ota:versions:create"
VERSION_PUBLISH_API = "ota:versions:publish"


def _require_forwarded_auth(
    request: Request,
    x_user_id: Annotated[
        str | None,
        Header(alias="X-User-Id", description="api-gateway 注入的已认证用户 ID（JWT sub）"),
    ] = None,
    x_roles: Annotated[
        str | None,
        Header(alias="X-Roles", description="api-gateway 注入的角色列表（逗号分隔）"),
    ] = None,
) -> tuple[str, set[str]]:
    """校验网关转发认证头（缺失 → 1001，而非 FastAPI 层 422）。"""
    if not x_user_id or not x_roles:
        raise AuthenticationError(message="未认证：缺少网关注入的 X-User-Id / X-Roles 请求头")
    # G-02：配置 GATEWAY_HMAC_SECRET 后验身份头签名（防集群内伪造）；未配置（dev/test）跳过保持兼容
    if settings.gateway_hmac_secret and not verify_identity_headers(
        settings.gateway_hmac_secret,
        request.headers,
        max_age_seconds=settings.gateway_identity_max_age_s,
    ):
        raise AuthenticationError(message="未认证：身份头签名缺失或无效（X-Internal-MAC 校验失败）")
    roles = {role.strip() for role in x_roles.split(",") if role.strip()}
    return x_user_id, roles


def require_read_permission(
    credentials: Annotated[tuple[str, set[str]], Depends(_require_forwarded_auth)],
) -> str:
    """ota:read 授权（1002：角色不足）。返回 user_id。"""
    user_id, roles = credentials
    if not roles & settings.ota_read_role_set:
        raise PermissionDeniedError(
            message=f"无权限：ota:read 需要角色 {sorted(settings.ota_read_role_set)}"
        )
    return user_id


def require_create_permission(
    credentials: Annotated[tuple[str, set[str]], Depends(_require_forwarded_auth)],
) -> str:
    """ota:create 授权（1002：角色不足）。返回 user_id。"""
    user_id, roles = credentials
    if not roles & settings.ota_create_role_set:
        raise PermissionDeniedError(
            message=f"无权限：ota:create 需要角色 {sorted(settings.ota_create_role_set)}"
        )
    return user_id


def require_execute_permission(
    credentials: Annotated[tuple[str, set[str]], Depends(_require_forwarded_auth)],
) -> str:
    """ota:execute 授权（1002：角色不足）。返回 user_id。"""
    user_id, roles = credentials
    if not roles & settings.ota_execute_role_set:
        raise PermissionDeniedError(
            message=f"无权限：ota:execute 需要角色 {sorted(settings.ota_execute_role_set)}"
        )
    return user_id


async def version_create_user(
    request: Request,
    user_id: Annotated[str, Depends(require_create_permission)],
) -> str:
    """POST /versions 专用依赖：ota:create + 附录 D 单用户 5 QPS 限流（Redis 固定窗口）。"""
    await enforce_user_rate_limit(
        user_id,
        request.app.state.redis,
        api=VERSION_CREATE_API,
        limit_per_min=settings.version_create_rate_limit_per_min,
        settings=settings,
    )
    return user_id


async def version_publish_user(
    request: Request,
    user_id: Annotated[str, Depends(require_execute_permission)],
) -> str:
    """POST /versions/{id}/publish 专用依赖：ota:execute + 建议限流 2 QPS（x-hunter-rate-limits）。"""
    await enforce_user_rate_limit(
        user_id,
        request.app.state.redis,
        api=VERSION_PUBLISH_API,
        limit_per_min=settings.publish_rate_limit_per_min,
        settings=settings,
    )
    return user_id


def trace_request_id() -> str:
    """请求 ID：复用链路 trace_id（main.py 中间件 set_trace_id），兜底 UUID4。"""
    return get_trace_id() or str(uuid4())


__all__ = [
    "VERSION_CREATE_API",
    "VERSION_PUBLISH_API",
    "require_create_permission",
    "require_execute_permission",
    "require_read_permission",
    "trace_request_id",
    "version_create_user",
    "version_publish_user",
]
