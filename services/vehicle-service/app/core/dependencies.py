"""核心依赖注入：RBAC（网关注入头 X-User-Id / X-Roles）+ 请求 ID。

安全机制（契约 x-hunter-service.rbac_role_bindings）：
- 认证由 api-gateway 完成（JWT 校验），本服务信任转发头 X-User-Id / X-Roles；
  网关保证外部请求无法伪造这些头（内部网络 + Ingress 覆盖），缺失 → 1001。
- G-02：配置 GATEWAY_HMAC_SECRET 后验 X-Internal-MAC 签名，防集群内伪造身份绕过 RBAC。
- 授权：`vehicle:{create,read,update,delete,execute}` 由本服务按角色集合判定（1002）。
"""
from __future__ import annotations

from typing import Annotated
from uuid import uuid4

from fastapi import Depends, Header, Request
from hunter_common.exceptions import AuthenticationError, PermissionDeniedError
from hunter_common.internal_auth import verify_identity_headers
from hunter_common.logging import get_trace_id

from app.config import settings


def _require_forwarded_auth(
    request: Request,
    x_user_id: Annotated[
        str | None,
        Header(alias="X-User-Id", description="api-gateway 注入的已认证用户 ID（JWT sub）"),
    ],
    x_roles: Annotated[
        str | None,
        Header(alias="X-Roles", description="api-gateway 注入的角色列表（逗号分隔）"),
    ],
) -> tuple[str, frozenset[str]]:
    """校验网关转发头（缺失 → 1001）并解析角色集合。"""
    if not x_user_id or not x_roles:
        raise AuthenticationError(
            message="未认证：缺少网关注入的 X-User-Id / X-Roles 请求头"
        )
    # G-02：配置 GATEWAY_HMAC_SECRET 后验身份头签名（防集群内伪造）；未配置（dev/test）跳过保持兼容
    if settings.gateway_hmac_secret and not verify_identity_headers(
        settings.gateway_hmac_secret,
        request.headers,
        max_age_seconds=settings.gateway_identity_max_age_s,
    ):
        raise AuthenticationError(message="未认证：身份头签名缺失或无效（X-Internal-MAC 校验失败）")
    roles = frozenset(role.strip() for role in x_roles.split(",") if role.strip())
    return x_user_id, roles


def _make_role_guard(
    permission_code: str, allowed: frozenset[str]
):
    """工厂：返回一个依赖函数，校验角色集合是否命中白名单。"""

    def _guard(
        credentials: Annotated[tuple[str, frozenset[str]], Depends(_require_forwarded_auth)],
    ) -> str:
        user_id, roles = credentials
        if not roles & allowed:
            raise PermissionDeniedError(
                message=f"无权限：{permission_code} 需要角色 {sorted(allowed)}"
            )
        return user_id

    _guard.__name__ = f"require_{permission_code.replace(':', '_')}"
    return _guard


require_vehicle_read = _make_role_guard("vehicle:read", settings.vehicle_read_role_set)
require_vehicle_create = _make_role_guard("vehicle:create", settings.vehicle_create_role_set)
require_vehicle_update = _make_role_guard("vehicle:update", settings.vehicle_update_role_set)
require_vehicle_delete = _make_role_guard("vehicle:delete", settings.vehicle_delete_role_set)
require_vehicle_execute = _make_role_guard("vehicle:execute", settings.vehicle_execute_role_set)


def trace_request_id() -> str:
    """请求 ID：复用链路 trace_id（main.py 中间件 set_trace_id），兜底 UUID4。"""
    return get_trace_id() or str(uuid4())


__all__ = [
    "require_vehicle_create",
    "require_vehicle_delete",
    "require_vehicle_execute",
    "require_vehicle_read",
    "require_vehicle_update",
    "trace_request_id",
]
