"""核心依赖注入：RBAC（网关注入头）+ 请求 ID + 接口限流（附录 D）。

安全机制（契约 x-hunter-security）：
- 认证由 api-gateway 完成（JWT 校验），本服务信任转发头 X-User-Id / X-Roles；
  网关保证外部请求无法伪造这些头（内部网络 + Ingress 覆盖）；
  头缺失 → AuthenticationError（1001，统一响应 401）。
- 授权：data:read → 遥测/事件/文件清单查询；data:execute → 事件确认等写操作；
  角色不足 → PermissionDeniedError（1002，统一响应 403）。
- 限流：GET /data/telemetry 单用户 20 QPS（Redis 固定窗口，超限 429 + Retry-After）。
"""
from __future__ import annotations

from typing import Annotated
from uuid import uuid4

from fastapi import Depends, Header, Request
from hunter_common.exceptions import AuthenticationError, PermissionDeniedError
from hunter_common.logging import get_trace_id

from app.config import settings
from app.core.rate_limit import enforce_user_rate_limit


def _require_forwarded_auth(
    x_user_id: Annotated[
        str | None,
        Header(alias="X-User-Id", description="api-gateway 注入的已认证用户 ID（JWT sub）"),
    ],
    x_roles: Annotated[
        str | None,
        Header(alias="X-Roles", description="api-gateway 注入的角色列表（逗号分隔）"),
    ],
) -> tuple[str, set[str]]:
    """校验网关转发认证头（缺失 → 1001）并解析角色集合。"""
    if not x_user_id or not x_roles:
        raise AuthenticationError(
            message="未认证：缺少网关注入的 X-User-Id / X-Roles 请求头"
        )
    roles = {role.strip() for role in x_roles.split(",") if role.strip()}
    return x_user_id, roles


def require_read_permission(
    credentials: Annotated[tuple[str, set[str]], Depends(_require_forwarded_auth)],
) -> str:
    """data:read 授权（1002：角色不足）。返回 user_id。"""
    user_id, roles = credentials
    if not roles & settings.data_read_role_set:
        raise PermissionDeniedError(
            message=f"无权限：data:read 需要角色 {sorted(settings.data_read_role_set)}"
        )
    return user_id


def require_execute_permission(
    credentials: Annotated[tuple[str, set[str]], Depends(_require_forwarded_auth)],
) -> str:
    """data:execute 授权（1002：角色不足）。返回 user_id。"""
    user_id, roles = credentials
    if not roles & settings.data_execute_role_set:
        raise PermissionDeniedError(
            message=f"无权限：data:execute 需要角色 {sorted(settings.data_execute_role_set)}"
        )
    return user_id


async def telemetry_query_user(
    request: Request,
    user_id: Annotated[str, Depends(require_read_permission)],
) -> str:
    """GET /telemetry 专用依赖：读权限 + 附录 D 单用户 20 QPS 限流（异步 Redis）。

    RedisManager 来自 app.state.redis（lifespan 装配）。
    """
    await enforce_user_rate_limit(user_id, request.app.state.redis)
    return user_id


def trace_request_id() -> str:
    """请求 ID：复用链路 trace_id（main.py 中间件 set_trace_id），兜底 UUID4。

    技术债（TBD-common-trace）：待共享库提供统一的请求 ID 依赖后收敛。
    """
    return get_trace_id() or str(uuid4())


__all__ = [
    "require_execute_permission",
    "require_read_permission",
    "telemetry_query_user",
    "trace_request_id",
]
