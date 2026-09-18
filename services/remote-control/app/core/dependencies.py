"""核心依赖注入：RBAC（网关注入头）+ 请求 ID + 限流依赖（契约 securitySchemes）。

安全机制（契约 securitySchemes.bearerAuth + x-hunter-remote-security.authorization）：
- 认证由 api-gateway 完成（JWT 校验），本服务信任转发头 X-User-Id / X-Roles；
  头缺失 → AuthenticationError（1001，统一响应 401）；
- 授权：remote:read（可控车辆/会话/历史查询）、remote:create（创建操控会话）、
  remote:execute（结束会话）；角色不足 → PermissionDeniedError（1002）；
- 限流：POST /remote/session 单用户 1 QPS（附录 D），超限 429 + Retry-After。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated
from uuid import uuid4

from fastapi import Depends, Header, Request
from hunter_common.exceptions import AuthenticationError, PermissionDeniedError
from hunter_common.logging import get_trace_id
from hunter_common.redis import RedisManager

from app.config import Settings, settings
from app.core.rate_limit import enforce_user_rate_limit
from app.repositories.storage import VideoArchiveStorage
from app.services.history_service import HistoryService
from app.services.session_service import SessionService
from app.services.vehicle_view import VehicleViewReader

#: 限流 API 标识（写入 rate_limit:{user_id}:{api} 的 api 段；附录 D POST /remote/session）
SESSION_CREATE_API = "remote:session:create"


@dataclass(frozen=True)
class OperatorContext:
    """操作员上下文（网关注入头解析结果）。"""

    user_id: str
    roles: frozenset[str]

    def is_admin(self, settings: Settings) -> bool:
        """是否平台管理员（跨操作员可见/可结束会话；角色名经配置注入）。"""
        return settings.rc_admin_role in self.roles


def _require_forwarded_auth(
    x_user_id: Annotated[
        str | None,
        Header(alias="X-User-Id", description="api-gateway 注入的已认证用户 ID（JWT sub）"),
    ] = None,
    x_roles: Annotated[
        str | None,
        Header(alias="X-Roles", description="api-gateway 注入的角色列表（逗号分隔）"),
    ] = None,
) -> OperatorContext:
    """校验网关转发认证头（缺失 → 1001，而非 FastAPI 层 422）。"""
    if not x_user_id or not x_roles:
        raise AuthenticationError(message="未认证：缺少网关注入的 X-User-Id / X-Roles 请求头")
    roles = frozenset(role.strip() for role in x_roles.split(",") if role.strip())
    return OperatorContext(user_id=x_user_id, roles=roles)


def require_read_permission(
    operator: Annotated[OperatorContext, Depends(_require_forwarded_auth)],
) -> OperatorContext:
    """remote:read 授权（1002：角色不足）。"""
    if not operator.roles & settings.rc_read_role_set:
        raise PermissionDeniedError(
            message=f"无权限：remote:read 需要角色 {sorted(settings.rc_read_role_set)}"
        )
    return operator


def require_create_permission(
    operator: Annotated[OperatorContext, Depends(_require_forwarded_auth)],
) -> OperatorContext:
    """remote:create 授权（1002：角色不足）。"""
    if not operator.roles & settings.rc_create_role_set:
        raise PermissionDeniedError(
            message=f"无权限：remote:create 需要角色 {sorted(settings.rc_create_role_set)}"
        )
    return operator


def require_execute_permission(
    operator: Annotated[OperatorContext, Depends(_require_forwarded_auth)],
) -> OperatorContext:
    """remote:execute 授权（1002：角色不足）。"""
    if not operator.roles & settings.rc_execute_role_set:
        raise PermissionDeniedError(
            message=f"无权限：remote:execute 需要角色 {sorted(settings.rc_execute_role_set)}"
        )
    return operator


async def session_create_operator(
    request: Request,
    operator: Annotated[OperatorContext, Depends(require_create_permission)],
) -> OperatorContext:
    """POST /session 专用依赖：remote:create + 附录 D 单用户 1 QPS 限流（Redis 固定窗口）。"""
    await enforce_user_rate_limit(
        operator.user_id,
        request.app.state.redis,
        api=SESSION_CREATE_API,
        limit_per_min=settings.rc_session_create_rate_limit_per_min,
        settings=settings,
    )
    return operator


def trace_request_id() -> str:
    """请求 ID：复用链路 trace_id（main.py 中间件 set_trace_id），兜底 UUID4。"""
    return get_trace_id() or str(uuid4())


# ---------- app.state 依赖（与 lifespan 装配结构一致；测试经夹具注入替身） ----------
def get_redis(request: Request) -> RedisManager:
    """Redis 管理器（app.state.redis）。"""
    return request.app.state.redis


def get_storage(request: Request) -> VideoArchiveStorage:
    """录像/sidecar 归档存储（app.state.storage）。"""
    return request.app.state.storage


def get_vehicle_view(request: Request) -> VehicleViewReader:
    """车辆可控性读模型（app.state.vehicle_view）。"""
    return request.app.state.vehicle_view


def get_session_service(request: Request) -> SessionService:
    """操控会话服务（app.state.session_service）。"""
    return request.app.state.session_service


def get_history_service(request: Request) -> HistoryService:
    """操控历史服务（app.state.history_service）。"""
    return request.app.state.history_service


RedisDep = Annotated[RedisManager, Depends(get_redis)]
StorageDep = Annotated[VideoArchiveStorage, Depends(get_storage)]
SessionServiceDep = Annotated[SessionService, Depends(get_session_service)]
HistoryServiceDep = Annotated[HistoryService, Depends(get_history_service)]
ReadOperatorDep = Annotated[OperatorContext, Depends(require_read_permission)]
ExecuteOperatorDep = Annotated[OperatorContext, Depends(require_execute_permission)]
CreateOperatorDep = Annotated[OperatorContext, Depends(session_create_operator)]
RequestIdDep = Annotated[str, Depends(trace_request_id)]

__all__ = [
    "SESSION_CREATE_API",
    "CreateOperatorDep",
    "ExecuteOperatorDep",
    "HistoryServiceDep",
    "OperatorContext",
    "ReadOperatorDep",
    "RedisDep",
    "RequestIdDep",
    "SessionServiceDep",
    "StorageDep",
    "get_history_service",
    "get_redis",
    "get_session_service",
    "get_storage",
    "get_vehicle_view",
    "session_create_operator",
    "trace_request_id",
]
