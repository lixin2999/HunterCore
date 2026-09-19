"""认证鉴权与依赖注入（FastAPI ``Depends``）。

身份来源：api-gateway 转发头 ``X-User-Id`` / ``X-Roles``（契约 securitySchemes.bearerAuth：
网关注入，服务侧据此执行 RBAC）。直连流量缺失头 → 401 + code=1001（禁止放行）。
授权动作：scene:read（列表/详情/模板/导出）、scene:create|update|delete（创建/更新/删除/复制）、
scene:execute（发布/下发 Carla 仿真）；角色不足 → 403 + code=1002。
业务服务经 ``app.state`` 装配（lifespan），测试可整体替换为替身。
"""
from __future__ import annotations

from typing import Annotated
from uuid import uuid4

from fastapi import Depends, Header, Request
from hunter_common.exceptions import AuthenticationError, PermissionDeniedError
from hunter_common.logging import get_trace_id

from app.config import settings
from app.services.export import SceneExportService
from app.services.scenes import SceneService
from app.services.simulation import SceneSimulationService
from app.services.templates import SceneTemplateService


def _require_forwarded_auth(
    x_user_id: Annotated[
        str | None, Header(alias="X-User-Id", description="网关注入的已认证用户 ID（JWT sub）")
    ] = None,
    x_roles: Annotated[
        str | None, Header(alias="X-Roles", description="网关注入的角色列表（逗号分隔）")
    ] = None,
) -> tuple[str, frozenset[str]]:
    """校验网关转发认证头（缺失 → 1001，而非 FastAPI 层 422）。"""
    if not x_user_id or not x_roles:
        raise AuthenticationError(message="未认证：缺少网关注入的 X-User-Id / X-Roles 请求头")
    roles = frozenset(role.strip() for role in x_roles.split(",") if role.strip())
    return x_user_id, roles


def require_read_principals(
    principals: Annotated[tuple[str, frozenset[str]], Depends(_require_forwarded_auth)],
) -> tuple[str, frozenset[str]]:
    """scene:read 授权（返回 (user_id, roles)，列表接口需 roles 做数据权限判定）。"""
    user_id, roles = principals
    if not roles & settings.scene_read_role_set:
        raise PermissionDeniedError(
            message=f"无权限：scene:read 需要角色 {sorted(settings.scene_read_role_set)}"
        )
    return user_id, roles


def require_read_permission(
    principals: Annotated[tuple[str, frozenset[str]], Depends(require_read_principals)],
) -> str:
    """scene:read 授权（仅需用户 ID 的端点使用）。"""
    return principals[0]


def require_write_permission(
    principals: Annotated[tuple[str, frozenset[str]], Depends(_require_forwarded_auth)],
) -> str:
    """scene:create|update|delete 授权（创建/更新/删除/复制）。"""
    user_id, roles = principals
    if not roles & settings.scene_write_role_set:
        raise PermissionDeniedError(
            message=f"无权限：scene:create|update|delete 需要角色 {sorted(settings.scene_write_role_set)}"
        )
    return user_id


def require_execute_permission(
    principals: Annotated[tuple[str, frozenset[str]], Depends(_require_forwarded_auth)],
) -> str:
    """scene:execute 授权（发布/下发 Carla 仿真）。"""
    user_id, roles = principals
    if not roles & settings.scene_execute_role_set:
        raise PermissionDeniedError(
            message=f"无权限：scene:execute 需要角色 {sorted(settings.scene_execute_role_set)}"
        )
    return user_id


def trace_request_id() -> str:
    """响应体 request_id（复用链路 trace_id，兜底 UUID4）。"""
    return get_trace_id() or str(uuid4())


# ---------- 业务服务（lifespan 装配于 app.state，测试可替换） ----------

def get_scene_service(request: Request) -> SceneService:
    """场景库服务。"""
    return request.app.state.scene_service  # type: ignore[no-any-return]


def get_template_service(request: Request) -> SceneTemplateService:
    """场景模板服务。"""
    return request.app.state.template_service  # type: ignore[no-any-return]


def get_export_service(request: Request) -> SceneExportService:
    """场景导出服务。"""
    return request.app.state.export_service  # type: ignore[no-any-return]


def get_simulation_service(request: Request) -> SceneSimulationService:
    """场景下发服务。"""
    return request.app.state.simulation_service  # type: ignore[no-any-return]


__all__ = [
    "get_export_service",
    "get_scene_service",
    "get_simulation_service",
    "get_template_service",
    "require_execute_permission",
    "require_read_permission",
    "require_read_principals",
    "require_write_permission",
    "trace_request_id",
]