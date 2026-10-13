"""FastAPI 依赖注入：session / RBAC / 服务获取。

认证由 api-gateway 完成（JWT 校验），本服务信任转发头 X-User-Id / X-Roles。
网关保证外部请求无法伪造这些头（内部网络 + Ingress 覆盖）。
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated
from uuid import uuid4

from fastapi import Depends, Header, Request
from hunter_common.database import DatabaseSessionManager
from hunter_common.exceptions import AuthenticationError, PermissionDeniedError
from hunter_common.internal_auth import verify_identity_headers
from hunter_common.logging import get_trace_id
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings

# ---------- Session 依赖 ----------

async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    """请求级数据库会话（成功提交 / 异常回滚）。"""
    db: DatabaseSessionManager = request.app.state.db
    async with db.session() as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_session)]


# ---------- 认证/授权 ----------

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
) -> tuple[str, set[str]]:
    """校验网关转发认证头（缺失 → 1001）。

    fail-closed（§13.1 审查 #14）：HMAC 密钥未配置时不得跳过签名校验，
    否则任何能直达端口的请求伪造 X-User-Id/X-Roles 即绕过认证。
    """
    if not x_user_id or not x_roles:
        raise AuthenticationError(
            message="未认证：缺少网关注入的 X-User-Id / X-Roles 请求头"
        )
    secret = settings.gateway_hmac_secret
    if not secret:
        raise AuthenticationError(message="未认证：服务端未配置身份签名密钥（fail-closed）")
    if not verify_identity_headers(
        secret,
        request.headers,
        max_age_seconds=settings.gateway_identity_max_age_s,
    ):
        raise AuthenticationError(message="未认证：身份头签名缺失或无效（X-Internal-MAC 校验失败）")
    roles = {role.strip() for role in x_roles.split(",") if role.strip()}
    return x_user_id, roles


def require_read(
    credentials: Annotated[tuple[str, set[str]], Depends(_require_forwarded_auth)],
) -> str:
    """data:read 权限校验（1002：角色不足）。"""
    user_id, roles = credentials
    if not roles & settings.data_read_role_set:
        raise PermissionDeniedError(
            message=f"无权限：data:read 需要角色 {sorted(settings.data_read_role_set)}"
        )
    return user_id


def require_execute(
    credentials: Annotated[tuple[str, set[str]], Depends(_require_forwarded_auth)],
) -> str:
    """data:execute 权限校验（1002：角色不足）。"""
    user_id, roles = credentials
    if not roles & settings.data_execute_role_set:
        raise PermissionDeniedError(
            message=f"无权限：data:execute 需要角色 {sorted(settings.data_execute_role_set)}"
        )
    return user_id


ReadUser = Annotated[str, Depends(require_read)]
ExecuteUser = Annotated[str, Depends(require_execute)]


# ---------- 服务获取 ----------

def get_storage(request: Request):
    """从 app.state 获取 LakeStorage（lifespan 装配）。"""
    return request.app.state.storage  # type: ignore[no-any-return]


def get_dataset_service(request: Request):
    return request.app.state.dataset_service  # type: ignore[no-any-return]


def get_cmd_service(request: Request):
    return request.app.state.cmd_service  # type: ignore[no-any-return]


def trace_request_id() -> str:
    """请求 ID：复用 trace_id，兜底 UUID4。"""
    return get_trace_id() or str(uuid4())


__all__ = [
    "ExecuteUser",
    "ReadUser",
    "SessionDep",
    "get_cmd_service",
    "get_dataset_service",
    "get_session",
    "get_storage",
    "require_execute",
    "require_read",
    "trace_request_id",
]
