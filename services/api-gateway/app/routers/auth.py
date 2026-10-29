"""统一认证路由（契约 paths：/api/v1/user/login|refresh|logout|me|change-password|service-token）。

- operationId 与契约一致（userLogin / userRefreshToken / userLogout / getCurrentUser /
  userChangePassword / issueServiceToken / revokeServiceToken）
- response_model 为契约响应 schema（ApiResponseTokenPair / ApiResponseUserProfile /
  ApiResponseEmpty / ApiResponseServiceToken；统一五字段）
- 错误映射遵循 x-hunter-error-status-map：1001→401、1002→403、1003→401、2001→422、
  429（code 复用 5001）+ Retry-After
- 服务令牌（V1.23.6）：管理员签发/撤销非交互组件令牌；令牌本体归属 ``session:service:{client_id}``，
  与用户单会话隔离（契约 x-hunter-service-token）
"""
from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends, Path, Request
from hunter_common.responses import success_response

from app.core.auth import get_current_user, get_logout_principal
from app.core.dependencies import get_auth_service
from app.core.http import get_client_ip, too_many_requests_response
from app.core.rate_limit import RateLimitExceeded
from app.schemas.auth import (
    ApiResponseEmpty,
    ApiResponseServiceToken,
    ApiResponseTokenPair,
    ApiResponseUserProfile,
    ChangePasswordRequest,
    LoginRequest,
    LogoutRequest,
    RefreshTokenRequest,
    ServiceTokenRequest,
)
from app.services.auth_service import AuthService

router = APIRouter(prefix="/api/v1/user", tags=["auth"])

__all__ = ["router"]


@router.post(
    "/login",
    operation_id="userLogin",
    summary="用户登录（用户名密码 + 可选 MFA(TOTP)）",
    response_model=ApiResponseTokenPair,
    responses={
        401: {"description": "1001 未认证（凭证错误/账号非启用，统一响应防用户名枚举）"},
        422: {"description": "2001 参数错误（请求体校验失败）"},
        429: {"description": "限流触发（响应体 code 复用 5001）"},
        500: {"description": "5000 服务器内部错误"},
        503: {"description": "5001 服务不可用（DB/Redis 依赖不可用）"},
    },
)
async def user_login(
    payload: LoginRequest,
    request: Request,
    service: Annotated[AuthService, Depends(get_auth_service)],
) -> ApiResponseTokenPair:
    """用户登录（设计文档 3.2.1 流程；成功写 Redis 会话并返回 Token 对）。"""
    try:
        pair = await service.login(payload, get_client_ip(request))
    except RateLimitExceeded as exc:
        return too_many_requests_response(exc)
    return success_response(data=pair)


@router.post(
    "/refresh",
    operation_id="userRefreshToken",
    summary="刷新 Access Token（Refresh Token 有效期 7 天）",
    response_model=ApiResponseTokenPair,
    responses={
        401: {"description": "1003 Refresh Token 失效/过期（需重新登录）"},
        422: {"description": "2001 参数错误"},
        429: {"description": "限流触发（响应体 code 复用 5001）"},
        500: {"description": "5000 服务器内部错误"},
    },
)
async def user_refresh_token(
    payload: RefreshTokenRequest,
    service: Annotated[AuthService, Depends(get_auth_service)],
) -> ApiResponseTokenPair:
    """刷新 Token 对（一次一换，旧 Refresh Token 立即作废，防重放）。"""
    try:
        pair = await service.refresh(payload)
    except RateLimitExceeded as exc:
        return too_many_requests_response(exc)
    return success_response(data=pair)


@router.post(
    "/logout",
    operation_id="userLogout",
    summary="注销会话（主动登出）",
    response_model=ApiResponseEmpty,
    responses={
        401: {"description": "1001/1003 Token 缺失或无效"},
        500: {"description": "5000 服务器内部错误"},
    },
)
async def user_logout(
    claims: Annotated[dict[str, Any], Depends(get_logout_principal)],
    service: Annotated[AuthService, Depends(get_auth_service)],
    payload: Annotated[LogoutRequest | None, Body()] = None,
) -> ApiResponseEmpty:
    """注销（幂等：重复调用仍返回 code=0；可选 refresh_token 随会话撤销一并作废）。

    ``payload`` 仅做结构校验与审计（Refresh Token 有效性绑定会话，随 DEL 生效），
    其值不落日志（脱敏约束）。
    """
    await service.logout(str(claims["sub"]))
    return success_response(data=None)


@router.get(
    "/me",
    operation_id="getCurrentUser",
    summary="当前登录用户信息与权限",
    response_model=ApiResponseUserProfile,
    responses={
        401: {"description": "1001 未认证 / 1003 Token 过期"},
        500: {"description": "5000 服务器内部错误"},
        503: {"description": "5001 服务不可用（Redis 会话强依赖不可用）"},
    },
)
async def get_current_user_profile(
    claims: Annotated[dict[str, Any], Depends(get_current_user)],
    service: Annotated[AuthService, Depends(get_auth_service)],
) -> ApiResponseUserProfile:
    """当前用户（JWT 解析结果 + 用户资料；前端渲染菜单与 v-permission 依据）。"""
    profile = await service.me(claims)
    return success_response(data=profile)


@router.post(
    "/change-password",
    operation_id="userChangePassword",
    summary="修改密码（G-06 首登强制改密）",
    response_model=ApiResponseEmpty,
    responses={
        401: {"description": "1001 未认证（旧口令错误/会话失效，统一响应不区分原因）"},
        422: {"description": "2001 参数错误（新口令强度不符/与旧口令相同）"},
        500: {"description": "5000 服务器内部错误"},
    },
)
async def user_change_password(
    payload: ChangePasswordRequest,
    claims: Annotated[dict[str, Any], Depends(get_current_user)],
    service: Annotated[AuthService, Depends(get_auth_service)],
) -> ApiResponseEmpty:
    """修改密码（G-06：初始化账号 must_change_password=true，首登必须改密）。

    口令明文仅经 TLS 传输，不落日志（字段 writeOnly）；成功后复位标志。
    """
    await service.change_password(claims, payload)
    return success_response(data=None)


@router.post(
    "/service-token",
    operation_id="issueServiceToken",
    summary="签发服务令牌（脚本/仿真等非交互组件的独立 Bearer Token）",
    response_model=ApiResponseServiceToken,
    responses={
        401: {"description": "1001/1003 控制台 Token 缺失或无效（服务令牌不得签发服务令牌）"},
        403: {"description": "1002 角色不足（需 SERVICE_TOKEN_ISSUE_ROLES，默认 admin）"},
        422: {"description": "2001 参数错误（client_id 格式 / ttl_seconds 越界）"},
        500: {"description": "5000 服务器内部错误"},
        503: {"description": "5001 服务不可用（Redis 会话强依赖不可用）"},
    },
)
async def issue_service_token(
    payload: ServiceTokenRequest,
    claims: Annotated[dict[str, Any], Depends(get_current_user)],
    service: Annotated[AuthService, Depends(get_auth_service)],
) -> ApiResponseServiceToken:
    """签发服务令牌（V1.23.6；令牌不落日志，同一 client_id 重复签发即轮换）。

    ``get_current_user`` 仅接受 Access Token，故服务令牌无法自签发（避免权限升级链）。
    """
    token = await service.issue_service_token(claims, payload)
    return success_response(data=token)


@router.delete(
    "/service-token/{client_id}",
    operation_id="revokeServiceToken",
    summary="撤销服务令牌（注销即失效）",
    response_model=ApiResponseEmpty,
    responses={
        401: {"description": "1001/1003 控制台 Token 缺失或无效"},
        403: {"description": "1002 角色不足（需 SERVICE_TOKEN_ISSUE_ROLES）"},
        422: {"description": "2001 参数错误（client_id 格式不符）"},
        500: {"description": "5000 服务器内部错误"},
        503: {"description": "5001 服务不可用（Redis 会话强依赖不可用）"},
    },
)
async def revoke_service_token(
    client_id: Annotated[
        str,
        Path(
            min_length=3,
            max_length=64,
            pattern=r"^[a-z0-9][a-z0-9._-]{2,63}$",
            description="服务令牌调用方标识（契约 ServiceClientId）",
        ),
    ],
    claims: Annotated[dict[str, Any], Depends(get_current_user)],
    service: Annotated[AuthService, Depends(get_auth_service)],
) -> ApiResponseEmpty:
    """撤销服务令牌（幂等：DEL session:service:{client_id}；用户会话不受影响）。"""
    await service.revoke_service_token(claims, client_id)
    return success_response(data=None)