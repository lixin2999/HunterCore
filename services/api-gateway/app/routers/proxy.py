"""网关转发路由（契约 x-hunter-gateway-routes；资源端点由各后端服务契约定义）。

catch-all 注册约定：``include_in_schema=False`` —— 转发端点的资源定义在各后端
服务契约（contracts/openapi/{service}.yaml），不进入本服务 openapi.json，
保证「实现路由 ⊆ 契约路由」校验（tests/integration/test_api_contract_surface.py）成立。

处理顺序约定（错误语义互斥，不得提前泄露后续环节状态）：
1. **路由表匹配**（纯静态配置）：路由表外路径不属于任何受保护资源 → 404 + code=3001
   （L5 契约面 test_unknown_path_unified_error_body 强制校验；认证不适用）
2. **认证**：契约 routes 全部 ``auth: jwt`` → 严格校验（签名/有效期/会话一致）；
   未认证 401 + 1001 / 过期 401 + 1003 / Redis 不可用 503 + 5001（强依赖）
3. **转发能力**：认证通过后才要求上游客户端就绪（未就绪 → 503 + 5001），
   禁止在认证前把"转发能力不可用"泄露给未认证调用方
"""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import Response
from hunter_common.exceptions import ResourceNotFoundError

from app.config import settings
from app.core.auth import authenticate_request
from app.core.dependencies import get_proxy_service
from app.services.proxy_service import build_route_table, resolve_route

router = APIRouter(tags=["gateway"])

#: 转发支持的 HTTP 方法（契约网关转发无方法限制；CORS 预检由 CORSMiddleware 拦截）
_PROXY_METHODS: tuple[str, ...] = ("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS")

__all__ = ["router"]


@router.api_route(
    "/{path:path}",
    methods=list(_PROXY_METHODS),
    operation_id="gatewayProxy",
    include_in_schema=False,
    summary="反向代理转发（路由表前缀）",
)
async def gateway_proxy(request: Request) -> Response:
    """catch-all 转发：路由匹配 → 404 / 认证 → 401 / 惰性转发 → 上游响应。

    - 路由表外（含 /api/v1/ 下未登记前缀与任何未知路径）→ 404 + code=3001 统一响应体
    - 路由表内 → 严格认证 → 转发（后端未配置 503 + 5001 / 熔断 503 + 5001 /
      连接失败与超时 503 + 5001，禁止透传裸错误）
    """
    if resolve_route(build_route_table(settings), request.url.path) is None:
        raise ResourceNotFoundError("资源不存在")
    claims = await authenticate_request(request)
    proxy = get_proxy_service(request)  # 认证后才要求上游客户端就绪（见模块 docstring 顺序约定）
    return await proxy.forward(request, claims)