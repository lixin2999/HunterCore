"""反向代理服务（契约 x-hunter-gateway-routes / route_headers / circuit_breaker）。

- 路由：7 个前缀（最长前缀匹配）；``strip_prefix=false`` → 转发完整路径
  （后端按 ``/api/v1/<domain>/...`` 全路径注册路由，与各服务契约一致）
- 身份注入（契约 route_headers）：``X-User-Id`` / ``X-Roles`` / ``X-Trace-Id``，
  客户端同名头一律覆盖（防身份伪造）；同时剥离客户端 ``Authorization``
  （后端不重复校验 JWT，仅信任网关注入头，权限校验由后端 RBAC 执行）
- 熔断（契约 circuit_breaker 默认策略）：连续失败/超时率超阈值 → 熔断期内
  直接 503 + code=5001
- 失败语义：连接失败/超时/熔断/后端未配置 → 统一响应 503 + code=5001，
  禁止透传裸错误；上游正常响应（含 4xx/5xx 统一体）原样透传
- 流式转发：请求体经 ``request.stream()`` 边收边发（支持 OTA 大包上传，禁止
  全量入内存）；响应体 ``aiter_raw()`` 透传（保留 content-length/content-encoding）
- WebSocket（``/ws/remote/**``）不在本服务实现范围（契约 auth_detail 标注
  传送方式待人工确认；REST 转发先行，WS 转发待契约确认后补充）
"""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Final

import httpx
from fastapi import Request
from fastapi.responses import Response, StreamingResponse
from hunter_common.exceptions import ResourceNotFoundError, ServiceUnavailableError
from hunter_common.logging import get_logger, get_trace_id
from starlette.background import BackgroundTask

from app.config import Settings
from app.core.circuit_breaker import CircuitBreaker

logger = get_logger("app.services.proxy_service")

#: 逐跳头（RFC 7230 13.5.1）：连接语义，禁止端到端透传
_HOP_BY_HOP: Final[frozenset[str]] = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)

#: 请求侧剥离头：逐跳头 + 网关强制注入的身份头（覆盖防伪造）+ 网关终结的端到端头
_STRIPPED_REQUEST_HEADERS: Final[frozenset[str]] = _HOP_BY_HOP | {
    "host",
    "content-length",  # httpx 依据实际内容重算（流式转发分块传输）
    "authorization",   # 后端信任网关注入的 X-User-Id/X-Roles，禁止透传客户端 Token
    "x-user-id",
    "x-roles",
    "x-trace-id",
    "x-request-id",
}

_BODY_METHODS: Final[frozenset[str]] = frozenset({"POST", "PUT", "PATCH"})

__all__ = [
    "GatewayRoute",
    "ProxyService",
    "build_route_table",
    "get_circuit_breaker",
    "reset_circuit_breakers",
    "resolve_route",
]


@dataclass(frozen=True)
class GatewayRoute:
    """转发路由（契约 routes 条目：prefix / target_service / strip_prefix=false）。"""

    prefix: str
    service_name: str
    base_url: str | None


#: 进程级熔断器注册表（按目标服务隔离；跨请求共享）
_BREAKERS: dict[str, CircuitBreaker] = {}


def get_circuit_breaker(service_name: str, settings: Settings) -> CircuitBreaker:
    """获取（或惰性创建）目标服务的熔断器（阈值来自 Settings）。"""
    breaker = _BREAKERS.get(service_name)
    if breaker is None:
        breaker = CircuitBreaker(
            service_name,
            failure_threshold=settings.circuit_breaker_failure_threshold,
            timeout_rate=settings.circuit_breaker_timeout_rate,
            open_seconds=settings.circuit_breaker_open_seconds,
        )
        _BREAKERS[service_name] = breaker
    return breaker


def reset_circuit_breakers() -> None:
    """清空进程内熔断器状态（测试辅助，生产勿调）。"""
    _BREAKERS.clear()


def build_route_table(settings: Settings) -> list[GatewayRoute]:
    """契约路由表（纯静态配置，不依赖运行时 HTTP 客户端；顺序与契约一致）。

    vehicle/user 前缀归属待确认（契约 ``pending_confirmation``）→ base_url 可能为 None。
    """
    return [
        GatewayRoute(prefix="/api/v1/scene", service_name="scene-service", base_url=settings.scene_service_url),
        GatewayRoute(prefix="/api/v1/data", service_name="data-collector", base_url=settings.data_collector_service_url),
        GatewayRoute(prefix="/api/v1/analytics", service_name="data-analytics", base_url=settings.data_analytics_service_url),
        GatewayRoute(prefix="/api/v1/ota", service_name="ota-service", base_url=settings.ota_service_url),
        GatewayRoute(prefix="/api/v1/remote", service_name="remote-control", base_url=settings.remote_control_service_url),
        GatewayRoute(prefix="/api/v1/vehicle", service_name="vehicle-service", base_url=settings.vehicle_service_url),
        GatewayRoute(prefix="/api/v1/user", service_name="user-service", base_url=settings.user_service_url),
    ]


def resolve_route(routes: Sequence[GatewayRoute], path: str) -> GatewayRoute | None:
    """最长前缀匹配（未命中返回 None → 404 / code=3001 资源不存在）。"""
    matched: GatewayRoute | None = None
    for route in routes:
        if path.startswith(route.prefix) and (
            matched is None or len(route.prefix) > len(matched.prefix)
        ):
            matched = route
    return matched


class ProxyService:
    """HTTP 反向代理（流式转发；失败统一 503 + 5001，禁止透传裸错误）。"""

    def __init__(self, client: httpx.AsyncClient, settings: Settings) -> None:
        self._client = client
        self._settings = settings

    # -----------------------------------------------------------------
    # 路由解析（契约 x-hunter-gateway-routes.routes；前缀不可更改）
    # -----------------------------------------------------------------
    def routes(self) -> list[GatewayRoute]:
        """转发路由表（顺序与契约一致；vehicle/user 归属待确认 → URL 可能为 None）。"""
        return build_route_table(self._settings)

    def resolve_route(self, path: str) -> GatewayRoute | None:
        """最长前缀匹配（未命中返回 None → 404 / code=3001 资源不存在）。"""
        return resolve_route(self.routes(), path)

    # -----------------------------------------------------------------
    # 转发
    # -----------------------------------------------------------------
    async def forward(self, request: Request, claims: dict[str, Any]) -> Response:
        """转发请求至后端（claims 为 get_current_user 解析出的 JWT 载荷）。"""
        path = request.url.path
        route = self.resolve_route(path)
        if route is None:
            raise ResourceNotFoundError  # 网关未路由的路径（契约外）→ 404 + 3001
        if route.base_url is None:
            # 归属待确认（vehicle/user-service 未登记端口与部署）→ 依赖不可用，禁止伪造转发
            logger.warning("proxy_backend_not_configured", service=route.service_name, path=path)
            raise ServiceUnavailableError
        breaker = get_circuit_breaker(route.service_name, self._settings)
        if not breaker.allow():
            logger.warning("circuit_breaker_open", service=route.service_name, path=path)
            raise ServiceUnavailableError

        url = f"{route.base_url}{path}"
        if request.url.query:
            url = f"{url}?{request.url.query}"
        content = None
        if request.method in _BODY_METHODS or request.headers.get("content-length"):
            content = request.stream()  # 流式转发（支持大文件上传，禁止全量入内存）

        forward_request = self._client.build_request(
            request.method, url, headers=self._forward_headers(request, claims), content=content
        )
        try:
            upstream = await self._client.send(forward_request, stream=True)
        except httpx.TimeoutException as exc:
            breaker.record_failure(timeout=True)
            logger.warning("proxy_upstream_timeout", service=route.service_name, path=path)
            raise ServiceUnavailableError("服务不可用") from exc
        except httpx.HTTPError as exc:
            breaker.record_failure(timeout=False)
            logger.warning(
                "proxy_upstream_unreachable",
                service=route.service_name,
                path=path,
                error=type(exc).__name__,
            )
            raise ServiceUnavailableError("服务不可用") from exc
        breaker.record_success()
        logger.info(
            "proxy_forwarded",
            service=route.service_name,
            path=path,
            status=upstream.status_code,
        )
        if upstream.is_stream_consumed:
            # 上游流已被底层完整读取（如 MockTransport 缓存/中间件预读）：
            # 回放缓冲内容，保证 body 语义不变（生产流式路径见下方 StreamingResponse）
            return Response(
                content=upstream.content,
                status_code=upstream.status_code,
                headers=self._response_headers(upstream.headers),
            )
        return StreamingResponse(
            upstream.aiter_raw(),
            status_code=upstream.status_code,
            headers=self._response_headers(upstream.headers),
            background=BackgroundTask(upstream.aclose),  # 确保上游连接释放
        )

    # -----------------------------------------------------------------
    # 请求/响应头处理
    # -----------------------------------------------------------------
    def _forward_headers(self, request: Request, claims: dict[str, Any]) -> dict[str, str]:
        """构造转发头：剥离逐跳/身份头后注入网关身份（契约 route_headers，覆盖防伪造）。"""
        headers: dict[str, str] = {}
        for name, value in request.headers.items():
            if name.lower() in _STRIPPED_REQUEST_HEADERS:
                continue
            headers[name] = value
        trace_id = get_trace_id()
        headers["X-User-Id"] = str(claims["sub"])
        headers["X-Roles"] = ",".join(str(role) for role in (claims.get("roles") or []))
        headers["X-Trace-Id"] = trace_id
        # X-Request-ID = trace_id（契约 info：链路追踪优先透传/生成 UUID）
        headers["X-Request-ID"] = trace_id
        return headers

    @staticmethod
    def _response_headers(upstream_headers: httpx.Headers) -> dict[str, str]:
        """透传上游响应头（剥离逐跳头；保留 content-length/encoding 供 Range/压缩语义）。"""
        headers: dict[str, str] = {}
        for name, value in upstream_headers.items():
            if name.lower() in _HOP_BY_HOP:
                continue
            headers[name] = value
        return headers