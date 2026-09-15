"""Prometheus 指标暴露（架构原则 6：可观测性 —— 指标 / 结构化日志 / 链路追踪）。

约定：
- 指标统一前缀 ``hunter_``，标签必须含 ``service``；禁止高基数标签
  （HTTP 路径使用**路由模板**而非原始 URL，避免 ``/api/v1/scene/{id}`` 基数爆炸）。
- ``/metrics`` 返回 Prometheus 文本格式（非统一 JSON 响应体，属契约明确例外：
  由 Prometheus 抓取器消费，见 infra/monitoring/prometheus/prometheus.yml）。
- 时延直方图桶覆盖性能指标 P95 ≤ 200ms（设计文档性能指标章节）。

多进程说明：``uvicorn --workers N`` 多进程部署需设置 ``PROMETHEUS_MULTIPROC_DIR``
并使用 MultiProcessCollector；本系统统一采用**单进程多副本**（K8s 水平扩展）方案。
"""
from __future__ import annotations

import time
from typing import Final

from fastapi import FastAPI, Request, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    Info,
    generate_latest,
)
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

# 独立注册表：只暴露本系统指标，避免第三方默认指标污染
REGISTRY: Final[CollectorRegistry] = CollectorRegistry()

# 时延桶（秒）：覆盖 API 响应 P95 ≤ 200ms 性能指标（设计文档性能指标章节）
_HTTP_LATENCY_BUCKETS: Final[tuple[float, ...]] = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.2,
    0.5,
    1.0,
    2.0,
    5.0,
)

HTTP_REQUESTS_TOTAL = Counter(
    "hunter_http_requests_total",
    "HTTP 请求总数（按服务/方法/路由模板/状态码）",
    labelnames=("service", "method", "path", "status"),
    registry=REGISTRY,
)

HTTP_REQUEST_DURATION = Histogram(
    "hunter_http_request_duration_seconds",
    "HTTP 请求时延（秒），P95 观测依据",
    labelnames=("service", "method", "path"),
    buckets=_HTTP_LATENCY_BUCKETS,
    registry=REGISTRY,
)

HTTP_REQUESTS_IN_PROGRESS = Gauge(
    "hunter_http_requests_in_progress",
    "当前处理中的 HTTP 请求数",
    labelnames=("service",),
    registry=REGISTRY,
)

SERVICE_INFO = Info(
    "hunter_service",
    "服务版本与运行信息（用于监控面板与服务发现核对）",
    registry=REGISTRY,
)


def observe_request(
    service: str,
    method: str,
    path_template: str,
    status: int,
    duration_s: float,
) -> None:
    """记录一次请求的计数与时延（供中间件或业务埋点调用）。"""
    HTTP_REQUESTS_TOTAL.labels(service, method, path_template, str(status)).inc()
    HTTP_REQUEST_DURATION.labels(service, method, path_template).observe(duration_s)


def render_metrics() -> Response:
    """渲染 Prometheus 文本格式指标（/metrics 端点响应）。"""
    return Response(content=generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)


class MetricsMiddleware(BaseHTTPMiddleware):
    """HTTP 指标埋点中间件（路由模板作为 path 标签，保证低基数）。"""

    def __init__(self, app: object, service_name: str) -> None:
        super().__init__(app)  # type: ignore[arg-type]
        self._service = service_name

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        if request.url.path == "/metrics":  # 不观测抓取端点自身，避免自指噪声
            return await call_next(request)

        HTTP_REQUESTS_IN_PROGRESS.labels(self._service).inc()
        start = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            return response
        finally:
            duration = time.perf_counter() - start
            route = request.scope.get("route")
            # 优先路由模板（如 /api/v1/scene/{scene_id}）；未匹配路由归入 unmatched
            template = getattr(route, "path", None) or (
                "unmatched" if status == 404 else request.url.path
            )
            observe_request(self._service, request.method, template, status, duration)
            HTTP_REQUESTS_IN_PROGRESS.labels(self._service).dec()


def register_metrics(app: FastAPI, service_name: str, version: str = "0.1.0") -> None:
    """为 FastAPI 应用注册 /metrics 端点与 HTTP 指标埋点中间件。

    Args:
        app: FastAPI 应用实例。
        service_name: 服务名（写入所有指标的 service 标签）。
        version: 服务版本（写入 hunter_service_info，供监控核对发布版本）。
    """
    SERVICE_INFO.info({"service": service_name, "version": version})
    app.add_middleware(MetricsMiddleware, service_name=service_name)

    async def _metrics_endpoint() -> Response:
        """Prometheus 抓取端点（文本格式，非统一 JSON 响应体）。"""
        return render_metrics()

    app.add_api_route(
        "/metrics",
        _metrics_endpoint,
        methods=["GET"],
        include_in_schema=False,
        tags=["observability"],
    )
