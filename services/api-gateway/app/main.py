"""api-gateway — FastAPI 应用入口。

API 网关：统一接入、JWT 认证鉴权、五级限流熔断、路由转发、日志审计

- 统一响应格式 + 预定义错误码（hunter_common）
- 统一认证：POST /api/v1/user/login|refresh|logout、GET /api/v1/user/me（契约自持端点）
- 五级限流中间件（附录 D）+ 反向代理转发（x-hunter-gateway-routes）
- 全链路 trace_id（X-Request-ID）中间件
- /healthz 存活探针、/readyz 就绪探针（DB/Redis 连通性）
- /metrics Prometheus 指标端点（供 infra/monitoring 抓取）

装配顺序（关键）：
- 路由：health → auth →（debug 调试端点）→ catch-all 代理【必须最后】，
  否则 catch-all 会吞掉自持端点
- 中间件（后添加者在外层先执行）：CORS → Metrics → 限流 → trace_id
"""
from __future__ import annotations

import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from hunter_common.database import DatabaseSessionManager
from hunter_common.logging import (
    configure_logging,
    get_logger,
    reset_trace_id,
    set_trace_id,
)
from hunter_common.metrics import register_metrics
from hunter_common.redis import RedisManager

from app.config import settings
from app.core.error_handlers import register_exception_handlers
from app.core.middleware import GatewayRateLimitMiddleware
from app.routers.auth import router as auth_router
from app.routers.health import router as health_router
from app.routers.proxy import router as proxy_router

logger = get_logger("app.main")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """应用生命周期：初始化日志/DB/Redis 管理器与转发 HTTP 客户端（惰性连接）。"""
    configure_logging(
        settings.service_name,
        settings.log_level,
        json_output=settings.environment != "dev",
    )
    app.state.db = DatabaseSessionManager(settings)
    app.state.db.init()
    app.state.redis = RedisManager(settings)
    app.state.redis.init()
    # 转发客户端：内部服务间转发禁用环境代理（trust_env=False），防误经外部代理
    app.state.http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(
            settings.proxy_timeout_seconds, connect=settings.proxy_connect_timeout_seconds
        ),
        limits=httpx.Limits(
            max_connections=settings.proxy_max_connections,
            max_keepalive_connections=max(1, settings.proxy_max_connections // 2),
        ),
        trust_env=False,
    )
    logger.info("service_started", service=settings.service_name, port=settings.api_port)
    try:
        yield
    finally:
        await app.state.http_client.aclose()
        app.state.http_client = None
        await app.state.redis.close()
        await app.state.db.close()
        logger.info("service_stopped", service=settings.service_name)


app = FastAPI(
    title="HunterEdge - api-gateway",
    description="API 网关：统一接入、JWT 认证鉴权、五级限流熔断、路由转发、日志审计",
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/docs" if settings.debug else None,  # 生产环境关闭 Swagger
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 指标埋点中间件（早于限流注册 → 位于限流内层：429 响应同样计入指标）
register_metrics(app, settings.service_name, version="0.1.0")

# 五级限流中间件（位于 trace_id 内层：复用链路 trace_id 构造统一响应）
app.add_middleware(GatewayRateLimitMiddleware, settings=settings)


@app.middleware("http")
async def trace_id_middleware(request: Request, call_next) -> Response:
    """全链路 trace_id：优先透传上游 X-Request-ID，否则生成 UUID（可观测性约束）。"""
    trace_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    token = set_trace_id(trace_id)
    start = time.perf_counter()
    try:
        response: Response = await call_next(request)
        response.headers["X-Request-ID"] = trace_id
        logger.info(
            "http_request",
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            duration_ms=round((time.perf_counter() - start) * 1000, 2),
        )
        return response
    finally:
        reset_trace_id(token)


app.include_router(health_router)
app.include_router(auth_router)
register_exception_handlers(app)

if settings.debug:
    # 仅 debug 环境：用于验证未预期异常的统一响应（code=5000）
    @app.get("/dev/null-500", include_in_schema=False)
    async def _unhandled_sample() -> None:
        raise RuntimeError("debug only: unhandled exception sample")

# catch-all 反向代理【必须最后注册】：仅承接未被自持端点匹配的路径
app.include_router(proxy_router)
