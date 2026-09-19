"""data-analytics — FastAPI 应用入口。

数据分析：Flink 实时流处理、Spark 离线批处理、指标计算、Corner Case 挖掘、报告生成

- 统一响应格式 + 预定义错误码（hunter_common）
- 全链路 trace_id（X-Request-ID）中间件
- /healthz 存活探针、/readyz 就绪探针（DB/Redis 连通性）
- /metrics Prometheus 指标端点（供 infra/monitoring 抓取）
"""
from __future__ import annotations

import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from hunter_common.logging import (
    configure_logging,
    get_logger,
    reset_trace_id,
    set_trace_id,
)
from hunter_common.metrics import register_metrics
from hunter_common.redis import RedisManager

from app.config import settings
from app.core import dependencies
from app.core.error_handlers import register_exception_handlers
from app.repositories.pipeline import MetricsReadOnlyRepository
from app.routers import corner_cases, coverage, dashboard, evaluation, reports
from app.routers.health import router as health_router

logger = get_logger("app.main")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """应用生命周期：初始化日志/DB/Redis 管理器（惰性连接，不阻塞启动）。"""
    configure_logging(
        settings.service_name,
        settings.log_level,
        json_output=settings.environment != "dev",
    )
    # 只读就绪探针仓库（审查 Y10）：本服务仅以 hunter_analytics_ro 只读账号访问数据库，
    # 不初始化全权限 DatabaseSessionManager（最小权限；契约 x-hunter-db-readonly）
    app.state.readonly_metrics = MetricsReadOnlyRepository(settings)
    app.state.redis = RedisManager(settings)
    app.state.redis.init()
    logger.info("service_started", service=settings.service_name, port=settings.api_port)
    yield
    # 关闭请求期惰性创建的下游资源（httpx 客户端 / asyncpg 池 / MinIO 客户端）
    closables: list[Any] = getattr(app.state, dependencies.KEY_CLOSABLES, [])
    for closable in closables:
        try:
            await closable.close()
        except Exception:  # noqa: BLE001 - 单个资源释放失败不得阻断其余资源关闭（已记 warning）
            logger.warning("closable_release_failed", type=type(closable).__name__)
    storage = getattr(app.state, dependencies.KEY_STORAGE, None)
    if storage is not None:
        await storage.close()
    await app.state.redis.close()
    await app.state.readonly_metrics.close()
    logger.info("service_stopped", service=settings.service_name)


app = FastAPI(
    title="HunterCore - data-analytics",
    description="数据分析：Flink 实时流处理、Spark 离线批处理、指标计算、Corner Case 挖掘、报告生成",
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
app.include_router(reports.router)
app.include_router(dashboard.router)
app.include_router(evaluation.router)
app.include_router(coverage.router)
app.include_router(corner_cases.router)
register_metrics(app, settings.service_name, version="0.1.0")
register_exception_handlers(app)


if settings.debug:
    # 仅 debug 环境：用于验证未预期异常的统一响应（code=5000）
    @app.get("/dev/null-500", include_in_schema=False)
    async def _unhandled_sample() -> None:
        raise RuntimeError("debug only: unhandled exception sample")
