"""remote-control — FastAPI 应用入口。

远程操控：WebRTC 视频流转发、控制指令转发（20Hz）、操作员权限管理、操控会话与录像记录

- 统一响应格式 + 预定义错误码（hunter_common）
- 全链路 trace_id（X-Request-ID）中间件
- /healthz 存活探针、/readyz 就绪探针（DB/Redis 连通性）
"""
from __future__ import annotations

import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from hunter_common.database import DatabaseSessionManager
from hunter_common.logging import (
    configure_logging,
    get_logger,
    reset_trace_id,
    set_trace_id,
)
from hunter_common.redis import RedisManager

from app.config import settings
from app.core.error_handlers import register_exception_handlers
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
    app.state.db = DatabaseSessionManager(settings)
    app.state.db.init()
    app.state.redis = RedisManager(settings)
    app.state.redis.init()
    logger.info("service_started", service=settings.service_name, port=settings.api_port)
    yield
    await app.state.redis.close()
    await app.state.db.close()
    logger.info("service_stopped", service=settings.service_name)


app = FastAPI(
    title="HunterEdge - remote-control",
    description="远程操控：WebRTC 视频流转发、控制指令转发（20Hz）、操作员权限管理、操控会话与录像记录",
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
register_exception_handlers(app)


if settings.debug:
    # 仅 debug 环境：用于验证未预期异常的统一响应（code=5000）
    @app.get("/dev/null-500", include_in_schema=False)
    async def _unhandled_sample() -> None:
        raise RuntimeError("debug only: unhandled exception sample")
