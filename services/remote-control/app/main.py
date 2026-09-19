"""remote-control — FastAPI 应用入口。

远程操控：WebRTC 视频流转发、控制指令转发（20Hz）、操作员权限管理、操控会话与录像记录

- 统一响应格式 + 预定义错误码（hunter_common）
- 全链路 trace_id（X-Request-ID）中间件
- /healthz 存活探针、/readyz 就绪探针（DB/Redis 连通性）
- /metrics Prometheus 指标端点（供 infra/monitoring 抓取）
- 业务路由：/api/v1/remote/**（vehicles / session(s) / history，契约 remote-control.yaml）
- Kafka：生产 hunter.{vehicle_id}.remote_control（会话帧）与 hunter.{vehicle_id}.command（信令）；
  消费侧（command_result → rc:session 统计回写）随 WS 控制通道接入（pending #13-#16）
"""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from hunter_common.database import DatabaseSessionManager
from hunter_common.kafka.producer import KafkaProducerManager
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
from app.producers.remote_control import RemoteControlFrameProducer
from app.producers.session_command import SessionCommandProducer
from app.repositories.storage import S3VideoArchiveStorage
from app.routers import history, sessions, vehicles
from app.routers.health import router as health_router
from app.services.history_service import HistoryService
from app.services.session_reaper import SessionReaper
from app.services.session_service import SessionService
from app.services.vehicle_view import VehicleViewReader

logger = get_logger("app.main")


async def _produce(topic: str, key: str | None, payload: dict[str, Any]) -> None:
    """ProduceFn 适配（契约 x-hunter-remote-config.produce_fn）：JSON 序列化 + 单例投递。

    key = vehicle_id（单车辆有序，系统约束第 4 条）；投递失败由 producer 层转 5001。
    """
    await KafkaProducerManager.instance().produce(
        topic, json.dumps(payload, ensure_ascii=False), key=key
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """应用生命周期：初始化日志/DB/Redis/Kafka/MinIO 与业务服务（惰性连接，不阻塞启动）。

    app.state 装配清单（路由经 Depends 从 state 取服务，测试可整体替换）：
    - db / redis：hunter_common 管理器；storage：hunter-video 桶归档存储（S3 协议）；
    - vehicle_view：车辆可控性读模型（Redis 只读）；
    - frame_producer / command_producer：车端会话帧与信令通道（ProduceFn 注入）；
    - session_service / history_service：业务服务。
    """
    configure_logging(
        settings.service_name,
        settings.log_level,
        json_output=settings.environment != "dev",
    )
    app.state.db = DatabaseSessionManager(settings)
    app.state.db.init()
    app.state.redis = RedisManager(settings)
    app.state.redis.init()
    # Kafka 生产者单例（幂等；Producer 构造为懒连接，不阻塞启动）
    KafkaProducerManager.initialize(settings)
    app.state.storage = S3VideoArchiveStorage(
        settings, bucket=settings.minio_video_bucket, region=settings.minio_region
    )
    app.state.vehicle_view = VehicleViewReader(app.state.redis, settings)
    app.state.frame_producer = RemoteControlFrameProducer(_produce, settings)
    app.state.command_producer = SessionCommandProducer(_produce, settings)
    app.state.session_service = SessionService(
        redis=app.state.redis,
        vehicle_view=app.state.vehicle_view,
        frame_producer=app.state.frame_producer,
        command_producer=app.state.command_producer,
        storage=app.state.storage,
        settings=settings,
    )
    app.state.history_service = HistoryService(
        storage=app.state.storage, settings=settings
    )
    # 陈旧会话守护（审查 R7）：残留会话会永久占用车辆互斥位（同车后续接管恒 7001）
    reaper = SessionReaper(
        redis=app.state.redis,
        session_service=app.state.session_service,
        settings=settings,
    )
    app.state.session_reaper = reaper
    await reaper.start()
    logger.info(
        "service_started", service=settings.service_name, port=settings.api_port
    )
    yield
    # 停机顺序：先停会话守护（避免使用已关闭的 Redis），再释放依赖
    await reaper.stop()
    await app.state.storage.close()
    await app.state.redis.close()
    await app.state.db.close()
    try:
        await KafkaProducerManager.instance().close()
    except RuntimeError:
        pass  # 未初始化（测试场景）则跳过 flush
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
app.include_router(vehicles.router)
app.include_router(sessions.router)
app.include_router(history.router)
register_metrics(app, settings.service_name, version="0.1.0")
register_exception_handlers(app)


if settings.debug:
    # 仅 debug 环境：用于验证未预期异常的统一响应（code=5000）
    @app.get("/dev/null-500", include_in_schema=False)
    async def _unhandled_sample() -> None:
        raise RuntimeError("debug only: unhandled exception sample")
