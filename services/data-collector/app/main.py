"""data-collector — FastAPI 应用入口。

数据采集：Kafka 消息消费接入、数据预处理、数据路由、文件上传管理

- 统一响应格式 + 预定义错误码（hunter_common）
- 全链路 trace_id（X-Request-ID）中间件
- /healthz 存活探针、/readyz 就绪探针（DB/Redis 连通性）
- /metrics Prometheus 指标端点（供 infra/monitoring 抓取）
- 业务路由：/api/v1/data/**（telemetry / events / files，契约 data-collector.yaml）
- 采集链路（审查 R1 补齐）：三路 Kafka 消费者（telemetry/health/event）+
  车辆状态守护（vehicle:status / vehicle:online:set 读模型唯一写方）
"""
from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

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

from app.config import Settings, settings
from app.consumers.base import BaseIngestConsumer
from app.consumers.events import EventIngestConsumer
from app.consumers.health import HealthIngestConsumer
from app.consumers.telemetry import TelemetryIngestConsumer
from app.core.error_handlers import register_exception_handlers
from app.producers.pipeline import PipelineProducer
from app.producers.sensor_file import SensorFileProducer
from app.repositories.events import EventRepository
from app.repositories.storage import MinioStorage, get_storage
from app.repositories.telemetry import TelemetryRepository
from app.routers import events, files, telemetry
from app.routers.health import router as health_router
from app.services.events import EventService
from app.services.files import FileService
from app.services.ingest import TelemetryIngestService
from app.services.telemetry import TelemetryService
from app.services.vehicle_status import VehicleStatusSweeper, VehicleStatusWriter

logger = get_logger("app.main")


def _build_consumers(
    config: Settings,
    ingest_service: TelemetryIngestService,
    event_repository: EventRepository,
    pipeline_producer: PipelineProducer,
    status_writer: VehicleStatusWriter,
) -> list[BaseIngestConsumer]:
    """按配置装配采集消费者（契约 consumer-groups.yaml 的 3 个 data-collector 组）。"""
    consumers: list[BaseIngestConsumer] = []
    if config.telemetry_consumer_enabled:
        consumers.append(
            TelemetryIngestConsumer(config, ingest_service, status_writer=status_writer)
        )
    if config.health_consumer_enabled:
        consumers.append(HealthIngestConsumer(config, status_writer))
    if config.event_consumer_enabled:
        consumers.append(EventIngestConsumer(config, event_repository, pipeline_producer))
    return consumers


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """应用生命周期：初始化日志/DB/Redis/MinIO 与业务服务（惰性连接，不阻塞启动）。

    app.state 装配清单（路由经 Depends 从 state 取服务，测试可整体替换）：
    - db / redis：hunter_common 管理器（连接池惰性建立）；
    - storage：MinioStorage（boto3 同步客户端，调用经 to_thread 包装；
      botocore 连接池自管理，无显式 close，退出时交由 GC 回收）；
    - telemetry_service / event_service / file_service：业务服务（构造注入仓储与依赖）。
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

    storage: MinioStorage = await get_storage()
    app.state.storage = storage
    telemetry_repository = TelemetryRepository(app.state.db)
    event_repository = EventRepository(app.state.db)
    app.state.telemetry_service = TelemetryService(telemetry_repository)
    app.state.event_service = EventService(event_repository, storage)
    app.state.file_service = FileService(storage, SensorFileProducer())

    # ---------- 采集链路（审查 R1 补齐）：Kafka 消费者 + 车辆状态守护 ----------
    # 契约 x-hunter-ingest-pipeline：telemetry/event/health 三路消费 + telemetry_raw/clean 投递
    pipeline_producer = PipelineProducer(settings)
    status_writer = VehicleStatusWriter(app.state.redis.client)
    ingest_service = TelemetryIngestService(telemetry_repository, pipeline_producer, settings)
    app.state.telemetry_ingest_service = ingest_service
    app.state.vehicle_status_writer = status_writer

    consumers = _build_consumers(settings, ingest_service, event_repository, pipeline_producer, status_writer)
    app.state.ingest_consumers = consumers
    consumer_tasks = [
        asyncio.create_task(consumer.run(), name=f"consumer-{consumer.group_id}")
        for consumer in consumers
    ]
    sweeper = VehicleStatusSweeper(app.state.redis.client, settings, status_writer)
    app.state.vehicle_status_sweeper = sweeper
    await sweeper.start()

    logger.info(
        "service_started",
        service=settings.service_name,
        port=settings.api_port,
        consumers=[consumer.group_id for consumer in consumers],
    )
    yield
    # 停机顺序：先停采集消费与守护（避免使用已关闭的 Redis/DB），再释放依赖
    for consumer in consumers:
        consumer.stop()
    for task in consumer_tasks:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:  # 单个消费者停机异常不得阻断其余资源释放
            logger.exception("consumer_stop_failed")
    await sweeper.stop()
    await app.state.redis.close()
    await app.state.db.close()
    try:
        await KafkaProducerManager.instance().close()
    except RuntimeError:
        pass  # 未初始化（测试场景）则跳过 flush
    logger.info("service_stopped", service=settings.service_name)


app = FastAPI(
    title="HunterCore - data-collector",
    description="数据采集：Kafka 消息消费接入、数据预处理、数据路由、文件上传管理",
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
app.include_router(telemetry.router)
app.include_router(events.router)
app.include_router(files.router)
register_metrics(app, settings.service_name, version="0.1.0")
register_exception_handlers(app)


if settings.debug:
    # 仅 debug 环境：用于验证未预期异常的统一响应（code=5000）
    @app.get("/dev/null-500", include_in_schema=False)
    async def _unhandled_sample() -> None:
        raise RuntimeError("debug only: unhandled exception sample")
