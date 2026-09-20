"""ota-service — FastAPI 应用入口。

OTA 管理：版本仓库管理、升级任务调度、灰度发布、升级监控、A/B 分区回滚

- 统一响应格式 + 预定义错误码（hunter_common）
- 全链路 trace_id（X-Request-ID）中间件
- /healthz 存活探针、/readyz 就绪探针（DB/Redis/MinIO 连通性）
- /metrics Prometheus 指标端点（供 infra/monitoring 抓取）
- 业务路由：/api/v1/ota/**（versions / tasks / records，契约 ota-service.yaml）
- Kafka：消费 hunter.*.ota_status（组 ota-service-ota-status）；生产 ota_notify / command
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

from app.config import settings
from app.consumers.ota_status import OtaStatusConsumer
from app.core.error_handlers import register_exception_handlers
from app.producers.ota_notify import OtaNotifyProducer
from app.producers.rollback_command import RollbackCommandProducer
from app.repositories.records import OtaRecordRepository
from app.repositories.storage import get_storage
from app.repositories.tasks import OtaTaskRepository
from app.repositories.versions import OtaVersionRepository
from app.routers import records, tasks, versions
from app.routers.health import router as health_router
from app.services.gates import VehicleStateReader
from app.services.records import RecordService
from app.services.scheduler import RolloutScheduler
from app.services.tasks import TaskService
from app.services.versions import VersionService

logger = get_logger("app.main")


class RedisVehicleStateReader:
    """vehicle:status:{vehicle_id} / vehicle:online:set 读模型访问（Redis 实现）。

    只读（权威值属 vehicle-service；禁止跨服务直连其他 schema，契约 db_cross_service_policy）。
    """

    def __init__(self, redis_manager: RedisManager) -> None:
        self._redis = redis_manager

    async def get_status(self, vehicle_id: str) -> dict[str, str] | None:
        """HGETALL vehicle:status:{vehicle_id}；空 Hash 视为缺失（None）。"""
        mapping = await self._redis.client.hgetall(f"vehicle:status:{vehicle_id}")
        return dict(mapping) if mapping else None

    async def is_online(self, vehicle_id: str) -> bool:
        """SISMEMBER vehicle:online:set。"""
        return bool(await self._redis.client.sismember("vehicle:online:set", vehicle_id))

    async def get_status_many(self, vehicle_ids: list[str]) -> dict[str, dict[str, str] | None]:
        """批量 HGETALL（pipeline，单次往返；G-14 批次批量取数）；空 Hash → None。"""
        if not vehicle_ids:
            return {}
        pipe = self._redis.client.pipeline(transaction=False)
        for vehicle_id in vehicle_ids:
            pipe.hgetall(f"vehicle:status:{vehicle_id}")
        raw = await pipe.execute()
        return {
            vehicle_id: (dict(mapping) if mapping else None)
            for vehicle_id, mapping in zip(vehicle_ids, raw, strict=True)
        }

    async def is_online_many(self, vehicle_ids: list[str]) -> dict[str, bool]:
        """批量 SISMEMBER（pipeline，单次往返；G-14 批次批量取数）。"""
        if not vehicle_ids:
            return {}
        pipe = self._redis.client.pipeline(transaction=False)
        for vehicle_id in vehicle_ids:
            pipe.sismember("vehicle:online:set", vehicle_id)
        raw = await pipe.execute()
        return {vehicle_id: bool(value) for vehicle_id, value in zip(vehicle_ids, raw, strict=True)}


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """应用生命周期：初始化日志/DB/Redis/MinIO/Kafka 与业务服务（惰性连接，不阻塞启动）。

    app.state 装配清单（路由经 Depends 从 state 取服务，测试可整体替换）：
    - db / redis：hunter_common 管理器；storage：MinIO（boto3 经 to_thread）；
    - version_service / task_service / record_service：业务服务（构造注入仓储与依赖）；
    - ota_status_consumer：Kafka 消费任务（OTA_STATUS_CONSUMER_ENABLED=false 可关闭）。
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

    storage = await get_storage()
    app.state.storage = storage
    version_repo = OtaVersionRepository(app.state.db)
    task_repo = OtaTaskRepository(app.state.db)
    record_repo = OtaRecordRepository(app.state.db)
    reader: VehicleStateReader = RedisVehicleStateReader(app.state.redis)
    app.state.version_service = VersionService(version_repo, storage, settings)
    app.state.task_service = TaskService(
        task_repo,
        version_repo,
        record_repo,
        reader,
        OtaNotifyProducer(),
        RollbackCommandProducer(),
        settings,
        storage,
        redis_manager=app.state.redis,
    )
    app.state.record_service = RecordService(record_repo, task_repo)

    consumer_task: asyncio.Task[None] | None = None
    if settings.ota_status_consumer_enabled:
        consumer = OtaStatusConsumer(settings, app.state.db, record_repo, task_repo)
        app.state.ota_status_consumer = consumer
        consumer_task = asyncio.create_task(consumer.run(), name="ota-status-consumer")

    # 灰度自动调度器（x-hunter-canary-rollout.scheduler；ROLLOUT_SCHEDULER_ENABLED=false 可关）
    scheduler_task: asyncio.Task[None] | None = None
    if settings.rollout_scheduler_enabled:
        scheduler = RolloutScheduler(app.state.task_service, settings)
        app.state.rollout_scheduler = scheduler
        scheduler_task = asyncio.create_task(scheduler.run(), name="rollout-scheduler")

    logger.info("service_started", service=settings.service_name, port=settings.api_port)
    yield
    if scheduler_task is not None:
        scheduler_task.cancel()
        try:
            await scheduler_task
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("rollout_scheduler_stop_failed")
    if consumer_task is not None:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("ota_status_consumer_stop_failed")
    # 等待后台任务收尾（审查 Y9：进度缓存 fire-and-forget 写入不遗留悬挂任务）
    await app.state.task_service.aclose()
    await app.state.redis.close()
    await app.state.db.close()
    try:
        await KafkaProducerManager.instance().close()
    except RuntimeError:
        pass  # 未初始化（测试场景）则跳过 flush
    logger.info("service_stopped", service=settings.service_name)


app = FastAPI(
    title="HunterCore - ota-service",
    description="OTA 管理：版本仓库管理、升级任务调度、灰度发布、升级监控、A/B 分区回滚",
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
app.include_router(versions.router)
app.include_router(tasks.router)
app.include_router(records.router)
register_metrics(app, settings.service_name, version="0.1.0")
register_exception_handlers(app)


if settings.debug:
    # 仅 debug 环境：用于验证未预期异常的统一响应（code=5000）
    @app.get("/dev/null-500", include_in_schema=False)
    async def _unhandled_sample() -> None:
        raise RuntimeError("debug only: unhandled exception sample")
