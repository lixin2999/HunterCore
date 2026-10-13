"""ingest-platform — FastAPI 应用入口。

车端 rosbag 消费解析与处理平台：
- 7 个 Kafka 消费者（vehicle.bag.* / vehicle.agent.*）
- PG advisory lock 单主 orchestrator（align/slice/qc 任务编排）
- REST 查询/导出 API（§8.1）

lifespan 装配清单：
- db / redis：hunter_common 管理器（连接池惰性建立）
- storage：LakeStorage（boto3 同步客户端）
- producer：PlatformEventProducer（§7.5）
- projection：ProjectionService（§5.2）
- cmd_service：CmdService（§9.4）
- dataset_service：DatasetService（§8.2）
- orchestrator：Orchestrator（§3.1 任务主循环）
- 7 个 _SupervisedConsumer（崩溃重启）
"""
from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from hunter_common.database import DatabaseSessionManager
from hunter_common.kafka.producer import KafkaProducerManager
from hunter_common.logging import configure_logging, get_logger, reset_trace_id, set_trace_id
from hunter_common.metrics import register_metrics
from hunter_common.redis import RedisManager

from app.config import Settings, settings
from app.consumers.base import BaseIngestConsumer
from app.consumers.consumers import (
    AlertConsumer,
    ArtifactConsumer,
    CmdAckConsumer,
    HeartbeatConsumer,
    ManifestConsumer,
    ParseProgressConsumer,
    TaskStatusConsumer,
)
from app.core.error_handlers import register_exception_handlers
from app.producers.platform_events import PlatformEventProducer
from app.services.align_worker import AlignWorker
from app.services.cmd_service import CmdService
from app.services.dataset_service import DatasetService
from app.services.orchestrator import Orchestrator
from app.services.projection import ProjectionService
from app.services.qc_worker import QcWorker
from app.services.slice_worker import SliceWorker
from app.storage.minio_client import LakeStorage

logger = get_logger("app.main")

#: 消费者崩溃后重启退避（秒）
CONSUMER_RESTART_BACKOFF_S = 5.0


# ========== 监督消费者 ==========

class _SupervisedConsumer:
    """消费者监督器：崩溃后重建底层实例并退避重启。"""

    def __init__(self, factory: Callable[[], BaseIngestConsumer], group_id: str) -> None:
        self._factory = factory
        self.group_id = group_id
        self._current: BaseIngestConsumer | None = None
        self._stopping = False

    async def run(self) -> None:
        while not self._stopping:
            consumer = self._factory()
            self._current = consumer
            try:
                await consumer.run()
                return
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                logger.exception(
                    "consumer_crashed_restarting",
                    group_id=self.group_id,
                    backoff_seconds=CONSUMER_RESTART_BACKOFF_S,
                )
                await asyncio.sleep(CONSUMER_RESTART_BACKOFF_S)
            finally:
                self._current = None

    def stop(self) -> None:
        self._stopping = True
        if self._current is not None:
            self._current.stop()


# ========== 消费者工厂 ==========

def _build_consumers(
    cfg: Settings,
    db_mgr: DatabaseSessionManager,
    projection: ProjectionService,
    cmd_service: CmdService,
) -> list[_SupervisedConsumer]:
    """按配置装配 7 个消费者监督器（§5.1）。"""
    supervisors: list[_SupervisedConsumer] = []

    if cfg.manifest_consumer_enabled:
        supervisors.append(_SupervisedConsumer(
            lambda: ManifestConsumer(cfg, projection),
            ManifestConsumer.group_id,
        ))
    if cfg.task_status_consumer_enabled:
        supervisors.append(_SupervisedConsumer(
            lambda: TaskStatusConsumer(cfg, projection),
            TaskStatusConsumer.group_id,
        ))
    if cfg.parse_progress_consumer_enabled:
        supervisors.append(_SupervisedConsumer(
            lambda: ParseProgressConsumer(cfg),
            ParseProgressConsumer.group_id,
        ))
    if cfg.artifact_consumer_enabled:
        supervisors.append(_SupervisedConsumer(
            lambda: ArtifactConsumer(cfg, projection),
            ArtifactConsumer.group_id,
        ))
    if cfg.heartbeat_consumer_enabled:
        supervisors.append(_SupervisedConsumer(
            lambda: HeartbeatConsumer(cfg, db_mgr),
            HeartbeatConsumer.group_id,
        ))
    if cfg.alert_consumer_enabled:
        supervisors.append(_SupervisedConsumer(
            lambda: AlertConsumer(cfg),
            AlertConsumer.group_id,
        ))
    if cfg.cmd_ack_consumer_enabled:
        supervisors.append(_SupervisedConsumer(
            lambda: CmdAckConsumer(cfg, cmd_service),
            CmdAckConsumer.group_id,
        ))

    return supervisors


# ========== lifespan ==========

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """应用生命周期：初始化 DB/Redis/Kafka/MinIO，装配服务，启动消费者与 orchestrator。"""
    configure_logging(
        settings.service_name,
        settings.log_level,
        json_output=settings.environment != "dev",
    )

    # ---------- 基础设施 ----------
    app.state.db = DatabaseSessionManager(settings)
    app.state.db.init()
    app.state.redis = RedisManager(settings)
    app.state.redis.init()
    KafkaProducerManager.initialize(settings)

    # MinIO 数据湖存储
    storage = LakeStorage(settings)
    app.state.storage = storage

    # ---------- 业务服务 ----------
    producer = PlatformEventProducer()
    app.state.producer = producer

    projection = ProjectionService(settings, app.state.db)
    app.state.projection = projection

    cmd_service = CmdService(settings, app.state.db)
    app.state.cmd_service = cmd_service

    dataset_service = DatasetService(settings, storage, app.state.db, producer)
    app.state.dataset_service = dataset_service

    # ---------- Workers（由 orchestrator 调用，共享服务实例） ----------
    align_worker = AlignWorker(settings, storage, app.state.db, producer)
    slice_worker = SliceWorker(settings, storage, app.state.db, producer)
    qc_worker = QcWorker(settings, storage, app.state.db, producer)

    # ---------- Orchestrator（§9.1 单主 + lease 队列） ----------
    orchestrator = Orchestrator(
        settings=settings,
        db_session=app.state.db.session_factory,  # session_factory（手动 commit）
        align_worker=align_worker,
        slice_worker=slice_worker,
        qc_worker=qc_worker,
    )
    app.state.orchestrator = orchestrator

    # ---------- 启动消费者 ----------
    consumers = _build_consumers(settings, app.state.db, projection, cmd_service)
    app.state.ingest_consumers = consumers
    consumer_tasks = [
        asyncio.create_task(c.run(), name=f"consumer-{c.group_id}")
        for c in consumers
    ]

    # ---------- 启动 orchestrator ----------
    await orchestrator.start()

    logger.info(
        "service_started",
        service=settings.service_name,
        port=settings.api_port,
        consumers=[c.group_id for c in consumers],
        orchestrator_poll_interval=settings.orchestrator_poll_interval_s,
    )
    yield

    # ---------- 优雅停机 ----------
    await orchestrator.stop()
    for consumer in consumers:
        consumer.stop()
    for task in consumer_tasks:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:  # noqa: BLE001
            logger.exception("consumer_stop_failed")

    await app.state.redis.close()
    await app.state.db.close()
    await storage.close()
    try:
        await KafkaProducerManager.instance().close()
    except RuntimeError:
        pass
    logger.info("service_stopped", service=settings.service_name)


# ========== FastAPI 应用 ==========

app = FastAPI(
    title="HunterCore - ingest-platform",
    description="车端 rosbag 消费解析与处理平台：解析入库、多模态对齐、场景切片、质量控制、数据集导出",
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/docs" if settings.debug else None,
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
    """全链路 trace_id：优先透传 X-Request-ID，否则生成 UUID。"""
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


# ---------- 注册路由 ----------
from app.routers import (  # noqa: E402
    agents,
    artifacts,
    bags,
    clips,
    commands,
    datasets,
    tasks,
)
from app.routers.health import router as health_router  # noqa: E402

app.include_router(health_router)
app.include_router(bags.router)
app.include_router(clips.router)
app.include_router(agents.router)
app.include_router(artifacts.router)
app.include_router(datasets.router)
app.include_router(tasks.router)
app.include_router(commands.router)

register_metrics(app, settings.service_name, version="0.1.0")
register_exception_handlers(app)


if settings.debug:
    @app.get("/dev/null-500", include_in_schema=False)
    async def _unhandled_sample() -> None:
        raise RuntimeError("debug only: unhandled exception sample")
