"""data-analytics — FastAPI 应用入口。

数据分析：Flink 实时流处理、Spark 离线批处理、指标计算、Corner Case 挖掘、报告生成

- 统一响应格式 + 预定义错误码（hunter_common）
- 全链路 trace_id（X-Request-ID）中间件
- /healthz 存活探针、/readyz 就绪探针（DB/Redis 连通性）
- /metrics Prometheus 指标端点（供 infra/monitoring 抓取）
"""
from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any

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
from app.consumers.algorithm_metrics import AlgorithmMetricsConsumer
from app.core import dependencies
from app.core.error_handlers import register_exception_handlers
from app.repositories.algorithm_metrics import AlgorithmMetricsWriter
from app.repositories.pipeline import MetricsReadOnlyRepository
from app.routers import corner_cases, coverage, dashboard, evaluation, reports
from app.routers.health import router as health_router
from app.services.algorithm_metrics_ingest import AlgorithmMetricsIngest

logger = get_logger("app.main")


#: 消费者崩溃后的重启退避（秒）：依赖未恢复时避免重启风暴（对齐 data-collector 采集链路）
CONSUMER_RESTART_BACKOFF_S = 5.0


class _SupervisedConsumer:
    """algorithm_metrics 消费者监督器：崩溃后重建底层实例并退避重启。

    背景：``KafkaConsumerManager.run()`` 在 ``finally`` 关闭底层 ``Consumer``，崩溃后旧实例
    不可复用，故以 factory 重建；无监督时任一消费任务异常退出即静默永久停摆（进程与
    /healthz 仍健康）。落库缓冲 ``AlgorithmMetricsIngest`` 跨重建复用（未冲刷样本不丢）。
    """

    def __init__(self, factory: Callable[[], AlgorithmMetricsConsumer]) -> None:
        self._factory = factory
        self.group_id = AlgorithmMetricsConsumer.group_id
        self._current: AlgorithmMetricsConsumer | None = None
        self._stopping = False

    async def run(self) -> None:
        """监督主循环：``run()`` 正常返回 = 优雅停机；抛异常 = 崩溃 → 退避后重建重启。"""
        while not self._stopping:
            consumer = self._factory()
            self._current = consumer
            try:
                await consumer.run()
                return  # stop() 已置位，底层消费循环正常收尾
            except asyncio.CancelledError:
                raise
            except Exception:  # 单消费者崩溃不得拖垮进程：记录堆栈后重建重启
                logger.exception(
                    "consumer_crashed_restarting",
                    group_id=self.group_id,
                    backoff_seconds=CONSUMER_RESTART_BACKOFF_S,
                )
                await asyncio.sleep(CONSUMER_RESTART_BACKOFF_S)
            finally:
                self._current = None

    def stop(self) -> None:
        """请求优雅停机：置位并停止当前活动实例（正处退避睡眠时由外层 task.cancel 打断）。"""
        self._stopping = True
        if self._current is not None:
            self._current.stop()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """应用生命周期：初始化日志/DB/Redis 管理器与 algorithm_metrics 落库消费者（惰性连接）。"""
    configure_logging(
        settings.service_name,
        settings.log_level,
        json_output=settings.environment != "dev",
    )
    # 只读就绪探针仓库（审查 Y10）：跨 schema 读路径与 /readyz 仅以 hunter_analytics_ro 只读账号执行
    app.state.readonly_metrics = MetricsReadOnlyRepository(settings)
    app.state.redis = RedisManager(settings)
    app.state.redis.init()
    # ---------- algorithm_metrics 落库链路（契约 consumer-groups.yaml: data-analytics-algorithm-metrics） ----------
    # Flink algorithm_performance_monitor 产出 algorithm_metrics Topic → 本服务常驻消费者落库。
    # Y10 只读约束仅针对**跨 schema 读路径与就绪探针**；本服务写**自身** data_analytics.algorithm_metrics
    # 合法（契约 db_access.write 声明该表为本服务唯一写方），故初始化 RW DatabaseSessionManager（POSTGRES_USER）。
    app.state.db = DatabaseSessionManager(settings)
    app.state.db.init()
    metrics_ingest = AlgorithmMetricsIngest(AlgorithmMetricsWriter(app.state.db), settings)
    app.state.algorithm_metrics_ingest = metrics_ingest
    supervisors: list[_SupervisedConsumer] = []
    if settings.algorithm_metrics_consumer_enabled:
        supervisors.append(
            _SupervisedConsumer(lambda: AlgorithmMetricsConsumer(settings, metrics_ingest))
        )
    app.state.metrics_consumers = supervisors
    consumer_tasks = [
        asyncio.create_task(supervisor.run(), name=f"consumer-{supervisor.group_id}")
        for supervisor in supervisors
    ]
    logger.info(
        "service_started",
        service=settings.service_name,
        port=settings.api_port,
        consumers=[supervisor.group_id for supervisor in supervisors],
    )
    yield
    # 停机顺序：先停消费（避免使用已关闭的 DB），再释放依赖
    for supervisor in supervisors:
        supervisor.stop()
    for task in consumer_tasks:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:  # 单个消费者停机异常不得阻断其余资源释放
            logger.exception("consumer_stop_failed")
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
    await app.state.db.close()
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
