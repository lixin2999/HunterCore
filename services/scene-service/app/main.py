"""scene-service — FastAPI 应用入口。

场景生成：场景库管理、场景编辑、场景参数化、OpenSCENARIO 导出、实车场景自动提取

- 统一响应格式 + 预定义错误码（hunter_common）
- 全链路 trace_id（X-Request-ID）中间件
- /healthz 存活探针、/readyz 就绪探针（DB/Redis 连通性）
- /metrics Prometheus 指标端点（供 infra/monitoring 抓取）
- 业务路由：/api/v1/scene/**（scenes / templates / export / simulation / sim_nodes，契约 scene-service.yaml）；
  场景引擎路由（logical / generate / concrete / testcases / suites / coverage / oracles）
  复用同一前缀，必须先于 scenes 注册（否则被 /{scene_id} 吞掉）；
- 下发链路（SIM_DISPATCH_MODE）：direct 直连固定地址 CARLA 管理 API（默认，V1.23.7 行为）；
  node 动态地址直推 —— 节点注册/心跳时平台从入站请求头自学其地址，下发时按 node_id
  取**当前**地址回访节点上的 CARLA 管理 API（V1.24.0，需平台→节点可达）；
  两种模式仅 backend 实现不同，服务层依 SimRuntimeBackend 协议复用同一套流程；
- Kafka：仅消费 analytics_result（组 scene-service-analytics-result，4.5 节实车场景自动提取）；
  契约禁止本服务生产任何 Topic（x-hunter-kafka.produces = []）
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
from hunter_common.kafka.admin import KafkaAdmin
from hunter_common.kafka.contracts import KafkaContractError
from hunter_common.logging import (
    configure_logging,
    get_logger,
    reset_trace_id,
    set_trace_id,
)
from hunter_common.metrics import register_metrics
from hunter_common.redis import RedisManager

from app.config import settings
from app.consumers.analytics_result import AnalyticsResultConsumer
from app.core.error_handlers import register_exception_handlers
from app.repositories.cache import SceneDetailCache
from app.repositories.carla import CarlaManagementClient
from app.repositories.engine import (
    AuditLogRepository,
    ConcreteScenarioRepository,
    GenerationTaskRepository,
    LogicalScenarioRepository,
    SuiteRepository,
    TestCaseRepository,
)
from app.repositories.scenes import SceneRepository
from app.repositories.sim_dispatch import SimNodeBackend
from app.repositories.sim_node_registry import SimNodeRegistryStore
from app.repositories.sim_nodes import SimNodeStore
from app.repositories.simulations import SimTopicProvisioner, VilSessionStore
from app.repositories.storage import get_storage
from app.routers import engine, export, scenes, sim_nodes, simulation, templates
from app.routers.health import router as health_router
from app.services.engine import SceneEngineService
from app.services.export import SceneExportService
from app.services.scenes import SceneService
from app.services.sim_nodes import SimNodeService
from app.services.simulation import SceneSimulationService
from app.services.templates import SceneTemplateService

logger = get_logger("app.main")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """应用生命周期：初始化日志/DB/Redis/MinIO/Carla 与业务服务（惰性连接，不阻塞启动）。

    app.state 装配清单（路由经 Depends 从 state 取服务，测试可整体替换）：
    - db / redis：hunter_common 管理器；storage：MinIO（boto3 经 to_thread）；
    - carla：下发后端（SimRuntimeBackend）—— node 模式为 SimNodeBackend（按自学习地址直推），
      否则 CarlaManagementClient（固定地址）；
      sim_node_store / sim_node_service：仿真节点控制面（两种模式都装配，便于运维查清单）；
    - scene_service / template_service / export_service / simulation_service：业务服务；
    - analytics_result_consumer：Kafka 消费任务（SCENE_EXTRACTION_CONSUMER_ENABLED=false 可关闭）。
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
    app.state.storage = await get_storage(
        settings.minio_endpoint,
        settings.minio_access_key,
        settings.minio_secret_key,
        settings.minio_region,
        settings.minio_bucket_scene_assets,
        settings.scene_export_presign_expire_seconds,
        secure=settings.minio_secure,
    )
    # 仿真节点仓储（两种模式都装配：node 用于下发寻址，direct 下仍可供运维查节点清单）
    sim_node_store = SimNodeStore(
        app.state.redis.client,
        lease_seconds=settings.sim_node_lease_seconds,
        instance_ttl_seconds=settings.sim_instance_ttl_seconds,
        default_capacity=settings.sim_node_default_capacity,
    )
    app.state.sim_node_store = sim_node_store
    if settings.sim_dispatch_via_node:
        # 动态地址直推：平台按注册/心跳学到的 ip:port 回访节点上的 CARLA 管理 API
        app.state.carla = SimNodeBackend(sim_node_store, settings)
        logger.info(
            "sim_dispatch_mode",
            mode=settings.sim_dispatch_mode,
            lease_seconds=settings.sim_node_lease_seconds,
            manager_default_port=settings.sim_node_manager_default_port,
            trust_forwarded_for=settings.sim_node_trust_forwarded_for,
            allowed_networks=settings.sim_node_allowed_networks or None,
        )
    else:
        app.state.carla = CarlaManagementClient(
            settings.carla_management_endpoint,
            create_path=settings.carla_instance_create_path,
            query_path=settings.carla_instance_query_path,
            scenario_path=settings.carla_scenario_submit_path,
            timeout_seconds=settings.carla_request_timeout_seconds,
            max_retries=settings.carla_request_max_retries,
            get_path=settings.carla_instance_get_path,
            result_path=settings.carla_instance_result_path,
            pause_path=settings.carla_instance_pause_path,
            resume_path=settings.carla_instance_resume_path,
            stop_path=settings.carla_instance_stop_path,
        )
    # VIL 会话仓储（scene-service 为唯一写方；vil-forwarder 只读）
    vil_sessions = VilSessionStore(
        app.state.redis.client, ttl_seconds=settings.vil_session_ttl_seconds
    )
    app.state.vil_sessions = vil_sessions
    # 动态 Topic 供给（Kafka Admin 同步调用经线程池；契约缺失时启动告警并降级为不创建）
    topic_provisioner: SimTopicProvisioner | None = None
    if settings.sim_topic_provisioning_enabled:
        try:
            app.state.kafka_admin = KafkaAdmin(
                settings, replication_factor=settings.sim_topic_replication_factor
            )
            topic_provisioner = SimTopicProvisioner(app.state.kafka_admin)
        except KafkaContractError:
            logger.exception("sim_topic_provisioning_unavailable")
    app.state.sim_topic_provisioner = topic_provisioner
    scene_repository = SceneRepository(app.state.db)
    app.state.scene_repository = scene_repository
    app.state.scene_service = SceneService(
        scene_repository,
        SceneDetailCache(app.state.redis, settings.scene_detail_cache_ttl_seconds),
        settings,
    )
    app.state.template_service = SceneTemplateService(settings)
    app.state.export_service = SceneExportService(
        app.state.scene_service, app.state.storage, settings
    )
    app.state.simulation_service = SceneSimulationService(
        app.state.scene_service,
        app.state.carla,
        settings,
        storage=app.state.storage,
        sessions=vil_sessions,
        topics=topic_provisioner,
        topic_delete_delay_seconds=settings.sim_topic_delete_delay_seconds,
    )
    app.state.sim_node_service = SimNodeService(
        sim_node_store,
        settings,
        # V1.24.1 仿真端管理：登记表（scene_svc.sim_node_registry）与 Redis 实时态互补；
        # 未初始化 DDL 时管理接口按需懒建行，读写走 DB（与令牌/证书登记一致）
        registry_store=SimNodeRegistryStore(app.state.db),
    )
    # 场景引擎（服务私有 schema scene_engine；未初始化 DDL 时读取自动回落到内置目录）
    app.state.engine_service = SceneEngineService(
        LogicalScenarioRepository(app.state.db),
        ConcreteScenarioRepository(app.state.db),
        GenerationTaskRepository(app.state.db),
        TestCaseRepository(app.state.db),
        SuiteRepository(app.state.db),
        settings,
        audit_repository=AuditLogRepository(app.state.db),
    )
    # 启动对账：K8s 多副本下不强制单实例，失联的 pending/running 任务置 failed（可重新提交）
    await app.state.engine_service.reconcile_generation_tasks()
    # VIL 会话启动对账（设计 6.4）：清理无实例/已终态会话，避免车辆被 TTL 阻塞 2 小时
    try:
        await asyncio.wait_for(
            app.state.simulation_service.reconcile_sessions(), timeout=15
        )
    except TimeoutError:
        logger.warning("vil_session_reconcile_timeout")
    except Exception:
        logger.exception("vil_session_reconcile_failed")

    consumer_task: asyncio.Task[None] | None = None
    if settings.scene_extraction_consumer_enabled:
        consumer = AnalyticsResultConsumer(settings, app.state.scene_service)
        app.state.analytics_result_consumer = consumer
        consumer_task = asyncio.create_task(consumer.run(), name="analytics-result-consumer")

    logger.info("service_started", service=settings.service_name, port=settings.api_port)
    yield
    if consumer_task is not None:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("analytics_result_consumer_stop_failed")
    await app.state.simulation_service.shutdown()
    await app.state.carla.close()
    await app.state.redis.close()
    await app.state.db.close()
    logger.info("service_stopped", service=settings.service_name)


app = FastAPI(
    title="HunterCore - scene-service",
    description="场景生成：场景库管理、场景编辑、场景参数化、OpenSCENARIO 导出、实车场景自动提取",
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
# 注意：templates / export / simulation / sim_nodes / engine / scenes 共用前缀 /api/v1/scene，
# 必须先注册具体路径（/templates、/export、/simulations、/sim-nodes、/oracles、/testcases、/suites、/generate、/concrete），
# 再注册含 {scene_id} 的 scenes 路由，否则具体路径会被 /{scene_id} 优先匹配。
app.include_router(templates.router)
app.include_router(export.router)
app.include_router(simulation.router)
app.include_router(sim_nodes.router)
app.include_router(engine.router)
app.include_router(scenes.router)
register_metrics(app, settings.service_name, version="0.1.0")
register_exception_handlers(app)


if settings.debug:
    # 仅 debug 环境：用于验证未预期异常的统一响应（code=5000）
    @app.get("/dev/null-500", include_in_schema=False)
    async def _unhandled_sample() -> None:
        raise RuntimeError("debug only: unhandled exception sample")
