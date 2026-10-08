"""vehicle-service — FastAPI 应用入口。

车辆台账与车端接入 provisioning：DB 台账 + Kafka SCRAM/Topic + 每车独立 mTLS 客户端证书 + 接入包下载。

- 统一响应格式 + 预定义错误码（hunter_common）
- 全链路 trace_id（X-Request-ID）中间件
- /healthz 存活、/readyz 就绪（DB / Kafka AdminClient / CA 私钥可读）
- /metrics Prometheus 指标端点
- 业务路由：/api/v1/vehicle/**（详见 contracts/openapi/vehicle-service.yaml）

装配（app.state）：
- db：DatabaseSessionManager；
- kafka_admin：KafkaAdminOps（AdminClient 长连接；构造失败服务不启动）；
- vehicle_store：VehicleStore；
- provisioner：Provisioner（组合 store + admin + openssl）。

⚠ Redis/MinIO 本服务不使用；不装 KafkaProducerManager / Consumer 线程 —— 完全控制面服务。
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
from hunter_common.metrics import register_metrics

from app.config import settings
from app.core.error_handlers import register_exception_handlers
from app.repositories.vehicle import VehicleStore
from app.routers import vehicles
from app.routers.health import router as health_router
from app.services import kafka_admin as kafka_admin_mod
from app.services.provisioner import Provisioner

logger = get_logger("app.main")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """应用生命周期：日志 → DB → Kafka AdminClient → 业务服务装配。

    AdminClient 构造失败视为不可用（服务不启动）：provisioning 属控制面核心，
    Kafka 不可达时其他 CRUD 也无意义（详情视图仍会尝试读 Topic 清单）。
    """
    configure_logging(
        settings.service_name,
        settings.log_level,
        json_output=settings.environment != "dev",
    )
    app.state.db = DatabaseSessionManager(settings)
    app.state.db.init()

    admin = await kafka_admin_mod.build_admin_client_safe()
    app.state.kafka_admin = admin

    app.state.vehicle_store = VehicleStore(app.state.db)
    app.state.provisioner = Provisioner(app.state.vehicle_store, admin, settings)

    logger.info(
        "service_started",
        service=settings.service_name,
        port=settings.api_port,
        admin_bootstrap=settings.admin_bootstrap,
        certs_dir=settings.vehicle_certs_dir,
    )
    try:
        yield
    finally:
        if admin is not None:
            try:
                await admin.aclose()
            except Exception:  # noqa: BLE001
                logger.exception("kafka_admin_close_failed")
        await app.state.db.close()
        logger.info("service_stopped", service=settings.service_name)


app = FastAPI(
    title="HunterCore - vehicle-service",
    description="车辆台账与车端接入 provisioning（一键开通 / 独立证书 / 接入包下载）",
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
    """全链路 trace_id：优先透传上游 X-Request-ID，否则生成 UUID。

    ⚠ 敏感字段防泄漏：请求体含 `scram_password`（写入路径不存在），响应体含一次性口令；
    `http_request` 事件仅记录 method/path/status/duration，不 echo body。
    """
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
register_metrics(app, settings.service_name, version="0.1.0")
register_exception_handlers(app)


if settings.debug:
    # 仅 debug 环境：用于验证未预期异常的统一响应（code=5000）
    @app.get("/dev/null-500", include_in_schema=False)
    async def _unhandled_sample() -> None:
        raise RuntimeError("debug only: unhandled exception sample")
