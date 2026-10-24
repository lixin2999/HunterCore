"""vil-forwarder — FastAPI 入口（健康探针 + 常驻 Kafka 消费者）。"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from hunter_common.kafka.producer import KafkaProducerManager
from hunter_common.logging import configure_logging, get_logger
from hunter_common.metrics import register_metrics
from hunter_common.redis import RedisManager

from app.config import settings
from app.consumer import ConsumerSupervisor, VilForwarderConsumer
from app.forwarder import VilForwarder
from app.session_registry import SessionRegistry

logger = get_logger("app.main")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging(
        settings.service_name,
        settings.log_level,
        json_output=settings.environment != "dev",
    )
    app.state.redis = RedisManager(settings)
    app.state.redis.init()
    KafkaProducerManager.initialize(settings)
    registry = SessionRegistry(
        app.state.redis.client,
        cache_ttl_seconds=settings.vil_session_cache_ttl_ms / 1000.0,
        max_entries=settings.vil_max_tracked_vehicles,
    )
    forwarder = VilForwarder(
        KafkaProducerManager.instance(),
        registry,
        target_hz=settings.vil_forward_target_hz,
        max_source_lag_ms=settings.vil_max_source_lag_ms,
        max_tracked_vehicles=settings.vil_max_tracked_vehicles,
        idle_ttl_seconds=settings.vil_state_idle_ttl_seconds,
    )
    app.state.forwarder = forwarder
    app.state.session_registry = registry

    task: asyncio.Task[None] | None = None
    supervisor: ConsumerSupervisor | None = None
    if settings.vil_consumer_enabled:
        supervisor = ConsumerSupervisor(lambda: VilForwarderConsumer(settings, forwarder))
        app.state.consumer_supervisor = supervisor
        task = asyncio.create_task(supervisor.run(), name="vil-forwarder-consumer")

    logger.info(
        "service_started",
        service=settings.service_name,
        port=settings.api_port,
        target_hz=settings.vil_forward_target_hz,
    )
    try:
        yield
    finally:
        if task is not None:
            if supervisor is not None:
                supervisor.stop()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("vil_forwarder_consumer_stop_failed")
        await forwarder.close()
        try:
            await KafkaProducerManager.instance().close()
        except RuntimeError:
            pass
        await app.state.redis.close()
        logger.info("service_stopped", service=settings.service_name)


app = FastAPI(
    title="HunterCore - vil-forwarder",
    description="VIL 实车状态转发：telemetry_clean → carla.{sim_id}.vil_state（10Hz latest-wins）",
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/docs" if settings.debug else None,
)
register_metrics(app, settings.service_name, version="0.1.0")


@app.get("/healthz", include_in_schema=False)
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/readyz", include_in_schema=False)
async def readyz() -> dict[str, object]:
    redis_ok = await app.state.redis.ping()
    try:
        kafka_ok = await KafkaProducerManager.instance().healthcheck()
    except RuntimeError:
        kafka_ok = False
    return {
        "status": "ok" if redis_ok and kafka_ok else "degraded",
        "checks": {"redis": redis_ok, "kafka": kafka_ok},
    }


@app.get("/status", include_in_schema=False)
async def status() -> dict[str, object]:
    forwarder: VilForwarder = app.state.forwarder
    return {
        "published": forwarder.published_count,
        "skipped": forwarder.skipped_count,
        "tracked_sessions": forwarder.tracked_sessions,
        "drop_counts": forwarder.drop_counts,
        "target_hz": settings.vil_forward_target_hz,
    }
