"""CARLA 仿真回传消费者（消费组 ``data-analytics-carla``）。

只消费 ``sim_status`` / ``sim_metrics``（其余类型由 data-collector-carla 持久化），
在终态时生成 ``scene_test`` 报告并写 ``hunter-reports``；不写 ``data_collector.*``。
"""
from __future__ import annotations

import json
from typing import Any, ClassVar

from confluent_kafka import Message
from hunter_common.kafka.consumer import SCHEMA_AUTO, KafkaConsumerManager
from hunter_common.kafka.idempotency import IdempotencyGuard
from hunter_common.logging import get_logger, reset_vehicle_id, set_vehicle_id
from hunter_common.redis_keys import sim_events_key, sim_metrics_key, sim_status_key
from redis.exceptions import RedisError

from app.config import Settings
from app.services.simulation import SimulationReportService, is_terminal_status

logger = get_logger("app.consumers.carla")

#: 本消费者组内的仿真状态/指标/事件缓存上限（同 key 分区有序，避免依赖其他消费组的 Redis 写入时序）
_MAX_CACHED_SIMS = 1000
_MAX_CACHED_EVENTS = 100


class CarlaSimulationConsumer:
    """scene_test 报告消费者（sim_metrics / sim_status）。"""

    group_id: ClassVar[str] = "data-analytics-carla"
    subscribe_pattern: ClassVar[str] = "carla.*.sensor_data"

    def __init__(
        self,
        settings: Settings,
        *,
        redis_client: Any,
        reports: SimulationReportService,
    ) -> None:
        self._settings = settings
        self._redis = redis_client
        self._reports = reports
        self._guard = IdempotencyGuard(
            namespace=self.group_id,
            max_size=settings.kafka_consumer_idempotency_cache_size,
            ttl_s=settings.kafka_consumer_idempotency_ttl_s,
        )
        # 本组自己的短时缓存：data-analytics-carla 与 data-collector-carla 是两个独立消费组，
        # 不能假设 data-collector 先写入 Redis，否则终态报告可能拿到空指标/空事件。
        self._status_cache: dict[str, dict[str, Any]] = {}
        self._metrics_cache: dict[str, dict[str, Any]] = {}
        self._events_cache: dict[str, list[dict[str, Any]]] = {}
        self._consumer = KafkaConsumerManager(
            settings,
            group_id=settings.carla_consumer_group,
            topics=[settings.carla_sensor_topic_pattern],
            batch_size=settings.consumer_batch_size,
            poll_timeout=settings.consumer_poll_timeout_seconds,
            schema_name=SCHEMA_AUTO,
            idempotency=self._guard,
            idempotency_key=self.idempotency_key,
        )

    @staticmethod
    def idempotency_key(_message: Message, value: Any) -> str:
        if not isinstance(value, dict):
            raise TypeError("carla_sensor_data 消息必须为 JSON 对象")
        return IdempotencyGuard.compose(value.get("sim_id"), value.get("seq"))

    async def _handle(self, message: Message, value: Any) -> None:
        if not isinstance(value, dict):
            raise TypeError(f"消息必须为 JSON 对象，实际 {type(value).__name__}")
        token = set_vehicle_id(str(value.get("vehicle_id") or ""))
        try:
            await self.process(value, topic=str(message.topic()))
        finally:
            reset_vehicle_id(token)

    async def process(self, payload: dict[str, Any], *, topic: str) -> None:
        sim_id = payload.get("sim_id")
        msg_type = payload.get("type")
        body = payload.get("payload")
        if not isinstance(sim_id, str) or not sim_id or not isinstance(body, dict):
            raise ValueError("carla_sensor_data 缺少 sim_id/payload")
        if msg_type == "sim_event":
            self._remember_event(sim_id, body)
        elif msg_type == "sim_metrics":
            self._remember(self._metrics_cache, sim_id, body)
            status = self._status_cache.get(sim_id) or await self._cached_status(sim_id)
            if status is not None and is_terminal_status(status.get("status")):
                await self._report(sim_id, payload, metrics=body, status_doc=status)
        elif msg_type == "sim_status":
            self._remember(self._status_cache, sim_id, body)
            if is_terminal_status(body.get("status")):
                metrics = self._metrics_cache.get(sim_id) or await self._cached_metrics(sim_id)
                await self._report(sim_id, payload, metrics=metrics, status_doc=body)
        logger.debug(
            "carla_simulation_message_ignored_or_processed",
            sim_id=sim_id,
            type=msg_type,
            topic=topic,
            seq=payload.get("seq"),
        )

    async def _report(
        self,
        sim_id: str,
        envelope: dict[str, Any],
        *,
        metrics: dict[str, Any] | None,
        status_doc: dict[str, Any] | None,
    ) -> None:
        events = self._events_cache.get(sim_id) or await self._cached_events(sim_id)
        status_doc = status_doc or {}
        await self._reports.generate(
            sim_id=sim_id,
            scene_id=envelope.get("scene_id") or status_doc.get("scene_id"),
            vehicle_id=envelope.get("vehicle_id") or status_doc.get("vehicle_id"),
            status=status_doc.get("status"),
            metrics=metrics,
            timestamp=envelope.get("timestamp"),
            events=events,
            mode=status_doc.get("mode"),
            started_at=status_doc.get("started_at"),
            finished_at=status_doc.get("finished_at"),
        )

    @staticmethod
    def _remember(cache: dict[str, dict[str, Any]], sim_id: str, value: dict[str, Any]) -> None:
        """写入有界本地缓存（超限淘汰最早写入的 sim_id）。"""
        cache[sim_id] = value
        while len(cache) > _MAX_CACHED_SIMS:
            cache.pop(next(iter(cache)))

    def _remember_event(self, sim_id: str, event: dict[str, Any]) -> None:
        events = self._events_cache.setdefault(sim_id, [])
        events.append(event)
        del events[:-_MAX_CACHED_EVENTS]
        while len(self._events_cache) > _MAX_CACHED_SIMS:
            self._events_cache.pop(next(iter(self._events_cache)))

    async def _cached_status(self, sim_id: str) -> dict[str, Any] | None:
        return await self._get_json(sim_status_key(sim_id))

    async def _cached_metrics(self, sim_id: str) -> dict[str, Any] | None:
        return await self._get_json(sim_metrics_key(sim_id))

    async def _cached_events(self, sim_id: str) -> list[dict[str, Any]]:
        key = sim_events_key(sim_id)
        try:
            items = await self._redis.lrange(key, 0, 99)
        except RedisError as exc:
            logger.warning("sim_events_cache_read_failed", sim_id=sim_id, error=str(exc))
            return []
        events: list[dict[str, Any]] = []
        for item in items:
            try:
                doc = json.loads(item.decode() if isinstance(item, bytes) else item)
            except (TypeError, json.JSONDecodeError):
                continue
            if isinstance(doc, dict):
                events.append(doc)
        # Redis List 用 LPUSH 写入（新→旧）；报告按时间正序展示
        events.reverse()
        return events

    async def _get_json(self, key: str) -> dict[str, Any] | None:
        try:
            raw = await self._redis.get(key)
        except RedisError as exc:
            logger.warning("sim_cache_read_failed", key=key, error=str(exc))
            return None
        if raw is None:
            return None
        try:
            doc = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
        except (TypeError, json.JSONDecodeError):
            logger.warning("sim_cache_corrupted", key=key)
            return None
        return doc if isinstance(doc, dict) else None

    async def run(self) -> None:
        await self._consumer.run(self._handle)

    def stop(self) -> None:
        self._consumer.stop()


__all__ = ["CarlaSimulationConsumer"]
