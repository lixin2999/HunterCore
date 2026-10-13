"""ingest-platform 消费者基类（§5.1 消费拓扑 + §5.2 层1 事件仓去重）。

处理流程（附录B on_event 伪码落地）：
1. 内存 IdempotencyGuard（KafkaConsumerManager 内置）——快速路径，拦截短窗口重投递；
2. ``_handle`` 内 INSERT events ON CONFLICT DO NOTHING（权威去重，append-only 事件仓）；
   事件行与业务处理共用同一事务：处理抛错 → 事件行回滚 → Kafka 重投后可重新执行；
3. 下游 process 内部保留层2 seq 守卫 / 层3 任务唯一键，兜住跨消费者重放。
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, ClassVar
from uuid import UUID, uuid5

from confluent_kafka import Message
from hunter_common.database import DatabaseSessionManager
from hunter_common.kafka.consumer import SCHEMA_AUTO, BatchEndHook, KafkaConsumerManager
from hunter_common.kafka.idempotency import IdempotencyGuard
from hunter_common.kafka.producer import KafkaProducerManager
from hunter_common.logging import get_logger, reset_vehicle_id, set_vehicle_id

from app.config import Settings
from app.repositories.events import IngestEventRepository

logger = get_logger("app.consumers.base")

#: 非 UUID 的 event_id 兜底命名空间（确定性派生，保持去重语义）
_EVENT_ID_NS = UUID("6ba7b812-0dae-5d38-9a0c-5b7bb9d095cb")  # NAMESPACE_URL


def _coerce_event_id(raw: Any, topic: str, partition: int, offset: int) -> UUID:
    """event_id 归一化为 UUID：非法值按 (topic, partition, offset) 确定性派生。"""
    try:
        return UUID(str(raw))
    except (ValueError, AttributeError, TypeError):
        return uuid5(_EVENT_ID_NS, f"ingest:{topic}:{partition}:{offset}")


def _parse_occurred_at(raw: Any) -> datetime:
    """occurred_at 解析（ISO 字符串 / epoch 秒 / epoch ns），失败回退当前时间。"""
    if isinstance(raw, (int, float)) and raw > 0:
        seconds = raw / 1e9 if raw > 1e15 else float(raw)
        return datetime.fromtimestamp(seconds, tz=UTC)
    if isinstance(raw, str):
        try:
            # Python 3.11+ fromisoformat 支持 'Z' 后缀
            parsed = datetime.fromisoformat(raw)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            return parsed
        except ValueError:
            pass
    return datetime.now(UTC)


class BaseIngestConsumer:
    """ingest-platform 消费者基类（与 data-collector 同源实现，消费组/主题独立）。"""

    group_id: ClassVar[str] = ""
    subscribe_topic: ClassVar[str] = ""   # 具体 topic 名（非正则）

    def __init__(
        self,
        settings: Settings,
        db_mgr: DatabaseSessionManager,
        *,
        on_batch_end: BatchEndHook | None = None,
    ) -> None:
        self._settings = settings
        self._db_mgr = db_mgr
        self._guard = IdempotencyGuard(
            namespace=self.group_id,
            max_size=settings.kafka_consumer_idempotency_cache_size,
            ttl_s=settings.kafka_consumer_idempotency_ttl_s,
        )
        schema_name = SCHEMA_AUTO if settings.ingest_schema_validation_enabled else None
        self._consumer = KafkaConsumerManager(
            settings,
            group_id=self.group_id,
            topics=[self.subscribe_topic],
            batch_size=settings.consumer_batch_size,
            poll_timeout=settings.consumer_poll_timeout_seconds,
            schema_name=schema_name,
            idempotency=self._guard,
            idempotency_key=self.idempotency_key,
            on_batch_end=on_batch_end,
        )

    async def run(self) -> None:
        await self._consumer.run(self._handle)

    def stop(self) -> None:
        self._consumer.stop()

    @staticmethod
    def idempotency_key(message: Message, value: Any) -> str:
        """默认使用 envelope 中的 event_id（§5.2 层1）。"""
        if isinstance(value, dict):
            return str(value.get("event_id", message.offset()))
        return str(message.offset())

    async def process(self, payload: dict[str, Any], *, topic: str) -> None:
        raise NotImplementedError

    async def _before_process(self, message: Message, value: dict[str, Any]) -> None:
        """处理前置钩子（§5.4 背压）：默认无操作，子类可插入限速/延迟。"""

    def _event_row(self, message: Message, value: dict[str, Any]) -> dict[str, Any]:
        """由 envelope 构造 events 行（§6.2 事件仓，payload 存完整 envelope 供重放）。"""
        topic = str(message.topic())
        partition = int(message.partition())
        offset = int(message.offset())
        seq_raw = value.get("seq")
        return {
            "event_id": _coerce_event_id(value.get("event_id"), topic, partition, offset),
            "event_type": str(value.get("event_type") or topic),
            "bag_id": value.get("bag_id"),
            "vehicle_id": str(value.get("vehicle_id") or ""),
            "seq": int(seq_raw) if seq_raw is not None else None,
            "occurred_at": _parse_occurred_at(value.get("occurred_at")),
            "payload": value,
            "kafka_partition": partition,
            "kafka_offset": offset,
            "origin": str(value.get("origin") or "vehicle"),
        }

    async def _handle(self, message: Message, value: Any) -> None:
        try:
            if not isinstance(value, dict):
                raise TypeError(f"消息必须为 JSON 对象，实际 {type(value).__name__}")
            vehicle_id = value.get("vehicle_id", "")
            token = set_vehicle_id(str(vehicle_id))
            try:
                await self._before_process(message, value)
                async with self._db_mgr.session() as session:
                    row = self._event_row(message, value)
                    is_new = await IngestEventRepository(session).insert_dedup(row)
                    if not is_new:
                        logger.debug(
                            "duplicate_event_skipped",
                            event_id=str(row["event_id"]), topic=row["event_type"],
                        )
                        return
                    # 业务处理与事件行同事务：process 内部自开的 session 先行提交，
                    # 若 process 抛错则事件行回滚，Kafka 重投时可重新执行（层2/层3 幂等兜底）。
                    await self.process(value, topic=str(message.topic()))
            finally:
                reset_vehicle_id(token)
        except (TypeError, ValueError, KeyError, IndexError) as exc:
            # §5.3 死信主题：解析/schema/字段校验类数据错误路由 ingest.dlt（原始消息+错误上下文），
            # 吞不上抛避免无效重试；基础设施错误（连接/超时）仍上抛走 {topic}.dlq 重试。
            await self._route_dlt(message, value, exc)

    async def _route_dlt(self, message: Message, value: Any, exc: BaseException) -> None:
        """将数据性错误消息路由到 DLT 主题（§5.3）；未启用或 Kafka 不可用时降级为日志。"""
        topic = str(message.topic())
        partition = int(message.partition())
        offset = int(message.offset())
        logger.error(
            "dlt_route", topic=topic, partition=partition, offset=offset,
            error_type=type(exc).__name__, error=str(exc),
        )
        if not self._settings.dlt_enabled:
            return
        record: dict[str, Any] = {
            "original_topic": topic,
            "original_partition": partition,
            "original_offset": offset,
            "consumer_group": self.group_id,
            "error_type": type(exc).__name__,
            "error_message": str(exc)[:1000],
            "message": value if isinstance(value, dict) else {"_raw_repr": repr(value)[:2000]},
            "failed_at": datetime.now(UTC).isoformat(),
        }
        try:
            producer = KafkaProducerManager.instance()
            # 低层 produce 只接受 bytes/str：直传 dict 会被 confluent_kafka 拒绝并被 except 吞掉，
            # 导致 DLT 永远不落料、replay 工具无从取回（§5.3/§12.3）。
            await producer.produce(
                self._settings.dlt_topic,
                key=str(offset),
                value=json.dumps(record, ensure_ascii=False),
            )
        except Exception as dlt_exc:  # noqa: BLE001
            # Kafka 未初始化（本地/单测）或不可用：记录后不阻断（数据错误已吞断，不会无限重试）
            logger.warning(
                "dlt_produce_failed", topic=self._settings.dlt_topic, error=str(dlt_exc)
            )


__all__ = ["BaseIngestConsumer"]
