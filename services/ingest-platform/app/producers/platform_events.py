"""平台内部事件生产者（§7.5 加工产物事件广播）。

发布以下 Topic：
- platform.derived.ready.v1  (bag_id key)  — 派生产物就绪，触发下游注册入库
- platform.bag.qc_failed.v1  (bag_id key)  — QC 分数低于阈值告警
- platform.clip.created.v1   (bag_id key)  — 新 clip 创建，检索索引更新
- platform.dataset.ready.v1  (dataset_id key) — 训练数据集导出完成
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from hunter_common.kafka.producer import KafkaProducerManager
from hunter_common.logging import get_logger

logger = get_logger("app.producers.platform_events")

# Topic 常量（§7.5）
_TOPIC_DERIVED_READY = "platform.derived.ready.v1"
_TOPIC_QC_FAILED = "platform.bag.qc_failed.v1"
_TOPIC_CLIP_CREATED = "platform.clip.created.v1"
_TOPIC_DATASET_READY = "platform.dataset.ready.v1"


def _envelope(event_type: str, bag_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    """构造内部事件 envelope（§7.5 origin: platform，复用 §5.2 幂等机制）。"""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "bag_id": bag_id,
        "vehicle_id": payload.get("vehicle_id", ""),
        "seq": None,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "origin": "platform",
        "payload": payload,
    }


class PlatformEventProducer:
    """平台加工事件生产者（无状态，由 lifespan 单例持有）。"""

    async def publish_derived_ready(
        self,
        bag_id: str,
        input_rev: str,
        *,
        kind: str,
        object_keys: list[str],
        count: int = 0,
    ) -> None:
        """§7.5 platform.derived.ready.v1：派生产物注册入库触发。"""
        msg = _envelope("platform.derived.ready.v1", bag_id, {
            "kind": kind,
            "input_rev": input_rev,
            "object_keys": object_keys,
            "count": count,
        })
        await self._produce(_TOPIC_DERIVED_READY, bag_id, msg)

    async def publish_qc_failed(
        self,
        bag_id: str,
        score: float,
        failing_checks: list[str],
    ) -> None:
        """§7.5 platform.bag.qc_failed.v1：低 QC 分数告警 + 可选下发 reparse。"""
        msg = _envelope("platform.bag.qc_failed.v1", bag_id, {
            "score": score,
            "failing_checks": failing_checks,
        })
        await self._produce(_TOPIC_QC_FAILED, bag_id, msg)

    async def publish_clip_created(
        self,
        bag_id: str,
        clip_id: int,
        tags: list[str],
        start_ns: int,
        end_ns: int,
    ) -> None:
        """§7.5 platform.clip.created.v1：检索索引更新触发。"""
        msg = _envelope("platform.clip.created.v1", bag_id, {
            "clip_id": clip_id,
            "tags": tags,
            "span": [start_ns, end_ns],
        })
        await self._produce(_TOPIC_CLIP_CREATED, bag_id, msg)

    async def publish_dataset_ready(
        self,
        dataset_id: str,
        version: str,
        manifest_key: str,
        clip_count: int,
    ) -> None:
        """§8.2 platform.dataset.ready.v1：训练数据集导出完成。"""
        msg = _envelope("platform.dataset.ready.v1", dataset_id, {
            "dataset_id": dataset_id,
            "version": version,
            "manifest_key": manifest_key,
            "clip_count": clip_count,
        })
        await self._produce(_TOPIC_DATASET_READY, dataset_id, msg)

    async def _produce(self, topic: str, key: str, value: dict[str, Any]) -> None:
        try:
            producer = KafkaProducerManager.instance()
            # 低层 produce 只接受 bytes/str：直传 dict 会被 confluent_kafka 拒绝（except 吞掉）。
            await producer.produce(topic, key=key, value=json.dumps(value, ensure_ascii=False))
            logger.debug("platform_event_published", topic=topic, key=key)
        except RuntimeError:
            # Kafka 未初始化（测试场景），静默跳过
            logger.debug("platform_event_skipped_no_kafka", topic=topic)
        except Exception as exc:
            logger.error("platform_event_publish_failed", topic=topic, error=str(exc))


__all__ = ["PlatformEventProducer"]
