"""数据管道健康仓库：TimescaleDB 只读计数 + Kafka 消费组滞后 / DLQ 水位。

契约 x-hunter-db-readonly：本服务仅以 hunter_analytics_ro 只读账号执行 SELECT；
Kafka 探测使用独立消费组 ID，不消费消息（仅取水位/提交位点，auto.commit 关闭）。
"""
from __future__ import annotations

import asyncio
from typing import Any, Protocol
from urllib.parse import quote

import asyncpg
from confluent_kafka import OFFSET_INVALID, Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

try:  # confluent-kafka 2.x：公开名在部分小版本中为私有别名（_ConsumerGroupTopicPartitions）
    from confluent_kafka.admin import ConsumerGroupTopicPartitions
except ImportError:  # pragma: no cover - 取决于安装的 confluent-kafka 版本
    from confluent_kafka.admin import (
        _ConsumerGroupTopicPartitions as ConsumerGroupTopicPartitions,
    )

from hunter_common.config import HunterBaseConfig
from hunter_common.logging import get_logger

logger = get_logger("app.repositories.pipeline")


class PipelineSource(Protocol):
    """管道健康来源协议（服务层依赖倒置；实现侧失败返回 None 由服务层降级）。"""

    async def telemetry_points(self, start_ts: float, end_ts: float) -> int | None:
        """窗口内遥测样本数（失败返回 None）。"""
        ...

    async def algorithm_averages(self, start_ts: float, end_ts: float) -> dict[str, float] | None:
        """窗口内算法指标均值（失败返回 None）。"""
        ...

    async def consumer_lag(self) -> dict[str, int] | None:
        """按消费组的消息滞后（失败返回 None）。"""
        ...

    async def dlq_depth(self) -> dict[str, int] | None:
        """DLQ 消息堆积（失败返回 None）。"""
        ...


class MetricsReadOnlyRepository:
    """TimescaleDB 只读聚合（data_collector.vehicle_telemetry / data_analytics.algorithm_metrics）。"""

    def __init__(self, settings: HunterBaseConfig) -> None:
        self._dsn = (
            f"postgresql://{quote(settings.analytics_ro_db_user)}:{quote(settings.analytics_ro_db_password)}"
            f"@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}"
        )
        self._pool: asyncpg.Pool | None = None
        self._lock = asyncio.Lock()

    async def _get_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            async with self._lock:
                if self._pool is None:
                    # 只读小连接池（1~4），避免抢占业务库资源
                    self._pool = await asyncpg.create_pool(dsn=self._dsn, min_size=1, max_size=4, command_timeout=5)
        return self._pool

    async def telemetry_points(self, start_ts: float, end_ts: float) -> int | None:
        """窗口内遥测样本数（时间界内 COUNT，禁止全表精确 COUNT 以保护 P95）。"""
        try:
            pool = await self._get_pool()
            async with pool.acquire() as conn:
                value = await conn.fetchval(
                    "SELECT count(*) FROM data_collector.vehicle_telemetry WHERE time >= $1 AND time < $2",
                    start_ts,
                    end_ts,
                )
        except Exception:
            logger.exception("telemetry_points_query_failed")
            return None
        return int(value or 0)

    async def algorithm_averages(self, start_ts: float, end_ts: float) -> dict[str, float] | None:
        """窗口内算法指标均值（{module}.{metric_name} → avg）。"""
        try:
            pool = await self._get_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT module, metric_name, avg(metric_value) AS avg_value "
                    "FROM data_analytics.algorithm_metrics WHERE time >= $1 AND time < $2 "
                    "GROUP BY module, metric_name",
                    start_ts,
                    end_ts,
                )
        except Exception:
            logger.exception("algorithm_averages_query_failed")
            return None
        return {f"{row['module']}.{row['metric_name']}": float(row["avg_value"]) for row in rows}

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None


class KafkaLagProbe:
    """Kafka 消费组滞后 / DLQ 水位探测（confluent-kafka 同步 API → to_thread 执行）。"""

    def __init__(self, settings: HunterBaseConfig) -> None:
        self._groups = settings.kafka_consumer_group_list
        self._dlq_topic = settings.dlq_topic
        self._timeout = max(settings.dependency_timeout_seconds, 1.0)
        # 探测专用消费组；不订阅任何主题，仅取水位/提交位点
        self._conf: dict[str, Any] = {
            "bootstrap.servers": ",".join(settings.kafka_bootstrap_servers),
            "security.protocol": settings.kafka_security_protocol,
            "group.id": "hunter-analytics-metrics-probe",
            "enable.auto.commit": False,
        }
        if settings.kafka_security_protocol in {"SASL_SSL", "SASL_PLAINTEXT"}:
            self._conf.update(
                {
                    "sasl.mechanism": settings.kafka_sasl_mechanism,
                    "sasl.username": settings.kafka_sasl_username,
                    "sasl.password": settings.kafka_sasl_password,
                }
            )
        if settings.kafka_security_protocol.endswith("SSL"):
            self._conf.update(
                {
                    "ssl.ca.location": settings.kafka_ssl_ca_location,
                    "ssl.certificate.location": settings.kafka_ssl_cert_location,
                    "ssl.key.location": settings.kafka_ssl_key_location,
                }
            )

    def _lag_sync(self) -> dict[str, int]:
        """同步读取各消费组提交位点与末端水位差（仅阻塞工作线程，不阻塞事件循环）。"""
        admin = AdminClient(self._conf)
        consumer = Consumer(self._conf)
        result: dict[str, int] = {}
        try:
            futures = admin.list_consumer_group_offsets(
                [ConsumerGroupTopicPartitions(group) for group in self._groups]
            )
            for group, future in futures.items():
                try:
                    offsets = future.result(timeout=self._timeout)
                except Exception:  # noqa: BLE001 - 单组位点失败不阻塞其余消费组探测（已记 warning）
                    logger.warning("consumer_group_offset_fetch_failed", group=group)
                    continue
                total_lag = 0
                for tp in getattr(offsets, "partitions", []) or []:
                    committed = tp.offset
                    try:
                        low, high = consumer.get_watermark_offsets(
                            TopicPartition(tp.topic, tp.partition), timeout=self._timeout, cached=False
                        )
                    except Exception:  # noqa: BLE001 - 单分区水位失败跳过，保证整体 lag 可得
                        logger.debug("watermark_fetch_failed", topic=tp.topic, partition=tp.partition)
                        continue
                    # OFFSET_INVALID（无提交位点）时以 low 为基准
                    base = low if committed in (None, OFFSET_INVALID) else committed
                    total_lag += max(0, high - base)
                result[group] = total_lag
            return result
        finally:
            consumer.close()

    def _dlq_sync(self) -> dict[str, int]:
        """同步读取 DLQ 主题消息堆积（各分区 high - low 之和）。"""
        admin = AdminClient(self._conf)
        consumer = Consumer(self._conf)
        try:
            metadata = admin.list_topics(topic=self._dlq_topic, timeout=self._timeout)
            topic_meta = metadata.topics.get(self._dlq_topic)
            if topic_meta is None or topic_meta.error is not None:
                return {}
            total = 0
            for partition_id in topic_meta.partitions:
                try:
                    low, high = consumer.get_watermark_offsets(
                        TopicPartition(self._dlq_topic, partition_id), timeout=self._timeout, cached=False
                    )
                except Exception:  # noqa: BLE001 - 单分区水位失败跳过，保证 DLQ 深度整体可得
                    logger.debug("dlq_watermark_fetch_failed", partition=partition_id)
                    continue
                total += max(0, high - low)
            return {self._dlq_topic: total}
        finally:
            consumer.close()

    async def consumer_lag(self) -> dict[str, int] | None:
        try:
            return await asyncio.wait_for(asyncio.to_thread(self._lag_sync), timeout=self._timeout + 1.0)
        except Exception:  # noqa: BLE001 - 探测失败降级为 None（上层按不可用处理），不向请求路径抛出
            logger.warning("kafka_lag_probe_failed")
            return None

    async def dlq_depth(self) -> dict[str, int] | None:
        try:
            return await asyncio.wait_for(asyncio.to_thread(self._dlq_sync), timeout=self._timeout + 1.0)
        except Exception:  # noqa: BLE001 - 探测失败降级为 None（上层按不可用处理），不向请求路径抛出
            logger.warning("kafka_dlq_probe_failed")
            return None


class PipelineRepository:
    """管道健康聚合门面（DB + Kafka 独立降级：失败返回 None，由服务层标记降级）。"""

    def __init__(self, settings: HunterBaseConfig) -> None:
        self._db = MetricsReadOnlyRepository(settings)
        self._kafka = KafkaLagProbe(settings)

    async def telemetry_points(self, start_ts: float, end_ts: float) -> int | None:
        return await self._db.telemetry_points(start_ts, end_ts)

    async def algorithm_averages(self, start_ts: float, end_ts: float) -> dict[str, float] | None:
        return await self._db.algorithm_averages(start_ts, end_ts)

    async def consumer_lag(self) -> dict[str, int] | None:
        return await self._kafka.consumer_lag()

    async def dlq_depth(self) -> dict[str, int] | None:
        return await self._kafka.dlq_depth()

    async def close(self) -> None:
        await self._db.close()