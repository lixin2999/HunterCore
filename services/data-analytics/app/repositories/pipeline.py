"""数据管道健康仓库：TimescaleDB 只读计数 + Kafka 消费组滞后 / DLQ 水位。

契约 x-hunter-db-readonly：本服务仅以 hunter_analytics_ro 只读账号执行 SELECT；
Kafka 探测使用独立消费组 ID，不消费消息（仅取水位/提交位点，auto.commit 关闭）。
"""
from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol
from urllib.parse import quote

import asyncpg
from confluent_kafka import OFFSET_INVALID, Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from redis.asyncio import Redis

try:  # confluent-kafka 2.x：公开名在部分小版本中为私有别名（_ConsumerGroupTopicPartitions）
    from confluent_kafka.admin import ConsumerGroupTopicPartitions
except ImportError:  # pragma: no cover - 取决于安装的 confluent-kafka 版本
    from confluent_kafka.admin import (
        _ConsumerGroupTopicPartitions as ConsumerGroupTopicPartitions,
    )

from hunter_common import redis_keys
from hunter_common.config import HunterBaseConfig
from hunter_common.logging import get_logger

logger = get_logger("app.repositories.pipeline")

#: 入库延迟诊断主因 → 可读文案（受控词表与写端
#: ``flink-jobs/hunter_flink/ingest_latency_job.COUNTER_KEYS`` / ``diagnostic_reason`` 同源，
#: 新增枚举值必须两边同步；本表只负责“把码翻成人话”，不得在此重新判定主因）
LATENCY_REASON_LABELS: dict[str, str] = {
    "no_input": "作业未从 telemetry_raw 读到任何消息",
    "unparsable_message": "telemetry_raw 载荷不可解析为 JSON",
    "no_vehicle_timestamp": "消息缺车端 timestamp（无法计算延迟）",
    "no_kafka_record_timestamp": "Kafka 记录无时间戳（无法取到达时间）",
    "negative_latency": "车端时钟超前于 broker（负延迟一律丢弃，不伪造）",
    "non_finite_latency": "延迟样本非有限值（丢弃）",
    "mixed_discards": "延迟样本因多种原因丢弃",
    "window_not_closed": "窗口尚未闭合（首个 P95 待冲刷）",
}


@dataclass(frozen=True)
class IngestLatencyReading:
    """入库延迟读数（值 + 诊断）；两者可同时为空（键不存在 = 作业从未产出）。"""

    p95_ms: float | None = None
    reason: str | None = None
    #: 键是否存在（区分「没这个键」与「有键但 p95 为 null」两种看板同样展示 `-` 的情况）
    key_present: bool = False


class PipelineSource(Protocol):
    """管道健康来源协议（服务层依赖倒置；实现侧失败返回 None 由服务层降级）。"""

    async def telemetry_points(self, start_ts: float, end_ts: float) -> int | None:
        """窗口内遥测样本数（失败返回 None）。"""
        ...

    async def algorithm_averages(self, start_ts: float, end_ts: float) -> dict[str, float] | None:
        """窗口内算法指标均值（失败返回 None）。"""
        ...

    async def consumer_lag(self) -> tuple[dict[str, int], list[str]] | None:
        """按消费组的消息滞后（失败返回 None）。

        成功时返回 ``(group → lag 映射, 未判定组清单)``：**未进映射的组一律不可读成「零积压」**
        （组不存在 / 从未提交位点 / 水位不可取，见 :meth:`KafkaLagProbe._lag_sync`）。
        """
        ...

    async def dlq_depth(self) -> dict[str, int] | None:
        """DLQ 消息堆积（失败返回 None）。"""
        ...

    async def ingest_latency(self) -> IngestLatencyReading:
        """遥测入库延迟 P95 与其诊断（Flink data_quality_monitor → Redis 指标键）。

        缺失/不可用时 `p95_ms` 为 None；`reason` 只在**键存在但无有效样本**时非空（说清
        「为什么没数」）。作业从未产出（键不存在/过期）与「样本全被丢弃」是两回事，不得
        在展示层混为同一个 `-`（V1.19.21）。
        """
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
        """窗口内遥测样本数（时间界内 COUNT，禁止全表精确 COUNT 以保护 P95）。

        ``time`` 为 TIMESTAMPTZ（data_collector.vehicle_telemetry），asyncpg 只接受
        ``datetime`` 绑定（传 float epoch 会抛类型错→整块降级），故服务层先转 UTC datetime。
        """
        try:
            pool = await self._get_pool()
            async with pool.acquire() as conn:
                value = await conn.fetchval(
                    "SELECT count(*) FROM data_collector.vehicle_telemetry WHERE time >= $1 AND time < $2",
                    datetime.fromtimestamp(start_ts, tz=UTC),
                    datetime.fromtimestamp(end_ts, tz=UTC),
                )
        except Exception:
            logger.exception("telemetry_points_query_failed")
            return None
        return int(value or 0)

    async def algorithm_averages(self, start_ts: float, end_ts: float) -> dict[str, float] | None:
        """窗口内算法指标均值（{module}.{metric_name} → avg）。

        契约数据源为 ``data_analytics.algorithm_metrics``（由 Flink ``algorithm_performance_monitor``
        作业写入）；该作业尚未实现→表恒空，看板 algorithm 块全为 ``-``。故先查 algorithm_metrics，
        **为空时回退**到从 ``data_collector.vehicle_telemetry`` 原始宽列即时聚合（同窗口列均值，
        口径一致）；将来 Flink 作业产出后自动优先用聚合表。任一查询失败返回 None（上层按降级处理）；
        asyncpg 只接受 datetime 绑定（同 telemetry_points：float→datetime）。
        """
        metrics = await self._algorithm_averages_from_metrics(start_ts, end_ts)
        if metrics is None:
            return None
        if metrics:
            return metrics
        return await self._algorithm_averages_from_telemetry(start_ts, end_ts)

    async def _algorithm_averages_from_metrics(self, start_ts: float, end_ts: float) -> dict[str, float] | None:
        """契约源：data_analytics.algorithm_metrics 长表按 (module, metric_name) 求均值（失败 None）。"""
        try:
            pool = await self._get_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT module, metric_name, avg(metric_value) AS avg_value "
                    "FROM data_analytics.algorithm_metrics WHERE time >= $1 AND time < $2 "
                    "GROUP BY module, metric_name",
                    datetime.fromtimestamp(start_ts, tz=UTC),
                    datetime.fromtimestamp(end_ts, tz=UTC),
                )
        except Exception:
            logger.exception("algorithm_averages_query_failed")
            return None
        return {f"{row['module']}.{row['metric_name']}": float(row["avg_value"]) for row in rows}

    async def _algorithm_averages_from_telemetry(self, start_ts: float, end_ts: float) -> dict[str, float] | None:
        """回退源：data_collector.vehicle_telemetry 原始宽列即时聚合（algorithm_metrics 未产数时）。

        列别名以 ``{module}__{metric_name}`` 命名，拆为 ``module.metric_name`` 键（与看板
        ``_CORE_ALGORITHM_METRIC_KEYS`` 一致）；Postgres ``avg`` 忽略 NULL，全空列不入结果。
        """
        try:
            pool = await self._get_pool()
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT avg(fps) AS perception__fps, "
                    "avg(latency_ms) AS perception__latency_ms, "
                    "avg(detected_objects) AS perception__detected_objects, "
                    "avg(planning_latency_ms) AS planning__planning_latency_ms, "
                    "avg(control_latency_ms) AS control__control_latency_ms, "
                    "avg(velocity_error) AS control__velocity_error, "
                    "avg(steer_error) AS control__steer_error "
                    "FROM data_collector.vehicle_telemetry WHERE time >= $1 AND time < $2",
                    datetime.fromtimestamp(start_ts, tz=UTC),
                    datetime.fromtimestamp(end_ts, tz=UTC),
                )
        except Exception:
            logger.exception("algorithm_averages_telemetry_fallback_failed")
            return None
        result: dict[str, float] = {}
        for alias, value in (row or {}).items():
            if value is None:
                continue
            module, _, metric = alias.partition("__")
            result[f"{module}.{metric}"] = float(value)
        return result

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def ping(self) -> bool:
        """只读账号连通性探针（``/readyz`` 专用；失败返回 False 不抛出）。

        审查 Y10：就绪探针必须复用只读账号（``hunter_analytics_ro``），
        禁止为本服务初始化全权限 ``DatabaseSessionManager``（最小权限原则）。
        """
        try:
            pool = await self._get_pool()
            async with pool.acquire() as conn:
                return bool(await conn.fetchval("SELECT 1"))
        except Exception:  # noqa: BLE001 - 探测失败即未就绪（禁止向探针路径抛出）
            logger.warning("analytics_readonly_ping_failed")
            return False


class KafkaLagProbe:
    """Kafka 消费组滞后 / DLQ 水位探测（confluent-kafka 同步 API → to_thread 执行）。"""

    def __init__(self, settings: HunterBaseConfig) -> None:
        self._groups = settings.kafka_consumer_group_list
        self._dlq_topic = settings.dlq_topic
        self._timeout = max(settings.dependency_timeout_seconds, 1.0)
        # 探测专用消费组；不订阅任何主题，仅取水位/提交位点
        self._conf: dict[str, Any] = {
            "bootstrap.servers": ",".join(settings.kafka_bootstrap_servers_list),
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

    def _group_inventory(self, admin: AdminClient) -> set[str] | None:
        """集群侧实际存在的消费组清单（只用于给「未判定」区分成因，不参与滞后计算）。

        列举失败（group 级 ACL、超时、confluent-kafka 小版本 API 差异）返回 ``None``，
        语义是**「无能力断言存在性」**而非「组都不存在」：调用方仍须把空位点组记为
        未判定，绝不能反过来据此写 0（宁可未判定，不可假绿）。

        confluent-kafka 2.15 实测：future 结果是 ``ListConsumerGroupsResult`` 对象
        （``.valid`` / ``.errors``）而**不是**裸 list；条目名称字段是 ``group_id``。两处形状
        差异都会默默让集合变空（进而把所有组错标为 group_not_found），故此处兼容取值 +
        部分失败时直接拒答。
        """
        try:
            listing = admin.list_consumer_groups().result(timeout=self._timeout)
        except Exception:  # noqa: BLE001 - 辅助信息，失败不阻断滞后探测（成因退化为 no_committed_offsets）
            logger.warning("consumer_group_inventory_failed")
            return None
        if getattr(listing, "errors", None):  # 清单不完整时不能拿“没列出”当“不存在”
            logger.warning("consumer_group_inventory_partial", errors=len(listing.errors))
            return None
        entries = getattr(listing, "valid", listing)
        inventory: set[str] = set()
        for item in entries or []:
            name = getattr(item, "group_id", None) or getattr(item, "group", None)
            if name:
                inventory.add(str(name))
        return inventory

    def _lag_sync(self) -> tuple[dict[str, int], list[str]]:
        """同步读取各消费组提交位点与末端水位差（仅阻塞工作线程，不阻塞事件循环）。

        confluent-kafka（2.16 实测）的 ``list_consumer_group_offsets`` 单次仅支持
        **一个**消费组，传多组会即时抛 ``ValueError: Currently we support listing offsets
        for a single consumer group only``，故此处逐组各调一次（单组异常不阻塞其余组）。

        ℞ V1.19.14（第七类假绿）：**空分区列表不等于零积压**。Kafka 对「组不存在」与
        「组存在但从未提交位点」都返回**空分区列表且不报错**（``list_consumer_group_offsets``
        对拼错的组名同样不抛错），旧实现遂逐组 ``result[group] = 0``，看板永远显示「各组零延迟」
        （V1.19.11 登记为待办③，现场兑现于 V1.19.13 实录：四个组 ``--describe`` 全部
        ``does not exist``，看板却回四个 ``0``）。现把「无位点」「水位全取不到」「位点读取失败」
        三类一律归入第二个返回值（未判定组清单），不再进入 lag 映射。
        """
        admin = AdminClient(self._conf)
        consumer = Consumer(self._conf)
        result: dict[str, int] = {}
        unknown: list[str] = []
        try:
            inventory = self._group_inventory(admin)
            for group in self._groups:
                try:
                    futmap = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group)])
                    offsets = next(iter(futmap.values())).result(timeout=self._timeout)
                except Exception:  # 单组位点失败不阻塞其余消费组探测（带异常上下文便于定位）
                    logger.exception("consumer_group_offset_fetch_failed", group=group)
                    unknown.append(group)
                    continue
                partitions = list(getattr(offsets, "partitions", None) or [])
                if not partitions:
                    logger.info(
                        "consumer_group_lag_undetermined",
                        group=group,
                        cause="group_not_found" if inventory is not None and group not in inventory
                        else "no_committed_offsets",
                    )
                    unknown.append(group)
                    continue
                total_lag = 0
                measured = 0
                for tp in partitions:
                    committed = tp.offset
                    try:
                        low, high = consumer.get_watermark_offsets(
                            TopicPartition(tp.topic, tp.partition), timeout=self._timeout, cached=False
                        )
                    except Exception:  # noqa: BLE001 - 单分区水位失败跳过，保证整体 lag 可得
                        logger.debug("watermark_fetch_failed", topic=tp.topic, partition=tp.partition)
                        continue
                    measured += 1
                    # OFFSET_INVALID（无提交位点）时以 low 为基准
                    base = low if committed in (None, OFFSET_INVALID) else committed
                    total_lag += max(0, high - base)
                if not measured:  # 有位点却一个水位都取不到：与「零积压」无关，同样不得写 0
                    logger.info("consumer_group_lag_undetermined", group=group, cause="watermark_unavailable")
                    unknown.append(group)
                    continue
                result[group] = total_lag
            return result, unknown
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

    async def consumer_lag(self) -> tuple[dict[str, int], list[str]] | None:
        try:
            return await asyncio.wait_for(asyncio.to_thread(self._lag_sync), timeout=self._timeout + 1.0)
        except Exception:  # 探测失败降级为 None（上层按不可用处理），不向请求路径抛出；带堆栈便于定位真因
            logger.exception("kafka_lag_probe_failed")
            return None

    async def dlq_depth(self) -> dict[str, int] | None:
        try:
            return await asyncio.wait_for(asyncio.to_thread(self._dlq_sync), timeout=self._timeout + 1.0)
        except Exception:  # 探测失败降级为 None（上层按不可用处理），不向请求路径抛出；带堆栈便于定位真因
            logger.exception("kafka_dlq_probe_failed")
            return None


class IngestLatencyRedisReader:
    """入库延迟指标只读器（读 Flink ``data_quality_monitor`` 写入的 Redis 指标键）。

    契约：``contracts/database/redis-keys.yaml`` 键 ``analytics:ingest_latency``（pending #10
    结案：Kafka 时间戳差 → Flink 流式统计 → Redis 指标键）；本服务**只读**（写方 = 本服务
    的 Flink 实时作业）。Redis 不可用/载荷不可解析 → 降级（看板该字段为 null，
    绝不展示陈旧值：作业停摆时键按 TTL 自然过期）。
    """

    #: 指标键（契约 redis-keys.yaml 登记的模式；经共享常量 :data:`hunter_common.redis_keys.ANALYTICS_INGEST_LATENCY`
    #: 引用，与 Flink ``ingest_latency_job.REDIS_KEY`` 同源，杜绝跨端拼写漂移）
    KEY = redis_keys.ANALYTICS_INGEST_LATENCY

    def __init__(self, redis: Redis | None) -> None:
        self._redis = redis

    async def read(self) -> IngestLatencyReading:
        """读指标键（V1.19.21）：一次 GET 同时取回 P95 与诊断载荷里的 ``reason``。

        四种读数必可区分（看板/巡检据此分支，不再把四种成因读成同一个 `-`）：
        ① 键存在且 ``p95_ms`` 为数值 → 真值；② 键存在但 ``p95_ms`` 为 null → 带 ``reason``
        的丢弃诊断（写端已计数）；③ 键不存在/过期（``key_present=False``）→ 作业从未产出；
        ④ Redis 不可用/载荷不可解析 → 降级（与③同样不给原因，但日志留痕）。
        """
        if self._redis is None:
            return IngestLatencyReading()
        try:
            raw = await self._redis.get(self.KEY)
        except Exception:  # noqa: BLE001 - 观测性读取，失败降级（不向请求路径抛出）
            logger.warning("ingest_latency_read_failed")
            return IngestLatencyReading()
        if raw is None:
            return IngestLatencyReading()
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            metric = json.loads(raw)
        except (json.JSONDecodeError, TypeError, ValueError):
            logger.warning("ingest_latency_value_unparsable")
            return IngestLatencyReading(key_present=True)
        if not isinstance(metric, dict):
            return IngestLatencyReading(key_present=True)
        value = metric.get("p95_ms")
        p95 = (
            float(value)
            if not isinstance(value, bool) and isinstance(value, (int, float))
            else None
        )
        return IngestLatencyReading(
            p95_ms=p95,
            reason=None if p95 is not None else _diagnostic_text(metric),
            key_present=True,
        )

    async def p95_ms(self) -> float | None:
        """只要 P95 数值的便捷入口（诊断分支请走 :meth:`read`）。"""
        return (await self.read()).p95_ms


def _diagnostic_text(metric: dict[str, Any]) -> str | None:
    """把诊断载荷里的 ``reason``/``dropped`` 翻成一句可定位的话（无 reason → 说写端版本旧）。

    主因判定在写端（``diagnostic_reason``）——本函数**不重新推断**，只翻译与附计数。
    """
    reason = metric.get("reason")
    if not isinstance(reason, str) or not reason:
        return "指标键已写入但无 p95_ms/reason（Flink 写端未升级至 V1.19.21，不展示旧值）"
    label = LATENCY_REASON_LABELS.get(reason, reason)
    bits: list[str] = []
    seen = metric.get("seen")
    if isinstance(seen, int) and not isinstance(seen, bool):
        bits.append(f"seen={seen}")
    dropped = metric.get("dropped")
    if isinstance(dropped, dict):
        bits.extend(
            f"{key}={value}"
            for key, value in sorted(dropped.items())
            if isinstance(value, int) and not isinstance(value, bool) and value > 0
        )
    return f"{label}（{'; '.join(bits)}）" if bits else label


class PipelineRepository:
    """管道健康聚合门面（DB + Kafka + Redis 独立降级：失败返回 None，由服务层标记降级）。"""

    def __init__(self, settings: HunterBaseConfig, *, redis: Redis | None = None) -> None:
        self._db = MetricsReadOnlyRepository(settings)
        self._kafka = KafkaLagProbe(settings)
        self._latency = IngestLatencyRedisReader(redis)

    async def telemetry_points(self, start_ts: float, end_ts: float) -> int | None:
        return await self._db.telemetry_points(start_ts, end_ts)

    async def algorithm_averages(self, start_ts: float, end_ts: float) -> dict[str, float] | None:
        return await self._db.algorithm_averages(start_ts, end_ts)

    async def consumer_lag(self) -> tuple[dict[str, int], list[str]] | None:
        return await self._kafka.consumer_lag()

    async def dlq_depth(self) -> dict[str, int] | None:
        return await self._kafka.dlq_depth()

    async def ingest_latency(self) -> IngestLatencyReading:
        return await self._latency.read()

    async def close(self) -> None:
        await self._db.close()