"""Kafka 链路 Prometheus 指标（架构原则 6：可观测性）。

命名规范：统一前缀 ``hunter_kafka_``，标签必含 ``service``。
标签基数受契约约束——平台内部 Topic 6 个 + 车端 Topic 按类型归并 9 个、消费者组 12 个、
分区数 ≤ 12（``contracts/kafka/topics.yaml``），**禁止**引入 ``vehicle_id`` 等高基数标签。

指标注册到 ``hunter_common.metrics.REGISTRY``，随各服务 ``/metrics`` 端点一并暴露
（Prometheus 文本格式，属统一 JSON 响应体的契约例外）。
"""
from __future__ import annotations

from typing import Final

from prometheus_client import Counter, Gauge, Histogram

from hunter_common.metrics import REGISTRY

# 生产投递时延桶（秒）：覆盖网络往返与 broker 确认；重试会整体抬高观测值
_PRODUCE_LATENCY_BUCKETS: Final[tuple[float, ...]] = (
    0.001,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
)

# 单条消息处理时延桶（秒）：对齐实时告警触发 ≤ 2s（性能指标）
_PROCESS_LATENCY_BUCKETS: Final[tuple[float, ...]] = (
    0.001,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.0,
)

KAFKA_PRODUCED_TOTAL = Counter(
    "hunter_kafka_messages_produced_total",
    "生产消息数（status=delivered 投递成功 / buffered 转本地磁盘缓冲 / failed 投递失败）",
    labelnames=("service", "topic", "status"),
    registry=REGISTRY,
)

KAFKA_PRODUCE_DURATION = Histogram(
    "hunter_kafka_produce_duration_seconds",
    "生产投递时延（秒，含重试；delivery 回调确认）",
    labelnames=("service", "topic"),
    buckets=_PRODUCE_LATENCY_BUCKETS,
    registry=REGISTRY,
)

KAFKA_PRODUCE_RETRIES_TOTAL = Counter(
    "hunter_kafka_produce_retries_total",
    "生产重试次数（可重试 Kafka 错误 / 本地队列背压）",
    labelnames=("service", "topic"),
    registry=REGISTRY,
)

KAFKA_LOCAL_BUFFER_MESSAGES = Gauge(
    "hunter_kafka_local_buffer_messages",
    "本地磁盘缓冲待重投消息数（网络中断场景，容量上限见契约 producer_defaults）",
    labelnames=("service",),
    registry=REGISTRY,
)

KAFKA_LOCAL_BUFFER_BYTES = Gauge(
    "hunter_kafka_local_buffer_bytes",
    "本地磁盘缓冲占用字节数（上限 1073741824 = 1GB，契约 producer_defaults.local_disk_buffer_bytes）",
    labelnames=("service",),
    registry=REGISTRY,
)

KAFKA_LOCAL_BUFFER_DROPPED_TOTAL = Counter(
    "hunter_kafka_local_buffer_dropped_total",
    "本地磁盘缓冲超限后丢弃的最旧消息数（丢弃顺序：整段最旧优先）",
    labelnames=("service",),
    registry=REGISTRY,
)

KAFKA_LOCAL_BUFFER_REPLAYED_TOTAL = Counter(
    "hunter_kafka_local_buffer_replayed_total",
    "本地磁盘缓冲重投成功消息数",
    labelnames=("service",),
    registry=REGISTRY,
)

KAFKA_CONSUMED_TOTAL = Counter(
    "hunter_kafka_messages_consumed_total",
    "消费消息数（status=processed / skipped_duplicate / schema_invalid / handler_failed / decode_failed）",
    labelnames=("service", "group", "topic", "status"),
    registry=REGISTRY,
)

KAFKA_PROCESS_DURATION = Histogram(
    "hunter_kafka_message_process_duration_seconds",
    "单条消息 handler 处理时延（秒，每次尝试；重试分别计时）",
    labelnames=("service", "group", "topic"),
    buckets=_PROCESS_LATENCY_BUCKETS,
    registry=REGISTRY,
)

KAFKA_CONSUMER_LAG = Gauge(
    "hunter_kafka_consumer_lag",
    "消费积压（分区高水位 - 当前消费位置，按分区；rebalance 后由新属主刷新）",
    labelnames=("service", "group", "topic", "partition"),
    registry=REGISTRY,
)

KAFKA_DLQ_TOTAL = Counter(
    "hunter_kafka_dlq_messages_total",
    "转入死信队列 {topic}.dlq 的消息数（reason=schema_invalid / handler_error / decode_failed）",
    labelnames=("service", "group", "topic", "reason"),
    registry=REGISTRY,
)


def record_produced(service: str, topic: str, status: str, duration_s: float) -> None:
    """记录一次生产结果（delivered / buffered / failed）。buffered/failed 也计入时延，便于定位链路劣化。"""
    KAFKA_PRODUCED_TOTAL.labels(service, topic, status).inc()
    KAFKA_PRODUCE_DURATION.labels(service, topic).observe(duration_s)


def record_produced_replay(service: str, topic: str) -> None:
    """记录一条由磁盘缓冲重投成功的消息（不重复观测生产时延：链路恢复期的时延不具代表性）。"""
    KAFKA_PRODUCED_TOTAL.labels(service, topic, "delivered").inc()


def record_produce_retry(service: str, topic: str) -> None:
    """记录一次生产重试（可重试错误或本地队列背压）。"""
    KAFKA_PRODUCE_RETRIES_TOTAL.labels(service, topic).inc()


def record_buffer_stats(service: str, message_count: int, bytes_size: int) -> None:
    """刷新本地磁盘缓冲水位（append / replay / 淘汰后调用）。"""
    KAFKA_LOCAL_BUFFER_MESSAGES.labels(service).set(message_count)
    KAFKA_LOCAL_BUFFER_BYTES.labels(service).set(bytes_size)


def record_buffer_dropped(service: str, dropped: int) -> None:
    """记录因超限丢弃的最旧消息数。"""
    if dropped > 0:
        KAFKA_LOCAL_BUFFER_DROPPED_TOTAL.labels(service).inc(dropped)


def record_buffer_replayed(service: str, replayed: int) -> None:
    """记录重投成功消息数。"""
    if replayed > 0:
        KAFKA_LOCAL_BUFFER_REPLAYED_TOTAL.labels(service).inc(replayed)


def record_consumed(service: str, group: str, topic: str, status: str, duration_s: float) -> None:
    """记录一条消费结果（status 见 KAFKA_CONSUMED_TOTAL 说明）。"""
    KAFKA_CONSUMED_TOTAL.labels(service, group, topic, status).inc()
    KAFKA_PROCESS_DURATION.labels(service, group, topic).observe(duration_s)


def record_dlq(service: str, group: str, topic: str, reason: str) -> None:
    """记录一条转入死信队列的消息（reason ∈ schema_invalid / handler_error / decode_failed）。"""
    KAFKA_DLQ_TOTAL.labels(service, group, topic, reason).inc()


def set_consumer_lag(service: str, group: str, topic: str, partition: int, lag: int) -> None:
    """设置分区消费积压（lag < 0 视为 0：位置可能领先于缓存高水位）。"""
    KAFKA_CONSUMER_LAG.labels(service, group, topic, str(partition)).set(max(lag, 0))
