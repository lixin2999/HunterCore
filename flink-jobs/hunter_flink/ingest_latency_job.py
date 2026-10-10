"""``data_quality_monitor`` 入库延迟统计（6.2.5「延迟」子项）实时作业核心（纯 Python）。

契约依据：``contracts/openapi/data-analytics.yaml`` → ``x-hunter-realtime-jobs
[ data_quality_monitor ]``（input=telemetry_raw，logic「延迟：车端 timestamp 与到达时间差（P95）」）
与 ``x-hunter-dashboard-contract.pipeline.ingest_latency``（pending #10 结案：采用
「Kafka 时间戳差估算」路线——Flink 流式统计 → Redis 指标键，看板 pipeline 块读该键）。

口径（固化）：
- 样本 = ``(Kafka 记录到达时间 arrival_time) - (车端事件时间 timestamp)``（秒差 → 毫秒）；
- 时钟超前导致的**负延迟样本丢弃**（``latency < 0`` 视为不可信，禁止伪造/取绝对值）；
- 非有限值丢弃；窗口内 ≥ 1 个有效样本才产出（缺失不伪造）；
- 车队级 P95（``percentile_cont`` 线性插值，与算法作业/看板只读聚合口径一致）。

落位：产出 ``{"p95_ms","count","window","window_end","metric"}`` 指标 → Redis 键
``analytics:ingest_latency``（契约 ``redis-keys.yaml`` 登记，TTL 兜底：作业停摆即过期，
看板降级为 null，绝不留陈旧值）。G-13「Flink 不直连业务库」——Redis 为指标缓存非系统事实库，
且看板 pipeline 块仅此一个实时估算指标，属契约批准路线（pending #10 结案）。

纪律：与 ``detection_job`` / ``algorithm_performance_job`` 同构——纯 Python 核心 + ``main()``
延迟 import pyflink；仓库单测无需 PyFlink 运行时。仅 import 无运行时的
``hunter_common.redis_keys``（纯字符串常量，redis-keys.yaml pending #7 落地的键构造单一事实来源），
不 import 任何服务 ``app.*`` 包或 ``hunter_common.database`` 仓储（写侧豁免边界不变）。
"""
from __future__ import annotations

import json
import math
import os
from collections.abc import Iterable, Mapping
from typing import Any

from hunter_common.redis_keys import ANALYTICS_INGEST_LATENCY

from hunter_flink.algorithm_performance_job import percentile_cont

#: Kafka broker（standalone/compose 单机形态；K8s 由环境变量下发）
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
#: 输入 Topic（原始遥测，与清洗流独立 offset 才能对比——契约 consumer-groups data-analytics-telemetry-raw）
INPUT_TOPIC = "telemetry_raw"
#: 消费组（契约 consumer-groups.yaml 固定）
CONSUMER_GROUP = "data-analytics-telemetry-raw"
#: 输出 Redis 指标键（契约 redis-keys.yaml 登记；与看板读端 IngestLatencyRedisReader.KEY
#: 同引用共享常量 hunter_common.redis_keys.ANALYTICS_INGEST_LATENCY，杜绝跨端拼写漂移；允许环境变量覆盖）
REDIS_KEY = os.environ.get("INGEST_LATENCY_REDIS_KEY", ANALYTICS_INGEST_LATENCY)
#: Redis 值 TTL（作业停摆时键过期 → 看板 ingest_latency_ms_p95 降级为 null，杜绝陈旧展示）
REDIS_TTL_SECONDS = int(os.environ.get("INGEST_LATENCY_TTL_SECONDS", "300"))

#: 毫秒时间戳判定阈值（Unix 秒在 5138 年前不会超过 1e11，与 data-collector 同口径）
_MILLISECOND_THRESHOLD = 1e11


def window_seconds_from_env(env: dict[str, str] | None = None) -> float:
    """入库延迟统计窗口（秒），默认 60（与算法指标窗口同量级；环境变量可覆盖，禁止放宽语义）。"""
    source = env if env is not None else os.environ
    raw = source.get("INGEST_LATENCY_WINDOW_SECONDS", "").strip()
    if not raw:
        return 60.0
    value = float(raw)
    if value <= 0:
        raise ValueError(f"INGEST_LATENCY_WINDOW_SECONDS 必须为正数，实际 {value}")
    return value


def _window_label(window_seconds: float) -> str:
    return f"{int(window_seconds)}s"


def _event_time(msg: Mapping[str, Any]) -> float | None:
    """取车端事件时间（顶层 timestamp，毫秒判定归一为秒）；非有限/缺失 → None。"""
    value = msg.get("timestamp") if isinstance(msg, Mapping) else None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    seconds = float(value)
    if not math.isfinite(seconds):
        return None
    if seconds > _MILLISECOND_THRESHOLD:
        seconds /= 1000.0
    return seconds


def latency_ms(event_time: float | None, arrival_time: float | None) -> float | None:
    """单样本入库延迟（毫秒）= 到达时间 - 事件时间；非有限或负值（时钟超前）→ None（丢弃）。"""
    if event_time is None or arrival_time is None:
        return None
    diff = (float(arrival_time) - float(event_time)) * 1000.0
    if not math.isfinite(diff) or diff < 0:
        return None
    return diff


def aggregate_latency(
    samples: Iterable[float],
    *,
    window_seconds: float,
    window_end: float,
) -> dict[str, Any] | None:
    """窗口内入库延迟样本 → 车队级 P95 指标；无有效样本返回 None（不伪造）。"""
    values = [float(v) for v in samples if v is not None]
    if not values:
        return None
    p95 = percentile_cont(values, 0.95)
    if p95 is None:
        return None
    return {
        "metric": "ingest_latency_ms_p95",
        "p95_ms": round(p95, 3),
        "count": len(values),
        "window": _window_label(window_seconds),
        "window_end": round(float(window_end), 3),
    }


def expand_latency(
    msg: Mapping[str, Any],
    *,
    arrival_time: float,
    window_seconds: float,
    buffers: dict[int, list[float]],
) -> list[dict[str, Any]]:
    """一条 telemetry_raw → 0..n 个「已完成窗口」的入库延迟指标（车队级、跨窗口边界冲刷）。

    ``buffers`` 为按窗口 ID（车队全局，非按车）维护的延迟样本状态后端；窗口以车端事件时间
    划分（tumbling，``window_id = event_time // window_seconds``）。仅当本样本进入更晚窗口时
    冲刷更早的已完成窗口（旧窗口不再接收数据 → 可安全产出 P95）。尾窗口由部署侧 Flink 状态兜底。
    """
    event_time = _event_time(msg)
    if event_time is None:
        return []
    sample = latency_ms(event_time, arrival_time)
    if sample is None:
        return []  # 时钟超前/异常样本：丢弃，不污染 P95
    window_id = int(event_time // window_seconds)
    buffers.setdefault(window_id, []).append(sample)
    flushed: list[dict[str, Any]] = []
    for wid in [w for w in buffers if w < window_id]:
        bucket = buffers.pop(wid)
        metric = aggregate_latency(
            bucket, window_seconds=window_seconds, window_end=(wid + 1) * window_seconds
        )
        if metric is not None:
            flushed.append(metric)
    return flushed


def encode_redis_value(metric: Mapping[str, Any]) -> str:
    """指标 → Redis 值（看板 :class:`IngestLatencyReader` 解析的 JSON 契约载荷）。"""
    return json.dumps(dict(metric), ensure_ascii=False)


def main() -> None:  # pragma: no cover - 需要 PyFlink + Redis 运行时，仓库内不可执行
    """PyFlink DataStream 作业提交入口（Flink 2.1）→ Redis 指标键。

    拓扑（契约 data_quality_monitor「延迟」子项，pending #10 结案路线）::

        Kafka telemetry_raw（group=data-analytics-telemetry-raw，携带 Kafka 记录到达时间）
          → keyBy(常量) → expand_latency 车队窗口缓冲（跨窗口冲刷）
          → aggregate P95 → Redis SET analytics:ingest_latency EX <ttl>

    ``arrival_time`` 取 Kafka 记录时间戳（broker LogAppendTime/接收时刻），车端 ``timestamp`` 取
    消息体；二者差即入库链路延迟。keyBy 常量以保证车队级 P95 精确（规模过大时可改近似分位聚合，
    须回改契约）。Redis 写失败由 Sink 重试；作业停摆时键按 TTL 自然过期（看板降级 null）。
    """
    import redis
    from pyflink.common import Types
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.connectors.kafka import (
        KafkaOffsetsInitializer,
        KafkaSource,
    )
    from pyflink.datastream.functions import FlatMapFunction, SinkFunction

    window_seconds = window_seconds_from_env()

    class LatencyFlatMap(FlatMapFunction):
        def __init__(self) -> None:
            self._buffers: dict[int, list[float]] = {}

        def flat_map(self, raw: tuple[str, float]) -> Iterable[str]:
            value, arrival_ms = raw
            msg = json.loads(value)
            metrics = expand_latency(
                msg,
                arrival_time=arrival_ms / 1000.0,
                window_seconds=window_seconds,
                buffers=self._buffers,
            )
            return [encode_redis_value(metric) for metric in metrics]

    class RedisSink(SinkFunction):
        def __init__(self) -> None:
            self._client: redis.Redis | None = None

        def invoke(self, value: str, context: Any) -> None:
            if self._client is None:
                self._client = redis.Redis.from_url(
                    os.environ.get("REDIS_URL", "redis://redis:6379/0"), decode_responses=True
                )
            self._client.set(REDIS_KEY, value, ex=REDIS_TTL_SECONDS)

    env = StreamExecutionEnvironment.get_execution_environment()
    # 记录到达时间来自 Kafka 时间戳：使用带时间戳的解码（value + Kafka record timestamp(ms)）
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(INPUT_TOPIC)
        .set_group_id(CONSUMER_GROUP)
        .set_starting_offsets(KafkaOffsetsInitializer.group_offsets())
        .value_only_decoder(Types.PRIMITIVE_STRING(Types.InformationType()))
        .build()
    )
    env.add_source(source) \
        .assign_timestamps_and_watermarks(_arrival_watermark_strategy()) \
        .key_by(lambda _raw: "fleet", key_type=Types.STRING()) \
        .flat_map(LatencyFlatMap(), output_type=Types.STRING()) \
        .add_sink(RedisSink())
    env.execute("data_quality_monitor_ingest_latency")


def _arrival_watermark_strategy() -> Any:  # pragma: no cover - 运行期占位（真实实现见部署侧）
    """提取 Kafka 记录到达时间作为处理元组 ``(value, arrival_ms)`` 的水印策略占位。

    真实 PyFlink 实现应通过自定义 ``KafkaRecordDeserializationSchema`` 把 ``record.timestamp()``
    与消息体一起下传；此处仅示意拓扑，运行依赖部署侧（不在仓库单测覆盖范围）。
    """
    raise NotImplementedError("部署侧提供 Kafka 记录时间戳解码（见作业 README）")


if __name__ == "__main__":  # pragma: no cover
    main()


__all__ = [
    "CONSUMER_GROUP",
    "INPUT_TOPIC",
    "KAFKA_BOOTSTRAP",
    "REDIS_KEY",
    "REDIS_TTL_SECONDS",
    "aggregate_latency",
    "encode_redis_value",
    "expand_latency",
    "latency_ms",
    "main",
    "window_seconds_from_env",
]
