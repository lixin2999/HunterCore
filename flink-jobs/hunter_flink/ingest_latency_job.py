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

可观测性（V1.19.21）：上述**丢弃口径一条都不放宽**，但「全都丢」与「根本没数据」过去在
看板上是同一个 `-`，运维无法三分（现场：三作业 RUNNING、看板入库延迟仍 `-`，日志里查不到
任何成因）。现按 ``seen`` / 各丢弃路径计数，并在**有输入但本节拍无有效样本**时写一份带
``reason`` + ``dropped`` 的**诊断载荷**（``p95_ms`` 仍为 null，不伪造数值），节拍由 Kafka 记录
时间戳对齐（``INGEST_LATENCY_DIAGNOSTIC_INTERVAL_SECONDS``，默认 60s）——不依赖事件时间窗口
冲刷（全丢弃时缓冲区永不建立，事件时间口径下永远不会冲刷，正是键永不存在的原因）。
载荷缺失本身仍是信号：``reason`` 读不到 = 作业没消费到 ``telemetry_raw``（巡检 ⑯ 三分）。

纪律：与 ``detection_job`` / ``algorithm_performance_job`` 同构——纯 Python 核心 + ``main()``
延迟 import pyflink；仓库单测无需 PyFlink 运行时。仅 import 无运行时的
``hunter_common.redis_keys``（纯字符串常量，redis-keys.yaml pending #7 落地的键构造单一事实来源），
不 import 任何服务 ``app.*`` 包或 ``hunter_common.database`` 仓储（写侧豁免边界不变）。
"""
from __future__ import annotations

import json
import logging
import math
import os
from collections.abc import Iterable, Mapping
from typing import Any

from hunter_common.redis_keys import ANALYTICS_INGEST_LATENCY

from hunter_flink.algorithm_performance_job import percentile_cont
from hunter_flink.checkpointing import enable_checkpointing
from hunter_flink.job_conn import (
    apply_kafka_properties,
    kafka_sasl_properties,
    redis_url,
)

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

#: 诊断载荷节拍（秒）：有输入但无有效样本时，最多每这么长写一次「为什么没数」（V1.19.21）
DIAGNOSTIC_INTERVAL_SECONDS = float(os.environ.get("INGEST_LATENCY_DIAGNOSTIC_INTERVAL_SECONDS", "60"))

logger = logging.getLogger(__name__)

#: 丢弃路径计数键（**顺序即主因判定优先级**；诊断载荷 ``dropped`` 的固定字段集，
#: 读端 ``IngestLatencyRedisReader`` 与巡检 ⑯ 按此口径解析，改名须同步三处 + redis-keys.yaml）
COUNTER_KEYS = (
    "unparsable_message",
    "no_vehicle_timestamp",
    "no_kafka_record_timestamp",
    "negative_latency",
    "non_finite_latency",
)


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


def new_counters() -> dict[str, int]:
    """新建诊断计数器组（``seen``/``valid`` + 各丢弃路径计数，均从 0 起）。"""
    counters: dict[str, int] = {"seen": 0, "valid": 0}
    counters.update(dict.fromkeys(COUNTER_KEYS, 0))
    return counters


def reset_counters(counters: dict[str, int]) -> None:
    """清零计数器（诊断载荷发出后调用：每份载荷只描述一个诊断节拍，不是作业启动至今的累计）。"""
    for key in counters:
        counters[key] = 0


def _bump(counters: dict[str, int] | None, key: str) -> None:
    if counters is not None:
        counters[key] = counters.get(key, 0) + 1


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
    arrival_time: float | None,
    window_seconds: float,
    buffers: dict[int, list[float]],
    counters: dict[str, int] | None = None,
) -> list[dict[str, Any]]:
    """一条 telemetry_raw → 0..n 个「已完成窗口」的入库延迟指标（车队级、跨窗口边界冲刷）。

    ``buffers`` 为按窗口 ID（车队全局，非按车）维护的延迟样本状态后端；窗口以车端事件时间
    划分（tumbling，``window_id = event_time // window_seconds``）。仅当本样本进入更晚窗口时
    冲刷更早的已完成窗口（旧窗口不再接收数据 → 可安全产出 P95）。尾窗口由部署侧 Flink 状态兜底。

    ``counters``（V1.19.21，可选）：传入时逐条累计 ``seen``/``valid`` 与各丢弃路径计数——
    **丢弃行为本身一字不改**（不放宽负值口径、不取绝对值、不用处理时间替代），只是把
    「丢了多少、为什么丢」留下来，否则全丢弃时看板只能看到一个无信息的 `-`。
    ``arrival_time`` 改为允许 ``None``（Kafka 记录时间戳缺失）：同样丢弃，但可计数。
    """
    _bump(counters, "seen")
    event_time = _event_time(msg)
    if event_time is None:
        _bump(counters, "no_vehicle_timestamp")
        return []
    if arrival_time is None:
        _bump(counters, "no_kafka_record_timestamp")
        return []
    sample = latency_ms(event_time, arrival_time)
    if sample is None:
        # 时钟超前/异常样本：丢弃，不污染 P95（口径不变，V1.19.21 起至少把「丢了多少」说清楚）
        _bump(
            counters,
            "negative_latency" if float(arrival_time) < event_time else "non_finite_latency",
        )
        return []
    _bump(counters, "valid")
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


def diagnostic_reason(counters: Mapping[str, int]) -> str:
    """按计数给出「本诊断节拍为什么没有 P95」的单一主因（供看板与巡检直接读）。

    返回值均为受控词表（与 :data:`COUNTER_KEYS` 同源）：``no_input``（没读到一个样本）/
    某一条丢弃路径名（该类占比 100%）/ ``mixed_discards``（多类混合）/ ``window_not_closed``
    （有有效样本但所属窗口未跨边界，尚未冲刷）。
    """
    seen = int(counters.get("seen", 0))
    if seen <= 0:
        return "no_input"
    if int(counters.get("valid", 0)) > 0:
        return "window_not_closed"
    for key in COUNTER_KEYS:
        if int(counters.get(key, 0)) == seen:
            return key
    return "mixed_discards"


def diagnostic_payload(
    counters: Mapping[str, int],
    *,
    window_seconds: float,
    window_end: float,
) -> dict[str, Any]:
    """无有效样本时的诊断载荷（V1.19.21）：``p95_ms`` 仍为 ``None``，**不伪造数值**。

    读端（``IngestLatencyRedisReader``）把 ``reason``/``dropped`` 透到看板 ``pipeline
    .ingest_latency_reason``；``window_end`` 取诊断节拍对齐到的到达时间（不是事件时间窗口）。
    """
    return {
        "metric": "ingest_latency_ms_p95",
        "p95_ms": None,
        "count": 0,
        "window": _window_label(window_seconds),
        "window_end": round(float(window_end), 3),
        "reason": diagnostic_reason(counters),
        "seen": int(counters.get("seen", 0)),
        "dropped": {key: int(counters.get(key, 0)) for key in COUNTER_KEYS},
    }


def should_emit_diagnostic(
    arrival_time: float,
    last_emit_at: float | None,
    interval_seconds: float,
) -> bool:
    """诊断节拍（以 Kafka 记录时间戳对齐）：首次必发，其后每 ``interval_seconds`` 最多一次。

    不用事件时间也不用处理时间：前者在全丢弃时根本没有事件时间可用，后者会把节拍交给
    TM 启动时的时钟偷走一个完整间隔。``last_emit_at`` 为 None 表示本 subtask 尚未写过。
    """
    if last_emit_at is None:
        return True
    return (float(arrival_time) - last_emit_at) >= interval_seconds


def encode_redis_value(metric: Mapping[str, Any]) -> str:
    """指标 → Redis 值（看板 :class:`IngestLatencyReader` 解析的 JSON 契约载荷）。"""
    return json.dumps(dict(metric), ensure_ascii=False)


def main() -> None:  # pragma: no cover - 需要 PyFlink + Redis 运行时，仓库内不可执行
    """PyFlink DataStream 作业提交入口（Flink 2.1）→ Redis 指标键。

    拓扑（契约 data_quality_monitor「延迟」子项，pending #10 结案路线）::

        Kafka telemetry_raw（group=data-analytics-telemetry-raw，取 Kafka 记录时间戳为到达时间）
          → keyBy(常量) → expand_latency 车队窗口缓冲（跨窗口冲刷）+ Redis SET 指标键

    ``arrival_time`` 取 Kafka 记录时间戳（broker LogAppendTime / 生产者 CreateTime，经算子上下文
    ``ctx.timestamp()`` 下传；PyFlink 无 Python 侧 ``KafkaRecordDeserializationSchema``，记录时间戳
    只能走该路径），车端 ``timestamp`` 取消息体；二者差即入库链路延迟。无记录时间戳的样本直接丢弃
    （不取处理时间替代、不伪造）。keyBy 常量以保证车队级 P95 精确（规模过大时可改近似分位聚合，
    须回改契约）。Redis 写失败不吞异常：作业重启后重放（at-least-once）；作业停摆时键按 TTL
    自然过期（看板降级 null）。

    ⚠ V1.19.21 起本算子对**单条消息的解析失败不再上抛**：``telemetry_raw`` 保留 7 天，而本组
    首次运行时无提交位点 ⇒ 从位点起点回放，旧版里一条非法 JSON 就能把作业打回 RESTARTING 并
    在重放下永远卡在同一条（实录：仅本作业 RESTARTING、另两个 RUNNING，而看板恒 `-`）。
    现计入 ``unparsable_message`` 并继续，成因从 TM 日志与看板 reason 两处可读。

    ⚠ 写入落在算子内而非 Sink：PyFlink 2.x 的 ``SinkFunction``/``SourceFunction`` 仅为 Java 包装
    （``JavaFunctionWrapper`` 构造须传 Java 对象或 Java 类名），Python 自定义 Sink 不被支持，
    故终端用 ``print`` 收口（每个窗口或每个诊断节拍 1 行，作业停摆即无输出，兼作投递轨迹）。
    """
    import redis
    from pyflink.common import Types, WatermarkStrategy
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.connectors.kafka import (
        KafkaOffsetResetStrategy,
        KafkaOffsetsInitializer,
        KafkaSource,
    )
    from pyflink.datastream.functions import KeyedProcessFunction

    window_seconds = window_seconds_from_env()

    class LatencyProcessor(KeyedProcessFunction):
        """车队窗口缓冲 + Redis 指标写（副作用），产出载荷供终端 sink 收口。

        两个写键路径（V1.19.21）：① 窗口冲刷 → 真 P95；② 有输入但本诊断节拍无有效样本 →
        诊断载荷（``p95_ms`` = null + ``reason`` + ``dropped``）。② 以 ``valid > 0`` 作门禁，
        **绝不用 null 载荷覆盖已有 P95**（否则真指标会被自己的可观测性改动顶掉）。
        """

        def __init__(self) -> None:
            self._buffers: dict[int, list[float]] = {}
            self._counters: dict[str, int] = new_counters()
            self._last_diagnostic_at: float | None = None
            self._client: redis.Redis | None = None

        def open(self, runtime_context: Any) -> None:
            # 口令由 redis_url() 从 REDIS_HOST/REDIS_PORT/REDIS_PASSWORD 拼装（已 URL 编码）；
            # compose 的 Redis 开了 requirepass，缺凭据时首条记录就会报错（不静默丢指标）
            self._client = redis.Redis.from_url(redis_url(), decode_responses=True)

        def close(self) -> None:
            if self._client is not None:
                self._client.close()
                self._client = None

        def process_element(self, value: str, ctx: Any):
            assert self._client is not None  # open() 已建连
            arrival_ms = ctx.timestamp()
            # Kafka 记录时间戳缺失：丢弃该样本（禁止用处理时间伪造），但仍可计数/诊断
            arrival_time = float(arrival_ms) / 1000.0 if isinstance(arrival_ms, int) and arrival_ms > 0 else None
            try:
                message = json.loads(value)
            except (json.JSONDecodeError, TypeError, ValueError):
                _bump(self._counters, "seen")
                _bump(self._counters, "unparsable_message")
                logger.warning(
                    "ingest_latency_unparsable_message offset_arrival=%s", arrival_ms
                )
                message = None
            metrics: list[dict[str, Any]] = []
            if isinstance(message, dict):
                metrics = expand_latency(
                    message,
                    arrival_time=arrival_time,
                    window_seconds=window_seconds,
                    buffers=self._buffers,
                    counters=self._counters,
                )
            for metric in metrics:
                payload = encode_redis_value(metric)
                self._client.set(REDIS_KEY, payload, ex=REDIS_TTL_SECONDS)
                yield payload
            if metrics:
                # 本节拍出了真值：推进诊断基准并清零计数，不留旧丢弃记录
                reset_counters(self._counters)
                if arrival_time is not None:
                    self._last_diagnostic_at = arrival_time
                return
            if arrival_time is None or self._counters["valid"] > 0:
                return  # 无诊断节拍基准 / 已有有效样本只是窗口未闭：不写 null 载荷
            if not should_emit_diagnostic(
                arrival_time, self._last_diagnostic_at, DIAGNOSTIC_INTERVAL_SECONDS
            ):
                return
            diagnostic = diagnostic_payload(
                self._counters, window_seconds=window_seconds, window_end=arrival_time
            )
            payload = encode_redis_value(diagnostic)
            self._client.set(REDIS_KEY, payload, ex=REDIS_TTL_SECONDS)
            logger.warning(
                "ingest_latency_window_discarded reason=%s seen=%s dropped=%s",
                diagnostic["reason"],
                diagnostic["seen"],
                diagnostic["dropped"],
            )
            reset_counters(self._counters)
            self._last_diagnostic_at = arrival_time
            yield payload

    env = StreamExecutionEnvironment.get_execution_environment()
    # 检查点（V1.19.15）：不开则本组永无提交位点（详见 hunter_flink.checkpointing 模块头）
    enable_checkpointing(env)
    # SASL 属性随作业图下发（语义同 detection_job，见 hunter_flink.job_conn）
    sasl_props = kafka_sasl_properties()
    # 记录到达时间来自 Kafka 时间戳：FLIP-27 Source 经 from_source 下传，水位不用于本作业
    source_builder = apply_kafka_properties(
        (
            KafkaSource.builder()
            .set_bootstrap_servers(KAFKA_BOOTSTRAP)
            .set_topics(INPUT_TOPIC)
            .set_group_id(CONSUMER_GROUP)
            # 已提交位点优先，无提交位点回落 earliest（契约 defaults.auto_offset_reset）
            .set_starting_offsets(
                KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST)
            )
            .set_value_only_deserializer(SimpleStringSchema())
        ),
        sasl_props,
    )
    source = source_builder.build()
    env.from_source(
        source, WatermarkStrategy.no_watermarks(), INPUT_TOPIC, type_info=Types.STRING()
    ) \
        .key_by(lambda _raw: "fleet", key_type=Types.STRING()) \
        .process(LatencyProcessor(), output_type=Types.STRING()) \
        .print("ingest_latency")
    env.execute("data_quality_monitor_ingest_latency")


if __name__ == "__main__":  # pragma: no cover
    main()


__all__ = [
    "CONSUMER_GROUP",
    "COUNTER_KEYS",
    "DIAGNOSTIC_INTERVAL_SECONDS",
    "INPUT_TOPIC",
    "KAFKA_BOOTSTRAP",
    "REDIS_KEY",
    "REDIS_TTL_SECONDS",
    "aggregate_latency",
    "diagnostic_payload",
    "diagnostic_reason",
    "encode_redis_value",
    "expand_latency",
    "latency_ms",
    "main",
    "new_counters",
    "reset_counters",
    "should_emit_diagnostic",
    "window_seconds_from_env",
]
