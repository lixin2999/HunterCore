"""``algorithm_performance_monitor``（6.2.4 算法性能监控）实时聚合核心（纯 Python）。

契约依据：``contracts/openapi/data-analytics.yaml`` → ``x-hunter-realtime-jobs.jobs
[algorithm_performance_monitor]``（metric_mapping 6 项 + ``ALGORITHM_METRICS_WINDOW_SECONDS``
60s 滚动窗口），输出载荷与 ``contracts/kafka/schemas/algorithm_metrics.schema.json`` 1:1
（字段严格 ⊆ 表列 ``{time, vehicle_id, module, metric_name, metric_value, tags}``，
``module`` 受控词表 perception/planning/control）。

口径（契约 logic + schema 注释固化）：
- ``perception.fps``：均值（``metric_name=fps``）+ 最小值（``metric_name=fps_min``，
  满足 logic「均值/最小值」，独立 metric_name 避免幂等键 ``(time,vehicle_id,module,metric_name)`` 冲突）；
- ``perception.latency_ms``：P95（``percentile_cont`` 线性插值，与看板只读聚合口径一致）；
- ``planning.planning_latency_ms`` / ``control.control_latency_ms``：窗口均值；
- ``control.velocity_error`` → ``metric_name=velocity_error_abs``、
  ``control.steer_error`` → ``metric_name=steer_error_abs``：误差取绝对值后窗口均值（schema 注释）。

纪律：不直连库（G-13），仅产出 ``algorithm_metrics`` Kafka 载荷；缺失字段跳过该指标（禁止伪造 0）。
"""
from __future__ import annotations

import json
import math
import os
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from hunter_common.database.enums import MetricModule

from hunter_flink.thresholds import AlertThresholds

#: Kafka broker（standalone/compose 单机形态；K8s 由环境变量下发）
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

#: 输出 Topic（契约 topics.yaml platform_topics；与 data-analytics 消费者一致）
OUTPUT_TOPIC = "algorithm_metrics"
#: 消费组（契约 consumer-groups.yaml：Flink 组，produces algorithm_metrics）
CONSUMER_GROUP = "data-analytics-telemetry"
#: 输入 Topic（清洗后遥测）
INPUT_TOPIC = "telemetry_clean"

# (module, metric_name, agg_kind)：agg_kind ∈ avg/min/p95；extractor 从样本取原始值
_EXTRACTOR = Callable[[Mapping[str, Any]], "float | None"]


def _seg(msg: Mapping[str, Any], segment: str, field: str) -> float | None:
    """安全取 ``msg[segment][field]`` 并归一为有限 float（缺失/非数值/NaN/Inf → None）。"""
    payload = msg.get(segment)
    if not isinstance(payload, Mapping):
        return None
    value = payload.get(field)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _abs_seg(msg: Mapping[str, Any], segment: str, field: str) -> float | None:
    """控制误差取绝对值（契约：velocity_error_abs / steer_error_abs）。"""
    number = _seg(msg, segment, field)
    return None if number is None else abs(number)


#: 指标规格：(module, metric_name, agg_kind, extractor)
_METRIC_SPECS: tuple[tuple[MetricModule, str, str, _EXTRACTOR], ...] = (
    (MetricModule.PERCEPTION, "fps", "avg", lambda m: _seg(m, "perception", "fps")),
    (MetricModule.PERCEPTION, "fps_min", "min", lambda m: _seg(m, "perception", "fps")),
    (MetricModule.PERCEPTION, "latency_ms", "p95", lambda m: _seg(m, "perception", "latency_ms")),
    (MetricModule.PLANNING, "planning_latency_ms", "avg", lambda m: _seg(m, "planning", "planning_latency_ms")),
    (MetricModule.CONTROL, "control_latency_ms", "avg", lambda m: _seg(m, "control", "control_latency_ms")),
    (MetricModule.CONTROL, "velocity_error_abs", "avg", lambda m: _abs_seg(m, "control", "velocity_error")),
    (MetricModule.CONTROL, "steer_error_abs", "avg", lambda m: _abs_seg(m, "control", "steer_error")),
)


def percentile_cont(values: Iterable[float], q: float) -> float | None:
    """连续百分位（线性插值，口径与 PostgreSQL ``percentile_cont`` 一致）；空输入返回 None。"""
    ordered = sorted(values)
    if not ordered:
        return None
    if len(ordered) == 1:
        return float(ordered[0])
    pos = q * (len(ordered) - 1)
    low = math.floor(pos)
    high = math.ceil(pos)
    if low == high:
        return float(ordered[low])
    return float(ordered[low] + (ordered[high] - ordered[low]) * (pos - low))


def _reduce(agg: str, values: list[float]) -> float | None:
    if not values:
        return None
    if agg == "min":
        return min(values)
    if agg == "p95":
        return percentile_cont(values, 0.95)
    return sum(values) / len(values)  # avg


def aggregate_metrics(
    vehicle_id: str,
    samples: Iterable[Mapping[str, Any]],
    *,
    window_seconds: float,
) -> list[dict[str, Any]]:
    """单车单窗口 telemetry_clean 样本 → 0..n 条 algorithm_metrics 载荷。

    - ``time`` 取窗口内车端遥测时间上界（max timestamp），禁止用处理时间替代（schema 约束）；
    - 每指标仅当窗口内至少有一个有效样本时产出（缺失不伪造）；
    - 无任何带时间戳样本时不产出（无法确定分区键 time）。
    """
    sample_list = [msg for msg in samples if isinstance(msg, Mapping)]
    # 车端事件时间戳（顶层 timestamp，非段内字段）
    event_times = [
        number
        for msg in sample_list
        if isinstance((number := _top_timestamp(msg)), float)
    ]
    if not event_times:
        return []
    window_end = max(event_times)

    messages: list[dict[str, Any]] = []
    for module, metric_name, agg, extractor in _METRIC_SPECS:
        values = [v for msg in sample_list if (v := extractor(msg)) is not None]
        reduced = _reduce(agg, values)
        if reduced is None:
            continue
        messages.append(
            {
                "vehicle_id": vehicle_id,
                "module": module.value,
                "metric_name": metric_name,
                "metric_value": round(reduced, 6),
                "time": window_end,
                "tags": {"window": f"{int(window_seconds)}s", "agg": agg},
            }
        )
    return messages


def _top_timestamp(msg: Mapping[str, Any]) -> float | None:
    """取顶层车端事件时间戳（Unix epoch 秒，含毫秒小数）。"""
    value = msg.get("timestamp")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def expand_metrics(
    msg: Mapping[str, Any],
    *,
    window_seconds: float,
    buffers: dict[tuple[str, int], list[Mapping[str, Any]]],
) -> list[dict[str, Any]]:
    """一条 telemetry_clean → 0..n 条 algorithm_metrics（单车有序、跨窗口边界冲刷）。

    Flink 算子与本地回测共用：``buffers`` 为按 ``(vehicle_id, window_id)`` 维护的状态后端。
    窗口以车端事件时间划分（tumbling，``window_id = ts // window_seconds``）；仅当本样本进入
    更晚窗口时冲刷该车辆更早的已完成窗口（旧窗口不再接收数据 → 可安全产出）。尾部未越界的
    窗口在下一条同车样本到达时冲刷；跨作业停止时的尾窗口由部署侧 Flink 状态兜底（不在核心处理）。
    """
    vehicle_id = msg.get("vehicle_id")
    ts = _top_timestamp(msg)
    if not isinstance(vehicle_id, str) or not vehicle_id or ts is None:
        return []
    window_id = int(ts // window_seconds)
    buffers.setdefault((vehicle_id, window_id), []).append(msg)
    flushed: list[dict[str, Any]] = []
    for (vid, wid) in [key for key in buffers if key[0] == vehicle_id and key[1] < window_id]:
        bucket = buffers.pop((vid, wid))
        flushed += aggregate_metrics(vid, bucket, window_seconds=window_seconds)
    return flushed


def main() -> None:  # pragma: no cover - 需要 PyFlink 运行时，仓库内不可执行
    """PyFlink DataStream 作业提交入口（Flink 2.1）。

    拓扑（契约 x-hunter-realtime-jobs algorithm_performance_monitor）::

        Kafka telemetry_clean（group=data-analytics-telemetry）
          → keyBy(vehicle_id) → 60s 事件时间滚动窗口聚合（expand_metrics 缓冲）
          → JSON 序列化 → Kafka algorithm_metrics（key=vehicle_id）

    与 ``detection_job`` 同构：窗口缓冲为按 key 的算子内状态；``expand_metrics`` 纯 Python，
    仓库单测无需 PyFlink 环境。反序列化/处理失败转 ``algorithm_metrics.dlq`` 由部署侧失败处理器承担。
    """
    from pyflink.common import Types, WatermarkStrategy
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.connectors.kafka import (
        KafkaOffsetResetStrategy,
        KafkaOffsetsInitializer,
        KafkaRecordSerializationSchema,
        KafkaSink,
        KafkaSource,
    )
    from pyflink.datastream.functions import FlatMapFunction

    window_seconds = AlertThresholds.from_env().algorithm_metrics_window_seconds

    class MetricsFlatMap(FlatMapFunction):
        def __init__(self) -> None:
            self._buffers: dict[tuple[str, int], list[Mapping[str, Any]]] = {}

        def flat_map(self, value: str) -> Iterable[str]:
            msg = json.loads(value)
            metrics = expand_metrics(msg, window_seconds=window_seconds, buffers=self._buffers)
            return [json.dumps(item, ensure_ascii=False) for item in metrics]

    env = StreamExecutionEnvironment.get_execution_environment()
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(INPUT_TOPIC)
        .set_group_id(CONSUMER_GROUP)
        # 已提交位点优先，无提交位点回落 earliest（契约 defaults.auto_offset_reset；
        # 与 detection_job 同组 data-analytics-telemetry，位点共享）
        .set_starting_offsets(
            KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST)
        )
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    # FLIP-27 Source 走 from_source（add_source 仅适用 SourceFunction）；窗口划分用载荷内的
    # 车端事件时间（expand_metrics 自管缓冲），不依赖 Flink 水位
    env.from_source(
        source, WatermarkStrategy.no_watermarks(), INPUT_TOPIC, type_info=Types.STRING()
    ) \
        .key_by(lambda raw: json.loads(raw).get("vehicle_id"), key_type=Types.STRING()) \
        .flat_map(MetricsFlatMap(), output_type=Types.STRING()) \
        .sink_to(
            KafkaSink.builder()
            .set_bootstrap_servers(KAFKA_BOOTSTRAP)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(OUTPUT_TOPIC)
                .set_key_serialization_schema(SimpleStringSchema())
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
            .build()
        )
    env.execute("algorithm_performance_monitor")


if __name__ == "__main__":  # pragma: no cover
    main()


__all__ = [
    "CONSUMER_GROUP",
    "INPUT_TOPIC",
    "KAFKA_BOOTSTRAP",
    "OUTPUT_TOPIC",
    "aggregate_metrics",
    "expand_metrics",
    "main",
    "percentile_cont",
]
