"""``driving_anomaly_detection``（6.2.2 异常驾驶检测）Flink 2.1 作业入口。

拓扑（契约 x-hunter-realtime-jobs）：
    Kafka telemetry_clean（group=data-analytics-telemetry-alert）
      → keyBy(vehicle_id) → VehicleAnomalyTracker（单车状态算子，无平行倾斜）
      → build_alert + JSON 序列化 → Kafka alert_event（key=vehicle_id）

反序列化失败 / 处理异常的消息转 ``alert_event`` 契约 DLQ 纪律（{topic}.dlq，保留原
topic/partition/offset 头）由部署侧 Flink 失败处理器承担；本作业内部禁止吞异常。

提交（standalone / compose 单机形态，README 有完整版）::

    docker exec hunter-flink-jm flink run -d \\
      -c hunter_flink.detection_job /opt/flink/jobs/hunter_flink.zip

依赖 pyflink 仅在 ``main()`` 内延迟导入（规则核心 ``rules`` / ``thresholds`` 纯
Python，仓库单测无需 PyFlink 环境）。
"""
from __future__ import annotations

import json
import os
import time
from collections.abc import Iterable
from typing import Any

from hunter_flink.job_conn import apply_kafka_properties, kafka_sasl_properties
from hunter_flink.rules import VehicleAnomalyTracker, build_alert
from hunter_flink.thresholds import AlertThresholds

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INPUT_TOPIC = "telemetry_clean"
OUTPUT_TOPIC = "alert_event"
# 一组一作业（V1.19.10）：原与 algorithm_performance_job 共用 data-analytics-telemetry，两个独立
# KafkaConsumer 共用 group.id 会使提交位点互相覆盖（LAG 监控失真、冷启动起点被带偏），若分区交由
# broker 分配则两作业各只拿到一半分区。现已拆组；契约对应 consumer-groups.yaml 的同名条目。
CONSUMER_GROUP = "data-analytics-telemetry-alert"


def expand_alerts(
    msg: dict[str, Any],
    *,
    thresholds: AlertThresholds,
    speed_limit_mps: float | None,
    trackers: dict[str, VehicleAnomalyTracker],
) -> Iterable[dict[str, Any]]:
    """一条 telemetry_clean 消息 → 0..n 条 alert_event 消息（单车有序）。

    Flink 算子与本地回测共用该函数：``trackers`` 即按 vehicle_id 维护的状态后端。
    """
    vehicle_id = str(msg.get("vehicle_id") or "")
    if not vehicle_id:
        return []
    tracker = trackers.get(vehicle_id)
    if tracker is None:
        tracker = VehicleAnomalyTracker(
            thresholds=thresholds, speed_limit_mps=speed_limit_mps
        )
        trackers[vehicle_id] = tracker
    hits = tracker.on_sample(msg, now=time.time())
    return [build_alert(vehicle_id, hit, now=time.time()) for hit in hits]


def main() -> None:  # pragma: no cover - 需要 PyFlink 运行时，仓库内不可执行
    """PyFlink DataStream 作业提交入口（Flink 2.1）。"""
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

    thresholds = AlertThresholds.from_env()
    limit_raw = os.environ.get("ALERT_OVER_SPEED_LIMIT_MPS", "").strip()
    speed_limit = float(limit_raw) if limit_raw else None

    class AlertFlatMap(FlatMapFunction):
        def __init__(self) -> None:
            self._trackers: dict[str, VehicleAnomalyTracker] = {}

        def flat_map(self, value: str) -> Iterable[str]:
            msg = json.loads(value)
            alerts = expand_alerts(
                msg,
                thresholds=thresholds,
                speed_limit_mps=speed_limit,
                trackers=self._trackers,
            )
            return [json.dumps(alert, ensure_ascii=False) for alert in alerts]

    env = StreamExecutionEnvironment.get_execution_environment()
    # Kafka 客户端 SASL 属性（V1.19.8）：broker 内部监听 9092 是 SASL_PLAINTEXT + SCRAM-SHA-512，
    # 作业必须同协议同凭据（否则不报错但 **静默空转**：fetch 不到数据而作业仍 RUNNING）；
    # 明文环境返回 {} 保持原行为，声明了 SASL 却缺凭据则在此抛错（提交期失败优于运行期假绿）。
    sasl_props = kafka_sasl_properties()
    source_builder = apply_kafka_properties(
        (
            KafkaSource.builder()
            .set_bootstrap_servers(KAFKA_BOOTSTRAP)
            .set_topics(INPUT_TOPIC)
            .set_group_id(CONSUMER_GROUP)
            # 已提交位点优先；无提交位点（首次部署 / offset 保留期已过）回落 earliest，
            # 与契约 consumer-groups.yaml ``defaults.auto_offset_reset: earliest`` 同语义
            # （Flink 2.x 名字：``committed_offsets``，1.x 的 ``group_offsets`` 已随 Java
            #  ``OffsetsInitializer`` 重组删除）
            .set_starting_offsets(
                KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST)
            )
            # 参数位是 DeserializationSchema（不是 TypeInformation）；2.x 无 ``value_only_decoder``
            .set_value_only_deserializer(SimpleStringSchema())
        ),
        sasl_props,
    )
    source = source_builder.build()
    sink_builder = apply_kafka_properties(
        (
            KafkaSink.builder()
            .set_bootstrap_servers(KAFKA_BOOTSTRAP)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(OUTPUT_TOPIC)
                .set_key_serialization_schema(SimpleStringSchema())
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
        ),
        sasl_props,
    )
    # KafkaSource 是 FLIP-27 Source：必须走 ``from_source``（``add_source`` 只接受
    # SourceFunction）；本作业不依赖事件时间，水位取 ``no_watermarks``
    env.from_source(
        source, WatermarkStrategy.no_watermarks(), INPUT_TOPIC, type_info=Types.STRING()
    ) \
        .key_by(lambda raw: json.loads(raw).get("vehicle_id"), key_type=Types.STRING()) \
        .flat_map(AlertFlatMap(), output_type=Types.STRING()) \
        .sink_to(sink_builder.build())  # Java ``sinkTo``：PyFlink 无 ``DataStream.sink``
    env.execute("driving_anomaly_detection")


if __name__ == "__main__":  # pragma: no cover
    main()

__all__ = [
    "CONSUMER_GROUP",
    "INPUT_TOPIC",
    "OUTPUT_TOPIC",
    "expand_alerts",
    "main",
]
