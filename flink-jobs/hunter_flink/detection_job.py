"""``driving_anomaly_detection``（6.2.2 异常驾驶检测）Flink 1.18 作业入口。

拓扑（契约 x-hunter-realtime-jobs）：
    Kafka telemetry_clean（group=data-analytics-telemetry）
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

from hunter_flink.rules import VehicleAnomalyTracker, build_alert
from hunter_flink.thresholds import AlertThresholds

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INPUT_TOPIC = "telemetry_clean"
OUTPUT_TOPIC = "alert_event"
CONSUMER_GROUP = "data-analytics-telemetry"


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
    """PyFlink DataStream 作业提交入口（Flink 1.18）。"""
    from pyflink.common import Types
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.connectors.kafka import (
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
    env.add_source(
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(INPUT_TOPIC)
        .set_group_id(CONSUMER_GROUP)
        .set_starting_offsets(KafkaOffsetsInitializer.group_offsets())
        .value_only_decoder(Types.PRIMITIVE_STRING(Types.InformationType()))
        .build()
    ) \
        .key_by(lambda raw: json.loads(raw).get("vehicle_id"), key_type=Types.STRING()) \
        .flat_map(AlertFlatMap(), output_type=Types.STRING()) \
        .sink(
            KafkaSink.builder()
            .set_bootstrap_servers(KAFKA_BOOTSTRAP)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(OUTPUT_TOPIC)
                .set_key_serialization_schema(Types.PRIMITIVE_STRING(Types.InformationType()))
                .set_value_serialization_schema(
                    Types.PRIMITIVE_STRING(Types.InformationType())
                )
                .build()
            )
            .build()
        )
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
