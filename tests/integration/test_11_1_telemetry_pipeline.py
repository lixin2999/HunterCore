"""11.1 车辆数据上行集成测试（Kafka → 路由 → TimescaleDB 落库）。

链路依据（系统关键约束第 4/6 条 + 第 10 条）：
- 车端 ``hunter.{vehicle_id}.telemetry``（6 分区 / acks=1 / 保留 7 天）→ data-collector 消费接入
- 预处理（校验/清洗/对齐）后路由到平台内部 ``telemetry_raw``（12 分区 / 保留 7 天）
- 扁平化写入 ``data_collector.vehicle_telemetry``（hypertable：按天分块、保留 90 天）
- 幂等：``ON CONFLICT (time, vehicle_id) DO NOTHING``（acks=1 允许重复投递）
- 性能：遥测入库延迟 ≤ 1s（此处断言单条入库延迟，吞吐见 performance 套件）

Topic 名 / 分区数 / 保留时间 / 表列名均取自 ``tests.support``（contracts + db），禁止硬编码。
"""
from __future__ import annotations

import time
from typing import Any

import pytest

from tests.support import broker, contracts, db, flow, messages, thresholds

pytestmark = [pytest.mark.integration, pytest.mark.l5_case("11.1")]

#: 车辆号取自契约示例（telemetry.schema.json examples[0]），避免硬编码
VEHICLE_ID: str = str(contracts.schema_example("telemetry")["vehicle_id"])


def _vehicle_topic(topic_type: str) -> str:
    """车端 Topic 名（由 flow 统一渲染模板，保证与契约一致）。"""
    return flow.platform_topic_of(VEHICLE_ID, topic_type)


def test_vehicle_telemetry_topic_partitions_match_contract(kafka_bootstrap: str) -> None:
    """车端遥测 Topic 分区数 = 契约值（6，按 vehicle_id 哈希）。"""
    topic = _vehicle_topic("telemetry")
    metadata = broker.topic_metadata(kafka_bootstrap, topic)
    assert len(metadata) == contracts.topic_partitions(topic) == 6


def test_vehicle_event_topic_partitions_match_contract(kafka_bootstrap: str) -> None:
    """车端事件 Topic 分区数 = 契约值（3）。"""
    topic = _vehicle_topic("event")
    assert len(broker.topic_metadata(kafka_bootstrap, topic)) == contracts.topic_partitions(topic)


def test_platform_topic_partitions_match_contract(kafka_bootstrap: str) -> None:
    """平台内部 Topic（telemetry_raw/telemetry_clean/event_raw…）分区数与契约一致。"""
    for spec in contracts.platform_topics().values():
        topic = str(spec["name"])
        metadata = broker.topic_metadata(kafka_bootstrap, topic)
        assert len(metadata) == spec["partitions"], f"{topic} 分区数与契约不一致"


def test_telemetry_topic_retention_matches_contract(kafka_bootstrap: str) -> None:
    """遥测 Topic 保留时间 = 7 天（契约 retention.ms）。"""
    topic = _vehicle_topic("telemetry")
    retention = broker.topic_config(kafka_bootstrap, topic, ["retention.ms"])
    assert int(retention["retention.ms"]) == contracts.topic_retention_ms(topic)


def test_telemetry_message_roundtrip_bytewise(kafka_bootstrap: str) -> None:
    """遥测消息生产→消费字节级无损（key = vehicle_id，保证单车有序）。"""
    topic = _vehicle_topic("telemetry")
    payload = messages.telemetry_message(VEHICLE_ID, seq=101)
    delivered = broker.produce(kafka_bootstrap, topic, VEHICLE_ID, payload)
    assert delivered["partition"] is not None
    received = broker.consume(kafka_bootstrap, topic, max_messages=1, timeout_s=30)
    assert received, "未在 30s 内消费到遥测消息"
    assert received[0] == payload
    contracts.assert_valid_message("telemetry", received[0])


def test_telemetry_row_covers_ddl_columns() -> None:
    """消息 → 行映射必须覆盖 vehicle_telemetry 全部 DDL 列（字段名漂移立即失败）。"""
    payload = messages.telemetry_message(VEHICLE_ID, seq=102)
    row = flow.telemetry_row(payload)
    assert list(db.missing_columns(row, db.TELEMETRY_TABLE)) == []
    assert set(row) == set(db.columns_of(db.TELEMETRY_TABLE))
    assert row["time"] == payload["timestamp"]
    assert row["vehicle_id"] == payload["vehicle_id"]
    assert row["seq"] == payload["seq"]
async def test_telemetry_persisted_within_latency_budget(postgres_handle: Any) -> None:
    """入库延迟 ≤ 1s：以单条批量写入耗时 + 上报时间戳差衡量（系统关键约束第 10 条）。"""
    rows = db.seed_rows(1, VEHICLE_ID, base_seq=103)
    elapsed = await db.bulk_insert(postgres_handle, rows)
    assert elapsed <= thresholds.TELEMETRY_INGEST_LATENCY_MAX_S, f"入库耗时 {elapsed}s 超阈值"
    latency = flow.telemetry_ingest_latency_s(time.time(), rows[0])
    assert 0 <= latency <= thresholds.TELEMETRY_INGEST_LATENCY_MAX_S
    persisted = await db.fetch(
        postgres_handle,
        f"SELECT seq FROM {db.qualified(db.TELEMETRY_TABLE)} WHERE vehicle_id = $1 AND time = $2",
        VEHICLE_ID,
        rows[0]["time"],
    )
    assert len(persisted) == 1
    assert int(persisted[0]["seq"]) == rows[0]["seq"]


async def test_telemetry_insert_is_idempotent(postgres_handle: Any) -> None:
    """重复投递（acks=1 场景）不得产生重复行：ON CONFLICT (time, vehicle_id) DO NOTHING。"""
    rows = db.seed_rows(3, VEHICLE_ID, base_seq=200)
    await db.bulk_insert(postgres_handle, rows)
    await db.bulk_insert(postgres_handle, rows)
    stored = await db.count_rows(
        postgres_handle, db.TELEMETRY_TABLE, "vehicle_id = $1 AND seq BETWEEN $2 AND $3",
        VEHICLE_ID, 200, 202,
    )
    assert stored == 3, f"重复写入导致行数异常：{stored}"
    assert set(db.primary_key_columns(db.TELEMETRY_TABLE)) == {"time", "vehicle_id"}


async def test_hypertable_config_matches_contract(postgres_handle: Any) -> None:
    """vehicle_telemetry 必须是 hypertable 且分块间隔 = 1 day（DDL 契约）。"""
    config = await db.hypertable_config(postgres_handle, db.TELEMETRY_TABLE)
    assert config["is_hypertable"] is True
    chunk_days = config["chunk_time_interval"]
    assert chunk_days is not None
    assert int(getattr(chunk_days, "days", chunk_days)) == thresholds.TIMESERIES_CHUNK_INTERVAL_DAYS


async def test_telemetry_columns_match_ddl(postgres_handle: Any) -> None:
    """实际表列名与 DDL 契约完全一致（命名与顺序均不可漂移）。"""
    actual = await db.table_columns_in_db(postgres_handle, db.TELEMETRY_TABLE)
    assert actual == db.columns_of(db.TELEMETRY_TABLE)


def test_sequence_gap_detection_matches_reference() -> None:
    """丢包检测基准：seq 不连续必须被识别（用于车端上报质量回归）。"""
    gaps = flow.detect_sequence_gap([1, 2, 3, 5, 6, 9])
    assert gaps == [(3, 5), (6, 9)]
    assert flow.detect_sequence_gap([1, 2, 3]) == []
