"""采集链路单元测试（审查 R1/R6：预处理流水线 + 批量入库 + 读模型 + 消费者）。

覆盖：
- 预处理 5.4 流水线：时间戳对齐（毫秒/时钟漂移）、数据清洗（NaN/Inf 丢弃、值域裁剪）、
  扁平行映射（与读路径 ``telemetry_row_to_dict`` 对称）；
- 批量入库与 raw/clean 投递（含「写库失败 → flush 抛错 → 消费者不提交 offset」链路）；
- ``vehicle:status`` / ``vehicle:online:set`` 读模型写入与离线守护；
- 消费者幂等键与处理入口（遥测/健康/事件）。
"""
from __future__ import annotations

import json
import time
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml
from hunter_common.kafka.idempotency import IdempotencyGuard

from app.config import Settings
from app.consumers.events import EventIngestConsumer
from app.consumers.health import HealthIngestConsumer
from app.consumers.telemetry import TelemetryIngestConsumer
from app.repositories.telemetry import telemetry_row_to_dict
from app.services.ingest import (
    SEGMENT_COLUMNS,
    TelemetryIngestService,
    normalize_epoch_seconds,
    prepare_telemetry,
)
from app.services.vehicle_status import (
    OFFLINE_STATUS,
    ONLINE_SET_KEY,
    VehicleStatusSweeper,
    VehicleStatusWriter,
    status_key,
)
from app.tests.fakes import (
    FakeEventIngestRepository,
    FakePipelineProducer,
    FakeRedisHash,
    FakeTelemetryIngestRepository,
)

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_DIR = ROOT / "contracts" / "kafka" / "schemas"
#: 基准时刻（进程启动时间）：使「时钟漂移/回溯窗口」用例确定化，同时贴近真实 now
NOW = time.time()


def load_schema(name: str) -> dict[str, Any]:
    """加载 Kafka 消息契约 Schema（用例输入直接取自契约示例，避免字段漂移）。"""
    return json.loads((SCHEMA_DIR / f"{name}.schema.json").read_text(encoding="utf-8"))


def telemetry_payload(**overrides: Any) -> dict[str, Any]:
    """契约 Schema 示例 + 近期时间戳（避免被「回溯窗口」规则丢弃）。"""
    payload = deepcopy(load_schema("telemetry")["examples"][0])
    payload["timestamp"] = NOW
    payload.update(overrides)
    return payload


def event_payload(**overrides: Any) -> dict[str, Any]:
    payload = deepcopy(load_schema("event")["examples"][0])
    payload["timestamp"] = NOW
    payload.update(overrides)
    return payload


def make_settings(**overrides: Any) -> Settings:
    kwargs: dict[str, Any] = {
        "telemetry_batch_size": 2,
        "telemetry_publish_concurrency": 2,
        "vehicle_offline_threshold_seconds": 10,
    }
    kwargs.update(overrides)
    return Settings(**kwargs)


def make_ingest_service(
    repository: FakeTelemetryIngestRepository | None = None,
    producer: FakePipelineProducer | None = None,
    **config_overrides: Any,
) -> tuple[TelemetryIngestService, FakeTelemetryIngestRepository, FakePipelineProducer]:
    repo = repository or FakeTelemetryIngestRepository()
    pub = producer or FakePipelineProducer()
    service = TelemetryIngestService(repo, pub, make_settings(**config_overrides))
    return service, repo, pub


# ---------------------------------------------------------------------------
# 第 3 步：时间戳对齐
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (NOW, NOW),
        (NOW * 1000, NOW),          # 毫秒 → 秒
        (None, None),
        ("1724035200", None),        # 字符串不视为合法时间戳
        (True, None),                # 布尔不是数值
        (float("nan"), None),
        (float("inf"), None),
    ],
)
def test_normalize_epoch_seconds(value: Any, expected: float | None) -> None:
    """时间戳归一：秒原样、毫秒换算、非法值一律 None。"""
    assert normalize_epoch_seconds(value) == expected


def test_prepare_telemetry_rejects_clock_skew_and_stale_samples() -> None:
    """未来漂移 > 60s 与超过回溯窗口（7 天）的样本丢弃。"""
    assert prepare_telemetry(telemetry_payload(timestamp=NOW + 600), now=NOW) is None
    assert prepare_telemetry(telemetry_payload(timestamp=NOW - 8 * 86400), now=NOW) is None
    assert prepare_telemetry(telemetry_payload(timestamp=NOW + 59), now=NOW) is not None


def test_prepare_telemetry_requires_six_segments() -> None:
    """六段结构缺失 → 丢弃（契约 schema 六段必填，此处防御式兜底）。"""
    payload = telemetry_payload()
    del payload["planning"]
    assert prepare_telemetry(payload, now=NOW) is None


# ---------------------------------------------------------------------------
# 第 4 步：数据清洗
# ---------------------------------------------------------------------------
def test_prepare_telemetry_drops_non_finite_values() -> None:
    """NaN/Inf 不被 JSON Schema 拦截 → 预处理必须丢弃整条样本（数值字段不允许 null）。"""
    payload = telemetry_payload()
    payload["chassis"]["velocity"] = float("nan")
    assert prepare_telemetry(payload, now=NOW) is None
    payload = telemetry_payload()
    payload["system"]["cpu_usage"] = float("inf")
    assert prepare_telemetry(payload, now=NOW) is None


def test_prepare_telemetry_clamps_out_of_range_values_in_clean_only() -> None:
    """值域裁剪只作用于 clean 版本：raw 保留上报原值，clean 裁剪到契约边界。"""
    payload = telemetry_payload()
    payload["chassis"]["battery_soc"] = 120
    payload["system"]["network_rssi"] = 5

    prepared = prepare_telemetry(payload, now=NOW)

    assert prepared is not None
    assert prepared.cleaned is True
    assert prepared.raw["chassis"]["battery_soc"] == 120      # 原始上报值
    assert prepared.clean["chassis"]["battery_soc"] == 100    # 裁剪到边界
    assert prepared.clean["system"]["network_rssi"] == 0
    assert prepared.row["battery_soc"] == 100


def test_prepare_telemetry_keeps_sample_uncleaned_when_values_in_range() -> None:
    """合法样本不触发清洗（cleaned=False），raw 与 clean 一致。"""
    prepared = prepare_telemetry(telemetry_payload(), now=NOW)
    assert prepared is not None
    assert prepared.cleaned is False
    assert prepared.raw == prepared.clean


# ---------------------------------------------------------------------------
# 第 5-6 步：扁平行映射与批量写入
# ---------------------------------------------------------------------------
def test_flatten_to_row_round_trips_with_read_path() -> None:
    """写入扁平行 → 读路径重组必须回到同一六段结构（读写对称契约）。"""
    prepared = prepare_telemetry(telemetry_payload(), now=NOW)
    assert prepared is not None

    row = SimpleNamespace(**prepared.row)
    mapped = telemetry_row_to_dict(row)

    assert mapped["vehicle_id"] == prepared.clean["vehicle_id"]
    assert mapped["seq"] == prepared.clean["seq"]
    # time 经 datetime 往返（微秒精度）允许亚毫秒误差
    assert abs(mapped["time"] - prepared.timestamp) < 1e-3
    for segment_name, columns in SEGMENT_COLUMNS.items():
        assert set(mapped[segment_name]) == set(columns)


async def test_ingest_service_flushes_batch_and_publishes_raw_then_clean() -> None:
    """批量冲刷：批量入库（单事务）→ 每样本投递 raw 与 clean（key=vehicle_id）。"""
    service, repo, producer = make_ingest_service(telemetry_batch_size=10)
    config = make_settings()

    assert await service.handle(telemetry_payload(), topic="hunter.HUNTER-001.telemetry") is True
    assert await service.handle(telemetry_payload(seq=12581)) is True
    assert service.pending == 2

    flushed = await service.flush()

    assert flushed == 2
    assert repo.commits == 1                     # 批量写入单事务（禁止逐条 commit）
    assert len(repo.rows) == 2
    assert {topic for topic, _, _ in producer.telemetry} == {
        config.telemetry_raw_topic,
        config.telemetry_clean_topic,
    }
    assert all(vehicle_id == "HUNTER-001" for _, _, vehicle_id in producer.telemetry)
    assert service.stats.accepted == 2 and service.stats.dropped == 0
    assert service.pending == 0


async def test_ingest_service_drops_invalid_sample_without_raising() -> None:
    """非法样本（时钟异常）按丢弃处理：不抛异常（不重试、不进 DLQ）。"""
    service, repo, producer = make_ingest_service()
    assert await service.handle(telemetry_payload(timestamp=NOW + 10_000)) is False
    assert service.stats.dropped == 1
    assert service.pending == 0 and repo.rows == [] and producer.telemetry == []


async def test_ingest_service_surfaces_flush_failure_for_offset_retention() -> None:
    """写库失败：flush 必须抛错（消费者批次钩子据此跳过 offset 提交 → 消息重投）。"""
    repo = FakeTelemetryIngestRepository(fail_on_insert=True)
    service, _, producer = make_ingest_service(repo)
    await service.handle(telemetry_payload())
    assert service.pending == 1

    with pytest.raises(RuntimeError):
        await service.flush()

    assert producer.telemetry == []   # 入库失败不得投递（避免下游读到未落库数据）


async def test_ingest_service_buffer_full_triggers_best_effort_flush() -> None:
    """缓冲达到 telemetry_batch_size → 自动冲刷（失败仅告警，由批次钩子兜底）。"""
    repo = FakeTelemetryIngestRepository(fail_on_insert=True)
    service, _, _ = make_ingest_service(repo)   # telemetry_batch_size = 2

    await service.handle(telemetry_payload())
    await service.handle(telemetry_payload(seq=2))

    assert service.pending >= 1      # 冲刷失败 → 数据仍留在缓冲，等待重投
    assert repo.rows == []


async def test_ingest_service_counts_cleaned_samples() -> None:
    """清洗计数：裁剪命中时 stats.cleaned 递增（可观测脏数据比例）。"""
    service, _, _ = make_ingest_service()
    payload = telemetry_payload()
    payload["chassis"]["battery_soc"] = 200
    await service.handle(payload)
    assert service.stats.cleaned == 1
    assert service.stats.accepted == 1


# ---------------------------------------------------------------------------
# 车辆读模型（vehicle:status / vehicle:online:set）
# ---------------------------------------------------------------------------
async def test_status_writer_updates_hash_and_online_set_from_health() -> None:
    """health 消息 → Hash（状态/系统段/心跳）+ 在线集合写入。"""
    redis = FakeRedisHash()
    writer = VehicleStatusWriter(redis)
    payload = load_schema("health")["examples"][0] | {"timestamp": NOW}

    status = await writer.update_from_health(payload, now=NOW)

    assert status == "auto_driving"
    assert redis.sets[ONLINE_SET_KEY] == {"HUNTER-001"}
    stored = redis.hashes[status_key("HUNTER-001")]
    assert stored["cpu_usage"] == "45.2"
    assert stored["last_seen_at"] == f"{NOW:.3f}"
    assert stored["last_seen_seconds"] == "0"


async def test_status_writer_removes_vehicle_when_offline_status_reported() -> None:
    """车端主动上报 offline → 移出在线集合（不等待守护判定）。"""
    redis = FakeRedisHash()
    writer = VehicleStatusWriter(redis)
    await writer.update_from_health(
        {"vehicle_id": "HUNTER-001", "status": "online_idle", "system": {}, "timestamp": NOW},
        now=NOW,
    )
    assert redis.sets[ONLINE_SET_KEY] == {"HUNTER-001"}

    await writer.update_from_health(
        {"vehicle_id": "HUNTER-001", "status": OFFLINE_STATUS, "system": {}, "timestamp": NOW},
        now=NOW,
    )
    assert redis.sets[ONLINE_SET_KEY] == set()
    assert redis.hashes[status_key("HUNTER-001")]["status"] == OFFLINE_STATUS


async def test_status_writer_updates_realtime_fields_from_telemetry() -> None:
    """遥测消息刷新电量/速度/底盘状态字与心跳，但不改写业务状态。"""
    redis = FakeRedisHash()
    writer = VehicleStatusWriter(redis)
    await writer.update_from_health(
        {"vehicle_id": "HUNTER-001", "status": "auto_driving", "system": {}, "timestamp": NOW},
        now=NOW,
    )

    await writer.update_from_telemetry(telemetry_payload(), now=NOW + 1)

    stored = redis.hashes[status_key("HUNTER-001")]
    assert stored["status"] == "auto_driving"      # 业务状态不被遥测覆盖
    assert stored["battery_soc"] == "78"
    assert stored["last_seen_at"] == f"{NOW + 1:.3f}"


async def test_sweeper_marks_stale_vehicle_offline_and_refreshes_live_one() -> None:
    """守护：心跳超阈值 → 离线（SREM + 状态置 offline）；心跳新鲜 → 刷新 last_seen_seconds。"""
    redis = FakeRedisHash()
    writer = VehicleStatusWriter(redis)
    settings = make_settings()
    stale, live = "HUNTER-001", "HUNTER-002"
    for vehicle_id, last_seen in ((stale, NOW - 60), (live, NOW - 2)):
        redis.hashes[status_key(vehicle_id)] = {
            "vehicle_id": vehicle_id,
            "status": "auto_driving",
            "last_seen_at": f"{last_seen:.3f}",
        }
    redis.sets[ONLINE_SET_KEY] = {stale, live}
    sweeper = VehicleStatusSweeper(redis, settings, writer)

    offline = await sweeper.run_once(now=NOW)

    assert offline == [stale]
    assert redis.sets[ONLINE_SET_KEY] == {live}                    # 陈旧车辆被移出在线集合
    assert redis.hashes[status_key(stale)]["status"] == OFFLINE_STATUS
    assert redis.hashes[status_key(live)]["last_seen_seconds"] == "2.000"


async def test_sweeper_marks_vehicle_without_heartbeat_offline() -> None:
    """无 last_seen_at（历史脏数据/首次写入失败）同样判定离线（安全默认）。"""
    redis = FakeRedisHash()
    redis.hashes[status_key("HUNTER-003")] = {"vehicle_id": "HUNTER-003", "status": "online_idle"}
    sweeper = VehicleStatusSweeper(redis, make_settings())

    assert await sweeper.run_once(now=NOW) == ["HUNTER-003"]


async def test_sweeper_start_stop_leaks_no_task() -> None:
    """守护任务可重复启停且无任务泄漏（停机 await 收尾）。"""
    redis = FakeRedisHash()
    sweeper = VehicleStatusSweeper(redis, make_settings(vehicle_offline_sweep_interval_seconds=1))

    await sweeper.start()
    await sweeper.start()          # 幂等
    assert sweeper._task is not None
    await sweeper.stop()
    assert sweeper._task is None
    assert sweeper._running is False


# ---------------------------------------------------------------------------
# 消费者（消费组契约对齐 + 幂等键 + 处理入口）
# ---------------------------------------------------------------------------
def test_consumer_groups_and_patterns_match_contract() -> None:
    """消费组名与订阅模式必须与 consumer-groups.yaml 完全一致（禁止自造组名）。"""
    groups = {
        group["group_id"]: group
        for group in yaml.safe_load(
            (ROOT / "contracts" / "kafka" / "consumer-groups.yaml").read_text(encoding="utf-8")
        )["groups"]
    }

    expected = {
        TelemetryIngestConsumer.group_id: (["hunter.*.telemetry"], ["telemetry_raw", "telemetry_clean"]),
        HealthIngestConsumer.group_id: (["hunter.*.health"], []),
        EventIngestConsumer.group_id: (["hunter.*.event"], ["event_raw"]),
    }
    for group_id, (subscribes, produces) in expected.items():
        assert group_id in groups, f"消费组 {group_id} 未登记契约"
        assert groups[group_id]["service"] == "data-collector"
        assert groups[group_id]["subscribes"] == subscribes
        assert groups[group_id]["produces"] == produces


def test_telemetry_consumer_uses_seq_as_idempotency_key() -> None:
    """遥测幂等键：有 seq 用 (vehicle_id, seq)，缺失时退化为 (vehicle_id, timestamp)。"""
    message = SimpleNamespace()  # 幂等键函数不读取 message（仅用 value）
    with_seq = TelemetryIngestConsumer.idempotency_key(message, {"vehicle_id": "HUNTER-001", "seq": 7})
    without_seq = TelemetryIngestConsumer.idempotency_key(
        message, {"vehicle_id": "HUNTER-001", "seq": None, "timestamp": NOW}
    )
    another_vehicle = TelemetryIngestConsumer.idempotency_key(
        message, {"vehicle_id": "HUNTER-002", "seq": 7}
    )
    assert with_seq == IdempotencyGuard.compose("HUNTER-001", 7)
    assert without_seq == IdempotencyGuard.compose("HUNTER-001", NOW)
    assert with_seq != another_vehicle


def test_event_consumer_idempotency_key_matches_unique_index() -> None:
    """事件幂等键 = (vehicle_id, event_type, event_time)（与 DDL 唯一索引一致）。"""
    key = EventIngestConsumer.idempotency_key(
        SimpleNamespace(),
        {"vehicle_id": "HUNTER-001", "event_type": "harsh_braking", "timestamp": NOW},
    )
    assert key == IdempotencyGuard.compose("HUNTER-001", "harsh_braking", NOW)


async def test_telemetry_consumer_processes_message_and_updates_read_model() -> None:
    """遥测消费者：入库缓冲 + 读模型刷新（心跳/电量）。"""
    service, repo, producer = make_ingest_service()
    redis = FakeRedisHash()
    consumer = TelemetryIngestConsumer(
        make_settings(), service, status_writer=VehicleStatusWriter(redis)
    )

    await consumer.process(telemetry_payload(), topic="hunter.HUNTER-001.telemetry")

    assert service.pending == 1                    # 未达批次阈值，等待批次收尾冲刷
    assert redis.sets[ONLINE_SET_KEY] == {"HUNTER-001"}
    assert redis.hashes[status_key("HUNTER-001")]["battery_soc"] == "78"
    assert repo.rows == [] and producer.telemetry == []


async def test_telemetry_consumer_skips_read_model_for_dropped_sample() -> None:
    """被丢弃的样本不得刷新读模型（避免脏数据把车辆"伪装"成在线）。"""
    service, _, _ = make_ingest_service()
    redis = FakeRedisHash()
    consumer = TelemetryIngestConsumer(
        make_settings(), service, status_writer=VehicleStatusWriter(redis)
    )

    await consumer.process(
        telemetry_payload(timestamp=NOW + 10_000), topic="hunter.HUNTER-001.telemetry"
    )

    assert redis.sets == {}
    assert service.stats.dropped == 1


async def test_health_consumer_writes_read_model() -> None:
    """健康消费者：写 vehicle:status + 在线集合（不落库、不投递）。"""
    redis = FakeRedisHash()
    consumer = HealthIngestConsumer(make_settings(), VehicleStatusWriter(redis))

    await consumer.process(
        load_schema("health")["examples"][0] | {"timestamp": NOW},
        topic="hunter.HUNTER-001.health",
    )

    assert redis.hashes[status_key("HUNTER-001")]["status"] == "auto_driving"
    assert redis.sets[ONLINE_SET_KEY] == {"HUNTER-001"}


async def test_event_consumer_persists_and_publishes_event_raw() -> None:
    """事件消费者：等级校验 → 幂等落库 → 投递 event_raw。"""
    repository = FakeEventIngestRepository()
    producer = FakePipelineProducer()
    consumer = EventIngestConsumer(make_settings(), repository, producer)
    payload = event_payload()

    await consumer.process(payload, topic="hunter.HUNTER-001.event")

    assert repository.commits == 1
    assert len(repository.rows) == 1
    row = repository.rows[0]
    assert row["vehicle_id"] == "HUNTER-001"
    assert row["event_time"].timestamp() == pytest.approx(payload["timestamp"], abs=1e-3)
    assert producer.events == [(payload, "HUNTER-001")]


async def test_event_consumer_rejects_level_mismatch_for_dlq() -> None:
    """event_level 与事件类型契约等级不一致 → 抛错（重试耗尽后 DLQ，禁止静默改写）。"""
    repository = FakeEventIngestRepository()
    consumer = EventIngestConsumer(make_settings(), repository, FakePipelineProducer())
    payload = event_payload()
    expected_level = payload["event_level"]
    payload["event_level"] = "info" if expected_level != "info" else "critical"

    with pytest.raises(ValueError, match="event_level"):
        await consumer.process(payload, topic="hunter.HUNTER-001.event")

    assert repository.rows == []
