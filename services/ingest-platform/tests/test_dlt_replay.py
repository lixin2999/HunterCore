"""DLT 死信重放工具单元测试（§5.3/§12.3，审查功能遗漏收口）。

覆盖：目标提取 / 记录过滤 / 重放 key 派生 / 消息解码 纯函数，
以及 DltReplayEngine 扫描-重放主循环（真实/替身消费者与生产者）、dry-run、限量、失败计数。
"""
from __future__ import annotations

import json
from typing import Any
from unittest.mock import patch

from app.config import Settings
from app.consumers.base import BaseIngestConsumer
from app.producers.platform_events import PlatformEventProducer
from app.services.cmd_service import CmdService
from app.tools.dlt_replay import (
    DltReplayEngine,
    ReplayFilter,
    decode_record,
    extract_replay_target,
    record_matches,
    replay_key,
)

# ---------- 替身 ----------

class _FakeMessage:
    def __init__(self, value: bytes | None, *, error: Any = None) -> None:
        self._value = value
        self._error = error

    def error(self) -> Any:
        return self._error

    def value(self) -> bytes | None:
        return self._value


class _FakeConsumer:
    """预置批次队列的消费替身：逐批返回，耗尽后返回空列表（触发 idle 退出）。"""

    def __init__(self, batches: list[list[_FakeMessage]]) -> None:
        self._batches: list[list[_FakeMessage]] = list(batches)
        self.subscribed: list[str] = []
        self.commit_count = 0
        self.closed = False

    def subscribe(self, topics: list[str]) -> None:
        self.subscribed = list(topics)

    def consume(self, num: int, timeout: float) -> list[_FakeMessage]:
        return self._batches.pop(0) if self._batches else []

    def commit(self) -> None:
        self.commit_count += 1

    def close(self) -> None:
        self.closed = True


class _FakeProducer:
    def __init__(self, *, fail: bool = False) -> None:
        self.produced: list[tuple[str, str | None, Any]] = []
        self._fail = fail

    async def produce(self, topic: str, *, key: Any = None, value: Any = None) -> None:
        if self._fail:
            raise RuntimeError("kafka down")
        self.produced.append((topic, key, value))


def _dlt_message_bytes(
    original_topic: str = "vehicle.bag.manifest",
    *,
    vehicle_id: str = "veh-A1",
    error_type: str = "ValueError",
    raw_repr: bool = False,
    offset: int = 42,
) -> bytes:
    envelope = {"event_id": "e1", "vehicle_id": vehicle_id, "payload": {}}
    record = {
        "original_topic": original_topic,
        "original_partition": 0,
        "original_offset": offset,
        "consumer_group": "g.ingest.manifest",
        "error_type": error_type,
        "error_message": "boom",
        "message": {"_raw_repr": "not-json"} if raw_repr else envelope,
        "failed_at": "2026-01-01T00:00:00+00:00",
    }
    return json.dumps(record).encode("utf-8")


def _make_engine(
    batches: list[list[_FakeMessage]],
    settings: Any,
    *,
    producer: _FakeProducer | None = None,
    dry_run: bool = False,
) -> tuple[DltReplayEngine, _FakeConsumer, _FakeProducer]:
    consumer = _FakeConsumer(batches)
    prod = producer or _FakeProducer()
    engine = DltReplayEngine(
        settings, consumer=consumer, producer=prod,  # type: ignore[arg-type]
        dry_run=dry_run, idle_rounds=1,
    )
    return engine, consumer, prod


# ---------- 纯函数 ----------

class TestExtractTarget:
    def test_valid(self) -> None:
        rec = {"original_topic": "t", "message": {"vehicle_id": "v"}}
        assert extract_replay_target(rec) == ("t", {"vehicle_id": "v"})

    def test_missing_topic(self) -> None:
        assert extract_replay_target({"message": {}}) is None

    def test_raw_repr_not_replayable(self) -> None:
        assert extract_replay_target({"original_topic": "t", "message": {"_raw_repr": "x"}}) is None

    def test_non_dict(self) -> None:
        assert extract_replay_target(["a"]) is None


class TestRecordMatches:
    def test_topic_filter(self) -> None:
        rec = {"original_topic": "a", "error_type": "E"}
        assert record_matches(rec, ReplayFilter(original_topic="a"))
        assert not record_matches(rec, ReplayFilter(original_topic="b"))

    def test_error_type_filter(self) -> None:
        rec = {"original_topic": "a", "error_type": "ValueError"}
        assert record_matches(rec, ReplayFilter(error_type="ValueError"))
        assert not record_matches(rec, ReplayFilter(error_type="TypeError"))

    def test_since_window(self) -> None:
        from datetime import UTC, datetime

        rec = {"failed_at": "2026-01-02T00:00:00+00:00"}
        lo = datetime(2026, 1, 1, tzinfo=UTC)
        hi = datetime(2026, 1, 3, tzinfo=UTC)
        assert record_matches(rec, ReplayFilter(since=lo, until=hi))
        assert not record_matches(rec, ReplayFilter(since=datetime(2026, 1, 5, tzinfo=UTC)))

    def test_unparseable_ts_passes(self) -> None:
        from datetime import UTC, datetime

        rec = {"failed_at": "not-a-date"}
        assert record_matches(rec, ReplayFilter(since=datetime(2030, 1, 1, tzinfo=UTC)))


class TestReplayKey:
    def test_prefers_vehicle_id(self) -> None:
        assert replay_key({"vehicle_id": "veh-9"}, {"original_offset": 1}) == "veh-9"

    def test_falls_back_to_offset(self) -> None:
        assert replay_key({}, {"original_offset": 7}) == "7"

    def test_empty(self) -> None:
        assert replay_key({}, {}) == ""


class TestDecodeRecord:
    def test_valid(self) -> None:
        msg = _FakeMessage(b'{"a": 1}')
        assert decode_record(msg) == {"a": 1}  # type: ignore[arg-type]

    def test_invalid_json(self) -> None:
        assert decode_record(_FakeMessage(b"{bad")) is None

    def test_non_object(self) -> None:
        assert decode_record(_FakeMessage(b"[1,2]")) is None

    def test_none_value(self) -> None:
        assert decode_record(_FakeMessage(None)) is None


# ---------- 引擎主循环 ----------

class TestEngineRun:
    async def test_replays_valid_skips_raw(self, test_settings: Any) -> None:
        batch = [
            _FakeMessage(_dlt_message_bytes(vehicle_id="veh-A")),
            _FakeMessage(_dlt_message_bytes(vehicle_id="veh-B")),
            _FakeMessage(_dlt_message_bytes(raw_repr=True)),
        ]
        engine, consumer, prod = _make_engine([batch, []], test_settings)
        report = await engine.run()
        assert report.scanned == 3
        assert report.matched == 2
        assert report.replayed == 2
        assert report.skipped == 1
        assert report.failed == 0
        assert consumer.subscribed == [test_settings.dlt_topic]
        assert consumer.closed is True
        # 每条重放 value 为 JSON 字符串（低层 produce 只接受 bytes/str），key=vehicle_id
        topics = {t for t, _, _ in prod.produced}
        assert topics == {"vehicle.bag.manifest"}
        for _topic, key, value in prod.produced:
            assert isinstance(value, str)
            assert json.loads(value) == {"event_id": "e1", "vehicle_id": key, "payload": {}}

    async def test_dry_run_does_not_produce(self, test_settings: Any) -> None:
        batch = [_FakeMessage(_dlt_message_bytes()), _FakeMessage(_dlt_message_bytes())]
        engine, _consumer, prod = _make_engine([batch, []], test_settings, dry_run=True)
        report = await engine.run()
        assert report.matched == 2
        assert report.replayed == 0
        assert prod.produced == []

    async def test_topic_filter(self, test_settings: Any) -> None:
        batch = [
            _FakeMessage(_dlt_message_bytes("vehicle.bag.manifest")),
            _FakeMessage(_dlt_message_bytes("vehicle.agent.alert")),
        ]
        engine, _consumer, prod = _make_engine([batch, []], test_settings)
        report = await engine.run(ReplayFilter(original_topic="vehicle.agent.alert"))
        assert report.matched == 1
        assert report.skipped == 1
        assert [t for t, _, _ in prod.produced] == ["vehicle.agent.alert"]

    async def test_max_records_stops(self, test_settings: Any) -> None:
        batch = [_FakeMessage(_dlt_message_bytes()) for _ in range(5)]
        engine, _consumer, _prod = _make_engine([batch, []], test_settings)
        report = await engine.run(ReplayFilter(max_records=2))
        assert report.matched == 2

    async def test_produce_failure_counted(self, test_settings: Any) -> None:
        batch = [_FakeMessage(_dlt_message_bytes())]
        engine, _consumer, _prod = _make_engine(
            [batch, []], test_settings, producer=_FakeProducer(fail=True)
        )
        report = await engine.run()
        assert report.matched == 1
        assert report.replayed == 0
        assert report.failed == 1

    async def test_kafka_error_message_ignored(self, test_settings: Any) -> None:
        batch = [_FakeMessage(None, error=object()), _FakeMessage(_dlt_message_bytes())]
        engine, _consumer, _prod = _make_engine([batch, []], test_settings)
        report = await engine.run()
        # 错误消息不计入 scanned（早退），仅第二条被扫描重放
        assert report.scanned == 1
        assert report.replayed == 1


class _RouteMessage:
    """适配 _route_dlt 需要的 topic/partition/offset 访问器。"""

    def __init__(self, topic: str, partition: int, offset: int) -> None:
        self._t, self._p, self._o = topic, partition, offset

    def topic(self) -> str:
        return self._t

    def partition(self) -> int:
        return self._p

    def offset(self) -> int:
        return self._o


class _CaptureProducer:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def produce(self, topic: str, *, key: Any = None, value: Any = None) -> None:
        self.calls.append({"topic": topic, "key": key, "value": value})


class TestProduceSerializationFix:
    """回归：DLT/cmd/platform 事件低层 produce 必须收到 JSON 字符串（非 dict）。"""

    async def test_route_dlt_produces_json_str(self, test_settings: Settings) -> None:
        consumer = object.__new__(BaseIngestConsumer)
        consumer._settings = test_settings  # type: ignore[attr-defined]
        consumer.group_id = "g.ingest.manifest"  # type: ignore[attr-defined]
        prod = _CaptureProducer()
        msg = _RouteMessage("vehicle.bag.manifest", 0, 9)
        with patch("app.consumers.base.KafkaProducerManager.instance", return_value=prod):
            await consumer._route_dlt(msg, {"event_id": "e1"}, ValueError("boom"))
        assert prod.calls[0]["topic"] == test_settings.dlt_topic
        value = prod.calls[0]["value"]
        assert isinstance(value, str)
        rec = json.loads(value)
        assert rec["original_topic"] == "vehicle.bag.manifest"
        assert rec["error_type"] == "ValueError"
        assert rec["message"] == {"event_id": "e1"}

    async def test_publish_cmd_produces_json_str(self, test_settings: Settings) -> None:
        svc = object.__new__(CmdService)
        svc._settings = test_settings  # type: ignore[attr-defined]
        prod = _CaptureProducer()
        with patch("app.services.cmd_service.KafkaProducerManager.instance", return_value=prod):
            await svc._publish_cmd("cmd-1", "veh-A", "cleanup", {"x": 1})
        assert prod.calls[0]["topic"] == "vehicle.agent.cmd"
        assert prod.calls[0]["key"] == "veh-A"
        value = prod.calls[0]["value"]
        assert isinstance(value, str)
        rec = json.loads(value)
        assert rec["vehicle_id"] == "veh-A"
        assert "signature" in rec

    async def test_platform_event_produces_json_str(self) -> None:
        prod = _CaptureProducer()
        producer_obj = PlatformEventProducer()
        with patch(
            "app.producers.platform_events.KafkaProducerManager.instance", return_value=prod
        ):
            await producer_obj.publish_derived_ready(
                "bag-1", "rev-1", kind="table", object_keys=["k1"], count=1
            )
        value = prod.calls[0]["value"]
        assert isinstance(value, str)
        assert json.loads(value)["bag_id"] == "bag-1"
