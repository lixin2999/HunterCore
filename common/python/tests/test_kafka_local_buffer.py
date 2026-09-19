"""本地磁盘缓冲（``hunter_common.kafka.buffer``）单测：落盘 / 容量淘汰 / 重投 / 崩溃恢复。

覆盖契约要求：网络中断时最多缓冲 1GB（``topics.yaml#producer_defaults``）、
重投 at-least-once 且失败保留剩余记录（失败即停，保证分区内顺序）。
"""
from __future__ import annotations

import base64
from pathlib import Path

from hunter_common.kafka.buffer import BufferedRecord, LocalDiskBuffer
from hunter_common.metrics import REGISTRY

SERVICE = "data-collector"


def record(index: int, topic: str = "telemetry_raw") -> BufferedRecord:
    return BufferedRecord(
        topic=topic,
        value=f'{{"seq":{index}}}'.encode(),
        key=b"HUNTER-001",
        headers=(("origin", b"l5"),),
    )


def make_buffer(tmp_path: Path, *, max_bytes: int = 1_000_000, segment_max_records: int = 1000) -> LocalDiskBuffer:
    return LocalDiskBuffer(
        tmp_path, service=SERVICE, max_bytes=max_bytes, segment_max_records=segment_max_records
    )


def segments_of(buffer: LocalDiskBuffer) -> list[Path]:
    return sorted(buffer.directory.glob("seg-*.jsonl"))


def dropped_total() -> float:
    return REGISTRY.get_sample_value(
        "hunter_kafka_local_buffer_dropped_total", {"service": SERVICE}
    ) or 0.0


def test_append_writes_jsonl_and_reports_stats(tmp_path: Path) -> None:
    buffer = make_buffer(tmp_path)
    for index in range(3):
        buffer.append(record(index))

    stats = buffer.stats()
    assert stats.message_count == 3
    assert stats.bytes_size > 0
    assert stats.segment_count == 1
    assert len(segments_of(buffer)[0].read_bytes().splitlines()) == 3
    assert REGISTRY.get_sample_value(
        "hunter_kafka_local_buffer_messages", {"service": SERVICE}
    ) == 3.0


def test_restart_restores_pending_records(tmp_path: Path) -> None:
    """进程重启后未投递消息继续保留（不清空缓冲）。"""
    make_buffer(tmp_path).append(record(1))
    make_buffer(tmp_path).append(record(2))
    restored = make_buffer(tmp_path)
    assert restored.stats().message_count == 2


def test_over_limit_evicts_oldest_segments_until_fit(tmp_path: Path) -> None:
    """超限后整段淘汰最旧数据（FIFO）并累计丢弃指标；最新数据必须保留。"""
    before = dropped_total()
    buffer = make_buffer(tmp_path, max_bytes=400, segment_max_records=1)
    for index in range(6):
        buffer.append(record(index))

    stats = buffer.stats()
    assert stats.bytes_size <= 400  # 淘汰后水位回落到上限内
    assert stats.segment_count < 6  # 已淘汰最旧段
    assert stats.message_count == stats.segment_count  # 每段 1 条（segment_max_records=1）
    assert dropped_total() > before
    newest = max(segments_of(buffer), key=lambda path: path.name).read_bytes()
    # 值以 base64 存储（字节级保真），断言最新记录未被抹除
    assert base64.b64encode(b'{"seq":5}') in newest


def test_single_segment_overflow_is_kept(tmp_path: Path) -> None:
    """仅剩单段且超限时不丢弃（避免刚写入数据被立即抹除）。"""
    buffer = make_buffer(tmp_path, max_bytes=10, segment_max_records=10)
    buffer.append(record(1))
    assert buffer.stats().message_count == 1


async def test_replay_delivers_all_and_clears(tmp_path: Path) -> None:
    buffer = make_buffer(tmp_path, segment_max_records=1)
    for index in range(3):
        buffer.append(record(index))

    delivered: list[BufferedRecord] = []

    async def send(item: BufferedRecord) -> None:
        delivered.append(item)

    replayed = await buffer.replay(send)
    assert replayed == 3
    assert [item.value for item in delivered] == [b'{"seq":0}', b'{"seq":1}', b'{"seq":2}']
    assert buffer.stats().message_count == 0
    assert segments_of(buffer) == []
    assert (REGISTRY.get_sample_value(
        "hunter_kafka_local_buffer_replayed_total", {"service": SERVICE}
    ) or 0) >= 3


async def test_replay_stops_on_failure_and_keeps_remainder(tmp_path: Path) -> None:
    """首条失败即停：失败记录及其后记录保留，已成功部分不重复投递。"""
    buffer = make_buffer(tmp_path, segment_max_records=1)
    for index in range(3):
        buffer.append(record(index))

    attempts: list[int] = []

    async def send(item: BufferedRecord) -> None:
        attempts.append(len(attempts))
        if len(attempts) == 2:  # 第 2 条投递失败
            raise RuntimeError("链路仍不可用")

    assert await buffer.replay(send) == 1
    assert buffer.stats().message_count == 2

    retried: list[bytes] = []

    async def send_ok(item: BufferedRecord) -> None:
        retried.append(item.value)

    assert await buffer.replay(send_ok) == 2
    assert retried == [b'{"seq":1}', b'{"seq":2}']
    assert buffer.stats().message_count == 0


async def test_replay_skips_corrupt_line(tmp_path: Path) -> None:
    """崩溃残留的损坏行被跳过，不阻塞其余消息重投。"""
    buffer = make_buffer(tmp_path)
    buffer.append(record(1))
    segment = segments_of(buffer)[0]
    with segment.open("ab") as handle:  # 模拟半行写入
        handle.write(b'{"t":"telemetry_raw"')

    delivered: list[bytes] = []

    async def send(item: BufferedRecord) -> None:
        delivered.append(item.value)

    assert await buffer.replay(send) == 1
    assert delivered == [b'{"seq":1}']
    assert segments_of(buffer) == []


async def test_replay_respects_max_records(tmp_path: Path) -> None:
    buffer = make_buffer(tmp_path, segment_max_records=1)
    for index in range(3):
        buffer.append(record(index))

    async def send(_item: BufferedRecord) -> None:
        return None

    assert await buffer.replay(send, max_records=2) == 2
    assert buffer.stats().message_count == 1


def test_recover_incomplete_segment_from_tmp(tmp_path: Path) -> None:
    """重投过程中崩溃：``.tmp`` 残留被还原为分段文件。"""
    directory = tmp_path / SERVICE
    directory.mkdir(parents=True, exist_ok=True)
    tmp_file = directory / "seg-0000000000001-abcdef12.jsonl.tmp"
    tmp_file.write_bytes(b'{"t":"telemetry_raw","v":"e30=","k":null,"h":[],"ts":1.0}\n')

    buffer = make_buffer(tmp_path)
    assert not tmp_file.exists()
    assert buffer.stats().message_count == 1
