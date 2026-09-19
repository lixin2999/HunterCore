"""生产者本地磁盘缓冲（契约 ``topics.yaml#producer_defaults.local_disk_buffer_bytes = 1073741824``）。

网络中断 / broker 长时间不可用时，投递失败的消息转为本地磁盘 JSONL 分段落盘，
待链路恢复后由 :meth:`LocalDiskBuffer.replay` 重投（at-least-once，消费侧保证幂等）。

设计约束：
- 单条记录完整性：逐行 JSONL，写入后 flush + fsync（进程崩溃只影响最后一行，重放按行容错跳过）；
- 容量上限：超过 ``max_bytes``（默认 1GB）时**整段淘汰最旧**（FIFO），累加丢弃指标 + WARNING 日志；
  唯一剩余段即使超限也不丢弃（避免刚写入的数据被立即抹掉），改为 CRITICAL 提示人工介入；
- 重投崩溃安全：重投后原子重写剩余记录（``os.replace``），已成功部分不会被重复投递；
- **异步纪律（审查 R4）**：本模块公开方法为同步阻塞实现（open/write/fsync/read_bytes/os.replace），
  禁止在事件循环内直接调用；生产者侧经 ``asyncio.to_thread`` 转投递（见 ``producer.produce`` /
  ``replay_buffered``），``replay`` 内部的文件 IO 同样下沉线程池（异步优先约束）。
"""
from __future__ import annotations

import asyncio
import base64
import json
import os
import threading
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from hunter_common.kafka import metrics as kafka_metrics
from hunter_common.logging import get_logger

logger = get_logger("hunter_common.kafka.buffer")

#: 单段最大记录数（达到后滚动新段；段越小淘汰粒度越细，但文件数越多）
DEFAULT_SEGMENT_MAX_RECORDS = 1000


@dataclass(frozen=True, slots=True)
class BufferedRecord:
    """缓冲中的一条待投递消息（字节级保留 key/value/headers；value=None 为 Kafka 墓碑消息）。"""

    topic: str
    value: bytes | None = None
    key: bytes | None = None
    headers: tuple[tuple[str, bytes], ...] = ()
    buffered_at: float = 0.0


@dataclass(frozen=True, slots=True)
class BufferStats:
    """缓冲水位（写入 Prometheus Gauge 的原始数据）。"""

    message_count: int
    bytes_size: int
    segment_count: int


#: 重投函数签名：投递一条记录（失败需抛出异常，缓冲保留该条及其后记录）
ReplaySender = Callable[[BufferedRecord], Awaitable[None]]


class LocalDiskBuffer:
    """本地磁盘缓冲（线程安全：生产者 poll 线程与事件循环均可能触发淘汰）。"""

    def __init__(
        self,
        directory: str | Path,
        *,
        service: str,
        max_bytes: int,
        segment_max_records: int = DEFAULT_SEGMENT_MAX_RECORDS,
    ) -> None:
        self._root = Path(directory) / service
        self._root.mkdir(parents=True, exist_ok=True)
        self._service = service
        self._max_bytes = max(int(max_bytes), 1)
        self._segment_max_records = max(int(segment_max_records), 1)
        self._lock = threading.Lock()
        self._segment_path: Path | None = None
        self._segment_records = 0
        self._message_count = 0
        self._bytes_size = 0
        self._recover_incomplete_segments()
        self._scan_existing()
        self._refresh_metrics()

    # ---------- 公共 API ----------

    @property
    def directory(self) -> Path:
        return self._root

    def append(self, record: BufferedRecord) -> None:
        """落盘一条消息（含容量淘汰）；水位同步刷新到 Prometheus。

        阻塞实现（open/flush/fsync + 目录扫描）：**禁止在事件循环内直接调用**，
        异步调用方必须经 ``asyncio.to_thread``（审查 R4：异步优先约束）。
        """
        with self._lock:
            stamped = record if record.buffered_at else BufferedRecord(
                topic=record.topic,
                value=record.value,
                key=record.key,
                headers=record.headers,
                buffered_at=time.time(),
            )
            path = self._current_segment()
            line = _encode_line(stamped)
            with path.open("ab") as handle:
                handle.write(line)
                handle.flush()
                os.fsync(handle.fileno())  # 断网场景：确保缓冲真实落盘而非停留在页缓存
            self._segment_records += 1
            self._message_count += 1
            self._bytes_size += len(line)
            self._evict_if_needed()
            self._refresh_metrics()

    async def replay(self, send: ReplaySender, *, max_records: int | None = None) -> int:
        """按最旧优先重投缓冲消息，返回成功投递条数。

        首条失败即停止（保持分区内顺序）：失败记录及其后记录保留在磁盘，
        已成功的记录从文件中原子移除（下次重投只处理剩余部分）。
        """
        replayed = 0
        # 目录扫描（glob）为阻塞 IO：下沉线程池（审查 R4，禁止阻塞事件循环）
        segments = await asyncio.to_thread(self._segments)
        for segment in segments:
            if max_records is not None and replayed >= max_records:
                break
            sent, drained = await self._replay_segment(segment, send, max_records)
            replayed += sent
            if not drained:
                # 投递失败或已达批量上限：剩余记录保留，停止本轮重投（保持分区内顺序）
                break
        if replayed:
            kafka_metrics.record_buffer_replayed(self._service, replayed)
            await asyncio.to_thread(self._refresh_metrics)
        return replayed

    def stats(self) -> BufferStats:
        """当前缓冲水位（重投期间也可安全读取）。

        阻塞实现（目录 glob）：异步调用方经 ``asyncio.to_thread``（审查 R4）。
        """
        return BufferStats(
            message_count=self._message_count,
            bytes_size=self._bytes_size,
            segment_count=len(self._segments()),
        )

    def clear(self) -> None:
        """清空缓冲（仅测试与人工运维使用；生产禁止在缓冲非空时调用）。"""
        with self._lock:
            for path in self._root.glob("*.jsonl"):
                path.unlink(missing_ok=True)
            self._segment_path = None
            self._segment_records = 0
            self._message_count = 0
            self._bytes_size = 0
            self._refresh_metrics()

    # ---------- 内部：重投 ----------

    async def _replay_segment(
        self, segment: Path, send: ReplaySender, max_records: int | None
    ) -> tuple[int, bool]:
        """重投单段；返回 ``(成功条数, 是否已处理完本段)``，并原子重写未处理部分（崩溃安全）。"""
        # 整段读入为阻塞 IO：下沉线程池（段大小受 segment_max_records 约束）
        raw = await asyncio.to_thread(segment.read_bytes)
        lines = raw.splitlines(keepends=True)
        sent = 0
        processed = 0
        remainder: list[bytes] = []
        for index, raw_line in enumerate(lines):
            if max_records is not None and sent >= max_records:
                remainder = lines[index:]
                processed = index
                break
            record = _decode_line(raw_line)
            if record is None:  # 损坏行（崩溃残留/磁盘故障）：跳过，不阻塞后续消息
                logger.warning("kafka_local_buffer_corrupt_line_skipped", segment=segment.name)
                continue
            try:
                await send(record)
            except Exception:  # noqa: BLE001 - 任何投递失败都必须停止重投并保留剩余记录
                logger.warning(
                    "kafka_local_buffer_replay_interrupted",
                    segment=segment.name,
                    sent=sent,
                    hint="投递失败，失败记录及其后记录保留待下次重投",
                )
                remainder = lines[index:]
                processed = index
                break
            sent += 1
        else:
            processed = len(lines)
        # 结果落盘（原子重写/删除）为阻塞 IO：下沉线程池（审查 R4）
        await asyncio.to_thread(self._finalize_segment, segment, remainder, processed)
        return sent, not remainder

    def _finalize_segment(self, segment: Path, remainder: list[bytes], processed: int) -> None:
        """重投结果落盘：全部处理完 → 删除分段；否则原子重写剩余记录。"""
        with self._lock:
            size_before = segment.stat().st_size if segment.exists() else 0
            if remainder:
                tmp = Path(f"{segment}.tmp")
                tmp.write_bytes(b"".join(remainder))
                os.replace(tmp, segment)
                size_after = segment.stat().st_size
                self._bytes_size = max(self._bytes_size - (size_before - size_after), 0)
            else:
                segment.unlink(missing_ok=True)
                if self._segment_path == segment:
                    self._segment_path = None
                    self._segment_records = 0
                self._bytes_size = max(self._bytes_size - size_before, 0)
            self._message_count = max(self._message_count - processed, 0)
            self._refresh_metrics()

    # ---------- 内部：恢复与统计 ----------

    def _recover_incomplete_segments(self) -> None:
        """恢复崩溃残留：``seg-*.jsonl.tmp`` 对应的分段已不存在时用临时文件还原。"""
        for tmp in self._root.glob("seg-*.jsonl.tmp"):
            target = Path(str(tmp)[: -len(".tmp")])
            try:
                if target.exists():
                    tmp.unlink(missing_ok=True)
                else:
                    os.replace(tmp, target)
                    logger.warning("kafka_local_buffer_segment_recovered", segment=target.name)
            except OSError:  # pragma: no cover - 磁盘异常
                logger.exception("kafka_local_buffer_recover_failed", segment=tmp.name)

    def _scan_existing(self) -> None:
        """启动时统计既有缓冲（进程重启后未投递消息继续保留，不清空）。"""
        segments = self._segments()
        self._message_count = 0
        self._bytes_size = 0
        for segment in segments:
            self._bytes_size += segment.stat().st_size
            self._message_count += self._count_lines(segment)
        if segments:
            self._segment_path = segments[-1]
            # 末段可能已有内容：按记录数续写，避免已满段继续追加
            self._segment_records = min(
                self._count_lines(segments[-1]), self._segment_max_records
            )
        if self._message_count:
            logger.warning(
                "kafka_local_buffer_restored",
                directory=str(self._root),
                messages=self._message_count,
                bytes=self._bytes_size,
            )

    @staticmethod
    def _count_lines(path: Path) -> int:
        try:
            return sum(1 for _ in path.open("rb"))
        except OSError:  # pragma: no cover - 磁盘异常
            return 0

    def _refresh_metrics(self) -> None:
        kafka_metrics.record_buffer_stats(self._service, self._message_count, self._bytes_size)

    # ---------- 内部：分段与淘汰 ----------

    def _segments(self) -> list[Path]:
        """按时间升序返回分段文件（文件名前缀为毫秒时间戳，字典序即时间序）。"""
        return sorted(self._root.glob("seg-*.jsonl"))

    def _current_segment(self) -> Path:
        if (
            self._segment_path is None
            or self._segment_records >= self._segment_max_records
            or not self._segment_path.exists()
        ):
            stamp = int(time.time() * 1000)
            self._segment_path = self._root / f"seg-{stamp:013d}-{uuid.uuid4().hex[:8]}.jsonl"
            self._segment_path.touch()
            self._segment_records = 0
        return self._segment_path

    def _evict_if_needed(self) -> None:
        """超限淘汰：整段删除最旧（保留最后一段），丢弃条数计入指标并 WARNING 告警。"""
        segments = self._segments()
        while self._bytes_size > self._max_bytes and len(segments) > 1:
            oldest = segments.pop(0)
            dropped = self._count_lines(oldest)
            size = oldest.stat().st_size
            oldest.unlink(missing_ok=True)
            self._message_count = max(self._message_count - dropped, 0)
            self._bytes_size = max(self._bytes_size - size, 0)
            kafka_metrics.record_buffer_dropped(self._service, dropped)
            logger.warning(
                "kafka_local_buffer_evicted",
                segment=oldest.name,
                dropped_messages=dropped,
                buffer_bytes=self._bytes_size,
                max_bytes=self._max_bytes,
            )
        if self._bytes_size > self._max_bytes:
            logger.critical(
                "kafka_local_buffer_overflow_single_segment",
                buffer_bytes=self._bytes_size,
                max_bytes=self._max_bytes,
                hint="缓冲仅剩单段且已超限：不再丢弃（防止刚写入数据被抹除），需人工扩容",
            )


# ---------- 行编解码（字节级保真：key/value/headers 统一 base64） ----------

def _b64(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _unb64(value: str) -> bytes:
    return base64.b64decode(value.encode("ascii"))


def _encode_line(record: BufferedRecord) -> bytes:
    """单行 JSONL：``t`` topic / ``v`` value / ``k`` key / ``h`` headers / ``ts`` 缓冲时刻。"""
    payload: dict[str, Any] = {
        "t": record.topic,
        "v": _b64(record.value) if record.value is not None else None,
        "k": _b64(record.key) if record.key is not None else None,
        "h": [[name, _b64(value)] for name, value in record.headers],
        "ts": record.buffered_at,
    }
    return (json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")


def _decode_line(raw_line: bytes) -> BufferedRecord | None:
    """解析一行缓冲记录；损坏/非法行返回 None（由调用方跳过并记录日志）。"""
    text = raw_line.strip()
    if not text:
        return None
    try:
        payload = json.loads(text)
        return BufferedRecord(
            topic=str(payload["t"]),
            value=_unb64(str(payload["v"])) if payload.get("v") is not None else None,
            key=_unb64(str(payload["k"])) if payload.get("k") is not None else None,
            headers=tuple((str(name), _unb64(str(value))) for name, value in payload.get("h", [])),
            buffered_at=float(payload.get("ts", 0.0)),
        )
    except (KeyError, ValueError, TypeError, json.JSONDecodeError):
        return None

