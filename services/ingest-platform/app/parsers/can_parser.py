"""CAN 帧解析器（can_msgs/Frame，设计文档 §2 分流表）。

解码 CAN ID + 数据字节，按 DBC 信号配置展开物理值，输出 CSV/Parquet 行。
"""
from __future__ import annotations

from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from typing import Any

from hunter_common.logging import get_logger

from app.parsers.rosbag_reader import RawMessage

logger = get_logger("app.parsers.can_parser")


@dataclass
class CanSignalDef:
    """单个 CAN 信号定义（DBC 解析后的简化模型）。"""
    name: str           # 信号名
    start_bit: int      # 起始位（MSB first 或 LSB first 由 byte_order 决定）
    length: int         # 位长度
    byte_order: str = "little_endian"   # "little_endian" | "big_endian"
    is_signed: bool = False
    scale: float = 1.0
    offset: float = 0.0
    minimum: float | None = None
    maximum: float | None = None
    unit: str = ""


@dataclass
class CanFrameRecord:
    """单条 CAN 帧原始记录（未解码为物理值）。"""
    stamp_ns: int
    bag_ts_ns: int
    topic: str
    can_id: int
    is_extended: bool
    dlc: int               # 数据长度码（0-8）
    data_hex: str          # 原始字节 hex


@dataclass
class CanSignalRecord:
    """解码后的 CAN 物理值行（每个信号一行）。"""
    stamp_ns: int
    bag_ts_ns: int
    topic: str
    can_id: int
    signal_name: str
    raw_value: float       # 原始整数（未缩放）
    phys_value: float      # 物理值 = raw * scale + offset
    unit: str = ""


# ========== 信号解码 ==========

def decode_signal(data: bytes, sig: CanSignalDef) -> float | None:
    """从 CAN 帧字节数组中提取单个信号的原始整数值，并转换为物理值。

    Returns:
        物理值（raw * scale + offset）；DLC 不足则返回 None。
    """
    total_bits = len(data) * 8
    if sig.start_bit + sig.length > total_bits:
        return None
    raw = _extract_bits(data, sig.start_bit, sig.length, sig.byte_order, sig.is_signed)
    return raw * sig.scale + sig.offset


def _extract_bits(
    data: bytes, start: int, length: int, byte_order: str, signed: bool
) -> int:
    """按起始位和位长提取整数（支持大/小端）。"""
    if byte_order == "big_endian":
        # DBC MSB first：start_bit 是 MSB 在线路中的绝对位序号
        bits = 0
        for i in range(length):
            bit_pos = start - i
            if bit_pos < 0:
                break
            byte_idx = bit_pos // 8
            bit_idx = bit_pos % 8
            if byte_idx < len(data):
                bits = (bits << 1) | ((data[byte_idx] >> bit_idx) & 1)
    else:
        # little_endian：start_bit 为 LSB 位置
        bits = 0
        for i in range(length):
            bit_pos = start + i
            byte_idx = bit_pos // 8
            bit_idx = bit_pos % 8
            if byte_idx < len(data):
                bits |= ((data[byte_idx] >> bit_idx) & 1) << i
    if signed and bits >= (1 << (length - 1)):
        bits -= 1 << length
    return bits


# ========== 消息解析 ==========

def parse_can_message(msg: RawMessage) -> CanFrameRecord | None:
    """解析单条 can_msgs/Frame（原始帧，不含 DBC 解码）。"""
    data: dict[str, Any] = msg.data
    try:
        header = data.get("header", {})
        ts_ns = msg.stamp_ns or msg.bag_ts_ns
        frame_id = str(header.get("frame_id", ""))
        can_id = int(data.get("id", 0))
        is_extended = bool(data.get("is_extended", False))
        dlc = int(data.get("dlc", 0))
        raw_bytes = bytes(data.get("data", []))[:8]
        return CanFrameRecord(
            stamp_ns=ts_ns,
            bag_ts_ns=msg.bag_ts_ns,
            topic=msg.topic,
            can_id=can_id,
            is_extended=is_extended,
            dlc=dlc,
            data_hex=raw_bytes.hex(),
        )
    except Exception as exc:
        logger.warning("can_parse_failed", topic=msg.topic, error=str(exc))
        return None


def decode_frame_to_signals(
    frame: CanFrameRecord,
    signal_defs: dict[int, list[CanSignalDef]],
) -> list[CanSignalRecord]:
    """将 CAN 帧解码为物理值信号列表。

    Args:
        frame: 原始 CAN 帧记录。
        signal_defs: {can_id → [CanSignalDef]} DBC 信号定义映射。
    """
    defs = signal_defs.get(frame.can_id, [])
    if not defs:
        return []
    raw_bytes = bytes.fromhex(frame.data_hex)
    records: list[CanSignalRecord] = []
    for sig in defs:
        phys = decode_signal(raw_bytes, sig)
        if phys is None:
            continue
        raw = (phys - sig.offset) / sig.scale if sig.scale != 0 else phys
        records.append(CanSignalRecord(
            stamp_ns=frame.stamp_ns,
            bag_ts_ns=frame.bag_ts_ns,
            topic=frame.topic,
            can_id=frame.can_id,
            signal_name=sig.name,
            raw_value=raw,
            phys_value=phys,
            unit=sig.unit,
        ))
    return records


def parse_messages(messages: Iterator[RawMessage]) -> Iterator[CanFrameRecord]:
    """批量解析 CAN 帧消息流（仅原始帧，信号解码需单独调用 decode_frame_to_signals）。"""
    for msg in messages:
        if msg.msg_type in ("can_msgs/Frame", "can_msgs/msg/Frame"):
            rec = parse_can_message(msg)
            if rec is not None:
                yield rec


def frame_to_dicts(records: list[CanFrameRecord]) -> list[dict[str, Any]]:
    return [asdict(r) for r in records]


def signal_to_dicts(records: list[CanSignalRecord]) -> list[dict[str, Any]]:
    return [asdict(r) for r in records]


__all__ = [
    "CanFrameRecord",
    "CanSignalDef",
    "CanSignalRecord",
    "decode_frame_to_signals",
    "decode_signal",
    "frame_to_dicts",
    "parse_can_message",
    "parse_messages",
    "signal_to_dicts",
]
