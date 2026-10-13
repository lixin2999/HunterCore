"""IMU 解析器（sensor_msgs/Imu，设计文档 §2 分流表）。

展开角速度、线性加速度、姿态四元组字段，输出 CSV/Parquet 行。
"""
from __future__ import annotations

from collections.abc import Iterator
from dataclasses import asdict, dataclass
from typing import Any

from hunter_common.logging import get_logger

from app.parsers.rosbag_reader import RawMessage

logger = get_logger("app.parsers.imu_parser")


@dataclass
class ImuRecord:
    """单条 IMU 记录（展开字段，对应 Parquet 行）。"""
    stamp_ns: int           # 消息时间（header.stamp，纳秒整数）
    bag_ts_ns: int          # bag 记录时间
    frame_id: str
    topic: str
    # 角速度 (rad/s)
    gx: float = 0.0
    gy: float = 0.0
    gz: float = 0.0
    # 线性加速度 (m/s²)
    ax: float = 0.0
    ay: float = 0.0
    az: float = 0.0
    # 姿态四元组 (x, y, z, w)
    qx: float = 0.0
    qy: float = 0.0
    qz: float = 0.0
    qw: float = 1.0
    # 协方差（对角线简化，完整 3x3 存 JSON）
    covariance_orientation: str = ""


def parse_imu_message(msg: RawMessage) -> ImuRecord | None:
    """解析 sensor_msgs/Imu 消息。"""
    data: dict[str, Any] = msg.data
    try:
        frame_id = str(data.get("header", {}).get("frame_id", ""))
        ts_ns = msg.stamp_ns or msg.bag_ts_ns

        gyro = data.get("angular_velocity", {})
        acc = data.get("linear_acceleration", {})
        ori = data.get("orientation", {})

        rec = ImuRecord(
            stamp_ns=ts_ns,
            bag_ts_ns=msg.bag_ts_ns,
            frame_id=frame_id,
            topic=msg.topic,
            gx=float(gyro.get("x", 0.0)),
            gy=float(gyro.get("y", 0.0)),
            gz=float(gyro.get("z", 0.0)),
            ax=float(acc.get("x", 0.0)),
            ay=float(acc.get("y", 0.0)),
            az=float(acc.get("z", 0.0)),
            qx=float(ori.get("x", 0.0)),
            qy=float(ori.get("y", 0.0)),
            qz=float(ori.get("z", 0.0)),
            qw=float(ori.get("w", 1.0)),
        )
        return rec
    except Exception as exc:
        logger.warning("imu_parse_failed", topic=msg.topic, error=str(exc))
        return None


def parse_messages(messages: Iterator[RawMessage]) -> Iterator[ImuRecord]:
    """批量解析 IMU 消息流。"""
    for msg in messages:
        if msg.msg_type in ("sensor_msgs/Imu", "sensor_msgs/msg/Imu"):
            rec = parse_imu_message(msg)
            if rec is not None:
                yield rec


def to_dicts(records: Iterator[ImuRecord] | list[ImuRecord]) -> list[dict[str, Any]]:
    """转为字典列表（供 Parquet writer 使用）。"""
    return [asdict(r) for r in records]


__all__ = [
    "ImuRecord",
    "parse_imu_message",
    "parse_messages",
    "to_dicts",
]
