"""里程计解析器（nav_msgs/Odometry，设计文档 §2 分流表）。

展开位姿（平移 + 四元数转欧拉）和速度，输出 CSV/Parquet 行。
"""
from __future__ import annotations

import math
from collections.abc import Iterator
from dataclasses import asdict, dataclass
from typing import Any

from hunter_common.logging import get_logger

from app.parsers.rosbag_reader import RawMessage

logger = get_logger("app.parsers.odometry_parser")


@dataclass
class OdometryRecord:
    """单条里程计记录（展开字段）。"""
    stamp_ns: int
    bag_ts_ns: int
    frame_id: str
    child_frame_id: str
    topic: str
    # 位姿（平移）
    x: float = 0.0
    y: float = 0.0
    z: float = 0.0
    # 姿态（四元数）
    qx: float = 0.0
    qy: float = 0.0
    qz: float = 0.0
    qw: float = 1.0
    # 姿态（欧拉角，rad）
    roll: float = 0.0
    pitch: float = 0.0
    yaw: float = 0.0
    # 线速度 (m/s)
    vx: float = 0.0
    vy: float = 0.0
    vz: float = 0.0
    # 角速度 (rad/s)
    wx: float = 0.0
    wy: float = 0.0
    wz: float = 0.0


def quaternion_to_euler(qx: float, qy: float, qz: float, qw: float) -> tuple[float, float, float]:
    """四元数转欧拉角 (roll, pitch, yaw)（ZYX 顺序，弧度）。"""
    # roll (x-axis rotation)
    sinr_cosp = 2.0 * (qw * qx + qy * qz)
    cosr_cosp = 1.0 - 2.0 * (qx * qx + qy * qy)
    roll = math.atan2(sinr_cosp, cosr_cosp)
    # pitch (y-axis rotation)
    sinp = 2.0 * (qw * qy - qz * qx)
    pitch = math.asin(sinp) if abs(sinp) >= 1 else math.copysign(math.pi / 2, sinp)
    # yaw (z-axis rotation)
    siny_cosp = 2.0 * (qw * qz + qx * qy)
    cosy_cosp = 1.0 - 2.0 * (qy * qy + qz * qz)
    yaw = math.atan2(siny_cosp, cosy_cosp)
    return roll, pitch, yaw


def parse_odometry_message(msg: RawMessage) -> OdometryRecord | None:
    """解析 nav_msgs/Odometry 消息。"""
    data: dict[str, Any] = msg.data
    try:
        frame_id = str(data.get("header", {}).get("frame_id", ""))
        child_frame_id = str(data.get("child_frame_id", ""))
        ts_ns = msg.stamp_ns or msg.bag_ts_ns

        pose = data.get("pose", {}).get("pose", {})
        pos = pose.get("position", {})
        ori = pose.get("orientation", {})
        twist = data.get("twist", {}).get("twist", {})
        lin = twist.get("linear", {})
        ang = twist.get("angular", {})

        qx = float(ori.get("x", 0.0))
        qy = float(ori.get("y", 0.0))
        qz = float(ori.get("z", 0.0))
        qw = float(ori.get("w", 1.0))
        roll, pitch, yaw = quaternion_to_euler(qx, qy, qz, qw)

        return OdometryRecord(
            stamp_ns=ts_ns,
            bag_ts_ns=msg.bag_ts_ns,
            frame_id=frame_id,
            child_frame_id=child_frame_id,
            topic=msg.topic,
            x=float(pos.get("x", 0.0)),
            y=float(pos.get("y", 0.0)),
            z=float(pos.get("z", 0.0)),
            qx=qx, qy=qy, qz=qz, qw=qw,
            roll=roll, pitch=pitch, yaw=yaw,
            vx=float(lin.get("x", 0.0)),
            vy=float(lin.get("y", 0.0)),
            vz=float(lin.get("z", 0.0)),
            wx=float(ang.get("x", 0.0)),
            wy=float(ang.get("y", 0.0)),
            wz=float(ang.get("z", 0.0)),
        )
    except Exception as exc:
        logger.warning("odometry_parse_failed", topic=msg.topic, error=str(exc))
        return None


def parse_messages(messages: Iterator[RawMessage]) -> Iterator[OdometryRecord]:
    """批量解析里程计消息流。"""
    for msg in messages:
        if msg.msg_type in ("nav_msgs/Odometry", "nav_msgs/msg/Odometry"):
            rec = parse_odometry_message(msg)
            if rec is not None:
                yield rec


def to_dicts(records: Iterator[OdometryRecord] | list[OdometryRecord]) -> list[dict[str, Any]]:
    return [asdict(r) for r in records]


__all__ = [
    "OdometryRecord",
    "parse_messages",
    "parse_odometry_message",
    "quaternion_to_euler",
    "to_dicts",
]
