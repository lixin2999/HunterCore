"""rosbag2 读取封装（设计文档 §2 服务端解析入口）。

使用 rosbags 库（纯 Python，无需 ROS2 运行时）读取 rosbag2 目录，
按 topic 消息类型分流到各专用解析器。

rosbag2 目录结构（车端产出）：
  bag_dir/
    metadata.yaml     # rosbag2 元信息（含 topics 列表、start/end time）
    *.db3             # SQLite3 数据库（rosbag2_storage_sqlite3）
"""
from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from hunter_common.logging import get_logger

logger = get_logger("app.parsers.rosbag_reader")

# topic → msg_type 分流规则（设计文档 §2 表格）
_IMAGE_TYPES = frozenset({"sensor_msgs/msg/Image", "sensor_msgs/Image"})
_COMPRESSED_IMAGE_TYPES = frozenset(
    {"sensor_msgs/msg/CompressedImage", "sensor_msgs/CompressedImage"}
)
_POINTCLOUD_TYPES = frozenset({"sensor_msgs/msg/PointCloud2", "sensor_msgs/PointCloud2"})
_IMU_TYPES = frozenset({"sensor_msgs/msg/Imu", "sensor_msgs/Imu"})
_ODOM_TYPES = frozenset({"nav_msgs/msg/Odometry", "nav_msgs/Odometry"})
_GNSS_TYPES = frozenset({"sensor_msgs/msg/NavSatFix", "sensor_msgs/NavSatFix"})
_CAN_TYPES = frozenset({"can_msgs/msg/Frame", "can_msgs/Frame"})
_TF_TYPES = frozenset({"tf2_msgs/msg/TFMessage", "tf2_msgs/TFMessage"})
_DIAGNOSTIC_TYPES = frozenset(
    {"diagnostic_msgs/msg/DiagnosticArray", "diagnostic_msgs/DiagnosticArray"}
)


@dataclass
class TopicInfo:
    """bag 内单个 topic 的统计信息。"""
    name: str
    msg_type: str
    message_count: int = 0
    start_ns: int | None = None
    end_ns: int | None = None


@dataclass
class BagInfo:
    """bag 元信息（从 metadata.yaml 读取）。"""
    bag_path: str
    ros_version: str = "ros2"
    start_ns: int = 0
    end_ns: int = 0
    duration_s: float = 0.0
    message_count: int = 0
    topics: list[TopicInfo] = field(default_factory=list)

    @property
    def topic_map(self) -> dict[str, TopicInfo]:
        return {t.name: t for t in self.topics}


@dataclass
class RawMessage:
    """从 bag 读出的单条消息（统一封装）。"""
    topic: str
    msg_type: str
    conn: int              # rosbag2 connection index
    stamp_ns: int | None   # header.stamp（消息时间，统一为纳秒整数）
    bag_ts_ns: int         # bag 记录时间（纳秒）
    data: dict[str, Any]   # 反序列化后的消息字段字典


# ========== 消息类型分类 ==========

def classify_topic(msg_type: str) -> str:
    """返回消息类型分类标签（image/compressed_image/pointcloud/imu/odometry/gnss/can/tf/diagnostic/other）。"""
    if msg_type in _IMAGE_TYPES:
        return "image"
    if msg_type in _COMPRESSED_IMAGE_TYPES:
        return "compressed_image"
    if msg_type in _POINTCLOUD_TYPES:
        return "pointcloud"
    if msg_type in _IMU_TYPES:
        return "imu"
    if msg_type in _ODOM_TYPES:
        return "odometry"
    if msg_type in _GNSS_TYPES:
        return "gnss"
    if msg_type in _CAN_TYPES:
        return "can"
    if msg_type in _TF_TYPES:
        return "tf"
    if msg_type in _DIAGNOSTIC_TYPES:
        return "diagnostic"
    return "other"


# ========== rosbag2 读取（使用 rosbags 库） ==========

def read_bag(bag_dir: Path) -> tuple[BagInfo, Iterator[RawMessage]]:
    """打开 rosbag2 目录，返回元信息和消息迭代器。

    Args:
        bag_dir: 本地 rosbag2 目录路径（含 metadata.yaml 和 *.db3）。

    Yields:
        RawMessage 对象（按 bag 内时间顺序）。
    """
    try:
        from rosbags.rosbag2 import Reader  # noqa: PLC0415
    except ImportError as exc:
        raise ImportError(
            "需要安装 rosbags 库（pip install rosbags>=0.9）才能读取 rosbag2 目录"
        ) from exc

    bag_info = BagInfo(bag_path=str(bag_dir))
    topic_infos: dict[str, TopicInfo] = {}

    def _iter() -> Iterator[RawMessage]:
        with Reader(bag_dir) as reader:
            # 填充元信息
            bag_info.start_ns = int(reader.duration[0] * 1e9) if reader.duration else 0
            bag_info.end_ns = int(reader.duration[1] * 1e9) if reader.duration else 0
            bag_info.duration_s = float(reader.duration[1] - reader.duration[0]) if reader.duration else 0.0
            bag_info.message_count = reader.message_count

            conn_to_topic: dict[int, tuple[str, str]] = {}
            for conn in reader.connections:
                ti = TopicInfo(name=conn.topic, msg_type=conn.msgtype)
                topic_infos[conn.topic] = ti
                bag_info.topics.append(ti)
                conn_to_topic[conn.conn_index] = (conn.topic, conn.msgtype)

            for conn, bag_ts_ns_raw, raw_data in reader.messages():
                topic, msg_type = conn_to_topic[conn.conn_index]
                # 反序列化消息体（rosbags.serde）
                try:
                    from rosbags.serde import deserialize_cdr  # noqa: PLC0415
                    msg_dict: dict[str, Any] = _to_dict(deserialize_cdr(raw_data, msg_type))
                except Exception:
                    msg_dict = {}
                # 提取 header.stamp
                stamp_ns = _extract_stamp_ns(msg_dict)
                # 更新 topic 统计
                ti = topic_infos[topic]
                ti.message_count += 1
                bag_ts = int(bag_ts_ns_raw)
                if ti.start_ns is None or bag_ts < ti.start_ns:
                    ti.start_ns = bag_ts
                if ti.end_ns is None or bag_ts > ti.end_ns:
                    ti.end_ns = bag_ts

                yield RawMessage(
                    topic=topic,
                    msg_type=msg_type,
                    conn=conn.conn_index,
                    stamp_ns=stamp_ns,
                    bag_ts_ns=bag_ts,
                    data=msg_dict,
                )

    return bag_info, _iter()


def _extract_stamp_ns(msg_dict: dict[str, Any]) -> int | None:
    """从消息字典中提取 header.stamp 并转为纳秒整数。"""
    header = msg_dict.get("header")
    if not isinstance(header, dict):
        return None
    stamp = header.get("stamp")
    if not isinstance(stamp, dict):
        return None
    sec = int(stamp.get("sec", 0))
    nanosec = int(stamp.get("nanosec", 0))
    return sec * 1_000_000_000 + nanosec


def _to_dict(obj: Any) -> dict[str, Any]:
    """递归将 rosbags Message 对象转为 Python 字典。"""
    if obj is None:
        return {}
    if isinstance(obj, (int, float, str, bool, bytes)):
        return obj  # type: ignore[return-value]
    if isinstance(obj, (list, tuple)):
        return [_to_dict(item) for item in obj]  # type: ignore[return-value]
    if isinstance(obj, dict):
        return {k: _to_dict(v) for k, v in obj.items()}
    # rosbags Message 对象（有 __slots__ 或 _fields）
    if hasattr(obj, "__slots__"):
        return {s: _to_dict(getattr(obj, s, None)) for s in obj.__slots__ if not s.startswith("_")}
    if hasattr(obj, "__dict__"):
        return {k: _to_dict(v) for k, v in vars(obj).items()}
    return str(obj)


__all__ = [
    "BagInfo",
    "RawMessage",
    "TopicInfo",
    "classify_topic",
    "read_bag",
]
