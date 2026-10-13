"""点云解析器（sensor_msgs/PointCloud2，设计文档 §2 分流表）。

按时间窗口切片，输出 PCD 片段（numpy 数组 + PCD 文件头）。
"""
from __future__ import annotations

import io
import struct
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import numpy as np
from hunter_common.logging import get_logger

from app.parsers.rosbag_reader import RawMessage

logger = get_logger("app.parsers.pointcloud_parser")

# PointCloud2 字段数据类型映射（sensor_msgs/PointField.datatype）
_DTYPE_MAP: dict[int, type[np.generic]] = {
    1: np.uint8,
    2: np.int8,
    3: np.uint16,
    4: np.int16,
    5: np.uint32,
    6: np.int32,
    7: np.float32,
    8: np.float64,
}


@dataclass
class PointField:
    """PointCloud2 字段描述。"""
    name: str
    offset: int
    datatype: int
    count: int


@dataclass
class PointCloudSlice:
    """点云切片解析结果（对应一个时间片段的 PCD）。"""
    topic: str
    start_ns: int
    end_ns: int
    frame_id: str
    point_count: int
    fields: list[PointField] = field(default_factory=list)
    points: np.ndarray = field(default_factory=lambda: np.array([]))  # structured array
    pcd_bytes: bytes = b""
    is_valid: bool = True          # False = 字段缺失（QC 计数）


def parse_pointcloud2(msg: RawMessage) -> PointCloudSlice | None:
    """解析单条 sensor_msgs/PointCloud2 消息。

    Returns:
        PointCloudSlice；解析失败返回 None（由 QC 记录解码异常）。
    """
    data: dict[str, Any] = msg.data
    try:
        width = int(data.get("width", 0))
        height = int(data.get("height", 1))
        point_step = int(data.get("point_step", 0))
        row_step = int(data.get("row_step", width * point_step))
        is_dense = bool(data.get("is_dense", True))
        raw: bytes = bytes(data.get("data", []))
        fields_raw: list[dict[str, Any]] = list(data.get("fields", []))

        n_points = width * height
        if n_points == 0 or not raw or point_step == 0:
            return None

        # 构建 numpy structured dtype
        fields: list[PointField] = []
        dtype_items: list[tuple[str, type, int]] = []
        for f in fields_raw:
            pf = PointField(
                name=str(f.get("name", "")),
                offset=int(f.get("offset", 0)),
                datatype=int(f.get("datatype", 7)),
                count=int(f.get("count", 1)),
            )
            fields.append(pf)
            np_type = _DTYPE_MAP.get(pf.datatype, np.float32)
            if pf.count > 1:
                dtype_items.append((pf.name, np_type, pf.count))
            else:
                dtype_items.append((pf.name, np_type))

        dt = np.dtype({"names": [d[0] for d in dtype_items],
                       "formats": [d[1:] if len(d) > 2 else d[1] for d in dtype_items],
                       "itemsize": point_step})

        arr = np.frombuffer(raw, dtype=dt, count=n_points)

        # 验证必要字段完整性（x, y, z）
        field_names = {f.name for f in fields}
        is_valid = {"x", "y", "z"} <= field_names

        frame_id = str(data.get("header", {}).get("frame_id", ""))
        ts_ns = msg.stamp_ns or msg.bag_ts_ns

        pcd_bytes = _numpy_to_pcd_bin(arr, fields, n_points)

        return PointCloudSlice(
            topic=msg.topic,
            start_ns=ts_ns,
            end_ns=ts_ns,
            frame_id=frame_id,
            point_count=n_points,
            fields=fields,
            points=arr.copy(),
            pcd_bytes=pcd_bytes,
            is_valid=is_valid,
        )
    except Exception as exc:
        logger.warning("pointcloud_parse_failed", topic=msg.topic, error=str(exc))
        return None


def slice_by_time_window(
    slices: list[PointCloudSlice],
    window_ns: int,
) -> list[PointCloudSlice]:
    """按时间窗口合并切片（§2：PointCloud2 按时间窗口切片）。

    Args:
        slices: 已解析的切片列表（已按 start_ns 排序）。
        window_ns: 合并窗口（纳秒），连续消息在此窗口内合并为一片。

    Returns:
        合并后的切片列表。
    """
    if not slices:
        return []
    merged: list[PointCloudSlice] = []
    cur = slices[0]
    for s in slices[1:]:
        if s.start_ns - cur.end_ns <= window_ns:
            # 合并：拼接 points
            all_pts = np.concatenate([cur.points, s.points])
            merged.append(PointCloudSlice(
                topic=cur.topic,
                start_ns=cur.start_ns,
                end_ns=s.end_ns,
                frame_id=cur.frame_id,
                point_count=len(all_pts),
                fields=cur.fields,
                points=all_pts,
                pcd_bytes=_numpy_to_pcd_bin(all_pts, cur.fields, len(all_pts)),
                is_valid=cur.is_valid and s.is_valid,
            ))
            cur = merged.pop()
        else:
            merged.append(cur)
            cur = s
    merged.append(cur)
    return merged


def parse_messages(messages: Iterator[RawMessage]) -> Iterator[PointCloudSlice]:
    """批量解析点云消息流。"""
    for msg in messages:
        if msg.msg_type in ("sensor_msgs/PointCloud2", "sensor_msgs/msg/PointCloud2"):
            s = parse_pointcloud2(msg)
            if s is not None:
                yield s


# ========== PCD 二进制格式 ==========

def _numpy_to_pcd_bin(arr: np.ndarray, fields: list[PointField], n: int) -> bytes:
    """将 numpy structured array 序列化为 PCD binary 格式（ASCII 头 + 二进制点数据）。"""
    buf = io.BytesIO()
    # PCD 0.7 头
    type_map = {1: "U", 2: "I", 3: "U", 4: "I", 5: "U", 6: "I", 7: "F", 8: "F"}
    size_map = {1: 1, 2: 1, 3: 2, 4: 2, 5: 4, 6: 4, 7: 4, 8: 8}
    field_names = " ".join(f.name for f in fields)
    field_sizes = " ".join(str(size_map.get(f.datatype, 4)) for f in fields)
    field_types = " ".join(type_map.get(f.datatype, "F") for f in fields)
    field_counts = " ".join(str(f.count) for f in fields)

    header = (
        f"# .PCD v0.7 - Point Cloud Data\n"
        f"VERSION 0.7\n"
        f"FIELDS {field_names}\n"
        f"SIZE {field_sizes}\n"
        f"TYPE {field_types}\n"
        f"COUNT {field_counts}\n"
        f"WIDTH {n}\n"
        f"HEIGHT 1\n"
        f"VIEWPOINT 0 0 0 1 0 0 0\n"
        f"POINTS {n}\n"
        f"DATA binary\n"
    )
    buf.write(header.encode("ascii"))
    buf.write(arr.tobytes())
    return buf.getvalue()


__all__ = [
    "PointCloudSlice",
    "PointField",
    "parse_messages",
    "parse_pointcloud2",
    "slice_by_time_window",
]
