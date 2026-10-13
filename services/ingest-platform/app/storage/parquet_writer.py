"""Parquet 数据集写入工具（设计文档 §6.3 + §7.2）。

负责：
- frame_index.parquet（跨模态对齐主索引，§7.2）
- 结构化传感器表格 Parquet（IMU / Odometry / GNSS / CAN）
- clips.index.parquet（全 bag clip 列表）
- 训练/仿真导出用的 snapshot.manifest.jsonl

写入规范（§6.3）：
- row_group 大小目标 ≤ 64 MB
- 单 bag 表格保持车端产物不重写（只写 derived）
- 跨 bag compaction 由 gc-svc 负责（本模块仅实现单 bag 写入）
"""
from __future__ import annotations

import io
import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import Any

import numpy as np
import numpy.typing as npt
import pyarrow as pa
import pyarrow.parquet as pq
from hunter_common.logging import get_logger

logger = get_logger("app.storage.parquet_writer")

#: 推荐 row_group 行数（对应 ~64 MB，具体取决于列数）
DEFAULT_ROW_GROUP_SIZE = 100_000


# ========== frame_index.parquet（§7.2 核心输出） ==========

def build_frame_index_schema(
    camera_ids: Sequence[str],
    lidar_ids: Sequence[str],
    imu_keys: Sequence[str],
    odom_keys: Sequence[str],
    gnss_keys: Sequence[str],
    can_signals: Sequence[str],
) -> pa.Schema:
    """动态构造 frame_index.parquet 的 Arrow Schema（§7.2 输出列规范）。"""
    fields: list[pa.Field] = [
        pa.field("ts_ns", pa.int64(), nullable=False),  # 主键：锚相机帧时间
    ]

    # 相机列（只存 object key，不嵌入像素）
    for cam in camera_ids:
        fields.append(pa.field(f"camera_{cam}_path", pa.string()))

    # 点云列（key + 时间偏差 ms）
    for lid in lidar_ids:
        fields.append(pa.field(f"lidar_{lid}_pcd_key", pa.string()))
        fields.append(pa.field(f"lidar_{lid}_dt_ms", pa.int32()))

    # IMU 展开列（各字段 + dt_ms）
    for imu in imu_keys:
        for col in ("ax", "ay", "az", "gx", "gy", "gz"):
            fields.append(pa.field(f"imu_{imu}_{col}", pa.float64()))
        fields.append(pa.field(f"imu_{imu}_dt_ms", pa.int32()))

    # 里程计列（位姿 + 速度）
    for odom in odom_keys:
        for col in ("x", "y", "z", "roll", "pitch", "yaw",
                    "vx", "vy", "vwz"):
            fields.append(pa.field(f"odom_{odom}_{col}", pa.float64()))
        fields.append(pa.field(f"odom_{odom}_dt_ms", pa.int32()))

    # GNSS 列（经纬高）
    for gnss in gnss_keys:
        for col in ("lat", "lon", "alt"):
            fields.append(pa.field(f"gnss_{gnss}_{col}", pa.float64()))
        fields.append(pa.field(f"gnss_{gnss}_dt_ms", pa.int32()))

    # CAN 信号列
    for sig in can_signals:
        fields.append(pa.field(f"can_{sig}", pa.float64()))

    # 对齐状态位图（§7.2 末列）
    fields.append(pa.field("align_status", pa.uint8()))

    return pa.schema(fields)


def write_frame_index(
    table_rows: Sequence[Mapping[str, Any]],
    schema: pa.Schema,
    sink: io.IOBase | str,
    *,
    extra_metadata: Mapping[str, str] | None = None,
) -> int:
    """写出 frame_index.parquet（§7.2）。

    Args:
        table_rows: 行字典列表，键为 schema 中的列名。
        schema: Arrow Schema（由 build_frame_index_schema 生成）。
        sink: 文件路径或写入流。
        extra_metadata: 附加到 Parquet file-level metadata 的 KV（§附录 C）。

    Returns:
        写入行数。
    """
    if not table_rows:
        raise ValueError("table_rows 不能为空")
    arrays: dict[str, npt.NDArray[Any]] = {}
    for field in schema:
        col_name = str(field.name)
        values = [row.get(col_name) for row in table_rows]
        col_type = field.type
        if pa.types.is_integer(col_type):
            arrays[col_name] = np.array(
                [v if v is not None else 0 for v in values],
                dtype=np.int64 if col_type.bit_width >= 64 else np.int32,
            )
        elif pa.types.is_floating(col_type):
            arrays[col_name] = np.array(
                [v if v is not None else np.nan for v in values], dtype=np.float64
            )
        elif pa.types.is_string(col_type):
            arrays[col_name] = pa.array([v or "" for v in values], type=pa.string())
        elif pa.types.is_boolean(col_type):
            arrays[col_name] = pa.array([bool(v) for v in values], type=pa.bool_())
        else:
            arrays[col_name] = pa.array(values, type=col_type)

    table = pa.table(arrays, schema=schema)
    metadata: dict[bytes, bytes] = {}
    if extra_metadata:
        metadata = {k.encode(): str(v).encode() for k, v in extra_metadata.items()}
    table = table.replace_schema_metadata(metadata or None)

    write_kwargs: dict[str, Any] = {"compression": "snappy", "use_deprecated_int96_timestamps": False}
    if isinstance(sink, str):
        pq.write_table(table, sink, **write_kwargs)
    else:
        pq.write_table(table, sink, **write_kwargs)

    n = len(table_rows)
    logger.info("frame_index_written", rows=n)
    return n


# ========== 通用单表 Parquet ==========

def write_records_parquet(
    records: Sequence[Mapping[str, Any]],
    columns: Sequence[tuple[str, pa.DataType]],
    sink: io.IOBase | str,
    *,
    row_group_size: int = DEFAULT_ROW_GROUP_SIZE,
) -> int:
    """通用 Parquet 写出（IMU/GNSS/CAN/Odometry 表格）。

    Args:
        records: 行字典列表。
        columns: [(col_name, pa.DataType)] 列声明。
        sink: 路径或流。
        row_group_size: row_group 行数上限。

    Returns:
        写入行数。
    """
    if not records:
        raise ValueError("records 不能为空")
    names = [c[0] for c in columns]
    types = [c[1] for c in columns]
    arrays = []
    for name, typ in zip(names, types, strict=True):
        vals = [r.get(name) for r in records]
        if pa.types.is_floating(typ):
            arr = pa.array(
                [float(v) if v is not None else np.nan for v in vals], type=typ
            )
        elif pa.types.is_integer(typ):
            arr = pa.array([int(v) if v is not None else 0 for v in vals], type=typ)
        else:
            arr = pa.array(vals, type=typ)
        arrays.append(arr)
    table = pa.table(dict(zip(names, arrays, strict=True)), schema=pa.schema(
        [pa.field(n, t) for n, t in columns]
    ))
    pq.write_table(
        table, sink,
        compression="snappy",
        use_deprecated_int96_timestamps=False,
    )
    return len(records)


# ========== clips.index.parquet ==========

def build_clips_index_schema() -> pa.Schema:
    """clip 索引 Parquet Schema（§7.3）。"""
    return pa.schema([
        pa.field("clip_id", pa.int64(), nullable=False),
        pa.field("bag_id", pa.string(), nullable=False),
        pa.field("input_rev", pa.string(), nullable=False),
        pa.field("start_ns", pa.int64(), nullable=False),
        pa.field("end_ns", pa.int64(), nullable=False),
        pa.field("source", pa.string()),
        pa.field("tags", pa.list_(pa.string())),
        pa.field("scene_label", pa.string()),
        pa.field("quality", pa.float64()),
        pa.field("manifest_key", pa.string()),
    ])


def write_clips_index(
    clips: Sequence[Mapping[str, Any]],
    sink: io.IOBase | str,
) -> int:
    """写 clips.index.parquet（§7.3）。"""
    schema = build_clips_index_schema()
    return write_records_parquet(clips, [(str(f.name), f.type) for f in schema], sink)


# ========== snapshot.manifest.jsonl ==========

def build_snapshot_line(
    clip_id: int,
    bag_id: str,
    input_rev: str,
    object_keys: Sequence[str],
    *,
    sha256_map: Mapping[str, str] | None = None,
    sample_params: Mapping[str, Any] | None = None,
) -> str:
    """构造单行 snapshot.manifest.jsonl（§8.2）。"""
    row: dict[str, Any] = {
        "clip_id": clip_id,
        "bag_id": bag_id,
        "input_rev": input_rev,
        "object_keys": list(object_keys),
    }
    if sha256_map:
        row["sha256"] = dict(sha256_map)
    if sample_params:
        row["sample_params"] = dict(sample_params)
    return json.dumps(row, ensure_ascii=False, default=str)


# ========== dataclass 行转 dict ==========

def dataclass_rows(rows: Sequence[Any]) -> list[dict[str, Any]]:
    """将 dataclass 实例列表转为字典列表（供写函数使用）。"""
    result = []
    for r in rows:
        if is_dataclass(r) and not isinstance(r, type):
            result.append(asdict(r))
        elif isinstance(r, dict):
            result.append(r)
        else:
            raise TypeError(f"不支持的行类型: {type(r)}")
    return result


__all__ = [
    "DEFAULT_ROW_GROUP_SIZE",
    "build_clips_index_schema",
    "build_frame_index_schema",
    "build_snapshot_line",
    "dataclass_rows",
    "write_clips_index",
    "write_frame_index",
    "write_records_parquet",
]
