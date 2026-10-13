"""parquet_writer 纯逻辑单元测试（app/storage/parquet_writer.py）。"""
from __future__ import annotations

import io
import json
from dataclasses import dataclass

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest


# ========== build_frame_index_schema ==========

class TestBuildFrameIndexSchema:
    def test_basic_columns(self) -> None:
        from app.storage.parquet_writer import build_frame_index_schema

        schema = build_frame_index_schema(
            camera_ids=["camera_front"],
            lidar_ids=["lidar_top"],
            imu_keys=["imu_0"],
            odom_keys=["odom_0"],
            gnss_keys=["gnss_0"],
            can_signals=["speed"],
        )
        names = [f.name for f in schema]
        # 必须含主键 ts_ns
        assert "ts_ns" in names
        # 相机列
        assert "camera_camera_front_path" in names
        # 点云列
        assert "lidar_lidar_top_pcd_key" in names
        assert "lidar_lidar_top_dt_ms" in names
        # IMU 展开
        assert "imu_imu_0_ax" in names
        assert "imu_imu_0_dt_ms" in names
        # 里程计
        assert "odom_odom_0_x" in names
        assert "odom_odom_0_dt_ms" in names
        # GNSS
        assert "gnss_gnss_0_lat" in names
        # CAN
        assert "can_speed" in names
        # 状态位图
        assert "align_status" in names

    def test_ts_ns_not_nullable(self) -> None:
        from app.storage.parquet_writer import build_frame_index_schema

        schema = build_frame_index_schema([], [], [], [], [], [])
        ts_field = schema.field("ts_ns")
        assert not ts_field.nullable

    def test_empty_ids(self) -> None:
        from app.storage.parquet_writer import build_frame_index_schema

        schema = build_frame_index_schema([], [], [], [], [], [])
        names = [f.name for f in schema]
        assert names == ["ts_ns", "align_status"]


# ========== write_frame_index ==========

class TestWriteFrameIndex:
    def test_write_and_read(self) -> None:
        from app.storage.parquet_writer import build_frame_index_schema, write_frame_index

        schema = build_frame_index_schema(
            camera_ids=["cam0"], lidar_ids=["lid0"],
            imu_keys=[], odom_keys=[], gnss_keys=[], can_signals=[],
        )
        rows = [
            {"ts_ns": 1000, "camera_cam0_path": "a/1000.jpg",
             "lidar_lid0_pcd_key": "a/1000.pcd", "lidar_lid0_dt_ms": 5,
             "align_status": 3},
            {"ts_ns": 2000, "camera_cam0_path": "a/2000.jpg",
             "lidar_lid0_pcd_key": None, "lidar_lid0_dt_ms": None,
             "align_status": 1},
        ]
        buf = io.BytesIO()
        n = write_frame_index(rows, schema, buf)
        assert n == 2

        # 回读验证
        buf.seek(0)
        table = pq.read_table(buf)
        assert table.num_rows == 2
        assert table.column("ts_ns").to_pylist() == [1000, 2000]

    def test_empty_rows_raises(self) -> None:
        from app.storage.parquet_writer import build_frame_index_schema, write_frame_index

        schema = build_frame_index_schema([], [], [], [], [], [])
        with pytest.raises(ValueError, match="不能为空"):
            write_frame_index([], schema, io.BytesIO())

    def test_extra_metadata(self) -> None:
        from app.storage.parquet_writer import build_frame_index_schema, write_frame_index

        schema = build_frame_index_schema([], [], [], [], [], [])
        rows = [{"ts_ns": 1, "align_status": 0}]
        buf = io.BytesIO()
        write_frame_index(rows, schema, buf, extra_metadata={"bag_id": "b-001"})
        buf.seek(0)
        table = pq.read_table(buf)
        meta = table.schema.metadata
        assert meta is not None
        assert b"bag_id" in meta


# ========== write_records_parquet ==========

class TestWriteRecordsParquet:
    def test_basic_write(self) -> None:
        from app.storage.parquet_writer import write_records_parquet

        columns = [("stamp_ns", pa.int64()), ("ax", pa.float64()), ("label", pa.string())]
        records = [
            {"stamp_ns": 100, "ax": 1.5, "label": "a"},
            {"stamp_ns": 200, "ax": 2.5, "label": "b"},
        ]
        buf = io.BytesIO()
        n = write_records_parquet(records, columns, buf)
        assert n == 2

        buf.seek(0)
        table = pq.read_table(buf)
        assert table.column("stamp_ns").to_pylist() == [100, 200]

    def test_empty_records_raises(self) -> None:
        from app.storage.parquet_writer import write_records_parquet

        with pytest.raises(ValueError, match="不能为空"):
            write_records_parquet([], [("x", pa.int64())], io.BytesIO())

    def test_null_handling(self) -> None:
        from app.storage.parquet_writer import write_records_parquet

        columns = [("stamp_ns", pa.int64()), ("value", pa.float64())]
        records = [
            {"stamp_ns": 1, "value": None},
            {"stamp_ns": None, "value": 3.14},
        ]
        buf = io.BytesIO()
        write_records_parquet(records, columns, buf)
        buf.seek(0)
        table = pq.read_table(buf)
        # int: None→0; float: None→NaN
        assert table.column("stamp_ns").to_pylist() == [1, 0]
        vals = table.column("value").to_pylist()
        assert np.isnan(vals[0])
        assert vals[1] == pytest.approx(3.14)


# ========== clips.index.parquet ==========

class TestClipsIndex:
    def test_build_schema(self) -> None:
        from app.storage.parquet_writer import build_clips_index_schema

        schema = build_clips_index_schema()
        names = [f.name for f in schema]
        assert "clip_id" in names
        assert "bag_id" in names
        assert "tags" in names
        assert "quality" in names

    def test_write_clips(self) -> None:
        from app.storage.parquet_writer import write_clips_index

        clips = [
            {"clip_id": 1, "bag_id": "b-001", "input_rev": "rev1",
             "start_ns": 1000, "end_ns": 2000, "source": "window",
             "tags": ["tag1"], "scene_label": None, "quality": 0.9, "manifest_key": "k"},
        ]
        buf = io.BytesIO()
        n = write_clips_index(clips, buf)
        assert n == 1


# ========== build_snapshot_line ==========

class TestSnapshotLine:
    def test_basic(self) -> None:
        from app.storage.parquet_writer import build_snapshot_line

        line = build_snapshot_line(
            clip_id=42, bag_id="b-001", input_rev="rev",
            object_keys=["key1.jpg", "key2.pcd"],
        )
        parsed = json.loads(line)
        assert parsed["clip_id"] == 42
        assert parsed["bag_id"] == "b-001"
        assert parsed["object_keys"] == ["key1.jpg", "key2.pcd"]

    def test_with_sha256(self) -> None:
        from app.storage.parquet_writer import build_snapshot_line

        line = build_snapshot_line(
            clip_id=1, bag_id="b", input_rev="r",
            object_keys=["a"],
            sha256_map={"a": "deadbeef"},
        )
        parsed = json.loads(line)
        assert parsed["sha256"]["a"] == "deadbeef"


# ========== dataclass_rows ==========

class TestDataclassRows:
    def test_dataclass_conversion(self) -> None:
        from app.storage.parquet_writer import dataclass_rows

        @dataclass
        class Row:
            x: int
            y: str

        rows = [Row(1, "a"), Row(2, "b")]
        result = dataclass_rows(rows)
        assert result == [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}]

    def test_dict_passthrough(self) -> None:
        from app.storage.parquet_writer import dataclass_rows

        rows = [{"a": 1}, {"b": 2}]
        assert dataclass_rows(rows) == rows

    def test_invalid_type_raises(self) -> None:
        from app.storage.parquet_writer import dataclass_rows

        with pytest.raises(TypeError):
            dataclass_rows([123])  # type: ignore
