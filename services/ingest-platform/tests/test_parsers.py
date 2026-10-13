"""解析器单元测试（app/parsers/，使用模拟 RawMessage）。"""
from __future__ import annotations

import io
from typing import Any

import numpy as np
import pytest

from app.parsers.rosbag_reader import RawMessage


def _make_raw_msg(topic: str, msg_type: str, data: dict[str, Any], *, stamp_ns: int | None = 1_700_000_000_000_000_000, bag_ts_ns: int = 1_700_000_000_100_000_000) -> RawMessage:
    return RawMessage(
        topic=topic,
        msg_type=msg_type,
        conn=0,
        stamp_ns=stamp_ns,
        bag_ts_ns=bag_ts_ns,
        data=data,
    )


# ========== image_parser ==========

class TestImageParser:
    def test_parse_image_raw(self) -> None:
        """sensor_msgs/Image（RGB8）→ ImageFrame with JPEG bytes。"""
        from app.parsers.image_parser import parse_image_message

        h, w = 4, 4
        raw_data = np.random.randint(0, 255, (h, w, 3), dtype=np.uint8).tobytes()
        msg = _make_raw_msg(
            "/camera_front/image_raw",
            "sensor_msgs/msg/Image",
            {
                "header": {"stamp": {"sec": 1_700_000_000, "nanosec": 0}, "frame_id": "camera"},
                "height": h,
                "width": w,
                "encoding": "rgb8",
                "data": raw_data,
                "is_bigendian": 0,
                "step": w * 3,
            },
        )
        frame = parse_image_message(msg)
        assert frame is not None
        assert frame.width == w
        assert frame.height == h
        assert frame.ts_ns == 1_700_000_000_000_000_000
        assert frame.is_decoded is True
        # JPEG magic
        assert frame.image_bytes[:2] == b"\xff\xd8"

    def test_parse_image_bgra(self) -> None:
        from app.parsers.image_parser import parse_image_message

        h, w = 2, 3
        data_arr = np.zeros((h, w, 4), dtype=np.uint8)
        data_arr[:, :, :3] = 128
        data_arr[:, :, 3] = 255
        msg = _make_raw_msg(
            "/camera/image", "sensor_msgs/msg/Image",
            {
                "header": {"stamp": {"sec": 1, "nanosec": 0}, "frame_id": ""},
                "height": h, "width": w, "encoding": "bgra8",
                "data": data_arr.tobytes(), "is_bigendian": 0, "step": w * 4,
            },
        )
        # RGBA 不支持 JPEG 输出，使用 PNG
        frame = parse_image_message(msg, output_fmt="png")
        assert frame is not None
        assert frame.encoding == "bgra8"
        assert frame.fmt == "png"

    def test_parse_compressed_image(self) -> None:
        """sensor_msgs/CompressedImage → ImageFrame。"""
        from app.parsers.image_parser import parse_compressed_image
        from PIL import Image

        img = Image.new("RGB", (8, 8), color=(255, 0, 0))
        buf = io.BytesIO()
        img.save(buf, format="JPEG")
        jpeg_bytes = buf.getvalue()

        msg = _make_raw_msg(
            "/camera/image_color/compressed",
            "sensor_msgs/msg/CompressedImage",
            {
                "header": {"stamp": {"sec": 1_700_000_000, "nanosec": 0}, "frame_id": "camera"},
                "format": "jpeg",
                "data": jpeg_bytes,
            },
        )
        frame = parse_compressed_image(msg)
        assert frame is not None
        assert frame.width == 8
        assert frame.height == 8
        assert frame.image_bytes[:2] == b"\xff\xd8"

    def test_compressed_image_invalid(self) -> None:
        """无效数据返回 None（不抛异常）。"""
        from app.parsers.image_parser import parse_compressed_image

        msg = _make_raw_msg(
            "/cam/compressed", "sensor_msgs/msg/CompressedImage",
            {"header": {}, "format": "jpeg", "data": b"not_valid"},
        )
        frame = parse_compressed_image(msg)
        assert frame is None


# ========== pointcloud_parser ==========

class TestPointCloudParser:
    def _make_pc2_msg(self, n: int = 5) -> RawMessage:
        """构造最小 PointCloud2 消息（xyz intensity）。"""
        fields = [
            {"name": "x", "offset": 0, "datatype": 7, "count": 1},
            {"name": "y", "offset": 4, "datatype": 7, "count": 1},
            {"name": "z", "offset": 8, "datatype": 7, "count": 1},
            {"name": "intensity", "offset": 12, "datatype": 7, "count": 1},
        ]
        point_step = 16
        data = bytearray()
        for i in range(n):
            row = np.array([float(i), float(i + 1), float(i + 2), float(i * 0.1)], dtype=np.float32)
            data.extend(row.tobytes())
        return _make_raw_msg(
            "/lidar_top/points_raw",
            "sensor_msgs/msg/PointCloud2",
            {
                "header": {"stamp": {"sec": 1_700_000_000, "nanosec": 0}, "frame_id": "lidar"},
                "height": 1,
                "width": n,
                "fields": fields,
                "is_dense": True,
                "point_step": point_step,
                "row_step": point_step * n,
                "data": bytes(data),
            },
        )

    def test_parse_to_slice(self) -> None:
        from app.parsers.pointcloud_parser import parse_pointcloud2

        msg = self._make_pc2_msg(5)
        result = parse_pointcloud2(msg)
        assert result is not None
        assert result.point_count == 5
        assert result.is_valid is True
        assert "x" in [f.name for f in result.fields]

    def test_pcd_bytes(self) -> None:
        from app.parsers.pointcloud_parser import parse_pointcloud2

        msg = self._make_pc2_msg(3)
        result = parse_pointcloud2(msg)
        assert result is not None
        # PCD 文件头包含 VERSION
        assert b"VERSION" in result.pcd_bytes[:100]
        assert b"FIELDS" in result.pcd_bytes

    def test_missing_xyz_invalid(self) -> None:
        """缺少 x/y/z 字段 → is_valid=False。"""
        from app.parsers.pointcloud_parser import parse_pointcloud2

        msg = _make_raw_msg(
            "/lidar", "sensor_msgs/msg/PointCloud2",
            {
                "header": {},
                "height": 1, "width": 2,
                "fields": [{"name": "intensity", "offset": 0, "datatype": 7, "count": 1}],
                "is_dense": True, "point_step": 4, "row_step": 8,
                "data": bytes(8),
            },
        )
        result = parse_pointcloud2(msg)
        assert result is not None
        assert result.is_valid is False


# ========== imu_parser ==========

class TestImuParser:
    def test_parse_imu_fields(self) -> None:
        from app.parsers.imu_parser import parse_imu_message

        msg = _make_raw_msg(
            "/imu/data", "sensor_msgs/msg/Imu",
            {
                "header": {"stamp": {"sec": 1_700_000_000, "nanosec": 0}, "frame_id": "imu_link"},
                "orientation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0},
                "angular_velocity": {"x": 0.1, "y": 0.2, "z": 0.3},
                "linear_acceleration": {"x": 0.0, "y": 0.0, "z": 9.8},
            },
        )
        rec = parse_imu_message(msg)
        assert rec is not None
        assert rec.stamp_ns == 1_700_000_000_000_000_000
        assert rec.az == pytest.approx(9.8)
        assert rec.qw == pytest.approx(1.0)
        assert rec.gz == pytest.approx(0.3)

    def test_parse_imu_missing_fields(self) -> None:
        """缺失字段应有默认值。"""
        from app.parsers.imu_parser import parse_imu_message

        msg = _make_raw_msg(
            "/imu", "sensor_msgs/msg/Imu",
            {"header": {"stamp": {"sec": 1, "nanosec": 0}}},
            stamp_ns=1_000_000_000,
        )
        rec = parse_imu_message(msg)
        assert rec is not None
        assert rec.ax == 0.0
        assert rec.qw == 1.0  # 默认单位四元数


# ========== odometry_parser ==========

class TestOdometryParser:
    def test_parse_odometry(self) -> None:
        from app.parsers.odometry_parser import parse_odometry_message

        msg = _make_raw_msg(
            "/odom", "nav_msgs/msg/Odometry",
            {
                "header": {"stamp": {"sec": 1_700_000_000, "nanosec": 500_000_000}, "frame_id": "odom"},
                "child_frame_id": "base_link",
                "pose": {
                    "pose": {
                        "position": {"x": 1.0, "y": 2.0, "z": 0.5},
                        "orientation": {"x": 0.0, "y": 0.0, "z": 0.707, "w": 0.707},
                    }
                },
                "twist": {
                    "twist": {
                        "linear": {"x": 5.0, "y": 0.0, "z": 0.0},
                        "angular": {"x": 0.0, "y": 0.0, "z": 0.1},
                    }
                },
            },
            stamp_ns=1_700_000_000_500_000_000,
        )
        rec = parse_odometry_message(msg)
        assert rec is not None
        assert rec.stamp_ns == 1_700_000_000_500_000_000
        assert rec.x == pytest.approx(1.0)
        assert rec.vx == pytest.approx(5.0)
        # yaw ≈ π/2（z=0.707, w=0.707 的四元数）
        assert rec.yaw == pytest.approx(np.pi / 2, abs=0.01)


# ========== gnss_parser ==========

class TestGnssParser:
    def test_parse_nav_sat_fix(self) -> None:
        from app.parsers.gnss_parser import parse_gnss_message

        msg = _make_raw_msg(
            "/gps/fix", "sensor_msgs/msg/NavSatFix",
            {
                "header": {"stamp": {"sec": 1_700_000_000, "nanosec": 0}, "frame_id": "gps"},
                "latitude": 39.9042,
                "longitude": 116.4074,
                "altitude": 50.0,
                "status": {"status": 0, "service": 1},
                "position_covariance": [1.0, 0.0, 0.0, 0.0, 2.0, 0.0, 0.0, 0.0, 3.0],
            },
        )
        rec = parse_gnss_message(msg)
        assert rec is not None
        assert rec.lat == pytest.approx(39.9042)
        assert rec.lon == pytest.approx(116.4074)
        assert rec.alt == pytest.approx(50.0)
        assert rec.cov_lat == pytest.approx(1.0)
        assert rec.cov_lon == pytest.approx(2.0)
        assert rec.cov_alt == pytest.approx(3.0)


# ========== can_parser ==========

class TestCanParser:
    def test_decode_signal_little_endian(self) -> None:
        from app.parsers.can_parser import CanSignalDef, decode_signal

        data = bytes([0x01, 0x02, 0x03, 0x04])
        sig = CanSignalDef(name="test", start_bit=0, length=8, byte_order="little_endian", scale=1.0, offset=0.0)
        value = decode_signal(data, sig)
        assert value == 1.0

    def test_decode_signal_big_endian(self) -> None:
        from app.parsers.can_parser import CanSignalDef, decode_signal

        # big_endian DBC: start_bit=7 (byte0 MSB), length=8 取整字节
        data = bytes([0xAB, 0xCD])
        sig = CanSignalDef(name="test", start_bit=7, length=8, byte_order="big_endian", scale=1.0, offset=0.0)
        value = decode_signal(data, sig)
        # 提取 byte0 的 8 bits (MSB first) → 0xAB = 171
        assert value is not None
        assert value == 171.0

    def test_decode_with_scale_offset(self) -> None:
        from app.parsers.can_parser import CanSignalDef, decode_signal

        data = bytes([0x64])  # raw = 100
        sig = CanSignalDef(name="speed", start_bit=0, length=8, byte_order="little_endian", scale=0.1, offset=-10.0)
        value = decode_signal(data, sig)
        # 100 * 0.1 + (-10) = 0.0
        assert value == pytest.approx(0.0)

    def test_decode_out_of_range(self) -> None:
        """DLC 不够 → 返回 None。"""
        from app.parsers.can_parser import CanSignalDef, decode_signal

        data = bytes([0x01])  # 1 byte = 8 bits
        sig = CanSignalDef(name="x", start_bit=0, length=16, byte_order="little_endian", scale=1.0, offset=0.0)
        value = decode_signal(data, sig)
        assert value is None
