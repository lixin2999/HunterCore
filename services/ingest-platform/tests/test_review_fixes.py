"""代码审查 #4–#21 修复项的纯逻辑单元测试。

覆盖新增的可单测纯函数与安全 fail-closed 行为：
- #4  QC 黑帧/冻结帧/标定冲突检测、传感器全集提取
- #6  align 传感器分组 / key 归类 / 表格分类
- #7  slice 服务端规则引擎（IMU 急刹/碰撞挖掘）
- #14 网关转发认证 fail-closed（空密钥拒绝）
- #15 下行命令签名 fail-closed（空密钥拒绝签发）
- #16 parsed 只读凭据与主凭据客户端隔离
- #21 背压守卫节流延迟传导
"""
from __future__ import annotations

import io
import time
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pytest


def _make_image_bytes(gray: int, size: tuple[int, int] = (8, 8)) -> bytes:
    """生成纯色 PNG（供黑帧检测）。"""
    from PIL import Image

    buf = io.BytesIO()
    Image.new("L", size, color=gray).save(buf, format="PNG")
    return buf.getvalue()


# ========== #4 黑帧 / 冻结帧检测 ==========

class TestBlackFrozenDetection:
    def test_black_frame_dark_below_threshold(self) -> None:
        from app.services.qc_worker import detect_black_frame

        assert detect_black_frame(_make_image_bytes(0), 10.0) is True

    def test_black_frame_bright_ok(self) -> None:
        from app.services.qc_worker import detect_black_frame

        assert detect_black_frame(_make_image_bytes(200), 10.0) is False

    def test_black_frame_undecodable_counts_as_black(self) -> None:
        from app.services.qc_worker import detect_black_frame

        assert detect_black_frame(b"not-an-image", 10.0) is True

    def test_detect_black_frozen_black_run(self) -> None:
        from app.services.qc_worker import detect_black_frozen

        frames = [
            _make_image_bytes(0),
            _make_image_bytes(0),
            _make_image_bytes(200),
        ]
        result = detect_black_frozen(frames, black_threshold=10.0, frozen_min_count=3)
        assert result["black_runs"] == 1
        assert result["ok"] is False

    def test_detect_black_frozen_frozen_run(self) -> None:
        from app.services.qc_worker import detect_black_frozen

        same = _make_image_bytes(128)
        frames = [same, same, same]
        result = detect_black_frozen(frames, black_threshold=10.0, frozen_min_count=3)
        assert result["frozen_runs"] == 1
        assert result["ok"] is False

    def test_detect_black_frozen_clean(self) -> None:
        from app.services.qc_worker import detect_black_frozen

        frames = [_make_image_bytes(100), _make_image_bytes(120), _make_image_bytes(140)]
        result = detect_black_frozen(frames, black_threshold=10.0, frozen_min_count=3)
        assert result == {"black_runs": 0, "frozen_runs": 0, "frames": 3, "ok": True}

    def test_detect_black_frozen_empty(self) -> None:
        from app.services.qc_worker import detect_black_frozen

        assert detect_black_frozen([], black_threshold=10.0, frozen_min_count=3)["ok"] is True


# ========== #4 标定冲突检测 ==========

class TestCalibrationConflict:
    def test_duplicate_child_is_conflict(self) -> None:
        from app.services.qc_worker import detect_calibration_conflict

        manifest = {
            "tf_static": {
                "transforms": [
                    {"parent_frame_id": "base", "child_frame_id": "camera_front"},
                    {"parent_frame_id": "odom", "child_frame_id": "camera_front"},
                ]
            }
        }
        result = detect_calibration_conflict(manifest, ["camera_front"])
        assert result["status"] == "conflict"
        assert result["score"] == 0.0
        assert "camera_front" in result["duplicate_children"]

    def test_uncovered_sensor_is_incomplete(self) -> None:
        from app.services.qc_worker import detect_calibration_conflict

        manifest = {"tf_static": {"transforms": [{"parent_frame_id": "base", "child_frame_id": "camera_front"}]}}
        result = detect_calibration_conflict(manifest, ["camera_front", "lidar_top"])
        assert result["status"] == "incomplete"
        assert "lidar_top" in result["uncovered_sensors"]

    def test_all_covered_ok(self) -> None:
        from app.services.qc_worker import detect_calibration_conflict

        manifest = {"tf_static": {"transforms": [{"parent_frame_id": "base", "child_frame_id": "camera_front"}]}}
        result = detect_calibration_conflict(manifest, ["camera_front"])
        assert result["status"] == "ok"
        assert result["score"] == 1.0

    def test_no_tf_degraded(self) -> None:
        from app.services.qc_worker import detect_calibration_conflict

        result = detect_calibration_conflict({}, ["camera_front"])
        assert result["status"] == "no_tf"
        assert result["score"] == 0.5


# ========== #4 传感器 id 提取 ==========

class TestSensorIdExtraction:
    def test_sensor_id_from_topic(self) -> None:
        from app.services.qc_worker import sensor_id_from_topic

        assert sensor_id_from_topic("/camera_front/image_raw") == "camera_front"
        assert sensor_id_from_topic("") == ""

    def test_collect_sensor_ids_dedup_and_order(self) -> None:
        from app.services.qc_worker import collect_sensor_ids

        topics = [
            {"topic": "/camera_front/image_raw"},
            {"topic": "/lidar_top/points_raw"},
            {"topic": "/camera_front/image_rect"},  # 重复
            {"topic": "/imu/data"},  # 非图像/点云，忽略
        ]
        assert collect_sensor_ids(topics) == ["camera_front", "lidar_top"]


# ========== #6 align 传感器分组 / 分类 ==========

class TestAlignGrouping:
    def test_sensor_from_key_parent_dir(self) -> None:
        from app.services.align_worker import _sensor_from_key

        key = "bags/v1/veh/images/camera_front/123.jpg"
        assert _sensor_from_key(key, ("images", "pointclouds"), "default") == "camera_front"

    def test_sensor_from_key_generic_parent_falls_back(self) -> None:
        from app.services.align_worker import _sensor_from_key

        key = "bags/v1/veh/images/123.jpg"
        assert _sensor_from_key(key, ("images", "pointclouds"), "default") == "default"

    def test_group_by_sensor(self) -> None:
        from app.services.align_worker import group_by_sensor

        keys = [
            "a/images/camera_front/1.jpg",
            "a/images/camera_rear/2.jpg",
            "a/images/camera_front/3.jpg",
        ]
        groups = group_by_sensor(keys, ("images",), "default")
        assert set(groups) == {"camera_front", "camera_rear"}
        assert len(groups["camera_front"]) == 2

    def test_kind_classification(self) -> None:
        from app.services.align_worker import _kind

        assert _kind("imu_top") == "imu"
        assert _kind("odom") == "odom"
        assert _kind("gnss_0") == "gnss"
        assert _kind("navsat") == "gnss"
        assert _kind("can_bus") == "can"
        assert _kind("whatever") == "other"


# ========== #17 GNSS 脱敏实际接入 align 读路径 ==========

class TestGnssMaskWiring:
    async def test_gnss_latlon_truncated_via_align(self) -> None:
        from app.services.align_worker import AlignWorker

        settings = MagicMock()
        settings.align_table_window_ms = 50
        settings.gnss_mask_enabled = True
        settings.gnss_descimal_places = 4
        worker = AlignWorker(settings, MagicMock(), MagicMock(), MagicMock())

        anchor = np.array([1000], dtype=np.int64)
        tbl = {
            "stamp_ns": np.array([1000], dtype=np.int64),
            "lat": np.array([31.2304167], dtype=np.float64),
            "lon": np.array([121.4737001], dtype=np.float64),
            "alt": np.array([12.5], dtype=np.float64),
        }

        async def _fake_read(key: str) -> dict[str, Any]:
            return tbl

        with patch.object(worker, "_read_parquet_table", _fake_read):
            res = await worker._align_tables(anchor, {"gnss_0": "k"}, set())

        values = res["gnss_0"]["values"]
        # 坐标截断到 4 位小数（mask_coordinates），alt 不脱敏
        assert values["lat"][0] == pytest.approx(31.2304)
        assert values["lon"][0] == pytest.approx(121.4737)
        assert values["alt"][0] == pytest.approx(12.5)

    async def test_gnss_mask_disabled_keeps_precision(self) -> None:
        from app.services.align_worker import AlignWorker

        settings = MagicMock()
        settings.align_table_window_ms = 50
        settings.gnss_mask_enabled = False
        settings.gnss_descimal_places = 4
        worker = AlignWorker(settings, MagicMock(), MagicMock(), MagicMock())

        anchor = np.array([1000], dtype=np.int64)
        tbl = {
            "stamp_ns": np.array([1000], dtype=np.int64),
            "lat": np.array([31.2304167], dtype=np.float64),
            "lon": np.array([121.4737001], dtype=np.float64),
            "alt": np.array([12.5], dtype=np.float64),
        }

        async def _fake_read(key: str) -> dict[str, Any]:
            return tbl

        with patch.object(worker, "_read_parquet_table", _fake_read):
            res = await worker._align_tables(anchor, {"gnss_0": "k"}, set())

        values = res["gnss_0"]["values"]
        assert values["lat"][0] == pytest.approx(31.2304167)


# ========== #7 服务端规则引擎（IMU 事件挖掘） ==========

class TestRuleMining:
    def _stamps(self, n: int) -> np.ndarray:
        return np.arange(n, dtype=np.int64) * 10_000_000

    def test_hard_brake_detected(self) -> None:
        from app.services.slice_worker import _mine_event_timestamps

        n = 10
        ax = np.zeros(n)
        ay = np.zeros(n)
        az = np.full(n, 9.8)
        ax[5] = 5.0  # 单帧超刹车阈值(<碰撞阈值)
        events = _mine_event_timestamps(self._stamps(n), ax, ay, az, 4.0, 8.0)
        assert len(events) == 1
        assert events[0]["kind"] == "hard_brake"
        assert events[0]["ts_ns"] == int(self._stamps(n)[5])

    def test_collision_detected(self) -> None:
        from app.services.slice_worker import _mine_event_timestamps

        n = 10
        ax = np.zeros(n)
        ay = np.zeros(n)
        az = np.full(n, 9.8)
        ax[3:6] = 12.0  # 连续段，峰值 ≥ 碰撞阈值
        events = _mine_event_timestamps(self._stamps(n), ax, ay, az, 4.0, 8.0)
        assert len(events) == 1
        assert events[0]["kind"] == "collision"

    def test_continuous_segment_collapses_to_one_event(self) -> None:
        from app.services.slice_worker import _mine_event_timestamps

        n = 20
        ax = np.zeros(n)
        ay = np.zeros(n)
        az = np.full(n, 9.8)
        ax[2:8] = 6.0  # 一段连续超阈
        ax[14:16] = 6.0  # 另一段
        events = _mine_event_timestamps(self._stamps(n), ax, ay, az, 4.0, 8.0)
        assert len(events) == 2

    def test_no_event_when_quiet(self) -> None:
        from app.services.slice_worker import _mine_event_timestamps

        n = 10
        az = np.full(n, 9.8)
        events = _mine_event_timestamps(self._stamps(n), np.zeros(n), np.zeros(n), az, 4.0, 8.0)
        assert events == []

    def test_empty_input(self) -> None:
        from app.services.slice_worker import _mine_event_timestamps

        e = np.array([], dtype=np.float64)
        s = np.array([], dtype=np.int64)
        assert _mine_event_timestamps(s, e, e, e, 4.0, 8.0) == []


# ========== #14 网关转发认证 fail-closed ==========

class TestForwardedAuthFailClosed:
    def _request(self) -> Any:
        req = MagicMock()
        req.headers = {}
        return req

    def test_empty_secret_rejects(self) -> None:
        from hunter_common.exceptions import AuthenticationError

        from app.core.dependencies import _require_forwarded_auth

        with patch("app.core.dependencies.settings") as mock_settings:
            mock_settings.gateway_hmac_secret = ""
            mock_settings.gateway_identity_max_age_s = 300
            with pytest.raises(AuthenticationError):
                _require_forwarded_auth(self._request(), x_user_id="u1", x_roles="reader")

    def test_missing_headers_rejects(self) -> None:
        from hunter_common.exceptions import AuthenticationError

        from app.core.dependencies import _require_forwarded_auth

        with pytest.raises(AuthenticationError):
            _require_forwarded_auth(self._request(), x_user_id=None, x_roles=None)

    def test_valid_signature_passes(self) -> None:
        from app.core.dependencies import _require_forwarded_auth

        req = MagicMock()
        req.headers = {"x-user-id": "u1", "x-roles": "data-reader"}
        with patch("app.core.dependencies.settings") as mock_settings, \
                patch("app.core.dependencies.verify_identity_headers", return_value=True) as mock_verify:
            mock_settings.gateway_hmac_secret = "sekret"
            mock_settings.gateway_identity_max_age_s = 300
            mock_settings.data_read_role_set = {"data-reader"}
            user_id, roles = _require_forwarded_auth(req, x_user_id="u1", x_roles="data-reader")
        assert mock_verify.called
        assert user_id == "u1"
        assert roles == {"data-reader"}


# ========== #15 下行命令签名 fail-closed ==========

class TestCmdSignFailClosed:
    def test_empty_secret_refuses_to_sign(self) -> None:
        from app.services.cmd_service import CmdService

        settings = MagicMock()
        settings.gateway_hmac_secret = ""
        svc = CmdService(settings, MagicMock())
        with pytest.raises(RuntimeError):
            svc._sign("cmd-1", "veh-A1", "seal", {})

    def test_sign_produces_hex_with_secret(self) -> None:
        from app.services.cmd_service import CmdService

        settings = MagicMock()
        settings.gateway_hmac_secret = "sekret"
        svc = CmdService(settings, MagicMock())
        sig = svc._sign("cmd-1", "veh-A1", "seal", {"k": 1})
        assert isinstance(sig, str) and len(sig) == 64


# ========== #16 parsed 只读凭据隔离 ==========

class TestParsedClientIsolation:
    def test_separate_client_when_parsed_creds_set(self) -> None:
        from app.storage.minio_client import LakeStorage

        settings = MagicMock()
        settings.minio_endpoint_url = "http://localhost:9000"
        settings.minio_access_key = "main"
        settings.minio_secret_key = "main-secret"
        settings.minio_region = "us-east-1"
        settings.minio_parsed_access_key = "ro"
        settings.minio_parsed_secret_key = "ro-secret"

        clients = []
        with patch("app.storage.minio_client.boto3.client", side_effect=lambda *a, **k: clients.append(object()) or clients[-1]):
            storage = LakeStorage(settings)
        assert len(clients) == 2
        assert storage._parsed_client is not storage._client

    def test_reuse_main_client_when_no_parsed_creds(self) -> None:
        from app.storage.minio_client import LakeStorage

        settings = MagicMock()
        settings.minio_endpoint_url = "http://localhost:9000"
        settings.minio_access_key = "main"
        settings.minio_secret_key = "main-secret"
        settings.minio_region = "us-east-1"
        settings.minio_parsed_access_key = ""
        settings.minio_parsed_secret_key = ""

        with patch("app.storage.minio_client.boto3.client", return_value=object()):
            storage = LakeStorage(settings)
        assert storage._parsed_client is storage._client


# ========== #21 背压守卫 ==========

class TestBackpressureGuard:
    def _guard(self, threshold: int, throttle_s: float) -> Any:
        from app.consumers.backpressure import BackpressureGuard

        settings = MagicMock()
        settings.artifact_ingest_rate_limit = 1000
        settings.backpressure_check_interval_s = 60.0
        settings.task_queue_backpressure_threshold = threshold
        settings.backpressure_throttle_s = throttle_s
        guard = BackpressureGuard(settings, MagicMock())
        # 冻结深度缓存，避免真实打库
        guard._last_depth_check = time.monotonic()
        return guard

    async def test_throttle_when_queue_deep(self) -> None:
        guard = self._guard(threshold=100, throttle_s=2.0)
        guard._depth = 150
        assert await guard.throttle_delay() == 2.0

    async def test_no_throttle_when_queue_shallow(self) -> None:
        guard = self._guard(threshold=100, throttle_s=2.0)
        guard._depth = 10
        assert await guard.throttle_delay() == 0.0
