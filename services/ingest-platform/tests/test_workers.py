"""Worker 纯逻辑单元测试（AlignWorker._extract_ts_from_keys / SliceWorker 切片算法）。"""
from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import numpy as np
import pytest

# ========== AlignWorker._extract_ts_from_keys ==========

class TestExtractTsFromKeys:
    """测试从 object key 文件名提取纳秒时间戳。"""

    @pytest.fixture
    def worker(self) -> Any:
        """构造 AlignWorker 实例（mock 外部依赖，仅测试纯方法）。"""
        from app.services.align_worker import AlignWorker

        settings = MagicMock()
        settings.anchor_camera = "camera_front"
        settings.max_lidar_dt_ms = 100
        settings.align_table_window_ms = 50
        storage = MagicMock()
        db_mgr = MagicMock()
        producer = MagicMock()
        return AlignWorker(settings, storage, db_mgr, producer)

    def test_single_image_key(self, worker: Any) -> None:
        keys = ["bags/v1/veh-A1/2026-01-01/bag-001/images/camera_front/1700000000000000000.jpg"]
        result = worker._extract_ts_from_keys(keys)
        assert result == [1700000000000000000]

    def test_pcd_range_key(self, worker: Any) -> None:
        keys = ["bags/v1/veh-A1/2026-01-01/bag-001/pointclouds/1700000000000000000_1700000001000000000.pcd"]
        result = worker._extract_ts_from_keys(keys)
        # 取第一段 start_ns
        assert result == [1700000000000000000]

    def test_multiple_keys(self, worker: Any) -> None:
        keys = [
            "a/b/c/100.jpg",
            "a/b/c/200.jpg",
            "a/b/c/300.jpg",
        ]
        result = worker._extract_ts_from_keys(keys)
        assert result == [100, 200, 300]

    def test_invalid_key_skipped(self, worker: Any) -> None:
        keys = [
            "a/b/c/notanumber.jpg",
            "a/b/c/123456789.png",
        ]
        result = worker._extract_ts_from_keys(keys)
        assert result == [123456789]

    def test_empty_list(self, worker: Any) -> None:
        assert worker._extract_ts_from_keys([]) == []


# ========== SliceWorker._slice_by_window ==========

class TestSliceByWindow:
    """测试固定窗口切片算法。"""

    @pytest.fixture
    def worker(self) -> Any:
        from app.services.slice_worker import SliceWorker

        settings = MagicMock()
        settings.slice_window_s = 10
        settings.slice_overlap_s = 2
        settings.slice_event_before_s = 5
        settings.slice_event_after_s = 10
        settings.anchor_camera = "camera_front"
        storage = MagicMock()
        db_mgr = MagicMock()
        producer = MagicMock()
        return SliceWorker(settings, storage, db_mgr, producer)

    def test_uniform_30s_bag(self, worker: Any) -> None:
        """30 秒 bag，10s 窗口 2s 重叠 → step=8s → 约 3-4 个 clip。"""
        # 模拟 10Hz 30 秒
        anchor_ts = np.arange(0, 30 * 1_000_000_000, 100_000_000, dtype=np.int64)
        clips = worker._slice_by_window(anchor_ts, "b-001", "rev1")
        # step=8s: windows start at 0, 8, 16, 24 → 4 windows within 30s
        assert len(clips) >= 3
        for c in clips:
            assert c["bag_id"] == "b-001"
            assert c["input_rev"] == "rev1"
            assert c["end_ns"] - c["start_ns"] == 10_000_000_000
            assert c["source"].value == "window"

    def test_short_bag_no_clips(self, worker: Any) -> None:
        """bag 时长 < 窗口 → 0 或 1 clip（取决于 >= 2 帧）。"""
        anchor_ts = np.array([0, 1_000_000_000], dtype=np.int64)  # 只有 1 秒
        clips = worker._slice_by_window(anchor_ts, "b-002", "rev1")
        # 2 帧在同一个窗口 → 1 clip
        assert len(clips) <= 1

    def test_very_short_bag(self, worker: Any) -> None:
        """只有 1 帧 → 无法形成窗口（>= 2 帧）。"""
        anchor_ts = np.array([1_000_000_000], dtype=np.int64)
        clips = worker._slice_by_window(anchor_ts, "b-003", "rev1")
        assert len(clips) == 0

    def test_window_boundaries(self, worker: Any) -> None:
        """验证相邻窗口有重叠。"""
        anchor_ts = np.arange(0, 20_000_000_000, 1_000_000_000, dtype=np.int64)
        clips = worker._slice_by_window(anchor_ts, "b", "r")
        # 20s, window=10s, step=8s → starts: 0, 8, 16 → [0-10), [8-18), [16-26)
        if len(clips) >= 2:
            # 第二个窗口 start = 第一个窗口 start + step
            assert clips[1]["start_ns"] - clips[0]["start_ns"] == 8_000_000_000


# ========== SliceWorker._slice_by_events ==========

class TestSliceByEvents:
    """测试事件驱动切片。"""

    @pytest.fixture
    def worker(self) -> Any:
        from app.services.slice_worker import SliceWorker

        settings = MagicMock()
        settings.slice_window_s = 10
        settings.slice_overlap_s = 2
        settings.slice_event_before_s = 5
        settings.slice_event_after_s = 10
        settings.anchor_camera = "camera_front"
        storage = MagicMock()
        db_mgr = MagicMock()
        producer = MagicMock()
        return SliceWorker(settings, storage, db_mgr, producer)

    def test_single_event(self, worker: Any) -> None:
        anchor_ts = np.arange(0, 30_000_000_000, 1_000_000_000, dtype=np.int64)
        bag = MagicMock()
        bag.manifest = {
            "event_marks": [
                {"ts_ns": 15_000_000_000, "tags": ["hard_brake"], "label": "intersection"},
            ]
        }
        clips = worker._slice_by_events(anchor_ts, bag, "b-001", "rev1")
        assert len(clips) == 1
        c = clips[0]
        assert c["source"].value == "event"
        assert c["start_ns"] == 10_000_000_000  # 15s - 5s
        assert c["end_ns"] == 25_000_000_000    # 15s + 10s
        assert "hard_brake" in c["tags"]
        assert c["scene_label"] == "intersection"

    def test_no_events(self, worker: Any) -> None:
        anchor_ts = np.arange(0, 10_000_000_000, 1_000_000_000, dtype=np.int64)
        bag = MagicMock()
        bag.manifest = {}
        clips = worker._slice_by_events(anchor_ts, bag, "b", "r")
        assert clips == []

    def test_event_clamped_to_bag_bounds(self, worker: Any) -> None:
        """事件靠近开头，before 超出 bag 范围 → clamp 到首帧。"""
        anchor_ts = np.arange(2_000_000_000, 20_000_000_000, 1_000_000_000, dtype=np.int64)
        bag = MagicMock()
        bag.manifest = {"event_marks": [{"ts_ns": 3_000_000_000, "tags": [], "label": None}]}
        clips = worker._slice_by_events(anchor_ts, bag, "b", "r")
        assert len(clips) == 1
        # start = max(2e9, 3e9-5e9) = 2e9
        assert clips[0]["start_ns"] == 2_000_000_000


# ========== QcWorker 权重与分数计算 ==========

class TestQcScoreCalculation:
    """测试 QC 加权评分逻辑。"""

    def test_weights_sum_to_one(self) -> None:
        from app.services.qc_worker import _WEIGHTS

        total = sum(_WEIGHTS.values())
        assert total == pytest.approx(1.0, abs=1e-6)

    def test_perfect_score(self) -> None:
        from app.services.qc_worker import _WEIGHTS, QcCheckItem

        checks = [QcCheckItem(name, 1.0, {}) for name in _WEIGHTS]
        score = sum(c.score * _WEIGHTS[c.name] for c in checks)
        assert score == pytest.approx(1.0)

    def test_zero_score(self) -> None:
        from app.services.qc_worker import _WEIGHTS, QcCheckItem

        checks = [QcCheckItem(name, 0.0, {}) for name in _WEIGHTS]
        score = sum(c.score * _WEIGHTS[c.name] for c in checks)
        assert score == pytest.approx(0.0)

    def test_partial_score(self) -> None:
        from app.services.qc_worker import _WEIGHTS, QcCheckItem

        # 9 项检查（§7.4 审查 #4）：权重和为 1.0
        checks = [
            QcCheckItem("hash_integrity", 1.0, {}),          # 0.22
            QcCheckItem("frame_gaps", 0.5, {}),              # 0.18 * 0.5
            QcCheckItem("monotonicity", 1.0, {}),            # 0.13
            QcCheckItem("message_count", 0.0, {}),           # 0.12 * 0
            QcCheckItem("image_decode", 0.8, {}),            # 0.08 * 0.8
            QcCheckItem("black_frozen_frames", 1.0, {}),     # 0.10
            QcCheckItem("pcd_valid", 1.0, {}),               # 0.09
            QcCheckItem("tf_available", 1.0, {}),            # 0.04
            QcCheckItem("calibration_conflict", 0.5, {}),    # 0.04 * 0.5
        ]
        expected = (
            0.22 + 0.18 * 0.5 + 0.13 + 0.0 + 0.08 * 0.8
            + 0.10 + 0.09 + 0.04 + 0.04 * 0.5
        )
        score = sum(c.score * _WEIGHTS[c.name] for c in checks)
        assert score == pytest.approx(expected)
