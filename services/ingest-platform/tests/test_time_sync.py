"""时间同步工具单元测试（app/time_sync.py）。"""
from __future__ import annotations

import numpy as np
import pytest

from app.time_sync import (
    NanoSec,
    RosTime,
    align_batch_nearest,
    check_frame_gaps,
    check_monotonicity,
    linear_interpolate,
    stamp_to_ns,
)


# ========== stamp_to_ns ==========

class TestStampToNs:
    def test_ros_time(self) -> None:
        rt = RosTime(sec=1_700_000_000, nanosec=500_000_000)
        assert stamp_to_ns(rt) == 1_700_000_000_500_000_000

    def test_int_passthrough(self) -> None:
        ns = 1_700_000_000_000_000_000
        assert stamp_to_ns(ns) == ns

    def test_float_seconds(self) -> None:
        """float 视为 Unix 秒，×1e9 转纳秒。"""
        result = stamp_to_ns(1_700_000_000.0)
        assert result == 1_700_000_000_000_000_000

    def test_datetime(self) -> None:
        from datetime import datetime, timezone
        dt = datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc)
        result = stamp_to_ns(dt)
        assert result == int(dt.timestamp() * 1_000_000_000)

    def test_none_returns_none(self) -> None:
        """None 透传 None（设计：缺时间戳的消息不报错）。"""
        result = stamp_to_ns(None)
        assert result is None


# ========== check_monotonicity ==========

class TestCheckMonotonicity:
    def test_strictly_increasing(self) -> None:
        ts = [1, 2, 3, 4, 5]
        r = check_monotonicity(ts)
        assert r.is_monotonic is True
        assert r.violation_count == 0

    def test_equal_ok(self) -> None:
        """非严格单调（允许相等）。"""
        ts = [1, 2, 2, 3]
        r = check_monotonicity(ts)
        assert r.is_monotonic is True

    def test_one_violation(self) -> None:
        ts = [1, 3, 2, 4]
        r = check_monotonicity(ts)
        assert r.is_monotonic is False
        assert r.violation_count == 1

    def test_empty(self) -> None:
        r = check_monotonicity([])
        assert r.is_monotonic is True
        assert r.violation_count == 0


# ========== check_frame_gaps ==========

class TestCheckFrameGaps:
    def test_uniform_frames(self) -> None:
        """均匀间隔无丢帧。"""
        ts = list(range(0, 100_000_000, 10_000_000))  # 10ms 间隔
        r = check_frame_gaps(ts)
        assert r.gap_count == 0

    def test_one_gap(self) -> None:
        """1 处大于中位数 ×2 的跳变。"""
        # 正常间隔 10ms，1 处 100ms 跳变
        ts = [0, 10, 20, 30, 130, 140, 150]  # 单位 ms × 1e6 = ns
        ts = [int(x * 1e6) for x in ts]
        r = check_frame_gaps(ts)
        assert r.gap_count >= 1

    def test_short_list(self) -> None:
        r = check_frame_gaps([1])
        assert r.gap_count == 0


# ========== align_batch_nearest ==========

class TestAlignBatchNearest:
    def test_perfect_match(self) -> None:
        """完全对齐。"""
        anchor = np.array([100, 200, 300], dtype=np.int64)
        target = np.array([100, 200, 300], dtype=np.int64)
        idx, dt = align_batch_nearest(anchor, target, max_dt_ns=10)
        assert list(idx) == [0, 1, 2]
        assert all(d == 0 for d in dt)

    def test_within_window(self) -> None:
        anchor = np.array([100, 200, 300], dtype=np.int64)
        target = np.array([105, 195, 310], dtype=np.int64)
        idx, dt = align_batch_nearest(anchor, target, max_dt_ns=20)
        assert list(idx) == [0, 1, 2]
        assert dt[0] == 5
        assert dt[1] == 5
        assert dt[2] == 10

    def test_out_of_window(self) -> None:
        anchor = np.array([100, 200], dtype=np.int64)
        target = np.array([1000], dtype=np.int64)
        idx, dt = align_batch_nearest(anchor, target, max_dt_ns=50)
        assert idx[0] == -1
        assert idx[1] == -1

    def test_empty_target(self) -> None:
        anchor = np.array([100, 200], dtype=np.int64)
        target = np.array([], dtype=np.int64)
        idx, dt = align_batch_nearest(anchor, target)
        assert list(idx) == [-1, -1]


# ========== linear_interpolate ==========

class TestLinearInterpolate:
    def test_exact_match(self) -> None:
        x = np.array([1, 2, 3], dtype=np.float64)
        y = np.array([10.0, 20.0, 30.0])
        result = linear_interpolate(x, y, 2.0)
        assert result == pytest.approx(20.0)

    def test_interpolation(self) -> None:
        x = np.array([0.0, 10.0])
        y = np.array([0.0, 100.0])
        result = linear_interpolate(x, y, 5.0)
        assert result == pytest.approx(50.0)

    def test_out_of_range_returns_none(self) -> None:
        """超出范围返回 None（不插值不外推）。"""
        x = np.array([1.0, 2.0])
        y = np.array([10.0, 20.0])
        result = linear_interpolate(x, y, 3.0)
        assert result is None
