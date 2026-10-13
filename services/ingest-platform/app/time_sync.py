"""时间同步工具（设计文档 §4 时间处理规范）。

三类时间戳：
- 消息时间（header.stamp）：传感器采样时刻，精度最高，**默认统一使用**
- bag 记录时间（bag_ts_ns）：rosbag 录制时写入的时间（可能受 IO 延迟影响）
- 系统时间（主机时间）：车端主机操作系统时间（可能与 UTC 存在偏差）

本模块提供：
- 时间戳单位转换（ros2 Time 消息 → 纳秒整数）
- 时间单调性检查
- 最近邻对齐（numpy searchsorted）
- 线性插值（高频 IMU 可选）
"""
from __future__ import annotations

import bisect
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Union

import numpy as np
import numpy.typing as npt

# ========== 时间戳格式与转换 ==========

#: 纳秒时间戳（整数）
NanoSec = int

#: 支持的时间戳来源格式
StampLike = Union["RosTime", datetime, int, float, None]


@dataclass(frozen=True)
class RosTime:
    """ros2 builtin_interfaces/Time 消息（sec + nanosec）。"""
    sec: int
    nanosec: int

    def to_ns(self) -> NanoSec:
        """转换为纳秒整数（统一时间轴格式）。"""
        return self.sec * 1_000_000_000 + self.nanosec


def stamp_to_ns(stamp: StampLike) -> NanoSec | None:
    """将任意时间格式归一为纳秒整数。None 透传 None（缺时间戳的消息）。"""
    if stamp is None:
        return None
    if isinstance(stamp, RosTime):
        return stamp.to_ns()
    if isinstance(stamp, int):
        # 已经是纳秒级整数
        return stamp
    if isinstance(stamp, float):
        # 浮点秒（bag record time），转纳秒
        return int(stamp * 1_000_000_000)
    if isinstance(stamp, datetime):
        return int(stamp.timestamp() * 1_000_000_000)
    # 字典格式（rosbags 库 message 反序列化后的原始 dict）
    if isinstance(stamp, dict):
        sec = int(stamp.get("sec", 0))
        nanosec = int(stamp.get("nanosec", 0))
        return sec * 1_000_000_000 + nanosec
    raise TypeError(f"无法转换的时间格式: {type(stamp)}")


def ns_to_datetime(ns: NanoSec) -> datetime:
    """纳秒整数转 UTC datetime。"""
    return datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc)


def ns_to_sec(ns: NanoSec) -> float:
    """纳秒整数转秒（浮点）。"""
    return ns / 1_000_000_000


# ========== 时间单调性检查（QC 用，设计文档 §6 校验项） ==========

@dataclass
class MonoCheckResult:
    """单调性检查结果。"""
    is_monotonic: bool
    violation_count: int           # 非单调递增的次数
    first_violation_idx: int       # 首次违规下标（-1 表示无违规）
    max_backward_ms: float         # 最大回溯幅度（ms）


def check_monotonicity(timestamps: Sequence[NanoSec]) -> MonoCheckResult:
    """检查时间戳序列是否单调非递减（允许相等，即同 ns 不违规）。"""
    arr = np.asarray(timestamps, dtype=np.int64)
    if arr.size == 0:
        return MonoCheckResult(True, 0, -1, 0.0)
    diffs = np.diff(arr)
    violations = diffs < 0
    count = int(violations.sum())
    first_idx = int(np.argmax(violations)) if count else -1
    max_back_ms = float(-diffs[violations].min() / 1e6) if count else 0.0
    return MonoCheckResult(
        is_monotonic=count == 0,
        violation_count=count,
        first_violation_idx=first_idx,
        max_backward_ms=max_back_ms,
    )


# ========== 丢帧检测 ==========

@dataclass
class GapCheckResult:
    """丢帧检测结果（设计文档 §6 校验项）。"""
    gap_count: int                  # 超出 max_interval_factor 倍的间隔数
    max_interval_ns: NanoSec        # 实际最大间隔
    expected_interval_ns: NanoSec   # 推断的预期间隔（p50）
    jitter_p95_ms: float            # 间隔分布的 p95 偏差（ms）


def check_frame_gaps(
    timestamps: Sequence[NanoSec],
    *,
    max_interval_factor: float = 2.0,
) -> GapCheckResult:
    """基于时间戳序列检测丢帧（间隔超过预期 max_interval_factor 倍视为丢帧）。"""
    arr = np.asarray(timestamps, dtype=np.int64)
    if arr.size < 2:
        return GapCheckResult(0, 0, 0, 0.0)
    diffs = np.diff(arr)
    diffs = diffs[diffs > 0]   # 过滤非正（已由单调性检测处理）
    if diffs.size == 0:
        return GapCheckResult(0, 0, 0, 0.0)
    expected = int(np.median(diffs))
    max_interval = int(diffs.max())
    gap_count = int((diffs > expected * max_interval_factor).sum())
    jitter_ms = float(np.percentile(np.abs(diffs - expected), 95) / 1e6)
    return GapCheckResult(
        gap_count=gap_count,
        max_interval_ns=max_interval,
        expected_interval_ns=expected,
        jitter_p95_ms=jitter_ms,
    )


# ========== 最近邻对齐（§7.2 核心算法） ==========

def find_nearest(
    sorted_source_ns: npt.NDArray[np.int64],
    target_ns: NanoSec,
    *,
    max_dt_ns: NanoSec | None = None,
) -> tuple[int, NanoSec]:
    """在已排序的源时间戳数组中找最近的索引及偏差（ns）。

    Args:
        sorted_source_ns: 升序排列的源时间戳数组（纳秒）。
        target_ns: 目标时间戳（纳秒）。
        max_dt_ns: 最大允许偏差（ns）；超出返回 (-1, abs偏差)。

    Returns:
        (index, dt_ns)：index 为源数组下标，dt_ns 为绝对偏差（ns）。
        找不到满足 max_dt_ns 的则返回 (-1, 最小偏差)。
    """
    n = len(sorted_source_ns)
    if n == 0:
        return -1, target_ns  # 空数组，偏差直接等于 target

    pos: int = bisect.bisect_left(sorted_source_ns.tolist(), target_ns)

    best_idx = -1
    best_dt = np.iinfo(np.int64).max

    if pos < n:
        dt = int(sorted_source_ns[pos]) - target_ns
        if abs(dt) < best_dt:
            best_dt = abs(dt)
            best_idx = pos
    if pos > 0:
        dt = target_ns - int(sorted_source_ns[pos - 1])
        if dt < best_dt:
            best_dt = dt
            best_idx = pos - 1

    if max_dt_ns is not None and best_dt > max_dt_ns:
        return -1, best_dt
    return best_idx, best_dt


def align_batch_nearest(
    anchor_ns: npt.NDArray[np.int64],
    source_ns: npt.NDArray[np.int64],
    *,
    max_dt_ns: NanoSec | None = None,
) -> tuple[npt.NDArray[np.int64], npt.NDArray[np.int64]]:
    """批量最近邻对齐：对 anchor_ns 中每个时间戳，在 source_ns 中找最近邻。

    Args:
        anchor_ns: 锚点时间戳数组（升序）。
        source_ns: 源时间戳数组（升序）。
        max_dt_ns: 最大允许偏差（ns），超出则索引设为 -1。

    Returns:
        (indices, dt_ns)：两个与 anchor_ns 等长的数组；
        indices 为在 source_ns 中的下标（-1 表示未命中）；
        dt_ns 为绝对偏差（ns）。
    """
    if anchor_ns.size == 0 or source_ns.size == 0:
        return (
            np.full(anchor_ns.size, -1, dtype=np.int64),
            np.full(anchor_ns.size, np.iinfo(np.int64).max, dtype=np.int64),
        )

    # searchsorted: 找每个 anchor 在 source 中的插入位置
    insert_pos: npt.NDArray[np.int64] = np.searchsorted(source_ns, anchor_ns, side="left")

    # 候选：插入位置及左邻（截断到有效范围）
    right_idx = np.clip(insert_pos, 0, source_ns.size - 1)
    left_idx = np.clip(insert_pos - 1, 0, source_ns.size - 1)

    right_dt = np.abs(source_ns[right_idx].astype(np.int64) - anchor_ns)
    left_dt = np.abs(source_ns[left_idx].astype(np.int64) - anchor_ns)

    # 取更近侧
    use_left = left_dt <= right_dt
    best_idx = np.where(use_left, left_idx, right_idx)
    best_dt = np.where(use_left, left_dt, right_dt)

    # 过滤越界（insert_pos == 0 时左侧不存在，insert_pos == len 时右侧不存在）
    valid = (insert_pos <= source_ns.size) & (insert_pos >= 0)
    best_idx = np.where(valid, best_idx, np.int64(-1))
    best_dt = np.where(valid, best_dt, np.iinfo(np.int64).max)

    if max_dt_ns is not None:
        too_far = best_dt > max_dt_ns
        best_idx = np.where(too_far, np.int64(-1), best_idx)

    return best_idx.astype(np.int64), best_dt.astype(np.int64)


# ========== 线性插值（高频 IMU 可选，§7.2 步骤4） ==========

def linear_interpolate(
    source_ns: npt.NDArray[np.int64],
    source_values: npt.NDArray[np.float64],
    target_ns: NanoSec,
) -> float | None:
    """在 source_ns/source_values 上对 target_ns 做线性插值。

    超出范围或 source 不足 2 点则返回 None。
    """
    if len(source_ns) < 2:
        return None
    if target_ns < source_ns[0] or target_ns > source_ns[-1]:
        return None
    pos = int(np.searchsorted(source_ns, target_ns, side="left"))
    if pos == 0:
        return float(source_values[0])
    if pos >= len(source_ns):
        return float(source_values[-1])
    x0, x1 = float(source_ns[pos - 1]), float(source_ns[pos])
    y0, y1 = float(source_values[pos - 1]), float(source_values[pos])
    if x1 == x0:
        return y0
    ratio = (target_ns - x0) / (x1 - x0)
    return y0 + ratio * (y1 - y0)


__all__ = [
    "GapCheckResult",
    "MonoCheckResult",
    "NanoSec",
    "RosTime",
    "align_batch_nearest",
    "check_frame_gaps",
    "check_monotonicity",
    "find_nearest",
    "linear_interpolate",
    "ns_to_datetime",
    "ns_to_sec",
    "stamp_to_ns",
]
