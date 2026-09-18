"""转发熔断器（契约 x-hunter-gateway-routes.circuit_breaker）。

默认策略（契约注释给出的建议默认，阈值经环境变量覆盖，待人工确认后回填契约）：
连续 5 次失败 或 最近窗口内超时率 > 50% → 熔断 30s → 半开放行单个探测请求。

熔断器为进程内状态、按目标服务隔离（K8s 多副本间不共享；后端不可用时各副本
独立进入熔断，对客户端语义一致：503 + code=5001，禁止透传裸错误）。
"""
from __future__ import annotations

import time
from collections import deque
from typing import Final

CLOSED: Final[str] = "closed"
OPEN: Final[str] = "open"
HALF_OPEN: Final[str] = "half_open"

__all__ = ["CLOSED", "HALF_OPEN", "OPEN", "CircuitBreaker"]


class CircuitBreaker:
    """进程内熔断器（closed/open/half_open 三态；半开仅放行单个探测请求）。"""

    def __init__(
        self,
        name: str,
        *,
        failure_threshold: int = 5,
        timeout_rate: float = 0.5,
        open_seconds: float = 30.0,
        window: int = 20,
    ) -> None:
        self.name = name
        self._failure_threshold = failure_threshold
        self._timeout_rate = timeout_rate
        self._open_seconds = open_seconds
        self._recent: deque[tuple[bool, bool]] = deque(maxlen=window)  # (成功?, 是否超时)
        self._consecutive_failures = 0
        self._opened_at: float | None = None
        self._half_open_probe_in_flight = False

    @property
    def state(self) -> str:
        """当前状态：closed / open / half_open（熔断窗口到期进入半开）。"""
        if self._opened_at is None:
            return CLOSED
        if time.monotonic() - self._opened_at >= self._open_seconds:
            return HALF_OPEN
        return OPEN

    def allow(self) -> bool:
        """是否放行本次转发（open 拒绝；half_open 仅放行单个探测请求）。"""
        state = self.state
        if state == CLOSED:
            return True
        if state == OPEN:
            return False
        # 半开：仅放行一个探测请求，避免半开期内并发探测打爆刚恢复的后端
        if self._half_open_probe_in_flight:
            return False
        self._half_open_probe_in_flight = True
        return True

    def record_success(self) -> None:
        """探测/转发成功：恢复闭合态并清零失败计数。"""
        self._recent.append((True, False))
        self._consecutive_failures = 0
        self._opened_at = None
        self._half_open_probe_in_flight = False

    def record_failure(self, *, timeout: bool = False) -> None:
        """转发失败（timeout=True 表示超时类失败，参与超时率统计）。"""
        self._recent.append((False, timeout))
        self._half_open_probe_in_flight = False
        if self.state == HALF_OPEN:
            # 半开探测失败：立即重新进入熔断（完整窗口）
            self._open()
            return
        self._consecutive_failures += 1
        if self._consecutive_failures >= self._failure_threshold:
            self._open()
            return
        samples = [is_timeout for _, is_timeout in self._recent]
        if (
            len(samples) >= self._failure_threshold
            and (sum(samples) / len(samples)) > self._timeout_rate
        ):
            # 窗口内超时率超阈值（契约默认 > 50%）：判定后端已不可用
            self._open()

    def _open(self) -> None:
        self._opened_at = time.monotonic()
        self._consecutive_failures = 0
        self._recent.clear()
        self._half_open_probe_in_flight = False