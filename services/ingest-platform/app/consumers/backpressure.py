"""背压守卫（§5.4：队列积压时向消费入口传导反压）。

两级信号：
1. 加工队列深度（tasks.state=pending 计数）≥ 阈值 → 命中后每条消息注入固定节流延迟；
2. 令牌桶速率上限（artifact_ingest_rate_limit msg/s）→ 平滑突发。

深度查询带 TTL 缓存（backpressure_check_interval_s），避免每条消息都打库。
"""
from __future__ import annotations

import asyncio
import time
from typing import Any

from hunter_common.logging import get_logger

from app.config import Settings
from app.repositories.tasks import TaskRepository

logger = get_logger("app.consumers.backpressure")


class BackpressureGuard:
    def __init__(self, settings: Settings, db_mgr: Any) -> None:
        self._settings = settings
        self._db_mgr = db_mgr
        self._depth = 0
        self._last_depth_check = 0.0
        self._rate = float(max(1, settings.artifact_ingest_rate_limit))
        self._tokens = self._rate
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def _refresh_depth(self) -> None:
        now = time.monotonic()
        if now - self._last_depth_check < self._settings.backpressure_check_interval_s:
            return
        try:
            async with self._db_mgr.session() as session:
                self._depth = await TaskRepository(session).queue_depth()
        except Exception as exc:  # noqa: BLE001
            logger.warning("backpressure_depth_query_failed", error=str(exc))
        self._last_depth_check = now

    async def throttle_delay(self) -> float:
        """当前应施加的单消息节流延迟（秒）。"""
        await self._refresh_depth()
        if self._depth >= self._settings.task_queue_backpressure_threshold:
            return self._settings.backpressure_throttle_s
        return 0.0

    async def acquire(self, cost: float = 1.0) -> None:
        """消费入口调用：令牌桶限速 + 队列深度节流（§5.4 背压传导）。"""
        async with self._lock:
            now = time.monotonic()
            self._tokens = min(
                self._rate, self._tokens + (now - self._last_refill) * self._rate
            )
            self._last_refill = now
            if self._tokens < cost:
                wait = (cost - self._tokens) / self._rate
                await asyncio.sleep(wait)
                self._tokens = 0.0
            else:
                self._tokens -= cost
        delay = await self.throttle_delay()
        if delay > 0:
            await asyncio.sleep(delay)

    @property
    def queue_depth(self) -> int:
        """最近一次缓存的队列深度（供指标暴露）。"""
        return self._depth


__all__ = ["BackpressureGuard"]
