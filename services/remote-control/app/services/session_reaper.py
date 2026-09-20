"""陈旧操控会话守护（审查 R7；契约 redis-keys pending #4 + schemas.SessionHeartbeat）。

背景（缺陷）：``rc:session:{vehicle_id}`` 无 TTL，且唯一清理路径是 ``DELETE /session/{id}``；
副本崩溃或客户端掉线（未走 DELETE）时残留会话会**永久**占用车辆互斥位，
同车后续任何接管请求恒返回 7001，需人工 DEL 才能恢复。

本守护依据契约 pending #4 的判定依据（「无心跳持续 N 秒 → 平台侧结束会话」）：
周期遍历 ``rc:session:*``，``last_heartbeat_at``（缺省退回 ``started_at``）超过
``RC_HEARTBEAT_END_AFTER_S``（默认 60s）→ 调 ``SessionService.close_stale_session``
以 ``reason=heartbeat_timeout`` 走完整「先安全后清理」流程（stop 帧 + session_end 信令 →
删 Hash → sidecar 归档），并回落 ``SESSIONS_ACTIVE`` 指标。

与操作员结束的差异：无操作员上下文（不校验数据权限）、幂等（会话已结束则跳过）。
"""
from __future__ import annotations

import asyncio
import time
from typing import Any

from hunter_common.logging import get_logger
from hunter_common.redis import RedisManager

from app.config import Settings
from app.schemas.common import SessionEndReason
from app.services.metrics import HEARTBEAT_TIMEOUT_TOTAL
from app.services.session_service import (
    FIELD_LAST_HEARTBEAT_AT,
    FIELD_SESSION_ID,
    FIELD_STARTED_AT,
    SessionService,
)

logger = get_logger("app.services.session_reaper")

#: 会话键扫描模式（契约键模式 ``rc:session:{vehicle_id}``，禁止其他拼法）
SESSION_SCAN_PATTERN = "rc:session:*"


class SessionReaper:
    """陈旧会话守护（周期任务；由应用 lifespan 启停）。"""

    def __init__(
        self,
        *,
        redis: RedisManager,
        session_service: SessionService,
        settings: Settings,
    ) -> None:
        self._redis = redis
        self._sessions = session_service
        self._settings = settings
        self._running = False
        self._task: asyncio.Task[None] | None = None

    @property
    def end_after_seconds(self) -> int:
        """无心跳持续多久判定会话失效（契约 rc_heartbeat_end_after_s，默认 60s）。"""
        return self._settings.rc_heartbeat_end_after_s

    async def run_once(self, *, now: float | None = None) -> list[str]:
        """单次对账：遍历 ``rc:session:*``，无心跳超阈值的会话强制结束。

        Returns:
            本次被强制结束的 session_id 列表（可观测性/用例断言）。
        """
        current = time.time() if now is None else now
        ended: list[str] = []
        async for raw_key in self._redis.client.scan_iter(
            match=SESSION_SCAN_PATTERN, count=self._settings.rc_sessions_scan_limit
        ):
            key = raw_key if isinstance(raw_key, str) else str(raw_key)
            mapping = await self._redis.client.hgetall(key)
            session_id = mapping.get(FIELD_SESSION_ID)
            if not session_id:
                # Hash 缺 session_id（半写状态）：直接清理，避免长期占用车辆互斥位
                await self._redis.client.delete(key)
                logger.warning("session_key_without_id_removed", key=key)
                continue
            last_beat = _to_float(mapping.get(FIELD_LAST_HEARTBEAT_AT)) or _to_float(
                mapping.get(FIELD_STARTED_AT)
            )
            if last_beat is None or current - last_beat <= self.end_after_seconds:
                continue
            closed = await self._sessions.close_stale_session(
                session_id, reason=SessionEndReason.HEARTBEAT_TIMEOUT
            )
            if closed:
                ended.append(session_id)
                # 心跳超时结束计数（契约 x-hunter-observability：heartbeat_timeout 指标）
                HEARTBEAT_TIMEOUT_TOTAL.labels(
                    vehicle_id=key.removeprefix("rc:session:")
                ).inc()
        return ended

    async def start(self) -> None:
        """启动周期守护任务（幂等）。"""
        if self._task is not None:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="session-reaper")

    async def stop(self) -> None:
        """停止守护任务（取消并等待收尾，避免任务泄漏）。"""
        self._running = False
        task, self._task = self._task, None
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:  # 守护任务停机异常不得影响服务关停
            logger.exception("session_reaper_stop_failed")

    async def _loop(self) -> None:
        """周期执行 :meth:`run_once`（Redis/依赖故障不中断循环，仅告警）。"""
        interval = max(1, self._settings.rc_session_reaper_interval_s)
        while self._running:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # 依赖故障时保持守护存活（下轮重试）
                logger.exception("session_reaper_failed")
            await asyncio.sleep(interval)


__all__ = ["SESSION_SCAN_PATTERN", "SessionReaper"]


def _to_float(value: Any, *, default: float | None = None) -> float | None:
    """宽松浮点解析（Redis 值统一为字符串；缺失/非法返回 default）。"""
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default