"""VIL 会话读取（只读 Redis：vil:session:{vehicle_id}）。

会话由 scene-service 写入；本模块只读并做 1s 本地缓存，避免 10-50Hz 遥测放大 Redis QPS。
"""
from __future__ import annotations

import json
import time
from collections.abc import Callable
from typing import Any

from hunter_common.exceptions import ServiceUnavailableError
from hunter_common.logging import get_logger
from hunter_common.redis_keys import vil_session_key
from redis.exceptions import RedisError

logger = get_logger("app.session_registry")


class SessionRegistry:
    """VIL 会话注册表（Redis → 本地短 TTL 缓存）。"""

    def __init__(
        self,
        redis_client: Any,
        *,
        cache_ttl_seconds: float = 1.0,
        max_entries: int = 10000,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._redis = redis_client
        self._ttl = max(0.0, cache_ttl_seconds)
        self._max_entries = max(1, max_entries)
        self._clock = clock
        self._cache: dict[str, tuple[float, dict[str, Any] | None]] = {}

    async def get(self, vehicle_id: str) -> dict[str, Any] | None:
        """读取会话；无会话/损坏返回 None，Redis 不可用抛 5001（消息重试后 DLQ）。"""
        now = self._clock()
        cached = self._cache.get(vehicle_id)
        if cached is not None and cached[0] > now:
            return cached[1]
        try:
            raw = await self._redis.get(vil_session_key(vehicle_id))
        except RedisError as exc:
            raise ServiceUnavailableError(
                "VIL 会话存储不可用", details={"vehicle_id": vehicle_id}
            ) from exc
        session: dict[str, Any] | None = None
        if raw is not None:
            try:
                payload = json.loads(raw)
                if isinstance(payload, dict):
                    session = payload
                else:
                    logger.warning("vil_session_not_object", vehicle_id=vehicle_id)
            except (TypeError, json.JSONDecodeError):
                logger.warning("vil_session_corrupted", vehicle_id=vehicle_id)
        self._cache[vehicle_id] = (now + self._ttl, session)
        # 缓存上界：车队规模固定时不会触发；异常 vehicle_id 洪泛时按过期优先淘汰，
        # 防止长期运行内存无界增长（V1.23.4）。
        if len(self._cache) > self._max_entries:
            expired = [key for key, (expires, _) in self._cache.items() if expires <= now]
            for key in expired:
                self._cache.pop(key, None)
            while len(self._cache) > self._max_entries:
                self._cache.pop(next(iter(self._cache)))
        return session

    def invalidate(self, vehicle_id: str) -> None:
        self._cache.pop(vehicle_id, None)

    def clear(self) -> None:
        self._cache.clear()


__all__ = ["SessionRegistry"]
