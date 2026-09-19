"""场景详情缓存访问层（契约 Redis Key：``cache:scene:{scene_id}``，String(JSON)，TTL 3600s）。

- Key 模式与 TTL 取自契约 x-hunter-service.redis_keys，禁止新增 Key 模式；
- 缓存故障（Redis 不可达/脏数据）一律降级为「未命中」，不得阻断请求路径（可用性优先）。
"""
from __future__ import annotations

import json
from typing import Any
from uuid import UUID

from hunter_common.logging import get_logger
from hunter_common.redis import RedisManager

logger = get_logger("app.repositories.cache")

#: 场景详情缓存键模式（契约固定，不可更改）
SCENE_CACHE_KEY_PATTERN = "cache:scene:{scene_id}"


def scene_cache_key(scene_id: UUID | str) -> str:
    """拼装场景详情缓存键（唯一允许的拼装方式）。"""
    return SCENE_CACHE_KEY_PATTERN.format(scene_id=scene_id)


class SceneDetailCache:
    """场景详情缓存（GET /api/v1/scene/{scene_id}；任何写操作后主动失效）。"""

    def __init__(self, redis: RedisManager, ttl_seconds: int) -> None:
        self._redis = redis
        self._ttl_seconds = ttl_seconds

    async def get(self, scene_id: UUID) -> dict[str, Any] | None:
        """读取缓存（未命中/解析失败/Redis 故障返回 None，由调用方回源数据库）。"""
        try:
            raw = await self._redis.get(scene_cache_key(scene_id))
        except Exception:  # noqa: BLE001 - 缓存故障降级为未命中（可观测性由日志承担）
            logger.warning("scene_cache_get_failed", scene_id=str(scene_id))
            return None
        if not raw:
            return None
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("scene_cache_payload_invalid", scene_id=str(scene_id))
            return None
        return payload if isinstance(payload, dict) else None

    async def set(self, scene_id: UUID, payload: dict[str, Any]) -> None:
        """写入缓存（TTL 契约固定 1 小时；写入失败仅告警）。"""
        try:
            await self._redis.set(
                scene_cache_key(scene_id),
                json.dumps(payload, ensure_ascii=False),
                expire_seconds=self._ttl_seconds,
            )
        except Exception:  # noqa: BLE001
            logger.warning("scene_cache_set_failed", scene_id=str(scene_id))

    async def invalidate(self, scene_id: UUID) -> None:
        """失效缓存（PUT / DELETE / publish 等写操作后调用；失败仅告警）。"""
        try:
            await self._redis.delete(scene_cache_key(scene_id))
        except Exception:  # noqa: BLE001
            logger.warning("scene_cache_invalidate_failed", scene_id=str(scene_id))


__all__ = ["SCENE_CACHE_KEY_PATTERN", "SceneDetailCache", "scene_cache_key"]