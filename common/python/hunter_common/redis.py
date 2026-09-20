"""redis-py 异步客户端封装（Redis 7）。

- 异步客户端（异步优先），连接池管理，进程级单例
- 常用 Key 模式（设计文档：Redis Key 设计）：
  session:{user_id} / vehicle:status:{vehicle_id} / vehicle:online:set /
  rate_limit:{ip}:{api} / ota:progress:{task_id} / rc:session:{vehicle_id} / cache:scene:{scene_id}
"""
from __future__ import annotations

from typing import Any

import redis.asyncio as aioredis
from redis.asyncio import Redis

from hunter_common.config import HunterBaseConfig
from hunter_common.logging import get_logger

logger = get_logger("hunter_common.redis")


class RedisManager:
    """进程级 Redis 管理器（init 一次，全局复用）。"""

    def __init__(self, config: HunterBaseConfig) -> None:
        self._config = config
        self._client: Redis | None = None

    @property
    def client(self) -> Redis:
        if self._client is None:
            raise RuntimeError("RedisManager 未初始化，请先调用 init()")
        return self._client

    def init(self) -> None:
        """创建异步客户端与连接池（幂等）。"""
        if self._client is not None:
            return
        self._client = aioredis.from_url(
            self._config.redis_url,
            max_connections=self._config.redis_max_connections,
            decode_responses=True,  # 统一返回 str，避免二进制处理
        )
        logger.info(
            "redis_client_initialized",
            host=self._config.redis_host,
            port=self._config.redis_port,
            db=self._config.redis_db,
        )

    async def close(self) -> None:
        """关闭客户端与连接池（应用关闭时调用）。"""
        if self._client is not None:
            await self._client.aclose()
            self._client = None
            logger.info("redis_client_closed")

    async def ping(self) -> bool:
        """就绪探针用：验证 Redis 连通性。"""
        try:
            return bool(await self.client.ping())
        except Exception:
            logger.exception("redis_ping_failed")
            return False

    # ---------- 通用封装（Key 命名遵循设计文档 Redis Key 设计，禁止随意命名） ----------

    async def get(self, key: str) -> str | None:
        return await self.client.get(key)

    async def set(self, key: str, value: str, *, expire_seconds: int | None = None) -> None:
        """写入键值；expire_seconds 用于过期控制（如 session:{user_id} 30 分钟，G-04①）。"""
        await self.client.set(key, value, ex=expire_seconds)

    async def delete(self, *keys: str) -> int:
        return int(await self.client.delete(*keys))

    async def exists(self, key: str) -> bool:
        return bool(await self.client.exists(key))

    async def incr_with_expire(self, key: str, expire_seconds: int = 60) -> int:
        """计数器自增并设置过期时间（接口限流，如 rate_limit:{ip}:{api}，窗口 1 分钟）。

        仅在 key 首次创建时设置过期时间，保证窗口语义正确。
        """
        async with self.client.pipeline(transaction=True) as pipe:
            pipe.incr(key)
            pipe.expire(key, expire_seconds, nx=True)
            result = await pipe.execute()
        return int(result[0])

    async def acquire_lock(self, name: str, timeout: float = 10.0) -> Any:
        """获取分布式锁（上下文管理器；用于互斥场景，如远程操控会话创建）。

        用法::

            async with await redis_manager.acquire_lock("rc:lock:vehicle-1") as lock:
                ...
        """
        return self.client.lock(name, timeout=timeout)
