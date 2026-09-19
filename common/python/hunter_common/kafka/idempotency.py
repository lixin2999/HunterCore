"""消费幂等守卫（契约 ``consumer-groups.yaml``：``defaults.idempotency = required``）。

契约按消费组声明幂等键（如 ``(vehicle_id, seq)``、``command_id``、``object_key``），
实现侧用 :meth:`IdempotencyGuard.compose` 把键元组归一为定长 sha256，避免不同组键长不一致。

存储策略：
- 默认进程内 LRU + TTL（K8s 单副本场景足够，多副本会重复处理但业务写库侧仍有唯一索引兜底）；
- 多副本严格去重需分布式存储，注入 :class:`IdempotencyStore` 实现即可。
  ⚠ 分布式存储若要落地为 Redis，Key 模式必须**先**登记到 ``contracts/database/redis-keys.yaml``
  （Redis Key 命名模式不可在实现中私自发明），因此本模块只提供注入点、不内置 Redis 键名。
"""
from __future__ import annotations

import hashlib
import time
from collections import OrderedDict
from collections.abc import Callable
from typing import Protocol, runtime_checkable

from hunter_common.logging import get_logger

logger = get_logger("hunter_common.kafka.idempotency")

#: 幂等键拼接分隔符（不可出现在业务键中：\x1f 为 ASCII Unit Separator）
_KEY_SEPARATOR = "\x1f"


@runtime_checkable
class IdempotencyStore(Protocol):
    """分布式幂等存储协议（多副本部署注入；Key 命名须先在 redis-keys.yaml 登记）。"""

    async def add(self, key: str) -> bool:
        """原子写入：返回 True 表示首次写入（此前未见过），False 表示已存在。"""
        ...


class IdempotencyGuard:
    """消费幂等守卫：``claim()`` 返回 True 才允许执行业务逻辑。

    用法（幂等键来历见 ``consumer-groups.yaml`` 各消费组 ``idempotency_key``）::

        guard = IdempotencyGuard(namespace="data-collector-telemetry")
        key = IdempotencyGuard.compose(payload["vehicle_id"], payload["seq"])
        if not await guard.claim(key):
            return  # 重复消息：跳过，不产生副作用
    """

    def __init__(
        self,
        *,
        namespace: str,
        max_size: int = 10_000,
        ttl_s: int = 86_400,
        store: IdempotencyStore | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._namespace = namespace
        self._max_size = max(max_size, 1)
        self._ttl_s = max(ttl_s, 1)
        self._store = store
        self._clock = clock
        # key -> 过期时刻（单调时钟秒）；OrderedDict 实现 LRU（最旧在首位）
        self._seen: OrderedDict[str, float] = OrderedDict()

    @property
    def namespace(self) -> str:
        return self._namespace

    @staticmethod
    def compose(*parts: object) -> str:
        """把幂等键元组归一为定长 sha256 字符串（同一元组恒等，跨进程稳定）。"""
        joined = _KEY_SEPARATOR.join("" if part is None else str(part) for part in parts)
        return hashlib.sha256(joined.encode("utf-8")).hexdigest()

    async def claim(self, key: str) -> bool:
        """认领幂等键：True = 首次处理（可继续），False = 重复消息（必须跳过）。"""
        now = self._clock()
        if self._is_duplicate(key, now):
            return False
        if self._store is not None:
            added = await self._store.add(f"{self._namespace}:{key}")
            if not added:  # 其他副本已处理（跨副本去重）
                self._remember(key, now)
                return False
        self._remember(key, now)
        return True

    # ---------- 内部 ----------

    def _is_duplicate(self, key: str, now: float) -> bool:
        self._purge_expired(now)
        return key in self._seen

    def _remember(self, key: str, now: float) -> None:
        self._seen[key] = now + self._ttl_s
        self._seen.move_to_end(key)
        while len(self._seen) > self._max_size:  # LRU 淘汰：容量上限由配置控制
            self._seen.popitem(last=False)

    def _purge_expired(self, now: float) -> None:
        while self._seen:
            _, expires_at = next(iter(self._seen.items()))
            if expires_at > now:
                break
            self._seen.popitem(last=False)

    def __len__(self) -> int:
        """当前缓存键数量（监控/测试用；不含已过期但未清理的条目）。"""
        self._purge_expired(self._clock())
        return len(self._seen)
