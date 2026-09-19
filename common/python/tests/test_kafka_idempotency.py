"""消费幂等守卫（``hunter_common.kafka.idempotency``）单测。

契约要求 ``consumer-groups.yaml#defaults.idempotency = required``：重复消息不得产生副作用。
"""
from __future__ import annotations

from hunter_common.kafka.idempotency import IdempotencyGuard


def test_compose_is_stable_and_collision_free() -> None:
    """同一键元组恒等；不同元组必须不同（空值与 None 归一处理）。"""
    assert IdempotencyGuard.compose("HUNTER-001", 1) == IdempotencyGuard.compose("HUNTER-001", 1)
    assert IdempotencyGuard.compose("HUNTER-001", 1) != IdempotencyGuard.compose("HUNTER-001", 2)
    assert IdempotencyGuard.compose("HUNTER-001", None) == IdempotencyGuard.compose("HUNTER-001", "")
    # 拼接口径明确：不做隐式截断，长度固定 64（sha256 hex）
    assert len(IdempotencyGuard.compose("a", "b")) == 64


async def test_claim_returns_true_only_once() -> None:
    guard = IdempotencyGuard(namespace="data-collector-telemetry")
    key = IdempotencyGuard.compose("HUNTER-001", 12580)
    assert await guard.claim(key) is True
    assert await guard.claim(key) is False
    assert len(guard) == 1


async def test_lru_eviction_allows_reprocessing_after_capacity() -> None:
    """容量上限（防内存膨胀）：被淘汰的键再次出现视为新消息。"""
    guard = IdempotencyGuard(namespace="g", max_size=2)
    assert await guard.claim("a") is True
    assert await guard.claim("b") is True
    assert await guard.claim("c") is True
    assert len(guard) == 2
    assert await guard.claim("a") is True


async def test_ttl_expiry_releases_key() -> None:
    """TTL 过期后同一幂等键可再次处理（对应 at-least-once 长期重投场景）。"""
    now = [1000.0]
    guard = IdempotencyGuard(namespace="g", ttl_s=10, clock=lambda: now[0])
    assert await guard.claim("k") is True
    now[0] += 5
    assert await guard.claim("k") is False
    now[0] += 6  # 累计 11s > TTL
    assert await guard.claim("k") is True


async def test_external_store_deduplicates_across_replicas() -> None:
    """注入分布式存储时：其他副本已处理 → 本副本跳过（多副本去重）。"""

    class FakeStore:
        def __init__(self) -> None:
            self.keys: set[str] = set()

        async def add(self, key: str) -> bool:
            if key in self.keys:
                return False
            self.keys.add(key)
            return True

    store = FakeStore()
    first = IdempotencyGuard(namespace="ota-service-ota-status", store=store)
    second = IdempotencyGuard(namespace="ota-service-ota-status", store=store)

    assert await first.claim("TASK:VEHICLE:SUCCESS") is True
    assert await second.claim("TASK:VEHICLE:SUCCESS") is False
    assert await second.claim("TASK:VEHICLE:SUCCESS") is False
    assert len(store.keys) == 1
