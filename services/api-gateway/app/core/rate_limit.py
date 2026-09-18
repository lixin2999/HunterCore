"""接口限流（设计文档附录 D + 契约 x-hunter-rate-limits）。

- 算法：Redis 滑动窗口计数，键 ``rate_limit:{ip}:{api}``，窗口 1 分钟
  （redis-keys.yaml 第 4 条；计数必须经 INCR + EXPIRE(NX) 原子组合，见
  ``hunter_common.redis.RedisManager.incr_with_expire``）
- 维度与阈值（附录 D，不可更改）：单 IP 200 QPS / 单用户 100 QPS；
  接口级（user 维度）：POST /api/v1/ota/versions 5、POST /api/v1/remote/session 1、
  GET /api/v1/data/telemetry 20
- 键占位符约定：``{ip}`` 槽位承载限流身份（匿名=客户端 IP、已认证=JWT sub），
  ``{api}`` 槽位承载作用域（``global``=全局维度，接口级=端点 slug）
- 可用性语义：``rate_limit`` 不在 redis-keys.yaml 强依赖清单（session/rc:session/
  rc:lock），Redis 故障时 fail-open 放行并告警；session 校验是强依赖（core/auth.py
  fail-closed 503）
- 超限响应：HTTP 429；附录 A 无限流专用错误码 → 响应体 ``code`` 统一复用 5001
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final

from hunter_common.logging import get_logger

logger = get_logger("app.core.rate_limit")

WINDOW_SECONDS: Final[int] = 60        # 附录 D：窗口 1 分钟（rate_limit ttl_seconds=60）
GLOBAL_SCOPE: Final[str] = "global"    # 全局维度作用域标识

#: 附录 D 接口级限流（user 维度）：(method, 对外路径) -> (作用域 slug, 单用户 QPS)
ENDPOINT_LIMITS: Final[dict[tuple[str, str], tuple[str, int]]] = {
    ("POST", "/api/v1/ota/versions"): ("ota-versions", 5),
    ("POST", "/api/v1/remote/session"): ("remote-session", 1),
    ("GET", "/api/v1/data/telemetry"): ("data-telemetry", 20),
}

__all__ = [
    "ENDPOINT_LIMITS",
    "GLOBAL_SCOPE",
    "WINDOW_SECONDS",
    "RateLimitExceeded",
    "RateLimitVerdict",
    "RateLimiter",
    "rate_limit_key",
]


def rate_limit_key(identity: str, scope: str) -> str:
    """构造限流键（契约模式 ``rate_limit:{ip}:{api}``，禁止其他拼法）。"""
    return f"rate_limit:{identity}:{scope}"


@dataclass(frozen=True)
class RateLimitVerdict:
    """一次放行的配额快照（限流头仅在 429 响应携带，放行路径无需使用）。"""

    limit: int
    remaining: int


class RateLimitExceeded(Exception):
    """限流触发（HTTP 429；附录 A 无专用错误码，响应体 code 统一复用 5001）。"""

    def __init__(self, *, limit: int, retry_after: int) -> None:
        self.limit = limit
        self.retry_after = max(1, int(retry_after))
        super().__init__(f"rate limit exceeded: limit={self.limit}, retry_after={self.retry_after}s")


class RateLimiter:
    """Redis 滑动窗口限流器。

    ``backend`` 为 ``RedisManager`` 或接口兼容对象（get/set/delete/exists/
    incr_with_expire/ping），便于单元测试注入伪实现；None 表示后端不可用
    （fail-open 放行）。
    """

    def __init__(self, backend: Any | None) -> None:
        self._backend = backend

    async def check(
        self,
        identity: str,
        scope: str,
        limit: int,
        *,
        window_seconds: int = WINDOW_SECONDS,
    ) -> RateLimitVerdict:
        """计数 +1 并判断是否超限；超限抛 :class:`RateLimitExceeded`。

        fail-open：后端不可用时放行并告警（限流器非强依赖，session 才是强依赖）。
        """
        if self._backend is None or limit <= 0:
            return RateLimitVerdict(limit=limit, remaining=limit)
        key = rate_limit_key(identity, scope)
        try:
            count = await self._backend.incr_with_expire(key, window_seconds)
        except Exception:  # noqa: BLE001 - fail-open：限流器非强依赖（redis-keys common_rules）
            logger.warning("rate_limit_backend_unavailable_fail_open", scope=scope)
            return RateLimitVerdict(limit=limit, remaining=limit)
        if count > limit:
            # Retry-After 近似为整窗：计数器 TTL 仅在首键创建时设置，无法精确回读剩余秒数
            raise RateLimitExceeded(limit=limit, retry_after=window_seconds)
        return RateLimitVerdict(limit=limit, remaining=limit - count)