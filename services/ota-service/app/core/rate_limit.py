"""Redis 固定窗口限流（附录 D；Key 模式 rate_limit:{scope}:{api} 不可更改）。

- 计数窗口使用 RedisManager.incr_with_expire（pipeline 原子：INCR + 首次 NX 过期）；
- 超限 → HTTP 429（响应体 code 复用 5001，附录 A 无限流专用码禁止新增）+ Retry-After；
- 限流动作失败时放行（fail-open）并记录 ERROR —— 限流器故障不应阻断主链路；
- 限流 Key 中的 scope 值：user 限流取 user_id（附录 D scope=user），ip 限流取客户端 IP
  （契约 Redis 表 rate_limit:{ip}:{api} 为 IP 维度实例，命名模式一致）。
"""
from __future__ import annotations

import structlog
from hunter_common.redis import RedisManager
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.config import Settings

logger = structlog.get_logger(service="ota-service")


async def enforce_user_rate_limit(
    user_id: str,
    redis_manager: RedisManager,
    *,
    api: str,
    limit_per_min: int,
    settings: Settings,
) -> None:
    """单用户限流（异步 Redis；附录 D 语义）。

    - 未超限 → 计数 +1 放行；
    - 超限 → 429（code=5001）+ Retry-After；
    - Redis 异常 → fail-open 放行并记录 ERROR。
    """
    key = f"rate_limit:{user_id}:{api}"
    try:
        current = await redis_manager.incr_with_expire(key, settings.rate_limit_window_seconds)
    except Exception as exc:  # noqa: BLE001 — fail-open：限流器故障不阻断主链路
        logger.error("rate_limiter_error", user_id=user_id, key=key, error=str(exc))
        return
    if current > limit_per_min:
        logger.warning(
            "rate_limit_exceeded",
            user_id=user_id,
            key=key,
            api=api,
            current=current,
            limit_per_min=limit_per_min,
        )
        raise StarletteHTTPException(
            status_code=429,
            detail=(
                f"请求过于频繁，请稍后重试（限流 {limit_per_min} 次/"
                f"{settings.rate_limit_window_seconds} 秒，api={api}）"
            ),
            headers={"Retry-After": str(settings.rate_limit_window_seconds)},
        )


__all__ = ["enforce_user_rate_limit"]
