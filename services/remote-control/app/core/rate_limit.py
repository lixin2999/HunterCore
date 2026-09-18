"""Redis 固定窗口限流（附录 D；Key 模式 rate_limit:{user_id}:{api} 不可更改）。

- 计数窗口使用 RedisManager.incr_with_expire（pipeline 原子：INCR + 首次 NX 过期）；
- 超限 → HTTP 429（响应体 code 复用 5001，附录 A 无限流专用码禁止新增）+ Retry-After；
- 限流动作失败时放行（fail-open）并记录 ERROR —— 限流器故障不应阻断主链路；
- remote-control 专用阈值：POST /remote/session 单用户 1 QPS（附录 D）。
"""
from __future__ import annotations

import structlog
from hunter_common.redis import RedisManager
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.config import Settings
from app.services.metrics import RATE_LIMITED_TOTAL

logger = structlog.get_logger(service="remote-control")


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
    - 超限 → 429（code=5001）+ Retry-After + 指标 hunter_remote_control_rate_limited_total；
    - Redis 异常 → fail-open 放行并记录 ERROR。
    """
    key = f"rate_limit:{user_id}:{api}"
    try:
        current = await redis_manager.incr_with_expire(key, settings.rate_limit_window_seconds)
    except Exception as exc:  # noqa: BLE001 — fail-open：限流器故障不阻断主链路
        logger.error("rate_limiter_error", user_id=user_id, key=key, error=str(exc))
        return
    if current > limit_per_min:
        RATE_LIMITED_TOTAL.labels(endpoint=api).inc()
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
