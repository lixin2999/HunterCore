"""Redis 固定窗口限流（附录 D；Key 模式 rate_limit:{user_id}:{api} 不可更改）。

- 计数窗口使用 RedisManager.incr_with_expire（pipeline 原子：INCR + 首次 NX 过期）；
- 超限 → HTTP 429（code=5001，统一响应）+ Retry-After 响应头（保留状态与头，
  由全局 HTTP 异常处理器输出统一五字段格式）；
- 限流动作失败时放行（fail-open）并记录 ERROR —— 限流器故障不应阻断主链路；
  服务健康与限流正确性由 Prometheus 指标与网关限流兜底。
"""
from __future__ import annotations

import structlog
from hunter_common.redis import RedisManager
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.config import settings

logger = structlog.get_logger(service="data-collector")

#: GET /data/telemetry 的限流 Key 后缀（附录 D：单用户 20 QPS）
TELEMETRY_QUERY_API = "data:telemetry"


async def enforce_user_rate_limit(user_id: str, redis_manager: RedisManager) -> None:
    """单用户限流（异步 Redis；附录 D 语义）。

    - 未超限 → 计数 +1 放行；
    - 超限 → 429（code=5001）+ Retry-After；
    - Redis 异常 → fail-open 放行并记录 ERROR。
    """
    key = f"rate_limit:{user_id}:{TELEMETRY_QUERY_API}"
    try:
        current = await redis_manager.incr_with_expire(key, settings.rate_limit_window_seconds)
    except Exception as exc:  # noqa: BLE001 — fail-open：限流器故障不阻断主链路
        logger.error("rate_limiter_error", user_id=user_id, key=key, error=str(exc))
        return
    if current > settings.telemetry_query_rate_limit_per_min:
        logger.warning(
            "rate_limit_exceeded",
            user_id=user_id,
            key=key,
            current=current,
            limit=settings.telemetry_query_rate_limit_per_min,
        )
        raise StarletteHTTPException(
            status_code=429,
            detail=(
                "请求过于频繁，请稍后重试"
                f"（限流 {settings.telemetry_query_rate_limit_per_min} 次/"
                f"{settings.rate_limit_window_seconds} 秒）"
            ),
            headers={"Retry-After": str(settings.rate_limit_window_seconds)},
        )


__all__ = ["TELEMETRY_QUERY_API", "enforce_user_rate_limit"]
