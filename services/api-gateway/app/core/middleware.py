"""网关限流中间件（设计文档附录 D 五级限流的进程内两级 + 接口级）。

- 单 IP 200 QPS（rate_limit:{ip}:global）
- 单用户 100 QPS（rate_limit:{user_id}:global；签名有效即计数，无效 Token
  交由认证依赖统一拒绝，此处仅识别身份）
- 接口级（user 维度）：POST /api/v1/ota/versions 5 / POST /api/v1/remote/session 1 /
  GET /api/v1/data/telemetry 20
- 全局 10000 QPS 为集群级容量（由 Ingress/Nginx 入口承担，进程内不重复计数，
  避免 Redis 热点键；见 README「全局限流」说明）
- 仅作用于 ``/api/v1/**``（探针 /metrics/docs 豁免；CORS 预检 OPTIONS 豁免）
- 429：响应体 code=5001（附录 A 无专用错误码）+ Retry-After / X-RateLimit-* 头
"""
from __future__ import annotations

from typing import Any

from fastapi import Request, Response
from hunter_common.exceptions import HunterBaseException
from hunter_common.logging import get_logger
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

from app.config import Settings, settings
from app.core.http import get_client_ip, too_many_requests_response
from app.core.rate_limit import ENDPOINT_LIMITS, GLOBAL_SCOPE, RateLimiter, RateLimitExceeded
from app.core.security import decode_access_token_ignoring_expiry

logger = get_logger("app.core.middleware")

__all__ = ["GatewayRateLimitMiddleware"]


class GatewayRateLimitMiddleware(BaseHTTPMiddleware):
    """五级限流中间件（阈值来自附录 D，经 Settings 注入，禁止硬编码）。"""

    def __init__(self, app: object, settings: Settings = settings) -> None:
        super().__init__(app)  # type: ignore[arg-type]
        self._settings = settings

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        path = request.url.path
        if request.method == "OPTIONS" or not path.startswith("/api/v1"):
            return await call_next(request)
        limiter = RateLimiter(getattr(request.app.state, "redis", None))
        try:
            # 1) 单 IP 全局限流（匿名维度）
            await limiter.check(
                get_client_ip(request, self._settings),
                GLOBAL_SCOPE,
                self._settings.rate_limit_per_ip_qps,
            )
            # 2) 单用户全局限流 + 3) 接口级限流（user 维度，附录 D）
            claims = self._peek_jwt_claims(request)
            if claims is not None:
                user_id = str(claims["sub"])
                await limiter.check(user_id, GLOBAL_SCOPE, self._settings.rate_limit_per_user_qps)
                endpoint = ENDPOINT_LIMITS.get((request.method, path))
                if endpoint is not None:
                    scope, limit = endpoint
                    await limiter.check(user_id, scope, limit)
        except RateLimitExceeded as exc:
            logger.warning(
                "rate_limit_blocked",
                method=request.method,
                path=path,
                retry_after=exc.retry_after,
                limit=exc.limit,
            )
            return too_many_requests_response(exc)
        return await call_next(request)

    def _peek_jwt_claims(self, request: Request) -> dict[str, Any] | None:
        """最佳努力解析 JWT（签名校验、忽略有效期）：仅用于限流身份识别。"""
        authorization = request.headers.get("authorization") or ""
        if not authorization.lower().startswith("bearer "):
            return None
        token = authorization[7:].strip()
        if not token:
            return None
        try:
            return decode_access_token_ignoring_expiry(token, self._settings)
        except HunterBaseException:
            return None