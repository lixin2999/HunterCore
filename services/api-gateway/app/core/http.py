"""HTTP 辅助：客户端 IP 提取、429 统一响应构造（限流中间件与认证路由共用）。"""
from __future__ import annotations

import time

from fastapi import Request
from fastapi.responses import JSONResponse
from hunter_common.logging import get_logger, get_trace_id
from hunter_common.responses import error_response

from app.config import Settings, settings
from app.core.rate_limit import RateLimitExceeded

logger = get_logger("app.core.http")

__all__ = ["get_client_ip", "too_many_requests_response"]


def get_client_ip(request: Request, config: Settings | None = None) -> str:
    """客户端 IP：默认信任 ``X-Forwarded-For`` 首跳（K8s Ingress 覆盖语义）。

    ``trust_forwarded_for=False`` 时取 TCP 对端地址（直连部署防伪造）；
    配置来源：附录 D 限流身份 + 设计文档 14.5 节登录 IP 锁定共用该身份。
    """
    cfg = config or settings
    if cfg.trust_forwarded_for:
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            return forwarded.split(",")[0].strip()
    client = request.client
    return client.host if client is not None else "unknown"


def too_many_requests_response(exc: RateLimitExceeded) -> JSONResponse:
    """构造 HTTP 429 统一响应。

    附录 A 无限流专用错误码 → 响应体 ``code`` 统一复用 5001（禁止新增错误码），
    客户端以 HTTP 429 + ``Retry-After`` 为准；限流头为契约 RateLimit* 系列定义。
    """
    logger.warning(
        "rate_limit_triggered", retry_after=exc.retry_after, limit=exc.limit
    )
    payload = error_response(5001, "服务不可用", request_id=get_trace_id() or None)
    return JSONResponse(
        status_code=429,
        content=payload.model_dump(),
        headers={
            "Retry-After": str(exc.retry_after),
            "X-RateLimit-Limit": str(exc.limit),
            "X-RateLimit-Remaining": "0",
            "X-RateLimit-Reset": str(int(time.time()) + exc.retry_after),
        },
    )