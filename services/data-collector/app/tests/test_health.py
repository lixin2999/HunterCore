"""data-collector 健康探针单元测试（不依赖外部基础设施）。"""
from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app


def _client() -> AsyncClient:
    """构造测试客户端；raise_app_exceptions=False 使全局 500 处理器生效（返回响应而非抛出）。"""
    return AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test",
    )


@pytest.mark.asyncio
async def test_healthz_ok() -> None:
    """/healthz 返回统一响应格式且 code=0。"""
    async with _client() as client:
        resp = await client.get("/healthz")
    assert resp.status_code == 200
    body = resp.json()
    assert body["code"] == 0
    assert set(body.keys()) == {"code", "message", "data", "request_id", "timestamp"}


@pytest.mark.asyncio
async def test_healthz_trace_id_header() -> None:
    """响应必须携带 X-Request-ID（trace_id 贯穿请求链路）。"""
    async with _client() as client:
        resp = await client.get("/healthz")
    assert resp.headers.get("X-Request-ID")


@pytest.mark.asyncio
async def test_unknown_error_unified_body() -> None:
    """未预期异常必须返回统一响应体 code=5000。"""
    async with _client() as client:
        resp = await client.get("/dev/null-500")
    assert resp.status_code == 500
    body = resp.json()
    assert body["code"] == 5000
    assert body["message"] == "服务器内部错误"
