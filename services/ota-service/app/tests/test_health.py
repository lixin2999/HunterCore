"""ota-service 健康探针单元测试（不依赖外部基础设施）。"""
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
    """/healthz 返回统一响应格式且 code=0（data = HealthStatus 三字段）。"""
    async with _client() as client:
        resp = await client.get("/healthz")
    assert resp.status_code == 200
    body = resp.json()
    assert body["code"] == 0
    assert set(body.keys()) == {"code", "message", "data", "request_id", "timestamp"}
    assert body["data"]["status"] == "ok"
    assert body["data"]["service"] == "ota-service"


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


@pytest.mark.asyncio
async def test_metrics_endpoint_prometheus_format() -> None:
    """/metrics 输出 Prometheus 文本格式，且指标带 service 标签（供 infra/monitoring 抓取）。"""
    async with _client() as client:
        await client.get("/healthz")  # 先产生一次请求指标
        resp = await client.get("/metrics")
    assert resp.status_code == 200
    assert "hunter_http_requests_total" in resp.text
    assert 'service="ota-service"' in resp.text


@pytest.mark.asyncio
async def test_readyz_contract_ready_checks(client: AsyncClient) -> None:
    """就绪探针（契约 ReadyChecks）：data 必含 database/redis/minio，全部 true → 200。"""
    resp = await client.get("/readyz")
    assert resp.status_code == 200
    body = resp.json()
    assert body["code"] == 0
    assert body["data"] == {"database": True, "redis": True, "minio": True}


@pytest.mark.asyncio
async def test_readyz_503_when_dependency_down(ota_env: object) -> None:
    """MinIO 未就绪 → HTTP 503 + code=5001 + data.minio=false（契约 /readyz 503 响应）。"""
    from app.main import app as asgi_app

    asgi_app.state.storage = None  # 模拟 MinIO 探测失败（storage 未装配）
    try:
        async with AsyncClient(
            transport=ASGITransport(app=asgi_app, raise_app_exceptions=False),
            base_url="http://test",
        ) as http:
            resp = await http.get("/readyz")
    finally:
        asgi_app.state.storage = ota_env.storage  # type: ignore[attr-defined]
    assert resp.status_code == 503
    body = resp.json()
    assert body["code"] == 5001
    assert body["data"]["minio"] is False
