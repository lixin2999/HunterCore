"""API 集成测试：以真实进程（uvicorn 子进程）验证统一契约面。

覆盖点（契约优先）：
- 统一响应格式五字段 + code=0
- trace_id 透传（X-Request-ID）与自动生成
- `/readyz` 依赖不可用返回 503 + code=5001（第 3 条错误码表）
- 实现路由 ⊆ 契约路由（禁止契约外端点）；契约端点未实现时不得返回 200 假成功
- `/metrics` 为 Prometheus 文本（契约明确例外）
- 未知路径返回统一响应体（不得裸 404 文本）
"""
from __future__ import annotations

import uuid
from typing import Any

import httpx
import pytest
from hunter_common.exceptions import ErrorCode

from tests.support import contracts

pytestmark = pytest.mark.integration

#: 统一响应格式五字段（系统关键约束第 2 条）
RESPONSE_FIELDS = {"code", "message", "data", "request_id", "timestamp"}


def _client(service: Any) -> httpx.AsyncClient:
    """构造指向服务进程的 HTTP 客户端。"""
    return httpx.AsyncClient(base_url=service.base_url, timeout=10.0)


@pytest.mark.parametrize("service_name", contracts.SERVICE_NAMES)
async def test_healthz_unified_response(service_factory: Any, service_name: str) -> None:
    """/healthz 返回统一响应格式且 code=0（全部 6 个服务）。"""
    service = service_factory(service_name)
    async with _client(service) as client:
        resp = await client.get("/healthz")
    assert resp.status_code == 200, f"{service_name} /healthz 异常：{resp.text}\n{service.logs()}"
    body = resp.json()
    assert set(body) == RESPONSE_FIELDS
    assert body["code"] == ErrorCode.SUCCESS
    assert body["message"] == "success"


@pytest.mark.parametrize("service_name", contracts.SERVICE_NAMES)
async def test_trace_id_propagation(service_factory: Any, service_name: str) -> None:
    """上游 X-Request-ID 必须透传（全链路 trace_id 可观测性约束）。"""
    service = service_factory(service_name)
    trace_id = f"l5-{uuid.uuid4().hex[:12]}"
    async with _client(service) as client:
        resp = await client.get("/healthz", headers={"X-Request-ID": trace_id})
    assert resp.headers.get("X-Request-ID") == trace_id


@pytest.mark.parametrize("service_name", contracts.SERVICE_NAMES)
async def test_trace_id_generated_when_absent(service_factory: Any, service_name: str) -> None:
    """未携带 X-Request-ID 时必须自动生成（不得为空）。"""
    service = service_factory(service_name)
    async with _client(service) as client:
        resp = await client.get("/healthz")
    assert resp.headers.get("X-Request-ID")


@pytest.mark.parametrize("service_name", contracts.SERVICE_NAMES)
async def test_readyz_reports_dependency_state(service_factory: Any, service_name: str) -> None:
    """/readyz 依据 DB/Redis 连通性返回 200 或 503；503 时必须为 code=5001。"""
    service = service_factory(service_name)
    async with _client(service) as client:
        resp = await client.get("/readyz")
    body = resp.json()
    assert set(body) == RESPONSE_FIELDS
    assert set(body["data"]) == {"database", "redis"}
    if resp.status_code == 200:
        assert body["code"] == ErrorCode.SUCCESS
        assert all(body["data"].values())
    else:
        assert resp.status_code == 503
        assert body["code"] == ErrorCode.SERVICE_UNAVAILABLE
@pytest.mark.parametrize("service_name", contracts.SERVICE_NAMES)
async def test_implemented_routes_subset_of_contract(service_factory: Any, service_name: str) -> None:
    """实现路由必须 ⊆ 契约路由（禁止自行新增端点；契约是单一事实来源）。"""
    service = service_factory(service_name)
    contract_paths = set(contracts.openapi_paths(service_name))
    async with _client(service) as client:
        spec = (await client.get("/openapi.json")).json()
    extra = set(spec["paths"]) - contract_paths
    assert not extra, f"{service_name} 出现契约外端点（必须先更新 contracts/openapi）: {sorted(extra)}"


@pytest.mark.parametrize("service_name", contracts.SERVICE_NAMES)
async def test_contract_endpoints_not_fake_success(service_factory: Any, service_name: str) -> None:
    """契约端点若尚未实现，不得返回 200 假成功（避免前端联调被误导）。"""
    service = service_factory(service_name)
    async with _client(service) as client:
        spec = (await client.get("/openapi.json")).json()
    implemented = set(spec["paths"])
    declared = set(contracts.openapi_paths(service_name))
    internal = contracts.openapi_internal_paths(service_name)
    unimplemented = sorted(declared - implemented - internal)
    if not unimplemented:
        pytest.skip(f"{service_name} 契约端点已全部实现")
    async with _client(service) as client:
        for path in unimplemented:
            probe = path.replace("**", "probe").replace("{", "").replace("}", "")
            resp = await client.get(probe)
            assert resp.status_code in {404, 405}, (
                f"{service_name} {probe} 未实现却返回 {resp.status_code}"
            )


@pytest.mark.parametrize("service_name", contracts.SERVICE_NAMES)
async def test_metrics_prometheus_text(service_factory: Any, service_name: str) -> None:
    """/metrics 为 Prometheus 文本且带 service 标签（契约例外，非统一 JSON）。"""
    service = service_factory(service_name)
    async with _client(service) as client:
        await client.get("/healthz")
        resp = await client.get("/metrics")
    assert resp.status_code == 200
    assert "hunter_http_requests_total" in resp.text
    assert f'service="{service_name}"' in resp.text


@pytest.mark.parametrize("service_name", contracts.SERVICE_NAMES)
async def test_unknown_path_unified_error_body(service_factory: Any, service_name: str) -> None:
    """未知路径必须返回统一响应体（含 request_id），不得裸文本 404。"""
    service = service_factory(service_name)
    async with _client(service) as client:
        resp = await client.get("/__l5_unknown__")
    assert resp.status_code == 404
    body = resp.json()
    assert set(body) == RESPONSE_FIELDS
    assert body["code"] in {ErrorCode.RESOURCE_NOT_FOUND, ErrorCode.INVALID_PARAM}
    assert body["request_id"]


@pytest.mark.parametrize("service_name", contracts.SERVICE_NAMES)
async def test_service_port_matches_contract(service_factory: Any, service_name: str) -> None:
    """服务监听端口必须与契约登记端口一致（8080-8085）。"""
    service = service_factory(service_name)
    assert service.port == contracts.service_port(service_name)
    async with _client(service) as client:
        assert (await client.get("/healthz")).status_code == 200
