"""data-analytics 端点测试公共夹具：状态隔离 + 测试客户端 + 认证头。"""
from __future__ import annotations

from collections.abc import Iterator

import pytest
from httpx import ASGITransport, AsyncClient

from app.core import dependencies as deps
from app.main import app

#: 依赖缓存键（测试前后清理，避免替身跨用例串扰）
STATE_KEYS: tuple[str, ...] = (
    deps.KEY_STORAGE,
    deps.KEY_REPORT_REPOSITORY,
    deps.KEY_EVAL_DOCUMENT_REPOSITORY,
    deps.KEY_FLEET_REPOSITORY,
    deps.KEY_PIPELINE_REPOSITORY,
    deps.KEY_EVENT_CLIENT,
    deps.KEY_VEHICLE_CLIENT,
    deps.KEY_REPORT_SERVICE,
    deps.KEY_DASHBOARD_SERVICE,
    deps.KEY_EVALUATION_SERVICE,
    deps.KEY_COVERAGE_SERVICE,
    deps.KEY_CORNER_CASE_SERVICE,
    deps.KEY_CLOSABLES,
)

#: 网关注入身份头：analyst 具备 read + execute
ANALYST_HEADERS = {"X-User-Id": "u-1", "X-Roles": "analyst"}
#: operator 仅具备 read（报告生成应 403/1002）
OPERATOR_HEADERS = {"X-User-Id": "u-2", "X-Roles": "operator"}


@pytest.fixture()
def clean_state() -> Iterator[None]:
    """每个用例前后清空依赖缓存键（test_health.py 直连 app 无需此夹具）。"""
    for key in STATE_KEYS:
        if hasattr(app.state, key):
            delattr(app.state, key)
    yield
    for key in STATE_KEYS:
        if hasattr(app.state, key):
            delattr(app.state, key)


def api() -> AsyncClient:
    """ASGI 测试客户端（raise_app_exceptions=False 使全局 500 处理器生效）。"""
    return AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test",
    )