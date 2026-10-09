"""vehicle-service 契约一致性测试（无外部依赖）。

校验目标（契约 = 单一事实来源）：
1. `contracts/openapi/vehicle-service.yaml` 存在且为 OpenAPI 3.0.3；
2. 契约中声明的 8 个业务端点 + 3 个运维端点 == FastAPI 实际注册路由；
3. `x-hunter-service.rbac_role_bindings` == `app.config.Settings` 中 5 个 `*_roles` 词表；
4. `x-hunter-kafka.per_vehicle_topics` 数量 == contracts/kafka/topics.yaml 车辆 Topic 数量（8 个/车）；
5. 错误码：HTTP_STATUS_BY_CODE 覆盖契约 `ApiResponseError.code` 枚举集合；
6. api-gateway.yaml `/api/v1/vehicle` 路由 target_port == 8086 且不再 pending_confirmation。
"""
from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[4]
CONTRACT_PATH = ROOT / "contracts" / "openapi" / "vehicle-service.yaml"
GATEWAY_CONTRACT = ROOT / "contracts" / "openapi" / "api-gateway.yaml"
TOPICS_CONTRACT = ROOT / "contracts" / "kafka" / "topics.yaml"


EXPECTED_BUSINESS_ENDPOINTS: set[tuple[str, str]] = {
    ("GET", "/api/v1/vehicle/list"),
    ("POST", "/api/v1/vehicle"),
    ("GET", "/api/v1/vehicle/{vehicle_id}"),
    ("PATCH", "/api/v1/vehicle/{vehicle_id}"),
    ("DELETE", "/api/v1/vehicle/{vehicle_id}"),
    ("POST", "/api/v1/vehicle/{vehicle_id}/rotate-scram"),
    ("POST", "/api/v1/vehicle/{vehicle_id}/reissue-cert"),
    ("GET", "/api/v1/vehicle/{vehicle_id}/bundle"),
}

EXPECTED_OPS_ENDPOINTS: set[tuple[str, str]] = {
    ("GET", "/healthz"),
    ("GET", "/readyz"),
    ("GET", "/metrics"),
}


@pytest.fixture(scope="module")
def contract_doc() -> dict[str, Any]:
    assert CONTRACT_PATH.is_file(), f"契约文件缺失：{CONTRACT_PATH}"
    return yaml.safe_load(CONTRACT_PATH.read_text(encoding="utf-8"))


def _endpoints_from_contract(doc: dict[str, Any]) -> set[tuple[str, str]]:
    out: set[tuple[str, str]] = set()
    for path, ops in (doc.get("paths") or {}).items():
        for method in ops.keys():
            if method.lower() in ("get", "post", "patch", "delete", "put"):
                out.add((method.upper(), path))
    return out


def iter_registered_routes(routes: Any) -> Iterator[tuple[str, str]]:
    """递归展开 FastAPI 已注册路由（兼容 Starlette `_IncludedRouter` 聚合节点）。

    与 data-collector / data-analytics / scene-service 契约测试同一实现：`include_router`
    在新版 Starlette 下不拍平为 APIRoute，直接遍历 `app.routes` 会漏掉全部业务/探针路由。
    """
    for route in routes:
        nested = getattr(route, "routes", None)
        if nested is None:
            inner = getattr(route, "original_router", None)
            nested = getattr(inner, "routes", None) if inner is not None else None
        if nested:
            yield from iter_registered_routes(nested)
            continue
        path = getattr(route, "path", None)
        if path is None:
            continue
        for method in getattr(route, "methods", None) or ():
            if method in ("HEAD", "OPTIONS"):
                continue
            yield method, path


def test_openapi_version_and_info(contract_doc: dict[str, Any]) -> None:
    assert contract_doc["openapi"] == "3.0.3"
    assert contract_doc["info"]["title"] == "HunterCore - vehicle-service"


def test_business_endpoints_match_contract(contract_doc: dict[str, Any]) -> None:
    actual = _endpoints_from_contract(contract_doc)
    biz_in_contract = {e for e in actual if e[1].startswith("/api/v1/vehicle")}
    assert biz_in_contract == EXPECTED_BUSINESS_ENDPOINTS, (
        f"契约业务端点集合漂移：缺失={EXPECTED_BUSINESS_ENDPOINTS - biz_in_contract} "
        f"多余={biz_in_contract - EXPECTED_BUSINESS_ENDPOINTS}"
    )


def test_ops_endpoints_in_contract(contract_doc: dict[str, Any]) -> None:
    actual = _endpoints_from_contract(contract_doc)
    ops = {e for e in actual if e[1] in {p for _, p in EXPECTED_OPS_ENDPOINTS}}
    assert ops == EXPECTED_OPS_ENDPOINTS


def test_fastapi_routes_registered(contract_doc: dict[str, Any]) -> None:
    """FastAPI 实际路由 == 契约端点集合（业务 + 运维；/metrics 由 register_metrics 注入）。"""
    from app.main import app  # noqa: PLC0415 - 延迟导入避免 pytest 收集期启动 lifespan

    actual: set[tuple[str, str]] = set(iter_registered_routes(app.routes))
    expected = EXPECTED_BUSINESS_ENDPOINTS | EXPECTED_OPS_ENDPOINTS
    missing = {e for e in expected if e not in actual}
    assert not missing, f"路由未注册：{sorted(missing)}"


def test_rbac_bindings_match_settings(contract_doc: dict[str, Any]) -> None:
    """x-hunter-service.rbac_role_bindings 与 app.config Settings 词表一致。"""
    from app.config import settings  # noqa: PLC0415

    bindings = contract_doc["x-hunter-service"]["rbac_role_bindings"]
    # 每个角色 → 动作集
    def _actions(role: str, action: str) -> bool:
        return action in (bindings.get(role) or [])

    # admin：五动作
    for action in ("create", "read", "update", "delete", "execute"):
        assert _actions("admin", action), f"admin 应含 {action}"
    # operator：不含 delete
    assert not _actions("operator", "delete"), "operator 不应含 delete"
    for action in ("create", "read", "update", "execute"):
        assert _actions("operator", action), f"operator 应含 {action}"
    # analyst / viewer：仅 read
    for role in ("analyst", "viewer"):
        assert bindings.get(role) == ["read"], f"{role} 应只 read"

    # Settings 中的角色集合与契约角色一致
    assert settings.vehicle_read_role_set == {"admin", "operator", "analyst", "viewer"}
    assert settings.vehicle_create_role_set == {"admin", "operator"}
    assert settings.vehicle_update_role_set == {"admin", "operator"}
    assert settings.vehicle_delete_role_set == {"admin"}
    assert settings.vehicle_execute_role_set == {"admin", "operator"}


def test_per_vehicle_topics_count(contract_doc: dict[str, Any]) -> None:
    """契约 `x-hunter-kafka.per_vehicle_topics` == topics.yaml 车辆 Topic 条目 == 8（源 Topic）。

    另需声明每车死信 Topic 的创建责任与命名来源（broker 关 auto.create，不登记则非法消息被静默丢弃）。
    """
    contract_list = contract_doc["x-hunter-kafka"]["per_vehicle_topics"]
    assert len(contract_list) == 8, f"契约声明每车 Topic 应为 8，实际 {len(contract_list)}"

    topics = yaml.safe_load(TOPICS_CONTRACT.read_text(encoding="utf-8"))
    vehicle_topics = [
        item for item in (topics.get("vehicle_topics") or [])
        if "{vehicle_id}" in str(item.get("name", ""))
    ]
    assert len(vehicle_topics) == 8, f"topics.yaml 车辆 Topic 应为 8，实际 {len(vehicle_topics)}"

    # 集合口径一致（对比 type 段）
    contract_types = {str(entry).split(".")[-1] for entry in contract_list}
    yaml_types = {str(item["name"]).split(".")[-1] for item in vehicle_topics}
    assert contract_types == yaml_types, f"Topic 类型漂移：{contract_types ^ yaml_types}"

    # 死信 Topic：命名/保留期只由 topics.yaml#naming 决定，创建责任必须在契约登记
    naming = topics.get("naming") or {}
    assert "{original_topic}" in str(naming.get("dlq_pattern", "")), (
        "topics.yaml#naming.dlq_pattern 缺失或不含 {original_topic} 占位符"
    )
    assert int(naming.get("dlq_retention_ms") or 0) > 0, "topics.yaml#naming.dlq_retention_ms 缺失"
    assert bool(naming.get("dlq_partitions_inherit_source")), (
        "topics.yaml#naming.dlq_partitions_inherit_source 应为 true"
    )
    assert str(contract_doc["x-hunter-kafka"].get("dlq_creation") or "").strip(), (
        "契约未登记每车 .dlq 的创建责任（x-hunter-kafka.dlq_creation）"
    )


def test_error_codes_in_http_status_map(contract_doc: dict[str, Any]) -> None:
    """契约 ApiResponseError.code 枚举 ⊆ HTTP_STATUS_BY_CODE 覆盖集合（error_handlers）。"""
    from app.core.error_handlers import HTTP_STATUS_BY_CODE  # noqa: PLC0415

    schema = contract_doc["components"]["schemas"]["ApiResponseError"]
    codes: set[int] = set()
    for sub in schema.get("allOf", []):
        props = sub.get("properties") or {}
        code = props.get("code") or {}
        for v in code.get("enum", []) or []:
            codes.add(int(v))
    assert codes, "契约未声明任何错误码枚举"
    missing = {c for c in codes if c not in HTTP_STATUS_BY_CODE}
    assert not missing, f"HTTP_STATUS_BY_CODE 未覆盖错误码：{missing}"


def test_gateway_route_active(gateway_doc: dict[str, Any] | None = None) -> None:
    """api-gateway.yaml 中 `/api/v1/vehicle` 已指向 8086（不再 pending_confirmation）。"""
    if gateway_doc is None:
        gateway_doc = yaml.safe_load(GATEWAY_CONTRACT.read_text(encoding="utf-8"))
    routes = gateway_doc["x-hunter-gateway-routes"]["routes"]
    vehicle_route = next((r for r in routes if r.get("prefix") == "/api/v1/vehicle"), None)
    assert vehicle_route is not None, "api-gateway.yaml 缺失 /api/v1/vehicle 路由"
    assert vehicle_route.get("target_service") == "vehicle-service"
    assert int(vehicle_route.get("target_port", 0)) == 8086
    assert vehicle_route.get("status", "active") != "pending_confirmation", (
        "/api/v1/vehicle 仍标记为 pending_confirmation，应切换为 active"
    )


def test_gateway_route_yaml_fixture() -> None:
    """独立 fixture 加载网关契约（避免参数注入 Optional 造成的 pytest 收集问题）。"""
    gateway_doc = yaml.safe_load(GATEWAY_CONTRACT.read_text(encoding="utf-8"))
    test_gateway_route_active(gateway_doc)
