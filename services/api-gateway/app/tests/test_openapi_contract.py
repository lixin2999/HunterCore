"""api-gateway OpenAPI 契约测试（无需运行服务/基础设施）。

校验目标（契约是单一事实来源）：
- `contracts/openapi/api-gateway.yaml` 本身合法：OpenAPI 3.0.3、`$ref` 可解析、`operationId` 唯一
- 错误码只取预定义值（附录 A）且与实现 `HTTP_STATUS_BY_CODE` 一致
- 网关路由表 / 五级限流阈值与设计文档 3.4 节、附录 D、K8s 清单（ConfigMap/Ingress）三方一致
- 网关自持端点与已实现探针一致；Kafka 参与度为空（contracts/kafka 无网关消费者组）
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml
from hunter_common.exceptions import ErrorCode

from app.core.error_handlers import HTTP_STATUS_BY_CODE

ROOT = Path(__file__).resolve().parents[4]
CONTRACT_PATH = ROOT / "contracts" / "openapi" / "api-gateway.yaml"
GATEWAY_K8S = ROOT / "infra" / "k8s" / "services" / "api-gateway.yaml"
INGRESS = ROOT / "infra" / "k8s" / "ingress.yaml"
CONSUMER_GROUPS = ROOT / "contracts" / "kafka" / "consumer-groups.yaml"

#: 设计文档附录 A 预定义错误码（与 hunter_common.exceptions.ErrorCode 一致）
EXPECTED_ERROR_CODES: set[int] = {int(code) for code in ErrorCode}

#: 设计文档 3.4 节网关路由表（路径前缀不可更改）
EXPECTED_ROUTE_PREFIXES: tuple[str, ...] = (
    "/api/v1/scene",
    "/api/v1/data",
    "/api/v1/analytics",
    "/api/v1/ota",
    "/api/v1/remote",
    "/api/v1/vehicle",
    "/api/v1/user",
)

#: 设计文档附录 D 限流阈值（不可更改）
EXPECTED_GLOBAL_LIMITS: dict[str, int] = {
    "global_qps": 10000,
    "per_user_qps": 100,
    "per_ip_qps": 200,
}

#: 设计文档附录 D 接口级限流（单用户 QPS）
EXPECTED_ENDPOINT_LIMITS: dict[tuple[str, str], int] = {
    ("POST", "/api/v1/ota/versions"): 5,
    ("POST", "/api/v1/remote/session"): 1,
    ("GET", "/api/v1/data/telemetry"): 20,
}

#: 网关自持端点（认证 4 个 + 运维探针 3 个）；探针端点已在 app/routers/health.py 实现
EXPECTED_PATHS: set[str] = {
    "/api/v1/user/login",
    "/api/v1/user/refresh",
    "/api/v1/user/logout",
    "/api/v1/user/me",
    "/healthz",
    "/readyz",
    "/metrics",
}

HTTP_METHODS: tuple[str, ...] = (
    "get",
    "post",
    "put",
    "patch",
    "delete",
    "head",
    "options",
)


@pytest.fixture(scope="module")
def contract() -> dict[str, Any]:
    """加载网关契约（YAML）。"""
    loaded = yaml.safe_load(CONTRACT_PATH.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def _iter_refs(node: Any) -> Iterator[str]:
    """递归收集文档内所有 `$ref` 字符串。"""
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "$ref" and isinstance(value, str):
                yield value
            else:
                yield from _iter_refs(value)
    elif isinstance(node, list):
        for item in node:
            yield from _iter_refs(item)


def _resolve_pointer(document: dict[str, Any], ref: str) -> Any:
    """解析文档内 JSON Pointer（`#/components/...`）；无法解析直接断言失败。"""
    assert ref.startswith("#/"), f"仅支持文档内引用：{ref}"
    node: Any = document
    for raw_token in ref[2:].split("/"):
        token = raw_token.replace("~1", "/").replace("~0", "~")
        assert isinstance(node, dict) and token in node, f"引用无法解析：{ref}"
        node = node[token]
    return node


def _operations(contract: dict[str, Any]) -> Iterator[tuple[str, str, dict[str, Any]]]:
    """遍历全部操作：产出 (HTTP 方法, 路径, 操作对象)。"""
    for path, item in contract["paths"].items():
        for method, operation in item.items():
            if method in HTTP_METHODS:
                yield method, path, operation


def _load_k8s_documents(path: Path) -> list[dict[str, Any]]:
    """按 YAML 多文档方式加载 K8s 清单。"""
    return [doc for doc in yaml.safe_load_all(path.read_text(encoding="utf-8")) if doc]


# =====================================================================
# 一、契约文件自身合法性
# =====================================================================
def test_contract_is_valid_openapi_3(contract: dict[str, Any]) -> None:
    assert contract["openapi"] == "3.0.3"
    assert contract["info"]["title"] == "HunterEdge - api-gateway"
    assert contract["info"]["version"] == "0.1.0"
    assert {"servers", "tags", "paths", "components"} <= set(contract)


def test_all_refs_resolve(contract: dict[str, Any]) -> None:
    refs = list(_iter_refs(contract))
    assert refs, "契约必须通过 components 复用安全方案/响应/模型（禁止重复定义）"
    for ref in refs:
        assert _resolve_pointer(contract, ref) is not None


def test_only_contract_declared_paths(contract: dict[str, Any]) -> None:
    """网关端点必须与契约声明完全一致（禁止在代码中新增未声明端点）。"""
    assert set(contract["paths"]) == EXPECTED_PATHS


def test_operation_ids_unique_and_annotated(contract: dict[str, Any]) -> None:
    seen: list[str] = []
    for method, path, operation in _operations(contract):
        assert operation.get("operationId"), f"{method.upper()} {path} 缺少 operationId"
        assert operation.get("summary"), f"{method.upper()} {path} 缺少 summary"
        assert operation.get("responses"), f"{method.upper()} {path} 缺少 responses"
        seen.append(operation["operationId"])
    assert len(seen) == len(set(seen)), "operationId 必须唯一（生成客户端 SDK 依赖）"


def test_every_inline_response_has_description(contract: dict[str, Any]) -> None:
    for method, path, operation in _operations(contract):
        for status, response in operation["responses"].items():
            if "$ref" in response:
                continue
            assert response.get("description"), (
                f"{method.upper()} {path} {status} 缺少 description"
            )


# =====================================================================
# 二、错误码与统一响应（附录 A）
# =====================================================================
def test_error_code_enum_matches_predefined_codes(contract: dict[str, Any]) -> None:
    enum = {
        int(code) for code in contract["components"]["schemas"]["ErrorCode"]["enum"]
    }
    assert enum == EXPECTED_ERROR_CODES | {0}


def test_example_codes_are_predefined(contract: dict[str, Any]) -> None:
    """契约中出现的所有示例业务码必须来自预定义集合（禁止自定义错误码）。"""
    text = CONTRACT_PATH.read_text(encoding="utf-8")
    found = {
        int(code) for code in re.findall(r"^\s+code: (\d+)$", text, flags=re.MULTILINE)
    }
    assert found, "契约应至少包含一个响应示例"
    assert found <= EXPECTED_ERROR_CODES


def test_error_status_map_matches_implementation(contract: dict[str, Any]) -> None:
    """契约声明的错误码→HTTP 状态必须与实现完全一致（防止前后端理解偏差）。"""
    declared = {
        int(code): int(status)
        for code, status in contract["x-hunter-error-status-map"]["map"].items()
    }
    assert declared == HTTP_STATUS_BY_CODE


def test_error_status_map_covers_all_error_codes(contract: dict[str, Any]) -> None:
    declared = {int(code) for code in contract["x-hunter-error-status-map"]["map"]}
    assert declared == EXPECTED_ERROR_CODES - {0}


def test_unified_response_schema_fields(contract: dict[str, Any]) -> None:
    schema = contract["components"]["schemas"]["ApiResponse"]
    assert schema["required"] == ["code", "message", "data", "request_id", "timestamp"]
    assert set(schema["properties"]) == set(schema["required"])


def test_too_many_requests_declares_retry_after(contract: dict[str, Any]) -> None:
    """限流超限必须返回 429 + Retry-After（模块契约要求）。"""
    response = contract["components"]["responses"]["TooManyRequests"]
    assert "Retry-After" in response["headers"]
    limits = contract["x-hunter-rate-limits"]["over_limit"]
    assert limits["http_status"] == 429
    assert limits["retry_after_header"] == "Retry-After"
    assert limits["response_code"] in EXPECTED_ERROR_CODES


# =====================================================================
# 三、认证与安全（设计文档 3.2 / 14.1）
# =====================================================================
def test_security_schemes_declared(contract: dict[str, Any]) -> None:
    schemes = contract["components"]["securitySchemes"]
    assert schemes["bearerAuth"]["type"] == "http"
    assert schemes["bearerAuth"]["scheme"] == "bearer"
    assert schemes["bearerAuth"]["bearerFormat"] == "JWT"
    # 车辆设备认证：X.509 双向 TLS（CommonName = vehicle_id），非 REST
    assert schemes["deviceCertificate"]["type"] == "mutualTLS"


def test_login_public_but_session_endpoints_require_bearer(
    contract: dict[str, Any],
) -> None:
    paths = contract["paths"]
    assert paths["/api/v1/user/login"]["post"]["security"] == []
    assert paths["/api/v1/user/refresh"]["post"]["security"] == []
    for path in ("/api/v1/user/logout", "/api/v1/user/me"):
        assert paths[path]["get" if path.endswith("/me") else "post"]["security"] == [
            {"bearerAuth": []}
        ]


def test_token_ttl_matches_security_contract(contract: dict[str, Any]) -> None:
    """Access Token 2h / Refresh Token 7d（设计文档 14.1 节）。"""
    props = contract["components"]["schemas"]["TokenPair"]["properties"]
    assert props["expires_in"]["example"] == 7200
    assert props["refresh_expires_in"]["example"] == 604800
    assert props["token_type"]["enum"] == ["Bearer"]


def test_password_fields_are_write_only(contract: dict[str, Any]) -> None:
    """密码/令牌类字段必须 writeOnly（禁止出现在响应或 OpenAPI 客户端返回模型）。"""
    schemas = contract["components"]["schemas"]
    assert schemas["LoginRequest"]["properties"]["password"]["writeOnly"] is True
    assert (
        schemas["RefreshTokenRequest"]["properties"]["refresh_token"]["writeOnly"]
        is True
    )
    assert schemas["TokenPair"]["properties"]["refresh_token"]["writeOnly"] is True


# =====================================================================
# 四、网关路由表（设计文档 3.4 节；前缀不可更改）
# =====================================================================
def test_gateway_routes_match_route_table(contract: dict[str, Any]) -> None:
    routes = contract["x-hunter-gateway-routes"]["routes"]
    assert tuple(route["prefix"] for route in routes) == EXPECTED_ROUTE_PREFIXES
    headers = contract["x-hunter-gateway-routes"]["route_headers"]["headers"]
    assert headers == ["X-User-Id", "X-Roles", "X-Trace-Id"]


def test_proxy_routes_target_correct_service_ports(contract: dict[str, Any]) -> None:
    routes = {
        route["prefix"]: route
        for route in contract["x-hunter-gateway-routes"]["routes"]
    }
    expected = {
        "/api/v1/scene": ("scene-service", 8081),
        "/api/v1/data": ("data-collector", 8082),
        "/api/v1/analytics": ("data-analytics", 8083),
        "/api/v1/ota": ("ota-service", 8084),
        "/api/v1/remote": ("remote-control", 8085),
    }
    for prefix, (service, port) in expected.items():
        assert routes[prefix]["target_service"] == service
        assert routes[prefix]["target_port"] == port
        assert routes[prefix]["auth"] == "jwt", f"{prefix} 必须强制 JWT 鉴权"


def test_pending_confirmation_routes_are_marked(contract: dict[str, Any]) -> None:
    """模块表仅 6 个微服务，vehicle-service / user-service 归属未定 —— 必须显式标记待确认。"""
    routes = contract["x-hunter-gateway-routes"]["routes"]
    pending = {r["prefix"] for r in routes if r.get("status") == "pending_confirmation"}
    assert pending == {"/api/v1/vehicle", "/api/v1/user"}
    discovery = contract["x-hunter-gateway-routes"]["service_discovery"]
    assert discovery["status"] == "pending_confirmation"


def test_gateway_routes_match_k8s_configmap(contract: dict[str, Any]) -> None:
    """契约 ↔ K8s ConfigMap：路由前缀与限流阈值必须一致（同一事实来源）。"""
    configmap = next(
        doc for doc in _load_k8s_documents(GATEWAY_K8S) if doc["kind"] == "ConfigMap"
    )
    data = configmap["data"]
    assert data["GATEWAY_ROUTE_PREFIXES"].split(",") == list(EXPECTED_ROUTE_PREFIXES)
    limits = contract["x-hunter-rate-limits"]
    assert int(data["RATE_LIMIT_GLOBAL_QPS"]) == limits["global_qps"]
    assert int(data["RATE_LIMIT_USER_QPS"]) == limits["per_user_qps"]
    assert int(data["RATE_LIMIT_IP_QPS"]) == limits["per_ip_qps"]


def test_ingress_paths_match_route_table() -> None:
    """契约 ↔ Ingress：入站前缀集合一致，且全部经 api-gateway:8080（禁止绕过鉴权/限流）。"""
    documents = _load_k8s_documents(INGRESS)
    declared = {
        path["path"]
        for doc in documents
        for rule in doc["spec"]["rules"]
        for path in rule["http"]["paths"]
    }
    assert declared == set(EXPECTED_ROUTE_PREFIXES) | {"/ws/remote"}
    for doc in documents:
        for rule in doc["spec"]["rules"]:
            for path in rule["http"]["paths"]:
                backend = path["backend"]["service"]
                assert backend["name"] == "api-gateway"
                assert backend["port"]["number"] == 8080


# =====================================================================
# 五、限流配置（设计文档附录 D）
# =====================================================================
def test_global_rate_limits_match_appendix_d(contract: dict[str, Any]) -> None:
    limits = contract["x-hunter-rate-limits"]
    for key, value in EXPECTED_GLOBAL_LIMITS.items():
        assert limits[key] == value, f"限流阈值 {key} 必须与附录 D 一致"
    assert "rate_limit:{ip}:{api}" in limits["algorithm"]


def test_endpoint_rate_limits_match_appendix_d(contract: dict[str, Any]) -> None:
    entries = {
        (entry["method"], entry["path"]): entry
        for entry in contract["x-hunter-rate-limits"]["endpoint_limits"]
    }
    assert {
        key: entry["qps"] for key, entry in entries.items()
    } == EXPECTED_ENDPOINT_LIMITS
    assert {entry["dimension"] for entry in entries.values()} == {"user"}


def test_vehicle_limits_match_appendix_d(contract: dict[str, Any]) -> None:
    """车辆级限流：遥测 ≤ 100 msg/s、文件上传 ≤ 10 Mbps。"""
    entries = {
        entry["metric"]: entry
        for entry in contract["x-hunter-rate-limits"]["vehicle_limits"]
    }
    assert entries["kafka_telemetry_msg_per_sec"]["limit"] == 100
    assert entries["file_upload_mbps"]["limit"] == 10


# =====================================================================
# 六、运维探针与指标（与 app/routers/health.py 实现一致）
# =====================================================================
def test_probe_endpoints_declared(contract: dict[str, Any]) -> None:
    assert set(contract["paths"]["/healthz"]["get"]["responses"]) == {"200"}
    assert set(contract["paths"]["/readyz"]["get"]["responses"]) == {"200", "503"}
    ready_schema = contract["components"]["schemas"]["ReadyChecks"]
    assert set(ready_schema["required"]) == {"database", "redis"}
    assert contract["components"]["schemas"]["HealthStatus"]["properties"]["status"][
        "enum"
    ] == ["ok"]


def test_internal_endpoints_flagged(contract: dict[str, Any]) -> None:
    """探针/指标端点标记 x-internal，且不经网关路由表对外暴露。"""
    for path in ("/healthz", "/readyz", "/metrics"):
        assert contract["paths"][path]["get"]["x-internal"] is True
        assert contract["paths"][path]["get"]["security"] == []


def test_metrics_is_text_plain_contract_exception(contract: dict[str, Any]) -> None:
    """契约明确例外：/metrics 返回 Prometheus 文本，而非统一 JSON 响应体。"""
    content = contract["paths"]["/metrics"]["get"]["responses"]["200"]["content"]
    assert set(content) == {"text/plain"}


# =====================================================================
# 七、WebSocket、Kafka 参与度与审计日志
# =====================================================================
def test_websocket_route_contract(contract: dict[str, Any]) -> None:
    routes = contract["x-hunter-websocket-routes"]
    assert len(routes) == 1
    route = routes[0]
    assert route["path"] == "/ws/remote/**"
    assert route["target_service"] == "remote-control"
    assert route["target_port"] == 8085
    joined = " ".join(route["constraints"])
    # 远程操控安全约束：指令 20Hz、>500ms 自动停车、视频 ≤200ms、指令 ≤100ms（不可更改）
    assert "20Hz" in joined and "500ms" in joined
    assert "200ms" in joined and "100ms" in joined
    assert {1001, 7001, 7002} <= set(route["error_codes"])


def test_gateway_has_no_kafka_participation(contract: dict[str, Any]) -> None:
    """网关只转发 REST/WebSocket，不生产/不消费消息；契约侧不得出现网关消费者组。"""
    kafka = contract["x-hunter-kafka"]
    assert kafka["produces"] == []
    assert kafka["consumes"] == []
    groups = yaml.safe_load(CONSUMER_GROUPS.read_text(encoding="utf-8"))["groups"]
    assert all(group["service"] != "api-gateway" for group in groups)


def test_audit_log_contract_fields(contract: dict[str, Any]) -> None:
    """访问日志字段（设计文档 3.4 节）与敏感信息脱敏约束。"""
    audit = contract["x-hunter-audit-log"]
    assert set(audit["access_log_fields"]) == {
        "time",
        "method",
        "path",
        "status_code",
        "duration_ms",
        "user_id",
        "client_ip",
        "trace_id",
    }
    assert "脱敏" in audit["masking"]
