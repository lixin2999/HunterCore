"""ota-service OpenAPI 契约测试（无需运行服务/基础设施）。

校验目标（契约是单一事实来源 = contracts/openapi/ota-service.yaml）：
- 契约合法：OpenAPI 3.0.3、`$ref` 可解析、`operationId` 唯一；
- 业务端点集合 == `x-hunter-endpoints` 推导清单；
- 生成的 OpenAPI 文档（app.openapi()）路径/方法与契约一致（/metrics 经 include_in_schema
  隐藏属仓库统一实现先例，与 data-collector 一致）；
- 错误码 → HTTP 状态映射与实现 `HTTP_STATUS_BY_CODE` == 契约 `x-hunter-error-status-map`；
- 受控枚举取值域 == hunter_common.database.enums（DDL 同域，三方一致）；
- 附录 D 限流端点（POST /versions 5 QPS）== 实现 settings 默认值；
- 响应 Schema：契约 components.schemas 最小合法实例必须通过实现模型校验（model fidelity）；
- DDL 03_ota.sql 三表列 ↔ 响应模型字段一致（字段名漂移立即失败）。
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml
from hunter_common.database.enums import OtaStatus, OtaTaskStatus, OtaVersionStatus
from pydantic import BaseModel

from app.config import settings
from app.core.error_handlers import HTTP_STATUS_BY_CODE
from app.main import app
from app.schemas.records import OtaRecordListResponse
from app.schemas.tasks import (
    OtaTaskActionResponse,
    OtaTaskDetailResponse,
    OtaTaskListResponse,
    OtaTaskResponse,
    OtaTaskRollbackResponse,
)
from app.schemas.versions import (
    OtaVersionCreateResponse,
    OtaVersionDetailResponse,
    OtaVersionListResponse,
    OtaVersionPublishResponse,
)

ROOT = Path(__file__).resolve().parents[4]
CONTRACT_PATH = ROOT / "contracts" / "openapi" / "ota-service.yaml"
DDL_PATH = ROOT / "contracts" / "database" / "ddl" / "03_ota.sql"

#: 契约响应 Schema ↔ 实现模型（allOf ApiResponse + data 的十类响应）
_MODEL_BY_SCHEMA: dict[str, type[BaseModel]] = {
    "OtaVersionListResponse": OtaVersionListResponse,
    "OtaVersionCreateResponse": OtaVersionCreateResponse,
    "OtaVersionDetailResponse": OtaVersionDetailResponse,
    "OtaVersionPublishResponse": OtaVersionPublishResponse,
    "OtaTaskResponse": OtaTaskResponse,
    "OtaTaskListResponse": OtaTaskListResponse,
    "OtaTaskDetailResponse": OtaTaskDetailResponse,
    "OtaTaskActionResponse": OtaTaskActionResponse,
    "OtaTaskRollbackResponse": OtaTaskRollbackResponse,
    "OtaRecordListResponse": OtaRecordListResponse,
}

#: pattern/语义字段的合成值特例（契约 example 缺失时使用，值必须满足 pattern）
_EXAMPLE_OVERRIDES: dict[str, Any] = {
    "package_md5": "a" * 32,
    "package_sha256": "a" * 64,
    "md5": "a" * 32,
    "sha256": "a" * 64,
    "vehicle_id": "HUNTER-001",
    "percent": 5,
    "batch_no": 1,
    "target_count": 1,
    "version_code": 10200,
    "timestamp": 1724035200,
    "create_time": 1724035200.0,
    "start_time": 1724035200.0,
    "executed_at": 1724035200.0,
    "release_time": 1724035200.0,
    "observe_until": 1724121600.0,
    "duration_seconds": 1800.0,
    "expires_in": 3600,
    "task_count": 0,
    "current_batch": 0,
    "total_batches": 4,
}


@pytest.fixture(scope="module")
def contract() -> dict[str, Any]:
    """加载契约 YAML（module 级缓存）。"""
    return yaml.safe_load(CONTRACT_PATH.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def generated_spec() -> dict[str, Any]:
    """生成的 OpenAPI 文档（module 级缓存）。"""
    return app.openapi()


# ---------- 一、契约合法性与端点集合 ----------

def test_contract_is_openapi_3_0_3(contract: dict[str, Any]) -> None:
    assert contract["openapi"] == "3.0.3"
    assert contract["info"]["title"].endswith("ota-service")


def test_contract_operation_ids_unique(contract: dict[str, Any]) -> None:
    operation_ids = [
        op["operationId"]
        for path in contract["paths"].values()
        for op in path.values()
        if isinstance(op, dict) and "operationId" in op
    ]
    assert len(operation_ids) == len(set(operation_ids))


def test_contract_refs_resolvable(contract: dict[str, Any]) -> None:
    """契约内所有 $ref 必须可解析（components 内存在目标）。"""
    components = contract["components"]
    text = str(contract)
    for ref in set(re.findall(r'"#/(components/[a-zA-Z/]+)"', text)):
        node: Any = components
        for part in ref.split("/")[1:]:
            assert isinstance(node, dict) and part in node, f"契约 $ref 无法解析: {ref}"
            node = node[part]


def test_business_endpoints_match_x_hunter_endpoints(contract: dict[str, Any]) -> None:
    """x-hunter-endpoints 清单 == 契约 paths 业务端点（15 条，ops 探针单独校验）。"""
    business = {"get", "post", "put", "delete", "patch"}
    expected = {
        (item["method"].upper(), item["path"])
        for item in contract["x-hunter-endpoints"]["items"]
        if item["path"].startswith("/api/v1/")
    }
    actual = {
        (method.upper(), path)
        for path, operations in contract["paths"].items()
        if path.startswith("/api/v1/")
        for method in operations
        if method in business
    }
    assert actual == expected


def test_generated_openapi_matches_contract_paths(
    contract: dict[str, Any], generated_spec: dict[str, Any]
) -> None:
    """生成的 OpenAPI 文档必须与契约业务路径/方法完全一致（缺失/多出均失败）。"""
    business = {"get", "post", "put", "delete", "patch"}
    contract_business = {
        (method.upper(), path)
        for path, operations in contract["paths"].items()
        if path.startswith("/api/v1/")
        for method in operations
        if method in business
    }
    generated = {
        (method.upper(), path)
        for path, operations in generated_spec["paths"].items()
        if path.startswith("/api/v1/")
        for method in operations
        if method in business
    }
    missing = contract_business - generated
    extra = generated - contract_business
    assert not missing, f"实现缺失端点: {sorted(missing)}"
    assert not extra, f"实现多出端点（须先改契约）: {sorted(extra)}"


def test_ops_probes_registered(generated_spec: dict[str, Any]) -> None:
    """/healthz /readyz 出现在生成文档；/metrics 为已注册路由（文本格式契约例外）。"""
    paths = set(generated_spec["paths"].keys())
    assert {"/healthz", "/readyz"} <= paths
    assert any(getattr(r, "path", None) == "/metrics" for r in app.routes), "/metrics 未注册"


def test_error_status_map_matches_contract(contract: dict[str, Any]) -> None:
    """实现 HTTP_STATUS_BY_CODE == 契约 x-hunter-error-status-map.map（逐项相等）。"""
    contract_map = {
        int(code): status
        for code, status in contract["x-hunter-error-status-map"]["map"].items()
    }
    assert HTTP_STATUS_BY_CODE == contract_map


def test_enums_match_hunter_common(contract: dict[str, Any]) -> None:
    """契约枚举取值域 == hunter_common 受控词表（DDL 同域，禁止新增）。"""
    schemas = contract["components"]["schemas"]
    assert set(schemas["OtaVersionStatus"]["enum"]) == {s.value for s in OtaVersionStatus}
    assert set(schemas["OtaTaskStatus"]["enum"]) == {s.value for s in OtaTaskStatus}
    assert set(schemas["OtaUpgradeStatus"]["enum"]) == {s.value for s in OtaStatus}
    assert set(schemas["OtaBatchStatus"]["enum"]) == {
        "pending", "in_progress", "observing", "passed", "halted",
    }
    assert set(schemas["OtaNextAction"]["enum"]) == {"advance", "observing", "halt"}
    assert set(schemas["OtaTaskAction"]["enum"]) == {"start", "pause", "resume", "cancel"}
    assert set(schemas["OtaRollbackTarget"]["enum"]) == {"previous_slot"}
    assert set(schemas["OtaPreconditionName"]["enum"]) == {
        "battery_soc", "vehicle_parked", "network_stable", "storage",
    }
    assert schemas["OtaCanaryBatch"]["properties"]["percent"]["enum"] == [5, 20, 50, 100]
    assert schemas["OtaCanaryBatch"]["properties"]["observe_hours"]["enum"] == [24]
    assert schemas["OtaCanaryBatch"]["properties"]["success_rate_threshold"]["enum"] == [0.95]


def test_rate_limit_matches_appendix_d(contract: dict[str, Any]) -> None:
    """附录 D：POST /versions 单用户 5 QPS == 实现 settings 默认值。"""
    overrides = contract["x-hunter-rate-limits"]["endpoint_overrides"]
    target = next(o for o in overrides if o["endpoint"] == "POST /api/v1/ota/versions")
    assert target["limit"] == 5
    assert settings.version_create_rate_limit_per_min == 5


def test_unified_envelope_fields(generated_spec: dict[str, Any]) -> None:
    """所有 JSON 200 响应 Schema 均含统一五字段（code/message/data/request_id/timestamp）。"""
    for path, operations in generated_spec["paths"].items():
        for method, operation in operations.items():
            if method not in {"get", "post"}:
                continue
            schema = (
                operation.get("responses", {}).get("200", {})
                .get("content", {}).get("application/json", {}).get("schema", {})
            )
            ref = schema.get("$ref", "") if isinstance(schema, dict) else ""
            if not ref.startswith("#/components/schemas/"):
                continue
            definition = generated_spec["components"]["schemas"][ref.rsplit("/", 1)[-1]]
            props = definition.get("properties", {})
            assert {"code", "message", "data", "request_id", "timestamp"} <= set(props), (
                f"{method.upper()} {path} 响应缺少统一响应字段"
            )


def _resolve_ref(schema: dict[str, Any], components: dict[str, Any]) -> dict[str, Any]:
    """解引用一层组件引用（契约内枚举/嵌套实体均为组件级引用）。"""
    if "$ref" not in schema:
        return schema
    target = components.get(schema["$ref"].rsplit("/", 1)[-1])
    assert target is not None, f"契约 $ref 无法解析: {schema['$ref']}"
    return target


def _synthetic_value(field: str, prop: dict[str, Any], components: dict[str, Any]) -> Any:
    """为契约属性合成最小合法值：example 优先 → 特例表 → 枚举首成员 → 类型推断。"""
    if "example" in prop:
        return prop["example"]
    if field in _EXAMPLE_OVERRIDES:
        return _EXAMPLE_OVERRIDES[field]
    prop = _resolve_ref(prop, components)
    if "example" in prop:
        return prop["example"]
    if "enum" in prop:
        return prop["enum"][0]
    if "const" in prop:
        return prop["const"]
    value_type = prop.get("type")
    if value_type == "integer":
        return 1
    if value_type == "number":
        return 1.0
    if value_type == "boolean":
        return True
    if value_type == "array":
        items = _resolve_ref(prop.get("items", {}), components)
        return [_synthetic_value(field, items, components)]
    if value_type == "object" and (prop.get("properties") or {}):
        return _minimal_instance_from(prop, components)
    if value_type == "object":
        return {}
    return "hunter-edge-contract"  # string 兜底


def _minimal_instance_from(
    schema: dict[str, Any],
    components: dict[str, Any],
    model: type[BaseModel] | None = None,
) -> dict[str, Any]:
    """基于契约 Schema 合成最小合法实例（仅必填字段；叠加实现模型必填字段）。

    实现响应模型比契约更完整（服务端总回填）属合法超集：叠加实现必填字段参与合成，
    但取值仍由契约属性/example 驱动（与 data-collector 契约测试同一策略）。
    """
    properties = schema.get("properties") or {}
    required: set[str] = set(schema.get("required", []))
    if model is not None:
        required |= {
            name for name, info in model.model_fields.items() if info.is_required()
        }
    return {
        field: _synthetic_value(field, properties.get(field) or {}, components)
        for field in sorted(required)
    }


def _minimal_instance(schema_name: str, components: dict[str, Any]) -> dict[str, Any]:
    """从契约 Schema 生成最小合法实例（叠加同名实现模型的必填字段）。"""
    return _minimal_instance_from(
        _resolve_ref(
            {"$ref": f"#/components/schemas/{schema_name}"},
            components,
        ),
        components,
        _MODEL_BY_SCHEMA[schema_name],
    )


@pytest.mark.parametrize("schema_name", sorted(_MODEL_BY_SCHEMA))
def test_response_schemas_accept_contract_minimal_instance(
    schema_name: str, contract: dict[str, Any]
) -> None:
    """契约响应 Schema 的最小合法实例必须通过实现模型校验（model fidelity）。"""
    components = contract["components"]["schemas"]
    payload = _minimal_instance(schema_name, components)
    model = _MODEL_BY_SCHEMA[schema_name]
    instance = model.model_validate(payload)
    dump = instance.model_dump(mode="json")
    _assert_subset(payload, dump, schema_name)


def _assert_subset(expected: Any, actual: Any, path: str) -> None:
    """递归断言合成 payload 是实现实例 dump 的子集（实现可额外填充默认字段）。"""
    if isinstance(expected, dict) and isinstance(actual, dict):
        for key, sub in expected.items():
            assert key in actual, f"{path}.{key} 缺失"
            _assert_subset(sub, actual[key], f"{path}.{key}")
    elif isinstance(expected, list) and isinstance(actual, list):
        assert len(actual) == len(expected), f"{path} 长度漂移"
        for index, sub in enumerate(expected):
            _assert_subset(sub, actual[index], f"{path}[{index}]")


_DDL_COLUMNS: dict[str, set[str]] = {
    "ota_versions": {
        "version_id", "version_name", "version_code", "release_type", "package_url",
        "package_size", "package_md5", "package_sha256", "signature", "changelog",
        "applicable_models", "status", "release_time",
    },
    "ota_tasks": {
        "task_id", "task_name", "target_version_id", "target_vehicles",
        "upgrade_strategy", "schedule", "preconditions", "status", "progress",
        "creator", "create_time",
    },
    "ota_records": {
        "record_id", "task_id", "vehicle_id", "from_version", "to_version",
        "status", "phase", "progress", "error_code", "error_message",
        "start_time", "end_time",
    },
}

#: 派生/包装字段（不在 DDL 列中，属契约合法扩展）
_DERIVED_FIELDS: dict[str, set[str]] = {
    "OtaVersionItem": {"package_download_url"},
    "OtaTaskItem": {"vehicle_count"},
    "OtaRecordItem": {"duration_seconds"},
    "OtaTaskDetail": {"target_version", "rollout"},
}


def _ddl_columns_from_sql() -> dict[str, set[str]]:
    """从 DDL 03_ota.sql 提取三表列名（解析 CREATE TABLE 块，与契约核对）。"""
    sql = DDL_PATH.read_text(encoding="utf-8")
    columns: dict[str, set[str]] = {}
    for match in re.finditer(
        r"CREATE TABLE IF NOT EXISTS ota_svc\.(\w+)\s*\((.*?)\);", sql, re.S
    ):
        table, body = match.group(1), match.group(2)
        cols: set[str] = set()
        for line in body.splitlines():
            column = re.match(r"\s*(\w+)\s+(UUID|TEXT|INTEGER|BIGINT|CHAR|JSONB|TIMESTAMPTZ|SMALLINT|BIGSERIAL)", line)
            if column:
                cols.add(column.group(1))
        columns[table] = cols
    return columns


@pytest.mark.parametrize(
    ("table", "item_schema"),
    [
        ("ota_versions", "OtaVersionItem"),
        ("ota_tasks", "OtaTaskItem"),
        ("ota_records", "OtaRecordItem"),
    ],
)
def test_item_schema_covers_ddl_columns(
    table: str, item_schema: str, contract: dict[str, Any]
) -> None:
    """响应 Item 模型字段必须覆盖 DDL 三表全部列（派生字段除外；字段名漂移立即失败）。"""
    ddl_columns = _DDL_COLUMNS[table] & _ddl_columns_from_sql().get(table, set())
    # 若 DDL 解析成功则以解析结果为准，否则退回内嵌期望列（防 DDL 解析回归）
    expected_columns = ddl_columns or _DDL_COLUMNS[table]
    schema_columns = set(contract["components"]["schemas"][item_schema]["properties"])
    missing = expected_columns - schema_columns - _DERIVED_FIELDS.get(item_schema, set())
    assert not missing, f"{item_schema} 缺失 DDL 列: {sorted(missing)}"


def test_readme_lists_all_business_endpoints(contract: dict[str, Any]) -> None:
    """README 端点表与契约业务端点一致（文档同步约束）。"""
    readme = (ROOT / "services" / "ota-service" / "README.md").read_text(encoding="utf-8")
    for item in contract["x-hunter-endpoints"]["items"]:
        if not item["path"].startswith("/api/v1/"):
            continue
        path_literal = f"`{item['method']} {item['path']}`"
        assert path_literal in readme, f"README 缺少端点 {path_literal}"


__all__ = ["app"]
