"""data-analytics OpenAPI 契约测试（无需运行服务/基础设施）。

校验目标（契约是单一事实来源）：
- `contracts/openapi/data-analytics.yaml` 合法：OpenAPI 3.0.3、`$ref` 可解析、`operationId` 唯一
- 业务端点集合 == 设计文档 12.4 节清单（8 个）+ 运维探针与 `app.main` 实际注册路由一致
- 6.2 节 5 个 Flink 实时作业：输入 Topic/消费组 ↔ `contracts/kafka/consumer-groups.yaml`；
  阈值 ↔ 受控词表（`EventType` / `EVENT_LEVEL_BY_TYPE`）↔ K8s ConfigMap 环境变量三方一致
- `alert_event` 消息 Schema ↔ 契约告警规则（类型/等级/来源作业）↔ topics.yaml / 消费组
- 6.3 节离线作业调度与 6.3.3 节控制性能阈值（0.2 m/s / 0.02 rad / 10% / 2s）不可放宽
- 6.4 节 Corner Case 5 类 + 算法 + 前后各 10 秒截取窗口 ↔ `analytics_result` Schema
- 6.5 节 5 类报告模板 / 异步生成语义 / MinIO hunter-reports 存储与预签名时效
- 错误码只取预定义值且与实现 `HTTP_STATUS_BY_CODE` 一致；统一响应五字段
- 限流继承网关（附录 D）/ DB 只读例外（er.md）/ K8s 清单三方一致

文件名在各服务内唯一：monorepo 中 pytest 以 importlib 模式收集，各服务 `app/tests/` 同名文件会
互相覆盖（模块级 fixture 丢失），故使用 `test_data_analytics_contract.py`。
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml
from hunter_common.database.enums import EVENT_LEVEL_BY_TYPE, EventLevel, EventType
from hunter_common.database.repository import MAX_PAGE_SIZE
from hunter_common.exceptions import ErrorCode

from app.core.error_handlers import HTTP_STATUS_BY_CODE
from app.main import app

ROOT = Path(__file__).resolve().parents[4]
CONTRACT_PATH = ROOT / "contracts" / "openapi" / "data-analytics.yaml"
GATEWAY_CONTRACT = ROOT / "contracts" / "openapi" / "api-gateway.yaml"
DATA_COLLECTOR_CONTRACT = ROOT / "contracts" / "openapi" / "data-collector.yaml"
TOPICS_CONTRACT = ROOT / "contracts" / "kafka" / "topics.yaml"
CONSUMER_GROUPS = ROOT / "contracts" / "kafka" / "consumer-groups.yaml"
SCHEMA_DIR = ROOT / "contracts" / "kafka" / "schemas"
DDL_TIMESERIES = ROOT / "contracts" / "database" / "ddl" / "05_timeseries.sql"
ER_DOC = ROOT / "contracts" / "database" / "er.md"
K8S_MANIFEST = ROOT / "infra" / "k8s" / "services" / "data-analytics.yaml"
K8S_COMMON_CONFIG = ROOT / "infra" / "k8s" / "base" / "01-configmap-common.yaml"
K8S_SECRET = ROOT / "infra" / "k8s" / "base" / "02-secret.example.yaml"
INGRESS = ROOT / "infra" / "k8s" / "ingress.yaml"

#: 业务端点（设计文档 12.4 节，不可增删）
EXPECTED_ENDPOINTS: set[tuple[str, str]] = {
    ("GET", "/api/v1/analytics/reports"),
    ("GET", "/api/v1/analytics/reports/{report_id}"),
    ("POST", "/api/v1/analytics/reports/generate"),
    ("GET", "/api/v1/analytics/dashboard"),
    ("GET", "/api/v1/analytics/perception/eval"),
    ("GET", "/api/v1/analytics/control/eval"),
    ("GET", "/api/v1/analytics/scene/coverage"),
    ("GET", "/api/v1/analytics/corner-cases"),
}

EXPECTED_OPS_ENDPOINTS: set[tuple[str, str]] = {
    ("GET", "/healthz"),
    ("GET", "/readyz"),
    ("GET", "/metrics"),
}

#: 6.2 节 5 个 Flink 实时作业（名称不可更改，alert_event.source_job 取值同域）
EXPECTED_REALTIME_JOBS = {
    "vehicle_state_monitor",
    "driving_anomaly_detection",
    "algorithm_performance_monitor",
    "collision_risk_assessment",
    "data_quality_monitor",
}

#: 6.3 / 6.4 / 6.5 节离线作业（Spark 3.5）
EXPECTED_OFFLINE_JOBS = {
    "daily_report",
    "perception_eval",
    "planning_quality_eval",
    "control_eval",
    "scene_coverage",
    "corner_case_mining",
    "monthly_operation",
}

#: 6.5 节 5 类标准报告模板
EXPECTED_REPORT_TYPES = {
    "vehicle_daily",
    "algorithm_eval",
    "scene_test",
    "ota_upgrade",
    "monthly_operation",
}
EXPECTED_REPORT_FORMATS = {"html", "pdf", "json"}
EXPECTED_REPORT_STATUSES = {"pending", "generating", "ready", "failed"}

#: 6.3.3 节控制性能阈值（不可更改）
EXPECTED_CONTROL_THRESHOLDS: dict[str, float] = {
    "velocity_rmse_ms": 0.2,
    "steering_rmse_rad": 0.02,
    "overshoot_percent": 10,
    "settling_time_s": 2,
}

#: 6.4 节 Corner Case 5 类 + 挖掘算法
EXPECTED_CORNER_CASE_CATEGORIES = {
    "kinematic",
    "perception",
    "planning",
    "interaction",
    "environment",
}
EXPECTED_CORNER_CASE_ALGORITHMS = {"isolation_forest", "dbscan"}

#: 6.3.2 节感知精度指标
EXPECTED_PERCEPTION_METRICS = {
    "map_3d",
    "map_bev",
    "iou",
    "recall",
    "precision",
    "mean_localization_error_m",
}

#: 6.2.2 / 6.2.3 节实时阈值（不可放宽；env → (值, 单位)）
EXPECTED_REALTIME_THRESHOLDS: dict[str, tuple[float, str]] = {
    "ALERT_COMMUNICATION_LOSS_SECONDS": (10, "s"),
    "ALERT_TRIGGER_MAX_LATENCY_SECONDS": (2, "s"),
    "ALERT_HARSH_ACCEL_MS2": (3.0, "m/s2"),
    "ALERT_HARSH_ACCEL_MIN_DURATION_SECONDS": (0.5, "s"),
    "ALERT_HARSH_BRAKING_MS2": (3.0, "m/s2"),
    "ALERT_HARSH_BRAKING_MIN_DURATION_SECONDS": (0.5, "s"),
    "ALERT_HARSH_TURN_RAD_S": (0.8, "rad/s"),
    "ALERT_OVER_SPEED_RATIO": (1.1, "ratio"),
    "ALERT_BATTERY_LOW_SOC": (20, "percent"),
    "ALERT_BATTERY_CRITICAL_SOC": (10, "percent"),
    "ALERT_COLLISION_TTC_CRITICAL_SECONDS": (1.5, "s"),
    "ALERT_COLLISION_TTC_WARNING_SECONDS": (3.0, "s"),
    "ALGORITHM_METRICS_WINDOW_SECONDS": (60, "s"),
    "ALERT_WINDOW_SECONDS": (2, "s"),
}

#: 报告与查询时效（MinIO 契约 / 性能指标）
EXPECTED_REPORT_DOWNLOAD_EXPIRES = 900
EXPECTED_REPORT_UPLOAD_EXPIRES = 3600
EXPECTED_REPORT_BUCKET = "hunter-reports"
EXPECTED_ALERT_LATENCY_MS = 2000
EXPECTED_API_P95_MS = 200

HTTP_METHODS: tuple[str, ...] = ("get", "post", "put", "patch", "delete", "head", "options")


def load_yaml(path: Path) -> dict[str, Any]:
    """加载 YAML 契约文件。"""
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def load_json(path: Path) -> dict[str, Any]:
    """加载 JSON 契约文件。"""
    loaded = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def iter_refs(node: Any) -> Iterator[str]:
    """递归收集文档内所有 `$ref` 字符串。"""
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "$ref" and isinstance(value, str):
                yield value
            else:
                yield from iter_refs(value)
    elif isinstance(node, list):
        for item in node:
            yield from iter_refs(item)


def resolve_pointer(document: dict[str, Any], ref: str) -> Any:
    """解析文档内 `#/...` JSON Pointer（支持 ~0/~1 转义）。"""
    assert ref.startswith("#/"), f"仅支持文档内引用: {ref}"
    node: Any = document
    for raw_token in ref[2:].split("/"):
        token = raw_token.replace("~1", "/").replace("~0", "~")
        assert isinstance(node, dict) and token in node, f"$ref 无法解析: {ref}"
        node = node[token]
    return node


def iter_operations(contract: dict[str, Any]) -> Iterator[tuple[str, str, dict[str, Any]]]:
    """遍历契约中的 (method, path, operation)。"""
    for path, path_item in contract["paths"].items():
        for method, operation in path_item.items():
            if method.lower() in HTTP_METHODS:
                yield method.upper(), path, operation


def iter_registered_routes(routes: Any) -> Iterator[tuple[str, str]]:
    """递归展开 FastAPI 已注册路由（兼容 Starlette `_IncludedRouter` 聚合节点）。"""
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
            yield method, path


def ddl_columns(path: Path, table: str) -> set[str]:
    """解析 DDL 文件中指定表的列名（排除表级约束行，如 `PRIMARY KEY (...)`）。"""
    text = path.read_text(encoding="utf-8")
    body = text.split(f"CREATE TABLE IF NOT EXISTS {table} (", 1)[1].split("\n);", 1)[0]
    table_constraints = {"PRIMARY", "UNIQUE", "CONSTRAINT", "FOREIGN", "CHECK", "EXCLUDE"}
    return {
        match.group(1)
        for match in re.finditer(r"^\s{4}(\w+)\s+", body, re.MULTILINE)
        if match.group(1) not in table_constraints
    }


def k8s_docs(path: Path) -> list[dict[str, Any]]:
    """加载多文档 YAML（K8s 清单）。"""
    return [doc for doc in yaml.safe_load_all(path.read_text(encoding="utf-8")) if doc]


def service_config_map(path: Path) -> dict[str, str]:
    """取服务级 ConfigMap 的 data 段。"""
    for doc in k8s_docs(path):
        if doc.get("kind") == "ConfigMap":
            return {str(key): str(value) for key, value in (doc.get("data") or {}).items()}
    raise AssertionError(f"{path} 缺少 ConfigMap")


def docs_by_kind(path: Path) -> dict[str, dict[str, Any]]:
    """按 kind 索引 K8s 清单。"""
    return {doc["kind"]: doc for doc in k8s_docs(path)}


def job_by_name(jobs: list[dict[str, Any]], name: str) -> dict[str, Any]:
    """按名称取作业定义（契约内作业清单）。"""
    matched = [job for job in jobs if job.get("name") == name]
    assert matched, f"契约缺少作业定义: {name}"
    return matched[0]


@pytest.fixture(scope="module")
def contract() -> dict[str, Any]:
    """加载 data-analytics 契约（YAML）。"""
    return load_yaml(CONTRACT_PATH)


# =====================================================================
# 一、契约自身合法性与端点完整性（12.4 节）
# =====================================================================
def test_contract_is_openapi_303(contract: dict[str, Any]) -> None:
    """契约必须为 OpenAPI 3.0.3，且声明生产与本地地址。"""
    assert contract["openapi"] == "3.0.3"
    urls = [server["url"] for server in contract["servers"]]
    assert any("hunter-edge.example.com" in url for url in urls)
    assert any("localhost:8083" in url for url in urls)


def test_all_refs_resolve(contract: dict[str, Any]) -> None:
    """所有 `$ref` 必须能在文档内解析（无悬空引用）。"""
    refs = list(iter_refs(contract))
    assert refs, "契约应包含组件引用"
    for ref in refs:
        resolve_pointer(contract, ref)


def test_operation_ids_are_unique_and_snake_case(contract: dict[str, Any]) -> None:
    """`operationId` 必须唯一且为小驼峰稳定命名（供 SDK/前端生成使用）。"""
    operations = list(iter_operations(contract))
    assert len(operations) == len(EXPECTED_ENDPOINTS) + len(EXPECTED_OPS_ENDPOINTS)
    ids = [operation["operationId"] for _method, _path, operation in operations]
    assert ids and len(ids) == len(set(ids)), f"operationId 重复: {ids}"
    for operation_id in ids:
        assert re.fullmatch(r"[a-z][A-Za-z0-9]*", operation_id), operation_id


def test_business_endpoints_match_12_4_inventory(contract: dict[str, Any]) -> None:
    """业务端点集合必须与设计文档 12.4 节清单逐条一致（不可增删）。"""
    actual = {
        (method, path)
        for method, path, _operation in iter_operations(contract)
        if (method, path) not in EXPECTED_OPS_ENDPOINTS
    }
    assert actual == EXPECTED_ENDPOINTS, f"端点集合偏差: {actual ^ EXPECTED_ENDPOINTS}"

    declared = {
        (entry.split(" ", 1)[0], entry.split(" ", 1)[1])
        for entry in contract["x-hunter-endpoints"]["endpoints"]
    }
    assert declared == EXPECTED_ENDPOINTS
    assert set(contract["x-hunter-endpoints"]["ops_endpoints"]) == {
        f"{method} {path}" for method, path in EXPECTED_OPS_ENDPOINTS
    }


def test_only_declared_write_operation_exists(contract: dict[str, Any]) -> None:
    """本服务为读模型 + 编排：唯一写操作为报告生成（POST），其余端点一律 GET。"""
    write_ops = {
        (method, path)
        for method, path, _operation in iter_operations(contract)
        if method != "GET"
    }
    assert write_ops == {("POST", "/api/v1/analytics/reports/generate")}
    generate = contract["paths"]["/api/v1/analytics/reports/generate"]["post"]
    assert set(generate["responses"]) >= {"202", "409", "422"}
    assert "202" in generate["responses"], "报告生成必须异步受理（202）"
    assert "REPORT_GENERATE_MAX_CONCURRENT" in generate["description"]


def test_gateway_prefix_and_ingress_alignment(contract: dict[str, Any]) -> None:
    """网关前缀 /api/v1/analytics → data-analytics:8083；Ingress 统一指向网关（不可绕过鉴权）。"""
    service_meta = contract["x-hunter-service"]
    assert service_meta["gateway_prefix"] == "/api/v1/analytics"
    for _method, path, _operation in iter_operations(contract):
        if path in {"/healthz", "/readyz", "/metrics"}:
            continue
        assert path.startswith(service_meta["gateway_prefix"]), path

    gateway = load_yaml(GATEWAY_CONTRACT)
    routes = gateway["x-hunter-gateway-routes"]["routes"]
    route = next(entry for entry in routes if entry["prefix"] == "/api/v1/analytics")
    assert route["target_service"] == "data-analytics"
    assert route["target_port"] == service_meta["port"]
    assert route["auth"] == "jwt"
    assert gateway["x-hunter-gateway-routes"]["strip_prefix"] is False

    ingress_docs = k8s_docs(INGRESS)
    ingress_paths = [
        path
        for doc in ingress_docs
        for rule in doc.get("spec", {}).get("rules", [])
        for path in rule.get("http", {}).get("paths", [])
    ]
    analytics_path = next(entry for entry in ingress_paths if entry["path"] == "/api/v1/analytics")
    backend = analytics_path["backend"]["service"]
    assert backend["name"] == "api-gateway", "Ingress 必须统一指向网关（禁止绕过 JWT/限流）"
    assert backend["port"]["number"] == 8080


def test_ops_endpoints_match_app_routes(contract: dict[str, Any]) -> None:
    """运维探针必须与 `app.main` 实际注册路由一致，且契约中声明为内部端点（免鉴权）。"""
    registered = set(iter_registered_routes(app.routes))
    for endpoint in EXPECTED_OPS_ENDPOINTS:
        assert endpoint in registered, f"未注册的路由: {endpoint}"
    for _method, path, operation in iter_operations(contract):
        if path in {"/healthz", "/readyz", "/metrics"}:
            assert operation["x-internal"] is True
            assert operation["security"] == []


# =====================================================================
# 二、统一响应与错误码
# =====================================================================
def test_response_envelope_matches_platform_convention(contract: dict[str, Any]) -> None:
    """统一响应五字段 + 数据类响应必须 allOf 组合 ApiResponse。"""
    schemas = contract["components"]["schemas"]
    assert set(schemas["ApiResponse"]["required"]) == {
        "code",
        "message",
        "data",
        "request_id",
        "timestamp",
    }
    data_responses = [
        name
        for name, schema in schemas.items()
        if name.endswith("Response") and name != "ApiResponse"
    ]
    assert len(data_responses) >= 6, f"数据类响应数量异常: {data_responses}"
    for name in data_responses:
        parts = schemas[name]["allOf"]
        assert {"$ref": "#/components/schemas/ApiResponse"} in parts, name
        body = next(part for part in parts if isinstance(part, dict) and "properties" in part)
        assert "data" in body["properties"], f"{name} 缺少 data 包装"


def test_error_codes_are_predefined_and_status_map_matches(contract: dict[str, Any]) -> None:
    """错误码只能取预定义值；错误码 → HTTP 状态映射必须与实现完全一致。"""
    schemas = contract["components"]["schemas"]
    assert set(schemas["ErrorCode"]["enum"]) == {int(code) for code in ErrorCode}
    declared = {
        int(code): int(status) for code, status in contract["x-hunter-error-status-map"]["map"].items()
    }
    assert declared == HTTP_STATUS_BY_CODE
    assert declared[int(ErrorCode.RESOURCE_STATE_CONFLICT)] == 409
    assert declared[int(ErrorCode.RESOURCE_NOT_FOUND)] == 404
    responses = contract["components"]["responses"]
    assert {"NotFound", "Conflict", "TooManyRequests"} <= set(responses)


def test_rate_limits_inherit_appendix_d(contract: dict[str, Any]) -> None:
    """限流继承网关五级限流；附录 D 未点名本服务端点 → 不得自造接口级 QPS。"""
    limits = contract["x-hunter-rate-limits"]
    assert limits["inherit_gateway"] == {
        "global_qps": 10000,
        "per_user_qps": 100,
        "per_ip_qps": 200,
    }
    assert limits["endpoint_limits"] == [], "附录 D 未定义本服务接口级限流，禁止自行新增"
    assert limits["over_limit"]["http_status"] == 429
    assert limits["over_limit"]["response_code"] == int(ErrorCode.SERVICE_UNAVAILABLE)
    assert limits["over_limit"]["retry_after_header"] == "Retry-After"
    assert limits["heavy_protection"]["report_generate_max_concurrent"]["default"] >= 1
    assert (
        limits["heavy_protection"]["query_max_range_days"]["env"]
        == "ANALYTICS_QUERY_MAX_RANGE_DAYS"
    )
    assert MAX_PAGE_SIZE == 200, "分页上限须与 BaseRepository.paginate 一致"
    page_size = contract["components"]["parameters"]["PageSizeQuery"]["schema"]
    assert page_size["maximum"] == MAX_PAGE_SIZE


# =====================================================================
# 三、6.2 节 Flink 实时分析（作业 / 阈值 / 告警）
# =====================================================================
def test_realtime_jobs_match_6_2(contract: dict[str, Any]) -> None:
    """6.2 节 5 个 Flink 实时作业必须齐备，且各自声明输入 Topic 与输出通道。"""
    realtime = contract["x-hunter-realtime-jobs"]
    assert "Flink 1.18" in realtime["engine"]
    jobs = realtime["jobs"]
    assert {job["name"] for job in jobs} == EXPECTED_REALTIME_JOBS
    for job in jobs:
        assert job["input"] in {"telemetry_clean", "telemetry_raw"}, job["name"]
        assert job.get("logic") or job.get("rules"), f"{job['name']} 缺少规则/逻辑说明"
        assert job["outputs"], f"{job['name']} 缺少输出声明"
        assert job["section"], f"{job['name']} 必须标注设计文档章节来源"
    # 数据质量监控必须消费原始流（与清洗后数据对比才有意义）
    quality = job_by_name(jobs, "data_quality_monitor")
    assert quality["input"] == "telemetry_raw"
    assert realtime["outputs"]["alert_event"].startswith("告警")


def test_realtime_job_inputs_match_consumer_groups(contract: dict[str, Any]) -> None:
    """实时作业的输入 Topic/消费组必须与 contracts/kafka/consumer-groups.yaml 双向一致。"""
    realtime = contract["x-hunter-realtime-jobs"]
    groups = {
        group["group_id"]: group
        for group in load_yaml(CONSUMER_GROUPS)["groups"]
        if group["service"] == "data-analytics"
    }
    assert len(groups) == 4, f"data-analytics 消费组数量异常: {sorted(groups)}"
    for entry in realtime["input"]:
        group = groups[entry["consumer_group"]]
        assert group["subscribes"] == [entry["topic"]]
        assert set(entry["jobs"]) <= set(group["jobs"])
    # 生产侧一致：alert_event + analytics_result，且不得声明生产 telemetry_clean
    produces = {entry["topic"] for entry in contract["x-hunter-kafka"]["produces"]}
    assert produces == {"analytics_result", "alert_event"}
    for group in groups.values():
        assert "telemetry_clean" not in group["produces"], (
            "telemetry_clean 生产者为 data-collector（topics.yaml），data-analytics 不得声明生产"
        )
    assert set(groups["data-analytics-telemetry"]["jobs"]) == {
        "vehicle_state_monitor",
        "driving_anomaly_detection",
        "collision_risk_assessment",
        "algorithm_performance_monitor",
    }


def test_driving_anomaly_rules_match_event_contract(contract: dict[str, Any]) -> None:
    """6.2.2 节异常驾驶规则（名称 + 等级）必须与受控词表 EVENT_LEVEL_BY_TYPE 一致。"""
    jobs = contract["x-hunter-realtime-jobs"]["jobs"]
    rules = job_by_name(jobs, "driving_anomaly_detection")["rules"]
    assert {rule["name"] for rule in rules} == {
        "harsh_acceleration",
        "harsh_braking",
        "harsh_turning",
        "over_speed",
        "battery_low",
        "battery_critical",
    }
    for rule in rules:
        event_type = EventType(rule["name"])
        assert rule["level"] == EVENT_LEVEL_BY_TYPE[event_type].value, rule["name"]
    joined = "\n".join(rule["condition"] for rule in rules)
    for keyword in ("3.0 m/s²", "0.5s", "0.8 rad/s", "1.1", "SOC", "20", "10"):
        assert keyword in joined, f"阈值表述缺失: {keyword}"


def test_realtime_thresholds_match_contract_and_configmap(contract: dict[str, Any]) -> None:
    """实时阈值三方一致：契约声明 ↔ 设计文档受控值 ↔ K8s ConfigMap（禁止硬编码）。"""
    thresholds = {
        entry["name"]: (entry["value"], entry["unit"])
        for entry in contract["x-hunter-realtime-jobs"]["thresholds"]
    }
    for name, (value, unit) in EXPECTED_REALTIME_THRESHOLDS.items():
        assert name in thresholds, f"契约缺少阈值 {name}"
        assert thresholds[name] == (value, unit), f"{name} 阈值/单位与设计文档不一致"

    config = service_config_map(K8S_MANIFEST)
    for name, (value, _unit) in EXPECTED_REALTIME_THRESHOLDS.items():
        assert name in config, f"K8s ConfigMap 未注入阈值 {name}（禁止硬编码）"
        assert float(config[name]) == pytest.approx(float(value)), name

    # 告警触发延迟 ≤ 2s（性能指标）与 timing 声明一致
    realtime = contract["x-hunter-realtime-jobs"]
    assert realtime["timeliness"]["target_latency_ms"] == EXPECTED_ALERT_LATENCY_MS
    assert (
        thresholds["ALERT_TRIGGER_MAX_LATENCY_SECONDS"][0] * 1000 == EXPECTED_ALERT_LATENCY_MS
    )
    assert contract["x-hunter-service"]["performance"]["alert_trigger_latency_ms"] == (
        EXPECTED_ALERT_LATENCY_MS
    )
    assert contract["x-hunter-service"]["performance"]["api_p95_ms"] == EXPECTED_API_P95_MS


def test_alert_contract_level_mapping_matches_orm(contract: dict[str, Any]) -> None:
    """告警等级映射必须与受控词表 EVENT_LEVEL_BY_TYPE 逐条一致（不可放宽）。"""
    alert = contract["x-hunter-alert-contract"]
    assert alert["topic"] == "alert_event"
    assert alert["schema"] == "contracts/kafka/schemas/alert_event.schema.json"
    expected = {
        event_type.value: EVENT_LEVEL_BY_TYPE[event_type].value for event_type in EventType
    }
    assert alert["level_mapping"]["by_type"] == expected
    assert len(alert["level_mapping"]["by_type"]) == 18
    assert set(alert["level_mapping"]["by_type"].values()) == {
        level.value for level in EventLevel
    }
    # 平台侧重算的类型集合 ⊆ 受控词表；车端上报类型不由本服务重算
    emitted = " ".join(alert["emitted_types"])
    for event_type in ("communication_loss", "harsh_braking", "over_speed", "battery_low",
                       "battery_critical", "collision_warning", "harsh_acceleration",
                       "harsh_turning"):
        assert event_type in emitted, f"实时作业未声明产出 {event_type}"
    assert "manual_takeover" in " ".join(alert["not_emitted"])


def test_alert_event_schema_matches_alert_contract(contract: dict[str, Any]) -> None:
    """alert_event Schema ↔ 契约告警规则 ↔ topics.yaml/消费组 三处一致。"""
    schema = load_json(SCHEMA_DIR / "alert_event.schema.json")
    assert schema["additionalProperties"] is False
    properties = schema["properties"]
    alert = contract["x-hunter-alert-contract"]
    # 类型 = 受控词表 18 种；等级 = 3 级；来源作业 = 6.2 节 5 个作业
    assert set(properties["alert_type"]["enum"]) == {t.value for t in EventType}
    assert set(properties["level"]["enum"]) == {level.value for level in EventLevel}
    assert set(properties["source_job"]["enum"]) == EXPECTED_REALTIME_JOBS
    assert set(properties["source_job"]["enum"]) == {
        "vehicle_state_monitor",
        "driving_anomaly_detection",
        "algorithm_performance_monitor",
        "collision_risk_assessment",
        "data_quality_monitor",
    }
    # 阈值随消息回带（rule），便于消费方审计而不硬编码
    assert set(properties["rule"]["required"]) == {"name", "metric", "comparator", "threshold"}
    assert "阈值" in properties["rule"]["description"]
    for example in schema["examples"]:
        assert example["level"] == alert["level_mapping"]["by_type"][example["alert_type"]]

    # topics.yaml ↔ 契约：生产者/消费者/Schema 引用
    platform = {
        entry["name"]: entry for entry in load_yaml(TOPICS_CONTRACT)["platform_topics"]
    }
    assert platform["alert_event"]["schema"] == "schemas/alert_event.schema.json"
    assert platform["alert_event"]["producer"] == "data-analytics"
    assert platform["alert_event"]["idempotency_key"] == "(vehicle_id, alert_id)"
    assert contract["x-hunter-kafka"]["produces"][1]["schema"] == (
        "contracts/kafka/schemas/alert_event.schema.json"
    )
    assert contract["x-hunter-kafka"]["produces"][1]["key"] == "vehicle_id"


def test_collision_ttc_level_conflict_is_registered(contract: dict[str, Any]) -> None:
    """6.2.3 节 3.0s 预警与受控词表等级冲突必须显式登记（禁止静默放宽等级）。"""
    jobs = contract["x-hunter-realtime-jobs"]["jobs"]
    collision = job_by_name(jobs, "collision_risk_assessment")
    assert collision["section"] == "6.2.3"
    assert "TTC = d / v_rel" in " ".join(collision["logic"])
    assert "collision_warning" in collision["conflict_note"]
    assert "pending #3" in collision["conflict_note"]

    questions = "\n".join(
        f"{item['question']}\n{item['contract_decision']}"
        for item in contract["x-hunter-pending-confirmation"]["items"]
    )
    assert "TTC" in questions and "collision_warning" in questions
    # 受控词表未扩展：alert_event.alert_type 仍为 18 种事件类型（3.0s 预警不新增类型）
    schema = load_json(SCHEMA_DIR / "alert_event.schema.json")
    assert len(schema["properties"]["alert_type"]["enum"]) == 18
    rules = job_by_name(jobs, "driving_anomaly_detection")["rules"]
    for rule in rules:
        assert rule["level"] == EVENT_LEVEL_BY_TYPE[EventType(rule["name"])].value


# =====================================================================
# 四、6.3 / 6.4 节离线分析与评估
# =====================================================================
def test_offline_jobs_match_design_sections(contract: dict[str, Any]) -> None:
    """6.3 / 6.4 / 6.5 节离线作业齐备，调度（cron）声明完整且为 5 段标准 cron（UTC）。"""
    offline = contract["x-hunter-offline-jobs"]
    assert "Spark 3.5" in offline["engine"]
    jobs = offline["jobs"]
    assert {job["name"] for job in jobs} == EXPECTED_OFFLINE_JOBS
    crons = service_config_map(K8S_MANIFEST)
    for job in jobs:
        schedule = job["schedule"]
        assert schedule["timezone"] == "UTC"
        assert re.fullmatch(r"(\S+\s+){4}\S+", schedule["default_cron"]), job["name"]
        assert schedule["env"] in contract["x-hunter-service"]["required_env"], job["name"]
        assert crons[schedule["env"]] == schedule["default_cron"], f"{job['name']} cron 不一致"
        assert job["outputs"], f"{job['name']} 缺少输出声明"
    # 频率归属：日报/评估每日，覆盖率与挖掘每周，月报每月
    assert job_by_name(jobs, "daily_report")["schedule"]["default_cron"].endswith("* * *")
    assert job_by_name(jobs, "scene_coverage")["schedule"]["default_cron"].endswith("* * 1")
    assert job_by_name(jobs, "corner_case_mining")["schedule"]["default_cron"].endswith("* * 1")
    assert job_by_name(jobs, "monthly_operation")["schedule"]["default_cron"].endswith("1 * *")


def test_control_eval_thresholds_match_6_3_3(contract: dict[str, Any]) -> None:
    """6.3.3 节控制性能阈值必须在契约、Schema、ConfigMap 三处一致（不可放宽）。"""
    job = job_by_name(contract["x-hunter-offline-jobs"]["jobs"], "control_eval")
    declared = {entry["name"]: entry for entry in job["thresholds"]}
    assert set(declared) == set(EXPECTED_CONTROL_THRESHOLDS)
    for name, threshold in EXPECTED_CONTROL_THRESHOLDS.items():
        assert declared[name]["threshold"] == threshold, name
        assert declared[name]["comparator"] == "lt", f"{name} 判定方向必须为 lt"
        assert "6.3.3" in declared[name]["source"]

    schemas = contract["components"]["schemas"]
    metrics = schemas["ControlEvalData"]["properties"]["metrics"]
    thresholds = schemas["ControlEvalData"]["properties"]["thresholds"]
    assert set(metrics["required"]) == set(EXPECTED_CONTROL_THRESHOLDS)
    assert set(thresholds["required"]) == set(EXPECTED_CONTROL_THRESHOLDS)
    for name in EXPECTED_CONTROL_THRESHOLDS:
        assert thresholds["properties"][name] == {
            "$ref": "#/components/schemas/ControlThresholdCheck"
        }
    check = schemas["ControlThresholdCheck"]
    assert set(check["required"]) == {"value", "threshold", "comparator", "pass"}
    assert set(check["properties"]["comparator"]["enum"]) == {"lt", "lte", "gt", "gte"}
    assert "overall_pass" in schemas["ControlEvalData"]["required"]


def test_perception_eval_metrics_match_6_3(contract: dict[str, Any]) -> None:
    """6.3.2 节感知精度指标（mAP 3D/BEV、IoU、Recall、Precision、平均定位误差）齐备。"""
    job = job_by_name(contract["x-hunter-offline-jobs"]["jobs"], "perception_eval")
    assert set(job["metrics"]) == EXPECTED_PERCEPTION_METRICS
    metrics = contract["components"]["schemas"]["PerceptionMetrics"]
    assert set(metrics["required"]) == EXPECTED_PERCEPTION_METRICS
    for name in ("map_3d", "map_bev", "iou", "recall", "precision"):
        assert metrics["properties"][name]["maximum"] == 1
        assert metrics["properties"][name]["minimum"] == 0
    data = contract["components"]["schemas"]["PerceptionEvalData"]
    assert "by_object_type" in data["properties"], "需支持按目标类型分维度指标"
    assert "真值" in contract["paths"]["/api/v1/analytics/perception/eval"]["get"]["description"]


def test_corner_case_contract_matches_6_4(contract: dict[str, Any]) -> None:
    """6.4 节：5 类异常 + Isolation Forest / DBSCAN + 事件前后各 10 秒截取窗口。"""
    mining = contract["x-hunter-corner-case-mining"]
    assert mining["frequency"] == "weekly"
    assert {entry["name"] for entry in mining["categories"]} == EXPECTED_CORNER_CASE_CATEGORIES
    assert {entry["name"] for entry in mining["algorithms"]} == EXPECTED_CORNER_CASE_ALGORITHMS
    assert mining["clipping"] == {
        "pre_seconds": 10,
        "post_seconds": 10,
        "source": "4.5 节：事件前后各 10 秒（与 analytics_result.clip 一致，不可更改）",
    }
    schemas = contract["components"]["schemas"]
    assert set(schemas["CornerCaseCategory"]["enum"]) == EXPECTED_CORNER_CASE_CATEGORIES
    assert set(schemas["CornerCaseAlgorithm"]["enum"]) == EXPECTED_CORNER_CASE_ALGORITHMS
    item = schemas["CornerCaseItem"]
    assert {"corner_case_id", "category", "anomaly_score", "algorithm"} <= set(item["required"])
    assert item["properties"]["anomaly_score"]["maximum"] == 1
    # 与 analytics_result Schema 的截取窗口一致（scene-service 实车场景提取依赖）
    analytics_result = load_json(SCHEMA_DIR / "analytics_result.schema.json")
    clip = analytics_result["properties"]["clip"]["properties"]
    assert clip["pre_seconds"]["enum"] == [mining["clipping"]["pre_seconds"]]
    assert clip["post_seconds"]["enum"] == [mining["clipping"]["post_seconds"]]
    assert "corner_case" in analytics_result["properties"]["result_type"]["enum"]
    # 端点过滤参数与类别枚举一致
    params = contract["paths"]["/api/v1/analytics/corner-cases"]["get"]["parameters"]
    category = next(param for param in params if param.get("name") == "category")
    assert category["schema"] == {"$ref": "#/components/schemas/CornerCaseCategory"}
    assert any(param.get("name") == "min_anomaly_score" for param in params)


# =====================================================================
# 五、6.5 节报告生成与 MinIO 存储
# =====================================================================
def test_report_templates_match_6_5(contract: dict[str, Any]) -> None:
    """6.5 节 5 类标准报告模板 + 三种输出格式必须齐备且与 Schema 枚举一致。"""
    templates = contract["x-hunter-report-templates"]
    declared = {entry["report_type"] for entry in templates["templates"]}
    assert declared == EXPECTED_REPORT_TYPES
    for entry in templates["templates"]:
        assert entry["name"] and entry["sections"] and entry["scope"]
        if entry["trigger"] == "schedule":
            assert entry["cron_env"] in contract["x-hunter-service"]["required_env"]
    assert set(templates["formats"]["supported"]) == EXPECTED_REPORT_FORMATS
    assert "Puppeteer" in templates["formats"]["pdf"]
    schemas = contract["components"]["schemas"]
    assert set(schemas["ReportType"]["enum"]) == EXPECTED_REPORT_TYPES
    assert set(schemas["ReportFormat"]["enum"]) == EXPECTED_REPORT_FORMATS
    assert set(schemas["ReportStatus"]["enum"]) == EXPECTED_REPORT_STATUSES
    assert templates["retention"].startswith("hunter-reports 永久保留")


def test_report_generation_is_async_and_guarded(contract: dict[str, Any]) -> None:
    """报告生成异步语义（202 + 轮询）与并发/窗口保护必须与环境变量绑定。"""
    generation = contract["x-hunter-report-templates"]["generation"]
    assert generation["mode"] == "async"
    assert generation["submit_endpoint"] == "POST /api/v1/analytics/reports/generate"
    assert generation["poll_endpoint"] == "GET /api/v1/analytics/reports/{report_id}"
    assert generation["concurrency_env"] == "REPORT_GENERATE_MAX_CONCURRENT"
    assert generation["max_range_env"] == "REPORT_MAX_RANGE_DAYS"

    request_schema = contract["components"]["schemas"]["ReportGenerateRequest"]
    assert set(request_schema["required"]) == {"report_type", "start_time", "end_time"}
    assert request_schema["properties"]["formats"]["default"] == ["pdf"]
    assert request_schema["properties"]["force"]["default"] is False
    data = contract["components"]["schemas"]["ReportGenerateData"]
    assert data["properties"]["status"]["$ref"].endswith("ReportStatus")
    assert "poll_url" in data["properties"]

    config = service_config_map(K8S_MANIFEST)
    assert config["REPORT_MAX_RANGE_DAYS"] == "90"
    assert config["REPORT_GENERATE_MAX_CONCURRENT"] == "2"
    assert config["REPORT_DOWNLOAD_EXPIRES_SECONDS"] == str(EXPECTED_REPORT_DOWNLOAD_EXPIRES)


def test_report_storage_matches_minio_contract(contract: dict[str, Any]) -> None:
    """报告存储必须落 hunter-reports（永久）并遵循预签名时效（下载 15 分钟 / 上传 1 小时）。"""
    storage = contract["x-hunter-report-storage"]
    assert storage["bucket"] == EXPECTED_REPORT_BUCKET
    assert EXPECTED_REPORT_BUCKET in contract["x-hunter-service"]["minio_buckets"]
    lifecycle = contract["x-hunter-service"]["minio_bucket_lifecycle"]
    assert "永久" in lifecycle[EXPECTED_REPORT_BUCKET]
    assert storage["presign"]["download_expires_in_seconds"] == EXPECTED_REPORT_DOWNLOAD_EXPIRES
    assert storage["presign"]["upload_expires_in_seconds"] == EXPECTED_REPORT_UPLOAD_EXPIRES
    assert storage["presign"]["range_download"] is True

    artifact = contract["components"]["schemas"]["ReportArtifact"]
    assert artifact["properties"]["expires_in"]["example"] == EXPECTED_REPORT_DOWNLOAD_EXPIRES
    assert "Range" in artifact["properties"]["download_url"]["description"]
    assert "hunter-reports" in artifact["properties"]["object_key"]["description"]
    documents = storage["object_layout"]["eval_documents"]
    assert set(documents) == {
        "perception_eval",
        "control_eval",
        "scene_coverage",
        "corner_cases",
    }
    assert storage["status"] == "pending_confirmation", "无 DB 表属契约缺口，必须显式登记"
    assert "hunter-reports" in contract["paths"]["/api/v1/analytics/reports"]["get"]["description"]


# =====================================================================
# 六、数据访问边界 / 看板 / Kafka 参与度
# =====================================================================
def test_db_access_follows_cross_schema_exception(contract: dict[str, Any]) -> None:
    """DB 访问必须限定在 data_analytics schema + er.md 登记的只读例外内。"""
    service_meta = contract["x-hunter-service"]
    assert service_meta["db_schema"] == "data_analytics"
    assert service_meta["db_tables"] == ["algorithm_metrics"]
    columns = ddl_columns(DDL_TIMESERIES, "data_analytics.algorithm_metrics")
    assert {"time", "vehicle_id", "module", "metric_name", "metric_value", "tags"} <= columns

    read_only = " ".join(service_meta["db_access"]["read_only"])
    assert "data_collector.vehicle_telemetry" in read_only
    assert "hunter_analytics_ro" in read_only
    assert "data_analytics.algorithm_metrics" in read_only
    forbidden = " ".join(service_meta["db_access"]["forbidden"])
    for table in ("data_collector.events", "scene_svc.scenes", "vehicle_svc"):
        assert table in forbidden
    er_text = ER_DOC.read_text(encoding="utf-8")
    assert "hunter_analytics_ro" in er_text, "只读例外必须已在 er.md 登记"
    assert service_meta["db_tables_absent"]["status"] == "pending_confirmation"


def test_dashboard_contract_covers_blocks_and_degradation(contract: dict[str, Any]) -> None:
    """看板四块（车队/管道/算法/事件）+ 局部降级语义，且不得跨 schema 直查或新增 Redis 键。"""
    dashboard = contract["x-hunter-dashboard-contract"]
    assert set(dashboard["blocks"]) == {"fleet", "pipeline", "algorithm", "events"}
    data = contract["components"]["schemas"]["DashboardData"]
    assert {"fleet", "pipeline", "algorithm", "events"} <= set(data["required"])
    assert "degraded" in data["properties"]
    for block in dashboard["blocks"].values():
        assert block["sources"], "每个聚合块必须声明数据源"
    assert "available=false" in dashboard["response_semantics"]["partial_failure"]

    events_source = " ".join(dashboard["blocks"]["events"]["sources"])
    assert "data-collector REST" in events_source
    collector_endpoints = load_yaml(DATA_COLLECTOR_CONTRACT)["x-hunter-endpoints"]["endpoints"]
    assert "GET /api/v1/data/events" in collector_endpoints

    assert "禁止新增键模式" in contract["x-hunter-service"]["redis_keys"]["note"]
    assert set(contract["x-hunter-service"]["redis_keys"]["read"]) == {
        "vehicle:online:set",
        "vehicle:status:{vehicle_id}",
    }


def test_kafka_participation_matches_contracts(contract: dict[str, Any]) -> None:
    """Kafka 消费/生产必须与 topics.yaml + consumer-groups.yaml 双向一致，且不越界订阅。"""
    kafka = contract["x-hunter-kafka"]
    platform = {
        entry["name"]: entry for entry in load_yaml(TOPICS_CONTRACT)["platform_topics"]
    }
    consumed = {entry["topic"] for entry in kafka["consumes"]}
    assert consumed == {"telemetry_clean", "telemetry_raw", "event_raw", "sensor_file"}
    for entry in kafka["consumes"]:
        assert entry["topic"] in platform, entry["topic"]
        assert (ROOT / entry["schema"]).is_file(), entry["schema"]
        assert entry["enabled"] is True
    produced = {entry["topic"] for entry in kafka["produces"]}
    assert produced == {"analytics_result", "alert_event"}
    for entry in kafka["produces"]:
        assert platform[entry["topic"]]["producer"] == "data-analytics"
        assert (ROOT / entry["schema"]).is_file(), entry["schema"]
        assert entry["key"] == "vehicle_id"

    not_owned = set(kafka["not_owned"]["topics"])
    assert not_owned & consumed == set(), "禁止订阅未归属本服务的 Topic"
    assert "hunter.{vehicle_id}.telemetry" in not_owned
    assert kafka["dlq"] == (
        "{original_topic}.dlq（hunter_common.kafka.consumer 统一派生，"
        "保留原 topic/partition/offset 头）"
    )
    assert kafka["offset_commit"].startswith("手动提交")

    groups = {group["group_id"]: group for group in load_yaml(CONSUMER_GROUPS)["groups"]}
    for entry in kafka["consumes"]:
        assert (
            groups[entry["group_id"]]["idempotency_key"]
            == kafka["idempotency_keys"][entry["topic"]]
        )
    result_schema = load_json(SCHEMA_DIR / "analytics_result.schema.json")
    assert "metric" in result_schema["properties"]["result_type"]["enum"]


def test_scene_coverage_contract_bounds(contract: dict[str, Any]) -> None:
    """场景覆盖率端点：热力图截断保护 + 未覆盖清单结构 + 场景库经 REST 获取。"""
    operation = contract["paths"]["/api/v1/analytics/scene/coverage"]["get"]
    params = operation["parameters"]
    max_cells = next(param for param in params if param.get("name") == "max_cells")
    assert max_cells["schema"]["default"] == 5000
    assert max_cells["schema"]["maximum"] == 50000
    grid = next(param for param in params if param.get("name") == "grid_size_m")
    assert grid["schema"]["default"] == 10

    data = contract["components"]["schemas"]["SceneCoverageData"]
    assert {"heatmap", "uncovered", "coverage_ratio", "covered_cells", "total_cells"} <= set(
        data["required"]
    )
    assert data["properties"]["coverage_ratio"]["maximum"] == 1
    assert data["properties"]["heatmap"]["items"] == {"$ref": "#/components/schemas/CoverageCell"}
    uncovered = contract["components"]["schemas"]["UncoveredArea"]
    assert set(uncovered["properties"]["kind"]["enum"]) == {"region", "scene_type"}
    cell = contract["components"]["schemas"]["CoverageCell"]
    assert set(cell["required"]) == {"x", "y", "count"}
    assert "scene-service REST" in operation["description"]
    assert "禁止直查" in operation["description"]


# =====================================================================
# 七、配置接线 / K8s 清单 / 待确认项
# =====================================================================
def test_required_env_keys_are_wired(contract: dict[str, Any]) -> None:
    """契约声明的环境变量必须实际由 ConfigMap/Secret 注入（禁止硬编码，禁止未接线）。"""
    service_meta = contract["x-hunter-service"]
    service_config = service_config_map(K8S_MANIFEST)
    common = service_config_map(K8S_COMMON_CONFIG)
    secret: dict[str, Any] = {}
    for doc in k8s_docs(K8S_SECRET):
        secret.update(doc.get("stringData") or {})

    required = set(service_meta["required_env"])
    derived = {entry["name"] for entry in service_meta["env_derived"]}
    assert derived <= required, f"env_derived 未在 required_env 中声明: {sorted(derived - required)}"
    wired = set(service_config) | set(common) | set(secret)
    missing = required - derived - wired
    assert not missing, f"以下环境变量未在 K8s 配置中注入: {sorted(missing)}"

    # 只读账号口令必须走 Secret（禁止出现在 ConfigMap）
    assert "ANALYTICS_RO_DB_PASSWORD" in secret
    assert "ANALYTICS_RO_DB_PASSWORD" not in service_config
    assert {"POSTGRES_PASSWORD", "MINIO_ACCESS_KEY"} <= set(secret)

    # 阈值与离线作业 cron 必须全部在服务级 ConfigMap 中（禁止硬编码）
    offline_envs = [job["schedule"]["env"] for job in contract["x-hunter-offline-jobs"]["jobs"]]
    for name in list(EXPECTED_REALTIME_THRESHOLDS) + offline_envs:
        assert name in service_config, f"ConfigMap 缺少 {name}"


def test_k8s_manifest_matches_service_meta(contract: dict[str, Any]) -> None:
    """K8s 清单（Deployment/Service）必须与契约的端口/探针/多副本一致。"""
    service_meta = contract["x-hunter-service"]
    docs = docs_by_kind(K8S_MANIFEST)
    assert {"ConfigMap", "Deployment", "Service"} <= set(docs)
    assert docs["ConfigMap"]["data"]["SERVICE_NAME"] == service_meta["name"]
    assert int(docs["ConfigMap"]["data"]["API_PORT"]) == service_meta["port"]

    deployment = docs["Deployment"]["spec"]
    assert deployment["replicas"] >= 2, "无状态服务多副本部署（高可用原则）"
    container = deployment["template"]["spec"]["containers"][0]
    assert container["ports"][0]["containerPort"] == service_meta["port"]
    annotations = deployment["template"]["metadata"]["annotations"]
    assert annotations["prometheus.io/port"] == str(service_meta["port"])
    assert annotations["prometheus.io/path"] == "/metrics"
    assert container["readinessProbe"]["httpGet"]["path"] == "/readyz"
    assert container["livenessProbe"]["httpGet"]["path"] == "/healthz"
    assert docs["Service"]["spec"]["ports"][0]["port"] == service_meta["port"]


def test_pending_confirmation_items_are_structured(contract: dict[str, Any]) -> None:
    """待确认项必须结构完整、ID 唯一（供人工评审逐条闭环）。"""
    items = contract["x-hunter-pending-confirmation"]["items"]
    assert len(items) >= 12
    ids = [item["id"] for item in items]
    assert ids == sorted(ids) and len(ids) == len(set(ids))
    for item in items:
        assert item["question"] and item["contract_decision"] and item["impact"]
    text = "\n".join(
        f"{item['question']}\n{item['contract_decision']}" for item in items
    )
    for keyword in ("报告", "异步", "TTC", "alert_event", "12.4 节", "消费组"):
        assert keyword in text, f"关键待确认项缺失: {keyword}"


def test_contract_registers_gaps_without_new_endpoints_or_topics(contract: dict[str, Any]) -> None:
    """契约缺口必须登记而非擅自扩展（12.4 节端点 / Kafka Topic / DB 表 / Redis 键）。"""
    endpoints = contract["x-hunter-endpoints"]
    assert endpoints["status"] == "provided"
    assert len(endpoints["contract_gaps"]) >= 3
    joined = "\n".join(endpoints["contract_gaps"])
    for keyword in ("规划质量评估", "数据质量", "报告删除"):
        assert keyword in joined
    # Kafka：不新增 Topic（消费/生产均取自 contracts/kafka）
    platform = {
        entry["name"] for entry in load_yaml(TOPICS_CONTRACT)["platform_topics"]
    }
    assert {entry["topic"] for entry in contract["x-hunter-kafka"]["consumes"]} <= platform
    assert {entry["topic"] for entry in contract["x-hunter-kafka"]["produces"]} <= platform
    # DB：不新增表（仅使用既有 algorithm_metrics）
    assert contract["x-hunter-service"]["db_tables"] == ["algorithm_metrics"]
    # 12.4 节端点无「报告删除」→ 契约不得出现 DELETE 方法
    assert all(method != "DELETE" for method, _path, _op in iter_operations(contract))
