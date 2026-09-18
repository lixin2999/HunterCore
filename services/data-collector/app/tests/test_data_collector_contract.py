"""data-collector OpenAPI 契约测试（无需运行服务/基础设施）。

校验目标（契约是单一事实来源）：
- `contracts/openapi/data-collector.yaml` 合法：OpenAPI 3.0.3、`$ref` 可解析、`operationId` 唯一
- 业务端点集合 == `x-hunter-endpoints` 推导清单（12.2 节未随仓库提供，推导依据逐条登记）；
  运维探针与 `app.main` 实际注册路由一致
- 5.3.3 节遥测消息结构 ↔ 查询响应 Schema ↔ `data_collector.vehicle_telemetry` 列一致
- 事件受控词表（18 类型 / 3 等级）↔ ORM `EventType`/`EVENT_LEVEL_BY_TYPE` ↔ events DDL 一致
- 5.4 节预处理流程、5.5 节文件上传流程（预签名/校验/命名规范）与设计文档一致
- Kafka 参与度 ↔ `contracts/kafka/{topics,consumer-groups}.yaml`（含补全的 sensor_file Schema）
- 错误码只取预定义值且与实现 `HTTP_STATUS_BY_CODE` 一致；统一响应五字段
- 限流继承网关（含附录 D 的 20 QPS）/ Redis 键 / MinIO Bucket 与 K8s 清单三方一致

文件名在各服务内唯一：monorepo 中 pytest 以 basedir 相对路径推导模块名，各服务
`app/tests/` 均含 `__init__.py` 时同名文件会得到相同模块名并互相覆盖（模块级 fixture 丢失）。
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
from hunter_common.database.repository import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from hunter_common.exceptions import ErrorCode
from pydantic import BaseModel

from app.core.error_handlers import HTTP_STATUS_BY_CODE
from app.main import app
from app.schemas.events import EventItem, EventListData
from app.schemas.files import (
    CompletedPart,
    FileCompleteData,
    FileCompleteRequest,
    FileListData,
    FileObjectItem,
    FilePresignData,
    FilePresignPart,
    FilePresignRequest,
)
from app.schemas.telemetry import TelemetryQueryData, TelemetrySample

ROOT = Path(__file__).resolve().parents[4]
CONTRACT_PATH = ROOT / "contracts" / "openapi" / "data-collector.yaml"
GATEWAY_CONTRACT = ROOT / "contracts" / "openapi" / "api-gateway.yaml"
TOPICS_CONTRACT = ROOT / "contracts" / "kafka" / "topics.yaml"
CONSUMER_GROUPS = ROOT / "contracts" / "kafka" / "consumer-groups.yaml"
SCHEMA_DIR = ROOT / "contracts" / "kafka" / "schemas"
K8S_MANIFEST = ROOT / "infra" / "k8s" / "services" / "data-collector.yaml"
K8S_COMMON_CONFIG = ROOT / "infra" / "k8s" / "base" / "01-configmap-common.yaml"
K8S_SECRET = ROOT / "infra" / "k8s" / "base" / "02-secret.example.yaml"
INGRESS = ROOT / "infra" / "k8s" / "ingress.yaml"
DDL_EVENTS = ROOT / "contracts" / "database" / "ddl" / "04_events.sql"
DDL_TIMESERIES = ROOT / "contracts" / "database" / "ddl" / "05_timeseries.sql"

#: 业务端点（推导清单：附录 D 限流端点 + 5.5 上传流程 + events 表列能力）
EXPECTED_ENDPOINTS: set[tuple[str, str]] = {
    ("GET", "/api/v1/data/telemetry"),
    ("GET", "/api/v1/data/events"),
    ("GET", "/api/v1/data/events/{event_id}"),
    ("POST", "/api/v1/data/events/{event_id}/acknowledge"),
    ("GET", "/api/v1/data/files"),
    ("POST", "/api/v1/data/files/presign"),
    ("POST", "/api/v1/data/files/complete"),
}

EXPECTED_OPS_ENDPOINTS: set[tuple[str, str]] = {
    ("GET", "/healthz"),
    ("GET", "/readyz"),
    ("GET", "/metrics"),
}

#: 5.3.3 节遥测消息六段（嵌套结构不可更改）
TELEMETRY_SEGMENTS: tuple[str, ...] = (
    "chassis",
    "localization",
    "perception",
    "planning",
    "control",
    "system",
)
SEGMENT_SCHEMA_BY_FIELD: dict[str, str] = {
    "chassis": "TelemetryChassis",
    "localization": "TelemetryLocalization",
    "perception": "TelemetryPerception",
    "planning": "TelemetryPlanning",
    "control": "TelemetryControl",
    "system": "TelemetrySystem",
}

#: 5.4 节预处理流程步骤数（解析 → 校验 → 对齐 → 清洗 → enrichment → 序列化写入）
EXPECTED_PIPELINE_STEPS = 6
#: 5.5 节文件上传流程步骤数（请求上传 → 预签名 → 直传 → 完成通知 → 校验 → 登记）
EXPECTED_UPLOAD_STEPS = 6
#: 车辆维度限流（附录 D，不可放宽）
EXPECTED_VEHICLE_LIMITS: dict[str, int] = {
    "kafka_telemetry_msg_per_sec": 100,
    "file_upload_mbps": 10,
}
#: MinIO 预签名时效（上传 1 小时 / 下载 15 分钟，支持 Range）
EXPECTED_UPLOAD_EXPIRES = 3600
EXPECTED_DOWNLOAD_EXPIRES = 900

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


@pytest.fixture(scope="module")
def contract() -> dict[str, Any]:
    """加载 data-collector 契约（YAML）。"""
    return load_yaml(CONTRACT_PATH)


# =====================================================================
# 一、契约自身合法性与端点完整性
# =====================================================================
def test_contract_is_openapi_303(contract: dict[str, Any]) -> None:
    """契约必须为 OpenAPI 3.0.3，且声明生产与本地地址。"""
    assert contract["openapi"] == "3.0.3"
    urls = [server["url"] for server in contract["servers"]]
    assert any("hunter-edge.example.com" in url for url in urls)
    assert any("localhost:8082" in url for url in urls)


def test_all_refs_resolve(contract: dict[str, Any]) -> None:
    """所有 `$ref` 必须能在文档内解析（无悬空引用）。"""
    refs = list(iter_refs(contract))
    assert refs, "契约应包含组件引用"
    for ref in refs:
        resolve_pointer(contract, ref)


def test_operation_ids_unique(contract: dict[str, Any]) -> None:
    """`operationId` 必须唯一且为驼峰命名（供 SDK 生成 / 前端 api 层使用）。"""
    operation_ids = [operation["operationId"] for _, _, operation in iter_operations(contract)]
    assert len(operation_ids) == len(set(operation_ids))
    assert all(re.fullmatch(r"[a-z][A-Za-z0-9]*", op) for op in operation_ids)


def test_business_endpoints_match_declared_inventory(contract: dict[str, Any]) -> None:
    """业务端点集合 == `x-hunter-endpoints.endpoints`（推导清单不得随意增删）。"""
    declared = {tuple(entry.split(" ")) for entry in contract["x-hunter-endpoints"]["endpoints"]}
    actual = {
        (method, path)
        for method, path, _ in iter_operations(contract)
        if path.startswith("/api/v1/data")
    }
    assert actual == declared == EXPECTED_ENDPOINTS
    assert set(contract["x-hunter-endpoints"]["ops_endpoints"]) == {
        f"{method} {path}" for method, path in EXPECTED_OPS_ENDPOINTS
    }
    assert contract["x-hunter-endpoints"]["status"] == "pending_confirmation", (
        "端点清单来源未确认必须显式标注"
    )
    assert len(contract["x-hunter-endpoints"]["derivation"]) >= 5, "每个端点族必须登记推导依据"


def test_ops_endpoints_match_implementation(contract: dict[str, Any]) -> None:
    """运维端点必须与 app.main 实际注册路由一致（K8s 探针不可失配）。"""
    implemented = {
        (method.upper(), path)
        for method, path in iter_registered_routes(app.routes)
        if path in {ops_path for _, ops_path in EXPECTED_OPS_ENDPOINTS}
    }
    assert implemented == EXPECTED_OPS_ENDPOINTS


def test_contract_scope_has_no_other_paths(contract: dict[str, Any]) -> None:
    """契约路径只能落在网关前缀 `/api/v1/data` 或运维端点。"""
    for path in contract["paths"]:
        assert path.startswith("/api/v1/data") or path in {p for _, p in EXPECTED_OPS_ENDPOINTS}


def test_every_operation_declares_tags_and_success_response(contract: dict[str, Any]) -> None:
    """每个操作必须有 tags、summary，并声明 2xx 成功响应；运维端点标记 x-internal。"""
    for method, path, operation in iter_operations(contract):
        assert operation.get("tags"), f"{method} {path} 缺少 tags"
        assert operation.get("summary"), f"{method} {path} 缺少 summary"
        statuses = [code for code in operation["responses"] if str(code).startswith("2")]
        assert statuses, f"{method} {path} 未声明成功响应"
        if path in {p for _, p in EXPECTED_OPS_ENDPOINTS}:
            assert operation.get("x-internal") is True


# =====================================================================
# 二、遥测（5.3.3 / 5.4 节）与事件（events 表）数据结构
# =====================================================================
def test_telemetry_schemas_mirror_kafka_message(contract: dict[str, Any]) -> None:
    """查询响应 Schema 的嵌套字段必须与 Kafka 遥测 Schema（5.3.3 节）逐段一致。"""
    schemas = contract["components"]["schemas"]
    message = load_json(SCHEMA_DIR / "telemetry.schema.json")
    assert set(schemas["TelemetrySample"]["properties"]) == {"time", "vehicle_id", "seq", *TELEMETRY_SEGMENTS}
    assert set(schemas["TelemetrySample"]["required"]) == {"time", "vehicle_id"}
    assert message["properties"]["vehicle_id"]["pattern"] == (
        schemas["TelemetrySample"]["properties"]["vehicle_id"]["pattern"]
    )
    for segment in TELEMETRY_SEGMENTS:
        component = schemas[SEGMENT_SCHEMA_BY_FIELD[segment]]
        # 六段嵌套字段名与消息 Schema 完全一致（禁止自造字段）
        assert set(component["properties"]) == set(message["properties"][segment]["properties"]), (
            f"{segment} 段字段与 5.3.3 节消息结构不一致"
        )
        ref = schemas["TelemetrySample"]["properties"][segment]
        assert ref["$ref"] == f"#/components/schemas/{SEGMENT_SCHEMA_BY_FIELD[segment]}"
    # 值域约束继承消息契约（battery_soc 0-100、cpu/gpu_usage 0-100、network_rssi ≤ 0）
    chassis = schemas["TelemetryChassis"]["properties"]
    assert (chassis["battery_soc"]["minimum"], chassis["battery_soc"]["maximum"]) == (0, 100)
    system = schemas["TelemetrySystem"]["properties"]
    assert (system["cpu_usage"]["minimum"], system["cpu_usage"]["maximum"]) == (0, 100)
    assert (system["gpu_usage"]["minimum"], system["gpu_usage"]["maximum"]) == (0, 100)
    assert system["network_rssi"]["maximum"] == 0


def test_telemetry_query_contract_matches_timeseries_ddl(contract: dict[str, Any]) -> None:
    """遥测查询契约的列映射必须与 `vehicle_telemetry` DDL 列一致（含 90 天保留）。"""
    query = contract["x-hunter-telemetry-query-contract"]
    columns = ddl_columns(DDL_TIMESERIES, "data_collector.vehicle_telemetry")
    assert columns, "未能解析 vehicle_telemetry 表列"
    mapped = {query["column_mapping"]["time"], query["column_mapping"]["vehicle_id"], *query["column_mapping"]["seq"].split()}
    for segment in TELEMETRY_SEGMENTS:
        mapped.update(query["column_mapping"][segment])
    assert mapped == columns, "列映射与 DDL 不一致（新增列必须先改契约）"
    assert query["table"] == "data_collector.vehicle_telemetry"
    assert query["retention_days"] == 90
    assert "idx_vehicle_telemetry_vehicle_time" in query["index"]
    # 查询必须限定车辆与时间区间（保护 P95 ≤ 200ms）
    assert set(query["required_filters"]) == {"vehicle_id", "start_time", "end_time"}
    for name in ("VehicleIdQuery", "StartTimeQuery", "EndTimeQuery"):
        assert contract["components"]["parameters"][name]["required"] is True


def test_event_type_enum_matches_controlled_vocabulary(contract: dict[str, Any]) -> None:
    """事件类型枚举必须等于受控词表（18 种，阈值不可更改）。"""
    schemas = contract["components"]["schemas"]
    declared = set(schemas["EventType"]["enum"])
    assert declared == {event_type.value for event_type in EventType}
    assert len(declared) == 18
    assert set(schemas["EventLevel"]["enum"]) == {level.value for level in EventLevel}


def test_event_level_map_matches_orm(contract: dict[str, Any]) -> None:
    """契约中的「事件类型 → 等级」映射必须与 ORM EVENT_LEVEL_BY_TYPE 完全一致。"""
    contract_map = contract["x-hunter-event-contract"]["level_by_type"]
    orm_map = {key: value.value for key, value in EVENT_LEVEL_BY_TYPE.items()}
    assert contract_map == orm_map
    assert set(contract_map) == {event_type.value for event_type in EventType}


def test_event_item_covers_events_table_columns(contract: dict[str, Any]) -> None:
    """EventItem 必须覆盖 events 表所有列（派生字段除外），幂等键与 DDL 唯一索引一致。"""
    properties = set(contract["components"]["schemas"]["EventItem"]["properties"])
    columns = ddl_columns(DDL_EVENTS, "data_collector.events")
    derived = {"data_file_download_url"}   # 服务端即时签发的 15 分钟预签名 URL（不落库）
    assert properties - derived == columns
    assert contract["x-hunter-event-contract"]["table"] == "data_collector.events"
    assert "uq_events_vehicle_type_time" in (DDL_EVENTS.read_text(encoding="utf-8"))
    assert contract["x-hunter-event-contract"]["duplicate_guard"].startswith(
        "Kafka 消费幂等键 (vehicle_id, event_type, event_time)"
    )


def test_acknowledge_uses_server_side_identity(contract: dict[str, Any]) -> None:
    """事件确认必须使用服务端身份（X-User-Id），禁止请求体指定确认人，且幂等。"""
    ack = contract["x-hunter-event-contract"]["acknowledge"]
    assert set(ack["fields"]) == {"acknowledged", "acknowledged_by", "acknowledge_time"}
    assert ack["acknowledged_by_source"].startswith("网关注入的 X-User-Id")
    assert ack["idempotent"] is True
    operation = contract["paths"]["/api/v1/data/events/{event_id}/acknowledge"]["post"]
    assert "requestBody" not in operation, "确认操作不得接受客户端指定的确认人"
    assert {entry["$ref"] for entry in operation["parameters"]} == {
        "#/components/parameters/EventIdPath",
        "#/components/parameters/TraceRequestId",
    }


# =====================================================================
# 三、5.4 预处理 / 5.5 文件上传 / Kafka 参与度
# =====================================================================
def test_upload_flow_matches_5_5(contract: dict[str, Any]) -> None:
    """5.5 节上传流程 6 步 + 命名规范 + 分片上传 + 完整性校验齐备。"""
    flow = contract["x-hunter-file-upload-flow"]
    assert len(flow["steps"]) == EXPECTED_UPLOAD_STEPS
    joined = "\n".join(flow["steps"])
    for keyword in ("请求上传", "预签名", "直传", "上报完成", "完整性", "sensor_file"):
        assert keyword in joined, f"5.5 节上传流程缺少关键步骤: {keyword}"
    assert flow["naming"] == "{bucket}/{vehicle_id}/{date}/{data_type}/{timestamp}_{seq}.{ext}"
    assert any("禁止指定路径" in rule for rule in flow["naming_rules"]), "必须禁止客户端指定对象路径"
    assert flow["presign"]["max_part_count"] == 10000
    assert flow["integrity"]["checks"] == ["size_bytes", "md5", "sha256"]


def test_presign_expiry_and_buckets_match_minio_contract(contract: dict[str, Any]) -> None:
    """预签名时效（上传 1h / 下载 15min + Range）与 MinIO Bucket 契约一致。"""
    flow = contract["x-hunter-file-upload-flow"]
    assert flow["presign"]["upload_expires_in_seconds"] == EXPECTED_UPLOAD_EXPIRES
    assert flow["presign"]["download_expires_in_seconds"] == EXPECTED_DOWNLOAD_EXPIRES
    assert flow["presign"]["range_download"] is True
    assert set(contract["components"]["schemas"]["UploadBucket"]["enum"]) == {
        "hunter-raw-data",
        "hunter-rosbag",
        "hunter-video",
    }
    assert flow["bucket_mapping"]["rosbag"] == "hunter-rosbag"
    assert contract["x-hunter-service"]["minio_buckets"] == [
        "hunter-raw-data",
        "hunter-rosbag",
        "hunter-video",
    ]
    # 契约 ↔ Schema 一致
    presign = contract["components"]["schemas"]["FilePresignData"]["properties"]
    assert presign["expires_in"]["example"] == EXPECTED_UPLOAD_EXPIRES
    file_item = contract["components"]["schemas"]["FileObjectItem"]["properties"]
    assert file_item["expires_in"]["example"] == EXPECTED_DOWNLOAD_EXPIRES
    assert "Range" in file_item["download_url"]["description"]


def test_object_key_naming_matches_sensor_file_schema(contract: dict[str, Any]) -> None:
    """对象路径命名规范必须与 sensor_file 消息 Schema 的 object_key 约束一致。"""
    flow = contract["x-hunter-file-upload-flow"]
    schema = load_json(SCHEMA_DIR / "sensor_file.schema.json")
    pattern = schema["properties"]["object_key"]["pattern"]
    example = contract["components"]["schemas"]["FilePresignData"]["properties"]["object_key"]["example"]
    assert re.fullmatch(pattern, example), f"契约示例不满足命名规范: {example}"
    assert schema["properties"]["data_type"]["enum"] == list(
        contract["components"]["schemas"]["FileDataType"]["enum"]
    )
    assert set(flow["bucket_mapping"]) == set(schema["properties"]["data_type"]["enum"])
    assert flow["notify_topic"] == "sensor_file"
    assert flow["notify_schema"] == "contracts/kafka/schemas/sensor_file.schema.json"

def test_integrity_checks_use_predefined_error_code(contract: dict[str, Any]) -> None:
    """完整性校验失败复用预定义错误码 6001，且与 HTTP 映射一致。"""
    integrity = contract["x-hunter-file-upload-flow"]["integrity"]
    assert integrity["on_failure_code"] == ErrorCode.OTA_PACKAGE_CHECKSUM_FAILED
    assert integrity["on_failure_http_status"] == HTTP_STATUS_BY_CODE[integrity["on_failure_code"]]
    assert integrity["on_failure_http_status"] == 422
    complete = contract["components"]["schemas"]["FileCompleteRequest"]["properties"]
    for field, pattern in (("md5", integrity["md5_pattern"]), ("sha256", integrity["sha256_pattern"])):
        assert complete[field]["pattern"] == pattern
    assert (
        contract["components"]["schemas"]["FilePresignRequest"]["properties"]["sha256"]["pattern"]
        == integrity["sha256_pattern"]
    )


def test_ingest_pipeline_matches_5_4(contract: dict[str, Any]) -> None:
    """5.4 节预处理流程 6 步齐备，且路由/幂等键/延迟目标与 Kafka 契约一致。"""
    pipeline = contract["x-hunter-ingest-pipeline"]
    assert len(pipeline["steps"]) == EXPECTED_PIPELINE_STEPS
    joined = "\n".join(pipeline["steps"])
    for keyword in ("JSON 解析", "数据校验", "时间戳对齐", "数据清洗", "enrichment", "序列化"):
        assert keyword in joined, f"5.4 节流程缺少步骤: {keyword}"
    assert pipeline["trigger"]["topic_pattern"] == "hunter.*.telemetry"
    assert pipeline["trigger"]["key"] == "vehicle_id"
    assert set(pipeline["routing"]) == {"telemetry_raw", "telemetry_clean", "vehicle_telemetry"}
    assert pipeline["targets"]["ingest_latency_ms"] == 1000
    assert pipeline["targets"]["timeseries_write_points_per_sec"] == 10000
    assert pipeline["targets"]["dlq_pattern"] == "{original_topic}.dlq"
    # 幂等键必须与 consumer-groups.yaml 登记一致
    groups = {group["group_id"]: group for group in load_yaml(CONSUMER_GROUPS)["groups"]}
    for key, group_id in (
        ("telemetry", "data-collector-telemetry"),
        ("events", "data-collector-events"),
        ("health", "data-collector-health"),
        ("command_result", "data-collector-command-result"),
    ):
        assert pipeline["idempotency_keys"][key] == groups[group_id]["idempotency_key"]


def test_kafka_participation_matches_contracts(contract: dict[str, Any]) -> None:
    """消费/生产清单必须与 topics.yaml、consumer-groups.yaml 双向一致。"""
    kafka = contract["x-hunter-kafka"]
    topics = load_yaml(TOPICS_CONTRACT)
    platform = {entry["name"]: entry for entry in topics["platform_topics"]}
    groups = {group["group_id"]: group for group in load_yaml(CONSUMER_GROUPS)["groups"]}

    consumes = {entry["group_id"]: entry for entry in kafka["consumes"]}
    assert set(consumes) == {
        "data-collector-telemetry",
        "data-collector-events",
        "data-collector-health",
        "data-collector-command-result",
    }
    for group_id, entry in consumes.items():
        assert entry["enabled"] is True
        assert entry["subscribe_mode"] == "regex_pattern"
        group = groups[group_id]
        assert group["service"] == "data-collector"
        assert entry["topic_pattern"].replace("*", "{vehicle_id}") in {
            topic.replace("*", "{vehicle_id}") for topic in group["subscribes"]
        }
        assert entry["produces"] == group["produces"], f"{group_id} 生产清单与消费组契约不一致"

    produced = {entry["topic"]: entry for entry in kafka["produces"]}
    assert set(produced) == {"telemetry_raw", "telemetry_clean", "event_raw", "sensor_file"}
    for topic, entry in produced.items():
        assert platform[topic]["producer"] == "data-collector"
        assert platform[topic]["schema"] == entry["schema"].replace("contracts/kafka/", "")
        assert (SCHEMA_DIR / Path(entry["schema"]).name).is_file()
    assert produced["sensor_file"]["trigger"].startswith("POST /api/v1/data/files/complete")

    # 其他模块的 Topic（含未登记的 Carla Topic）一律不得出现在本服务的生产清单中
    not_owned = set(kafka["not_owned"]["topics"])
    assert set(produced) & not_owned == set()
    assert {entry["topic_pattern"] for entry in kafka["consumes"]} & not_owned == set()
    assert "carla.{sim_id}.sensor_data" in not_owned
    assert "Carla Topic" in kafka["not_owned"]["carla_note"]
    assert "carla.{sim_id}.vil_state" not in set(platform), "未登记 Carla Topic 不得擅自写入 topics.yaml"

# =====================================================================
# 四、错误码 / 限流 / Redis / 网关与基础设施三方一致
# =====================================================================
def test_error_codes_are_predefined(contract: dict[str, Any]) -> None:
    """契约中出现的错误码必须全部来自附录 A 预定义集合。"""
    predefined = {int(code) for code in ErrorCode}
    used: set[int] = set()
    for name, response in contract["components"]["responses"].items():
        payload = response["content"]["application/json"]
        examples = payload.get("examples")
        if examples:
            codes = [entry["value"]["code"] for entry in examples.values()]
        else:
            codes = [payload["example"]["code"]]
        for code in codes:
            assert code in predefined, f"响应 {name} 使用了未定义错误码 {code}"
            used.add(code)
    assert used == {1001, 1002, 2001, 2002, 3001, 4001, 5000, 5001, 6001}
    assert set(contract["components"]["schemas"]["ErrorCode"]["enum"]) == predefined


def test_error_status_map_matches_implementation(contract: dict[str, Any]) -> None:
    """契约的 code→HTTP 映射必须与 error_handlers.HTTP_STATUS_BY_CODE 完全一致。"""
    declared = {int(code): status for code, status in contract["x-hunter-error-status-map"]["map"].items()}
    assert declared == HTTP_STATUS_BY_CODE


def test_unified_response_shape(contract: dict[str, Any]) -> None:
    """统一响应五字段；所有成功响应 data 已定型（避免前端使用 any）。"""
    schemas = contract["components"]["schemas"]
    assert set(schemas["ApiResponse"]["required"]) == {
        "code",
        "message",
        "data",
        "request_id",
        "timestamp",
    }
    for name in (
        "TelemetryQueryResponse",
        "EventListResponse",
        "EventResponse",
        "FilePresignResponse",
        "FileCompleteResponse",
        "FileListResponse",
        "ApiResponseHealthy",
        "ApiResponseReady",
    ):
        wrapper = schemas[name]["allOf"]
        assert wrapper[0]["$ref"].endswith("/ApiResponse")
        assert wrapper[1]["required"] == ["data"]
    assert set(schemas["ReadyChecks"]["required"]) == {"database", "redis"}


def test_pagination_bounds_match_repository(contract: dict[str, Any]) -> None:
    """分页上限必须与 hunter_common BaseRepository 常量一致（保护 P95 ≤ 200ms）。"""
    page_size = contract["components"]["parameters"]["PageSizeQuery"]["schema"]
    assert page_size["default"] == DEFAULT_PAGE_SIZE
    assert page_size["maximum"] == MAX_PAGE_SIZE
    performance = contract["x-hunter-service"]["performance"]
    assert performance["list_max_page_size"] == MAX_PAGE_SIZE
    assert performance["list_default_page_size"] == DEFAULT_PAGE_SIZE
    assert performance["api_p95_ms"] == 200
    assert performance["ingest_latency_ms"] == 1000
    assert performance["timeseries_write_points_per_sec"] == 10000
    assert performance["telemetry_max_msg_per_sec_per_vehicle"] == 100
    assert performance["file_upload_mbps_per_vehicle"] == 10


def test_rate_limits_match_gateway_and_appendix_d(contract: dict[str, Any]) -> None:
    """限流阈值必须与附录 D / 网关契约一致（含 GET /api/v1/data/telemetry 20 QPS）。"""
    limits = contract["x-hunter-rate-limits"]
    gateway = load_yaml(GATEWAY_CONTRACT)["x-hunter-rate-limits"]
    for key in ("global_qps", "per_user_qps", "per_ip_qps"):
        assert limits["inherit_gateway"][key] == gateway[key]
    gateway_limits = {(entry["method"], entry["path"]): entry for entry in gateway["endpoint_limits"]}
    entry = next(item for item in limits["endpoint_limits"] if item["path"] == "/api/v1/data/telemetry")
    assert entry["method"] == "GET"
    assert entry["qps"] == 20
    assert gateway_limits[("GET", "/api/v1/data/telemetry")]["qps"] == entry["qps"], (
        "接口级限流必须与网关契约同源（附录 D）"
    )
    vehicle_limits = {item["metric"]: item["limit"] for item in limits["vehicle_limits"]}
    assert vehicle_limits == EXPECTED_VEHICLE_LIMITS
    throttles = {
        entry["metric"]: entry["limit"] for entry in load_yaml(TOPICS_CONTRACT)["throttles"]
    }
    assert throttles["telemetry_produce_rate"] == vehicle_limits["kafka_telemetry_msg_per_sec"]
    assert throttles["file_upload_bandwidth"] == vehicle_limits["file_upload_mbps"]
    assert limits["over_limit"]["http_status"] == 429
    assert limits["over_limit"]["response_code"] == ErrorCode.SERVICE_UNAVAILABLE


def test_redis_keys_match_redis_contract(contract: dict[str, Any]) -> None:
    """Redis 键必须取自系统 Redis Key 契约，且与健康数据消费职责一致。"""
    patterns = {entry["pattern"] for entry in contract["x-hunter-service"]["redis_keys"]}
    assert patterns == {"vehicle:status:{vehicle_id}", "vehicle:online:set", "rate_limit:{ip}:{api}"}
    health = next(
        entry for entry in contract["x-hunter-kafka"]["consumes"] if entry["group_id"] == "data-collector-health"
    )
    assert "vehicle:status" in health["reason"]
    groups = {group["group_id"]: group for group in load_yaml(CONSUMER_GROUPS)["groups"]}
    assert "vehicle:status" in groups["data-collector-health"]["notes"]
    assert "vehicle:online:set" in groups["data-collector-health"]["notes"]


def test_gateway_route_alignment(contract: dict[str, Any]) -> None:
    """网关路由表 / Ingress / 本服务端口三方一致（前缀 /api/v1/data 不可更改）。"""
    prefix = contract["x-hunter-service"]["gateway_prefix"]
    assert prefix == "/api/v1/data"
    gateway_routes = load_yaml(GATEWAY_CONTRACT)["x-hunter-gateway-routes"]
    route = next(entry for entry in gateway_routes["routes"] if entry["prefix"] == prefix)
    assert route["target_service"] == contract["x-hunter-service"]["name"] == "data-collector"
    assert route["target_port"] == contract["x-hunter-service"]["port"] == 8082
    assert route["auth"] == "jwt"
    assert gateway_routes["strip_prefix"] is False, "服务按全路径注册路由，网关不得剥离前缀"
    ingress_text = INGRESS.read_text(encoding="utf-8")
    assert f"path: {prefix}" in ingress_text, "Ingress 必须显式列出该前缀（指向 api-gateway:8080）"


def test_k8s_manifest_alignment(contract: dict[str, Any]) -> None:
    """K8s ConfigMap/Deployment/Service 必须与契约（端口/Bucket/Kafka/探针）一致。"""
    docs = [doc for doc in yaml.safe_load_all(K8S_MANIFEST.read_text(encoding="utf-8")) if doc]
    by_kind = {doc["kind"]: doc for doc in docs}
    service_meta = contract["x-hunter-service"]

    config = by_kind["ConfigMap"]["data"]
    assert config["SERVICE_NAME"] == service_meta["name"]
    assert int(config["API_PORT"]) == service_meta["port"]
    # 消费者组 / 生产 Topic 必须与契约 x-hunter-kafka 一致
    assert config["KAFKA_CONSUMER_GROUPS"].split(",") == [
        entry["group_id"] for entry in contract["x-hunter-kafka"]["consumes"]
    ]
    assert config["KAFKA_SUBSCRIBE_PATTERNS"].split(",") == [
        entry["topic_pattern"] for entry in contract["x-hunter-kafka"]["consumes"]
    ]
    assert config["KAFKA_PRODUCE_TOPICS"].split(",") == [
        entry["topic"] for entry in contract["x-hunter-kafka"]["produces"]
    ]
    # Bucket / 预签名时效 / 车辆限流与契约一致
    assert [
        config["MINIO_BUCKET_RAW_DATA"],
        config["MINIO_BUCKET_ROSBAG"],
        config["MINIO_BUCKET_VIDEO"],
    ] == service_meta["minio_buckets"]
    assert int(config["PRESIGNED_UPLOAD_EXPIRE_SECONDS"]) == EXPECTED_UPLOAD_EXPIRES
    assert int(config["PRESIGNED_DOWNLOAD_EXPIRE_SECONDS"]) == EXPECTED_DOWNLOAD_EXPIRES
    assert int(config["VEHICLE_TELEMETRY_MAX_MSG_PER_SEC"]) == EXPECTED_VEHICLE_LIMITS[
        "kafka_telemetry_msg_per_sec"
    ]
    assert int(config["FILE_UPLOAD_MAX_BYTES_PER_SECOND"]) == EXPECTED_VEHICLE_LIMITS[
        "file_upload_mbps"
    ] * 1024 * 1024 // 8

    deployment = by_kind["Deployment"]["spec"]
    assert deployment["replicas"] >= 2, "无状态服务多副本部署（高可用原则）"
    container = deployment["template"]["spec"]["containers"][0]
    assert container["ports"][0]["containerPort"] == service_meta["port"]
    annotations = deployment["template"]["metadata"]["annotations"]
    assert annotations["prometheus.io/port"] == str(service_meta["port"])
    assert annotations["prometheus.io/path"] == "/metrics"
    assert container["readinessProbe"]["httpGet"]["path"] == "/readyz"
    assert container["livenessProbe"]["httpGet"]["path"] == "/healthz"
    assert by_kind["Service"]["spec"]["ports"][0]["port"] == service_meta["port"]


def test_required_env_keys_are_wired(contract: dict[str, Any]) -> None:
    """契约声明的环境变量必须实际由 ConfigMap/Secret 注入（禁止硬编码，禁止未接线）。"""
    service_meta = contract["x-hunter-service"]
    service_config = next(
        doc["data"]
        for doc in yaml.safe_load_all(K8S_MANIFEST.read_text(encoding="utf-8"))
        if doc and doc.get("kind") == "ConfigMap"
    )
    common = load_yaml(K8S_COMMON_CONFIG)["data"]
    secret: dict[str, Any] = {}
    for doc in yaml.safe_load_all(K8S_SECRET.read_text(encoding="utf-8")):
        if doc:
            secret.update(doc.get("stringData") or {})
    wired = set(service_config) | set(common) | set(secret)
    derived = {entry["name"] for entry in service_meta["env_derived"]}
    assert derived <= set(service_meta["required_env"])
    assert set(service_meta["required_env"]) - derived <= wired, (
        f"以下环境变量未在 K8s 配置中注入: {sorted(set(service_meta['required_env']) - derived - wired)}"
    )
    # MinIO 凭据走 Secret（禁止出现在 ConfigMap）
    assert {"MINIO_ACCESS_KEY", "MINIO_SECRET_KEY"} <= set(secret)
    assert not {"MINIO_ACCESS_KEY", "MINIO_SECRET_KEY"} & wired - set(secret)
    # 遥测查询时间跨度上限（性能保护）必须登记并环境变量化
    assert "TELEMETRY_QUERY_MAX_RANGE_HOURS" in service_meta["required_env"]
    assert contract["x-hunter-telemetry-query-contract"]["max_range"]["env"] == (
        "TELEMETRY_QUERY_MAX_RANGE_HOURS"
    )


def test_db_tables_match_data_layer_contract(contract: dict[str, Any]) -> None:
    """契约声明的表必须与 contracts/database 中 data_collector schema 的表一致（不擅自新增）。"""
    service_meta = contract["x-hunter-service"]
    assert service_meta["db_schema"] == "data_collector"
    assert set(service_meta["db_tables"]) == {"vehicle_telemetry", "events"}
    for table, ddl in (("events", DDL_EVENTS), ("vehicle_telemetry", DDL_TIMESERIES)):
        assert ddl_columns(ddl, f"data_collector.{table}"), f"{table} 未在 DDL 中定义"
    # 元信息入库缺表必须显式登记（禁止静默新增表）
    questions = "\n".join(
        item["question"] for item in contract["x-hunter-pending-confirmation"]["items"]
    )
    assert "元信息入库" in questions


def test_pending_confirmation_items_are_structured(contract: dict[str, Any]) -> None:
    """待确认项必须结构完整、ID 唯一（供人工评审逐条闭环）。"""
    items = contract["x-hunter-pending-confirmation"]["items"]
    assert len(items) >= 10
    ids = [item["id"] for item in items]
    assert ids == sorted(ids) and len(ids) == len(set(ids))
    for item in items:
        assert item["question"] and item["contract_decision"] and item["impact"]
    text = "\n".join(
        f"{item['question']}\n{item['contract_decision']}" for item in items
    )
    for keyword in ("端点", "元信息入库", "sensor_file", "Carla", "6001"):
        assert keyword in text, f"关键待确认项缺失: {keyword}"


# =====================================================================
# 六、生成的 OpenAPI ↔ 契约 diff（实现即契约证据）
# =====================================================================
#: FastAPI 内置组件（422 校验错误体），不属于业务契约
_FASTAPI_BUILTIN_SCHEMAS = {"HTTPValidationError", "ValidationError"}
#: 契约 paths 中由共享库运维端点提供、不进入 FastAPI openapi 文档的路径。
#: /metrics 由 hunter_common.metrics 注册（test_ops_endpoints_match_implementation 已覆盖其存在性）。
_OPS_ONLY_CONTRACT_PATHS = {"/metrics"}


def _effective_properties(
    schema: dict[str, Any], components: dict[str, Any], depth: int = 0
) -> set[str]:
    """展开 OpenAPI schema 的 $ref / allOf，返回合并后的属性名集合。

    契约响应模型用 allOf 组合（如 ApiResponse + data 实体），实现侧为等价的
    内联展开，因此 diff 必须基于"有效属性集"而非原始结构。required 语义差异
    （实现侧 code/message/data 提供默认值）由统一响应格式测试单独覆盖。
    """
    if depth > 8:  # 防御循环引用
        return set()
    if "$ref" in schema:
        target = components.get(schema["$ref"].rsplit("/", 1)[-1], {})
        return _effective_properties(target, components, depth + 1)
    props: set[str] = set()
    for sub in schema.get("allOf", []):
        props |= _effective_properties(sub, components, depth + 1)
    props |= set((schema.get("properties") or {}).keys())
    return props


def test_generated_openapi_paths_equal_contract(contract: dict[str, Any]) -> None:
    """实现注册的业务端点（方法+路径）必须与契约 paths 完全一致（双向 diff 为空）。"""
    generated = {
        path: frozenset(method for method in ops if method in HTTP_METHODS)
        for path, ops in app.openapi()["paths"].items()
    }
    expected = {
        path: frozenset(method for method in ops if method in HTTP_METHODS)
        for path, ops in contract["paths"].items()
        if path not in _OPS_ONLY_CONTRACT_PATHS
    }
    assert generated == expected, (
        f"路径漂移: 仅契约有={sorted(set(expected) - set(generated))} "
        f"仅实现有={sorted(set(generated) - set(expected))}"
    )


def test_generated_schemas_share_names_with_contract(contract: dict[str, Any]) -> None:
    """生成文档中的业务 Schema 必须与契约同名（禁止实现私增/改名）。"""
    generated = set(app.openapi()["components"]["schemas"]) - _FASTAPI_BUILTIN_SCHEMAS
    contract_names = set(contract["components"]["schemas"])
    assert generated <= contract_names, f"实现私增 Schema: {sorted(generated - contract_names)}"
    # 契约中的请求/数据实体 Schema 必须全部在实现文档中同名可见（防契约漂移为内部模型）
    entity_pattern = re.compile(r"(Request|Data|Item|Sample|Part)$")
    for name in sorted(contract_names):
        if entity_pattern.search(name):
            assert name in generated, f"契约实体 Schema 未在实现中暴露: {name}"


def test_same_named_schemas_effective_fields_match_contract(contract: dict[str, Any]) -> None:
    """同名 Schema 的有效属性集必须一致（契约 allOf 组合 ↔ 实现内联展开）。"""
    generated = app.openapi()["components"]["schemas"]
    contract_schemas = contract["components"]["schemas"]
    shared = (set(generated) & set(contract_schemas)) - _FASTAPI_BUILTIN_SCHEMAS
    for name in sorted(shared):
        expected = _effective_properties(contract_schemas[name], contract_schemas)
        actual = set((generated[name].get("properties") or {}).keys())
        assert actual == expected, (
            f"Schema {name} 属性漂移: 仅契约有={sorted(expected - actual)} "
            f"仅实现有={sorted(actual - expected)}"
        )


def test_enum_schemas_match_contract_vocabulary(contract: dict[str, Any]) -> None:
    """枚举 Schema（事件类型/等级、Bucket、数据类型、上传方式）成员必须与契约一致。

    ErrorCode 不经 response_model 进入实现文档（由 test_error_codes_are_predefined
    基于 hunter_common 错误码表覆盖），故此处仅校验文档内可见枚举。
    """
    generated = app.openapi()["components"]["schemas"]
    contract_schemas = contract["components"]["schemas"]
    checked = 0
    for name, contract_schema in contract_schemas.items():
        expected = contract_schema.get("enum")
        if expected is None or name == "ErrorCode":  # ErrorCode 见 test_error_codes_are_predefined
            continue
        generated_schema = generated.get(name)
        assert generated_schema is not None, f"枚举 Schema 未在实现中暴露: {name}"
        assert sorted(generated_schema.get("enum", [])) == sorted(expected), f"枚举成员漂移: {name}"
        checked += 1
    assert checked >= 4, f"契约枚举覆盖不足: 仅校验 {checked} 个"


def test_telemetry_206_declared_on_both_sides(contract: dict[str, Any]) -> None:
    """206 截断语义必须双侧声明：契约 queryTelemetry ↔ 实现路由装饰器（待确认项 7）。"""
    contract_responses = contract["paths"]["/api/v1/data/telemetry"]["get"]["responses"]
    assert "206" in contract_responses, "契约 queryTelemetry 缺少 206 截断响应声明"
    truncated_header = contract_responses["206"]["headers"]["X-Truncated-Range"]
    assert truncated_header["schema"]["pattern"] == "^seconds=[0-9]+$"
    generated_responses = app.openapi()["paths"]["/api/v1/data/telemetry"]["get"]["responses"]
    assert "206" in generated_responses, "实现 query_telemetry 未声明 206 响应"


# =====================================================================
# 七、契约示例 ↔ 实现模型（example-based 反序列化验证）
# =====================================================================
#: 契约 Schema → 实现模型：契约最小合法实例必须被同名实现模型接受
_MODEL_BY_SCHEMA: dict[str, type[BaseModel]] = {
    "FilePresignRequest": FilePresignRequest,
    "FileCompleteRequest": FileCompleteRequest,
    "TelemetrySample": TelemetrySample,
    "TelemetryQueryData": TelemetryQueryData,
    "EventItem": EventItem,
    "EventListData": EventListData,
    "FilePresignData": FilePresignData,
    "FileCompleteData": FileCompleteData,
    "FileListData": FileListData,
    "FileObjectItem": FileObjectItem,
    "FilePresignPart": FilePresignPart,
    "CompletedPart": CompletedPart,
}
#: pattern / 语义字段的合成值特例（契约 example 缺失时使用）
_EXAMPLE_OVERRIDES: dict[str, Any] = {
    "md5": "a" * 32,
    "sha256": "a" * 64,
    "vehicle_id": "HUNTER-001",
    "time": 1724035200.123,  # 遥测采集时间（设计文档 5.3.3 示例）
    "timestamp": 1724035200.123,
    "etag": "d41d8cd98f00b204e9800998ecf8427e",
}


def _resolve_ref(schema: dict[str, Any], components: dict[str, Any]) -> dict[str, Any]:
    """解引用一层组件引用（契约内枚举/嵌套实体均为组件级引用）。"""
    if "$ref" not in schema:
        return schema
    target = components.get(schema["$ref"].rsplit("/", 1)[-1])
    assert target is not None, f"契约 $ref 无法解析: {schema['$ref']}"
    return target


def _synthetic_value(field: str, prop: dict[str, Any], components: dict[str, Any]) -> Any:
    """为契约属性合成最小合法值：example 优先，其次枚举首成员/pattern/类型推断。"""
    if "example" in prop:
        return prop["example"]
    if field in _EXAMPLE_OVERRIDES:
        return _EXAMPLE_OVERRIDES[field]
    prop = _resolve_ref(prop, components)  # $ref 枚举（bucket/data_type/event_type 等）
    if "example" in prop:
        return prop["example"]
    if "enum" in prop:
        return prop["enum"][0]
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
        return _minimal_instance_from(prop, components)  # 嵌套实体（items[] 元素）递归构造
    if value_type == "object":
        return {}
    return "hunter-edge-contract"  # string 兜底


def _minimal_instance_from(
    schema: dict[str, Any],
    components: dict[str, Any],
    model: type[BaseModel] | None = None,
) -> dict[str, Any]:
    """基于契约 Schema 合成最小合法实例（仅必填字段）。

    允许实现响应模型比契约更完整（服务端总回填，如 FileCompleteData.data_type
    契约可选、实现必填）：叠加实现必填字段参与合成，但取值仍由契约属性/example
    驱动；契约必填字段在实现侧被收紧为必填属正常（实现是契约的合法超集）。
    """
    properties = schema.get("properties") or {}
    required: set[str] = set(schema.get("required", []))
    if model is not None:
        required |= {name for name, info in model.model_fields.items() if info.is_required()}
    return {
        field: _synthetic_value(field, properties.get(field) or {}, components)
        for field in required
    }


def _minimal_instance(schema_name: str, components: dict[str, Any]) -> dict[str, Any]:
    """从契约 Schema 生成最小合法实例（叠加同名实现模型的必填字段）。"""
    return _minimal_instance_from(
        _resolve_ref({"$ref": f"#/components/schemas/{schema_name}"}, components),
        components,
        _MODEL_BY_SCHEMA[schema_name],
    )


def _assert_payload_subset(actual: Any, expected: Any, path: str) -> None:
    """递归断言合成 payload 是实现实例 dump 的子集（实现可额外填充默认字段如 None）。"""
    if isinstance(expected, dict) and isinstance(actual, dict):
        for key, sub in expected.items():
            assert key in actual, f"{path}.{key} 缺失"
            _assert_payload_subset(actual[key], sub, f"{path}.{key}")
    elif isinstance(expected, list) and isinstance(actual, list):
        assert len(actual) == len(expected), f"{path} 长度漂移: {len(actual)} != {len(expected)}"
        for index, sub in enumerate(expected):
            _assert_payload_subset(actual[index], sub, f"{path}[{index}]")
    else:
        assert actual == expected, f"{path} 取值漂移: {actual!r} != {expected!r}"


@pytest.mark.parametrize("schema_name", sorted(_MODEL_BY_SCHEMA))
def test_contract_schemas_validate_against_implementation_models(
    contract: dict[str, Any], schema_name: str
) -> None:
    """契约 Schema 的最小合法实例必须被同名实现模型接受且逐字段保值（防字段漂移）。"""
    components = contract["components"]["schemas"]
    payload = _minimal_instance(schema_name, components)
    instance = _MODEL_BY_SCHEMA[schema_name].model_validate(payload)
    dumped = instance.model_dump()  # 嵌套实体转 dict（StrEnum == str 成立）
    _assert_payload_subset(dumped, payload, schema_name)


def test_ready_checks_example_matches_readiness_contract(contract: dict[str, Any]) -> None:
    """契约 ReadyChecks 顶层示例（就绪检查项）必须与 /readyz 响应 data 键一致。"""
    example = contract["components"]["schemas"]["ReadyChecks"]["example"]
    assert set(example) == {"database", "redis"}, f"就绪检查项漂移: {sorted(example)}"

