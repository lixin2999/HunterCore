"""scene-service OpenAPI 契约测试（无需运行服务/基础设施）。

校验目标（契约是单一事实来源）：
- `contracts/openapi/scene-service.yaml` 合法：OpenAPI 3.0.3、`$ref` 可解析、`operationId` 唯一
- 业务端点集合 == 设计文档 12.2 节（不得擅自增删）；运维探针与 `app.main` 实现一致
- 4.2.2 场景配置结构 ↔ SceneConfig/SceneMeta ↔ `scene_svc.scenes` 列一致
- 4.2.1 场景分类体系 ↔ SceneType 枚举；`scenes.status` ↔ enums.md 受控词表 ↔ DDL CHECK
- 4.3 导出格式、4.4 下发流程、4.5 实车提取（Kafka 契约）与设计文档一致
- 错误码只取预定义值且与实现 `HTTP_STATUS_BY_CODE` 一致；统一响应五字段
- Redis 缓存键 / MinIO Bucket 与预签名时效 / 限流继承 / 网关路由 / K8s 清单三方一致

文件名在各服务内唯一（`test_scene_contract.py`）：monorepo 中 pytest 以 basedir 相对路径推导模块名，
各服务 `app/tests/` 均含 `__init__.py` 时同名文件会得到相同模块名（`app.tests.<basename>`）并互相覆盖，
导致模块级 fixture 丢失，故契约测试文件不使用通用名。
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml
from hunter_common.database.enums import SceneStatus
from hunter_common.database.repository import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from hunter_common.exceptions import ErrorCode

from app.core.error_handlers import HTTP_STATUS_BY_CODE
from app.main import app

ROOT = Path(__file__).resolve().parents[4]
CONTRACT_PATH = ROOT / "contracts" / "openapi" / "scene-service.yaml"
GATEWAY_CONTRACT = ROOT / "contracts" / "openapi" / "api-gateway.yaml"
TOPICS_CONTRACT = ROOT / "contracts" / "kafka" / "topics.yaml"
CONSUMER_GROUPS = ROOT / "contracts" / "kafka" / "consumer-groups.yaml"
SCHEMA_DIR = ROOT / "contracts" / "kafka" / "schemas"
K8S_SCENE = ROOT / "infra" / "k8s" / "services" / "scene-service.yaml"
INGRESS = ROOT / "infra" / "k8s" / "ingress.yaml"
DDL_SCENE = ROOT / "contracts" / "database" / "ddl" / "02_scene.sql"
ENUMS_DOC = ROOT / "contracts" / "database" / "enums.md"

#: 设计文档 12.2 节 scene-service 端点（方法大写 + 契约路径）+ 决策 G-20① 新增的仿真进度/结果查询
EXPECTED_ENDPOINTS: set[tuple[str, str]] = {
    ("GET", "/api/v1/scene"),
    ("GET", "/api/v1/scene/{scene_id}"),
    ("POST", "/api/v1/scene"),
    ("PUT", "/api/v1/scene/{scene_id}"),
    ("DELETE", "/api/v1/scene/{scene_id}"),
    ("POST", "/api/v1/scene/{scene_id}/duplicate"),
    ("POST", "/api/v1/scene/{scene_id}/publish"),
    ("POST", "/api/v1/scene/export"),
    ("POST", "/api/v1/scene/{scene_id}/run"),
    ("GET", "/api/v1/scene/templates"),
    ("GET", "/api/v1/scene/simulations/{sim_instance_id}"),
    ("GET", "/api/v1/scene/simulations/{sim_instance_id}/result"),
}

#: 运维端点（K8s 探针 / Prometheus 抓取）
EXPECTED_OPS_ENDPOINTS: set[tuple[str, str]] = {
    ("GET", "/healthz"),
    ("GET", "/readyz"),
    ("GET", "/metrics"),
}

#: 设计文档 4.2.2 节：场景配置结构顶层字段
EXPECTED_METADATA_FIELDS: set[str] = {
    "scene_id",
    "scene_name",
    "scene_type",
    "description",
    "version",
    "creator",
    "tags",
}
EXPECTED_SIMULATION_FIELDS: set[str] = {
    "map",
    "ego_vehicle",
    "weather",
    "actors",
    "events",
    "success_criteria",
    "duration",
}
EXPECTED_NESTED_FIELDS: dict[str, set[str]] = {
    "map": {"map_id", "map_type", "spawn_point"},
    "ego_vehicle": {"model", "initial_speed", "initial_steer"},
    "weather": {"cloudiness", "rain", "wetness", "fog", "wind", "sun_azimuth", "sun_altitude"},
    "actors": {"actor_id", "type", "spawn_point", "behavior"},
    "events": {"event_id", "type", "trigger", "action"},
    "success_criteria": {"max_speed_deviation", "no_collision", "min_safe_distance"},
}

#: 4.2.2 顶层字段 → 组件 Schema 名（map→SceneMap、actors→Actor、events→SceneEvent …）
NESTED_SCHEMA_BY_FIELD: dict[str, str] = {
    "map": "SceneMap",
    "ego_vehicle": "EgoVehicle",
    "weather": "Weather",
    "actors": "Actor",
    "events": "SceneEvent",
    "success_criteria": "SuccessCriteria",
}

#: 设计文档 4.2.1 节分类体系（分类 → 场景数量）
EXPECTED_CATEGORY_SIZES: dict[str, int] = {
    "basic": 4,
    "interactive": 4,
    "environment": 4,
    "corner_case": 4,
    "custom": 1,
    "real_vehicle": 1,
}

#: 设计文档 4.4 节下发流程步骤数（7 步）
EXPECTED_FLOW_STEPS = 7

#: 设计文档 4.5 节实车场景提取：触发事件 + 截取窗口（前后各 10 秒）
EXPECTED_EXTRACTION_TRIGGERS: set[tuple[str, str]] = {
    ("harsh_braking", "warning"),
    ("collision_warning", "critical"),
    ("manual_takeover", "info"),
}
EXPECTED_EXTRACTION_WINDOW = 10

HTTP_METHODS: tuple[str, ...] = ("get", "post", "put", "patch", "delete", "head", "options")


@pytest.fixture(scope="module")
def contract() -> dict[str, Any]:
    """加载 scene-service 契约（YAML）。"""
    loaded = yaml.safe_load(CONTRACT_PATH.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def load_yaml(path: Path) -> dict[str, Any]:
    """加载 YAML 契约文件。"""
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
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
    """递归展开 FastAPI 已注册路由，返回 (method, path)。

    兼容新版 Starlette：`include_router()` 可能保留为聚合节点（`_IncludedRouter`，仅暴露
    `original_router`）而不展开，因此遇到聚合节点必须下钻，否则会漏判探针端点。
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
            yield method, path


# =====================================================================
# 一、契约自身合法性与端点完整性
# =====================================================================
def test_contract_is_openapi_303(contract: dict[str, Any]) -> None:
    """契约必须为 OpenAPI 3.0.3，且声明网关地址与本地直连地址。"""
    assert contract["openapi"] == "3.0.3"
    assert contract["info"]["title"] == "HunterCore - scene-service"
    urls = {server["url"] for server in contract["servers"]}
    assert "http://localhost:8081" in urls
    assert any("hunter-core.example.com" in url for url in urls), "必须声明经网关的生产地址"


def test_all_refs_resolve(contract: dict[str, Any]) -> None:
    """所有 `$ref` 必须可解析（避免悬空引用导致前后端生成失败）。"""
    refs = list(iter_refs(contract))
    assert refs, "契约应使用 $ref 复用组件"
    for ref in refs:
        assert resolve_pointer(contract, ref) is not None


def test_operation_ids_unique(contract: dict[str, Any]) -> None:
    """`operationId` 必须唯一（前端 SDK/TS 类型生成依赖）。"""
    operation_ids = [operation["operationId"] for _, _, operation in iter_operations(contract)]
    assert len(operation_ids) == len(set(operation_ids))
    assert all(re.fullmatch(r"[a-z][A-Za-z0-9]*", oid) for oid in operation_ids)


def test_every_operation_declares_tags_and_success_response(contract: dict[str, Any]) -> None:
    """每个操作必须有 tags、summary 与至少一个 2xx 响应。"""
    for method, path, operation in iter_operations(contract):
        assert operation.get("tags"), f"{method} {path} 缺少 tags"
        assert operation.get("summary"), f"{method} {path} 缺少 summary"
        success = [code for code in operation["responses"] if code.startswith("2")]
        assert success, f"{method} {path} 缺少成功响应"


def test_business_endpoints_match_design_doc_12_2(contract: dict[str, Any]) -> None:
    """业务端点必须与设计文档 12.2 节逐条一致（G-20① 决策新增的仿真查询端点除外）。"""
    declared = {
        (method, path)
        for method, path, _ in iter_operations(contract)
        if path.startswith("/api/v1/scene")
    }
    assert declared == EXPECTED_ENDPOINTS
    documented = set(contract["x-hunter-endpoints"]["endpoints"])
    assert documented == {f"{method} {path}" for method, path in EXPECTED_ENDPOINTS}


def test_ops_endpoints_match_implementation(contract: dict[str, Any]) -> None:
    """运维端点必须与 FastAPI 应用实际注册的路由一致（探针不可偏离运维配置）。"""
    declared = {(method, path) for method, path, _ in iter_operations(contract) if not path.startswith("/api/v1/")}
    assert declared == EXPECTED_OPS_ENDPOINTS
    implemented = {
        (method, path)
        for method, path in iter_registered_routes(app.routes)
        if path in {ops_path for _, ops_path in EXPECTED_OPS_ENDPOINTS}
    }
    assert implemented == EXPECTED_OPS_ENDPOINTS


def test_contract_scope_has_no_other_paths(contract: dict[str, Any]) -> None:
    """契约路径只能落在网关前缀 `/api/v1/scene` 或运维端点（幂等、无越界端点）。"""
    for path in contract["paths"]:
        assert path.startswith("/api/v1/scene") or path in {p for _, p in EXPECTED_OPS_ENDPOINTS}



# =====================================================================
# 二、场景数据结构（4.2.2 节）与分类体系（4.2.1 节）
# =====================================================================
def _ddl_scene_columns() -> set[str]:
    """解析 contracts/database/ddl/02_scene.sql 中 scenes 表的列名。"""
    text = DDL_SCENE.read_text(encoding="utf-8")
    body = text.split("CREATE TABLE IF NOT EXISTS scene_svc.scenes (", 1)[1].split("\n);", 1)[0]
    return {match.group(1) for match in re.finditer(r"^\s{4}(\w+)\s+", body, re.MULTILINE)}


def test_scene_config_structure_matches_4_2_2(contract: dict[str, Any]) -> None:
    """场景配置结构必须与 4.2.2 节完全一致（元信息 7 字段 + 仿真配置 7 字段）。"""
    schemas = contract["components"]["schemas"]
    assert set(schemas["SceneConfig"]["required"]) == EXPECTED_SIMULATION_FIELDS
    assert set(schemas["SceneConfig"]["properties"]) == EXPECTED_SIMULATION_FIELDS
    server_managed = {"status", "create_time", "update_time"}
    assert EXPECTED_METADATA_FIELDS <= set(schemas["SceneMeta"]["required"]), "4.2.2 元信息字段必须全部必填"
    assert set(schemas["SceneMeta"]["required"]) == EXPECTED_METADATA_FIELDS | server_managed
    assert set(schemas["SceneMeta"]["properties"]) == EXPECTED_METADATA_FIELDS | server_managed

    for field, schema_name in NESTED_SCHEMA_BY_FIELD.items():
        schema = schemas[schema_name]
        fields = EXPECTED_NESTED_FIELDS[field]
        assert set(schema["properties"]) == fields, f"{field} 字段与 4.2.2 节不一致"
        assert set(schema["required"]) == fields, f"{field} 必填字段缺失"
        # SceneConfig 中该字段必须引用同名组件（数组字段取其 items）
        reference = schemas["SceneConfig"]["properties"][field]
        if "items" in reference:
            reference = reference["items"]
        assert reference["$ref"] == f"#/components/schemas/{schema_name}", f"{field} 未复用组件 {schema_name}"

    declared = contract["x-hunter-scene-config-contract"]
    assert set(declared["metadata_fields"]) == EXPECTED_METADATA_FIELDS
    assert set(declared["simulation_fields"]) == EXPECTED_SIMULATION_FIELDS
    for name, fields in EXPECTED_NESTED_FIELDS.items():
        assert set(declared["nested_fields"][name]) == fields
    assert declared["required_all"] is True


def test_scene_object_composes_meta_and_config(contract: dict[str, Any]) -> None:
    """Scene = SceneMeta + config（4.2.2 完整对象）；SceneConfig 落库 config_json。"""
    scene = contract["components"]["schemas"]["Scene"]
    parts = scene["allOf"]
    assert parts[0]["$ref"].endswith("/SceneMeta")
    extra = parts[1]
    assert set(extra["properties"]) == {"config"}
    assert extra["properties"]["config"]["$ref"].endswith("/SceneConfig")
    assert contract["x-hunter-scene-config-contract"]["storage"]["simulation_fields"].startswith(
        "scene_svc.scenes.config_json"
    )


def test_scene_meta_fields_cover_db_columns(contract: dict[str, Any]) -> None:
    """元信息字段必须一一对应 scene_svc.scenes 列（config_json / deleted_at 除外）。"""
    columns = _ddl_scene_columns()
    assert columns, "未能解析 scenes 表列"
    meta_fields = set(contract["x-hunter-scene-config-contract"]["metadata_fields"])
    assert columns - meta_fields == {"config_json", "deleted_at", "create_time", "update_time", "status"}


def test_scene_type_enum_matches_classification(contract: dict[str, Any]) -> None:
    """SceneType 枚举必须等于 4.2.1 分类体系的类型并集（含 4.5 实车回放）。"""
    scene_types = set(contract["components"]["schemas"]["SceneType"]["enum"])
    classification = contract["x-hunter-scene-types"]
    assert classification["status"] == "pending_confirmation", "⚠ 编码值未经人工确认，必须标记"
    grouped: set[str] = set()
    for category in classification["categories"]:
        assert len(category["types"]) == EXPECTED_CATEGORY_SIZES[category["value"]]
        assert set(category["type_labels"]) == set(category["types"]), "每个类型必须有中文标签"
        assert not grouped & set(category["types"]), "类型不得跨分类重复"
        grouped |= set(category["types"])
    assert grouped == scene_types
    assert "real_vehicle_replay" in scene_types, "4.5 节实车回放类型缺失"
    # 模板查询接口的分类枚举必须与分类体系一致
    template_params = contract["paths"]["/api/v1/scene/templates"]["get"]["parameters"]
    category_enum = next(p for p in template_params if p["name"] == "category")["schema"]["enum"]
    all_categories = {category["value"] for category in classification["categories"]}
    # 实车回放（4.5 节自动提取产物）不存在预置模板，故不在模板分类枚举中
    assert set(category_enum) == all_categories - {"real_vehicle"}


def test_scene_status_enum_matches_controlled_vocabulary(contract: dict[str, Any]) -> None:
    """scenes.status 必须与 enums.md 受控词表、ORM 枚举、DDL CHECK 四处一致。"""
    values = set(contract["components"]["schemas"]["SceneStatus"]["enum"])
    assert values == {status.value for status in SceneStatus}
    ddl = DDL_SCENE.read_text(encoding="utf-8")
    check = re.search(r"status IN \(([^)]*)\)", ddl)
    assert check is not None
    assert set(re.findall(r"'([^']*)'", check.group(1))) == values
    enums_doc = ENUMS_DOC.read_text(encoding="utf-8")
    for value in values:
        assert f"`{value}`" in enums_doc, f"{value} 未登记于 contracts/database/enums.md"



def test_lifecycle_rules_declared(contract: dict[str, Any]) -> None:
    """状态机规则：仅 draft 可编辑；published 可下发；发布仅 draft→published。"""
    lifecycle = contract["x-hunter-lifecycle"]
    assert lifecycle["states"] == [status.value for status in SceneStatus]
    assert lifecycle["editable_states"] == ["draft"]
    assert lifecycle["deployable_states"] == ["published"]
    assert lifecycle["deletable_states"] == ["draft", "archived"]
    assert "published" not in lifecycle["deletable_states"], "published 场景须先归档"
    publish = next(t for t in lifecycle["transitions"] if t["to"] == "published")
    assert publish["from"] == "draft"
    assert publish["endpoint"] == "POST /api/v1/scene/{scene_id}/publish"
    archived = next(t for t in lifecycle["transitions"] if t["to"] == "archived")
    assert archived["endpoint"] is None, "12.2 节未定义归档端点，禁止擅自新增"


def test_update_and_delete_state_conflict_documented(contract: dict[str, Any]) -> None:
    """PUT/DELETE 必须声明 3003 状态冲突响应（仅 draft 可编辑 / published 不可删）。"""
    for method, path in (("put", "/api/v1/scene/{scene_id}"), ("delete", "/api/v1/scene/{scene_id}")):
        responses = contract["paths"][path][method]["responses"]
        assert "409" in responses, f"{method.upper()} {path} 缺少 409（code=3003）"
        ref = responses["409"]["$ref"]
        assert resolve_pointer(contract, ref)["description"].startswith("资源状态冲突")
    assert "仅 `draft` 状态可编辑" in contract["paths"]["/api/v1/scene/{scene_id}"]["put"]["description"]


# =====================================================================
# 三、导出（4.3 节）/ 仿真下发（4.4 节）/ 实车提取（4.5 节）
# =====================================================================
def test_export_formats_match_4_3(contract: dict[str, Any]) -> None:
    """导出格式必须为 Carla ScenarioRunner XML + OpenSCENARIO 1.2。"""
    schemas = contract["components"]["schemas"]
    assert set(schemas["SceneExportFormat"]["enum"]) == {
        "carla_scenariorunner_xml",
        "openscenario_1_2",
    }
    extension = {fmt["value"]: fmt["extension"] for fmt in contract["x-hunter-export"]["formats"]}
    assert extension == {"carla_scenariorunner_xml": ".xml", "openscenario_1_2": ".xosc"}


def test_export_uses_scene_assets_bucket_with_15min_presign(contract: dict[str, Any]) -> None:
    """导出产物落 MinIO hunter-scene-assets（永久），下载预签名 15 分钟、支持 Range。"""
    export = contract["x-hunter-export"]
    assert export["bucket"] == contract["x-hunter-service"]["minio_buckets"][0] == "hunter-scene-assets"
    assert export["presign_expires_in_seconds"] == 900
    assert export["range_download"] is True
    data = contract["components"]["schemas"]["SceneExportData"]
    assert {"download_url", "expires_in", "object_key", "sha256"} <= set(data["properties"])
    assert contract["components"]["schemas"]["SceneExportData"]["properties"]["sha256"]["pattern"] == (
        "^[0-9a-f]{64}$"
    )


def test_simulation_flow_matches_4_4(contract: dict[str, Any]) -> None:
    """下发流程 7 步 + sim_instance_id + running 状态；Carla 地址必须环境变量化。"""
    flow = contract["x-hunter-simulation-flow"]
    assert len(flow["steps"]) == EXPECTED_FLOW_STEPS
    joined = "\n".join(flow["steps"])
    for keyword in ("校验场景配置完整性", "Carla 管理 API", "sim_instance_id", "回收资源"):
        assert keyword in joined, f"4.4 节流程缺少关键步骤: {keyword}"
    assert "running" in flow["status_values"]
    assert set(flow["status_values"]) == set(contract["components"]["schemas"]["SimulationStatus"]["enum"])
    assert flow["entrypoint_env"] in contract["x-hunter-service"]["required_env"]
    assert flow["implementations_pending"], "进度监控/结果保存端点缺口必须显式登记"

    run_data = contract["components"]["schemas"]["SceneRunData"]
    assert set(run_data["required"]) == {"sim_instance_id", "scene_id", "status", "started_at"}
    assert contract["paths"]["/api/v1/scene/{scene_id}/run"]["post"]["operationId"] == "runScene"


def test_real_vehicle_extraction_matches_4_5(contract: dict[str, Any]) -> None:
    """实车提取：触发事件、前后各 10 秒窗口、三类提取内容、输出 real_vehicle_replay。"""
    extraction = contract["x-hunter-real-vehicle-extraction"]
    triggers = {(t["event_type"], t["event_level"]) for t in extraction["triggers"]}
    assert triggers == EXPECTED_EXTRACTION_TRIGGERS
    assert extraction["window"] == {
        "pre_seconds": EXPECTED_EXTRACTION_WINDOW,
        "post_seconds": EXPECTED_EXTRACTION_WINDOW,
    }
    assert set(extraction["extracted"]) == {"ego_trajectory", "object_trajectories", "environment"}
    assert extraction["output"]["scene_type"] in contract["components"]["schemas"]["SceneType"]["enum"]
    assert extraction["output"]["status"] == "draft"
    assert extraction["consume"]["group_id"] == "scene-service-analytics-result"

    # 与 Kafka 契约一致：触发事件类型 ⊆ 受控词表，截取窗口固定 10/10
    schema = yaml.safe_load((SCHEMA_DIR / "analytics_result.schema.json").read_text(encoding="utf-8"))
    trigger_enum = set(schema["properties"]["trigger_event_type"]["enum"])
    assert {event_type for event_type, _ in triggers} <= trigger_enum
    assert schema["properties"]["clip"]["properties"]["pre_seconds"]["enum"] == [EXPECTED_EXTRACTION_WINDOW]
    assert schema["properties"]["clip"]["properties"]["post_seconds"]["enum"] == [EXPECTED_EXTRACTION_WINDOW]
    assert set(schema["properties"]["environment"]["properties"]) >= {"weather", "road_type", "speed_limit"}



# =====================================================================
# 四、Kafka 参与度 / 错误码 / 限流 / 基础设施三方一致
# =====================================================================
def test_kafka_participation_is_contract_compliant(contract: dict[str, Any]) -> None:
    """scene-service 不生产 Kafka 消息；仅消费 analytics_result（消费组已登记、Schema 存在）。"""
    kafka = contract["x-hunter-kafka"]
    assert kafka["produces"] == [], "契约禁止新增 Kafka 生产方"
    enabled = [entry for entry in kafka["consumes"] if entry["enabled"]]
    assert [entry["topic"] for entry in enabled] == ["analytics_result"]

    topics = load_yaml(TOPICS_CONTRACT)
    platform = {entry["name"]: entry for entry in topics["platform_topics"]}
    schema_ref = enabled[0]["schema"].replace("contracts/kafka/", "")
    assert platform["analytics_result"]["schema"] == schema_ref
    assert (SCHEMA_DIR / Path(schema_ref).name).is_file()
    scene_producers = [
        name for name, entry in platform.items() if "scene-service" in str(entry.get("producer", ""))
    ]
    assert scene_producers == [], "topics.yaml 不得把 scene-service 登记为生产方"

    groups = {group["group_id"]: group for group in load_yaml(CONSUMER_GROUPS)["groups"]}
    group = groups[enabled[0]["group_id"]]
    assert group["service"] == "scene-service"
    assert group["subscribes"] == ["analytics_result"]
    assert group["produces"] == []
    assert group["idempotency_key"] == contract["x-hunter-real-vehicle-extraction"]["idempotency_key"]


def test_error_codes_are_predefined(contract: dict[str, Any]) -> None:
    """契约中出现的错误码必须全部来自附录 A 预定义集合。"""
    predefined = {int(code) for code in ErrorCode}
    used: set[int] = set()
    responses = contract["components"]["responses"]
    for name, response in responses.items():
        example = response["content"]["application/json"]["example"]
        used.add(example["code"])
        assert example["code"] in predefined, f"{name} 使用了未定义错误码"
    assert used == {1001, 1002, 1003, 2001, 2002, 3001, 3002, 3003, 5000, 5001}
    assert set(contract["components"]["schemas"]["ErrorCode"]["enum"]) == predefined


def test_error_status_map_matches_implementation(contract: dict[str, Any]) -> None:
    """契约的 code→HTTP 映射必须与 error_handlers.HTTP_STATUS_BY_CODE 完全一致。"""
    declared = {int(code): status for code, status in contract["x-hunter-error-status-map"]["map"].items()}
    assert declared == HTTP_STATUS_BY_CODE


def test_unified_response_shape(contract: dict[str, Any]) -> None:
    """统一响应五字段；所有成功响应 data 已定型（避免前端使用 any）。"""
    base = contract["components"]["schemas"]["ApiResponse"]
    assert set(base["required"]) == {"code", "message", "data", "request_id", "timestamp"}
    for name in (
        "SceneResponse",
        "SceneListResponse",
        "SceneTemplateListResponse",
        "SceneDeleteResponse",
        "SceneExportResponse",
        "SceneRunResponse",
    ):
        wrapper = contract["components"]["schemas"][name]["allOf"]
        assert wrapper[0]["$ref"].endswith("/ApiResponse")
        assert wrapper[1]["required"] == ["data"]
    assert "data" in contract["components"]["schemas"]["ReadyResponse"]["required"]


def test_pagination_bounds_match_repository(contract: dict[str, Any]) -> None:
    """分页上限必须与 hunter_common BaseRepository 常量一致（保护 P95 ≤ 200ms）。"""
    page_size = contract["components"]["parameters"]["PageSizeQuery"]["schema"]
    assert page_size["default"] == DEFAULT_PAGE_SIZE
    assert page_size["maximum"] == MAX_PAGE_SIZE
    assert contract["x-hunter-service"]["performance"]["list_max_page_size"] == MAX_PAGE_SIZE
    assert contract["x-hunter-service"]["performance"]["list_default_page_size"] == DEFAULT_PAGE_SIZE


def test_rate_limits_inherit_gateway(contract: dict[str, Any]) -> None:
    """scene 域无专属限流（附录 D 未列出），必须显式声明继承网关五级限流。"""
    limits = contract["x-hunter-rate-limits"]
    assert limits["endpoint_limits"] == []
    gateway = load_yaml(GATEWAY_CONTRACT)["x-hunter-rate-limits"]
    for key in ("global_qps", "per_user_qps", "per_ip_qps"):
        assert limits["inherit_gateway"][key] == gateway[key]
    assert not [entry for entry in gateway["endpoint_limits"] if "scene" in str(entry.get("path", ""))]


def test_redis_cache_key_matches_contract(contract: dict[str, Any]) -> None:
    """场景详情缓存键必须与 Redis Key 契约一致（cache:scene:{scene_id}，TTL 1 小时）。"""
    cache = contract["x-hunter-service"]["redis_keys"][0]
    assert cache["pattern"] == "cache:scene:{scene_id}"
    assert cache["ttl_seconds"] == 3600
    get_scene = contract["paths"]["/api/v1/scene/{scene_id}"]["get"]
    assert "cache:scene:{scene_id}" in get_scene["description"]



def test_gateway_route_alignment(contract: dict[str, Any]) -> None:
    """网关路由表 / Ingress / 本服务端口三方一致（前缀 /api/v1/scene 不可更改）。"""
    prefix = contract["x-hunter-service"]["gateway_prefix"]
    assert prefix == "/api/v1/scene"
    gateway_routes = load_yaml(GATEWAY_CONTRACT)["x-hunter-gateway-routes"]
    route = next(entry for entry in gateway_routes["routes"] if entry["prefix"] == prefix)
    assert route["target_service"] == contract["x-hunter-service"]["name"] == "scene-service"
    assert route["target_port"] == contract["x-hunter-service"]["port"] == 8081
    assert route["auth"] == "jwt", "场景端点必须经网关 JWT 鉴权"
    assert gateway_routes["strip_prefix"] is False, "服务按全路径注册路由，网关不得剥离前缀"

    ingress_text = INGRESS.read_text(encoding="utf-8")
    assert f"path: {prefix}" in ingress_text, "Ingress 必须显式列出该前缀（指向 api-gateway:8080）"
    assert "api-gateway" in ingress_text


def test_k8s_manifest_alignment(contract: dict[str, Any]) -> None:
    """K8s ConfigMap/Deployment/Service 必须与契约（端口/Bucket/探针）一致。"""
    docs = [doc for doc in yaml.safe_load_all(K8S_SCENE.read_text(encoding="utf-8")) if doc]
    by_kind = {doc["kind"]: doc for doc in docs}

    config = by_kind["ConfigMap"]["data"]
    assert config["SERVICE_NAME"] == contract["x-hunter-service"]["name"]
    assert int(config["API_PORT"]) == contract["x-hunter-service"]["port"]
    assert config["MINIO_BUCKET_SCENE_ASSETS"] == contract["x-hunter-service"]["minio_buckets"][0]

    deployment = by_kind["Deployment"]["spec"]
    assert deployment["replicas"] >= 2, "无状态服务多副本部署（高可用原则）"
    container = deployment["template"]["spec"]["containers"][0]
    assert container["ports"][0]["containerPort"] == contract["x-hunter-service"]["port"]
    annotations = deployment["template"]["metadata"]["annotations"]
    assert annotations["prometheus.io/port"] == str(contract["x-hunter-service"]["port"])
    assert annotations["prometheus.io/path"] == "/metrics"
    assert container["readinessProbe"]["httpGet"]["path"] == "/readyz"
    assert container["livenessProbe"]["httpGet"]["path"] == "/healthz"

    service_port = by_kind["Service"]["spec"]["ports"][0]["port"]
    assert service_port == contract["x-hunter-service"]["port"] == 8081


def test_required_env_keys_are_declared(contract: dict[str, Any]) -> None:
    """Carla 等外部依赖地址必须从环境变量读取（禁止硬编码），且键名在契约中登记。"""
    required = set(contract["x-hunter-service"]["required_env"])
    assert {"SERVICE_NAME", "API_PORT", "DATABASE_URL", "REDIS_URL", "MINIO_BUCKET_SCENE_ASSETS"} <= required
    assert "CARLA_MANAGEMENT_ENDPOINT" in required
    pending_ids = {item["id"] for item in contract["x-hunter-pending-confirmation"]["items"]}
    assert any(
        "CARLA_MANAGEMENT_ENDPOINT" in item["question"] or "CARLA_MANAGEMENT_ENDPOINT" in item["contract_decision"]
        for item in contract["x-hunter-pending-confirmation"]["items"]
        if item["id"] in pending_ids
    ), "Carla 配置键名待确认项必须登记"


def test_pending_confirmation_items_are_structured(contract: dict[str, Any]) -> None:
    """待确认项必须结构完整、ID 唯一（供人工评审逐条闭环）。"""
    pending = contract["x-hunter-pending-confirmation"]
    items = pending["items"]
    assert len(items) >= 10
    ids = [item["id"] for item in items]
    assert ids == sorted(ids) and len(ids) == len(set(ids))
    for item in items:
        assert item["question"] and item["contract_decision"] and item["impact"]
    questions = "\n".join(item["question"] for item in items)
    for keyword in ("scene_type", "端点", "archived", "导出", "analytics_result"):
        assert keyword in questions, f"关键待确认项缺失: {keyword}"

