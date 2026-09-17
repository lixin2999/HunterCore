"""L5 测试层契约加载器：所有测试常量的唯一来源（contracts/）。

禁止在测试代码中硬编码 Topic 名、字段名、分区数、Key 模式、Bucket 名与阈值——
一律经本模块从契约文件读取（开发规则：契约先行 / 禁止硬编码字段）。

契约位置：
- ``contracts/kafka/topics.yaml``、``consumer-groups.yaml``、``schemas/*.schema.json``
- ``contracts/database/ddl/*.sql``、``redis-keys.yaml``、``object-storage.yaml``
- ``contracts/openapi/*.yaml``
"""
from __future__ import annotations

import copy
import json
import re
from functools import cache
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft7Validator

#: 仓库根目录（tests/support/contracts.py → parents[2]）
ROOT = Path(__file__).resolve().parents[2]
CONTRACTS_DIR = ROOT / "contracts"
KAFKA_DIR = CONTRACTS_DIR / "kafka"
SCHEMA_DIR = KAFKA_DIR / "schemas"
DB_DIR = CONTRACTS_DIR / "database"
DDL_DIR = DB_DIR / "ddl"
OPENAPI_DIR = CONTRACTS_DIR / "openapi"
SERVICES_DIR = ROOT / "services"

#: 六个微服务（模块划分表；不可新增/合并）
SERVICE_NAMES: tuple[str, ...] = (
    "api-gateway",
    "scene-service",
    "data-collector",
    "data-analytics",
    "ota-service",
    "remote-control",
)

#: 平台内部 Topic（6 个）
PLATFORM_TOPIC_NAMES: tuple[str, ...] = (
    "telemetry_raw",
    "telemetry_clean",
    "event_raw",
    "sensor_file",
    "analytics_result",
    "alert_event",
)

_VEHICLE_TOPIC_RE = re.compile(r"^hunter\.\{vehicle_id\}\.(\w+)$")
_VEHICLE_TOPIC_RUNTIME_RE = re.compile(r"^hunter\.([^.]+)\.(\w+)$")
_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-z_]+)\.([a-z_]+)", re.IGNORECASE
)
_GATEWAY_PREFIX_RE = re.compile(r"^(/api/v1/[a-z-]+|/ws/[a-z-]+)")


# ---------------------------------------------------------------- 通用加载

@cache
def load_yaml(path: str | Path) -> dict[str, Any]:
    """加载 YAML 契约文件（带缓存）。"""
    return yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}


@cache
def load_json(path: str | Path) -> dict[str, Any]:
    """加载 JSON 契约文件（带缓存）。"""
    return json.loads(Path(path).read_text(encoding="utf-8"))


# ---------------------------------------------------------------- Kafka

def kafka_topics_doc() -> dict[str, Any]:
    """topics.yaml 全文。"""
    return load_yaml(KAFKA_DIR / "topics.yaml")


def platform_topics() -> dict[str, dict[str, Any]]:
    """平台内部 Topic：{name: spec}。"""
    return {entry["name"]: entry for entry in kafka_topics_doc()["platform_topics"]}


def vehicle_topics() -> dict[str, dict[str, Any]]:
    """车端 Topic 模板：{type: spec}（契约中 name 形如 hunter.{vehicle_id}.telemetry）。"""
    result: dict[str, dict[str, Any]] = {}
    for entry in kafka_topics_doc()["vehicle_topics"]:
        match = _VEHICLE_TOPIC_RE.match(str(entry["name"]))
        if match is None:
            # 非模板条目（如 hunter.broadcast.command 广播 Topic）不计入车端单车主线
            continue
        result[match.group(1)] = entry
    return result


def vehicle_topic(vehicle_id: str, topic_type: str) -> str:
    """拼装车端 Topic 名（唯一允许的拼装方式：hunter.{vehicle_id}.<type>）。"""
    assert topic_type in vehicle_topics(), f"未登记的 Topic 类型: {topic_type}"
    return f"hunter.{vehicle_id}.{topic_type}"


def broadcast_topic() -> str:
    """广播指令 Topic（命名规范 naming.broadcast_topic）。"""
    return str(kafka_topics_doc()["naming"]["broadcast_topic"])


def dlq_topic(topic: str) -> str:
    """DLQ Topic 名（命名规范 naming.dlq_pattern）。"""
    return str(kafka_topics_doc()["naming"]["dlq_pattern"]).format(original_topic=topic)


def topic_spec(topic: str) -> dict[str, Any]:
    """按 Topic 名解析规格（分区数 / acks / 保留时间 / key / schema）。"""
    if topic in platform_topics():
        return platform_topics()[topic]
    match = _VEHICLE_TOPIC_RUNTIME_RE.match(topic)
    if match and match.group(2) in vehicle_topics():
        return vehicle_topics()[match.group(2)]
    raise KeyError(f"Topic 未在 contracts/kafka/topics.yaml 登记: {topic}")


def topic_partitions(topic: str) -> int:
    """Topic 分区数（契约不可更改）。"""
    return int(topic_spec(topic)["partitions"])


def topic_retention_ms(topic: str) -> int:
    """Topic 保留时间（毫秒）。"""
    return int(topic_spec(topic)["retention_ms"])


def topic_acks(topic: str) -> str:
    """Topic acks 配置（字符串形式，如 "1" / "all"）。"""
    return str(topic_spec(topic)["acks"])


def render_topic(template: str, vehicle_id: str) -> str:
    """把 Topic 模板中的 ``{vehicle_id}`` 占位符实例化（Topic 名不可自行改写）。"""
    return template.replace("{vehicle_id}", vehicle_id)


def topic_schema_name(topic: str) -> str:
    """Topic 绑定的 JSON Schema 名（去目录、去扩展名）。"""
    return Path(str(topic_spec(topic)["schema"])).name.removesuffix(".schema.json")


def consumer_groups() -> dict[str, dict[str, Any]]:
    """消费组契约：{group_id: spec}。"""
    doc = load_yaml(KAFKA_DIR / "consumer-groups.yaml")
    return {entry["group_id"]: entry for entry in doc["groups"]}


def kafka_schema(name: str) -> dict[str, Any]:
    """加载 JSON Schema；``name`` 可传 ``telemetry`` 或 ``telemetry.schema.json``。"""
    file_name = name if name.endswith(".json") else f"{name}.schema.json"
    path = SCHEMA_DIR / file_name
    assert path.is_file(), f"Schema 不存在: {path}"
    return load_json(path)


def schema_example(name: str, index: int = 0) -> dict[str, Any]:
    """取 Schema 中登记的 examples[index]（深拷贝，可安全改动）。"""
    examples = kafka_schema(name).get("examples") or []
    assert examples, f"Schema {name} 未登记 examples（测试消息工厂依赖契约示例）"
    return copy.deepcopy(examples[index])


def validate_message(name: str, payload: dict[str, Any]) -> list[str]:
    """按 JSON Schema 校验消息，返回错误列表（空列表 = 通过）。"""
    validator = Draft7Validator(kafka_schema(name))
    errors = sorted(validator.iter_errors(payload), key=lambda err: str(err.absolute_path))
    return [f"{'/'.join(str(part) for part in err.absolute_path)}: {err.message}" for err in errors]


def assert_valid_message(name: str, payload: dict[str, Any]) -> None:
    """校验失败即断言失败（用于「测试构造的消息必须符合契约」）。"""
    errors = validate_message(name, payload)
    assert not errors, f"消息不符合 contracts/kafka/schemas/{name}.schema.json: {errors}"


def platform_topic_throttle(metric: str) -> dict[str, Any]:
    """Topic 级限流配置（附录 D：telemetry_produce_rate / file_upload_bandwidth）。"""
    for entry in kafka_topics_doc()["throttles"]:
        if entry["metric"] == metric:
            return entry
    raise KeyError(f"未登记的限流指标: {metric}")


# ---------------------------------------------------------------- 数据库

def ddl_files() -> list[Path]:
    """DDL 契约文件（按文件名顺序执行：00 → 05）。"""
    return sorted(path for path in DDL_DIR.glob("*.sql"))


def ddl_sql() -> str:
    """全部 DDL 拼装（幂等，可重复执行）。"""
    return "\n".join(path.read_text(encoding="utf-8") for path in ddl_files())


def ddl_tables() -> list[tuple[str, str]]:
    """DDL 中登记的表：(schema, table) 列表。"""
    tables: list[tuple[str, str]] = []
    for path in ddl_files():
        for schema, table in _CREATE_TABLE_RE.findall(path.read_text(encoding="utf-8")):
            pair = (schema, table)
            if pair not in tables:
                tables.append(pair)
    return tables


def hypertables() -> list[tuple[str, str]]:
    """DDL 中登记的 hypertable：(schema, table) 列表。"""
    result: list[tuple[str, str]] = []
    for path in ddl_files():
        raw = path.read_text(encoding="utf-8")
        lowered = raw.lower()
        if "create_hypertable" not in lowered:
            continue
        for schema, table in _CREATE_TABLE_RE.findall(raw):
            if f"{schema}.{table}".lower() in lowered.split("create_hypertable", 1)[1]:
                pair = (schema, table)
                if pair not in result:
                    result.append(pair)
    return result


def redis_keys_doc() -> dict[str, Any]:
    """redis-keys.yaml 全文。"""
    return load_yaml(DB_DIR / "redis-keys.yaml")


def redis_keys() -> list[dict[str, Any]]:
    """Redis 受控键列表（契约中 name 含 {placeholder} 占位符）。"""
    doc = redis_keys_doc()
    return list(doc.get("keys") or doc.get("redis_keys") or [])


def redis_key(pattern: str) -> dict[str, Any]:
    """按契约键模式（含占位符形式，如 ``vehicle:status:{vehicle_id}``）取受控键定义。"""
    for entry in redis_keys():
        if entry.get("pattern") == pattern or entry.get("name") == pattern:
            return entry
    raise KeyError(f"Redis 键未在 contracts/database/redis-keys.yaml 登记: {pattern}")


def object_storage_doc() -> dict[str, Any]:
    """object-storage.yaml 全文。"""
    return load_yaml(DB_DIR / "object-storage.yaml")


def buckets() -> dict[str, dict[str, Any]]:
    """MinIO Bucket 契约：{bucket: spec}。"""
    return {entry["name"]: entry for entry in object_storage_doc()["buckets"]}


def presign_policy() -> dict[str, Any]:
    """预签名策略（上传 3600s / 下载 900s，不可调整）。"""
    return object_storage_doc()["presign_policy"]


def lifecycle_expire_days(bucket: str) -> dict[str | None, int | None]:
    """Bucket 生命周期：{prefix: expire_days}（prefix=None 表示整桶规则）。"""
    lifecycle = buckets()[bucket]["lifecycle"]
    if lifecycle.get("mode") == "expire" and "rules" not in lifecycle:
        return {None: lifecycle.get("expire_days")}
    rules_map: dict[str | None, int | None] = {}
    for rule in lifecycle.get("rules", []):
        rules_map[rule.get("prefix") or None] = rule.get("expire_days")
    return rules_map


def gateway_routes() -> list[dict[str, Any]]:
    """网关路由表（contracts/openapi/api-gateway.yaml#x-hunter-gateway-routes.routes）。"""
    return list(openapi_spec("api-gateway")["x-hunter-gateway-routes"]["routes"])


def gateway_route_prefixes() -> dict[str, str]:
    """路由前缀 → 目标服务（前缀不可自行更改）。"""
    return {entry["prefix"]: entry["target_service"] for entry in gateway_routes()}


def gateway_websocket_routes() -> list[dict[str, Any]]:
    """网关 WebSocket 路由（/ws/remote/** → remote-control）。"""
    return list(openapi_spec("api-gateway")["x-hunter-websocket-routes"])


def rate_limits() -> dict[str, Any]:
    """网关限流契约（附录 D：全局限流/单用户/单 IP/端点级 + 车辆侧约束）。"""
    return openapi_spec("api-gateway")["x-hunter-rate-limits"]


def endpoint_rate_limit(method: str, path: str) -> dict[str, Any] | None:
    """端点级限流声明（POST /ota/versions、POST /remote/session、GET /data/telemetry）。"""
    for entry in rate_limits()["endpoint_limits"]:
        if entry["method"].upper() == method.upper() and entry["path"] == path:
            return entry
    return None


def service_port(service: str) -> int:
    """服务监听端口。

    端口来源（均取自契约，禁止硬编码）：
    1. ``contracts/openapi/api-gateway.yaml`` 路由表 ``x-hunter-routes[].target_port``
    2. 服务自身 OpenAPI 的 ``servers[].url``（网关自身不在路由表中，如 8080）
    """
    for entry in gateway_routes():
        if entry["target_service"] == service and entry.get("target_port"):
            return int(entry["target_port"])
    for server in openapi_spec(service).get("servers", []):
        url = str(server.get("url", ""))
        match = re.search(r":(\d{2,5})(?:/|$)", url)
        if match:
            return int(match.group(1))
    raise KeyError(f"服务端口未在契约中登记: {service}")


def _strip_sql_comments(sql: str) -> str:
    """去掉 ``--`` 行注释，便于解析 DDL。"""
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


def _split_sql_items(body: str) -> list[str]:
    """按括号深度 0 的逗号切分 CREATE TABLE 主体（正确保留 ``CHECK (...)`` 内的逗号）。"""
    items: list[str] = []
    depth = 0
    current: list[str] = []
    for char in body:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == "," and depth == 0:
            items.append("".join(current).strip())
            current = []
            continue
        current.append(char)
    tail = "".join(current).strip()
    if tail:
        items.append(tail)
    return items


_CONSTRAINT_KEYWORDS = frozenset(
    {
        "CONSTRAINT",
        "CHECK",
        "PRIMARY",
        "UNIQUE",
        "FOREIGN",
        "REFERENCES",
        "EXCLUDE",
        "LIKE",
        "INHERITS",
        "WITH",
    }
)


def _create_table_body(schema: str, table: str) -> str:
    """取 CREATE TABLE 主体（已去注释）。"""
    pattern = re.compile(
        rf"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+{re.escape(schema)}\.{re.escape(table)}\s*\((.*?)\n\)\s*;",
        re.IGNORECASE | re.DOTALL,
    )
    for path in ddl_files():
        match = pattern.search(_strip_sql_comments(path.read_text(encoding="utf-8")))
        if match is not None:
            return match.group(1)
    raise KeyError(f"DDL 中未找到表定义: {schema}.{table}")


def table_columns(schema: str, table: str) -> list[tuple[str, str]]:
    """解析 DDL 取列定义：[(列名, 类型), ...]（列名以契约为准，测试禁止硬编码）。"""
    columns: list[tuple[str, str]] = []
    for item in _split_sql_items(_create_table_body(schema, table)):
        parts = item.split(None, 2)
        if len(parts) < 2 or parts[0].upper().rstrip(",") in _CONSTRAINT_KEYWORDS:
            continue
        columns.append((parts[0].strip('"').strip(","), parts[1].strip(",")))
    return columns


def table_column_names(schema: str, table: str) -> list[str]:
    """表列名列表（用于生成 INSERT / 校验落库字段）。"""
    return [name for name, _ in table_columns(schema, table)]


def table_constraints(schema: str, table: str) -> list[str]:
    """表级约束/外键片段（用于断言 CHECK 枚举、REFERENCES 存在）。"""
    return [
        item
        for item in _split_sql_items(_create_table_body(schema, table))
        if item.split(None, 1)[0].upper() in _CONSTRAINT_KEYWORDS
    ]


def primary_key_columns(schema: str, table: str) -> list[str]:
    """表主键列（DDL 内联 ``PRIMARY KEY`` 标记）。"""
    keys: list[str] = []
    for item in _split_sql_items(_create_table_body(schema, table)):
        upper = item.upper()
        if "PRIMARY KEY" not in upper:
            continue
        first = item.split(None, 1)[0].strip('"').strip(",")
        if first.upper() == "PRIMARY":  # 表级 PRIMARY KEY (a, b) 形式
            inside = item[item.index("(") + 1 : item.rindex(")")]
            keys.extend(part.strip().strip('"') for part in inside.split(","))
        else:
            keys.append(first)
    return keys


def quoted_columns(schema: str, table: str) -> str:
    """生成 SQL 列清单片段（供批量插入语句拼接，列名来自 DDL 契约）。"""
    return ", ".join(f'"{name}"' for name in table_column_names(schema, table))


# ---------------------------------------------------------------- OpenAPI

def openapi_spec(service: str) -> dict[str, Any]:
    """加载服务 OpenAPI 契约（service 形如 data-collector）。"""
    path = OPENAPI_DIR / f"{service}.yaml"
    assert path.is_file(), f"OpenAPI 契约不存在: {path}"
    return load_yaml(path)


def openapi_paths(service: str) -> dict[str, set[str]]:
    """契约声明端点：{path: {METHOD, ...}}。"""
    result: dict[str, set[str]] = {}
    for path, item in (openapi_spec(service).get("paths") or {}).items():
        methods = {
            method.upper()
            for method in item
            if method.lower() in {"get", "post", "put", "patch", "delete", "head", "options"}
        }
        if methods:
            result[str(path)] = methods
    return result


def openapi_all_endpoints() -> set[tuple[str, str, str]]:
    """六服务契约端点全集：(service, METHOD, path)。"""
    return {
        (service, method, path)
        for service in SERVICE_NAMES
        for path, methods in openapi_paths(service).items()
        for method in methods
    }


def openapi_internal_paths(service: str) -> set[str]:
    """契约中标记为「内部运维端点」的路径（``x-internal: true`` 或 ``tags: [ops]``）。

    依据：六服务契约均在 /healthz、/readyz、/metrics 上标注 ``tags: [ops]``
    （部分服务另加 ``x-internal: true``），实现侧以 ``include_in_schema=False`` 隐藏，
    因此不能把它们计入「契约声明但未实现」的端点。
    """
    result: set[str] = set()
    for path, item in (openapi_spec(service).get("paths") or {}).items():
        operations = [
            operation
            for method, operation in item.items()
            if method.lower() in {"get", "post", "put", "patch", "delete", "head", "options"}
            and isinstance(operation, dict)
        ]
        if operations and all(
            operation.get("x-internal") is True or "ops" in (operation.get("tags") or [])
            for operation in operations
        ):
            result.add(str(path))
    return result


def gateway_route_prefixes_legacy() -> dict[str, list[str]]:
    """由 api-gateway.yaml paths 推导的前缀分组（辅助信息，正式路由表见 gateway_routes()）。"""
    routes: dict[str, list[str]] = {}
    for path in openapi_paths("api-gateway"):
        match = _GATEWAY_PREFIX_RE.match(path)
        if match:
            routes.setdefault(match.group(1), []).append(path)
    return routes


def consumer_group(group_id: str) -> dict[str, Any]:
    """按 group_id 取消费组契约（不存在即失败，禁止自造消费者组）。"""
    groups = consumer_groups()
    assert group_id in groups, f"消费组未在 contracts/kafka/consumer-groups.yaml 登记: {group_id}"
    return groups[group_id]
