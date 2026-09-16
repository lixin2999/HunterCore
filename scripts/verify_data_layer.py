#!/usr/bin/env python3
"""HunterEdge 数据层契约校验（L2）。

校验项：
  1. 契约文件齐备（contracts/database/ddl/*.sql + enums.md + er.md；contracts/kafka 清单与 Schema）
  2. DDL ↔ ORM 逐表逐列一致（列名、规范化类型、可空性、主键）
  3. DDL CHECK 枚举 ↔ hunter_common.database.enums 受控词表一致
  4. TimescaleDB hypertable 契约（1 day 分块 / 90 天保留）在 DDL、迁移、代码常量三处一致
  5. Alembic 离线 SQL 可生成（无需数据库），包含全部 13 张表与 hypertable/保留策略语句
  6. Kafka Topic 契约三处同步（contracts/kafka/topics.yaml、docker create-topics.sh、K8s kafka-init Job）
  7. JSON Schema（draft-07）：结构合法、required 非空、examples 通过自身校验
  8. consumer-groups.yaml 消费的 Topic 均已登记、消费者组 ID 唯一

用法：python scripts/verify_data_layer.py
退出码：0 全部通过；1 存在失败项
"""
from __future__ import annotations

import io
import json
import re
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
DDL_DIR = ROOT / "contracts" / "database" / "ddl"
DB_CONTRACT_DIR = ROOT / "contracts" / "database"
KAFKA_CONTRACT_DIR = ROOT / "contracts" / "kafka"
SCHEMA_DIR = KAFKA_CONTRACT_DIR / "schemas"
COMPOSE_TOPICS_SCRIPT = ROOT / "infra" / "docker" / "kafka" / "create-topics.sh"
K8S_TOPICS_JOB = ROOT / "infra" / "k8s" / "jobs" / "kafka-init-job.yaml"
ALEMBIC_INI = ROOT / "common" / "python" / "alembic.ini"

sys.path.insert(0, str(ROOT / "common" / "python"))

#: 契约表（fq 名称）→ 定义文件（防止漏建表/漏文件）
EXPECTED_TABLES: dict[str, str] = {
    "vehicle_svc.vehicles": "01_core.sql",
    "user_svc.users": "01_core.sql",
    "user_svc.roles": "01_core.sql",
    "user_svc.permissions": "01_core.sql",
    "user_svc.user_roles": "01_core.sql",
    "user_svc.role_permissions": "01_core.sql",
    "scene_svc.scenes": "02_scene.sql",
    "ota_svc.ota_versions": "03_ota.sql",
    "ota_svc.ota_tasks": "03_ota.sql",
    "ota_svc.ota_records": "03_ota.sql",
    "data_collector.events": "04_events.sql",
    "data_collector.vehicle_telemetry": "05_timeseries.sql",
    "data_analytics.algorithm_metrics": "05_timeseries.sql",
}

#: 契约 schema（ddl/00_schemas.sql 中必须全部创建）
EXPECTED_SCHEMAS = (
    "vehicle_svc",
    "user_svc",
    "scene_svc",
    "data_collector",
    "data_analytics",
    "ota_svc",
    "remote_control",
    "gateway",
)

#: 平台内部 Topic 契约（名称 → 分区数、保留毫秒）——与设计文档/基座一致，改动需同步 4 处
EXPECTED_PLATFORM_TOPICS: dict[str, tuple[int, int]] = {
    "telemetry_raw": (12, 604_800_000),
    "telemetry_clean": (12, 604_800_000),
    "event_raw": (6, 2_592_000_000),
    "sensor_file": (3, 604_800_000),
    "analytics_result": (6, 2_592_000_000),
    "alert_event": (3, 2_592_000_000),
}

#: 车端 Topic 契约（名称模式 → 分区数、acks、频率）
EXPECTED_VEHICLE_TOPICS: dict[str, tuple[int, str, str]] = {
    "hunter.{vehicle_id}.telemetry": (6, "1", "10-50Hz"),
    "hunter.{vehicle_id}.event": (3, "all", "事件触发"),
    "hunter.{vehicle_id}.health": (3, "0", "1Hz"),
    "hunter.{vehicle_id}.command": (3, "all", "按需"),
    "hunter.{vehicle_id}.command_result": (3, "all", "按需"),
    "hunter.{vehicle_id}.ota_notify": (3, "all", "按需"),
    "hunter.{vehicle_id}.ota_status": (3, "all", "状态变更"),
    "hunter.{vehicle_id}.remote_control": (3, "all", "20Hz"),
    "hunter.broadcast.command": (3, "all", "按需"),
}

#: 必须存在的消息 Schema（topic/pattern → schema 文件）
EXPECTED_SCHEMAS_FILES = (
    "telemetry.schema.json",
    "event.schema.json",
    "health.schema.json",
    "command.schema.json",
    "command_result.schema.json",
    "ota_notify.schema.json",
    "ota_status.schema.json",
    "remote_control.schema.json",
)

CREATE_TABLE_RE = re.compile(
    r"CREATE TABLE\s+(?:IF NOT EXISTS\s+)?([A-Za-z_][\w.]*)\s*\((.*?)\n\);", re.DOTALL
)
CREATE_SCHEMA_RE = re.compile(r"CREATE SCHEMA IF NOT EXISTS\s+([a-z_][\w]*)")
CHECK_IN_RE = re.compile(r"([a-z_][\w]*)\s+IN\s*\(([^)]*)\)", re.IGNORECASE)
QUOTED_RE = re.compile(r"'([^']*)'")
COLUMN_STOP_WORDS = {
    "NOT",
    "NULL",
    "DEFAULT",
    "CHECK",
    "REFERENCES",
    "PRIMARY",
    "UNIQUE",
    "GENERATED",
    "COLLATE",
    "CONSTRAINT",
}

failures: list[str] = []
checks: list[str] = []


def ok(msg: str) -> None:
    checks.append(f"[PASS] {msg}")


def fail(msg: str) -> None:
    failures.append(f"[FAIL] {msg}")
    checks.append(f"[FAIL] {msg}")


def normalize_type(raw: str) -> str:
    """DDL/ORM 类型规范化（统一比较口径）。"""
    normalized = " ".join(raw.upper().split())
    normalized = normalized.replace("TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE")
    if normalized.startswith("BIGSERIAL"):
        normalized = normalized.replace("BIGSERIAL", "BIGINT")
    elif normalized.startswith("SERIAL"):
        normalized = normalized.replace("SERIAL", "INTEGER")
    return normalized


def split_top_level(body: str) -> list[str]:
    """按顶层逗号切分 CREATE TABLE 主体（忽略括号内的逗号，如 CHECK (...)）。"""
    items: list[str] = []
    depth = 0
    current: list[str] = []
    for char in body:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == "," and depth == 0:
            items.append("".join(current))
            current = []
        else:
            current.append(char)
    tail = "".join(current).strip()
    if tail:
        items.append(tail)
    return items


def parse_ddl() -> dict[str, dict[str, Any]]:
    """解析 DDL 契约 → ``{fq_table: {"columns": {name: {...}}, "pk": [...], "file": ...}}``。"""
    tables: dict[str, dict[str, Any]] = {}
    for path in sorted(DDL_DIR.glob("*.sql")):
        text = path.read_text(encoding="utf-8")
        for match in CREATE_TABLE_RE.finditer(text):
            fq_name, body = match.group(1), match.group(2)
            columns: dict[str, dict[str, Any]] = {}
            pk_columns: list[str] = []
            for item in split_top_level(body):
                line = " ".join(re.sub(r"--[^\n]*", "", item).split())
                if not line:
                    continue
                upper = line.upper()
                if upper.startswith("PRIMARY KEY"):
                    pk_columns.extend(re.findall(r"[a-z_][\w]*", line.split("(", 1)[1]))
                    continue
                if upper.startswith(("CONSTRAINT", "CHECK", "FOREIGN", "UNIQUE", "EXCLUDE")):
                    continue
                tokens = line.split()
                name = tokens[0].lower()
                type_tokens: list[str] = []
                for token in tokens[1:]:
                    if token.upper().rstrip(",") in COLUMN_STOP_WORDS:
                        break
                    type_tokens.append(token)
                inline_pk = "PRIMARY KEY" in upper
                columns[name] = {
                    "type": normalize_type(" ".join(type_tokens)),
                    "nullable": "NOT NULL" not in upper,
                }
                if inline_pk:
                    pk_columns.append(name)
            # 主键列隐含 NOT NULL（DDL 内联 PRIMARY KEY 未显式声明）
            for pk_name in pk_columns:
                if pk_name in columns:
                    columns[pk_name]["nullable"] = False
            tables[fq_name] = {"columns": columns, "pk": pk_columns, "file": path.name}
    return tables


def orm_tables() -> dict[str, dict[str, Any]]:
    """读取 SQLAlchemy metadata → ``{fq_table: {"columns": {...}, "pk": [...]}}``。"""
    import hunter_common.database.models  # noqa: F401  (register models)
    from hunter_common.database.base import Base
    from sqlalchemy.dialects import postgresql

    dialect = postgresql.dialect()
    tables: dict[str, dict[str, Any]] = {}
    for key, table in Base.metadata.tables.items():
        columns: dict[str, dict[str, Any]] = {}
        for column in table.columns:
            columns[column.name] = {
                "type": normalize_type(column.type.compile(dialect=dialect)),
                "nullable": bool(column.nullable),
            }
        tables[key] = {
            "columns": columns,
            "pk": [column.name for column in table.primary_key.columns],
        }
    return tables


def check_contract_files() -> None:
    """校验 1：契约文件齐备。"""
    for name in sorted({*EXPECTED_TABLES.values(), "00_schemas.sql"}):
        path = DDL_DIR / name
        if path.is_file():
            ok(f"DDL 契约存在: contracts/database/ddl/{name}")
        else:
            fail(f"缺失 DDL 契约: contracts/database/ddl/{name}")
    for name in ("README.md", "er.md", "enums.md"):
        path = DB_CONTRACT_DIR / name
        if path.is_file():
            ok(f"数据库契约文档存在: contracts/database/{name}")
        else:
            fail(f"缺失数据库契约文档: contracts/database/{name}")

    schemas_sql = DDL_DIR / "00_schemas.sql"
    if schemas_sql.is_file():
        found = set(CREATE_SCHEMA_RE.findall(schemas_sql.read_text(encoding="utf-8")))
        missing = set(EXPECTED_SCHEMAS) - found
        if missing:
            fail(f"00_schemas.sql 缺少 schema 定义: {sorted(missing)}")
        else:
            ok(f"schema 定义齐备（{len(EXPECTED_SCHEMAS)} 个）")

    for name in ("topics.yaml", "consumer-groups.yaml", "README.md"):
        path = KAFKA_CONTRACT_DIR / name
        if path.is_file():
            ok(f"Kafka 契约存在: contracts/kafka/{name}")
        else:
            fail(f"缺失 Kafka 契约: contracts/kafka/{name}")

    found_schemas = {path.name for path in SCHEMA_DIR.glob("*.schema.json")}
    missing_schemas = sorted(set(EXPECTED_SCHEMAS_FILES) - found_schemas)
    if missing_schemas:
        fail(f"缺失消息 JSON Schema: {missing_schemas}")
    else:
        ok(f"消息 JSON Schema 齐备（{len(EXPECTED_SCHEMAS_FILES)} 个）")


def check_ddl_orm_parity() -> tuple[dict[str, Any], dict[str, Any]]:
    """校验 2：DDL ↔ ORM 表/列/类型/可空性/主键完全一致。"""
    ddl = parse_ddl()
    orm = orm_tables()
    before = len(failures)

    missing_tables = sorted(set(EXPECTED_TABLES) - set(ddl))
    extra_tables = sorted(set(ddl) - set(EXPECTED_TABLES))
    if missing_tables:
        fail(f"DDL 缺失契约表: {missing_tables}")
    if extra_tables:
        fail(f"DDL 出现契约外新增表: {extra_tables}（需先更新契约并评审）")
    if not missing_tables and not extra_tables:
        ok(f"DDL 表清单与契约一致（{len(EXPECTED_TABLES)} 张）")

    orm_missing = sorted(set(EXPECTED_TABLES) - set(orm))
    orm_extra = sorted(set(orm) - set(EXPECTED_TABLES))
    if orm_missing:
        fail(f"ORM 缺失契约表映射: {orm_missing}")
    if orm_extra:
        fail(f"ORM 出现契约外表: {orm_extra}")
    if not orm_missing and not orm_extra:
        ok(f"ORM 表清单与契约一致（{len(orm)} 张）")

    for fq_name in sorted(set(EXPECTED_TABLES) & set(ddl) & set(orm)):
        ddl_columns = ddl[fq_name]["columns"]
        orm_columns = orm[fq_name]["columns"]
        file_name = EXPECTED_TABLES[fq_name]

        only_ddl = sorted(set(ddl_columns) - set(orm_columns))
        only_orm = sorted(set(orm_columns) - set(ddl_columns))
        if only_ddl:
            fail(f"{fq_name}: ORM 缺少列 {only_ddl}（见 {file_name}）")
        if only_orm:
            fail(f"{fq_name}: ORM 多出列 {only_orm}（契约外新增字段需评审）")

        for name in sorted(set(ddl_columns) & set(orm_columns)):
            ddl_spec, orm_spec = ddl_columns[name], orm_columns[name]
            if ddl_spec["type"] != orm_spec["type"]:
                fail(f"{fq_name}.{name}: 类型不一致 DDL={ddl_spec['type']} ORM={orm_spec['type']}")
            if ddl_spec["nullable"] != orm_spec["nullable"]:
                fail(
                    f"{fq_name}.{name}: 可空性不一致 "
                    f"DDL={'NULL' if ddl_spec['nullable'] else 'NOT NULL'} "
                    f"ORM={'NULL' if orm_spec['nullable'] else 'NOT NULL'}"
                )
        if sorted(ddl[fq_name]["pk"]) != sorted(orm[fq_name]["pk"]):
            fail(
                f"{fq_name}: 主键不一致 "
                f"DDL={sorted(ddl[fq_name]['pk'])} ORM={sorted(orm[fq_name]['pk'])}"
            )

    if len(failures) == before:
        ok("DDL ↔ ORM 逐列一致（列名/类型/可空性/主键）")
    return ddl, orm


def parse_ddl_checks() -> dict[str, dict[str, set[str]]]:
    """解析 DDL 中的 ``CHECK (col IN (...))`` → ``{fq_table: {col: {values}}}``。"""
    result: dict[str, dict[str, set[str]]] = {}
    for path in sorted(DDL_DIR.glob("*.sql")):
        text = path.read_text(encoding="utf-8")
        for match in CREATE_TABLE_RE.finditer(text):
            fq_name, body = match.group(1), match.group(2)
            per_column: dict[str, set[str]] = {}
            for column, values in CHECK_IN_RE.findall(body):
                per_column.setdefault(column.lower(), set()).update(QUOTED_RE.findall(values))
            result[fq_name] = per_column
    return result


def check_enum_contract(ddl: dict[str, Any]) -> None:
    """校验 3：DDL CHECK 枚举 ↔ hunter_common.database.enums 受控词表一致。"""
    from hunter_common.database import enums as db_enums

    expected: dict[tuple[str, str], type] = {
        ("vehicle_svc.vehicles", "status"): db_enums.VehicleStatus,
        ("user_svc.users", "status"): db_enums.UserStatus,
        ("user_svc.roles", "status"): db_enums.RoleStatus,
        ("scene_svc.scenes", "status"): db_enums.SceneStatus,
        ("ota_svc.ota_versions", "status"): db_enums.OtaVersionStatus,
        ("ota_svc.ota_tasks", "status"): db_enums.OtaTaskStatus,
        ("ota_svc.ota_records", "status"): db_enums.OtaStatus,
        ("ota_svc.ota_records", "phase"): db_enums.OtaStatus,
        ("data_collector.events", "event_type"): db_enums.EventType,
        ("data_collector.events", "event_level"): db_enums.EventLevel,
        ("data_analytics.algorithm_metrics", "module"): db_enums.MetricModule,
    }
    ddl_checks = parse_ddl_checks()
    before = len(failures)

    for (fq_name, column), enum_class in expected.items():
        declared = ddl_checks.get(fq_name, {}).get(column)
        enum_values = {member.value for member in enum_class}
        if declared is None:
            fail(f"{fq_name}.{column}: DDL 缺少受控词表 CHECK 约束（应含 {len(enum_values)} 个取值）")
            continue
        if declared != enum_values:
            fail(
                f"{fq_name}.{column}: DDL 取值与 {enum_class.__name__} 不一致 "
                f"仅DDL={sorted(declared - enum_values)} 仅代码={sorted(enum_values - declared)}"
            )
    if len(failures) == before:
        ok(f"受控词表一致（{len(expected)} 处 CHECK ↔ StrEnum，含 18 种事件类型/9 态 OTA 状态机）")

    # 事件类型 → 等级映射完整性（data-collector 落库前校验依据）
    mapping = db_enums.EVENT_LEVEL_BY_TYPE
    missing = sorted({t.value for t in db_enums.EventType} - set(mapping))
    if missing:
        fail(f"EVENT_LEVEL_BY_TYPE 缺少事件类型映射: {missing}")
    else:
        ok(f"事件类型等级映射完整（{len(mapping)} 条）")


def check_hypertable_contract() -> None:
    """校验 4：hypertable 契约（1 day 分块 / 90 天保留）在 DDL、迁移、代码常量三处一致。"""
    from hunter_common.database.schema_names import (
        CHUNK_TIME_INTERVAL,
        HYPERTABLES,
        RETENTION_INTERVAL,
    )

    expected_tables = {
        "data_collector.vehicle_telemetry",
        "data_analytics.algorithm_metrics",
    }
    if set(HYPERTABLES) != expected_tables:
        fail(f"schema_names.HYPERTABLES 与契约不一致: {sorted(HYPERTABLES)}")
    if CHUNK_TIME_INTERVAL != "1 day" or RETENTION_INTERVAL != "90 days":
        fail(
            f"分块/保留策略常量不符契约: chunk={CHUNK_TIME_INTERVAL!r} retention={RETENTION_INTERVAL!r}"
        )

    timeseries_sql = (DDL_DIR / "05_timeseries.sql").read_text(encoding="utf-8")
    for table in sorted(expected_tables):
        if f"create_hypertable('{table}', 'time'" not in timeseries_sql:
            fail(f"05_timeseries.sql 缺少 create_hypertable: {table}")
        if f"add_retention_policy('{table}', INTERVAL '90 days'" not in timeseries_sql:
            fail(f"05_timeseries.sql 缺少 90 天保留策略: {table}")

    migration = (
        ROOT
        / "common"
        / "python"
        / "hunter_common"
        / "database"
        / "migrations"
        / "versions"
        / "0001_initial_schema.py"
    )
    migration_sql = migration.read_text(encoding="utf-8") if migration.is_file() else ""
    for keyword in ("create_hypertable", "add_retention_policy", "common_set_update_time"):
        if keyword not in migration_sql:
            fail(f"迁移 0001 缺少语句: {keyword}")
    if not failures:
        ok("hypertable 契约一致（ddl/05_timeseries.sql ↔ 迁移 0001 ↔ schema_names 常量）")


def check_alembic_offline_sql() -> None:
    """校验 5：Alembic 离线 SQL 生成成功且覆盖全部契约表（无需数据库）。"""
    try:
        from alembic import command
        from alembic.config import Config
    except ImportError as exc:  # pragma: no cover - alembic 为运行期依赖
        fail(f"alembic 未安装，无法校验迁移: {exc}")
        return

    buffer = io.StringIO()
    config = Config(str(ALEMBIC_INI))
    config.output_buffer = buffer
    try:
        command.upgrade(config, "head", sql=True)
    except Exception as exc:  # noqa: BLE001 - alembic/sqlalchemy 任意异常都必须上报为校验失败
        fail(f"Alembic 离线 SQL 生成失败: {exc!r}")
        return

    sql = buffer.getvalue()
    missing = [name for name in sorted(EXPECTED_TABLES) if f"CREATE TABLE {name}" not in sql]
    if missing:
        fail(f"迁移未创建契约表: {missing}")
    else:
        ok(f"Alembic 离线 SQL 覆盖全部契约表（{len(EXPECTED_TABLES)} 张）")

    for keyword in (
        "CREATE EXTENSION IF NOT EXISTS timescaledb",
        "create_hypertable",
        "add_retention_policy",
        "trg_scenes_set_update_time",
    ):
        if keyword not in sql:
            fail(f"迁移 SQL 缺少: {keyword}")


def load_yaml(path: Path) -> dict[str, Any]:
    """读取 YAML 契约文件。"""
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def parse_create_topic_calls(text: str) -> dict[str, tuple[int, int]]:
    """解析 ``create_topic "<name>" <partitions> <retention_ms>`` 调用（docker 脚本与 K8s Job 同格式）。"""
    pattern = re.compile(r'create_topic\s+"([\w.]+)"\s+(\d+)\s+(\d+)')
    return {
        name: (int(partitions), int(retention))
        for name, partitions, retention in pattern.findall(text)
    }


def check_kafka_topics() -> dict[str, Any]:
    """校验 6：Kafka Topic 契约三处同步（契约 / docker 脚本 / K8s Job）。"""
    topics = load_yaml(KAFKA_CONTRACT_DIR / "topics.yaml")
    before = len(failures)

    platform_topics = {entry["name"]: entry for entry in topics.get("platform_topics", [])}
    if set(platform_topics) != set(EXPECTED_PLATFORM_TOPICS):
        fail(
            f"平台内部 Topic 清单不一致: 契约={sorted(platform_topics)} "
            f"期望={sorted(EXPECTED_PLATFORM_TOPICS)}"
        )
    for name, (partitions, retention) in EXPECTED_PLATFORM_TOPICS.items():
        entry = platform_topics.get(name)
        if entry is None:
            continue
        if entry.get("partitions") != partitions:
            fail(f"Topic {name}: 分区数 {entry.get('partitions')} != {partitions}")
        if entry.get("retention_ms") != retention:
            fail(f"Topic {name}: 保留时间 {entry.get('retention_ms')} != {retention}")

    vehicle_topics = {entry["name"]: entry for entry in topics.get("vehicle_topics", [])}
    if set(vehicle_topics) != set(EXPECTED_VEHICLE_TOPICS):
        fail(
            f"车端 Topic 清单不一致: 契约={sorted(vehicle_topics)} "
            f"期望={sorted(EXPECTED_VEHICLE_TOPICS)}"
        )
    for name, (partitions, acks, frequency) in EXPECTED_VEHICLE_TOPICS.items():
        entry = vehicle_topics.get(name)
        if entry is None:
            continue
        if entry.get("partitions") != partitions:
            fail(f"Topic {name}: 分区数 {entry.get('partitions')} != {partitions}")
        if str(entry.get("acks")) != acks:
            fail(f"Topic {name}: acks={entry.get('acks')} != {acks}")
        if entry.get("frequency") != frequency:
            fail(f"Topic {name}: 频率 {entry.get('frequency')} != {frequency}")
        if name != "hunter.broadcast.command" and entry.get("key") != "vehicle_id":
            fail(f"Topic {name}: 消息 key 必须为 vehicle_id（保证单车辆有序）")

    naming = topics.get("naming", {})
    if naming.get("dlq_pattern") != "{original_topic}.dlq":
        fail(f"DLQ 命名必须为 {{original_topic}}.dlq，当前={naming.get('dlq_pattern')}")
    defaults = topics.get("defaults", {})
    if defaults.get("security_protocol") != "SASL_SSL":
        fail("车端 Topic 默认安全协议必须为 SASL_SSL")
    if defaults.get("sasl_mechanism") != "SCRAM-SHA-512":
        fail("Kafka SASL 机制必须为 SCRAM-SHA-512")
    producer_defaults = topics.get("producer_defaults", {})
    expected_producer = {"linger_ms": 5, "batch_size": 16384, "retries": 3}
    for key, value in expected_producer.items():
        if producer_defaults.get(key) != value:
            fail(f"生产者基准参数 {key}={producer_defaults.get(key)} != {value}")
    if topics.get("consumer_defaults", {}).get("enable_auto_commit") is not False:
        fail("消费者必须手动提交 offset（enable_auto_commit=false）")

    # Schema 引用：已定义的必须存在；未定义者仅允许 3 个平台内部 Topic
    tbd_allowed = {"sensor_file", "analytics_result", "alert_event"}
    tbd_actual: set[str] = set()
    for group in ("vehicle_topics", "platform_topics"):
        for entry in topics.get(group, []):
            ref = entry.get("schema")
            if ref is None:
                tbd_actual.add(entry["name"])
                continue
            if not (KAFKA_CONTRACT_DIR / str(ref)).is_file():
                fail(f"Topic {entry['name']} 引用的 Schema 不存在: {ref}")
    if tbd_actual != tbd_allowed:
        fail(f"Schema 待定 Topic 集合异常: {sorted(tbd_actual)}（期望 {sorted(tbd_allowed)}）")

    compose_calls = parse_create_topic_calls(COMPOSE_TOPICS_SCRIPT.read_text(encoding="utf-8"))
    if compose_calls != EXPECTED_PLATFORM_TOPICS:
        fail(f"infra/docker/kafka/create-topics.sh 与契约不一致: {compose_calls}")

    k8s_script = ""
    for doc in yaml.safe_load_all(K8S_TOPICS_JOB.read_text(encoding="utf-8")):
        if doc and doc.get("kind") == "ConfigMap":
            k8s_script = (doc.get("data") or {}).get("create-topics.sh", "")
    k8s_calls = parse_create_topic_calls(k8s_script)
    if k8s_calls != EXPECTED_PLATFORM_TOPICS:
        fail(f"infra/k8s/jobs/kafka-init-job.yaml 与契约不一致: {k8s_calls}")

    if len(failures) == before:
        ok(
            f"Kafka Topic 契约一致（车端 {len(vehicle_topics)} + 平台 {len(platform_topics)}，"
            "topics.yaml ↔ create-topics.sh ↔ kafka-init Job）"
        )
    return topics


def has_enum(node: Any) -> bool:
    """递归判断 Schema 是否包含 enum 约束。"""
    if isinstance(node, dict):
        if "enum" in node:
            return True
        return any(has_enum(value) for value in node.values())
    if isinstance(node, list):
        return any(has_enum(item) for item in node)
    return False


def check_json_schemas() -> None:
    """校验 7：JSON Schema（draft-07）合法、required/enum 齐备、examples 自校验通过。"""
    try:
        from jsonschema import Draft7Validator
        from jsonschema.exceptions import SchemaError
    except ImportError as exc:  # pragma: no cover - jsonschema 为 dev 依赖
        fail(f"jsonschema 未安装（dev 依赖），无法校验消息 Schema: {exc}")
        return

    #: 必须以枚举约束取值域的消息（受控词表）
    enum_required = {
        "event.schema.json",
        "ota_status.schema.json",
        "health.schema.json",
        "command.schema.json",
    }
    before = len(failures)
    for name in EXPECTED_SCHEMAS_FILES:
        path = SCHEMA_DIR / name
        try:
            schema = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            fail(f"{name}: 读取/解析失败 -> {exc}")
            continue
        if schema.get("$schema") != "http://json-schema.org/draft-07/schema#":
            fail(f"{name}: 必须声明 JSON Schema draft-07")
        try:
            Draft7Validator.check_schema(schema)
        except SchemaError as exc:
            fail(f"{name}: Schema 非法 -> {exc.message}")
            continue
        if not schema.get("required"):
            fail(f"{name}: 缺少顶层 required 字段声明")
        if name in enum_required and not has_enum(schema):
            fail(f"{name}: 缺少受控词表 enum 约束")
        examples = schema.get("examples") or []
        if not examples:
            fail(f"{name}: 缺少 examples（无法自校验）")
        validator = Draft7Validator(schema)
        for index, example in enumerate(examples):
            errors = sorted(validator.iter_errors(example), key=lambda error: list(error.path))
            if errors:
                fail(f"{name} examples[{index}] 校验失败 -> {errors[0].message}")
    if len(failures) == before:
        ok(
            f"消息 JSON Schema 全部合法（draft-07，{len(EXPECTED_SCHEMAS_FILES)} 个文件，"
            "examples 自校验通过）"
        )


def check_consumer_groups() -> None:
    """校验 8：消费者组契约（Topic 已登记、组 ID 唯一且合规、幂等键齐备）。"""
    groups = load_yaml(KAFKA_CONTRACT_DIR / "consumer-groups.yaml").get("groups", [])
    before = len(failures)

    known_topics = set(EXPECTED_PLATFORM_TOPICS) | set(EXPECTED_VEHICLE_TOPICS)
    # 车端 Topic 支持 regex 订阅（新车动态接入无需改配置）：hunter.{vehicle_id}.telemetry → hunter.*.telemetry
    known_topics |= {name.replace("{vehicle_id}", "*") for name in EXPECTED_VEHICLE_TOPICS}
    known_topics |= {f"{name}.dlq" for name in known_topics}

    ids = [group.get("group_id") for group in groups]
    duplicates = sorted({gid for gid in ids if ids.count(gid) > 1})
    if duplicates:
        fail(f"消费者组 ID 重复: {duplicates}")

    for group in groups:
        gid = str(group.get("group_id"))
        if not re.fullmatch(r"[a-z0-9]+(-[a-z0-9]+)*", gid):
            fail(f"消费者组 ID 不符命名规范 <service>-<domain>[-<purpose>]: {gid}")
        if not group.get("service"):
            fail(f"{gid}: 缺少 service（归属服务）")
        subscribes = group.get("subscribes") or []
        if not subscribes:
            fail(f"{gid}: 缺少 subscribes")
        for topic in subscribes:
            if topic not in known_topics:
                fail(f"{gid}: 订阅了未登记 Topic {topic}（需先在 topics.yaml 登记）")
        if not group.get("idempotency_key"):
            fail(f"{gid}: 缺少幂等键说明（消费必须幂等）")

    if len(failures) == before:
        ok(f"消费者组契约合法（{len(groups)} 个组，订阅 Topic 均已登记、幂等键齐备）")


def main() -> int:
    # Windows 控制台默认 GBK：显式以 UTF-8 输出，避免契约符号（↔ / ⚠）编码失败
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            reconfigure(encoding="utf-8", errors="replace")

    print("HunterEdge 数据层契约校验（L2）")
    print("=" * 78)
    check_contract_files()
    ddl, _orm = check_ddl_orm_parity()
    check_enum_contract(ddl)
    check_hypertable_contract()
    check_alembic_offline_sql()
    check_kafka_topics()
    check_json_schemas()
    check_consumer_groups()
    print("-" * 78)
    for line in checks:
        print(line)
    print("-" * 78)
    passed = len(checks) - len(failures)
    print(f"结果：通过 {passed} 项，失败 {len(failures)} 项")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())


