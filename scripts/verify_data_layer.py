#!/usr/bin/env python3
"""HunterCore 数据层契约校验（L2）。

校验项：
  1. 契约文件齐备（contracts/database/ddl/*.sql + enums.md + er.md；contracts/kafka 清单与 Schema）
  2. DDL ↔ ORM 逐表逐列一致（列名、规范化类型、可空性、主键）
  3. DDL CHECK 枚举 ↔ hunter_common.database.enums 受控词表一致
  4. TimescaleDB hypertable 契约（1 day 分块 / 90 天保留）在 DDL、迁移、代码常量三处一致
  5. Alembic 离线 SQL 可生成（无需数据库），包含全部 13 张表与 hypertable/保留策略语句
  6. Kafka Topic 契约三处同步（contracts/kafka/topics.yaml、docker create-topics.sh、K8s kafka-init Job）
  7. JSON Schema（draft-07）：结构合法、required 非空、examples 通过自身校验
  8. consumer-groups.yaml 消费的 Topic 均已登记、消费者组 ID 唯一
  9. Redis 键契约（contracts/database/redis-keys.yaml）↔ 各服务 x-hunter-service.redis_keys
     （键模式 / 类型 / TTL / 读写方归属；8 个受控键与系统约束第 8 条一致）
 10. 对象存储契约（contracts/database/object-storage.yaml）↔ docker 与 K8s MinIO 初始化脚本
     ↔ 各服务 x-hunter-service.minio_buckets / minio_bucket_lifecycle / 预签名 TTL（三方一致）

  11. 模型 ↔ Repository 一一对应（hunter_common.database.repositories.REPOSITORY_BY_MODEL ↔ ALL_MODELS）
  12. relationship 异步安全 lazy 策略（契约 orm-mapping.md 第 1 节白名单，禁止隐式 lazy="select"）
  13. ORM 映射与 Repository 契约文档（contracts/database/orm-mapping.md）↔ 实现同步
  14. 服务 → 共享 Repository 白名单（orm-mapping.md 第 3.5 节；禁止跨服务直查其他服务 schema）
  15. 写路径纪律与序列读取窗口规则在契约中显式声明（SAVEPOINT 顺序 / 禁止 expunge / 时序必须有窗口）

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
SERVICE_CONTRACT_DIR = ROOT / "contracts" / "openapi"
ORM_MAPPING_CONTRACT = DB_CONTRACT_DIR / "orm-mapping.md"
#: 契约关系表/Repository 表所在区块标记（避免误匹配其他表格）
RELATION_TABLE_START = "<!-- relationship-table:start -->"
RELATION_TABLE_END = "<!-- relationship-table:end -->"
REPOSITORY_TABLE_START = "<!-- repository-table:start -->"
REPOSITORY_TABLE_END = "<!-- repository-table:end -->"
BASE_METHODS_TABLE_START = "<!-- base-methods-table:start -->"
BASE_METHODS_TABLE_END = "<!-- base-methods-table:end -->"
#: 服务 → Repository 白名单区块标记（orm-mapping.md 第 3.5 节，禁止跨服务直查其他服务 schema）
SERVICE_REPOSITORY_TABLE_START = "<!-- service-repository-table:start -->"
SERVICE_REPOSITORY_TABLE_END = "<!-- service-repository-table:end -->"
#: 写路径纪律区块标记（SAVEPOINT 顺序 + 禁止 expunge）
WRITE_PATH_DISCIPLINE_START = "<!-- write-path-discipline:start -->"
WRITE_PATH_DISCIPLINE_END = "<!-- write-path-discipline:end -->"
#: 时序读取窗口规则区块标记
SERIES_WINDOW_RULE_START = "<!-- series-window-rule:start -->"
SERIES_WINDOW_RULE_END = "<!-- series-window-rule:end -->"
#: 共享 Repository 导入语句（from hunter_common.database[.repositories...] import ...）
SHARED_REPOSITORY_IMPORT_RE = re.compile(
    r"from\s+hunter_common\.database[\w.]*\s+import\s+([^#\n]+)"
)
#: Repository 类名（大驼峰 + Repository 后缀）
REPOSITORY_CLASS_RE = re.compile(r"\b([A-Z]\w*Repository)\b")
#: 服务 → Repository 白名单行：| `scene-service` | `SceneRepository`、`UserRepository` | 说明 |
SERVICE_REPOSITORY_ROW_RE = re.compile(r"^\|\s*`([\w-]+)`\s*\|\s*([^|]+)\|", re.MULTILINE)
#: 写路径纪律/窗口规则必须出现的关键锚点（契约不得静默丢失这些约束）
WRITE_PATH_ANCHORS = ("begin_nested", "expunge", "SAVEPOINT")
SERIES_WINDOW_ANCHORS = ("start_time", "end_time", "2001")
SERVICES_DIR = ROOT / "services"
#: 关系行：`Class.attr` 位于首列
RELATION_ROW_RE = re.compile(r"^\|\s*`([A-Za-z_]\w*)\.([a-z_]\w*)`\s*\|", re.MULTILINE)
#: Repository 行：`XxxRepository` | `Model`
REPOSITORY_ROW_RE = re.compile(r"^\|\s*`(\w+Repository)`\s*\|\s*`(\w+)`\s*\|", re.MULTILINE)
#: Repository 行整行（含「专属方法」列）：| `XxxRepository` | `Model` | `module.py` | methods |
REPOSITORY_METHOD_ROW_RE = re.compile(
    r"^\|\s*`(\w+Repository)`\s*\|\s*`(\w+)`\s*\|\s*`[^`]+`\s*\|(.+?)\|\s*$", re.MULTILINE
)
#: 方法列中的标识符；DDL 索引名前缀需排除（见 INDEX_NAME_PREFIXES）
METHOD_TOKEN_RE = re.compile(r"`([a-z_][a-z0-9_]*)`")
INDEX_NAME_PREFIXES = ("uq_", "idx_", "pk_", "ck_", "fk_")
#: 异步安全 lazy 策略白名单（contracts/database/orm-mapping.md 第 1 节）
ASYNC_SAFE_LAZY = frozenset({"selectin", "raise_on_sql"})

REDIS_CONTRACT = DB_CONTRACT_DIR / "redis-keys.yaml"
OBJECT_STORAGE_CONTRACT = DB_CONTRACT_DIR / "object-storage.yaml"
MINIO_DOCKER_INIT = ROOT / "infra" / "docker" / "minio" / "init-buckets.sh"
MINIO_K8S_INIT = ROOT / "infra" / "k8s" / "jobs" / "minio-init-job.yaml"

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

#: Redis 受控键契约（键模式 → (类型, TTL 秒)；None = 不过期）——来源：系统关键约束第 8 条
#: 改动需同步 contracts/database/redis-keys.yaml 与各服务 x-hunter-service.redis_keys 声明
EXPECTED_REDIS_KEYS: dict[str, tuple[str, int | None]] = {
    "session:{user_id}": ("String", 7200),
    "vehicle:status:{vehicle_id}": ("Hash", None),
    "vehicle:online:set": ("Set", None),
    "rate_limit:{ip}:{api}": ("String", 60),
    "ota:progress:{task_id}": ("Hash", 86400),
    "rc:session:{vehicle_id}": ("Hash", None),
    "cache:scene:{scene_id}": ("String(JSON)", 3600),
    "rc:lock:{vehicle_id}": ("String", 30),
}

#: MinIO Bucket 契约（名称 → 过期天数；None = 永久）——来源：系统关键约束第 7 条（名称不可更改）
EXPECTED_BUCKETS: dict[str, int | None] = {
    "hunter-raw-data": 30,
    "hunter-rosbag": 30,  # 前缀级规则：regular/ 30 天，events/ 永久（见 EXPECTED_ROSBAG_PREFIX_DAYS）
    "hunter-video": 90,
    "hunter-ota-packages": None,
    "hunter-reports": None,
    "hunter-logs": 30,
    "hunter-scene-assets": None,
}

#: hunter-rosbag 前缀级生命周期（前缀 → 过期天数；None = 无过期规则 = 永久）
EXPECTED_ROSBAG_PREFIX_DAYS: dict[str, int | None] = {"regular/": 30, "events/": None}

#: 预签名 URL 有效期（系统关键约束第 7 条：上传 1 小时 / 下载 15 分钟）
EXPECTED_PRESIGN_TTL: dict[str, int] = {
    "upload_expires_in_seconds": 3600,
    "download_expires_in_seconds": 900,
}

#: 各服务契约必须声明的预签名 TTL 集合（object-storage.yaml cross_check 第 3 条）
EXPECTED_SERVICE_PRESIGN_TTL: dict[str, set[int]] = {
    "data-collector": {3600, 900},
    "data-analytics": {3600, 900},
    "ota-service": {3600, 900},
    "scene-service": {900},
    "remote-control": {900},
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
    "analytics_result.schema.json",
    "sensor_file.schema.json",
    "alert_event.schema.json",
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

#: MinIO 初始化脚本解析（docker init-buckets.sh 与 K8s Job 内嵌脚本同格式）
MINIO_CREATE_BUCKET_RE = re.compile(r'create_bucket\s+"([\w.-]+)"')
MINIO_ADD_EXPIRY_RE = re.compile(r'add_expiry\s+"([\w.-]+)"\s+(\d+)(?:\s+"([^"]*)")?')
MINIO_ENCRYPT_BUCKET_RE = re.compile(r'encrypt_bucket\s+"([\w.-]+)"')
#: 契约中以配置键形式声明的预签名 TTL（如 remote-control：RC_PRESIGNED_DOWNLOAD_EXPIRE_SECONDS（默认 900））
PRESIGN_CONFIG_TTL_RE = re.compile(r"[A-Z_]*(?:PRESIGN)[A-Z_]*(?:EXPIRE)[A-Z_]*（默认\s*(\d+)）")
#: Redis 键命名规范 <domain>:<entity>[:<qualifier>]（全小写 + 冒号分隔 + 花括号占位符）
REDIS_KEY_NAMING_RE = re.compile(r"^[a-z0-9_:{}-]+$")

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
    for name in ("README.md", "er.md", "enums.md", "orm-mapping.md"):
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


def check_orm_repository_layer() -> None:
    """校验 11/12/13：模型 ↔ Repository 一一对应、relationship 异步安全、契约文档同步。"""
    import hunter_common.database.models  # noqa: F401  (register models)
    from hunter_common.database.base import Base
    from hunter_common.database.models import ALL_MODELS
    from hunter_common.database.repositories import REPOSITORY_BY_MODEL
    from hunter_common.database.repository import BaseRepository

    # ---- 校验 11：模型 ↔ Repository 一一对应 ----
    model_set = set(ALL_MODELS)
    registry = dict(REPOSITORY_BY_MODEL)
    if model_set != set(registry):
        missing = sorted(model.__name__ for model in model_set - set(registry))
        extra = sorted(model.__name__ for model in set(registry) - model_set)
        fail(f"模型 ↔ Repository 未一一对应（缺 {missing}，多 {extra}）")
    else:
        mismatched = [
            repository.__name__
            for model, repository in registry.items()
            if repository.model is not model or not issubclass(repository, BaseRepository)
        ]
        if mismatched:
            fail(f"Repository.model 未绑定到对应模型或未继承 BaseRepository: {mismatched}")
        else:
            ok(f"模型 ↔ Repository 一一对应（{len(model_set)} 个模型 / Repository）")

    # ---- 校验 12：relationship 异步安全 lazy 策略 ----
    relations = {
        f"{mapper.class_.__name__}.{prop.key}": prop
        for mapper in Base.registry.mappers
        for prop in mapper.relationships
    }
    unsafe = {
        name: prop.lazy for name, prop in relations.items() if prop.lazy not in ASYNC_SAFE_LAZY
    }
    if unsafe:
        fail(f"relationship 使用非异步安全 lazy 策略（应取 {sorted(ASYNC_SAFE_LAZY)}）: {unsafe}")
    else:
        ok(f"relationship 全部显式声明异步安全 lazy 策略（{len(relations)} 条）")

    # ---- 校验 13：契约文档 orm-mapping.md ↔ 实现同步 ----
    if not ORM_MAPPING_CONTRACT.is_file():
        fail("缺失契约: contracts/database/orm-mapping.md")
        return
    text = ORM_MAPPING_CONTRACT.read_text(encoding="utf-8")
    if not all(
        marker in text
        for marker in (
            RELATION_TABLE_START,
            RELATION_TABLE_END,
            REPOSITORY_TABLE_START,
            REPOSITORY_TABLE_END,
            BASE_METHODS_TABLE_START,
            BASE_METHODS_TABLE_END,
        )
    ):
        fail("orm-mapping.md 缺少关系表/Repository 表/通用方法表区块标记")
        return

    relation_block = text.split(RELATION_TABLE_START, 1)[1].split(RELATION_TABLE_END, 1)[0]
    declared_relations = {f"{cls}.{attr}" for cls, attr in RELATION_ROW_RE.findall(relation_block)}
    if declared_relations != set(relations):
        fail(
            "orm-mapping.md 关系表与实现不一致"
            f"（文档多 {sorted(declared_relations - set(relations))}，"
            f"实现多 {sorted(set(relations) - declared_relations)}）"
        )
    else:
        ok(f"orm-mapping.md 关系表与实现一致（{len(declared_relations)} 条 relationship）")

    repository_block = text.split(REPOSITORY_TABLE_START, 1)[1].split(REPOSITORY_TABLE_END, 1)[0]
    declared_repos = dict(REPOSITORY_ROW_RE.findall(repository_block))
    implemented = {
        repository.__name__: model.__name__ for model, repository in registry.items()
    }
    if declared_repos != implemented:
        fail(f"orm-mapping.md Repository 表与实现不一致：文档 {declared_repos} vs 实现 {implemented}")
    else:
        ok(f"orm-mapping.md Repository 表与实现一致（{len(declared_repos)} 个 Repository）")

    # ---- 校验 13b：Repository「专属方法」列 ↔ 实现双向一致 ----
    from inspect import iscoroutinefunction, isfunction

    method_rows = REPOSITORY_METHOD_ROW_RE.findall(repository_block)
    documented_by_model: dict[str, set[str]] = {}
    for _name, model_name, methods_cell in method_rows:
        tokens = {
            token
            for token in METHOD_TOKEN_RE.findall(methods_cell)
            if not token.startswith(INDEX_NAME_PREFIXES)
        }
        documented_by_model[model_name] = tokens

    if len(method_rows) != len(registry):
        fail(f"orm-mapping.md Repository 行数 {len(method_rows)} != 注册表 {len(registry)}")
    else:
        problems: list[str] = []
        for model, repository in registry.items():
            documented = documented_by_model.get(model.__name__)
            if documented is None:
                problems.append(f"{model.__name__} 未登记专属方法列")
                continue
            implemented_methods = {
                name
                for name, value in vars(repository).items()
                if not name.startswith("_") and (isfunction(value) or iscoroutinefunction(value))
            }
            if documented - implemented_methods:
                problems.append(
                    f"{repository.__name__} 契约声明但未实现 {sorted(documented - implemented_methods)}"
                )
            if implemented_methods - documented:
                problems.append(
                    f"{repository.__name__} 已实现但未登记契约 {sorted(implemented_methods - documented)}"
                )
        if problems:
            fail("Repository 专属方法 ↔ 契约不一致：" + "；".join(problems))
        else:
            ok(
                f"Repository 专属方法与契约双向一致（{len(method_rows)} 个 Repository / "
                f"{sum(len(v) for v in documented_by_model.values())} 个专属方法）"
            )


def load_yaml(path: Path) -> dict[str, Any]:
    """读取 YAML 契约文件。"""
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def marked_block(text: str, start: str, end: str) -> str | None:
    """截取 ``<!-- xxx:start -->`` 与 ``<!-- xxx:end -->`` 之间的区块（缺失返回 None）。"""
    if start not in text or end not in text:
        return None
    return text.split(start, 1)[1].split(end, 1)[0]


def parse_service_repository_allowlist() -> dict[str, set[str]]:
    """解析 orm-mapping.md 第 3.5 节的服务 → 共享 Repository 白名单。"""
    text = ORM_MAPPING_CONTRACT.read_text(encoding="utf-8")
    block = marked_block(text, SERVICE_REPOSITORY_TABLE_START, SERVICE_REPOSITORY_TABLE_END)
    if block is None:
        return {}
    allowlist: dict[str, set[str]] = {}
    for service, names in SERVICE_REPOSITORY_ROW_RE.findall(block):
        if service in {"服务", "service"}:
            continue
        allowlist[service] = set(REPOSITORY_CLASS_RE.findall(names))
    return allowlist


def collect_service_repository_usage() -> dict[str, set[str]]:
    """扫描各服务源码，收集从 ``hunter_common.database`` 导入的 Repository 类名。"""
    usage: dict[str, set[str]] = {}
    for service_dir in sorted(path for path in SERVICES_DIR.iterdir() if path.is_dir()):
        used: set[str] = set()
        for source_path in service_dir.rglob("*.py"):
            source = source_path.read_text(encoding="utf-8")
            for match in SHARED_REPOSITORY_IMPORT_RE.finditer(source):
                chunk = match.group(1)
                if chunk.strip().startswith("("):
                    end = source.find(")", match.end())
                    chunk = source[match.end() : end] if end != -1 else chunk
                used.update(REPOSITORY_CLASS_RE.findall(chunk))
        usage[service_dir.name] = used
    return usage


def check_service_repository_boundaries() -> None:
    """校验 14：服务 → 共享 Repository 白名单（防跨服务直查其他服务 schema）。"""
    allowlist = parse_service_repository_allowlist()
    if not allowlist:
        fail("orm-mapping.md 缺少「服务 → Repository 白名单」表（第 3.5 节区块标记缺失）")
        return

    problems: list[str] = []
    usage = collect_service_repository_usage()
    for service, used in sorted(usage.items()):
        allowed = allowlist.get(service)
        if allowed is None:
            problems.append(f"{service} 未登记白名单")
            continue
        unauthorized = used - allowed
        if unauthorized:
            problems.append(f"{service} 使用了未授权 Repository {sorted(unauthorized)}")

    if problems:
        fail("服务 → Repository 边界越界：" + "；".join(problems))
    else:
        ok(
            f"服务 → Repository 白名单一致（{len(usage)} 个服务，越界 0 处；"
            f"当前共享层实际被引用 {sum(len(v) for v in usage.values())} 处）"
        )


def check_write_path_and_window_contract() -> None:
    """校验 15：写路径纪律与序列读取窗口规则必须在契约中显式声明（防文档静默丢失约束）。"""
    text = ORM_MAPPING_CONTRACT.read_text(encoding="utf-8")
    problems: list[str] = []
    for name, start, end, anchors in (
        ("写路径纪律", WRITE_PATH_DISCIPLINE_START, WRITE_PATH_DISCIPLINE_END, WRITE_PATH_ANCHORS),
        ("序列读取窗口", SERIES_WINDOW_RULE_START, SERIES_WINDOW_RULE_END, SERIES_WINDOW_ANCHORS),
    ):
        block = marked_block(text, start, end)
        if block is None:
            problems.append(f"{name} 区块标记缺失")
            continue
        missing = [anchor for anchor in anchors if anchor not in block]
        if missing:
            problems.append(f"{name} 区块缺少关键锚点 {missing}")

    if problems:
        fail("契约声明不完整：" + "；".join(problems))
    else:
        ok("写路径纪律（SAVEPOINT 顺序 / 禁止 expunge）与序列读取窗口规则均已在契约显式声明")


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

    # Schema 引用：全部平台内部/车端 Topic 必须已定义 Schema（tbd 集合为空）
    # （analytics_result 见 scene-service 契约补全；sensor_file 由 data-collector 契约 5.5 节补全；
    #   alert_event 由 data-analytics 契约 6.2 节补全）
    tbd_allowed: set[str] = set()
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
        "analytics_result.schema.json",
        "sensor_file.schema.json",
        "alert_event.schema.json",
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


def normalize_redis_declarations(node: Any) -> tuple[list[dict[str, Any]], list[str]]:
    """归一化 ``x-hunter-service.redis_keys`` 声明（返回（字段可比对声明, 宽松字符串声明））。

    兼容两种既有形态（结构统一列为 redis-keys.yaml 待确认 #8）：
    1) 对象列表：``[{pattern, type, ttl_seconds, usage}, ...]``
    2) 只读映射：``{note: ..., read: ["vehicle:status:{vehicle_id}", ...]}``
    """
    if isinstance(node, list):
        return [item for item in node if isinstance(item, dict) and item.get("pattern")], []
    if isinstance(node, dict):
        entries: list[dict[str, Any]] = []
        loose: list[str] = []
        for section in ("read", "write"):
            for item in node.get(section) or []:
                if isinstance(item, str):
                    loose.append(item)
                elif isinstance(item, dict) and item.get("pattern"):
                    entries.append(item)
        return entries, loose
    return [], []


def check_redis_keys() -> None:
    """校验 9：Redis 键契约 ↔ 各服务 OpenAPI 声明（键模式 / 类型 / TTL / 读写方归属）。"""
    contract = load_yaml(REDIS_CONTRACT)
    before = len(failures)

    keys: dict[str, dict[str, Any]] = {
        str(entry.get("pattern")): entry for entry in contract.get("keys") or []
    }
    if set(keys) != set(EXPECTED_REDIS_KEYS):
        fail(
            f"Redis 键契约清单不一致: 契约={sorted(keys)} 期望={sorted(EXPECTED_REDIS_KEYS)}"
            "（新增/删除键模式必须先改系统约束第 8 条）"
        )
    for pattern, (expected_type, expected_ttl) in EXPECTED_REDIS_KEYS.items():
        entry = keys.get(pattern)
        if entry is None:
            continue
        if entry.get("type") != expected_type:
            fail(f"Redis 键 {pattern}: type={entry.get('type')} != {expected_type}")
        if entry.get("ttl_seconds") != expected_ttl:
            fail(f"Redis 键 {pattern}: ttl_seconds={entry.get('ttl_seconds')} != {expected_ttl}")
        if not str(entry.get("lifecycle") or "").strip():
            fail(f"Redis 键 {pattern}: 必须显式声明失效路径（lifecycle）")
        if expected_ttl is None:
            ops = " ".join(str(op) for op in entry.get("ops") or [])
            if "DEL" not in ops and "SREM" not in ops:
                fail(f"Redis 键 {pattern}: 无 TTL 时必须声明主动清理（ops 需含 DEL / SREM）")
        if not (entry.get("writers") and entry.get("readers")):
            fail(f"Redis 键 {pattern}: writers / readers 不能为空（键归属必须明确）")
        if not REDIS_KEY_NAMING_RE.match(pattern):
            fail(f"Redis 键命名不符 <domain>:<entity>[:<qualifier>] 规范（全小写冒号分隔）: {pattern}")
        owner = entry.get("owner_service")
        allowed = set(entry.get("writers") or []) | set(entry.get("readers") or [])
        if owner is not None and owner not in allowed:
            fail(f"Redis 键 {pattern}: owner_service={owner} 既非 writers 也非 readers")

    # 各服务契约声明 ↔ 本契约（cross_check.contracts 为声明文件清单的单一事实来源）
    declared_files = [
        str(rel) for rel in (contract.get("cross_check") or {}).get("contracts") or []
    ]
    for rel in declared_files:
        path = ROOT / rel
        if not path.is_file():
            fail(f"Redis 契约 cross_check 引用的契约文件不存在: {rel}")
            continue
        service = path.stem
        node = (load_yaml(path).get("x-hunter-service") or {}).get("redis_keys")
        entries, loose = normalize_redis_declarations(node)
        for pattern in loose + [str(entry.get("pattern")) for entry in entries]:
            target = keys.get(pattern)
            if target is None:
                fail(f"{service}: 声明了契约未登记的 Redis 键 {pattern}")
                continue
            allowed = set(target.get("writers") or []) | set(target.get("readers") or [])
            if service not in allowed:
                fail(f"{service}: 声明使用 {pattern} 但不在契约 writers ∪ readers 中")
        for entry in entries:
            target = keys.get(str(entry.get("pattern")))
            if target is None:
                continue
            if entry.get("type") is not None and entry.get("type") != target.get("type"):
                fail(
                    f"{service}: {entry['pattern']} 类型 {entry.get('type')} "
                    f"!= 契约 {target.get('type')}"
                )
            if "ttl_seconds" in entry and entry.get("ttl_seconds") != target.get("ttl_seconds"):
                fail(
                    f"{service}: {entry['pattern']} ttl_seconds={entry.get('ttl_seconds')} "
                    f"!= 契约 {target.get('ttl_seconds')}"
                )

    if len(failures) == before:
        ok(
            f"Redis 键契约一致（{len(keys)} 个受控键 ↔ {len(declared_files)} 份服务契约声明，"
            "键模式 / 类型 / TTL / 读写方全部匹配）"
        )


def expected_expiry_rules() -> dict[str, list[tuple[int, str | None]]]:
    """由契约基准值推导 MinIO 生命周期期望（bucket → [(过期天数, 前缀), ...]）。"""
    rules: dict[str, list[tuple[int, str | None]]] = {
        name: ([] if days is None else [(days, None)]) for name, days in EXPECTED_BUCKETS.items()
    }
    # hunter-rosbag 为前缀级规则（events/ 无过期规则 → 永久）
    rules["hunter-rosbag"] = [
        (days, prefix) for prefix, days in EXPECTED_ROSBAG_PREFIX_DAYS.items() if days is not None
    ]
    return rules


def collect_presign_ttls(node: Any, in_presign: bool = False) -> list[int]:
    """递归收集契约中的预签名有效期声明（预签名上下文 + 键名含 expire 的整数值）。"""
    found: list[int] = []
    if isinstance(node, dict):
        for key, value in node.items():
            name = str(key).lower()
            context = in_presign or "presign" in name
            if isinstance(value, bool):
                continue
            if isinstance(value, int) and context and "expire" in name:
                found.append(value)
            elif isinstance(value, (dict, list)):
                found.extend(collect_presign_ttls(value, context))
    elif isinstance(node, list):
        for item in node:
            found.extend(collect_presign_ttls(item, in_presign))
    return found


def check_object_storage() -> None:
    """校验 10：对象存储契约 ↔ MinIO 初始化脚本 ↔ 各服务契约（Bucket/生命周期/预签名/SSE）。"""
    contract = load_yaml(OBJECT_STORAGE_CONTRACT)
    before = len(failures)

    buckets: dict[str, dict[str, Any]] = {
        str(entry.get("name")): entry for entry in contract.get("buckets") or []
    }
    if set(buckets) != set(EXPECTED_BUCKETS):
        fail(
            f"MinIO Bucket 契约清单不一致: 契约={sorted(buckets)} 期望={sorted(EXPECTED_BUCKETS)}"
            "（Bucket 名称不可更改，也不可新增）"
        )
    for name, expected_days in EXPECTED_BUCKETS.items():
        entry = buckets.get(name)
        if entry is None:
            continue
        lifecycle = entry.get("lifecycle") or {}
        mode = lifecycle.get("mode")
        if mode == "expire":
            if lifecycle.get("expire_days") != expected_days:
                fail(
                    f"Bucket {name}: expire_days={lifecycle.get('expire_days')} != {expected_days}"
                )
        elif mode == "permanent":
            if expected_days is not None or lifecycle.get("expire_days") is not None:
                fail(f"Bucket {name}: 永久保留但声明 expire_days={lifecycle.get('expire_days')}")
        elif mode == "mixed":
            rules = {
                str(rule.get("prefix")): rule.get("expire_days")
                for rule in lifecycle.get("rules") or []
            }
            if rules != EXPECTED_ROSBAG_PREFIX_DAYS:
                fail(f"Bucket {name}: 前缀生命周期 {rules} != {EXPECTED_ROSBAG_PREFIX_DAYS}")
        else:
            fail(f"Bucket {name}: 非法生命周期 mode={mode}")
        if not (entry.get("writers") and entry.get("readers")):
            fail(f"Bucket {name}: writers / readers 不能为空")
        if entry.get("sse") != "sse-s3":
            fail(f"Bucket {name}: 必须启用 SSE-S3 服务端加密（数据安全约束）")

    presign = contract.get("presign_policy") or {}
    for field, expected in EXPECTED_PRESIGN_TTL.items():
        if presign.get(field) != expected:
            fail(f"预签名策略 {field}={presign.get(field)} != {expected}")
    if presign.get("range_download") is not True:
        fail("预签名策略必须支持 Range 分片下载（大文件断点续传）")
    if presign.get("url_logging") != "forbidden":
        fail("预签名 URL 禁止落日志（presign_policy.url_logging 必须为 forbidden）")

    # MinIO 初始化脚本（docker 与 K8s Job 两处必须与契约一致）
    expected_rules = expected_expiry_rules()
    for script in (MINIO_DOCKER_INIT, MINIO_K8S_INIT):
        rel = script.relative_to(ROOT).as_posix()
        if not script.is_file():
            fail(f"缺少 MinIO 初始化脚本: {rel}")
            continue
        text = script.read_text(encoding="utf-8")
        created = MINIO_CREATE_BUCKET_RE.findall(text)
        if set(created) != set(EXPECTED_BUCKETS) or len(created) != len(EXPECTED_BUCKETS):
            fail(
                f"{rel}: create_bucket 集合 {sorted(set(created))} != 契约 {sorted(EXPECTED_BUCKETS)}"
            )
        actual: dict[str, list[tuple[int, str | None]]] = {}
        for bucket, days, prefix in MINIO_ADD_EXPIRY_RE.findall(text):
            actual.setdefault(bucket, []).append((int(days), prefix or None))
        for bucket, rules in expected_rules.items():
            if sorted(actual.get(bucket, [])) != sorted(rules):
                fail(
                    f"{rel}: {bucket} 生命周期 {sorted(actual.get(bucket, []))} "
                    f"!= 契约 {sorted(rules)}"
                )
        for bucket in actual:
            if bucket not in EXPECTED_BUCKETS:
                fail(f"{rel}: 为未登记 Bucket {bucket} 配置了生命周期规则")

    k8s_text = MINIO_K8S_INIT.read_text(encoding="utf-8") if MINIO_K8S_INIT.is_file() else ""
    encrypted = set(MINIO_ENCRYPT_BUCKET_RE.findall(k8s_text))
    if encrypted != set(EXPECTED_BUCKETS):
        fail(f"minio-init-job.yaml: SSE-S3 未覆盖全部 Bucket: {sorted(encrypted)}")

    # 各服务契约声明 ↔ 本契约（cross_check.service_contracts 为声明文件清单）
    service_contracts = [
        str(rel) for rel in (contract.get("cross_check") or {}).get("service_contracts") or []
    ]
    for rel in service_contracts:
        path = ROOT / rel
        if not path.is_file():
            fail(f"对象存储契约 cross_check 引用的契约文件不存在: {rel}")
            continue
        service = path.stem
        doc = load_yaml(path)
        node = doc.get("x-hunter-service") or {}
        for bucket in node.get("minio_buckets") or []:
            entry = buckets.get(str(bucket))
            if entry is None:
                fail(f"{service}: 声明了契约未登记的 Bucket {bucket}（Bucket 名称不可更改）")
                continue
            allowed = set(entry.get("writers") or []) | set(entry.get("readers") or [])
            if service not in allowed:
                fail(f"{service}: 声明使用 {bucket} 但不在契约 writers ∪ readers 中")
        for bucket, text in (node.get("minio_bucket_lifecycle") or {}).items():
            if str(bucket) not in EXPECTED_BUCKETS:
                continue
            declared_days = [int(value) for value in re.findall(r"(\d+)\s*天", str(text))]
            expected_days = EXPECTED_BUCKETS[str(bucket)]
            if expected_days is None:
                if declared_days:
                    fail(f"{service}: {bucket} 声明 {declared_days} 天，与契约「永久保留」不一致")
            elif expected_days not in declared_days:
                fail(
                    f"{service}: {bucket} 生命周期未声明 {expected_days} 天"
                    f"（文本：{str(text)[:40]}...）"
                )
        # 预签名 TTL（契约 cross_check 第 3 条：上传 3600s / 下载 900s）
        raw_text = path.read_text(encoding="utf-8")
        ttls = set(collect_presign_ttls(doc)) | {
            int(value) for value in PRESIGN_CONFIG_TTL_RE.findall(raw_text)
        }
        required = EXPECTED_SERVICE_PRESIGN_TTL.get(service)
        if required is not None:
            missing = sorted(required - ttls)
            if missing:
                fail(f"{service}: 未声明预签名 TTL {missing}（契约：上传 3600s / 下载 900s）")
        unexpected = sorted(ttls - set(EXPECTED_PRESIGN_TTL.values()))
        if unexpected:
            fail(
                f"{service}: 预签名 TTL {unexpected} 不属于契约允许值 "
                f"{sorted(set(EXPECTED_PRESIGN_TTL.values()))}"
            )

    if len(failures) == before:
        ok(
            f"对象存储契约一致（{len(buckets)} 个 Bucket ↔ 2 份初始化脚本 ↔ "
            f"{len(service_contracts)} 份服务契约：生命周期 / 预签名 TTL / 读写方全部匹配）"
        )


def main() -> int:
    # Windows 控制台默认 GBK：显式以 UTF-8 输出，避免契约符号（↔ / ⚠）编码失败
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            reconfigure(encoding="utf-8", errors="replace")

    print("HunterCore 数据层契约校验（L2）")
    print("=" * 78)
    check_contract_files()
    ddl, _orm = check_ddl_orm_parity()
    check_enum_contract(ddl)
    check_hypertable_contract()
    check_alembic_offline_sql()
    check_orm_repository_layer()
    check_service_repository_boundaries()
    check_write_path_and_window_contract()
    check_kafka_topics()
    check_json_schemas()
    check_consumer_groups()
    check_redis_keys()
    check_object_storage()
    print("-" * 78)
    for line in checks:
        print(line)
    print("-" * 78)
    passed = len(checks) - len(failures)
    print(f"结果：通过 {passed} 项，失败 {len(failures)} 项")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
