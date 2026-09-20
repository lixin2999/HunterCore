"""ORM 模型 ↔ 数据库 DDL 契约一致性测试。

本测试使用**独立实现**的轻量 DDL 解析（不复用 scripts/verify_data_layer.py），
形成交叉校验：契约文件一旦与 ORM 模型脱节即失败。
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest
from sqlalchemy.dialects import postgresql

from hunter_common.database import (
    ALL_SCHEMAS,
    CHUNK_TIME_INTERVAL,
    HYPERTABLES,
    RETENTION_INTERVAL,
    supports_soft_delete,
)
from hunter_common.database.base import Base, StrEnumType
from hunter_common.database.enums import (
    EVENT_LEVEL_BY_TYPE,
    EventLevel,
    EventType,
    MetricModule,
    OtaStatus,
    SceneStatus,
    VehicleStatus,
)
from hunter_common.database.models import ALL_MODELS, Scene, Vehicle

import hunter_common.database.models  # noqa: F401  (register models)  # isort: skip

ROOT = Path(__file__).resolve().parents[3]
DDL_DIR = ROOT / "contracts" / "database" / "ddl"

CREATE_TABLE_RE = re.compile(
    r"CREATE TABLE\s+(?:IF NOT EXISTS\s+)?([A-Za-z_][\w.]*)\s*\((.*?)\n\);", re.DOTALL
)
CHECK_IN_RE = re.compile(r"([a-z_][\w]*)\s+IN\s*\(([^)]*)\)", re.IGNORECASE)
QUOTED_RE = re.compile(r"'([^']*)'")

EXPECTED_TABLES: set[str] = {
    "vehicle_svc.vehicles",
    "user_svc.users",
    "user_svc.roles",
    "user_svc.permissions",
    "user_svc.user_roles",
    "user_svc.role_permissions",
    "scene_svc.scenes",
    "ota_svc.ota_versions",
    "ota_svc.ota_tasks",
    "ota_svc.ota_records",
    "data_collector.events",
    "data_collector.vehicle_telemetry",
    "data_analytics.algorithm_metrics",
}


def ddl_tables() -> dict[str, set[str]]:
    """提取 DDL 中每张表的列名（列定义固定 4 空格缩进，续行缩进更深）。"""
    tables: dict[str, set[str]] = {}
    for path in sorted(DDL_DIR.glob("*.sql")):
        text = path.read_text(encoding="utf-8")
        for match in CREATE_TABLE_RE.finditer(text):
            columns: set[str] = set()
            for line in match.group(2).splitlines():
                if not line.startswith("    ") or line.startswith("     "):
                    continue
                stripped = line.strip()
                if not stripped or stripped.startswith("--"):
                    continue
                if re.match(r"(PRIMARY KEY|CHECK|CONSTRAINT|FOREIGN|UNIQUE)\b", stripped, re.IGNORECASE):
                    continue
                columns.add(stripped.split()[0].lower())
            tables[match.group(1)] = columns
    return tables


def ddl_enum_values(table: str, column: str) -> set[str]:
    """提取指定列在 DDL 中 CHECK (col IN (...)) 的取值集合。"""
    for path in sorted(DDL_DIR.glob("*.sql")):
        text = path.read_text(encoding="utf-8")
        for match in CREATE_TABLE_RE.finditer(text):
            if match.group(1) != table:
                continue
            for name, values in CHECK_IN_RE.findall(match.group(2)):
                if name.lower() == column:
                    return set(QUOTED_RE.findall(values))
    raise AssertionError(f"DDL 未找到 {table}.{column} 的 CHECK 约束")


def test_contract_files_exist() -> None:
    for name in (
        "00_schemas.sql",
        "01_core.sql",
        "02_scene.sql",
        "03_ota.sql",
        "04_events.sql",
        "05_timeseries.sql",
    ):
        assert (DDL_DIR / name).is_file(), f"缺失 DDL 契约文件: {name}"
    for name in ("er.md", "enums.md", "README.md"):
        assert (DDL_DIR.parent / name).is_file(), f"缺失数据库契约文档: {name}"


def test_orm_tables_match_contract() -> None:
    """ORM metadata 与契约表清单一致（含 schema 归属）。"""
    tables = set(Base.metadata.tables)
    assert tables == EXPECTED_TABLES
    assert tables == {model.__table__.fullname for model in ALL_MODELS}
    schemas_sql = (DDL_DIR / "00_schemas.sql").read_text(encoding="utf-8")
    for schema in ALL_SCHEMAS:
        assert f"CREATE SCHEMA IF NOT EXISTS {schema}" in schemas_sql


def test_ddl_column_parity_with_orm() -> None:
    """DDL ↔ ORM 列名逐表一致（缺列/多列都会失败）。"""
    parsed = ddl_tables()
    assert set(parsed) == EXPECTED_TABLES, "DDL 表清单与契约不一致"
    for fq_name, table in Base.metadata.tables.items():
        assert parsed[fq_name] == {column.name for column in table.columns}, f"{fq_name} 列不一致"


def test_primary_keys_match_orm() -> None:
    expected: dict[str, set[str]] = {
        "vehicle_svc.vehicles": {"vehicle_id"},
        "user_svc.user_roles": {"user_id", "role_id"},
        "user_svc.role_permissions": {"role_id", "permission_id"},
        "data_collector.vehicle_telemetry": {"time", "vehicle_id"},
        "data_analytics.algorithm_metrics": {"time", "vehicle_id", "module", "metric_name"},
        "ota_svc.ota_records": {"record_id"},
        "data_collector.events": {"event_id"},
    }
    for fq_name, pk_columns in expected.items():
        table = Base.metadata.tables[fq_name]
        assert {column.name for column in table.primary_key.columns} == pk_columns, fq_name


def test_orm_column_types_compile_to_contract_types() -> None:
    """ORM 列类型编译结果必须与 DDL 契约类型一致（抽样覆盖各类）。"""
    dialect = postgresql.dialect()
    expected: dict[tuple[str, str], str] = {
        ("vehicle_svc.vehicles", "vehicle_id"): "TEXT",
        ("vehicle_svc.vehicles", "status"): "TEXT",
        ("user_svc.users", "user_id"): "UUID",
        ("user_svc.users", "create_time"): "TIMESTAMP WITH TIME ZONE",
        ("scene_svc.scenes", "config_json"): "JSONB",
        ("scene_svc.scenes", "tags"): "TEXT[]",
        ("scene_svc.scenes", "deleted_at"): "TIMESTAMP WITH TIME ZONE",
        ("ota_svc.ota_versions", "package_md5"): "CHAR(32)",
        ("ota_svc.ota_versions", "package_sha256"): "CHAR(64)",
        ("ota_svc.ota_versions", "package_size"): "BIGINT",
        ("ota_svc.ota_versions", "applicable_models"): "TEXT[]",
        ("ota_svc.ota_records", "record_id"): "BIGINT",
        ("ota_svc.ota_records", "progress"): "SMALLINT",
        ("data_collector.vehicle_telemetry", "motor_rpm"): "INTEGER[]",
        ("data_collector.vehicle_telemetry", "angular_velocity"): "DOUBLE PRECISION[]",
        ("data_collector.vehicle_telemetry", "battery_soc"): "SMALLINT",
        ("data_collector.events", "acknowledged"): "BOOLEAN",
        ("data_analytics.algorithm_metrics", "metric_value"): "DOUBLE PRECISION",
    }
    for (fq_name, column), expected_type in expected.items():
        compiled = Base.metadata.tables[fq_name].columns[column].type.compile(dialect=dialect)
        assert compiled.upper() == expected_type, f"{fq_name}.{column} -> {compiled}"


def test_enum_check_values_match_python_enums() -> None:
    """受控词表：DDL CHECK 取值 ↔ Python StrEnum 完全一致。"""
    assert ddl_enum_values("vehicle_svc.vehicles", "status") == {v.value for v in VehicleStatus}
    assert ddl_enum_values("scene_svc.scenes", "status") == {s.value for s in SceneStatus}
    assert ddl_enum_values("data_collector.events", "event_type") == {t.value for t in EventType}
    assert len(EventType) == 19, "事件类型必须为 19 种（阈值不可更改；G-22② 新增 collision_pre_warning）"
    assert ddl_enum_values("data_collector.events", "event_level") == {e.value for e in EventLevel}
    assert ddl_enum_values("ota_svc.ota_records", "status") == {s.value for s in OtaStatus}
    assert ddl_enum_values("ota_svc.ota_records", "phase") == {s.value for s in OtaStatus}
    assert ddl_enum_values("data_analytics.algorithm_metrics", "module") == {
        m.value for m in MetricModule
    }


def test_event_level_mapping_is_complete_and_consistent() -> None:
    """事件类型 → 等级映射完整，且事件等级均在受控词表内。"""
    assert set(EVENT_LEVEL_BY_TYPE) == {t.value for t in EventType}
    assert EVENT_LEVEL_BY_TYPE[EventType.OVER_SPEED] is EventLevel.CRITICAL
    assert EVENT_LEVEL_BY_TYPE[EventType.BATTERY_LOW] is EventLevel.WARNING
    assert EVENT_LEVEL_BY_TYPE[EventType.MANUAL_TAKEOVER] is EventLevel.INFO


def test_soft_delete_capability_is_declared_per_model() -> None:
    """仅 scenes 具备软删除（deleted_at）；其他表用状态字段表达生命周期。"""
    assert supports_soft_delete(Scene) is True
    assert supports_soft_delete(Vehicle) is False
    instance = Scene(
        scene_name="crossing-01",
        scene_type="urban",
        creator="00000000-0000-0000-0000-000000000001",
    )
    assert instance.is_deleted is False


def test_hypertable_contract_constants() -> None:
    """hypertable 契约：按天分块 + 90 天保留，且主键含分区键 time。"""
    assert set(HYPERTABLES) == {
        "data_collector.vehicle_telemetry",
        "data_analytics.algorithm_metrics",
    }
    assert CHUNK_TIME_INTERVAL == "1 day"
    assert RETENTION_INTERVAL == "90 days"
    for fq_name in HYPERTABLES:
        pk_columns = {column.name for column in Base.metadata.tables[fq_name].primary_key.columns}
        assert "time" in pk_columns, f"{fq_name} 主键必须包含分区键 time"


def test_str_enum_type_binds_and_validates() -> None:
    """StrEnumType：合法值转字符串落库，非法值抛 ValueError，读取还原枚举成员。"""
    column_type = StrEnumType(VehicleStatus)
    assert column_type.process_bind_param(VehicleStatus.AUTO_DRIVING, None) == "auto_driving"
    assert column_type.process_bind_param("fault", None) == "fault"
    assert column_type.process_bind_param(None, None) is None
    with pytest.raises(ValueError, match="VehicleStatus"):
        column_type.process_bind_param("taking_off", None)
    assert column_type.process_result_value("emergency", None) is VehicleStatus.EMERGENCY
    assert column_type.process_result_value(None, None) is None


def test_ota_state_machine_constants() -> None:
    """OTA 状态机：终态/进行中状态集合与设计文档一致（9 态）。"""
    from hunter_common.database.enums import OTA_ACTIVE_STATUSES, OTA_TERMINAL_STATUSES

    assert len(OtaStatus) == 9
    assert OTA_TERMINAL_STATUSES == {
        OtaStatus.SUCCESS,
        OtaStatus.ROLLED_BACK,
        OtaStatus.FAILED,
    }
    assert OtaStatus.TEST in OTA_ACTIVE_STATUSES
    assert OtaStatus.IDLE not in OTA_ACTIVE_STATUSES


