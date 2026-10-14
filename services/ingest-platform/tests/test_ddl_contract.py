"""ingest_platform DDL ↔ ORM 契约校验（06_ingest_platform.sql 为单一事实来源）。

共享库 common/python/tests/test_database_models.py 的表级校验覆盖
hunter_common 归属的 schema；ingest_platform 的 ORM 模型归属服务侧，
其 DDL↔ORM 一致性在此校验。
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from app.models.ingest import (
    Agent,
    AgentAlert,
    Artifact,
    ArtifactClass,
    Bag,
    BagState,
    BagTopic,
    Clip,
    ClipSource,
    CmdAckStatus,
    Command,
    Dataset,
    DerivedAsset,
    DerivedKind,
    IngestEvent,
    Label,
    ProjectionAnomaly,
    Task,
    TaskState,
    TaskType,
    UploadState,
)

DDL_PATH = (
    Path(__file__).resolve().parents[3] / "contracts" / "database" / "ddl" / "06_ingest_platform.sql"
)

CREATE_TABLE_RE = re.compile(
    r"CREATE TABLE (?:IF NOT EXISTS )?([\w.]+)\s*\((.*?)\n\)\s*(?:PARTITION BY[^;]*)?;",
    re.DOTALL,
)
CREATE_TYPE_RE = re.compile(
    r"CREATE\s+TYPE\s+ingest_platform\.(\w+)\s+AS\s+ENUM", re.IGNORECASE
)
CHECK_ENUM_RE = re.compile(r"CHECK\s*\(\s*(\w+)\s+IN\s*\((.*?)\)", re.DOTALL)
QUOTED_RE = re.compile(r"'([^']*)'")

ORM_MODELS = [
    IngestEvent, Bag, BagTopic, Artifact, DerivedAsset, Task,
    Clip, Label, Agent, AgentAlert, Command, ProjectionAnomaly, Dataset,
]

# 枚举列的 TEXT+CHECK 表示：(表名, 列名) → StrEnum（V1.20.5，与 StrEnumType(impl=Text) 对齐）
ENUM_CHECK_MAP: dict[tuple[str, str], type] = {
    ("bags", "state"): BagState,
    ("tasks", "state"): TaskState,
    ("tasks", "task_type"): TaskType,
    ("artifacts", "artifact_class"): ArtifactClass,
    ("artifacts", "upload_state"): UploadState,
    ("derived_assets", "kind"): DerivedKind,
    ("commands", "ack_status"): CmdAckStatus,
    ("clips", "source"): ClipSource,
}


def _ddl_text() -> str:
    assert DDL_PATH.is_file(), f"缺失 DDL 契约文件: {DDL_PATH}"
    return DDL_PATH.read_text(encoding="utf-8")


def _ddl_tables() -> dict[str, set[str]]:
    """DDL 表 → 列名集合（列定义固定 4 空格缩进；约束行按关键字跳过）。"""
    tables: dict[str, set[str]] = {}
    for match in CREATE_TABLE_RE.finditer(_ddl_text()):
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
        tables[match.group(1).lower()] = columns
    return tables


def test_ingest_ddl_tables_match_orm() -> None:
    """DDL 表清单与 ORM 模型一一对应（ingest_platform schema 内）。"""
    ddl_fullnames = set(_ddl_tables())
    orm_fullnames = {m.__table__.fullname for m in ORM_MODELS}
    assert ddl_fullnames == orm_fullnames, (
        f"DDL 多出: {sorted(ddl_fullnames - orm_fullnames)} / "
        f"ORM 多出: {sorted(orm_fullnames - ddl_fullnames)}"
    )


@pytest.mark.parametrize("model", ORM_MODELS, ids=lambda m: m.__tablename__)
def test_ingest_ddl_columns_match_orm(model) -> None:
    """逐表列名一致（缺列/多列都会失败）。"""
    tables = _ddl_tables()
    fullname = model.__table__.fullname
    ddl_cols = tables[fullname]
    orm_cols = {c.name for c in model.__table__.columns}
    assert ddl_cols == orm_cols, (
        f"{fullname} DDL 多出: {sorted(ddl_cols - orm_cols)} / "
        f"ORM 多出: {sorted(orm_cols - ddl_cols)}"
    )


def _ddl_enum_checks() -> dict[tuple[str, str], set[str]]:
    """从各 CREATE TABLE 主体解析 CHECK (col IN (...)) → {(table, column): 取值集合}。"""
    result: dict[tuple[str, str], set[str]] = {}
    for match in CREATE_TABLE_RE.finditer(_ddl_text()):
        table = match.group(1).lower().split(".")[-1]
        for column, values in CHECK_ENUM_RE.findall(match.group(2)):
            result[(table, column.lower())] = set(QUOTED_RE.findall(values))
    return result


def test_ingest_ddl_no_native_enum_types() -> None:
    """回归护栏：ingest DDL 禁止使用 PG 原生 CREATE TYPE ... AS ENUM。

    原生枚举会使物理列类型为 ingest_platform.<type>，而 ORM 以 StrEnumType(impl=Text)
    绑定比较参数，asyncpg 下报 UndefinedFunctionError: operator does not exist:
    ingest_platform.<type> = character varying（仅真实 PostgreSQL 复现，SQLite 单测不暴露）。
    受控枚举必须用 TEXT + CHECK (col IN (...)) 表示（与 01~05 schema 一致）。
    """
    assert not CREATE_TYPE_RE.search(_ddl_text()), (
        "06_ingest_platform.sql 不得声明原生枚举类型，应改用 TEXT + CHECK"
    )


def test_ingest_ddl_enum_values_match_orm() -> None:
    """DDL 枚举列 CHECK (col IN (...)) 取值与 ORM StrEnum 完全一致（受控词表，contracts/database/enums.md）。"""
    ddl_checks = _ddl_enum_checks()
    for (table, column), enum_cls in ENUM_CHECK_MAP.items():
        key = (table, column)
        assert key in ddl_checks, (
            f"DDL 缺失枚举 CHECK：ingest_platform.{table}.{column}（对应 {enum_cls.__name__}）"
        )
        orm_values = {e.value for e in enum_cls}
        assert ddl_checks[key] == orm_values, (
            f"{table}.{column} 取值不一致：DDL 多出 {sorted(ddl_checks[key] - orm_values)} / "
            f"ORM 多出 {sorted(orm_values - ddl_checks[key])}"
        )
