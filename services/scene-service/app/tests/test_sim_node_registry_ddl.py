"""`scene_svc.sim_node_registry` DDL ↔ ORM 契约校验（08_sim_nodes.sql 为单一事实来源）。

边界：本表位于**共享 schema** `scene_svc`（与 scenes 同库同 schema），但 ORM 模型归属
scene-service 服务私有（`app/models/sim_node_registry.py`），**不入**共享包
`hunter_common.database.models.ALL_MODELS`。共享库测试
（`common/python/tests/test_database_models.py`）以 `SERVICE_OWNED_TABLES` 豁免本表的
DDL↔ORM 对账 —— 豁免与覆盖必须成对出现（该文件有对账文件存在性校验），否则会退化成
「共享侧不算、服务侧没人管」的假绿。逐列/类型/可空性/主键由本文件钉住。
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy.dialects import postgresql

from app.models.sim_node_registry import SimNodeRegistry

DDL_PATH = (
    Path(__file__).resolve().parents[4] / "contracts" / "database" / "ddl" / "08_sim_nodes.sql"
)

CREATE_TABLE_RE = re.compile(
    r"CREATE TABLE (?:IF NOT EXISTS )?(scene_svc\.sim_node_registry)\s*\((.*?)\n\)\s*;",
    re.DOTALL,
)
ALTER_ADD_COLUMN_RE = re.compile(
    r"ALTER TABLE scene_svc\.sim_node_registry(.*?);", re.DOTALL | re.IGNORECASE
)
ADD_COLUMN_LINE_RE = re.compile(
    r"ADD COLUMN IF NOT EXISTS\s+([a-z_][\w]*)", re.IGNORECASE
)
CONSTRAINT_HEAD_RE = re.compile(r"(PRIMARY KEY|CHECK|CONSTRAINT|FOREIGN|UNIQUE)\b", re.IGNORECASE)

#: DDL 类型简写 → SQLAlchemy 编译结果（本表仅三种，逐列对账用）
DDL_TYPE_CANON: dict[str, str] = {
    "TEXT": "TEXT",
    "TIMESTAMPTZ": "TIMESTAMP WITH TIME ZONE",
    "JSONB": "JSONB",
}

#: V1.24.2 一键开通增列（新建库走 CREATE TABLE、存量库走 ALTER，两条路径必须等价）
V1_24_2_COLUMNS: tuple[str, ...] = (
    "provision_status",
    "scram_username",
    "scram_credential_updated_at",
    "cert_not_after",
)

TABLE_FQ = "scene_svc.sim_node_registry"


def _ddl_text() -> str:
    assert DDL_PATH.is_file(), f"缺失 DDL 契约文件: {DDL_PATH}"
    return DDL_PATH.read_text(encoding="utf-8")


def _parse_columns() -> dict[str, dict[str, Any]]:
    """解析 CREATE TABLE 列定义 → ``{列名: {"type": 规范类型, "nullable": bool}}``。

    列定义固定 4 空格缩进（续行/注释缩进更深，一并跳过）；行尾 ``--`` 注释先剥离。
    """
    match = CREATE_TABLE_RE.search(_ddl_text())
    assert match is not None, f"08_sim_nodes.sql 未找到 {TABLE_FQ} 的 CREATE TABLE 语句"
    columns: dict[str, dict[str, Any]] = {}
    for raw_line in match.group(2).splitlines():
        if not raw_line.startswith("    ") or raw_line.startswith("     "):
            continue
        stripped = raw_line.split("--", 1)[0].strip().rstrip(",")
        if not stripped or CONSTRAINT_HEAD_RE.match(stripped):
            continue
        tokens = stripped.split()
        name = tokens[0].lower()
        ddl_type = tokens[1].upper()
        assert ddl_type in DDL_TYPE_CANON, f"{TABLE_FQ}.{name} 出现契约外类型: {ddl_type}"
        columns[name] = {
            "type": DDL_TYPE_CANON[ddl_type],
            "nullable": "NOT NULL" not in stripped.upper(),
        }
    return columns


def _orm_columns() -> dict[str, dict[str, Any]]:
    dialect = postgresql.dialect()
    return {
        column.name: {
            "type": column.type.compile(dialect=dialect).upper(),
            "nullable": bool(column.nullable),
        }
        for column in SimNodeRegistry.__table__.columns
    }


def test_registry_ddl_columns_match_orm() -> None:
    """列名逐表一致（缺列/多列都失败）。"""
    ddl_columns = set(_parse_columns())
    orm_columns = set(_orm_columns())
    assert ddl_columns == orm_columns, (
        f"{TABLE_FQ} DDL 多出: {sorted(ddl_columns - orm_columns)} / "
        f"ORM 多出: {sorted(orm_columns - ddl_columns)}"
    )


def test_registry_ddl_types_and_nullability_match_orm() -> None:
    """逐列类型与可空性一致（TIMESTAMPTZ ↔ DateTime(timezone=True) 等）。"""
    ddl_columns = _parse_columns()
    orm_columns = _orm_columns()
    for name in sorted(set(ddl_columns) & set(orm_columns)):
        assert ddl_columns[name] == orm_columns[name], (
            f"{TABLE_FQ}.{name} 不一致: DDL={ddl_columns[name]} ORM={orm_columns[name]}"
        )


def test_registry_primary_key_matches_orm() -> None:
    ddl_pk = {"node_id"}
    orm_pk = {column.name for column in SimNodeRegistry.__table__.primary_key.columns}
    assert orm_pk == ddl_pk, f"{TABLE_FQ} 主键不一致: DDL={sorted(ddl_pk)} ORM={sorted(orm_pk)}"


def test_registry_v1_24_2_columns_covered_by_both_paths() -> None:
    """增列必须同时出现在 CREATE TABLE 与幂等 ALTER 中（新库/存量库两路径等价）。"""
    create_columns = set(_parse_columns())
    alter_match = ALTER_ADD_COLUMN_RE.search(_ddl_text())
    assert alter_match is not None, f"{TABLE_FQ} 缺 V1.24.2 幂等增列 ALTER 语句"
    alter_columns = {name.lower() for name in ADD_COLUMN_LINE_RE.findall(alter_match.group(1))}
    for name in V1_24_2_COLUMNS:
        assert name in create_columns, f"{TABLE_FQ} CREATE TABLE 缺列 {name}（新库路径）"
        assert name in alter_columns, f"{TABLE_FQ} ALTER 未补列 {name}（存量库升版路径）"


@pytest.mark.parametrize("column", ["labels", "provision_status"])
def test_registry_jsonb_columns_are_not_null_with_default(column: str) -> None:
    """JSONB 两列（标签 / 编排态）DDL 与 ORM 都必须 NOT NULL + 服务端默认值。"""
    ddl = _parse_columns()[column]
    orm_column = SimNodeRegistry.__table__.columns[column]
    assert ddl["nullable"] is False and ddl["type"] == "JSONB"
    assert orm_column.nullable is False
    assert orm_column.server_default is not None, f"{column} 缺 server_default"
