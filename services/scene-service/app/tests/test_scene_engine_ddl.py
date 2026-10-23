"""scene_engine DDL ↔ ORM 契约校验（07_scene_engine.sql 为单一事实来源）。

边界：scene_engine 是 scene-service 服务私有 schema，ORM 归属服务侧，
不参与 ``hunter_common`` 的共享表清单；此文件逐表/逐列/逐枚举钉住契约。
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from app.models.scene_engine import (
    Avoidability,
    BaselineLock,
    ConcreteScenario,
    ConcreteScenarioStatus,
    CriticalityLevel,
    GenerationProfile,
    GenerationStrategy,
    GenerationTask,
    GenerationTaskStatus,
    LogicalScenario,
    LogicalScenarioStatus,
    RarityZone,
    RiskLevel,
    ScenarioSuite,
    SuiteItem,
    SuiteItemType,
    SuiteStatus,
)
from app.models.scene_engine import TestCase as TestCaseModel
from app.models.scene_engine import TestCaseStatus as TestCaseStatusEnum

DDL_PATH = (
    Path(__file__).resolve().parents[4] / "contracts" / "database" / "ddl" / "07_scene_engine.sql"
)

CREATE_TABLE_RE = re.compile(
    r"CREATE TABLE (?:IF NOT EXISTS )?([\w.]+)\s*\((.*?)\n\)\s*;",
    re.DOTALL,
)
CHECK_ENUM_RE = re.compile(r"CHECK\s*\(\s*(\w+)\s+IN\s*\((.*?)\)", re.DOTALL)
QUOTED_RE = re.compile(r"'([^']*)'")
CREATE_TYPE_RE = re.compile(r"CREATE\s+TYPE\s+scene_engine\.\w+\s+AS\s+ENUM", re.IGNORECASE)
CONSTRAINT_HEAD_RE = re.compile(
    r"(PRIMARY KEY|CHECK|CONSTRAINT|FOREIGN|UNIQUE)\b", re.IGNORECASE
)

ORM_MODELS = [
    LogicalScenario,
    ConcreteScenario,
    TestCaseModel,
    ScenarioSuite,
    SuiteItem,
    BaselineLock,
    GenerationTask,
]

#: 枚举列 → StrEnum（TEXT + CHECK 表示，与 StrEnumType(impl=Text) 对齐）
ENUM_CHECK_MAP: dict[tuple[str, str], type] = {
    ("logical_scenarios", "status"): LogicalScenarioStatus,
    ("concrete_scenarios", "rarity_zone"): RarityZone,
    ("concrete_scenarios", "criticality_level"): CriticalityLevel,
    ("concrete_scenarios", "risk_level"): RiskLevel,
    ("concrete_scenarios", "avoidability"): Avoidability,
    ("concrete_scenarios", "status"): ConcreteScenarioStatus,
    ("test_cases", "status"): TestCaseStatusEnum,
    ("scenario_suites", "status"): SuiteStatus,
    ("suite_items", "item_type"): SuiteItemType,
    ("generation_tasks", "strategy"): GenerationStrategy,
    ("generation_tasks", "profile"): GenerationProfile,
    ("generation_tasks", "status"): GenerationTaskStatus,
}


def _ddl_text() -> str:
    assert DDL_PATH.is_file(), f"缺失 DDL 契约文件: {DDL_PATH}"
    return DDL_PATH.read_text(encoding="utf-8")


def _strip_constraint_blocks(body: str) -> list[str]:
    """去掉约束块（含跨行 CHECK）只剩列定义行。"""
    kept: list[str] = []
    depth = 0
    for line in body.splitlines():
        stripped = line.strip()
        if depth == 0 and CONSTRAINT_HEAD_RE.match(stripped):
            depth += line.count("(") - line.count(")")
            continue
        if depth > 0:
            depth += line.count("(") - line.count(")")
            continue
        kept.append(line)
    return kept


def _ddl_tables() -> dict[str, set[str]]:
    tables: dict[str, set[str]] = {}
    for match in CREATE_TABLE_RE.finditer(_ddl_text()):
        columns: set[str] = set()
        for line in _strip_constraint_blocks(match.group(2)):
            if not line.startswith("    ") or line.startswith("     "):
                continue
            stripped = line.strip()
            if not stripped or stripped.startswith("--"):
                continue
            columns.add(stripped.split()[0].lower())
        tables[match.group(1).lower()] = columns
    return tables


def test_scene_engine_ddl_tables_match_orm() -> None:
    ddl_fullnames = set(_ddl_tables())
    orm_fullnames = {model.__table__.fullname for model in ORM_MODELS}
    assert ddl_fullnames == orm_fullnames, (
        f"DDL 多出: {sorted(ddl_fullnames - orm_fullnames)} / "
        f"ORM 多出: {sorted(orm_fullnames - ddl_fullnames)}"
    )


@pytest.mark.parametrize("model", ORM_MODELS, ids=lambda model: model.__tablename__)
def test_scene_engine_ddl_columns_match_orm(model) -> None:
    tables = _ddl_tables()
    fullname = model.__table__.fullname
    ddl_columns = tables[fullname]
    orm_columns = {column.name for column in model.__table__.columns}
    assert ddl_columns == orm_columns, (
        f"{fullname} DDL 多出: {sorted(ddl_columns - orm_columns)} / "
        f"ORM 多出: {sorted(orm_columns - ddl_columns)}"
    )


def test_scene_engine_ddl_no_native_enum_types() -> None:
    """回归护栏：禁止 PG 原生枚举（asyncpg 会报 operator does not exist）。"""
    assert not CREATE_TYPE_RE.search(_ddl_text())


def _ddl_enum_checks() -> dict[tuple[str, str], set[str]]:
    result: dict[tuple[str, str], set[str]] = {}
    for match in CREATE_TABLE_RE.finditer(_ddl_text()):
        table = match.group(1).lower().split(".")[-1]
        for column, values in CHECK_ENUM_RE.findall(match.group(2)):
            result[(table, column.lower())] = set(QUOTED_RE.findall(values))
    return result


def test_scene_engine_ddl_enum_values_match_orm() -> None:
    ddl_checks = _ddl_enum_checks()
    for (table, column), enum_cls in ENUM_CHECK_MAP.items():
        key = (table, column)
        assert key in ddl_checks, f"DDL 缺失枚举 CHECK：scene_engine.{table}.{column}"
        orm_values = {item.value for item in enum_cls}
        assert ddl_checks[key] == orm_values, (
            f"{table}.{column} 取值不一致：DDL 多出 {sorted(ddl_checks[key] - orm_values)} / "
            f"ORM 多出 {sorted(orm_values - ddl_checks[key])}"
        )
