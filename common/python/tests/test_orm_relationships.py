"""ORM relationship 契约测试。

契约：``contracts/database/orm-mapping.md``（关系表 + lazy 策略规范）。
覆盖：lazy 策略白名单、mapper 零告警、RBAC/OTA 关系方向与级联语义、
跨服务逻辑外键不建关系、契约文档 ↔ 实现逐条同步、模型 ↔ Repository 一一对应。
"""
from __future__ import annotations

import re
import warnings
from pathlib import Path
from typing import Any

from sqlalchemy.exc import SAWarning
from sqlalchemy.orm import configure_mappers

import hunter_common.database.models  # noqa: F401  (register models)
from hunter_common.database.base import Base
from hunter_common.database.models import (
    AlgorithmMetric,
    Event,
    OtaRecord,
    OtaTask,
    OtaVersion,
    Permission,
    Role,
    RolePermission,
    Scene,
    User,
    UserRole,
    Vehicle,
    VehicleTelemetry,
)
from hunter_common.database.repositories import REPOSITORY_BY_MODEL
from hunter_common.database.repository import BaseRepository

ROOT = Path(__file__).resolve().parents[3]
CONTRACT = ROOT / "contracts" / "database" / "orm-mapping.md"

#: 异步安全 lazy 白名单（契约第 1 节；隐式 lazy="select" 在异步会话中会抛 MissingGreenlet）
ASYNC_SAFE_LAZY = frozenset({"selectin", "raise_on_sql"})

RELATION_ROW_RE = re.compile(r"^\|\s*`([A-Za-z_]\w*)\.([a-z_]\w*)`\s*\|", re.MULTILINE)
REPOSITORY_ROW_RE = re.compile(r"^\|\s*`(\w+Repository)`\s*\|\s*`(\w+)`\s*\|", re.MULTILINE)


def relation_names() -> set[str]:
    """实现侧全部关系（``ClassName.attr``）。"""
    return {
        f"{mapper.class_.__name__}.{prop.key}"
        for mapper in Base.registry.mappers
        for prop in mapper.relationships
    }


def contract_relation_names() -> set[str]:
    """契约文档 relationship 表声明的关系集合。"""
    text = CONTRACT.read_text(encoding="utf-8")
    start = text.index("<!-- relationship-table:start -->")
    end = text.index("<!-- relationship-table:end -->")
    return {f"{cls}.{attr}" for cls, attr in RELATION_ROW_RE.findall(text[start:end])}


def rel(model: type[Base], name: str) -> Any:  # type: ignore[type-arg]
    """取 mapper 上的 relationship 属性（缺失即失败）。"""
    return model.__mapper__.relationships[name]


# ---------- 契约同步 ----------


def test_relationship_contract_table_matches_implementation() -> None:
    """契约文档关系表 ↔ 实现逐条一致（先改契约再改实现）。"""
    assert relation_names() == contract_relation_names()
    assert len(relation_names()) == 16, "关系条数变化必须同步契约文档与 release.md"


def test_every_relationship_declares_async_safe_lazy_strategy() -> None:
    """每条关系必须显式声明白名单内的 lazy 策略（禁止隐式 lazy="select"）。"""
    offenders = {
        f"{mapper.class_.__name__}.{prop.key}": prop.lazy
        for mapper in Base.registry.mappers
        for prop in mapper.relationships
        if prop.lazy not in ASYNC_SAFE_LAZY
    }
    assert offenders == {}, f"存在非异步安全 lazy 策略: {offenders}"


def test_mapper_configuration_emits_no_sqlalchemy_warnings() -> None:
    """ORM 映射配置零 SAWarning（overlaps / lazy / cascade 组合正确）。"""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        configure_mappers()
    messages = [str(item.message) for item in caught if issubclass(item.category, SAWarning)]
    assert messages == [], f"ORM 映射产生 SAWarning: {messages}"


# ---------- RBAC 关系语义 ----------


def test_rbac_many_to_many_uses_association_tables_as_viewonly() -> None:
    """RBAC 多对多经关联表、只读（写入走关联对象 Repository，避免双写冲突）。"""
    for prop in (
        rel(User, "roles"),
        rel(Role, "users"),
        rel(Role, "permissions"),
        rel(Permission, "roles"),
    ):
        assert prop.viewonly is True
        assert prop.lazy == "selectin"
        assert prop.uselist is True
    assert rel(User, "roles").secondary is UserRole.__table__
    assert rel(Role, "users").secondary is UserRole.__table__
    assert rel(Role, "permissions").secondary is RolePermission.__table__
    assert rel(Permission, "roles").secondary is RolePermission.__table__


def test_association_object_relationships_rely_on_database_cascade() -> None:
    """关联对象关系：raise_on_sql + delete-orphan + passive_deletes（DDL ON DELETE CASCADE）。"""
    for model, name in (
        (User, "user_roles"),
        (Role, "user_roles"),
        (Role, "role_permissions"),
        (Permission, "role_permissions"),
    ):
        prop = rel(model, name)
        assert prop.lazy == "raise_on_sql"
        assert "delete-orphan" in prop.cascade
        assert prop.passive_deletes is True
        assert prop.back_populates is not None


def test_rbac_authorization_path_exposes_explicit_loading_only() -> None:
    """鉴权展开路径：多对多用 selectin（自动批量预取），关联对象用 raise_on_sql（显式加载）。"""
    assert rel(User, "roles").lazy == "selectin"
    assert rel(Role, "permissions").lazy == "selectin"
    assert rel(UserRole, "user").lazy == "raise_on_sql"
    assert rel(RolePermission, "permission").lazy == "raise_on_sql"


# ---------- OTA 关系语义 ----------


def test_ota_relationships_match_ddl_foreign_key_rules() -> None:
    """OTA 关系级联必须与 DDL 外键语义一致：版本 RESTRICT、记录 CASCADE。"""
    version_tasks = rel(OtaVersion, "tasks")
    assert version_tasks.back_populates == "version"
    assert "delete-orphan" not in version_tasks.cascade, "DDL FK 为 ON DELETE RESTRICT"
    assert version_tasks.passive_deletes is True
    assert version_tasks.lazy == "raise_on_sql"

    task_version = rel(OtaTask, "version")
    assert task_version.back_populates == "tasks"
    assert task_version.uselist is False
    assert task_version.lazy == "selectin"

    task_records = rel(OtaTask, "records")
    assert task_records.back_populates == "task"
    assert "delete-orphan" in task_records.cascade, "DDL FK 为 ON DELETE CASCADE"
    assert task_records.passive_deletes is True
    assert task_records.lazy == "raise_on_sql", "记录量级大，禁止隐式加载"

    record_task = rel(OtaRecord, "task")
    assert record_task.back_populates == "records"
    assert record_task.lazy == "selectin"


# ---------- 跨服务逻辑外键 ----------


def test_logical_foreign_keys_are_never_mapped_as_relationships() -> None:
    """跨 schema 逻辑外键不建 relationship（服务解耦，补全走 REST）。"""
    assert set(Scene.__mapper__.relationships.keys()) == set()
    assert set(Vehicle.__mapper__.relationships.keys()) == set()
    assert set(Event.__mapper__.relationships.keys()) == set()
    assert set(VehicleTelemetry.__mapper__.relationships.keys()) == set()
    assert set(AlgorithmMetric.__mapper__.relationships.keys()) == set()
    # OTA 仅映射同 schema 物理外键；creator / vehicle_id 为逻辑外键（走 REST）
    assert set(OtaTask.__mapper__.relationships.keys()) == {"version", "records"}
    assert set(OtaRecord.__mapper__.relationships.keys()) == {"task"}


# ---------- 模型 ↔ Repository ----------


def test_every_model_has_exactly_one_repository() -> None:
    from hunter_common.database.models import ALL_MODELS

    assert set(REPOSITORY_BY_MODEL) == set(ALL_MODELS)
    assert len(REPOSITORY_BY_MODEL) == len(ALL_MODELS) == 13
    for model, repository in REPOSITORY_BY_MODEL.items():
        assert repository.model is model, f"{repository.__name__}.model 必须为 {model.__name__}"
        assert issubclass(repository, BaseRepository)


def test_repository_contract_table_matches_registry() -> None:
    """契约文档 Repository 表 ↔ 注册表一致。"""
    text = CONTRACT.read_text(encoding="utf-8")
    start = text.index("<!-- repository-table:start -->")
    end = text.index("<!-- repository-table:end -->")
    rows = REPOSITORY_ROW_RE.findall(text[start:end])
    assert {name for name, _model in rows} == {
        repository.__name__ for repository in REPOSITORY_BY_MODEL.values()
    }
    assert {model for _name, model in rows} == {
        model.__name__ for model in REPOSITORY_BY_MODEL
    }

