"""场景引擎 OpenAPI 契约测试（无需运行服务/基础设施）。

校验目标（契约是单一事实来源，`contracts/openapi/scene-engine.yaml`）：
- OpenAPI 3.0.3 合法：`$ref` 全部可解析、`operationId` 唯一
- 端点集合（method + path）与 `app.routers.engine` 实际注册完全一致（不得擅自增删）
- 契约 `x-hunter-endpoints` 分组登记 == 全部 `operationId`
- 与 `scene-service.yaml` 路径不重叠（同前缀下禁止两处声明同一端点）
- 受控词表 schema 与 ORM `StrEnum` 逐字一致；错误码 ⊆ 预定义错误码且 HTTP 映射齐全
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
import yaml
from hunter_common.exceptions import ErrorCode

from app.core.error_handlers import HTTP_STATUS_BY_CODE
from app.main import app
from app.models.scene_engine import (
    ConcreteScenarioStatus,
    CriticalityLevel,
    GenerationProfile,
    GenerationStrategy,
    GenerationTaskStatus,
    LogicalScenarioStatus,
    RarityZone,
    RiskLevel,
    SuiteItemType,
    SuiteStatus,
    TestCaseStatus,
)

ROOT = Path(__file__).resolve().parents[4]
CONTRACT_PATH = ROOT / "contracts" / "openapi" / "scene-engine.yaml"
LEGACY_CONTRACT_PATH = ROOT / "contracts" / "openapi" / "scene-service.yaml"

#: ORM StrEnum ↔ 契约 schema 名称（取值必须逐字一致）
ENUM_SCHEMAS = {
    "LogicalScenarioStatus": LogicalScenarioStatus,
    "ConcreteScenarioStatus": ConcreteScenarioStatus,
    "RarityZone": RarityZone,
    "CriticalityLevel": CriticalityLevel,
    "RiskLevel": RiskLevel,
    "GenerationStrategy": GenerationStrategy,
    "GenerationProfile": GenerationProfile,
    "GenerationTaskStatus": GenerationTaskStatus,
    "TestCaseStatus": TestCaseStatus,
    "SuiteStatus": SuiteStatus,
    "SuiteItemType": SuiteItemType,
}


@pytest.fixture(scope="module")
def contract() -> dict:
    return yaml.safe_load(CONTRACT_PATH.read_text(encoding="utf-8"))


def _walk(node: object, path: str = "") -> Iterator[tuple[str, str]]:
    """遍历文档，产出 (位置, $ref) 对。"""
    if isinstance(node, dict):
        for key, value in node.items():
            yield from _walk(value, f"{path}/{key}")
    elif isinstance(node, list):
        for index, value in enumerate(node):
            yield from _walk(value, f"{path}[{index}]")
    elif isinstance(node, str) and node.startswith("#/components/"):
        yield path, node


def _operations(contract: dict) -> list[tuple[str, str, str]]:
    """(method, path, operationId) 列表。"""
    items: list[tuple[str, str, str]] = []
    for path, path_item in contract["paths"].items():
        for method, operation in path_item.items():
            items.append((method.upper(), path, operation["operationId"]))
    return items


def _implementation() -> dict[tuple[str, str], str]:
    """实际实现的 (method, path) → operationId（tags=scene-engine）。

    FastAPI 新版对 `include_router` 采用惰性合并（`_IncludedRouter`），
    直接遍历 `app.routes` 看不到子路由，故经 `app.openapi()` 取真实汇总结果。
    """
    routes: dict[tuple[str, str], str] = {}
    for path, path_item in app.openapi()["paths"].items():
        for method, operation in path_item.items():
            if "scene-engine" in (operation.get("tags") or ()):
                routes[(method.upper(), path)] = operation["operationId"]
    return routes


def test_contract_is_openapi_3(contract: dict) -> None:
    assert contract["openapi"] == "3.0.3"
    assert contract["info"]["title"].startswith("HunterCore")
    assert "/api/v1/scene/logical" in contract["paths"]


def test_all_refs_resolve(contract: dict) -> None:
    components = contract["components"]
    broken: list[str] = []
    for location, ref in _walk(contract):
        node: object = components
        for part in ref[2:].split("/")[1:]:
            if not isinstance(node, dict) or part not in node:
                broken.append(f"{ref} @ {location}")
                break
            node = node[part]
    assert broken == []


def test_operation_ids_unique(contract: dict) -> None:
    operation_ids = [operation_id for _, _, operation_id in _operations(contract)]
    assert len(operation_ids) == len(set(operation_ids))


def test_endpoints_match_implementation(contract: dict) -> None:
    declared = {(method, path): operation_id for method, path, operation_id in _operations(contract)}
    assert declared == _implementation()


def test_endpoint_groups_match_operation_ids(contract: dict) -> None:
    declared = {operation_id for _, _, operation_id in _operations(contract)}
    groups = contract["x-hunter-endpoints"]
    registered = {operation_id for ids in groups.values() for operation_id in ids}
    assert registered == declared
    assert set(groups) == {
        "logical",
        "generation",
        "concrete",
        "oracle",
        "suite",
        "coverage",
    }


def test_no_path_overlap_with_scene_service_contract(contract: dict) -> None:
    legacy = yaml.safe_load(LEGACY_CONTRACT_PATH.read_text(encoding="utf-8"))
    legacy_paths = {
        (method.upper(), path)
        for path, path_item in legacy["paths"].items()
        for method in path_item
    }
    engine_paths = {(method, path) for method, path, _ in _operations(contract)}
    assert legacy_paths & engine_paths == set()


@pytest.mark.parametrize("schema_name", sorted(ENUM_SCHEMAS))
def test_enum_schema_matches_orm(contract: dict, schema_name: str) -> None:
    values = contract["components"]["schemas"][schema_name]["enum"]
    expected = {member.value for member in ENUM_SCHEMAS[schema_name]}
    assert set(values) == expected


def test_error_codes_are_predefined(contract: dict) -> None:
    codes = set(contract["components"]["schemas"]["ErrorCode"]["enum"])
    predefined = {code.value for code in ErrorCode} | {0}
    assert codes <= predefined
    non_zero = {code for code in codes if code != 0}
    assert non_zero <= set(HTTP_STATUS_BY_CODE)
