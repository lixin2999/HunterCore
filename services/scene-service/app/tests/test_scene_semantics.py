"""场景语义层单元测试（G9 闸门 / 角色审计 / 语义指纹 / 摘要目录）。

契约来源：``contracts/scene-taxonomy.yaml::scene_semantics``。
覆盖：必需角色缺失、类型/行为不符、实例不足、事件缺失、角色回溯与兜底绑定、
指纹可区分变体、无合成语义类型跳过、语义摘要与目录一致。
"""
from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest
from httpx import AsyncClient
from hunter_common.database.enums import SceneStatus

from app.scene_engine.oracles import ORACLE_BY_REF
from app.scene_engine.semantics import (
    audit_roles,
    build_semantic_config,
    check_scene_semantics,
    group_actors,
    list_semantics,
    missing_roles,
    require_semantics,
    semantic_fingerprint,
    semantics_for,
    semantics_summary,
    taxonomy,
)
from app.tests.conftest import ADMIN_HEADERS
from app.tests.fakes import InMemorySceneRepository, config_payload, make_scene_model

GHOST = "occluded_pedestrian"


def _ghost_config(**values: float) -> dict[str, Any]:
    """鬼探头语义实例化的完整片段（行人 + 遮挡车 + 2 个事件）。"""
    return build_semantic_config(require_semantics(GHOST), values)


def _without_role(config: dict[str, Any], role: str) -> dict[str, Any]:
    config["actors"] = [
        actor
        for actor in config["actors"]
        if not str(actor.get("actor_id") or "").startswith(role)
    ]
    return config


def test_list_semantics_covers_catalog_and_contract() -> None:
    """语义摘要与契约逐类型一致：判据存在、必需角色是 roles 子集、时长非空。"""
    items = list_semantics()
    assert len(items) == 46
    types = {item["scene_type"] for item in items}
    # 摘要条数与键集合必须与契约 scene_semantics 完全一致（多一类/少一类都算漂移）
    assert types == set(taxonomy().semantics)
    for item in items:
        assert item["objective"]
        assert item["oracle_ref"] in ORACLE_BY_REF
        role_names = {role["role"] for role in item["roles"]}
        assert set(item["required_roles"]) <= role_names
        assert item["duration"]
    assert semantics_for("custom") is None
    assert semantics_summary("custom") is None
    assert taxonomy().version


def test_ghost_semantic_config_satisfies_contract() -> None:
    """语义实例化的鬼探头场景应通过 G9，且必需角色都有实际参与者。"""
    config = _ghost_config()
    assert check_scene_semantics(GHOST, config) == []
    audit = {row.role: row for row in audit_roles(GHOST, config)}
    assert audit["dart_out_pedestrian"].status == "ok"
    assert audit["dart_out_pedestrian"].actor_ids[0].startswith("dart_out_pedestrian")
    assert audit["occluder"].actual == 1
    assert config["events"], "鬼探头必须带事件"


def test_missing_required_role_is_reported() -> None:
    """删除行人 → role_missing；missing_roles 返回角色名。"""
    config = _without_role(_ghost_config(), "dart_out_pedestrian")
    problems = check_scene_semantics(GHOST, config)
    assert any(problem.code == "role_missing" for problem in problems)
    assert "dart_out_pedestrian" in missing_roles(GHOST, config)
    audit = {row.role: row for row in audit_roles(GHOST, config)}
    assert audit["dart_out_pedestrian"].status == "missing"


def test_role_type_and_behavior_mismatch_is_reported() -> None:
    """角色类型/行为被改写 → 对应 mismatch 问题，而不是静默通过。"""
    config = _ghost_config()
    for actor in config["actors"]:
        if actor["actor_id"].startswith("dart_out_pedestrian"):
            actor["type"] = "vehicle"
            actor["behavior"]["behavior_type"] = "lane_follow"
            break
    codes = {problem.code for problem in check_scene_semantics(GHOST, config)}
    assert {"role_type_mismatch", "role_behavior_mismatch"} <= codes


def test_count_underflow_is_reported() -> None:
    """角色存在但实例数少于 min_count → role_underflow（而非 role_missing）。"""
    scene_type = "multi_vehicle_mixed"
    semantics = require_semantics(scene_type)
    multi = next(role for role in semantics.roles if role.role == "mixed_vehicle")
    assert multi.required and multi.min_count >= 3, "该用例依赖多实例必需角色"
    config = build_semantic_config(semantics)
    kept = 0
    actors = []
    for actor in config["actors"]:
        if str(actor["actor_id"]).startswith("mixed_vehicle"):
            kept += 1
            if kept > multi.min_count - 1:
                continue  # 只保留 min_count-1 个，触发数量不足
        actors.append(actor)
    config["actors"] = actors
    problems = check_scene_semantics(scene_type, config)
    assert any(
        problem.code == "role_underflow" and problem.role == "mixed_vehicle"
        for problem in problems
    )
    assert all(problem.code != "role_missing" for problem in problems)


def test_events_required_but_empty_is_reported() -> None:
    """必需事件的类型清空 → events_missing。"""
    config = _ghost_config()
    config["events"] = []
    problems = check_scene_semantics(GHOST, config)
    assert any(problem.code == "events_missing" for problem in problems)


def test_group_actors_binds_by_unique_type_behavior() -> None:
    """actor_id 不按角色命名时，唯一 (类型, 行为) 组合可兜底绑定。"""
    semantics = require_semantics(GHOST)
    config = _ghost_config()
    for actor in config["actors"]:
        if actor["actor_id"].startswith("dart_out_pedestrian"):
            actor["actor_id"] = "mystery-1"
    groups, unmatched = group_actors(semantics, config["actors"])
    assert [actor["actor_id"] for actor in groups["dart_out_pedestrian"]] == ["mystery-1"]
    assert all(actor["actor_id"] != "mystery-1" for actor in unmatched)


def test_semantic_fingerprint_distinguishes_variants() -> None:
    """同类型不同变体（天气档位不同）→ 语义指纹不同（防止场景雷同）。"""
    semantics = require_semantics(GHOST)
    variants = {variant.key: variant for variant in semantics.variants}
    day = build_semantic_config(semantics, variant=variants["parked_van"])
    night = build_semantic_config(semantics, variant=variants["night_rain"])
    assert semantic_fingerprint(GHOST, day) != semantic_fingerprint(GHOST, night)


@pytest.mark.parametrize("scene_type", ["custom", "real_vehicle_replay"])
def test_types_without_semantics_are_skipped(scene_type: str) -> None:
    """无合成语义的类型：检查返回空列表（由调用方标注跳过）。"""
    config: dict[str, Any] = {"actors": [{"actor_id": "a", "type": "vehicle"}], "events": []}
    assert check_scene_semantics(scene_type, config) == []
    assert audit_roles(scene_type, config) == []


def test_builder_is_deterministic_for_same_values() -> None:
    """同一参数向量重复构造结果一致（语义层不做随机采样）。"""
    build: Callable[[], dict[str, Any]] = _ghost_config
    assert build() == build()


def test_child_chasing_ball_requires_child_and_ball() -> None:
    """儿童追球：儿童与球都是必需角色，删掉儿童即 role_missing。"""
    scene_type = "child_chasing_ball"
    sem = require_semantics(scene_type)
    assert {role.role for role in sem.required_roles()} == {"child", "ball"}
    config = build_semantic_config(sem)
    assert check_scene_semantics(scene_type, config) == []
    config = _without_role(config, "child")
    problems = check_scene_semantics(scene_type, config)
    assert [(problem.code, problem.role) for problem in problems] == [("role_missing", "child")]


def test_minimal_risk_manoeuvre_allows_no_roles_but_requires_events() -> None:
    """最小风险策略：无角色契约（允许 0 参与者）但必须有事件，空事件 → events_missing。"""
    scene_type = "minimal_risk_manoeuvre"
    sem = require_semantics(scene_type)
    assert not sem.roles and sem.required_events
    config = build_semantic_config(sem)
    assert config["actors"] == []
    assert check_scene_semantics(scene_type, config) == []
    config["events"] = []
    assert [problem.code for problem in check_scene_semantics(scene_type, config)] == [
        "events_missing"
    ]


# =====================================================================
# G9 闸门接线：入库 / 下发双侧拦截（均回 2001 并点名缺口角色）
# =====================================================================
async def test_create_scene_rejects_missing_required_roles(
    client: AsyncClient, scene_env: object
) -> None:
    """入库：鬼探头只给 1 辆前车 → 422 + code=2001，消息点名缺失角色。"""
    lead = {
        "actor_id": "lead-1",
        "type": "vehicle",
        "spawn_point": {"x": 20.0, "y": 0.0, "z": 0.0, "yaw": 0.0},
        "behavior": {"behavior_type": "lane_follow", "parameters": {}},
    }
    payload = {
        "scene_name": "鬼探头缺角色",
        "scene_type": GHOST,
        "config": config_payload(scene_type=GHOST, actors=[lead]),
    }
    resp = await client.post("/api/v1/scene", headers=ADMIN_HEADERS, json=payload)
    assert resp.status_code == 422
    body = resp.json()
    assert body["code"] == 2001
    assert "dart_out_pedestrian" in str(body["message"])


async def test_dispatch_blocks_scene_missing_required_roles(
    client: AsyncClient, scene_env: object
) -> None:
    """下发：绕过入库校验的存量/篡改场景仍被复检拦住（否则仿真端只能看到 1 辆车）。"""
    sem = require_semantics(GHOST)
    config = config_payload(scene_type=GHOST)
    config["actors"] = [
        actor for actor in config["actors"] if str(actor["actor_id"]).startswith("occluder")
    ]
    assert sem.required_roles(), "该用例依赖必需角色契约"
    model = make_scene_model(
        scene_type=GHOST, status=SceneStatus.PUBLISHED, config=config
    )
    repository: InMemorySceneRepository = scene_env.repository  # type: ignore[attr-defined]
    repository.rows[model.scene_id] = model
    resp = await client.post(f"/api/v1/scene/{model.scene_id}/run", headers=ADMIN_HEADERS)
    assert resp.status_code == 422
    body = resp.json()
    assert body["code"] == 2001
    assert "dart_out_pedestrian" in str(body["message"])

