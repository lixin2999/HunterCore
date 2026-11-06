"""CARLA 自车出生点表与出生点有效性（V1.24.8：仿真端黑屏/只见路面修复）。

覆盖：出生点表加载/解析与来源哈希、原点判定、按表指派（仅原点 + 仅 carla_town + 需已校准）、
下发载荷的相对→绝对折算与 ``spawn_semantics`` 标记、校验门 G3 的分级口径（未校准只提示、
已校准拦原点与猜填坐标）、生成/编译链路注入，以及下发前置拦截（原点出生点 → 2001）。
"""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from typing import Any

import pytest
from httpx import AsyncClient
from hunter_common.database.enums import SceneStatus

from app.config import settings
from app.scene_engine.catalog import CATALOG_BY_ID
from app.scene_engine.compiler import compile_scene, default_values
from app.scene_engine.generator import GenerationOptions, SceneGenerator
from app.scene_engine.ir import SceneIR, compile_parameters
from app.scene_engine.spawn_points import (
    DEFAULT_CARLA_SPAWN_POINTS_PATH,
    UNCALIBRATED_HINT,
    SpawnPointsError,
    SpawnPointTable,
    assign_ego_spawn_point,
    clear_spawn_point_table_cache,
    get_spawn_point_table,
    is_origin_like,
    load_spawn_point_table,
    match_candidate,
    parse_spawn_point_table,
    pick_candidate,
    to_absolute_spawn_offsets,
    wrap_rad,
)
from app.scene_engine.validators import run_gates
from app.tests.conftest import ADMIN_HEADERS
from app.tests.fakes import config_payload, make_scene_model

SCENE_BASE = "/api/v1/scene"
TOWN_POINTS: list[dict[str, float]] = [
    {"x": 100.0, "y": 200.0, "z": 0.5, "yaw": 0.0},
    {"x": 300.0, "y": 400.0, "z": 0.0, "yaw": 1.5},
]


def _table(
    *,
    calibrated: bool = True,
    points: list[dict[str, float]] | None = None,
    map_id: str = "Town01",
) -> SpawnPointTable:
    """构造测试表（默认单地图两点；``points`` 为空即"该地图无候选点"）。"""
    payload = {
        "calibrated": calibrated,
        "source": "live-carla-export",
        "maps": {map_id: {"spawn_points": list(TOWN_POINTS if points is None else points)}},
    }
    return parse_spawn_point_table(payload, source="unit-test", source_hash="0" * 64)


def _write_table(path: Path, **overrides: Any) -> Path:
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "calibrated": True,
        "source": "live-carla-export",
        "carla_version": "0.9.16",
        "exported_at": "2026-11-06T00:00:00Z",
        "maps": {"Town01": {"spawn_points": list(TOWN_POINTS)}},
    }
    payload.update(overrides)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _ir() -> SceneIR:
    return CATALOG_BY_ID["LS-BAS-001"]


def _gate(report: Any, gate: str) -> Any:
    return next(result for result in report.gates if result.gate == gate)


def _report(config: dict[str, Any], *, table: SpawnPointTable | None) -> Any:
    ir = _ir()
    values = default_values(ir)
    return run_gates(
        ir,
        values,
        config,
        max_duration=settings.scene_duration_max_seconds,
        spawn_table=table,
    )


def _compiled_config(**mutate: Any) -> dict[str, Any]:
    ir = _ir()
    config = compile_parameters(ir, default_values(ir))
    config.update(mutate)
    return config


# =====================================================================
# 一、表的加载与解析
# =====================================================================


def test_packaged_table_is_uncalibrated_skeleton() -> None:
    """出厂表必须是"未校准空骨架"：平台绝不携带编造出来的坐标。"""
    assert DEFAULT_CARLA_SPAWN_POINTS_PATH.is_file()
    table = load_spawn_point_table(DEFAULT_CARLA_SPAWN_POINTS_PATH)
    assert table.calibrated is False
    assert table.usable("Town01") is False
    assert all(not points for points in table.maps.values())
    assert table.source_hash == hashlib.sha256(
        DEFAULT_CARLA_SPAWN_POINTS_PATH.read_bytes()
    ).hexdigest()


def test_table_source_hash_tracks_file_bytes(tmp_path: Path) -> None:
    first = load_spawn_point_table(_write_table(tmp_path / "a.json"))
    same = load_spawn_point_table(_write_table(tmp_path / "b.json"))
    changed = load_spawn_point_table(_write_table(tmp_path / "c.json", calibrated=False))
    assert first.calibrated is True
    assert first.source_hash == same.source_hash, "内容一致则哈希一致（与文件名无关）"
    assert changed.source_hash != first.source_hash, "内容变更必须体现在哈希上"
    assert first.carla_version == "0.9.16"
    assert first.candidates("Town01")[1].y == 400.0


@pytest.mark.parametrize(
    "overrides",
    [
        {"calibrated": "true"},
        {"maps": []},
        {"maps": {"Town01": {"spawn_points": {}}}},
        {"maps": {"Town01": {"spawn_points": [{"x": "1.0", "y": 2.0}]}}},
        {"maps": {"Town01": {"spawn_points": [{"x": 1.0, "y": math.nan}]}}},
        {"maps": {"Town01": {"spawn_points": []}}, "calibrated": True},
        {"origin_tolerance_m": 0},
    ],
    ids=[
        "calibrated-not-bool",
        "maps-not-object",
        "points-not-array",
        "coordinate-not-number",
        "coordinate-not-finite",
        "calibrated-without-points",
        "zero-tolerance",
    ],
)
def test_table_rejects_illegal_payloads(tmp_path: Path, overrides: dict[str, Any]) -> None:
    with pytest.raises(SpawnPointsError):
        load_spawn_point_table(_write_table(tmp_path / "p.json", **overrides))


def test_table_rejects_missing_or_broken_file(tmp_path: Path) -> None:
    with pytest.raises(SpawnPointsError):
        load_spawn_point_table(tmp_path / "not-found.json")
    broken = tmp_path / "broken.json"
    broken.write_text("{not-json", encoding="utf-8")
    with pytest.raises(SpawnPointsError):
        load_spawn_point_table(broken)


def test_get_table_follows_configured_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """provider 为进程级单一事实来源：路径由配置决定，结果按路径缓存。"""
    path = _write_table(tmp_path / "spawn.json", carla_version="0.9.15")
    monkeypatch.setattr(settings, "scene_carla_spawn_points_path", str(path))
    clear_spawn_point_table_cache()
    try:
        loaded = get_spawn_point_table()
        assert loaded.carla_version == "0.9.15"
        assert get_spawn_point_table() is loaded
    finally:
        clear_spawn_point_table_cache()
        monkeypatch.undo()
        clear_spawn_point_table_cache()


def test_wrap_rad_maps_into_contract_range() -> None:
    """契约 SpawnPoint.yaw 区间 [-pi, pi]：折算后必须回落。"""
    assert wrap_rad(0.0) == pytest.approx(0.0)
    assert wrap_rad(math.pi * 1.5) == pytest.approx(-math.pi / 2)
    assert wrap_rad(-math.pi * 3) == pytest.approx(-math.pi)
    assert all(abs(wrap_rad(degrees)) <= math.pi for degrees in range(-1080, 1080, 15))


# =====================================================================
# 二、判定 / 指派 / 折算
# =====================================================================


def test_is_origin_like_uses_tolerance() -> None:
    assert is_origin_like({"x": 0.0, "y": 0.0}) is True
    assert is_origin_like({"x": 0.9, "y": -0.4}, tolerance_m=1.0) is True
    assert is_origin_like({"x": 0.9, "y": -0.4}, tolerance_m=0.5) is False
    assert is_origin_like({"x": 120.0, "y": 0.0}) is False
    # 非数值交由 SceneConfig/G3 的"生成点有限"检查处理，这里不判为原点
    assert is_origin_like({"x": "abc", "y": 0.0}) is False


def test_pick_candidate_rotates_and_requires_usable_table() -> None:
    table = _table()
    assert pick_candidate("Town01", 0, table) == table.candidates("Town01")[0]
    assert pick_candidate("Town01", 1, table) == table.candidates("Town01")[1]
    assert pick_candidate("Town01", 2, table) == table.candidates("Town01")[0]
    assert pick_candidate("Town09", 0, table) is None
    assert pick_candidate("Town01", 0, _table(calibrated=False)) is None


def test_match_candidate_respects_tolerance() -> None:
    table = _table()
    assert match_candidate("Town01", {"x": 101.0, "y": 200.5}, table) is not None
    assert match_candidate("Town01", {"x": 110.0, "y": 200.0}, table) is None
    assert match_candidate("Town02", {"x": 100.0, "y": 200.0}, table) is None


def test_assign_overrides_only_origin_for_carla_town() -> None:
    """人工填写的有效坐标一律保留，只覆写原点。"""
    table = _table()
    config = config_payload()
    config["map"]["spawn_point"] = {"x": 0.0, "y": 0.0, "z": 0.0, "yaw": 0.0}
    assigned, message = assign_ego_spawn_point(config, seq=1, table=table)
    assert assigned is True
    assert config["map"]["spawn_point"]["x"] == 300.0
    assert "已按出生点表指派" in message

    kept = config_payload()
    kept["map"]["spawn_point"] = {"x": 55.5, "y": 66.6, "z": 0.0, "yaw": 0.0}
    assigned, message = assign_ego_spawn_point(kept, seq=0, table=table)
    assert assigned is False
    assert kept["map"]["spawn_point"]["x"] == 55.5
    assert "人工指定值保留" in message

    hd = config_payload()
    hd["map"]["map_type"] = "hd_map"
    hd["map"]["spawn_point"] = {"x": 0.0, "y": 0.0, "z": 0.0, "yaw": 0.0}
    assigned, message = assign_ego_spawn_point(hd, seq=0, table=table)
    assert assigned is False
    assert "部署方定义" in message


def test_assign_requires_calibrated_table() -> None:
    config = config_payload()
    config["map"]["spawn_point"] = {"x": 0.0, "y": 0.0, "z": 0.0, "yaw": 0.0}
    assigned, message = assign_ego_spawn_point(config, seq=0, table=_table(calibrated=False))
    assert assigned is False
    assert config["map"]["spawn_point"]["x"] == 0.0
    assert UNCALIBRATED_HINT in message


def test_absolute_offsets_fold_base_and_mark_semantics() -> None:
    """下发折算：参与者相对偏移 → 绝对坐标；yaw 归一；入参不被修改。"""
    config = config_payload()
    config["map"]["spawn_point"] = {"x": 100.0, "y": 200.0, "z": 1.0, "yaw": 3.0}
    config["actors"] = [
        {
            "actor_id": "lead",
            "type": "vehicle",
            "spawn_point": {"x": 6.0, "y": 0.0, "z": 0.0, "yaw": 0.3},
            "behavior": {"behavior_type": "constant_velocity", "parameters": {}},
        }
    ]
    snapshot = json.dumps(config, sort_keys=True)

    payload = to_absolute_spawn_offsets(config)

    assert json.dumps(config, sort_keys=True) == snapshot, "折算必须是纯函数，不改入参"
    assert payload["spawn_semantics"] == "absolute"
    actor = payload["actors"][0]["spawn_point"]
    assert actor["x"] == pytest.approx(106.0)
    assert actor["y"] == pytest.approx(200.0)
    assert actor["z"] == pytest.approx(1.0)
    assert actor["yaw"] == pytest.approx(wrap_rad(3.3))
    # 自车出生点保持原值（本就是绝对坐标）
    assert payload["map"]["spawn_point"]["x"] == 100.0


def test_absolute_offsets_tolerate_missing_sections() -> None:
    assert to_absolute_spawn_offsets({}) == {}
    payload = {"map": {"map_id": "Town01"}}
    assert to_absolute_spawn_offsets(payload) == {"map": {"map_id": "Town01"}}


# =====================================================================
# 三、校验门 G3 的分级口径
# =====================================================================


def test_g3_flags_origin_without_failing_while_table_uncalibrated() -> None:
    """未校准：平台无从判定任意坐标有效性 ⇒ 只标记风险，不瘫痪生成链路（硬拦在下发）。"""
    gate = _gate(_report(_compiled_config(), table=_table(calibrated=False, points=[])), "G3")
    assert gate.passed is True
    assert "地图原点" in gate.message


def test_g3_rejects_origin_and_guessed_coordinate_when_table_calibrated() -> None:
    config = _compiled_config()
    table = _table(map_id=config["map"]["map_id"])

    origin = _gate(_report(config, table=table), "G3")
    assert origin.passed is False
    assert "落在地图原点" in origin.message

    guessed = _compiled_config()
    guessed["map"]["spawn_point"] = {"x": 812.3, "y": 99.1, "z": 0.0, "yaw": 0.0}
    gate = _gate(_report(guessed, table=table), "G3")
    assert gate.passed is False
    assert "未命中地图" in gate.message and "凭猜测填写" in gate.message


def test_g3_accepts_table_hit_and_notes_source() -> None:
    config = _compiled_config()
    config["map"]["spawn_point"] = dict(TOWN_POINTS[0])
    gate = _gate(_report(config, table=_table(map_id=config["map"]["map_id"])), "G3")
    assert gate.passed is True
    assert "出生点命中实机表" in gate.message


def test_g3_skips_spawn_table_rules_for_non_carla_town() -> None:
    config = _compiled_config()
    config["map"]["map_type"] = "hd_map"
    gate = _gate(_report(config, table=_table()), "G3")
    assert gate.passed is True
    assert "出生点" not in gate.message


def test_g3_fails_when_table_file_is_unusable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """表文件缺失/损坏必须显式失败，不得静默降级为"没有表"。"""
    monkeypatch.setattr(settings, "scene_carla_spawn_points_path", str(tmp_path / "gone.json"))
    clear_spawn_point_table_cache()
    try:
        gate = _gate(_report(_compiled_config(), table=None), "G3")
        assert gate.passed is False
        assert "出生点表不可用" in gate.message
    finally:
        monkeypatch.setattr(settings, "scene_carla_spawn_points_path", "")
        clear_spawn_point_table_cache()


# =====================================================================
# 四、生成/编译链路注入
# =====================================================================


def test_generator_assigns_spawn_from_calibrated_table() -> None:
    ir = _ir()
    table = _table(map_id=str(ir.base_config["map"]["map_id"]))
    generator = SceneGenerator(
        max_duration=settings.scene_duration_max_seconds, spawn_table=table
    )
    outcome = generator.generate(
        ir,
        GenerationOptions(
            logical_version="1.0.0", count=4, strategy="distribution", profile="regression", seed=7
        ),
    )
    assert outcome.produced == 4, outcome.gate_failures
    coordinates = {row["scene_config"]["map"]["spawn_point"]["x"] for row in outcome.rows}
    assert coordinates <= {point["x"] for point in TOWN_POINTS}
    assert coordinates != {0.0}, "出生点必须由表指派，不得继续产出原点场景"


def test_generator_leaves_origin_while_table_uncalibrated() -> None:
    """未校准不覆写：出生点仍为原点，但 G3 通过并带风险标记（下发时才会被拦）。"""
    ir = _ir()
    table = _table(calibrated=False, points=[])
    generator = SceneGenerator(
        max_duration=settings.scene_duration_max_seconds, spawn_table=table
    )
    outcome = generator.generate(
        ir,
        GenerationOptions(
            logical_version="1.0.0", count=2, strategy="distribution", profile="regression", seed=3
        ),
    )
    assert outcome.produced == 2, outcome.gate_failures
    assert outcome.rows[0]["scene_config"]["map"]["spawn_point"]["x"] == 0.0


def test_compile_scene_spawn_assignment_is_idempotent() -> None:
    """同一 (IR, 参数) 重复编译必须落同一出生点（复检/重放结果可复现）。"""
    ir = _ir()
    values = default_values(ir)
    table = _table(map_id=str(ir.base_config["map"]["map_id"]))
    first = compile_scene(
        ir, values, max_duration=settings.scene_duration_max_seconds, spawn_table=table
    )
    second = compile_scene(
        ir, values, max_duration=settings.scene_duration_max_seconds, spawn_table=table
    )
    assert first.report.passed is True
    assert (
        first.config["map"]["spawn_point"] == second.config["map"]["spawn_point"]
    )
    assert first.config["map"]["spawn_point"]["x"] in {point["x"] for point in TOWN_POINTS}


# =====================================================================
# 五、下发前置拦截与载荷折算
# =====================================================================


@pytest.mark.asyncio
async def test_run_scene_rejects_origin_spawn_point(client: AsyncClient, scene_env: Any) -> None:
    config = config_payload()
    config["map"]["spawn_point"] = {"x": 0.0, "y": 0.0, "z": 0.0, "yaw": 0.0}
    model = make_scene_model(
        scene_name="原点场景", status=SceneStatus.PUBLISHED, config=config
    )
    scene_env.repository.rows[model.scene_id] = model

    resp = await client.post(f"{SCENE_BASE}/{model.scene_id}/run", headers=ADMIN_HEADERS)

    assert resp.status_code == 422, resp.text
    body = resp.json()
    assert body["code"] == 2001
    assert "落在地图原点" in body["message"]
    # 未创建任何实例：黑屏场景不得空跑
    assert scene_env.carla.created == []
    assert scene_env.carla.submitted == []


@pytest.mark.asyncio
async def test_run_scene_dispatches_absolute_actor_offsets(
    client: AsyncClient, scene_env: Any
) -> None:
    config = config_payload()
    config["map"]["spawn_point"] = {"x": 200.0, "y": 300.0, "z": 0.0, "yaw": 1.0}
    config["actors"] = [
        {
            "actor_id": "lead",
            "type": "vehicle",
            "spawn_point": {"x": 8.0, "y": 1.0, "z": 0.0, "yaw": 0.5},
            "behavior": {"behavior_type": "constant_velocity", "parameters": {}},
        }
    ]
    model = make_scene_model(
        scene_name="偏移折算场景", status=SceneStatus.PUBLISHED, config=config
    )
    scene_env.repository.rows[model.scene_id] = model

    resp = await client.post(f"{SCENE_BASE}/{model.scene_id}/run", headers=ADMIN_HEADERS)
    assert resp.status_code == 200, resp.text

    dispatched = scene_env.carla.created[0]["scene_config"]
    assert dispatched["spawn_semantics"] == "absolute"
    assert dispatched["actors"][0]["spawn_point"]["x"] == pytest.approx(208.0)
    assert dispatched["actors"][0]["spawn_point"]["yaw"] == pytest.approx(wrap_rad(1.5))
    # 库内存储语义不变（仍是相对偏移），只有下发载荷被折算
    assert model.config_json["actors"][0]["spawn_point"]["x"] == pytest.approx(8.0)
