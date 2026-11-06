"""CARLA 车辆蓝图几何表与下发挂载参考块（V1.24.9：问题 2「画面只见路面」的平台侧缓解）。

覆盖：几何表加载/解析与来源哈希、车型→蓝图解析（显式映射 / 蓝图 ID / 展示名，归一化匹配）、
包围盒边界推导、参考块的四种状态与「未命中不携带数字」口径，以及下发链路挂载参考块的
附加行为（不改变其它字段、表不可用不阻断下发）。
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
from httpx import AsyncClient
from hunter_common.database.enums import SceneStatus

from app.config import settings
from app.scene_engine.vehicle_geometry import (
    DEFAULT_CARLA_VEHICLE_GEOMETRY_PATH,
    GEOMETRY_UNCALIBRATED_HINT,
    MOUNT_POLICY_DERIVE_FROM_BBOX,
    SENSOR_MOUNT_REFERENCE_KEY,
    STATUS_BLUEPRINT_UNMATCHED,
    STATUS_RESOLVED,
    STATUS_TABLE_UNAVAILABLE,
    STATUS_TABLE_UNCALIBRATED,
    VehicleGeometryError,
    VehicleGeometryTable,
    build_sensor_mount_reference,
    clear_vehicle_geometry_table_cache,
    get_vehicle_geometry_table,
    load_vehicle_geometry_table,
    normalize_model_key,
    parse_vehicle_geometry_table,
    unavailable_reference,
)
from app.tests.conftest import ADMIN_HEADERS
from app.tests.fakes import config_payload, make_scene_model

SCENE_BASE = "/api/v1/scene"
BLUEPRINT_ID = "vehicle.hunter_se.01"
#: 实机尺寸样例：车长 4.0 / 宽 1.8 / 高 1.5，盒心 (0.2, 0.0, 0.7)、半尺寸 (2.0, 0.9, 0.75)
GEOMETRY_ENTRY: dict[str, Any] = {
    "display_name": "Hunter SE",
    "dimension": {"length_m": 4.0, "width_m": 1.8, "height_m": 1.5},
    "bounding_box": {
        "center": {"x": 0.2, "y": 0.0, "z": 0.7},
        "extent": {"x": 2.0, "y": 0.9, "z": 0.75},
    },
}


def _table(
    *,
    calibrated: bool = True,
    blueprints: dict[str, Any] | None = None,
    mapping: dict[str, Any] | None = None,
) -> VehicleGeometryTable:
    """构造测试表（默认单蓝图 + HUNTER_SE 显式映射；``mapping={}`` 即"只靠自动索引"）。"""
    payload = {
        "calibrated": calibrated,
        "source": "live-carla-export",
        "mount_policy": MOUNT_POLICY_DERIVE_FROM_BBOX,
        "blueprints": {BLUEPRINT_ID: GEOMETRY_ENTRY} if blueprints is None else blueprints,
        "model_to_blueprint": {"HUNTER_SE": BLUEPRINT_ID} if mapping is None else mapping,
    }
    return parse_vehicle_geometry_table(payload, source="unit-test", source_hash="0" * 64)


def _unrelated_table() -> VehicleGeometryTable:
    """已校准但与平台车型无关的表（展示名/蓝图 ID 均不同于 HUNTER_SE）→ 未命中。"""
    entry = {**GEOMETRY_ENTRY, "display_name": "Other Car"}
    return _table(blueprints={"vehicle.other.01": entry}, mapping={})


def _write_table(path: Path, **overrides: Any) -> Path:
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "calibrated": True,
        "source": "live-carla-export",
        "carla_version": "0.9.16",
        "exported_at": "2026-11-06T00:00:00Z",
        "mount_policy": MOUNT_POLICY_DERIVE_FROM_BBOX,
        "model_to_blueprint": {"HUNTER_SE": BLUEPRINT_ID},
        "blueprints": {BLUEPRINT_ID: GEOMETRY_ENTRY},
    }
    payload.update(overrides)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


# =====================================================================
# 一、表的加载与解析
# =====================================================================


def test_packaged_table_is_uncalibrated_skeleton() -> None:
    """出厂表必须是"未校准空骨架"：平台绝不携带编造出来的车辆尺寸。"""
    assert DEFAULT_CARLA_VEHICLE_GEOMETRY_PATH.is_file()
    table = load_vehicle_geometry_table(DEFAULT_CARLA_VEHICLE_GEOMETRY_PATH)
    assert table.calibrated is False
    assert table.blueprints == {}
    assert table.usable() is False
    # 骨架里的车型映射值必须留空（null），否则等于平台替部署方猜蓝图
    raw = json.loads(DEFAULT_CARLA_VEHICLE_GEOMETRY_PATH.read_text(encoding="utf-8"))
    assert all(value is None for value in raw["model_to_blueprint"].values())
    assert table.source_hash == hashlib.sha256(
        DEFAULT_CARLA_VEHICLE_GEOMETRY_PATH.read_bytes()
    ).hexdigest()


def test_table_source_hash_tracks_file_bytes(tmp_path: Path) -> None:
    first = load_vehicle_geometry_table(_write_table(tmp_path / "a.json"))
    same = load_vehicle_geometry_table(_write_table(tmp_path / "b.json"))
    changed = load_vehicle_geometry_table(
        _write_table(tmp_path / "c.json", carla_version="0.9.15")
    )
    assert first.calibrated is True
    assert first.source_hash == same.source_hash, "内容一致则哈希一致（与文件名无关）"
    assert changed.source_hash != first.source_hash, "内容变更必须体现在哈希上"
    assert first.carla_version == "0.9.16"


@pytest.mark.parametrize(
    "overrides",
    [
        {"calibrated": "true"},
        {"blueprints": []},
        {"blueprints": {}, "calibrated": True},
        {"blueprints": {BLUEPRINT_ID: "not-object"}},
        {"blueprints": {BLUEPRINT_ID: {**GEOMETRY_ENTRY, "bounding_box": {"center": {"x": 1}}}}},
        {"blueprints": {BLUEPRINT_ID: {**GEOMETRY_ENTRY, "bounding_box": {
            "center": {"x": 0.0, "y": 0.0, "z": 0.0},
            "extent": {"x": 0.0, "y": 0.9, "z": 0.75},
        }}}},
        {"blueprints": {BLUEPRINT_ID: {**GEOMETRY_ENTRY, "dimension": {
            "length_m": -4.0, "width_m": 1.8, "height_m": 1.5,
        }}}},
        {"mount_policy": "fixed_offsets"},
        {"model_to_blueprint": {"HUNTER_SE": "vehicle.unknown"}},
        {"model_to_blueprint": []},
    ],
    ids=[
        "calibrated-not-bool",
        "blueprints-not-object",
        "calibrated-without-blueprints",
        "entry-not-object",
        "bbox-incomplete",
        "extent-not-positive",
        "dimension-negative",
        "unsupported-mount-policy",
        "mapping-to-unknown-blueprint",
        "mapping-not-object",
    ],
)
def test_table_rejects_illegal_payloads(tmp_path: Path, overrides: dict[str, Any]) -> None:
    with pytest.raises(VehicleGeometryError):
        load_vehicle_geometry_table(_write_table(tmp_path / "p.json", **overrides))


def test_table_rejects_missing_or_broken_file(tmp_path: Path) -> None:
    with pytest.raises(VehicleGeometryError):
        load_vehicle_geometry_table(tmp_path / "not-found.json")
    broken = tmp_path / "broken.json"
    broken.write_text("{not-json", encoding="utf-8")
    with pytest.raises(VehicleGeometryError):
        load_vehicle_geometry_table(broken)


def test_mapping_null_value_is_tolerated(tmp_path: Path) -> None:
    """骨架里的 model_to_blueprint 允许留 null（部署方未填），不视为非法、也不可用。"""
    table = load_vehicle_geometry_table(
        _write_table(
            tmp_path / "s.json",
            calibrated=False,
            blueprints={},
            model_to_blueprint={"HUNTER_SE": None},
        )
    )
    assert table.calibrated is False
    assert table.resolve("HUNTER_SE") is None


def test_get_table_follows_configured_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """provider 为进程级单一事实来源：路径由配置决定，结果按路径缓存。"""
    path = _write_table(tmp_path / "geometry.json", carla_version="0.9.15")
    monkeypatch.setattr(settings, "scene_carla_vehicle_geometry_path", str(path))
    clear_vehicle_geometry_table_cache()
    try:
        loaded = get_vehicle_geometry_table()
        assert loaded.carla_version == "0.9.15"
        assert get_vehicle_geometry_table() is loaded
    finally:
        clear_vehicle_geometry_table_cache()
        monkeypatch.undo()
        clear_vehicle_geometry_table_cache()


# =====================================================================
# 二、车型 → 蓝图解析与尺寸推导
# =====================================================================


@pytest.mark.parametrize(
    "model",
    [
        "HUNTER_SE",
        "hunter_se",
        "Hunter SE",
        "hunter-se",
        "VEHICLE.HUNTER_SE.01",
        "hunter se",
    ],
)
def test_resolve_matches_by_mapping_or_normalized_name(model: str) -> None:
    """显式映射、蓝图 ID、展示名三条路径都要能命中，且对大小写/分隔符不敏感。"""
    table = _table()
    assert table.resolve(model) is not None, model
    assert table.resolve("NOT_A_MODEL") is None
    assert table.resolve("") is None
    assert table.resolve(None) is None


def test_envelope_is_derived_from_bounding_box() -> None:
    geometry = _table().resolve("HUNTER_SE")
    assert geometry is not None
    envelope = geometry.envelope_dict()
    assert envelope["front_edge_x_m"] == pytest.approx(2.2)
    assert envelope["rear_edge_x_m"] == pytest.approx(-1.8)
    assert envelope["roof_z_m"] == pytest.approx(1.45)
    assert envelope["bottom_z_m"] == pytest.approx(-0.05)
    assert envelope["side_edge_y_m"] == pytest.approx(0.9)
    assert envelope["half_length_m"] == pytest.approx(2.0)
    assert geometry.dimension_dict() == {"length_m": 4.0, "width_m": 1.8, "height_m": 1.5}


def test_dimension_falls_back_to_double_extent(tmp_path: Path) -> None:
    """未导出 dimension 时按 2 × extent 推导（仍是实机数据的算术结果，不是猜测）。"""
    entry = {
        "display_name": "No Dimension",
        "bounding_box": {
            "center": {"x": 0.0, "y": 0.0, "z": 0.0},
            "extent": {"x": 2.0, "y": 0.9, "z": 0.7},
        },
    }
    table = load_vehicle_geometry_table(
        _write_table(
            tmp_path / "d.json",
            blueprints={"vehicle.x": entry},
            model_to_blueprint={},
        )
    )
    geometry = table.resolve("vehicle.x")
    assert geometry is not None
    assert geometry.dimension_dict() == {"length_m": 4.0, "width_m": 1.8, "height_m": 1.4}


def test_normalize_model_key() -> None:
    assert normalize_model_key("  Hunter-SE /01 ") == "hunter_se_01"
    assert normalize_model_key(None) == ""


# =====================================================================
# 三、下发参考块
# =====================================================================


def test_resolve_requires_usable_table() -> None:
    """表未校准时一律不解析；已校准但无蓝图的表在解析阶段即被拒。"""
    assert _table(calibrated=False).resolve("HUNTER_SE") is None
    assert _table(calibrated=False, blueprints={}, mapping={}).usable() is False
    with pytest.raises(VehicleGeometryError):
        _table(blueprints={}, mapping={})


def test_reference_carries_real_dimensions_when_resolved() -> None:
    reference = build_sensor_mount_reference(config_payload(), table=_table())
    assert reference["status"] == STATUS_RESOLVED
    assert reference["mount_policy"] == MOUNT_POLICY_DERIVE_FROM_BBOX
    assert reference["blueprint"] == BLUEPRINT_ID
    assert reference["dimension_m"]["length_m"] == 4.0
    assert reference["bounding_box_m"]["extent"]["z"] == 0.75
    assert reference["envelope_m"]["roof_z_m"] == pytest.approx(1.45)
    assert "hint" not in reference  # 命中时不写提示


@pytest.mark.parametrize(
    ("table", "expected_status"),
    [
        (_table(calibrated=False), STATUS_TABLE_UNCALIBRATED),
        (_unrelated_table(), STATUS_BLUEPRINT_UNMATCHED),
    ],
    ids=["uncalibrated-table", "unmatched-model"],
)
def test_reference_omits_numbers_when_unresolved(
    table: VehicleGeometryTable, expected_status: str
) -> None:
    """未命中时只给状态与提示：仿真端不得把空信息当作尺寸。"""
    reference = build_sensor_mount_reference(config_payload(), table=table)
    assert reference["status"] == expected_status
    assert expected_status != STATUS_RESOLVED
    for key in ("dimension_m", "bounding_box_m", "envelope_m", "blueprint"):
        assert key not in reference
    assert GEOMETRY_UNCALIBRATED_HINT in reference["hint"]


def test_unavailable_reference_has_no_numbers() -> None:
    reference = unavailable_reference("车辆几何表文件不存在: /app/config/x.json")
    assert reference["status"] == STATUS_TABLE_UNAVAILABLE
    assert reference["calibrated"] is False
    assert "dimension_m" not in reference
    assert "不存在" in reference["hint"]


def test_reference_is_pure_function() -> None:
    config = config_payload()
    snapshot = json.dumps(config, sort_keys=True)
    build_sensor_mount_reference(config, table=_table())
    assert json.dumps(config, sort_keys=True) == snapshot, "构造参考块不得修改场景配置"


# =====================================================================
# 四、下发链路：附加信息、不阻断
# =====================================================================


@pytest.mark.asyncio
async def test_run_scene_attaches_uncalibrated_reference_by_default(
    client: AsyncClient, scene_env: Any
) -> None:
    """出厂状态（表未校准）：下发照常成功，但参考块只标状态、不给数字。"""
    model = make_scene_model(
        scene_name="挂载参考默认场景", status=SceneStatus.PUBLISHED, config=config_payload()
    )
    scene_env.repository.rows[model.scene_id] = model

    resp = await client.post(f"{SCENE_BASE}/{model.scene_id}/run", headers=ADMIN_HEADERS)
    assert resp.status_code == 200, resp.text

    reference = scene_env.carla.created[0]["scene_config"][SENSOR_MOUNT_REFERENCE_KEY]
    assert reference["status"] == STATUS_TABLE_UNCALIBRATED, "出厂骨架即未校准状态"
    assert "dimension_m" not in reference
    # 不影响任何既有字段
    assert scene_env.carla.created[0]["scene_config"]["spawn_semantics"] == "absolute"
    assert scene_env.carla.created[0]["scene_config"]["map"]["spawn_point"]["x"] == pytest.approx(
        120.5
    )


@pytest.mark.asyncio
async def test_run_scene_dispatches_geometry_when_table_calibrated(
    client: AsyncClient, scene_env: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _write_table(tmp_path / "geometry.json")
    monkeypatch.setattr(settings, "scene_carla_vehicle_geometry_path", str(path))
    clear_vehicle_geometry_table_cache()
    model = make_scene_model(
        scene_name="挂载参考已校准场景", status=SceneStatus.PUBLISHED, config=config_payload()
    )
    scene_env.repository.rows[model.scene_id] = model
    try:
        resp = await client.post(f"{SCENE_BASE}/{model.scene_id}/run", headers=ADMIN_HEADERS)
        assert resp.status_code == 200, resp.text

        dispatched = scene_env.carla.created[0]["scene_config"]
        reference = dispatched[SENSOR_MOUNT_REFERENCE_KEY]
        assert reference["status"] == STATUS_RESOLVED
        assert reference["blueprint"] == BLUEPRINT_ID
        assert reference["envelope_m"]["front_edge_x_m"] == pytest.approx(2.2)
        assert reference["source_hash"] == hashlib.sha256(path.read_bytes()).hexdigest()
        # 库内存储不含参考块（仅下发载荷附加）
        assert SENSOR_MOUNT_REFERENCE_KEY not in model.config_json
    finally:
        clear_vehicle_geometry_table_cache()
        monkeypatch.undo()
        clear_vehicle_geometry_table_cache()


@pytest.mark.asyncio
async def test_run_scene_survives_broken_geometry_table(
    client: AsyncClient, scene_env: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """几何表损坏只降级参考块状态，绝不阻断下发（与出生点硬拦解耦）。"""
    broken = tmp_path / "geometry.json"
    broken.write_text("{not-json", encoding="utf-8")
    monkeypatch.setattr(settings, "scene_carla_vehicle_geometry_path", str(broken))
    clear_vehicle_geometry_table_cache()
    model = make_scene_model(
        scene_name="几何表损坏场景", status=SceneStatus.PUBLISHED, config=config_payload()
    )
    scene_env.repository.rows[model.scene_id] = model
    try:
        resp = await client.post(f"{SCENE_BASE}/{model.scene_id}/run", headers=ADMIN_HEADERS)
        assert resp.status_code == 200, resp.text

        reference = scene_env.carla.created[0]["scene_config"][SENSOR_MOUNT_REFERENCE_KEY]
        assert reference["status"] == STATUS_TABLE_UNAVAILABLE
        assert "dimension_m" not in reference
    finally:
        clear_vehicle_geometry_table_cache()
        monkeypatch.undo()
        clear_vehicle_geometry_table_cache()
