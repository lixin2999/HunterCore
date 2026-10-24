"""V1.23.1 加固回归（代码审查 3 P0 / 8 P1 / 7 P2 的修复钉桩）。

覆盖：车辆参数单一事实来源、esmini 干跑 skipped 语义、G3 几何围栏、G7 真实性口径、
overrides 全量早失败校验、CI 饱和标记、任务归属/协作取消/启动对账、审计哈希链、
批量入库与复检、sync 状态语义、发布与基线锁定 RBAC、错误摘要脱敏。
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import sys
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy.exc import IntegrityError

from app.config import settings
from app.models.scene_engine import (
    ConcreteScenario,
    GenerationTaskStatus,
    LogicalScenarioStatus,
    SuiteStatus,
)
from app.repositories.engine import CONCRETE_DEFERRED, _is_unique_violation
from app.scene_engine.catalog import CATALOG_BY_ID, CATALOG_VERSION
from app.scene_engine.compiler import default_values
from app.scene_engine.esmini import EsminiRunner, build_xosc
from app.scene_engine.generator import (
    GenerationOptions,
    SceneGenerator,
    ensure_overrides_shape,
)
from app.scene_engine.ir import (
    BindingIR,
    ConstraintIR,
    DistributionIR,
    ParameterIR,
    SceneIR,
    compile_parameters,
)
from app.scene_engine.validators import run_gates
from app.scene_engine.vehicle_params import (
    DEFAULT_VEHICLE_PARAMS_PATH,
    VehicleParamsError,
    clear_vehicle_params_cache,
    get_vehicle_params,
    load_vehicle_params,
)
from app.schemas.engine import GenerateRequest
from app.services.engine import _safe_error_message
from app.tests.conftest import ADMIN_HEADERS, OPERATOR_HEADERS, REVIEWER_HEADERS

VALID_BASE_CONFIG: dict[str, Any] = {
    "map": {
        "map_id": "Town01",
        "map_type": "carla_town",
        "spawn_point": {"x": 0.0, "y": 0.0, "z": 0.0, "yaw": 0.0},
    },
    "ego_vehicle": {"model": "HUNTER_SE", "initial_speed": 0.3, "initial_steer": 0.0},
    "weather": {
        "cloudiness": 0.0,
        "rain": 0.0,
        "wetness": 0.0,
        "fog": 0.0,
        "wind": 0.0,
        "sun_azimuth": 0.0,
        "sun_altitude": 45.0,
    },
    "actors": [
        {
            "actor_id": "lead",
            "type": "vehicle",
            "spawn_point": {"x": 6.0, "y": 0.0, "z": 0.0, "yaw": 0.0},
            "behavior": {"behavior_type": "constant_velocity", "parameters": {}},
        }
    ],
    "events": [],
    "success_criteria": {
        "max_speed_deviation": 1.0,
        "no_collision": True,
        "min_safe_distance": 1.0,
    },
    "duration": 30.0,
}


def _ir(*, with_constraint: bool = True, gap_low: float = 1.0) -> SceneIR:
    """共用 IR：2 个数值参数 + 1 个分类参数（含可选线性约束 gap - v0 >= 1.0）。"""
    parameters = [
        ParameterIR(
            name="v0",
            distribution=DistributionIR(kind="uniform", params={"low": 0.0, "high": 0.5}),
        ),
        ParameterIR(
            name="gap",
            distribution=DistributionIR(kind="uniform", params={"low": gap_low, "high": 10.0}),
        ),
        ParameterIR(
            name="surface",
            distribution=DistributionIR(
                kind="categorical",
                params={"choices": [{"value": "dry"}, {"value": "wet"}]},
            ),
        ),
    ]
    constraints = (
        [ConstraintIR(name="gap_ge_v0", terms={"gap": 1.0, "v0": -1.0}, op=">=", value=1.0)]
        if with_constraint
        else []
    )
    return SceneIR(
        logical_id="LS-TEST-001",
        name="加固回归用例场景",
        category="basic",
        scene_type="straight_cruise",
        parameters=parameters,
        constraints=constraints,
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="actors[0].spawn_point.x", expr="gap"),
        ],
        base_config=json.loads(json.dumps(VALID_BASE_CONFIG)),
        oracle_ref="oracle.task.speed_tracking/v1",
        criticality={"speed_param": "v0", "gap_param": "gap", "d_margin": 1.0},
    )


def _gate(report: Any, gate: str) -> Any:
    return next(item for item in report.gates if item.gate == gate)


# =====================================================================
# 一、车辆参数单一事实来源（P0）
# =====================================================================


def _write_params(path: Path, **overrides: Any) -> Path:
    payload: dict[str, Any] = {
        "profile": "HUNTER_SE",
        "wheelbase_m": 0.46,
        "min_turn_radius_m": 1.9,
        "max_steer_deg": 22.0,
        "max_decel_mps2": 1.5,
        "max_speed_mps": 4.8,
        "max_lateral_accel_mps2": 4.0,
        "source": "vehicle-export",
    }
    payload.update(overrides)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def test_vehicle_params_source_hash_tracks_file_bytes(tmp_path: Path) -> None:
    first = load_vehicle_params(_write_params(tmp_path / "a.json"))
    same = load_vehicle_params(_write_params(tmp_path / "b.json"))
    changed = load_vehicle_params(_write_params(tmp_path / "c.json", max_decel_mps2=2.0))
    assert first.profile == "HUNTER_SE"
    assert first.source_hash == hashlib.sha256((tmp_path / "a.json").read_bytes()).hexdigest()
    # 内容一致 → 哈希一致（与文件名无关）；内容变化 → 哈希变化
    assert first.source_hash == same.source_hash
    assert changed.source_hash != first.source_hash
    assert first.to_dict()["max_speed_mps"] == 4.8


def test_vehicle_params_takes_conservative_steer_limit(tmp_path: Path) -> None:
    """声明转角大于几何可实现转角时取较小者（G4 保守）。"""
    declared_only = load_vehicle_params(_write_params(tmp_path / "a.json", min_turn_radius_m=0.3))
    assert declared_only.max_steer_rad == pytest.approx(math.radians(22.0))
    geometric = load_vehicle_params(_write_params(tmp_path / "b.json", max_steer_deg=80.0))
    assert geometric.max_steer_rad == pytest.approx(math.atan(0.46 / 1.9))
    assert geometric.max_steer_rad < math.radians(80.0)


@pytest.mark.parametrize(
    "overrides",
    [
        {"wheelbase_m": 0},
        {"max_decel_mps2": -1.5},
        {"max_steer_deg": 90.0},
        {"min_turn_radius_m": "1.9"},
        {"max_speed_mps": float("inf")},
    ],
    ids=["zero-wheelbase", "negative-decel", "steer-90", "string-radius", "infinite-speed"],
)
def test_vehicle_params_rejects_illegal_values(tmp_path: Path, overrides: dict[str, Any]) -> None:
    with pytest.raises(VehicleParamsError):
        load_vehicle_params(_write_params(tmp_path / "p.json", **overrides))


def test_vehicle_params_rejects_missing_or_broken_file(tmp_path: Path) -> None:
    with pytest.raises(VehicleParamsError):
        load_vehicle_params(tmp_path / "not-found.json")
    broken = tmp_path / "broken.json"
    broken.write_text("{not-json", encoding="utf-8")
    with pytest.raises(VehicleParamsError):
        load_vehicle_params(broken)
    array = tmp_path / "array.json"
    array.write_text("[1, 2]", encoding="utf-8")
    with pytest.raises(VehicleParamsError):
        load_vehicle_params(array)


def test_get_vehicle_params_follows_configured_path(tmp_path: Path, monkeypatch) -> None:
    """provider 为进程级单一事实来源：路径由配置决定，结果按路径缓存。"""
    path = _write_params(tmp_path / "vehicle.json", profile="HUNTER_SE_ALT")
    monkeypatch.setattr(settings, "scene_vehicle_params_path", str(path))
    clear_vehicle_params_cache()
    try:
        loaded = get_vehicle_params()
        assert loaded.profile == "HUNTER_SE_ALT"
        assert get_vehicle_params() is loaded
    finally:
        clear_vehicle_params_cache()


# =====================================================================
# 一b、内置默认车辆参数必须随镜像构建（V1.23.1 实机回归）
# =====================================================================


def test_packaged_default_vehicle_params_is_loadable() -> None:
    """源码布局下包内默认文件可加载（回落路径存在，非仅靠挂载）。"""
    assert DEFAULT_VEHICLE_PARAMS_PATH.is_file()
    assert load_vehicle_params(DEFAULT_VEHICLE_PARAMS_PATH).profile == "HUNTER_SE"


def test_scene_service_dockerfile_ships_vehicle_params_config() -> None:
    """容器内 DEFAULT 路径解析为 /app/config/vehicle_params.json（__file__=/app/app/... 的 parents[2]），
    而 app 包不含服务根级 config/——若 Dockerfile 不显式 COPY config 到 /app/config，scene-service 会启动即崩溃。
    本测试钉住该构建行，防止“测试全绿但镜像缺文件”的实机回归（V1.23.1 实机首发）。"""
    dockerfile = Path(__file__).resolve().parents[2] / "Dockerfile"
    text = dockerfile.read_text(encoding="utf-8")
    assert "WORKDIR /app" in text
    assert re.search(r"COPY\s+--from=builder\s+/build/service/config\s+\./config", text)


# =====================================================================
# 二、G5 esmini 干跑语义（P1）
# =====================================================================


def test_esmini_unconfigured_or_missing_binary_is_skipped_not_failed() -> None:
    config = json.loads(json.dumps(VALID_BASE_CONFIG))
    for command in ("", "   ", "esmini-not-installed --headless"):
        result = EsminiRunner(command, timeout_seconds=1.0).run(config)
        assert result.skipped is True
        assert result.passed is True
        assert "跳过" in result.message


def test_esmini_timeout_is_failure_not_skip(tmp_path: Path) -> None:
    """真实子进程超时必须判失败（skipped=False），不能当作"未部署"放行。"""
    script = tmp_path / "slow.py"
    script.write_text("import time\ntime.sleep(5)\n", encoding="utf-8")
    runner = EsminiRunner(f'"{sys.executable}" "{script}"', timeout_seconds=0.2)
    result = runner.run(json.loads(json.dumps(VALID_BASE_CONFIG)))
    assert result.skipped is False
    assert result.passed is False
    assert "超时" in result.message


def test_build_xosc_renders_pedestrian_entity_and_stop_trigger() -> None:
    config = json.loads(json.dumps(VALID_BASE_CONFIG))
    config["duration"] = 12.5
    config["actors"] = [
        {
            "actor_id": "ped",
            "type": "pedestrian",
            "spawn_point": {"x": 3.0, "y": 1.0, "z": 0.0, "yaw": 0.0},
            "behavior": {"behavior_type": "cross_road", "parameters": {"crossing_speed": 1.2}},
        }
    ]
    xosc = build_xosc(config)
    assert '<Pedestrian name="ped"' in xosc
    assert '<Vehicle name="ego"' in xosc
    assert 'SimulationTimeCondition value="12.500"' in xosc
    assert 'filepath="road.xodr"' in xosc


# =====================================================================
# 三、G3 围栏 / G7 真实性口径（P1）
# =====================================================================


def _report(
    ir: SceneIR | None = None,
    *,
    bounds: tuple[float, float, float, float] | None = None,
    values: dict[str, float | str] | None = None,
    mutate: Callable[[dict[str, Any]], None] | None = None,
    esmini: EsminiRunner | None = None,
) -> Any:
    ir = ir or _ir()
    resolved = dict(default_values(ir))
    resolved.update(values or {})
    config = compile_parameters(ir, resolved)
    if mutate is not None:
        mutate(config)
    return run_gates(
        ir,
        resolved,
        config,
        max_duration=settings.scene_duration_max_seconds,
        bounds=bounds,
        esmini=esmini,
    )


def test_g3_fence_rejects_out_of_map_spawn_point() -> None:
    report = _report(bounds=(0.0, 0.0, 4.0, 4.0))
    assert _gate(report, "G3").passed is False
    assert "超出地图围栏" in _gate(report, "G3").message
    assert report.passed is False


def test_g3_fence_accepts_within_bounds_and_rejects_non_finite_point() -> None:
    assert _report(bounds=(0.0, 0.0, 1000.0, 1000.0)).passed is True

    # NaN 能通过 SceneConfig 的 float 字段，但必须被 G3 的“生成点有限”检查拦下
    def break_point(config: dict[str, Any]) -> None:
        config["map"]["spawn_point"]["x"] = math.nan

    g3 = _gate(_report(mutate=break_point, bounds=(0.0, 0.0, 10.0, 10.0)), "G3")
    assert g3.passed is False
    assert "非数值" in g3.message


def test_g7_applies_pedestrian_speed_limit_by_actor_type() -> None:
    """SceneConfig 参与者类型字段是 ``type``：行人 4 m/s 判失败，车辆同样 4 m/s 放行。"""

    def as_pedestrian(config: dict[str, Any]) -> None:
        config["actors"][0]["type"] = "pedestrian"
        config["actors"][0]["behavior"]["parameters"]["target_speed"] = 4.0

    pedestrian = _report(mutate=as_pedestrian)
    assert _gate(pedestrian, "G7").passed is False
    assert "4.0" in _gate(pedestrian, "G7").message

    def as_vehicle(config: dict[str, Any]) -> None:
        config["actors"][0]["type"] = "vehicle"
        config["actors"][0]["behavior"]["parameters"]["target_speed"] = 4.0

    assert _gate(_report(mutate=as_vehicle), "G7").passed is True


def test_g7_uses_vehicle_max_speed_for_ego() -> None:
    limit = get_vehicle_params().max_speed

    def too_fast(config: dict[str, Any]) -> None:
        config["ego_vehicle"]["initial_speed"] = limit + 0.5

    report = _report(mutate=too_fast)
    assert _gate(report, "G7").passed is False
    assert f"[0, {limit:.2f}]" in _gate(report, "G7").message


def test_skipped_gate_does_not_fail_report() -> None:
    """未部署 esmini 的 G5 skipped：不计失败，但在报告中显式标注。"""
    baseline = _report()
    assert baseline.passed is True
    skipped = _report(esmini=EsminiRunner("esmini-not-installed"))
    g5 = _gate(skipped, "G5")
    assert g5.skipped is True
    assert g5.passed is True
    assert skipped.passed is True
    assert skipped.failures == []


# =====================================================================
# 四、overrides 全量早失败校验（P2）
# =====================================================================


@pytest.mark.parametrize(
    "payload",
    [
        {"typo_field": 1},
        {"fixed": {"unknown_speed": 0.1}},
        {"fixed": {"v0": 9.9}},
        {"fixed": {"surface": "icy"}},
        {"fixed": {"v0": "0.1"}},
        {"fixed": {"v0": 0.5, "gap": 1.0}},
        {"ranges": {"unknown_speed": {"low": 0.0, "high": 0.2}}},
        {"ranges": {"surface": {"low": 0.0, "high": 1.0}}},
        {"ranges": {"v0": {"low": 0.4, "high": 0.2}}},
        {"ranges": {"v0": {"low": 0.0, "high": 99.0}}},
        {"ranges": {"v0": {}}},
        {"candidates": []},
        {"candidates": "not-a-list"},
        {"candidates": [{"unknown_speed": 0.1}]},
        {"candidates": [{"v0": 99.0}]},
        {"base": {"unknown_speed": 0.1}},
        {"base": {"v0": 99.0}},
        {"grid_bins": 0},
        {"grid_bins": 65},
        {"grid_bins": "10"},
        {"dedup_distance": -0.1},
        {"mutation_scale": 0.0},
        {"mutation_scale": 2.0},
        {"target_zone": "Z9"},
    ],
)
def test_ensure_overrides_shape_rejects(payload: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        ensure_overrides_shape(payload, _ir())


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"fixed": {"v0": 0.2, "surface": "wet"}},
        {"ranges": {"v0": {"low": 0.1, "high": 0.4}}},
        {"ranges": {"gap": {"high": 8.0}}},
        {"candidates": [{"v0": 0.2, "gap": 6.0}]},
        {"base": {"v0": 0.2, "gap": 6.0}, "mutation_scale": 0.2},
        {"grid_bins": 20, "dedup_distance": 0.1, "target_zone": "Z3"},
        {"fixed": {"v0": 0.5}},
    ],
)
def test_ensure_overrides_shape_accepts(payload: dict[str, Any]) -> None:
    ensure_overrides_shape(payload, _ir())


def test_ensure_overrides_shape_without_ir_only_checks_structure() -> None:
    ensure_overrides_shape({"fixed": {"any_name": 123.0}})
    with pytest.raises(ValueError):
        ensure_overrides_shape({"fixed": 5})


def test_generator_marks_ci_saturation_instead_of_infinite_value() -> None:
    """净距 <= d_margin → a_req 为无穷：以 999.0 落库并打 ci_saturated 标记。"""
    ir = _ir(with_constraint=False)
    generator = SceneGenerator(max_duration=settings.scene_duration_max_seconds)
    outcome = generator.generate(
        ir,
        GenerationOptions(
            logical_version="1.0.0",
            count=1,
            strategy="ai_generated",
            profile="regression",
            seed=1,
            overrides={"candidates": [{"v0": 0.4, "gap": 1.0005, "surface": "dry"}]},
        ),
    )
    assert outcome.produced == 1, outcome.gate_failures
    row = outcome.rows[0]
    assert row["criticality_ci"] == 999.0
    assert row["generator"]["ci_saturated"] is True
    assert row["criticality_level"] == "C3"
    assert row["avoidability"] == "unavoidable"


def test_generator_row_name_and_vehicle_profile_are_deterministic() -> None:
    ir = _ir()
    generator = SceneGenerator(max_duration=settings.scene_duration_max_seconds)
    options = GenerationOptions(
        logical_version="1.0.0",
        count=1,
        strategy="ai_generated",
        profile="regression",
        seed=1,
        overrides={"candidates": [{"v0": 0.2, "gap": 6.0, "surface": "dry"}]},
    )
    first = generator.generate(ir, options).rows[0]
    second = generator.generate(ir, options).rows[0]
    expected_index = int(first["content_hash"][:8], 16) % 10000
    assert first["name"] == f"LS-TEST-001-{first['rarity_zone']}-{expected_index:04d}"
    assert first["name"] == second["name"]
    assert first["generator"]["vehicle_params_hash"] == get_vehicle_params().source_hash


# =====================================================================
# 五、任务归属 / 协作取消 / 启动对账（P1）
# =====================================================================


def _service_with_generator(scene_env: Any, generator: Any) -> Any:
    return type(scene_env.engine)(
        scene_env.logical_repository,
        scene_env.concrete_repository,
        scene_env.task_repository,
        scene_env.testcase_repository,
        scene_env.suite_repository,
        settings,
        generator=generator,
    )


async def _pending_task(scene_env: Any, seed: int = 7) -> Any:
    return await scene_env.engine.create_generation_task(
        GenerateRequest(logical_id="LS-BAS-001", count=2, seed=seed),
        user_id="22222222-2222-4222-8222-222222222222",
    )


async def test_task_claim_is_exclusive_per_owner(scene_env: Any) -> None:
    repo = scene_env.task_repository
    task = await _pending_task(scene_env)
    claimed = await repo.claim(task.task_id, "instance-a")
    assert claimed is not None
    assert claimed.owner_id == "instance-a"
    assert claimed.status == GenerationTaskStatus.RUNNING.value
    # 其他实例不得重复抢占（多副本不双跑）
    assert await repo.claim(task.task_id, "instance-b") is None
    # 同实例可重入（归属自己的 running 任务继续持有）
    assert await repo.claim(task.task_id, "instance-a") is not None


async def test_cancel_pending_vs_running_semantics(scene_env: Any) -> None:
    repo = scene_env.task_repository
    pending = await _pending_task(scene_env)
    assert await repo.request_cancel(pending.task_id) is not None
    assert repo.rows[pending.task_id].status == GenerationTaskStatus.CANCELED.value

    running = await _pending_task(scene_env, seed=8)
    await repo.claim(running.task_id, settings.task_owner)
    await repo.request_cancel(running.task_id)
    assert repo.rows[running.task_id].status == GenerationTaskStatus.RUNNING.value
    assert repo.rows[running.task_id].cancel_requested is True
    assert await repo.is_cancel_requested(running.task_id) is True


async def test_reconcile_stale_only_touches_timed_out_tasks(scene_env: Any) -> None:
    repo = scene_env.task_repository
    fresh = await _pending_task(scene_env, seed=9)
    stale = await _pending_task(scene_env, seed=10)
    repo.rows[stale.task_id].update_time = datetime.now(UTC) - timedelta(hours=2)
    finished = await _pending_task(scene_env, seed=11)
    await repo.update(finished.task_id, status=GenerationTaskStatus.SUCCEEDED.value)

    assert await scene_env.engine.reconcile_generation_tasks() == 1
    assert repo.rows[stale.task_id].status == GenerationTaskStatus.FAILED.value
    assert "失联" in (repo.rows[stale.task_id].error_message or "")
    assert repo.rows[fresh.task_id].status == GenerationTaskStatus.PENDING.value
    assert repo.rows[finished.task_id].status == GenerationTaskStatus.SUCCEEDED.value


async def test_cancel_before_generation_drops_products(scene_env: Any) -> None:
    repo = scene_env.task_repository
    task = await _pending_task(scene_env)
    repo.rows[task.task_id].cancel_requested = True
    await scene_env.engine.execute_generation_task(task.task_id)
    assert repo.rows[task.task_id].status == GenerationTaskStatus.CANCELED.value
    assert scene_env.concrete_repository.rows == {}


async def test_cancel_during_generation_discards_rows(scene_env: Any) -> None:
    """产物入库前回查取消：置 canceled 且不写入任何具体场景。"""
    repo = scene_env.task_repository
    inner = SceneGenerator(max_duration=settings.scene_duration_max_seconds)
    task = await _pending_task(scene_env, seed=12)

    class _MidwayCancel:
        def generate(self, ir: SceneIR, options: GenerationOptions, *, on_progress: Any = None):
            outcome = inner.generate(ir, options, on_progress=on_progress)
            # 先确认“本轮确有产物”，否则“未写入”不能说明取消生效
            assert outcome.produced > 0
            repo.rows[task.task_id].cancel_requested = True
            return outcome

    await _service_with_generator(scene_env, _MidwayCancel()).execute_generation_task(task.task_id)
    assert repo.rows[task.task_id].status == GenerationTaskStatus.CANCELED.value
    assert scene_env.concrete_repository.rows == {}


async def test_unexpected_failure_message_is_sanitized(scene_env: Any) -> None:
    repo = scene_env.task_repository
    task = await _pending_task(scene_env, seed=14)

    class _Boom:
        def generate(self, ir: SceneIR, options: GenerationOptions, *, on_progress: Any = None):
            raise RuntimeError("INSERT INTO scene_engine.x VALUES ('secret-detail')")

    await _service_with_generator(scene_env, _Boom()).execute_generation_task(task.task_id)
    row = repo.rows[task.task_id]
    assert row.status == GenerationTaskStatus.FAILED.value
    assert row.error_message == "RuntimeError: 生成失败（详见服务日志）"


def test_safe_error_message_keeps_business_errors_readable() -> None:
    from hunter_common.exceptions import InvalidParameterError

    # 业务错误保留原文（含错误码前缀），便于前端直接展示
    assert _safe_error_message(InvalidParameterError("参数越界")) == "[2001] 参数越界"
    assert _safe_error_message(ValueError("count 必须在 1..5000 之间")) == "count 必须在 1..5000 之间"
    # 未知异常只留类型名，不外泄堆栈/SQL
    assert _safe_error_message(KeyError("k")) == "KeyError: 生成失败（详见服务日志）"
    leaked = _safe_error_message(RuntimeError("SELECT * FROM scene_engine.audit_logs"))
    assert "audit_logs" not in leaked


# =====================================================================
# 六、审计哈希链 / sync 状态语义 / 批量入库（P1、P2）
# =====================================================================


def _canonical(row: Any) -> str:
    return json.dumps(
        {
            "action": row.action,
            "target_type": row.target_type,
            "target_id": row.target_id,
            "actor": row.actor,
            "payload": dict(row.payload or {}),
        },
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )


async def test_audit_chain_links_entries_by_hash(scene_env: Any) -> None:
    await scene_env.engine.sync_catalog(created_by="ops")
    await scene_env.engine.publish("LS-BAS-001", None, user_id="reviewer-1")
    rows = scene_env.audit_repository.rows
    assert [row.action for row in rows] == ["sync_catalog", "publish"]
    assert rows[0].prev_hash == "0" * 64
    for index, row in enumerate(rows):
        assert row.prev_hash == (rows[index - 1].entry_hash if index else "0" * 64)
        assert row.entry_hash == hashlib.sha256(
            f"{row.prev_hash}:{_canonical(row)}".encode()
        ).hexdigest()
    assert rows[1].target_id == f"LS-BAS-001@{CATALOG_VERSION}"


async def test_sync_catalog_inserts_draft_and_keeps_published(scene_env: Any) -> None:
    """取舍④：同步只补 draft，已发布版本的状态不得被目录同步回退。"""
    repo = scene_env.logical_repository
    key = ("LS-BAS-001", CATALOG_VERSION)
    await scene_env.engine.sync_catalog(created_by="ops")
    assert repo.rows[key].status == LogicalScenarioStatus.DRAFT.value

    await repo.update_status("LS-BAS-001", CATALOG_VERSION, LogicalScenarioStatus.PUBLISHED.value)
    await scene_env.engine.sync_catalog(created_by="ops")
    assert repo.rows[key].status == LogicalScenarioStatus.PUBLISHED.value


async def test_create_many_skips_only_duplicate_hashes(scene_env: Any) -> None:
    outcome = SceneGenerator(max_duration=settings.scene_duration_max_seconds).generate(
        CATALOG_BY_ID["LS-BAS-001"],
        GenerationOptions(
            logical_version=CATALOG_VERSION,
            count=3,
            strategy="distribution",
            profile="regression",
            seed=5,
        ),
    )
    assert outcome.produced >= 2
    created = await scene_env.concrete_repository.create_many(list(outcome.rows))
    assert len(created) == outcome.produced
    assert all(isinstance(item, UUID) for item in created)
    # 全部哈希冲突 → 返回空列表（不抛错、不重复入库）
    assert await scene_env.concrete_repository.create_many(list(outcome.rows)) == []
    assert len(scene_env.concrete_repository.rows) == outcome.produced


class _UniqueViolation(Exception):
    sqlstate = "23505"


class _CheckViolation(Exception):
    sqlstate = "23514"


def test_unique_violation_detection_is_sqlstate_scoped() -> None:
    """只有 23505（唯一冲突）被吞；CHECK/外键等完整性错误必须上抛。"""
    assert _is_unique_violation(IntegrityError("INSERT", {}, _UniqueViolation())) is True
    assert _is_unique_violation(IntegrityError("INSERT", {}, _CheckViolation())) is False
    assert _is_unique_violation(
        IntegrityError("INSERT", {}, Exception("asyncpg 错误 23505: duplicate key"))
    ) is True
    assert _is_unique_violation(
        IntegrityError("INSERT", {}, Exception("violates check constraint ck_x"))
    ) is False


def test_concrete_deferred_columns_are_real_columns() -> None:
    """列表/统计路径延迟加载的大字段必须与模型列一致（防止改名后静默失效）。"""
    for name in CONCRETE_DEFERRED:
        assert getattr(ConcreteScenario, name) is not None
    assert {"param_values", "scene_config"} <= set(CONCRETE_DEFERRED)


# =====================================================================
# 七、端点契约（RBAC / 任务字段 / 复检顺序）
# =====================================================================


async def test_publish_requires_review_role(client: AsyncClient) -> None:
    forbidden = await client.post(
        "/api/v1/scene/logical/LS-BAS-001/publish", headers=OPERATOR_HEADERS
    )
    assert forbidden.status_code == 403
    assert forbidden.json()["code"] == 1002

    allowed = await client.post(
        "/api/v1/scene/logical/LS-BAS-001/publish", headers=REVIEWER_HEADERS
    )
    assert allowed.status_code == 200
    assert allowed.json()["data"]["status"] == "published"


async def test_suite_lock_requires_review_role(client: AsyncClient) -> None:
    created = await client.post(
        "/api/v1/scene/suites", headers=OPERATOR_HEADERS, json={"name": "锁定权限集"}
    )
    suite_id = created.json()["data"]["suite_id"]
    await client.post(
        "/api/v1/scene/testcases",
        headers=OPERATOR_HEADERS,
        json={
            "case_key": "TC-RBAC-001",
            "name": "权限用例",
            "logical_id": "LS-BAS-001",
            "objective": "验证锁定权限",
            "preconditions": {},
            "steps": [],
            "platforms": ["simulation"],
            "oracle_ref": "oracle.safety.no_collision/v1",
            "timeout_seconds": 60,
            "tags": [],
        },
    )
    added = await client.post(
        f"/api/v1/scene/suites/{suite_id}/items",
        headers=OPERATOR_HEADERS,
        json={"items": [{"item_id": "item-1", "item_type": "test_case", "ref_id": "TC-RBAC-001"}]},
    )
    assert added.status_code == 200

    forbidden = await client.post(
        f"/api/v1/scene/suites/{suite_id}/lock", headers=OPERATOR_HEADERS, json={}
    )
    assert forbidden.status_code == 403
    assert forbidden.json()["code"] == 1002

    locked = await client.post(
        f"/api/v1/scene/suites/{suite_id}/lock", headers=REVIEWER_HEADERS, json={}
    )
    assert locked.status_code == 200
    assert locked.json()["data"]["status"] == SuiteStatus.LOCKED.value


async def test_task_detail_exposes_owner_and_cancel_flag(client: AsyncClient) -> None:
    accepted = await client.post(
        "/api/v1/scene/generate",
        headers=OPERATOR_HEADERS,
        json={"logical_id": "LS-BAS-001", "count": 2, "seed": 3},
    )
    task_id = accepted.json()["data"]["task_id"]
    detail = (await client.get(f"/api/v1/scene/generate/{task_id}", headers=ADMIN_HEADERS)).json()[
        "data"
    ]
    assert detail["status"] == "succeeded"
    assert detail["owner_id"] == settings.task_owner
    assert detail["cancel_requested"] is False


async def test_cancel_running_task_sets_cooperative_flag(
    client: AsyncClient, scene_env: Any
) -> None:
    task = await _pending_task(scene_env, seed=21)
    await scene_env.task_repository.claim(task.task_id, settings.task_owner)
    resp = await client.post(
        f"/api/v1/scene/generate/{task.task_id}/cancel", headers=OPERATOR_HEADERS
    )
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["status"] == GenerationTaskStatus.RUNNING.value
    assert data["cancel_requested"] is True


async def test_batch_validate_keeps_request_order_and_flags_unknown(
    client: AsyncClient,
) -> None:
    accepted = await client.post(
        "/api/v1/scene/generate",
        headers=OPERATOR_HEADERS,
        json={"logical_id": "LS-BAS-001", "count": 2, "seed": 23},
    )
    task_id = accepted.json()["data"]["task_id"]
    produced = (await client.get(f"/api/v1/scene/generate/{task_id}", headers=ADMIN_HEADERS)).json()[
        "data"
    ]["produced"]
    listed = (
        await client.get(
            "/api/v1/scene/concrete",
            headers=ADMIN_HEADERS,
            params={"logical_id": "LS-BAS-001", "page_size": 50},
        )
    ).json()["data"]["items"]
    known = [item["scenario_id"] for item in listed][:produced]
    assert len(known) == produced >= 1
    request_ids = [*known, str(uuid4())]

    resp = await client.post(
        "/api/v1/scene/concrete/batch-validate",
        headers=OPERATOR_HEADERS,
        json={"scenario_ids": request_ids},
    )
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert [item["scenario_id"] for item in data["items"]] == request_ids
    assert data["items"][-1]["passed"] is False
    assert any("3001" in failure for failure in data["items"][-1]["failures"])
    assert data["quarantined"] == 1
    assert data["validated"] == len(known)


async def test_generate_rejects_override_semantic_errors(client: AsyncClient) -> None:
    """IR 感知校验后，未知/越界参数的 overrides 在受理阶段即 2001（不再空跑）。"""
    unknown_param = await client.post(
        "/api/v1/scene/generate",
        headers=OPERATOR_HEADERS,
        json={"logical_id": "LS-BAS-001", "count": 1, "overrides": {"fixed": {"typo_v": 0.1}}},
    )
    assert unknown_param.status_code == 422
    assert unknown_param.json()["code"] == 2001

    out_of_range = await client.post(
        "/api/v1/scene/generate",
        headers=OPERATOR_HEADERS,
        json={"logical_id": "LS-BAS-001", "count": 1, "overrides": {"fixed": {"v0": 99.0}}},
    )
    assert out_of_range.status_code == 422
    assert "越界" in out_of_range.json()["message"]
