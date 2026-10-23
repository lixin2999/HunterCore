"""场景引擎核心单测：目录/IR/统计/校验门/生成器/编译。"""
from __future__ import annotations

import pytest

from app.config import settings
from app.scene_engine.catalog import CATALOG, CATALOG_BY_ID, CATALOG_VERSION
from app.scene_engine.compiler import default_values, render_scene
from app.scene_engine.generator import GenerationOptions, SceneGenerator
from app.scene_engine.ir import (
    BindingIR,
    ConstraintIR,
    DistributionIR,
    ExpressionError,
    ParameterIR,
    SceneIR,
    compile_parameters,
    evaluate_expression,
    expression_names,
)
from app.scene_engine.oracles import ORACLE_BY_REF, get_oracle, list_oracles
from app.scene_engine.stats import (
    beta_ppf,
    chi2_ppf,
    distribution_moments,
    distribution_quantile,
    mahalanobis_d2,
    normal_cdf,
    normal_ppf,
    rarity_zone,
)
from app.scene_engine.validators import (
    RISK_MATRIX,
    avoidability_label,
    content_hash,
    criticality_index,
    criticality_level,
    risk_level,
)


def _uniform_ir() -> SceneIR:
    """两参数均匀分布的最小 IR（供表达式/约束/编译用例）。"""
    return SceneIR(
        logical_id="LS-TEST-001",
        name="单测场景",
        category="basic",
        scene_type="straight_driving",
        parameters=[
            ParameterIR(
                name="v0",
                distribution=DistributionIR(
                    kind="uniform", params={"low": 4.0, "high": 12.0}
                ),
            ),
            ParameterIR(
                name="gap",
                distribution=DistributionIR(
                    kind="uniform", params={"low": 4.0, "high": 20.0}
                ),
            ),
        ],
        constraints=[
            ConstraintIR(name="gap_gt_v0", terms={"gap": 1.0, "v0": -1.0}, op=">=", value=1.0)
        ],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="actors[0].spawn_point.x", expr="gap + 4.0"),
        ],
        base_config={
            "map": {
                "map_id": "Town01",
                "map_type": "carla_town",
                "spawn_point": {"x": 0.0, "y": 0.0, "z": 0.0, "yaw": 0.0},
            },
            "ego_vehicle": {"model": "HUNTER_SE", "initial_speed": 5.0, "initial_steer": 0.0},
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
                    "actor_type": "vehicle",
                    "spawn_point": {"x": 10.0, "y": 0.0, "z": 0.0, "yaw": 0.0},
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
        },
        oracle_ref="oracle.safety.min_distance/v1",
        criticality={"speed_param": "v0", "gap_param": "gap"},
    )


# =====================================================================
# 目录 / IR
# =====================================================================


def test_catalog_version_matches_settings() -> None:
    assert CATALOG_VERSION == settings.scene_catalog_version


def test_catalog_has_17_scenarios_and_unique_ids() -> None:
    assert len(CATALOG) == 17
    assert len(CATALOG_BY_ID) == 17


@pytest.mark.parametrize("ir", CATALOG, ids=lambda ir: ir.logical_id)
def test_catalog_ir_passes_gates_at_median(ir: SceneIR) -> None:
    """内置目录必须全部通过 G1-G7（缺省参数 = 分布中位数）。"""
    values = default_values(ir)
    compilation = render_scene(
        ir, values, max_duration=settings.scene_duration_max_seconds
    )
    assert compilation.passed, compilation.report.failures


@pytest.mark.parametrize("ir", CATALOG, ids=lambda ir: ir.logical_id)
def test_catalog_oracle_refs_exist(ir: SceneIR) -> None:
    assert ir.oracle_ref in ORACLE_BY_REF


def test_expression_whitelist_rejects_attributes_and_unknown_names() -> None:
    assert expression_names("max(v0, 1.5) + gap") == {"v0", "gap"}
    with pytest.raises(ExpressionError):
        expression_names("__import__('os').system('ls')")
    with pytest.raises(ExpressionError):
        evaluate_expression("v0 + missing", {"v0": 1.0})


def test_ir_rejects_unknown_binding_parameter() -> None:
    payload = _uniform_ir().model_dump(mode="json")
    payload["bindings"][0]["expr"] = "unknown_param + 1"
    with pytest.raises(ValueError):
        SceneIR.model_validate(payload)


def test_compile_parameters_writes_bound_paths() -> None:
    ir = _uniform_ir()
    config = compile_parameters(ir, {"v0": 7.0, "gap": 12.0})
    assert config["ego_vehicle"]["initial_speed"] == 7.0
    assert config["actors"][0]["spawn_point"]["x"] == 16.0


# =====================================================================
# 统计 / 分级
# =====================================================================


def test_normal_and_chi2_quantiles_are_consistent() -> None:
    assert normal_cdf(0.0) == pytest.approx(0.5, abs=1e-6)
    assert normal_ppf(0.5) == pytest.approx(0.0, abs=1e-6)
    assert normal_ppf(0.975) == pytest.approx(1.96, abs=5e-3)
    assert chi2_ppf(0.5, 3) > 0
    assert beta_ppf(0.5, 2, 2) == pytest.approx(0.5, abs=1e-3)


def test_distribution_quantile_for_uniform_and_truncated() -> None:
    assert distribution_quantile("uniform", {"low": 0.0, "high": 10.0}, 0.5) == pytest.approx(5.0)
    mean, sigma = distribution_moments(
        "truncated_normal", {"mean": 10.0, "std": 2.0, "low": 0.0, "high": 30.0}
    )
    assert 9.0 < mean < 11.0
    assert sigma > 0


def test_mahalanobis_zero_at_mean() -> None:
    d2, z = mahalanobis_d2([10.0, 5.0], [10.0, 5.0], [2.0, 1.0], [], ["a", "b"])
    assert d2 == pytest.approx(0.0)
    assert z == [0.0, 0.0]


def test_rarity_zone_boundaries() -> None:
    assert rarity_zone(0.9) == "Z1"
    assert rarity_zone(0.5) == "Z1"
    assert rarity_zone(0.3) == "Z2"
    assert rarity_zone(0.05) == "Z3"
    assert rarity_zone(0.001) == "Z4"


def test_criticality_levels_and_avoidability() -> None:
    assert criticality_level(0.1) == "C0"
    assert criticality_level(0.4) == "C1"
    assert criticality_level(0.7) == "C2"
    assert criticality_level(1.2) == "C3"
    assert criticality_level(float("inf")) == "C3"
    assert avoidability_label(0.05) == "trivial_safe"
    assert avoidability_label(0.5) == "avoidable"
    assert avoidability_label(0.8) == "marginal"
    assert avoidability_label(0.99) == "unavoidable"


def test_criticality_index_formula() -> None:
    ir = _uniform_ir()
    # a_req = v^2 / (2*(gap - d_margin)); CI = a_req / max_decel
    values = {"v0": 10.0, "gap": 5.0}
    expected = (10.0**2 / (2 * (5.0 - ir.criticality.d_margin))) / ir.criticality.max_decel
    assert criticality_index(ir, values) == pytest.approx(expected)


def test_risk_matrix_is_monotonic() -> None:
    for zone in ("Z1", "Z2", "Z3", "Z4"):
        levels = [RISK_MATRIX[zone][f"C{index}"] for index in range(4)]
        assert levels == sorted(levels)
    assert risk_level("Z4", "C3") == "L4"
    assert risk_level("Z1", "C0") == "L1"


# =====================================================================
# 生成器
# =====================================================================


def test_generator_is_deterministic_for_same_seed() -> None:
    ir = CATALOG_BY_ID["LS-INT-001"]
    generator = SceneGenerator(max_duration=settings.scene_duration_max_seconds)
    options = GenerationOptions(
        logical_version=CATALOG_VERSION,
        count=8,
        strategy="distribution",
        profile="robustness",
        seed=20261023,
    )
    first = generator.generate(ir, options)
    second = generator.generate(ir, options)
    assert [row["content_hash"] for row in first.rows] == [
        row["content_hash"] for row in second.rows
    ]
    assert first.zone_counts == second.zone_counts


def test_generator_honors_exploration_quota() -> None:
    ir = CATALOG_BY_ID["LS-INT-001"]
    generator = SceneGenerator(max_duration=settings.scene_duration_max_seconds)
    options = GenerationOptions(
        logical_version=CATALOG_VERSION,
        count=20,
        strategy="distribution",
        profile="exploration",
        seed=7,
    )
    outcome = generator.generate(ir, options)
    assert outcome.produced <= 20
    assert outcome.attempts > 0
    assert outcome.zone_counts.get("Z3", 0) >= 1


def test_generator_rejects_unknown_overrides() -> None:
    ir = CATALOG_BY_ID["LS-BAS-001"]
    generator = SceneGenerator(max_duration=settings.scene_duration_max_seconds)
    options = GenerationOptions(
        logical_version=CATALOG_VERSION,
        count=1,
        strategy="distribution",
        profile="regression",
        seed=1,
        overrides={"typo_field": 1},
    )
    with pytest.raises(ValueError):
        generator.generate(ir, options)


def test_ai_generated_requires_candidates() -> None:
    ir = CATALOG_BY_ID["LS-BAS-001"]
    generator = SceneGenerator(max_duration=settings.scene_duration_max_seconds)
    options = GenerationOptions(
        logical_version=CATALOG_VERSION,
        count=1,
        strategy="ai_generated",
        profile="regression",
        seed=1,
    )
    with pytest.raises(ValueError):
        generator.generate(ir, options)


def test_content_hash_is_stable_and_sensitive_to_parameters() -> None:
    left = content_hash("LS-BAS-001", "1.0.0", {"v0": 1.23456789, "gap": 3.0})
    right = content_hash("LS-BAS-001", "1.0.0", {"v0": 1.23456789, "gap": 3.0})
    other = content_hash("LS-BAS-001", "1.0.0", {"v0": 1.2346, "gap": 3.0})
    assert left == right
    assert left != other


# =====================================================================
# 判据目录
# =====================================================================


def test_oracle_catalog_and_lookup() -> None:
    refs = {item["oracle_ref"] for item in list_oracles()}
    assert "oracle.safety.no_collision/v1" in refs
    assert "oracle.energy.soc_reserve/v1" in refs
    assert get_oracle("oracle.safety.no_collision/v1") is not None
    assert get_oracle("oracle.unknown/v1") is None
