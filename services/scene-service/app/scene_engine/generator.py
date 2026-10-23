"""具体场景生成器（§2.9 策略插件 + §2.2 分层配额 + G1-G8）。

策略：
- ``distribution``：按剖面配额在目标 Z 分区内反解采样（Z1-Z4 精确分层）；
- ``coverage_driven``：参数空间 LHS 覆盖 + 边缘分位；
- ``replay_mutation``：以回放参数为基准做高斯变异（需 overrides.base）；
- ``search_based``：过采样后按临界度 CI 取 Top-N（临界度导向搜索）；
- ``ai_generated``：仅接受外部（AI）给出的候选参数集并逐条过闸（护栏：不凭空补样本）。

去重（G8）：粗分辨率网格分桶 + 桶内标准化欧氏距离阈值（默认 0.05σ），
替代 V1.0 的余弦相似度 0.95（余弦对尺度不敏感，会把"更极端"的样本误判为重复）。

确定性：同一 (IR, 策略, 剖面, 种子, overrides) 输出完全一致（便于回归复现）。
"""
from __future__ import annotations

import random
from collections import Counter
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any

from app.models.scene_engine import (
    Avoidability,
    GenerationProfile,
    GenerationStrategy,
    RarityZone,
)
from app.scene_engine.esmini import EsminiRunner
from app.scene_engine.ir import (
    ExpressionError,
    ParameterIR,
    SceneIR,
    compile_parameters,
)
from app.scene_engine.stats import (
    copula_samples,
    distribution_moments,
    distribution_quantile,
    mahalanobis_d2,
    sample_distribution,
    zone_for_distance,
    zone_samples,
)
from app.scene_engine.validators import (
    assess_rarity,
    avoidability_label,
    content_hash,
    criticality_index,
    criticality_level,
    risk_level,
    run_gates,
)
from app.scene_engine.vehicle_params import VehicleParams, get_vehicle_params

#: 剖面 → Z1-Z4 配额（§2.9；和为 1）
PROFILE_QUOTAS: dict[str, dict[str, float]] = {
    GenerationProfile.REGRESSION.value: {"Z1": 0.70, "Z2": 0.25, "Z3": 0.05, "Z4": 0.0},
    GenerationProfile.ROBUSTNESS.value: {"Z1": 0.40, "Z2": 0.40, "Z3": 0.20, "Z4": 0.0},
    GenerationProfile.EXPLORATION.value: {"Z1": 0.10, "Z2": 0.30, "Z3": 0.50, "Z4": 0.10},
}

#: 单个参数在网格去重时的分桶数（10 = 10% 量程一格）
GRID_BINS = 10
#: 桶内标准化距离阈值（σ 单位）
DEFAULT_DEDUP_DISTANCE = 0.05
#: 允许的 overrides 键（其余一律 2001，防止静默忽略拼写错误）
ALLOWED_OVERRIDES = frozenset(
    {"fixed", "ranges", "grid_bins", "dedup_distance", "base", "candidates", "mutation_scale", "target_zone"}
)


#: 定向采样时每个配额位的过采样倍数（门禁/分区漂移导致的损耗补偿）
ZONE_OVERSAMPLE = 6


@dataclass(frozen=True, slots=True)
class GenerationOptions:
    """一次生成任务的输入。"""

    logical_version: str
    count: int
    strategy: str
    profile: str
    seed: int
    overrides: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class GenerationOutcome:
    """生成结果（行数据 + 统计 + 闸门明细）。"""

    rows: list[dict[str, Any]]
    zone_quota: dict[str, int]
    zone_counts: dict[str, int]
    gate_failures: dict[str, int]
    attempts: int
    duplicates: int
    rejected: int
    review_count: int

    @property
    def produced(self) -> int:
        return len(self.rows)


def _zone_plan(count: int, profile: str, target_zone: str | None) -> dict[str, int]:
    """把剖面比例换算为整数配额（余数补 Z1，保证总和 = count）。"""
    if target_zone is not None:
        return {target_zone: count}
    ratios = PROFILE_QUOTAS.get(profile, PROFILE_QUOTAS[GenerationProfile.REGRESSION.value])
    plan: dict[str, int] = {}
    assigned = 0
    for zone in ("Z1", "Z2", "Z3", "Z4"):
        value = round(count * ratios.get(zone, 0.0))
        plan[zone] = value
        assigned += value
    plan["Z1"] += count - assigned
    return {zone: value for zone, value in plan.items() if value > 0}


def _support(ir: SceneIR, name: str) -> tuple[float, float]:
    params = ir.parameter_map[name].distribution.params
    mean, sigma = distribution_moments(ir.parameter_map[name].distribution.kind, params)
    low = float(params.get("low", params.get("min", mean - 3.0 * max(sigma, 1e-6))))
    high = float(params.get("high", params.get("max", mean + 3.0 * max(sigma, 1e-6))))
    if not (low < high):
        high = low + 1e-6
    return low, high


def _bucket_key(ir: SceneIR, values: dict[str, float | str], bins: int) -> tuple[Any, ...]:
    key: list[Any] = []
    for parameter in ir.numeric_parameters:
        value = float(values[parameter.name])
        low, high = _support(ir, parameter.name)
        cell = int((value - low) / (high - low) * bins)
        key.append(cell)
    for parameter in ir.parameters:
        if not parameter.distribution.is_numeric:
            key.append(str(values[parameter.name]))
    return tuple(key)


def _distance(ir: SceneIR, left: dict[str, float | str], right: dict[str, float | str]) -> float:
    total = 0.0
    for parameter in ir.numeric_parameters:
        _, sigma = distribution_moments(parameter.distribution.kind, parameter.distribution.params)
        scale = sigma if sigma > 0 else 1.0
        total += ((float(left[parameter.name]) - float(right[parameter.name])) / scale) ** 2
    return total**0.5


def _observed_zone(ir: SceneIR, values: dict[str, float | str]) -> str:
    """按实际参数值重算稀有度分区（边缘分布非高斯时定向采样会漂移）。"""
    names: list[str] = []
    vector: list[float] = []
    means: list[float] = []
    stds: list[float] = []
    for parameter in ir.numeric_parameters:
        mean, sigma = distribution_moments(parameter.distribution.kind, parameter.distribution.params)
        names.append(parameter.name)
        vector.append(float(values[parameter.name]))
        means.append(mean)
        stds.append(sigma)
    if not names:
        return RarityZone.Z1.value
    d2, _ = mahalanobis_d2(vector, means, stds, ir.correlation_pairs, names)
    return zone_for_distance(d2, len(names))


class SceneGenerator:
    """无状态生成器（线程外使用；随机性全部来自 seed）。"""

    def __init__(
        self,
        *,
        max_duration: float,
        allowed_maps: frozenset[str] | None = None,
        vehicle: VehicleParams | None = None,
        bounds: tuple[float, float, float, float] | None = None,
        esmini: EsminiRunner | None = None,
        esmini_ratio: float = 0.0,
    ) -> None:
        self._max_duration = max_duration
        self._allowed_maps = allowed_maps
        self._vehicle = vehicle or get_vehicle_params()
        self._bounds = bounds
        self._esmini = esmini
        self._esmini_ratio = min(max(float(esmini_ratio), 0.0), 1.0)

    # ---------- 对外入口 ----------

    def generate(
        self,
        ir: SceneIR,
        options: GenerationOptions,
        *,
        on_progress: Callable[[int, int], None] | None = None,
    ) -> GenerationOutcome:
        """生成具体场景（G1-G8 全流程）。"""
        self._validate_options(options)
        overrides = options.overrides
        rng = random.Random(options.seed)
        target_zone = overrides.get("target_zone")
        plan = _zone_plan(options.count, options.profile, target_zone)

        produced_rows: list[dict[str, Any]] = []
        zone_counts: Counter[str] = Counter()
        gate_failures: Counter[str] = Counter()
        buckets: dict[tuple[Any, ...], list[dict[str, float | str]]] = {}
        seen_hashes: set[str] = set()
        attempts = duplicates = rejected = review_count = 0
        grid_bins = int(overrides.get("grid_bins", GRID_BINS))
        dedup_distance = float(overrides.get("dedup_distance", DEFAULT_DEDUP_DISTANCE))
        progress_step = max(1, options.count // 20)

        for raw_values in self._candidates(ir, options, plan, rng):
            if len(produced_rows) >= options.count:
                break
            attempts += 1
            values = self._complete_values(ir, raw_values, overrides, rng)
            if values is None:
                rejected += 1
                gate_failures["G2"] += 1
                continue

            try:
                config = compile_parameters(ir, values)
            except (ExpressionError, ValueError):
                rejected += 1
                gate_failures["G1"] += 1
                continue

            report = run_gates(
                ir,
                values,
                config,
                max_duration=self._max_duration,
                allowed_maps=self._allowed_maps,
                vehicle=self._vehicle,
                bounds=self._bounds,
                # 生成阶段按比例抽样干跑；validate/dryrun/复检全量
                esmini=(
                    self._esmini
                    if self._esmini is not None and rng.random() < self._esmini_ratio
                    else None
                ),
            )
            if not report.passed:
                rejected += 1
                gate_failures[report.gates[-1].gate if report.gates else "G1"] += 1
                continue

            p_value, zone, _ = assess_rarity(ir, values)
            ci = criticality_index(ir, values, self._vehicle)
            level = criticality_level(ci)
            risk = risk_level(zone, level)
            label = avoidability_label(ci)
            if label == Avoidability.UNAVOIDABLE.value:
                review_count += 1

            digest = content_hash(ir.logical_id, options.logical_version, values)
            if digest in seen_hashes:
                duplicates += 1
                continue
            bucket = _bucket_key(ir, values, grid_bins)
            near = any(
                _distance(ir, values, other) < dedup_distance for other in buckets.get(bucket, [])
            )
            if near:
                duplicates += 1
                if on_progress is not None:
                    on_progress(len(produced_rows), options.count)
                continue

            seen_hashes.add(digest)
            buckets.setdefault(bucket, []).append(values)
            zone_counts[zone] += 1
            produced_rows.append(
                self._row(ir, options, values, config, p_value, zone, ci, level, risk, label, report)
            )
            if on_progress is not None and len(produced_rows) % progress_step == 0:
                on_progress(len(produced_rows), options.count)

        if on_progress is not None:
            on_progress(len(produced_rows), options.count)

        return GenerationOutcome(
            rows=produced_rows,
            zone_quota=plan,
            zone_counts=dict(zone_counts),
            gate_failures=dict(gate_failures),
            attempts=attempts,
            duplicates=duplicates,
            rejected=rejected,
            review_count=review_count,
        )

    # ---------- 候选生成 ----------

    def _validate_options(self, options: GenerationOptions) -> None:
        unknown = sorted(set(options.overrides) - ALLOWED_OVERRIDES)
        if unknown:
            raise ValueError(f"overrides 含未支持字段: {unknown}")
        if options.strategy not in {item.value for item in GenerationStrategy}:
            raise ValueError(f"未知生成策略: {options.strategy}")
        if options.profile not in {item.value for item in GenerationProfile}:
            raise ValueError(f"未知生成剖面: {options.profile}")
        if not 1 <= options.count <= 5000:
            raise ValueError("count 必须在 1..5000 之间")
        if options.strategy == GenerationStrategy.REPLAY_MUTATION.value and not options.overrides.get("base"):
            raise ValueError("replay_mutation 必须提供 overrides.base 参数基准")
        if options.strategy == GenerationStrategy.AI_GENERATED.value and not options.overrides.get("candidates"):
            raise ValueError("ai_generated 必须提供 overrides.candidates 候选参数集")

    def _candidates(
        self, ir: SceneIR, options: GenerationOptions, plan: dict[str, int], rng: random.Random
    ) -> Iterator[dict[str, float | str]]:
        strategy = options.strategy
        if strategy == GenerationStrategy.AI_GENERATED.value:
            for candidate in options.overrides.get("candidates") or []:
                yield {str(key): value for key, value in candidate.items()}
            return
        if strategy == GenerationStrategy.REPLAY_MUTATION.value:
            yield from self._mutation_candidates(ir, options, rng)
            return
        if strategy == GenerationStrategy.COVERAGE_DRIVEN.value:
            yield from self._coverage_candidates(ir, options, rng)
            return
        if strategy == GenerationStrategy.SEARCH_BASED.value:
            yield from self._search_candidates(ir, options, rng)
            return
        # 默认：distribution（按 Z 配额反解采样）
        yield from self._distribution_candidates(ir, options, plan, rng)

    def _distribution_candidates(
        self, ir: SceneIR, options: GenerationOptions, plan: dict[str, int], rng: random.Random
    ) -> Iterator[dict[str, float | str]]:
        names, distributions, pairs = self._numeric_space(ir, options)
        if not names:
            for _ in range(options.count):
                yield {}
            return
        # 定向采样按目标 Z 分区反解，但边缘分布非高斯/门禁拒绝会造成损耗：
        # 先过采样并校验实际分区，再用自然 Copula 采样补齐（生成器在达到 count 时停止）。
        for zone, count in plan.items():
            matched = 0
            for row in zone_samples(names, distributions, pairs, zone, count * ZONE_OVERSAMPLE, rng):
                values = dict(zip(names, row))
                if _observed_zone(ir, values) != zone:
                    continue
                matched += 1
                yield values
                if matched >= count:
                    break
        for row in copula_samples(names, distributions, pairs, options.count, rng):
            yield dict(zip(names, row))

    def _coverage_candidates(
        self, ir: SceneIR, options: GenerationOptions, rng: random.Random
    ) -> Iterator[dict[str, float | str]]:
        from app.scene_engine.stats import latin_hypercube

        names, distributions, _ = self._numeric_space(ir, options)
        if not names:
            for _ in range(options.count):
                yield {}
            return
        units = latin_hypercube(options.count * 3, len(names), rng)
        for unit in units:
            values: dict[str, float | str] = {}
            for name, (kind, params), level in zip(names, distributions, unit):
                quantile = distribution_quantile(kind, params, level)
                values[name] = quantile if quantile is not None else 0.0
            yield values

    def _mutation_candidates(
        self, ir: SceneIR, options: GenerationOptions, rng: random.Random
    ) -> Iterator[dict[str, float | str]]:
        base = {str(key): value for key, value in (options.overrides.get("base") or {}).items()}
        scale = float(options.overrides.get("mutation_scale", 0.1))
        for _ in range(options.count * 5):
            values: dict[str, float | str] = {}
            for parameter in ir.parameters:
                anchor = base.get(parameter.name)
                if parameter.distribution.kind == "categorical":
                    values[parameter.name] = str(anchor) if anchor is not None else str(
                        sample_distribution(parameter.distribution.kind, parameter.distribution.params, rng)
                    )
                    continue
                if not isinstance(anchor, (int, float)):
                    anchor = distribution_quantile(
                        parameter.distribution.kind, parameter.distribution.params, 0.5
                    ) or 0.0
                low, high = _support(ir, parameter.name)
                jitter = rng.gauss(0.0, scale * (high - low))
                values[parameter.name] = min(max(float(anchor) + jitter, low), high)
            yield values

    def _search_candidates(
        self, ir: SceneIR, options: GenerationOptions, rng: random.Random
    ) -> Iterator[dict[str, float | str]]:
        names, distributions, pairs = self._numeric_space(ir, options)
        candidates: list[dict[str, float | str]] = []
        for row in copula_samples(names, distributions, pairs, options.count * 3, rng):
            candidates.append(dict(zip(names, row)))
        for zone in ("Z3", "Z4"):
            for row in zone_samples(names, distributions, pairs, zone, options.count, rng):
                candidates.append(dict(zip(names, row)))

        def score(values: dict[str, float | str]) -> float:
            return criticality_index(ir, values, self._vehicle) + 0.001 * assess_rarity(ir, values)[0]

        candidates.sort(key=score, reverse=True)
        yield from candidates

    # ---------- 参数空间 ----------

    def _numeric_space(
        self, ir: SceneIR, options: GenerationOptions
    ) -> tuple[list[str], list[tuple[str, dict[str, Any]]], list[tuple[str, str, float]]]:
        """构造（含 overrides.fixed/ranges 修正的）数值参数空间。"""
        fixed = options.overrides.get("fixed") or {}
        ranges = options.overrides.get("ranges") or {}
        names: list[str] = []
        distributions: list[tuple[str, dict[str, Any]]] = []
        for parameter in ir.numeric_parameters:
            if parameter.name in fixed:
                continue
            kind = parameter.distribution.kind
            params = dict(parameter.distribution.params)
            override = ranges.get(parameter.name) or {}
            if "low" in override or "high" in override:
                low = float(override.get("low", params.get("low", params.get("min", 0.0))))
                high = float(override.get("high", params.get("high", params.get("max", low + 1.0))))
                params["low"], params["high"] = low, high
                for key in ("mean", "mu"):
                    if key in params:
                        params[key] = min(max(float(params[key]), low), high)
                if "std" in params:
                    params["std"] = min(float(params["std"]), max((high - low) / 6.0, 1e-6))
                if "sigma" in params:
                    params["sigma"] = min(float(params["sigma"]), max((high - low) / 6.0, 1e-6))
            names.append(parameter.name)
            distributions.append((kind, params))
        pairs = [(left, right, rho) for left, right, rho in ir.correlation_pairs if left in names and right in names]
        return names, distributions, pairs

    def _complete_values(
        self,
        ir: SceneIR,
        raw: dict[str, float | str],
        overrides: dict[str, Any],
        rng: random.Random,
    ) -> dict[str, float | str] | None:
        """补齐所有参数（未采样到的用中位数/分类默认值），并应用 fixed。"""
        fixed = overrides.get("fixed") or {}
        values: dict[str, float | str] = {}
        for parameter in ir.parameters:
            if parameter.name in fixed:
                values[parameter.name] = fixed[parameter.name]
                continue
            value = raw.get(parameter.name)
            if value is None:
                if parameter.distribution.kind == "categorical":
                    value = sample_distribution(parameter.distribution.kind, parameter.distribution.params, rng)
                else:
                    value = distribution_quantile(parameter.distribution.kind, parameter.distribution.params, 0.5)
            if value is None:
                return None
            values[parameter.name] = value
        return values

    # ---------- 行数据 ----------

    def _row(
        self,
        ir: SceneIR,
        options: GenerationOptions,
        values: dict[str, float | str],
        config: dict[str, Any],
        p_value: float,
        zone: str,
        ci: float,
        level: str,
        risk: str,
        label: str,
        report: Any,
    ) -> dict[str, Any]:
        numeric_vector = [float(values[parameter.name]) for parameter in ir.numeric_parameters]
        digest = content_hash(ir.logical_id, options.logical_version, values)
        # 确定性命名（禁止用内置 hash()，其受 PYTHONHASHSEED 影响，跨进程不稳定）
        index = int(digest[:8], 16) % 10000
        allowed_modes = list(ir.allowed_modes)
        # VIL 可用判据：模式白名单含 vil + 低风险（L1/L2）+ 高频分区（Z1/Z2）
        vil_safe = "vil" in allowed_modes and risk in {"L1", "L2"} and zone in {
            RarityZone.Z1.value,
            RarityZone.Z2.value,
        }
        # CI 饱和：净距 <= d_margin 时 a_req 为无穷，DB 列为有限浮点→以 999.0 落库，
        # 并用 generator.ci_saturated 区分“饱和”与“实测高临界”（下游不得将 999.0 当连续量比较）
        ci_saturated = ci == float("inf")
        return {
            "logical_id": ir.logical_id,
            "logical_version": options.logical_version,
            "name": f"{ir.logical_id}-{zone}-{index:04d}",
            "param_values": values,
            "param_vec": numeric_vector,
            "rarity_p": round(p_value, 12),
            "rarity_zone": zone,
            "criticality_ci": 999.0 if ci_saturated else round(ci, 6),
            "criticality_level": level,
            "risk_level": risk,
            "avoidability": label,
            "validity": report.to_dict(),
            "generator": {
                "strategy": options.strategy,
                "profile": options.profile,
                "seed": options.seed,
                "catalog": "builtin",
                "vehicle_profile": self._vehicle.profile,
                "vehicle_params_hash": self._vehicle.source_hash,
                "ci_saturated": ci_saturated,
            },
            "allowed_modes": allowed_modes,
            "vil_safe": vil_safe,
            "seed": options.seed,
            "content_hash": digest,
            "scene_config": config,
            "status": "generated",
        }


def _mapping_override(overrides: Mapping[str, Any], key: str) -> dict[str, Any]:
    value = overrides.get(key)
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(f"overrides.{key} 必须是对象")  # noqa: TRY004
    return {str(name): item for name, item in value.items()}


def _hard_bounds(parameter: ParameterIR) -> tuple[float, float] | None:
    """分布声明的硬边界（low/high 或 min/max）；未声明则不做区间断言。"""
    params = parameter.distribution.params
    low, high = params.get("low", params.get("min")), params.get("high", params.get("max"))
    if low is None or high is None:
        return None
    return float(low), float(high)


def _ensure_parameter_value(label: str, parameter: ParameterIR, value: Any) -> None:
    """参数取值合法性（数值落硬界内，分类值在 choices 内）。"""
    if parameter.distribution.is_numeric:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{label} 必须是数值")
        bounds = _hard_bounds(parameter)
        if bounds is not None and not bounds[0] <= float(value) <= bounds[1]:
            raise ValueError(f"{label} 越界：必须在 {bounds[0]}..{bounds[1]} 之间")
        return
    choices = {
        str(item.get("value")) for item in parameter.distribution.params.get("choices") or []
    }
    if str(value) not in choices:
        raise ValueError(f"{label} 取值非法，可选：{sorted(choices)}")


def ensure_overrides_shape(overrides: Mapping[str, Any], ir: SceneIR | None = None) -> None:
    """overrides 早失败校验（2001）：给定 IR 时同时做语义与取值域校验。

    只查结构时，参数名拼错、fixed 值越界要等到异步任务跑完才表现为“0 条产物”，
    排查成本高；传入 IR 后在创建任务时即可拒绝（包含 base/candidates 的未知键，
    早期会被采样环节静默忽略）。
    """
    unknown = sorted(set(overrides) - ALLOWED_OVERRIDES)
    if unknown:
        raise ValueError(f"overrides 含未支持字段: {unknown}")
    target_zone = overrides.get("target_zone")
    if target_zone is not None and target_zone not in {zone.value for zone in RarityZone}:
        raise ValueError("overrides.target_zone 必须是 Z1/Z2/Z3/Z4")
    grid_bins = overrides.get("grid_bins")
    if grid_bins is not None and (
        isinstance(grid_bins, bool) or not isinstance(grid_bins, int) or not 1 <= grid_bins <= 64
    ):
        raise ValueError("overrides.grid_bins 必须是 1..64 的整数")
    dedup_distance = overrides.get("dedup_distance")
    if dedup_distance is not None and (
        isinstance(dedup_distance, bool)
        or not isinstance(dedup_distance, (int, float))
        or not 0.0 <= float(dedup_distance) <= 10.0
    ):
        raise ValueError("overrides.dedup_distance 必须在 0..10 之间")
    mutation_scale = overrides.get("mutation_scale")
    if mutation_scale is not None and (
        isinstance(mutation_scale, bool)
        or not isinstance(mutation_scale, (int, float))
        or not 0.0 < float(mutation_scale) <= 1.0
    ):
        raise ValueError("overrides.mutation_scale 必须在 (0, 1] 之间")

    fixed = _mapping_override(overrides, "fixed")
    ranges = _mapping_override(overrides, "ranges")
    base = _mapping_override(overrides, "base")
    candidates = overrides.get("candidates")
    if candidates is not None:
        if not isinstance(candidates, list) or not candidates:
            raise ValueError("overrides.candidates 必须是非空数组")
        for index, candidate in enumerate(candidates):
            if not isinstance(candidate, Mapping):
                raise ValueError(f"overrides.candidates[{index}] 必须是对象")  # noqa: TRY004

    if ir is None:
        return
    declared = ir.parameter_map
    for key, section in (("fixed", fixed), ("base", base)):
        for name, value in section.items():
            parameter = declared.get(name)
            if parameter is None:
                raise ValueError(f"overrides.{key} 含未声明参数: {name}")
            _ensure_parameter_value(f"overrides.{key}.{name}", parameter, value)
    for name, bounds in ranges.items():
        parameter = declared.get(name)
        if parameter is None:
            raise ValueError(f"overrides.ranges 含未声明参数: {name}")
        if not parameter.distribution.is_numeric:
            raise ValueError(f"overrides.ranges.{name} 不适用于分类参数")
        if not bounds:
            raise ValueError(f"overrides.ranges.{name} 必须含 low/high")
        hard = _hard_bounds(parameter)
        low, high = bounds.get("low"), bounds.get("high")
        for label, value in (("low", low), ("high", high)):
            if value is None:
                continue
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"overrides.ranges.{name}.{label} 必须是数值")  # noqa: TRY004
            if hard is not None and not hard[0] <= float(value) <= hard[1]:
                raise ValueError(
                    f"overrides.ranges.{name}.{label} 越界：必须在 {hard[0]}..{hard[1]} 之间"
                )
        if low is not None and high is not None and float(low) >= float(high):
            raise ValueError(f"overrides.ranges.{name} 要求 low < high")
    for index, candidate in enumerate(candidates or []):
        unknown_keys = sorted(set(candidate) - set(declared))
        if unknown_keys:
            raise ValueError(f"overrides.candidates[{index}] 含未声明参数: {unknown_keys}")
        for name, value in candidate.items():
            _ensure_parameter_value(f"overrides.candidates[{index}].{name}", declared[name], value)
    # fixed 同时越出线性约束时必然产出 0 条，提前拒绝（仅能全量代入时判定）
    if fixed:
        for constraint in ir.constraints:
            if not set(constraint.terms) <= set(fixed):
                continue
            involved = [fixed[name] for name in constraint.terms]
            if any(
                isinstance(value, bool) or not isinstance(value, (int, float)) for value in involved
            ):
                continue
            total = sum(
                float(coefficient) * float(fixed[name])
                for name, coefficient in constraint.terms.items()
            )
            violated = {
                "<=": total > constraint.value,
                "<": total >= constraint.value,
                ">=": total < constraint.value,
                ">": total <= constraint.value,
            }[constraint.op]
            if violated:
                hint = constraint.message or f"{constraint.op} {constraint.value}"
                raise ValueError(f"overrides.fixed 违反约束 {constraint.name}（{hint}）")


def summarize_failures(failures: Iterable[str]) -> str:
    """闸门失败原因摘要（写任务 error_message / 日志）。"""
    counter = Counter(failures)
    return ", ".join(f"{gate}×{count}" for gate, count in counter.items()) or "无"


__all__ = [
    "ALLOWED_OVERRIDES",
    "DEFAULT_DEDUP_DISTANCE",
    "PROFILE_QUOTAS",
    "GenerationOptions",
    "GenerationOutcome",
    "SceneGenerator",
    "ensure_overrides_shape",
    "summarize_failures",
]
