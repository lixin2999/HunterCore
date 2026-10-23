"""场景引擎统计内核（纯 Python，无 numpy/scipy 依赖）。

覆盖设计文档 §2.2 的数学要素：
- 边缘分布采样与分位：截断正态 / 对数正态 / Beta / 均匀 / 分类 / 高斯混合 / 常量；
- 高斯 Copula：相关矩阵 Cholesky 分解 + 边缘分位变换（``u = Φ(z)``）；
- 马氏距离 ``d_M² = zᵀ R⁻¹ z``（z 为按边缘均值/标准差标准化后的偏差），
  ``d_M² ~ χ²ₙ`` ⇒ 稀有度 p 值 ``p_rar = 1 - F_χ²(d_M²)``；
- Z1-Z4 分区：p ≥ 0.5 / 0.1 ≤ p < 0.5 / 0.01 ≤ p < 0.1 / p < 0.01
  （等价于 χ² 的 50% / 90% / 99% 分位阈值）。

精度取舍：χ² CDF 用正则化不完全 Gamma（级数 + 连分式），Beta 分位用二分；
单次生成（≤ 5000 样本）耗时在毫秒级，不需要 SciPy。
"""
from __future__ import annotations

import math
import random
from statistics import NormalDist
from typing import Any

_NORMAL = NormalDist()
_TWO_PI = 2.0 * math.pi


# =====================================================================
# 一、正态 / Gamma / Beta 基础函数
# =====================================================================


def normal_cdf(x: float) -> float:
    return _NORMAL.cdf(x)


def normal_ppf(p: float) -> float:
    """标准正态分位（p 截断到 (1e-12, 1-1e-12)，避免 ±inf）。"""
    return _NORMAL.inv_cdf(min(max(p, 1e-12), 1.0 - 1e-12))


def normal_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(_TWO_PI)


def _gamma_series(a: float, x: float) -> float:
    """正则化下不完全 Gamma P(a, x) 的级数展开（x < a + 1 时收敛快）。"""
    term = 1.0 / a
    total = term
    ap = a
    for _ in range(500):
        ap += 1.0
        term *= x / ap
        total += term
        if abs(term) < abs(total) * 1e-14:
            break
    return total * math.exp(-x + a * math.log(x) - math.lgamma(a))


def _gamma_continued_fraction(a: float, x: float) -> float:
    """正则化上不完全 Gamma Q(a, x) 的 Lentz 连分式（x ≥ a + 1 时稳定）。"""
    tiny = 1e-300
    b = x + 1.0 - a
    c = 1.0 / tiny
    d = 1.0 / b
    h = d
    for i in range(1, 500):
        an = -i * (i - a)
        b += 2.0
        d = an * d + b
        if abs(d) < tiny:
            d = tiny
        c = b + an / c
        if abs(c) < tiny:
            c = tiny
        d = 1.0 / d
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < 1e-14:
            break
    return math.exp(-x + a * math.log(x) - math.lgamma(a)) * h


def gamma_cdf(a: float, x: float) -> float:
    """正则化下不完全 Gamma P(a, x)。"""
    if x <= 0:
        return 0.0
    if x < a + 1.0:
        return _gamma_series(a, x)
    return 1.0 - _gamma_continued_fraction(a, x)


def chi2_cdf(x: float, df: int) -> float:
    """χ²(df) 累积分布。"""
    if df <= 0:
        raise ValueError("df 必须 ≥ 1")
    return gamma_cdf(df / 2.0, x / 2.0)


def chi2_sf(x: float, df: int) -> float:
    """χ²(df) 生存函数 ``1 - F(x)``（= 稀有度 p 值）。"""
    return min(max(1.0 - chi2_cdf(x, df), 0.0), 1.0)


def chi2_ppf(p: float, df: int) -> float:
    """χ² 分位（二分法，单调且无初值敏感）。"""
    target = min(max(p, 1e-12), 1.0 - 1e-12)
    low, high = 0.0, max(1.0, df * 10.0)
    while chi2_cdf(high, df) < target:
        high *= 2.0
        if high > 1e9:
            break
    for _ in range(200):
        mid = (low + high) / 2.0
        if chi2_cdf(mid, df) < target:
            low = mid
        else:
            high = mid
    return (low + high) / 2.0


def beta_cdf(x: float, alpha: float, beta: float) -> float:
    """正则化不完全 Beta I_x(α, β)（连分式）。"""
    if x <= 0:
        return 0.0
    if x >= 1:
        return 1.0
    log_front = (
        math.lgamma(alpha + beta)
        - math.lgamma(alpha)
        - math.lgamma(beta)
        + alpha * math.log(x)
        + beta * math.log(1.0 - x)
    )
    front = math.exp(log_front)
    if x < (alpha + 1.0) / (alpha + beta + 2.0):
        return front * _beta_continued_fraction(alpha, beta, x) / alpha
    return 1.0 - front * _beta_continued_fraction(beta, alpha, 1.0 - x) / beta


def _beta_continued_fraction(a: float, b: float, x: float) -> float:
    tiny = 1e-300
    c = 1.0
    d = 1.0 - (a + b) * x / (a + 1.0)
    if abs(d) < tiny:
        d = tiny
    d = 1.0 / d
    h = d
    for m in range(1, 300):
        m2 = 2 * m
        aa = m * (b - m) * x / ((a + m2 - 1.0) * (a + m2))
        d = 1.0 + aa * d
        if abs(d) < tiny:
            d = tiny
        c = 1.0 + aa / c
        if abs(c) < tiny:
            c = tiny
        d = 1.0 / d
        h *= d * c
        aa = -(a + m) * (a + b + m) * x / ((a + m2) * (a + m2 + 1.0))
        d = 1.0 + aa * d
        if abs(d) < tiny:
            d = tiny
        c = 1.0 + aa / c
        if abs(c) < tiny:
            c = tiny
        d = 1.0 / d
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < 1e-14:
            break
    return h


def beta_ppf(p: float, alpha: float, beta: float) -> float:
    """Beta 分位（二分）。"""
    target = min(max(p, 1e-12), 1.0 - 1e-12)
    low, high = 0.0, 1.0
    for _ in range(120):
        mid = (low + high) / 2.0
        if beta_cdf(mid, alpha, beta) < target:
            low = mid
        else:
            high = mid
    return (low + high) / 2.0


def _sample_gamma(shape: float, rng: random.Random) -> float:
    """Marsaglia-Tsang Gamma 采样。"""
    if shape < 1.0:
        return _sample_gamma(shape + 1.0, rng) * rng.random() ** (1.0 / shape)
    d = shape - 1.0 / 3.0
    c = 1.0 / math.sqrt(9.0 * d)
    while True:
        x = rng.gauss(0.0, 1.0)
        v = (1.0 + c * x) ** 3
        if v <= 0:
            continue
        u = rng.random()
        if u < 1.0 - 0.0331 * x**4:
            return d * v
        if math.log(u) < 0.5 * x * x + d * (1.0 - v + math.log(v)):
            return d * v


def _sample_beta(alpha: float, beta: float, rng: random.Random) -> float:
    left = _sample_gamma(alpha, rng)
    right = _sample_gamma(beta, rng)
    return left / (left + right)


# =====================================================================
# 二、边缘分布
# =====================================================================


def _params(params: dict[str, Any]) -> tuple[float, float, float, float]:
    """提取 (mean/loc, sigma/scale, low, high) 通用参数。"""
    mean = float(params.get("mean", params.get("mu", params.get("loc", 0.0))))
    sigma = float(params.get("std", params.get("sigma", params.get("scale", 0.0))))
    low = float(params.get("low", params.get("min", -math.inf)))
    high = float(params.get("high", params.get("max", math.inf)))
    return mean, sigma, low, high


def _truncated_normal_sample(
    mean: float, sigma: float, low: float, high: float, rng: random.Random
) -> float:
    if sigma <= 0:
        return min(max(mean, low), high)
    for _ in range(200):
        value = rng.gauss(mean, sigma)
        if low <= value <= high:
            return value
    return min(max(mean, low), high)


def _truncated_normal_quantile(
    mean: float, sigma: float, low: float, high: float, u: float
) -> float:
    if sigma <= 0:
        return min(max(mean, low), high)
    alpha = (low - mean) / sigma if math.isfinite(low) else 0.0
    beta = (high - mean) / sigma if math.isfinite(high) else 1.0
    cdf_low = normal_cdf(alpha) if math.isfinite(low) else 0.0
    cdf_high = normal_cdf(beta) if math.isfinite(high) else 1.0
    span = max(cdf_high - cdf_low, 1e-12)
    return mean + sigma * normal_ppf(cdf_low + u * span)


def sample_distribution(kind: str, params: dict[str, Any], rng: random.Random) -> float | str:
    """按分布类型采样（返回 float；categorical 返回字符串）。"""
    if kind == "constant":
        return float(params.get("value", 0.0))
    if kind == "uniform":
        low, high = float(params.get("low", 0.0)), float(params.get("high", 1.0))
        return low + (high - low) * rng.random()
    if kind == "categorical":
        choices = params.get("choices") or []
        if not choices:
            raise ValueError("categorical 分布缺少 choices")
        weights = [float(item.get("weight", 1.0)) for item in choices]
        values = [str(item.get("value")) for item in choices]
        return rng.choices(values, weights=weights, k=1)[0]
    if kind == "lognormal":
        mean, sigma, low, high = _params(params)
        if sigma <= 0:
            return min(max(math.exp(mean), low), high)
        for _ in range(200):
            value = math.exp(rng.gauss(mean, sigma))
            if low <= value <= high:
                return value
        return min(max(math.exp(mean), low), high)
    if kind == "beta":
        alpha = float(params.get("alpha", 2.0))
        beta = float(params.get("beta", 2.0))
        low, high = float(params.get("low", 0.0)), float(params.get("high", 1.0))
        return low + (high - low) * _sample_beta(alpha, beta, rng)
    if kind == "gmm":
        components = params.get("components") or []
        if not components:
            raise ValueError("gmm 分布缺少 components")
        weights = [float(item.get("weight", 1.0)) for item in components]
        component = rng.choices(components, weights=weights, k=1)[0]
        mean, sigma, low, high = _params(component)
        return _truncated_normal_sample(mean, sigma, low, high, rng)
    # 默认：截断正态
    mean, sigma, low, high = _params(params)
    return _truncated_normal_sample(mean, sigma, low, high, rng)


def distribution_quantile(kind: str, params: dict[str, Any], u: float) -> float | None:
    """边缘分位（Copula 用）；不支持的分布返回 None（调用方退化为独立采样）。"""
    u = min(max(u, 1e-9), 1.0 - 1e-9)
    if kind == "constant":
        return float(params.get("value", 0.0))
    if kind == "uniform":
        low, high = float(params.get("low", 0.0)), float(params.get("high", 1.0))
        return low + (high - low) * u
    if kind in {"truncated_normal", "normal"}:
        mean, sigma, low, high = _params(params)
        return _truncated_normal_quantile(mean, sigma, low, high, u)
    if kind == "lognormal":
        mean, sigma, low, high = _params(params)
        value = math.exp(mean + sigma * normal_ppf(u)) if sigma > 0 else math.exp(mean)
        return min(max(value, low), high)
    if kind == "beta":
        alpha = float(params.get("alpha", 2.0))
        beta = float(params.get("beta", 2.0))
        low, high = float(params.get("low", 0.0)), float(params.get("high", 1.0))
        return low + (high - low) * beta_ppf(u, alpha, beta)
    if kind == "gmm":
        components = params.get("components") or []
        if not components:
            return None
        total = sum(float(item.get("weight", 1.0)) for item in components)
        acc = 0.0
        for component in components:
            weight = float(component.get("weight", 1.0)) / total
            if u <= acc + weight:
                local = (u - acc) / max(weight, 1e-12)
                mean, sigma, low, high = _params(component)
                return _truncated_normal_quantile(mean, sigma, low, high, local)
            acc += weight
        mean, sigma, low, high = _params(components[-1])
        return _truncated_normal_quantile(mean, sigma, low, high, u)
    return None


def distribution_moments(kind: str, params: dict[str, Any]) -> tuple[float, float]:
    """边缘均值/标准差（马氏距离标准化口径；对截断分布用解析近似）。"""
    if kind == "constant":
        return float(params.get("value", 0.0)), 0.0
    if kind == "uniform":
        low, high = float(params.get("low", 0.0)), float(params.get("high", 1.0))
        return (low + high) / 2.0, math.sqrt((high - low) ** 2 / 12.0)
    if kind == "beta":
        alpha = float(params.get("alpha", 2.0))
        beta = float(params.get("beta", 2.0))
        low, high = float(params.get("low", 0.0)), float(params.get("high", 1.0))
        mean = low + (high - low) * alpha / (alpha + beta)
        var = (
            (high - low) ** 2
            * alpha
            * beta
            / ((alpha + beta) ** 2 * (alpha + beta + 1.0))
        )
        return mean, math.sqrt(var)
    if kind == "lognormal":
        mu, sigma, low, high = _params(params)
        mean = math.exp(mu + sigma**2 / 2.0)
        var = (math.exp(sigma**2) - 1.0) * math.exp(2.0 * mu + sigma**2)
        return min(max(mean, low), high), math.sqrt(max(var, 0.0))
    if kind == "gmm":
        components = params.get("components") or []
        if not components:
            return 0.0, 0.0
        total = sum(float(item.get("weight", 1.0)) for item in components)
        mean = 0.0
        second = 0.0
        for component in components:
            weight = float(component.get("weight", 1.0)) / total
            comp_mean, comp_sigma, _, _ = _params(component)
            mean += weight * comp_mean
            second += weight * (comp_sigma**2 + comp_mean**2)
        return mean, math.sqrt(max(second - mean**2, 0.0))
    mean, sigma, low, high = _params(params)
    if sigma <= 0:
        return min(max(mean, low), high), 0.0
    # 截断正态：解析均值/方差（未截断时退化为 σ）
    if not math.isfinite(low) and not math.isfinite(high):
        return mean, sigma
    alpha = (low - mean) / sigma if math.isfinite(low) else 0.0
    beta = (high - mean) / sigma if math.isfinite(high) else 1.0
    cdf_low = normal_cdf(alpha) if math.isfinite(low) else 0.0
    cdf_high = normal_cdf(beta) if math.isfinite(high) else 1.0
    span = max(cdf_high - cdf_low, 1e-12)
    phi_low = normal_pdf(alpha) if math.isfinite(low) else 0.0
    phi_high = normal_pdf(beta) if math.isfinite(high) else 0.0
    mean_t = mean + sigma * (phi_low - phi_high) / span
    var_t = sigma**2 * (
        1.0 + (alpha * phi_low - beta * phi_high) / span - ((phi_low - phi_high) / span) ** 2
    )
    return mean_t, math.sqrt(max(var_t, 0.0))


# =====================================================================
# 三、Copula / 马氏距离 / 分区
# =====================================================================


def cholesky(matrix: list[list[float]]) -> list[list[float]]:
    """下三角 Cholesky 分解（非正定时对角加抖动，保证可分解）。"""
    size = len(matrix)
    lower = [[0.0] * size for _ in range(size)]
    for i in range(size):
        for j in range(i + 1):
            acc = matrix[i][j] - sum(lower[i][k] * lower[j][k] for k in range(j))
            if i == j:
                lower[i][j] = math.sqrt(max(acc, 1e-12))
            else:
                lower[i][j] = acc / lower[j][j]
    return lower


def correlation_matrix(names: list[str], pairs: list[tuple[str, str, float]]) -> list[list[float]]:
    """按参数名构造相关矩阵（对角线 1；未知参数/越界相关系数忽略或截断）。"""
    index = {name: idx for idx, name in enumerate(names)}
    size = len(names)
    matrix = [[1.0 if i == j else 0.0 for j in range(size)] for i in range(size)]
    for left, right, rho in pairs:
        if left not in index or right not in index or left == right:
            continue
        value = min(max(float(rho), -0.99), 0.99)
        i, j = index[left], index[right]
        matrix[i][j] = value
        matrix[j][i] = value
    return matrix


def copula_samples(
    names: list[str],
    distributions: list[tuple[str, dict[str, Any]]],
    pairs: list[tuple[str, str, float]],
    count: int,
    rng: random.Random,
) -> list[list[float]]:
    """高斯 Copula 采样（返回 count × len(names) 的数值矩阵）。

    若任一维不支持分位（如 categorical），该维退化为独立采样，
    其余维仍按相关矩阵联动（文档化的降级行为，不静默丢弃相关性）。
    """
    size = len(names)
    lower = cholesky(correlation_matrix(names, pairs))
    independent = [
        distribution_quantile(kind, params, 0.5) is None for kind, params in distributions
    ]
    rows: list[list[float]] = []
    for _ in range(count):
        eps = [rng.gauss(0.0, 1.0) for _ in range(size)]
        z = [sum(lower[i][k] * eps[k] for k in range(i + 1)) for i in range(size)]
        row: list[float] = []
        for idx, ((kind, params), independent_dim) in enumerate(zip(distributions, independent)):
            if independent_dim:
                value = sample_distribution(kind, params, rng)
                row.append(float(value) if isinstance(value, (int, float)) else 0.0)
            else:
                quantile = distribution_quantile(kind, params, normal_cdf(z[idx]))
                row.append(float(quantile if quantile is not None else 0.0))
        rows.append(row)
    return rows


def _solve_lower(lower: list[list[float]], vector: list[float]) -> list[float]:
    size = len(vector)
    result = [0.0] * size
    for i in range(size):
        acc = vector[i] - sum(lower[i][k] * result[k] for k in range(i))
        result[i] = acc / lower[i][i]
    return result


def mahalanobis_d2(
    values: list[float],
    means: list[float],
    stds: list[float],
    pairs: list[tuple[str, str, float]],
    names: list[str],
) -> tuple[float, list[float]]:
    """返回 (d_M², 标准化偏差 z)。std=0 的维度不计入（z=0）。"""
    z = [
        (value - mean) / std if std > 0 else 0.0
        for value, mean, std in zip(values, means, stds)
    ]
    lower = cholesky(correlation_matrix(names, pairs))
    solved = _solve_lower(lower, z)
    return sum(item * item for item in solved), z


def rarity_zone(p_value: float) -> str:
    """稀有度分区：Z1 常见 / Z2 较少见 / Z3 稀有 / Z4 极稀有（§2.2.3）。"""
    if p_value >= 0.5:
        return "Z1"
    if p_value >= 0.1:
        return "Z2"
    if p_value >= 0.01:
        return "Z3"
    return "Z4"


def latin_hypercube(count: int, dimensions: int, rng: random.Random) -> list[list[float]]:
    """LHS：每维分层打散（返回单位超立方体样本）。"""
    if count <= 0 or dimensions <= 0:
        return []
    columns: list[list[float]] = []
    for _ in range(dimensions):
        strata = [(i + rng.random()) / count for i in range(count)]
        rng.shuffle(strata)
        columns.append(strata)
    return [[columns[dim][row] for dim in range(dimensions)] for row in range(count)]


def unit_vector(size: int, rng: random.Random) -> list[float]:
    """n 维单位球面随机方向（高斯归一）。"""
    if size <= 0:
        return []
    vector = [rng.gauss(0.0, 1.0) for _ in range(size)]
    norm = math.sqrt(sum(item * item for item in vector)) or 1.0
    return [item / norm for item in vector]


def zone_distance_bounds(df: int, zone: str) -> tuple[float, float]:
    """稀有度分区对应的马氏距离区间（用 χ² 分位表达 p 阈值）。"""
    bounds = {
        "Z1": (0.0, chi2_ppf(0.5, df)),
        "Z2": (chi2_ppf(0.5, df), chi2_ppf(0.9, df)),
        "Z3": (chi2_ppf(0.9, df), chi2_ppf(0.99, df)),
        "Z4": (chi2_ppf(0.99, df), chi2_ppf(0.9999, df)),
    }
    return bounds.get(zone, (0.0, chi2_ppf(0.5, df)))


def zone_for_distance(d2: float, df: int) -> str:
    """马氏距离 → Z1-Z4。"""
    return rarity_zone(chi2_sf(d2, df))


def z_with_distance(size: int, d2: float, rng: random.Random) -> list[float]:
    """构造满足指定 d_M² 的标准化偏差向量（方向均匀，模长 = sqrt(d2)）。"""
    direction = unit_vector(size, rng)
    norm = math.sqrt(max(d2, 0.0))
    return [item * norm for item in direction]


def copula_point_from_z(
    names: list[str],
    distributions: list[tuple[str, dict[str, Any]]],
    pairs: list[tuple[str, str, float]],
    z: list[float],
    rng: random.Random,
) -> list[float]:
    """由标准正态向量 z 经相关矩阵与边缘分位得到参数向量。"""
    lower = cholesky(correlation_matrix(names, pairs))
    correlated = [sum(lower[i][k] * z[k] for k in range(i + 1)) for i in range(len(names))]
    row: list[float] = []
    for idx, (kind, params) in enumerate(distributions):
        quantile = distribution_quantile(kind, params, normal_cdf(correlated[idx]))
        if quantile is None:
            value = sample_distribution(kind, params, rng)
            row.append(float(value) if isinstance(value, (int, float)) else 0.0)
        else:
            row.append(float(quantile))
    return row


def zone_samples(
    names: list[str],
    distributions: list[tuple[str, dict[str, Any]]],
    pairs: list[tuple[str, str, float]],
    zone: str,
    count: int,
    rng: random.Random,
) -> list[list[float]]:
    """按目标 Z 分区采样（在分区区间内均匀取 d²，再反解参数）。"""
    df = max(len(names), 1)
    low, high = zone_distance_bounds(df, zone)
    rows: list[list[float]] = []
    for _ in range(count):
        target = low + (high - low) * rng.random()
        rows.append(
            copula_point_from_z(names, distributions, pairs, z_with_distance(df, target, rng), rng)
        )
    return rows


__all__ = [
    "beta_cdf",
    "beta_ppf",
    "chi2_cdf",
    "chi2_ppf",
    "chi2_sf",
    "cholesky",
    "copula_samples",
    "correlation_matrix",
    "distribution_moments",
    "distribution_quantile",
    "latin_hypercube",
    "mahalanobis_d2",
    "normal_cdf",
    "normal_pdf",
    "normal_ppf",
    "rarity_zone",
    "sample_distribution",
    "unit_vector",
    "z_with_distance",
    "zone_distance_bounds",
    "zone_for_distance",
    "zone_samples",
]
