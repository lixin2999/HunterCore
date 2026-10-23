"""场景 IR（``hunter.ir/1.2``）：逻辑场景的单一事实来源。

IR 只描述**可参数化的逻辑场景**，不绑定具体参数值：
- ``parameters``：边缘分布（截断正态 / 对数正态 / Beta / 均匀 / 分类 / GMM / 常量）；
- ``correlations``：高斯 Copula 的相关对（ρ ∈ (-1, 1)）；
- ``constraints``：线性约束（含单参数边界），生成时拒绝不可行样本（G2）；
- ``bindings``：参数 → ``SceneConfig`` 字段路径的算术表达式（编译为具体场景）；
- ``criticality``：临界度计算所需参数名与安全余量；物理能力（a_max/轴距/最大转角）
  来自车端 ``vehicle_params``，不写入 IR。

表达式求值只允许数字、参数名与 ``+ - * / ** () min max abs``（AST 白名单），
禁止属性访问/函数调用/名称注入。
"""
from __future__ import annotations

import ast
import copy
import math
import operator
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

IR_VERSION = "hunter.ir/1.2"

DistributionKind = Literal[
    "truncated_normal",
    "normal",
    "lognormal",
    "beta",
    "uniform",
    "categorical",
    "gmm",
    "constant",
]

_BINARY_OPERATORS: dict[type[ast.operator], Any] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.Pow: operator.pow,
}
_UNARY_OPERATORS: dict[type[ast.unaryop], Any] = {
    ast.UAdd: operator.pos,
    ast.USub: operator.neg,
}
_ALLOWED_FUNCTIONS = {"min": min, "max": max, "abs": abs, "round": round}


class DistributionIR(BaseModel):
    """边缘分布声明（params 语义见 ``scene_engine.stats``）。"""

    model_config = ConfigDict(extra="forbid")

    kind: DistributionKind = Field(description="分布类型")
    params: dict[str, Any] = Field(default_factory=dict, description="分布参数")

    @model_validator(mode="after")
    def _check(self) -> DistributionIR:
        if self.kind == "categorical":
            choices = self.params.get("choices") or []
            if not choices:
                raise ValueError("categorical 分布必须提供非空 choices")
            for item in choices:
                if "value" not in item:
                    raise ValueError("categorical choices 每项必须含 value")
        if self.kind == "gmm" and not (self.params.get("components") or []):
            raise ValueError("gmm 分布必须提供非空 components")
        if self.kind == "beta" and (
            float(self.params.get("alpha", 2.0)) <= 0
            or float(self.params.get("beta", 2.0)) <= 0
        ):
            raise ValueError("beta 分布 alpha/beta 必须 > 0")
        if self.kind in {"truncated_normal", "normal", "lognormal"} and float(
            self.params.get("std", self.params.get("sigma", 0.0))
        ) < 0:
            raise ValueError("正态族分布 std/sigma 不能为负")
        low = self.params.get("low", self.params.get("min"))
        high = self.params.get("high", self.params.get("max"))
        if low is not None and high is not None and float(low) > float(high):
            raise ValueError("分布 low 不能大于 high")
        return self

    @property
    def is_numeric(self) -> bool:
        return self.kind != "categorical"


class ParameterIR(BaseModel):
    """逻辑场景参数。"""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(pattern=r"^[a-z][a-z0-9_]{0,31}$", description="参数名（小写下划线）")
    distribution: DistributionIR = Field(description="边缘分布")
    unit: str = Field(default="", max_length=16, description="单位（仅元信息）")
    description: str = Field(default="", max_length=128, description="参数说明")


class CorrelationIR(BaseModel):
    """Copula 相关对。"""

    model_config = ConfigDict(extra="forbid")

    left: str = Field(min_length=1, max_length=32)
    right: str = Field(min_length=1, max_length=32)
    rho: float = Field(ge=-0.99, le=0.99, description="相关系数（避免奇异矩阵）")

    @model_validator(mode="after")
    def _check(self) -> CorrelationIR:
        if self.left == self.right:
            raise ValueError("相关对不能是同一参数")
        return self


class ConstraintIR(BaseModel):
    """线性约束：``Σ terms[i] * param[i] op value``（生成时拒绝违反样本）。"""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=64)
    terms: dict[str, float] = Field(min_length=1, description="参数 → 系数")
    op: Literal["<=", "<", ">=", ">"] = Field(description="比较运算符")
    value: float = Field(description="约束右值")
    message: str = Field(default="", max_length=160, description="违反时的可读说明")


class BindingIR(BaseModel):
    """参数 → SceneConfig 字段路径绑定（expr 为算术表达式）。"""

    model_config = ConfigDict(extra="forbid")

    path: str = Field(
        min_length=1,
        max_length=128,
        description="字段路径，如 ego_vehicle.initial_speed / actors[0].spawn_point.x",
    )
    expr: str = Field(min_length=1, max_length=128, description="参数算术表达式")


class CriticalityIR(BaseModel):
    """临界度/运动学校验参数（§2.3、G4）。

    物理能力字段（``max_decel``/``wheelbase``/``max_steer_rad``/``max_lateral_accel``）
    仅为兼容 V1.23.0 已发布快照保留，**不再参与计算**；运行时一律以车端
    ``vehicle_params``（``app.scene_engine.vehicle_params``）为准。
    """

    model_config = ConfigDict(extra="forbid")

    speed_param: str | None = Field(default=None, description="速度参数名（计算 a_req = v²/2(d-d_m)）")
    gap_param: str | None = Field(default=None, description="净距参数名（米）")
    d_margin: float = Field(default=1.0, ge=0, le=5, description="安全余量 d_m（m）")
    # ---- 兼容字段：V1.23.0 快照可能带值，运行时忽略，物理参数以 vehicle_params 为准 ----
    max_decel: float | None = Field(default=None, gt=0, le=15, description="[已废弃] 请用 vehicle_params.max_decel_mps2")
    wheelbase: float | None = Field(default=None, gt=0, le=6, description="[已废弃] 请用 vehicle_params.wheelbase_m")
    max_steer_rad: float | None = Field(default=None, gt=0, le=1.2, description="[已废弃] 请用 vehicle_params.max_steer_deg")
    max_lateral_accel: float | None = Field(default=None, gt=0, le=10, description="[已废弃] 请用 vehicle_params.max_lateral_accel_mps2")


class SceneIR(BaseModel):
    """逻辑场景 IR 根对象。"""

    model_config = ConfigDict(extra="forbid")

    ir_version: Literal["hunter.ir/1.2"] = Field(default=IR_VERSION)
    logical_id: str = Field(pattern=r"^LS-[A-Z0-9-]{3,40}$", description="逻辑场景 ID")
    name: str = Field(min_length=1, max_length=128)
    category: str = Field(min_length=1, max_length=32)
    scene_type: str = Field(min_length=1, max_length=64)
    description: str = Field(default="", max_length=500)
    parameters: list[ParameterIR] = Field(min_length=1, max_length=24)
    correlations: list[CorrelationIR] = Field(default_factory=list, max_length=64)
    constraints: list[ConstraintIR] = Field(default_factory=list, max_length=64)
    bindings: list[BindingIR] = Field(min_length=1, max_length=64)
    base_config: dict[str, Any] = Field(description="未绑定字段的基准 SceneConfig 片段")
    oracle_ref: str = Field(min_length=1, max_length=64, description="判据引用（oracles 目录）")
    allowed_modes: list[str] = Field(default_factory=lambda: ["simulation"], max_length=4)
    criticality: CriticalityIR = Field(default_factory=CriticalityIR)
    tags: list[str] = Field(default_factory=list, max_length=20)

    @model_validator(mode="after")
    def _check_references(self) -> SceneIR:
        names = {parameter.name for parameter in self.parameters}
        if len(names) != len(self.parameters):
            raise ValueError("参数名重复")
        for pair in self.correlations:
            if pair.left not in names or pair.right not in names:
                raise ValueError(f"相关对引用了未知参数: {pair.left}/{pair.right}")
        for constraint in self.constraints:
            unknown = sorted(set(constraint.terms) - names)
            if unknown:
                raise ValueError(f"约束 {constraint.name} 引用了未知参数: {unknown}")
            if not any(abs(coef) > 0 for coef in constraint.terms.values()):
                raise ValueError(f"约束 {constraint.name} 系数不能全为 0")
        for binding in self.bindings:
            used = expression_names(binding.expr)
            unknown = sorted(used - names)
            if unknown:
                raise ValueError(f"绑定 {binding.path} 引用了未知参数: {unknown}")
        for field_name in ("speed_param", "gap_param"):
            value = getattr(self.criticality, field_name)
            if value is not None and value not in names:
                raise ValueError(f"criticality.{field_name} 引用了未知参数: {value}")
        return self

    # ---------- 派生 ----------

    @property
    def parameter_map(self) -> dict[str, ParameterIR]:
        return {parameter.name: parameter for parameter in self.parameters}

    @property
    def numeric_parameters(self) -> list[ParameterIR]:
        return [parameter for parameter in self.parameters if parameter.distribution.is_numeric]

    @property
    def correlation_pairs(self) -> list[tuple[str, str, float]]:
        return [(pair.left, pair.right, pair.rho) for pair in self.correlations]


# =====================================================================
# 表达式求值（AST 白名单）
# =====================================================================


class ExpressionError(ValueError):
    """表达式非法或引用了未知名称。"""


def expression_names(expr: str) -> set[str]:
    """表达式引用的名称集合（校验用；非法表达式抛 ExpressionError）。"""
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as exc:
        raise ExpressionError(f"表达式语法错误: {expr}") from exc
    names: set[str] = set()
    function_names = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id not in function_names:
            names.add(node.id)
        elif isinstance(node, ast.Attribute):
            raise ExpressionError(f"表达式禁止属性访问: {expr}")
    return names


def evaluate_expression(expr: str, variables: dict[str, float]) -> float:
    """求值算术表达式（未知名称/非法节点抛 ExpressionError）。"""

    def _eval(node: ast.AST) -> float:
        if isinstance(node, ast.Expression):
            return _eval(node.body)
        if isinstance(node, ast.Constant):
            if isinstance(node.value, bool) or not isinstance(node.value, (int, float)):
                raise ExpressionError(f"表达式只允许数字常量: {expr}")
            return float(node.value)
        if isinstance(node, ast.Name):
            if node.id not in variables:
                raise ExpressionError(f"表达式引用未知参数: {node.id}")
            return float(variables[node.id])
        if isinstance(node, ast.BinOp) and type(node.op) in _BINARY_OPERATORS:
            return float(_BINARY_OPERATORS[type(node.op)](_eval(node.left), _eval(node.right)))
        if isinstance(node, ast.UnaryOp) and type(node.op) in _UNARY_OPERATORS:
            return float(_UNARY_OPERATORS[type(node.op)](_eval(node.operand)))
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id not in _ALLOWED_FUNCTIONS or node.keywords:
                raise ExpressionError(f"表达式函数不在白名单: {expr}")
            return float(_ALLOWED_FUNCTIONS[node.func.id](*[_eval(arg) for arg in node.args]))
        raise ExpressionError(f"表达式含不允许的语法: {expr}")

    try:
        parsed = ast.parse(expr, mode="eval")
    except SyntaxError as exc:
        raise ExpressionError(f"表达式语法错误: {expr}") from exc
    return _eval(parsed)


# =====================================================================
# SceneConfig 路径写入（支持 actors[0].spawn_point.x）
# =====================================================================


def _parse_path(path: str) -> list[str | int]:
    tokens: list[str | int] = []
    for segment in path.split("."):
        if not segment:
            raise ExpressionError(f"非法字段路径: {path}")
        name, _, rest = segment.partition("[")
        if name:
            tokens.append(name)
        if rest:
            if not rest.endswith("]"):
                raise ExpressionError(f"非法字段路径: {path}")
            index_text = rest[:-1]
            if not index_text.isdigit():
                raise ExpressionError(f"非法数组下标: {path}")
            tokens.append(int(index_text))
    return tokens


def set_path(config: dict[str, Any], path: str, value: Any) -> None:
    """按路径写入（路径必须已存在，避免静默创建错误结构）。"""
    tokens = _parse_path(path)
    current: Any = config
    for position, token in enumerate(tokens[:-1]):
        try:
            current = current[token]
        except (KeyError, IndexError, TypeError) as exc:
            raise ExpressionError(f"字段路径不存在: {path}") from exc
        if isinstance(token, str) and isinstance(current, list) and not isinstance(tokens[position + 1], int):
            raise ExpressionError(f"字段路径数组缺少下标: {path}")
    last = tokens[-1]
    if isinstance(current, dict) and isinstance(last, str):
        if last not in current:
            raise ExpressionError(f"字段路径不存在: {path}")
        current[last] = value
        return
    if isinstance(current, list) and isinstance(last, int):
        if last >= len(current):
            raise ExpressionError(f"字段路径下标越界: {path}")
        current[last] = value
        return
    raise ExpressionError(f"字段路径类型不匹配: {path}")


def compile_parameters(ir: SceneIR, values: dict[str, float | str]) -> dict[str, Any]:
    """把参数向量编译为 SceneConfig（base_config 深拷贝 + bindings 求值）。"""
    config = copy.deepcopy(ir.base_config)
    numeric_values = {name: float(value) for name, value in values.items() if isinstance(value, (int, float))}
    for binding in ir.bindings:
        set_path(config, binding.path, evaluate_expression(binding.expr, numeric_values))
    return config


def is_finite_number(value: float) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(float(value))


__all__ = [
    "IR_VERSION",
    "BindingIR",
    "ConstraintIR",
    "CorrelationIR",
    "CriticalityIR",
    "DistributionIR",
    "ExpressionError",
    "ParameterIR",
    "SceneIR",
    "compile_parameters",
    "evaluate_expression",
    "expression_names",
    "is_finite_number",
    "set_path",
]
