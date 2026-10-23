"""场景引擎（IR / 目录 / 统计 / 校验门 / 生成 / 编译）。

模块边界（自下而上）：
- ``stats``：概率与采样内核（截断分布、Copula、马氏距离、分区反解）；
- ``ir``：``hunter.ir/1.2`` 数据模型与表达式白名单求值；
- ``catalog`` / ``oracles``：内置逻辑场景与判据目录（代码即事实来源）；
- ``validators``：G1-G7 与稀有度/临界度/风险分级；
- ``generator``：五种生成策略 + G8 去重；
- ``compiler``：一次编译（缺省值 + G1-G7 + SceneConfig）。
"""
from __future__ import annotations

from app.scene_engine.catalog import CATALOG, CATALOG_BY_ID, CATALOG_VERSION, get_ir
from app.scene_engine.compiler import (
    Compilation,
    GateFailure,
    compile_scene,
    default_values,
    render_scene,
)
from app.scene_engine.generator import (
    ALLOWED_OVERRIDES,
    PROFILE_QUOTAS,
    GenerationOptions,
    GenerationOutcome,
    SceneGenerator,
    ensure_overrides_shape,
)
from app.scene_engine.ir import IR_VERSION, SceneIR
from app.scene_engine.oracles import ORACLES, get_oracle, list_oracles
from app.scene_engine.validators import (
    RISK_MATRIX,
    ValidationReport,
    assess_rarity,
    content_hash,
    criticality_index,
    criticality_level,
    ir_content_hash,
    risk_level,
    run_gates,
)

__all__ = [
    "ALLOWED_OVERRIDES",
    "CATALOG",
    "CATALOG_BY_ID",
    "CATALOG_VERSION",
    "IR_VERSION",
    "ORACLES",
    "PROFILE_QUOTAS",
    "RISK_MATRIX",
    "Compilation",
    "GateFailure",
    "GenerationOptions",
    "GenerationOutcome",
    "SceneGenerator",
    "SceneIR",
    "ValidationReport",
    "assess_rarity",
    "compile_scene",
    "content_hash",
    "criticality_index",
    "criticality_level",
    "default_values",
    "ensure_overrides_shape",
    "get_ir",
    "get_oracle",
    "ir_content_hash",
    "list_oracles",
    "render_scene",
    "risk_level",
    "run_gates",
]
