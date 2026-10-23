"""车端车辆参数（``vehicle_params``）单一事实来源。

设计约束（《场景生成器与场景管理详细设计文档》§2.3.4 / G4）：
- 轴距、最大转角、最大制动减速度、最高车速等**物理能力参数不得硬编码**，
  一律来自车端导出的 ``vehicle_params``；
- 服务默认读取包内 ``config/vehicle_params.json``，部署时可用
  ``SCENE_VEHICLE_PARAMS_PATH`` 挂载车端实际导出文件覆盖；
- 每次加载计算文件内容 SHA-256（``source_hash``），随生成产物/校验报告留痕，
  保证"同一批场景使用同一份车辆参数"可追溯、可复现。

最大前轮转角取「车端声明值」与「由轴距/最小转弯半径推导值」的较小者：
两份口径不一致时按更保守（更小）的能力做 G4 运动学校验。
"""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

#: 默认参数文件（仓库内 ``services/scene-service/config/vehicle_params.json``）
DEFAULT_VEHICLE_PARAMS_PATH = (
    Path(__file__).resolve().parents[2] / "config" / "vehicle_params.json"
)


class VehicleParamsError(ValueError):
    """车辆参数缺失/非法（启动即失败，禁止带病运行）。"""


@dataclass(frozen=True, slots=True)
class VehicleParams:
    """车辆物理能力（全部来自车端导出文件，不含代码内默认值）。"""

    profile: str
    wheelbase: float
    max_steer_rad: float
    max_decel: float
    max_speed: float
    min_turn_radius: float
    max_lateral_accel: float
    source: str
    source_hash: str

    def to_dict(self) -> dict[str, Any]:
        """可序列化摘要（写入生成元数据 / 校验报告，用于追溯参数来源）。"""
        return {
            "profile": self.profile,
            "wheelbase_m": round(self.wheelbase, 6),
            "max_steer_rad": round(self.max_steer_rad, 6),
            "max_steer_deg": round(math.degrees(self.max_steer_rad), 3),
            "max_decel_mps2": round(self.max_decel, 6),
            "max_speed_mps": round(self.max_speed, 6),
            "min_turn_radius_m": round(self.min_turn_radius, 6),
            "max_lateral_accel_mps2": round(self.max_lateral_accel, 6),
            "source": self.source,
            "source_hash": self.source_hash,
        }


def _positive(payload: dict[str, Any], key: str) -> float:
    value = payload.get(key)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise VehicleParamsError(f"vehicle_params.{key} 必须是数值")
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise VehicleParamsError(f"vehicle_params.{key} 必须为正有限数值")
    return number


def load_vehicle_params(path: str | Path | None = None) -> VehicleParams:
    """读取并校验车辆参数文件（非法即抛 ``VehicleParamsError``）。"""
    resolved = Path(path) if path is not None else resolve_vehicle_params_path()
    try:
        raw = resolved.read_bytes()
    except OSError as exc:
        raise VehicleParamsError(f"车辆参数文件不可读: {resolved}") from exc
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise VehicleParamsError(f"车辆参数文件不是合法 JSON: {resolved}") from exc
    if not isinstance(payload, dict):
        raise VehicleParamsError("vehicle_params 根节点必须是对象")

    profile = str(payload.get("profile") or resolved.stem)
    wheelbase = _positive(payload, "wheelbase_m")
    min_turn_radius = _positive(payload, "min_turn_radius_m")
    max_steer_deg = _positive(payload, "max_steer_deg")
    if max_steer_deg >= 90.0:
        raise VehicleParamsError("vehicle_params.max_steer_deg 必须小于 90")
    max_decel = _positive(payload, "max_decel_mps2")
    max_speed = _positive(payload, "max_speed_mps")
    max_lateral_accel = _positive(payload, "max_lateral_accel_mps2")

    declared = math.radians(max_steer_deg)
    geometric = math.atan(wheelbase / min_turn_radius)
    # 车端声明值与几何推导值不一致时取较小者（保守），避免 G4 放行不可行转向
    max_steer_rad = min(declared, geometric)
    if max_steer_rad <= 0:
        raise VehicleParamsError("vehicle_params 推导出的最大前轮转角必须为正")

    return VehicleParams(
        profile=profile,
        wheelbase=wheelbase,
        max_steer_rad=max_steer_rad,
        max_decel=max_decel,
        max_speed=max_speed,
        min_turn_radius=min_turn_radius,
        max_lateral_accel=max_lateral_accel,
        source=str(payload.get("source") or resolved),
        source_hash=hashlib.sha256(raw).hexdigest(),
    )


def resolve_vehicle_params_path() -> Path:
    """解析参数文件路径（``SCENE_VEHICLE_PARAMS_PATH`` 优先，缺省包内文件）。"""
    from app.config import settings

    configured = (settings.scene_vehicle_params_path or "").strip()
    path = Path(configured) if configured else DEFAULT_VEHICLE_PARAMS_PATH
    if not path.is_file():
        raise VehicleParamsError(f"车辆参数文件不存在: {path}")
    return path


@lru_cache(maxsize=8)
def _load_cached(path: str) -> VehicleParams:
    return load_vehicle_params(path)


def get_vehicle_params() -> VehicleParams:
    """进程内缓存的车辆参数（按文件路径缓存；部署变更后需重启/清缓存）。"""
    return _load_cached(str(resolve_vehicle_params_path()))


def clear_vehicle_params_cache() -> None:
    """清空缓存（测试或运行期热更新参数文件后调用）。"""
    _load_cached.cache_clear()


__all__ = [
    "DEFAULT_VEHICLE_PARAMS_PATH",
    "VehicleParams",
    "VehicleParamsError",
    "clear_vehicle_params_cache",
    "get_vehicle_params",
    "load_vehicle_params",
    "resolve_vehicle_params_path",
]
