"""CARLA 车辆蓝图几何表（``carla_vehicle_geometry``）单一事实来源。

为什么需要这张表
----------------
仿真端的传感器（尤其前视相机）挂载点必须相对**车辆蓝图的实际外形**给出：固定偏移
（历史上按 0.40 / 0.30 m 写死）一旦碰上「蓝图资产缺失 → 回退到全尺寸蓝图」这类情形，镜头
就会埋进车身或贴地，画面表现为「只有路面」。平台无法替仿真端决定挂载点，但可以把判定
所需的**客观尺寸**给全：CARLA 蓝图的 `get_dimension()` 与 `get_bounding_box()` 是实机数据，
由 ``scripts/collect_carla_vehicle_geometry.py`` 导出到本文件。

下发时（``services.simulation``）按 ``ego_vehicle.model`` 解析出蓝图并把结果放进载荷根节点
``sensor_mount_reference``——纯附加信息，不改变任何现有字段：

- 表已校准且命中蓝图：给出 ``dimension_m``（实机长宽高）与 ``envelope_m``（由包围盒推导的
  前缘 / 后缘 / 侧缘 / 车顶 / 底部坐标，相对车辆自身坐标系，单位米）；
- 表未校准 / 未命中蓝图：只给出 ``status`` 与提示，**不携带任何数字**——平台不编造尺寸，
  仿真端看到该状态即应回退到自身按蓝图实时查询的逻辑。

``mount_policy`` 只是双方约定的口径标记（挂载点由仿真端按尺寸换算），平台不自行换算。

加载策略与 ``vehicle_params`` / ``carla_spawn_points`` 一致：默认读包内
``config/carla_vehicle_geometry.json``，部署时可用 ``SCENE_CARLA_VEHICLE_GEOMETRY_PATH`` 覆盖；
每次加载计算文件内容 SHA-256（``source_hash``）留痕。
"""
from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

from app.scene_engine.ir import is_finite_number

#: 默认几何表（仓库内 ``services/scene-service/config/carla_vehicle_geometry.json``；
#: 容器内解析为 ``/app/config/carla_vehicle_geometry.json``，由 Dockerfile 的 ``COPY config`` 内置）
DEFAULT_CARLA_VEHICLE_GEOMETRY_PATH = (
    Path(__file__).resolve().parents[2] / "config" / "carla_vehicle_geometry.json"
)

#: 下发载荷中挂载参考块的根节点键（契约 openapi ``x-hunter-simulation-flow.camera_mount_reference``）
SENSOR_MOUNT_REFERENCE_KEY = "sensor_mount_reference"
#: 挂载参考块的结构版本（仿真端按该版本解释字段含义）
SENSOR_MOUNT_REFERENCE_VERSION = "1.0"
#: 约定的挂载点换算口径：由仿真端按蓝图包围盒推导（平台不写死偏移）
MOUNT_POLICY_DERIVE_FROM_BBOX = "derive_from_blueprint_bounding_box"

#: 参考块状态：命中蓝图 / 表未校准 / 表已校准但车型未命中蓝图 / 表文件不可用
STATUS_RESOLVED = "resolved"
STATUS_TABLE_UNCALIBRATED = "table_uncalibrated"
STATUS_BLUEPRINT_UNMATCHED = "blueprint_unmatched"
STATUS_TABLE_UNAVAILABLE = "table_unavailable"

#: 几何表缺数据时的说明（供下发提示与脚本复用）
GEOMETRY_UNCALIBRATED_HINT = (
    "车辆几何表未校准（config/carla_vehicle_geometry.json 的 calibrated=false 或无蓝图数据），"
    "请在装有 CARLA PythonAPI 的机器上运行 scripts/collect_carla_vehicle_geometry.py 导出实机尺寸"
)

_NON_ALNUM = re.compile(r"[^a-z0-9]+")


class VehicleGeometryError(ValueError):
    """车辆几何表缺失/结构非法（加载即失败，禁止带病运行）。"""


def normalize_model_key(value: Any) -> str:
    """车型/蓝图标识归一化（大小写、空格、连字符/斜杠差异统一，便于跨口径匹配）。"""
    return _NON_ALNUM.sub("_", str(value or "").strip().lower()).strip("_")


@dataclass(frozen=True, slots=True)
class VehicleGeometry:
    """单个车辆蓝图的实机外形（尺寸与包围盒，单位米，车辆自身坐标系：X 向前、Z 向上）。"""

    blueprint_id: str
    display_name: str
    length_m: float
    width_m: float
    height_m: float
    center_x_m: float
    center_y_m: float
    center_z_m: float
    extent_x_m: float
    extent_y_m: float
    extent_z_m: float

    def dimension_dict(self) -> dict[str, float]:
        """实机长宽高（契约字段；仿真端可直接据此换算）。"""
        return {
            "length_m": round(self.length_m, 4),
            "width_m": round(self.width_m, 4),
            "height_m": round(self.height_m, 4),
        }

    def envelope_dict(self) -> dict[str, float]:
        """包围盒边界（相对车辆自身坐标原点，由 center ± extent 推导，纯算术）。

        只给「边界」而不给「挂载点」：Y 轴朝向的正负约定由仿真端按其渲染框架解释，
        平台不臆断左右（故侧缘给对称的绝对值）。
        """
        return {
            "front_edge_x_m": round(self.center_x_m + self.extent_x_m, 4),
            "rear_edge_x_m": round(self.center_x_m - self.extent_x_m, 4),
            "side_edge_y_m": round(abs(self.center_y_m) + self.extent_y_m, 4),
            "roof_z_m": round(self.center_z_m + self.extent_z_m, 4),
            "bottom_z_m": round(self.center_z_m - self.extent_z_m, 4),
            "half_length_m": round(self.extent_x_m, 4),
            "half_width_m": round(self.extent_y_m, 4),
            "half_height_m": round(self.extent_z_m, 4),
        }

    def to_dict(self) -> dict[str, Any]:
        """可序列化摘要（写入参考块与校验报告，用于追溯尺寸来源）。"""
        return {
            "blueprint": self.blueprint_id,
            "display_name": self.display_name,
            "dimension_m": self.dimension_dict(),
            "bounding_box_m": {
                "center": {
                    "x": round(self.center_x_m, 4),
                    "y": round(self.center_y_m, 4),
                    "z": round(self.center_z_m, 4),
                },
                "extent": {
                    "x": round(self.extent_x_m, 4),
                    "y": round(self.extent_y_m, 4),
                    "z": round(self.extent_z_m, 4),
                },
            },
            "envelope_m": self.envelope_dict(),
        }


@dataclass(frozen=True, slots=True)
class VehicleGeometryTable:
    """车辆几何表（按 CARLA 蓝图 ID 索引，另含车型 → 蓝图映射）。"""

    calibrated: bool
    mount_policy: str
    blueprints: Mapping[str, VehicleGeometry] = field(default_factory=dict)
    #: 归一化车型/蓝图标识 → 蓝图 ID（显式映射优先，其次 display_name / 蓝图 ID 归一化匹配）
    model_index: Mapping[str, str] = field(default_factory=dict)
    source: str = ""
    source_hash: str = ""
    carla_version: str | None = None
    exported_at: str | None = None

    def usable(self) -> bool:
        """该表能否给出可信尺寸（已校准且至少登记一个蓝图）。"""
        return self.calibrated and bool(self.blueprints)

    def resolve(self, ego_model: Any) -> VehicleGeometry | None:
        """按 ``ego_vehicle.model`` 解析蓝图几何；表不可用或未命中时返回 ``None``。"""
        if not self.usable():
            return None
        key = normalize_model_key(ego_model)
        if not key:
            return None
        blueprint_id = self.model_index.get(key)
        if blueprint_id is None:
            return None
        return self.blueprints.get(blueprint_id)

    def to_dict(self) -> dict[str, Any]:
        """可序列化摘要（不含逐蓝图明细，明细见 :meth:`resolve`）。"""
        return {
            "calibrated": self.calibrated,
            "mount_policy": self.mount_policy,
            "blueprints": sorted(self.blueprints),
            "model_index": dict(sorted(self.model_index.items())),
            "source": self.source,
            "source_hash": self.source_hash,
            "carla_version": self.carla_version,
            "exported_at": self.exported_at,
        }


# =====================================================================
# 加载与解析
# =====================================================================


def _positive(raw: Mapping[str, Any], key: str, label: str) -> float:
    value = raw.get(key)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise VehicleGeometryError(f"{label}.{key} 必须是数值")
    number = float(value)
    if not is_finite_number(number) or number <= 0:
        raise VehicleGeometryError(f"{label}.{key} 必须为正有限数值")
    return number


def _coordinate(raw: Any, key: str, label: str) -> tuple[float, float, float]:
    if not isinstance(raw, Mapping):
        raise VehicleGeometryError(f"{label}.{key} 必须是对象")
    out: list[float] = []
    for axis in ("x", "y", "z"):
        value = raw.get(axis)
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise VehicleGeometryError(f"{label}.{key}.{axis} 必须是数值")
        number = float(value)
        if not is_finite_number(number):
            raise VehicleGeometryError(f"{label}.{key}.{axis} 必须是有限数值")
        out.append(number)
    return out[0], out[1], out[2]


def _parse_geometry(blueprint_id: str, raw: Any) -> VehicleGeometry:
    label = f"carla_vehicle_geometry.blueprints.{blueprint_id}"
    if not isinstance(raw, Mapping):
        raise VehicleGeometryError(f"{label} 必须是对象")
    center = raw.get("center") if "center" in raw else None
    extent = raw.get("extent") if "extent" in raw else None
    if "bounding_box" in raw:
        bbox = raw["bounding_box"]
        if not isinstance(bbox, Mapping):
            raise VehicleGeometryError(f"{label}.bounding_box 必须是对象")
        center = bbox.get("center", center)
        extent = bbox.get("extent", extent)
    if center is None or extent is None:
        raise VehicleGeometryError(f"{label} 必须同时提供 bounding_box.center 与 bounding_box.extent")
    cx, cy, cz = _coordinate(center, "center", label)
    ex, ey, ez = _coordinate(extent, "extent", label)
    for axis, value in zip(("x", "y", "z"), (ex, ey, ez), strict=True):
        if value <= 0 or not is_finite_number(value):
            raise VehicleGeometryError(f"{label}.bounding_box.extent.{axis} 必须为正有限数值")

    dimension = raw.get("dimension") or {}
    if not isinstance(dimension, Mapping):
        raise VehicleGeometryError(f"{label}.dimension 必须是对象")
    # 未显式导出 dimension 时按包围盒推导（2 × extent），保证不引入编造数值
    length = _positive(dimension, "length_m", f"{label}.dimension") if "length_m" in dimension else 2.0 * ex
    width = _positive(dimension, "width_m", f"{label}.dimension") if "width_m" in dimension else 2.0 * ey
    height = _positive(dimension, "height_m", f"{label}.dimension") if "height_m" in dimension else 2.0 * ez

    return VehicleGeometry(
        blueprint_id=str(blueprint_id),
        display_name=str(raw.get("display_name") or ""),
        length_m=length,
        width_m=width,
        height_m=height,
        center_x_m=cx,
        center_y_m=cy,
        center_z_m=cz,
        extent_x_m=ex,
        extent_y_m=ey,
        extent_z_m=ez,
    )


def _build_model_index(
    raw_mapping: Mapping[str, Any], blueprints: Mapping[str, VehicleGeometry]
) -> dict[str, str]:
    """归一化「显式车型映射 + 蓝图 ID + 展示名」三级索引（显式映射优先，冲突不覆盖）。"""
    index: dict[str, str] = {}
    for blueprint_id in blueprints:
        index.setdefault(normalize_model_key(blueprint_id), blueprint_id)
        display = normalize_model_key(blueprints[blueprint_id].display_name)
        if display:
            index.setdefault(display, blueprint_id)
    for model, blueprint_id in raw_mapping.items():
        if blueprint_id is None:
            continue  # 骨架里留空（null）= 部署方尚未填写，按未映射处理
        key = normalize_model_key(model)
        value = str(blueprint_id).strip()
        if not key or not value:
            raise VehicleGeometryError("carla_vehicle_geometry.model_to_blueprint 的键与值必须为非空字符串")
        index[key] = value  # 显式映射覆盖自动索引
    return index


def parse_vehicle_geometry_table(
    payload: Mapping[str, Any], *, source: str, source_hash: str
) -> VehicleGeometryTable:
    """解析并校验车辆几何表载荷（非法即抛 ``VehicleGeometryError``）。"""
    if not isinstance(payload, Mapping):
        raise VehicleGeometryError("carla_vehicle_geometry 根节点必须是对象")
    calibrated = payload.get("calibrated")
    if not isinstance(calibrated, bool):
        raise VehicleGeometryError("carla_vehicle_geometry.calibrated 必须是布尔值")
    mount_policy = str(payload.get("mount_policy") or MOUNT_POLICY_DERIVE_FROM_BBOX)
    if mount_policy != MOUNT_POLICY_DERIVE_FROM_BBOX:
        raise VehicleGeometryError(
            f"carla_vehicle_geometry.mount_policy 目前只支持 {MOUNT_POLICY_DERIVE_FROM_BBOX!r}"
        )

    # 区分「未提供该键」与「提供了但类型不对」：空数组等 falsy 值不得被当作空对象默默放行
    blueprints_raw = payload.get("blueprints", {})
    if not isinstance(blueprints_raw, Mapping):
        raise VehicleGeometryError("carla_vehicle_geometry.blueprints 必须是对象")
    blueprints = {
        str(blueprint_id): _parse_geometry(str(blueprint_id), raw)
        for blueprint_id, raw in blueprints_raw.items()
    }
    if calibrated and not blueprints:
        raise VehicleGeometryError("carla_vehicle_geometry.calibrated=true 但没有任何蓝图尺寸")

    mapping_raw = payload.get("model_to_blueprint", {})
    if not isinstance(mapping_raw, Mapping):
        raise VehicleGeometryError("carla_vehicle_geometry.model_to_blueprint 必须是对象")
    index = _build_model_index(mapping_raw, blueprints)
    if calibrated:
        unknown = sorted(
            str(model)
            for model, blueprint_id in mapping_raw.items()
            if blueprint_id is not None and str(blueprint_id).strip() not in blueprints
        )
        if unknown:
            raise VehicleGeometryError(
                "carla_vehicle_geometry.model_to_blueprint 指向了未登记尺寸的蓝图: "
                f"{unknown}（请先用 scripts/collect_carla_vehicle_geometry.py 采集该蓝图）"
            )

    return VehicleGeometryTable(
        calibrated=calibrated,
        mount_policy=mount_policy,
        blueprints=blueprints,
        model_index=index,
        source=str(payload.get("source") or ""),
        source_hash=source_hash,
        carla_version=payload.get("carla_version"),
        exported_at=payload.get("exported_at"),
    )


def load_vehicle_geometry_table(path: str | Path | None = None) -> VehicleGeometryTable:
    """读取并校验车辆几何表文件（缺失/非法即抛 ``VehicleGeometryError``）。"""
    resolved = Path(path) if path is not None else resolve_vehicle_geometry_table_path()
    try:
        raw = resolved.read_bytes()
    except OSError as exc:
        raise VehicleGeometryError(f"车辆几何表文件不可读: {resolved}") from exc
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise VehicleGeometryError(f"车辆几何表文件不是合法 JSON: {resolved}") from exc
    if not isinstance(payload, dict):
        raise VehicleGeometryError("carla_vehicle_geometry 根节点必须是对象")
    return parse_vehicle_geometry_table(
        payload,
        source=str(payload.get("source") or resolved),
        source_hash=hashlib.sha256(raw).hexdigest(),
    )


def resolve_vehicle_geometry_table_path() -> Path:
    """解析表文件路径（``SCENE_CARLA_VEHICLE_GEOMETRY_PATH`` 优先，缺省包内文件）。"""
    from app.config import settings

    configured = (settings.scene_carla_vehicle_geometry_path or "").strip()
    path = Path(configured) if configured else DEFAULT_CARLA_VEHICLE_GEOMETRY_PATH
    if not path.is_file():
        raise VehicleGeometryError(f"车辆几何表文件不存在: {path}")
    return path


@lru_cache(maxsize=8)
def _load_cached(path: str) -> VehicleGeometryTable:
    return load_vehicle_geometry_table(path)


def get_vehicle_geometry_table() -> VehicleGeometryTable:
    """进程内缓存的车辆几何表（部署变更后需重启或清缓存）。"""
    return _load_cached(str(resolve_vehicle_geometry_table_path()))


def clear_vehicle_geometry_table_cache() -> None:
    """清空缓存（测试或运行期热更新表文件后调用）。"""
    _load_cached.cache_clear()


# =====================================================================
# 下发参考块
# =====================================================================


def build_sensor_mount_reference(
    config: Mapping[str, Any], *, table: VehicleGeometryTable | None = None
) -> dict[str, Any]:
    """构造下发载荷根节点的 ``sensor_mount_reference``（纯函数，不修改入参）。

    表不可用或车型未命中时只给状态与提示、不给任何数字：仿真端据此回退自身查询逻辑，
    平台绝不代填尺寸。
    """
    resolved = table if table is not None else get_vehicle_geometry_table()
    ego = config.get("ego_vehicle") if isinstance(config.get("ego_vehicle"), Mapping) else {}
    ego_model = str((ego or {}).get("model") or "")
    reference: dict[str, Any] = {
        "schema_version": SENSOR_MOUNT_REFERENCE_VERSION,
        "mount_policy": resolved.mount_policy,
        "calibrated": resolved.calibrated,
        "ego_model": ego_model,
        "source": resolved.source,
        "source_hash": resolved.source_hash,
        "carla_version": resolved.carla_version,
        "exported_at": resolved.exported_at,
    }
    geometry = resolved.resolve(ego_model)
    if geometry is None:
        if not resolved.usable():
            reference["status"] = (
                STATUS_TABLE_UNCALIBRATED if not resolved.calibrated else STATUS_BLUEPRINT_UNMATCHED
            )
            reference["hint"] = GEOMETRY_UNCALIBRATED_HINT
        else:
            reference["status"] = STATUS_BLUEPRINT_UNMATCHED
            reference["hint"] = (
                f"车型 {ego_model!r} 未在车辆几何表中登记尺寸，请补充 model_to_blueprint 映射"
                f"或用 scripts/collect_carla_vehicle_geometry.py 采集该蓝图（{GEOMETRY_UNCALIBRATED_HINT}）"
            )
        return reference
    reference["status"] = STATUS_RESOLVED
    reference.update(geometry.to_dict())
    return reference


def unavailable_reference(error: str) -> dict[str, Any]:
    """几何表不可用（文件缺失/结构非法）时的参考块：只给状态与原因，不携带尺寸。

    下发不因该表而失败（参考块属附加信息），但仿真端看到 ``table_unavailable`` 即应回退
    自身按蓝图实时查询的逻辑，而不是把平台传来的空信息当作有效尺寸。
    """
    return {
        "schema_version": SENSOR_MOUNT_REFERENCE_VERSION,
        "mount_policy": MOUNT_POLICY_DERIVE_FROM_BBOX,
        "calibrated": False,
        "status": STATUS_TABLE_UNAVAILABLE,
        "hint": str(error),
    }


__all__ = [
    "DEFAULT_CARLA_VEHICLE_GEOMETRY_PATH",
    "GEOMETRY_UNCALIBRATED_HINT",
    "MOUNT_POLICY_DERIVE_FROM_BBOX",
    "SENSOR_MOUNT_REFERENCE_KEY",
    "SENSOR_MOUNT_REFERENCE_VERSION",
    "STATUS_BLUEPRINT_UNMATCHED",
    "STATUS_RESOLVED",
    "STATUS_TABLE_UNAVAILABLE",
    "STATUS_TABLE_UNCALIBRATED",
    "VehicleGeometry",
    "VehicleGeometryError",
    "VehicleGeometryTable",
    "build_sensor_mount_reference",
    "clear_vehicle_geometry_table_cache",
    "get_vehicle_geometry_table",
    "load_vehicle_geometry_table",
    "normalize_model_key",
    "parse_vehicle_geometry_table",
    "resolve_vehicle_geometry_table_path",
    "unavailable_reference",
]
