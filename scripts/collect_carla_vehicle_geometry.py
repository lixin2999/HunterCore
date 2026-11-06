#!/usr/bin/env python3
"""从实机 CARLA 导出车辆蓝图尺寸，生成平台车辆几何表（``carla_vehicle_geometry.json``）。

为什么需要这个脚本
------------------
仿真端把传感器（尤其前视相机）装在车辆上时，挂载点必须相对**蓝图实际外形**换算。历史实现
用的是写死偏移（0.40 / 0.30 m 一类），一旦蓝图资产缺失而回退到全尺寸车辆，镜头就会埋进车身
或贴地，画面表现为「只有路面」。挂载点属于仿真端的实现细节，平台不代算；平台能做的是把换算
依据——CARLA 蓝图的 ``get_dimension()`` 与 ``get_bounding_box()``——随场景下发。

这些尺寸**只能来自实机**：同一车型在不同 CARLA 版本、不同资产包下的包围盒并不相同，仓库里
的 ``services/scene-service/config/carla_vehicle_geometry.json`` 出厂是**未校准骨架**
（``calibrated=false``、无蓝图数据），必须由本脚本跑一次导出真实数值。

用法
----
::

    # 1) 在能连到 CARLA 的机器上（与服务器同版本 wheel），按 ID 或通配采集
    python scripts/collect_carla_vehicle_geometry.py --host 192.168.31.35 \
        --blueprints 'vehicle.*se*' 'vehicle.lincoln.mkz_2020'

    # 2) 同时登记「平台车型 → 蓝图」映射（ego_vehicle.model 用它解析蓝图）
    python scripts/collect_carla_vehicle_geometry.py --blueprints 'vehicle.*se*' \
        --model-map HUNTER_SE=vehicle.hunter_se.01

    # 3) 先打印确认内容、不落盘
    python scripts/collect_carla_vehicle_geometry.py --all-vehicles --print

说明：
- 蓝图库是服务器级的（与当前地图无关），本脚本**不会** ``load_world``，因此在有仿真实例运行
  时采集也安全；
- 已存在目标文件时按蓝图**增量合并**（只覆盖本次采集到的蓝图，其余保留），便于分多次采完；
- ``model_to_blueprint`` 的值必须是本次或此前已采集到尺寸的蓝图，否则平台加载该表会直接报错；
- 未指定 ``--blueprints``/``--all-vehicles`` 时脚本拒绝执行：避免无目的导出上百个蓝图。

退出码：0 全部采集成功；1 部分蓝图未找到/失败（已采集结果仍会写出）；2 环境/参数错误。
"""
from __future__ import annotations

import argparse
import fnmatch
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
#: 几何表默认落盘位置（scene-service 包内默认路径，容器内为 /app/config/carla_vehicle_geometry.json）
DEFAULT_OUTPUT = ROOT / "services" / "scene-service" / "config" / "carla_vehicle_geometry.json"
#: 与 provider 一致的挂载点换算口径标记（平台不写死偏移，由仿真端按包围盒推导）
MOUNT_POLICY = "derive_from_blueprint_bounding_box"


def _vector3d(value: Any) -> dict[str, float]:
    return {"x": round(float(value.x), 4), "y": round(float(value.y), 4), "z": round(float(value.z), 4)}


def _select_blueprints(blueprint_library: Any, patterns: list[str], *, all_vehicles: bool) -> list[Any]:
    """按 ID / 通配筛选车辆蓝图（``vehicle.*`` 之内，排除 walker 等非机动车）。"""
    vehicles = list(blueprint_library.filter("vehicle.*"))
    if all_vehicles:
        return vehicles
    matched: list[Any] = []
    seen: set[str] = set()
    for pattern in patterns:
        hit = [bp for bp in vehicles if bp.id == pattern or fnmatch.fnmatch(bp.id, pattern)]
        if not hit:
            print(f"[warn] 未匹配到蓝图：{pattern}", file=sys.stderr)
        for bp in hit:
            if bp.id not in seen:
                seen.add(bp.id)
                matched.append(bp)
    return matched


def _collect_one(blueprint: Any) -> dict[str, Any]:
    """抽取单个蓝图的尺寸（dimension + bounding_box，均为实机数值）。"""
    box = blueprint.get_bounding_box()
    dimension = blueprint.get_dimension()
    return {
        "display_name": str(blueprint.display_name or ""),
        "dimension": {
            # CARLA 的 dimension 为 Vector3D：x=车长、y=车宽、z=车高（单位米）
            "length_m": round(float(dimension.x), 4),
            "width_m": round(float(dimension.y), 4),
            "height_m": round(float(dimension.z), 4),
        },
        "bounding_box": {
            # 包围盒相对车辆自身坐标原点：center 为盒心偏移，extent 为半尺寸
            "center": _vector3d(box.location),
            "extent": _vector3d(box.extent),
        },
    }


def _parse_model_map(entries: list[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for entry in entries:
        model, _, blueprint_id = entry.partition("=")
        if not model or not blueprint_id:
            raise ValueError(f"--model-map 需写成 MODEL=blueprint_id，实际收到：{entry!r}")
        mapping[model.strip()] = blueprint_id.strip()
    return mapping


def _merge_payload(
    existing: Path,
    collected: dict[str, Any],
    model_map: dict[str, str],
    *,
    carla_version: str,
) -> dict[str, Any]:
    """增量合并：只覆盖本次采集到的蓝图，其余蓝图与车型映射沿用原文件。"""
    payload: dict[str, Any] = {}
    if existing.is_file():
        try:
            loaded = json.loads(existing.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"现有几何表不是合法 JSON，无法合并：{existing}（{exc}）") from exc
        if isinstance(loaded, dict):
            payload = loaded
    blueprints = payload.get("blueprints")
    if not isinstance(blueprints, dict):
        blueprints = {}
    blueprints.update(collected)
    mapping = payload.get("model_to_blueprint")
    if not isinstance(mapping, dict):
        mapping = {}
    mapping.update(model_map)
    payload.setdefault("schema_version", "1.0")
    payload["calibrated"] = bool(blueprints)
    payload["mount_policy"] = MOUNT_POLICY
    payload["blueprints"] = blueprints
    payload["model_to_blueprint"] = mapping
    payload["source"] = "live-carla-export"
    payload["carla_version"] = carla_version
    payload["exported_at"] = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="导出实机 CARLA 车辆蓝图尺寸，生成平台 carla_vehicle_geometry.json",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--host", default="127.0.0.1", help="CARLA 服务器地址")
    parser.add_argument("--port", type=int, default=2000, help="CARLA RPC 端口")
    parser.add_argument(
        "--blueprints",
        nargs="+",
        default=[],
        metavar="PATTERN",
        help="待采集蓝图 ID 或通配（如 'vehicle.*se*'）；与 --all-vehicles 二选一，必填其一",
    )
    parser.add_argument(
        "--all-vehicles", action="store_true", help="导出全部 vehicle.* 蓝图（体量大，仅按需使用）"
    )
    parser.add_argument(
        "--model-map",
        action="append",
        default=[],
        metavar="MODEL=BLUEPRINT",
        help="登记平台车型 → 蓝图映射（可重复；MODEL 取 ego_vehicle.model 值）",
    )
    parser.add_argument("--timeout", type=float, default=20.0, help="CARLA 客户端超时（秒）")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUTPUT, help="导出文件路径")
    parser.add_argument("--print", dest="echo", action="store_true", help="只打印结果，不写文件")
    return parser


def main(argv: list[str] | None = None) -> int:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            reconfigure(encoding="utf-8", errors="replace")

    args = build_parser().parse_args(argv)
    if not args.blueprints and not args.all_vehicles:
        print(
            "[error] 请至少指定 --blueprints PATTERN ... 或 --all-vehicles（避免无目的导出全部蓝图）",
            file=sys.stderr,
        )
        return 2
    try:
        model_map = _parse_model_map(args.model_map)
    except ValueError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 2

    try:
        import carla  # type: ignore
    except ImportError:
        print(
            "[error] 未找到 carla PythonAPI。请在能连接 CARLA 的机器上安装与服务器同版本的 wheel：\n"
            "        pip install carla==<与服务器一致的版本号>\n"
            "        （wheel 位于 CARLA 安装目录 PythonAPI/carla/dist/，或从 GitHub Releases 获取）",
            file=sys.stderr,
        )
        return 2

    client = carla.Client(args.host, int(args.port))
    client.set_client_timeout(float(args.timeout))
    try:
        carla_version = client.get_server_version()
        blueprint_library = client.get_world().get_blueprint_library()
    except Exception as exc:  # noqa: BLE001
        print(
            f"[error] 无法连接 CARLA {args.host}:{args.port}：{exc}\n"
            "        请确认服务已启动、RPC 端口可达（默认 2000）、且本机与服务器同网段。",
            file=sys.stderr,
        )
        return 2
    print(f"[carla] 已连接 {args.host}:{args.port}（server {carla_version}）")

    selected = _select_blueprints(blueprint_library, list(args.blueprints), all_vehicles=args.all_vehicles)
    if not selected:
        print("[error] 没有匹配到任何车辆蓝图，未写出文件", file=sys.stderr)
        return 1

    requested = {entry.split("=", 1)[1].strip() for entry in args.model_map if "=" in entry}
    missing_targets = sorted(requested - {bp.id for bp in selected})
    if missing_targets:
        # 映射目标必须本次一并采集，否则合并后的表会指向无尺寸的蓝图
        extra = [
            bp
            for bp in blueprint_library.filter("vehicle.*")
            if bp.id in missing_targets
        ]
        if len(extra) < len(missing_targets):
            not_found = sorted(set(missing_targets) - {bp.id for bp in extra})
            print(f"[error] --model-map 指向了不存在的蓝图：{not_found}", file=sys.stderr)
            return 2
        selected.extend(extra)

    collected: dict[str, Any] = {}
    failures: list[str] = []
    for blueprint in selected:
        try:
            collected[blueprint.id] = _collect_one(blueprint)
        except Exception as exc:  # noqa: BLE001
            print(f"[warn] {blueprint.id} 采集失败：{type(exc).__name__}: {exc}", file=sys.stderr)
            failures.append(blueprint.id)
            continue
        box = collected[blueprint.id]["bounding_box"]["extent"]
        print(
            f"[bp] {blueprint.id}: {collected[blueprint.id]['dimension']['length_m']:.2f} × "
            f"{collected[blueprint.id]['dimension']['width_m']:.2f} × "
            f"{collected[blueprint.id]['dimension']['height_m']:.2f} m"
            f"（半尺寸 x={box['x']:.2f} y={box['y']:.2f} z={box['z']:.2f}）"
        )

    if not collected:
        print("[error] 全部蓝图采集失败，未写出文件", file=sys.stderr)
        return 1

    try:
        payload = _merge_payload(args.out, collected, model_map, carla_version=carla_version)
    except RuntimeError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 2
    data = json.dumps(payload, ensure_ascii=False, indent=2)

    if args.echo:
        print(data)
        return 1 if failures else 0

    try:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(data + "\n", encoding="utf-8")
    except OSError as exc:
        print(f"[error] 写出失败：{args.out}（{exc}）", file=sys.stderr)
        return 2
    print(
        f"\n[done] 已写出 {args.out}\n"
        f"       calibrated={payload['calibrated']}，登记 {len(collected)} 个蓝图，"
        f"车型映射 {len(payload['model_to_blueprint'])} 项"
        + (f"，失败：{', '.join(failures)}" if failures else "")
        + "\n       后续：重启 scene-service（或更新 K8s ConfigMap 后滚动重启）使新表生效；"
        "挂载点换算仍需仿真端消费 sensor_mount_reference（本脚本不改变 HunterSim 行为）。"
    )
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
