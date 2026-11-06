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
- 本脚本只依赖标准库 + ``carla``，**在部署机系统 Python 3.10 上直接跑**（不用项目 .venv），因此禁用 3.11+ 才有的
  标准库写法（由 ``tests/test_host_scripts_compat.py`` 钉住）；
- 本脚本**不预设 CARLA 小版本**：客户端超时接口名在不同构建间并不一致（官方 wheel 多为
  ``set_client_timeout``，而部署机上的自编译 wheel 自称 0.9.16 却只有 ``set_timeout``），
  两个名字都会尝试；缺少必需接口（如 ``Blueprint.get_bounding_box``）时会以 ``[error]`` + 版本不匹配提示退出，
  而不是采到一半报 AttributeError，也**不会**用推算值冒充实机包围盒（由 ``tests/test_carla_collectors_api_compat.py`` 钉住）；
- ⚠ **客户端与服务器必须同版本**：否则 ``client.get_world()`` 可能先打 ``WARNING: Version mismatch
  detected``，随后在 **C++ 层 ``std::bad_alloc`` 直接 abort（核心已转储）**——这不是 Python 异常，
  本脚本无法兜住。部署环境若服务器为源码编译的 dev build（版本号是 git 提交号而非 x.y.z），
  必须用**同一份源码构建的 carla wheel** 采集；否则本表只能保持未校准，由仿真端在本地实机取尺寸；
- ⚠ ``--model-map`` 里的蓝图 ID 必须在实机蓝图库里真实存在。当前自车模型 ``HUNTER_SE`` **尚未导入
  CARLA 资产蓝图库**，因此该映射暂时无解（脚本会报蓝图未匹配、退出码 1，不会编造 ID）；
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
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
#: 几何表默认落盘位置（scene-service 包内默认路径，容器内为 /app/config/carla_vehicle_geometry.json）
DEFAULT_OUTPUT = ROOT / "services" / "scene-service" / "config" / "carla_vehicle_geometry.json"
#: 与 provider 一致的挂载点换算口径标记（平台不写死偏移，由仿真端按包围盒推导）
MOUNT_POLICY = "derive_from_blueprint_bounding_box"


def _vector3d(value: Any) -> dict[str, float]:
    return {"x": round(float(value.x), 4), "y": round(float(value.y), 4), "z": round(float(value.z), 4)}


#: 客户端超时接口在 PythonAPI 0.9.10 前后**改名**（更早为 ``set_timeout``），写死新名会在旧 wheel 上
#: 抛 ``AttributeError: 'Client' object has no attribute 'set_client_timeout'``（实机首发）。
_TIMEOUT_SETTERS: tuple[str, ...] = ("set_client_timeout", "set_timeout")


def _apply_client_timeout(client: Any, seconds: float) -> str:
    """按当前 PythonAPI **实际提供**的接口设置 RPC 超时，返回用到的方法名（都没有则返回空串）。

    部分构建只接受 ``datetime.timedelta``，故 float 抛 TypeError 时再试一次；两者都不可用
    则不阻断采集（用 wheel 自带默认超时，并由调用侧给出警告）。
    """
    for name in _TIMEOUT_SETTERS:
        setter = getattr(client, name, None)
        if not callable(setter):
            continue
        for value in (float(seconds), timedelta(seconds=seconds)):
            try:
                setter(value)
            except TypeError:
                continue
            return name
        raise RuntimeError(f"carla.Client.{name}() 既不接受 float 也不接受 timedelta")
    return ""


def _missing_api(obj: Any, names: tuple[str, ...]) -> list[str]:
    """返回 obj 上不存在的接口名——把「wheel 与服务器不同版本」变成可读错误而非中途 AttributeError。"""
    return [name for name in names if not callable(getattr(obj, name, None))]


def _client_version(client: Any) -> str:
    """尽力取得本地 wheel 的版本号（旧构建无 ``get_client_version`` 时返回「未知」）。"""
    getter = getattr(client, "get_client_version", None)
    if not callable(getter):
        return "未知"
    try:
        return str(getter())
    except Exception:  # noqa: BLE001
        return "未知"


def _api_mismatch_hint(
    missing: list[str], *, where: str, client_version: str, server_version: str
) -> str:
    """版本不匹配的统一处置文案（带可直接粘的 wheel 名）。"""
    abi = f"cp{sys.version_info[0]}{sys.version_info[1]}"
    return (
        f"[error] 本地 carla PythonAPI（{client_version}）在 {where} 上缺少必需接口 {missing}，"
        f"与服务器 {server_version} 不匹配。\n"
        "        几何表需要实机包围盒，**平台不会用 dimension/2 推算值冒充**（那样会得到错的挂载边界）。\n"
        "        请安装该服务器同版本发行包自带的 wheel（不要裸 `pip install carla`，PyPI 版本与服务器不一定对应）：\n"
        f"        pip3 install --force-reinstall <CARLA 安装目录>/PythonAPI/carla/dist/carla-{server_version}-{abi}-*.whl"
    )


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
    payload["exported_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
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
    try:
        timeout_api = _apply_client_timeout(client, float(args.timeout))
    except (AttributeError, TypeError, RuntimeError) as exc:
        print(f"[error] 设置 CARLA 客户端超时失败：{type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    try:
        carla_version = client.get_server_version()
    except Exception as exc:  # noqa: BLE001
        print(
            f"[error] 无法连接 CARLA {args.host}:{args.port}：{exc}\n"
            "        请确认服务已启动、RPC 端口可达（默认 2000）、且本机与服务器同网段；\n"
            "        若错误里提到 protocol/version 不兼容，则是**本地 wheel 与服务器版本不一致**（需同版本 PythonAPI）。",
            file=sys.stderr,
        )
        return 2
    client_version = _client_version(client)
    print(
        f"[carla] 已连接 {args.host}:{args.port}"
        f"（server {carla_version}，本地 PythonAPI {client_version}，超时接口 {timeout_api or '无'}）"
    )
    if not timeout_api:
        print(
            "[warn] 该 PythonAPI 既无 set_client_timeout 也无 set_timeout，本次将用其默认超时。",
            file=sys.stderr,
        )
    missing = _missing_api(client, ("get_world",))
    if missing:
        print(
            _api_mismatch_hint(missing, where="Client", client_version=client_version, server_version=carla_version),
            file=sys.stderr,
        )
        return 2
    world = client.get_world()
    missing = _missing_api(world, ("get_blueprint_library",))
    if missing:
        print(
            _api_mismatch_hint(missing, where="World", client_version=client_version, server_version=carla_version),
            file=sys.stderr,
        )
        return 2
    blueprint_library = world.get_blueprint_library()
    missing = _missing_api(blueprint_library, ("filter",))
    if missing:
        print(
            _api_mismatch_hint(
                missing, where="BlueprintLibrary", client_version=client_version, server_version=carla_version
            ),
            file=sys.stderr,
        )
        return 2

    selected = _select_blueprints(blueprint_library, list(args.blueprints), all_vehicles=args.all_vehicles)
    if not selected:
        print("[error] 没有匹配到任何车辆蓝图，未写出文件", file=sys.stderr)
        return 1

    missing = _missing_api(selected[0], ("get_dimension", "get_bounding_box"))
    if missing:
        print(
            _api_mismatch_hint(
                missing,
                where=f"Blueprint（示例 {selected[0].id}）",
                client_version=client_version,
                server_version=carla_version,
            ),
            file=sys.stderr,
        )
        return 2

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
