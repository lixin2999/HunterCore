#!/usr/bin/env python3
"""从实机 CARLA 采集各内置城镇的合法出生点，导出为平台出生点表。

为什么需要这个脚本
------------------
``map.spawn_point``（自车出生点）必须是**目标地图上的有效车道坐标**。CARLA 内置城镇的世界
原点 (0, 0, 0) 普遍落在空地/水面/路网外：平台若把出生点写成原点，仿真端表现为「黑屏」或
「画面只有路面」，且参与者相对偏移会一起失真。

平台不猜测坐标：合法出生点只能来自实机 CARLA 的 ``world.get_map().get_spawn_points()``。
仓库内的 ``services/scene-service/config/carla_spawn_points.json`` 出厂时是**未校准骨架**
（``calibrated=false``、各城镇候选点为空），必须由本脚本在能连到 CARLA 的机器上跑一次，
把真实坐标写回并把 ``calibrated`` 置为 ``true``。

用法
----
::

    # 1) 在装有 CARLA PythonAPI 的机器上（与 CARLA 同版本 wheel），采集全部内置城镇
    python scripts/collect_carla_spawn_points.py --host 192.168.31.35 --port 2000

    # 2) 只采指定城镇、每镇最多 40 个点，先写到临时文件确认内容
    python scripts/collect_carla_spawn_points.py --towns Town03 Town10HD_Opt \
        --max-points 40 --out /tmp/carla_spawn_points.json

    # 3) 采集完成后分发到部署位（scene-service 读同一份文件；K8s 下更新 ConfigMap 后重启）
    python scripts/collect_carla_spawn_points.py --out services/scene-service/config/carla_spawn_points.json

说明：
- 本脚本只依赖标准库 + ``carla``，**在部署机系统 Python 3.10 上直接跑**（不用项目 .venv），因此禁用 3.11+ 才有的
  标准库写法（由 ``tests/test_host_scripts_compat.py`` 钉住）；
- 本脚本**不预设 CARLA 小版本**：客户端超时接口在 0.9.10 前后改名（``set_timeout`` → ``set_client_timeout``），
  两个名字都会尝试；缺少必需接口时会以 ``[error]`` + 版本不匹配提示退出，而不是采到一半报 AttributeError
  （由 ``tests/test_carla_collectors_api_compat.py`` 钉住）；
- 逐城镇 ``load_world`` 会重置该世界，**请勿在有仿真实例运行时采集**；
- 已存在目标文件时按城镇**增量合并**（只覆盖本次采集到的城镇，其余城镇保留），便于分多次采完；
- ``--print`` 只打印不落盘，用于快速核对；
- CARLA 的 ``rotation.yaw`` 单位为度，导出时统一折算为契约要求的弧度并归一到 ``[-pi, pi]``。

退出码：0 全部城镇采集成功；1 部分城镇失败（已采集结果仍会写出）；2 环境/参数错误。
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
#: 出生点表默认落盘位置（scene-service 包内默认路径，容器内为 /app/config/carla_spawn_points.json）
DEFAULT_OUTPUT = ROOT / "services" / "scene-service" / "config" / "carla_spawn_points.json"
#: 原点退化坐标丢弃阈值（米）：与校验门 G3 的 :func:`is_origin_like` 口径保持一致
DEFAULT_ORIGIN_TOLERANCE_M = 1.0
#: 与 contracts/scene-taxonomy.yaml / 批量脚本一致的内置城镇集合
DEFAULT_TOWNS: tuple[str, ...] = (
    "Town01",
    "Town02",
    "Town03",
    "Town04",
    "Town05",
    "Town06",
    "Town07",
    "Town10HD_Opt",
)


def _wrap_rad(angle_deg: float) -> float:
    """CARLA 航向角（度）→ 契约弧度，并归一到 ``[-pi, pi]``。"""
    wrapped = math.radians(float(angle_deg))
    return (wrapped + math.pi) % (2.0 * math.pi) - math.pi


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


def _collect_town(client: Any, town_name: str, *, origin_tolerance: float) -> list[dict[str, Any]]:
    """加载指定城镇并抽取出生点（丢弃落在原点的退化坐标）。"""
    world = client.load_world(town_name)
    spawn_points = world.get_map().get_spawn_points()
    points: list[dict[str, Any]] = []
    for transform in spawn_points:
        location, rotation = transform.location, transform.rotation
        if abs(location.x) <= origin_tolerance and abs(location.y) <= origin_tolerance:
            continue
        points.append(
            {
                "x": round(location.x, 3),
                "y": round(location.y, 3),
                "z": round(location.z, 3),
                "yaw": round(_wrap_rad(rotation.yaw), 6),
            }
        )
    if not points:
        raise RuntimeError(f"{town_name} 未返回任何有效出生点")
    return points


def _thin(points: list[dict[str, Any]], max_points: int) -> list[dict[str, Any]]:
    """等间隔抽稀到 ``max_points``（保持 CARLA 返回顺序，使同批场景分散到不同车道）。"""
    if max_points <= 0 or len(points) <= max_points:
        return points
    step = len(points) / max_points
    return [points[int(index * step)] for index in range(max_points)]


def _merge_payload(existing: Path, collected: dict[str, list[dict[str, Any]]], *, carla_version: str) -> dict[str, Any]:
    """增量合并：只覆盖本次采集到的城镇，其余城镇与容差配置沿用原文件。"""
    payload: dict[str, Any] = {}
    if existing.is_file():
        try:
            loaded = json.loads(existing.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"现有出生点表不是合法 JSON，无法合并：{existing}（{exc}）") from exc
        if isinstance(loaded, dict):
            payload = loaded
    maps = payload.get("maps")
    if not isinstance(maps, dict):
        maps = {}
    for town_name, points in collected.items():
        maps[town_name] = {"spawn_points": points}
    payload.setdefault("schema_version", "1.0")
    payload.setdefault("origin_tolerance_m", DEFAULT_ORIGIN_TOLERANCE_M)
    payload.setdefault("match_tolerance_m", 2.0)
    payload["maps"] = maps
    payload["calibrated"] = any(
        isinstance(section, dict) and section.get("spawn_points") for section in maps.values()
    )
    payload["source"] = "live-carla-export"
    payload["carla_version"] = carla_version
    payload["exported_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="采集实机 CARLA 内置城镇出生点，导出平台 carla_spawn_points.json",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--host", default="127.0.0.1", help="CARLA 服务器地址")
    parser.add_argument("--port", type=int, default=2000, help="CARLA RPC 端口")
    parser.add_argument(
        "--towns",
        nargs="+",
        default=list(DEFAULT_TOWNS),
        help=f"待采集城镇（默认全部：{', '.join(DEFAULT_TOWNS)}）",
    )
    parser.add_argument("--max-points", type=int, default=60, help="每城镇最多保留点数（<=0 不限）")
    parser.add_argument(
        "--origin-tolerance",
        type=float,
        default=DEFAULT_ORIGIN_TOLERANCE_M,
        help="落在原点多近内视为退化坐标并丢弃（米）",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="CARLA 客户端超时（秒；load_world 较慢，不建议调小）",
    )
    parser.add_argument("--out", type=Path, default=DEFAULT_OUTPUT, help="导出文件路径")
    parser.add_argument("--print", dest="echo", action="store_true", help="只打印结果，不写文件")
    return parser


def main(argv: list[str] | None = None) -> int:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            reconfigure(encoding="utf-8", errors="replace")

    args = build_parser().parse_args(argv)

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
            "[warn] 该 PythonAPI 既无 set_client_timeout 也无 set_timeout，本次将用其默认超时；"
            "逐城镇 load_world 较慢时可能超时报错。",
            file=sys.stderr,
        )
    missing = _missing_api(client, ("load_world",))
    if missing:
        abi = f"cp{sys.version_info[0]}{sys.version_info[1]}"
        print(
            f"[error] 本地 carla PythonAPI（{client_version}）缺少必需接口 {missing}，与服务器 {carla_version} 不匹配，\n"
            f"        采到一半才报 AttributeError 即是此成因。请安装**该服务器同版本**发行包自带的 wheel\n"
            f"        （不要裸 `pip install carla`，PyPI 上的版本号与服务器不一定对应）：\n"
            f"        pip3 install --force-reinstall <CARLA 安装目录>/PythonAPI/carla/dist/carla-{carla_version}-{abi}-*.whl",
            file=sys.stderr,
        )
        return 2

    collected: dict[str, list[dict[str, Any]]] = {}
    failures: list[str] = []
    attr_failure = False
    for town_name in args.towns:
        try:
            points = _thin(_collect_town(client, town_name, origin_tolerance=args.origin_tolerance), args.max_points)
        except Exception as exc:  # noqa: BLE001
            print(f"[warn] {town_name} 采集失败：{type(exc).__name__}: {exc}", file=sys.stderr)
            attr_failure = attr_failure or isinstance(exc, AttributeError)
            failures.append(town_name)
            continue
        collected[town_name] = points
        print(f"[town] {town_name}: {len(points)} 个出生点（首个 ({points[0]['x']:.1f}, {points[0]['y']:.1f})）")

    if not collected:
        print("[error] 全部城镇采集失败，未写出文件", file=sys.stderr)
        if attr_failure:
            print(
                "        失败形态为 AttributeError：基本肯定是本地 wheel 与服务器 PythonAPI 不一致（看上一条 warn 的具体接口名）。",
                file=sys.stderr,
            )
        return 1

    try:
        payload = _merge_payload(args.out, collected, carla_version=carla_version)
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
        f"       calibrated={payload['calibrated']}，覆盖 {len(collected)} 个城镇"
        + (f"，失败：{', '.join(failures)}" if failures else "")
        + "\n       后续：重启 scene-service（或更新 K8s ConfigMap 后滚动重启）使新表生效，"
        "再重新批量生成/复检存量场景。"
    )
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
