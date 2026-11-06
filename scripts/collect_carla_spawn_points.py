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
from datetime import UTC, datetime
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
    payload["exported_at"] = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
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
    client.set_client_timeout(float(args.timeout))
    try:
        carla_version = client.get_server_version()
    except Exception as exc:  # noqa: BLE001
        print(
            f"[error] 无法连接 CARLA {args.host}:{args.port}：{exc}\n"
            "        请确认服务已启动、RPC 端口可达（默认 2000）、且本机与服务器同网段。",
            file=sys.stderr,
        )
        return 2
    print(f"[carla] 已连接 {args.host}:{args.port}（server {carla_version}）")

    collected: dict[str, list[dict[str, Any]]] = {}
    failures: list[str] = []
    for town_name in args.towns:
        try:
            points = _thin(_collect_town(client, town_name, origin_tolerance=args.origin_tolerance), args.max_points)
        except Exception as exc:  # noqa: BLE001
            print(f"[warn] {town_name} 采集失败：{type(exc).__name__}: {exc}", file=sys.stderr)
            failures.append(town_name)
            continue
        collected[town_name] = points
        print(f"[town] {town_name}: {len(points)} 个出生点（首个 ({points[0]['x']:.1f}, {points[0]['y']:.1f})）")

    if not collected:
        print("[error] 全部城镇采集失败，未写出文件", file=sys.stderr)
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
