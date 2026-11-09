#!/usr/bin/env python3
"""场景语义自检：按 ``scene_type`` 输出「必需角色 / 实际角色 / 缺失 / 多余」表。

动机
----
平台下发场景时仿真端曾出现 ``actors=1/1``（鬼探头场景看不到横穿行人），根因是场景内容
本身缺少交通参与者，而不是别名键下发问题。本脚本把 ``contracts/scene-taxonomy.yaml::
scene_semantics`` 的必需角色与**真实落库/待入库**的场景 config 逐条比对，供重建后现场自证：

- 必需角色缺失 / 实例不足 / 角色类型或行为不符 → 计入失败，退出码 1；
- 参与者无法回溯到任何声明角色（多余角色）→ 仅提示，不判失败（可能是背景车等扩展）。

两种数据源
----------
1. 平台 API（默认）：读取 ``GET /api/v1/scene/semantics`` 与具体场景详情，按类型抽样审计；
2. dry-run 产物（``--from-file``）：直接审计 ``batch_add_scenes.py --dry-run`` 写出的 JSON。

用法（仓库根目录）::

    # 审计平台已入库场景（direct：X-User-Id/X-Roles；gateway：账号换 JWT）
    python scripts/verify_scene_semantics.py --base-url http://localhost:8081
    python scripts/verify_scene_semantics.py --mode gateway --base-url http://localhost:8080 \\
        --username admin --password 'Admin@12345'

    # 审计 dry-run 产物（不依赖平台）
    python scripts/verify_scene_semantics.py --from-file scripts/output/scenes.json

退出码：0 全部覆盖；1 存在必需角色缺失/不足/不符；2 参数、依赖或连接错误。
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import unicodedata
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "common" / "python"))
sys.path.insert(0, str(ROOT / "services" / "scene-service"))

from app.scene_engine.catalog import CATALOG_VERSION
from app.scene_engine.semantics import (
    audit_roles,
    check_scene_semantics,
    group_actors,
    list_semantics,
    semantics_for,
)

#: 角色问题码（出现即判失败）
ROLE_FAILURE_CODES = frozenset({"role_missing", "role_underflow", "role_type_mismatch", "role_behavior_mismatch"})
#: 表头（列宽按显示宽度计算，兼容中文）
HEADERS = ("场景类型", "抽样", "必需角色", "实际角色", "缺失", "多余", "语义违例")


@dataclass
class TypeAudit:
    """单场景类型的角色覆盖汇总。"""

    scene_type: str
    total: int = 0
    sampled: int = 0
    required: list[str] = field(default_factory=list)
    actual: dict[str, int] = field(default_factory=dict)
    missing: set[str] = field(default_factory=set)
    extras: set[str] = field(default_factory=set)
    problems: list[str] = field(default_factory=list)

    @property
    def failed(self) -> bool:
        return bool(self.missing or self.problems)


# =====================================================================
# 审计（API 与 dry-run 产物共用）
# =====================================================================
def _scene_label(record: dict[str, Any]) -> str:
    """场景可读标识（API 详情 / dry-run 记录两套字段）。"""
    return str(
        record.get("scene_name")
        or record.get("name")
        or record.get("scenario_id")
        or "<unknown>"
    )


def _audit_one(audit: TypeAudit, record: dict[str, Any]) -> None:
    """把单条场景的语义审计结果并入类型汇总。"""
    stype = audit.scene_type
    config = record.get("scene_config") or record.get("config") or {}
    label = _scene_label(record)
    for row in audit_roles(stype, config):
        if row.actual:
            audit.actual[row.role] = audit.actual.get(row.role, 0) + 1
    for problem in check_scene_semantics(stype, config):
        if problem.role is not None and problem.code in ROLE_FAILURE_CODES:
            audit.missing.add(problem.role)
        audit.problems.append(f"{label}: [{problem.code}] {problem.message}")
    semantics = semantics_for(stype)
    if semantics is not None:
        _, unmatched = group_actors(semantics, config.get("actors") or [])
        for actor in unmatched:
            behavior = str((actor.get("behavior") or {}).get("behavior_type") or "")
            audit.extras.add(f"{actor.get('type')}/{behavior}")


def _audit_records(records: list[dict[str, Any]], *, sample_per_type: int) -> dict[str, TypeAudit]:
    """按类型抽样审计；``sample_per_type<=0`` 表示全量。"""
    grouped: dict[str, list[dict[str, Any]]] = {}
    unknown: list[str] = []
    for record in records:
        stype = str(record.get("scene_type") or "")
        if not stype or semantics_for(stype) is None:
            unknown.append(_scene_label(record))
            continue
        grouped.setdefault(stype, []).append(record)
    if unknown:
        print(f"[warn] 跳过 {len(unknown)} 条无合成语义的场景（custom/实车回放等）："
              f"{', '.join(unknown[:5])}{' …' if len(unknown) > 5 else ''}", file=sys.stderr)

    audits: dict[str, TypeAudit] = {}
    for stype, items in grouped.items():
        semantics = semantics_for(stype)
        assert semantics is not None  # 前面已过滤
        audit = TypeAudit(
            scene_type=stype,
            total=len(items),
            required=[role.role for role in semantics.required_roles()],
        )
        picked = items if sample_per_type <= 0 else items[:sample_per_type]
        audit.sampled = len(picked)
        for record in picked:
            _audit_one(audit, record)
        audits[stype] = audit
    return audits


# =====================================================================
# 输出
# =====================================================================
def _width(text: str) -> int:
    return sum(2 if unicodedata.east_asian_width(ch) in "WF" else 1 for ch in text)


def _pad(text: str, width: int) -> str:
    return text + " " * max(0, width - _width(text))


def _role_cell(audit: TypeAudit) -> str:
    """实际角色列：``role×出现抽样数``；0 次即缺失。"""
    if not audit.actual:
        return "-"
    return ",".join(f"{role}×{count}" for role, count in sorted(audit.actual.items()))


def print_table(audits: dict[str, TypeAudit], *, only_failed: bool = False) -> None:
    widths = (28, 6, 32, 34, 24, 20, 8)
    line_width = sum(widths) + 2 * (len(widths) - 1)
    print("=" * line_width)
    print("  ".join(_pad(title, width) for title, width in zip(HEADERS, widths, strict=True)))
    print("-" * line_width)
    rows = sorted(audits.values(), key=lambda row: (not row.failed, row.scene_type))
    shown = 0
    for audit in rows:
        if only_failed and not audit.failed:
            continue
        shown += 1
        sample = f"{audit.sampled}/{audit.total}"
        missing = ",".join(sorted(audit.missing)) or "-"
        extras = ",".join(sorted(audit.extras)) or "-"
        cells = (
            audit.scene_type,
            sample,
            ",".join(audit.required) or "-",
            _role_cell(audit),
            missing,
            extras,
            str(len(audit.problems)),
        )
        print("  ".join(_pad(cell, width) for cell, width in zip(cells, widths, strict=True)))
    if shown == 0:
        print("（无匹配场景类型）")


def print_failures(audits: dict[str, TypeAudit]) -> None:
    failures = [audit for audit in audits.values() if audit.failed]
    if not failures:
        return
    print("\n=== 失败明细 ===")
    for audit in failures:
        print(f"[FAIL] {audit.scene_type}")
        if audit.missing:
            print(f"       缺失必需角色: {', '.join(sorted(audit.missing))}")
        for problem in audit.problems[:5]:
            print(f"       - {problem}")
        if len(audit.problems) > 5:
            print(f"       … 其余 {len(audit.problems) - 5} 条见 --json-out")


def write_json(path: str, audits: dict[str, TypeAudit]) -> None:
    payload = {
        "generated_at": uuid.uuid4().hex[:8],
        "types": {
            stype: {
                "total": audit.total,
                "sampled": audit.sampled,
                "required_roles": audit.required,
                "actual_roles": audit.actual,
                "missing_roles": sorted(audit.missing),
                "extra_actors": sorted(audit.extras),
                "problems": audit.problems,
                "failed": audit.failed,
            }
            for stype, audit in sorted(audits.items())
        },
    }
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[json] 已写出 {out}")


# =====================================================================
# 数据源一：dry-run 产物
# =====================================================================
def run_from_file(args: argparse.Namespace) -> int:
    path = Path(args.from_file)
    if not path.is_file():
        print(f"[error] 文件不存在: {path}", file=sys.stderr)
        return 2
    try:
        records = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"[error] 读取失败: {exc}", file=sys.stderr)
        return 2
    if not isinstance(records, list):
        print("[error] 产物必须是 JSON 数组", file=sys.stderr)
        return 2
    audits = _audit_records(records, sample_per_type=args.sample_per_type)
    print(f"\n=== 场景语义自检（dry-run 产物：{path}，共 {len(records)} 条）===")
    print_table(audits)
    print_failures(audits)
    if args.json_out:
        write_json(args.json_out, audits)
    return _exit_code(audits)


# =====================================================================
# 数据源二：平台 API
# =====================================================================
def _check_semantics_api(payload: dict[str, Any]) -> list[str]:
    """核对 API 语义目录与本地契约一致（防止服务加载了旧契约）。"""
    issues: list[str] = []
    items = payload.get("items") or []
    if payload.get("total") != len(items):
        issues.append(f"total={payload.get('total')} 与 items 数 {len(items)} 不一致")
    api_types = {str(item.get("scene_type")): item for item in items}
    local_types = {item["scene_type"]: item for item in list_semantics()}
    if set(api_types) != set(local_types):
        only_api = sorted(set(api_types) - set(local_types))
        only_local = sorted(set(local_types) - set(api_types))
        issues.append(f"语义类型集合不一致（API 独有 {only_api}，契约独有 {only_local}）")
    for stype, item in api_types.items():
        local = local_types.get(stype)
        if local is None:
            continue
        api_required = sorted(item.get("required_roles") or [])
        local_required = sorted(local.get("required_roles") or [])
        if api_required != local_required:
            issues.append(f"{stype}: API 必需角色 {api_required} ≠ 契约 {local_required}")
    return issues


async def _headers(client: Any, args: argparse.Namespace) -> dict[str, str]:
    """读取身份头（direct：X-User-Id/X-Roles；gateway：登录换 JWT）。"""
    if args.mode == "gateway":
        if not args.username or not args.password:
            raise SystemExit("[error] gateway 模式需要 --username/--password")
        resp = await client.post(
            "/api/v1/user/login",
            json={"username": args.username, "password": args.password},
        )
        body = _safe_json(resp)
        token = ((body or {}).get("data") or {}).get("access_token")
        if resp.status_code != 200 or not body or body.get("code") != 0 or not token:
            raise SystemExit(f"[error] 网关登录失败: HTTP {resp.status_code} {body}")
        return {"Authorization": f"Bearer {token}"}
    if args.hmac_secret:
        from hunter_common.internal_auth import build_identity_headers

        return build_identity_headers(
            args.hmac_secret, user_id=args.user_id, roles=args.roles, trace_id=str(uuid.uuid4())
        )
    return {"X-User-Id": args.user_id, "X-Roles": args.roles, "X-Trace-Id": str(uuid.uuid4())}


def _safe_json(resp: Any) -> dict[str, Any] | None:
    try:
        return resp.json()
    except Exception:  # noqa: BLE001
        return None


async def _get_data(client: Any, path: str, headers: dict[str, str], **params: Any) -> Any:
    resp = await client.get(path, headers=headers, params=params)
    body = _safe_json(resp)
    if resp.status_code != 200 or not body or body.get("code") != 0:
        raise RuntimeError(f"GET {path} 失败: HTTP {resp.status_code} {body}")
    return body.get("data")


async def _logical_types(client: Any, headers: dict[str, str]) -> dict[str, str]:
    data = await _get_data(
        client, "/api/v1/scene/logical", headers, include_catalog="true", limit=200
    )
    return {
        str(item["logical_id"]): str(item["scene_type"])
        for item in (data or {}).get("items") or []
        if item.get("logical_id") and item.get("scene_type")
    }


async def _list_concretes(client: Any, headers: dict[str, str], args: argparse.Namespace) -> list[dict[str, Any]]:
    """分页拉取具体场景摘要（可按 --only-type 对应的 logical_id 过滤）。"""
    items: list[dict[str, Any]] = []
    page = 1
    while True:
        data = await _get_data(
            client,
            "/api/v1/scene/concrete",
            headers,
            page=page,
            page_size=args.page_size,
        )
        batch = (data or {}).get("items") or []
        items.extend(batch)
        total = int((data or {}).get("total") or 0)
        if not batch or len(items) >= total:
            return items
        page += 1


async def _detail(client: Any, headers: dict[str, str], scenario_id: str) -> dict[str, Any] | None:
    try:
        return await _get_data(client, f"/api/v1/scene/concrete/{scenario_id}", headers)
    except RuntimeError as exc:
        print(f"[warn] 详情读取失败（跳过）: {exc}", file=sys.stderr)
        return None


async def run_api(args: argparse.Namespace) -> int:
    try:
        import httpx
    except ImportError:
        print("[error] 缺少 httpx：pip install httpx", file=sys.stderr)
        return 2

    async with httpx.AsyncClient(base_url=args.base_url, timeout=args.timeout) as client:
        try:
            headers = await _headers(client, args)
            semantics = await _get_data(client, "/api/v1/scene/semantics", headers)
        except SystemExit:
            raise
        except (RuntimeError, httpx.HTTPError) as exc:
            print(f"[error] 语义目录读取失败: {exc}", file=sys.stderr)
            return 2

        issues = _check_semantics_api(semantics or {})
        if issues:
            print("[warn] 服务端语义目录与本地契约不一致（可能未重启服务）：", file=sys.stderr)
            for issue in issues:
                print(f"  - {issue}", file=sys.stderr)

        try:
            logical_types = await _logical_types(client, headers)
            summaries = await _list_concretes(client, headers, args)
        except (RuntimeError, httpx.HTTPError) as exc:
            print(f"[error] 具体场景读取失败: {exc}", file=sys.stderr)
            return 2

        wanted = {item.strip() for item in args.only_type.split(",") if item.strip()}
        sampled_by_type: dict[str, int] = {}
        records: list[dict[str, Any]] = []
        skipped_unknown: set[str] = set()
        for summary in summaries:
            logical_id = str(summary.get("logical_id") or "")
            stype = logical_types.get(logical_id, "")
            if not stype:
                skipped_unknown.add(logical_id or "<unknown>")
                continue
            if wanted and stype not in wanted:
                continue
            if args.sample_per_type > 0 and sampled_by_type.get(stype, 0) >= args.sample_per_type:
                continue
            detail = await _detail(client, headers, str(summary.get("scenario_id")))
            if detail is None:
                continue
            detail.setdefault("scene_type", stype)
            detail.setdefault("scene_name", summary.get("name"))
            records.append(detail)
            sampled_by_type[stype] = sampled_by_type.get(stype, 0) + 1

        if skipped_unknown:
            print(
                f"[warn] 跳过 {len(skipped_unknown)} 个无法映射 scene_type 的逻辑场景："
                f"{', '.join(sorted(skipped_unknown)[:5])}",
                file=sys.stderr,
            )

        # total 以平台统计为准（抽样前），供表中「抽样/总数」列展示
        totals: dict[str, int] = {}
        for summary in summaries:
            stype = logical_types.get(str(summary.get("logical_id") or ""), "")
            if stype and (not wanted or stype in wanted):
                totals[stype] = totals.get(stype, 0) + 1

    audits = _audit_records(records, sample_per_type=0)  # 已按类型抽样
    for stype, audit in audits.items():
        audit.total = totals.get(stype, audit.total)
    print(f"\n=== 场景语义自检（平台 {args.base_url}，具体场景 {len(summaries)} 条）===")
    print_table(audits)
    print_failures(audits)
    if args.json_out:
        write_json(args.json_out, audits)
    if issues:
        return 1
    return _exit_code(audits)


def _exit_code(audits: dict[str, TypeAudit]) -> int:
    failed = sorted(stype for stype, audit in audits.items() if audit.failed)
    if failed:
        print(f"\n[fail] {len(failed)} 个场景类型存在语义问题: {', '.join(failed)}")
        return 1
    print(f"\n[ok] {len(audits)} 个场景类型的必需角色全部覆盖（契约 {CATALOG_VERSION}）")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="场景语义自检（必需角色 / 实际角色 / 缺失 / 多余）")
    parser.add_argument(
        "--from-file", default="", help="dry-run 产物 JSON 路径；省略则走平台 API"
    )
    parser.add_argument(
        "--base-url",
        default=os.getenv("HUNTER_BATCH_BASE_URL", "http://localhost:8081"),
        help="scene-service 地址（默认 http://localhost:8081）",
    )
    parser.add_argument("--mode", choices=("direct", "gateway"), default="direct")
    parser.add_argument(
        "--user-id",
        default=os.getenv("HUNTER_BATCH_USER_ID", "00000000-0000-0000-0000-000000000001"),
    )
    parser.add_argument("--roles", default=os.getenv("HUNTER_BATCH_ROLES", "admin"))
    parser.add_argument(
        "--hmac-secret", default=os.getenv("GATEWAY_HMAC_SECRET", ""), help="direct 模式下可选验签"
    )
    parser.add_argument("--username", default=os.getenv("HUNTER_BATCH_USERNAME", ""))
    parser.add_argument("--password", default=os.getenv("HUNTER_BATCH_PASSWORD", ""))
    parser.add_argument(
        "--sample-per-type", type=int, default=3, help="每类型抽样场景数（0=全量；默认 3）"
    )
    parser.add_argument("--only-type", default="", help="仅审计指定场景类型（逗号分隔）")
    parser.add_argument("--page-size", type=int, default=200, help="具体场景分页大小")
    parser.add_argument("--timeout", type=float, default=30.0, help="单请求超时秒数")
    parser.add_argument("--json-out", default="", help="把审计结果写出到 JSON")
    return parser


def main(argv: list[str] | None = None) -> int:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            reconfigure(encoding="utf-8", errors="replace")
    args = build_parser().parse_args(argv)
    if args.from_file:
        return run_from_file(args)
    return asyncio.run(run_api(args))


if __name__ == "__main__":
    raise SystemExit(main())
