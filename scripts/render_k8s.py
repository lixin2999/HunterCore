#!/usr/bin/env python3
"""HunterEdge K8s 清单版本渲染（`{version}` 占位符 → 具体镜像标签）。

设计意图（部署约束）：
  仓库内所有 `infra/k8s/**` 清单的**自有镜像**（`hunter/<service>`）一律写成
  `{version}` 占位符，禁止硬编码具体版本号 —— 版本是部署期变量（CI 用
  `$CI_COMMIT_TAG`，手工部署用发布版本号），单一来源、可审计。

  第三方镜像（timescaledb / redis / minio / kafka / prometheus ...）使用各自
  真实版本标签，不参与渲染。

渲染范围：`infra/k8s/` 全树（base / services / statefulsets / jobs /
  networkpolicies / autoscaling / disruption / ingress.yaml），保持目录结构输出。

用法：
  # 渲染到 build/k8s（默认输出目录，已被 .gitignore 忽略）
  python scripts/render_k8s.py --version 0.1.0

  # 渲染到指定目录后部署（部署顺序见 infra/k8s/README.md）
  python scripts/render_k8s.py --version "$CI_COMMIT_TAG" --out build/k8s
  kubectl apply -f build/k8s/base/

  # 仅校验占位符（CI lint；不写文件）：所有 hunter/* 镜像必须是 {version}
  python scripts/render_k8s.py --check

退出码：0 成功；1 校验/渲染失败（版本非法、占位符缺失、输出目录越界等）
"""
from __future__ import annotations

import argparse
import re
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "infra" / "k8s"
DEFAULT_OUT_DIR = ROOT / "build" / "k8s"

#: 版本变量占位符（清单中唯一允许的镜像版本写法）
PLACEHOLDER = "{version}"
#: 版本号白名单：语义化版本，可选 v 前缀 / 预发布后缀（禁止 latest / SHA 之外的注入）
VERSION_PATTERN = re.compile(r"^v?\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$")
#: 自有镜像行（`hunter/<service>:<tag>`，可带引号）
OWN_IMAGE_PATTERN = re.compile(r"(?P<name>hunter/[a-z0-9-]+):(?P<tag>[^\"'\s]+)")
#: 期望渲染的微服务镜像（与设计文档模块表一致，禁止增减）
EXPECTED_IMAGES: tuple[str, ...] = (
    "hunter/api-gateway",
    "hunter/scene-service",
    "hunter/data-collector",
    "hunter/data-analytics",
    "hunter/ota-service",
    "hunter/remote-control",
)


def rel(path: Path) -> str:
    """返回相对仓库根目录的 POSIX 风格路径（日志可读性）。"""
    return str(path.relative_to(ROOT)).replace("\\", "/")


def iter_manifests() -> list[Path]:
    """列出 `infra/k8s/` 下全部 YAML 清单（保持稳定顺序）。

    排除 `*.example.yaml`（Secret 模板）：模板含 `REPLACE_WITH_*` 占位值，
    禁止进入部署目录被误 apply（真实 Secret 由 CI/Vault/kubectl create 生成）。
    """
    return sorted(
        path for path in SRC_DIR.rglob("*.yaml") if not path.name.endswith(".example.yaml")
    )


def read_yaml_files(paths: list[Path]) -> dict[Path, str]:
    """读取清单文本（渲染为纯文本替换，保持注释与格式不变）。"""
    return {path: path.read_text(encoding="utf-8") for path in paths}


def validate_version(version: str) -> None:
    """校验版本号合法性（防注入 + 禁止 latest）。"""
    if version == "latest":
        raise ValueError("镜像版本禁止使用 latest（部署约束：必须显式版本）")
    if not VERSION_PATTERN.match(version):
        raise ValueError(
            f"版本号非法: {version!r}（允许格式：语义化版本，可带 v 前缀，如 0.1.0 / v1.2.3-rc1）"
        )


def check_placeholders(contents: dict[Path, str]) -> list[str]:
    """校验全部自有镜像均使用 `{version}` 占位符，返回问题描述列表。"""
    problems: list[str] = []
    seen: set[str] = set()
    for path, text in contents.items():
        for match in OWN_IMAGE_PATTERN.finditer(text):
            name, tag = match.group("name"), match.group("tag")
            if name not in EXPECTED_IMAGES:
                problems.append(f"{rel(path)}: 未登记的自有镜像 {name}（模块表不可自行新增）")
                continue
            seen.add(name)
            if tag != PLACEHOLDER:
                problems.append(f"{rel(path)}: {name} 镜像标签必须为 {PLACEHOLDER}，实际 {tag!r}")
    missing = sorted(set(EXPECTED_IMAGES) - seen)
    if missing:
        problems.append(f"以下微服务镜像未在清单中声明: {missing}")
    return problems


def render_text(text: str, version: str) -> tuple[str, int]:
    """把 `hunter/<service>:{version}` 替换为具体版本，返回 (新文本, 替换次数)。"""
    rendered, count = OWN_IMAGE_PATTERN.subn(
        lambda m: f"{m.group('name')}:{version}" if m.group("tag") == PLACEHOLDER else m.group(0),
        text,
    )
    return rendered, count


def render(version: str, out_dir: Path) -> int:
    """渲染全部清单到 out_dir（保持目录结构），返回替换总次数。"""
    out_root = out_dir.resolve()
    # 安全边界：禁止把输出写到 infra/k8s 自身（避免覆盖单一事实来源清单）
    if out_root == SRC_DIR.resolve() or SRC_DIR.resolve() in out_root.parents:
        raise ValueError(f"输出目录不得位于 {rel(SRC_DIR)} 之内: {out_dir}")

    contents = read_yaml_files(iter_manifests())
    problems = check_placeholders(contents)
    if problems:
        raise ValueError("清单占位符校验失败：\n  - " + "\n  - ".join(problems))

    if out_root.exists():
        shutil.rmtree(out_root)

    total = 0
    for path, text in contents.items():
        rendered, count = render_text(text, version)
        total += count
        target = out_root / path.relative_to(SRC_DIR)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(rendered, encoding="utf-8")
        print(f"[RENDER] {rel(path)} -> {target.name}（替换 {count} 处）")
    return total


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="K8s 清单 {version} 占位符渲染")
    parser.add_argument("--version", help="镜像版本（如 0.1.0 / v1.2.0-rc1），与 docker build 标签同源")
    parser.add_argument("--out", default=str(DEFAULT_OUT_DIR), help=f"输出目录（默认 {rel(DEFAULT_OUT_DIR)}）")
    parser.add_argument("--check", action="store_true", help="仅校验占位符，不渲染文件")
    args = parser.parse_args(argv)

    contents = read_yaml_files(iter_manifests())
    if not contents:
        print(f"[FAIL] 未在 {rel(SRC_DIR)} 找到 K8s 清单")
        return 1

    problems = check_placeholders(contents)
    if problems:
        for item in problems:
            print(f"[FAIL] {item}")
        return 1
    print(f"[PASS] 占位符校验通过（{len(EXPECTED_IMAGES)} 个微服务镜像均使用 {PLACEHOLDER}）")

    if args.check:
        return 0

    if not args.version:
        print("[FAIL] 渲染模式必须提供 --version（或使用 --check 仅校验）")
        return 1
    try:
        validate_version(args.version)
        total = render(args.version, Path(args.out))
    except ValueError as exc:
        print(f"[FAIL] {exc}")
        return 1
    print(f"[PASS] 渲染完成：{len(contents)} 个清单，镜像标签替换 {total} 处，版本 {args.version}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
