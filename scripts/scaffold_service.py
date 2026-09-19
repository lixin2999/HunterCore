#!/usr/bin/env python3
"""HunterCore 微服务骨架生成器（L0）。

为 6 个微服务生成统一骨架：FastAPI 入口（/healthz /readyz + trace_id 中间件）、
pydantic-settings 配置、分层空目录（routers/schemas/models/services/repositories/
consumers/producers/core/tests）、全局异常处理器、Dockerfile、pyproject.toml、README。

用法（仓库根目录）::

    python scripts/scaffold_service.py            # 生成全部服务骨架（幂等，覆盖生成物）
    python scripts/scaffold_service.py scene-service

约束：模板为全部服务统一基线（scripts/_templates_a|b|c.py），业务逻辑按契约在各服务内开发。
"""
from __future__ import annotations

import sys
from pathlib import Path

# 支持 `python scripts/scaffold_service.py` 直接执行（脚本目录加入导入路径）
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _templates_a import (
    CONFIG_TMPL,
    DOCKERFILE_TMPL,
    INIT_TMPL,
    PYPROJECT_TMPL,
    TEST_HEALTH_TMPL,
)
from _templates_b import MAIN_TMPL, README_TMPL
from _templates_c import ERROR_HANDLERS_TMPL, HEALTH_ROUTER_TMPL

ROOT = Path(__file__).resolve().parents[1]

#: 服务名 -> (端口, 模块描述)（端口见设计文档模块划分，不可更改）
SERVICES: dict[str, tuple[int, str]] = {
    "api-gateway": (8080, "API 网关：统一接入、JWT 认证鉴权、五级限流熔断、路由转发、日志审计"),
    "scene-service": (8081, "场景生成：场景库管理、场景编辑、场景参数化、OpenSCENARIO 导出、实车场景自动提取"),
    "data-collector": (8082, "数据采集：Kafka 消息消费接入、数据预处理、数据路由、文件上传管理"),
    "data-analytics": (8083, "数据分析：Flink 实时流处理、Spark 离线批处理、指标计算、Corner Case 挖掘、报告生成"),
    "ota-service": (8084, "OTA 管理：版本仓库管理、升级任务调度、灰度发布、升级监控、A/B 分区回滚"),
    "remote-control": (8085, "远程操控：WebRTC 视频流转发、控制指令转发（20Hz）、操作员权限管理、操控会话与录像记录"),
}

#: 需要生成 __init__.py 的分层包（每个微服务内部结构固定，见开发规则）
LAYER_PACKAGES = (
    "routers", "schemas", "models", "services",
    "repositories", "consumers", "producers", "core", "tests",
)


def generate_service(service: str, port: int, desc: str) -> list[Path]:
    """生成单个服务骨架（已存在的生成物会被覆盖，保证骨架与模板一致）。"""
    ctx = {"service": service, "port": str(port), "desc": desc}
    base = ROOT / "services" / service
    written: list[Path] = []

    def write(rel: str, content: str) -> None:
        path = base / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8", newline="\n")
        written.append(path)

    # 应用包 + 分层空包
    write("app/__init__.py", INIT_TMPL.substitute(desc=f"{service} 应用包"))
    for pkg in LAYER_PACKAGES:
        write(f"app/{pkg}/__init__.py", INIT_TMPL.substitute(desc=f"{service} app.{pkg} 包"))

    # 核心文件
    write("app/config.py", CONFIG_TMPL.substitute(**ctx))
    write("app/core/error_handlers.py", ERROR_HANDLERS_TMPL.substitute())
    write("app/routers/health.py", HEALTH_ROUTER_TMPL.substitute())
    write("app/main.py", MAIN_TMPL.substitute(**ctx))
    write("app/tests/test_health.py", TEST_HEALTH_TMPL.substitute(**ctx))

    # 工程文件
    write("Dockerfile", DOCKERFILE_TMPL.substitute(**ctx))
    write("pyproject.toml", PYPROJECT_TMPL.substitute(**ctx))
    write("README.md", README_TMPL.substitute(**ctx))
    return written


def main() -> None:
    targets = sys.argv[1:] or list(SERVICES)
    unknown = [t for t in targets if t not in SERVICES]
    if unknown:
        raise SystemExit(f"未知服务: {unknown}，可选: {list(SERVICES)}")
    total = 0
    for name in targets:
        port, desc = SERVICES[name]
        files = generate_service(name, port, desc)
        total += len(files)
        print(f"[scaffold] {name:<16} {len(files)} files (port={port})")
    print(f"[scaffold] done: {total} files across {len(targets)} services.")
    _auto_ruff_fix()


def _auto_ruff_fix() -> None:
    """生成后自动执行 ruff check --fix（模板产出的 import 排序等机械问题）。"""
    import subprocess

    targets = ["common/python/hunter_common", "common/python/tests", "scripts", "services"]
    try:
        result = subprocess.run(
            [sys.executable, "-m", "ruff", "check", "--fix", *targets],
            check=False,
            capture_output=True,
            text=True,
            timeout=120,
        )
        tail = (result.stdout or result.stderr).strip().splitlines()
        print(f"[scaffold] ruff --fix: {tail[-1] if tail else 'no output'}")
    except FileNotFoundError:
        print("[scaffold] ruff 不可用，请手动运行: ruff check --fix common services scripts")
    except subprocess.TimeoutExpired:
        print("[scaffold] ruff --fix 超时，请手动运行")


if __name__ == "__main__":
    main()
