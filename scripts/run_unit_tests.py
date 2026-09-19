#!/usr/bin/env python3
"""Monorepo 单元测试聚合运行器（每个测试根目录一个独立 pytest 进程）。

背景（代码审查 R6）：6 个服务共用顶层包名 ``app``，在**同一个** pytest 进程里收集多个服务测试目录会失败：
- ≥2 个服务 → ``ModuleNotFoundError: No module named 'app.schemas.events'``（``sys.modules['app']`` 被另一个服务占用）
- ≥3 个服务 → ``ValueError: Plugin already registered under a different name``（``app.tests.conftest`` 重名）

因此本项目约定：**测试根目录逐个运行**。本脚本是该约定的唯一入口，CI（unit 阶段）与本地开发都用它，
避免"为了跑全量回归而手工逐目录执行"。
彻底修复（各服务改用唯一顶层包名）需独立变更，见 ``release.md`` 待确认项。

用法::

    python scripts/run_unit_tests.py                    # 全部目标（不含容器/e2e/性能用例）
    python scripts/run_unit_tests.py common/python      # 只跑指定目标（可多个）

退出码：0 全部通过；1 存在失败目标或目标不存在。
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
#: 单元测试目标（每项一个独立 pytest 进程；services 下必须逐个服务运行）
UNIT_TARGETS: tuple[str, ...] = (
    "common/python",
    "tests",
    "services/api-gateway",
    "services/scene-service",
    "services/data-collector",
    "services/data-analytics",
    "services/ota-service",
    "services/remote-control",
)
#: 单元阶段排除的标记（容器集成 / e2e / 性能用例由各自 CI 阶段运行）
DEFAULT_MARK_EXPR = "not integration and not e2e and not performance"
#: 目标 → 标记表达式覆盖（L5 测试层按 ``unit`` 套件运行，其余用例由 integration/e2e/performance 阶段跑）
MARK_EXPR_BY_TARGET: dict[str, str] = {"tests": "unit"}


def run_target(target: str) -> bool:
    """在独立进程中运行一个目标的单元测试；返回是否通过。

    pytest 退出码 5 = 未收集到用例（该目标在当前标记下无用例）——L5 ``tests/`` 层只含
    integration/e2e/performance 用例，由各自阶段运行，此处视为通过（仅做 import/收集烟测）。
    """
    mark_expr = MARK_EXPR_BY_TARGET.get(target, DEFAULT_MARK_EXPR)
    command = [sys.executable, "-m", "pytest", target, "-m", mark_expr, "-q"]
    print(f"\n=== {target} | {' '.join(command[2:])} ===", flush=True)
    returncode = subprocess.run(command, cwd=ROOT, check=False).returncode
    if returncode == 5:
        print(f"[跳过] {target}: 当前标记下无单元用例（容器/e2e/性能用例由其专属阶段运行）")
        return True
    return returncode == 0


def main(argv: list[str]) -> int:
    targets = tuple(argv[1:]) or UNIT_TARGETS
    unknown = [target for target in targets if not (ROOT / target).exists()]
    if unknown:
        print(f"未知测试目标：{unknown}")
        return 1

    failed = [target for target in targets if not run_target(target)]

    print("\n" + "=" * 72)
    if failed:
        print(f"失败目标：{failed}")
        return 1
    print(f"全部通过（{len(targets)} 个目标，各自独立进程）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
