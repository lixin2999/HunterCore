"""L5 测试层自检脚本：防止测试层与 pytest.ini / 契约阈值 / 报告模板漂移。

检查项（任一失败退出码 1，CI 阻断合并）：
1. 标记一致性：``pytest.ini`` 的 ``markers`` 与 ``tests/support/report.py::MARKERS`` 一致
2. 阈值登记完整性：``thresholds.SOURCES`` 每个 key 在模块中存在同名常量（防登记空转）
3. 报告模板对齐：``tests/report-template.md`` 占位符与 ``report.py`` 渲染替换键一一对应
4. 用例号规范：``tests/{integration,e2e,performance}`` 中 ``l5_case(...)`` 取值合法
   （11.1 数据上行 / 11.2 OTA / 11.3 远程操控 / PERF 性能基准）

用法：``python scripts/verify_test_layer.py``
"""
from __future__ import annotations

import configparser
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from tests.support import report as report_module
from tests.support import thresholds

#: 用例号白名单（业务链路 11.1/11.2/11.3 + 性能基准 PERF）
VALID_CASE_IDS = {"11.1", "11.2", "11.3", "PERF"}

PYTEST_INI = REPO_ROOT / "pytest.ini"
TEMPLATE = REPO_ROOT / "tests" / "report-template.md"
L5_TEST_DIRS = ("tests/integration", "tests/e2e", "tests/performance")

CASE_ID_PATTERN = re.compile(r'l5_case\(\s*["\']([^"\']+)["\']\s*\)')


def check_markers() -> list[str]:
    """检查 1：pytest.ini markers 与 report.MARKERS 一致（双向）。"""
    parser = configparser.ConfigParser()
    parser.read(PYTEST_INI, encoding="utf-8")
    declared: dict[str, str] = {}
    for line in parser.get("pytest", "markers", fallback="").splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue
        name, description = line.split(":", 1)
        declared[name.strip()] = description.strip()
    # report.py 为单一事实来源：套件标记（MARKERS）+ 用例号标记（EXTRA_MARKERS）全量比对
    expected = {**report_module.MARKERS, **report_module.EXTRA_MARKERS}
    problems: list[str] = []
    for name, description in expected.items():
        if name not in declared:
            problems.append(f"pytest.ini 缺少标记: {name}")
        elif declared[name] != description:
            problems.append(f"标记 {name} 描述与 report.MARKERS 不一致")
    for name in declared:
        if name not in expected:
            problems.append(f"pytest.ini 多出标记: {name}（report.MARKERS 未登记）")
    return problems


def check_threshold_sources() -> list[str]:
    """检查 2：SOURCES 登记的每个常量名必须真实存在（防登记空转）。"""
    problems: list[str] = []
    for name in thresholds.SOURCES:
        if not hasattr(thresholds, name):
            problems.append(f"thresholds.SOURCES 引用了不存在的常量: {name}")
    for name in ("API_P95_MAX_MS", "TIMESERIES_WRITE_MIN_POINTS_PER_S"):
        if name not in thresholds.SOURCES:
            problems.append(f"thresholds.SOURCES 缺少关键阈值登记: {name}")
    return problems


def check_template() -> list[str]:
    """检查 3：报告模板占位符与 report.py 渲染替换键一一对应。"""
    template_text = TEMPLATE.read_text(encoding="utf-8")
    placeholders = set(re.findall(r"\{\{[A-Z_]+\}\}", template_text))
    known = {
        "{{VERSION}}", "{{DATE}}", "{{ENVIRONMENT}}", "{{PYTHON}}", "{{PLATFORM}}",
        "{{SUMMARY_TABLE}}", "{{CASE_TABLE}}", "{{PERF_TABLE}}",
        "{{CONCLUSION}}", "{{PERF_THRESHOLD_SOURCES}}",
    }
    problems: list[str] = []
    unknown = placeholders - known
    if unknown:
        problems.append(f"模板含未知占位符（report.py 不渲染）: {sorted(unknown)}")
    missing = known - placeholders
    if missing:
        problems.append(f"模板缺少必要占位符: {sorted(missing)}")
    return problems


def check_case_ids() -> list[str]:
    """检查 4：l5_case 用例号取值必须属于白名单。"""
    problems: list[str] = []
    for directory in L5_TEST_DIRS:
        for path in sorted((REPO_ROOT / directory).rglob("test_*.py")):
            for match in CASE_ID_PATTERN.finditer(path.read_text(encoding="utf-8")):
                case_id = match.group(1)
                if case_id not in VALID_CASE_IDS:
                    problems.append(f"{path.relative_to(REPO_ROOT)}: 非法用例号 {case_id}")
    return problems


def main() -> int:
    """依次执行全部检查，输出报告并返回退出码。"""
    failures = 0
    for title, problems in (
        ("标记一致性", check_markers()),
        ("阈值登记", check_threshold_sources()),
        ("报告模板", check_template()),
        ("用例号", check_case_ids()),
    ):
        if problems:
            failures += len(problems)
            print(f"[FAIL] {title}:")
            for problem in problems:
                print(f"       - {problem}")
        else:
            print(f"[ OK ] {title}")
    if failures:
        print(f"\n自检未通过：{failures} 项问题")
        return 1
    print("\nL5 测试层自检通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
