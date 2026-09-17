"""测试报告生成：把 L5 测试结果汇总为可归档的 Markdown 报告。

- 模板：``tests/report-template.md``（随仓库追踪，占位符 ``{{KEY}}`` 由本模块填充）
- 产物：``docs/test-reports/test-report-<version>-<date>.md``（docs/ 按开发规则不入库）
- 性能记录带阈值与方向（max/min），自动判定 PASS/FAIL，禁止人工改判

用法（本地/CI 均可）::

    python -m tests.support.report --stdout
"""
from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import os
import platform
import sys
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tests.support import thresholds

ROOT = Path(__file__).resolve().parents[2]
TEMPLATE_PATH = ROOT / "tests" / "report-template.md"
REPORT_DIR = ROOT / "docs" / "test-reports"
#: 结果 JSON（供 CI artifact 与报告生成复用）
RESULT_JSON_NAME = "l5-results.json"

#: 与 pytest.ini 的 markers 一一对应（scripts/verify_test_layer.py 会校验一致性）
MARKERS: dict[str, str] = {
    "unit": "纯单元测试：不依赖容器/网络（默认运行）",
    "integration": "集成测试：需要 Docker 容器或已启动的中间件",
    "e2e": "端到端业务流程测试（11.1 数据采集 / 11.2 OTA / 11.3 远程操控）",
    "performance": "性能基准与阈值断言（对照第 10 条性能指标）",
    "cw": "契约：文档约定值（车端/云端字段、阈值、状态名）一致性校验",
}

#: 三档测试套件（与 CI stage 对应）
SUITES: tuple[str, ...] = ("unit", "integration", "e2e", "performance")

#: 用例号标记：`@pytest.mark.l5_case("11.1")`，用于报告归档与文档章节对齐
CASE_MARKER: str = "l5_case"
#: 非套件类标记（与套件标记分开注册，避免污染套件判定）
EXTRA_MARKERS: dict[str, str] = {
    CASE_MARKER: "用例号（如 11.1/11.2/11.3），供测试报告归档与设计文档章节对齐",
}


@dataclass
class ReportEntry:
    """单条用例结果。"""

    suite: str
    case_id: str
    name: str
    status: str  # pass / fail / skip
    duration_ms: float = 0.0
    detail: str = ""
    contract_refs: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """序列化为 JSON 友好字典。"""
        return dataclasses.asdict(self)


@dataclass
class PerformanceRecord:
    """性能指标记录：值 + 阈值 + 方向，自动判定。"""

    metric: str
    value: float
    unit: str
    threshold: float
    direction: str  # max: value <= threshold 通过；min: value >= threshold 通过
    source: str = ""
    detail: str = ""

    @property
    def passed(self) -> bool:
        """按方向判定是否满足阈值。"""
        if self.direction == "max":
            return self.value <= self.threshold
        if self.direction == "min":
            return self.value >= self.threshold
        raise ValueError(f"未知阈值方向: {self.direction}")

    def to_dict(self) -> dict[str, Any]:
        """序列化（含判定结果）。"""
        payload = dataclasses.asdict(self)
        payload["passed"] = self.passed
        return payload



@dataclass
class TestReport:
    """一次完整 L5 测试运行的报告聚合器。"""

    version: str = os.getenv("HUNTER_REPORT_VERSION", "0.1.0")
    environment: str = os.getenv("ENVIRONMENT", "dev")
    entries: list[ReportEntry] = field(default_factory=list)
    perf_records: list[PerformanceRecord] = field(default_factory=list)
    generated_at: dt.datetime = field(default_factory=lambda: dt.datetime.now(dt.UTC))

    # ---------------------------------------------------------- 采集
    def add_entry(
        self,
        suite: str,
        case_id: str,
        name: str,
        status: str,
        duration_ms: float = 0.0,
        detail: str = "",
        contract_refs: Iterable[str] = (),
    ) -> ReportEntry:
        """登记一条用例结果（suite 必须属于 ``SUITES``）。"""
        if suite not in SUITES:
            raise ValueError(f"未知测试套件: {suite}（允许值：{SUITES}）")
        if status not in {"pass", "fail", "skip"}:
            raise ValueError(f"未知状态: {status}")
        entry = ReportEntry(
            suite=suite,
            case_id=case_id,
            name=name,
            status=status,
            duration_ms=round(duration_ms, 2),
            detail=detail,
            contract_refs=tuple(contract_refs),
        )
        self.entries.append(entry)
        return entry

    def add_perf(
        self,
        metric: str,
        value: float,
        unit: str,
        threshold: float,
        direction: str,
        source: str = "",
        detail: str = "",
    ) -> PerformanceRecord:
        """登记一条性能指标（含阈值与来源说明）。"""
        record = PerformanceRecord(
            metric=metric, value=round(value, 3), unit=unit,
            threshold=threshold, direction=direction, source=source, detail=detail,
        )
        self.perf_records.append(record)
        return record

    # ---------------------------------------------------------- 统计
    def counts(self, suite: str | None = None) -> dict[str, int]:
        """按套件统计 pass/fail/skip。"""
        scoped = [e for e in self.entries if suite is None or e.suite == suite]
        return {
            "total": len(scoped),
            "pass": sum(e.status == "pass" for e in scoped),
            "fail": sum(e.status == "fail" for e in scoped),
            "skip": sum(e.status == "skip" for e in scoped),
        }

    @property
    def perf_failed(self) -> list[PerformanceRecord]:
        """不满足阈值的性能指标（报告中标红）。"""
        return [r for r in self.perf_records if not r.passed]

    @property
    def exit_code(self) -> int:
        """结论：存在失败用例或未达标性能指标 -> 1。"""
        any_case_failed = any(e.status == "fail" for e in self.entries)
        return 1 if any_case_failed or self.perf_failed else 0

    # ---------------------------------------------------------- 渲染
    def to_dict(self) -> dict[str, Any]:
        """序列化为 JSON（CI artifact）。"""
        return {
            "version": self.version,
            "environment": self.environment,
            "generated_at": self.generated_at.isoformat(),
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "summary": {suite: self.counts(suite) for suite in SUITES},
            "counts": self.counts(),
            "entries": [e.to_dict() for e in self.entries],
            "performance": [r.to_dict() for r in self.perf_records],
            "performance_failed": len(self.perf_failed),
            "exit_code": self.exit_code,
        }

    def to_markdown(self) -> str:
        """按 ``tests/report-template.md`` 渲染 Markdown 报告。"""
        template = TEMPLATE_PATH.read_text(encoding="utf-8")
        replacements = {
            "{{VERSION}}": self.version,
            "{{DATE}}": self.generated_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "{{ENVIRONMENT}}": self.environment,
            "{{PYTHON}}": sys.version.split()[0],
            "{{PLATFORM}}": platform.platform(),
            "{{SUMMARY_TABLE}}": self._summary_table(),
            "{{CASE_TABLE}}": self._case_table(),
            "{{PERF_TABLE}}": self._perf_table(),
            "{{CONCLUSION}}": "通过" if self.exit_code == 0 else "未通过（存在失败用例或未达标指标）",
            "{{PERF_THRESHOLD_SOURCES}}": self._threshold_sources(),
        }
        for key, value in replacements.items():
            template = template.replace(key, value)
        return template

    def _summary_table(self) -> str:
        """套件汇总表。"""
        rows = ["| 套件 | 用例数 | 通过 | 失败 | 跳过 |", "| --- | --- | --- | --- | --- |"]
        for suite in SUITES:
            stat = self.counts(suite)
            rows.append(
                f"| {suite} | {stat['total']} | {stat['pass']} | {stat['fail']} | {stat['skip']} |"
            )
        total = self.counts()
        rows.append(
            f"| **合计** | {total['total']} | {total['pass']} | {total['fail']} | {total['skip']} |"
        )
        return "\n".join(rows)

    def _case_table(self) -> str:
        """用例明细表（按套件/用例号排序）。"""
        rows = [
            "| 套件 | 用例号 | 用例 | 结果 | 耗时(ms) | 契约引用 |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
        for entry in sorted(self.entries, key=lambda e: (e.suite, e.case_id, e.name)):
            refs = "<br>".join(entry.contract_refs) or "-"
            rows.append(
                f"| {entry.suite} | {entry.case_id} | {entry.name} | {entry.status} "
                f"| {entry.duration_ms} | {refs} |"
            )
        return "\n".join(rows)

    def _perf_table(self) -> str:
        """性能指标表（对照系统关键约束第 10 条）。"""
        rows = [
            "| 指标 | 实测 | 阈值 | 方向 | 判定 | 来源 |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
        for record in self.perf_records:
            verdict = "PASS" if record.passed else "FAIL"
            rows.append(
                f"| {record.metric} | {record.value} {record.unit} | {record.threshold} "
                f"| {record.direction} | {verdict} | {record.source or '-'} |"
            )
        return "\n".join(rows) if len(rows) > 2 else "（本次运行未采集性能指标）"

    def _threshold_sources(self) -> str:
        """阈值来源登记表（与 thresholds.SOURCES 同步，防漂移）。"""
        expected: dict[str, float] = {
            "API_P95_MAX_MS": thresholds.API_P95_MAX_MS,
            "TELEMETRY_INGEST_LATENCY_MAX_S": thresholds.TELEMETRY_INGEST_LATENCY_MAX_S,
            "ALERT_TRIGGER_LATENCY_MAX_S": thresholds.ALERT_TRIGGER_LATENCY_MAX_S,
            "RC_VIDEO_E2E_LATENCY_MAX_MS": thresholds.RC_VIDEO_E2E_LATENCY_MAX_MS,
            "RC_COMMAND_LATENCY_MAX_MS": thresholds.RC_COMMAND_LATENCY_MAX_MS,
            "TIMESERIES_WRITE_MIN_POINTS_PER_S": thresholds.TIMESERIES_WRITE_MIN_POINTS_PER_S,
        }
        rows = ["| 常量 | 值 | 来源 |", "| --- | --- | --- |"]
        for name, value in expected.items():
            rows.append(f"| {name} | {value} | {thresholds.SOURCES.get(name, '-')} |")
        return "\n".join(rows)

    # ---------------------------------------------------------- 输出
    def write(self, directory: Path | None = None) -> tuple[Path, Path]:
        """写出 Markdown 报告与 JSON 结果，返回两个路径。"""
        target_dir = directory or REPORT_DIR
        target_dir.mkdir(parents=True, exist_ok=True)
        stamp = self.generated_at.strftime("%Y%m%d-%H%M%S")
        md_path = target_dir / f"test-report-{self.version}-{stamp}.md"
        json_path = target_dir / RESULT_JSON_NAME
        md_path.write_text(self.to_markdown(), encoding="utf-8")
        json_path.write_text(
            json.dumps(self.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return md_path, json_path


def main(argv: list[str] | None = None) -> int:
    """CLI：生成报告骨架（``--stdout`` 打印 Markdown，否则写入 docs/test-reports）。"""
    parser = argparse.ArgumentParser(description="生成 HunterEdge L5 测试报告")
    parser.add_argument("--stdout", action="store_true", help="仅打印 Markdown，不落盘")
    args = parser.parse_args(argv)
    report = TestReport()
    if args.stdout:
        print(report.to_markdown())
        return 0
    md_path, json_path = report.write()
    print(f"报告已生成：{md_path}")
    print(f"结果 JSON：{json_path}")
    return report.exit_code


if __name__ == "__main__":  # pragma: no cover - CLI 入口
    sys.exit(main())

