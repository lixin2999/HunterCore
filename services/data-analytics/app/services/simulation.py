"""仿真评估报告（scene_test）：sim_metrics → hunter-reports。

按 CARLA 接入方案 7.4：
- 报告对象：``scene_test/{yyyy-mm-dd}/{report_id}.json|html``；
- sidecar：``scene_test/{yyyy-mm-dd}/{report_id}.meta.json``（同时保留
  ``reports/scene_test/{report_id}.json`` 供现有报告列表/详情接口复用）；
- 指针：``eval/simulation/latest.json``（看板读取最新仿真评估）。

指标字段兼容两种命名：设计文档 6.5/7.4 的 ``collisions``/``lane_invasions`` 与
CARLA 回传示例的 ``collision_count``/``lane_invasion_count``（V1.23.4）。
"""
from __future__ import annotations

import html
import json
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from hunter_common.logging import get_logger

from app.repositories.reports import ReportRepository
from app.repositories.storage import ObjectStorage
from app.schemas.reports import ReportFormat, ReportStatus, ReportType

logger = get_logger("app.services.simulation")

#: 仿真报告 ID 命名空间（sim_id 决定稳定 UUID → 重放覆盖同一条报告，不产生重复）
_REPORT_NAMESPACE = uuid.UUID("6f1f8b1e-2f2b-4d7a-9b7c-4e6f8d9a0b1c")

_TERMINAL_STATUSES = frozenset({"COMPLETED", "SUCCEEDED", "FAILED", "CANCELED", "CANCELLED"})

#: 成功终态（用于缺省 passed 判定）
_SUCCESS_STATUSES = frozenset({"COMPLETED", "SUCCEEDED"})


def is_terminal_status(status: Any) -> bool:
    """CARLA 终态判定（大小写不敏感）。"""
    return str(status or "").strip().upper() in _TERMINAL_STATUSES


def _as_float(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _pick(source: dict[str, Any], *keys: str) -> float | None:
    """按别名顺序取第一个数值字段（兼容设计/实现两种命名）。"""
    for key in keys:
        value = _as_float(source.get(key))
        if value is not None:
            return value
    return None


def _as_epoch(value: Any) -> float | None:
    """把 Unix 秒或 ISO-8601 时间转成 epoch 秒。"""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip())
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.timestamp()
    return None


def summarize_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    """从 sim_metrics 载荷提取看板/报告摘要（缺失指标保持 None）。"""
    safety = metrics.get("safety") if isinstance(metrics.get("safety"), dict) else {}
    efficiency = metrics.get("efficiency") if isinstance(metrics.get("efficiency"), dict) else {}
    comfort = metrics.get("comfort") if isinstance(metrics.get("comfort"), dict) else {}
    task = metrics.get("task") if isinstance(metrics.get("task"), dict) else {}
    success = metrics.get("success_criteria_result")
    if not isinstance(success, dict):
        success = {}
    summary = {
        "safety": {
            # 设计 6.5/7.4 用复数；CARLA 示例用 *_count。两者都接受。
            "collisions": _pick(safety, "collisions", "collision_count"),
            "min_ttc": _pick(safety, "min_ttc"),
            "min_distance": _pick(safety, "min_distance"),
            "emergency_brakes": _pick(safety, "emergency_brakes", "emergency_brake_count"),
            "lane_invasions": _pick(safety, "lane_invasions", "lane_invasion_count"),
            "red_light_runs": _pick(safety, "red_light_runs", "red_light_run_count"),
        },
        "efficiency": {
            "avg_speed": _pick(efficiency, "avg_speed"),
            "distance": _pick(efficiency, "distance"),
            "duration": _pick(efficiency, "duration", "completion_time"),
            "completion_time": _pick(efficiency, "completion_time", "duration"),
        },
        "comfort": {
            "avg_acceleration": _pick(comfort, "avg_acceleration"),
            "max_acceleration": _pick(comfort, "max_acceleration"),
            "avg_jerk": _pick(comfort, "avg_jerk"),
            "steering_smoothness": _pick(comfort, "steering_smoothness"),
        },
        "task": {
            "completion": _pick(task, "completion", "goal_achievement_rate"),
            "pass_rate": _pick(task, "pass_rate"),
            "collision_rate": _pick(task, "collision_rate"),
            "takeover_rate": _pick(task, "takeover_rate"),
            "goal_achievement_rate": _pick(task, "goal_achievement_rate", "completion"),
        },
        "success_criteria_result": success,
        "grade": metrics.get("grade") if isinstance(metrics.get("grade"), str) else None,
        "score": _as_float(metrics.get("score")),
    }
    explicit_passed = success.get("passed")
    summary["passed"] = explicit_passed if isinstance(explicit_passed, bool) else None
    return summary


def _criteria_all_passed(success: dict[str, Any]) -> bool | None:
    """从 success_criteria_result 推导是否全部通过（无布尔判据返回 None）。"""
    flags: list[bool] = []
    for value in success.values():
        if isinstance(value, bool):
            flags.append(value)
        elif isinstance(value, dict) and isinstance(value.get("pass"), bool):
            flags.append(value["pass"])
    if not flags:
        return None
    return all(flags)


def compute_grade(
    *, score: float | None, collision_rate: float | None, passed: bool | None
) -> str | None:
    """按附录 E 计算等级：S ≥95% 且碰撞 0%；A ≥85% 且 <2%；B ≥70% 且 <5%；
    C ≥50% 且 <10%；D 其余。缺分数时用通过/碰撞兜底。"""
    if score is None:
        if passed is True and (collision_rate is None or collision_rate == 0):
            return "S"
        if passed is False:
            return "D"
        return None
    # 兼容 0–1 归一化分数
    normalized = score * 100.0 if 0.0 <= score <= 1.0 else score
    rate = collision_rate or 0.0
    if normalized >= 95 and rate == 0:
        return "S"
    if normalized >= 85 and rate < 2:
        return "A"
    if normalized >= 70 and rate < 5:
        return "B"
    if normalized >= 50 and rate < 10:
        return "C"
    return "D"


def render_html(report: dict[str, Any]) -> bytes:
    """生成轻量 HTML 报告（无外部依赖，满足 scene_test 多格式要求）。"""
    summary = report.get("summary") or {}
    rows: list[str] = []
    for group, values in summary.items():
        if not isinstance(values, dict):
            continue
        cells = "".join(
            f"<tr><td>{html.escape(str(key))}</td><td>{html.escape(str(value))}</td></tr>"
            for key, value in values.items()
        )
        rows.append(
            f"<h2>{html.escape(str(group))}</h2>"
            f"<table border='1' cellpadding='4' cellspacing='0'>{cells}</table>"
        )
    body = "".join(rows)
    return (
        "<!doctype html><html lang='zh-CN'><head><meta charset='utf-8'>"
        f"<title>scene_test {html.escape(str(report.get('report_id')))}</title>"
        "<style>body{font-family:system-ui,sans-serif;margin:24px}"
        "table{border-collapse:collapse;margin-bottom:16px}</style></head><body>"
        f"<h1>仿真评估报告</h1>"
        f"<p>sim_id: {html.escape(str(report.get('sim_id')))}<br>"
        f"scene_id: {html.escape(str(report.get('scene_id')))}<br>"
        f"status: {html.escape(str(report.get('sim_status')))}<br>"
        f"grade: {html.escape(str(summary.get('grade')))}<br>"
        f"score: {html.escape(str(summary.get('score')))}</p>"
        f"{body}</body></html>"
    ).encode()


class SimulationReportService:
    """生成/覆盖 scene_test 报告与 sidecar（无 DB 依赖）。"""

    def __init__(
        self,
        storage: ObjectStorage,
        reports: ReportRepository,
        *,
        pointer_key: str = "eval/simulation/latest.json",
        object_prefix: str = "scene_test",
    ) -> None:
        self._storage = storage
        self._reports = reports
        self._pointer_key = pointer_key.strip("/")
        self._prefix = object_prefix.strip("/")

    @staticmethod
    def report_id_for(sim_id: str) -> str:
        """稳定 report_id：同一 sim_id 重放覆盖同一对象（幂等）。"""
        return str(uuid.uuid5(_REPORT_NAMESPACE, sim_id))

    async def generate(
        self,
        *,
        sim_id: str,
        scene_id: str | None,
        vehicle_id: str | None,
        status: str | None,
        metrics: dict[str, Any] | None,
        timestamp: float | None = None,
        events: list[dict[str, Any]] | None = None,
        mode: str | None = None,
        started_at: Any = None,
        finished_at: Any = None,
    ) -> dict[str, Any]:
        """写入报告 JSON/HTML + sidecar + latest 指针，返回报告摘要。"""
        metrics = metrics or {}
        now = time.time()
        event_time = float(timestamp) if timestamp is not None else now
        start_time = _as_epoch(started_at) or event_time
        end_time = _as_epoch(finished_at) or event_time
        if end_time < start_time:
            start_time, end_time = end_time, start_time
        day = datetime.fromtimestamp(event_time, tz=UTC).strftime("%Y-%m-%d")
        report_id = self.report_id_for(sim_id)
        summary = summarize_metrics(metrics)

        success = summary.get("success_criteria_result") or {}
        criteria_passed = _criteria_all_passed(success)
        collisions = (summary.get("safety") or {}).get("collisions")
        collision_rate = (summary.get("task") or {}).get("collision_rate")
        if collision_rate is None and isinstance(collisions, (int, float)):
            # 无显式碰撞率时，以「有无碰撞」作为 0%/≥2% 的粗粒度兜底
            collision_rate = 0.0 if collisions == 0 else 2.0
        if summary.get("passed") is None:
            if criteria_passed is not None:
                summary["passed"] = criteria_passed
            elif status is not None:
                summary["passed"] = (
                    str(status).strip().upper() in _SUCCESS_STATUSES
                    and (collisions in (None, 0))
                )
        if not summary.get("grade"):
            summary["grade"] = compute_grade(
                score=summary.get("score"),
                collision_rate=collision_rate,
                passed=summary.get("passed"),
            )
        summary["sim_status"] = status

        report = {
            "schema_version": "1.0.0",
            "report_id": report_id,
            "report_type": ReportType.SCENE_TEST.value,
            "status": ReportStatus.READY.value,
            "sim_id": sim_id,
            "scene_id": scene_id,
            "vehicle_id": vehicle_id,
            "mode": mode,
            "sim_status": status,
            "start_time": start_time,
            "end_time": end_time,
            "generated_at": now,
            "summary": summary,
            "metrics": metrics,
            "events": events or [],
        }
        payload = json.dumps(report, ensure_ascii=False, indent=2).encode("utf-8")
        object_key = f"{self._prefix}/{day}/{report_id}.json"
        await self._storage.put_json(object_key, report)

        artifacts: list[dict[str, Any]] = [
            {
                "format": ReportFormat.JSON.value,
                "object_key": object_key,
                "size_bytes": len(payload),
            }
        ]
        html_payload = render_html(report)
        html_key = f"{self._prefix}/{day}/{report_id}.html"
        put_bytes = getattr(self._storage, "put_bytes", None)
        if put_bytes is not None:
            try:
                await put_bytes(html_key, html_payload, content_type="text/html; charset=utf-8")
                artifacts.append(
                    {
                        "format": ReportFormat.HTML.value,
                        "object_key": html_key,
                        "size_bytes": len(html_payload),
                    }
                )
            except Exception:
                logger.exception("simulation_report_html_failed", sim_id=sim_id)
        else:  # 兼容仅实现 put_json 的测试替身
            logger.warning("simulation_report_html_unsupported", sim_id=sim_id)

        meta: dict[str, Any] = {
            "report_id": report_id,
            "report_type": ReportType.SCENE_TEST.value,
            "status": ReportStatus.READY.value,
            "vehicle_id": vehicle_id,
            "scene_id": scene_id,
            "sim_id": sim_id,
            "start_time": start_time,
            "end_time": end_time,
            "formats": [item["format"] for item in artifacts],
            "created_at": now,
            "completed_at": now,
            "generated_by": None,
            "trigger": "schedule",
            "artifacts": artifacts,
            "summary": summary,
        }
        # 设计 7.4.3 规定的 sidecar 键（与报告同目录）；同时保留 reports/ 布局供列表接口复用
        meta_key = f"{self._prefix}/{day}/{report_id}.meta.json"
        await self._storage.put_json(meta_key, meta)
        meta["sidecar_object_key"] = meta_key
        await self._reports.save(meta)

        pointer = {
            "report_id": report_id,
            "report_type": ReportType.SCENE_TEST.value,
            "sim_id": sim_id,
            "scene_id": scene_id,
            "vehicle_id": vehicle_id,
            "object_key": object_key,
            "status": ReportStatus.READY.value,
            "grade": summary.get("grade"),
            "score": summary.get("score"),
            "sim_status": status,
            "updated_at": now,
        }
        await self._storage.put_json(self._pointer_key, pointer)
        await self._storage.put_json(f"eval/simulation/{sim_id}.json", report)
        logger.info(
            "simulation_report_ready",
            report_id=report_id,
            sim_id=sim_id,
            object_key=object_key,
            grade=summary.get("grade"),
        )
        return {
            "report_id": report_id,
            "object_key": object_key,
            "size_bytes": len(payload),
            "grade": summary.get("grade"),
            "score": summary.get("score"),
        }


__all__ = [
    "SimulationReportService",
    "compute_grade",
    "is_terminal_status",
    "render_html",
    "summarize_metrics",
]
