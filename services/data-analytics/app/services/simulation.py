"""仿真评估报告（scene_test）：sim_metrics → hunter-reports。

按 CARLA 接入方案 7.4：
- 报告对象：``scene_test/{yyyy-mm-dd}/{report_id}.json``；
- sidecar：``reports/scene_test/{report_id}.json``（复用现有报告列表/详情接口）；
- 指针：``eval/simulation/latest.json``（看板读取最新仿真评估）。
"""
from __future__ import annotations

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


def is_terminal_status(status: Any) -> bool:
    """CARLA 终态判定（大小写不敏感）。"""
    return str(status or "").strip().upper() in _TERMINAL_STATUSES


def _as_float(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


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
            "collision_count": _as_float(safety.get("collision_count")),
            "min_ttc": _as_float(safety.get("min_ttc")),
            "min_distance": _as_float(safety.get("min_distance")),
            "emergency_brake_count": _as_float(safety.get("emergency_brake_count")),
            "lane_invasion_count": _as_float(safety.get("lane_invasion_count")),
            "red_light_run_count": _as_float(safety.get("red_light_run_count")),
        },
        "efficiency": {
            "avg_speed": _as_float(efficiency.get("avg_speed")),
            "distance": _as_float(efficiency.get("distance")),
            "duration": _as_float(efficiency.get("duration")),
        },
        "comfort": {
            "avg_acceleration": _as_float(comfort.get("avg_acceleration")),
            "max_acceleration": _as_float(comfort.get("max_acceleration")),
            "avg_jerk": _as_float(comfort.get("avg_jerk")),
            "steering_smoothness": _as_float(comfort.get("steering_smoothness")),
        },
        "task": {
            "completion": _as_float(task.get("completion")),
            "pass_rate": _as_float(task.get("pass_rate")),
        },
        "success_criteria_result": success,
        "grade": metrics.get("grade"),
        "score": _as_float(metrics.get("score")),
    }
    summary["passed"] = (
        bool(success.get("passed"))
        if isinstance(success.get("passed"), bool)
        else None
    )
    return summary


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
    ) -> dict[str, Any]:
        """写入报告 JSON + sidecar + latest 指针，返回报告摘要。"""
        metrics = metrics or {}
        now = time.time()
        event_time = float(timestamp) if timestamp is not None else now
        day = datetime.fromtimestamp(event_time, tz=UTC).strftime("%Y-%m-%d")
        report_id = self.report_id_for(sim_id)
        summary = summarize_metrics(metrics)
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
            "start_time": event_time,
            "end_time": event_time,
            "generated_at": now,
            "summary": summary,
            "metrics": metrics,
            "events": events or [],
        }
        payload = json.dumps(report, ensure_ascii=False, indent=2).encode("utf-8")
        object_key = f"{self._prefix}/{day}/{report_id}.json"
        await self._storage.put_json(object_key, report)
        meta: dict[str, Any] = {
            "report_id": report_id,
            "report_type": ReportType.SCENE_TEST.value,
            "status": ReportStatus.READY.value,
            "vehicle_id": vehicle_id,
            "scene_id": scene_id,
            "sim_id": sim_id,
            "start_time": event_time,
            "end_time": event_time,
            "formats": [ReportFormat.JSON.value],
            "created_at": now,
            "completed_at": now,
            "generated_by": None,
            "trigger": "schedule",
            "artifacts": [
                {
                    "format": ReportFormat.JSON.value,
                    "object_key": object_key,
                    "size_bytes": len(payload),
                }
            ],
            "summary": summary,
        }
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


__all__ = ["SimulationReportService", "is_terminal_status", "summarize_metrics"]
