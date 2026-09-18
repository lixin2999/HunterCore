"""算法评估 / 场景覆盖率 / Corner Case 端点测试（文档缺失 503、窗口降级、截断、过滤排序分页）。"""
from __future__ import annotations

import pytest

from app.core import dependencies as deps
from app.main import app
from app.tests.conftest import ANALYST_HEADERS, OPERATOR_HEADERS, api
from app.tests.fakes import InMemoryObjectStorage

WINDOW = {"start_time": 1000.0, "end_time": 2000.0}
PERCEPTION_METRICS = {
    "map_3d": 0.62,
    "map_bev": 0.71,
    "iou": 0.55,
    "recall": 0.83,
    "precision": 0.9,
    "mean_localization_error_m": 0.12,
}


def _seed_eval_documents(
    perception: dict[str, object] | None = None,
    control: dict[str, object] | None = None,
    coverage: dict[str, object] | None = None,
    corner_cases: dict[str, object] | None = None,
) -> None:
    """写入 latest.json 指针 + 各类评估文档（缺失的类别不写入指针）。"""
    storage = InMemoryObjectStorage()
    pointer: dict[str, str] = {}
    for kind, doc in (
        ("perception", perception),
        ("control", control),
        ("scene_coverage", coverage),
        ("corner_cases", corner_cases),
    ):
        if doc is not None:
            key = f"eval_documents/{kind}.json"
            pointer[kind] = key
    storage._objects.update(
        {
            "eval_documents/latest.json": pointer,
            **{
                pointer[kind]: doc
                for kind, doc in (
                    ("perception", perception),
                    ("control", control),
                    ("scene_coverage", coverage),
                    ("corner_cases", corner_cases),
                )
                if doc is not None
            },
        }
    )
    app.state.__setattr__(deps.KEY_STORAGE, storage)
    from app.repositories.eval_documents import EvalDocumentRepository

    app.state.__setattr__(
        deps.KEY_EVAL_DOCUMENT_REPOSITORY, EvalDocumentRepository(storage, pointer_key="eval_documents/latest.json")
    )


def _control_doc() -> dict[str, object]:
    """控制性能评估文档（契约示例，阈值判定由离线作业写入）。"""
    return {
        "vehicle_id": "HUNTER-001",
        "window": WINDOW,
        "sample_count": 800,
        "metrics": {
            "velocity_rmse_ms": 0.15,
            "steering_rmse_rad": 0.01,
            "overshoot_percent": 5.0,
            "settling_time_s": 1.2,
        },
        "thresholds": {
            "velocity_rmse_ms": {"value": 0.15, "threshold": 0.2, "comparator": "lt", "unit": "m/s", "pass": True},
            "steering_rmse_rad": {"value": 0.01, "threshold": 0.02, "comparator": "lt", "unit": "rad", "pass": True},
            "overshoot_percent": {"value": 5.0, "threshold": 10, "comparator": "lt", "unit": "%", "pass": True},
            "settling_time_s": {"value": 1.2, "threshold": 2, "comparator": "lt", "unit": "s", "pass": True},
        },
        "overall_pass": True,
        "data_source": "flink_stream",
        "job_name": "eval-control-1h",
        "updated_at": 2001.0,
        "report_id": None,
    }


@pytest.mark.asyncio
async def test_eval_documents_missing_service_unavailable(clean_state: None) -> None:
    """指针未生成 → 感知/控制/覆盖率/Corner Case 均 503 code=5001。"""
    _seed_eval_documents()
    async with api() as client:
        for path in (
            "/api/v1/analytics/perception/eval",
            "/api/v1/analytics/control/eval",
            "/api/v1/analytics/scene/coverage",
            "/api/v1/analytics/corner-cases",
        ):
            resp = await client.get(path, headers=ANALYST_HEADERS)
            assert resp.status_code == 503, path
            assert resp.json()["code"] == 5001, path


@pytest.mark.asyncio
async def test_perception_eval_normal_and_window_mismatch(clean_state: None) -> None:
    """感知评估：命中文档返回指标；查询窗口不匹配 → 空数据降级（sample_count=0）。"""
    _seed_eval_documents(
        perception={
            "vehicle_id": None,
            "window": WINDOW,
            "sample_count": 1200,
            "metrics": PERCEPTION_METRICS,
            "data_source": "spark_batch",
            "job_name": "eval-perception-daily",
            "updated_at": 2001.0,
        }
    )
    async with api() as client:
        hit = await client.get("/api/v1/analytics/perception/eval", headers=ANALYST_HEADERS)
        mismatch = await client.get(
            "/api/v1/analytics/perception/eval",
            params={"start_time": 3000.0, "end_time": 4000.0},
            headers=ANALYST_HEADERS,
        )
    assert hit.status_code == 200
    data = hit.json()["data"]
    assert data["metrics"]["map_3d"] == pytest.approx(0.62)
    assert data["sample_count"] == 1200
    assert data["window"]["duration_hours"] == pytest.approx(1000.0 / 3600.0)
    assert mismatch.status_code == 200
    assert mismatch.json()["data"]["sample_count"] == 0


@pytest.mark.asyncio
async def test_control_eval_thresholds_and_vehicle_mismatch(clean_state: None) -> None:
    """控制评估：阈值取文档（6.3.3 常量）；车辆不匹配 → 空数据 + overall_pass=False。"""
    _seed_eval_documents(control=_control_doc())
    async with api() as client:
        hit = await client.get(
            "/api/v1/analytics/control/eval",
            params={"vehicle_id": "HUNTER-001"},
            headers=ANALYST_HEADERS,
        )
        mismatch = await client.get(
            "/api/v1/analytics/control/eval",
            params={"vehicle_id": "HUNTER-002"},
            headers=ANALYST_HEADERS,
        )
    assert hit.status_code == 200
    data = hit.json()["data"]
    assert data["thresholds"]["velocity_rmse_ms"]["threshold"] == pytest.approx(0.2)
    assert data["overall_pass"] is True
    assert mismatch.json()["data"]["sample_count"] == 0
    assert mismatch.json()["data"]["overall_pass"] is False


@pytest.mark.asyncio
async def test_eval_query_window_validation(clean_state: None) -> None:
    """评估查询窗口：不成对/倒置/超 7 天 → 422 code=2001。"""
    _seed_eval_documents(control=_control_doc())
    async with api() as client:
        cases = [
            {"start_time": 1000.0},
            {"start_time": 2000.0, "end_time": 1000.0},
            {"start_time": 0.0, "end_time": 8 * 86400.0},
        ]
        for params in cases:
            resp = await client.get("/api/v1/analytics/control/eval", params=params, headers=ANALYST_HEADERS)
            assert resp.status_code == 422, params
            assert resp.json()["code"] == 2001, params


@pytest.mark.asyncio
async def test_eval_readable_by_operator(clean_state: None) -> None:
    """operator 属于 read 角色 → 评估查询 200（只读端点）。"""
    _seed_eval_documents(control=_control_doc())
    async with api() as client:
        resp = await client.get("/api/v1/analytics/control/eval", headers=OPERATOR_HEADERS)
    assert resp.status_code == 200


def _coverage_doc() -> dict[str, object]:
    """场景覆盖率文档：6 个栅格单元，3 个已覆盖（coverage_ratio=0.5）。"""
    heatmap = [
        {"x": i, "y": 0, "count": i + 1}
        for i in range(6)
    ]
    return {
        "vehicle_id": None,
        "window": WINDOW,
        "grid_size_m": 10,
        "covered_cells": 3,
        "total_cells": 6,
        "coverage_ratio": 0.5,
        "heatmap": heatmap,
        "data_source": "spark_batch",
        "job_name": "scene-coverage-daily",
        "updated_at": 2001.0,
    }


@pytest.mark.asyncio
async def test_coverage_normal_without_truncation(clean_state: None) -> None:
    """/scene/coverage 默认 max_cells=5000 → 不截断。"""
    _seed_eval_documents(coverage=_coverage_doc())
    async with api() as client:
        resp = await client.get("/api/v1/analytics/scene/coverage", headers=ANALYST_HEADERS)
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["coverage_ratio"] == pytest.approx(0.5)
    assert data["grid_size_m"] == pytest.approx(10.0)
    assert len(data["heatmap"]) == 6
    assert data["heatmap_truncated"] is False


@pytest.mark.asyncio
async def test_coverage_heatmap_truncated_flag(clean_state: None) -> None:
    """max_cells=2 → 热力图截断至 2 个单元且 heatmap_truncated=true。"""
    _seed_eval_documents(coverage=_coverage_doc())
    async with api() as client:
        resp = await client.get(
            "/api/v1/analytics/scene/coverage", params={"max_cells": 2}, headers=ANALYST_HEADERS
        )
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert len(data["heatmap"]) == 2
    assert data["heatmap_truncated"] is True


def _corner_cases_doc() -> dict[str, object]:
    """Corner Case 文档：3 条有效样本（乱序 anomaly_score）+ 1 条非法样本。"""
    items = [
        {
            "corner_case_id": "cc-1",
            "category": "kinematic",
            "vehicle_id": "HUNTER-001",
            "event_time": 1500.0,
            "window_start": 1495.0,
            "window_end": 1505.0,
            "anomaly_score": 0.9,
            "algorithm": "isolation_forest",
            "description": "急加速场景",
        },
        {
            "corner_case_id": "cc-2",
            "category": "object",
            "vehicle_id": "HUNTER-002",
            "event_time": 1600.0,
            "window_start": 1595.0,
            "window_end": 1605.0,
            "anomaly_score": 0.8,
            "algorithm": "dbscan",
            "description": "近距离目标",
        },
        {
            "corner_case_id": "cc-3",
            "category": "environment",
            "vehicle_id": "HUNTER-001",
            "event_time": 1700.0,
            "window_start": 1695.0,
            "window_end": 1705.0,
            "anomaly_score": 0.7,
            "algorithm": "isolation_forest",
            "description": "低光照场景",
        },
        {"corner_case_id": "cc-invalid"},
    ]
    mining = {
        "algorithm": ["isolation_forest"],
        "last_run_at": 2001.0,
        "job_name": "corner-case-miner-daily",
        "window": WINDOW,
        "feature_count": 32,
        "total_found": 4,
    }
    return {"window": WINDOW, "items": items, "mining": mining}


@pytest.mark.asyncio
async def test_corner_cases_sort_filter_paginate_and_meta(clean_state: None) -> None:
    """默认按 anomaly_score 降序；category 过滤；分页正确；mining 元数据透传；非法样本跳过。"""
    _seed_eval_documents(corner_cases=_corner_cases_doc())
    async with api() as client:
        default = await client.get("/api/v1/analytics/corner-cases", headers=ANALYST_HEADERS)
        filtered = await client.get(
            "/api/v1/analytics/corner-cases", params={"category": "object"}, headers=ANALYST_HEADERS
        )
        paged = await client.get(
            "/api/v1/analytics/corner-cases", params={"page": 2, "page_size": 2}, headers=ANALYST_HEADERS
        )
    assert default.status_code == 200
    data = default.json()["data"]
    assert [item["corner_case_id"] for item in data["items"]] == ["cc-1", "cc-2", "cc-3"]
    assert data["total"] == 3  # 非法样本跳过且计警告日志
    assert data["category_counts"] == {"kinematic": 1, "object": 1, "environment": 1}
    assert data["mining"]["job_name"] == "corner-case-miner-daily"
    assert data["page"] == 1 and data["page_size"] == 20

    assert filtered.json()["data"]["total"] == 1
    assert filtered.json()["data"]["items"][0]["corner_case_id"] == "cc-2"

    assert [i["corner_case_id"] for i in paged.json()["data"]["items"]] == ["cc-3"]
    assert paged.json()["data"]["page"] == 2