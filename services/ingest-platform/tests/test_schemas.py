"""Pydantic schemas 验证测试。"""
from __future__ import annotations

import pytest

from app.schemas.schemas import (
    ArtifactUrlResp,
    BagDetailResp,
    BagSummary,
    ClipSummary,
    DatasetCreateReq,
    DatasetResp,
    HealthzResp,
    IssueCmdReq,
    ListBagsResp,
    PageMeta,
    ReadyzResp,
    ReplayTaskReq,
    TaskSummary,
)


class TestPageMeta:
    def test_defaults(self) -> None:
        m = PageMeta()
        assert m.page == 1
        assert m.page_size == 20
        assert m.total == 0
        assert m.has_next is False


class TestBagSummary:
    def test_minimal(self) -> None:
        b = BagSummary(bag_id="b-001", vehicle_id="veh-A1", state="done")
        assert b.bag_id == "b-001"
        assert b.qc_score is None
        assert b.duration_s is None

    def test_full(self) -> None:
        b = BagSummary(
            bag_id="b-002",
            vehicle_id="veh-B2",
            state="parsed",
            start_ns=1_700_000_000_000_000_000,
            end_ns=1_700_000_010_000_000_000,
            duration_s=10.0,
            size_bytes=1_048_576,
            message_count=3000,
            qc_score=0.95,
        )
        assert b.duration_s == 10.0
        assert b.qc_score == 0.95


class TestDatasetCreateReq:
    def test_valid(self) -> None:
        req = DatasetCreateReq(
            dataset_id="ds-001",
            version="v1",
            selector={"clip_ids": [1, 2, 3]},
            format="webdataset",
        )
        assert req.dataset_id == "ds-001"
        assert req.format == "webdataset"

    def test_invalid_format(self) -> None:
        with pytest.raises(ValueError):
            DatasetCreateReq(
                dataset_id="ds-001",
                version="v1",
                format="invalid_format",  # type: ignore
            )

    def test_empty_dataset_id_rejected(self) -> None:
        with pytest.raises(ValueError):
            DatasetCreateReq(dataset_id="", version="v1")


class TestReplayTaskReq:
    def test_defaults(self) -> None:
        req = ReplayTaskReq(bag_ids=["b-001", "b-002"])
        assert "align" in req.task_types
        assert req.force_rerun is False

    def test_empty_bag_ids_rejected(self) -> None:
        with pytest.raises(ValueError):
            ReplayTaskReq(bag_ids=[])


class TestIssueCmdReq:
    def test_valid_reupload(self) -> None:
        req = IssueCmdReq(
            vehicle_id="veh-A1",
            cmd_type="reupload",
            payload={"bag_id": "b-001"},
        )
        assert req.cmd_type == "reupload"

    def test_invalid_cmd_type(self) -> None:
        with pytest.raises(ValueError):
            IssueCmdReq(vehicle_id="veh-A1", cmd_type="invalid_cmd")  # type: ignore


class TestHealthzResp:
    def test_ok(self) -> None:
        r = HealthzResp(status="ok", service="ingest-platform")
        assert r.version == "0.1.0"


class TestReadyzResp:
    def test_degraded(self) -> None:
        r = ReadyzResp(status="degraded", db="ok", redis="error", kafka="ok", minio="ok")
        assert r.redis == "error"

    def test_invalid_status(self) -> None:
        with pytest.raises(ValueError):
            ReadyzResp(status="invalid")  # type: ignore


class TestClipSummary:
    def test_required_fields(self) -> None:
        c = ClipSummary(
            clip_id=1,
            bag_id="b-001",
            input_rev="abc123",
            start_ns=1_000_000_000,
            end_ns=2_000_000_000,
            source="window",
        )
        assert c.clip_id == 1
        assert c.tags == []
