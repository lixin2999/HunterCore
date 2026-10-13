"""Pydantic API schemas（§8.1 REST 摘要对应请求/响应模型）。"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


# ---------- 通用分页 ----------

class PageMeta(BaseModel):
    page: int = 1
    page_size: int = 20
    total: int = 0
    has_next: bool = False


class PageResp(BaseModel):
    meta: PageMeta
    items: list[Any] = []


# ---------- Bag ----------

class BagSummary(BaseModel):
    bag_id: str
    vehicle_id: str
    state: str
    start_ns: Optional[int] = None
    end_ns: Optional[int] = None
    duration_s: Optional[float] = None
    size_bytes: Optional[int] = None
    message_count: Optional[int] = None
    qc_score: Optional[float] = None
    parse_status: Optional[str] = None
    updated_at: Optional[datetime] = None


class BagDetailResp(BagSummary):
    manifest: Optional[dict[str, Any]] = None
    topics: list[dict[str, Any]] = []
    artifacts: list[dict[str, Any]] = []


class ListBagsResp(BaseModel):
    meta: PageMeta
    items: list[BagSummary] = []


# ---------- Clip ----------

class ClipSummary(BaseModel):
    clip_id: int
    bag_id: str
    input_rev: str
    start_ns: int
    end_ns: int
    source: str
    tags: list[str] = []
    scene_label: Optional[str] = None
    quality: Optional[float] = None
    manifest_key: Optional[str] = None


class ListClipsResp(BaseModel):
    meta: PageMeta
    items: list[ClipSummary] = []


# ---------- Agent ----------

class AgentSummary(BaseModel):
    vehicle_id: str
    agent_version: Optional[str] = None
    state: Optional[str] = None
    last_heartbeat: Optional[datetime] = None
    metrics: dict[str, Any] = {}


class ListAgentsResp(BaseModel):
    items: list[AgentSummary] = []


# ---------- Artifact 预签名 ----------

class ArtifactUrlResp(BaseModel):
    object_key: str
    url: str
    expires_in: int = 600


# ---------- Task ----------

class TaskSummary(BaseModel):
    task_id: int
    bag_id: Optional[str] = None
    task_type: str
    state: str
    priority: int = 5
    attempts: int = 0
    error_code: Optional[str] = None
    created_at: Optional[datetime] = None


class ListTasksResp(BaseModel):
    meta: PageMeta
    items: list[TaskSummary] = []


class ReplayTaskReq(BaseModel):
    bag_ids: list[str] = Field(..., min_length=1, description="重算的 bag_id 列表")
    task_types: list[str] = Field(default_factory=lambda: ["align", "slice", "qc"])
    force_rerun: bool = False


# ---------- Dataset ----------

class DatasetCreateReq(BaseModel):
    dataset_id: str = Field(..., min_length=1)
    version: str = Field(..., min_length=1)
    selector: dict[str, Any] = {}
    format: Literal["webdataset", "lance"] = "webdataset"
    shuffle_seed: Optional[int] = None


class DatasetResp(BaseModel):
    dataset_id: str
    version: str
    state: str
    clip_count: Optional[int] = None
    manifest_key: Optional[str] = None


# ---------- 命令下发 ----------

class IssueCmdReq(BaseModel):
    vehicle_id: str
    cmd_type: Literal["reupload", "reparse", "cleanup", "seal", "set_config"]
    payload: dict[str, Any] = {}


class IssueCmdResp(BaseModel):
    cmd_id: str
    status: str = "issued"


# ---------- 健康检查 ----------

class HealthzResp(BaseModel):
    status: Literal["ok"] = "ok"
    service: str
    version: str = "0.1.0"


class ReadyzResp(BaseModel):
    status: Literal["ok", "degraded", "error"]
    db: str = "unknown"
    redis: str = "unknown"
    kafka: str = "unknown"
    minio: str = "unknown"
