"""ORM 模型：ingest_platform schema（对应 06_ingest_platform.sql）。

设计来源：《服务端数据消费解析与处理详细设计文档》v1.0 §6.2。
"""
from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    Numeric,
    PrimaryKeyConstraint,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from hunter_common.database.base import Base, StrEnumType
from hunter_common.database.schema_names import INGEST_PLATFORM_SCHEMA


# ========== 枚举（对应 DDL 中的 TYPE 定义） ==========

class BagState(StrEnum):
    DISCOVERED = "discovered"
    VERIFIED = "verified"
    PARSED = "parsed"
    UPLOADED = "uploaded"
    DONE = "done"
    PARSE_FAILED = "parse_failed"

    @property
    def rank(self) -> int:
        """状态机单调 rank（§5.2 层2 投影守卫）：discovered < verified < parsed < uploaded < done。"""
        return _BAG_STATE_RANKS[self]


_BAG_STATE_RANKS: dict[BagState, int] = {
    BagState.DISCOVERED: 0,
    BagState.VERIFIED: 1,
    BagState.PARSED: 2,
    BagState.UPLOADED: 3,
    BagState.DONE: 4,
    BagState.PARSE_FAILED: -1,   # 失败态不参与单调守卫
}


class TaskState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    DEAD = "dead"


class TaskType(StrEnum):
    VERIFY = "verify"
    ALIGN = "align"
    SLICE = "slice"
    QC = "qc"
    EXPORT = "export"
    COMPACT = "compact"
    ENRICH = "enrich"


class ArtifactClass(StrEnum):
    MANIFEST = "manifest"
    IMAGE = "image"
    PCD = "pcd"
    TABLE = "table"
    RAW = "raw"
    META = "meta"
    DERIVED = "derived"


class UploadState(StrEnum):
    UPLOADED = "uploaded"
    VERIFIED = "verified"
    MISSING = "missing"
    CORRUPT = "corrupt"


class DerivedKind(StrEnum):
    ALIGNED = "aligned"
    CLIP = "clip"
    QC = "qc"
    ENRICH = "enrich"


class CmdAckStatus(StrEnum):
    PENDING = "pending"
    ACK = "ack"
    TIMEOUT = "timeout"
    FAILED = "failed"


class ClipSource(StrEnum):
    EVENT = "event"
    WINDOW = "window"
    MINED = "mined"
    MANUAL = "manual"


# ========== 1. IngestEvent（事件仓，append-only） ==========

class IngestEvent(Base):
    """事件记录（ingest_platform.events，§6.2）。

    event_id 为幂等键（§5.2 层1）：INSERT ON CONFLICT DO NOTHING 返回 0 行 = 重复消息。
    """

    __tablename__ = "events"
    __table_args__ = (
        PrimaryKeyConstraint("event_id", "received_at", name="pk_ingest_events"),
        Index("idx_ingest_events_bag_seq", "bag_id", "seq"),
        Index("idx_ingest_events_vehicle", "vehicle_id", text("received_at DESC")),
        Index("idx_ingest_events_type", "event_type", text("received_at DESC")),
        {"schema": INGEST_PLATFORM_SCHEMA},
    )

    event_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    bag_id: Mapped[str | None] = mapped_column(Text)
    vehicle_id: Mapped[str] = mapped_column(Text, nullable=False)
    seq: Mapped[int | None] = mapped_column(BigInteger)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), primary_key=True
    )
    payload: Mapped[dict[str, object]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    kafka_partition: Mapped[int | None] = mapped_column(Integer)
    kafka_offset: Mapped[int | None] = mapped_column(BigInteger)
    origin: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'vehicle'"))


# ========== 2. Bag（bag 主档与状态投影） ==========

class Bag(Base):
    """bag 主档（ingest_platform.bags，§6.2）。"""

    __tablename__ = "bags"
    __table_args__ = (
        CheckConstraint("qc_score IS NULL OR (qc_score BETWEEN 0 AND 1)", name="ck_bags_qc_score"),
        Index("idx_bags_vehicle", "vehicle_id", text("updated_at DESC")),
        Index("idx_bags_state", "state"),
        {"schema": INGEST_PLATFORM_SCHEMA},
    )

    bag_id: Mapped[str] = mapped_column(Text, primary_key=True)
    vehicle_id: Mapped[str] = mapped_column(Text, nullable=False)
    ros_version: Mapped[str | None] = mapped_column(Text)
    start_ns: Mapped[int | None] = mapped_column(BigInteger)
    end_ns: Mapped[int | None] = mapped_column(BigInteger)
    duration_s: Mapped[float | None] = mapped_column(Numeric(12, 3))
    size_bytes: Mapped[int | None] = mapped_column(BigInteger)
    sha256: Mapped[str | None] = mapped_column(Text)
    message_count: Mapped[int | None] = mapped_column(BigInteger)
    manifest_rev: Mapped[str | None] = mapped_column(Text)
    state: Mapped[BagState] = mapped_column(
        StrEnumType(BagState), nullable=False, server_default=text("'discovered'")
    )
    seq: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))
    parse_status: Mapped[str | None] = mapped_column(Text)
    qc_score: Mapped[float | None] = mapped_column(Numeric(4, 3))
    manifest: Mapped[dict[str, object] | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


# ========== 3. BagTopic（bag 内各 topic 统计） ==========

class BagTopic(Base):
    """bag topic 明细（ingest_platform.bag_topics，§6.2）。"""

    __tablename__ = "bag_topics"
    __table_args__ = (
        PrimaryKeyConstraint("bag_id", "topic", name="pk_bag_topics"),
        {"schema": INGEST_PLATFORM_SCHEMA},
    )

    bag_id: Mapped[str] = mapped_column(Text, primary_key=True)
    topic: Mapped[str] = mapped_column(Text, primary_key=True)
    msg_type: Mapped[str] = mapped_column(Text, nullable=False)
    message_count: Mapped[int | None] = mapped_column(Integer)
    start_ns: Mapped[int | None] = mapped_column(BigInteger)
    end_ns: Mapped[int | None] = mapped_column(BigInteger)
    hz: Mapped[float | None] = mapped_column(Numeric(10, 4))


# ========== 4. Artifact（产物目录） ==========

class Artifact(Base):
    """产物记录（ingest_platform.artifacts，§6.2）。"""

    __tablename__ = "artifacts"
    __table_args__ = (
        Index("idx_artifacts_bag_class", "bag_id", "artifact_class"),
        {"schema": INGEST_PLATFORM_SCHEMA},
    )

    object_key: Mapped[str] = mapped_column(Text, primary_key=True)
    bag_id: Mapped[str | None] = mapped_column(Text)
    artifact_class: Mapped[ArtifactClass] = mapped_column(StrEnumType(ArtifactClass), nullable=False)
    size_bytes: Mapped[int | None] = mapped_column(BigInteger)
    sha256: Mapped[str | None] = mapped_column(Text)
    etag: Mapped[str | None] = mapped_column(Text)
    rows: Mapped[int | None] = mapped_column(Integer)
    count: Mapped[int | None] = mapped_column(Integer)
    upload_state: Mapped[UploadState] = mapped_column(
        StrEnumType(UploadState), nullable=False, server_default=text("'uploaded'")
    )
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


# ========== 5. DerivedAsset（派生资产） ==========

class DerivedAsset(Base):
    """派生资产记录（ingest_platform.derived_assets，§6.2）。"""

    __tablename__ = "derived_assets"
    __table_args__ = (
        PrimaryKeyConstraint("bag_id", "input_rev", "kind", "object_key", name="pk_derived_assets"),
        Index("idx_derived_bag", "bag_id", "input_rev"),
        {"schema": INGEST_PLATFORM_SCHEMA},
    )

    bag_id: Mapped[str] = mapped_column(Text, primary_key=True)
    input_rev: Mapped[str] = mapped_column(Text, primary_key=True)
    kind: Mapped[DerivedKind] = mapped_column(StrEnumType(DerivedKind), primary_key=True)
    object_key: Mapped[str] = mapped_column(Text, primary_key=True)
    meta: Mapped[dict[str, object]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


# ========== 6. Task（任务编排） ==========

class Task(Base):
    """任务记录（ingest_platform.tasks，§6.2 + §9.1）。

    幂等唯一键：(bag_id, task_type, input_rev)，同一组合只允许一条有效任务。
    """

    __tablename__ = "tasks"
    __table_args__ = (
        CheckConstraint("priority BETWEEN 0 AND 10", name="ck_tasks_priority"),
        CheckConstraint("attempts >= 0", name="ck_tasks_attempts"),
        Index("idx_tasks_queue", "state", "priority", "next_retry_at"),
        {"schema": INGEST_PLATFORM_SCHEMA},
    )

    task_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    bag_id: Mapped[str | None] = mapped_column(Text)
    input_rev: Mapped[str | None] = mapped_column(Text)
    task_type: Mapped[TaskType] = mapped_column(StrEnumType(TaskType), nullable=False)
    state: Mapped[TaskState] = mapped_column(
        StrEnumType(TaskState), nullable=False, server_default=text("'pending'")
    )
    priority: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("5"))
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("3"))
    next_retry_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    lease_owner: Mapped[str | None] = mapped_column(Text)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    result: Mapped[dict[str, object] | None] = mapped_column(JSONB)
    error_code: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    # 唯一约束在 DDL 中已声明：uq_tasks_bag_type_rev (bag_id, task_type, input_rev)


# ========== 7. Clip（场景切片） ==========

class Clip(Base):
    """场景切片（ingest_platform.clips，§6.2 + §7.3）。"""

    __tablename__ = "clips"
    __table_args__ = (
        Index("idx_clips_bag", "bag_id"),
        Index("idx_clips_tags", "tags", postgresql_using="gin"),
        {"schema": INGEST_PLATFORM_SCHEMA},
    )

    clip_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    bag_id: Mapped[str] = mapped_column(Text, nullable=False)
    input_rev: Mapped[str] = mapped_column(Text, nullable=False)
    start_ns: Mapped[int] = mapped_column(BigInteger, nullable=False)
    end_ns: Mapped[int] = mapped_column(BigInteger, nullable=False)
    source: Mapped[ClipSource] = mapped_column(
        StrEnumType(ClipSource), nullable=False, server_default=text("'window'")
    )
    tags: Mapped[list[str]] = mapped_column(
        ARRAY(Text), nullable=False, server_default=text("'{}'")
    )
    scene_label: Mapped[str | None] = mapped_column(Text)
    quality: Mapped[float | None] = mapped_column(Numeric(4, 3))
    manifest_key: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


# ========== 8. Label（标签） ==========

class Label(Base):
    """标签记录（ingest_platform.labels，§6.2）。"""

    __tablename__ = "labels"
    __table_args__ = (
        PrimaryKeyConstraint("bag_id", "clip_id", "key", "origin", name="pk_labels"),
        Index("idx_labels_bag", "bag_id"),
        {"schema": INGEST_PLATFORM_SCHEMA},
    )

    bag_id: Mapped[str] = mapped_column(Text, primary_key=True)
    clip_id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, server_default=text("0")  # 0 = bag 级标签
    )
    key: Mapped[str] = mapped_column(Text, primary_key=True)
    value: Mapped[dict[str, object]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    origin: Mapped[str] = mapped_column(Text, primary_key=True, server_default=text("'manual'"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


# ========== 9. Agent（车端 Agent 视图） ==========

class Agent(Base):
    """车端 Agent 状态（ingest_platform.agents，§6.2）。"""

    __tablename__ = "agents"
    __table_args__ = {"schema": INGEST_PLATFORM_SCHEMA}

    vehicle_id: Mapped[str] = mapped_column(Text, primary_key=True)
    agent_version: Mapped[str | None] = mapped_column(Text)
    config_rev: Mapped[str | None] = mapped_column(Text)
    state: Mapped[str | None] = mapped_column(Text)
    last_heartbeat: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    metrics: Mapped[dict[str, object]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


# ========== 10. Command（命令闭环） ==========

class Command(Base):
    """下行命令（ingest_platform.commands，§6.2 + §9.4）。"""

    __tablename__ = "commands"
    __table_args__ = (
        Index("idx_commands_vehicle", "vehicle_id", text("issued_at DESC")),
        {"schema": INGEST_PLATFORM_SCHEMA},
    )

    cmd_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    vehicle_id: Mapped[str] = mapped_column(Text, nullable=False)
    type: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, object]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    issued_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    ack_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ack_status: Mapped[CmdAckStatus] = mapped_column(
        StrEnumType(CmdAckStatus), nullable=False, server_default=text("'pending'")
    )
    ack_detail: Mapped[dict[str, object] | None] = mapped_column(JSONB)


# ========== 11. ProjectionAnomaly（投影异常） ==========

class ProjectionAnomaly(Base):
    """乱序投影异常记录（§5.2 层2 乱序兜底）。"""

    __tablename__ = "projection_anomaly"
    __table_args__ = (
        Index("idx_anomaly_bag", "bag_id", text("logged_at DESC")),
        {"schema": INGEST_PLATFORM_SCHEMA},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    bag_id: Mapped[str] = mapped_column(Text, nullable=False)
    event_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True))
    received_seq: Mapped[int | None] = mapped_column(BigInteger)
    current_seq: Mapped[int | None] = mapped_column(BigInteger)
    received_state: Mapped[str | None] = mapped_column(Text)
    event_state: Mapped[str | None] = mapped_column(Text)
    reason: Mapped[str | None] = mapped_column(Text)
    logged_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


# ========== 12. Dataset（数据集快照） ==========

class Dataset(Base):
    """训练数据集定义（ingest_platform.datasets，§8.2）。"""

    __tablename__ = "datasets"
    __table_args__ = (
        PrimaryKeyConstraint("dataset_id", "version", name="pk_datasets"),
        {"schema": INGEST_PLATFORM_SCHEMA},
    )

    dataset_id: Mapped[str] = mapped_column(Text, primary_key=True)
    version: Mapped[str] = mapped_column(Text, primary_key=True)
    selector: Mapped[dict[str, object]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    format: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'webdataset'"))
    shuffle_seed: Mapped[int | None] = mapped_column(BigInteger)
    template: Mapped[str | None] = mapped_column(Text)
    manifest_key: Mapped[str | None] = mapped_column(Text)
    state: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'pending'"))
    clip_count: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


__all__ = [
    "ArtifactClass",
    "Artifact",
    "Agent",
    "Bag",
    "BagState",
    "BagTopic",
    "Clip",
    "ClipSource",
    "CmdAckStatus",
    "Command",
    "Dataset",
    "DerivedAsset",
    "DerivedKind",
    "IngestEvent",
    "Label",
    "ProjectionAnomaly",
    "Task",
    "TaskState",
    "TaskType",
    "UploadState",
]
