"""ORM 模型：算法指标时序（data_analytics.algorithm_metrics）。

对应契约：contracts/database/ddl/05_timeseries.sql。
hypertable 化（chunk_time_interval=1 day）+ 90 天保留策略由 Alembic migration 0001 执行
（``SELECT create_hypertable(...)`` / ``add_retention_policy(...)``），ORM 仅做结构映射。

写入规范：批量写入（≥10000 点/秒），重复 (time, vehicle_id, module, metric_name) 幂等跳过。
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Double,
    Index,
    PrimaryKeyConstraint,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from hunter_common.database.base import Base, StrEnumType
from hunter_common.database.enums import MetricModule
from hunter_common.database.schema_names import ANALYTICS_SCHEMA

_METRIC_MODULE_VALUES = ", ".join(f"'{m.value}'" for m in MetricModule)


class AlgorithmMetric(Base):
    """算法模块指标时序（hypertable，按天分块，保留 90 天）。"""

    __tablename__ = "algorithm_metrics"
    __table_args__ = (
        PrimaryKeyConstraint(
            "time", "vehicle_id", "module", "metric_name", name="pk_algorithm_metrics"
        ),
        CheckConstraint(f"module IN ({_METRIC_MODULE_VALUES})", name="algorithm_metrics_module_check"),
        Index("idx_algorithm_metrics_vehicle_time", "vehicle_id", text("time DESC")),
        Index(
            "idx_algorithm_metrics_module_metric_time",
            "module",
            "metric_name",
            text("time DESC"),
        ),
        Index("idx_algorithm_metrics_tags", "tags", postgresql_using="gin"),
        {"schema": ANALYTICS_SCHEMA},
    )

    time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    vehicle_id: Mapped[str] = mapped_column(Text, nullable=False)
    module: Mapped[MetricModule] = mapped_column(StrEnumType(MetricModule), nullable=False)
    metric_name: Mapped[str] = mapped_column(Text, nullable=False)
    metric_value: Mapped[float] = mapped_column(Double, nullable=False)
    tags: Mapped[dict[str, object]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )


__all__ = ["AlgorithmMetric"]
