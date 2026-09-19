"""算法指标时序数据访问层（data_analytics.algorithm_metrics，hypertable）。

契约：contracts/database/ddl/05_timeseries.sql + orm-mapping.md 第 3 节。
写入：批量 + ``ON CONFLICT (time, vehicle_id, module, metric_name) DO NOTHING``（幂等）；
性能：批量写入 ≥ 10000 点/秒（executemany 分片），禁止逐条 INSERT + commit；
保留策略（90 天）由 TimescaleDB ``add_retention_policy`` 负责，本层不提供物理删除。
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from hunter_common.database.enums import MetricModule
from hunter_common.database.models import AlgorithmMetric
from hunter_common.database.repository import BaseRepository

#: 幂等冲突列 = DDL 主键 PRIMARY KEY (time, vehicle_id, module, metric_name)
METRIC_CONFLICT_COLUMNS: tuple[str, ...] = ("time", "vehicle_id", "module", "metric_name")


class AlgorithmMetricRepository(BaseRepository[AlgorithmMetric]):
    """算法模块指标时序读写（metric_value + tags 维度标签）。"""

    model = AlgorithmMetric
    default_order_by = ("-time",)

    async def insert_metrics(self, rows: Sequence[Mapping[str, Any]]) -> int:
        """批量写入指标点（幂等：重复时间点跳过），返回处理行数。"""
        return await self.bulk_create_ignore_conflicts(
            rows, conflict_columns=METRIC_CONFLICT_COLUMNS
        )

    async def list_series(
        self,
        vehicle_id: str,
        *,
        module: MetricModule | None = None,
        metric_name: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int | None = None,
    ) -> list[AlgorithmMetric]:
        """按车辆（可选模块/指标名/时间窗）查询指标时序。

        命中 idx_algorithm_metrics_vehicle_time 与
        idx_algorithm_metrics_module_metric_time（时间倒序）。
        """
        conditions: list[Any] = [AlgorithmMetric.vehicle_id == vehicle_id]
        if module is not None:
            conditions.append(AlgorithmMetric.module == module)
        if metric_name is not None:
            conditions.append(AlgorithmMetric.metric_name == metric_name)
        if start_time is not None:
            conditions.append(AlgorithmMetric.time >= start_time)
        if end_time is not None:
            conditions.append(AlgorithmMetric.time <= end_time)
        return await self.find_all(*conditions, limit=limit)

    async def latest(
        self, vehicle_id: str, *, module: MetricModule, metric_name: str
    ) -> AlgorithmMetric | None:
        """最近一个指标点（车辆详情/仪表盘最新值）。"""
        rows = await self.find_all(
            AlgorithmMetric.vehicle_id == vehicle_id,
            AlgorithmMetric.module == module,
            AlgorithmMetric.metric_name == metric_name,
            order_by=("-time",),
            limit=1,
        )
        return rows[0] if rows else None


__all__ = ["METRIC_CONFLICT_COLUMNS", "AlgorithmMetricRepository"]
