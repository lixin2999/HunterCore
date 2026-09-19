"""算法指标时序数据访问层（data_analytics.algorithm_metrics，hypertable）。

契约：contracts/database/ddl/05_timeseries.sql + orm-mapping.md 第 3 节。
写入：批量 + ``ON CONFLICT (time, vehicle_id, module, metric_name) DO NOTHING``（幂等），
返回**提交（attempted）行数**（被跳过的重复点仍计入，异步驱动无法提供精确插入行数）；
性能：批量写入 ≥ 10000 点/秒（executemany 分片），禁止逐条 INSERT + commit；
读取：``max_query_limit`` 放宽到 ``MAX_SERIES_POINTS``（指标趋势序列），调用方必须给出时间窗；
保留策略（90 天）由 TimescaleDB ``add_retention_policy`` 负责，本层不提供物理删除。
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from hunter_common.database.enums import MetricModule
from hunter_common.database.models import AlgorithmMetric
from hunter_common.database.repository import MAX_SERIES_POINTS, BaseRepository

#: 幂等冲突列 = DDL 主键 PRIMARY KEY (time, vehicle_id, module, metric_name)
METRIC_CONFLICT_COLUMNS: tuple[str, ...] = ("time", "vehicle_id", "module", "metric_name")


class AlgorithmMetricRepository(BaseRepository[AlgorithmMetric]):
    """算法模块指标时序读写（metric_value + tags 维度标签）。

    基类 ``get`` / ``get_or_raise`` / ``hard_delete`` 对复合主键表会被显式拒绝
    （首列主键 ``time`` 会跨车辆误命中），请使用 ``list_series`` / ``latest``。
    """

    model = AlgorithmMetric
    default_order_by = ("-time",)
    #: 指标趋势序列读取上限（契约 orm-mapping 第 3.3 节）
    max_query_limit = MAX_SERIES_POINTS

    async def insert_metrics(self, rows: Sequence[Mapping[str, Any]]) -> int:
        """批量写入指标点（幂等：重复时间点跳过），返回提交（attempted）行数。"""
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

        命中 ``idx_algorithm_metrics_vehicle_time``（``vehicle_id, time DESC``，最早停）
        或 ``idx_algorithm_metrics_module_metric_time``（``module, metric_name, time DESC``，趋势图）；
        单车辆明细查询建议给出时间窗并显式传 ``vehicle_id``。
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
        """最近一个指标点（车辆详情/仪表盘最新值）。

        说明：``vehicle_id + module + metric_name + time DESC LIMIT 1`` 有两个候选路径，
        执行计划由 planner 选择（``idx_algorithm_metrics_vehicle_time`` 可在命中该车首个点后早停；
        ``idx_algorithm_metrics_module_metric_time`` 在车队规模大时需多扫若干行）。
        上线前建议用 EXPLAIN ANALYZE 按真实数据分布确认；如需固定路径，
        须先在 DDL 契约评审 ``(vehicle_id, module, metric_name, time DESC)`` 索引。
        """
        rows = await self.find_all(
            AlgorithmMetric.vehicle_id == vehicle_id,
            AlgorithmMetric.module == module,
            AlgorithmMetric.metric_name == metric_name,
            order_by=("-time",),
            limit=1,
        )
        return rows[0] if rows else None


__all__ = ["METRIC_CONFLICT_COLUMNS", "AlgorithmMetricRepository"]
