"""algorithm_metrics 落库写入（``data_analytics.algorithm_metrics``）。

契约：``contracts/kafka/consumer-groups.yaml`` 消费组 ``data-analytics-algorithm-metrics``
（``writes: [data_analytics.algorithm_metrics]``，``ON CONFLICT (time, vehicle_id, module,
metric_name) DO NOTHING``）。本服务对**自身 schema** 的写入合法（契约 ``db_access.write``），
不同于跨库只读例外（审查 Y10 仅约束读路径与就绪探针，不禁止写自身分析表）。

写入经由 ``hunter_common`` 的 :class:`AlgorithmMetricRepository`（批量 + 幂等），
事务边界由 ``DatabaseSessionManager.session()`` 负责（RW 账号，见 main 装配）。
"""
from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from typing import Any, Protocol

from hunter_common.database.repositories import AlgorithmMetricRepository
from hunter_common.logging import get_logger
from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger("app.repositories.algorithm_metrics")


class SessionProvider(Protocol):
    """RW 会话来源协议（``DatabaseSessionManager`` 结构子集；测试注入替身）。"""

    def session(self) -> AsyncIterator[AsyncSession]: ...  # pragma: no cover - 协议声明


class AlgorithmMetricsWriter:
    """algorithm_metrics 批量写入门面（每批一事务；供消费者冲刷调用）。"""

    def __init__(self, db: SessionProvider) -> None:
        self._db = db

    async def insert(self, rows: Sequence[Mapping[str, Any]]) -> int:
        """单事务批量落库，返回提交（attempted）行数（重复点被 ON CONFLICT 跳过仍计数）。"""
        if not rows:
            return 0
        async with self._db.session() as session:
            return await AlgorithmMetricRepository(session).insert_metrics(rows)


__all__ = ["AlgorithmMetricsWriter", "SessionProvider"]
