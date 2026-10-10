"""algorithm_metrics 落库缓冲与冲刷（消费者批次钩子：写库成功才提交 offset）。

契约依据：
- 载荷结构 ``contracts/kafka/schemas/algorithm_metrics.schema.json``（字段 ⊆ 表列，
  ``module`` 受控词表 perception/planning/control）；
- 幂等落库 ``ON CONFLICT (time, vehicle_id, module, metric_name) DO NOTHING``
  （``consumer-groups.yaml`` data-analytics-algorithm-metrics）。

冲刷语义与 data-collector 采集链路一致：``handle`` 累积缓冲，达阈值尽力冲刷（失败仅告警）；
消费者批次收尾钩子 = :meth:`flush`——**写库成功才提交 offset**，钩子抛错则消息重投
（写侧 ON CONFLICT 幂等，不产生重复点）。

行映射口径（``to_row``）：
- ``time``：Unix epoch 秒（含毫秒小数，> 1e11 判为毫秒并换算）→ ``datetime``（UTC），
  对齐 ORM ``DateTime(timezone=True)`` 分区键；
- ``module``：字符串 → 受控枚举 :class:`MetricModule`（非法取值丢弃该条，禁止伪造）；
- ``metric_value``：必须为有限数值（bool/NaN/Inf 丢弃）；
- ``tags``：原样透传（缺省 {}，对齐 JSONB 列 server_default）。
"""
from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from hunter_common.database.enums import MetricModule
from hunter_common.logging import get_logger

from app.config import Settings

logger = get_logger("app.services.algorithm_metrics_ingest")

#: 毫秒时间戳判定阈值（Unix 秒在 5138 年前不会超过 1e11，与 data-collector 同口径）
_MILLISECOND_THRESHOLD = 1e11


class MetricsWriterProtocol(Protocol):
    """落库能力协议（依赖倒置，便于单测注入替身；实现见 AlgorithmMetricsWriter）。"""

    async def insert(self, rows: Sequence[Mapping[str, Any]]) -> int: ...  # pragma: no cover


def _epoch_to_datetime(value: Any) -> datetime | None:
    """epoch 秒（含毫秒判定）→ UTC datetime；非有限数值返回 None。"""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    seconds = float(value)
    if not math.isfinite(seconds):
        return None
    if seconds > _MILLISECOND_THRESHOLD:
        seconds /= 1000.0
    try:
        return datetime.fromtimestamp(seconds, tz=UTC)
    except (OverflowError, OSError, ValueError):
        return None


def _metric_module(value: Any) -> MetricModule | None:
    """受控词表解析（非法取值返回 None，不伪造、不新增）。"""
    try:
        return MetricModule(str(value))
    except ValueError:
        return None


def to_row(message: Mapping[str, Any]) -> dict[str, Any] | None:
    """algorithm_metrics 载荷 → ORM 行；任一必需字段非法返回 None（该条丢弃）。"""
    vehicle_id = message.get("vehicle_id")
    if not isinstance(vehicle_id, str) or not vehicle_id:
        return None
    metric_name = message.get("metric_name")
    if not isinstance(metric_name, str) or not metric_name:
        return None
    module = _metric_module(message.get("module"))
    if module is None:
        return None
    raw_value = message.get("metric_value")
    if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)):
        return None
    if not math.isfinite(float(raw_value)):
        return None
    time = _epoch_to_datetime(message.get("time"))
    if time is None:
        return None
    tags = message.get("tags")
    return {
        "time": time,
        "vehicle_id": vehicle_id,
        "module": module,
        "metric_name": metric_name,
        "metric_value": float(raw_value),
        "tags": dict(tags) if isinstance(tags, Mapping) else {},
    }


@dataclass(slots=True)
class IngestStats:
    """落库统计（可观测性与用例断言）。"""

    received: int = 0
    accepted: int = 0
    dropped: int = 0
    inserted: int = 0


class AlgorithmMetricsIngest:
    """algorithm_metrics 缓冲与批量落库（第 6 步：序列化写入）。

    缓冲策略：``handle`` 累积合法行，达 ``algorithm_metrics_flush_batch_size`` 时尽力冲刷；
    冲刷失败不抛出（由消费者批次收尾钩子重试并阻止 offset 提交，避免误入 DLQ）。
    """

    def __init__(self, writer: MetricsWriterProtocol, settings: Settings) -> None:
        self._writer = writer
        self._settings = settings
        self._buffer: list[dict[str, Any]] = []
        self._stats = IngestStats()

    @property
    def stats(self) -> IngestStats:
        """落库统计快照。"""
        return self._stats

    @property
    def pending(self) -> int:
        """待冲刷行数。"""
        return len(self._buffer)

    async def handle(self, payload: Mapping[str, Any]) -> bool:
        """单条消息入口（行映射 + 缓冲累积）。

        Returns:
            True = 合法行已入缓冲；False = 字段非法被丢弃（不重试、不进 DLQ：脏数据重放不自愈）。
        """
        self._stats.received += 1
        row = to_row(payload)
        if row is None:
            self._stats.dropped += 1
            logger.warning(
                "algorithm_metric_row_dropped",
                vehicle_id=payload.get("vehicle_id"),
                module=payload.get("module"),
                metric_name=payload.get("metric_name"),
                reason="invalid_field_or_uncontrolled_module",
            )
            return False
        self._stats.accepted += 1
        self._buffer.append(row)
        if len(self._buffer) >= self._settings.algorithm_metrics_flush_batch_size:
            await self._flush_safely()
        return True

    async def flush(self) -> int:
        """冲刷缓冲：单事务批量落库（幂等），返回本批行数。

        异常向上抛出（供消费者批次收尾钩子捕获 → 跳过 offset 提交 → 消息重投）；
        失败回填缓冲头部，钩子可立即重试。
        """
        if not self._buffer:
            return 0
        batch, self._buffer = self._buffer, []
        try:
            inserted = await self._writer.insert(batch)
        except Exception:
            self._buffer = batch + self._buffer
            logger.warning("algorithm_metrics_flush_requeued", requeued=len(batch))
            raise
        self._stats.inserted += inserted
        logger.info(
            "algorithm_metrics_batch_flushed",
            points=len(batch),
            inserted=inserted,
            duplicate_skipped=len(batch) - inserted,
        )
        return len(batch)

    async def _flush_safely(self) -> None:
        """缓冲满时的尽力冲刷：失败仅告警（最终由批次收尾钩子决定是否提交 offset）。"""
        try:
            await self.flush()
        except Exception:
            logger.exception("algorithm_metrics_flush_failed", pending=len(self._buffer))


__all__ = [
    "AlgorithmMetricsIngest",
    "IngestStats",
    "MetricsWriterProtocol",
    "to_row",
]
