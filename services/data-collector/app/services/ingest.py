"""遥测预处理与批量入库（设计文档 5.4 节 6 步流水线；审查 R1 补齐）。

流水线（顺序固定，契约 data-collector.yaml `x-hunter-ingest-pipeline`）：
1. JSON 解析 + 契约 Schema 校验（由 ``KafkaConsumerManager(schema_name="auto")`` 承担，
   非法消息直接进 ``{topic}.dlq``，不进入本模块）；
2. 数据校验：六段结构完整性与**数值有限性**（JSON Schema 不拦截 NaN/Inf）→ 非法样本丢弃；
3. 时间戳对齐：毫秒时间戳归一为秒；未来漂移 > 60s 或早于保留窗口的样本丢弃；
4. 数据清洗：值域裁剪（``battery_soc`` 0-100 / ``cpu_usage``、``gpu_usage`` 0-100 /
   ``network_rssi`` ≤ 0，区间来源 telemetry.schema.json 的 minimum/maximum）；
5. enrichment：生成入库扁平行（列名与读路径 ``telemetry_row_to_dict`` 严格对称）；
   采集侧元信息仅入日志与指标——契约 ``telemetry.schema.json`` 为
   ``additionalProperties: false``，禁止增删消息字段；
6. 序列化写入：批量写 ``data_collector.vehicle_telemetry``（``ON CONFLICT DO NOTHING`` 幂等）
   + 投递 ``telemetry_raw``（校验后原样）/ ``telemetry_clean``（清洗后）。

性能与可靠性：
- 批量插入（≥10000 点/秒，契约 consumer-groups.yaml）；
- 缓冲条数达 ``telemetry_batch_size`` 或消费者批次收尾钩子触发时冲刷
  （``KafkaConsumerManager.on_batch_end``：**写库成功才提交 offset**，钩子失败则消息重投）。
"""
from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from hunter_common.database.enums import EVENT_LEVEL_BY_TYPE, EventLevel, EventType
from hunter_common.logging import get_logger

from app.config import Settings

logger = get_logger("app.services.ingest")

#: 六段字段 → 入库列映射（与 repositories.telemetry.telemetry_row_to_dict 严格对称）
SEGMENT_COLUMNS: dict[str, tuple[str, ...]] = {
    "chassis": (
        "velocity",
        "steering_angle",
        "battery_voltage",
        "battery_soc",
        "battery_current",
        "battery_temp",
        "control_mode",
        "vehicle_state",
        "fault_code",
        "motor_rpm",
        "motor_current",
        "motor_temp",
    ),
    "localization": (
        "x",
        "y",
        "z",
        "roll",
        "pitch",
        "heading",
        "linear_velocity",
        "angular_velocity",
        "position_std",
        "heading_std",
    ),
    "perception": ("detected_objects", "fps", "latency_ms", "object_types"),
    "planning": ("trajectory_length", "trajectory_points", "planning_latency_ms", "current_behavior"),
    "control": (
        "target_velocity",
        "target_steer",
        "velocity_error",
        "steer_error",
        "control_latency_ms",
    ),
    "system": (
        "cpu_usage",
        "gpu_usage",
        "memory_usage_mb",
        "gpu_temp",
        "cpu_temp",
        "network_rssi",
        "network_latency_ms",
    ),
}

#: 清洗裁剪区间（键为列名；来源：contracts/kafka/schemas/telemetry.schema.json 的 minimum/maximum）
CLAMP_RANGES: dict[str, tuple[float, float]] = {
    "battery_soc": (0.0, 100.0),
    "cpu_usage": (0.0, 100.0),
    "gpu_usage": (0.0, 100.0),
    "network_rssi": (-200.0, 0.0),
}

#: 时间戳容忍的未来漂移（秒）：车载时钟轻微超前可接受，超此值视为时钟异常
MAX_FUTURE_SKEW_SECONDS = 60
#: 最大回溯（秒）：telemetry_raw 保留 7 天，更早的补传样本无消费价值
MAX_BACKFILL_SECONDS = 7 * 24 * 3600
#: 毫秒时间戳判定阈值（Unix 秒在 5138 年前不会超过 1e11）
_MILLISECOND_THRESHOLD = 1e11


class TelemetryRepositoryProtocol(Protocol):
    """入库所需仓储能力（依赖倒置，便于单元测试注入替身）。"""

    def transaction(self) -> Any:  # pragma: no cover - 协议声明
        ...

    async def insert_points(self, session: Any, rows: Sequence[Mapping[str, Any]]) -> int:
        ...


__all__ = [
    "CLAMP_RANGES",
    "MAX_BACKFILL_SECONDS",
    "MAX_FUTURE_SKEW_SECONDS",
    "SEGMENT_COLUMNS",
    "IngestStats",
    "PreparedEvent",
    "PreparedTelemetry",
    "TelemetryIngestService",
    "clean_segment",
    "flatten_to_row",
    "normalize_epoch_seconds",
    "prepare_event",
    "prepare_telemetry",
]


def normalize_epoch_seconds(value: Any) -> float | None:
    """时间戳归一（毫秒 → 秒）；非有限数值返回 None。

    契约 ``timestamp`` 单位为 Unix epoch 秒；车端个别 SDK 以毫秒上报（13 位），
    此处按 > 1e11 判定为毫秒并换算，避免时序表出现未来 5 万年的时间点。
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    seconds = float(value)
    if not math.isfinite(seconds):
        return None
    if seconds > _MILLISECOND_THRESHOLD:
        seconds /= 1000.0
    return seconds


def _clean_scalar(name: str, value: Any) -> tuple[Any, bool, bool]:
    """单值清洗：返回 ``(清洗后值, 是否变更, 是否因非法而丢弃)``。

    - 非有限数值（NaN/Inf）→ 丢弃该值（JSON Schema 不拦截，必须在此拦截）；
    - 命中裁剪区间 → 裁剪到边界（值变更但保留样本）。
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return value, False, False
    number = float(value)
    if not math.isfinite(number):
        return None, True, True
    limits = CLAMP_RANGES.get(name)
    if limits is None:
        return value, False, False
    low, high = limits
    clamped = min(max(number, low), high)
    if clamped != number:
        return clamped, True, False
    return value, False, False


def clean_segment(segment_name: str, segment: Mapping[str, Any]) -> tuple[dict[str, Any], bool, bool]:
    """清洗单段（第 4 步）：返回 ``(清洗后段, 是否变更, 是否含非法值)``。

    列表字段（motor_rpm 等）逐元素清洗：非法元素剔除，其余保留。
    """
    cleaned: dict[str, Any] = {}
    changed = False
    invalid = False
    for column in SEGMENT_COLUMNS[segment_name]:
        value = segment.get(column)
        if isinstance(value, list):
            items: list[Any] = []
            for item in value:
                cleaned_item, item_changed, item_invalid = _clean_scalar(column, item)
                changed = changed or item_changed
                invalid = invalid or item_invalid
                if not item_invalid:
                    items.append(cleaned_item)
            cleaned[column] = items
            continue
        cleaned_value, value_changed, value_invalid = _clean_scalar(column, value)
        changed = changed or value_changed
        invalid = invalid or value_invalid
        if not value_invalid:
            cleaned[column] = cleaned_value
    return cleaned, changed, invalid


@dataclass(frozen=True, slots=True)
class PreparedTelemetry:
    """一条通过预处理的遥测样本（可入库 + 可投递）。"""

    vehicle_id: str
    timestamp: float
    seq: int | None
    raw: dict[str, Any]
    clean: dict[str, Any]
    row: dict[str, Any]
    cleaned: bool = False


def flatten_to_row(clean: Mapping[str, Any]) -> dict[str, Any]:
    """清洗后消息 → 入库扁平行（列名与读路径 ``telemetry_row_to_dict`` 严格对称）。"""
    row: dict[str, Any] = {}
    for segment_name, columns in SEGMENT_COLUMNS.items():
        segment = clean.get(segment_name) or {}
        for column in columns:
            row[column] = segment.get(column)
    return row


def prepare_telemetry(
    payload: Mapping[str, Any], *, now: float | None = None
) -> PreparedTelemetry | None:
    """执行流水线第 2-5 步；返回 ``None`` 表示样本被丢弃（非法/时钟异常）。

    Args:
        payload: 已通过契约 Schema 校验的遥测消息。
        now: 当前时间（测试注入；缺省取 ``time.time()``）。
    """
    vehicle_id = payload.get("vehicle_id")
    if not isinstance(vehicle_id, str) or not vehicle_id:
        return None
    timestamp = normalize_epoch_seconds(payload.get("timestamp"))
    if timestamp is None:
        return None
    current = time.time() if now is None else now
    if timestamp > current + MAX_FUTURE_SKEW_SECONDS:
        return None
    if timestamp < current - MAX_BACKFILL_SECONDS:
        return None
    seq = payload.get("seq")
    if isinstance(seq, bool) or not isinstance(seq, (int, float)):
        seq = None
    raw: dict[str, Any] = {
        "vehicle_id": vehicle_id,
        "timestamp": timestamp,
        "seq": seq if seq is None else int(seq),
    }
    clean: dict[str, Any] = dict(raw)
    cleaned_any = False
    for segment_name, columns in SEGMENT_COLUMNS.items():
        segment = payload.get(segment_name)
        if not isinstance(segment, Mapping):
            return None  # 契约：六段必填（Schema 已校验，此处防御式兜底）
        cleaned_segment, changed, invalid = clean_segment(segment_name, segment)
        if invalid:
            # 含 NaN/Inf 的数值字段无法进入「数值型且必填」的契约消息 → 整条样本丢弃
            return None
        cleaned_any = cleaned_any or changed
        raw[segment_name] = {column: segment.get(column) for column in columns}
        clean[segment_name] = cleaned_segment
    row = flatten_to_row(clean)
    row["vehicle_id"] = vehicle_id
    row["time"] = datetime.fromtimestamp(timestamp, tz=UTC)
    row["seq"] = seq if seq is None else int(seq)
    return PreparedTelemetry(
        vehicle_id=vehicle_id,
        timestamp=timestamp,
        seq=row["seq"],
        raw=raw,
        clean=clean,
        row=row,
        cleaned=cleaned_any,
    )


@dataclass(frozen=True, slots=True)
class PreparedEvent:
    """一条通过预处理的事件（可落库 + 可投递 event_raw）。"""

    vehicle_id: str
    row: dict[str, Any]
    message: dict[str, Any]


def prepare_event(payload: Mapping[str, Any], *, now: float | None = None) -> PreparedEvent:
    """事件预处理（5.4 流水线的事件分支）：等级一致性 + 时间对齐 + 落库行映射。

    契约依据：
    - 事件等级必须等于 ``contracts/database/enums.md`` 第 3 节的受控映射
      （``EVENT_LEVEL_BY_TYPE``，不可放宽）；
    - ``timestamp`` 落 ``events.event_time``（禁止用入库时间替代，见 event.schema.json）。

    Raises:
        ValueError: 结构/等级映射/时间戳非法（重试不会自愈 → 由消费者转 DLQ 供人工排查）。
    """
    vehicle_id = payload.get("vehicle_id")
    if not isinstance(vehicle_id, str) or not vehicle_id:
        raise ValueError("event 消息缺少 vehicle_id")
    timestamp = normalize_epoch_seconds(payload.get("timestamp"))
    if timestamp is None:
        raise ValueError("event 消息 timestamp 非法（需 Unix epoch 秒）")
    try:
        event_type = EventType(str(payload.get("event_type")))
        event_level = EventLevel(str(payload.get("event_level")))
    except ValueError as exc:
        raise ValueError(
            f"event_type/event_level 不在受控词表："
            f"{payload.get('event_type')}/{payload.get('event_level')}"
        ) from exc
    expected_level = EVENT_LEVEL_BY_TYPE[event_type]
    if event_level != expected_level:
        raise ValueError(
            f"event_level 与事件类型契约等级不一致（{event_type.value}："
            f"期望 {expected_level.value}，实际 {event_level.value}）"
        )
    current = time.time() if now is None else now
    if timestamp > current + MAX_FUTURE_SKEW_SECONDS:
        raise ValueError("event 时间戳超前（时钟异常）")
    if timestamp < current - MAX_BACKFILL_SECONDS:
        raise ValueError("event 时间戳超出可接收窗口（> 7 天补传）")
    row: dict[str, Any] = {
        "vehicle_id": vehicle_id,
        "event_type": event_type,
        "event_level": event_level,
        "event_time": datetime.fromtimestamp(timestamp, tz=UTC),
        "description": payload.get("description"),
        "data_json": dict(payload.get("data") or {}),
        "data_file_url": payload.get("data_file_url"),
        "acknowledged": False,
        "acknowledged_by": None,
        "acknowledge_time": None,
    }
    return PreparedEvent(vehicle_id=vehicle_id, row=row, message=dict(payload))


@dataclass(slots=True)
class IngestStats:
    """采集统计（可观测性与用例断言）。"""

    received: int = 0
    accepted: int = 0
    dropped: int = 0
    cleaned: int = 0
    inserted: int = 0
    published: int = 0


class TelemetryPublisherProtocol(Protocol):
    """投递能力协议（raw/clean 投递；实现见 ``app/producers/pipeline.py``）。"""

    async def publish_telemetry(
        self, topic: str, payload: Mapping[str, Any], vehicle_id: str
    ) -> None:  # pragma: no cover - 协议声明
        ...


class TelemetryIngestService:
    """遥测批量入库与 raw/clean 投递（第 5-6 步）。

    缓冲策略：``handle()`` 累积样本，达 ``telemetry_batch_size`` 时尝试冲刷；
    冲刷失败**不抛出**（由消费者批次收尾钩子重试并阻止 offset 提交，
    避免单条消息因批量写入失败被误判为 handler 错误而进 DLQ）。
    """

    def __init__(
        self,
        repository: TelemetryRepositoryProtocol,
        publisher: TelemetryPublisherProtocol,
        settings: Settings,
    ) -> None:
        self._repository = repository
        self._publisher = publisher
        self._settings = settings
        self._buffer: list[PreparedTelemetry] = []
        self._stats = IngestStats()

    @property
    def stats(self) -> IngestStats:
        """采集统计快照。"""
        return self._stats

    @property
    def pending(self) -> int:
        """待冲刷样本数。"""
        return len(self._buffer)

    async def handle(self, payload: Mapping[str, Any], *, topic: str | None = None) -> bool:
        """单条消息入口（流水线第 2-5 步 + 批量累积）。

        Returns:
            True = 样本被接收（已入缓冲）；False = 样本被丢弃（非法/时钟异常），
            丢弃不视为 handler 失败（不重试、不进 DLQ：脏数据重放不会自愈）。
        """
        self._stats.received += 1
        prepared = prepare_telemetry(payload)
        if prepared is None:
            self._stats.dropped += 1
            logger.warning(
                "telemetry_sample_dropped",
                topic=topic,
                vehicle_id=payload.get("vehicle_id"),
                reason="invalid_or_clock_skew",
            )
            return False
        if prepared.cleaned:
            self._stats.cleaned += 1
        self._stats.accepted += 1
        self._buffer.append(prepared)
        if len(self._buffer) >= self._settings.telemetry_batch_size:
            await self._flush_safely()
        return True

    async def flush(self) -> int:
        """冲刷缓冲：批量入库（幂等）→ 投递 raw/clean；返回本批样本数。

        异常向上抛出（供消费者批次收尾钩子捕获 → 跳过 offset 提交 → 消息重投）。
        """
        if not self._buffer:
            return 0
        batch, self._buffer = self._buffer, []
        rows = [item.row for item in batch]
        try:
            async with self._repository.transaction() as session:
                inserted = await self._repository.insert_points(session, rows)
            # 先落库后投递：保证 telemetry_clean 的下游（Flink）不会读到早于入库的乱序数据
            await self._publish_batch(batch)
        except Exception:
            # 失败回填缓冲：批次收尾钩子可立即重试；即便进程崩溃，
            # 未提交的 offset 也会触发重投（写侧 ON CONFLICT 幂等，不产生重复点）
            self._buffer = batch + self._buffer
            logger.warning("telemetry_flush_requeued", requeued=len(batch))
            raise
        self._stats.inserted += inserted
        self._stats.published += len(batch) * 2
        logger.info(
            "telemetry_batch_flushed",
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
            logger.exception("telemetry_flush_failed", pending=len(self._buffer))

    async def _publish_batch(self, batch: Sequence[PreparedTelemetry]) -> None:
        """并发投递 raw/clean（受 ``telemetry_publish_concurrency`` 限流）。

        契约 acks=all：单条投递需等待 broker 确认，故以有界并发而非逐条串行，
        避免批量投递耗时突破「入库延迟 ≤ 1s」。
        """
        concurrency = max(1, self._settings.telemetry_publish_concurrency)
        for start in range(0, len(batch), concurrency):
            chunk = batch[start : start + concurrency]
            await asyncio.gather(*(self._publish_one(item) for item in chunk))

    async def _publish_one(self, item: PreparedTelemetry) -> None:
        """投递单条样本的原始与清洗版本（key=vehicle_id，Schema 契约校验）。"""
        await self._publisher.publish_telemetry(
            self._settings.telemetry_raw_topic, item.raw, item.vehicle_id
        )
        await self._publisher.publish_telemetry(
            self._settings.telemetry_clean_topic, item.clean, item.vehicle_id
        )
