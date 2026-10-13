"""DLT 死信重放工具（§5.3 死信主题 + §12.3 replay 工具）。

背景：``app.consumers.base._route_dlt`` 把解析/字段级数据错误消息连同错误上下文
（``original_topic``、原始 envelope、``error_type``、``error_message``、``failed_at``）
路由到 ``ingest.dlt``。故障根因修复后，本工具从 DLT 主题读回原始 envelope 并重新投递
到其 ``original_topic``，让消息重走一遍正常消费链路——原事务已回滚（events 层1去重
未落库），重放会被当作新事件重新处理。

设计约束：
- 仅重放「原始消息为合法 JSON 对象」的记录；``{"_raw_repr": ...}``（非对象消息）无法作
  envelope 重放，计入 skipped；
- 支持按 ``original_topic`` / ``error_type`` / ``failed_at`` 时间窗过滤 + 限量 + dry-run 预览；
- 消费者/生产者均可注入（单测用替身；CLI 用真实 ``confluent_kafka`` + ``KafkaProducerManager``）；
- 重放以 ``vehicle_id`` 为 key（契约 key=vehicle_id，保证单车辆分区内有序），缺失回退原 offset。

CLI：``python -m app.tools.dlt_replay --dry-run --max 200``（详见 ``--help``）。
"""
from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import Consumer, Message
from hunter_common.kafka.consumer import KafkaConsumerManager
from hunter_common.kafka.producer import KafkaProducerManager
from hunter_common.logging import configure_logging, get_logger

from app.config import Settings

logger = get_logger("app.tools.dlt_replay")

#: 默认重放消费组（独立于在线消费者组，避免干扰其位点）
DEFAULT_REPLAY_GROUP = "g.ingest.dlt-replay"


@dataclass(frozen=True, slots=True)
class ReplayFilter:
    """DLT 记录筛选条件（None 表示该维度不过滤）。"""

    original_topic: str | None = None
    error_type: str | None = None
    since: datetime | None = None
    until: datetime | None = None
    #: 匹配计数达到该上限即停止扫描/重放
    max_records: int | None = None


@dataclass
class ReplayReport:
    """重放结果统计。"""

    dry_run: bool = False
    scanned: int = 0
    matched: int = 0
    replayed: int = 0
    skipped: int = 0
    failed: int = 0
    by_topic: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "dry_run": self.dry_run,
            "scanned": self.scanned,
            "matched": self.matched,
            "replayed": self.replayed,
            "skipped": self.skipped,
            "failed": self.failed,
            "by_topic": dict(self.by_topic),
        }


# ---------- 纯函数（可单测） ----------

def extract_replay_target(record: Any) -> tuple[str, dict[str, Any]] | None:
    """从 DLT 记录提取 ``(target_topic, envelope)``；非法/不可重放返回 None。

    不可重放：非对象记录、缺 ``original_topic``、原始消息非 JSON 对象
    （``_route_dlt`` 对非对象消息以 ``{"_raw_repr": ...}`` 存证，无法作为 envelope 重投）。
    """
    if not isinstance(record, dict):
        return None
    topic = record.get("original_topic")
    if not isinstance(topic, str) or not topic:
        return None
    payload = record.get("message")
    if not isinstance(payload, dict) or "_raw_repr" in payload:
        return None
    return topic, payload


def _parse_failed_at(raw: Any) -> datetime | None:
    """解析 ``failed_at``（ISO 字符串）；无时区视为 UTC，非法返回 None。"""
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=UTC)
    if isinstance(raw, str):
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return None


def record_matches(record: dict[str, Any], filt: ReplayFilter) -> bool:
    """按筛选条件判定 DLT 记录是否应被重放。"""
    if filt.original_topic and record.get("original_topic") != filt.original_topic:
        return False
    if filt.error_type and record.get("error_type") != filt.error_type:
        return False
    if filt.since or filt.until:
        ts = _parse_failed_at(record.get("failed_at"))
        if ts is not None:  # 无法解析时间戳的记录保守放行
            if filt.since and ts < filt.since:
                return False
            if filt.until and ts > filt.until:
                return False
    return True


def replay_key(payload: dict[str, Any], record: dict[str, Any]) -> str:
    """重放 key：优先消息体 ``vehicle_id``（契约单车辆有序），回退原 offset。"""
    vehicle_id = payload.get("vehicle_id")
    if vehicle_id:
        return str(vehicle_id)
    offset = record.get("original_offset")
    return str(offset) if offset is not None else ""


def decode_record(message: Message) -> dict[str, Any] | None:
    """解码 DLT Kafka 消息体为记录 dict；非法 JSON/非对象返回 None。"""
    raw = message.value()
    if raw is None:
        return None
    text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
    try:
        obj = json.loads(text)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    return obj if isinstance(obj, dict) else None


# ---------- 重放引擎 ----------

class DltReplayEngine:
    """从 DLT 主题读回原始 envelope 并重投到 original_topic 的执行引擎。

    消费者须实现 ``subscribe(list)`` / ``consume(num, timeout) -> list[Message]`` /
    ``commit()`` / ``close()``（真实 confluent_kafka.Consumer 或单测替身）；
    生产者须实现 ``async produce(topic, *, key, value)``（KafkaProducerManager 或替身）。
    """

    def __init__(
        self,
        settings: Settings,
        *,
        consumer: Consumer,
        producer: Any,
        dry_run: bool = False,
        poll_batch: int = 100,
        poll_timeout: float = 1.0,
        idle_rounds: int = 3,
    ) -> None:
        self._settings = settings
        self._consumer = consumer
        self._producer = producer
        self._dry_run = dry_run
        self._poll_batch = poll_batch
        self._poll_timeout = poll_timeout
        #: 连续空 poll 达该轮数视为读尽（DLT 为有界回填队列，非持续流）
        self._idle_rounds = max(idle_rounds, 1)

    async def run(self, filt: ReplayFilter | None = None) -> ReplayReport:
        filt = filt or ReplayFilter()
        report = ReplayReport(dry_run=self._dry_run)
        loop = asyncio.get_running_loop()
        self._consumer.subscribe([self._settings.dlt_topic])
        logger.info(
            "dlt_replay_started",
            dlt_topic=self._settings.dlt_topic, dry_run=self._dry_run, filter=asdict(filt),
        )
        empty_rounds = 0
        stop = False
        try:
            while not stop:
                msgs = await loop.run_in_executor(
                    None, self._consumer.consume, self._poll_batch, self._poll_timeout
                )
                if not msgs:
                    empty_rounds += 1
                    if empty_rounds >= self._idle_rounds:
                        break
                    continue
                empty_rounds = 0
                for msg in msgs:
                    await self._handle_message(msg, filt, report)
                    if filt.max_records is not None and report.matched >= filt.max_records:
                        stop = True
                        break
                await loop.run_in_executor(None, self._consumer.commit)
        finally:
            await loop.run_in_executor(None, self._consumer.close)
        logger.info("dlt_replay_finished", **report.as_dict())
        return report

    async def _handle_message(
        self, message: Message, filt: ReplayFilter, report: ReplayReport
    ) -> None:
        if message.error() is not None:
            return
        report.scanned += 1
        record = decode_record(message)
        target = extract_replay_target(record) if record is not None else None
        if target is None or not record_matches(record, filt):  # type: ignore[arg-type]
            report.skipped += 1
            return
        topic, payload = target
        report.matched += 1
        report.by_topic[topic] = report.by_topic.get(topic, 0) + 1
        if self._dry_run:
            return
        key = replay_key(payload, record)  # type: ignore[arg-type]
        try:
            # 低层 produce 只接受 bytes/str：envelope 序列化为 JSON 字符串再投
            await self._producer.produce(
                topic, key=key, value=json.dumps(payload, ensure_ascii=False)
            )
            report.replayed += 1
        except Exception as exc:  # noqa: BLE001 - 单条重放失败不中断整批回填
            report.failed += 1
            logger.error("dlt_replay_produce_failed", topic=topic, key=key, error=str(exc))


# ---------- CLI ----------

def _make_consumer(settings: Settings, group_id: str) -> Consumer:
    """构造 DLT 读取消费者（复用 hunter_common 的 SASL/SSL 配置装配）。"""
    conf = KafkaConsumerManager._build_conf(settings, group_id)
    conf["auto.offset.reset"] = settings.kafka_auto_offset_reset
    return Consumer(conf)


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dlt_replay",
        description="ingest.dlt 死信重放工具（§5.3/§12.3）：读回原始消息并重投到其原 Topic。",
    )
    parser.add_argument("--dry-run", action="store_true", help="仅统计将要重放的记录，不实际投递")
    parser.add_argument("--topic", dest="topic", default=None, help="仅重放指定 original_topic")
    parser.add_argument("--error-type", dest="error_type", default=None, help="仅重放指定错误类型")
    parser.add_argument("--since", dest="since", default=None, help="failed_at 下界（ISO8601）")
    parser.add_argument("--until", dest="until", default=None, help="failed_at 上界（ISO8601）")
    parser.add_argument("--max", dest="max", type=int, default=None, help="匹配达该条数即停止")
    parser.add_argument("--group-id", dest="group_id", default=DEFAULT_REPLAY_GROUP,
                        help=f"重放消费组（默认 {DEFAULT_REPLAY_GROUP}）")
    parser.add_argument("--json", dest="as_json", action="store_true", help="以 JSON 输出统计报告")
    return parser


def _filter_from_args(args: argparse.Namespace) -> ReplayFilter:
    return ReplayFilter(
        original_topic=args.topic,
        error_type=args.error_type,
        since=_parse_iso(args.since),
        until=_parse_iso(args.until),
        max_records=args.max,
    )


async def _amain(args: argparse.Namespace, settings: Settings) -> int:
    KafkaProducerManager.initialize(settings)
    producer = KafkaProducerManager.instance()
    consumer = _make_consumer(settings, args.group_id)
    engine = DltReplayEngine(settings, consumer=consumer, producer=producer, dry_run=args.dry_run)
    report = await engine.run(_filter_from_args(args))
    await producer.flush()
    if args.as_json:
        print(json.dumps(report.as_dict(), ensure_ascii=False))
    else:
        scope = "（dry-run 预览，未实际投递）" if args.dry_run else ""
        print(f"DLT 重完成{scope}：扫描 {report.scanned} / 匹配 {report.matched} / "
              f"重放 {report.replayed} / 跳过 {report.skipped} / 失败 {report.failed}")
        for topic, count in sorted(report.by_topic.items()):
            print(f"  - {topic}: {count}")
    return 1 if report.failed else 0


def main(argv: Iterable[str] | None = None, *, settings: Settings | None = None) -> int:
    settings = settings or _load_settings()
    configure_logging(settings.service_name, settings.log_level,
                      json_output=settings.environment != "dev")
    args = _build_arg_parser().parse_args(list(argv) if argv is not None else None)
    return asyncio.run(_amain(args, settings))


def _load_settings() -> Settings:
    """CLI 独立进程加载配置（env/.env 由 pydantic-settings 读取）。"""
    from app.config import settings as global_settings

    return global_settings


if __name__ == "__main__":  # pragma: no cover - CLI 入口
    raise SystemExit(main())

__all__ = [
    "DEFAULT_REPLAY_GROUP",
    "DltReplayEngine",
    "ReplayFilter",
    "ReplayReport",
    "decode_record",
    "extract_replay_target",
    "main",
    "record_matches",
    "replay_key",
]
