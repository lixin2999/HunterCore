"""Kafka 异步消费者封装（confluent-kafka 2.x，契约驱动）。

能力（对齐开发规则「Kafka 消费」与契约 ``consumer-groups.yaml``）：

- **手动提交**：``enable.auto.commit=false``，整批处理（含 DLQ 转投）成功后才 commit；
- **契约校验**：``schema_name`` 启用后按 ``contracts/kafka/schemas`` 校验消息体，
  非法消息直接进 DLQ（``reason=schema_invalid``，不重试——数据本身不会自愈）；
- **重试 + 死信队列**：handler 失败按 ``kafka_consumer_max_attempts`` 指数退避重试，
  耗尽后转投 ``{topic}.dlq``（``reason=handler_error``，保留原 topic/partition/offset 溯源头）；
- **幂等**：可注入 :class:`IdempotencyGuard` + 幂等键提取函数，重复消息跳过 handler
  （契约 ``defaults.idempotency=required``）；
- **消费延迟监控**：每批处理完刷新 ``hunter_kafka_consumer_lag``（高水位 - 当前位点，按分区）；
- **异步**：阻塞调用（consume/commit/watermark）全部在线程池执行，不阻塞事件循环。
"""
from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from typing import Any, Final, cast

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from hunter_common.config import HunterBaseConfig
from hunter_common.kafka import metrics as kafka_metrics
from hunter_common.kafka.contracts import (
    KafkaContract,
    KafkaContractError,
    KafkaMessageSchemaError,
    get_contract,
)
from hunter_common.kafka.idempotency import IdempotencyGuard
from hunter_common.kafka.messages import parse_payload
from hunter_common.logging import get_logger

logger = get_logger("hunter_common.kafka.consumer")

#: 消息处理函数签名：(原始 Message, 解码后的值) -> None
MessageHandler = Callable[[Message, Any], Awaitable[None]]

#: 幂等键提取函数签名：(原始 Message, 解码后的值) -> 幂等键
IdempotencyKeyFn = Callable[[Message, Any], str]

#: schema_name 特殊值：按消息实际 Topic 自动解析契约 Schema
SCHEMA_AUTO: Final[str] = "auto"

#: DLQ 溯源头（保留原始位置与失败原因，便于人工排查与重放）
DLQ_REASON_HEADER: Final[str] = "dlq.reason"
DLQ_ERROR_HEADER: Final[str] = "dlq.error"

#: DLQ 头中错误信息截断长度（避免大段堆栈污染消息头）
_DLQ_ERROR_MAX_CHARS: Final[int] = 500

#: 退避指数上限（防止位移溢出）
_MAX_BACKOFF_EXPONENT: Final[int] = 10


class KafkaConsumerManager:
    """Kafka 消费者封装（契约 Schema 校验 + 手动提交 + 重试 + DLQ + 积压指标）。"""

    def __init__(
        self,
        config: HunterBaseConfig,
        group_id: str,
        topics: list[str],
        *,
        dlq_enabled: bool = True,
        poll_timeout: float = 1.0,
        batch_size: int = 100,
        schema_name: str | None = None,
        handler_max_attempts: int | None = None,
        retry_backoff_ms: int | None = None,
        idempotency: IdempotencyGuard | None = None,
        idempotency_key: IdempotencyKeyFn | None = None,
        producer_manager: Any | None = None,
        contract: KafkaContract | None = None,
        lag_metrics_enabled: bool | None = None,
        consumer: Consumer | None = None,
    ) -> None:
        """
        Args:
            group_id: 消费者组（须在 ``contracts/kafka/consumer-groups.yaml`` 登记；未登记仅告警）。
            topics: 订阅列表（支持契约正则写法 ``hunter.*.telemetry``，新车接入无需改配置）。
            schema_name: ``None`` = 不做 Schema 校验（兼容既有调用方）；
                ``"auto"`` = 按消息实际 Topic 从契约解析 Schema（契约不可用则构造时抛错）；
                其他值 = 固定 Schema 逻辑名（如 ``telemetry``，不存在则构造时抛错）。
            handler_max_attempts / retry_backoff_ms: 覆盖配置中的重试参数（默认取配置）。
            idempotency / idempotency_key: 同时提供才启用幂等跳过（契约 idempotency=required）。
            producer_manager: DLQ 投递用的生产者（默认取全局单例；测试可注入替身）。
            consumer: 底层消费者实例（测试注入；默认按配置创建）。
        """
        self._config = config
        self._service = config.service_name
        self._group_id = group_id
        self._topics = list(topics)
        self._dlq_enabled = dlq_enabled
        self._poll_timeout = poll_timeout
        self._batch_size = batch_size
        self._schema_name = schema_name
        self._handler_max_attempts = max(
            handler_max_attempts or config.kafka_consumer_max_attempts, 1
        )
        self._retry_backoff_ms = retry_backoff_ms or config.kafka_consumer_retry_backoff_ms
        self._idempotency = idempotency
        self._idempotency_key = idempotency_key
        self._producer_manager = producer_manager
        self._lag_metrics_enabled = (
            config.kafka_consumer_lag_metrics_enabled
            if lag_metrics_enabled is None
            else lag_metrics_enabled
        )
        self._contract = (
            contract if contract is not None else get_contract(base_dir=config.kafka_contract_dir)
        )
        self._validate_schema_config()
        self._warn_unregistered_group()
        self._consumer = (
            consumer if consumer is not None else Consumer(self._build_conf(config, group_id))
        )
        self._running = False

    # ---------- 配置 ----------

    def _validate_schema_config(self) -> None:
        """Schema 配置 fail fast：显式 Schema 必须存在；auto 模式必须有契约。"""
        if not self._schema_name:
            return
        if self._schema_name == SCHEMA_AUTO:
            if self._contract is None:
                raise KafkaContractError(
                    "schema_name='auto' 需要可用的 Kafka 契约，请设置 KAFKA_CONTRACT_DIR"
                )
            return
        if self._contract is not None:
            self._contract.schema(self._schema_name)  # 不存在 → KafkaContractError（fail fast）

    def _warn_unregistered_group(self) -> None:
        """消费组未登记则告警（禁止自造消费组名上线，同时保留测试/运维临时组能力）。"""
        if self._contract is not None and self._contract.try_consumer_group(self._group_id) is None:
            logger.warning(
                "kafka_consumer_group_not_in_contract",
                group_id=self._group_id,
                hint="消费组须登记到 contracts/kafka/consumer-groups.yaml",
            )

    @staticmethod
    def _build_conf(config: HunterBaseConfig, group_id: str) -> dict[str, Any]:
        """构建 confluent-kafka 消费者配置（含 SASL_SSL 支持）。"""
        conf: dict[str, Any] = {
            "bootstrap.servers": ",".join(config.kafka_bootstrap_servers_list),
            "group.id": group_id,
            "security.protocol": config.kafka_security_protocol,
            "enable.auto.commit": False,  # 手动提交：处理成功后 commit（契约 consumer_defaults）
            "auto.offset.reset": "earliest",
            "client.id": config.service_name,
        }
        if config.kafka_security_protocol in ("SASL_SSL", "SASL_PLAINTEXT"):
            conf.update(
                {
                    "sasl.mechanisms": config.kafka_sasl_mechanism,
                    "sasl.username": config.kafka_sasl_username,
                    "sasl.password": config.kafka_sasl_password,
                }
            )
        if config.kafka_security_protocol in ("SSL", "SASL_SSL") and config.kafka_ssl_cafile:
            conf["ssl.ca.location"] = config.kafka_ssl_cafile
        return conf

    # ---------- 消费主循环 ----------

    async def run(self, handler: MessageHandler) -> None:
        """消费主循环；单条消息处理异常不中断消费（重试耗尽后转入 DLQ）。

        Args:
            handler: 异步处理函数，参数为 ``(Message, 解码后的值)``；
                解码与契约校验见 :meth:`_decode_and_validate`。
        """
        self._consumer.subscribe(self._topics)
        self._running = True
        loop = asyncio.get_running_loop()
        logger.info(
            "kafka_consumer_started",
            topics=self._topics,
            group_id=self._group_id,
            schema=self._schema_name or "-",
            idempotency=self._idempotency is not None,
        )
        try:
            while self._running:
                # consume() 为阻塞调用，放入线程池避免阻塞事件循环（异步优先约束）
                messages = await loop.run_in_executor(
                    None, self._consumer.consume, self._batch_size, self._poll_timeout
                )
                if not messages:
                    continue
                for message in messages:
                    await self._handle_message(message, handler)
                # 批次处理完成（含 DLQ 转投）后手动提交 offset（at-least-once 语义）
                await loop.run_in_executor(None, self._commit)
                await loop.run_in_executor(None, self._refresh_lag)
        finally:
            await loop.run_in_executor(None, self._close)
            logger.info("kafka_consumer_stopped", topics=self._topics, group_id=self._group_id)

    def stop(self) -> None:
        """请求停止消费循环（优雅停机：当前批次处理完成后退出）。"""
        self._running = False

    # ---------- 单条消息处理 ----------

    async def _handle_message(self, message: Message, handler: MessageHandler) -> None:
        """单条消息：契约校验 → 幂等认领 → handler（重试）→ 失败转 DLQ。"""
        error = message.error()
        if error is not None:
            if error.code() == KafkaError._PARTITION_EOF:
                return
            logger.error("kafka_message_error", code=error.code(), error=str(error))
            return
        topic = str(message.topic())
        started = time.perf_counter()
        try:
            value = self._decode_and_validate(message)
        except (KafkaMessageSchemaError, KafkaContractError) as exc:
            kafka_metrics.record_consumed(
                self._service, self._group_id, topic, "schema_invalid", time.perf_counter() - started
            )
            logger.error(
                "kafka_message_schema_invalid",
                topic=topic,
                partition=message.partition(),
                offset=message.offset(),
                error=str(exc),
            )
            await self._send_to_dlq(message, "schema_invalid", exc)
            return
        if not await self._claim_message(message, value):
            kafka_metrics.record_consumed(
                self._service,
                self._group_id,
                topic,
                "skipped_duplicate",
                time.perf_counter() - started,
            )
            return
        try:
            await self._invoke_handler(message, value, handler)
        except Exception as exc:
            kafka_metrics.record_consumed(
                self._service, self._group_id, topic, "handler_failed", time.perf_counter() - started
            )
            logger.exception(
                "kafka_message_process_failed",
                topic=topic,
                partition=message.partition(),
                offset=message.offset(),
            )
            await self._send_to_dlq(message, "handler_error", exc)
            return
        kafka_metrics.record_consumed(
            self._service, self._group_id, topic, "processed", time.perf_counter() - started
        )

    def _decode_and_validate(self, message: Message) -> Any:
        """解码消息体；启用契约校验时，不通过抛 :class:`KafkaMessageSchemaError`。"""
        topic = str(message.topic())
        schema_name = self._resolve_schema_name(topic)
        if schema_name:
            return parse_payload(
                topic,
                message.value(),
                schema_name=schema_name,
                contract=self._contract,
                contract_required=True,
            )
        return self._decode(message)

    def _resolve_schema_name(self, topic: str) -> str | None:
        """``auto`` 模式按消息实际 Topic 解析契约 Schema；契约外 Topic 告警后放行（不误判非法消息）。"""
        if not self._schema_name:
            return None
        if self._schema_name != SCHEMA_AUTO:
            return self._schema_name
        assert self._contract is not None  # 构造期已保证（_validate_schema_config）
        spec = self._contract.try_topic_spec(topic)
        if spec is None:
            logger.warning("kafka_message_topic_not_in_contract", topic=topic)
            return None
        return spec.schema_name

    async def _claim_message(self, message: Message, value: Any) -> bool:
        """幂等认领：False = 重复消息（跳过 handler，不产生副作用）。"""
        if self._idempotency is None or self._idempotency_key is None:
            return True
        key = str(self._idempotency_key(message, value))
        if await self._idempotency.claim(key):
            return True
        logger.info(
            "kafka_duplicate_message_skipped",
            group_id=self._group_id,
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
            idempotency_key=key,
        )
        return False

    async def _invoke_handler(self, message: Message, value: Any, handler: MessageHandler) -> None:
        """调用 handler 并按配置指数退避重试（耗尽后向上抛出，由调用方转 DLQ）。"""
        attempt = 0
        while True:
            attempt += 1
            try:
                await handler(message, value)
                return
            except Exception:
                if attempt >= self._handler_max_attempts:
                    raise
                logger.warning(
                    "kafka_handler_retry",
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset(),
                    attempt=attempt,
                    max_attempts=self._handler_max_attempts,
                )
                await asyncio.sleep(self._backoff_seconds(attempt))

    # ---------- DLQ / 提交 / 指标 ----------

    async def _send_to_dlq(
        self, message: Message, reason: str, error: BaseException | None = None
    ) -> None:
        """转投死信队列 ``{topic}.dlq``（保留原位置与失败原因，便于排查与人工重放）。"""
        topic = str(message.topic())
        kafka_metrics.record_dlq(self._service, self._group_id, topic, reason)
        if not self._dlq_enabled:
            logger.error("kafka_dlq_disabled", topic=topic, reason=reason)
            return
        producer = self._resolve_producer()
        if producer is None:
            # 生产者未初始化：仅记录日志，避免 DLQ 转投失败掩盖原始业务异常
            logger.error("dlq_producer_not_initialized", topic=topic, reason=reason)
            return
        headers: list[tuple[str, bytes]] = [
            ("dlq.original.topic", topic.encode("utf-8")),
            ("dlq.partition", str(message.partition()).encode("utf-8")),
            ("dlq.offset", str(message.offset()).encode("utf-8")),
            (DLQ_REASON_HEADER, reason.encode("utf-8")),
        ]
        if error is not None:
            detail = f"{type(error).__name__}: {error}"[:_DLQ_ERROR_MAX_CHARS]
            headers.append((DLQ_ERROR_HEADER, detail.encode("utf-8")))
        try:
            await producer.produce(
                self._dlq_topic(topic),
                value=message.value(),
                key=message.key(),
                headers=headers,
            )
            logger.warning(
                "kafka_message_sent_to_dlq",
                topic=topic,
                partition=message.partition(),
                offset=message.offset(),
                reason=reason,
            )
        except KafkaException:
            logger.exception("kafka_dlq_produce_failed", topic=topic, reason=reason)

    def _resolve_producer(self) -> Any | None:
        """DLQ 生产者解析（优先注入替身，其次全局单例；均不可用返回 None）。"""
        if self._producer_manager is not None:
            return self._producer_manager
        from hunter_common.kafka.producer import KafkaProducerManager

        try:
            return KafkaProducerManager.instance()
        except RuntimeError:
            return None

    def _commit(self) -> None:
        """手动提交 offset（批次成功后调用；失败仅告警，下批重投 → at-least-once）。"""
        try:
            self._consumer.commit(asynchronous=False)
        except KafkaException as exc:
            logger.error("kafka_commit_failed", error=str(exc))

    def _refresh_lag(self) -> None:
        """刷新分区消费积压（高水位 - 当前位点）；批次提交后调用，失败不影响消费。"""
        if not self._lag_metrics_enabled:
            return
        try:
            assignment = self._consumer.assignment()
        except KafkaException:  # pragma: no cover - 未分配分区时部分版本抛错
            return
        for partition in assignment or []:
            try:
                _low, high = self._consumer.get_watermark_offsets(
                    partition, timeout=1.0, cached=True
                )
                # confluent-kafka 支持单分区与分区列表两种调用，统一用列表形态
                # （类型桩把返回值标注为 list[TopicPartition]，与实际 list[int] 不符，故做显式 cast）
                raw_positions = self._consumer.position([partition])
                positions = cast("list[int]", raw_positions)
                position = int(positions[0]) if positions else 0
            except KafkaException:  # pragma: no cover - rebalance 竞态下位点可能不可用
                continue
            kafka_metrics.set_consumer_lag(
                self._service,
                self._group_id,
                str(partition.topic),
                int(partition.partition),
                int(high) - position,
            )

    def _close(self) -> None:
        """关闭底层消费者（触发 rebalance 离开消费组并释放分区）。"""
        try:
            self._consumer.close()
        except KafkaException:  # pragma: no cover - 关闭期异常不影响进程退出
            logger.warning("kafka_consumer_close_failed", group_id=self._group_id)

    # ---------- 内部工具 ----------

    def _dlq_topic(self, original_topic: str) -> str:
        """DLQ 命名（契约 topics.yaml#naming.dlq_pattern：``{original_topic}.dlq``）。"""
        return f"{original_topic}.dlq"

    @staticmethod
    def _decode(message: Message) -> Any:
        """未启用契约校验时的解码：优先 JSON，失败回退原始 bytes（由 handler 自行处理）。"""
        raw = message.value()
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return raw

    def _backoff_seconds(self, attempt: int) -> float:
        """指数退避：base * 2^(attempt-1)（毫秒 → 秒），base 取 ``kafka_consumer_retry_backoff_ms``。"""
        base_ms = max(int(self._retry_backoff_ms), 1)
        return (base_ms << min(attempt - 1, _MAX_BACKOFF_EXPONENT)) / 1000.0



