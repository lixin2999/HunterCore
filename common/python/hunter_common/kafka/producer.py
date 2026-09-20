"""Kafka 异步生产者封装（confluent-kafka 2.x，契约驱动）。

能力（对齐开发规则「Kafka 生产」与契约 ``topics.yaml``）：

- **单例**：``KafkaProducerManager.initialize(config)`` 后进程内复用，参数取配置（禁止硬编码）；
- **契约 acks**：按 Topic 契约 ``acks`` 选择生产者实例（telemetry=1 / health=0 / 其余=all），
  契约未登记的 Topic（如 ``{topic}.dlq``）回退配置值并告警；
- **消息 key**：契约 ``key=vehicle_id`` 的 Topic 强制 key 与消息体 ``vehicle_id`` 一致
  （``publish_payload`` 入口），保证单车辆消息分区内有序；
- **重试**：契约 ``retries=3`` + 指数退避（仅对可重试错误重试，编程错误立即抛出）；
- **本地磁盘缓冲**：网络中断且重试耗尽时消息落盘（上限 1GB，契约 producer_defaults），
  链路恢复后由 :meth:`KafkaProducerManager.replay_buffered` 重投；
- **异步**：``produce`` 异步等待 delivery 回调，poll 由独立线程驱动，不阻塞事件循环。

低层 ``produce`` 保持宽松（用于 DLQ 等契约外 Topic，既有服务调用方零改动）；
契约驱动的严格入口是 :meth:`publish` / :meth:`publish_payload`（Schema 校验 + key 规则）。
"""
from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass
from typing import Any, Literal

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from hunter_common.config import HunterBaseConfig
from hunter_common.kafka import metrics as kafka_metrics
from hunter_common.kafka.buffer import BufferedRecord, BufferStats, LocalDiskBuffer
from hunter_common.kafka.contracts import KafkaContract, KafkaContractError, get_contract
from hunter_common.kafka.messages import KafkaRecord, build_record
from hunter_common.logging import get_logger

logger = get_logger("hunter_common.kafka.producer")

#: 投递结果：delivered = broker 已确认；buffered = 已落盘待重投
ProduceStatus = Literal["delivered", "buffered"]

#: 退避指数上限（2^10 足够覆盖退避上限，防止位移溢出）
_MAX_BACKOFF_EXPONENT = 10


@dataclass(frozen=True, slots=True)
class ProduceResult:
    """投递结果（调用方据此判断是否需要业务重试/告警）。"""

    topic: str
    status: ProduceStatus
    attempts: int
    partition: int | None = None
    offset: int | None = None


class KafkaProducerManager:
    """进程级 Kafka 生产者（单例，按契约 acks 维护多个底层 Producer 实例）。"""

    _instance: KafkaProducerManager | None = None
    _init_lock = threading.Lock()

    def __init__(self, config: HunterBaseConfig, *, contract: KafkaContract | None = None) -> None:
        self._config = config
        self._service = config.service_name
        self._contract = (
            contract if contract is not None else get_contract(base_dir=config.kafka_contract_dir)
        )
        self._producers: dict[str, Producer] = {}
        self._producer_lock = threading.Lock()
        self._buffer: LocalDiskBuffer | None = None
        if config.kafka_local_buffer_enabled:
            self._buffer = LocalDiskBuffer(
                config.kafka_local_buffer_dir,
                service=self._service,
                max_bytes=config.kafka_local_buffer_max_bytes,
                segment_max_records=config.kafka_local_buffer_segment_max_records,
            )
        self._closed = False
        # 预建默认 acks 生产者：配置非法时启动即失败（fail fast），而不是首条消息才暴露
        self._producer_for(config.kafka_producer_acks)
        # 后台线程驱动投递回调（Producer 线程安全，poll 不阻塞事件循环）
        self._poll_thread = threading.Thread(
            target=self._poll_loop, name="kafka-producer-poll", daemon=True
        )
        self._poll_thread.start()

    # ---------- 单例管理 ----------

    @classmethod
    def initialize(
        cls, config: HunterBaseConfig, *, contract: KafkaContract | None = None
    ) -> KafkaProducerManager:
        """初始化全局单例（应用启动时调用一次，幂等）。"""
        with cls._init_lock:
            if cls._instance is None:
                cls._instance = cls(config, contract=contract)
            return cls._instance

    @classmethod
    def instance(cls) -> KafkaProducerManager:
        """获取全局单例（未初始化则抛出 RuntimeError）。"""
        if cls._instance is None:
            raise RuntimeError("KafkaProducerManager 未初始化，请先调用 initialize(config)")
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """重置单例（仅测试使用）。"""
        with cls._init_lock:
            cls._instance = None

    # ---------- 契约与配置 ----------

    @property
    def contract(self) -> KafkaContract | None:
        """已加载的 Kafka 契约（未定位到契约目录时为 None）。"""
        return self._contract

    @property
    def buffer(self) -> LocalDiskBuffer | None:
        """本地磁盘缓冲（``kafka_local_buffer_enabled=False`` 时为 None）。"""
        return self._buffer

    async def buffer_stats_async(self) -> BufferStats | None:
        """缓冲水位（异步安全：目录扫描下沉线程池；事件循环内必须用本方法）。"""
        if self._buffer is None:
            return None
        return await asyncio.to_thread(self._buffer.stats)

    def buffer_stats(self) -> BufferStats | None:
        """缓冲水位（无缓冲时返回 None）。

        ⚠ 同步阻塞实现（目录 glob）：仅限同步上下文/测试使用；异步路径请用
        :meth:`buffer_stats_async`（审查 R4）。
        """
        return self._buffer.stats() if self._buffer is not None else None

    @staticmethod
    def _build_conf(config: HunterBaseConfig, *, acks: str | None = None) -> dict[str, Any]:
        """构建 confluent-kafka 生产者配置（含 SASL_SSL 支持）。"""
        conf: dict[str, Any] = {
            "bootstrap.servers": ",".join(config.kafka_bootstrap_servers_list),
            "security.protocol": config.kafka_security_protocol,
            "acks": acks or config.kafka_producer_acks,
            "compression.type": config.kafka_compression_type,
            "linger.ms": config.kafka_linger_ms,
            "batch.size": config.kafka_batch_size,
            "retries": config.kafka_retries,
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
        if config.kafka_security_protocol in ("SSL", "SASL_SSL"):
            # 严格 mTLS（设计文档 3.2.3/14.2，G-01）：broker ssl.client.auth=required
            # 时必须出示客户端证书/私钥，否则握手失败
            if config.kafka_ssl_cafile:
                conf["ssl.ca.location"] = config.kafka_ssl_cafile
            if config.kafka_ssl_certfile:
                conf["ssl.certificate.location"] = config.kafka_ssl_certfile
            if config.kafka_ssl_keyfile:
                conf["ssl.key.location"] = config.kafka_ssl_keyfile
            if config.kafka_ssl_keypassword:
                conf["ssl.key.password"] = config.kafka_ssl_keypassword
        return conf

    def _acks_for(self, topic: str, explicit: str | None) -> str:
        """按契约解析 Topic 的 acks（显式 > 契约 > 配置兜底）。"""
        if explicit:
            return explicit
        if self._contract is None:
            return self._config.kafka_producer_acks
        try:
            return self._contract.topic_spec(topic).acks
        except KafkaContractError:
            # 契约外 Topic（典型：DLQ {topic}.dlq）：回退配置值并告警，不中断投递链路
            logger.warning("kafka_topic_not_in_contract", topic=topic)
            return self._config.kafka_producer_acks

    def _producer_for(self, acks: str) -> Producer:
        """取得指定 acks 的生产者实例（按需创建并复用；acks 属生产者级参数）。"""
        with self._producer_lock:
            producer = self._producers.get(acks)
            if producer is None:
                producer = Producer(self._build_conf(self._config, acks=acks))
                self._producers[acks] = producer
                logger.info("kafka_producer_created", acks=acks, service=self._service)
            return producer

    # ---------- 运行 ----------

    def _poll_loop(self) -> None:
        """后台驱动所有生产者实例的 delivery 回调（线程安全，不阻塞事件循环）。"""
        while not self._closed:
            for producer in list(self._producers.values()):
                try:
                    producer.poll(0.05)
                except Exception:  # pragma: no cover - 生产者线程不可因单次异常退出
                    logger.exception("kafka_producer_poll_failed")

    # ---------- 投递 ----------

    async def produce(
        self,
        topic: str,
        value: bytes | str | None,
        *,
        key: str | bytes | None = None,
        headers: list[tuple[str, bytes]] | dict[str, str] | None = None,
        partition: int | None = None,
        acks: str | None = None,
        use_buffer: bool | None = None,
    ) -> ProduceResult:
        """异步投递（低层入口：不做 Schema/key 契约校验，供 DLQ 等契约外 Topic 使用）。

        语义：
        - 成功 → ``status="delivered"``（broker 按契约 acks 确认）；
        - 可重试错误按 ``kafka_produce_max_attempts`` 重试（指数退避），仍失败且缓冲启用 →
          落盘并返回 ``status="buffered"``；
        - 不可重试错误（消息体非法/超长等）或缓冲关闭 → 抛 KafkaException（由调用方转 5001）。
        """
        value_bytes, key_bytes, header_items = _normalize(value, key, headers)
        producer = self._producer_for(self._acks_for(topic, acks))
        max_attempts = max(int(self._config.kafka_produce_max_attempts), 1)
        started = time.perf_counter()
        attempts = 0
        last_error: KafkaException | None = None
        retriable = False
        while attempts < max_attempts:
            attempts += 1
            try:
                partition_id, offset = await self._deliver(
                    producer, topic, value_bytes, key_bytes, header_items, partition
                )
            except KafkaException as exc:
                last_error = exc
                retriable = _is_retriable(exc)
                if not retriable or attempts >= max_attempts:
                    break
                kafka_metrics.record_produce_retry(self._service, topic)
                await asyncio.sleep(self._backoff_seconds(attempts))
                continue
            kafka_metrics.record_produced(
                self._service, topic, "delivered", time.perf_counter() - started
            )
            return ProduceResult(topic, "delivered", attempts, partition_id, offset)

        duration = time.perf_counter() - started
        buffering = self._config.kafka_local_buffer_enabled if use_buffer is None else use_buffer
        if last_error is not None and retriable and buffering and self._buffer is not None:
            # 落盘为阻塞 IO（open/flush/fsync + 容量淘汰目录扫描）：下沉线程池，
            # 禁止在事件循环内做同步文件 IO（异步优先约束；审查 R4）
            await asyncio.to_thread(
                self._buffer.append,
                BufferedRecord(
                    topic=topic, value=value_bytes, key=key_bytes, headers=header_items
                ),
            )
            kafka_metrics.record_produced(self._service, topic, "buffered", duration)
            logger.error(
                "kafka_produce_buffered_to_disk",
                topic=topic,
                attempts=attempts,
                error=str(last_error),
                hint="链路中断：消息已落盘，恢复后由 replay_buffered() 重投",
            )
            return ProduceResult(topic, "buffered", attempts)
        kafka_metrics.record_produced(self._service, topic, "failed", duration)
        logger.error("kafka_produce_failed", topic=topic, attempts=attempts, error=str(last_error))
        if last_error is not None:
            raise last_error
        raise KafkaException(KafkaError(KafkaError._FAIL))  # pragma: no cover - 防御性兜底

    async def publish(
        self, record: KafkaRecord, *, partition: int | None = None, use_buffer: bool | None = None
    ) -> ProduceResult:
        """投递契约构建的记录（Schema 与 key 规则已在 build_record 阶段校验）。"""
        return await self.produce(
            record.topic,
            record.value,
            key=record.key,
            headers=record.headers_list(),
            partition=partition,
            use_buffer=use_buffer,
        )

    async def publish_payload(
        self,
        topic: str,
        payload: Any,
        *,
        key: str | bytes | None = None,
        headers: dict[str, str] | list[tuple[str, bytes]] | None = None,
        schema_name: str | None = None,
        contract_required: bool = False,
        use_buffer: bool | None = None,
    ) -> ProduceResult:
        """契约驱动投递入口：Schema 校验 + ``key = vehicle_id`` 强制 + 按契约 acks 投递。

        Raises:
            KafkaMessageSchemaError: 消息体不符合契约 Schema 或 key 与 vehicle_id 不一致（2001）。
            KafkaContractError: Topic/Schema 未登记或契约不可用（5000）。
        """
        record = build_record(
            topic,
            payload,
            key=key,
            headers=headers,
            schema_name=schema_name,
            contract=self._contract,
            contract_required=contract_required,
        )
        return await self.publish(record, use_buffer=use_buffer)

    async def replay_buffered(self, *, max_records: int | None = None) -> int:
        """重投本地磁盘缓冲（链路恢复后调用；返回重投成功条数）。

        以最旧优先重投，失败即停并保留剩余记录（保证分区内顺序）。
        """
        if self._buffer is None:
            return 0

        async def _send(buffered: BufferedRecord) -> None:
            producer = self._producer_for(self._acks_for(buffered.topic, None))
            await self._deliver(
                producer,
                buffered.topic,
                buffered.value,
                buffered.key,
                buffered.headers,
                None,
            )
            kafka_metrics.record_produced_replay(self._service, buffered.topic)

        return await self._buffer.replay(_send, max_records=max_records)

    # ---------- 生命周期 ----------

    async def flush(self, timeout: float = 10.0) -> None:
        """等待本地缓冲消息全部投递完成（线程池执行，不阻塞事件循环）。"""
        loop = asyncio.get_running_loop()
        for acks, producer in list(self._producers.items()):
            remaining = int(await loop.run_in_executor(None, producer.flush, timeout))
            if remaining:
                logger.warning("kafka_flush_incomplete", acks=acks, remaining=remaining)

    async def close(self) -> None:
        """关闭生产者（应用关闭时调用）：先尽力重投缓冲，再停止 poll 线程并 flush。"""
        if self._closed:
            return
        try:
            # 关闭前尽力重投：poll 线程仍在运行，回调可正常驱动
            replayed = await self.replay_buffered()
            if replayed:
                logger.info("kafka_buffer_replayed_on_close", replayed=replayed)
        except Exception:  # noqa: BLE001 - 关闭期链路仍不可用属预期：缓冲保留，下次启动继续重投
            logger.warning("kafka_buffer_replay_on_close_failed")
        self._closed = True
        self._poll_thread.join(timeout=2)
        await self.flush()
        stats = await self.buffer_stats_async()
        if stats is not None and stats.message_count:
            logger.warning(
                "kafka_producer_closed_with_pending_buffer",
                messages=stats.message_count,
                bytes=stats.bytes_size,
                directory=str(self._buffer.directory) if self._buffer is not None else "",
            )
        logger.info("kafka_producer_closed")

    # ---------- 内部 ----------

    async def _deliver(
        self,
        producer: Producer,
        topic: str,
        value: bytes | None,
        key: bytes | None,
        headers: tuple[tuple[str, bytes], ...],
        partition: int | None,
    ) -> tuple[int | None, int | None]:
        """单次投递并等待 delivery 回调，返回 ``(partition, offset)``。"""
        loop = asyncio.get_running_loop()
        future: asyncio.Future[tuple[int | None, int | None]] = loop.create_future()

        def _on_delivery(err: KafkaError | None, msg: Message | None) -> None:
            """delivery 回调（poll 线程执行），线程安全地回写 Future。"""
            if future.done():
                return
            if err is not None:
                loop.call_soon_threadsafe(future.set_exception, KafkaException(err))
            else:
                loop.call_soon_threadsafe(
                    future.set_result,
                    (msg.partition() if msg is not None else None, msg.offset() if msg else None),
                )

        kwargs: dict[str, Any] = {"on_delivery": _on_delivery}
        if key is not None:
            kwargs["key"] = key
        if headers:
            kwargs["headers"] = list(headers)
        if partition is not None:
            kwargs["partition"] = partition

        try:
            producer.produce(topic, value=value, **kwargs)
        except BufferError:
            # 本地队列满（背压）：flush 腾出空间后重试一次，避免阻塞事件循环
            logger.warning("kafka_produce_backpressure", topic=topic)
            await loop.run_in_executor(None, producer.flush, 5)
            producer.produce(topic, value=value, **kwargs)
        return await future

    def _backoff_seconds(self, attempt: int) -> float:
        """指数退避：base * 2^(attempt-1)，不超过 ``kafka_produce_retry_backoff_max_ms``。"""
        base_ms = max(int(self._config.kafka_produce_retry_backoff_ms), 1)
        capped = min(
            base_ms << min(attempt - 1, _MAX_BACKOFF_EXPONENT),
            max(int(self._config.kafka_produce_retry_backoff_max_ms), 1),
        )
        return capped / 1000.0


# ---------- 模块级辅助 ----------

def _normalize(
    value: bytes | str | None,
    key: str | bytes | None,
    headers: list[tuple[str, bytes]] | dict[str, str] | None,
) -> tuple[bytes | None, bytes | None, tuple[tuple[str, bytes], ...]]:
    """value/key/headers 归一为字节形态（str 一律 UTF-8 编码）。"""
    value_bytes = value.encode("utf-8") if isinstance(value, str) else value
    key_bytes: bytes | None = key.encode("utf-8") if isinstance(key, str) else key
    if not headers:
        return value_bytes, key_bytes, ()
    if isinstance(headers, dict):
        return (
            value_bytes,
            key_bytes,
            tuple((str(name), item.encode("utf-8")) for name, item in headers.items()),
        )
    return value_bytes, key_bytes, tuple((str(name), item) for name, item in headers)


def _is_retriable(error: KafkaException) -> bool:
    """是否为可重试的 Kafka 错误（网络/超时/leader 切换；取 broker 侧 retriable 标志）。"""
    kafka_error = error.args[0] if error.args else None
    retriable = getattr(kafka_error, "retriable", None)
    return bool(retriable()) if callable(retriable) else False



