"""Kafka 异步生产者封装（confluent-kafka 2.x）。

- 单例模式：KafkaProducerManager.initialize(config) 后全局复用
- 非阻塞 produce：投递结果通过 delivery 回调写入 asyncio.Future
- 独立后台线程调用 poll() 驱动投递回调，不阻塞事件循环
- 生产者参数对齐设计文档车端生产者基准：lz4 / linger.ms=5 / batch.size=16384 / retries=3
"""
from __future__ import annotations

import asyncio
import threading
from typing import Any

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from hunter_common.config import HunterBaseConfig
from hunter_common.logging import get_logger

logger = get_logger("hunter_common.kafka.producer")


class KafkaProducerManager:
    """进程级 Kafka 生产者（单例）。"""

    _instance: KafkaProducerManager | None = None
    _init_lock = threading.Lock()

    def __init__(self, config: HunterBaseConfig) -> None:
        self._producer = Producer(self._build_conf(config))
        self._closed = False
        # 后台线程驱动投递回调（Producer 线程安全，poll 不阻塞事件循环）
        self._poll_thread = threading.Thread(
            target=self._poll_loop, name="kafka-producer-poll", daemon=True
        )
        self._poll_thread.start()

    # ---------- 单例管理 ----------

    @classmethod
    def initialize(cls, config: HunterBaseConfig) -> KafkaProducerManager:
        """初始化全局单例（应用启动时调用一次，幂等）。"""
        with cls._init_lock:
            if cls._instance is None:
                cls._instance = cls(config)
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

    # ---------- 配置 ----------

    @staticmethod
    def _build_conf(config: HunterBaseConfig) -> dict[str, Any]:
        """构建 confluent-kafka 生产者配置（含 SASL_SSL 支持）。"""
        conf: dict[str, Any] = {
            "bootstrap.servers": ",".join(config.kafka_bootstrap_servers_list),
            "security.protocol": config.kafka_security_protocol,
            "acks": config.kafka_producer_acks,
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
        if config.kafka_security_protocol in ("SSL", "SASL_SSL") and config.kafka_ssl_cafile:
            conf["ssl.ca.location"] = config.kafka_ssl_cafile
        return conf

    # ---------- 运行 ----------

    def _poll_loop(self) -> None:
        while not self._closed:
            self._producer.poll(0.1)

    async def produce(
        self,
        topic: str,
        value: bytes | str | None,
        *,
        key: str | bytes | None = None,
        headers: list[tuple[str, bytes]] | None = None,
        partition: int | None = None,
    ) -> None:
        """异步投递消息；broker 确认（按 acks 配置）后返回，失败抛 KafkaException。

        约束：车端相关 Topic 消息 key 必须传 vehicle_id，保证单车辆消息有序。
        """
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()

        def _on_delivery(err: KafkaError | None, msg: Message | None) -> None:
            """delivery 回调（poll 线程执行），线程安全地回写 Future。"""
            if future.done():
                return
            if err is not None:
                loop.call_soon_threadsafe(future.set_exception, KafkaException(err))
            else:
                loop.call_soon_threadsafe(future.set_result, None)

        kwargs: dict[str, Any] = {"on_delivery": _on_delivery}
        if key is not None:
            kwargs["key"] = key
        if headers is not None:
            kwargs["headers"] = headers
        if partition is not None:
            kwargs["partition"] = partition

        try:
            self._producer.produce(topic, value=value, **kwargs)
        except BufferError:
            # 本地队列满：flush 腾出空间后重试一次（背压保护）
            logger.warning("kafka_produce_backpressure", topic=topic)
            await loop.run_in_executor(None, self._producer.flush, 5)
            self._producer.produce(topic, value=value, **kwargs)
        await future

    async def flush(self, timeout: float = 10.0) -> None:
        """等待本地缓冲消息全部投递完成（线程池执行，不阻塞事件循环）。"""
        remaining = await asyncio.get_running_loop().run_in_executor(
            None, self._producer.flush, timeout
        )
        if remaining:
            logger.warning("kafka_flush_incomplete", remaining=remaining)

    async def close(self) -> None:
        """停止 poll 线程并 flush（应用关闭时调用）。"""
        if self._closed:
            return
        self._closed = True
        self._poll_thread.join(timeout=2)
        await self.flush()
        logger.info("kafka_producer_closed")
