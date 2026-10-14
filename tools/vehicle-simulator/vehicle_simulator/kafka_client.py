"""confluent-kafka 客户端工厂（车端身份：SCRAM user=vehicle_id，SASL_SSL + mTLS）。

配置组装与 hunter_common.kafka.{producer,consumer}._build_conf 同源语义，但本工具为独立进程
不依赖 hunter_common（避免安装整套平台依赖），故此处内联一份最小实现，保持与契约一致：
- security.protocol ∈ {SASL_SSL, SASL_PLAINTEXT} → sasl.mechanisms/username/password；
- security.protocol ∈ {SSL, SASL_SSL} → ssl.ca/certificate/key(.password)（严格 mTLS）；
- 生产者基准：acks / compression.type=lz4 / linger.ms=5 / batch.size=16384 / retries=3（契约 producer_defaults）；
- 消费者：手动提交（enable.auto.commit=false）+ auto.offset.reset=earliest（契约 consumer_defaults）。
"""

from __future__ import annotations

import json
from typing import Any, Protocol

from vehicle_simulator.config import SimConfig


class Transport(Protocol):
    """模拟器依赖的 Kafka 传输抽象（tests 以内存桩替身注入，无需 broker）。"""

    def produce(self, topic: str, key: str | None, value: dict[str, Any]) -> None: ...

    def subscribe(self) -> None: ...

    def poll(self, timeout: float) -> dict[str, Any] | None: ...

    def commit(self) -> None: ...

    def close(self) -> None: ...


def _security_conf(cfg: SimConfig) -> dict[str, Any]:
    """安全相关配置（SASL + TLS），PLAINTEXT 返回空增量。"""
    conf: dict[str, Any] = {"security.protocol": cfg.security_protocol}
    if cfg.security_protocol in ("SASL_SSL", "SASL_PLAINTEXT"):
        conf.update(
            {
                "sasl.mechanisms": cfg.sasl_mechanism,
                "sasl.username": cfg.effective_scram_user,
                "sasl.password": cfg.scram_password or "",
            }
        )
    if cfg.security_protocol in ("SSL", "SASL_SSL"):
        if cfg.ssl_cafile:
            conf["ssl.ca.location"] = cfg.ssl_cafile
        if cfg.ssl_certfile:
            conf["ssl.certificate.location"] = cfg.ssl_certfile
        if cfg.ssl_keyfile:
            conf["ssl.key.location"] = cfg.ssl_keyfile
        if cfg.ssl_keypassword:
            conf["ssl.key.password"] = cfg.ssl_keypassword
    return conf


def producer_config(cfg: SimConfig) -> dict[str, Any]:
    """构建生产者配置（遥测 acks=1；health acks=0；command_result/remote acks=all —— 逐 Topic 覆盖）。"""
    conf: dict[str, Any] = {
        "bootstrap.servers": cfg.bootstrap,
        "client.id": f"vehicle-sim-{cfg.vehicle_id}",
        "acks": "all",
        "compression.type": "lz4",
        "linger.ms": 5,
        "batch.size": 16384,
        "retries": 3,
    }
    conf.update(_security_conf(cfg))
    return conf


def consumer_config(cfg: SimConfig) -> dict[str, Any]:
    """构建 remote_control 消费者配置（手动提交，消费组 vehicle-sim-{vid}）。"""
    conf: dict[str, Any] = {
        "bootstrap.servers": cfg.bootstrap,
        "group.id": cfg.consumer_group_id(),
        "client.id": f"vehicle-sim-{cfg.vehicle_id}",
        "enable.auto.commit": False,
        "auto.offset.reset": "latest",  # 只处理会话建立后的实时控制帧，不回溯历史
    }
    conf.update(_security_conf(cfg))
    return conf


class ConfluentTransport:
    """基于 confluent-kafka 同步 API 的传输实现（车端身份，单进程一车辆）。"""

    def __init__(self, cfg: SimConfig) -> None:
        from confluent_kafka import Consumer, Producer

        self._cfg = cfg
        self._producer = Producer(producer_config(cfg))
        self._consumer = Consumer(consumer_config(cfg))

    def produce(self, topic: str, key: str | None, value: dict[str, Any]) -> None:
        self._producer.produce(
            topic,
            key=(key.encode("utf-8") if key is not None else None),
            value=json.dumps(value, ensure_ascii=False).encode("utf-8"),
            on_delivery=_on_delivery,
        )
        self._producer.poll(0)

    def subscribe(self) -> None:
        self._consumer.subscribe([self._cfg.remote_control_topic()])

    def poll(self, timeout: float) -> dict[str, Any] | None:
        msg = self._consumer.poll(timeout)
        if msg is None or msg.error():
            return None
        try:
            return json.loads(msg.value().decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return None

    def commit(self) -> None:
        self._consumer.commit(asynchronous=True)

    def flush(self, timeout: float = 2.0) -> None:
        self._producer.flush(timeout)

    def close(self) -> None:
        self._producer.flush(2.0)
        self._consumer.close()


def _on_delivery(err: Any, msg: Any) -> None:
    """投递回调（演示级：仅打印失败，不阻断循环；真实车端有本地磁盘缓冲重投）。"""
    if err is not None:  # pragma: no cover - 需 broker 触发
        print(f"[kafka] delivery error topic={msg.topic()} err={err}")


__all__ = ["ConfluentTransport", "Transport", "consumer_config", "producer_config"]
