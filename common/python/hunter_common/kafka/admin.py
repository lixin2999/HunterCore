"""Kafka 管理操作（动态 Topic 创建/删除）。

用途：CARLA 仿真会话的 ``carla.{sim_id}.*`` Topic 不能在 ``create-topics.sh`` 中静态创建
（sim_id 运行时才生成），由 scene-service 在调用 CARLA 创建实例前通过本模块显式创建；
实例终态后延迟删除，删除失败则由 retention 自然清理。

约束：
- broker 关闭 ``auto.create.topics.enable``，生产目标 Topic 必须显式创建；
- 分区/保留时间一律取自 ``contracts/kafka/topics.yaml``，禁止调用方硬编码；
- 创建是幂等的：Topic 已存在返回 False（不报错、不修改已有配置）。
"""
from __future__ import annotations

from typing import Any

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from hunter_common.config import HunterBaseConfig
from hunter_common.kafka.contracts import KafkaContract, TopicSpec, get_contract
from hunter_common.logging import get_logger

logger = get_logger("hunter_common.kafka.admin")


def _admin_config(config: HunterBaseConfig) -> dict[str, Any]:
    """由 HunterBaseConfig 生成 AdminClient 连接配置（与 producer/consumer 同源）。"""
    settings: dict[str, Any] = {
        "bootstrap.servers": ",".join(config.kafka_bootstrap_servers_list),
        "security.protocol": config.kafka_security_protocol,
    }
    if config.kafka_security_protocol in ("SASL_SSL", "SASL_PLAINTEXT"):
        settings["sasl.mechanism"] = config.kafka_sasl_mechanism
        if config.kafka_sasl_username:
            settings["sasl.username"] = config.kafka_sasl_username
        if config.kafka_sasl_password:
            settings["sasl.password"] = config.kafka_sasl_password
    if config.kafka_security_protocol in ("SSL", "SASL_SSL"):
        if config.kafka_ssl_cafile:
            settings["ssl.ca.location"] = config.kafka_ssl_cafile
        if config.kafka_ssl_certfile:
            settings["ssl.certificate.location"] = config.kafka_ssl_certfile
        if config.kafka_ssl_keyfile:
            settings["ssl.key.location"] = config.kafka_ssl_keyfile
        if config.kafka_ssl_keypassword:
            settings["ssl.key.password"] = config.kafka_ssl_keypassword
    return settings


class KafkaAdmin:
    """轻量 Kafka Admin 封装（同步调用；异步服务用 ``asyncio.to_thread`` 包装）。"""

    def __init__(
        self,
        config: HunterBaseConfig,
        *,
        replication_factor: int = 1,
        request_timeout: float = 10.0,
        contract: KafkaContract | None = None,
    ) -> None:
        self._client = AdminClient(_admin_config(config))
        self._replication_factor = max(1, replication_factor)
        self._request_timeout = request_timeout
        self._contract = contract

    @property
    def contract(self) -> KafkaContract:
        if self._contract is None:
            self._contract = get_contract(required=True)
        return self._contract

    def ensure_topic(
        self,
        topic: str,
        *,
        partitions: int,
        retention_ms: int | None = None,
        replication_factor: int | None = None,
    ) -> bool:
        """创建 Topic（已存在返回 False）；返回 True 表示本次新建。"""
        if partitions < 1:
            raise ValueError(f"Topic 分区数必须 ≥ 1：{topic}")
        config: dict[str, str] = {}
        if retention_ms is not None:
            config["retention.ms"] = str(int(retention_ms))
        new_topic = NewTopic(
            topic,
            num_partitions=int(partitions),
            replication_factor=replication_factor or self._replication_factor,
            config=config,
        )
        try:
            future = self._client.create_topics([new_topic], request_timeout=self._request_timeout)[topic]
            future.result(timeout=self._request_timeout)
        except KafkaException as exc:
            error = exc.args[0] if exc.args else None
            if error is not None and error.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                return False
            logger.error("kafka_topic_create_failed", topic=topic, error=str(exc))
            raise
        logger.info("kafka_topic_created", topic=topic, partitions=partitions, retention_ms=retention_ms)
        return True

    def delete_topic(self, topic: str) -> bool:
        """删除 Topic（不存在返回 False）；返回 True 表示本次删除成功。"""
        try:
            future = self._client.delete_topics([topic], request_timeout=self._request_timeout)[topic]
            future.result(timeout=self._request_timeout)
        except KafkaException as exc:
            error = exc.args[0] if exc.args else None
            if error is not None and error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                return False
            logger.warning("kafka_topic_delete_failed", topic=topic, error=str(exc))
            raise
        logger.info("kafka_topic_deleted", topic=topic)
        return True

    def ensure_topic_spec(self, spec: TopicSpec, *, topic: str | None = None) -> bool:
        """按契约条目创建 Topic（名称缺省用模板名；动态模板需调用方显式给 topic）。"""
        target = topic or spec.name
        if "{" in target:
            raise ValueError(f"动态 Topic 模板必须渲染后创建：{target}")
        return self.ensure_topic(
            target,
            partitions=spec.partitions,
            retention_ms=spec.retention_ms,
        )

    def ensure_carla_session(self, sim_id: str) -> list[str]:
        """创建单个 CARLA 会话所需的全部动态 Topic（含 sensor_data.dlq）。

        返回本次实际新建的 Topic 名；已存在的不重复创建。任何一步失败向上抛，
        由 scene-service 转 5001 并阻止实例创建（避免孤儿实例）。
        """
        created: list[str] = []
        dlq_retention = int(
            (self.contract.topics_doc.get("naming") or {}).get("dlq_retention_ms", 2_592_000_000)
        )
        try:
            for template, spec in self.contract.carla_topics.items():
                topic = self.contract.render_carla_topic(template, sim_id)
                if self.ensure_topic_spec(spec, topic=topic):
                    created.append(topic)
                if spec.dlq:
                    dlq = self.contract.dlq_topic(topic)
                    if self.ensure_topic(
                        dlq,
                        partitions=spec.partitions,
                        retention_ms=dlq_retention,
                    ):
                        created.append(dlq)
        except Exception:
            # 部分创建失败必须回滚已建 Topic，否则同一 sim_id 重试会残留半套 Topic；
            # 调用方拿不到 created 列表，无法自行清理（V1.23.4）。
            for topic in created:
                try:
                    self.delete_topic(topic)
                except Exception:
                    logger.exception("carla_topic_rollback_failed", topic=topic, sim_id=sim_id)
            raise
        return created

    def delete_carla_session(self, sim_id: str) -> list[str]:
        """删除单个 CARLA 会话的动态 Topic（幂等；不存在的忽略）。"""
        deleted: list[str] = []
        for template, spec in self.contract.carla_topics.items():
            topic = self.contract.render_carla_topic(template, sim_id)
            if self.delete_topic(topic):
                deleted.append(topic)
            if spec.dlq:
                dlq = self.contract.dlq_topic(topic)
                if self.delete_topic(dlq):
                    deleted.append(dlq)
        return deleted


__all__ = ["KafkaAdmin"]
