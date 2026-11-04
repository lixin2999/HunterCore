"""仿真端一键开通的 Kafka 控制面精简封装（V1.24.2，scene 内自持，不动 vehicle-service）。

契约依据：``contracts/openapi/scene-service.yaml#x-hunter-sim-provisioning`` +
``contracts/kafka/topics.yaml#simulation_node_topics``。

覆盖范围刻意收敛到"节点级一键开通"所需的两类操作（车辆侧 provisioning 与本模块无关）：

1. **SCRAM 用户**（KIP-95）：主体 ``sim-{node_id}``（与 Kafka 客户端证书 CN 一致），机制固定
   SCRAM-SHA-512；``alter_user_scram_credentials`` 天然 UPSERT（对应 provision/rotate-scram），
   删除不存在主体视为幂等成功（对应回滚/deprovision）。
2. **节点级回传 Topic**：``simulation.{node_id}.result`` + 死信 ``.dlq``（分区/保留期一律取自契约，
   禁止硬编码）；broker 关闭 ``auto.create.topics.enable``，故死信必须显式创建。

为什么单独建 confluent AdminClient 而非复用 ``app.state.kafka_admin``（共享 ``KafkaAdmin``）：
共享封装只做 Topic（无 SCRAM），且实例语义与 CARLA 会话绑定；本模块需要 SCRAM 能力，用独立的
confluent 连接承载"控制面专用"操作，与业务生产者/消费者隔离，耦合最低（用户决策：scene 内新建精简版）。

同步 → 异步：confluent AdminClient 为阻塞 API，全部经 ``asyncio.to_thread`` 移交线程池，
不卡事件循环（对齐"全异步"服务规范）。失败一律向上抛，由 ``sim_provisioner`` 决策回滚与
``provision_status`` 记录，本模块不做静默吞异常（禁止"部分成功"假象）。
"""
from __future__ import annotations

import asyncio
import threading
from typing import Any

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import (
    AdminClient,
    NewTopic,
    ScramCredentialInfo,
    ScramMechanism,
    UserScramCredentialDeletion,
    UserScramCredentialUpsertion,
)
from hunter_common.exceptions import ServiceUnavailableError
from hunter_common.kafka.contracts import KafkaContract, get_contract
from hunter_common.logging import get_logger

logger = get_logger("app.services.sim_kafka_admin")

#: 节点级回传 Topic 模板前缀（契约 topics.yaml#simulation_node_topics[].name）
_SIMNODE_TOPIC_PREFIX = "simulation.{node_id}."


def _is_scram_principal_missing(exc: BaseException) -> bool:
    """判定 SCRAM 删除异常是否为"用户/凭据不存在"（下线/回滚可幂等忽略）。

    主判据：confluent ``KafkaError.code() == RESOURCE_NOT_FOUND``（KIP-95 删除不存在主体的标准
    返回）；回退判据：异常文本关键词（不同 broker/版本措辞差异）。与 vehicle-service kafka_admin 同源。
    """
    err = exc.args[0] if getattr(exc, "args", None) else None
    code = getattr(err, "code", None)
    if callable(code) and code() == KafkaError.RESOURCE_NOT_FOUND:
        return True
    text = str(exc).lower()
    return any(
        token in text
        for token in ("resource_not_found", "not found", "does not exist", "nosuchuser", "no such")
    )


def _admin_config(settings: Any) -> dict[str, Any]:
    """由 scene Settings 生成 AdminClient 连接配置（与 producer/consumer 同源）。"""
    settings_map: dict[str, Any] = {
        "bootstrap.servers": ",".join(settings.kafka_bootstrap_servers_list),
        "client.id": f"{settings.service_name}-sim-provision-admin",
        "security.protocol": settings.kafka_security_protocol,
    }
    protocol = settings.kafka_security_protocol
    if protocol in ("SASL_SSL", "SASL_PLAINTEXT"):
        settings_map["sasl.mechanism"] = settings.kafka_sasl_mechanism
        if settings.kafka_sasl_username:
            settings_map["sasl.username"] = settings.kafka_sasl_username
        if settings.kafka_sasl_password:
            settings_map["sasl.password"] = settings.kafka_sasl_password
    if protocol in ("SSL", "SASL_SSL"):
        if settings.kafka_ssl_cafile:
            settings_map["ssl.ca.location"] = settings.kafka_ssl_cafile
        if settings.kafka_ssl_certfile:
            settings_map["ssl.certificate.location"] = settings.kafka_ssl_certfile
        if settings.kafka_ssl_keyfile:
            settings_map["ssl.key.location"] = settings.kafka_ssl_keyfile
        if settings.kafka_ssl_keypassword:
            settings_map["ssl.key.password"] = settings.kafka_ssl_keypassword
    return settings_map


class SimKafkaAdminOps:
    """仿真节点一键开通控制面（SCRAM + 节点级回传 Topic）；无业务状态，可安全并发调用。"""

    def __init__(
        self,
        client: AdminClient,
        *,
        contract: KafkaContract,
        replication_factor: int = 1,
        request_timeout_seconds: float = 10.0,
        scram_iterations: int = 8192,
    ) -> None:
        self._client = client
        self._contract = contract
        self._replication_factor = max(1, replication_factor)
        self._request_timeout = request_timeout_seconds
        self._scram_iterations = scram_iterations
        # confluent AdminClient 的 future 结果可并发，但 SCRAM alter 与 Topic create 混用时
        # 串行化更利于回滚判定的确定性（与 vehicle-service kafka_admin 的 _lock 同动机）
        self._lock = threading.Lock()

    # ---------- 构造入口（lifespan 调用；连接失败 → None，服务仍可起） ----------

    @classmethod
    def build(cls, settings: Any, *, contract: KafkaContract | None = None) -> SimKafkaAdminOps:
        """按 Settings 构造（阻塞建连）；契约缺失时向上抛 KafkaContractError。"""
        resolved = contract if contract is not None else get_contract(required=True)
        client = AdminClient(_admin_config(settings))
        return cls(
            client,
            contract=resolved,
            replication_factor=settings.sim_topic_replication_factor,
            request_timeout_seconds=settings.sim_kafka_admin_timeout_ms / 1000.0,
            scram_iterations=settings.sim_scram_iterations,
        )

    @classmethod
    async def build_safe(cls, settings: Any) -> SimKafkaAdminOps | None:
        """异步安全建连：失败返回 None（控制面依赖不可达时不阻塞启动，业务路径再抛 5001）。"""
        try:
            return await asyncio.to_thread(cls.build, settings)
        except Exception:
            logger.exception("sim_kafka_admin_bootstrap_failed")
            return None

    # ---------- SCRAM（KIP-95） ----------

    async def upsert_scram_user(self, username: str, password: str) -> None:
        """创建/覆盖 SCRAM-SHA-512 用户（provision scram 步 / rotate-scram）。"""
        await asyncio.to_thread(self._upsert_scram_sync, username, password)

    def _upsert_scram_sync(self, username: str, password: str) -> None:
        try:
            with self._lock:
                upsertion = UserScramCredentialUpsertion(
                    user=username,
                    scram_credential_info=ScramCredentialInfo(
                        mechanism=ScramMechanism.SCRAM_SHA_512,
                        iterations=self._scram_iterations,
                    ),
                    password=password.encode("utf-8"),
                )
                futures = self._client.alter_user_scram_credentials([upsertion])
                for future in futures.values():
                    future.result()
        except KafkaException as exc:
            logger.error("sim_kafka_scram_upsert_failed", username=username, error=str(exc))
            raise ServiceUnavailableError(
                f"Kafka SCRAM 用户写入失败: {exc}", details={"username": username}
            ) from exc
        logger.info("sim_kafka_scram_upsert_ok", username=username)

    async def delete_scram_user(self, username: str) -> None:
        """删除 SCRAM 用户（回滚/deprovision）；不存在视为幂等成功。"""
        await asyncio.to_thread(self._delete_scram_sync, username)

    def _delete_scram_sync(self, username: str) -> None:
        try:
            with self._lock:
                deletion = UserScramCredentialDeletion(
                    user=username, mechanism=ScramMechanism.SCRAM_SHA_512
                )
                futures = self._client.alter_user_scram_credentials([deletion])
                for future in futures.values():
                    future.result()
        except KafkaException as exc:
            if _is_scram_principal_missing(exc):
                logger.info("sim_kafka_scram_delete_missing", username=username)
                return
            logger.error("sim_kafka_scram_delete_failed", username=username, error=str(exc))
            raise ServiceUnavailableError(
                f"Kafka SCRAM 用户删除失败: {exc}", details={"username": username}
            ) from exc
        logger.info("sim_kafka_scram_delete_ok", username=username)

    # ---------- 节点级回传 Topic（含死信） ----------

    def render_node_topics(self, node_id: str) -> list[dict[str, Any]]:
        """契约模板 → 该节点的实际 Topic（源 + 派生死信）；分区/保留期取自契约。"""
        dlq_retention = int(
            (self._contract.topics_doc.get("naming") or {}).get("dlq_retention_ms", 2_592_000_000)
        )
        result: list[dict[str, Any]] = []
        for template, spec in self._contract.simulation_node_topics.items():
            topic = self._contract.render_simulation_node_topic(template, node_id)
            result.append(
                {"name": topic, "partitions": spec.partitions, "retention_ms": spec.retention_ms, "is_dlq": False}
            )
            if spec.dlq:
                result.append(
                    {
                        "name": self._contract.dlq_topic(topic),
                        "partitions": spec.partitions,
                        "retention_ms": dlq_retention,
                        "is_dlq": True,
                        "source_topic": topic,
                    }
                )
        return result

    async def ensure_node_topics(self, node_id: str, *, skip_existing: bool = True) -> list[str]:
        """创建该节点全部 Topic（含死信）；返回本次实际新建的名称（供回滚，只删本次新建）。"""
        return await asyncio.to_thread(self._ensure_node_topics_sync, node_id, skip_existing)

    def _ensure_node_topics_sync(self, node_id: str, skip_existing: bool) -> list[str]:
        specs = self.render_node_topics(node_id)
        created: list[str] = []
        try:
            for entry in specs:
                if entry["partitions"] < 1:
                    raise ServiceUnavailableError(
                        f"节点级 Topic 分区数非法：{entry['name']}", details={"node_id": node_id}
                    )
                config: dict[str, str] = {}
                if entry["retention_ms"] is not None:
                    config["retention.ms"] = str(int(entry["retention_ms"]))
                new_topic = NewTopic(
                    entry["name"],
                    num_partitions=int(entry["partitions"]),
                    replication_factor=self._replication_factor,
                    config=config,
                )
                with self._lock:
                    future = self._client.create_topics([new_topic], request_timeout=self._request_timeout)[entry["name"]]
                    future.result(timeout=self._request_timeout)
                created.append(entry["name"])
            # 任一 Topic 已存在时 create_topics 抛 TOPIC_ALREADY_EXISTS：整体按"已就绪"处理
        except KafkaException as exc:
            error = exc.args[0] if exc.args else None
            already = error is not None and error.code() == KafkaError.TOPIC_ALREADY_EXISTS
            if already and skip_existing:
                logger.info("sim_kafka_node_topic_exists_skip", node_id=node_id)
                return created
            # 部分创建失败必须回滚本次已建，否则同一 node_id 重试残留半套 Topic
            if created:
                self._delete_node_topics_sync(created)
            logger.error("sim_kafka_node_topic_create_failed", node_id=node_id, error=str(exc))
            raise ServiceUnavailableError(
                f"Kafka 节点级 Topic 创建失败: {exc}", details={"node_id": node_id}
            ) from exc
        logger.info("sim_kafka_node_topics_created", node_id=node_id, count=len(created))
        return created

    async def delete_node_topics(self, names: list[str]) -> list[str]:
        """删除指定 Topic；不存在视为幂等成功。返回实际删除的名称。"""
        if not names:
            return []
        return await asyncio.to_thread(self._delete_node_topics_sync, list(names))

    def _delete_node_topics_sync(self, names: list[str]) -> list[str]:
        deleted: list[str] = []
        for name in names:
            try:
                with self._lock:
                    future = self._client.delete_topics([name], request_timeout=self._request_timeout)[name]
                    future.result(timeout=self._request_timeout)
                deleted.append(name)
            except KafkaException as exc:
                error = exc.args[0] if exc.args else None
                if error is not None and error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    logger.info("sim_kafka_node_topic_delete_missing", name=name)
                    continue
                logger.error("sim_kafka_node_topic_delete_failed", name=name, error=str(exc))
                raise ServiceUnavailableError(
                    f"Kafka 节点级 Topic 删除失败: {exc}", details={"topic": name}
                ) from exc
        logger.info("sim_kafka_node_topics_deleted", count=len(deleted))
        return deleted


__all__ = ["SimKafkaAdminOps"]
