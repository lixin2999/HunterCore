"""Kafka AdminClient 封装：SCRAM 用户 + 每车 8 Topic 的创建/删除。

契约依据：contracts/openapi/vehicle-service.yaml `x-hunter-kafka` + `x-hunter-provisioning`。

设计要点：
- **控制面专用**：本服务不参与业务 Topic 生产/消费，仅在 provisioning/下线时调用 AdminClient；
- **同步 → 异步**：kafka-python-ng AdminClient 为阻塞 API，全部通过 ``asyncio.to_thread`` 移交
  线程池执行，避免卡住事件循环（对齐"全异步" 服务规范）；
- **凭据管理（KIP-95）**：`AlterUserScramCredentials` 需 broker ≥ 2.7（项目 Kafka 3.6 ✓）；
  机制固定 SCRAM-SHA-512（与车端一致，contracts/kafka/topics.yaml `defaults.sasl_mechanism`）；
- **Topic 清单**：从 ``contracts/kafka/topics.yaml`` 的 ``vehicle_topics`` 中筛出 ``hunter.{vehicle_id}.``
  模式条目（共 8 个/车；广播 Topic ``hunter.broadcast.command`` 不属任何单车，跳过），
  分区/保留期严格按契约字段读取，禁止硬编码；
- **幂等**：`create_topics` / `alter_user_scram_credentials(UPSERT)` 天然幂等，
  Topic 已存在视为成功；SCRAM 用户已存在时覆盖口令（对应 rotate-scram 与 takeover_existing 语义）；
- **失败传播**：底层 KafkaError 一律抛出，由 provisioner 决策回滚与 provision_status 记录，
  本模块不做静默吞异常（禁止"部分成功"假象）。
"""
from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from typing import Any

import yaml
from hunter_common.exceptions import ServiceUnavailableError
from hunter_common.logging import get_logger
from kafka.admin import KafkaAdminClient, NewTopic  # type: ignore[import-untyped]
from kafka.errors import (  # type: ignore[import-untyped]
    KafkaError,
    TopicAlreadyExistsError,
)

# 兼容兜底：`AleadyHasPartitionException` 是 kafka-python 旧仓库的拼写错误类名（"Aleady"），
# kafka-python-ng 已移除/修正该名，直接顶层导入会在 import 期抛 ImportError 致服务无法启动。
# create_topics 对已存在 Topic 实际抛 TopicAlreadyExistsError，故缺失时回退到同一异常，
# 保留模块级名字以不破坏下方 `except (TopicAlreadyExistsError, AleadyHasPartitionException)` 与既有测试。
try:  # pragma: no cover - 依版本分支：老 kafka-python 有此名，ng fork 无
    from kafka.errors import AleadyHasPartitionException  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover - kafka-python-ng 回退路径
    AleadyHasPartitionException = TopicAlreadyExistsError  # type: ignore[assignment,misc]

try:  # kafka-python-ng 提供的 KIP-95 请求（fork ≥ 2.2）
    from kafka.protocol.scram import (  # type: ignore[import-not-found,import-untyped]
        AlterUserScramCredentialsRequest,
        ScramCredentialUpdate,
    )
    from kafka.scram_types import (  # type: ignore[import-not-found,import-untyped]
        ScramMechanism,
    )
except ImportError:  # pragma: no cover - 依赖版本回退路径
    AlterUserScramCredentialsRequest = None  # type: ignore[assignment]
    ScramCredentialUpdate = None  # type: ignore[assignment]
    ScramMechanism = None  # type: ignore[assignment]

from app.config import settings

logger = get_logger("app.services.kafka_admin")

#: Topic 命名模式：`hunter.{vehicle_id}.<type>`（契约 vehicle_topics，8 项/车；
#: 广播 `hunter.broadcast.command` 无 vehicle_id 段，创建每车 Topic 时跳过）
VEHICLE_TOPIC_PATTERN = "hunter.{vehicle_id}."

#: SCRAM 迭代次数（KIP-95 建议默认值，与 kafka-configs.sh --scram-mechanisms 默认对齐）
_SCRAM_ITERATIONS = 8192


# =====================================================================
# 契约 Topic 清单加载（进程级缓存）
# =====================================================================
_VEHICLE_TOPICS_CACHE: list[dict[str, Any]] | None = None
_CACHE_LOCK = threading.Lock()


def _find_contracts_dir() -> Path:
    """定位 contracts/kafka 目录：优先 env `KAFKA_CONTRACT_DIR`，回退从包路径向上查找。"""
    if settings.kafka_contract_dir:
        candidate = Path(settings.kafka_contract_dir)
        if candidate.is_dir():
            return candidate
    here = Path(__file__).resolve()
    for parent in here.parents:
        probe = parent / "contracts" / "kafka"
        if probe.is_dir():
            return probe
    raise ServiceUnavailableError(
        "未找到 contracts/kafka 目录（KAFKA_CONTRACT_DIR 未配置且向上探测失败）",
        details={"from": str(here)},
    )


def load_vehicle_topics() -> list[dict[str, Any]]:
    """从 topics.yaml 加载车辆 Topic 模板（进程内缓存；契约变更需重启服务）。

    返回列表元素结构：``{"name": 模板, "partitions": int, "retention_ms": int}``；
    `name` 保留 `{vehicle_id}` 占位符，由调用方 `.format(vehicle_id=...)` 展开。
    """
    global _VEHICLE_TOPICS_CACHE  # noqa: PLW0603 - 单例缓存，进程内允许
    with _CACHE_LOCK:
        if _VEHICLE_TOPICS_CACHE is not None:
            return _VEHICLE_TOPICS_CACHE
        topics_file = _find_contracts_dir() / "topics.yaml"
        if not topics_file.is_file():
            raise ServiceUnavailableError(
                f"Kafka Topic 契约文件缺失: {topics_file}",
            )
        raw = yaml.safe_load(topics_file.read_text(encoding="utf-8")) or {}
        defaults = raw.get("defaults") or {}
        default_rf = int(defaults.get("replication_factor", settings.kafka_default_replication_factor))
        entries: list[dict[str, Any]] = []
        for item in raw.get("vehicle_topics") or []:
            name = str(item.get("name", ""))
            if VEHICLE_TOPIC_PATTERN.format(vehicle_id="") not in name:
                continue  # 广播 Topic 与每车 provisioning 无关
            entries.append(
                {
                    "name": name,
                    "partitions": int(item.get("partitions", 3)),
                    "retention_ms": int(item.get("retention_ms", 604_800_000)),
                    # 单机部署 RF 强制降为 1（compose 单 broker），否则沿用契约默认
                    "replication_factor": min(int(item.get("replication_factor", default_rf)), settings.kafka_default_replication_factor),
                    "cleanup_policy": str(item.get("cleanup_policy", defaults.get("cleanup_policy", "delete"))),
                }
            )
        if not entries:
            raise ServiceUnavailableError(
                f"topics.yaml 未包含任何车辆 Topic 条目（模式 {VEHICLE_TOPIC_PATTERN}）",
                details={"file": str(topics_file)},
            )
        _VEHICLE_TOPICS_CACHE = entries
        logger.info("vehicle_topics_loaded", count=len(entries), file=str(topics_file))
        return entries


# =====================================================================
# AdminClient 包装（同步 API → asyncio.to_thread）
# =====================================================================
class KafkaAdminOps:
    """Kafka 控制面操作集合（SCRAM + Topic）；不持业务状态，可安全并发调用。

    生命周期由 main.py 在 lifespan 中 `build_admin_client()` 创建、进程关闭时 `close()`；
    实例被 health router 复用做 list_topics 探测（app.state.kafka_admin）。
    """

    def __init__(self, admin: KafkaAdminClient) -> None:
        self._admin = admin
        # kafka-python AdminClient 非线程安全：串行化所有请求，避免并发调用交叉写
        self._lock = threading.Lock()

    # ---------- 元数据探测 ----------
    def list_topics(self) -> list[str]:
        """就绪探针使用：返回全部 Topic 名（阻塞）。"""
        with self._lock:
            return list(self._admin.list_topics())

    async def alist_topics(self) -> list[str]:
        return await asyncio.to_thread(self.list_topics)

    # ---------- SCRAM（KIP-95） ----------
    async def upsert_scram_user(self, username: str, password: str) -> None:
        """创建或覆盖 SCRAM-SHA-512 用户（对应 provisioner.steps.scram 与 rotate-scram）。

        语义：UPSERT（AlterUserScramCredentialsRequest 的 create_or_update_credentials）；
        已存在用户会覆盖旧凭据（旧口令立即失效，符合 rotate 语义）。
        """
        await asyncio.to_thread(self._upsert_scram_user_sync, username, password)

    def _upsert_scram_user_sync(self, username: str, password: str) -> None:
        if AlterUserScramCredentialsRequest is None or ScramCredentialUpdate is None:
            raise ServiceUnavailableError(
                "kafka-python-ng 版本不支持 KIP-95（AlterUserScramCredentials）；"
                "请升级至 kafka-python-ng>=2.2",
            )
        try:
            with self._lock:
                update = ScramCredentialUpdate(
                    principal=name_principal(username),
                    scram_mechanism=SCRAM_MECHANISM_ID,
                    iterations=_SCRAM_ITERATIONS,
                    password=password.encode("utf-8"),
                )
                request = AlterUserScramCredentialsRequest(upserts=[update], deletes=[])
                future = self._admin.client.send_request(request)
                future.result(
                    timeout_ms=settings.kafka_admin_timeout_ms
                )  # 阻塞至 broker 落盘（含元数据同步）
        except KafkaError as exc:
            logger.error("kafka_scram_upsert_failed", username=username, error=str(exc))
            raise ServiceUnavailableError(
                f"Kafka SCRAM 用户写入失败: {exc}",
                details={"username": username},
            ) from exc
        logger.info("kafka_scram_upsert_ok", username=username)

    async def delete_scram_user(self, username: str) -> None:
        """删除 SCRAM 用户（下线/回滚）；不存在视为幂等成功。"""
        await asyncio.to_thread(self._delete_scram_user_sync, username)

    def _delete_scram_user_sync(self, username: str) -> None:
        if AlterUserScramCredentialsRequest is None:
            raise ServiceUnavailableError(
                "kafka-python-ng 版本不支持 KIP-95（AlterUserScramCredentials）",
            )
        try:
            with self._lock:
                request = AlterUserScramCredentialsRequest(
                    upserts=[],
                    deletes=[delete_principal(username, SCRAM_MECHANISM_ID)],
                )
                future = self._admin.client.send_request(request)
                future.result(timeout_ms=settings.kafka_admin_timeout_ms)
        except KafkaError as exc:
            # 用户不存在（SASL_PRINCIPAL_NOT_FOUND / 类似）：幂等成功
            text = str(exc).lower()
            if "not found" in text or "nosuchuser" in text or "no such" in text:
                logger.info("kafka_scram_delete_missing", username=username)
                return
            logger.error("kafka_scram_delete_failed", username=username, error=str(exc))
            raise ServiceUnavailableError(
                f"Kafka SCRAM 用户删除失败: {exc}",
                details={"username": username},
            ) from exc
        logger.info("kafka_scram_delete_ok", username=username)

    # ---------- Topic 批量创建/删除 ----------
    async def create_vehicle_topics(
        self, vehicle_id: str, *, skip_existing: bool = True
    ) -> list[str]:
        """为单车创建 8 个 `hunter.{vehicle_id}.*` Topic；返回本次实际新建的名称列表。

        - 已存在 → 跳过（对应 `takeover_existing=true`）；
        - 底层 `TopicAlreadyExistsError` 归类为"跳过"而非失败；
        - 返回的 `created` 供 provisioner 回滚使用：只删本次真的新建的 Topic，
          不动外部既有资源（契约 x-hunter-provisioning.topics.rollback）。
        """
        return await asyncio.to_thread(self._create_vehicle_topics_sync, vehicle_id, skip_existing)

    def _create_vehicle_topics_sync(
        self, vehicle_id: str, skip_existing: bool
    ) -> list[str]:
        topics = render_vehicle_topics(vehicle_id)
        new_topics = [
            NewTopic(
                name=t["name"],
                num_partitions=t["partitions"],
                replication_factor=t["replication_factor"],
                topic_config={
                    "retention.ms": str(t["retention_ms"]),
                    "cleanup.policy": t["cleanup_policy"],
                },
            )
            for t in topics
        ]
        created: list[str] = []
        with self._lock:
            for nt in new_topics:
                try:
                    self._admin.create_topics([nt])
                    created.append(nt.name)
                except (TopicAlreadyExistsError, AleadyHasPartitionException):
                    if skip_existing:
                        logger.info("kafka_topic_exists_skip", name=nt.name)
                        continue
                    raise
                except KafkaError as exc:
                    logger.error(
                        "kafka_topic_create_failed", name=nt.name, error=str(exc)
                    )
                    raise ServiceUnavailableError(
                        f"Kafka Topic 创建失败: {exc}",
                        details={"topic": nt.name},
                    ) from exc
        logger.info("kafka_topics_created", vehicle_id=vehicle_id, count=len(created))
        return created

    async def delete_topics(self, names: list[str]) -> list[str]:
        """删除指定 Topic；不存在视为幂等成功。返回实际删除的名称。"""
        if not names:
            return []
        return await asyncio.to_thread(self._delete_topics_sync, list(names))

    def _delete_topics_sync(self, names: list[str]) -> list[str]:
        deleted: list[str] = []
        with self._lock:
            for name in names:
                try:
                    self._admin.delete_topics([name])
                    deleted.append(name)
                except KafkaError as exc:
                    text = str(exc).lower()
                    if "unknowntopic" in text or "unknown topic" in text or "does not exist" in text:
                        logger.info("kafka_topic_delete_missing", name=name)
                        continue
                    logger.error("kafka_topic_delete_failed", name=name, error=str(exc))
                    raise ServiceUnavailableError(
                        f"Kafka Topic 删除失败: {exc}",
                        details={"topic": name},
                    ) from exc
        logger.info("kafka_topics_deleted", count=len(deleted))
        return deleted

    # ---------- 关闭 ----------
    def close(self) -> None:
        try:
            self._admin.close()
        except Exception:  # noqa: BLE001 - 关闭尽力而为
            logger.exception("kafka_admin_close_failed")

    async def aclose(self) -> None:
        await asyncio.to_thread(self.close)


# =====================================================================
# SCRAM 协议辅助（KIP-95 数据结构）
# =====================================================================
#: SCRAM-SHA-512 对应 kafka.api.ScramType 数值 1（KIP-576；0=SCRAM-SHA-256）
SCRAM_MECHANISM_ID = 1


def name_principal(username: str) -> Any:
    """构造 `AlterUserScramCredentials` 请求体的 `ScramCredentialUpdate.principal` 结构。

    kafka-python-ng 通过 dataclasses 定义 `ScramName(principal_name, principal_type)`；
    principal_type=2 = KAFKA_PRINCIPAL_TYPE（唯一合法值，见 KIP-576）。
    未导出该类时回退到 duck-typing 匿名对象（协议层只看字段名）。
    """
    try:
        from kafka.protocol.scram import ScramName  # type: ignore[import-not-found]
        return ScramName(principal_name=username, principal_type=2)
    except Exception:  # noqa: BLE001 - 未导出则使用轻量替代
        class _Name:
            def __init__(self, principal_name: str, principal_type: int) -> None:
                self.principal_name = principal_name
                self.principal_type = principal_type

        return _Name(username, 2)


def delete_principal(username: str, mechanism: int) -> Any:
    """构造删除项（principal + mechanism）。"""
    try:
        from kafka.protocol.scram import DeleteScramCredential  # type: ignore[import-not-found]
        return DeleteScramCredential(name=name_principal(username), scram_mechanism=mechanism)
    except Exception:  # noqa: BLE001
        class _Del:
            def __init__(self, name: Any, scram_mechanism: int) -> None:
                self.name = name
                self.scram_mechanism = scram_mechanism

        return _Del(name_principal(username), mechanism)


def render_vehicle_topics(vehicle_id: str) -> list[dict[str, Any]]:
    """把契约模板展开为该车辆的实际 Topic 名（保留分区/保留期/RF/cleanup 元数据）。"""
    result: list[dict[str, Any]] = []
    for entry in load_vehicle_topics():
        rendered = dict(entry)
        rendered["name"] = str(entry["name"]).format(vehicle_id=vehicle_id)
        result.append(rendered)
    return result


# =====================================================================
# 构造入口（main.py lifespan 调用）
# =====================================================================
def build_admin_client() -> KafkaAdminOps:
    """按 Settings 构造 AdminClient（阻塞 IO，调用方通过 asyncio.to_thread 包装）。

    - bootstrap：`settings.admin_bootstrap`（默认 INTERNAL 9092）；
    - 安全协议：`SASL_PLAINTEXT`（控制面走内部网络，不重复 SSL）；
    - 凭据：`settings.kafka_sasl_username/password` = 平台侧 hunter-client。
    """
    overrides: dict[str, Any] = {
        "request_timeout_ms": settings.kafka_admin_timeout_ms,
        "connections_max_idle_ms": 60_000,
    }
    if settings.kafka_security_protocol in ("SASL_PLAINTEXT", "SASL_SSL"):
        overrides["security_protocol"] = settings.kafka_security_protocol
        overrides["sasl_mechanism"] = settings.kafka_sasl_mechanism
        overrides["sasl_plain_username"] = settings.kafka_sasl_username
        overrides["sasl_plain_password"] = settings.kafka_sasl_password
    elif settings.kafka_security_protocol == "PLAINTEXT":
        overrides["security_protocol"] = "PLAINTEXT"
    else:  # SSL：控制面走 mTLS 需证书（当前未启用）
        overrides["security_protocol"] = settings.kafka_security_protocol
        if settings.kafka_ssl_cafile:
            overrides["ssl_cafile"] = settings.kafka_ssl_cafile
        if settings.kafka_ssl_certfile:
            overrides["ssl_certfile"] = settings.kafka_ssl_certfile
        if settings.kafka_ssl_keyfile:
            overrides["ssl_keyfile"] = settings.kafka_ssl_keyfile
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=settings.admin_bootstrap,
            client_id=f"{settings.service_name}-admin",
            **overrides,
        )
    except KafkaError as exc:
        logger.error("kafka_admin_connect_failed", error=str(exc), bootstrap=settings.admin_bootstrap)
        raise ServiceUnavailableError(
            f"Kafka AdminClient 连接失败: {exc}",
            details={"bootstrap": settings.admin_bootstrap},
        ) from exc
    return KafkaAdminOps(admin)


async def build_admin_client_safe() -> KafkaAdminOps | None:
    """异步安全的建连入口（lifespan 使用）：连接失败 → 返回 None，服务仍可启动。

    动机：控制面依赖不可达时，/healthz /readyz 需能正常响应就绪探针（返回 503 探定），
    而非直接拒绝启动 —— 保证部署时其它依赖先就绪后本服务能自动恢复。
    业务路径（provisioning/rotate/reissue）遇到 admin=None 将抛 5001。
    """
    try:
        return await asyncio.to_thread(build_admin_client)
    except ServiceUnavailableError as exc:
        logger.warning("kafka_admin_bootstrap_deferred", error=str(exc))
        return None
    except Exception:  # noqa: BLE001 - 任何建连异常都不阻断启动
        logger.exception("kafka_admin_bootstrap_failed")
        return None


__all__ = [
    "KafkaAdminOps",
    "SCRAM_MECHANISM_ID",
    "VEHICLE_TOPIC_PATTERN",
    "build_admin_client",
    "build_admin_client_safe",
    "load_vehicle_topics",
    "render_vehicle_topics",
]
