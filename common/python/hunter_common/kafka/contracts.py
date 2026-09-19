"""Kafka 契约运行时加载（单一事实来源：``contracts/kafka/``）。

- ``topics.yaml``：Topic 清单（分区数 / acks / 保留 / key 策略 / Schema 引用）
- ``consumer-groups.yaml``：消费者组（订阅 / 生产 / 幂等键 / DLQ / 单批量）
- ``schemas/*.schema.json``：11 个 draft-07 消息 Schema（``additionalProperties=false``）

用途：生产者按契约选 acks、按契约校验消息体与 key=vehicle_id；消费者按契约解析订阅 Topic 的
Schema 并校验消息，非法消息进 DLQ。契约不一致时**报错而非降级**（避免"实现漂移悄悄上线"）。

目录定位顺序（均指向包含 topics.yaml 的目录，即仓库 ``contracts/kafka/``）：
1. 显式 ``base_dir``（服务配置项 ``kafka_contract_dir`` 或测试注入）；
2. 环境变量 ``KAFKA_CONTRACT_DIR``（K8s 可由 ConfigMap 挂载）；
3. 从当前工作目录向上查找 ``contracts/kafka/topics.yaml``；
4. 从本模块所在位置向上查找（本地 editable 安装 / 仓库内运行）。
"""
from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Final

import yaml
from jsonschema import Draft7Validator
from jsonschema.exceptions import SchemaError

from hunter_common.exceptions import HunterBaseException, InvalidParameterError
from hunter_common.logging import get_logger

logger = get_logger("hunter_common.kafka.contracts")

#: 环境变量名（与配置项 kafka_contract_dir 对应）
CONTRACT_DIR_ENV: Final[str] = "KAFKA_CONTRACT_DIR"

#: 契约中 key 的两种取值
KEY_NONE: Final[str] = "none"
KEY_VEHICLE_ID: Final[str] = "vehicle_id"

#: DLQ 后缀（契约 topics.yaml#naming.dlq_pattern）
_DLQ_SUFFIX: Final[str] = ".dlq"


class KafkaContractError(HunterBaseException):
    """Kafka 契约不可用（文件缺失/结构非法）：属部署配置错误，返回 5000。"""

    code = 5000
    message = "Kafka 契约不可用"


class KafkaMessageSchemaError(InvalidParameterError):
    """消息体不符合契约 JSON Schema（2001）：由消费者转投 DLQ，不重试。"""

    message = "Kafka 消息不符合契约 Schema"


@dataclass(frozen=True, slots=True)
class TopicSpec:
    """Topic 契约条目（``topics.yaml`` 单条）。"""

    name: str
    partitions: int
    acks: str
    key: str
    retention_ms: int | None = None
    schema: str | None = None
    direction: str | None = None
    producer: str | None = None
    consumers: tuple[str, ...] = ()
    frequency: str | None = None

    @property
    def schema_name(self) -> str | None:
        """Schema 逻辑名（``schemas/telemetry.schema.json`` → ``telemetry``）。"""
        if not self.schema:
            return None
        return Path(self.schema).name.removesuffix(".schema.json")

    @property
    def requires_vehicle_key(self) -> bool:
        """是否强制 ``key = vehicle_id``（契约 defaults.key 与 Topic 级 key）。"""
        return self.key == KEY_VEHICLE_ID

    @property
    def is_broadcast(self) -> bool:
        """广播 Topic：key 必须为空（契约 ``hunter.broadcast.command``）。"""
        return self.key == KEY_NONE


@dataclass(frozen=True, slots=True)
class ConsumerGroupSpec:
    """消费者组契约条目（``consumer-groups.yaml`` 单条）。"""

    group_id: str
    service: str | None = None
    subscribes: tuple[str, ...] = ()
    produces: tuple[str, ...] = ()
    idempotency_key: str | None = None
    dlq_enabled: bool = True
    subscribe_mode: str | None = None
    engine: str | None = None
    target_latency: str | None = None


# ---------- 契约目录定位 ----------

def _looks_like_contract_dir(path: Path) -> bool:
    """目录是否同时含 topics.yaml 与 consumer-groups.yaml（即 contracts/kafka）。"""
    return (path / "topics.yaml").is_file() and (path / "consumer-groups.yaml").is_file()


def _search_upward(start: Path) -> Path | None:
    """从 ``start`` 向上查找契约目录（同时兼容仓库根与 contracts/kafka 两种参照点）。"""
    current = start if start.is_dir() else start.parent
    for candidate in (current, *current.parents):
        direct = candidate / "contracts" / "kafka"
        if _looks_like_contract_dir(direct):
            return direct
        if _looks_like_contract_dir(candidate):
            return candidate
    return None


def locate_contract_dir(base_dir: str | Path | None = None) -> Path | None:
    """定位契约目录（见模块 docstring 的四级顺序）；找不到返回 None。

    显式 ``base_dir`` 为空白串（如未填写的环境变量）时按"未提供"处理，继续自动探测。
    """
    explicit = str(base_dir).strip() if base_dir is not None else ""
    if explicit:
        path = Path(explicit)
        return path if _looks_like_contract_dir(path) else None
    env_dir = os.environ.get(CONTRACT_DIR_ENV, "").strip()
    if env_dir:
        path = Path(env_dir)
        if _looks_like_contract_dir(path):
            return path
        logger.warning("kafka_contract_dir_env_invalid", contract_dir=env_dir)
    for start in (Path.cwd(), Path(__file__).resolve()):
        found = _search_upward(start)
        if found is not None:
            return found
    return None


class KafkaContract:
    """Kafka 契约（topics / consumer-groups / schemas）只读视图。"""

    def __init__(
        self,
        base_dir: Path,
        *,
        topics_doc: dict[str, Any],
        groups_doc: dict[str, Any],
    ) -> None:
        self._base_dir = base_dir
        self._topics_doc = topics_doc
        self._groups_doc = groups_doc
        self._platform: dict[str, TopicSpec] = {}
        self._broadcast: dict[str, TopicSpec] = {}
        self._vehicle_by_type: dict[str, TopicSpec] = {}
        self._vehicle_by_template: dict[str, TopicSpec] = {}
        self._groups: dict[str, ConsumerGroupSpec] = {}
        self._schema_cache: dict[str, dict[str, Any]] = {}
        self._validator_cache: dict[str, Draft7Validator] = {}
        self._index()

    # ---------- 加载 ----------

    @classmethod
    def load(cls, base_dir: str | Path) -> KafkaContract:
        """读取契约文件并建立索引（结构非法 → KafkaContractError）。"""
        directory = Path(base_dir)
        try:
            topics_doc = yaml.safe_load((directory / "topics.yaml").read_text(encoding="utf-8")) or {}
            groups_doc = (
                yaml.safe_load((directory / "consumer-groups.yaml").read_text(encoding="utf-8"))
                or {}
            )
        except (OSError, yaml.YAMLError) as exc:
            raise KafkaContractError(
                f"Kafka 契约加载失败：{directory}", details={"error": str(exc)}
            ) from exc
        return cls(directory, topics_doc=topics_doc, groups_doc=groups_doc)

    @property
    def base_dir(self) -> Path:
        """契约目录（contracts/kafka）。"""
        return self._base_dir

    @property
    def topics_doc(self) -> dict[str, Any]:
        """topics.yaml 全量（含 defaults / producer_defaults / consumer_defaults / throttles）。"""
        return self._topics_doc

    @property
    def consumer_groups_doc(self) -> dict[str, Any]:
        """consumer-groups.yaml 全量（含 defaults）。"""
        return self._groups_doc

    def _index(self) -> None:
        """建立索引：平台内部 Topic / 车端 Topic（按类型）/ 广播 Topic / 消费者组。"""
        defaults = self._topics_doc.get("defaults") or {}
        for entry in self._topics_doc.get("vehicle_topics") or []:
            spec = _topic_spec_of(entry, defaults)
            if spec.name.startswith("hunter.broadcast."):
                self._broadcast[spec.name] = spec
                continue
            topic_type = spec.name.removeprefix("hunter.{vehicle_id}.")
            self._vehicle_by_type[topic_type] = spec
            self._vehicle_by_template[spec.name] = spec
        for entry in self._topics_doc.get("platform_topics") or []:
            spec = _topic_spec_of(entry, defaults)
            self._platform[spec.name] = spec
        for entry in self._groups_doc.get("groups") or []:
            self._groups[str(entry["group_id"])] = _consumer_group_spec_of(
                entry, self._groups_doc.get("defaults") or {}
            )
        logger.debug(
            "kafka_contract_indexed",
            base_dir=str(self._base_dir),
            platform_topics=len(self._platform),
            vehicle_topics=len(self._vehicle_by_type),
            consumer_groups=len(self._groups),
        )

    # ---------- Topic 解析 ----------

    @property
    def platform_topics(self) -> dict[str, TopicSpec]:
        """平台内部 Topic：{名称: spec}。"""
        return dict(self._platform)

    @property
    def vehicle_topics(self) -> dict[str, TopicSpec]:
        """车端 Topic 模板（不含广播）：{hunter.{vehicle_id}.<type>: spec}。"""
        return dict(self._vehicle_by_template)

    @property
    def broadcast_topics(self) -> dict[str, TopicSpec]:
        """广播 Topic：{hunter.broadcast.command: spec}（key 必须为空）。"""
        return dict(self._broadcast)

    def try_topic_spec(self, topic: str) -> TopicSpec | None:
        """解析 Topic，未登记返回 None。

        接受四种写法（DLQ 名 ``{topic}.dlq`` 自动剥离后按源 Topic 解析，分区数继承源 Topic）：
        契约条目名 ``telemetry_raw`` / 模板名 ``hunter.{vehicle_id}.telemetry`` /
        正则订阅写法 ``hunter.*.telemetry`` / 具体实例 ``hunter.HUNTER-001.telemetry``。
        """
        base = strip_dlq(topic)
        spec = (
            self._platform.get(base)
            or self._broadcast.get(base)
            or self._vehicle_by_template.get(base)
        )
        if spec is not None:
            return spec
        topic_type = _vehicle_type_of(base)
        if topic_type is not None:
            return self._vehicle_by_type.get(topic_type)
        return None

    def topic_spec(self, topic: str) -> TopicSpec:
        """解析 Topic，未登记抛 :class:`KafkaContractError`（禁止使用契约外 Topic）。"""
        spec = self.try_topic_spec(topic)
        if spec is None:
            raise KafkaContractError(
                f"Topic 未在 contracts/kafka/topics.yaml 登记：{topic}", details={"topic": topic}
            )
        return spec

    def has_topic(self, topic: str) -> bool:
        return self.try_topic_spec(topic) is not None

    @staticmethod
    def render_topic(template: str, vehicle_id: str) -> str:
        """模板 → 具体 Topic（``hunter.{vehicle_id}.telemetry`` + ``HUNTER-001``）。"""
        return template.replace("{vehicle_id}", vehicle_id)

    @staticmethod
    def dlq_topic(topic: str) -> str:
        """死信队列名（契约 naming.dlq_pattern：``{original_topic}.dlq``）。"""
        return f"{topic}{_DLQ_SUFFIX}"

    @staticmethod
    def is_dlq(topic: str) -> bool:
        return topic.endswith(_DLQ_SUFFIX)

    def schema_name_for_topics(self, topics: Sequence[str]) -> str | None:
        """订阅项集合解析出的 Schema 名唯一时返回该名；多义/未登记返回 None。"""
        names = set()
        for topic in topics:
            spec = self.try_topic_spec(topic)
            if spec is None or spec.schema_name is None:
                return None
            names.add(spec.schema_name)
        return names.pop() if len(names) == 1 else None

    # ---------- 消费者组 ----------

    def try_consumer_group(self, group_id: str) -> ConsumerGroupSpec | None:
        return self._groups.get(group_id)

    def consumer_group(self, group_id: str) -> ConsumerGroupSpec:
        """按契约取消费组（未登记抛 KafkaContractError；禁止自造消费组名）。"""
        spec = self._groups.get(group_id)
        if spec is None:
            raise KafkaContractError(
                f"消费组未在 contracts/kafka/consumer-groups.yaml 登记：{group_id}",
                details={"group_id": group_id},
            )
        return spec

    @property
    def consumer_groups(self) -> dict[str, ConsumerGroupSpec]:
        return dict(self._groups)

    # ---------- 消息 Schema ----------

    def schema(self, name: str) -> dict[str, Any]:
        """按逻辑名读取消息 Schema（``telemetry`` → ``schemas/telemetry.schema.json``）。

        同时校验 Schema 自身合法（draft-07），避免非法契约进入运行链路。
        """
        if name not in self._schema_cache:
            path = self._base_dir / "schemas" / f"{name}.schema.json"
            try:
                doc: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
            except OSError as exc:
                raise KafkaContractError(
                    f"消息 Schema 文件缺失：{path}", details={"schema": name}
                ) from exc
            except json.JSONDecodeError as exc:
                raise KafkaContractError(
                    f"消息 Schema 非法 JSON：{path}", details={"schema": name, "error": str(exc)}
                ) from exc
            try:
                Draft7Validator.check_schema(doc)
            except SchemaError as exc:
                raise KafkaContractError(
                    f"消息 Schema 非法 draft-07：{path}",
                    details={"schema": name, "error": exc.message},
                ) from exc
            if "$schema" in doc and "draft-07" not in str(doc["$schema"]):
                raise KafkaContractError(
                    f"消息 Schema 必须为 draft-07：{path}", details={"schema": name}
                )
            self._schema_cache[name] = doc
        return self._schema_cache[name]

    def validator(self, name: str) -> Draft7Validator:
        """取得（并缓存）draft-07 校验器。"""
        if name not in self._validator_cache:
            self._validator_cache[name] = Draft7Validator(self.schema(name))
        return self._validator_cache[name]

    def validate_message(self, schema_name: str, payload: Any) -> None:
        """校验消息体是否符合契约 Schema；不通过抛 :class:`KafkaMessageSchemaError`。

        错误详情只保留前 5 条（含字段路径），足够定位消息缺陷且避免日志膨胀。
        """
        errors = sorted(
            self.validator(schema_name).iter_errors(payload),
            key=lambda error: list(error.absolute_path),
        )
        if errors:
            raise KafkaMessageSchemaError(
                f"消息不符合契约 Schema {schema_name}",
                details={"schema": schema_name, "errors": [_format_error(e) for e in errors[:5]]},
            )


# ---------- 进程级访问 ----------

@lru_cache(maxsize=8)
def _load_cached(base_dir: str) -> KafkaContract:
    return KafkaContract.load(base_dir)


@lru_cache(maxsize=1)
def _locate_cached() -> str | None:
    found = locate_contract_dir()
    return str(found) if found is not None else None


def get_contract(
    *,
    required: bool = False,
    base_dir: str | Path | None = None,
) -> KafkaContract | None:
    """取得进程级契约单例。

    Args:
        required: True 时契约缺失直接抛 :class:`KafkaContractError`（显式启用 Schema 校验的场景
            必须 fail fast，禁止静默降级）；False 时返回 None（调用方按配置兜底）。
        base_dir: 显式契约目录（服务配置项 ``kafka_contract_dir`` 或测试注入）。
    """
    directory: Path | None
    if base_dir is not None:
        directory = locate_contract_dir(base_dir)
    else:
        cached = _locate_cached()
        directory = Path(cached) if cached is not None else None
    if directory is None:
        if required:
            raise KafkaContractError(
                "未找到 Kafka 契约目录（需含 topics.yaml / consumer-groups.yaml），"
                f"请设置环境变量 {CONTRACT_DIR_ENV}",
            )
        return None
    return _load_cached(str(directory))


def reset_contract_cache() -> None:
    """清理契约缓存（测试与配置热变更使用）。"""
    _load_cached.cache_clear()
    _locate_cached.cache_clear()


# ---------- 解析辅助 ----------

def strip_dlq(topic: str) -> str:
    """剥离 ``.dlq`` 后缀（支持 ``a.dlq.dlq`` 形式，防止重复后缀解析失败）。"""
    while topic.endswith(_DLQ_SUFFIX):
        topic = topic[: -len(_DLQ_SUFFIX)]
    return topic


def _vehicle_type_of(topic: str) -> str | None:
    """``hunter.<vehicle_id|*>.telemetry``（含模板写法）→ ``telemetry``。"""
    parts = topic.split(".")
    if len(parts) == 3 and parts[0] == "hunter" and parts[1] != "broadcast" and parts[2]:
        return parts[2]
    return None


def _format_error(error: Any) -> str:
    """JSON Schema 校验错误 → ``字段路径: 说明``（无路径记 ``<root>``）。"""
    path = "/".join(str(part) for part in error.absolute_path) or "<root>"
    return f"{path}: {error.message}"


def _topic_spec_of(entry: Mapping[str, Any], defaults: Mapping[str, Any]) -> TopicSpec:
    """topics.yaml 条目 → :class:`TopicSpec`（缺失字段回退全局 defaults）。"""
    return TopicSpec(
        name=str(entry["name"]),
        partitions=int(entry.get("partitions", 0)),
        acks=str(entry.get("acks", defaults.get("acks", "all"))),
        key=str(entry.get("key", defaults.get("key", KEY_VEHICLE_ID))),
        retention_ms=int(entry["retention_ms"]) if entry.get("retention_ms") is not None else None,
        schema=str(entry["schema"]) if entry.get("schema") else None,
        direction=str(entry["direction"]) if entry.get("direction") else None,
        producer=str(entry["producer"]) if entry.get("producer") else None,
        consumers=tuple(str(item) for item in entry.get("consumers", []) or []),
        frequency=str(entry["frequency"]) if entry.get("frequency") else None,
    )


def _consumer_group_spec_of(
    entry: Mapping[str, Any], defaults: Mapping[str, Any]
) -> ConsumerGroupSpec:
    """consumer-groups.yaml 条目 → :class:`ConsumerGroupSpec`。"""
    return ConsumerGroupSpec(
        group_id=str(entry["group_id"]),
        service=str(entry["service"]) if entry.get("service") else None,
        subscribes=tuple(str(item) for item in entry.get("subscribes", []) or []),
        produces=tuple(str(item) for item in entry.get("produces", []) or []),
        idempotency_key=str(entry["idempotency_key"]) if entry.get("idempotency_key") else None,
        dlq_enabled=bool(entry.get("dlq_enabled", defaults.get("dlq_enabled", True))),
        subscribe_mode=str(entry["subscribe_mode"]) if entry.get("subscribe_mode") else None,
        engine=str(entry["engine"]) if entry.get("engine") else None,
        target_latency=str(entry["target_latency"]) if entry.get("target_latency") else None,
    )




