"""Kafka 消息编解码（契约驱动，配合 ``contracts/kafka/schemas/*.schema.json``）。

- ``encode_payload`` / ``decode_payload``：JSON 字节级往返（UTF-8，非 ASCII 不转义，紧凑分隔符）；
- ``build_record``：生产侧组装记录并强制契约约束——``key = vehicle_id``（Topic 契约
  ``key: vehicle_id``）、广播 Topic ``key: none``、消息体按契约 Schema 校验
  （``additionalProperties=false``，字段名/类型不可更改）；
- ``parse_payload``：消费侧解码 + 契约 Schema 校验（不通过抛 :class:`KafkaMessageSchemaError`，
  由消费者转投 DLQ，不重试）。

契约缺失时的行为由 ``contract_required`` 控制：显式启用 Schema 校验的场景必须 fail fast，
禁止静默跳过校验（避免"契约漂移悄悄上线"）。
"""
from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from hunter_common.kafka.contracts import (
    KafkaContract,
    KafkaContractError,
    KafkaMessageSchemaError,
    TopicSpec,
    get_contract,
)


@dataclass(frozen=True, slots=True)
class KafkaRecord:
    """待投递记录（字节级，可直接交给 confluent-kafka 的 produce）。"""

    topic: str
    value: bytes
    key: bytes | None = None
    headers: tuple[tuple[str, bytes], ...] = ()

    def headers_list(self) -> list[tuple[str, bytes]]:
        """confluent-kafka 的 headers 需要 list 形态。"""
        return list(self.headers)


def vehicle_key(vehicle_id: str) -> bytes:
    """消息 key = vehicle_id（UTF-8 字节），保证单车辆消息在分区内有序。"""
    if not vehicle_id:
        raise KafkaMessageSchemaError("vehicle_id 不可为空（消息 key 必须等于 vehicle_id）")
    return vehicle_id.encode("utf-8")


def encode_payload(payload: Mapping[str, Any] | str | bytes) -> bytes:
    """序列化消息体为 JSON 字节（bytes/str 原样透传，避免已有编码被二次包装）。"""
    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, str):
        return payload.encode("utf-8")
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def decode_payload(raw: bytes | str | None) -> Any:
    """反序列化消息体；非法 JSON 抛 :class:`KafkaMessageSchemaError`（消费侧转 DLQ）。"""
    if raw is None:
        raise KafkaMessageSchemaError("消息体为空，无法解析（契约要求 JSON 对象）")
    text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise KafkaMessageSchemaError(
            "消息体不是合法 JSON", details={"error": str(exc), "length": len(text)}
        ) from exc


def resolve_schema_name(
    topic: str,
    *,
    schema_name: str | None = None,
    contract: KafkaContract | None = None,
    contract_required: bool = False,
) -> str | None:
    """确定 Topic 对应的 Schema 逻辑名（显式指定优先，否则按契约推导）。

    Raises:
        KafkaContractError: 契约缺失且 ``contract_required=True``；
            或契约存在但 Topic/显式 Schema 未登记（禁止契约外 Topic 与 Schema）。
    """
    resolved = contract if contract is not None else get_contract(required=contract_required)
    if resolved is None:
        return schema_name
    if schema_name:
        resolved.schema(schema_name)  # 不存在则抛 KafkaContractError（fail fast）
        return schema_name
    return resolved.topic_spec(topic).schema_name


def build_record(
    topic: str,
    payload: Mapping[str, Any] | str | bytes,
    *,
    key: str | bytes | None = None,
    headers: Mapping[str, str] | list[tuple[str, bytes]] | None = None,
    schema_name: str | None = None,
    contract: KafkaContract | None = None,
    contract_required: bool = False,
) -> KafkaRecord:
    """按契约组装生产记录（key 规则 + Schema 校验 + 序列化）。

    Args:
        topic: 契约登记 Topic（模板名 ``hunter.{vehicle_id}.telemetry``、具体实例、
            平台内部名或 DLQ 名均可解析）。
        payload: 消息体（Mapping 会按契约 Schema 校验；bytes/str 视为已编码，跳过校验）。
        key: 显式 key；契约要求 ``key=vehicle_id`` 时必须与消息体 ``vehicle_id`` 一致。
        schema_name: 覆盖契约推导的 Schema（默认取 Topic 契约的 ``schema`` 字段）。
    """
    resolved = contract if contract is not None else get_contract(required=contract_required)
    spec = _require_spec(topic, resolved, contract_required)
    target_schema = schema_name or (spec.schema_name if spec else None)
    if target_schema and isinstance(payload, Mapping):
        if resolved is None:  # pragma: no cover - _require_spec 已保证
            raise KafkaContractError("缺少契约，无法校验消息体 Schema")
        resolved.validate_message(target_schema, dict(payload))
    return KafkaRecord(
        topic=topic,
        value=encode_payload(payload),
        key=_enforce_key_contract(topic, payload, key, spec),
        headers=_normalize_headers(headers),
    )


def parse_payload(
    topic: str,
    raw: bytes | str | None,
    *,
    schema_name: str | None = None,
    contract: KafkaContract | None = None,
    contract_required: bool = False,
) -> Any:
    """消费侧解码 + 契约 Schema 校验（非法消息抛 KafkaMessageSchemaError → DLQ）。"""
    resolved = contract if contract is not None else get_contract(required=contract_required)
    spec = _require_spec(topic, resolved, contract_required)
    payload = decode_payload(raw)
    target_schema = schema_name or (spec.schema_name if spec else None)
    if target_schema and resolved is not None:
        resolved.validate_message(target_schema, payload)
    return payload


# ---------- 内部 ----------

def _require_spec(
    topic: str,
    contract: KafkaContract | None,
    contract_required: bool,
) -> TopicSpec | None:
    """Topic 契约解析：契约不可用时不额外约束；可用但未登记则报错（禁止契约外 Topic）。"""
    if contract is None:
        if contract_required:
            raise KafkaContractError(f"契约不可用，无法校验 Topic：{topic}")
        return None
    return contract.topic_spec(topic)


def _enforce_key_contract(
    topic: str,
    payload: Mapping[str, Any] | str | bytes,
    key: str | bytes | None,
    spec: TopicSpec | None,
) -> bytes | None:
    """key 契约执行：vehicle_id 强制一致 / 广播 Topic 必须无 key。"""
    if isinstance(key, bytes):
        key_bytes: bytes | None = key
    elif key is None:
        key_bytes = None
    else:
        key_bytes = key.encode("utf-8")

    if spec is None:  # 无契约信息：保持调用方原样（不做额外约束）
        return key_bytes

    if spec.is_broadcast:
        if key_bytes is not None:
            raise KafkaMessageSchemaError(
                f"广播 Topic {topic} 不允许携带 key（契约 key: none）", details={"topic": topic}
            )
        return None

    if not spec.requires_vehicle_key:
        return key_bytes

    payload_vehicle = payload.get("vehicle_id") if isinstance(payload, Mapping) else None
    if payload_vehicle is None:
        raise KafkaMessageSchemaError(
            f"{topic} 契约要求 key=vehicle_id，但消息体缺少 vehicle_id", details={"topic": topic}
        )
    expected = vehicle_key(str(payload_vehicle))
    if key_bytes is None:
        return expected
    if key_bytes != expected:
        raise KafkaMessageSchemaError(
            f"{topic} 消息 key 必须等于 vehicle_id（单车辆有序）",
            details={"topic": topic, "key": key_bytes.decode("utf-8", "replace")},
        )
    return key_bytes


def _normalize_headers(
    headers: Mapping[str, str] | list[tuple[str, bytes]] | None,
) -> tuple[tuple[str, bytes], ...]:
    """headers 归一为 ``tuple[(str, bytes)]``（明文 str 值按 UTF-8 编码）。"""
    if not headers:
        return ()
    if isinstance(headers, Mapping):
        return tuple((str(name), value.encode("utf-8")) for name, value in headers.items())
    return tuple((str(name), value) for name, value in headers)


