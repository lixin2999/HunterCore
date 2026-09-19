"""Kafka 消息契约 round-trip 单测：契约解析 / 编解码往返 / key 规则 / Schema 校验。

契约来源：``contracts/kafka/{topics.yaml, consumer-groups.yaml, schemas/*.schema.json}``；
示例消息取 Schema 的 ``examples[0]``（禁止在测试中手写消息体，避免与契约漂移）。
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from hunter_common.config import HunterBaseConfig
from hunter_common.kafka.contracts import (
    KafkaContract,
    KafkaContractError,
    KafkaMessageSchemaError,
    get_contract,
)
from hunter_common.kafka.messages import (
    build_record,
    decode_payload,
    encode_payload,
    parse_payload,
    vehicle_key,
)

ROOT = Path(__file__).resolve().parents[3]
CONTRACT_DIR = ROOT / "contracts" / "kafka"
VEHICLE_ID = "HUNTER-001"


@pytest.fixture(scope="module")
def contract() -> KafkaContract:
    return KafkaContract.load(CONTRACT_DIR)


def example(contract: KafkaContract, name: str) -> dict[str, Any]:
    """契约示例消息（单一事实来源）。"""
    payload: dict[str, Any] = contract.schema(name)["examples"][0]
    return payload


def test_get_contract_locates_repository_contracts() -> None:
    """进程级契约单例可定位仓库 contracts/kafka（工作目录/包位置向上查找）。"""
    loaded = get_contract(required=True)
    assert loaded is not None
    assert (loaded.base_dir / "topics.yaml").is_file()


def test_get_contract_required_raises_on_missing_dir(tmp_path: Path) -> None:
    """显式指定无效契约目录 + required=True：fail fast（禁止静默降级）。"""
    with pytest.raises(KafkaContractError):
        get_contract(required=True, base_dir=tmp_path)
    assert get_contract(base_dir=tmp_path) is None


def test_blank_contract_dir_falls_back_to_autodetection(monkeypatch: pytest.MonkeyPatch) -> None:
    """KAFKA_CONTRACT_DIR 留空（.env 常见写法）不应关闭自动探测。"""
    monkeypatch.setenv("KAFKA_CONTRACT_DIR", "")
    config = HunterBaseConfig(kafka_contract_dir="   ")
    assert config.kafka_contract_dir is None
    loaded = get_contract(base_dir=config.kafka_contract_dir, required=True)
    assert loaded is not None and (loaded.base_dir / "topics.yaml").is_file()


def test_topic_spec_resolves_all_notations(contract: KafkaContract) -> None:
    """模板名 / 正则写法 / 具体实例 / 平台内部名 / 广播 / DLQ 六种写法均可解析。"""
    template = contract.topic_spec("hunter.{vehicle_id}.telemetry")
    assert contract.topic_spec("hunter.*.telemetry") is template
    assert contract.topic_spec(f"hunter.{VEHICLE_ID}.telemetry") is template
    assert contract.topic_spec("hunter.*.telemetry.dlq") is template
    assert contract.is_dlq("hunter.*.telemetry.dlq") is True
    assert contract.dlq_topic("telemetry_raw") == "telemetry_raw.dlq"
    assert contract.render_topic(template.name, VEHICLE_ID) == f"hunter.{VEHICLE_ID}.telemetry"
    assert contract.topic_spec("hunter.broadcast.command").is_broadcast is True
    with pytest.raises(KafkaContractError):
        contract.topic_spec("hunter.HUNTER-001.not_a_type")
    assert contract.has_topic("telemetry_raw") is True


def test_topic_contract_values_match_design_doc(contract: KafkaContract) -> None:
    """分区数 / acks / key 策略与设计文档一致（不可放宽）。"""
    telemetry = contract.topic_spec("hunter.*.telemetry")
    assert (telemetry.partitions, telemetry.acks, telemetry.key) == (6, "1", "vehicle_id")
    assert telemetry.schema_name == "telemetry"
    assert telemetry.requires_vehicle_key is True

    health = contract.topic_spec("hunter.*.health")
    assert (health.partitions, health.acks) == (3, "0")

    raw = contract.topic_spec("telemetry_raw")
    assert (raw.partitions, raw.acks, raw.retention_ms) == (12, "all", 604_800_000)

    assert contract.topic_spec("hunter.broadcast.command").key == "none"


def test_consumer_group_contract(contract: KafkaContract) -> None:
    """消费组订阅 / 幂等键 / DLQ 开关取自契约。"""
    group = contract.consumer_group("data-collector-telemetry")
    assert group.subscribes == ("hunter.*.telemetry",)
    assert group.produces == ("telemetry_raw", "telemetry_clean")
    assert group.idempotency_key
    assert group.dlq_enabled is True
    assert contract.schema_name_for_topics(list(group.subscribes)) == "telemetry"
    with pytest.raises(KafkaContractError):
        contract.consumer_group("not-registered-group")
    assert contract.try_consumer_group("not-registered-group") is None


def test_telemetry_roundtrip_preserves_payload(contract: KafkaContract) -> None:
    """生产编码 → 消费解码：消息体逐字段一致，key = vehicle_id。"""
    payload = example(contract, "telemetry")
    topic = f"hunter.{VEHICLE_ID}.telemetry"
    record = build_record(topic, payload, contract=contract)

    assert record.key == vehicle_key(VEHICLE_ID)
    assert decode_payload(record.value) == payload
    assert parse_payload(topic, record.value, contract=contract) == payload
    # 紧凑 JSON（无多余空格）+ 非 ASCII 不转义
    assert b", " not in record.value
    assert encode_payload({"a": "中文"}) == '{"a":"中文"}'.encode()


def test_key_is_derived_from_payload_and_mismatch_rejected(contract: KafkaContract) -> None:
    """key 由消息体 vehicle_id 推导；显式 key 不一致 → 拒绝（单车辆有序约束）。"""
    payload = example(contract, "telemetry")
    topic = "hunter.{vehicle_id}.telemetry"
    record = build_record(topic, payload, contract=contract)
    assert record.key == payload["vehicle_id"].encode("utf-8")

    with pytest.raises(KafkaMessageSchemaError):
        build_record(topic, payload, key="HUNTER-999", contract=contract)

    without_vehicle = {key: value for key, value in payload.items() if key != "vehicle_id"}
    with pytest.raises(KafkaMessageSchemaError):
        build_record(topic, without_vehicle, contract=contract)


def test_broadcast_topic_forbids_key(contract: KafkaContract) -> None:
    """广播 Topic（契约 key: none）不允许携带 key。"""
    payload = example(contract, "command")
    record = build_record("hunter.broadcast.command", payload, contract=contract)
    assert record.key is None
    with pytest.raises(KafkaMessageSchemaError):
        build_record("hunter.broadcast.command", payload, key=VEHICLE_ID, contract=contract)


def test_schema_violation_reports_field_paths(contract: KafkaContract) -> None:
    """非法消息（缺必填段 / 多字段）→ 错误码 2001，详情含字段路径。"""
    payload = example(contract, "telemetry")
    broken = dict(payload)
    broken.pop("chassis")
    broken["unexpected_field"] = 1

    with pytest.raises(KafkaMessageSchemaError) as excinfo:
        contract.validate_message("telemetry", broken)
    errors = excinfo.value.details["errors"]
    assert isinstance(errors, list) and errors
    assert any("chassis" in str(item) for item in errors)
    assert excinfo.value.code == 2001


def test_platform_topic_roundtrip_sensor_file(contract: KafkaContract) -> None:
    """平台内部 Topic（sensor_file）同样按契约校验与往返。"""
    payload = example(contract, "sensor_file")
    record = build_record("sensor_file", payload, contract=contract)
    assert record.key == payload["vehicle_id"].encode("utf-8")
    assert parse_payload("sensor_file", record.value, contract=contract) == payload


def test_unregistered_topic_rejected_when_contract_available(contract: KafkaContract) -> None:
    """契约可用时禁止投递契约外 Topic（避免「先实现后补契约」）。"""
    with pytest.raises(KafkaContractError):
        build_record("hunter_core.not_registered", {"vehicle_id": VEHICLE_ID}, contract=contract)


def test_decode_rejects_invalid_json() -> None:
    """非法 JSON 消息体 → 2001（消费侧转 DLQ）。"""
    with pytest.raises(KafkaMessageSchemaError):
        decode_payload(b"{not-json")
    with pytest.raises(KafkaMessageSchemaError):
        decode_payload(None)


def test_schema_file_is_valid_draft7(contract: KafkaContract) -> None:
    """Schema 加载时校验自身合法（draft-07）并缓存。"""
    schema = contract.schema("telemetry")
    assert "draft-07" in schema["$schema"]
    assert contract.schema("telemetry") is schema
    assert json.dumps(schema).startswith("{")
    with pytest.raises(KafkaContractError):
        contract.schema("not_a_schema")

