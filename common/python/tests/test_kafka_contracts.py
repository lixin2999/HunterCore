"""Kafka 契约测试：topics.yaml / consumer-groups.yaml / 消息 JSON Schema ↔ 代码受控词表一致性。"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

from hunter_common.database.enums import EventLevel, EventType, OtaStatus, VehicleStatus

ROOT = Path(__file__).resolve().parents[3]
KAFKA_DIR = ROOT / "contracts" / "kafka"
SCHEMA_DIR = KAFKA_DIR / "schemas"
COMPOSE_SCRIPT = ROOT / "infra" / "docker" / "kafka" / "create-topics.sh"
K8S_JOB = ROOT / "infra" / "k8s" / "jobs" / "kafka-init-job.yaml"

#: 平台内部 Topic（分区数, 保留毫秒）——设计文档契约值
EXPECTED_PLATFORM_TOPICS: dict[str, tuple[int, int]] = {
    "telemetry_raw": (12, 604_800_000),
    "telemetry_clean": (12, 604_800_000),
    "event_raw": (6, 2_592_000_000),
    "sensor_file": (3, 604_800_000),
    "analytics_result": (6, 2_592_000_000),
    "alert_event": (3, 2_592_000_000),
}

#: 车端 Topic（分区数, acks, 频率）
EXPECTED_VEHICLE_TOPICS: dict[str, tuple[int, str, str]] = {
    "hunter.{vehicle_id}.telemetry": (6, "1", "10-50Hz"),
    "hunter.{vehicle_id}.event": (3, "all", "事件触发"),
    "hunter.{vehicle_id}.health": (3, "0", "1Hz"),
    "hunter.{vehicle_id}.command": (3, "all", "按需"),
    "hunter.{vehicle_id}.command_result": (3, "all", "按需"),
    "hunter.{vehicle_id}.ota_notify": (3, "all", "按需"),
    "hunter.{vehicle_id}.ota_status": (3, "all", "状态变更"),
    "hunter.{vehicle_id}.remote_control": (3, "all", "20Hz"),
    "hunter.broadcast.command": (3, "all", "按需"),
}

SCHEMA_FILES = (
    "telemetry.schema.json",
    "event.schema.json",
    "health.schema.json",
    "command.schema.json",
    "command_result.schema.json",
    "ota_notify.schema.json",
    "ota_status.schema.json",
    "remote_control.schema.json",
    "analytics_result.schema.json",
    "sensor_file.schema.json",
    "alert_event.schema.json",
)

CREATE_TOPIC_CALL_RE = re.compile(r'create_topic\s+"([\w.]+)"\s+(\d+)\s+(\d+)')


def load_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def load_schema(name: str) -> dict[str, Any]:
    return json.loads((SCHEMA_DIR / name).read_text(encoding="utf-8"))


def parse_create_topic_calls(text: str) -> dict[str, tuple[int, int]]:
    return {
        name: (int(partitions), int(retention))
        for name, partitions, retention in CREATE_TOPIC_CALL_RE.findall(text)
    }


def test_platform_topic_contract_values() -> None:
    topics = load_yaml(KAFKA_DIR / "topics.yaml")
    platform = {entry["name"]: entry for entry in topics["platform_topics"]}
    assert set(platform) == set(EXPECTED_PLATFORM_TOPICS)
    for name, (partitions, retention) in EXPECTED_PLATFORM_TOPICS.items():
        assert platform[name]["partitions"] == partitions
        assert platform[name]["retention_ms"] == retention


def test_vehicle_topic_contract_values() -> None:
    topics = load_yaml(KAFKA_DIR / "topics.yaml")
    vehicle = {entry["name"]: entry for entry in topics["vehicle_topics"]}
    assert set(vehicle) == set(EXPECTED_VEHICLE_TOPICS)
    for name, (partitions, acks, frequency) in EXPECTED_VEHICLE_TOPICS.items():
        assert vehicle[name]["partitions"] == partitions
        assert str(vehicle[name]["acks"]) == acks
        assert vehicle[name]["frequency"] == frequency
    # 车端消息 key = vehicle_id（单车辆有序）；广播 Topic 例外
    assert vehicle["hunter.{vehicle_id}.telemetry"]["key"] == "vehicle_id"
    assert vehicle["hunter.broadcast.command"]["key"] == "none"


def test_topic_defaults_follow_security_and_producer_baseline() -> None:
    topics = load_yaml(KAFKA_DIR / "topics.yaml")
    assert topics["defaults"]["security_protocol"] == "SASL_SSL"
    assert topics["defaults"]["sasl_mechanism"] == "SCRAM-SHA-512"
    producer = topics["producer_defaults"]
    assert producer["linger_ms"] == 5
    assert producer["batch_size"] == 16384
    assert producer["retries"] == 3
    assert producer["local_disk_buffer_bytes"] == 1024**3
    assert topics["consumer_defaults"]["enable_auto_commit"] is False
    assert topics["naming"]["dlq_pattern"] == "{original_topic}.dlq"


def test_topics_yaml_synced_with_compose_and_k8s_scripts() -> None:
    compose_calls = parse_create_topic_calls(COMPOSE_SCRIPT.read_text(encoding="utf-8"))
    assert compose_calls == EXPECTED_PLATFORM_TOPICS

    k8s_script = ""
    for doc in yaml.safe_load_all(K8S_JOB.read_text(encoding="utf-8")):
        if doc and doc.get("kind") == "ConfigMap":
            k8s_script = (doc.get("data") or {}).get("create-topics.sh", "")
    assert k8s_script, "kafka-init Job 缺少 create-topics.sh"
    assert parse_create_topic_calls(k8s_script) == EXPECTED_PLATFORM_TOPICS


def test_all_schemas_are_valid_draft7_with_examples() -> None:
    """11 个消息 Schema：draft-07 合法、required 非空、examples 通过自身校验。"""
    jsonschema = pytest.importorskip("jsonschema")
    for name in SCHEMA_FILES:
        schema = load_schema(name)
        assert schema["$schema"] == "http://json-schema.org/draft-07/schema#", name
        jsonschema.Draft7Validator.check_schema(schema)
        assert schema.get("required"), f"{name} 缺少 required"
        examples = schema.get("examples") or []
        assert examples, f"{name} 缺少 examples"
        validator = jsonschema.Draft7Validator(schema)
        for example in examples:
            errors = sorted(validator.iter_errors(example), key=lambda error: list(error.path))
            assert not errors, f"{name}: {errors[0].message}"


def test_telemetry_schema_covers_design_doc_example() -> None:
    """遥测 Schema 必须覆盖设计文档六段结构，且示例消息通过校验。"""
    jsonschema = pytest.importorskip("jsonschema")
    schema = load_schema("telemetry.schema.json")
    assert set(schema["required"]) == {
        "vehicle_id",
        "timestamp",
        "seq",
        "chassis",
        "localization",
        "perception",
        "planning",
        "control",
        "system",
    }
    assert schema["properties"]["chassis"]["properties"]["battery_soc"]["maximum"] == 100
    example = schema["examples"][0]
    jsonschema.validate(example, schema)

    # 消息字段 ↔ 时序表列名映射（chassis/system 段为扁平化列）
    from hunter_common.database.base import Base

    columns = set(Base.metadata.tables["data_collector.vehicle_telemetry"].columns.keys())
    for block in ("chassis", "localization", "perception", "planning", "control", "system"):
        for field in example[block]:
            assert field in columns, f"遥测字段 {block}.{field} 未映射到时序表列"


def test_event_schema_enums_match_code_contract() -> None:
    schema = load_schema("event.schema.json")
    assert set(schema["properties"]["event_type"]["enum"]) == {t.value for t in EventType}
    assert set(schema["properties"]["event_level"]["enum"]) == {level.value for level in EventLevel}
    assert len(schema["properties"]["event_type"]["enum"]) == 18


def test_ota_status_schema_matches_state_machine() -> None:
    schema = load_schema("ota_status.schema.json")
    expected = {status.value for status in OtaStatus}
    assert set(schema["properties"]["status"]["enum"]) == expected
    assert set(schema["properties"]["phase"]["enum"]) == expected
    assert schema["properties"]["progress"]["minimum"] == 0
    assert schema["properties"]["progress"]["maximum"] == 100


def test_health_schema_status_matches_vehicle_states() -> None:
    schema = load_schema("health.schema.json")
    assert set(schema["properties"]["status"]["enum"]) == {s.value for s in VehicleStatus}
    assert set(schema["properties"]["system"]["required"]) == set(
        load_schema("telemetry.schema.json")["properties"]["system"]["required"]
    ), "health.system 段必须与遥测 system 段字段一致（复用采集代码）"


def test_remote_control_schema_safety_constraints() -> None:
    """远程操控安全约束：20Hz + 速度上限 2.0 m/s + 序号单调 + 心跳标记。"""
    schema = load_schema("remote_control.schema.json")
    assert schema["properties"]["control"]["properties"]["target_velocity"]["maximum"] == 2.0
    assert schema["properties"]["heartbeat"]["default"] is False
    assert "seq" in schema["required"]
    topics = load_yaml(KAFKA_DIR / "topics.yaml")
    vehicle = {entry["name"]: entry for entry in topics["vehicle_topics"]}
    assert vehicle["hunter.{vehicle_id}.remote_control"]["frequency"] == "20Hz"


def test_ota_notify_schema_requires_integrity_and_signature_fields() -> None:
    schema = load_schema("ota_notify.schema.json")
    required = set(schema["required"])
    assert {"package_md5", "package_sha256", "signature", "version_code", "task_id"} <= required
    assert schema["properties"]["package_md5"]["pattern"] == "^[0-9a-f]{32}$"
    assert schema["properties"]["package_sha256"]["pattern"] == "^[0-9a-f]{64}$"
    assert set(schema["properties"]["preconditions"]["properties"]) == {
        "soc_min",
        "must_be_parked",
        "network_stable",
        "min_storage_mb",
    }


def test_command_schema_requires_vehicle_or_broadcast_target() -> None:
    """单车辆指令与广播指令二选一（oneOf），command_id 为幂等键。"""
    schema = load_schema("command.schema.json")
    assert "oneOf" in schema
    assert "command_id" in schema["required"]


def test_consumer_groups_only_reference_registered_topics() -> None:
    groups = load_yaml(KAFKA_DIR / "consumer-groups.yaml")["groups"]
    known = set(EXPECTED_PLATFORM_TOPICS) | set(EXPECTED_VEHICLE_TOPICS)
    known |= {name.replace("{vehicle_id}", "*") for name in EXPECTED_VEHICLE_TOPICS}
    known |= {f"{name}.dlq" for name in known}

    group_ids = [group["group_id"] for group in groups]
    assert len(group_ids) == len(set(group_ids)), "消费者组 ID 必须唯一"
    for group in groups:
        assert re.fullmatch(r"[a-z0-9]+(-[a-z0-9]+)*", group["group_id"])
        assert group["idempotency_key"], f"{group['group_id']} 缺少幂等键说明"
        for topic in group["subscribes"]:
            assert topic in known, f"{group['group_id']} 订阅未登记 Topic: {topic}"



def test_analytics_result_schema_matches_scene_extraction_contract() -> None:
    """实车场景自动提取契约（设计文档 4.5 节）：触发事件 + 前后各 10 秒窗口 + 三项提取内容。"""
    schema = load_schema("analytics_result.schema.json")
    properties = schema["properties"]

    assert set(properties["result_type"]["enum"]) == {"metric", "corner_case", "report"}
    # 触发事件类型必须取自受控词表（contracts/database/enums.md 第 3 节，18 种）
    trigger_enum = set(properties["trigger_event_type"]["enum"])
    assert trigger_enum <= {event_type.value for event_type in EventType}
    assert {"harsh_braking", "collision_warning", "manual_takeover"} <= trigger_enum, (
        "4.5 节触发条件：急刹车 / 碰撞预警 / 人工接管"
    )
    assert set(properties["event_level"]["enum"]) == {level.value for level in EventLevel}

    # 截取窗口固定为事件前后各 10 秒（设计文档 4.5 节，不可配置）
    clip = properties["clip"]
    assert clip["properties"]["pre_seconds"]["enum"] == [10]
    assert clip["properties"]["post_seconds"]["enum"] == [10]
    assert set(clip["required"]) == {"pre_seconds", "post_seconds"}

    # 4.5 节提取内容：自车轨迹 / 周边目标轨迹 / 环境条件
    assert {"ego_trajectory", "objects", "environment"} <= set(properties)
    point = schema["definitions"]["trajectory_point"]
    assert set(point["required"]) == {"timestamp", "x", "y", "heading", "velocity"}
    track = schema["definitions"]["object_track"]
    assert set(track["properties"]["type"]["enum"]) == {"vehicle", "pedestrian", "other"}

    # 契约三处一致：topics.yaml 引用 / 消费组登记 / Schema 文件
    platform = {
        entry["name"]: entry for entry in load_yaml(KAFKA_DIR / "topics.yaml")["platform_topics"]
    }
    assert platform["analytics_result"]["schema"] == "schemas/analytics_result.schema.json"
    groups = load_yaml(KAFKA_DIR / "consumer-groups.yaml")["groups"]
    scene_group = next(g for g in groups if g["group_id"] == "scene-service-analytics-result")
    assert scene_group["subscribes"] == ["analytics_result"]
    assert scene_group["produces"] == [], "scene-service 不生产 Kafka 消息（提取结果直接落库）"


def test_all_platform_internal_topic_schemas_are_defined() -> None:
    """平台内部 Topic 消息结构必须全部已定义（禁止「先实现后补契约」）。"""
    platform = {
        entry["name"]: entry for entry in load_yaml(KAFKA_DIR / "topics.yaml")["platform_topics"]
    }
    for name in EXPECTED_PLATFORM_TOPICS:
        assert platform[name]["schema"], f"{name} 仍为 schema: null（必须先补契约）"
        assert (SCHEMA_DIR / platform[name]["schema"].split("/")[-1]).is_file()
    # alert_event：data-analytics Flink 告警（6.2 节）已补全 Schema
    assert platform["alert_event"]["producer"] == "data-analytics"
    assert platform["alert_event"]["schema"] == "schemas/alert_event.schema.json"
    assert platform["alert_event"]["idempotency_key"] == "(vehicle_id, alert_id)"


def test_alert_event_schema_matches_realtime_job_contract() -> None:
    """alert_event（6.2 节 Flink 告警）：类型/等级取自受控词表，source_job = 5 个实时作业。"""
    schema = load_schema("alert_event.schema.json")
    properties = schema["properties"]
    assert schema["additionalProperties"] is False
    assert set(properties["alert_type"]["enum"]) == {t.value for t in EventType}
    assert set(properties["level"]["enum"]) == {level.value for level in EventLevel}
    assert set(properties["source_job"]["enum"]) == {
        "vehicle_state_monitor",
        "driving_anomaly_detection",
        "algorithm_performance_monitor",
        "collision_risk_assessment",
        "data_quality_monitor",
    }
    # 等级必须等于受控映射（示例逐条校验，防止实现放宽等级）
    from hunter_common.database.enums import EVENT_LEVEL_BY_TYPE

    for example in schema["examples"]:
        assert example["level"] == EVENT_LEVEL_BY_TYPE[EventType(example["alert_type"])].value
    # 阈值与规则元信息完整（实现侧禁止硬编码：阈值必须在 rule 中回带）
    rule = properties["rule"]
    assert set(rule["required"]) == {"name", "metric", "comparator", "threshold"}
    assert set(rule["properties"]["comparator"]["enum"]) == {"gt", "gte", "lt", "lte"}
    assert properties["alert_id"]["pattern"].startswith("^[0-9a-fA-F]{8}")

    # 消费组登记（告警消费方落位待定但消费组与幂等键必须已登记）
    groups = load_yaml(KAFKA_DIR / "consumer-groups.yaml")["groups"]
    alert_group = next(g for g in groups if g["group_id"] == "platform-alert-event")
    assert alert_group["subscribes"] == ["alert_event"]
    assert alert_group["idempotency_key"] == "(vehicle_id, alert_id)"


def test_sensor_file_schema_matches_upload_contract() -> None:
    """sensor_file（设计文档 5.5 节上传完成通知）：命名规范 + 校验值 + 消费幂等键四处一致。"""
    schema = load_schema("sensor_file.schema.json")
    assert schema["additionalProperties"] is False
    assert set(schema["required"]) == {
        "vehicle_id",
        "timestamp",
        "bucket",
        "object_key",
        "data_type",
        "size_bytes",
        "md5",
        "sha256",
    }
    # Bucket 枚举 = 车端上传类 Bucket（MinIO Bucket 规划）
    assert set(schema["properties"]["bucket"]["enum"]) == {
        "hunter-raw-data",
        "hunter-rosbag",
        "hunter-video",
    }
    # 完整性校验字段与 MinIO 契约一致
    assert schema["properties"]["md5"]["pattern"] == "^[0-9a-f]{32}$"
    assert schema["properties"]["sha256"]["pattern"] == "^[0-9a-f]{64}$"
    # 5.5 节命名规范：{bucket}/{vehicle_id}/{date}/{data_type}/{timestamp}_{seq}.{ext}
    key_pattern = schema["properties"]["object_key"]["pattern"]
    for fragment in ("hunter-(raw-data|rosbag|video)", "[0-9]{4}-[0-9]{2}-[0-9]{2}", "[0-9]+_[0-9]+"):
        assert fragment in key_pattern, f"object_key 未约束命名规范片段: {fragment}"

    # 契约三处一致：topics.yaml 引用（含生产者）/ Schema 文件存在 / 消费组幂等键
    platform = {
        entry["name"]: entry for entry in load_yaml(KAFKA_DIR / "topics.yaml")["platform_topics"]
    }
    assert platform["sensor_file"]["schema"] == "schemas/sensor_file.schema.json"
    assert platform["sensor_file"]["producer"] == "data-collector"
    assert (SCHEMA_DIR / "sensor_file.schema.json").is_file()
    groups = load_yaml(KAFKA_DIR / "consumer-groups.yaml")["groups"]
    analytics_group = next(g for g in groups if g["group_id"] == "data-analytics-sensor-file")
    assert analytics_group["subscribes"] == ["sensor_file"]
    assert analytics_group["idempotency_key"] == "object_key"

