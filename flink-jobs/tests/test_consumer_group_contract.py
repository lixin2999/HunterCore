"""Flink 作业消费组与 ``contracts/kafka/consumer-groups.yaml`` 的契约一致性单测（V1.19.10）。

背景（本用例存在的理由）：原契约把 4 个 data-analytics 实时作业挂在**同一个** ``data-analytics-telemetry``
组下，而 ``detection_job`` 与 ``algorithm_performance_job`` 也确实各自 ``set_group_id`` 到该组。
Kafka 的 ``group.id`` 是**消费进度（committed offset）的命名空间**，不是逻辑归属标签：两个独立作业
（各自的 KafkaConsumer 实例）共用一组时

- 提交位点互相覆盖 → 组 ``LAG`` 监控失真、冷启动 ``committed_offsets(EARLIEST)`` 起点被另一作业带偏；
- 若分区分配交由 broker，则两作业**各只拿到一半分区**（告警覆盖面腰斩且不报任何错）。

两者都是"作业 RUNNING 但结果错"的形态，故在此把纪律固化为可执行断言（纯 Python，无需 PyFlink）。
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml
from hunter_flink import algorithm_performance_job, detection_job, ingest_latency_job

ROOT = Path(__file__).resolve().parents[2]
CONSUMER_GROUPS_PATH = ROOT / "contracts" / "kafka" / "consumer-groups.yaml"
CONTRACT_PATH = ROOT / "contracts" / "openapi" / "data-analytics.yaml"

#: group_id 命名规范（consumer-groups.yaml 文件头 + scripts/verify_data_layer.py 同源校验）
GROUP_ID_PATTERN = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")

#: 已交付作业 → (消费组常量, 输入 Topic, 契约作业名)。顺序即 flink-jobs/README.md 作业表
DELIVERED_JOBS: tuple[tuple[Any, str, str, str], ...] = (
    (detection_job, "data-analytics-telemetry-alert", "telemetry_clean", "driving_anomaly_detection"),
    (
        algorithm_performance_job,
        "data-analytics-telemetry-algo",
        "telemetry_clean",
        "algorithm_performance_monitor",
    ),
    (ingest_latency_job, "data-analytics-telemetry-raw", "telemetry_raw", "data_quality_monitor"),
)


def _load(path: Path) -> dict[str, Any]:
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict), path
    return loaded


@pytest.fixture(scope="module")
def groups() -> dict[str, dict[str, Any]]:
    entries = _load(CONSUMER_GROUPS_PATH)["groups"]
    assert isinstance(entries, list)
    return {entry["group_id"]: entry for entry in entries}


@pytest.fixture(scope="module")
def contract() -> dict[str, Any]:
    return _load(CONTRACT_PATH)


@pytest.mark.parametrize(
    "module,group_id,input_topic,job_name",
    DELIVERED_JOBS,
    ids=[module.__name__.rsplit(".", 1)[-1] for module, *_ in DELIVERED_JOBS],
)
def test_job_consumer_group_matches_contract(
    module: Any,
    group_id: str,
    input_topic: str,
    job_name: str,
    groups: dict[str, dict[str, Any]],
) -> None:
    """作业侧 ``CONSUMER_GROUP`` / ``INPUT_TOPIC`` 必须与契约组条目逐项相等（禁止各自演化）。"""
    assert module.CONSUMER_GROUP == group_id, f"{module.__name__} 组名与契约不一致"
    assert module.INPUT_TOPIC == input_topic, f"{module.__name__} 输入 Topic 与契约不一致"

    group = groups[group_id]
    assert group["service"] == "data-analytics"
    assert group["subscribes"] == [input_topic], (group_id, group["subscribes"])
    assert group["jobs"] == [job_name], f"{group_id} 应只承载 {job_name}（一组一作业）"
    # 已/部分交付组的 status 必须点名实现文件，避免"契约称已交付、代码里找不到作业"的漂移
    status = str(group.get("status", ""))
    assert status.startswith(("已交付", "部分交付")), (group_id, status)
    assert module.__name__.rsplit(".", 1)[-1] in status, status


@pytest.mark.parametrize("module", [item[0] for item in DELIVERED_JOBS], ids=lambda m: m.__name__)
def test_consumer_group_is_not_hardcoded_env_dependent(module: Any) -> None:
    """组名是契约常量而非环境变量：部署侧改 env 不得能悄悄把两个作业并到同一组。"""
    assert isinstance(module.CONSUMER_GROUP, str)
    assert GROUP_ID_PATTERN.match(module.CONSUMER_GROUP), module.CONSUMER_GROUP
    assert module.CONSUMER_GROUP.startswith("data-analytics-")


def test_no_group_carries_multiple_jobs(groups: dict[str, dict[str, Any]]) -> None:
    """全契约范围不变式：一个 ``group_id`` 至多承载一个作业（V1.19.10 拆组的固化）。"""
    multi = {
        gid: group["jobs"] for gid, group in groups.items() if len(group.get("jobs") or []) > 1
    }
    assert not multi, f"以下消费组承载了多个作业，位点将互相覆盖：{multi}"


def test_openapi_consumes_agree_with_group_entries(
    groups: dict[str, dict[str, Any]],
    contract: dict[str, Any],
) -> None:
    """openapi ``x-hunter-kafka.consumes`` 的组/topic/jobs 必须与 consumer-groups.yaml 逐条一致。"""
    for entry in contract["x-hunter-kafka"]["consumes"]:
        group = groups[entry["group_id"]]
        assert entry["topic"] in group["subscribes"], (entry["group_id"], entry["topic"])
        if "jobs" in entry and group.get("jobs") is not None:
            assert entry["jobs"] == group["jobs"], entry["group_id"]


def test_delivered_job_groups_are_enabled(
    groups: dict[str, dict[str, Any]],
    contract: dict[str, Any],
) -> None:
    """已交付作业对应的组在 openapi 中必须 ``enabled: true``；占位组必须显式 ``false`` + 理由。"""
    enabled = {entry["group_id"]: entry for entry in contract["x-hunter-kafka"]["consumes"]}
    delivered = {group_id for _module, group_id, _t, _j in DELIVERED_JOBS}
    for group_id, entry in enabled.items():
        status = str(groups[group_id].get("status", ""))
        if not status:
            continue
        if status.startswith(("已交付", "部分交付")):
            assert entry["enabled"] is True, group_id
        else:
            assert group_id not in delivered, f"契约称未交付但代码已实现: {group_id}"
            assert entry["enabled"] is False, group_id
            assert "尚未实现" in str(entry.get("reason", "")), group_id
