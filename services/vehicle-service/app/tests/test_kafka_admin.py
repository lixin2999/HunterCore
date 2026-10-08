"""kafka_admin 单元测试：Topic 清单加载 + AdminClient 调用参数。

Mock 边界：不真连 Kafka；直接构造 `KafkaAdminOps` 传入 `MagicMock` 化的 admin。
`load_vehicle_topics` 通过 `KAFKA_CONTRACT_DIR` env 指向临时目录（或仓库真实契约）。
"""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from hunter_common.exceptions import ServiceUnavailableError

from app.config import settings
from app.services import kafka_admin


# =====================================================================
# 契约加载（真实 contracts/kafka/topics.yaml）
# =====================================================================
def test_load_vehicle_topics_returns_8() -> None:
    """从仓库 contracts/kafka/topics.yaml 加载：8 项车辆 Topic（不含广播）。"""
    # 清缓存以隔离
    kafka_admin._VEHICLE_TOPICS_CACHE = None  # type: ignore[attr-defined]
    topics = kafka_admin.load_vehicle_topics()
    assert len(topics) == 8
    names = [t["name"] for t in topics]
    for expected_type in (
        "telemetry", "event", "health", "command",
        "command_result", "ota_notify", "ota_status", "remote_control",
    ):
        assert f"hunter.{{vehicle_id}}.{expected_type}" in names
    # 广播 Topic 不在清单（无 {vehicle_id} 段）
    assert all("{vehicle_id}" in n for n in names)


def test_render_vehicle_topics_expands_placeholder() -> None:
    kafka_admin._VEHICLE_TOPICS_CACHE = None  # type: ignore[attr-defined]
    rendered = kafka_admin.render_vehicle_topics("HUNTER-042")
    assert len(rendered) == 8
    assert all("{vehicle_id}" not in t["name"] for t in rendered)
    assert any(t["name"] == "hunter.HUNTER-042.telemetry" for t in rendered)
    # 元数据保留
    for entry in rendered:
        assert entry["partitions"] >= 1
        assert entry["retention_ms"] > 0


def test_missing_contract_dir_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    kafka_admin._VEHICLE_TOPICS_CACHE = None  # type: ignore[attr-defined]  # noqa: SLF001
    monkeypatch.setattr(settings, "kafka_contract_dir", "/nonexistent/dir")
    # 让向上探测也失败：临时改 __file__ 位置不可行；改为直接构造空 dir
    with pytest.raises(ServiceUnavailableError):
        # 强制走非存在路径 —— 通过 patch _find_contracts_dir
        def _broken() -> Path:
            raise ServiceUnavailableError("no contracts dir")

        monkeypatch.setattr(kafka_admin, "_find_contracts_dir", _broken)
        kafka_admin.load_vehicle_topics()


# =====================================================================
# KafkaAdminOps（Mock admin 断言调用参数）
# =====================================================================
class _FakeAdmin:
    def __init__(self) -> None:
        self.created: list[Any] = []
        self.deleted: list[str] = []
        self.closed = False

    def list_topics(self) -> list[str]:
        return ["hunter.broadcast.command"]

    def create_topics(self, topics: list[Any]) -> None:
        # 幂等：模拟"Topic 已存在"抛错
        for t in topics:
            if t.name == "hunter.dup.telemetry":
                from kafka.errors import TopicAlreadyExistsError  # noqa: PLC0415
                raise TopicAlreadyExistsError(t.name)
            self.created.append(t)

    def delete_topics(self, names: list[str]) -> None:
        self.deleted.extend(names)

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def ops() -> kafka_admin.KafkaAdminOps:
    return kafka_admin.KafkaAdminOps(_FakeAdmin())  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_create_vehicle_topics_skip_existing(ops: kafka_admin.KafkaAdminOps) -> None:
    """契约 8 Topic 全部创建；调用方拿到的 created 应不含外部既有资源。"""
    created = await ops.create_vehicle_topics("v-100", skip_existing=True)
    assert len(created) == 8
    assert all(name.startswith("hunter.v-100.") for name in created)
    # 元数据落 NewTopic（分区/保留期/cleanup.policy）
    underlying = ops._admin.created  # noqa: SLF001
    telemetry = next(t for t in underlying if t.name == "hunter.v-100.telemetry")
    assert telemetry.num_partitions == 6  # 契约：telemetry=6 分区
    assert telemetry.topic_config["retention.ms"] == "604800000"  # 7 天
    event = next(t for t in underlying if t.name == "hunter.v-100.event")
    assert event.topic_config["retention.ms"] == "2592000000"  # 30 天


@pytest.mark.asyncio
async def test_delete_topics_returns_actual(ops: kafka_admin.KafkaAdminOps) -> None:
    deleted = await ops.delete_topics(["hunter.v-1.a", "hunter.v-1.b"])
    assert deleted == ["hunter.v-1.a", "hunter.v-1.b"]
    assert ops._admin.deleted == deleted  # noqa: SLF001


@pytest.mark.asyncio
async def test_delete_topics_empty_short_circuits(ops: kafka_admin.KafkaAdminOps) -> None:
    assert await ops.delete_topics([]) == []


@pytest.mark.asyncio
async def test_alist_topics_roundtrip(ops: kafka_admin.KafkaAdminOps) -> None:
    topics = await ops.alist_topics()
    assert topics == ["hunter.broadcast.command"]


# =====================================================================
# SCRAM 操作（走 confluent-kafka AdminClient；未就绪时明确 5001）
# =====================================================================
class _FakeScramAdmin:
    """模拟 confluent_kafka.admin.AdminClient.alter_user_scram_credentials。"""

    def __init__(self, raise_on_result: Exception | None = None) -> None:
        self.altered: list[Any] = []
        self._raise = raise_on_result

    def alter_user_scram_credentials(self, alterations: list[Any]) -> dict[str, Any]:
        self.altered.extend(alterations)
        futures: dict[str, Any] = {}
        for i in range(len(alterations)):
            future = MagicMock()
            if self._raise is not None:
                future.result = MagicMock(side_effect=self._raise)
            else:
                future.result = MagicMock(return_value=None)
            futures[f"op-{i}"] = future
        return futures


@pytest.mark.asyncio
async def test_upsert_scram_not_ready_raises(ops: kafka_admin.KafkaAdminOps) -> None:
    """SCRAM AdminClient 未就绪（confluent admin=None）：明确 5001 而非静默失败。"""
    assert ops._scram is None  # noqa: SLF001 - 默认 fixture 未注入 scram
    with pytest.raises(ServiceUnavailableError):
        await ops.upsert_scram_user("v-1", "pw")


@pytest.mark.asyncio
async def test_upsert_scram_success(ops: kafka_admin.KafkaAdminOps) -> None:
    """注入假 confluent admin：验证走 alter_user_scram_credentials 且构造 Upsertion。"""
    from confluent_kafka.admin import UserScramCredentialUpsertion  # noqa: PLC0415

    fake = _FakeScramAdmin()
    ops._scram = fake  # noqa: SLF001
    await ops.upsert_scram_user("HUNTER-7", "s3cret")

    assert len(fake.altered) == 1
    assert isinstance(fake.altered[0], UserScramCredentialUpsertion)


@pytest.mark.asyncio
async def test_delete_scram_user_not_found_idempotent(ops: kafka_admin.KafkaAdminOps) -> None:
    """底层 KafkaException 含 not found 语义 → 幂等成功（不抛）。"""
    from confluent_kafka import KafkaException  # noqa: PLC0415

    fake = _FakeScramAdmin(
        raise_on_result=KafkaException("SASL_PRINCIPAL_NOT_FOUND: principal not found")
    )
    ops._scram = fake  # noqa: SLF001
    await ops.delete_scram_user("ghost")  # 不应抛


@pytest.mark.asyncio
async def test_delete_scram_user_error_raises(ops: kafka_admin.KafkaAdminOps) -> None:
    """非 not-found 的 KafkaException → 明确 5001。"""
    from confluent_kafka import KafkaException  # noqa: PLC0415

    fake = _FakeScramAdmin(raise_on_result=KafkaException("some broker error"))
    ops._scram = fake  # noqa: SLF001
    with pytest.raises(ServiceUnavailableError):
        await ops.delete_scram_user("v-1")


# =====================================================================
# 元数据同步等待（构造 NewTopic 时应含 retention/cleanup）
# =====================================================================
def test_new_topic_config_includes_cleanup(ops: kafka_admin.KafkaAdminOps) -> None:
    asyncio.run(ops.create_vehicle_topics("v-9", skip_existing=True))
    for t in ops._admin.created:  # noqa: SLF001
        assert "cleanup.policy" in t.topic_config
        assert "retention.ms" in t.topic_config
