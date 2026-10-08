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
import yaml
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
# SCRAM 操作（依赖 kafka-python-ng 的 KIP-95 请求类；缺失时明确 5001）
# =====================================================================
@pytest.mark.asyncio
async def test_upsert_scram_unsupported_raises(
    ops: kafka_admin.KafkaAdminOps, monkeypatch: pytest.MonkeyPatch
) -> None:
    """未安装 kafka-python-ng 或版本不支持 KIP-95：明确 5001 而非静默失败。"""
    monkeypatch.setattr(kafka_admin, "AlterUserScramCredentialsRequest", None, raising=False)
    monkeypatch.setattr(kafka_admin, "ScramCredentialUpdate", None, raising=False)
    with pytest.raises(ServiceUnavailableError):
        await ops.upsert_scram_user("v-1", "pw")


@pytest.mark.asyncio
async def test_upsert_scram_success(
    ops: kafka_admin.KafkaAdminOps, monkeypatch: pytest.MonkeyPatch
) -> None:
    """注入假的 Request/Update 类，验证调用参数与 future.result 超时。"""

    class _Update:
        def __init__(self, principal: Any, scram_mechanism: int, iterations: int, password: bytes) -> None:
            self.principal = principal
            self.scram_mechanism = scram_mechanism
            self.iterations = iterations
            self.password = password

    captured: dict[str, Any] = {}

    class _Req:
        def __init__(self, upserts: list[_Update], deletes: list[Any]) -> None:
            captured["upserts"] = upserts
            captured["deletes"] = deletes

    fake_future = MagicMock()
    fake_future.result = MagicMock(return_value=None)
    fake_client = MagicMock()
    fake_client.send_request = MagicMock(return_value=fake_future)
    ops._admin.client = fake_client  # noqa: SLF001

    monkeypatch.setattr(kafka_admin, "AlterUserScramCredentialsRequest", _Req, raising=False)
    monkeypatch.setattr(kafka_admin, "ScramCredentialUpdate", _Update, raising=False)

    await ops.upsert_scram_user("HUNTER-7", "s3cret")

    assert captured["deletes"] == []
    assert len(captured["upserts"]) == 1
    upd = captured["upserts"][0]
    assert upd.principal.principal_name == "HUNTER-7"
    assert upd.principal.principal_type == 2  # KAFKA_PRINCIPAL_TYPE
    assert upd.scram_mechanism == kafka_admin.SCRAM_MECHANISM_ID  # 1 = SCRAM-SHA-512
    assert upd.password == b"s3cret"
    # 超时参数来自 settings
    fake_future.result.assert_called_once_with(timeout_ms=settings.kafka_admin_timeout_ms)


@pytest.mark.asyncio
async def test_delete_scram_user_not_found_idempotent(
    ops: kafka_admin.KafkaAdminOps, monkeypatch: pytest.MonkeyPatch
) -> None:
    """底层 KafkaError 含 not found 语义 → 幂等成功（不抛）。"""

    class _Req:
        def __init__(self, **kwargs: Any) -> None:
            pass

    fake_future = MagicMock()
    from kafka.errors import KafkaError  # noqa: PLC0415
    fake_future.result = MagicMock(
        side_effect=KafkaError("SASL_PRINCIPAL_NOT_FOUND: principal not found")
    )
    fake_client = MagicMock()
    fake_client.send_request = MagicMock(return_value=fake_future)
    ops._admin.client = fake_client  # noqa: SLF001

    monkeypatch.setattr(kafka_admin, "AlterUserScramCredentialsRequest", _Req, raising=False)
    await ops.delete_scram_user("ghost")  # 不应抛


# =====================================================================
# 元数据同步等待（构造 NewTopic 时应含 retention/cleanup）
# =====================================================================
def test_new_topic_config_includes_cleanup(ops: kafka_admin.KafkaAdminOps) -> None:
    asyncio.run(ops.create_vehicle_topics("v-9", skip_existing=True))
    for t in ops._admin.created:  # noqa: SLF001
        assert "cleanup.policy" in t.topic_config
        assert "retention.ms" in t.topic_config
