"""一键开通编排单测（V1.24.2）：四步顺序 / 逆序回滚 / takeover / retry / rotate / deprovision。

外部依赖全部 fake（无 live Kafka/CA/DB）：
- registry：AsyncMock 版 SimNodeRegistryStore（行以 SimpleNamespace 快照模拟）；
- admin：AsyncMock 版 SimKafkaAdminOps（SCRAM + 节点级 Topic）；
- cert 模块：AsyncMock 版 aissue_*，MagicMock 版 delete_node_cert_dir。

实机端到端（真实 broker SCRAM/Topic 创建、openssl 签发）见 docs/仿真端接入平台系统方案.md 验收清单，
本地无 Kafka/CA 环境不在单测覆盖内。
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from hunter_common.exceptions import ResourceAlreadyExistsError, ResourceNotFoundError

from app.services.sim_provisioner import (
    SimProvisioner,
    aggregate_from_status,
    aggregate_state,
    initial_provision_status,
    steps_to_list,
)

NODE = "sim-node-01"
USERNAME = f"sim-{NODE}"


def _settings(**over: object) -> SimpleNamespace:
    base = {
        "sim_provision_enabled": True,
        "sim_scram_enabled": True,
        "sim_node_scram_prefix": "sim-",
        "sim_scram_iterations": 8192,
        "sim_node_cert_validity_days": 3650,
    }
    base.update(over)
    return SimpleNamespace(**base)


def _registry_row(provision_status: dict | None = None, **over: object) -> SimpleNamespace:
    row = SimpleNamespace(
        node_id=NODE,
        display_name=None,
        description=None,
        labels={},
        provision_status=provision_status,
        scram_username=None,
        scram_credential_updated_at=None,
        cert_not_after=None,
        kafka_cert_sn=None,
        plat_cert_sn=None,
    )
    for k, v in over.items():
        setattr(row, k, v)
    return row


class _Fixtures:
    """registry + admin + cert 三替身（记录调用序，支持按步注入失败）。"""

    def __init__(self, *, row_exists: bool = False, row: SimpleNamespace | None = None) -> None:
        self.registry = AsyncMock()
        self.registry.exists = AsyncMock(return_value=row_exists)
        # row_exists=True → get 返回该行（未显式给则默认空行）；False → get 返回 None
        current = row if row is not None else _registry_row()
        self.registry.get = AsyncMock(return_value=current if row_exists else None)
        self.admin = AsyncMock()
        self.admin.ensure_node_topics = AsyncMock(
            return_value=[f"simulation.{NODE}.result", f"simulation.{NODE}.result.dlq"]
        )
        self.admin.delete_node_topics = AsyncMock(return_value=[])
        self.admin.render_node_topics = MagicMock(
            return_value=[
                {"name": f"simulation.{NODE}.result"},
                {"name": f"simulation.{NODE}.result.dlq"},
            ]
        )
        self.certs = MagicMock()
        self.certs.aissue_kafka_cert = AsyncMock(
            return_value=SimpleNamespace(serial_hex="AA11", p12_password="pwd")
        )
        self.certs.aissue_platform_cert = AsyncMock(
            return_value=SimpleNamespace(serial_hex="BB22")
        )
        self.certs.delete_node_cert_dir = MagicMock(return_value=True)

    def provisioner(self, **setting_over: object) -> SimProvisioner:
        return SimProvisioner(
            self.registry, self.admin, _settings(**setting_over), cert_module_obj=self.certs
        )


# =====================================================================
# 成功路径
# =====================================================================


@pytest.mark.asyncio
async def test_provision_all_steps_ok_and_persisted() -> None:
    fx = _Fixtures()
    outcome = await fx.provisioner().provision(NODE, display_name="节点一")

    assert outcome.aggregate == "ready"
    assert aggregate_state(outcome.steps) == "ready"
    assert outcome.scram_username == USERNAME
    assert outcome.scram_password and len(outcome.scram_password) >= 20
    assert outcome.kafka_cert_sn == "AA11"
    assert outcome.plat_cert_sn == "BB22"
    # 四步外部资源各调用一次
    fx.admin.upsert_scram_user.assert_awaited_once_with(USERNAME, outcome.scram_password)
    fx.admin.ensure_node_topics.assert_awaited_once_with(NODE, skip_existing=False)
    fx.certs.aissue_kafka_cert.assert_awaited_once_with(NODE)
    fx.certs.aissue_platform_cert.assert_awaited_once_with(NODE)
    # 成功终态回写：ready + 快照列
    persist = fx.registry.update_provision_status.await_args
    assert persist.args[0] == NODE
    assert persist.args[1]["state"] == "ready"
    assert persist.kwargs["scram_username"] == USERNAME
    assert persist.kwargs["kafka_cert_sn"] == "AA11"
    assert persist.kwargs["cert_not_after"] is not None
    # 口令不回写登记（scram_password 不在任何 registry 调用参数里）
    for call in fx.registry.mock_calls:
        assert outcome.scram_password not in str(call)


@pytest.mark.asyncio
async def test_provision_disabled_returns_4001_style_error() -> None:
    from hunter_common.exceptions import InvalidParameterError

    fx = _Fixtures()
    with pytest.raises(InvalidParameterError):
        await fx.provisioner(sim_provision_enabled=False).provision(NODE)


# =====================================================================
# db 步冲突
# =====================================================================


@pytest.mark.asyncio
async def test_provision_existing_row_rejected_without_takeover() -> None:
    fx = _Fixtures(row_exists=True)
    with pytest.raises(ResourceAlreadyExistsError):
        await fx.provisioner().provision(NODE)
    # 未创建外部资源，也未覆写既有行的 provision_status
    fx.admin.upsert_scram_user.assert_not_awaited()
    fx.registry.update_provision_status.assert_not_awaited()
    fx.registry.upsert.assert_not_awaited()


# =====================================================================
# 失败 + 逆序回滚
# =====================================================================


@pytest.mark.asyncio
async def test_provision_cert_failure_rolls_back_topics_and_scram() -> None:
    fx = _Fixtures()
    fx.certs.aissue_kafka_cert = AsyncMock(side_effect=RuntimeError("openssl boom"))

    from hunter_common.exceptions import ServiceUnavailableError

    with pytest.raises(ServiceUnavailableError):
        await fx.provisioner().provision(NODE)

    # 逆序：本次新建 Topic → SCRAM；cert 未签发成功（openssl 失败）→ 不回收证书目录
    fx.certs.delete_node_cert_dir.assert_not_called()
    fx.admin.delete_node_topics.assert_awaited_once_with(
        [f"simulation.{NODE}.result", f"simulation.{NODE}.result.dlq"]
    )
    fx.admin.delete_scram_user.assert_awaited_once_with(USERNAME)
    # DB 行保留（供 retry），落失败态且 steps 保留 created 清单
    persist = fx.registry.update_provision_status.await_args
    assert persist.args[1]["state"] == "failed"
    assert persist.args[1]["steps"]["cert"]["state"] == "failed"
    assert persist.args[1]["steps"]["topics"]["created"]


@pytest.mark.asyncio
async def test_provision_scram_failure_rolls_back_no_topics() -> None:
    fx = _Fixtures()
    fx.admin.upsert_scram_user = AsyncMock(side_effect=RuntimeError("broker down"))

    from hunter_common.exceptions import ServiceUnavailableError

    with pytest.raises(ServiceUnavailableError):
        await fx.provisioner().provision(NODE)
    fx.admin.ensure_node_topics.assert_not_awaited()
    fx.admin.delete_scram_user.assert_not_awaited()  # scram 未成功 → 无可回收
    assert fx.registry.update_provision_status.await_args.args[1]["state"] == "failed"


@pytest.mark.asyncio
async def test_rollback_failures_do_not_mask_original_error() -> None:
    fx = _Fixtures()
    fx.certs.aissue_kafka_cert = AsyncMock(side_effect=RuntimeError("openssl boom"))
    fx.admin.delete_node_topics = AsyncMock(side_effect=RuntimeError("delete boom"))
    fx.admin.delete_scram_user = AsyncMock(side_effect=RuntimeError("delete boom"))

    from hunter_common.exceptions import ServiceUnavailableError

    with pytest.raises(ServiceUnavailableError, match="证书签发失败"):
        await fx.provisioner().provision(NODE)


# =====================================================================
# takeover / SCRAM 关闭 → skipped
# =====================================================================


@pytest.mark.asyncio
async def test_provision_takeover_skips_existing_topics_and_certs() -> None:
    row = _registry_row(kafka_cert_sn="OLD1", plat_cert_sn="OLD2")
    fx = _Fixtures(row_exists=True, row=row)
    fx.admin.ensure_node_topics = AsyncMock(return_value=[])  # 本次零新建

    outcome = await fx.provisioner().provision(NODE, takeover_existing=True)

    assert outcome.aggregate == "ready"
    assert outcome.steps["topics"]["state"] == "skipped"
    assert outcome.steps["cert"]["state"] == "skipped"
    assert outcome.kafka_cert_sn == "OLD1"  # 沿用登记既有值
    # 回滚/清理不动 skipped 资源
    fx.certs.aissue_kafka_cert.assert_not_awaited()


@pytest.mark.asyncio
async def test_provision_scram_disabled_marks_skipped_no_password() -> None:
    fx = _Fixtures()
    outcome = await fx.provisioner(sim_scram_enabled=False).provision(NODE)
    assert outcome.steps["scram"]["state"] == "skipped"
    assert outcome.scram_password is None
    fx.admin.upsert_scram_user.assert_not_awaited()
    assert outcome.aggregate == "ready"  # skipped 计入 ready


# =====================================================================
# retry：回收残留 → 重跑
# =====================================================================


@pytest.mark.asyncio
async def test_retry_recycles_leftovers_then_reprovisions() -> None:
    failed = {
        "state": "failed",
        "steps": {
            "db": {"name": "db", "state": "ok"},
            "scram": {"name": "scram", "state": "ok"},
            "topics": {
                "name": "topics",
                "state": "ok",
                "created": [f"simulation.{NODE}.result", f"simulation.{NODE}.result.dlq"],
            },
            "cert": {"name": "cert", "state": "failed", "cert_issued": True},
        },
    }
    row = _registry_row(failed, display_name="节点一", labels={"gpu": "A100"})
    fx = _Fixtures(row_exists=True, row=row)

    outcome = await fx.provisioner().retry_provision(NODE)

    # 回收：证书目录 + 本次真建 Topic + SCRAM 用户
    fx.certs.delete_node_cert_dir.assert_called_once_with(NODE)
    fx.admin.delete_node_topics.assert_awaited_once_with(
        [f"simulation.{NODE}.result", f"simulation.{NODE}.result.dlq"]
    )
    fx.admin.delete_scram_user.assert_awaited_once_with(USERNAME)
    # 重跑成功且沿用登记元信息
    assert outcome.aggregate == "ready"
    upsert_kwargs = fx.registry.upsert.await_args.kwargs
    assert upsert_kwargs["display_name"] == "节点一"
    assert upsert_kwargs["labels"] == {"gpu": "A100"}


@pytest.mark.asyncio
async def test_retry_unknown_node_404() -> None:
    fx = _Fixtures(row_exists=False)
    with pytest.raises(ResourceNotFoundError):
        await fx.provisioner().retry_provision(NODE)


# =====================================================================
# rotate-scram
# =====================================================================


@pytest.mark.asyncio
async def test_rotate_scram_upserts_and_persists_timestamp() -> None:
    row = _registry_row(initial_provision_status(), scram_username=USERNAME)
    fx = _Fixtures(row_exists=True, row=row)

    password, updated_at = await fx.provisioner().rotate_scram(NODE)

    fx.admin.upsert_scram_user.assert_awaited_once_with(USERNAME, password)
    persist = fx.registry.update_provision_status.await_args
    assert persist.args[1] is row.provision_status  # 编排态保持原快照
    assert persist.kwargs["scram_credential_updated_at"] == updated_at


@pytest.mark.asyncio
async def test_rotate_scram_requires_existing_row() -> None:
    fx = _Fixtures(row_exists=False)
    with pytest.raises(ResourceNotFoundError):
        await fx.provisioner().rotate_scram(NODE)


# =====================================================================
# deprovision：证书目录→SCRAM→Topic→登记行→Redis
# =====================================================================


@pytest.mark.asyncio
async def test_deprovision_full_chain_summary() -> None:
    row = _registry_row(initial_provision_status(), scram_username=USERNAME)
    fx = _Fixtures(row_exists=True, row=row)
    fx.admin.delete_node_topics = AsyncMock(
        return_value=[f"simulation.{NODE}.result", f"simulation.{NODE}.result.dlq"]
    )
    fx.registry.delete = AsyncMock(return_value=True)
    redis_deleter = AsyncMock(return_value=True)

    summary = await fx.provisioner().deprovision(NODE, redis_deleter=redis_deleter)

    assert summary == {
        "node_id": NODE,
        "cert_dir_removed": True,
        "scram_removed": True,
        "topics_removed": [f"simulation.{NODE}.result", f"simulation.{NODE}.result.dlq"],
        "registry_removed": True,
        "redis_removed": True,
    }
    fx.certs.delete_node_cert_dir.assert_called_once_with(NODE)
    fx.admin.delete_scram_user.assert_awaited_once_with(USERNAME)
    redis_deleter.assert_awaited_once_with(NODE)


@pytest.mark.asyncio
async def test_deprovision_missing_row_falls_back_to_derived_username() -> None:
    fx = _Fixtures(row_exists=False)
    fx.registry.delete = AsyncMock(return_value=False)

    summary = await fx.provisioner().deprovision(NODE)

    # 行已不存在仍按前缀推导主体回收（幂等）；redis_deleter 未注入 → redis_removed False
    fx.admin.delete_scram_user.assert_awaited_once_with(USERNAME)
    assert summary["registry_removed"] is False
    assert summary["redis_removed"] is False


# =====================================================================
# 辅助函数
# =====================================================================


def test_steps_to_list_and_aggregate_from_status() -> None:
    assert [s.name for s in steps_to_list(None)] == ["db", "scram", "topics", "cert"]
    assert all(s.state == "pending" for s in steps_to_list(None))
    # 无顶层 state 时按 steps 归并（provisioner 失败落库会写 state，此处测重算路径）
    recomputed = {"steps": {"scram": {"state": "failed"}, "db": {"state": "ok"}}}
    assert aggregate_from_status(recomputed) == "failed"
    assert aggregate_from_status({"state": "ready", "steps": {}}) == "ready"
    assert aggregate_from_status(None) == "pending"
    # 辅助键（created/cert_issued）不进入响应模型
    status = initial_provision_status()
    status["steps"]["topics"]["created"] = ["a"]
    steps = steps_to_list(status)
    assert not hasattr(steps[2], "created")
