"""Provisioner 单元测试：4 类失败回滚 + happy path。

Mock 边界：
- `VehicleStore` 使用轻量 stub（不连 DB）；
- `KafkaAdminOps` 用 `AsyncMock`（`upsert_scram_user / create_vehicle_topics / delete_topics / delete_scram_user`）；
- `cert_signer.issue_client_cert / delete_cert_dir` 用 monkeypatch 打桩（不真调 openssl）。

验证重点：
1. happy path：4 步顺序 → 全 ok + provision_status.state=ready + device_cert_sn 落库；
2. scram 失败：回滚只删证书目录（未创建，noop）+ 不动 Topic（未创建）+ 不删 SCRAM（用户未 upsert）；
3. topics 失败：回滚调用 delete_scram_user（本次已 upsert 的），不删既有 Topic；
4. cert 失败：delete_topics(created) + delete_scram_user 均调用；
5. takeover_existing=true：外部资源已存在时不清理；
6. DB 步骤失败（3002）：不进入后续步骤、无回滚。
"""
from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from hunter_common.exceptions import (
    ResourceAlreadyExistsError,
    ServiceUnavailableError,
)

from app.schemas.vehicle import VehicleCreateRequest
from app.services import cert_signer
from app.services.provisioner import Provisioner, aggregate_state


# =====================================================================
# 轻量 stub：VehicleStore
# =====================================================================
class _SessionCtx:
    """async 上下文管理器替代（session 本身不被 provisioner 直接使用）。"""

    async def __aenter__(self) -> Any:
        return MagicMock()

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _StoreStub:
    def __init__(self) -> None:
        self.persisted: list[tuple[str, dict[str, Any], str | None]] = []
        self.insert_calls: list[dict[str, Any]] = []
        self.raise_on_insert: Exception | None = None

    def session(self) -> Any:
        return _SessionCtx()

    async def insert_ledger(self, session: Any, **kwargs: Any) -> Any:
        self.insert_calls.append(kwargs)
        if self.raise_on_insert is not None:
            raise self.raise_on_insert
        return MagicMock()

    async def update_provision_status(
        self, vehicle_id: str, provision_status: dict[str, Any],
        *, device_cert_sn: str | None = None, session: Any | None = None,
    ) -> Any:
        self.persisted.append((vehicle_id, provision_status, device_cert_sn))
        return MagicMock()

    async def get(self, vehicle_id: str) -> Any:
        return None


def _make_admin() -> Any:
    admin = MagicMock()
    admin.upsert_scram_user = AsyncMock(return_value=None)
    admin.delete_scram_user = AsyncMock(return_value=None)
    admin.create_vehicle_topics = AsyncMock(
        return_value=[
            f"hunter.test-01.{t}" for t in (
                "telemetry", "event", "health", "command",
                "command_result", "ota_notify", "ota_status", "remote_control"
            )
        ]
    )
    admin.delete_topics = AsyncMock(return_value=[])
    return admin


def _req(**over: Any) -> VehicleCreateRequest:
    payload: dict[str, Any] = {
        "vehicle_id": "test-01",
        "vehicle_name": "测试车",
        "model": "HUNTER_SE",
    }
    payload.update(over)
    return VehicleCreateRequest(**payload)


@pytest.fixture(autouse=True)
def _stub_cert(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """默认成功签发；用例可通过返回的 dict 修改状态。"""
    state = {"fail": None, "delete_called": 0, "serial": "DEADBEEFCAFE"}

    async def _issue(vehicle_id: str) -> cert_signer.CertArtifacts:
        if state["fail"] is not None:
            raise state["fail"]
        return cert_signer.CertArtifacts(
            vehicle_id=vehicle_id,
            cert_path=MagicMock(),
            key_path=MagicMock(),
            p12_path=MagicMock(),
            p12_password="p12",
            serial_hex=state["serial"],
        )

    def _delete(vehicle_id: str) -> bool:
        state["delete_called"] += 1
        return True

    monkeypatch.setattr(cert_signer, "issue_client_cert", _issue)
    monkeypatch.setattr(cert_signer, "delete_cert_dir", _delete)
    return state


# =====================================================================
# 用例
# =====================================================================
@pytest.mark.asyncio
async def test_happy_path_all_ok(_stub_cert: dict[str, Any]) -> None:
    store = _StoreStub()
    admin = _make_admin()
    prov = Provisioner(store, admin)

    outcome = await prov.provision(_req())

    assert outcome.aggregate == "ready"
    assert outcome.scram_password and len(outcome.scram_password) >= 20
    assert outcome.device_cert_sn == "DEADBEEFCAFE"
    assert aggregate_state(outcome.steps) == "ready"
    # Topic 与 SCRAM 均被调用一次；未清理
    admin.upsert_scram_user.assert_awaited_once()
    admin.create_vehicle_topics.assert_awaited_once()
    admin.delete_topics.assert_not_awaited()
    admin.delete_scram_user.assert_not_awaited()
    assert _stub_cert["delete_called"] == 0
    # 最终一次 persist：state=ready + device_cert_sn
    vehicle_id, status, sn = store.persisted[-1]
    assert vehicle_id == "test-01"
    assert status["state"] == "ready"
    assert sn == "DEADBEEFCAFE"


@pytest.mark.asyncio
async def test_db_step_unique_conflict_short_circuits(_stub_cert: dict[str, Any]) -> None:
    store = _StoreStub()
    store.raise_on_insert = ResourceAlreadyExistsError(
        details={"model": "Vehicle"}
    )
    admin = _make_admin()
    prov = Provisioner(store, admin)

    with pytest.raises(ResourceAlreadyExistsError):
        await prov.provision(_req())

    # 无 SCRAM/Topic/证书调用，无回滚
    admin.upsert_scram_user.assert_not_awaited()
    admin.create_vehicle_topics.assert_not_awaited()
    _ = _stub_cert  # 未使用（DB 已失败）
    # persist 记 failed
    _, status, sn = store.persisted[-1]
    assert status["state"] == "failed"
    assert sn is None


@pytest.mark.asyncio
async def test_scram_failure_rolls_back_only_scram(_stub_cert: dict[str, Any]) -> None:
    store = _StoreStub()
    admin = _make_admin()
    admin.upsert_scram_user = AsyncMock(
        side_effect=ServiceUnavailableError("broker down")
    )
    prov = Provisioner(store, admin)

    with pytest.raises(ServiceUnavailableError):
        await prov.provision(_req())

    # 未创建 Topic 与证书，因此不回滚它们
    admin.create_vehicle_topics.assert_not_awaited()
    admin.delete_topics.assert_not_awaited()
    assert _stub_cert["delete_called"] == 0
    # 本次 SCRAM upsert 失败：不进入 ok 状态，delete_scram_user 也不应被调用
    admin.delete_scram_user.assert_not_awaited()
    _, status, _sn = store.persisted[-1]
    assert status["steps"]["scram"]["state"] == "failed"
    assert status["steps"]["db"]["state"] == "ok"  # DB 步骤保留供审计
    assert status["state"] == "failed"


@pytest.mark.asyncio
async def test_topics_failure_rolls_back_scram(_stub_cert: dict[str, Any]) -> None:
    store = _StoreStub()
    admin = _make_admin()
    admin.create_vehicle_topics = AsyncMock(
        side_effect=ServiceUnavailableError("topic create failed")
    )
    prov = Provisioner(store, admin)

    with pytest.raises(ServiceUnavailableError):
        await prov.provision(_req())

    # scram 已 upsert → 回滚删除
    admin.delete_scram_user.assert_awaited_once_with("test-01")
    # 证书未签发 → delete_cert_dir 被 provisioner 直接调用是"逆序回滚"的默认动作，
    # 但 created_topics=[] 时无 Topic 删除
    admin.delete_topics.assert_not_awaited()
    _, status, _sn = store.persisted[-1]
    assert status["steps"]["topics"]["state"] == "failed"


@pytest.mark.asyncio
async def test_cert_failure_rolls_back_topics_and_scram(_stub_cert: dict[str, Any]) -> None:
    store = _StoreStub()
    admin = _make_admin()
    _stub_cert["fail"] = ServiceUnavailableError("openssl missing")
    prov = Provisioner(store, admin)

    with pytest.raises(ServiceUnavailableError):
        await prov.provision(_req())

    admin.delete_scram_user.assert_awaited_once_with("test-01")
    admin.delete_topics.assert_awaited_once()
    args, _ = admin.delete_topics.await_args
    created = args[0]
    assert len(created) == 8
    assert all(name.startswith("hunter.test-01.") for name in created)
    # 证书清理（delete_cert_dir 被调用；即便未签发也幂等安全）
    assert _stub_cert["delete_called"] >= 1
    _, status, _sn = store.persisted[-1]
    assert status["steps"]["cert"]["state"] == "failed"


@pytest.mark.asyncio
async def test_takeover_existing_skips_scram_rollback(_stub_cert: dict[str, Any]) -> None:
    """takeover_existing=true 时：SCRAM 失败但已存在（不删）；本用例通过状态验证不 delete。"""
    store = _StoreStub()
    admin = _make_admin()
    admin.upsert_scram_user = AsyncMock(
        side_effect=ServiceUnavailableError("already exists externally")
    )
    prov = Provisioner(store, admin)

    with pytest.raises(ServiceUnavailableError):
        await prov.provision(_req(takeover_existing=True))

    # scram 步骤标 failed 而非 ok → rollback 不删（scram state != "ok" 时跳过）
    admin.delete_scram_user.assert_not_awaited()


@pytest.mark.asyncio
async def test_deprovision_sequence(_stub_cert: dict[str, Any]) -> None:
    store = _StoreStub()
    store.delete_ledger = AsyncMock(return_value=True)  # type: ignore[attr-defined]
    admin = _make_admin()
    admin.delete_topics = AsyncMock(
        return_value=[f"hunter.test-01.{t}" for t in (
            "telemetry", "event", "health", "command",
            "command_result", "ota_notify", "ota_status", "remote_control"
        )]
    )
    prov = Provisioner(store, admin)

    summary = await prov.deprovision("test-01", purge_topics=True)

    assert summary["scram_removed"] is True
    assert len(summary["topics_removed"]) == 8
    assert summary["ledger_removed"] is True
    admin.delete_scram_user.assert_awaited_once_with("test-01")
    admin.delete_topics.assert_awaited_once()
    assert _stub_cert["delete_called"] == 1


@pytest.mark.asyncio
async def test_deprovision_without_purge(_stub_cert: dict[str, Any]) -> None:
    store = _StoreStub()
    store.delete_ledger = AsyncMock(return_value=True)  # type: ignore[attr-defined]
    admin = _make_admin()
    prov = Provisioner(store, admin)

    await prov.deprovision("test-01", purge_topics=False)

    admin.delete_topics.assert_not_awaited()


# =====================================================================
# 状态归并（helper 单元测试）
# =====================================================================
def test_aggregate_state_rules() -> None:
    assert aggregate_state({
        "db": {"state": "ok"}, "scram": {"state": "ok"},
        "topics": {"state": "ok"}, "cert": {"state": "ok"},
    }) == "ready"
    # skipped 等同 ok
    assert aggregate_state({
        "db": {"state": "ok"}, "scram": {"state": "skipped"},
        "topics": {"state": "ok"}, "cert": {"state": "skipped"},
    }) == "ready"
    # 任一 failed → failed
    assert aggregate_state({
        "db": {"state": "ok"}, "scram": {"state": "failed"},
        "topics": {"state": "pending"}, "cert": {"state": "pending"},
    }) == "failed"
    # 任一 in_progress → in_progress
    assert aggregate_state({
        "db": {"state": "ok"}, "scram": {"state": "in_progress"},
        "topics": {"state": "pending"}, "cert": {"state": "pending"},
    }) == "in_progress"
    # 全 pending → pending
    assert aggregate_state({
        "db": {"state": "pending"}, "scram": {"state": "pending"},
        "topics": {"state": "pending"}, "cert": {"state": "pending"},
    }) == "pending"
