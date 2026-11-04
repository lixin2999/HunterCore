"""清单并入 DB 登记行（V1.24.2 修复：一键开通后未自注册节点「已开通却不可见」）。

根因：``list_nodes`` 旧实现仅枚举 Redis 实时登记表，DB registry 只用于给 Redis 已有节点补充
管理字段；一键开通只写 DB 行、节点未自注册 ⇒ 不在 Redis ⇒ 列表 0 条，管理员无法进入详情。

修复：``online_only=False`` 时并入 DB 登记行中不在 Redis 的节点（离线态）；``online_only=True``
维持 Redis 在线语义。本文件用 stub store/registry 隔离该逻辑（无需 Redis/DB）。
"""
from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

from app.config import settings
from app.services.sim_nodes import SimNodeService


class _StubStore:
    """Redis 实时登记表替身（本用例下可控返回在线记录）。"""

    lease_seconds = 90
    default_capacity = 1

    def __init__(self, records: list[dict[str, Any]]) -> None:
        self._records = records

    async def list_nodes(self, *, online_only: bool, now: float) -> list[dict[str, Any]]:
        if online_only:
            return [r for r in self._records if r.get("online")]
        return list(self._records)


class _StubRegistry:
    def __init__(self, rows: list[SimpleNamespace]) -> None:
        self._rows = rows

    async def get_map(self, node_ids: list[str]) -> dict[str, SimpleNamespace]:
        return {r.node_id: r for r in self._rows if r.node_id in node_ids}

    async def list_all(self) -> list[SimpleNamespace]:
        return list(self._rows)


def _ready_row(node_id: str = "sim-node-01") -> SimpleNamespace:
    steps = {name: {"name": name, "state": "ok"} for name in ("db", "scram", "topics", "cert")}
    return SimpleNamespace(
        node_id=node_id,
        display_name="Carla仿真01",
        description=None,
        owner_client_id="service:admin",
        token_id=None,
        token_issued_at=None,
        kafka_cert_sn="AABBCC",
        kafka_cert_issued_at=None,
        plat_cert_sn="DDEEFF",
        plat_cert_issued_at=None,
        create_time=datetime(2026, 11, 4, tzinfo=UTC),
        first_seen_at=None,
        labels={},
        provision_status={"state": "ready", "steps": steps},
        scram_username=f"sim-{node_id}",
        scram_credential_updated_at=None,
        cert_not_after=None,
    )


async def test_list_offline_includes_registry_only_node() -> None:
    """Redis 空 + 一条 DB 已开通行 ⇒ online_only=False 应出现该离线行且开通字段完整。"""
    svc = SimNodeService(_StubStore([]), settings, registry_store=_StubRegistry([_ready_row()]))
    data = await svc.list_nodes(online_only=False)
    assert data.total == 1
    item = data.items[0]
    assert item.node_id == "sim-node-01"
    assert item.online is False
    assert item.display_name == "Carla仿真01"
    assert item.scram_username == "sim-sim-node-01"
    assert item.provision_state == "ready"
    assert {s.name for s in item.provision_steps} == {"db", "scram", "topics", "cert"}


async def test_list_online_only_excludes_registry_only_node() -> None:
    """online_only=True 维持 Redis 在线语义：未自注册的 DB-only 行不应出现。"""
    svc = SimNodeService(_StubStore([]), settings, registry_store=_StubRegistry([_ready_row()]))
    data = await svc.list_nodes(online_only=True)
    assert data.total == 0


async def test_list_no_duplicate_when_node_in_both_sources() -> None:
    """同一节点既在 Redis（在线）又在 DB ⇒ 只出一条、在线态、开通字段合并、不重复合成。"""
    redis_record = {"node_id": "sim-node-01", "online": True, "owner": "service:admin"}
    svc = SimNodeService(
        _StubStore([redis_record]), settings, registry_store=_StubRegistry([_ready_row()])
    )
    data = await svc.list_nodes(online_only=False)
    assert data.total == 1
    assert data.items[0].online is True
    assert data.items[0].scram_username == "sim-sim-node-01"
