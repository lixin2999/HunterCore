"""仿真节点登记表与实例归属仓储（V1.24.0）。

契约：``contracts/openapi/scene-service.yaml#x-hunter-simulation-flow.node_dispatch``
与 ``contracts/database/redis-keys.yaml`` 第 18-21 号键（键名一律经
``hunter_common.redis_keys``，禁止字面量拼接）。

链路（实机拓扑）：HunterSim 走 WiFi 出向连接平台做注册/心跳，平台与它同网段可回访 ⇒
本仓储把**从入站请求头学到的地址**随注册信息一并登记（每次覆盖），并按 ``node_id`` 解析出
当前可下发的 ``base_url``（:meth:`SimNodeStore.node_endpoint`）。实例只绑 ``node_id``
（``sim:runtime``），地址在每次调用瞬间解析 —— IP 漂移因此自愈。

分层：本模块只做 Redis 原语；业务编排见 ``app/services/sim_nodes.py``；
按节点动态基址的下发适配见 ``app/repositories/sim_dispatch.py``。
"""
from __future__ import annotations

import json
import time
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import Any, TypeVar

from hunter_common.exceptions import ServiceUnavailableError
from hunter_common.logging import get_logger
from hunter_common.redis_keys import (
    SIM_INSTANCE_ACTIVE,
    SIM_NODE_SEEN,
    sim_node_key,
    sim_runtime_key,
)
from redis.exceptions import RedisError

from app.core.client_ip import build_base_url

logger = get_logger("app.repositories.sim_nodes")

_T = TypeVar("_T")

#: 存储不可用时统一上抛的文案前缀（错误码 5001，与 VIL 会话仓储一致）
_STORAGE_MESSAGE = "仿真节点存储不可用"
#: 节点基址允许的协议（契约 endpoint_learning.base_url；登记值非法时回落 http）
_NODE_SCHEMES = frozenset({"http", "https"})


def now_epoch() -> float:
    """统一时钟源（便于测试注入与懒清理判定）。"""
    return time.time()


@dataclass(frozen=True, slots=True)
class NodeEndpoint:
    """节点当前的下发目标（每次调用重解，**不随实例持久化**）。"""

    node_id: str
    base_url: str
    ip: str
    port: int
    scheme: str = "http"
    source: str | None = None


def _as_int(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _dumps(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _loads(raw: Any) -> dict[str, Any] | None:
    """解析 JSON 对象；损坏/非对象返回 ``None``（按不存在处理，不静默当成空对象）。"""
    if raw is None:
        return None
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


class SimNodeStore:
    """节点登记表（含自学习地址）/ 实例归属索引的 Redis 原语（任何命令失败 → 5001）。"""

    def __init__(
        self,
        redis_client: Any,
        *,
        lease_seconds: int = 90,
        instance_ttl_seconds: int = 86400,
        default_capacity: int = 1,
    ) -> None:
        self._redis = redis_client
        self._lease = max(1, lease_seconds)
        self._instance_ttl = max(1, instance_ttl_seconds)
        self._default_capacity = max(1, default_capacity)

    # ---------- 内部：统一 RedisError → 5001 ----------

    async def _await(self, awaitable: Awaitable[_T], *, op: str, **details: Any) -> _T:
        try:
            return await awaitable
        except RedisError as exc:
            logger.warning("sim_node_store_unavailable", op=op, error=type(exc).__name__)
            raise ServiceUnavailableError(_STORAGE_MESSAGE, details=details or None) from exc

    @property
    def lease_seconds(self) -> int:
        """在线租约秒数（回显给节点，用于计算心跳间隔；也是离线判定的窗口）。"""
        return self._lease

    # ---------- 节点登记表（sim:node / sim:node:seen） ----------

    async def get_node(self, node_id: str) -> dict[str, Any] | None:
        return _loads(
            await self._await(
                self._redis.get(sim_node_key(node_id)), op="get_node", node_id=node_id
            )
        )

    async def save_node(self, node_id: str, record: dict[str, Any], *, now: float) -> None:
        """覆盖式登记 + 续租（注册与心跳共用：SET EX lease + ZADD seen）。

        调用方负责先把自学习到的 ``endpoint_ip`` / ``manager_port`` / ``protocol`` /
        ``endpoint_source`` / ``learned_at`` 写进 ``record`` —— 本方法整键覆写，
        因此地址天然是「最后一次入站看到的地址」。
        """
        key = sim_node_key(node_id)
        await self._await(
            self._redis.set(key, _dumps(record), ex=self._lease),
            op="save_node",
            node_id=node_id,
        )
        await self._await(
            self._redis.zadd(SIM_NODE_SEEN, {node_id: now}),
            op="save_node_seen",
            node_id=node_id,
        )

    async def node_online(self, node_id: str, *, now: float) -> bool:
        """单点在线判定（租约内且元数据键仍存活）。"""
        score = await self._await(
            self._redis.zscore(SIM_NODE_SEEN, node_id), op="node_online", node_id=node_id
        )
        if score is None or float(score) <= now - self._lease:
            return False
        return (await self.get_node(node_id)) is not None

    async def list_nodes(self, *, online_only: bool, now: float) -> list[dict[str, Any]]:
        """枚举节点（附在线标记与当前学到的地址）；元数据键已消失的成员顺手 ZREM（懒清理）。"""
        pairs = await self._await(
            self._redis.zrange(SIM_NODE_SEEN, 0, -1, withscores=True), op="list_nodes"
        )
        nodes: list[dict[str, Any]] = []
        for member, score in pairs or ():
            node_id = str(member)
            record = await self.get_node(node_id)
            if record is None:
                await self._await(
                    self._redis.zrem(SIM_NODE_SEEN, node_id),
                    op="list_nodes_cleanup",
                    node_id=node_id,
                )
                continue
            online = float(score) > now - self._lease
            if online_only and not online:
                continue
            running = record.get("running_instance_ids")
            nodes.append(
                {
                    **record,
                    "node_id": node_id,
                    "online": online,
                    "last_seen_at": float(score),
                    "lease_seconds": self._lease,
                    "running_count": len(running) if isinstance(running, list) else 0,
                }
            )
        nodes.sort(key=lambda item: str(item.get("node_id") or ""))
        return nodes

    # ---------- 下发寻址（按 node_id 解析**当前**学到的地址） ----------

    async def node_endpoint(self, node_id: str, *, now: float) -> NodeEndpoint | None:
        """解析节点当前的下发目标；离线（租约过期）或无合法地址 → ``None``（调用方转 5001）。

        每次调用都重读登记表 ⇒ 心跳刷新过地址后本方法立即返回新 ``base_url``：
        这是「实例只绑 node_id、IP 漂移不导致下发失败」的落点（契约 addressing_rule）。
        """
        score = await self._await(
            self._redis.zscore(SIM_NODE_SEEN, node_id), op="node_endpoint", node_id=node_id
        )
        if score is None or float(score) <= now - self._lease:
            return None
        record = await self.get_node(node_id)
        if record is None:
            return None
        ip = str(record.get("endpoint_ip") or "").strip()
        port = _as_int(record.get("manager_port")) or 0
        if not ip or not 1 <= port <= 65535:
            # 存量节点（本次升级前登记）或脏记录：按离线处置，不拼一个不可能成功的基址
            return None
        scheme = str(record.get("protocol") or "http").strip().lower()
        if scheme not in _NODE_SCHEMES:
            scheme = "http"
        return NodeEndpoint(
            node_id=node_id,
            base_url=build_base_url(ip, port, scheme),
            ip=ip,
            port=port,
            scheme=scheme,
            source=str(record.get("endpoint_source") or "") or None,
        )

    # ---------- 运行时快照（sim:runtime）与活动实例索引 ----------

    async def save_runtime(self, sim_instance_id: str, snapshot: dict[str, Any]) -> None:
        """整键覆盖实例→节点归属（与 sim:instance 元数据分键，避免 bind_instance 覆写冲突）。"""
        await self._await(
            self._redis.set(
                sim_runtime_key(sim_instance_id), _dumps(snapshot), ex=self._instance_ttl
            ),
            op="save_runtime",
            sim_instance_id=sim_instance_id,
        )

    async def get_runtime(self, sim_instance_id: str) -> dict[str, Any] | None:
        return _loads(
            await self._await(
                self._redis.get(sim_runtime_key(sim_instance_id)),
                op="get_runtime",
                sim_instance_id=sim_instance_id,
            )
        )

    async def add_active_instance(self, sim_instance_id: str, *, now: float) -> None:
        await self._await(
            self._redis.zadd(SIM_INSTANCE_ACTIVE, {sim_instance_id: now}),
            op="add_active_instance",
            sim_instance_id=sim_instance_id,
        )

    async def remove_active_instance(self, sim_instance_id: str) -> None:
        await self._await(
            self._redis.zrem(SIM_INSTANCE_ACTIVE, sim_instance_id),
            op="remove_active_instance",
            sim_instance_id=sim_instance_id,
        )

    # ---------- 节点下线清理（V1.24.1 仿真端管理） ----------

    async def delete_node(self, node_id: str) -> None:
        """删除节点登记：DEL sim:node:{node_id} + ZREM sim:node:seen（幂等）。"""
        await self._await(
            self._redis.delete(sim_node_key(node_id)),
            op="delete_node",
            node_id=node_id,
        )
        await self._await(
            self._redis.zrem(SIM_NODE_SEEN, node_id),
            op="delete_node_seen",
            node_id=node_id,
        )

    async def active_instance_records(self, *, now: float) -> list[dict[str, Any]]:
        """活动实例的运行时快照清单；快照已消失（回收/TTL）的成员顺手 ZREM。"""
        members = await self._await(
            self._redis.zrange(SIM_INSTANCE_ACTIVE, 0, -1), op="active_instances"
        )
        records: list[dict[str, Any]] = []
        for member in members or ():
            sim_instance_id = str(member)
            snapshot = await self.get_runtime(sim_instance_id)
            if snapshot is None:
                await self._await(
                    self._redis.zrem(SIM_INSTANCE_ACTIVE, sim_instance_id),
                    op="active_instance_cleanup",
                    sim_instance_id=sim_instance_id,
                )
                continue
            records.append({**snapshot, "sim_instance_id": sim_instance_id})
        return records

    # ---------- 节点选取（② 创建实例落到哪个节点） ----------

    async def select_node(
        self, *, now: float, map_id: str | None = None, prefer: str | None = None
    ) -> dict[str, Any] | None:
        """在线节点中选空闲容量最多者（同空闲度按 node_id 升序，保证确定性）。

        - ``prefer``：调用方显式指定节点（必须在线，否则视为不可用）；
        - ``map_id``：节点声明了 ``maps`` 且不含该地图时被排除；未声明 maps 视为通用。
        ⚠ 容量为节点自述值（平台无法得知真实占用，故不做二次削减）：本模式是同步直推，
        超额时由节点上的 CARLA 自行拒绝创建 ⇒ 失败按 5001 原样暴露给调用方。
        """
        nodes = await self.list_nodes(online_only=True, now=now)
        if not nodes:
            return None
        if prefer:
            return next((node for node in nodes if node.get("node_id") == prefer), None)
        candidates = [
            node
            for node in nodes
            if not map_id
            or not _declared_maps(node)
            or map_id in _declared_maps(node)
        ]
        if not candidates:
            return None
        candidates.sort(key=_free_capacity_sort_key)
        return candidates[0]

    @property
    def default_capacity(self) -> int:
        """注册未声明 capacity 时的默认可并发数。"""
        return self._default_capacity


def _declared_maps(node: dict[str, Any]) -> tuple[str, ...]:
    maps = node.get("maps")
    if not isinstance(maps, list):
        return ()
    return tuple(str(item) for item in maps if str(item).strip())


def _free_capacity_sort_key(node: dict[str, Any]) -> tuple[int, str]:
    """排序键：空闲容量降序 → node_id 升序（确定性，避免同容量下飘忽选点）。"""
    capacity = _as_int(node.get("capacity")) or 1
    running = node.get("running_instance_ids")
    busy = len(running) if isinstance(running, list) else 0
    return (-(max(0, capacity - busy)), str(node.get("node_id") or ""))


__all__ = [
    "NodeEndpoint",
    "SimNodeStore",
    "now_epoch",
]
