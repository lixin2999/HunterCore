"""仿真节点控制面服务（V1.24.0：地址自学习 + 按节点动态直推）。

契约：``contracts/openapi/scene-service.yaml#x-hunter-simulation-flow.node_dispatch``。

职责边界：
- 仓储层（``repositories.sim_nodes``）只做 Redis 原语，``repositories.sim_dispatch`` 只做
  「按 node_id 解析当前基址 + 委派 CARLA 管理 API」；
- 本层负责**身份、幂等与地址登记**：owner 首绑/校验、租约续期、把入站学到的地址覆盖写入登记表。

实机拓扑（需求实质）：HunterSim 只能出向连接平台（WiFi），平台与它同网段可回访其 CARLA
管理端口 ⇒ 平台不写死节点地址，而是在注册/心跳时**从入站请求头自学习**：
- 学习失败（所有来源都不合法）→ 2001「无法判定节点地址来源」，不把无法下发的节点登为在线；
- 节点自述的 ``runtime_hint`` 仅展示，**绝不参与寻址**；
- owner 不一致的写调用 → 1002（防止跨调用方接管节点）。

IP 漂移不需要人工处置：心跳是唯一的自愈点 —— 每次心跳都重写地址，实例只绑 ``node_id``，
后续操控在每次调用瞬间重新解析基址。
"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal, cast

from fastapi import Request
from hunter_common.exceptions import (
    InvalidParameterError,
    PermissionDeniedError,
    ResourceNotFoundError,
)
from hunter_common.logging import get_logger

from app.config import Settings
from app.core.client_ip import LearnedAddress, learn_node_endpoint
from app.repositories.sim_nodes import SimNodeStore, now_epoch
from app.schemas.sim_nodes import (
    ENDPOINT_SOURCES,
    NODE_PROTOCOLS,
    SimNodeData,
    SimNodeHeartbeatRequest,
    SimNodeListData,
    SimNodeRegisterRequest,
)

logger = get_logger("app.services.sim_nodes")

#: 契约响应枚举的字面量类型（与 app.schemas.sim_nodes.SimNodeData 逐字对齐）
NodeProtocol = Literal["http", "https"]
NodeEndpointSource = Literal["real-ip", "forwarded-for", "socket-peer"]

#: 契约枚举集合（与 app.schemas.sim_nodes 同源；脏记录里的越界值一律按未提供处置）
NODE_PROTOCOL_VALUES = frozenset(NODE_PROTOCOLS)
ENDPOINT_SOURCE_VALUES = frozenset(ENDPOINT_SOURCES)


def _to_datetime(epoch: Any) -> datetime | None:
    """Redis 分数/时间戳（epoch 秒）→ UTC datetime；非数值返回 None（不伪造时间）。"""
    if isinstance(epoch, bool) or not isinstance(epoch, (int, float)):
        return None
    return datetime.fromtimestamp(float(epoch), UTC)


def _epoch_of(value: datetime | None) -> float | None:
    return value.timestamp() if value is not None else None


def _as_opt_text(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _as_opt_int(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


class SimNodeService:
    """节点注册 / 心跳（含地址自学习）/ 清单。"""

    def __init__(self, store: SimNodeStore, settings: Settings) -> None:
        self._store = store
        self._settings = settings

    # ---------- 运维视图 ----------

    async def list_nodes(self, *, online_only: bool = True) -> SimNodeListData:
        """节点清单（RBAC scene:read）。

        ``online_only=False`` 时含租约已过但元数据键尚存的节点：现场排障需要先分清
        「节点掉线」与「平台不可达节点」，二者的判据都在 ``endpoint_*`` 与 ``last_seen_at`` 上。
        """
        now = now_epoch()
        records = await self._store.list_nodes(online_only=online_only, now=now)
        items = [self._to_node_data(record) for record in records]
        return SimNodeListData(items=items, total=len(items))

    # ---------- 节点侧调用（owner 绑定与校验 + 地址自学习） ----------

    async def register(
        self,
        node_id: str,
        payload: SimNodeRegisterRequest,
        caller: str,
        http_request: Request,
    ) -> SimNodeData:
        """幂等 upsert：首次注册绑定 owner，能力清单与地址每次覆盖，同时续租。"""
        now = now_epoch()
        existing = await self._store.get_node(node_id)
        if existing is not None:
            self._ensure_owner(existing, caller, node_id)
        address = self._learn_address(http_request, node_id)
        record: dict[str, Any] = {
            "node_id": node_id,
            "owner": str((existing or {}).get("owner") or caller),
            "carla_version": payload.carla_version,
            "maps": list(payload.maps) if payload.maps is not None else None,
            "capacity": payload.capacity,
            "labels": dict(payload.labels) if payload.labels is not None else None,
            "runtime_hint": payload.runtime_hint,
            "registered_at": _epoch_of(
                _to_datetime((existing or {}).get("registered_at"))
            )
            or now,
            "last_heartbeat_at": now,
            "running_instance_ids": (existing or {}).get("running_instance_ids") or [],
            **self._address_fields(existing, address, payload, now),
        }
        await self._store.save_node(node_id, record, now=now)
        logger.info(
            "sim_node_registered",
            node_id=node_id,
            owner=record["owner"],
            capacity=record["capacity"],
            endpoint_ip=record["endpoint_ip"],
            endpoint_source=record["endpoint_source"],
        )
        return self._node_data(node_id, record, now=now)

    async def heartbeat(
        self,
        node_id: str,
        payload: SimNodeHeartbeatRequest | None,
        caller: str,
        http_request: Request,
    ) -> SimNodeData:
        """续租 + **覆盖式刷新来源地址**（设计取舍：不引入后台定时器，心跳即地址刷新唯一触发点）。"""
        now = now_epoch()
        existing = await self._require_node(node_id, caller, now=now)
        address = self._learn_address(http_request, node_id)
        running = (
            list(payload.running_instance_ids)
            if payload is not None and payload.running_instance_ids is not None
            else existing.get("running_instance_ids") or []
        )
        capacity = (
            payload.capacity
            if payload is not None and payload.capacity is not None
            else existing.get("capacity")
        )
        if isinstance(capacity, int) and len(running) > capacity:
            # 仅告警不改写：真实占用只有节点自己知道，平台据此选节点已经是启发式
            logger.warning(
                "sim_node_over_capacity",
                node_id=node_id,
                capacity=capacity,
                running=len(running),
            )
        record: dict[str, Any] = {
            **existing,
            "node_id": node_id,
            "capacity": capacity,
            "running_instance_ids": running,
            "message": (payload.message if payload and payload.message is not None else None)
            or existing.get("message"),
            "last_heartbeat_at": now,
            **self._address_fields(existing, address, payload, now),
        }
        await self._store.save_node(node_id, record, now=now)
        return self._node_data(node_id, record, now=now)

    # ---------- 内部：地址自学习 ----------

    def _learn_address(self, http_request: Request, node_id: str) -> LearnedAddress:
        """自学习失败一律 2001（不把无法下发的节点登记为在线，避免下发时才暴露）。"""
        address = learn_node_endpoint(http_request, self._settings)
        if address is None:
            logger.warning("sim_node_address_unresolved", node_id=node_id)
            raise InvalidParameterError(
                "无法判定节点地址来源（X-Real-IP / X-Forwarded-For / TCP 对端均不可用或不合法）",
                details={"node_id": node_id},
            )
        return address

    def _address_fields(
        self,
        existing: dict[str, Any] | None,
        address: LearnedAddress,
        payload: SimNodeRegisterRequest | SimNodeHeartbeatRequest | None,
        now: float,
    ) -> dict[str, Any]:
        """把学到的地址 + 声明的端口/协议组装成登记字段（心跳缺省沿用已登记值）。

        ⚠ 覆盖式写入是「最后一次入站看到的地址 = 当前下发目标」的实现落点；端口/协议只在
        注册体里声明，心跳未带注册体时沿用上一次声明值（首次心跳前取配置默认）。
        """
        # 端口优先级：本次声明 → 已登记值 → SIM_NODE_MANAGER_DEFAULT_PORT（缺省必须可下发，
        # 否则登记出的地址拼不出基址，node_endpoint 会按「离线」处置）
        manager_port = (
            _as_opt_int(getattr(payload, "manager_port", None))
            or _as_opt_int((existing or {}).get("manager_port"))
            or self._settings.sim_node_manager_default_port
        )
        protocol = (
            str(getattr(payload, "protocol", None) or "").strip().lower()
            or str((existing or {}).get("protocol") or "").strip().lower()
            or self._settings.sim_node_manager_scheme
        )
        previous_ip = str((existing or {}).get("endpoint_ip") or "")
        previous_port = _as_opt_int((existing or {}).get("manager_port"))
        if previous_ip and (
            previous_ip != address.ip or (previous_port is not None and previous_port != manager_port)
        ):
            # IP 漂移自愈点的现场留痕：排障时用它对齐「节点换址时刻」与「下发成功时刻」
            logger.info(
                "sim_node_endpoint_learned",
                node_id=str((existing or {}).get("node_id") or ""),
                old_ip=previous_ip,
                old_port=previous_port,
                ip=address.ip,
                manager_port=manager_port,
                source=address.source,
            )
        return {
            "endpoint_ip": address.ip,
            "manager_port": manager_port,
            "protocol": protocol,
            "endpoint_source": address.source,
            "learned_at": now,
        }

    # ---------- 内部：身份与视图 ----------

    async def _require_node(self, node_id: str, caller: str, *, now: float) -> dict[str, Any]:
        """节点必须已注册（3001）且 owner 一致（1002）——未注册即心跳属于配置错误。"""
        record = await self._store.get_node(node_id)
        if record is None:
            raise ResourceNotFoundError(
                f"仿真节点未注册或租约已过期：{node_id}",
                details={
                    "node_id": node_id,
                    "hint": "先调用 PUT /api/v1/scene/sim-nodes/{node_id} 注册",
                },
            )
        self._ensure_owner(record, caller, node_id)
        return record

    @staticmethod
    def _ensure_owner(record: dict[str, Any], caller: str, node_id: str) -> None:
        owner = str(record.get("owner") or "")
        if owner and owner != caller:
            raise PermissionDeniedError(
                message="无权限：节点归属其它调用方",
                details={"node_id": node_id, "owner": owner},
            )

    def _node_data(self, node_id: str, record: dict[str, Any], *, now: float) -> SimNodeData:
        """注册/心跳响应视图：刚写入的登记表就是最新的在线状态与地址。"""
        running = record.get("running_instance_ids")
        return self._to_node_data(
            {
                **record,
                "node_id": node_id,
                "online": True,
                "last_seen_at": now,
                "lease_seconds": self._store.lease_seconds,
                "running_count": len(running) if isinstance(running, list) else 0,
            }
        )

    def _to_node_data(self, record: dict[str, Any]) -> SimNodeData:
        capacity = record.get("capacity")
        maps = record.get("maps")
        return SimNodeData(
            node_id=str(record.get("node_id") or ""),
            owner=str(record.get("owner") or ""),
            online=bool(record.get("online")),
            last_seen_at=_to_datetime(record.get("last_seen_at")),
            lease_seconds=int(record.get("lease_seconds") or self._store.lease_seconds),
            carla_version=_as_opt_text(record.get("carla_version")),
            maps=[str(item) for item in maps] if isinstance(maps, list) else None,
            capacity=capacity if isinstance(capacity, int) else self._store.default_capacity,
            running_count=int(record.get("running_count") or 0),
            labels=(
                {str(k): str(v) for k, v in record["labels"].items()}
                if isinstance(record.get("labels"), dict)
                else None
            ),
            endpoint_ip=_as_opt_text(record.get("endpoint_ip")),
            manager_port=_as_opt_int(record.get("manager_port")),
            protocol=_as_protocol(record.get("protocol")),
            endpoint_source=_as_endpoint_source(record.get("endpoint_source")),
            learned_at=_to_datetime(record.get("learned_at")),
            runtime_hint=_as_opt_text(record.get("runtime_hint")),
            registered_at=_to_datetime(record.get("registered_at")),
            message=_as_opt_text(record.get("message")),
        )


def _as_protocol(value: Any) -> NodeProtocol | None:
    text = str(value or "").strip().lower()
    return cast("NodeProtocol", text) if text in NODE_PROTOCOL_VALUES else None


def _as_endpoint_source(value: Any) -> NodeEndpointSource | None:
    text = str(value or "").strip().lower()
    return cast("NodeEndpointSource", text) if text in ENDPOINT_SOURCE_VALUES else None


__all__ = ["SimNodeService"]
