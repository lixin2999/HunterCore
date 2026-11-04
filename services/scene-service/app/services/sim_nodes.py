"""仿真节点控制面服务（V1.24.0：地址自学习 + 按节点动态直推）。

契约：``contracts/openapi/scene-service.yaml#x-hunter-simulation-flow.node_dispatch``。

职责边界：
- 仓储层（``repositories.sim_nodes``）只做 Redis 原语，``repositories.sim_dispatch`` 只做
  「按 node_id 解析当前基址 + 委派 CARLA 管理 API」；
- 本层负责**身份、幂等与地址登记**：owner 首绑/校验、租约续期、把入站学到的地址覆盖写入登记表。

V1.24.1 仿真端管理扩展：
- ``get_detail``：合并 Redis 实时态 + PostgreSQL 登记表，管理员详情视图；
- ``patch_registry``：更新展示名/描述/标签；
- ``issue_token`` / ``revoke_token``：代理 api-gateway 签发/撤销服务令牌；
- ``issue_kafka_cert`` / ``issue_platform_cert``：通过 openssl 签发 mTLS 客户端证书；
- ``download_bundle``：打包 ZIP（CA + 两类证书 + kafka.properties + README）；
- ``remove_node``：删除 Redis + 登记表 + 证书目录。

IP 漂移不需要人工处置：心跳是唯一的自愈点 —— 每次心跳都重写地址，实例只绑 ``node_id``，
后续操控在每次调用瞬间重新解析基址。
"""
from __future__ import annotations

import base64
import io
import json
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, cast

import httpx
from fastapi import Request
from hunter_common.exceptions import (
    InvalidParameterError,
    PermissionDeniedError,
    ResourceNotFoundError,
    ServiceUnavailableError,
)
from hunter_common.logging import get_logger

from app.config import Settings
from app.core.client_ip import LearnedAddress, learn_node_endpoint
from app.repositories.sim_node_registry import SimNodeRegistryStore
from app.repositories.sim_nodes import SimNodeStore, now_epoch
from app.schemas.sim_nodes import (
    ENDPOINT_SOURCES,
    NODE_PROTOCOLS,
    IssueCertResult,
    IssueTokenResult,
    SimNodeData,
    SimNodeDetailData,
    SimNodeHeartbeatRequest,
    SimNodeListData,
    SimNodeRegisterRequest,
    SimNodeRegistryPatch,
)
from app.services.sim_node_cert_signer import (
    NodeCertArtifacts,
    aissue_kafka_cert,
    aissue_platform_cert,
    delete_node_cert_dir,
    node_dir,
    read_p12_password,
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


def _extract_jwt_jti(token: str) -> str | None:
    """从 JWT payload 解出 ``jti``（仅 base64 解码，**不验签**：令牌刚由 api-gateway 签发）。

    解不出（非三段式 / 非法 base64 / 无 jti）一律返回 None，由调用方回退 node_id 作登记标记。
    """
    try:
        payload_b64 = token.split(".")[1]
    except IndexError:
        return None
    payload_b64 += "=" * (-len(payload_b64) % 4)  # 补齐 base64url padding
    try:
        claims = json.loads(base64.urlsafe_b64decode(payload_b64.encode()))
    except (ValueError, UnicodeDecodeError):
        return None
    jti = claims.get("jti") if isinstance(claims, dict) else None
    return str(jti) if jti else None


class SimNodeService:
    """节点注册 / 心跳（含地址自学习）/ 清单 / 管理（令牌/证书）。"""

    def __init__(
        self,
        store: SimNodeStore,
        settings: Settings,
        registry_store: SimNodeRegistryStore | None = None,
    ) -> None:
        self._store = store
        self._settings = settings
        self._registry = registry_store  # None 表示登记表未装配（测试场景），管理接口会降级

    # ---------- 运维视图 ----------

    async def list_nodes(self, *, online_only: bool = True) -> SimNodeListData:
        """节点清单（RBAC scene:read）。

        ``online_only=False`` 时含租约已过但元数据键尚存的节点：现场排障需要先分清
        「节点掉线」与「平台不可达节点」，二者的判据都在 ``endpoint_*`` 与 ``last_seen_at`` 上。

        清单附带登记管理字段（展示名/令牌/证书序列号）：仅在登记表可用时批量联查补充，
        未登记的节点这些字段为 None（不影响清单可读性）。
        """
        now = now_epoch()
        records = await self._store.list_nodes(online_only=online_only, now=now)
        items = [self._to_detail(r) for r in records]
        if self._registry is not None and items:
            reg_map = await self._registry.get_map([it.node_id for it in items])
            if reg_map:
                items = [self._merge_registry(it, reg_map.get(it.node_id)) for it in items]
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

    def _to_detail(self, record: dict[str, Any]) -> SimNodeDetailData:
        """清单行视图：SimNodeData 字段 + 登记字段占位（None），后续 ``_merge_registry`` 覆盖。"""
        return SimNodeDetailData(**self._to_node_data(record).model_dump())

    @staticmethod
    def _merge_registry(node: SimNodeDetailData, row: Any) -> SimNodeDetailData:
        """把登记行字段合并进清单行（``row`` 为 None 时原样返回）。"""
        if row is None:
            return node
        return node.model_copy(
            update={
                "display_name": row.display_name,
                "registry_description": row.description,
                "owner_client_id": row.owner_client_id,
                "token_id": row.token_id,
                "token_issued_at": row.token_issued_at,
                "kafka_cert_sn": row.kafka_cert_sn,
                "kafka_cert_issued_at": row.kafka_cert_issued_at,
                "plat_cert_sn": row.plat_cert_sn,
                "plat_cert_issued_at": row.plat_cert_issued_at,
                "registry_first_seen_at": row.create_time,
                "registry_labels": row.labels or None,
            }
        )

    # ===================================================================
    # V1.24.1 仿真端管理：详情 / 令牌 / 证书 / 下线
    # ===================================================================

    def _require_registry(self) -> SimNodeRegistryStore:
        """登记表可用（未装配时 5001）。"""
        if self._registry is None:
            raise ServiceUnavailableError("仿真端登记表不可用（数据库未初始化）")
        return self._registry

    async def _ensure_registry_row(self, node_id: str) -> None:
        """懒建登记行：若不存在则创建（仅存 node_id + Redis 中的 owner/first_seen）。"""
        registry = self._require_registry()
        if await registry.exists(node_id):
            return
        # 尝试从 Redis 补充 owner 和 registered_at
        redis_record = await self._store.get_node(node_id)
        defaults: dict[str, Any] = {}
        if redis_record is not None:
            owner = str(redis_record.get("owner") or "")
            if owner:
                defaults["owner_client_id"] = owner
            reg_at = _to_datetime(redis_record.get("registered_at"))
            if reg_at is not None:
                defaults["first_seen_at"] = reg_at
        await registry.upsert(node_id, **defaults)

    async def get_detail(self, node_id: str) -> SimNodeDetailData:
        """节点详情：合并 Redis 实时态 + PostgreSQL 登记表（懒建）。"""
        now = now_epoch()
        registry = self._require_registry()
        await self._ensure_registry_row(node_id)
        # Redis 态（未注册或离线时 record 为 None）
        redis_record = await self._store.get_node(node_id)
        online = await self._store.node_online(node_id, now=now) if redis_record else False
        # DB 登记
        reg_row = await registry.get(node_id)
        # 合并视图
        base_data: dict[str, Any] = dict(redis_record) if redis_record else {"node_id": node_id}
        base_data["node_id"] = node_id
        base_data["online"] = online
        base_data["last_seen_at"] = (
            float((redis_record or {}).get("last_heartbeat_at") or 0) or None
        )
        base_data["lease_seconds"] = self._store.lease_seconds
        running = (redis_record or {}).get("running_instance_ids") or []
        base_data["running_count"] = len(running) if isinstance(running, list) else 0
        node_data = self._to_node_data(base_data)
        # 叠加上登记字段
        return SimNodeDetailData(
            **node_data.model_dump(),
            display_name=reg_row.display_name if reg_row else None,
            registry_description=reg_row.description if reg_row else None,
            owner_client_id=reg_row.owner_client_id if reg_row else None,
            token_id=reg_row.token_id if reg_row else None,
            token_issued_at=reg_row.token_issued_at if reg_row else None,
            kafka_cert_sn=reg_row.kafka_cert_sn if reg_row else None,
            kafka_cert_issued_at=reg_row.kafka_cert_issued_at if reg_row else None,
            plat_cert_sn=reg_row.plat_cert_sn if reg_row else None,
            plat_cert_issued_at=reg_row.plat_cert_issued_at if reg_row else None,
            registry_first_seen_at=reg_row.create_time if reg_row else None,
            registry_labels=reg_row.labels if reg_row and reg_row.labels else None,
        )

    async def patch_registry(self, node_id: str, payload: SimNodeRegistryPatch) -> SimNodeDetailData:
        """管理员 PATCH 登记信息（展示名/描述/标签）。"""
        registry = self._require_registry()
        await self._ensure_registry_row(node_id)
        await registry.patch(
            node_id,
            display_name=payload.display_name,
            description=payload.description,
            labels=payload.labels,
        )
        return await self.get_detail(node_id)

    async def issue_token(self, node_id: str, bearer_token: str) -> IssueTokenResult:
        """代理 api-gateway 签发服务令牌（一次性返回 access_token）。"""
        registry = self._require_registry()
        await self._ensure_registry_row(node_id)
        url = f"{self._settings.apigateway_internal_url}/api/v1/user/service-token"
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(
                    url,
                    json={"client_id": node_id},
                    headers={"Authorization": f"Bearer {bearer_token}"},
                )
        except httpx.RequestError as exc:
            logger.error("sim_node_token_issue_request_failed", node_id=node_id, error=str(exc))
            raise ServiceUnavailableError("无法连接 api-gateway，请检查集群内部通信") from exc
        if resp.status_code != 200:
            body: dict = resp.json() if resp.headers.get("content-type", "").startswith("application") else {}
            msg = body.get("message") or resp.text[:200]
            raise ServiceUnavailableError(f"api-gateway 签发令牌失败（HTTP {resp.status_code}）: {msg}")
        # 契约 ServiceToken：access_token / token_type / expires_in(秒) / client_id / roles（无 jti 字段）
        payload: dict = resp.json().get("data") or {}
        access_token: str = str(payload.get("access_token") or "")
        if not access_token:
            raise ServiceUnavailableError("api-gateway 返回的服务令牌为空")
        issued_at = datetime.now(UTC)
        expires_in = payload.get("expires_in")
        expires_at: datetime | None = (
            issued_at + timedelta(seconds=int(expires_in))
            if isinstance(expires_in, int) and not isinstance(expires_in, bool)
            else None
        )
        # token_id 取 JWT 的 jti（撤销按 client_id=node_id 走 api-gateway，jti 仅用于登记展示）
        token_id = _extract_jwt_jti(access_token) or node_id
        # 更新登记表
        await registry.set_token(
            node_id,
            token_id=token_id,
            token_issued_at=issued_at,
        )
        logger.info("sim_node_token_issued", node_id=node_id, token_id=token_id)
        return IssueTokenResult(
            node_id=node_id,
            access_token=access_token,
            token_id=token_id,
            expires_at=expires_at,
        )

    async def revoke_token(self, node_id: str, bearer_token: str) -> None:
        """代理 api-gateway 撤销服务令牌，并清空登记表。"""
        registry = self._require_registry()
        url = f"{self._settings.apigateway_internal_url}/api/v1/user/service-token/{node_id}"
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.delete(
                    url,
                    headers={"Authorization": f"Bearer {bearer_token}"},
                )
        except httpx.RequestError as exc:
            logger.error("sim_node_token_revoke_request_failed", node_id=node_id, error=str(exc))
            raise ServiceUnavailableError("无法连接 api-gateway，请检查集群内部通信") from exc
        # api-gateway 幂等：不存在也返回 200；非 200 仅记日志不阻断清空
        if resp.status_code != 200:
            logger.warning(
                "sim_node_token_revoke_unexpected_status",
                node_id=node_id,
                status=resp.status_code,
            )
        await registry.clear_token(node_id)
        logger.info("sim_node_token_revoked", node_id=node_id)

    async def issue_kafka_cert(self, node_id: str) -> IssueCertResult:
        """签发/重签 Kafka mTLS 客户端证书，并更新登记表。"""
        registry = self._require_registry()
        await self._ensure_registry_row(node_id)
        artifacts: NodeCertArtifacts = await aissue_kafka_cert(node_id)
        issued_at = datetime.now(UTC)
        await registry.set_kafka_cert(node_id, serial_hex=artifacts.serial_hex, issued_at=issued_at)
        logger.info(
            "sim_node_kafka_cert_issued",
            node_id=node_id,
            serial=artifacts.serial_hex,
        )
        return IssueCertResult(
            node_id=node_id,
            cert_type="kafka",
            serial_hex=artifacts.serial_hex,
            issued_at=issued_at,
        )

    async def issue_platform_cert(self, node_id: str) -> IssueCertResult:
        """签发/重签平台访问 mTLS 客户端证书，并更新登记表。"""
        registry = self._require_registry()
        await self._ensure_registry_row(node_id)
        artifacts: NodeCertArtifacts = await aissue_platform_cert(node_id)
        issued_at = datetime.now(UTC)
        await registry.set_plat_cert(node_id, serial_hex=artifacts.serial_hex, issued_at=issued_at)
        logger.info(
            "sim_node_platform_cert_issued",
            node_id=node_id,
            serial=artifacts.serial_hex,
        )
        return IssueCertResult(
            node_id=node_id,
            cert_type="platform",
            serial_hex=artifacts.serial_hex,
            issued_at=issued_at,
        )

    async def download_bundle(self, node_id: str) -> bytes:
        """构建并返回 ZIP（CA + 两类证书 + kafka.properties + README）。

        证书不存在时抛 ResourceNotFoundError，由路由层转 3001。
        """
        base = node_dir(node_id)
        kafka_dir = base / "kafka"
        plat_dir = base / "platform"
        if not kafka_dir.is_dir() and not plat_dir.is_dir():
            raise ResourceNotFoundError(
                f"仿真节点 {node_id} 尚未签发证书",
                details={"node_id": node_id, "hint": "请先签发 Kafka 或平台证书"},
            )
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            # CA 证书
            ca_path = Path(self._settings.kafka_ca_cert_path)
            if ca_path.is_file():
                zf.write(ca_path, "ca-cert.pem")
            # Kafka 证书
            for fname in ("client-key.pem", "client-cert.pem", "kafka-client.p12", "p12-password"):
                fpath = kafka_dir / fname
                if fpath.is_file():
                    zf.write(fpath, f"kafka/{fname}")
            # 平台证书
            for fname in ("client-key.pem", "client-cert.pem"):
                fpath = plat_dir / fname
                if fpath.is_file():
                    zf.write(fpath, f"platform/{fname}")
            # kafka.properties
            kafka_props = self._build_kafka_properties(node_id)
            if kafka_props:
                zf.writestr("kafka/kafka.properties", kafka_props)
            # README
            readme = self._build_readme(node_id)
            zf.writestr("README.md", readme)
        return buf.getvalue()

    def _build_kafka_properties(self, node_id: str, bootstrap: str = "") -> str:
        p12_pwd = read_p12_password(node_id) or "<P12_PASSWORD>"
        return (
            f"# Kafka 客户端配置（仿真节点 {node_id}）\n"
            f"# 注意：口令仅占位，详见 p12-password 文件（0600）\n"
            f"#__P12_PASSWORD__={p12_pwd}\n"
            f"bootstrap.servers={bootstrap or '<KAFKA_BOOTSTRAP>'}\n"
            f"security.protocol=SASL_SSL\n"
            f"sasl.mechanism=SCRAM-SHA-512\n"
            f"ssl.truststore.location=./ca-cert.pem\n"
            f"ssl.keystore.type=PKCS12\n"
            f"ssl.keystore.location=./kafka-client.p12\n"
            f"client.id=sim-{node_id}\n"
        )

    def _build_readme(self, node_id: str) -> str:
        host = self._settings.server_ip or "<平台 IP>"
        return (
            f"# 仿真节点接入包（{node_id}）\n\n"
            f"## 文件列表\n\n"
            f"| 文件 | 用途 |\n"
            f"|------|------|\n"
            f"| `ca-cert.pem` | Kafka CA 根证书（所有连接均需信任） |\n"
            f"| `kafka/client-key.pem` | Kafka mTLS 客户端私钥（0600） |\n"
            f"| `kafka/client-cert.pem` | Kafka mTLS 客户端证书（CN=sim-{node_id}） |\n"
            f"| `kafka/kafka-client.p12` | Kafka PKCS12（key+cert+CA 打包） |\n"
            f"| `kafka/p12-password` | PKCS12 口令（0600） |\n"
            f"| `kafka/kafka.properties` | Kafka 客户端配置示例 |\n"
            f"| `platform/client-key.pem` | 平台访问 mTLS 私钥 |\n"
            f"| `platform/client-cert.pem` | 平台访问 mTLS 证书（CN=sim-platform-{node_id}） |\n"
            f"\n## 快速开始\n\n"
            f"1. 获取服务令牌：由管理员在平台后台「仿真端管理」中签发，见平台操作手册\n"
            f"2. 使用注册 API：\n"
            f"   PUT https://{host}/api/v1/scene/sim-nodes/{node_id}\n"
            f"3. 启动心跳循环：POST .../heartbeat（周期 ≤30s）\n"
        )

    async def remove_node(self, node_id: str) -> None:
        """删除节点：Redis + 登记表 + 证书目录（幂等）。"""
        # 1. 尝试撤销令牌（best-effort，不影响删除）
        registry = self._registry
        if registry is not None:
            try:
                await registry.delete(node_id)
            except Exception:  # noqa: BLE001
                logger.exception("sim_node_registry_delete_failed", node_id=node_id)
        # 2. 删除 Redis（无论登记表是否存在）
        await self._store.delete_node(node_id)
        # 3. 删除证书目录
        deleted = delete_node_cert_dir(node_id)
        logger.info(
            "sim_node_removed",
            node_id=node_id,
            certs_cleaned=deleted,
        )


def _as_protocol(value: Any) -> NodeProtocol | None:
    text = str(value or "").strip().lower()
    return cast("NodeProtocol", text) if text in NODE_PROTOCOL_VALUES else None


def _as_endpoint_source(value: Any) -> NodeEndpointSource | None:
    text = str(value or "").strip().lower()
    return cast("NodeEndpointSource", text) if text in ENDPOINT_SOURCE_VALUES else None


__all__ = ["SimNodeService"]
