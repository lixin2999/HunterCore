"""node 模式下发适配（V1.24.0）：按节点自学习地址直推 CARLA 管理 API。

契约：``contracts/openapi/scene-service.yaml#x-hunter-simulation-flow.node_dispatch``。

为什么需要这一层：``SceneSimulationService``（855 行）承载 4.4 节 7 步流程的全部业务语义
（会话唯一性、动态 Topic、sim_id 校验、失败回滚、终态回收、对账）。direct 与 node 的差别只在
「请求发到哪个地址」，因此本模块把 ``CarlaManagementClient`` 原样复用为「按 node_id 解析当前
基址 → 委派」的代理，服务层**只依赖 :class:`SimRuntimeBackend` 协议**即可整体复用。

关键取舍：
- 实例只绑 ``node_id``（``sim:runtime``），**不绑地址串**：每次调用都重新
  :meth:`SimNodeStore.node_endpoint` 解析 ⇒ HunterSim 换 IP 后下一次心跳即自愈，
  既有实例的 ③⑥⑦ 操控不会因地址漂移而失败；
- ``sim_instance_id`` 由节点上的 CARLA 返回（与 direct 模式一致，平台不自行生成）；
- 每 node_id 缓存一个客户端，基址变化时新建并关闭旧的（上限 ``SIM_NODE_CLIENT_CACHE_MAX``）；
- 推送失败**不回落队列**：按 5001 明确暴露，``details.endpoint`` 回显目标地址，
  现场才能区分「节点离线（心跳没了）」与「节点在线但回访不通（防火墙/AP 隔离）」。
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, Protocol, TypeVar, runtime_checkable

from hunter_common.exceptions import ServiceUnavailableError
from hunter_common.logging import get_logger

from app.config import Settings
from app.repositories.carla import (
    ACTIVE_STATUSES,
    CarlaManagementClient,
    SimInstance,
    SimInstanceSnapshot,
)
from app.repositories.sim_nodes import NodeEndpoint, SimNodeStore, now_epoch
from app.services.sim_node_cert_signer import cert_paths_ready
from app.services.sim_status import to_platform_status

logger = get_logger("app.repositories.sim_dispatch")

_T = TypeVar("_T")


@runtime_checkable
class SimRuntimeBackend(Protocol):
    """仿真实例运行时后端（直连固定地址 或 按节点动态地址直推）。

    签名与 ``CarlaManagementClient`` 逐字对齐：服务层按本协议依赖，两种实现可互换。
    返回的 ``status`` 一律是「CARLA 原始状态」口径，由 ``sim_status`` 统一映射。
    """

    async def create_instance(
        self,
        *,
        scene_id: str,
        scene_name: str,
        scene_config: dict[str, Any],
        sim_id: str | None = None,
        mode: str | None = None,
        vehicle_id: str | None = None,
        vil: dict[str, Any] | None = None,
        replay_config: dict[str, Any] | None = None,
        kafka: dict[str, Any] | None = None,
        quality: str | None = None,
    ) -> SimInstance: ...

    async def find_active_instance(self, scene_id: str) -> SimInstance | None: ...

    async def submit_scenario(self, instance_id: str, payload: dict[str, Any]) -> None: ...

    async def get_instance(self, instance_id: str) -> SimInstanceSnapshot: ...

    async def get_result(self, instance_id: str) -> SimInstanceSnapshot: ...

    async def pause_instance(self, instance_id: str) -> None: ...

    async def resume_instance(self, instance_id: str) -> None: ...

    async def stop_instance(self, instance_id: str) -> None: ...

    async def close(self) -> None: ...


#: 客户端工厂签名：(base_url, node_id) → backend（node_id 用于解析该节点的 mTLS 客户端证书）
ClientFactory = Callable[[str, str], "SimRuntimeBackend"]


def _platform_mtls_material(
    node_id: str, settings: Settings
) -> tuple[tuple[str, str], str | bool] | None:
    """解析平台回访该节点所需的 mTLS 材料（``sim-platform-{node_id}`` 客户端证书 + CA）。

    仅当 ``SIM_DISPATCH_MTLS_ENABLED=true`` 且该节点平台证书产物齐全时返回；否则 None
    （回落旧明文直推，便于灰度/未开通节点仍可 http 下发）。

    V1.24.7：``SIM_DISPATCH_SSL_VERIFY=false`` 时 ssl_ca 返回 False（跳过服务端证书验证），
    适用于节点服务端证书非 Kafka CA 签发（如自签）的场景，mTLS 客户端认证仍保留。
    """
    if not settings.sim_dispatch_mtls_enabled or not node_id:
        return None
    if not cert_paths_ready(node_id, "platform"):
        logger.warning("sim_dispatch_mtls_cert_missing", node_id=node_id)
        return None
    plat_dir = Path(settings.sim_node_certs_dir) / node_id / "platform"
    cert = (str(plat_dir / "client-cert.pem"), str(plat_dir / "client-key.pem"))
    if not settings.sim_dispatch_ssl_verify:
        # 跳过服务端证书验证（节点服务端证书不由 Kafka CA 签发时使用）
        ca: str | bool = False
    else:
        ca = settings.kafka_ca_cert_path or True
    return cert, ca


def _carla_client_factory(base_url: str, settings: Settings, *, node_id: str = "") -> SimRuntimeBackend:
    """默认客户端工厂：同一套子路径配置，只把基址换成节点学到的 ``http://{ip}:{port}``。

    V1.24.2：``sim_dispatch_mtls_enabled`` 且节点平台证书就绪时，附带 mTLS 客户端证书回访。
    """
    ssl_cert: tuple[str, str] | None = None
    ssl_ca: str | bool = True
    material = _platform_mtls_material(node_id, settings)
    if material is not None:
        ssl_cert, ssl_ca = material
    return CarlaManagementClient(
        base_url,
        create_path=settings.carla_instance_create_path,
        query_path=settings.carla_instance_query_path,
        scenario_path=settings.carla_scenario_submit_path,
        timeout_seconds=settings.carla_request_timeout_seconds,
        max_retries=settings.carla_request_max_retries,
        get_path=settings.carla_instance_get_path,
        result_path=settings.carla_instance_result_path,
        pause_path=settings.carla_instance_pause_path,
        resume_path=settings.carla_instance_resume_path,
        stop_path=settings.carla_instance_stop_path,
        ssl_cert=ssl_cert,
        ssl_ca=ssl_ca,
    )


class SimNodeBackend:
    """node 模式后端：按 ``node_id`` 解析当前学到的地址，委派 CARLA 管理 API 客户端。"""

    def __init__(
        self,
        store: SimNodeStore,
        settings: Settings,
        *,
        client_factory: Callable[..., SimRuntimeBackend] | None = None,
    ) -> None:
        self._store = store
        self._settings = settings
        self._client_factory = client_factory or (
            lambda base_url, node_id="": _carla_client_factory(base_url, settings, node_id=node_id)
        )
        #: node_id → (base_url, client)；dict 保序 ⇒ 超上限时按插入序淘汰
        self._clients: dict[str, tuple[str, SimRuntimeBackend]] = {}
        self._lock = asyncio.Lock()
        self._cache_max = max(1, settings.sim_node_client_cache_max)

    # ---------- ② 创建实例（选节点 + 落归属索引） ----------

    async def create_instance(
        self,
        *,
        scene_id: str,
        scene_name: str,
        scene_config: dict[str, Any],
        sim_id: str | None = None,
        mode: str | None = None,
        vehicle_id: str | None = None,
        vil: dict[str, Any] | None = None,
        replay_config: dict[str, Any] | None = None,
        kafka: dict[str, Any] | None = None,
        quality: str | None = None,
    ) -> SimInstance:
        """选定在线节点 → 按当前地址委派创建 → 成功后登记实例归属。"""
        now = now_epoch()
        map_id = _map_id_of(scene_config)
        node = await self._store.select_node(now=now, map_id=map_id)
        if node is None:
            raise ServiceUnavailableError(
                "无可用仿真节点",
                details={
                    "scene_id": scene_id,
                    "map_id": map_id,
                    "hint": "确认 HunterSim 已注册且在租约内心跳（键 sim:node:seen）",
                    "lease_seconds": self._store.lease_seconds,
                },
            )
        node_id = str(node.get("node_id") or "")
        endpoint, client = await self._resolve(node_id)
        logger.info(
            "sim_node_dispatch_target",
            node_id=node_id,
            base_url=endpoint.base_url,
            scene_id=scene_id,
            stage="create_instance",
        )
        try:
            instance = await client.create_instance(
                scene_id=scene_id,
                scene_name=scene_name,
                scene_config=scene_config,
                sim_id=sim_id,
                mode=mode,
                vehicle_id=vehicle_id,
                vil=vil,
                replay_config=replay_config,
                kafka=kafka,
                quality=quality,
            )
        except Exception as exc:
            # sim_id 不一致等「实例已在节点上创建」的失败：先登记归属，
            # 服务层的 _stop_orphan 才能把 stop 路由回同一个节点（否则必留孤儿实例）。
            orphan_id = getattr(exc, "instance_id", None)
            if orphan_id:
                await self._bind(
                    str(orphan_id), node_id=node_id, scene_id=scene_id, sim_id=sim_id
                )
            raise
        await self._bind(
            instance.instance_id,
            node_id=node_id,
            scene_id=scene_id,
            sim_id=sim_id,
            status=instance.status or "pending",
        )
        return instance

    async def find_active_instance(self, scene_id: str) -> SimInstance | None:
        """该场景进行中的实例（读平台侧活动索引 + 向归属节点单点复核）。

        复核为终态 ⇒ 移出索引并继续找下一个（索引随查询自然收敛，不需要后台定时器）。
        ⚠ 只向候选实例的归属节点取，**不向所有节点 fan-out**：其它节点没有这个实例。
        """
        for record in await self._store.active_instance_records(now=now_epoch()):
            if str(record.get("scene_id") or "") != scene_id:
                continue
            instance_id = str(record.get("sim_instance_id") or "")
            status = str(record.get("last_status") or "")
            if to_platform_status(status).value not in ACTIVE_STATUSES:
                # 索引里的缓存值已判终态：清理后继续（无需再打节点）
                await self._store.remove_active_instance(instance_id)
                continue
            snapshot = await self._get_instance_on_node(
                str(record.get("node_id") or ""), instance_id
            )
            live_status = str(snapshot.status or "")
            await self._write_back_status(instance_id, snapshot.status)
            if to_platform_status(live_status).value not in ACTIVE_STATUSES:
                await self._store.remove_active_instance(instance_id)
                continue
            return SimInstance(
                instance_id=instance_id,
                status=live_status,
                sim_id=_as_opt_str(record.get("sim_id")),
                data=dict(snapshot.data),
            )
        return None

    # ---------- ③⑥⑦ 下发场景 / 查询进度与结果 / 控制 ----------

    async def submit_scenario(self, instance_id: str, payload: dict[str, Any]) -> None:
        await self._delegate(
            await self._node_of(instance_id),
            "submit_scenario",
            lambda client: client.submit_scenario(instance_id, payload),
        )

    async def get_instance(self, instance_id: str) -> SimInstanceSnapshot:
        snapshot = await self._delegate(
            await self._node_of(instance_id),
            "get_instance",
            lambda client: client.get_instance(instance_id),
        )
        await self._write_back_status(instance_id, snapshot.status)
        return snapshot

    async def get_result(self, instance_id: str) -> SimInstanceSnapshot:
        snapshot = await self._delegate(
            await self._node_of(instance_id),
            "get_result",
            lambda client: client.get_result(instance_id),
        )
        await self._write_back_status(instance_id, snapshot.status)
        return snapshot

    async def pause_instance(self, instance_id: str) -> None:
        await self._delegate(
            await self._node_of(instance_id),
            "pause_instance",
            lambda client: client.pause_instance(instance_id),
        )

    async def resume_instance(self, instance_id: str) -> None:
        await self._delegate(
            await self._node_of(instance_id),
            "resume_instance",
            lambda client: client.resume_instance(instance_id),
        )

    async def stop_instance(self, instance_id: str) -> None:
        await self._delegate(
            await self._node_of(instance_id),
            "stop_instance",
            lambda client: client.stop_instance(instance_id),
        )
        # 取消意图已成立：先移出活动索引，同场景可立即再次下发（allow_parallel=false 语义）；
        # 真正的会话/Topic 回收由服务层在终态时触发（与 direct 模式同一路径）。
        await self._store.remove_active_instance(instance_id)

    async def close(self) -> None:
        """关闭全部缓存客户端（应用关停时由 lifespan 调用）。"""
        async with self._lock:
            clients = [client for _, client in self._clients.values()]
            self._clients.clear()
        for client in clients:
            await self._aclose(client)

    # ---------- 内部：寻址 / 客户端缓存 / 委派 ----------

    async def _get_instance_on_node(self, node_id: str, instance_id: str) -> SimInstanceSnapshot:
        """⑥ 单点复核：只向实例的**归属节点**取（其它节点没有这个实例）。"""
        return await self._delegate(
            node_id, "get_instance", lambda client: client.get_instance(instance_id)
        )

    async def _resolve(self, node_id: str) -> tuple[NodeEndpoint, SimRuntimeBackend]:
        """解析节点当前地址并取（或重建）客户端；离线/无地址 → 5001「仿真节点离线」。"""
        if not node_id:
            raise ServiceUnavailableError(
                "仿真实例缺少节点归属", details={"hint": "sim:runtime 未记录 node_id"}
            )
        endpoint = await self._store.node_endpoint(node_id, now=now_epoch())
        if endpoint is None:
            logger.warning("sim_node_offline", node_id=node_id)
            raise ServiceUnavailableError(
                "仿真节点离线",
                details={
                    "node_id": node_id,
                    "lease_seconds": self._store.lease_seconds,
                    "hint": "心跳未在租约内刷新地址（GET /api/v1/scene/sim-nodes 可查 endpoint_ip）",
                },
            )
        client = await self._client_for(endpoint)
        return endpoint, client

    async def _client_for(self, endpoint: NodeEndpoint) -> SimRuntimeBackend:
        """按 node_id 复用客户端；基址变化（IP 漂移/端口变更）时新建并关闭旧的。"""
        async with self._lock:
            cached = self._clients.get(endpoint.node_id)
            if cached is not None and cached[0] == endpoint.base_url:
                return cached[1]
            if cached is not None:
                logger.info(
                    "sim_node_endpoint_refreshed",
                    node_id=endpoint.node_id,
                    old_base_url=cached[0],
                    new_base_url=endpoint.base_url,
                    source=endpoint.source,
                )
                del self._clients[endpoint.node_id]
                await self._aclose(cached[1])
            client = self._client_factory(endpoint.base_url, endpoint.node_id)
            self._clients[endpoint.node_id] = (endpoint.base_url, client)
            while len(self._clients) > self._cache_max:
                # dict 保序 ⇒ 迭代首项就是最早插入的节点（dict.popitem 不支持 last= 关键字）
                evicted_id = next(iter(self._clients))
                _, evicted = self._clients.pop(evicted_id)
                logger.info("sim_node_client_cache_evicted", node_id=evicted_id, max=self._cache_max)
                await self._aclose(evicted)
            return client

    async def _delegate(
        self,
        node_id: str,
        action: str,
        call: Callable[[SimRuntimeBackend], Awaitable[_T]],
    ) -> _T:
        """解析地址 → 委派 → 失败时把目标地址补进 details（5001 可定位，不吞成 5000）。"""
        endpoint, client = await self._resolve(node_id)
        logger.info(
            "sim_node_dispatch_target",
            node_id=node_id,
            base_url=endpoint.base_url,
            stage=action,
        )
        try:
            return await call(client)
        except ServiceUnavailableError as exc:
            details = dict(exc.details or {})
            details.setdefault("endpoint", endpoint.base_url)
            details.setdefault("node_id", node_id)
            raise ServiceUnavailableError(exc.message, details=details) from exc

    async def _node_of(self, instance_id: str) -> str:
        """实例归属节点（②创建时落库；此后 IP 漂移与寻址无关，每次调用重新解析地址）。"""
        record = await self._store.get_runtime(instance_id)
        if record is None:
            raise ServiceUnavailableError(
                "仿真实例缺少节点归属",
                details={
                    "sim_instance_id": instance_id,
                    "hint": "sim:runtime 不存在（未创建成功、已过 TTL，或由旧链路写入）",
                },
            )
        return str(record.get("node_id") or "")

    async def _bind(
        self,
        instance_id: str,
        *,
        node_id: str,
        scene_id: str,
        sim_id: str | None,
        status: str = "pending",
    ) -> None:
        """登记实例→节点归属 + 活动实例索引（node 模式下的寻址事实来源）。"""
        now = now_epoch()
        await self._store.save_runtime(
            instance_id,
            {
                "node_id": node_id,
                "scene_id": scene_id,
                "sim_id": sim_id,
                "last_status": status,
                "created_at": now,
                "updated_at": now,
            },
        )
        await self._store.add_active_instance(instance_id, now=now)
        logger.info(
            "sim_node_instance_bound",
            sim_instance_id=instance_id,
            node_id=node_id,
            scene_id=scene_id,
            sim_id=sim_id,
        )

    async def _write_back_status(self, instance_id: str, status: str) -> None:
        """查询成功后回写状态：让索引随查询收敛（终态成员在下次复核时被移出）。"""
        record = await self._store.get_runtime(instance_id)
        if record is None:
            return
        if str(record.get("last_status") or "") == status:
            return
        await self._store.save_runtime(
            instance_id, {**record, "last_status": status, "updated_at": now_epoch()}
        )

    async def _aclose(self, client: SimRuntimeBackend) -> None:
        try:
            await client.close()
        except Exception:  # noqa: BLE001 - 关闭旧客户端失败不应影响在途调用
            logger.warning("sim_node_client_close_failed")


def _map_id_of(scene_config: dict[str, Any]) -> str | None:
    """从场景配置取地图 ID（节点按声明的 maps 被过滤；结构异常时视为不限地图）。"""
    if not isinstance(scene_config, dict):
        return None
    map_section = scene_config.get("map")
    if not isinstance(map_section, dict):
        return None
    return _as_opt_str(map_section.get("map_id"))


def _as_opt_str(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


__all__ = ["SimNodeBackend", "SimRuntimeBackend"]
