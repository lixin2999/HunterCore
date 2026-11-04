"""node 模式链路测试（V1.24.0：地址自学习 + 按节点动态 IP 直推）。

覆盖三段语义（契约 ``x-hunter-simulation-flow.node_dispatch``）：
- 地址自学习：``X-Real-IP`` → ``X-Forwarded-For`` 首跳 → TCP 对端 逐级降级；反代不可信时只认对端；
  CIDR 白名单过滤；全部来源不可得 → 2001（不把下发不到的节点登成在线）；
- 按节点直推：下发基址 = ``{protocol}://{学到的 IP}:{manager_port}``，实例只绑 ``node_id``（``sim:runtime``）；
- 漂移自愈：心跳换 IP 后，③⑥⑦ 每次调用重新解析基址 ⇒ 立刻打到新地址，旧客户端被关闭（无人工处置）。

替身：``InMemorySimNodeStore``（与 Redis 实现逐条对齐语义）+ ``RecordingCarlaClientFactory``
（记录「哪个基址被调了哪个动作」），故本文件既不需要 Redis，也不需要真实网络。
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest
from httpx import AsyncClient
from hunter_common.database.enums import SceneStatus

from app.config import settings
from app.core.client_ip import build_base_url
from app.main import app
from app.repositories.sim_dispatch import SimNodeBackend
from app.repositories.sim_nodes import now_epoch
from app.services.sim_nodes import SimNodeService
from app.services.simulation import SceneSimulationService
from app.tests.conftest import ADMIN_HEADERS, VIEWER_HEADERS
from app.tests.fakes import (
    InMemorySceneRepository,
    RecordingCarlaClientFactory,
    make_scene_model,
)

SCENE_BASE = "/api/v1/scene"
NODE_BASE = f"{SCENE_BASE}/sim-nodes"
RESPONSE_FIELDS = {"code", "message", "data", "request_id", "timestamp"}
NODE_A = "huntersim-lab-01"
NODE_B = "huntersim-lab-02"
#: 实机拓扑：HunterSim 走 WiFi 被 DHCP 分配到 192.168.31.20，平台（192.168.31.35）同网段可回访
LEARNED_IP = "192.168.31.20"
DRIFT_IP = "192.168.31.21"
OTHER_IP = "192.168.31.30"
#: httpx ASGITransport 的 TCP 对端（直连部署时唯一的地址来源）
SOCKET_PEER_IP = "127.0.0.1"
#: 节点身份（服务令牌经网关注入：X-User-Id = service:{client_id}）
NODE_HEADERS = {"X-User-Id": "service:huntersim-lab", "X-Roles": "data-uploader"}
OTHER_NODE_HEADERS = {"X-User-Id": "service:other-tenant", "X-Roles": "data-uploader"}


def _data(body: dict[str, object], code: int = 0) -> dict[str, object]:
    assert set(body) == RESPONSE_FIELDS
    assert body["code"] == code, body
    return body["data"]  # type: ignore[return-value]


def _headers(
    ip: str | None = LEARNED_IP,
    *,
    base: dict[str, str] | None = None,
    extra: dict[str, str] | None = None,
) -> dict[str, str]:
    """节点身份头 + 入站地址头（``ip=None`` 表示不带 X-Real-IP，用于验证降级）。"""
    headers = dict(base if base is not None else NODE_HEADERS)
    if ip is not None:
        headers["X-Real-IP"] = ip
    headers.update(extra or {})
    return headers


def _attach_node_mode(scene_env: SimpleNamespace, factory: RecordingCarlaClientFactory) -> SimpleNamespace:
    """把运行时后端替换为 node 模式代理（等价于 ``SIM_DISPATCH_MODE=node`` 的装配）。"""
    backend = SimNodeBackend(scene_env.sim_node_store, settings, client_factory=factory)
    app.state.simulation_service = SceneSimulationService(
        scene_env.service,
        backend,
        settings,
        storage=scene_env.storage,
        sessions=scene_env.vil_sessions,
        topics=scene_env.topic_provisioner,
        topic_delete_delay_seconds=0,
    )
    app.state.sim_node_service = SimNodeService(scene_env.sim_node_store, settings)
    scene_env.simulation_service = app.state.simulation_service
    scene_env.sim_node_service = app.state.sim_node_service
    scene_env.dispatch_backend = backend
    scene_env.carla_factory = factory
    return scene_env


@pytest.fixture()
def node_env(scene_env: SimpleNamespace) -> SimpleNamespace:
    return _attach_node_mode(scene_env, RecordingCarlaClientFactory())


async def _register(
    client: AsyncClient,
    node_id: str = NODE_A,
    *,
    ip: str | None = LEARNED_IP,
    payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    extra: dict[str, str] | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {"carla_version": "0.9.15", "maps": ["Town01"], "capacity": 2}
    body.update(payload or {})
    resp = await client.put(
        f"{NODE_BASE}/{node_id}",
        headers=headers or _headers(ip, extra=extra),
        json=body,
    )
    assert resp.status_code == 200, resp.text
    return _data(resp.json())  # type: ignore[return-value]


async def _heartbeat(
    client: AsyncClient,
    node_id: str = NODE_A,
    *,
    ip: str | None = DRIFT_IP,
    body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    resp = await client.post(
        f"{NODE_BASE}/{node_id}/heartbeat", headers=_headers(ip), json=body if body is not None else {}
    )
    assert resp.status_code == 200, resp.text
    return _data(resp.json())  # type: ignore[return-value]


async def _post_run(
    client: AsyncClient, scene_id: str, json_body: dict[str, Any] | None = None
) -> Any:
    return await client.post(f"{SCENE_BASE}/{scene_id}/run", headers=ADMIN_HEADERS, json=json_body)


async def _run_scene(
    client: AsyncClient, node_env: SimpleNamespace, scene_name: str = "动态直推场景"
) -> dict[str, Any]:
    """发布态场景下发（node 模式）；返回 ``data`` 并附带 ``_scene_id`` 供后续用例复用。"""
    model = make_scene_model(scene_name=scene_name, status=SceneStatus.PUBLISHED)
    repository: InMemorySceneRepository = node_env.repository
    repository.rows[model.scene_id] = model
    resp = await _post_run(client, str(model.scene_id))
    assert resp.status_code == 200, resp.text
    data = _data(resp.json())
    data["_scene_id"] = str(model.scene_id)
    return data


# =====================================================================
# 一、地址自学习（注册/心跳入站请求头 → 登记表）
# =====================================================================


async def test_register_learns_real_ip_over_forwarded_for(
    client: AsyncClient, node_env: SimpleNamespace
) -> None:
    """X-Real-IP 优先于 X-Forwarded-For；端口未声明时取 SIM_NODE_MANAGER_DEFAULT_PORT。"""
    data = await _register(
        client,
        extra={"X-Forwarded-For": f"{DRIFT_IP}, 10.0.0.1"},
        payload={"runtime_hint": "192.168.31.20:8090"},
    )
    assert data["endpoint_ip"] == LEARNED_IP
    assert data["endpoint_source"] == "real-ip"
    assert data["manager_port"] == settings.sim_node_manager_default_port
    assert data["protocol"] == "http"
    assert data["learned_at"] is not None
    # 节点自述地址只展示，绝不参与寻址
    assert data["runtime_hint"] == "192.168.31.20:8090"
    assert "pending_tasks" not in data


async def test_register_uses_forwarded_for_first_hop(client: AsyncClient, node_env: SimpleNamespace) -> None:
    """无 X-Real-IP 时取 XFF 首跳（最靠近客户端的一跳），忽略后续代理地址。"""
    data = await _register(
        client, ip=None, extra={"X-Forwarded-For": f"{OTHER_IP}, 10.0.0.1, 172.16.0.1"}
    )
    assert data["endpoint_ip"] == OTHER_IP
    assert data["endpoint_source"] == "forwarded-for"


async def test_untrusted_proxy_ignores_forward_headers(
    client: AsyncClient, node_env: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SIM_NODE_TRUST_FORWARDED_FOR=false：客户端可伪造来源头 ⇒ 只认 TCP 对端。"""
    monkeypatch.setattr(settings, "sim_node_trust_forwarded_for", False)
    data = await _register(
        client,
        ip="8.8.8.8",
        extra={"X-Forwarded-For": "9.9.9.9"},
    )
    assert data["endpoint_ip"] == SOCKET_PEER_IP
    assert data["endpoint_source"] == "socket-peer"


async def test_illegal_forward_headers_degrade_step_by_step(
    client: AsyncClient, node_env: SimpleNamespace
) -> None:
    """域名/端口后缀等非法形态：丢弃并继续降级，而不是把垃圾值写进登记表。"""
    data = await _register(
        client,
        ip="huntersim.local",
        extra={"X-Forwarded-For": "192.168.31.20:8090, 192.168.31.24"},
    )
    assert data["endpoint_ip"] == SOCKET_PEER_IP
    assert data["endpoint_source"] == "socket-peer"


async def test_allowed_networks_accepts_in_range_address(
    client: AsyncClient, node_env: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CIDR 白名单命中的来源地址正常登记。"""
    monkeypatch.setattr(settings, "sim_node_allowed_networks", "192.168.31.0/24")
    data = await _register(client)
    assert data["endpoint_ip"] == LEARNED_IP
    assert data["endpoint_source"] == "real-ip"


async def test_address_outside_allowed_networks_returns_2001(
    client: AsyncClient, node_env: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    """所有来源都落在白名单之外 → 422 + 2001（宁可拒绝注册，也不登记一个打不通的目标）。"""
    monkeypatch.setattr(settings, "sim_node_allowed_networks", "10.0.0.0/8")
    resp = await client.put(
        f"{NODE_BASE}/{NODE_A}",
        headers=_headers(LEARNED_IP, extra={"X-Forwarded-For": OTHER_IP}),
        json={"capacity": 1},
    )
    assert resp.status_code == 422
    assert "无法判定节点地址来源" in resp.json()["message"]
    assert node_env.sim_node_store.nodes == {}


async def test_declared_port_and_protocol_shape_base_url(
    client: AsyncClient, node_env: SimpleNamespace
) -> None:
    """注册声明的 manager_port/protocol 参与基址拼装；心跳沿用上次声明、只刷新 IP。"""
    await _register(client, payload={"manager_port": 9000, "protocol": "https"})
    data = await _run_scene(client, node_env, "端口声明场景")
    base = build_base_url(LEARNED_IP, 9000, "https")
    assert node_env.carla_factory.actions_for(base) == ["create_instance", "submit_scenario"]

    refreshed = await _heartbeat(client)
    assert refreshed["manager_port"] == 9000
    assert refreshed["protocol"] == "https"
    assert refreshed["endpoint_ip"] == DRIFT_IP
    await client.get(f"{SCENE_BASE}/simulations/{data['sim_instance_id']}", headers=ADMIN_HEADERS)
    # 心跳换址后查询立刻打到新 IP:9000（协议与端口都沿用声明值）
    assert node_env.carla_factory.actions_for(build_base_url(DRIFT_IP, 9000, "https")) == [
        "get_instance"
    ]


# =====================================================================
# 二、身份、租约与 RBAC
# =====================================================================


async def test_register_binds_owner_and_echoes_lease(client: AsyncClient, node_env: SimpleNamespace) -> None:
    """首次注册把调用方写入 owner，并回显服务端租约（节点据此计算心跳间隔）。"""
    data = await _register(client, payload={"capacity": 3})
    assert data["node_id"] == NODE_A
    assert data["owner"] == "service:huntersim-lab"
    assert data["online"] is True
    assert data["capacity"] == 3
    assert data["lease_seconds"] == settings.sim_node_lease_seconds


async def test_other_caller_cannot_take_over_node(client: AsyncClient, node_env: SimpleNamespace) -> None:
    """owner 不一致的写调用 → 403 + 1002（禁止跨调用方接管节点）。"""
    await _register(client)
    resp = await client.put(
        f"{NODE_BASE}/{NODE_A}",
        headers=_headers(base=OTHER_NODE_HEADERS),
        json={"capacity": 1},
    )
    assert resp.status_code == 403
    _data(resp.json(), code=1002)


async def test_heartbeat_requires_registration(client: AsyncClient, node_env: SimpleNamespace) -> None:
    """未注册节点心跳 → 404 + 3001（必须先 PUT 注册）。"""
    resp = await client.post(
        f"{NODE_BASE}/ghost-node-01/heartbeat", headers=_headers(), json={}
    )
    assert resp.status_code == 404
    _data(resp.json(), code=3001)


async def test_node_role_gate_rejects_unauthorized_role(
    client: AsyncClient, node_env: SimpleNamespace
) -> None:
    """角色不在 SIM_NODE_ROLES → 403 + 1002（viewer 不能注册节点）。"""
    resp = await client.put(
        f"{NODE_BASE}/{NODE_A}", headers=_headers(base=VIEWER_HEADERS), json={}
    )
    assert resp.status_code == 403
    _data(resp.json(), code=1002)


async def test_unauthenticated_node_call_rejected(client: AsyncClient, node_env: SimpleNamespace) -> None:
    """缺少网关身份头 → 401 + 1001。"""
    resp = await client.put(f"{NODE_BASE}/{NODE_A}", headers={"X-Real-IP": LEARNED_IP}, json={})
    assert resp.status_code == 401
    _data(resp.json(), code=1001)


async def test_list_nodes_requires_read_permission(
    client: AsyncClient, node_env: SimpleNamespace
) -> None:
    """节点清单沿用 scene:read，并暴露当前下发目标（IP 漂移排障的入口）。"""
    await _register(client)
    listing = await client.get(NODE_BASE, headers=ADMIN_HEADERS)
    assert listing.status_code == 200
    data = _data(listing.json())
    assert data["total"] == 1
    item = data["items"][0]  # type: ignore[index]
    assert item["node_id"] == NODE_A
    assert item["endpoint_ip"] == LEARNED_IP
    assert item["endpoint_source"] == "real-ip"
    denied = await client.get(NODE_BASE, headers=VIEWER_HEADERS)
    assert denied.status_code == 403
    _data(denied.json(), code=1002)


# =====================================================================
# 三、下发：按学到的地址直推节点 CARLA 管理 API
# =====================================================================


async def test_run_scene_pushes_to_learned_endpoint(
    client: AsyncClient, node_env: SimpleNamespace
) -> None:
    """②③ 都打到 ``http://{学到的 IP}:{manager_port}``；实例 ID 由节点上 CARLA 返回。"""
    await _register(client)
    base = build_base_url(LEARNED_IP, settings.sim_node_manager_default_port, "http")
    data = await _run_scene(client, node_env)
    instance_id = str(data["sim_instance_id"])

    factory: RecordingCarlaClientFactory = node_env.carla_factory
    assert factory.actions_for(base) == ["create_instance", "submit_scenario"]
    assert factory.created[0]["base_url"] == base
    assert factory.created[0]["instance_id"] == instance_id
    assert data["sim_id"] == factory.created[0]["sim_id"]
    assert data["status"] == "running"

    store = node_env.sim_node_store
    # 实例只绑 node_id（不绑地址串）：这是抗 IP 漂移的落点
    assert store.runtime[instance_id]["node_id"] == NODE_A
    assert store.runtime[instance_id]["scene_id"] == data["_scene_id"]
    assert instance_id in store.active


async def test_ipv6_endpoint_gets_bracketed_base_url(
    client: AsyncClient, node_env: SimpleNamespace
) -> None:
    """IPv6 来源地址必须加方括号拼 URL，否则端口分隔符会被当作地址的一部分。"""
    await _register(client, ip="fe80::1", payload={"manager_port": 8099})
    await _run_scene(client, node_env, "IPv6 场景")
    assert node_env.carla_factory.actions_for("http://[fe80::1]:8099") == [
        "create_instance",
        "submit_scenario",
    ]


async def test_control_and_query_use_same_learned_endpoint(
    client: AsyncClient, node_env: SimpleNamespace
) -> None:
    """⑥⑦ 与控制面全部委派同一个基址；子路径配置与 direct 模式完全共用。"""
    await _register(client)
    base = build_base_url(LEARNED_IP, settings.sim_node_manager_default_port, "http")
    data = await _run_scene(client, node_env, "同源操控场景")
    instance_id = str(data["sim_instance_id"])

    progress = await client.get(f"{SCENE_BASE}/simulations/{instance_id}", headers=ADMIN_HEADERS)
    assert progress.status_code == 200
    assert _data(progress.json())["status"] == "running"
    for action in ("pause", "resume", "cancel"):
        resp = await client.post(f"{SCENE_BASE}/simulations/{instance_id}/{action}", headers=ADMIN_HEADERS)
        assert resp.status_code == 200, resp.text

    factory: RecordingCarlaClientFactory = node_env.carla_factory
    assert factory.actions_for(base) == [
        "create_instance",
        "submit_scenario",
        "get_instance",  # ⑥ 进度
        "get_instance",  # pause 前的状态校验
        "pause_instance",
        "get_instance",
        "resume_instance",
        "get_instance",
        "stop_instance",
    ]
    assert len([item for item in factory.clients if item.base_url == base]) == 1
    assert instance_id not in node_env.sim_node_store.active  # cancel 即刻移出索引


async def test_endpoint_drift_self_heals_on_heartbeat(
    client: AsyncClient, node_env: SimpleNamespace
) -> None:
    """需求实质：HunterSim 换 IP 后不导致下发失败 —— 心跳刷新登记表，后续操控立刻打新地址。"""
    await _register(client)
    old_base = build_base_url(LEARNED_IP, settings.sim_node_manager_default_port, "http")
    new_base = build_base_url(DRIFT_IP, settings.sim_node_manager_default_port, "http")
    data = await _run_scene(client, node_env, "IP 漂移场景")
    instance_id = str(data["sim_instance_id"])
    factory: RecordingCarlaClientFactory = node_env.carla_factory

    await _heartbeat(client)  # DHCP 重新分配后的下一次心跳

    for action in (None, "pause", "cancel"):
        url = f"{SCENE_BASE}/simulations/{instance_id}" + (f"/{action}" if action else "")
        if action:
            resp = await client.post(url, headers=ADMIN_HEADERS)
        else:
            resp = await client.get(url, headers=ADMIN_HEADERS)
        assert resp.status_code == 200, resp.text

    assert factory.actions_for(new_base) == [
        "get_instance",
        "get_instance",
        "pause_instance",
        "get_instance",
        "stop_instance",
    ]
    # 旧基址在漂移后没有被再打过，且旧客户端已关闭（不会残留连接池指向已失效地址）
    assert factory.actions_for(old_base) == ["create_instance", "submit_scenario"]
    assert old_base in factory.closed_base_urls
    assert len([item for item in factory.clients if item.base_url == new_base]) == 1


async def test_result_query_pushes_to_current_endpoint_and_writes_back_status(
    client: AsyncClient, node_env: SimpleNamespace
) -> None:
    """终态判定回到「查询即回收」的既有路径：get_result 打到当前地址并回写 last_status。"""
    await _register(client)
    base = build_base_url(LEARNED_IP, settings.sim_node_manager_default_port, "http")
    data = await _run_scene(client, node_env, "结果查询场景")
    instance_id = str(data["sim_instance_id"])
    node_env.carla_factory.client_for(base).results[instance_id] = {
        "status": "succeeded",
        "success": True,
        "message": "完成",
    }

    result = await client.get(f"{SCENE_BASE}/simulations/{instance_id}/result", headers=ADMIN_HEADERS)
    assert result.status_code == 200
    body = _data(result.json())
    assert body["status"] == "succeeded"
    assert body["success"] is True
    assert node_env.sim_node_store.runtime[instance_id]["last_status"] == "succeeded"
    await asyncio.sleep(0.01)  # 延迟删 Topic（topic_delete_delay_seconds=0）落地
    assert data["sim_id"] in node_env.topic_provisioner.deleted


async def test_run_scene_without_online_node_returns_5001(
    client: AsyncClient, node_env: SimpleNamespace
) -> None:
    """无在线节点 → 503 + 5001（不静默排队、也不把「没节点」当成服务内部错误）。"""
    resp = await _post_run(client, await _published_scene_id(node_env, "无节点场景"))
    assert resp.status_code == 503
    assert "无可用仿真节点" in resp.json()["message"]


async def test_map_declaration_mismatch_returns_5001(
    client: AsyncClient, node_env: SimpleNamespace
) -> None:
    """节点声明的 maps 不含场景地图 → 选不出节点 → 5001（不盲目推给必然失败的节点）。"""
    await _register(client, payload={"maps": ["Town07"]})
    resp = await _post_run(client, await _published_scene_id(node_env, "地图不匹配场景"))
    assert resp.status_code == 503
    assert "无可用仿真节点" in resp.json()["message"]
    assert node_env.carla_factory.calls == []


async def test_unreachable_endpoint_returns_5001_not_5000(
    client: AsyncClient, node_env: SimpleNamespace
) -> None:
    """节点在线（心跳正常）但平台回访不通（防火墙/AP 隔离）→ 5001 明确暴露，不吞成 5000。"""
    base = build_base_url(LEARNED_IP, settings.sim_node_manager_default_port, "http")
    node_env.carla_factory.unreachable.add(base)
    await _register(client)
    resp = await _post_run(client, await _published_scene_id(node_env, "回访不通场景"))
    assert resp.status_code == 503
    assert resp.json()["code"] == 5001
    # 创建未成功 ⇒ 不留下任何归属索引
    assert node_env.sim_node_store.runtime == {}
    assert node_env.sim_node_store.active == {}


async def test_lease_expiry_maps_to_node_offline_5001(
    client: AsyncClient, node_env: SimpleNamespace
) -> None:
    """租约过期（心跳断了）→ 后续操控 503 + 5001「仿真节点离线」，而不是打到旧地址。"""
    await _register(client)
    data = await _run_scene(client, node_env, "租约过期场景")
    instance_id = str(data["sim_instance_id"])
    node_env.sim_node_store.seen[NODE_A] = now_epoch() - settings.sim_node_lease_seconds - 1

    resp = await client.get(f"{SCENE_BASE}/simulations/{instance_id}", headers=ADMIN_HEADERS)
    assert resp.status_code == 503
    assert "仿真节点离线" in resp.json()["message"]


async def test_missing_runtime_binding_returns_5001(
    client: AsyncClient, node_env: SimpleNamespace
) -> None:
    """``sim:runtime`` 没有归属记录（旧链路写入/已过 TTL）→ 5001 指向排障入口，不猜地址。"""
    resp = await client.get(f"{SCENE_BASE}/simulations/sim-node-9999", headers=ADMIN_HEADERS)
    assert resp.status_code == 503
    assert "缺少节点归属" in resp.json()["message"]


async def test_second_run_conflicts_until_parallel_allowed(
    client: AsyncClient, node_env: SimpleNamespace
) -> None:
    """活动索引内已有实例 → 3003（allow_parallel=false）；允许并行则复用同一节点上的实例。"""
    await _register(client)
    data = await _run_scene(client, node_env, "并行判定场景")
    scene_id = data["_scene_id"]
    conflict = await _post_run(client, scene_id)
    assert conflict.status_code == 409
    _data(conflict.json(), code=3003)

    reused = await _post_run(client, scene_id, {"sim_config": {"allow_parallel": True}})
    assert reused.status_code == 200
    assert _data(reused.json())["sim_instance_id"] == data["sim_instance_id"]


async def test_active_index_rechecks_only_owning_node(
    client: AsyncClient, node_env: SimpleNamespace
) -> None:
    """find_active_instance 只向候选实例的归属节点单点复核（不 fan-out）；终态后移出索引可再下发。"""
    await _register(client, NODE_A, ip=LEARNED_IP)
    await _register(client, NODE_B, ip=OTHER_IP)
    base_a = build_base_url(LEARNED_IP, settings.sim_node_manager_default_port, "http")
    base_b = build_base_url(OTHER_IP, settings.sim_node_manager_default_port, "http")
    data = await _run_scene(client, node_env, "单点复核场景")
    instance_id = str(data["sim_instance_id"])
    scene_id = data["_scene_id"]
    factory: RecordingCarlaClientFactory = node_env.carla_factory
    # 节点上已跑完：复核发现终态 ⇒ 移出索引，同场景允许再次下发（无需后台定时器）
    factory.client_for(base_a).progress[instance_id] = {"status": "succeeded"}

    again = await _post_run(client, scene_id)
    assert again.status_code == 200
    new_instance_id = str(_data(again.json())["sim_instance_id"])
    assert new_instance_id != instance_id
    assert instance_id not in node_env.sim_node_store.active
    # 复核只打了归属节点；另一台在线节点从头到尾没被联系过（不 fan-out）
    assert (base_b, "get_instance") not in factory.calls
    assert factory.actions_for(base_b) == []
    assert factory.actions_for(base_a).count("get_instance") == 1


async def test_client_cache_evicts_when_over_limit(
    client: AsyncClient, scene_env: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    """缓存超上限按插入序淘汰；被淘汰节点的后续调用会重建客户端（不影响下发正确性）。"""
    # 上限在构造期取值 ⇒ 必须先改配置、再装配后端（因此不依赖 node_env 夹具）
    monkeypatch.setattr(settings, "sim_node_client_cache_max", 1)
    node_env = _attach_node_mode(scene_env, RecordingCarlaClientFactory())
    await _register(client, NODE_A, ip=LEARNED_IP)
    await _register(client, NODE_B, ip=OTHER_IP)
    base_a = build_base_url(LEARNED_IP, settings.sim_node_manager_default_port, "http")
    base_b = build_base_url(OTHER_IP, settings.sim_node_manager_default_port, "http")
    first = await _run_scene(client, node_env, "缓存淘汰场景")
    instance_a = str(first["sim_instance_id"])
    # 节点 A 报满 ⇒ 下一次下发选到 B，插入 B 时淘汰 A 的客户端
    await _heartbeat(client, NODE_A, ip=LEARNED_IP, body={"running_instance_ids": [instance_a]})
    second = await _run_scene(client, node_env, "缓存淘汰场景-2")
    assert node_env.carla_factory.actions_for(base_b) == ["create_instance", "submit_scenario"]

    progress = await client.get(
        f"{SCENE_BASE}/simulations/{instance_a}", headers=ADMIN_HEADERS
    )
    assert progress.status_code == 200
    assert base_a in node_env.carla_factory.closed_base_urls
    assert len([item for item in node_env.carla_factory.clients if item.base_url == base_a]) == 2
    assert str(_data(progress.json())["sim_instance_id"]) == instance_a
    # 两个实例各自绑住创建时的节点（淘汰客户端不改变归属）
    assert node_env.sim_node_store.runtime[instance_a]["node_id"] == NODE_A
    assert (
        node_env.sim_node_store.runtime[str(second["sim_instance_id"])]["node_id"] == NODE_B
    )


async def test_close_releases_every_cached_client(client: AsyncClient, node_env: SimpleNamespace) -> None:
    """应用关停（lifespan）关闭全部缓存客户端，不遗留连接池。"""
    await _register(client)
    await _run_scene(client, node_env, "关停场景")
    assert node_env.carla_factory.live_clients()
    await node_env.dispatch_backend.close()
    assert node_env.carla_factory.live_clients() == []


async def test_store_failure_maps_to_5001(client: AsyncClient, node_env: SimpleNamespace) -> None:
    """节点存储不可用 → 503 + 5001（不得伪装成 5000）。"""
    await _register(client)
    node_env.sim_node_store.unavailable = True
    resp = await client.put(f"{NODE_BASE}/{NODE_A}", headers=_headers(), json={"capacity": 1})
    assert resp.status_code == 503
    _data(resp.json(), code=5001)


async def _published_scene_id(node_env: SimpleNamespace, scene_name: str) -> str:
    """登记一个发布态场景并返回 ID（下发失败类用例只需要 scene_id）。"""
    model = make_scene_model(scene_name=scene_name, status=SceneStatus.PUBLISHED)
    node_env.repository.rows[model.scene_id] = model
    return str(model.scene_id)


# =====================================================================
# 装配完整性：路由必须在 scenes 之前注册，operationId 与契约逐字一致
# =====================================================================


@pytest.mark.parametrize(
    ("method", "path", "operation_id"),
    [
        ("get", "/api/v1/scene/sim-nodes", "listSimNodes"),
        ("put", "/api/v1/scene/sim-nodes/{node_id}", "registerSimNode"),
        ("post", "/api/v1/scene/sim-nodes/{node_id}/heartbeat", "heartbeatSimNode"),
        # V1.24.1 仿真端管理
        ("get", "/api/v1/scene/sim-nodes/{node_id}", "getSimNode"),
        ("patch", "/api/v1/scene/sim-nodes/{node_id}", "patchSimNode"),
        ("delete", "/api/v1/scene/sim-nodes/{node_id}", "deleteSimNode"),
        ("post", "/api/v1/scene/sim-nodes/{node_id}/issue-token", "issueSimNodeToken"),
        ("delete", "/api/v1/scene/sim-nodes/{node_id}/token", "revokeSimNodeToken"),
        ("post", "/api/v1/scene/sim-nodes/{node_id}/issue-kafka-cert", "issueSimNodeKafkaCert"),
        ("post", "/api/v1/scene/sim-nodes/{node_id}/issue-platform-cert", "issueSimNodePlatformCert"),
        ("get", "/api/v1/scene/sim-nodes/{node_id}/bundle", "downloadSimNodeBundle"),
    ],
)
def test_openapi_exposes_contract_operations(method: str, path: str, operation_id: str) -> None:
    """FastAPI 实际注册的路由与契约 operationId 一致（防止 include_router 顺序/拼写漂移）。"""
    schema = app.openapi()
    assert operation_id == schema["paths"][path][method]["operationId"]


def test_task_queue_endpoints_are_removed_from_openapi() -> None:
    """队列/长轮询/ack/回报四个端点已随链路改造删除（残留即说明装配未收敛）。"""
    paths = app.openapi()["paths"]
    for legacy in (
        "/api/v1/scene/sim-nodes/{node_id}/tasks",
        "/api/v1/scene/sim-nodes/{node_id}/tasks/{task_id}/ack",
        "/api/v1/scene/sim-nodes/{node_id}/instances/{sim_instance_id}/report",
    ):
        assert legacy not in paths
    node_paths = {path for path in paths if "sim-nodes" in path}
    assert node_paths == {
        "/api/v1/scene/sim-nodes",
        "/api/v1/scene/sim-nodes/{node_id}",
        "/api/v1/scene/sim-nodes/{node_id}/heartbeat",
        "/api/v1/scene/sim-nodes/{node_id}/issue-token",
        "/api/v1/scene/sim-nodes/{node_id}/token",
        "/api/v1/scene/sim-nodes/{node_id}/issue-kafka-cert",
        "/api/v1/scene/sim-nodes/{node_id}/issue-platform-cert",
        "/api/v1/scene/sim-nodes/{node_id}/bundle",
        # V1.24.2 一键开通（provisionSimNode 复用 POST /sim-nodes；以下为新增子路径）
        "/api/v1/scene/sim-nodes/{node_id}/rotate-scram",
        "/api/v1/scene/sim-nodes/{node_id}/retry-provision",
    }
