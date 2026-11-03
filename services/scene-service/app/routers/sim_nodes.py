"""仿真节点控制面路由（V1.24.0，契约 tag: sim-node，operationId 逐字对齐）。

三个端点（`x-hunter-endpoints`）：清单 / 注册 / 心跳。全部落在既有网关前缀 ``/api/v1/scene`` 下
⇒ 无需修改 ``x-hunter-gateway-routes``；必须**先于 scenes 路由注册**（否则 ``/sim-nodes``
会被 ``/{scene_id}`` 抢先匹配）。

认证：节点使用**服务令牌**（api-gateway ``POST /api/v1/user/service-token``）经网关访问，
网关注入 ``X-User-Id = service:{client_id}`` 与 ``X-Roles``。角色门禁 = ``SIM_NODE_ROLES``；
节点清单是运维视图，沿用 ``scene:read``。

⚠ 注册/心跳必须拿到 ``Request`` 本体：下发目标地址是从**入站请求头**自学习的（契约
``node_dispatch.endpoint_learning``），不是请求体里的字段。
"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query, Request

from app.core.dependencies import (
    get_sim_node_service,
    require_node_permission,
    require_read_permission,
    trace_request_id,
)
from app.routers.errors import COMMON_ERRORS
from app.schemas.sim_nodes import (
    NODE_ID_PATTERN,
    SimNodeHeartbeatRequest,
    SimNodeListResponse,
    SimNodeRegisterRequest,
    SimNodeResponse,
)
from app.services.sim_nodes import SimNodeService

router = APIRouter(prefix="/api/v1/scene", tags=["sim-node"])

NodeIdPath = Annotated[
    str, Path(pattern=NODE_ID_PATTERN, description="仿真节点标识（与 IP 无关，节点自行命名且保持稳定）")
]


@router.get(
    "/sim-nodes",
    operation_id="listSimNodes",
    response_model=SimNodeListResponse,
    summary="查询仿真节点清单（含在线状态与当前下发的目标地址）",
    responses={**COMMON_ERRORS},
)
async def list_sim_nodes(
    service: Annotated[SimNodeService, Depends(get_sim_node_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
    online_only: Annotated[bool, Query(description="仅返回在线节点（默认 true）")] = True,
) -> SimNodeListResponse:
    """在线判定 = 心跳未落后超租约；读取时顺手清理已过期成员（懒清理）。"""
    data = await service.list_nodes(online_only=online_only)
    return SimNodeListResponse(data=data, request_id=trace_request_id())


@router.put(
    "/sim-nodes/{node_id}",
    operation_id="registerSimNode",
    response_model=SimNodeResponse,
    summary="注册/更新仿真节点（幂等 upsert：能力声明 + 地址自学习 + 续租）",
    responses={**COMMON_ERRORS},
)
async def register_sim_node(
    node_id: NodeIdPath,
    http_request: Request,
    payload: SimNodeRegisterRequest,
    service: Annotated[SimNodeService, Depends(get_sim_node_service)],
    caller: Annotated[str, Depends(require_node_permission)],
) -> SimNodeResponse:
    """首次注册绑定 owner；地址由服务端自学习（学习失败 → 2001），``runtime_hint`` 仅展示。"""
    data = await service.register(node_id, payload, caller, http_request)
    return SimNodeResponse(data=data, request_id=trace_request_id())


@router.post(
    "/sim-nodes/{node_id}/heartbeat",
    operation_id="heartbeatSimNode",
    response_model=SimNodeResponse,
    summary="节点心跳（续租 + 覆盖式刷新来源地址：IP 漂移的唯一自愈点）",
    responses={**COMMON_ERRORS},
)
async def heartbeat_sim_node(
    node_id: NodeIdPath,
    http_request: Request,
    service: Annotated[SimNodeService, Depends(get_sim_node_service)],
    caller: Annotated[str, Depends(require_node_permission)],
    payload: SimNodeHeartbeatRequest | None = None,
) -> SimNodeResponse:
    """轻量续租（请求体可空）；未注册 → 3001（必须先 PUT 注册）。"""
    data = await service.heartbeat(node_id, payload, caller, http_request)
    return SimNodeResponse(data=data, request_id=trace_request_id())


__all__ = ["router"]
