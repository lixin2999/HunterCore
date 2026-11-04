"""仿真节点控制面路由（V1.24.0，契约 tag: sim-node，operationId 逐字对齐）。

三个端点（`x-hunter-endpoints`）：清单 / 注册 / 心跳。全部落在既有网关前缀 ``/api/v1/scene`` 下
⇒ 无需修改 ``x-hunter-gateway-routes``；必须**先于 scenes 路由注册**（否则 ``/sim-nodes``
会被 ``/{scene_id}`` 抢先匹配）。

认证：节点使用**服务令牌**（api-gateway ``POST /api/v1/user/service-token``）经网关访问，
网关注入 ``X-User-Id = service:{client_id}`` 与 ``X-Roles``。角色门禁 = ``SIM_NODE_ROLES``；
节点清单是运维视图，沿用 ``scene:read``。

⚠ 注册/心跳必须拿到 ``Request`` 本体：下发目标地址是从**入站请求头**自学习的（契约
``node_dispatch.endpoint_learning``），不是请求体里的字段。

V1.24.1 仿真端管理（契约 tag: sim-node 追加 8 个管理端点）：
- 详情/PATCH（scene:read / scene:update）、令牌签发/撤销 + 证书签发 + 接入包下载（scene:execute）、
  删除节点（scene:delete）；
- ⚠ 令牌签发/撤销需代理调用 api-gateway ``POST/DELETE /api/v1/user/service-token``，而网关**会剥离
  Authorization**（后端只信任 X-User-Id/X-Roles）。故前端需经**自定义头** ``X-Admin-Access-Token``
  携带原始管理员 JWT（网关透传自定义头），``require_admin_bearer`` 优先取该头、回退 Authorization（直连/dev）。

V1.24.2 一键开通（契约 x-hunter-sim-provisioning）：
- ``POST /sim-nodes``（provisionSimNode，201）：DB→SCRAM→Topics→Cert 四步编排，响应一次性返回口令；
- ``POST /sim-nodes/{node_id}/rotate-scram``：轮换口令（新口令一次性返回）；
- ``POST /sim-nodes/{node_id}/retry-provision``（201）：失败步续跑（先回收残留再重跑，幂等）；
- ``DELETE /sim-nodes/{node_id}`` 升级为 deprovision 全链路回收，响应体回显清理摘要。
"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Header, Path, Query, Request, Response
from hunter_common.exceptions import InvalidParameterError

from app.core.dependencies import (
    get_sim_node_service,
    require_execute_permission,
    require_node_permission,
    require_read_permission,
    require_write_permission,
    trace_request_id,
)
from app.routers.errors import COMMON_ERRORS
from app.schemas.sim_nodes import (
    NODE_ID_PATTERN,
    IssueCertResponse,
    IssueTokenResponse,
    RotateScramResponse,
    SimNodeDeprovisionResponse,
    SimNodeDetailResponse,
    SimNodeEmptyResponse,
    SimNodeHeartbeatRequest,
    SimNodeListResponse,
    SimNodeProvisionRequest,
    SimNodeProvisionResponse,
    SimNodeRegisterRequest,
    SimNodeRegistryPatch,
    SimNodeRegistryPatchResponse,
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


# =====================================================================
# V1.24.1 仿真端管理：详情 / 登记 / 令牌 / 证书 / 下线
# =====================================================================


async def require_admin_bearer(
    authorization: Annotated[str | None, Header(alias="Authorization")] = None,
    x_admin_access_token: Annotated[
        str | None, Header(alias="X-Admin-Access-Token")
    ] = None,
) -> str:
    """提取可转发的管理员 Access Token（令牌签发/撤销代理用）。

    网关会剥离 ``Authorization``，故生产链路取 ``X-Admin-Access-Token``（前端携带原始 JWT，
    网关透传自定义头）；直连 / dev（无网关）回退 ``Authorization: Bearer <jwt>``。两者均缺 → 2001。
    """
    raw = x_admin_access_token or authorization or ""
    token = raw[7:].strip() if raw.lower().startswith("bearer ") else raw.strip()
    if not token:
        raise InvalidParameterError(
            "缺少可转发的管理员 Access Token：网关已剥离 Authorization，"
            "请经 X-Admin-Access-Token 头携带原始 JWT（供代理签发服务令牌）"
        )
    return token


AdminBearer = Annotated[str, Depends(require_admin_bearer)]


@router.post(
    "/sim-nodes",
    operation_id="provisionSimNode",
    response_model=SimNodeProvisionResponse,
    status_code=201,
    summary="一键开通仿真端节点（DB 建登记行 → Kafka SCRAM 口令 → 节点级回传 Topic → 双证书）",
    responses={**COMMON_ERRORS},
)
async def provision_sim_node(
    payload: SimNodeProvisionRequest,
    service: Annotated[SimNodeService, Depends(get_sim_node_service)],
    _user_id: Annotated[str, Depends(require_execute_permission)],
) -> SimNodeProvisionResponse:
    """V1.24.2：平台先建节点（不依赖节点先自注册）；``scram_password`` 仅此一次可见、不落日志。"""
    data = await service.provision(payload)
    return SimNodeProvisionResponse(data=data, request_id=trace_request_id())


@router.get(
    "/sim-nodes/{node_id}",
    operation_id="getSimNode",
    response_model=SimNodeDetailResponse,
    summary="查询仿真节点详情（Redis 实时态 + 登记表合并；懒建登记行）",
    responses={**COMMON_ERRORS},
)
async def get_sim_node(
    node_id: NodeIdPath,
    service: Annotated[SimNodeService, Depends(get_sim_node_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
) -> SimNodeDetailResponse:
    data = await service.get_detail(node_id)
    return SimNodeDetailResponse(data=data, request_id=trace_request_id())


@router.patch(
    "/sim-nodes/{node_id}",
    operation_id="patchSimNode",
    response_model=SimNodeRegistryPatchResponse,
    summary="更新仿真节点管理字段（展示名 / 描述 / 标签）",
    responses={**COMMON_ERRORS},
)
async def patch_sim_node(
    node_id: NodeIdPath,
    payload: SimNodeRegistryPatch,
    service: Annotated[SimNodeService, Depends(get_sim_node_service)],
    _user_id: Annotated[str, Depends(require_write_permission)],
) -> SimNodeRegistryPatchResponse:
    data = await service.patch_registry(node_id, payload)
    return SimNodeRegistryPatchResponse(data=data, request_id=trace_request_id())


@router.post(
    "/sim-nodes/{node_id}/issue-token",
    operation_id="issueSimNodeToken",
    response_model=IssueTokenResponse,
    summary="为仿真节点签发服务令牌（代理 api-gateway；access_token 一次性返回、不落日志）",
    responses={**COMMON_ERRORS},
)
async def issue_sim_node_token(
    node_id: NodeIdPath,
    bearer: AdminBearer,
    service: Annotated[SimNodeService, Depends(get_sim_node_service)],
    _user_id: Annotated[str, Depends(require_execute_permission)],
) -> IssueTokenResponse:
    data = await service.issue_token(node_id, bearer)
    return IssueTokenResponse(data=data, request_id=trace_request_id())


@router.delete(
    "/sim-nodes/{node_id}/token",
    operation_id="revokeSimNodeToken",
    response_model=SimNodeEmptyResponse,
    summary="撤销仿真节点服务令牌（代理 api-gateway + 清空登记表 token）",
    responses={**COMMON_ERRORS},
)
async def revoke_sim_node_token(
    node_id: NodeIdPath,
    bearer: AdminBearer,
    service: Annotated[SimNodeService, Depends(get_sim_node_service)],
    _user_id: Annotated[str, Depends(require_execute_permission)],
) -> SimNodeEmptyResponse:
    await service.revoke_token(node_id, bearer)
    return SimNodeEmptyResponse(data=None, request_id=trace_request_id())


@router.post(
    "/sim-nodes/{node_id}/issue-kafka-cert",
    operation_id="issueSimNodeKafkaCert",
    response_model=IssueCertResponse,
    summary="签发 / 重签 Kafka mTLS 客户端证书（CN=sim-{node_id}）",
    responses={**COMMON_ERRORS},
)
async def issue_sim_node_kafka_cert(
    node_id: NodeIdPath,
    service: Annotated[SimNodeService, Depends(get_sim_node_service)],
    _user_id: Annotated[str, Depends(require_execute_permission)],
) -> IssueCertResponse:
    data = await service.issue_kafka_cert(node_id)
    return IssueCertResponse(data=data, request_id=trace_request_id())


@router.post(
    "/sim-nodes/{node_id}/issue-platform-cert",
    operation_id="issueSimNodePlatformCert",
    response_model=IssueCertResponse,
    summary="签发 / 重签平台访问 mTLS 客户端证书（CN=sim-platform-{node_id}）",
    responses={**COMMON_ERRORS},
)
async def issue_sim_node_platform_cert(
    node_id: NodeIdPath,
    service: Annotated[SimNodeService, Depends(get_sim_node_service)],
    _user_id: Annotated[str, Depends(require_execute_permission)],
) -> IssueCertResponse:
    data = await service.issue_platform_cert(node_id)
    return IssueCertResponse(data=data, request_id=trace_request_id())


@router.get(
    "/sim-nodes/{node_id}/bundle",
    operation_id="downloadSimNodeBundle",
    summary="下载仿真节点接入包（ZIP：CA + Kafka/平台证书 + kafka.properties + README）",
    responses={
        200: {
            "description": "ZIP 二进制",
            "content": {"application/zip": {"schema": {"type": "string", "format": "binary"}}},
            "headers": {
                "Content-Disposition": {
                    "schema": {"type": "string"},
                    "description": 'attachment; filename="<node_id>-bundle.zip"',
                }
            },
        },
        **COMMON_ERRORS,
    },
)
async def download_sim_node_bundle(
    node_id: NodeIdPath,
    service: Annotated[SimNodeService, Depends(get_sim_node_service)],
    _user_id: Annotated[str, Depends(require_execute_permission)],
) -> Response:
    payload = await service.download_bundle(node_id)
    headers = {
        "Content-Disposition": f'attachment; filename="{node_id}-bundle.zip"',
        "X-Request-ID": trace_request_id(),
    }
    return Response(content=payload, media_type="application/zip", headers=headers)


@router.post(
    "/sim-nodes/{node_id}/rotate-scram",
    operation_id="rotateSimNodeScram",
    response_model=RotateScramResponse,
    summary="轮换仿真节点 Kafka SCRAM 口令（AdminClient UPSERT 覆盖旧凭据；新口令一次性返回）",
    responses={**COMMON_ERRORS},
)
async def rotate_sim_node_scram(
    node_id: NodeIdPath,
    service: Annotated[SimNodeService, Depends(get_sim_node_service)],
    _user_id: Annotated[str, Depends(require_execute_permission)],
) -> RotateScramResponse:
    data = await service.rotate_scram(node_id)
    return RotateScramResponse(data=data, request_id=trace_request_id())


@router.post(
    "/sim-nodes/{node_id}/retry-provision",
    operation_id="retrySimNodeProvision",
    response_model=SimNodeProvisionResponse,
    status_code=201,
    summary="从失败步续跑一键开通（幂等：先回收残留再重跑，适用于 provision 中途失败）",
    responses={**COMMON_ERRORS},
)
async def retry_sim_node_provision(
    node_id: NodeIdPath,
    service: Annotated[SimNodeService, Depends(get_sim_node_service)],
    _user_id: Annotated[str, Depends(require_execute_permission)],
) -> SimNodeProvisionResponse:
    data = await service.retry_provision(node_id)
    return SimNodeProvisionResponse(data=data, request_id=trace_request_id())


@router.delete(
    "/sim-nodes/{node_id}",
    operation_id="deleteSimNode",
    response_model=SimNodeDeprovisionResponse,
    summary="下线仿真节点并回收全部资源（证书目录 → SCRAM 用户 → 节点级 Topic → 登记行 → Redis）",
    responses={**COMMON_ERRORS},
)
async def delete_sim_node(
    node_id: NodeIdPath,
    service: Annotated[SimNodeService, Depends(get_sim_node_service)],
    _user_id: Annotated[str, Depends(require_write_permission)],
) -> SimNodeDeprovisionResponse:
    """V1.24.2：升级为 deprovision 全链路回收；响应 data 回显清理摘要（供审计）。"""
    data = await service.deprovision_node(node_id)
    return SimNodeDeprovisionResponse(data=data, request_id=trace_request_id())


__all__ = ["router"]

