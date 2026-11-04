/**
 * 仿真端管理 API（scene-service，tag: sim-node）
 *
 * 契约：contracts/openapi/scene-service.yaml
 * RBAC：复用 scene 权限域 —— 详情读取 scene:read；管理字段 PATCH scene:update；
 *      令牌签发/撤销 + 证书签发 + 接入包下载 scene:execute；删除节点 scene:delete。
 * 关键约束：
 * - 服务令牌 access_token 仅签发响应一次性返回，前端不缓存、不写日志；
 * - 令牌签发/撤销需代理 api-gateway，而网关剥离 Authorization，
 *   故这两类请求经 withAdminBearer 附带自定义头 X-Admin-Access-Token（见 api/request.ts）。
 */
import { downloadBlob, request } from './request'
import type {
  IssueCertResult,
  IssueTokenResult,
  RotateScramResult,
  SimNodeDeprovisionResult,
  SimNodeDetail,
  SimNodeListData,
  SimNodeListQuery,
  SimNodeProvisionRequest,
  SimNodeProvisionResult,
  SimNodeRegistryPatch,
} from '@/types/sim-node'

/* ------------------------------ 清单与详情 ------------------------------ */

/** 节点清单（GET /api/v1/scene/sim-nodes；online_only=false 含离线/登记节点） */
export function listSimNodes(params: SimNodeListQuery = {}): Promise<SimNodeListData> {
  return request<SimNodeListData>({ url: '/scene/sim-nodes', method: 'get', params })
}

/** 节点详情（GET /api/v1/scene/sim-nodes/{node_id}；Redis 实时态 + 登记表合并，懒建登记行） */
export function getSimNode(nodeId: string): Promise<SimNodeDetail> {
  return request<SimNodeDetail>({
    url: `/scene/sim-nodes/${encodeURIComponent(nodeId)}`,
    method: 'get',
  })
}

/** 更新管理字段（PATCH /api/v1/scene/sim-nodes/{node_id}；展示名/描述/标签） */
export function patchSimNode(nodeId: string, payload: SimNodeRegistryPatch): Promise<SimNodeDetail> {
  return request<SimNodeDetail>({
    url: `/scene/sim-nodes/${encodeURIComponent(nodeId)}`,
    method: 'patch',
    data: payload,
  })
}

/* ------------------------------ 服务令牌 ------------------------------ */

/**
 * 签发服务令牌（POST /api/v1/scene/sim-nodes/{node_id}/issue-token）
 * 一次性返回 access_token（不缓存）；代理 api-gateway，故 withAdminBearer 透传原始 JWT。
 */
export function issueSimNodeToken(nodeId: string): Promise<IssueTokenResult> {
  return request<IssueTokenResult>({
    url: `/scene/sim-nodes/${encodeURIComponent(nodeId)}/issue-token`,
    method: 'post',
    withAdminBearer: true,
  })
}

/** 撤销服务令牌（DELETE /api/v1/scene/sim-nodes/{node_id}/token；同样需代理，withAdminBearer） */
export function revokeSimNodeToken(nodeId: string): Promise<null> {
  return request<null>({
    url: `/scene/sim-nodes/${encodeURIComponent(nodeId)}/token`,
    method: 'delete',
    withAdminBearer: true,
  })
}

/* ------------------------------ 证书 ------------------------------ */

/** 签发/重签 Kafka mTLS 客户端证书（POST .../issue-kafka-cert；CN=sim-{node_id}） */
export function issueSimNodeKafkaCert(nodeId: string): Promise<IssueCertResult> {
  return request<IssueCertResult>({
    url: `/scene/sim-nodes/${encodeURIComponent(nodeId)}/issue-kafka-cert`,
    method: 'post',
  })
}

/** 签发/重签平台访问 mTLS 客户端证书（POST .../issue-platform-cert；CN=sim-platform-{node_id}） */
export function issueSimNodePlatformCert(nodeId: string): Promise<IssueCertResult> {
  return request<IssueCertResult>({
    url: `/scene/sim-nodes/${encodeURIComponent(nodeId)}/issue-platform-cert`,
    method: 'post',
  })
}

/**
 * 下载接入包 ZIP（GET .../bundle；契约非统一 JSON 响应 → 走 downloadBlob）
 * 文件名从 Content-Disposition 提取（attachment; filename="<node_id>-bundle.zip"）。
 */
export function downloadSimNodeBundle(nodeId: string): Promise<{ blob: Blob; filename: string }> {
  return downloadBlob(
    `/scene/sim-nodes/${encodeURIComponent(nodeId)}/bundle`,
    `${nodeId}-bundle.zip`,
  )
}

/* ------------------------------ 下线 ------------------------------ */

/**
 * 下线回收（DELETE /api/v1/scene/sim-nodes/{node_id}，V1.24.2 升级为 deprovision）
 * 逆序回收：证书目录 → SCRAM 用户 → 节点级 Topic（含 .dlq）→ 登记行 → Redis；返回各项清理摘要。
 */
export function deleteSimNode(nodeId: string): Promise<SimNodeDeprovisionResult> {
  return request<SimNodeDeprovisionResult>({
    url: `/scene/sim-nodes/${encodeURIComponent(nodeId)}`,
    method: 'delete',
  })
}

/* ------------------------------ V1.24.2 一键开通 ------------------------------ */

/**
 * 一键开通仿真端（POST /api/v1/scene/sim-nodes，scene:execute，201）
 * 四步编排 db→scram→topics→cert；任一步失败逆序回滚。
 * scram_password 仅此响应一次性返回，不落库/不入接入包/不写日志。
 */
export function provisionSimNode(payload: SimNodeProvisionRequest): Promise<SimNodeProvisionResult> {
  return request<SimNodeProvisionResult>({
    url: '/scene/sim-nodes',
    method: 'post',
    data: payload,
  })
}

/** 从失败步续跑开通（POST .../retry-provision，scene:execute，201；先回收本次残留再重跑四步） */
export function retrySimNodeProvision(nodeId: string): Promise<SimNodeProvisionResult> {
  return request<SimNodeProvisionResult>({
    url: `/scene/sim-nodes/${encodeURIComponent(nodeId)}/retry-provision`,
    method: 'post',
  })
}

/** 轮换 SCRAM 口令（POST .../rotate-scram，scene:execute；新口令仅一次性返回） */
export function rotateSimNodeScram(nodeId: string): Promise<RotateScramResult> {
  return request<RotateScramResult>({
    url: `/scene/sim-nodes/${encodeURIComponent(nodeId)}/rotate-scram`,
    method: 'post',
  })
}
