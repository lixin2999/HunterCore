/**
 * 仿真端管理展示词表（scene-service，tag: sim-node）
 *
 * 来源：contracts/openapi/scene-service.yaml（SimNodeData.online、
 * IssueCertResult.cert_type、endpoint_source 等）。
 *
 * ⚠ 仅用于 UI 展示；业务分支必须以契约 code/enum 为准，禁止基于本表字符串匹配。
 * RBAC 说明：仿真端管理**复用 scene 权限域**（scene:read/update/execute/delete），
 * 不引入新权限域，故 constants/permissions.ts 无新增编码。
 */

/** 在线状态展示标签（SimNodeRow.online） */
export const SIM_NODE_ONLINE_LABELS: Record<string, string> = {
  true: '在线',
  false: '离线',
}

/** 在线状态 → Element Plus 标签类型 */
export const SIM_NODE_ONLINE_TAG_TYPES: Record<string, string> = {
  true: 'success',
  false: 'info',
}

/** 凭证签发状态标签（依据详情字段是否为空派生：非业务枚举，仅 UI） */
export const SIM_NODE_CRED_STATUS_LABELS: Record<string, string> = {
  issued: '已签发',
  none: '未签发',
}

/** 凭证签发状态 → Element Plus 标签类型 */
export const SIM_NODE_CRED_STATUS_TAG_TYPES: Record<string, string> = {
  issued: 'success',
  none: 'info',
}

/** 证书类型展示标签（IssueCertResult.cert_type） */
export const SIM_NODE_CERT_TYPE_LABELS: Record<string, string> = {
  kafka: 'Kafka 客户端证书',
  platform: '平台访问证书',
}

/** 地址来源展示标签（SimNodeData.endpoint_source，排障用） */
export const SIM_NODE_ENDPOINT_SOURCE_LABELS: Record<string, string> = {
  'real-ip': 'real-ip（可信）',
  'forwarded-for': 'X-Forwarded-For（代理改写）',
  'socket-peer': 'socket 对端（直连）',
}

/** node_id 校验（契约 SimNodeIdPath.pattern：与后端 NODE_ID_PATTERN 一致） */
export const SIM_NODE_ID_PATTERN = /^[a-z0-9][a-z0-9._-]{2,63}$/

/** 管理字段长度上限（契约 SimNodeRegistryPatch） */
export const SIM_NODE_DISPLAY_NAME_MAX = 128
export const SIM_NODE_DESCRIPTION_MAX = 500
