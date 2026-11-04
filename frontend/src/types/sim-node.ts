/**
 * 仿真端（仿真节点）管理类型（scene-service，tag: sim-node）
 *
 * 契约：contracts/openapi/scene-service.yaml（SimNodeData / SimNodeDetailData /
 * SimNodeListData / SimNodeRegistryPatch / IssueTokenResult / IssueCertResult）
 * 字段命名与契约完全一致（snake_case），禁止在 API 层做驼峰转换。
 *
 * 数据来源：
 * - Redis 实时登记表 → 在线态/地址/心跳（SimNodeRow 基础字段）；
 * - PostgreSQL scene_svc.sim_node_registry → 管理侧字段（展示名/令牌/证书，仅详情叠加）。
 */

/** 地址来源标记（契约 SimNodeData.endpoint_source；排查是否被代理/网关改写） */
export type SimNodeEndpointSource = 'real-ip' | 'forwarded-for' | 'socket-peer'

/** 基址协议（契约 endpoint_learning.base_url） */
export type SimNodeProtocol = 'http' | 'https'

/** 证书类型（契约 IssueCertResult.cert_type） */
export type SimNodeCertType = 'kafka' | 'platform'

/** 节点列表行（契约 SimNodeData：清单/实时视图） */
export interface SimNodeRow {
  node_id: string
  /** 首次注册绑定的调用方身份（服务令牌为 service:{client_id}） */
  owner: string
  /** 心跳未超租约 = 在线 */
  online: boolean
  /** ISO-8601；最后一次心跳时间 */
  last_seen_at?: string | null
  lease_seconds: number
  carla_version?: string | null
  maps?: string[] | null
  capacity: number
  running_count: number
  /** 节点自述标签（机房/显卡型号等） */
  labels?: Record<string, string> | null
  /** 当前学习到的节点来源地址 */
  endpoint_ip?: string | null
  manager_port?: number | null
  protocol?: SimNodeProtocol | null
  endpoint_source?: SimNodeEndpointSource | null
  learned_at?: string | null
  /** 仅展示，不参与寻址 */
  runtime_hint?: string | null
  registered_at?: string | null
  message?: string | null
}

/** 节点详情（契约 SimNodeDetailData = SimNodeData + 登记表字段） */
export interface SimNodeDetail extends SimNodeRow {
  /** 管理员设定的展示名称 */
  display_name?: string | null
  /** 管理员设定的备注（响应字段名与请求体 description 不同，注意区分） */
  registry_description?: string | null
  /** 注册时绑定的调用方 client_id */
  owner_client_id?: string | null
  /** 已签发服务令牌的 jti；未签发或已撤销 = null */
  token_id?: string | null
  token_issued_at?: string | null
  /** Kafka 客户端证书序列号（大写 HEX） */
  kafka_cert_sn?: string | null
  kafka_cert_issued_at?: string | null
  /** 平台访问证书序列号（大写 HEX） */
  plat_cert_sn?: string | null
  plat_cert_issued_at?: string | null
  /** 登记行创建时间（≈ 管理员首次查看时间） */
  registry_first_seen_at?: string | null
  /** 管理员在登记表中设定的标签（与节点自述 labels 分开） */
  registry_labels?: Record<string, string> | null
}

/** 清单响应数据（契约 SimNodeListData；items 复用详情结构以附带登记管理字段） */
export interface SimNodeListData {
  /** 清单行附带 display_name/token_id/cert_sn 等登记字段（未登记时为 null） */
  items: SimNodeDetail[]
  total: number
}

/** 清单查询参数（GET /scene/sim-nodes；契约无分页，仅在线过滤） */
export interface SimNodeListQuery {
  /** 仅返回在线节点（契约默认 true；管理视图需 false 查看离线/登记节点） */
  online_only?: boolean
}

/** 管理字段修改请求（契约 SimNodeRegistryPatch；PATCH /scene/sim-nodes/{node_id}） */
export interface SimNodeRegistryPatch {
  display_name?: string | null
  description?: string | null
  labels?: Record<string, string> | null
}

/** 签发服务令牌一次性响应（契约 IssueTokenResult；access_token 仅出现一次，不缓存不写日志） */
export interface IssueTokenResult {
  node_id: string
  access_token: string
  token_id: string
  /** 令牌到期时间；永久令牌为 null */
  expires_at?: string | null
}

/** 签发/重签证书响应（契约 IssueCertResult） */
export interface IssueCertResult {
  node_id: string
  cert_type: SimNodeCertType
  /** 新证书序列号（大写十六进制） */
  serial_hex: string
  issued_at: string
}
