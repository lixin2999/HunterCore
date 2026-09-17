/**
 * RBAC 权限编码（契约依据）
 *
 * 来源：
 * - api-gateway.yaml UserProfile.permissions 描述：权限编码形如 `scene:read`；
 * - scene-service.yaml：资源域 `scene`，action ∈ create/read/update/delete/execute；
 * - data-collector.yaml：资源域 `data`，action ∈ create/read/execute（`data:read` 含全量数据权限）；
 * - data-analytics.yaml：`analytics:read`（查询）、`analytics:execute`（报告生成）；
 * - ota-service.yaml：`ota:read` / `ota:create` / `ota:execute`；
 * - remote-control.yaml：`remote:read` / `remote:create` / `remote:execute`。
 *
 * 约定：编码由服务端下发（JWT → /user/me），前端只做成员判断，**不得自行发明编码**。
 */

/** 资源域（各服务契约 RBAC 资源名） */
export const PERMISSION_RESOURCES = ['scene', 'data', 'analytics', 'ota', 'remote'] as const

/** 资源动作集合（scene 支持全 5 种；其余按契约取子集） */
export const PERMISSION_ACTIONS = ['create', 'read', 'update', 'delete', 'execute'] as const

/** 前端使用到的权限编码常量（均可在上述契约 RBAC 说明中溯源） */
export const PERMISSIONS = {
  sceneRead: 'scene:read',
  sceneCreate: 'scene:create',
  sceneUpdate: 'scene:update',
  sceneDelete: 'scene:delete',
  sceneExecute: 'scene:execute',
  dataRead: 'data:read',
  analyticsRead: 'analytics:read',
  analyticsExecute: 'analytics:execute',
  otaRead: 'ota:read',
  otaCreate: 'ota:create',
  otaExecute: 'ota:execute',
  remoteRead: 'remote:read',
  remoteCreate: 'remote:create',
  remoteExecute: 'remote:execute',
} as const

/** 角色编码（api-gateway.yaml UserProfile.roles 描述：admin / operator / analyst / viewer） */
export const ADMIN_ROLE = 'admin'

/** 角色展示名 */
export const ROLE_LABELS: Record<string, string> = {
  admin: '系统管理员',
  operator: '操控员',
  analyst: '分析师',
  viewer: '只读用户',
}
