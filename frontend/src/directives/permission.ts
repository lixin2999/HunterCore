/**
 * v-permission 权限指令
 *
 * 用法：
 * - `v-permission="'ota:execute'"` 单个权限
 * - `v-permission="['ota:read', 'ota:execute']"` 多权限（默认满足其一）
 * - `v-permission.every="['a', 'b']"` 需全部满足
 *
 * 说明：权限编码来自服务端（GET /api/v1/user/me → permissions，形如 `scene:read`），
 * 前端仅做成员判断，不自行发明编码（见 constants/permissions.ts）。
 * 该指令只控制展示层，最终鉴权由网关与服务端 RBAC 强制执行。
 */
import type { Directive, DirectiveBinding } from 'vue'

import { PERMISSIONS } from '@/constants/permissions'
import { useUserStore } from '@/stores/user'

type PermissionValue = string | string[]

function resolveDenied(binding: DirectiveBinding<PermissionValue>): boolean {
  const userStore = useUserStore()
  const required = binding.value
  const mode = binding.modifiers.every ? 'every' : 'some'
  return !userStore.hasPermission(required || PERMISSIONS.dataRead, mode)
}

/** 移除元素（保留占位注释以便权限变化时无法恢复 —— 简化实现：直接删除） */
function applyDirective(el: HTMLElement, binding: DirectiveBinding<PermissionValue>): void {
  if (resolveDenied(binding)) {
    el.parentNode?.removeChild(el)
  }
}

export const permissionDirective: Directive<HTMLElement, PermissionValue> = {
  mounted(el, binding) {
    applyDirective(el, binding)
  },
  updated(el, binding) {
    // 权限变化（如刷新 profile）时重新判定：已移除的元素无法恢复，故仅二次校验未移除的元素
    if (el.parentNode && resolveDenied(binding)) {
      el.parentNode.removeChild(el)
    }
  },
}
