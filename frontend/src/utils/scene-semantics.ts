/**
 * 场景语义自检工具（前端）：必需角色 vs 当前 config.actors
 *
 * 判据与后端 ``app/scene_engine/semantics.py::audit_roles`` 对齐：
 * 1. 先按 ``actor_id`` 命名回溯角色（``{role}`` / ``{role}-N`` / ``{role}_N``）；
 * 2. 未命名的参与者仅在「(类型, 行为) 组合在全契约内唯一」时才兜底绑定到该角色，
 *    避免把第二个前车误判成行人角色。
 *
 * 用途：场景详情「语义自检」区块、编辑器「添加参与者」的角色建议。
 */
import type { SceneRoleSemantics, SceneSemantics } from '@/types/engine'
import type { Actor, ActorType, SceneConfig } from '@/types/scene'

export type RoleAuditStatus = 'ok' | 'missing' | 'underflow'

/** 单角色覆盖审计行 */
export interface RoleAuditRow {
  role: string
  type: string
  behavior: string
  required: boolean
  minCount: number
  actual: number
  actorIds: string[]
  status: RoleAuditStatus
}

export interface RoleAuditResult {
  rows: RoleAuditRow[]
  /** 无法回溯到任何声明角色的参与者（可能是背景车等扩展） */
  unmatched: Actor[]
}

function roleOfActorId(actorId: string, roles: SceneRoleSemantics[]): string | null {
  let best: string | null = null
  for (const role of roles) {
    if (actorId === role.role) {
      return role.role
    }
    for (const separator of ['-', '_']) {
      if (
        actorId.startsWith(role.role + separator) &&
        (best === null || role.role.length > best.length)
      ) {
        best = role.role
      }
    }
  }
  return best
}

function pairKey(type: string, behavior: string): string {
  return `${type}|${behavior}`
}

/** 逐角色审计参与者覆盖（语义缺失/未传 config 时返回空行）。 */
export function auditSceneRoles(
  config: SceneConfig | null | undefined,
  semantics: SceneSemantics | null | undefined,
): RoleAuditResult {
  if (!config || !semantics) {
    return { rows: [], unmatched: [] }
  }
  const groups = new Map<string, Actor[]>(semantics.roles.map((role) => [role.role, []]))
  const unmatched: Actor[] = []
  for (const actor of config.actors ?? []) {
    const role = roleOfActorId(actor.actor_id ?? '', semantics.roles)
    if (role === null) {
      unmatched.push(actor)
    } else {
      groups.get(role)?.push(actor)
    }
  }

  const pairCounts = new Map<string, number>()
  for (const role of semantics.roles) {
    const key = pairKey(role.type, role.behavior_type)
    pairCounts.set(key, (pairCounts.get(key) ?? 0) + 1)
  }
  for (const role of semantics.roles) {
    const bucket = groups.get(role.role) ?? []
    if (bucket.length || pairCounts.get(pairKey(role.type, role.behavior_type)) !== 1) {
      continue
    }
    const limit = Math.max(role.min_count, 1)
    for (const actor of [...unmatched]) {
      if (bucket.length >= limit) {
        break
      }
      if (actor.type === role.type && actor.behavior?.behavior_type === role.behavior_type) {
        bucket.push(actor)
        unmatched.splice(unmatched.indexOf(actor), 1)
      }
    }
  }

  const rows = semantics.roles.map<RoleAuditRow>((role) => {
    const actors = groups.get(role.role) ?? []
    const threshold = Math.max(role.min_count, role.required ? 1 : 0)
    let status: RoleAuditStatus = 'ok'
    if (actors.length < threshold) {
      status = actors.length === 0 ? 'missing' : 'underflow'
    }
    return {
      role: role.role,
      type: role.type,
      behavior: role.behavior_type,
      required: role.required,
      minCount: role.min_count,
      actual: actors.length,
      actorIds: actors.map((actor) => actor.actor_id),
      status,
    }
  })
  return { rows, unmatched }
}

/** 必需角色中未覆盖（缺失/实例不足）的行。 */
export function missingRequiredRoles(rows: RoleAuditRow[]): RoleAuditRow[] {
  return rows.filter((row) => row.required && row.status !== 'ok')
}

/**
 * 按语义角色生成一条默认参与者（相对自车偏移的占位生成点，仿真端会再换算）。
 * 序号用于 ``{role}-N`` 命名，保证后端可按 actor_id 回溯角色。
 */
export function roleActorSuggestion(
  role: Pick<SceneRoleSemantics, 'role' | 'type' | 'behavior_type'>,
  ordinal: number,
): Actor {
  return {
    actor_id: `${role.role}-${Math.max(1, ordinal)}`,
    type: role.type as ActorType,
    spawn_point: { x: 10, y: 0, z: 0, yaw: 0 },
    behavior: { behavior_type: role.behavior_type, parameters: {} },
  }
}

/** 计算某角色的下一个可用序号（``{role}-1``、``{role}-2``…）。 */
export function nextRoleOrdinal(actors: Actor[], role: string): number {
  const used = new Set(actors.map((actor) => actor.actor_id))
  let ordinal = 1
  while (used.has(`${role}-${ordinal}`) || used.has(`${role}_${ordinal}`)) {
    ordinal += 1
  }
  return ordinal
}
