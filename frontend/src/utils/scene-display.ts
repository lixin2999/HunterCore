/**
 * 场景展示辅助（场景库列表 / 详情共用）
 *
 * 类型：
 * - 受控字典 48 类的中文标签来自 `constants/sceneTaxonomy.ts`（由
 *   `scripts/gen_scene_taxonomy_ts.py` 从 `contracts/scene-taxonomy.yaml` 生成，禁止手改产物）。
 * - 字典外取值（历史脏数据，或旧前端产物未重跑 codegen/未重建 web-portal）统一显示
 *   “未知类型”，不直接回退英文 value，保证界面统一中文。
 *
 * 创建者：
 * - `scripts/batch_add_scenes.py` 生成器固定写入受控标签 `src:synthetic`（运维手册 §3.9），
 *   据此判定为脚本生成并显示“批量创建”。
 * - 手动创建由当前登录账号发起（creator = 网关注入的 X-User-Id），显示账号名称
 *   （real_name 优先，回退 username）。
 * - 其余（其他账号 / 实车回放提取的系统账号）显示“其他账号”，悬停可查看 creator UUID。
 */
import { SCENE_TYPE_LABELS } from '@/constants'

/** 批量脚本生成判定依据（batch_add_scenes.py 固定标签，运维手册 §3.9 受控前缀 src:） */
export const BATCH_SCENE_TAG = 'src:synthetic'
export const BATCH_CREATE_LABEL = '批量创建'
export const UNKNOWN_SCENE_TYPE_LABEL = '未知类型'
export const OTHER_CREATOR_LABEL = '其他账号'

/** 是否脚本批量生成（tags 含 src:synthetic） */
export function isBatchCreatedScene(tags: readonly string[] | null | undefined): boolean {
  return Array.isArray(tags) && tags.includes(BATCH_SCENE_TAG)
}

/** 场景类型中文名（字典外不回退英文 value） */
export function sceneTypeLabel(value: string | null | undefined): string {
  if (!value) {
    return UNKNOWN_SCENE_TYPE_LABEL
  }
  return SCENE_TYPE_LABELS[value] ?? UNKNOWN_SCENE_TYPE_LABEL
}

/**
 * 创建者展示名
 * @param scene 至少含 creator 与 tags
 * @param currentUserId 当前登录用户 user_id（未登录为空）
 * @param currentUserName 当前登录用户展示名（real_name 回退 username）
 */
export function sceneCreatorLabel(
  scene: { creator: string; tags?: readonly string[] | null },
  currentUserId: string | null | undefined,
  currentUserName: string | null | undefined,
): string {
  if (isBatchCreatedScene(scene.tags)) {
    return BATCH_CREATE_LABEL
  }
  if (currentUserId && scene.creator === currentUserId && currentUserName) {
    return currentUserName
  }
  return OTHER_CREATOR_LABEL
}
