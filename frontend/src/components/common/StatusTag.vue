<script setup lang="ts">
/**
 * 受控词表标签（车辆状态 / 事件等级 / 任务状态 / 会话状态等）
 * 用法：`<StatusTag kind="vehicle" :value="row.status" />`
 * 词表映射集中在 constants/enums.ts 等文件，禁止在组件中硬编码取值域。
 */
import { computed } from 'vue'

import {
  EVENT_LEVEL_LABELS,
  OTA_BATCH_STATUS_LABELS,
  OTA_BATCH_STATUS_TAG_TYPES,
  OTA_TASK_STATUS_LABELS,
  OTA_TASK_STATUS_TAG_TYPES,
  OTA_UPGRADE_STATUS_LABELS,
  OTA_UPGRADE_STATUS_TAG_TYPES,
  OTA_VERSION_STATUS_LABELS,
  OTA_VERSION_STATUS_TAG_TYPES,
  REPORT_STATUS_LABELS,
  REPORT_STATUS_TAG_TYPES,
  SCENE_STATUS_LABELS,
  SCENE_STATUS_TAG_TYPES,
  SESSION_STATUS_LABELS,
  SESSION_STATUS_TAG_TYPES,
  SIMULATION_STATUS_LABELS,
  SIMULATION_STATUS_TAG_TYPES,
  VEHICLE_STATUS_LABELS,
  VEHICLE_STATUS_TAG_TYPES,
} from '@/constants'

/** 支持的词表种类 */
export type StatusKind =
  | 'vehicle'
  | 'scene'
  | 'simulation'
  | 'ota-version'
  | 'ota-task'
  | 'ota-batch'
  | 'ota-upgrade'
  | 'report'
  | 'session'
  | 'event-level'

const props = withDefaults(
  defineProps<{
    kind: StatusKind
    value?: string | null
    size?: 'large' | 'default' | 'small'
  }>(),
  { value: '', size: 'small' },
)

interface LabelMap {
  label: string
  type: string
}

const MAPS: Record<StatusKind, { labels: Record<string, string>; types: Record<string, string> }> = {
  vehicle: { labels: VEHICLE_STATUS_LABELS, types: VEHICLE_STATUS_TAG_TYPES },
  scene: { labels: SCENE_STATUS_LABELS, types: SCENE_STATUS_TAG_TYPES },
  simulation: { labels: SIMULATION_STATUS_LABELS, types: SIMULATION_STATUS_TAG_TYPES },
  'ota-version': { labels: OTA_VERSION_STATUS_LABELS, types: OTA_VERSION_STATUS_TAG_TYPES },
  'ota-task': { labels: OTA_TASK_STATUS_LABELS, types: OTA_TASK_STATUS_TAG_TYPES },
  'ota-batch': { labels: OTA_BATCH_STATUS_LABELS, types: OTA_BATCH_STATUS_TAG_TYPES },
  'ota-upgrade': { labels: OTA_UPGRADE_STATUS_LABELS, types: OTA_UPGRADE_STATUS_TAG_TYPES },
  report: { labels: REPORT_STATUS_LABELS, types: REPORT_STATUS_TAG_TYPES },
  session: { labels: SESSION_STATUS_LABELS, types: SESSION_STATUS_TAG_TYPES },
  'event-level': { labels: EVENT_LEVEL_LABELS, types: { info: 'info', warning: 'warning', critical: 'danger' } },
}

const resolved = computed<LabelMap>(() => {
  const key = props.value ?? ''
  const map = MAPS[props.kind]
  return { label: map.labels[key] ?? key ?? '-', type: map.types[key] ?? 'info' }
})
</script>

<template>
  <el-tag :type="resolved.type as never" :size="size" effect="light">{{ resolved.label }}</el-tag>
</template>
