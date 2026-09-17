<script setup lang="ts">
/**
 * OTA 管理布局（版本仓库 / 升级任务）
 * 说明：仅承载子路由与页签导航，业务逻辑在各子页面。
 */
import { computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'

const route = useRoute()
const router = useRouter()

const TABS = [
  { name: 'ota-versions', label: '版本仓库' },
  { name: 'ota-tasks', label: '升级任务（灰度发布）' },
] as const

/** 任务详情页归属「升级任务」页签 */
const activeTab = computed<string>(() => {
  const current = String(route.name ?? '')
  return current.startsWith('ota-task') ? 'ota-tasks' : current
})

function handleTabChange(name: string): void {
  void router.push({ name })
}
</script>

<template>
  <el-card shadow="never">
    <el-tabs :model-value="activeTab" @tab-change="handleTabChange">
      <el-tab-pane v-for="tab in TABS" :key="tab.name" :label="tab.label" :name="tab.name" />
    </el-tabs>
    <router-view />
  </el-card>
</template>
