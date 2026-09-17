<script setup lang="ts">
/**
 * 远程操控布局（操控台 / 操控记录）
 * 约束：视频媒体流不经 api-gateway（WebRTC/SRTP），本布局仅做路由与页签承载。
 */
import { computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'

const route = useRoute()
const router = useRouter()

const TABS = [
  { name: 'remote-console', label: '操控台' },
  { name: 'remote-history', label: '操控记录' },
] as const

const activeTab = computed<string>(() => String(route.name ?? ''))

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
