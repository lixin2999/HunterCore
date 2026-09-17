<script setup lang="ts">
/**
 * 数据分析（data-analytics 模块）
 *
 * 契约（data-analytics.yaml）：
 * - GET /api/v1/analytics/dashboard（看板见 DashboardView）
 * - GET /api/v1/analytics/perception/eval
 * - GET /api/v1/analytics/control/eval
 * - GET /api/v1/analytics/scene/coverage
 * - GET /api/v1/analytics/corner-cases
 * - GET /api/v1/analytics/reports、POST /api/v1/analytics/reports/generate
 * 说明：各维度响应带 available 降级标记（Flink/Spark 结果不可用时展示降级提示）。
 */
import { ref } from 'vue'

import ControlPanel from './ControlPanel.vue'
import CornerCasePanel from './CornerCasePanel.vue'
import CoveragePanel from './CoveragePanel.vue'
import PerceptionPanel from './PerceptionPanel.vue'
import ReportPanel from './ReportPanel.vue'

const activeTab = ref<'perception' | 'control' | 'coverage' | 'corner-case' | 'report'>('perception')
</script>

<template>
  <el-tabs v-model="activeTab" type="border-card" class="analytics">
    <el-tab-pane label="感知评估" name="perception">
      <PerceptionPanel />
    </el-tab-pane>
    <el-tab-pane label="控制评估" name="control">
      <ControlPanel />
    </el-tab-pane>
    <el-tab-pane label="场景覆盖率" name="coverage">
      <CoveragePanel />
    </el-tab-pane>
    <el-tab-pane label="Corner Case 挖掘" name="corner-case">
      <CornerCasePanel />
    </el-tab-pane>
    <el-tab-pane label="分析报告" name="report">
      <ReportPanel />
    </el-tab-pane>
  </el-tabs>
</template>

<style scoped>
.analytics {
  background: #fff;
}
</style>
