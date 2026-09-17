<script setup lang="ts">
/**
 * 场景覆盖率（GET /api/v1/analytics/scene/coverage）
 *
 * 契约要点：
 * - heatmap 单元结构 {x, y, count}（网格中心坐标，单位 m），可能被服务端按 max_cells 截断
 *   → heatmap_truncated 为 true 时提示用户缩小时间范围；
 * - uncovered 为未覆盖区域/场景类型（kind: region / scene_type），用于指导场景库补齐。
 */
import { computed, onMounted, reactive, ref } from 'vue'
import { ElMessage } from 'element-plus'
import type { EChartsOption } from 'echarts'

import { fetchSceneCoverage } from '@/api/analytics'
import EchartBase from '@/components/charts/EchartBase.vue'
import DataSourceNotice from '@/components/common/DataSourceNotice.vue'
import StatCard from '@/components/common/StatCard.vue'
import { SCENE_TYPE_LABELS } from '@/constants'
import { useVehicleStore } from '@/stores/vehicle'
import type { SceneCoverageData, SceneCoverageQuery } from '@/types/analytics'
import { HunterApiError } from '@/utils/error-code'
import { formatPercent, formatTime } from '@/utils/format'

const vehicleStore = useVehicleStore()

const query = reactive<SceneCoverageQuery>({ vehicle_id: undefined, grid_size_m: 5 })
const timeRange = ref<[number, number] | null>(null)
const loading = ref(false)
const result = ref<SceneCoverageData | null>(null)

/** 热力图（散点 + 视觉映射：位置为网格中心坐标，颜色深浅为采样计数） */
const heatmapOption = computed<EChartsOption>(() => {
  const heatmap = result.value?.heatmap ?? []
  return {
    tooltip: {
      trigger: 'item',
      formatter: (params: unknown) => {
        const item = params as { data: [number, number, number] }
        return `x=${item.data[0]} m, y=${item.data[1]} m<br/>采样 ${item.data[2]} 次`
      },
    },
    grid: { left: 48, right: 24, top: 24, bottom: 48 },
    xAxis: { type: 'value', name: 'x (m)' },
    yAxis: { type: 'value', name: 'y (m)' },
    visualMap: {
      min: 0,
      max: Math.max(1, ...heatmap.map((cell) => cell.count)),
      dimension: 2,
      orient: 'vertical',
      right: 0,
      top: 'center',
      text: ['高', '低'],
    },
    series: [
      {
        type: 'scatter',
        symbolSize: 10,
        data: heatmap.map((cell) => [cell.x, cell.y, cell.count]),
      },
    ],
  }
})

/** 未覆盖项（region / scene_type 两类） */
const uncoveredRows = computed(() =>
  (result.value?.uncovered ?? []).map((item) => ({
    kind: item.kind,
    kindLabel: item.kind === 'region' ? '区域' : '场景类型',
    description: item.kind === 'scene_type' ? (SCENE_TYPE_LABELS[item.description] ?? item.description) : item.description,
    reason: item.reason ?? '-',
    sampleCount: item.sample_count ?? null,
  })),
)

async function load(): Promise<void> {
  loading.value = true
  try {
    const [start, end] = timeRange.value ?? []
    result.value = await fetchSceneCoverage({ ...query, start_time: start, end_time: end })
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '覆盖率数据加载失败')
  } finally {
    loading.value = false
  }
}

onMounted(async () => {
  await vehicleStore.fetchVehicles()
  await load()
})
</script>

<template>
  <div v-loading="loading">
    <el-form :inline="true">
      <el-form-item label="车辆">
        <el-select v-model="query.vehicle_id" clearable filterable placeholder="全部车辆" style="width: 180px">
          <el-option
            v-for="vehicle in vehicleStore.vehicles"
            :key="vehicle.vehicle_id"
            :label="vehicle.vehicle_name || vehicle.vehicle_id"
            :value="vehicle.vehicle_id"
          />
        </el-select>
      </el-form-item>
      <el-form-item label="网格粒度 (m)">
        <el-input-number v-model="query.grid_size_m" :min="1" :max="50" :step="1" />
      </el-form-item>
      <el-form-item label="时间范围">
        <el-date-picker
          v-model="timeRange"
          type="datetimerange"
          value-format="x"
          start-placeholder="开始时间"
          end-placeholder="结束时间"
          unlink-panels
        />
      </el-form-item>
      <el-form-item>
        <el-button type="primary" :loading="loading" @click="load">查询</el-button>
      </el-form-item>
    </el-form>

    <DataSourceNotice
      :data-source="result?.data_source"
      :job-name="result?.job_name"
      :updated-at="result?.updated_at"
    />

    <el-row :gutter="12">
      <el-col :span="6">
        <StatCard
          title="覆盖率"
          :value="formatPercent(result?.coverage_ratio ?? null)"
          :tone="(result?.coverage_ratio ?? 0) >= 0.8 ? 'success' : 'warning'"
         
          hint="covered_cells / total_cells"
        />
      </el-col>
      <el-col :span="6">
        <StatCard title="已覆盖网格" :value="result?.covered_cells ?? 0" hint="网格数" />
      </el-col>
      <el-col :span="6">
        <StatCard title="总网格数" :value="result?.total_cells ?? 0" tone="info" hint="按网格粒度统计" />
      </el-col>
      <el-col :span="6">
        <StatCard
          title="评估窗口"
          :value="formatTime(result?.window.start_time ?? null, 'MM-DD HH:mm')"
          tone="info"
         
          :hint="`至 ${formatTime(result?.window.end_time ?? null, 'MM-DD HH:mm')}`"
        />
      </el-col>
    </el-row>

    <el-alert
      v-if="result?.heatmap_truncated"
      class="notice"
      type="warning"
      :closable="false"
      show-icon
      title="热力图单元已被服务端按 max_cells 截断，请缩小时间范围或放大网格粒度后重试"
    />

    <el-row :gutter="12" class="panel__body">
      <el-col :span="14">
        <el-card shadow="never" header="覆盖率热力图（网格中心坐标 / 采样计数）">
          <EchartBase :option="heatmapOption" :loading="loading" height="360px" />
        </el-card>
      </el-col>
      <el-col :span="10">
        <el-card shadow="never" header="未覆盖区域 / 场景类型">
          <el-table :data="uncoveredRows" size="small" empty-text="暂无未覆盖项">
            <el-table-column label="类型" width="90">
              <template #default="{ row }">
                <el-tag size="small" :type="row.kind === 'region' ? 'warning' : 'info'">{{ row.kindLabel }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="description" label="描述" min-width="140" show-overflow-tooltip />
            <el-table-column prop="reason" label="原因" min-width="140" show-overflow-tooltip />
            <el-table-column label="采样数" width="90">
              <template #default="{ row }">{{ row.sampleCount ?? '-' }}</template>
            </el-table-column>
          </el-table>
        </el-card>
      </el-col>
    </el-row>
  </div>
</template>

<style scoped>
.panel__body {
  margin-top: 12px;
}

.notice {
  margin-top: 12px;
}
</style>
