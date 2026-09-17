<script setup lang="ts">
/**
 * 控制评估（GET /api/v1/analytics/control/eval）
 *
 * 契约难点：`metrics` 与 `thresholds` 均为**动态键**（指标名 → 数值 / 阈值检查项），
 * 因此前端必须按响应数据渲染，不得硬编码指标清单（CONTROL_METRIC_LABELS 仅用于展示映射）。
 * 阈值为服务端下发（设计文档 6.3 节口径），前端不得自行放宽比较符或阈值。
 */
import { computed, onMounted, reactive, ref } from 'vue'
import { ElMessage } from 'element-plus'
import type { EChartsOption } from 'echarts'

import { fetchControlEval } from '@/api/analytics'
import EchartBase from '@/components/charts/EchartBase.vue'
import DataSourceNotice from '@/components/common/DataSourceNotice.vue'
import StatCard from '@/components/common/StatCard.vue'
import { COMPARATOR_LABELS, CONTROL_METRIC_LABELS } from '@/constants'
import { useVehicleStore } from '@/stores/vehicle'
import type { ControlEvalData, ControlMetrics, EvalQuery } from '@/types/analytics'
import { HunterApiError } from '@/utils/error-code'
import { formatNumber, formatTime } from '@/utils/format'

const vehicleStore = useVehicleStore()

const query = reactive<EvalQuery>({ vehicle_id: undefined })
const timeRange = ref<[number, number] | null>(null)
const loading = ref(false)
const result = ref<ControlEvalData | null>(null)

interface ThresholdRow {
  name: string
  label: string
  value: number | null
  threshold: number | null
  comparator: string
  unit: string
  pass: boolean | null
}

/** 阈值检查行（键固定 4 项，来源：ControlMetrics/ControlThresholdSet 契约） */
const thresholdRows = computed<ThresholdRow[]>(() =>
  (Object.keys(CONTROL_METRIC_LABELS) as Array<keyof ControlMetrics>).map((key) => {
    const check = result.value?.thresholds?.[key]
    return {
      name: key,
      label: CONTROL_METRIC_LABELS[key] ?? key,
      value: check?.value ?? result.value?.metrics?.[key] ?? null,
      threshold: check?.threshold ?? null,
      comparator: check ? (COMPARATOR_LABELS[check.comparator] ?? check.comparator) : '-',
      unit: check?.unit ?? '',
      pass: check ? check.pass : null,
    }
  }),
)

/** 实测值 vs 阈值对比图（仅展示带阈值的指标） */
const compareOption = computed<EChartsOption>(() => {
  const rows = thresholdRows.value.filter((row) => row.threshold !== null)
  return {
    tooltip: { trigger: 'axis' },
    legend: { data: ['实测值', '阈值'] },
    grid: { left: 60, right: 24, top: 40, bottom: 60 },
    xAxis: { type: 'category', data: rows.map((row) => row.label), axisLabel: { rotate: 20 } },
    yAxis: { type: 'value' },
    series: [
      { name: '实测值', type: 'bar', data: rows.map((row) => row.value ?? 0), itemStyle: { color: '#409eff' } },
      { name: '阈值', type: 'bar', data: rows.map((row) => row.threshold ?? 0), itemStyle: { color: '#e6a23c' } },
    ],
  }
})

const failedCount = computed(() => thresholdRows.value.filter((row) => row.pass === false).length)

async function load(): Promise<void> {
  loading.value = true
  try {
    const [start, end] = timeRange.value ?? []
    result.value = await fetchControlEval({ ...query, start_time: start, end_time: end })
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '控制评估数据加载失败')
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
        <StatCard title="样本数" :value="result?.sample_count ?? 0" hint="采样窗口内控制数据点" />
      </el-col>
      <el-col :span="6">
        <StatCard
          title="阈值未通过项"
          :value="failedCount"
          :tone="failedCount > 0 ? 'danger' : 'success'"
         
          hint="对照 6.3 节阈值口径"
        />
      </el-col>
      <el-col :span="6">
        <StatCard
          title="整体判定"
          :value="result?.overall_pass ? '通过' : '未通过'"
          :tone="result?.overall_pass ? 'success' : 'danger'"
         
          hint="overall_pass"
        />
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

    <el-row :gutter="12" class="panel__body">
      <el-col :span="12">
        <el-table :data="thresholdRows" size="small" empty-text="暂无阈值数据">
          <el-table-column prop="label" label="指标" min-width="170" />
          <el-table-column label="实测值" width="110">
            <template #default="{ row }">{{ formatNumber(row.value, 4) }}</template>
          </el-table-column>
          <el-table-column label="比较" width="70">
            <template #default="{ row }">{{ row.comparator }}</template>
          </el-table-column>
          <el-table-column label="阈值" width="110">
            <template #default="{ row }">{{ row.threshold === null ? '-' : formatNumber(row.threshold, 4) }}</template>
          </el-table-column>
          <el-table-column label="单位" width="80" prop="unit" />
          <el-table-column label="判定" width="90">
            <template #default="{ row }">
              <el-tag v-if="row.pass === null" type="info" size="small">无阈值</el-tag>
              <el-tag v-else :type="row.pass ? 'success' : 'danger'" size="small">{{ row.pass ? '通过' : '未通过' }}</el-tag>
            </template>
          </el-table-column>
        </el-table>
      </el-col>
      <el-col :span="12">
        <EchartBase :option="compareOption" :loading="loading" height="320px" />
      </el-col>
    </el-row>
  </div>
</template>

<style scoped>
.panel__body {
  margin-top: 12px;
}
</style>
