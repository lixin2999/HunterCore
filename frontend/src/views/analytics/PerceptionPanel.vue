<script setup lang="ts">
/**
 * 感知评估（GET /api/v1/analytics/perception/eval）
 *
 * 指标口径（设计文档 6.2 节）：map_3d / map_bev / iou / recall / precision /
 * mean_localization_error_m；by_object_type 为按对象类型的细分（vehicle/pedestrian/other）。
 */
import { computed, onMounted, reactive, ref } from 'vue'
import { ElMessage } from 'element-plus'
import type { EChartsOption } from 'echarts'

import { fetchPerceptionEval } from '@/api/analytics'
import EchartBase from '@/components/charts/EchartBase.vue'
import DataSourceNotice from '@/components/common/DataSourceNotice.vue'
import StatCard from '@/components/common/StatCard.vue'
import { OBJECT_TYPE_LABELS, PERCEPTION_METRIC_LABELS } from '@/constants'
import { useVehicleStore } from '@/stores/vehicle'
import type { EvalQuery, PerceptionEvalData, PerceptionMetrics } from '@/types/analytics'
import { HunterApiError } from '@/utils/error-code'
import { formatNumber, formatTime } from '@/utils/format'

const vehicleStore = useVehicleStore()

const query = reactive<EvalQuery>({ vehicle_id: undefined })
const timeRange = ref<[number, number] | null>(null)
const loading = ref(false)
const result = ref<PerceptionEvalData | null>(null)

/** 指标行（键顺序与 PERCEPTION_METRIC_LABELS 对齐） */
const metricRows = computed(() =>
  (Object.keys(PERCEPTION_METRIC_LABELS) as Array<keyof PerceptionMetrics>).map((key) => ({
    key,
    label: PERCEPTION_METRIC_LABELS[key as string],
    value: result.value?.metrics[key],
  })),
)

/** 按对象类型细分（by_object_type 为动态结构，仅取数值型条目） */
const byObjectTypeOption = computed<EChartsOption>(() => {
  const data = result.value?.by_object_type ?? {}
  const categories: string[] = []
  const values: number[] = []
  for (const [type, metrics] of Object.entries(data)) {
    for (const [metric, metricValue] of Object.entries(metrics) as Array<[string, number]>) {
      categories.push(`${OBJECT_TYPE_LABELS[type] ?? type} · ${metric}`)
      values.push(metricValue)
    }
  }
  return {
    tooltip: { trigger: 'axis' },
    grid: { left: 170, right: 24, top: 24, bottom: 24 },
    xAxis: { type: 'value' },
    yAxis: { type: 'category', data: categories },
    series: [{ type: 'bar', barWidth: 14, data: values }],
  }
})

async function load(): Promise<void> {
  loading.value = true
  try {
    const [start, end] = timeRange.value ?? []
    result.value = await fetchPerceptionEval({ ...query, start_time: start, end_time: end })
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '感知评估数据加载失败')
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
      <el-col :span="4">
        <StatCard title="3D 地图精度" :value="formatNumber(result?.metrics.map_3d ?? null, 4)" hint="map_3d" />
      </el-col>
      <el-col :span="4">
        <StatCard title="BEV 地图精度" :value="formatNumber(result?.metrics.map_bev ?? null, 4)" hint="map_bev" />
      </el-col>
      <el-col :span="4">
        <StatCard title="IoU" :value="formatNumber(result?.metrics.iou ?? null, 4)" tone="success" hint="交并比" />
      </el-col>
      <el-col :span="4">
        <StatCard title="召回率" :value="formatNumber(result?.metrics.recall ?? null, 4)" tone="success" hint="recall" />
      </el-col>
      <el-col :span="4">
        <StatCard title="准确率" :value="formatNumber(result?.metrics.precision ?? null, 4)" tone="success" hint="precision" />
      </el-col>
      <el-col :span="4">
        <StatCard
          title="平均定位误差"
          :value="formatNumber(result?.metrics.mean_localization_error_m ?? null, 3)"
          unit="m"
          tone="warning"
         
          hint="mean_localization_error_m"
        />
      </el-col>
    </el-row>

    <el-row :gutter="12" class="panel__body">
      <el-col :span="10">
        <el-table :data="metricRows" size="small" empty-text="暂无指标">
          <el-table-column prop="label" label="指标" min-width="150" />
          <el-table-column label="数值" width="120">
            <template #default="{ row }">{{ formatNumber(row.value, 4) }}</template>
          </el-table-column>
        </el-table>
        <p class="panel__meta">
          样本数 {{ result?.sample_count ?? 0 }} ·
          窗口 {{ formatTime(result?.window.start_time ?? null) }} ~ {{ formatTime(result?.window.end_time ?? null) }}
        </p>
      </el-col>
      <el-col :span="14">
        <EchartBase :option="byObjectTypeOption" :loading="loading" height="300px" />
      </el-col>
    </el-row>
  </div>
</template>

<style scoped>
.panel__body {
  margin-top: 12px;
}

.panel__meta {
  margin: 8px 0 0;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}
</style>
