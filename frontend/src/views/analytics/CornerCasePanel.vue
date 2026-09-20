<script setup lang="ts">
/**
 * Corner Case 挖掘结果（GET /api/v1/analytics/corner-cases）
 *
 * 契约要点：
 * - 类别 5 类受控词表（kinematic/perception/planning/interaction/environment）；
 * - 算法为 isolation_forest（无监督异常检测）/ dbscan（密度聚类），可组合；
 * - 响应内 mining 块给出算法、最近运行时间与特征数；category_counts 为类别分布。
 */
import { computed, onMounted, reactive, ref } from 'vue'
import { ElMessage } from 'element-plus'
import type { EChartsOption } from 'echarts'

import { fetchCornerCases } from '@/api/analytics'
import EchartBase from '@/components/charts/EchartBase.vue'
import StatCard from '@/components/common/StatCard.vue'
import {
  CORNER_CASE_ALGORITHM_LABELS,
  CORNER_CASE_CATEGORY_LABELS,
  DEFAULT_PAGE_SIZE,
  EVENT_TYPE_LABELS,
} from '@/constants'
import { useVehicleStore } from '@/stores/vehicle'
import type { CornerCaseItem, CornerCaseListData, CornerCaseQuery } from '@/types/analytics'
import { HunterApiError } from '@/utils/error-code'
import { formatNumber, formatTime } from '@/utils/format'

const vehicleStore = useVehicleStore()

const query = reactive<CornerCaseQuery>({
  category: undefined,
  vehicle_id: undefined,
  algorithm: undefined,
  min_anomaly_score: undefined,
  page: 1,
  page_size: DEFAULT_PAGE_SIZE,
})
const loading = ref(false)
const result = ref<CornerCaseListData | null>(null)

const categoryOptions = Object.entries(CORNER_CASE_CATEGORY_LABELS).map(([value, label]) => ({ value, label }))
const algorithmOptions = Object.entries(CORNER_CASE_ALGORITHM_LABELS).map(([value, label]) => ({ value, label }))

/** 类别分布图（category_counts 为动态键） */
const categoryOption = computed<EChartsOption>(() => {
  const counts = result.value?.category_counts ?? {}
  const entries = Object.entries(counts)
  return {
    tooltip: { trigger: 'axis' },
    grid: { left: 90, right: 24, top: 24, bottom: 24 },
    xAxis: { type: 'value' },
    yAxis: {
      type: 'category',
      data: entries.map(([category]) => CORNER_CASE_CATEGORY_LABELS[category] ?? category),
    },
    series: [{ type: 'bar', barWidth: 16, data: entries.map(([, count]) => count), itemStyle: { color: '#f56c6c' } }],
  }
})

async function load(): Promise<void> {
  loading.value = true
  try {
    result.value = await fetchCornerCases({ ...query })
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : 'Corner Case 数据加载失败')
  } finally {
    loading.value = false
  }
}

/** 触发事件类型展示（trigger_event_type 采用 19 种事件词表） */
function eventTypeLabel(value: string | null | undefined): string {
  if (!value) {
    return '-'
  }
  return EVENT_TYPE_LABELS[value] ?? value
}

/** 关联场景跳转（若挖掘结果已关联场景） */
function goScene(row: CornerCaseItem): void {
  if (row.scene_id) {
    window.open(`/scenes/${row.scene_id}`, '_blank')
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
      <el-form-item label="类别">
        <el-select v-model="query.category" clearable placeholder="全部类别" style="width: 150px">
          <el-option v-for="option in categoryOptions" :key="option.value" :label="option.label" :value="option.value" />
        </el-select>
      </el-form-item>
      <el-form-item label="算法">
        <el-select v-model="query.algorithm" clearable placeholder="全部算法" style="width: 160px">
          <el-option v-for="option in algorithmOptions" :key="option.value" :label="option.label" :value="option.value" />
        </el-select>
      </el-form-item>
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
      <el-form-item label="最小异常分">
        <el-input-number v-model="query.min_anomaly_score" :min="0" :max="1" :step="0.05" :controls="false" style="width: 110px" />
      </el-form-item>
      <el-form-item>
        <el-button type="primary" :loading="loading" @click="load">查询</el-button>
      </el-form-item>
    </el-form>

    <el-row :gutter="12">
      <el-col :span="6">
        <StatCard title="挖掘结果总数" :value="result?.total ?? 0" hint="当前过滤条件下命中数" />
      </el-col>
      <el-col :span="6">
        <StatCard
          title="最近挖掘时间"
          :value="formatTime(result?.mining.last_run_at ?? null, 'MM-DD HH:mm')"
          tone="info"
          hint="Spark 离线作业运行时间"
        />
      </el-col>
      <el-col :span="6">
        <StatCard
          title="特征维度"
          :value="result?.mining.feature_count ?? 0"
          tone="info"
          hint="隔离森林/DBSCAN 输入特征数"
        />
      </el-col>
      <el-col :span="6">
        <StatCard
          title="挖掘算法"
          :value="(result?.mining.algorithm ?? []).map((item) => CORNER_CASE_ALGORITHM_LABELS[item] ?? item).join(' + ') || '-'"
          tone="primary"
          :hint="result?.mining.job_name ?? '作业名未知'"
        />
      </el-col>
    </el-row>

    <el-row :gutter="12" class="panel__body">
      <el-col :span="8">
        <el-card shadow="never" header="类别分布">
          <EchartBase :option="categoryOption" :loading="loading" height="300px" />
        </el-card>
      </el-col>
      <el-col :span="16">
        <el-table :data="result?.items ?? []" size="small" empty-text="暂无挖掘结果">
          <el-table-column label="时间" width="160">
            <template #default="{ row }">{{ formatTime(row.event_time) }}</template>
          </el-table-column>
          <el-table-column label="类别" width="100">
            <template #default="{ row }">
              {{ CORNER_CASE_CATEGORY_LABELS[row.category] ?? row.category }}
            </template>
          </el-table-column>
          <el-table-column label="车辆" width="130">
            <template #default="{ row }">{{ vehicleStore.resolveName(row.vehicle_id) }}</template>
          </el-table-column>
          <el-table-column label="异常分" width="100">
            <template #default="{ row }">{{ formatNumber(row.anomaly_score, 4) }}</template>
          </el-table-column>
          <el-table-column label="算法" width="110">
            <template #default="{ row }">{{ CORNER_CASE_ALGORITHM_LABELS[row.algorithm] ?? row.algorithm }}</template>
          </el-table-column>
          <el-table-column label="触发事件" width="120">
            <template #default="{ row }">{{ eventTypeLabel(row.trigger_event_type) }}</template>
          </el-table-column>
          <el-table-column prop="description" label="描述" min-width="160" show-overflow-tooltip />
          <el-table-column label="关联场景" width="100" fixed="right">
            <template #default="{ row }">
              <el-button v-if="row.scene_id" link type="primary" @click="goScene(row)">查看</el-button>
              <span v-else>-</span>
            </template>
          </el-table-column>
        </el-table>
      </el-col>
    </el-row>

    <el-pagination
      class="pager"
      layout="total, prev, pager, next"
      :total="result?.total ?? 0"
      :page-size="query.page_size"
      @current-change="(page: number) => { query.page = page; load() }"
    />
  </div>
</template>

<style scoped>
.panel__body {
  margin-top: 12px;
}

.pager {
  margin-top: 12px;
  text-align: right;
}
</style>
