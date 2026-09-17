<script setup lang="ts">
/**
 * 运营看板（五大页面模块之一）
 *
 * 数据来源：
 * - GET /api/v1/analytics/dashboard（车队/管道/算法/事件四维聚合，各维度带 available 降级标记）
 * - GET /api/v1/remote/vehicles（Redis 车辆读模型，用于在线车辆明细）
 * 刷新策略：POLL_INTERVALS.dashboard（环境变量注入，默认 5s，满足附录 D 限流）
 */
import { computed, onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import type { EChartsOption } from 'echarts'

import { fetchDashboard } from '@/api/analytics'
import EchartBase from '@/components/charts/EchartBase.vue'
import StatCard from '@/components/common/StatCard.vue'
import StatusTag from '@/components/common/StatusTag.vue'
import { usePolling } from '@/composables/usePolling'
import {
  DASHBOARD_TIME_RANGES,
  EVENT_LEVEL_COLORS,
  POLL_INTERVALS,
  VEHICLE_STATUS_LABELS,
} from '@/constants'
import { PERMISSIONS } from '@/constants/permissions'
import { useUserStore } from '@/stores/user'
import { useVehicleStore } from '@/stores/vehicle'
import type { DashboardData } from '@/types/analytics'
import { HunterApiError } from '@/utils/error-code'
import { formatNumber, formatPercent, formatTime } from '@/utils/format'

const router = useRouter()
const userStore = useUserStore()
const vehicleStore = useVehicleStore()

const timeRange = ref<'1h' | '24h' | '7d' | '30d'>('24h')
const dashboard = ref<DashboardData | null>(null)
const loading = ref(false)

/** 车队维度 */
const fleet = computed(() => dashboard.value?.fleet)
const onlineRatio = computed<number | null>(() => {
  const value = fleet.value
  if (!value || !value.available || !value.total_vehicles) {
    return null
  }
  return value.online_vehicles / value.total_vehicles
})

/** 事件维度 */
const events = computed(() => dashboard.value?.events)
const isDegraded = computed<boolean>(() => (dashboard.value?.degraded ?? []).length > 0)

async function loadDashboard(): Promise<void> {
  loading.value = true
  try {
    const [dashboardData] = await Promise.all([
      fetchDashboard({ time_range: timeRange.value }),
      vehicleStore.fetchVehicles(),
    ])
    dashboard.value = dashboardData
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '看板数据加载失败')
  } finally {
    loading.value = false
  }
}

const polling = usePolling(loadDashboard, POLL_INTERVALS.dashboard, {
  onError: (error) => {
    // 轮询异常静默重试，避免频繁弹窗；仅首次加载失败提示
    if (!dashboard.value && error instanceof HunterApiError) {
      ElMessage.error(error.message)
    }
  },
})

/** 车辆状态分布（8 态受控词表，by_status 缺失时置 0） */
const vehicleStatusOption = computed<EChartsOption>(() => {
  const byStatus = fleet.value?.by_status ?? {}
  const data = Object.keys(VEHICLE_STATUS_LABELS).map((status) => ({
    name: VEHICLE_STATUS_LABELS[status],
    value: byStatus[status] ?? 0,
  }))
  return {
    tooltip: { trigger: 'item' },
    legend: { bottom: 0, type: 'scroll' },
    series: [
      {
        type: 'pie',
        radius: ['40%', '68%'],
        avoidLabelOverlap: true,
        label: { formatter: '{b}: {c}' },
        data,
      },
    ],
  }
})

/** 事件等级 + Top 事件类型分布 */
const eventTypeOption = computed<EChartsOption>(() => {
  const byType = events.value?.by_type ?? {}
  const entries = Object.entries(byType)
    .sort((left, right) => right[1] - left[1])
    .slice(0, 10)
  return {
    tooltip: { trigger: 'axis' },
    grid: { left: 120, right: 24, top: 24, bottom: 24 },
    xAxis: { type: 'value' },
    yAxis: { type: 'category', data: entries.map(([type]) => type) },
    series: [
      {
        type: 'bar',
        barWidth: 14,
        data: entries.map(([, count]) => count),
        itemStyle: { color: EVENT_LEVEL_COLORS.warning },
      },
    ],
  }
})

onMounted(() => {
  polling.start()
})
</script>

<template>
  <div class="dashboard">
    <el-card shadow="never" class="dashboard__toolbar">
      <div class="toolbar">
        <el-radio-group v-model="timeRange" @change="polling.trigger">
          <el-radio-button v-for="range in DASHBOARD_TIME_RANGES" :key="range.value" :value="range.value">
            {{ range.label }}
          </el-radio-button>
        </el-radio-group>
        <div class="toolbar__right">
          <el-tag v-if="isDegraded" type="warning" size="small">
            部分维度降级：{{ dashboard?.degraded?.join(', ') }}
          </el-tag>
          <span class="toolbar__time">
            更新于 {{ formatTime(dashboard ? Math.floor(dashboard.generated_at) : null) }}
          </span>
          <el-button :loading="loading" @click="polling.trigger">刷新</el-button>
        </div>
      </div>
    </el-card>

    <el-row :gutter="12" class="dashboard__stats">
      <el-col :span="6">
        <StatCard
          title="车辆总数"
          :value="fleet?.total_vehicles ?? 0"
          :available="fleet?.available ?? true"
          :reason="fleet?.reason"
          hint="统计口径：平台注册车辆"
        />
      </el-col>
      <el-col :span="6">
        <StatCard
          title="在线车辆"
          :value="fleet?.online_vehicles ?? 0"
          tone="success"
          :available="fleet?.available ?? true"
          :reason="fleet?.reason"
          :hint="onlineRatio === null ? '在线率 -' : `在线率 ${formatPercent(onlineRatio)}`"
        />
      </el-col>
      <el-col :span="6">
        <StatCard
          title="遥测入库点（区间）"
          :value="dashboard?.pipeline.telemetry_points ?? 0"
          tone="info"
          :available="dashboard?.pipeline.available ?? true"
          :reason="dashboard?.pipeline.reason"
          hint="TimescaleDB vehicle_telemetry 写入点数"
        />
      </el-col>
      <el-col :span="6">
        <StatCard
          title="入库延迟 P95"
          :value="formatNumber(dashboard?.pipeline.ingest_latency_ms_p95 ?? null, 0)"
          unit="ms"
          :tone="(dashboard?.pipeline.ingest_latency_ms_p95 ?? 0) > 1000 ? 'danger' : 'success'"
          :available="dashboard?.pipeline.available ?? true"
          :reason="dashboard?.pipeline.reason"
          hint="SLO ≤ 1000 ms（系统约束第 10 条）"
        />
      </el-col>
    </el-row>

    <el-row :gutter="12" class="dashboard__stats">
      <el-col :span="6">
        <StatCard
          title="感知帧率均值"
          :value="formatNumber(dashboard?.algorithm.perception_fps_avg ?? null, 1)"
          unit="fps"
          tone="primary"
          :available="dashboard?.algorithm.available ?? true"
          :reason="dashboard?.algorithm.reason"
          hint="感知模块 FPS"
        />
      </el-col>
      <el-col :span="6">
        <StatCard
          title="感知时延均值"
          :value="formatNumber(dashboard?.algorithm.perception_latency_ms_avg ?? null, 0)"
          unit="ms"
          tone="info"
          :available="dashboard?.algorithm.available ?? true"
          :reason="dashboard?.algorithm.reason"
          hint="感知模块端到端时延"
        />
      </el-col>
      <el-col :span="6">
        <StatCard
          title="控制时延均值"
          :value="formatNumber(dashboard?.algorithm.control_latency_ms_avg ?? null, 0)"
          unit="ms"
          :tone="(dashboard?.algorithm.control_latency_ms_avg ?? 0) > 100 ? 'warning' : 'success'"
          :available="dashboard?.algorithm.available ?? true"
          :reason="dashboard?.algorithm.reason"
          hint="远程操控指令 SLO ≤ 100 ms"
        />
      </el-col>
      <el-col :span="6">
        <StatCard
          title="未确认事件"
          :value="events?.unacknowledged ?? 0"
          :tone="(events?.critical ?? 0) > 0 ? 'danger' : 'warning'"
          :available="events?.available ?? true"
          :reason="events?.reason"
          :hint="`严重 ${events?.critical ?? 0} / 警告 ${events?.warning ?? 0} / 提示 ${events?.info ?? 0}`"
        />
      </el-col>
    </el-row>

    <el-row :gutter="12" class="dashboard__charts">
      <el-col :span="12">
        <el-card shadow="never" header="车辆状态分布（8 态受控词表）">
          <EchartBase :option="vehicleStatusOption" :loading="loading" height="320px" />
        </el-card>
      </el-col>
      <el-col :span="12">
        <el-card shadow="never" header="事件类型 Top 10">
          <EchartBase :option="eventTypeOption" :loading="loading" height="320px" />
        </el-card>
      </el-col>
    </el-row>

    <el-card shadow="never" header="车辆明细（Redis 读模型）">
      <el-table :data="vehicleStore.vehicles" size="small" empty-text="暂无车辆数据">
        <el-table-column prop="vehicle_id" label="车辆 ID" width="150" />
        <el-table-column prop="vehicle_name" label="名称" width="150" />
        <el-table-column label="状态" width="120">
          <template #default="{ row }">
            <StatusTag kind="vehicle" :value="row.status" />
          </template>
        </el-table-column>
        <el-table-column label="电量" width="100">
          <template #default="{ row }">{{ formatNumber(row.battery_soc, 0) }}%</template>
        </el-table-column>
        <el-table-column label="速度" width="120">
          <template #default="{ row }">{{ formatNumber(row.velocity ?? null, 2) }} m/s</template>
        </el-table-column>
        <el-table-column label="最近在线" width="180">
          <template #default="{ row }">{{ formatTime(row.last_online_time) }}</template>
        </el-table-column>
        <el-table-column label="操作" min-width="140">
          <template #default="{ row }">
            <el-button
              v-if="userStore.hasPermission(PERMISSIONS.remoteRead)"
              link
              type="primary"
              @click="router.push({ name: 'remote-console', query: { vehicle_id: row.vehicle_id } })"
            >
              前往操控台
            </el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-card>
  </div>
</template>

<style scoped>
.dashboard__toolbar {
  margin-bottom: 12px;
}

.toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
}

.toolbar__right {
  display: flex;
  align-items: center;
  gap: 12px;
}

.toolbar__time {
  font-size: 12px;
  color: var(--el-text-color-secondary);
}

.dashboard__stats,
.dashboard__charts {
  margin-bottom: 12px;
}
</style>
