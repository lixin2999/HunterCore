<script setup lang="ts">
/**
 * 单车运营详情抽屉（运营看板钻取）
 *
 * 复用后端已支持的单车聚合：GET /api/v1/analytics/dashboard?vehicle_id=
 * 叠加：近 60 点实时遥测（GET /api/v1/data/telemetry）、该车事件（GET /api/v1/data/events）。
 * 抽屉经 teleport 渲染到 body，脱离看板 .dashboard-scope，故自带深色样式（非 scoped）。
 */
import { computed, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import type { EChartsOption } from 'echarts'

import { fetchDashboard } from '@/api/analytics'
import { listEvents, queryTelemetry } from '@/api/data'
import EchartBase from '@/components/charts/EchartBase.vue'
import StatusTag from '@/components/common/StatusTag.vue'
import { useVehicleStore } from '@/stores/vehicle'
import { PERMISSIONS } from '@/constants/permissions'
import { useUserStore } from '@/stores/user'
import type { DashboardData } from '@/types/analytics'
import type { EventItem, TelemetrySample } from '@/types/data'
import { formatNumber, formatSoc, formatTime } from '@/utils/format'

const props = defineProps<{
  modelValue: boolean
  vehicleId: string
  timeRange: '1h' | '24h' | '7d' | '30d'
}>()

const emit = defineEmits<{ 'update:modelValue': [boolean] }>()

const router = useRouter()
const vehicleStore = useVehicleStore()
const userStore = useUserStore()

const loading = ref(false)
const dashboard = ref<DashboardData | null>(null)
const telemetry = ref<TelemetrySample[]>([])
const events = ref<EventItem[]>([])

const vehicle = computed(() => (props.vehicleId ? vehicleStore.findVehicle(props.vehicleId) : undefined))
const vehicleName = computed(() => vehicleStore.resolveName(props.vehicleId))

async function load(): Promise<void> {
  if (!props.vehicleId) {
    return
  }
  loading.value = true
  try {
    // 契约要求 telemetry 的 start_time/end_time 必选（缺失 → 422）：取最近 1h 窗口内最新 60 点
    const nowSec = Date.now() / 1000
    const [dash, tele, evt] = await Promise.all([
      fetchDashboard({ time_range: props.timeRange, vehicle_id: props.vehicleId }),
      queryTelemetry({
        vehicle_id: props.vehicleId,
        start_time: nowSec - 3600,
        end_time: nowSec,
        page: 1,
        page_size: 60,
      }),
      listEvents({ vehicle_id: props.vehicleId, page: 1, page_size: 10, order: 'desc', sort: 'event_time' }),
    ])
    dashboard.value = dash
    telemetry.value = [...tele.items].reverse()
    events.value = evt.items
  } finally {
    loading.value = false
  }
}

watch(
  () => [props.modelValue, props.vehicleId] as const,
  ([visible]) => {
    if (visible && props.vehicleId) {
      void load()
    }
  },
  { immediate: true },
)

/** 遥测速度 / 转角趋势（近 60 点，双轴） */
const telemetryOption = computed<EChartsOption>(() => {
  const samples = telemetry.value
  const times = samples.map((item) => formatTime(Math.floor(item.time), 'HH:mm:ss'))
  const velocity = samples.map((item) => item.chassis?.velocity ?? null)
  const steer = samples.map((item) => item.chassis?.steering_angle ?? null)
  return {
    animationDuration: 600,
    tooltip: { trigger: 'axis' },
    legend: { data: ['速度 (m/s)', '转角 (rad)'], textStyle: { color: '#9fb6d6' }, top: 0 },
    grid: { left: 44, right: 44, top: 34, bottom: 28 },
    xAxis: {
      type: 'category',
      data: times,
      axisLabel: { color: '#6f849f', showMaxLabel: true, showMinLabel: true },
      axisLine: { lineStyle: { color: 'rgba(120,150,190,0.25)' } },
    },
    yAxis: [
      { type: 'value', name: 'm/s', axisLabel: { color: '#6f849f' }, splitLine: { lineStyle: { color: 'rgba(120,150,190,0.12)' } } },
      { type: 'value', name: 'rad', axisLabel: { color: '#6f849f' }, splitLine: { show: false } },
    ],
    series: [
      {
        name: '速度 (m/s)',
        type: 'line',
        smooth: true,
        showSymbol: false,
        data: velocity,
        lineStyle: { color: '#22d3ee', width: 2 },
        areaStyle: { color: 'rgba(34,211,238,0.16)' },
      },
      {
        name: '转角 (rad)',
        type: 'line',
        smooth: true,
        showSymbol: false,
        yAxisIndex: 1,
        data: steer,
        lineStyle: { color: '#a78bfa', width: 2 },
      },
    ],
  }
})

const latest = computed<TelemetrySample | undefined>(() => telemetry.value[telemetry.value.length - 1])

function goConsole(): void {
  emit('update:modelValue', false)
  void router.push({ name: 'remote-console', query: { vehicle_id: props.vehicleId } })
}
</script>

<template>
  <el-drawer
    :model-value="modelValue"
    class="drilldown-drawer"
    size="640px"
    direction="rtl"
    :with-header="false"
    @update:model-value="emit('update:modelValue', $event)"
  >
    <div v-loading="loading" class="dd">
      <div class="dd__head">
        <div class="dd__title">
          <span class="dd__name">{{ vehicleName }}</span>
          <StatusTag kind="vehicle" :value="vehicle?.status ?? 'offline'" />
        </div>
        <div class="dd__head-right">
          <el-button
            v-if="userStore.hasPermission(PERMISSIONS.remoteRead)"
            type="primary"
            size="small"
            :disabled="!vehicle?.controllable"
            @click="goConsole"
          >
            进入操控台
          </el-button>
          <el-button size="small" @click="load">刷新</el-button>
          <el-button link size="small" @click="emit('update:modelValue', false)">关闭</el-button>
        </div>
      </div>
      <div class="dd__sub">{{ props.vehicleId }} · 统计区间 {{ timeRange }}</div>

      <div class="dd__kpis">
        <div class="dd__kpi">
          <span>实时速度</span>
          <strong>{{ formatNumber(latest?.chassis?.velocity ?? null, 2) }} <i>m/s</i></strong>
        </div>
        <div class="dd__kpi">
          <span>电量</span>
          <strong>{{ formatSoc(latest?.chassis?.battery_soc ?? vehicle?.battery_soc ?? null) }}</strong>
        </div>
        <div class="dd__kpi">
          <span>遥测入库点</span>
          <strong>{{ formatNumber(dashboard?.pipeline.telemetry_points ?? 0, 0) }}</strong>
        </div>
        <div class="dd__kpi">
          <span>未确认事件</span>
          <strong>{{ dashboard?.events.unacknowledged ?? 0 }}</strong>
        </div>
      </div>

      <div class="dd__block">
        <div class="dd__block-title">实时遥测趋势（近 60 采样点）</div>
        <EchartBase :option="telemetryOption" :loading="loading" :not-merge="false" height="240px" />
      </div>

      <div class="dd__block">
        <div class="dd__block-title">算法健康</div>
        <el-descriptions :column="2" size="small" border class="dd__desc">
          <el-descriptions-item label="感知帧率">{{ formatNumber(dashboard?.algorithm.perception_fps_avg ?? null, 1) }} fps</el-descriptions-item>
          <el-descriptions-item label="感知时延">{{ formatNumber(dashboard?.algorithm.perception_latency_ms_avg ?? null, 0) }} ms</el-descriptions-item>
          <el-descriptions-item label="规划时延">{{ formatNumber(dashboard?.algorithm.planning_latency_ms_avg ?? null, 0) }} ms</el-descriptions-item>
          <el-descriptions-item label="控制时延">{{ formatNumber(dashboard?.algorithm.control_latency_ms_avg ?? null, 0) }} ms</el-descriptions-item>
        </el-descriptions>
      </div>

      <div class="dd__block">
        <div class="dd__block-title">最近事件</div>
        <el-table :data="events" size="small" empty-text="暂无事件" class="dd__table">
          <el-table-column label="时间" width="150">
            <template #default="{ row }">{{ formatTime(row.event_time) }}</template>
          </el-table-column>
          <el-table-column prop="event_type" label="类型" min-width="140" />
          <el-table-column label="等级" width="90">
            <template #default="{ row }">
              <StatusTag kind="event-level" :value="row.event_level" />
            </template>
          </el-table-column>
          <el-table-column label="确认" width="70">
            <template #default="{ row }">
              <span :class="row.acknowledged ? 'dd__ok' : 'dd__pending'">{{ row.acknowledged ? '已确认' : '未确认' }}</span>
            </template>
          </el-table-column>
        </el-table>
      </div>
    </div>
  </el-drawer>
</template>

<style>
/* 非 scoped：el-drawer 经 teleport 渲染到 body，需全局作用域命中 */
.drilldown-drawer .el-drawer__body {
  padding: 0;
  background: linear-gradient(160deg, #0d1728 0%, #0a1120 100%);
  color: #cfe0f5;
}

.dd {
  padding: 20px 22px 28px;
  min-height: 100%;
}

.dd__head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.dd__title {
  display: flex;
  align-items: center;
  gap: 10px;
}

.dd__name {
  font-size: 20px;
  font-weight: 700;
  color: #eaf4ff;
}

.dd__head-right {
  display: flex;
  align-items: center;
  gap: 8px;
}

.dd__sub {
  margin-top: 4px;
  font-size: 12px;
  color: #6f849f;
}

.dd__kpis {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 10px;
  margin: 18px 0 6px;
}

.dd__kpi {
  padding: 12px;
  border-radius: 12px;
  border: 1px solid rgba(56, 189, 248, 0.14);
  background: rgba(20, 33, 56, 0.7);
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.dd__kpi span {
  font-size: 12px;
  color: #9fb6d6;
}

.dd__kpi strong {
  font-size: 19px;
  color: #eaf4ff;
  font-variant-numeric: tabular-nums;
}

.dd__kpi i {
  font-size: 12px;
  font-style: normal;
  color: #8aa2c0;
}

.dd__block {
  margin-top: 18px;
}

.dd__block-title {
  font-size: 13px;
  letter-spacing: 0.5px;
  color: #7fd6ff;
  margin-bottom: 8px;
  padding-left: 8px;
  border-left: 3px solid #22d3ee;
}

.dd__desc {
  --el-text-color-primary: #cfe0f5;
  --el-fill-color-blank: transparent;
}

.dd__desc .el-descriptions__label {
  color: #9fb6d6 !important;
  background: rgba(20, 33, 56, 0.6) !important;
}

.dd__desc .el-descriptions__content {
  color: #eaf4ff !important;
  background: rgba(13, 23, 40, 0.4) !important;
}

.dd__table {
  --el-table-bg-color: transparent;
  --el-table-tr-bg-color: transparent;
  --el-table-header-bg-color: rgba(20, 33, 56, 0.6);
  --el-table-text-color: #cfe0f5;
  --el-table-header-text-color: #9fb6d6;
  --el-table-border-color: rgba(120, 150, 190, 0.14);
}

.dd__ok {
  color: #34d399;
}

.dd__pending {
  color: #fbbf24;
}
</style>
