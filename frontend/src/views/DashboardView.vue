<script setup lang="ts">
/**
 * 运营看板（深色科技风 · V1.21.0 重构）
 *
 * 数据来源（不变，沿用既有契约聚合）：
 * - GET /api/v1/analytics/dashboard（车队/管道/算法/事件四维聚合，各维度带 available 降级标记）
 * - GET /api/v1/remote/vehicles（Redis 车辆读模型，用于在线车辆明细）
 * 刷新策略：POLL_INTERVALS.dashboard（环境变量注入，默认 5s，满足附录 D 限流）
 *
 * 主题作用域：深色科技风仅作用于本页（.dash 根节点局部 CSS 变量 + 全出血覆盖 layout__main 浅色底），
 * 不改全局主题、不影响其它页面 Element Plus 浅色风格。
 */
import { computed, onMounted, reactive, ref } from 'vue'
import { ElMessage } from 'element-plus'
import type { EChartsOption } from 'echarts'

import { fetchDashboard } from '@/api/analytics'
import EchartBase from '@/components/charts/EchartBase.vue'
import MetricCard from '@/components/dashboard/MetricCard.vue'
import StatusTag from '@/components/common/StatusTag.vue'
import VehicleDrilldownDrawer from '@/views/dashboard/VehicleDrilldownDrawer.vue'
import { usePolling } from '@/composables/usePolling'
import {
  DASHBOARD_TIME_RANGES,
  EVENT_TYPE_LABELS,
  POLL_INTERVALS,
  VEHICLE_STATUS_LABELS,
} from '@/constants'
import { useVehicleStore } from '@/stores/vehicle'
import type { DashboardData } from '@/types/analytics'
import { HunterApiError } from '@/utils/error-code'
import { formatNumber, formatPercent, formatRelativeTime, formatSoc, formatTime } from '@/utils/format'

const vehicleStore = useVehicleStore()

const timeRange = ref<'1h' | '24h' | '7d' | '30d'>('24h')
const dashboard = ref<DashboardData | null>(null)
const loading = ref(false)

/** 单车抽屉钻取 */
const drawerVisible = ref(false)
const activeVehicleId = ref('')

// =====================================================================
// 客户端环形缓冲：把每次轮询快照的标量指标累积成趋势序列（供 sparkline / 面积图）
// =====================================================================
const SPARK_CAPACITY = 24
const spark = reactive<Record<string, number[]>>({})

function pushSpark(key: string, value: number | null | undefined): void {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return
  }
  const series = spark[key] ?? (spark[key] = [])
  series.push(value)
  if (series.length > SPARK_CAPACITY) {
    series.shift()
  }
}

function resetSpark(): void {
  for (const key of Object.keys(spark)) {
    spark[key] = []
  }
}

/** 维度分块 */
const fleet = computed(() => dashboard.value?.fleet)
const pipeline = computed(() => dashboard.value?.pipeline)
const algorithm = computed(() => dashboard.value?.algorithm)
const events = computed(() => dashboard.value?.events)

const onlineRatio = computed<number | null>(() => {
  const value = fleet.value
  if (!value || !value.available || !value.total_vehicles) {
    return null
  }
  return value.online_vehicles / value.total_vehicles
})

const isDegraded = computed<boolean>(() => (dashboard.value?.degraded ?? []).length > 0)

/**
 * 入库延迟（V1.19.21）：无数据时不只显示孤零零的 `-`。
 * 后端 `ingest_latency_reason` 已分档给出口径，卡片副文案直接用它，兜底退回 SLO 口径。
 */
const INGEST_LATENCY_SLO_HINT = 'SLO ≤ 1000 ms（系统约束第 10 条）'
const ingestLatencyMs = computed<number | null | undefined>(
  () => pipeline.value?.ingest_latency_ms_p95,
)
const ingestLatencyHint = computed<string>(() => {
  if (ingestLatencyMs.value != null) {
    return INGEST_LATENCY_SLO_HINT
  }
  return pipeline.value?.ingest_latency_reason || '暂无数据（Flink 未产出诊断，看健康检查 ⑯）'
})

async function loadDashboard(): Promise<void> {
  loading.value = true
  try {
    const [dashboardData] = await Promise.all([
      fetchDashboard({ time_range: timeRange.value }),
      vehicleStore.fetchVehicles(),
    ])
    dashboard.value = dashboardData
    // 快照标量入环形缓冲（轮询趋势）
    pushSpark('online', dashboardData.fleet.online_vehicles)
    pushSpark('total', dashboardData.fleet.total_vehicles)
    pushSpark('telemetry_points', dashboardData.pipeline.telemetry_points ?? 0)
    pushSpark('ingest_latency', dashboardData.pipeline.ingest_latency_ms_p95 ?? null)
    pushSpark('perception_fps', dashboardData.algorithm.perception_fps_avg ?? null)
    pushSpark('perception_latency', dashboardData.algorithm.perception_latency_ms_avg ?? null)
    pushSpark('control_latency', dashboardData.algorithm.control_latency_ms_avg ?? null)
    pushSpark('unack', dashboardData.events.unacknowledged ?? 0)
  } catch (error) {
    notifyLoadError(error)
  } finally {
    loading.value = false
  }
}

/** 仅首次加载失败提示；已有数据时轮询异常静默（避免弹窗刷屏） */
function notifyLoadError(error: unknown): void {
  if (!dashboard.value) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '看板数据加载失败')
  }
}

const polling = usePolling(loadDashboard, POLL_INTERVALS.dashboard, {
  onError: notifyLoadError,
})

function handleRangeChange(): void {
  resetSpark()
  void polling.trigger()
}

function openDrilldown(vehicleId: string): void {
  if (!vehicleId) {
    return
  }
  activeVehicleId.value = vehicleId
  drawerVisible.value = true
}

// =====================================================================
// 图表配置（深色 + 渐变 + 动态过渡：统一 notMerge=false 增量合并）
// =====================================================================

/** hex → 竖向渐变描述对象（ECharts 原生支持渐变 plain object） */
function verticalGradient(hex: string): Record<string, unknown> {
  return {
    type: 'linear',
    x: 0,
    y: 0,
    x2: 0,
    y2: 1,
    colorStops: [
      { offset: 0, color: hex },
      { offset: 1, color: `${hex}66` },
    ],
  }
}

function horizontalGradient(hex: string): Record<string, unknown> {
  return {
    type: 'linear',
    x: 0,
    y: 0,
    x2: 1,
    y2: 0,
    colorStops: [
      { offset: 0, color: `${hex}55` },
      { offset: 1, color: hex },
    ],
  }
}

/** 车辆状态 → 主色（与 KPI/明细表状态灯一致） */
const STATUS_COLORS: Record<string, string> = {
  offline: '#64748b',
  online_idle: '#34d399',
  auto_driving: '#60a5fa',
  remote_controlled: '#fbbf24',
  upgrading: '#a78bfa',
  charging: '#38bdf8',
  fault: '#f87171',
  emergency: '#ef4444',
}

/** 车辆状态分布（环形图，8 态受控词表，渐变 + 中心总量） */
const vehicleStatusOption = computed<EChartsOption>(() => {
  const byStatus = fleet.value?.by_status ?? {}
  const data = Object.keys(VEHICLE_STATUS_LABELS).map((status) => ({
    name: VEHICLE_STATUS_LABELS[status],
    value: byStatus[status] ?? 0,
    itemStyle: { color: verticalGradient(STATUS_COLORS[status] ?? '#38bdf8') },
  }))
  const total = fleet.value?.total_vehicles ?? 0
  return {
    animationDuration: 800,
    animationDurationUpdate: 600,
    tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)' },
    legend: {
      bottom: 0,
      type: 'scroll',
      icon: 'circle',
      textStyle: { color: '#9fb6d6' },
    },
    graphic: [
      {
        type: 'text',
        left: 'center',
        top: '40%',
        style: {
          text: String(total),
          fill: '#eaf4ff',
          font: '700 26px system-ui',
          textAlign: 'center',
        },
      },
      {
        type: 'text',
        left: 'center',
        top: '50%',
        style: { text: '车辆总数', fill: '#6f849f', font: '12px system-ui', textAlign: 'center' },
      },
    ],
    series: [
      {
        type: 'pie',
        radius: ['52%', '74%'],
        center: ['50%', '46%'],
        avoidLabelOverlap: true,
        itemStyle: { borderColor: 'rgba(13,23,40,0.9)', borderWidth: 2 },
        label: { show: false },
        emphasis: {
          label: { show: true, fontSize: 14, color: '#eaf4ff', formatter: '{b}\n{c}' },
          itemStyle: { shadowBlur: 16, shadowColor: 'rgba(56,189,248,0.5)' },
        },
        data,
      },
    ],
  }
})

/** 事件类型 Top 10（横向渐变柱，中文标签） */
const eventTypeOption = computed<EChartsOption>(() => {
  const byType = events.value?.by_type ?? {}
  const entries = Object.entries(byType)
    .sort((left, right) => right[1] - left[1])
    .slice(0, 10)
    .reverse()
  return {
    animationDuration: 700,
    animationDurationUpdate: 600,
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
    grid: { left: 96, right: 40, top: 16, bottom: 24, containLabel: true },
    xAxis: {
      type: 'value',
      axisLabel: { color: '#6f849f' },
      axisLine: { lineStyle: { color: 'rgba(120,150,190,0.25)' } },
      splitLine: { lineStyle: { color: 'rgba(120,150,190,0.12)' } },
    },
    yAxis: {
      type: 'category',
      data: entries.map(([type]) => EVENT_TYPE_LABELS[type] ?? type),
      axisLabel: { color: '#9fb6d6' },
      axisLine: { lineStyle: { color: 'rgba(120,150,190,0.25)' } },
    },
    series: [
      {
        type: 'bar',
        barWidth: 14,
        data: entries.map(([, count]) => count),
        itemStyle: { borderRadius: [0, 7, 7, 0], color: horizontalGradient('#22d3ee') },
        label: { show: true, position: 'right', color: '#cfe0f5' },
      },
    ],
  }
})

/**
 * Kafka 消费组积压（known lag 渐变柱 + unknown_groups 按**成因**逐行标注「未判定」，不当零）
 *
 * V1.21.14：未判定行**不再画成 0 长度灰条**（与「一条积压为 0 的组」视觉上无法区分，且全组未判定时
 * 坐标轴退化成 0~1，整块被读成「没有数据」）。改为：灰底行带 + 行内写着成因人话
 * （后端 `cause_text`），成因与处置提示在悬浮里给全；一条可测的滞后都没有时直接收起数值轴。
 */
const kafkaLagOption = computed<EChartsOption>(() => {
  const lag = pipeline.value?.kafka_consumer_lag ?? {}
  const unknown = pipeline.value?.unknown_groups ?? []
  const knownEntries = Object.entries(lag)
  const data = [
    ...knownEntries.map(([, value]) => ({
      value,
      itemStyle: { color: horizontalGradient('#a78bfa'), borderRadius: [0, 6, 6, 0] },
      real: true,
    })),
    ...unknown.map((item) => ({
      value: 0,
      itemStyle: { color: 'transparent' },
      real: false,
      cause: item.cause_text || item.cause || '成因未取到',
      hint: item.hint || '',
    })),
  ]
  // 未判定行的类目名整体压暗（确定性做法：不依赖类目轴上按行铺灰底的坐标换算——
  // markArea 在类目轴上按小数下标铺行带会错位，实测行带会盖到相邻两行）
  const categories = [
    ...knownEntries.map(([group]) => group),
    ...unknown.map((item) => ({ value: item.group, textStyle: { color: '#8298b3' } })),
  ]
  return {
    animationDuration: 700,
    animationDurationUpdate: 600,
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      formatter: (params: unknown) => {
        const list = params as {
          name: string
          value: number
          data?: { real?: boolean; cause?: string; hint?: string }
        }[]
        const item = list[0]
        if (!item) {
          return ''
        }
        if (item.data?.real !== false) {
          return `${item.name}<br/>滞后：${item.value}`
        }
        const detail = [item.data?.hint].filter(Boolean).join('')
        return (
          `${item.name}<br/>滞后：未判定（${item.data?.cause ?? '成因未取到'}）` +
          `${detail ? `<br/>${detail}` : ''}<br/>⚠ 未判定 ≠ 零积压`
        )
      },
    },
    grid: { left: 12, right: 32, top: 16, bottom: 24, containLabel: true },
    xAxis: {
      type: 'value',
      // 一条可测的滞后都没有时，数值轴只是噪声（旧渲染的 0~1 空轴正是「没有数据」的观感来源）
      show: knownEntries.length > 0,
      max: knownEntries.length > 0 ? undefined : 1,
      axisLabel: { color: '#6f849f' },
      splitLine: { show: knownEntries.length > 0, lineStyle: { color: 'rgba(120,150,190,0.12)' } },
    },
    yAxis: {
      type: 'category',
      data: categories,
      axisLabel: { color: '#9fb6d6', width: 120, overflow: 'truncate' },
      axisLine: { lineStyle: { color: 'rgba(120,150,190,0.25)' } },
    },
    series: [
      {
        type: 'bar',
        barWidth: 14,
        data,
        label: {
          show: true,
          position: 'right',
          color: '#cfe0f5',
          formatter: (params: unknown) => {
            const item = params as { value: number; data?: { real?: boolean; cause?: string } }
            // 未判定行把成因写在行内（以前这里只有一个「未判定」，四个组同形 ⇒ 读成「没有数据」）
            return item.data?.real === false
              ? `未判定 · ${item.data.cause ?? '成因未取到'}`
              : String(item.value)
          },
        },
      },
    ],
  }
})

/** 面板抬头口径（V1.21.14）：全组未判定时不能只留一句静态说明，得先说出「有几个组、为什么」 */
const kafkaLagNote = computed<string>(() => {
  const known = Object.keys(pipeline.value?.kafka_consumer_lag ?? {}).length
  const unknown = (pipeline.value?.unknown_groups ?? []).length
  if (!unknown) {
    return '灰条「未判定」= 无提交位点/组不存在，非零积压'
  }
  return known === 0
    ? `全部 ${unknown} 个消费组未判定（成因见行内；未判定 ≠ 零积压）`
    : `${unknown} 个组未判定（成因见行内；未判定 ≠ 零积压）`
})

/** 入库延迟趋势（面积图 + SLO 1000ms 标线，数据来自轮询环形缓冲） */
const INGEST_LATENCY_SLO_MS = 1000
const ingestLatencyOption = computed<EChartsOption>(() => {
  const series = spark.ingest_latency ?? []
  return {
    animationDuration: 600,
    animationDurationUpdate: 500,
    tooltip: { trigger: 'axis', valueFormatter: (value) => `${value} ms` },
    grid: { left: 44, right: 20, top: 24, bottom: 28 },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      data: series.map((_, index) => `#${index + 1}`),
      axisLabel: { color: '#6f849f', showMaxLabel: true, showMinLabel: true },
      axisLine: { lineStyle: { color: 'rgba(120,150,190,0.25)' } },
    },
    yAxis: {
      type: 'value',
      axisLabel: { color: '#6f849f', formatter: '{value} ms' },
      splitLine: { lineStyle: { color: 'rgba(120,150,190,0.12)' } },
    },
    series: [
      {
        type: 'line',
        name: '入库延迟 P95',
        smooth: true,
        showSymbol: false,
        data: series,
        lineStyle: { color: '#22d3ee', width: 2 },
        areaStyle: { color: verticalGradient('#22d3ee') },
        markLine: {
          symbol: 'none',
          data: [{ yAxis: INGEST_LATENCY_SLO_MS, name: 'SLO' }],
          lineStyle: { color: '#f87171', type: 'dashed' },
          label: { formatter: 'SLO 1000ms', color: '#f87171', position: 'insideEndTop' },
        },
      },
    ],
  }
})

const hasLatencyTrend = computed<boolean>(() => (spark.ingest_latency ?? []).length >= 2)

/** 明细表增强：SOC 进度色 */
function socColor(soc: number | null): string {
  if (soc === null || Number.isNaN(soc)) {
    return '#475569'
  }
  if (soc < 10) {
    return '#f87171'
  }
  if (soc < 20) {
    return '#fbbf24'
  }
  return '#34d399'
}

onMounted(() => {
  polling.start()
})
</script>

<template>
  <div class="dash">
    <div class="dash__bg-grid" />

    <!-- 顶部工具栏 -->
    <header class="dash__bar">
      <div class="dash__brand">
        <span class="dash__title">运营看板</span>
        <span class="dash__subtitle">Fleet Operations · Realtime</span>
      </div>
      <div class="dash__bar-right">
        <el-tag v-if="isDegraded" type="warning" size="small" effect="dark">
          部分维度降级：{{ dashboard?.degraded?.join(', ') }}
        </el-tag>
        <el-radio-group v-model="timeRange" size="default" @change="handleRangeChange">
          <el-radio-button v-for="range in DASHBOARD_TIME_RANGES" :key="range.value" :value="range.value">
            {{ range.label }}
          </el-radio-button>
        </el-radio-group>
        <span class="dash__updated">更新于 {{ formatTime(dashboard ? Math.floor(dashboard.generated_at) : null) }}</span>
        <el-button :loading="loading" @click="polling.trigger">刷新</el-button>
      </div>
    </header>

    <!-- 第一层：KPI 发光带 -->
    <section class="dash__kpis">
      <MetricCard
        title="车辆总数"
        :value="fleet?.total_vehicles ?? 0"
        tone="blue"
        :available="fleet?.available ?? true"
        :reason="fleet?.reason"
        hint="统计口径：平台注册车辆"
        :spark="spark.total ?? []"
      />
      <MetricCard
        title="在线率"
        :value="formatPercent(onlineRatio)"
        tone="green"
        :available="fleet?.available ?? true"
        :reason="fleet?.reason"
        :badge="`${fleet?.online_vehicles ?? 0}/${fleet?.total_vehicles ?? 0} 在线`"
        hint="在线 = offline 之外 7 态"
        :spark="spark.online ?? []"
      />
      <MetricCard
        title="遥测入库点"
        :value="formatNumber(pipeline?.telemetry_points ?? 0, 0)"
        tone="cyan"
        :available="pipeline?.available ?? true"
        :reason="pipeline?.reason"
        hint="TimescaleDB vehicle_telemetry 写入点数"
        :spark="spark.telemetry_points ?? []"
      />
      <MetricCard
        title="入库延迟 P95"
        :value="formatNumber(ingestLatencyMs ?? null, 0)"
        :unit="ingestLatencyMs != null ? 'ms' : ''"
        :tone="ingestLatencyMs != null && ingestLatencyMs > INGEST_LATENCY_SLO_MS ? 'red' : 'amber'"
        :available="pipeline?.available ?? true"
        :reason="pipeline?.reason"
        :hint="ingestLatencyHint"
        :spark="spark.ingest_latency ?? []"
      />
      <MetricCard
        title="感知帧率"
        :value="formatNumber(algorithm?.perception_fps_avg ?? null, 1)"
        unit="fps"
        tone="violet"
        :available="algorithm?.available ?? true"
        :reason="algorithm?.reason"
        hint="感知模块 FPS 均值"
        :spark="spark.perception_fps ?? []"
      />
      <MetricCard
        title="感知时延"
        :value="formatNumber(algorithm?.perception_latency_ms_avg ?? null, 0)"
        unit="ms"
        tone="cyan"
        :available="algorithm?.available ?? true"
        :reason="algorithm?.reason"
        hint="感知端到端时延均值"
        :spark="spark.perception_latency ?? []"
      />
      <MetricCard
        title="控制时延"
        :value="formatNumber(algorithm?.control_latency_ms_avg ?? null, 0)"
        unit="ms"
        :tone="(algorithm?.control_latency_ms_avg ?? 0) > 100 ? 'red' : 'green'"
        :available="algorithm?.available ?? true"
        :reason="algorithm?.reason"
        hint="远程操控指令 SLO ≤ 100 ms"
        :spark="spark.control_latency ?? []"
      />
      <MetricCard
        title="未确认事件"
        :value="events?.unacknowledged ?? 0"
        :tone="(events?.critical ?? 0) > 0 ? 'red' : 'amber'"
        :available="events?.available ?? true"
        :reason="events?.reason"
        :hint="`严重 ${events?.critical ?? 0} / 警告 ${events?.warning ?? 0} / 提示 ${events?.info ?? 0}`"
        :spark="spark.unack ?? []"
      />
    </section>

    <!-- 第二层：动态图表区 -->
    <section class="dash__charts">
      <div class="dash__panel dash__panel--half">
        <div class="dash__panel-head">车辆状态分布（8 态受控词表）</div>
        <EchartBase :option="vehicleStatusOption" :loading="loading" :not-merge="false" height="300px" />
      </div>
      <div class="dash__panel dash__panel--half">
        <div class="dash__panel-head">事件类型 Top 10</div>
        <EchartBase :option="eventTypeOption" :loading="loading" :not-merge="false" height="300px" />
      </div>
      <div class="dash__panel dash__panel--half">
        <div class="dash__panel-head">
          Kafka 消费组积压
          <span class="dash__panel-note">{{ kafkaLagNote }}</span>
        </div>
        <EchartBase :option="kafkaLagOption" :loading="loading" :not-merge="false" height="300px" />
      </div>
      <div class="dash__panel dash__panel--half">
        <div class="dash__panel-head">入库延迟趋势（轮询快照）</div>
        <EchartBase
          v-if="hasLatencyTrend"
          :option="ingestLatencyOption"
          :loading="loading"
          :not-merge="false"
          height="300px"
        />
        <div v-else class="dash__placeholder">趋势采样累积中（需 ≥ 2 次轮询）</div>
      </div>
    </section>

    <!-- 第三层：车辆明细表（点击行/按钮 → 单车抽屉） -->
    <section class="dash__panel dash__panel--full">
      <div class="dash__panel-head">车辆明细（Redis 读模型 · 点击查看单车运营详情）</div>
      <el-table
        :data="vehicleStore.vehicles"
        size="small"
        empty-text="暂无车辆数据"
        class="dash__table"
        @row-click="(row: any) => openDrilldown(row.vehicle_id)"
      >
        <el-table-column label="状态" width="72" align="center">
          <template #default="{ row }">
            <span class="dash__dot" :style="{ background: STATUS_COLORS[row.status] ?? '#64748b' }" />
          </template>
        </el-table-column>
        <el-table-column prop="vehicle_id" label="车辆 ID" width="150" />
        <el-table-column label="名称" min-width="140">
          <template #default="{ row }">{{ row.vehicle_name || row.vehicle_id }}</template>
        </el-table-column>
        <el-table-column label="运行状态" width="120">
          <template #default="{ row }">
            <StatusTag kind="vehicle" :value="row.status" />
          </template>
        </el-table-column>
        <el-table-column label="电量" width="160">
          <template #default="{ row }">
            <div class="dash__soc">
              <el-progress
                :percentage="row.battery_soc ?? 0"
                :color="socColor(row.battery_soc)"
                :stroke-width="8"
                :show-text="false"
                class="dash__soc-bar"
              />
              <span class="dash__soc-text">{{ formatSoc(row.battery_soc) }}</span>
            </div>
          </template>
        </el-table-column>
        <el-table-column label="实时速度" width="120">
          <template #default="{ row }">{{ formatNumber(row.velocity ?? null, 2) }} m/s</template>
        </el-table-column>
        <el-table-column label="最近在线" width="130">
          <template #default="{ row }">{{ row.last_online_time > 0 ? formatRelativeTime(row.last_online_time) : '尚未上线' }}</template>
        </el-table-column>
        <el-table-column label="操作" width="200" align="right">
          <template #default="{ row }">
            <el-button link type="primary" @click.stop="openDrilldown(row.vehicle_id)">查看运营详情</el-button>
          </template>
        </el-table-column>
      </el-table>
    </section>

    <VehicleDrilldownDrawer
      v-model="drawerVisible"
      :vehicle-id="activeVehicleId"
      :time-range="timeRange"
    />
  </div>
</template>

<style scoped>
/* 深色科技风：仅作用于本页根节点，全出血覆盖 layout__main 的浅色 padding */
.dash {
  --dash-cyan: #22d3ee;
  --dash-text: #cfe0f5;
  --dash-muted: #6f849f;
  --dash-panel-border: rgba(56, 189, 248, 0.16);

  position: relative;
  margin: -16px;
  padding: 20px 22px 30px;
  min-height: calc(100vh - 60px);
  color: var(--dash-text);
  background:
    radial-gradient(1200px 500px at 80% -10%, rgba(34, 211, 238, 0.12), transparent 60%),
    radial-gradient(900px 400px at 0% 0%, rgba(96, 165, 250, 0.1), transparent 55%),
    linear-gradient(160deg, #0a1120 0%, #0b1526 50%, #090f1c 100%);
  overflow: hidden;
}

.dash__bg-grid {
  position: absolute;
  inset: 0;
  background-image:
    linear-gradient(rgba(120, 150, 190, 0.05) 1px, transparent 1px),
    linear-gradient(90deg, rgba(120, 150, 190, 0.05) 1px, transparent 1px);
  background-size: 44px 44px;
  pointer-events: none;
  mask-image: radial-gradient(circle at 50% 0%, #000 30%, transparent 80%);
}

.dash__bar {
  position: relative;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
  margin-bottom: 18px;
}

.dash__brand {
  display: flex;
  align-items: baseline;
  gap: 12px;
}

.dash__title {
  font-size: 22px;
  font-weight: 700;
  letter-spacing: 1px;
  color: #eaf4ff;
  text-shadow: 0 0 20px rgba(34, 211, 238, 0.35);
}

.dash__subtitle {
  font-size: 12px;
  letter-spacing: 2px;
  text-transform: uppercase;
  color: var(--dash-muted);
}

.dash__bar-right {
  display: flex;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
}

.dash__updated {
  font-size: 12px;
  color: var(--dash-muted);
}

/* KPI 带 */
.dash__kpis {
  position: relative;
  display: grid;
  grid-template-columns: repeat(8, 1fr);
  gap: 12px;
  margin-bottom: 16px;
}

@media (max-width: 1600px) {
  .dash__kpis {
    grid-template-columns: repeat(4, 1fr);
  }
}

@media (max-width: 900px) {
  .dash__kpis {
    grid-template-columns: repeat(2, 1fr);
  }
}

/* 图表面板 */
.dash__charts {
  position: relative;
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 14px;
  margin-bottom: 16px;
}

@media (max-width: 1100px) {
  .dash__charts {
    grid-template-columns: 1fr;
  }
}

.dash__panel {
  padding: 14px 16px 10px;
  border-radius: 14px;
  border: 1px solid var(--dash-panel-border);
  background: linear-gradient(150deg, rgba(23, 37, 64, 0.72), rgba(12, 20, 38, 0.72));
  backdrop-filter: blur(6px);
  box-shadow: 0 6px 22px rgba(2, 8, 23, 0.5);
}

.dash__panel--full {
  position: relative;
}

.dash__panel-head {
  display: flex;
  align-items: center;
  gap: 10px;
  font-size: 14px;
  font-weight: 600;
  letter-spacing: 0.5px;
  color: #7fd6ff;
  margin-bottom: 10px;
  padding-left: 8px;
  border-left: 3px solid var(--dash-cyan);
}

.dash__panel-note {
  font-size: 11px;
  font-weight: 400;
  color: var(--dash-muted);
}

.dash__placeholder {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 300px;
  color: rgba(148, 197, 233, 0.55);
  font-size: 13px;
}

/* 明细表（深色） */
.dash__dot {
  display: inline-block;
  width: 10px;
  height: 10px;
  border-radius: 50%;
  box-shadow: 0 0 8px currentColor;
}

.dash__soc {
  display: flex;
  align-items: center;
  gap: 8px;
}

.dash__soc-bar {
  flex: 1;
}

.dash__soc-text {
  width: 34px;
  text-align: right;
  font-variant-numeric: tabular-nums;
}

.dash__table {
  --el-table-bg-color: transparent;
  --el-table-tr-bg-color: transparent;
  --el-table-header-bg-color: rgba(20, 33, 56, 0.6);
  --el-table-text-color: var(--dash-text);
  --el-table-header-text-color: #9fb6d6;
  --el-table-border-color: rgba(120, 150, 190, 0.14);
  --el-table-row-hover-bg-color: rgba(34, 211, 238, 0.08);
  cursor: pointer;
}
</style>
