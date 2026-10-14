<script setup lang="ts">
/**
 * 迷你趋势线（SVG，零依赖）
 *
 * 用途：看板 KPI 卡内的 sparkline（轮询快照在客户端累积的环形缓冲）。
 * 说明：不引入 ECharts 实例，避免每卡片一个图表的重开销；纯展示，不参与业务判定。
 */
import { computed } from 'vue'

const props = withDefaults(
  defineProps<{
    /** 数值序列（按时间升序，最新在右） */
    data: number[]
    /** 主色（描边 + 渐变填充） */
    color?: string
    /** 高度（像素） */
    height?: number
    /** 空数据占位文案 */
    emptyText?: string
  }>(),
  { color: '#38bdf8', height: 40, emptyText: '暂无趋势' },
)

/** viewBox 归一化坐标系（宽 100 · 高按 props.height 等比） */
const VB_WIDTH = 100

const geometry = computed<{ line: string; area: string; last: [number, number] | null }>(() => {
  const values = props.data.filter((item) => Number.isFinite(item))
  const vbHeight = props.height
  if (values.length < 2) {
    return { line: '', area: '', last: null }
  }
  const min = Math.min(...values)
  const max = Math.max(...values)
  const span = max - min || 1
  const stepX = VB_WIDTH / (values.length - 1)
  const points = values.map((value, index) => {
    const x = index * stepX
    const y = vbHeight - ((value - min) / span) * (vbHeight - 6) - 3
    return [x, y] as [number, number]
  })
  const line = points
    .map(([x, y], index) => `${index === 0 ? 'M' : 'L'}${x.toFixed(2)},${y.toFixed(2)}`)
    .join(' ')
  const area = `${line} L${VB_WIDTH},${vbHeight} L0,${vbHeight} Z`
  return { line, area, last: points[points.length - 1] ?? null }
})

/** 每实例唯一渐变 id（避免多卡片 SVG defs 冲突） */
const gradientId = computed(() => `spark-${Math.random().toString(36).slice(2, 9)}`)
</script>

<template>
  <div class="sparkline" :style="{ height: `${height}px` }">
    <svg
      v-if="geometry.line"
      :viewBox="`0 0 ${VB_WIDTH} ${height}`"
      preserveAspectRatio="none"
      class="sparkline__svg"
    >
      <defs>
        <linearGradient :id="gradientId" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" :stop-color="color" stop-opacity="0.42" />
          <stop offset="100%" :stop-color="color" stop-opacity="0" />
        </linearGradient>
      </defs>
      <path :d="geometry.area" :fill="`url(#${gradientId})`" />
      <path :d="geometry.line" fill="none" :stroke="color" stroke-width="1.6" vector-effect="non-scaling-stroke" />
      <circle
        v-if="geometry.last"
        :cx="geometry.last[0]"
        :cy="geometry.last[1]"
        r="1.8"
        :fill="color"
        vector-effect="non-scaling-stroke"
      />
    </svg>
    <span v-else class="sparkline__empty">{{ emptyText }}</span>
  </div>
</template>

<style scoped>
.sparkline {
  position: relative;
  width: 100%;
  display: flex;
  align-items: flex-end;
}

.sparkline__svg {
  width: 100%;
  height: 100%;
  display: block;
}

.sparkline__empty {
  font-size: 11px;
  color: rgba(148, 197, 233, 0.5);
  align-self: center;
}
</style>
