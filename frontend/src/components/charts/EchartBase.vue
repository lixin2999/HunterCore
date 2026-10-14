<script setup lang="ts">
/**
 * ECharts 通用封装
 *
 * 说明：图表配置（option）由业务页构造，本组件只负责生命周期、resize 与 loading；
 * 阈值线/坐标轴口径必须引用常量（constants/analysis.ts）而非就地硬编码。
 */
import { onBeforeUnmount, onMounted, ref, watch } from 'vue'
import * as echarts from 'echarts'
import type { ECharts, EChartsOption } from 'echarts'

const props = withDefaults(
  defineProps<{
    option: EChartsOption
    height?: string
    loading?: boolean
    /**
     * 更新时是否替换而非合并（默认 true，保持既有页面行为）。
     * 看板动态图表传 false：增量合并 + 动画，轮询刷新时数据平滑过渡而非闪断重绘。
     */
    notMerge?: boolean
  }>(),
  { height: '320px', loading: false, notMerge: true },
)

const container = ref<HTMLDivElement | null>(null)
let chart: ECharts | null = null
let resizeObserver: ResizeObserver | null = null

onMounted(() => {
  if (!container.value) {
    return
  }
  chart = echarts.init(container.value)
  chart.setOption(props.option)
  resizeObserver = new ResizeObserver(() => chart?.resize())
  resizeObserver.observe(container.value)
})

watch(
  () => props.option,
  (option) => {
    chart?.setOption(option, { notMerge: props.notMerge })
  },
  { deep: true },
)

watch(
  () => props.loading,
  (loading) => {
    if (loading) {
      chart?.showLoading('default', { text: '加载中' })
    } else {
      chart?.hideLoading()
    }
  },
)

onBeforeUnmount(() => {
  resizeObserver?.disconnect()
  resizeObserver = null
  chart?.dispose()
  chart = null
})
</script>

<template>
  <div ref="container" class="echart" :style="{ height }" />
</template>

<style scoped>
.echart {
  width: 100%;
}
</style>
