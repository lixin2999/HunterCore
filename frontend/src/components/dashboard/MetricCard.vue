<script setup lang="ts">
/**
 * 看板指标卡（深色科技风）
 *
 * 仅用于运营看板（.dashboard-scope 内），与全局浅色 StatCard 区分。
 * 数值/单位/阈值提示由调用方传入，禁止在此硬编码业务阈值；
 * available=false 时展示降级原因（Flink/Spark 结果缺失），不臆造字段。
 */
import MiniSparkline from '@/components/charts/MiniSparkline.vue'

const props = withDefaults(
  defineProps<{
    title: string
    value: string | number
    /** 单位（如 %、ms、m/s） */
    unit?: string
    /** 次要说明（SLO 阈值 / 统计口径 / 降级原因） */
    hint?: string
    /** 主题色（决定发光与 sparkline 配色） */
    tone?: 'cyan' | 'green' | 'amber' | 'red' | 'violet' | 'blue'
    /** 是否可用（false 时展示降级说明） */
    available?: boolean
    /** 不可用原因 */
    reason?: string | null
    /** 迷你趋势序列 */
    spark?: number[]
    /** 右上角徽标文案（如「在线率 100%」） */
    badge?: string
  }>(),
  {
    unit: '',
    hint: '',
    tone: 'cyan',
    available: true,
    reason: null,
    spark: () => [],
    badge: '',
  },
)

const TONE_COLORS: Record<string, string> = {
  cyan: '#22d3ee',
  green: '#34d399',
  amber: '#fbbf24',
  red: '#f87171',
  violet: '#a78bfa',
  blue: '#60a5fa',
}

const accent = () => TONE_COLORS[props.tone] ?? TONE_COLORS.cyan
</script>

<template>
  <div class="metric-card" :style="{ '--metric-accent': accent() }">
    <div class="metric-card__glow" />
    <div class="metric-card__head">
      <span class="metric-card__title">{{ title }}</span>
      <span v-if="badge && available" class="metric-card__badge">{{ badge }}</span>
      <span v-else-if="!available" class="metric-card__badge metric-card__badge--warn">数据不可用</span>
    </div>
    <div class="metric-card__value">
      <template v-if="available">
        <span class="metric-card__num">{{ value }}</span>
        <span v-if="unit" class="metric-card__unit">{{ unit }}</span>
      </template>
      <span v-else class="metric-card__num metric-card__num--muted">--</span>
    </div>
    <MiniSparkline
      v-if="available && spark.length > 1"
      :data="spark"
      :color="accent()"
      :height="34"
    />
    <div class="metric-card__hint">{{ !available && reason ? reason : hint }}</div>
  </div>
</template>

<style scoped>
.metric-card {
  position: relative;
  overflow: hidden;
  height: 100%;
  padding: 16px 18px 14px;
  border-radius: 14px;
  border: 1px solid rgba(56, 189, 248, 0.16);
  background: linear-gradient(150deg, rgba(23, 37, 64, 0.92), rgba(12, 20, 38, 0.92));
  backdrop-filter: blur(6px);
  box-shadow: 0 6px 22px rgba(2, 8, 23, 0.55);
  transition: transform 0.25s ease, border-color 0.25s ease, box-shadow 0.25s ease;
}

.metric-card:hover {
  transform: translateY(-2px);
  border-color: color-mix(in srgb, var(--metric-accent) 55%, transparent);
  box-shadow: 0 10px 30px rgba(2, 8, 23, 0.7);
}

.metric-card__glow {
  position: absolute;
  top: -40px;
  right: -30px;
  width: 120px;
  height: 120px;
  border-radius: 50%;
  background: radial-gradient(circle, color-mix(in srgb, var(--metric-accent) 40%, transparent), transparent 70%);
  opacity: 0.5;
  pointer-events: none;
}

.metric-card__head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.metric-card__title {
  font-size: 13px;
  letter-spacing: 0.5px;
  color: #9fb6d6;
}

.metric-card__badge {
  font-size: 11px;
  padding: 1px 8px;
  border-radius: 10px;
  color: var(--metric-accent);
  background: color-mix(in srgb, var(--metric-accent) 14%, transparent);
  border: 1px solid color-mix(in srgb, var(--metric-accent) 30%, transparent);
  white-space: nowrap;
}

.metric-card__badge--warn {
  color: #fbbf24;
  background: rgba(251, 191, 36, 0.12);
  border-color: rgba(251, 191, 36, 0.3);
}

.metric-card__value {
  display: flex;
  align-items: baseline;
  gap: 4px;
  margin: 10px 0 6px;
}

.metric-card__num {
  font-size: 30px;
  font-weight: 700;
  line-height: 1.1;
  color: #eaf4ff;
  text-shadow: 0 0 18px color-mix(in srgb, var(--metric-accent) 55%, transparent);
  font-variant-numeric: tabular-nums;
}

.metric-card__num--muted {
  color: #5b6b82;
  text-shadow: none;
}

.metric-card__unit {
  font-size: 13px;
  color: #8aa2c0;
}

.metric-card__hint {
  margin-top: 6px;
  min-height: 16px;
  font-size: 11.5px;
  color: #6f849f;
}
</style>
