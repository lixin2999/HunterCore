<script setup lang="ts">
/**
 * 指标卡片（看板/评估页复用）
 * 数值单位与阈值提示由调用方传入，避免在此硬编码业务阈值。
 */
withDefaults(
  defineProps<{
    title: string
    value: string | number
    /** 单位（如 %, ms, m/s） */
    unit?: string
    /** 次要说明（如 SLO 阈值、统计口径） */
    hint?: string
    /** 主题色：primary/success/warning/danger/info */
    tone?: 'primary' | 'success' | 'warning' | 'danger' | 'info'
    /** 是否可用（false 时展示降级说明，用于 Flink/Spark 结果缺失） */
    available?: boolean
    /** 不可用原因 */
    reason?: string | null
  }>(),
  { unit: '', hint: '', tone: 'primary', available: true, reason: null },
)
</script>

<template>
  <el-card class="stat-card" shadow="hover">
    <div class="stat-card__header">
      <span class="stat-card__title">{{ title }}</span>
      <el-tag v-if="!available" type="warning" size="small" effect="plain">数据不可用</el-tag>
    </div>
    <div class="stat-card__value" :class="`stat-card__value--${tone}`">
      <span v-if="available">{{ value }}</span>
      <span v-else class="stat-card__value--muted">--</span>
      <span v-if="available && unit" class="stat-card__unit">{{ unit }}</span>
    </div>
    <div class="stat-card__hint">
      {{ !available && reason ? reason : hint }}
    </div>
  </el-card>
</template>

<style scoped>
.stat-card__header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.stat-card__title {
  font-size: 13px;
  color: var(--el-text-color-secondary);
}

.stat-card__value {
  font-size: 26px;
  font-weight: 600;
  line-height: 1.5;
}

.stat-card__value--primary {
  color: var(--el-color-primary);
}

.stat-card__value--success {
  color: var(--el-color-success);
}

.stat-card__value--warning {
  color: var(--el-color-warning);
}

.stat-card__value--danger {
  color: var(--el-color-danger);
}

.stat-card__value--info {
  color: var(--el-text-color-primary);
}

.stat-card__value--muted {
  color: var(--el-text-color-placeholder);
  font-size: 20px;
}

.stat-card__unit {
  font-size: 13px;
  margin-left: 4px;
  color: var(--el-text-color-secondary);
}

.stat-card__hint {
  margin-top: 6px;
  min-height: 18px;
  font-size: 12px;
  color: var(--el-text-color-placeholder);
}
</style>
