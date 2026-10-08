<script setup lang="ts">
/**
 * 一次性凭证展示组件（BundleDownloadNotice）
 *
 * 用途：承载 SCRAM 口令等一次性敏感字段展示（服务端仅在响应中出现一次，
 *      未保存只能重新 rotate-scram 才能再次获取）。
 *
 * 交互规范：
 * - 醒目警告（type=warning + 图标）
 * - 提供"复制到剪贴板"与"下载完整 kafka.properties 文本"两个动作
 * - 关闭前必须"我已安全保存"复选框为真（禁用"确认"按钮）—— 与 G-06 首登强制改密一致
 */
import { computed, ref, watch } from 'vue'
import { ElMessage } from 'element-plus'

const props = withDefaults(
  defineProps<{
    /** 一次性口令（明文，父组件负责在 close 事件后清理） */
    password: string
    /** 展示标题（默认"车端 SCRAM 口令（一次性）"） */
    title?: string
    /** Kafka properties 全文（含占位符 <SCRAM_PASSWORD>；提供则展示"复制 kafka.properties"） */
    kafkaProperties?: string
    /** 用户名（= vehicle_id）；用于 kafka.properties 模板替换 */
    username?: string
    /** 关闭时是否强制要求确认已保存（默认 true） */
    requireAck?: boolean
  }>(),
  { title: '车端 SCRAM 口令（一次性）', requireAck: true },
)

const emit = defineEmits<{
  (e: 'close'): void
}>()

const acknowledged = ref(false)
const visible = ref(true)

// 父组件重新触发（口令轮换 / 新车开通）时复位
watch(
  () => props.password,
  () => {
    acknowledged.value = false
  },
)

const canClose = computed(() => !props.requireAck || acknowledged.value)

async function copyToClipboard(text: string, hint: string): Promise<void> {
  if (!text) {
    ElMessage.warning('内容为空')
    return
  }
  try {
    await navigator.clipboard.writeText(text)
    ElMessage.success(`${hint}已复制到剪贴板`)
  } catch {
    ElMessage.error('浏览器剪贴板不可用，请手动选中复制')
  }
}

/** 将 kafka.properties 模板中的占位符替换成实际口令与用户名 */
const resolvedProperties = computed(() => {
  if (!props.kafkaProperties) return ''
  return props.kafkaProperties
    .replaceAll('<SCRAM_PASSWORD>', props.password)
    .replaceAll('<SCRAM_USERNAME>', props.username ?? '')
})

function handleClose(): void {
  if (!canClose.value) {
    ElMessage.warning('请先勾选"我已安全保存"再关闭')
    return
  }
  visible.value = false
  emit('close')
}
</script>

<template>
  <el-alert
    v-if="visible && password"
    class="bundle-notice"
    type="warning"
    :closable="false"
    show-icon
  >
    <template #title>
      <span class="bundle-notice__title">{{ title }}</span>
    </template>
    <div class="bundle-notice__body">
      <div class="bundle-notice__password" data-testid="scram-password">{{ password }}</div>
      <div class="bundle-notice__actions">
        <el-button size="small" type="primary" @click="copyToClipboard(password, 'SCRAM 口令')">
          复制口令
        </el-button>
        <el-button
          v-if="resolvedProperties"
          size="small"
          @click="copyToClipboard(resolvedProperties, 'kafka.properties 全文')"
        >
          复制 kafka.properties
        </el-button>
      </div>
      <el-divider />
      <p class="bundle-notice__warn">
        ⚠ 该口令仅在开通/重置响应中一次性下发，服务端与数据库均不保留明文。请立即保存到车端安全存储；
        遗失需再次"重发 SCRAM 口令"并同步替换 kafka.properties。
      </p>
      <el-checkbox v-model="acknowledged" :disabled="!requireAck">我已安全保存（关闭后不再展示）</el-checkbox>
      <div class="bundle-notice__footer">
        <el-button type="primary" :disabled="!canClose" @click="handleClose">确认关闭</el-button>
      </div>
    </div>
  </el-alert>
</template>

<style scoped>
.bundle-notice {
  margin-bottom: 12px;
}

.bundle-notice__title {
  font-weight: 600;
}

.bundle-notice__body {
  margin-top: 4px;
}

.bundle-notice__password {
  font-family: 'Consolas', 'Courier New', monospace;
  font-size: 15px;
  padding: 8px 12px;
  background: #fff8e1;
  border: 1px dashed #e6a23c;
  border-radius: 4px;
  user-select: all;
  word-break: break-all;
}

.bundle-notice__actions {
  margin-top: 8px;
  display: flex;
  gap: 8px;
}

.bundle-notice__warn {
  font-size: 13px;
  color: var(--el-text-color-regular);
  margin: 0 0 8px;
  line-height: 1.5;
}

.bundle-notice__footer {
  margin-top: 12px;
  text-align: right;
}
</style>
