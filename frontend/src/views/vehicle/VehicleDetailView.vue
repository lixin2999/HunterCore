<script setup lang="ts">
/**
 * 车辆详情（vehicle-service）
 *
 * 契约：GET /api/v1/vehicle/{vehicle_id}
 *      展示：台账基本信息 + 接入配置摘要 + 4 步 provisioning 时间线 + 行内动作（下载 bundle / 重发口令 / 重发证书 / 修改台账）
 * 路由参数：vehicle_id（对齐 RESTful 路径段）
 */
import { computed, onMounted, reactive, ref } from 'vue'
import { ElMessage, ElMessageBox, type FormInstance, type FormRules } from 'element-plus'
import { useRoute } from 'vue-router'

import {
  downloadBundle,
  getVehicle,
  patchVehicle,
  reissueCert,
  rotateScram,
} from '@/api/vehicle'
import StatusTag from '@/components/common/StatusTag.vue'
import BundleDownloadNotice from './BundleDownloadNotice.vue'
import {
  PROVISION_STATE_LABELS,
  PROVISION_STATE_TAG_TYPES,
  PROVISION_STEP_LABELS,
  PROVISION_STEP_ORDER,
  PROVISION_STEP_STATE_LABELS,
  PROVISION_STEP_STATE_TAG_TYPES,
} from '@/constants/vehicle'
import { PERMISSIONS } from '@/constants/permissions'
import type { ProvisionStep, VehicleDetail, VehicleUpdateRequest } from '@/types/vehicle'
import { HunterApiError } from '@/utils/error-code'
import { formatDateTimeString, formatRelativeTime } from '@/utils/format'

const route = useRoute()
const vehicleId = String(route.params.vehicle_id ?? '')

const loading = ref(false)
const detail = ref<VehicleDetail | null>(null)

async function load(): Promise<void> {
  if (!vehicleId) {
    ElMessage.error('缺少 vehicle_id')
    return
  }
  loading.value = true
  try {
    detail.value = await getVehicle(vehicleId)
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '车辆详情加载失败')
  } finally {
    loading.value = false
  }
}

onMounted(load)

/* ------------------------------ 时间线：4 步固定顺序 ------------------------------ */
interface TimelineItem {
  name: string
  label: string
  state: string
  stateLabel: string
  stateTag: string
  error?: string | null
  ts?: number | null
}

const timeline = computed<TimelineItem[]>(() => {
  const row = detail.value
  if (!row) return []
  const map = new Map<string, ProvisionStep>()
  for (const s of row.provision_steps ?? []) map.set(s.name, s)
  return PROVISION_STEP_ORDER.map((name) => {
    const step = map.get(name) ?? ({ name, state: 'pending', error: null, ts: null } as ProvisionStep)
    return {
      name,
      label: PROVISION_STEP_LABELS[name] ?? name,
      state: step.state,
      stateLabel: PROVISION_STEP_STATE_LABELS[step.state] ?? step.state,
      stateTag: PROVISION_STEP_STATE_TAG_TYPES[step.state] ?? 'info',
      error: step.error,
      ts: step.ts,
    }
  })
})

const provisionStateTag = computed(() => {
  const v = detail.value?.provision_state ?? ''
  return PROVISION_STATE_TAG_TYPES[v] ?? 'info'
})
const provisionStateLabel = computed(() => {
  const v = detail.value?.provision_state ?? ''
  return PROVISION_STATE_LABELS[v] ?? v
})

/* ------------------------------ 一次性口令（重发 SCRAM 后展示） ------------------------------ */
const revealedPassword = ref('')
const kafkaPropertiesPreview = computed(() => {
  const v = detail.value
  if (!v || !revealedPassword.value) return ''
  return [
    `bootstrap.servers=${v.kafka_bootstrap}`,
    `security.protocol=SASL_SSL`,
    `sasl.mechanism=SCRAM-SHA-512`,
    `sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \\`,
    `  username="${v.scram_username}" \\`,
    `  password="<SCRAM_PASSWORD>";`,
    `ssl.truststore.location=./ca-cert.pem`,
    `ssl.keystore.type=PKCS12`,
    `ssl.keystore.location=./kafka-client.p12`,
    `client.id=${v.vehicle_id}`,
  ].join('\n')
})

async function handleRotate(): Promise<void> {
  try {
    await ElMessageBox.confirm(
      `将为 ${vehicleId} 生成新的 SCRAM 口令；旧口令立即失效，车端需同步替换 kafka.properties。是否继续？`,
      '重发 SCRAM 口令',
      { type: 'warning', confirmButtonText: '确认重置', cancelButtonText: '取消' },
    )
  } catch {
    return
  }
  try {
    const result = await rotateScram(vehicleId)
    revealedPassword.value = result.scram_password
    await load()
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : 'SCRAM 口令重置失败')
  }
}

function handlePasswordClose(): void {
  revealedPassword.value = ''
}

/* ------------------------------ 下载 bundle / 重发证书 ------------------------------ */
const downloading = ref(false)

async function handleDownload(): Promise<void> {
  downloading.value = true
  try {
    const { blob, filename } = await downloadBundle(vehicleId)
    const url = URL.createObjectURL(blob)
    const anchor = document.createElement('a')
    anchor.href = url
    anchor.download = filename
    anchor.rel = 'noopener'
    document.body.appendChild(anchor)
    anchor.click()
    anchor.remove()
    URL.revokeObjectURL(url)
    ElMessage.success(`已下载 ${filename}`)
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '接入包下载失败')
  } finally {
    downloading.value = false
  }
}

async function handleReissueCert(): Promise<void> {
  try {
    await ElMessageBox.confirm(
      '将重新签发本车的 mTLS 客户端证书（旧证书仍在 CA 有效期内，本 MVP 不实现 CRL）。是否继续？',
      '重发客户端证书',
      { type: 'warning', confirmButtonText: '确认重签', cancelButtonText: '取消' },
    )
  } catch {
    return
  }
  try {
    const result = await reissueCert(vehicleId)
    ElMessage.success(`证书已重签（新序列号 ${result.device_cert_sn}）`)
    await load()
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '证书重签失败')
  }
}

/* ------------------------------ 修改台账基础字段 ------------------------------ */
const editVisible = ref(false)
const editFormRef = ref<FormInstance>()
const editSubmitting = ref(false)
const editForm = reactive<VehicleUpdateRequest>({
  vehicle_name: '',
  model: '',
  firmware_version: '',
  software_version: '',
  description: '',
})

const editRules: FormRules<VehicleUpdateRequest> = {
  vehicle_name: [{ max: 64, message: '名称长度 ≤ 64', trigger: 'blur' }],
  description: [{ max: 500, message: '描述长度 ≤ 500', trigger: 'blur' }],
}

function openEdit(): void {
  const v = detail.value
  if (!v) return
  Object.assign(editForm, {
    vehicle_name: v.vehicle_name,
    model: v.model,
    firmware_version: v.firmware_version ?? '',
    software_version: v.software_version ?? '',
    description: v.description ?? '',
  })
  editVisible.value = true
}

async function submitEdit(): Promise<void> {
  const inst = editFormRef.value
  if (!inst) return
  try {
    await inst.validate()
  } catch {
    return
  }
  editSubmitting.value = true
  try {
    await patchVehicle(vehicleId, { ...editForm })
    ElMessage.success('台账信息已更新')
    editVisible.value = false
    await load()
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '更新失败')
  } finally {
    editSubmitting.value = false
  }
}

/* ------------------------------ fence_json 展示 ------------------------------ */
const fenceText = computed(() => {
  const v = detail.value
  if (!v?.fence_json) return '-'
  try {
    return JSON.stringify(v.fence_json, null, 2)
  } catch {
    return String(v.fence_json)
  }
})
</script>

<template>
  <div v-loading="loading" class="detail">
    <el-page-header content="车辆详情" @back="() => $router.back()">
      <template #extra>
        <div class="actions">
          <el-button v-permission="PERMISSIONS.vehicleUpdate" @click="openEdit">修改台账</el-button>
          <el-button
            v-permission="PERMISSIONS.vehicleExecute"
            type="warning"
            @click="handleRotate"
          >
            重发 SCRAM 口令
          </el-button>
          <el-button
            v-permission="PERMISSIONS.vehicleExecute"
            type="warning"
            @click="handleReissueCert"
          >
            重发证书
          </el-button>
          <el-button
            v-permission="PERMISSIONS.vehicleExecute"
            type="primary"
            :loading="downloading"
            @click="handleDownload"
          >
            下载接入包
          </el-button>
        </div>
      </template>
    </el-page-header>

    <template v-if="detail">
      <!-- 一次性口令展示（重发 SCRAM 后弹出） -->
      <BundleDownloadNotice
        v-if="revealedPassword"
        :password="revealedPassword"
        :username="detail.scram_username"
        :kafka-properties="kafkaPropertiesPreview"
        title="新 SCRAM 口令（一次性）"
        @close="handlePasswordClose"
      />

      <!-- 基本信息卡 -->
      <el-card shadow="never" class="card">
        <template #header>
          <div class="card__header">
            <span>基本信息</span>
            <el-tag :type="provisionStateTag as never" size="small">{{ provisionStateLabel }}</el-tag>
          </div>
        </template>
        <el-descriptions :column="3" border size="small">
          <el-descriptions-item label="车辆 ID">{{ detail.vehicle_id }}</el-descriptions-item>
          <el-descriptions-item label="名称">{{ detail.vehicle_name }}</el-descriptions-item>
          <el-descriptions-item label="型号">{{ detail.model }}</el-descriptions-item>
          <el-descriptions-item label="运行状态">
            <StatusTag kind="vehicle" :value="detail.status" />
          </el-descriptions-item>
          <el-descriptions-item label="固件版本">{{ detail.firmware_version || '-' }}</el-descriptions-item>
          <el-descriptions-item label="软件版本">{{ detail.software_version || '-' }}</el-descriptions-item>
          <el-descriptions-item label="注册时间">{{ formatDateTimeString(detail.register_time) }}</el-descriptions-item>
          <el-descriptions-item label="最近在线">
            {{ detail.last_online_time ? formatRelativeTime(Date.parse(detail.last_online_time) / 1000) : '尚未上线' }}
          </el-descriptions-item>
          <el-descriptions-item label="证书序列号">
            <code>{{ detail.device_cert_sn || '-' }}</code>
          </el-descriptions-item>
          <el-descriptions-item label="备注" :span="3">{{ detail.description || '-' }}</el-descriptions-item>
          <el-descriptions-item label="地理围栏" :span="3">
            <pre class="fence">{{ fenceText }}</pre>
          </el-descriptions-item>
        </el-descriptions>
      </el-card>

      <!-- 接入配置摘要 -->
      <el-card shadow="never" class="card">
        <template #header>车端 Kafka 接入摘要</template>
        <el-descriptions :column="1" border size="small">
          <el-descriptions-item label="Bootstrap">
            <code>{{ detail.kafka_bootstrap }}</code>
            <span class="hint">（SASL_SSL · 9093）</span>
          </el-descriptions-item>
          <el-descriptions-item label="SCRAM 用户名">
            <code>{{ detail.scram_username }}</code>
            <span class="hint">（= vehicle_id；口令一次性下发不缓存）</span>
          </el-descriptions-item>
          <el-descriptions-item label="Topic 清单">
            <el-tag
              v-for="topic in detail.topics_preview"
              :key="topic"
              size="small"
              effect="plain"
              class="topic-tag"
            >
              {{ topic }}
            </el-tag>
            <div class="hint">共 {{ detail.topics_preview.length }} 个（telemetry/event/health/command/command_result/ota_notify/ota_status/remote_control）</div>
          </el-descriptions-item>
        </el-descriptions>
      </el-card>

      <!-- Provisioning 时间线 -->
      <el-card shadow="never" class="card">
        <template #header>Provisioning 步骤（DB · SCRAM · Topic · Cert）</template>
        <el-timeline>
          <el-timeline-item
            v-for="step in timeline"
            :key="step.name"
            :type="step.stateTag as never"
            :timestamp="step.ts ? formatDateTimeString(new Date(step.ts * 1000).toISOString()) : '-'"
          >
            <div class="step-line">
              <span class="step-line__label">{{ step.label }}</span>
              <el-tag :type="step.stateTag as never" size="small">{{ step.stateLabel }}</el-tag>
            </div>
            <div v-if="step.error" class="step-line__error">{{ step.error }}</div>
          </el-timeline-item>
        </el-timeline>
      </el-card>
    </template>

    <!-- 修改台账对话框 -->
    <el-dialog v-model="editVisible" title="修改台账基础字段" width="520px">
      <el-form ref="editFormRef" :model="editForm" :rules="editRules" label-width="100px">
        <el-form-item label="名称" prop="vehicle_name">
          <el-input v-model="editForm.vehicle_name" maxlength="64" show-word-limit />
        </el-form-item>
        <el-form-item label="型号">
          <el-input v-model="editForm.model" maxlength="32" />
        </el-form-item>
        <el-form-item label="固件版本">
          <el-input v-model="editForm.firmware_version" maxlength="32" />
        </el-form-item>
        <el-form-item label="软件版本">
          <el-input v-model="editForm.software_version" maxlength="32" />
        </el-form-item>
        <el-form-item label="备注" prop="description">
          <el-input v-model="editForm.description" type="textarea" :rows="3" maxlength="500" show-word-limit />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="editVisible = false">取消</el-button>
        <el-button type="primary" :loading="editSubmitting" @click="submitEdit">保存</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<style scoped>
.detail {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.actions {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

.card {
  border-radius: 6px;
}

.card__header {
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.fence {
  margin: 0;
  font-size: 12px;
  color: var(--el-text-color-secondary);
  max-height: 200px;
  overflow: auto;
  white-space: pre-wrap;
  word-break: break-all;
}

.topic-tag {
  margin: 0 4px 4px 0;
}

.hint {
  margin-left: 8px;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}

.step-line {
  display: flex;
  align-items: center;
  gap: 8px;
}

.step-line__label {
  font-weight: 600;
}

.step-line__error {
  margin-top: 4px;
  font-size: 12px;
  color: var(--el-color-danger);
  word-break: break-all;
}
</style>
