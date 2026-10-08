<script setup lang="ts">
/**
 * 车辆一键开通向导（VehicleCreateDialog）
 *
 * 契约：POST /api/v1/vehicle（4 步：DB → SCRAM → Topics → Cert）
 * 交互流程：
 *  - Step 1：录入台账基础字段（vehicle_id/name/model/desc/fence）+ takeover_existing 开关
 *  - Step 2：一次性展示 SCRAM 口令 + 下载 bundle 按钮 + 复制 kafka.properties
 *  - Step 3：完成提示，关闭向导；父组件刷新列表
 *
 * 安全：scram_password 只保留在本组件内存中；父组件 close 后即清空；不落 localStorage/sessionStorage。
 */
import { reactive, ref, watch } from 'vue'
import { ElMessage, type FormInstance, type FormRules } from 'element-plus'

import { createVehicle, downloadBundle } from '@/api/vehicle'
import BundleDownloadNotice from './BundleDownloadNotice.vue'
import {
  PER_VEHICLE_TOPIC_COUNT,
  VEHICLE_ID_STRICT_PATTERN,
  VEHICLE_MODEL_OPTIONS,
} from '@/constants/vehicle'
import type { ProvisionResult, VehicleCreateRequest } from '@/types/vehicle'
import { HunterApiError } from '@/utils/error-code'

const props = defineProps<{ modelValue: boolean }>()
const emit = defineEmits<{
  (e: 'update:modelValue', value: boolean): void
  (e: 'created', result: ProvisionResult): void
}>()

type Step = 1 | 2 | 3
const currentStep = ref<Step>(1)
const submitting = ref(false)
const downloading = ref(false)

const formRef = ref<FormInstance>()
const form = reactive<VehicleCreateRequest>({
  vehicle_id: '',
  vehicle_name: '',
  model: 'HUNTER_SE',
  firmware_version: '',
  software_version: '',
  description: '',
  fence_json: null,
  takeover_existing: false,
})

const rules: FormRules<VehicleCreateRequest> = {
  vehicle_id: [
    { required: true, message: '请输入车辆 ID（如 HUNTER-001）', trigger: 'blur' },
    {
      pattern: VEHICLE_ID_STRICT_PATTERN,
      message: '仅允许字母/数字/下划线/短横线，长度 1–32',
      trigger: 'blur',
    },
  ],
  vehicle_name: [
    { required: true, message: '请输入车辆名称', trigger: 'blur' },
    { max: 64, message: '名称长度 ≤ 64', trigger: 'blur' },
  ],
  description: [{ max: 500, message: '描述长度 ≤ 500', trigger: 'blur' }],
}

/** 一次性口令结果（Step 2 展示；关闭对话框后清空） */
const provisionResult = ref<ProvisionResult | null>(null)

/** Step 2 提供 kafka.properties 模板（前端预览文本；正式模板在服务端 bundle 内） */
const kafkaPropertiesPreview = ref<string>('')

function buildKafkaPropertiesPreview(result: ProvisionResult): string {
  const v = result.vehicle
  return [
    `# Kafka 车端接入配置（预览；以 bundle/kafka.properties 为准）`,
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
}

watch(
  () => props.modelValue,
  (open) => {
    if (open) {
      currentStep.value = 1
      provisionResult.value = null
      kafkaPropertiesPreview.value = ''
    }
  },
)

async function handleSubmit(): Promise<void> {
  const inst = formRef.value
  if (!inst) return
  try {
    await inst.validate()
  } catch {
    return
  }
  submitting.value = true
  try {
    const result = await createVehicle({ ...form })
    provisionResult.value = result
    kafkaPropertiesPreview.value = buildKafkaPropertiesPreview(result)
    currentStep.value = 2
    ElMessage.success(`车辆 ${result.vehicle.vehicle_id} 已完成 4 步 provisioning`)
    emit('created', result)
  } catch (error) {
    if (error instanceof HunterApiError) {
      // 3002 已存在 → 提示改用 takeover_existing；4002 Kafka 冲突同上
      if (error.code === 3002 || error.code === 4002) {
        ElMessage.error(`${error.message}（如为历史遗留资源，可勾选"接管已有 SCRAM/Topic/证书"重试）`)
      } else {
        ElMessage.error(error.message)
      }
    } else {
      ElMessage.error((error as Error).message || '开通失败')
    }
  } finally {
    submitting.value = false
  }
}

async function handleDownloadBundle(): Promise<void> {
  const result = provisionResult.value
  if (!result) {
    ElMessage.warning('尚未完成开通')
    return
  }
  downloading.value = true
  try {
    const { blob, filename } = await downloadBundle(result.vehicle.vehicle_id)
    const url = URL.createObjectURL(blob)
    const anchor = document.createElement('a')
    anchor.href = url
    anchor.download = filename
    anchor.rel = 'noopener'
    document.body.appendChild(anchor)
    anchor.click()
    anchor.remove()
    URL.revokeObjectURL(url)
    ElMessage.success(`已下载 ${filename}（内含 ca-cert/client-cert/client-key/p12/kafka.properties/README）`)
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '接入包下载失败')
  } finally {
    downloading.value = false
  }
}

function handleNoticeClose(): void {
  currentStep.value = 3
  // 一次性口令在 Step 3 后彻底清空（父组件刷新列表，本对话框随后关闭）
  provisionResult.value = null
  kafkaPropertiesPreview.value = ''
}

function handleFinish(): void {
  emit('update:modelValue', false)
}

function handleCancel(): void {
  emit('update:modelValue', false)
}
</script>

<template>
  <el-dialog
    :model-value="modelValue"
    title="一键开通车辆（DB · SCRAM · Topic · 证书）"
    width="720px"
    :close-on-click-modal="false"
    :close-on-press-escape="currentStep !== 2"
    :show-close="currentStep !== 2"
    @update:model-value="(v: boolean) => emit('update:modelValue', v)"
  >
    <el-steps :active="currentStep - 1" align-center finish-status="success" class="steps">
      <el-step title="填写台账信息" />
      <el-step title="保存 SCRAM 口令" />
      <el-step title="下载接入包 / 完成" />
    </el-steps>

    <!-- Step 1：录入 -->
    <div v-show="currentStep === 1" class="step-body">
      <el-form ref="formRef" :model="form" :rules="rules" label-width="120px">
        <el-form-item label="车辆 ID" prop="vehicle_id">
          <el-input v-model="form.vehicle_id" placeholder="如 HUNTER-001（= SCRAM 用户名 = 证书 CN = Topic 前缀）" />
        </el-form-item>
        <el-form-item label="车辆名称" prop="vehicle_name">
          <el-input v-model="form.vehicle_name" placeholder="展示名，如 一号车" maxlength="64" show-word-limit />
        </el-form-item>
        <el-form-item label="型号">
          <el-select v-model="form.model" filterable allow-create default-first-option style="width: 100%">
            <el-option v-for="opt in VEHICLE_MODEL_OPTIONS" :key="opt.value" :label="opt.label" :value="opt.value" />
          </el-select>
        </el-form-item>
        <el-form-item label="固件版本">
          <el-input v-model="form.firmware_version" placeholder="可留空，OTA 上报后自动写入" maxlength="32" />
        </el-form-item>
        <el-form-item label="软件版本">
          <el-input v-model="form.software_version" placeholder="可留空" maxlength="32" />
        </el-form-item>
        <el-form-item label="备注">
          <el-input v-model="form.description" type="textarea" :rows="2" maxlength="500" show-word-limit />
        </el-form-item>
        <el-form-item label="接管已有资源">
          <el-switch v-model="form.takeover_existing" />
          <span class="hint">
            若同名 SCRAM/Topic/证书已存在（历史遗留），勾选后跳过创建直接接管；否则开通会返回 3002/4002。
          </span>
        </el-form-item>
      </el-form>

      <el-alert type="info" :closable="false" show-icon class="tip">
        开通将按序执行 4 步（DB → SCRAM → Topic → 证书）；任一步失败自动逆序回滚。
        Topic 数量 {{ PER_VEHICLE_TOPIC_COUNT }} 个（telemetry/event/health/command/command_result/ota_notify/ota_status/remote_control）。
      </el-alert>
    </div>

    <!-- Step 2：一次性口令 -->
    <div v-show="currentStep === 2" class="step-body">
      <BundleDownloadNotice
        v-if="provisionResult"
        :password="provisionResult.scram_password"
        :username="provisionResult.vehicle.scram_username"
        :kafka-properties="kafkaPropertiesPreview"
        title="车端 SCRAM 口令（一次性）"
        @close="handleNoticeClose"
      />
      <div v-if="provisionResult">
        <el-descriptions :column="1" border size="small">
          <el-descriptions-item label="车辆 ID">{{ provisionResult.vehicle.vehicle_id }}</el-descriptions-item>
          <el-descriptions-item label="SCRAM 用户名">{{ provisionResult.vehicle.scram_username }}</el-descriptions-item>
          <el-descriptions-item label="Bootstrap">{{ provisionResult.vehicle.kafka_bootstrap }}</el-descriptions-item>
          <el-descriptions-item label="Topic 清单">
            <el-tag
              v-for="topic in provisionResult.vehicle.topics_preview"
              :key="topic"
              size="small"
              effect="plain"
              class="topic-tag"
            >
              {{ topic }}
            </el-tag>
          </el-descriptions-item>
        </el-descriptions>
        <div class="actions">
          <el-button type="primary" :loading="downloading" @click="handleDownloadBundle">
            下载接入包（ZIP）
          </el-button>
          <span class="hint">ZIP 含 ca-cert.pem / client-cert.pem / client-key.pem / kafka-client.p12 / kafka.properties / README.md</span>
        </div>
      </div>
    </div>

    <!-- Step 3：完成 -->
    <div v-show="currentStep === 3" class="step-body">
      <el-result icon="success" title="车辆已开通" sub-title="SCRAM 口令已关闭不再展示；如遗失可在列表上“重发 SCRAM 口令”重新获取">
        <template #extra>
          <el-button type="primary" @click="handleFinish">完成</el-button>
        </template>
      </el-result>
    </div>

    <template #footer>
      <div v-if="currentStep === 1" class="footer">
        <el-button @click="handleCancel">取消</el-button>
        <el-button type="primary" :loading="submitting" @click="handleSubmit">开始一键开通</el-button>
      </div>
      <div v-else-if="currentStep === 3" class="footer">
        <el-button type="primary" @click="handleFinish">关闭窗口</el-button>
      </div>
    </template>
  </el-dialog>
</template>

<style scoped>
.steps {
  margin-bottom: 20px;
}

.step-body {
  min-height: 240px;
}

.hint {
  margin-left: 8px;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}

.tip {
  margin-top: 12px;
  font-size: 12px;
}

.topic-tag {
  margin: 0 4px 4px 0;
}

.actions {
  margin-top: 12px;
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px;
}

.footer {
  text-align: right;
}
</style>
