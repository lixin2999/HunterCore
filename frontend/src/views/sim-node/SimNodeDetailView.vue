<script setup lang="ts">
/**
 * 仿真端详情（scene-service，tag: sim-node）
 *
 * 契约：GET /api/v1/scene/sim-nodes/{node_id}（Redis 实时态 + 登记表合并，懒建登记行）
 * 三段卡：基本信息 + 接入凭证（服务令牌 / Kafka 证书 / 平台证书）+ 实时状态（地址 / 心跳）
 * 操作区权限（对齐服务端实际门禁）：
 *   - 签发/撤销令牌、签发/重签证书、下载接入包 → scene:execute
 *   - 修改管理信息（PATCH）、删除节点 → scene:update
 * 关键约束：服务令牌 access_token 仅签发响应一次性返回，展示后不缓存、不写日志。
 */
import { computed, reactive, ref, watch } from 'vue'
import { ElMessage, ElMessageBox, type FormInstance, type FormRules } from 'element-plus'
import { useRoute, useRouter } from 'vue-router'

import {
  deleteSimNode,
  downloadSimNodeBundle,
  getSimNode,
  issueSimNodeKafkaCert,
  issueSimNodePlatformCert,
  issueSimNodeToken,
  patchSimNode,
  retrySimNodeProvision,
  revokeSimNodeToken,
  rotateSimNodeScram,
} from '@/api/sim-node'
import {
  SIM_NODE_CERT_TYPE_LABELS,
  SIM_NODE_DESCRIPTION_MAX,
  SIM_NODE_DISPLAY_NAME_MAX,
  SIM_NODE_ENDPOINT_SOURCE_LABELS,
  SIM_NODE_ONLINE_LABELS,
  SIM_NODE_ONLINE_TAG_TYPES,
  SIM_NODE_PROVISION_STATE_LABELS,
  SIM_NODE_PROVISION_STATE_TAG_TYPES,
  SIM_NODE_PROVISION_STEP_LABELS,
  SIM_NODE_PROVISION_STEP_STATE_LABELS,
} from '@/constants/sim-node'
import { PERMISSIONS } from '@/constants/permissions'
import type { SimNodeDetail, SimNodeProvisionResult, SimNodeProvisionStep } from '@/types/sim-node'
import { HunterApiError } from '@/utils/error-code'
import { formatDateTimeString, formatRelativeTime } from '@/utils/format'

const route = useRoute()
const router = useRouter()
// 组件实例在参数切换时复用（同 route record + keep-alive）→ node_id 必须保持响应式
const nodeId = computed<string>(() => String(route.params.node_id ?? ''))

const loading = ref(false)
const detail = ref<SimNodeDetail | null>(null)

async function load(): Promise<void> {
  if (!nodeId.value) {
    ElMessage.error('缺少 node_id')
    return
  }
  loading.value = true
  try {
    detail.value = await getSimNode(nodeId.value)
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '仿真端详情加载失败')
  } finally {
    loading.value = false
  }
}

/* ------------------------------ 展示辅助 ------------------------------ */
const addressText = computed(() => {
  const d = detail.value
  if (!d?.endpoint_ip) return '-'
  const proto = d.protocol ?? 'http'
  const port = d.manager_port ? `:${d.manager_port}` : ''
  return `${proto}://${d.endpoint_ip}${port}`
})

function onlineLabel(online: boolean): string {
  return SIM_NODE_ONLINE_LABELS[String(online)] ?? (online ? '在线' : '离线')
}
function onlineTag(online: boolean): string {
  return SIM_NODE_ONLINE_TAG_TYPES[String(online)] ?? 'info'
}
function sourceLabel(source?: string | null): string {
  if (!source) return '-'
  return SIM_NODE_ENDPOINT_SOURCE_LABELS[source] ?? source
}
function certTypeLabel(t: string): string {
  return SIM_NODE_CERT_TYPE_LABELS[t] ?? t
}

/** 标签展示（登记标签优先，其次节点自报标签） */
const labelPairs = computed<Array<[string, string]>>(() => {
  const d = detail.value
  const src = d?.registry_labels && Object.keys(d.registry_labels).length ? d.registry_labels : d?.labels
  return src ? Object.entries(src) : []
})

/* ------------------------------ 一键开通（四步编排） ------------------------------ */
const provisionSteps = computed<SimNodeProvisionStep[]>(() => detail.value?.provision_steps ?? [])
const provisionState = computed<string>(() => detail.value?.provision_state ?? 'pending')
function provisionStateLabel(state: string): string {
  return SIM_NODE_PROVISION_STATE_LABELS[state] ?? state
}
function provisionStateTag(state: string): string {
  return SIM_NODE_PROVISION_STATE_TAG_TYPES[state] ?? 'info'
}
function stepLabel(name: string): string {
  return SIM_NODE_PROVISION_STEP_LABELS[name] ?? name
}
/** 单步状态 → el-step status */
function stepStatus(state: string): 'wait' | 'process' | 'finish' | 'error' {
  if (state === 'ok' || state === 'skipped') return 'finish'
  if (state === 'failed') return 'error'
  if (state === 'in_progress') return 'process'
  return 'wait'
}

const retrying = ref(false)
async function handleRetryProvision(): Promise<void> {
  try {
    await ElMessageBox.confirm(
      `将从失败步续跑开通 ${nodeId.value}（先回收本次残留 → 重跑四步，幂等可多次调用）。成功时一次性返回新口令。是否继续？`,
      '重试开通',
      { type: 'warning', confirmButtonText: '确认重试', cancelButtonText: '取消' },
    )
  } catch {
    return
  }
  retrying.value = true
  try {
    const result = await retrySimNodeProvision(nodeId.value)
    await showProvisionResult(result)
    await load()
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '重试开通失败')
  } finally {
    retrying.value = false
  }
}

const rotating = ref(false)
async function handleRotateScram(): Promise<void> {
  try {
    await ElMessageBox.confirm(
      `将轮换 ${nodeId.value} 的 SCRAM 口令（覆盖旧值）。节点需用新口令更新 kafka.properties 后重连。是否继续？`,
      '轮换 SCRAM 口令',
      { type: 'warning', confirmButtonText: '确认轮换', cancelButtonText: '取消' },
    )
  } catch {
    return
  }
  rotating.value = true
  try {
    const result = await rotateSimNodeScram(nodeId.value)
    await ElMessageBox.alert(
      `新口令（仅展示一次，请立即复制）：\n\n${result.scram_password}`,
      'SCRAM 口令已轮换',
      { type: 'warning', confirmButtonText: '我已安全保存' },
    ).catch(() => undefined)
    await load()
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : 'SCRAM 口令轮换失败')
  } finally {
    rotating.value = false
  }
}

/** 开通结果展示（与列表页一致：一次性口令 + 四步终态） */
async function showProvisionResult(result: SimNodeProvisionResult): Promise<void> {
  const failed = result.provision_state === 'failed'
  const lines: string[] = []
  lines.push(`SCRAM 主体：${result.scram_username}`)
  if (result.scram_password) {
    lines.push('一次性口令（请立即复制，不入库/不入接入包）：')
    lines.push(result.scram_password)
  }
  lines.push(
    result.steps.map((s) => `${stepLabel(s.name)}: ${SIM_NODE_PROVISION_STEP_STATE_LABELS[s.state] ?? s.state}${s.error ? `（${s.error}）` : ''}`).join('\n'),
  )
  await ElMessageBox.alert(lines.join('\n\n'), failed ? '开通失败（可重试）' : '开通成功', {
    type: failed ? 'error' : 'success',
    confirmButtonText: '我已记录口令',
  }).catch(() => undefined)
}

/* ------------------------------ 服务令牌 ------------------------------ */
const issuingToken = ref(false)

async function handleIssueToken(): Promise<void> {
  const hasToken = Boolean(detail.value?.token_id)
  try {
    await ElMessageBox.confirm(
      hasToken
        ? `将为 ${nodeId.value} 重新签发服务令牌；旧令牌仍有效直至撤销或到期，请尽快在节点侧替换。是否继续？`
        : `将为仿真端 ${nodeId.value} 签发服务令牌（代理网关下发）。令牌仅展示一次，请立即复制保存到节点。是否继续？`,
      '签发服务令牌',
      { type: 'warning', confirmButtonText: '确认签发', cancelButtonText: '取消' },
    )
  } catch {
    return
  }
  issuingToken.value = true
  try {
    const result = await issueSimNodeToken(nodeId.value)
    await ElMessageBox.alert(
      `服务令牌（仅展示一次，请立即复制）：\n\n${result.access_token}`,
      '服务令牌已签发',
      { type: 'warning', confirmButtonText: '我已安全保存', customClass: 'sim-token-alert' },
    )
    await load()
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '服务令牌签发失败')
  } finally {
    issuingToken.value = false
  }
}

async function handleRevokeToken(): Promise<void> {
  try {
    await ElMessageBox.confirm(
      `将撤销仿真端 ${nodeId.value} 的服务令牌；使用该令牌的节点将失去接入凭据，需重新签发。是否继续？`,
      '撤销服务令牌',
      { type: 'warning', confirmButtonText: '确认撤销', cancelButtonText: '取消' },
    )
  } catch {
    return
  }
  try {
    await revokeSimNodeToken(nodeId.value)
    ElMessage.success('服务令牌已撤销')
    await load()
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '服务令牌撤销失败')
  }
}

/* ------------------------------ 证书 ------------------------------ */
async function handleIssueCert(kind: 'kafka' | 'platform'): Promise<void> {
  const label = certTypeLabel(kind)
  const existing = kind === 'kafka' ? detail.value?.kafka_cert_sn : detail.value?.plat_cert_sn
  const commonName = kind === 'kafka' ? `sim-${nodeId.value}` : `sim-platform-${nodeId.value}`
  try {
    await ElMessageBox.confirm(
      existing
        ? `将重新签发「${label}」（CN=${commonName}）。旧证书仍在 CA 有效期内（本版本不实现 CRL）。是否继续？`
        : `将为 CN=${commonName} 签发「${label}」。是否继续？`,
      `${existing ? '重签' : '签发'}${label}`,
      { type: 'warning', confirmButtonText: '确认', cancelButtonText: '取消' },
    )
  } catch {
    return
  }
  try {
    const result = kind === 'kafka' ? await issueSimNodeKafkaCert(nodeId.value) : await issueSimNodePlatformCert(nodeId.value)
    ElMessage.success(`${label}已签发（序列号 ${result.serial_hex}）`)
    await load()
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : `${label}签发失败`)
  }
}

/* ------------------------------ 下载接入包 ------------------------------ */
const downloading = ref(false)

async function handleDownload(): Promise<void> {
  downloading.value = true
  try {
    const { blob, filename } = await downloadSimNodeBundle(nodeId.value)
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

/* ------------------------------ 修改管理信息（PATCH） ------------------------------ */
const editVisible = ref(false)
const editFormRef = ref<FormInstance>()
const editSubmitting = ref(false)
const editForm = reactive<{ display_name: string; description: string; labels: Array<{ key: string; value: string }> }>({
  display_name: '',
  description: '',
  labels: [],
})
const editRules: FormRules = {
  display_name: [{ max: SIM_NODE_DISPLAY_NAME_MAX, message: `展示名长度 ≤ ${SIM_NODE_DISPLAY_NAME_MAX}`, trigger: 'blur' }],
  description: [{ max: SIM_NODE_DESCRIPTION_MAX, message: `描述长度 ≤ ${SIM_NODE_DESCRIPTION_MAX}`, trigger: 'blur' }],
}

function openEdit(): void {
  const d = detail.value
  if (!d) return
  editForm.display_name = d.display_name ?? ''
  editForm.description = d.registry_description ?? ''
  const src = d.registry_labels ?? {}
  editForm.labels = Object.entries(src).map(([key, value]) => ({ key, value }))
  editVisible.value = true
}

function addLabelRow(): void {
  editForm.labels.push({ key: '', value: '' })
}
function removeLabelRow(index: number): void {
  editForm.labels.splice(index, 1)
}

async function submitEdit(): Promise<void> {
  const inst = editFormRef.value
  if (!inst) return
  try {
    await inst.validate()
  } catch {
    return
  }
  const labels: Record<string, string> = {}
  for (const row of editForm.labels) {
    const k = row.key.trim()
    if (k) labels[k] = row.value
  }
  editSubmitting.value = true
  try {
    await patchSimNode(nodeId.value, {
      display_name: editForm.display_name,
      description: editForm.description,
      labels,
    })
    ElMessage.success('管理信息已更新')
    editVisible.value = false
    await load()
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '更新失败')
  } finally {
    editSubmitting.value = false
  }
}

/* ------------------------------ 下线（deprovision） ------------------------------ */
async function handleDelete(): Promise<void> {
  try {
    await ElMessageBox.confirm(
      `将下线仿真端 ${nodeId.value}，逆序回收全部资源（证书目录 → SCRAM 用户 → 节点级回传 Topic（含 .dlq）→ 登记行 → Redis），令牌/证书随之失效。是否继续？`,
      '下线仿真端',
      { type: 'warning', confirmButtonText: '确认下线', cancelButtonText: '取消' },
    )
  } catch {
    return
  }
  try {
    const result = await deleteSimNode(nodeId.value)
    const parts = [
      `证书目录${result.cert_dir_removed ? '已删' : '未删'}`,
      `SCRAM${result.scram_removed ? '已删' : '未删'}`,
      `Topic 已删 ${result.topics_removed.length} 个`,
      `登记行${result.registry_removed ? '已删' : '未删'}`,
      `Redis${result.redis_removed ? '已清理' : '未清理'}`,
    ]
    ElMessage.success(`仿真端已下线：${parts.join('，')}`)
    router.push({ name: 'sim-node-list' })
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '下线失败')
  }
}

/* 数据加载由文末 watch(nodeId, …, immediate) 触发 */
watch(
  nodeId,
  (id) => {
    if (!id) return
    detail.value = null
    void load()
  },
  { immediate: true },
)
</script>

<template>
  <div v-loading="loading" class="detail">
    <el-page-header content="仿真端详情" @back="() => $router.back()">
      <template #extra>
        <div class="actions">
          <el-button
            v-if="detail?.provision_state === 'failed'"
            v-permission="PERMISSIONS.sceneExecute"
            type="warning"
            plain
            :loading="retrying"
            @click="handleRetryProvision"
          >
            重试开通
          </el-button>
          <el-button
            v-if="detail?.scram_username"
            v-permission="PERMISSIONS.sceneExecute"
            type="warning"
            plain
            :loading="rotating"
            @click="handleRotateScram"
          >
            轮换口令
          </el-button>
          <el-button v-permission="PERMISSIONS.sceneUpdate" @click="openEdit">修改管理信息</el-button>
          <el-button
            v-if="detail?.token_id"
            v-permission="PERMISSIONS.sceneExecute"
            type="danger"
            plain
            @click="handleRevokeToken"
          >
            撤销令牌
          </el-button>
          <el-button
            v-permission="PERMISSIONS.sceneExecute"
            type="warning"
            :loading="issuingToken"
            @click="handleIssueToken"
          >
            {{ detail?.token_id ? '重签服务令牌' : '签发服务令牌' }}
          </el-button>
          <el-button
            v-permission="PERMISSIONS.sceneExecute"
            type="primary"
            :loading="downloading"
            :disabled="!detail?.kafka_cert_sn && !detail?.plat_cert_sn"
            @click="handleDownload"
          >
            下载接入包
          </el-button>
          <el-button v-permission="PERMISSIONS.sceneUpdate" type="danger" @click="handleDelete">下线节点</el-button>
        </div>
      </template>
    </el-page-header>

    <template v-if="detail">
      <!-- 一键开通卡（四步编排：DB → SCRAM → Topics → Cert） -->
      <el-card shadow="never" class="card">
        <template #header>
          <div class="card__header">
            <span>一键开通</span>
            <el-tag :type="provisionStateTag(provisionState) as never" size="small">
              {{ provisionStateLabel(provisionState) }}
            </el-tag>
          </div>
        </template>
        <el-steps v-if="provisionSteps.length" :active="provisionSteps.length" finish-status="success" align-center>
          <el-step
            v-for="step in provisionSteps"
            :key="step.name"
            :title="stepLabel(step.name)"
            :status="stepStatus(step.state)"
            :description="step.error || SIM_NODE_PROVISION_STEP_STATE_LABELS[step.state] || step.state"
          />
        </el-steps>
        <el-alert
          v-else
          type="info"
          :closable="false"
          show-icon
          title="尚未开通"
          description="该节点尚无一键开通编排记录。可在列表页「创建仿真端」发起四步开通（登记入库 → SCRAM 主体 → 回传 Topic → 证书签发），成功后一次性获取 SCRAM 口令。"
          class="provision-alert"
        />
        <el-descriptions v-if="detail.scram_username" :column="2" border size="small" class="provision-desc">
          <el-descriptions-item label="SCRAM 主体">{{ detail.scram_username }}</el-descriptions-item>
          <el-descriptions-item label="口令更新时">{{ formatDateTimeString(detail.scram_credential_updated_at) }}</el-descriptions-item>
          <el-descriptions-item label="证书到期">{{ formatDateTimeString(detail.cert_not_after) }}</el-descriptions-item>
          <el-descriptions-item label="Kafka 证书">{{ detail.kafka_cert_sn || '-' }}</el-descriptions-item>
        </el-descriptions>
      </el-card>

      <!-- 基本信息卡 -->
      <el-card shadow="never" class="card">
        <template #header>
          <div class="card__header">
            <span>基本信息</span>
            <el-tag :type="onlineTag(detail.online) as never" size="small">{{ onlineLabel(detail.online) }}</el-tag>
          </div>
        </template>
        <el-descriptions :column="2" border size="small">
          <el-descriptions-item label="节点 ID">{{ detail.node_id }}</el-descriptions-item>
          <el-descriptions-item label="展示名">{{ detail.display_name || '-' }}</el-descriptions-item>
          <el-descriptions-item label="归属调用方">{{ detail.owner || detail.owner_client_id || '-' }}</el-descriptions-item>
          <el-descriptions-item label="CARLA 版本">{{ detail.carla_version || '-' }}</el-descriptions-item>
          <el-descriptions-item label="容量 / 运行实例">{{ detail.capacity }} / {{ detail.running_count }}</el-descriptions-item>
          <el-descriptions-item label="注册时间">{{ formatDateTimeString(detail.registered_at) }}</el-descriptions-item>
          <el-descriptions-item label="备注" :span="2">{{ detail.registry_description || '-' }}</el-descriptions-item>
          <el-descriptions-item label="标签" :span="2">
            <template v-if="labelPairs.length">
              <el-tag v-for="[k, v] in labelPairs" :key="k" size="small" effect="plain" class="label-tag">
                {{ k }}: {{ v }}
              </el-tag>
            </template>
            <span v-else class="hint">（无）</span>
          </el-descriptions-item>
          <el-descriptions-item v-if="detail.maps && detail.maps.length" label="可用地图" :span="2">
            <el-tag v-for="m in detail.maps" :key="m" size="small" effect="plain" class="label-tag">{{ m }}</el-tag>
          </el-descriptions-item>
        </el-descriptions>
      </el-card>

      <!-- 接入凭证卡 -->
      <el-card shadow="never" class="card">
        <template #header>接入凭证</template>
        <el-descriptions :column="1" border size="small">
          <el-descriptions-item label="服务令牌">
            <el-tag v-if="detail.token_id" type="success" size="small">已签发</el-tag>
            <el-tag v-else type="info" size="small">未签发</el-tag>
            <code v-if="detail.token_id" class="sn">{{ detail.token_id }}</code>
            <span v-if="detail.token_issued_at" class="hint">签发于 {{ formatDateTimeString(detail.token_issued_at) }}</span>
          </el-descriptions-item>
          <el-descriptions-item :label="certTypeLabel('kafka')">
            <el-tag v-if="detail.kafka_cert_sn" type="success" size="small">已签发</el-tag>
            <el-tag v-else type="info" size="small">未签发</el-tag>
            <code v-if="detail.kafka_cert_sn" class="sn">{{ detail.kafka_cert_sn }}</code>
            <span v-if="detail.kafka_cert_issued_at" class="hint">签发于 {{ formatDateTimeString(detail.kafka_cert_issued_at) }}</span>
            <div class="sub-actions">
              <el-button
                v-permission="PERMISSIONS.sceneExecute"
                link
                type="primary"
                @click="handleIssueCert('kafka')"
              >
                {{ detail.kafka_cert_sn ? '重签' : '签发' }}
              </el-button>
            </div>
          </el-descriptions-item>
          <el-descriptions-item :label="certTypeLabel('platform')">
            <el-tag v-if="detail.plat_cert_sn" type="success" size="small">已签发</el-tag>
            <el-tag v-else type="info" size="small">未签发</el-tag>
            <code v-if="detail.plat_cert_sn" class="sn">{{ detail.plat_cert_sn }}</code>
            <span v-if="detail.plat_cert_issued_at" class="hint">签发于 {{ formatDateTimeString(detail.plat_cert_issued_at) }}</span>
            <div class="sub-actions">
              <el-button
                v-permission="PERMISSIONS.sceneExecute"
                link
                type="primary"
                @click="handleIssueCert('platform')"
              >
                {{ detail.plat_cert_sn ? '重签' : '签发' }}
              </el-button>
            </div>
          </el-descriptions-item>
        </el-descriptions>
      </el-card>

      <!-- 实时状态卡 -->
      <el-card shadow="never" class="card">
        <template #header>实时状态（下发寻址依据）</template>
        <el-descriptions :column="2" border size="small">
          <el-descriptions-item label="当前地址"><code>{{ addressText }}</code></el-descriptions-item>
          <el-descriptions-item label="在线状态">
            <el-tag :type="onlineTag(detail.online) as never" size="small">{{ onlineLabel(detail.online) }}</el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="最近心跳">
            {{ detail.last_seen_at ? formatRelativeTime(Date.parse(detail.last_seen_at) / 1000) : '-' }}
          </el-descriptions-item>
          <el-descriptions-item label="租约(秒)">{{ detail.lease_seconds }}</el-descriptions-item>
          <el-descriptions-item label="地址来源">{{ sourceLabel(detail.endpoint_source) }}</el-descriptions-item>
          <el-descriptions-item label="地址学习时间">{{ formatDateTimeString(detail.learned_at) }}</el-descriptions-item>
          <el-descriptions-item v-if="detail.runtime_hint" label="节点自述地址" :span="2">{{ detail.runtime_hint }}</el-descriptions-item>
          <el-descriptions-item v-if="detail.message" label="节点提示" :span="2">{{ detail.message }}</el-descriptions-item>
        </el-descriptions>
      </el-card>
    </template>

    <!-- 修改管理信息对话框 -->
    <el-dialog v-model="editVisible" title="修改管理信息" width="560px">
      <el-form ref="editFormRef" :model="editForm" :rules="editRules" label-width="90px">
        <el-form-item label="展示名" prop="display_name">
          <el-input v-model="editForm.display_name" :maxlength="SIM_NODE_DISPLAY_NAME_MAX" show-word-limit />
        </el-form-item>
        <el-form-item label="备注" prop="description">
          <el-input v-model="editForm.description" type="textarea" :rows="3" :maxlength="SIM_NODE_DESCRIPTION_MAX" show-word-limit />
        </el-form-item>
        <el-form-item label="标签">
          <div class="label-editor">
            <div v-for="(row, idx) in editForm.labels" :key="idx" class="label-row">
              <el-input v-model="row.key" placeholder="键" style="width: 160px" />
              <el-input v-model="row.value" placeholder="值" style="width: 200px" />
              <el-button link type="danger" @click="removeLabelRow(idx)">删除</el-button>
            </div>
            <el-button link type="primary" @click="addLabelRow">添加标签</el-button>
          </div>
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

.sn {
  margin-left: 8px;
  font-size: 12px;
}

.hint {
  margin-left: 8px;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}

.label-tag {
  margin: 0 4px 4px 0;
}

.sub-actions {
  margin-left: 8px;
  display: inline-block;
}

.label-editor {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.label-row {
  display: flex;
  align-items: center;
  gap: 8px;
}

.provision-alert {
  margin-top: 4px;
}

.provision-desc {
  margin-top: 16px;
}
</style>
