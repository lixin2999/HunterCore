<script setup lang="ts">
/**
 * 仿真端管理列表（scene-service，tag: sim-node）
 *
 * 契约：GET /api/v1/scene/sim-nodes?online_only=（清单沿用 scene:read）
 *      行内动作：查看详情 / 下载接入包（execute）/ 删除节点（scene:update，下线清理）
 *
 * 注册模型 = 节点自注册 + 后台管理：无"新建"端点（节点经 PUT 注册），管理员在后台查看已接入节点并签发凭证。
 * 清单项复用详情结构（附带 display_name/token_id/cert_sn），未登记节点这些字段为空。
 */
import { computed, onMounted, reactive, ref } from 'vue'
import { ElMessage, ElMessageBox, type FormInstance, type FormRules } from 'element-plus'
import { useRouter } from 'vue-router'

import { deleteSimNode, downloadSimNodeBundle, listSimNodes, provisionSimNode } from '@/api/sim-node'
import {
  SIM_NODE_CRED_STATUS_LABELS,
  SIM_NODE_CRED_STATUS_TAG_TYPES,
  SIM_NODE_DESCRIPTION_MAX,
  SIM_NODE_DISPLAY_NAME_MAX,
  SIM_NODE_ID_PATTERN,
  SIM_NODE_ONLINE_LABELS,
  SIM_NODE_ONLINE_TAG_TYPES,
  SIM_NODE_PROVISION_STATE_LABELS,
  SIM_NODE_PROVISION_STATE_TAG_TYPES,
} from '@/constants/sim-node'
import { PERMISSIONS } from '@/constants/permissions'
import type { SimNodeDeprovisionResult, SimNodeDetail, SimNodeProvisionResult } from '@/types/sim-node'
import { HunterApiError } from '@/utils/error-code'
import { formatDateTimeString, formatRelativeTime } from '@/utils/format'

const router = useRouter()

/* ------------------------------ 查询 ------------------------------ */
const loading = ref(false)
const rows = ref<SimNodeDetail[]>([])
const total = ref(0)
const query = reactive<{ online: 'online' | 'all'; keyword: string }>({
  online: 'all',
  keyword: '',
})

async function load(): Promise<void> {
  loading.value = true
  try {
    const data = await listSimNodes({ online_only: query.online === 'online' })
    rows.value = data.items
    total.value = data.total
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '仿真端列表加载失败')
  } finally {
    loading.value = false
  }
}

function resetQuery(): void {
  query.online = 'all'
  query.keyword = ''
  void load()
}

/** 关键词过滤（node_id / 展示名 / 地址；纯前端，清单上限 100） */
const filteredRows = computed<SimNodeDetail[]>(() => {
  const kw = query.keyword.trim().toLowerCase()
  if (!kw) return rows.value
  return rows.value.filter((row) => {
    const haystack = [row.node_id, row.display_name ?? '', row.endpoint_ip ?? '']
      .join(' ')
      .toLowerCase()
    return haystack.includes(kw)
  })
})

/* ------------------------------ 展示辅助 ------------------------------ */
function addressText(row: SimNodeDetail): string {
  if (!row.endpoint_ip) return '-'
  const proto = row.protocol ?? 'http'
  const port = row.manager_port ? `:${row.manager_port}` : ''
  return `${proto}://${row.endpoint_ip}${port}`
}

function onlineLabel(online: boolean): string {
  return SIM_NODE_ONLINE_LABELS[String(online)] ?? (online ? '在线' : '离线')
}
function onlineTag(online: boolean): string {
  return SIM_NODE_ONLINE_TAG_TYPES[String(online)] ?? 'info'
}

/** 令牌状态：token_id 非空即视为已签发 */
function tokenStatus(row: SimNodeDetail): { label: string; tag: string } {
  const issued = Boolean(row.token_id)
  const key = issued ? 'issued' : 'none'
  return { label: SIM_NODE_CRED_STATUS_LABELS[key] ?? key, tag: SIM_NODE_CRED_STATUS_TAG_TYPES[key] ?? 'info' }
}

/** 证书状态：kafka / platform 任一有序列号即视为已签发 */
function certStatus(row: SimNodeDetail): { label: string; tag: string } {
  const issued = Boolean(row.kafka_cert_sn || row.plat_cert_sn)
  const key = issued ? 'issued' : 'none'
  const detail = `Kafka:${row.kafka_cert_sn ? '已签发' : '未签发'} · 平台:${row.plat_cert_sn ? '已签发' : '未签发'}`
  return { label: `${SIM_NODE_CRED_STATUS_LABELS[key] ?? key}（${detail}）`, tag: SIM_NODE_CRED_STATUS_TAG_TYPES[key] ?? 'info' }
}

const summary = computed(() => {
  const online = rows.value.filter((r) => r.online).length
  return { online, offline: rows.value.length - online }
})

/** 开通态展示（provision_state；旧行无该字段→按未开通处理） */
function provisionTag(row: SimNodeDetail): { label: string; tag: string } {
  const state = row.provision_state ?? 'pending'
  return {
    label: SIM_NODE_PROVISION_STATE_LABELS[state] ?? state,
    tag: SIM_NODE_PROVISION_STATE_TAG_TYPES[state] ?? 'info',
  }
}

/* ------------------------------ 创建仿真端（一键开通向导） ------------------------------ */
const createVisible = ref(false)
const createFormRef = ref<FormInstance>()
const createSubmitting = ref(false)
const createForm = reactive<{
  node_id: string
  display_name: string
  description: string
  labels: Array<{ key: string; value: string }>
  takeover_existing: boolean
}>({
  node_id: '',
  display_name: '',
  description: '',
  labels: [],
  takeover_existing: false,
})
const createRules: FormRules = {
  node_id: [
    { required: true, message: '请输入节点 ID', trigger: 'blur' },
    { pattern: SIM_NODE_ID_PATTERN, message: 'node_id 需匹配 ^[a-z0-9][a-z0-9._-]{2,63}$', trigger: 'blur' },
  ],
  display_name: [{ max: SIM_NODE_DISPLAY_NAME_MAX, message: `展示名长度 ≤ ${SIM_NODE_DISPLAY_NAME_MAX}`, trigger: 'blur' }],
  description: [{ max: SIM_NODE_DESCRIPTION_MAX, message: `描述长度 ≤ ${SIM_NODE_DESCRIPTION_MAX}`, trigger: 'blur' }],
}

function openCreate(): void {
  createForm.node_id = ''
  createForm.display_name = ''
  createForm.description = ''
  createForm.labels = []
  createForm.takeover_existing = false
  createVisible.value = true
}
function addCreateLabel(): void {
  createForm.labels.push({ key: '', value: '' })
}
function removeCreateLabel(index: number): void {
  createForm.labels.splice(index, 1)
}

async function submitCreate(): Promise<void> {
  const inst = createFormRef.value
  if (!inst) return
  try {
    await inst.validate()
  } catch {
    return
  }
  const labels: Record<string, string> = {}
  for (const row of createForm.labels) {
    const k = row.key.trim()
    if (k) labels[k] = row.value
  }
  createSubmitting.value = true
  try {
    const result = await provisionSimNode({
      node_id: createForm.node_id.trim(),
      display_name: createForm.display_name || null,
      description: createForm.description || null,
      labels: Object.keys(labels).length ? labels : null,
      takeover_existing: createForm.takeover_existing,
    })
    createVisible.value = false
    await showProvisionResult(result)
    await load()
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '一键开通失败')
  } finally {
    createSubmitting.value = false
  }
}

/** 开通结果展示：一次性 SCRAM 口令（仅此处可见）+ 四步终态；失败高亮错误摘要 */
async function showProvisionResult(result: SimNodeProvisionResult): Promise<void> {
  const failed = result.provision_state === 'failed'
  const lines: string[] = []
  lines.push(
    `节点 ${result.node_id} 开通态：${
      SIM_NODE_PROVISION_STATE_LABELS[result.provision_state] ?? result.provision_state
    }`,
  )
  lines.push(`SCRAM 主体：${result.scram_username}`)
  if (result.scram_password) {
    lines.push('一次性口令（请立即复制，不入库/不入接入包）：')
    lines.push(result.scram_password)
  }
  lines.push(
    result.steps
      .map((s) => `${s.name}: ${s.state}${s.error ? `（${s.error}）` : ''}`)
      .join('\n'),
  )
  await ElMessageBox.alert(lines.join('\n\n'), failed ? '开通失败（可重试）' : '开通成功', {
    type: failed ? 'error' : 'success',
    confirmButtonText: '我已记录口令',
    customClass: 'sim-provision-alert',
  }).catch(() => undefined)
}

/* ------------------------------ 行内动作 ------------------------------ */
function openDetail(row: SimNodeDetail): void {
  router.push({ name: 'sim-node-detail', params: { node_id: row.node_id } })
}

async function handleDownload(row: SimNodeDetail): Promise<void> {
  try {
    const { blob, filename } = await downloadSimNodeBundle(row.node_id)
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
  }
}

async function handleDelete(row: SimNodeDetail): Promise<void> {
  try {
    await ElMessageBox.confirm(
      `将下线仿真端 ${row.node_id}，逆序回收资源（证书目录 → SCRAM 用户 → 节点级回传 Topic（含 .dlq）→ 登记行 → Redis）。` +
        `若节点仍在运行会自动下线。是否继续？`,
      '下线仿真端',
      { type: 'warning', confirmButtonText: '确认下线', cancelButtonText: '取消' },
    )
  } catch {
    return
  }
  try {
    const summary = await deprovisionSummary(row.node_id)
    ElMessage.success(summary)
    await load()
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '下线失败')
  }
}

/** 回收摘要文本（契约 SimNodeDeprovisionResult） */
async function deprovisionSummary(nodeId: string): Promise<string> {
  const result: SimNodeDeprovisionResult = await deleteSimNode(nodeId)
  const parts: string[] = []
  parts.push(`证书目录${result.cert_dir_removed ? '已删' : '未删'}`)
  parts.push(`SCRAM${result.scram_removed ? '已删' : '未删'}`)
  parts.push(`Topic 已删 ${result.topics_removed.length} 个`)
  parts.push(`登记行${result.registry_removed ? '已删' : '未删'}`)
  parts.push(`Redis${result.redis_removed ? '已清理' : '未清理'}`)
  return `仿真端 ${result.node_id} 已下线：${parts.join('，')}`
}

onMounted(load)
</script>

<template>
  <div v-loading="loading">
    <!-- 顶部工具栏 -->
    <el-form :inline="true" class="toolbar">
      <el-form-item label="关键词">
        <el-input
          v-model="query.keyword"
          clearable
          placeholder="node_id / 展示名 / 地址"
          style="width: 200px"
        />
      </el-form-item>
      <el-form-item label="在线状态">
        <el-select v-model="query.online" style="width: 120px" @change="load">
          <el-option label="全部" value="all" />
          <el-option label="仅在线" value="online" />
        </el-select>
      </el-form-item>
      <el-form-item>
        <el-button type="primary" :loading="loading" @click="load">刷新</el-button>
        <el-button @click="resetQuery">重置</el-button>
        <el-button v-permission="PERMISSIONS.sceneExecute" type="success" @click="openCreate">
          创建仿真端
        </el-button>
      </el-form-item>
    </el-form>

    <!-- 汇总条 + 注册模型说明 -->
    <div class="summary">
      <el-tag type="success" size="small">在线 {{ summary.online }}</el-tag>
      <el-tag type="info" size="small">离线 {{ summary.offline }}</el-tag>
      <span class="summary__hint">
        共 {{ total }} 个节点；可由管理员“创建仿真端”一键开通（DB→SCRAM→Topic→证书），或节点自行注册（PUT /sim-nodes）后补录
      </span>
    </div>

    <!-- 主表 -->
    <el-table :data="filteredRows" size="small" empty-text="暂无仿真端">
      <el-table-column prop="node_id" label="节点 ID" min-width="150" show-overflow-tooltip />
      <el-table-column label="展示名" min-width="120" show-overflow-tooltip>
        <template #default="{ row }">{{ row.display_name || '-' }}</template>
      </el-table-column>
      <el-table-column label="当前地址" min-width="200" show-overflow-tooltip>
        <template #default="{ row }"><code>{{ addressText(row) }}</code></template>
      </el-table-column>
      <el-table-column label="在线状态" width="90">
        <template #default="{ row }">
          <el-tag :type="onlineTag(row.online) as never" size="small">{{ onlineLabel(row.online) }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="运行/容量" width="100">
        <template #default="{ row }">{{ row.running_count }} / {{ row.capacity }}</template>
      </el-table-column>
      <el-table-column label="最近心跳" width="140">
        <template #default="{ row }">
          {{ row.last_seen_at ? formatRelativeTime(Date.parse(row.last_seen_at) / 1000) : '-' }}
        </template>
      </el-table-column>
      <el-table-column label="注册时间" width="170">
        <template #default="{ row }">{{ formatDateTimeString(row.registered_at) }}</template>
      </el-table-column>
      <el-table-column label="服务令牌" width="100">
        <template #default="{ row }">
          <el-tag :type="tokenStatus(row).tag as never" size="small">{{ tokenStatus(row).label }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="证书" min-width="180">
        <template #default="{ row }">
          <el-tag :type="certStatus(row).tag as never" size="small">{{ certStatus(row).label }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="开通态" width="100">
        <template #default="{ row }">
          <el-tag :type="provisionTag(row).tag as never" size="small">{{ provisionTag(row).label }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="操作" width="240" fixed="right">
        <template #default="{ row }">
          <el-button v-permission="PERMISSIONS.sceneRead" link type="primary" @click="openDetail(row)">
            详情
          </el-button>
          <el-button
            v-if="row.kafka_cert_sn || row.plat_cert_sn"
            v-permission="PERMISSIONS.sceneExecute"
            link
            type="primary"
            @click="handleDownload(row)"
          >
            下载接入包
          </el-button>
          <el-button v-permission="PERMISSIONS.sceneUpdate" link type="danger" @click="handleDelete(row)">
            下线
          </el-button>
        </template>
      </el-table-column>
    </el-table>

    <!-- 创建仿真端（一键开通向导） -->
    <el-dialog v-model="createVisible" title="创建仿真端（一键开通）" width="600px">
      <el-alert
        type="info"
        :closable="false"
        show-icon
        title="开通四步：登记入库 → 创建 SCRAM 主体（sim-{node_id}）→ 创建节点级回传 Topic（含 .dlq）→ 签发双证书"
        description="口令仅在开通响应中一次性返回，请立即复制并随接入包下发节点；node_id 格式与节点自注册一致，后续节点以同一 node_id 完成注册/心跳。"
        style="margin-bottom: 12px"
      />
      <el-form ref="createFormRef" :model="createForm" :rules="createRules" label-width="90px">
        <el-form-item label="节点 ID" prop="node_id">
          <el-input v-model="createForm.node_id" placeholder="如 sim-node-01" />
        </el-form-item>
        <el-form-item label="展示名" prop="display_name">
          <el-input v-model="createForm.display_name" :maxlength="SIM_NODE_DISPLAY_NAME_MAX" show-word-limit />
        </el-form-item>
        <el-form-item label="备注" prop="description">
          <el-input v-model="createForm.description" type="textarea" :rows="2" :maxlength="SIM_NODE_DESCRIPTION_MAX" show-word-limit />
        </el-form-item>
        <el-form-item label="标签">
          <div class="label-editor">
            <div v-for="(row, idx) in createForm.labels" :key="idx" class="label-row">
              <el-input v-model="row.key" placeholder="键" style="width: 160px" />
              <el-input v-model="row.value" placeholder="值" style="width: 200px" />
              <el-button link type="danger" @click="removeCreateLabel(idx)">删除</el-button>
            </div>
            <el-button link type="primary" @click="addCreateLabel">添加标签</el-button>
          </div>
        </el-form-item>
        <el-form-item label="接管已有">
          <el-switch v-model="createForm.takeover_existing" />
          <span class="summary__hint">SCRAM/Topic/证书已存在时接管复用（跳过创建，回滚不清理这些外部资源）</span>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="createVisible = false">取消</el-button>
        <el-button type="primary" :loading="createSubmitting" @click="submitCreate">一键开通</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<style scoped>
.toolbar {
  margin-bottom: 8px;
}

.summary {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 12px;
}

.summary__hint {
  margin-left: 8px;
  font-size: 12px;
  color: var(--el-text-color-secondary);
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
</style>
