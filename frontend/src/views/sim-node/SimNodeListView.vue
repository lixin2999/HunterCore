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
import { ElMessage, ElMessageBox } from 'element-plus'
import { useRouter } from 'vue-router'

import { deleteSimNode, downloadSimNodeBundle, listSimNodes } from '@/api/sim-node'
import {
  SIM_NODE_CRED_STATUS_LABELS,
  SIM_NODE_CRED_STATUS_TAG_TYPES,
  SIM_NODE_ONLINE_LABELS,
  SIM_NODE_ONLINE_TAG_TYPES,
} from '@/constants/sim-node'
import { PERMISSIONS } from '@/constants/permissions'
import type { SimNodeDetail } from '@/types/sim-node'
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
      `将从登记表移除仿真端 ${row.node_id} 并清理资源（Redis 登记 + 登记表行 + 证书目录）。` +
        `若节点仍在运行会自动下线。是否继续？`,
      '删除仿真端',
      { type: 'warning', confirmButtonText: '确认删除', cancelButtonText: '取消' },
    )
  } catch {
    return
  }
  try {
    await deleteSimNode(row.node_id)
    ElMessage.success('仿真端已删除')
    await load()
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '删除失败')
  }
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
      </el-form-item>
    </el-form>

    <!-- 汇总条 + 注册模型说明 -->
    <div class="summary">
      <el-tag type="success" size="small">在线 {{ summary.online }}</el-tag>
      <el-tag type="info" size="small">离线 {{ summary.offline }}</el-tag>
      <span class="summary__hint">
        共 {{ total }} 个节点；仿真端由节点自行注册（PUT /sim-nodes），管理端仅查看并签发凭证
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
            删除
          </el-button>
        </template>
      </el-table-column>
    </el-table>
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
</style>
