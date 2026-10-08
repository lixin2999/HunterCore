<script setup lang="ts">
/**
 * 车辆管理列表（vehicle-service）
 *
 * 契约：GET /api/v1/vehicle/list；每行展示 4 步 provisioning 徽标；
 *      行内动作：查看详情 / 下载接入包 / 重发 SCRAM 口令 / 重发证书 / 下线。
 *
 * 权限：列表读取 vehicle:read；重发（execute）与下线（delete）在按钮上分别用 v-permission 控制展示。
 */
import { computed, onMounted, reactive, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { useRouter } from 'vue-router'

import {
  deleteVehicle,
  downloadBundle,
  listVehicles,
  reissueCert,
  rotateScram,
} from '@/api/vehicle'
import StatusTag from '@/components/common/StatusTag.vue'
import VehicleCreateDialog from './VehicleCreateDialog.vue'
import {
  DEFAULT_PAGE_SIZE,
  VEHICLE_STATUS_LABELS,
} from '@/constants'
import {
  PROVISION_STATE_LABELS,
  PROVISION_STATE_TAG_TYPES,
  PROVISION_STEP_LABELS,
  PROVISION_STEP_ORDER,
  PROVISION_STEP_STATE_LABELS,
  PROVISION_STEP_STATE_TAG_TYPES,
} from '@/constants/vehicle'
import { PERMISSIONS } from '@/constants/permissions'
import type {
  ProvisionStep,
  ProvisionStepName,
  VehicleListQuery,
  VehicleRow,
} from '@/types/vehicle'
import { HunterApiError } from '@/utils/error-code'
import { formatDateTimeString, formatRelativeTime } from '@/utils/format'

const router = useRouter()

/* ------------------------------ 查询与分页 ------------------------------ */
const loading = ref(false)
const rows = ref<VehicleRow[]>([])
const total = ref(0)
const query = reactive<VehicleListQuery>({
  keyword: '',
  status: undefined,
  provision_state: undefined,
  page: 1,
  page_size: DEFAULT_PAGE_SIZE,
})

const statusOptions = Object.entries(VEHICLE_STATUS_LABELS).map(([value, label]) => ({ value, label }))
const provisionStateOptions = Object.entries(PROVISION_STATE_LABELS).map(([value, label]) => ({ value, label }))

async function load(): Promise<void> {
  loading.value = true
  try {
    const data = await listVehicles({ ...query })
    rows.value = data.items
    total.value = data.total
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '车辆列表加载失败')
  } finally {
    loading.value = false
  }
}

function resetQuery(): void {
  query.keyword = ''
  query.status = undefined
  query.provision_state = undefined
  query.page = 1
  void load()
}

function handlePageChange(page: number): void {
  query.page = page
  void load()
}

/* ------------------------------ Provision 步骤徽标 ------------------------------ */
/**
 * 从行的 steps 数组按固定顺序映射为 4 项（缺项按 pending 兜底）
 * 契约保证 steps 长度 = 4；此处仍兜底避免历史数据缺失渲染错乱。
 */
function orderedSteps(row: VehicleRow): ProvisionStep[] {
  const map = new Map<ProvisionStepName, ProvisionStep>()
  for (const step of row.provision_steps ?? []) {
    map.set(step.name, step)
  }
  return PROVISION_STEP_ORDER.map(
    (name) => map.get(name) ?? { name, state: 'pending' as const, error: null, ts: null },
  )
}

/* ------------------------------ 行内动作 ------------------------------ */
const createVisible = ref(false)

function openDetail(row: VehicleRow): void {
  router.push({ name: 'vehicle-detail', params: { vehicle_id: row.vehicle_id } })
}

async function handleDownload(row: VehicleRow): Promise<void> {
  try {
    const { blob, filename } = await downloadBundle(row.vehicle_id)
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

/** SCRAM 口令重置成功后弹出的一次性展示（用 ElMessageBox + HTML 简化，避免新增独立路由） */
async function handleRotateScram(row: VehicleRow): Promise<void> {
  try {
    await ElMessageBox.confirm(
      `将为车辆 ${row.vehicle_id} 生成新的 SCRAM 口令；旧口令立即失效，车端需同步更新 kafka.properties。是否继续？`,
      '重发 SCRAM 口令',
      { type: 'warning', confirmButtonText: '确认重置', cancelButtonText: '取消' },
    )
  } catch {
    return
  }
  try {
    const result = await rotateScram(row.vehicle_id)
    await ElMessageBox.alert(
      `新 SCRAM 口令（仅展示一次，请立即复制保存到车端）：\n\n${result.scram_password}`,
      'SCRAM 口令已重置',
      {
        type: 'warning',
        confirmButtonText: '我已安全保存',
        customClass: 'scram-password-alert',
      },
    )
    await load()
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : 'SCRAM 口令重置失败')
  }
}

async function handleReissueCert(row: VehicleRow): Promise<void> {
  try {
    await ElMessageBox.confirm(
      `将重新签发车辆 ${row.vehicle_id} 的 mTLS 客户端证书（旧证书仍在 CA 有效期内，本 MVP 不实现 CRL）。是否继续？`,
      '重发客户端证书',
      { type: 'warning', confirmButtonText: '确认重签', cancelButtonText: '取消' },
    )
  } catch {
    return
  }
  try {
    const result = await reissueCert(row.vehicle_id)
    ElMessage.success(`证书已重签（新序列号 ${result.device_cert_sn}）`)
    await load()
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '证书重签失败')
  }
}

async function handleOffline(row: VehicleRow): Promise<void> {
  let purgeTopics = true
  try {
    await ElMessageBox.confirm(
      `下线车辆 ${row.vehicle_id}：逆序回收证书 → SCRAM → Topic → DB 台账。` +
        `勾选"同时删除 Kafka Topic"将丢失未消费的短期消息（时序/事件历史数据保留审计）。\n\n是否继续？`,
      '下线车辆',
      {
        type: 'warning',
        confirmButtonText: '确认下线',
        cancelButtonText: '取消',
        showCancelButton: true,
      },
    )
  } catch {
    return
  }
  // 二次询问是否 purge Topic（默认 true）
  try {
    await ElMessageBox.confirm(
      '是否同时删除 8 个车端 Topic？（选择"取消"仅删除 SCRAM/证书/DB 行，保留 Topic）',
      '删除 Kafka Topic',
      { type: 'info', confirmButtonText: '同时删除 Topic', cancelButtonText: '保留 Topic' },
    ).catch(() => {
      purgeTopics = false
    })
  } catch {
    purgeTopics = false
  }
  try {
    await deleteVehicle(row.vehicle_id, purgeTopics)
    ElMessage.success(purgeTopics ? '车辆已下线（含 Topic 已删除）' : '车辆已下线（保留 Topic）')
    await load()
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '下线失败')
  }
}

/* ------------------------------ 展示辅助 ------------------------------ */
const provisionStateTag = (value: string): string => PROVISION_STATE_TAG_TYPES[value] ?? 'info'
const provisionStateLabel = (value: string): string => PROVISION_STATE_LABELS[value] ?? value
const stepLabel = (name: string): string => PROVISION_STEP_LABELS[name] ?? name
const stepStateLabel = (state: string): string => PROVISION_STEP_STATE_LABELS[state] ?? state
const stepStateTag = (state: string): string => PROVISION_STEP_STATE_TAG_TYPES[state] ?? 'info'

const summary = computed(() => {
  const counter: Record<string, number> = { ready: 0, failed: 0, in_progress: 0, pending: 0 }
  for (const row of rows.value) {
    counter[row.provision_state] = (counter[row.provision_state] ?? 0) + 1
  }
  return counter
})

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
          placeholder="vehicle_id / name"
          style="width: 180px"
          @keyup.enter="query.page = 1; load()"
        />
      </el-form-item>
      <el-form-item label="车辆状态">
        <el-select v-model="query.status" clearable placeholder="全部" style="width: 140px">
          <el-option v-for="opt in statusOptions" :key="opt.value" :label="opt.label" :value="opt.value" />
        </el-select>
      </el-form-item>
      <el-form-item label="开通状态">
        <el-select v-model="query.provision_state" clearable placeholder="全部" style="width: 130px">
          <el-option v-for="opt in provisionStateOptions" :key="opt.value" :label="opt.label" :value="opt.value" />
        </el-select>
      </el-form-item>
      <el-form-item>
        <el-button type="primary" :loading="loading" @click="query.page = 1; load()">查询</el-button>
        <el-button @click="resetQuery">重置</el-button>
        <el-button
          v-permission="PERMISSIONS.vehicleCreate"
          type="success"
          @click="createVisible = true"
        >
          新增车辆（一键开通）
        </el-button>
      </el-form-item>
    </el-form>

    <!-- 汇总条 -->
    <div class="summary">
      <el-tag type="success" size="small">就绪 {{ summary.ready ?? 0 }}</el-tag>
      <el-tag type="warning" size="small">进行中 {{ summary.in_progress ?? 0 }}</el-tag>
      <el-tag type="danger" size="small">失败 {{ summary.failed ?? 0 }}</el-tag>
      <el-tag type="info" size="small">待开通 {{ summary.pending ?? 0 }}</el-tag>
      <span class="summary__hint">当前页统计；开通 4 步：DB · SCRAM · Topic · Cert</span>
    </div>

    <!-- 主表 -->
    <el-table :data="rows" size="small" empty-text="暂无车辆">
      <el-table-column prop="vehicle_id" label="车辆 ID" width="130" />
      <el-table-column prop="vehicle_name" label="名称" min-width="120" show-overflow-tooltip />
      <el-table-column prop="model" label="型号" width="100" />
      <el-table-column label="状态" width="100">
        <template #default="{ row }">
          <StatusTag kind="vehicle" :value="row.status" />
        </template>
      </el-table-column>
      <el-table-column label="开通态" width="100">
        <template #default="{ row }">
          <el-tag :type="provisionStateTag(row.provision_state) as never" size="small">
            {{ provisionStateLabel(row.provision_state) }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column label="Provisioning 步骤" min-width="320">
        <template #default="{ row }">
          <el-tooltip v-for="step in orderedSteps(row)" :key="step.name" placement="top">
            <template #content>
              <div>
                {{ stepLabel(step.name) }}：{{ stepStateLabel(step.state) }}
                <template v-if="step.ts"> · {{ formatRelativeTime(step.ts) }}</template>
                <div v-if="step.error">{{ step.error }}</div>
              </div>
            </template>
            <el-tag :type="stepStateTag(step.state) as never" size="small" class="step-badge">
              {{ stepLabel(step.name) }}
            </el-tag>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column label="最近在线" width="140">
        <template #default="{ row }">{{ formatRelativeTime(row.last_online_time ? Date.parse(row.last_online_time) / 1000 : null) }}</template>
      </el-table-column>
      <el-table-column label="注册时间" width="170">
        <template #default="{ row }">{{ formatDateTimeString(row.register_time) }}</template>
      </el-table-column>
      <el-table-column label="操作" width="360" fixed="right">
        <template #default="{ row }">
          <el-button v-permission="PERMISSIONS.vehicleRead" link type="primary" @click="openDetail(row)">
            详情
          </el-button>
          <el-button
            v-if="row.provision_state === 'ready'"
            v-permission="PERMISSIONS.vehicleExecute"
            link
            type="primary"
            @click="handleDownload(row)"
          >
            下载接入包
          </el-button>
          <el-button
            v-permission="PERMISSIONS.vehicleExecute"
            link
            type="warning"
            @click="handleRotateScram(row)"
          >
            重发口令
          </el-button>
          <el-button
            v-permission="PERMISSIONS.vehicleExecute"
            link
            type="warning"
            @click="handleReissueCert(row)"
          >
            重发证书
          </el-button>
          <el-button
            v-permission="PERMISSIONS.vehicleDelete"
            link
            type="danger"
            @click="handleOffline(row)"
          >
            下线
          </el-button>
        </template>
      </el-table-column>
    </el-table>

    <el-pagination
      class="pager"
      layout="total, prev, pager, next"
      :total="total"
      :page-size="query.page_size"
      :current-page="query.page"
      @current-change="handlePageChange"
    />

    <VehicleCreateDialog
      v-model="createVisible"
      @created="load"
    />
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

.step-badge {
  margin-right: 6px;
}

.pager {
  margin-top: 12px;
  text-align: right;
}
</style>
