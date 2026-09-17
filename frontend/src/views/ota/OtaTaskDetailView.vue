<script setup lang="ts">
/**
 * OTA 任务详情（灰度发布推进视图）
 *
 * 契约（ota-service.yaml）：
 * - GET  /api/v1/ota/tasks/{task_id}（含 rollout：批次进度、观察截止、next_action）
 * - GET  /api/v1/ota/tasks/{task_id}/records（单车升级记录：状态机 phase + progress）
 * - POST .../start|pause|resume|cancel|rollback（见 OtaTasksView 说明）
 * 阈值：任一批次成功率 < 95% 时服务端将 next_action 置为 halt（前端只展示，不自行判定）。
 */
import { computed, onMounted, reactive, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'

import {
  getOtaTask,
  listOtaTaskRecords,
  pauseOtaTask,
  resumeOtaTask,
  rollbackOtaTask,
  startOtaTask,
} from '@/api/ota'
import StatusTag from '@/components/common/StatusTag.vue'
import StatCard from '@/components/common/StatCard.vue'
import {
  DEFAULT_PAGE_SIZE,
  OTA_BATCH_STATUS_LABELS,
  OTA_NEXT_ACTION_LABELS,
  OTA_UPGRADE_FLOW,
  OTA_UPGRADE_STATUS_LABELS,
} from '@/constants'
import { PERMISSIONS } from '@/constants/permissions'
import { useVehicleStore } from '@/stores/vehicle'
import type { OtaRecordListQuery, OtaTaskDetail, OtaUpgradeStatus } from '@/types/ota'
import { HunterApiError } from '@/utils/error-code'
import { formatNumber, formatPercent, formatTime } from '@/utils/format'

const route = useRoute()
const router = useRouter()
const vehicleStore = useVehicleStore()

const taskId = computed<string>(() => String(route.params.task_id ?? ''))
const loading = ref(false)
const task = ref<OtaTaskDetail | null>(null)

/** 升级记录（支持按车辆 / 状态过滤） */
const recordQuery = reactive<OtaRecordListQuery>({
  vehicle_id: undefined,
  status: undefined,
  page: 1,
  page_size: DEFAULT_PAGE_SIZE,
})
const records = ref<Awaited<ReturnType<typeof listOtaTaskRecords>>['items']>([])
const recordTotal = ref(0)
const recordLoading = ref(false)

const statusOptions = Object.entries(OTA_UPGRADE_STATUS_LABELS).map(([value, label]) => ({ value, label }))
const batchStatusLabels = OTA_BATCH_STATUS_LABELS
const nextActionLabels = OTA_NEXT_ACTION_LABELS
/** 状态机阶段顺序（时间轴展示，不可更改：IDLE→PENDING→DOWNLOAD→INSTALL→TEST→SUCCESS） */
const upgradeFlow = OTA_UPGRADE_FLOW

/** 批次进度（rollout.batches，最多 4 批） */
const batches = computed(() => task.value?.rollout?.batches ?? [])

/** 当前批次进度百分比（用于进度条） */
function batchPercent(success: number, target: number): number {
  return target > 0 ? Math.round((success / target) * 100) : 0
}

/** 状态机阶段序号（用于进度条状态判定） */
function phaseIndex(status: OtaUpgradeStatus): number {
  const index = upgradeFlow.indexOf(status)
  return index >= 0 ? index : 0
}

async function loadTask(): Promise<void> {
  if (!taskId.value) {
    return
  }
  loading.value = true
  try {
    task.value = await getOtaTask(taskId.value)
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '任务详情加载失败')
  } finally {
    loading.value = false
  }
}

async function loadRecords(): Promise<void> {
  if (!taskId.value) {
    return
  }
  recordLoading.value = true
  try {
    const data = await listOtaTaskRecords(taskId.value, { ...recordQuery })
    records.value = data.items
    recordTotal.value = data.total
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '升级记录加载失败')
  } finally {
    recordLoading.value = false
  }
}

/* ------------------------------ 批次推进动作 ------------------------------ */
const actionLoading = ref(false)

/** 启动/继续当前批次（服务端按门禁放行，返回 released/blocked 明细） */
async function handleAdvance(action: 'start' | 'resume'): Promise<void> {
  actionLoading.value = true
  try {
    const data =
      action === 'start'
        ? await startOtaTask(taskId.value, {})
        : await resumeOtaTask(taskId.value, {})
    ElMessage.success(
      `批次 ${data.batch_no ?? '-'} 已${action === 'start' ? '启动' : '继续'}：放行 ${data.released_count} 台，阻塞 ${data.blocked_count} 台`,
    )
    await Promise.all([loadTask(), loadRecords()])
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '操作失败')
  } finally {
    actionLoading.value = false
  }
}

/** 暂停（成功率不达标时人工介入） */
async function handlePause(): Promise<void> {
  actionLoading.value = true
  try {
    const data = await pauseOtaTask(taskId.value, { reason: '控制台人工暂停' })
    ElMessage.success(`任务已暂停（状态 ${data.status}）`)
    await loadTask()
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '暂停失败')
  } finally {
    actionLoading.value = false
  }
}

/** 回滚（A/B 分区回退到上一分区） */
async function handleRollback(): Promise<void> {
  actionLoading.value = true
  try {
    const data = await rollbackOtaTask(taskId.value, { target: 'previous_slot', reason: '控制台人工回滚' })
    ElMessage.success(`回滚指令已下发：受理 ${data.accepted_count} 台，拒绝 ${data.rejected_count} 台`)
    await Promise.all([loadTask(), loadRecords()])
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '回滚失败')
  } finally {
    actionLoading.value = false
  }
}

onMounted(async () => {
  await vehicleStore.fetchVehicles()
  await Promise.all([loadTask(), loadRecords()])
})
</script>

<template>
  <div v-loading="loading" class="task-detail">
    <el-card shadow="never">
      <template #header>
        <div class="header">
          <div class="header__title">
            <span>{{ task?.task_name ?? '任务详情' }}</span>
            <StatusTag v-if="task" kind="ota-task" :value="task.status" />
          </div>
          <div class="header__actions">
            <el-button link @click="router.push({ name: 'ota-tasks' })">返回列表</el-button>
            <el-button
              v-if="task && ['created', 'pending_approval', 'paused'].includes(task.status)"
              v-permission="PERMISSIONS.otaExecute"
              type="primary"
              :loading="actionLoading"
              @click="handleAdvance('start')"
            >
              启动 / 继续批次
            </el-button>
            <el-button
              v-if="task?.status === 'running'"
              v-permission="PERMISSIONS.otaExecute"
              type="warning"
              :loading="actionLoading"
              @click="handlePause"
            >
              暂停
            </el-button>
            <el-button
              v-if="task?.status === 'paused'"
              v-permission="PERMISSIONS.otaExecute"
              type="primary"
              :loading="actionLoading"
              @click="handleAdvance('resume')"
            >
              继续
            </el-button>
            <el-button
              v-if="task && ['running', 'paused', 'failed'].includes(task.status)"
              v-permission="PERMISSIONS.otaExecute"
              type="danger"
              :loading="actionLoading"
              @click="handleRollback"
            >
              回滚（A/B 分区）
            </el-button>
          </div>
        </div>
      </template>

      <el-descriptions v-if="task" :column="3" size="small" border>
        <el-descriptions-item label="任务 ID">{{ task.task_id }}</el-descriptions-item>
        <el-descriptions-item label="目标版本">{{ task.target_version_id }}</el-descriptions-item>
        <el-descriptions-item label="车辆数">{{ task.vehicle_count ?? task.target_vehicles?.length ?? 0 }}</el-descriptions-item>
        <el-descriptions-item label="创建者">{{ task.creator }}</el-descriptions-item>
        <el-descriptions-item label="创建时间">{{ formatTime(task.create_time) }}</el-descriptions-item>
        <el-descriptions-item label="排期">
          {{ task.schedule?.mode === 'scheduled' ? `定时 ${formatTime(task.schedule.start_time ?? null)}` : '立即' }}
        </el-descriptions-item>
      </el-descriptions>
    </el-card>

    <el-row v-if="task" :gutter="12" class="task-detail__stats">
      <el-col :span="4">
        <StatCard title="总车辆" :value="task.progress.total" hint="任务目标车辆数" />
      </el-col>
      <el-col :span="4">
        <StatCard title="进行中" :value="task.progress.in_progress" tone="primary" hint="当前升级中" />
      </el-col>
      <el-col :span="4">
        <StatCard title="成功" :value="task.progress.succeeded" tone="success" hint="升级成功" />
      </el-col>
      <el-col :span="4">
        <StatCard title="失败" :value="task.progress.failed" :tone="task.progress.failed > 0 ? 'danger' : 'info'" hint="升级失败" />
      </el-col>
      <el-col :span="4">
        <StatCard title="已回滚" :value="task.progress.rolled_back" tone="warning" hint="分区已回退" />
      </el-col>
      <el-col :span="4">
        <StatCard
          title="成功率"
          :value="formatPercent(task.progress.success_rate ?? null)"
          :tone="(task.progress.success_rate ?? 0) >= 0.95 ? 'success' : 'danger'"
          hint="门禁阈值 95%（低于则暂停 + 告警）"
        />
      </el-col>
    </el-row>

    <el-card v-if="task?.rollout" shadow="never" class="task-detail__card">
      <template #header>
        <div class="header">
          <span>灰度批次推进（{{ task.rollout.current_batch }} / {{ task.rollout.total_batches }}）</span>
          <el-tag :type="task.rollout.next_action === 'halt' ? 'danger' : task.rollout.next_action === 'advance' ? 'success' : 'warning'">
            {{ nextActionLabels[task.rollout.next_action] ?? task.rollout.next_action }}
          </el-tag>
        </div>
      </template>

      <el-alert
        v-if="task.rollout.next_action === 'halt'"
        class="alert"
        type="error"
        :closable="false"
        show-icon
        :title="`批次已暂停，需人工介入：${task.rollout.halt_reason ?? '成功率低于 95% 阈值'}`"
      />

      <el-table :data="batches" size="small">
        <el-table-column prop="batch_no" label="批次" width="70" />
        <el-table-column label="比例" width="80">
          <template #default="{ row }">{{ row.percent }}%</template>
        </el-table-column>
        <el-table-column label="状态" width="110">
          <template #default="{ row }">
            <el-tag size="small" :type="row.status === 'passed' ? 'success' : row.status === 'halted' ? 'danger' : 'warning'">
              {{ batchStatusLabels[row.status] ?? row.status }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="进度" min-width="220">
          <template #default="{ row }">
            <el-progress :percentage="batchPercent(row.success_count, row.target_count)" :stroke-width="12" />
          </template>
        </el-table-column>
        <el-table-column label="目标/成功/失败/回滚" min-width="170">
          <template #default="{ row }">
            {{ row.target_count }} / {{ row.success_count }} / {{ row.failed_count }} / {{ row.rolled_back_count }}
          </template>
        </el-table-column>
        <el-table-column label="成功率" width="100">
          <template #default="{ row }">{{ formatPercent(row.success_rate ?? null) }}</template>
        </el-table-column>
        <el-table-column label="观察截止" width="170">
          <template #default="{ row }">{{ formatTime(row.observe_until ?? null) }}</template>
        </el-table-column>
      </el-table>
    </el-card>

    <el-card shadow="never" class="task-detail__card">
      <template #header>
        <div class="header">
          <span>单车升级记录</span>
          <div class="filters">
            <el-select v-model="recordQuery.vehicle_id" clearable filterable placeholder="全部车辆" size="small" style="width: 160px">
              <el-option
                v-for="vehicle in vehicleStore.vehicles"
                :key="vehicle.vehicle_id"
                :label="vehicle.vehicle_name || vehicle.vehicle_id"
                :value="vehicle.vehicle_id"
              />
            </el-select>
            <el-select v-model="recordQuery.status" clearable placeholder="全部状态" size="small" style="width: 140px">
              <el-option v-for="option in statusOptions" :key="option.value" :label="option.label" :value="option.value" />
            </el-select>
            <el-button size="small" type="primary" :loading="recordLoading" @click="loadRecords">查询</el-button>
          </div>
        </div>
      </template>

      <el-table v-loading="recordLoading" :data="records" size="small" empty-text="暂无升级记录">
        <el-table-column label="车辆" width="150">
          <template #default="{ row }">{{ vehicleStore.resolveName(row.vehicle_id) }}</template>
        </el-table-column>
        <el-table-column label="版本变更" width="200">
          <template #default="{ row }">{{ row.from_version ?? '-' }} → {{ row.to_version }}</template>
        </el-table-column>
        <el-table-column label="状态机阶段" width="150">
          <template #default="{ row }">
            <StatusTag kind="ota-upgrade" :value="row.phase" />
          </template>
        </el-table-column>
        <el-table-column label="进度" min-width="180">
          <template #default="{ row }">
            <el-progress :percentage="row.progress" :stroke-width="12" :status="row.status === 'FAILED' ? 'exception' : undefined" />
          </template>
        </el-table-column>
        <el-table-column label="阶段序号" width="100">
          <template #default="{ row }">{{ formatNumber(phaseIndex(row.phase) + 1, 0) }}/{{ upgradeFlow.length }}</template>
        </el-table-column>
        <el-table-column label="开始时间" width="170">
          <template #default="{ row }">{{ formatTime(row.start_time) }}</template>
        </el-table-column>
        <el-table-column label="耗时 (s)" width="110">
          <template #default="{ row }">{{ formatNumber(row.duration_seconds ?? null, 0) }}</template>
        </el-table-column>
        <el-table-column label="错误" min-width="180">
          <template #default="{ row }">
            <span v-if="row.error_code || row.error_message">{{ row.error_code ?? '' }} {{ row.error_message ?? '' }}</span>
            <span v-else>-</span>
          </template>
        </el-table-column>
      </el-table>

      <el-pagination
        class="pager"
        layout="total, prev, pager, next"
        :total="recordTotal"
        :page-size="recordQuery.page_size"
        @current-change="(page: number) => { recordQuery.page = page; loadRecords() }"
      />
    </el-card>
  </div>
</template>

<style scoped>
.header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
}

.header__title {
  display: flex;
  align-items: center;
  gap: 8px;
}

.header__actions,
.filters {
  display: flex;
  align-items: center;
  gap: 8px;
}

.task-detail__stats,
.task-detail__card {
  margin-top: 12px;
}

.alert {
  margin-bottom: 12px;
}

.pager {
  margin-top: 12px;
  text-align: right;
}
</style>
