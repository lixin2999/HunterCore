<script setup lang="ts">
/**
 * OTA 升级任务列表（ota-service.yaml）
 *
 * 契约流程：
 * - GET  /api/v1/ota/tasks（列表）→ POST /api/v1/ota/tasks（创建 201）
 * - POST /api/v1/ota/tasks/{task_id}/start|pause|resume|cancel（返回 OtaTaskActionData，
 *   含 released / blocked 车辆明细；门禁不满足时 blocked 非空或返回 6003）
 * - POST /api/v1/ota/tasks/{task_id}/rollback（A/B 分区回退）
 * 灰度策略：5% → 20% → 50% → 100%，每批观察 24h、成功率阈值 95%（系统约束第 14 条，前端不可改）。
 */
import { onMounted, reactive, ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'

import {
  cancelOtaTask,
  createOtaTask,
  getOtaTask,
  listOtaTasks,
  listOtaVersions,
  pauseOtaTask,
  resumeOtaTask,
  rollbackOtaTask,
  startOtaTask,
} from '@/api/ota'
import StatusTag from '@/components/common/StatusTag.vue'
import {
  DEFAULT_PAGE_SIZE,
  OTA_DEFAULT_CANARY_BATCHES,
  OTA_DEFAULT_PRECONDITIONS,
  OTA_PRECONDITION_LABELS,
  OTA_TASK_STATUS_LABELS,
} from '@/constants'
import { PERMISSIONS } from '@/constants/permissions'
import { useVehicleStore } from '@/stores/vehicle'
import type {
  OtaTaskActionData,
  OtaTaskCreateRequest,
  OtaTaskDetail,
  OtaTaskItem,
  OtaTaskListQuery,
  OtaTaskRollbackData,
  OtaVersionItem,
} from '@/types/ota'
import { HunterApiError } from '@/utils/error-code'
import { formatNumber, formatPercent, formatTime } from '@/utils/format'

const router = useRouter()
const vehicleStore = useVehicleStore()

const loading = ref(false)
const tasks = ref<OtaTaskItem[]>([])
const total = ref(0)
const query = reactive<OtaTaskListQuery>({
  status: undefined,
  target_version_id: undefined,
  page: 1,
  page_size: DEFAULT_PAGE_SIZE,
})

const statusOptions = Object.entries(OTA_TASK_STATUS_LABELS).map(([value, label]) => ({ value, label }))
const preconditionLabels = OTA_PRECONDITION_LABELS

/** 版本下拉选项（仅 published 版本可被任务引用） */
const publishedVersions = ref<OtaVersionItem[]>([])

async function load(): Promise<void> {
  loading.value = true
  try {
    const data = await listOtaTasks({ ...query })
    tasks.value = data.items
    total.value = data.total
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '任务列表加载失败')
  } finally {
    loading.value = false
  }
}

async function loadVersions(): Promise<void> {
  try {
    const data = await listOtaVersions({ status: 'published', page: 1, page_size: 200 })
    publishedVersions.value = data.items
  } catch {
    // 版本缺失不阻断列表展示
  }
}

/** 创建任务弹窗 */
const createVisible = ref(false)
const creating = ref(false)
const createForm = reactive<OtaTaskCreateRequest>({
  task_name: '',
  target_version_id: '',
  target_vehicles: [],
  upgrade_strategy: {
    batches: OTA_DEFAULT_CANARY_BATCHES.map((batch) => ({ ...batch })),
    stage_gate: true,
  },
  schedule: { mode: 'immediate', start_time: null, window_end: null },
  preconditions: { ...OTA_DEFAULT_PRECONDITIONS },
})

async function submitCreate(): Promise<void> {
  if (!createForm.task_name || !createForm.target_version_id || createForm.target_vehicles.length === 0) {
    ElMessage.warning('请填写任务名称、选择目标版本与目标车辆')
    return
  }
  creating.value = true
  try {
    const created = await createOtaTask({ ...createForm })
    ElMessage.success('任务已创建，请在任务详情中启动首批发（5%）')
    createVisible.value = false
    await load()
    await router.push({ name: 'ota-task-detail', params: { task_id: created.task_id } })
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '任务创建失败')
  } finally {
    creating.value = false
  }
}

/* ------------------------------ 任务动作 ------------------------------ */
const actionLoadingId = ref<string | null>(null)
/** 启动/暂停/继续/取消结果（含门禁放行与阻塞明细） */
const actionResult = ref<OtaTaskActionData | null>(null)
const actionVisible = ref(false)
/** 回滚结果明细 */
const rollbackResult = ref<OtaTaskRollbackData | null>(null)
const rollbackVisible = ref(false)

async function confirmAction(message: string, title = '确认操作'): Promise<boolean> {
  try {
    await ElMessageBox.confirm(message, title, { type: 'warning' })
    return true
  } catch {
    return false
  }
}

/** 启动批次（批量放行受门禁约束：SOC ≥ 50%、P 档静止、网络稳定、存储 ≥ 2GB） */
async function handleStart(row: OtaTaskItem): Promise<void> {
  if (!(await confirmAction(`确认启动任务「${row.task_name}」当前灰度批次？`))) {
    return
  }
  actionLoadingId.value = row.task_id
  try {
    actionResult.value = await startOtaTask(row.task_id, {})
    actionVisible.value = true
    await load()
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '启动失败')
  } finally {
    actionLoadingId.value = null
  }
}

async function handlePause(row: OtaTaskItem): Promise<void> {
  let reason = ''
  try {
    const result = await ElMessageBox.prompt('暂停后当前批次不再下发新车辆（已下发车辆继续完成）。请输入暂停原因：', '暂停任务', {
      inputPattern: /.+/,
      inputErrorMessage: '请填写暂停原因',
    })
    reason = result.value
  } catch {
    return
  }
  actionLoadingId.value = row.task_id
  try {
    actionResult.value = await pauseOtaTask(row.task_id, { reason })
    actionVisible.value = true
    await load()
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '暂停失败')
  } finally {
    actionLoadingId.value = null
  }
}

async function handleResume(row: OtaTaskItem): Promise<void> {
  actionLoadingId.value = row.task_id
  try {
    actionResult.value = await resumeOtaTask(row.task_id, {})
    actionVisible.value = true
    await load()
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '继续失败')
  } finally {
    actionLoadingId.value = null
  }
}

async function handleCancel(row: OtaTaskItem): Promise<void> {
  let reason = ''
  try {
    const result = await ElMessageBox.prompt('取消为不可逆操作（已升级车辆不自动回滚）。请输入取消原因：', '取消任务', {
      inputPattern: /.+/,
      inputErrorMessage: '请填写取消原因',
    })
    reason = result.value
  } catch {
    return
  }
  actionLoadingId.value = row.task_id
  try {
    actionResult.value = await cancelOtaTask(row.task_id, { reason })
    actionVisible.value = true
    await load()
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '取消失败')
  } finally {
    actionLoadingId.value = null
  }
}

/** 回滚（A/B 分区回退到上一分区） */
async function handleRollback(row: OtaTaskItem): Promise<void> {
  let reason = ''
  try {
    const result = await ElMessageBox.prompt(
      '回滚将向目标车辆下发 A/B 分区回退指令（默认回退至上一分区）。请输入回滚原因：',
      '回滚任务',
      { inputPattern: /.+/, inputErrorMessage: '请填写回滚原因' },
    )
    reason = result.value
  } catch {
    return
  }
  actionLoadingId.value = row.task_id
  try {
    rollbackResult.value = await rollbackOtaTask(row.task_id, { target: 'previous_slot', reason })
    rollbackVisible.value = true
    await load()
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '回滚失败')
  } finally {
    actionLoadingId.value = null
  }
}

/** 查看门禁预检（不启动批次，仅拉取最新任务详情） */
async function handlePreviewGate(row: OtaTaskItem): Promise<void> {
  try {
    const detail: OtaTaskDetail = await getOtaTask(row.task_id)
    const blocked = detail.blocked_vehicles ?? []
    if (blocked.length === 0) {
      ElMessage.success('当前无门禁阻塞车辆，可执行批次启动')
      return
    }
    await ElMessageBox.alert(
      `${blocked.length} 台车辆不满足门禁条件，将在启动批次时被跳过（服务端复核为准）。`,
      '门禁预检',
      { type: 'warning' },
    )
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '预检失败')
  }
}

/** 进度文案（成功/总数 + 成功率） */
function progressText(row: OtaTaskItem): string {
  const progress = row.progress
  const rate = progress.success_rate ?? null
  return `${progress.succeeded}/${progress.total}（成功率 ${formatPercent(rate)}）`
}

/** 可启动批次的终态判定（created / 待审批 / 已暂停 允许启动或续跑） */
function canStart(status: string): boolean {
  return status === 'created' || status === 'pending_approval' || status === 'paused'
}

onMounted(async () => {
  await Promise.all([load(), loadVersions(), vehicleStore.fetchVehicles()])
})
</script>

<template>
  <div v-loading="loading">
    <el-form :inline="true">
      <el-form-item label="状态">
        <el-select v-model="query.status" clearable placeholder="全部状态" style="width: 150px">
          <el-option v-for="option in statusOptions" :key="option.value" :label="option.label" :value="option.value" />
        </el-select>
      </el-form-item>
      <el-form-item label="目标版本">
        <el-select v-model="query.target_version_id" clearable placeholder="全部版本" style="width: 200px">
          <el-option
            v-for="version in publishedVersions"
            :key="version.version_id"
            :label="`${version.version_name}（code ${version.version_code}）`"
            :value="version.version_id"
          />
        </el-select>
      </el-form-item>
      <el-form-item>
        <el-button type="primary" :loading="loading" @click="load">查询</el-button>
        <el-button v-permission="PERMISSIONS.otaCreate" type="success" @click="createVisible = true">新建升级任务</el-button>
      </el-form-item>
    </el-form>

    <el-table :data="tasks" size="small" empty-text="暂无升级任务">
      <el-table-column label="任务名称" min-width="180">
        <template #default="{ row }">
          <el-button link type="primary" @click="router.push({ name: 'ota-task-detail', params: { task_id: row.task_id } })">
            {{ row.task_name }}
          </el-button>
        </template>
      </el-table-column>
      <el-table-column label="状态" width="110">
        <template #default="{ row }">
          <StatusTag kind="ota-task" :value="row.status" />
        </template>
      </el-table-column>
      <el-table-column label="当前批次" width="100">
        <template #default="{ row }">
          {{ formatNumber(row.progress.current_batch ?? 0, 0) }} / 4
        </template>
      </el-table-column>
      <el-table-column label="车辆数" width="90" prop="vehicle_count" />
      <el-table-column label="进度" min-width="200">
        <template #default="{ row }">{{ progressText(row) }}</template>
      </el-table-column>
      <el-table-column label="创建者" width="120" prop="creator" />
      <el-table-column label="创建时间" width="170">
        <template #default="{ row }">{{ formatTime(row.create_time) }}</template>
      </el-table-column>
      <el-table-column label="操作" width="300" fixed="right">
        <template #default="{ row }">
          <el-button v-permission="PERMISSIONS.otaRead" link type="info" @click="handlePreviewGate(row)">门禁预检</el-button>
          <el-button
            v-if="canStart(row.status)"
            v-permission="PERMISSIONS.otaExecute"
            link
            type="success"
            :loading="actionLoadingId === row.task_id"
            @click="handleStart(row)"
          >
            启动批次
          </el-button>
          <el-button
            v-if="row.status === 'running'"
            v-permission="PERMISSIONS.otaExecute"
            link
            type="warning"
            :loading="actionLoadingId === row.task_id"
            @click="handlePause(row)"
          >
            暂停
          </el-button>
          <el-button
            v-if="row.status === 'paused'"
            v-permission="PERMISSIONS.otaExecute"
            link
            type="primary"
            :loading="actionLoadingId === row.task_id"
            @click="handleResume(row)"
          >
            继续
          </el-button>
          <el-button
            v-if="['running', 'paused', 'pending_approval'].includes(row.status)"
            v-permission="PERMISSIONS.otaExecute"
            link
            type="danger"
            :loading="actionLoadingId === row.task_id"
            @click="handleCancel(row)"
          >
            取消
          </el-button>
          <el-button
            v-if="['running', 'paused', 'failed'].includes(row.status)"
            v-permission="PERMISSIONS.otaExecute"
            link
            type="danger"
            :loading="actionLoadingId === row.task_id"
            @click="handleRollback(row)"
          >
            回滚
          </el-button>
        </template>
      </el-table-column>
    </el-table>

    <el-pagination
      class="pager"
      layout="total, prev, pager, next"
      :total="total"
      :page-size="query.page_size"
      @current-change="(page: number) => { query.page = page; load() }"
    />

    <!-- 动作结果：门禁放行 / 阻塞明细 -->
    <el-dialog v-model="actionVisible" title="任务动作结果" width="620px">
      <template v-if="actionResult">
        <el-descriptions :column="2" size="small" border>
          <el-descriptions-item label="任务">{{ actionResult.task_id }}</el-descriptions-item>
          <el-descriptions-item label="动作">{{ actionResult.action }}</el-descriptions-item>
          <el-descriptions-item label="任务状态">{{ actionResult.status }}</el-descriptions-item>
          <el-descriptions-item label="批次号">{{ actionResult.batch_no ?? '-' }}</el-descriptions-item>
          <el-descriptions-item label="放行车辆">{{ actionResult.released_count }}</el-descriptions-item>
          <el-descriptions-item label="阻塞车辆">{{ actionResult.blocked_count }}</el-descriptions-item>
        </el-descriptions>
        <el-table :data="actionResult.blocked_vehicles ?? []" size="small" class="dialog-table" empty-text="无门禁阻塞车辆">
          <el-table-column label="车辆" width="150">
            <template #default="{ row }">{{ vehicleStore.resolveName(row.vehicle_id) }}</template>
          </el-table-column>
          <el-table-column label="未满足条件" min-width="220">
            <template #default="{ row }">
              <el-tag v-for="condition in row.failed_conditions" :key="condition" size="small" type="warning" class="tag">
                {{ preconditionLabels[condition] ?? condition }}
              </el-tag>
            </template>
          </el-table-column>
        </el-table>
      </template>
    </el-dialog>

    <!-- 回滚结果 -->
    <el-dialog v-model="rollbackVisible" title="回滚指令下发结果" width="640px">
      <template v-if="rollbackResult">
        <el-descriptions :column="3" size="small" border>
          <el-descriptions-item label="任务">{{ rollbackResult.task_id }}</el-descriptions-item>
          <el-descriptions-item label="回滚目标">{{ rollbackResult.target ?? 'previous_slot' }}</el-descriptions-item>
          <el-descriptions-item label="请求车辆数">{{ rollbackResult.requested_count }}</el-descriptions-item>
          <el-descriptions-item label="已受理">{{ rollbackResult.accepted_count }}</el-descriptions-item>
          <el-descriptions-item label="已拒绝">{{ rollbackResult.rejected_count }}</el-descriptions-item>
          <el-descriptions-item label="执行时间">{{ formatTime(rollbackResult.executed_at ?? null) }}</el-descriptions-item>
        </el-descriptions>
        <el-table :data="rollbackResult.results" size="small" class="dialog-table">
          <el-table-column label="车辆" width="150">
            <template #default="{ row }">{{ vehicleStore.resolveName(row.vehicle_id) }}</template>
          </el-table-column>
          <el-table-column label="是否受理" width="100">
            <template #default="{ row }">
              <el-tag :type="row.accepted ? 'success' : 'danger'" size="small">{{ row.accepted ? '已受理' : '已拒绝' }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column label="当前状态" width="120">
            <template #default="{ row }">
              <StatusTag v-if="row.current_status" kind="ota-upgrade" :value="row.current_status" />
              <span v-else>-</span>
            </template>
          </el-table-column>
          <el-table-column prop="reason" label="说明" min-width="160" show-overflow-tooltip />
        </el-table>
      </template>
    </el-dialog>

    <!-- 新建任务（灰度策略与门禁为契约固定值，不允许前端修改） -->
    <el-dialog v-model="createVisible" title="新建 OTA 升级任务" width="720px">
      <el-form :model="createForm" label-width="120px">
        <el-form-item label="任务名称">
          <el-input v-model="createForm.task_name" placeholder="如：v1.2.0 全量灰度升级" maxlength="128" />
        </el-form-item>
        <el-form-item label="目标版本">
          <el-select v-model="createForm.target_version_id" filterable placeholder="仅可选中已发布版本" style="width: 100%">
            <el-option
              v-for="version in publishedVersions"
              :key="version.version_id"
              :label="`${version.version_name}（code ${version.version_code}）`"
              :value="version.version_id"
            />
          </el-select>
        </el-form-item>
        <el-form-item label="目标车辆">
          <el-select v-model="createForm.target_vehicles" multiple filterable placeholder="选择目标车辆" style="width: 100%">
            <el-option
              v-for="vehicle in vehicleStore.vehicles"
              :key="vehicle.vehicle_id"
              :label="`${vehicle.vehicle_name || vehicle.vehicle_id}（SOC ${formatNumber(vehicle.battery_soc, 0)}%）`"
              :value="vehicle.vehicle_id"
            />
          </el-select>
        </el-form-item>
        <el-form-item label="排期">
          <el-select v-model="createForm.schedule!.mode" style="width: 160px">
            <el-option label="立即" value="immediate" />
            <el-option label="定时" value="scheduled" />
          </el-select>
          <el-date-picker
            v-if="createForm.schedule!.mode === 'scheduled'"
            v-model="createForm.schedule!.start_time"
            type="datetime"
            value-format="x"
            placeholder="开始时间"
            style="margin-left: 12px"
          />
        </el-form-item>
        <el-divider content-position="left">灰度策略（契约固定：5% → 20% → 50% → 100%，每批观察 24h，成功率阈值 95%）</el-divider>
        <el-table :data="createForm.upgrade_strategy!.batches" size="small">
          <el-table-column prop="batch_no" label="批次" width="80" />
          <el-table-column label="比例" width="100">
            <template #default="{ row }">{{ row.percent }}%</template>
          </el-table-column>
          <el-table-column label="观察时长" width="120">
            <template #default="{ row }">{{ row.observe_hours }} 小时</template>
          </el-table-column>
          <el-table-column label="成功率阈值">
            <template #default="{ row }">{{ formatPercent(row.success_rate_threshold) }}</template>
          </el-table-column>
        </el-table>
        <el-divider content-position="left">升级门禁（契约固定值）</el-divider>
        <el-form-item label="SOC 下限">
          <el-input-number v-model="createForm.preconditions!.soc_min" :min="0" :max="100" disabled />
        </el-form-item>
        <el-form-item label="必须 P 档静止">
          <el-switch v-model="createForm.preconditions!.must_be_parked" disabled />
        </el-form-item>
        <el-form-item label="网络稳定">
          <el-switch v-model="createForm.preconditions!.network_stable" disabled />
        </el-form-item>
        <el-form-item label="最小存储 (MB)">
          <el-input-number v-model="createForm.preconditions!.min_storage_mb" :min="0" disabled />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="createVisible = false">取消</el-button>
        <el-button type="primary" :loading="creating" @click="submitCreate">创建任务</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<style scoped>
.pager {
  margin-top: 12px;
  text-align: right;
}

.dialog-table {
  margin-top: 12px;
}

.tag {
  margin-right: 4px;
}
</style>
