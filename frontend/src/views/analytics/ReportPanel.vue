<script setup lang="ts">
/**
 * 分析报告（data-analytics 模块）
 *
 * 契约（data-analytics.yaml）：
 * - GET  /api/v1/analytics/reports（列表，支持 report_type/status/vehicle_id）
 * - POST /api/v1/analytics/reports/generate（202 受理，返回 report_id + poll_url）
 * - GET  /api/v1/analytics/reports/{report_id}（轮询生成状态）
 * 流程：提交生成 → 前端按 5s 间隔轮询至 ready/failed → 通过 artifacts[].download_url 下载。
 */
import { computed, onMounted, reactive, ref } from 'vue'
import { ElMessage } from 'element-plus'

import { generateReport, getReport, listReports } from '@/api/analytics'
import StatusTag from '@/components/common/StatusTag.vue'
import { DEFAULT_PAGE_SIZE, REPORT_FORMAT_LABELS, REPORT_TYPE_LABELS } from '@/constants'
import { PERMISSIONS } from '@/constants/permissions'
import { useVehicleStore } from '@/stores/vehicle'
import type { ReportGenerateRequest, ReportListData, ReportListQuery } from '@/types/analytics'
import { HunterApiError } from '@/utils/error-code'
import { downloadByUrl } from '@/utils/hash'
import { formatBytes, formatTime } from '@/utils/format'

/** 报告状态轮询间隔（生成中才轮询；与附录 D 限流兼容） */
const REPORT_POLL_INTERVAL_MS = 5000

const vehicleStore = useVehicleStore()

const query = reactive<ReportListQuery>({
  report_type: undefined,
  vehicle_id: undefined,
  status: undefined,
  page: 1,
  page_size: DEFAULT_PAGE_SIZE,
})
const loading = ref(false)
const result = ref<ReportListData | null>(null)
const pollTimer = ref<number | null>(null)

const reportTypeOptions = Object.entries(REPORT_TYPE_LABELS).map(([value, label]) => ({ value, label }))
const reportFormatOptions = Object.entries(REPORT_FORMAT_LABELS).map(([value, label]) => ({ value, label }))

/** 生成表单 */
const generateVisible = ref(false)
const generating = ref(false)
const range = ref<[number, number] | null>(null)
const generateForm = reactive<ReportGenerateRequest>({
  report_type: 'vehicle_daily',
  vehicle_id: null,
  start_time: 0,
  end_time: 0,
  formats: ['html', 'pdf'],
  force: false,
})

const hasPendingReport = computed<boolean>(() =>
  (result.value?.items ?? []).some((item) => item.status === 'pending' || item.status === 'generating'),
)

async function load(): Promise<void> {
  loading.value = true
  try {
    result.value = await listReports({ ...query })
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '报告列表加载失败')
  } finally {
    loading.value = false
  }
}

/** 当前唯一进行中的报告 ID（用于状态轮询） */
function pendingReportId(): string | null {
  const pending = (result.value?.items ?? []).find(
    (item) => item.status === 'pending' || item.status === 'generating',
  )
  return pending?.report_id ?? null
}

/** 轮询生成状态，直到 ready/failed 后停止 */
function schedulePolling(): void {
  if (pollTimer.value !== null) {
    return
  }
  pollTimer.value = window.setInterval(async () => {
    const reportId = pendingReportId()
    if (!reportId) {
      window.clearInterval(pollTimer.value ?? undefined)
      pollTimer.value = null
      return
    }
    try {
      await getReport(reportId)
      await load()
    } catch {
      // 轮询异常下次重试，不打断页面
    }
  }, REPORT_POLL_INTERVAL_MS)
}

async function submitGenerate(): Promise<void> {
  if (!range.value) {
    ElMessage.warning('请选择报告时间范围')
    return
  }
  const [start, end] = range.value
  generateForm.start_time = start
  generateForm.end_time = end
  generating.value = true
  try {
    const data = await generateReport({ ...generateForm })
    ElMessage.success(`报告生成已受理（report_id=${data.report_id}，状态 ${data.status}）`)
    generateVisible.value = false
    await load()
    schedulePolling()
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '报告生成请求失败')
  } finally {
    generating.value = false
  }
}

/** 下载报告产物（预签名 URL） */
function handleDownload(objectKey: string, downloadUrl?: string | null): void {
  if (!downloadUrl) {
    ElMessage.warning('该产物暂未提供下载链接，请稍后刷新')
    return
  }
  downloadByUrl(downloadUrl, objectKey.split('/').pop())
}

onMounted(async () => {
  await vehicleStore.fetchVehicles()
  await load()
  schedulePolling()
})
</script>

<template>
  <div v-loading="loading">
    <el-form :inline="true">
      <el-form-item label="报告类型">
        <el-select v-model="query.report_type" clearable placeholder="全部类型" style="width: 160px">
          <el-option v-for="option in reportTypeOptions" :key="option.value" :label="option.label" :value="option.value" />
        </el-select>
      </el-form-item>
      <el-form-item label="车辆">
        <el-select v-model="query.vehicle_id" clearable filterable placeholder="全部车辆" style="width: 180px">
          <el-option
            v-for="vehicle in vehicleStore.vehicles"
            :key="vehicle.vehicle_id"
            :label="vehicle.vehicle_name || vehicle.vehicle_id"
            :value="vehicle.vehicle_id"
          />
        </el-select>
      </el-form-item>
      <el-form-item label="状态">
        <el-select v-model="query.status" clearable placeholder="全部状态" style="width: 140px">
          <el-option label="排队中" value="pending" />
          <el-option label="生成中" value="generating" />
          <el-option label="可下载" value="ready" />
          <el-option label="生成失败" value="failed" />
        </el-select>
      </el-form-item>
      <el-form-item>
        <el-button type="primary" :loading="loading" @click="load">查询</el-button>
        <el-button v-permission="PERMISSIONS.analyticsExecute" type="success" @click="generateVisible = true">
          生成报告
        </el-button>
      </el-form-item>
    </el-form>

    <el-alert
      v-if="hasPendingReport"
      class="notice"
      type="info"
      :closable="false"
      show-icon
      title="存在生成中的报告，页面每 5s 自动刷新状态"
    />

    <el-table :data="result?.items ?? []" size="small" empty-text="暂无报告">
      <el-table-column prop="report_id" label="报告 ID" width="220" show-overflow-tooltip />
      <el-table-column label="类型" width="120">
        <template #default="{ row }">{{ REPORT_TYPE_LABELS[row.report_type] ?? row.report_type }}</template>
      </el-table-column>
      <el-table-column label="车辆" width="140">
        <template #default="{ row }">{{ row.vehicle_id ? vehicleStore.resolveName(row.vehicle_id) : '全平台' }}</template>
      </el-table-column>
      <el-table-column label="状态" width="110">
        <template #default="{ row }">
          <StatusTag kind="report" :value="row.status" />
        </template>
      </el-table-column>
      <el-table-column label="数据窗口" min-width="240">
        <template #default="{ row }">
          {{ formatTime(row.start_time ?? null) }} ~ {{ formatTime(row.end_time ?? null) }}
        </template>
      </el-table-column>
      <el-table-column label="创建时间" width="170">
        <template #default="{ row }">{{ formatTime(row.created_at) }}</template>
      </el-table-column>
      <el-table-column label="产物" min-width="260">
        <template #default="{ row }">
          <template v-if="row.artifacts?.length">
            <el-button
              v-for="artifact in row.artifacts"
              :key="artifact.object_key"
              link
              type="primary"
              @click="handleDownload(artifact.object_key, artifact.download_url)"
            >
              {{ REPORT_FORMAT_LABELS[artifact.format] ?? artifact.format }}（{{ formatBytes(artifact.size_bytes) }}）
            </el-button>
          </template>
          <span v-else>-</span>
        </template>
      </el-table-column>
      <el-table-column label="触发方式" width="100">
        <template #default="{ row }">{{ row.trigger === 'schedule' ? '定时' : '手动' }}</template>
      </el-table-column>
    </el-table>

    <el-pagination
      class="pager"
      layout="total, prev, pager, next"
      :total="result?.total ?? 0"
      :page-size="query.page_size"
      @current-change="(page: number) => { query.page = page; load() }"
    />

    <el-dialog v-model="generateVisible" title="生成分析报告" width="520px">
      <el-form :model="generateForm" label-width="110px">
        <el-form-item label="报告类型">
          <el-select v-model="generateForm.report_type" style="width: 100%">
            <el-option v-for="option in reportTypeOptions" :key="option.value" :label="option.label" :value="option.value" />
          </el-select>
        </el-form-item>
        <el-form-item label="车辆（可选）">
          <el-select v-model="generateForm.vehicle_id" clearable filterable placeholder="不选则为全平台" style="width: 100%">
            <el-option
              v-for="vehicle in vehicleStore.vehicles"
              :key="vehicle.vehicle_id"
              :label="vehicle.vehicle_name || vehicle.vehicle_id"
              :value="vehicle.vehicle_id"
            />
          </el-select>
        </el-form-item>
        <el-form-item label="数据窗口">
          <el-date-picker v-model="range" type="datetimerange" value-format="x" start-placeholder="开始" end-placeholder="结束" />
        </el-form-item>
        <el-form-item label="输出格式">
          <el-select v-model="generateForm.formats" multiple style="width: 100%">
            <el-option v-for="option in reportFormatOptions" :key="option.value" :label="option.label" :value="option.value" />
          </el-select>
        </el-form-item>
        <el-form-item label="强制重算">
          <el-switch v-model="generateForm.force" />
          <span class="hint">开启后将忽略缓存结果重新计算（耗时更长）</span>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="generateVisible = false">取消</el-button>
        <el-button type="primary" :loading="generating" @click="submitGenerate">提交生成</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<style scoped>
.notice,
.pager {
  margin-top: 12px;
}

.pager {
  text-align: right;
}

.hint {
  margin-left: 8px;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}
</style>
