<script setup lang="ts">
/**
 * 操控记录（remote-control.yaml）
 *
 * 契约：
 * - GET /api/v1/remote/history（来源 MinIO sidecar 归档，保留 90 天，查询跨度上限 31 天 → 超限 2001）
 * - GET /api/v1/remote/history/{session_id}（含原始 sidecar 与 video_available）
 * - GET /api/v1/remote/history/{session_id}/video（预签名 15 分钟 + HTTP Range 分片，可 seek）
 * 数据权限：普通用户仅可查询自身 operator_id 记录；管理员可查全部（服务端强制）。
 */
import { computed, onMounted, reactive, ref } from 'vue'
import { ElMessage } from 'element-plus'

import { getControlHistory, getControlVideoAccess, listControlHistory } from '@/api/remote'
import StatusTag from '@/components/common/StatusTag.vue'
import { DEFAULT_PAGE_SIZE, RC_LIMITS, SESSION_END_REASON_LABELS } from '@/constants'
import { useVehicleStore } from '@/stores/vehicle'
import type { ControlHistoryDetail, ControlHistoryItem, ControlHistoryQuery } from '@/types/remote'
import { HunterApiError } from '@/utils/error-code'
import { formatBytes, formatDuration, formatNumber, formatTime } from '@/utils/format'

const vehicleStore = useVehicleStore()

const loading = ref(false)
const items = ref<ControlHistoryItem[]>([])
const total = ref(0)
const retentionDays = ref<number | null>(null)
const source = ref<string>('')

const query = reactive<ControlHistoryQuery>({
  vehicle_id: undefined,
  operator_id: undefined,
  end_reason: undefined,
  page: 1,
  page_size: DEFAULT_PAGE_SIZE,
})
const startedRange = ref<[number, number] | null>(null)

const endReasonOptions = Object.entries(SESSION_END_REASON_LABELS).map(([value, label]) => ({ value, label }))

/** 时间跨度是否超出契约上限（31 天 → 服务端返回 2001） */
const rangeTooLong = computed<boolean>(() => {
  const range = startedRange.value
  if (!range) {
    return false
  }
  return range[1] - range[0] > RC_LIMITS.historyMaxRangeDays * 86_400
})

async function load(): Promise<void> {
  if (rangeTooLong.value) {
    ElMessage.warning(`查询跨度不得超过 ${RC_LIMITS.historyMaxRangeDays} 天`)
    return
  }
  loading.value = true
  try {
    const [start, end] = startedRange.value ?? []
    const data = await listControlHistory({
      ...query,
      started_from: start,
      started_to: end,
    })
    items.value = data.items
    total.value = data.total
    retentionDays.value = data.retention_days ?? null
    source.value = data.source ?? ''
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '操控记录加载失败')
  } finally {
    loading.value = false
  }
}

/* ------------------------------ 详情与录像 ------------------------------ */
const detailVisible = ref(false)
const detailLoading = ref(false)
const detail = ref<ControlHistoryDetail | null>(null)
const sidecarText = computed<string>(() =>
  detail.value?.sidecar ? JSON.stringify(detail.value.sidecar, null, 2) : '（sidecar 内容不可用）',
)

async function openDetail(row: ControlHistoryItem): Promise<void> {
  detailVisible.value = true
  detailLoading.value = true
  detail.value = null
  try {
    detail.value = await getControlHistory(row.session_id)
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '记录详情加载失败')
  } finally {
    detailLoading.value = false
  }
}

const videoVisible = ref(false)
const videoUrl = ref('')
const videoLoading = ref(false)

/** 获取预签名录像地址（15 分钟有效，禁止缓存/落日志） */
async function openVideo(row: ControlHistoryItem | ControlHistoryDetail): Promise<void> {
  videoLoading.value = true
  try {
    const access = await getControlVideoAccess(row.session_id)
    videoUrl.value = access.video_url
    videoVisible.value = true
    ElMessage.info(`录像链接有效期 ${formatNumber(access.expires_in / 60, 0)} 分钟，支持 Range 分片拖动`)
  } catch (error) {
    // 3001：录像已过 90 天保留期或未封存
    ElMessage.error(error instanceof HunterApiError ? error.message : '录像获取失败')
  } finally {
    videoLoading.value = false
  }
}

onMounted(async () => {
  await vehicleStore.fetchVehicles()
  await load()
})
</script>

<template>
  <div v-loading="loading">
    <el-form :inline="true">
      <el-form-item label="车辆">
        <el-select v-model="query.vehicle_id" clearable filterable placeholder="全部车辆" style="width: 170px">
          <el-option
            v-for="vehicle in vehicleStore.vehicles"
            :key="vehicle.vehicle_id"
            :label="vehicle.vehicle_name || vehicle.vehicle_id"
            :value="vehicle.vehicle_id"
          />
        </el-select>
      </el-form-item>
      <el-form-item label="操作员 ID">
        <el-input v-model="query.operator_id" clearable placeholder="operator_id（UUID）" style="width: 200px" />
      </el-form-item>
      <el-form-item label="结束原因">
        <el-select v-model="query.end_reason" clearable placeholder="全部原因" style="width: 160px">
          <el-option v-for="option in endReasonOptions" :key="option.value" :label="option.label" :value="option.value" />
        </el-select>
      </el-form-item>
      <el-form-item label="会话开始时间">
        <el-date-picker
          v-model="startedRange"
          type="datetimerange"
          value-format="x"
          start-placeholder="开始"
          end-placeholder="结束"
          unlink-panels
        />
      </el-form-item>
      <el-form-item>
        <el-button type="primary" :loading="loading" @click="load">查询</el-button>
      </el-form-item>
    </el-form>

    <el-alert
      v-if="rangeTooLong"
      class="alert"
      type="warning"
      :closable="false"
      show-icon
      :title="`查询跨度超过 ${RC_LIMITS.historyMaxRangeDays} 天（契约 RC_HISTORY_QUERY_MAX_RANGE_DAYS），请缩小范围`"
    />

    <div class="meta">
      <el-tag v-if="source" size="small" type="info" effect="plain">来源：{{ source }}</el-tag>
      <el-tag v-if="retentionDays" size="small" type="warning" effect="plain">录像保留 {{ retentionDays }} 天</el-tag>
    </div>

    <el-table :data="items" size="small" empty-text="暂无操控记录">
      <el-table-column label="会话开始" width="170">
        <template #default="{ row }">{{ formatTime(row.started_at) }}</template>
      </el-table-column>
      <el-table-column label="车辆" width="140">
        <template #default="{ row }">{{ vehicleStore.resolveName(row.vehicle_id) }}</template>
      </el-table-column>
      <el-table-column label="操作员" width="150">
        <template #default="{ row }">{{ row.operator_name || row.operator_id }}</template>
      </el-table-column>
      <el-table-column label="时长" width="110">
        <template #default="{ row }">{{ formatDuration(row.duration_s) }}</template>
      </el-table-column>
      <el-table-column label="结束原因" width="140">
        <template #default="{ row }">{{ SESSION_END_REASON_LABELS[row.end_reason] ?? row.end_reason }}</template>
      </el-table-column>
      <el-table-column label="指令 / 回执" width="130">
        <template #default="{ row }">{{ row.commands_sent ?? 0 }} / {{ row.commands_acked ?? 0 }}</template>
      </el-table-column>
      <el-table-column label="回执 P95" width="110">
        <template #default="{ row }">{{ formatNumber(row.ack_latency_ms_p95 ?? null, 0) }} ms</template>
      </el-table-column>
      <el-table-column label="视频延迟 P95" width="130">
        <template #default="{ row }">{{ formatNumber(row.video_e2e_latency_ms_p95 ?? null, 0) }} ms</template>
      </el-table-column>
      <el-table-column label="录像大小" width="110">
        <template #default="{ row }">{{ formatBytes(row.video_size_bytes ?? null) }}</template>
      </el-table-column>
      <el-table-column label="操作" width="170" fixed="right">
        <template #default="{ row }">
          <el-button link type="primary" @click="openDetail(row)">详情</el-button>
          <el-button link type="success" :loading="videoLoading" @click="openVideo(row)">播放录像</el-button>
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

    <el-drawer v-model="detailVisible" title="操控会话归档" size="620px">
      <div v-loading="detailLoading">
        <template v-if="detail">
          <el-descriptions :column="1" size="small" border>
            <el-descriptions-item label="会话 ID">{{ detail.session_id }}</el-descriptions-item>
            <el-descriptions-item label="车辆">{{ vehicleStore.resolveName(detail.vehicle_id) }}</el-descriptions-item>
            <el-descriptions-item label="操作员">{{ detail.operator_name || detail.operator_id }}</el-descriptions-item>
            <el-descriptions-item label="时间">
              {{ formatTime(detail.started_at) }} ~ {{ formatTime(detail.ended_at) }}（{{ formatDuration(detail.duration_s) }}）
            </el-descriptions-item>
            <el-descriptions-item label="结束原因">
              <StatusTag kind="session" value="ended" />
              {{ SESSION_END_REASON_LABELS[detail.end_reason] ?? detail.end_reason }}
              （来源 {{ detail.end_reason_source ?? '-' }}）
            </el-descriptions-item>
            <el-descriptions-item label="录像对象">{{ detail.video_object_key }}</el-descriptions-item>
            <el-descriptions-item label="sidecar 对象">{{ detail.sidecar_object_key }}</el-descriptions-item>
            <el-descriptions-item label="录像可下载">
              <el-tag :type="detail.video_available ? 'success' : 'warning'" size="small">
                {{ detail.video_available ? '可播放' : '已过保留期或未封存' }}
              </el-tag>
            </el-descriptions-item>
            <el-descriptions-item label="SHA-256">{{ detail.video_sha256 ?? '-' }}</el-descriptions-item>
          </el-descriptions>

          <el-divider content-position="left">归档 sidecar（原始 JSON）</el-divider>
          <pre class="sidecar">{{ sidecarText }}</pre>
        </template>
        <el-empty v-else description="暂无详情" />
      </div>
    </el-drawer>

    <el-dialog v-model="videoVisible" title="操控录像回放（H.264）" width="820px" @closed="videoUrl = ''">
      <video v-if="videoUrl" :src="videoUrl" class="player" controls preload="metadata" />
      <p class="hint">预签名链接 15 分钟有效且支持 HTTP Range（可拖动 seek）；请勿分享或缓存该链接。</p>
    </el-dialog>
  </div>
</template>

<style scoped>
.alert,
.meta {
  margin-bottom: 12px;
}

.meta {
  display: flex;
  gap: 8px;
}

.pager {
  margin-top: 12px;
  text-align: right;
}

.sidecar {
  max-height: 420px;
  overflow: auto;
  padding: 10px;
  font-size: 12px;
  line-height: 1.5;
  background: #0f1419;
  color: #e6edf3;
  border-radius: 4px;
}

.player {
  width: 100%;
  max-height: 460px;
  background: #0f1419;
}

.hint {
  margin: 8px 0 0;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}
</style>
