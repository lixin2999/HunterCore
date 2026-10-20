<script setup lang="ts">
/**
 * 事件与文件（data-collector 模块）
 *
 * 契约：
 * - GET  /api/v1/data/events（分页、类型/等级/确认状态/时间范围过滤）
 * - POST /api/v1/data/events/{event_id}/acknowledge（事件确认，审计留痕）
 * - GET  /api/v1/data/files（游标分页；download_url 预签名 15 分钟，支持 Range）
 * 说明：事件类型与等级为受控词表（系统约束第 13 条），等级不可由前端放宽。
 */
import { computed, onMounted, reactive, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'

import { acknowledgeEvent, deleteFiles, listEvents, listFiles } from '@/api/data'
import StatusTag from '@/components/common/StatusTag.vue'
import {
  DEFAULT_PAGE_SIZE,
  EVENT_TYPE_LABELS,
  EVENT_TYPE_THRESHOLDS,
  FILE_DATA_TYPE_LABELS,
  MAX_PAGE_SIZE,
  UPLOAD_BUCKET_LABELS,
} from '@/constants'
import { PERMISSIONS } from '@/constants/permissions'
import { useUserStore } from '@/stores/user'
import { useVehicleStore } from '@/stores/vehicle'
import type {
  EventItem,
  EventListQuery,
  FileObjectItem,
  FileListQuery,
  UploadBucket,
} from '@/types/data'
import { HunterApiError } from '@/utils/error-code'
import { downloadByUrl } from '@/utils/hash'
import { formatBytes, formatNumber, formatTime } from '@/utils/format'

const userStore = useUserStore()
const vehicleStore = useVehicleStore()

const activeTab = ref<'events' | 'files'>('events')

/* ------------------------------- 事件 ------------------------------- */
const eventQuery = reactive<EventListQuery>({
  vehicle_id: undefined,
  event_type: undefined,
  event_level: undefined,
  acknowledged: undefined,
  page: 1,
  page_size: DEFAULT_PAGE_SIZE,
})
const eventRange = ref<[number, number] | null>(null)
const events = ref<EventItem[]>([])
const eventTotal = ref(0)
const eventLoading = ref(false)
const acknowledgingId = ref<number | null>(null)

/** 事件类型选项（受控词表 19 种） */
const eventTypeOptions = computed(() =>
  Object.entries(EVENT_TYPE_LABELS).map(([value, label]) => ({
    value,
    label,
    threshold: EVENT_TYPE_THRESHOLDS[value] ?? '',
  })),
)

async function loadEvents(): Promise<void> {
  eventLoading.value = true
  try {
    const [start, end] = eventRange.value ?? []
    const data = await listEvents({
      ...eventQuery,
      start_time: start,
      end_time: end,
    })
    events.value = data.items
    eventTotal.value = data.total
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '事件加载失败')
  } finally {
    eventLoading.value = false
  }
}

function resetEventQuery(): void {
  eventQuery.vehicle_id = undefined
  eventQuery.event_type = undefined
  eventQuery.event_level = undefined
  eventQuery.acknowledged = undefined
  eventQuery.page = 1
  eventRange.value = null
  void loadEvents()
}

/** 确认事件（需 data:read 之上的写权限由服务端校验；此处按权限显示入口） */
async function handleAcknowledge(row: EventItem): Promise<void> {
  acknowledgingId.value = row.event_id
  try {
    await acknowledgeEvent(row.event_id)
    ElMessage.success('事件已确认')
    await loadEvents()
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '确认失败')
  } finally {
    acknowledgingId.value = null
  }
}

/* ------------------------------- 文件 ------------------------------- */
const fileQuery = reactive<FileListQuery>({
  bucket: 'hunter-raw-data',
  vehicle_id: undefined,
  data_type: undefined,
  date: undefined,
})
const files = ref<FileObjectItem[]>([])
const fileLoading = ref(false)
const fileLimit = ref(50)
const selectedKeys = ref<string[]>([])
const deleting = ref(false)

/**
 * 游标 marker 栈分页（契约 GET /files 仅返回 next_marker，无 total）：
 * `markers[i]` = 请求第 i+1 页所用的 marker（首页为 undefined）；
 * 加载后据 truncated/next_marker 写回 `markers[i+1]`，实现上一页 / 下一页双向导航。
 */
const markers = ref<(string | undefined)[]>([undefined])
const filePageIndex = ref(0)
const hasNext = ref(false)

const canDelete = computed(() => userStore.hasPermission(PERMISSIONS.dataExecute))

/** hunter-video 桶：启用 sidecar↔视频配对标注与有效录像护栏（V1.21.22） */
const VIDEO_BUCKET: UploadBucket = 'hunter-video'
const isVideoBucket = computed(() => fileQuery.bucket === VIDEO_BUCKET)

/** 仅孤儿行可勾选（video 桶）；非 video 桶全部可勾（无有效性概念） */
function isRowSelectable(row: FileObjectItem): boolean {
  return !isVideoBucket.value || row.is_orphan === true
}

const bucketOptions = computed(() =>
  Object.entries(UPLOAD_BUCKET_LABELS).map(([value, label]) => ({ value, label })),
)

function resetFilePagination(): void {
  markers.value = [undefined]
  filePageIndex.value = 0
  hasNext.value = false
  files.value = []
  selectedKeys.value = []
}

/** 加载指定页（index 对应 markers[index]；过滤条件变更时从第 0 页重来） */
async function loadFilePage(index: number): Promise<void> {
  fileLoading.value = true
  try {
    const data = await listFiles({
      ...fileQuery,
      limit: fileLimit.value,
      marker: markers.value[index],
    })
    files.value = data.items
    filePageIndex.value = index
    selectedKeys.value = []
    // 丢弃比当前页更深的已知游标，再登记下一页游标
    markers.value = markers.value.slice(0, index + 1)
    if (data.truncated && data.next_marker) {
      markers.value[index + 1] = data.next_marker
      hasNext.value = true
    } else {
      hasNext.value = false
    }
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '文件列表加载失败')
  } finally {
    fileLoading.value = false
  }
}

/** 条件/每页条数变更：回到首页重新列举 */
function handleFileSearch(): void {
  resetFilePagination()
  void loadFilePage(0)
}

function goFilePrev(): void {
  if (filePageIndex.value > 0) void loadFilePage(filePageIndex.value - 1)
}

function goFileNext(): void {
  if (hasNext.value) void loadFilePage(filePageIndex.value + 1)
}

function handleSelectionChange(rows: FileObjectItem[]): void {
  selectedKeys.value = rows.map((row) => row.object_key)
}

/** 批量删除选中对象（权限 data:execute；video 桶按会话配对删且护栏保护有效录像） */
async function handleDelete(): Promise<void> {
  if (!selectedKeys.value.length) return
  const count = selectedKeys.value.length
  const hint = isVideoBucket.value
    ? '将按会话配对删除（.json 记录 + .mp4 视频一并移除）；有效录像（视频 >0B）受护栏保护不会删除。'
    : '删除后不可恢复。'
  try {
    await ElMessageBox.confirm(
      `确认删除选中的 ${count} 个对象？${hint}`,
      '批量删除确认',
      { type: 'warning', confirmButtonText: '删除', cancelButtonText: '取消' },
    )
  } catch {
    return // 用户取消
  }
  deleting.value = true
  try {
    const data = await deleteFiles({ bucket: fileQuery.bucket, object_keys: selectedKeys.value })
    const skipped = data.skipped_count ?? 0
    const parts = [`已删除 ${data.deleted_count} 个`]
    if (skipped) parts.push(`${skipped} 个有效录像被护栏跳过未删`)
    if (data.failed.length) parts.push(`${data.failed.length} 个失败`)
    const text = parts.join('，')
    if (data.failed.length || skipped) {
      ElMessage.warning(text)
    } else {
      ElMessage.success(text)
    }
    await loadFilePage(filePageIndex.value)
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '删除失败')
  } finally {
    deleting.value = false
  }
}

/** 下载（预签名 URL 15 分钟有效，禁止落日志/缓存） */
function handleDownload(row: FileObjectItem): void {
  downloadByUrl(row.download_url, row.object_key.split('/').pop())
}

onMounted(async () => {
  await vehicleStore.fetchVehicles()
  await loadEvents()
})
</script>

<template>
  <div class="data-view">
    <el-tabs v-model="activeTab" type="border-card">
      <el-tab-pane label="事件列表" name="events">
        <el-form :inline="true" class="filter">
          <el-form-item label="车辆">
            <el-select v-model="eventQuery.vehicle_id" clearable filterable placeholder="全部车辆" style="width: 170px">
              <el-option
                v-for="vehicle in vehicleStore.vehicles"
                :key="vehicle.vehicle_id"
                :label="vehicle.vehicle_name || vehicle.vehicle_id"
                :value="vehicle.vehicle_id"
              />
            </el-select>
          </el-form-item>
          <el-form-item label="事件类型">
            <el-select v-model="eventQuery.event_type" clearable placeholder="全部类型" style="width: 180px">
              <el-option
                v-for="option in eventTypeOptions"
                :key="option.value"
                :label="option.label"
                :value="option.value"
              />
            </el-select>
          </el-form-item>
          <el-form-item label="等级">
            <el-select v-model="eventQuery.event_level" clearable placeholder="全部等级" style="width: 130px">
              <el-option label="提示" value="info" />
              <el-option label="警告" value="warning" />
              <el-option label="严重" value="critical" />
            </el-select>
          </el-form-item>
          <el-form-item label="确认状态">
            <el-select v-model="eventQuery.acknowledged" clearable placeholder="全部" style="width: 120px">
              <el-option label="已确认" :value="true" />
              <el-option label="未确认" :value="false" />
            </el-select>
          </el-form-item>
          <el-form-item label="时间范围">
            <el-date-picker
              v-model="eventRange"
              type="datetimerange"
              value-format="x"
              start-placeholder="开始时间"
              end-placeholder="结束时间"
              unlink-panels
            />
          </el-form-item>
          <el-form-item>
            <el-button type="primary" :loading="eventLoading" @click="loadEvents">查询</el-button>
            <el-button @click="resetEventQuery">重置</el-button>
          </el-form-item>
        </el-form>

        <el-table v-loading="eventLoading" :data="events" size="small" empty-text="暂无事件">
          <el-table-column label="时间" width="170">
            <template #default="{ row }">{{ formatTime(row.event_time) }}</template>
          </el-table-column>
          <el-table-column label="车辆" width="150">
            <template #default="{ row }">{{ vehicleStore.resolveName(row.vehicle_id) }}</template>
          </el-table-column>
          <el-table-column label="类型" width="140">
            <template #default="{ row }">
              <el-tooltip :content="EVENT_TYPE_THRESHOLDS[row.event_type] || '阈值见系统约束第 13 条'" placement="top">
                <span>{{ EVENT_TYPE_LABELS[row.event_type] ?? row.event_type }}</span>
              </el-tooltip>
            </template>
          </el-table-column>
          <el-table-column label="等级" width="100">
            <template #default="{ row }">
              <StatusTag kind="event-level" :value="row.event_level" />
            </template>
          </el-table-column>
          <el-table-column prop="description" label="描述" min-width="200" show-overflow-tooltip />
          <el-table-column label="数据文件" width="100">
            <template #default="{ row }">
              <el-link v-if="row.data_file_download_url" type="primary" @click="downloadByUrl(row.data_file_download_url)">
                下载
              </el-link>
              <span v-else>-</span>
            </template>
          </el-table-column>
          <el-table-column label="确认" width="90">
            <template #default="{ row }">
              <el-tag :type="row.acknowledged ? 'success' : 'warning'" size="small">
                {{ row.acknowledged ? '已确认' : '未确认' }}
              </el-tag>
            </template>
          </el-table-column>
          <el-table-column label="操作" width="110" fixed="right">
            <template #default="{ row }">
              <el-button
                v-if="!row.acknowledged && userStore.hasPermission(PERMISSIONS.dataRead)"
                link
                type="primary"
                :loading="acknowledgingId === row.event_id"
                @click="handleAcknowledge(row)"
              >
                确认
              </el-button>
            </template>
          </el-table-column>
        </el-table>

        <el-pagination
          class="pager"
          layout="total, sizes, prev, pager, next"
          :total="eventTotal"
          :page-size="eventQuery.page_size"
          :page-sizes="[DEFAULT_PAGE_SIZE, 50, 100, MAX_PAGE_SIZE]"
          @current-change="(page: number) => { eventQuery.page = page; loadEvents() }"
          @size-change="(size: number) => { eventQuery.page_size = size; eventQuery.page = 1; loadEvents() }"
        />
      </el-tab-pane>

      <el-tab-pane label="文件浏览（MinIO）" name="files">
        <el-form :inline="true" class="filter">
          <el-form-item label="Bucket">
            <el-select v-model="fileQuery.bucket" placeholder="选择 Bucket" style="width: 220px" @change="handleFileSearch">
              <el-option
                v-for="option in bucketOptions"
                :key="option.value"
                :label="option.label"
                :value="option.value as UploadBucket"
              />
            </el-select>
          </el-form-item>
          <el-form-item label="车辆">
            <el-select v-model="fileQuery.vehicle_id" clearable filterable placeholder="全部车辆" style="width: 170px">
              <el-option
                v-for="vehicle in vehicleStore.vehicles"
                :key="vehicle.vehicle_id"
                :label="vehicle.vehicle_id"
                :value="vehicle.vehicle_id"
              />
            </el-select>
          </el-form-item>
          <el-form-item label="数据类型">
            <el-select v-model="fileQuery.data_type" clearable placeholder="全部" style="width: 140px">
              <el-option v-for="(label, value) in FILE_DATA_TYPE_LABELS" :key="value" :label="label" :value="value" />
            </el-select>
          </el-form-item>
          <el-form-item label="日期">
            <el-date-picker v-model="fileQuery.date" type="date" value-format="YYYY-MM-DD" placeholder="全部日期" />
          </el-form-item>
          <el-form-item label="每页">
            <el-select v-model="fileLimit" style="width: 110px" @change="handleFileSearch">
              <el-option v-for="size in [20, 50, 100, MAX_PAGE_SIZE]" :key="size" :label="`${size} 条`" :value="size" />
            </el-select>
          </el-form-item>
          <el-form-item>
            <el-button type="primary" :loading="fileLoading" @click="handleFileSearch">查询</el-button>
            <el-button
              v-if="canDelete"
              type="danger"
              :disabled="!selectedKeys.length"
              :loading="deleting"
              @click="handleDelete"
            >
              删除选中{{ selectedKeys.length ? `（${selectedKeys.length}）` : '' }}
            </el-button>
          </el-form-item>
        </el-form>

        <el-table
          v-loading="fileLoading"
          :data="files"
          size="small"
          empty-text="暂无文件"
          @selection-change="handleSelectionChange"
        >
          <el-table-column v-if="canDelete" type="selection" width="48" :selectable="isRowSelectable" />
          <el-table-column prop="object_key" label="对象键" min-width="320" show-overflow-tooltip />
          <el-table-column label="大小" width="120">
            <template #default="{ row }">{{ formatBytes(row.size_bytes) }}</template>
          </el-table-column>
          <el-table-column v-if="isVideoBucket" label="录像有效性" width="150">
            <template #default="{ row }">
              <el-tag v-if="row.is_orphan" type="info" size="small">无效（视频缺失/0B）</el-tag>
              <el-tag v-else type="success" size="small">有效（{{ formatBytes(row.video_size_bytes ?? 0) }}）</el-tag>
            </template>
          </el-table-column>
          <el-table-column label="修改时间" width="170">
            <template #default="{ row }">{{ formatTime(row.last_modified) }}</template>
          </el-table-column>
          <el-table-column label="下载链接有效期" width="150">
            <template #default="{ row }">{{ formatNumber(row.expires_in / 60, 0) }} 分钟</template>
          </el-table-column>
          <el-table-column label="操作" width="90" fixed="right">
            <template #default="{ row }">
              <el-button link type="primary" @click="handleDownload(row)">下载</el-button>
            </template>
          </el-table-column>
        </el-table>

        <div class="pager">
          <span class="pager-info">第 {{ filePageIndex + 1 }} 页</span>
          <el-button :disabled="filePageIndex === 0 || fileLoading" @click="goFilePrev">上一页</el-button>
          <el-button :disabled="!hasNext || fileLoading" @click="goFileNext">下一页</el-button>
        </div>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<style scoped>
.filter {
  margin-bottom: 8px;
}

.pager {
  margin-top: 12px;
  text-align: right;
}

.pager-info {
  margin-right: 12px;
  color: var(--el-text-color-secondary);
  font-size: 13px;
}
</style>
