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
import { ElMessage } from 'element-plus'

import { acknowledgeEvent, listEvents, listFiles } from '@/api/data'
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
import type { EventItem, EventListQuery, FileObjectItem, FileListQuery, UploadBucket } from '@/types/data'
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

/** 事件类型选项（受控词表 18 种） */
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
  limit: 50,
})
const files = ref<FileObjectItem[]>([])
const nextMarker = ref<string | null>(null)
const fileLoading = ref(false)

const bucketOptions = computed(() =>
  Object.entries(UPLOAD_BUCKET_LABELS).map(([value, label]) => ({ value, label })),
)

async function loadFiles(reset = true): Promise<void> {
  fileLoading.value = true
  try {
    const data = await listFiles({
      ...fileQuery,
      marker: reset ? undefined : (nextMarker.value ?? undefined),
    })
    files.value = reset ? data.items : [...files.value, ...data.items]
    nextMarker.value = data.next_marker
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '文件列表加载失败')
  } finally {
    fileLoading.value = false
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
            <el-select v-model="fileQuery.bucket" placeholder="选择 Bucket" style="width: 220px" @change="loadFiles(true)">
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
          <el-form-item>
            <el-button type="primary" :loading="fileLoading" @click="loadFiles(true)">查询</el-button>
          </el-form-item>
        </el-form>

        <el-table v-loading="fileLoading" :data="files" size="small" empty-text="暂无文件">
          <el-table-column prop="object_key" label="对象键" min-width="320" show-overflow-tooltip />
          <el-table-column label="大小" width="120">
            <template #default="{ row }">{{ formatBytes(row.size_bytes) }}</template>
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
          <el-button :disabled="!nextMarker" :loading="fileLoading" @click="loadFiles(false)">
            加载更多（游标分页）
          </el-button>
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
</style>
