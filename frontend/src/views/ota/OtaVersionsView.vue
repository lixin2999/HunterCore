<script setup lang="ts">
/**
 * OTA 版本仓库（ota-service.yaml）
 *
 * 契约流程：
 * 1. POST /api/v1/ota/versions（附录 D 限流 5 QPS/用户）→ 返回 version + upload（预签名，1 小时）
 * 2. 前端本地计算 package_md5 + package_sha256（utils/hash.ts）后按分包 PUT 上传
 * 3. POST /api/v1/ota/versions/{version_id}/publish → 服务端校验（失败 6001/6002）
 * 4. POST /api/v1/ota/versions/{version_id}/deprecate → 弃用/下线（不可删除，保证审计）
 * 注意：release_type 契约未定义 enum（pending #2），前端仅原样展示，不做分支判断。
 */
import { computed, onMounted, reactive, ref } from 'vue'
import { ElMessage, ElMessageBox, type FormInstance, type FormRules } from 'element-plus'

import {
  createOtaVersion,
  deprecateOtaVersion,
  listOtaVersions,
  publishOtaVersion,
} from '@/api/ota'
import { putToPresignedUrl } from '@/api/request'
import StatusTag from '@/components/common/StatusTag.vue'
import {
  DEFAULT_PAGE_SIZE,
  OTA_PUBLISH_CHECK_LABELS,
  OTA_UPLOAD_PART_SIZE,
  OTA_VERSION_STATUS_LABELS,
} from '@/constants'
import { PERMISSIONS } from '@/constants/permissions'
import type {
  OtaVersionCreateRequest,
  OtaVersionItem,
  OtaVersionListQuery,
} from '@/types/ota'
import { HunterApiError } from '@/utils/error-code'
import { computeFileDigests } from '@/utils/hash'
import { formatBytes, formatTime } from '@/utils/format'

const loading = ref(false)
const versions = ref<OtaVersionItem[]>([])
const total = ref(0)
const query = reactive<OtaVersionListQuery>({
  status: undefined,
  applicable_model: undefined,
  page: 1,
  page_size: DEFAULT_PAGE_SIZE,
})

const statusOptions = Object.entries(OTA_VERSION_STATUS_LABELS).map(([value, label]) => ({ value, label }))

async function load(): Promise<void> {
  loading.value = true
  try {
    const data = await listOtaVersions({ ...query })
    versions.value = data.items
    total.value = data.total
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '版本列表加载失败')
  } finally {
    loading.value = false
  }
}

/* ------------------------------ 新建版本 ------------------------------ */
const createVisible = ref(false)
const createFormRef = ref<FormInstance>()
const submitting = ref(false)
const uploadPercent = ref(0)
const selectedFile = ref<File | null>(null)
const createForm = reactive<OtaVersionCreateRequest>({
  version_name: '',
  version_code: 1,
  release_type: 'formal',
  package_size: 0,
  package_md5: '',
  package_sha256: '',
  signature: '',
  changelog: {},
  applicable_models: ['HUNTER_SE'],
})

const createRules: FormRules<OtaVersionCreateRequest> = {
  version_name: [{ required: true, message: '请输入版本名称（如 v1.2.0）', trigger: 'blur' }],
  version_code: [{ required: true, message: '请输入版本号（单调递增）', trigger: 'blur' }],
  signature: [{ required: true, message: '请粘贴 RSA-2048 签名（Base64）', trigger: 'blur' }],
}

function handleFileChange(file: { raw?: File }): void {
  selectedFile.value = file.raw ?? null
  if (selectedFile.value) {
    createForm.package_size = selectedFile.value.size
  }
}

/** 计算摘要（本地校验前置，服务端 publish 时复核 → 失败返回 6001） */
async function fillDigests(): Promise<void> {
  if (!selectedFile.value) {
    ElMessage.warning('请先选择升级包文件')
    return
  }
  const digests = await computeFileDigests(selectedFile.value)
  createForm.package_md5 = digests.md5
  createForm.package_sha256 = digests.sha256
  createForm.package_size = digests.sizeBytes
  ElMessage.success('已计算 MD5 与 SHA-256')
}

/**
 * 分片上传升级包
 * 说明：契约未暴露多段上传 complete 端点，分片 ETag 由服务端在 publish 阶段汇总校验；
 * 若响应未返回 parts（method=put），则一次性 PUT 整个文件。
 */
async function uploadPackage(payload: {
  upload_url: string
  parts?: Array<{ part_number: number; upload_url: string }>
}): Promise<void> {
  const file = selectedFile.value
  if (!file) {
    return
  }
  if (!payload.parts || payload.parts.length === 0) {
    await putToPresignedUrl(payload.upload_url, file, (percent) => {
      uploadPercent.value = percent
    })
    return
  }
  const orderedParts = [...payload.parts].sort((left, right) => left.part_number - right.part_number)
  for (const [index, part] of orderedParts.entries()) {
    const start = (part.part_number - 1) * OTA_UPLOAD_PART_SIZE
    const blob = file.slice(start, start + OTA_UPLOAD_PART_SIZE)
    await putToPresignedUrl(part.upload_url, blob)
    uploadPercent.value = Math.round(((index + 1) / orderedParts.length) * 100)
  }
}

async function submitCreate(): Promise<void> {
  if (!createFormRef.value) {
    return
  }
  const valid = await createFormRef.value.validate().catch(() => false)
  if (!valid) {
    return
  }
  if (!selectedFile.value) {
    ElMessage.warning('请选择升级包文件')
    return
  }
  submitting.value = true
  uploadPercent.value = 0
  try {
    const partCount =
      selectedFile.value.size > OTA_UPLOAD_PART_SIZE
        ? Math.ceil(selectedFile.value.size / OTA_UPLOAD_PART_SIZE)
        : 1
    const created = await createOtaVersion({ ...createForm, part_count: partCount })
    await uploadPackage(created.upload)
    ElMessage.success(`版本已创建并上传完成（${created.version.version_name}），请执行「发布」以完成完整性校验`)
    createVisible.value = false
    await load()
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '版本创建失败')
  } finally {
    submitting.value = false
  }
}

/* ------------------------------ 发布 / 弃用 ------------------------------ */
const publishResult = ref<Record<string, boolean> | null>(null)

const publishChecks = computed(() => {
  const checks = publishResult.value
  if (!checks) {
    return []
  }
  return Object.entries(OTA_PUBLISH_CHECK_LABELS).map(([key, label]) => ({
    label,
    passed: Boolean(checks[key]),
  }))
})

async function handlePublish(row: OtaVersionItem): Promise<void> {
  try {
    const data = await publishOtaVersion(row.version_id, { note: '控制台发布' })
    publishResult.value = data.checks as unknown as Record<string, boolean>
    ElMessage.success(`版本 ${data.version_code} 已发布（${formatTime(data.release_time)}）`)
    await load()
  } catch (error) {
    // 6001 完整性校验失败 / 6002 签名验证失败
    ElMessage.error(error instanceof HunterApiError ? error.message : '发布失败')
  }
}

async function handleDeprecate(row: OtaVersionItem): Promise<void> {
  let reason = ''
  try {
    const result = await ElMessageBox.prompt(
      '弃用后该版本不可再用于新建升级任务（历史任务与记录保留）。请输入弃用原因：',
      '弃用版本',
      { inputPattern: /.+/, inputErrorMessage: '请填写弃用原因' },
    )
    reason = result.value
  } catch {
    return
  }
  try {
    await deprecateOtaVersion(row.version_id, { status: 'deprecated', reason })
    ElMessage.success('版本已弃用')
    await load()
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '弃用失败')
  }
}

/** 下载升级包（预签名 URL，按需下发） */
function handleDownloadPackage(row: OtaVersionItem): void {
  if (!row.package_download_url) {
    ElMessage.warning('当前未返回下载链接，请刷新后重试')
    return
  }
  window.open(row.package_download_url, '_blank', 'noopener')
}

onMounted(load)
</script>

<template>
  <div v-loading="loading">
    <el-form :inline="true">
      <el-form-item label="状态">
        <el-select v-model="query.status" clearable placeholder="全部状态" style="width: 140px">
          <el-option v-for="option in statusOptions" :key="option.value" :label="option.label" :value="option.value" />
        </el-select>
      </el-form-item>
      <el-form-item label="适用车型">
        <el-input v-model="query.applicable_model" clearable placeholder="如 HUNTER_SE" style="width: 160px" />
      </el-form-item>
      <el-form-item>
        <el-button type="primary" :loading="loading" @click="load">查询</el-button>
        <el-button v-permission="PERMISSIONS.otaCreate" type="success" @click="createVisible = true">新建版本</el-button>
      </el-form-item>
    </el-form>

    <el-alert
      v-if="publishChecks.length"
      class="notice"
      type="success"
      :closable="true"
      show-icon
      title="最近一次发布校验结果"
    >
      <div class="checks">
        <el-tag v-for="check in publishChecks" :key="check.label" :type="check.passed ? 'success' : 'danger'" size="small">
          {{ check.label }}：{{ check.passed ? '通过' : '未通过' }}
        </el-tag>
      </div>
    </el-alert>

    <el-table :data="versions" size="small" empty-text="暂无版本">
      <el-table-column prop="version_name" label="版本名称" width="130" />
      <el-table-column prop="version_code" label="版本号" width="90" />
      <el-table-column label="发布类型" width="110">
        <template #default="{ row }">
          <!-- release_type 契约未定义 enum（pending #2），原样展示 -->
          {{ row.release_type }}
        </template>
      </el-table-column>
      <el-table-column label="状态" width="100">
        <template #default="{ row }">
          <StatusTag kind="ota-version" :value="row.status" />
        </template>
      </el-table-column>
      <el-table-column label="包大小" width="110">
        <template #default="{ row }">{{ formatBytes(row.package_size) }}</template>
      </el-table-column>
      <el-table-column label="适用车型" min-width="140">
        <template #default="{ row }">
          <el-tag v-for="model in row.applicable_models" :key="model" size="small" effect="plain" class="tag">{{ model }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="发布时间" width="170">
        <template #default="{ row }">{{ formatTime(row.release_time) }}</template>
      </el-table-column>
      <el-table-column label="SHA-256" min-width="180" show-overflow-tooltip>
        <template #default="{ row }">{{ row.package_sha256 }}</template>
      </el-table-column>
      <el-table-column label="操作" width="230" fixed="right">
        <template #default="{ row }">
          <el-button
            v-if="row.status === 'draft'"
            v-permission="PERMISSIONS.otaExecute"
            link
            type="success"
            @click="handlePublish(row)"
          >
            发布
          </el-button>
          <el-button v-permission="PERMISSIONS.otaRead" link type="primary" @click="handleDownloadPackage(row)">
            下载包
          </el-button>
          <el-button
            v-if="row.status === 'published'"
            v-permission="PERMISSIONS.otaExecute"
            link
            type="warning"
            @click="handleDeprecate(row)"
          >
            弃用
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

    <el-dialog v-model="createVisible" title="新建 OTA 版本" width="640px">
      <el-form ref="createFormRef" :model="createForm" :rules="createRules" label-width="120px">
        <el-form-item label="版本名称" prop="version_name">
          <el-input v-model="createForm.version_name" placeholder="如 v1.2.0" />
        </el-form-item>
        <el-form-item label="版本号" prop="version_code">
          <el-input-number v-model="createForm.version_code" :min="1" :step="1" />
          <span class="hint">必须单调递增（防回滚，服务端校验 version_code_monotonic）</span>
        </el-form-item>
        <el-form-item label="发布类型">
          <el-input v-model="createForm.release_type" placeholder="formal / gray / patch（契约未定枚举，pending #2）" />
        </el-form-item>
        <el-form-item label="适用车型">
          <el-select v-model="createForm.applicable_models" multiple filterable allow-create default-first-option style="width: 100%" />
        </el-form-item>
        <el-form-item label="升级包">
          <el-upload :auto-upload="false" :limit="1" :on-change="handleFileChange" :show-file-list="true">
            <el-button>选择文件</el-button>
          </el-upload>
        </el-form-item>
        <el-form-item label="摘要信息">
          <el-button size="small" :disabled="!selectedFile" @click="fillDigests">计算 MD5 / SHA-256</el-button>
          <div class="digests">
            <div>大小：{{ formatBytes(createForm.package_size) }}</div>
            <div>MD5：{{ createForm.package_md5 || '-' }}</div>
            <div>SHA-256：{{ createForm.package_sha256 || '-' }}</div>
          </div>
        </el-form-item>
        <el-form-item label="数字签名" prop="signature">
          <el-input
            v-model="createForm.signature"
            type="textarea"
            :rows="3"
            placeholder="RSA-2048 签名（Base64，由离线发布工具产出于安全环境）"
          />
        </el-form-item>
      </el-form>
      <el-progress v-if="submitting" :percentage="uploadPercent" />
      <template #footer>
        <el-button @click="createVisible = false">取消</el-button>
        <el-button type="primary" :loading="submitting" @click="submitCreate">创建并上传</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<style scoped>
.notice {
  margin-bottom: 12px;
}

.checks {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  margin-top: 6px;
}

.tag {
  margin-right: 4px;
}

.pager {
  margin-top: 12px;
  text-align: right;
}

.hint {
  margin-left: 8px;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}

.digests {
  margin-top: 6px;
  font-size: 12px;
  color: var(--el-text-color-secondary);
  word-break: break-all;
}
</style>
