<script setup lang="ts">
/**
 * 场景用例与测试集（scene-service 场景引擎）
 *
 * 契约（设计文档 §4.1）：
 * - GET/POST /api/v1/scene/testcases（ISO 34505：目标/前置/步骤/平台/判据/超时）
 * - GET/POST /api/v1/scene/suites、/{id}/items、/{id}/lock、/{id}/diff、/{id}/baselines
 * - GET  /api/v1/scene/oracles（判据目录，版本化 ref）
 *
 * 语义：测试集锁定后条目冻结（lock_hash 写入，操作返回 3003）；
 *       基线 = lock_hash + 指标快照，供回归差异比对。
 */
import { computed, onMounted, reactive, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'

import {
  addSuiteItems,
  createSuite,
  createTestCase,
  diffSuite,
  getSuite,
  listOracles,
  listSuiteBaselines,
  listSuites,
  listTestCases,
  lockSuite,
} from '@/api/engine'
import {
  DEFAULT_PAGE_SIZE,
  SUITE_ITEM_TYPE_LABELS,
  SUITE_STATUS_LABELS,
  SUITE_STATUS_TAG_TYPES,
  TEST_CASE_STATUS_LABELS,
  TEST_CASE_STATUS_TAG_TYPES,
} from '@/constants'
import { PERMISSIONS } from '@/constants/permissions'
import type {
  Baseline,
  OracleData,
  SuiteDetail,
  SuiteDiffData,
  SuiteItem,
  SuiteItemType,
  SuiteSummary,
  TestCase,
  TestCaseCreateRequest,
  TestCaseListQuery,
  TestCaseStatus,
} from '@/types/engine'
import { HunterApiError } from '@/utils/error-code'
import { formatDateTimeString } from '@/utils/format'

function errorMessage(error: unknown, fallback: string): string {
  return error instanceof HunterApiError ? error.message : fallback
}

function parseJsonObject(text: string, label: string): Record<string, unknown> | null {
  const trimmed = text.trim()
  if (!trimmed) {
    return {}
  }
  try {
    const parsed: unknown = JSON.parse(trimmed)
    if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
      ElMessage.error(`${label}必须是 JSON 对象`)
      return null
    }
    return parsed as Record<string, unknown>
  } catch {
    ElMessage.error(`${label}不是合法 JSON`)
    return null
  }
}

/* =====================================================================
 * 判据目录（用例下拉 + 展示）
 * ===================================================================== */
const oracles = ref<OracleData[]>([])

async function loadOracles(): Promise<void> {
  try {
    const data = await listOracles()
    oracles.value = data.items
  } catch (error) {
    ElMessage.error(errorMessage(error, '判据目录加载失败'))
  }
}

const testCaseStatusOptions = Object.entries(TEST_CASE_STATUS_LABELS).map(([value, label]) => ({
  value: value as TestCaseStatus,
  label,
}))
const itemTypeOptions = Object.entries(SUITE_ITEM_TYPE_LABELS).map(([value, label]) => ({
  value: value as SuiteItemType,
  label,
}))

/* =====================================================================
 * 一、测试用例
 * ===================================================================== */
const testCaseLoading = ref(false)
const testCases = ref<TestCase[]>([])
const testCaseTotal = ref(0)
const testCaseQuery = reactive<TestCaseListQuery>({
  page: 1,
  page_size: DEFAULT_PAGE_SIZE,
  logical_id: '',
  status: undefined,
})

async function loadTestCases(): Promise<void> {
  testCaseLoading.value = true
  try {
    const params: Record<string, unknown> = {
      page: testCaseQuery.page,
      page_size: testCaseQuery.page_size,
    }
    if (testCaseQuery.logical_id) {
      params.logical_id = testCaseQuery.logical_id
    }
    if (testCaseQuery.status) {
      params.status = testCaseQuery.status
    }
    const data = await listTestCases(params as TestCaseListQuery)
    testCases.value = data.items
    testCaseTotal.value = data.total
  } catch (error) {
    ElMessage.error(errorMessage(error, '测试用例加载失败'))
  } finally {
    testCaseLoading.value = false
  }
}

const caseVisible = ref(false)
const caseSubmitting = ref(false)
const caseForm = reactive({
  case_key: '',
  name: '',
  logical_id: '',
  objective: '',
  oracle_ref: '',
  timeout_seconds: 120,
  platforms: ['simulation'] as string[],
  tags: [] as string[],
  preconditionsText: '{}',
  stepsText: '[{"index": 1, "action": "spawn"}]',
})

function openCaseDialog(): void {
  caseForm.case_key = ''
  caseForm.name = ''
  caseForm.logical_id = ''
  caseForm.objective = ''
  caseForm.oracle_ref = oracles.value[0]?.oracle_ref ?? ''
  caseForm.timeout_seconds = 120
  caseForm.platforms = ['simulation']
  caseForm.tags = []
  caseForm.preconditionsText = '{}'
  caseForm.stepsText = '[{"index": 1, "action": "spawn"}]'
  caseVisible.value = true
}

async function submitCase(): Promise<void> {
  if (!caseForm.case_key || !caseForm.name || !caseForm.logical_id || !caseForm.objective) {
    ElMessage.warning('请填写用例编号、名称、逻辑场景与目标')
    return
  }
  const preconditions = parseJsonObject(caseForm.preconditionsText, '前置条件')
  if (preconditions === null) {
    return
  }
  let steps: Array<Record<string, unknown>>
  try {
    const parsed: unknown = JSON.parse(caseForm.stepsText)
    if (!Array.isArray(parsed)) {
      ElMessage.error('测试步骤必须是 JSON 数组')
      return
    }
    steps = parsed as Array<Record<string, unknown>>
  } catch {
    ElMessage.error('测试步骤不是合法 JSON')
    return
  }
  const payload: TestCaseCreateRequest = {
    case_key: caseForm.case_key,
    name: caseForm.name,
    logical_id: caseForm.logical_id,
    objective: caseForm.objective,
    preconditions,
    steps,
    platforms: caseForm.platforms,
    oracle_ref: caseForm.oracle_ref,
    timeout_seconds: caseForm.timeout_seconds,
    tags: caseForm.tags,
  }
  caseSubmitting.value = true
  try {
    await createTestCase(payload)
    ElMessage.success('测试用例已创建')
    caseVisible.value = false
    await loadTestCases()
  } catch (error) {
    ElMessage.error(errorMessage(error, '测试用例创建失败'))
  } finally {
    caseSubmitting.value = false
  }
}

const caseDetailVisible = ref(false)
const caseDetail = ref<TestCase | null>(null)
const caseStepsText = computed(() =>
  caseDetail.value ? JSON.stringify(caseDetail.value.steps, null, 2) : '',
)

function openCaseDetail(row: TestCase): void {
  caseDetail.value = row
  caseDetailVisible.value = true
}

/* =====================================================================
 * 二、测试集
 * ===================================================================== */
const suiteLoading = ref(false)
const suites = ref<SuiteSummary[]>([])
const suiteTotal = ref(0)
const suitePage = reactive({ page: 1, page_size: DEFAULT_PAGE_SIZE })

async function loadSuites(): Promise<void> {
  suiteLoading.value = true
  try {
    const data = await listSuites({ page: suitePage.page, page_size: suitePage.page_size })
    suites.value = data.items
    suiteTotal.value = data.total
  } catch (error) {
    ElMessage.error(errorMessage(error, '测试集加载失败'))
  } finally {
    suiteLoading.value = false
  }
}

const suiteCreateVisible = ref(false)
const suiteCreateForm = reactive({ name: '', description: '' })

async function submitSuiteCreate(): Promise<void> {
  if (!suiteCreateForm.name) {
    ElMessage.warning('请输入测试集名称')
    return
  }
  try {
    await createSuite({ name: suiteCreateForm.name, description: suiteCreateForm.description || null })
    ElMessage.success('测试集已创建')
    suiteCreateVisible.value = false
    suiteCreateForm.name = ''
    suiteCreateForm.description = ''
    await loadSuites()
  } catch (error) {
    ElMessage.error(errorMessage(error, '测试集创建失败'))
  }
}

/** 测试集详情抽屉（含条目） */
const suiteDrawerVisible = ref(false)
const suiteLoadingDetail = ref(false)
const suiteDetail = ref<SuiteDetail | null>(null)

async function openSuiteDetail(suite: SuiteSummary): Promise<void> {
  suiteDrawerVisible.value = true
  suiteLoadingDetail.value = true
  suiteDetail.value = null
  try {
    suiteDetail.value = await getSuite(suite.suite_id)
  } catch (error) {
    ElMessage.error(errorMessage(error, '测试集详情加载失败'))
  } finally {
    suiteLoadingDetail.value = false
  }
}

/** 加条目 */
const itemVisible = ref(false)
const itemSubmitting = ref(false)
const itemForm = reactive({
  item_id: '',
  item_type: 'test_case' as SuiteItemType,
  ref_id: '',
  weight: 1.0,
})

function openItemDialog(): void {
  itemForm.item_id = ''
  itemForm.item_type = 'test_case'
  itemForm.ref_id = ''
  itemForm.weight = 1.0
  itemVisible.value = true
}

async function submitItem(): Promise<void> {
  const suite = suiteDetail.value
  if (!suite) {
    return
  }
  if (!itemForm.item_id || !itemForm.ref_id) {
    ElMessage.warning('请填写条目 ID 与引用对象')
    return
  }
  itemSubmitting.value = true
  try {
    suiteDetail.value = await addSuiteItems(suite.suite_id, [
      {
        item_id: itemForm.item_id,
        item_type: itemForm.item_type,
        ref_id: itemForm.ref_id,
        weight: itemForm.weight,
      },
    ])
    ElMessage.success('条目已写入')
    itemVisible.value = false
    await loadSuites()
  } catch (error) {
    ElMessage.error(errorMessage(error, '条目写入失败'))
  } finally {
    itemSubmitting.value = false
  }
}

/** 锁定（写 lock_hash，可建基线） */
const lockVisible = ref(false)
const lockSubmitting = ref(false)
const lockForm = reactive({ baseline_name: '', metricsText: '{}' })

function openLockDialog(): void {
  lockForm.baseline_name = ''
  lockForm.metricsText = '{}'
  lockVisible.value = true
}

async function submitLock(): Promise<void> {
  const suite = suiteDetail.value
  if (!suite) {
    return
  }
  const metrics = parseJsonObject(lockForm.metricsText, '指标快照')
  if (metrics === null) {
    return
  }
  try {
    await ElMessageBox.confirm(
      `锁定后条目冻结、不可再变更。确认锁定「${suite.name}」？`,
      '锁定确认',
      { type: 'warning' },
    )
  } catch {
    return
  }
  lockSubmitting.value = true
  try {
    const data = await lockSuite(suite.suite_id, {
      baseline_name: lockForm.baseline_name || null,
      metrics,
    })
    ElMessage.success(`已锁定：${data.item_count} 个条目，哈希 ${data.lock_hash.slice(0, 12)}…`)
    lockVisible.value = false
    suiteDetail.value = await getSuite(suite.suite_id)
    await loadSuites()
  } catch (error) {
    ElMessage.error(errorMessage(error, '锁定失败'))
  } finally {
    lockSubmitting.value = false
  }
}

/** 差异（当前条目 vs 最新基线） */
const diffVisible = ref(false)
const diffLoading = ref(false)
const diffResult = ref<SuiteDiffData | null>(null)

async function openDiff(): Promise<void> {
  const suite = suiteDetail.value
  if (!suite) {
    return
  }
  diffVisible.value = true
  diffLoading.value = true
  diffResult.value = null
  try {
    diffResult.value = await diffSuite(suite.suite_id)
  } catch (error) {
    ElMessage.error(errorMessage(error, '差异计算失败'))
  } finally {
    diffLoading.value = false
  }
}

/** 基线列表 */
const baselineVisible = ref(false)
const baselineLoading = ref(false)
const baselines = ref<Baseline[]>([])

async function openBaselines(): Promise<void> {
  const suite = suiteDetail.value
  if (!suite) {
    return
  }
  baselineVisible.value = true
  baselineLoading.value = true
  try {
    const data = await listSuiteBaselines(suite.suite_id)
    baselines.value = data.items
  } catch (error) {
    ElMessage.error(errorMessage(error, '基线加载失败'))
  } finally {
    baselineLoading.value = false
  }
}

/** 引用候选（test_case 用编号/ID；concrete 用 UUID） */
const itemRefOptions = computed(() => {
  if (itemForm.item_type === 'test_case') {
    return testCases.value.map((row) => ({
      value: row.case_key,
      label: `${row.case_key} · ${row.name}`,
    }))
  }
  return []
})

function itemRefId(row: SuiteItem): string {
  return row.ref_id
}

onMounted(() => {
  void loadOracles()
  void loadTestCases()
  void loadSuites()
})
</script>

<template>
  <div class="scene-testcase">
    <el-row :gutter="12">
      <!-- ========================= 测试用例 ========================= -->
      <el-col :span="14">
        <el-card shadow="never">
          <template #header>
            <div class="card-header">
              <span>测试用例（ISO 34505）</span>
              <el-button v-permission="PERMISSIONS.sceneCreate" type="primary" size="small" @click="openCaseDialog">
                新建用例
              </el-button>
            </div>
          </template>

          <el-form :inline="true" class="filter">
            <el-form-item label="逻辑场景">
              <el-input v-model="testCaseQuery.logical_id" clearable placeholder="LS-…" style="width: 140px" />
            </el-form-item>
            <el-form-item label="状态">
              <el-select v-model="testCaseQuery.status" clearable placeholder="全部" style="width: 120px">
                <el-option
                  v-for="option in testCaseStatusOptions"
                  :key="option.value"
                  :label="option.label"
                  :value="option.value"
                />
              </el-select>
            </el-form-item>
            <el-form-item>
              <el-button type="primary" :loading="testCaseLoading" @click="loadTestCases">查询</el-button>
            </el-form-item>
          </el-form>

          <el-table v-loading="testCaseLoading" :data="testCases" size="small" row-key="test_case_id" empty-text="暂无用例">
            <el-table-column prop="case_key" label="编号" width="140" />
            <el-table-column prop="name" label="名称" min-width="170" show-overflow-tooltip />
            <el-table-column prop="logical_id" label="逻辑场景" width="130" />
            <el-table-column label="判据" min-width="190" show-overflow-tooltip>
              <template #default="{ row }">{{ row.oracle_ref }}</template>
            </el-table-column>
            <el-table-column label="平台" width="120" show-overflow-tooltip>
              <template #default="{ row }">{{ row.platforms.join(' / ') }}</template>
            </el-table-column>
            <el-table-column label="状态" width="90">
              <template #default="{ row }">
                <el-tag :type="TEST_CASE_STATUS_TAG_TYPES[row.status] ?? 'info'" size="small">
                  {{ TEST_CASE_STATUS_LABELS[row.status] ?? row.status }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column label="超时" width="80">
              <template #default="{ row }">{{ row.timeout_seconds }}s</template>
            </el-table-column>
            <el-table-column label="操作" width="80" fixed="right">
              <template #default="{ row }">
                <el-button link type="primary" @click="openCaseDetail(row)">详情</el-button>
              </template>
            </el-table-column>
          </el-table>

          <el-pagination
            class="pager"
            layout="total, prev, pager, next"
            :total="testCaseTotal"
            :page-size="testCaseQuery.page_size"
            @current-change="(page: number) => { testCaseQuery.page = page; loadTestCases() }"
          />
        </el-card>
      </el-col>

      <!-- ========================= 测试集 ========================= -->
      <el-col :span="10">
        <el-card shadow="never">
          <template #header>
            <div class="card-header">
              <span>测试集与基线</span>
              <el-button v-permission="PERMISSIONS.sceneCreate" type="primary" size="small" @click="suiteCreateVisible = true">
                新建测试集
              </el-button>
            </div>
          </template>

          <el-table v-loading="suiteLoading" :data="suites" size="small" row-key="suite_id" empty-text="暂无测试集">
            <el-table-column prop="name" label="名称" min-width="140" show-overflow-tooltip />
            <el-table-column label="状态" width="90">
              <template #default="{ row }">
                <el-tag :type="SUITE_STATUS_TAG_TYPES[row.status] ?? 'info'" size="small">
                  {{ SUITE_STATUS_LABELS[row.status] ?? row.status }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="item_count" label="条目" width="70" />
            <el-table-column label="更新时间" width="160">
              <template #default="{ row }">{{ formatDateTimeString(row.update_time) }}</template>
            </el-table-column>
            <el-table-column label="操作" width="90" fixed="right">
              <template #default="{ row }">
                <el-button link type="primary" @click="openSuiteDetail(row)">详情</el-button>
              </template>
            </el-table-column>
          </el-table>
          <div class="hint total">共 {{ suiteTotal }} 个测试集</div>
        </el-card>
      </el-col>
    </el-row>

    <!-- ========================= 新建用例 ========================= -->
    <el-dialog v-model="caseVisible" title="新建测试用例" width="680px">
      <el-form label-width="110px">
        <el-row :gutter="12">
          <el-col :span="12">
            <el-form-item label="用例编号" required>
              <el-input v-model="caseForm.case_key" placeholder="TC-SAFE-001" maxlength="48" />
            </el-form-item>
          </el-col>
          <el-col :span="12">
            <el-form-item label="逻辑场景" required>
              <el-input v-model="caseForm.logical_id" placeholder="LS-INT-001" maxlength="40" />
            </el-form-item>
          </el-col>
        </el-row>
        <el-form-item label="用例名称" required>
          <el-input v-model="caseForm.name" maxlength="128" />
        </el-form-item>
        <el-form-item label="测试目标" required>
          <el-input v-model="caseForm.objective" type="textarea" :rows="2" maxlength="500" show-word-limit />
        </el-form-item>
        <el-form-item label="判据" required>
          <el-select v-model="caseForm.oracle_ref" filterable style="width: 100%">
            <el-option
              v-for="oracle in oracles"
              :key="oracle.oracle_ref"
              :label="`${oracle.name}（${oracle.oracle_ref}）`"
              :value="oracle.oracle_ref"
            />
          </el-select>
        </el-form-item>
        <el-row :gutter="12">
          <el-col :span="12">
            <el-form-item label="平台">
              <el-select v-model="caseForm.platforms" multiple filterable allow-create default-first-option style="width: 100%">
                <el-option label="simulation" value="simulation" />
                <el-option label="vil" value="vil" />
                <el-option label="replay" value="replay" />
                <el-option label="hil" value="hil" />
              </el-select>
            </el-form-item>
          </el-col>
          <el-col :span="12">
            <el-form-item label="超时（s）">
              <el-input-number v-model="caseForm.timeout_seconds" :min="1" :max="3600" style="width: 100%" />
            </el-form-item>
          </el-col>
        </el-row>
        <el-form-item label="标签">
          <el-select v-model="caseForm.tags" multiple filterable allow-create default-first-option style="width: 100%" />
        </el-form-item>
        <el-form-item label="前置条件">
          <el-input v-model="caseForm.preconditionsText" type="textarea" :rows="2" placeholder='{"map_id": "Town01"}' />
        </el-form-item>
        <el-form-item label="测试步骤">
          <el-input v-model="caseForm.stepsText" type="textarea" :rows="4" placeholder='[{"index": 1, "action": "spawn"}]' />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="caseVisible = false">取消</el-button>
        <el-button type="primary" :loading="caseSubmitting" @click="submitCase">创建</el-button>
      </template>
    </el-dialog>

    <!-- ========================= 用例详情 ========================= -->
    <el-dialog v-model="caseDetailVisible" title="测试用例详情" width="720px">
      <template v-if="caseDetail">
        <el-descriptions :column="2" border size="small">
          <el-descriptions-item label="编号">{{ caseDetail.case_key }}</el-descriptions-item>
          <el-descriptions-item label="名称">{{ caseDetail.name }}</el-descriptions-item>
          <el-descriptions-item label="逻辑场景">{{ caseDetail.logical_id }}（{{ caseDetail.logical_version }}）</el-descriptions-item>
          <el-descriptions-item label="判据">{{ caseDetail.oracle_ref }}</el-descriptions-item>
          <el-descriptions-item label="平台">{{ caseDetail.platforms.join(' / ') }}</el-descriptions-item>
          <el-descriptions-item label="超时">{{ caseDetail.timeout_seconds }}s</el-descriptions-item>
          <el-descriptions-item label="目标" :span="2">{{ caseDetail.objective }}</el-descriptions-item>
          <el-descriptions-item label="前置条件" :span="2">
            <span class="mono">{{ JSON.stringify(caseDetail.preconditions) }}</span>
          </el-descriptions-item>
        </el-descriptions>
        <el-divider content-position="left">测试步骤</el-divider>
        <pre class="code">{{ caseStepsText }}</pre>
      </template>
    </el-dialog>

    <!-- ========================= 新建测试集 ========================= -->
    <el-dialog v-model="suiteCreateVisible" title="新建测试集" width="460px">
      <el-form label-width="90px">
        <el-form-item label="名称" required>
          <el-input v-model="suiteCreateForm.name" maxlength="128" />
        </el-form-item>
        <el-form-item label="描述">
          <el-input v-model="suiteCreateForm.description" type="textarea" :rows="2" maxlength="500" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="suiteCreateVisible = false">取消</el-button>
        <el-button type="primary" @click="submitSuiteCreate">创建</el-button>
      </template>
    </el-dialog>

    <!-- ========================= 测试集详情 ========================= -->
    <el-drawer v-model="suiteDrawerVisible" title="测试集详情" size="56%">
      <div v-loading="suiteLoadingDetail">
        <template v-if="suiteDetail">
          <el-descriptions :column="2" border size="small">
            <el-descriptions-item label="名称">{{ suiteDetail.name }}</el-descriptions-item>
            <el-descriptions-item label="状态">
              <el-tag :type="SUITE_STATUS_TAG_TYPES[suiteDetail.status] ?? 'info'" size="small">
                {{ SUITE_STATUS_LABELS[suiteDetail.status] ?? suiteDetail.status }}
              </el-tag>
            </el-descriptions-item>
            <el-descriptions-item label="条目数">{{ suiteDetail.item_count }}</el-descriptions-item>
            <el-descriptions-item label="锁定哈希">
              <span class="mono">{{ suiteDetail.lock_hash ?? '-' }}</span>
            </el-descriptions-item>
            <el-descriptions-item label="锁定时间">{{ formatDateTimeString(suiteDetail.locked_at) }}</el-descriptions-item>
            <el-descriptions-item label="锁定人">{{ suiteDetail.locked_by ?? '-' }}</el-descriptions-item>
            <el-descriptions-item label="描述" :span="2">{{ suiteDetail.description || '-' }}</el-descriptions-item>
          </el-descriptions>

          <div class="toolbar">
            <el-button
              v-permission="PERMISSIONS.sceneCreate"
              :disabled="suiteDetail.status !== 'draft'"
              size="small"
              @click="openItemDialog"
            >
              加条目
            </el-button>
            <el-button
              v-permission="PERMISSIONS.sceneCreate"
              :disabled="suiteDetail.status !== 'draft'"
              size="small"
              type="primary"
              @click="openLockDialog"
            >
              锁定并建基线
            </el-button>
            <el-button size="small" @click="openDiff">差异</el-button>
            <el-button size="small" @click="openBaselines">基线</el-button>
          </div>

          <el-table :data="suiteDetail.items" size="small" empty-text="暂无条目">
            <el-table-column prop="item_id" label="条目 ID" width="140" />
            <el-table-column label="类型" width="110">
              <template #default="{ row }">
                {{ SUITE_ITEM_TYPE_LABELS[row.item_type] ?? row.item_type }}
              </template>
            </el-table-column>
            <el-table-column label="引用" min-width="240" show-overflow-tooltip>
              <template #default="{ row }">{{ itemRefId(row) }}</template>
            </el-table-column>
            <el-table-column prop="weight" label="权重" width="80" />
          </el-table>
        </template>
      </div>
    </el-drawer>

    <!-- ========================= 加条目 ========================= -->
    <el-dialog v-model="itemVisible" title="测试集加条目（item_id 冲突覆盖）" width="480px">
      <el-form label-width="90px">
        <el-form-item label="条目 ID" required>
          <el-input v-model="itemForm.item_id" maxlength="64" placeholder="item-1" />
        </el-form-item>
        <el-form-item label="类型" required>
          <el-select v-model="itemForm.item_type" style="width: 100%">
            <el-option v-for="option in itemTypeOptions" :key="option.value" :label="option.label" :value="option.value" />
          </el-select>
        </el-form-item>
        <el-form-item label="引用" required>
          <el-select
            v-if="itemForm.item_type === 'test_case'"
            v-model="itemForm.ref_id"
            filterable
            allow-create
            default-first-option
            placeholder="用例编号（如 TC-SAFE-001）"
            style="width: 100%"
          >
            <el-option v-for="option in itemRefOptions" :key="option.value" :label="option.label" :value="option.value" />
          </el-select>
          <el-input v-else v-model="itemForm.ref_id" placeholder="具体场景 UUID" />
        </el-form-item>
        <el-form-item label="权重">
          <el-input-number v-model="itemForm.weight" :min="0.1" :max="100" :step="0.5" style="width: 100%" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="itemVisible = false">取消</el-button>
        <el-button type="primary" :loading="itemSubmitting" @click="submitItem">写入</el-button>
      </template>
    </el-dialog>

    <!-- ========================= 锁定 ========================= -->
    <el-dialog v-model="lockVisible" title="锁定测试集" width="520px">
      <el-form label-width="110px">
        <el-form-item label="基线名称">
          <el-input v-model="lockForm.baseline_name" maxlength="128" placeholder="可选，如 baseline-2026-10" />
        </el-form-item>
        <el-form-item label="指标快照">
          <el-input v-model="lockForm.metricsText" type="textarea" :rows="4" placeholder='{"pass_rate": 0.97}' />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="lockVisible = false">取消</el-button>
        <el-button type="primary" :loading="lockSubmitting" @click="submitLock">确认锁定</el-button>
      </template>
    </el-dialog>

    <!-- ========================= 差异 ========================= -->
    <el-dialog v-model="diffVisible" title="测试集差异（当前 vs 最新基线）" width="640px">
      <div v-loading="diffLoading">
        <template v-if="diffResult">
          <el-alert
            :type="diffResult.identical ? 'success' : 'warning'"
            :title="diffResult.identical ? '与基线完全一致' : '与基线存在差异'"
            :closable="false"
            show-icon
          />
          <el-descriptions :column="1" border size="small" class="sub-table">
            <el-descriptions-item label="当前哈希">
              <span class="mono">{{ diffResult.current_hash }}</span>
            </el-descriptions-item>
            <el-descriptions-item label="基线哈希">
              <span class="mono">{{ diffResult.baseline_hash ?? '-' }}</span>
            </el-descriptions-item>
            <el-descriptions-item label="新增条目">{{ diffResult.added.join('、') || '-' }}</el-descriptions-item>
            <el-descriptions-item label="移除条目">{{ diffResult.removed.join('、') || '-' }}</el-descriptions-item>
            <el-descriptions-item label="变更条目">{{ diffResult.changed.join('、') || '-' }}</el-descriptions-item>
          </el-descriptions>
        </template>
      </div>
    </el-dialog>

    <!-- ========================= 基线 ========================= -->
    <el-dialog v-model="baselineVisible" title="基线列表" width="720px">
      <el-table v-loading="baselineLoading" :data="baselines" size="small" empty-text="暂无基线（锁定时可创建）">
        <el-table-column prop="name" label="名称" width="170" />
        <el-table-column label="内容哈希" min-width="260">
          <template #default="{ row }"><span class="mono">{{ row.suite_hash }}</span></template>
        </el-table-column>
        <el-table-column prop="locked_by" label="锁定人" width="140" show-overflow-tooltip />
        <el-table-column label="创建时间" width="170">
          <template #default="{ row }">{{ formatDateTimeString(row.create_time) }}</template>
        </el-table-column>
      </el-table>
    </el-dialog>
  </div>
</template>

<style scoped>
.filter {
  margin-bottom: 8px;
}

.card-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.toolbar {
  margin: 12px 0;
  display: flex;
  gap: 8px;
}

.hint {
  color: var(--el-text-color-secondary);
  font-size: 12px;
}

.total {
  margin-top: 8px;
  text-align: right;
}

.pager {
  margin-top: 12px;
  text-align: right;
}

.sub-table {
  margin-top: 8px;
}

.code {
  max-height: 280px;
  overflow: auto;
  padding: 12px;
  background: var(--el-fill-color-lighter);
  border-radius: 6px;
  font-family: 'JetBrains Mono', Consolas, monospace;
  font-size: 12px;
  line-height: 1.5;
}

.mono {
  font-family: 'JetBrains Mono', Consolas, monospace;
  font-size: 12px;
}
</style>
