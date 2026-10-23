<script setup lang="ts">
/**
 * 场景生成器（scene-service 场景引擎）
 *
 * 契约（设计文档 §4.1）：
 * - GET  /api/v1/scene/logical            逻辑场景（DB 快照 + 内置目录）
 * - POST /api/v1/scene/logical/sync       目录幂等同步
 * - GET  /api/v1/scene/logical/{id}       详情（IR / 参数 / 绑定 / 约束）
 * - POST /api/v1/scene/logical/{id}/validate|dryrun|publish
 * - POST /api/v1/scene/generate           202 + task_id（异步批量生成）
 * - GET  /api/v1/scene/concrete           具体场景（Z1-Z4 / L1-L4 过滤）
 * - POST /api/v1/scene/concrete/{id}/render、/concrete/batch-validate
 * - GET  /api/v1/scene/coverage/overview|gaps
 */
import { computed, onBeforeUnmount, onMounted, reactive, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'

import {
  batchValidateConcreteScenarios,
  cancelGenerationTask,
  createGenerationTask,
  dryRunLogicalScenario,
  getConcreteScenario,
  getCoverageOverview,
  getLogicalScenario,
  getGenerationTask,
  listConcreteScenarios,
  listCoverageGaps,
  listGenerationTasks,
  listLogicalScenarios,
  publishLogicalScenario,
  renderConcreteScenario,
  syncLogicalCatalog,
  validateLogicalScenario,
} from '@/api/engine'
import {
  AVOIDABILITY_LABELS,
  CONCRETE_STATUS_LABELS,
  CONCRETE_STATUS_TAG_TYPES,
  CRITICALITY_LEVEL_LABELS,
  DEFAULT_PAGE_SIZE,
  GENERATION_PROFILE_LABELS,
  GENERATION_STRATEGY_LABELS,
  GENERATION_TASK_STATUS_LABELS,
  GENERATION_TASK_STATUS_TAG_TYPES,
  LOGICAL_STATUS_LABELS,
  LOGICAL_STATUS_TAG_TYPES,
  POLL_INTERVALS,
  RARITY_ZONE_LABELS,
  RARITY_ZONE_TAG_TYPES,
  RISK_LEVEL_LABELS,
  RISK_LEVEL_TAG_TYPES,
  SCENE_TEMPLATE_CATEGORY_LABELS,
} from '@/constants'
import { PERMISSIONS } from '@/constants/permissions'
import type {
  ConcreteListQuery,
  ConcreteScenarioDetail,
  ConcreteScenarioSummary,
  DryRunData,
  GenerateRequest,
  GenerationProfile,
  GenerationStrategy,
  GenerationTask,
  LogicalScenarioDetail,
  LogicalScenarioSummary,
  RenderData,
  ValidationReport,
  CoverageGap,
  CoverageOverview,
  GenerationTaskStatus,
} from '@/types/engine'
import { HunterApiError } from '@/utils/error-code'
import { formatDateTimeString, formatNumber } from '@/utils/format'

/** 当前 Tab（逻辑 / 生成 / 具体 / 覆盖） */
const activeTab = ref('logical')

const strategyOptions = Object.entries(GENERATION_STRATEGY_LABELS).map(([value, label]) => ({
  value: value as GenerationStrategy,
  label,
}))
const profileOptions = Object.entries(GENERATION_PROFILE_LABELS).map(([value, label]) => ({
  value: value as GenerationProfile,
  label,
}))
const categoryOptions = Object.entries(SCENE_TEMPLATE_CATEGORY_LABELS)
  .filter(([value]) => ['basic', 'interactive', 'environment', 'corner_case'].includes(value))
  .map(([value, label]) => ({ value, label }))
const taskStatusOptions = Object.entries(GENERATION_TASK_STATUS_LABELS).map(([value, label]) => ({
  value: value as GenerationTaskStatus,
  label,
}))
const zoneOptions = Object.entries(RARITY_ZONE_LABELS).map(([value, label]) => ({ value, label }))
const riskOptions = Object.entries(RISK_LEVEL_LABELS).map(([value, label]) => ({ value, label }))
const concreteStatusOptions = Object.entries(CONCRETE_STATUS_LABELS).map(([value, label]) => ({
  value,
  label,
}))

/** 去除空过滤项（避免把空字符串传给服务端） */
function compactParams<T extends object>(query: T): Record<string, unknown> {
  const params: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(query)) {
    if (value !== '' && value !== null && value !== undefined) {
      params[key] = value
    }
  }
  return params
}

/** 文本域 JSON → 对象（空文本 = {}） */
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

function errorMessage(error: unknown, fallback: string): string {
  return error instanceof HunterApiError ? error.message : fallback
}

/* =====================================================================
 * 一、逻辑场景
 * ===================================================================== */
const logicalLoading = ref(false)
const logicalRows = ref<LogicalScenarioSummary[]>([])
const logicalQuery = reactive({ category: '', keyword: '' })

async function loadLogical(): Promise<void> {
  logicalLoading.value = true
  try {
    const data = await listLogicalScenarios(compactParams(logicalQuery))
    logicalRows.value = data.items
  } catch (error) {
    ElMessage.error(errorMessage(error, '逻辑场景加载失败'))
  } finally {
    logicalLoading.value = false
  }
}

/** 同步内置目录（幂等 upsert，17 个逻辑场景） */
async function handleSyncCatalog(): Promise<void> {
  try {
    const data = await syncLogicalCatalog()
    ElMessage.success(`目录已同步（版本 ${data.version}，${data.synced} 个逻辑场景）`)
    await loadLogical()
  } catch (error) {
    ElMessage.error(errorMessage(error, '目录同步失败'))
  }
}

/** 详情抽屉 */
const detailVisible = ref(false)
const detailLoading = ref(false)
const detail = ref<LogicalScenarioDetail | null>(null)

async function openDetail(row: LogicalScenarioSummary): Promise<void> {
  detailVisible.value = true
  detailLoading.value = true
  detail.value = null
  try {
    detail.value = await getLogicalScenario(row.logical_id)
  } catch (error) {
    ElMessage.error(errorMessage(error, '详情加载失败'))
  } finally {
    detailLoading.value = false
  }
}

const detailIrText = computed(() =>
  detail.value ? JSON.stringify(detail.value.ir, null, 2) : '',
)

/** 静态校验（G1-G7 + 判据存在性） */
const validationVisible = ref(false)
const validationLoading = ref(false)
const validation = ref<ValidationReport | null>(null)

async function handleValidate(row: LogicalScenarioSummary): Promise<void> {
  validationVisible.value = true
  validationLoading.value = true
  validation.value = null
  try {
    validation.value = await validateLogicalScenario(row.logical_id)
  } catch (error) {
    ElMessage.error(errorMessage(error, '校验失败'))
  } finally {
    validationLoading.value = false
  }
}

/** 干跑（参数覆盖 → SceneConfig 预览） */
const dryrunVisible = ref(false)
const dryrunLoading = ref(false)
const dryrunTarget = ref<LogicalScenarioSummary | null>(null)
const dryrunValuesText = ref('{}')
const dryrunResult = ref<DryRunData | null>(null)

function openDryrun(row: LogicalScenarioSummary): void {
  dryrunTarget.value = row
  dryrunValuesText.value = JSON.stringify({}, null, 2)
  dryrunResult.value = null
  dryrunVisible.value = true
}

function applyDefaults(): void {
  const defaults = validation.value?.parameter_defaults
  if (!defaults) {
    ElMessage.info('请先执行一次「校验」以获取缺省参数')
    return
  }
  dryrunValuesText.value = JSON.stringify(defaults, null, 2)
}

async function submitDryrun(): Promise<void> {
  const target = dryrunTarget.value
  if (!target) {
    return
  }
  const values = parseJsonObject(dryrunValuesText.value, '参数覆盖')
  if (values === null) {
    return
  }
  dryrunLoading.value = true
  try {
    dryrunResult.value = await dryRunLogicalScenario(target.logical_id, {
      values: values as Record<string, number | string>,
    })
  } catch (error) {
    ElMessage.error(errorMessage(error, '干跑失败'))
  } finally {
    dryrunLoading.value = false
  }
}

/** 发布逻辑场景 */
async function handlePublish(row: LogicalScenarioSummary): Promise<void> {
  try {
    await ElMessageBox.confirm(
      `确认发布逻辑场景「${row.name}」？发布后作为生成/用例的快照版本。`,
      '发布确认',
      { type: 'warning' },
    )
  } catch {
    return
  }
  try {
    await publishLogicalScenario(row.logical_id)
    ElMessage.success('逻辑场景已发布')
    await loadLogical()
  } catch (error) {
    ElMessage.error(errorMessage(error, '发布失败'))
  }
}

/* =====================================================================
 * 二、生成任务
 * ===================================================================== */
const generateForm = reactive<GenerateRequest>({
  logical_id: '',
  count: 20,
  strategy: 'distribution',
  profile: 'regression',
  seed: 0,
  overrides: {},
})
const overridesText = ref('')
const generateSubmitting = ref(false)

const taskLoading = ref(false)
const tasks = ref<GenerationTask[]>([])
const taskTotal = ref(0)
const taskStatusFilter = ref<GenerationTaskStatus | undefined>(undefined)

let taskTimer: ReturnType<typeof setInterval> | null = null

function stopPolling(): void {
  if (taskTimer !== null) {
    clearInterval(taskTimer)
    taskTimer = null
  }
}

function ensurePolling(): void {
  if (taskTimer !== null) {
    return
  }
  taskTimer = setInterval(() => {
    void refreshTasks(true)
  }, POLL_INTERVALS.sceneTask)
}

async function refreshTasks(silent = false): Promise<void> {
  if (!silent) {
    taskLoading.value = true
  }
  try {
    const data = await listGenerationTasks({
      page: 1,
      page_size: 20,
      status: taskStatusFilter.value,
    })
    tasks.value = data.items
    taskTotal.value = data.total
    const active = data.items.some(
      (task) => task.status === 'pending' || task.status === 'running',
    )
    if (active) {
      ensurePolling()
    } else {
      stopPolling()
    }
  } catch (error) {
    if (!silent) {
      ElMessage.error(errorMessage(error, '生成任务加载失败'))
    }
    stopPolling()
  } finally {
    if (!silent) {
      taskLoading.value = false
    }
  }
}

async function submitGenerate(): Promise<void> {
  if (!generateForm.logical_id) {
    ElMessage.warning('请选择逻辑场景')
    return
  }
  const overrides = parseJsonObject(overridesText.value, '策略覆盖')
  if (overrides === null) {
    return
  }
  generateSubmitting.value = true
  try {
    const task = await createGenerationTask({ ...generateForm, overrides })
    ElMessage.success(`生成任务已受理（${task.task_id.slice(0, 8)}…），后台执行中`)
    await refreshTasks()
  } catch (error) {
    ElMessage.error(errorMessage(error, '生成任务提交失败'))
  } finally {
    generateSubmitting.value = false
  }
}

async function handleRefreshTask(task: GenerationTask): Promise<void> {
  try {
    const latest = await getGenerationTask(task.task_id)
    const index = tasks.value.findIndex((item) => item.task_id === latest.task_id)
    if (index >= 0) {
      tasks.value[index] = latest
    }
    if (latest.status === 'succeeded' || latest.status === 'failed') {
      await refreshTasks(true)
    }
  } catch (error) {
    ElMessage.error(errorMessage(error, '任务状态刷新失败'))
  }
}

async function handleCancelTask(task: GenerationTask): Promise<void> {
  try {
    await cancelGenerationTask(task.task_id)
    ElMessage.success('任务已取消')
    await refreshTasks()
  } catch (error) {
    ElMessage.error(errorMessage(error, '任务取消失败'))
  }
}

/** 查看任务产物（切到具体场景 Tab 并按逻辑场景过滤） */
function viewTaskScenarios(task: GenerationTask): void {
  concreteQuery.logical_id = task.logical_id
  concreteQuery.page = 1
  activeTab.value = 'concrete'
  void loadConcrete()
}

function taskProgressPercent(task: GenerationTask): number {
  return Math.min(100, Math.max(0, Math.round(task.progress * 100)))
}

function zoneSummary(counts: Record<string, number>): string {
  const zones = ['Z1', 'Z2', 'Z3', 'Z4']
  const parts = zones.filter((zone) => (counts[zone] ?? 0) > 0).map((zone) => `${zone}:${counts[zone]}`)
  return parts.length > 0 ? parts.join(' / ') : '-'
}

/* =====================================================================
 * 三、具体场景
 * ===================================================================== */
const concreteLoading = ref(false)
const concreteRows = ref<ConcreteScenarioSummary[]>([])
const concreteTotal = ref(0)
const concreteSelected = ref<ConcreteScenarioSummary[]>([])
const concreteQuery = reactive<ConcreteListQuery>({
  page: 1,
  page_size: DEFAULT_PAGE_SIZE,
  logical_id: '',
  rarity_zone: undefined,
  risk_level: undefined,
  status: undefined,
})
const validatingBatch = ref(false)

async function loadConcrete(): Promise<void> {
  concreteLoading.value = true
  try {
    const data = await listConcreteScenarios(compactParams(concreteQuery))
    concreteRows.value = data.items
    concreteTotal.value = data.total
  } catch (error) {
    ElMessage.error(errorMessage(error, '具体场景加载失败'))
  } finally {
    concreteLoading.value = false
  }
}

function handleConcreteSelection(rows: ConcreteScenarioSummary[]): void {
  concreteSelected.value = rows
}

async function handleBatchValidate(): Promise<void> {
  if (concreteSelected.value.length === 0) {
    ElMessage.warning('请先勾选需要复检的具体场景')
    return
  }
  validatingBatch.value = true
  try {
    const data = await batchValidateConcreteScenarios(
      concreteSelected.value.map((row) => row.scenario_id),
    )
    ElMessage.success(`复检完成：通过 ${data.validated}，隔离 ${data.quarantined}`)
    await loadConcrete()
  } catch (error) {
    ElMessage.error(errorMessage(error, '批量复检失败'))
  } finally {
    validatingBatch.value = false
  }
}

const concreteDetailVisible = ref(false)
const concreteDetailLoading = ref(false)
const concreteDetail = ref<ConcreteScenarioDetail | null>(null)
const concreteParamRows = computed(() =>
  Object.entries(concreteDetail.value?.param_values ?? {}).map(([name, value]) => ({
    name,
    value,
  })),
)
const concreteConfigText = computed(() =>
  concreteDetail.value ? JSON.stringify(concreteDetail.value.scene_config, null, 2) : '',
)

async function openConcreteDetail(row: ConcreteScenarioSummary): Promise<void> {
  concreteDetailVisible.value = true
  concreteDetailLoading.value = true
  concreteDetail.value = null
  try {
    concreteDetail.value = await getConcreteScenario(row.scenario_id)
  } catch (error) {
    ElMessage.error(errorMessage(error, '具体场景详情加载失败'))
  } finally {
    concreteDetailLoading.value = false
  }
}

const renderVisible = ref(false)
const renderLoading = ref(false)
const renderTarget = ref<ConcreteScenarioSummary | null>(null)
const renderResult = ref<RenderData | null>(null)
const renderConfigText = computed(() =>
  renderResult.value ? JSON.stringify(renderResult.value.scene_config, null, 2) : '',
)

async function openRender(row: ConcreteScenarioSummary): Promise<void> {
  renderTarget.value = row
  renderResult.value = null
  renderVisible.value = true
  renderLoading.value = true
  try {
    renderResult.value = await renderConcreteScenario(row.scenario_id)
  } catch (error) {
    ElMessage.error(errorMessage(error, '渲染失败'))
  } finally {
    renderLoading.value = false
  }
}

/* =====================================================================
 * 四、覆盖分析
 * ===================================================================== */
const coverageLoading = ref(false)
const coverageProfile = ref<GenerationProfile>('exploration')
const coverage = ref<CoverageOverview | null>(null)
const coverageGaps = ref<CoverageGap[]>([])

async function loadCoverage(): Promise<void> {
  coverageLoading.value = true
  try {
    const [overview, gaps] = await Promise.all([
      getCoverageOverview(),
      listCoverageGaps(coverageProfile.value),
    ])
    coverage.value = overview
    coverageGaps.value = gaps.items
  } catch (error) {
    ElMessage.error(errorMessage(error, '覆盖分析加载失败'))
  } finally {
    coverageLoading.value = false
  }
}

/** 盲区 → 带 target_zone 覆盖去生成 */
function handleFillGap(gap: CoverageGap): void {
  generateForm.logical_id = gap.logical_id
  generateForm.count = Math.min(Math.max(gap.deficit, 1), 500)
  generateForm.profile = coverageProfile.value
  generateForm.strategy = 'distribution'
  overridesText.value = JSON.stringify({ target_zone: gap.zone }, null, 2)
  activeTab.value = 'generate'
}

/* =====================================================================
 * 生命周期
 * ===================================================================== */
function handleTabChange(name: string | number): void {
  const tab = String(name)
  if (tab === 'tasks') {
    void refreshTasks()
  } else if (tab === 'concrete') {
    void loadConcrete()
  } else if (tab === 'coverage') {
    void loadCoverage()
  }
}

onMounted(() => {
  void loadLogical()
  void refreshTasks()
})

onBeforeUnmount(stopPolling)
</script>

<template>
  <div class="scene-generator">
    <el-tabs v-model="activeTab" class="tabs" @tab-change="handleTabChange">
      <!-- ========================= 逻辑场景 ========================= -->
      <el-tab-pane label="逻辑场景" name="logical">
        <el-card shadow="never">
          <el-form :inline="true" class="filter">
            <el-form-item label="分类">
              <el-select v-model="logicalQuery.category" clearable placeholder="全部分类" style="width: 150px">
                <el-option
                  v-for="option in categoryOptions"
                  :key="option.value"
                  :label="option.label"
                  :value="option.value"
                />
              </el-select>
            </el-form-item>
            <el-form-item label="关键词">
              <el-input
                v-model="logicalQuery.keyword"
                clearable
                placeholder="逻辑场景 ID / 名称"
                style="width: 200px"
                @keyup.enter="loadLogical"
              />
            </el-form-item>
            <el-form-item>
              <el-button type="primary" :loading="logicalLoading" @click="loadLogical">查询</el-button>
            </el-form-item>
          </el-form>

          <div class="toolbar">
            <el-button
              v-permission="PERMISSIONS.sceneCreate"
              :loading="logicalLoading"
              @click="handleSyncCatalog"
            >
              同步内置目录
            </el-button>
            <span class="hint">IR 单一事实来源为内置目录，数据库为发布快照（DB 优先、目录兜底）</span>
          </div>

          <el-table
            v-loading="logicalLoading"
            :data="logicalRows"
            size="small"
            row-key="logical_id"
            empty-text="暂无逻辑场景"
          >
            <el-table-column prop="logical_id" label="逻辑场景 ID" width="140" />
            <el-table-column prop="name" label="名称" min-width="180" show-overflow-tooltip />
            <el-table-column label="分类" width="110">
              <template #default="{ row }">
                {{ SCENE_TEMPLATE_CATEGORY_LABELS[row.category] ?? row.category }}
              </template>
            </el-table-column>
            <el-table-column prop="scene_type" label="场景类型" width="150" show-overflow-tooltip />
            <el-table-column label="参数" width="70">
              <template #default="{ row }">{{ row.parameter_count }}</template>
            </el-table-column>
            <el-table-column prop="oracle_ref" label="判据" min-width="210" show-overflow-tooltip />
            <el-table-column label="状态" width="100">
              <template #default="{ row }">
                <el-tag :type="LOGICAL_STATUS_TAG_TYPES[row.status] ?? 'info'" size="small">
                  {{ LOGICAL_STATUS_LABELS[row.status] ?? row.status }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column label="来源" width="90">
              <template #default="{ row }">
                <el-tag :type="row.origin === 'database' ? 'success' : 'info'" size="small" effect="plain">
                  {{ row.origin === 'database' ? '数据库' : '目录' }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column label="操作" width="260" fixed="right">
              <template #default="{ row }">
                <el-button link type="primary" @click="openDetail(row)">详情</el-button>
                <el-button v-permission="PERMISSIONS.sceneRead" link type="primary" @click="handleValidate(row)">
                  校验
                </el-button>
                <el-button v-permission="PERMISSIONS.sceneRead" link type="primary" @click="openDryrun(row)">
                  干跑
                </el-button>
                <el-button
                  v-permission="PERMISSIONS.sceneExecute"
                  link
                  type="success"
                  @click="handlePublish(row)"
                >
                  发布
                </el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-card>
      </el-tab-pane>

      <!-- ========================= 生成任务 ========================= -->
      <el-tab-pane label="生成任务" name="tasks">
        <el-card shadow="never" class="block">
          <template #header>
            <span>提交生成任务（202 + task_id，结果按内容哈希去重）</span>
          </template>
          <el-form label-width="110px" class="generate-form">
            <el-row :gutter="16">
              <el-col :span="12">
                <el-form-item label="逻辑场景" required>
                  <el-select v-model="generateForm.logical_id" filterable placeholder="选择逻辑场景" style="width: 100%">
                    <el-option
                      v-for="row in logicalRows"
                      :key="row.logical_id"
                      :label="`${row.logical_id} · ${row.name}`"
                      :value="row.logical_id"
                    />
                  </el-select>
                </el-form-item>
              </el-col>
              <el-col :span="6">
                <el-form-item label="策略">
                  <el-select v-model="generateForm.strategy" style="width: 100%">
                    <el-option
                      v-for="option in strategyOptions"
                      :key="option.value"
                      :label="option.label"
                      :value="option.value"
                    />
                  </el-select>
                </el-form-item>
              </el-col>
              <el-col :span="6">
                <el-form-item label="剖面">
                  <el-select v-model="generateForm.profile" style="width: 100%">
                    <el-option
                      v-for="option in profileOptions"
                      :key="option.value"
                      :label="option.label"
                      :value="option.value"
                    />
                  </el-select>
                </el-form-item>
              </el-col>
            </el-row>
            <el-row :gutter="16">
              <el-col :span="12">
                <el-form-item label="目标数量">
                  <el-input-number v-model="generateForm.count" :min="1" :max="5000" style="width: 100%" />
                </el-form-item>
              </el-col>
              <el-col :span="12">
                <el-form-item label="随机种子">
                  <el-input-number v-model="generateForm.seed" :min="0" :max="9007199254740991" style="width: 100%" />
                </el-form-item>
              </el-col>
            </el-row>
            <el-form-item label="策略覆盖">
              <el-input
                v-model="overridesText"
                type="textarea"
                :rows="3"
                placeholder='可选，JSON 对象：{"fixed": {...}} / {"target_zone": "Z3"} / {"base": {...}}（replay_mutation 必填）/ {"candidates": [...]}（ai_generated 必填）'
              />
            </el-form-item>
            <el-form-item>
              <el-button
                v-permission="PERMISSIONS.sceneExecute"
                type="primary"
                :loading="generateSubmitting"
                @click="submitGenerate"
              >
                提交生成
              </el-button>
              <span class="hint">seed=0 由服务端生成；同 seed 重放按内容哈希幂等跳过</span>
            </el-form-item>
          </el-form>
        </el-card>

        <el-card shadow="never">
          <div class="toolbar">
            <el-select
              v-model="taskStatusFilter"
              clearable
              placeholder="全部状态"
              style="width: 150px"
              @change="() => refreshTasks()"
            >
              <el-option
                v-for="option in taskStatusOptions"
                :key="option.value"
                :label="option.label"
                :value="option.value"
              />
            </el-select>
            <el-button :loading="taskLoading" @click="refreshTasks()">刷新</el-button>
            <span class="hint">执行中的任务每 {{ POLL_INTERVALS.sceneTask / 1000 }}s 自动刷新</span>
          </div>

          <el-table v-loading="taskLoading" :data="tasks" size="small" row-key="task_id" empty-text="暂无生成任务">
            <el-table-column label="任务" width="120">
              <template #default="{ row }">
                <el-tooltip :content="row.task_id" placement="top">
                  <span>{{ row.task_id.slice(0, 8) }}…</span>
                </el-tooltip>
              </template>
            </el-table-column>
            <el-table-column prop="logical_id" label="逻辑场景" width="140" />
            <el-table-column label="策略 / 剖面" min-width="190" show-overflow-tooltip>
              <template #default="{ row }">
                {{ GENERATION_STRATEGY_LABELS[row.strategy] ?? row.strategy }} /
                {{ GENERATION_PROFILE_LABELS[row.profile] ?? row.profile }}
              </template>
            </el-table-column>
            <el-table-column label="状态" width="100">
              <template #default="{ row }">
                <el-tag :type="GENERATION_TASK_STATUS_TAG_TYPES[row.status] ?? 'info'" size="small">
                  {{ GENERATION_TASK_STATUS_LABELS[row.status] ?? row.status }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column label="进度" width="150">
              <template #default="{ row }">
                <el-progress :percentage="taskProgressPercent(row)" :stroke-width="8" />
              </template>
            </el-table-column>
            <el-table-column label="产出" width="150">
              <template #default="{ row }">
                实产 {{ row.produced }} / 目标 {{ row.count }}
                <div class="hint">重复 {{ row.duplicates }} · 拒绝 {{ row.rejected }}</div>
              </template>
            </el-table-column>
            <el-table-column label="分区" width="150">
              <template #default="{ row }">{{ zoneSummary(row.zone_counts) }}</template>
            </el-table-column>
            <el-table-column label="创建时间" width="170">
              <template #default="{ row }">{{ formatDateTimeString(row.create_time) }}</template>
            </el-table-column>
            <el-table-column label="操作" width="200" fixed="right">
              <template #default="{ row }">
                <el-button link type="primary" @click="handleRefreshTask(row)">刷新</el-button>
                <el-button
                  v-if="row.status === 'succeeded'"
                  link
                  type="primary"
                  @click="viewTaskScenarios(row)"
                >
                  查看产物
                </el-button>
                <el-button
                  v-if="row.status === 'pending'"
                  v-permission="PERMISSIONS.sceneCreate"
                  link
                  type="danger"
                  @click="handleCancelTask(row)"
                >
                  取消
                </el-button>
                <el-tooltip v-if="row.error_message" :content="row.error_message" placement="top">
                  <el-tag type="danger" size="small">失败原因</el-tag>
                </el-tooltip>
              </template>
            </el-table-column>
          </el-table>
          <div class="hint total">共 {{ taskTotal }} 个任务</div>
        </el-card>
      </el-tab-pane>

      <!-- ========================= 具体场景 ========================= -->
      <el-tab-pane label="具体场景" name="concrete">
        <el-card shadow="never">
          <el-form :inline="true" class="filter">
            <el-form-item label="逻辑场景">
              <el-input v-model="concreteQuery.logical_id" clearable placeholder="LS-…" style="width: 160px" />
            </el-form-item>
            <el-form-item label="稀有度">
              <el-select v-model="concreteQuery.rarity_zone" clearable placeholder="全部" style="width: 130px">
                <el-option v-for="option in zoneOptions" :key="option.value" :label="option.label" :value="option.value" />
              </el-select>
            </el-form-item>
            <el-form-item label="风险">
              <el-select v-model="concreteQuery.risk_level" clearable placeholder="全部" style="width: 110px">
                <el-option v-for="option in riskOptions" :key="option.value" :label="option.label" :value="option.value" />
              </el-select>
            </el-form-item>
            <el-form-item label="状态">
              <el-select v-model="concreteQuery.status" clearable placeholder="全部" style="width: 120px">
                <el-option
                  v-for="option in concreteStatusOptions"
                  :key="option.value"
                  :label="option.label"
                  :value="option.value"
                />
              </el-select>
            </el-form-item>
            <el-form-item>
              <el-button type="primary" :loading="concreteLoading" @click="loadConcrete">查询</el-button>
            </el-form-item>
          </el-form>

          <div class="toolbar">
            <el-button
              v-permission="PERMISSIONS.sceneCreate"
              :disabled="concreteSelected.length === 0"
              :loading="validatingBatch"
              @click="handleBatchValidate"
            >
              批量复检（已选 {{ concreteSelected.length }}）
            </el-button>
            <span class="hint">复检重跑 G1-G7：通过 → validated，失败 → quarantined（不删除）</span>
          </div>

          <el-table
            v-loading="concreteLoading"
            :data="concreteRows"
            size="small"
            row-key="scenario_id"
            empty-text="暂无具体场景（先生成）"
            @selection-change="handleConcreteSelection"
          >
            <el-table-column type="selection" width="46" />
            <el-table-column prop="name" label="名称" min-width="170" show-overflow-tooltip />
            <el-table-column prop="logical_id" label="逻辑场景" width="140" />
            <el-table-column label="稀有度" width="120">
              <template #default="{ row }">
                <el-tag :type="RARITY_ZONE_TAG_TYPES[row.rarity_zone] ?? 'info'" size="small">
                  {{ RARITY_ZONE_LABELS[row.rarity_zone] ?? row.rarity_zone }}
                </el-tag>
                <div class="hint">p={{ formatNumber(row.rarity_p, 4) }}</div>
              </template>
            </el-table-column>
            <el-table-column label="临界度" width="110">
              <template #default="{ row }">
                {{ CRITICALITY_LEVEL_LABELS[row.criticality_level] ?? row.criticality_level }}
                <div class="hint">CI={{ formatNumber(row.criticality_ci, 3) }}</div>
              </template>
            </el-table-column>
            <el-table-column label="风险" width="90">
              <template #default="{ row }">
                <el-tag :type="RISK_LEVEL_TAG_TYPES[row.risk_level] ?? 'info'" size="small" effect="dark">
                  {{ row.risk_level }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column label="可避免性" width="110">
              <template #default="{ row }">
                {{ AVOIDABILITY_LABELS[row.avoidability] ?? row.avoidability }}
              </template>
            </el-table-column>
            <el-table-column label="VIL 安全" width="90">
              <template #default="{ row }">
                <el-tag :type="row.vil_safe ? 'success' : 'warning'" size="small" effect="plain">
                  {{ row.vil_safe ? '安全' : '需复核' }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column label="状态" width="100">
              <template #default="{ row }">
                <el-tag :type="CONCRETE_STATUS_TAG_TYPES[row.status] ?? 'info'" size="small">
                  {{ CONCRETE_STATUS_LABELS[row.status] ?? row.status }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column label="操作" width="170" fixed="right">
              <template #default="{ row }">
                <el-button link type="primary" @click="openConcreteDetail(row)">详情</el-button>
                <el-button v-permission="PERMISSIONS.sceneRead" link type="primary" @click="openRender(row)">
                  渲染
                </el-button>
              </template>
            </el-table-column>
          </el-table>

          <el-pagination
            class="pager"
            layout="total, sizes, prev, pager, next"
            :total="concreteTotal"
            :page-size="concreteQuery.page_size"
            :page-sizes="[DEFAULT_PAGE_SIZE, 50, 100]"
            @current-change="(page: number) => { concreteQuery.page = page; loadConcrete() }"
            @size-change="(size: number) => { concreteQuery.page_size = size; concreteQuery.page = 1; loadConcrete() }"
          />
        </el-card>
      </el-tab-pane>

      <!-- ========================= 覆盖分析 ========================= -->
      <el-tab-pane label="覆盖分析" name="coverage">
        <el-card v-loading="coverageLoading" shadow="never" class="block">
          <template #header>
            <div class="coverage-header">
              <span>覆盖总览（稀有度 × 风险）</span>
              <el-select v-model="coverageProfile" style="width: 220px" @change="loadCoverage">
                <el-option
                  v-for="option in profileOptions"
                  :key="option.value"
                  :label="option.label"
                  :value="option.value"
                />
              </el-select>
            </div>
          </template>
          <el-row :gutter="12">
            <el-col :span="6">
              <div class="metric">
                <div class="metric__label">具体场景总数</div>
                <div class="metric__value">{{ coverage?.total ?? 0 }}</div>
              </div>
            </el-col>
            <el-col v-for="zone in ['Z1', 'Z2', 'Z3', 'Z4']" :key="zone" :span="4">
              <div class="metric">
                <div class="metric__label">{{ RARITY_ZONE_LABELS[zone] }}</div>
                <div class="metric__value">{{ coverage?.zone_counts[zone] ?? 0 }}</div>
                <div class="hint">
                  占比 {{ formatNumber((coverage?.zone_ratio[zone] ?? 0) * 100, 1) }}%
                </div>
              </div>
            </el-col>
          </el-row>
          <div class="hint matrix">风险矩阵：{{ coverage?.risk_matrix_applied ? '已应用（稀有度 × 临界度）' : '未应用' }}</div>
        </el-card>

        <el-card v-loading="coverageLoading" shadow="never">
          <template #header>
            <span>覆盖盲区（按 {{ GENERATION_PROFILE_LABELS[coverageProfile] }} 配额计算 Z3/Z4 缺口）</span>
          </template>
          <el-table :data="coverageGaps" size="small" empty-text="当前剖面下无缺口">
            <el-table-column prop="logical_id" label="逻辑场景" width="140" />
            <el-table-column prop="name" label="名称" min-width="180" show-overflow-tooltip />
            <el-table-column label="分区" width="120">
              <template #default="{ row }">
                <el-tag :type="RARITY_ZONE_TAG_TYPES[row.zone] ?? 'info'" size="small">
                  {{ RARITY_ZONE_LABELS[row.zone] ?? row.zone }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="count" label="现有" width="80" />
            <el-table-column prop="target" label="目标" width="80" />
            <el-table-column prop="deficit" label="缺口" width="80" />
            <el-table-column prop="recommendation" label="补采建议" min-width="300" show-overflow-tooltip />
            <el-table-column label="操作" width="100" fixed="right">
              <template #default="{ row }">
                <el-button v-permission="PERMISSIONS.sceneExecute" link type="primary" @click="handleFillGap(row)">
                  去生成
                </el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-card>
      </el-tab-pane>
    </el-tabs>

    <!-- ========================= 逻辑场景详情 ========================= -->
    <el-drawer v-model="detailVisible" title="逻辑场景详情" size="60%">
      <div v-loading="detailLoading">
        <template v-if="detail">
          <el-descriptions :column="2" border size="small">
            <el-descriptions-item label="逻辑场景 ID">{{ detail.logical_id }}</el-descriptions-item>
            <el-descriptions-item label="版本">{{ detail.version }}</el-descriptions-item>
            <el-descriptions-item label="名称">{{ detail.name }}</el-descriptions-item>
            <el-descriptions-item label="场景类型">{{ detail.scene_type }}</el-descriptions-item>
            <el-descriptions-item label="判据">{{ detail.oracle_ref }}</el-descriptions-item>
            <el-descriptions-item label="内容哈希">
              <span class="mono">{{ detail.content_hash }}</span>
            </el-descriptions-item>
            <el-descriptions-item label="支持模式" :span="2">
              {{ detail.allowed_modes.join(' / ') || '-' }}
            </el-descriptions-item>
            <el-descriptions-item label="描述" :span="2">{{ detail.description || '-' }}</el-descriptions-item>
          </el-descriptions>

          <el-divider content-position="left">参数分布</el-divider>
          <el-table :data="detail.parameters" size="small">
            <el-table-column prop="name" label="参数" width="130" />
            <el-table-column label="分布" width="150">
              <template #default="{ row }">{{ row.distribution.kind }}</template>
            </el-table-column>
            <el-table-column label="分布参数" min-width="240">
              <template #default="{ row }">
                <span class="mono">{{ JSON.stringify(row.distribution.params) }}</span>
              </template>
            </el-table-column>
            <el-table-column prop="unit" label="单位" width="80" />
            <el-table-column prop="description" label="说明" min-width="180" show-overflow-tooltip />
          </el-table>

          <el-divider content-position="left">绑定 / 约束 / 相关性</el-divider>
          <el-table :data="detail.bindings" size="small" empty-text="无绑定">
            <el-table-column prop="path" label="SceneConfig 路径" min-width="220" />
            <el-table-column prop="expr" label="表达式" min-width="200" />
          </el-table>
          <el-table :data="detail.constraints" size="small" class="sub-table" empty-text="无约束">
            <el-table-column prop="name" label="约束" width="160" />
            <el-table-column label="表达式" min-width="320">
              <template #default="{ row }">
                <span class="mono">{{ JSON.stringify(row) }}</span>
              </template>
            </el-table-column>
          </el-table>

          <el-divider content-position="left">IR（hunter.ir/1.2）</el-divider>
          <pre class="code">{{ detailIrText }}</pre>
        </template>
      </div>
    </el-drawer>

    <!-- ========================= 静态校验 ========================= -->
    <el-dialog v-model="validationVisible" title="静态校验（G1-G7）" width="720px">
      <div v-loading="validationLoading">
        <template v-if="validation">
          <el-alert
            :type="validation.passed ? 'success' : 'error'"
            :title="validation.passed ? '校验通过（参数缺省值可编译）' : '校验未通过'"
            :description="validation.failures.join('；')"
            show-icon
            :closable="false"
          />
          <el-table :data="validation.gates" size="small" class="sub-table">
            <el-table-column prop="gate" label="门" width="70" />
            <el-table-column label="结果" width="90">
              <template #default="{ row }">
                <el-tag :type="row.passed ? 'success' : 'danger'" size="small">
                  {{ row.passed ? '通过' : '失败' }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="message" label="说明" min-width="420" show-overflow-tooltip />
          </el-table>
          <div class="hint">
            判据 {{ validation.oracle_ref }}：{{ validation.oracle_found ? '存在' : '未知' }}；缺省参数
            {{ Object.keys(validation.parameter_defaults).length }} 个
          </div>
        </template>
      </div>
    </el-dialog>

    <!-- ========================= 干跑 ========================= -->
    <el-dialog v-model="dryrunVisible" title="干跑（SceneConfig 预览）" width="820px">
      <el-form label-width="110px">
        <el-form-item label="参数覆盖">
          <el-input v-model="dryrunValuesText" type="textarea" :rows="4" placeholder='{"v0": 10.0}' />
        </el-form-item>
        <el-form-item>
          <el-button size="small" @click="applyDefaults">填入校验缺省值</el-button>
          <el-button type="primary" size="small" :loading="dryrunLoading" @click="submitDryrun">
            执行干跑
          </el-button>
        </el-form-item>
      </el-form>
      <template v-if="dryrunResult">
        <el-alert
          :type="dryrunResult.passed ? 'success' : 'warning'"
          :title="dryrunResult.passed ? '干跑通过' : '干跑未通过（可参考闸门明细）'"
          :description="dryrunResult.failures.join('；')"
          show-icon
          :closable="false"
        />
        <el-table :data="dryrunResult.gates" size="small" class="sub-table">
          <el-table-column prop="gate" label="门" width="70" />
          <el-table-column label="结果" width="80">
            <template #default="{ row }">
              <el-tag :type="row.passed ? 'success' : 'danger'" size="small">
                {{ row.passed ? '通过' : '失败' }}
              </el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="message" label="说明" min-width="480" show-overflow-tooltip />
        </el-table>
        <el-divider content-position="left">SceneConfig</el-divider>
        <pre class="code">{{ JSON.stringify(dryrunResult.scene_config, null, 2) }}</pre>
      </template>
    </el-dialog>

    <!-- ========================= 具体场景详情 ========================= -->
    <el-dialog v-model="concreteDetailVisible" title="具体场景详情" width="820px">
      <div v-loading="concreteDetailLoading">
        <template v-if="concreteDetail">
          <el-descriptions :column="2" border size="small">
            <el-descriptions-item label="场景 ID">
              <span class="mono">{{ concreteDetail.scenario_id }}</span>
            </el-descriptions-item>
            <el-descriptions-item label="逻辑场景">
              {{ concreteDetail.logical_id }}（{{ concreteDetail.logical_version }}）
            </el-descriptions-item>
            <el-descriptions-item label="稀有度">
              {{ RARITY_ZONE_LABELS[concreteDetail.rarity_zone] }}（p={{ formatNumber(concreteDetail.rarity_p, 4) }}）
            </el-descriptions-item>
            <el-descriptions-item label="临界度">
              {{ CRITICALITY_LEVEL_LABELS[concreteDetail.criticality_level] }}（CI={{ formatNumber(concreteDetail.criticality_ci, 3) }}）
            </el-descriptions-item>
            <el-descriptions-item label="风险等级">{{ concreteDetail.risk_level }}</el-descriptions-item>
            <el-descriptions-item label="可避免性">
              {{ AVOIDABILITY_LABELS[concreteDetail.avoidability] ?? concreteDetail.avoidability }}
            </el-descriptions-item>
            <el-descriptions-item label="种子">{{ concreteDetail.seed }}</el-descriptions-item>
            <el-descriptions-item label="内容哈希">
              <span class="mono">{{ concreteDetail.content_hash }}</span>
            </el-descriptions-item>
          </el-descriptions>
          <el-divider content-position="left">参数向量</el-divider>
          <el-table :data="concreteParamRows" size="small">
            <el-table-column prop="name" label="参数" width="150" />
            <el-table-column prop="value" label="取值" min-width="200" />
          </el-table>
          <el-divider content-position="left">SceneConfig</el-divider>
          <pre class="code">{{ concreteConfigText }}</pre>
        </template>
      </div>
    </el-dialog>

    <!-- ========================= 渲染 ========================= -->
    <el-dialog v-model="renderVisible" title="渲染 SceneConfig" width="780px">
      <div v-loading="renderLoading">
        <template v-if="renderResult">
          <el-alert
            :type="renderResult.passed ? 'success' : 'warning'"
            :title="renderResult.passed ? '渲染通过（可直接下发 Carla）' : '渲染未通过，请检查闸门明细'"
            :closable="false"
            show-icon
          />
          <el-table :data="renderResult.gates" size="small" class="sub-table">
            <el-table-column prop="gate" label="门" width="70" />
            <el-table-column label="结果" width="80">
              <template #default="{ row }">
                <el-tag :type="row.passed ? 'success' : 'danger'" size="small">
                  {{ row.passed ? '通过' : '失败' }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="message" label="说明" min-width="460" show-overflow-tooltip />
          </el-table>
          <pre class="code">{{ renderConfigText }}</pre>
        </template>
      </div>
    </el-dialog>
  </div>
</template>

<style scoped>
.scene-generator {
  padding: 4px;
}

.filter {
  margin-bottom: 8px;
}

.toolbar {
  margin-bottom: 12px;
  display: flex;
  align-items: center;
  gap: 12px;
}

.hint {
  color: var(--el-text-color-secondary);
  font-size: 12px;
}

.block {
  margin-bottom: 12px;
}

.generate-form {
  max-width: 980px;
}

.sub-table {
  margin-top: 8px;
}

.pager {
  margin-top: 12px;
  text-align: right;
}

.total {
  margin-top: 8px;
  text-align: right;
}

.coverage-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.metric {
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 6px;
  padding: 12px;
  margin-bottom: 8px;
}

.metric__label {
  color: var(--el-text-color-secondary);
  font-size: 12px;
}

.metric__value {
  font-size: 26px;
  font-weight: 600;
  line-height: 1.4;
}

.matrix {
  margin-top: 8px;
}

.code {
  max-height: 320px;
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
