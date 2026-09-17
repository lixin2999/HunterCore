<script setup lang="ts">
/**
 * 场景库（scene-service 模块）
 *
 * 契约：
 * - GET    /api/v1/scene（分页 + scene_type/status/tags/keyword 过滤）
 * - POST   /api/v1/scene（新建 201；唯一键冲突 3002）
 * - POST   /api/v1/scene/{scene_id}/duplicate（复制）
 * - POST   /api/v1/scene/{scene_id}/publish（发布草稿）
 * - DELETE /api/v1/scene/{scene_id}
 * - POST   /api/v1/scene/export（批量导出 → 预签名下载 URL）
 * - POST   /api/v1/scene/{scene_id}/run（下发 Carla 仿真）
 */
import { reactive, ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox, type FormInstance, type FormRules } from 'element-plus'

import {
  createScene,
  deleteScene,
  duplicateScene,
  exportScenes,
  listSceneTemplates,
  listScenes,
  publishScene,
  runScene,
} from '@/api/scene'
import StatusTag from '@/components/common/StatusTag.vue'
import {
  DEFAULT_PAGE_SIZE,
  SCENE_EXPORT_FORMAT_LABELS,
  SCENE_STATUS_LABELS,
  SCENE_TYPE_LABELS,
} from '@/constants'
import { PERMISSIONS } from '@/constants/permissions'
import type {
  Scene,
  SceneExportFormat,
  SceneListQuery,
  SceneTemplate,
  SceneUpsertRequest,
} from '@/types/scene'
import { HunterApiError } from '@/utils/error-code'
import { downloadByUrl } from '@/utils/hash'
import { formatBytes, formatDateTimeString } from '@/utils/format'

const router = useRouter()

const loading = ref(false)
const scenes = ref<Scene[]>([])
const total = ref(0)
const selected = ref<Scene[]>([])

const query = reactive<SceneListQuery>({
  page: 1,
  page_size: DEFAULT_PAGE_SIZE,
  scene_type: undefined,
  status: undefined,
  keyword: undefined,
  sort: 'update_time',
  order: 'desc',
})

const sceneTypeOptions = Object.entries(SCENE_TYPE_LABELS).map(([value, label]) => ({ value, label }))
const sceneStatusOptions = Object.entries(SCENE_STATUS_LABELS).map(([value, label]) => ({ value, label }))
const exportFormatOptions = Object.entries(SCENE_EXPORT_FORMAT_LABELS).map(([value, label]) => ({
  value: value as SceneExportFormat,
  label,
}))

/** 列表加载 */
async function loadScenes(): Promise<void> {
  loading.value = true
  try {
    const data = await listScenes({ ...query })
    scenes.value = data.items
    total.value = data.total
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '场景列表加载失败')
  } finally {
    loading.value = false
  }
}

function handleSelectionChange(rows: Scene[]): void {
  selected.value = rows
}

/* ------------------------------ 新建场景 ------------------------------ */
const createVisible = ref(false)
const createFormRef = ref<FormInstance>()
const createSubmitting = ref(false)
const templates = ref<SceneTemplate[]>([])
const createForm = reactive<SceneUpsertRequest>({
  scene_name: '',
  scene_type: 'straight_cruise',
  description: '',
  tags: [],
  config: {
    map: { map_id: 'Town01', map_type: 'carla_town', spawn_point: { x: 0, y: 0, z: 0, yaw: 0 } },
    ego_vehicle: { model: 'HUNTER_SE', initial_speed: 0, initial_steer: 0 },
    weather: { cloudiness: 0, rain: 0, wetness: 0, fog: 0, wind: 0, sun_azimuth: 0, sun_altitude: 45 },
    actors: [],
    events: [],
    success_criteria: { max_speed_deviation: 0.5, no_collision: true, min_safe_distance: 2 },
    duration: 60,
  },
})

const createRules: FormRules<SceneUpsertRequest> = {
  scene_name: [{ required: true, message: '请输入场景名称', trigger: 'blur' }],
  scene_type: [{ required: true, message: '请选择场景类型', trigger: 'change' }],
}

async function openCreate(): Promise<void> {
  createVisible.value = true
  if (templates.value.length === 0) {
    try {
      const data = await listSceneTemplates()
      templates.value = data.items
    } catch (error) {
      ElMessage.warning(error instanceof HunterApiError ? error.message : '模板加载失败')
    }
  }
}

/** 套用模板配置（模板来自 GET /api/v1/scene/templates） */
function applyTemplate(templateId: string): void {
  const template = templates.value.find((item) => item.template_id === templateId)
  if (!template) {
    return
  }
  createForm.scene_type = template.scene_type
  createForm.config = JSON.parse(JSON.stringify(template.config)) as SceneUpsertRequest['config']
  ElMessage.success(`已套用模板：${template.template_name}`)
}

async function submitCreate(): Promise<void> {
  if (!createFormRef.value) {
    return
  }
  const valid = await createFormRef.value.validate().catch(() => false)
  if (!valid) {
    return
  }
  createSubmitting.value = true
  try {
    const scene = await createScene(createForm)
    ElMessage.success('场景创建成功')
    createVisible.value = false
    await loadScenes()
    await router.push({ name: 'scene-detail', params: { scene_id: scene.scene_id } })
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '创建失败')
  } finally {
    createSubmitting.value = false
  }
}

/* ------------------------------ 行操作 ------------------------------ */
async function handleDuplicate(row: Scene): Promise<void> {
  try {
    await duplicateScene(row.scene_id, { new_scene_name: `${row.scene_name}-副本` })
    ElMessage.success('已复制场景')
    await loadScenes()
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '复制失败')
  }
}

async function handlePublish(row: Scene): Promise<void> {
  try {
    await publishScene(row.scene_id, { version: row.version })
    ElMessage.success('场景已发布')
    await loadScenes()
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '发布失败')
  }
}

async function handleDelete(row: Scene): Promise<void> {
  try {
    await ElMessageBox.confirm(`确认删除场景「${row.scene_name}」？该操作不可撤销。`, '危险操作', {
      type: 'warning',
    })
  } catch {
    return
  }
  try {
    await deleteScene(row.scene_id)
    ElMessage.success('场景已删除')
    await loadScenes()
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '删除失败')
  }
}

/** 下发 Carla 仿真（scene:execute） */
async function handleRun(row: Scene): Promise<void> {
  try {
    const data = await runScene(row.scene_id, {})
    ElMessage.success(`仿真已下发（实例 ${data.sim_instance_id}，状态 ${data.status}）`)
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '下发失败')
  }
}

/* ------------------------------ 批量导出 ------------------------------ */
const exportVisible = ref(false)
const exportFormat = ref<SceneExportFormat>('carla_scenariorunner_xml')
const exporting = ref(false)

async function submitExport(): Promise<void> {
  if (selected.value.length === 0) {
    ElMessage.warning('请先勾选需要导出的场景')
    return
  }
  exporting.value = true
  try {
    const data = await exportScenes({
      scene_ids: selected.value.map((item) => item.scene_id),
      format: exportFormat.value,
    })
    downloadByUrl(data.download_url, data.file_name)
    ElMessage.success(`导出成功（${formatBytes(data.size_bytes)}，有效期 ${data.expires_in}s）`)
    exportVisible.value = false
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '导出失败')
  } finally {
    exporting.value = false
  }
}

void loadScenes()
</script>

<template>
  <div class="scene-list">
    <el-card shadow="never">
      <el-form :inline="true" class="filter">
        <el-form-item label="关键词">
          <el-input v-model="query.keyword" clearable placeholder="场景名称" style="width: 180px" @keyup.enter="loadScenes" />
        </el-form-item>
        <el-form-item label="场景类型">
          <el-select v-model="query.scene_type" clearable placeholder="全部类型" style="width: 170px">
            <el-option v-for="option in sceneTypeOptions" :key="option.value" :label="option.label" :value="option.value" />
          </el-select>
        </el-form-item>
        <el-form-item label="状态">
          <el-select v-model="query.status" clearable placeholder="全部状态" style="width: 130px">
            <el-option v-for="option in sceneStatusOptions" :key="option.value" :label="option.label" :value="option.value" />
          </el-select>
        </el-form-item>
        <el-form-item>
          <el-button type="primary" :loading="loading" @click="loadScenes">查询</el-button>
        </el-form-item>
      </el-form>

      <div class="toolbar">
        <el-button v-permission="PERMISSIONS.sceneCreate" type="primary" @click="openCreate">新建场景</el-button>
        <el-button
          v-permission="PERMISSIONS.sceneRead"
          :disabled="selected.length === 0"
          @click="exportVisible = true"
        >
          批量导出（已选 {{ selected.length }}）
        </el-button>
      </div>

      <el-table
        v-loading="loading"
        :data="scenes"
        size="small"
        row-key="scene_id"
        empty-text="暂无场景"
        @selection-change="handleSelectionChange"
      >
        <el-table-column type="selection" width="46" />
        <el-table-column prop="scene_name" label="场景名称" min-width="180" show-overflow-tooltip />
        <el-table-column label="类型" width="130">
          <template #default="{ row }">{{ SCENE_TYPE_LABELS[row.scene_type] ?? row.scene_type }}</template>
        </el-table-column>
        <el-table-column label="状态" width="100">
          <template #default="{ row }">
            <StatusTag kind="scene" :value="row.status" />
          </template>
        </el-table-column>
        <el-table-column prop="version" label="版本" width="90" />
        <el-table-column label="标签" min-width="150">
          <template #default="{ row }">
            <el-tag v-for="tag in row.tags" :key="tag" size="small" effect="plain" class="tag">{{ tag }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="创建者" width="130" prop="creator" />
        <el-table-column label="更新时间" width="170">
          <template #default="{ row }">{{ formatDateTimeString(row.update_time) }}</template>
        </el-table-column>
        <el-table-column label="操作" width="240" fixed="right">
          <template #default="{ row }">
            <el-button
              link
              type="primary"
              @click="router.push({ name: 'scene-detail', params: { scene_id: row.scene_id } })"
            >
              详情
            </el-button>
            <el-button v-permission="PERMISSIONS.sceneCreate" link type="primary" @click="handleDuplicate(row)">复制</el-button>
            <el-button
              v-if="row.status === 'draft'"
              v-permission="PERMISSIONS.sceneExecute"
              link
              type="success"
              @click="handlePublish(row)"
            >
              发布
            </el-button>
            <el-button v-permission="PERMISSIONS.sceneExecute" link type="warning" @click="handleRun(row)">下发仿真</el-button>
            <el-button v-permission="PERMISSIONS.sceneDelete" link type="danger" @click="handleDelete(row)">删除</el-button>
          </template>
        </el-table-column>
      </el-table>

      <el-pagination
        class="pager"
        layout="total, sizes, prev, pager, next"
        :total="total"
        :page-size="query.page_size"
        :page-sizes="[DEFAULT_PAGE_SIZE, 50, 100]"
        @current-change="(page: number) => { query.page = page; loadScenes() }"
        @size-change="(size: number) => { query.page_size = size; query.page = 1; loadScenes() }"
      />
    </el-card>

    <!-- 新建场景（可套用模板） -->
    <el-dialog v-model="createVisible" title="新建场景" width="640px">
      <el-form ref="createFormRef" :model="createForm" :rules="createRules" label-width="110px">
        <el-form-item label="套用模板">
          <el-select placeholder="选择场景模板（可选）" clearable style="width: 100%" @change="applyTemplate">
            <el-option
              v-for="template in templates"
              :key="template.template_id"
              :label="template.template_name"
              :value="template.template_id"
            />
          </el-select>
        </el-form-item>
        <el-form-item label="场景名称" prop="scene_name">
          <el-input v-model="createForm.scene_name" placeholder="如：园区路口行人横穿" maxlength="128" />
        </el-form-item>
        <el-form-item label="场景类型" prop="scene_type">
          <el-select v-model="createForm.scene_type" style="width: 100%">
            <el-option v-for="option in sceneTypeOptions" :key="option.value" :label="option.label" :value="option.value" />
          </el-select>
        </el-form-item>
        <el-form-item label="描述">
          <el-input v-model="createForm.description" type="textarea" :rows="2" maxlength="512" show-word-limit />
        </el-form-item>
        <el-form-item label="标签">
          <el-select v-model="createForm.tags" multiple filterable allow-create default-first-option style="width: 100%" />
        </el-form-item>
        <el-divider content-position="left">关键配置（其余参数可在详情页调整）</el-divider>
        <el-form-item label="地图 ID">
          <el-input v-model="createForm.config.map.map_id" />
        </el-form-item>
        <el-form-item label="场景时长（s）">
          <el-input-number v-model="createForm.config.duration" :min="1" :max="3600" />
        </el-form-item>
        <el-form-item label="自车初始速度">
          <el-input-number v-model="createForm.config.ego_vehicle.initial_speed" :min="0" :max="20" :step="0.5" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="createVisible = false">取消</el-button>
        <el-button type="primary" :loading="createSubmitting" @click="submitCreate">创建并编辑</el-button>
      </template>
    </el-dialog>

    <!-- 批量导出（失败 3001/2001 由服务端返回） -->
    <el-dialog v-model="exportVisible" title="批量导出场景" width="440px">
      <el-form label-width="90px">
        <el-form-item label="导出格式">
          <el-select v-model="exportFormat" style="width: 100%">
            <el-option v-for="option in exportFormatOptions" :key="option.value" :label="option.label" :value="option.value" />
          </el-select>
        </el-form-item>
        <el-form-item label="已选场景">
          <span>{{ selected.length }} 个</span>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="exportVisible = false">取消</el-button>
        <el-button type="primary" :loading="exporting" @click="submitExport">导出并下载</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<style scoped>
.filter {
  margin-bottom: 8px;
}

.toolbar {
  margin-bottom: 12px;
  display: flex;
  gap: 8px;
}

.tag {
  margin-right: 4px;
}

.pager {
  margin-top: 12px;
  text-align: right;
}
</style>

