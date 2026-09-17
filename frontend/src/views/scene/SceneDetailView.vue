<script setup lang="ts">
/**
 * 场景详情（编辑 / 预览 / 发布 / 导出 / 下发仿真）
 *
 * 契约（scene-service.yaml）：
 * - GET  /api/v1/scene/{scene_id}
 * - PUT  /api/v1/scene/{scene_id}（仅 draft 可改；状态冲突 3003）
 * - POST /api/v1/scene/{scene_id}/publish
 * - POST /api/v1/scene/export（单场景导出复用批量端点）
 * - POST /api/v1/scene/{scene_id}/run
 * 说明：参数字段与 config_json 结构严格对齐 SceneConfig（字段名不可更改）。
 */
import { computed, reactive, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'

import { exportScenes, getScene, publishScene, runScene, updateScene } from '@/api/scene'
import ScenePreview from '@/components/three/ScenePreview.vue'
import {
  ACTOR_TYPE_LABELS,
  MAP_TYPE_LABELS,
  SCENE_EVENT_TYPE_LABELS,
  SCENE_EXPORT_FORMAT_LABELS,
  SCENE_TYPE_LABELS,
} from '@/constants'
import { PERMISSIONS } from '@/constants/permissions'
import type { Scene, SceneConfig, SceneUpsertRequest } from '@/types/scene'
import { HunterApiError } from '@/utils/error-code'
import { downloadByUrl } from '@/utils/hash'
import { formatBytes, formatDateTimeString } from '@/utils/format'

const route = useRoute()
const router = useRouter()

const sceneId = computed<string>(() => String(route.params.scene_id ?? ''))
const loading = ref(false)
const saving = ref(false)
const scene = ref<Scene | null>(null)
/** 是否可编辑：状态为 draft（契约：发布后只读，修改需复制为新草稿） */
const editable = computed<boolean>(() => scene.value?.status === 'draft')

const form = reactive<SceneUpsertRequest>({
  scene_name: '',
  scene_type: 'straight_cruise',
  description: '',
  tags: [],
  config: {
    map: { map_id: '', map_type: 'carla_town', spawn_point: { x: 0, y: 0, z: 0, yaw: 0 } },
    ego_vehicle: { model: 'HUNTER_SE', initial_speed: 0, initial_steer: 0 },
    weather: { cloudiness: 0, rain: 0, wetness: 0, fog: 0, wind: 0, sun_azimuth: 0, sun_altitude: 45 },
    actors: [],
    events: [],
    success_criteria: { max_speed_deviation: 0.5, no_collision: true, min_safe_distance: 2 },
    duration: 60,
  },
})

/** 3D 预览用配置（保存前即预览当前表单） */
const previewConfig = computed<SceneConfig>(() => form.config)

const actorTypeOptions = Object.entries(ACTOR_TYPE_LABELS).map(([value, label]) => ({ value, label }))
const mapTypeOptions = Object.entries(MAP_TYPE_LABELS).map(([value, label]) => ({ value, label }))
const sceneTypeOptions = Object.entries(SCENE_TYPE_LABELS).map(([value, label]) => ({ value, label }))
const eventTypeOptions = Object.entries(SCENE_EVENT_TYPE_LABELS).map(([value, label]) => ({ value, label }))
const exportFormatOptions = Object.entries(SCENE_EXPORT_FORMAT_LABELS).map(([value, label]) => ({ value, label }))

function syncFormFromScene(source: Scene): void {
  form.scene_name = source.scene_name
  form.scene_type = source.scene_type
  form.description = source.description ?? ''
  form.tags = [...source.tags]
  if (source.config) {
    form.config = JSON.parse(JSON.stringify(source.config)) as SceneConfig
  }
}

async function loadScene(): Promise<void> {
  if (!sceneId.value) {
    return
  }
  loading.value = true
  try {
    const data = await getScene(sceneId.value)
    scene.value = data
    syncFormFromScene(data)
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '场景加载失败')
  } finally {
    loading.value = false
  }
}

/* --------------------------- 参与者 / 事件编辑 --------------------------- */
function addActor(): void {
  const index = form.config.actors.length + 1
  form.config.actors.push({
    actor_id: `actor_${index}`,
    type: 'vehicle',
    spawn_point: { x: 10, y: 0, z: 0, yaw: 0 },
    behavior: { behavior_type: 'cruise', parameters: {} },
  })
}

function removeActor(index: number): void {
  form.config.actors.splice(index, 1)
}

function addEvent(): void {
  const index = form.config.events.length + 1
  form.config.events.push({
    event_id: `event_${index}`,
    type: 'cut_in',
    trigger: { condition: 'distance', parameters: { distance_m: 20 } },
    action: { action_type: 'lane_change', parameters: {} },
  })
}

function removeEvent(index: number): void {
  form.config.events.splice(index, 1)
}

/* ------------------------------- 保存 / 发布 ------------------------------- */
async function handleSave(): Promise<void> {
  saving.value = true
  try {
    const data = await updateScene(sceneId.value, form)
    scene.value = data
    ElMessage.success('场景已保存')
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '保存失败')
  } finally {
    saving.value = false
  }
}

async function handlePublish(): Promise<void> {
  try {
    const data = await publishScene(sceneId.value, { version: scene.value?.version ?? null })
    scene.value = data
    ElMessage.success('场景已发布（发布后需复制为新草稿再修改）')
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '发布失败')
  }
}

/** 导出当前场景（Carla ScenarioRunner XML / OpenSCENARIO 1.2） */
async function handleExport(format: string): Promise<void> {
  try {
    const data = await exportScenes({ scene_ids: [sceneId.value], format: format as never })
    downloadByUrl(data.download_url, data.file_name)
    ElMessage.success(`导出成功（${formatBytes(data.size_bytes)}）`)
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '导出失败')
  }
}

/** 下发 Carla 仿真（scene:execute） */
async function handleRun(): Promise<void> {
  try {
    const data = await runScene(sceneId.value, {})
    ElMessage.success(`已下发仿真：实例 ${data.sim_instance_id}（${data.status}）`)
  } catch (error) {
    ElMessage.error(error instanceof HunterApiError ? error.message : '下发失败')
  }
}

void loadScene()
</script>

<template>
  <div v-loading="loading" class="scene-detail">
    <el-card shadow="never">
      <template #header>
        <div class="detail__header">
          <span>{{ form.scene_name || '场景详情' }}</span>
          <div class="detail__actions">
            <el-tag v-if="scene" :type="editable ? 'info' : 'success'" size="small">
              {{ editable ? '草稿（可编辑）' : '已发布（只读）' }}
            </el-tag>
            <el-button link @click="router.push({ name: 'scenes' })">返回列表</el-button>
            <el-button
              v-permission="PERMISSIONS.sceneUpdate"
              type="primary"
              :disabled="!editable"
              :loading="saving"
              @click="handleSave"
            >
              保存
            </el-button>
            <el-button
              v-permission="PERMISSIONS.sceneExecute"
              type="success"
              :disabled="editable"
              @click="handlePublish"
            >
              发布
            </el-button>
            <el-button v-permission="PERMISSIONS.sceneExecute" type="warning" @click="handleRun">下发 Carla 仿真</el-button>
            <el-dropdown v-permission="PERMISSIONS.sceneRead" @command="handleExport">
              <el-button>导出<el-icon><ArrowDown /></el-icon></el-button>
              <template #dropdown>
                <el-dropdown-menu>
                  <el-dropdown-item v-for="option in exportFormatOptions" :key="option.value" :command="option.value">
                    {{ option.label }}
                  </el-dropdown-item>
                </el-dropdown-menu>
              </template>
            </el-dropdown>
          </div>
        </div>
      </template>

      <el-descriptions v-if="scene" :column="3" size="small" border>
        <el-descriptions-item label="场景 ID">{{ scene.scene_id }}</el-descriptions-item>
        <el-descriptions-item label="版本">{{ scene.version }}</el-descriptions-item>
        <el-descriptions-item label="创建者">{{ scene.creator }}</el-descriptions-item>
        <el-descriptions-item label="创建时间">{{ formatDateTimeString(scene.create_time) }}</el-descriptions-item>
        <el-descriptions-item label="更新时间">{{ formatDateTimeString(scene.update_time) }}</el-descriptions-item>
        <el-descriptions-item label="标签">{{ scene.tags.join(', ') || '-' }}</el-descriptions-item>
      </el-descriptions>
    </el-card>

    <el-row :gutter="12" class="scene-detail__body">
      <el-col :span="14">
        <el-card shadow="never" header="3D 预览（Three.js）">
          <ScenePreview :config="previewConfig" height="420px" />
          <p class="hint">绿色为自车，蓝色/橙色/灰色分别为车辆/行人/其他参与者；坐标系为 Carla 右手系（x 前、y 左、z 上）。</p>
        </el-card>
      </el-col>

      <el-col :span="10">
        <el-card shadow="never" header="基础信息">
          <el-form :model="form" label-width="110px" :disabled="!editable">
            <el-form-item label="场景名称">
              <el-input v-model="form.scene_name" maxlength="128" />
            </el-form-item>
            <el-form-item label="场景类型">
              <el-select v-model="form.scene_type" style="width: 100%">
                <el-option v-for="option in sceneTypeOptions" :key="option.value" :label="option.label" :value="option.value" />
              </el-select>
            </el-form-item>
            <el-form-item label="描述">
              <el-input v-model="form.description" type="textarea" :rows="2" maxlength="512" show-word-limit />
            </el-form-item>
            <el-form-item label="标签">
              <el-select v-model="form.tags" multiple filterable allow-create default-first-option style="width: 100%" />
            </el-form-item>
            <el-form-item label="场景时长（s）">
              <el-input-number v-model="form.config.duration" :min="1" :max="3600" />
            </el-form-item>
          </el-form>
        </el-card>
      </el-col>
    </el-row>

    <el-card shadow="never" header="场景参数（config_json）" class="scene-detail__card">
      <el-form :model="form.config" label-width="130px" :disabled="!editable">
        <el-divider content-position="left">地图与自车</el-divider>
        <el-row :gutter="12">
          <el-col :span="8">
            <el-form-item label="地图 ID">
              <el-input v-model="form.config.map.map_id" placeholder="Carla 地图名 / 高精地图 ID" />
            </el-form-item>
          </el-col>
          <el-col :span="8">
            <el-form-item label="地图类型">
              <el-select v-model="form.config.map.map_type" style="width: 100%">
                <el-option v-for="option in mapTypeOptions" :key="option.value" :label="option.label" :value="option.value" />
              </el-select>
            </el-form-item>
          </el-col>
          <el-col :span="8">
            <el-form-item label="自车模型">
              <el-input v-model="form.config.ego_vehicle.model" />
            </el-form-item>
          </el-col>
        </el-row>
        <el-row :gutter="12">
          <el-col :span="6">
            <el-form-item label="生成点 x (m)">
              <el-input-number v-model="form.config.map.spawn_point.x" :step="1" />
            </el-form-item>
          </el-col>
          <el-col :span="6">
            <el-form-item label="生成点 y (m)">
              <el-input-number v-model="form.config.map.spawn_point.y" :step="1" />
            </el-form-item>
          </el-col>
          <el-col :span="6">
            <el-form-item label="朝向 yaw (rad)">
              <el-input-number v-model="form.config.map.spawn_point.yaw" :min="-3.1416" :max="3.1416" :step="0.1" />
            </el-form-item>
          </el-col>
          <el-col :span="6">
            <el-form-item label="初始速度 (m/s)">
              <el-input-number v-model="form.config.ego_vehicle.initial_speed" :min="0" :max="20" :step="0.5" />
            </el-form-item>
          </el-col>
        </el-row>

        <el-divider content-position="left">天气（Carla 参数，0–1 归一化；sun_* 为角度）</el-divider>
        <el-row :gutter="12">
          <el-col :span="4">
            <el-form-item label="云量"><el-input-number v-model="form.config.weather.cloudiness" :min="0" :max="1" :step="0.1" /></el-form-item>
          </el-col>
          <el-col :span="4">
            <el-form-item label="降雨"><el-input-number v-model="form.config.weather.rain" :min="0" :max="1" :step="0.1" /></el-form-item>
          </el-col>
          <el-col :span="4">
            <el-form-item label="湿度"><el-input-number v-model="form.config.weather.wetness" :min="0" :max="1" :step="0.1" /></el-form-item>
          </el-col>
          <el-col :span="4">
            <el-form-item label="雾"><el-input-number v-model="form.config.weather.fog" :min="0" :max="1" :step="0.1" /></el-form-item>
          </el-col>
          <el-col :span="4">
            <el-form-item label="风"><el-input-number v-model="form.config.weather.wind" :min="0" :max="1" :step="0.1" /></el-form-item>
          </el-col>
          <el-col :span="4">
            <el-form-item label="太阳方位角"><el-input-number v-model="form.config.weather.sun_azimuth" :min="0" :max="360" /></el-form-item>
          </el-col>
        </el-row>

        <el-divider content-position="left">成功判据</el-divider>
        <el-row :gutter="12">
          <el-col :span="8">
            <el-form-item label="速度偏差上限 (m/s)">
              <el-input-number v-model="form.config.success_criteria.max_speed_deviation" :min="0" :step="0.1" />
            </el-form-item>
          </el-col>
          <el-col :span="8">
            <el-form-item label="最小安全距离 (m)">
              <el-input-number v-model="form.config.success_criteria.min_safe_distance" :min="0" :step="0.5" />
            </el-form-item>
          </el-col>
          <el-col :span="8">
            <el-form-item label="禁止碰撞">
              <el-switch v-model="form.config.success_criteria.no_collision" />
            </el-form-item>
          </el-col>
        </el-row>
      </el-form>
    </el-card>

    <el-row :gutter="12">
      <el-col :span="12">
        <el-card shadow="never" header="参与者（actors）">
          <template #header>
            <div class="detail__header">
              <span>参与者（actors）</span>
              <el-button v-permission="PERMISSIONS.sceneUpdate" size="small" :disabled="!editable" @click="addActor">
                添加参与者
              </el-button>
            </div>
          </template>
          <el-table :data="form.config.actors" size="small" empty-text="暂无参与者">
            <el-table-column label="ID" width="120">
              <template #default="{ row }">
                <el-input v-model="row.actor_id" :disabled="!editable" size="small" />
              </template>
            </el-table-column>
            <el-table-column label="类型" width="100">
              <template #default="{ row }">
                <el-select v-model="row.type" :disabled="!editable" size="small">
                  <el-option v-for="option in actorTypeOptions" :key="option.value" :label="option.label" :value="option.value" />
                </el-select>
              </template>
            </el-table-column>
            <el-table-column label="x" width="90">
              <template #default="{ row }">
                <el-input-number v-model="row.spawn_point.x" :disabled="!editable" :controls="false" size="small" />
              </template>
            </el-table-column>
            <el-table-column label="y" width="90">
              <template #default="{ row }">
                <el-input-number v-model="row.spawn_point.y" :disabled="!editable" :controls="false" size="small" />
              </template>
            </el-table-column>
            <el-table-column label="yaw" width="90">
              <template #default="{ row }">
                <el-input-number v-model="row.spawn_point.yaw" :disabled="!editable" :controls="false" :step="0.1" size="small" />
              </template>
            </el-table-column>
            <el-table-column label="行为类型" min-width="130">
              <template #default="{ row }">
                <el-input v-model="row.behavior.behavior_type" :disabled="!editable" size="small" placeholder="如 cruise / cut_in" />
              </template>
            </el-table-column>
            <el-table-column label="操作" width="70" fixed="right">
              <template #default="{ $index }">
                <el-button v-permission="PERMISSIONS.sceneUpdate" link type="danger" :disabled="!editable" @click="removeActor($index)">
                  删除
                </el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-card>
      </el-col>

      <el-col :span="12">
        <el-card shadow="never">
          <template #header>
            <div class="detail__header">
              <span>场景事件（events）</span>
              <el-button v-permission="PERMISSIONS.sceneUpdate" size="small" :disabled="!editable" @click="addEvent">
                添加事件
              </el-button>
            </div>
          </template>
          <el-table :data="form.config.events" size="small" empty-text="暂无事件">
            <el-table-column label="ID" width="120">
              <template #default="{ row }">
                <el-input v-model="row.event_id" :disabled="!editable" size="small" />
              </template>
            </el-table-column>
            <el-table-column label="事件类型" width="150">
              <template #default="{ row }">
                <el-select v-model="row.type" :disabled="!editable" size="small">
                  <el-option v-for="option in eventTypeOptions" :key="option.value" :label="option.label" :value="option.value" />
                </el-select>
              </template>
            </el-table-column>
            <el-table-column label="触发条件" min-width="140">
              <template #default="{ row }">
                <el-input v-model="row.trigger.condition" :disabled="!editable" size="small" placeholder="条件标识（取值域待契约确认）" />
              </template>
            </el-table-column>
            <el-table-column label="动作类型" min-width="140">
              <template #default="{ row }">
                <el-input v-model="row.action.action_type" :disabled="!editable" size="small" placeholder="动作标识（取值域待契约确认）" />
              </template>
            </el-table-column>
            <el-table-column label="操作" width="70" fixed="right">
              <template #default="{ $index }">
                <el-button v-permission="PERMISSIONS.sceneUpdate" link type="danger" :disabled="!editable" @click="removeEvent($index)">
                  删除
                </el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-card>
      </el-col>
    </el-row>
  </div>
</template>

<style scoped>
.detail__header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.detail__actions {
  display: flex;
  align-items: center;
  gap: 8px;
}

.scene-detail__body,
.scene-detail__card {
  margin-top: 12px;
}

.hint {
  margin: 8px 0 0;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}
</style>

