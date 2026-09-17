/**
 * 场景生成 API（scene-service）
 * 契约：contracts/openapi/scene-service.yaml
 * 注意：场景 CRUD 属于写操作，前端不做乐观更新，以服务端返回实体为准。
 */
import { request } from './request'
import type {
  Scene,
  SceneDeleteData,
  SceneDuplicateRequest,
  SceneExportData,
  SceneExportRequest,
  SceneListData,
  SceneListQuery,
  ScenePublishRequest,
  SceneRunData,
  SceneRunRequest,
  SceneTemplateCategory,
  SceneTemplateListData,
  SceneType,
  SceneUpsertRequest,
} from '@/types/scene'

/** 场景列表（GET /api/v1/scene） */
export function listScenes(params: SceneListQuery): Promise<SceneListData> {
  return request<SceneListData>({ url: '/scene', method: 'get', params })
}

/** 场景详情（GET /api/v1/scene/{scene_id}） */
export function getScene(sceneId: string): Promise<Scene> {
  return request<Scene>({ url: `/scene/${sceneId}`, method: 'get' })
}

/** 新建场景（POST /api/v1/scene，201；唯一键冲突 3002） */
export function createScene(payload: SceneUpsertRequest): Promise<Scene> {
  return request<Scene>({ url: '/scene', method: 'post', data: payload })
}

/** 更新场景（PUT /api/v1/scene/{scene_id}） */
export function updateScene(sceneId: string, payload: SceneUpsertRequest): Promise<Scene> {
  return request<Scene>({ url: `/scene/${sceneId}`, method: 'put', data: payload })
}

/** 删除场景（DELETE /api/v1/scene/{scene_id}） */
export function deleteScene(sceneId: string): Promise<SceneDeleteData> {
  return request<SceneDeleteData>({ url: `/scene/${sceneId}`, method: 'delete' })
}

/** 复制场景（POST /api/v1/scene/{scene_id}/duplicate，201） */
export function duplicateScene(sceneId: string, payload: SceneDuplicateRequest = {}): Promise<Scene> {
  return request<Scene>({ url: `/scene/${sceneId}/duplicate`, method: 'post', data: payload })
}

/** 发布场景（POST /api/v1/scene/{scene_id}/publish） */
export function publishScene(sceneId: string, payload: ScenePublishRequest = {}): Promise<Scene> {
  return request<Scene>({ url: `/scene/${sceneId}/publish`, method: 'post', data: payload })
}

/** 场景模板列表（GET /api/v1/scene/templates） */
export function listSceneTemplates(params: {
  category?: SceneTemplateCategory
  scene_type?: SceneType
} = {}): Promise<SceneTemplateListData> {
  return request<SceneTemplateListData>({ url: '/scene/templates', method: 'get', params })
}

/** 导出场景（POST /api/v1/scene/export，返回预签名下载 URL） */
export function exportScenes(payload: SceneExportRequest): Promise<SceneExportData> {
  return request<SceneExportData>({ url: '/scene/export', method: 'post', data: payload })
}

/** 下发到 Carla 仿真（POST /api/v1/scene/{scene_id}/run） */
export function runScene(sceneId: string, payload: SceneRunRequest = {}): Promise<SceneRunData> {
  return request<SceneRunData>({ url: `/scene/${sceneId}/run`, method: 'post', data: payload })
}
