/**
 * 数据采集相关受控词表
 * 来源：contracts/openapi/data-collector.yaml（UploadBucket / FileDataType / UploadMethod）
 *       + 系统约束第 7 条 MinIO Bucket 生命周期。
 */

/** 允许前端发起预签名上传的 Bucket（契约仅暴露 3 个） */
export const UPLOAD_BUCKET_LABELS: Record<string, string> = {
  'hunter-raw-data': '原始传感器数据（30 天）',
  'hunter-rosbag': 'ROS Bag（事件永久 / 常规 30 天）',
  'hunter-video': '视频（90 天）',
}

/** 文件数据类型 */
export const FILE_DATA_TYPE_LABELS: Record<string, string> = {
  point_cloud: '点云',
  camera_image: '相机图像',
  rosbag: 'ROS Bag',
  video: '视频',
  other: '其他',
}

/** 上传方式（FilePresignData.method） */
export const UPLOAD_METHOD_LABELS: Record<string, string> = {
  put: '单次 PUT',
  multipart: '分片上传',
}

/** 遥测排序字段（GET /api/v1/data/telemetry?sort=） */
export const TELEMETRY_SORT_OPTIONS = [
  { value: 'time', label: '时间' },
  { value: 'battery_soc', label: '电量' },
] as const

/** 事件列表默认分页大小（≤ 200，与 BaseRepository.paginate 一致） */
export const DEFAULT_PAGE_SIZE = 20

/** 分页大小上限（契约 PageSizeQuery.maximum = 200） */
export const MAX_PAGE_SIZE = 200
