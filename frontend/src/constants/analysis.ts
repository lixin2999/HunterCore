/**
 * 数据分析相关受控词表与维度定义
 * 来源：contracts/openapi/data-analytics.yaml（ReportType / ReportStatus / ReportFormat /
 * CornerCaseCategory / CornerCaseAlgorithm）+ 6.2/6.4/6.5 节指标口径。
 */

/** 报告类型 */
export const REPORT_TYPE_LABELS: Record<string, string> = {
  vehicle_daily: '车辆日报',
  algorithm_eval: '算法评估',
  scene_test: '场景测试',
  ota_upgrade: 'OTA 升级',
  monthly_operation: '月度运营',
}

/** 报告状态 */
export const REPORT_STATUS_LABELS: Record<string, string> = {
  pending: '排队中',
  generating: '生成中',
  ready: '可下载',
  failed: '生成失败',
}

/** 报告状态 → 标签类型 */
export const REPORT_STATUS_TAG_TYPES: Record<string, string> = {
  pending: 'info',
  generating: 'primary',
  ready: 'success',
  failed: 'danger',
}

/** 报告格式 */
export const REPORT_FORMAT_LABELS: Record<string, string> = {
  html: 'HTML',
  pdf: 'PDF',
  json: 'JSON',
}

/** Corner Case 类别 */
export const CORNER_CASE_CATEGORY_LABELS: Record<string, string> = {
  kinematic: '运动学',
  perception: '感知',
  planning: '规划',
  interaction: '交互',
  environment: '环境',
}

/** Corner Case 挖掘算法 */
export const CORNER_CASE_ALGORITHM_LABELS: Record<string, string> = {
  isolation_forest: '孤立森林',
  dbscan: 'DBSCAN 聚类',
}

/** 看板时间范围（GET /api/v1/analytics/dashboard?time_range=） */
export const DASHBOARD_TIME_RANGES = [
  { value: '1h', label: '近 1 小时' },
  { value: '24h', label: '近 24 小时' },
  { value: '7d', label: '近 7 天' },
  { value: '30d', label: '近 30 天' },
] as const

/** 感知评估指标（PerceptionMetrics 字段 → 中文标签 + 单位） */
export const PERCEPTION_METRIC_LABELS: Record<string, string> = {
  map_3d: '3D 地图精度',
  map_bev: 'BEV 地图精度',
  iou: 'IoU',
  recall: '召回率',
  precision: '准确率',
  mean_localization_error_m: '平均定位误差 (m)',
}

/**
 * 控制评估指标（ControlEvalData.metrics 固定 4 键 —— 来源：data-analytics.yaml 6.3.3 节口径）
 */
export const CONTROL_METRIC_LABELS: Record<string, string> = {
  velocity_rmse_ms: '速度跟踪 RMSE (m/s)',
  steering_rmse_rad: '转向跟踪 RMSE (rad)',
  overshoot_percent: '超调 (%)',
  settling_time_s: '调节时间 (s)',
}

/** 控制评估比较符（ControlThresholdCheck.comparator） */
export const COMPARATOR_LABELS: Record<string, string> = {
  lt: '<',
  lte: '≤',
  gt: '>',
  gte: '≥',
}

/** 遥测对象类型计数键（TelemetryPerception.object_types） */
export const OBJECT_TYPE_LABELS: Record<string, string> = {
  vehicle: '车辆',
  pedestrian: '行人',
  other: '其他',
}
