/**
 * 数据分析（data-analytics）类型（一）：报告 + 看板
 * 来源：contracts/openapi/data-analytics.yaml —— 字段名与契约完全一致（snake_case）。
 * 说明：各项 `available` 标记表示 Flink/Spark 结果不可用时降级返回（data_source 标识来源）。
 */

/** 报告类型（5 类标准模板，不可新增） */
export type ReportType =
  | 'vehicle_daily'
  | 'algorithm_eval'
  | 'scene_test'
  | 'ota_upgrade'
  | 'monthly_operation'

/** 报告格式 */
export type ReportFormat = 'html' | 'pdf' | 'json'

/** 报告状态（异步生成流程） */
export type ReportStatus = 'pending' | 'generating' | 'ready' | 'failed'

/** 报告产物 */
export interface ReportArtifact {
  format: ReportFormat
  object_key: string
  size_bytes: number
  /** 预签名下载 URL（按需下发） */
  download_url?: string | null
  expires_in?: number | null
}

/** 报告元数据 */
export interface ReportMeta {
  report_id: string
  report_type: ReportType
  status: ReportStatus
  vehicle_id?: string | null
  start_time?: number
  end_time?: number
  formats: ReportFormat[]
  created_at: number
  completed_at?: number | null
  generated_by?: string | null
  trigger?: 'manual' | 'schedule'
  artifacts?: ReportArtifact[]
  summary?: Record<string, unknown>
}

/** 报告列表响应数据 */
export interface ReportListData {
  items: ReportMeta[]
  total: number
  page: number
  page_size: number
}

/** 报告列表查询参数 */
export interface ReportListQuery {
  report_type?: ReportType
  vehicle_id?: string
  status?: ReportStatus
  start_time?: number
  end_time?: number
  page?: number
  page_size?: number
}

/** 报告生成请求（POST /api/v1/analytics/reports/generate，返回 202） */
export interface ReportGenerateRequest {
  report_type: ReportType
  vehicle_id?: string | null
  start_time: number
  end_time: number
  formats?: ReportFormat[]
  /** true = 忽略缓存强制重算 */
  force?: boolean
}

/** 报告生成受理结果（轮询 poll_url 或按 report_id 查询） */
export interface ReportGenerateData {
  report_id: string
  report_type: ReportType
  status: ReportStatus
  created_at: number
  estimated_ready_seconds?: number | null
  poll_url?: string
}

/** 车队总览（不可用时 available=false + reason） */
export interface FleetOverview {
  available: boolean
  reason?: string | null
  total_vehicles: number
  online_vehicles: number
  /** 按车辆状态分组计数（8 态受控词表） */
  by_status?: Record<string, number>
}

/** 数据管道指标 */
export interface PipelineStats {
  available: boolean
  reason?: string | null
  telemetry_points?: number
  /** 入库延迟 P95（SLO ≤ 1s） */
  ingest_latency_ms_p95?: number | null
  /** Kafka consumer lag（按 topic/partition） */
  kafka_consumer_lag?: Record<string, unknown>
  dlq_messages?: Record<string, unknown>
}

/** 算法健康指标 */
export interface AlgorithmHealth {
  available: boolean
  reason?: string | null
  perception_fps_avg?: number | null
  perception_latency_ms_avg?: number | null
  planning_latency_ms_avg?: number | null
  control_latency_ms_avg?: number | null
  metrics?: Record<string, unknown>
}

/** 事件统计 */
export interface EventStats {
  available: boolean
  reason?: string | null
  total: number
  info?: number
  warning?: number
  critical?: number
  unacknowledged?: number
  by_type?: Record<string, number>
}

/** 看板聚合数据（GET /api/v1/analytics/dashboard） */
export interface DashboardData {
  time_range: '1h' | '24h' | '7d' | '30d'
  vehicle_id?: string | null
  generated_at: number
  fleet: FleetOverview
  pipeline: PipelineStats
  algorithm: AlgorithmHealth
  events: EventStats
  /** 降级维度列表（如 ['pipeline']） */
  degraded?: string[]
}

/**
 * 可降级/可溯源数据块（各评估响应共有字段，契约 required: data_source, updated_at）
 * 注意：评估类数据块**没有** available/reason 字段（仅 dashboard 的各分块有），
 * 不可用时服务端返回业务错误码（如 5001/3001），前端按异常处理，不得臆造字段。
 */
export interface DegradableData {
  /** 数据来源描述（如 "algorithm_metrics(module=control) + vehicle_telemetry 只读聚合"） */
  data_source: string
  /** 产出该结果的离线作业名（见 x-hunter-offline-jobs） */
  job_name?: string
  /** 结果生成时间（Unix epoch 秒）—— 前端据此提示数据新鲜度 */
  updated_at: number
  /** 关联报告 ID（hunter-reports，可经 GET /reports/{id} 下载） */
  report_id?: string | null
}

/** 评估窗口 */
export interface EvalWindow {
  start_time: number
  end_time: number
  duration_hours?: number
}

/** 感知评估指标（6.2 节口径） */
export interface PerceptionMetrics {
  map_3d: number
  map_bev: number
  iou: number
  recall: number
  precision: number
  mean_localization_error_m: number
}

/** 感知评估数据（by_object_type：键 ∈ vehicle/pedestrian/other → 同构指标） */
export interface PerceptionEvalData extends DegradableData {
  vehicle_id?: string | null
  window: EvalWindow
  sample_count: number
  metrics: PerceptionMetrics
  by_object_type?: Record<string, PerceptionMetrics>
}

/** 控制性能指标（6.3.3 节口径：4 项固定键，契约 required 不可增删） */
export interface ControlMetrics {
  /** 速度跟踪 RMSE（m/s） */
  velocity_rmse_ms: number
  /** 转向跟踪 RMSE（rad） */
  steering_rmse_rad: number
  /** 超调（%） */
  overshoot_percent: number
  /** 调节时间（s） */
  settling_time_s: number
}

/** 控制阈值检查项（6.3 节：指标 + 阈值 + 比较符 + 是否通过） */
export interface ControlThresholdCheck {
  value: number
  threshold: number
  comparator: 'lt' | 'lte' | 'gt' | 'gte'
  unit?: string
  pass: boolean
}

/** 控制阈值判定集（键与 ControlMetrics 一一对应） */
export type ControlThresholdSet = Record<keyof ControlMetrics, ControlThresholdCheck>

/** 控制评估数据 */
export interface ControlEvalData extends DegradableData {
  vehicle_id?: string | null
  window: EvalWindow
  sample_count: number
  metrics: ControlMetrics
  thresholds: ControlThresholdSet
  /** 全部指标是否达标（任一项不达标为 false） */
  overall_pass: boolean
}

/** 覆盖率热力图单元（网格中心坐标 + 采样计数） */
export interface CoverageCell {
  x: number
  y: number
  count: number
}

/** 未覆盖区域（region 区域 / scene_type 场景类型） */
export interface UncoveredArea {
  kind: 'region' | 'scene_type'
  description: string
  reason?: string | null
  sample_count?: number
}

/** 场景覆盖率数据（6.4 节） */
export interface SceneCoverageData extends DegradableData {
  vehicle_id?: string | null
  window: EvalWindow
  grid_size_m: number
  covered_cells: number
  total_cells: number
  coverage_ratio: number
  heatmap: CoverageCell[]
  /** heatmap 超过 max_cells 被截断 */
  heatmap_truncated?: boolean
  uncovered: UncoveredArea[]
  /** 各场景类型覆盖率（键 = 场景类型，取 scene-service 场景库维度） */
  scene_type_coverage?: Record<string, number>
}

/** Corner Case 类别（5 类，不可新增） */
export type CornerCaseCategory =
  | 'kinematic'
  | 'perception'
  | 'planning'
  | 'interaction'
  | 'environment'

/** 挖掘算法（Isolation Forest 无监督异常检测 + DBSCAN 密度聚类，可组合） */
export type CornerCaseAlgorithm = 'isolation_forest' | 'dbscan'

/** Corner Case 明细 */
export interface CornerCaseItem {
  corner_case_id: string
  category: CornerCaseCategory
  vehicle_id: string
  event_time: number
  window_start?: number
  window_end?: number
  anomaly_score: number
  algorithm: CornerCaseAlgorithm
  cluster_id?: number | null
  trigger_event_type?: string | null
  description?: string | null
  /** 异常时刻特征值（如 min_ttc / max_lateral_accel / perception_latency_ms） */
  metrics?: Record<string, number>
  data_file_url?: string | null
  scene_id?: string | null
}

/** 挖掘任务元信息 */
export interface CornerCaseMiningMeta {
  algorithm: CornerCaseAlgorithm[]
  last_run_at: number
  job_name?: string
  window?: EvalWindow
  feature_count?: number
  total_found?: number
}

/** Corner Case 列表响应数据 */
export interface CornerCaseListData {
  items: CornerCaseItem[]
  total: number
  page: number
  page_size: number
  mining: CornerCaseMiningMeta
  category_counts?: Record<string, number>
}

/** Corner Case 查询参数 */
export interface CornerCaseQuery {
  category?: CornerCaseCategory
  vehicle_id?: string
  algorithm?: CornerCaseAlgorithm
  min_anomaly_score?: number
  start_time?: number
  end_time?: number
  page?: number
  page_size?: number
}

/** 时间窗口查询参数（感知/控制评估共用） */
export interface EvalQuery {
  vehicle_id?: string
  start_time?: number
  end_time?: number
}

/** 场景覆盖查询参数（额外支持网格粒度与单元上限） */
export interface SceneCoverageQuery extends EvalQuery {
  grid_size_m?: number
  max_cells?: number
}

