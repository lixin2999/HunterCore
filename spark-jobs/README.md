# spark-jobs — Spark 3.5 离线分析作业

离线批处理作业（PySpark），从 TimescaleDB / MinIO 读取数据（只读账号 `hunter_analytics_ro`）。

**契约（单一事实来源）**：`contracts/openapi/data-analytics.yaml` 的 `x-hunter-offline-jobs`（6.3/6.4/6.5 节）。
调度（cron）、阈值与产物落位以契约为准（cron 走环境变量 `OFFLINE_JOB_CRON_*`，UTC）。

| 作业 | 调度（cron env） | 产物 |
|------|-----------------|------|
| `daily_report` | `OFFLINE_JOB_CRON_DAILY_REPORT`（每日 00:30） | 车辆运行日报（HTML/PDF/JSON → `hunter-reports`） |
| `perception_eval` | `OFFLINE_JOB_CRON_PERCEPTION_EVAL`（每日 01:00） | 感知精度评估（mAP 3D/BEV、IoU、Recall、Precision、平均定位误差） |
| `planning_quality_eval` | `OFFLINE_JOB_CRON_PERCEPTION_EVAL`（同上批次） | 规划质量（平滑度/偏离度/舒适度），并入算法评估报告 |
| `control_eval` | `OFFLINE_JOB_CRON_CONTROL_EVAL`（每日 01:30） | 控制性能评估 + 6.3.3 阈值判定（RMSE < 0.2 m/s 与 < 0.02 rad、超调 < 10%、调节时间 < 2s） |
| `scene_coverage` | `OFFLINE_JOB_CRON_SCENE_COVERAGE`（每周一 01:00） | 场景覆盖率（热力图 + 未覆盖清单） |
| `corner_case_mining` | `OFFLINE_JOB_CRON_CORNER_CASE_MINING`（每周一 02:00） | Corner Case（Isolation Forest + DBSCAN，5 类异常）→ 逐条投 `analytics_result` |
| `monthly_operation` | `OFFLINE_JOB_CRON_MONTHLY_OPERATION`（每月 1 日 03:00） | 车队月度运营报告 |

约定：

- 报告产物统一落 MinIO `hunter-reports`（永久保留）；下载走 15 分钟预签名（支持 Range）
- 事件/场景/车辆数据一律经服务 REST（data-collector / scene-service / vehicle-service），禁止跨 schema 直查
- 结果消息投 `analytics_result`（`result_type ∈ {metric, corner_case, report}`，key = `vehicle_id`）

> 当前为 L0 骨架占位，作业实现随 data-analytics 模块开发。
