# flink-jobs — Flink 1.18 实时分析作业

实时流处理作业（Python DataStream API / PyFlink），消费 Kafka 平台内部 Topic。

**契约（单一事实来源）**：`contracts/openapi/data-analytics.yaml` 的 `x-hunter-realtime-jobs`（6.2 节）。
作业清单、输入 Topic/消费组、阈值与输出通道以契约为准，禁止在作业代码中硬编码阈值（走环境变量）。

| 作业 | 输入（消费组） | 输出 | 说明 |
|------|---------------|------|------|
| `vehicle_state_monitor` | `telemetry_clean`（`data-analytics-telemetry`） | `alert_event` + `algorithm_metrics` | 6.2.1 心跳检测（>10s → `communication_loss`）+ 状态变更检测 |
| `driving_anomaly_detection` | `telemetry_clean`（同上） | `alert_event` | 6.2.2 急加速/急减速/急转弯/超速/低电量（阈值 3.0 m/s²、0.5s、0.8 rad/s、1.1 倍、SOC 20%/10%） |
| `algorithm_performance_monitor` | `telemetry_clean`（同上） | `algorithm_metrics` + `analytics_result` | 感知帧率/延迟、规划延迟、控制误差（60s 滚动窗口） |
| `collision_risk_assessment` | `telemetry_clean`（同上） | `alert_event` + `analytics_result` | 6.2.3 TTC = d / v_rel；<1.5s → `collision_warning`(critical) |
| `data_quality_monitor` | `telemetry_raw`（`data-analytics-telemetry-raw`） | `analytics_result` | 6.2.5 丢包率/延迟/异常值占比（需与清洗后数据对比） |

约定：

- 告警等级必须等于受控词表 `EVENT_LEVEL_BY_TYPE[alert_type]`（`contracts/database/enums.md` 第 3 节，不可放宽）
- 消息 key = `vehicle_id`（单车辆有序）；失败消息转 `{topic}.dlq`；消费手动提交 offset
- 实时告警触发延迟 ≤ 2s（`timestamp - event_time`）

> 当前为 L0 骨架占位，作业实现随 data-analytics 模块开发（Step 5）。
