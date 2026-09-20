# flink-jobs — Flink 1.18 实时分析作业

实时流处理作业（Python DataStream API / PyFlink），消费 Kafka 平台内部 Topic。

**契约（单一事实来源）**：`contracts/openapi/data-analytics.yaml` 的 `x-hunter-realtime-jobs`（6.2 节）。
作业清单、输入 Topic/消费组、阈值与输出通道以契约为准，禁止在作业代码中硬编码阈值（走环境变量）。

| 作业 | 输入（消费组） | 输出 | 说明 | 状态 |
|------|---------------|------|------|------|
| `vehicle_state_monitor` | `telemetry_clean`（`data-analytics-telemetry`） | `alert_event` + `algorithm_metrics` | 6.2.1 心跳检测（>10s → `communication_loss`）+ 状态变更检测 | 占位（后续批次） |
| `driving_anomaly_detection` | `telemetry_clean`（同上） | `alert_event` | 6.2.2 急加速/急减速/急转弯/超速/低电量（阈值 3.0 m/s²、0.5s、0.8 rad/s、1.1 倍、SOC 20%/10%） | **已交付（G-13）**：`hunter_flink/` |
| `algorithm_performance_monitor` | `telemetry_clean`（同上） | `algorithm_metrics` + `analytics_result` | 感知帧率/延迟、规划延迟、控制误差（60s 滚动窗口） | 占位（后续批次） |
| `collision_risk_assessment` | `telemetry_clean`（同上） | `alert_event` + `analytics_result` | 6.2.3 TTC = d / v_rel；<1.5s → `collision_warning`(critical) | 占位（后续批次） |
| `data_quality_monitor` | `telemetry_raw`（`data-analytics-telemetry-raw`） | `analytics_result` | 6.2.5 丢包率/延迟/异常值占比（需与清洗后数据对比） | 占位（后续批次） |

约定：

- 告警等级必须等于受控词表 `EVENT_LEVEL_BY_TYPE[alert_type]`（`contracts/database/enums.md` 第 3 节，不可放宽）
- 消息 key = `vehicle_id`（单车辆有序）；失败消息转 `{topic}.dlq`；消费手动提交 offset
- 实时告警触发延迟 ≤ 2s（`timestamp - event_time`）

## 已交付：`driving_anomaly_detection`（G-13）

包结构（规则核心纯 Python，仓库单测无需 PyFlink 环境）：

```
hunter_flink/
├── thresholds.py     # 契约 14 项阈值（默认=契约基准，全部环境变量化）
├── rules.py          # 6.2.2 规则引擎：episode 语义（每个异常区间只触发一次，恢复后重新武装）
│                     #   · 纵向加速度 = 相邻样本 velocity 差分（telemetry 无 acceleration 字段；dt>1s 视为断流不参与）
│                     #   · over_speed 无限速输入时跳过（telemetry 无 speed_limit 字段，来源见契约 pending #13）
└── detection_job.py  # PyFlink DataStream 壳（telemetry_clean → keyBy(vehicle_id) → flat_map → alert_event）
```

实现语义补充（契约未细化部分）：加速度差分最大间隔 `MAX_ACCEL_INTERVAL_SECONDS=1.0`（非契约阈值）；
battery_low / battery_critical 独立边沿触发（SOC 8% 时两条各一次）。

### 阈值环境变量（14 项，默认 = 契约基准）

`ALERT_COMMUNICATION_LOSS_SECONDS`、`ALERT_TRIGGER_MAX_LATENCY_SECONDS`、`ALERT_HARSH_ACCEL_MS2`、
`ALERT_HARSH_ACCEL_MIN_DURATION_SECONDS`、`ALERT_HARSH_BRAKING_MS2`、`ALERT_HARSH_BRAKING_MIN_DURATION_SECONDS`、
`ALERT_HARSH_TURN_RAD_S`、`ALERT_OVER_SPEED_RATIO`、`ALERT_BATTERY_LOW_SOC`、`ALERT_BATTERY_CRITICAL_SOC`、
`ALERT_COLLISION_TTC_CRITICAL_SECONDS`、`ALERT_COLLISION_TTC_WARNING_SECONDS`、`ALGORITHM_METRICS_WINDOW_SECONDS`、
`ALERT_WINDOW_SECONDS`；另有可选 `ALERT_OVER_SPEED_LIMIT_MPS`（注入固定限速以启用 over_speed 规则，缺省跳过）。
连接参数走 `KAFKA_BOOTSTRAP_SERVERS`（默认 `kafka:9092`）。

### 提交与测试

```bash
# 打包（zip 即作业包；pyflink 由集群侧提供）
cd flink-jobs && zip -r hunter_flink.zip hunter_flink

# compose 单机形态（flink-jobs/ 已只读挂载至 /opt/flink/jobs）
docker compose -f infra/deploy/docker-compose.yml exec flink-jm \
  flink run -d -c hunter_flink.detection_job /opt/flink/jobs/hunter_flink.zip

# 仓库单测（规则核心 + 契约基准比对 + alert_event schema 校验）
python scripts/run_unit_tests.py flink-jobs
```

### 纪律与豁免登记

- **写侧豁免**：本目录不是服务、不在 `services/`（verify-14 不扫描），已在
  `contracts/database/orm-mapping.md` §3.5 白名单表书面登记——作业只经 Kafka 写
  `alert_event`，禁止 import 任何服务 `app.*` 包或 `hunter_common.database.repositories`。
- **部署形态**：compose 单机（flink-jm + 单 flink-tm）为受控降级形态，书面豁免见
  `docs/deployment.md` §12（G-13/G-28 决策①）。
