# flink-jobs — Flink 2.1 实时分析作业

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
# 本地单测（规则核心 + 契约基准比对 + alert_event schema 校验）
python scripts/run_unit_tests.py flink-jobs

# 仓库根构建作业镜像（compose 与 K8s 同一镜像，V1.19.2 起 compose 也已切换）
# ⚠ 标签与镜像内 Python 必须与基座 Flink 同版本（V1.19.5 钉 Flink 2.1.1 + CPython 3.12）：
#   PyPI 上 apache-flink 1.18.1 只发 cp37–cp310、1.20.x 到 cp311、2.1.x 才发 cp312，
#   而 hunter_common 下限 3.11（enum.StrEnum）——升级 Flink 才能把作业对齐到服务基线 3.12。
docker build -f flink-jobs/Dockerfile -t hunter-flink/jobs:2.1.1 .
# ⚠ 构建会额外从 Maven Central 拉 DataStream Kafka 连接器 jar（官方基座与 apache-flink wheel 都不带它，
#   缺则提交后报 ClassNotFoundException: ...connector.kafka.source.KafkaSource）：Flink 2.1 对应
#   `flink-sql-connector-kafka-5.0.0-2.1.jar`（命名是 `<连接器版本>-<Flink 版本>`）。镜像源覆盖：
#   docker build -f flink-jobs/Dockerfile --build-arg KAFKA_CONNECTOR_URL=<同路径镜像地址> -t … .

# compose 单机形态（生产机用 sudo，.env 为 600/root）：镜像内置 hunter_flink 与 hunter_common，
# 提交走 -pym（官方纯 JVM 镜像无 Python，必报 `Cannot run program "python"`）
docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build flink-jm flink-tm
docker compose -f infra/deploy/docker-compose.yml --project-directory . exec flink-jm \
  flink run -d -m localhost:8081 -pym hunter_flink.detection_job
# …… algorithm_performance_job / ingest_latency_job 同式各一行
```

### 纪律与豁免登记

- **写侧豁免**：本目录不是服务、不在 `services/`（verify-14 不扫描），已在
  `contracts/database/orm-mapping.md` §3.5 白名单表书面登记——作业只经 Kafka 写
  `alert_event`，禁止 import 任何服务 `app.*` 包或 `hunter_common.database.repositories`。
- **CI lint**：本目录已纳入流水线 lint 阶段（`.gitlab-ci.yml` → `ruff check tests common/python
  services flink-jobs`）；作业代码须与其余模块同守 ruff 规则（导入分组：第三方 `hunter_common`
  与第一方 `hunter_flink` 分块）。
- **部署形态**（两种并存，按环境择一）：
  1. **K8s 自建集群（推荐生产）**：`infra/k8s/flink/`（`01-configmap` + `02-jobmanager`
     Deployment/Service + `03-taskmanager` Deployment + `04-submit-job` batch/v1）+ `hunter-flink/jobs:2.1.1`
     **代码内置镜像**（`flink-jobs/Dockerfile`）。提交 Job 经 `flink run -pym hunter_flink.<job>` 拉起三个
     已交付流作业；`data-analytics` 的 `FLINK_JOBMANAGER_URL` 即指向此处 `flink-jobmanager:8081`。
     入向放行见 `infra/k8s/networkpolicies/flink.yaml`。
     ⚠ **镜像前缀必须为 `hunter-flink/`、禁止 `hunter/`**：`verify_infra`/`render_k8s` 的 6 微服务
     模块表只放行 `hunter/<svc>`，`hunter/flink-jobs` 会判「未登记微服务镜像」致 lint 失败。
  2. **compose 单机（受控降级）**：flink-jm + 单 flink-tm，**V1.19.2 起同样使用 `hunter-flink/jobs:2.1.1`
     内置镜像**（compose 定义已带 `build: flink-jobs/Dockerfile`，与 K8s 形态单一事实来源；旧官方镜像无
     Python 不能跑作业），`flink-jobs/` 只读挂载保留作应急热替换；书面豁免见 `docs/deployment.md` §12
     （G-13/G-28 决策①）。
- **K8s 形态待人工确认项**（见 `release.md`）：`hunter-flink/jobs` 镜像仓库推送凭据、Kafka 侧
  SASL 凭据注入路径（作业当前仅读 `KAFKA_BOOTSTRAP_SERVERS`，走 broker 内部监听）、JM 高可用与
  checkpoint 持久卷（现为 `emptyDir`）。
