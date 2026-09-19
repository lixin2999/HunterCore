# infra/monitoring — 监控栈（Prometheus + Alertmanager + Grafana + Exporters）

> 可观测性三支柱中的「指标」侧实现；日志（ELK/Loki）与链路追踪（Jaeger/SkyWalking）在后续层级接入。
> 阈值来源：《数据采集与分析系统详细设计文档 V4.0》15.4.2 告警阈值 + 性能指标章节
> （⚠ 设计文档未随仓库提供，当前阈值按系统上下文基座中的性能指标与通用运维经验取值，需人工核对）。

## 1. 目录结构

```
infra/monitoring/
├── kustomization.yaml                    # 监控栈入口（kubectl apply -k infra/monitoring）
├── prometheus/
│   ├── prometheus.yml                    # 采集配置（k8s 服务发现 + 中间件 exporter）
│   └── alert-rules.yaml                  # 告警规则（5 组 20 条）
├── alertmanager/
│   └── alertmanager.yml                  # 路由与接收器（critical 立即通知 + 抑制规则）
├── grafana/
│   ├── provisioning/
│   │   ├── datasources/prometheus.yml    # 自动注册数据源
│   │   └── dashboards/dashboards.yml     # 看板供给（禁止 UI 修改，Git 为准）
│   └── dashboards/
│       ├── hunter-fleet-overview.json    # 车队概览（在线车辆/服务健康/积压/磁盘）
│       ├── hunter-kafka.json             # Kafka（broker/lag/分区契约核对/流量）
│       └── hunter-api-performance.json   # API 性能（P95/P99/QPS/错误率/慢路由）
├── exporters/
│   └── exporters.yaml                    # node-exporter / kube-state-metrics / PG / Redis / Kafka exporter
└── k8s/
    ├── rbac.yaml                         # Prometheus 服务发现所需最小 RBAC
    ├── prometheus.yaml                   # Deployment + PVC 100Gi + Service
    ├── alertmanager.yaml                 # Deployment + PVC 10Gi + Service
    └── grafana.yaml                      # Deployment + PVC 10Gi + Service
```

## 2. 部署

```bash
# 监控组件（命名空间 hunter-core；配置由 configMapGenerator 从仓库源文件生成）
kubectl apply -k infra/monitoring

# 采集器（命名空间 monitoring；node-exporter 需要 host 网络/文件系统）
kubectl apply -f infra/monitoring/exporters/exporters.yaml

# 校验
kubectl -n hunter-core rollout status deploy/prometheus
kubectl -n hunter-core rollout status deploy/grafana
python scripts/verify_infra.py
```

访问入口（生产建议经网关统一暴露并加鉴权，禁止直接对外暴露 Prometheus/Grafana）：

```bash
kubectl -n hunter-core port-forward svc/grafana 3000:3000     # 本地调试
kubectl -n hunter-core port-forward svc/prometheus 9090:9090
```

Grafana 管理员口令来自 `hunter-app-secrets/GRAFANA_ADMIN_PASSWORD`（见 `infra/k8s/base/02-secret.example.yaml`）。

## 3. 指标来源

| 来源 | 端点 | 关键指标 |
|------|------|---------|
| 6 个微服务 | `GET /metrics`（Pod 注解 `prometheus.io/scrape`） | `hunter_http_requests_total`、`hunter_http_request_duration_seconds_bucket`、`hunter_http_requests_in_progress`、`hunter_service_info` |
| node-exporter | `:9100/metrics` | `node_filesystem_*`（磁盘告警）、`node_cpu_*`、`node_memory_*` |
| kube-state-metrics | `:8080/metrics` | `kube_pod_container_status_restarts_total`、`kube_pod_container_status_last_terminated_reason` |
| postgres-exporter | `:9187/metrics` | `pg_stat_activity_count`、`pg_settings_max_connections` |
| redis-exporter | `:9121/metrics` | `redis_memory_used_bytes`、`redis_memory_max_bytes` |
| kafka-exporter | `:9308/metrics` | `kafka_consumergroup_lag`、`kafka_topic_partition_current_offset`、`kafka_brokers` |
| MinIO | `/minio/v2/metrics/cluster`（Pod 注解） | `minio_cluster_nodes_online_total`、`minio_cluster_usage_total_bytes` |
| Kafka JMX | `:9404/metrics`（侧车，待接入） | `kafka_server_replicamanager_underreplicatedpartitions`、`kafka_server_brokertopicmetrics_*` |

指标命名与埋点实现见 `common/python/hunter_common/metrics.py`；业务指标（`hunter_telemetry_*`、
`hunter_ota_*`、`hunter_remote_control_*`、`hunter_events_total` 等）由各服务在 L2+ 实现，未落地前对应告警规则不触发。

## 4. 告警规则（`prometheus/alert-rules.yaml`，5 组）

| 组 | 规则 | 阈值 | 级别 |
|----|------|------|------|
| 服务健康 | `HunterServiceDown` | `up==0` 持续 1m | critical |
| | `HunterServiceHighErrorRate` | 5xx 占比 > 1%（5m） | warning |
| | `HunterContainerRestarting` | 15m 重启 > 3 次 | warning |
| | `HunterContainerOOMKilled` | 最近一次终止原因为 OOMKilled | critical |
| API 性能 | `HunterApiLatencyP95Warning` | P95 > 200ms（5m） | warning |
| | `HunterApiLatencyP95Critical` | P95 > 500ms（5m） | critical |
| | `HunterApiConcurrencySaturated` | 并发 > 200（10m） | warning |
| 数据管道 | `HunterKafkaConsumerLagHigh` | lag > 10000（10m） | warning |
| | `HunterKafkaConsumerLagCritical` | lag > 100000（5m） | critical |
| | `HunterKafkaBrokerDown` | broker < 3（2m） | critical |
| | `HunterTelemetryIngestLagHigh` ⚠ | 入库延迟 > 1s | critical |
| | `HunterTelemetryWriteRateLow` ⚠ | 写入 < 5000 点/秒 | warning |
| 中间件 | `HunterPostgresDown` / `HunterPostgresConnectionSaturated` | 不可达 / 连接数 > 80% | critical / warning |
| | `HunterRedisUnavailable` / `HunterRedisMemoryHigh` | 不可达 / 内存 > 85% | critical / warning |
| | `HunterMinioClusterDegraded` / `HunterMinioClusterUnavailable` | 离线 ≥1 / 在线 <3 | warning / critical |
| | `HunterDiskSpaceWarning` / `HunterDiskSpaceCritical` | > 85% / > 92% | warning / critical |
| | `HunterCertificateExpiringSoon` | 30 天内过期 | warning |
| 业务约束 | `HunterAlertTriggerLatencyHigh` ⚠ | 告警触发 > 2s | critical |
| | `HunterVehicleTelemetrySilence` ⚠ | 遥测中断 > 10s（communication_loss） | critical |
| | `HunterRemoteControlCommandLatencyHigh` ⚠ | 指令延迟 > 100ms | warning |
| | `HunterRemoteControlVideoLatencyHigh` ⚠ | 视频延迟 > 200ms | warning |
| | `HunterRemoteControlSessionConflict` ⚠ | 10m 冲突 > 5 次（7001） | warning |
| | `HunterOtaCanarySuccessRateLow` ⚠ | 灰度成功率 < 95% | critical |
| | `HunterOtaPackageVerifyFailed` ⚠ | 校验失败（6001/6002） | critical |

⚠ 标记的规则依赖业务层指标（L2+ 实现），未落地前表达式无数据、不会触发。

## 5. 看板

| 看板 | 用途 | 关键面板 |
|------|------|---------|
| `hunter-fleet-overview` | 车队与平台总览 | 在线车辆、服务实例健康（期望 15）、MinIO 在线节点、API 速率、Kafka 积压、告警事件、磁盘 |
| `hunter-kafka` | 消息链路 | Broker 数、副本不足分区、总积压、写入速率、积压趋势、分区数契约核对、Broker 流量 |
| `hunter-api-performance` | SLO 观测 | P95（阈值线 200ms）、P99、QPS、5xx 错误率、并发数、Top10 慢路由 |

看板由 Git 供给（`allowUiUpdates: false`），修改必须提交仓库并重新 `kubectl apply -k infra/monitoring`。

## 6. 待办与已知约束

1. **业务指标落地**：各服务按 `hunter_<domain>_<metric>` 前缀暴露遥测延迟、OTA 成功率、远程操控延迟等指标（L2+）。
2. **PostgreSQL TLS**：当前 `postgres-exporter` 使用 `sslmode=disable`，启用 PG TLS 后必须改为 `verify-full`。
3. **Kafka JMX 指标**：`kafka-jmx` 抓取任务已配置（`:9404`），需在 `statefulsets/kafka.yaml` 中加入 JMX Exporter 侧车。
4. **Alertmanager 接收器**：`alertmanager.yml` 中 webhook 为占位地址，生产替换为企业微信/钉钉/邮件/短信网关，凭据经 Secret 注入。
5. **Prometheus 高可用**：当前单副本 + 100Gi PVC；跨机房高可用（双副本 + Thanos/Cortex 远程写）在后续层级评估。
6. **日志与链路追踪**：ELK/Loki + Jaeger/SkyWalking 接入属后续层级，本层仅提供指标与告警。
7. **告警阈值核对**：所有阈值需与设计文档 15.4.2 节逐条核对（见文件头说明）。
