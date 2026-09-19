# infra/k8s — Kubernetes 部署清单（L1）

> 目标：将 HunterEdge 6 个微服务与 4 类有状态中间件以生产可用方式部署到 K8s 1.28。
> 所有清单使用 `apps/v1` / `v1` / `batch/v1` / `networking.k8s.io/v1` / `rbac.authorization.k8s.io/v1`。

## 1. 目录结构

```
infra/k8s/
├── base/
│   ├── 00-namespace.yaml          # 命名空间 hunter-edge（pod-security=restricted）
│   ├── 01-configmap-common.yaml   # 共享非敏感配置（envFrom 注入所有服务）
│   └── 02-secret.example.yaml     # Secret 模板（禁止提交真实值；复制为 02-secret.yaml 使用）
├── services/                      # 6 个微服务：ConfigMap + Deployment + Service
│   ├── api-gateway.yaml           # 8080，3 副本
│   ├── scene-service.yaml         # 8081，2 副本
│   ├── data-collector.yaml        # 8082，3 副本
│   ├── data-analytics.yaml        # 8083，2 副本
│   ├── ota-service.yaml           # 8084，2 副本
│   └── remote-control.yaml        # 8085，3 副本
│       （以上镜像标签统一为 {version} 占位符，部署前经 scripts/render_k8s.py 渲染）
├── statefulsets/                  # 有状态中间件：headless Service + StatefulSet + PVC
│   ├── postgres.yaml              # PostgreSQL 15 + TimescaleDB 2.13
│   ├── kafka.yaml                 # Kafka 3.6（KRaft 3 节点，SASL_SSL + SCRAM-SHA-512）
│   ├── redis.yaml                 # Redis 7（AOF，noeviction）
│   └── minio.yaml                 # MinIO（4 节点纠删码 EC:2，SSE-S3 + HTTPS）
├── jobs/
│   ├── kafka-init-job.yaml        # 内部 Topic 创建（契约分区数/保留时间）
│   └── minio-init-job.yaml        # 7 个 Bucket + 生命周期 + SSE-S3
├── autoscaling/
│   └── hpa.yaml                   # HPA：6 微服务，min=2 / max=5 / CPU 70%（缩容稳定窗口 300s）
├── disruption/
│   └── poddisruptionbudgets.yaml  # PDB：节点排空/升级时的副本下限（自愿中断保护）
├── networkpolicies/
│   └── network-policies.yaml      # 默认拒绝入向 + 仅网关可达后端（纵深防御）
└── ingress.yaml                   # 网关路由表（/api/v1/**、/ws/remote/**）+ TLS
```

## 2. 部署顺序（必须遵守）

```bash
# 0) 版本渲染（镜像标签 = {version} 占位符，禁止硬编码版本）
python scripts/render_k8s.py --check                  # 仅校验占位符契约（CI lint 同款）
python scripts/render_k8s.py --version 0.1.0          # 渲染产物 build/k8s/（已被 .gitignore 忽略）
K8S=build/k8s                                         # 以下全部 apply 渲染产物，不要直接 apply infra/k8s/

# 1) 前置：镜像仓库凭据（私有仓库时）
kubectl -n hunter-edge create secret docker-registry hunter-registry \
  --docker-server=<registry> --docker-username=<user> --docker-password=<token>

# 2) 命名空间与配置
kubectl apply -f $K8S/base/00-namespace.yaml
kubectl apply -f $K8S/base/01-configmap-common.yaml
kubectl apply -f $K8S/base/03-configmap-contracts.yaml     # 运行时代码契约（消费侧 Schema 校验）
cp $K8S/base/02-secret.example.yaml infra/k8s/base/02-secret.yaml   # 替换全部占位值后（不入版本库）
kubectl apply -f infra/k8s/base/02-secret.yaml

# 3) TLS 证书类 Secret（二进制，见第 3 节）
#    hunter-kafka-tls / hunter-minio-tls / hunter-edge-tls

# 4) 中间件（有状态）
kubectl apply -f $K8S/statefulsets/postgres.yaml
kubectl apply -f $K8S/statefulsets/redis.yaml
kubectl apply -f $K8S/statefulsets/kafka.yaml
kubectl apply -f $K8S/statefulsets/minio.yaml

# 5) 等待就绪（Kafka 三节点 quorum / MinIO ≥3 节点）
kubectl -n hunter-edge rollout status statefulset/kafka --timeout=10m
kubectl -n hunter-edge rollout status statefulset/minio --timeout=10m
kubectl -n hunter-edge rollout status statefulset/postgres --timeout=5m

# 6) 初始化任务（Topic / Bucket）
kubectl apply -f $K8S/jobs/kafka-init-job.yaml
kubectl apply -f $K8S/jobs/minio-init-job.yaml
kubectl -n hunter-edge wait --for=condition=complete job/kafka-init --timeout=10m
kubectl -n hunter-edge wait --for=condition=complete job/minio-init --timeout=10m

# 7) 微服务与入口
kubectl apply -f $K8S/services/
kubectl apply -f $K8S/ingress.yaml

# 8) 网络策略（默认拒绝入向 + 仅网关可达后端）
kubectl apply -f $K8S/networkpolicies/

# 9) 自动扩缩容与可用性（需 metrics-server；HPA 接管 Deployment 副本数）
kubectl apply -f $K8S/autoscaling/hpa.yaml
kubectl apply -f $K8S/disruption/poddisruptionbudgets.yaml
kubectl -n hunter-edge get hpa,pdb

# 10) 监控栈（见 infra/monitoring/README.md）
kubectl apply -k infra/monitoring
kubectl apply -f infra/monitoring/exporters/exporters.yaml
```

> 也可由 GitLab CI 完成上述链路（tag 流水线）：`build-images` → `render-manifests` →
> `deploy-prod`（手工触发），详见 `.gitlab-ci.yml`。

静态校验（无需集群）：

```bash
python scripts/render_k8s.py --check   # {version} 占位符 + 6 个微服务镜像齐备
python scripts/verify_infra.py         # API 版本/命名空间/探针/资源/HPA/PDB/敏感字段/端口/契约一致性
```

> 注意：`kubectl apply --dry-run=client` 仍需集群 API（openapi/group list），
> 无集群环境请以上述静态校验脚本为准。

## 3. TLS 证书与 Secret（全链路 TLS 前置）

Kafka 采用 **PEM 格式证书**（`ssl.keystore.type=PEM`），MinIO 与 Ingress 采用
`kubernetes.io/tls` 类型 Secret，证书 CommonName/SAN 必须与集群内 DNS 名一致。

```bash
# 3.1 内部 CA
openssl req -x509 -newkey rsa:4096 -sha256 -days 3650 -nodes \
  -keyout ca.key -out ca.crt -subj "/CN=hunter-edge-internal-ca"

# 3.2 Kafka broker 证书（SAN 覆盖 3 个 Pod DNS + Service DNS）
cat > kafka-san.cnf <<'EOF'
[req]
distinguished_name = dn
req_extensions = v3_req
prompt = no
[dn]
CN = kafka
[v3_req]
subjectAltName = @alt
[alt]
DNS.1 = kafka-0.kafka-headless.hunter-edge.svc.cluster.local
DNS.2 = kafka-1.kafka-headless.hunter-edge.svc.cluster.local
DNS.3 = kafka-2.kafka-headless.hunter-edge.svc.cluster.local
DNS.4 = kafka.hunter-edge.svc.cluster.local
EOF
openssl req -new -newkey rsa:2048 -nodes -keyout tls.key -out tls.csr -config kafka-san.cnf
openssl x509 -req -in tls.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out tls.crt -days 825 -extensions v3_req -extfile kafka-san.cnf

kubectl -n hunter-edge create secret generic hunter-kafka-tls \
  --from-file=ca.crt=ca.crt --from-file=tls.crt=tls.crt --from-file=tls.key=tls.key

# 3.3 MinIO 证书（SAN 含 minio.hunter-edge.svc / minio-0..3.minio-headless）
kubectl -n hunter-edge create secret tls hunter-minio-tls \
  --cert=minio.crt --key=minio.key

# 3.4 Ingress 控制台证书
kubectl -n hunter-edge create secret tls hunter-edge-tls \
  --cert=console.crt --key=console.key
```

Kafka SCRAM 用户（`KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD`）无需手工创建：
broker 首次启动执行 `kafka-storage.sh format --add-scram` 时以
`hunter-app-secrets` 中的凭据建立初始超级用户（见 `statefulsets/kafka.yaml`）。

## 4. 资源配额与副本（⚠ 数值需与设计文档 15.2 节核对）

| 工作负载 | 副本 | requests | limits | DB_POOL_SIZE | 说明 |
|---------|------|----------|--------|--------------|------|
| api-gateway | 3 | 500m / 512Mi | 2 / 2Gi | 5 | 鉴权与限流 CPU 敏感 |
| scene-service | 2 | 250m / 512Mi | 1 / 1Gi | 3 | 场景 CRUD 与导出 |
| data-collector | 3 | 500m / 1Gi | 2 / 4Gi | 5 | Kafka 消费 + 批量写入（≥1 万点/秒） |
| data-analytics | 2 | 1 / 2Gi | 4 / 8Gi | 9 | 聚合与 Corner Case 挖掘，内存密集 |
| ota-service | 2 | 250m / 512Mi | 1 / 1Gi | 3 | 任务调度与灰度编排 |
| remote-control | 3 | 500m / 1Gi | 2 / 4Gi | 5 | WebRTC 转发，长连接 IO 密集 |
| postgres | 1 | 1 / 2Gi | 4 / 8Gi | — | max_connections=300；PVC 200Gi |
| kafka | 3 | 1 / 4Gi | 4 / 12Gi | — | KRaft，每 broker PVC 500Gi |
| redis | 1 | 500m / 512Mi | 2 / 2Gi | — | AOF everysec，PVC 20Gi |
| minio | 4 | 1 / 2Gi | 2 / 4Gi | — | 纠删码 EC:2，每节点 PVC 500Gi |

**数据库连接池核算**（开发规则：连接池 = CPU 核数 × 2 + 1，按单副本 limits 计算）：
`3×5 + 2×3 + 3×5 + 2×9 + 2×3 + 3×5 = 75`（含 max_overflow 峰值约 150）< `max_connections=300`，
留出监控/运维会话余量。

## 5. 自动扩缩容与可用性（HPA / PDB）

### 5.1 HPA（`autoscaling/hpa.yaml`，基于 CPU 使用率）

| 服务 | 基线副本 | minReplicas | maxReplicas | 指标 | 缩容行为 |
|------|---------|-------------|-------------|------|---------|
| api-gateway | 3 | 2 | 5 | CPU 平均利用率 70% | 稳定窗口 300s，每分钟至多 -1 |
| scene-service | 2 | 2 | 5 | 同上 | 同上 |
| data-collector | 3 | 2 | 5 | 同上 | 同上 |
| data-analytics | 2 | 2 | 5 | 同上 | 同上 |
| ota-service | 2 | 2 | 5 | 同上 | 同上 |
| remote-control | 3 | 2 | 5 | 同上 | 同上 |

- 前置依赖：**metrics-server**（未安装时 `kubectl get hpa` 的 `TARGETS` 显示 `<unknown>`，
  副本数回落 `minReplicas`，不影响服务可用性）。
- 扩容策略：30s 窗口内 `Percent 100%` 或 `Pods +2` 取大者；缩容策略保守，避免指标抖动引发反复扩缩。
- 仅作用于无状态微服务；有状态中间件（PostgreSQL/Kafka/Redis/MinIO）**禁止** HPA。

⚠ 人工确认点（详见清单内注释）：
1. 3 副本基线的服务在 HPA 接管后可能被缩到 **2 副本**；若要求“永不低于 3 副本”，
   把这 3 个 HPA 的 `minReplicas` 改为 3（唯一改动点）。
2. `remote-control` 为 WebRTC 长连接且副本不迁移（契约 pending #14）：缩容会中断在线操控会话，
   建议操控期间人工冻结缩容。
3. `data-collector` 的有效并行度受 `telemetry` 主题分区数（12）约束，`maxReplicas=5` 在范围内。

### 5.2 PDB（`disruption/poddisruptionbudgets.yaml`，自愿中断保护）

| 服务 | minAvailable | 基线副本 | 说明 |
|------|--------------|---------|------|
| api-gateway / data-collector / remote-control | 2 | 3 | 节点排空时至少保留 2 副本 |
| scene-service / data-analytics / ota-service | 1 | 2 | 节点排空时至少保留 1 副本 |

- 与 Deployment 的 `maxUnavailable: 0`（滚动更新不降级）互补：PDB 约束**自愿中断**
  （`kubectl drain` / 集群升级 / 节点自动缩容），配合 `terminationGracePeriodSeconds`
  （30s / 60s，见各服务清单）实现优雅终止，支撑可用性 ≥ 99.9%。
- `minAvailable` 必须**小于**基线副本数，否则排空永久阻塞（`scripts/verify_infra.py` 强制校验）。
- ⚠ HPA 与 PDB 叠加：HPA 缩容到 2 副本后，3 副本基线服务的 `minAvailable: 2` 会阻塞 drain，
  建议改用 `maxUnavailable: 1`（随副本数浮动）—— 需人工确认。

## 6. 关键设计决策

| 决策 | 原因 |
|------|------|
| Ingress 全部前缀指向 `api-gateway:8080` | 网关承担 JWT 鉴权、五级限流、审计日志；直连服务会绕过安全控制（路由前缀与设计文档网关路由表一一对应） |
| 三种探针齐备（startup/liveness/readiness） | distroless 无 shell，统一使用 HTTP GET：`/healthz`（存活）、`/readyz`（就绪，内部带 2s 超时，依赖不可用时快速 503 + code=5001） |
| `maxUnavailable: 0` + 3 副本 | 滚动更新期间不降低接入能力（可用性 ≥ 99.9%） |
| `topologySpreadConstraints` | 同服务副本分散到不同节点，规避单节点故障 |
| Pod 安全上下文：非 root + drop ALL + seccomp RuntimeDefault | `hunter-edge` 命名空间启用 `pod-security=restricted`；distroless `:nonroot` 为 UID 65532 |
| `automountServiceAccountToken: false`（业务服务） | 服务不访问 K8s API，最小权限（Prometheus 例外，需服务发现） |
| Kafka 显式 `server.properties` + 启动脚本 | 不依赖镜像默认魔法；SASL_SSL + SCRAM-SHA-512 + `auto.create.topics.enable=false`（Topic 仅按契约创建） |
| Kafka 3 副本 + `min.insync.replicas=2` | 单 broker 故障仍可写，配合 `acks=all` 满足事件/指令类不丢消息要求 |
| Redis `maxmemory-policy noeviction` | Redis 承载会话、分布式锁（远程操控互斥）与限流计数，淘汰将导致掉线与互斥失效 |
| MinIO 4 节点纠删码 | 容忍 2 节点故障（EC:2）；SSE-S3 服务端加密；HTTPS 强制 |
| 有状态中间件单一 `standard` StorageClass | 生产按集群实际 StorageClass 覆盖；PVC 容量数值待核对 |
| 镜像标签使用 `{version}` 占位符（仓库清单禁止硬编码版本） | 版本是部署期变量（CI 取 `$CI_COMMIT_TAG`）：单一来源、可审计；由 `scripts/render_k8s.py` 渲染后 apply，避免“清单版本与镜像版本漂移” |
| HPA `min=2 / max=5 / CPU 70%` | 满足突发流量弹性与空闲成本；有状态中间件禁止自动扩缩（数据一致性与副本语义） |
| PDB `minAvailable < 基线副本数` | 补齐“自愿中断”（节点排空/集群升级）的可用性约束；取值必须留出至少 1 个可驱逐副本，否则 drain 永久阻塞 |
| 探针统一 HTTP GET `/healthz`（存活）/ `/readyz`（就绪） | distroless 无 shell，无法使用 exec/tcp 探针；就绪端点内部 2s 超时快速返回 503 + `code=5001` |

## 7. 常见问题排查

| 现象 | 排查方向 |
|------|---------|
| Pod 长期 `Pending` | PVC 无法绑定（StorageClass 名称/容量）、节点资源不足 |
| Pod `ContainerCreating` 卡住 | 依赖的 Secret 未创建（`hunter-kafka-tls` / `hunter-minio-tls` / `hunter-app-secrets`） |
| Kafka Pod `CrashLoopBackOff` | 证书 SAN 与 Pod DNS 不匹配、`KRAFT_CLUSTER_ID` 不一致、PVC 权限（fsGroup 1001） |
| Kafka init Job 失败 | broker 未就绪（Job 内置 5 分钟等待）、SCRAM 凭据与 broker 初始用户不一致 |
| MinIO init Job 失败 | 集群在线节点 < 3（纠删码可读阈值）；`MC_OPTS=--insecure` 仅用于自签证书 |
| 服务连不上 Kafka | 应用需配置 `KAFKA_SSL_CAFILE`；如需严格 mTLS 需补 `KAFKA_SSL_CERTFILE/KEYFILE` 配置项 |
| 服务 `/readyz` 返回 503 | 数据库/Redis 不可达（内部 2s 超时保护），检查中间件与网络策略 |
| 容器无 shell 无法排查 | 使用 `kubectl debug -it --image=busybox --target=<container>` 注入临时容器 |
| Pod `ImagePullBackOff`（镜像名含 `{version}`） | 直接 apply 了 `infra/k8s/` 源清单；先 `python scripts/render_k8s.py --version <版本>` 再 apply `build/k8s/` |
| HPA `TARGETS` 显示 `<unknown>` | metrics-server 未部署或未就绪；`kubectl -n kube-system get deploy metrics-server` |
| HPA 期望副本与 `kubectl get deploy` 的 `spec.replicas` 不一致 | 正常现象：HPA 接管后 Deployment 的 `replicas` 会被持续覆盖，以 `kubectl get hpa` 为准 |
| `kubectl drain` 长时间卡在 “evicting pod” | PDB 的 `minAvailable` 大于等于当前副本数（HPA 已缩容）；核对 PDB 与 HPA 取值（见第 5 节） |
| HPA 频繁扩缩（抖动） | 核对 `behavior.scaleDown.stabilizationWindowSeconds`（默认清单为 300s）；确认 CPU requests 设置合理（利用率按 requests 计算） |
