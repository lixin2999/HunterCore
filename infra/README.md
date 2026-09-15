# infra — 基础设施配置

| 目录 | 内容 |
|------|------|
| `docker/` | 本地开发 docker-compose 相关初始化脚本 + 各服务 Dockerfile 约定（builder `python:3.11-slim` → runner `gcr.io/distroless/python3-debian12:nonroot`） |
| `k8s/` | Kubernetes 部署清单：6 微服务（Deployment + Service + ConfigMap）、4 类有状态中间件（StatefulSet + PVC）、初始化 Job、Ingress |
| `monitoring/` | Prometheus 采集与告警规则、Alertmanager 路由、Grafana 供给与 3 个看板、exporters、监控组件清单 |

## 本地开发入口

```bash
docker compose up -d          # TimescaleDB / Kafka / Redis / MinIO + 一次性初始化任务
```

## 镜像构建（构建上下文必须是仓库根目录）

```bash
docker build -f services/scene-service/Dockerfile -t hunter/scene-service:0.1.0 .
```

## Kubernetes 部署

```bash
# 静态校验（无需集群）：API 版本/命名空间/探针/资源配额/敏感字段/端口/契约一致性
python scripts/verify_infra.py

# 应用清单（含中间件、初始化 Job、微服务、Ingress）
kubectl apply -f infra/k8s/base/00-namespace.yaml
# ... 完整顺序见 infra/k8s/README.md

# 监控栈
kubectl apply -k infra/monitoring
kubectl apply -f infra/monitoring/exporters/exporters.yaml
```

详见：[`k8s/README.md`](k8s/README.md)（部署顺序、TLS 证书生成、资源配额表、排查手册）、
[`monitoring/README.md`](monitoring/README.md)（指标来源、告警规则、看板、待办）。

## 安全基线（基础设施侧）

- 敏感值只经 K8s Secret 注入；仓库仅保留 `02-secret.example.yaml` 模板（真实 Secret 已被 `.gitignore` 忽略）
- `hunter-edge` 命名空间启用 `pod-security=restricted`（非 root、drop ALL、seccomp RuntimeDefault）
- 全链路 TLS：Ingress（HTTPS）+ Kafka（SASL_SSL/SCRAM-SHA-512 + 内部双向校验）+ MinIO（HTTPS + SSE-S3）

