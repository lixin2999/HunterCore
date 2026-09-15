# infra — 基础设施配置

| 目录 | 内容 |
|------|------|
| `docker/` | Dockerfile 约定 + 本地开发 docker-compose 相关初始化脚本（postgres/kafka/minio init） |
| `k8s/` | Kubernetes 部署清单（Deployment / StatefulSet / Service / Ingress / ConfigMap / Secret 模板），随服务开发逐步添加 |
| `monitoring/` | Prometheus + Grafana 配置（指标采集、告警规则、看板） |

本地开发入口：仓库根目录 `docker compose up -d`。
