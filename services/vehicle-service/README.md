# vehicle-service

车辆台账与车端接入 provisioning 服务：DB 记录 + Kafka SCRAM/Topic + 每车独立 mTLS 客户端证书 + 一键接入包（ZIP）下载。

- **端口**：8086（环境变量 `API_PORT` 可覆盖）
- **网关前缀**：`/api/v1/vehicle`（详见 [contracts/openapi/api-gateway.yaml](file:///e:/python_work/HunterPilot/HunterCore/contracts/openapi/api-gateway.yaml)）
- **契约**：[contracts/openapi/vehicle-service.yaml](file:///e:/python_work/HunterPilot/HunterCore/contracts/openapi/vehicle-service.yaml)
- **技术栈**：Python 3.11 / FastAPI / pydantic-settings / SQLAlchemy 2.0 (asyncpg) / kafka-python-ng (AdminClient) / openssl 子进程

## 能力总览

| 端点 | 权限 | 用途 |
|---|---|---|
| `GET /api/v1/vehicle/list` | `vehicle:read` | 分页 + 状态/provision_state/关键词过滤 |
| `GET /api/v1/vehicle/{id}` | `vehicle:read` | 详情 + 接入摘要 + 4 步状态时间线 |
| `POST /api/v1/vehicle` | `vehicle:create` | **一键开通向导**（4 步：DB → SCRAM → 8 Topic → 独立证书） |
| `PATCH /api/v1/vehicle/{id}` | `vehicle:update` | 修改台账基础字段（不涉及 provisioning 资源） |
| `DELETE /api/v1/vehicle/{id}?purge_topics=true` | `vehicle:delete` | 下线（反序回收：证书 → SCRAM → Topic → DB 行） |
| `POST /api/v1/vehicle/{id}/rotate-scram` | `vehicle:execute` | 重置 SCRAM 口令（一次性返回） |
| `POST /api/v1/vehicle/{id}/reissue-cert` | `vehicle:execute` | 重签客户端证书（覆盖旧产物） |
| `GET /api/v1/vehicle/{id}/bundle` | `vehicle:execute` | 下载 ZIP 接入包 |

运维端点：`/healthz`（存活）、`/readyz`（DB + Kafka AdminClient + CA 私钥可读）、`/metrics`（Prometheus）。

## Provisioning 编排（4 步 + 失败自动回滚）

```
1. db      →  INSERT vehicle_svc.vehicles（provision_status 初始骨架）
2. scram   →  AdminClient.alter_user_scram_credentials UPSERT（KIP-95，SCRAM-SHA-512）
3. topics  →  AdminClient.create_topics  8 × hunter.{vehicle_id}.*（分区/保留期严格按 contracts/kafka/topics.yaml）
4. cert    →  openssl 签发独立客户端证书（CN=vehicle_id, SAN=DNS:vehicle_id, 3650d）+ PKCS12
```

任一步失败：**逆序回收**已成功步骤（Topic → SCRAM → 证书；DB 行保留供审计，标 failed）；
`takeover_existing=true` 允许接管历史遗留 SCRAM/Topic/证书（跳过创建，state="skipped"，回滚时不清理外部既有资源）。

## 一次性 SCRAM 口令的安全约束

- 服务端 `secrets.token_urlsafe(24)` 生成，**只在 `POST /api/v1/vehicle` / `POST /rotate-scram` 响应体中出现一次**；
- 数据库、日志、Prometheus 指标、trace 中间件均**不记录**（`error_handlers.py` 对 `RequestValidationError` 只记录数量、不 echo 字段值）；
- 用户丢失口令：调用 `/rotate-scram` 生成新口令（旧即时失效）；
- Bundle ZIP **不含 SCRAM 口令**，`kafka.properties` 中以 `<SCRAM_PASSWORD>` 占位符出现，由运营拷入车端后手工填写。

## Bundle ZIP 结构

```
{vehicle_id}/
  ca-cert.pem         # 平台 CA（车端信任链）
  client-cert.pem     # 该车独立客户端证书（CN=vehicle_id）
  client-key.pem      # 客户端私钥（0600）
  kafka-client.p12    # PKCS12（Java Producer/Consumer 直接可用；p12 口令见 kafka.properties 首行注释）
  kafka.properties    # bootstrap=SASL_SSL/SCRAM-SHA-512 + 生产者/消费者基准参数
  README.md           # 车端 5 步接入说明
```

产物落地：`{VEHICLE_CERTS_DIR}/{vehicle_id}/`（默认 `/app/data/vehicle-certs/`，K8s/compose 挂 `:rw`）。

## 本地运行

```bash
# 1. 装共享库与服务
pip install -e "common/python"
pip install -e "services/vehicle-service[dev]"

# 2. 依赖准备
#    - PostgreSQL + TimescaleDB：跑 infra/deploy/sql/{schema,init-data}.sql
#    - Kafka 3.6：broker ≥ 2.7 支持 KIP-95；本地可用 PLAINTEXT
#    - CA 证书：bash scripts/gen-kafka-certs.sh（生成 /opt/hunter-core/certs/kafka/ca-{cert,key}.pem）

# 3. 启动
cd services/vehicle-service
export KAFKA_CA_KEY_PATH=/opt/hunter-core/certs/kafka/ca-key.pem
export KAFKA_CA_CERT_PATH=/opt/hunter-core/certs/kafka/ca-cert.pem
export VEHICLE_CERTS_DIR=$(pwd)/data/vehicle-certs
export SERVER_IP=127.0.0.1
mkdir -p "$VEHICLE_CERTS_DIR"
uvicorn app.main:app --reload --port 8086
```

## 测试

```bash
cd services/vehicle-service
pytest -q
```

覆盖：契约一致性（`test_contract.py`）、provisioner 4 类失败回滚（`test_provisioner.py`）、
cert_signer openssl 输出可 verify（`test_cert_signer.py`）、kafka_admin mock 调用参数（`test_kafka_admin.py`）。

## 部署配置要点

- **CA 私钥可读**：`ca-key.pem` 通过 `setfacl -m g:10001:r` 授权 vehicle-service gid（`scripts/install.sh` 已内建）；
- **证书目录**：`/data/vehicle-certs` 由 `install.sh` 预建 0750 gid=10001（vehicle-service 用户可写）；
- **Kafka 凭据**：AdminClient 使用平台侧 `hunter-client` SCRAM 账号（`gen-passwords.sh` 生成，走 INTERNAL 9092）；
- **RBAC**：`admin` 五动作齐全；`operator` 除 delete 外；`analyst/viewer` 只读；见 [init-data.sql](file:///e:/python_work/HunterPilot/HunterCore/infra/deploy/sql/init-data.sql)。

## 已知边界与后续计划

- **CA 私钥暴露给业务容器**：本 MVP 简化为文件挂载 + ACL；生产环境建议引入独立 CA 服务或 KMS/HSM；
- **CRL/OCSP 未实现**：重签证书后旧证书仍在 CA 有效期内可被 broker 接受，但 SCRAM 已覆盖 → 攻击面有限；后续接入吊销列表；
- **广播 Topic `hunter.broadcast.command`**：本服务不创建（不属任何单车），由部署初始化脚本统一创建；
- **限流**：`POST /api/v1/vehicle` / `/bundle` 建议单用户 1 QPS（附录 D 待补），当前沿用全局 QPS。
