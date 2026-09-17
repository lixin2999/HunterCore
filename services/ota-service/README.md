# ota-service

OTA 管理：版本仓库管理、升级任务调度、灰度发布（5%→20%→50%→100%）、升级监控、A/B 分区回滚

- 端口：**8084**（环境变量 `API_PORT` 可覆盖）；网关前缀 `/api/v1/ota`（不剥离前缀）
- 技术栈：Python 3.11+ / FastAPI / pydantic-settings / SQLAlchemy 2.0 (asyncpg)
- 健康探针：`GET /healthz`（存活）、`GET /readyz`（就绪，含 DB/Redis 检查）
- 指标端点：`GET /metrics`（Prometheus 文本格式，由 `infra/monitoring` 抓取）

## 接口契约（单一事实来源）

`contracts/openapi/ota-service.yaml`（OpenAPI 3.0.3）—— 开发顺序：先改契约，再改实现。

| 端点 | 说明 |
|------|------|
| `GET /api/v1/ota/versions` | 版本列表（分页 + 状态/机型/关键字过滤） |
| `POST /api/v1/ota/versions` | 创建版本草稿（返回 1 小时上传预签名地址与对象键） |
| `GET /api/v1/ota/versions/{version_id}` | 版本详情（含校验状态、下载预签名 15 分钟 + Range） |
| `POST /api/v1/ota/versions/{version_id}/publish` | 发布（**长耗时**：流式 MD5/SHA-256/RSA-2048/版本单调校验 → `published`） |
| `POST /api/v1/ota/versions/{version_id}/deprecate` | 废弃（`published → deprecated`，禁止删除，保证审计可追溯） |
| `GET /api/v1/ota/tasks` | 任务列表（进度摘要 + 灰度批次状态） |
| `POST /api/v1/ota/tasks` | 创建任务（目标版本 + 目标车辆 + 灰度策略 + 门禁条件） |
| `GET /api/v1/ota/tasks/{task_id}` | 任务详情（`progress` 聚合 + 各批次统计） |
| `POST /api/v1/ota/tasks/{task_id}/start` | 启动（门禁过滤 → 第一批下发，`blocked[]` 返回不满足车辆） |
| `POST /api/v1/ota/tasks/{task_id}/pause` | 暂停（成功率 < 0.95 自动暂停 + 告警 `alert_event`） |
| `POST /api/v1/ota/tasks/{task_id}/resume` | 恢复（人工确认后继续，已观察满 24h 才推进下一批） |
| `POST /api/v1/ota/tasks/{task_id}/cancel` | 取消（仅未终态任务，`cancelled`） |
| `POST /api/v1/ota/tasks/{task_id}/rollback` | 回滚（按记录下发 `ota_rollback` 指令，仅 `previous_slot`） |
| `GET /api/v1/ota/tasks/{task_id}/records` | 任务升级记录（逐车 status/phase/progress 高水位） |
| `GET /api/v1/ota/vehicles/{vehicle_id}/records` | 单车升级历史（跨任务，按时间倒序） |

设计文档章节 ↔ 实现落位：

- **8.x 节升级流程**：状态机 `IDLE → PENDING → DOWNLOAD → INSTALL → TEST → SUCCESS`（车端），
  TEST 自检失败 → `ROLLBACK → ROLLED_BACK / FAILED`；平台侧任务状态机
  `draft → pending_approval → running/observing → paused → completed/rolled_back/cancelled`
- **8.x 节灰度发布**：4 批 `5% / 20% / 50% / 100%`，每批观察 24h，成功率 ≥ `OTA_CANARY_MIN_SUCCESS_RATE`(0.95) 才推进；
  < 0.95 立即暂停 + 告警，人工只可 rollback / cancel；批次推进由 PG 行锁串行化（防跳批）
- **升级门禁**：电量 ≥50%、车辆静止(P 档)、网络稳定（遥测间隔 ≤ `OTA_OFFLINE_THRESHOLD_SECONDS`）、存储 ≥2GB
- **Kafka**：消费 `hunter.{vehicle_id}.ota_status`（正则 `hunter\..*\.ota_status`，消费组 `ota-service-ota-status`，
  手动提交 + DLQ `{original_topic}.dlq`）；生产 `hunter.{vehicle_id}.ota_notify`（逐车幂等下发）与
  `hunter.{vehicle_id}.command`（仅 `ota_rollback`）
- **OTA 安全**：MinIO `hunter-ota-packages`（永久）+ SSE-S3；SHA-256 完整性 + RSA-2048 签名（`RSASSA-PKCS1-v1_5 + SHA-256`）
  + 版本号单调递增（防回滚）→ 错误码 6001 / 6002 / 6003

数据访问边界（`contracts/database/er.md`）：仅写 `ota_svc` schema 的 `ota_versions` / `ota_tasks` / `ota_records`
（禁止 DELETE，废弃用状态字段）；不跨 schema 直连；不新增 Redis 键模式（写 `ota:progress:{task_id}`，
只读 `vehicle:status:{vehicle_id}` / `vehicle:online:set`）。

> ⚠ 待人工确认 19 项见契约 `x-hunter-pending-confirmation`（§12.5 原文缺失 / `release_type` 取值域 / 跳批策略 /
> publish 网关超时与异步化 / DLQ Topic 登记 / `command_result` 与 `broadcast.command` 归属 / 审计保留期等）。

## 本地运行

```bash
pip install -e "common/python"     # 仓库根目录：共享库
pip install -e "services/ota-service[dev]"
cd services/ota-service
uvicorn app.main:app --reload --port 8084
```

## 测试

```bash
cd services/ota-service && pytest -q
# 契约校验（无需运行服务；随实现步骤落地）
cd services/ota-service && pytest app/tests/test_ota_contract.py -q
```

> 实现进度：契约（Step 1）已就绪；Step 2 起按 Pydantic Schema → ORM/Alembic → services → routers → Kafka → tests 分层实现。
