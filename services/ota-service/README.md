# ota-service

OTA 管理：版本仓库管理、升级任务调度、灰度发布（5%→20%→50%→100%）、升级监控、A/B 分区回滚

- 端口：**8084**（环境变量 `API_PORT` 可覆盖）；网关前缀 `/api/v1/ota`（不剥离前缀）
- 技术栈：Python 3.11+ / FastAPI / pydantic-settings / SQLAlchemy 2.0 (asyncpg) / MinIO(boto3) / Kafka(confluent-kafka)
- 健康探针：`GET /healthz`（存活）、`GET /readyz`（就绪，含 DB/Redis/**MinIO** 检查）
- 指标端点：`GET /metrics`（Prometheus 文本格式，由 `infra/monitoring` 抓取）

## 接口契约（单一事实来源）

`contracts/openapi/ota-service.yaml`（OpenAPI 3.0.3）—— 开发顺序：先改契约，再改实现。

| 端点 | 说明 |
|------|------|
| `GET /api/v1/ota/versions` | 版本列表（分页 + 状态/机型/关键字过滤，即时签发 15 分钟下载地址） |
| `POST /api/v1/ota/versions` | 创建版本草稿（返回 1 小时上传预签名地址与对象键；限流 5 QPS/用户） |
| `GET /api/v1/ota/versions/{version_id}` | 版本详情（含 task_count 引用评估） |
| `POST /api/v1/ota/versions/{version_id}/submit-testing` | 提交测试（G-18②：`draft → testing`，HeadObject 校验包已直传，缺失 → 6001） |
| `POST /api/v1/ota/versions/{version_id}/submit-review` | 提交审核（G-18②：`testing → reviewing`，test_summary 仅审计留痕） |
| `POST /api/v1/ota/versions/{version_id}/reject-review` | 审核驳回（G-18②：`reviewing → draft`，reason 必填留痕） |
| `POST /api/v1/ota/versions/{version_id}/publish` | 发布/审核批准（前置仅 `reviewing`；**长耗时**：流式 MD5/SHA-256/RSA-2048/版本单调校验 → `published`） |
| `POST /api/v1/ota/versions/{version_id}/deprecate` | 废弃（`published → deprecated/disabled`，禁止删除，保证审计可追溯） |
| `GET /api/v1/ota/tasks` | 任务列表（进度摘要；target_vehicles 仅返回计数） |
| `POST /api/v1/ota/tasks` | 创建任务（目标版本 + 目标车辆 + 冻结灰度策略 + 门禁条件） |
| `GET /api/v1/ota/tasks/{task_id}` | 任务详情（`progress` 聚合 + `rollout` 灰度推进视图） |
| `POST /api/v1/ota/tasks/{task_id}/start` | 启动（逐车门禁 → 批次下发 ota_notify；`released[]/blocked[]` 回执，幂等） |
| `POST /api/v1/ota/tasks/{task_id}/pause` | 暂停（`running → paused`，冻结批次推进） |
| `POST /api/v1/ota/tasks/{task_id}/resume` | 恢复（重新评估批次；未达门禁 → 3003 + `halt_reason`） |
| `POST /api/v1/ota/tasks/{task_id}/cancel` | 终止（终态 `canceled`，进度冻结） |
| `POST /api/v1/ota/tasks/{task_id}/rollback` | 回滚（仅 SUCCESS 记录，逐车下发 `ota_rollback` 指令，仅 `previous_slot`） |
| `GET /api/v1/ota/tasks/{task_id}/records` | 任务升级记录（逐车 status/phase/progress + 任务级 summary） |
| `GET /api/v1/ota/vehicles/{vehicle_id}/records` | 单车升级历史（跨任务，按时间倒序） |

## 实现结构（契约驱动，已完成 REST/Kafka 主链路）

```
app/
├── main.py                # lifespan 装配（DB/Redis/MinIO/Kafka/服务），业务路由注册
├── config.py              # pydantic-settings（OTA_* 全量环境变量；灰度契约固定值启动强校验）
├── schemas/               # Pydantic v2 模型（common/versions/tasks/records，契约一一对应）
├── repositories/          # storage(MinIO) / versions / tasks / records（SQLAlchemy 2.0 异步）
├── services/              # versions（G-18② 审核流 + 发布五项校验）/ tasks（灰度/门禁/动作/回滚 + G-14 调度入口）/ records
│   ├── rollout.py         # 纯函数：批次取整（ceil+末批补足）/成功率/next_action 派生 + derive_scheduler_action 决策
│   ├── gates.py           # 纯函数：升级门禁（电量/P 档/网络/存储）+ check_batch 批量门禁（两次 pipeline 读）
│   └── scheduler.py       # G-14：RolloutScheduler 后台循环（到点启 scheduled/推进/暂停/完成）
├── producers/             # hunter.{vehicle_id}.ota_notify / hunter.{vehicle_id}.command(ota_rollback)
├── consumers/ota_status.py  # 消费 hunter.*.ota_status（组 ota-service-ota-status，推进记录+灰度门禁）
├── core/                  # RBAC 依赖（ota:read/create/execute）/限流（附录 D）/RSA 验签/指标
└── tests/                 # 契约一致性 + API + 纯逻辑 + 验签 + G-18② 审核流 + G-14 调度器（128 项，无基础设施依赖）
```

- **RBAC**：网关注入 `X-User-Id/X-Roles`；`ota:read`（查询）、`ota:create`（创建）、
  `ota:execute`（提测/提审/驳回/发布/退役/动作/回滚）；缺失 → 1001、不足 → 1002
- **限流**（附录 D）：`POST /versions` 单用户 5 QPS；`publish` 建议值 2 QPS；
  超限 429 + `Retry-After`（响应体 code 复用 5001，Redis 键 `rate_limit:{user}:{api}`）
- **灰度**（x-hunter-canary-rollout）：4 批 5/20/50/100、观察 24h、门禁 0.95（契约固定值，
  启动强校验）；批次取整 ceil+末批补足；FOR UPDATE 行锁串行化；禁止跳批（batch_no=当前+1）
- **灰度自动调度**（G-14，x-hunter-canary-rollout.scheduler）：后台协程按 `OTA_ROLLOUT_TICK_SECONDS`
  周期扫描（`ROLLOUT_SCHEDULER_ENABLED` 总开关，默认启用；单机/测试可关）——到点启动 scheduled 首批、
  观察窗到期且达标则推进下批、成功率 <0.95 置 paused（CRITICAL 日志）、末批达标置 succeeded；
  FOR UPDATE 行锁 + 状态机校验保证多副本下单任务只推进一次，不新增 Redis 键。批次下发以
  `check_batch` 两次批量读（pipeline `is_online_many`/`get_status_many`）替代逐车 2N 次 Redis 往返
- **OTA 安全**：MinIO `hunter-ota-packages`（永久）+ 流式 MD5/SHA-256 + RSA-2048 验签
  （`RSASSA-PKCS1-v1_5 + SHA-256`，公钥 `OTA_SIGNATURE_PUBLIC_KEY`）+ version_code 单调递增
  → 错误码 6001/6002/6003（data 携带 expected/actual 等上下文）
- **Kafka**：消费 `hunter.*.ota_status`（手动提交 + DLQ `{topic}.dlq`，幂等键
  `(task_id, vehicle_id, status)`，批次成功率 <0.95 → 任务 paused + CRITICAL 告警日志；
  alert_event 生产者属 data-analytics，本服务不生产）；生产 ota_notify（package_url =
  发送时刻签发 1h 预签名）与 command（仅 ota_rollback）
- **Redis**：写 `ota:progress:{task_id}`（Hash，TTL 86400）；只读
  `vehicle:status:{vehicle_id}` / `vehicle:online:set`（缺数据即拒绝，安全默认 #19）

数据访问边界（`contracts/database/er.md`）：仅写 `ota_svc` schema 的 `ota_versions` / `ota_tasks` /
`ota_records`（禁止 DELETE）；不跨 schema 直连；不新增 Redis 键模式。

> ⚠ 待人工确认 20 项见契约 `x-hunter-pending-confirmation`。实现默认采纳契约 contract_decision：
> 签名载荷 = SHA-256 hex 字符串（契约缺失项，见 release.md 风险清单）；broadcast.command 不生产。

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
# 契约一致性（无需运行服务/基础设施）
pytest app/tests/test_ota_contract.py -q
```
