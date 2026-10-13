# ingest-platform — 服务端数据消费解析与处理

车端 rosbag 数据的消费接入、解析、对齐、切片、质检与任务编排平台（《服务端数据消费解析与处理详细设计文档》
6 项核心需求的实现服务，V1.20.0 交付；审查 P0 三项修复见 V1.20.1；审查 P1 剩余 #4–#21（数据处理/安全/性能）
全部落地见 V1.20.2；审查「功能遗漏汇总」剩余 §12.3 DLT replay 工具 + `produce(dict)` 序列化缺陷修复见 V1.20.3）。端口 **8089**，schema `ingest_platform`（13 表）。

> ⚠ **部署状态**：本服务尚未纳入 `infra/deploy/docker-compose.yml` 与 `infra/k8s/` 编排，当前仅在
> 联调/手动接入环境运行；接入步骤与容量核算见 `docs/deployment.md` §14，运维口径见《运维操作手册》§3.7、
> 《故障排查指南》场景 14。

## 能力清单（对应设计文档章节）

| # | 能力 | 模块 |
|---|------|------|
| 1 | 消费接入与幂等（§5）：7 个 Kafka 消费者（bag manifest / artifact / 解析进度 / 告警 / 心跳 / 指令回执 / 任务状态）；入口层 1 events 表 `insert_dedup` 去重 + 层 2 seq/rank 守卫；手动提交 + DLQ；`_SupervisedConsumer` 崩溃退避重启 | `app/consumers/`、`app/main.py` |
| 2 | 元数据投影与 bag 状态机（§5.2）：事件 → `bags` 状态投影与到达判定，满足时创建任务链首节点（verify）；解析进度落 `bags.progress`（§5.1，V1.20.2 #10） | `app/services/projection.py`、`app/repositories/`、`app/models/ingest.py`（13 表 ORM） |
| 3 | 多传感器解析（§6）：rosbag2 读取（rosbags 纯 Python，无需 ROS2 运行时）+ pointcloud/image/imu/odometry/gnss/can 逐模态解析 → Parquet | `app/parsers/`、`app/storage/parquet_writer.py` |
| 4 | 跨模态时间对齐（§6/§7）：时钟源/偏移估计/容差判定 + aligned 派生资产写出；按 manifest 传感器全集分组、tf 外参缺失传感器整列置 null（V1.20.2 #6）；GNSS 读路径经 `mask_coordinates` 坐标截断脱敏（§13.4，V1.20.2 #17） | `app/time_sync.py`、`app/services/align_worker.py` |
| 5 | 场景切片与质检（§7.3/§7.4）：窗口/标签切片 + **服务端规则引擎**（IMU 去重力合加速度挖掘急刹/碰撞 → `clips.source='mined'`，V1.20.2 #7）；QC **9 项加权**（含黑帧/冻结帧/标定冲突三检 + 抽样真解码 + hash 声明值实算，V1.20.2 #4/#5） | `app/services/slice_worker.py`、`app/services/qc_worker.py` |
| 6 | 任务编排 + API 与指令下发（§7.1/§8/§9）：orchestrator 主循环（每轮事务内 `pg_try_advisory_xact_lock` 争锁、**并发轮批量 claim+gather 执行**（V1.20.2 #20）、§7.1 依赖序 verify→{align,qc}→slice）；数据集 **webdataset/lance 分片打包 + EXPORT 任务化**（V1.20.2 #8/#9）；指令下发超时**扫描重发一次再告警**（§9.4，V1.20.2 #13）、告警**持久化 + 分级路由**（V1.20.2 #11）、平台事件生产 | `app/services/orchestrator.py`、`app/services/task_graph.py`、`app/services/verify_worker.py`、`app/services/export_worker.py`、`app/routers/`、`app/services/dataset_service.py`、`app/services/cmd_service.py`、`app/repositories/alerts.py`、`app/producers/platform_events.py` |

对象存储：`app/storage/minio_client.py`（LakeStorage，预签名上传/下载 + HEAD 元数据，bucket 按 `contracts/database/object-storage.yaml`）。
**V1.20.2 #18**：对外公开方法均 **async**，同步 boto3 统一经 `ThreadPoolExecutor(8)` 投递，worker 执行期间不阻塞事件循环；
**#19** QC 拆三段事务（读元数据 / 无 session 存储 IO+计算 / 写结果），消除持连接做网络 IO 的长事务。

消费韧性：**#12 DLT 死信主题**（`app/consumers/base.py`）——解析/字段级无效消息（`TypeError/ValueError/KeyError/IndexError`）路由
`ingest.dlt`（保留原始消息 + 错误上下文），基础设施类失败仍走 `hunter_common` 的 `{topic}.dlq`；`DLT_ENABLED=False` 退回旧行为。
**§12.3 DLT replay 工具**（`app/tools/dlt_replay.py`，V1.20.3）——从 `ingest.dlt` 读回原始 envelope 并按 `original_topic` 重投，
让消息重走正常消费链路（原事务已回滚 ⇒ 事件仓层1去重重新放行）。CLI：`python -m app.tools.dlt_replay [--dry-run] [--topic T] [--error-type E] [--since/--until ISO] [--max N] [--group-id G] [--json]`，
默认独立消费组 `g.ingest.dlt-replay`；**先 `--dry-run` 评估、再收窄范围分批回填**。
**#21 背压**（`app/consumers/backpressure.py.BackpressureGuard`）——加工队列深度（`tasks.state=pending`）≥ `TASK_QUEUE_BACKPRESSURE_THRESHOLD`
时注入单消息节流延迟 + 令牌桶限速，`_before_process` 钩子接入 ArtifactConsumer。

## 安全与数据湖最小权限（§13）

- **认证 fail-closed（#14）**：`GATEWAY_HMAC_SECRET` **必须配置**——为空时 `_require_forwarded_auth` 直接 `401`（不再跳过验签），
  否则任何能直达端口者可伪造 `X-User-Id/X-Roles` 绕过 RBAC。
- **命令签名 fail-closed（#15）**：下行命令（cleanup/reparse/seal 等）签名密钥缺失即 `RuntimeError` 拒签，已删除公开回退常量 `dev-hmac-key`。
- **GNSS 脱敏（#17）**：frame_index 的 gnss lat/lon 经 `mask_coordinates(lat, lon, GNSS_DESCIMAL_PLACES)` 截断（derived 读路径即脱敏），
  `GNSS_MASK_ENABLED` 默认 True。
- **parsed 层只读隔离（#16，§13.2）**：湖上 parsed/derived/dataset 三层此前共用 `MINIO_ACCESS_KEY`。现支持
  `MINIO_PARSED_ACCESS_KEY`/`MINIO_PARSED_SECRET_KEY` 独立凭据，配置后 parsed 层全部读路径走专用只读 client，未配置回退主凭据（dev 兼容）。
  ⚠ **生产必须落实两步**：① 在 MinIO/IAM 建仅具 `s3:GetObject`/`s3:ListBucket` 于 `hunter-rosbag` 的专用 key 并注入本服务；
  ② 对 parsed bucket 挂只读 bucket policy（拒绝写/删），代码仅提供凭据通道，权限收敛在存储侧完成。

## REST 端点（经 api-gateway 统一前缀 `/api/v1/ingest` 接入，V1.20.4）

`/healthz`、`/readyz`、`/metrics`（共享库基线，直连）＋ `/api/v1/ingest/bags`、`/api/v1/ingest/clips`、`/api/v1/ingest/tasks`、
`/api/v1/ingest/artifacts`、`/api/v1/ingest/datasets`、`/api/v1/ingest/agents`、`/api/v1/ingest/commands`（均由网关 JWT 鉴权后转发）。

## 本地运行

```bash
pip install -e "common/python[dev]"
pip install -e "services/ingest-platform"
alembic -c common/python/alembic.ini upgrade head          # 含 ingest_platform schema（06_ingest_platform.sql）
uvicorn app.main:app --reload --port 8089                   # 需可达 PG/Kafka/Redis/MinIO（docker compose 基础设施）
```

配置继承 `hunter_common.config.HunterBaseConfig`（全量见 `app/config.py`）。V1.20.2 新增/关键项：
`GATEWAY_HMAC_SECRET`（**必填**，fail-closed）、`MINIO_PARSED_ACCESS_KEY`/`MINIO_PARSED_SECRET_KEY`（只读隔离）、
`DLT_ENABLED`/`DLT_TOPIC`、`RULE_MINING_ENABLED`/`RULE_HARD_BRAKE_MPS2`/`RULE_COLLISION_MPS2`、
`QC_BLACK_FRAME_THRESHOLD`/`QC_FROZEN_FRAME_MIN_COUNT`/`QC_IMAGE_SAMPLE_COUNT`、`GNSS_MASK_ENABLED`/`GNSS_DESCIMAL_PLACES`、
`ORCHESTRATOR_MAX_PARALLEL_TASKS`、`TASK_QUEUE_BACKPRESSURE_THRESHOLD`/`BACKPRESSURE_*`/`ARTIFACT_INGEST_RATE_LIMIT`、
`CMD_ACK_TIMEOUT_S`/`CMD_TIMEOUT_SCAN_INTERVAL_S`、`ALERT_CRITICAL_LEVELS`/`ALERT_WEBHOOK_URL`、`DATASET_SHARD_MAX_CLIPS`。
`ENRICH_WORKER_ENABLED` 默认 **False**（M4/M5 预留，worker 未实现）。

## 测试与静态检查

```bash
cd services/ingest-platform && pytest -q                   # 全内存替身，无外部依赖
python scripts/run_unit_tests.py ingest-platform           # 全仓回归口径（独立进程）
ruff check services/ingest-platform
```

`tests/test_ddl_contract.py` 钉住 `contracts/database/ddl/06_ingest_platform.sql` ↔ ORM 的表清单/逐列/枚举取值一致性
（ingest_platform 表级契约校验由本服务承担，共享库 `test_database_models` 跳过该 schema）。

## 镜像

`Dockerfile`：多阶段 `python:3.11-slim`(builder) → distroless nonroot(runner)，构建上下文 = 仓库根目录，
与其余服务同基线。
