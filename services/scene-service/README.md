# scene-service

场景生成：场景库管理、场景编辑、场景参数化、OpenSCENARIO 导出、实车场景自动提取

- 端口：**8081**（环境变量 `API_PORT` 可覆盖）
- 技术栈：Python 3.11+ / FastAPI / pydantic-settings / SQLAlchemy 2.0 (asyncpg) / httpx（Carla）/ boto3（MinIO）
- 健康探针：`GET /healthz`（存活）、`GET /readyz`（就绪，含 DB/Redis 检查）
- 指标端点：`GET /metrics`（Prometheus 文本格式，由 `infra/monitoring` 抓取）

## 本地运行

```bash
pip install -e "common/python"     # 仓库根目录：共享库
pip install -e "services/scene-service[dev]"
cd services/scene-service
uvicorn app.main:app --reload --port 8081
```

## 接口实现（契约 12.2 节 10 个业务端点 + 决策 G-20① 新增 2 个，共 12 个）

| 端点 | operationId | 实现要点 |
|------|-------------|---------|
| `GET /api/v1/scene` | listScenes | 分页（默认 20/上限 200）、分类/状态/标签(AND)/关键字/creator 筛选、排序白名单、软删除过滤 |
| `POST /api/v1/scene` | createScene | status=draft、creator=网关注入用户、version=1.0.0、重名 3002、时长上限 2001 |
| `GET /api/v1/scene/{scene_id}` | getScene | 读穿缓存 `cache:scene:{scene_id}`（TTL 3600s）；软删除视为不存在（3001） |
| `PUT /api/v1/scene/{scene_id}` | updateScene | 仅 draft 可编辑（否则 3003）、重名 3002、写后失效缓存 |
| `DELETE /api/v1/scene/{scene_id}` | deleteScene | 软删除（置 `deleted_at`）；仅 draft/archived 可删（published → 3003） |
| `POST /api/v1/scene/{scene_id}/duplicate` | duplicateScene | 复制为新草稿；缺省 `<源名称>-copy`（冲突追加序号） |
| `POST /api/v1/scene/{scene_id}/publish` | publishScene | draft → published；发布前复校 4.2.2 结构；显式 version 需 ≥ 当前版本 |
| `GET /api/v1/scene/templates` | listSceneTemplates | 内置模板清单（17 个叶子场景，实车回放无预置模板），分类/类型过滤 |
| `POST /api/v1/scene/export` | exportScenes | Carla ScenarioRunner XML / OpenSCENARIO 1.2 → MinIO `hunter-scene-assets` + 预签名 900s |
| `POST /api/v1/scene/{scene_id}/run` | runScene | 4.4 节 ①②③④⑤：校验 → 创建实例 → 下发配置 → 返回 sim_instance_id |
| `GET /api/v1/scene/simulations/{sim_instance_id}` | getSimulationProgress | 4.4 节第⑥步（G-20①）：无状态代理 Carla，进度字段宽松透传，缺失置 null；RBAC scene:read |
| `GET /api/v1/scene/simulations/{sim_instance_id}/result` | getSimulationResult | 4.4 节第⑦步（G-20①）：仅终态可查（非终态 3003）；object_key 产物换发预签名 900s；RBAC scene:read |

运维端点：`GET /healthz`、`GET /readyz`、`GET /metrics`（契约 ops_endpoints）。

## 场景引擎（V1.23.0，契约 `contracts/openapi/scene-engine.yaml`）

实现《场景生成器与场景管理详细设计文档》V2.0：三层场景抽象（功能 / 逻辑 / 具体）、
双轴分级（稀有度 Z1-Z4 × 临界度 C0-C3 → 风险 L1-L4）、八道校验门与测试用例/测试集/基线。
共 28 个端点，与场景库共用前缀 `/api/v1/scene`，**必须先于** `/{scene_id}` 路由注册。

| 分组 | 端点 | 要点 |
|------|------|------|
| 逻辑场景 | `GET /logical`、`POST /logical/sync`、`GET /logical/{logical_id}`、`GET /logical/{logical_id}/versions`、`POST /logical/{logical_id}/validate`、`POST /logical/{logical_id}/dryrun`、`POST /logical/{logical_id}/publish` | DB 快照优先、内置 17 条目录兜底；sync 为幂等 upsert（**不在启动时自动执行**，否则覆盖 status）；校验/干跑返回 G1-G7 明细 |
| 生成任务 | `POST /generate`（202）、`GET /generate`、`GET /generate/{task_id}`、`POST /generate/{task_id}/cancel` | BackgroundTasks + `asyncio.to_thread`（CPU 采样不阻塞事件循环）；进度节流回写；仅 pending 可取消（running → 3003） |
| 具体场景 | `GET /concrete`、`POST /concrete/batch-validate`、`GET /concrete/{scenario_id}`、`POST /concrete/{scenario_id}/render` | `content_hash` 全局唯一（同种子重放 `produced=0`）；批量复检失败进 `quarantined`；渲染返回可下发 SceneConfig |
| 判据/用例 | `GET /oracles`、`GET /testcases`、`POST /testcases`（201）、`GET /testcases/{test_case_id}` | 判据目录代码即事实来源（7 条版本化 ref）；用例为 ISO 34505 结构，`case_key` 唯一（3002） |
| 测试集/基线 | `GET /suites`、`POST /suites`（201）、`GET /suites/{suite_id}`、`POST /suites/{suite_id}/items`、`POST /suites/{suite_id}/lock`、`GET /suites/{suite_id}/diff`、`GET /suites/{suite_id}/baselines` | 锁定后条目不可改（3003）；锁定可同时建基线；diff 输出 added/removed/changed |
| 覆盖分析 | `GET /coverage/overview`、`GET /coverage/gaps` | Z1-Z4 / L1-L4 计数与占比；盲区按剖面配额（70:25:5:0 / 40:40:20:0 / 10:30:50:10）算缺口 |

存储：服务私有 schema `scene_engine`（7 表，`contracts/database/ddl/07_scene_engine.sql`），
ORM 在 `app/models/scene_engine.py`、仓储在 `app/repositories/engine.py`，不入共享包；
DDL ↔ ORM 一致性由 `app/tests/test_scene_engine_ddl.py` 钉住。
未初始化该 schema 时，逻辑场景列表/详情自动回落内置目录（不报错）；写路径返回 5000/5001。

鉴权：读 `scene:read`；写（同步/发布/复检/用例/测试集/锁定/取消）`scene:create|update|delete`；
生成 `scene:execute`。

Kafka：**不生产任何 Topic**（契约 `x-hunter-kafka.produces = []`）；仅消费 `analytics_result`
（消费组 `scene-service-analytics-result`，4.5 节实车场景自动提取：`harsh_braking` /
`collision_warning` / `manual_takeover` 触发，事件前后各 10 秒，输出 `real_vehicle_replay` + `draft`，
幂等键 `(vehicle_id, window_start)`，异常消息转投 `analytics_result.dlq`）。

## 分层结构

```
app/
├── main.py            # 应用入口：lifespan 装配（DB/Redis/MinIO/Carla/服务/消费者）+ 路由注册
├── config.py          # pydantic-settings（契约 required_env 一一对应，禁止硬编码）
├── routers/           # scenes / templates / export / simulation / engine + health + errors
├── schemas/           # common（统一响应/探针/分页边界）+ scene（4.2.2 结构）+ engine（引擎请求/响应）
├── services/          # scenes（CRUD/复制/发布）、templates、serializers、export、simulation、engine
├── scene_engine/      # 引擎内核：ir（IR 1.2）/ catalog（17 条目录）/ stats / compiler / validators（G1-G7）/ generator / oracles
├── models/            # scene_engine（服务私有 7 表 ORM，12 个受控词表）
├── repositories/      # scenes、cache、storage、carla、engine（5 个场景引擎仓储）
├── consumers/         # analytics_result（实车场景自动提取，4.5 节）
├── core/              # dependencies（RBAC + 服务注入）、error_handlers（错误码 → HTTP 映射）
└── tests/             # 契约测试 / 端点测试 / 服务层测试 / 引擎核心与 DDL / 消费者测试
```

## 接口契约（先契约后实现）

| 契约 | 内容 |
|------|------|
| `contracts/openapi/scene-service.yaml` | 12 个业务端点（含 G-20① 仿真进度/结果查询）+ 会话/错误/分页组件 + `x-hunter-*` 机器可校验扩展字段 |
| `contracts/openapi/scene-engine.yaml` | 28 个场景引擎端点（逻辑/生成/具体/判据用例/测试集/覆盖）+ 受控词表 + 错误响应组件 |
| `contracts/kafka/schemas/analytics_result.schema.json` | 唯一消费消息（实车场景自动提取，4.5 节） |
| `contracts/database/ddl/02_scene.sql` | `scene_svc.scenes`（元信息列 + `config_json`，软删除） |
| `contracts/database/ddl/07_scene_engine.sql` | `scene_engine` 服务私有 schema（7 表 + 触发器，幂等） |

关键映射：4.2.2 场景配置结构 = `SceneMeta`（→ `scenes` 列）+ `SceneConfig`（→ `config_json`）；
状态机 `draft → published → archived`（仅 `draft` 可编辑、`published` 可下发、`published` 需先归档再删除）。

## 测试

```bash
cd services/scene-service && pytest -q          # 222 项（场景引擎 108：核心 52 + DDL 10 + 端点 28 + 契约 18；其余为场景库/模板/导出/仿真/消费者/健康探针）
python -m pytest services/scene-service -q      # 仓库根目录同样可运行
```

## ⚠ 待人工确认（契约 `x-hunter-pending-confirmation`，12 项）

`scene_type` 编码值、~~4.4 节第 6/7 步端点缺口~~（已决策 G-20①：仿真进度/结果查询端点已交付）、
`archived` 无归档端点、导出返回形式与批量语义、
`version` 递增策略、Carla 管理 API 键名与子路径（`CARLA_INSTANCE_*`，含 G-20① 新增的
`CARLA_INSTANCE_GET_PATH`/`CARLA_INSTANCE_RESULT_PATH`）与上限、
实车提取触发源（`analytics_result` vs `event_raw`）、模板数据来源（当前为内置静态清单）、weather 取值范围。
实现侧已把上述决策点全部**环境变量化**，确认后仅调整配置即可，无需改代码。

