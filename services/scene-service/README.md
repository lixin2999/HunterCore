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

Kafka：**不生产任何 Topic**（契约 `x-hunter-kafka.produces = []`）；仅消费 `analytics_result`
（消费组 `scene-service-analytics-result`，4.5 节实车场景自动提取：`harsh_braking` /
`collision_warning` / `manual_takeover` 触发，事件前后各 10 秒，输出 `real_vehicle_replay` + `draft`，
幂等键 `(vehicle_id, window_start)`，异常消息转投 `analytics_result.dlq`）。

## 分层结构

```
app/
├── main.py            # 应用入口：lifespan 装配（DB/Redis/MinIO/Carla/服务/消费者）+ 路由注册
├── config.py          # pydantic-settings（契约 required_env 一一对应，禁止硬编码）
├── routers/           # scenes / templates / export / simulation + health + errors（统一错误响应声明）
├── schemas/           # common（统一响应/探针/分页边界）+ scene（4.2.2 结构与请求/响应）
├── services/          # scenes（CRUD/复制/发布）、templates、serializers（4.3 节序列化）、export、simulation
├── repositories/      # scenes（SQLAlchemy）、cache（Redis）、storage（MinIO）、carla（httpx）
├── consumers/         # analytics_result（实车场景自动提取，4.5 节）
├── core/              # dependencies（RBAC + 服务注入）、error_handlers（错误码 → HTTP 映射）
└── tests/             # 契约测试 / 端点测试 / 服务层测试 / 消费者测试
```

## 接口契约（先契约后实现）

| 契约 | 内容 |
|------|------|
| `contracts/openapi/scene-service.yaml` | 12 个业务端点（含 G-20① 仿真进度/结果查询）+ 会话/错误/分页组件 + `x-hunter-*` 机器可校验扩展字段 |
| `contracts/kafka/schemas/analytics_result.schema.json` | 唯一消费消息（实车场景自动提取，4.5 节） |
| `contracts/database/ddl/02_scene.sql` | `scene_svc.scenes`（元信息列 + `config_json`，软删除） |

关键映射：4.2.2 场景配置结构 = `SceneMeta`（→ `scenes` 列）+ `SceneConfig`（→ `config_json`）；
状态机 `draft → published → archived`（仅 `draft` 可编辑、`published` 可下发、`published` 需先归档再删除）。

## 测试

```bash
cd services/scene-service && pytest -q          # 108 项（契约 29 + 端点 50 + 服务层 15 + 消费者 10 + 健康探针 4）
python -m pytest services/scene-service -q      # 仓库根目录同样可运行
```

## ⚠ 待人工确认（契约 `x-hunter-pending-confirmation`，12 项）

`scene_type` 编码值、~~4.4 节第 6/7 步端点缺口~~（已决策 G-20①：仿真进度/结果查询端点已交付）、
`archived` 无归档端点、导出返回形式与批量语义、
`version` 递增策略、Carla 管理 API 键名与子路径（`CARLA_INSTANCE_*`，含 G-20① 新增的
`CARLA_INSTANCE_GET_PATH`/`CARLA_INSTANCE_RESULT_PATH`）与上限、
实车提取触发源（`analytics_result` vs `event_raw`）、模板数据来源（当前为内置静态清单）、weather 取值范围。
实现侧已把上述决策点全部**环境变量化**，确认后仅调整配置即可，无需改代码。

