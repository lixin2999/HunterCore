# api-gateway

API 网关：统一接入、JWT 认证鉴权、五级限流熔断、路由转发、日志审计

- 端口：**8080**（环境变量 `API_PORT` 可覆盖）
- 技术栈：Python 3.11+ / FastAPI / PyJWT / bcrypt / httpx / pydantic-settings / SQLAlchemy 2.0 (asyncpg)
- 健康探针：`GET /healthz`（存活）、`GET /readyz`（就绪，含 DB/Redis 检查）
- 指标端点：`GET /metrics`（Prometheus 文本格式，由 `infra/monitoring` 抓取）

## 接口契约

`contracts/openapi/api-gateway.yaml`（OpenAPI 3.0.3）是网关的单一事实来源：

- 自持端点（已实现）：
  - `POST /api/v1/user/login` — 登录（bcrypt 校验 + 可选 MFA(TOTP) 载荷 + IP 防暴力锁定）
  - `POST /api/v1/user/refresh` — 刷新（一次一换，旧 Refresh Token 立即作废防重放）
  - `POST /api/v1/user/logout` — 注销（幂等；会话撤销即刻失效旧 Access/Refresh）
  - `GET /api/v1/user/me` — 当前用户（JWT 解析结果 + 用户资料，前端 `v-permission` 依据）
  - `POST /api/v1/user/change-password` — 改密（G-06 首登强制改密）
  - `POST /api/v1/user/service-token` — 签发服务令牌（脚本/仿真等非交互组件，仅限授权角色）
  - `DELETE /api/v1/user/service-token/{client_id}` — 撤销服务令牌（即时失效、幂等）
- 运维端点（已实现）：`/healthz`、`/readyz`、`/metrics`
- 转发端点（已实现）：根级 `x-hunter-gateway-routes` 声明 7 个路由前缀 → 目标服务与端口
  （各服务资源端点见其自身契约）；转发注入 `X-User-Id` / `X-Roles` / `X-Trace-Id`
  （覆盖客户端同名头防伪造），并剥离客户端 `Authorization`
- 系统契约：`x-hunter-rate-limits`（附录 D 五级限流）、`x-hunter-websocket-routes`（`/ws/remote/**`）、
  `x-hunter-audit-log`、`x-hunter-kafka`（网关不参与 Kafka）、`x-hunter-error-status-map`、
  `x-hunter-device-ingress`（车端 mTLS）、`x-hunter-service-token`（服务令牌）

> 修改接口前必须先更新契约（含 ⚠ `pending_confirmation` 项需人工确认后再实现），再改代码。

## 统一认证实现要点

- **口令**：bcrypt 哈希（`user_svc.users.password_hash`；轮次 `PASSWORD_BCRYPT_ROUNDS=12`）
- **JWT**（设计文档 3.2.2 节）：载荷 `sub/username/roles/permissions/iat/exp/iss/jti`；
  Access 60min（V1.27.1：G-04 决策① 原 30min，仍短于原 2h）+ Refresh 7d；算法白名单（`HS256`），`iss=hunter-platform`
- **会话**：Redis `session:{user_id}` = Access Token 原样存储，TTL 3600s
  （redis-keys.yaml 第 1 条，owner=api-gateway）；**强依赖** —— Redis 不可用统一 503 + code=5001，禁止降级
- **撤销语义**（redis-keys.yaml pending #6 选项②）：不引入黑名单键 —— 登录写入 / 刷新覆盖 /
  登出 DEL 会话；`get_current_user` 校验「会话存在且与当前 Token 全等」，旧 Token 即刻失效
- **防重放**：Refresh Token 绑定配对 Access Token 的 `at_jti`；刷新后旧 Refresh 重放 → 1003
- **防枚举**：登录失败（用户名不存在/密码错误/账号非启用）统一 401 + code=1001
- **IP 锁定**（设计文档 14.5 节）：失败计数键 `rate_limit:{ip}:login-fail`（复用契约键模式），
  达阈值锁定，期间任何尝试（含正确密码）429 + Retry-After；另有登录频率限制 `rate_limit:{ip}:login`
- **审计**：登录成功/失败、登出、限流触发均落结构化日志（密码/Token 永不入日志）

## 服务令牌（V1.23.6，契约 `x-hunter-service-token`）

**解决的问题**：`session:{user_id}` 为单值会话，同一账号再次登录会覆盖旧值 → 旧 Token 立即失效。
仿真运行期间若再用 admin 登录 Web 前端，HunterSim 持有的 `HUNTER_CORE_JWT_TOKEN` 会立刻失效。

**方案**：为非交互组件（脚本/仿真 bag 上传）签发独立 Bearer Token，会话键与之隔离：

| 维度 | 用户 Access Token | 服务令牌 |
|------|------------------|----------|
| `typ` | `access` | `service` |
| `sub` | `user_id` | `service:{client_id}` |
| 会话键 | `session:{user_id}`（3600s） | `session:service:{client_id}`（默认 604800s） |
| 生效范围 | 全部控制台端点 + 转发 | 仅 `data-collector` 上传端点（presign/complete） |
| 失效路径 | 重新登录 / 刷新 / 登出 | 重新签发（轮换）/ DELETE 撤销 / 到期 |

- 签发/撤销需控制台 Bearer + `SERVICE_TOKEN_ISSUE_ROLES`（默认 admin），否则 403 + 1002
- 服务角色默认 `data-uploader`（`SERVICE_TOKEN_ROLE`），**不**加入 `data:read`/`data:execute` 白名单
- `get_current_user` 仅接受 `typ=access` → 服务令牌访问 `/me`、`/logout`、`/refresh`、`/change-password`、
  再签发一律 401 + 1001（避免权限升级链）；catch-all 转发接受 `access|service`
- 用户登录/刷新/登出只操作 `session:{user_id}` → 管理员重新登录**不会**使服务令牌失效（两模块并行）

```bash
# 1) 管理员登录取控制台 Token
TOKEN=$(curl -s -X POST http://localhost:8080/api/v1/user/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"<ADMIN_PASSWORD>"}' | jq -r .data.access_token)
# 2) 签发服务令牌（同一 client_id 重复签发即轮换）
curl -s -X POST http://localhost:8080/api/v1/user/service-token \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"client_id":"hunter-sim"}' | jq -r .data.access_token   # → HUNTER_CORE_JWT_TOKEN
# 3) 撤销（令牌即刻失效；用户会话不受影响）
curl -s -X DELETE http://localhost:8080/api/v1/user/service-token/hunter-sim \
  -H "Authorization: Bearer $TOKEN"
```

## 五级限流（设计文档附录 D，阈值不可更改）

| 维度 | 阈值 | 键 |
|------|------|----|
| 全局 | 10000 QPS | 集群级（Ingress/Nginx 入口承担，进程内不重复计数） |
| 单用户 | 100 QPS | `rate_limit:{user_id}:global` |
| 单 IP | 200 QPS | `rate_limit:{ip}:global` |
| POST /api/v1/ota/versions | 5 QPS（user） | `rate_limit:{user_id}:ota-versions` |
| POST /api/v1/remote/session | 1 QPS（user） | `rate_limit:{user_id}:remote-session` |
| GET /api/v1/data/telemetry | 20 QPS（user） | `rate_limit:{user_id}:data-telemetry` |

- 算法：Redis 固定窗口（INCR + EXPIRE(NX)，窗口 1s，V1.21.9 从 60s 修正为 1s 以匹配 QPS 单位）
- 超限：HTTP 429 + `Retry-After` / `X-RateLimit-Limit|Remaining|Reset`；响应体 `code` 复用 5001（附录 A 无专用错误码）
- 可用性：`rate_limit` 非强依赖 → Redis 故障 fail-open 放行并告警（session 校验为强依赖 fail-closed）
- 探针与 `/metrics`/`/docs` 豁免限流；CORS 预检（OPTIONS）豁免

## 路由转发

- 路由表（`x-hunter-gateway-routes.routes`，前缀不可更改）：scene→8081 / data→8082 /
  analytics→8083 / ota→8084 / remote→8085 / vehicle→8086（新交付）；`/api/v1/user`
  除网关自持认证端点外归属待确认（不配置 URL → 503 + 5001）
- `strip_prefix=false`：转发完整路径；请求体**流式转发**（支持 OTA 大包上传），响应透传
  （剥离逐跳头，保留 content-length/encoding 语义）
- 熔断（契约 circuit_breaker 默认策略，阈值经 env 调整）：连续 5 次失败或窗口超时率 > 50%
  → 熔断 30s → 半开放行单探测；熔断/连接失败/超时统一 503 + code=5001，禁止透传裸错误
- 路由表外未知路径 → 404 + code=3001（统一响应体）
- catch-all 代理注册为 `include_in_schema=False`（转发资源定义在各服务契约，保证
  「实现路由 ⊆ 契约」校验成立）

## 配置（环境变量，全部可覆盖）

| 变量 | 默认 | 说明 |
|------|------|------|
| `SCENE_SERVICE_URL` 等五个 | `http://localhost:<port>` | 转发目标（K8s 内为 Service DNS） |
| `VEHICLE_SERVICE_URL` | `http://localhost:8086` | 车辆台账服务（新交付） |
| `USER_SERVICE_URL` | 未配置 | pending_confirmation，未配置返回 503 |
| `PROXY_CONNECT_TIMEOUT_SECONDS` / `PROXY_TIMEOUT_SECONDS` | 5 / 30 | 转发超时 |
| `CIRCUIT_BREAKER_FAILURE_THRESHOLD` / `_TIMEOUT_RATE` / `_OPEN_SECONDS` | 5 / 0.5 / 30 | 熔断参数 |
| `RATE_LIMIT_GLOBAL_QPS` / `_USER_QPS` / `_IP_QPS` | 10000 / 100 / 200 | 附录 D（不可更改） |
| `LOGIN_RATE_LIMIT_PER_IP` / `LOGIN_MAX_FAILURES` / `LOGIN_LOCK_WINDOW_SECONDS` | 30 / 5 / 900 | 登录防护（待人工确认） |
| `JWT_ISSUER` | hunter-platform | JWT 签发方 |
| `PASSWORD_BCRYPT_ROUNDS` | 12 | bcrypt 轮次 |
| `TRUST_FORWARDED_FOR` | true | 是否信任 `X-Forwarded-For`（直连部署建议 false） |
| `SERVICE_TOKEN_TTL_SECONDS` | 604800 | 服务令牌默认有效期（7 天） |
| `SERVICE_TOKEN_MAX_TTL_SECONDS` | 2592000 | 签发请求 `ttl_seconds` 上限（30 天） |
| `SERVICE_TOKEN_ROLE` | data-uploader | 服务令牌角色（需与 data-collector `UPLOAD_SERVICE_ROLE` 同名同值） |
| `SERVICE_TOKEN_ISSUE_ROLES` | admin | 允许签发/撤销服务令牌的控制台角色 |
| `JWT_SECRET_KEY`（继承共享配置） | 必须覆盖 | 生产经 K8s Secret 注入 |

## 本地运行

```bash
pip install -e "common/python"     # 仓库根目录：共享库
pip install -e "services/api-gateway[dev]"
cd services/api-gateway
uvicorn app.main:app --reload --port 8080
```

登录前需 `user_svc.users` 存在用户（bcrypt 哈希）与 RBAC 五表数据；
本地可用 `docker compose up -d postgres redis` 后经 Alembic 初始化（见根 README 快速开始）。

## 测试

```bash
cd services/api-gateway && pytest -q
# 仓库根目录亦可：pytest services/api-gateway -q
```

- `app/tests/test_health.py`：探针 / 统一响应 / trace_id / 指标端点
- `app/tests/test_openapi_contract.py`：契约 ↔ 实现 ↔ K8s 清单三方一致性（含服务令牌护栏）
- `app/tests/test_auth.py`：登录/刷新轮换防重放/注销幂等/会话强依赖 503/参数校验/IP 锁定
- `app/tests/test_service_token.py`：服务令牌签发/并行（再次登录不掉线）/撤销/轮换/能力面收窄/转发身份头
- `app/tests/test_rate_limit.py`：单 IP / 单用户 / 接口级限流、ops 豁免、fail-open、键模式
- `app/tests/test_proxy.py`：路径与查询透传、身份头注入与覆盖、Authorization 剥离、
  未知前缀 404、pending 前缀 503、熔断、上游错误透传

## ⚠ 待人工确认项（实现前已按契约保守处理）

1. **MFA(TOTP)**：`user_svc.users` 无 TOTP 密文字段（数据库契约未定义）——当前所有账号视为
   未启用 MFA，`totp_code` 按契约「未启用时忽略」处理；启用前须先扩展 DDL 与 OpenAPI 契约
2. **Refresh Token 防重放**：采用会话绑定（pending #6 选项②）实现「一次一换」；
   如需独立黑名单键须先更新系统约束第 8 条与 redis-keys.yaml
3. **单会话语义**：`session:{user_id}` 为单值模型，二次登录覆盖前一会话（多设备互踢）——
   该语义**仅作用于用户账号**；非交互组件请用服务令牌（`session:service:{client_id}`，
   V1.23.6），两者互不影响。多终端并存仍需契约扩展
4. **`/user/me` 的 `real_name` 降级**：DB 异常时返回 `real_name=null`（契约允许 503，
   这里选择部分降级保可用性）
5. **登录防护阈值**（30 次/分钟、5 次锁定/15 分钟）与熔断参数为契约建议默认值，待 14.5 节确认
6. **`/ws/remote/**` WebSocket 转发**：未实现（契约 auth 传送方式待确认）；REST 转发已就绪
7. **全局 10000 QPS**：由 Ingress/Nginx 入口承担（进程内不重复计数避免 Redis 热点）
