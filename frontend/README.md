# frontend — HunterEdge 运营管理后台（Vue 3）

技术栈：**Vue 3 + TypeScript + Vite 5 + Element Plus + Pinia + Vue Router 4 + ECharts 5 + Three.js**。

前端只通过 **api-gateway(8080)** 访问后端（REST `/api/v1/**` + WebSocket `/ws/remote/**`），
媒体面（WebRTC/SRTP）不经网关；所有后端地址、轮询间隔、限幅提示均来自环境变量（禁止硬编码）。

## 快速开始

```bash
cd frontend
npm install                 # 首次（如遇 npm 缓存目录权限问题：npm install --cache .npm-cache）
npm run dev                 # 开发服务器 http://localhost:5173（代理 /api、/ws → http://localhost:8080）
npm run type-check          # vue-tsc --noEmit（严格模式：noUnusedLocals/noImplicitAny）
npm run build               # type-check + 生产构建（dist/）
npm run preview             # 预览构建产物 http://localhost:4173
```

> 依赖：Node.js ≥ 20.19。开发环境需先启动网关与后端服务（见仓库根 README 的启动顺序）。

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `VITE_APP_TITLE` | HunterEdge 运营管理后台 | 应用标题（浏览器标签 + 顶栏） |
| `VITE_API_BASE_URL` | `/api/v1` | REST 基础路径（网关统一入口，相对路径） |
| `VITE_WS_BASE_URL` | `/ws` | WebSocket 基础路径（`/ws/remote/**`） |
| `VITE_PROXY_TARGET` | `http://localhost:8080` | 仅开发环境：Vite 代理目标（网关） |
| `VITE_DEV_PORT` | `5173` | 开发端口 |
| `VITE_POLL_DASHBOARD_MS` | `5000` | 看板轮询间隔 |
| `VITE_POLL_TELEMETRY_MS` | `2000` | 遥测轮询间隔（附录 D：`GET /data/telemetry` 单用户 20 QPS 下限 2000ms） |
| `VITE_POLL_SESSION_MS` | `1000` | 远程会话状态兜底轮询间隔 |
| `VITE_RC_MAX_SPEED_MPS` | `2.0` | 远程操控 UI 速度上限提示（真实限幅由服务端执行） |
| `VITE_RC_MAX_STEER_RAD` | `0.6` | 远程操控 UI 转角提示（⚠ 契约 pending #18） |
| `VITE_RC_HISTORY_MAX_RANGE_DAYS` | `31` | 操控历史查询最大跨度（契约 `RC_HISTORY_QUERY_MAX_RANGE_DAYS`） |

## 目录结构

```
frontend/
├── src/
│   ├── api/            # 按模块封装的 API（request/user/scene/data/analytics/ota/remote）
│   ├── components/     # 通用组件（common/ 卡片·标签·数据来源、charts/ ECharts 封装、three/ 场景预览）
│   ├── composables/    # usePolling（轮询：页面隐藏暂停 + 防请求堆积）
│   ├── constants/      # 运行时配置 + 受控词表（enums/data/analysis/ota/remote/permissions）
│   ├── directives/     # v-permission（服务端下发权限编码成员判定）
│   ├── layouts/        # MainLayout（侧边菜单 + 顶栏 + 内容区）
│   ├── router/         # 路由与守卫（登录态 + 页面权限）
│   ├── stores/         # Pinia：user（会话/RBAC）、vehicle（Redis 车辆读模型）
│   ├── types/          # 契约类型（字段名严格 snake_case，不做驼峰转换）
│   ├── utils/          # storage（Token）/ format / error-code / websocket / webrtc / hash
│   └── views/          # 五大模块页面 + 登录/错误页
├── .env.development / .env.production / .env.example
├── vite.config.ts / tsconfig.json / package.json
```

## 页面模块与契约映射

| 页面 | 路由 | 契约端点 |
|------|------|----------|
| 运营看板 | `/dashboard` | `GET /api/v1/analytics/dashboard`、`GET /api/v1/remote/vehicles` |
| 场景库 | `/scenes`、`/scenes/:scene_id` | `/api/v1/scene`（列表/CRUD/复制/发布/模板/导出/下发仿真） |
| 数据分析 | `/analytics` | `/api/v1/analytics/{dashboard,perception/eval,control/eval,scene/coverage,corner-cases,reports*}` |
| OTA 管理 | `/ota/versions`、`/ota/tasks`、`/ota/tasks/:task_id` | `/api/v1/ota/{versions*,tasks*}` |
| 远程操控 | `/remote/console`、`/remote/history` | `/api/v1/remote/{vehicles,session*,history*}` + WS `/ws/remote/{session_id}/{control,signal}` |
| 事件与文件 | `/data` | `/api/v1/data/{events*,files*}` |

## 关键实现约定

- **契约字段直传**：类型定义与响应字段**保持 snake_case**（`types/` 与 `contracts/openapi/*.yaml` 一一对应），禁止在 API 层做命名转换。
- **统一响应与错误码**：`api/request.ts` 统一解析 `ApiResponse{code,message,data,request_id,timestamp}`；`code≠0` 抛 `HunterApiError`；`1003` 单飞静默刷新后重放一次，`1001` 清理会话并广播失效事件（布局层展示后跳登录页）。
- **限流友好**：所有轮询间隔来自环境变量并预留余量（遥测 ≥ 2000ms）；页面隐藏（`visibilitychange`）自动暂停；请求未完成时跳过本轮。
- **远程操控安全**：控制指令 20Hz（50ms 固定节拍，`types/remote.ts` 帧格式与契约一致）；速度取会话响应 `control_channel.max_speed_mps` 限幅；心跳 10s；`estop` 帧立即下发；离开页面自动结束会话（释放车辆互斥锁）；关闭码 `1008/4001/4003/4010` 不重连。
- **视频**：原生 WebRTC（`utils/webrtc.ts`：recvonly + 信令 WS 中继 + `getStats()` 统计码率/丢包/RTT/抖动缓冲延迟），不使用任何第三方云视频服务。
- **权限**：`v-permission` 指令 + 路由 `meta.permission`（编码来自 `GET /user/me` → `permissions`，形如 `scene:read`，见 `constants/permissions.ts`）；前端隐藏仅影响展示，最终鉴权由网关与服务端 RBAC 执行。
- **Token 存储**：`localStorage`（无 HttpOnly Cookie 端点契约的已知折衷），日志与界面均不输出 Token；预签名 URL 不落缓存。

## 待人工确认（前端侧）

| # | 事项 | 前端现状 |
|---|------|----------|
| 1 | `OtaReleaseType` 契约未定义 enum（pending #2） | 类型为 `string`，仅原样展示，不做分支判断 |
| 2 | `RC_MAX_STEER_RAD` 底盘转角量程（pending #18） | UI 归一化用环境变量值，真实限幅由服务端执行 |
| 3 | WS 握手 JWT 传送方式（pending #20） | 采用子协议 `hunter-jwt`（常量 `WS_JWT_SUBPROTOCOL`），禁止查询串明文携带 Token |
| 4 | SRS 应用名/流名与 WHIP/WHEP 端点（pending #10） | 信令地址取会话响应 `webrtc.signal_ws_url`，缺失时按 `/ws/remote/{session_id}/signal` 拼装 |
| 5 | OTA 分片上传 complete 端点未在契约暴露 | 前端按 `upload.parts` 分片 PUT，完成汇总由服务端在 publish 阶段处理 |
| 6 | 场景事件 `trigger.condition` / `action.action_type` 取值域未定 | 表单按字符串透传，不做枚举校验 |

