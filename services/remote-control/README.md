# remote-control

远程操控：WebRTC 视频流转发、控制指令转发（20Hz）、操作员权限管理、操控会话与录像记录

- 端口：**8085**（环境变量 `API_PORT` 可覆盖）；网关前缀 `/api/v1/remote`（不剥离前缀）+ `/ws/remote/**`（WebSocket 升级）
- 技术栈：Python 3.11+ / FastAPI / pydantic-settings / SQLAlchemy 2.0 (asyncpg) / Redis / Kafka / MinIO / SRS 5.0 + WebRTC
- 健康探针：`GET /healthz`（存活）、`GET /readyz`（就绪，含 DB/Redis 检查）
- 指标端点：`GET /metrics`（Prometheus 文本格式，由 `infra/monitoring` 抓取）

## 接口契约（单一事实来源）

`contracts/openapi/remote-control.yaml`（OpenAPI 3.0.3）—— 开发顺序：先改契约，再改实现。
⚠ 设计文档 §12.6 / §8.3–8.5 原文未随仓库提供，端点清单为**推导集合**，依据逐条登记在
契约 `x-hunter-endpoints.items[].source`（附录 D 明列端点 / 网关 `x-hunter-websocket-routes` /
第 15 条安全约束 / MinIO `hunter-video` 录像）。

| 端点 | 说明 |
|------|------|
| `GET /api/v1/remote/vehicles` | 可操控车辆列表（在线 + 状态允许，来自 Redis 读模型） |
| `GET /api/v1/remote/sessions` | 当前进行中的操控会话列表（Redis 扫描 `rc:session:*`） |
| `POST /api/v1/remote/session` | 创建会话（**1 QPS/用户**、同车互斥 → 7001） |
| `GET /api/v1/remote/session/{session_id}` | 会话状态与实时统计（指令计数 / 视频质量） |
| `DELETE /api/v1/remote/session/{session_id}` | 结束会话（执行类动作，记录与录像保留） |
| `GET /api/v1/remote/history` | 操控记录列表（MinIO 前缀枚举 + sidecar JSON，日期范围必填） |
| `GET /api/v1/remote/history/{session_id}` | 单场操控记录详情（sidecar JSON） |
| `GET /api/v1/remote/history/{session_id}/video` | 录像下载/播放地址（**15 分钟预签名 + Range**） |
| `WS /ws/remote/{session_id}/control` | 20Hz 指令上行 + ack/status/error 下行 + 10s 心跳 |
| `WS /ws/remote/{session_id}/signal` | WebRTC SDP/ICE 信令中继（服务端不解析媒体） |

设计文档章节 ↔ 实现落位：

- **15 条安全约束（不可更改）**：指令 20Hz（50ms）→ Kafka `hunter.{vehicle_id}.remote_control`
  （`acks=all`、`key=vehicle_id`、lz4）；>500ms 无指令 → 车端自动减速停车（平台侧会话降级，**执行主体为车端**）；
  速度上限 2.0 m/s（`RC_MAX_SPEED_MPS`，服务端对 `target_velocity` 限幅）；同车同一时间仅一名操作员
  （Redis 分布式锁 `rc:lock:{vehicle_id}` + 错误码 7001）；视频 H.264（AGX Orin NVENC）/ 720p@30fps /
  2048–4096 kbps / 关键帧 1s；延迟预算 采集编码 ≤50ms + 网络 ≤100ms + 解码渲染 ≤30ms = 视频 E2E ≤200ms，指令 ≤100ms
- **会话生命周期**：`connecting → active → degraded → ended`；心跳 10s、连续缺失 3 次降级、60s 超时结束；
  结束收敛顺序 **session_end → 停录像 → 封存 sidecar → 释放锁 → 删 Redis 键**（不依赖车端回执）
- **操控记录（方案 A，已确认）**：**不新增数据库表 / ORM / Alembic** —— 会话态唯一来源 Redis
  `rc:session:{vehicle_id}`；操控记录 = MinIO `hunter-video` 录像
  `remote-control/{vehicle_id}/{yyyy}/{mm}/{dd}/{session_id}.mp4` + 同目录 sidecar JSON（保留 90 天）；
  代价：历史查询无 SQL 过滤/聚合能力（须按日期前缀收敛，`RC_HISTORY_QUERY_MAX_RANGE_DAYS` 默认 31，超限 2001）
- **媒体面**：SRTP/UDP **不经** api-gateway/Ingress，须在 K8s 单独暴露 UDP（端口与 SRS 参数见 pending #10 / #12）
- **Kafka**：生产 `hunter.{vehicle_id}.remote_control`（20Hz 指令）与 `hunter.{vehicle_id}.command`
  （会话开始/结束信令，`command_type` 取值待 pending #17 定稿）；消费 `hunter.*.command_result`
  （消费组 `remote-control-command-result`，正则订阅、手动提交、异常进 DLQ `{topic}.dlq`）；
  ⚠ 因 `remote_control.schema.json` **无 `command_id`**，回执只能按 `(session_id, seq, 时序窗口)` 近似关联（pending #16）
- **数据访问边界**：`remote_control` schema 为预留（无业务表），不跨服务直连 DB；Redis 只读
  `vehicle:status:{vehicle_id}` / `vehicle:online:set`，写 `rc:session:{vehicle_id}` / `rc:lock:{vehicle_id}`

> ⚠ 待人工确认 **20 项**（其中 `blocking=true` 8 项：#2 Redis 车辆状态字段清单 / #10 SRS 应用名与流名 /
> #12 新增环境变量同步 K8s / #14 多副本 WS 故障转移与粘性路由 / #15 WS 子路径拆分 /
> #16 操控指令 `command_id` 缺失 / #17 `command_type` 取值 / #20 WS 握手 JWT 传送方式），
> 全量见契约 `x-hunter-pending-confirmation`。

## 本地运行

```bash
pip install -e "common/python"     # 仓库根目录：共享库
pip install -e "services/remote-control[dev]"
cd services/remote-control
uvicorn app.main:app --reload --port 8085
```

## 测试

```bash
cd services/remote-control && pytest -q
```

> 骨架层级（L0）：业务逻辑按 `contracts/` 契约逐层实现。
