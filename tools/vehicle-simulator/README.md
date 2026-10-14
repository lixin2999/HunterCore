# 内置车辆模拟器（tools/vehicle-simulator）

远程操控链路的**端到端演示 / 自测**工具：以「车端」身份接入平台 Kafka，驱动一维运动学模型，
按契约生产遥测/健康/指令回执，并向 SRS 推送合成视频，使浏览器可在**不依赖真实车辆**的情况下
完成「看板实时刷新 → 发起接管 → 视频出画 → 指令回执计数增长」的全链路验证。

## 数据链路

```
浏览器 WHEP 拉流 ◄── SRS ◄── RTMP/WHIP 合成视频 ◄──┐
                                                  │  vehicle-simulator（车端身份）
浏览器 WS 控制帧 ─► remote-control ─► Kafka ─► hunter.{vid}.remote_control ─► 运动学积分
                                                  │
                     hunter.{vid}.telemetry ─────┤ data-collector 入库 → 看板实时遥测
                     hunter.{vid}.health ────────┤ Redis 在线态 / 车辆状态读模型
                     hunter.{vid}.command_result ┘ remote-control ack → 已回执计数 / 控制时延
```

- Kafka 身份：SCRAM 用户名 = `vehicle_id`，`SASL_SSL` + `SCRAM-SHA-512`（严格 mTLS 时附客户端证书）。
- 消息 key 恒为 `vehicle_id`（契约：单车辆有序）。
- 生产消息严格按 `contracts/kafka/schemas/*.schema.json`（`additionalProperties=false`）构造，
  `tests/` 用真实 schema 自校验。

## 安装

```bash
cd tools/vehicle-simulator
pip install -e ".[dev]"          # 数据链路 + 单测
pip install -e ".[whip]"         # 可选：无 ffmpeg 时的 aiortc WHIP 推流回退
```

视频主路径依赖系统 `ffmpeg`（合成帧经管道编码为 H.264 后 RTMP 推流）；缺失且未装 `[whip]` 时
自动降级为「仅数据链路」，不影响 telemetry/health/command_result 自测。

## 运行

```bash
vehicle-simulator \
  --vehicle-id HUNTER-001 \
  --bootstrap <KAFKA_BOOTSTRAP> \
  --scram-pass <SCRAM 口令> \
  --security-protocol SASL_SSL \
  --ssl-cafile ca.pem --ssl-certfile client.crt --ssl-keyfile client.key \
  --srs-rtmp rtmp://<SERVER_IP>:1935/remote_control \
  --duration 0
```

常用参数（亦可用环境变量，前缀 `SIM_` / `KAFKA_`）：

| 参数 | 说明 |
| --- | --- |
| `--vehicle-id` | 车辆 ID（= SCRAM 用户名 = 消息 key） |
| `--bootstrap` | Kafka broker（逗号分隔），默认 `KAFKA_BOOTSTRAP_SERVERS` |
| `--scram-user` / `--scram-pass` | SCRAM 凭据（user 默认取 vehicle-id） |
| `--security-protocol` | `SASL_SSL`(默认) / `SSL` / `SASL_PLAINTEXT` / `PLAINTEXT` |
| `--ssl-cafile/--ssl-certfile/--ssl-keyfile/--ssl-keypassword` | mTLS 客户端证书 |
| `--srs-rtmp` | SRS RTMP 基址（视频主路径），如 `rtmp://IP:1935/remote_control` |
| `--srs-whip` | SRS WHIP 基址（无 ffmpeg 回退），如 `http://IP:1985/rtc/v1/whip/` |
| `--stream-prefix` | 流名前缀，须与 remote-control `RC_SRS_STREAM_PREFIX`（默认 `hunter`）一致 |
| `--telemetry-hz` / `--health-hz` | 遥测（默认 10Hz）/ 健康（默认 1Hz）频率 |
| `--duration` | 运行秒数，`0`=一直运行（Ctrl-C 退出） |
| `--no-video` / `--no-ack` | 关闭视频推流 / 关闭控制帧回执 |

> 流名 `hunter_{vehicle_id}_{session_id}` 与后端 `Settings.srs_stream_name()` 完全一致；
> `session_id` 由模拟器从消费的 boot 帧获取，故需先在控制台发起一次接管，模拟器才会开始推对应会话的视频。

## 端到端自测步骤

1. `docker compose` 起平台（Kafka / data-collector / remote-control / SRS / 前端）。
2. 启动本模拟器（用上表参数，`--no-video` 可先只验数据）。
3. 前端登录 → 运营看板：确认该车在线、遥测实时刷新、SOC/速度更新。
4. 进入操控台发起接管 → 模拟器收到 boot 帧开始推流 → 浏览器 WHEP 出画。
5. 打杆下发控制帧 → 「已回执」计数增长、控制时延出数；停止接管 → stop 帧使模拟器停推、回归 idle。

## 测试

```bash
pytest                      # 运动学 / 契约 schema 自校验 / 编排（内存桩 Kafka）
```

## 安全与限制

- 仅用于本地/staging 演示与自测，**不得**接入生产车队。
- 运动学为一阶惯性 + 简易自行车模型（非高保真动力学），量纲与契约对齐即可。
- 严格 mTLS 环境必须提供客户端证书，否则 broker 拒绝握手。
