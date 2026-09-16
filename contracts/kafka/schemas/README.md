# contracts/kafka/schemas — 消息 JSON Schema

规范：**JSON Schema draft-07**，文件名 = 消息名 + `.schema.json`，`$id` 固定为
`https://hunter-edge.local/contracts/kafka/schemas/<name>.schema.json`，每个 Schema 自带 `examples`
（设计文档原例），由 `scripts/verify_data_layer.py` 与
`common/python/tests/test_kafka_contracts.py` 校验：Schema 合法 + examples 通过自身校验 + required/enum 完整。

## 文件清单

| Schema | 对应 Topic | 关键约束 |
|--------|-----------|---------|
| `telemetry.schema.json` | `hunter.{vehicle_id}.telemetry`、`telemetry_raw`、`telemetry_clean` | 六段结构（chassis/localization/perception/planning/control/system）全部必填；`battery_soc` 0-100；`motor_*`/`linear_velocity`/`angular_velocity` 为定长或定序数组 |
| `event.schema.json` | `hunter.{vehicle_id}.event`、`event_raw` | `event_type` 18 种枚举、`event_level` 3 级枚举；类型↔等级映射由消费侧 `EVENT_LEVEL_BY_TYPE` 校验 |
| `health.schema.json` | `hunter.{vehicle_id}.health` | `status` 8 态枚举；`system` 段与遥测一致 |
| `command.schema.json` | `hunter.{vehicle_id}.command`、`hunter.broadcast.command` | `command_id` UUID 幂等键；单车辆 `vehicle_id` 与广播 `target_filter` 二选一（`oneOf`） |
| `command_result.schema.json` | `hunter.{vehicle_id}.command_result` | `command_id` 回带关联请求；`success` 必填 |
| `ota_notify.schema.json` | `hunter.{vehicle_id}.ota_notify` | `package_md5`（32 位 hex）/`package_sha256`（64 位 hex）/`signature` 必填；`preconditions` 含 4 项门禁 |
| `ota_status.schema.json` | `hunter.{vehicle_id}.ota_status` | `status`/`phase` = OTA 状态机 9 态；`progress` 0-100 |
| `remote_control.schema.json` | `hunter.{vehicle_id}.remote_control` | 20Hz；`seq` 单调递增；`target_velocity` ∈ [-2.0, 2.0] m/s；`heartbeat` 用于超时保护计时 |
| `analytics_result.schema.json` | `analytics_result` | `result_type` ∈ {metric, corner_case, report}；`trigger_event_type` 取受控事件词表（18 种）；`clip.pre_seconds/post_seconds` 固定 10（4.5 节事件前后各 10 秒）；含 `ego_trajectory`/`objects`/`environment` |
| `sensor_file.schema.json` | `sensor_file` | 5.5 节上传完成通知（生产者 data-collector）；`bucket` ∈ 3 个车端上传类 Bucket；`object_key` 强制命名规范 `{bucket}/{vehicle_id}/{date}/{data_type}/{timestamp}_{seq}.{ext}`；`md5`(32)/`sha256`(64) 与 MinIO 校验一致；幂等键 = `object_key` |
| `alert_event.schema.json` | `alert_event` | 6.2 节 Flink 实时告警（生产者 data-analytics）；`alert_type` 取 18 种受控事件类型、`level` 必须 = `EVENT_LEVEL_BY_TYPE[alert_type]`、`source_job` 取 5 个实时作业；`rule` 回带命中规则/比较符/阈值（阈值环境变量化）；幂等键 = `(vehicle_id, alert_id)`、延迟 ≤ 2s |

## 通用约定（所有 Schema）

- `additionalProperties: false`：新增字段必须先改契约，禁止「先发后补」字段
- 时间统一为 Unix epoch 秒（`number`，含毫秒小数），禁止字符串时间与本地时区
- `vehicle_id` 同时是 Kafka 消息 key（`hunter.broadcast.command` 除外，key 为空）
- 不在消息中传输敏感信息（Token/证书私钥/密码），审计仅记录操作者 ID
- 平台内部 Topic 复用关系：`telemetry_raw`/`telemetry_clean` → `telemetry.schema.json`；
  `event_raw` → `event.schema.json`
- 变更：`analytics_result` 已补全 Schema（scene-service 实车场景自动提取依赖，设计文档 4.5 节）；
  `sensor_file` 已补全 Schema（data-collector 文件上传完成通知，设计文档 5.5 节）；
  `alert_event` 已补全 Schema（data-analytics Flink 实时告警，设计文档 6.2 节）
  → **11 个 Schema 全部落地，`topics.yaml` 中已无 `schema: null`**

## ⚠ 待设计文档核对项

| 项 | 现状 |
|----|------|
| `alert_event` 字段全集 | 已按 6.2 节作业规则定义（vehicle_id/alert_id/alert_type/level/source_job/event_time/timestamp/rule/metrics/location），需与设计文档 5.3 节逐字段核对 |
| `alert_event.alert_type` 取值域 | 复用受控事件类型 18 种（不新增）；6.2.3 节 TTC<3.0s 的 warning 预警无对应类型 → 走 `analytics_result`（待确认，见 data-analytics 契约 pending #3） |
| `sensor_file` 字段全集 | 已按 5.5 节上传流程定义（bucket/object_key/data_type/size/md5/sha256 + 可选 seq/etag/content_type），需与设计文档 5.5 节逐字段核对 |
| `analytics_result` 字段全集 | 已按 4.5 节需求定义（触发事件 + 截取窗口 + 轨迹/环境），`result_type=metric|report` 的其余字段待 5.3 节确认 |
| `analytics_result.environment.road_type` | 无 enum（取值域待确认） |
| `command.command_type` 取值域 | 未设 enum，仅字符串 |
| `health` 扩展字段（磁盘、服务进程、传感器健康） | 未定义，当前仅 status + system |
| `telemetry.chassis.control_mode` / `vehicle_state` / `planning.current_behavior` 取值域 | 未设 enum |
