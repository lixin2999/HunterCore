# contracts/database/enums.md — 受控词表（不可新增/更改）

以下词表为全系统受控枚举，必须与 `common/python/hunter_common/database/enums.py` 的 `StrEnum` 成员
逐字一致（单元测试 `test_database_models.py::test_enum_contract_matches_orm` 强制校验）。
业务代码禁止硬编码字符串字面量，必须引用枚举成员。

## 1. 车辆状态 `VehicleStatus`（vehicles.status / Redis vehicle:status:{vehicle_id}）

| 值 | 含义 | ORM 成员 |
|----|------|---------|
| `offline` | 离线 | `VehicleStatus.OFFLINE` |
| `online_idle` | 在线空闲 | `VehicleStatus.ONLINE_IDLE` |
| `auto_driving` | 自动驾驶中 | `VehicleStatus.AUTO_DRIVING` |
| `remote_controlled` | 远程操控中 | `VehicleStatus.REMOTE_CONTROLLED` |
| `upgrading` | OTA 升级中 | `VehicleStatus.UPGRADING` |
| `charging` | 充电中 | `VehicleStatus.CHARGING` |
| `fault` | 故障 | `VehicleStatus.FAULT` |
| `emergency` | 紧急状态 | `VehicleStatus.EMERGENCY` |

## 2. 事件等级 `EventLevel`（events.event_level）

| 值 | 含义 | ORM 成员 |
|----|------|---------|
| `info` | 提示 | `EventLevel.INFO` |
| `warning` | 告警 | `EventLevel.WARNING` |
| `critical` | 严重 | `EventLevel.CRITICAL` |

## 3. 事件类型 `EventType`（events.event_type，19 种；触发阈值不可更改）

| 值 | 等级 | 触发条件（设计文档） |
|----|------|---------------------|
| `harsh_acceleration` | warning | 加速度 a > 3 m/s² |
| `harsh_braking` | warning | 减速度 > 3 m/s² |
| `harsh_turning` | warning | 横摆角速度 > 0.8 rad/s |
| `over_speed` | critical | 超速 > 10% |
| `collision_pre_warning` | warning | TTC < 3.0 s（G-22② 新增：低一级的碰撞预警） |
| `collision_warning` | critical | TTC < 1.5 s |
| `manual_takeover` | info | 人工接管 |
| `emergency_stop` | critical | 紧急停车 |
| `battery_low` | warning | SOC < 20% |
| `battery_critical` | critical | SOC < 10% |
| `communication_loss` | critical | 遥测中断 > 10 s |
| `sensor_fault` | critical | 传感器故障 |
| `perception_fault` | critical | 感知模块故障 |
| `planning_fault` | critical | 规划模块故障 |
| `control_fault` | critical | 控制模块故障 |
| `ota_start` | info | OTA 升级开始 |
| `ota_success` | info | OTA 升级成功 |
| `ota_failed` | critical | OTA 升级失败 |
| `ota_rollback` | warning | OTA 回滚 |

> 等级映射由 `EventLevelForType`（`enums.py`）提供，落库前校验事件类型与等级组合；
> 表中等级为契约值，任何代码放宽阈值或改等级视为契约违规。
>
> G-22② 决策：`collision_pre_warning`（warning，TTC<3.0s）与 `collision_warning`（critical，TTC<1.5s）
> 分级，消解设计文档 6.2.3 节 3.0s 预警与旧受控词表的等级冲突（见 data-analytics 契约 pending #3 结案）。

## 4. OTA 状态机 `OtaStatus` / `OtaPhase`（ota_records.status / ota_records.phase / Kafka ota_status）

流转（不可更改）：`IDLE → PENDING → DOWNLOAD → INSTALL → TEST → SUCCESS`；
`TEST` 自检失败 → `ROLLBACK` → `ROLLED_BACK`（或 `FAILED`）。

| 值 | 含义 | 是否终态 |
|----|------|---------|
| `IDLE` | 空闲（无进行中任务） | 否 |
| `PENDING` | 待执行（门禁校验中） | 否 |
| `DOWNLOAD` | 下载升级包 | 否 |
| `INSTALL` | 写入非活动分区 | 否 |
| `TEST` | 自检（新分区启动验证） | 否 |
| `SUCCESS` | 升级成功 | 是 |
| `ROLLBACK` | 回滚中（A/B 分区切回） | 否 |
| `ROLLED_BACK` | 已回滚 | 是 |
| `FAILED` | 升级失败 | 是 |

车端升级状态（OtaStatus）之外，版本仓库另有独立状态机 `OtaVersionStatus`（ota_versions.status，
G-18 决策②定稿，设计文档 §7.2.2 审核流）：

流转（不可更改）：`draft → testing → reviewing → published → deprecated / disabled`；
`reviewing` 审核驳回回退 `draft`；发布（publish）前置状态仅 `reviewing`。

| 值 | 含义 | 是否终态 |
|----|------|---------|
| `draft` | 草稿（已创建待直传升级包） | 否 |
| `testing` | 测试中（包已直传，待提交审核） | 否 |
| `reviewing` | 审核中（待发布批准/驳回） | 否 |
| `published` | 已发布（可被升级任务引用） | 否 |
| `deprecated` | 已废弃（历史可查，不可再下发） | 是 |
| `disabled` | 已停用（紧急止血，不可再下发） | 是 |

## 5. 算法模块 `MetricModule`（algorithm_metrics.module）

| 值 | ORM 成员 |
|----|---------|
| `perception` | `MetricModule.PERCEPTION` |
| `planning` | `MetricModule.PLANNING` |
| `control` | `MetricModule.CONTROL` |

## 6. 场景状态 `SceneStatus`（scenes.status）

| 值 | 含义 | 可编辑 | ORM 成员 |
|----|------|--------|---------|
| `draft` | 草稿 | 是 | `SceneStatus.DRAFT` |
| `published` | 已发布 | 否 | `SceneStatus.PUBLISHED` |
| `archived` | 已归档 | 否 | `SceneStatus.ARCHIVED` |

## 7. OTA 任务状态 `OtaTaskStatus`（ota_tasks.status，⚠ 取值域需与设计文档核对）

| 值 | 含义 |
|----|------|
| `created` | 已创建（待提交审批） |
| `pending_approval` | 待审批 |
| `running` | 执行中（灰度批次推进） |
| `paused` | 已暂停（批次成功率 < 95% 或人工暂停） |
| `succeeded` | 全部批次成功 |
| `failed` | 失败 |
| `canceled` | 已取消 |

## 8. 用户状态 / 角色状态（user_svc）

| 表 | 字段 | 取值 |
|----|------|------|
| `users` | `status` | `enabled` / `disabled` / `locked`（⚠ 需核对） |
| `roles` | `status` | `enabled` / `disabled` |
| `permissions` | `resource` | `scene` / `data` / `analytics` / `ota` / `remote` / `vehicle` / `user`（与网关路由表资源域一致） |
| `permissions` | `action` | `create` / `read` / `update` / `delete` / `execute` |

## 9. 尚未定稿的取值域（⚠ 需设计文档核对，禁止前端/后端硬编码分支）

| 字段 | 现状 | 待确认 |
|------|------|--------|
| `scenes.scene_type` | TEXT，无 CHECK | 场景分类枚举值 |
| `ota_versions.release_type` | TEXT，无 CHECK | 发布类型枚举值 |
| `ota_versions.status` | 已定稿（G-18②）：CHECK 六值 draft/testing/reviewing/published/deprecated/disabled，见 §4 | — |
| `data_collector.vehicle_telemetry.control_mode` | TEXT（遥测示例值 `CAN`） | 完整取值域 |
| `data_collector.vehicle_telemetry.vehicle_state` | TEXT（遥测示例值 `NORMAL`，与车辆 8 态状态定义不同域） | 完整取值域 |

