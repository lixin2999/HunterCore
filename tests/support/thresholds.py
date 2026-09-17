"""L5 测试层阈值常量：与系统关键约束/设计文档性能指标一一对应。

来源位置通过 ``SOURCES`` 显式登记，``scripts/verify_test_layer.py`` 会逐条回读契约文件比对，
防止测试阈值与契约/设计文档漂移（禁止在测试断言中写魔法数字）。
"""
from __future__ import annotations

import os
from typing import Any

# ---------------------------------------------------------------- 性能指标（第 10 条）
#: API 接口响应 P95（毫秒）
API_P95_MAX_MS: float = 200.0
#: 遥测数据入库延迟（秒）
TELEMETRY_INGEST_LATENCY_MAX_S: float = 1.0
#: 实时告警触发延迟（秒）
ALERT_TRIGGER_LATENCY_MAX_S: float = 2.0
#: 远程操控视频端到端延迟（毫秒）
RC_VIDEO_E2E_LATENCY_MAX_MS: float = 200.0
#: 远程操控指令延迟（毫秒）
RC_COMMAND_LATENCY_MAX_MS: float = 100.0
#: 时序数据写入速率（点/秒）
TIMESERIES_WRITE_MIN_POINTS_PER_S: float = 10_000.0
#: 系统整体可用性
AVAILABILITY_TARGET: float = 0.999

# ---------------------------------------------------------------- 限流（第 11 条 / 附录 D）
#: 全局限流（QPS）
RATE_LIMIT_GLOBAL_QPS: int = 10_000
#: 单用户限流（QPS）
RATE_LIMIT_USER_QPS: int = 100
#: 单 IP 限流（QPS）
RATE_LIMIT_IP_QPS: int = 200
#: 限流窗口（秒）
RATE_LIMIT_WINDOW_SECONDS: int = 60
#: 单车遥测上报上限（msg/s）
TELEMETRY_PRODUCE_MAX_MSGS_PER_S: int = 100
#: 单车文件上传带宽（Mbps）
FILE_UPLOAD_MAX_MBPS: int = 10

# ---------------------------------------------------------------- 远程操控安全（第 15 条）
#: 控制指令发送频率（Hz）
RC_COMMAND_HZ: int = 20
#: 控制指令间隔（毫秒）
RC_COMMAND_INTERVAL_MS: int = 50
#: 指令超时保护（毫秒，>500ms 未收到指令车辆自动减速停车）
RC_COMMAND_TIMEOUT_MS: int = 500
#: 远程操控最高速度（m/s）
RC_MAX_SPEED_MPS: float = 2.0
#: 视频编码参数（720p@30fps，2-4Mbps，关键帧间隔 1s）
RC_VIDEO_WIDTH: int = 1280
RC_VIDEO_HEIGHT: int = 720
RC_VIDEO_FPS: int = 30
RC_VIDEO_BITRATE_MIN_BPS: int = 2_000_000
RC_VIDEO_BITRATE_MAX_BPS: int = 4_000_000
RC_VIDEO_KEYFRAME_INTERVAL_S: int = 1

# ---------------------------------------------------------------- OTA（第 14 条）
#: 灰度批次百分比
OTA_ROLLOUT_BATCHES: tuple[int, ...] = (5, 20, 50, 100)
#: 每批观察时长（小时）
OTA_OBSERVE_HOURS: int = 24
#: 批次最低成功率（低于该值立即暂停 + 告警 + 人工介入）
OTA_MIN_SUCCESS_RATE: float = 0.95
#: 升级门禁：最低电量 SOC（%）
OTA_MIN_SOC: int = 50
#: 升级门禁：最低可用存储（GB）
OTA_MIN_FREE_STORAGE_GB: int = 2
#: 车端 OTA 状态机（不可更改流转）
OTA_STATE_MACHINE: tuple[str, ...] = (
    "IDLE",
    "PENDING",
    "DOWNLOAD",
    "INSTALL",
    "TEST",
    "SUCCESS",
)
OTA_ROLLBACK_PATH: tuple[str, ...] = ("TEST", "ROLLBACK", "ROLLED_BACK")

# ---------------------------------------------------------------- Redis TTL（第 8 条）
#: 用户会话 TTL（秒，2 小时）
SESSION_TTL_SECONDS: int = 7200
#: 场景配置缓存 TTL（秒，1 小时）
SCENE_CACHE_TTL_SECONDS: int = 3600
#: OTA 进度 Hash TTL（秒，任务结束后 1 天）
OTA_PROGRESS_TTL_SECONDS: int = 86_400

# ---------------------------------------------------------------- 对象存储（第 7 条）
#: 预签名上传有效期（秒，1 小时）
PRESIGN_UPLOAD_TTL_SECONDS: int = 3600
#: 预签名下载有效期（秒，15 分钟）
PRESIGN_DOWNLOAD_TTL_SECONDS: int = 900

# ---------------------------------------------------------------- 数据保留（第 6/7 条）
#: 遥测时序保留（天）
TELEMETRY_RETENTION_DAYS: int = 90
#: hypertable 分块间隔（天）
TIMESERIES_CHUNK_INTERVAL_DAYS: int = 1

# ---------------------------------------------------------------- 基准测试环境参数
#: 基准并发/规模（可通过环境变量放大，CI 使用默认值保证时长可控）
BENCH_API_REQUESTS: int = int(os.getenv("HUNTER_BENCH_API_REQUESTS", "300"))
BENCH_API_WARMUP: int = int(os.getenv("HUNTER_BENCH_API_WARMUP", "20"))
BENCH_TELEMETRY_MESSAGES: int = int(os.getenv("HUNTER_BENCH_TELEMETRY_MESSAGES", "2000"))
BENCH_TIMESERIES_POINTS: int = int(os.getenv("HUNTER_BENCH_TIMESERIES_POINTS", "50_000".replace("_", "")))
#: 开发机容器环境下 Kafka 端到端吞吐下限（契约未规定平台侧吞吐目标，仅规定单车 ≤100 msg/s；
#: 该下限用于回归告警，可在 CI 通过环境变量调整）
BENCH_KAFKA_MIN_MSGS_PER_S: float = float(os.getenv("HUNTER_BENCH_KAFKA_MIN_MSGS_PER_S", "300"))

# ---------------------------------------------------------------- 事件类型定义（第 13 条）
#: 18 种事件：event_type → event_level + 触发阈值（阈值与等级不可更改，测试不得放宽）
EVENT_RULES: dict[str, dict[str, Any]] = {
    "harsh_acceleration": {"level": "warning", "metric": "acceleration", "threshold": 3.0, "unit": "m/s^2"},
    "harsh_braking": {"level": "warning", "metric": "deceleration", "threshold": 3.0, "unit": "m/s^2"},
    "harsh_turning": {"level": "warning", "metric": "yaw_rate", "threshold": 0.8, "unit": "rad/s"},
    "over_speed": {"level": "critical", "metric": "speed_over_limit_ratio", "threshold": 1.1, "unit": ""},
    "collision_warning": {"level": "critical", "metric": "ttc", "threshold": 1.5, "unit": "s", "comparator": "lt"},
    "manual_takeover": {"level": "info", "metric": None, "threshold": None, "unit": ""},
    "emergency_stop": {"level": "critical", "metric": None, "threshold": None, "unit": ""},
    "battery_low": {"level": "warning", "metric": "battery_soc", "threshold": 20, "unit": "%", "comparator": "lt"},
    "battery_critical": {"level": "critical", "metric": "battery_soc", "threshold": 10, "unit": "%", "comparator": "lt"},
    "communication_loss": {"level": "critical", "metric": "telemetry_gap", "threshold": 10, "unit": "s"},
    "sensor_fault": {"level": "critical", "metric": None, "threshold": None, "unit": ""},
    "perception_fault": {"level": "critical", "metric": "fault_code", "threshold": 0, "unit": ""},
    "planning_fault": {"level": "critical", "metric": "fault_code", "threshold": 0, "unit": ""},
    "control_fault": {"level": "critical", "metric": "fault_code", "threshold": 0, "unit": ""},
    "ota_start": {"level": "info", "metric": None, "threshold": None, "unit": ""},
    "ota_success": {"level": "info", "metric": None, "threshold": None, "unit": ""},
    "ota_failed": {"level": "critical", "metric": None, "threshold": None, "unit": ""},
    "ota_rollback": {"level": "warning", "metric": None, "threshold": None, "unit": ""},
}

#: 事件等级枚举（events 表 event_level CHECK）
EVENT_LEVELS: tuple[str, ...] = ("info", "warning", "critical")

#: 车辆状态定义（第 12 条，不可新增/更改状态名）
VEHICLE_STATES: tuple[str, ...] = (
    "offline",
    "online_idle",
    "auto_driving",
    "remote_controlled",
    "upgrading",
    "charging",
    "fault",
    "emergency",
)

#: 平台内部 Topic 流转（第 4 条：车端 Topic → 平台内部 Topic）
INTERNAL_TOPIC_ROUTING: dict[str, str] = {
    "telemetry": "telemetry_raw",
    "event": "event_raw",
    "sensor_file": "sensor_file",
}

#: 清理/清洗后 Topic（预处理链路：telemetry_raw → telemetry_clean）
CLEAN_TOPIC: str = "telemetry_clean"

#: 阈值来源登记（供 scripts/verify_test_layer.py 回读契约比对）
SOURCES: dict[str, str] = {
    "API_P95_MAX_MS": "系统关键约束第 10 条（API 接口响应 P95 ≤ 200ms）",
    "TELEMETRY_INGEST_LATENCY_MAX_S": "系统关键约束第 10 条（遥测数据入库延迟 ≤ 1s）",
    "ALERT_TRIGGER_LATENCY_MAX_S": "系统关键约束第 10 条（实时告警触发延迟 ≤ 2s）",
    "RC_VIDEO_E2E_LATENCY_MAX_MS": "系统关键约束第 10 条（远程操控视频端到端 ≤ 200ms）",
    "RC_COMMAND_LATENCY_MAX_MS": "系统关键约束第 10 条（远程操控指令延迟 ≤ 100ms）",
    "TIMESERIES_WRITE_MIN_POINTS_PER_S": "系统关键约束第 10 条（时序数据写入 ≥ 10000 点/秒）",
    "TELEMETRY_PRODUCE_MAX_MSGS_PER_S": "contracts/kafka/topics.yaml#throttles[telemetry_produce_rate]",
    "FILE_UPLOAD_MAX_MBPS": "contracts/kafka/topics.yaml#throttles[file_upload_bandwidth]",
    "RC_COMMAND_HZ": "系统关键约束第 15 条（控制指令 20Hz / 50ms）",
    "RC_COMMAND_TIMEOUT_MS": "系统关键约束第 15 条（>500ms 未收到指令自动减速停车）",
    "RC_MAX_SPEED_MPS": "系统关键约束第 15 条（远程操控最高速度 2.0 m/s）",
    "OTA_ROLLOUT_BATCHES": "系统关键约束第 14 条（灰度 5%→20%→50%→100%）",
    "OTA_MIN_SUCCESS_RATE": "系统关键约束第 14 条（成功率 ≥95%）",
    "OTA_MIN_SOC": "系统关键约束第 14 条（升级门禁 电量≥50%）",
    "OTA_MIN_FREE_STORAGE_GB": "系统关键约束第 14 条（升级门禁 存储≥2GB）",
    "OTA_STATE_MACHINE": "系统关键约束第 14 条（OTA 状态机）",
    "PRESIGN_UPLOAD_TTL_SECONDS": "contracts/database/object-storage.yaml#presign_policy",
    "PRESIGN_DOWNLOAD_TTL_SECONDS": "contracts/database/object-storage.yaml#presign_policy",
    "TELEMETRY_RETENTION_DAYS": "contracts/database/ddl/05_timeseries.sql（保留 90 天）",
    "TIMESERIES_CHUNK_INTERVAL_DAYS": "contracts/database/ddl/05_timeseries.sql（chunk_time_interval=1 day）",
    "EVENT_RULES": "系统关键约束第 13 条（事件类型定义：18 种 + 触发阈值/等级）",
    "EVENT_LEVELS": "contracts/database/ddl/04_events.sql（event_level CHECK IN info/warning/critical）",
    "VEHICLE_STATES": "系统关键约束第 12 条（车辆状态定义，8 种）",
    "INTERNAL_TOPIC_ROUTING": "系统关键约束第 4 条（平台内部 Topic：telemetry_raw/event_raw/sensor_file）",
    "CLEAN_TOPIC": "系统关键约束第 4 条（平台内部 Topic：telemetry_clean）",
}
