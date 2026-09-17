"""业务流程参考实现（E2E 用例的「平台侧逻辑基准」）。

本模块把系统关键约束中 11.1 车辆数据上行、11.2 OTA 升级、11.3 远程操控三条主链路的
判定逻辑固化为**纯函数/纯状态机**，供：

1. ``tests/e2e/*`` 在不依赖外部中间件时验证流程语义（契约级回归）；
2. ``tests/integration/*`` 在真实 Postgres/Kafka/Redis 上比对落库/流转结果；
3. ``tests/performance/*`` 复用消息与行构造逻辑做压测，避免测试数据各写一份。

约束：所有字段名取自 ``contracts/``（DDL / JSON Schema / Topic YAML），所有阈值取自
``tests.support.thresholds``（登记来源见其 ``SOURCES``），本模块不新增任何业务规则。

⚠ 说明：11.1/11.2/11.3 的《详细设计文档 V4.0》原文未纳入本仓库，流程步骤按「系统关键
约束」+ 契约推导实现，并在 docstring 中标注契约出处；文档补齐后需回读本模块核对。
"""
from __future__ import annotations

import itertools
import math
from dataclasses import dataclass, field
from typing import Any

from tests.support import contracts, thresholds

TELEMETRY_TABLE = ("data_collector", "vehicle_telemetry")
EVENTS_TABLE = ("data_collector", "events")


# ================================================================ 11.1 车辆数据上行

def platform_topic_of(vehicle_id: str, topic_type: str) -> str:
    """车端 Topic 名（``hunter.{vehicle_id}.<type>``，模板取自 topics.yaml）。"""
    spec = contracts.vehicle_topics()[topic_type]
    return contracts.render_topic(spec["name"], vehicle_id)


def route_topic(topic_type: str) -> str:
    """车端 Topic 类型 → 平台内部 Topic（data-collector 消费接入后的路由目标）。"""
    return thresholds.INTERNAL_TOPIC_ROUTING[topic_type]


def telemetry_row(payload: dict[str, Any]) -> dict[str, Any]:
    """遥测消息 → ``data_collector.vehicle_telemetry`` 行（列名取自 DDL 契约）。

    映射规则：消息 ``chassis/localization/perception/planning/control/system`` 展平为列；
    ``time`` 列 = 车端上报时间戳（入库延迟以该列与本机时间之差衡量）。
    """
    contracts.assert_valid_message("telemetry", payload)
    chassis, localization = payload["chassis"], payload["localization"]
    perception, planning = payload["perception"], payload["planning"]
    control, system = payload["control"], payload["system"]
    row: dict[str, Any] = {
        "time": payload["timestamp"],
        "vehicle_id": payload["vehicle_id"],
        "seq": payload["seq"],
        "velocity": chassis["velocity"],
        "steering_angle": chassis["steering_angle"],
        "battery_voltage": chassis["battery_voltage"],
        "battery_soc": chassis["battery_soc"],
        "battery_current": chassis["battery_current"],
        "battery_temp": chassis["battery_temp"],
        "control_mode": chassis["control_mode"],
        "vehicle_state": chassis["vehicle_state"],
        "fault_code": chassis["fault_code"],
        "motor_rpm": chassis["motor_rpm"],
        "motor_current": chassis["motor_current"],
        "motor_temp": chassis["motor_temp"],
        "x": localization["x"],
        "y": localization["y"],
        "z": localization["z"],
        "roll": localization["roll"],
        "pitch": localization["pitch"],
        "heading": localization["heading"],
        "linear_velocity": localization["linear_velocity"],
        "angular_velocity": localization["angular_velocity"],
        "position_std": localization["position_std"],
        "heading_std": localization["heading_std"],
        "detected_objects": perception["detected_objects"],
        "fps": perception["fps"],
        "latency_ms": perception["latency_ms"],
        "object_types": perception["object_types"],
        "trajectory_length": planning["trajectory_length"],
        "trajectory_points": planning["trajectory_points"],
        "planning_latency_ms": planning["planning_latency_ms"],
        "current_behavior": planning["current_behavior"],
        "target_velocity": control["target_velocity"],
        "target_steer": control["target_steer"],
        "velocity_error": control["velocity_error"],
        "steer_error": control["steer_error"],
        "control_latency_ms": control["control_latency_ms"],
        "cpu_usage": system["cpu_usage"],
        "gpu_usage": system["gpu_usage"],
        "memory_usage_mb": system["memory_usage_mb"],
        "gpu_temp": system["gpu_temp"],
        "cpu_temp": system["cpu_temp"],
        "network_rssi": system["network_rssi"],
        "network_latency_ms": system["network_latency_ms"],
    }
    unknown = set(row) - set(contracts.table_column_names(*TELEMETRY_TABLE))
    assert not unknown, f"vehicle_telemetry 行含 DDL 未定义列（契约漂移）: {sorted(unknown)}"
    return row




def trigger_events(
    *,
    acceleration: float | None = None,
    deceleration: float | None = None,
    yaw_rate: float | None = None,
    velocity: float | None = None,
    speed_limit: float | None = None,
    ttc: float | None = None,
    battery_soc: int | None = None,
    telemetry_gap_s: float | None = None,
) -> list[str]:
    """阈值型事件判定（阈值来自「事件类型定义」，代码不得放宽/收严）。

    仅覆盖可由遥测量直接判定的 8 类；语义型事件（manual_takeover/emergency_stop/
    sensor_fault/perception_fault/planning_fault/control_fault/ota_*）由车端或平台任务
    判定后上报，本函数不自行发明触发条件。
    """
    events: list[str] = []
    rules = thresholds.EVENT_RULES
    if acceleration is not None and acceleration > rules["harsh_acceleration"]["threshold"]:
        events.append("harsh_acceleration")
    if deceleration is not None and deceleration > rules["harsh_braking"]["threshold"]:
        events.append("harsh_braking")
    if yaw_rate is not None and yaw_rate > rules["harsh_turning"]["threshold"]:
        events.append("harsh_turning")
    if velocity is not None and speed_limit and velocity / speed_limit > rules["over_speed"]["threshold"]:
        events.append("over_speed")
    if ttc is not None and ttc < rules["collision_warning"]["threshold"]:
        events.append("collision_warning")
    if battery_soc is not None:
        if battery_soc < rules["battery_critical"]["threshold"]:
            events.append("battery_critical")
        elif battery_soc < rules["battery_low"]["threshold"]:
            events.append("battery_low")
    if telemetry_gap_s is not None and telemetry_gap_s > rules["communication_loss"]["threshold"]:
        events.append("communication_loss")
    return events


def event_level_of(event_type: str) -> str:
    """事件类型 → 事件等级（契约枚举内，禁止自定义）。"""
    return str(thresholds.EVENT_RULES[event_type]["level"])


def telemetry_ingest_latency_s(persisted_at: float, row: dict[str, Any]) -> float:
    """入库延迟（秒）：车端上报时间（``time`` 列）到入库时刻的差值，阈值 ≤ 1s。"""
    return round(persisted_at - float(row["time"]), 6)


def detect_sequence_gap(seqs: list[int]) -> list[tuple[int, int]]:
    """丢包检测：返回 ``(前一 seq, 当前 seq)`` 跳变列表（seq 由车端单调递增）。"""
    return [
        (previous, current)
        for previous, current in itertools.pairwise(seqs)
        if current != previous + 1
    ]


def trajectory_continuity_ok(points: list[tuple[float, float]], max_jump_m: float) -> bool:
    """轨迹连续性：相邻定位点平面距离不得突跳超过 ``max_jump_m``（跳变即数据异常）。"""
    return all(
        math.hypot(x2 - x1, y2 - y1) <= max_jump_m
        for (x1, y1), (x2, y2) in itertools.pairwise(points)
    )


def upload_bandwidth_mbps(size_bytes: int, elapsed_s: float) -> float:
    """文件上传带宽（Mbps）；单车上限见 ``thresholds.FILE_UPLOAD_MAX_MBPS``。"""
    assert elapsed_s > 0, "耗时必须为正数"
    return round(size_bytes * 8 / elapsed_s / 1_000_000, 6)


def event_row(payload: dict[str, Any]) -> dict[str, Any]:
    """事件消息 → ``data_collector.events`` 行（event_level 由 event_type 决定，不可放宽）。"""
    contracts.assert_valid_message("event", payload)
    rule = thresholds.EVENT_RULES[payload["event_type"]]
    assert payload["event_level"] == rule["level"], (
        f"event_level 与事件类型定义不符：{payload['event_type']} 应为 {rule['level']}，"
        f"实际 {payload['event_level']}"
    )
    row = {
        "vehicle_id": payload["vehicle_id"],
        "event_type": payload["event_type"],
        "event_level": payload["event_level"],
        "event_time": payload["timestamp"],
        "description": payload.get("description"),
        "data_json": payload.get("data") or {},
        "data_file_url": payload.get("data_file_url"),
        "acknowledged": False,
    }
    unknown = set(row) - set(contracts.table_column_names(*EVENTS_TABLE))
    assert not unknown, f"events 行含 DDL 未定义列（契约漂移）: {sorted(unknown)}"
    return row


# ================================================================ 11.2 OTA 升级

#: 车端 OTA 状态机合法流转（系统关键约束第 14 条，不可更改）
OTA_TRANSITIONS: dict[str, tuple[str, ...]] = {
    "IDLE": ("PENDING",),
    "PENDING": ("DOWNLOAD",),
    "DOWNLOAD": ("INSTALL",),
    "INSTALL": ("TEST",),
    "TEST": ("SUCCESS", "ROLLBACK"),
    "ROLLBACK": ("ROLLED_BACK", "FAILED"),
    "SUCCESS": (),
    "ROLLED_BACK": (),
    "FAILED": (),
}


@dataclass(frozen=True)
class UpgradePrecondition:
    """升级门禁（系统关键约束第 14 条：电量≥50%、车辆静止(P档)、网络稳定、存储≥2GB）。"""

    battery_soc: int
    parked: bool
    network_stable: bool
    free_storage_gb: float

    def failures(self) -> list[tuple[int, str]]:
        """返回不满足项 ``(错误码, 说明)``；全满足返回空列表（错误码 6003）。"""
        problems: list[tuple[int, str]] = []
        if self.battery_soc < thresholds.OTA_MIN_SOC:
            problems.append((6003, f"电量不足：{self.battery_soc}% < {thresholds.OTA_MIN_SOC}%"))
        if not self.parked:
            problems.append((6003, "车辆未处于静止(P 档)"))
        if not self.network_stable:
            problems.append((6003, "网络不稳定"))
        if self.free_storage_gb < thresholds.OTA_MIN_FREE_STORAGE_GB:
            problems.append(
                (6003, f"存储不足：{self.free_storage_gb}GB < {thresholds.OTA_MIN_FREE_STORAGE_GB}GB")
            )
        return problems

    @property
    def satisfied(self) -> bool:
        """门禁是否全部满足。"""
        return not self.failures()


@dataclass
class RolloutBatch:
    """灰度批次（百分比 + 观察窗口 + 成功率门槛）。"""

    index: int
    percent: int
    target_vehicles: list[str]
    status: str = "pending"
    success_rate: float | None = None
    observation_hours: int = thresholds.OTA_OBSERVE_HOURS

    def evaluate(self, success: int, failed: int) -> bool:
        """按本批结果判定是否可推进：成功率 ≥ 95%（<95% 立即暂停 + 告警 + 人工介入）。"""
        total = success + failed
        assert total > 0, "批次内必须存在升级记录"
        self.success_rate = round(success / total, 6)
        if self.success_rate >= thresholds.OTA_MIN_SUCCESS_RATE:
            self.status = "succeeded"
            return True
        self.status = "paused"
        return False


def plan_rollout(vehicles: list[str]) -> list[RolloutBatch]:
    """生成灰度发布计划：5% → 20% → 50% → 100%，每批观察 24h（第 1 批至少 1 台）。"""
    assert vehicles, "目标车辆列表不能为空"
    ordered = sorted(set(vehicles))
    batches: list[RolloutBatch] = []
    for index, percent in enumerate(thresholds.OTA_ROLLOUT_BATCHES, start=1):
        if percent >= 100:
            selected = ordered
        else:
            count = max(1, math.ceil(len(ordered) * percent / 100))
            selected = ordered[:count]
        batches.append(RolloutBatch(index=index, percent=percent, target_vehicles=list(selected)))
    return batches


def validate_version_monotonic(current_code: int, target_code: int) -> tuple[bool, int | None]:
    """版本号单调递增（防回滚）：不满足返回 ``(False, 6003)``；签名/完整性错误码为 6001/6002。"""
    if target_code <= current_code:
        return False, 6003
    return True, None


def verify_package(
    *,
    actual_sha256: str,
    expected_sha256: str,
    signature_valid: bool,
    actual_md5: str | None = None,
    expected_md5: str | None = None,
) -> tuple[bool, int | None]:
    """OTA 包校验：SHA-256/MD5 完整性（6001）+ RSA-2048 验签（6002，先完整性后签名）。"""
    if expected_md5 and actual_md5 and actual_md5 != expected_md5:
        return False, 6001
    if actual_sha256 != expected_sha256:
        return False, 6001
    if not signature_valid:
        return False, 6002
    return True, None


def advance_ota_state(current: str, target: str) -> str:
    """推进车端 OTA 状态机；非法流转直接断言失败（状态名/流转方向不可更改）。"""
    allowed = OTA_TRANSITIONS[current]
    assert target in allowed, f"非法 OTA 状态流转：{current} → {target}（允许：{allowed}）"
    return target


def rollout_progress(batches: list[RolloutBatch]) -> dict[str, Any]:
    """发布进度汇总（写入 ``ota:progress:{task_id}`` Hash 与 ota_tasks.progress JSONB）。"""
    return {
        "total_batches": len(batches),
        "completed_batches": sum(1 for batch in batches if batch.status == "succeeded"),
        "current_batch": next(
            (batch.index for batch in batches if batch.status in {"pending", "running"}), None
        ),
        "paused": any(batch.status == "paused" for batch in batches),
        "target_vehicles": sorted({vehicle for batch in batches for vehicle in batch.target_vehicles}),
    }


# ================================================================ 11.3 远程操控

#: 远程操控会话状态（会话生命周期，非车辆状态定义）
RC_SESSION_ACTIVE: str = "active"
RC_SESSION_ENDED: str = "ended"
RC_SESSION_REJECTED: str = "rejected"


@dataclass
class RemoteControlSession:
    """远程操控会话基准实现（系统关键约束第 15 条，安全约束不可更改）。

    互斥规则：同一车辆同一时间仅允许一名操作员操控；冲突返回错误码 7001。
    指令节奏：20Hz（50ms 间隔）；>500ms 未收到指令 → 车辆自动减速停车。
    速度限制：最高 2.0 m/s（服务端限幅，不依赖车端裁剪）。
    """

    vehicle_id: str
    session_id: str
    operator_id: str
    vehicle_online: bool = True
    vehicle_busy: bool = False
    locked_by: str | None = None
    status: str = RC_SESSION_ACTIVE
    max_speed_mps: float = thresholds.RC_MAX_SPEED_MPS
    sent_seqs: list[int] = field(default_factory=list)
    last_command_at: float | None = None
    close_reason: str | None = None

    def open(self) -> tuple[bool, int | None]:
        """创建会话：返回 ``(成功, 错误码)``。

        - 车辆离线 → 4001；车辆忙（OTA 中/执行其他任务）→ 4002
        - 车辆已被他人锁定 → 7001；被同一操作员锁定视为重入，幂等成功
        """
        if not self.vehicle_online:
            return False, 4001
        if self.vehicle_busy:
            return False, 4002
        if self.locked_by not in (None, self.operator_id):
            return False, 7001
        self.locked_by = self.operator_id
        self.status = RC_SESSION_ACTIVE
        return True, None

    def close(self, *, reason: str = "operator_released") -> str:
        """结束会话：释放车辆锁（Redis ``rc:lock:{vehicle_id}`` 置空、``rc:session`` 清理）。"""
        self.locked_by = None
        self.status = RC_SESSION_ENDED
        self.close_reason = reason  # type: ignore[attr-defined]
        return self.status

    def clamp_velocity(self, target_velocity: float) -> float:
        """指令限幅：超出 ±max_speed_mps 一律截断（Schema 上限同为 ±2.0 m/s）。"""
        return max(-self.max_speed_mps, min(self.max_speed_mps, target_velocity))

    def accept_command(self, seq: int, target_velocity: float, now: float) -> dict[str, float]:
        """接收指令：校验会话有效 → 限幅 → 记录节奏（间隔 50ms，20Hz）。"""
        assert self.status == RC_SESSION_ACTIVE, f"会话不可用：{self.status}"
        velocity = self.clamp_velocity(target_velocity)
        self.sent_seqs.append(seq)
        self.last_command_at = now
        return {"seq": seq, "target_velocity": velocity, "interval_ms": thresholds.RC_COMMAND_INTERVAL_MS}

    def command_timeout_hit(self, now: float) -> bool:
        """指令超时保护：>500ms 未收到控制指令判定为超时（车辆应自动减速停车）。"""
        if self.last_command_at is None:
            return False
        return (now - self.last_command_at) * 1000 > thresholds.RC_COMMAND_TIMEOUT_MS

    def safety_stop_action(self) -> dict[str, float | str]:
        """超时后的安全动作：目标速度归零（减速停车），保持会话以免影响接管流程。"""
        return {"action": "decelerate_to_stop", "target_velocity": 0.0, "session_id": self.session_id}

    def command_rate_ok(self, intervals_ms: list[float]) -> bool:
        """指令节奏合规：平均频率不得超过 20Hz（契约上限，超出即服务端限流/丢弃）。"""
        if not intervals_ms:
            return True
        average = sum(intervals_ms) / len(intervals_ms)
        return average >= thresholds.RC_COMMAND_INTERVAL_MS * 0.9


def rc_lock_key(vehicle_id: str) -> str:
    """远程操控互斥锁 Redis 键（``rc:lock:{vehicle_id}``，TTL 30s）。"""
    pattern = contracts.redis_key("rc:lock:{vehicle_id}")["pattern"]
    return pattern.replace("{vehicle_id}", vehicle_id)


def rc_session_key(vehicle_id: str) -> str:
    """远程操控会话 Redis 键（``rc:session:{vehicle_id}``，会话期间有效）。"""
    pattern = contracts.redis_key("rc:session:{vehicle_id}")["pattern"]
    return pattern.replace("{vehicle_id}", vehicle_id)


def vehicle_status_key(vehicle_id: str) -> str:
    """车辆最新状态 Redis 键（``vehicle:status:{vehicle_id}`` Hash）。"""
    pattern = contracts.redis_key("vehicle:status:{vehicle_id}")["pattern"]
    return pattern.replace("{vehicle_id}", vehicle_id)


def ota_progress_key(task_id: str) -> str:
    """OTA 进度 Redis 键（``ota:progress:{task_id}`` Hash，TTL 1 天）。"""
    pattern = contracts.redis_key("ota:progress:{task_id}")["pattern"]
    return pattern.replace("{task_id}", task_id)


def session_key(user_id: str) -> str:
    """用户会话 Redis 键（``session:{user_id}``，TTL 2 小时）。"""
    pattern = contracts.redis_key("session:{user_id}")["pattern"]
    return pattern.replace("{user_id}", user_id)


def rate_limit_key(ip: str, api: str) -> str:
    """限流 Redis 键（``rate_limit:{ip}:{api}``，窗口 1 分钟）。"""
    pattern = contracts.redis_key("rate_limit:{ip}:{api}")["pattern"]
    return pattern.replace("{ip}", ip).replace("{api}", api)


def scene_cache_key(scene_id: str) -> str:
    """场景配置缓存 Redis 键（``cache:scene:{scene_id}``，TTL 1 小时）。"""
    pattern = contracts.redis_key("cache:scene:{scene_id}")["pattern"]
    return pattern.replace("{scene_id}", scene_id)


def processing_step_p95_ms(latencies_ms: list[float]) -> float:
    """P95 统计（性能断言统一口径：升序取 95 分位，单值样本即其本身）。"""
    assert latencies_ms, "样本不能为空"
    ordered = sorted(latencies_ms)
    index = max(0, math.ceil(0.95 * len(ordered)) - 1)
    return round(ordered[index], 6)
