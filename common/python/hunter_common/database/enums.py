"""数据库受控词表（StrEnum）。

单一事实来源：``contracts/database/enums.md``（取值不可新增/更改）。
DDL 中受控列统一为 ``TEXT + CHECK``（不使用 PG ENUM 类型，便于扩展与在线迁移）；
ORM 侧经 :class:`hunter_common.database.base.StrEnumType` 做字符串 ↔ 枚举双向映射，
业务代码禁止硬编码枚举字面量（引用枚举成员，例如 ``VehicleStatus.AUTO_DRIVING``）。
"""
from __future__ import annotations

from enum import StrEnum
from typing import Final


class VehicleStatus(StrEnum):
    """车辆状态（8 态；vehicles.status / Redis vehicle:status:{vehicle_id}）。"""

    OFFLINE = "offline"
    ONLINE_IDLE = "online_idle"
    AUTO_DRIVING = "auto_driving"
    REMOTE_CONTROLLED = "remote_controlled"
    UPGRADING = "upgrading"
    CHARGING = "charging"
    FAULT = "fault"
    EMERGENCY = "emergency"


class EventLevel(StrEnum):
    """事件等级（3 级；events.event_level）。"""

    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class EventType(StrEnum):
    """事件类型（19 种；events.event_type）。触发阈值不可更改（设计文档：事件类型定义）。

    决策 G-22②：新增 `collision_pre_warning`（warning，TTC < 3.0s），与
    `collision_warning`（critical，TTC < 1.5s）分级，解决 6.2.3 节 3.0s 预警与受控词表等级冲突。
    """

    HARSH_ACCELERATION = "harsh_acceleration"
    HARSH_BRAKING = "harsh_braking"
    HARSH_TURNING = "harsh_turning"
    OVER_SPEED = "over_speed"
    COLLISION_PRE_WARNING = "collision_pre_warning"
    COLLISION_WARNING = "collision_warning"
    MANUAL_TAKEOVER = "manual_takeover"
    EMERGENCY_STOP = "emergency_stop"
    BATTERY_LOW = "battery_low"
    BATTERY_CRITICAL = "battery_critical"
    COMMUNICATION_LOSS = "communication_loss"
    SENSOR_FAULT = "sensor_fault"
    PERCEPTION_FAULT = "perception_fault"
    PLANNING_FAULT = "planning_fault"
    CONTROL_FAULT = "control_fault"
    OTA_START = "ota_start"
    OTA_SUCCESS = "ota_success"
    OTA_FAILED = "ota_failed"
    OTA_ROLLBACK = "ota_rollback"


class OtaStatus(StrEnum):
    """OTA 状态机（9 态；ota_records.status/phase，Kafka ota_status）。

    流转：IDLE → PENDING → DOWNLOAD → INSTALL → TEST → SUCCESS；
    TEST 自检失败 → ROLLBACK → ROLLED_BACK（或 FAILED）。
    """

    IDLE = "IDLE"
    PENDING = "PENDING"
    DOWNLOAD = "DOWNLOAD"
    INSTALL = "INSTALL"
    TEST = "TEST"
    SUCCESS = "SUCCESS"
    ROLLBACK = "ROLLBACK"
    ROLLED_BACK = "ROLLED_BACK"
    FAILED = "FAILED"


class MetricModule(StrEnum):
    """算法模块（3 种；algorithm_metrics.module）。"""

    PERCEPTION = "perception"
    PLANNING = "planning"
    CONTROL = "control"


class SceneStatus(StrEnum):
    """场景状态（scenes.status）：仅 draft 可编辑。"""

    DRAFT = "draft"
    PUBLISHED = "published"
    ARCHIVED = "archived"


class OtaVersionStatus(StrEnum):
    """OTA 版本状态（ota_versions.status）：设计文档 7.2.2 审核流（G-18 决策②）。

    状态机：draft → testing → reviewing → published → deprecated / disabled；
    reviewing 审核驳回回退 draft；发布（publish/approve）前置状态为 reviewing。
    """

    DRAFT = "draft"
    TESTING = "testing"
    REVIEWING = "reviewing"
    PUBLISHED = "published"
    DEPRECATED = "deprecated"
    DISABLED = "disabled"


class OtaTaskStatus(StrEnum):
    """OTA 任务状态（ota_tasks.status）⚠ 取值域需与设计文档核对。"""

    CREATED = "created"
    PENDING_APPROVAL = "pending_approval"
    RUNNING = "running"
    PAUSED = "paused"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"


class UserStatus(StrEnum):
    """用户状态（users.status）⚠ 取值域需与设计文档核对。"""

    ENABLED = "enabled"
    DISABLED = "disabled"
    LOCKED = "locked"


class RoleStatus(StrEnum):
    """角色状态（roles.status）。"""

    ENABLED = "enabled"
    DISABLED = "disabled"


class PermissionResource(StrEnum):
    """权限资源域（permissions.resource）；与网关路由表资源域一致。"""

    SCENE = "scene"
    DATA = "data"
    ANALYTICS = "analytics"
    OTA = "ota"
    REMOTE = "remote"
    VEHICLE = "vehicle"
    USER = "user"


class PermissionAction(StrEnum):
    """权限动作（permissions.action）。"""

    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    EXECUTE = "execute"


#: 事件类型 → 契约等级映射（data-collector 落库前校验；禁止放宽阈值或改等级）
EVENT_LEVEL_BY_TYPE: Final[dict[str, EventLevel]] = {
    EventType.HARSH_ACCELERATION: EventLevel.WARNING,
    EventType.HARSH_BRAKING: EventLevel.WARNING,
    EventType.HARSH_TURNING: EventLevel.WARNING,
    EventType.OVER_SPEED: EventLevel.CRITICAL,
    EventType.COLLISION_PRE_WARNING: EventLevel.WARNING,
    EventType.COLLISION_WARNING: EventLevel.CRITICAL,
    EventType.MANUAL_TAKEOVER: EventLevel.INFO,
    EventType.EMERGENCY_STOP: EventLevel.CRITICAL,
    EventType.BATTERY_LOW: EventLevel.WARNING,
    EventType.BATTERY_CRITICAL: EventLevel.CRITICAL,
    EventType.COMMUNICATION_LOSS: EventLevel.CRITICAL,
    EventType.SENSOR_FAULT: EventLevel.CRITICAL,
    EventType.PERCEPTION_FAULT: EventLevel.CRITICAL,
    EventType.PLANNING_FAULT: EventLevel.CRITICAL,
    EventType.CONTROL_FAULT: EventLevel.CRITICAL,
    EventType.OTA_START: EventLevel.INFO,
    EventType.OTA_SUCCESS: EventLevel.INFO,
    EventType.OTA_FAILED: EventLevel.CRITICAL,
    EventType.OTA_ROLLBACK: EventLevel.WARNING,
}

#: OTA 终态（不再流转）
OTA_TERMINAL_STATUSES: Final[frozenset[OtaStatus]] = frozenset(
    {OtaStatus.SUCCESS, OtaStatus.ROLLED_BACK, OtaStatus.FAILED}
)

#: OTA 进行中状态（灰度监控 / 卡死检测）
OTA_ACTIVE_STATUSES: Final[frozenset[OtaStatus]] = frozenset(
    {
        OtaStatus.PENDING,
        OtaStatus.DOWNLOAD,
        OtaStatus.INSTALL,
        OtaStatus.TEST,
        OtaStatus.ROLLBACK,
    }
)


def event_level_for(event_type: EventType | str) -> EventLevel:
    """返回事件类型对应的契约等级；类型非法时抛 ``ValueError``（禁止静默降级）。"""
    key = str(event_type)
    try:
        return EVENT_LEVEL_BY_TYPE[key]
    except KeyError as exc:
        raise ValueError(f"非契约事件类型: {event_type}") from exc


__all__ = [
    "EVENT_LEVEL_BY_TYPE",
    "OTA_ACTIVE_STATUSES",
    "OTA_TERMINAL_STATUSES",
    "EventLevel",
    "EventType",
    "MetricModule",
    "OtaStatus",
    "OtaTaskStatus",
    "OtaVersionStatus",
    "PermissionAction",
    "PermissionResource",
    "RoleStatus",
    "SceneStatus",
    "UserStatus",
    "VehicleStatus",
    "event_level_for",
]
