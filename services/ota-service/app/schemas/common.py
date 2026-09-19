"""ota-service 共享常量与受控枚举（契约 components.schemas 公共部分）。

- 枚举取值域严格对齐 contracts/openapi/ota-service.yaml（禁止新增）；
- DDL 同域枚举（版本状态/任务状态/车端 9 态状态机）直接复用
  ``hunter_common.database.enums``，保证 Pydantic 模型、ORM StrEnumType 与
  DDL CHECK 三方一致（单一事实来源 = contracts/database/ddl/03_ota.sql）。
"""
from __future__ import annotations

import time
from enum import StrEnum
from uuid import uuid4

from pydantic import BaseModel, Field

# ---------- 契约 pattern 常量 ----------
VEHICLE_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$"  # 契约 VehicleId（= 证书 CN = Kafka key）
MODEL_NAME_PATTERN = r"^[A-Za-z0-9_]{1,32}$"  # 契约 applicable_models.items / applicable_model 查询
MD5_PATTERN = r"^[0-9a-f]{32}$"  # 契约 package_md5（小写十六进制，DDL CHAR(32)）
SHA256_PATTERN = r"^[0-9a-f]{64}$"  # 契约 package_sha256（小写十六进制，DDL CHAR(64)）

# ---------- 统一分页（与 hunter_common.database.repository 一致） ----------
DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 200


class ScheduleMode(StrEnum):
    """调度模式（契约 OtaTaskSchedule.mode；immediate=start 立即下发，scheduled=到点由调度器下发）。"""

    IMMEDIATE = "immediate"
    SCHEDULED = "scheduled"


class OtaBatchStatus(StrEnum):
    """灰度批次状态（契约 OtaBatchStatus；平台派生，非 DDL 列）。"""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    OBSERVING = "observing"
    PASSED = "passed"
    HALTED = "halted"


class OtaNextAction(StrEnum):
    """批次推进判定（契约 OtaNextAction；服务端派生，供前端展示与人工决策）。"""

    ADVANCE = "advance"
    OBSERVING = "observing"
    HALT = "halt"


class OtaTaskAction(StrEnum):
    """任务动作类型（契约 OtaTaskAction；用于动作结果回执）。"""

    START = "start"
    PAUSE = "pause"
    RESUME = "resume"
    CANCEL = "cancel"


class OtaRollbackTarget(StrEnum):
    """回滚目标（契约 OtaRollbackTarget；当前仅支持 A/B 分区回退到上一分区）。"""

    PREVIOUS_SLOT = "previous_slot"


class OtaPreconditionName(StrEnum):
    """升级门禁项名称（契约 OtaPreconditionName；与 Kafka ota_notify.preconditions 对齐）。"""

    BATTERY_SOC = "battery_soc"
    VEHICLE_PARKED = "vehicle_parked"
    NETWORK_STABLE = "network_stable"
    STORAGE = "storage"


class OtaApiResponse(BaseModel):
    """统一响应基类（契约 ApiResponse 五字段，字段名不可更改）。

    code=0 成功；data 结构随端点而定（无数据时为 null）；
    request_id/timestamp 缺省自动填充（路由层显式传入链路 trace_id）。
    """

    code: int = 0
    message: str = "success"
    data: object | None = None
    request_id: str = Field(default_factory=lambda: str(uuid4()))
    timestamp: int = Field(default_factory=lambda: int(time.time()))


__all__ = [
    "DEFAULT_PAGE_SIZE",
    "MAX_PAGE_SIZE",
    "MD5_PATTERN",
    "MODEL_NAME_PATTERN",
    "SHA256_PATTERN",
    "VEHICLE_ID_PATTERN",
    "OtaApiResponse",
    "OtaBatchStatus",
    "OtaNextAction",
    "OtaPreconditionName",
    "OtaRollbackTarget",
    "OtaTaskAction",
    "ScheduleMode",
]
