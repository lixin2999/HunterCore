"""升级任务与灰度发布模型（契约 components.schemas：OtaCanaryBatch → OtaTaskRollbackResponse）。

灰度策略（5% → 20% → 50% → 100%，每批观察 24h，成功率门禁 ≥ 95%）为契约固定值，
字段类型使用 Literal 锁定（OtaCanaryBatch），任何偏离在请求校验阶段即被拒绝。
"""
from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from hunter_common.database.enums import OtaStatus, OtaTaskStatus

from app.schemas.common import (
    OtaApiResponse,
    OtaBatchStatus,
    OtaNextAction,
    OtaPreconditionName,
    OtaRollbackTarget,
    OtaTaskAction,
    ScheduleMode,
    VEHICLE_ID_PATTERN,
)


class OtaCanaryBatch(BaseModel):
    """单个灰度批次（契约 OtaCanaryBatch；比例与门禁不可更改）。"""

    model_config = ConfigDict(extra="forbid")

    batch_no: int = Field(ge=1, le=4, description="批次序号（1 基）")
    percent: Literal[5, 20, 50, 100] = Field(description="本批覆盖目标车辆比例（%）")
    observe_hours: Literal[24] = Field(default=24, description="本批观察时长（固定 24h，不可更改）")
    success_rate_threshold: Literal[0.95] = Field(
        default=0.95, description="本批成功率门禁（阈值来源：设计文档 OTA 灰度流程）"
    )


class OtaUpgradeStrategy(BaseModel):
    """灰度发布策略（契约 OtaUpgradeStrategy；写入 ota_tasks.upgrade_strategy JSONB）。

    规范序列：4 批 5/20/50/100 + 观察 24h + 门禁 0.95；创建请求如显式给出，
    必须逐字段等于该规范值，否则 → 2001（灰度流程不可更改）。
    """

    model_config = ConfigDict(extra="forbid")

    batches: list[OtaCanaryBatch] = Field(min_length=4, max_length=4, description="四批灰度序列")
    stage_gate: Literal[True] = Field(
        default=True, description="严格按批次串行推进（固定 true：任一批成功率 < 95% 立即暂停 + 告警）"
    )


class OtaTaskSchedule(BaseModel):
    """调度窗口（契约 OtaTaskSchedule；写入 ota_tasks.schedule JSONB）。"""

    model_config = ConfigDict(extra="forbid")

    mode: ScheduleMode = Field(description="调度模式（immediate=start 立即下发；scheduled=到点下发）")
    start_time: float | None = Field(
        default=None, description="计划开始时间（Unix epoch 秒；mode=scheduled 时必填且 > 当前时间）"
    )
    window_end: float | None = Field(
        default=None, description="计划结束时间（超时未下发完的车辆自动跳过；可选）"
    )


class OtaTaskPreconditions(BaseModel):
    """升级门禁（契约 OtaTaskPreconditions；请求值不得弱于平台门禁，放宽 → 2001）。

    默认值对齐系统约束「电量 ≥ 50% / 车辆静止(P 档) / 网络稳定 / 存储 ≥ 2GB」。
    """

    model_config = ConfigDict(extra="forbid")

    soc_min: int = Field(default=50, ge=0, le=100, description="最低电量百分比（OTA_PRECONDITION_MIN_SOC）")
    must_be_parked: bool = Field(default=True, description="必须静止（P 档）")
    network_stable: bool = Field(default=True, description="网络稳定（遥测间隔 ≤ OTA_OFFLINE_THRESHOLD_SECONDS）")
    min_storage_mb: int = Field(default=2048, ge=0, description="最小可用存储 MB（默认 2048 = 2GB）")


class OtaTaskCreateRequest(BaseModel):
    """创建升级任务请求（契约 OtaTaskCreateRequest；创建不下发，start 统一触发）。"""

    model_config = ConfigDict(extra="forbid")

    task_name: str = Field(min_length=1, max_length=128, description="任务名称")
    target_version_id: UUID = Field(description="目标版本（必须为 published；draft/disabled → 3003）")
    target_vehicles: list[str] = Field(
        min_length=1, description="目标车辆清单（去重后入库；上限 OTA_TASK_MAX_TARGET_VEHICLES）"
    )
    upgrade_strategy: OtaUpgradeStrategy | None = Field(
        default=None, description="可选；省略时服务端写入规范灰度序列（推荐省略）"
    )
    schedule: OtaTaskSchedule | None = Field(default=None, description="调度窗口（缺省 immediate）")
    preconditions: OtaTaskPreconditions | None = Field(
        default=None, description="升级门禁（缺省对齐平台门禁；不得弱于平台门禁）"
    )


class OtaTaskProgress(BaseModel):
    """任务聚合进度（契约 OtaTaskProgress；写入 ota_tasks.progress JSONB 并同步 Redis 热点）。"""

    model_config = ConfigDict(extra="forbid")

    total: int = Field(ge=0, description="目标车辆总数")
    pending: int = Field(ge=0, description="未开始/等待门禁")
    in_progress: int = Field(ge=0, description="处于 PENDING/DOWNLOAD/INSTALL/TEST")
    succeeded: int = Field(ge=0, description="到达 SUCCESS")
    failed: int = Field(ge=0, description="到达 FAILED")
    rolled_back: int = Field(ge=0, description="到达 ROLLED_BACK")
    success_rate: float | None = Field(
        default=None,
        description="已完成车辆中的成功率（分母 = succeeded+failed+rolled_back；无完成车辆为 null）",
    )
    current_batch: int = Field(ge=0, le=4, description="当前批次序号（0=尚未开始）")


class OtaTaskItem(BaseModel):
    """`ota_svc.ota_tasks` 单行（契约 OtaTaskItem；字段与 DDL 一一对应）。

    `target_vehicles` 为该表 TEXT[] 列原样返回；列表接口省略（null）以控制响应体积。
    """

    model_config = ConfigDict(extra="forbid")

    task_id: UUID = Field(description="任务 ID（UUID 主键）")
    task_name: str = Field(max_length=128, description="任务名称")
    target_version_id: UUID = Field(description="目标版本 ID")
    target_vehicles: list[str] | None = Field(
        default=None, description="目标车辆清单（列表接口在 need_vehicles=false 时为 null）"
    )
    vehicle_count: int = Field(default=0, ge=0, description="目标车辆数（= array_length(target_vehicles,1)）")
    upgrade_strategy: OtaUpgradeStrategy | None = Field(default=None, description="冻结的灰度策略")
    schedule: OtaTaskSchedule | None = Field(default=None, description="调度窗口")
    preconditions: OtaTaskPreconditions | None = Field(default=None, description="升级门禁")
    status: OtaTaskStatus = Field(description="任务状态（DDL CHECK 七态）")
    progress: OtaTaskProgress = Field(description="任务聚合进度")
    creator: UUID = Field(description="创建人 user_id（网关注入 JWT sub，服务端不接收客户端传入）")
    create_time: float = Field(description="创建时间（Unix epoch 秒）")


class OtaTaskList(BaseModel):
    """任务分页数据（契约 OtaTaskList；排序 create_time DESC）。"""

    model_config = ConfigDict(extra="forbid")

    items: list[OtaTaskItem] = Field(default_factory=list, description="任务列表（target_vehicles 为 null）")
    total: int = Field(ge=0, description="满足条件的总记录数")
    page: int = Field(ge=1, description="页码")
    page_size: int = Field(ge=1, le=200, description="每页条数")


class OtaTaskListResponse(OtaApiResponse):
    """GET /api/v1/ota/tasks 统一响应。"""

    data: OtaTaskList | None = None


class OtaTaskResponse(OtaApiResponse):
    """POST /api/v1/ota/tasks 统一响应。"""

    data: OtaTaskItem | None = None


class OtaBatchProgress(BaseModel):
    """单批次进度视图（契约 OtaBatchProgress；服务端按 ota_records 与策略派生，不落库）。"""

    model_config = ConfigDict(extra="forbid")

    batch_no: int = Field(ge=1, le=4, description="批次序号")
    percent: Literal[5, 20, 50, 100] = Field(description="本批覆盖目标车辆比例（%）")
    status: OtaBatchStatus = Field(description="批次状态（pending/in_progress/observing/passed/halted）")
    target_count: int = Field(ge=0, description="本批分配车辆数")
    success_count: int = Field(ge=0, description="到达 SUCCESS 车辆数")
    failed_count: int = Field(ge=0, description="到达 FAILED 车辆数")
    rolled_back_count: int = Field(ge=0, description="到达 ROLLED_BACK 车辆数")
    in_progress_count: int = Field(ge=0, description="升级中（PENDING/DOWNLOAD/INSTALL/TEST）车辆数")
    success_rate: float | None = Field(default=None, description="本批成功率（门禁 ≥ 0.95；无终态为 null）")
    observe_until: float | None = Field(default=None, description="本批观察截止（= 全部完成时间 + 24h）")
    started_at: float | None = Field(default=None, description="本批首次下发时间")
    finished_at: float | None = Field(default=None, description="本批全部到达终态时间")


class OtaRolloutView(BaseModel):
    """灰度推进视图（契约 OtaRolloutView；任务详情内返回，供前端展示决策建议）。"""

    model_config = ConfigDict(extra="forbid")

    current_batch: int = Field(ge=0, le=4, description="当前批次序号（0=尚未开始）")
    total_batches: Literal[4] = Field(default=4, description="总批次数（固定 4）")
    observe_until: float | None = Field(default=None, description="当前批次观察截止时间")
    next_action: OtaNextAction = Field(
        description="advance（可推进）/ observing（观察中）/ halt（成功率 < 95%，需人工介入）"
    )
    halt_reason: str | None = Field(
        default=None, description="暂停原因（next_action=halt 时给出，如 success_rate 0.93 < 0.95）"
    )
    batches: list[OtaBatchProgress] = Field(default_factory=list, description="四批计划与实测")


class OtaTaskDetail(OtaTaskItem):
    """任务详情（契约 OtaTaskDetail = OtaTaskItem + target_version 快照 + rollout 视图）。

    ``target_version`` 运行时解析为 app.schemas.versions.OtaVersionItem
    （前向引用见模块尾部 ``_rebuild_models``，避免循环导入）。
    """

    target_version: "OtaVersionItemLike" = Field(description="内联目标版本（前端一次请求渲染任务全貌）")
    rollout: OtaRolloutView = Field(description="灰度推进视图")


class OtaTaskDetailResponse(OtaApiResponse):
    """GET /api/v1/ota/tasks/{task_id} 统一响应。"""

    data: OtaTaskDetail | None = None


class OtaTaskStartRequest(BaseModel):
    """启动/恢复请求体（契约 OtaTaskStartRequest；可省略，仅承载审计备注与批次号）。"""

    model_config = ConfigDict(extra="forbid")

    note: str | None = Field(
        default=None, max_length=512, description="操作备注（写入审计日志，禁止含密钥/证书内容）"
    )
    batch_no: int | None = Field(
        default=None,
        ge=1,
        le=4,
        description="仅 paused 且人工确认推进时可选填目标批次号；必须等于「当前批次 + 1」，否则 → 3003",
    )


class OtaTaskPauseRequest(BaseModel):
    """暂停请求体（契约 OtaTaskPauseRequest；可省略）。"""

    model_config = ConfigDict(extra="forbid")

    reason: str | None = Field(default=None, max_length=512, description="暂停原因（审计留痕）")


class OtaTaskCancelRequest(BaseModel):
    """终止请求体（契约 OtaTaskCancelRequest；可省略）。"""

    model_config = ConfigDict(extra="forbid")

    reason: str | None = Field(default=None, max_length=512, description="终止原因（审计留痕）")


class OtaPreconditionFailure(BaseModel):
    """单车门禁失败明细（契约 OtaPreconditionFailure；错误码 6003 的 precondition_failures[]）。"""

    model_config = ConfigDict(extra="forbid")

    vehicle_id: str = Field(pattern=VEHICLE_ID_PATTERN, description="车辆标识")
    failed_conditions: list[OtaPreconditionName] = Field(
        min_length=1, description="未通过的门禁项（1=电量, 2=静止/档位, 3=网络, 4=存储）"
    )
    actual: dict[str, object] | None = Field(
        default=None, description="实测值（battery_soc / gear / last_seen_seconds / free_storage_mb）"
    )


class OtaTaskActionData(BaseModel):
    """任务动作回执（契约 OtaTaskActionData；start / pause / resume / cancel 共用）。"""

    model_config = ConfigDict(extra="forbid")

    task_id: UUID = Field(description="任务 ID")
    action: OtaTaskAction = Field(description="动作类型")
    status: OtaTaskStatus = Field(description="动作执行后的任务状态")
    batch_no: int | None = Field(default=None, description="本次动作涉及的批次号（pause/cancel 可能为 null）")
    released_vehicles: list[str] = Field(
        default_factory=list, description="本次放行（已下发 ota_notify）的车辆清单"
    )
    blocked_vehicles: list[OtaPreconditionFailure] = Field(
        default_factory=list, description="本次因门禁/状态被拦截的车辆清单（单车失败不阻断同批其余车辆）"
    )
    released_count: int = Field(ge=0, description="放行车辆数")
    blocked_count: int = Field(ge=0, description="拦截车辆数")
    executed_at: float = Field(description="动作执行时间（Unix epoch 秒）")
    operator_id: UUID | None = Field(default=None, description="操作人 user_id（网关注入 JWT 主体）")
    reason: str | None = Field(default=None, description="动作原因（pause/cancel 回显请求值）")


class OtaTaskActionResponse(OtaApiResponse):
    """POST /tasks/{task_id}/start|pause|resume|cancel 统一响应。"""

    data: OtaTaskActionData | None = None


class OtaTaskRollbackRequest(BaseModel):
    """人工回滚请求（契约 OtaTaskRollbackRequest；reason 必填，随 command params 下发车端）。"""

    model_config = ConfigDict(extra="forbid")

    vehicle_ids: list[str] | None = Field(
        default=None,
        min_length=1,
        description="待回滚车辆子集；省略 = 该任务下全部 ota_records.status=SUCCESS 的车辆",
    )
    target: OtaRollbackTarget = Field(
        default=OtaRollbackTarget.PREVIOUS_SLOT, description="回滚目标（当前仅支持 A/B 上一分区）"
    )
    reason: str = Field(min_length=1, max_length=512, description="回滚原因（审计留痕，必填）")


class OtaTaskRollbackResultItem(BaseModel):
    """单车回滚受理结论（契约 OtaTaskRollbackResultItem；单车失败不影响其余车辆）。"""

    model_config = ConfigDict(extra="forbid")

    vehicle_id: str = Field(pattern=VEHICLE_ID_PATTERN, description="车辆标识")
    accepted: bool = Field(description="true=已下发 ota_rollback 指令；false=被拒绝")
    command_id: UUID | None = Field(default=None, description="指令 ID（accepted=true；关联 command_result）")
    topic: str | None = Field(
        default=None, description="指令下发 Topic（accepted=true 时固定 hunter.{vehicle_id}.command）"
    )
    reason: str | None = Field(default=None, description="拒绝原因（accepted=false，如 离线/状态不允许）")
    current_status: OtaStatus | None = Field(default=None, description="拒绝时车辆的 ota_records.status")


class OtaTaskRollbackData(BaseModel):
    """回滚受理结果（契约 OtaTaskRollbackData）。"""

    model_config = ConfigDict(extra="forbid")

    task_id: UUID = Field(description="任务 ID")
    target: OtaRollbackTarget = Field(description="回滚目标")
    requested_count: int = Field(ge=0, description="本次请求回滚车辆数")
    accepted_count: int = Field(ge=0, description="成功下发指令车辆数")
    rejected_count: int = Field(ge=0, description="被拒绝车辆数（门禁/状态/离线）")
    results: list[OtaTaskRollbackResultItem] = Field(default_factory=list, description="逐车受理结论")
    executed_at: float = Field(description="动作执行时间（Unix epoch 秒）")
    operator_id: UUID | None = Field(default=None, description="操作人 user_id")


class OtaTaskRollbackResponse(OtaApiResponse):
    """POST /tasks/{task_id}/rollback 统一响应。"""

    data: OtaTaskRollbackData | None = None


def _rebuild_models() -> None:
    """前向引用解析（OtaTaskDetail.target_version → app.schemas.versions.OtaVersionItem）。

    局部导入避免循环依赖；替换模块级占位名后重建模型（Pydantic v2 必需）。
    """
    global OtaVersionItemLike  # noqa: PLW0603
    from app.schemas.versions import OtaVersionItem  # noqa: PLC0415

    OtaVersionItemLike = OtaVersionItem
    OtaTaskDetail.model_rebuild()


_rebuild_models()


__all__ = [
    "OtaBatchProgress",
    "OtaCanaryBatch",
    "OtaPreconditionFailure",
    "OtaRolloutView",
    "OtaTaskActionData",
    "OtaTaskActionResponse",
    "OtaTaskCancelRequest",
    "OtaTaskCreateRequest",
    "OtaTaskDetail",
    "OtaTaskDetailResponse",
    "OtaTaskItem",
    "OtaTaskList",
    "OtaTaskListResponse",
    "OtaTaskPauseRequest",
    "OtaTaskProgress",
    "OtaTaskResponse",
    "OtaTaskRollbackData",
    "OtaTaskRollbackRequest",
    "OtaTaskRollbackResponse",
    "OtaTaskRollbackResultItem",
    "OtaTaskSchedule",
    "OtaTaskStartRequest",
    "OtaUpgradeStrategy",
]
