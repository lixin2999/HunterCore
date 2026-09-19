"""升级任务业务服务（灰度创建 / 启动 / 暂停 / 恢复 / 终止 / A/B 回滚）。

契约要点（x-hunter-canary-rollout / x-hunter-ota-security）：
- 创建冻结灰度策略（默认 5/20/50/100 + 观察 24h + 门禁 0.95），不下发任何通知；
- start/resume 逐车门禁（电量/静止/网络/存储，读 Redis vehicle:status 读模型），
  单车失败不阻断同批其余车辆；整批门禁全失败 → 任务 failed + 告警日志；
- 批次推进以 SELECT ... FOR UPDATE 行锁串行化；禁止跳批（batch_no = 当前批次 + 1）；
- 重复 start/resume 幂等：仅对 ota_records.status=PENDING 车辆下发 ota_notify；
- 回滚仅对 SUCCESS 记录下发 hunter.{vehicle_id}.command（ota_rollback）；
- halt（成功率 < 0.95）→ paused + 告警日志（alert_event 生产者为 data-analytics，
  本服务不生产该 Topic，见 x-hunter-pending-confirmation #12）。
"""
from __future__ import annotations

import asyncio
import json
import time
from uuid import UUID

from hunter_common.database.enums import OtaStatus, OtaTaskStatus, OtaVersionStatus
from hunter_common.database.models import OtaTask
from hunter_common.exceptions import (
    InvalidParameterError,
    MissingParameterError,
    ResourceNotFoundError,
    ResourceStateConflictError,
)
from hunter_common.logging import get_logger, get_trace_id

from app.config import Settings
from app.producers.ota_notify import OtaNotifyProducer
from app.producers.rollback_command import RollbackCommandProducer
from app.repositories.records import OtaRecordRepository
from app.repositories.storage import OtaPackageStorage
from app.repositories.tasks import OtaTaskRepository
from app.repositories.versions import OtaVersionRepository
from app.schemas.common import OtaPreconditionName, OtaTaskAction
from app.schemas.tasks import (
    OtaPreconditionFailure,
    OtaTaskActionData,
    OtaTaskCancelRequest,
    OtaTaskCreateRequest,
    OtaTaskDetail,
    OtaTaskItem,
    OtaTaskList,
    OtaTaskPauseRequest,
    OtaTaskPreconditions,
    OtaTaskProgress,
    OtaTaskRollbackData,
    OtaTaskRollbackRequest,
    OtaTaskRollbackResultItem,
    OtaTaskSchedule,
    OtaTaskStartRequest,
    OtaUpgradeStrategy,
)
from app.services.gates import GateResult, VehicleStateReader, check_vehicle
from app.services.rollout import (
    allocate_batches,
    build_rollout_view,
    build_task_progress,
    resolve_strategy,
)

logger = get_logger("app.services.ota_tasks")

#: 终态任务集合（start 禁止；cancel 对终态重复终止 → 3003）
_TERMINAL_TASK_STATUSES = frozenset(
    {OtaTaskStatus.SUCCEEDED, OtaTaskStatus.FAILED, OtaTaskStatus.CANCELED}
)

#: 车端未下发状态（重复 start/resume 时允许再次通知的唯一记录状态）
_PENDING_STATUS = OtaStatus.PENDING


class TaskService:
    """升级任务业务逻辑（repository 组合 + 门禁/灰度/通知编排）。"""

    def __init__(
        self,
        task_repository: OtaTaskRepository,
        version_repository: OtaVersionRepository,
        record_repository: OtaRecordRepository,
        reader: VehicleStateReader,
        notify_producer: OtaNotifyProducer,
        command_producer: RollbackCommandProducer,
        settings: Settings,
        storage: OtaPackageStorage,
        redis_manager: object | None = None,
    ) -> None:
        self._tasks = task_repository
        self._versions = version_repository
        self._records = record_repository
        self._reader = reader
        self._notify = notify_producer
        self._commands = command_producer
        self._settings = settings
        self._storage = storage
        self._redis = redis_manager
        #: 后台任务强引用（审查 Y9：fire-and-forget 任务无引用可能被 GC 回收，
        #: 异常也无人观察；由 :meth:`aclose` 在停机时统一等待收尾）
        self._background_tasks: set[asyncio.Task[None]] = set()

    async def aclose(self) -> None:
        """等待后台任务收尾（应用停机时调用；异常已在完成回调中记录，不向外抛）。"""
        pending = list(self._background_tasks)
        if not pending:
            return
        await asyncio.gather(*pending, return_exceptions=True)

    # ---------- 查询 ----------

    async def list_tasks(
        self,
        *,
        status: OtaTaskStatus | None = None,
        target_version_id: UUID | None = None,
        vehicle_id: str | None = None,
        creator: UUID | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> OtaTaskList:
        """任务列表（create_time DESC；target_vehicles 置 null 控制响应体积）。"""
        rows, total = await self._tasks.list_page(
            status=status,
            target_version_id=target_version_id,
            vehicle_id=vehicle_id,
            creator=creator,
            page=page,
            page_size=page_size,
        )
        items = [self._to_item(row, include_vehicles=False) for row in rows]
        return OtaTaskList(items=items, total=total, page=page, page_size=page_size)

    async def get_task(self, task_id: UUID) -> OtaTaskDetail:
        """任务详情（含目标版本快照 + 实时灰度视图 rollout）。"""
        task = await self._require_task(task_id)
        version_row = await self._versions.get(task.target_version_id)
        if version_row is None:
            raise ResourceNotFoundError(details={"target_version_id": str(task.target_version_id)})
        snapshots = await self._records.snapshot_by_task(task_id)
        strategy = self._strategy_of(task)
        allocation = allocate_batches(
            list(task.target_vehicles or []),
            [batch.percent for batch in strategy.batches],
        )
        rollout = build_rollout_view(
            strategy=strategy, allocation=allocation, snapshots=snapshots, now=time.time()
        )
        progress = build_task_progress(
            total_vehicles=len(task.target_vehicles or []),
            snapshots=snapshots,
            current_batch=rollout.current_batch,
        )
        item = self._to_item(task, include_vehicles=True)
        item.progress = progress
        from app.schemas.versions import (
            OtaVersionItem,  # 局部导入避免循环
        )

        target_version = OtaVersionItem(
            version_id=version_row.version_id,
            version_name=version_row.version_name,
            version_code=version_row.version_code,
            release_type=version_row.release_type,
            package_url=version_row.package_url,
            package_size=version_row.package_size,
            package_md5=version_row.package_md5,
            package_sha256=version_row.package_sha256,
            signature=version_row.signature,
            changelog=version_row.changelog,
            applicable_models=list(version_row.applicable_models),
            status=version_row.status,
            release_time=(
                version_row.release_time.timestamp() if version_row.release_time else None
            ),
            package_download_url=None,
        )
        return OtaTaskDetail(**item.model_dump(), target_version=target_version, rollout=rollout)


    # ---------- 创建（灰度批次规划，不下发） ----------

    async def create_task(self, payload: OtaTaskCreateRequest, user_id: str) -> OtaTaskItem:
        """创建任务并冻结灰度策略（2001 策略不一致/超上限；3001 版本不存在；3003 未发布）。"""
        vehicles = list(dict.fromkeys(payload.target_vehicles))  # 去重且保序
        if not vehicles:
            raise MissingParameterError(message="target_vehicles 为空（2002 参数缺失）")
        if len(vehicles) > self._settings.ota_task_max_target_vehicles:
            raise InvalidParameterError(
                message=(
                    f"目标车辆数 {len(vehicles)} 超过上限 "
                    f"{self._settings.ota_task_max_target_vehicles}（2001 参数错误）"
                ),
                details={"field": "target_vehicles", "limit": self._settings.ota_task_max_target_vehicles},
            )
        version = await self._versions.get(payload.target_version_id)
        if version is None:
            raise ResourceNotFoundError(details={"target_version_id": str(payload.target_version_id)})
        if version.status not in (OtaVersionStatus.PUBLISHED, OtaVersionStatus.DEPRECATED):
            raise ResourceStateConflictError(
                details={
                    "current_status": version.status.value,
                    "allowed_status": ["published", "deprecated"],
                }
            )

        strategy = resolve_strategy(payload.upgrade_strategy, self._settings)
        schedule = payload.schedule or OtaTaskSchedule(mode="immediate")
        if schedule.mode.value == "scheduled":
            if schedule.start_time is None:
                raise MissingParameterError(
                    message="schedule.mode=scheduled 时 start_time 必填（2002 参数缺失）"
                )
            if schedule.start_time <= time.time():
                raise InvalidParameterError(
                    message="schedule.start_time 必须大于当前时间（2001 参数错误）",
                    details={"field": "schedule.start_time", "value": schedule.start_time},
                )
        preconditions = payload.preconditions or OtaTaskPreconditions()
        self._validate_preconditions(preconditions)

        task = OtaTask(
            task_name=payload.task_name,
            target_version_id=payload.target_version_id,
            target_vehicles=vehicles,
            upgrade_strategy=strategy.model_dump(),
            schedule=schedule.model_dump(),
            preconditions=preconditions.model_dump(),
            status=OtaTaskStatus.CREATED,
            progress=OtaTaskProgress(total=len(vehicles), pending=len(vehicles), in_progress=0,
                                     succeeded=0, failed=0, rolled_back=0,
                                     success_rate=None, current_batch=0).model_dump(),
            creator=UUID(user_id),
        )
        async with self._tasks.transaction() as session:
            created = await self._tasks.create(session, task)
            await session.commit()
        logger.info(
            "ota_task_created",
            user_id=user_id,
            task_id=str(created.task_id),
            target_version_id=str(payload.target_version_id),
            vehicle_count=len(vehicles),
            trace_id=get_trace_id(),
        )
        return self._to_item(created, include_vehicles=True)

    def _validate_preconditions(self, preconditions: OtaTaskPreconditions) -> None:
        """门禁不得弱于平台门禁（放宽 → 2001，契约 OtaTaskCreateRequest.description）。"""
        platform = self._settings
        violations: dict[str, object] = {}
        if preconditions.soc_min < platform.ota_precondition_min_soc:
            violations["soc_min"] = platform.ota_precondition_min_soc
        if platform.ota_precondition_require_parked and not preconditions.must_be_parked:
            violations["must_be_parked"] = True
        if not preconditions.network_stable:
            violations["network_stable"] = True
        if preconditions.min_storage_mb < platform.ota_precondition_min_storage_mb:
            violations["min_storage_mb"] = platform.ota_precondition_min_storage_mb
        if violations:
            raise InvalidParameterError(
                message=(
                    "升级门禁不得弱于平台门禁（2001 参数错误；"
                    "电量 ≥ 50% / 静止(P 档) / 网络稳定 / 存储 ≥ 2GB 不可放宽）"
                ),
                details={"field": "preconditions", "minimum": violations},
            )


    # ---------- 行映射与冻结字段解析 ----------

    def _to_item(self, task: OtaTask, *, include_vehicles: bool) -> OtaTaskItem:
        """ORM 行 → OtaTaskItem（列表接口省略 target_vehicles；progress 取 JSONB 快照）。"""
        return OtaTaskItem(
            task_id=task.task_id,
            task_name=task.task_name,
            target_version_id=task.target_version_id,
            target_vehicles=list(task.target_vehicles) if include_vehicles else None,
            vehicle_count=len(task.target_vehicles or []),
            upgrade_strategy=self._strategy_of(task),
            schedule=self._schedule_of(task),
            preconditions=self._preconditions_of(task),
            status=task.status,
            progress=self._progress_of(task),
            creator=task.creator,
            create_time=task.create_time.timestamp(),
        )

    @staticmethod
    def _strategy_of(task: OtaTask) -> OtaUpgradeStrategy:
        """冻结灰度策略 JSONB → 模型（异常数据拒绝服务而非静默降级）。"""
        return OtaUpgradeStrategy.model_validate(task.upgrade_strategy)

    @staticmethod
    def _schedule_of(task: OtaTask) -> OtaTaskSchedule:
        """调度窗口 JSONB → 模型。"""
        return OtaTaskSchedule.model_validate(task.schedule or {"mode": "immediate"})

    @staticmethod
    def _preconditions_of(task: OtaTask) -> OtaTaskPreconditions:
        """升级门禁 JSONB → 模型。"""
        return OtaTaskPreconditions.model_validate(task.preconditions or {})

    @staticmethod
    def _progress_of(task: OtaTask) -> OtaTaskProgress:
        """进度快照 JSONB → 模型（缺省全零进度）。"""
        if task.progress:
            return OtaTaskProgress.model_validate(task.progress)
        return OtaTaskProgress(
            total=len(task.target_vehicles or []),
            pending=len(task.target_vehicles or []),
            in_progress=0,
            succeeded=0,
            failed=0,
            rolled_back=0,
            success_rate=None,
            current_batch=0,
        )

    async def _require_task(self, task_id: UUID) -> OtaTask:
        """按 ID 取任务，缺失抛 3001。"""
        task = await self._tasks.get(task_id)
        if task is None:
            raise ResourceNotFoundError(details={"task_id": str(task_id)})
        return task

    def _allocation_of(self, task: OtaTask) -> list[list[str]]:
        """批次车辆分配（确定性：target_vehicles 保序去重后按策略比例切分）。"""
        strategy = self._strategy_of(task)
        return allocate_batches(
            list(task.target_vehicles or []),
            [batch.percent for batch in strategy.batches],
        )

    def _current_batch_of(self, task: OtaTask) -> int:
        """当前批次号（progress JSONB 快照；0=尚未开始）。"""
        value = (task.progress or {}).get("current_batch", 0)
        try:
            return max(0, min(4, int(value)))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return 0

    def _audit(self, event: str, user_id: str, task_id: UUID, **extra: object) -> None:
        """审计日志（x-hunter-ota-security.audit；必含 user_id/task_id/trace_id）。"""
        logger.info(
            event,
            user_id=user_id,
            task_id=str(task_id),
            trace_id=get_trace_id(),
            **extra,
        )


    # ---------- 启动（门禁校验 + 下发批次通知） ----------

    async def start_task(
        self, task_id: UUID, payload: OtaTaskStartRequest | None, user_id: str
    ) -> OtaTaskActionData:
        """启动任务（created|pending_approval|paused → running；幂等重复启动不重复下发）。"""
        async with self._tasks.transaction() as session:
            task = await self._tasks.get_for_update(session, task_id)
            if task is None:
                raise ResourceNotFoundError(details={"task_id": str(task_id)})
            if task.status in _TERMINAL_TASK_STATUSES:
                raise ResourceStateConflictError(
                    details={"current_status": task.status.value, "allowed_status": ["running"]}
                )
            batch_no = self._resolve_target_batch(task, payload)
            version = await self._require_published_version(task)
            if task.status == OtaTaskStatus.RUNNING:
                # 幂等：运行中重复 start 不重复下发，仅回执当前批次状态
                return self._action_receipt(task, OtaTaskAction.START, batch_no=batch_no)
            released, blocked = await self._release_batch(session, task, batch_no, version)
            new_status = self._status_after_release(task, released, blocked)
            self._persist_progress(session, task, new_status, batch_no)
            await session.commit()

        await self._dispatch_notifications(task, released, version)
        self._write_progress_cache(task_id, task.progress)
        self._audit(
            "ota_task_started",
            user_id,
            task_id,
            batch_no=batch_no,
            released=[g.vehicle_id for g in released],
            blocked=[b.vehicle_id for b in blocked],
        )
        return self._action_data(
            task_id, OtaTaskAction.START, new_status, batch_no, released, blocked, reason=None
        )

    def _resolve_target_batch(self, task: OtaTask, payload: OtaTaskStartRequest | None) -> int:
        """目标批次：请求 batch_no 必须等于「当前批次 + 1」（禁止跳批，#3），缺省自动推进。"""
        current = self._current_batch_of(task)
        requested = payload.batch_no if payload else None
        if requested is not None and requested != current + 1:
            raise ResourceStateConflictError(
                message=(
                    f"禁止跳批（3003）：目标批次 {requested} 必须等于当前批次 + 1（{current + 1}）"
                ),
                details={
                    "current_batch": current,
                    "requested_batch": requested,
                    "reason": "batch_no must be current_batch + 1",
                },
            )
        return requested if requested is not None else current + 1

    async def _require_published_version(self, task: OtaTask):
        """目标版本必须为 published（否则 3003；缺失 3001）。"""
        version = await self._versions.get(task.target_version_id)
        if version is None:
            raise ResourceNotFoundError(details={"target_version_id": str(task.target_version_id)})
        if version.status != OtaVersionStatus.PUBLISHED:
            raise ResourceStateConflictError(
                details={"current_status": version.status.value, "allowed_status": ["published"]}
            )
        return version


    async def _release_batch(
        self, session, task: OtaTask, batch_no: int, version
    ) -> tuple[list[GateResult], list[OtaPreconditionFailure]]:
        """批次下发核心：逐车门禁 → 插入记录（幂等）→ 返回放行/拦截清单（不提交事务）。"""
        allocation = self._allocation_of(task)
        batch_vehicles = allocation[batch_no - 1] if batch_no <= len(allocation) else []
        if not batch_vehicles:
            return [], []
        existing = await self._records.map_by_vehicles(session, task.task_id, batch_vehicles)
        released: list[GateResult] = []
        blocked: list[OtaPreconditionFailure] = []
        to_insert: list[dict[str, object]] = []
        for vehicle_id in batch_vehicles:
            record = existing.get(vehicle_id)
            if record is not None and record.status != _PENDING_STATUS:
                # 幂等：已进入 DOWNLOAD/INSTALL/TEST/SUCCESS 的车辆不二次通知
                continue
            gate = await check_vehicle(self._reader, vehicle_id, self._settings)
            if gate.released:
                released.append(gate)
                to_insert.append(
                    {
                        "task_id": task.task_id,
                        "vehicle_id": vehicle_id,
                        "from_version": None,  # 车辆版本权威值属 vehicle-service（读模型无该字段）
                        "to_version": version.version_name,
                        "status": _PENDING_STATUS,
                        "phase": OtaStatus.IDLE,
                        "progress": 0,
                    }
                )
            elif gate.offline:
                blocked.append(
                    OtaPreconditionFailure(
                        vehicle_id=vehicle_id,
                        failed_conditions=[OtaPreconditionName.NETWORK_STABLE],
                        actual={"reason": "vehicle_offline"},
                    )
                )
            else:
                blocked.append(
                    OtaPreconditionFailure(
                        vehicle_id=vehicle_id,
                        failed_conditions=list(gate.failed_conditions),
                        actual=gate.actual,
                    )
                )
        if to_insert:
            await self._records.insert_released(session, to_insert)
        return released, blocked

    def _status_after_release(
        self, task: OtaTask, released: list[GateResult], blocked: list[OtaPreconditionFailure]
    ) -> OtaTaskStatus:
        """下发后任务状态：整批门禁全失败 → failed（告警日志）；否则 running。"""
        if not released and blocked:
            # 契约：整批门禁全失败 → 任务转 failed 并产生告警（alert_event 生产者属
            # data-analytics，本服务以 CRITICAL 审计日志承载，见模块 docstring）
            logger.critical(
                "ota_task_batch_fully_blocked",
                task_id=str(task.task_id),
                blocked=[b.vehicle_id for b in blocked],
            )
            return OtaTaskStatus.FAILED
        return OtaTaskStatus.RUNNING

    def _persist_progress(
        self, session, task: OtaTask, status: OtaTaskStatus, batch_no: int
    ) -> None:
        """更新任务状态 + 进度 JSONB 快照（调用方事务内；终态计数由消费链路刷新）。"""
        total = len(task.target_vehicles or [])
        task.status = status
        task.progress = OtaTaskProgress(
            total=total,
            pending=total,
            in_progress=0,
            succeeded=0,
            failed=0,
            rolled_back=0,
            success_rate=None,
            current_batch=(
                batch_no if status in (OtaTaskStatus.RUNNING, OtaTaskStatus.FAILED) else 0
            ),
        ).model_dump()
        session.add(task)


    async def _dispatch_notifications(self, task: OtaTask, released: list[GateResult], version) -> None:
        """事务提交后逐车下发 ota_notify（package_url = 发送时刻签发的 1h 预签名地址）。"""
        if not released:
            return
        object_key = self._object_key_from_url(version.package_url)
        package_url = await asyncio.to_thread(
            self._storage.presign_get, self._settings.minio_bucket_ota_packages, object_key
        )
        preconditions = self._preconditions_of(task).model_dump()
        for gate in released:
            await self._notify.send(
                vehicle_id=gate.vehicle_id,
                task_id=str(task.task_id),
                version_name=version.version_name,
                version_code=version.version_code,
                package_url=package_url,
                package_size=version.package_size,
                package_md5=version.package_md5,
                package_sha256=version.package_sha256,
                signature=version.signature,
                changelog=version.changelog,
                preconditions=preconditions,
            )

    def _write_progress_cache(self, task_id: UUID, progress: dict[str, object]) -> None:
        """同步 Redis ota:progress:{task_id}（Hash；TTL 86400s = 任务结束后保留 1 天）。

        fire-and-forget 任务：缓存写失败不阻断主链路（DB progress JSONB 为权威快照）。
        """
        if self._redis is None:
            return

        async def _write() -> None:
            key = f"ota:progress:{task_id}"
            client = self._redis.client  # type: ignore[attr-defined]
            await client.hset(
                key,
                mapping={
                    "task_id": str(task_id),
                    "updated_at": str(time.time()),
                    "progress": json.dumps(progress, ensure_ascii=False),
                },
            )
            await client.expire(key, 86400)

        def _on_done(task: asyncio.Task[None]) -> None:
            """完成回调：释放引用并观察异常（审查 Y9，避免 "exception was never retrieved"）。"""
            self._background_tasks.discard(task)
            if not task.cancelled() and task.exception() is not None:
                logger.warning(
                    "ota_progress_cache_write_failed",
                    task_id=str(task_id),
                    error=str(task.exception()),
                )

        task = asyncio.get_running_loop().create_task(
            _write(), name=f"ota-progress-cache-{task_id}"
        )
        self._background_tasks.add(task)
        task.add_done_callback(_on_done)

    def _action_receipt(self, task: OtaTask, action: OtaTaskAction, *, batch_no: int | None) -> OtaTaskActionData:
        """幂等回执（重复 start：released/blocked 均为空，状态保持 running）。"""
        return OtaTaskActionData(
            task_id=task.task_id,
            action=action,
            status=task.status,
            batch_no=batch_no,
            released_vehicles=[],
            blocked_vehicles=[],
            released_count=0,
            blocked_count=0,
            executed_at=time.time(),
            operator_id=None,
            reason=None,
        )

    def _action_data(
        self,
        task_id: UUID,
        action: OtaTaskAction,
        status: OtaTaskStatus,
        batch_no: int | None,
        released: list[GateResult],
        blocked: list[OtaPreconditionFailure],
        *,
        reason: str | None,
    ) -> OtaTaskActionData:
        """动作回执（契约 OtaTaskActionData；released/blocked 逐车明细）。"""
        return OtaTaskActionData(
            task_id=task_id,
            action=action,
            status=status,
            batch_no=batch_no,
            released_vehicles=[g.vehicle_id for g in released],
            blocked_vehicles=blocked,
            released_count=len(released),
            blocked_count=len(blocked),
            executed_at=time.time(),
            operator_id=None,
            reason=reason,
        )

    @staticmethod
    def _object_key_from_url(package_url: str) -> str:
        """从 s3://{bucket}/{key} 还原对象键（通知载荷需预签名地址）。"""
        prefix = "s3://"
        if package_url.startswith(prefix):
            return package_url[len(prefix) :].split("/", 1)[1]
        return package_url


    # ---------- 暂停 / 恢复 / 终止 ----------

    async def pause_task(
        self, task_id: UUID, payload: OtaTaskPauseRequest | None, user_id: str
    ) -> OtaTaskActionData:
        """暂停（running → paused：冻结批次推进，不影响已下发车辆）。"""
        async with self._tasks.transaction() as session:
            task = await self._tasks.get_for_update(session, task_id)
            if task is None:
                raise ResourceNotFoundError(details={"task_id": str(task_id)})
            if task.status != OtaTaskStatus.RUNNING:
                raise ResourceStateConflictError(
                    details={"current_status": task.status.value, "allowed_status": ["running"]}
                )
            batch_no = self._current_batch_of(task)
            task.status = OtaTaskStatus.PAUSED
            await session.commit()

        self._write_progress_cache(task_id, task.progress)
        reason = payload.reason if payload else None
        self._audit(
            "ota_task_paused", user_id, task_id, batch_no=batch_no, reason=reason
        )
        return OtaTaskActionData(
            task_id=task_id,
            action=OtaTaskAction.PAUSE,
            status=OtaTaskStatus.PAUSED,
            batch_no=batch_no,
            released_vehicles=[],
            blocked_vehicles=[],
            released_count=0,
            blocked_count=0,
            executed_at=time.time(),
            operator_id=None,
            reason=reason,
        )

    async def resume_task(
        self, task_id: UUID, payload: OtaTaskStartRequest | None, user_id: str
    ) -> OtaTaskActionData:
        """恢复（paused → running）：重新评估当前批次——窗口未过观察、达标推进、未达标拒绝。"""
        async with self._tasks.transaction() as session:
            task = await self._tasks.get_for_update(session, task_id)
            if task is None:
                raise ResourceNotFoundError(details={"task_id": str(task_id)})
            if task.status != OtaTaskStatus.PAUSED:
                raise ResourceStateConflictError(
                    details={"current_status": task.status.value, "allowed_status": ["paused"]}
                )
            batch_no = self._resolve_target_batch(task, payload)
            version = await self._require_published_version(task)
            snapshots = await self._records.snapshot_by_task(task_id)
            strategy = self._strategy_of(task)
            allocation = self._allocation_of(task)
            rollout = build_rollout_view(
                strategy=strategy, allocation=allocation, snapshots=snapshots, now=time.time()
            )
            current = next(
                (b for b in rollout.batches if b.batch_no == rollout.current_batch), None
            )
            if current is not None and current.status.value == "halted":
                # 成功率 < 门禁 → 拒绝恢复（3003 + data.halt_reason），人工只能 rollback/cancel
                raise ResourceStateConflictError(
                    message=f"当前批次成功率未达门禁，拒绝恢复（3003）：{rollout.halt_reason}",
                    details={"halt_reason": rollout.halt_reason},
                )
            released, blocked = await self._release_batch(session, task, batch_no, version)
            new_status = OtaTaskStatus.RUNNING
            task.status = new_status
            self._persist_progress(session, task, new_status, batch_no)
            await session.commit()

        await self._dispatch_notifications(task, released, version)
        self._write_progress_cache(task_id, task.progress)
        note = payload.note if payload else None
        self._audit(
            "ota_task_resumed",
            user_id,
            task_id,
            batch_no=batch_no,
            released=[g.vehicle_id for g in released],
            blocked=[b.vehicle_id for b in blocked],
        )
        return self._action_data(
            task_id, OtaTaskAction.RESUME, new_status, batch_no, released, blocked, reason=note
        )

    async def cancel_task(
        self, task_id: UUID, payload: OtaTaskCancelRequest | None, user_id: str
    ) -> OtaTaskActionData:
        """终止（created|pending_approval|running|paused → canceled；终态重复终止 → 3003）。"""
        async with self._tasks.transaction() as session:
            task = await self._tasks.get_for_update(session, task_id)
            if task is None:
                raise ResourceNotFoundError(details={"task_id": str(task_id)})
            if task.status in _TERMINAL_TASK_STATUSES:
                raise ResourceStateConflictError(
                    message=f"任务已到达终态 {task.status.value}，禁止重复终止（3003）",
                    details={"current_status": task.status.value},
                )
            batch_no = self._current_batch_of(task) or None
            task.status = OtaTaskStatus.CANCELED
            await session.commit()

        reason = payload.reason if payload else None
        self._audit(
            "ota_task_canceled",
            user_id,
            task_id,
            canceled_batch_no=batch_no,
            dispatched_count=0,
            reason=reason,
        )
        return OtaTaskActionData(
            task_id=task_id,
            action=OtaTaskAction.CANCEL,
            status=OtaTaskStatus.CANCELED,
            batch_no=batch_no,
            released_vehicles=[],
            blocked_vehicles=[],
            released_count=0,
            blocked_count=0,
            executed_at=time.time(),
            operator_id=None,
            reason=reason,
        )


    # ---------- 人工回滚（A/B 分区，仅 SUCCESS 记录） ----------

    async def rollback_task(
        self, task_id: UUID, payload: OtaTaskRollbackRequest, user_id: str
    ) -> OtaTaskRollbackData:
        """人工回滚：对 SUCCESS 记录下发 ota_rollback 指令（逐车独立，单车失败不影响其余）。

        门禁：静止 + P 档 + 网络稳定（回退不刷写镜像，不要求电量 ≥ 50%）；
        非 SUCCESS 记录 → rejected（reason=status_not_allowed）；离线 → rejected（vehicle_offline）。
        """
        # 3001 守卫：任务不存在直接 404（返回值本方法不使用，故不赋值）
        await self._require_task(task_id)
        snapshots = await self._records.snapshot_by_task(task_id)
        success_vehicles = [
            v for v, s in snapshots.items() if s.status == OtaStatus.SUCCESS
        ]
        requested = list(payload.vehicle_ids) if payload.vehicle_ids else success_vehicles
        results: list[OtaTaskRollbackResultItem] = []
        accepted_count = 0
        topic_pattern = self._settings.vehicle_command_topic_pattern
        for vehicle_id in requested:
            snapshot = snapshots.get(vehicle_id)
            if snapshot is None:
                results.append(
                    OtaTaskRollbackResultItem(
                        vehicle_id=vehicle_id,
                        accepted=False,
                        reason="no_record",
                        current_status=None,
                    )
                )
                continue
            if snapshot.status != OtaStatus.SUCCESS:
                # 非 SUCCESS：PENDING/DOWNLOAD/INSTALL/TEST 升级中禁止回滚；
                # ROLLED_BACK/FAILED 幂等拒绝（已回滚）
                results.append(
                    OtaTaskRollbackResultItem(
                        vehicle_id=vehicle_id,
                        accepted=False,
                        reason="status_not_allowed",
                        current_status=snapshot.status,
                    )
                )
                continue
            gate = await check_vehicle(self._reader, vehicle_id, self._settings, require_soc=False)
            if gate.offline:
                results.append(
                    OtaTaskRollbackResultItem(
                        vehicle_id=vehicle_id,
                        accepted=False,
                        reason="vehicle_offline",
                        current_status=snapshot.status,
                    )
                )
                continue
            if not gate.released:
                # 静止/网络不满足 → 6003 语义（逐车 rejected，不抛全局异常）
                results.append(
                    OtaTaskRollbackResultItem(
                        vehicle_id=vehicle_id,
                        accepted=False,
                        reason="precondition_failed",
                        current_status=snapshot.status,
                    )
                )
                continue
            command_id = await self._commands.send_rollback(
                vehicle_id=vehicle_id,
                task_id=task_id,
                target=payload.target.value,
                reason=payload.reason,
                operator_id=UUID(user_id),
            )
            accepted_count += 1
            results.append(
                OtaTaskRollbackResultItem(
                    vehicle_id=vehicle_id,
                    accepted=True,
                    command_id=command_id,
                    topic=topic_pattern.format(vehicle_id=vehicle_id),
                )
            )
        rejected_count = len(results) - accepted_count
        self._audit(
            "ota_task_rollback",
            user_id,
            task_id,
            vehicles=requested,
            accepted_count=accepted_count,
            rejected_count=rejected_count,
            reason=payload.reason,
        )
        return OtaTaskRollbackData(
            task_id=task_id,
            target=payload.target,
            requested_count=len(requested),
            accepted_count=accepted_count,
            rejected_count=rejected_count,
            results=results,
            executed_at=time.time(),
            operator_id=UUID(user_id),
        )


__all__ = ["TaskService"]
