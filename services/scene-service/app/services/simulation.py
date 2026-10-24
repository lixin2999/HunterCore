"""场景下发/控制服务（4.4 节 7 步流程 + CARLA 接入方案 P1）。

流程：① 校验配置/状态/模式 → ② 生成 sim_id 并显式创建动态 Topic（失败 5001，不建实例）
→ ③ 创建 CARLA 实例（VIL 先原子占位 Redis 会话）→ ④ 下发场景配置 → ⑤ 返回实例信息。
⑥ 进度 / ⑦ 结果查询无状态代理 CARLA，另提供 pause/resume/cancel 控制。

状态映射见 :mod:`app.services.sim_status`：未知 CARLA 状态返回 failed（不再静默回落
running）。VIL 唯一会话由 ``vil:session:{vehicle_id}`` 的 SET NX 保证。
"""
from __future__ import annotations

import asyncio
from collections import OrderedDict
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from hunter_common.exceptions import (
    InvalidParameterError,
    ResourceNotFoundError,
    ResourceStateConflictError,
    ServiceUnavailableError,
)
from hunter_common.logging import get_logger
from pydantic import ValidationError

from app.config import Settings
from app.repositories.carla import (
    ACTIVE_STATUSES,
    CarlaManagementClient,
    CarlaSimIdMismatchError,
    SimInstance,
    SimInstanceSnapshot,
)
from app.repositories.simulations import (
    TopicProvisioner,
    VilSessionStore,
    carla_topic_names,
)
from app.schemas.scene import (
    SceneConfig,
    SceneRunData,
    SceneRunRequest,
    SimulationArtifact,
    SimulationControlData,
    SimulationMode,
    SimulationProgress,
    SimulationResult,
    SimulationStatus,
)
from app.services.scenes import SceneService
from app.services.sim_status import is_paused, normalize_carla_status, to_platform_status

logger = get_logger("app.services.simulation")

#: 可下发状态（契约 x-hunter-lifecycle.deployable_states）
DEPLOYABLE_STATES = frozenset({"published"})

#: 已占位但未绑定 CARLA 实例的会话宽限期（秒）：覆盖 create_instance + submit_scenario 耗时，
#: 超过后视为崩溃残留，启动对账时清理（V1.23.4）。
UNBOUND_SESSION_GRACE_SECONDS = 300

#: 已终态实例的内存去重上限（避免重复调度 Topic 删除；仅本进程生命周期）
_MAX_FINALIZED_INSTANCES = 4096


def _forbidden_match(forbidden: frozenset[str], path: str) -> bool:
    """路径是否命中禁止项（支持 ``map`` 与 ``map.map_id`` 两种粒度）。"""
    return any(path == item or path.startswith(f"{item}.") for item in forbidden)


def _set_by_path(target: dict[str, Any], path: str, value: Any) -> None:
    """按点分路径写入（支持列表下标；中间节点缺失/类型不符 → 2001）。"""
    segments = path.split(".")
    cursor: Any = target
    for segment in segments[:-1]:
        if isinstance(cursor, list):
            if not segment.isdigit() or int(segment) >= len(cursor):
                raise InvalidParameterError(f"参数覆盖路径不存在: {path}", details={"path": path})
            cursor = cursor[int(segment)]
        elif isinstance(cursor, dict):
            if segment not in cursor:
                raise InvalidParameterError(f"参数覆盖路径不存在: {path}", details={"path": path})
            cursor = cursor[segment]
        else:
            raise InvalidParameterError(f"参数覆盖路径不可写: {path}", details={"path": path})
    last = segments[-1]
    if isinstance(cursor, list):
        if not last.isdigit() or int(last) >= len(cursor):
            raise InvalidParameterError(f"参数覆盖路径不存在: {path}", details={"path": path})
        cursor[int(last)] = value
        return
    if not isinstance(cursor, dict):
        raise InvalidParameterError(f"参数覆盖路径不可写: {path}", details={"path": path})
    cursor[last] = value


def apply_param_overrides(
    config: SceneConfig, overrides: dict[str, Any], settings: Settings
) -> tuple[SceneConfig, dict[str, Any]]:
    """按白名单深合并参数覆盖（点分路径；越权路径/非法结构 → 2001）。"""
    if not overrides:
        return config, {}
    allowed = settings.param_override_allowed_set
    forbidden = settings.param_override_forbidden_set
    merged = config.model_dump(mode="json")
    applied: dict[str, Any] = {}
    for path, value in overrides.items():
        top = path.split(".")[0]
        if _forbidden_match(forbidden, path):
            raise InvalidParameterError(
                f"参数覆盖路径不允许: {path}", details={"path": path}
            )
        if top not in allowed:
            raise InvalidParameterError(
                f"参数覆盖路径不在白名单内: {path}",
                details={"path": path, "allowed": sorted(allowed)},
            )
        _set_by_path(merged, path, value)
        applied[path] = value
    try:
        return SceneConfig.model_validate(merged), applied
    except ValidationError as exc:
        raise InvalidParameterError(
            "参数覆盖后场景配置非法", details={"paths": sorted(applied)}
        ) from exc


class SceneSimulationService:
    """场景下发/控制服务（CARLA 不可达统一 5001）。"""

    def __init__(
        self,
        scenes: SceneService,
        carla: CarlaManagementClient,
        settings: Settings,
        storage: Any | None = None,
        *,
        sessions: VilSessionStore | None = None,
        topics: TopicProvisioner | None = None,
        topic_delete_delay_seconds: int | None = None,
    ) -> None:
        self._scenes = scenes
        self._carla = carla
        self._settings = settings
        self._storage = storage
        self._sessions = sessions
        self._topics = topics
        self._topic_delete_delay = (
            settings.sim_topic_delete_delay_seconds
            if topic_delete_delay_seconds is None
            else max(0, topic_delete_delay_seconds)
        )
        self._cleanup_tasks: set[asyncio.Task[None]] = set()
        # 终态清理去重：同一实例并发查询 get_progress/get_result/cancel 时只释放/删除一次
        self._finalized: OrderedDict[str, None] = OrderedDict()

    @staticmethod
    def _new_sim_id() -> str:
        """平台生成 sim_id（CARLA 回传 Topic 前缀；匹配 ^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$）。"""
        stamp = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
        return f"sim-{stamp}-{uuid4().hex[:8]}"

    @staticmethod
    def _as_mode(value: Any) -> SimulationMode | None:
        try:
            return SimulationMode(str(value)) if value is not None else None
        except ValueError:
            return None

    async def run_scene(self, scene_id: UUID, payload: SceneRunRequest | None) -> SceneRunData:
        """下发场景到 CARLA 并返回实例信息（4.4 节第 1-5 步）。"""
        model = await self._scenes.load_scene_model(scene_id)
        status = str(model.status)
        if status not in DEPLOYABLE_STATES:
            raise ResourceStateConflictError(
                f"场景状态为 {status}，仅 published 可下发",
                details={"scene_id": str(scene_id), "status": status},
            )
        request = payload or SceneRunRequest()
        self._validate_mode_request(request)
        scene = self._scenes.to_schema(model)
        self._validate_duration(scene.config)
        merged, applied = apply_param_overrides(
            scene.config, request.param_overrides, self._settings
        )

        mode = request.mode
        vehicle_id = request.vehicle_id
        # SIL/REPLAY 保留场景级并行语义；VIL 的唯一性由车辆会话键保证（同场景允许多车）。
        if mode is not SimulationMode.VIL:
            existing = await self._carla.find_active_instance(str(scene_id))
            if existing is not None and not request.sim_config.allow_parallel:
                raise ResourceStateConflictError(
                    "该场景已有进行中的仿真实例（sim_config.allow_parallel=false）",
                    details={"scene_id": str(scene_id), "sim_instance_id": existing.instance_id},
                )
            if existing is not None:
                logger.warning(
                    "scene_simulation_reused",
                    scene_id=str(scene_id),
                    sim_instance_id=existing.instance_id,
                    overrides_ignored=sorted(applied),
                )
                existing_sim_id = existing.sim_id
                if not existing_sim_id and self._sessions is not None:
                    meta = await self._sessions.lookup_instance(existing.instance_id)
                    if meta and meta.get("sim_id"):
                        existing_sim_id = str(meta["sim_id"])
                # 兼容旧实例未登记 sim_id 的情况；新实例必须由 CARLA 回显平台 sim_id。
                existing_sim_id = existing_sim_id or existing.instance_id
                vil_topic, sensor_topic = carla_topic_names(existing_sim_id)
                return SceneRunData(
                    sim_instance_id=existing.instance_id,
                    sim_id=existing_sim_id,
                    scene_id=scene_id,
                    mode=mode,
                    vehicle_id=vehicle_id,
                    status=to_platform_status(existing.status),
                    carla_status=normalize_carla_status(existing.status),
                    vil_state_topic=vil_topic if mode is SimulationMode.VIL else None,
                    sensor_data_topic=sensor_topic,
                    started_at=datetime.now(UTC),
                    param_overrides={},
                )

        if mode is SimulationMode.VIL and self._sessions is not None and vehicle_id:
            active = await self._sessions.get(vehicle_id)
            if active is not None:
                raise ResourceStateConflictError(
                    "该车辆已有进行中的 VIL 会话",
                    details={"vehicle_id": vehicle_id, "sim_id": active.get("sim_id")},
                )

        sim_id = self._new_sim_id()
        vil_state_topic, sensor_data_topic = carla_topic_names(sim_id)
        created_topics: list[str] = []
        session_acquired = False
        instance: SimInstance | None = None
        try:
            # ② Topic 必须先于实例创建：失败直接 5001，避免孤儿实例
            if self._topics is not None:
                try:
                    created_topics = await self._topics.ensure_session(sim_id)
                except Exception as exc:
                    logger.exception("sim_topic_provision_failed", sim_id=sim_id)
                    raise ServiceUnavailableError(
                        "仿真动态 Topic 创建失败", details={"sim_id": sim_id}
                    ) from exc

            if mode is SimulationMode.VIL and self._sessions is not None and vehicle_id:
                session_acquired = await self._sessions.acquire(
                    {
                        "sim_id": sim_id,
                        "scene_id": str(scene_id),
                        "vehicle_id": vehicle_id,
                        "mode": mode.value,
                        "map_id": self._map_id(scene.config),
                        "vil_state_topic": vil_state_topic,
                        "sensor_data_topic": sensor_data_topic,
                        "calibration": request.sim_config.vil.calibration.model_dump(
                            mode="json"
                        )
                        if request.sim_config.vil
                        else None,
                        "created_at": datetime.now(UTC).timestamp(),
                        "expires_at": (
                            datetime.now(UTC) + timedelta(seconds=self._settings.vil_session_ttl_seconds)
                        ).timestamp(),
                    }
                )
                if not session_acquired:
                    raise ResourceStateConflictError(
                        "该车辆已有进行中的 VIL 会话",
                        details={"vehicle_id": vehicle_id, "sim_id": sim_id},
                    )

            try:
                instance = await self._carla.create_instance(
                    scene_id=str(scene_id),
                    scene_name=model.scene_name,
                    scene_config=merged.model_dump(mode="json"),
                    sim_id=sim_id,
                    mode=mode.value,
                    vehicle_id=vehicle_id,
                    vil=(
                        request.sim_config.vil.model_dump(mode="json")
                        if request.sim_config.vil
                        else None
                    ),
                    replay_config=(
                        request.replay_config.model_dump(mode="json")
                        if request.replay_config
                        else None
                    ),
                    kafka={
                        "vil_state_topic": vil_state_topic,
                        "sensor_data_topic": sensor_data_topic,
                    },
                    quality=request.sim_config.quality.value,
                )
            except CarlaSimIdMismatchError as exc:
                # CARLA 已建实例但 sim_id 不一致：先回收再上抛，避免孤儿实例（V1.23.4）。
                await self._stop_orphan(exc.instance_id, reason="sim_id_mismatch")
                raise
            await self._carla.submit_scenario(
                instance.instance_id,
                {
                    "sim_id": sim_id,
                    "scene_id": str(scene_id),
                    "scene_config": merged.model_dump(mode="json"),
                    "param_overrides": applied,
                    "mode": mode.value,
                    "vehicle_id": vehicle_id,
                    "kafka": {
                        "vil_state_topic": vil_state_topic,
                        "sensor_data_topic": sensor_data_topic,
                    },
                },
            )
            if self._sessions is not None:
                await self._sessions.bind_instance(
                    vehicle_id=vehicle_id or "",
                    sim_instance_id=instance.instance_id,
                    sim_id=sim_id,
                    scene_id=str(scene_id),
                    mode=mode.value,
                    vil_state_topic=vil_state_topic,
                    sensor_data_topic=sensor_data_topic,
                    instance_ttl_seconds=self._settings.sim_instance_ttl_seconds,
                )
            logger.info(
                "scene_simulation_started",
                scene_id=str(scene_id),
                sim_instance_id=instance.instance_id,
                sim_id=sim_id,
                mode=mode.value,
                vehicle_id=vehicle_id,
                override_count=len(applied),
            )
            return SceneRunData(
                sim_instance_id=instance.instance_id,
                sim_id=sim_id,
                scene_id=scene_id,
                mode=mode,
                vehicle_id=vehicle_id,
                status=to_platform_status(instance.status),
                carla_status=normalize_carla_status(instance.status),
                vil_state_topic=vil_state_topic if mode is SimulationMode.VIL else None,
                sensor_data_topic=sensor_data_topic,
                stream_url=self._stream_url(instance.data),
                started_at=datetime.now(UTC),
                param_overrides=applied,
            )
        except Exception:
            # 失败回滚：停止已创建的实例（避免孤儿）→ 释放会话 → 删除已创建 Topic
            # （均为 best effort；原异常继续上抛）。
            if instance is not None:
                await self._stop_orphan(instance.instance_id, reason="run_scene_rollback")
            if session_acquired and vehicle_id and self._sessions is not None:
                try:
                    await self._sessions.release(vehicle_id, sim_id=sim_id)
                except Exception:
                    logger.exception("vil_session_rollback_failed", vehicle_id=vehicle_id)
            if created_topics and self._topics is not None:
                try:
                    await self._topics.delete_session(sim_id)
                except Exception:
                    logger.exception("sim_topic_rollback_failed", sim_id=sim_id)
            raise

    def _validate_mode_request(self, request: SceneRunRequest) -> None:
        """模式必填项校验（Pydantic 已做；此处防御式兜底并给出 2001）。"""
        if request.mode is SimulationMode.VIL:
            if not request.vehicle_id:
                raise InvalidParameterError("mode=vil 时必须提供 vehicle_id")
            if request.sim_config.vil is None:
                raise InvalidParameterError("mode=vil 时必须提供 sim_config.vil")
            if request.sim_config.allow_parallel:
                # 同一车辆同时仅允许一个 VIL 会话；allow_parallel 与车辆会话唯一性冲突
                raise InvalidParameterError("mode=vil 不支持 sim_config.allow_parallel=true")
        if request.mode is SimulationMode.REPLAY:
            if not request.vehicle_id:
                raise InvalidParameterError("mode=replay 时必须提供 vehicle_id")
            if request.replay_config is None:
                raise InvalidParameterError("mode=replay 时必须提供 replay_config")
            if request.replay_config.vehicle_id != request.vehicle_id:
                raise InvalidParameterError(
                    "replay_config.vehicle_id 必须与 vehicle_id 一致",
                    details={
                        "vehicle_id": request.vehicle_id,
                        "replay_vehicle_id": request.replay_config.vehicle_id,
                    },
                )
        if (
            request.mode is SimulationMode.SIL
            and (request.sim_config.vil is not None or request.replay_config is not None)
        ):
            raise InvalidParameterError(
                "mode=sil 时 sim_config.vil 与 replay_config 必须为空"
            )

    @staticmethod
    def _map_id(config: SceneConfig) -> str | None:
        value = getattr(getattr(config, "map", None), "map_id", None)
        return str(value) if value else None

    @staticmethod
    def _stream_url(data: dict[str, Any]) -> str | None:
        for key in ("stream_url", "video_stream_url", "stream"):
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    def _validate_duration(self, config: SceneConfig) -> None:
        """下发前校验场景时长上限（上限经配置环境变量化）。"""
        limit = self._settings.scene_duration_max_seconds
        if config.duration > limit:
            raise InvalidParameterError(
                f"场景时长超过上限 {limit}s",
                details={"duration": config.duration, "limit": limit},
            )

    # ---------- ⑥⑦ 仿真进度/结果查询（G-20①，无状态代理） ----------

    async def get_progress(self, sim_instance_id: str) -> SimulationProgress:
        """查询仿真实例进度（CARLA 404 → 3001 已在仓储层处理）。"""
        snapshot = await self._carla.get_instance(sim_instance_id)
        meta = await self._lookup_meta(sim_instance_id)
        snapshot_status = to_platform_status(snapshot.status)
        message = self._as_text(snapshot.data, ("message", "detail"))
        if is_paused(snapshot.status) and not message:
            message = "paused"
        if snapshot_status.value not in ACTIVE_STATUSES:
            # 终态不一定有人调用 get_result/cancel；进度轮询也必须触发会话/Topic 回收，
            # 否则 VIL 会话会阻塞该车辆直到 TTL（V1.23.4）。
            await self._finalize_instance(meta, sim_instance_id)
        return SimulationProgress(
            sim_instance_id=snapshot.instance_id,
            scene_id=self._as_scene_id(snapshot.data),
            mode=self._as_mode(meta.get("mode") if meta else snapshot.data.get("mode")),
            vehicle_id=(
                str(meta.get("vehicle_id")) if meta and meta.get("vehicle_id") else None
            ),
            status=snapshot_status,
            carla_status=normalize_carla_status(snapshot.status),
            progress_percent=self._as_number(snapshot.data, ("progress_percent", "progress")),
            current_time_s=self._as_number(snapshot.data, ("current_time_s", "elapsed_s")),
            total_time_s=self._as_number(snapshot.data, ("total_time_s", "duration")),
            actors_count=self._as_int(snapshot.data, ("actors_count", "actors")),
            collisions=self._as_int(snapshot.data, ("collisions", "collision_count")),
            lane_invasions=self._as_int(snapshot.data, ("lane_invasions", "lane_invasion_count")),
            ego_state=self._as_object(snapshot.data, ("ego_state", "ego")),
            message=message,
            updated_at=self._as_datetime(snapshot.data, ("updated_at", "last_update")),
        )

    async def get_result(self, sim_instance_id: str) -> SimulationResult:
        """查询仿真实例结果；非终态 → 3003，终态查询后触发会话/Topic 清理。"""
        snapshot = await self._carla.get_result(sim_instance_id)
        status = to_platform_status(snapshot.status)
        if status.value in ACTIVE_STATUSES:
            raise ResourceStateConflictError(
                f"仿真仍在进行（{status.value}），结果未就绪",
                details={"sim_instance_id": sim_instance_id, "status": status.value},
            )
        meta = await self._lookup_meta(sim_instance_id)
        await self._finalize_instance(meta, sim_instance_id)
        return SimulationResult(
            sim_instance_id=snapshot.instance_id,
            scene_id=self._as_scene_id(snapshot.data),
            mode=self._as_mode(meta.get("mode") if meta else snapshot.data.get("mode")),
            vehicle_id=(
                str(meta.get("vehicle_id")) if meta and meta.get("vehicle_id") else None
            ),
            status=status,
            carla_status=normalize_carla_status(snapshot.status),
            success=self._as_bool(snapshot.data, ("success",)),
            success_criteria_result=self._as_object(snapshot.data, ("success_criteria_result",)),
            evaluation=self._as_object(snapshot.data, ("evaluation",)),
            message=self._as_text(snapshot.data, ("message", "failure_reason")),
            artifacts=await self._build_artifacts(snapshot),
            started_at=self._as_datetime(snapshot.data, ("started_at",)),
            finished_at=self._as_datetime(snapshot.data, ("finished_at", "ended_at")),
        )

    # ---------- 控制面：pause / resume / cancel ----------

    async def pause_instance(self, sim_instance_id: str) -> SimulationControlData:
        """暂停运行中的实例；重复 pause 幂等返回 running + paused。"""
        snapshot, meta = await self._active_snapshot(sim_instance_id)
        if is_paused(snapshot.status):
            return SimulationControlData(
                sim_instance_id=sim_instance_id,
                status=SimulationStatus.RUNNING,
                carla_status="PAUSED",
                message="paused",
            )
        await self._carla.pause_instance(sim_instance_id)
        logger.info("simulation_paused", sim_instance_id=sim_instance_id, sim_id=meta.get("sim_id"))
        return SimulationControlData(
            sim_instance_id=sim_instance_id,
            status=SimulationStatus.RUNNING,
            carla_status="PAUSED",
            message="paused",
        )

    async def resume_instance(self, sim_instance_id: str) -> SimulationControlData:
        """恢复暂停的实例。"""
        await self._active_snapshot(sim_instance_id)
        await self._carla.resume_instance(sim_instance_id)
        logger.info("simulation_resumed", sim_instance_id=sim_instance_id)
        return SimulationControlData(
            sim_instance_id=sim_instance_id,
            status=SimulationStatus.RUNNING,
            carla_status="RUNNING",
        )

    async def cancel_instance(self, sim_instance_id: str) -> SimulationControlData:
        """终止实例并回收会话/Topic（终态返回 3003）。"""
        _snapshot, meta = await self._active_snapshot(sim_instance_id)
        await self._carla.stop_instance(sim_instance_id)
        await self._finalize_instance(meta, sim_instance_id)
        logger.info("simulation_canceled", sim_instance_id=sim_instance_id, sim_id=meta.get("sim_id"))
        return SimulationControlData(
            sim_instance_id=sim_instance_id,
            status=SimulationStatus.CANCELED,
            carla_status="CANCELED",
            message="canceled",
        )

    async def _active_snapshot(
        self, sim_instance_id: str
    ) -> tuple[SimInstanceSnapshot, dict[str, Any]]:
        snapshot = await self._carla.get_instance(sim_instance_id)
        status = to_platform_status(snapshot.status)
        if status.value not in ACTIVE_STATUSES:
            raise ResourceStateConflictError(
                f"仿真已处于终态（{status.value}）",
                details={"sim_instance_id": sim_instance_id, "status": status.value},
            )
        meta = await self._lookup_meta(sim_instance_id) or {}
        return snapshot, meta

    async def _lookup_meta(self, sim_instance_id: str) -> dict[str, Any] | None:
        if self._sessions is None:
            return None
        return await self._sessions.lookup_instance(sim_instance_id)

    async def _finalize_instance(
        self, meta: dict[str, Any] | None, sim_instance_id: str
    ) -> bool:
        """释放 VIL 会话并延迟删除动态 Topic（best effort，不阻断查询/控制响应）。

        返回 True 表示本次完成了清理；重复清理或释放失败返回 False。
        """
        if not meta:
            return False
        if not self._mark_finalized(sim_instance_id):
            return False
        vehicle_id = meta.get("vehicle_id")
        sim_id = meta.get("sim_id")
        if vehicle_id and self._sessions is not None:
            try:
                await self._sessions.release(
                    str(vehicle_id),
                    sim_id=str(sim_id) if sim_id else None,
                    sim_instance_id=sim_instance_id,
                )
            except Exception:
                # 释放失败（如 Redis 短暂不可用）：撤销去重标记，让下一次进度轮询/对账重试；
                # 否则该车辆会被残留会话阻塞到 TTL（release 本身幂等，重试安全）。
                self._finalized.pop(sim_instance_id, None)
                logger.exception("vil_session_release_failed", vehicle_id=str(vehicle_id))
                return False
        if sim_id and self._topics is not None:
            self._schedule_topic_delete(str(sim_id))
        return True

    def _mark_finalized(self, sim_instance_id: str) -> bool:
        """记录实例已清理；返回 False 表示本次是重复清理（并发查询/控制去重）。"""
        if sim_instance_id in self._finalized:
            return False
        self._finalized[sim_instance_id] = None
        while len(self._finalized) > _MAX_FINALIZED_INSTANCES:
            self._finalized.popitem(last=False)
        return True

    def _schedule_topic_delete(self, sim_id: str) -> None:
        if self._topic_delete_delay <= 0:
            task = asyncio.create_task(self._delete_topics(sim_id))
        else:
            task = asyncio.create_task(self._delete_topics_later(sim_id))
        self._cleanup_tasks.add(task)
        task.add_done_callback(self._cleanup_tasks.discard)

    async def _delete_topics_later(self, sim_id: str) -> None:
        await asyncio.sleep(self._topic_delete_delay)
        await self._delete_topics(sim_id)

    async def shutdown(self) -> None:
        """取消未执行的延迟清理任务（服务优雅停机；已进入删除的等待其完成）。"""
        pending = [task for task in self._cleanup_tasks if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    async def reconcile_sessions(self) -> int:
        """启动对账：清理无实例、实例已回收或已终态的 VIL 会话（设计 6.4）。

        - ``vil:active`` 中有成员但 ``vil:session`` 已过期 → 移除成员；
        - 会话已占位但超过宽限期仍未绑定 CARLA 实例（创建中崩溃）→ 释放并删 Topic；
        - CARLA 返回 404 / 终态 → 释放会话并延迟删除 Topic；
        - CARLA 暂不可达 → 跳过，留给下次启动或 TTL 兜底。

        返回清理的会话数。单条失败不影响其余会话。
        """
        if self._sessions is None:
            return 0
        try:
            active = await self._sessions.list_active()
        except ServiceUnavailableError:
            logger.warning("vil_session_reconcile_list_failed")
            return 0
        cleaned = 0
        now = datetime.now(UTC).timestamp()
        for vehicle_id in active:
            try:
                session = await self._sessions.get(vehicle_id)
            except ServiceUnavailableError:
                logger.warning("vil_session_reconcile_read_failed", vehicle_id=vehicle_id)
                continue
            if session is None:
                try:
                    await self._sessions.release(vehicle_id)
                    cleaned += 1
                except ServiceUnavailableError:
                    logger.warning(
                        "vil_session_reconcile_release_failed", vehicle_id=vehicle_id
                    )
                continue
            sim_id = str(session.get("sim_id") or "") or None
            sim_instance_id = str(session.get("sim_instance_id") or "") or None
            if sim_instance_id is None:
                created_at = self._as_epoch(session.get("created_at"))
                if created_at is None or now - created_at > UNBOUND_SESSION_GRACE_SECONDS:
                    try:
                        await self._sessions.release(vehicle_id, sim_id=sim_id)
                        if sim_id:
                            self._schedule_topic_delete(sim_id)
                        cleaned += 1
                        logger.warning(
                            "vil_session_unbound_reconciled",
                            vehicle_id=vehicle_id,
                            sim_id=sim_id,
                        )
                    except ServiceUnavailableError:
                        logger.warning(
                            "vil_session_reconcile_release_failed", vehicle_id=vehicle_id
                        )
                continue
            try:
                snapshot = await self._carla.get_instance(sim_instance_id)
            except ResourceNotFoundError:
                try:
                    await self._sessions.release(
                        vehicle_id, sim_id=sim_id, sim_instance_id=sim_instance_id
                    )
                    if sim_id:
                        self._schedule_topic_delete(sim_id)
                    cleaned += 1
                    logger.warning(
                        "vil_session_instance_missing_reconciled",
                        vehicle_id=vehicle_id,
                        sim_instance_id=sim_instance_id,
                    )
                except ServiceUnavailableError:
                    logger.warning(
                        "vil_session_reconcile_release_failed", vehicle_id=vehicle_id
                    )
                continue
            except ServiceUnavailableError:
                # CARLA 暂不可达：保留会话，避免误删仍在运行的实例
                logger.warning(
                    "vil_session_reconcile_carla_unavailable",
                    vehicle_id=vehicle_id,
                    sim_instance_id=sim_instance_id,
                )
                continue
            if (
                to_platform_status(snapshot.status).value not in ACTIVE_STATUSES
                and await self._finalize_instance(session, sim_instance_id)
            ):
                cleaned += 1
        if cleaned:
            logger.info("vil_session_reconciled", cleaned=cleaned, active=len(active))
        return cleaned

    @staticmethod
    def _as_epoch(value: Any) -> float | None:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        return float(value)

    async def _stop_orphan(self, sim_instance_id: str, *, reason: str) -> None:
        """尽力停止一个平台不再跟踪的 CARLA 实例（失败仅告警，不掩盖原始异常）。"""
        try:
            await self._carla.stop_instance(sim_instance_id)
            logger.info(
                "carla_orphan_instance_stopped",
                sim_instance_id=sim_instance_id,
                reason=reason,
            )
        except Exception:
            logger.exception(
                "carla_orphan_instance_stop_failed",
                sim_instance_id=sim_instance_id,
                reason=reason,
            )

    async def _delete_topics(self, sim_id: str) -> None:
        if self._topics is None:
            return
        try:
            await self._topics.delete_session(sim_id)
            logger.info("sim_topics_deleted", sim_id=sim_id)
        except Exception:
            logger.exception("sim_topics_delete_failed", sim_id=sim_id)

    async def _build_artifacts(self, snapshot: SimInstanceSnapshot) -> list[SimulationArtifact]:
        """解析产物清单；object_key 落 hunter-scene-assets 时换发预签名 URL（15 分钟）。"""
        raw = snapshot.data.get("artifacts") or snapshot.data.get("files") or []
        if not isinstance(raw, list):
            logger.warning(
                "carla_artifacts_shape_unknown", sim_instance_id=snapshot.instance_id
            )
            return []
        artifacts: list[SimulationArtifact] = []
        for entry in raw:
            if not isinstance(entry, dict):
                continue
            name = str(entry.get("name") or entry.get("file_name") or "").strip()
            if not name:
                continue
            object_key = entry.get("object_key") or entry.get("key")
            size_bytes = entry.get("size_bytes") if isinstance(entry.get("size_bytes"), int) else None
            download_url: str | None = None
            expires_in: int | None = None
            if object_key and self._storage is not None:
                try:
                    download_url = await self._storage.presign_download(str(object_key))
                    expires_in = self._storage.presign_expire_seconds
                except ServiceUnavailableError:
                    logger.warning(
                        "carla_artifact_presign_failed",
                        sim_instance_id=snapshot.instance_id,
                        object_key=str(object_key),
                    )
            artifacts.append(
                SimulationArtifact(
                    name=name,
                    object_key=str(object_key) if object_key else None,
                    size_bytes=size_bytes,
                    download_url=download_url,
                    expires_in=expires_in,
                )
            )
        return artifacts

    # ---------- 内部：CARLA 响应宽松解析 ----------

    @staticmethod
    def _as_scene_id(data: dict[str, Any]) -> UUID | None:
        raw = data.get("scene_id")
        if not raw:
            return None
        try:
            return UUID(str(raw))
        except ValueError:
            logger.warning("carla_scene_id_invalid", scene_id=str(raw))
            return None

    @staticmethod
    def _as_number(data: dict[str, Any], keys: tuple[str, ...]) -> float | None:
        for key in keys:
            value = data.get(key)
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                return float(value)
        return None

    @staticmethod
    def _as_int(data: dict[str, Any], keys: tuple[str, ...]) -> int | None:
        for key in keys:
            value = data.get(key)
            if isinstance(value, int) and not isinstance(value, bool):
                return value
        return None

    @staticmethod
    def _as_bool(data: dict[str, Any], keys: tuple[str, ...]) -> bool | None:
        for key in keys:
            value = data.get(key)
            if isinstance(value, bool):
                return value
        return None

    @staticmethod
    def _as_text(data: dict[str, Any], keys: tuple[str, ...]) -> str | None:
        for key in keys:
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()[:512]
        return None

    @staticmethod
    def _as_object(data: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any] | None:
        for key in keys:
            value = data.get(key)
            if isinstance(value, dict):
                return value
        return None

    @staticmethod
    def _as_datetime(data: dict[str, Any], keys: tuple[str, ...]) -> datetime | None:
        for key in keys:
            value = data.get(key)
            if not value:
                continue
            try:
                parsed = datetime.fromisoformat(str(value))
            except ValueError:
                logger.warning("carla_timestamp_unparsable", key=key, value=str(value))
                return None
            return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        return None


__all__ = ["DEPLOYABLE_STATES", "SceneSimulationService", "apply_param_overrides"]
