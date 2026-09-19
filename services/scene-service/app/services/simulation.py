"""场景下发服务（4.4 节 7 步流程的第 1-5 步）。

① 校验配置完整性 + 状态必须 ``published`` → ② 调用 Carla 管理 API 创建仿真实例（失败 5001）→
③ 下发场景配置 JSON（``param_overrides`` 白名单深合并）→ ④ Carla 加载地图/参与者/环境 →
⑤ 返回 ``sim_instance_id``，状态置 ``running``。
⑥ 进度监控 / ⑦ 结果保存设计文档未定义端点（契约待确认 #2），本服务不实现、不臆造。
并行规则（契约 runScene）：同场景已有进行中实例时，``sim_config.allow_parallel=false`` → 3003；
``true`` → 复用该实例（不重复创建；此时参数覆盖不再下发，回显为空并记 warning）。
"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from hunter_common.exceptions import InvalidParameterError, ResourceStateConflictError
from hunter_common.logging import get_logger
from pydantic import ValidationError

from app.config import Settings
from app.repositories.carla import CarlaManagementClient
from app.schemas.scene import (
    SceneConfig,
    SceneRunData,
    SceneRunRequest,
    SimulationStatus,
)
from app.services.scenes import SceneService

logger = get_logger("app.services.simulation")

#: 可下发状态（契约 x-hunter-lifecycle.deployable_states）
DEPLOYABLE_STATES = frozenset({"published"})


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
    """按白名单深合并参数覆盖（点分路径；越权路径/非法结构 → 2001）。

    白名单与禁止路径均来自配置（``SCENE_PARAM_OVERRIDE_ALLOWED_PATHS`` /
    ``SCENE_PARAM_OVERRIDE_FORBIDDEN_PATHS``，契约禁止覆盖 ``map.map_id``）。
    """
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
    """场景下发服务（4.4 节；Carla 不可达统一 5001）。"""

    def __init__(
        self, scenes: SceneService, carla: CarlaManagementClient, settings: Settings
    ) -> None:
        self._scenes = scenes
        self._carla = carla
        self._settings = settings

    async def run_scene(self, scene_id: UUID, payload: SceneRunRequest | None) -> SceneRunData:
        """下发场景到 Carla 仿真并返回仿真实例信息（4.4 节第 1-5 步）。"""
        model = await self._scenes.load_scene_model(scene_id)
        status = str(model.status)
        if status not in DEPLOYABLE_STATES:
            raise ResourceStateConflictError(
                f"场景状态为 {status}，仅 published 可下发",
                details={"scene_id": str(scene_id), "status": status},
            )
        request = payload or SceneRunRequest()
        scene = self._scenes.to_schema(model)
        self._validate_duration(scene.config)
        merged, applied = apply_param_overrides(
            scene.config, request.param_overrides, self._settings
        )
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
            return SceneRunData(
                sim_instance_id=existing.instance_id,
                scene_id=scene_id,
                status=self._as_status(existing.status),
                started_at=datetime.now(UTC),
                param_overrides={},
            )
        instance = await self._carla.create_instance(
            scene_id=str(scene_id),
            scene_name=model.scene_name,
            scene_config=merged.model_dump(mode="json"),
        )
        await self._carla.submit_scenario(
            instance.instance_id,
            {
                "scene_id": str(scene_id),
                "scene_config": merged.model_dump(mode="json"),
                "param_overrides": applied,
            },
        )
        logger.info(
            "scene_simulation_started",
            scene_id=str(scene_id),
            sim_instance_id=instance.instance_id,
            override_count=len(applied),
        )
        return SceneRunData(
            sim_instance_id=instance.instance_id,
            scene_id=scene_id,
            status=self._as_status(instance.status),
            started_at=datetime.now(UTC),
            param_overrides=applied,
        )

    def _validate_duration(self, config: SceneConfig) -> None:
        """下发前校验场景时长上限（上限经配置环境变量化，契约待确认 #9）。"""
        limit = self._settings.scene_duration_max_seconds
        if config.duration > limit:
            raise InvalidParameterError(
                f"场景时长超过上限 {limit}s",
                details={"duration": config.duration, "limit": limit},
            )

    @staticmethod
    def _as_status(raw: str) -> SimulationStatus:
        """Carla 状态字符串 → 契约 SimulationStatus（未知取值回落 running 并告警）。"""
        try:
            return SimulationStatus(raw)
        except ValueError:
            logger.warning("carla_status_unknown", status=raw)
            return SimulationStatus.RUNNING


__all__ = ["DEPLOYABLE_STATES", "SceneSimulationService", "apply_param_overrides"]