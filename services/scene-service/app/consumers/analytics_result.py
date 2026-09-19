"""实车场景自动提取消费者（设计文档 4.5 节 + 契约 x-hunter-real-vehicle-extraction）。

- Topic：``analytics_result``（消费组 ``scene-service-analytics-result``，契约 consumer-groups.yaml）；
- 触发条件：``harsh_braking`` / ``collision_warning`` / ``manual_takeover``（其它结果类型与事件跳过）；
- 截取窗口：事件前后各 10 秒（契约固定，不可配置）；
- 提取内容：自车轨迹 / 目标轨迹 / 环境条件 → 4.2.2 结构 config；输出 ``real_vehicle_replay`` + ``draft``；
- 幂等键：``(vehicle_id, window_start)``（经 scene_name 唯一索引落地；重复消息跳过并记日志）；
- 异常消息抛 ValueError，由 KafkaConsumerManager 转投 DLQ（``analytics_result.dlq``）。
"""
from __future__ import annotations

from typing import Any

from hunter_common.exceptions import ResourceAlreadyExistsError
from hunter_common.kafka.consumer import KafkaConsumerManager
from hunter_common.logging import get_logger

from app.config import Settings
from app.schemas.scene import (
    Actor,
    ActorBehavior,
    ActorType,
    EgoVehicle,
    MapType,
    Scene,
    SceneConfig,
    SceneCreateRequest,
    SceneEvent,
    SceneEventAction,
    SceneEventTrigger,
    SceneEventType,
    SceneMap,
    SceneType,
    SpawnPoint,
    SuccessCriteria,
    Weather,
)
from app.services.scenes import SceneService

logger = get_logger("app.consumers.analytics_result")

#: 契约 4.5 节触发事件（harsh_braking / collision_warning / manual_takeover）
TRIGGER_RULES: dict[str, tuple[SceneEventType, str]] = {
    "harsh_braking": (SceneEventType.EMERGENCY_BRAKE, "emergency_stop"),
    "collision_warning": (SceneEventType.COLLISION, "emergency_stop"),
    "manual_takeover": (SceneEventType.MANUAL_TAKEOVER, "manual_takeover"),
}
#: 契约固定截取窗口（4.5 节：事件前后各 10 秒，不可更改）
CLIP_PRE_SECONDS = 10
CLIP_POST_SECONDS = 10
#: 必填字段（契约 analytics_result.schema.json required）
_REQUIRED_FIELDS = ("vehicle_id", "result_id", "result_type", "window_start", "window_end", "timestamp")
#: 结果类型受控词表（契约 result_type enum）
_RESULT_TYPES = frozenset({"metric", "corner_case", "report"})
#: 晴天基准天气（Carla WeatherParameters 语义）
_CLEAR_WEATHER: dict[str, float] = {
    "cloudiness": 0.0,
    "rain": 0.0,
    "wetness": 0.0,
    "fog": 0.0,
    "wind": 0.0,
    "sun_azimuth": 0.0,
    "sun_altitude": 45.0,
}


def _validate_payload(payload: Any) -> dict[str, Any]:
    """校验消息必填字段与受控取值（违规抛 ValueError → DLQ，禁止静默丢弃）。

    消息载荷类型不符（非 JSON 对象）属「消息内容非法」而非编程错误，
    沿用 ValueError 以保持 DLQ 单一异常语义（契约 x-hunter-real-vehicle-extraction）。
    """
    if not isinstance(payload, dict):
        raise ValueError("analytics_result 消息必须为 JSON 对象")  # noqa: TRY004
    for field in _REQUIRED_FIELDS:
        if payload.get(field) in (None, ""):
            raise ValueError(f"analytics_result 缺少必填字段: {field}")
    result_type = str(payload["result_type"])
    if result_type not in _RESULT_TYPES:
        raise ValueError(f"analytics_result 结果类型非法: {result_type}")
    window_start = float(payload["window_start"])
    window_end = float(payload["window_end"])
    if window_end <= window_start:
        raise ValueError("analytics_result window_end 必须大于 window_start")
    clip = payload.get("clip")
    if isinstance(clip, dict):
        if clip.get("pre_seconds") not in (None, CLIP_PRE_SECONDS):
            raise ValueError("analytics_result clip.pre_seconds 必须为 10（4.5 节固定窗口）")
        if clip.get("post_seconds") not in (None, CLIP_POST_SECONDS):
            raise ValueError("analytics_result clip.post_seconds 必须为 10（4.5 节固定窗口）")
    return payload


def _weather_for(environment: dict[str, Any]) -> Weather:
    """环境条件映射（环境字符串 → Carla WeatherParameters；未知取值回落晴天基准）。"""
    raw = str(environment.get("weather") or "").lower()
    values = dict(_CLEAR_WEATHER)
    if "rain" in raw:
        values.update(cloudiness=90.0, rain=80.0, wetness=70.0, wind=20.0)
    elif "fog" in raw:
        values.update(cloudiness=60.0, fog=60.0, wetness=30.0)
    elif "snow" in raw:
        values.update(cloudiness=90.0, rain=60.0, wetness=80.0)
    elif "night" in raw:
        values.update(sun_altitude=-10.0, sun_azimuth=180.0)
    elif "cloud" in raw:
        values.update(cloudiness=60.0)
    return Weather(**values)


def _point_from(entry: dict[str, Any]) -> SpawnPoint:
    """轨迹点 → SpawnPoint（yaw 取 heading；缺字段按 0 处理）。"""
    return SpawnPoint(
        x=float(entry.get("x") or 0.0),
        y=float(entry.get("y") or 0.0),
        z=float(entry.get("z") or 0.0),
        yaw=float(entry.get("heading") or 0.0),
    )


def _actors_from(objects: list[Any]) -> list[Actor]:
    """目标轨迹 → 4.2.2 参与者（轨迹本身落在 behavior.parameters，便于回放）。"""
    actors: list[Actor] = []
    for index, item in enumerate(objects):
        if not isinstance(item, dict):
            continue
        trajectory = [point for point in (item.get("trajectory") or []) if isinstance(point, dict)]
        if not trajectory:
            continue
        try:
            actor_type = ActorType(str(item.get("type") or "other"))
        except ValueError:
            actor_type = ActorType.OTHER
        actors.append(
            Actor(
                actor_id=str(item.get("object_id") or f"object-{index}")[:64],
                type=actor_type,
                spawn_point=_point_from(trajectory[0]),
                behavior=ActorBehavior(
                    behavior_type="trajectory_replay",
                    parameters={
                        "replay": True,
                        "source_object_id": item.get("object_id"),
                        "point_count": len(trajectory),
                        "trajectory": trajectory,
                    },
                ),
            )
        )
    return actors[:100]


class AnalyticsResultConsumer:
    """``analytics_result`` 消费者（4.5 节实车场景自动提取）。"""

    def __init__(self, settings: Settings, scene_service: SceneService) -> None:
        self._settings = settings
        self._scenes = scene_service

    # ---------- 消费主循环 ----------

    async def run(self) -> None:
        """启动消费循环（手动提交 offset；处理失败消息自动转投 DLQ）。"""
        manager = KafkaConsumerManager(
            self._settings,
            group_id=self._settings.scene_extraction_group_id,
            topics=[self._settings.analytics_result_topic],
            dlq_enabled=True,
            poll_timeout=self._settings.kafka_extraction_poll_timeout_seconds,
        )
        await manager.run(self._handle_message)

    async def _handle_message(self, message: Any, value: Any) -> None:
        """单条消息处理入口（校验失败抛 ValueError → DLQ）。"""
        payload = _validate_payload(value)
        await self.handle_payload(payload)

    # ---------- 业务处理（可独立单测） ----------

    async def handle_payload(self, payload: dict[str, Any]) -> Scene | None:
        """按 4.5 节规则生成实车回放场景；非 corner_case / 非触发事件返回 None（跳过）。"""
        data = _validate_payload(payload)
        if str(data["result_type"]) != "corner_case":
            return None
        trigger = str(data.get("trigger_event_type") or "")
        if trigger not in TRIGGER_RULES:
            logger.info("scene_extraction_trigger_skipped", trigger=trigger)
            return None
        request = self._build_request(data, trigger)
        try:
            scene = await self._scenes.create_scene(
                request, user_id=self._settings.scene_extraction_creator_id
            )
        except ResourceAlreadyExistsError:
            # 幂等键 (vehicle_id, window_start) 已存在 → 重复消息直接跳过（不重复建场景）
            logger.info(
                "scene_extraction_duplicate_skipped",
                scene_name=request.scene_name,
                vehicle_id=data["vehicle_id"],
            )
            return None
        logger.info(
            "scene_extracted",
            scene_id=str(scene.scene_id),
            vehicle_id=data["vehicle_id"],
            trigger=trigger,
        )
        return scene

    # ---------- 映射与构造 ----------

    def _build_request(self, data: dict[str, Any], trigger: str) -> SceneCreateRequest:
        """消息 → 场景创建请求（scene_type=real_vehicle_replay，status 由服务端置 draft）。"""
        config = self._build_config(data, trigger)
        window_start = float(data["window_start"])
        name = f"实车回放-{data['vehicle_id']}-{int(window_start)}"[:128]
        description = str(data.get("description") or "") or (
            f"4.5 节实车场景自动提取（{trigger}，窗口起点 {window_start}）"
        )
        return SceneCreateRequest(
            scene_name=name,
            scene_type=SceneType.REAL_VEHICLE_REPLAY,
            description=description[:1024],
            tags=[SceneType.REAL_VEHICLE_REPLAY.value, trigger],
            config=config,
        )

    def _build_config(self, data: dict[str, Any], trigger: str) -> SceneConfig:
        """消息 → 4.2.2 结构 config（自车轨迹 / 目标轨迹 / 环境条件三项提取内容）。"""
        raw_environment = data.get("environment")
        environment: dict[str, Any] = raw_environment if isinstance(raw_environment, dict) else {}
        ego_trajectory = [p for p in (data.get("ego_trajectory") or []) if isinstance(p, dict)]
        objects = [item for item in (data.get("objects") or []) if isinstance(item, dict)]
        spawn = (
            _point_from(ego_trajectory[0]) if ego_trajectory else SpawnPoint(x=0.0, y=0.0, yaw=0.0)
        )
        initial_speed = float(ego_trajectory[0].get("velocity") or 0.0) if ego_trajectory else 0.0
        event_type, action_type = TRIGGER_RULES[trigger]
        road_type = str(environment.get("road_type") or "real-vehicle-site")
        return SceneConfig(
            map=SceneMap(
                map_id=road_type[:128], map_type=MapType.SITE_MAP, spawn_point=spawn
            ),
            ego_vehicle=EgoVehicle(
                model=self._settings.scene_extraction_default_ego_model,
                initial_speed=max(0.0, initial_speed),
                initial_steer=0.0,
            ),
            weather=_weather_for(environment),
            actors=_actors_from(objects),
            events=[
                SceneEvent(
                    event_id=f"ev-{trigger}"[:64],
                    type=event_type,
                    trigger=SceneEventTrigger(
                        condition="real_vehicle_event",
                        parameters={
                            "event_type": trigger,
                            "event_level": data.get("event_level"),
                            "result_id": data.get("result_id"),
                            "window_start": data.get("window_start"),
                            "window_end": data.get("window_end"),
                        },
                    ),
                    action=SceneEventAction(
                        action_type=action_type,
                        parameters={"extracted_from": "analytics_result"},
                    ),
                )
            ],
            success_criteria=SuccessCriteria(
                max_speed_deviation=self._settings.scene_extraction_max_speed_deviation,
                no_collision=True,
                min_safe_distance=self._settings.scene_extraction_min_safe_distance,
            ),
            duration=max(0.001, float(data["window_end"]) - float(data["window_start"])),
        )


__all__ = [
    "CLIP_POST_SECONDS",
    "CLIP_PRE_SECONDS",
    "TRIGGER_RULES",
    "AnalyticsResultConsumer",
]