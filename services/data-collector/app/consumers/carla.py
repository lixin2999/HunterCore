"""CARLA 仿真回传消费者（消费组 ``data-collector-carla``）。

契约：``contracts/kafka/consumer-groups.yaml`` / ``carla_sensor_data.schema.json``：
- 订阅 ``carla.*.sensor_data``（动态 Topic，key=sim_id）；
- ``sim_status`` / ``sim_metrics`` / ``sim_event`` / ``artifact`` → Redis 缓存；
- ``sim_telemetry`` → ``data_collector.vehicle_telemetry``（``vehicle_id=SIM-{sim_id}``）；
- 可映射的 ``sim_event`` → ``data_collector.events`` + ``event_raw`` 镜像；
- ``sensor_file`` → 复用 ``sensor_file`` Topic 通知 data-analytics。

不写 ``data_analytics.*``（跨 schema 写禁止）；报告由 data-analytics-carla 生成。
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, ClassVar

from confluent_kafka import Message
from hunter_common.database.enums import EVENT_LEVEL_BY_TYPE, EventType
from hunter_common.exceptions import InvalidParameterError, ServiceUnavailableError
from hunter_common.logging import get_logger, reset_vehicle_id, set_vehicle_id
from hunter_common.redis_keys import (
    sim_artifacts_key,
    sim_events_key,
    sim_metrics_key,
    sim_status_key,
)
from redis.exceptions import RedisError

from app.config import Settings
from app.consumers.base import BaseIngestConsumer
from app.producers.pipeline import PipelineProducer
from app.producers.sensor_file import SensorFileProducer
from app.repositories.events import EventRepository
from app.repositories.telemetry import TelemetryRepository
from app.services.ingest import flatten_to_row, prepare_event

logger = get_logger("app.consumers.carla")

#: 仿真事件 → 车端 19 类事件（仅语义一致的镜像；其余只留 Redis）
SIM_EVENT_TYPE_MAP: dict[str, EventType] = {
    "collision": EventType.COLLISION_WARNING,
    "collision_warning": EventType.COLLISION_WARNING,
    "near_miss": EventType.COLLISION_PRE_WARNING,
    "collision_pre_warning": EventType.COLLISION_PRE_WARNING,
    "harsh_acceleration": EventType.HARSH_ACCELERATION,
    "harsh_braking": EventType.HARSH_BRAKING,
    "harsh_turning": EventType.HARSH_TURNING,
    "over_speed": EventType.OVER_SPEED,
    "manual_takeover": EventType.MANUAL_TAKEOVER,
    "emergency_stop": EventType.EMERGENCY_STOP,
    "ego_emergency_stop": EventType.EMERGENCY_STOP,
    "sensor_fault": EventType.SENSOR_FAULT,
    "perception_fault": EventType.PERCEPTION_FAULT,
    "planning_fault": EventType.PLANNING_FAULT,
    "control_fault": EventType.CONTROL_FAULT,
}

_SENSOR_DATA_TYPE_MAP = {
    "camera": "camera_image",
    "camera_image": "camera_image",
    "lidar": "point_cloud",
    "point_cloud": "point_cloud",
    "rosbag": "rosbag",
    "video": "video",
}


def sim_vehicle_id(sim_id: str) -> str:
    """仿真轨迹在 vehicle_telemetry 中的隔离前缀（避免与实车 ID 冲突）。"""
    return f"SIM-{sim_id}"


def build_sim_telemetry_row(envelope: dict[str, Any]) -> dict[str, Any]:
    """``sim_telemetry`` → ``vehicle_telemetry`` 扁平行（缺失段保留 NULL）。"""
    sim_id = str(envelope["sim_id"])
    payload = envelope["payload"]
    pose = payload.get("pose") or {}
    twist = payload.get("twist") or {}
    linear = twist.get("linear") if isinstance(twist, dict) else None
    angular = twist.get("angular") if isinstance(twist, dict) else None
    clean: dict[str, Any] = {
        "vehicle_id": sim_vehicle_id(sim_id),
        "timestamp": float(envelope["timestamp"]),
        "seq": int(envelope["seq"]),
        "chassis": {
            "velocity": payload.get("velocity"),
            "steering_angle": payload.get("steering_angle"),
        },
        "localization": {
            "x": pose.get("x"),
            "y": pose.get("y"),
            "z": pose.get("z"),
            "heading": pose.get("yaw"),
            "linear_velocity": linear,
            "angular_velocity": angular,
        },
    }
    row = flatten_to_row(clean)
    row["vehicle_id"] = sim_vehicle_id(sim_id)
    row["time"] = datetime.fromtimestamp(float(envelope["timestamp"]), tz=UTC)
    row["seq"] = int(envelope["seq"])
    return row


class CarlaSensorConsumer(BaseIngestConsumer):
    """carla.*.sensor_data 消费者（按 type 分发）。"""

    group_id: ClassVar[str] = "data-collector-carla"
    subscribe_pattern: ClassVar[str] = "carla.*.sensor_data"

    def __init__(
        self,
        settings: Settings,
        *,
        redis_client: Any,
        telemetry_repository: TelemetryRepository,
        event_repository: EventRepository,
        pipeline_producer: PipelineProducer,
        sensor_file_producer: SensorFileProducer | None = None,
    ) -> None:
        self._redis = redis_client
        self._telemetry = telemetry_repository
        self._events = event_repository
        self._pipeline = pipeline_producer
        self._sensor_files = sensor_file_producer or SensorFileProducer()
        self._ttl = max(1, settings.carla_sim_cache_ttl_seconds)
        self._events_max = max(1, settings.carla_sim_events_max)
        super().__init__(settings)

    @staticmethod
    def idempotency_key(_message: Message, value: Any) -> str:
        """契约：``(sim_id, seq)``。"""
        if not isinstance(value, dict):
            raise TypeError("carla_sensor_data 消息必须为 JSON 对象")
        return f"{value.get('sim_id')}:{value.get('seq')}"

    async def process(self, payload: dict[str, Any], *, topic: str) -> None:
        sim_id = payload.get("sim_id")
        msg_type = payload.get("type")
        if not isinstance(sim_id, str) or not sim_id:
            raise InvalidParameterError("carla_sensor_data 缺少 sim_id")
        if not isinstance(msg_type, str):
            raise InvalidParameterError("carla_sensor_data 缺少 type")
        body = payload.get("payload")
        if not isinstance(body, dict):
            raise InvalidParameterError("carla_sensor_data.payload 必须为对象")
        if msg_type == "sim_telemetry":
            await self._store_telemetry(payload)
        elif msg_type == "sim_status":
            await self._cache_status(payload)
        elif msg_type == "sim_metrics":
            await self._cache_metrics(payload)
        elif msg_type == "sim_event":
            await self._store_event(payload)
        elif msg_type == "sensor_file":
            await self._publish_sensor_file(payload)
        elif msg_type == "artifact":
            await self._cache_artifact(payload)
        else:  # schema 已限制枚举；防御式兜底（重试不会自愈 → DLQ）
            raise InvalidParameterError(f"未知仿真回传类型: {msg_type}", details={"topic": topic})
        logger.debug(
            "carla_sensor_data_processed",
            sim_id=sim_id,
            type=msg_type,
            seq=payload.get("seq"),
            topic=topic,
        )

    # ---------- 落库 ----------

    async def _store_telemetry(self, envelope: dict[str, Any]) -> None:
        row = build_sim_telemetry_row(envelope)
        async with self._telemetry.transaction() as session:
            await self._telemetry.insert_points(session, [row])

    async def _store_event(self, envelope: dict[str, Any]) -> None:
        sim_id = str(envelope["sim_id"])
        body = envelope["payload"]
        record = {
            "event_id": body.get("event_id"),
            "event_type": body.get("event_type"),
            "event_level": body.get("event_level"),
            "sim_time": body.get("sim_time"),
            "description": body.get("description"),
            "data": body.get("data") or {},
            "sim_id": sim_id,
            "vehicle_id": envelope.get("vehicle_id"),
            "scene_id": envelope.get("scene_id"),
            "timestamp": envelope.get("timestamp"),
            "seq": envelope.get("seq"),
        }
        await self._redis_list_push(
            sim_events_key(sim_id), json.dumps(record, ensure_ascii=False)
        )

        mapped = SIM_EVENT_TYPE_MAP.get(str(body.get("event_type")))
        if mapped is None:
            return  # 无对应车端事件：仅 Redis，禁止硬塞进 19 类枚举
        vehicle_id = sim_vehicle_id(sim_id)
        event_payload = {
            "vehicle_id": vehicle_id,
            "timestamp": float(envelope["timestamp"]),
            "event_type": mapped.value,
            "event_level": EVENT_LEVEL_BY_TYPE[mapped].value,
            "description": body.get("description") or f"仿真事件 {body.get('event_type')}",
            "data": {
                "sim_id": sim_id,
                "sim_event_id": body.get("event_id"),
                "sim_event_type": body.get("event_type"),
                "sim_time": body.get("sim_time"),
                **(body.get("data") if isinstance(body.get("data"), dict) else {}),
            },
        }
        prepared = prepare_event(event_payload)
        async with self._events.transaction() as session:
            await self._events.insert_events(session, [prepared.row])
        await self._pipeline.publish_event(event_payload, vehicle_id)

    # ---------- Redis 缓存 ----------

    async def _cache_status(self, envelope: dict[str, Any]) -> None:
        sim_id = str(envelope["sim_id"])
        body = dict(envelope["payload"])
        body.update(
            {
                "sim_id": sim_id,
                "vehicle_id": envelope.get("vehicle_id"),
                "scene_id": envelope.get("scene_id"),
                "timestamp": envelope.get("timestamp"),
                "seq": envelope.get("seq"),
                "updated_at": body.get("updated_at") or envelope.get("timestamp"),
            }
        )
        await self._redis_set(sim_status_key(sim_id), body)

    async def _cache_metrics(self, envelope: dict[str, Any]) -> None:
        sim_id = str(envelope["sim_id"])
        body = dict(envelope["payload"])
        body.update(
            {
                "sim_id": sim_id,
                "vehicle_id": envelope.get("vehicle_id"),
                "scene_id": envelope.get("scene_id"),
                "timestamp": envelope.get("timestamp"),
                "seq": envelope.get("seq"),
            }
        )
        await self._redis_set(sim_metrics_key(sim_id), body)

    async def _cache_artifact(self, envelope: dict[str, Any]) -> None:
        sim_id = str(envelope["sim_id"])
        body = dict(envelope["payload"])
        body.update(
            {
                "sim_id": sim_id,
                "timestamp": envelope.get("timestamp"),
                "seq": envelope.get("seq"),
            }
        )
        key = sim_artifacts_key(sim_id)
        object_key = str(body.get("object_key") or body.get("name") or envelope.get("seq"))
        try:
            await self._redis.hset(key, object_key, json.dumps(body, ensure_ascii=False))
            await self._redis.expire(key, self._ttl)
        except RedisError as exc:
            raise ServiceUnavailableError(
                "仿真产物缓存写入失败", details={"sim_id": sim_id}
            ) from exc

    async def _publish_sensor_file(self, envelope: dict[str, Any]) -> None:
        sim_id = str(envelope["sim_id"])
        body = envelope["payload"]
        sensor_type = str(body.get("sensor_type") or "other")
        data_type = body.get("data_type") or _SENSOR_DATA_TYPE_MAP.get(sensor_type, "other")
        await self._sensor_files.publish(
            sim_vehicle_id(sim_id),
            float(envelope["timestamp"]),
            str(body["bucket"]),
            str(body["object_key"]),
            str(data_type),
            int(body["size_bytes"]),
            str(body["md5"]),
            str(body["sha256"]),
        )

    async def _redis_set(self, key: str, payload: dict[str, Any]) -> None:
        try:
            await self._redis.set(
                key, json.dumps(payload, ensure_ascii=False), ex=self._ttl
            )
        except RedisError as exc:
            raise ServiceUnavailableError(
                "仿真缓存写入失败", details={"key": key}
            ) from exc

    async def _redis_list_push(self, key: str, value: str) -> None:
        try:
            await self._redis.lpush(key, value)
            await self._redis.ltrim(key, 0, self._events_max - 1)
            await self._redis.expire(key, self._ttl)
        except RedisError as exc:
            raise ServiceUnavailableError(
                "仿真事件缓存写入失败", details={"key": key}
            ) from exc

    async def _handle(self, message: Message, value: Any) -> None:
        """继承基类日志上下文（sim 消息 vehicle_id 可能为 null）。"""
        if not isinstance(value, dict):
            raise TypeError(f"消息必须为 JSON 对象，实际 {type(value).__name__}")
        vehicle_id = value.get("vehicle_id")
        token = set_vehicle_id(str(vehicle_id) if vehicle_id else "")
        try:
            await self.process(value, topic=str(message.topic()))
        finally:
            reset_vehicle_id(token)


__all__ = [
    "SIM_EVENT_TYPE_MAP",
    "CarlaSensorConsumer",
    "build_sim_telemetry_row",
    "sim_vehicle_id",
]
