"""仿真会话仓储与动态 Topic 供给（CARLA 接入方案 P1）。

- ``VilSessionStore``：``vil:session:{vehicle_id}`` + ``vil:active`` 的唯一写方；
  vil-forwarder 只读。同一车辆同时仅允许一个活动 VIL 会话（``SET NX`` 原子占位）。
- ``SimTopicProvisioner``：包装 ``hunter_common.kafka.admin.KafkaAdmin``，在创建
  CARLA 实例前显式创建 ``carla.{sim_id}.vil_state`` / ``sensor_data`` 及 DLQ；
  删除操作放到线程池，避免阻塞事件循环。
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Protocol

from hunter_common.exceptions import ServiceUnavailableError
from hunter_common.kafka.admin import KafkaAdmin
from hunter_common.logging import get_logger
from hunter_common.redis_keys import (
    VIL_ACTIVE,
    sim_instance_key,
    vil_session_key,
)
from redis.exceptions import RedisError

logger = get_logger("app.repositories.simulations")


class VilSessionStore:
    """VIL 会话 Redis 读写（依据 ``contracts/database/redis-keys.yaml``）。"""

    def __init__(self, redis_client: Any, *, ttl_seconds: int = 7200) -> None:
        self._redis = redis_client
        self._ttl = max(1, ttl_seconds)

    @staticmethod
    def _dumps(payload: dict[str, Any]) -> str:
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    async def get(self, vehicle_id: str) -> dict[str, Any] | None:
        """读取会话；键不存在/JSON 损坏返回 None（损坏告警，按无会话处理）。"""
        try:
            raw = await self._redis.get(vil_session_key(vehicle_id))
        except RedisError as exc:
            raise ServiceUnavailableError(
                "VIL 会话存储不可用", details={"vehicle_id": vehicle_id}
            ) from exc
        if raw is None:
            return None
        try:
            payload = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            logger.warning("vil_session_corrupted", vehicle_id=vehicle_id)
            return None
        return payload if isinstance(payload, dict) else None

    async def acquire(self, payload: dict[str, Any]) -> bool:
        """原子占位（SET NX EX）；False = 该车辆已有活动 VIL 会话。"""
        vehicle_id = str(payload.get("vehicle_id") or "")
        if not vehicle_id:
            raise ValueError("VIL 会话缺少 vehicle_id")
        key = vil_session_key(vehicle_id)
        try:
            created = await self._redis.set(
                key,
                self._dumps(payload),
                nx=True,
                ex=self._ttl,
            )
        except RedisError as exc:
            raise ServiceUnavailableError(
                "VIL 会话存储不可用", details={"vehicle_id": vehicle_id}
            ) from exc
        if not created:
            return False
        try:
            await self._redis.sadd(VIL_ACTIVE, vehicle_id)
        except RedisError as exc:
            # SADD 失败必须回滚已占位的会话键，否则该车辆被无主会话阻塞到 TTL 到期
            # （默认 2 小时），且 vil:active 对账也看不到它（V1.23.4）。
            try:
                await self._redis.delete(key)
            except RedisError:
                logger.exception("vil_session_acquire_rollback_failed", vehicle_id=vehicle_id)
            raise ServiceUnavailableError(
                "VIL 活动集合写入失败", details={"vehicle_id": vehicle_id}
            ) from exc
        return True

    async def bind_instance(
        self,
        *,
        vehicle_id: str,
        sim_instance_id: str,
        sim_id: str,
        scene_id: str,
        mode: str,
        vil_state_topic: str,
        sensor_data_topic: str,
        instance_ttl_seconds: int = 86400,
    ) -> None:
        """实例创建成功后补充 CARLA 实例 ID，并登记 sim:instance 反查键。"""
        session = await self.get(vehicle_id) if vehicle_id else None
        if session is not None:
            session.update(
                {
                    "sim_instance_id": sim_instance_id,
                    "sim_id": sim_id,
                    "scene_id": scene_id,
                    "mode": mode,
                    "vil_state_topic": vil_state_topic,
                    "sensor_data_topic": sensor_data_topic,
                }
            )
            try:
                await self._redis.set(
                    vil_session_key(vehicle_id), self._dumps(session), ex=self._ttl
                )
            except RedisError as exc:
                raise ServiceUnavailableError(
                    "VIL 会话存储不可用", details={"vehicle_id": vehicle_id}
                ) from exc
        meta = {
            "sim_instance_id": sim_instance_id,
            "sim_id": sim_id,
            "scene_id": scene_id,
            "vehicle_id": vehicle_id,
            "mode": mode,
            "vil_state_topic": vil_state_topic,
            "sensor_data_topic": sensor_data_topic,
            "updated_at": time.time(),
        }
        try:
            await self._redis.set(
                sim_instance_key(sim_instance_id),
                self._dumps(meta),
                ex=max(1, instance_ttl_seconds),
            )
        except RedisError as exc:
            raise ServiceUnavailableError(
                "仿真实例登记失败", details={"sim_instance_id": sim_instance_id}
            ) from exc

    async def lookup_instance(self, sim_instance_id: str) -> dict[str, Any] | None:
        """按 CARLA 实例 ID 反查会话元数据（不存在返回 None）。"""
        try:
            raw = await self._redis.get(sim_instance_key(sim_instance_id))
        except RedisError as exc:
            raise ServiceUnavailableError(
                "仿真实例登记不可用", details={"sim_instance_id": sim_instance_id}
            ) from exc
        if raw is None:
            return None
        try:
            payload = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            logger.warning("sim_instance_corrupted", sim_instance_id=sim_instance_id)
            return None
        return payload if isinstance(payload, dict) else None

    async def release(
        self, vehicle_id: str, *, sim_id: str | None = None, sim_instance_id: str | None = None
    ) -> None:
        """释放 VIL 会话（可选仅当 sim_id 匹配时释放，避免误删新会话）。

        三个键各自 best effort 清理：任一失败不影响其余键，最终统一抛 5001 供调用方告警。
        """
        if sim_id is not None:
            current = await self.get(vehicle_id)
            if current is not None and str(current.get("sim_id")) != sim_id:
                return
        errors: list[str] = []
        try:
            await self._redis.delete(vil_session_key(vehicle_id))
        except RedisError as exc:
            errors.append(f"session: {exc}")
        try:
            await self._redis.srem(VIL_ACTIVE, vehicle_id)
        except RedisError as exc:
            errors.append(f"active: {exc}")
        if sim_instance_id:
            try:
                await self._redis.delete(sim_instance_key(sim_instance_id))
            except RedisError as exc:
                errors.append(f"instance: {exc}")
        if errors:
            logger.warning(
                "vil_session_release_failed",
                vehicle_id=vehicle_id,
                errors=errors,
            )
            raise ServiceUnavailableError(
                "VIL 会话释放失败", details={"vehicle_id": vehicle_id}
            )

    async def list_active(self) -> list[str]:
        """活动 VIL 车辆列表（启动对账/健康检查用）。"""
        try:
            members = await self._redis.smembers(VIL_ACTIVE)
        except RedisError as exc:
            raise ServiceUnavailableError("VIL 活动集合不可用") from exc
        return [item.decode() if isinstance(item, bytes) else str(item) for item in members]


class TopicProvisioner(Protocol):
    """动态 Topic 供给协议（生产实现见 :class:`SimTopicProvisioner`）。"""

    async def ensure_session(self, sim_id: str) -> list[str]: ...

    async def delete_session(self, sim_id: str) -> list[str]: ...


class SimTopicProvisioner:
    """按契约创建/删除 ``carla.{sim_id}.*`` Topic（Kafka Admin 同步调用放线程池）。"""

    def __init__(self, admin: KafkaAdmin) -> None:
        self._admin = admin

    async def ensure_session(self, sim_id: str) -> list[str]:
        return await asyncio.to_thread(self._admin.ensure_carla_session, sim_id)

    async def delete_session(self, sim_id: str) -> list[str]:
        return await asyncio.to_thread(self._admin.delete_carla_session, sim_id)


def carla_topic_names(sim_id: str) -> tuple[str, str]:
    """平台约定的两个动态 Topic 名（与 topics.yaml#carla_topics 模板一致）。"""
    return f"carla.{sim_id}.vil_state", f"carla.{sim_id}.sensor_data"


__all__ = [
    "SimTopicProvisioner",
    "TopicProvisioner",
    "VilSessionStore",
    "carla_topic_names",
]
