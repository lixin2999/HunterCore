"""实车场景自动提取消费者测试（设计文档 4.5 节 / 契约 x-hunter-real-vehicle-extraction）。

覆盖：触发事件映射、10/10 截取窗口、三类提取内容（自车轨迹 / 目标轨迹 / 环境）、
输出 real_vehicle_replay + draft、幂等键 (vehicle_id, window_start)、非法消息抛 ValueError（→ DLQ）。
"""
from __future__ import annotations

import copy
from uuid import UUID

import pytest

from app.config import settings
from app.consumers.analytics_result import (
    CLIP_POST_SECONDS,
    CLIP_PRE_SECONDS,
    TRIGGER_RULES,
    AnalyticsResultConsumer,
)
from app.schemas.scene import SceneEventType, SceneType
from app.services.scenes import SceneService
from app.tests.fakes import FakeSceneCache, InMemorySceneRepository

#: 契约 analytics_result.schema.json 示例消息（corner_case / harsh_braking）
MESSAGE: dict[str, object] = {
    "vehicle_id": "HUNTER-001",
    "result_id": "b7f1c3d2-5a4e-4c9b-8f2d-1e0a3b5c7d90",
    "result_type": "corner_case",
    "trigger_event_type": "harsh_braking",
    "event_level": "warning",
    "window_start": 1724035190.123,
    "window_end": 1724035220.456,
    "timestamp": 1724035221.789,
    "clip": {"pre_seconds": 10, "post_seconds": 10},
    "ego_trajectory": [
        {"timestamp": 1724035190.123, "x": 125.34, "y": 67.89, "z": 0.32, "heading": 1.234, "velocity": 1.52},
        {"timestamp": 1724035200.123, "x": 126.12, "y": 68.41, "z": 0.32, "heading": 1.236, "velocity": 0.31},
    ],
    "objects": [
        {
            "object_id": "obj-1024",
            "type": "vehicle",
            "trajectory": [
                {"timestamp": 1724035190.123, "x": 130.2, "y": 67.9, "heading": 1.23, "velocity": 0.0}
            ],
        }
    ],
    "environment": {"weather": "rainy", "road_type": "urban", "speed_limit": 5.0},
    "description": "雨天城市道路前车急刹",
}


def _consumer() -> tuple[AnalyticsResultConsumer, InMemorySceneRepository]:
    """构造消费者与内存仓储（复用 SceneService 业务规则）。"""
    repository = InMemorySceneRepository()
    service = SceneService(repository, FakeSceneCache(), settings)
    return AnalyticsResultConsumer(settings, service), repository


def test_trigger_rules_match_design_doc_4_5() -> None:
    """触发事件与场景事件类型映射（4.5 节 3 类触发）。"""
    assert set(TRIGGER_RULES) == {"harsh_braking", "collision_warning", "manual_takeover"}
    assert TRIGGER_RULES["harsh_braking"][0] is SceneEventType.EMERGENCY_BRAKE
    assert TRIGGER_RULES["collision_warning"][0] is SceneEventType.COLLISION
    assert TRIGGER_RULES["manual_takeover"][0] is SceneEventType.MANUAL_TAKEOVER
    assert (CLIP_PRE_SECONDS, CLIP_POST_SECONDS) == (10, 10)


async def test_handle_payload_creates_real_vehicle_replay_scene() -> None:
    """合法 corner_case 消息 → 生成 real_vehicle_replay 草稿场景（含 4.2.2 结构）。"""
    consumer, repository = _consumer()
    scene = await consumer.handle_payload(copy.deepcopy(MESSAGE))
    assert scene is not None
    assert scene.scene_type is SceneType.REAL_VEHICLE_REPLAY
    assert scene.status.value == "draft"
    assert scene.tags == ["real_vehicle_replay", "harsh_braking"]
    assert scene.version == "1.0.0"
    assert scene.creator == UUID(settings.scene_extraction_creator_id)
    assert scene.scene_name == "实车回放-HUNTER-001-1724035190"

    config = scene.config
    assert config.duration == pytest.approx(30.333, abs=1e-3)
    assert config.map.map_id == "urban"
    assert config.ego_vehicle.model == settings.scene_extraction_default_ego_model
    assert config.ego_vehicle.initial_speed == 1.52
    assert config.weather.rain == 80.0  # rainy 环境映射
    assert len(config.actors) == 1
    assert config.actors[0].actor_id == "obj-1024"
    assert config.actors[0].behavior.parameters["point_count"] == 1
    assert config.events[0].type is SceneEventType.EMERGENCY_BRAKE
    assert config.events[0].trigger.condition == "real_vehicle_event"
    assert config.events[0].trigger.parameters["result_id"] == MESSAGE["result_id"]
    assert len(repository.rows) == 1


async def test_handle_payload_is_idempotent() -> None:
    """同一 (vehicle_id, window_start) 重复消息 → 跳过，不重复建场景。"""
    consumer, repository = _consumer()
    first = await consumer.handle_payload(copy.deepcopy(MESSAGE))
    second = await consumer.handle_payload(copy.deepcopy(MESSAGE))
    assert first is not None
    assert second is None
    assert len(repository.rows) == 1


async def test_handle_payload_skips_non_trigger_results() -> None:
    """非 corner_case / 非触发事件的消息跳过（不建场景）。"""
    consumer, repository = _consumer()
    metric = copy.deepcopy(MESSAGE)
    metric["result_type"] = "metric"
    assert await consumer.handle_payload(metric) is None

    other_trigger = copy.deepcopy(MESSAGE)
    other_trigger["trigger_event_type"] = "over_speed"
    assert await consumer.handle_payload(other_trigger) is None
    assert repository.rows == {}


@pytest.mark.parametrize(
    "mutate",
    [
        lambda payload: payload.pop("vehicle_id"),
        lambda payload: payload.update({"result_type": "unknown"}),
        lambda payload: payload.update({"window_end": payload["window_start"]}),
        lambda payload: payload.update({"clip": {"pre_seconds": 5, "post_seconds": 10}}),
        lambda payload: payload.update({"clip": {"pre_seconds": 10, "post_seconds": 20}}),
    ],
)
async def test_handle_payload_rejects_invalid_message(mutate: object) -> None:
    """字段缺失/词表非法/窗口非法 → ValueError（由消费者转投 DLQ）。"""
    consumer, _ = _consumer()
    payload = copy.deepcopy(MESSAGE)
    mutate(payload)  # type: ignore[operator]
    with pytest.raises(ValueError):
        await consumer.handle_payload(payload)


async def test_handle_payload_rejects_non_object_message() -> None:
    """非 JSON 对象消息 → ValueError（DLQ），不得静默丢弃。"""
    consumer, _ = _consumer()
    with pytest.raises(ValueError):
        await consumer.handle_payload(["not", "an", "object"])  # type: ignore[arg-type]
