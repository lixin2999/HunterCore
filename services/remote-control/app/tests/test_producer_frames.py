"""G-10 生产者帧契约一致性测试（boot/stop/control 三类帧全部通过 remote_control.schema.json）。

收对登记：boot/stop 帧此前缺失 required 键 vehicle_id、控制量键名用自造词
（throttle/steer）——本用例把三类帧逐一经 jsonschema 校验，防止再次漂移；
同时覆盖 send_control_frame 的可选控制量（brake 截断 / gear 验词 / mode 透传）。
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import jsonschema

from app.config import settings
from app.producers.remote_control import RemoteControlFrameProducer
from app.schemas.common import SessionEndReason
from app.schemas.sessions import ControlChannelInfo, SessionHeartbeat, VideoConfig

ROOT = Path(__file__).resolve().parents[4]
SCHEMA = json.loads(
    (ROOT / "contracts" / "kafka" / "schemas" / "remote_control.schema.json").read_text(
        encoding="utf-8"
    )
)

VEHICLE_ID = "HUNTER-001"
SESSION_ID = "9c8b7a65-4d3e-2f10-9a8b-7c6d5e4f3a2b"
OPERATOR_ID = "0b6c1b7e-8f2a-4d3e-9a11-2b3c4d5e6f70"


class CapturingProducer(RemoteControlFrameProducer):
    """记录投递 (topic, key, message) 的真实生产者（不 Mock 帧构建逻辑本身）。"""

    def __init__(self) -> None:
        self.sent: list[tuple[str, str | None, dict[str, Any]]] = []

        async def produce(topic: str, key: str | None, message: dict[str, Any]) -> None:
            self.sent.append((topic, key, message))

        super().__init__(produce, settings)

    @property
    def last_message(self) -> dict[str, Any]:
        return self.sent[-1][2]

    def assert_last_valid(self) -> dict[str, Any]:
        """最后一帧：JSON 可序列化 + Schema 校验通过（往返后校验，贴近真实投递形态）。"""
        topic, key, message = self.sent[-1]
        assert topic == f"hunter.{VEHICLE_ID}.remote_control" and key == VEHICLE_ID
        payload = json.loads(json.dumps(message))
        jsonschema.validate(payload, SCHEMA)
        return payload


def _video() -> VideoConfig:
    return VideoConfig(fps=30, min_bitrate_kbps=2048, max_bitrate_kbps=4096)


def _control_info() -> ControlChannelInfo:
    return ControlChannelInfo(ack_timeout_ms=100, max_speed_mps=2.0)


def _heartbeat() -> SessionHeartbeat:
    return SessionHeartbeat(degraded_after_misses=3, end_after_s=60)


def test_boot_frame_conforms_to_schema() -> None:
    p = CapturingProducer()
    asyncio.run(
        p.send_boot(
            VEHICLE_ID,
            session_id=SESSION_ID,
            operator_id=OPERATOR_ID,
            video=_video(),
            control=_control_info(),
            heartbeat=_heartbeat(),
            webrtc_ready=True,
        )
    )
    frame = p.assert_last_valid()
    # G-10 收对：补发 vehicle_id（required）；控制量契约键名（target_velocity/target_steer）
    assert frame["vehicle_id"] == VEHICLE_ID
    assert frame["type"] == "remote_control"
    assert frame["control"] == {"target_velocity": 0.0, "target_steer": 0.0, "brake": 0.0, "gear": None}
    assert frame["heartbeat"] is True
    assert frame["video"]["recording"] is True and frame["video"]["webrtc_ready"] is True


def test_stop_frame_conforms_to_schema() -> None:
    p = CapturingProducer()
    asyncio.run(
        p.send_stop(
            VEHICLE_ID,
            session_id=SESSION_ID,
            operator_id=OPERATOR_ID,
            reason=SessionEndReason.HEARTBEAT_TIMEOUT,
        )
    )
    frame = p.assert_last_valid()
    assert frame["vehicle_id"] == VEHICLE_ID
    assert frame["heartbeat"] is False
    assert frame["reason"] == "heartbeat_timeout"  # SessionEndReason 受控词表
    assert frame["video"] == {"recording": False, "webrtc_ready": False}


def test_control_frame_optional_fields() -> None:
    p = CapturingProducer()
    # 基线帧：无 brake/gear/mode → control 仅契约必填两键（None 不写键）
    asyncio.run(
        p.send_control_frame(
            VEHICLE_ID,
            session_id=SESSION_ID,
            operator_id=OPERATOR_ID,
            seq=101,
            target_velocity=1.5,
            target_steer=0.2,
        )
    )
    frame = p.assert_last_valid()
    assert set(frame["control"]) == {"target_velocity", "target_steer"}
    assert "type" not in frame  # 20Hz 控制帧不带 boot/stop 扩展键

    # gear/brake 正常透传（含契约示例形态：换挡 + 比例制动）
    asyncio.run(
        p.send_control_frame(
            VEHICLE_ID,
            session_id=SESSION_ID,
            operator_id=OPERATOR_ID,
            seq=102,
            target_velocity=1.5,
            target_steer=0.2,
            gear="D",
            brake=0.3,
        )
    )
    frame = p.assert_last_valid()
    assert frame["control"]["gear"] == "D" and frame["control"]["brake"] == 0.3

    # 防御：brake 越界截断 0~1；非法档位词丢键；mode 透传（内部/测试路径）
    asyncio.run(
        p.send_control_frame(
            VEHICLE_ID,
            session_id=SESSION_ID,
            operator_id=OPERATOR_ID,
            seq=103,
            target_velocity=9.9,  # Schema 硬界截断到 2.0
            target_steer=0.0,
            gear="L",
            brake=2.0,
            mode="remote_default",
        )
    )
    frame = p.assert_last_valid()
    control = frame["control"]
    assert control["target_velocity"] == 2.0
    assert control["brake"] == 1.0
    assert "gear" not in control  # 非法词不写入
    assert control["mode"] == "remote_default"


def test_estop_zero_frame_conforms() -> None:
    """estop 形态（路由层参数组合）：零速 + 满制动 + 空挡 → Schema 合法。"""
    p = CapturingProducer()
    asyncio.run(
        p.send_control_frame(
            VEHICLE_ID,
            session_id=SESSION_ID,
            operator_id=OPERATOR_ID,
            seq=200,
            target_velocity=0.0,
            target_steer=0.0,
            brake=1.0,
            gear="N",
        )
    )
    frame = p.assert_last_valid()
    assert frame["control"] == {
        "target_velocity": 0.0,
        "target_steer": 0.0,
        "brake": 1.0,
        "gear": "N",
    }
