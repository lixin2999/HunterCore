"""模拟器编排单测（内存桩 Transport：boot/stop 会话切换、控制帧驱动、回执/遥测生产）。"""

from __future__ import annotations

from collections import deque
from typing import Any

import pytest

from vehicle_simulator.config import SimConfig
from vehicle_simulator.simulator import VehicleSimulator

VID = "HUNTER-SIM-001"
SID = "9c8b7a65-4d3e-2f10-9a8b-7c6d5e4f3a2b"


class StubTransport:
    """内存替身：记录 produce，按队列回放 poll。"""

    def __init__(self, frames: deque | None = None) -> None:
        self.produced: list[tuple[str, str | None, dict[str, Any]]] = []
        self.frames = frames or deque()
        self.subscribed = False
        self.commits = 0
        self.closed = False

    def produce(self, topic, key, value):
        self.produced.append((topic, key, value))

    def subscribe(self):
        self.subscribed = True

    def poll(self, timeout):
        return self.frames.popleft() if self.frames else None

    def commit(self):
        self.commits += 1

    def flush(self, timeout=2.0):
        pass

    def close(self):
        self.closed = True

    def by_topic(self, topic):
        return [v for t, _k, v in self.produced if t == topic]


@pytest.fixture
def cfg():
    return SimConfig(vehicle_id=VID, ack_control_frames=True)


def _boot_frame():
    return {
        "type": "remote_control",
        "vehicle_id": VID,
        "timestamp": 1.0,
        "seq": 0,
        "session_id": SID,
        "control": {"target_velocity": 0.0, "target_steer": 0.0, "gear": None},
        "heartbeat": True,
        "video": {"recording": True, "webrtc_ready": True},
    }


def _control_frame(seq=1, v=1.2, steer=0.05):
    return {
        "vehicle_id": VID,
        "timestamp": 1.05,
        "seq": seq,
        "session_id": SID,
        "control": {"target_velocity": v, "target_steer": steer},
        "heartbeat": False,
    }


def _stop_frame():
    return {
        "type": "remote_control",
        "vehicle_id": VID,
        "timestamp": 2.0,
        "seq": 9,
        "session_id": SID,
        "control": {"target_velocity": 0.0, "target_steer": 0.0, "brake": 1.0, "gear": "N"},
        "heartbeat": False,
        "video": {"recording": False, "webrtc_ready": False},
        "reason": "operator_end",
    }


def test_handle_frame_produces_command_result_ack(cfg):
    t = StubTransport()
    sim = VehicleSimulator(cfg, t)
    sim.handle_frame(_control_frame())
    results = t.by_topic(f"hunter.{VID}.command_result")
    assert len(results) == 1
    assert results[0]["vehicle_id"] == VID
    assert results[0]["success"] is True
    assert sim.acks_sent == 1


def test_boot_frame_activates_session(cfg):
    t = StubTransport()
    sim = VehicleSimulator(cfg, t)
    sim.handle_frame(_boot_frame())
    assert sim.session_id == SID
    assert sim.model.state.control_enabled is True


def test_stop_frame_deactivates_session(cfg):
    t = StubTransport()
    sim = VehicleSimulator(cfg, t)
    sim.handle_frame(_boot_frame())
    sim.handle_frame(_stop_frame())
    assert sim.model.state.control_enabled is False


def test_no_ack_when_disabled(cfg):
    cfg.ack_control_frames = False
    t = StubTransport()
    sim = VehicleSimulator(cfg, t)
    sim.handle_frame(_control_frame())
    assert t.by_topic(f"hunter.{VID}.command_result") == []


def test_emit_telemetry_increments_seq(cfg):
    t = StubTransport()
    sim = VehicleSimulator(cfg, t)
    sim._emit_telemetry()
    sim._emit_telemetry()
    telem = t.by_topic(f"hunter.{VID}.telemetry")
    assert [m["seq"] for m in telem] == [1, 2]


def test_run_loop_drains_frames_and_produces(cfg):
    """duration 到点后主循环退出：boot+control 帧被排空并各自产出遥测/健康/回执。"""
    cfg.duration_s = 0.2
    cfg.telemetry_hz = 50.0
    cfg.health_hz = 50.0
    t = StubTransport(frames=deque([_boot_frame(), _control_frame()]))
    sim = VehicleSimulator(cfg, t)
    sim.run()  # 不注入 video → 仅数据链路
    assert t.subscribed is True
    assert t.by_topic(f"hunter.{VID}.telemetry"), "应至少产出一条遥测"
    assert t.by_topic(f"hunter.{VID}.health"), "应至少产出一条健康"
    assert len(t.by_topic(f"hunter.{VID}.command_result")) == 2  # 两帧各回执一次
