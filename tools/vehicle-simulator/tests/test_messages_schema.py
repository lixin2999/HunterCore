"""契约一致性：模拟器生产消息必须通过真实 schema 校验（additionalProperties=false）。"""

from __future__ import annotations

import json

import jsonschema
import pytest

from vehicle_simulator.config import SimConfig
from vehicle_simulator.kinematics import KinematicModel
from vehicle_simulator.messages import build_command_result, build_health, build_telemetry

VID = "HUNTER-SIM-001"


@pytest.fixture
def model():
    m = KinematicModel()
    m.state.battery_soc = 77
    m.apply_control({"target_velocity": 1.2, "target_steer": 0.05}, now=0.0)
    m.step(0.1, now=0.1)
    return m


def _validate(schema_loader, name, payload):
    jsonschema.validate(instance=payload, schema=schema_loader(name))


def test_telemetry_matches_schema(schema_loader, model):
    msg = build_telemetry(
        VID, model.state, seq=1, target_velocity=1.2, target_steer=0.05, now=1_700_000_000.5
    )
    _validate(schema_loader, "telemetry", msg)
    assert msg["chassis"]["battery_soc"] == 77
    assert len(msg["localization"]["linear_velocity"]) == 3


def test_health_matches_schema(schema_loader, model):
    msg = build_health(VID, model.state, now=1_700_000_000.5)
    _validate(schema_loader, "health", msg)
    assert msg["status"] in {
        "offline", "online_idle", "auto_driving", "remote_controlled",
        "upgrading", "charging", "fault", "emergency",
    }


def test_command_result_matches_schema(schema_loader):
    msg = build_command_result(
        VID, success=True, result_code="0", message="applied", data={"velocity": 1.2}, now=1_700_000_000.5
    )
    _validate(schema_loader, "command_result", msg)
    # command_id 必须是合法 uuid 形态
    import uuid

    uuid.UUID(msg["command_id"])


def test_command_result_long_message_truncated():
    msg = build_command_result(VID, message="x" * 1000)
    assert len(msg["message"]) <= 512


def test_config_derives_topics_and_stream():
    cfg = SimConfig(
        vehicle_id=VID,
        srs_rtmp="rtmp://10.0.0.5:1935/remote_control",
        srs_whip="http://10.0.0.5:9090/rtc/v1/whip/",
    )
    assert cfg.remote_control_topic() == f"hunter.{VID}.remote_control"
    assert cfg.telemetry_topic() == f"hunter.{VID}.telemetry"
    sid = "9c8b7a65-4d3e-2f10-9a8b-7c6d5e4f3a2b"
    assert cfg.stream_name(sid) == f"hunter_{VID}_{sid}"
    assert cfg.rtmp_url_for(sid) == f"rtmp://10.0.0.5:1935/remote_control/hunter_{VID}_{sid}"
    assert "app=remote_control" in cfg.whip_url_for(sid)  # type: ignore[arg-type]
    assert cfg.effective_scram_user == VID


def test_build_telemetry_json_serializable(schema_loader, model):
    msg = build_telemetry(VID, model.state, seq=2, target_velocity=0.0, target_steer=0.0)
    json.dumps(msg)  # 不得含不可序列化对象
