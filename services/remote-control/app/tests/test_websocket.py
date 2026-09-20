"""G-09/G-10 WS control/signal 端点测试（契约 x-hunter-websocket-contract）。

- 纯函数面：子协议 Token 提取（双值/单值形态，决策 #20①）、Bearer 头提取、上行帧解析；
- 传输面：starlette TestClient（**不进 with 上下文——避免 lifespan 用真实服务覆盖
  rc_env 注入的替身 app.state**；Authorization 头携带 Token，hunter-jwt 子协议承载形态
  经纯函数用例覆盖——TestClient 子协议列表由框架协商，无法伪造双值承载）；
- 会话预置经 session_service.create_session 直调（与 REST 行为等价的夹具路径）；
- status 帧 1Hz 异步穿插 → 断言经 :func:`_recv_until` 按 type 过滤。
"""
from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections import deque
from typing import Any

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from app.config import settings
from app.consumers.command_result import CommandResultConsumer, CommandResultMessage
from app.core.ws_auth import extract_bearer_token, extract_subprotocol_token
from app.main import app
from app.schemas.sessions import CreateRemoteSessionRequest
from app.schemas.websocket import UplinkFrameError, parse_uplink_frame
from app.services.ws_hub import WsControlConnection
from app.tests.conftest import (
    ADMIN_ID,
    OPERATOR_ID,
    OTHER_ID,
    VEHICLE_ONLINE,
    make_operator,
    make_ws_token,
)


# =====================================================================
# 纯函数：Token 提取（决策 #20① 子协议承载）
# =====================================================================
def test_extract_subprotocol_token_dual_form() -> None:
    token, echo = extract_subprotocol_token(["hunter-jwt", "abc.def"], "hunter-jwt")
    assert token == "abc.def" and echo == "hunter-jwt"


def test_extract_subprotocol_token_single_form() -> None:
    token, echo = extract_subprotocol_token(["hunter-jwt.abc.def"], "hunter-jwt")
    assert token == "abc.def" and echo == "hunter-jwt.abc.def"


def test_extract_subprotocol_token_absent() -> None:
    assert extract_subprotocol_token(["chat"], "hunter-jwt") == (None, None)
    # 仅子协议名无后续 Token 值：提取失败（路由层 1001 拒绝），echo 仍可回显
    assert extract_subprotocol_token(["hunter-jwt"], "hunter-jwt") == (None, "hunter-jwt")
    assert extract_subprotocol_token(["hunter-jwt."], "hunter-jwt") == (None, None)


def test_extract_bearer_token() -> None:
    assert extract_bearer_token("Bearer tok123") == "tok123"
    assert extract_bearer_token("bearer tok123") == "tok123"
    assert extract_bearer_token("Basic tok123") is None
    assert extract_bearer_token(None) is None


# ---------- 上行帧解析 ----------
def test_parse_uplink_frame_control_gear_brake_passthrough() -> None:
    """G-10：gear/brake 契约化可选字段透传；未契约化键（mode 等）仍丢弃。"""
    frame = parse_uplink_frame(
        '{"type":"control","session_id":"s1","seq":9,"timestamp":1.0,'
        '"control":{"target_velocity":1.2,"target_steer":0.1,"gear":"D","brake":0.3,"mode":"x"}}'
    )
    assert frame.control.target_velocity == 1.2
    assert frame.control.gear == "D"  # G-10 后不再丢弃
    assert frame.control.brake == 0.3
    assert not hasattr(frame.control, "mode")  # 平台侧预留字段，不接受上行（extra=ignore）


def test_parse_uplink_frame_rejects_bad_gear_and_brake() -> None:
    """G-10 验词/限幅在 WS 层前置：非法档位词、brake 越界 → UplinkFrameError（路由转 2001）。"""
    with pytest.raises(UplinkFrameError):
        parse_uplink_frame(
            '{"type":"control","session_id":"s1","control":{"target_velocity":1.0,"gear":"L"}}'
        )
    with pytest.raises(UplinkFrameError):
        parse_uplink_frame(
            '{"type":"control","session_id":"s1","control":{"target_velocity":1.0,"brake":1.5}}'
        )


def test_parse_uplink_frame_rejects_bad_input() -> None:
    with pytest.raises(UplinkFrameError):
        parse_uplink_frame("not-json")
    with pytest.raises(UplinkFrameError):
        parse_uplink_frame('{"type":"unknown"}')
    with pytest.raises(UplinkFrameError):
        parse_uplink_frame('{"type":"control","session_id":"s"}')  # 缺 control 对象


# =====================================================================
# 集成面：TestClient WS（不经 lifespan，app.state 为 rc_env 替身）
# =====================================================================
def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _create_session(env: Any) -> str:
    info = asyncio.run(
        env.session_service.create_session(
            VEHICLE_ONLINE,
            make_operator(),
            CreateRemoteSessionRequest(vehicle_id=VEHICLE_ONLINE),
        )
    )
    return info.session_id


def _recv_until(ws: Any, frame_type: str, max_frames: int = 30) -> dict[str, Any]:
    """按 type 过滤接收（status 帧 1Hz 穿插，不保证次序）。"""
    for _ in range(max_frames):
        frame = ws.receive_json()
        if frame.get("type") == frame_type:
            return frame
    raise AssertionError(f"未收到 {frame_type} 帧")


def _control_frame(session_id: str, velocity: float = 1.0, steer: float = 0.1) -> str:
    return json.dumps(
        {
            "type": "control",
            "session_id": session_id,
            "seq": 1,
            "timestamp": time.time(),
            "control": {"target_velocity": velocity, "target_steer": steer, "gear": "D"},
        }
    )


def _client() -> TestClient:
    return TestClient(app)


def test_ws_control_connect_and_forward(rc_env: Any) -> None:
    env = rc_env
    session_id = _create_session(env)
    with _client().websocket_connect(
        f"/ws/remote/{session_id}/control", headers=_auth(make_ws_token())
    ) as ws:
        ws.send_text(_control_frame(session_id))
        status = _recv_until(ws, "status")
        # 测试环境无真实 command_result 回执：>500ms 后 ticker 按设计判 command_timeout
        # 降级（链路健康语义），首个 status 帧即为降级后推送
        assert status["status"] in ("connecting", "active", "degraded")
        assert status["control_stats"]["commands_sent"] >= 1
        assert status["control_stats"]["last_seq"] == 1
    frames = [f for f in env.frame.frames if f[0] == "control"]
    assert len(frames) == 1
    kwargs = frames[0][2]
    assert kwargs["seq"] == 1  # 服务端 seq（不复用前端 seq）
    assert kwargs["target_velocity"] == 1.0
    assert kwargs["session_id"] == session_id
    assert kwargs["operator_id"] == OPERATOR_ID
    assert kwargs["gear"] == "D"  # G-10：前端 gear 透传（不再丢弃）
    assert kwargs["brake"] is None  # 未携带 → None（生产者不写键）


def test_ws_control_clamps_values(rc_env: Any) -> None:
    """超限目标值入向限幅（±RC_MAX_SPEED_MPS / ±RC_MAX_STEER_RAD）。"""
    env = rc_env
    session_id = _create_session(env)
    with _client().websocket_connect(
        f"/ws/remote/{session_id}/control", headers=_auth(make_ws_token())
    ) as ws:
        ws.send_text(_control_frame(session_id, velocity=9.9, steer=-3.3))
        _recv_until(ws, "status")
    kwargs = [f for f in env.frame.frames if f[0] == "control"][-1][2]
    assert kwargs["target_velocity"] == settings.rc_max_speed_mps
    assert kwargs["target_steer"] == -settings.rc_max_steer_rad


def test_ws_control_over_frequency_dropped(rc_env: Any) -> None:
    """>25Hz 超频：滑窗内多余帧丢弃不投递（不断连、不 429）。"""
    env = rc_env
    session_id = _create_session(env)
    with _client().websocket_connect(
        f"/ws/remote/{session_id}/control", headers=_auth(make_ws_token())
    ) as ws:
        for _ in range(settings.rc_max_command_rate_hz + 15):
            ws.send_text(_control_frame(session_id))
        _recv_until(ws, "status")
    delivered = [f for f in env.frame.frames if f[0] == "control"]
    assert 0 < len(delivered) <= settings.rc_max_command_rate_hz


def test_ws_invalid_frame_error_without_disconnect(rc_env: Any) -> None:
    """非法上行帧 → error(2001) 帧但保持连接（契约 frames.error 语义）。"""
    env = rc_env
    session_id = _create_session(env)
    with _client().websocket_connect(
        f"/ws/remote/{session_id}/control", headers=_auth(make_ws_token())
    ) as ws:
        ws.send_text('{"type":"control","session_id":"broken"}')
        error = _recv_until(ws, "error")
        assert error["code"] == 2001
        ws.send_text(_control_frame(session_id))  # 连接仍可用
        _recv_until(ws, "status")
    assert [f for f in env.frame.frames if f[0] == "control"]


def test_ws_heartbeat_frame_refreshes_session(rc_env: Any) -> None:
    env = rc_env
    session_id = _create_session(env)
    key = f"rc:session:{VEHICLE_ONLINE}"
    assert not env.redis.store[key].get("last_heartbeat_at")  # 创建时置空，首帧刷新
    with _client().websocket_connect(
        f"/ws/remote/{session_id}/control", headers=_auth(make_ws_token())
    ) as ws:
        ws.send_text(json.dumps({"type": "heartbeat", "session_id": session_id}))
        _recv_until(ws, "status")
    assert float(env.redis.store[key]["last_heartbeat_at"]) > 0.0  # 心跳帧已刷新


def test_ws_estop_converges_session(rc_env: Any) -> None:
    """estop：零速帧尽力投递 → 会话收敛 ended → status(ended) 帧 + 正常关闭。"""
    env = rc_env
    session_id = _create_session(env)
    with _client().websocket_connect(
        f"/ws/remote/{session_id}/control", headers=_auth(make_ws_token())
    ) as ws:
        ws.send_text(
            json.dumps(
                {
                    "type": "estop",
                    "session_id": session_id,
                    "reason": "operator_emergency_stop",
                }
            )
        )
        frame = _recv_until(ws, "status")
        assert frame["status"] == "ended"
        with pytest.raises(WebSocketDisconnect):
            ws.receive_json()
    control = [f for f in env.frame.frames if f[0] == "control"]
    assert control[-1][2]["target_velocity"] == 0.0
    assert control[-1][2]["target_steer"] == 0.0
    assert control[-1][2]["brake"] == 1.0  # G-10：estop 零速帧固定满制动请求
    assert key_gone(env)  # end_session 完整收敛（Hash 删除）


def key_gone(env: Any) -> bool:
    return f"rc:session:{VEHICLE_ONLINE}" not in env.redis.store


def test_ws_signal_relays_sdp_and_ice(rc_env: Any) -> None:
    env = rc_env
    session_id = _create_session(env)
    with _client().websocket_connect(
        f"/ws/remote/{session_id}/signal", headers=_auth(make_ws_token())
    ) as ws:
        ws.send_text(
            json.dumps({"type": "sdp", "sdp_type": "offer", "sdp": "v=0\r\nm=video ..."})
        )
        ws.send_text(
            json.dumps(
                {
                    "type": "ice",
                    "candidate": "candidate:1 1 udp 2122260223 1.2.3.4 5 typ host",
                    "sdpMid": "0",
                    "sdpMLineIndex": 0,
                }
            )
        )
    relays = [c for c in env.command.commands if c[0] == "rc_signal_relay"]
    assert [r[2]["signal"]["type"] for r in relays] == ["sdp", "ice"]
    assert all(r[1] == VEHICLE_ONLINE for r in relays)
    assert relays[0][2]["session_id"] == session_id


def test_ws_signal_rejects_control_frame(rc_env: Any) -> None:
    """signal 通道不接受 control 帧 → error(2001)（通道隔离）。"""
    env = rc_env
    session_id = _create_session(env)
    with _client().websocket_connect(
        f"/ws/remote/{session_id}/signal", headers=_auth(make_ws_token())
    ) as ws:
        ws.send_text(_control_frame(session_id))
        error = _recv_until(ws, "error")
        assert error["code"] == 2001
    assert not [f for f in env.frame.frames if f[0] == "control"]


# =====================================================================
# 握手拒绝路径（不 accept → 关闭；TestClient 表现为 WebSocketDisconnect）
# =====================================================================
def test_ws_handshake_requires_token(rc_env: Any) -> None:
    session_id = _create_session(rc_env)
    with pytest.raises(WebSocketDisconnect) as exc_info, _client().websocket_connect(
        f"/ws/remote/{session_id}/control"
    ):
        pass
    assert exc_info.value.code in (1000, 1008)  # 未 accept 即关闭（HTTP 拒绝语义）


def test_ws_handshake_rejects_expired_token(rc_env: Any) -> None:
    session_id = _create_session(rc_env)
    token = make_ws_token(expires_in=-30)
    with pytest.raises(WebSocketDisconnect) as exc_info, _client().websocket_connect(
        f"/ws/remote/{session_id}/control", headers=_auth(token)
    ):
        pass
    assert exc_info.value.code in (1000, 1008)


def test_ws_handshake_rejects_refresh_token(rc_env: Any) -> None:
    """typ != access（Refresh Token 冒用）→ 1001 语义拒绝。"""
    import jwt as pyjwt

    now = int(time.time())
    token = pyjwt.encode(
        {
            "sub": OPERATOR_ID,
            "roles": ["operator"],
            "iat": now,
            "exp": now + 300,
            "iss": settings.jwt_issuer,
            "typ": "refresh",
        },
        settings.jwt_secret_key,
        algorithm=settings.jwt_algorithm,
    )
    session_id = _create_session(rc_env)
    with pytest.raises(WebSocketDisconnect), _client().websocket_connect(
        f"/ws/remote/{session_id}/control", headers=_auth(token)
    ):
        pass


def test_ws_handshake_rejects_viewer_role(rc_env: Any) -> None:
    """roles 与 rc_execute_role_set 无交集 → 1002 语义拒绝（不进入帧循环）。"""
    session_id = _create_session(rc_env)
    token = make_ws_token(roles=("viewer",))
    with pytest.raises(WebSocketDisconnect), _client().websocket_connect(
        f"/ws/remote/{session_id}/control", headers=_auth(token)
    ):
        pass


def test_ws_handshake_rejects_foreign_session(rc_env: Any) -> None:
    """会话归属其他操作员 → 拒绝；admin 例外放行（与 REST 权限口径一致）。"""
    session_id = _create_session(rc_env)
    with pytest.raises(WebSocketDisconnect), _client().websocket_connect(
        f"/ws/remote/{session_id}/control",
        headers=_auth(make_ws_token(user_id=OTHER_ID)),
    ):
        pass
    admin_token = make_ws_token(user_id=ADMIN_ID, roles=("admin",))
    with _client().websocket_connect(
        f"/ws/remote/{session_id}/control", headers=_auth(admin_token)
    ) as ws:
        _recv_until(ws, "status")


def test_ws_handshake_unknown_session_closed(rc_env: Any) -> None:
    with pytest.raises(WebSocketDisconnect) as exc_info, _client().websocket_connect(
        f"/ws/remote/{uuid.uuid4()}/control", headers=_auth(make_ws_token())
    ):
        pass
    assert exc_info.value.code in (1000, 4001)


def test_ws_control_replaced_by_new_connection(rc_env: Any) -> None:
    """同一会话新 control 连接替代旧连接（契约 close 4008）。"""
    session_id = _create_session(rc_env)
    client = _client()
    with client.websocket_connect(
        f"/ws/remote/{session_id}/control", headers=_auth(make_ws_token())
    ) as old:
        with client.websocket_connect(
            f"/ws/remote/{session_id}/control", headers=_auth(make_ws_token())
        ):
            pass
        with pytest.raises(WebSocketDisconnect) as exc_info:
            old.receive_json()
    assert exc_info.value.code in (1000, 4008)


def test_ws_token_expiry_closes_connection(rc_env: Any) -> None:
    """连接期 Token 过期 → error(1003) 帧 + 关闭（ticker 复检路径）。"""
    session_id = _create_session(rc_env)
    token = make_ws_token(expires_in=2)
    with _client().websocket_connect(
        f"/ws/remote/{session_id}/control", headers=_auth(token)
    ) as ws:
        error = _recv_until(ws, "error", max_frames=12)
        assert error["code"] == 1003
        with pytest.raises(WebSocketDisconnect):
            ws.receive_json()


# =====================================================================
# command_result 消费者处理链路（绕过 Kafka 循环，直测 _process）
# =====================================================================
def _bare_consumer(env: Any) -> CommandResultConsumer:
    consumer = CommandResultConsumer.__new__(CommandResultConsumer)
    consumer._settings = settings
    consumer._service = env.session_service
    consumer._hub = env.ws_hub
    return consumer


def test_command_result_process_writes_ack(rc_env: Any) -> None:
    env = rc_env
    session_id = _create_session(env)
    consumer = _bare_consumer(env)
    conn = WsControlConnection(
        websocket=None,  # send_json 容错：发送异常仅记 debug 日志
        session_id=session_id,
        vehicle_id=VEHICLE_ONLINE,
        operator_id=OPERATOR_ID,
        expires_at=None,
        connected_at=time.time(),
    )
    conn.last_sent_at = time.time() - 0.08
    conn.last_sent_seq = 5
    conn.latency_samples = deque(maxlen=50)
    env.ws_hub.attach_control(conn)

    payload = CommandResultMessage(
        command_id=str(uuid.uuid4()),
        vehicle_id=VEHICLE_ONLINE,
        timestamp=time.time(),
        success=True,
    )
    asyncio.run(consumer._process(payload))
    mapping = env.redis.store[f"rc:session:{VEHICLE_ONLINE}"]
    assert int(mapping["commands_acked"]) == 1
    assert conn.last_ack_at is not None
    assert len(conn.latency_samples) == 1
    assert 0 < conn.latency_samples[0] < 5000


def test_command_result_without_session_is_noop(rc_env: Any) -> None:
    """无活跃会话的回执（历史指令/会话已删）→ 静默丢弃不抛错。"""
    consumer = _bare_consumer(rc_env)
    payload = CommandResultMessage(
        command_id=str(uuid.uuid4()),
        vehicle_id="HUNTER-900",
        timestamp=time.time(),
        success=True,
    )
    asyncio.run(consumer._process(payload))


def test_command_result_without_connection_is_stats_only(rc_env: Any) -> None:
    """回执落在他副本/连接已断开（本注册表无连接）→ 仅统计回写。"""
    env = rc_env
    _create_session(env)  # 会话存在但本副本无 control 连接
    consumer = _bare_consumer(env)
    payload = CommandResultMessage(
        command_id=str(uuid.uuid4()),
        vehicle_id=VEHICLE_ONLINE,
        timestamp=time.time(),
        success=False,
        result_code="4002",
    )
    asyncio.run(consumer._process(payload))
    mapping = env.redis.store[f"rc:session:{VEHICLE_ONLINE}"]
    assert int(mapping["commands_acked"]) == 1
