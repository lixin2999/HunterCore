"""11.3 远程操控端到端用例（会话互斥 → 20Hz 指令流 → 超时保护 → 状态回写）。

依据（系统关键约束第 15 条 + 第 8/10/12 条，安全约束不可更改）：
- 互斥：同一车辆同一时间仅允许一名操作员（冲突 → 7001）；车辆离线 → 4001 / 忙 → 4002
- 指令 20Hz（50ms 间隔）；>500ms 未收到指令 → 车辆自动减速停车
- 速度限制 2.0 m/s（服务端限幅，不依赖车端裁剪；Schema 上限同为 ±2.0）
- 视频：H.264 硬编码 720p@30fps，2-4Mbps，关键帧 1s（WSDL 信令走 /ws/remote/**）
- 指令延迟 ≤ 100ms（第 10 条）
- Redis：``rc:lock:{vehicle_id}``（SET NX PX 30000）、``rc:session:{vehicle_id}``（Hash）、
  ``vehicle:status:{vehicle_id}`` → remote_controlled（第 8/12 条）

容器型用例（Kafka/Redis）依赖 Docker，不可用时自动 skip（附原因）。
"""
from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from tests.support import broker, contracts, flow, messages, thresholds

pytestmark = [pytest.mark.e2e, pytest.mark.l5_case("11.3")]

#: 车辆号取自契约示例（remote_control.schema.json examples[0]）
VEHICLE_ID: str = str(contracts.schema_example("remote_control")["vehicle_id"])


def _session(operator: str = "op-A", **overrides: Any) -> flow.RemoteControlSession:
    """构造基准会话（默认在线/空闲，供互斥与指令用例覆写）。"""
    defaults: dict[str, Any] = {
        "vehicle_id": VEHICLE_ID,
        "session_id": "sess-l5-11-3",
        "operator_id": operator,
    }
    defaults.update(overrides)
    return flow.RemoteControlSession(**defaults)


# ---------------------------------------------------------------- 会话互斥与准入

def test_session_mutex_second_operator_rejected_7001() -> None:
    """互斥规则：A 占用后 B 请求 → (False, 7001)，不得抢占。

    纯逻辑层面以 ``locked_by="op-A"`` 模拟第二会话读到的 Redis 锁状态；
    Redis 真实互斥由 ``test_rc_lock_and_session_keys`` 验证（SET NX）。
    """
    first = _session("op-A")
    assert first.open() == (True, None)
    second = _session("op-B", locked_by="op-A")
    ok, err = second.open()
    assert not ok and err == 7001


def test_session_offline_4001_and_busy_4002() -> None:
    """准入：车辆离线 → 4001；车辆忙（OTA 中等）→ 4002；错误码不可自造。"""
    ok, err = _session(vehicle_online=False).open()
    assert not ok and err == 4001
    ok, err = _session(vehicle_busy=True).open()
    assert not ok and err == 4002


def test_session_reentry_same_operator_idempotent() -> None:
    """同一操作员重入（如页面刷新重连）视为幂等成功，不产生 7001。"""
    session = _session("op-A")
    assert session.open() == (True, None)
    assert session.open() == (True, None)
    assert session.locked_by == "op-A"


def test_session_close_releases_lock() -> None:
    """关闭会话必须释放车辆锁，之后新操作员可立即接管。"""
    first = _session("op-A")
    first.open()
    assert first.close() == flow.RC_SESSION_ENDED
    second = _session("op-B")
    assert second.open() == (True, None)
    assert second.locked_by == "op-B"


# ---------------------------------------------------------------- 指令安全（限幅/节奏/超时）

def test_velocity_clamped_to_contract_limit() -> None:
    """限幅：|v| > 2.0 m/s 一律截断到 ±2.0（服务端兜底，不依赖车端裁剪）。"""
    session = _session()
    session.open()
    assert session.clamp_velocity(3.0) == thresholds.RC_MAX_SPEED_MPS
    assert session.clamp_velocity(-3.0) == -thresholds.RC_MAX_SPEED_MPS
    assert session.clamp_velocity(1.2) == 1.2
    assert session.clamp_velocity(0.0) == 0.0


def test_command_rate_20hz_enforced() -> None:
    """指令节奏：50ms 间隔（20Hz）合规；平均 30ms（≈33Hz）超频必须判不合规。"""
    session = _session()
    session.open()
    ack = session.accept_command(seq=1, target_velocity=1.0, now=time.monotonic())
    assert ack["interval_ms"] == thresholds.RC_COMMAND_INTERVAL_MS
    assert session.command_rate_ok([50.0] * 20) is True
    assert session.command_rate_ok([30.0] * 20) is False
    assert session.command_rate_ok([]) is True


# ---------------------------------------------------------------- 视频与信令契约

def test_video_pipeline_parameters_match_contract() -> None:
    """视频参数（第 15 条）：H.264 720p@30fps、码率 2-4Mbps、关键帧 1s；信令走 /ws/remote/**。"""
    assert (thresholds.RC_VIDEO_WIDTH, thresholds.RC_VIDEO_HEIGHT, thresholds.RC_VIDEO_FPS) == (
        1280, 720, 30,
    )
    assert thresholds.RC_VIDEO_BITRATE_MIN_BPS == 2_000_000
    assert thresholds.RC_VIDEO_BITRATE_MAX_BPS == 4_000_000
    assert thresholds.RC_VIDEO_KEYFRAME_INTERVAL_S == 1
    ws_routes = contracts.gateway_websocket_routes()
    remote_ws = [route for route in ws_routes if route["path"].startswith("/ws/remote")]
    assert remote_ws, "网关契约必须包含 /ws/remote/** WebSocket 路由"
    assert remote_ws[0]["target_service"] == "remote-control"
    assert 7001 in remote_ws[0]["error_codes"] and 7002 in remote_ws[0]["error_codes"]


def test_remote_control_message_contract_and_speed_guard() -> None:
    """指令消息契约：合法 20Hz 指令通过 Schema；超速指令（3.0 m/s）必须被 Schema 拒绝。"""
    payload = messages.remote_control_message(
        VEHICLE_ID, seq=1, target_velocity=1.2, target_steer=0.05, session_id="sess-l5-11-3",
        operator_id="op-A",
    )
    contracts.assert_valid_message("remote_control", payload)
    illegal = messages.out_of_range_remote_control_message(VEHICLE_ID, seq=2, velocity=3.0)
    assert contracts.validate_message("remote_control", illegal), "超速指令必须被 Schema 拦截"


def test_vehicle_state_remote_controlled_is_contract_state() -> None:
    """操控中车辆状态必须落在第 12 条枚举内（remote_controlled），不可自造状态名。"""
    assert "remote_controlled" in thresholds.VEHICLE_STATES


# ---------------------------------------------------------------- Kafka 20Hz 指令流（容器）

async def test_command_stream_20hz_roundtrip_with_latency(kafka_bootstrap: str) -> None:
    """端到端：按 50ms 节奏下发 20 条指令 → 消费回读（顺序一致、延迟 ≤ 100ms p95）。"""
    topic = flow.platform_topic_of(VEHICLE_ID, "remote_control")
    session = _session()
    session.open()
    interval = thresholds.RC_COMMAND_INTERVAL_MS / 1000.0
    latencies_ms: list[float] = []
    for seq in range(1, 21):
        payload = messages.remote_control_message(
            VEHICLE_ID, seq=seq, target_velocity=session.clamp_velocity(1.2),
            session_id="sess-l5-11-3", operator_id="op-A",
        )
        started = time.monotonic()
        broker.produce(kafka_bootstrap, topic, VEHICLE_ID, payload)
        received = broker.consume(kafka_bootstrap, topic, max_messages=1, timeout_s=30)
        latencies_ms.append((time.monotonic() - started) * 1000.0)
        assert received and received[0]["seq"] == seq
        await asyncio.sleep(interval)
    assert session.command_rate_ok([interval * 1000.0] * 20) is True
    p95 = flow.processing_step_p95_ms(latencies_ms)
    assert p95 <= thresholds.RC_COMMAND_LATENCY_MAX_MS, f"指令链路 p95={p95}ms 超过 100ms"


# ---------------------------------------------------------------- Redis 会话键（容器）

async def test_rc_lock_and_session_keys(redis_handle: Any) -> None:
    """Redis：rc:lock 互斥（SET NX PX 30000）、rc:session Hash、vehicle:status 回写。"""
    redis_module = pytest.importorskip("redis.asyncio", reason="缺少 redis 依赖：pip install redis")
    lock_key = flow.rc_lock_key(VEHICLE_ID)
    session_key = flow.rc_session_key(VEHICLE_ID)
    status_key = flow.vehicle_status_key(VEHICLE_ID)
    client = redis_module.Redis(
        host=redis_handle.host, port=redis_handle.port, decode_responses=True
    )
    try:
        first = await client.set(lock_key, "op-A", nx=True, px=30_000)
        assert first is True, "首操作员必须能获取互斥锁"
        second = await client.set(lock_key, "op-B", nx=True, px=30_000)
        assert second is None, "锁被占用时第二操作员不得抢占（对应 7001）"
        pttl = await client.pttl(lock_key)
        assert 0 < pttl <= 30_000, "互斥锁必须带 30s TTL（异常退出兜底释放）"

        await client.hset(
            session_key,
            mapping={"session_id": "sess-l5-11-3", "operator_id": "op-A", "status": "active"},
        )
        snapshot = await client.hgetall(session_key)
        assert snapshot["operator_id"] == "op-A" and snapshot["status"] == "active"

        await client.hset(status_key, mapping={"status": "remote_controlled"})
        assert (await client.hget(status_key, "status")) == "remote_controlled"
    finally:
        await client.delete(lock_key, session_key, status_key)
        await client.aclose()


def test_command_timeout_triggers_safety_stop() -> None:
    """超时保护：>500ms 无指令 → 判定超时 → 安全动作 = 减速停车（目标速度 0）。"""
    session = _session()
    session.open()
    now = time.monotonic()
    session.accept_command(seq=1, target_velocity=1.5, now=now)
    assert session.command_timeout_hit(now + 0.4) is False
    assert session.command_timeout_hit(now + 0.6) is True
    action = session.safety_stop_action()
    assert action["action"] == "decelerate_to_stop"
    assert action["target_velocity"] == 0.0


def test_commands_rejected_without_active_session() -> None:
    """会话未激活（ended）时不得接受指令（断言失败即服务端应拒绝）。"""
    session = _session()
    session.open()
    session.close()
    with pytest.raises(AssertionError):
        session.accept_command(seq=2, target_velocity=1.0, now=time.monotonic())
