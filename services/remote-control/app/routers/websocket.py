"""/ws/remote/{session_id}/** WebSocket 路由（G-09；契约 x-hunter-websocket-contract）。

端点（operationId 对齐契约 x-hunter-endpoints）：
- ``wsRemoteControl``：control 通道（rbac remote:execute）——control 帧 20Hz 转发 Kafka
  ``hunter.{vehicle_id}.remote_control``（服务端 seq/限幅/超频丢弃）+ heartbeat 帧刷新
  ``rc:session.last_heartbeat_at`` + estop 帧零速收敛 + 下行 ack/status/error 帧；
- ``wsRemoteSignal``：signal 通道（rbac remote:create）——sdp/ice 帧经 command topic
  中继车端（command_type 配置注入，pending #17 同模式）。

握手拒绝语义（契约 handshake.failure「不进入帧循环」）：
- 鉴权/RBAC/Origin/限流失败 → 不 accept，以 1008 关闭（ASGI 层转 HTTP 拒绝）；
- 会话不存在/已结束 → 4001；越权接入 → 1008；
- 连接期 Token 过期 → error 帧（1003）+ 1008 关闭（契约 close_codes）。

active/degraded 状态机（近似落地，契约定稿前冻结）：
- connecting → active：WS 建连后首个被服务端接受的 control 帧（契约原文要求「首个指令
  回执」双就绪，回执近似关联受 pending #16 限制，落地按首帧激活并在此登记）；
- active → degraded：心跳缺失（> 3×10s）/ 指令 >500ms 无回执 / 浏览器断开（等待
  60s 心跳超时由 SessionReaper 收敛结束）；degraded → active：判据消失自动恢复（允许，
  契约 pending #4 现状）；
- 会话消失（DELETE/守护/管理员强制结束）→ status ticker 推送 error(3001) + 4001 关闭
  （4010 与 4001 的区分需结束原因跨副本回读 sidecar，暂统一 4001，登记 pending #14）。
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from hunter_common.exceptions import (
    HunterBaseException,
    ResourceNotFoundError,
    ServiceUnavailableError,
)
from hunter_common.logging import get_logger, set_trace_id

from app.config import settings
from app.core.dependencies import OperatorContext
from app.core.rate_limit import enforce_user_rate_limit
from app.core.ws_auth import WsIdentity, authenticate_ws_handshake
from app.schemas.common import DegradedReason, SessionEndReason, SessionStatus
from app.schemas.sessions import ControlStats
from app.schemas.websocket import (
    UplinkFrameError,
    WsControlFrameIn,
    WsEstopFrameIn,
    WsHeartbeatFrameIn,
    WsIceFrameIn,
    WsSdpFrameIn,
    error_frame,
    parse_uplink_frame,
    status_frame,
)
from app.services.metrics import FRAME_DROPPED_TOTAL
from app.services.session_service import (
    FIELD_COMMANDS_ACKED,
    FIELD_COMMANDS_SENT,
    FIELD_LAST_HEARTBEAT_AT,
    FIELD_SEQ_LAST,
    FIELD_SESSION_ID,
    FIELD_STARTED_AT,
    FIELD_STATUS,
    SessionService,
)
from app.services.ws_hub import WsControlConnection, WsHub

logger = get_logger("app.routers.websocket")

router = APIRouter(tags=["websocket"])

# ---------- WS 关闭码（契约 x-hunter-websocket-contract.frames.close_codes） ----------
CLOSE_NORMAL = 1000  # 正常结束（estop 收敛完成后）
CLOSE_POLICY = 1008  # 认证/权限失效（握手拒绝与连接期 Token 过期）
CLOSE_SESSION_GONE = 4001  # 会话已结束或不存在（3001 语义）
CLOSE_REPLACED = 4008  # 同一会话新连接替代旧连接
CLOSE_ADMIN = 4010  # 被管理员强制结束（区分依赖 sidecar 回读，当前统一 4001，见模块 docstring）

#: WS 握手限流 API 标识（rate_limit:{user_id}:{api}）
WS_HANDSHAKE_API = "remote:ws:handshake"


def _services(websocket: WebSocket) -> tuple[SessionService, WsHub]:
    state = websocket.app.state
    return state.session_service, state.ws_hub


async def _reject(websocket: WebSocket, code: int, reason: str) -> None:
    """握手阶段拒绝（未 accept：ASGI 服务器转 HTTP 4xx/403，不进入帧循环）。"""
    try:
        await websocket.close(code=code, reason=reason)
    except RuntimeError:  # 已 accept 的竞态路径：close 正常走帧层
        pass


async def _handshake(
    websocket: WebSocket, session_id: str, *, role_set: set[str]
) -> WsIdentity | None:
    """握手鉴权 + 限流 + 会话归属校验（失败已拒绝并返回 None）。"""
    set_trace_id(websocket.headers.get("x-request-id") or None)
    service, _hub = _services(websocket)
    try:
        identity = await authenticate_ws_handshake(websocket, role_set=role_set)
    except HunterBaseException as exc:
        logger.warning(
            "ws_handshake_auth_rejected",
            code=int(exc.code),
            session_id=session_id,
            reason=exc.message,
        )
        await _reject(websocket, CLOSE_POLICY, str(exc.code))
        return None
    except Exception:
        logger.exception("ws_handshake_auth_error")
        await _reject(websocket, CLOSE_POLICY, "handshake_error")
        return None
    try:
        await enforce_user_rate_limit(
            identity.operator.user_id,
            websocket.app.state.redis,
            api=WS_HANDSHAKE_API,
            limit_per_min=settings.ws_handshake_rate_limit_per_min,
            settings=settings,
        )
    except Exception:  # noqa: BLE001 — 限流超限/fail-open 异常外溢 → 一律拒绝握手
        logger.warning("ws_handshake_rate_limited", user_id=identity.operator.user_id)
        await _reject(websocket, CLOSE_POLICY, "rate_limited")
        return None
    try:
        await service.resolve_ws_session(session_id, identity.operator)
    except ResourceNotFoundError:
        await _reject(websocket, CLOSE_SESSION_GONE, "session_gone")
        return None
    except HunterBaseException:
        await _reject(websocket, CLOSE_POLICY, "forbidden")
        return None
    return identity


# =====================================================================
# control 通道（wsRemoteControl）
# =====================================================================
@router.websocket("/ws/remote/{session_id}/control")
async def ws_remote_control(websocket: WebSocket, session_id: str) -> None:
    """控制指令上行（20Hz）+ 回执/状态下行 + 10s 心跳（契约 frames.control_*）。"""
    service, hub = _services(websocket)
    identity = await _handshake(
        websocket, session_id, role_set=settings.rc_execute_role_set
    )
    if identity is None:
        return
    vehicle_id, _mapping = await service.resolve_ws_session(session_id, identity.operator)
    await websocket.accept(subprotocol=identity.echo_subprotocol)

    conn = WsControlConnection(
        websocket=websocket,
        session_id=session_id,
        vehicle_id=vehicle_id,
        operator_id=identity.operator.user_id,
        expires_at=identity.expires_at,
        connected_at=time.time(),
    )
    old = hub.attach_control(conn)
    if old is not None:
        # 同一会话新连接替代旧连接（契约 close 4008；单会话仅保留一条 control）
        await old.websocket.close(code=CLOSE_REPLACED, reason="replaced")
    logger.info(
        "ws_control_connected",
        vehicle_id=vehicle_id,
        session_id=session_id,
        operator_id=conn.operator_id,
    )
    ticker = asyncio.create_task(
        _status_ticker(service, hub, conn), name=f"rc-status-{session_id[:8]}"
    )
    try:
        await _control_recv_loop(service, hub, conn, identity.operator)
    except WebSocketDisconnect:
        pass
    finally:
        ticker.cancel()
        hub.detach_control(session_id, conn)
        # 浏览器断开 → 会话 degraded（等待心跳超时 60s 由 SessionReaper 收敛；
        # 期间车辆因 >500ms 无指令已车端减速停车，契约 disconnect 第 1 条）
        await service.set_session_status(vehicle_id, SessionStatus.DEGRADED)
        logger.info(
            "ws_control_disconnected",
            vehicle_id=vehicle_id,
            session_id=session_id,
            operator_id=conn.operator_id,
        )


async def _control_recv_loop(
    service: SessionService,
    hub: WsHub,
    conn: WsControlConnection,
    operator: OperatorContext,
) -> None:
    """上行帧循环（control/heartbeat/estop；非法帧 error(2001) 不断连）。"""
    websocket = conn.websocket
    while True:
        raw = await websocket.receive_text()
        if len(raw.encode("utf-8")) > settings.ws_uplink_max_bytes:
            await hub.send_json(
                websocket, error_frame(2001, "上行帧超出单帧字节上限", None)
            )
            continue
        try:
            frame = parse_uplink_frame(raw)
        except UplinkFrameError as exc:
            await hub.send_json(websocket, error_frame(2001, exc.message, None))
            continue
        if isinstance(frame, WsControlFrameIn):
            stop = await _handle_control_frame(service, hub, conn, frame)
        elif isinstance(frame, WsHeartbeatFrameIn):
            if frame.session_id != conn.session_id:
                await hub.send_json(
                    websocket, error_frame(2001, "session_id 与连接会话不一致", None)
                )
                continue
            await service.touch_session_heartbeat(conn.vehicle_id)
            stop = False
        elif isinstance(frame, WsEstopFrameIn):
            stop = await _handle_estop(service, hub, conn, operator, frame)
        else:
            await hub.send_json(
                websocket,
                error_frame(2001, f"control 通道不接受 {frame.type} 帧", None),  # type: ignore[attr-defined]
            )
            stop = False
        if stop:
            return


async def _handle_control_frame(
    service: SessionService,
    hub: WsHub,
    conn: WsControlConnection,
    frame: WsControlFrameIn,
) -> bool:
    """control 帧：超频丢弃 → 限幅 → 服务端 seq → Kafka 投递（最新值优先语义）。

    单会话窗口「仅保留最近 1 帧」由串行事件循环天然满足（后到帧即最新值）。
    返回 True = 需要结束接收循环。
    """
    if frame.session_id != conn.session_id:
        await hub.send_json(
            conn.websocket, error_frame(2001, "session_id 与连接会话不一致", None)
        )
        return False
    now_m = time.monotonic()
    if now_m - conn.window_start >= 1.0:
        conn.window_start, conn.window_count = now_m, 0
    conn.window_count += 1
    if conn.window_count > settings.rc_max_command_rate_hz:
        # 上行超频（>25Hz）：丢弃多余帧并计数（WS 不适用 HTTP 429，契约 flow_control）
        FRAME_DROPPED_TOTAL.labels(reason="over_frequency").inc()
        return False
    max_v = settings.rc_max_speed_mps
    max_s = settings.rc_max_steer_rad
    target_velocity = max(-max_v, min(max_v, frame.control.target_velocity))
    target_steer = max(-max_s, min(max_s, frame.control.target_steer))
    seq = await service.next_control_seq(conn.vehicle_id)
    if seq is None:
        await hub.send_json(
            conn.websocket, error_frame(3001, "操控会话已结束或不存在", None)
        )
        await conn.websocket.close(code=CLOSE_SESSION_GONE, reason="session_gone")
        return True
    try:
        await conn.websocket.app.state.frame_producer.send_control_frame(
            conn.vehicle_id,
            session_id=conn.session_id,
            operator_id=conn.operator_id,
            seq=seq,
            target_velocity=target_velocity,
            target_steer=target_steer,
            # G-10 契约化可选控制量（None 不写键；gear 已经 Literal 验词、brake 已限幅 0~1）
            gear=frame.control.gear,
            brake=frame.control.brake,
        )
    except ServiceUnavailableError as exc:
        await hub.send_json(conn.websocket, error_frame(5001, exc.message, None))
        return False
    conn.last_sent_at = time.time()
    conn.last_sent_seq = seq
    # 首个被接受的 control 帧 → connecting 激活（近似，登记见模块 docstring）
    await service.touch_session_heartbeat(conn.vehicle_id, activate=True)
    return False


async def _handle_estop(
    service: SessionService,
    hub: WsHub,
    conn: WsControlConnection,
    operator: OperatorContext,
    frame: WsEstopFrameIn,
) -> bool:
    """estop：立即零速帧 → 「先安全后清理」收敛会话（审计日志，契约 x-hunter-audit-log）。"""
    logger.warning(
        "rc_estop_received",
        vehicle_id=conn.vehicle_id,
        session_id=conn.session_id,
        operator_id=conn.operator_id,
        reason=frame.reason,
    )
    seq = await service.next_control_seq(conn.vehicle_id)
    if seq is not None:
        try:
            await conn.websocket.app.state.frame_producer.send_control_frame(
                conn.vehicle_id,
                session_id=conn.session_id,
                operator_id=conn.operator_id,
                seq=seq,
                target_velocity=0.0,
                target_steer=0.0,
                brake=1.0,  # G-10：estop 零速帧固定满制动请求（契约 control.brake 语义）
            )
        except ServiceUnavailableError as exc:
            # 零速帧失败不阻断收敛（车端 >500ms 无指令自动减速停车兜底）
            await hub.send_json(conn.websocket, error_frame(5001, exc.message, None))
    try:
        await service.end_session(
            conn.session_id, operator, SessionEndReason.OPERATOR_END
        )
    except ResourceNotFoundError:
        pass  # 幂等：已被其他路径结束
    await hub.send_json(
        conn.websocket, status_frame(SessionStatus.ENDED.value, [], None)
    )
    await conn.websocket.close(code=CLOSE_NORMAL, reason="estop_converged")
    return True


async def _status_ticker(
    service: SessionService, hub: WsHub, conn: WsControlConnection
) -> None:
    """1Hz 状态推送 + 连接期守护（会话消失 4001 / Token 过期 1008 / 降级判定）。"""
    interval_s = settings.rc_heartbeat_interval_s
    degraded_after = interval_s * settings.rc_heartbeat_degraded_misses
    timeout_s = settings.rc_stop_on_timeout_ms / 1000
    previous_reasons: set[str] = set()
    while True:
        await asyncio.sleep(1)
        mapping = await service.get_session_mapping(conn.vehicle_id)
        if mapping is None or mapping.get(FIELD_SESSION_ID) != conn.session_id:
            await hub.send_json(
                conn.websocket, error_frame(3001, "操控会话已结束或不存在", None)
            )
            await conn.websocket.close(code=CLOSE_SESSION_GONE, reason="session_gone")
            return
        now = time.time()
        if conn.expires_at is not None and now >= conn.expires_at:
            await hub.send_json(
                conn.websocket, error_frame(1003, "Token 已过期，请刷新后重连", None)
            )
            await conn.websocket.close(code=CLOSE_POLICY, reason="token_expired")
            return
        reasons: set[str] = set()
        last_beat = _to_float(mapping.get(FIELD_LAST_HEARTBEAT_AT)) or _to_float(
            mapping.get(FIELD_STARTED_AT)
        )
        beat_base = max(last_beat or 0.0, conn.connected_at)
        if now - beat_base > degraded_after:
            reasons.add(DegradedReason.HEARTBEAT_MISS.value)
        if (
            conn.last_sent_at is not None
            and now - conn.last_sent_at > timeout_s
            and (conn.last_ack_at is None or conn.last_ack_at < conn.last_sent_at)
        ):
            reasons.add(DegradedReason.COMMAND_TIMEOUT.value)
        # 超时事件边沿计数（进入 command_timeout 时 +1，契约 control_stats.timeout_events）
        if DegradedReason.COMMAND_TIMEOUT.value in reasons and (
            DegradedReason.COMMAND_TIMEOUT.value not in previous_reasons
        ):
            conn.timeout_events += 1
        previous_reasons = reasons
        status = SessionStatus(
            mapping.get(FIELD_STATUS) or SessionStatus.CONNECTING.value
        )
        if reasons and status is SessionStatus.ACTIVE:
            await service.set_session_status(conn.vehicle_id, SessionStatus.DEGRADED)
            status = SessionStatus.DEGRADED
        elif not reasons and status is SessionStatus.DEGRADED:
            await service.set_session_status(conn.vehicle_id, SessionStatus.ACTIVE)
            status = SessionStatus.ACTIVE
        stats = ControlStats(
            commands_sent=_to_int(mapping.get(FIELD_COMMANDS_SENT)) or 0,
            commands_acked=_to_int(mapping.get(FIELD_COMMANDS_ACKED)) or 0,
            ack_latency_ms_avg=conn.latency_avg_ms(),
            ack_latency_ms_p95=None,  # 逐条精确关联受 pending #16 限制
            timeout_events=conn.timeout_events,
            last_seq=_to_int(mapping.get(FIELD_SEQ_LAST)) or 0,
            last_ack_at=conn.last_ack_at,
        )
        await hub.send_json(
            conn.websocket,
            status_frame(status.value, sorted(reasons), stats.model_dump(mode="json")),
        )


# =====================================================================
# signal 通道（wsRemoteSignal）
# =====================================================================
@router.websocket("/ws/remote/{session_id}/signal")
async def ws_remote_signal(websocket: WebSocket, session_id: str) -> None:
    """WebRTC 信令中继（浏览器 sdp/ice → command topic 中继车端；多连接允许）。"""
    service, hub = _services(websocket)
    identity = await _handshake(
        websocket, session_id, role_set=settings.rc_create_role_set
    )
    if identity is None:
        return
    vehicle_id, _mapping = await service.resolve_ws_session(
        session_id, identity.operator
    )
    await websocket.accept(subprotocol=identity.echo_subprotocol)
    hub.attach_signal(session_id, websocket)
    logger.info(
        "ws_signal_connected",
        vehicle_id=vehicle_id,
        session_id=session_id,
        operator_id=identity.operator.user_id,
    )
    try:
        while True:
            raw = await websocket.receive_text()
            if len(raw.encode("utf-8")) > settings.ws_uplink_max_bytes:
                await hub.send_json(
                    websocket, error_frame(2001, "信令帧超出单帧字节上限", None)
                )
                continue
            try:
                frame = parse_uplink_frame(raw)
            except UplinkFrameError as exc:
                await hub.send_json(websocket, error_frame(2001, exc.message, None))
                continue
            if not isinstance(frame, (WsSdpFrameIn, WsIceFrameIn)):
                await hub.send_json(
                    websocket,
                    error_frame(2001, f"signal 通道不接受 {frame.type} 帧", None),  # type: ignore[attr-defined]
                )
                continue
            await _relay_signal(hub, websocket, vehicle_id, session_id, identity, raw)
    except WebSocketDisconnect:
        pass
    finally:
        hub.detach_signal(session_id, websocket)
        logger.info("ws_signal_disconnected", vehicle_id=vehicle_id, session_id=session_id)


async def _relay_signal(
    hub: WsHub,
    websocket: WebSocket,
    vehicle_id: str,
    session_id: str,
    identity: WsIdentity,
    raw: str,
) -> None:
    """信令帧原文中继车端（command topic；失败 error(5001) 不断连）。"""
    signal: dict[str, Any] = json.loads(raw)
    try:
        await websocket.app.state.command_producer.send_signal_relay(
            vehicle_id,
            session_id=session_id,
            operator_id=identity.operator.user_id,
            signal=signal,
        )
    except ServiceUnavailableError as exc:
        await hub.send_json(websocket, error_frame(5001, exc.message, None))


# ---------- Hash 值宽松解析（与 session_service 内部工具同语义，避免跨模块私有依赖） ----------
def _to_int(value: str | None) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _to_float(value: str | None) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


__all__ = ["WS_HANDSHAKE_API", "router"]
