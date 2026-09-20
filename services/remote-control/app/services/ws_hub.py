"""WS 连接注册表（G-09；单副本内存注册表 + 会话粘性路由前提）。

- 契约 x-hunter-websocket-contract.sticky_routing：网关/Ingress 按 session_id 哈希
  将会话固定到同一副本，跨副本共享经 Redis rc:session 收敛（本注册表仅登记本副本连接）；
- control 通道同一会话仅保留 1 条连接：新连接建立 → 旧连接以 4008 关闭（替代语义）；
- signal 通道允许多连接（浏览器多标签/调试），广播式中继；
- :class:`WsControlConnection` 同时承载**连接级流控与统计状态**：
  上行超频滑窗（>RC_MAX_COMMAND_RATE_hz 丢弃计数）、最近指令下发时间（ack 近似关联）、
  回执延迟样本（status 帧 control_stats 均值；P95 待服务端 RTP 精确关联，pending #16）。
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any

from fastapi import WebSocket
from hunter_common.logging import get_logger

logger = get_logger("app.services.ws_hub")

#: 回执延迟样本窗口（status 帧均值统计；契约无逐条精确关联，仅链路健康视角）
_LATENCY_SAMPLE_WINDOW = 50


@dataclass
class WsControlConnection:
    """control 通道连接（含流控/统计状态）。"""

    websocket: WebSocket
    session_id: str
    vehicle_id: str
    operator_id: str
    expires_at: float | None  # Access Token exp（连接期复检 → 1008）
    #: 建连时间（心跳缺失判定宽限基准：新建连尚未首帧时不以 started_at 误判）
    connected_at: float = 0.0
    #: 超时事件计数（status 帧 control_stats.timeout_events；降级边沿触发）
    timeout_events: int = 0
    #: 超频滑窗起点（time.monotonic）
    window_start: float = 0.0
    #: 滑窗内已接收 control 帧数
    window_count: int = 0
    #: 最近一次成功投递 Kafka 的指令（ack 近似关联锚点）
    last_sent_at: float | None = None
    last_sent_seq: int = 0
    #: 最近一次收到 command_result 的本地时间
    last_ack_at: float | None = None
    #: 回执延迟样本（ms， deque 定长）
    latency_samples: deque[float] = field(
        default_factory=lambda: deque(maxlen=_LATENCY_SAMPLE_WINDOW)
    )

    def latency_avg_ms(self) -> float | None:
        if not self.latency_samples:
            return None
        return sum(self.latency_samples) / len(self.latency_samples)


class WsHub:
    """本副本 WS 连接注册表（路由与 command_result 消费者共用，事件循环内串行访问）。"""

    def __init__(self) -> None:
        self._control: dict[str, WsControlConnection] = {}
        self._signals: dict[str, list[WebSocket]] = {}

    # ---------- control 通道 ----------
    def attach_control(self, conn: WsControlConnection) -> WsControlConnection | None:
        """登记 control 连接；返回被替代的旧连接（调用方负责以 4008 关闭）。"""
        old = self._control.get(conn.session_id)
        self._control[conn.session_id] = conn
        return old

    def detach_control(self, session_id: str, conn: WsControlConnection) -> None:
        """注销连接（仅当仍是当前登记连接——避免被替代的旧连接误删新连接）。"""
        if self._control.get(session_id) is conn:
            self._control.pop(session_id, None)

    def get_control(self, session_id: str) -> WsControlConnection | None:
        return self._control.get(session_id)

    def find_control_by_vehicle(self, vehicle_id: str) -> WsControlConnection | None:
        """按车辆定位连接（command_result 仅携带 vehicle_id；单会话互斥下唯一）。"""
        for conn in self._control.values():
            if conn.vehicle_id == vehicle_id:
                return conn
        return None

    # ---------- signal 通道 ----------
    def attach_signal(self, session_id: str, websocket: WebSocket) -> None:
        self._signals.setdefault(session_id, []).append(websocket)

    def detach_signal(self, session_id: str, websocket: WebSocket) -> None:
        conns = self._signals.get(session_id)
        if not conns:
            return
        self._signals[session_id] = [item for item in conns if item is not websocket]
        if not self._signals[session_id]:
            self._signals.pop(session_id, None)

    def signal_peers(self, session_id: str) -> list[WebSocket]:
        """同会话其他 signal 连接（车端下行中继落地前的浏览器侧回环通道）。"""
        return list(self._signals.get(session_id, ()))

    # ---------- 下行推送（发送失败仅记录：断开由接收循环收尾） ----------
    async def send_json(self, websocket: WebSocket, frame: dict[str, Any]) -> bool:
        try:
            await websocket.send_json(frame)
            return True
        except Exception as exc:  # noqa: BLE001 — 推送失败不打断主循环（对端断开由 receive 侧收敛）
            logger.debug("ws_send_failed", error=str(exc), frame_type=frame.get("type"))
            return False

    async def broadcast_control(self, session_id: str, frame: dict[str, Any]) -> None:
        conn = self._control.get(session_id)
        if conn is not None:
            await self.send_json(conn.websocket, frame)

    async def close_all(self, code: int, reason: str) -> int:
        """停机广播关闭（服务端优雅退出路径；返回值 = 关闭连接数）。"""
        closed = 0
        for conn in list(self._control.values()):
            self.detach_control(conn.session_id, conn)
            try:
                await conn.websocket.close(code=code, reason=reason)
                closed += 1
            except Exception as exc:  # noqa: BLE001 — 停机路径尽力而为（已断开则忽略）
                logger.debug("ws_close_failed", session_id=conn.session_id, error=str(exc))
        for session_id in list(self._signals):
            for websocket in self.signal_peers(session_id):
                self.detach_signal(session_id, websocket)
                try:
                    await websocket.close(code=code, reason=reason)
                    closed += 1
                except Exception as exc:  # noqa: BLE001
                    logger.debug("ws_close_failed", session_id=session_id, error=str(exc))
        return closed


__all__ = ["WsControlConnection", "WsHub"]
