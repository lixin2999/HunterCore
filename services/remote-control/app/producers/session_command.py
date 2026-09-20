"""``hunter.{vehicle_id}.command`` 会话信令生产者（契约 command.schema.json）。

- 信令：rc_session_start / rc_session_end（类型取值经环境变量注入 —— pending #17
  的 command.type 枚举未定稿，契约定稿后仅调整 RC_COMMAND_SESSION_*_TYPE 配置）；
- 信封字段（command_id/type/issued_by/issued_at/timeout_ms/payload）与
  contracts/kafka/schemas/command.schema.json 逐字对齐；
- 消息 key = vehicle_id（单车辆有序）；投递失败 → ServiceUnavailableError（5001）。
"""
from __future__ import annotations

import time
from typing import Any
from uuid import uuid4

from hunter_common.exceptions import ServiceUnavailableError
from hunter_common.logging import get_logger

from app.config import Settings
from app.producers.remote_control import ProduceFn
from app.schemas.common import SessionEndReason
from app.schemas.sessions import RecordArchiveInfo, VideoConfig, WebRtcConnectionInfo
from app.services.metrics import COMMANDS_TOTAL

logger = get_logger("app.producers.session_command")


def _topic(vehicle_id: str) -> str:
    """车端指令 Topic 名（契约固定模板 hunter.{vehicle_id}.command）。"""
    return f"hunter.{vehicle_id}.command"


class SessionCommandProducer:
    """操控会话信令生产者（platform→vehicle；acks=all 由生产者全局配置保证）。"""

    def __init__(self, produce: ProduceFn, settings: Settings) -> None:
        self._produce = produce
        self._settings = settings

    async def _send(self, vehicle_id: str, envelope: dict[str, Any]) -> None:
        """投递信令信封（失败 → 5001 + 指标）。"""
        try:
            await self._produce(_topic(vehicle_id), vehicle_id, envelope)
        except Exception as exc:
            COMMANDS_TOTAL.labels(result="failed").inc()
            logger.error(
                "session_command_failed",
                vehicle_id=vehicle_id,
                command_id=envelope.get("command_id"),
                type=envelope.get("type"),
                error=str(exc),
            )
            raise ServiceUnavailableError(message="车辆指令通道不可用（Kafka 投递失败）") from exc
        COMMANDS_TOTAL.labels(result="sent").inc()

    async def send_session_start(
        self,
        vehicle_id: str,
        *,
        session_id: str,
        operator_id: str,
        issued_by: str,
        video: VideoConfig,
        webrtc: WebRtcConnectionInfo,
        record: RecordArchiveInfo,
    ) -> str:
        """下发 rc_session_start 信令；返回 command_id（供审计追踪）。"""
        settings = self._settings
        command_id = str(uuid4())
        envelope: dict[str, Any] = {
            "command_id": command_id,
            # pending #17：type 取值域未定稿，经 RC_COMMAND_SESSION_START_TYPE 配置注入
            "type": settings.rc_command_session_start_type,
            "issued_by": issued_by,
            "issued_at": round(time.time(), 3),
            "timeout_ms": settings.rc_session_command_timeout_ms,
            "payload": {
                "session_id": session_id,
                "operator_id": operator_id,
                "max_speed_mps": settings.rc_max_speed_mps,
                "stop_on_timeout_ms": settings.rc_stop_on_timeout_ms,
                "video": {
                    "width": video.width,
                    "height": video.height,
                    "fps": video.fps,
                    "codec": video.codec,
                    "bitrate_kbps": video.max_bitrate_kbps,
                    "keyframe_interval_s": video.keyframe_interval_s,
                    "recording": True,
                },
                "record": {
                    "object_key": record.object_key,
                    "container": record.container,
                },
                "webrtc": {
                    "signal_ws_url": webrtc.signal_ws_url,
                    "media_server": {
                        "app": webrtc.media_server.app,
                        "stream": webrtc.media_server.stream,
                    },
                },
            },
        }
        await self._send(vehicle_id, envelope)
        logger.info(
            "rc_session_start_sent",
            vehicle_id=vehicle_id,
            session_id=session_id,
            command_id=command_id,
            issued_by=issued_by,
        )
        return command_id

    async def send_session_end(
        self,
        vehicle_id: str,
        *,
        session_id: str,
        operator_id: str,
        issued_by: str,
        reason: SessionEndReason,
    ) -> str:
        """下发 rc_session_end 信令；返回 command_id（释放路径尽力而为）。"""
        command_id = str(uuid4())
        envelope: dict[str, Any] = {
            "command_id": command_id,
            "type": self._settings.rc_command_session_end_type,
            "issued_by": issued_by,
            "issued_at": round(time.time(), 3),
            "timeout_ms": self._settings.rc_session_command_timeout_ms,
            "payload": {
                "session_id": session_id,
                "operator_id": operator_id,
                "reason": reason,
            },
        }
        try:
            await self._send(vehicle_id, envelope)
        except ServiceUnavailableError:
            # 释放路径尽力而为：信令失败不阻断会话清理（车端另有 500ms 无指令超时停车兜底）
            logger.error("rc_session_end_send_failed", vehicle_id=vehicle_id, session_id=session_id)
            return command_id
        logger.info(
            "rc_session_end_sent",
            vehicle_id=vehicle_id,
            session_id=session_id,
            command_id=command_id,
            reason=reason,
        )
        return command_id

    async def send_signal_relay(
        self,
        vehicle_id: str,
        *,
        session_id: str,
        operator_id: str,
        signal: dict[str, Any],
    ) -> str:
        """下发 WebRTC 信令中继（sdp/ice 帧 → 车端；G-09 signal 通道）。

        command_type 取值经 RC_COMMAND_SIGNAL_RELAY_TYPE 配置注入（pending #17 同模式，
        定稿后只改配置不改代码）；payload.signal 为 WS 上行帧原文（契约
        x-hunter-websocket-contract.frames.signal）；车端→浏览器方向下行中继待
        SRS 集成定稿（pending #10/#12）。投递失败 → 5001（路由层转 error 帧）。
        """
        settings = self._settings
        command_id = str(uuid4())
        envelope: dict[str, Any] = {
            "command_id": command_id,
            "type": settings.rc_signal_relay_command_type,
            "issued_by": operator_id,
            "issued_at": round(time.time(), 3),
            "timeout_ms": settings.rc_session_command_timeout_ms,
            "payload": {
                "session_id": session_id,
                "operator_id": operator_id,
                "signal": signal,
            },
        }
        await self._send(vehicle_id, envelope)
        logger.info(
            "rc_signal_relay_sent",
            vehicle_id=vehicle_id,
            session_id=session_id,
            signal_type=signal.get("type"),
            command_id=command_id,
        )
        return command_id


__all__ = ["SessionCommandProducer"]
