"""``hunter.{vehicle_id}.remote_control`` 帧生产者（契约 Kafka Topic 表）。

- 本任务（WS 通道延迟实现，pending #13/#14/#15/#16 被阻塞）仅产出会话 boot/stop
  心跳帧（heartbeat=true/false），控制量恒为零值 —— 语义：通知车端会话建立/释放，
  并触发车端录像机起停；车端在 boot 后自行按 20Hz 循环下发零速安全帧；
- 消息 key = vehicle_id（单车辆有序，系统约束第 4 条）；JSON 帧结构与
  contracts/kafka/schemas/remote_control.schema.json 对齐（type/timestamp/seq/
  session_id/operator_id/control/heartbeat/video/reason）；
- 投递失败 → ServiceUnavailableError（5001），由全局异常处理器转统一响应。
"""
from __future__ import annotations

import itertools
import time
from collections import defaultdict
from collections.abc import Awaitable, Callable
from typing import Any

from hunter_common.exceptions import ServiceUnavailableError
from hunter_common.logging import get_logger

from app.config import Settings
from app.schemas.common import SessionEndReason
from app.schemas.sessions import ControlChannelInfo, SessionHeartbeat, VideoConfig
from app.services.metrics import COMMANDS_TOTAL

#: 帧投递函数签名（依赖倒置：生产实现经 KafkaProducerManager.produce 包装，测试注入替身）
ProduceFn = Callable[[str, str | None, dict[str, Any]], Awaitable[None]]

logger = get_logger("app.producers.remote_control")


def _topic(vehicle_id: str) -> str:
    """车端 Topic 名（契约固定模板 hunter.{vehicle_id}.remote_control）。"""
    return f"hunter.{vehicle_id}.remote_control"


class RemoteControlFrameProducer:
    """远程操控 Kafka 帧生产者（boot/stop 心跳帧；20Hz 指令帧由 WS 通道接入后产出）。"""

    def __init__(self, produce: ProduceFn, settings: Settings) -> None:
        self._produce = produce
        self._settings = settings
        # seq 按车辆独立自增（remote_control.schema.json 要求单调递增）
        self._seq_counters: dict[str, itertools.count[int]] = defaultdict(itertools.count)

    def _next_seq(self, vehicle_id: str) -> int:
        """取下一帧序号（事件循环内串行调用，无并发竞争）。"""
        return next(self._seq_counters[vehicle_id])

    def _boot_frame(
        self,
        *,
        vehicle_id: str,
        session_id: str,
        operator_id: str,
        video: VideoConfig,
        control: ControlChannelInfo,
        heartbeat: SessionHeartbeat,
        webrtc_ready: bool,
    ) -> dict[str, Any]:
        """构建 boot 帧（heartbeat=true；控制量零值；video 通知车端起录像机）。"""
        return {
            "type": "remote_control",
            "timestamp": round(time.time(), 3),
            "seq": self._next_seq(vehicle_id),
            "session_id": session_id,
            "operator_id": operator_id,
            # 控制量零值 + heartbeat=true：仅会话建立通知，不产生任何动作
            # （真实 20Hz 控制帧由 WS 控制通道下发，本任务范围外）
            "control": {"throttle": 0.0, "brake": 0.0, "steer": 0.0, "gear": None},
            "heartbeat": True,
            "video": {
                "width": video.width,
                "height": video.height,
                "fps": video.fps,
                "codec": video.codec,
                "bitrate_kbps": video.max_bitrate_kbps,
                "keyframe_interval_s": video.keyframe_interval_s,
                "recording": True,  # boot → 车端启动 H.264 硬编录像（设计文档远程操控章节）
                "webrtc_ready": webrtc_ready,
            },
            "reason": None,
        }

    def _stop_frame(
        self,
        *,
        vehicle_id: str,
        session_id: str,
        operator_id: str,
        reason: SessionEndReason,
    ) -> dict[str, Any]:
        """构建 stop 帧（heartbeat=false；控制量零值；通知车端停录像并退出操控态）。"""
        return {
            "type": "remote_control",
            "timestamp": round(time.time(), 3),
            "seq": self._next_seq(vehicle_id),
            "session_id": session_id,
            "operator_id": operator_id,
            "control": {"throttle": 0.0, "brake": 0.0, "steer": 0.0, "gear": None},
            "heartbeat": False,
            "video": {
                "recording": False,  # stop → 车端停止录像并进入封装上传流程
                "webrtc_ready": False,
            },
            "reason": reason,
        }

    async def send_boot(
        self,
        vehicle_id: str,
        *,
        session_id: str,
        operator_id: str,
        video: VideoConfig,
        control: ControlChannelInfo,
        heartbeat: SessionHeartbeat,
        webrtc_ready: bool = False,
    ) -> None:
        """投递会话建立心跳帧（失败 → 5001，由调用方回滚会话状态）。"""
        frame = self._boot_frame(
            vehicle_id=vehicle_id,
            session_id=session_id,
            operator_id=operator_id,
            video=video,
            control=control,
            heartbeat=heartbeat,
            webrtc_ready=webrtc_ready,
        )
        try:
            await self._produce(_topic(vehicle_id), vehicle_id, frame)
        except Exception as exc:
            COMMANDS_TOTAL.labels(result="failed").inc()
            logger.error("rc_frame_boot_failed", vehicle_id=vehicle_id, session_id=session_id, error=str(exc))
            raise ServiceUnavailableError(message="远程操控指令通道不可用（Kafka 投递失败）") from exc
        COMMANDS_TOTAL.labels(result="sent").inc()
        logger.info(
            "rc_frame_boot_sent",
            vehicle_id=vehicle_id,
            session_id=session_id,
            hz=control.hz,
            max_speed_mps=control.max_speed_mps,
        )

    async def send_stop(
        self,
        vehicle_id: str,
        *,
        session_id: str,
        operator_id: str,
        reason: SessionEndReason,
    ) -> None:
        """投递会话释放心跳帧（尽力而为：失败仅记录，不阻断会话清理）。"""
        frame = self._stop_frame(
            vehicle_id=vehicle_id, session_id=session_id, operator_id=operator_id, reason=reason
        )
        try:
            await self._produce(_topic(vehicle_id), vehicle_id, frame)
        except Exception as exc:  # noqa: BLE001 — 释放路径尽力而为，失败不回滚已结束会话
            COMMANDS_TOTAL.labels(result="failed").inc()
            logger.error("rc_frame_stop_failed", vehicle_id=vehicle_id, session_id=session_id, error=str(exc))
            return
        COMMANDS_TOTAL.labels(result="sent").inc()
        logger.info("rc_frame_stop_sent", vehicle_id=vehicle_id, session_id=session_id, reason=reason)


__all__ = ["ProduceFn", "RemoteControlFrameProducer"]
