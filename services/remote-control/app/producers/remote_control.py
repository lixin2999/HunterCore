"""``hunter.{vehicle_id}.remote_control`` 帧生产者（契约 Kafka Topic 表）。

- 会话 boot/stop 心跳帧（heartbeat=true/false，控制量恒零值）——通知车端会话建立/释放
  并触发车端录像机起停；20Hz 控制帧由 WS 控制通道接入（G-09 :meth:`send_control_frame`）；
- 消息 key = vehicle_id（单车辆有序，系统约束第 4 条）；帧 JSON 与
  contracts/kafka/schemas/remote_control.schema.json 逐字段对齐（vehicle_id/timestamp/
  seq/session_id/operator_id/control/heartbeat + boot/stop 帧 type/video/reason）；
  G-10 契约修订后已全量收对（control 可选 brake/gear/mode，boot/stop 补发 vehicle_id、
  控制量键名改用契约词 target_velocity/target_steer）；
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


#: gear 受控词表（与 contracts/kafka/schemas/health.schema.json gear enum 一致，G-10）
GEAR_VOCAB: frozenset[str] = frozenset({"P", "R", "N", "D"})


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
        """构建 boot 帧（heartbeat=true；控制量零值；video 通知车端起录像机；G-10 收对 Schema）。"""
        return {
            "type": "remote_control",
            "vehicle_id": vehicle_id,
            "timestamp": round(time.time(), 3),
            "seq": self._next_seq(vehicle_id),
            "session_id": session_id,
            "operator_id": operator_id,
            # 控制量零值 + heartbeat=true：仅会话建立通知，不产生任何动作（gear=null 无换挡请求）
            "control": {"target_velocity": 0.0, "target_steer": 0.0, "brake": 0.0, "gear": None},
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
        """构建 stop 帧（heartbeat=false；控制量零值；通知车端停录像并退出操控态；G-10 收对 Schema）。"""
        return {
            "type": "remote_control",
            "vehicle_id": vehicle_id,
            "timestamp": round(time.time(), 3),
            "seq": self._next_seq(vehicle_id),
            "session_id": session_id,
            "operator_id": operator_id,
            "control": {"target_velocity": 0.0, "target_steer": 0.0, "brake": 0.0, "gear": None},
            "heartbeat": False,
            "video": {
                "recording": False,  # stop → 车端停止录像并进入封装上传流程
                "webrtc_ready": False,
            },
            "reason": reason,
        }

    async def send_control_frame(
        self,
        vehicle_id: str,
        *,
        session_id: str,
        operator_id: str,
        seq: int,
        target_velocity: float,
        target_steer: float,
        brake: float | None = None,
        gear: str | None = None,
        mode: str | None = None,
        heartbeat: bool = False,
    ) -> None:
        """投递 20Hz 控制帧（WS 控制通道 → Kafka；字段严格对齐 remote_control.schema.json）。

        seq 由调用方从 Redis rc:session.seq_last HINCRBY 取得（服务端序号不复用前端）；
        限幅（±RC_MAX_SPEED_MPS / ±RC_MAX_STEER_RAD）也在调用方完成，本方法只保证
        Schema 兼容兼截断（-2.0/2.0 为 Kafka Schema 硬界）。投递失败 → 5001（路由层转 error 帧）。

        G-10 可选控制量（None = 无请求，不写键保持 additionalProperties 兼容）：
        brake 截断到 0~1；gear 验词表 P/R/N/D（非法词丢弃该键）；mode 平台侧预留字段
        （值域 pending #21，当前路由层不传入，仅供内部/测试使用）。
        """
        control: dict[str, Any] = {
            "target_velocity": max(-2.0, min(2.0, target_velocity)),
            "target_steer": target_steer,
        }
        if brake is not None:
            control["brake"] = max(0.0, min(1.0, brake))
        if gear in GEAR_VOCAB:
            control["gear"] = gear
        if mode:
            control["mode"] = mode
        message = {
            "vehicle_id": vehicle_id,
            "timestamp": round(time.time(), 3),
            "seq": seq,
            "session_id": session_id,
            "operator_id": operator_id,
            "control": control,
            "heartbeat": heartbeat,
        }
        try:
            await self._produce(_topic(vehicle_id), vehicle_id, message)
        except Exception as exc:
            COMMANDS_TOTAL.labels(result="failed").inc()
            logger.error(
                "rc_frame_control_failed",
                vehicle_id=vehicle_id,
                session_id=session_id,
                seq=seq,
                error=str(exc),
            )
            raise ServiceUnavailableError(
                message="远程操控指令通道不可用（Kafka 投递失败）"
            ) from exc
        COMMANDS_TOTAL.labels(result="sent").inc()

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


__all__ = ["GEAR_VOCAB", "ProduceFn", "RemoteControlFrameProducer"]
