"""模拟器主循环编排（消费 remote_control → 运动学积分 → 生产 telemetry/health/command_result）。

单线程事件循环（confluent 非阻塞 poll）避免竞态：控制帧经 ``poll(0)`` 就地排空驱动模型，
遥测/健康按各自频率节拍产出，视频推流线程只读运动学状态（float 读在 GIL 下原子）。
transport 经依赖注入（tests 用内存桩替身），运行时用 ``ConfluentTransport``。
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any

from vehicle_simulator.config import SimConfig
from vehicle_simulator.kafka_client import Transport
from vehicle_simulator.kinematics import KinematicModel, KinematicsConfig
from vehicle_simulator.messages import (
    build_command_result,
    build_health,
    build_telemetry,
    new_command_id,
)


class VehicleSimulator:
    """单车模拟器运行时（一个实例 = 一辆车 = 一个 SCRAM 身份）。"""

    def __init__(
        self,
        cfg: SimConfig,
        transport: Transport,
        *,
        video: Any = None,
        log: Callable[..., None] | None = None,
    ) -> None:
        self.cfg = cfg
        self.transport = transport
        self.video = video
        self._log = log or _default_log
        self.model = KinematicModel(KinematicsConfig(max_speed_mps=2.0))
        self.model.state.battery_soc = cfg.start_soc
        self._seq = 0
        self._acks_sent = 0
        self._frames_seen = 0
        self._session_id: str | None = None
        self._stop = threading.Event()

    # ---------- 状态快照（供视频线程读取） ----------
    def state_snapshot(self):
        return self.model.state

    @property
    def session_id(self) -> str | None:
        return self._session_id

    @property
    def acks_sent(self) -> int:
        return self._acks_sent

    # ---------- 控制面：单帧处理 ----------
    def handle_frame(self, frame: dict[str, Any]) -> None:
        """处理一帧 remote_control（boot/stop 会话切换 + 控制量应用 + 回执）。"""
        self._frames_seen += 1
        now = time.time()
        session_id = frame.get("session_id")
        if session_id:
            self._session_id = session_id

        control = frame.get("control")
        if isinstance(control, dict):
            self.model.apply_control(control, now=now, heartbeat=bool(frame.get("heartbeat", False)))

        video = frame.get("video")
        if isinstance(video, dict) and "recording" in video:
            if video.get("recording"):
                self.model.set_session(active=True)
                self._start_video(self._session_id)
                self._log("session_boot", session_id=self._session_id)
            else:
                self.model.set_session(active=False)
                self._stop_video()
                self._log("session_stop", session_id=self._session_id, reason=frame.get("reason"))

        if self.cfg.ack_control_frames:
            self._produce_ack(control if isinstance(control, dict) else {})

    def _produce_ack(self, control: dict[str, Any]) -> None:
        tv = control.get("target_velocity", 0.0)
        ts = control.get("target_steer", 0.0)
        result = build_command_result(
            self.cfg.vehicle_id,
            command_id=new_command_id(),
            success=True,
            result_code="0",
            message=f"applied v={tv} steer={ts}",
            data={"velocity": round(self.model.state.velocity, 3), "gear": self.model.state.gear},
        )
        self._safe_produce(self.cfg.command_result_topic(), self.cfg.vehicle_id, result)
        self._acks_sent += 1

    # ---------- 数据面：遥测 / 健康 ----------
    def _emit_telemetry(self) -> None:
        self._seq += 1
        msg = build_telemetry(
            self.cfg.vehicle_id,
            self.model.state,
            seq=self._seq,
            target_velocity=self.model.target.target_velocity,
            target_steer=self.model.target.target_steer,
        )
        self._safe_produce(self.cfg.telemetry_topic(), self.cfg.vehicle_id, msg)

    def _emit_health(self) -> None:
        msg = build_health(self.cfg.vehicle_id, self.model.state)
        self._safe_produce(self.cfg.health_topic(), self.cfg.vehicle_id, msg)

    def _safe_produce(self, topic: str, key: str, value: dict[str, Any]) -> None:
        try:
            self.transport.produce(topic, key, value)
        except Exception as exc:  # noqa: BLE001 - 单条投递失败不应中断模拟
            self._log("produce_failed", topic=topic, error=str(exc))

    # ---------- 视频会话联动 ----------
    def _start_video(self, session_id: str | None) -> None:
        if self.video is not None and session_id:
            self.video.start_session(session_id)

    def _stop_video(self) -> None:
        if self.video is not None:
            self.video.stop_session()

    # ---------- 主循环 ----------
    def run(self) -> None:
        """阻塞运行直至 ``stop()`` 或达到 ``duration_s``。"""
        cfg = self.cfg
        self.transport.subscribe()
        telemetry_interval = 1.0 / max(0.1, cfg.telemetry_hz)
        health_interval = 1.0 / max(0.1, cfg.health_hz)
        start = time.monotonic()
        last_step = start
        next_telemetry = start
        next_health = start
        deadline = start + cfg.duration_s if cfg.duration_s > 0 else None
        self._log(
            "simulator_started",
            vehicle_id=cfg.vehicle_id,
            bootstrap=cfg.bootstrap,
            telemetry_hz=cfg.telemetry_hz,
            video=bool(self.video),
        )
        try:
            while not self._stop.is_set():
                now = time.monotonic()
                if deadline is not None and now >= deadline:
                    break
                # 排空控制帧（非阻塞）
                while True:
                    frame = self.transport.poll(0.0)
                    if frame is None:
                        break
                    self.handle_frame(frame)
                # 运动学积分（真实 dt）
                dt = now - last_step
                last_step = now
                self.model.step(dt, now=time.time())
                # 遥测节拍
                if now >= next_telemetry:
                    self._emit_telemetry()
                    next_telemetry += telemetry_interval
                    if next_telemetry < now:
                        next_telemetry = now + telemetry_interval
                    self.transport.commit()
                # 健康节拍
                if now >= next_health:
                    self._emit_health()
                    next_health += health_interval
                    if next_health < now:
                        next_health = now + health_interval
                time.sleep(min(telemetry_interval, 0.02))
        finally:
            self._shutdown()
        self._log(
            "simulator_stopped",
            frames_seen=self._frames_seen,
            telemetry_seq=self._seq,
            acks_sent=self._acks_sent,
        )

    def stop(self) -> None:
        self._stop.set()

    def _shutdown(self) -> None:
        self._stop_video()
        flush = getattr(self.transport, "flush", None)
        if callable(flush):
            try:
                flush(2.0)
            except Exception as exc:  # noqa: BLE001 - 退出 flush 失败仅记录，不阻断
                self._log("transport_flush_failed", error=str(exc))


def _default_log(event: str, **fields: Any) -> None:
    print(f"[sim] {event} {fields}" if fields else f"[sim] {event}")
