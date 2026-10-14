"""合成视频推流（速度/转角叠加文本的动画帧 → SRS）。

主路径：ffmpeg 从 stdin 读 rawvideo（rgb24）→ libx264 零延迟编码 → RTMP 推流到
``rtmp://{ip}:1935/remote_control/hunter_{vid}_{sid}``（浏览器侧经 WHEP 拉流）。
回退路径（无 ffmpeg 时，可选 extra ``whip``）：aiortc 自定义 VideoStreamTrack 经 WHIP 推流。

依赖缺失（无 Pillow / 无 ffmpeg / 未装 aiortc）时不抛致命错误，仅禁用视频并告警，
保证「数据链路自测」在纯 Python 环境也可运行（``--no-video`` 可显式关闭）。
"""

from __future__ import annotations

import math
import shutil
import subprocess
import threading
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from vehicle_simulator.config import SimConfig
    from vehicle_simulator.kinematics import VehicleState

try:  # Pillow 为强依赖，但缺失时降级为无视频（不误杀整个模拟器进程）
    from PIL import Image, ImageDraw, ImageFont

    _HAS_PIL = True
except ImportError:  # pragma: no cover
    _HAS_PIL = False


class FrameSource:
    """合成帧生成器：滚动路面 + 速度/转角/挡位/SOC 文本叠加（读运动学状态）。"""

    def __init__(self, cfg: SimConfig, state_getter) -> None:
        self.cfg = cfg
        self._state_getter = state_getter
        self._phase = 0.0
        self._font = self._load_font(28)
        self._font_small = self._load_font(20)

    @staticmethod
    def _load_font(size: int):
        if not _HAS_PIL:
            return None
        for name in ("arial.ttf", "DejaVuSans.ttf"):
            try:
                return ImageFont.truetype(name, size)
            except OSError:
                continue
        return ImageFont.load_default()

    def render_image(self, t: float, *, session_id: str | None):
        """生成一帧 PIL Image（供 ffmpeg raw 管道与 aiortc WHIP 两路复用）。"""
        if not _HAS_PIL:  # pragma: no cover
            raise RuntimeError("Pillow 未安装，无法生成合成帧")
        state: VehicleState = self._state_getter()
        w, h = self.cfg.video_width, self.cfg.video_height
        img = Image.new("RGB", (w, h), (10, 18, 30))
        draw = ImageDraw.Draw(img)

        # 天空/路面分界 + 随速度滚动车道线（演示“画面在动”）
        horizon = int(h * 0.45)
        draw.rectangle([0, horizon, w, h], fill=(24, 40, 60))
        self._phase += state.velocity * 6.0
        dash = 60
        offset = int(self._phase) % (dash * 2)
        for x in range(-dash * 2, w + dash, dash * 2):
            cx = x - offset + w // 2
            draw.polygon([(cx, h), (cx + dash, h), (cx + dash // 2 + 40, horizon + 20), (cx + dash // 2 - 40, horizon + 20)], fill=(200, 210, 220))
        # 转角指示：车头方向线随 steering 摆动
        ang = -state.steer * 3.0
        draw.line([(w // 2, h - 10), (int(w // 2 + math.sin(ang) * 200), h - 220)], fill=(34, 211, 238), width=6)

        # 文本叠加
        speed_kmh = state.velocity * 3.6
        lines = [
            f"HUNTER SIM  vehicle={self.cfg.vehicle_id}",
            f"session={session_id or '-'}",
            f"speed={speed_kmh:5.1f} km/h   steer={state.steer:+.3f} rad",
            f"gear={state.gear}   soc={state.battery_soc}%   heading={state.heading:+.2f} rad",
        ]
        y = 16
        for i, line in enumerate(lines):
            draw.text((16, y), line, fill=(226, 232, 240), font=self._font if i == 0 else self._font_small)
            y += 34 if i == 0 else 28
        # 顶部录制标记
        rec = "REC ●" if session_id else "IDLE"
        rec_color = (248, 113, 113) if session_id else (148, 163, 184)
        draw.text((w - 120, 16), rec, fill=rec_color, font=self._font_small)
        return img

    def render(self, t: float, *, session_id: str | None) -> bytes:
        """生成一帧 RGB raw 字节（width*height*3），供 ffmpeg stdin 消费。"""
        return self.render_image(t, session_id=session_id).tobytes()


class VideoStreamer:
    """按会话（boot/stop 帧）起停的 RTMP 推流器。"""

    def __init__(self, cfg: SimConfig, state_getter, *, log=None) -> None:
        self.cfg = cfg
        self._state_getter = state_getter
        self._log = log or _default_log
        self._lock = threading.Lock()
        self._session_id: str | None = None
        self._proc: subprocess.Popen | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._enabled = cfg.video and _HAS_PIL and shutil.which("ffmpeg") is not None
        if cfg.video and not _HAS_PIL:
            self._log("video_disabled_pillow_missing")
        elif cfg.video and shutil.which("ffmpeg") is None:
            self._log("video_fallback_whip_no_ffmpeg")

    @property
    def ffmpeg_available(self) -> bool:
        return self._enabled

    def start_session(self, session_id: str) -> None:
        """boot（video.recording=true）→ 启动 RTMP 推流线程（幂等）。"""
        with self._lock:
            if self._session_id == session_id and self._proc is not None:
                return
            self._session_id = session_id
        if not self._enabled:
            return
        self._spawn_rtmp(session_id)

    def stop_session(self) -> None:
        """stop（video.recording=false）→ 停流并释放（幂等）。"""
        with self._lock:
            self._session_id = None
        self._shutdown_proc()

    def close(self) -> None:
        self._stop.set()
        self._shutdown_proc()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)

    def _spawn_rtmp(self, session_id: str) -> None:
        rtmp_url = self.cfg.rtmp_url_for(session_id)
        if not rtmp_url:
            self._log("video_no_rtmp_base", session_id=session_id)
            return
        self._shutdown_proc()
        cmd = [
            "ffmpeg", "-y", "-loglevel", "error",
            "-f", "rawvideo", "-pix_fmt", "rgb24",
            "-s", f"{self.cfg.video_width}x{self.cfg.video_height}",
            "-r", str(self.cfg.video_fps), "-i", "pipe:",
            "-an", "-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency",
            "-pix_fmt", "yuv420p", "-profile:v", "main",
            "-b:v", "2500k", "-maxrate", "2500k", "-bufsize", "5000k",
            "-g", str(self.cfg.video_fps),
            "-f", "flv", rtmp_url,
        ]
        try:
            proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stderr=subprocess.DEVNULL)
        except OSError as exc:  # pragma: no cover
            self._log("video_ffmpeg_spawn_failed", error=str(exc))
            return
        self._proc = proc
        self._stop.clear()
        self._thread = threading.Thread(target=self._pump, args=(session_id, proc), daemon=True)
        self._thread.start()
        self._log("video_rtmp_started", rtmp_url=rtmp_url)

    def _pump(self, session_id: str, proc: subprocess.Popen) -> None:
        src = FrameSource(self.cfg, self._state_getter)
        interval = 1.0 / self.cfg.video_fps
        next_at = time.monotonic()
        frame_interval_ms = int(1000 / self.cfg.video_fps)
        try:
            while not self._stop.is_set() and proc.poll() is None:
                with self._lock:
                    current = self._session_id
                if current != session_id or current is None:
                    break
                try:
                    frame = src.render(time.time(), session_id=current)
                    proc.stdin.write(frame)
                except (BrokenPipeError, OSError, ValueError):
                    break
                next_at += interval
                sleep = next_at - time.monotonic()
                if sleep > 0:
                    time.sleep(min(sleep, frame_interval_ms / 1000.0))
                else:
                    next_at = time.monotonic()
        finally:
            self._log("video_pump_exit", session_id=session_id)

    def _shutdown_proc(self) -> None:
        self._stop.set()
        proc = self._proc
        self._proc = None
        if proc is None:
            return
        try:
            if proc.stdin:
                proc.stdin.close()
        except OSError:
            pass
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:  # pragma: no cover
            proc.terminate()


def _default_log(event: str, **fields: Any) -> None:
    print(f"[video] {event} {fields}" if fields else f"[video] {event}")


__all__ = ["FrameSource", "VideoStreamer"]
