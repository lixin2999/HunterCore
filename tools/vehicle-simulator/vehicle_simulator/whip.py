"""aiortc WHIP 推流回退（无 ffmpeg 时的可选路径，依赖 extra ``whip``）。

WHIP（WebRTC-HTTP ingestion protocol）：车端生成 offer SDP → POST 到
``{whip_base}?app=remote_control&stream=hunter_{vid}_{sid}`` → 取回 answer 设置远端描述，
媒体经 SRS 落地供浏览器 WHEP 拉流。帧源复用 video.FrameSource（合成动画 + 叠加文本）。

未安装 aiortc 时本模块整体不可用（``available()`` 返回 False），由调用方降级为无视频。
"""

from __future__ import annotations

import asyncio
import threading
import time
from typing import TYPE_CHECKING, Any

try:
    from aiortc import RTCPeerConnection, RTCSessionDescription, VideoStreamTrack

    _HAS_AIORTC = True
except ImportError:  # pragma: no cover
    _HAS_AIORTC = False

if TYPE_CHECKING:
    from vehicle_simulator.config import SimConfig
    from vehicle_simulator.video import FrameSource

try:
    import av
except ImportError:  # pragma: no cover
    av = None


class SimVideoTrack(VideoStreamTrack if _HAS_AIORTC else object):  # type: ignore[misc,valid-type]
    """自定义视频轨：按 fps 从 FrameSource 取帧转 av.VideoFrame。"""

    def __init__(self, frame_source: FrameSource, fps: int, session_holder) -> None:
        super().__init__()
        self._src = frame_source
        self._interval = 1.0 / max(1, fps)
        self._session_holder = session_holder

    async def recv(self):  # pragma: no cover - 依赖 aiortc 运行时
        frame = self._src.render_image(time.time(), session_id=self._session_holder())
        video_frame = av.VideoFrame.from_image(frame)
        return video_frame


class WhipPusher:
    """WHIP 推流封装（独立事件循环线程，随会话起停）。"""

    def __init__(self, cfg: SimConfig, frame_source, session_getter, *, log=None) -> None:
        self.cfg = cfg
        self._src = frame_source
        self._session_getter = session_getter
        self._log = log or (lambda event, **f: print(f"[whip] {event} {f}" if f else f"[whip] {event}"))
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: Any = None
        self._pc: Any = None
        self._whip_url: str | None = None

    @staticmethod
    def available() -> bool:
        return _HAS_AIORTC and av is not None

    async def _connect(self, session_id: str) -> None:  # pragma: no cover - 需 aiortc + SRS
        import aiohttp

        whip_url = self.cfg.whip_url_for(session_id)
        if not whip_url:
            self._log("whip_no_base", session_id=session_id)
            return
        pc = RTCPeerConnection()
        pc.addTrack(SimVideoTrack(self._src, self.cfg.video_fps, self._session_getter))

        @pc.on("connectionstatechange")
        async def _on_state() -> None:
            self._log("whip_state", state=pc.connectionState)
            if pc.connectionState in ("failed", "closed"):
                await pc.close()

        offer = await pc.createOffer()
        await pc.setLocalDescription(offer)
        async with aiohttp.ClientSession() as session, session.post(
            whip_url,
            data=pc.localDescription.sdp,
            headers={"Content-Type": "application/sdp"},
        ) as resp:
            resp.raise_for_status()
            answer_sdp = await resp.text()
        await pc.setRemoteDescription(RTCSessionDescription(sdp=answer_sdp, type="answer"))
        self._pc = pc
        self._whip_url = whip_url
        self._log("whip_started", whip_url=whip_url)

    def start_session(self, session_id: str) -> None:  # pragma: no cover - 需 aiortc
        if not self.available():
            return
        self._ensure_loop()
        asyncio.run_coroutine_threadsafe(self._connect(session_id), self._loop)

    def stop_session(self) -> None:  # pragma: no cover - 需 aiortc
        if self._loop and self._pc is not None:
            pc, self._pc = self._pc, None
            asyncio.run_coroutine_threadsafe(pc.close(), self._loop)

    def _ensure_loop(self) -> None:  # pragma: no cover - 需 aiortc
        if self._loop is not None:
            return
        self._loop = asyncio.new_event_loop()
        self._thread = _LoopThread(self._loop)
        self._thread.start()


class _LoopThread:  # pragma: no cover - 需 aiortc
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._t = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def start(self) -> None:
        self._t.start()


__all__ = ["SimVideoTrack", "WhipPusher"]
