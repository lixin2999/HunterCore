"""CLI 入口：``python -m vehicle_simulator --vehicle-id HUNTER-001 ...``。

装配顺序：解析配置 → 建 Kafka 传输 → 选视频后端（ffmpeg RTMP 主路径 / aiortc WHIP 回退 /
无则禁用）→ 运行主循环（SIGINT 优雅退出）。
"""

from __future__ import annotations

import signal
import sys

from vehicle_simulator.config import SimConfig, from_args
from vehicle_simulator.kafka_client import ConfluentTransport
from vehicle_simulator.simulator import VehicleSimulator
from vehicle_simulator.video import VideoStreamer


def _pick_video(cfg: SimConfig, sim: VehicleSimulator):
    """优先 ffmpeg RTMP；无 ffmpeg 但装了 aiortc 且配了 --srs-whip → WHIP 回退；否则 None。"""
    if not cfg.video:
        return None
    streamer = VideoStreamer(cfg, sim.state_snapshot)
    if streamer.ffmpeg_available:
        return streamer
    from vehicle_simulator.whip import WhipPusher

    if WhipPusher.available() and cfg.srs_whip:
        pusher = WhipPusher(cfg, _FrameSourceForWhip(cfg, sim), lambda: sim.session_id)
        # 适配 VideoStreamer 的 start_session/stop_session/close 接口
        return _WhipAdapter(pusher)
    print("[sim] 视频不可用（无 ffmpeg 且无 aiortc[whip]），仅运行数据链路")
    return None


class _FrameSourceForWhip:
    def __init__(self, cfg: SimConfig, sim: VehicleSimulator) -> None:
        from vehicle_simulator.video import FrameSource

        self._src = FrameSource(cfg, sim.state_snapshot)

    def render_image(self, t: float, *, session_id):
        return self._src.render_image(t, session_id=session_id)


class _WhipAdapter:
    """把 WhipPusher 适配到 VideoStreamer 的 start/stop/close 接口。"""

    def __init__(self, pusher) -> None:
        self._pusher = pusher

    def start_session(self, session_id: str) -> None:
        self._pusher.start_session(session_id)

    def stop_session(self) -> None:
        self._pusher.stop_session()

    def close(self) -> None:
        self._pusher.stop_session()


def run(cfg: SimConfig) -> int:
    transport = ConfluentTransport(cfg)
    sim = VehicleSimulator(cfg, transport)
    sim.video = _pick_video(cfg, sim)

    def _on_sigint(signum, frame):
        print("\n[sim] 收到中断信号，正在优雅退出…")
        sim.stop()

    signal.signal(signal.SIGINT, _on_sigint)
    try:
        sim.run()
    finally:
        if sim.video is not None:
            sim.video.close()
        transport.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    cfg = from_args(argv)
    try:
        return run(cfg)
    except KeyboardInterrupt:
        return 130
    except Exception as exc:  # noqa: BLE001
        print(f"[sim] 致命错误：{exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
