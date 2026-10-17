"""录像封存（SRS DVR 产物 → MinIO hunter-video；契约 x-hunter-history-archive）。

契约依据（``contracts/openapi/remote-control.yaml`` → ``x-hunter-history-archive``）：
- ``upload``：「SRS 录制产物由服务端**直传 MinIO**（不经浏览器）；预签名上传仅用于异常人工补录」；
- ``sealing`` 顺序：「录像对象 PutObject 成功 → sidecar JSON PutObject 成功 → 才释放锁」；
- 录像对象键 ``remote-control/{vehicle_id}/{yyyy}/{mm}/{dd}/{session_id}.mp4``。

V1.21.16 补齐的缺口：此前只写 sidecar（``size_bytes`` 恒 0）而**从未上传 mp4**，
``hunter-video`` 里根本没有录像对象 ⇒ ``head_video`` 为 None ⇒ 「播放录像」恒 3001
（「不存在（未封存或已过保留期）」）。本模块把「会话结束 → SRS DVR 落盘 → 上传录像对象」
这一环补上。

实现要点：
- 录制产物来源 = SRS DVR 输出目录（``RC_VIDEO_SOURCE_DIR``，与 SRS 容器共享挂载），
  路径 ``{dir}/{rc_srs_app}/{stream}.mp4``，``stream = {prefix}_{vehicle_id}_{session_id}``；
- 会话结束到文件最终落地之间存在延迟（车端停推 → SRS unpublish → MP4 封装完成），
  在 ``RC_VIDEO_SEAL_WAIT_S`` 内轮询「文件出现且大小连续两次一致」后再读整份上传；
- 超时 / 文件缺失 / 上传失败 → 返回 None（调用方写 sidecar 时 ``size_bytes=0``，
  历史查询以 ``head_video`` 为准 → 播放时 3001），并计入 ARCHIVE_FAILED_TOTAL{stage=video}。
"""
from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Protocol

from hunter_common.logging import get_logger

from app.config import Settings
from app.repositories.storage import VideoArchiveStorage
from app.services.metrics import ARCHIVE_FAILED_TOTAL

logger = get_logger("app.services.video_sealer")


class RecordingSource(Protocol):
    """录像产物来源协议（本地共享目录实现见 LocalDvrRecordingSource）。"""

    async def fetch(
        self, vehicle_id: str, session_id: str, *, wait_s: float, poll_s: float
    ) -> bytes | None:
        """取本会话录像整份字节；等待超时或文件不存在返回 None。"""
        ...


class LocalDvrRecordingSource:
    """SRS DVR 共享目录来源（SRS 与 remote-control 容器共享挂载同一宿主目录）。"""

    def __init__(
        self, directory: str, *, app: str, stream_prefix: str, container: str = "mp4"
    ) -> None:
        self._dir = Path(directory) if directory else None
        self._app = app
        self._stream_prefix = stream_prefix
        self._container = container

    def recording_path(self, vehicle_id: str, session_id: str) -> Path | None:
        """SRS DVR 产物路径（未配置目录返回 None）。"""
        if self._dir is None:
            return None
        stream = f"{self._stream_prefix}_{vehicle_id}_{session_id}"
        return self._dir / self._app / f"{stream}.{self._container}"

    async def fetch(
        self, vehicle_id: str, session_id: str, *, wait_s: float, poll_s: float
    ) -> bytes | None:
        path = self.recording_path(vehicle_id, session_id)
        if path is None:
            return None
        return await asyncio.to_thread(self._read_when_stable, path, wait_s, poll_s)

    @staticmethod
    def _read_when_stable(path: Path, wait_s: float, poll_s: float) -> bytes | None:
        """轮询等待文件出现且大小连续两次一致（封装完成）后读取整份内容。

        等待下限取 ``poll_s``（至少给一次稳定性确认），避免 ``wait_s=0`` 时永远读不到已就绪文件。
        """
        poll = max(0.05, poll_s)
        deadline = time.monotonic() + max(poll, max(0.0, wait_s))
        previous = -1
        while True:
            try:
                size = path.stat().st_size if path.exists() else -1
            except OSError:
                size = -1
            if size > 0 and size == previous:
                try:
                    return path.read_bytes()
                except OSError:
                    return None
            previous = size
            if time.monotonic() >= deadline:
                return None
            time.sleep(poll)


class VideoSealer:
    """把本会话录像封存到 MinIO hunter-video（返回上传字节数；不可用返回 None）。"""

    def __init__(
        self, *, storage: VideoArchiveStorage, source: RecordingSource, settings: Settings
    ) -> None:
        self._storage = storage
        self._source = source
        self._settings = settings

    async def seal(self, *, vehicle_id: str, session_id: str, object_key: str) -> int | None:
        """上传录像对象到 ``object_key``；成功返回字节数，未就绪/失败返回 None（调用方降级 size_bytes=0）。

        ``object_key`` 由创建会话时算定的 Hash 字段给出（与 sidecar.video.object_key 同源），
        避免封存与 sidecar 指向不同键。
        """
        try:
            data = await self._source.fetch(
                vehicle_id,
                session_id,
                wait_s=float(self._settings.rc_video_seal_wait_s),
                poll_s=self._settings.rc_video_seal_poll_s,
            )
        except Exception:
            logger.exception(
                "video_seal_source_failed", vehicle_id=vehicle_id, session_id=session_id
            )
            ARCHIVE_FAILED_TOTAL.labels(stage="video").inc()
            return None
        if not data:
            logger.warning(
                "video_seal_not_ready", vehicle_id=vehicle_id, session_id=session_id
            )
            ARCHIVE_FAILED_TOTAL.labels(stage="video").inc()
            return None
        try:
            await self._storage.put_video(object_key, data)
        except Exception:
            logger.exception(
                "video_seal_upload_failed", vehicle_id=vehicle_id, session_id=session_id
            )
            ARCHIVE_FAILED_TOTAL.labels(stage="video").inc()
            return None
        logger.info(
            "video_sealed",
            vehicle_id=vehicle_id,
            session_id=session_id,
            object_key=object_key,
            size_bytes=len(data),
        )
        return len(data)


__all__ = ["LocalDvrRecordingSource", "RecordingSource", "VideoSealer"]
