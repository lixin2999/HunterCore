"""录像封存链路单元测试（V1.21.16）。

覆盖：
- ``LocalDvrRecordingSource`` 路径拼装与「文件就绪（大小稳定）」读取语义；
- ``VideoSealer`` 上传对象键/字节数与失败降级（计入 ARCHIVE_FAILED_TOTAL{stage=video}）；
- 与 ``SessionService.end_session`` 的集成：真实字节数写入 sidecar（此前恒 0 的缺口）。
"""
from __future__ import annotations

from typing import Any

from hunter_common.metrics import REGISTRY

from app.config import settings
from app.repositories.storage import build_video_object_key
from app.schemas.common import SessionEndReason
from app.services.session_service import SESSION_HASH_KEY
from app.services.video_sealer import LocalDvrRecordingSource, VideoSealer
from app.tests.conftest import OPERATOR_ID, VEHICLE_ONLINE, FakeVideoStorage, make_operator
from app.tests.test_session_service import _seed_session

_MP4 = b"fake-mp4-bytes"


class FakeRecordingSource:
    """RecordingSource 替身：返回预置字节（None 表示未就绪/缺失）。"""

    def __init__(self, data: bytes | None) -> None:
        self.data = data
        self.calls: list[tuple[str, str]] = []

    async def fetch(
        self, vehicle_id: str, session_id: str, *, wait_s: float, poll_s: float
    ) -> bytes | None:
        self.calls.append((vehicle_id, session_id))
        return self.data


def _video_failure_count() -> float:
    """ARCHIVE_FAILED_TOTAL{stage=video} 当前值（未创建时视为 0）。"""
    value = REGISTRY.get_sample_value(
        "hunter_remote_control_archive_failed_total", {"stage": "video"}
    )
    return float(value or 0.0)


# ---------- LocalDvrRecordingSource ----------
def test_recording_path_composes_srs_stream_name() -> None:
    """路径约定：{dir}/{rc_srs_app}/{stream}.mp4，stream 与下发 URL 同源（含 session_id）。"""
    source = LocalDvrRecordingSource(
        "/data/srs-dvr", app="remote_control", stream_prefix="hunter"
    )
    path = source.recording_path("HUNTER-001", "sess-1")
    assert path is not None
    assert path.as_posix() == "/data/srs-dvr/remote_control/hunter_HUNTER-001_sess-1.mp4"


def test_recording_path_none_when_directory_unset() -> None:
    """未配置来源目录 → 无路径（生产侧据此禁用封存，保持旧行为）。"""
    source = LocalDvrRecordingSource("", app="remote_control", stream_prefix="hunter")
    assert source.recording_path("HUNTER-001", "sess-1") is None


async def test_local_source_reads_file_when_stable(tmp_path: Any) -> None:
    """文件就绪（存在且大小稳定）→ 返回整份字节。"""
    app_dir = tmp_path / "remote_control"
    app_dir.mkdir()
    (app_dir / "hunter_HUNTER-001_sess-1.mp4").write_bytes(_MP4)
    source = LocalDvrRecordingSource(
        str(tmp_path), app="remote_control", stream_prefix="hunter"
    )
    assert await source.fetch("HUNTER-001", "sess-1", wait_s=0.0, poll_s=0.01) == _MP4


async def test_local_source_returns_none_when_missing(tmp_path: Any) -> None:
    """文件缺失（SRS 未落盘）→ 等待超时返回 None，不抛异常。"""
    source = LocalDvrRecordingSource(
        str(tmp_path), app="remote_control", stream_prefix="hunter"
    )
    assert await source.fetch("HUNTER-001", "nope", wait_s=0.0, poll_s=0.01) is None


# ---------- VideoSealer ----------
async def test_seal_uploads_to_contract_key_and_returns_size() -> None:
    """封存：按契约对象键上传录像并返回字节数（历史查询 head_video 从此非 None）。"""
    storage = FakeVideoStorage()
    sealer = VideoSealer(
        storage=storage, source=FakeRecordingSource(_MP4), settings=settings
    )

    size = await sealer.seal(
        vehicle_id="HUNTER-001",
        session_id="sess-1",
        object_key=build_video_object_key("HUNTER-001", 1724035200.0, "sess-1"),
    )

    key = build_video_object_key("HUNTER-001", 1724035200.0, "sess-1")
    assert size == len(_MP4)
    assert storage.objects[key] == _MP4
    assert await storage.head_video(key) == len(_MP4)


async def test_seal_returns_none_and_counts_failure_when_not_ready() -> None:
    """未就绪/缺失：返回 None（调用方 size_bytes=0）+ 计入归档失败（stage=video）。"""
    storage = FakeVideoStorage()
    sealer = VideoSealer(
        storage=storage, source=FakeRecordingSource(None), settings=settings
    )
    before = _video_failure_count()

    assert (
        await sealer.seal(
            vehicle_id="HUNTER-001",
            session_id="sess-1",
            object_key=build_video_object_key("HUNTER-001", 1724035200.0, "sess-1"),
        )
        is None
    )
    assert storage.objects == {}
    assert _video_failure_count() == before + 1


# ---------- 与 end_session 的集成 ----------
async def test_end_session_seals_video_object_and_records_real_size(rc_env: Any) -> None:
    """结束会话：录像对象已上传 + sidecar 写入真实字节数（V1.21.16 缺口修复）。

    此前 sidecar 恒 ``size_bytes=0`` 且从未上传 mp4 ⇒「播放录像」恒 3001。
    """
    session_id = _seed_session(rc_env.redis, VEHICLE_ONLINE, OPERATOR_ID)
    rc_env.session_service._video_sealer = VideoSealer(
        storage=rc_env.storage, source=FakeRecordingSource(_MP4), settings=settings
    )

    result = await rc_env.session_service.end_session(session_id, make_operator())

    assert result.sidecar_written is True
    video_key = result.record.object_key
    assert rc_env.storage.objects[video_key] == _MP4
    doc = await rc_env.storage.read_sidecar(result.record.sidecar_object_key)
    assert doc is not None
    assert doc["video"]["size_bytes"] == len(_MP4)
    # 会话键仍按原流程清理
    assert not await rc_env.redis.hgetall(
        SESSION_HASH_KEY.format(vehicle_id=VEHICLE_ONLINE)
    )


async def test_end_session_without_recording_keeps_size_zero(rc_env: Any) -> None:
    """录像未就绪：不阻断结束，sidecar ``size_bytes=0``（历史查询据 head_video 判 3001）。"""
    session_id = _seed_session(rc_env.redis, VEHICLE_ONLINE, OPERATOR_ID)
    rc_env.session_service._video_sealer = VideoSealer(
        storage=rc_env.storage, source=FakeRecordingSource(None), settings=settings
    )

    result = await rc_env.session_service.end_session(
        session_id, make_operator(), SessionEndReason.OPERATOR_END
    )

    assert result.sidecar_written is True
    assert result.record.object_key not in rc_env.storage.objects
    doc = await rc_env.storage.read_sidecar(result.record.sidecar_object_key)
    assert doc is not None
    assert doc["video"]["size_bytes"] == 0
