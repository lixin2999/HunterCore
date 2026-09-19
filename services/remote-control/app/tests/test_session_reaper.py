"""陈旧操控会话守护与硬 TTL 单元测试（审查 R7）。

缺陷背景：``rc:session:{vehicle_id}`` 无 TTL，唯一清理路径是 ``DELETE /session/{id}``；
副本崩溃/客户端掉线时残留会话永久占用车辆互斥位（同车接管恒 7001）。
修复：①创建会话写入硬 TTL；②守护任务按心跳超阈值强制结束（走完整"先安全后清理"）。
"""
from __future__ import annotations

import time

from app.config import Settings
from app.schemas.common import SessionEndReason
from app.services.session_reaper import SESSION_SCAN_PATTERN, SessionReaper
from app.tests.conftest import FakeRedis


class RecordingSessionService:
    """会话服务替身：记录被强制结束的会话（不触碰真实信令/归档链路）。"""

    def __init__(self, *, close_succeeds: bool = True) -> None:
        self.closed: list[tuple[str, SessionEndReason]] = []
        self._close_succeeds = close_succeeds

    async def close_stale_session(self, session_id: str, *, reason: SessionEndReason) -> bool:
        self.closed.append((session_id, reason))
        return self._close_succeeds


def make_settings(**overrides: object) -> Settings:
    kwargs: dict[str, object] = {
        "rc_heartbeat_end_after_s": 60,
        "rc_session_reaper_interval_s": 1,
    }
    kwargs.update(overrides)
    return Settings(**kwargs)


async def test_reaper_closes_session_without_recent_heartbeat() -> None:
    """无心跳超阈值 → 强制结束（reason=heartbeat_timeout）。"""
    redis = FakeRedis()
    now = time.time()
    await redis.hset(
        "rc:session:HUNTER-001",
        mapping={
            "session_id": "s-1",
            "operator_id": "u-1",
            "status": "active",
            "started_at": str(now - 600),
            "last_heartbeat_at": str(now - 120),
        },
    )
    sessions = RecordingSessionService()
    reaper = SessionReaper(redis=redis, session_service=sessions, settings=make_settings())

    ended = await reaper.run_once(now=now)

    assert ended == ["s-1"]
    assert sessions.closed == [("s-1", SessionEndReason.HEARTBEAT_TIMEOUT)]


async def test_reaper_keeps_live_session_and_falls_back_to_started_at() -> None:
    """心跳新鲜 → 保留；无 last_heartbeat_at 时以 started_at 为基准判定。"""
    redis = FakeRedis()
    now = time.time()
    await redis.hset(
        "rc:session:HUNTER-002",
        mapping={"session_id": "s-live", "last_heartbeat_at": str(now - 5)},
    )
    await redis.hset(
        "rc:session:HUNTER-003",
        mapping={"session_id": "s-stale", "started_at": str(now - 300)},
    )
    sessions = RecordingSessionService()
    reaper = SessionReaper(redis=redis, session_service=sessions, settings=make_settings())

    ended = await reaper.run_once(now=now)

    assert ended == ["s-stale"]
    assert [session_id for session_id, _ in sessions.closed] == ["s-stale"]


async def test_reaper_removes_half_written_key_without_session_id() -> None:
    """Hash 缺 session_id（半写状态）→ 直接清理键，避免长期占用车辆互斥位。"""
    redis = FakeRedis()
    await redis.hset("rc:session:HUNTER-004", mapping={"status": "connecting"})
    sessions = RecordingSessionService()
    reaper = SessionReaper(redis=redis, session_service=sessions, settings=make_settings())

    ended = await reaper.run_once(now=time.time())

    assert ended == []
    assert sessions.closed == []
    assert await redis.hgetall("rc:session:HUNTER-004") == {}


async def test_reaper_skips_already_closed_session() -> None:
    """会话已被操作员结束（close 返回 False）→ 不计入结束清单（幂等）。"""
    redis = FakeRedis()
    now = time.time()
    await redis.hset(
        "rc:session:HUNTER-005",
        mapping={"session_id": "s-gone", "last_heartbeat_at": str(now - 600)},
    )
    sessions = RecordingSessionService(close_succeeds=False)
    reaper = SessionReaper(redis=redis, session_service=sessions, settings=make_settings())

    assert await reaper.run_once(now=now) == []
    assert sessions.closed == [("s-gone", SessionEndReason.HEARTBEAT_TIMEOUT)]


def test_session_scan_pattern_matches_contract_key() -> None:
    """扫描模式必须落在契约键模式 ``rc:session:{vehicle_id}`` 上（禁止其他拼法）。"""
    assert SESSION_SCAN_PATTERN == "rc:session:*"


async def test_reaper_start_stop_leaks_no_task() -> None:
    """守护任务可重复启停且无任务泄漏。"""
    reaper = SessionReaper(
        redis=FakeRedis(),
        session_service=RecordingSessionService(),
        settings=make_settings(rc_session_reaper_interval_s=1),
    )

    await reaper.start()
    await reaper.start()          # 幂等
    assert reaper._task is not None
    await reaper.stop()
    assert reaper._task is None and reaper._running is False
