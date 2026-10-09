"""车辆台账回写单元测试（V1.18.11：车辆管理页「状态/最近在线」数据源）。

覆盖三条不可省略的约束：
- 节流：状态跃迁立即写、稳态按窗口合并（health 1Hz 不得变成 1 次 UPDATE/秒）；
- 职责边界：telemetry 只刷 ``last_online_time``，**绝不**改写 ``status``；
- 隔离：DB 异常只告警不上抛（消费失败会重试/进 DLQ，把"页面数字旧一点"放大成管道中断），
  且失败也计入节流窗口（否则 DB 故障期间每条消息都打一次库）。
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import pytest
from hunter_common.database.enums import VehicleStatus
from sqlalchemy.dialects import postgresql

from app.config import Settings
from app.repositories.vehicle_ledger import VehicleLedgerRepositoryAdapter
from app.services.vehicle_ledger import (
    MAX_TRACKED_VEHICLES,
    OFFLINE_STATUS_NAME,
    VehicleLedgerWriter,
)
from app.tests.fakes import FakeVehicleLedgerRepository


class FakeClock:
    """可控时钟（避免用例依赖真实时间，节流窗口判定需确定化）。"""

    def __init__(self, now: float = 1_700_000_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def make_writer(
    repository: FakeVehicleLedgerRepository,
    clock: FakeClock,
    **overrides: Any,
) -> VehicleLedgerWriter:
    kwargs: dict[str, Any] = {"vehicle_ledger_write_interval_seconds": 30}
    kwargs.update(overrides)
    return VehicleLedgerWriter(repository, Settings(**kwargs), clock=clock)


VEHICLE = "HUNTER-001"


async def test_status_transition_writes_immediately_and_steady_state_is_throttled() -> None:
    """首次上报立即写；同状态在窗口内合并；超窗口再写一次（保活）。"""
    repo = FakeVehicleLedgerRepository()
    clock = FakeClock()
    writer = make_writer(repo, clock)

    assert await writer.record_status(VEHICLE, "auto_driving") is True
    assert await writer.record_status(VEHICLE, "auto_driving") is False   # 稳态：10 条只写 1 条
    clock.advance(10)
    assert await writer.record_status(VEHICLE, "auto_driving") is False
    clock.advance(21)                                                     # 累计 31s > 30s
    assert await writer.record_status(VEHICLE, "auto_driving") is True

    assert [call[1] for call in repo.status_calls] == [
        VehicleStatus.AUTO_DRIVING,
        VehicleStatus.AUTO_DRIVING,
    ]


async def test_status_change_bypasses_throttle() -> None:
    """状态跃迁是事件，必须立即落库（页面不能等到节流窗口结束才变化）。"""
    repo = FakeVehicleLedgerRepository()
    writer = make_writer(repo, FakeClock())

    await writer.record_status(VEHICLE, "online_idle")
    assert await writer.record_status(VEHICLE, "charging") is True
    assert [call[1] for call in repo.status_calls] == [
        VehicleStatus.ONLINE_IDLE,
        VehicleStatus.CHARGING,
    ]


async def test_written_timestamp_is_utc_aware() -> None:
    """TIMESTAMPTZ 列要求 tz-aware（naive datetime 会被驱动按会话时区误读）。"""
    repo = FakeVehicleLedgerRepository()
    clock = FakeClock()
    writer = make_writer(repo, clock)

    await writer.record_status(VEHICLE, "fault")

    stamped = repo.status_calls[0][2]
    assert isinstance(stamped, datetime)
    assert stamped.tzinfo is not None
    assert stamped == datetime.fromtimestamp(clock.now, tz=UTC)


async def test_record_seen_touches_last_online_time_only() -> None:
    """遥测只证明「在上报」：只刷新最近在线时间，不得改写业务状态。"""
    repo = FakeVehicleLedgerRepository()
    writer = make_writer(repo, FakeClock())

    assert await writer.record_seen(VEHICLE) is True
    assert await writer.record_seen(VEHICLE) is False        # 窗口内节流
    assert repo.status_calls == []
    assert len(repo.touch_calls) == 1
    assert repo.touch_calls[0][0] == VEHICLE


async def test_mark_offline_writes_once_and_dedupes_repeated_sweeps() -> None:
    """Sweeper 每轮都会对陈旧车辆调用：首次立即写，已离线则不重复 UPDATE。"""
    repo = FakeVehicleLedgerRepository()
    clock = FakeClock()
    writer = make_writer(repo, clock)

    await writer.record_status(VEHICLE, "auto_driving")
    assert await writer.mark_offline(VEHICLE) is True
    assert await writer.mark_offline(VEHICLE) is False       # 同一轮之后的重复判定
    clock.advance(10_000)
    assert await writer.mark_offline(VEHICLE) is False       # 不节流，但按状态去重

    assert repo.status_calls[-1][1] == VehicleStatus.OFFLINE


async def test_health_offline_status_resumes_writing_after_status_returns() -> None:
    """离线后车端重新上线：状态从 offline 跃迁回来仍立即写（去重不粘连）。"""
    repo = FakeVehicleLedgerRepository()
    writer = make_writer(repo, FakeClock())

    await writer.mark_offline(VEHICLE)
    assert await writer.record_status(VEHICLE, "online_idle") is True


async def test_database_failure_is_swallowed_and_counts_in_throttle_window() -> None:
    """DB 故障：返回 False 且不上抛；失败仍计入窗口（避免故障期写风暴）。"""
    repo = FakeVehicleLedgerRepository(fail_with=RuntimeError("database unavailable"))
    clock = FakeClock()
    writer = make_writer(repo, clock)

    assert await writer.record_status(VEHICLE, "auto_driving") is False
    clock.advance(1)
    assert await writer.record_status(VEHICLE, "auto_driving") is False   # 被节流（未再打库）

    assert writer._status_writes[VEHICLE][1] == clock.now - 1.0   # 窗口锚在失败那次尝试
    assert repo.status_calls == []

    clock.advance(30)                                             # 超窗口后重试一次
    assert await writer.record_status(VEHICLE, "auto_driving") is False
    assert writer._status_writes[VEHICLE][1] == clock.now          # 失败同样刷新窗口


async def test_missing_ledger_row_does_not_raise() -> None:
    """台账无该行（未开通车辆）：仓储返回 False 而非异常，读模型仍正常。"""
    repo = FakeVehicleLedgerRepository()
    repo.known_vehicles = set()
    writer = make_writer(repo, FakeClock())

    assert await writer.record_seen(VEHICLE) is True    # 回写器不判定命中与否，只保证不冒泡


@pytest.mark.parametrize("value", ["not_a_status", "", "OFFLINE"])
async def test_unknown_status_value_is_not_written(value: str) -> None:
    """未知状态取值 = 契约/车端漂移：记 WARN 且不猜测映射（禁止写入非法枚举）。"""
    repo = FakeVehicleLedgerRepository()
    writer = make_writer(repo, FakeClock())

    assert await writer.record_status(VEHICLE, value) is False
    assert repo.status_calls == []


async def test_disabled_switch_short_circuits_every_path() -> None:
    """``VEHICLE_LEDGER_WRITE_ENABLED=false`` 时三条路径全部短路（不触碰 DB）。"""
    repo = FakeVehicleLedgerRepository()
    writer = make_writer(repo, FakeClock(), vehicle_ledger_write_enabled=False)

    assert await writer.record_status(VEHICLE, "auto_driving") is False
    assert await writer.record_seen(VEHICLE) is False
    assert await writer.mark_offline(VEHICLE) is False
    assert repo.status_calls == [] and repo.touch_calls == []


async def test_zero_interval_disables_throttling() -> None:
    """``INTERVAL=0`` = 不节流（本地调试/压测口径），每条都写。"""
    repo = FakeVehicleLedgerRepository()
    writer = make_writer(repo, FakeClock(), vehicle_ledger_write_interval_seconds=0)

    await writer.record_seen(VEHICLE)
    assert await writer.record_seen(VEHICLE) is True
    assert len(repo.touch_calls) == 2


async def test_tracking_tables_are_bounded() -> None:
    """vehicle_id 取自消息体（外部可控）：跟踪表 FIFO 淘汰，常驻内存有界。"""
    repo = FakeVehicleLedgerRepository()
    writer = make_writer(repo, FakeClock())

    for index in range(MAX_TRACKED_VEHICLES + 500):
        await writer.record_seen(f"VEHICLE-{index}")

    assert len(writer._seen_writes) == MAX_TRACKED_VEHICLES
    assert next(iter(writer._seen_writes)) == "VEHICLE-500"   # 最早写入的被淘汰


async def test_empty_vehicle_id_is_rejected() -> None:
    """空标识不写库（避免 UPDATE 落到无匹配条件）。"""
    repo = FakeVehicleLedgerRepository()
    writer = make_writer(repo, FakeClock())

    assert await writer.record_status("", "auto_driving") is False
    assert await writer.record_seen("") is False
    assert await writer.mark_offline("") is False
    assert repo.status_calls == [] and repo.touch_calls == []


def test_offline_status_name_matches_reader_model_constant() -> None:
    """离线常量与读模型同源（本模块不得 import vehicle_status，故以取值钉住）。"""
    from app.services.vehicle_status import OFFLINE_STATUS

    assert OFFLINE_STATUS_NAME == OFFLINE_STATUS == VehicleStatus.OFFLINE.value


# ---------------------------------------------------------------------------
# 会话适配层（装配错误的真正守护点）
# ---------------------------------------------------------------------------
class StubResult:
    def __init__(self, *, rowcount: int = 1) -> None:
        self.rowcount = rowcount

    def scalars(self) -> StubResult:
        return self

    def first(self) -> Any:
        return None


class StubSession:
    def __init__(self) -> None:
        self.statements: list[Any] = []
        self.committed = 0

    async def execute(self, statement: Any, params: Any = None) -> StubResult:
        self.statements.append(statement)
        return StubResult()


class StubSessionManager:
    """伪 ``DatabaseSessionManager``：记录借出了几个会话、是否各自提交。"""

    def __init__(self) -> None:
        self.sessions: list[StubSession] = []

    @asynccontextmanager
    async def session(self) -> Any:
        session = StubSession()
        self.sessions.append(session)
        yield session
        session.committed += 1


async def test_adapter_uses_one_short_transaction_per_writeback() -> None:
    """一次回写 = 一个短事务（``BaseRepository`` 只接 ``AsyncSession``、不 commit）。

    直接 ``VehicleRepository(会话管理器)`` 会让每次回写抛 ``AttributeError`` 并被
    异常隔离吞成 WARN（表现为“页面永远离线”），故以会话粒度钉住接线正确性。
    """
    manager = StubSessionManager()
    adapter = VehicleLedgerRepositoryAdapter(manager)  # type: ignore[arg-type]
    seen = datetime(2026, 9, 19, 8, 0, tzinfo=UTC)

    await adapter.update_status(VEHICLE, VehicleStatus.ONLINE_IDLE, last_online_time=seen)
    await adapter.touch_last_online_time(VEHICLE, seen_at=seen)

    assert len(manager.sessions) == 2, "禁止长期持有会话（消费循环不得跨消息复用连接）"
    assert [session.committed for session in manager.sessions] == [1, 1]
    statement = manager.sessions[0].statements[0]
    sql = " ".join(str(statement.compile(dialect=postgresql.dialect())).split())
    assert sql.startswith("UPDATE vehicle_svc.vehicles")
