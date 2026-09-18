"""SessionService 单元测试（契约 x-hunter-session-lifecycle 生命周期 + 预定义错误码）。"""

from __future__ import annotations

import time
from uuid import uuid4

import pytest
from hunter_common.exceptions import (
    AuthenticationError,
    PermissionDeniedError,
    RemoteControlSessionConflictError,
    ResourceNotFoundError,
    ServiceUnavailableError,
    VehicleBusyError,
    VehicleOfflineError,
)

from app.schemas.common import SessionEndReason, SessionStatus
from app.schemas.sessions import CreateRemoteSessionRequest
from app.services.session_service import (
    FIELD_SESSION_ID,
    FIELD_STARTED_AT,
    FIELD_STATUS,
    SESSION_HASH_KEY,
    SessionService,
)
from app.tests.conftest import (
    OPERATOR_ID,
    OTHER_ID,
    VEHICLE_BUSY,
    VEHICLE_OFFLINE,
    VEHICLE_ONLINE,
    FakeRedis,
    make_operator,
)

pytestmark = pytest.mark.asyncio

_HASH_FIELDS = {
    "session_id",
    "operator_id",
    "operator_name",
    "started_at",
    "status",
    "seq_last",
    "last_heartbeat_at",
    "commands_sent",
    "commands_acked",
    "video_object_key",
    "sidecar_object_key",
}


def _request(vehicle_id: str = VEHICLE_ONLINE) -> CreateRemoteSessionRequest:
    """构造创建请求（默认视频偏好，契约 CreateRemoteSessionRequest）。"""
    return CreateRemoteSessionRequest(vehicle_id=vehicle_id)


def _seed_session(redis: FakeRedis, vehicle_id: str, operator_id: str) -> str:
    """预置活跃会话 Hash（11 字段；started_at 60s 前，心跳刚刷）。"""
    session_id = str(uuid4())
    redis.store[SESSION_HASH_KEY.format(vehicle_id=vehicle_id)] = {
        FIELD_SESSION_ID: session_id,
        "operator_id": operator_id,
        "operator_name": "",
        FIELD_STARTED_AT: repr(time.time() - 60),
        FIELD_STATUS: SessionStatus.ACTIVE.value,
        "seq_last": "10",
        "last_heartbeat_at": repr(time.time()),
        "commands_sent": "10",
        "commands_acked": "9",
        "video_object_key": f"remote-control/{vehicle_id}/2024/08/19/{session_id}.mp4",
        "sidecar_object_key": f"remote-control/{vehicle_id}/2024/08/19/{session_id}.json",
    }
    return session_id


# ---------- 创建 ----------
async def test_create_session_success(rc_env) -> None:
    """创建成功：Hash 11 字段 + 车端信令 + boot 帧 + 契约默认链路参数。"""
    service: SessionService = rc_env.session_service
    info = await service.create_session(VEHICLE_ONLINE, make_operator(), _request())

    assert info.status == SessionStatus.CONNECTING
    assert info.operator_id == OPERATOR_ID
    mapping = await rc_env.redis.hgetall(
        SESSION_HASH_KEY.format(vehicle_id=VEHICLE_ONLINE)
    )
    assert set(mapping) == _HASH_FIELDS  # 契约固定 11 字段，不可增减
    assert mapping[FIELD_SESSION_ID] == info.session_id

    # 车端信令与 boot 帧各 1 条（先 session_start 后 boot）
    assert [kind for kind, _vid, _kw in rc_env.command.commands] == ["rc_session_start"]
    assert [kind for kind, _vid, _kw in rc_env.frame.frames] == ["boot"]

    # 契约链路参数：20Hz/500ms/2.0m/s/10s 心跳（系统约束第 15 条）
    assert info.control_channel.hz == 20
    assert info.control_channel.stop_on_timeout_ms == 500
    assert info.control_channel.max_speed_mps == 2.0
    assert info.heartbeat.interval_s == 10
    assert info.video.width == 1280 and info.video.height == 720
    assert info.webrtc.publisher == "vehicle" and info.webrtc.transport == "srtp"
    assert info.record.bucket == "hunter-video" and info.record.retention_days == 90


async def test_create_session_conflict_when_already_controlled(rc_env) -> None:
    """已有活跃会话 → 7001（锁内二次判定；契约 169 行）。"""
    _seed_session(rc_env.redis, VEHICLE_ONLINE, OTHER_ID)
    with pytest.raises(RemoteControlSessionConflictError):
        await rc_env.session_service.create_session(
            VEHICLE_ONLINE, make_operator(), _request()
        )


async def test_create_session_vehicle_offline(rc_env) -> None:
    """离线车辆 → 4001（契约 77 行 block_reason=offline）。"""
    with pytest.raises(VehicleOfflineError):
        await rc_env.session_service.create_session(
            VEHICLE_OFFLINE, make_operator(), _request()
        )


async def test_create_session_vehicle_busy(rc_env) -> None:
    """OTA 升级中 → 4002（契约 77 行 block_reason=upgrading）。"""
    with pytest.raises(VehicleBusyError):
        await rc_env.session_service.create_session(
            VEHICLE_BUSY, make_operator(), _request()
        )


async def test_create_session_invalid_operator(rc_env) -> None:
    """网关注入身份非法（非 UUID）→ 1001（认证失败）。"""
    with pytest.raises(AuthenticationError):
        await rc_env.session_service.create_session(
            VEHICLE_ONLINE, make_operator(user_id="not-a-uuid"), _request()
        )


async def test_create_session_kafka_failure_rolls_back(rc_env) -> None:
    """boot 帧投递失败 → 5001 + 回滚 Hash（会话未建立不留脏状态）。"""
    rc_env.frame.fail = True
    with pytest.raises(ServiceUnavailableError):
        await rc_env.session_service.create_session(
            VEHICLE_ONLINE, make_operator(), _request()
        )
    assert not await rc_env.redis.hgetall(
        SESSION_HASH_KEY.format(vehicle_id=VEHICLE_ONLINE)
    )


# ---------- 查询 ----------
async def test_get_session_owner_detail(rc_env) -> None:
    """归属者查询：返回实时统计块（Hash 汇总）；include_stats=False 时统计为 None。"""
    session_id = _seed_session(rc_env.redis, VEHICLE_ONLINE, OPERATOR_ID)
    detail = await rc_env.session_service.get_session(session_id, make_operator())
    assert detail.session_id == session_id
    assert detail.control_stats is not None
    assert detail.control_stats.commands_sent == 10
    assert detail.control_stats.last_seq == 10

    brief = await rc_env.session_service.get_session(
        session_id, make_operator(), include_stats=False
    )
    assert brief.control_stats is None and brief.degraded is False


async def test_get_session_foreign_hidden(rc_env) -> None:
    """非归属者（且非 admin）查询 → 3001（不泄漏会话存在性；契约 240 行）。"""
    session_id = _seed_session(rc_env.redis, VEHICLE_ONLINE, OPERATOR_ID)
    with pytest.raises(ResourceNotFoundError):
        await rc_env.session_service.get_session(
            session_id, make_operator(user_id=OTHER_ID)
        )


async def test_get_session_admin_visible(rc_env) -> None:
    """admin 跨操作员可见（契约 278 行数据权限）。"""
    session_id = _seed_session(rc_env.redis, VEHICLE_ONLINE, OPERATOR_ID)
    detail = await rc_env.session_service.get_session(
        session_id, make_operator(roles=("admin",))
    )
    assert detail.session_id == session_id


# ---------- 列表 ----------
async def test_list_sessions_data_permission(rc_env) -> None:
    """普通用户仅见自身会话；admin 全量 + operator_id 过滤（契约 139 行）。"""
    _seed_session(rc_env.redis, VEHICLE_ONLINE, OPERATOR_ID)
    _seed_session(rc_env.redis, VEHICLE_BUSY, OTHER_ID)

    own = await rc_env.session_service.list_active_sessions(make_operator())
    assert own.total == 1 and own.items[0].operator_id == OPERATOR_ID

    admin_all = await rc_env.session_service.list_active_sessions(
        make_operator(roles=("admin",))
    )
    assert admin_all.total == 2
    assert [item.started_at for item in admin_all.items] == sorted(
        (item.started_at for item in admin_all.items), reverse=True
    )

    admin_filtered = await rc_env.session_service.list_active_sessions(
        make_operator(roles=("admin",)), operator_id=OTHER_ID
    )
    assert admin_filtered.total == 1 and admin_filtered.items[0].operator_id == OTHER_ID


# ---------- 结束 ----------
async def test_end_session_success(rc_env) -> None:
    """结束成功：先车端释放（stop + session_end）后清理（删 Hash）+ sidecar 归档。"""
    session_id = _seed_session(rc_env.redis, VEHICLE_ONLINE, OPERATOR_ID)
    result = await rc_env.session_service.end_session(
        session_id, make_operator(), SessionEndReason.OPERATOR_END
    )

    assert result.status == SessionStatus.ENDED
    assert result.end_reason == SessionEndReason.OPERATOR_END
    assert result.duration_s >= 60
    assert result.sidecar_written is True
    assert result.record.object_key.endswith(".mp4")
    assert result.control_stats is not None and result.control_stats.commands_acked == 9

    # 会话键已删除（幂等依据；契约 280 行）
    assert not await rc_env.redis.hgetall(
        SESSION_HASH_KEY.format(vehicle_id=VEHICLE_ONLINE)
    )
    # 释放信令顺序：stop 帧与 session_end 均已投递（先安全后清理）
    assert [kind for kind, _v, _kw in rc_env.frame.frames] == ["stop"]
    assert [kind for kind, _v, _kw in rc_env.command.commands] == ["rc_session_end"]
    # sidecar 已归档到 hunter-video（对象键 = Hash 计划位置）
    sidecar_keys = [key for key in rc_env.storage.objects if key.endswith(".json")]
    assert (
        len(sidecar_keys) == 1 and result.record.sidecar_object_key == sidecar_keys[0]
    )


async def test_end_session_missing_idempotent(rc_env) -> None:
    """会话已结束（键不存在）→ 3001，不重复下发车端信令（契约 280 行幂等）。"""
    with pytest.raises(ResourceNotFoundError):
        await rc_env.session_service.end_session(str(uuid4()), make_operator())
    assert rc_env.frame.frames == [] and rc_env.command.commands == []


async def test_end_session_foreign_forbidden(rc_env) -> None:
    """非归属操作员结束他人会话 → 1002（契约 279 行）；admin 可结束。"""
    session_id = _seed_session(rc_env.redis, VEHICLE_ONLINE, OPERATOR_ID)
    with pytest.raises(PermissionDeniedError):
        await rc_env.session_service.end_session(
            session_id, make_operator(user_id=OTHER_ID)
        )
    # admin 终止（admin_terminate 语义）成功
    result = await rc_env.session_service.end_session(
        session_id, make_operator(roles=("admin",)), SessionEndReason.ADMIN_TERMINATE
    )
    assert result.end_reason == SessionEndReason.ADMIN_TERMINATE
    assert result.sidecar_written is True


async def test_end_session_sidecar_failure_flagged(rc_env) -> None:
    """sidecar 归档失败不阻断结束响应 → sidecar_written=false（人工核查）。"""
    session_id = _seed_session(rc_env.redis, VEHICLE_ONLINE, OPERATOR_ID)
    rc_env.storage.fail_write = True
    result = await rc_env.session_service.end_session(session_id, make_operator())
    assert result.status == SessionStatus.ENDED
    assert result.sidecar_written is False
    assert not await rc_env.redis.hgetall(
        SESSION_HASH_KEY.format(vehicle_id=VEHICLE_ONLINE)
    )
