"""HistoryService 单元测试（契约 x-hunter-history-archive：sidecar 投影 + 预签名访问）。"""

from __future__ import annotations

import json
from typing import Any
from uuid import uuid4

import pytest
from hunter_common.exceptions import InvalidParameterError, ResourceNotFoundError

from app.schemas.common import EndReasonSource, SessionEndReason
from app.schemas.history import (
    SessionSidecar,
    SidecarControl,
    SidecarEnvironment,
    SidecarVideo,
)
from app.services.history_service import HistoryService
from app.tests.conftest import OPERATOR_ID, OTHER_ID, VEHICLE_ONLINE

pytestmark = pytest.mark.asyncio

_BASE = 1_724_035_200.0  # 2024-08-19 00:00:00 UTC


def _doc(
    session_id: str,
    *,
    vehicle_id: str = VEHICLE_ONLINE,
    operator_id: str = OPERATOR_ID,
    started_at: float = _BASE,
    duration_s: int = 600,
    end_reason: SessionEndReason = SessionEndReason.OPERATOR_END,
) -> dict[str, Any]:
    """构造契约 sidecar 文档（SessionSidecar 模型自校验后 dump）。"""
    ended_at = started_at + duration_s
    return SessionSidecar(
        session_id=session_id,
        vehicle_id=vehicle_id,
        operator_id=operator_id,
        started_at=started_at,
        ended_at=ended_at,
        duration_s=duration_s,
        end_reason=end_reason,
        end_reason_source=EndReasonSource.PLATFORM,
        video=SidecarVideo(
            object_key=f"remote-control/{vehicle_id}/2024/08/19/{session_id}.mp4",
            size_bytes=2048,
            sha256="a" * 64,
            fps=30,
            bitrate_kbps_avg=4096,
            e2e_latency_ms_p95=150.0,
        ),
        control=SidecarControl(
            commands_sent=600,
            commands_acked=598,
            ack_latency_ms_avg=40.0,
            ack_latency_ms_p95=80.0,
            timeout_events=2,
            max_speed_mps=2.0,
        ),
        environment=SidecarEnvironment(service_version="dev", config_digest="x" * 12),
    ).model_dump(mode="json")


def _put(storage, doc: dict[str, Any]) -> str:
    """sidecar 写入内存桶（键 = remote-control/{vid}/2024/08/19/{sid}.json）。"""
    key = f"remote-control/{doc['vehicle_id']}/2024/08/19/{doc['session_id']}.json"
    storage.objects[key] = json.dumps(doc, ensure_ascii=False).encode("utf-8")
    return key


async def test_list_history_sorted_and_filtered(rc_env) -> None:
    """列表：started_at 降序 + operator/end_reason/时间范围过滤（契约 333-357 行）。"""
    service: HistoryService = rc_env.history_service
    storage = rc_env.storage
    _put(storage, _doc(str(uuid4()), started_at=_BASE))
    _put(storage, _doc(str(uuid4()), started_at=_BASE + 3600))
    _put(storage, _doc(str(uuid4()), started_at=_BASE + 7200, operator_id=OTHER_ID))
    _put(
        storage,
        _doc(
            str(uuid4()), started_at=_BASE + 10800, end_reason=SessionEndReason.ADMIN_TERMINATE
        ),
    )

    page = await service.list_history(page=1, page_size=10)
    assert page.total == 4
    started = [item.started_at for item in page.items]
    assert started == sorted(started, reverse=True)

    mine = await service.list_history(operator_id=OPERATOR_ID)
    assert mine.total == 3

    admin_ended = await service.list_history(end_reason=SessionEndReason.ADMIN_TERMINATE)
    assert admin_ended.total == 1

    ranged = await service.list_history(started_from=_BASE, started_to=_BASE + 7200)
    assert ranged.total == 3  # 上界含（契约 351 行「含」）


async def test_list_history_range_limit(rc_env) -> None:
    """started_from/started_to 跨度 > 31 天 → 2001（契约 322-323 行）。"""
    with pytest.raises(InvalidParameterError):
        await rc_env.history_service.list_history(
            started_from=_BASE, started_to=_BASE + 32 * 86400
        )


async def test_get_history_detail(rc_env) -> None:
    """详情：sidecar 原文 + video_available（录像对象存在性）。"""
    session_id = str(uuid4())
    doc = _doc(session_id)
    _put(rc_env.storage, doc)

    detail = await rc_env.history_service.get_history_detail(session_id)
    assert detail.session_id == session_id
    assert detail.sidecar == doc
    assert detail.video_available is False  # 未预置录像对象（未封存/已回收语义）

    rc_env.storage.objects[doc["video"]["object_key"]] = b"mp4-bytes"
    detail2 = await rc_env.history_service.get_history_detail(session_id)
    assert detail2.video_available is True


async def test_get_history_detail_missing(rc_env) -> None:
    """记录不存在 → 3001（契约 398 行 404）。"""
    with pytest.raises(ResourceNotFoundError):
        await rc_env.history_service.get_history_detail(str(uuid4()))


async def test_get_video_access(rc_env) -> None:
    """录像访问：预签名 900s + 大小；URL 不落日志（此处验证返回值语义）。"""
    session_id = str(uuid4())
    doc = _doc(session_id)
    _put(rc_env.storage, doc)
    rc_env.storage.objects[doc["video"]["object_key"]] = b"x" * 4096

    access = await rc_env.history_service.get_video_access(session_id)
    assert access.expires_in == 900  # 契约 Literal[900]（下载预签名 15 分钟）
    assert access.size_bytes == 4096
    assert access.range_supported is True
    assert access.video_url.startswith("https://minio.test/hunter-video/")


async def test_get_video_access_object_gone(rc_env) -> None:
    """录像对象被生命周期回收 → 3001（契约 412 行）。"""
    session_id = str(uuid4())
    _put(rc_env.storage, _doc(session_id))  # 仅 sidecar，无录像对象
    with pytest.raises(ResourceNotFoundError):
        await rc_env.history_service.get_video_access(session_id)


async def test_list_history_skips_corrupted_sidecar(rc_env) -> None:
    """脏 sidecar（缺字段）不阻断整页：跳过并继续（容错语义）。"""
    _put(rc_env.storage, _doc(str(uuid4())))
    rc_env.storage.objects["remote-control/HUNTER-001/2024/08/19/broken.json"] = (
        b"{}"  # 合法 JSON 但缺必填字段：由 service 层投影容错跳过
    )
    page = await rc_env.history_service.list_history()
    assert page.total == 1
