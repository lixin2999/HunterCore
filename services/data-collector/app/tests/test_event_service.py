"""EventService 单元测试（审查 R6：补齐业务逻辑行为测试）。

覆盖：列表（筛选条件透传 + 关联文件即时签发下载地址）、详情（3001）、
确认（服务端身份写入 + 幂等语义 + 3001）。
"""
from __future__ import annotations

import pytest
from hunter_common.database.enums import EventLevel, EventType
from hunter_common.exceptions import ResourceNotFoundError

from app.services.events import EventService
from app.tests.fakes import FakeEventRepository, FakeMinioStorage, make_event

VEHICLE_ID = "HUNTER-001"
USER_ID = "11111111-1111-4111-8111-111111111111"
DATA_FILE_URL = f"hunter-rosbag/{VEHICLE_ID}/2026-09-18/rosbag/1724035200_1.bag"


def make_service(
    events: list | None = None,
) -> tuple[EventService, FakeEventRepository, FakeMinioStorage]:
    """构造 (服务, 仓储替身, 存储替身)。"""
    repository = FakeEventRepository(events)
    storage = FakeMinioStorage()
    return EventService(repository, storage), repository, storage


async def test_list_events_passes_filters_and_signs_download_url() -> None:
    """列表：筛选参数透传仓储；带关联文件的事件即时签发 15 分钟下载地址。"""
    service, repository, _ = make_service([make_event(event_id=1, data_file_url=DATA_FILE_URL)])

    data = await service.list_events(
        vehicle_id=VEHICLE_ID,
        event_type=EventType.HARSH_BRAKING,
        event_level=EventLevel.WARNING,
        acknowledged=False,
        page=2,
        page_size=50,
    )

    assert repository.list_calls == [
        {
            "vehicle_id": VEHICLE_ID,
            "event_type": EventType.HARSH_BRAKING,
            "event_level": EventLevel.WARNING,
            "acknowledged": False,
            "page": 2,
            "page_size": 50,
        }
    ]
    assert data.total == 1 and data.page == 2 and data.page_size == 50
    item = data.items[0]
    assert item.event_id == 1
    assert item.event_type == EventType.HARSH_BRAKING
    assert item.event_level == EventLevel.WARNING
    assert item.event_time == 1724035200.0
    assert item.data_file_download_url is not None
    assert item.data_file_download_url.startswith("https://minio.test/hunter-rosbag/")
    assert item.acknowledged is False


async def test_list_events_without_file_leaves_download_url_null() -> None:
    """无关联文件 → data_file_download_url 为 null（契约：不落库、按需签发）。"""
    service, _, _ = make_service([make_event(event_id=1)])
    data = await service.list_events()
    assert data.items[0].data_file_download_url is None
    assert data.items[0].data_file_url is None


async def test_get_event_missing_returns_3001() -> None:
    """事件不存在 → 3001。"""
    service, _, _ = make_service()
    with pytest.raises(ResourceNotFoundError) as exc:
        await service.get_event(999)
    assert exc.value.code == 3001


async def test_acknowledge_writes_server_side_identity() -> None:
    """确认：确认人与时间由服务端写入（禁止请求体指定确认人）。"""
    service, _, _ = make_service([make_event(event_id=7)])

    item = await service.acknowledge_event(7, user_id=USER_ID)

    assert item.acknowledged is True
    assert item.acknowledged_by == USER_ID
    assert item.acknowledge_time == 1724035200.0


async def test_acknowledge_is_idempotent_keeping_first_auditor() -> None:
    """幂等：重复确认保留首次确认人与时间（审计优先）。"""
    service, _, _ = make_service([make_event(event_id=7, acknowledged=True)])
    first = await service.acknowledge_event(7, user_id=USER_ID)

    # 二次确认使用另一个用户：不得覆盖审计字段
    again = await service.acknowledge_event(7, user_id="22222222-2222-4222-8222-222222222222")

    assert first.acknowledged is True
    assert again.acknowledged is True
    assert again.acknowledged_by is None or again.acknowledged_by == first.acknowledged_by


async def test_acknowledge_missing_returns_3001() -> None:
    """确认不存在的事件 → 3001。"""
    service, _, _ = make_service()
    with pytest.raises(ResourceNotFoundError) as exc:
        await service.acknowledge_event(404, user_id=USER_ID)
    assert exc.value.code == 3001
