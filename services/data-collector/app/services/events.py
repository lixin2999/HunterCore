"""事件查询/确认服务（契约：EventItem / acknowledge 幂等语义）。"""
from __future__ import annotations

import asyncio

from hunter_common.database.enums import EventLevel, EventType
from hunter_common.database.models import Event
from hunter_common.exceptions import ResourceNotFoundError

from app.repositories.events import EventRepository
from app.repositories.storage import MinioStorage
from app.schemas.events import EventItem, EventListData


class EventService:
    """事件业务逻辑（repository → 契约响应组装 + 下载 URL 即时签发）。"""

    def __init__(self, repository: EventRepository, storage: MinioStorage) -> None:
        self._repository = repository
        self._storage = storage

    async def list_events(
        self,
        vehicle_id: str | None = None,
        event_type: EventType | None = None,
        event_level: EventLevel | None = None,
        acknowledged: bool | None = None,
        start_time: float | None = None,
        end_time: float | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> EventListData:
        """事件列表（event_time DESC；total 为筛选条件下总数）。"""
        items, total = await self._repository.list_events(
            vehicle_id=vehicle_id,
            event_type=event_type,
            event_level=event_level,
            acknowledged=acknowledged,
            start_time=start_time,
            end_time=end_time,
            page=page,
            page_size=page_size,
        )
        return EventListData(
            items=[await self._to_item(event) for event in items],
            total=total,
            page=page,
            page_size=page_size,
        )

    async def get_event(self, event_id: int) -> EventItem:
        """事件详情（3001：事件不存在）。"""
        event = await self._repository.get_by_id(event_id)
        if event is None:
            raise ResourceNotFoundError(message=f"事件不存在（event_id={event_id}）")
        return await self._to_item(event)

    async def acknowledge_event(self, event_id: int, user_id: str) -> EventItem:
        """确认事件（幂等；3001：事件不存在）。

        已确认事件重复确认 → 原样返回（保留首次确认人与时间，审计优先）。
        """
        event = await self._repository.acknowledge(event_id, user_id=user_id)
        if event is None:
            raise ResourceNotFoundError(message=f"事件不存在（event_id={event_id}）")
        return await self._to_item(event)

    async def _to_item(self, event: Event) -> EventItem:
        """ORM 事件 → 契约 EventItem；关联文件即时签发 15 分钟预签名下载 URL。

        类型转换：ORM datetime（TIMESTAMPTZ）→ 契约 Unix epoch 秒 float；
        ORM UUID → 契约 str；event_type/event_level 为 StrEnumType 直接透传。
        """
        data_file_url = event.data_file_url
        download_url: str | None = None
        if data_file_url:
            bucket, _, key = data_file_url.partition("/")
            if bucket and key:
                # 同步 boto3 经 to_thread 包装，避免事件循环阻塞（异步优先原则）
                download_url = await asyncio.to_thread(
                    self._storage.presign_get, bucket, key
                )
        return EventItem(
            event_id=int(event.event_id),
            vehicle_id=str(event.vehicle_id),
            event_type=EventType(event.event_type.value),
            event_level=EventLevel(event.event_level.value),
            event_time=event.event_time.timestamp() if event.event_time else 0.0,
            description=event.description,
            data_json=dict(event.data_json or {}),
            data_file_url=data_file_url,
            data_file_download_url=download_url,
            acknowledged=bool(event.acknowledged),
            acknowledged_by=str(event.acknowledged_by) if event.acknowledged_by else None,
            acknowledge_time=(
                event.acknowledge_time.timestamp() if event.acknowledge_time else None
            ),
        )
