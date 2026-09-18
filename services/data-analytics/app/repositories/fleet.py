"""车队状态仓库：Redis 只读聚合（vehicle:status:{vehicle_id} Hash + vehicle:online:set）。

Key 命名遵循设计文档 Redis Key 设计约束，本服务只读不写。
"""
from __future__ import annotations

from typing import Protocol

from hunter_common.logging import get_logger
from redis.asyncio import Redis

logger = get_logger("app.repositories.fleet")

#: 车辆状态受控词表（设计文档：车辆状态定义 8 态，不可新增或更改）
VEHICLE_STATES: tuple[str, ...] = (
    "offline",
    "online_idle",
    "auto_driving",
    "remote_controlled",
    "upgrading",
    "charging",
    "fault",
    "emergency",
)


class FleetSource(Protocol):
    """车队状态来源协议（服务层依赖倒置）。"""

    async def online_count(self) -> int:
        """在线车辆数（vehicle:online:set 基数）。"""
        ...

    async def status_distribution(self) -> dict[str, int]:
        """按 8 态的状态计数（未知状态仅告警不计数）。"""
        ...


class RedisFleetRepository:
    """Redis 车队状态实现（SCARD + SCAN，只读操作）。"""

    STATUS_KEY_PREFIX = "vehicle:status:"
    ONLINE_SET_KEY = "vehicle:online:set"
    #: vehicle:status Hash 的状态字段约定（设计文档 7.6 节：最新状态快照，与写入方 data-collector 约定）
    STATUS_FIELD = "status"

    def __init__(self, redis: Redis, *, scan_count: int = 200) -> None:
        self._redis = redis
        self._scan_count = scan_count

    async def online_count(self) -> int:
        """在线车辆数（S cardinality）。"""
        return int(await self._redis.scard(self.ONLINE_SET_KEY))

    async def status_distribution(self) -> dict[str, int]:
        """按 8 态聚合状态计数；Hash 缺失或未知状态的车辆不计入（看板侧兜底为 offline）。"""
        counts = {state: 0 for state in VEHICLE_STATES}
        async for key in self._redis.scan_iter(match=f"{self.STATUS_KEY_PREFIX}*", count=self._scan_count):
            status = await self._redis.hget(key, self.STATUS_FIELD)
            if status is None:
                continue
            if isinstance(status, bytes):
                status = status.decode("utf-8")
            if status in counts:
                counts[status] += 1
            else:
                # 未知状态：不发明新状态，仅记录告警（设计文档状态词表之外）
                logger.warning("unknown_vehicle_state", state=str(status))
        return counts