"""车辆可控性读模型（Redis：vehicle:online:set + vehicle:status:{vehicle_id}）。

只读消费（Key 由 data-collector 遥测消费链路维护，本服务禁止写入）：
- 在线判定：``vehicle:online:set``（Set）成员命中；
- 状态明细：``vehicle:status:{vehicle_id}``（Hash）——字段名以**唯一写方 data-collector
  实际写入的字段**为准（redis-keys.yaml fields_draft，pending #1/#2）：
  status / battery_soc / velocity / last_seen_at / vehicle_name / model；
  ⚠ 本读方曾按 ``last_online_time`` 取数——Hash 里从未有写方写该字段，
  致看板/操控台「最近在线」恒兜底为 0 → 展示 1970-01-01 08:00:00（响应字段名不变，
  只改 Hash 取数源）；
  ⚠ 本读方还曾把缺失的 ``battery_soc``/``velocity`` 兜成 0/0.0（V1.19.21 去掉）：
  「车没上报」被说成「电量 0%」是同一类谎报，与写侧「未上报即不写字段」的纪律冲突，
  故这两个字段现在缺失即 null（契约标 nullable，量纲值不得伪造）；
- 活跃会话：``rc:session:{vehicle_id}``（Hash，本服务写）交叉判定互斥。

BlockReason → 错误码映射（契约 x-hunter-remote-session-lifecycle.responses）：
offline→4001；upgrading/charging/fault/emergency→4002；already_controlled→7001。
"""
from __future__ import annotations

from hunter_common.logging import get_logger
from hunter_common.redis import RedisManager

from app.config import Settings
from app.schemas.common import BlockReason, VehicleStatus
from app.schemas.vehicles import ActiveSessionBrief, ControllableVehicle

logger = get_logger("app.services.vehicle_view")

KEY_ONLINE_SET = "vehicle:online:set"
KEY_VEHICLE_STATUS = "vehicle:status:{vehicle_id}"
KEY_RC_SESSION = "rc:session:{vehicle_id}"

#: Hash 字段名（data-collector 写入侧实际字段；**缺失即 null，不得兜 0**——见下方说明）
_FIELD_STATUS = "status"
_FIELD_SOC = "battery_soc"
_FIELD_VELOCITY = "velocity"
#: 最近在线取数源 = 写方实际维护的 ``last_seen_at``（health/telemetry 每次 HSET 刷新）
_FIELD_LAST_ONLINE = "last_seen_at"
_FIELD_VEHICLE_NAME = "vehicle_name"
_FIELD_MODEL = "model"


class VehicleViewReader:
    """车辆可控性读模型访问器（异步 Redis；缺失/脏数据降级为不可控）。"""

    def __init__(self, redis_manager: RedisManager, settings: Settings) -> None:
        self._redis = redis_manager
        self._settings = settings

    async def is_online(self, vehicle_id: str) -> bool:
        """车辆是否在线（vehicle:online:set 成员判定）。"""
        return bool(await self._redis.client.sismember(KEY_ONLINE_SET, vehicle_id))

    async def get_active_session_id(self, vehicle_id: str) -> str | None:
        """读取车辆当前活跃会话 ID（rc:session:{vehicle_id}.session_id）。"""
        mapping = await self._redis.client.hgetall(KEY_RC_SESSION.format(vehicle_id=vehicle_id))
        if not mapping:
            return None
        session_id = mapping.get("session_id")
        return session_id or None

    async def get_controllable_view(self, vehicle_id: str) -> ControllableVehicle:
        """构建单车辆可控性视图（契约 GET /vehicles 条目结构）。

        判定优先级（互斥）：离线 → online=False/block=offline；
        车辆状态为 upgrading/charging/fault/emergency → block=对应值（4002）；
        车辆状态 remote_controlled 或存在活跃会话 → block=already_controlled（7001）；
        其余（online_idle 等）→ controllable=True。
        """
        online = await self.is_online(vehicle_id)
        mapping: dict[str, str] = {}
        if online:
            raw = await self._redis.client.hgetall(KEY_VEHICLE_STATUS.format(vehicle_id=vehicle_id))
            mapping = dict(raw) if raw else {}

        status_value = mapping.get(_FIELD_STATUS) or ""
        session_hash = await self._redis.client.hgetall(KEY_RC_SESSION.format(vehicle_id=vehicle_id))
        active_session_id = (session_hash or {}).get("session_id") or None

        controllable = False
        block_reason: BlockReason | None = None
        if not online:
            status_enum = VehicleStatus.OFFLINE
            block_reason = BlockReason.OFFLINE
        else:
            try:
                status_enum = VehicleStatus(status_value)
            except ValueError:
                # 未知状态值：保守视为不可控（读模型脏数据容错），并记录告警日志
                logger.warning("unknown_vehicle_status", vehicle_id=vehicle_id, status=status_value)
                status_enum = VehicleStatus.ONLINE_IDLE
                block_reason = BlockReason.CHARGING  # 非 online_idle 的兜底 4002 语义
            if status_enum in {
                VehicleStatus.UPGRADING,
                VehicleStatus.CHARGING,
                VehicleStatus.FAULT,
                VehicleStatus.EMERGENCY,
            }:
                block_reason = BlockReason(status_enum)
            elif status_enum == VehicleStatus.REMOTE_CONTROLLED or active_session_id:
                block_reason = BlockReason.ALREADY_CONTROLLED
            elif status_enum in {VehicleStatus.ONLINE_IDLE, VehicleStatus.AUTO_DRIVING}:
                # 契约 POST /session 描述（167 行）：auto_driving 允许接管（切换为 remote_controlled）
                controllable = True

        active_session: ActiveSessionBrief | None = None
        if session_hash and active_session_id:
            # 契约 ActiveSessionBrief：session_id/operator_id 必填，operator_name 缺失为 null
            operator_id = session_hash.get("operator_id") or ""
            started_at = _parse_float(session_hash.get("started_at"))
            if operator_id and started_at is not None:
                active_session = ActiveSessionBrief(
                    session_id=active_session_id,
                    operator_id=operator_id,
                    operator_name=session_hash.get("operator_name") or None,
                    started_at=started_at,
                )

        return ControllableVehicle(
            vehicle_id=vehicle_id,
            vehicle_name=mapping.get(_FIELD_VEHICLE_NAME) or None,
            # 车型基线固定 HUNTER_SE（契约 Literal；读模型 model 字段仅参考，不一致以基线为准）
            model="HUNTER_SE",
            status=status_enum,
            controllable=controllable,
            block_reason=block_reason,
            # ℞ V1.19.21：旧写法 `... or 0` 把「读模型根本没有这个字段」渲染成「电量 0%」，
            #   而 0% 在业务上是「车快没电了」的强语义（<10% 还对应 battery_critical 事件）——
            #   与本仓「禁止伪造默认值」纪律（G-08 gear/free_storage_mb、G-11 lat/lng）自相矛盾，
            #   且现场无法三分「车端没报」「遥测链路断」「真的是 0」。现按 null 如实透出，
            #   0 仍然是 0（写方报什么就是什么），只有「无数据」才是 null。
            battery_soc=_parse_int(mapping.get(_FIELD_SOC)),
            velocity=_parse_float(mapping.get(_FIELD_VELOCITY)),
            # 最近在线：无心跳时刻时仍给 0.0（前端按 `>0` 判「尚未上线」，1970 已由 V1.19.20 修掉；
            #   本字段是时间戳而非量纲值，兜 0 不会被误读成某种业务量）
            last_online_time=_parse_float(mapping.get(_FIELD_LAST_ONLINE)) or 0.0,
            active_session=active_session,
        )

    async def list_online_views(self, *, limit: int = 500) -> list[ControllableVehicle]:
        """列举全部在线车辆视图（含不可控项；vehicle_id 升序）。

        先读 vehicle:online:set 全量成员（在线集合规模有限），再逐车辆构建视图；
        status/controllable_only/vehicle_id 过滤与分页由服务层组合（契约 GET /vehicles）。
        """
        members = await self._redis.client.smembers(KEY_ONLINE_SET)
        vehicle_ids = sorted(str(member) for member in members or [])[:limit]
        return [await self.get_controllable_view(vehicle_id) for vehicle_id in vehicle_ids]


def _parse_int(raw: str | None) -> int | None:
    """宽松整型解析（读模型容错；失败返回 None）。"""
    if raw is None or raw == "":
        return None
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return None


def _parse_float(raw: str | None) -> float | None:
    """宽松浮点解析（读模型容错；失败返回 None）。"""
    if raw is None or raw == "":
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


__all__ = ["VehicleViewReader"]
