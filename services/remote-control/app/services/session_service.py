"""操控会话服务（契约 x-hunter-session-lifecycle 生命周期编排）。

- 会话态唯一事实来源：Redis Hash ``rc:session:{vehicle_id}``（11 字段固定，
  契约 x-hunter-remote-config.redis_keys；⚠ pending #6 字段定稿前冻结）；
- 互斥：``rc:lock:{vehicle_id}`` 分布式锁（TTL 30s）+ 锁内二次判定（创建），
  同车同时仅一名操作员（系统约束第 15 条）；
- 创建 = 可控性判定（4001/4002/7001）→ 加锁 → 写 Hash → rc_session_start 信令 →
  boot 心跳帧；任一 Kafka 投递失败 → 回滚 Hash 删除 → 5001（ServiceUnavailableError）；
- 结束 = 「先安全后清理」（契约 267 行顺序不可颠倒）：stop 帧 + session_end 信令
  （均尽力而为）→ 删 Hash → sidecar 归档（尽力而为，失败置 sidecar_written=false）；
- 操作员身份只取网关注入头（X-User-Id / X-Roles），不查 user-service 表
  （契约 db_cross_service_policy）；
- 数据权限：普通用户仅可见/可结束自身会话；admin 可跨操作员（契约 278/324 行）。
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING
from uuid import uuid4

from hunter_common.exceptions import (
    AuthenticationError,
    PermissionDeniedError,
    RemoteControlSessionConflictError,
    ResourceNotFoundError,
    VehicleBusyError,
    VehicleOfflineError,
)
from hunter_common.logging import get_logger
from hunter_common.redis import RedisManager
from redis.exceptions import LockError

from app.config import Settings
from app.producers.remote_control import RemoteControlFrameProducer
from app.producers.session_command import SessionCommandProducer
from app.repositories.storage import (
    VideoArchiveStorage,
    build_sidecar_object_key,
    build_video_object_key,
)
from app.schemas.common import (
    UUID_PATTERN,
    BlockReason,
    DegradedReason,
    EndReasonSource,
    SessionEndReason,
    SessionStatus,
)
from app.schemas.history import (
    SessionSidecar,
    SidecarControl,
    SidecarEnvironment,
    SidecarVideo,
)
from app.schemas.sessions import (
    ControlChannelInfo,
    ControlStats,
    CreateRemoteSessionRequest,
    IceServer,
    RecordArchiveInfo,
    RemoteSessionDetail,
    RemoteSessionInfo,
    RemoteSessionList,
    RemoteSessionResult,
    SessionHeartbeat,
    SrsMediaInfo,
    VideoConfig,
    VideoPreference,
    WebRtcConnectionInfo,
)
from app.services.metrics import (
    ARCHIVE_FAILED_TOTAL,
    SESSION_CONFLICTS_TOTAL,
    SESSIONS_ACTIVE,
)
from app.services.vehicle_view import VehicleViewReader

if TYPE_CHECKING:
    # 循环依赖规避：dependencies.py 装配本服务，类型仅注解期引用（运行时鸭子类型）
    from app.core.dependencies import OperatorContext

logger = get_logger("app.services.session_service")

SESSION_HASH_KEY = "rc:session:{vehicle_id}"
SESSION_LOCK_KEY = "rc:lock:{vehicle_id}"

# rc:session Hash 字段名（契约固定 11 字段，不可增减；⚠ pending #6）
FIELD_SESSION_ID = "session_id"
FIELD_OPERATOR_ID = "operator_id"
FIELD_OPERATOR_NAME = "operator_name"
FIELD_STARTED_AT = "started_at"
FIELD_STATUS = "status"
FIELD_SEQ_LAST = "seq_last"
FIELD_LAST_HEARTBEAT_AT = "last_heartbeat_at"
FIELD_COMMANDS_SENT = "commands_sent"
FIELD_COMMANDS_ACKED = "commands_acked"
FIELD_VIDEO_OBJECT_KEY = "video_object_key"
FIELD_SIDECAR_OBJECT_KEY = "sidecar_object_key"


class SessionService:
    """操控会话生命周期服务（创建/查询/列表/结束；Kafka 信令 + sidecar 归档）。"""

    def __init__(
        self,
        *,
        redis: RedisManager,
        vehicle_view: VehicleViewReader,
        frame_producer: RemoteControlFrameProducer,
        command_producer: SessionCommandProducer,
        storage: VideoArchiveStorage,
        settings: Settings,
    ) -> None:
        self._redis = redis
        self._vehicle_view = vehicle_view
        self._frame_producer = frame_producer
        self._command_producer = command_producer
        self._storage = storage
        self._settings = settings

    # ---------- 创建（POST /session；契约 x-hunter-session-lifecycle） ----------
    async def create_session(
        self,
        vehicle_id: str,
        operator: OperatorContext,
        request: CreateRemoteSessionRequest,
    ) -> RemoteSessionInfo:
        """创建操控会话：可控性判定 → 互斥锁 → 写会话 Hash → 车端信令 → boot 帧。"""
        if not re.match(UUID_PATTERN, operator.user_id):
            # 网关注入身份非法视为未认证（1001），避免下游 Pydantic 校验炸 5000
            raise AuthenticationError(message="操作员身份无效（X-User-Id 非 UUID）")

        view = await self._vehicle_view.get_controllable_view(vehicle_id)
        if not view.controllable:
            self._raise_blocked(vehicle_id, view.block_reason)

        session_id = str(uuid4())
        started_at = time.time()
        video = self._negotiate_video(request.video)
        control_channel = self._build_control_channel()
        heartbeat = self._build_heartbeat()
        webrtc = self._build_webrtc(vehicle_id, session_id)
        video_key = build_video_object_key(vehicle_id, started_at, session_id)
        sidecar_key = build_sidecar_object_key(vehicle_id, started_at, session_id)

        lock = await self._redis.acquire_lock(
            SESSION_LOCK_KEY.format(vehicle_id=vehicle_id),
            timeout=float(self._settings.rc_session_lock_ttl_s),
        )
        if not await lock.acquire(blocking=False):
            # 锁被并发创建持有 → 7001（同车同时仅一名操作员，不排队）
            SESSION_CONFLICTS_TOTAL.labels(vehicle_id=vehicle_id).inc()
            raise RemoteControlSessionConflictError(
                message=f"车辆 {vehicle_id} 正在建立其他操控会话，请稍后重试"
            )
        try:
            session_key = SESSION_HASH_KEY.format(vehicle_id=vehicle_id)
            existing = await self._redis.client.hget(session_key, FIELD_SESSION_ID)
            if existing:
                # 锁内二次判定（Double-check）：判定与加锁窗口期他人已建会话 → 7001
                SESSION_CONFLICTS_TOTAL.labels(vehicle_id=vehicle_id).inc()
                raise RemoteControlSessionConflictError(
                    message=f"车辆 {vehicle_id} 已被其他操作员操控"
                )

            mapping = self._session_mapping(
                session_id=session_id,
                operator_id=operator.user_id,
                started_at=started_at,
                video_key=video_key,
                sidecar_key=sidecar_key,
            )
            await self._redis.client.hset(session_key, mapping=mapping)
            record = RecordArchiveInfo(
                object_key=video_key, sidecar_object_key=sidecar_key
            )
            try:
                # 信令与 boot 帧失败 → 5001 并回滚 Hash（会话未建立不留脏状态）
                await self._command_producer.send_session_start(
                    vehicle_id,
                    session_id=session_id,
                    operator_id=operator.user_id,
                    issued_by=operator.user_id,
                    video=video,
                    webrtc=webrtc,
                    record=record,
                )
                await self._frame_producer.send_boot(
                    vehicle_id,
                    session_id=session_id,
                    operator_id=operator.user_id,
                    video=video,
                    control=control_channel,
                    heartbeat=heartbeat,
                )
            except Exception:
                await self._redis.client.delete(session_key)
                raise

            SESSIONS_ACTIVE.labels(vehicle_id=vehicle_id).inc()
            logger.info(
                "session_created",
                vehicle_id=vehicle_id,
                session_id=session_id,
                operator_id=operator.user_id,
            )
            return RemoteSessionInfo(
                session_id=session_id,
                vehicle_id=vehicle_id,
                operator_id=operator.user_id,
                # 身份只取 JWT 声明（无 user-service 读模型，契约 db_cross_service_policy）
                operator_name=None,
                started_at=started_at,
                status=SessionStatus.CONNECTING,
                video=video,
                control_channel=control_channel,
                heartbeat=heartbeat,
                webrtc=webrtc,
                record=record,
            )
        finally:
            try:
                await lock.release()
            except LockError:
                # 锁 TTL 到期被 Redis 自动释放（创建流程超长兜底），不阻断响应
                logger.warning(
                    "session_lock_auto_released",
                    vehicle_id=vehicle_id,
                    session_id=session_id,
                )

    # ---------- 查询（GET /session/{session_id}；契约 include_stats） ----------
    async def get_session(
        self, session_id: str, operator: OperatorContext, *, include_stats: bool = True
    ) -> RemoteSessionDetail:
        """查询单个活跃会话（含实时统计块；会话不存在/已结束 → 3001）。"""
        found = await self._find_session(session_id)
        if found is None:
            raise ResourceNotFoundError(message=f"操控会话 {session_id} 不存在或已结束")
        vehicle_id, mapping = found
        if not self._can_access(operator, mapping):
            # 资源级越权查询返回 3001 而非 1002：不向非归属者泄漏会话存在性
            raise ResourceNotFoundError(message=f"操控会话 {session_id} 不存在或已结束")
        return self._build_detail(vehicle_id, mapping, include_stats=include_stats)

    # ---------- 列表（GET /sessions；契约：普通用户仅自身，admin 可全量） ----------
    async def list_active_sessions(
        self,
        operator: OperatorContext,
        *,
        vehicle_id: str | None = None,
        operator_id: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> RemoteSessionList:
        """列举活跃会话（SCAN rc:session:*；started_at 降序；数据权限过滤）。"""
        is_admin = operator.is_admin(self._settings)
        # 契约 139 行：普通用户仅能查询自身会话（operator_id 过滤参数被强制覆盖）
        effective_operator = operator_id if is_admin else operator.user_id
        items: list[RemoteSessionInfo] = []
        async for vid, mapping in self._iter_sessions():
            if vehicle_id is not None and vid != vehicle_id:
                continue
            if (
                effective_operator is not None
                and mapping.get(FIELD_OPERATOR_ID) != effective_operator
            ):
                continue
            items.append(self._build_info(vid, mapping))
        items.sort(key=lambda item: item.started_at, reverse=True)
        total = len(items)
        start = (page - 1) * page_size
        return RemoteSessionList(
            items=items[start : start + page_size],
            total=total,
            page=page,
            page_size=page_size,
        )

    # ---------- 结束（DELETE /session/{session_id}；契约 267 行「先安全后清理」） ----------
    async def end_session(
        self,
        session_id: str,
        operator: OperatorContext,
        reason: SessionEndReason = SessionEndReason.OPERATOR_END,
    ) -> RemoteSessionResult:
        """结束会话：车端释放信令（尽力而为）→ 删 Hash → sidecar 归档（尽力而为）。"""
        found = await self._find_session(session_id)
        if found is None:
            # 幂等：会话已结束（键不存在）→ 3001，不重复下发车端信令（契约 280 行）
            raise ResourceNotFoundError(message=f"操控会话 {session_id} 不存在或已结束")
        vehicle_id, mapping = found
        if not self._can_access(operator, mapping):
            # 越权结束 → 1002（仅会话所属操作员或 admin，契约 278 行）
            raise PermissionDeniedError(
                message="无权限结束他人操控会话（仅会话归属操作员或管理员）"
            )

        session_operator_id = mapping.get(FIELD_OPERATOR_ID) or operator.user_id
        # Hash 缺失/非法时以当前时间兜底（_to_float 的 None 分支仅类型层面兜底）
        started_at = _to_float(mapping.get(FIELD_STARTED_AT)) or time.time()
        ended_at = time.time()
        duration_s = max(0, int(ended_at - started_at))
        record = RecordArchiveInfo(
            object_key=mapping.get(FIELD_VIDEO_OBJECT_KEY) or "",
            sidecar_object_key=mapping.get(FIELD_SIDECAR_OBJECT_KEY) or "",
        )
        commands_sent = _to_int(mapping.get(FIELD_COMMANDS_SENT)) or 0
        commands_acked = _to_int(mapping.get(FIELD_COMMANDS_ACKED)) or 0
        seq_last = _to_int(mapping.get(FIELD_SEQ_LAST)) or 0

        # 1) 车端释放（stop 帧 + session_end 信令；producer 内部尽力而为不抛 5001）
        await self._frame_producer.send_stop(
            vehicle_id,
            session_id=session_id,
            operator_id=session_operator_id,
            reason=reason,
        )
        await self._command_producer.send_session_end(
            vehicle_id,
            session_id=session_id,
            operator_id=session_operator_id,
            issued_by=operator.user_id,
            reason=reason,
        )

        # 2) 删会话键（归档转 MinIO sidecar，契约 redis_keys.usage）+ 活跃数指标回落
        session_key = SESSION_HASH_KEY.format(vehicle_id=vehicle_id)
        await self._redis.client.delete(session_key)
        SESSIONS_ACTIVE.labels(vehicle_id=vehicle_id).dec()

        # 3) sidecar 归档（尽力而为：失败不阻断结束响应，置 sidecar_written=false 人工核查）
        sidecar_written = await self._archive_sidecar(
            vehicle_id=vehicle_id,
            mapping=mapping,
            session_id=session_id,
            started_at=started_at,
            ended_at=ended_at,
            duration_s=duration_s,
            reason=reason,
        )
        logger.info(
            "session_ended",
            vehicle_id=vehicle_id,
            session_id=session_id,
            reason=reason,
            duration_s=duration_s,
            sidecar_written=sidecar_written,
        )
        return RemoteSessionResult(
            session_id=session_id,
            vehicle_id=vehicle_id,
            operator_id=session_operator_id,
            operator_name=mapping.get(FIELD_OPERATOR_NAME) or None,
            status=SessionStatus.ENDED,
            end_reason=reason,
            started_at=started_at,
            ended_at=ended_at,
            duration_s=duration_s,
            control_stats=ControlStats(
                commands_sent=commands_sent,
                commands_acked=commands_acked,
                ack_latency_ms_avg=None,  # 回执延迟统计随 WS 控制通道接入（pending #13-#16）
                ack_latency_ms_p95=None,
                timeout_events=0,  # Hash 11 字段无超时计数；WS 通道接入后由内存统计补充
                last_seq=seq_last,
                last_ack_at=_to_float(
                    mapping.get(FIELD_LAST_HEARTBEAT_AT), default=None
                ),
            ),
            video_stats=None,  # 视频质量统计源（SRS API/WS 回执）未接入（pending #10/#12）
            record=record,
            sidecar_written=sidecar_written,
        )

    # ---------- 内部：会话定位与权限 ----------
    async def _find_session(self, session_id: str) -> tuple[str, dict[str, str]] | None:
        """按 session_id 在 rc:session:* 中定位会话（返回 (vehicle_id, Hash)）。

        会话数 ≤ 在线车辆数（契约 GET /sessions 描述，规模小），SCAN + 逐键 HGETALL 可控。
        """
        async for vid, mapping in self._iter_sessions():
            if mapping.get(FIELD_SESSION_ID) == session_id:
                return vid, mapping
        return None

    async def _iter_sessions(self) -> AsyncIterator[tuple[str, dict[str, str]]]:
        """迭代全部活跃会话（SCAN rc:session:*，COUNT=100 + 总键数上限保护防放大）。"""
        cursor: int = 0
        scanned = 0
        limit = self._settings.rc_sessions_scan_limit
        while scanned < limit:
            cursor, keys = await self._redis.client.scan(
                cursor=cursor, match="rc:session:*", count=100
            )
            for key in keys:
                mapping = await self._redis.client.hgetall(key)
                if mapping:
                    yield key.removeprefix("rc:session:"), dict(mapping)
            scanned += len(keys)
            if int(cursor) == 0:
                break

    def _can_access(self, operator: OperatorContext, mapping: dict[str, str]) -> bool:
        """数据权限：admin 全量可见；普通用户仅自身会话（契约 278/324 行）。"""
        if operator.is_admin(self._settings):
            return True
        return mapping.get(FIELD_OPERATOR_ID) == operator.user_id

    # ---------- 内部：投影构建 ----------
    def _build_info(
        self, vehicle_id: str, mapping: dict[str, str]
    ) -> RemoteSessionInfo:
        """Hash → RemoteSessionInfo（链路参数以当前配置重建；Hash 未存协商结果，pending #6）。"""
        return RemoteSessionInfo(
            session_id=mapping.get(FIELD_SESSION_ID) or "",
            vehicle_id=vehicle_id,
            operator_id=mapping.get(FIELD_OPERATOR_ID) or "",
            operator_name=mapping.get(FIELD_OPERATOR_NAME) or None,
            started_at=_to_float(mapping.get(FIELD_STARTED_AT)) or 0.0,
            status=SessionStatus(
                mapping.get(FIELD_STATUS) or SessionStatus.CONNECTING.value
            ),
            video=self._negotiate_video(VideoPreference()),
            control_channel=self._build_control_channel(),
            heartbeat=self._build_heartbeat(),
            webrtc=self._build_webrtc(vehicle_id, mapping.get(FIELD_SESSION_ID) or ""),
            record=RecordArchiveInfo(
                object_key=mapping.get(FIELD_VIDEO_OBJECT_KEY) or "",
                sidecar_object_key=mapping.get(FIELD_SIDECAR_OBJECT_KEY) or "",
            ),
        )

    def _build_detail(
        self, vehicle_id: str, mapping: dict[str, str], *, include_stats: bool
    ) -> RemoteSessionDetail:
        """Hash → RemoteSessionDetail（include_stats=false 时仅返回元信息与状态）。"""
        base = self._build_info(vehicle_id, mapping)
        if not include_stats:
            return RemoteSessionDetail(
                **base.model_dump(),
                last_heartbeat_at=None,
                control_stats=None,
                video_stats=None,
                degraded=False,
                degraded_reasons=[],
            )
        heartbeat_at = _to_float(mapping.get(FIELD_LAST_HEARTBEAT_AT), default=None)
        # 降级判定：最近心跳距今超过 连续丢失阈值×间隔（契约 x-hunter-session-heartbeat；pending #4）
        degraded = heartbeat_at is not None and (time.time() - heartbeat_at) > (
            self._settings.rc_heartbeat_interval_s
            * self._settings.rc_heartbeat_degraded_misses
        )
        return RemoteSessionDetail(
            **base.model_dump(),
            last_heartbeat_at=heartbeat_at,
            control_stats=ControlStats(
                commands_sent=_to_int(mapping.get(FIELD_COMMANDS_SENT)) or 0,
                commands_acked=_to_int(mapping.get(FIELD_COMMANDS_ACKED)) or 0,
                ack_latency_ms_avg=None,  # 回执延迟统计随 WS 控制通道接入（pending #13-#16）
                ack_latency_ms_p95=None,
                timeout_events=0,  # Hash 11 字段无超时计数；WS 通道接入后由内存统计补充
                last_seq=_to_int(mapping.get(FIELD_SEQ_LAST)) or 0,
                last_ack_at=heartbeat_at,
            ),
            video_stats=None,  # 视频质量统计源（SRS API/WS 回执）未接入（pending #10/#12）
            degraded=degraded,
            degraded_reasons=[DegradedReason.HEARTBEAT_MISS] if degraded else [],
        )

    # ---------- 内部：结束归档 ----------
    async def _archive_sidecar(
        self,
        *,
        vehicle_id: str,
        mapping: dict[str, str],
        session_id: str,
        started_at: float,
        ended_at: float,
        duration_s: int,
        reason: SessionEndReason,
    ) -> bool:
        """写入 sidecar JSON（契约 sidecar_schema；失败 → ARCHIVE_FAILED_TOTAL + False）。"""
        sidecar_key = mapping.get(FIELD_SIDECAR_OBJECT_KEY) or ""
        video_key = mapping.get(FIELD_VIDEO_OBJECT_KEY) or ""
        if not sidecar_key or not video_key:
            logger.error(
                "sidecar_key_missing", vehicle_id=vehicle_id, session_id=session_id
            )
            ARCHIVE_FAILED_TOTAL.labels(stage="sidecar").inc()
            return False
        video = self._negotiate_video(VideoPreference())
        try:
            doc = SessionSidecar(
                session_id=session_id,
                vehicle_id=vehicle_id,
                operator_id=mapping.get(FIELD_OPERATOR_ID) or "",
                operator_name=mapping.get(FIELD_OPERATOR_NAME) or None,
                started_at=started_at,
                ended_at=ended_at,
                duration_s=duration_s,
                end_reason=reason,
                end_reason_source=EndReasonSource.PLATFORM,
                degraded_events=[],  # 降级事件随 WS 控制通道接入记录（pending #4/#13-#16）
                video=SidecarVideo(
                    object_key=video_key,
                    size_bytes=0,  # 录像管线（SRS 封存上传）回写前为 0；历史查询以 head_video 为准
                    sha256="",  # 录像管线未回写校验和时为空串
                    fps=video.fps,
                    bitrate_kbps_avg=video.max_bitrate_kbps,  # 协商上限作为平均码率占位
                    e2e_latency_ms_p95=0.0,
                ),
                control=SidecarControl(
                    commands_sent=_to_int(mapping.get(FIELD_COMMANDS_SENT)) or 0,
                    commands_acked=_to_int(mapping.get(FIELD_COMMANDS_ACKED)) or 0,
                    ack_latency_ms_avg=0.0,
                    ack_latency_ms_p95=0.0,
                    timeout_events=0,
                    max_speed_mps=self._settings.rc_max_speed_mps,
                ),
                note=None,
                environment=SidecarEnvironment(
                    service_version=_service_version(),
                    config_digest=_config_digest(self._settings),
                ),
            )
            await self._storage.write_sidecar(sidecar_key, doc.model_dump(mode="json"))
        except Exception:
            ARCHIVE_FAILED_TOTAL.labels(stage="sidecar").inc()
            logger.exception(
                "sidecar_write_failed", vehicle_id=vehicle_id, session_id=session_id
            )
            return False
        return True

    # ---------- 内部：创建辅助 ----------
    def _session_mapping(
        self,
        *,
        session_id: str,
        operator_id: str,
        started_at: float,
        video_key: str,
        sidecar_key: str,
    ) -> dict[str, str]:
        """rc:session Hash 11 字段（契约固定，全部 str 编码；不可增减，⚠ pending #6）。"""
        return {
            FIELD_SESSION_ID: session_id,
            FIELD_OPERATOR_ID: operator_id,
            FIELD_OPERATOR_NAME: "",  # 无 user-service 读模型，显示名恒空（读取侧转 None）
            FIELD_STARTED_AT: repr(started_at),
            FIELD_STATUS: SessionStatus.CONNECTING.value,
            FIELD_SEQ_LAST: "0",
            FIELD_LAST_HEARTBEAT_AT: "",
            FIELD_COMMANDS_SENT: "0",
            FIELD_COMMANDS_ACKED: "0",
            FIELD_VIDEO_OBJECT_KEY: video_key,
            FIELD_SIDECAR_OBJECT_KEY: sidecar_key,
        }

    def _raise_blocked(self, vehicle_id: str, block_reason: BlockReason | None) -> None:
        """不可控原因 → 预定义错误码（契约 209 行：4001/4002/7001 → HTTP 409）。"""
        if block_reason == BlockReason.OFFLINE:
            raise VehicleOfflineError(
                message=f"车辆 {vehicle_id} 不在线，无法建立操控会话"
            )
        if block_reason == BlockReason.ALREADY_CONTROLLED:
            SESSION_CONFLICTS_TOTAL.labels(vehicle_id=vehicle_id).inc()
            raise RemoteControlSessionConflictError(
                message=f"车辆 {vehicle_id} 已被其他操作员操控"
            )
        raise VehicleBusyError(
            message=f"车辆 {vehicle_id} 当前状态（{block_reason}）不允许建立操控会话"
        )

    # ---------- 内部：链路协商 ----------
    def _negotiate_video(self, preference: VideoPreference | None) -> VideoConfig:
        """视频参数协商：客户端期望与平台/车端能力上限取交集（契约 video_config 描述）。

        分辨率/编码/关键帧间隔为契约 Literal 固定值（1280x720 h264 GOP 1s，系统约束第 15 条）；
        可协商项仅 fps（1-30）与目标码率（2048-4096 kbps，与平台上限取小）。
        """
        settings = self._settings
        pref = preference or VideoPreference()
        return VideoConfig(
            # 分辨率/编码/GOP 为契约 Literal 固定值（config 校验器保证环境变量恒等，
            # 否则启动失败 —— 此处字面量即契约常量，非可调参数）
            width=1280,  # 契约 Literal[1280]（RC_VIDEO_WIDTH）
            height=720,  # 契约 Literal[720]（RC_VIDEO_HEIGHT）
            fps=min(pref.fps or settings.rc_video_fps, settings.rc_video_fps),
            min_bitrate_kbps=settings.rc_video_bitrate_min_kbps,
            max_bitrate_kbps=min(
                pref.target_bitrate_kbps or settings.rc_video_bitrate_max_kbps,
                settings.rc_video_bitrate_max_kbps,
            ),
            keyframe_interval_s=1,  # 契约 Literal[1]（RC_VIDEO_KEYFRAME_INTERVAL_S）
        )

    def _build_control_channel(self) -> ControlChannelInfo:
        """控制通道参数（契约 ControlChannelInfo：20Hz/50ms 固定 + ACK/限速来自配置）。"""
        settings = self._settings
        return ControlChannelInfo(
            hz=20,  # 契约 Literal[20]（RC_COMMAND_HZ 校验器保证恒等）
            interval_ms=50,  # 契约 Literal[50]（RC_COMMAND_INTERVAL_MS）
            ack_timeout_ms=settings.rc_command_ack_timeout_ms,
            stop_on_timeout_ms=500,  # 契约 Literal[500]（RC_STOP_ON_TIMEOUT_MS）
            max_speed_mps=settings.rc_max_speed_mps,
        )

    def _build_heartbeat(self) -> SessionHeartbeat:
        """心跳参数（契约 SessionHeartbeat：10s 周期固定，降级/结束阈值来自配置）。"""
        settings = self._settings
        return SessionHeartbeat(
            interval_s=10,  # 契约 Literal[10]（RC_HEARTBEAT_INTERVAL_S 校验器保证恒等）
            degraded_after_misses=settings.rc_heartbeat_degraded_misses,
            end_after_s=settings.rc_heartbeat_end_after_s,
        )

    def _build_ice_servers(self) -> list[IceServer]:
        """ICE 服务器列表（STUN 列表 + TURN 短期凭证；契约 IceServer 描述）。"""
        settings = self._settings
        servers = [IceServer(urls=[url]) for url in settings.rc_stun_url_list]
        turns = settings.rc_turn_url_list
        if turns:
            servers.append(
                IceServer(
                    urls=turns,
                    username=settings.rc_turn_username or None,
                    credential=settings.rc_turn_shared_secret
                    or None,  # 敏感：禁止落日志
                    credential_ttl_s=settings.rc_turn_credential_ttl_s,
                )
            )
        return servers

    def _build_webrtc(self, vehicle_id: str, session_id: str) -> WebRtcConnectionInfo:
        """WebRTC 接入上下文（WSS 信令/控制通道 + SRS 媒体信息；契约 WebRtcConnectionInfo）。

        通道路径对齐网关路由 /ws/remote/**；SRS 集成参数（WHIP/WHEP/RTMP）来自配置，
        pending #10 定稿前可为空（契约 optional 字段）。
        """
        settings = self._settings
        return WebRtcConnectionInfo(
            signal_ws_url=f"{settings.rc_public_ws_base_url}/ws/remote/{session_id}/signal",
            control_ws_url=f"{settings.rc_public_ws_base_url}/ws/remote/{session_id}/control",
            publisher="vehicle",
            subscriber="browser",
            ice_servers=self._build_ice_servers(),
            media_server=SrsMediaInfo(
                app=settings.rc_srs_app,
                stream=f"{settings.rc_srs_stream_prefix}/{vehicle_id}_{session_id}",
                whip_url=settings.rc_srs_whip_url or None,
                whep_url=settings.rc_srs_whep_url or None,
                rtmp_url=settings.rc_srs_rtmp_url or None,
            ),
        )


# ---------- 模块级工具 ----------
def _to_int(value: str | None) -> int | None:
    """Hash str → int（空串/非法 → None）。"""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _to_float(value: str | None, *, default: float | None = None) -> float | None:
    """Hash str → float（空串/非法 → default）。"""
    if value is None or value == "":
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _service_version() -> str:
    """sidecar.environment.service_version（来源：容器环境 RC_SERVICE_VERSION，默认 dev）。"""
    return os.getenv("RC_SERVICE_VERSION", "dev")


def _config_digest(settings: Settings) -> str:
    """影响回放语义的配置摘要（sha256 前 12 位；非敏感，仅链路参数入摘要）。"""
    payload = json.dumps(
        {
            "rate_hz": settings.rc_command_hz,
            "ack_timeout_ms": settings.rc_command_ack_timeout_ms,
            "max_speed_mps": settings.rc_max_speed_mps,
            "heartbeat_interval_s": settings.rc_heartbeat_interval_s,
        },
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:12]


__all__ = ["SessionService"]
