"""操控历史与录像访问服务（MinIO hunter-video sidecar 投影；契约 x-hunter-history-archive）。

- 数据源：``hunter-video`` 桶 sidecar JSON（**不新增数据库表**，契约 x-hunter-history-archive）；
- 列表：对象键前缀检索 → 逐条解析 sidecar → 内存过滤 → started_at 降序分页
  （契约 311-330 行：无 DB 表，total 为解析出的 sidecar 数量，非 DB COUNT）；
- 时间过滤：对象键日期前缀粗筛 + sidecar.started_at 精筛（契约 320-321 行）；
  started_from/started_to 跨度 > RC_HISTORY_QUERY_MAX_RANGE_DAYS → 2001（契约 323 行）；
- 数据权限：普通用户强制 operator_id=自身（契约 324-325 行，由路由层收敛传入）；
- 预签名：下载有效期 rc_presign_get_ttl_s（默认 900s，契约上限 15 分钟，不可调高）；
  URL 不落库、不落日志（契约 413 行 data_protection，仅记录对象键）。
"""

from __future__ import annotations

from typing import Any, Literal, cast

from hunter_common.exceptions import InvalidParameterError, ResourceNotFoundError
from hunter_common.logging import get_logger

from app.config import Settings
from app.repositories.storage import KEY_PREFIX, VideoArchiveStorage
from app.schemas.common import SessionEndReason
from app.schemas.history import (
    ControlHistoryDetail,
    ControlHistoryItem,
    ControlHistoryList,
    ControlVideoAccess,
)

logger = get_logger("app.services.history_service")

SECONDS_PER_DAY = 86400


class HistoryService:
    """操控历史查询服务（sidecar 投影 + 录像预签名访问）。"""

    def __init__(self, *, storage: VideoArchiveStorage, settings: Settings) -> None:
        self._storage = storage
        self._settings = settings

    # ---------- 列表（GET /history；契约 listControlHistory） ----------
    async def list_history(
        self,
        *,
        vehicle_id: str | None = None,
        operator_id: str | None = None,
        started_from: float | None = None,
        started_to: float | None = None,
        end_reason: SessionEndReason | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> ControlHistoryList:
        """列举操控记录（sidecar 投影；started_at 降序；过滤项见契约 333-357 行）。"""
        self._validate_range(started_from, started_to)
        # vehicle_id 过滤映射到对象键前缀（契约 336 行）；否则全桶 remote-control/ 前缀
        prefix = f"{KEY_PREFIX}/{vehicle_id}/" if vehicle_id else f"{KEY_PREFIX}/"
        pairs = await self._storage.list_sidecars(prefix)
        items: list[ControlHistoryItem] = []
        for _key, doc in pairs:
            item = self._project_item(doc)
            if item is None:  # 脏数据/解析失败：跳过并告警（不阻断整页）
                continue
            if operator_id is not None and item.operator_id != operator_id:
                continue
            if started_from is not None and item.started_at < started_from:
                continue
            if started_to is not None and item.started_at > started_to:
                continue
            if end_reason is not None and item.end_reason != end_reason:
                continue
            items.append(item)
        items.sort(key=lambda item: item.started_at, reverse=True)
        total = len(items)
        start = (page - 1) * page_size
        return ControlHistoryList(
            items=items[start : start + page_size],
            total=total,
            page=page,
            page_size=page_size,
        )

    # ---------- 详情（GET /history/{session_id}；契约 getControlHistoryDetail） ----------
    async def get_history_detail(self, session_id: str) -> ControlHistoryDetail:
        """单场操控记录详情（sidecar 原文 + video_available）。"""
        doc, video_key = await self._find_sidecar(session_id)
        item = self._project_item(doc)
        if item is None:
            # sidecar 结构损坏：按资源不存在处理（契约 398 行 404/3001）
            raise ResourceNotFoundError(
                message=f"操控记录 {session_id} 归档元信息损坏或不存在"
            )
        video_available = await self._storage.head_video(video_key) is not None
        return ControlHistoryDetail(
            **item.model_dump(), sidecar=doc, video_available=video_available
        )

    # ---------- 录像访问（GET /history/{session_id}/video；契约 getControlVideo） ----------
    async def get_video_access(self, session_id: str) -> ControlVideoAccess:
        """签发录像下载预签名 URL（有效期 900s；对象缺失 → 3001，契约 412 行）。"""
        _doc, video_key = await self._find_sidecar(session_id)
        size = await self._storage.head_video(video_key)
        if size is None:
            # 归档未完成或已过 90 天生命周期回收 → 3001（契约 412 行）
            raise ResourceNotFoundError(
                message=f"操控录像 {session_id} 不存在（未封存或已过保留期）"
            )
        expires_in = self._settings.rc_presign_get_ttl_s
        url = await self._storage.presign_get(video_key, expires_in)
        logger.info(
            "video_presigned",
            session_id=session_id,
            object_key=video_key,
            expires_in=expires_in,
        )
        return ControlVideoAccess(
            video_url=url,
            # 契约 Literal[900]（15 分钟上限，RC_PRESIGN_GET_TTL_S 校验器保证 ≤900）；
            # 配置偏离 900 时由模型校验失败快速暴露
            expires_in=cast("Literal[900]", expires_in),
            size_bytes=size,
            duration_s=None,  # 时长以 sidecar.duration_s 为准（容器级探测不引入额外 IO）
            etag=None,  # ETag 由对象存储响应头携带，head 元数据接口未暴露时为 null
        )

    # ---------- 内部：辅助 ----------
    def _validate_range(
        self, started_from: float | None, started_to: float | None
    ) -> None:
        """时间范围校验：跨度 > RC_HISTORY_QUERY_MAX_RANGE_DAYS → 2001（契约 323 行）。"""
        if started_from is None or started_to is None:
            return
        if started_to < started_from:
            raise InvalidParameterError(message="started_to 不得早于 started_from")
        max_days = self._settings.rc_history_query_max_range_days
        if started_to - started_from > max_days * SECONDS_PER_DAY:
            raise InvalidParameterError(message=f"时间范围跨度不得超过 {max_days} 天")

    async def _find_sidecar(self, session_id: str) -> tuple[dict[str, Any], str]:
        """按 session_id 检索 sidecar（返回 (doc, video_object_key)；缺失 → 3001）。

        无 DB 表的定位方式：全桶前缀检索 *.json 逐条比对（契约 383-385 行；
        pending #8 记录 ListObjectsV2 检索成本风险，靠日期前缀收敛）。
        """
        pairs = await self._storage.list_sidecars(f"{KEY_PREFIX}/")
        for _key, doc in pairs:
            if isinstance(doc, dict) and doc.get("session_id") == session_id:
                video = doc.get("video")
                video_key = (
                    video.get("object_key", "") if isinstance(video, dict) else ""
                )
                return doc, video_key
        raise ResourceNotFoundError(message=f"操控记录 {session_id} 不存在或已过保留期")

    def _project_item(self, doc: Any) -> ControlHistoryItem | None:
        """sidecar dict → ControlHistoryItem 投影（解析失败返回 None 并告警，不阻断整页）。"""
        if not isinstance(doc, dict):
            logger.warning("sidecar_doc_invalid", reason="not_a_dict")
            return None
        raw_video = doc.get("video")
        video: dict[str, Any] = raw_video if isinstance(raw_video, dict) else {}
        raw_control = doc.get("control")
        control: dict[str, Any] = raw_control if isinstance(raw_control, dict) else {}
        try:
            return ControlHistoryItem(
                session_id=doc.get("session_id", ""),
                vehicle_id=doc.get("vehicle_id", ""),
                operator_id=doc.get("operator_id", ""),
                operator_name=doc.get("operator_name") or None,
                started_at=doc.get("started_at", 0.0),
                ended_at=doc.get("ended_at", 0.0),
                duration_s=doc.get("duration_s", 0),
                end_reason=doc.get("end_reason", "operator_end"),
                end_reason_source=doc.get("end_reason_source") or None,
                commands_sent=control.get("commands_sent"),
                commands_acked=control.get("commands_acked"),
                ack_latency_ms_avg=control.get("ack_latency_ms_avg"),
                ack_latency_ms_p95=control.get("ack_latency_ms_p95"),
                timeout_events=control.get("timeout_events"),
                # sidecar 未存 bitrate 上下限 → video_config 置 None（契约 optional）
                video_config=None,
                video_object_key=video.get("object_key", ""),
                video_size_bytes=video.get("size_bytes"),
                video_sha256=video.get("sha256") or None,
                sidecar_object_key="",  # 列表投影无对象键上下文；详情以 video.object_key 定位
                note=doc.get("note") or None,
            )
        except Exception:  # noqa: BLE001 — 单条 sidecar 脏数据不阻断整页查询
            logger.warning("sidecar_parse_failed", session_id=doc.get("session_id"))
            return None


__all__ = ["HistoryService"]
