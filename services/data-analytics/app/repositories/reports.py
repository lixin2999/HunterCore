"""报告元信息仓库：MinIO sidecar JSON（无 DB 表，见契约 x-hunter-report-storage）。

布局：hunter-reports/reports/<report_type>/<report_id>.json
sidecar 结构见 components.schemas.ReportMeta（本服务自身写入，离线作业续写/完成）。
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from hunter_common.logging import get_logger

from app.repositories.storage import ObjectStorage
from app.schemas.reports import REPORT_TYPE_KEYS, ReportStatus

logger = get_logger("app.repositories.reports")


@dataclass(frozen=True, slots=True)
class ReportListFilter:
    """报告列表过滤参数（与 GET /reports Query 一一对应）。"""

    report_type: str | None = None
    vehicle_id: str | None = None
    status: str | None = None
    start_time: float | None = None
    end_time: float | None = None
    page: int = 1
    page_size: int = 20


class ReportRepository:
    """报告 sidecar 仓储（前缀扫描聚合；分页在服务侧内存完成）。"""

    #: 单次全量扫描的对象数上限（保护 P95 ≤ 200ms；报告量增长后应迁移 DB 表，见风险说明）
    SCAN_LIMIT = 2000

    def __init__(self, storage: ObjectStorage, *, prefix: str = "reports") -> None:
        self._storage = storage
        self._prefix = prefix.strip("/")

    def sidecar_key(self, report_type: str, report_id: str) -> str:
        """sidecar 对象键（契约布局 reports/<report_type>/<report_id>.json）。"""
        return f"{self._prefix}/{report_type}/{report_id}.json"

    @property
    def storage(self) -> ObjectStorage:
        """底层对象存储（详情预签名复用）。"""
        return self._storage

    async def save(self, meta: Mapping[str, Any]) -> None:
        """写入/覆盖 sidecar（提交入队时创建，离线作业完成时更新）。"""
        report_type = str(meta["report_type"])
        report_id = str(meta["report_id"])
        await self._storage.put_json(self.sidecar_key(report_type, report_id), dict(meta))

    async def _scan_all(self) -> list[dict[str, Any]]:
        """扫描全部报告 sidecar（5 类模板前缀合并）。"""
        docs: list[dict[str, Any]] = []
        for report_type in REPORT_TYPE_KEYS:
            # list_json 返回 (key, doc) 元组序列，仅取文档部分
            docs.extend(doc for _key, doc in await self._storage.list_json(f"{self._prefix}/{report_type}/"))
            if len(docs) >= self.SCAN_LIMIT:
                logger.warning("report_sidecar_scan_truncated", limit=self.SCAN_LIMIT)
                break
        return docs

    async def get(self, report_id: str) -> dict[str, Any] | None:
        """按 report_id 读取 sidecar（跨 5 类模板前缀查找）。"""
        for report_type in REPORT_TYPE_KEYS:
            doc = await self._storage.get_json(self.sidecar_key(report_type, report_id))
            if doc is not None:
                return doc
        return None

    @staticmethod
    def _matches(meta: Mapping[str, Any], flt: ReportListFilter) -> bool:
        """列表过滤：类型/车辆/状态/窗口重叠（报告窗口与过滤窗口相交）。"""
        if flt.report_type is not None and meta.get("report_type") != flt.report_type:
            return False
        if flt.vehicle_id is not None and meta.get("vehicle_id") != flt.vehicle_id:
            return False
        if flt.status is not None and meta.get("status") != flt.status:
            return False
        if flt.start_time is not None and flt.end_time is not None:
            meta_start = meta.get("start_time")
            meta_end = meta.get("end_time")
            if meta_start is None or meta_end is None:
                return False
            if float(meta_end) < flt.start_time or float(meta_start) > flt.end_time:
                return False
        return True

    async def list(
        self, flt: ReportListFilter
    ) -> tuple[list[dict[str, Any]], int, int, int]:
        """返回 (页内元信息列表, 总数, page, page_size)，按 created_at DESC 排序。"""
        docs = [d for d in await self._scan_all() if self._matches(d, flt)]
        docs.sort(key=lambda d: (float(d.get("created_at") or 0.0), str(d.get("report_id"))), reverse=True)
        total = len(docs)
        start = (flt.page - 1) * flt.page_size
        return docs[start : start + flt.page_size], total, flt.page, flt.page_size

    async def count_in_flight(self) -> int:
        """生成中（pending/generating）报告数（并发守卫用）。"""
        in_flight = {ReportStatus.PENDING.value, ReportStatus.GENERATING.value}
        return sum(1 for d in await self._scan_all() if d.get("status") in in_flight)

    async def has_duplicate_in_flight(
        self, *, report_type: str, vehicle_id: str | None, start_time: float, end_time: float
    ) -> bool:
        """同参数（模板+车辆+窗口）报告是否已在生成中（防重复提交）。"""
        in_flight = {ReportStatus.PENDING.value, ReportStatus.GENERATING.value}
        for doc in await self._scan_all():
            if doc.get("status") not in in_flight:
                continue
            if doc.get("report_type") != report_type:
                continue
            if doc.get("vehicle_id") != vehicle_id:
                continue
            if doc.get("start_time") != start_time or doc.get("end_time") != end_time:
                continue
            return True
        return False