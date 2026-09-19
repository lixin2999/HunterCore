"""版本仓库业务服务（版本查询 / 创建 / 发布五项校验 / 退役）。

契约：POST /versions（两步式预签名直传）、POST /versions/{id}/publish（唯一发布门禁：
单次流式 size/MD5/SHA-256 + RSA-2048 验签 + version_code 单调递增，x-hunter-ota-security）、
POST /versions/{id}/deprecate（published 之外 → 3003；running/paused 任务引用 → 3003）。
"""
from __future__ import annotations

import asyncio
import time
from uuid import UUID

from hunter_common.database.enums import OtaVersionStatus
from hunter_common.database.models import OtaVersion
from hunter_common.exceptions import (
    OtaPackageChecksumError,
    OtaPreconditionError,
    OtaSignatureError,
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
    ResourceStateConflictError,
    ServiceUnavailableError,
)
from hunter_common.logging import get_logger, get_trace_id

from app.config import Settings
from app.core.metrics import OTA_PACKAGE_VERIFY_FAILURES
from app.core.signature import verify_package_signature
from app.repositories.storage import OtaPackageStorage
from app.repositories.versions import OtaVersionRepository
from app.schemas.versions import (
    OtaVersionCreateData,
    OtaVersionCreateRequest,
    OtaVersionDeprecateRequest,
    OtaVersionDetail,
    OtaVersionItem,
    OtaVersionList,
    OtaVersionPublishChecks,
    OtaVersionPublishData,
    OtaVersionUploadInfo,
    OtaVersionUploadPart,
)

logger = get_logger("app.services.versions")

#: 对象键命名模板（x-hunter-version-upload-flow.object_key_pattern，契约固定）
OBJECT_KEY_TEMPLATE = "hunter-core/ota/{model}/{version_name}/{version_code}/package.tar.gz"

#: 发布校验失败归因（Prometheus reason 标签）
_VERIFY_REASON_SIZE = "size"
_VERIFY_REASON_MD5 = "md5"
_VERIFY_REASON_SHA256 = "sha256"
_VERIFY_REASON_SIGNATURE = "signature"
_VERIFY_REASON_MONOTONIC = "monotonic"


class VersionService:
    """版本仓库业务逻辑（repository → 契约响应组装 + MinIO 流式校验）。"""

    def __init__(
        self,
        repository: OtaVersionRepository,
        storage: OtaPackageStorage,
        settings: Settings,
    ) -> None:
        self._repository = repository
        self._storage = storage
        self._settings = settings

    # ---------- 查询 ----------

    async def list_versions(
        self,
        *,
        status: OtaVersionStatus | None = None,
        release_type: str | None = None,
        version_code: int | None = None,
        applicable_model: str | None = None,
        version_name: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> OtaVersionList:
        """版本列表（release_time DESC NULLS LAST, version_code DESC；即时签发下载地址）。"""
        rows, total = await self._repository.list_page(
            status=status,
            release_type=release_type,
            version_code=version_code,
            applicable_model=applicable_model,
            version_name=version_name,
            page=page,
            page_size=page_size,
        )
        items = await asyncio.to_thread(self._to_items_with_download_url, rows)
        return OtaVersionList(items=items, total=total, page=page, page_size=page_size)

    async def get_version(self, version_id: UUID) -> OtaVersionDetail:
        """版本详情（404：不存在；含 task_count 与即时下载地址）。"""
        row = await self._repository.get(version_id)
        if row is None:
            raise ResourceNotFoundError(details={"version_id": str(version_id)})
        item = self._to_item(row)
        item.package_download_url = await asyncio.to_thread(
            self._storage.presign_get,
            self._settings.minio_bucket_ota_packages,
            self._object_key_of(row),
        )
        task_count = await self._repository.count_tasks_for_version(version_id)
        return OtaVersionDetail(**item.model_dump(), task_count=task_count)


    # ---------- 创建（两步式第 1 步） ----------

    async def create_version(
        self, payload: OtaVersionCreateRequest, user_id: str
    ) -> OtaVersionCreateData:
        """创建草稿 + 签发上传预签名地址（唯一性 3002 / 防回滚 6003）。"""
        if await self._repository.get_by_name(payload.version_name) is not None:
            raise ResourceAlreadyExistsError(
                details={"field": "version_name", "value": payload.version_name}
            )
        if await self._repository.get_by_code(payload.version_code) is not None:
            raise ResourceAlreadyExistsError(
                details={"field": "version_code", "value": payload.version_code}
            )
        max_code = await self._repository.max_published_code(payload.applicable_models)
        if max_code is not None and payload.version_code <= max_code:
            raise OtaPreconditionError(
                message=(
                    f"version_code 必须大于同车型已发布最大编码 {max_code}"
                    "（6003 防回滚：版本单调递增门禁）"
                ),
                details={
                    "field": "version_code",
                    "max_published": max_code,
                    "value": payload.version_code,
                },
            )

        object_key = OBJECT_KEY_TEMPLATE.format(
            model=payload.applicable_models[0],
            version_name=payload.version_name,
            version_code=payload.version_code,
        )
        row = OtaVersion(
            version_name=payload.version_name,
            version_code=payload.version_code,
            release_type=payload.release_type,
            package_url=f"s3://{self._settings.minio_bucket_ota_packages}/{object_key}",
            package_size=payload.package_size,
            package_md5=payload.package_md5,
            package_sha256=payload.package_sha256,
            signature=payload.signature,
            changelog=payload.changelog,
            applicable_models=list(payload.applicable_models),
            status=OtaVersionStatus.DRAFT,
        )
        created = await self._repository.create(row)
        upload = await asyncio.to_thread(self._presign_upload, object_key, payload.part_count)
        # 审计日志（x-hunter-ota-security.audit：user_id/version_code/object_key/trace_id，脱敏）
        logger.info(
            "ota_version_created",
            user_id=user_id,
            version_code=created.version_code,
            version_name=created.version_name,
            object_key=object_key,
            trace_id=get_trace_id(),
        )
        return OtaVersionCreateData(version=self._to_item(created), upload=upload)

    def _presign_upload(self, object_key: str, part_count: int) -> OtaVersionUploadInfo:
        """签发上传地址（part_count>1 走 multipart 分片，服务端不代理 GB 级流量）。"""
        bucket = self._settings.minio_bucket_ota_packages
        if part_count > 1:
            upload_id = self._storage.create_multipart_upload(bucket, object_key)
            parts = [
                OtaVersionUploadPart(
                    part_number=part_number,
                    upload_url=self._storage.presign_upload_part(
                        bucket, object_key, upload_id, part_number
                    ),
                )
                for part_number in range(1, part_count + 1)
            ]
            return OtaVersionUploadInfo(
                object_key=object_key,
                upload_url=parts[0].upload_url,
                part_count=part_count,
                parts=parts,
            )
        return OtaVersionUploadInfo(
            object_key=object_key,
            upload_url=self._storage.presign_put(bucket, object_key),
            part_count=1,
            parts=None,
        )


    # ---------- 发布（两步式第 2 步，唯一发布门禁） ----------

    async def publish_version(
        self, version_id: UUID, note: str | None, user_id: str
    ) -> OtaVersionPublishData:
        """发布：五项校验全部通过才置 published（6001/6002/6003），非 draft → 3003。"""
        row = await self._require_version(version_id)
        if row.status != OtaVersionStatus.DRAFT:
            raise ResourceStateConflictError(
                details={"current_status": row.status.value, "allowed_status": ["draft"]}
            )
        bucket = self._settings.minio_bucket_ota_packages
        object_key = self._object_key_of(row)
        checks = OtaVersionPublishChecks(
            package_size_verified=False,
            md5_verified=False,
            sha256_verified=False,
            signature_verified=False,
            version_code_monotonic=False,
        )
        metadata = await asyncio.to_thread(self._storage.head_object, bucket, object_key)
        if metadata is None:
            OTA_PACKAGE_VERIFY_FAILURES.labels(_VERIFY_REASON_SIZE).inc()
            raise OtaPackageChecksumError(
                message="升级包对象不存在（6001），请先完成直传",
                details={"field": "package_size", "expected": row.package_size, "actual": None},
            )
        if metadata["size_bytes"] != row.package_size:
            OTA_PACKAGE_VERIFY_FAILURES.labels(_VERIFY_REASON_SIZE).inc()
            raise OtaPackageChecksumError(
                message=(
                    f"包大小校验失败（6001）：声明 {row.package_size} 字节，"
                    f"对象实际 {metadata['size_bytes']} 字节"
                ),
                details={
                    "field": "package_size",
                    "expected": row.package_size,
                    "actual": metadata["size_bytes"],
                },
            )
        checks.package_size_verified = True

        size, md5_hex, sha256_hex = await asyncio.to_thread(
            self._storage.stream_hashes, bucket, object_key
        )
        if size != row.package_size:
            OTA_PACKAGE_VERIFY_FAILURES.labels(_VERIFY_REASON_SIZE).inc()
            raise OtaPackageChecksumError(
                message=f"流式读取长度与声明不符（6001）：{size} != {row.package_size}",
                details={"field": "package_size", "expected": row.package_size, "actual": size},
            )
        if md5_hex != row.package_md5:
            OTA_PACKAGE_VERIFY_FAILURES.labels(_VERIFY_REASON_MD5).inc()
            raise OtaPackageChecksumError(
                message="MD5 校验失败（6001）",
                details={"field": "package_md5", "expected": row.package_md5, "actual": md5_hex},
            )
        checks.md5_verified = True
        # 审查 Y3：校验开关关闭时**如实**回报（此前无论是否校验都置 True，构成假声明）
        checks.sha256_verified = self._settings.ota_package_verify_sha256
        if checks.sha256_verified and sha256_hex != row.package_sha256:
            OTA_PACKAGE_VERIFY_FAILURES.labels(_VERIFY_REASON_SHA256).inc()
            raise OtaPackageChecksumError(
                message="SHA-256 校验失败（6001）",
                details={"field": "package_sha256", "expected": row.package_sha256, "actual": sha256_hex},
            )

        checks.signature_verified = self._settings.ota_package_verify_signature
        if checks.signature_verified:
            try:
                signature_ok = await asyncio.to_thread(
                    verify_package_signature,
                    self._settings.ota_signature_public_key,
                    row.signature,
                    sha256_hex,
                )
            except ServiceUnavailableError:
                OTA_PACKAGE_VERIFY_FAILURES.labels(_VERIFY_REASON_SIGNATURE).inc()
                raise
            if not signature_ok:
                OTA_PACKAGE_VERIFY_FAILURES.labels(_VERIFY_REASON_SIGNATURE).inc()
                raise OtaSignatureError(details={"algorithm": "RSASSA-PKCS1-v1_5_SHA256"})
        else:
            # 开关关闭属于高危配置（契约 pending：开关是否允许关闭）：显式告警留痕
            logger.warning(
                "ota_package_verify_disabled",
                version_id=str(version_id),
                sha256_enabled=self._settings.ota_package_verify_sha256,
                signature_enabled=False,
            )

        max_code = await self._repository.max_published_code(list(row.applicable_models))
        if max_code is not None and row.version_code <= max_code:
            OTA_PACKAGE_VERIFY_FAILURES.labels(_VERIFY_REASON_MONOTONIC).inc()
            raise OtaPreconditionError(
                message=(
                    f"version_code {row.version_code} 未大于同车型已发布最大编码 {max_code}"
                    "（6003 防回滚）"
                ),
                details={"field": "version_code", "max_published": max_code},
            )
        checks.version_code_monotonic = True

        release_time = time.time()
        await self._repository.set_status(
            version_id, status=OtaVersionStatus.PUBLISHED, release_time=release_time
        )
        logger.info(
            "ota_version_published",
            user_id=user_id,
            version_id=str(version_id),
            version_code=row.version_code,
            package_sha256=row.package_sha256,
            note=note,
            trace_id=get_trace_id(),
        )
        return OtaVersionPublishData(
            version_id=version_id,
            version_code=row.version_code,
            status=OtaVersionStatus.PUBLISHED,
            release_time=release_time,
            checks=checks,
        )

    # ---------- 退役 ----------

    async def deprecate_version(
        self, version_id: UUID, payload: OtaVersionDeprecateRequest, user_id: str
    ) -> OtaVersionDetail:
        """退役：published 之外 → 3003；存在 running/paused 任务引用 → 3003（先处置任务）。"""
        row = await self._require_version(version_id)
        if row.status != OtaVersionStatus.PUBLISHED:
            raise ResourceStateConflictError(
                details={"current_status": row.status.value, "allowed_status": ["published"]}
            )
        active_tasks = await self._repository.count_active_tasks_for_version(version_id)
        if active_tasks:
            raise ResourceStateConflictError(
                details={
                    "current_status": row.status.value,
                    "active_tasks": active_tasks,
                    "reason": "存在 running/paused 任务引用该版本，须先处置任务",
                }
            )
        target = OtaVersionStatus(payload.status)
        await self._repository.set_status(version_id, status=target)
        logger.info(
            "ota_version_deprecated",
            user_id=user_id,
            version_id=str(version_id),
            status=target.value,
            reason=payload.reason,
            trace_id=get_trace_id(),
        )
        return await self.get_version(version_id)

    # ---------- 内部工具 ----------

    async def _require_version(self, version_id: UUID) -> OtaVersion:
        """按 ID 取版本，缺失抛 3001。"""
        row = await self._repository.get(version_id)
        if row is None:
            raise ResourceNotFoundError(details={"version_id": str(version_id)})
        return row

    @staticmethod
    def _object_key_of(row: OtaVersion) -> str:
        """从 package_url（s3://{bucket}/{key}）还原对象键。"""
        prefix = "s3://"
        url = row.package_url
        if url.startswith(prefix):
            return url[len(prefix) :].split("/", 1)[1]
        return url

    def _to_items_with_download_url(self, rows: list[OtaVersion]) -> list[OtaVersionItem]:
        """行 → 契约模型 + 即时签发下载地址（单线程批量预签名，避免逐条 to_thread）。"""
        items = []
        for row in rows:
            item = self._to_item(row)
            item.package_download_url = self._storage.presign_get(
                self._settings.minio_bucket_ota_packages, self._object_key_of(row)
            )
            items.append(item)
        return items

    @staticmethod
    def _to_item(row: OtaVersion) -> OtaVersionItem:
        """ORM 行 → OtaVersionItem（TIMESTAMPTZ → epoch 秒；draft 的 release_time 为 null）。"""
        return OtaVersionItem(
            version_id=row.version_id,
            version_name=row.version_name,
            version_code=row.version_code,
            release_type=row.release_type,
            package_url=row.package_url,
            package_size=row.package_size,
            package_md5=row.package_md5,
            package_sha256=row.package_sha256,
            signature=row.signature,
            changelog=row.changelog,
            applicable_models=list(row.applicable_models),
            status=row.status,
            release_time=row.release_time.timestamp() if row.release_time else None,
            package_download_url=None,
        )


__all__ = ["VersionService"]
