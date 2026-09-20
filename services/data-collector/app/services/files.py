"""文件上传服务（5.5 节：预签名直传 + 完整性校验 + sensor_file 通知）。

流程契约（x-hunter-file-upload-flow / x-hunter-pending-confirmation.p1/p2）：
- presign：data_type→Bucket 映射 + 5.5 节命名规范生成对象路径（服务端生成，客户端不可指定）；
  对象路径含 `{timestamp}_{seq}` 段防覆盖（p1 已确认），seq 为秒内自增计数器；
- complete：head_object 定位对象 → **流式重算 size/MD5/SHA-256**（三者必须与声明值全部一致，
  任一不符 → 6001 / HTTP 422，审查 R2 修复）→ multipart 先合并分片再校验 →
  hunter-rosbag 对象打生命周期归类 Tag（G-12，hunter-retention=regular|event，
  打标失败 → 5001 不通知下游）→ 发布 sensor_file（key=vehicle_id，载荷契约固定）；
- list：prefix 列举 + marker 游标分页，即时签发 15 分钟下载 URL。
"""
from __future__ import annotations

import asyncio
import itertools
import re
import time
from typing import Any

import structlog
from hunter_common.exceptions import (
    InvalidParameterError,
    OtaPackageChecksumError,
    PermissionDeniedError,
    ResourceNotFoundError,
    ServiceUnavailableError,
)

from app.config import settings
from app.producers.sensor_file import SensorFileProducer
from app.repositories.storage import MinioStorage
from app.schemas.common import (
    OBJECT_KEY_TEMPLATE,
    SENSOR_FILE_TOPIC,
    FileDataType,
    UploadMethod,
)
from app.schemas.files import (
    FileCompleteData,
    FileCompleteRequest,
    FileListData,
    FileObjectItem,
    FilePresignData,
    FilePresignPart,
    FilePresignRequest,
)

logger = structlog.get_logger(service="data-collector")

_PARAM_ERROR = 2001

#: 允许的对象扩展名（5.5 节命名规范；未知扩展名 → other）
KNOWN_EXTENSIONS: dict[str, str] = {
    ".pcd": "point_cloud",
    ".bag": "rosbag",
    ".mp4": "video",
    ".jpg": "camera_image",
    ".jpeg": "camera_image",
    ".png": "camera_image",
}

#: 扩展名 → MIME（content_type 缺省时推断）
MIME_BY_EXTENSION: dict[str, str] = {
    ".pcd": "application/octet-stream",
    ".bag": "application/ros-bag",
    ".mp4": "video/mp4",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
}

#: 分片 complete 请求参数缺失的校验（multipart 必须回带 upload_id + parts）
_DATE_LINE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class FileService:
    """文件上传业务逻辑（MinIO 仓储 + sensor_file 生产者）。"""

    def __init__(self, storage: MinioStorage, producer: SensorFileProducer) -> None:
        self._storage = storage
        self._producer = producer
        self._seq_counter = itertools.count(start=1)

    # ---------- presign ----------

    async def presign(self, payload: FilePresignRequest) -> FilePresignData:
        """预签名（契约 POST /files/presign 唯一入口）。

        - data_type 必须落在请求 bucket 的合法映射内（1002：Bucket 与数据类型不匹配）；
        - part_count > 1 → multipart（upload_id + parts[]），否则 put 单地址。
        """
        self._validate_bucket_mapping(payload.bucket.value, payload.data_type.value)
        bucket = payload.bucket.value
        key = await asyncio.to_thread(self._build_object_key, payload)
        content_type = payload.content_type or MIME_BY_EXTENSION.get(
            self._extension_of(payload.file_name), "application/octet-stream"
        )
        if payload.part_count > 1:
            upload_id = await asyncio.to_thread(
                self._storage.create_multipart_upload, bucket, key, content_type
            )
            parts = [
                FilePresignPart(
                    part_number=part_number,
                    upload_url=await asyncio.to_thread(
                        self._storage.presign_upload_part,
                        bucket,
                        key,
                        upload_id,
                        part_number,
                    ),
                )
                for part_number in range(1, payload.part_count + 1)
            ]
            return FilePresignData(
                bucket=payload.bucket,
                object_key=key,
                method=UploadMethod.MULTIPART,
                upload_id=upload_id,
                parts=parts,
                expires_in=settings.presigned_upload_expire_seconds,
            )
        upload_url = await asyncio.to_thread(
            self._storage.presign_put, bucket, key, content_type
        )
        return FilePresignData(
            bucket=payload.bucket,
            object_key=key,
            method=UploadMethod.PUT,
            upload_url=upload_url,
            expires_in=settings.presigned_upload_expire_seconds,
        )

    def _validate_bucket_mapping(self, bucket: str, data_type: str) -> None:
        """Bucket 与 data_type 映射校验（x-hunter-file-upload-flow.bucket_mapping）。

        不匹配 → 1002（资源归属错误，非 2001：请求格式合法但 Bucket 语义不符）。
        """
        if settings.data_type_bucket_mapping.get(data_type) != bucket:
            raise PermissionDeniedError(
                message=(
                    f"Bucket 与数据类型不匹配（data_type={data_type} 应归属 "
                    f"{settings.data_type_bucket_mapping.get(data_type)}）"
                )
            )

    def _build_object_key(self, payload: FilePresignRequest) -> str:
        """生成对象路径（5.5 节命名规范；{timestamp}_{seq} 防覆盖，p1 已确认）。"""
        timestamp = time.time()
        extension = self._extension_of(payload.file_name)
        return OBJECT_KEY_TEMPLATE.format(
            vehicle_id=payload.vehicle_id,
            date=time.strftime("%Y-%m-%d", time.gmtime(timestamp)),
            data_type=payload.data_type.value,
            timestamp=int(timestamp),
            seq=next(self._seq_counter),
            ext=extension,
        )

    @staticmethod
    def _extension_of(file_name: str) -> str:
        """取扩展名（小写；未知扩展名 → other 语义，保留原始后缀）。"""
        suffix = file_name[file_name.rfind(".") :].lower() if "." in file_name else ""
        return suffix if suffix in KNOWN_EXTENSIONS or suffix else ".bin"

    # ---------- complete ----------

    async def complete(
        self, payload: FileCompleteRequest, user_id: str
    ) -> FileCompleteData:
        """上传完成校验（契约 POST /files/complete 唯一入口）。

        步骤（x-hunter-file-upload-flow.complete，全部满足才发通知）：
        1. 对象路径必须归属请求车辆目录（前缀 {bucket}/{vehicle_id}/，否则 1002）；
        2. multipart：先合并分片（缺 upload_id/parts → 2001，非法会话中止并回滚）；
        3. head_object：对象不存在 → 3001；
        4. 流式重算 size/md5/sha256 与声明值三者一致（任一不符 → 6001，HTTP 422）；
        5. hunter-rosbag：校验通过后打 Tag hunter-retention=<retention>（G-12，失败 → 5001）；
        6. 校验通过发布 sensor_file（key=vehicle_id；Kafka 故障 → 5001）。
        """
        self._validate_vehicle_prefix(
            vehicle_id=payload.vehicle_id, bucket=payload.bucket.value, object_key=payload.object_key
        )
        if payload.upload_id or payload.parts:
            if not (payload.upload_id and payload.parts):
                raise InvalidParameterError(
                    message="multipart 上传必须同时提供 upload_id 与 parts（2001 参数错误）"
                )
            await self._merge_multipart(payload)

        metadata = await asyncio.to_thread(
            self._storage.head_object, payload.bucket.value, payload.object_key
        )
        if metadata is None:
            raise ResourceNotFoundError(
                message=(
                    f"对象不存在或尚未上传完成（bucket={payload.bucket.value}, "
                    f"object_key={payload.object_key}）"
                )
            )
        await self._verify_integrity(payload, metadata)
        await self._apply_retention_tag(payload)

        data_type = self._resolve_data_type(payload)
        await self._producer.publish(
            vehicle_id=payload.vehicle_id,
            timestamp=payload.timestamp,
            bucket=payload.bucket.value,
            object_key=payload.object_key,
            data_type=data_type,
            size_bytes=metadata["size_bytes"],
            md5=payload.md5,
            sha256=payload.sha256,
        )
        # 审计日志：操作者 user_id（网关注入）记录入库级日志（日志脱敏：不记 hash 值本身以外的敏感信息）
        logger.info(
            "file_upload_completed",
            vehicle_id=payload.vehicle_id,
            bucket=payload.bucket.value,
            object_key=payload.object_key,
            data_type=data_type,
            size_bytes=metadata["size_bytes"],
            operator=user_id,
        )
        return FileCompleteData(
            bucket=payload.bucket,
            object_key=payload.object_key,
            size_bytes=metadata["size_bytes"],
            md5=payload.md5,
            sha256=payload.sha256,
            data_type=data_type,
            verified=True,
            notify_topic=SENSOR_FILE_TOPIC,
            timestamp=payload.timestamp,
        )

    async def _merge_multipart(self, payload: FileCompleteRequest) -> None:
        """合并分片；失败时中止服务端会话（回滚），要求客户端重新获取预签名。"""
        assert payload.upload_id is not None and payload.parts is not None
        parts = [
            {"part_number": part.part_number, "etag": self._clean_etag(part.etag)}
            for part in payload.parts
        ]
        try:
            await asyncio.to_thread(
                self._storage.complete_multipart_upload,
                payload.bucket.value,
                payload.object_key,
                payload.upload_id,
                parts,
            )
        except Exception as exc:
            await asyncio.to_thread(
                self._storage.abort_multipart_upload,
                payload.bucket.value,
                payload.object_key,
                payload.upload_id,
            )
            raise InvalidParameterError(
                message=f"分片合并失败（upload_id={payload.upload_id}）：{exc}"
            ) from exc

    @staticmethod
    def _validate_vehicle_prefix(vehicle_id: str, bucket: str, object_key: str) -> None:
        """对象路径归属校验（x-hunter-file-upload-flow.complete 第 3 步；不符 → 1002）。"""
        if not object_key.startswith(f"{vehicle_id}/"):
            raise PermissionDeniedError(
                message=(
                    f"对象路径必须归属本车辆目录（期望前缀 {bucket}/{vehicle_id}/，"
                    "实际对象路径与其不匹配）"
                )
            )

    @staticmethod
    def _clean_etag(etag: str) -> str:
        """分片 ETag 归一化（客户端可能回带引号）。"""
        return etag.strip('"')

    async def _verify_integrity(
        self, payload: FileCompleteRequest, metadata: dict[str, Any]
    ) -> None:
        """完整性校验（x-hunter-file-upload-flow.complete 第 4 步；审查 R2 修复）。

        契约依据：``contracts/openapi/data-collector.yaml``（POST /files/complete）
        「完整性校验（``size_bytes`` / ``md5`` / ``sha256`` 三者必须全部一致，
        任一不符返回 6001）」。校验数据来自 ``MinioStorage.stream_hashes``：
        单次流式重算对象真实摘要（1 MiB 分块，GB 级 ROS Bag 不整包入内存），
        multipart 合并后的对象同样按内容校验（此前实现跳过 ETag/MD5 比对，
        sha256 从未参与校验，且仍返回 ``verified=True``，属契约违反 + 假声明）。

        差异语义：``size_bytes`` 先用对象元数据快速比对（省一次全量读取即失败），
        再用流式读取长度二次比对（元数据与实际内容一致性的最终依据）。
        """
        if payload.size_bytes != metadata["size_bytes"]:
            raise OtaPackageChecksumError(
                message=(
                    f"文件大小校验失败（6001）：声明 {payload.size_bytes} 字节，"
                    f"对象实际 {metadata['size_bytes']} 字节"
                ),
                details={
                    "field": "size_bytes",
                    "expected": payload.size_bytes,
                    "actual": metadata["size_bytes"],
                },
            )
        # 流式哈希为阻塞 IO（boto3）：经 to_thread 包装，禁止阻塞事件循环
        size, md5_hex, sha256_hex = await asyncio.to_thread(
            self._storage.stream_hashes, payload.bucket.value, payload.object_key
        )
        if size != payload.size_bytes:
            raise OtaPackageChecksumError(
                message=f"对象读取长度与声明不符（6001）：{size} != {payload.size_bytes}",
                details={"field": "size_bytes", "expected": payload.size_bytes, "actual": size},
            )
        if md5_hex != payload.md5:
            raise OtaPackageChecksumError(
                message="MD5 校验失败（6001）",
                details={"field": "md5", "expected": payload.md5, "actual": md5_hex},
            )
        if sha256_hex != payload.sha256:
            raise OtaPackageChecksumError(
                message="SHA-256 校验失败（6001）",
                details={"field": "sha256", "expected": payload.sha256, "actual": sha256_hex},
            )

    async def _apply_retention_tag(self, payload: FileCompleteRequest) -> None:
        """生命周期归类打标（G-12，x-hunter-file-upload-flow.tagging；仅 hunter-rosbag）。

        完整性校验通过后、sensor_file 发布前打 Tag ``hunter-retention=<retention>``；
        未打标对象等同永久保留（S3 tag 过滤器无法表达无 Tag），打标失败必须
        阻断发布（5001），禁止静默降级。
        """
        if payload.bucket.value != settings.minio_bucket_rosbag:
            return
        try:
            await asyncio.to_thread(
                self._storage.put_object_tags,
                payload.bucket.value,
                payload.object_key,
                payload.retention,
            )
        except Exception as exc:
            raise ServiceUnavailableError(
                message="rosbag 生命周期打标失败，已阻断 sensor_file 发布（G-12）",
                details={
                    "object_key": payload.object_key,
                    "retention": payload.retention,
                },
            ) from exc

    @staticmethod
    def _resolve_data_type(payload: FileCompleteRequest) -> str:
        """data_type 解析（请求显式指定优先；否则按对象路径 {data_type} 段解析）。"""
        if payload.data_type is not None:
            return payload.data_type.value
        segments = payload.object_key.split("/")
        for segment in segments:
            try:
                return FileDataType(segment).value
            except ValueError:
                continue
        raise InvalidParameterError(
            message=(
                f"无法从对象路径解析 data_type（object_key={payload.object_key}，"
                "请在请求中显式指定 data_type）"
            )
        )

    # ---------- list ----------

    async def list_files(
        self,
        vehicle_id: str | None,
        bucket: str,
        prefix: str | None,
        marker: str | None,
        limit: int,
    ) -> FileListData:
        """文件清单（契约 GET /files：prefix 列举 + marker 游标分页）。

        - max_keys = limit + 1 判断截断（契约 limit ≤ 1000）；
        - 每项即时签发 15 分钟预签名下载 URL（900s，支持 Range）。
        """
        full_prefix = f"{vehicle_id}/{prefix or ''}" if vehicle_id else (prefix or "")
        items, truncated, next_marker = await asyncio.to_thread(
            self._storage.list_objects,
            bucket,
            full_prefix,
            limit + 1,
            marker,
        )
        has_more = truncated or len(items) > limit
        if len(items) > limit:
            items = items[:limit]
        if has_more and not next_marker and items:
            next_marker = items[-1]["object_key"]
        file_items = [
            FileObjectItem(
                bucket=bucket,  # type: ignore[arg-type]  # 契约枚举校验在路由层完成
                object_key=item["object_key"],
                size_bytes=item["size_bytes"],
                etag=item["etag"],
                last_modified=item["last_modified"],
                download_url=await asyncio.to_thread(
                    self._storage.presign_get, bucket, item["object_key"]
                ),
                expires_in=settings.presigned_download_expire_seconds,
            )
            for item in items
        ]
        return FileListData(
            items=file_items,
            next_marker=next_marker if has_more else None,
            truncated=has_more,
        )
