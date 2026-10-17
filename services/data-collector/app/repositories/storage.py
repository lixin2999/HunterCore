"""MinIO 数据访问（S3 协议）。

对应 data-analytics 的 repositories/storage.py 模式：同步 boto3 调用经
asyncio.to_thread 包装为异步，禁止在事件循环内做阻塞 IO（异步优先原则）。

生命周期一致性：桶级生命周期规则由 MinIO 初始化脚本配置（G-12 后 hunter-rosbag
按对象 Tag hunter-retention 区分，Tag 由本模块 ``put_object_tags`` 在 complete 阶段打标，
契约 x-hunter-file-upload-flow.tagging）。

完整性校验（审查 R2 修复）：``stream_hashes`` 单次流式计算对象真实 size/MD5/SHA-256，
是 complete 阶段「size_bytes / md5 / sha256 三者一致」契约判定的唯一数据来源
（此前实现仅比对 ETag 与声明尺寸，sha256 从未参与校验）。
"""
from __future__ import annotations

import asyncio
import hashlib
from typing import Any

from botocore.exceptions import ClientError
from hunter_common.exceptions import ResourceNotFoundError, ServiceUnavailableError

from app.config import settings

#: 流式哈希分块大小（1 MiB：内存占用与吞吐折中，非业务阈值；与 ota-service 一致）
_STREAM_CHUNK_SIZE = 1024 * 1024
#: 对象不存在（MinIO 可能返回 404 / NoSuchKey / NotFound）
_NOT_FOUND_CODES = frozenset({"404", "NoSuchKey", "NotFound"})
#: hunter-rosbag 生命周期归类 Tag 键（G-12，contracts/database/object-storage.yaml lifecycle.tagging）
ROSBAG_RETENTION_TAG_KEY = "hunter-retention"


class MinioStorage:
    """MinIO 对象存储访问（预签名/元数据/列举/分片合并）。

    双 client 设计：
    - ``_client``：内部连接（MINIO_ENDPOINT，集群内地址），用于 head/get/list/tagging 等服务侧操作。
    - ``_presign_client``：预签名专用（MINIO_PUBLIC_ENDPOINT，车端可达外部地址），
      保证生成的上传/下载 URL 中 host 为车端可路由地址。
      未配置 MINIO_PUBLIC_ENDPOINT 时两者指向相同 endpoint（回落，单机部署同网段情形）。
    """

    def __init__(self, internal_client: Any, presign_client: Any | None = None) -> None:
        self._client = internal_client
        # 预签名 client：未传则回落到内部 client（向后兼容测试注入单 client 的场景）
        self._presign_client = presign_client if presign_client is not None else internal_client

    # ---------- 单次直传（PUT 预签名） ----------

    def presign_put(self, bucket: str, key: str, content_type: str | None = None) -> str:
        """PUT 预签名 URL（上传有效期 1 小时，MinIO 契约）。

        使用 _presign_client（endpoint = MINIO_PUBLIC_ENDPOINT）确保 URL 中的 host
        为车端可达的外部地址。
        """
        params: dict[str, Any] = {"Bucket": bucket, "Key": key}
        if content_type:
            params["ContentType"] = content_type
        return self._presign_client.generate_presigned_url(
            ClientMethod="put_object",
            Params=params,
            ExpiresIn=settings.presigned_upload_expire_seconds,
        )

    def presign_upload_part(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        part_number: int,
    ) -> str:
        """分片上传预签名 URL（有效期与单次直传一致：1 小时）。

        使用 _presign_client 确保分片 URL 中的 host 为车端可达地址。
        """
        return self._presign_client.generate_presigned_url(
            ClientMethod="upload_part",
            Params={
                "Bucket": bucket,
                "Key": key,
                "UploadId": upload_id,
                "PartNumber": part_number,
            },
            ExpiresIn=settings.presigned_upload_expire_seconds,
        )

    def create_multipart_upload(self, bucket: str, key: str, content_type: str | None) -> str:
        """创建分片上传会话，返回 upload_id。

        通过内部 client 在 MinIO 服务端创建会话；upload_id 与对象路径绑定，
        与后续 presign_upload_part 使用哪个 endpoint 无关。
        """
        args: dict[str, Any] = {"Bucket": bucket, "Key": key}
        if content_type:
            args["ContentType"] = content_type
        resp = self._client.create_multipart_upload(**args)
        upload_id = resp.get("UploadId")
        if not upload_id:
            raise RuntimeError("MinIO create_multipart_upload 未返回 UploadId")
        return str(upload_id)

    # ---------- 下载（GET 预签名，支持 Range 分片下载） ----------

    def presign_get(self, bucket: str, key: str) -> str:
        """GET 预签名 URL（下载有效期 15 分钟，支持 Range）。

        使用 _presign_client 确保下载 URL 中的 host 为车端/浏览器可达地址。
        """
        return self._presign_client.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=settings.presigned_download_expire_seconds,
        )

    # ---------- 元数据 ----------

    def head_object(self, bucket: str, key: str) -> dict[str, Any] | None:
        """对象元数据（ContentLength/ETag/LastModified）；不存在返回 None。"""
        try:
            resp = self._client.head_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in _NOT_FOUND_CODES:
                return None
            raise
        etag = resp.get("ETag")
        if etag:
            etag = etag.strip('"')
        last_modified = resp.get("LastModified")
        return {
            "size_bytes": int(resp.get("ContentLength") or 0),
            "etag": etag,
            "last_modified": last_modified.timestamp() if last_modified else 0.0,
            "content_type": resp.get("ContentType"),
        }

    # ---------- 流式完整性校验（complete 阶段，审查 R2） ----------

    def stream_hashes(self, bucket: str, key: str) -> tuple[int, str, str]:
        """单次流式计算对象真实 ``(size_bytes, md5_hex, sha256_hex)``，不整包入内存。

        契约依据：``contracts/openapi/data-collector.yaml``（POST /files/complete）
        「完整性校验（size_bytes / md5 / sha256 三者必须全部一致，任一不符返回 6001）」。

        Raises:
            ResourceNotFoundError: 对象不存在（3001）。
            ServiceUnavailableError: 对象存储不可用（5001）。
                禁止静默降级为「校验通过」——完整性校验不可因依赖故障而失效。
        """
        try:
            resp = self._client.get_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in _NOT_FOUND_CODES:
                raise ResourceNotFoundError(
                    message=f"对象不存在或尚未上传完成（bucket={bucket}, key={key}）"
                ) from exc
            raise ServiceUnavailableError(
                message=f"对象存储不可用，无法完成完整性校验（bucket={bucket}）"
            ) from exc
        body = resp["Body"]
        md5 = hashlib.md5()
        sha256 = hashlib.sha256()
        size = 0
        try:
            while True:
                chunk = body.read(_STREAM_CHUNK_SIZE)
                if not chunk:
                    break
                md5.update(chunk)
                sha256.update(chunk)
                size += len(chunk)
        finally:
            body.close()
        return size, md5.hexdigest(), sha256.hexdigest()

    # ---------- 清单（list_objects_v2，游标分页） ----------

    def list_objects(
        self,
        bucket: str,
        prefix: str,
        max_keys: int,
        marker: str | None = None,
    ) -> tuple[list[dict[str, Any]], bool, str | None]:
        """列举对象；返回 (items, truncated, next_marker)。

        - items 元素含 object_key/size_bytes/etag/last_modified；
        - 游标续列用 **``StartAfter``**（对象键）；``IsTruncated`` 时缺省回退末项 Key。

        ℞ V1.21.16（实机确诊）：``list_objects_v2`` **没有 ``Marker`` 参数**——那是 v1
        ``list_objects`` 的续列入参。旧实现把 ``Marker`` 传给 v2，会被 botocore 判为
        未知入参并抛 ``ParamValidationError`` → HTTP 500「服务器内部错误」。症状正是：
        **首页（marker 为 None）正常，一点「加载更多（游标分页）」即 500**。
        v2 的续列入参是不透明 ``ContinuationToken`` 或对象键 ``StartAfter``；本服务自产
        ``next_marker`` = 末项 object_key，故用 ``StartAfter`` 与之语义对齐。
        """
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": max_keys}
        if marker:
            kwargs["StartAfter"] = marker
        resp = self._client.list_objects_v2(**kwargs)
        items = [
            {
                "object_key": str(obj.get("Key") or ""),
                "size_bytes": int(obj.get("Size") or 0),
                "etag": (str(obj["ETag"]).strip('"') if obj.get("ETag") else None),
                "last_modified": obj["LastModified"].timestamp()
                if obj.get("LastModified")
                else 0.0,
            }
            for obj in resp.get("Contents", [])
        ]
        truncated = bool(resp.get("IsTruncated"))
        next_marker = resp.get("NextMarker")
        if truncated and not next_marker and items:
            next_marker = items[-1]["object_key"]
        return items, truncated, next_marker

    # ---------- 生命周期归类打标（G-12） ----------

    def put_object_tags(self, bucket: str, key: str, retention: str) -> None:
        """打生命周期归类 Tag ``hunter-retention=<retention>``（仅 hunter-rosbag，G-12）。

        未打标对象等同永久保留（S3 tag 过滤器无法表达无 Tag），因此打标失败
        必须阻断 sensor_file 发布（调用侧转 5001），禁止静默降级。
        """
        self._client.put_object_tagging(
            Bucket=bucket,
            Key=key,
            Tagging={"TagSet": [{"Key": ROSBAG_RETENTION_TAG_KEY, "Value": retention}]},
        )

    # ---------- 分片合并 / 中止 ----------

    def complete_multipart_upload(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        parts: list[dict[str, Any]],
    ) -> None:
        """合并分片（parts: [{part_number, etag}]，来自 complete 请求回带）。"""
        self._client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [
                    {"PartNumber": p["part_number"], "ETag": p["etag"]} for p in parts
                ]
            },
        )

    def abort_multipart_upload(self, bucket: str, key: str, upload_id: str) -> None:
        """中止分片上传（回滚：complete 参数非法时清理服务端会话）。"""
        try:
            self._client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in {"404", "NoSuchUpload"}:
                raise


async def get_storage() -> MinioStorage:
    """FastAPI 依赖：构建 MinioStorage（同步 boto3 客户端，调用经 to_thread 包装）。"""
    internal_client = await asyncio.to_thread(_build_sync_client, settings.minio_endpoint_url)
    # 预签名专用客户端：endpoint 替换为车端可达的公网/外网地址（MINIO_PUBLIC_ENDPOINT）。
    # 若未配置 MINIO_PUBLIC_ENDPOINT，则与 internal_client 指向同一地址（回落逻辑见 property）。
    presign_client = await asyncio.to_thread(_build_sync_client, settings.minio_public_endpoint_url)
    return MinioStorage(internal_client=internal_client, presign_client=presign_client)


def _build_sync_client(endpoint_url: str) -> Any:
    """构建 boto3 S3 客户端（同步；构造为纯 CPU 操作，亦在 to_thread 中执行）。"""
    import boto3  # 局部导入：避免模块导入期建立连接

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        region_name=settings.minio_region,
    )


__all__ = ["ROSBAG_RETENTION_TAG_KEY", "MinioStorage", "get_storage"]
