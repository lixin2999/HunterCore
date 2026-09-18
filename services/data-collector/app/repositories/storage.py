"""MinIO 数据访问（S3 协议）。

对应 data-analytics 的 repositories/storage.py 模式：同步 boto3 调用经
asyncio.to_thread 包装为异步，禁止在事件循环内做阻塞 IO（异步优先原则）。

生命周期一致性：桶生命周期（30/90/永久）由 infra/docker/minio-init Job 配置，
本模块只做对象操作（契约 x-hunter-file-upload-flow）。
"""
from __future__ import annotations

import asyncio
from typing import Any

from botocore.exceptions import ClientError

from app.config import settings


class MinioStorage:
    """MinIO 对象存储访问（预签名/元数据/列举/分片合并）。"""

    def __init__(self, client: Any) -> None:
        self._client = client

    # ---------- 单次直传（PUT 预签名） ----------

    def presign_put(self, bucket: str, key: str, content_type: str | None = None) -> str:
        """PUT 预签名 URL（上传有效期 1 小时，MinIO 契约）。"""
        params: dict[str, Any] = {"Bucket": bucket, "Key": key}
        if content_type:
            params["ContentType"] = content_type
        return self._client.generate_presigned_url(
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
        """分片上传预签名 URL（有效期与单次直传一致：1 小时）。"""
        return self._client.generate_presigned_url(
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
        """创建分片上传会话，返回 upload_id。"""
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
        """GET 预签名 URL（下载有效期 15 分钟，支持 Range）。"""
        return self._client.generate_presigned_url(
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
            if code in {"404", "NoSuchKey", "NotFound"}:
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

    # ---------- 清单（list_objects_v2，marker 游标分页） ----------

    def list_objects(
        self,
        bucket: str,
        prefix: str,
        max_keys: int,
        marker: str | None = None,
    ) -> tuple[list[dict[str, Any]], bool, str | None]:
        """列举对象；返回 (items, truncated, next_marker)。

        - items 元素含 object_key/size_bytes/etag/last_modified；
        - NextMarker 仅在截断时有值（is_truncated=True），缺省回退到末项 Key。
        """
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": max_keys}
        if marker:
            kwargs["Marker"] = marker
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
    return MinioStorage(await asyncio.to_thread(_build_sync_client))


def _build_sync_client() -> Any:
    """构建 boto3 S3 客户端（同步；构造为纯 CPU 操作，亦在 to_thread 中执行）。"""
    import boto3  # 局部导入：避免模块导入期建立连接

    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        region_name=settings.minio_region,
    )


__all__ = ["MinioStorage", "get_storage"]
