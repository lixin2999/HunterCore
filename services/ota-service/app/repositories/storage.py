"""MinIO 升级包存储访问（bucket hunter-ota-packages，S3 协议）。

与 data-collector 的 repositories/storage.py 模式一致：同步 boto3 调用经
``asyncio.to_thread`` 包装为异步（调用方负责），禁止在事件循环内做阻塞 IO。

安全约束（契约 x-hunter-version-upload-flow / x-hunter-ota-security）：
- 上传预签名 1 小时 / 下载预签名 15 分钟 + Range 分片下载；
- ``stream_hashes`` 单次流式计算 size/MD5/SHA-256（GB 级包体不整包入内存）；
- 日志脱敏：仅记录对象键，禁止输出预签名 URL 全量查询串与 MINIO_SECRET_KEY。
"""
from __future__ import annotations

import asyncio
import hashlib
from typing import Any

from botocore.exceptions import ClientError

from app.config import settings

#: 流式哈希分块大小（1 MiB：内存占用与吞吐折中，非业务阈值）
_STREAM_CHUNK_SIZE = 1024 * 1024


class OtaPackageStorage:
    """升级包对象存储（预签名 / 元数据 / 流式哈希）。"""

    def __init__(self, client: Any) -> None:
        self._client = client

    # ---------- 上传预签名（1 小时） ----------

    def presign_put(self, bucket: str, key: str) -> str:
        """PUT 预签名 URL（有效期 3600s，契约 presign_policy.upload_expires_seconds）。"""
        return self._client.generate_presigned_url(
            ClientMethod="put_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=settings.presigned_upload_expire_seconds,
        )

    def presign_upload_part(self, bucket: str, key: str, upload_id: str, part_number: int) -> str:
        """分片上传预签名 URL（有效期与单次直传一致）。"""
        return self._client.generate_presigned_url(
            ClientMethod="upload_part",
            Params={"Bucket": bucket, "Key": key, "UploadId": upload_id, "PartNumber": part_number},
            ExpiresIn=settings.presigned_upload_expire_seconds,
        )

    def create_multipart_upload(self, bucket: str, key: str) -> str:
        """创建分片上传会话，返回 upload_id。"""
        resp = self._client.create_multipart_upload(Bucket=bucket, Key=key)
        upload_id = resp.get("UploadId")
        if not upload_id:
            raise RuntimeError("MinIO create_multipart_upload 未返回 UploadId")
        return str(upload_id)

    # ---------- 下载预签名（15 分钟 + Range） ----------

    def presign_get(self, bucket: str, key: str) -> str:
        """GET 预签名 URL（有效期 900s，支持 Range 分片下载）。"""
        return self._client.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=settings.presigned_download_expire_seconds,
        )

    # ---------- 元数据 / 连通性 ----------

    def head_object(self, bucket: str, key: str) -> dict[str, Any] | None:
        """对象元数据（size_bytes/etag）；不存在返回 None。"""
        try:
            resp = self._client.head_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise
        etag = resp.get("ETag")
        return {
            "size_bytes": int(resp.get("ContentLength") or 0),
            "etag": etag.strip('"') if etag else None,
        }

    def head_bucket(self, bucket: str) -> bool:
        """bucket 可访问性（/readyz 探针用；不可访问返回 False 不抛出）。"""
        try:
            self._client.head_bucket(Bucket=bucket)
        except ClientError:
            return False
        return True

    # ---------- 流式完整性校验（发布门禁第 1-2 步） ----------

    def stream_hashes(self, bucket: str, key: str) -> tuple[int, str, str]:
        """单次流式计算 (size_bytes, md5_hex, sha256_hex)，不整包入内存。

        阈值来源：x-hunter-version-upload-flow.steps 第 3 步（GB 级对象流式哈希）。
        """
        resp = self._client.get_object(Bucket=bucket, Key=key)
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


async def get_storage() -> OtaPackageStorage:
    """构建 OtaPackageStorage（同步 boto3 客户端在 to_thread 中构造）。"""
    return OtaPackageStorage(await asyncio.to_thread(_build_sync_client))


def _build_sync_client() -> Any:
    """构建 boto3 S3 客户端（同步；局部导入避免模块导入期建立连接）。"""
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        region_name=settings.minio_region,
    )


__all__ = ["OtaPackageStorage", "get_storage"]
