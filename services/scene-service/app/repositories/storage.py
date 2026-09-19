"""MinIO 场景资源存储访问（bucket hunter-scene-assets，S3 协议，生命周期永久）。

与 ota-service / data-collector 模式一致：同步 boto3 调用经 ``asyncio.to_thread`` 包装为异步
（禁止在事件循环内做阻塞 IO）。契约 x-hunter-export：对象前缀 ``scenarios/``、命名
``scene-{scene_id}-{version}.{ext}``、下载预签名 900s 且支持 Range 分片下载。
日志脱敏：仅记录对象键，禁止输出预签名 URL 查询串与 MINIO_SECRET_KEY。
"""
from __future__ import annotations

import asyncio
from typing import Any

from botocore.exceptions import ClientError
from hunter_common.exceptions import ServiceUnavailableError
from hunter_common.logging import get_logger

logger = get_logger("app.repositories.storage")


def build_object_key(prefix: str, *, scene_id: str, version: str, extension: str) -> str:
    """构造导出对象键（契约 x-hunter-export.object_prefix + naming，命名不可更改）。"""
    return f"{prefix}scene-{scene_id}-{version}{extension}"


class SceneAssetStorage:
    """场景资源对象存储（上传/预签名下载/连通性探测）。"""

    def __init__(self, client: Any, bucket: str, presign_expire_seconds: int) -> None:
        self._client = client
        self._bucket = bucket
        self._presign_expire_seconds = presign_expire_seconds

    @property
    def bucket(self) -> str:
        """bucket 名称（契约固定 hunter-scene-assets）。"""
        return self._bucket

    @property
    def presign_expire_seconds(self) -> int:
        """下载预签名有效期（契约固定 900s）。"""
        return self._presign_expire_seconds

    # ---------- 上传（同步 boto3 经线程池执行） ----------

    async def upload_bytes(self, key: str, payload: bytes, *, content_type: str) -> None:
        """上传导出产物（失败抛 5001；boto3 调用放线程池避免阻塞事件循环）。"""
        try:
            await asyncio.to_thread(self._put_object, key, payload, content_type)
        except ClientError as exc:
            logger.exception("scene_asset_upload_failed", key=key)
            raise ServiceUnavailableError("对象存储写入失败", details={"key": key}) from exc

    def _put_object(self, key: str, payload: bytes, content_type: str) -> None:
        """同步 PUT（仅在线程池中调用）。"""
        self._client.put_object(
            Bucket=self._bucket, Key=key, Body=payload, ContentType=content_type
        )

    # ---------- 下载预签名（15 分钟 + Range） ----------

    async def presign_download(self, key: str) -> str:
        """生成下载预签名 URL（900s；S3 GET 天然支持 Range 分片下载）。"""
        try:
            return str(
                await asyncio.to_thread(
                    self._client.generate_presigned_url,
                    "get_object",
                    Params={"Bucket": self._bucket, "Key": key},
                    ExpiresIn=self._presign_expire_seconds,
                )
            )
        except ClientError as exc:
            logger.exception("scene_asset_presign_failed", key=key)
            raise ServiceUnavailableError("对象存储预签名失败", details={"key": key}) from exc

    # ---------- 连通性 ----------

    async def healthcheck(self) -> bool:
        """bucket 可访问性（探测失败返回 False，不向请求路径抛出）。"""
        try:
            await asyncio.to_thread(self._client.head_bucket, Bucket=self._bucket)
        except Exception:  # noqa: BLE001 - 健康探测失败即视为不可用
            logger.warning("scene_asset_healthcheck_failed", bucket=self._bucket)
            return False
        return True


async def get_storage(
    minio_endpoint: str,
    access_key: str,
    secret_key: str,
    region: str,
    bucket: str,
    presign_expire_seconds: int,
    *,
    secure: bool = False,
) -> SceneAssetStorage:
    """构建 SceneAssetStorage（同步 boto3 客户端在线程池中构造，避免阻塞启动）。"""
    client = await asyncio.to_thread(
        _build_sync_client, minio_endpoint, access_key, secret_key, region, secure
    )
    return SceneAssetStorage(client, bucket, presign_expire_seconds)


def endpoint_url_for(minio_endpoint: str, *, secure: bool) -> str:
    """规范化 MinIO 端点：boto3 要求含 scheme，无 scheme 时按 MINIO_SECURE 补全。

    共享配置默认值为 ``host:port`` 形式，直接传给 boto3 会抛
    ``ValueError: Invalid endpoint``，故此处统一补全（禁止硬编码地址）。
    """
    if "://" in minio_endpoint:
        return minio_endpoint
    return f"{'https' if secure else 'http'}://{minio_endpoint}"


def _build_sync_client(
    minio_endpoint: str, access_key: str, secret_key: str, region: str, secure: bool
) -> Any:
    """构建 boto3 S3 客户端（局部导入，避免模块导入期建立连接）。"""
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url_for(minio_endpoint, secure=secure),
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )


__all__ = ["SceneAssetStorage", "build_object_key", "endpoint_url_for", "get_storage"]