"""录像/sidecar 归档访问层：hunter-video 桶（S3 协议，Bucket 名契约固定）。

生产实现基于 aioboto3（可选依赖，见服务 pyproject [minio] extra，与 data-analytics
repositories/storage.py 同构）；单元测试以内存实现替换（遵循同一 Protocol）。

对象键布局（契约 RecordArchiveInfo/x-hunter-history-archive.discoverability，不可更改）：
- 录像：``remote-control/{vehicle_id}/{yyyy}/{mm}/{dd}/{session_id}.mp4``
- sidecar：``remote-control/{vehicle_id}/{yyyy}/{mm}/{dd}/{session_id}.json``（同目录同名）
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Protocol, runtime_checkable

from hunter_common.config import HunterBaseConfig
from hunter_common.exceptions import ServiceUnavailableError
from hunter_common.logging import get_logger

logger = get_logger("app.repositories.storage")

VIDEO_CONTAINER = "mp4"
SIDECAR_SUFFIX = ".json"
KEY_PREFIX = "remote-control"


def build_video_object_key(vehicle_id: str, started_at: float, session_id: str) -> str:
    """录像对象键：remote-control/{vehicle_id}/{yyyy}/{mm}/{dd}/{session_id}.mp4。"""
    day = datetime.fromtimestamp(started_at, tz=timezone.utc)
    return (
        f"{KEY_PREFIX}/{vehicle_id}/{day.strftime('%Y')}/{day.strftime('%m')}/"
        f"{day.strftime('%d')}/{session_id}.{VIDEO_CONTAINER}"
    )


def build_sidecar_object_key(
    vehicle_id: str, started_at: float, session_id: str
) -> str:
    """sidecar 对象键：与录像同目录同名 .json（契约 sidecar_schema）。"""
    day = datetime.fromtimestamp(started_at, tz=timezone.utc)
    return (
        f"{KEY_PREFIX}/{vehicle_id}/{day.strftime('%Y')}/{day.strftime('%m')}/"
        f"{day.strftime('%d')}/{session_id}{SIDECAR_SUFFIX}"
    )


@runtime_checkable
class VideoArchiveStorage(Protocol):
    """录像归档存储协议（依赖倒置：服务层仅依赖该接口）。"""

    async def head_video(self, key: str) -> int | None:
        """查询录像对象大小（字节）；对象不存在返回 None。"""
        ...

    async def read_sidecar(self, key: str) -> dict[str, Any] | None:
        """读取 sidecar JSON；对象不存在返回 None。"""
        ...

    async def write_sidecar(self, key: str, doc: dict[str, Any]) -> None:
        """写入 sidecar JSON（覆盖语义）。"""
        ...

    async def list_sidecars(
        self, prefix: str, *, limit: int | None = None
    ) -> list[tuple[str, dict[str, Any]]]:
        """按前缀列举 sidecar JSON（返回 (key, doc) 列表）。"""
        ...

    async def presign_get(self, key: str, expires_in: int) -> str:
        """生成下载预签名 URL（支持 Range 分片下载）。"""
        ...

    async def healthcheck(self) -> bool:
        """健康探测（/healthz 可观测）。"""
        ...

    async def close(self) -> None:
        """释放底层连接。"""
        ...


class S3VideoArchiveStorage:
    """基于 aioboto3 的 MinIO（S3 协议）访问实现（客户端惰性创建）。"""

    def __init__(
        self, settings: HunterBaseConfig, bucket: str, region: str = "us-east-1"
    ) -> None:
        self._bucket = bucket
        self._region = region
        self._endpoint_url = f"{'https' if settings.minio_secure else 'http'}://{settings.minio_endpoint}"
        self._access_key = settings.minio_access_key
        self._secret_key = settings.minio_secret_key
        self._cm: Any | None = None
        self._client: Any | None = None
        self._lock = asyncio.Lock()

    def _create_client(self) -> Any:
        """构建 aioboto3 客户端（aioboto3 为可选依赖，缺失时快速失败为 5001）。"""
        try:
            import aioboto3  # 延迟导入：避免未安装 minio extra 时阻断服务启动
        except ModuleNotFoundError as exc:  # pragma: no cover - 依赖缺失场景
            raise ServiceUnavailableError(
                "对象存储客户端不可用（aioboto3 未安装）"
            ) from exc
        session = aioboto3.Session()
        return session.client(
            "s3",
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            region_name=self._region,
        )

    async def _ensure_client(self) -> Any:
        if self._client is None:
            async with self._lock:
                if self._client is None:
                    self._cm = self._create_client()
                    self._client = await self._cm.__aenter__()
        return self._client

    @staticmethod
    def _is_not_found(exc: Exception) -> bool:
        """识别 S3 NoSuchKey / 404（不引入 botocore 类型依赖）。"""
        response = getattr(exc, "response", None)
        if not isinstance(response, dict):
            return False
        error = response.get("Error") or {}
        if error.get("Code") in {"NoSuchKey", "404"}:
            return True
        return response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404

    async def head_video(self, key: str) -> int | None:
        """HEAD 对象取 ContentLength；404 → None。"""
        client = await self._ensure_client()
        try:
            resp = await client.head_object(Bucket=self._bucket, Key=key)
        except Exception as exc:
            if self._is_not_found(exc):
                return None
            logger.exception("storage_head_failed", key=key)
            raise ServiceUnavailableError("对象存储查询失败") from exc
        return int(resp.get("ContentLength") or 0)

    async def read_sidecar(self, key: str) -> dict[str, Any] | None:
        client = await self._ensure_client()
        try:
            resp = await client.get_object(Bucket=self._bucket, Key=key)
            body = await resp["Body"].read()
        except Exception as exc:
            if self._is_not_found(exc):
                return None
            logger.exception("storage_get_failed", key=key)
            raise ServiceUnavailableError("对象存储读取失败") from exc
        return json.loads(body)

    async def write_sidecar(self, key: str, doc: dict[str, Any]) -> None:
        client = await self._ensure_client()
        try:
            await client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=json.dumps(doc, ensure_ascii=False).encode("utf-8"),
                ContentType="application/json",
            )
        except Exception as exc:
            logger.exception("storage_put_failed", key=key)
            raise ServiceUnavailableError("对象存储写入失败") from exc

    async def list_sidecars(
        self, prefix: str, *, limit: int | None = None
    ) -> list[tuple[str, dict[str, Any]]]:
        """按前缀列举 sidecar JSON（分页列举 + 上限保护，防查询放大）。"""
        client = await self._ensure_client()
        out: list[tuple[str, dict[str, Any]]] = []
        try:
            paginator = client.get_paginator("list_objects_v2")
            async for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if not key.endswith(SIDECAR_SUFFIX):
                        continue
                    doc = await self.read_sidecar(key)
                    if doc is not None:
                        out.append((key, doc))
                    if limit is not None and len(out) >= limit:
                        return out
        except ServiceUnavailableError:
            raise
        except Exception as exc:
            logger.exception("storage_list_failed", prefix=prefix)
            raise ServiceUnavailableError("对象存储列举失败") from exc
        return out

    async def presign_get(self, key: str, expires_in: int) -> str:
        client = await self._ensure_client()
        try:
            return client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self._bucket, "Key": key},
                ExpiresIn=expires_in,
            )
        except Exception as exc:
            logger.exception("storage_presign_failed", key=key)
            raise ServiceUnavailableError("对象存储预签名失败") from exc

    async def healthcheck(self) -> bool:
        try:
            client = await self._ensure_client()
            await client.head_bucket(Bucket=self._bucket)
        except Exception:  # noqa: BLE001 - 健康探测失败返回 False 降级，不向请求路径抛出
            logger.warning("storage_healthcheck_failed", bucket=self._bucket)
            return False
        return True

    async def close(self) -> None:
        if self._cm is not None:
            await self._cm.__aexit__(None, None, None)
            self._cm = None
            self._client = None


__all__ = [
    "KEY_PREFIX",
    "S3VideoArchiveStorage",
    "VideoArchiveStorage",
    "build_sidecar_object_key",
    "build_video_object_key",
]
