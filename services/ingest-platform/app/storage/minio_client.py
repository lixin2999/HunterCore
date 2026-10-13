"""MinIO 数据湖对象存储客户端（设计文档 §6.1 数据湖布局）。

职责：
- 读取 parsed 层（车端所有权，只读）：manifest.json / images/ / pointclouds/ / tables/ / metadata/
- 写入 derived 层（平台所有权）：derived/v1/{bag_id}/{input_rev}/...
- 写入 dataset 层（训练出口）：datasets/{dataset_id}/{version}/...
- 生成预签名下载 URL（§8.1 时效 10min）
- HEAD 抽样校验（§7.4 QC hash 核验）

对象路径规约（与车端文档 §7.2 对齐）：
  parsed:  bags/v1/{vehicle_id}/{date}/{bag_id}/...
  derived: derived/v1/{bag_id}/{input_rev}/aligned/frame_index.parquet
                                  /clips/clip_{start}_{end}/clip.manifest.json
                                  /qc/qc_result.json
                                  /enrich/traj.parquet
  dataset: datasets/{dataset_id}/{version}/snapshot.manifest.jsonl
"""
from __future__ import annotations

import asyncio
import hashlib
import io
import json
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date as date_type
from typing import Any

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from hunter_common.logging import get_logger

from app.config import Settings

logger = get_logger("app.storage.minio_client")


# ========== 路径生成工具 ==========

def parsed_key(vehicle_id: str, dt: date_type, bag_id: str, suffix: str = "") -> str:
    """生成 parsed 层对象 key（§6.1）。"""
    base = f"bags/v1/{vehicle_id}/{dt.isoformat()}/{bag_id}"
    return f"{base}/{suffix}" if suffix else base


def derived_key(bag_id: str, input_rev: str, suffix: str) -> str:
    """生成 derived 层对象 key（§6.1）。"""
    return f"derived/v1/{bag_id}/{input_rev}/{suffix}"


def dataset_key(dataset_id: str, version: str, suffix: str) -> str:
    """生成 dataset 层对象 key（§6.1）。"""
    return f"datasets/{dataset_id}/{version}/{suffix}"


def clip_dir_key(bag_id: str, input_rev: str, start_ns: int, end_ns: int) -> str:
    """clip 目录前缀 key（§7.3）。"""
    return derived_key(bag_id, input_rev, f"clips/clip_{start_ns}_{end_ns}")


# ========== 数据类 ==========

@dataclass
class ObjectMeta:
    """对象 HEAD 结果。"""
    key: str
    size: int
    etag: str
    last_modified: str
    metadata: dict[str, str] = field(default_factory=dict)

    @property
    def sha256(self) -> str | None:
        """对象元数据中携带的 sha256（平台写入的 derived 对象会包含此字段）。"""
        return self.metadata.get("x-amz-meta-sha256")

    @property
    def source_rev(self) -> str | None:
        """来源 input_rev 元数据（§6.1 约定）。"""
        return self.metadata.get("x-amz-meta-source-rev")


# ========== 主客户端 ==========

class LakeStorage:
    """数据湖 MinIO 存储封装（boto3 同步客户端，经 asyncio.to_thread 调用）。

    Bucket 归属：
    - hunter-rosbag: parsed 层（车端上传，平台只读）
    - hunter-derived: derived 层（平台专属）
    - hunter-datasets: dataset 层（平台专属）
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = boto3.client(
            "s3",
            endpoint_url=settings.minio_endpoint_url,
            aws_access_key_id=settings.minio_access_key,
            aws_secret_access_key=settings.minio_secret_key,
            region_name=settings.minio_region,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )
        self._executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="s3")

    # ---------- 读：parsed 层 ----------

    def get_manifest(self, vehicle_id: str, dt: date_type, bag_id: str) -> dict[str, Any]:
        """读取 manifest.json（parsed 层），返回解析后的 dict。"""
        key = parsed_key(vehicle_id, dt, bag_id, "manifest.json")
        data = self._get_object_bytes(self._settings.minio_bucket_parsed, key)
        return json.loads(data.decode("utf-8"))

    def get_parsed_object(self, key: str) -> bytes:
        """读取 parsed 层任意对象（图片、点云等）。"""
        return self._get_object_bytes(self._settings.minio_bucket_parsed, key)

    def list_parsed_prefix(self, vehicle_id: str, dt: date_type, bag_id: str, suffix: str = "") -> list[str]:
        """列举 parsed 层某 bag 某子目录下的对象 key。"""
        prefix = parsed_key(vehicle_id, dt, bag_id, suffix)
        if suffix and not prefix.endswith("/"):
            prefix += "/"
        return self._list_keys(self._settings.minio_bucket_parsed, prefix)

    # ---------- 写：derived 层 ----------

    def put_derived_json(
        self,
        bag_id: str,
        input_rev: str,
        suffix: str,
        data: dict | list,
    ) -> str:
        """写 JSON 到 derived 层，返回完整 object key。"""
        key = derived_key(bag_id, input_rev, suffix)
        body = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
        self._put_object(
            self._settings.minio_bucket_derived,
            key,
            body,
            content_type="application/json",
            extra_meta={"source-rev": input_rev},
        )
        return key

    def put_derived_bytes(
        self,
        bag_id: str,
        input_rev: str,
        suffix: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
    ) -> str:
        """写二进制到 derived 层（Parquet 文件等），返回完整 object key。"""
        key = derived_key(bag_id, input_rev, suffix)
        sha = hashlib.sha256(data).hexdigest()
        self._put_object(
            self._settings.minio_bucket_derived,
            key,
            data,
            content_type=content_type,
            extra_meta={"source-rev": input_rev, "sha256": sha},
        )
        return key

    # ---------- 写：dataset 层 ----------

    def put_dataset_manifest_line(self, dataset_id: str, version: str, line: str) -> str:
        """向 snapshot.manifest.jsonl 追加一行（单条 clip 记录）。"""
        key = dataset_key(dataset_id, version, "snapshot.manifest.jsonl")
        body = (line + "\n").encode("utf-8")
        self._put_object(
            self._settings.minio_bucket_dataset,
            key,
            body,
            content_type="application/x-ndjson",
        )
        return key

    # ---------- 预签名 URL（§8.1 时效 10min） ----------

    def presigned_url(self, bucket: str, key: str, *, expiry_s: int | None = None) -> str:
        """生成预签名 GET URL（用于图像/点云/Parquet 下载）。"""
        ttl = expiry_s or self._settings.presigned_download_expire_seconds
        url: str = self._client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=ttl,
        )
        return url

    def presigned_parsed_url(self, key: str, *, expiry_s: int | None = None) -> str:
        """parsed 层预签名 URL（便捷方法）。"""
        return self.presigned_url(self._settings.minio_bucket_parsed, key, expiry_s=expiry_s)

    def presigned_derived_url(self, key: str, *, expiry_s: int | None = None) -> str:
        """derived 层预签名 URL。"""
        return self.presigned_url(self._settings.minio_bucket_derived, key, expiry_s=expiry_s)

    # ---------- HEAD 核验（QC 用，§7.4） ----------

    def head_parsed(self, key: str) -> ObjectMeta | None:
        """HEAD parsed 层对象；不存在返回 None。"""
        return self._head(self._settings.minio_bucket_parsed, key)

    async def ahead_parsed(self, key: str) -> ObjectMeta | None:
        """HEAD parsed 层对象（异步：boto3 同步调用投递到线程池，避免阻塞事件循环）。"""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, self.head_parsed, key)

    def head_derived(self, key: str) -> ObjectMeta | None:
        """HEAD derived 层对象。"""
        return self._head(self._settings.minio_bucket_derived, key)

    def sample_hash_check(
        self,
        bucket: str,
        keys: list[str],
        expected_hashes: dict[str, str],
        *,
        sample_ratio: float = 0.05,
    ) -> list[str]:
        """抽样校验 hash（§7.4）；返回校验失败的 key 列表。

        Args:
            keys: 待校验对象 key 列表。
            expected_hashes: key → sha256 hex 映射（来自 manifest 声明）。
            sample_ratio: 抽样比例（0~1）。
        """
        import random
        n = max(1, int(len(keys) * sample_ratio))
        sampled = random.sample(keys, min(n, len(keys)))
        corrupt: list[str] = []
        for key in sampled:
            expected = expected_hashes.get(key)
            if not expected:
                continue
            meta = self._head(bucket, key)
            if meta is None:
                corrupt.append(key)
                continue
            # MinIO etag 对非分块上传 = MD5；sha256 需从元数据取
            stored_sha = meta.sha256
            if stored_sha and stored_sha != expected:
                corrupt.append(key)
        return corrupt

    # ---------- 内部方法 ----------

    def _get_object_bytes(self, bucket: str, key: str) -> bytes:
        try:
            resp = self._client.get_object(Bucket=bucket, Key=key)
            data: bytes = resp["Body"].read()
            return data
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code in ("NoSuchKey", "404"):
                raise FileNotFoundError(f"对象不存在: s3://{bucket}/{key}") from exc
            raise

    def _put_object(
        self,
        bucket: str,
        key: str,
        body: bytes,
        *,
        content_type: str = "application/octet-stream",
        extra_meta: dict[str, str] | None = None,
    ) -> None:
        metadata: dict[str, str] = {}
        if extra_meta:
            metadata.update({f"x-amz-meta-{k}": v for k, v in extra_meta.items()})
        self._client.put_object(
            Bucket=bucket,
            Key=key,
            Body=io.BytesIO(body),
            ContentType=content_type,
            Metadata=metadata,
        )
        logger.debug("put_object", bucket=bucket, key=key, size=len(body))

    def _head(self, bucket: str, key: str) -> ObjectMeta | None:
        try:
            resp = self._client.head_object(Bucket=bucket, Key=key)
            return ObjectMeta(
                key=key,
                size=int(resp["ContentLength"]),
                etag=str(resp["ETag"]).strip('"'),
                last_modified=str(resp["LastModified"]),
                metadata=dict(resp.get("Metadata", {})),
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return None
            raise

    def _list_keys(self, bucket: str, prefix: str) -> list[str]:
        keys: list[str] = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(str(obj["Key"]))
        return keys

    def iter_parsed_objects(self, bucket: str, prefix: str) -> Iterator[str]:
        """迭代器模式列举（适合大量对象，避免一次加载到内存）。"""
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                yield str(obj["Key"])

    async def close(self) -> None:
        self._executor.shutdown(wait=False)


__all__ = [
    "LakeStorage",
    "ObjectMeta",
    "clip_dir_key",
    "dataset_key",
    "derived_key",
    "parsed_key",
]
