"""FileService 单元测试（审查 R2 / R6：补齐业务逻辑行为测试）。

覆盖：
- 预签名：Bucket↔data_type 映射（1002）、服务端命名规范、multipart 分片地址；
- 上传完成：路径归属（1002）、对象缺失（3001）、size/MD5/SHA-256 三类校验（6001）、
  multipart 合并与合并失败回滚（2001）、sensor_file 投递载荷、Kafka 故障（5001）；
- 清单：车辆前缀隔离、limit+1 截断与 next_marker 游标、预签名下载地址。

**关键回归（审查 R2）**：完整性校验必须基于「对象真实内容摘要」——
sha256 必须参与判定，且不得以 ETag 代替内容校验（multipart 对象 ETag 带 ``-N`` 后缀）。
"""
from __future__ import annotations

import hashlib
from typing import Any

import pytest
from hunter_common.exceptions import (
    InvalidParameterError,
    OtaPackageChecksumError,
    PermissionDeniedError,
    ResourceNotFoundError,
    ServiceUnavailableError,
)

from app.schemas.common import (
    OBJECT_KEY_TEMPLATE,
    SENSOR_FILE_TOPIC,
    FileDataType,
    UploadBucket,
)
from app.schemas.files import CompletedPart, FileCompleteRequest, FilePresignRequest
from app.services.files import FileService
from app.tests.fakes import FakeMinioStorage, FakeSensorFileProducer

VEHICLE_ID = "HUNTER-001"
USER_ID = "11111111-1111-4111-8111-111111111111"
RAW_BUCKET = UploadBucket.RAW_DATA.value
ROSBAG_BUCKET = UploadBucket.ROSBAG.value
DATE = "2026-09-18"
TIMESTAMP = 1724035200.0
CONTENT = b"point-cloud-binary-content"


def make_service() -> tuple[FileService, FakeMinioStorage, FakeSensorFileProducer]:
    """构造 (服务, 存储替身, 生产者替身)。"""
    storage = FakeMinioStorage()
    producer = FakeSensorFileProducer()
    return FileService(storage, producer), storage, producer


def object_key(data_type: FileDataType = FileDataType.POINT_CLOUD, seq: int = 1) -> str:
    """按 5.5 节命名规范构造对象路径（契约模板）。"""
    return OBJECT_KEY_TEMPLATE.format(
        vehicle_id=VEHICLE_ID,
        date=DATE,
        data_type=data_type.value,
        timestamp=int(TIMESTAMP),
        seq=seq,
        ext=".pcd",
    )


def complete_payload(
    key: str,
    content: bytes,
    *,
    bucket: UploadBucket = UploadBucket.RAW_DATA,
    **overrides: Any,
) -> FileCompleteRequest:
    """构造 complete 请求（默认声明值与 content 完全一致）。"""
    values: dict[str, Any] = {
        "vehicle_id": VEHICLE_ID,
        "bucket": bucket,
        "object_key": key,
        "size_bytes": len(content),
        "md5": hashlib.md5(content).hexdigest(),
        "sha256": hashlib.sha256(content).hexdigest(),
        "timestamp": TIMESTAMP,
    }
    values.update(overrides)
    return FileCompleteRequest(**values)


# ---------------------------------------------------------------------------
# presign
# ---------------------------------------------------------------------------
async def test_presign_single_put_uses_server_side_object_key() -> None:
    """单次直传：method=put，对象路径由服务端生成（客户端不可指定）。"""
    service, _, _ = make_service()
    data = await service.presign(
        FilePresignRequest(
            vehicle_id=VEHICLE_ID,
            bucket=UploadBucket.RAW_DATA,
            data_type=FileDataType.POINT_CLOUD,
            file_name="lidar.pcd",
            file_size=1024,
        )
    )
    assert data.method.value == "put"
    assert data.upload_url is not None
    assert data.upload_id is None and data.parts is None
    assert data.expires_in == 3600
    assert data.object_key.startswith(f"{VEHICLE_ID}/")
    assert "/point_cloud/" in data.object_key
    assert data.object_key.endswith(".pcd")


async def test_presign_rejects_bucket_data_type_mismatch() -> None:
    """Bucket 与 data_type 归属不符 → 1002（资源归属错误，非 2001）。"""
    service, _, _ = make_service()
    with pytest.raises(PermissionDeniedError) as exc:
        await service.presign(
            FilePresignRequest(
                vehicle_id=VEHICLE_ID,
                bucket=UploadBucket.RAW_DATA,
                data_type=FileDataType.ROSBAG,
                file_name="run.bag",
                file_size=2048,
            )
        )
    assert exc.value.code == 1002


async def test_presign_multipart_returns_upload_id_and_all_parts() -> None:
    """分片上传（大文件 ROS Bag）：返回 upload_id 与逐分片预签名地址。"""
    service, storage, _ = make_service()
    data = await service.presign(
        FilePresignRequest(
            vehicle_id=VEHICLE_ID,
            bucket=UploadBucket.ROSBAG,
            data_type=FileDataType.ROSBAG,
            file_name="run.bag",
            file_size=8 * 1024 * 1024,
            part_count=3,
        )
    )
    assert data.method.value == "multipart"
    assert data.upload_id is not None and data.upload_url is None
    assert data.parts is not None
    assert [part.part_number for part in data.parts] == [1, 2, 3]
    assert all("partNumber=" in part.upload_url for part in data.parts)
    assert data.object_key.endswith(".bag")
    assert len(storage.multipart_sessions) == 1  # 服务端已创建分片上传会话（upload_id）


async def test_presign_content_type_inferred_from_extension() -> None:
    """未显式指定 content_type 时按扩展名推断（.bag → application/ros-bag）。"""
    service, _, _ = make_service()
    data = await service.presign(
        FilePresignRequest(
            vehicle_id=VEHICLE_ID,
            bucket=UploadBucket.ROSBAG,
            data_type=FileDataType.ROSBAG,
            file_name="run.bag",
            file_size=1024,
        )
    )
    assert data.upload_url is not None  # 预签名地址由服务端签发（有效期 1 小时）
    assert "application/ros-bag" in data.upload_url

# ---------------------------------------------------------------------------
# complete：路径归属 / 对象存在性
# ---------------------------------------------------------------------------
async def test_complete_rejects_object_key_outside_vehicle_prefix() -> None:
    """对象路径必须归属请求车辆目录 → 否则 1002，且不投递通知。"""
    service, storage, producer = make_service()
    key = object_key().replace(VEHICLE_ID, "HUNTER-999")
    storage.put(RAW_BUCKET, key, CONTENT)
    with pytest.raises(PermissionDeniedError) as exc:
        await service.complete(complete_payload(key, CONTENT), user_id=USER_ID)
    assert exc.value.code == 1002
    assert producer.published == []


async def test_complete_missing_object_returns_3001() -> None:
    """对象不存在（未上传/已被生命周期清理）→ 3001。"""
    service, _, producer = make_service()
    key = object_key()
    with pytest.raises(ResourceNotFoundError) as exc:
        await service.complete(complete_payload(key, CONTENT), user_id=USER_ID)
    assert exc.value.code == 3001
    assert producer.published == []


# ---------------------------------------------------------------------------
# complete：完整性校验（审查 R2）
# ---------------------------------------------------------------------------
async def test_complete_size_mismatch_returns_6001() -> None:
    """声明 size_bytes 与对象元数据不符 → 6001（附 field/expected/actual 上下文）。"""
    service, storage, producer = make_service()
    key = object_key()
    storage.put(RAW_BUCKET, key, CONTENT)
    payload = complete_payload(key, CONTENT, size_bytes=len(CONTENT) + 1)
    with pytest.raises(OtaPackageChecksumError) as exc:
        await service.complete(payload, user_id=USER_ID)
    assert exc.value.code == 6001
    assert exc.value.details["field"] == "size_bytes"
    assert producer.published == []


async def test_complete_md5_mismatch_returns_6001() -> None:
    """MD5 声明值与对象实际内容不符 → 6001。"""
    service, storage, _ = make_service()
    key = object_key()
    storage.put(RAW_BUCKET, key, CONTENT)
    payload = complete_payload(key, CONTENT, md5=hashlib.md5(b"other").hexdigest())
    with pytest.raises(OtaPackageChecksumError) as exc:
        await service.complete(payload, user_id=USER_ID)
    assert exc.value.details["field"] == "md5"


async def test_complete_sha256_mismatch_returns_6001() -> None:
    """审查 R2 核心回归：SHA-256 必须参与校验。

    体积与 MD5 均正确、仅 SHA-256 声明错误——修复前实现从不校验 sha256，
    该请求会被错误地判定为「校验通过」并投递未验证的哈希值。
    """
    service, storage, producer = make_service()
    key = object_key()
    storage.put(RAW_BUCKET, key, CONTENT)
    payload = complete_payload(key, CONTENT, sha256=hashlib.sha256(b"other").hexdigest())
    with pytest.raises(OtaPackageChecksumError) as exc:
        await service.complete(payload, user_id=USER_ID)
    assert exc.value.code == 6001
    assert exc.value.details["field"] == "sha256"
    assert exc.value.details["actual"] == hashlib.sha256(CONTENT).hexdigest()
    assert producer.published == []


async def test_complete_detects_tampered_content_despite_multipart_etag() -> None:
    """审查 R2 核心回归：不得以 ETag 代替内容校验。

    multipart 对象的 ETag 带 ``-N`` 后缀（非整体 MD5），修复前实现对该类对象
    跳过全部摘要比对；本用例将对象内容替换为「等长的另一段内容」并声明原内容摘要，
    必须被 6001 拒绝（证明判定依据是流式重算的真实摘要）。
    """
    service, storage, producer = make_service()
    key = object_key()
    tampered = CONTENT.replace(b"-", b"_")
    assert len(tampered) == len(CONTENT)
    storage.put(RAW_BUCKET, key, tampered)
    storage.etag_overrides[(RAW_BUCKET, key)] = "d41d8cd98f00b204e9800998ecf8427e-3"
    with pytest.raises(OtaPackageChecksumError) as exc:
        await service.complete(complete_payload(key, CONTENT), user_id=USER_ID)
    assert exc.value.code == 6001
    assert exc.value.details["field"] in {"md5", "sha256"}
    assert producer.published == []


# ---------------------------------------------------------------------------
# complete：成功路径 / multipart / Kafka 故障
# ---------------------------------------------------------------------------
async def test_complete_success_marks_verified_and_publishes_sensor_file() -> None:
    """校验全部通过 → verified=True 并投递 sensor_file（载荷字段与 Schema 一致）。"""
    service, storage, producer = make_service()
    key = object_key()
    storage.put(RAW_BUCKET, key, CONTENT)
    data = await service.complete(complete_payload(key, CONTENT), user_id=USER_ID)

    assert data.verified is True
    assert data.notify_topic == SENSOR_FILE_TOPIC
    assert data.size_bytes == len(CONTENT)
    assert data.md5 == hashlib.md5(CONTENT).hexdigest()
    assert data.sha256 == hashlib.sha256(CONTENT).hexdigest()
    assert producer.published == [
        {
            "vehicle_id": VEHICLE_ID,
            "timestamp": TIMESTAMP,
            "bucket": RAW_BUCKET,
            "object_key": key,
            "data_type": FileDataType.POINT_CLOUD.value,
            "size_bytes": len(CONTENT),
            "md5": hashlib.md5(CONTENT).hexdigest(),
            "sha256": hashlib.sha256(CONTENT).hexdigest(),
        }
    ]


async def test_complete_kafka_failure_propagates_5001() -> None:
    """Kafka 不可用 → 5001（不得吞异常降级为「上传成功」）。"""
    service, storage, producer = make_service()
    key = object_key()
    storage.put(RAW_BUCKET, key, CONTENT)
    producer.fail_with = ServiceUnavailableError("Kafka 不可用，上传完成通知发布失败")
    with pytest.raises(ServiceUnavailableError) as exc:
        await service.complete(complete_payload(key, CONTENT), user_id=USER_ID)
    assert exc.value.code == 5001


async def test_complete_multipart_merges_parts_then_verifies_content() -> None:
    """multipart：先合并分片，再按合并后对象内容校验（真实摘要）。"""
    service, storage, _ = make_service()
    part_a, part_b = b"bag-part-1", b"bag-part-2"
    merged = part_a + part_b
    presign = await service.presign(
        FilePresignRequest(
            vehicle_id=VEHICLE_ID,
            bucket=UploadBucket.ROSBAG,
            data_type=FileDataType.ROSBAG,
            file_name="run.bag",
            file_size=len(merged),
            part_count=2,
        )
    )
    assert presign.upload_id is not None
    storage.put_part(presign.upload_id, part_a)
    storage.put_part(presign.upload_id, part_b)

    payload = FileCompleteRequest(
        vehicle_id=VEHICLE_ID,
        bucket=UploadBucket.ROSBAG,
        object_key=presign.object_key,
        size_bytes=len(merged),
        md5=hashlib.md5(merged).hexdigest(),
        sha256=hashlib.sha256(merged).hexdigest(),
        upload_id=presign.upload_id,
        parts=[
            CompletedPart(part_number=1, etag='"etag-1"'),
            CompletedPart(part_number=2, etag="etag-2"),
        ],
        timestamp=TIMESTAMP,
    )
    data = await service.complete(payload, user_id=USER_ID)

    assert data.verified is True
    assert storage.objects[(ROSBAG_BUCKET, presign.object_key)] == merged
    assert data.data_type == FileDataType.ROSBAG


async def test_complete_multipart_merge_failure_aborts_and_returns_2001() -> None:
    """合并失败（非法 upload_id）→ 中止服务端会话（回滚）+ 2001。"""
    service, storage, producer = make_service()
    key = object_key(FileDataType.ROSBAG)
    payload = FileCompleteRequest(
        vehicle_id=VEHICLE_ID,
        bucket=UploadBucket.ROSBAG,
        object_key=key,
        size_bytes=len(CONTENT),
        md5=hashlib.md5(CONTENT).hexdigest(),
        sha256=hashlib.sha256(CONTENT).hexdigest(),
        upload_id="upload-unknown",
        parts=[CompletedPart(part_number=1, etag="etag-1")],
        timestamp=TIMESTAMP,
    )
    with pytest.raises(InvalidParameterError) as exc:
        await service.complete(payload, user_id=USER_ID)
    assert exc.value.code == 2001
    assert storage.aborted_sessions == [(ROSBAG_BUCKET, key, "upload-unknown")]
    assert producer.published == []


async def test_complete_multipart_requires_upload_id_and_parts_together() -> None:
    """multipart 参数必须成对提供（仅有 upload_id 或缺 parts → 2001）。"""
    service, storage, _ = make_service()
    key = object_key(FileDataType.ROSBAG)
    payload = FileCompleteRequest(
        vehicle_id=VEHICLE_ID,
        bucket=UploadBucket.ROSBAG,
        object_key=key,
        size_bytes=len(CONTENT),
        md5=hashlib.md5(CONTENT).hexdigest(),
        sha256=hashlib.sha256(CONTENT).hexdigest(),
        upload_id="upload-1",
        timestamp=TIMESTAMP,
    )
    with pytest.raises(InvalidParameterError) as exc:
        await service.complete(payload, user_id=USER_ID)
    assert exc.value.code == 2001
    assert storage.aborted_sessions == []  # 参数校验失败发生在合并之前，无需回滚


# ---------------------------------------------------------------------------
# list_files
# ---------------------------------------------------------------------------
async def test_list_files_scopes_prefix_to_vehicle_and_presigns_download() -> None:
    """清单：车辆前缀隔离 + limit+1 截断探测 + 即时签发 15 分钟下载地址。"""
    service, storage, _ = make_service()
    mine = f"{VEHICLE_ID}/2026-09-18/point_cloud/1724035200_1.pcd"
    other = "HUNTER-002/2026-09-18/point_cloud/1724035200_2.pcd"
    storage.put(RAW_BUCKET, mine, CONTENT)
    storage.put(RAW_BUCKET, other, CONTENT)

    data = await service.list_files(
        vehicle_id=VEHICLE_ID, bucket=RAW_BUCKET, prefix=None, marker=None, limit=100
    )

    assert [item.object_key for item in data.items] == [mine]
    assert data.truncated is False and data.next_marker is None
    assert data.items[0].download_url.startswith(f"https://minio.test/{RAW_BUCKET}/")
    assert data.items[0].expires_in == 900
    assert storage.list_calls[0]["prefix"] == f"{VEHICLE_ID}/"
    assert storage.list_calls[0]["max_keys"] == 101  # limit + 1：用于判断是否还有下一页


async def test_list_files_truncates_and_returns_marker_cursor() -> None:
    """超过 limit → 截断并给出 next_marker（游标分页，客户端据此取下一页）。"""
    service, storage, _ = make_service()
    keys = [f"{VEHICLE_ID}/2026-09-18/point_cloud/1724035200_{seq}.pcd" for seq in range(1, 4)]
    for key in keys:
        storage.put(RAW_BUCKET, key, CONTENT)

    data = await service.list_files(
        vehicle_id=VEHICLE_ID, bucket=RAW_BUCKET, prefix=None, marker=None, limit=2
    )

    assert [item.object_key for item in data.items] == keys[:2]
    assert data.truncated is True
    assert data.next_marker == keys[1]

    page2 = await service.list_files(
        vehicle_id=VEHICLE_ID, bucket=RAW_BUCKET, prefix=None, marker=data.next_marker, limit=2
    )
    assert [item.object_key for item in page2.items] == keys[2:]
    assert page2.truncated is False and page2.next_marker is None


async def test_list_files_without_vehicle_lists_all_buckets_objects() -> None:
    """未指定 vehicle_id 时不加车辆前缀（运维/管理端跨车辆检索场景）。"""
    service, storage, _ = make_service()
    storage.put(RAW_BUCKET, f"{VEHICLE_ID}/2026-09-18/point_cloud/a.pcd", CONTENT)
    storage.put(RAW_BUCKET, "HUNTER-002/2026-09-18/point_cloud/b.pcd", CONTENT)

    data = await service.list_files(
        vehicle_id=None, bucket=RAW_BUCKET, prefix=None, marker=None, limit=10
    )

    assert len(data.items) == 2
    assert storage.list_calls[0]["prefix"] == ""
