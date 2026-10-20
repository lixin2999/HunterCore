"""FileService 单元测试（审查 R2 / R6：补齐业务逻辑行为测试）。

覆盖：
- 预签名：Bucket↔data_type 映射（1002）、服务端命名规范、multipart 分片地址；
- 上传完成：路径归属（1002）、对象缺失（3001）、size/MD5/SHA-256 三类校验（6001）、
  multipart 合并与合并失败回滚（2001）、sensor_file 投递载荷、Kafka 故障（5001）；
- 生命周期归类打标（G-12）：hunter-rosbag 按 retention 打 Tag、非 rosbag 不打标、
  打标失败 5001 且不发布 sensor_file；
- 清单：车辆/日期/数据类型前缀段隔离、limit+1 截断与 next_marker 游标、预签名下载地址。

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
VIDEO_BUCKET = UploadBucket.VIDEO.value
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
# complete：生命周期归类打标（G-12）
# ---------------------------------------------------------------------------
async def test_complete_rosbag_tags_regular_by_default() -> None:
    """hunter-rosbag 缺省归类 regular：校验通过后打 Tag hunter-retention=regular。"""
    service, storage, producer = make_service()
    key = object_key(FileDataType.ROSBAG)
    storage.put(ROSBAG_BUCKET, key, CONTENT)
    payload = complete_payload(key, CONTENT, bucket=UploadBucket.ROSBAG)
    data = await service.complete(payload, user_id=USER_ID)
    assert data.verified is True
    assert storage.tags == {(ROSBAG_BUCKET, key): "regular"}
    assert len(producer.published) == 1


async def test_complete_rosbag_event_retention_tagged_event() -> None:
    """事件包显式声明 retention=event → 打 Tag hunter-retention=event（永久保留）。"""
    service, storage, _ = make_service()
    key = object_key(FileDataType.ROSBAG)
    storage.put(ROSBAG_BUCKET, key, CONTENT)
    payload = complete_payload(key, CONTENT, bucket=UploadBucket.ROSBAG, retention="event")
    await service.complete(payload, user_id=USER_ID)
    assert storage.tags[(ROSBAG_BUCKET, key)] == "event"


async def test_complete_non_rosbag_bucket_skips_tagging() -> None:
    """打标义务仅限 hunter-rosbag（其余 Bucket 为整桶生命周期规则）。"""
    service, storage, _ = make_service()
    key = object_key()
    storage.put(RAW_BUCKET, key, CONTENT)
    await service.complete(complete_payload(key, CONTENT), user_id=USER_ID)
    assert storage.tags == {}


async def test_complete_tagging_failure_blocks_publish_5001() -> None:
    """G-12 核心约束：打标失败 → 5001 且不发布 sensor_file（未打标等同永久，禁止静默降级）。"""
    service, storage, producer = make_service()
    key = object_key(FileDataType.ROSBAG)
    storage.put(ROSBAG_BUCKET, key, CONTENT)
    storage.tag_fail_with = RuntimeError("MinIO 不可用")
    payload = complete_payload(key, CONTENT, bucket=UploadBucket.ROSBAG)
    with pytest.raises(ServiceUnavailableError) as exc:
        await service.complete(payload, user_id=USER_ID)
    assert exc.value.code == 5001
    assert exc.value.details["retention"] == "regular"
    assert producer.published == []


async def test_complete_integrity_failure_precedes_tagging() -> None:
    """校验不通过（6001）时不得打标：打标仅在完整性校验之后发生。"""
    service, storage, _ = make_service()
    key = object_key(FileDataType.ROSBAG)
    storage.put(ROSBAG_BUCKET, key, CONTENT)
    payload = complete_payload(
        key, CONTENT, bucket=UploadBucket.ROSBAG, sha256=hashlib.sha256(b"other").hexdigest()
    )
    with pytest.raises(OtaPackageChecksumError):
        await service.complete(payload, user_id=USER_ID)
    assert storage.tags == {}


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
        vehicle_id=VEHICLE_ID, bucket=RAW_BUCKET, date=None, data_type=None, marker=None, limit=100
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
        vehicle_id=VEHICLE_ID, bucket=RAW_BUCKET, date=None, data_type=None, marker=None, limit=2
    )

    assert [item.object_key for item in data.items] == keys[:2]
    assert data.truncated is True
    assert data.next_marker == keys[1]

    page2 = await service.list_files(
        vehicle_id=VEHICLE_ID,
        bucket=RAW_BUCKET,
        date=None,
        data_type=None,
        marker=data.next_marker,
        limit=2,
    )
    assert [item.object_key for item in page2.items] == keys[2:]
    assert page2.truncated is False and page2.next_marker is None


async def test_list_files_without_vehicle_lists_all_buckets_objects() -> None:
    """未指定 vehicle_id 时不加车辆前缀（运维/管理端跨车辆检索场景）。"""
    service, storage, _ = make_service()
    storage.put(RAW_BUCKET, f"{VEHICLE_ID}/2026-09-18/point_cloud/a.pcd", CONTENT)
    storage.put(RAW_BUCKET, "HUNTER-002/2026-09-18/point_cloud/b.pcd", CONTENT)

    data = await service.list_files(
        vehicle_id=None, bucket=RAW_BUCKET, date=None, data_type=None, marker=None, limit=10
    )

    assert len(data.items) == 2
    assert storage.list_calls[0]["prefix"] == ""


async def test_list_files_filters_by_date_and_data_type_prefix_segments() -> None:
    """清单：date / data_type 必须落进对象键前缀（契约 GET /files 的 date/data_type 过滤）。

    回归（V1.21.16）：旧实现漏了这两个参数，前端按契约发 date/data_type 被静默忽略。
    """
    service, storage, _ = make_service()
    wanted = f"{VEHICLE_ID}/2026-09-18/rosbag/1724035200_1.bag"
    other_date = f"{VEHICLE_ID}/2026-09-19/rosbag/1724035200_2.bag"
    other_type = f"{VEHICLE_ID}/2026-09-18/point_cloud/1724035200_3.pcd"
    for key in (wanted, other_date, other_type):
        storage.put(RAW_BUCKET, key, CONTENT)

    data = await service.list_files(
        vehicle_id=VEHICLE_ID,
        bucket=RAW_BUCKET,
        date="2026-09-18",
        data_type="rosbag",
        marker=None,
        limit=100,
    )

    assert storage.list_calls[0]["prefix"] == f"{VEHICLE_ID}/2026-09-18/rosbag/"
    assert [item.object_key for item in data.items] == [wanted]

    # 只给 date（含跨车辆）：前缀 = {date}/，不带车辆段也不带类型段
    await service.list_files(
        vehicle_id=None, bucket=RAW_BUCKET, date="2026-09-19", data_type=None, marker=None, limit=100
    )
    assert storage.list_calls[-1]["prefix"] == "2026-09-19/"


# ---------------------------------------------------------------------------
# delete_files（V1.21.20：批量删除 / 清理无效记录）
# ---------------------------------------------------------------------------
async def test_delete_files_removes_objects_and_counts_deleted() -> None:
    """删除存在的对象：从存储移除并回报 deleted/deleted_count。"""
    service, storage, _ = make_service()
    key_a = f"{VEHICLE_ID}/2026-09-18/video/1724035200_1.mp4"
    key_b = f"{VEHICLE_ID}/2026-09-18/video/1724035200_2.json"
    storage.put(RAW_BUCKET, key_a, CONTENT)
    storage.put(RAW_BUCKET, key_b, CONTENT)

    data = await service.delete_files(bucket=RAW_BUCKET, object_keys=[key_a, key_b], user_id=USER_ID)

    assert (RAW_BUCKET, key_a) not in storage.objects
    assert (RAW_BUCKET, key_b) not in storage.objects
    assert sorted(data.deleted) == sorted([key_a, key_b])
    assert data.failed == []
    assert data.deleted_count == 2
    assert data.bucket == RAW_BUCKET


async def test_delete_files_dedupes_and_drops_empty_keys() -> None:
    """去重去空（保序）：传给存储的键列表已剔除重复与空串。"""
    service, storage, _ = make_service()
    key = f"{VEHICLE_ID}/2026-09-18/video/1724035200_1.mp4"
    storage.put(RAW_BUCKET, key, CONTENT)

    await service.delete_files(
        bucket=RAW_BUCKET, object_keys=[key, "", key, "  "], user_id=USER_ID
    )

    # 空串被剔除；"  "（含空白）非空保留；key 去重后仅一次
    assert storage.delete_calls[-1] == (RAW_BUCKET, [key, "  "])


async def test_delete_files_all_empty_keys_raises_invalid_parameter() -> None:
    """object_keys 全为空 → InvalidParameterError（2001），不下发存储调用。"""
    service, storage, _ = make_service()
    with pytest.raises(InvalidParameterError):
        await service.delete_files(bucket=RAW_BUCKET, object_keys=["", ""], user_id=USER_ID)
    assert storage.delete_calls == []


async def test_delete_files_is_idempotent_for_missing_objects() -> None:
    """幂等：不存在的对象亦计 deleted（适配“清空无效记录”）。"""
    service, _storage, _ = make_service()
    missing = f"{VEHICLE_ID}/2026-09-18/video/1724035200_9.mp4"

    data = await service.delete_files(bucket=RAW_BUCKET, object_keys=[missing], user_id=USER_ID)

    assert data.deleted == [missing]
    assert data.deleted_count == 1
    assert data.failed == []


async def test_delete_files_storage_failure_maps_to_service_unavailable() -> None:
    """存储故障 → 5001（ServiceUnavailableError），不静默返回部分成功。"""
    service, storage, _ = make_service()
    storage.delete_fail_with = RuntimeError("minio down")
    with pytest.raises(ServiceUnavailableError):
        await service.delete_files(
            bucket=RAW_BUCKET, object_keys=[f"{VEHICLE_ID}/a.pcd"], user_id=USER_ID
        )


async def test_delete_files_reports_partial_failures() -> None:
    """部分失败：失败键进 failed，不计入 deleted/deleted_count。"""
    service, storage, _ = make_service()
    ok_key = f"{VEHICLE_ID}/2026-09-18/video/a.mp4"
    bad_key = f"{VEHICLE_ID}/2026-09-18/video/b.mp4"
    storage.put(RAW_BUCKET, ok_key, CONTENT)
    storage.put(RAW_BUCKET, bad_key, CONTENT)
    storage.delete_error_keys = {bad_key}

    data = await service.delete_files(
        bucket=RAW_BUCKET, object_keys=[ok_key, bad_key], user_id=USER_ID
    )

    assert data.deleted == [ok_key]
    assert data.deleted_count == 1
    assert [f.object_key for f in data.failed] == [bad_key]
    assert (RAW_BUCKET, bad_key) in storage.objects  # 失败键未被移除


# ---------------------------------------------------------------------------
# hunter-video 会话配对删除 + 有效录像护栏（V1.21.22）
# ---------------------------------------------------------------------------
async def test_delete_video_orphan_removes_sidecar_and_zero_byte_video() -> None:
    """孤儿会话（视频 0 B）：删 sidecar 时连带删 0 B 视频，不留失联对象。"""
    service, storage, _ = make_service()
    base = "remote-control/HUNTER-001/2026/10/20/sess-1"
    sidecar, video = f"{base}.json", f"{base}.mp4"
    storage.put(VIDEO_BUCKET, sidecar, b'{"session_id":"sess-1"}')
    storage.put(VIDEO_BUCKET, video, b"")  # 0 B 无效视频

    data = await service.delete_files(bucket=VIDEO_BUCKET, object_keys=[sidecar], user_id=USER_ID)

    assert sorted(data.deleted) == sorted([sidecar, video])
    assert data.skipped == []
    assert (VIDEO_BUCKET, sidecar) not in storage.objects
    assert (VIDEO_BUCKET, video) not in storage.objects


async def test_delete_video_orphan_without_video_only_removes_sidecar() -> None:
    """孤儿 sidecar（视频根本不存在）：仅删 sidecar，不把不存在的键计入 deleted。"""
    service, storage, _ = make_service()
    sidecar = "remote-control/HUNTER-001/2026/10/20/sess-2.json"
    storage.put(VIDEO_BUCKET, sidecar, b"{}")

    data = await service.delete_files(bucket=VIDEO_BUCKET, object_keys=[sidecar], user_id=USER_ID)

    assert data.deleted == [sidecar]
    assert data.deleted_count == 1


async def test_delete_video_valid_session_is_skipped_by_guard() -> None:
    """有效会话（视频 >0 B）默认被护栏跳过：不删 sidecar/视频，计入 skipped（防误删有效录像）。"""
    service, storage, _ = make_service()
    base = "remote-control/HUNTER-001/2026/10/20/sess-3"
    sidecar, video = f"{base}.json", f"{base}.mp4"
    storage.put(VIDEO_BUCKET, sidecar, b"{}")
    storage.put(VIDEO_BUCKET, video, b"real-video-bytes")

    data = await service.delete_files(bucket=VIDEO_BUCKET, object_keys=[sidecar], user_id=USER_ID)

    assert data.deleted == []
    assert data.deleted_count == 0
    assert data.skipped == [sidecar]
    assert data.skipped_count == 1
    assert (VIDEO_BUCKET, sidecar) in storage.objects  # 有效录像未被删
    assert (VIDEO_BUCKET, video) in storage.objects


async def test_delete_video_valid_session_deleted_when_include_valid() -> None:
    """include_valid=True 显式越过护栏：有效会话也配对删除。"""
    service, storage, _ = make_service()
    base = "remote-control/HUNTER-001/2026/10/20/sess-4"
    sidecar, video = f"{base}.json", f"{base}.mp4"
    storage.put(VIDEO_BUCKET, sidecar, b"{}")
    storage.put(VIDEO_BUCKET, video, b"real-video-bytes")

    data = await service.delete_files(
        bucket=VIDEO_BUCKET, object_keys=[sidecar], user_id=USER_ID, include_valid=True
    )

    assert sorted(data.deleted) == sorted([sidecar, video])
    assert data.skipped == []


async def test_delete_video_non_paired_ext_deleted_as_is() -> None:
    """video 桶内非 .json/.mp4 对象：按裸对象删除，无配对展开。"""
    service, storage, _ = make_service()
    key = "remote-control/HUNTER-001/2026/10/20/notes.txt"
    storage.put(VIDEO_BUCKET, key, b"hi")

    data = await service.delete_files(bucket=VIDEO_BUCKET, object_keys=[key], user_id=USER_ID)

    assert data.deleted == [key]


async def test_list_files_annotates_video_orphan_and_valid() -> None:
    """清单标注：有效 sidecar（视频>0）is_orphan=False 且带真实大小；0 B 视频 sidecar is_orphan=True。"""
    service, storage, _ = make_service()
    valid_base = "remote-control/HUNTER-001/2026/10/20/sess-a"
    orphan_base = "remote-control/HUNTER-001/2026/10/20/sess-b"
    storage.put(VIDEO_BUCKET, f"{valid_base}.json", b"{}")
    storage.put(VIDEO_BUCKET, f"{valid_base}.mp4", b"12345")  # 5 B 有效
    storage.put(VIDEO_BUCKET, f"{orphan_base}.json", b"{}")
    storage.put(VIDEO_BUCKET, f"{orphan_base}.mp4", b"")  # 0 B 孤儿

    data = await service.list_files(
        vehicle_id=None, bucket=VIDEO_BUCKET, date=None, data_type=None, marker=None, limit=50
    )
    by_key = {item.object_key: item for item in data.items}

    assert by_key[f"{valid_base}.json"].is_orphan is False
    assert by_key[f"{valid_base}.json"].video_size_bytes == 5
    assert by_key[f"{orphan_base}.json"].is_orphan is True
    assert by_key[f"{orphan_base}.json"].video_size_bytes == 0
    # .mp4 行：有效视频自身大小、非孤儿；0 B 视频为孤儿
    assert by_key[f"{valid_base}.mp4"].video_size_bytes == 5
    assert by_key[f"{valid_base}.mp4"].is_orphan is False
    assert by_key[f"{orphan_base}.mp4"].is_orphan is True


async def test_list_files_no_annotation_for_non_video_bucket() -> None:
    """非 video 桶：不标注（video_size_bytes=None、is_orphan=False），保持通用文件浏览语义。"""
    service, storage, _ = make_service()
    key = f"{VEHICLE_ID}/2026-09-18/video/1724035200_1.mp4"
    storage.put(RAW_BUCKET, key, b"bytes")

    data = await service.list_files(
        vehicle_id=None, bucket=RAW_BUCKET, date=None, data_type=None, marker=None, limit=50
    )

    assert data.items[0].video_size_bytes is None
    assert data.items[0].is_orphan is False
