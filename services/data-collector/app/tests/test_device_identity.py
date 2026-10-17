"""设备主体身份测试（契约 x-hunter-device-ingress / pending #6 决议，V1.21.13）。

覆盖：
- require_uploader_identity 双主体判定（设备主体角色/车辆头成对出现、混合角色拒绝、
  控制台代传路线保持、无权限角色 1002）；
- FileService 层 vehicle_id == 证书 CN 强校验（presign/complete，越权 1002）。
"""
from __future__ import annotations

import pytest
from hunter_common.exceptions import PermissionDeniedError

from app.config import settings
from app.core.dependencies import UploaderPrincipal, require_uploader_identity
from app.schemas.common import FileDataType, UploadBucket
from app.schemas.files import FilePresignRequest
from app.services.files import FileService
from app.tests.fakes import FakeMinioStorage, FakeSensorFileProducer

USER_ID = "11111111-1111-4111-8111-111111111111"
VEHICLE_ID = "HUNTER-001"


def _principal(roles: set[str], device_vehicle_id: str | None) -> UploaderPrincipal:
    """直接调用依赖函数（_require_forwarded_auth 的头/HMAC 校验由共享库测试覆盖）。"""
    return require_uploader_identity((USER_ID, roles), device_vehicle_id)


# ---------------------------------------------------------------------------
# require_uploader_identity 主体判定
# ---------------------------------------------------------------------------


def test_device_principal_requires_role_and_vehicle_header_paired() -> None:
    p = _principal({"device"}, VEHICLE_ID)
    assert p.device_vehicle_id == VEHICLE_ID
    assert p.user_id == USER_ID


def test_device_role_without_header_rejected() -> None:
    with pytest.raises(PermissionDeniedError) as exc:
        _principal({"device"}, None)
    assert exc.value.code == 1002


def test_device_header_with_mixed_roles_rejected() -> None:
    """设备头 + 非纯 device 角色（如混入 admin）→ 拒绝（设备不得携带控制台能力）。"""
    with pytest.raises(PermissionDeniedError):
        _principal({"device", "admin"}, VEHICLE_ID)


def test_console_operator_still_supported_for_delegated_upload() -> None:
    """控制台代传路线保持（operator 属 data:read 白名单，device_vehicle_id=None）。"""
    p = _principal({"operator"}, None)
    assert p.device_vehicle_id is None


def test_unprivileged_role_rejected() -> None:
    with pytest.raises(PermissionDeniedError):
        _principal({"viewer"}, None)


def test_device_principal_without_read_whitelist_membership() -> None:
    """定案能力面：device 不进入 data:read 白名单 → 查询端点（require_read_permission）
    对设备主体天然 1002；此处校验白名单配置本身不含 device。"""
    assert "device" not in settings.data_read_role_set
    assert "device" not in settings.data_execute_role_set


# ---------------------------------------------------------------------------
# FileService：vehicle_id == 证书 CN 强校验
# ---------------------------------------------------------------------------


def make_service() -> tuple[FileService, FakeMinioStorage, FakeSensorFileProducer]:
    storage = FakeMinioStorage()
    producer = FakeSensorFileProducer()
    return FileService(storage, producer), storage, producer


def _presign_request(vehicle_id: str) -> FilePresignRequest:
    return FilePresignRequest(
        vehicle_id=vehicle_id,
        bucket=UploadBucket.RAW_DATA,
        data_type=FileDataType.POINT_CLOUD,
        file_name="lidar.pcd",
        file_size=1024,
    )


async def test_presign_device_mismatch_rejected() -> None:
    """已认证设备为他车目录申请预签名 → 1002（契约：vehicle_id 一律以 CN 为准）。"""
    service, _, _ = make_service()
    with pytest.raises(PermissionDeniedError) as exc:
        await service.presign(_presign_request("HUNTER-999"), device_vehicle_id=VEHICLE_ID)
    assert exc.value.code == 1002
    assert "CN" in exc.value.message


async def test_presign_device_matched_ok() -> None:
    service, _, _ = make_service()
    data = await service.presign(_presign_request(VEHICLE_ID), device_vehicle_id=VEHICLE_ID)
    assert data.object_key.startswith(f"{VEHICLE_ID}/")


async def test_presign_console_principal_not_constrained() -> None:
    """控制台代传（device_vehicle_id=None）不触发 CN 校验（越权收敛在 data:read 角色层）。"""
    service, _, _ = make_service()
    data = await service.presign(_presign_request("HUNTER-999"), device_vehicle_id=None)
    assert data.object_key.startswith("HUNTER-999/")


async def test_complete_device_mismatch_rejected_before_storage_calls() -> None:
    from app.tests.test_file_service import complete_payload  # 复用既有载荷构造

    service, _, _ = make_service()
    payload = complete_payload(f"{VEHICLE_ID}/2026-09-18/point_cloud/1724035200_1.pcd", b"x")
    # 对象并未写入存储：若 CN 校验未最先执行，到达 head_object 会先报 3001 而非 1002
    with pytest.raises(PermissionDeniedError) as exc:
        await service.complete(payload, user_id=USER_ID, device_vehicle_id="OTHER-001")
    assert exc.value.code == 1002
