"""版本仓库端点测试（契约 ota-service.yaml versions 组；数据来自契约示例值）。"""
from __future__ import annotations

import hashlib
from typing import Any

import pytest
from httpx import AsyncClient

from app.tests.conftest import (
    ADMIN_HEADERS,
    DEFAULT_MD5,
    DEFAULT_SHA256,
    PACKAGE_BYTES,
    VIEWER_HEADERS,
    make_version,
)


def _create_payload(**overrides: Any) -> dict[str, Any]:
    """契约 OtaVersionCreateRequest 示例载荷（OtaVersionCreateRequest.example 衍生）。"""
    payload: dict[str, Any] = {
        "version_name": "V1.2.0",
        "version_code": 10200,
        "release_type": "formal",
        "package_size": len(PACKAGE_BYTES),
        "package_md5": DEFAULT_MD5,
        "package_sha256": DEFAULT_SHA256,
        "signature": "MEQCIF9kRSA2048BASE64SIGNATURE",
        "changelog": {"features": ["感知模型升级至 v2.1"], "fixes": ["修复远程操控时延抖动"]},
        "applicable_models": ["HUNTER_SE"],
        "part_count": 1,
    }
    payload.update(overrides)
    return payload


async def test_list_versions_requires_auth(client: AsyncClient) -> None:
    """缺少网关注入头 → 401 + code=1001（契约 Unauthenticated）。"""
    resp = await client.get("/api/v1/ota/versions")
    assert resp.status_code == 401
    assert resp.json()["code"] == 1001


async def test_list_versions_forbidden_for_viewer(client: AsyncClient) -> None:
    """viewer 无 ota:read 角色 → 403 + code=1002（契约 Forbidden）。"""
    resp = await client.get("/api/v1/ota/versions", headers=VIEWER_HEADERS)
    assert resp.status_code == 403
    assert resp.json()["code"] == 1002


async def test_list_versions_happy_path(client: AsyncClient, ota_env: Any) -> None:
    """列表返回分页结构 + 即时签发的 15 分钟下载预签名地址。"""
    row = make_version()
    row2 = make_version(status="draft", code=10300, name="V1.3.0")
    ota_env.versions.rows[row.version_id] = row
    ota_env.versions.rows[row2.version_id] = row2
    resp = await client.get("/api/v1/ota/versions", headers=ADMIN_HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    assert body["code"] == 0
    assert body["data"]["total"] == 2
    assert len(body["data"]["items"]) == 2
    item = body["data"]["items"][0]
    assert item["package_download_url"].startswith("https://minio.test/")
    assert resp.headers.get("X-Request-ID")


async def test_create_version_returns_presigned_upload(client: AsyncClient) -> None:
    """创建草稿：返回 version(draft) + upload(object_key/upload_url/expires_in=3600)。"""
    resp = await client.post(
        "/api/v1/ota/versions", json=_create_payload(), headers=ADMIN_HEADERS
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["code"] == 0
    assert body["data"]["version"]["status"] == "draft"
    assert body["data"]["version"]["release_time"] is None
    upload = body["data"]["upload"]
    assert upload["expires_in"] == 3600
    assert upload["part_count"] == 1
    assert upload["object_key"] == "hunter-edge/ota/HUNTER_SE/V1.2.0/10200/package.tar.gz"


async def test_create_version_multipart_parts(client: AsyncClient) -> None:
    """part_count > 1 → 返回分片上传地址清单（契约 OtaVersionUploadInfo.parts）。"""
    resp = await client.post(
        "/api/v1/ota/versions", json=_create_payload(part_count=3), headers=ADMIN_HEADERS
    )
    parts = resp.json()["data"]["upload"]["parts"]
    assert parts is not None
    assert [p["part_number"] for p in parts] == [1, 2, 3]


async def test_create_version_duplicate_code_conflict(
    client: AsyncClient, ota_env: Any
) -> None:
    """version_code 唯一冲突 → 409 + code=3002 + data.field（契约 Conflict.already_exists）。"""
    row = make_version()
    ota_env.versions.rows[row.version_id] = row
    resp = await client.post(
        "/api/v1/ota/versions", json=_create_payload(), headers=ADMIN_HEADERS
    )
    assert resp.status_code == 409
    body = resp.json()
    assert body["code"] == 3002
    assert body["data"]["field"] in {"version_code", "version_name"}


async def test_create_version_rollback_protection(
    client: AsyncClient, ota_env: Any
) -> None:
    """version_code 未大于已发布最大编码 → 422 + code=6003（防回滚门禁；编码唯一性之外）。

    用例取 version_code=10100（唯一）但小于已发布最大编码 10200，命中 6003 而非 3002。
    """
    row = make_version(code=10200)
    ota_env.versions.rows[row.version_id] = row
    resp = await client.post(
        "/api/v1/ota/versions",
        json=_create_payload(version_code=10100, version_name="V1.0.0"),
        headers=ADMIN_HEADERS,
    )
    assert resp.status_code == 422
    body = resp.json()
    assert body["code"] == 6003
    assert body["data"]["max_published"] == 10200


async def test_version_request_validation_error(client: AsyncClient) -> None:
    """请求体校验失败（md5 非法）→ 422 + code=2001（附录 A，禁止自定义）。"""
    payload = _create_payload(package_md5="not-a-md5")
    resp = await client.post("/api/v1/ota/versions", json=payload, headers=ADMIN_HEADERS)
    assert resp.status_code == 422
    assert resp.json()["code"] == 2001


async def test_get_version_detail_includes_task_count(
    client: AsyncClient, ota_env: Any
) -> None:
    """详情含 task_count 引用评估与即时下载地址（契约 OtaVersionDetail）。"""
    row = make_version()
    ota_env.versions.rows[row.version_id] = row
    resp = await client.get(f"/api/v1/ota/versions/{row.version_id}", headers=ADMIN_HEADERS)
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["task_count"] == 0
    assert data["package_download_url"].startswith("https://minio.test/")


async def test_get_version_not_found(client: AsyncClient) -> None:
    """不存在 → 404 + code=3001（契约 ResourceNotFound）。"""
    resp = await client.get(
        "/api/v1/ota/versions/00000000-0000-4000-8000-000000000000", headers=ADMIN_HEADERS
    )
    assert resp.status_code == 404
    assert resp.json()["code"] == 3001


async def test_publish_success_returns_five_checks(
    client: AsyncClient, ota_env: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """发布成功：五项校验全 true + status=published（契约 OtaVersionPublishData）。

    验签以替身放行（真实 RSA-2048 验签链路见 test_signature.py，importorskip cryptography）。
    """
    import app.services.versions as versions_module

    row = make_version(status="draft")
    ota_env.versions.rows[row.version_id] = row
    object_key = row.package_url.removeprefix("s3://").split("/", 1)[1]
    ota_env.storage.put(object_key, PACKAGE_BYTES)
    monkeypatch.setattr(versions_module, "verify_package_signature", lambda *args: True)
    resp = await client.post(
        f"/api/v1/ota/versions/{row.version_id}/publish", headers=ADMIN_HEADERS
    )
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["status"] == "published"
    assert all(data["checks"].values()), data["checks"]
    assert data["release_time"] > 0


async def test_publish_reports_verification_switches_honestly(
    client: AsyncClient, ota_env: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """审查 Y3 回归：校验开关关闭时 checks 必须如实回报 False（禁止假声明）。

    修复前无论是否真正执行校验，``sha256_verified`` / ``signature_verified`` 恒为 true，
    前端与审计据此会误判"已验签"，掩盖高危配置（契约 pending：开关是否允许关闭）。
    """
    from app.config import settings

    monkeypatch.setattr(settings, "ota_package_verify_sha256", False)
    monkeypatch.setattr(settings, "ota_package_verify_signature", False)

    row = make_version(status="draft", name="V1.3.0", code=10300)
    # 声明的 md5 与实际内容一致（通过 MD5 门禁），但 sha256 为错误值：
    # 开关关闭时该差异被跳过 → 必须如实回报 sha256_verified=false（修复前为 true）
    row.package_sha256 = hashlib.sha256(b"unrelated-content").hexdigest()
    ota_env.versions.rows[row.version_id] = row
    object_key = row.package_url.removeprefix("s3://").split("/", 1)[1]
    ota_env.storage.put(object_key, PACKAGE_BYTES)
    resp = await client.post(
        f"/api/v1/ota/versions/{row.version_id}/publish", headers=ADMIN_HEADERS
    )

    assert resp.status_code == 200
    checks = resp.json()["data"]["checks"]
    assert checks["sha256_verified"] is False
    assert checks["signature_verified"] is False
    # 未关闭的校验项仍如实为 true（size/MD5 恒校验）
    assert checks["package_size_verified"] is True
    assert checks["md5_verified"] is True


async def test_publish_checksum_mismatch_returns_6001(
    client: AsyncClient, ota_env: Any
) -> None:
    """SHA-256 不匹配 → 422 + code=6001 + expected/actual（契约 checksum_failed 示例）。"""
    row = make_version(status="draft", name="V9.9.9", code=99900)
    ota_env.versions.rows[row.version_id] = row
    object_key = row.package_url.removeprefix("s3://").split("/", 1)[1]
    ota_env.storage.put(object_key, b"tampered-content")
    resp = await client.post(
        f"/api/v1/ota/versions/{row.version_id}/publish", headers=ADMIN_HEADERS
    )
    assert resp.status_code == 422
    body = resp.json()
    assert body["code"] == 6001
    assert set(body["data"]) >= {"expected", "actual"}


async def test_publish_non_draft_conflict(client: AsyncClient, ota_env: Any) -> None:
    """重复发布（非 draft）→ 409 + code=3003 + current_status（契约 state_conflict 示例）。"""
    row = make_version(status="published")
    ota_env.versions.rows[row.version_id] = row
    resp = await client.post(
        f"/api/v1/ota/versions/{row.version_id}/publish", headers=ADMIN_HEADERS
    )
    assert resp.status_code == 409
    assert resp.json()["code"] == 3003


async def test_deprecate_published_version(client: AsyncClient, ota_env: Any) -> None:
    """退役：published → deprecated，返回 OtaVersionDetailResponse。"""
    row = make_version()
    ota_env.versions.rows[row.version_id] = row
    resp = await client.post(
        f"/api/v1/ota/versions/{row.version_id}/deprecate",
        json={"status": "deprecated", "reason": "灰度观察期内发现定位漂移"},
        headers=ADMIN_HEADERS,
    )
    assert resp.status_code == 200
    assert resp.json()["data"]["status"] == "deprecated"


async def test_deprecate_requires_published_state(
    client: AsyncClient, ota_env: Any
) -> None:
    """draft 版本不可退役 → 409 + code=3003。"""
    row = make_version(status="draft")
    ota_env.versions.rows[row.version_id] = row
    resp = await client.post(
        f"/api/v1/ota/versions/{row.version_id}/deprecate",
        json={"status": "disabled", "reason": "误发布"},
        headers=ADMIN_HEADERS,
    )
    assert resp.status_code == 409
    assert resp.json()["code"] == 3003
