"""版本仓库模型（契约 components.schemas：OtaVersionItem → OtaVersionDeprecateRequest）。

字段与 DDL 03_ota.sql `ota_svc.ota_versions` 一一对应；该表无 create_time/update_time 列，
响应不返回创建时间（契约 OtaVersionItem.description）。
"""
from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from hunter_common.database.enums import OtaVersionStatus

from app.schemas.common import MD5_PATTERN, OtaApiResponse, SHA256_PATTERN

#: 上传预签名有效期（契约 OtaVersionUploadInfo.expires_in enum [3600]，固定 1 小时不可更改）
PRESIGNED_UPLOAD_EXPIRES_IN: Literal[3600] = 3600


class OtaVersionItem(BaseModel):
    """`ota_svc.ota_versions` 单行 + 服务端即时签发的下载地址（契约 OtaVersionItem）。"""

    model_config = ConfigDict(extra="forbid")

    version_id: UUID = Field(description="版本 ID（UUID 主键）")
    version_name: str = Field(max_length=64, description="版本展示名（唯一）")
    version_code: int = Field(ge=1, description="单调递增编码（防回滚）")
    release_type: str = Field(max_length=32, description="发布类型（契约不设 enum，透传存库值）")
    package_url: str = Field(description="落库对象地址（MinIO hunter-ota-packages，永久保留）")
    package_size: int = Field(ge=1, description="升级包字节数")
    package_md5: str = Field(pattern=MD5_PATTERN, description="升级包 MD5（小写十六进制）")
    package_sha256: str = Field(pattern=SHA256_PATTERN, description="升级包 SHA-256（小写十六进制）")
    signature: str = Field(description="RSA-2048 签名（base64）")
    changelog: dict[str, object] = Field(default_factory=dict, description="变更说明（JSONB）")
    applicable_models: list[str] = Field(min_length=1, description="适用车型")
    status: OtaVersionStatus = Field(description="版本状态（draft/published/deprecated/disabled）")
    release_time: float | None = Field(default=None, description="发布时间（Unix epoch 秒；draft 为 null）")
    package_download_url: str | None = Field(
        default=None, description="即时签发的下载预签名地址（15 分钟，支持 Range 分片下载）"
    )


class OtaVersionDetail(OtaVersionItem):
    """版本详情（契约 OtaVersionDetail = OtaVersionItem + task_count）。"""

    task_count: int = Field(ge=0, description="以该版本为目标的升级任务数（退役前引用评估）")


class OtaVersionList(BaseModel):
    """版本分页数据（契约 OtaVersionList；排序 release_time DESC NULLS LAST, version_code DESC）。"""

    model_config = ConfigDict(extra="forbid")

    items: list[OtaVersionItem] = Field(default_factory=list, description="版本列表")
    total: int = Field(ge=0, description="满足条件的总记录数（DB COUNT(*)）")
    page: int = Field(ge=1, description="页码")
    page_size: int = Field(ge=1, le=200, description="每页条数")


class OtaVersionListResponse(OtaApiResponse):
    """GET /api/v1/ota/versions 统一响应。"""

    data: OtaVersionList | None = None


class OtaVersionDetailResponse(OtaApiResponse):
    """GET /versions/{version_id} 与 POST /versions/{version_id}/deprecate 统一响应。"""

    data: OtaVersionDetail | None = None


class OtaVersionCreateRequest(BaseModel):
    """创建版本草稿 + 申请上传预签名地址（契约 OtaVersionCreateRequest）。

    `package_url` / `object_key` 由服务端生成，客户端不得指定（防越权写入其他对象路径）。
    """

    model_config = ConfigDict(extra="forbid")

    version_name: str = Field(min_length=1, max_length=64, description="版本展示名（唯一；冲突 → 3002）")
    version_code: int = Field(
        ge=1, le=2147483647, description="单调递增编码（须大于同车型已发布最大编码，否则 → 6003）"
    )
    release_type: str = Field(max_length=32, description="发布类型（契约不设 enum，禁止硬编码分支）")
    package_size: int = Field(ge=1, description="升级包字节数")
    package_md5: str = Field(pattern=MD5_PATTERN, description="升级包 MD5（小写十六进制）")
    package_sha256: str = Field(
        pattern=SHA256_PATTERN, description="发布时服务端会对 MinIO 对象重新流式计算比对"
    )
    signature: str = Field(
        min_length=1,
        max_length=1024,
        description="RSA-2048 签名（base64，RSASSA-PKCS1-v1_5 + SHA-256，发布方私钥离线生成）",
    )
    changelog: dict[str, object] = Field(default_factory=dict, description="变更说明（JSONB，默认 {}）")
    applicable_models: list[str] = Field(min_length=1, description="适用车型（默认 [HUNTER_SE]）")
    part_count: int = Field(
        default=1, ge=1, le=10000, description="期望分片数（> 1 时返回 parts[]，支持断点续传）"
    )


class OtaVersionUploadPart(BaseModel):
    """分片上传预签名地址（契约 OtaVersionUploadPart）。"""

    model_config = ConfigDict(extra="forbid")

    part_number: int = Field(ge=1, le=10000, description="分片序号（从 1 开始）")
    upload_url: str = Field(description="预签名 PUT 地址（有效期同 expires_in）")


class OtaVersionUploadInfo(BaseModel):
    """上传预签名信息（契约 OtaVersionUploadInfo；有效期 3600s = 1 小时）。"""

    model_config = ConfigDict(extra="forbid")

    object_key: str = Field(description="服务端生成的对象键（bucket hunter-ota-packages）")
    upload_url: str = Field(description="单分片预签名 PUT 地址（part_count=1 时直接使用）")
    expires_in: int = Field(default=PRESIGNED_UPLOAD_EXPIRES_IN, description="有效期秒数（固定 3600）")
    part_count: int = Field(ge=1, description="分片数")
    parts: list[OtaVersionUploadPart] | None = Field(
        default=None, description="part_count > 1 时返回的分片地址清单"
    )


class OtaVersionCreateData(BaseModel):
    """创建结果（契约 OtaVersionCreateData = 草稿版本 + 上传信息）。"""

    model_config = ConfigDict(extra="forbid")

    version: OtaVersionItem = Field(description="已创建的草稿版本")
    upload: OtaVersionUploadInfo = Field(description="上传预签名信息（1 小时）")


class OtaVersionCreateResponse(OtaApiResponse):
    """POST /api/v1/ota/versions 统一响应。"""

    data: OtaVersionCreateData | None = None


class OtaVersionPublishRequest(BaseModel):
    """发布请求（契约 OtaVersionPublishRequest；可省略请求体，仅提供审计备注）。"""

    model_config = ConfigDict(extra="forbid")

    note: str | None = Field(
        default=None, max_length=512, description="发布备注（写入审计日志，禁止含密钥/证书内容）"
    )


class OtaVersionPublishChecks(BaseModel):
    """发布五项校验结论（契约 OtaVersionPublishChecks；全部 true 才会置 published）。"""

    model_config = ConfigDict(extra="forbid")

    package_size_verified: bool = Field(description="MinIO 对象实际字节数与 package_size 一致")
    md5_verified: bool = Field(description="流式 MD5 与 package_md5 一致（不一致 → 6001）")
    sha256_verified: bool = Field(description="流式 SHA-256 与 package_sha256 一致（不一致 → 6001）")
    signature_verified: bool = Field(description="RSA-2048 验签通过（失败 → 6002）")
    version_code_monotonic: bool = Field(description="大于同车型已发布最大 version_code（失败 → 6003）")


class OtaVersionPublishData(BaseModel):
    """发布结果（契约 OtaVersionPublishData）。"""

    model_config = ConfigDict(extra="forbid")

    version_id: UUID = Field(description="版本 ID")
    version_code: int = Field(ge=1, description="版本编码")
    status: OtaVersionStatus = Field(description="发布后的版本状态（published）")
    release_time: float = Field(description="本次发布时间（Unix epoch 秒）")
    checks: OtaVersionPublishChecks = Field(description="五项校验结论（全部 true）")


class OtaVersionPublishResponse(OtaApiResponse):
    """POST /api/v1/ota/versions/{version_id}/publish 统一响应。"""

    data: OtaVersionPublishData | None = None


class OtaVersionDeprecateRequest(BaseModel):
    """废弃/停用请求（契约 OtaVersionDeprecateRequest；目标状态仅 deprecated / disabled）。"""

    model_config = ConfigDict(extra="forbid")

    status: Literal["deprecated", "disabled"] = Field(
        description="目标状态（仅允许 deprecated / disabled；published 之外的当前状态 → 3003）"
    )
    reason: str = Field(min_length=1, max_length=512, description="退役原因（审计留痕，必填）")


__all__ = [
    "OtaVersionCreateData",
    "OtaVersionCreateRequest",
    "OtaVersionCreateResponse",
    "OtaVersionDeprecateRequest",
    "OtaVersionDetail",
    "OtaVersionDetailResponse",
    "OtaVersionItem",
    "OtaVersionList",
    "OtaVersionListResponse",
    "OtaVersionPublishChecks",
    "OtaVersionPublishData",
    "OtaVersionPublishRequest",
    "OtaVersionPublishResponse",
    "OtaVersionUploadInfo",
    "OtaVersionUploadPart",
    "PRESIGNED_UPLOAD_EXPIRES_IN",
]
