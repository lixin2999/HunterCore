"""文件上传模型（契约 components.schemas：UploadBucket → FileCompleteResponse，5.5 节）。"""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from app.schemas.common import FileDataType, UploadBucket, UploadMethod

#: 生命周期归类（G-12，契约 FilePresignRequest/FileCompleteRequest.retention）
FileRetention = Literal["regular", "event"]

#: 打标义务说明（与 contracts/database/object-storage.yaml lifecycle.tagging 同源）
_RETENTION_DESC = (
    "生命周期归类（G-12，仅 hunter-rosbag 消费）：事件包必须显式 event（永久），"
    "缺省 regular（30 天）；服务端 complete 阶段据此打 Tag hunter-retention=<retention>"
)


class FilePresignRequest(BaseModel):
    """预签名请求（5.5 节：请求上传；对象路径由服务端生成，不可由客户端指定）。"""

    model_config = ConfigDict(extra="forbid")

    vehicle_id: str = Field(
        pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$",
        description="车辆标识（车端必须等于证书 CommonName，否则返回 1002）",
    )
    bucket: UploadBucket
    data_type: FileDataType
    file_name: str = Field(min_length=1, max_length=128, description="原始文件名（仅取扩展名）")
    file_size: int = Field(ge=1, description="文件字节数（用于分片判定与配额校验）")
    part_count: int = Field(
        default=1, ge=1, le=10000, description="分片数；>1 时返回分片上传地址"
    )
    content_type: str | None = Field(
        default=None, max_length=128, description="MIME 类型；空则按扩展名推断"
    )
    sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
        description="可选：文件 SHA-256（上传前声明，供 complete 阶段校验）",
    )
    retention: FileRetention = Field(default="regular", description=_RETENTION_DESC)


class FilePresignPart(BaseModel):
    """分片上传地址。"""

    model_config = ConfigDict(extra="forbid")

    part_number: int = Field(ge=1, le=10000, description="分片序号（从 1 开始）")
    upload_url: str = Field(description="该分片的预签名上传地址（截止时间与 upload_url 一致）")


class FilePresignData(BaseModel):
    """预签名结果（method=put → upload_url；method=multipart → upload_id + parts[]）。"""

    model_config = ConfigDict(extra="forbid")

    bucket: UploadBucket
    object_key: str = Field(
        description="对象路径（服务端生成）：{bucket}/{vehicle_id}/{date}/{data_type}/{timestamp}_{seq}.{ext}"
    )
    method: UploadMethod
    upload_url: str | None = Field(
        default=None, description="单次直传地址（method=put 时非空；有效期 1 小时）"
    )
    upload_id: str | None = Field(
        default=None, description="分片上传 ID（method=multipart 时非空，complete 阶段必须回带）"
    )
    parts: list[FilePresignPart] | None = Field(
        default=None, description="分片上传地址列表（method=multipart 时非空）"
    )
    expires_in: int = Field(description="上传地址有效期（秒），固定 3600（MinIO 契约）")


class FilePresignResponse(BaseModel):
    """POST /files/presign 统一响应。"""

    code: int = 0
    message: str = "success"
    data: FilePresignData | None = None
    request_id: str
    timestamp: int


class CompletedPart(BaseModel):
    """已完成分片（complete 阶段回带，用于合并对象）。"""

    model_config = ConfigDict(extra="forbid")

    part_number: int = Field(ge=1, le=10000, description="分片序号")
    etag: str = Field(description="分片 ETag（MinIO 返回，用于合并分片）")


class FileCompleteRequest(BaseModel):
    """上传完成通知（校验 size/md5/sha256 与 MinIO 对象一致，不一致返回 6001）。"""

    model_config = ConfigDict(extra="forbid")

    vehicle_id: str = Field(
        pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$",
        description="车辆标识（必须等于证书 CommonName；对象路径前缀不匹配返回 1002）",
    )
    bucket: UploadBucket
    object_key: str = Field(description="presign 阶段返回的对象路径（必须匹配本车辆目录前缀）")
    size_bytes: int = Field(ge=0, description="实际上传字节数")
    md5: str = Field(pattern=r"^[0-9a-f]{32}$", description="文件 MD5（小写十六进制）")
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$", description="文件 SHA-256（小写十六进制）")
    data_type: FileDataType | None = Field(default=None, description="数据类型（缺省从路径解析）")
    upload_id: str | None = Field(
        default=None, description="分片上传 ID（method=multipart 时必传）"
    )
    parts: list[CompletedPart] | None = Field(
        default=None, description="分片 ETag 列表（method=multipart 时必传，用于合并对象）"
    )
    timestamp: float = Field(description="上传完成时间（Unix epoch 秒，车端时间）")
    retention: FileRetention = Field(default="regular", description=_RETENTION_DESC)


class FileCompleteData(BaseModel):
    """上传完成结果（verified 恒为 True；失败时返回 6001）。"""

    model_config = ConfigDict(extra="forbid")

    bucket: UploadBucket
    object_key: str = Field(description="已校验的对象路径")
    size_bytes: int = Field(description="对象实际字节数")
    md5: str = Field(pattern=r"^[0-9a-f]{32}$")
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    data_type: FileDataType
    verified: bool = Field(description="完整性校验是否通过（失败时返回 6001，不会出现 false）")
    notify_topic: str = Field(description="通知 Topic（Kafka 契约：sensor_file，key = vehicle_id）")
    timestamp: float = Field(description="上传完成时间（Unix epoch 秒，回带请求值）")


class FileCompleteResponse(BaseModel):
    """POST /files/complete 统一响应。"""

    code: int = 0
    message: str = "success"
    data: FileCompleteData | None = None
    request_id: str
    timestamp: int


class FileObjectItem(BaseModel):
    """文件对象清单项（MinIO 列举 + 即时预签名下载地址）。"""

    model_config = ConfigDict(extra="forbid")

    bucket: UploadBucket
    object_key: str
    size_bytes: int = Field(ge=0, description="对象字节数")
    etag: str | None = Field(default=None, description="对象 ETag（MinIO 返回）")
    last_modified: float = Field(description="对象最后修改时间（Unix epoch 秒）")
    download_url: str = Field(description="预签名下载地址（有效期 900s，支持 Range 分片下载）")
    expires_in: int = Field(description="下载地址有效期（秒），固定 900", examples=[900])


class FileListData(BaseModel):
    """文件清单数据体（prefix 列举 + marker 游标分页）。"""

    model_config = ConfigDict(extra="forbid")

    items: list[FileObjectItem] = Field(default_factory=list, description="对象清单")
    next_marker: str | None = Field(default=None, description="下一页游标（无更多数据时为 null）")
    truncated: bool = Field(description="是否被截断（存在更多对象）")


class FileListResponse(BaseModel):
    """GET /files 统一响应。"""

    code: int = 0
    message: str = "success"
    data: FileListData | None = None
    request_id: str
    timestamp: int
