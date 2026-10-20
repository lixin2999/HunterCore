"""文件上传模型（契约 components.schemas：UploadBucket → FileCompleteResponse，5.5 节）。"""
from __future__ import annotations

import time
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
    timestamp: int = Field(default_factory=lambda: int(time.time()))


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
    timestamp: int = Field(default_factory=lambda: int(time.time()))


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
    # ↓ 仅 hunter-video 桶（操控录像 sidecar↔视频配对）填充；其余桶为 null/false（V1.21.22）
    video_size_bytes: int | None = Field(
        default=None,
        description="所属会话的 .mp4 实际字节数（.json 行经配对探测；.mp4 行为自身大小；无对应视频为 null）",
    )
    is_orphan: bool = Field(
        default=False,
        description="是否为可安全删除的孤儿（对应视频缺失或 0 B）；true=无效记录，false=有效录像受保护",
    )


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
    timestamp: int = Field(default_factory=lambda: int(time.time()))


class FileDeleteRequest(BaseModel):
    """批量删除请求（POST /files/delete，V1.21.20：清理无效/过期对象）。"""

    model_config = ConfigDict(extra="forbid")

    bucket: UploadBucket
    object_keys: list[str] = Field(
        min_length=1,
        max_length=200,
        description="待删除对象键列表（相对 bucket，≤200/次；与 GET /files 返回的 object_key 一致）",
    )
    include_valid: bool = Field(
        default=False,
        description=(
            "hunter-video 桶专用护栏（V1.21.22）：默认 false——拒删对应视频有效（>0 B）的会话"
            "（避免误删有效录像），此类项计入 skipped；true 时强制连有效会话一并删除。"
            "删除按会话配对（.json+.mp4 同删），不留失联对象。非 video 桶忽略本字段。"
        ),
    )


class FileDeleteFailure(BaseModel):
    """单对象删除失败项（DeleteObjects 部分失败时回带）。"""

    model_config = ConfigDict(extra="forbid")

    object_key: str
    code: str = Field(default="", description="S3 错误码")
    message: str = Field(default="", description="错误描述")


class FileDeleteData(BaseModel):
    """批量删除结果（幂等：不存在对象亦计入 deleted）。"""

    model_config = ConfigDict(extra="forbid")

    bucket: UploadBucket
    deleted: list[str] = Field(default_factory=list, description="已成功删除的对象键")
    failed: list[FileDeleteFailure] = Field(default_factory=list, description="删除失败项")
    deleted_count: int = Field(ge=0, description="删除成功数")
    # ↓ hunter-video 护栏（V1.21.22）：被保护而未删的请求键
    skipped: list[str] = Field(
        default_factory=list,
        description="因对应视频有效（>0 B）且未指定 include_valid 而被跳过未删的请求键",
    )
    skipped_count: int = Field(default=0, ge=0, description="被护栏跳过数")


class FileDeleteResponse(BaseModel):
    """POST /files/delete 统一响应。"""

    code: int = 0
    message: str = "success"
    data: FileDeleteData | None = None
    request_id: str
    timestamp: int = Field(default_factory=lambda: int(time.time()))
