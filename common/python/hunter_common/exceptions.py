"""HunterEdge 统一异常体系。

错误码为全局预定义值（见 System Prompt 错误码表），禁止新增或更改含义。
所有业务异常必须继承 HunterBaseException，由各服务全局异常处理器转换为统一响应格式。
"""
from __future__ import annotations

from enum import IntEnum


class ErrorCode(IntEnum):
    """预定义错误码（不可新增或更改含义）。"""

    SUCCESS = 0
    UNAUTHENTICATED = 1001              # 未认证：Token 缺失或无效
    PERMISSION_DENIED = 1002            # 无权限：角色权限不足
    TOKEN_EXPIRED = 1003                # Token 过期：需刷新 Token
    INVALID_PARAM = 2001                # 参数错误：请求参数校验失败
    MISSING_PARAM = 2002                # 参数缺失：必填参数未提供
    RESOURCE_NOT_FOUND = 3001           # 资源不存在：请求的资源 ID 不存在
    RESOURCE_ALREADY_EXISTS = 3002      # 资源已存在：创建时唯一键冲突
    RESOURCE_STATE_CONFLICT = 3003      # 资源状态冲突：当前状态不允许该操作
    VEHICLE_OFFLINE = 4001              # 车辆不在线：目标车辆当前离线
    VEHICLE_BUSY = 4002                 # 车辆忙：正在执行其他任务（如 OTA 中）
    INTERNAL_ERROR = 5000               # 服务器内部错误：未预期的异常
    SERVICE_UNAVAILABLE = 5001          # 服务不可用：依赖服务不可用
    OTA_PACKAGE_CHECKSUM_FAILED = 6001  # OTA 包校验失败：MD5/SHA256 不匹配
    OTA_SIGNATURE_FAILED = 6002         # OTA 签名验证失败：数字签名无效
    OTA_PRECONDITION_FAILED = 6003      # OTA 前置条件不满足：电量/停车/存储等
    RC_SESSION_CONFLICT = 7001          # 远程操控会话冲突：车辆已被其他操作员操控
    RC_VIDEO_SETUP_FAILED = 7002        # 远程操控视频建立失败：WebRTC 连接失败


class HunterBaseException(Exception):
    """HunterEdge 业务异常基类。

    Attributes:
        code: 预定义错误码（默认 5000 服务器内部错误）。
        message: 人类可读错误信息。
        details: 附加上下文（仅用于日志与调试，不得包含敏感信息）。
    """

    code: int = ErrorCode.INTERNAL_ERROR
    message: str = "服务器内部错误"

    def __init__(
        self,
        message: str | None = None,
        *,
        code: int | None = None,
        details: dict[str, object] | None = None,
    ) -> None:
        if message is not None:
            self.message = message
        if code is not None:
            self.code = code
        self.details: dict[str, object] = details or {}
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"[{self.code}] {self.message}"


# ---------- 1xxx 认证授权 ----------
class AuthenticationError(HunterBaseException):
    """1001 未认证：Token 缺失或无效。"""

    code = ErrorCode.UNAUTHENTICATED
    message = "未认证"


class PermissionDeniedError(HunterBaseException):
    """1002 无权限：角色权限不足。"""

    code = ErrorCode.PERMISSION_DENIED
    message = "无权限"


class TokenExpiredError(HunterBaseException):
    """1003 Token 过期：需刷新 Token。"""

    code = ErrorCode.TOKEN_EXPIRED
    message = "Token 已过期"


# ---------- 2xxx 参数 ----------
class InvalidParameterError(HunterBaseException):
    """2001 参数错误：请求参数校验失败。"""

    code = ErrorCode.INVALID_PARAM
    message = "参数错误"


class MissingParameterError(HunterBaseException):
    """2002 参数缺失：必填参数未提供。"""

    code = ErrorCode.MISSING_PARAM
    message = "参数缺失"


# ---------- 3xxx 资源 ----------
class ResourceNotFoundError(HunterBaseException):
    """3001 资源不存在。"""

    code = ErrorCode.RESOURCE_NOT_FOUND
    message = "资源不存在"


class ResourceAlreadyExistsError(HunterBaseException):
    """3002 资源已存在：创建时唯一键冲突。"""

    code = ErrorCode.RESOURCE_ALREADY_EXISTS
    message = "资源已存在"


class ResourceStateConflictError(HunterBaseException):
    """3003 资源状态冲突：当前状态不允许该操作。"""

    code = ErrorCode.RESOURCE_STATE_CONFLICT
    message = "资源状态冲突"


# ---------- 4xxx 车辆 ----------
class VehicleOfflineError(HunterBaseException):
    """4001 车辆不在线。"""

    code = ErrorCode.VEHICLE_OFFLINE
    message = "车辆不在线"


class VehicleBusyError(HunterBaseException):
    """4002 车辆忙：正在执行其他任务（如 OTA 中）。"""

    code = ErrorCode.VEHICLE_BUSY
    message = "车辆忙"


# ---------- 5xxx 服务 ----------
class InternalServerError(HunterBaseException):
    """5000 服务器内部错误：未预期的异常。"""

    code = ErrorCode.INTERNAL_ERROR
    message = "服务器内部错误"


class ServiceUnavailableError(HunterBaseException):
    """5001 服务不可用：依赖服务不可用。"""

    code = ErrorCode.SERVICE_UNAVAILABLE
    message = "服务不可用"


# ---------- 6xxx OTA ----------
class OtaPackageChecksumError(HunterBaseException):
    """6001 OTA 包校验失败：MD5/SHA256 不匹配。"""

    code = ErrorCode.OTA_PACKAGE_CHECKSUM_FAILED
    message = "OTA 包校验失败"


class OtaSignatureError(HunterBaseException):
    """6002 OTA 签名验证失败：数字签名无效。"""

    code = ErrorCode.OTA_SIGNATURE_FAILED
    message = "OTA 签名验证失败"


class OtaPreconditionError(HunterBaseException):
    """6003 OTA 前置条件不满足：电量/停车/存储等。"""

    code = ErrorCode.OTA_PRECONDITION_FAILED
    message = "OTA 前置条件不满足"


# ---------- 7xxx 远程操控 ----------
class RemoteControlSessionConflictError(HunterBaseException):
    """7001 远程操控会话冲突：车辆已被其他操作员操控。"""

    code = ErrorCode.RC_SESSION_CONFLICT
    message = "远程操控会话冲突"


class RemoteControlVideoError(HunterBaseException):
    """7002 远程操控视频建立失败：WebRTC 连接失败。"""

    code = ErrorCode.RC_VIDEO_SETUP_FAILED
    message = "远程操控视频建立失败"


__all__ = [
    "AuthenticationError",
    "ErrorCode",
    "HunterBaseException",
    "InternalServerError",
    "InvalidParameterError",
    "MissingParameterError",
    "OtaPackageChecksumError",
    "OtaPreconditionError",
    "OtaSignatureError",
    "PermissionDeniedError",
    "RemoteControlSessionConflictError",
    "RemoteControlVideoError",
    "ResourceAlreadyExistsError",
    "ResourceNotFoundError",
    "ResourceStateConflictError",
    "ServiceUnavailableError",
    "TokenExpiredError",
    "VehicleBusyError",
    "VehicleOfflineError",
]
