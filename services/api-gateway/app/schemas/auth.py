"""认证端点请求/响应模型（契约 components.schemas 一一对应）。

单一事实来源：``contracts/openapi/api-gateway.yaml``（字段名/类型/required/枚举不可更改）。
统一响应五字段封装复用 ``hunter_common.responses.ApiResponse``。
"""
from __future__ import annotations

from typing import Literal
from uuid import UUID

from hunter_common.responses import ApiResponse
from pydantic import BaseModel, Field, field_validator

__all__ = [
    "ApiResponseEmpty",
    "ApiResponseTokenPair",
    "ApiResponseUserProfile",
    "ChangePasswordRequest",
    "LoginRequest",
    "LogoutRequest",
    "RefreshTokenRequest",
    "TokenPair",
    "UserProfile",
]


# =====================================================================
# 请求模型（契约 LoginRequest / RefreshTokenRequest / LogoutRequest）
# =====================================================================
class LoginRequest(BaseModel):
    """POST /api/v1/user/login 请求体（契约 LoginRequest；security: []）。"""

    username: str = Field(
        min_length=1,
        max_length=64,
        description="用户名（user_svc.users.username）",
    )
    password: str = Field(
        min_length=8,
        max_length=128,
        description="明文密码（TLS 传输；服务端 bcrypt 校验；禁止写入日志）",
    )
    totp_code: str | None = Field(
        default=None,
        pattern=r"^[0-9]{6}$",
        description="MFA(TOTP) 动态口令（6 位数字）；账号未启用 MFA 时忽略",
    )


class RefreshTokenRequest(BaseModel):
    """POST /api/v1/user/refresh 请求体（契约 RefreshTokenRequest；security: []）。"""

    refresh_token: str = Field(description="登录返回的 Refresh Token（有效期 7 天，单次使用轮换）")


class LogoutRequest(BaseModel):
    """POST /api/v1/user/logout 请求体（契约 LogoutRequest；requestBody required=false）。"""

    refresh_token: str | None = Field(
        default=None,
        description="可选；提供时一并作废（其有效性随会话撤销，见 auth_service.refresh）",
    )


class ChangePasswordRequest(BaseModel):
    """POST /api/v1/user/change-password 请求体（契约 ChangePasswordRequest；G-06）。"""

    old_password: str = Field(
        min_length=8,
        max_length=128,
        description="当前口令（bcrypt 校验；失败统一 1001，禁止写入日志）",
    )
    new_password: str = Field(
        min_length=8,
        max_length=128,
        description="新口令（≥8 位且含大写、小写、数字；设计文档 14.1 节口令强度策略）",
    )

    @field_validator("new_password")
    @classmethod
    def _require_complexity(cls, value: str) -> str:
        """契约 pattern `^(?=.*[a-z])(?=.*[A-Z])(?=.*\\d).+$` 的等价实现。

        pydantic 内置 regex 引擎不支持 lookahead，故用校验器实现同一强度策略；
        失败→422 + 2001（与其他字段校验同一映射）。
        """
        if not (any(c.islower() for c in value) and any(c.isupper() for c in value)
                and any(c.isdigit() for c in value)):
            raise ValueError("新口令需包含大写字母、小写字母与数字")
        return value


# =====================================================================
# 数据模型（契约 UserProfile / TokenPair）
# =====================================================================
class UserProfile(BaseModel):
    """用户信息（契约 UserProfile；前端渲染菜单与 v-permission 指令依据）。"""

    user_id: UUID = Field(description="user_svc.users.user_id（JWT `sub`）")
    username: str = Field(description="user_svc.users.username")
    real_name: str | None = Field(default=None, description="姓名（users.real_name，可空）")
    roles: list[str] = Field(description="角色编码列表（user_svc.roles.role_code，如 admin/operator）")
    permissions: list[str] = Field(
        default_factory=list,
        description="权限编码列表（user_svc.permissions.permission_code，形如 `scene:read`）",
    )
    must_change_password: bool = Field(
        default=False,
        description="G-06 首登强制改密标志；true 时前端应引导调用 /user/change-password",
    )


class TokenPair(BaseModel):
    """Token 对（契约 TokenPair；Access 30min + Refresh 7d 单次轮换）。"""

    access_token: str = Field(description="JWT Access Token（30 分钟，G-04①）")
    refresh_token: str = Field(description="Refresh Token（7 天，单次使用轮换）")
    token_type: Literal["Bearer"] = "Bearer"
    expires_in: int = Field(description="Access Token 有效期（秒）")
    refresh_expires_in: int = Field(description="Refresh Token 有效期（秒）")
    user: UserProfile = Field(description="登录用户信息")


# =====================================================================
# 统一响应封装（契约 ApiResponseTokenPair / ApiResponseUserProfile / ApiResponseEmpty）
# =====================================================================
class ApiResponseTokenPair(ApiResponse[TokenPair]):
    """登录 / 刷新成功响应（data=TokenPair）。"""


class ApiResponseUserProfile(ApiResponse[UserProfile]):
    """当前用户信息响应（data=UserProfile）。"""


class ApiResponseEmpty(ApiResponse[None]):
    """注销成功响应（data 恒为 null，契约 ApiResponseEmpty）。"""