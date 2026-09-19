"""公共 Schema：统一响应 / 错误码 / 分页边界 / 运维探针响应。

契约来源：`contracts/openapi/scene-service.yaml`（components.schemas 的 ApiResponse /
HealthResponse / ReadyResponse，components.parameters 的 PageQuery / PageSizeQuery）。
分页常量复用 hunter_common 单一事实来源（契约测试校验其一致性）。
"""
from __future__ import annotations

from hunter_common.database.repository import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from hunter_common.exceptions import ErrorCode
from hunter_common.responses import ApiResponse
from pydantic import BaseModel, Field

# ---------- 契约常量（与 OpenAPI components 声明保持一致） ----------
#: 语义化版本（契约 SceneMeta.version / ScenePublishRequest.version pattern）
VERSION_PATTERN = r"^\d+\.\d+\.\d+$"
#: SHA-256 十六进制（契约 SceneExportData.sha256 pattern）
SHA256_PATTERN = r"^[0-9a-f]{64}$"
#: 场景名称长度（契约 SceneMeta.scene_name minLength/maxLength）
SCENE_NAME_MIN_LENGTH = 1
SCENE_NAME_MAX_LENGTH = 128
#: 场景描述长度（契约 description maxLength，可空）
SCENE_DESCRIPTION_MAX_LENGTH = 1024
#: 标签约束（契约 tags：maxItems 20，items 长度 1..32）
TAG_MAX_ITEMS = 20
TAG_MIN_LENGTH = 1
TAG_MAX_LENGTH = 32
#: 参与者 / 事件数量上限（契约 SceneConfig.actors / events maxItems）
ACTORS_MAX_ITEMS = 100
EVENTS_MAX_ITEMS = 100


class HealthData(BaseModel):
    """存活探针数据（契约 HealthResponse.data：status 固定 ok）。"""

    status: str = Field(default="ok", pattern="^ok$", description="固定为 ok")


class ReadyData(BaseModel):
    """就绪探针数据（契约 ReadyResponse.data：各依赖探测结果）。"""

    database: bool = Field(description="PostgreSQL 连通性")
    redis: bool = Field(description="Redis 连通性")


class HealthResponse(ApiResponse[HealthData]):
    """GET /healthz 响应（code=0，data.status=ok）。"""


class ReadyResponse(ApiResponse[ReadyData]):
    """GET /readyz 响应（依赖不可用时 HTTP 503 + code=5001，data 为探测明细）。"""


__all__ = [
    "ACTORS_MAX_ITEMS",
    "DEFAULT_PAGE_SIZE",
    "EVENTS_MAX_ITEMS",
    "MAX_PAGE_SIZE",
    "SCENE_DESCRIPTION_MAX_LENGTH",
    "SCENE_NAME_MAX_LENGTH",
    "SCENE_NAME_MIN_LENGTH",
    "SHA256_PATTERN",
    "TAG_MAX_ITEMS",
    "TAG_MAX_LENGTH",
    "TAG_MIN_LENGTH",
    "VERSION_PATTERN",
    "ErrorCode",
    "HealthData",
    "HealthResponse",
    "ReadyData",
    "ReadyResponse",
]
