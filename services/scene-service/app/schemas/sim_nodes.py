"""仿真节点控制面 Schema（严格对齐 contracts/openapi/scene-service.yaml）。

契约段落：components.schemas 的 SimNode*，设计说明见
`x-hunter-simulation-flow.node_dispatch`（V1.24.0：注册/心跳 + 按自学习地址直推）。

地址字段的来源：``endpoint_ip`` / ``endpoint_source`` / ``learned_at`` **只能由服务端写入**
（从入站请求头自学习），因此只出现在响应模型里；请求体只声明 ``manager_port`` / ``protocol``。
"""
from __future__ import annotations

from datetime import datetime
from typing import Literal

from hunter_common.responses import ApiResponse
from pydantic import BaseModel, ConfigDict, Field

# ---------- 契约常量（与 OpenAPI components 声明保持一致） ----------
#: 节点标识（契约 SimNodeIdPath pattern：与 IP 无关，节点自行命名且保持稳定）
NODE_ID_PATTERN = r"^[a-z0-9][a-z0-9._-]{2,63}$"
#: 容量上下限（契约 capacity minimum/maximum）
CAPACITY_MIN = 1
CAPACITY_MAX = 32
#: 节点管理 API 端口上下限（契约 manager_port minimum/maximum）
MANAGER_PORT_MIN = 1
MANAGER_PORT_MAX = 65535
#: 能力清单/心跳上报的数量上限（契约 maxItems）
NODE_MAPS_MAX_ITEMS = 64
RUNNING_INSTANCES_MAX_ITEMS = 32
#: 节点清单上限（契约 SimNodeListData.items maxItems）
NODE_LIST_MAX_ITEMS = 100
#: 文本字段上限
NODE_TEXT_MAX_LENGTH = 512
NODE_HINT_MAX_LENGTH = 256
NODE_VERSION_MAX_LENGTH = 64

#: 基址协议（契约 endpoint_learning.base_url）
NODE_PROTOCOLS = ("http", "https")
#: 地址来源标记（契约 SimNodeData.endpoint_source；排查是否被代理/网关改写）
ENDPOINT_SOURCES = ("real-ip", "forwarded-for", "socket-peer")


# =====================================================================
# 一、请求体
# =====================================================================


class SimNodeRegisterRequest(BaseModel):
    """节点能力声明（契约 SimNodeRegisterRequest，幂等 upsert）。

    ⚠ 地址不在请求体里：``endpoint_ip`` 由平台从入站请求头自学习（契约
    node_dispatch.endpoint_learning）；本模型只声明监听端口与协议。
    """

    model_config = ConfigDict(extra="forbid")

    carla_version: str | None = Field(
        default=None, max_length=NODE_VERSION_MAX_LENGTH, description="节点上 CARLA 服务版本"
    )
    maps: list[str] | None = Field(
        default=None,
        max_length=NODE_MAPS_MAX_ITEMS,
        description="可用地图清单（供选节点策略与前端提示）",
    )
    capacity: int = Field(
        default=CAPACITY_MIN,
        ge=CAPACITY_MIN,
        le=CAPACITY_MAX,
        description="可同时运行的仿真实例上限",
    )
    labels: dict[str, str] | None = Field(
        default=None, description="自定义标签（机房/显卡型号等）"
    )
    manager_port: int | None = Field(
        default=None,
        ge=MANAGER_PORT_MIN,
        le=MANAGER_PORT_MAX,
        description="节点 CARLA 管理 API 端口（缺省取 SIM_NODE_MANAGER_DEFAULT_PORT）",
    )
    protocol: Literal["http", "https"] | None = Field(
        default=None, description="拼接基址的协议（缺省取 SIM_NODE_MANAGER_SCHEME）"
    )
    runtime_hint: str | None = Field(
        default=None, max_length=NODE_HINT_MAX_LENGTH, description="节点自述服务地址（仅展示）"
    )


class SimNodeHeartbeatRequest(BaseModel):
    """轻量续租（契约 SimNodeHeartbeatRequest，可空体）。"""

    model_config = ConfigDict(extra="forbid")

    running_instance_ids: list[str] | None = Field(
        default=None,
        max_length=RUNNING_INSTANCES_MAX_ITEMS,
        description="节点正在运行的实例 ID",
    )
    capacity: int | None = Field(
        default=None, ge=CAPACITY_MIN, le=CAPACITY_MAX, description="容量变更时携带"
    )
    message: str | None = Field(
        default=None, max_length=NODE_TEXT_MAX_LENGTH, description="节点侧健康提示"
    )


# =====================================================================
# 二、响应数据
# =====================================================================


class SimNodeData(BaseModel):
    """节点视图（契约 SimNodeData）。

    ``endpoint_*`` 四条是排障与下发的唯一事实来源：它们只反映**最后一次注册/心跳时学到的地址**，
    IP 漂移后会在下一次心跳自动刷新（因此不需要人工处置）。
    """

    node_id: str
    owner: str = Field(description="首次注册绑定的调用方身份（服务令牌为 service:{client_id}）")
    online: bool = Field(description="心跳未超租约")
    last_seen_at: datetime | None = None
    lease_seconds: int
    carla_version: str | None = None
    maps: list[str] | None = None
    capacity: int = CAPACITY_MIN
    running_count: int = 0
    labels: dict[str, str] | None = None
    endpoint_ip: str | None = Field(default=None, description="当前学习到的节点来源地址")
    manager_port: int | None = Field(default=None, description="节点 CARLA 管理 API 端口")
    protocol: Literal["http", "https"] | None = Field(default=None, description="拼接基址所用协议")
    endpoint_source: Literal["real-ip", "forwarded-for", "socket-peer"] | None = Field(
        default=None, description="地址来源（排障：判断是否被代理/网关改写）"
    )
    learned_at: datetime | None = Field(default=None, description="最后一次地址刷新时间")
    runtime_hint: str | None = Field(default=None, description="仅展示，不参与寻址")
    registered_at: datetime | None = None
    message: str | None = None


class SimNodeListData(BaseModel):
    """节点清单（契约 SimNodeListData）。"""

    items: list[SimNodeData] = Field(default_factory=list, max_length=NODE_LIST_MAX_ITEMS)
    total: int = 0


# =====================================================================
# 三、统一响应包装
# =====================================================================


class SimNodeResponse(ApiResponse[SimNodeData]):
    """节点注册/心跳响应（回显租约与当前学到的地址）。"""


class SimNodeListResponse(ApiResponse[SimNodeListData]):
    """节点清单响应（GET /api/v1/scene/sim-nodes）。"""


__all__ = [
    "CAPACITY_MAX",
    "CAPACITY_MIN",
    "ENDPOINT_SOURCES",
    "MANAGER_PORT_MAX",
    "MANAGER_PORT_MIN",
    "NODE_ID_PATTERN",
    "NODE_LIST_MAX_ITEMS",
    "NODE_MAPS_MAX_ITEMS",
    "NODE_PROTOCOLS",
    "NODE_TEXT_MAX_LENGTH",
    "NODE_VERSION_MAX_LENGTH",
    "RUNNING_INSTANCES_MAX_ITEMS",
    "SimNodeData",
    "SimNodeHeartbeatRequest",
    "SimNodeListData",
    "SimNodeListResponse",
    "SimNodeRegisterRequest",
    "SimNodeResponse",
]
