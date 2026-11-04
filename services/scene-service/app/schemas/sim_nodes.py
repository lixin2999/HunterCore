"""仿真节点控制面 Schema（严格对齐 contracts/openapi/scene-service.yaml）。

契约段落：components.schemas 的 SimNode*，设计说明见
`x-hunter-simulation-flow.node_dispatch`（V1.24.0：注册/心跳 + 按自学习地址直推）。

V1.24.1 仿真端管理扩展：
- ``SimNodeDetailData``：在 SimNodeData 基础上叠加登记表字段（展示名/令牌/证书）；
- ``SimNodeRegistryPatch``：管理员 PATCH 请求体（展示名/描述/标签）；
- ``IssueTokenResult``：签发服务令牌一次性响应；
- ``IssueCertResult``：签发证书响应。

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


class SimNodeDetailData(SimNodeData):
    """节点详情视图（在 SimNodeData 上叠加登记表字段）。

    字段来源于：
    - Redis 实时登记表：``endpoint_*``/``online``/``last_seen_at``/``running_count`` 等；
    - PostgreSQL ``scene_svc.sim_node_registry``：``display_name``/``description``/``token_*``/``kafka_cert_*``/``plat_cert_*``；
    - 懒建语义：管理员首次访问时自动创建登记行，因此即使节点未注册也可预览详情。

    同时用于清单 ``items``：``online_only=False`` 时后端从登记表补充管理字段（展示名/令牌/证书序列号），
    未登记或无登记表时为 ``None``（响应格式向后兼容，仅新增可空字段）。
    """

    display_name: str | None = Field(default=None, description="管理员设定的展示名称")
    registry_description: str | None = Field(default=None, description="管理员设定的备注")
    owner_client_id: str | None = Field(
        default=None, description="注册时绑定的调用方 client_id（与 owner 字段含义相同，来源登记）"
    )
    token_id: str | None = Field(default=None, description="已签发服务令牌的 jti；未签发或已撤销 = None")
    token_issued_at: datetime | None = None
    kafka_cert_sn: str | None = Field(default=None, description="Kafka 客户端证书序列号（大写 HEX）")
    kafka_cert_issued_at: datetime | None = None
    plat_cert_sn: str | None = Field(default=None, description="平台访问证书序列号（大写 HEX）")
    plat_cert_issued_at: datetime | None = None
    registry_first_seen_at: datetime | None = Field(
        default=None, description="登记行创建时间（≈ 管理员首次查看时间，与 registered_at 可相同）"
    )
    registry_labels: dict[str, str] | None = Field(
        default=None, description="管理员在登记表中设定的标签（与 labels 分开）"
    )


class SimNodeListData(BaseModel):
    """节点清单（契约 SimNodeListData；items 复用 SimNodeDetailData 以附带登记字段）。"""

    items: list[SimNodeDetailData] = Field(default_factory=list, max_length=NODE_LIST_MAX_ITEMS)
    total: int = 0


# =====================================================================
# 三、V1.24.1 仿真端管理：详情 / 登记 / 令牌 / 证书
# =====================================================================

#: 管理员 PATCH 字段上限
REGISTRY_DISPLAY_NAME_MAX_LENGTH = 128
REGISTRY_DESCRIPTION_MAX_LENGTH = 500


class SimNodeRegistryPatch(BaseModel):
    """管理员 PATCH 请求体（仅更新登记基础信息，不涉及令牌/证书）。"""

    model_config = ConfigDict(extra="forbid")

    display_name: str | None = Field(
        default=None, max_length=REGISTRY_DISPLAY_NAME_MAX_LENGTH,
        description="展示名称；传空串 = 清除名称",
    )
    description: str | None = Field(
        default=None, max_length=REGISTRY_DESCRIPTION_MAX_LENGTH,
        description="备注描述；传空串 = 清除描述",
    )
    labels: dict[str, str] | None = Field(
        default=None, description="管理员自定义标签（整体覆盖）"
    )


class IssueTokenResult(BaseModel):
    """签发服务令牌一次性响应（``access_token`` 不落日志，仅响应体出现）。"""

    node_id: str
    access_token: str = Field(
        ..., write_only=True,
        description="一次性服务令牌（JWT）；平台不落盘不写日志，丢失后需重新签发",
    )
    token_id: str = Field(..., description="服务令牌 jti（撤销时需要）")
    expires_at: datetime | None = Field(
        default=None, description="令牌到期时间；永久令牌为 None"
    )


class IssueCertResult(BaseModel):
    """签发/重签证书响应。"""

    node_id: str
    cert_type: Literal["kafka", "platform"]
    serial_hex: str = Field(..., description="新证书序列号（大写十六进制）")
    issued_at: datetime


# =====================================================================
# 四、统一响应包装
# =====================================================================


class SimNodeResponse(ApiResponse[SimNodeData]):
    """节点注册/心跳响应（回显租约与当前学到的地址）。"""


class SimNodeListResponse(ApiResponse[SimNodeListData]):
    """节点清单响应（GET /api/v1/scene/sim-nodes）。"""


class SimNodeDetailResponse(ApiResponse[SimNodeDetailData]):
    """节点详情响应（GET /api/v1/scene/sim-nodes/{node_id}）。"""


class SimNodeRegistryPatchResponse(ApiResponse[SimNodeDetailData]):
    """PATCH 登记响应（更新后返回最新详情）。"""


class IssueTokenResponse(ApiResponse[IssueTokenResult]):
    """签发服务令牌响应。"""


class IssueCertResponse(ApiResponse[IssueCertResult]):
    """签发证书响应。"""


class SimNodeEmptyResponse(ApiResponse[None]):
    """无数据响应（撤销令牌/删除节点等）。"""


__all__ = [
    "CAPACITY_MAX",
    "CAPACITY_MIN",
    "ENDPOINT_SOURCES",
    "IssueCertResponse",
    "IssueCertResult",
    "IssueTokenResponse",
    "IssueTokenResult",
    "MANAGER_PORT_MAX",
    "MANAGER_PORT_MIN",
    "NODE_ID_PATTERN",
    "NODE_LIST_MAX_ITEMS",
    "NODE_MAPS_MAX_ITEMS",
    "NODE_PROTOCOLS",
    "NODE_TEXT_MAX_LENGTH",
    "NODE_VERSION_MAX_LENGTH",
    "REGISTRY_DESCRIPTION_MAX_LENGTH",
    "REGISTRY_DISPLAY_NAME_MAX_LENGTH",
    "RUNNING_INSTANCES_MAX_ITEMS",
    "SimNodeData",
    "SimNodeDetailData",
    "SimNodeDetailResponse",
    "SimNodeEmptyResponse",
    "SimNodeHeartbeatRequest",
    "SimNodeListData",
    "SimNodeListResponse",
    "SimNodeRegisterRequest",
    "SimNodeRegistryPatch",
    "SimNodeRegistryPatchResponse",
    "SimNodeResponse",
]
