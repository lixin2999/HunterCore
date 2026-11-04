"""ORM 模型：scene_svc.sim_node_registry（仿真端管理登记表，对应 08_sim_nodes.sql）。

V1.24.1 仿真端管理功能：存管理侧持久化数据（令牌/证书序列号、管理员标签等），
与 Redis sim:node:{node_id} 实时登记表互补。

边界：
- 本表归属 scene-service **服务私有**（schema = scene_svc，与 scenes 共用），
  不入 hunter_common.database.models.ALL_MODELS；
- node_id 格式校验由应用层（路由层 Pydantic pattern）负责，DDL 无 CHECK；
- 懒建语义：节点首次注册只写 Redis，管理员首次查看/操作时在本表创建行。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from hunter_common.database.base import Base, CreateTimeMixin, UpdateTimeMixin
from hunter_common.database.schema_names import SCENE_SCHEMA
from sqlalchemy import DateTime, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column


class SimNodeRegistry(Base, CreateTimeMixin, UpdateTimeMixin):
    """scene_svc.sim_node_registry（仿真端管理登记行）。"""

    __tablename__ = "sim_node_registry"
    __table_args__ = {"schema": SCENE_SCHEMA}

    # 主键：节点标识，与 Redis sim:node:{node_id} 同值
    node_id: Mapped[str] = mapped_column(
        Text,
        primary_key=True,
        nullable=False,
        comment="节点标识（^[a-z0-9][a-z0-9._-]{2,63}$，校验由应用层负责）",
    )

    # 管理员维护的基础信息
    display_name: Mapped[str | None] = mapped_column(Text, nullable=True, comment="展示名称")
    description: Mapped[str | None] = mapped_column(Text, nullable=True, comment="备注描述")

    # 首次注册绑定的调用方身份（service:{client_id}，来自 Redis owner 快照）
    owner_client_id: Mapped[str | None] = mapped_column(Text, nullable=True)

    # 服务令牌（api-gateway 签发，jti 落此便于撤销校验）
    token_id: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment="已签发服务令牌的 jti；撤销后置 NULL"
    )
    token_issued_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Kafka mTLS 客户端证书
    kafka_cert_sn: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment="Kafka 证书序列号（大写 HEX）"
    )
    kafka_cert_issued_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # 平台访问 mTLS 客户端证书
    plat_cert_sn: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment="平台证书序列号（大写 HEX）"
    )
    plat_cert_issued_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # 首次注册时间快照（从 Redis registered_at 写入，本行创建时更新）
    first_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # 管理员自定义标签（机房/GPU 型号等）
    labels: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default="{}"
    )


__all__ = ["SimNodeRegistry"]
