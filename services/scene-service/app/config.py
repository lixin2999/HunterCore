"""scene-service 服务配置（pydantic-settings，全部参数来自环境变量/.env）。

字段与 contracts/openapi/scene-service.yaml `x-hunter-service.required_env` 一一对应；
所有 URL/密钥/端口/阈值一律可配置，禁止硬编码业务参数（配置管理约束第 16 条）：
- Carla 管理 API 地址与各步骤路径（契约 x-hunter-simulation-flow.entrypoint_env，⚠ 键名待确认 #9）；
- 场景时长/导出数量/参数覆盖白名单等实现约束（契约 SceneConfig.duration / SceneExportRequest）；
- RBAC 角色集合（资源域 scene，动作 create/read/update/delete/execute）。
"""
from __future__ import annotations

import os
import socket
from typing import ClassVar

from hunter_common.config import HunterBaseConfig
from pydantic import model_validator


class Settings(HunterBaseConfig):
    """scene-service 配置（服务私有项，基类通用项见 hunter_common.config）。"""

    # 服务标识与默认端口（仍可被环境变量 SERVICE_NAME / API_PORT 覆盖）
    service_name: str = "scene-service"
    api_port: int = 8081

    # ---------- MinIO（场景资源；bucket 名称契约固定，生命周期永久） ----------
    minio_bucket_scene_assets: str = "hunter-scene-assets"
    minio_region: str = "us-east-1"
    # 对象前缀与命名（契约 x-hunter-export.object_prefix / naming，不可更改）
    scene_export_object_prefix: str = "scenarios/"
    # 下载预签名有效期（契约 x-hunter-export.presign_expires_in_seconds 固定 900s）
    scene_export_presign_expire_seconds: int = 900

    # ---------- Carla 管理 API（契约 x-hunter-simulation-flow；禁止硬编码地址） ----------
    # ⚠ 配置键名与各步骤子路径为契约待确认项 #9（设计文档未定义），此处全部环境变量化
    carla_management_endpoint: str = ""
    carla_instance_create_path: str = "/api/v1/instances"
    carla_instance_query_path: str = "/api/v1/instances"
    carla_scenario_submit_path: str = "/api/v1/instances/{sim_instance_id}/scenario"
    # G-20① 仿真进度/结果查询子路径（无状态代理，契约 x-hunter-simulation-flow ⑥⑦）
    carla_instance_get_path: str = "/api/v1/instances/{sim_instance_id}"
    carla_instance_result_path: str = "/api/v1/instances/{sim_instance_id}/result"
    # 仿真控制（数据采集与分析系统 ↔ CARLA 详细设计 6.1.2）
    carla_instance_pause_path: str = "/api/v1/instances/{sim_instance_id}/pause"
    carla_instance_resume_path: str = "/api/v1/instances/{sim_instance_id}/resume"
    carla_instance_stop_path: str = "/api/v1/instances/{sim_instance_id}/stop"
    carla_request_timeout_seconds: float = 5.0
    carla_request_max_retries: int = 1

    # ---------- 仿真下发链路模式（V1.24.0，契约 x-hunter-simulation-flow.node_dispatch） ----------
    # direct：平台直连 Carla 管理 API（默认，保持 V1.23.7 行为不变）
    # node  ：按自学习地址直推节点 CARLA 管理 API（需平台→节点可达）
    #         地址来自注册/心跳的入站请求头，实例只绑 node_id、每次调用重新解析 ⇒ IP 漂移自愈；
    #         此模式下 carla_management_endpoint 不参与寻址（子路径配置继续复用）
    sim_dispatch_mode: str = "direct"
    # 在线租约：sim:node:{node_id} 的 TTL 与 sim:node:seen 的在线判定窗口
    sim_node_lease_seconds: int = 90
    # 注册未声明 capacity 时的默认可并发实例数
    sim_node_default_capacity: int = 1
    # 允许调用节点控制面的角色（服务令牌角色默认 data-uploader；契约 x-hunter-simulation-flow.node_dispatch.auth）
    sim_node_roles: str = "data-uploader,admin"
    # 注册未声明 manager_port 时的节点 CARLA 管理 API 默认端口
    sim_node_manager_default_port: int = 8090
    # 拼接节点基址的默认协议（仅 http/https；注册体 protocol 优先）
    sim_node_manager_scheme: str = "http"
    # 是否采信 X-Real-IP / X-Forwarded-For（false = 只认 TCP 对端，用于直连部署与防头伪造）
    sim_node_trust_forwarded_for: bool = True
    # 节点地址 CIDR 白名单（逗号分隔，空 = 不限制；不命中则降级到下一地址来源）
    sim_node_allowed_networks: str = ""
    # 每节点 CARLA 客户端缓存上限（超出按插入序淘汰；基址变化即新建并关闭旧的）
    sim_node_client_cache_max: int = 32

    # ---------- 仿真端管理（V1.24.1：令牌/证书/登记） ----------
    # 节点证书存储根目录（与车辆证书目录独立；子目录：{node_id}/{kafka|platform}/）
    sim_node_certs_dir: str = "/data/sim-node-certs"
    # 节点 mTLS 客户端证书有效期（天）；3650 ≈ 10 年，与车辆证书一致
    sim_node_cert_validity_days: int = 3650
    # openssl 子进程超时（秒）
    sim_node_cert_signer_timeout_seconds: int = 15
    # api-gateway 内部地址（签发/撤销服务令牌时代理调用）
    apigateway_internal_url: str = "http://api-gateway:8080"
    # CA 证书与私钥路径（与 vehicle-service 使用同一套 Kafka CA；只读挂载）
    kafka_ca_cert_path: str = "/certs/ca-cert.pem"
    kafka_ca_key_path: str = "/certs/ca-key.pem"
    # 平台对外可达 IP/域名（接入包 README 中组装注册 URL 用；None → 占位符）
    server_ip: str | None = None

    # ---------- 仿真端一键开通（V1.24.2：DB→SCRAM→Topics→Cert 四步编排，对齐车辆 provisioning） ----------
    # 总开关：false 时 POST /sim-nodes 一键开通端点返回 5001（保留逐项签发/管理端点不受影响）
    sim_provision_enabled: bool = True
    # SCRAM 步开关：false 时跳过创建 sim-{node_id} SCRAM 用户（仅供无 KIP-95 权限的灰度环境）
    sim_scram_enabled: bool = True
    # SCRAM 主体前缀（与 Kafka 证书 CN 前缀一致；主体 = f"{prefix}{node_id}"）
    sim_node_scram_prefix: str = "sim-"
    # SCRAM 迭代次数（KIP-95 建议默认，与 vehicle-service kafka_admin._SCRAM_ITERATIONS 对齐）
    sim_scram_iterations: int = 8192
    # 节点级回传 Topic（simulation.{node_id}.result + .dlq）创建/删除超时（毫秒）
    sim_kafka_admin_timeout_ms: int = 10_000
    # 下发通道 mTLS（V1.24.2，x-hunter-simulation-flow.node_dispatch.security）：
    # false = 旧直推（http 明文，仅供灰度/回滚）；true = 平台以 sim-platform-{node_id} 客户端证书回访节点 CARLA 管理 API
    sim_dispatch_mtls_enabled: bool = False
    # 下发通道服务端证书验证（V1.24.7）：
    # true（默认）= 用 KAFKA_CA_CERT_PATH 验证节点服务端证书；
    # false = 跳过服务端验证（节点服务端证书非 Kafka CA 签发时使用，mTLS 客户端认证仍保留、链路仍加密）
    sim_dispatch_ssl_verify: bool = True

    # ---------- 仿真会话 / 动态 Topic（CARLA 接入方案 P1） ----------
    # VIL 会话 Redis 契约：vil:session:{vehicle_id}（String JSON，7200s）、vil:active（Set）
    vil_session_ttl_seconds: int = 7200
    # 动态 Topic 由 scene-service 在创建实例前显式创建（broker 关闭自动建 Topic）
    sim_topic_provisioning_enabled: bool = True
    sim_topic_replication_factor: int = 1
    # 终态后延迟删除 Topic（秒）；<=0 表示立即删除
    sim_topic_delete_delay_seconds: int = 600
    # 实例会话登记（sim:instance:{sim_instance_id}，TTL 86400s）
    sim_instance_ttl_seconds: int = 86400

    # ---------- 场景约束（契约 SceneConfig / SceneExportRequest） ----------
    # ⚠ 时长上限设计文档未定义（待确认项 #9）：默认 600s，超限返回 2001
    scene_duration_max_seconds: float = 600.0
    # 契约 SceneExportRequest.scene_ids maxItems = 50
    scene_export_max_scenes: int = 50
    # 参数覆盖白名单（契约 SceneRunRequest：仅允许覆盖 4.2.2 白名单路径，禁止覆盖 map.map_id）
    scene_param_override_allowed_paths: str = "ego_vehicle,weather,duration,actors,events,success_criteria"
    scene_param_override_forbidden_paths: str = "map"
    # 场景详情缓存 TTL（契约 redis_keys: cache:scene:{scene_id} = String(JSON)，3600s）
    scene_detail_cache_ttl_seconds: int = 3600
    # 复制场景命名后缀与重试上限（契约 duplicateScene：<源名称>-copy，冲突追加序号）
    scene_duplicate_name_suffix: str = "-copy"
    scene_duplicate_max_attempts: int = 100
    # 版本默认值（契约 SCENE 默认语义化版本 1.0.0）
    scene_default_version: str = "1.0.0"

    # ---------- 场景引擎（逻辑/具体场景、生成任务） ----------
    # 生成数量上限（DDL generation_tasks.count CHECK 1..5000，二者必须一致）
    scene_generation_max_count: int = 5000
    # 地图白名单（G3；逗号分隔，空 = 不限制，具体部署可按开放区域收紧）
    scene_allowed_maps: str = ""
    # 地图几何围栏（G3；"xmin,ymin,xmax,ymax"，空 = 不校验）
    scene_map_bounds: str = ""
    # 车端车辆参数文件（vehicle_params 导出）；空 = 使用包内 config/vehicle_params.json
    scene_vehicle_params_path: str = ""
    # CARLA 车辆蓝图几何表（实机 get_dimension / get_bounding_box 导出，V1.24.9）；
    # 空 = 使用包内 config/carla_vehicle_geometry.json（内置默认未校准，下发时只标状态、不给尺寸）
    scene_carla_vehicle_geometry_path: str = ""
    # esmini 干跑（G5）：可执行文件/命令前缀；空 = 未部署，G5 标记 skipped（不计失败）
    scene_esmini_bin: str = ""
    scene_esmini_timeout_seconds: float = 10.0
    # 生成时 esmini 抽样比例（0~1；validate/dryrun/复检始终全量干跑）
    scene_esmini_sample_ratio: float = 0.05
    # 生成任务归属实例（空 = POD_NAME / hostname）；用于启动对账与取消归属
    scene_task_owner: str = ""
    # 任务心跳超时秒数：超过则该 pending/running 任务视为失联，启动时对账置 failed
    scene_task_stale_seconds: int = 300
    # 批量复检并发上限（含编译与 esmini 子进程，属阻塞操作 → 线程池限流）
    scene_validate_concurrency: int = 4
    # 内置 IR 目录版本（与 scene_engine.catalog.CATALOG_VERSION 一致性由测试钉住）
    scene_catalog_version: str = "1.1.0"

    # ---------- RBAC（网关注入 X-Roles；资源域 scene，契约 securitySchemes.bearerAuth） ----------
    scene_read_roles: str = "admin,analyst,operator"
    scene_write_roles: str = "admin,operator"
    scene_execute_roles: str = "admin,operator"
    # 发布/基线锁定需评审角色（设计 §9.2：reviewer/admin）
    scene_review_roles: str = "admin,reviewer"
    # 追加式审计（发布/锁定/目录同步）；哈希链防篡改
    scene_audit_enabled: bool = True
    # 数据权限隔离（契约：creator 参数「数据权限隔离时由服务端强制注入」）；
    # 默认关闭 = 场景库为团队共享资源，开启后非豁免角色仅可见/可查自有场景
    scene_scope_by_creator: bool = False
    scene_scope_exempt_roles: str = "admin"

    # ---------- 实车场景自动提取（设计文档 4.5 节 + 契约 x-hunter-real-vehicle-extraction） ----------
    analytics_result_topic: str = "analytics_result"
    scene_extraction_group_id: str = "scene-service-analytics-result"
    scene_extraction_consumer_enabled: bool = True
    kafka_extraction_poll_timeout_seconds: float = 1.0
    # 提取产物 creator：4.5 节由 Kafka 自动触发，无人类操作者（⚠ 系统账号 uuid 待确认，见 README 风险清单）
    scene_extraction_creator_id: str = "00000000-0000-0000-0000-000000000000"
    # 车端基线车型（HUNTER SE）；实车回放场景的 ego_vehicle.model
    scene_extraction_default_ego_model: str = "HUNTER_SE"
    # 提取场景的成功判据默认值（analytics_result 未携带 success_criteria）
    scene_extraction_min_safe_distance: float = 1.0
    scene_extraction_max_speed_deviation: float = 1.0

    # ---------- Carla 子路径归一（V1.23.7） ----------

    #: 下发模式合法取值（契约 x-hunter-simulation-flow.node_dispatch.mode_switch）
    SIM_DISPATCH_MODES: ClassVar[tuple[str, ...]] = ("direct", "node")

    #: 节点基址协议合法取值（契约 x-hunter-simulation-flow.node_dispatch.endpoint_learning.base_url）
    NODE_MANAGER_SCHEMES: ClassVar[tuple[str, ...]] = ("http", "https")

    #: 允许“空串回落默认”的 Carla 子路径键（V1.23.7）。
    #
    #: compose 侧统一用 ``${CARLA_*_PATH:-}`` 透传：① 避开 docker compose 变量插值在默认值
    #: 含 ``{...}`` 时的截断风险；② “未设置”与“设为空”在容器内同为空串，必须由应用侧归一。
    CARLA_PATH_FIELDS: ClassVar[tuple[str, ...]] = (
        "carla_instance_create_path",
        "carla_instance_query_path",
        "carla_scenario_submit_path",
        "carla_instance_get_path",
        "carla_instance_result_path",
        "carla_instance_pause_path",
        "carla_instance_resume_path",
        "carla_instance_stop_path",
    )

    @model_validator(mode="after")
    def _blank_carla_paths_fallback(self) -> Settings:
        """子路径留空/纯空白 → 回落字段默认值（与 K8s ConfigMap 默认一致）；同时归一下发模式。"""
        fields = type(self).model_fields
        for name in self.CARLA_PATH_FIELDS:
            value = getattr(self, name)
            if not value.strip():
                setattr(self, name, fields[name].default)
            elif value != value.strip():
                setattr(self, name, value.strip())
        # 下发模式归一：大小写/空白容忍，非法值回落 direct（禁止因拼写错误静默换链路）
        mode = self.sim_dispatch_mode.strip().lower()
        if mode not in self.SIM_DISPATCH_MODES:
            mode = "direct"
        object.__setattr__(self, "sim_dispatch_mode", mode)
        # 节点协议归一：同上，非法值回落 http（避免拼出 http://…:// 形态的非法基址）
        scheme = self.sim_node_manager_scheme.strip().lower()
        if scheme not in self.NODE_MANAGER_SCHEMES:
            scheme = "http"
        object.__setattr__(self, "sim_node_manager_scheme", scheme)
        return self

    # ---------- 派生集合 ----------

    @property
    def scene_read_role_set(self) -> frozenset[str]:
        """scene:read 授权角色集合（列表/详情/模板/导出）。"""
        return self._role_set(self.scene_read_roles)

    @property
    def scene_write_role_set(self) -> frozenset[str]:
        """scene:create|update|delete 授权角色集合（创建/更新/删除/复制）。"""
        return self._role_set(self.scene_write_roles)

    @property
    def scene_execute_role_set(self) -> frozenset[str]:
        """scene:execute 授权角色集合（发布/下发 Carla 仿真）。"""
        return self._role_set(self.scene_execute_roles)

    @property
    def scene_review_role_set(self) -> frozenset[str]:
        """评审授权角色集合（发布/基线锁定）。"""
        return self._role_set(self.scene_review_roles)

    @property
    def sim_node_role_set(self) -> frozenset[str]:
        """节点控制面授权角色集合（注册/心跳）。"""
        return self._role_set(self.sim_node_roles)

    @property
    def sim_node_allowed_network_list(self) -> tuple[str, ...]:
        """节点地址 CIDR 白名单（已去空白；空元组 = 不限制）。"""
        return tuple(item.strip() for item in self.sim_node_allowed_networks.split(",") if item.strip())

    @property
    def sim_dispatch_via_node(self) -> bool:
        """当前是否按节点动态地址直推（node 模式；否则为固定地址直连 Carla 管理 API）。"""
        return self.sim_dispatch_mode == "node"

    @property
    def map_bounds(self) -> tuple[float, float, float, float] | None:
        """G3 地图几何围栏（xmin, ymin, xmax, ymax）；空/非法 = 不校验。"""
        parts = [item.strip() for item in self.scene_map_bounds.split(",") if item.strip()]
        if len(parts) != 4:
            return None
        try:
            xmin, ymin, xmax, ymax = (float(item) for item in parts)
        except ValueError:
            return None
        if xmin >= xmax or ymin >= ymax:
            return None
        return xmin, ymin, xmax, ymax

    @property
    def task_owner(self) -> str:
        """任务归属实例标识（配置优先，其次 POD_NAME / hostname）。"""
        configured = self.scene_task_owner.strip()
        if configured:
            return configured
        return os.getenv("POD_NAME") or socket.gethostname()

    @property
    def scene_scope_exempt_role_set(self) -> frozenset[str]:
        """数据权限隔离豁免角色（默认 admin 可见全部场景）。"""
        return self._role_set(self.scene_scope_exempt_roles)

    @property
    def allowed_map_set(self) -> frozenset[str]:
        """G3 地图白名单（空集合 = 不限制）。"""
        return self._path_set(self.scene_allowed_maps)

    @property
    def param_override_allowed_set(self) -> frozenset[str]:
        """参数覆盖白名单顶层路径（点分路径的首段）。"""
        return self._path_set(self.scene_param_override_allowed_paths)

    @property
    def param_override_forbidden_set(self) -> frozenset[str]:
        """参数覆盖禁止路径（优先级高于白名单，如 map）。"""
        return self._path_set(self.scene_param_override_forbidden_paths)

    @staticmethod
    def _role_set(raw: str) -> frozenset[str]:
        return frozenset(role.strip() for role in raw.split(",") if role.strip())

    @staticmethod
    def _path_set(raw: str) -> frozenset[str]:
        return frozenset(path.strip() for path in raw.split(",") if path.strip())


settings = Settings()

