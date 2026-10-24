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

from hunter_common.config import HunterBaseConfig


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
    scene_catalog_version: str = "1.0.1"

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

