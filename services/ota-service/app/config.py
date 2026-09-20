"""ota-service 服务配置（pydantic-settings，全部参数来自环境变量/.env）。

字段与 contracts/openapi/ota-service.yaml `x-hunter-service.required_env` 一一对应；
所有 URL/密钥/端口/阈值一律可配置，禁止硬编码业务参数（配置管理约束）。

⚠ 契约固定值（不可更改，启动时强校验，见 ``_validate_canary``）：
- 灰度批次比例 OTA_CANARY_STAGES 必须为 "5,20,50,100"（x-hunter-canary-rollout）；
- 每批观察 OTA_CANARY_OBSERVE_HOURS 固定 24（契约 OtaCanaryBatch.observe_hours enum [24]）；
- 成功率门禁 OTA_CANARY_MIN_SUCCESS_RATE 固定 0.95（契约 enum [0.95]）。
  如需调整必须先变更契约（x-hunter-pending-confirmation #16/#17）。
"""
from __future__ import annotations

from hunter_common.config import HunterBaseConfig
from pydantic import field_validator, model_validator


class Settings(HunterBaseConfig):
    """ota-service 配置（服务私有项，基类通用项见 hunter_common.config）。"""

    # 服务标识与默认端口（仍可被环境变量 SERVICE_NAME / API_PORT 覆盖）
    service_name: str = "ota-service"
    api_port: int = 8084

    # ---------- MinIO（升级包存储；bucket 名称契约固定，永久保留 + SSE-S3） ----------
    minio_bucket_ota_packages: str = "hunter-ota-packages"
    minio_region: str = "us-east-1"
    # 预签名有效期（x-hunter-version-upload-flow.presign_policy：上传 1 小时 / 下载 15 分钟）
    presigned_upload_expire_seconds: int = 3600
    presigned_download_expire_seconds: int = 900

    # ---------- 灰度发布（x-hunter-canary-rollout，契约固定值，启动强校验） ----------
    ota_canary_stages: str = "5,20,50,100"
    ota_canary_observe_hours: int = 24
    ota_canary_min_success_rate: float = 0.95
    # 灰度调度器扫描周期（观察窗口到期判定 + 批次推进；契约 OTA_ROLLOUT_TICK_SECONDS）
    ota_rollout_tick_seconds: int = 60
    # 灰度自动调度器总开关（x-hunter-canary-rollout.scheduler；单机/测试可关闭，默认启用）
    rollout_scheduler_enabled: bool = True
    # 单任务目标车辆上限（防误操作全量下发；超限 2001）
    ota_task_max_target_vehicles: int = 1000

    # ---------- 升级门禁（设计文档：电量≥50% / 静止(P 档) / 网络稳定 / 存储≥2GB） ----------
    ota_precondition_min_soc: int = 50
    ota_precondition_require_parked: bool = True
    ota_precondition_min_storage_mb: int = 2048
    # 网络稳定判定：遥测最后上报间隔 ≤ 该值（默认 10s，与 communication_loss 阈值同源）
    ota_offline_threshold_seconds: int = 10
    # ⚠ 门禁读模型缺失时的策略：默认 false = 缺数据即拒绝（安全默认，x-hunter-pending-confirmation #19）
    ota_offline_fallback_enabled: bool = False

    # ---------- 发布校验开关（x-hunter-ota-security：生产禁止关闭，仅 DEBUG 联调可关） ----------
    # ⚠ G-05：staging/prod 下关闭任一开关或缺验签公钥 → 启动 fail-fast（见下方 model_validator），
    #   杜绝“OTA_PACKAGE_VERIFY_* = false 上线 → 恶意/篡改升级包直推车端”路径
    ota_package_verify_sha256: bool = True
    ota_package_verify_signature: bool = True
    # RSA-2048 验签公钥（PEM；由 hunter-app-secrets 注入；私钥仅存发布方 CI/KMS）
    ota_signature_public_key: str = ""

    # ---------- Kafka 消费/生产声明（对齐 contracts/kafka/{topics,consumer-groups}.yaml） ----------
    # 消费组与订阅 pattern 契约固定：consumer-groups.yaml → group_id: ota-service-ota-status
    kafka_ota_status_group_id: str = "ota-service-ota-status"
    ota_status_subscribe_pattern: str = "^hunter\\..*\\.ota_status$"
    ota_status_consumer_enabled: bool = True
    # 生产 Topic 命名模板（契约固定：hunter.{vehicle_id}.ota_notify / hunter.{vehicle_id}.command）
    ota_notify_topic_pattern: str = "hunter.{vehicle_id}.ota_notify"
    vehicle_command_topic_pattern: str = "hunter.{vehicle_id}.command"

    # ---------- RBAC（网关注入 X-Roles；ota 资源域三动作，契约 securitySchemes.bearerAuth） ----------
    ota_read_roles: str = "admin,analyst,operator"
    ota_create_roles: str = "admin,operator"
    ota_execute_roles: str = "admin,operator"

    # ---------- 接口限流（附录 D：POST /versions 单用户 5 QPS；publish 建议值 2 QPS） ----------
    version_create_rate_limit_per_min: int = 5
    publish_rate_limit_per_min: int = 2
    rate_limit_window_seconds: int = 60

    @field_validator("ota_canary_stages")
    @classmethod
    def _validate_canary_stages(cls, value: str) -> str:
        """灰度批次定义不可更改（契约 x-hunter-canary-rollout.batches）。"""
        stages = [stage.strip() for stage in value.split(",") if stage.strip()]
        if [float(stage) for stage in stages] != [5.0, 20.0, 50.0, 100.0]:
            raise ValueError(
                "OTA_CANARY_STAGES 必须为 '5,20,50,100'（灰度流程不可更改，"
                "变更须先修改 contracts/openapi/ota-service.yaml）"
            )
        return ",".join(stages)

    @field_validator("ota_canary_observe_hours")
    @classmethod
    def _validate_observe_hours(cls, value: int) -> int:
        """每批观察时长固定 24h（契约 OtaCanaryBatch.observe_hours enum [24]）。"""
        if value != 24:
            raise ValueError(
                "OTA_CANARY_OBSERVE_HOURS 固定为 24（契约不可更改），"
                "如需调整须先修改 contracts/openapi/ota-service.yaml"
            )
        return value

    @field_validator("ota_canary_min_success_rate")
    @classmethod
    def _validate_success_rate(cls, value: float) -> float:
        """每批成功率门禁固定 0.95（契约 OtaCanaryBatch.success_rate_threshold enum [0.95]）。"""
        if value != 0.95:
            raise ValueError(
                "OTA_CANARY_MIN_SUCCESS_RATE 固定为 0.95（契约不可更改），"
                "如需调整须先修改 contracts/openapi/ota-service.yaml"
            )
        return value

    @model_validator(mode="after")
    def _require_package_verification_in_production(self) -> Settings:
        """G-05（x-hunter-ota-security，设计文档 7.2.4/14.3）：发布校验生产不可关闭。

        staging/prod 下：SHA-256/签名验证任一被关或验签公钥缺失 → 启动即失败；
        dev/test 保留关闭能力供联调（上传接口校验失败仅 2001，不阻断开发）。
        """
        if self.environment not in ("staging", "prod"):
            return self
        disabled: list[str] = []
        if not self.ota_package_verify_sha256:
            disabled.append("OTA_PACKAGE_VERIFY_SHA256=false")
        if not self.ota_package_verify_signature:
            disabled.append("OTA_PACKAGE_VERIFY_SIGNATURE=false")
        if not self.ota_signature_public_key.strip():
            disabled.append("OTA_SIGNATURE_PUBLIC_KEY 未注入（验签无公钥可用）")
        if disabled:
            raise ValueError(
                "生产环境（environment={}）禁止降级升级包校验：{}；"
                "校验开关仅允许在 dev/test 联调使用（契约 x-hunter-ota-security）".format(
                    self.environment, "、".join(disabled)
                )
            )
        return self

    @property
    def ota_read_role_set(self) -> frozenset[str]:
        """ota:read 授权角色集合（版本/任务/记录查询）。"""
        return frozenset(role.strip() for role in self.ota_read_roles.split(",") if role.strip())

    @property
    def ota_create_role_set(self) -> frozenset[str]:
        """ota:create 授权角色集合（创建版本与任务）。"""
        return frozenset(role.strip() for role in self.ota_create_roles.split(",") if role.strip())

    @property
    def ota_execute_role_set(self) -> frozenset[str]:
        """ota:execute 授权角色集合（发布/退役/启动/暂停/恢复/终止/回滚）。"""
        return frozenset(role.strip() for role in self.ota_execute_roles.split(",") if role.strip())

    @property
    def canonical_canary_stages(self) -> list[float]:
        """灰度批次比例（契约固定 5/20/50/100；x-hunter-canary-rollout）。"""
        return [5.0, 20.0, 50.0, 100.0]


settings = Settings()

