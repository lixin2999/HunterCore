"""ota-service 服务配置（pydantic-settings，全部参数来自环境变量/.env）。"""
from __future__ import annotations

from hunter_common.config import HunterBaseConfig


class Settings(HunterBaseConfig):
    """ota-service 配置；服务私有配置项按契约逐步补充（禁止硬编码业务参数）。"""

    # 服务标识与默认端口（仍可被环境变量 SERVICE_NAME / API_PORT 覆盖）
    service_name: str = "ota-service"
    api_port: int = 8084


settings = Settings()
