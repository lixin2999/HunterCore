"""HunterCore 共享代码库（common/python/hunter_common）。

模块清单：
- config      pydantic-settings 基础配置（所有服务配置基类）
- logging     structlog JSON 日志（trace_id / vehicle_id 上下文注入 + 敏感字段脱敏）
- exceptions  HunterBaseException 与预定义错误码异常（1001-7002）
- responses   统一 API 响应模型 ApiResponse[T]
- kafka       confluent-kafka 异步生产者/消费者封装（契约驱动：契约 acks/重试/磁盘缓冲/手动提交/DLQ/Schema 校验）
- database    SQLAlchemy 2.0 数据访问层包（session/base/enums/repository/models/migrations）
- redis       redis-py 异步客户端封装
- metrics     Prometheus 指标注册与 /metrics 端点（需显式导入 hunter_common.metrics）

注意：为保持核心导入轻量，config/exceptions/responses 在此导出；
logging/kafka/database/redis/metrics 由使用方按需显式导入（如
``from hunter_common.database import BaseRepository``）。
"""
from __future__ import annotations

from hunter_common.config import HunterBaseConfig
from hunter_common.exceptions import ErrorCode, HunterBaseException
from hunter_common.responses import ApiResponse, error_response, success_response

__version__ = "0.1.0"

__all__ = [
    "ApiResponse",
    "ErrorCode",
    "HunterBaseConfig",
    "HunterBaseException",
    "__version__",
    "error_response",
    "success_response",
]
