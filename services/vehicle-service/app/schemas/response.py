"""vehicle-service 统一响应信封（对齐契约 ApiResponseXxx 系列）。

契约要求：所有业务响应体形如 `{code, message, data, request_id, timestamp}`；
本模块提供各种 `data` 具体类型的信封模型，供路由层显式声明 `response_model`。

ZIP（/bundle）作为契约明确例外，不走信封（直接 Response 二进制）。
"""
from __future__ import annotations

import time
from typing import Generic, TypeVar
from uuid import uuid4

from pydantic import BaseModel, Field

from app.schemas.vehicle import (
    ProvisionResult,
    ReissueCertResult,
    RotateScramResult,
    VehicleDetail,
    VehiclePage,
)

T = TypeVar("T")


class ApiResponse(BaseModel, Generic[T]):
    code: int = 0
    message: str = "success"
    data: T | None = None
    request_id: str = Field(default_factory=lambda: str(uuid4()))
    timestamp: int = Field(default_factory=lambda: int(time.time()))


ApiResponseVehiclePage = ApiResponse[VehiclePage]
ApiResponseVehicleDetail = ApiResponse[VehicleDetail]
ApiResponseProvisionResult = ApiResponse[ProvisionResult]
ApiResponseRotateScramResult = ApiResponse[RotateScramResult]
ApiResponseReissueCertResult = ApiResponse[ReissueCertResult]
ApiResponseEmpty = ApiResponse[None]


__all__ = [
    "ApiResponse",
    "ApiResponseEmpty",
    "ApiResponseProvisionResult",
    "ApiResponseReissueCertResult",
    "ApiResponseRotateScramResult",
    "ApiResponseVehicleDetail",
    "ApiResponseVehiclePage",
]
