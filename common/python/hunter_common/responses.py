"""统一 API 响应格式（所有服务必须遵守，字段名不可更改）。

```json
{
  "code": 0,
  "message": "success",
  "data": {},
  "request_id": "uuid",
  "timestamp": 1724035200
}
```

- code=0 表示成功，非 0 表示预定义错误码（见 hunter_common.exceptions.ErrorCode）
"""
from __future__ import annotations

import time
import uuid
from typing import Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class ApiResponse(BaseModel, Generic[T]):
    """统一响应模型：code=0 成功，非 0 为预定义错误码。"""

    code: int = 0
    message: str = "success"
    data: T | None = None
    request_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: int = Field(default_factory=lambda: int(time.time()))


def success_response(
    data: T | None = None,
    *,
    message: str = "success",
    request_id: str | None = None,
) -> ApiResponse[T]:
    """构造成功响应（code=0）。"""
    return ApiResponse[T](
        code=0,
        message=message,
        data=data,
        request_id=request_id or str(uuid.uuid4()),
    )


def error_response(
    code: int,
    message: str,
    *,
    data: T | None = None,
    request_id: str | None = None,
) -> ApiResponse[T]:
    """构造错误响应（code 必须为预定义错误码，禁止自定义）。"""
    return ApiResponse[T](
        code=code,
        message=message,
        data=data,
        request_id=request_id or str(uuid.uuid4()),
    )
