"""业务端点统一错误响应声明（附录 A 预定义错误码 → HTTP 状态映射见 core/error_handlers）。

契约 components.responses 与 x-hunter-error-status-map 为单一事实来源；
此处仅用于 OpenAPI 文档声明，实际状态码由全局异常处理器按错误码映射。
"""
from __future__ import annotations

from typing import Any

#: 通用错误响应（各业务端点共用；2001/2002 → 422，3001 → 404，3002/3003 → 409，
#: 1001/1003 → 401，1002 → 403，5001 → 503，5000 → 500）
COMMON_ERRORS: dict[int | str, dict[str, Any]] = {
    401: {"description": "1001/1003 未认证（Token 缺失/无效/过期）"},
    403: {"description": "1002 无权限（RBAC 资源域 scene 权限不足）"},
    404: {"description": "3001 场景不存在（含已软删除场景）"},
    409: {"description": "3002 场景名称已存在 / 3003 资源状态冲突"},
    422: {"description": "2001 参数错误 / 2002 参数缺失（请求体或查询参数校验失败）"},
    500: {"description": "5000 服务器内部错误（未预期异常）"},
    503: {"description": "5001 服务不可用（PostgreSQL / Redis / MinIO / Carla 不可达）"},
}

#: 创建/复制端点的错误响应（201 成功 + 名称冲突 409）
CREATE_ERRORS: dict[int | str, dict[str, Any]] = {
    **COMMON_ERRORS,
    409: {"description": "3002 场景名称已存在（唯一索引 uq_scenes_scene_name）"},
}

__all__ = ["COMMON_ERRORS", "CREATE_ERRORS"]