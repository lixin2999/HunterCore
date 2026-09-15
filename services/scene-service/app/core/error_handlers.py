"""全局异常处理器：任何异常均返回统一响应格式（code 必须为预定义错误码）。"""
from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from hunter_common.exceptions import ErrorCode, HunterBaseException
from hunter_common.logging import get_logger, get_trace_id
from hunter_common.responses import error_response

logger = get_logger("app.core.error_handlers")

#: 业务错误码 -> HTTP 状态码映射（响应体统一为 ApiResponse 五字段格式）
HTTP_STATUS_BY_CODE: dict[int, int] = {
    1001: 401, 1002: 403, 1003: 401,
    2001: 422, 2002: 422,
    3001: 404, 3002: 409, 3003: 409,
    4001: 409, 4002: 409,
    5000: 500, 5001: 503,
    6001: 422, 6002: 422, 6003: 422,
    7001: 409, 7002: 502,
}


def _unified_json(code: int, message: str) -> JSONResponse:
    """按错误码映射 HTTP 状态，响应体为统一格式（request_id 复用链路 trace_id）。"""
    status = HTTP_STATUS_BY_CODE.get(code, 500)
    payload = error_response(code, message, request_id=get_trace_id() or None)
    return JSONResponse(status_code=status, content=payload.model_dump())


def register_exception_handlers(app: FastAPI) -> None:
    """注册三类全局异常处理：业务异常 / 参数校验异常 / 未预期异常。"""

    @app.exception_handler(HunterBaseException)
    async def _hunter_handler(request: Request, exc: HunterBaseException) -> JSONResponse:
        logger.warning(
            "business_exception",
            code=exc.code,
            message=exc.message,
            path=request.url.path,
        )
        return _unified_json(exc.code, exc.message)

    @app.exception_handler(RequestValidationError)
    async def _validation_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        # 参数校验失败 -> 2001 参数错误（预定义错误码，禁止自定义）
        logger.warning("validation_error", errors=str(exc.errors()[:5]), path=request.url.path)
        return _unified_json(ErrorCode.INVALID_PARAM, "参数错误")

    @app.exception_handler(Exception)
    async def _unhandled_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("unhandled_exception", path=request.url.path)
        return _unified_json(ErrorCode.INTERNAL_ERROR, "服务器内部错误")
