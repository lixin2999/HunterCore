"""全局异常处理器：任何异常均返回统一响应格式（code 必须为预定义错误码）。"""
from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from hunter_common.exceptions import ErrorCode, HunterBaseException
from hunter_common.logging import get_logger, get_trace_id
from hunter_common.responses import error_response
from starlette.exceptions import HTTPException as StarletteHTTPException

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


def _unified_json(code: int, message: str, *, data: object | None = None) -> JSONResponse:
    """按错误码映射 HTTP 状态，响应体为统一格式（request_id 复用链路 trace_id）。"""
    status = HTTP_STATUS_BY_CODE.get(code, 500)
    payload = error_response(code, message, data=data, request_id=get_trace_id() or None)
    return JSONResponse(status_code=status, content=payload.model_dump())


#: HTTP 层异常（路由不存在/方法不允许等）-> 预定义错误码，禁止自定义
ERROR_CODE_BY_HTTP_STATUS: dict[int, int] = {
    401: 1001, 403: 1002, 404: 3001, 405: 2001, 429: 5001, 503: 5001,
}
#: HTTP 状态 -> 统一响应 message（缺省取原始 detail）
MESSAGE_BY_HTTP_STATUS: dict[int, str] = {404: "资源不存在", 405: "请求方法不允许"}


def _http_error_code(status_code: int) -> int:
    """HTTP 状态映射预定义错误码：4xx 兜底 2001 参数错误，5xx 兜底 5000 服务器内部错误。"""
    if status_code in ERROR_CODE_BY_HTTP_STATUS:
        return ERROR_CODE_BY_HTTP_STATUS[status_code]
    return ErrorCode.INTERNAL_ERROR if status_code >= 500 else ErrorCode.INVALID_PARAM


def _unified_http_json(
    code: int,
    message: str,
    *,
    status_code: int,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    """HTTP 层异常响应：保留原始 HTTP 状态码与响应头（如 429 的 Retry-After）。"""
    payload = error_response(code, message, request_id=get_trace_id() or None)
    return JSONResponse(status_code=status_code, content=payload.model_dump(), headers=headers)


def register_exception_handlers(app: FastAPI) -> None:
    """注册全局异常处理：业务异常 / 参数校验异常 / 未预期异常 / HTTP 层异常（404/405）。"""

    @app.exception_handler(HunterBaseException)
    async def _hunter_handler(request: Request, exc: HunterBaseException) -> JSONResponse:
        logger.warning(
            "business_exception",
            code=exc.code,
            message=exc.message,
            path=request.url.path,
        )
        # data 承载错误上下文（契约 ApiResponseError：6001 expected/actual、
        # 6003 precondition_failures[]、3003 current_status 等），一般错误为 None
        return _unified_json(exc.code, exc.message, data=exc.details or None)

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

    @app.exception_handler(StarletteHTTPException)
    async def _http_exception_handler(
        request: Request, exc: StarletteHTTPException
    ) -> JSONResponse:
        # 路由不存在(404)/方法不允许(405) 等 HTTP 层异常同样必须为统一响应格式
        code = _http_error_code(exc.status_code)
        message = MESSAGE_BY_HTTP_STATUS.get(exc.status_code, str(exc.detail or "HTTP 异常"))
        logger.warning(
            "http_exception",
            status_code=exc.status_code,
            code=code,
            path=request.url.path,
        )
        return _unified_http_json(
            code, message, status_code=exc.status_code, headers=exc.headers
        )
