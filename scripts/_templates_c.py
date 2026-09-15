"""HunterEdge 骨架模板（C）：全局异常处理器 / 健康探针路由。"""
from __future__ import annotations

from string import Template

ERROR_HANDLERS_TMPL = Template('''"""全局异常处理器：任何异常均返回统一响应格式（code 必须为预定义错误码）。"""
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
''')

HEALTH_ROUTER_TMPL = Template('''"""健康探针路由：/healthz（存活）、/readyz（就绪，含 DB/Redis 连通性）。"""
from __future__ import annotations

import asyncio

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from hunter_common.logging import get_trace_id
from hunter_common.responses import error_response, success_response

# 单项探测超时（秒）：依赖挂起时必须快速失败返回 503，
# 而不是被底层客户端重试拖住（实测 redis-py/asyncpg 重试可达 4s+，
# 会触发 K8s readinessProbe 超时误判）。阈值来源：K8s probe 默认 1s，留 2s 余量。
_PROBE_TIMEOUT_S = 2.0

router = APIRouter(tags=["health"])


@router.get("/healthz")
async def healthz() -> dict[str, object]:
    """存活探针：进程可响应即返回统一格式 code=0。"""
    return success_response(data={"status": "ok"}).model_dump()


async def _bounded_probe(name: str, coro: object) -> bool:
    """带超时的单项探测：超时/异常一律视为未就绪（禁止探测挂起请求）。"""
    try:
        return bool(await asyncio.wait_for(coro, timeout=_PROBE_TIMEOUT_S))
    except BaseException:  # noqa: BLE001, S110 - 探测失败/超时即未就绪，禁止向请求路径抛出
        return False


@router.get("/readyz")
async def readyz(request: Request) -> JSONResponse:
    """就绪探针：DB / Redis 连通性检查（应用未完成初始化视为未就绪）。

    用于 K8s readinessProbe；基础设施不可用时返回 503（code=5001 服务不可用）。
    """
    checks: dict[str, bool] = {}
    db = getattr(request.app.state, "db", None)
    checks["database"] = (
        await _bounded_probe("database", db.check_connection()) if db is not None else False
    )
    redis = getattr(request.app.state, "redis", None)
    checks["redis"] = await _bounded_probe("redis", redis.ping()) if redis is not None else False

    if all(checks.values()):
        return JSONResponse(content=success_response(data=checks).model_dump())
    payload = error_response(5001, "服务不可用", data=checks, request_id=get_trace_id() or None)
    return JSONResponse(status_code=503, content=payload.model_dump())
''')
