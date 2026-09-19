"""健康探针路由：/healthz（存活）、/readyz（就绪，含 DB/Redis 连通性）。"""
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
    except BaseException:  # noqa: BLE001 - 探测失败/超时即未就绪，禁止向请求路径抛出
        return False


@router.get("/readyz")
async def readyz(request: Request) -> JSONResponse:
    """就绪探针：DB / Redis 连通性检查（应用未完成初始化视为未就绪）。

    用于 K8s readinessProbe；基础设施不可用时返回 503（code=5001 服务不可用）。
    """
    checks: dict[str, bool] = {}
    # 数据库探针走只读账号（审查 Y10：本服务不得持有业务库读写凭据）
    readonly = getattr(request.app.state, "readonly_metrics", None)
    checks["database"] = (
        await _bounded_probe("database", readonly.ping()) if readonly is not None else False
    )
    redis = getattr(request.app.state, "redis", None)
    checks["redis"] = await _bounded_probe("redis", redis.ping()) if redis is not None else False

    if all(checks.values()):
        return JSONResponse(content=success_response(data=checks).model_dump())
    payload = error_response(5001, "服务不可用", data=checks, request_id=get_trace_id() or None)
    return JSONResponse(status_code=503, content=payload.model_dump())
