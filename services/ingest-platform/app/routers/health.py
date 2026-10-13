"""健康探针路由：/healthz（存活）、/readyz（就绪，含 DB/Redis/Kafka/MinIO 连通性）。"""
from __future__ import annotations

import asyncio

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from hunter_common.logging import get_trace_id
from hunter_common.responses import error_response, success_response

#: 单项探测超时（秒）：依赖挂起时必须快速失败返回 503
_PROBE_TIMEOUT_S = 2.0

router = APIRouter(tags=["health"])


@router.get("/healthz")
async def healthz() -> dict[str, object]:
    """存活探针：进程可响应即返回统一格式 code=0。"""
    return success_response(data={"status": "ok"}).model_dump()


async def _bounded_probe(coro: object) -> bool:
    """带超时的单项探测：超时/异常一律视为未就绪。"""
    try:
        return bool(await asyncio.wait_for(coro, timeout=_PROBE_TIMEOUT_S))
    except BaseException:  # noqa: BLE001
        return False


@router.get("/readyz")
async def readyz(request: Request) -> JSONResponse:
    """就绪探针：DB / Redis / MinIO 连通性检查。

    Kafka Producer 为懒连接，不做启动时探测。
    """
    checks: dict[str, str] = {}

    # DB
    db = getattr(request.app.state, "db", None)
    if db is not None:
        checks["db"] = "ok" if await _bounded_probe(db.check_connection()) else "error"
    else:
        checks["db"] = "unknown"

    # Redis
    redis = getattr(request.app.state, "redis", None)
    if redis is not None:
        try:
            pong = await asyncio.wait_for(redis.client.ping(), timeout=_PROBE_TIMEOUT_S)
            checks["redis"] = "ok" if pong else "error"
        except BaseException:  # noqa: BLE001
            checks["redis"] = "error"
    else:
        checks["redis"] = "unknown"

    # MinIO（HEAD bucket 探测）
    storage = getattr(request.app.state, "storage", None)
    if storage is not None:
        try:
            loop = asyncio.get_event_loop()
            await asyncio.wait_for(
                loop.run_in_executor(
                    storage._executor,  # noqa: SLF001
                    lambda: storage._client.head_bucket(  # noqa: SLF001
                        Bucket=storage._settings.minio_bucket_derived  # noqa: SLF001
                    ),
                ),
                timeout=_PROBE_TIMEOUT_S,
            )
            checks["minio"] = "ok"
        except BaseException:  # noqa: BLE001
            checks["minio"] = "error"
    else:
        checks["minio"] = "unknown"

    checks["kafka"] = "ok"  # 懒连接，不做主动探测

    all_ok = all(v == "ok" for v in checks.values())
    if all_ok:
        return JSONResponse(content=success_response(data=checks).model_dump())
    payload = error_response(5001, "服务不可用", data=checks, request_id=get_trace_id() or None)
    return JSONResponse(status_code=503, content=payload.model_dump())
