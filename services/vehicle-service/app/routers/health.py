"""健康探针路由：/healthz（存活）、/readyz（就绪：DB / Kafka AdminClient / CA 私钥可读）。"""
from __future__ import annotations

import asyncio
import os
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from hunter_common.logging import get_logger, get_trace_id
from hunter_common.responses import error_response, success_response

from app.config import settings

logger = get_logger("app.routers.health")

# 单项探测超时（秒）：依赖挂起时必须快速失败返回 503，与骨架模板同策略
_PROBE_TIMEOUT_S = 2.0

router = APIRouter(tags=["health"])


@router.get("/healthz")
async def healthz() -> dict[str, object]:
    """存活探针：进程可响应即返回统一格式 code=0。"""
    return success_response(data={"status": "ok"}).model_dump()


async def _bounded_probe(coro: object) -> bool:
    """带超时的单项探测：超时/异常一律视为未就绪。"""
    try:
        return bool(await asyncio.wait_for(coro, timeout=_PROBE_TIMEOUT_S))  # type: ignore[arg-type]
    except BaseException:  # noqa: BLE001 - 探测失败/超时即未就绪，禁止向请求路径抛出
        return False


async def _kafka_admin_ready(request: Request) -> bool:
    """Kafka AdminClient 探测：调用 list_topics（同步阻塞）→ 转 asyncio.to_thread。"""
    admin = getattr(request.app.state, "kafka_admin", None)
    if admin is None:
        return False

    def _call() -> bool:
        try:
            # 任一元数据请求即可（describe_clusters 在 kafka-python 里等价于 list_topics）
            return bool(admin.list_topics())
        except Exception:  # noqa: BLE001 - 交由 _bounded_probe 收敛为 False
            return False

    return await _bounded_probe(asyncio.to_thread(_call))


def _ca_readable() -> bool:
    """CA 私钥文件存在且当前进程可读（`os.R_OK`）。"""
    path = Path(settings.kafka_ca_key_path)
    if not path.is_file():
        return False
    try:
        return os.access(path, os.R_OK)
    except OSError:
        return False


@router.get("/readyz")
async def readyz(request: Request) -> JSONResponse:
    """就绪探针：DB / Kafka AdminClient / CA 可读，任一未就绪返回 503。"""
    checks: dict[str, bool] = {}
    db = getattr(request.app.state, "db", None)
    checks["database"] = (
        await _bounded_probe(db.check_connection()) if db is not None else False
    )
    checks["kafka_admin"] = await _kafka_admin_ready(request)
    checks["ca_readable"] = _ca_readable()

    if all(checks.values()):
        return JSONResponse(content=success_response(data=checks).model_dump())
    payload = error_response(5001, "服务不可用", data=checks, request_id=get_trace_id() or None)
    return JSONResponse(status_code=503, content=payload.model_dump())
