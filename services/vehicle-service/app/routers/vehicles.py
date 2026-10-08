"""`/api/v1/vehicle/**` 业务路由（8 端点）。

契约：contracts/openapi/vehicle-service.yaml。

设计要点：
- **依赖注入**：路由通过 `Depends(get_store)` / `Depends(get_provisioner)` 从 `app.state`
  取服务，测试可整体替换（lifespan 装配）；
- **响应信封**：所有 JSON 响应统一 `{code, message, data, request_id, timestamp}`；
  ZIP（/bundle）例外直接返回二进制 + Content-Disposition；
- **RBAC**：5 个动作分别绑定 `require_vehicle_*`（dependencies.py 工厂生成）；
- **SCRAM 口令安全**：路由日志与 error handler 均不 echo 请求/响应体的 `scram_password`；
  ProvisionResult.scram_password 通过 Pydantic `writeOnly=True` 保证只出现在响应体。
"""
from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, Path, Query, Request, Response
from hunter_common.database import VehicleStatus
from hunter_common.database.models import Vehicle
from hunter_common.database.repository import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from hunter_common.exceptions import (
    ResourceNotFoundError,
    ServiceUnavailableError,
)
from hunter_common.logging import get_logger
from hunter_common.responses import success_response

from app.config import settings
from app.core.dependencies import (
    require_vehicle_create,
    require_vehicle_delete,
    require_vehicle_execute,
    require_vehicle_read,
    require_vehicle_update,
    trace_request_id,
)
from app.repositories.vehicle import VehicleStore
from app.schemas.response import (
    ApiResponseEmpty,
    ApiResponseProvisionResult,
    ApiResponseReissueCertResult,
    ApiResponseRotateScramResult,
    ApiResponseVehicleDetail,
    ApiResponseVehiclePage,
)
from app.schemas.vehicle import (
    ProvisionResult,
    ReissueCertResult,
    RotateScramResult,
    VehicleCreateRequest,
    VehicleDetail,
    VehiclePage,
    VehicleRow,
    VehicleUpdateRequest,
)
from app.services import bundle as bundle_mod
from app.services import kafka_admin
from app.services.provisioner import Provisioner, aggregate_from_row, steps_to_list

logger = get_logger("app.routers.vehicles")

router = APIRouter(prefix="/api/v1/vehicle", tags=["vehicle"])

_VEHICLE_ID_PATTERN = r"^[A-Za-z0-9_-]{1,32}$"

_ERRORS = {
    401: {"description": "1001/1003 未认证"},
    403: {"description": "1002 无权限"},
    404: {"description": "3001 资源不存在"},
    422: {"description": "2001 参数错误"},
    500: {"description": "5000 服务器内部错误"},
    503: {"description": "5001 服务不可用（Kafka/CA/DB 依赖异常）"},
}


# =====================================================================
# 依赖提取（lifespan 装配到 app.state，测试可 monkeypatch）
# =====================================================================
def get_store(request: Request) -> VehicleStore:
    return request.app.state.vehicle_store  # type: ignore[no-any-return]


def get_provisioner(request: Request) -> Provisioner:
    return request.app.state.provisioner  # type: ignore[no-any-return]


# =====================================================================
# ORM → Pydantic 转换
# =====================================================================
def _topics_preview(vehicle_id: str) -> list[str]:
    try:
        return [t["name"] for t in kafka_admin.render_vehicle_topics(vehicle_id)]
    except Exception:  # noqa: BLE001 - 详情视图不应因契约加载失败而整体 500
        return []


def _bootstrap_value() -> str:
    host = settings.server_ip or "127.0.0.1"
    return f"{host}:{settings.kafka_external_port}"


def _to_row(v: Vehicle) -> VehicleRow:
    return VehicleRow(
        vehicle_id=v.vehicle_id,
        vehicle_name=v.vehicle_name,
        model=v.model,
        firmware_version=v.firmware_version,
        software_version=v.software_version,
        status=cast_status(v.status),
        register_time=v.register_time,
        last_online_time=v.last_online_time,
        device_cert_sn=v.device_cert_sn,
        provision_state=aggregate_from_row(v.provision_status),  # type: ignore[arg-type]
        provision_steps=steps_to_list(v.provision_status),
    )


def _to_detail(v: Vehicle) -> VehicleDetail:
    row = _to_row(v)
    return VehicleDetail(
        **row.model_dump(),
        fence_json=v.fence_json,
        description=v.description,
        kafka_bootstrap=_bootstrap_value(),
        scram_username=v.vehicle_id,
        topics_preview=_topics_preview(v.vehicle_id),
    )


def cast_status(value: object) -> str:
    """VehicleStatus StrEnum → Literal 字符串（Pydantic 会二次校验词表）。"""
    return str(value)


# =====================================================================
# 列表 + 详情 + 修改（读侧 + 台账 PATCH）
# =====================================================================
@router.get(
    "/list",
    operation_id="listVehicles",
    response_model=ApiResponseVehiclePage,
    summary="车辆列表（分页 + 状态过滤）",
    responses={**_ERRORS},
)
async def list_vehicles(
    _user: Annotated[str, Depends(require_vehicle_read)],
    store: Annotated[VehicleStore, Depends(get_store)],
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)] = DEFAULT_PAGE_SIZE,
    status: Annotated[VehicleStatus | None, Query(description="车辆状态过滤（8 态）")] = None,
    provision_state: Annotated[
        str | None,
        Query(pattern="^(pending|in_progress|ready|failed)$", description="provisioning 汇总态"),
    ] = None,
    keyword: Annotated[str | None, Query(max_length=64, description="模糊匹配 vehicle_id/vehicle_name")] = None,
) -> ApiResponseVehiclePage:
    rows, total = await store.list_page(
        status=status,
        provision_state=provision_state,
        keyword=keyword,
        page=page,
        page_size=page_size,
    )
    payload = VehiclePage(
        items=[_to_row(v) for v in rows],
        total=total,
        page=page,
        page_size=page_size,
    )
    return ApiResponseVehiclePage(
        data=payload, request_id=trace_request_id()
    )


@router.get(
    "/{vehicle_id}",
    operation_id="getVehicle",
    response_model=ApiResponseVehicleDetail,
    summary="车辆详情（含 provisioning 步骤与接入摘要）",
    responses={**_ERRORS},
)
async def get_vehicle(
    vehicle_id: Annotated[str, Path(pattern=_VEHICLE_ID_PATTERN)],
    _user: Annotated[str, Depends(require_vehicle_read)],
    store: Annotated[VehicleStore, Depends(get_store)],
) -> ApiResponseVehicleDetail:
    row = await store.get(vehicle_id)
    if row is None:
        raise ResourceNotFoundError(
            details={"model": "Vehicle", "vehicle_id": vehicle_id}
        )
    return ApiResponseVehicleDetail(data=_to_detail(row), request_id=trace_request_id())


@router.patch(
    "/{vehicle_id}",
    operation_id="patchVehicle",
    response_model=ApiResponseVehicleDetail,
    summary="修改台账基础字段（不涉及 provisioning 资源）",
    responses={**_ERRORS},
)
async def patch_vehicle(
    vehicle_id: Annotated[str, Path(pattern=_VEHICLE_ID_PATTERN)],
    body: VehicleUpdateRequest,
    _user: Annotated[str, Depends(require_vehicle_update)],
    store: Annotated[VehicleStore, Depends(get_store)],
) -> ApiResponseVehicleDetail:
    updated = await store.patch_ledger(vehicle_id, **body.model_dump(exclude_unset=True))
    if updated is None:
        raise ResourceNotFoundError(
            details={"model": "Vehicle", "vehicle_id": vehicle_id}
        )
    return ApiResponseVehicleDetail(data=_to_detail(updated), request_id=trace_request_id())


# =====================================================================
# 一键开通 / 下线 / 轮换 / 重签 / Bundle
# =====================================================================
@router.post(
    "",
    operation_id="createVehicle",
    response_model=ApiResponseProvisionResult,
    summary="一键开通车辆（provisioning 向导）",
    responses={409: {"description": "3002 vehicle_id 已存在"}, **_ERRORS},
)
async def create_vehicle(
    body: VehicleCreateRequest,
    _user: Annotated[str, Depends(require_vehicle_create)],
    provisioner: Annotated[Provisioner, Depends(get_provisioner)],
) -> ApiResponseProvisionResult:
    outcome = await provisioner.provision(body)
    row = await provisioner._store.get(body.vehicle_id)  # noqa: SLF001 - 路由与 Provisioner 同层协作
    if row is None:  # 理论上不应发生（刚成功 provision 后行必存在）
        raise ServiceUnavailableError("provisioning 后台账不可读")
    detail = _to_detail(row)
    result = ProvisionResult(
        vehicle=detail,
        scram_password=outcome.scram_password or "",
        bundle_download_url=f"/api/v1/vehicle/{body.vehicle_id}/bundle",
    )
    return ApiResponseProvisionResult(data=result, request_id=trace_request_id())


@router.delete(
    "/{vehicle_id}",
    operation_id="deleteVehicle",
    response_model=ApiResponseEmpty,
    summary="下线车辆（回收 Kafka SCRAM/Topic 与证书目录）",
    responses={**_ERRORS},
)
async def delete_vehicle(
    vehicle_id: Annotated[str, Path(pattern=_VEHICLE_ID_PATTERN)],
    _user: Annotated[str, Depends(require_vehicle_delete)],
    provisioner: Annotated[Provisioner, Depends(get_provisioner)],
    store: Annotated[VehicleStore, Depends(get_store)],
    purge_topics: Annotated[bool, Query(description="是否删除 8 个车端 Topic")] = True,
) -> ApiResponseEmpty:
    if not await store.exists(vehicle_id):
        raise ResourceNotFoundError(
            details={"model": "Vehicle", "vehicle_id": vehicle_id}
        )
    await provisioner.deprovision(vehicle_id, purge_topics=purge_topics)
    return ApiResponseEmpty(data=None, request_id=trace_request_id())


@router.post(
    "/{vehicle_id}/rotate-scram",
    operation_id="rotateScram",
    response_model=ApiResponseRotateScramResult,
    summary="重置车端 SCRAM 口令（服务端生成新口令，一次性返回）",
    responses={**_ERRORS},
)
async def rotate_scram(
    vehicle_id: Annotated[str, Path(pattern=_VEHICLE_ID_PATTERN)],
    _user: Annotated[str, Depends(require_vehicle_execute)],
    provisioner: Annotated[Provisioner, Depends(get_provisioner)],
    store: Annotated[VehicleStore, Depends(get_store)],
) -> ApiResponseRotateScramResult:
    if not await store.exists(vehicle_id):
        raise ResourceNotFoundError(
            details={"model": "Vehicle", "vehicle_id": vehicle_id}
        )
    password = await provisioner.rotate_scram(vehicle_id)
    return ApiResponseRotateScramResult(
        data=RotateScramResult(vehicle_id=vehicle_id, scram_password=password),
        request_id=trace_request_id(),
    )


@router.post(
    "/{vehicle_id}/reissue-cert",
    operation_id="reissueCert",
    response_model=ApiResponseReissueCertResult,
    summary="重签客户端证书（覆盖 VEHICLE_CERTS_DIR/{id}/）",
    responses={**_ERRORS},
)
async def reissue_cert(
    vehicle_id: Annotated[str, Path(pattern=_VEHICLE_ID_PATTERN)],
    _user: Annotated[str, Depends(require_vehicle_execute)],
    provisioner: Annotated[Provisioner, Depends(get_provisioner)],
    store: Annotated[VehicleStore, Depends(get_store)],
) -> ApiResponseReissueCertResult:
    if not await store.exists(vehicle_id):
        raise ResourceNotFoundError(
            details={"model": "Vehicle", "vehicle_id": vehicle_id}
        )
    serial, issued_at = await provisioner.reissue_cert(vehicle_id)
    return ApiResponseReissueCertResult(
        data=ReissueCertResult(
            vehicle_id=vehicle_id, device_cert_sn=serial, issued_at=issued_at
        ),
        request_id=trace_request_id(),
    )


@router.get(
    "/{vehicle_id}/bundle",
    operation_id="downloadBundle",
    summary="下载车端接入包（ZIP：CA + 独立客户端证书 + kafka.properties + README）",
    responses={
        200: {
            "description": "ZIP 二进制",
            "content": {"application/zip": {"schema": {"type": "string", "format": "binary"}}},
            "headers": {
                "Content-Disposition": {
                    "schema": {"type": "string"},
                    "description": 'attachment; filename="<vehicle_id>-bundle.zip"',
                }
            },
        },
        **_ERRORS,
    },
)
async def download_bundle(
    vehicle_id: Annotated[str, Path(pattern=_VEHICLE_ID_PATTERN)],
    _user: Annotated[str, Depends(require_vehicle_execute)],
    store: Annotated[VehicleStore, Depends(get_store)],
) -> Response:
    if not await store.exists(vehicle_id):
        raise ResourceNotFoundError(
            details={"model": "Vehicle", "vehicle_id": vehicle_id}
        )
    payload: bytes = await bundle_mod.abuild_bundle_zip(vehicle_id)
    headers = {
        "Content-Disposition": f'attachment; filename="{vehicle_id}-bundle.zip"',
        "X-Request-ID": trace_request_id(),
    }
    return Response(content=payload, media_type="application/zip", headers=headers)


# =====================================================================
# 兜底：兼容 success_response 从 hunter_common.responses 引入（未在直接响应使用；保留供测试断言）
# =====================================================================
_ = success_response


def _collect_errors(exc: Exception) -> dict[str, Any]:  # pragma: no cover - 调试辅助
    return {"type": type(exc).__name__, "message": str(exc)}


__all__ = ["router"]
