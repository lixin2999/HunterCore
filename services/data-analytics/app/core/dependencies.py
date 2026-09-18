"""认证鉴权与提供者依赖（FastAPI Depends 注入；提供者惰性构建并缓存于 app.state）。

身份来源：api-gateway 转发头 X-User-Id / X-Roles / X-Permissions / X-Trace-Id
（api-gateway 契约 §Security；直连流量缺失 X-User-Id → 401/1001）。
"""
from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Annotated, Any, TypeVar

from fastapi import Depends, Request
from hunter_common.exceptions import (
    AuthenticationError,
    PermissionDeniedError,
    ServiceUnavailableError,
)
from hunter_common.logging import get_trace_id

from app.config import Settings, settings
from app.repositories.eval_documents import EvalDocumentRepository
from app.repositories.events import DataCollectorEventClient
from app.repositories.fleet import RedisFleetRepository
from app.repositories.pipeline import PipelineRepository
from app.repositories.reports import ReportRepository
from app.repositories.storage import ObjectStorage, S3ObjectStorage
from app.repositories.vehicles import VehicleDirectoryClient
from app.services.corner_cases import CornerCaseService
from app.services.coverage import CoverageService
from app.services.dashboard import DashboardService
from app.services.evaluation import EvaluationService
from app.services.reports import ReportService

T = TypeVar("T")

#: app.state 上缓存实例的键（测试可通过同名键注入替身）
KEY_STORAGE = "analytics_storage"
KEY_REPORT_REPOSITORY = "report_repository"
KEY_EVAL_DOCUMENT_REPOSITORY = "eval_document_repository"
KEY_FLEET_REPOSITORY = "fleet_repository"
KEY_PIPELINE_REPOSITORY = "pipeline_repository"
KEY_EVENT_CLIENT = "event_client"
KEY_VEHICLE_CLIENT = "vehicle_client"
KEY_REPORT_SERVICE = "report_service"
KEY_DASHBOARD_SERVICE = "dashboard_service"
KEY_EVALUATION_SERVICE = "evaluation_service"
KEY_COVERAGE_SERVICE = "coverage_service"
KEY_CORNER_CASE_SERVICE = "corner_case_service"
KEY_CLOSABLES = "analytics_closables"


def get_settings() -> Settings:
    """全局配置单例。"""
    return settings


def current_user_id(request: Request) -> str:
    """X-User-Id 头（网关注入）；缺失视为未认证（401/1001）。"""
    user_id = request.headers.get("X-User-Id")
    if not user_id:
        raise AuthenticationError()
    return user_id


def current_roles(request: Request) -> frozenset[str]:
    """X-Roles 头（逗号分隔角色列表，网关注入）。"""
    raw = request.headers.get("X-Roles", "")
    return frozenset(role.strip() for role in raw.split(",") if role.strip())


def require_read_permission(
    user_id: Annotated[str, Depends(current_user_id)],
    roles: Annotated[frozenset[str], Depends(current_roles)],
    config: Annotated[Settings, Depends(get_settings)],
) -> str:
    """analytics:read 复核（查询类端点）。"""
    if not roles & config.analytics_read_role_set:
        raise PermissionDeniedError()
    return user_id


def require_execute_permission(
    user_id: Annotated[str, Depends(current_user_id)],
    roles: Annotated[frozenset[str], Depends(current_roles)],
    config: Annotated[Settings, Depends(get_settings)],
) -> str:
    """analytics:execute 复核（报告生成等重操作）。"""
    if not roles & config.analytics_execute_role_set:
        raise PermissionDeniedError()
    return user_id


def trace_request_id() -> str:
    """响应体 request_id（与 X-Request-ID 对齐；trace 缺失时生成 UUID）。"""
    return get_trace_id() or str(uuid.uuid4())


def _cached(request: Request, key: str, factory: Callable[[], T]) -> T:
    """app.state 惰性缓存（先查缓存，未命中构建并回填）。"""
    instance: T | None = getattr(request.app.state, key, None)
    if instance is None:
        instance = factory()
        setattr(request.app.state, key, instance)
    return instance


def _track_closable(request: Request, closable: Any) -> None:
    """登记需在 lifespan 关闭时释放的资源（httpx 客户端等）。"""
    closables: list[Any] = getattr(request.app.state, KEY_CLOSABLES, None)
    if closables is None:
        closables = []
        setattr(request.app.state, KEY_CLOSABLES, closables)
    closables.append(closable)


def get_storage(request: Request) -> ObjectStorage:
    """MinIO 对象存储（报告 sidecar + 评估文档，bucket=hunter-reports）。"""

    def factory() -> S3ObjectStorage:
        return S3ObjectStorage(settings, settings.minio_bucket_reports)

    return _cached(request, KEY_STORAGE, factory)


def get_report_repository(request: Request) -> ReportRepository:
    """报告 sidecar 仓储。"""

    def factory() -> ReportRepository:
        return ReportRepository(get_storage(request), prefix=settings.report_sidecar_prefix)

    return _cached(request, KEY_REPORT_REPOSITORY, factory)


def get_eval_document_repository(request: Request) -> EvalDocumentRepository:
    """评估文档仓储。"""

    def factory() -> EvalDocumentRepository:
        return EvalDocumentRepository(get_storage(request), pointer_key=settings.eval_document_pointer_key)

    return _cached(request, KEY_EVAL_DOCUMENT_REPOSITORY, factory)


def get_fleet_repository(request: Request) -> RedisFleetRepository:
    """Redis 车队状态仓储（复用 lifespan 初始化的连接池）。"""

    def factory() -> RedisFleetRepository:
        redis_manager = getattr(request.app.state, "redis", None)
        if redis_manager is None:
            raise ServiceUnavailableError("Redis 未初始化")
        return RedisFleetRepository(redis_manager.client)

    return _cached(request, KEY_FLEET_REPOSITORY, factory)


def get_pipeline_repository(request: Request) -> PipelineRepository:
    """管道健康仓储（只读 DB + Kafka 探测）。"""

    def factory() -> PipelineRepository:
        repository = PipelineRepository(settings)
        # asyncpg 连接池惰性创建，登记到关闭清单（lifespan 统一释放）
        _track_closable(request, repository)
        return repository

    return _cached(request, KEY_PIPELINE_REPOSITORY, factory)


def get_event_client(request: Request) -> DataCollectorEventClient:
    """data-collector 事件统计客户端。"""

    def factory() -> DataCollectorEventClient:
        client = DataCollectorEventClient(
            settings.data_collector_base_url,
            timeout=settings.dependency_timeout_seconds,
            max_retries=settings.dependency_max_retries,
        )
        _track_closable(request, client)
        return client

    return _cached(request, KEY_EVENT_CLIENT, factory)


def get_vehicle_client(request: Request) -> VehicleDirectoryClient:
    """vehicle-service 车辆目录客户端。"""

    def factory() -> VehicleDirectoryClient:
        client = VehicleDirectoryClient(
            settings.vehicle_service_base_url,
            timeout=settings.dependency_timeout_seconds,
            max_retries=settings.dependency_max_retries,
        )
        _track_closable(request, client)
        return client

    return _cached(request, KEY_VEHICLE_CLIENT, factory)


def get_report_service(request: Request) -> ReportService:
    """报告服务。"""

    def factory() -> ReportService:
        return ReportService(get_report_repository(request), get_vehicle_client(request), settings)

    return _cached(request, KEY_REPORT_SERVICE, factory)


def get_dashboard_service(request: Request) -> DashboardService:
    """看板聚合服务。"""

    def factory() -> DashboardService:
        return DashboardService(
            get_fleet_repository(request),
            get_pipeline_repository(request),
            get_event_client(request),
            get_vehicle_client(request),
            settings,
        )

    return _cached(request, KEY_DASHBOARD_SERVICE, factory)


def get_evaluation_service(request: Request) -> EvaluationService:
    """算法评估服务。"""

    def factory() -> EvaluationService:
        return EvaluationService(get_eval_document_repository(request), settings)

    return _cached(request, KEY_EVALUATION_SERVICE, factory)


def get_coverage_service(request: Request) -> CoverageService:
    """场景覆盖率服务。"""

    def factory() -> CoverageService:
        return CoverageService(get_eval_document_repository(request), settings)

    return _cached(request, KEY_COVERAGE_SERVICE, factory)


def get_corner_case_service(request: Request) -> CornerCaseService:
    """Corner Case 检索服务。"""

    def factory() -> CornerCaseService:
        return CornerCaseService(get_eval_document_repository(request), settings)

    return _cached(request, KEY_CORNER_CASE_SERVICE, factory)