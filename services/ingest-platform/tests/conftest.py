"""ingest-platform 测试公共 fixture。"""
from __future__ import annotations

import os
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

# 在导入 app.config 前设置环境变量（pydantic-settings 会读取这些）
os.environ.update({
    "ENVIRONMENT": "test",
    "LOG_LEVEL": "WARNING",
    "DEBUG": "true",
    "DATABASE_URL": "postgresql+asyncpg://test:test@localhost/test",
    "REDIS_URL": "redis://localhost:6379/0",
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
    "MINIO_ENDPOINT_URL": "http://localhost:9000",
    "MINIO_ACCESS_KEY": "minioadmin",
    "MINIO_SECRET_KEY": "minioadmin",
    "GATEWAY_HMAC_SECRET": "test-secret",
})

from app.config import Settings  # noqa: E402


@pytest.fixture(scope="session")
def test_settings() -> Settings:
    """测试用配置（覆盖环境变量）。"""
    return Settings(
        environment="test",
        debug=True,
        log_level="WARNING",
        database_url="postgresql+asyncpg://test:test@localhost/testdb",
        redis_url="redis://localhost:6379/1",
        kafka_bootstrap_servers="localhost:9092",
        minio_endpoint_url="http://localhost:9000",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin",
        gateway_hmac_secret="test-hmac-secret",
    )


@pytest.fixture
def mock_session() -> AsyncMock:
    """模拟 AsyncSession（用于仓储单元测试）。"""
    session = AsyncMock(spec=AsyncSession)
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.execute = AsyncMock()
    session.flush = AsyncMock()
    return session


@pytest.fixture
def mock_db_mgr(mock_session: AsyncMock) -> MagicMock:
    """模拟 DatabaseSessionManager（返回 mock_session 作为 session CM）。"""
    from contextlib import asynccontextmanager

    mgr = MagicMock()

    @asynccontextmanager
    async def _session_cm() -> AsyncIterator[AsyncMock]:
        yield mock_session

    mgr.session = _session_cm
    mgr.session_factory = MagicMock(return_value=_session_cm())
    mgr.check_connection = AsyncMock(return_value=True)
    return mgr


# ========== 事件 fixture（§附录A）==========

def make_manifest_event(
    bag_id: str = "bag-001",
    vehicle_id: str = "veh-A1",
    seq: int = 1,
    sha256: str = "abc123def456" * 4,
    topics: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """构造 bag.manifest.v1 事件（§附录A）。"""
    if topics is None:
        topics = [
            {"topic": "/camera_front/image_raw", "type": "sensor_msgs/msg/Image", "count": 300},
            {"topic": "/lidar_top/points_raw", "type": "sensor_msgs/msg/PointCloud2", "count": 30},
            {"topic": "/imu/data", "type": "sensor_msgs/msg/Imu", "count": 3000},
        ]
    return {
        "event_id": f"evt-{bag_id}-manifest-{seq}",
        "event_type": "bag.manifest.v1",
        "bag_id": bag_id,
        "vehicle_id": vehicle_id,
        "seq": seq,
        "occurred_at": "2026-01-01T00:00:00Z",
        "origin": "vehicle",
        "payload": {
            "sha256": sha256,
            "ros_version": "2",
            "start_ns": 1_700_000_000_000_000_000,
            "end_ns": 1_700_000_010_000_000_000,
            "duration_s": 10.0,
            "size_bytes": 1_048_576_000,
            "message_count": 3330,
            "topics": topics,
            "tf_static": {"transforms": [{"parent_frame_id": "base_link", "child_frame_id": "camera_front"}]},
        },
    }


def make_artifact_event(
    bag_id: str = "bag-001",
    object_key: str = "bags/v1/veh-A1/2026-01-01/bag-001/manifest.json",
    artifact_class: str = "manifest",
) -> dict[str, Any]:
    """构造 bag.artifact.uploaded.v1 事件。"""
    return {
        "event_id": f"evt-{bag_id}-artifact-{object_key[-10:]}",
        "event_type": "bag.artifact.uploaded.v1",
        "bag_id": bag_id,
        "vehicle_id": "veh-A1",
        "seq": 2,
        "occurred_at": "2026-01-01T00:01:00Z",
        "origin": "vehicle",
        "payload": {
            "object_key": object_key,
            "artifact_class": artifact_class,
            "size_bytes": 1024,
            "sha256": "deadbeef" * 8,
            "etag": "abc123",
            "rows": None,
            "count": 1,
        },
    }
