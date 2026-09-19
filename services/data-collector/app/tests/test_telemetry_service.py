"""TelemetryService 单元测试（审查 R6：补齐业务逻辑行为测试）。

覆盖：时间区间校验（2001）、跨度超限截断（服务端行为，路由层返回 206）、
扁平行 → 六段消息结构映射（未采集段为 null）、分页参数透传与 retention_days 契约值。
"""
from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
from hunter_common.exceptions import InvalidParameterError

from app.repositories.telemetry import telemetry_row_to_dict
from app.schemas.common import TELEMETRY_RETENTION_DAYS
from app.services.telemetry import TelemetryService
from app.tests.fakes import FakeTelemetryRepository

VEHICLE_ID = "HUNTER-001"
BASE_TS = 1724035200.0
MAX_RANGE_SECONDS = 24 * 3600


def make_row(**overrides: Any) -> SimpleNamespace:
    """构造 vehicle_telemetry 扁平行（默认全 null，仅覆盖所需列）。"""
    fields: dict[str, Any] = {
        "time": datetime.fromtimestamp(BASE_TS, tz=UTC),
        "vehicle_id": VEHICLE_ID,
        "seq": None,
    }
    for name in [
        "velocity",
        "steering_angle",
        "battery_voltage",
        "battery_soc",
        "battery_current",
        "battery_temp",
        "control_mode",
        "vehicle_state",
        "fault_code",
        "motor_rpm",
        "motor_current",
        "motor_temp",
        "x",
        "y",
        "z",
        "roll",
        "pitch",
        "heading",
        "linear_velocity",
        "angular_velocity",
        "position_std",
        "heading_std",
        "detected_objects",
        "fps",
        "latency_ms",
        "object_types",
        "trajectory_length",
        "trajectory_points",
        "planning_latency_ms",
        "current_behavior",
        "target_velocity",
        "target_steer",
        "velocity_error",
        "steer_error",
        "control_latency_ms",
        "cpu_usage",
        "gpu_usage",
        "memory_usage_mb",
        "gpu_temp",
        "cpu_temp",
        "network_rssi",
        "network_latency_ms",
    ]:
        fields[name] = None
    fields.update(overrides)
    return SimpleNamespace(**fields)


def make_service(rows: list[Any] | None = None, total: int = 0) -> tuple[Any, FakeTelemetryRepository]:
    repository = FakeTelemetryRepository(rows, total)
    return TelemetryService(repository), repository


# ---------------------------------------------------------------------------
# 区间校验与截断（x-hunter-telemetry-query-contract）
# ---------------------------------------------------------------------------
async def test_query_rejects_reversed_range() -> None:
    """end_time ≤ start_time → 2001（参数错误）。"""
    service, repository = make_service()
    with pytest.raises(InvalidParameterError) as exc:
        await service.query_telemetry(VEHICLE_ID, start_time=BASE_TS, end_time=BASE_TS - 1)
    assert exc.value.code == 2001
    assert repository.calls == []  # 参数非法不触达仓储


async def test_query_accepts_range_within_limit() -> None:
    """跨度未超限：不截断（truncated_seconds=0），仓储收到原始区间。"""
    service, repository = make_service()
    end = BASE_TS + MAX_RANGE_SECONDS
    _, truncated = await service.query_telemetry(VEHICLE_ID, start_time=BASE_TS, end_time=end)
    assert truncated == 0
    assert repository.calls[0]["end_time"] == end


async def test_query_truncates_over_long_range_to_protect_p95() -> None:
    """跨度超上限 → 截断 end_time（206 语义），并回传被截去秒数。"""
    service, repository = make_service()
    requested_end = BASE_TS + MAX_RANGE_SECONDS + 600
    data, truncated = await service.query_telemetry(
        VEHICLE_ID, start_time=BASE_TS, end_time=requested_end
    )
    assert truncated == 600
    assert repository.calls[0]["end_time"] == BASE_TS + MAX_RANGE_SECONDS
    assert data.page == 1
    assert data.retention_days == TELEMETRY_RETENTION_DAYS


# ---------------------------------------------------------------------------
# 行 → 六段结构映射
# ---------------------------------------------------------------------------
def test_row_mapping_builds_all_segments_and_drops_null_fields() -> None:
    """六段结构映射：已采集字段入段，段内 null 字段剔除（契约：未采集为 null）。"""
    mapped = telemetry_row_to_dict(
        make_row(
            seq=12580,
            velocity=1.52,
            battery_soc=78,
            x=125.34,
            heading=1.234,
            detected_objects=12,
            object_types={"vehicle": 5, "pedestrian": 3},
            planning_latency_ms=45.0,
            target_velocity=1.6,
            cpu_usage=45.2,
        )
    )
    assert mapped["vehicle_id"] == VEHICLE_ID
    assert mapped["seq"] == 12580
    assert mapped["time"] == BASE_TS
    assert mapped["chassis"] == {"velocity": 1.52, "battery_soc": 78}
    assert mapped["localization"] == {"x": 125.34, "heading": 1.234}
    assert mapped["perception"] == {
        "detected_objects": 12,
        "object_types": {"vehicle": 5, "pedestrian": 3},
    }
    assert mapped["planning"] == {"planning_latency_ms": 45.0}
    assert mapped["control"] == {"target_velocity": 1.6}
    assert mapped["system"] == {"cpu_usage": 45.2}


def test_row_mapping_returns_null_for_absent_segments() -> None:
    """整段未采集 → 该段为 null（客户端可区分「未采集」与「值为 0」）。"""
    mapped = telemetry_row_to_dict(make_row(velocity=0.0))
    assert mapped["chassis"] == {"velocity": 0.0}
    assert mapped["localization"] is None
    assert mapped["perception"] is None
    assert mapped["planning"] is None
    assert mapped["control"] is None
    assert mapped["system"] is None


async def test_query_maps_rows_into_contract_samples() -> None:
    """查询结果按契约 TelemetrySample 组装（含分页与总量）。"""
    row = make_row(seq=1, velocity=1.5, cpu_usage=30.0)
    service, _ = make_service(rows=[row], total=1)
    data, truncated = await service.query_telemetry(
        VEHICLE_ID, start_time=BASE_TS, end_time=BASE_TS + 60, page=2, page_size=50
    )
    assert truncated == 0
    assert data.total == 1 and data.page == 2 and data.page_size == 50
    assert len(data.items) == 1
    sample = data.items[0]
    assert sample.vehicle_id == VEHICLE_ID
    assert sample.chassis is not None and sample.chassis.velocity == 1.5
    assert sample.system is not None and sample.system.cpu_usage == 30.0
    assert sample.localization is None
