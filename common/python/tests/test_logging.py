"""hunter_common.logging 单元测试：JSON 输出 / trace_id 注入 / 敏感字段脱敏。"""
from __future__ import annotations

import json

import pytest

from hunter_common.logging import (
    configure_logging,
    get_logger,
    reset_trace_id,
    set_trace_id,
)


def _parse_last_json_line(capsys: pytest.CaptureFixture[str]) -> dict[str, object]:
    out = capsys.readouterr().out
    lines = [line for line in out.strip().splitlines() if line.strip()]
    return json.loads(lines[-1])


def test_json_log_contains_service_and_trace_id(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("unit-test", "INFO", json_output=True)
    token = set_trace_id("trace-123")
    try:
        get_logger("test").info("hello", user_id="u1")
    finally:
        reset_trace_id(token)
    event = _parse_last_json_line(capsys)
    assert event["message"] == "hello"
    assert event["service"] == "unit-test"
    assert event["trace_id"] == "trace-123"
    assert event["level"] == "info"


def test_sensitive_fields_masked(capsys: pytest.CaptureFixture[str]) -> None:
    """日志脱敏：password/token/secret 不允许明文输出。"""
    configure_logging("unit-test", "INFO", json_output=True)
    get_logger("test").info(
        "login", password="p@ss", access_token="tk", secret_key="sk", username="alice"
    )
    event = _parse_last_json_line(capsys)
    assert event["password"] == "***MASKED***"
    assert event["access_token"] == "***MASKED***"
    assert event["secret_key"] == "***MASKED***"
    assert event["username"] == "alice"  # 非敏感字段不受影响


def test_vehicle_id_context_injected(capsys: pytest.CaptureFixture[str]) -> None:
    from hunter_common.logging import reset_vehicle_id, set_vehicle_id

    configure_logging("unit-test", "INFO", json_output=True)
    token = set_vehicle_id("HUNTER-001")
    try:
        get_logger("test").info("telemetry_report")
    finally:
        reset_vehicle_id(token)
    event = _parse_last_json_line(capsys)
    assert event["vehicle_id"] == "HUNTER-001"
