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


def test_nested_sensitive_fields_masked(capsys: pytest.CaptureFixture[str]) -> None:
    """审查 Y1 回归：嵌套 dict / list 内的敏感字段必须同样掩码。

    修复前仅掩码顶层 key，``payload={"password": ...}``、``items=[{"token": ...}]``
    会明文落盘（结构化日志的实际泄露路径）。
    """
    configure_logging("unit-test", "INFO", json_output=True)
    get_logger("test").info(
        "probe",
        payload={"password": "P@ssw0rd", "nested": [{"token": "abc"}, {"ok": "v"}]},
        vehicle_id="HUNTER-001",
    )
    event = _parse_last_json_line(capsys)
    assert event["payload"]["password"] == "***MASKED***"
    assert event["payload"]["nested"][0]["token"] == "***MASKED***"
    assert event["payload"]["nested"][1]["ok"] == "v"      # 非敏感字段保留
    assert event["vehicle_id"] == "HUNTER-001"             # 非敏感顶层字段保留


def test_sensitive_string_values_masked(capsys: pytest.CaptureFixture[str]) -> None:
    """字段名正常但值是凭据：Bearer Token / PEM 私钥块必须掩码。"""
    configure_logging("unit-test", "INFO", json_output=True)
    get_logger("test").info(
        "upstream_call",
        request_header="Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.payload.signature",
        key_material="-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkq\n-----END PRIVATE KEY-----",
        note="plain text",
    )
    event = _parse_last_json_line(capsys)
    assert event["request_header"] == "***MASKED***"
    assert event["key_material"] == "***MASKED***"
    assert event["note"] == "plain text"


def test_deeply_nested_structures_are_truncated_not_leaked(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """深度超限的嵌套结构一律掩码（防自引用结构拖垮日志路径）。"""
    configure_logging("unit-test", "INFO", json_output=True)
    payload: dict[str, object] = {"password": "deep-secret"}
    for _ in range(10):
        payload = {"level": payload}
    get_logger("test").info("deep", payload=payload)

    event = _parse_last_json_line(capsys)
    serialized = json.dumps(event["payload"], ensure_ascii=False)
    assert "deep-secret" not in serialized
