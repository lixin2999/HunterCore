"""hunter_common.internal_auth 单元测试（G-02 身份头 HMAC 签名加固）。"""
from __future__ import annotations

import time

import pytest

from hunter_common.internal_auth import (
    HEADER_MAC,
    HEADER_ROLES,
    HEADER_TIMESTAMP,
    HEADER_TRACE_ID,
    HEADER_USER_ID,
    build_identity_headers,
    sign_identity,
    verify_identity_headers,
)

SECRET = "test-secret-32-bytes-long-xxxxxxxxx"


def test_sign_identity_is_deterministic_and_ordered() -> None:
    mac1 = sign_identity(SECRET, "u1", "admin", "t1", "1000")
    mac2 = sign_identity(SECRET, "u1", "admin", "t1", "1000")
    assert mac1 == mac2
    # 字段顺序参与签名：roles 与 trace 交换 → 摘要不同（防歧义拼接攻击）
    assert mac1 != sign_identity(SECRET, "u1", "t1", "admin", "1000")


def test_build_and_verify_roundtrip() -> None:
    headers = build_identity_headers(SECRET, user_id="u1", roles="admin,operator", trace_id="tr-1")
    assert headers[HEADER_USER_ID] == "u1"
    assert verify_identity_headers(SECRET, headers)


def test_verify_rejects_forged_identity() -> None:
    headers = build_identity_headers(SECRET, user_id="u1", roles="viewer", trace_id="tr-1")
    headers[HEADER_ROLES] = "admin"  # 集群内提权伪造
    assert not verify_identity_headers(SECRET, headers)


def test_verify_rejects_wrong_secret_and_missing_mac() -> None:
    headers = build_identity_headers(SECRET, user_id="u1", roles="viewer", trace_id="tr-1")
    assert not verify_identity_headers("another-secret-value-zzzzzzzzzzzz", headers)
    del headers[HEADER_MAC]
    assert not verify_identity_headers(SECRET, headers)


def test_verify_rejects_stale_timestamp() -> None:
    stale_ts = str(int(time.time()) - 400)
    from hunter_common.internal_auth import HEADER_TIMESTAMP as TS

    headers = {
        HEADER_USER_ID: "u1",
        HEADER_ROLES: "viewer",
        HEADER_TRACE_ID: "tr-1",
        TS: stale_ts,
        HEADER_MAC: sign_identity(SECRET, "u1", "viewer", "tr-1", stale_ts),
    }
    assert not verify_identity_headers(SECRET, headers, max_age_seconds=300)
    # 放宽窗口后同一签名有效（时间戳新鲜度是唯一判据）
    assert verify_identity_headers(SECRET, headers, max_age_seconds=600)


def test_verify_rejects_invalid_timestamp_format() -> None:
    headers = build_identity_headers(SECRET, user_id="u1", roles="viewer", trace_id="tr-1")
    headers[HEADER_TIMESTAMP] = "not-a-number"
    headers[HEADER_MAC] = sign_identity(SECRET, "u1", "viewer", "tr-1", "not-a-number")
    assert not verify_identity_headers(SECRET, headers)


@pytest.mark.parametrize("empty_field", [HEADER_USER_ID, HEADER_TIMESTAMP, HEADER_MAC])
def test_verify_rejects_missing_required_headers(empty_field: str) -> None:
    headers = build_identity_headers(SECRET, user_id="u1", roles="viewer", trace_id="tr-1")
    headers[empty_field] = None
    assert not verify_identity_headers(SECRET, headers)
