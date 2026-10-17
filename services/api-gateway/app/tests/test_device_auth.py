"""车端 REST 设备认证测试（契约 x-hunter-device-ingress；V1.21.13 pending #6 决议）。

覆盖：证书链重验通过与各类拒绝（异 CA/过期/无 clientAuth/无 CN/URL 编码 PEM）、
路径白名单与通道开关语义（fail-closed）、转发侧设备身份头注入 + PEM 不扩散。
证书为运行期生成的 EC P-256 测试链（快且自包含，不依赖仓库证书材料）。
"""
from __future__ import annotations

import datetime
from typing import Any
from urllib.parse import quote

import httpx
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from fastapi import Request
from hunter_common.exceptions import AuthenticationError
from hunter_common.internal_auth import HEADER_DEVICE_VEHICLE_ID, verify_identity_headers

from app.config import settings
from app.core.device_auth import authenticate_device_request, reset_ca_cache
from app.services.proxy_service import ProxyService

PRESIGN_PATH = "/api/v1/data/files/presign"
NOW = datetime.datetime.now(datetime.UTC)


# ---------- 测试证书链构造 ----------


def _make_ca() -> tuple[x509.Certificate, ec.EllipticCurvePrivateKey]:
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "HunterCore Test CA")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(NOW - datetime.timedelta(days=1))
        .not_valid_after(NOW + datetime.timedelta(days=3650))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    return cert, key


def _make_device_cert(
    ca_cert: x509.Certificate,
    ca_key: ec.EllipticCurvePrivateKey,
    *,
    cn: str | None = "HUNTER-001",
    eku: x509.ExtendedKeyUsage | None = x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]),
    not_after: datetime.datetime | None = None,
    issuer: tuple[x509.Certificate, ec.EllipticCurvePrivateKey] | None = None,
) -> x509.Certificate:
    """CA 签发设备客户端证书（默认对齐 cert_signer 产物：CN=vehicle_id + clientAuth EKU；
    eku=None 表示不挂扩展（no_extension 用例）。"""
    _ca_cert, _ca_key = issuer or (ca_cert, ca_key)
    key = ec.generate_private_key(ec.SECP256R1())
    attrs = [x509.NameAttribute(NameOID.ORGANIZATION_NAME, "HunterCore")]
    if cn is not None:
        attrs.append(x509.NameAttribute(NameOID.COMMON_NAME, cn))
    builder = (
        x509.CertificateBuilder()
        .subject_name(x509.Name(attrs))
        .issuer_name(_ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(NOW - datetime.timedelta(days=1))
        .not_valid_after(not_after or NOW + datetime.timedelta(days=365))
    )
    if eku is not None:
        builder = builder.add_extension(eku, critical=False)
    return builder.sign(_ca_key, hashes.SHA256())


def _pem(cert: x509.Certificate) -> str:
    return cert.public_bytes(serialization.Encoding.PEM).decode()


def _request(path: str, headers: dict[str, str]) -> Request:
    scope = {
        "type": "http",
        "method": "POST",
        "path": path,
        "query_string": b"",
        "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()],
        "scheme": "http",
    }
    return Request(scope)


@pytest.fixture()
def ca(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> tuple[x509.Certificate, ec.EllipticCurvePrivateKey]:
    """写 CA 文件并启用设备通道（每测试独立路径，天然绕开 mtime 缓存串扰）。"""
    ca_cert, ca_key = _make_ca()
    ca_file = tmp_path / "ca-cert.pem"
    ca_file.write_text(_pem(ca_cert), encoding="utf-8")
    monkeypatch.setattr(settings, "device_ca_certfile", str(ca_file))
    monkeypatch.setattr(settings, "device_role", "device")
    reset_ca_cache()
    yield ca_cert, ca_key
    reset_ca_cache()


def _client_cert_header(cert: x509.Certificate) -> dict[str, str]:
    """nginx $ssl_client_escaped_cert 等价物（URL 编码 PEM）。"""
    return {"X-Client-Cert": quote(_pem(cert), safe="")}


# ---------- 重验通过 ----------


def test_valid_device_cert_yields_device_claims(ca: Any) -> None:
    ca_cert, ca_key = ca
    cert = _make_device_cert(ca_cert, ca_key, cn="HUNTER-042")
    claims = authenticate_device_request(_request(PRESIGN_PATH, _client_cert_header(cert)))
    assert claims == {
        "sub": "device:HUNTER-042",
        "roles": ["device"],
        "type": "device",
        "vehicle_id": "HUNTER-042",
    }


# ---------- 拒绝路径（一律 1001，不透出证书细节） ----------


def test_foreign_ca_signed_cert_rejected(ca: Any) -> None:
    ca_cert, ca_key = ca
    other_ca = _make_ca()  # 自签"第二 CA"：无平台 CA 私钥的攻击者所能做到的极限
    cert = _make_device_cert(ca_cert, ca_key, issuer=other_ca)
    with pytest.raises(AuthenticationError):
        authenticate_device_request(_request(PRESIGN_PATH, _client_cert_header(cert)))


def test_expired_cert_rejected(ca: Any) -> None:
    ca_cert, ca_key = ca
    cert = _make_device_cert(
        ca_cert, ca_key, not_after=NOW - datetime.timedelta(seconds=60)
    )
    with pytest.raises(AuthenticationError):
        authenticate_device_request(_request(PRESIGN_PATH, _client_cert_header(cert)))


def test_missing_client_auth_eku_rejected(ca: Any) -> None:
    ca_cert, ca_key = ca
    server_only = x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH])
    cert = _make_device_cert(ca_cert, ca_key, eku=server_only)
    with pytest.raises(AuthenticationError):
        authenticate_device_request(_request(PRESIGN_PATH, _client_cert_header(cert)))


def test_no_extension_eku_rejected(ca: Any) -> None:
    ca_cert, ca_key = ca
    cert = _make_device_cert(ca_cert, ca_key, eku=None)
    with pytest.raises(AuthenticationError):
        authenticate_device_request(_request(PRESIGN_PATH, _client_cert_header(cert)))


def test_empty_common_name_rejected(ca: Any) -> None:
    ca_cert, ca_key = ca
    cert = _make_device_cert(ca_cert, ca_key, cn=None)
    with pytest.raises(AuthenticationError):
        authenticate_device_request(_request(PRESIGN_PATH, _client_cert_header(cert)))


def test_unparsable_cert_header_rejected(ca: Any) -> None:
    with pytest.raises(AuthenticationError):
        authenticate_device_request(_request(PRESIGN_PATH, {"X-Client-Cert": "not-a-pem"}))


def test_device_channel_disabled_rejects_cert_bearing_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """fail-closed：反代已放行但网关未配 CA（旧镜像/漏配）→ 显式 1001，不静默回落 JWT。"""
    monkeypatch.setattr(settings, "device_ca_certfile", None)
    with pytest.raises(AuthenticationError):
        authenticate_device_request(_request(PRESIGN_PATH, {"X-Client-Cert": "%2D%2D%2D"}))


# ---------- 白名单与回落语义 ----------


def test_non_device_path_with_cert_header_falls_back_to_jwt(ca: Any) -> None:
    """契约 device_paths 外路径携带证书头 → 返回 None（回落控制台 JWT 路线，不视为设备）。"""
    ca_cert, ca_key = ca
    cert = _make_device_cert(ca_cert, ca_key)
    assert authenticate_device_request(
        _request("/api/v1/data/telemetry", _client_cert_header(cert))
    ) is None


def test_device_path_without_cert_header_is_console_route(ca: Any) -> None:
    assert authenticate_device_request(_request(PRESIGN_PATH, {})) is None


# ---------- 转发头注入（proxy_service 单元） ----------


def _forward_headers(claims: dict[str, Any], incoming: dict[str, str], monkeypatch: pytest.MonkeyPatch, secret: str | None) -> dict[str, str]:
    monkeypatch.setattr(settings, "gateway_hmac_secret", secret)
    svc = ProxyService(httpx.AsyncClient(), settings)
    request = _request(PRESIGN_PATH, incoming)
    return svc._forward_headers(request, claims)


def test_device_claims_inject_signed_device_header(monkeypatch: pytest.MonkeyPatch) -> None:
    secret = "test-secret-32-bytes-long-xxxxxxxxx"
    claims = {"sub": "device:HUNTER-001", "roles": ["device"], "type": "device", "vehicle_id": "HUNTER-001"}
    headers = _forward_headers(claims, {"X-Client-Cert": "PEM-SHOULD-NOT-LEAK", "X-Device-Vehicle-Id": "forged"}, monkeypatch, secret)
    assert headers["X-User-Id"] == "device:HUNTER-001"
    assert headers["X-Roles"] == "device"
    assert headers[HEADER_DEVICE_VEHICLE_ID] == "HUNTER-001"  # 伪造值必须被覆盖
    assert "x-client-cert" not in {k.lower() for k in headers}  # PEM 不向后端扩散
    # 设备头纳入 HMAC 第 5 段：验签方以完整头集合为准
    assert verify_identity_headers(secret, {k.lower(): v for k, v in headers.items()})


def test_console_claims_signature_stays_four_segment(monkeypatch: pytest.MonkeyPatch) -> None:
    """回归护栏：控制台通道（无设备头）签名与历史 4 段格式逐字节兼容。"""
    secret = "test-secret-32-bytes-long-xxxxxxxxx"
    claims = {"sub": "u-1", "roles": ["operator"]}
    headers = _forward_headers(claims, {}, monkeypatch, secret)
    assert HEADER_DEVICE_VEHICLE_ID not in headers
    assert verify_identity_headers(secret, {k.lower(): v for k, v in headers.items()})
    # 若第 5 段被错误追加，恒定时间比较将失配
    forged = dict(headers)
    forged[HEADER_DEVICE_VEHICLE_ID] = "HUNTER-999"
    assert not verify_identity_headers(secret, {k.lower(): v for k, v in forged.items()})


def test_device_header_injected_even_without_hmac_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    """dev/test 未配 GATEWAY_HMAC_SECRET：明文注入语义与历史一致（设备头同样注入）。"""
    claims = {"sub": "device:HUNTER-001", "roles": ["device"], "type": "device", "vehicle_id": "HUNTER-001"}
    headers = _forward_headers(claims, {}, monkeypatch, None)
    assert headers[HEADER_DEVICE_VEHICLE_ID] == "HUNTER-001"
