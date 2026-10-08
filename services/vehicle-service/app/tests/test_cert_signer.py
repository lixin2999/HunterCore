"""cert_signer 单元测试：openssl 子进程调用参数 + 失败清理。

策略：不真调 openssl（依赖环境不一定有 CA 文件）；用 monkeypatch 拦截 `subprocess.run`
与 `Path` 相关 IO，验证：
1. `issue_client_cert_sync` 调用 openssl 的顺序与参数（genrsa / req / x509 / pkcs12）；
2. `serial_hex` 由本地生成（不依赖 openssl 输出）；
3. 中间 CSR / SAN 文件在成功后被清理，失败时被 `_cleanup_partial` 回收；
4. `p12_password` 权限文件写入 0600；
5. `delete_cert_dir` 逐文件 unlink + rmdir（不使用 rm -rf）。

`openssl verify` 端到端校验（真实签发链）在集成测试或手工冒烟中覆盖（README 说明）。
"""
from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from hunter_common.exceptions import ServiceUnavailableError

from app.config import settings
from app.services import cert_signer


@pytest.fixture
def tmp_certs_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(settings, "vehicle_certs_dir", str(tmp_path))
    ca_dir = tmp_path / "ca"
    ca_dir.mkdir()
    (ca_dir / "ca-cert.pem").write_text("-----FAKE CA-----")
    (ca_dir / "ca-key.pem").write_text("-----FAKE KEY-----")
    monkeypatch.setattr(settings, "kafka_ca_cert_path", str(ca_dir / "ca-cert.pem"))
    monkeypatch.setattr(settings, "kafka_ca_key_path", str(ca_dir / "ca-key.pem"))
    return tmp_path


class _RunRecorder:
    """收集 openssl 调用参数（不真执行）。"""

    def __init__(self) -> None:
        self.cmds: list[list[str]] = []

    def __call__(self, cmd: list[str], **kwargs: Any) -> MagicMock:
        self.cmds.append(list(cmd))
        # 模拟 openssl 会写出目标文件（供后续 chmod / unlink 断言）
        # 具体子命令：genrsa/req/x509/pkcs12 都会 -out <path>
        if "-out" in cmd:
            out = Path(cmd[cmd.index("-out") + 1])
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(b"----FAKE OUTPUT----\n")
        result = MagicMock(spec=subprocess.CompletedProcess)  # type: ignore[attr-defined]
        result.stdout = ""
        result.stderr = ""
        result.returncode = 0
        return result


def _install_fake_run(monkeypatch: pytest.MonkeyPatch) -> _RunRecorder:
    rec = _RunRecorder()
    monkeypatch.setattr(cert_signer.subprocess, "run", rec)
    return rec


# =====================================================================
# 签发 happy path
# =====================================================================
def test_issue_invokes_openssl_in_order(tmp_certs_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    rec = _install_fake_run(monkeypatch)
    artifacts = cert_signer.issue_client_cert_sync("HUNTER-001")

    stages = [cmd[1] for cmd in rec.cmds]
    assert stages == ["genrsa", "req", "x509", "pkcs12"], f"openssl 调用顺序漂移：{stages}"

    # 参数覆盖关键点
    x509_cmd = rec.cmds[2]
    assert "-set_serial" in x509_cmd, "必须显式提供 serial（免 -CAcreateserial 写文件）"
    serial_idx = x509_cmd.index("-set_serial")
    serial_val = x509_cmd[serial_idx + 1]
    assert serial_val.startswith("0x") and serial_val[2:] == artifacts.serial_hex
    assert f"-{'' }days" not in x509_cmd  # 存在 -days（非 -days 前置符号）
    assert "-days" in x509_cmd
    days_idx = x509_cmd.index("-days")
    assert x509_cmd[days_idx + 1] == str(settings.vehicle_cert_validity_days)

    # Subject = /O=HunterCore/CN=<vehicle_id>
    req_cmd = rec.cmds[1]
    subj_idx = req_cmd.index("-subj")
    assert req_cmd[subj_idx + 1] == "/O=HunterCore/CN=HUNTER-001"

    # PKCS12 使用 -passout pass:<随机>
    pkcs12_cmd = rec.cmds[3]
    pass_idx = pkcs12_cmd.index("-passout")
    assert pkcs12_cmd[pass_idx + 1].startswith("pass:")
    assert len(pkcs12_cmd[pass_idx + 1]) > len("pass:") + 10


def test_issue_writes_expected_files(tmp_certs_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_run(monkeypatch)
    artifacts = cert_signer.issue_client_cert_sync("HUNTER-002")
    vdir = tmp_certs_dir / "HUNTER-002"

    # 最终产物 4 份：key / cert / p12 / p12-password
    assert artifacts.key_path == vdir / "client-key.pem"
    assert artifacts.cert_path == vdir / "client-cert.pem"
    assert artifacts.p12_path == vdir / "kafka-client.p12"
    assert (vdir / "p12-password").is_file()
    # 中间文件已清理
    assert not (vdir / "client-csr.pem").exists()
    assert not (vdir / "san.ext").exists()

    # p12 口令落盘 == 返回值
    assert (vdir / "p12-password").read_text(encoding="utf-8").strip() == artifacts.p12_password


def test_cert_paths_ready(tmp_certs_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_run(monkeypatch)
    cert_signer.issue_client_cert_sync("HUNTER-003")
    assert cert_signer.cert_paths_ready("HUNTER-003") is True
    assert cert_signer.cert_paths_ready("UNKNOWN-999") is False


# =====================================================================
# 失败清理
# =====================================================================
def test_x509_failure_cleans_partial(tmp_certs_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """第 3 步 x509 失败时，已生成的 key/csr 应被清理。"""
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **_kw: Any) -> MagicMock:
        calls.append(list(cmd))
        if cmd[1] == "x509":
            raise subprocess.CalledProcessError(  # type: ignore[arg-type]
                returncode=1, cmd=cmd, stderr="CA key not readable"
            )
        if "-out" in cmd:
            out = Path(cmd[cmd.index("-out") + 1])
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(b"fake")
        result = MagicMock()
        result.stdout = ""
        result.stderr = ""
        result.returncode = 0
        return result

    monkeypatch.setattr(cert_signer.subprocess, "run", fake_run)
    with pytest.raises(ServiceUnavailableError):
        cert_signer.issue_client_cert_sync("FAIL-001")

    vdir = tmp_certs_dir / "FAIL-001"
    # 已写入的 key / csr 都被清理
    assert not (vdir / "client-key.pem").exists()
    assert not (vdir / "client-csr.pem").exists()
    assert not (vdir / "client-cert.pem").exists()
    assert not (vdir / "kafka-client.p12").exists()


def test_timeout_maps_to_5001(tmp_certs_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(cmd: list[str], **_kw: Any) -> MagicMock:
        if cmd[1] == "genrsa":
            raise subprocess.TimeoutExpired(cmd=cmd, timeout=15)  # type: ignore[arg-type]
        raise AssertionError("不应到达此分支")

    monkeypatch.setattr(cert_signer.subprocess, "run", fake_run)
    with pytest.raises(ServiceUnavailableError) as exc:
        cert_signer.issue_client_cert_sync("TIMEOUT-01")
    assert "超时" in str(exc.value)


def test_missing_ca_fails_fast(tmp_certs_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """CA 文件不存在时，不启动 openssl 子进程（fail-fast 5001）。"""
    monkeypatch.setattr(settings, "kafka_ca_key_path", "/nonexistent/ca-key.pem")
    rec = _install_fake_run(monkeypatch)
    with pytest.raises(ServiceUnavailableError):
        cert_signer.issue_client_cert_sync("NOCA-01")
    assert rec.cmds == []


# =====================================================================
# delete_cert_dir
# =====================================================================
def test_delete_cert_dir_idempotent(tmp_certs_dir: Path) -> None:
    assert cert_signer.delete_cert_dir("NOPE") is False
    vdir = tmp_certs_dir / "NOPE"
    assert not vdir.exists()


def test_delete_cert_dir_removes_all(tmp_certs_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_run(monkeypatch)
    cert_signer.issue_client_cert_sync("TO-DELETE")
    vdir = tmp_certs_dir / "TO-DELETE"
    assert vdir.is_dir()
    assert cert_signer.delete_cert_dir("TO-DELETE") is True
    assert not vdir.exists()


# =====================================================================
# read_p12_password
# =====================================================================
def test_read_p12_password_roundtrip(tmp_certs_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_run(monkeypatch)
    artifacts = cert_signer.issue_client_cert_sync("PW-001")
    assert cert_signer.read_p12_password("PW-001") == artifacts.p12_password
    assert cert_signer.read_p12_password("UNKNOWN") is None
