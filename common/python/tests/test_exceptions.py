"""hunter_common.exceptions 单元测试：错误码必须与预定义错误码表一致。"""
from __future__ import annotations

import pytest

from hunter_common import exceptions

ALL_EXCEPTION_CLASSES: list[type[exceptions.HunterBaseException]] = [
    exceptions.AuthenticationError,               # 1001
    exceptions.PermissionDeniedError,             # 1002
    exceptions.TokenExpiredError,                 # 1003
    exceptions.InvalidParameterError,             # 2001
    exceptions.MissingParameterError,             # 2002
    exceptions.ResourceNotFoundError,             # 3001
    exceptions.ResourceAlreadyExistsError,        # 3002
    exceptions.ResourceStateConflictError,        # 3003
    exceptions.VehicleOfflineError,               # 4001
    exceptions.VehicleBusyError,                  # 4002
    exceptions.InternalServerError,               # 5000
    exceptions.ServiceUnavailableError,           # 5001
    exceptions.OtaPackageChecksumError,           # 6001
    exceptions.OtaSignatureError,                 # 6002
    exceptions.OtaPreconditionError,              # 6003
    exceptions.RemoteControlSessionConflictError, # 7001
    exceptions.RemoteControlVideoError,           # 7002
]

EXPECTED_CODES = [1001, 1002, 1003, 2001, 2002, 3001, 3002, 3003,
                  4001, 4002, 5000, 5001, 6001, 6002, 6003, 7001, 7002]


@pytest.mark.parametrize("exc_cls", ALL_EXCEPTION_CLASSES)
def test_exception_code_matches_spec(exc_cls: type[exceptions.HunterBaseException]) -> None:
    exc = exc_cls()
    assert exc.code == exc_cls.code
    assert exc.message
    assert str(exc) == f"[{exc.code}] {exc.message}"


@pytest.mark.parametrize("exc_cls,expected", list(zip(ALL_EXCEPTION_CLASSES, EXPECTED_CODES)))
def test_error_codes_are_predefined(
    exc_cls: type[exceptions.HunterBaseException], expected: int
) -> None:
    assert exc_cls.code == expected


def test_custom_message_and_details() -> None:
    exc = exceptions.VehicleOfflineError("HUNTER-001 离线", details={"vehicle_id": "HUNTER-001"})
    assert exc.code == 4001
    assert exc.message == "HUNTER-001 离线"
    assert exc.details == {"vehicle_id": "HUNTER-001"}
    assert str(exc) == "[4001] HUNTER-001 离线"


def test_base_exception_default_is_internal_error() -> None:
    exc = exceptions.HunterBaseException()
    assert exc.code == 5000
    assert exc.message == "服务器内部错误"
