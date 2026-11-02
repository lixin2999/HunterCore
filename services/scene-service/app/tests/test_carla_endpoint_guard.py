"""Carla 管理 API 配置门禁与 5001 文案分流回归（V1.23.7）。

现场起因：``.env`` 中 ``CARLA_MANAGEMENT_ENDPOINT=   # 例：http://…`` 的行尾注释被 docker
compose 当成**值**下发进容器 ⇒ 值非空绕过「未配置」判定 ⇒ httpx 抛 ``UnsupportedProtocol``
⇒ 被兜底文案伪装成「Carla 管理 API 不可达」，现场按网络故障多绕一轮。

本文件钉住三件事：
① 配置类缺陷（地址形态 / 子路径模板）必须在**发出请求前**指名，且不得泄成 5000；
② 传输类失败三类文案互不混用（超时 / 协议非法 / 不可达）；
③ 子路径空串回落服务默认值（compose 以 ``${VAR:-}`` 透传的必然要求）。
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

import httpx
import pytest
from hunter_common.exceptions import ServiceUnavailableError

from app.config import Settings
from app.repositories.carla import CarlaManagementClient, validate_management_endpoint

#: 现场被污染的实际取值（docker exec hunter-scene env 采证）
POLLUTED_ENDPOINT = "# 例：http://carla-manager.hunter-core:2000"


def _client(
    base_url: str,
    handler: Callable[[httpx.Request], Awaitable[httpx.Response]] | None = None,
    **overrides: Any,
) -> CarlaManagementClient:
    kwargs: dict[str, Any] = {
        "create_path": "/api/v1/instances",
        "query_path": "/api/v1/instances",
        "scenario_path": "/api/v1/instances/{sim_instance_id}/scenario",
        "timeout_seconds": 1.0,
        "max_retries": 0,
    }
    kwargs.update(overrides)
    client = CarlaManagementClient(base_url, **kwargs)
    if handler is not None:
        client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return client


# ---------------------------------------------------------------- ① 配置门禁


@pytest.mark.asyncio
async def test_empty_endpoint_is_reported_as_not_configured() -> None:
    client = _client("")
    with pytest.raises(ServiceUnavailableError) as exc_info:
        await client.find_active_instance("scene-1")
    assert exc_info.value.message == "Carla 管理 API 未配置"
    assert client._client is None  # 未创建连接池 ⇒ 确实未发出请求


@pytest.mark.asyncio
async def test_inline_comment_polluted_endpoint_is_rejected_before_any_request() -> None:
    """V1.23.7 主用例：行尾注释污染值 ⇒ 文案必须是「地址非法」，不能再是「不可达」。"""
    client = _client(POLLUTED_ENDPOINT)
    assert client.configured  # 值非空，旧的 bool 判定确实区分不出来
    assert client.endpoint_problem is not None

    with pytest.raises(ServiceUnavailableError) as exc_info:
        await client.find_active_instance("scene-1")
    assert exc_info.value.message == "Carla 管理 API 地址非法"
    assert exc_info.value.details["env"] == "CARLA_MANAGEMENT_ENDPOINT"
    assert "scheme" in exc_info.value.details["problem"]
    assert client._client is None  # 请求前拦截：不打网络、不留副作用


@pytest.mark.asyncio
async def test_endpoint_carrying_subpath_is_rejected() -> None:
    """基址带子路径会与 *_PATH 拼成错误 URL（静默 404），故按配置非法直接拦下。"""
    client = _client("http://carla-manager:8090/api/v1")
    with pytest.raises(ServiceUnavailableError) as exc_info:
        await client.create_instance(scene_id="s", scene_name="n", scene_config={})
    assert exc_info.value.message == "Carla 管理 API 地址非法"
    assert "子路径" in exc_info.value.details["problem"]


@pytest.mark.parametrize(
    ("base_url", "expected_problem"),
    [
        ("http://192.168.31.35:2000", None),
        ("https://carla.example.com", None),
        ("http://carla-manager/", None),
        ("", None),  # 空值由「未配置」分支处理，不算形态缺陷
        (POLLUTED_ENDPOINT, "scheme"),
        ("carla-manager:8090", "scheme 非法"),
        ("ftp://carla:21", "仅支持 http/https"),
        ("http://", "缺少主机名"),
        ("http://host:8090/sim", "不得携带子路径"),
    ],
)
def test_validate_management_endpoint_shapes(base_url: str, expected_problem: str | None) -> None:
    problem = validate_management_endpoint(base_url)
    if expected_problem is None:
        assert problem is None, f"{base_url!r} 应判合法，实际：{problem}"
    else:
        assert problem is not None and expected_problem in problem


@pytest.mark.asyncio
async def test_malformed_scenario_template_is_config_error_not_internal_error() -> None:
    """子路径模板缺 `{sim_instance_id}` ⇒ 5001 配置非法（不得让 KeyError 泄成 5000）。"""
    client = _client(
        "http://carla:8090",
        handler=_echo_ok,
        scenario_path="/api/v1/instances/{instance_id}/scenario",
    )
    with pytest.raises(ServiceUnavailableError) as exc_info:
        await client.submit_scenario("inst-1", {})
    assert exc_info.value.message == "Carla 管理 API 子路径配置非法"
    assert exc_info.value.details["env"] == "CARLA_SCENARIO_SUBMIT_PATH"
    await client.close()


async def _echo_ok(_request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json={"instance_id": "inst-1", "status": "created"})


@pytest.mark.asyncio
async def test_valid_endpoint_and_template_still_dispatch() -> None:
    """门禁不得误伤正常配置：合法地址 + 合法模板仍要打通。"""
    client = _client("http://carla:8090", handler=_echo_ok)
    assert client.endpoint_problem is None
    instance = await client.create_instance(
        scene_id="scene-1", scene_name="n", scene_config={}, sim_id="sim-1"
    )
    assert instance.instance_id == "inst-1"
    await client.close()


# ---------------------------------------------------------------- ② 文案分流


@pytest.mark.parametrize(
    ("exc", "expected_message"),
    [
        (httpx.ConnectTimeout("slow"), "Carla 管理 API 连接超时"),
        (httpx.ReadTimeout("slow"), "Carla 管理 API 连接超时"),
        (httpx.ConnectError("refused"), "Carla 管理 API 不可达"),
        (
            httpx.UnsupportedProtocol("no scheme"),
            "Carla 管理 API 地址协议非法（需 http/https）",
        ),
    ],
)
@pytest.mark.asyncio
async def test_transport_failures_have_distinct_messages(
    exc: Exception, expected_message: str
) -> None:
    async def failing(_request: httpx.Request) -> httpx.Response:
        raise exc

    client = _client("http://carla:8090", handler=failing)
    with pytest.raises(ServiceUnavailableError) as info:
        await client.find_active_instance("scene-1")
    assert info.value.message == expected_message
    assert info.value.details["error"] == type(exc).__name__
    await client.close()


# ---------------------------------------------------------------- ③ 配置归一


def test_blank_carla_paths_fall_back_to_defaults() -> None:
    """compose ``${VAR:-}`` 透传后，容器内空串必须回落默认（否则路径为空、请求打飞）。"""
    conf = Settings(
        carla_instance_create_path="",
        carla_scenario_submit_path="   ",
        carla_instance_get_path="",
    )
    defaults = Settings()
    assert conf.carla_instance_create_path == defaults.carla_instance_create_path
    assert conf.carla_scenario_submit_path == defaults.carla_scenario_submit_path
    assert conf.carla_instance_get_path == defaults.carla_instance_get_path
    assert "{sim_instance_id}" in conf.carla_scenario_submit_path


def test_explicit_carla_path_override_is_kept() -> None:
    conf = Settings(
        carla_instance_create_path=" /v2/sessions ",
        carla_instance_stop_path="/v2/sessions/{sim_instance_id}/abort",
    )
    assert conf.carla_instance_create_path == "/v2/sessions"
    assert conf.carla_instance_stop_path == "/v2/sessions/{sim_instance_id}/abort"
