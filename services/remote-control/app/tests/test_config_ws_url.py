"""远程操控 WS 通道地址拼装单测（V1.21.17：基址归一化，根治双前缀）。

契约 x-hunter-websocket-contract / openapi `WebRtcConnectionInfo` 约定：
- 地址形如 `{scheme}://{host}[:port]/ws/remote/{session_id}/{control|signal}`；
- 基址为「scheme://host[:port]」，**不含路径**（openapi 示例 `wss://api.hunter-core.example.com`）。

但 `infra/deploy/.env.example` 历史形态把基址写成含路径的 `ws://<ip>/ws/remote`，
与业务代码再拼一次 `/ws/remote/{sid}/{channel}` 叠加 → 双前缀 → 浏览器 WS 握手必失败
（前端 `startsWith('/')` 同源兜底掩盖了该缺陷；兜底一旦失效或基址改为相对路径即当场断链）。
故 `Settings.rc_ws_url_for` 对基址做幂等归一，本模块把该语义钉死。
"""

from __future__ import annotations

from app.config import Settings

_SID = "57a6e3d3-9971-43af-a5cd-666123e52992"


def test_contract_base_yields_canonical_url() -> None:
    """契约基址（不含路径）→ 与 openapi 示例同形的单一 /ws/remote 前缀。"""
    settings = Settings(rc_public_ws_base_url="wss://api.hunter-core.example.com")
    assert settings.rc_ws_url_for(_SID, "control") == (
        f"wss://api.hunter-core.example.com/ws/remote/{_SID}/control"
    )
    assert settings.rc_ws_url_for(_SID, "signal") == (
        f"wss://api.hunter-core.example.com/ws/remote/{_SID}/signal"
    )


def test_deploy_template_base_with_path_is_normalized() -> None:
    """部署模板形态 `ws://<ip>/ws/remote` → 不得产出双前缀（实机断链回归）。"""
    settings = Settings(rc_public_ws_base_url="ws://192.168.31.35/ws/remote")
    url = settings.rc_ws_url_for(_SID, "control")
    assert url == f"ws://192.168.31.35/ws/remote/{_SID}/control"
    assert url.count("/ws/remote") == 1


def test_default_base_without_path_is_untouched() -> None:
    """默认值（`wss://hunter-core.example.com`，无路径）不因归一而变形。"""
    settings = Settings()
    url = settings.rc_ws_url_for(_SID, "control")
    assert url == f"{settings.rc_public_ws_base_url}/ws/remote/{_SID}/control"


def test_normalization_is_idempotent_across_accepted_forms() -> None:
    """四种被接受写法（含结尾斜杠/带端口/裸基址）归一后必须产出同一 URL。

    幂等性 = 反复套用 rc_ws_url_for 不会继续吃掉前缀（避免"修一次少一层"）。
    """
    expected = f"ws://h:8085/ws/remote/{_SID}/control"
    for base in (
        "ws://h:8085",
        "ws://h:8085/",
        "ws://h:8085/ws",
        "ws://h:8085/ws/remote",
        "ws://h:8085/ws/remote/",
    ):
        assert Settings(rc_public_ws_base_url=base).rc_ws_url_for(_SID, "control") == expected


def test_custom_path_prefix_is_preserved() -> None:
    """非 /ws、非 /ws/remote 的自定义路径前缀（反向代理子路径）必须保留。"""
    settings = Settings(rc_public_ws_base_url="wss://gw.example.com/hunter")
    assert settings.rc_ws_url_for(_SID, "control") == (
        f"wss://gw.example.com/hunter/ws/remote/{_SID}/control"
    )


def test_schemeless_base_is_not_stripped_into_empty() -> None:
    """无 scheme 的相对基址不做剥离（保持信息可诊断，而非产出无 host 地址）。

    相对基址本身已超出契约（需 `scheme://host`），此处只保证归一化**不吞掉**唯一前缀：
    一旦把 `/ws/remote` 剥成空串，产出会退化为无 host 形态、更难定位。
    """
    settings = Settings(rc_public_ws_base_url="/ws/remote")
    assert settings.rc_ws_url_for(_SID, "control") == (f"/ws/remote/ws/remote/{_SID}/control")
