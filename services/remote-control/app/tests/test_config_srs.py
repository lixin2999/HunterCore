"""SRS 媒体地址拼装单测（V1.21.0：车端 RTMP 推流 → SRS → 浏览器 WHEP 拉流）。

契约 x-hunter-websocket-contract / srs.conf 约定：
- 流名 = {prefix}_{vehicle_id}_{session_id}（app=remote_control）
- WHEP/WHIP 基址 + ?app=…&stream=… 查询由后端拼装下发；基址未配置返回 None（前端提示媒体未就绪）。
"""

from __future__ import annotations

from app.config import Settings

_VID = "HUNTER-001"
_SID = "3f2a-uuid"


def test_stream_name_uses_underscore_join() -> None:
    """流名与 srs.conf 约定一致（hunter_{vid}_{sid}，非斜杠）。"""
    settings = Settings()
    assert settings.srs_stream_name(_VID, _SID) == f"hunter_{_VID}_{_SID}"


def test_whep_whip_assemble_app_and_stream_query() -> None:
    """WHEP/WHIP 基址后拼 ?app=remote_control&stream=…（基址不含查询时用 ?）。"""
    settings = Settings(
        rc_srs_whep_url="http://10.0.0.9:9090/rtc/v1/whep/",
        rc_srs_whip_url="http://10.0.0.9:9090/rtc/v1/whip/",
    )
    stream = f"hunter_{_VID}_{_SID}"
    assert settings.srs_whep_url_for(_VID, _SID) == (
        f"http://10.0.0.9:9090/rtc/v1/whep/?app=remote_control&stream={stream}"
    )
    assert settings.srs_whip_url_for(_VID, _SID) == (
        f"http://10.0.0.9:9090/rtc/v1/whip/?app=remote_control&stream={stream}"
    )


def test_existing_query_uses_ampersand_separator() -> None:
    """基址已含查询串时用 & 续接，避免生成双 ?。"""
    settings = Settings(rc_srs_whep_url="http://h:9090/rtc/v1/whep/?reuse=1")
    url = settings.srs_whep_url_for(_VID, _SID)
    assert url is not None
    assert url.count("?") == 1
    assert "&app=remote_control&stream=" in url


def test_rtmp_appends_stream_path_segment() -> None:
    """RTMP 基址后接 /stream（车端推流地址，非查询）。"""
    settings = Settings(rc_srs_rtmp_url="rtmp://10.0.0.9:1935/remote_control")
    assert settings.srs_rtmp_url_for(_VID, _SID) == (
        f"rtmp://10.0.0.9:1935/remote_control/hunter_{_VID}_{_SID}"
    )


def test_blank_base_urls_return_none() -> None:
    """基址未配置（默认空串）→ 对应字段 None，前端据此提示媒体服务未就绪。"""
    settings = Settings()
    assert settings.srs_whep_url_for(_VID, _SID) is None
    assert settings.srs_whip_url_for(_VID, _SID) is None
    assert settings.srs_rtmp_url_for(_VID, _SID) is None
