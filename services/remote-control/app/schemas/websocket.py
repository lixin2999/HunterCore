"""WS 帧模型（契约 x-hunter-websocket-contract.frames）。

- 上行（control 通道）：control（20Hz）/ heartbeat（10s）/ estop；
  上行（signal 通道）：sdp / ice；
  G-10：control.gear（P/R/N/D）与 control.brake（0~1）为契约化可选字段，限幅/验词后
  透传 Kafka（remote_control.schema.json control 可选键）；mode 为平台侧预留字段，
  **不接受上行**（防注入，值域 pending #21）；其余未契约化键仍丢弃；
- 下行：ack / status / error（code 取预定义错误码，禁止自定义；构建函数输出 dict 直发）；
- 解析入口 :func:`parse_uplink_frame`：JSON 非法/type 未知 → InvalidParameterError（2001
  语义由路由层转 error 帧，不断连接）。
"""
from __future__ import annotations

import json
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

UPLINK_TYPES = ("control", "heartbeat", "estop", "sdp", "ice")


class _UplinkBase(BaseModel):
    """上行帧基类：忽略未契约化附加字段，禁止因未知键炸错。"""

    model_config = ConfigDict(extra="ignore")


class WsControlPayloadRef(BaseModel):
    """控制量（G-10 后可转发 Kafka 的字段集：velocity/steer 必填 + gear/brake 可选）。"""

    model_config = ConfigDict(extra="ignore")

    target_velocity: float
    target_steer: float = 0.0
    gear: Literal["P", "R", "N", "D"] | None = None   # 词表同 health.schema.json（非法词 → 2001）
    brake: float | None = Field(default=None, ge=0.0, le=1.0)  # 归一化制动请求 0~1


class WsControlFrameIn(_UplinkBase):
    type: Literal["control"]
    session_id: str
    seq: int = 0
    timestamp: float = 0.0
    control: WsControlPayloadRef


class WsHeartbeatFrameIn(_UplinkBase):
    type: Literal["heartbeat"]
    session_id: str
    timestamp: float = 0.0


class WsEstopFrameIn(_UplinkBase):
    type: Literal["estop"]
    session_id: str
    reason: str = ""


class WsSdpFrameIn(_UplinkBase):
    type: Literal["sdp"]
    sdp_type: Literal["offer", "answer"]
    sdp: str


class WsIceFrameIn(_UplinkBase):
    type: Literal["ice"]
    candidate: str
    sdpMid: str | None = None       # 契约帧字段名即 camelCase（WebRTC 惯例）
    sdpMLineIndex: int | None = None


_MODEL_BY_TYPE: dict[str, type[BaseModel]] = {
    "control": WsControlFrameIn,
    "heartbeat": WsHeartbeatFrameIn,
    "estop": WsEstopFrameIn,
    "sdp": WsSdpFrameIn,
    "ice": WsIceFrameIn,
}


class UplinkFrameError(ValueError):
    """上行帧非法（JSON 解析失败 / type 不在受控词表 / 字段校验失败）。"""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


def parse_uplink_frame(raw: str | bytes) -> BaseModel:
    """解析并校验上行帧（失败抛 :class:`UplinkFrameError`，路由层转 error 帧 2001）。"""
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise UplinkFrameError(f"上行帧不是合法 JSON：{exc}") from exc
    if not isinstance(payload, dict):
        raise UplinkFrameError("上行帧必须是 JSON 对象")
    frame_type = payload.get("type")
    model = _MODEL_BY_TYPE.get(frame_type if isinstance(frame_type, str) else "")
    if model is None:
        raise UplinkFrameError(
            f"上行帧 type 非法：{frame_type!r}（受控词表：{', '.join(UPLINK_TYPES)}）"
        )
    try:
        return model.model_validate(payload)
    except ValidationError as exc:
        raise UplinkFrameError(f"上行帧字段校验失败：{exc.errors(include_url=False)}") from exc


# ---------- 下行帧构建（dict 直发，字段与 types/remote.ts 前端类型逐字对齐） ----------
def ack_frame(
    session_id: str, seq: int, ack_latency_ms: float, vehicle_state: str | None
) -> dict[str, Any]:
    """ack 帧（来源 command_result；seq 按时序近似关联，pending #16 禁逐条精确）。"""
    return {
        "type": "ack",
        "session_id": session_id,
        "seq": seq,
        "ack_latency_ms": round(ack_latency_ms, 1),
        "vehicle_state": vehicle_state,
    }


def status_frame(
    status: str,
    degraded_reasons: list[str],
    control_stats: dict[str, Any] | None,
    video_stats: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """status 帧（1Hz 或状态变更时推送）。"""
    return {
        "type": "status",
        "status": status,
        "degraded_reasons": degraded_reasons,
        "control_stats": control_stats,
        "video_stats": video_stats,
    }


def error_frame(code: int, message: str, request_id: str | None = None) -> dict[str, Any]:
    """error 帧（code 取预定义错误码，禁止自定义）。"""
    return {"type": "error", "code": code, "message": message, "request_id": request_id}


__all__ = [
    "UPLINK_TYPES",
    "UplinkFrameError",
    "WsControlFrameIn",
    "WsControlPayloadRef",
    "WsEstopFrameIn",
    "WsHeartbeatFrameIn",
    "WsIceFrameIn",
    "WsSdpFrameIn",
    "ack_frame",
    "error_frame",
    "parse_uplink_frame",
    "status_frame",
]
