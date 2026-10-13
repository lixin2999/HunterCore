"""GNSS 解析器（sensor_msgs/NavSatFix，设计文档 §2 分流表）。

展开经纬高、协方差状态，输出 CSV/Parquet 行。
"""
from __future__ import annotations

from collections.abc import Iterator
from dataclasses import asdict, dataclass
from typing import Any

from hunter_common.logging import get_logger

from app.parsers.rosbag_reader import RawMessage

logger = get_logger("app.parsers.gnss_parser")


@dataclass
class GnssRecord:
    """单条 GNSS 记录（展开字段）。"""
    stamp_ns: int
    bag_ts_ns: int
    frame_id: str
    topic: str
    # WGS84 经纬高
    lat: float = 0.0      # 纬度（度）
    lon: float = 0.0      # 经度（度）
    alt: float = 0.0      # 高程（米，椭球基准）
    # 定位状态（sensor_msgs/NavSatStatus）
    status_service: int = 0   # 位掩码：GPS=1, GLONASS=2, COMPASS=4, GALILEO=8
    status: int = -1          # -1=ERROR, 0=OK, 1=SB_FIX, 2=GBAS_FIX
    # 位置协方差（对角线，m²）
    cov_lat: float = 0.0
    cov_lon: float = 0.0
    cov_alt: float = 0.0


def parse_gnss_message(msg: RawMessage) -> GnssRecord | None:
    """解析 sensor_msgs/NavSatFix 消息。"""
    data: dict[str, Any] = msg.data
    try:
        frame_id = str(data.get("header", {}).get("frame_id", ""))
        ts_ns = msg.stamp_ns or msg.bag_ts_ns

        status_raw = data.get("status", {})
        cov = data.get("position_covariance", [])
        # position_covariance 是 9 元素（3x3）或 3 元素（对角线）
        cov_lat = float(cov[0]) if len(cov) >= 1 else 0.0
        cov_lon = float(cov[4]) if len(cov) >= 5 else (float(cov[1]) if len(cov) >= 2 else 0.0)
        cov_alt = float(cov[8]) if len(cov) >= 9 else (float(cov[2]) if len(cov) >= 3 else 0.0)

        return GnssRecord(
            stamp_ns=ts_ns,
            bag_ts_ns=msg.bag_ts_ns,
            frame_id=frame_id,
            topic=msg.topic,
            lat=float(data.get("latitude", 0.0)),
            lon=float(data.get("longitude", 0.0)),
            alt=float(data.get("altitude", 0.0)),
            status_service=int(status_raw.get("service", 0)),
            status=int(status_raw.get("status", -1)),
            cov_lat=cov_lat,
            cov_lon=cov_lon,
            cov_alt=cov_alt,
        )
    except Exception as exc:
        logger.warning("gnss_parse_failed", topic=msg.topic, error=str(exc))
        return None


def parse_messages(messages: Iterator[RawMessage]) -> Iterator[GnssRecord]:
    """批量解析 GNSS 消息流。"""
    for msg in messages:
        if msg.msg_type in ("sensor_msgs/NavSatFix", "sensor_msgs/msg/NavSatFix"):
            rec = parse_gnss_message(msg)
            if rec is not None:
                yield rec


def to_dicts(records: Iterator[GnssRecord] | list[GnssRecord]) -> list[dict[str, Any]]:
    return [asdict(r) for r in records]


def mask_coordinates(lat: float, lon: float, places: int = 4) -> tuple[float, float]:
    """脱敏坐标（§13.4：截断到指定小数位）。"""
    factor = 10 ** places
    return round(lat * factor) / factor, round(lon * factor) / factor


__all__ = [
    "GnssRecord",
    "mask_coordinates",
    "parse_gnss_message",
    "parse_messages",
    "to_dicts",
]
