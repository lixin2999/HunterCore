"""图像解析器（sensor_msgs/Image + sensor_msgs/CompressedImage，设计文档 §2 分流表）。

Image: 解码为 PNG/JPG 帧文件（按时间戳命名）
CompressedImage: 直接解压存储
"""
from __future__ import annotations

import io
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import numpy as np
from hunter_common.logging import get_logger
from PIL import Image as PILImage

from app.parsers.rosbag_reader import RawMessage

logger = get_logger("app.parsers.image_parser")

# 支持的颜色编码
_ENCODING_RGB8 = "rgb8"
_ENCODING_BGR8 = "bgr8"
_ENCODING_MONO8 = "mono8"
_ENCODING_RGBA8 = "rgba8"
_ENCODING_BGRA8 = "bgra8"


@dataclass
class ImageFrame:
    """单帧图像解析结果。"""
    topic: str
    ts_ns: int
    frame_id: str
    width: int
    height: int
    encoding: str
    image_bytes: bytes        # 编码后的 PNG/JPG 字节
    fmt: str                  # "png" | "jpeg"
    is_decoded: bool          # True = 成功解码（QC 用）
    sha256: str               # 图像字节 sha256


def parse_image_message(msg: RawMessage, *, output_fmt: str = "jpeg") -> ImageFrame | None:
    """解析 sensor_msgs/Image 原始像素数据。

    Args:
        msg: rosbag 读出的 RawMessage（msg_type 为 sensor_msgs/Image）。
        output_fmt: 输出格式 "png" 或 "jpeg"。

    Returns:
        ImageFrame；解码失败返回 None（QC 计数解码失败）。
    """
    import hashlib
    data: dict[str, Any] = msg.data
    try:
        height = int(data.get("height", 0))
        width = int(data.get("width", 0))
        encoding = str(data.get("encoding", _ENCODING_RGB8))
        is_bigendian = int(data.get("is_bigendian", 0))
        step = int(data.get("step", width * _channels(encoding)))
        raw_data: bytes = bytes(data.get("data", []))

        if height == 0 or width == 0 or not raw_data:
            return None

        # 构造 numpy 数组
        arr = _raw_to_array(raw_data, height, width, step, encoding)
        pil_img = _array_to_pil(arr, encoding)

        # 编码输出
        buf = io.BytesIO()
        pil_img.save(buf, format=output_fmt.upper(), quality=90)
        img_bytes = buf.getvalue()
        sha = hashlib.sha256(img_bytes).hexdigest()

        frame_id = str(data.get("header", {}).get("frame_id", ""))
        ts_ns = msg.stamp_ns or msg.bag_ts_ns

        return ImageFrame(
            topic=msg.topic,
            ts_ns=ts_ns,
            frame_id=frame_id,
            width=width,
            height=height,
            encoding=encoding,
            image_bytes=img_bytes,
            fmt=output_fmt,
            is_decoded=True,
            sha256=sha,
        )
    except Exception as exc:
        logger.warning("image_decode_failed", topic=msg.topic, error=str(exc))
        return None


def parse_compressed_image(msg: RawMessage) -> ImageFrame | None:
    """解析 sensor_msgs/CompressedImage（直接解压）。"""
    import hashlib
    data: dict[str, Any] = msg.data
    try:
        fmt = str(data.get("format", "jpeg"))
        compressed: bytes = bytes(data.get("data", []))
        if not compressed:
            return None

        # PIL 验证可解码
        pil_img = PILImage.open(io.BytesIO(compressed))
        w, h = pil_img.size
        encoding = pil_img.mode.lower()

        frame_id = str(data.get("header", {}).get("frame_id", ""))
        ts_ns = msg.stamp_ns or msg.bag_ts_ns
        sha = hashlib.sha256(compressed).hexdigest()

        return ImageFrame(
            topic=msg.topic,
            ts_ns=ts_ns,
            frame_id=frame_id,
            width=w,
            height=h,
            encoding=encoding,
            image_bytes=compressed,
            fmt="jpeg" if "jpeg" in fmt or "jpg" in fmt else "png",
            is_decoded=True,
            sha256=sha,
        )
    except Exception as exc:
        logger.warning("compressed_image_decode_failed", topic=msg.topic, error=str(exc))
        return None


def parse_messages(messages: Iterator[RawMessage], *, output_fmt: str = "jpeg") -> Iterator[ImageFrame]:
    """批量解析图像消息流（过滤非图像 topic）。"""
    for msg in messages:
        if msg.msg_type in ("sensor_msgs/Image", "sensor_msgs/msg/Image"):
            frame = parse_image_message(msg, output_fmt=output_fmt)
        elif msg.msg_type in ("sensor_msgs/CompressedImage", "sensor_msgs/msg/CompressedImage"):
            frame = parse_compressed_image(msg)
        else:
            continue
        if frame is not None:
            yield frame


# ========== 内部工具 ==========

def _channels(encoding: str) -> int:
    enc = encoding.lower()
    if enc in (_ENCODING_MONO8, "mono16"):
        return 1
    if enc in (_ENCODING_RGB8, _ENCODING_BGR8):
        return 3
    if enc in (_ENCODING_RGBA8, _ENCODING_BGRA8):
        return 4
    return 3


def _raw_to_array(
    raw: bytes, height: int, width: int, step: int, encoding: str
) -> np.ndarray:
    """将原始字节转为 (H, W, C) numpy 数组。"""
    ch = _channels(encoding)
    dtype = np.uint16 if "16" in encoding else np.uint8
    arr = np.frombuffer(raw, dtype=dtype)
    # 按行步幅重塑
    arr = arr.reshape(height, step // np.dtype(dtype).itemsize)[:, : width * ch]
    return arr.reshape(height, width, ch)


def _array_to_pil(arr: np.ndarray, encoding: str) -> PILImage:
    enc = encoding.lower()
    if enc == _ENCODING_BGR8:
        arr = arr[:, :, ::-1]   # BGR → RGB
    elif enc == _ENCODING_BGRA8:
        arr = arr[:, :, [2, 1, 0, 3]]
    if arr.shape[2] == 1:
        return PILImage.fromarray(arr.squeeze(2), mode="L")
    if arr.shape[2] == 4:
        return PILImage.fromarray(arr.astype(np.uint8), mode="RGBA")
    return PILImage.fromarray(arr.astype(np.uint8), mode="RGB")


__all__ = [
    "ImageFrame",
    "parse_compressed_image",
    "parse_image_message",
    "parse_messages",
]
