"""内置车辆模拟器（tools/vehicle-simulator）。

独立进程：以车端身份（Kafka SCRAM user=vehicle_id）消费 ``hunter.{vid}.remote_control``
驱动运动学，按契约生产 ``telemetry``/``health``/``command_result``，并向 SRS 推送合成视频，
用于远程操控链路的端到端演示与自测（不依赖真实车辆）。
"""

from __future__ import annotations

__all__ = ["__version__"]

__version__ = "0.1.0"
