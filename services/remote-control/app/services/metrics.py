"""remote-control 服务专属 Prometheus 指标（契约 x-hunter-observability）。

- 指标统一前缀 hunter_，注册到 hunter_common.metrics.REGISTRY（/metrics 端点统一暴露）；
- 多副本部署下 Gauge（sessions_active）为副本本地值，聚合由 PromQL sum() 完成；
- pending #19：Grafana 看板与告警规则接入待监控配置更新时补充。
"""
from __future__ import annotations

from hunter_common.metrics import REGISTRY
from prometheus_client import Counter, Gauge

SESSIONS_ACTIVE = Gauge(
    "hunter_remote_control_sessions_active",
    "当前活跃操控会话数（标签 vehicle_id；副本本地 Gauge，聚合用 sum）",
    labelnames=("vehicle_id",),
    registry=REGISTRY,
)

SESSION_CONFLICTS_TOTAL = Counter(
    "hunter_remote_control_session_conflicts_total",
    "创建会话时的 7001 冲突次数（标签 vehicle_id）",
    labelnames=("vehicle_id",),
    registry=REGISTRY,
)

ARCHIVE_FAILED_TOTAL = Counter(
    "hunter_remote_control_archive_failed_total",
    "录像/sidecar 归档失败次数（标签 stage：video / sidecar）",
    labelnames=("stage",),
    registry=REGISTRY,
)

RATE_LIMITED_TOTAL = Counter(
    "hunter_remote_control_rate_limited_total",
    "限流触发次数（标签 endpoint）",
    labelnames=("endpoint",),
    registry=REGISTRY,
)

COMMANDS_TOTAL = Counter(
    "hunter_remote_control_commands_total",
    "下发车端的控制指令/信令帧计数（标签 result：sent / failed）",
    labelnames=("result",),
    registry=REGISTRY,
)

__all__ = [
    "ARCHIVE_FAILED_TOTAL",
    "COMMANDS_TOTAL",
    "RATE_LIMITED_TOTAL",
    "SESSIONS_ACTIVE",
    "SESSION_CONFLICTS_TOTAL",
]
