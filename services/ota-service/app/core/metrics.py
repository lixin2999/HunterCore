"""Prometheus 业务指标（契约 x-hunter-service.observability.metrics 节选）。

注册到 hunter_common.metrics.REGISTRY（与 HTTP 指标同 registry，/metrics 统一暴露）；
只登记本服务可直接产出且契约明确列出的指标：
- ota_package_verify_failures_total{reason}：发布校验失败计数（6001/6002/6003 归因）；
- ota_canary_success_rate{task_id,batch_no}：灰度批次成功率（消费/动作链路写入）。
"""
from __future__ import annotations

from hunter_common.metrics import REGISTRY
from prometheus_client import Counter, Gauge

OTA_PACKAGE_VERIFY_FAILURES = Counter(
    "ota_package_verify_failures_total",
    "OTA 发布校验失败次数（reason: size/md5/sha256/signature/monotonic）",
    labelnames=("reason",),
    registry=REGISTRY,
)

OTA_CANARY_SUCCESS_RATE = Gauge(
    "ota_canary_success_rate",
    "灰度批次成功率（0..1；task_id/batch_no 标签）",
    labelnames=("task_id", "batch_no"),
    registry=REGISTRY,
)

__all__ = ["OTA_CANARY_SUCCESS_RATE", "OTA_PACKAGE_VERIFY_FAILURES"]
