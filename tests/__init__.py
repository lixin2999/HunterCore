"""HunterCore L5 集成与测试层（tests/）。

分层：
- ``tests/support``     测试支撑库（契约加载、消息工厂、容器基础设施、业务流程参考实现、报告）
- ``tests/integration`` 集成测试（真实 PostgreSQL/TimescaleDB、Kafka、Redis、MinIO、六服务）
- ``tests/e2e``         关键业务流程端到端测试（11.1 车辆数据上行 / 11.2 OTA / 11.3 远程操控）
- ``tests/performance`` 性能基准（API P95 / Kafka 吞吐 / 时序写入速率）
- ``tests/test_test_layer_static.py`` 静态自检（无 Docker 亦可运行）

运行入口见 tests/README.md 与 scripts/verify_test_layer.py。
"""
from __future__ import annotations

__all__ = ["support"]
