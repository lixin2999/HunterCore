"""flink-jobs 共享包 —— 设计文档 6.2 节 Flink 实时分析作业（契约单一事实来源：
``contracts/openapi/data-analytics.yaml`` 的 ``x-hunter-realtime-jobs``）。

纪律（G-13）：
- 阈值禁止硬编码：全部经 ``thresholds.AlertThresholds.from_env()`` 读取（默认值 = 契约基准）；
- 告警等级禁止自造：唯一来源 ``hunter_common.database.enums.EVENT_LEVEL_BY_TYPE``（受控词表）；
- 写侧豁免登记：本包不属于 6 个微服务，不 import 任何服务 ``app.*`` 包；
  直写 alert_event（Kafka）与 algorithm_metrics（JDBC）的豁免见
  ``contracts/database/orm-mapping.md`` 第 3.5 节。
"""
