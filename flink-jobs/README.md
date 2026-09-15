# flink-jobs — Flink 1.18 实时分析作业

实时流处理作业（Python DataStream API / PyFlink），消费 Kafka 平台内部 Topic：

- `telemetry_clean` → 指标计算 + 实时告警（alert_event，告警延迟 ≤ 2s）
- Corner Case 挖掘（异常驾驶事件检测，阈值见设计文档事件类型定义）

> 当前为 L0 骨架占位，作业实现随 data-analytics 模块开发。
