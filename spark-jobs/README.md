# spark-jobs — Spark 3.5 离线分析作业

离线批处理作业（PySpark），从 TimescaleDB / MinIO 读取数据：

- 批量指标计算与日报/周报生成（输出至 hunter-reports Bucket）
- 大规模 Corner Case 离线挖掘

> 当前为 L0 骨架占位，作业实现随 data-analytics 模块开发。
