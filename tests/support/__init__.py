"""L5 测试支撑库：契约加载 / 消息工厂 / 容器基础设施 / 业务流程参考实现 / 报告生成。

约束（与仓库开发规则一致）：
- 所有契约常量（Topic/字段/阈值/键名/Bucket）只能从 contracts/ 读取，禁止在测试内硬编码
- testcontainers 等重型依赖采用「惰性导入 + 缺失即 skip」，保证无 Docker 环境可正常收集测试
"""
from __future__ import annotations

__all__ = ["contracts", "flow", "infra", "messages", "report", "thresholds"]
