"""仿真端节点级独立回传消费者（消费组 ``data-collector-simulation``，V1.24.2 一键开通）。

契约：``contracts/kafka/consumer-groups.yaml``（data-collector-simulation）+
``topics.yaml#simulation_node_topics``：
- 订阅 ``simulation.*.result``（节点级动态 Topic，按 node_id 隔离，key=sim_id）；
- 消息结构复用 ``carla_sensor_data.schema.json``（与实例级 ``carla.*.sensor_data`` 同 Schema）；
- 处理逻辑与 :class:`~app.consumers.carla.CarlaSensorConsumer` **同构**（契约 consumer-groups
  notes 明确要求）：回传→``vehicle_telemetry``（``vehicle_id=SIM-{sim_id}``）/可映射事件→``events``
  +``event_raw``；状态/指标/产物→Redis；``sensor_file`` 复用现有通知 Topic。

因两条通道语义完全一致、仅消费组与订阅模式不同，本类直接继承 CARLA 消费者复用其分发/落库/
缓存实现——幂等命名空间取自 ``group_id``（两组独立 offset、互不干扰），Topic→Schema 由契约按
实际 Topic 自动解析（``simulation.{node_id}.result`` 亦映射到同一 ``carla_sensor_data`` Schema）。

与实例级 ``carla.*.sensor_data``（场景仿真实例生命周期产出）分离：本通道承载**节点级**产出/状态
回传（非单次仿真会话）。不跨 schema 写 ``data_analytics.*``（报告由 data-analytics 唯一写方生成）。
"""
from __future__ import annotations

from typing import ClassVar

from app.consumers.carla import CarlaSensorConsumer


class SimulationNodeConsumer(CarlaSensorConsumer):
    """``simulation.*.result`` 消费者（与 ``carla.*.sensor_data`` 同构，独立消费组）。"""

    group_id: ClassVar[str] = "data-collector-simulation"
    subscribe_pattern: ClassVar[str] = "simulation.*.result"


__all__ = ["SimulationNodeConsumer"]
