"""vil-forwarder — VIL 数据面转发器。

消费 ``telemetry_clean``，按 Redis ``vil:session:{vehicle_id}`` 会话映射筛选 VIL 车辆，
以 10Hz latest-wins 降采样发布 ``carla.{sim_id}.vil_state``（key=sim_id）。
"""
__all__: list[str] = []
