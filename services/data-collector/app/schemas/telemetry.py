"""遥测查询响应模型（契约 components.schemas：TelemetrySample → TelemetryQueryResponse）。

字段与 contracts/kafka/schemas/telemetry.schema.json 一一对应（5.3.3 节六段嵌套结构
不可更改），由 data_collector.vehicle_telemetry 扁平列重组；段内字段全部 nullable，
未采集到的段返回 null。
"""
from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from app.schemas.common import TELEMETRY_RETENTION_DAYS, VEHICLE_ID_PATTERN


class TelemetryChassis(BaseModel):
    """底盘段（5.3.3 节 chassis）。"""

    model_config = ConfigDict(extra="forbid")

    velocity: float | None = Field(default=None, description="车速 m/s（前进为正）")
    steering_angle: float | None = Field(default=None, description="前轮转角 rad")
    battery_voltage: float | None = Field(default=None, description="电池电压 V")
    battery_soc: int | None = Field(default=None, ge=0, le=100, description="剩余电量 %")
    battery_current: float | None = Field(default=None, description="电池电流 A（放电为负）")
    battery_temp: float | None = Field(default=None, description="电池温度 ℃")
    control_mode: str | None = Field(default=None, description="底盘控制模式（示例 CAN）")
    vehicle_state: str | None = Field(default=None, description="底盘状态字（示例 NORMAL）")
    fault_code: int | None = Field(default=None, description="底盘故障码，0 表示无故障")
    motor_rpm: list[int] | None = Field(default=None, description="各驱动电机转速 rpm")
    motor_current: list[float] | None = Field(default=None, description="各驱动电机电流 A")
    motor_temp: list[int] | None = Field(default=None, description="各驱动电机温度 ℃")


class TelemetryLocalization(BaseModel):
    """定位段（5.3.3 节 localization）。"""

    model_config = ConfigDict(extra="forbid")

    x: float | None = Field(default=None, description="地图坐标系 X / m")
    y: float | None = Field(default=None, description="地图坐标系 Y / m")
    z: float | None = Field(default=None, description="地图坐标系 Z / m")
    roll: float | None = Field(default=None, description="横滚角 rad")
    pitch: float | None = Field(default=None, description="俯仰角 rad")
    heading: float | None = Field(default=None, description="航向角 rad")
    linear_velocity: list[float] | None = Field(default=None, description="[vx, vy, vz] m/s")
    angular_velocity: list[float] | None = Field(default=None, description="[ωx, ωy, ωz] rad/s")
    position_std: float | None = Field(default=None, description="定位位置标准差 m")
    heading_std: float | None = Field(default=None, description="航向标准差 rad")


class TelemetryPerception(BaseModel):
    """感知段（5.3.3 节 perception）。"""

    model_config = ConfigDict(extra="forbid")

    detected_objects: int | None = Field(default=None, description="检出目标数")
    fps: float | None = Field(default=None, description="感知帧率")
    latency_ms: float | None = Field(default=None, description="感知延迟 ms")
    object_types: dict[str, int] | None = Field(
        default=None, description="按类别统计（vehicle / pedestrian / other）；落库 JSONB"
    )


class TelemetryPlanning(BaseModel):
    """规划段（5.3.3 节 planning）。"""

    model_config = ConfigDict(extra="forbid")

    trajectory_length: float | None = Field(default=None, description="规划轨迹长度 m")
    trajectory_points: int | None = Field(default=None, description="轨迹点数量")
    planning_latency_ms: float | None = Field(default=None, description="规划计算延迟 ms")
    current_behavior: str | None = Field(default=None, description="当前行为（示例 cruise）")


class TelemetryControl(BaseModel):
    """控制段（5.3.3 节 control）。"""

    model_config = ConfigDict(extra="forbid")

    target_velocity: float | None = Field(default=None, description="目标速度 m/s")
    target_steer: float | None = Field(default=None, description="目标前轮转角 rad")
    velocity_error: float | None = Field(default=None, description="速度误差 m/s")
    steer_error: float | None = Field(default=None, description="转角误差 rad")
    control_latency_ms: float | None = Field(default=None, description="控制链路延迟 ms")


class TelemetrySystem(BaseModel):
    """系统段（5.3.3 节 system，车载计算平台 AGX Orin）。"""

    model_config = ConfigDict(extra="forbid")

    cpu_usage: float | None = Field(default=None, ge=0, le=100, description="CPU 使用率 %")
    gpu_usage: float | None = Field(default=None, ge=0, le=100, description="GPU 使用率 %")
    memory_usage_mb: int | None = Field(default=None, ge=0, description="内存占用 MB")
    gpu_temp: float | None = Field(default=None, description="GPU 温度 ℃")
    cpu_temp: float | None = Field(default=None, description="CPU 温度 ℃")
    network_rssi: int | None = Field(default=None, le=0, description="无线信号强度 dBm（负值）")
    network_latency_ms: float | None = Field(
        default=None, ge=0, description="车-云网络往返延迟 ms"
    )


class TelemetrySample(BaseModel):
    """单条遥测样本（= 5.3.3 节消息结构 + 入库列 time）。"""

    model_config = ConfigDict(extra="forbid")

    time: float = Field(description="车端采集时间（Unix epoch 秒，含毫秒小数）= 消息 timestamp")
    vehicle_id: str = Field(pattern=VEHICLE_ID_PATTERN, description="车辆标识")
    seq: int | None = Field(default=None, description="车端消息序号（幂等/丢包检测）")
    chassis: TelemetryChassis | None = Field(default=None, description="底盘段（未采集为 null）")
    localization: TelemetryLocalization | None = Field(
        default=None, description="定位段（未采集为 null）"
    )
    perception: TelemetryPerception | None = Field(
        default=None, description="感知段（未采集为 null）"
    )
    planning: TelemetryPlanning | None = Field(default=None, description="规划段（未采集为 null）")
    control: TelemetryControl | None = Field(default=None, description="控制段（未采集为 null）")
    system: TelemetrySystem | None = Field(default=None, description="系统段（未采集为 null）")


class TelemetryQueryData(BaseModel):
    """遥测查询数据体（时间区间 + 分页）。"""

    model_config = ConfigDict(extra="forbid")

    items: list[TelemetrySample] = Field(default_factory=list, description="样本列表")
    total: int = Field(ge=0, description="时间区间内样本数")
    page: int = Field(ge=1, description="页码")
    page_size: int = Field(ge=1, le=200, description="每页条数")
    retention_days: int = Field(
        default=TELEMETRY_RETENTION_DAYS,
        description="时序表保留天数（固定 90：contracts/database/ddl/05_timeseries.sql）",
    )


class TelemetryQueryResponse(BaseModel):
    """GET /telemetry 统一响应。"""

    code: int = 0
    message: str = "success"
    data: TelemetryQueryData | None = None
    request_id: str
    timestamp: int
