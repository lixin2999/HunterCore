"""一维运动学模型（速度/转角/挡位积分 → 车速、航向、位置、电量）。

设计取舍（演示/自测导向，非高保真动力学）：
- 纵向：速度一阶惯性逼近目标车速（``target_velocity``），``brake`` 比例减速；
- 转向：前轮转角经简易自行车模型换算航向角速度（``angular_velocity_z``），位置按航向积分；
- 电量：随行驶/ idle 缓慢衰减（用于看板 SOC 进度条与低电量事件演示）；
- 安全默认：>500ms 未收到控制帧自动减速停车（契约 remote_control 安全约束，系统约束第 15 条）。

所有量纲与 telemetry.schema.json 对齐（速度 m/s、转角 rad、航向 [-π,π] rad、位置 m）。
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field


@dataclass
class ControlTarget:
    """当前生效的控制目标（来自最近一帧 remote_control.control）。"""

    target_velocity: float = 0.0
    target_steer: float = 0.0
    brake: float = 0.0
    gear: str | None = None
    mode: str | None = None


@dataclass
class KinematicsConfig:
    """运动学可调参数（演示默认值；均可经 CLI 覆盖关键项）。"""

    accel_rate: float = 1.5          # 速度跟踪带宽 1/s（越大越接近目标）
    max_accel_mps2: float = 1.0      # 加速度上限 m/s²
    max_decel_mps2: float = 2.5      # 制动减速度上限 m/s²
    wheelbase_m: float = 3.0         # 轴距（自行车模型）
    steer_max_rad: float = 0.6       # 前轮最大转角（保护上限）
    max_speed_mps: float = 2.0       # 远程操控限速（系统约束第 15 条，不可上调）
    battery_capacity_ah: float = 100.0
    idle_drain_ahps: float = 0.0009  # 停车电流衰减（Ah/s）
    drive_drain_ahps: float = 0.004  # 行驶电流衰减系数（×|v|）
    estop_timeout_s: float = 0.5     # >500ms 无控制帧 → 自动减速停车


@dataclass
class VehicleState:
    """运动学状态（遥测/健康的单一真相）。"""

    velocity: float = 0.0            # m/s（前进为正）
    steer: float = 0.0               # 前轮转角 rad
    heading: float = 0.0             # 航向 rad [-π,π]
    x: float = 0.0                   # 地图坐标 m
    y: float = 0.0
    z: float = 0.0
    battery_soc: int = 80            # 0-100 %
    gear: str = "N"
    angular_velocity_z: float = 0.0  # 航向角速度 rad/s
    last_command_at: float | None = field(default=None, repr=False)
    control_enabled: bool = False    # 是否处于远程操控会话（boot 后 true）

    @property
    def battery_voltage(self) -> float:
        """标称电压随 SOC 近似线性（24.0~25.6V，演示用）。"""
        return 24.0 + 1.6 * (self.battery_soc / 100.0)


class KinematicModel:
    """一维运动学积分器（线程安全：仅主循环单线程 step，控制帧经 set_target 原子更新）。"""

    def __init__(self, cfg: KinematicsConfig | None = None, state: VehicleState | None = None) -> None:
        self.cfg = cfg or KinematicsConfig()
        self.state = state or VehicleState()
        self.target = ControlTarget()

    def apply_control(self, control: dict, *, now: float, heartbeat: bool = False) -> None:
        """应用一帧 remote_control.control（限幅到契约/安全上限；记录锚点用于超时保护）。"""
        target_velocity = float(control.get("target_velocity", 0.0) or 0.0)
        target_steer = float(control.get("target_steer", 0.0) or 0.0)
        self.target.target_velocity = max(-self.cfg.max_speed_mps, min(self.cfg.max_speed_mps, target_velocity))
        self.target.target_steer = max(-self.cfg.steer_max_rad, min(self.cfg.steer_max_rad, target_steer))
        brake = control.get("brake")
        self.target.brake = max(0.0, min(1.0, float(brake))) if brake is not None else 0.0
        gear = control.get("gear")
        if gear in ("P", "R", "N", "D"):
            self.target.gear = gear
            self.state.gear = gear
        self.target.mode = control.get("mode")
        # heartbeat 帧不改变控制目标（保持上一帧），仅刷新超时锚点
        if not heartbeat:
            self.state.control_enabled = True
        self.state.last_command_at = now

    def set_session(self, *, active: bool) -> None:
        """boot → active=True（进入远程操控态）；stop → active=False 并清空目标（归零减速）。"""
        self.state.control_enabled = active
        if not active:
            self.target = ControlTarget()
            self.state.last_command_at = None

    def step(self, dt: float, *, now: float) -> VehicleState:
        """按时间步长 dt 积分一帧（遥测/健康循环调用）。"""
        if dt <= 0:
            return self.state

        # 超时保护：>500ms 无控制帧 → 视目标为零速 + 全制动（车端自动减速停车）
        stale = (
            self.state.last_command_at is not None
            and (now - self.state.last_command_at) > self.cfg.estop_timeout_s
        )
        tgt_v = 0.0 if (stale or not self.state.control_enabled) else self.target.target_velocity
        brake = self.target.brake if not stale else 1.0
        steer = self.target.target_steer if not stale else self.state.steer

        # 转角一阶逼近目标（打舵速率上限 ~0.5 rad/s）
        steer_delta = max(-0.5 * dt, min(0.5 * dt, steer - self.state.steer))
        self.state.steer += steer_delta

        # 纵向：制动优先于驱动；否则速度向目标一阶逼近（受加/减速度上限约束）
        if brake > 0 or tgt_v < self.state.velocity:
            decel = self.cfg.max_decel_mps2 * (1.0 if brake >= 1.0 else max(brake, 0.2))
            dv = -decel * dt if tgt_v <= self.state.velocity else decel * dt
        else:
            raw = (tgt_v - self.state.velocity) * min(1.0, self.cfg.accel_rate * dt)
            dv = max(-self.cfg.max_accel_mps2 * dt, min(self.cfg.max_accel_mps2 * dt, raw))
        self.state.velocity += dv
        if tgt_v == 0.0 and abs(self.state.velocity) < 0.02:
            self.state.velocity = 0.0

        # 自行车模型：ω = v/L·tan(δ)
        self.state.angular_velocity_z = (self.state.velocity / self.cfg.wheelbase_m) * math.tan(self.state.steer)
        self.state.heading = _wrap_pi(self.state.heading + self.state.angular_velocity_z * dt)

        # 位置积分（沿航向）
        self.state.x += self.state.velocity * math.cos(self.state.heading) * dt
        self.state.y += self.state.velocity * math.sin(self.state.heading) * dt

        # 电量衰减（行驶 ∝|v|，停车 idle）
        drain = self.cfg.drive_drain_ahps * abs(self.state.velocity) + self.cfg.idle_drain_ahps
        soc_drop = (drain * dt) / self.cfg.battery_capacity_ah * 100.0
        self.state.battery_soc = max(0, round(self.state.battery_soc - soc_drop))
        return self.state


def _wrap_pi(angle: float) -> float:
    """航向角归一化到 [-π, π]（契约 heading 值域）。"""
    return (angle + math.pi) % (2 * math.pi) - math.pi
