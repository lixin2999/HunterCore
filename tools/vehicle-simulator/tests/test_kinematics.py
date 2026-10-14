"""运动学模型单测（速度逼近、超时停车、航向归一、电量衰减、限幅）。"""

from __future__ import annotations

import math

from vehicle_simulator.kinematics import KinematicModel, KinematicsConfig


def test_velocity_approaches_target_within_limit():
    model = KinematicModel()
    model.apply_control({"target_velocity": 2.0, "target_steer": 0.0}, now=0.0)
    now = 0.0
    for _ in range(30):
        now += 0.1
        model.step(0.1, now=now)
        model.apply_control({"target_velocity": 2.0, "target_steer": 0.0}, now=now)
    assert 0.0 < model.state.velocity <= 2.0
    assert model.state.velocity > 0.5  # 收敛到接近目标


def test_control_target_clamped_to_max_speed():
    model = KinematicModel()
    model.apply_control({"target_velocity": 9.9, "target_steer": 5.0}, now=0.0)
    assert model.target.target_velocity == 2.0  # 系统约束第 15 条限速
    assert abs(model.target.target_steer) <= model.cfg.steer_max_rad


def test_estop_after_command_timeout():
    model = KinematicModel(KinematicsConfig(estop_timeout_s=0.5))
    model.apply_control({"target_velocity": 2.0, "target_steer": 0.0}, now=0.0)
    model.step(0.1, now=0.1)  # 未超时，加速
    v_before = model.state.velocity
    # 500ms 无控制帧 → 目标归零 + 全制动
    for i in range(1, 6):
        model.step(0.1, now=0.5 + i * 0.1)
    assert model.state.velocity < v_before


def test_heading_stays_wrapped():
    model = KinematicModel()
    now = 0.0
    for _ in range(2000):
        now += 0.1
        model.apply_control({"target_velocity": 1.0, "target_steer": 0.4}, now=now)
        model.step(0.1, now=now)
    assert -math.pi - 1e-9 <= model.state.heading <= math.pi + 1e-9


def test_battery_soc_decays():
    model = KinematicModel()
    model.state.battery_soc = 80
    model.apply_control({"target_velocity": 2.0, "target_steer": 0.0}, now=0.0)
    now = 0.0
    for _ in range(200):
        now += 0.1
        model.apply_control({"target_velocity": 2.0, "target_steer": 0.0}, now=now)
        model.step(0.1, now=now)
    assert model.state.battery_soc <= 80


def test_session_stop_clears_control():
    model = KinematicModel()
    model.apply_control({"target_velocity": 2.0, "target_steer": 0.1}, now=0.0)
    model.set_session(active=False)
    assert model.state.control_enabled is False
    assert model.target.target_velocity == 0.0
