"""分析服务共享 Schema：评估窗口 / 车辆 ID 受控模式。"""
from __future__ import annotations

from pydantic import BaseModel, Field, model_validator

#: 车辆 ID 受控模式（与遥测/事件/报告契约一致，禁止放宽）
VEHICLE_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$"


class EvalWindow(BaseModel):
    """评估统计窗口（components.schemas.EvalWindow）。"""

    start_time: float = Field(ge=0, description="窗口起点（Unix epoch 秒）")
    end_time: float = Field(ge=0, description="窗口终点（Unix epoch 秒）")
    duration_hours: float | None = Field(default=None, description="窗口时长（小时）")

    @model_validator(mode="after")
    def _fill_duration(self) -> EvalWindow:
        """duration_hours 未提供时按窗口差值推导（契约示例包含该字段）。"""
        if self.duration_hours is None:
            self.duration_hours = (self.end_time - self.start_time) / 3600.0
        return self