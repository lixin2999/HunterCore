"""hunter.*.command_result 消费者（G-09 WS ack 下行；消费组 remote-control-command-result）。

处理链路（契约 x-hunter-kafka.consumes.command_result + pending #16 近似关联）：
1. Schema 校验（command_result.schema.json 子集：vehicle_id/command_id/timestamp/success）；
2. ``rc:session:{vehicle_id}`` 直读定位会话——无活跃会话 = 非操控类指令回执（boot/stop 之外的
   历史指令），计数后丢弃；
3. ``commands_acked`` HINCRBY 回写（REST GET /session 统计口径一致）；
4. 本副本存在 control 连接且已下发过指令（``last_sent_at`` 锚点）→ 按**时序近似**计算
   链路时延并推送 ack 帧（seq 取最近下发序号；禁止逐条 command_id 精确关联，pending #16）；
5. 回执在他副本（会话粘性路由竞态/已断开）→ 仅统计，不推送。

手动提交 offset + 失败重试进 DLQ 由 KafkaConsumerManager 内置（at-least-once + 幂等计数容忍）。
"""
from __future__ import annotations

import time
from typing import Any

from confluent_kafka import Message
from hunter_common.kafka.consumer import KafkaConsumerManager
from hunter_common.logging import get_logger, set_vehicle_id
from pydantic import BaseModel, ConfigDict, ValidationError

from app.config import Settings
from app.schemas.websocket import ack_frame
from app.services.metrics import COMMAND_LATENCY_SECONDS, COMMANDS_TOTAL
from app.services.session_service import SessionService
from app.services.ws_hub import WsHub

logger = get_logger("app.consumers.command_result")


class CommandResultMessage(BaseModel):
    """command_result 消息模型（contracts/kafka/schemas/command_result.schema.json 子集）。"""

    model_config = ConfigDict(extra="ignore")

    command_id: str
    vehicle_id: str
    timestamp: float
    success: bool
    result_code: str | None = None
    message: str | None = None


class CommandResultConsumer:
    """指令回执消费者（ack 帧下行 + 会话统计回写 + 链路时延观测）。"""

    def __init__(
        self,
        settings: Settings,
        session_service: SessionService,
        hub: WsHub,
    ) -> None:
        self._settings = settings
        self._service = session_service
        self._hub = hub
        self._consumer = KafkaConsumerManager(
            settings,
            group_id=settings.rc_command_result_group_id,
            topics=[settings.rc_command_result_subscribe_pattern],
        )

    async def run(self) -> None:
        """消费主循环（KafkaConsumerManager 手动提交 + DLQ）。"""
        await self._consumer.run(self._handle)

    async def _handle(self, message: Message, value: Any) -> None:
        """单条消息处理（抛异常 → KafkaConsumerManager 重试后转投 DLQ）。"""
        if not isinstance(value, dict):
            raise TypeError(
                f"command_result 消息必须为 JSON 对象，实际 {type(value).__name__}"
            )
        try:
            payload = CommandResultMessage.model_validate(value)
        except ValidationError as exc:
            raise ValueError(
                f"command_result 消息结构非法（进 DLQ）：{exc.errors()[:3]}"
            ) from exc
        token = set_vehicle_id(payload.vehicle_id)
        try:
            await self._process(payload)
        finally:
            set_vehicle_id(token)  # 覆盖式设置：消费循环协程隔离由 contextvars 保证

    async def _process(self, payload: CommandResultMessage) -> None:
        """回执落账：会话统计 + 本副本连接 ack 帧推送（近似关联）。"""
        vehicle_id = payload.vehicle_id
        mapping = await self._service.get_session_mapping(vehicle_id)
        session_id = (mapping or {}).get("session_id") or None
        if session_id is None:
            # 非操控会话类指令回执（或会话已结束）：不推送，仅记录观测
            logger.debug(
                "command_result_no_session",
                vehicle_id=vehicle_id,
                command_id=payload.command_id,
            )
            return
        acked = await self._service.record_commands_acked(vehicle_id)
        if acked is None:
            return  # 会话在读取间隙结束（竞态容忍）

        conn = self._hub.get_control(session_id)
        if conn is None:
            # 回执落在他副本（粘性路由竞态）或连接已断开：统计已回写，跳过推送
            return
        if conn.last_sent_at is None:
            # 尚未向车端下发过 control 帧（回执来自 start/信令等非指令帧）：无可关联锚点
            return
        latency_s = max(time.time() - conn.last_sent_at, 0.0)
        conn.latency_samples.append(latency_s * 1000)
        conn.last_ack_at = time.time()
        COMMAND_LATENCY_SECONDS.observe(latency_s)
        COMMANDS_TOTAL.labels(result="acked").inc()

        vehicle_state = await self._service.vehicle_status_snapshot(vehicle_id)
        await self._hub.send_json(
            conn.websocket,
            ack_frame(
                session_id,
                conn.last_sent_seq,
                latency_s * 1000,
                vehicle_state,
            ),
        )
        if not payload.success:
            logger.info(
                "command_result_failed",
                vehicle_id=vehicle_id,
                session_id=session_id,
                command_id=payload.command_id,
                result_code=payload.result_code,
            )


__all__ = ["CommandResultConsumer", "CommandResultMessage"]
