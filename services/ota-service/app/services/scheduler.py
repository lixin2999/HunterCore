"""灰度自动调度器（x-hunter-canary-rollout.scheduler）。

后台协程周期驱动 :meth:`TaskService.scheduler_tick`：到点启动 scheduled 任务、
推进满足「观察 24h 且 成功率 ≥ 95%」的批次、成功率不达标置 paused、末批达标置 succeeded。

并发安全（契约 §scheduler.concurrency_control）：
- 推进以 ``SELECT ... FOR UPDATE``（ota_tasks 行锁）+ 状态机校验保证单任务只被推进一次；
- **不新增 Redis key**（Redis 键命名受契约约束）；
- 多副本部署时各副本 tick 幂等竞争同一行锁，仅一个成功推进，其余读到状态已变更即跳过。

单机 compose 形态下同样运行（受控降级，G-13① 书面豁免）；测试环境经
``ROLLOUT_SCHEDULER_ENABLED=false`` 关闭，由测试直接调用 ``scheduler_tick`` 验证逻辑。
"""
from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from hunter_common.logging import get_logger

if TYPE_CHECKING:
    from app.config import Settings
    from app.services.tasks import TaskService

logger = get_logger("app.services.rollout_scheduler")

#: 扫描周期下限（防误配 0/负值忙轮询；非灰度门禁阈值，仅保护 CPU）
_MIN_TICK_SECONDS = 1


class RolloutScheduler:
    """灰度推进后台循环（单任务失败不终止循环；协程 cancel 即优雅退出）。"""

    def __init__(self, task_service: TaskService, settings: Settings) -> None:
        self._svc = task_service
        self._tick = max(_MIN_TICK_SECONDS, settings.ota_rollout_tick_seconds)

    async def run(self) -> None:
        """调度主循环：每 tick 执行一轮 :meth:`scheduler_tick`（异常已就地记录）。"""
        logger.info("rollout_scheduler_started", tick_seconds=self._tick)
        try:
            while True:
                try:
                    counters = await self._svc.scheduler_tick()
                    if any(counters.values()):
                        logger.info("rollout_scheduler_tick", **counters)
                except asyncio.CancelledError:
                    raise
                except Exception:  # 单轮失败必须容错，保持循环存活
                    logger.exception("rollout_scheduler_tick_failed")
                await asyncio.sleep(self._tick)
        except asyncio.CancelledError:
            logger.info("rollout_scheduler_stopped")
            raise


__all__ = ["RolloutScheduler"]
