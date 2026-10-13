"""任务图依赖序（§7.1 task_graph：verify_assets → align/qc → slice）。

设计文档 §7.1 模板：
    verify_assets: {after: ready}
    align:         {after: verify_assets, needs: [images, tables, tf]}
    qc:            {after: verify_assets}          # 与 align 并行
    slice_event:   {after: align}
    slice_window:  {after: align}
    enrich_traj:   {after: align, optional: true}  # M4/M5 实现，worker 未接入前不创建

就绪时（projection）仅创建首节点 VERIFY；每个任务 done 后由 orchestrator
调用 :func:`next_tasks` + TaskRepository.create 推进后继（层3 唯一键幂等）。
后继仅派发给已注册 worker 的任务类型，避免 NO_WORKER 死信。
"""
from __future__ import annotations

from app.models.ingest import TaskType

#: 完成 → 后继任务集合（§7.1 依赖序；ENRICH 为 optional 项，worker 未实现故不在图内）
_SUCCESSORS: dict[TaskType, tuple[TaskType, ...]] = {
    TaskType.VERIFY: (TaskType.ALIGN, TaskType.QC),
    TaskType.ALIGN: (TaskType.SLICE,),
}


def next_tasks(completed_type: TaskType) -> tuple[TaskType, ...]:
    """返回某类任务完成后的后继任务类型（无后继则空元组）。"""
    return _SUCCESSORS.get(completed_type, ())


__all__ = ["next_tasks"]
