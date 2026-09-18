"""评估文档仓库：MinIO 指针文档 + 各类最新评估文档（契约 x-hunter-minio-eval-documents）。

布局：hunter-reports/eval_documents/latest.json（指针）→ 各 kind 对象键
kind ∈ {perception, control, scene_coverage, corner_cases}（由 Flink/Spark 作业写入）。
"""
from __future__ import annotations

from typing import Any

from hunter_common.logging import get_logger

from app.repositories.storage import ObjectStorage

logger = get_logger("app.repositories.eval_documents")

#: 评估文档受控类别（契约指针键，不可新增）
EVAL_DOCUMENT_KINDS: tuple[str, ...] = ("perception", "control", "scene_coverage", "corner_cases")


class EvalDocumentRepository:
    """评估文档仓储（只读；写入方为 Flink/Spark 分析作业）。"""

    def __init__(self, storage: ObjectStorage, *, pointer_key: str = "eval_documents/latest.json") -> None:
        self._storage = storage
        self._pointer_key = pointer_key

    async def load_pointer(self) -> dict[str, Any] | None:
        """读取 latest.json 指针（kind → 对象键）；未生成返回 None。"""
        return await self._storage.get_json(self._pointer_key)

    async def load(self, kind: str) -> dict[str, Any] | None:
        """按类别读取最近一次评估文档；指针/文档缺失返回 None。"""
        pointer = await self.load_pointer()
        if pointer is None:
            logger.warning("eval_pointer_missing", pointer_key=self._pointer_key)
            return None
        object_key = pointer.get(kind)
        if not object_key or not isinstance(object_key, str):
            logger.warning("eval_document_key_missing", kind=kind)
            return None
        doc = await self._storage.get_json(object_key)
        if doc is None:
            logger.warning("eval_document_missing", kind=kind, object_key=object_key)
        return doc