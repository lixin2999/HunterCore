"""P0 修复单元测试：任务图依赖序 / VerifyWorker / orchestrator 事务锁 / events 层1去重。"""
from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.consumers.base import BaseIngestConsumer, _coerce_event_id, _parse_occurred_at
from app.models.ingest import ArtifactClass, TaskType
from app.repositories.artifacts import ArtifactRepository
from app.repositories.tasks import TaskRepository
from app.services.orchestrator import Orchestrator
from app.services.task_graph import next_tasks
from app.services.verify_worker import VerifyWorker

# ========== §7.1 task_graph 依赖序 ==========

class TestTaskGraph:
    def test_verify_successors(self) -> None:
        assert set(next_tasks(TaskType.VERIFY)) == {TaskType.ALIGN, TaskType.QC}

    def test_align_successor_slice(self) -> None:
        assert next_tasks(TaskType.ALIGN) == (TaskType.SLICE,)

    def test_terminal_tasks(self) -> None:
        assert next_tasks(TaskType.SLICE) == ()
        assert next_tasks(TaskType.QC) == ()

    def test_enrich_not_in_graph(self) -> None:
        """ENRICH worker 未实现（M4/M5），不得出现在任何后继集合。"""
        for successors in (next_tasks(t) for t in TaskType):
            assert TaskType.ENRICH not in successors


# ========== VerifyWorker（§7.1 verify_assets） ==========

def _artifact(key: str, cls: ArtifactClass, size: int | None = 1024, sha: str | None = None) -> MagicMock:
    a = MagicMock()
    a.object_key = key
    a.artifact_class = cls
    a.size_bytes = size
    a.sha256 = sha
    return a


class TestVerifyWorker:
    @pytest.fixture
    def storage(self) -> MagicMock:
        storage = MagicMock()
        storage.ahead_parsed = AsyncMock()
        return storage

    @pytest.fixture
    def task(self) -> MagicMock:
        t = MagicMock()
        t.bag_id = "bag-001"
        t.input_rev = "rev-abc"
        return t

    async def test_all_verified(self, test_settings, mock_db_mgr, storage, task) -> None:
        artifacts = [_artifact("k1", ArtifactClass.MANIFEST), _artifact("k2", ArtifactClass.TABLE)]
        storage.ahead_parsed.return_value = MagicMock(size=1024, sha256=None)
        with patch.object(ArtifactRepository, "list_by_bag", new=AsyncMock(return_value=artifacts)), \
             patch.object(ArtifactRepository, "set_verified", new=AsyncMock()) as m_verified, \
             patch.object(ArtifactRepository, "set_missing", new=AsyncMock()) as m_missing:
            worker = VerifyWorker(test_settings, storage, mock_db_mgr)
            result = await worker.execute(task)
        assert result["checked"] == 2 and result["verified"] == 2
        assert m_verified.await_count == 2
        m_missing.assert_not_called()

    async def test_required_missing_raises_after_marking(self, test_settings, mock_db_mgr, storage, task) -> None:
        artifacts = [_artifact("k1", ArtifactClass.MANIFEST)]
        storage.ahead_parsed.return_value = None  # 对象不存在
        with patch.object(ArtifactRepository, "list_by_bag", new=AsyncMock(return_value=artifacts)), \
             patch.object(ArtifactRepository, "set_missing", new=AsyncMock()) as m_missing:
            worker = VerifyWorker(test_settings, storage, mock_db_mgr)
            with pytest.raises(ValueError, match="required artifacts"):
                await worker.execute(task)
        m_missing.assert_awaited_once_with("k1")

    async def test_size_mismatch_marks_corrupt(self, test_settings, mock_db_mgr, storage, task) -> None:
        # 非必需类（image）损坏不阻断任务，仅留痕
        artifacts = [_artifact("k2", ArtifactClass.IMAGE, size=2048)]
        storage.ahead_parsed.return_value = MagicMock(size=1024, sha256=None)  # 实际大小不符
        with patch.object(ArtifactRepository, "list_by_bag", new=AsyncMock(return_value=artifacts)), \
             patch.object(ArtifactRepository, "set_corrupt", new=AsyncMock()) as m_corrupt, \
             patch.object(ArtifactRepository, "set_verified", new=AsyncMock()) as m_verified:
            worker = VerifyWorker(test_settings, storage, mock_db_mgr)
            result = await worker.execute(task)
        assert result["corrupt"] == 1
        m_corrupt.assert_awaited_once_with("k2")
        m_verified.assert_not_called()

    async def test_no_artifacts_raises(self, test_settings, mock_db_mgr, storage, task) -> None:
        with patch.object(ArtifactRepository, "list_by_bag", new=AsyncMock(return_value=[])):
            worker = VerifyWorker(test_settings, storage, mock_db_mgr)
            with pytest.raises(ValueError, match="no artifacts"):
                await worker.execute(task)


# ========== Orchestrator：事务锁仲裁 + 任务图推进 ==========

def _lock_result(acquired: bool) -> MagicMock:
    res = MagicMock()
    res.scalar.return_value = acquired
    return res


def _make_orchestrator(test_settings, mock_db_mgr) -> tuple[Orchestrator, dict[str, MagicMock]]:
    workers = {}
    for ttype in (TaskType.VERIFY, TaskType.ALIGN, TaskType.SLICE, TaskType.QC):
        w = MagicMock()
        w.execute = AsyncMock(return_value={"ok": 1})
        workers[ttype] = w
    orch = Orchestrator(
        settings=test_settings,
        db_session=mock_db_mgr.session_factory,
        verify_worker=workers[TaskType.VERIFY],
        align_worker=workers[TaskType.ALIGN],
        slice_worker=workers[TaskType.SLICE],
        qc_worker=workers[TaskType.QC],
    )
    return orch, workers


def _claimed_task(task_type: TaskType) -> MagicMock:
    t = MagicMock()
    t.task_id = 42
    t.task_type = task_type
    t.bag_id = "bag-001"
    t.input_rev = "rev-abc"
    return t


class TestOrchestrator:
    async def test_not_leader_round_does_not_claim(self, test_settings, mock_session) -> None:
        """session.execute 返回 False（未抢到锁）→ 不调用 claim_next。"""
        mgr = MagicMock()
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def _cm():
            yield mock_session

        mgr.session_factory = MagicMock(side_effect=lambda: _cm())
        mock_session.execute.return_value = _lock_result(False)
        orch, _ = _make_orchestrator(test_settings, mgr)
        with patch.object(TaskRepository, "claim_next", new=AsyncMock()) as m_claim:
            await orch._process_one_task()
        m_claim.assert_not_called()
        assert orch._is_leader is False

    async def test_verify_done_creates_successors_only(self, test_settings, mock_session) -> None:
        """verify 完成 → 同事务创建 align+qc 后继；worker 收到任务。"""
        from contextlib import asynccontextmanager

        mgr = MagicMock()

        @asynccontextmanager
        async def _cm():
            yield mock_session

        mgr.session_factory = MagicMock(side_effect=lambda: _cm())
        mock_session.execute.return_value = _lock_result(True)
        task = _claimed_task(TaskType.VERIFY)
        orch, workers = _make_orchestrator(test_settings, mgr)
        with patch.object(TaskRepository, "claim_next", new=AsyncMock(return_value=task)), \
             patch.object(TaskRepository, "complete", new=AsyncMock()) as m_complete, \
             patch.object(TaskRepository, "create", new=AsyncMock(return_value=True)) as m_create:
            await orch._process_one_task()
        workers[TaskType.VERIFY].execute.assert_awaited_once_with(task)
        m_complete.assert_awaited()
        created_types = {c.args[2] for c in m_create.await_args_list}
        assert created_types == {TaskType.ALIGN, TaskType.QC}

    async def test_qc_done_creates_no_successors(self, test_settings, mock_session) -> None:
        from contextlib import asynccontextmanager

        mgr = MagicMock()

        @asynccontextmanager
        async def _cm():
            yield mock_session

        mgr.session_factory = MagicMock(side_effect=lambda: _cm())
        mock_session.execute.return_value = _lock_result(True)
        task = _claimed_task(TaskType.QC)
        orch, _ = _make_orchestrator(test_settings, mgr)
        with patch.object(TaskRepository, "claim_next", new=AsyncMock(return_value=task)), \
             patch.object(TaskRepository, "complete", new=AsyncMock()), \
             patch.object(TaskRepository, "create", new=AsyncMock(return_value=True)) as m_create:
            await orch._process_one_task()
        m_create.assert_not_called()

    async def test_no_worker_fails_task(self, test_settings, mock_session) -> None:
        """未注册 worker 的任务类型（COMPACT）→ fail(NO_WORKER)，不执行不推进。"""
        from contextlib import asynccontextmanager

        mgr = MagicMock()

        @asynccontextmanager
        async def _cm():
            yield mock_session

        mgr.session_factory = MagicMock(side_effect=lambda: _cm())
        mock_session.execute.return_value = _lock_result(True)
        task = _claimed_task(TaskType.COMPACT)
        orch, _ = _make_orchestrator(test_settings, mgr)
        with patch.object(TaskRepository, "claim_next", new=AsyncMock(return_value=task)), \
             patch.object(TaskRepository, "fail", new=AsyncMock()) as m_fail:
            await orch._process_one_task()
        assert m_fail.await_args.kwargs.get("error_code") == "NO_WORKER"

    def test_export_worker_registered_when_provided(self, test_settings, mock_db_mgr) -> None:
        """注入 export_worker → TaskType.EXPORT 进入 _workers（审查 #9）。"""
        from contextlib import asynccontextmanager

        workers = {}
        for ttype in (TaskType.VERIFY, TaskType.ALIGN, TaskType.SLICE, TaskType.QC):
            w = MagicMock()
            w.execute = AsyncMock(return_value={"ok": 1})
            workers[ttype] = w
        export_w = MagicMock()
        export_w.execute = AsyncMock(return_value={"ok": 1})

        @asynccontextmanager
        async def _cm():
            yield mock_db_mgr.session_factory()

        orch = Orchestrator(
            settings=test_settings,
            db_session=mock_db_mgr.session_factory,
            verify_worker=workers[TaskType.VERIFY],
            align_worker=workers[TaskType.ALIGN],
            slice_worker=workers[TaskType.SLICE],
            qc_worker=workers[TaskType.QC],
            export_worker=export_w,
        )
        assert TaskType.EXPORT in orch._workers
        assert TaskType.COMPACT not in orch._workers

    async def test_process_round_claims_batch_and_runs_concurrently(self, test_settings, mock_session) -> None:
        """#20 并发轮：锁事务内批量领取→锁外 gather 并发执行。"""
        from contextlib import asynccontextmanager

        mgr = MagicMock()

        @asynccontextmanager
        async def _cm():
            yield mock_session

        mgr.session_factory = MagicMock(side_effect=lambda: _cm())
        mock_session.execute.return_value = _lock_result(True)
        orch, workers = _make_orchestrator(test_settings, mgr)
        batch = [_claimed_task(TaskType.ALIGN), _claimed_task(TaskType.QC)]
        with patch.object(TaskRepository, "claim_next_batch", new=AsyncMock(return_value=batch)) as m_batch, \
             patch.object(TaskRepository, "complete", new=AsyncMock()), \
             patch.object(TaskRepository, "create", new=AsyncMock(return_value=True)):
            await orch._process_round()
        m_batch.assert_awaited()
        workers[TaskType.ALIGN].execute.assert_awaited()
        workers[TaskType.QC].execute.assert_awaited()


# ========== 层1 events 去重（BaseIngestConsumer._handle） ==========

class _EchoConsumer(BaseIngestConsumer):
    group_id = "g.test.echo"
    subscribe_topic = "test.topic"

    def __init__(self, settings: Any, db_mgr: Any) -> None:
        super().__init__(settings, db_mgr)
        self.processed: list[dict[str, Any]] = []

    async def process(self, payload: dict[str, Any], *, topic: str) -> None:
        self.processed.append(payload)


def _kafka_message(topic: str = "test.topic", partition: int = 1, offset: int = 99) -> MagicMock:
    msg = MagicMock()
    msg.topic.return_value = topic
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    return msg


class TestEventDedup:
    @pytest.fixture
    def consumer(self, test_settings, mock_db_mgr) -> _EchoConsumer:
        with patch("app.consumers.base.KafkaConsumerManager"):
            return _EchoConsumer(test_settings, mock_db_mgr)

    async def test_new_event_processed(self, consumer, test_settings) -> None:
        value = {"event_id": "550e8400-e29b-41d4-a716-446655440000", "vehicle_id": "veh-1", "seq": 3}
        with patch(
            "app.consumers.base.IngestEventRepository.insert_dedup", new=AsyncMock(return_value=True)
        ):
            await consumer._handle(_kafka_message(), value)
        assert consumer.processed == [value]

    async def test_duplicate_event_skipped(self, consumer) -> None:
        value = {"event_id": "550e8400-e29b-41d4-a716-446655440000", "vehicle_id": "veh-1"}
        with patch(
            "app.consumers.base.IngestEventRepository.insert_dedup", new=AsyncMock(return_value=False)
        ):
            await consumer._handle(_kafka_message(), value)
        assert consumer.processed == []

    async def test_process_error_rolls_back_event_row(self, consumer, mock_db_mgr) -> None:
        """process 抛错 → 事件行回滚（不提交），异常上抛交 Kafka 重试。"""
        consumer.process = AsyncMock(side_effect=RuntimeError("boom"))  # type: ignore[method-assign]
        value = {"event_id": "550e8400-e29b-41d4-a716-446655440000", "vehicle_id": "veh-1"}
        with patch(
            "app.consumers.base.IngestEventRepository.insert_dedup", new=AsyncMock(return_value=True)
        ), pytest.raises(RuntimeError):
            await consumer._handle(_kafka_message(), value)

    def test_event_row_fields(self, consumer) -> None:
        row = consumer._event_row(_kafka_message(), {
            "event_id": "550e8400-e29b-41d4-a716-446655440000",
            "event_type": "bag.manifest.v1",
            "bag_id": "bag-001",
            "vehicle_id": "veh-A1",
            "seq": 7,
            "occurred_at": "2026-01-01T00:00:00Z",
            "payload": {},
        })
        assert str(row["event_id"]) == "550e8400-e29b-41d4-a716-446655440000"
        assert row["event_type"] == "bag.manifest.v1"
        assert row["bag_id"] == "bag-001"
        assert row["seq"] == 7
        assert row["kafka_partition"] == 1 and row["kafka_offset"] == 99
        assert row["origin"] == "vehicle"

    def test_coerce_event_id_deterministic_fallback(self) -> None:
        from uuid import UUID as U
        bad = _coerce_event_id("evt-bag-001-manifest-1", "t", 0, 5)
        again = _coerce_event_id("evt-bag-001-manifest-1", "t", 0, 5)
        assert isinstance(bad, U) and bad == again
        good = _coerce_event_id("550e8400-e29b-41d4-a716-446655440000", "t", 0, 5)
        assert str(good) == "550e8400-e29b-41d4-a716-446655440000"

    def test_parse_occurred_at_variants(self) -> None:
        dt = _parse_occurred_at("2026-01-01T00:00:00Z")
        assert dt.tzinfo is not None
        assert dt == _parse_occurred_at(dt.timestamp())
        naive = _parse_occurred_at(None)
        assert naive.tzinfo is not None


# ========== projection：就绪只创建 VERIFY ==========

class TestProjectionFirstNode:
    async def test_only_verify_created(self, test_settings, mock_db_mgr) -> None:
        from app.services.projection import ProjectionService

        projection = ProjectionService(test_settings, mock_db_mgr)
        created: list[TaskType] = []

        async def _fake_create(bag_id: str, input_rev: str, task_type: TaskType, **kw: Any) -> bool:
            created.append(task_type)
            return True

        bag = MagicMock()
        bag.duration_s = 10.0
        with patch("app.services.projection.BagRepository") as m_bag, \
             patch("app.services.projection.BagTopicRepository"), \
             patch("app.services.projection.ArtifactRepository") as m_art, \
             patch("app.services.projection.TaskRepository") as m_task, \
             patch("app.services.projection.ProjectionAnomalyRepository"):
            m_bag.return_value.get_by_id = AsyncMock(return_value=bag)
            m_bag.return_value.upsert_from_manifest = AsyncMock()
            m_bag.return_value.project_state = AsyncMock(return_value=True)
            m_art.return_value.count_by_bag_class = AsyncMock(
                return_value={ArtifactClass.MANIFEST: 1, ArtifactClass.TABLE: 2}
            )
            m_task.return_value.create = _fake_create
            await projection.handle_manifest({
                "event_id": "550e8400-e29b-41d4-a716-446655440000",
                "bag_id": "bag-001",
                "vehicle_id": "veh-A1",
                "seq": 1,
                "payload": {"sha256": "a" * 64, "duration_s": 10.0, "topics": []},
            })
        assert created == [TaskType.VERIFY]
