"""``KafkaLagProbe`` 消费组滞后探测单测（V1.19.14，第七类假绿；V1.21.14 补成因）。

契约要点（``x-hunter-dashboard-contract.blocks.pipeline.lag_semantics``）：
Kafka 的 ``list_consumer_group_offsets`` 对**不存在的组**与**存在但从未提交位点的组**都返回
**空分区列表且不报错**（对拼错的组名同样不抛错）。因此「空位点」在语义上是
**「无法判定」**，绝不能折算成 ``lag=0``——旧实现正是这样把四个不存在的组报成「零积压」。
本文件锁死五件事：未判定组一律进第二个返回值、绝不进映射、映射里永不出现兜底的 0、
第二个返回值必须**带成因码**（只给组名会让看板四个组同显「未判定」，形同「没有数据」）、
以及**结果对象的取值必须与真库同形**（V1.21.15：分区属性是 `topic_partitions` 而非 `partitions`——
按想象取名会让 `partitions` 恒空、每个组都被误判「无提交位点」）。
"""
from __future__ import annotations

from typing import Any

import pytest
from confluent_kafka import OFFSET_INVALID, ConsumerGroupTopicPartitions, TopicPartition

from app.config import Settings
from app.repositories import pipeline as pipeline_mod
from app.repositories.pipeline import UNKNOWN_GROUP_CAUSES, undetermined_cause

_KNOWN = "g-known"
_EMPTY = "g-empty"
_MISSING = "g-missing"
_FETCH_FAIL = "g-fetch-fail"
_NO_WATERMARK = "g-no-watermark"
_ALL_GROUPS = f"{_KNOWN},{_EMPTY},{_MISSING},{_FETCH_FAIL},{_NO_WATERMARK}"


class _Future:
    def __init__(self, value: Any = None, error: Exception | None = None) -> None:
        self._value = value
        self._error = error

    def result(self, timeout: float | None = None) -> Any:
        if self._error is not None:
            raise self._error
        return self._value


class _Listing:
    """对齐 confluent-kafka 2.15 的 ConsumerGroupListing（字段名是 group_id）。"""

    def __init__(self, group_id: str) -> None:
        self.group_id = group_id


class _ListResult:
    """对齐 ListConsumerGroupsResult（.valid / .errors），而不是裸 list。"""

    def __init__(self, valid: list[_Listing], errors: list[Exception] | None = None) -> None:
        self.valid = valid
        self.errors = errors or []


def _Committed(topic: str, partition: int, offset: int) -> TopicPartition:
    """提交位点条目——**直接用真库的 ``TopicPartition``**（属性 topic/partition/offset）。

    V1.21.15 教训：曾用自造类冒充条目、并给结果容器自造 ``partitions`` 属性，与真库
    ``ConsumerGroupTopicPartitions.topic_partitions`` 不同形 ⇒ 单测全绿、线上恒空。
    形状必须来自库本身，不得按读方想象发明字段（同 V1.19.20/21 的 Mock 纪律）。
    """
    return TopicPartition(topic, partition, offset)


def _build(
    monkeypatch: pytest.MonkeyPatch,
    *,
    groups: str,
    offsets: dict[str, Any],
    inventory: list[str] | None,
    watermarks: dict[tuple[str, int], tuple[int, int]] | None = None,
    watermark_fail: set[tuple[str, int]] | None = None,
    inventory_style: str = "result",
    inventory_errors: list[Exception] | None = None,
) -> tuple[pipeline_mod.KafkaLagProbe, dict[str, Any]]:
    """桩掉 confluent-kafka 客户端，跑真实的 ``_lag_sync`` 判定逻辑。

    ``inventory=None`` 表示「集群组清单列不出来」（group 级 ACL / 超时）；
    ``watermark_fail`` 指定按分区抛错的水位探测；``inventory_style="list"`` 用于旧版直接返回
    裸 list 的形状。
    """
    state: dict[str, Any] = {
        "requested": [],
        "inventory": inventory,
        "offsets": offsets,
        "watermarks": watermarks or {},
        "watermark_fail": watermark_fail or set(),
        "inventory_style": inventory_style,
        "inventory_errors": inventory_errors or [],
    }

    class _Admin:
        def __init__(self, conf: dict[str, Any]) -> None:
            self._conf = conf

        def list_consumer_groups(self) -> _Future:
            if state["inventory"] is None:
                raise RuntimeError("group listing not authorized")
            listings = [_Listing(g) for g in state["inventory"]]
            if state["inventory_style"] == "list":
                return _Future(listings)
            return _Future(_ListResult(listings, state["inventory_errors"]))

        def list_consumer_group_offsets(self, targets: list[Any]) -> dict[str, _Future]:
            # confluent-kafka 2.16：单次仅支持一个组（传多组直接抛 ValueError）
            assert len(targets) == 1, "必须逐组调用 list_consumer_group_offsets"
            group = targets[0].group_id
            state["requested"].append(group)
            entry = state["offsets"][group]
            if isinstance(entry, Exception):
                return {group: _Future(error=entry)}
            # 结果对象用真库类型（形状门禁 V1.21.15：分区列表属性名是 topic_partitions，非 partitions）
            return {group: _Future(ConsumerGroupTopicPartitions(group, entry))}

    class _Consumer:
        def __init__(self, conf: dict[str, Any]) -> None:
            self._conf = conf

        def get_watermark_offsets(
            self, tp: Any, timeout: float | None = None, cached: bool = False
        ) -> tuple[int, int]:
            key = (tp.topic, tp.partition)
            state["requested_watermarks"].append(key)
            if key in state["watermark_fail"]:
                raise RuntimeError("partition leader unavailable")
            return state["watermarks"][key]

        def close(self) -> None:
            pass

    state["requested_watermarks"] = []
    monkeypatch.setattr(pipeline_mod, "AdminClient", _Admin)
    monkeypatch.setattr(pipeline_mod, "Consumer", _Consumer)
    return pipeline_mod.KafkaLagProbe(Settings(kafka_consumer_groups=groups)), state


def test_undetermined_groups_never_masquerade_as_zero_lag(monkeypatch: pytest.MonkeyPatch) -> None:
    """五组同测：只有「有位点且水位可读」的组能进映射，其余四类全部记未判定（且带成因码）。"""
    probe, state = _build(
        monkeypatch,
        groups=_ALL_GROUPS,
        offsets={
            _KNOWN: [_Committed("telemetry_clean", 0, 40)],
            _EMPTY: [],  # 组存在但从未提交位点
            _MISSING: [],  # 组压根不存在（清单里没有它）
            _FETCH_FAIL: RuntimeError("offset fetch timed out"),
            _NO_WATERMARK: [_Committed("telemetry_raw", 3, 7)],
        },
        inventory=[_KNOWN, _EMPTY, _FETCH_FAIL, _NO_WATERMARK],
        watermarks={("telemetry_clean", 0): (10, 100), ("telemetry_raw", 3): (0, 50)},
        watermark_fail={("telemetry_raw", 3)},
    )

    lag, unknown = probe._lag_sync()

    assert lag == {_KNOWN: 60}, f"仅确证组可进映射（100-40）：{lag}"
    assert unknown == {
        _EMPTY: "no_committed_offsets",
        _MISSING: "group_not_found",
        _FETCH_FAIL: "offset_fetch_failed",
        _NO_WATERMARK: "watermark_unavailable",
    }, f"四类成因必须各自可辨：{unknown}"
    assert 0 not in lag.values(), "未判定组不得以任何形式折算为 lag=0"
    assert not set(lag) & set(unknown), "映射与未判定清单必须互斥"
    assert state["requested"] == [_KNOWN, _EMPTY, _MISSING, _FETCH_FAIL, _NO_WATERMARK]


def test_group_not_found_is_separated_from_no_committed_offsets(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """成因必须可归因：日志与返回值里 `group_not_found`、`no_committed_offsets` 各就各位。"""
    probe, _ = _build(
        monkeypatch,
        groups=f"{_MISSING},{_EMPTY}",
        offsets={_MISSING: [], _EMPTY: []},
        inventory=[_EMPTY],
    )
    lag, unknown = probe._lag_sync()
    assert lag == {}
    assert unknown == {_MISSING: "group_not_found", _EMPTY: "no_committed_offsets"}
    log_output = capsys.readouterr().out
    assert "group_not_found" in log_output, "「组不存在」必须能在日志里单独认出来"
    assert "no_committed_offsets" in log_output, "「有组无位点」不得与「组不存在」混为一谈"


def test_watermark_unavailable_is_undetermined_not_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    """有位点但一个水位都取不到 → 未判定（旧实现把 total_lag 初值 0 原样写出）。"""
    probe, _ = _build(
        monkeypatch,
        groups=_KNOWN,
        offsets={_KNOWN: [_Committed("telemetry_clean", 0, 40), _Committed("telemetry_clean", 1, 40)]},
        inventory=[_KNOWN],
        watermark_fail={("telemetry_clean", 0), ("telemetry_clean", 1)},
    )
    lag, unknown = probe._lag_sync()
    assert lag == {} and unknown == {_KNOWN: "watermark_unavailable"}


def test_every_emitted_cause_has_controlled_copy(monkeypatch: pytest.MonkeyPatch) -> None:
    """形态门禁：探测侧吐出的每个成因码都必须在受控词表里有「人话」条目（V1.21.14）。

    缺条目时 :func:`undetermined_cause` 会把码原样回显到看板上（英文码当文案），
    这类「新增成因忘了登记文案」的漂移必须在本单测里当场红。
    """
    probe, _ = _build(
        monkeypatch,
        groups=_ALL_GROUPS,
        offsets={
            _KNOWN: [_Committed("telemetry_clean", 0, 40)],
            _EMPTY: [],
            _MISSING: [],
            _FETCH_FAIL: RuntimeError("offset fetch timed out"),
            _NO_WATERMARK: [_Committed("telemetry_raw", 3, 7)],
        },
        inventory=[_KNOWN, _EMPTY, _FETCH_FAIL, _NO_WATERMARK],
        watermarks={("telemetry_clean", 0): (10, 100)},
        watermark_fail={("telemetry_raw", 3)},
    )
    _lag, unknown = probe._lag_sync()
    assert unknown, "本用例必须真出成因码，否则门禁是空跑"
    assert set(unknown.values()) <= set(UNKNOWN_GROUP_CAUSES), (
        f"成因码未登记人话文案：{sorted(set(unknown.values()) - set(UNKNOWN_GROUP_CAUSES))}"
    )
    for cause in unknown.values():
        copy = undetermined_cause(cause)
        assert copy.label and copy.hint, f"{cause} 的文案不得为空"


def test_unknown_cause_code_is_echoed_not_guessed() -> None:
    """未登记的码原样回显：宁可露出英文码，也不给它套一句「大概是零积压」的默认结论。"""
    copy = undetermined_cause("some_new_cause")
    assert copy.label == "some_new_cause" and copy.hint == "some_new_cause"


def test_offset_result_uses_real_confluent_attr_name() -> None:
    """形状门禁（V1.21.15）：结果对象的分区属性是 **``topic_partitions``**，不是 ``partitions``。

    实机确诊的根因：``_lag_sync`` 旧代码读 ``getattr(offsets, "partitions")`` 恒为空 ⇒ 每个组都被
    判成「无提交位点」，而 ``--describe`` 明明有位点。此测试直接对**真库类**断言属性名，
    并验证 ``_lag_sync`` 能从真实结果对象里取到条目——替身再也不能按想象发明字段名。
    """
    result = ConsumerGroupTopicPartitions("g", [TopicPartition("telemetry_clean", 0, 5)])
    assert result.topic_partitions and result.topic_partitions[0].offset == 5
    assert not hasattr(result, "partitions"), (
        "真库结果对象若出现/改名 `partitions`，须同步 `_lag_sync` 的取值与本断言"
    )



def test_partial_watermark_failure_still_reports_measured_lag(monkeypatch: pytest.MonkeyPatch) -> None:
    """多分区只坏一个 → 仍以已测分区求和（不因单分区失败把整组判死）。"""
    probe, state = _build(
        monkeypatch,
        groups=_KNOWN,
        offsets={_KNOWN: [_Committed("telemetry_clean", 0, 40), _Committed("telemetry_clean", 1, 1000)]},
        inventory=[_KNOWN],
        watermarks={("telemetry_clean", 0): (10, 100)},
        watermark_fail={("telemetry_clean", 1)},
    )

    lag, unknown = probe._lag_sync()

    assert lag == {_KNOWN: 60} and unknown == {}  # 仅已测分区计入（100-40），坏分区不拖垂整组
    assert state["requested_watermarks"] == [("telemetry_clean", 0), ("telemetry_clean", 1)]


def test_invalid_committed_offset_uses_low_watermark_base(monkeypatch: pytest.MonkeyPatch) -> None:
    """提交位点为 OFFSET_INVALID（无有效位点）时以 low 为基准，且该组仍属「有位点」。"""
    probe, _ = _build(
        monkeypatch,
        groups=_KNOWN,
        offsets={_KNOWN: [_Committed("telemetry_clean", 0, OFFSET_INVALID)]},
        inventory=[_KNOWN],
        watermarks={("telemetry_clean", 0): (300, 1000)},
    )
    lag, unknown = probe._lag_sync()
    assert lag == {_KNOWN: 700} and unknown == {}


def test_group_inventory_failure_still_marks_empty_offsets_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    """集群组清单列不出来时不得反推「组都不存在」，但空位点仍须记未判定。"""
    probe, _ = _build(
        monkeypatch,
        groups=f"{_KNOWN},{_EMPTY}",
        offsets={_KNOWN: [_Committed("telemetry_clean", 0, 1)], _EMPTY: []},
        inventory=None,  # list_consumer_groups 直接抛
        watermarks={("telemetry_clean", 0): (0, 9)},
    )
    lag, unknown = probe._lag_sync()
    assert lag == {_KNOWN: 8} and unknown == {_EMPTY: "no_committed_offsets"}


@pytest.mark.asyncio
async def test_consumer_lag_returns_pair_not_bare_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    """对外协程返回 ``(映射, 成因映射)``，服务层据此区分「Kafka 不可达」与「读不到位点」。"""
    probe, _ = _build(monkeypatch, groups=_MISSING, offsets={_MISSING: []}, inventory=[])
    assert await probe.consumer_lag() == ({}, {_MISSING: "group_not_found"})


def test_legacy_bare_list_inventory_shape_is_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    """旧版 ``list_consumer_groups`` 直接给裸 list 时也要能读出清单（不因形状差异拒答）。"""
    probe, _ = _build(
        monkeypatch,
        groups=f"{_KNOWN},{_MISSING}",
        offsets={_KNOWN: [_Committed("telemetry_clean", 0, 5)], _MISSING: []},
        inventory=[_KNOWN],
        watermarks={("telemetry_clean", 0): (0, 8)},
        inventory_style="list",
    )
    lag, unknown = probe._lag_sync()
    assert lag == {_KNOWN: 3} and unknown == {_MISSING: "group_not_found"}


def test_partial_inventory_is_refused_rather_than_trusted(monkeypatch: pytest.MonkeyPatch) -> None:
    """清单只返了一部分（``.errors`` 非空）时不得把“没列出”当“不存在”。"""
    probe, _ = _build(
        monkeypatch,
        groups=_EMPTY,
        offsets={_EMPTY: []},
        inventory=[],
        inventory_errors=[RuntimeError("one broker unreachable")],
    )
    lag, unknown = probe._lag_sync()
    assert lag == {} and unknown == {_EMPTY: "no_committed_offsets"}
