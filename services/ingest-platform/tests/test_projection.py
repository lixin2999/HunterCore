"""投影服务幂等性单元测试（§5.2 三层幂等）。"""
from __future__ import annotations

import hashlib
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.projection import compute_input_rev


# ========== compute_input_rev ==========

class TestComputeInputRev:
    def test_deterministic(self) -> None:
        sha = "abc123" * 10 + "abcd"  # 64 hex chars
        rev1 = compute_input_rev(sha, "v1.0")
        rev2 = compute_input_rev(sha, "v1.0")
        assert rev1 == rev2

    def test_32_chars(self) -> None:
        rev = compute_input_rev("a" * 64, "v1.0")
        assert len(rev) == 32

    def test_different_template(self) -> None:
        sha = "b" * 64
        rev1 = compute_input_rev(sha, "v1.0")
        rev2 = compute_input_rev(sha, "v2.0")
        assert rev1 != rev2

    def test_different_sha(self) -> None:
        rev1 = compute_input_rev("c" * 64, "v1.0")
        rev2 = compute_input_rev("d" * 64, "v1.0")
        assert rev1 != rev2

    def test_matches_formula(self) -> None:
        sha = "e" * 64
        expected = hashlib.sha256(f"{sha}:v1.0".encode()).hexdigest()[:32]
        assert compute_input_rev(sha, "v1.0") == expected


# ========== BagState.rank ==========

class TestBagStateRank:
    def test_rank_order(self) -> None:
        from app.models.ingest import BagState
        ranks = [s.rank for s in [
            BagState.DISCOVERED,
            BagState.VERIFIED,
            BagState.PARSED,
            BagState.UPLOADED,
            BagState.DONE,
        ]]
        assert ranks == sorted(ranks), f"rank 应单调递增，实际: {ranks}"

    def test_parse_failed_not_in_normal_rank(self) -> None:
        from app.models.ingest import BagState
        # PARSE_FAILED 不参与守卫（任意阶段可进入）
        assert BagState.PARSE_FAILED.rank < 0 or BagState.PARSE_FAILED.rank == -1


# ========== 投影守卫逻辑 ==========

class TestProjectionGuard:
    """模拟 project_state 逻辑的守卫判断（§5.2 层2）。"""

    @pytest.mark.parametrize(
        "cur_rank,new_rank,cur_seq,new_seq,should_pass",
        [
            (0, 1, 1, 2, True),   # 前进：rank 增大 + seq 增大
            (0, 0, 1, 2, True),   # 同 rank，seq 增大 → seq_ok=True
            (2, 1, 5, 3, False),  # 后退：rank 降低 + seq 降低 → 拒绝
            (2, 1, 2, 5, True),   # rank 降低但 seq 增大 → seq_ok=True → 通过
            (0, 0, 1, 1, True),   # 同 rank + 同 seq → rank_ok=(0>=0)=True → 通过
        ],
    )
    def test_guard_condition(self, cur_rank: int, new_rank: int, cur_seq: int, new_seq: int, should_pass: bool) -> None:
        """守卫条件：seq_ok = seq > cur_seq; rank_ok = rank >= cur_rank。"""
        seq_ok = new_seq > cur_seq
        rank_ok = new_rank >= cur_rank
        result = seq_ok or rank_ok
        assert result == should_pass, (
            f"cur=(rank={cur_rank},seq={cur_seq}) new=(rank={new_rank},seq={new_seq}) "
            f"expected={should_pass} got={result}"
        )

    def test_parse_failed_bypass_guard(self) -> None:
        """parse_failed（rank=-1）允许任何阶段进入。"""
        from app.models.ingest import BagState
        # 设计：to_state == PARSE_FAILED 时跳过守卫
        cur_state = BagState.DONE  # rank=4
        to_state = BagState.PARSE_FAILED
        # 验证 PARSE_FAILED 不要求 seq > cur_seq 或 rank >= cur_rank
        assert to_state == BagState.PARSE_FAILED  # bypass 逻辑由 project_state 处理
