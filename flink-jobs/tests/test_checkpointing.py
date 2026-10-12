"""检查点装配单测（V1.19.15）：周期取值、非法值即失败、以及「与部署侧配置不得漂移」的静态对账。

背景（服务器实录）：两个作业 RUNNING、四个契约消费组 `--describe` 全部 `does not exist` ——
Flink `KafkaSource` 只在**检查点完成时**提交位点，而单机 compose 的 `FLINK_PROPERTIES` 只配了
`execution.checkpointing.dir`（目录）没配 `execution.checkpointing.interval`（周期），于是位点
永不提交。本目录用单测把三件事钉住：① 周期取值与非法值行为；② 代码默认值与 K8s 现值一致
（防的正是「两种形态各配一份、其中一份漏配」）；③ 三个作业都真的启用了检查点
（见 `test_pyflink_api_usage.py`，新增作业忘了开即失败）。
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest
from hunter_flink import checkpointing

ROOT = Path(__file__).resolve().parents[2]
K8S_CONFIGMAP = ROOT / "infra" / "k8s" / "flink" / "01-configmap.yaml"
COMPOSE_FILE = ROOT / "infra" / "deploy" / "docker-compose.yml"


class _FakeEnvironment:
    """只记录调用的假 `StreamExecutionEnvironment`（本测试不需要 PyFlink）。"""

    def __init__(self) -> None:
        self.intervals: list[int] = []

    def enable_checkpointing(self, interval: int) -> None:
        self.intervals.append(interval)


def test_default_interval_is_ten_seconds() -> None:
    assert checkpointing.DEFAULT_CHECKPOINT_INTERVAL_MS == 10_000
    assert checkpointing.checkpoint_interval_ms({}) == 10_000


@pytest.mark.parametrize("raw", ["", "   "])
def test_blank_value_falls_back_to_default(raw: str) -> None:
    """未设置/空串 = 没配（compose 的 `${VAR:-}` 会把空串传下来，不能因为空串就在提交期报错）。"""
    assert checkpointing.checkpoint_interval_ms({checkpointing.CHECKPOINT_INTERVAL_ENV: raw}) == (
        checkpointing.DEFAULT_CHECKPOINT_INTERVAL_MS
    )


@pytest.mark.parametrize("raw", ["30000", " 30000 ", "60000"])
def test_env_var_overrides_interval(raw: str) -> None:
    assert checkpointing.checkpoint_interval_ms({checkpointing.CHECKPOINT_INTERVAL_ENV: raw}) == (
        int(raw)
    )


@pytest.mark.parametrize("raw", ["10s", "0.5", "abc", "10 000"])
def test_non_integer_fails_at_submit_time(raw: str) -> None:
    """非法值必须**当场抛错**：静默退回默认就等于「以为配了其实没配」，与 job_conn 同一取向。"""
    with pytest.raises(ValueError, match="不是整数毫秒值"):
        checkpointing.checkpoint_interval_ms({checkpointing.CHECKPOINT_INTERVAL_ENV: raw})


def test_underscore_separated_value_is_accepted() -> None:
    """Python 的 `int()` 允许下划线分隔（`int("1_000") == 1000`）——**不属非法值**。

    不拦住它是因为拦住只能靠正则收紧，而 `.env` 里写下划线的运维少到不值得为此多一个报错面；
    本用例把这条意外行为钉住，避开下一轮“要不要拒”的重复争论。
    """
    assert checkpointing.checkpoint_interval_ms(
        {checkpointing.CHECKPOINT_INTERVAL_ENV: "30_000"}
    ) == 30_000


@pytest.mark.parametrize("raw", ["0", "-1"])
def test_non_positive_fails(raw: str) -> None:
    """0/负数不等于「关闭检查点」：关闭请删掉作业里的 enable_checkpointing 调用。"""
    with pytest.raises(ValueError, match="必须为正整数毫秒"):
        checkpointing.checkpoint_interval_ms({checkpointing.CHECKPOINT_INTERVAL_ENV: raw})


def test_enable_checkpointing_calls_environment_and_reports_interval(
    capsys: pytest.CaptureFixture[str],
) -> None:
    environment = _FakeEnvironment()
    used = checkpointing.enable_checkpointing(environment, {})
    assert environment.intervals == [checkpointing.DEFAULT_CHECKPOINT_INTERVAL_MS]
    assert used == checkpointing.DEFAULT_CHECKPOINT_INTERVAL_MS
    # 提交进程的标准输出是「检查点到底开没开」的第一手证据（进 flink run 输出与 JM 容器日志）
    assert "checkpointing enabled: interval_ms=10000" in capsys.readouterr().out


def test_enable_checkpointing_honours_override(capsys: pytest.CaptureFixture[str]) -> None:
    environment = _FakeEnvironment()
    checkpointing.enable_checkpointing(
        environment, {checkpointing.CHECKPOINT_INTERVAL_ENV: "20000"}
    )
    assert environment.intervals == [20_000]
    assert "interval_ms=20000" in capsys.readouterr().out


def test_default_matches_k8s_configmap_interval() -> None:
    """代码默认值必须与 `01-configmap.yaml` 的 `execution.checkpointing.interval` 同值。

    本条缺陷的成因正是「K8s 配了周期、compose 没配」——两份配置各写一遍就一定会漂移，
    所以周期以作业代码为准，此处静态断言两侧同值（改一侧不改另一侧即失败）。
    """
    text = K8S_CONFIGMAP.read_text(encoding="utf-8")
    match = re.search(r"^(\s*)execution\.checkpointing\.interval:\s*(\d+)\s*$", text, re.MULTILINE)
    assert match, "infra/k8s/flink/01-configmap.yaml 缺 execution.checkpointing.interval"
    assert int(match.group(2)) == checkpointing.DEFAULT_CHECKPOINT_INTERVAL_MS


def test_compose_passes_interval_variable_through() -> None:
    """compose 必须把覆盖变量透传给 flink-jm（提交进程所在容器）与 flink-tm，否则调不动。"""
    text = COMPOSE_FILE.read_text(encoding="utf-8")
    assert text.count(f"{checkpointing.CHECKPOINT_INTERVAL_ENV}:") >= 2, (
        "infra/deploy/docker-compose.yml 的 flink-jm 与 flink-tm 都需透传 "
        f"{checkpointing.CHECKPOINT_INTERVAL_ENV}"
    )
