"""检查点装配（V1.19.15）：把「位点提交」这件事从部署侧配置收回到作业代码里。

现场（服务器实录 V1.19.13 续跑块）：`flink list -a` 显示 `driving_anomaly_detection` 与
`data_quality_monitor_ingest_latency` 两个作业 **RUNNING**，而四个 data-analytics 契约消费组
`kafka-groups.sh --describe` 全部 `Error: Consumer group '…' does not exist.`，看板却对同一批组
回四个 `lag=0`（第七类假绿，读数口径已由 V1.19.14 修）。真正的原因在**运行形态**：

- Flink 的 `KafkaSource` **只在检查点完成时提交消费位点**（`commit.offsets.on.checkpoint` 默认
  `true`，且没有别的提交路径）；
- 单机 compose 的 `FLINK_PROPERTIES` 只设了 `execution.checkpointing.dir`（目录），**没设
  `execution.checkpointing.interval`（周期）**——没有周期就没有检查点；
- 于是位点**永远不会提交**：组在集群上不存在 ⇒ 积压不可观测、作业每次重启都回落 `earliest`
  重放整个保留窗口（7 天）、窗口状态也随故障全丢。
- K8s 形态 `infra/k8s/flink/01-configmap.yaml` 设了 `execution.checkpointing.interval: 10000`，
  **只有单机 compose 漏了** ⇒ 两种形态行为不一致，属本仓库反复踩过的「同一件事两处配置漂移」。

设计取舍：

- **启不启检查点属作业语义，不属部署环境**：写进 `main()` 后 compose/K8s/本地 MiniCluster 一律
  同形，也不会再被「换了编排就忘一行」的坑命中；部署侧的同名配置仅作兜底（代码值优先）。
- **周期可由环境变量覆盖**（`FLINK_CHECKPOINTING_INTERVAL_MS`），默认 10s 与 K8s 现值对齐；
  值不合法时**提交期即 :func:`ValueError`**，绝不静默退回默认（与 `job_conn` 同一套取向）。
- **只开周期，不动其他检查点参数**：模式取 PyFlink 默认 `EXACTLY_ONCE`（三个作业的 sink 都是
  KafkaSink 默认 `AT_LEAST_ONCE` 投递，不涉及事务与 `transactional.id` 前缀，开检查点不改变
  端到端投递语义，只增加位点提交与状态快照）。
"""
from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

#: 覆盖周期的环境变量（compose 的 flink-jm/flink-tm 与 K8s flink-common-config 均透传本名）
CHECKPOINT_INTERVAL_ENV = "FLINK_CHECKPOINTING_INTERVAL_MS"

#: 默认周期：与 `infra/k8s/flink/01-configmap.yaml` 的 `execution.checkpointing.interval` 同值
DEFAULT_CHECKPOINT_INTERVAL_MS = 10_000


def checkpoint_interval_ms(env: Mapping[str, str] | None = None) -> int:
    """解析检查点周期（毫秒）。

    未设置或设为空串 ⇒ :data:`DEFAULT_CHECKPOINT_INTERVAL_MS`（compose 里 `${VAR:-}` 传下来的
    空串等价于「没配」，不能因为空串就在提交期报错）。

    Raises:
        ValueError: 非整数或 ≤ 0（宁可在 `flink run` 当场失败，也不要「以为开了其实没开」）。
    """
    source = os.environ if env is None else env
    raw = source.get(CHECKPOINT_INTERVAL_ENV, "").strip()
    if not raw:
        return DEFAULT_CHECKPOINT_INTERVAL_MS
    try:
        interval = int(raw)
    except ValueError as exc:
        raise ValueError(
            f"{CHECKPOINT_INTERVAL_ENV}={raw!r} 不是整数毫秒值（形如 10000）；"
            "检查点周期不允许模糊取值"
        ) from exc
    if interval <= 0:
        raise ValueError(
            f"{CHECKPOINT_INTERVAL_ENV}={interval} 必须为正整数毫秒"
            f"（0/负数不等于「关闭检查点」——关闭请删掉本作业的 enable_checkpointing 调用）"
        )
    return interval


def enable_checkpointing(environment: Any, env: Mapping[str, str] | None = None) -> int:
    """对 `StreamExecutionEnvironment` 启用周期性检查点，返回实际生效的周期（毫秒）。

    参数名用 `environment` 而非 `env`：本函数的第一个位置参数在三个作业里就是 PyFlink 的 env
    对象，而 `env` 在本包里惯指「环境变量映射」（见 `job_conn`），两者混在一个函数签名里必读错。

    ⚠ 成员名 `enable_checkpointing` 已列入 `api_guard.API_WHITELIST`，由镜像内第四道自检
    （`python -m hunter_flink.api_guard`）验证其在 apache-flink 2.1 里确实存在。
    """
    interval = checkpoint_interval_ms(env)
    environment.enable_checkpointing(interval)
    # 提交进程的标准输出会进 `flink run` 的返回与 JM 容器日志，是「检查点到底开没开」的第一手证据
    print(f"[hunter_flink] checkpointing enabled: interval_ms={interval}")
    return interval


__all__ = [
    "CHECKPOINT_INTERVAL_ENV",
    "DEFAULT_CHECKPOINT_INTERVAL_MS",
    "checkpoint_interval_ms",
    "enable_checkpointing",
]
