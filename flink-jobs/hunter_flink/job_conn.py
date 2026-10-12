"""作业连接参数装配（V1.19.8）：Kafka SASL 属性 + Redis 连接串，纯 Python、可在无 PyFlink 下单测。

背景（服务器实录）：V1.19.7 后三个作业已 RUNNING，但 `flink-jm`/`flink-tm` 的容器环境里
**没有任何 Kafka/Redis 凭据**（compose 未下发、作业也无读取路径，`infra/k8s/flink/01-configmap.yaml`
第 10-11 行早已登记为待办），而 broker 的内部监听 9092 是 **SASL_PLAINTEXT + SCRAM-SHA-512**、
Redis 开了 `requirepass`（实录 `NOAUTH Authentication required.`）。作业以明文连 SASL 监听时
不会立刻报错，而是**静默空转**（fetch 不到数据、`flink list` 仍显示 RUNNING）——这是本仓库已踩过
两次的一类坑（CLI 明文连 SCRAM broker 报 node assignment timeout）。

设计取舍：

- **无凭据就快速失败**：环境变量声明了 SASL 协议却缺用户名/口令 ⇒ 直接 :func:`ValueError`，
  而不是退化成明文连接后长时间静默空转。提交期报错（`flink run` 立即失败）远好于运行期假绿。
- **属性随作业图下发**：这些 property 在 ``main()``（提交进程）里求值并写进 JobGraph，
  因此**提交侧容器（compose 的 flink-jm / K8s 的 flink-jobmanager + 提交 Job）必须持有凭据**；
  TM 侧同样下发以便作业内其他路径与排障一致性。⚠ 代价：口令会出现在 JobGraph/REST plan 里，
  属内部网络形态的已知折衷（release.md 风险登记）。
- **JAAS login module 类名可覆盖**：Flink 的 Kafka 连接器是 **shaded fat jar**，Kafka 客户端类被
  重定位到 ``org.apache.flink.kafka.shaded.org.apache.kafka.*``，故 JAAS 里的 login module 必须用
  **shade 后的类名**；默认值按此给，异常情况下可用 ``KAFKA_SASL_LOGIN_MODULE`` 覆盖（核验命令见
  docs/故障排查指南.md）。
"""
from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any
from urllib.parse import quote

#: 连接器 fat jar 内被 shade 的 SCRAM login module（默认；可用 KAFKA_SASL_LOGIN_MODULE 覆盖）
DEFAULT_SCRAM_LOGIN_MODULE = (
    "org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule"
)

#: 需要 SASL 的协议（其余按明文/SSL 处理，不下发 sasl.* 属性）
_SASL_PROTOCOLS = frozenset({"SASL_PLAINTEXT", "SASL_SSL"})

_DEFAULT_MECHANISM = "SCRAM-SHA-512"


def _source(env: Mapping[str, str] | None) -> Mapping[str, str]:
    return os.environ if env is None else env


def kafka_sasl_properties(env: Mapping[str, str] | None = None) -> dict[str, str]:
    """作业需透传给 Kafka 客户端的 SASL 属性；明文协议返回 ``{}``。

    读取的环境变量与六个业务服务的 `x-business-env` **同名同语义**（避免两套拼写漂移）：
    ``KAFKA_SECURITY_PROTOCOL`` / ``KAFKA_SASL_MECHANISM`` / ``KAFKA_SASL_USERNAME`` /
    ``KAFKA_SASL_PASSWORD``，可选 ``KAFKA_SASL_LOGIN_MODULE``。

    Raises:
        ValueError: 协议声明为 SASL 但缺少用户名或口令（宁可提交期失败，不可明文空转）。
    """
    source = _source(env)
    protocol = source.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").strip().upper()
    if protocol not in _SASL_PROTOCOLS:
        return {}

    username = source.get("KAFKA_SASL_USERNAME", "").strip()
    password = source.get("KAFKA_SASL_PASSWORD", "")
    if not username or not password:
        raise ValueError(
            f"KAFKA_SECURITY_PROTOCOL={protocol} 需要 SASL 凭据，但 "
            f"KAFKA_SASL_USERNAME/KAFKA_SASL_PASSWORD 未下发（作业不得明文连 SASL 监听——"
            f"那样只会静默空转）；请在容器环境注入这两项（compose: x-business-env 同源变量）"
        )

    mechanism = source.get("KAFKA_SASL_MECHANISM", _DEFAULT_MECHANISM).strip().upper()
    login_module = source.get("KAFKA_SASL_LOGIN_MODULE", DEFAULT_SCRAM_LOGIN_MODULE).strip()
    if not login_module:
        raise ValueError("KAFKA_SASL_LOGIN_MODULE 显式设为空值：JAAS login module 类名不可为空")
    # JAAS 串内需转义双引号，避免口令含 `"` 时破坏语法（注入面同时收敛）
    escaped = password.replace("\\", "\\\\").replace('"', '\\"')
    return {
        "security.protocol": protocol,
        "sasl.mechanism": mechanism,
        "sasl.jaas.config": (
            f'{login_module} required username="{username}" password="{escaped}";'
        ),
    }


def apply_kafka_properties(builder: Any, props: Mapping[str, str]) -> Any:
    """把 SASL 属性逐个 ``set_property`` 到 Source/Sink Builder 上（返回同一 builder 便于链式）。

    参数/返回类型定为 ``Any`` 而非 ``object``：PyFlink 两个 Builder 是具体类，调用方需要接着写
    ``.build()``；用 ``object`` 会让 mypy 报「object 无 build 属性」而逼出 `type: ignore`。
    """
    for key, value in props.items():
        builder = builder.set_property(key, value)
    return builder


def redis_url(env: Mapping[str, str] | None = None) -> str:
    """Redis 连接串：优先分段变量（可带口令），其次 ``REDIS_URL``，最后 compose 默认。

    分段变量与 ``hunter_common.config`` 的 ``REDIS_HOST/REDIS_PORT/REDIS_PASSWORD/REDIS_DB``
    同名；口令做 URL 编码，含 ``@ : / # %`` 等字符的口令不再需要人工规避（实机口令为随机串，
    历史上正是这类字符踩过坑）。⚠ 只要设了 ``REDIS_HOST`` 就走分段分支、**忽略** ``REDIS_URL``，
    避免「ConfigMap 里遗留的无口令 REDIS_URL 悄悄覆盖住真凭据」。
    """
    source = _source(env)
    host = source.get("REDIS_HOST", "").strip()
    if not host:
        url = source.get("REDIS_URL", "").strip()
        return url or "redis://redis:6379/0"

    port = source.get("REDIS_PORT", "6379").strip() or "6379"
    db = source.get("REDIS_DB", "0").strip() or "0"
    password = source.get("REDIS_PASSWORD", "")
    auth = f":{quote(password, safe='')}@" if password else ""
    return f"redis://{auth}{host}:{port}/{db}"


__all__ = [
    "DEFAULT_SCRAM_LOGIN_MODULE",
    "apply_kafka_properties",
    "kafka_sasl_properties",
    "redis_url",
]
