"""PyFlink API 面守卫（V1.19.7）：把「凭印象写 API」的运行期崩溃前移为构建期/CI 失败。

现场（服务器实录）：Flink 1.18 → 2.1 升级后三个作业提交齐报
``AttributeError: type object 'KafkaOffsetsInitializer' has no attribute 'group_offsets'``。
作业 ``main()`` 标 ``pragma: no cover``（仓库内无 PyFlink 运行时），API 名字从未被任何单测校验，
故同类漂移（``value_only_decoder`` / ``DataStream.sink`` / ``env.add_source(FLIP-27 Source)``）
一并存在于三个作业里——每个都要一次实机往返才能暴露。

两道防线（名字集合是本模块的单一事实来源，两侧共用）：

1. :func:`check` —— 在**装有 apache-flink 的作业镜像内**执行（Dockerfile 第四道自检
   ``python -m hunter_flink.api_guard``）：用 ``hasattr`` 逐个断言白名单成员存在、2.x 已删除
   的名字确已消失。纯 Python 属性内省，**不启 JVM、不建 gateway**，秒级完成。
2. ``flink-jobs/tests/test_pyflink_api_usage.py`` —— 在**仓库 CI（无 pyflink）**用 ``ast``
   静态解析三个作业 ``main()``：断言 pyflink import 与被调用的成员名全部落在本模块白名单内，
   且禁用语（1.x 旧名 / 从未存在的名字）不出现。

白名单依据 release-2.1 权威源码逐条核对（``flink-python/pyflink/datastream/connectors/kafka.py``、
``data_stream.py``、``stream_execution_environment.py``、``functions.py``、
``common/serialization.py``、``common/watermark_strategy.py``）。
"""
from __future__ import annotations

import importlib
import sys
from typing import Any

#: 作业允许使用的 pyflink 成员：模块路径 → ``类点路径`` → 成员名（空元组 = 只断言类可导入）。
#: ⚠ 这里的每一项都必须与 apache-flink（与 Flink 运行时严格同版本）的实际 API 面一致，
#:   由 :func:`check` 在镜像内验证；新增作业用到新 API 时先补本表，再补镜像自检。
API_WHITELIST: dict[str, dict[str, tuple[str, ...]]] = {
    "pyflink.common": {
        "Types": ("STRING",),
        "WatermarkStrategy": ("no_watermarks",),
    },
    "pyflink.common.serialization": {
        "SimpleStringSchema": (),
    },
    "pyflink.datastream": {
        "StreamExecutionEnvironment": (
            "get_execution_environment",
            "from_source",
            "execute",
        ),
    },
    "pyflink.datastream.data_stream": {
        "DataStream": (
            "key_by",
            "map",
            "flat_map",
            "process",
            "sink_to",
            "add_sink",
            "print",
        ),
        "KeyedStream": ("map", "flat_map", "process", "add_sink"),
    },
    "pyflink.datastream.functions": {
        "Function": ("open", "close"),
        "FlatMapFunction": ("flat_map",),
        "KeyedProcessFunction": ("process_element", "Context"),
        "KeyedProcessFunction.Context": ("timestamp", "timer_service"),
    },
    "pyflink.datastream.connectors.kafka": {
        "KafkaSource": ("builder",),
        "KafkaSourceBuilder": (
            "set_bootstrap_servers",
            "set_topics",
            "set_group_id",
            "set_starting_offsets",
            "set_value_only_deserializer",
            "build",
        ),
        "KafkaOffsetsInitializer": (
            "committed_offsets",
            "earliest",
            "latest",
            "timestamp",
        ),
        "KafkaOffsetResetStrategy": ("EARLIEST", "LATEST", "NONE"),
        "KafkaSink": ("builder",),
        "KafkaSinkBuilder": (
            "set_bootstrap_servers",
            "set_record_serializer",
            "build",
        ),
        "KafkaRecordSerializationSchema": ("builder",),
        "KafkaRecordSerializationSchemaBuilder": (
            "set_topic",
            "set_key_serialization_schema",
            "set_value_serialization_schema",
            "build",
        ),
    },
}

#: Flink 2.x 确已删除（或从未存在）的名字：:func:`check` 断言其在镜像内**不存在**，
#: 一旦 PyFlink 改回这些名字，本守卫先响，提醒复审作业侧用法是否需要回改。
#: ① ``group_offsets``/``specific_offsets`` → ``committed_offsets``/``offsets``（1.19 起对齐
#:   Java ``OffsetsInitializer``）；② ``DataStream.sink`` 从来只有 ``sink_to``（Java ``sinkTo``）；
#:   ③ ``value_only_decoder`` 从来只有 ``set_value_only_deserializer``。
REMOVED_IN_FLINK_21: tuple[tuple[str, str, str], ...] = (
    ("pyflink.datastream.connectors.kafka", "KafkaOffsetsInitializer", "group_offsets"),
    ("pyflink.datastream.connectors.kafka", "KafkaOffsetsInitializer", "specific_offsets"),
    ("pyflink.datastream.connectors.kafka", "KafkaSourceBuilder", "value_only_decoder"),
    ("pyflink.datastream.data_stream", "DataStream", "sink"),
)

#: 禁止出现在作业源码里的名字（1.x 旧名 / 未列入白名单的凭印象写法）：
#: 由 ast 单测拦截，不检测镜像（``add_source``/``PRIMITIVE_STRING`` 在 PyFlink 里仍然合法，
#: 只是不适用 FLIP-27 Source 与序列化 schema 参数位——见 :func:`check` 的注释）。
FORBIDDEN_IN_JOBS: frozenset[str] = frozenset(
    {
        "add_source",  # 只接受 SourceFunction；FLIP-27 Source 必须走 from_source
        "group_offsets",
        "specific_offsets",
        "value_only_decoder",
        "sink",  # 正确名是 sink_to
        "PRIMITIVE_STRING",  # 序列化/反序列化参数位要 DeserializationSchema，不是 TypeInformation
        "InformationType",
    }
)


def _resolve(module: Any, dotted: str) -> Any:
    target = module
    for part in dotted.split("."):
        target = getattr(target, part)
    return target


#: fluent 链的后继类：``KafkaSource.builder().set_*().build()`` 这类链上，``set_*``/``build``
#: 实际属于 Builder 类而不是产品类。**仅供仓库侧 ast 静态测试展开调用链**；镜像内的
#: :func:`check` 逐类做 ``hasattr``，不走本表（所以本表写错也不会漏过镜像那道门禁）。
FLUENT_SUCCESSORS: dict[tuple[str, str], tuple[tuple[str, str], ...]] = {
    ("pyflink.datastream.connectors.kafka", "KafkaSource"): (
        ("pyflink.datastream.connectors.kafka", "KafkaSourceBuilder"),
    ),
    ("pyflink.datastream.connectors.kafka", "KafkaSink"): (
        ("pyflink.datastream.connectors.kafka", "KafkaSinkBuilder"),
    ),
    ("pyflink.datastream.connectors.kafka", "KafkaRecordSerializationSchema"): (
        ("pyflink.datastream.connectors.kafka", "KafkaRecordSerializationSchemaBuilder"),
    ),
    ("pyflink.datastream", "StreamExecutionEnvironment"): (
        ("pyflink.datastream.data_stream", "DataStream"),
    ),
    ("pyflink.datastream.data_stream", "DataStream"): (
        ("pyflink.datastream.data_stream", "KeyedStream"),
    ),
    ("pyflink.datastream.data_stream", "KeyedStream"): (
        ("pyflink.datastream.data_stream", "DataStream"),
    ),
}


def allowed_members(module_path: str, dotted: str) -> set[str]:
    """类自身成员 + fluent 后继类成员（闭包展开），供 ast 静态测试比对调用链。"""
    out: set[str] = set()
    seen: set[tuple[str, str]] = set()
    stack = [(module_path, dotted)]
    while stack:
        key = stack.pop()
        if key in seen:
            continue
        seen.add(key)
        out.update(API_WHITELIST.get(key[0], {}).get(key[1], ()))
        stack.extend(FLUENT_SUCCESSORS.get(key, ()))
    return out


def check() -> list[str]:
    """返回问题列表（空 = PyFlink API 面与白名单一致）；未安装 pyflink 时由调用方跳过。"""
    problems: list[str] = []
    for module_path, classes in API_WHITELIST.items():
        try:
            module = importlib.import_module(module_path)
        except ImportError as exc:  # pragma: no cover - 环境异常（无 apache-flink）
            problems.append(f"import {module_path} failed: {exc!r}")
            continue
        for dotted, members in classes.items():
            try:
                target = _resolve(module, dotted)
            except AttributeError:
                problems.append(f"{module_path}.{dotted} missing")
                continue
            for member in members:
                if not hasattr(target, member):
                    problems.append(f"{module_path}.{dotted} has no attribute {member!r}")
    for module_path, dotted, member in REMOVED_IN_FLINK_21:
        try:
            target = _resolve(importlib.import_module(module_path), dotted)
        except (ImportError, AttributeError):  # pragma: no cover - 上面的白名单检查已报告
            continue
        if hasattr(target, member):
            problems.append(
                f"{module_path}.{dotted}.{member} reappeared (Flink 2.x removed name)"
            )
    return problems


def main() -> int:
    """镜像内自检入口（``python -m hunter_flink.api_guard``，Dockerfile 第四道门禁）。"""
    problems = check()
    if problems:
        for item in problems:
            print(f"pyflink api guard FAIL: {item}", file=sys.stderr)
        return 1
    print(f"pyflink api guard OK ({sum(len(v) for v in API_WHITELIST.values())} classes)")
    return 0


__all__ = [
    "API_WHITELIST",
    "FLUENT_SUCCESSORS",
    "FORBIDDEN_IN_JOBS",
    "REMOVED_IN_FLINK_21",
    "allowed_members",
    "check",
    "main",
]


# ⚠ 本块是 Dockerfile 第四道自检（`python -m hunter_flink.api_guard`）的**唯一执行入口**：
#   缺少它时 `-m` 只导入模块就退出（返回码 0），自检会**静默变为空跑**——正是本模块要防的
#   “自检消失”类缺陷，故由 tests/test_pyflink_api_usage.py 静态断言本块存在。
if __name__ == "__main__":  # pragma: no cover - 镜像内入口，由 Dockerfile 调用
    raise SystemExit(main())
