"""structlog 结构化日志（设计文档：可观测性 / 数据安全）。

- JSON 格式输出（本地开发可切换 ConsoleRenderer 便于调试）
- 每条日志自动附带 service / trace_id / vehicle_id（如已设置）
- 敏感字段（password/token/secret/key 等）自动脱敏，禁止记录明文
- trace_id / vehicle_id 通过 contextvars 注入，天然协程安全
"""
from __future__ import annotations

import contextvars
import logging
import re
import sys
from typing import Any

import structlog

# 请求链路上下文（协程安全）
trace_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("trace_id", default="")
vehicle_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("vehicle_id", default="")

# 敏感字段名匹配（日志脱敏，设计文档：数据安全）
_SENSITIVE_KEY_RE = re.compile(
    r"(password|passwd|secret|token|api[-_]?key|private[-_]?key|authorization|certificate)",
    re.IGNORECASE,
)
_MASKED = "***MASKED***"

_service_name: str = "hunter-service"


class _DynamicStdout:
    """动态输出流代理：每次写入转发到【当前】sys.stdout，IO 异常静默降级。

    动机（可观测性 + 可用性约束）：
    - 若在 configure_logging 时固定 sys.stdout 引用，pytest capsys / 容器日志
      采集器替换流后，旧流关闭会导致日志写入抛 ValueError，进而打垮业务请求
      （实测中间件日志调用抛异常 -> /healthz 返回 500）。
    - 日志系统故障绝不应影响业务主流程，故 write/flush 全部兜底。
    """

    def write(self, message: str) -> None:
        try:
            sys.stdout.write(message)
        except (ValueError, OSError):  # 流已关闭/不可写：静默丢弃
            pass

    def flush(self) -> None:
        try:
            sys.stdout.flush()
        except (ValueError, OSError):
            pass


def get_trace_id() -> str:
    """获取当前协程的 trace_id（未设置返回空字符串）。"""
    return trace_id_var.get()


def set_trace_id(trace_id: str) -> contextvars.Token[str]:
    """设置当前协程的 trace_id（返回 Token 用于恢复）。"""
    return trace_id_var.set(trace_id)


def reset_trace_id(token: contextvars.Token[str]) -> None:
    """恢复 trace_id 上下文（请求结束时调用）。"""
    trace_id_var.reset(token)


def set_vehicle_id(vehicle_id: str) -> contextvars.Token[str]:
    """设置当前协程的 vehicle_id（车端相关请求日志自动附带）。"""
    return vehicle_id_var.set(vehicle_id)


def reset_vehicle_id(token: contextvars.Token[str]) -> None:
    """恢复 vehicle_id 上下文（请求结束时调用）。"""
    vehicle_id_var.reset(token)


def _add_service_name(_: Any, __: str, event_dict: structlog.typing.EventDict) -> structlog.typing.EventDict:
    """注入 service 字段。"""
    event_dict.setdefault("service", _service_name)
    return event_dict


def _add_trace_context(_: Any, __: str, event_dict: structlog.typing.EventDict) -> structlog.typing.EventDict:
    """注入 trace_id / vehicle_id 上下文（如已设置）。"""
    trace_id = trace_id_var.get()
    if trace_id:
        event_dict.setdefault("trace_id", trace_id)
    vehicle_id = vehicle_id_var.get()
    if vehicle_id:
        event_dict.setdefault("vehicle_id", vehicle_id)
    return event_dict


def _mask_sensitive(_: Any, __: str, event_dict: structlog.typing.EventDict) -> structlog.typing.EventDict:
    """敏感字段脱敏（password/token/secret/key 等），防止敏感信息泄露。"""
    for key in list(event_dict):
        if _SENSITIVE_KEY_RE.search(str(key)):
            event_dict[key] = _MASKED
    return event_dict


def configure_logging(
    service_name: str,
    log_level: str = "INFO",
    *,
    json_output: bool = True,
) -> None:
    """初始化全局 structlog 配置（应用启动时调用一次，重复调用安全）。

    Args:
        service_name: 服务名，写入每条日志的 service 字段。
        log_level: DEBUG / INFO / WARNING / ERROR。
        json_output: True 输出 JSON（生产/容器），False 输出彩色控制台（本地开发）。
    """
    global _service_name
    _service_name = service_name
    level = getattr(logging, log_level.upper(), logging.INFO)

    shared_processors: list[structlog.typing.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        _add_service_name,
        _add_trace_context,
        _mask_sensitive,
        structlog.processors.format_exc_info,
        structlog.processors.EventRenamer("message"),
    ]
    renderer: structlog.typing.Processor = (
        structlog.processors.JSONRenderer(ensure_ascii=False, default=str)
        if json_output
        else structlog.dev.ConsoleRenderer(colors=sys.stdout.isatty())
    )

    structlog.configure(
        processors=[*shared_processors, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        # 动态流代理：禁止固定捕获 sys.stdout（流被 pytest/capsys 或容器采集器
        # 替换后旧流会关闭；固定引用会让后续日志写入抛 ValueError -> 请求 500）
        logger_factory=structlog.PrintLoggerFactory(file=_DynamicStdout()),
        cache_logger_on_first_use=True,
    )
    _configure_stdlib_bridge(shared_processors, renderer, level)


def _configure_stdlib_bridge(
    shared_processors: list[structlog.typing.Processor],
    renderer: structlog.typing.Processor,
    level: int,
) -> None:
    """将标准库 logging（uvicorn/sqlalchemy 等第三方库日志）桥接到 structlog 输出。"""
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )
    # 动态流代理（见 _DynamicStdout 说明）：第三方库日志写入异常也不影响业务
    handler = logging.StreamHandler(_DynamicStdout())
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)


def get_logger(name: str | None = None) -> structlog.typing.FilteringBoundLogger:
    """获取结构化 logger（每条日志自动携带 service / trace_id / vehicle_id）。"""
    return structlog.get_logger(name)
