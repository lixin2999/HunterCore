"""L5 测试层根 conftest：标记注册、报告采集、服务进程与 HTTP 客户端夹具。

- 标记（markers）定义在 ``tests/support/report.py``，此处注入并校验，避免 pytest.ini 与代码漂移
- 报告：``--l5-report`` 时把用例结果与性能指标汇总到 ``docs/test-reports/``（JSON 供 CI artifact）
- 服务进程夹具：以 uvicorn 子进程按契约端口拉起微服务，供 API 集成测试使用
"""
from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from tests.support import contracts, infra
from tests.support.infra import (
    docker_guard,
    kafka_container,
    minio_container,
    postgres_container,
    redis_container,
)
from tests.support.report import EXTRA_MARKERS, MARKERS, TestReport

#: 容器型夹具注册清单（显式引用导入名，避免与下方同名 fixture 参数触发 F811）
_CONTAINER_FIXTURES: tuple[Any, ...] = (
    docker_guard, kafka_container, minio_container, postgres_container, redis_container,
)

#: 全局报告实例（pytest hook 无法访问 fixture，故用模块级单例采集）
L5_REPORT = TestReport()
#: 当前执行的用例（pytest_runtest_logreport 中读取用例号）
_CURRENT_ITEM: Any = None
#: 是否写出报告（`--l5-report` 或环境变量 HUNTER_L5_REPORT=1）
_WRITE_REPORT = os.getenv("HUNTER_L5_REPORT", "0") == "1"


def pytest_addoption(parser: pytest.Parser) -> None:
    """注册 ``--l5-report`` 选项。"""
    parser.addoption(
        "--l5-report",
        action="store_true",
        default=False,
        help="运行结束后生成 L5 测试报告（docs/test-reports/）",
    )


def pytest_configure(config: pytest.Config) -> None:
    """注入 L5 标记（与 ``MARKERS`` 保持单一事实来源）。"""
    for name, description in {**MARKERS, **EXTRA_MARKERS}.items():
        config.addinivalue_line("markers", f"{name}: {description}")
    global _WRITE_REPORT
    if config.getoption("--l5-report"):
        _WRITE_REPORT = True


def _suite_of(item: pytest.Item) -> str:
    """由标记推导用例所属套件（默认 unit）。"""
    for suite in MARKERS:
        if suite in item.keywords:
            return suite
    return "unit"


def _case_id_of(item: pytest.Item) -> str:
    """用例号：优先取 ``@pytest.mark.l5_case("11.1")``，否则用模块名。"""
    marker = item.get_closest_marker("l5_case")
    if marker and marker.args:
        return str(marker.args[0])
    return Path(str(item.fspath)).stem


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    """采集用例结果（call 阶段成败 + setup 阶段跳过）。"""
    if _CURRENT_ITEM is None:
        return
    if report.when == "call" or (report.when == "setup" and report.skipped):
        status = "skip" if report.skipped else ("pass" if report.passed else "fail")
        detail = ""
        if report.skipped:
            detail = str(getattr(report, "wasxfail", "") or report.longrepr)
        elif report.failed:
            detail = str(report.longrepr).splitlines()[-1] if report.longrepr else ""
        L5_REPORT.add_entry(
            suite=_suite_of(_CURRENT_ITEM),
            case_id=_case_id_of(_CURRENT_ITEM),
            name=report.nodeid.split("::")[-1],
            status=status,
            duration_ms=report.duration * 1000.0,
            detail=detail[:500],
        )


def pytest_runtest_setup(item: pytest.Item) -> None:
    """记录当前用例（供 logreport 采集用例号）。"""
    global _CURRENT_ITEM
    _CURRENT_ITEM = item


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """会话结束：按需写出 Markdown + JSON 报告。"""
    if not _WRITE_REPORT:
        return
    md_path, json_path = L5_REPORT.write()
    print(f"\n[L5] 测试报告: {md_path}\n[L5] 结果 JSON: {json_path}")


@pytest.fixture(scope="session")
def l5_report() -> TestReport:
    """会话级报告对象（性能用例登记指标用）。"""
    return L5_REPORT


# ---------------------------------------------------------------- 服务进程与 HTTP 客户端

@pytest.fixture(scope="session")
def service_factory() -> Iterator[Any]:
    """服务进程工厂：``start_service(name)`` 按契约端口拉起服务，会话结束统一回收。"""
    started: list[infra.ServiceProcess] = []

    def _start(name: str, env: dict[str, str] | None = None) -> infra.ServiceProcess:
        """拉起服务并复用已启动实例（同端口只启一次）。"""
        port = contracts.service_port(name)
        for proc in started:
            if proc.name == name:
                return proc
        proc = infra.start_service(name, port, env=env)
        started.append(proc)
        return proc

    yield _start
    for proc in started:
        proc.stop()


# ---------------------------------------------------------------- 中间件（Kafka/Redis/MinIO）

@pytest.fixture(scope="session")
def postgres_handle(postgres_container: Any) -> Any:
    """PostgreSQL/TimescaleDB 端点句柄（DDL 契约已执行；供落库/时序用例使用）。"""
    return postgres_container


@pytest.fixture(scope="session")
def redis_handle(redis_container: Any) -> Any:
    """Redis 7 端点句柄（会话/限流/互斥锁/TTL 用例使用）。"""
    return redis_container


@pytest.fixture(scope="session")
def kafka_bootstrap(kafka_container: Any) -> str:
    """Kafka bootstrap 地址，并按契约创建全部 Topic（分区数取自 topics.yaml）。"""
    bootstrap = kafka_container.url
    from tests.support import broker

    broker.ensure_topics(bootstrap, broker.contract_topics())
    return bootstrap


@pytest.fixture(scope="session")
def s3_credentials(minio_container: Any) -> Any:
    """MinIO SigV4 凭证（环境变量注入，避免硬编码）。"""
    from tests.support.broker import S3Credentials

    return S3Credentials(
        access_key=infra.MINIO_ROOT_USER, secret_key=infra.MINIO_ROOT_PASSWORD
    )


@pytest.fixture(scope="session")
def gateway_service(service_factory: Any) -> infra.ServiceProcess:
    """api-gateway 进程（端口 8080，契约登记值）。"""
    return service_factory("api-gateway")


@pytest.fixture(scope="session")
def collector_service(service_factory: Any) -> infra.ServiceProcess:
    """data-collector 进程（端口 8082）。"""
    return service_factory("data-collector")


@pytest.fixture(scope="session")
def all_services(service_factory: Any) -> dict[str, infra.ServiceProcess]:
    """全部 6 个微服务进程（用于统一契约面探测）。"""
    return {name: service_factory(name) for name in contracts.SERVICE_NAMES}
