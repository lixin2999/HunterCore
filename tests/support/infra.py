"""L5 测试基础设施夹具：容器（Postgres/TimescaleDB、Kafka、Redis、MinIO）+ 服务进程。

设计原则（保证「无 Docker / 无依赖也能收集并运行契约级用例」）：
- 重量依赖（testcontainers / docker / confluent_kafka / asyncpg / redis）**惰性导入**
- 环境不可用时夹具 **skip**（附明确原因），绝不静默通过或伪造结果
- 容器端口、镜像、凭证全部来自环境变量（禁止硬编码 URL/端口），默认值仅用于本地开发
- 集成测试用 ``pytest.mark.integration``、E2E 用 ``pytest.mark.e2e``、性能用 ``pytest.mark.performance``
"""
from __future__ import annotations

import asyncio
import importlib
import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from tests.support import contracts

REPO_ROOT = Path(__file__).resolve().parents[2]

# ---------------------------------------------------------------- 镜像与凭证（环境变量优先）

POSTGRES_IMAGE = os.getenv(
    "HUNTER_TEST_POSTGRES_IMAGE", "timescale/timescaledb:2.13.1-pg15"
)
KAFKA_IMAGE = os.getenv("HUNTER_TEST_KAFKA_IMAGE", "confluentinc/cp-kafka:7.6.0")
REDIS_IMAGE = os.getenv("HUNTER_TEST_REDIS_IMAGE", "redis:7.2-alpine")
MINIO_IMAGE = os.getenv("HUNTER_TEST_MINIO_IMAGE", "minio/minio:RELEASE.2024-01-16T16-07-38Z")

POSTGRES_DB = os.getenv("HUNTER_TEST_POSTGRES_DB", "hunter_core")
POSTGRES_USER = os.getenv("HUNTER_TEST_POSTGRES_USER", "hunter")
POSTGRES_PASSWORD = os.getenv("HUNTER_TEST_POSTGRES_PASSWORD", "hunter_test_pwd")
MINIO_ROOT_USER = os.getenv("HUNTER_TEST_MINIO_USER", "minioadmin")
MINIO_ROOT_PASSWORD = os.getenv("HUNTER_TEST_MINIO_PASSWORD", "minioadmin")

#: 服务启动等待上限（秒）
SERVICE_STARTUP_TIMEOUT_S = float(os.getenv("HUNTER_TEST_SERVICE_TIMEOUT_S", "30"))
#: 基础设施就绪等待上限（秒）
CONTAINER_READY_TIMEOUT_S = int(os.getenv("HUNTER_TEST_CONTAINER_TIMEOUT_S", "180"))


class MissingDependencyError(RuntimeError):
    """测试依赖未安装（提示安装命令，不静默跳过）。"""


def _import_optional(module_name: str, hint: str) -> Any:
    """惰性导入可选依赖；缺失时抛出带安装提示的异常（由夹具转为 skip）。"""
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError as exc:  # pragma: no cover - 环境相关
        raise MissingDependencyError(f"缺少依赖 {module_name}：{hint}") from exc


def docker_available() -> tuple[bool, str]:
    """检测 Docker 守护进程可用性（不可用返回原因，用于 skip 提示）。"""
    try:
        docker_module = _import_optional("docker", 'pip install -e "common/python[dev]"')
    except MissingDependencyError as exc:
        return False, str(exc)
    try:
        client = docker_module.from_env()
        client.ping()
    except Exception as exc:  # noqa: BLE001 - 任何连接失败均视为不可用
        return False, f"Docker 守护进程不可用：{type(exc).__name__}: {exc}"
    return True, "ok"


def skip_if_no_docker() -> None:
    """无 Docker 时跳过用例（附原因），保证 CI 无容器环境仍可收集。"""
    ok, reason = docker_available()
    if not ok:
        pytest.skip(f"跳过容器型集成测试：{reason}")


def free_port() -> int:
    """申请一个空闲端口（供服务进程类夹具使用，避免端口硬编码冲突）。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_for_port(host: str, port: int, timeout_s: float) -> bool:
    """等待 TCP 端口可连接（容器/服务就绪探测）。"""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(1.0)
            if sock.connect_ex((host, port)) == 0:
                return True
        time.sleep(0.5)
    return False


@dataclass
class ContainerEndpoint:
    """容器对外端点。"""

    host: str
    port: int

    @property
    def url(self) -> str:
        """``host:port`` 字符串。"""
        return f"{self.host}:{self.port}"

    def http(self, scheme: str = "http") -> str:
        """HTTP(S) 基地址。"""
        return f"{scheme}://{self.host}:{self.port}"


@dataclass
class PostgresHandle(ContainerEndpoint):
    """PostgreSQL/TimescaleDB 端点。"""

    database: str = POSTGRES_DB
    user: str = POSTGRES_USER
    password: str = POSTGRES_PASSWORD

    @property
    def dsn(self) -> str:
        """SQLAlchemy 异步 DSN（asyncpg 驱动，与 hunter_common 一致）。"""
        return (
            f"postgresql+asyncpg://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )



# ---------------------------------------------------------------- 容器夹具（session 级）

@pytest.fixture(scope="session")
def docker_guard() -> None:
    """统一 Docker 可用性门禁（依赖它的夹具在无 Docker 时 skip）。"""
    skip_if_no_docker()


@pytest.fixture(scope="session")
def postgres_container(docker_guard: None) -> Iterator[PostgresHandle]:
    """PostgreSQL 15 + TimescaleDB 2.13 容器，并执行 ``contracts/database`` DDL 建表。"""
    testcontainers_postgres = _import_optional(
        "testcontainers.postgres", "pip install testcontainers docker"
    )
    container = testcontainers_postgres.PostgresContainer(
        image=POSTGRES_IMAGE,
        username=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
        driver="asyncpg",
    )
    container.with_command("postgres -c shared_preload_libraries=timescaledb")
    container.start()
    try:
        handle = PostgresHandle(
            host=container.get_container_host_ip(),
            port=int(container.get_exposed_port(5432)),
        )
        assert wait_for_port(handle.host, handle.port, CONTAINER_READY_TIMEOUT_S), "Postgres 未就绪"
        _apply_ddl(handle)
        yield handle
    finally:
        container.stop()


def _apply_ddl(handle: PostgresHandle) -> None:
    """执行 DDL 契约脚本（幂等），失败即断言失败（契约必须可执行）。"""

    async def _run() -> None:
        asyncpg = _import_optional("asyncpg", "Postgres 容器夹具需要 asyncpg 驱动")
        connection = await asyncpg.connect(
            host=handle.host,
            port=handle.port,
            user=handle.user,
            password=handle.password,
            database=handle.database,
        )
        try:
            await connection.execute(contracts.ddl_sql())
        finally:
            await connection.close()

    asyncio.run(_run())


@pytest.fixture(scope="session")
def redis_container(docker_guard: None) -> Iterator[ContainerEndpoint]:
    """Redis 7 容器（会话/限流/锁/进度用例）。"""
    generic = _import_optional("testcontainers.core.container", "pip install testcontainers docker")
    container = generic.DockerContainer(REDIS_IMAGE, exposed_ports=[6379])
    container.start()
    try:
        handle = ContainerEndpoint(
            host=container.get_container_host_ip(),
            port=int(container.get_exposed_port(6379)),
        )
        assert wait_for_port(handle.host, handle.port, CONTAINER_READY_TIMEOUT_S), "Redis 未就绪"
        yield handle
    finally:
        container.stop()


@pytest.fixture(scope="session")
def kafka_container(docker_guard: None) -> Iterator[ContainerEndpoint]:
    """Kafka 3.6+ 单节点容器（生产→消费→落库链路用例；测试环境允许 PLAINTEXT）。"""
    kafka_module = _import_optional("testcontainers.kafka", "pip install testcontainers docker")
    container = kafka_module.KafkaContainer(image=KAFKA_IMAGE)
    container.start()
    try:
        handle = ContainerEndpoint(
            host=container.get_container_host_ip(),
            port=int(container.get_exposed_port(9093)),
        )
        assert wait_for_port(handle.host, handle.port, CONTAINER_READY_TIMEOUT_S), "Kafka 未就绪"
        yield handle
    finally:
        container.stop()


@pytest.fixture(scope="session")
def minio_container(docker_guard: None) -> Iterator[ContainerEndpoint]:
    """MinIO 容器（Bucket 规划与 SigV4 预签名链路用例）。"""
    generic = _import_optional("testcontainers.core.container", "pip install testcontainers docker")
    container = generic.DockerContainer(
        MINIO_IMAGE,
        command="server /data --console-address :9001",
        env={"MINIO_ROOT_USER": MINIO_ROOT_USER, "MINIO_ROOT_PASSWORD": MINIO_ROOT_PASSWORD},
        exposed_ports=[9000],
    )
    container.start()
    try:
        handle = ContainerEndpoint(
            host=container.get_container_host_ip(),
            port=int(container.get_exposed_port(9000)),
        )
        assert wait_for_port(handle.host, handle.port, CONTAINER_READY_TIMEOUT_S), "MinIO 未就绪"
        yield handle
    finally:
        container.stop()


# ---------------------------------------------------------------- 服务进程夹具

@dataclass
class ServiceProcess:
    """以 uvicorn 子进程方式拉起的微服务句柄。"""

    name: str
    port: int
    base_url: str
    process: subprocess.Popen[str]
    log_path: Path

    def stop(self) -> None:
        """停止服务进程（先 terminate，超时后 kill，禁止残留子进程）。"""
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:  # pragma: no cover - 异常路径
                self.process.kill()
                self.process.wait(timeout=10)

    def logs(self) -> str:
        """读取服务输出（断言失败时定位用）。"""
        if self.log_path.is_file():
            return self.log_path.read_text(encoding="utf-8", errors="replace")
        return ""


def start_service(name: str, port: int, env: dict[str, str] | None = None) -> ServiceProcess:
    """拉起单个微服务（uvicorn 子进程，cwd=服务目录，端口来自契约登记值）。"""
    service_dir = REPO_ROOT / "services" / name
    assert service_dir.is_dir(), f"服务目录不存在: {service_dir}"
    runtime_env = os.environ.copy()
    runtime_env.update(
        {
            "PYTHONPATH": f"{REPO_ROOT / 'common' / 'python'}{os.pathsep}{service_dir}",
            "PYTHONUNBUFFERED": "1",
            "SERVICE_NAME": name,
            "API_PORT": str(port),
            "ENVIRONMENT": "dev",
            "DEBUG": "true",
            **(env or {}),
        }
    )
    log_dir = REPO_ROOT / ".pytest_cache" / "service-logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{name}-{port}.log"
    log_file = log_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn", "app.main:app",
            "--host", "127.0.0.1", "--port", str(port),
        ],
        cwd=str(service_dir),
        env=runtime_env,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
    )
    if not wait_for_port("127.0.0.1", port, SERVICE_STARTUP_TIMEOUT_S):  # pragma: no cover
        process.kill()
        log_file.close()
        pytest.fail(
            f"{name} 启动超时（{SERVICE_STARTUP_TIMEOUT_S}s），日志：\n{log_path.read_text('utf-8', 'replace')}"
        )
    return ServiceProcess(
        name=name, port=port, base_url=f"http://127.0.0.1:{port}",
        process=process, log_path=log_path,
    )

