"""HunterEdge 骨架模板（A）：包说明 / 配置 / 测试 / Dockerfile / pyproject。

模板占位符使用 string.Template 语法（$service / $port / $desc），避免与代码中的花括号冲突。
"""
from __future__ import annotations

from string import Template

INIT_TMPL = Template('"""$desc。"""\n')

CONFIG_TMPL = Template('''"""$service 服务配置（pydantic-settings，全部参数来自环境变量/.env）。"""
from __future__ import annotations

from hunter_common.config import HunterBaseConfig


class Settings(HunterBaseConfig):
    """$service 配置；服务私有配置项按契约逐步补充（禁止硬编码业务参数）。"""

    # 服务标识与默认端口（仍可被环境变量 SERVICE_NAME / API_PORT 覆盖）
    service_name: str = "$service"
    api_port: int = $port


settings = Settings()
''')

TEST_HEALTH_TMPL = Template('''"""$service 健康探针单元测试（不依赖外部基础设施）。"""
from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app


def _client() -> AsyncClient:
    """构造测试客户端；raise_app_exceptions=False 使全局 500 处理器生效（返回响应而非抛出）。"""
    return AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test",
    )


@pytest.mark.asyncio
async def test_healthz_ok() -> None:
    """/healthz 返回统一响应格式且 code=0。"""
    async with _client() as client:
        resp = await client.get("/healthz")
    assert resp.status_code == 200
    body = resp.json()
    assert body["code"] == 0
    assert set(body.keys()) == {"code", "message", "data", "request_id", "timestamp"}


@pytest.mark.asyncio
async def test_healthz_trace_id_header() -> None:
    """响应必须携带 X-Request-ID（trace_id 贯穿请求链路）。"""
    async with _client() as client:
        resp = await client.get("/healthz")
    assert resp.headers.get("X-Request-ID")


@pytest.mark.asyncio
async def test_unknown_error_unified_body() -> None:
    """未预期异常必须返回统一响应体 code=5000。"""
    async with _client() as client:
        resp = await client.get("/dev/null-500")
    assert resp.status_code == 500
    body = resp.json()
    assert body["code"] == 5000
    assert body["message"] == "服务器内部错误"


@pytest.mark.asyncio
async def test_metrics_endpoint_prometheus_format() -> None:
    """/metrics 输出 Prometheus 文本格式，且指标带 service 标签（供 infra/monitoring 抓取）。"""
    async with _client() as client:
        await client.get("/healthz")  # 先产生一次请求指标
        resp = await client.get("/metrics")
    assert resp.status_code == 200
    assert "hunter_http_requests_total" in resp.text
    assert 'service="$service"' in resp.text
''')

DOCKERFILE_TMPL = Template('''# $service（多阶段构建；构建上下文为仓库根目录）
# 构建：docker build -f services/$service/Dockerfile -t hunter/$service:0.1.0 .
# 运行用户：distroless :nonroot（UID 65532），禁止 root 运行（安全机制约束）
#
# 阶段 1：builder —— python:3.11-slim（Python 小版本必须与运行时 distroless 一致，
#         否则 site-packages 路径不匹配）。安装到独立前缀 /install 便于整体搬运。
FROM python:3.11-slim AS builder
ENV PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /build
COPY common/python ./common/python
COPY services/$service ./service
RUN pip install --prefix=/install ./common/python ./service

# 阶段 2：runner —— distroless（无 shell / 无包管理器，最小化攻击面）
# 回退方案（若镜像仓库不可达）：python:3.11-slim + 非 root 用户（useradd -u 10001）。
FROM gcr.io/distroless/python3-debian12:nonroot
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \\
    PYTHONPATH=/usr/lib/python3/dist-packages:/app \\
    API_PORT=$port
WORKDIR /app
# Debian 12 系统解释器 site-packages 路径（与 builder 的 lib/python3.11/site-packages 对应）
COPY --from=builder /install/lib/python3.11/site-packages /usr/lib/python3/dist-packages
COPY --from=builder /build/service/app ./app
EXPOSE $port
# 健康检查由 K8s livenessProbe/readinessProbe 以 HTTP GET 承担
# （distroless 无 shell，无法使用 Dockerfile HEALTHCHECK）
ENTRYPOINT ["python3"]
CMD ["-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "$port"]
''')

PYPROJECT_TMPL = Template('''[project]
name = "$service"
version = "0.1.0"
description = "$desc"
requires-python = ">=3.11"
dependencies = [
    "fastapi>=0.100,<1.0",
    "uvicorn[standard]>=0.23",
    "prometheus-client>=0.20,<1",
]

# 注意：hunter_common 由仓库统一安装（先执行 pip install -e "common/python"），
# 服务内通过 import hunter_common 使用；不声明为 PyPI 依赖以免误装同名包。
[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.23",
    "httpx>=0.27",
]

[tool.pytest.ini_options]
testpaths = ["app/tests"]
asyncio_mode = "auto"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["app"]
''')
