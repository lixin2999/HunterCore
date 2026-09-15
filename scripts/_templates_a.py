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
''')

DOCKERFILE_TMPL = Template('''# $service（多阶段构建；构建上下文为仓库根目录）
# 构建示例：docker build -f services/$service/Dockerfile -t hunter/$service:dev .
FROM python:3.12-slim AS builder
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /build
COPY common/python ./common/python
COPY services/$service ./service
RUN pip install --prefix=/install ./common/python ./service

FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY --from=builder /install /usr/local
WORKDIR /app
COPY --from=builder /build/service/app ./app
EXPOSE $port
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "$port"]
''')

PYPROJECT_TMPL = Template('''[project]
name = "$service"
version = "0.1.0"
description = "$desc"
requires-python = ">=3.11"
dependencies = [
    "fastapi>=0.100,<1.0",
    "uvicorn[standard]>=0.23",
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
