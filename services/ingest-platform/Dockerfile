# ingest-platform（多阶段构建；构建上下文为仓库根目录）
# 构建：docker build -f services/ingest-platform/Dockerfile -t hunter/ingest-platform:$VERSION .
# 运行用户：非 root UID 10001
#
# 阶段 1：builder —— python:3.12-slim。安装到独立前缀 /install 便于整体搬运到运行阶段。
FROM python:3.12-slim AS builder
ENV PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple \
    PIP_TRUSTED_HOST=pypi.tuna.tsinghua.edu.cn \
    PIP_DEFAULT_TIMEOUT=120 PIP_RETRIES=10
WORKDIR /build
COPY common/python ./common/python
COPY services/ingest-platform ./service
RUN pip install --prefix=/install ./common/python ./service

# 阶段 2：runner —— python:3.12-slim + 非 root 用户
FROM python:3.12-slim
RUN groupadd -r --gid 10001 appgroup && useradd -u 10001 -r -g appgroup -s /sbin/nologin appuser
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    API_PORT=8089
WORKDIR /app
COPY --from=builder /install/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /build/service/app ./app
EXPOSE 8089
HEALTHCHECK --interval=30s --timeout=3s --start-period=20s --retries=3 \
  CMD ["python3", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8089/healthz', timeout=2)"]
USER 10001
ENTRYPOINT ["python3"]
CMD ["-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8089"]
