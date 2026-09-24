# api-gateway（多阶段构建；构建上下文为仓库根目录）
# 构建：docker build -f services/api-gateway/Dockerfile -t hunter/api-gateway:$VERSION .
#       （$VERSION 与 K8s 清单的 {version} 占位符同源，由 scripts/render_k8s.py 注入）
# 运行用户：非 root UID 10001（distroless 方案因 gcr.io 国内不可达且版本匹性限定 3.11，改为 slim 回退方案）
#
# 阶段 1：builder —— python:3.12-slim。安装到独立前缀 /install 便于整体搬运到运行阶段。
FROM python:3.12-slim AS builder
ENV PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /build
COPY common/python ./common/python
COPY services/api-gateway ./service
RUN pip install --prefix=/install ./common/python ./service

# 阶段 2：runner —— python:3.12-slim + 非 root 用户（回退方案，原 distroless 镜像国内 GCR 不可达）
FROM python:3.12-slim
RUN groupadd -r --gid 10001 appgroup && useradd -u 10001 -r -g appgroup -s /sbin/nologin appuser
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    API_PORT=8080
WORKDIR /app
# pip --prefix=/install 安装路径：/install/lib/python3.12/site-packages → slim 镜像默认 site-packages
COPY --from=builder /install/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /build/service/app ./app
EXPOSE 8080
# 容器级健康检查（K8s 环境忽略此指令，由 livenessProbe/readinessProbe 承担）
HEALTHCHECK --interval=30s --timeout=3s --start-period=20s --retries=3 \
  CMD ["python3", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=2)"]
USER 10001
ENTRYPOINT ["python3"]
CMD ["-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
