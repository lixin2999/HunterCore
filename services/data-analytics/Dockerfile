# data-analytics（多阶段构建；构建上下文为仓库根目录）
# 构建：docker build -f services/data-analytics/Dockerfile -t hunter/data-analytics:$VERSION .
#       （$VERSION 与 K8s 清单的 {version} 占位符同源，由 scripts/render_k8s.py 注入）
# 运行用户：distroless :nonroot（UID 65532），禁止 root 运行（安全机制约束）
#
# 阶段 1：builder —— python:3.11-slim（Python 小版本必须与运行时 distroless 一致，
#         否则 site-packages 路径不匹配）。安装到独立前缀 /install 便于整体搬运。
FROM python:3.11-slim AS builder
ENV PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /build
COPY common/python ./common/python
COPY services/data-analytics ./service
RUN pip install --prefix=/install ./common/python ./service

# 阶段 2：runner —— distroless（无 shell / 无包管理器，最小化攻击面）
# 回退方案（若镜像仓库不可达）：python:3.11-slim + 非 root 用户（useradd -u 10001）。
FROM gcr.io/distroless/python3-debian12:nonroot
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    PYTHONPATH=/usr/lib/python3/dist-packages:/app \
    API_PORT=8083
WORKDIR /app
# Debian 12 系统解释器 site-packages 路径（与 builder 的 lib/python3.11/site-packages 对应）
COPY --from=builder /install/lib/python3.11/site-packages /usr/lib/python3/dist-packages
COPY --from=builder /build/service/app ./app
EXPOSE 8083
# 容器级健康检查：exec 形式（distroless 无 shell，用 python3 -c 探测 /healthz）
# K8s 环境忽略 Dockerfile HEALTHCHECK，探针由 livenessProbe/readinessProbe（HTTP GET）承担
HEALTHCHECK --interval=30s --timeout=3s --start-period=20s --retries=3 \
  CMD ["python3", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8083/healthz', timeout=2)"]
ENTRYPOINT ["python3"]
CMD ["-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8083"]
