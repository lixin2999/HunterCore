# vehicle-service（多阶段构建；构建上下文为仓库根目录）
# 构建：docker build -f services/vehicle-service/Dockerfile -t hunter/vehicle-service:0.1.0 .
#
# 与其它 5 个业务服务不同：本服务运行时依赖 `openssl` 子进程签发每车独立客户端证书，
# distroless（无 shell / 无 openssl）不适用；使用 python:3.11-slim 并显式创建非 root
# 用户（uid=10001 / gid=10001，与 kafka-buffer 现有约定一致）。
# CA 私钥（ca-key.pem）以 :ro 挂载 + setfacl 授权 gid 10001 只读，禁止落入镜像层。
#
# 阶段 1：builder —— 安装依赖到 /install 前缀
FROM python:3.11-slim AS builder
ENV PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /build
COPY common/python ./common/python
COPY services/vehicle-service ./service
RUN pip install --prefix=/install ./common/python ./service

# 阶段 2：runner —— python:3.11-slim + 非 root 用户 + openssl 客户端
FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    API_PORT=8086
# openssl 用于 cert_signer 子进程签发；ca-certificates 保证出站 HTTPS 信任链
# 单装 openssl 约 5MB，符合最小镜像原则
RUN apt-get update \
    && apt-get install -y --no-install-recommends openssl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -g 10001 hunter \
    && useradd -u 10001 -g 10001 -m -s /usr/sbin/nologin hunter
WORKDIR /app
COPY --from=builder /install /usr/local
COPY --chown=10001:10001 services/vehicle-service/app ./app
# VEHICLE_CERTS_DIR（默认 /app/data/vehicle-certs）：compose 挂载持久卷，运行期写入每车证书
RUN mkdir -p /app/data/vehicle-certs && chown -R 10001:10001 /app/data
USER 10001:10001
EXPOSE 8086
# 健康检查由 K8s livenessProbe/readinessProbe 或 compose healthcheck 承担；
# 本镜像含 python 无 shell，用 `python -c` 探测 /healthz
HEALTHCHECK --interval=30s --timeout=3s --start-period=15s --retries=3 \
  CMD ["python", "-c", "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8086/healthz', timeout=2).status==200 else 1)"]
ENTRYPOINT ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8086"]
