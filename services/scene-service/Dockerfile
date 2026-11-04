# scene-service（多阶段构建；构建上下文为仓库根目录）
# 构建：docker build -f services/scene-service/Dockerfile -t hunter/scene-service:$VERSION .
#       （$VERSION 与 K8s 清单的 {version} 占位符同源，由 scripts/render_k8s.py 注入）
# 运行用户：非 root UID 10001（distroless 方案因 gcr.io 国内不可达且版本匹性限定 3.11，改为 slim 回退方案）
#
# V1.24.1 仿真端管理：运行期依赖 openssl 子进程签发每节点 mTLS 客户端证书（与 vehicle-service 同策略），
#   故 runner 阶段显式安装 openssl；CA 私钥（ca-key.pem）以 :ro 挂载 + gid 10001 只读，禁止落入镜像层。
#
# 阶段 1：builder —— python:3.12-slim。安装到独立前缀 /install 便于整体搬运到运行阶段。
FROM python:3.12-slim AS builder
ENV PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple \
    PIP_TRUSTED_HOST=pypi.tuna.tsinghua.edu.cn \
    PIP_DEFAULT_TIMEOUT=120 PIP_RETRIES=10
WORKDIR /build
COPY common/python ./common/python
COPY services/scene-service ./service
RUN pip install --prefix=/install ./common/python ./service

# 阶段 2：runner —— python:3.12-slim + 非 root 用户（回退方案，原 distroless 镜像国内 GCR 不可达）
FROM python:3.12-slim
# openssl 供 sim_node_cert_signer 子进程签发节点证书；ca-certificates 保证出站 HTTPS 信任链
RUN apt-get update \
    && apt-get install -y --no-install-recommends openssl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN groupadd -r --gid 10001 appgroup && useradd -u 10001 -r -g appgroup -s /sbin/nologin appuser
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    API_PORT=8081 \
    SCENE_TAXONOMY_FILE=/app/contracts/scene-taxonomy.yaml
WORKDIR /app
# pip --prefix=/install 安装路径：/install/lib/python3.12/site-packages → slim 镜像默认 site-packages
COPY --from=builder /install/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /build/service/app ./app
# 车辆参数内置默认（V1.23.1：vehicle_params provider 为启动硬依赖，缺文件即启动失败不降级）。
# DEFAULT_VEHICLE_PARAMS_PATH 在容器内解析为 /app/config/vehicle_params.json（__file__=/app/app/... 的 parents[2]），
# 而 app 包并不含服务根级 config/，故必须显式内置——否则回落路径不存在会直接 crash 启动。
# 生产接车端真实参数：挂 bind mount / ConfigMap 后设 SCENE_VEHICLE_PARAMS_PATH 覆盖此默认。
COPY --from=builder /build/service/config ./config
# 场景受控字典（单一事实来源）：hunter_common.scene_taxonomy 启动即加载，缺失报错不降级，
# 故必须随镜像内置（也可由 K8s 以 ConfigMap/bind mount 挂载 contracts/ 覆盖）。
COPY contracts/scene-taxonomy.yaml ./contracts/scene-taxonomy.yaml
# SIM_NODE_CERTS_DIR（默认 /app/data/sim-node-certs）：compose 挂载持久卷，运行期写入每节点证书
RUN mkdir -p /app/data/sim-node-certs && chown -R 10001:10001 /app/data
EXPOSE 8081
# 容器级健康检查（K8s 环境忽略此指令，由 livenessProbe/readinessProbe 承担）
HEALTHCHECK --interval=30s --timeout=3s --start-period=20s --retries=3 \
  CMD ["python3", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8081/healthz', timeout=2)"]
USER 10001
ENTRYPOINT ["python3"]
CMD ["-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8081"]
