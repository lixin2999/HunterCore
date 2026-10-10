# flink-jobs PyFlink 作业镜像（G-13/G-28「代码内置」部署形态——单一事实来源，杜绝把作业源码内联进 ConfigMap 造成的漂移）
# 构建：docker build -f flink-jobs/Dockerfile -t hunter-flink/jobs:1.18.1 .
#       ⚠ 镜像前缀必须是 `hunter-flink/`，禁止 `hunter/`：
#         scripts/render_k8s.py（OWN_IMAGE_PATTERN/EXPECTED_IMAGES）与 scripts/verify_infra.py
#         （check_version_placeholder「模块表不可新增」）只放行 6 个 `hunter/<svc>` 微服务镜像，
#         `hunter/flink-jobs` 会被判「未登记的微服务镜像」致 CI lint 失败。
#         第三方/独立前缀镜像按自身版本标签管理，不参与 {version} 渲染（标签与 Flink 运行时同版）。
# 基础镜像：flink:1.18.1-scala_2.12-java17（与 compose 单机形态 docs/deployment.md §12 对齐）；
#           升级 Flink 运行时 → 同步改本标签 + apache-flink 版本 + infra/k8s/flink/ 清单。
FROM flink:1.18.1-scala_2.12-java17

# flink 官方镜像为纯 JVM（temurin 基座），不含 Python；PyFlink 需要 python3 + apache-flink（与基础镜像同小版本 1.18.1）
USER root
RUN set -eux; \
    apt-get update -qq; \
    apt-get install -y -qq --no-install-recommends python3 python3-pip python3-venv ca-certificates; \
    rm -rf /var/lib/apt/lists/*
ENV PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple \
    PIP_TRUSTED_HOST=pypi.tuna.tsinghua.edu.cn \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# 安装 PyFlink 运行时 + 共享库 hunter_common（作业依赖其受控词表/键常量；
# 经 hunter_common/__init__ 传递导入 sqlalchemy/redis/confluent-kafka 等，需随镜像一并提供）。
# FLINK_HOME 复用基础镜像自带的 JVM 发行版（/opt/flink），apache-flink 仅补 Python 绑定层。
COPY common/python /opt/hunter/common/python
RUN pip install "apache-flink==1.18.1" && pip install /opt/hunter/common/python

# 作业代码内置：JobManager / TaskManager / 提交 Job 共用本镜像 →
# worker 侧经 PYTHONPATH 直接 import hunter_flink.*，无需 --pyfiles 逐包分发（代码内置形态的核心收益）。
COPY flink-jobs/hunter_flink /opt/hunter/jobs/hunter_flink
ENV PYTHONPATH=/opt/hunter/jobs \
    PYTHON_HOME=/usr/bin/python3

# 运行用户：flink 官方镜像内置非 root 用户 flink（UID 9999）——与 infra/k8s/flink/ 清单
# Pod securityContext.runAsUser 一致（verify_infra 对 Deployment 强制非 root）。
USER flink
WORKDIR /opt/flink
# 保留 flink 镜像默认 ENTRYPOINT（/docker-entrypoint.sh）：以 args `jobmanager`/`taskmanager` 选择角色；
# 提交作业由 batch/v1 Job 覆盖 command（`flink run -pym hunter_flink.<job>`），不在镜像内固化 CMD。
