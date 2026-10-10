# flink-jobs PyFlink 作业镜像（G-13/G-28「代码内置」部署形态——单一事实来源，杜绝把作业源码内联进 ConfigMap 造成的漂移）
# 构建：docker build -f flink-jobs/Dockerfile -t hunter-flink/jobs:2.1.1 .
#       ⚠ 镜像前缀必须是 `hunter-flink/`，禁止 `hunter/`：
#         scripts/render_k8s.py（OWN_IMAGE_PATTERN/EXPECTED_IMAGES）与 scripts/verify_infra.py
#         （check_version_placeholder「模块表不可新增」）只放行 6 个 `hunter/<svc>` 微服务镜像，
#         `hunter/flink-jobs` 会被判「未登记的微服务镜像」致 CI lint 失败。
#         第三方/独立前缀镜像按自身版本标签管理，不参与 {version} 渲染（标签与 Flink 运行时同版）。
# 基础镜像：flink:2.1.1-scala_2.12-java17（**V1.19.5** 由 1.18.1 升级；与 PyPI 上 apache-flink 严格同版本）；
#           升级 Flink 运行时 → 同步改本标签 + apache-flink 版本 + infra/k8s/flink/ 清单
#           + infra/deploy/docker-compose.yml flink-jm/tm + .gitlab-ci.yml build-images。
#
# ⚠ Python 版本三元约束（V1.19.5，服务器实机逐层踩坑后查 PyPI 定位）：
#   ① hunter_common 声明 requires-python>=3.11，且 database/enums.py 用 enum.StrEnum（3.11+ 才有）；
#   ② PyFlink 按 CPython ABI 发轮子（已核 PyPI Linux wheel）：apache-flink **1.18.1 只到 cp310**、
#      1.20.x 到 cp311、**只有 2.1.x 才发 cp312**；③ 全系统服务基线 = python:3.12-slim（V1.16.1）。
#   → 三者唯一交集：**Flink 2.1.1 + CPython 3.12**（作业与服务解释器对齐）。
#   解释器不用 apt：基座 apt python3 与 3.12 无交集，改自 python:3.12-slim-bookworm 拷入自包含 CPython。
#   ⚠ 跨发行版拷 /usr/local 时，CPython 的 `_ssl` 等扩展必须能在基座找到它链接的 OpenSSL：
#     V1.19.4 拷 bullseye（OpenSSL 1.1 / libssl.so.1.1）到旧基座时构建即报 ImportError（服务器实录），
#     bookworm 与 2.x 基座同代 OpenSSL 3（libssl.so.3）→ 换 bookworm 即消除。任何 ABI 不匹配
#     会在下面四道构建期自检的第一行（python --version）立即失败，不会产出坏镜像。
FROM python:3.12-slim-bookworm AS pyrel

# Kafka 连接器 jar（V1.19.5 新增）：官方 flink 镜像与 apache-flink wheel **都不自带** DataStream
# Kafka 连接器（`org.apache.flink.connector.kafka.*`），缺 jar 时作业提交后即报
# `ClassNotFoundException: org.apache.flink.connector.kafka.source.KafkaSource`（V1.19.2~4
# 均卡在解释器层、未触达此层）。Flink 1.15 起连接器独立版本线：Flink **2.1** 对应
# `flink-sql-connector-kafka` **5.0.0-2.1**（已核 Maven Central，目录无 `_2.12` 后缀）。
# 用 python 标准库下载（不依赖基座是否带 curl/wget）；内网镜像源可用 ARG 覆盖（同路径结构）：
#   --build-arg KAFKA_CONNECTOR_URL=https://maven.aliyun.com/repository/central/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.1/flink-sql-connector-kafka-5.0.0-2.1.jar
FROM python:3.12-slim-bookworm AS conn
ARG KAFKA_CONNECTOR_URL=https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.1/flink-sql-connector-kafka-5.0.0-2.1.jar
ARG KAFKA_CONNECTOR_JAR=flink-sql-connector-kafka-5.0.0-2.1.jar
RUN mkdir -p /opt/flink-connector && python -c "import os, urllib.request; urllib.request.urlretrieve(os.environ['KAFKA_CONNECTOR_URL'], os.path.join('/opt/flink-connector', os.environ['KAFKA_CONNECTOR_JAR']))"

FROM flink:2.1.1-scala_2.12-java17

# 携入 CPython 3.12（/usr/local 整棵：bin/python3、python3.12、pip、lib/libpython3.12.so 与扩展 .so）
# + 建 `python` 软链：`flink run -pym` 客户端与 worker 默认查找 `python`（config 键
# python.client.executable / python.executable 的默认值），Debian/Ubuntu 只提供 python3。
# apt 只装两类东西：ca-certificates（提交/拉取依赖）与 libsasl2-2（confluent-kafka 自带 librdkafka
# 的 SASL 运行时依赖，Kafka 作业必然路径；基座镜像不定带，宁先补不后补）。
USER root
COPY --from=pyrel /usr/local /usr/local
# 第一道自检（ABI 门禁）：解释器版本必须是 3.12；ssl 能加载（抓 libssl 缺失）。
# ⚠ 禁止在 RUN 续行（行尾 `\`）之间写 `#` 注释：多行会被拼接成单条 shell 命令，`#` 会把它后面的
#   全部自检静默注释掉（等于自检消失）——注释必须放在 RUN 指令上方。
RUN set -eux; \
    apt-get update -qq; \
    apt-get install -y -qq --no-install-recommends ca-certificates libsasl2-2; \
    rm -rf /var/lib/apt/lists/*; \
    ldconfig; \
    ln -sf python3 /usr/local/bin/python; \
    python --version && pip --version && python -c "import ssl; print('ssl OK', ssl.OPENSSL_VERSION)"
ENV PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple \
    PIP_TRUSTED_HOST=pypi.tuna.tsinghua.edu.cn \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# 安装 PyFlink 运行时 + 共享库 hunter_common（作业依赖其受控词表/键常量；
# 经 hunter_common/__init__ 传递导入 sqlalchemy/redis/confluent-kafka 等，需随镜像一并提供）。
# FLINK_HOME 复用基础镜像自带的 JVM 发行版（/opt/flink），apache-flink 仅补 Python 绑定层。
# 第三道自检（依赖门禁）：本层末尾 import 一遍作业真实用到的包。
# ⚠ 版本必须与基座 Flink 完全一致（1.18.1 → 曾无 cp311 轮子而默默回退 sdist 的教训）：
#   pip 若找不到匹配 cp312 的轮子会报错/转尝试源码包，本层自检 import pyflink 会立即拉响。
COPY common/python /opt/hunter/common/python
# 连接器 jar 入基座 classpath（JM/TM/提交 Job 共用本镜像，三侧同时生效）+ 第二道自检：
# 直接开 jar（zip）断言两个关键类存在，避免“文件拷了但不是那个连接器”的静默失败。
# ⚠ 同样禁止在本 RUN 续行之间写 `#` 注释。
COPY --from=conn --chown=flink:flink /opt/flink-connector/ /opt/flink/lib/
RUN python -c "import glob, zipfile; paths=glob.glob('/opt/flink/lib/flink-sql-connector-kafka-*.jar'); assert paths, 'connector jar not in /opt/flink/lib'; names=zipfile.ZipFile(paths[0]).namelist(); missing=[c for c in ('org/apache/flink/connector/kafka/source/KafkaSource.class', 'org/apache/flink/connector/kafka/sink/KafkaSink.class') if c not in names]; assert not missing, 'jar lacks classes: '+str(missing); print('kafka connector jar OK:', paths[0])"
RUN pip install "apache-flink==2.1.1" && pip install /opt/hunter/common/python && \
    python -c "import sys, pyflink, hunter_common.database.enums, hunter_common.redis_keys; \
import confluent_kafka, redis, sqlalchemy, pydantic; \
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink; \
print('job deps OK on', sys.version.split()[0])"

# 作业代码内置：JobManager / TaskManager / 提交 Job 共用本镜像 →
# worker 侧经 PYTHONPATH 直接 import hunter_flink.*，无需 --pyfiles 逐包分发（代码内置形态的核心收益）。
COPY flink-jobs/hunter_flink /opt/hunter/jobs/hunter_flink
ENV PYTHONPATH=/opt/hunter/jobs \
    PYTHON_HOME=/usr/local/bin/python3

# 第四道自检（V1.19.7：PyFlink API 面门禁）。作业 main() 标 `pragma: no cover`（仓库内无 PyFlink
# 运行时），故 1.x → 2.x 的改名只在**提交期**暴露（服务器实录：三个作业齐报
# `KafkaOffsetsInitializer has no attribute 'group_offsets'`）。此处用纯 Python 属性内省
# （不启 JVM、不建 gateway）按白名单逐个断言成员存在 + 2.x 已删名字确已消失，把同类漂移
# 前移为构建失败；白名单单一事实来源 = hunter_flink/api_guard.py（仓库侧另有 ast 静态单测）。
RUN python -m hunter_flink.api_guard

# 运行用户：flink 官方镜像内置非 root 用户 flink（UID 9999）——与 infra/k8s/flink/ 清单
# Pod securityContext.runAsUser 一致（verify_infra 对 Deployment 强制非 root）。
# ⚠ V1.19.5 升级后需实机复核 UID 仍为 9999（`docker run --rm flink:2.1.1-scala_2.12-java17 id flink`），
#   否则 K8s 形态 runAsUser 与镜像内文件属主不一致（compose 形态不受影响）。
USER flink
WORKDIR /opt/flink
# 保留 flink 镜像默认 ENTRYPOINT（/docker-entrypoint.sh）：以 args `jobmanager`/`taskmanager` 选择角色；
# 提交作业由 batch/v1 Job 覆盖 command（`flink run -pym hunter_flink.<job>`），不在镜像内固化 CMD。
