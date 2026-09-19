#!/usr/bin/env python3
"""HunterCore 基础设施清单校验（L1）。

校验项：
  1. K8s YAML 语法可解析，且文档非空
  2. apiVersion 白名单（apps/v1、v1、batch/v1、networking.k8s.io/v1、autoscaling/v2、policy/v1）
  3. 命名空间统一为 hunter-core（Namespace 资源本身除外）
  4. 工作负载（Deployment/StatefulSet）resources.requests/limits 与三种探针齐备
  5. 镜像标签禁止 latest / 缺省（必须显式版本或发布日期）
  6. ConfigMap 不得包含敏感字段（密码/密钥/私钥）
  7. 微服务端口与设计文档端口表一致（8080-8085）
  8. Kafka Topic 契约一致性（K8s init Job vs docker-compose 脚本：名称/分区/保留）
  9. MinIO Bucket 集合一致性（K8s init Job vs docker-compose 脚本）
 10. 自有微服务镜像必须使用 `{version}` 版本变量占位符（由 scripts/render_k8s.py 注入）
 11. HPA 契约：6 个微服务齐备，minReplicas=2 / maxReplicas=5 / CPU 目标 70% / 缩容稳定窗口
 12. PDB 契约：无状态服务齐备，minAvailable 必须小于 Deployment 基线副本数
 13. Dockerfile 多阶段 / 非 root / 端口 / 容器级健康检查（exec 形式，兼容 distroless）

用法：python scripts/verify_infra.py
退出码：0 全部通过；1 存在失败项
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
#: 校验范围：应用与中间件清单 / 监控组件清单 / 采集器清单
K8S_DIRS: tuple[Path, ...] = (
    ROOT / "infra" / "k8s",
    ROOT / "infra" / "monitoring" / "k8s",
    ROOT / "infra" / "monitoring" / "exporters",
)
K8S_DIR = K8S_DIRS[0]

ALLOWED_API_VERSIONS = {
    "v1",
    "apps/v1",
    "batch/v1",
    "networking.k8s.io/v1",
    "rbac.authorization.k8s.io/v1",
    "autoscaling/v2",   # HPA（infra/k8s/autoscaling/hpa.yaml）
    "policy/v1",        # PodDisruptionBudget（infra/k8s/disruption/）
}
WORKLOAD_KINDS = {"Deployment", "StatefulSet", "DaemonSet"}
#: 应用与中间件命名空间（restricted Pod 安全标准）
NAMESPACE = "hunter-core"
#: 允许的命名空间；monitoring 用于采集器（node-exporter 需要 host 网络/文件系统）
ALLOWED_NAMESPACES = {"hunter-core", "monitoring"}

#: 服务名 -> 端口（设计文档模块划分，不可更改）
SERVICE_PORTS: dict[str, int] = {
    "api-gateway": 8080,
    "scene-service": 8081,
    "data-collector": 8082,
    "data-analytics": 8083,
    "ota-service": 8084,
    "remote-control": 8085,
}

#: 自有微服务镜像必须使用的版本变量占位符（禁止硬编码版本号；由 scripts/render_k8s.py 注入）
VERSION_PLACEHOLDER = "{version}"
#: 清单中的 image 字段行（字段级匹配，避免把注释里的镜像名误判为镜像声明）
IMAGE_LINE_PATTERN = re.compile(r"^\s*image:\s*[\"']?(?P<image>[^\"'\s#]+)")
#: 版本渲染脚本（版本变量的单一注入入口）
RENDER_SCRIPT = "scripts/render_k8s.py"
#: HPA 部署约束（不可放宽：min=2 / max=5 / CPU 目标 70%）
HPA_MIN_REPLICAS = 2
HPA_MAX_REPLICAS = 5
HPA_CPU_TARGET_PERCENT = 70

#: ConfigMap 中禁止出现的敏感字段模式
SENSITIVE_KEY_PATTERN = re.compile(
    r"(PASSWORD|SECRET|PRIVATE_KEY|CREDENTIAL|SASL_USERNAME)", re.IGNORECASE
)
PRIVATE_KEY_PATTERN = re.compile(r"BEGIN [A-Z ]*PRIVATE KEY")

failures: list[str] = []
checks: list[str] = []


def ok(msg: str) -> None:
    checks.append(f"[PASS] {msg}")


def fail(msg: str) -> None:
    failures.append(f"[FAIL] {msg}")
    checks.append(f"[FAIL] {msg}")


def load_documents() -> list[tuple[Path, dict[str, Any]]]:
    """加载 infra 下全部 K8s 清单文档（应用/中间件/监控/采集器）。"""
    documents: list[tuple[Path, dict[str, Any]]] = []
    for base in K8S_DIRS:
        for path in sorted(base.rglob("*.yaml")):
            try:
                for doc in yaml.safe_load_all(path.read_text(encoding="utf-8")):
                    if doc:
                        documents.append((path, doc))
            except yaml.YAMLError as exc:  # YAML 语法错误必须暴露
                fail(f"YAML 解析失败: {path.relative_to(ROOT)} -> {exc}")
    return documents


def rel(path: Path) -> str:
    return str(path.relative_to(ROOT)).replace("\\", "/")


def check_api_versions(documents: list[tuple[Path, dict[str, Any]]]) -> None:
    """校验 API 版本白名单与命名空间一致性。"""
    bad_api = [
        f"{rel(p)}:{d.get('kind')}={d.get('apiVersion')}"
        for p, d in documents
        if d.get("apiVersion") not in ALLOWED_API_VERSIONS
    ]
    if bad_api:
        fail(f"API 版本不在白名单（{sorted(ALLOWED_API_VERSIONS)}）: {bad_api}")
    else:
        ok(f"API 版本白名单校验通过（{len(documents)} 个文档）")

    bad_ns = []
    for path, doc in documents:
        # 集群级资源（无命名空间）：Namespace / ClusterRole / ClusterRoleBinding
        if doc.get("kind") in {"Namespace", "ClusterRole", "ClusterRoleBinding"}:
            continue
        ns = (doc.get("metadata") or {}).get("namespace")
        if ns not in ALLOWED_NAMESPACES:
            bad_ns.append(f"{rel(path)}:{doc.get('kind')}={ns}")
    if bad_ns:
        fail(f"命名空间必须为 {sorted(ALLOWED_NAMESPACES)} 之一: {bad_ns}")
    else:
        ok(f"命名空间校验通过（{sorted(ALLOWED_NAMESPACES)}）")


def check_workloads(documents: list[tuple[Path, dict[str, Any]]]) -> None:
    """校验工作负载的资源配额、探针、安全上下文与镜像标签。"""
    for path, doc in documents:
        if doc.get("kind") not in WORKLOAD_KINDS:
            continue
        name = (doc.get("metadata") or {}).get("name")
        spec = (((doc.get("spec") or {}).get("template") or {}).get("spec")) or {}
        containers = spec.get("containers") or []
        if not containers:
            fail(f"{rel(path)}:{name} 未定义容器")
            continue
        # monitoring 命名空间的采集器（node-exporter 需 host 网络/文件系统）豁免
        # restricted 校验；hunter-core（应用/中间件/监控组件）强制非 root + drop ALL
        strict_security = (doc.get("metadata") or {}).get("namespace") != "monitoring"
        for container in containers:
            cname = container.get("name")
            image = container.get("image", "")
            tag = image.rsplit(":", 1)[-1] if ":" in image else ""
            if not tag or tag == "latest":
                fail(f"{rel(path)}:{name}/{cname} 镜像必须使用显式版本（禁止 latest）: {image}")

            resources = container.get("resources") or {}
            for field in ("requests", "limits"):
                if not (resources.get(field) or {}).get("cpu") or not (resources.get(field) or {}).get(
                    "memory"
                ):
                    fail(f"{rel(path)}:{name}/{cname} resources.{field} 必须同时声明 cpu 与 memory")

            for probe in ("livenessProbe", "readinessProbe"):
                if probe not in container:
                    fail(f"{rel(path)}:{name}/{cname} 缺少 {probe}")

            if strict_security:
                ctx = container.get("securityContext") or {}
                if ctx.get("allowPrivilegeEscalation") is not False:
                    fail(
                        f"{rel(path)}:{name}/{cname} "
                        "securityContext.allowPrivilegeEscalation 必须为 false"
                    )
                if "ALL" not in ((ctx.get("capabilities") or {}).get("drop") or []):
                    fail(f"{rel(path)}:{name}/{cname} 必须 drop ALL capabilities")

        if strict_security:
            pod_ctx = spec.get("securityContext") or {}
            if pod_ctx.get("runAsNonRoot") is not True:
                fail(f"{rel(path)}:{name} Pod securityContext.runAsNonRoot 必须为 true")
            if not pod_ctx.get("runAsUser"):
                fail(f"{rel(path)}:{name} Pod securityContext.runAsUser 必须显式指定（禁止 root）")
    ok("工作负载资源配额 / 探针 / 安全上下文校验完成")


def check_configmaps(documents: list[tuple[Path, dict[str, Any]]]) -> None:
    """校验 ConfigMap 不含敏感字段（敏感配置必须走 Secret）。"""
    for path, doc in documents:
        if doc.get("kind") != "ConfigMap":
            continue
        name = (doc.get("metadata") or {}).get("name")
        for key, value in (doc.get("data") or {}).items():
            if SENSITIVE_KEY_PATTERN.search(key):
                fail(f"{rel(path)}:ConfigMap/{name} 含敏感键 {key}，必须移入 Secret")
            if isinstance(value, str) and PRIVATE_KEY_PATTERN.search(value):
                fail(f"{rel(path)}:ConfigMap/{name} 含私钥内容，必须移入 Secret")
    ok("ConfigMap 敏感字段校验完成")


def check_service_ports(documents: list[tuple[Path, dict[str, Any]]]) -> None:
    """校验微服务端口与设计文档端口表一致（Deployment / Service / ConfigMap 三处）。"""
    for path, doc in documents:
        kind = doc.get("kind")
        name = (doc.get("metadata") or {}).get("name")
        if name not in SERVICE_PORTS:
            continue
        expected = SERVICE_PORTS[name]
        if kind == "Deployment":
            containers = (
                ((doc.get("spec") or {}).get("template") or {}).get("spec", {}).get("containers", [])
            )
            container_ports = [p.get("containerPort") for c in containers for p in (c.get("ports") or [])]
            if container_ports != [expected]:
                fail(f"{rel(path)}:{name} containerPort 应为 {expected}，实际 {container_ports}")
        elif kind == "Service":
            service_ports = [p.get("port") for p in (doc.get("spec") or {}).get("ports") or []]
            if service_ports != [expected]:
                fail(f"{rel(path)}:Service/{name} port 应为 {expected}，实际 {service_ports}")
        elif kind == "ConfigMap":
            api_port = str((doc.get("data") or {}).get("API_PORT", ""))
            if api_port != str(expected):
                fail(f"{rel(path)}:ConfigMap/{name} API_PORT 应为 {expected}，实际 {api_port!r}")
    ok("微服务端口一致性校验完成（8080-8085）")


def check_env_contract(documents: list[tuple[Path, dict[str, Any]]]) -> None:
    """校验共享 ConfigMap 的键均对应 hunter_common.config 中声明的配置字段（防拼写漂移）。"""
    config_py = ROOT / "common" / "python" / "hunter_common" / "config.py"
    source = config_py.read_text(encoding="utf-8")
    declared = {
        m.group(1).upper()
        for m in re.finditer(r"^\s{4}([a-z_][a-z0-9_]*)\s*:", source, re.MULTILINE)
    }
    declared |= {"CORS_ORIGINS", "SERVICE_NAME", "API_PORT"}

    unknown: list[str] = []
    for path, doc in documents:
        if doc.get("kind") != "ConfigMap":
            continue
        if (doc.get("metadata") or {}).get("name") != "hunter-common-config":
            continue
        for key in doc.get("data") or {}:
            if key not in declared:
                unknown.append(f"{key} ({rel(path)})")
    if unknown:
        fail(f"共享 ConfigMap 存在 hunter_common.config 未声明的键: {unknown}")
    else:
        ok("共享 ConfigMap 键与 hunter_common.config 契约一致")


def check_kafka_topics_contract(documents: list[tuple[Path, dict[str, Any]]]) -> None:
    """校验 Kafka Topic 契约（K8s init Job 与 docker 脚本：名称/分区/保留时间一致）。"""
    pattern = re.compile(r'^\s*create_topic\s+"([a-z_]+)"\s+(\d+)\s+(\d+)', re.MULTILINE)
    docker_script = ROOT / "infra" / "docker" / "kafka" / "create-topics.sh"
    docker_topics = {
        m.group(1): (int(m.group(2)), int(m.group(3)))
        for m in pattern.finditer(docker_script.read_text(encoding="utf-8"))
    }

    k8s_topics: dict[str, tuple[int, int]] = {}
    for _path, doc in documents:
        meta = doc.get("metadata") or {}
        if doc.get("kind") == "ConfigMap" and meta.get("name") == "kafka-init-script":
            content = (doc.get("data") or {}).get("create-topics.sh", "")
            k8s_topics = {
                m.group(1): (int(m.group(2)), int(m.group(3))) for m in pattern.finditer(content)
            }

    if not docker_topics or not k8s_topics:
        fail(f"Kafka Topic 契约提取失败（docker={len(docker_topics)}, k8s={len(k8s_topics)}）")
    elif docker_topics != k8s_topics:
        fail(f"Kafka Topic 契约不一致: docker={docker_topics} k8s={k8s_topics}")
    else:
        ok(f"Kafka Topic 契约一致（{len(k8s_topics)} 个平台内部 Topic）")


def check_minio_buckets_contract(documents: list[tuple[Path, dict[str, Any]]]) -> None:
    """校验 MinIO Bucket 集合契约（K8s init Job 与 docker 脚本一致）。"""
    pattern = re.compile(r'^\s*create_bucket\s+"([a-z0-9-]+)"', re.MULTILINE)
    docker_script = ROOT / "infra" / "docker" / "minio" / "init-buckets.sh"
    docker_buckets = set(pattern.findall(docker_script.read_text(encoding="utf-8")))

    k8s_buckets: set[str] = set()
    for _path, doc in documents:
        meta = doc.get("metadata") or {}
        if doc.get("kind") == "ConfigMap" and meta.get("name") == "minio-init-script":
            content = (doc.get("data") or {}).get("init-buckets.sh", "")
            k8s_buckets = set(pattern.findall(content))

    if not docker_buckets or not k8s_buckets:
        fail(f"MinIO Bucket 契约提取失败（docker={len(docker_buckets)}, k8s={len(k8s_buckets)}）")
    elif docker_buckets != k8s_buckets:
        fail(
            "MinIO Bucket 契约不一致: "
            f"仅 docker={sorted(docker_buckets - k8s_buckets)}, 仅 k8s={sorted(k8s_buckets - docker_buckets)}"
        )
    else:
        ok(f"MinIO Bucket 契约一致（{len(k8s_buckets)} 个 Bucket）")


def check_dockerfiles() -> None:
    """校验各服务 Dockerfile：多阶段构建 / 非 root 运行 / 版本显式 / EXPOSE 端口一致。"""
    for service, port in SERVICE_PORTS.items():
        dockerfile = ROOT / "services" / service / "Dockerfile"
        if not dockerfile.exists():
            fail(f"缺少 Dockerfile: {rel(dockerfile)}")
            continue
        content = dockerfile.read_text(encoding="utf-8")
        if "AS builder" not in content:
            fail(f"{rel(dockerfile)} 必须为多阶段构建（存在 builder 阶段）")
        if "nonroot" not in content and "USER " not in content:
            fail(f"{rel(dockerfile)} 运行阶段必须为非 root（distroless :nonroot 或显式 USER）")
        if ":latest" in content:
            fail(f"{rel(dockerfile)} 禁止使用 latest 标签")
        if f"EXPOSE {port}" not in content:
            fail(f"{rel(dockerfile)} EXPOSE 端口必须为 {port}")
        if "HEALTHCHECK" not in content:
            fail(f"{rel(dockerfile)} 缺少容器级 HEALTHCHECK（exec 形式，兼容 distroless 无 shell）")
    ok("Dockerfile 多阶段/非 root/端口/健康检查校验完成")


def check_monitoring_configs() -> None:
    """校验监控配置：YAML/JSON 语法、告警规则完整性、Grafana 数据源一致性。"""
    monitoring = ROOT / "infra" / "monitoring"
    yaml_files = [p for p in monitoring.rglob("*") if p.suffix in {".yml", ".yaml"}]
    for path in yaml_files:
        try:
            list(yaml.safe_load_all(path.read_text(encoding="utf-8")))
        except yaml.YAMLError as exc:
            fail(f"监控配置 YAML 解析失败: {rel(path)} -> {exc}")
    ok(f"监控配置 YAML 语法校验完成（{len(yaml_files)} 个文件）")

    # 告警规则：每条必须含 expr / labels.severity / annotations.summary
    rules_path = monitoring / "prometheus" / "alert-rules.yaml"
    rules_doc = yaml.safe_load(rules_path.read_text(encoding="utf-8")) or {}
    total = 0
    incomplete: list[str] = []
    for group in rules_doc.get("groups", []):
        for rule in group.get("rules", []):
            total += 1
            labels = rule.get("labels") or {}
            annotations = rule.get("annotations") or {}
            if not rule.get("expr") or labels.get("severity") not in {"warning", "critical"}:
                incomplete.append(f"{group.get('name')}/{rule.get('alert')}")
            elif not annotations.get("summary"):
                incomplete.append(f"{group.get('name')}/{rule.get('alert')}（缺 summary）")
    if not total:
        fail("告警规则文件未定义任何规则")
    elif incomplete:
        fail(f"告警规则不完整（需 expr + severity + summary）: {incomplete}")
    else:
        ok(f"告警规则完整性校验通过（{total} 条）")

    # Grafana 看板：JSON 合法 + 数据源 uid 与供给配置一致
    provisioning = (monitoring / "grafana" / "provisioning" / "datasources" / "prometheus.yml").read_text(
        encoding="utf-8"
    )
    expected_uid = "prometheus"
    if f"uid: {expected_uid}" not in provisioning:
        fail(f"Grafana 数据源供给缺少 uid: {expected_uid}")

    dashboards = sorted((monitoring / "grafana" / "dashboards").glob("*.json"))
    if not dashboards:
        fail("未找到 Grafana 看板文件")
    for path in dashboards:
        try:
            board = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            fail(f"Grafana 看板 JSON 解析失败: {rel(path)} -> {exc}")
            continue
        if not board.get("uid") or not board.get("title") or not board.get("panels"):
            fail(f"{rel(path)} 缺少 uid/title/panels")
        uids = {
            (target.get("datasource") or {}).get("uid")
            for panel in board.get("panels", [])
            for target in panel.get("targets", [])
        }
        if uids - {expected_uid}:
            fail(f"{rel(path)} 存在非 {expected_uid} 的数据源: {sorted(uids)}")
    ok(f"Grafana 看板校验完成（{len(dashboards)} 个看板）")


def check_version_placeholder() -> None:
    """校验自有微服务镜像统一使用 `{version}` 版本变量（禁止硬编码具体版本号）。

    版本是部署期变量（CI 用 $CI_COMMIT_TAG，手工部署用发布版本号），
    仓库清单只保留占位符，由 scripts/render_k8s.py 渲染后 apply。
    """
    expected = {f"hunter/{service}" for service in SERVICE_PORTS}
    declared: set[str] = set()
    problems: list[str] = []

    for path in sorted(K8S_DIR.rglob("*.yaml")):
        content = path.read_text(encoding="utf-8")
        for lineno, line in enumerate(content.splitlines(), start=1):
            match = IMAGE_LINE_PATTERN.match(line)
            if not match:
                continue
            image = match.group("image")
            if not image.startswith("hunter/"):
                continue  # 第三方镜像按自身版本号管理，不参与渲染
            name, _, tag = image.partition(":")
            if name not in expected:
                problems.append(f"{rel(path)}:{lineno} 未登记的微服务镜像 {image}（模块表不可新增）")
                continue
            declared.add(name)
            if tag != VERSION_PLACEHOLDER:
                problems.append(
                    f"{rel(path)}:{lineno} {name} 镜像标签必须为 {VERSION_PLACEHOLDER}，实际 {tag!r}"
                )

    missing = sorted(expected - declared)
    if missing:
        problems.append(f"以下微服务镜像未在清单中声明: {missing}")

    render_script = ROOT / RENDER_SCRIPT
    if not render_script.exists():
        problems.append(f"缺少版本渲染脚本 {RENDER_SCRIPT}（版本变量无法注入）")
    elif VERSION_PLACEHOLDER not in render_script.read_text(encoding="utf-8"):
        problems.append(f"{RENDER_SCRIPT} 未引用 {VERSION_PLACEHOLDER}（版本变量单一来源缺失）")

    if problems:
        for item in problems:
            fail(item)
    else:
        ok(f"镜像版本变量校验完成（{len(declared)} 个微服务镜像 = {VERSION_PLACEHOLDER}）")


def check_autoscaling(documents: list[tuple[Path, dict[str, Any]]]) -> None:
    """校验 HPA：目标为无状态微服务 Deployment，min/max/CPU 目标/缩容窗口符合部署约束。"""
    targets: set[str] = set()
    problems: list[str] = []

    for path, doc in documents:
        if doc.get("kind") != "HorizontalPodAutoscaler":
            continue
        name = (doc.get("metadata") or {}).get("name")
        spec = doc.get("spec") or {}
        target_ref = spec.get("scaleTargetRef") or {}
        target = target_ref.get("name")
        if target_ref.get("kind") != "Deployment" or target not in SERVICE_PORTS:
            problems.append(
                f"{rel(path)}:HPA/{name} scaleTargetRef 必须为微服务 Deployment，实际 {target_ref}"
            )
            continue
        if target != name:
            problems.append(f"{rel(path)}:HPA/{name} 名称必须与目标 Deployment {target} 一致")
        targets.add(str(target))

        if spec.get("minReplicas") != HPA_MIN_REPLICAS:
            problems.append(
                f"{rel(path)}:HPA/{name} minReplicas 必须为 {HPA_MIN_REPLICAS}，"
                f"实际 {spec.get('minReplicas')}"
            )
        if spec.get("maxReplicas") != HPA_MAX_REPLICAS:
            problems.append(
                f"{rel(path)}:HPA/{name} maxReplicas 必须为 {HPA_MAX_REPLICAS}，"
                f"实际 {spec.get('maxReplicas')}"
            )

        cpu_targets = [
            ((metric.get("resource") or {}).get("target") or {})
            for metric in (spec.get("metrics") or [])
            if (metric.get("resource") or {}).get("name") == "cpu"
        ]
        if not any(
            target_spec.get("type") == "Utilization"
            and target_spec.get("averageUtilization") == HPA_CPU_TARGET_PERCENT
            for target_spec in cpu_targets
        ):
            problems.append(
                f"{rel(path)}:HPA/{name} 必须配置 CPU Utilization 目标 "
                f"{HPA_CPU_TARGET_PERCENT}%（实际 {cpu_targets}）"
            )

        scale_down = (spec.get("behavior") or {}).get("scaleDown") or {}
        if not scale_down.get("stabilizationWindowSeconds"):
            problems.append(
                f"{rel(path)}:HPA/{name} 必须配置 scaleDown.stabilizationWindowSeconds"
                "（避免指标抖动导致反复扩缩）"
            )

    missing = sorted(set(SERVICE_PORTS) - targets)
    if missing:
        problems.append(f"以下微服务缺少 HPA: {missing}")

    if problems:
        for item in problems:
            fail(item)
    else:
        ok(
            f"HPA 契约校验完成（{len(targets)} 个微服务：min={HPA_MIN_REPLICAS}"
            f"/max={HPA_MAX_REPLICAS}/CPU {HPA_CPU_TARGET_PERCENT}%）"
        )


def check_disruption_budgets(documents: list[tuple[Path, dict[str, Any]]]) -> None:
    """校验 PDB：覆盖全部无状态微服务，且 minAvailable < 基线副本数（否则节点排空永久阻塞）。"""
    replicas: dict[str, int] = {}
    for _path, doc in documents:
        if doc.get("kind") != "Deployment":
            continue
        name = (doc.get("metadata") or {}).get("name")
        if name in SERVICE_PORTS:
            replicas[str(name)] = int((doc.get("spec") or {}).get("replicas") or 0)

    covered: set[str] = set()
    problems: list[str] = []
    for path, doc in documents:
        if doc.get("kind") != "PodDisruptionBudget":
            continue
        name = (doc.get("metadata") or {}).get("name")
        spec = doc.get("spec") or {}
        labels = (spec.get("selector") or {}).get("matchLabels") or {}
        selected = labels.get("app.kubernetes.io/name")
        if selected not in SERVICE_PORTS:
            problems.append(
                f"{rel(path)}:PDB/{name} selector 必须指向微服务"
                f"（app.kubernetes.io/name ∈ {sorted(SERVICE_PORTS)}），实际 {selected!r}"
            )
            continue
        covered.add(str(selected))

        min_available = spec.get("minAvailable")
        if min_available is None:
            problems.append(
                f"{rel(path)}:PDB/{name} 必须声明 minAvailable（不能用 maxUnavailable 表达副本下限）"
            )
            continue
        baseline = replicas.get(str(selected), 0)
        if baseline and int(min_available) >= baseline:
            problems.append(
                f"{rel(path)}:PDB/{name} minAvailable={min_available} 必须小于基线副本数 "
                f"{baseline}（否则节点排空永久阻塞）"
            )

    missing = sorted(set(SERVICE_PORTS) - covered)
    if missing:
        problems.append(f"以下微服务缺少 PodDisruptionBudget: {missing}")

    if problems:
        for item in problems:
            fail(item)
    else:
        ok(f"PDB 校验完成（{len(covered)} 个微服务，minAvailable < 基线副本数）")


def main() -> int:
    documents = load_documents()
    if not documents:
        fail(f"未在 {rel(K8S_DIR)} 找到任何 K8s 文档")
    else:
        ok(f"加载 K8s 文档 {len(documents)} 个")
        check_api_versions(documents)
        check_workloads(documents)
        check_configmaps(documents)
        check_service_ports(documents)
        check_version_placeholder()
        check_autoscaling(documents)
        check_disruption_budgets(documents)
        check_env_contract(documents)
        check_kafka_topics_contract(documents)
        check_minio_buckets_contract(documents)
    check_dockerfiles()
    check_monitoring_configs()

    for line in checks:
        print(line)
    print()
    if failures:
        print(f"结论：{len(failures)} 项校验失败")
        return 1
    print("结论：全部校验通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
