#!/usr/bin/env python3
"""HunterEdge 基础设施清单校验（L1）。

校验项：
  1. K8s YAML 语法可解析，且文档非空
  2. apiVersion 白名单（apps/v1、v1、batch/v1、networking.k8s.io/v1）
  3. 命名空间统一为 hunter-edge（Namespace 资源本身除外）
  4. 工作负载（Deployment/StatefulSet）resources.requests/limits 与三种探针齐备
  5. 镜像标签禁止 latest / 缺省（必须显式版本或发布日期）
  6. ConfigMap 不得包含敏感字段（密码/密钥/私钥）
  7. 微服务端口与设计文档端口表一致（8080-8085）
  8. Kafka Topic 契约一致性（K8s init Job vs docker-compose 脚本：名称/分区/保留）
  9. MinIO Bucket 集合一致性（K8s init Job vs docker-compose 脚本）

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
}
WORKLOAD_KINDS = {"Deployment", "StatefulSet", "DaemonSet"}
#: 应用与中间件命名空间（restricted Pod 安全标准）
NAMESPACE = "hunter-edge"
#: 允许的命名空间；monitoring 用于采集器（node-exporter 需要 host 网络/文件系统）
ALLOWED_NAMESPACES = {"hunter-edge", "monitoring"}

#: 服务名 -> 端口（设计文档模块划分，不可更改）
SERVICE_PORTS: dict[str, int] = {
    "api-gateway": 8080,
    "scene-service": 8081,
    "data-collector": 8082,
    "data-analytics": 8083,
    "ota-service": 8084,
    "remote-control": 8085,
}

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
        # restricted 校验；hunter-edge（应用/中间件/监控组件）强制非 root + drop ALL
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
    ok("Dockerfile 多阶段/非 root/端口校验完成")


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
