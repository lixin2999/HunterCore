#!/usr/bin/env python3
"""从 ``contracts/kafka`` 生成 K8s ConfigMap 清单（消费侧运行时 Schema 校验依赖）。

背景（审查 R1）：数据采集消费者以 ``KafkaConsumerManager(schema_name="auto")`` 做
**消息契约 Schema 校验**，运行时必须能访问 ``KAFKA_CONTRACT_DIR``。集群内通过本
ConfigMap 挂载（键保留相对路径，如 ``schemas/telemetry.schema.json``，
保证契约加载器按目录结构解析）。

用法::

    python scripts/generate_contracts_configmap.py            # 生成/更新清单
    python scripts/generate_contracts_configmap.py --check    # CI：校验清单与契约一致

产物：``infra/k8s/base/03-configmap-contracts.yaml``（ConfigMap ``hunter-contracts``）。
契约变更后必须重新生成（``--check`` 会在 CI 中拦截遗漏）。
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_DIR = ROOT / "contracts" / "kafka"
OUTPUT = ROOT / "infra" / "k8s" / "base" / "03-configmap-contracts.yaml"

CONFIGMAP_NAME = "hunter-contracts"
NAMESPACE = "hunter-core"

HEADER = """# =====================================================================
# HunterCore Kafka 契约 ConfigMap（由脚本生成，禁止手工编辑）
#
# 生成命令：python scripts/generate_contracts_configmap.py
# 一致性校验：python scripts/generate_contracts_configmap.py --check（CI lint 阶段）
#
# 用途：data-collector 采集消费者的运行时消息 Schema 校验（schema_name="auto"）；
# 键保留相对路径（schemas/*.schema.json），挂载到 KAFKA_CONTRACT_DIR 后
# 契约加载器即可按目录结构解析。
# =====================================================================
apiVersion: v1
kind: ConfigMap
metadata:
  name: {name}
  namespace: {namespace}
  labels:
    app.kubernetes.io/name: {name}
    app.kubernetes.io/part-of: hunter-core
    app.kubernetes.io/component: contracts
  annotations:
    hunter-core.local/generated-by: scripts/generate_contracts_configmap.py
    hunter-core.local/source: contracts/kafka
data:
"""


def collect_files() -> dict[str, str]:
    """收集运行时必需的契约文件（Topic/消费组定义 + 消息 Schema）。"""
    files: dict[str, str] = {}
    for name in ("topics.yaml", "consumer-groups.yaml"):
        path = CONTRACT_DIR / name
        files[name] = path.read_text(encoding="utf-8")
    for path in sorted((CONTRACT_DIR / "schemas").glob("*.schema.json")):
        files[f"schemas/{path.name}"] = path.read_text(encoding="utf-8")
    return files


def render(files: dict[str, str]) -> str:
    """渲染 ConfigMap YAML（键按字典序，值用字面块标量保留格式）。"""
    lines = [HEADER.format(name=CONFIGMAP_NAME, namespace=NAMESPACE)]
    for name in sorted(files):
        lines.append(f'  "{name}": |-')
        for line in files[name].rstrip("\n").splitlines():
            lines.append(f"    {line}" if line.strip() else "")
    return "\n".join(lines) + "\n"


def main(argv: list[str]) -> int:
    content = render(collect_files())
    if "--check" in argv[1:]:
        if not OUTPUT.exists():
            print(f"[FAIL] 缺少 {OUTPUT.relative_to(ROOT)}，请运行生成脚本")
            return 1
        if OUTPUT.read_text(encoding="utf-8") != content:
            print(
                f"[FAIL] {OUTPUT.relative_to(ROOT)} 与 contracts/kafka 不一致："
                "请重新运行 python scripts/generate_contracts_configmap.py"
            )
            return 1
        print(f"[PASS] {OUTPUT.relative_to(ROOT)} 与 contracts/kafka 一致")
        return 0

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(content, encoding="utf-8")
    print(f"[OK] 已生成 {OUTPUT.relative_to(ROOT)}（{len(content)} 字节）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
