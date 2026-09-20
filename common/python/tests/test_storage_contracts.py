"""数据存储契约测试：redis-keys.yaml / object-storage.yaml ↔ 服务契约 ↔ MinIO 初始化脚本。

契约（单一事实来源）：
  ``contracts/database/redis-keys.yaml``     —— Redis Key 设计（系统关键约束第 8 条）
  ``contracts/database/object-storage.yaml`` —— MinIO Bucket 规划（系统关键约束第 7 条）

校验维度：契约基准值（名称/类型/TTL/生命周期/SSE）→ 各服务 OpenAPI ``x-hunter-service`` 声明
→ ``infra/docker/minio/init-buckets.sh`` 与 ``infra/k8s/jobs/minio-init-job.yaml`` 实测脚本。
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[3]
DB_DIR = ROOT / "contracts" / "database"
OPENAPI_DIR = ROOT / "contracts" / "openapi"
MINIO_DOCKER_INIT = ROOT / "infra" / "docker" / "minio" / "init-buckets.sh"
MINIO_K8S_INIT = ROOT / "infra" / "k8s" / "jobs" / "minio-init-job.yaml"

#: Redis 受控键（键模式 → (类型, TTL 秒)；None = 不过期）——系统关键约束第 8 条
EXPECTED_REDIS_KEYS: dict[str, tuple[str, int | None]] = {
    "session:{user_id}": ("String", 1800),  # G-04① 收紧（原系统约束第 8 条 7200）
    "vehicle:status:{vehicle_id}": ("Hash", None),
    "vehicle:online:set": ("Set", None),
    "rate_limit:{ip}:{api}": ("String", 60),
    "ota:progress:{task_id}": ("Hash", 86400),
    "rc:session:{vehicle_id}": ("Hash", None),
    "cache:scene:{scene_id}": ("String(JSON)", 3600),
    "rc:lock:{vehicle_id}": ("String", 30),
}

#: MinIO Bucket（名称 → 过期天数；None = 永久）——系统关键约束第 7 条（名称不可更改）
EXPECTED_BUCKETS: dict[str, int | None] = {
    "hunter-raw-data": 30,
    "hunter-rosbag": 30,  # Tag 级规则（G-12）：hunter-retention=regular 30 天、event 永久
    "hunter-video": 90,
    "hunter-ota-packages": None,
    "hunter-reports": None,
    "hunter-logs": 30,
    "hunter-scene-assets": None,
}

#: hunter-rosbag Tag 级生命周期（G-12：对象 Tag 选择器 → 过期天数；None = 无过期规则）
EXPECTED_ROSBAG_TAG_DAYS: dict[str, int | None] = {
    "hunter-retention=regular": 30,
    "hunter-retention=event": None,
}

#: 预签名 URL 有效期（系统关键约束第 7 条：上传 1 小时 / 下载 15 分钟）
EXPECTED_PRESIGN: dict[str, int] = {
    "upload_expires_in_seconds": 3600,
    "download_expires_in_seconds": 900,
}

#: 各服务契约必须声明的预签名 TTL（object-storage.yaml cross_check 第 3 条）
EXPECTED_SERVICE_PRESIGN: dict[str, set[int]] = {
    "data-collector": {3600, 900},
    "data-analytics": {3600, 900},
    "ota-service": {3600, 900},
    "scene-service": {900},
    "remote-control": {900},
}

#: 声明 MinIO Bucket 的服务契约清单（object-storage.yaml cross_check.service_contracts）
SERVICE_CONTRACTS = tuple(EXPECTED_SERVICE_PRESIGN)

REDIS_KEY_NAMING_RE = re.compile(r"^[a-z0-9_:{}-]+$")
MINIO_CREATE_BUCKET_RE = re.compile(r'create_bucket\s+"([\w.-]+)"')
MINIO_ADD_EXPIRY_RE = re.compile(r'add_expiry\s+"([\w.-]+)"\s+(\d+)(?:\s+"([^"]*)")?')
MINIO_ENCRYPT_BUCKET_RE = re.compile(r'encrypt_bucket\s+"([\w.-]+)"')
PRESIGN_CONFIG_TTL_RE = re.compile(r"[A-Z_]*(?:PRESIGN)[A-Z_]*(?:EXPIRE)[A-Z_]*（默认\s*(\d+)）")


def load_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def redis_contract() -> dict[str, Any]:
    return load_yaml(DB_DIR / "redis-keys.yaml")


def storage_contract() -> dict[str, Any]:
    return load_yaml(DB_DIR / "object-storage.yaml")


def service_node(service: str) -> dict[str, Any]:
    return load_yaml(OPENAPI_DIR / f"{service}.yaml").get("x-hunter-service") or {}


def contract_keys() -> dict[str, dict[str, Any]]:
    return {str(entry["pattern"]): entry for entry in redis_contract()["keys"]}


def contract_buckets() -> dict[str, dict[str, Any]]:
    return {str(entry["name"]): entry for entry in storage_contract()["buckets"]}


def normalize_redis_declarations(node: Any) -> tuple[list[dict[str, Any]], list[str]]:
    """归一化 ``x-hunter-service.redis_keys``：兼容「对象列表」与「read: [字符串]」两种形态。"""
    if isinstance(node, list):
        return [item for item in node if isinstance(item, dict) and item.get("pattern")], []
    if isinstance(node, dict):
        entries: list[dict[str, Any]] = []
        loose: list[str] = []
        for section in ("read", "write"):
            for item in node.get(section) or []:
                if isinstance(item, str):
                    loose.append(item)
                elif isinstance(item, dict) and item.get("pattern"):
                    entries.append(item)
        return entries, loose
    return [], []


def collect_presign_ttls(node: Any, in_presign: bool = False) -> list[int]:
    """递归收集契约中的预签名有效期（预签名上下文 + 键名含 expire 的整数）。"""
    found: list[int] = []
    if isinstance(node, dict):
        for key, value in node.items():
            name = str(key).lower()
            context = in_presign or "presign" in name
            if isinstance(value, bool):
                continue
            if isinstance(value, int) and context and "expire" in name:
                found.append(value)
            elif isinstance(value, (dict, list)):
                found.extend(collect_presign_ttls(value, context))
    elif isinstance(node, list):
        for item in node:
            found.extend(collect_presign_ttls(item, in_presign))
    return found


def parse_minio_script(text: str) -> tuple[set[str], dict[str, list[tuple[int, str | None]]]]:
    """解析 MinIO 初始化脚本：create_bucket 集合 + add_expiry 规则（bucket → [(天数, 选择器)]）。"""
    created = set(MINIO_CREATE_BUCKET_RE.findall(text))
    rules: dict[str, list[tuple[int, str | None]]] = {}
    for bucket, days, selector in MINIO_ADD_EXPIRY_RE.findall(text):
        rules.setdefault(bucket, []).append((int(days), selector or None))
    return created, rules


def expected_expiry_rules() -> dict[str, list[tuple[int, str | None]]]:
    rules: dict[str, list[tuple[int, str | None]]] = {
        name: ([] if days is None else [(days, None)]) for name, days in EXPECTED_BUCKETS.items()
    }
    rules["hunter-rosbag"] = [
        (days, tag) for tag, days in EXPECTED_ROSBAG_TAG_DAYS.items() if days is not None
    ]
    return rules
def test_redis_contract_keys_and_ttls_match_constraints() -> None:
    """8 个受控键：键模式 / 类型 / TTL 与系统关键约束第 8 条逐项一致。"""
    keys = contract_keys()
    assert set(keys) == set(EXPECTED_REDIS_KEYS)
    for pattern, (expected_type, expected_ttl) in EXPECTED_REDIS_KEYS.items():
        assert keys[pattern]["type"] == expected_type, pattern
        assert keys[pattern]["ttl_seconds"] == expected_ttl, pattern
    # 命名规范 <domain>:<entity>[:<qualifier>]（全小写、冒号分隔、花括号占位符）
    for pattern in keys:
        assert REDIS_KEY_NAMING_RE.match(pattern), pattern
    assert redis_contract()["naming"]["db_index"] == 0


def test_redis_contract_keys_declare_failure_path_and_ownership() -> None:
    """每个键必须有失效路径（TTL 或主动清理）与明确的写入方/读取方。"""
    for pattern, entry in contract_keys().items():
        assert str(entry.get("lifecycle") or "").strip(), f"{pattern} 缺少失效路径"
        assert entry.get("writers"), f"{pattern} 缺少 writers"
        assert entry.get("readers"), f"{pattern} 缺少 readers"
        if entry.get("ttl_seconds") is None:
            ops = " ".join(str(op) for op in entry.get("ops") or [])
            assert "DEL" in ops or "SREM" in ops, f"{pattern} 无 TTL 时必须主动清理"
        owner = entry.get("owner_service")
        if owner is not None:
            assert owner in set(entry["writers"]) | set(entry["readers"]), pattern


def test_service_redis_declarations_subset_of_contract() -> None:
    """各服务声明使用的键 ∈ 契约；声明字段一致；声明方 ∈ writers ∪ readers。"""
    keys = contract_keys()
    for rel in redis_contract()["cross_check"]["contracts"]:
        path = ROOT / rel
        assert path.is_file(), f"契约引用的服务契约不存在: {rel}"
        service = path.stem
        node = (load_yaml(path).get("x-hunter-service") or {}).get("redis_keys")
        entries, loose = normalize_redis_declarations(node)
        for pattern in loose + [str(entry["pattern"]) for entry in entries]:
            assert pattern in keys, f"{service} 声明了未登记键 {pattern}"
            entry = keys[pattern]
            allowed = set(entry["writers"]) | set(entry["readers"])
            assert service in allowed, f"{service} / {pattern}"
        for entry in entries:
            target = keys[str(entry["pattern"])]
            if entry.get("type") is not None:
                assert entry["type"] == target["type"], f"{service} / {entry['pattern']}"
            if "ttl_seconds" in entry:
                assert entry["ttl_seconds"] == target["ttl_seconds"], (
                    f"{service} / {entry['pattern']}"
                )


def test_redis_declaration_shapes_are_normalized() -> None:
    """两种既有声明形态（对象列表 / read 字符串列表）均可解析，未知键可被检出。"""
    entries, loose = normalize_redis_declarations(
        [{"pattern": "vehicle:online:set", "type": "Set", "ttl_seconds": None}]
    )
    assert len(entries) == 1 and loose == []
    entries, loose = normalize_redis_declarations(
        {"note": "仅读取", "read": ["vehicle:online:set", "bogus:key"]}
    )
    assert entries == []
    assert loose == ["vehicle:online:set", "bogus:key"]
    assert set(loose) - set(contract_keys()) == {"bogus:key"}
    assert normalize_redis_declarations(None) == ([], [])


def test_object_storage_bucket_contract_values() -> None:
    """7 个 Bucket：名称 / 生命周期 / SSE-S3 / 读写方与系统关键约束第 7 条一致。"""
    buckets = contract_buckets()
    assert set(buckets) == set(EXPECTED_BUCKETS)
    for name, expected_days in EXPECTED_BUCKETS.items():
        entry = buckets[name]
        lifecycle = entry["lifecycle"]
        assert entry["sse"] == "sse-s3", name
        assert entry["writers"] and entry["readers"], name
        if lifecycle["mode"] == "expire":
            assert lifecycle["expire_days"] == expected_days, name
        elif lifecycle["mode"] == "permanent":
            assert expected_days is None, name
            assert lifecycle["expire_days"] is None, name
        elif lifecycle["mode"] == "mixed":
            rules = {str(rule["tags"]): rule["expire_days"] for rule in lifecycle["rules"]}
            assert rules == EXPECTED_ROSBAG_TAG_DAYS, name
            assert lifecycle.get("selector") == "tags", name
            tagging = lifecycle.get("tagging") or {}
            assert tagging.get("key") == "hunter-retention", name
            assert set(tagging.get("values") or ()) == {"regular", "event"}, name
            assert tagging.get("applied_by") and tagging.get("untagged_semantics"), name
        else:  # pragma: no cover - 契约已限定 mode 取值
            raise AssertionError(f"{name} 非法生命周期 mode={lifecycle['mode']}")


def test_object_storage_presign_policy_matches_constraints() -> None:
    """预签名策略：上传 3600s / 下载 900s / Range 分片 / 禁止落日志 / 私有 Bucket。"""
    contract = storage_contract()
    policy = contract["presign_policy"]
    for field, value in EXPECTED_PRESIGN.items():
        assert policy[field] == value, field
    assert policy["range_download"] is True
    assert policy["url_logging"] == "forbidden"
    assert policy["download_authorization"]["rule"]
    assert contract["public_access"].startswith("全部 Bucket 私有")
    assert contract["object_key_rules"]["forbidden"]


def test_minio_init_scripts_match_contract() -> None:
    """docker 脚本与 K8s Job：create_bucket 集合 / 生命周期天数 / SSE-S3 覆盖 7 个 Bucket。"""
    expected_rules = expected_expiry_rules()
    for script in (MINIO_DOCKER_INIT, MINIO_K8S_INIT):
        assert script.is_file(), f"缺少初始化脚本: {script}"
        created, rules = parse_minio_script(script.read_text(encoding="utf-8"))
        assert created == set(EXPECTED_BUCKETS), script.name
        for bucket, expected in expected_rules.items():
            assert sorted(rules.get(bucket, [])) == sorted(expected), f"{script.name} / {bucket}"
        assert set(rules) <= set(EXPECTED_BUCKETS), f"{script.name} 配置了未登记 Bucket"

    encrypted = set(MINIO_ENCRYPT_BUCKET_RE.findall(MINIO_K8S_INIT.read_text(encoding="utf-8")))
    assert encrypted == set(EXPECTED_BUCKETS), "K8s Job 的 SSE-S3 未覆盖全部 Bucket"


def test_service_minio_declarations_match_contract() -> None:
    """各服务声明的 Bucket ⊆ 契约、声明方 ∈ writers ∪ readers、生命周期与预签名 TTL 一致。"""
    buckets = contract_buckets()
    for rel in storage_contract()["cross_check"]["service_contracts"]:
        path = ROOT / rel
        assert path.is_file(), f"契约引用的服务契约不存在: {rel}"
        service = path.stem
        node = service_node(service)
        for bucket in node.get("minio_buckets") or []:
            assert bucket in buckets, f"{service} 声明了未登记 Bucket {bucket}"
            entry = buckets[str(bucket)]
            allowed = set(entry["writers"]) | set(entry["readers"])
            assert service in allowed, f"{service} / {bucket}"
        for bucket, text in (node.get("minio_bucket_lifecycle") or {}).items():
            days = [int(value) for value in re.findall(r"(\d+)\s*天", str(text))]
            expected_days = EXPECTED_BUCKETS[str(bucket)]
            if expected_days is None:
                assert not days, f"{service}: {bucket} 应为永久保留，声明为 {days} 天"
            else:
                assert expected_days in days, f"{service}: {bucket} 未声明 {expected_days} 天"
        raw_text = path.read_text(encoding="utf-8")
        ttls = set(collect_presign_ttls(load_yaml(path))) | {
            int(value) for value in PRESIGN_CONFIG_TTL_RE.findall(raw_text)
        }
        assert EXPECTED_SERVICE_PRESIGN[service] <= ttls, f"{service} 预签名 TTL 声明缺失"
        assert ttls <= set(EXPECTED_PRESIGN.values()), f"{service} 预签名 TTL 超出契约允许值"


def test_storage_contracts_track_open_conflicts() -> None:
    """两份契约必须以既有 ``x-hunter-pending-confirmation`` 结构登记待确认项（禁止静默发明）。"""
    for name, contract in (
        ("redis-keys.yaml", redis_contract()),
        ("object-storage.yaml", storage_contract()),
    ):
        items = contract["x-hunter-pending-confirmation"]["items"]
        assert items, f"{name} 缺少待确认项"
        for item in items:
            assert set(item) == {"id", "question", "impact", "contract_decision"}, f"{name}: {item}"
            assert item["impact"] and item["contract_decision"], f"{name} #{item['id']}"
    # rosbag 前缀语义冲突必须留痕：原阻塞冲突已由 G-12 按 Tag 生命周期定稿（方案 c）
    items = storage_contract()["x-hunter-pending-confirmation"]["items"]
    rosbag = next(item for item in items if "rosbag" in item["question"])
    assert "regular/" in rosbag["question"] and "阻塞" in rosbag["impact"]
    assert "hunter-retention" in rosbag["contract_decision"]
