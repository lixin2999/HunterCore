"""11.2 OTA 升级端到端用例（版本仓库 → 灰度下发 → 车端状态机 → 进度汇总）。

依据（系统关键约束第 14 条 + 第 6/7/8 条）：
- 版本单调递增（防回滚）；SHA-256 完整性（6001）+ RSA-2048 验签（6002）
- 升级门禁：电量 ≥50%、静止(P 档)、网络稳定、存储 ≥2GB（不满足 → 6003）
- 车端状态机 IDLE→PENDING→DOWNLOAD→INSTALL→TEST→SUCCESS；
  自检失败 TEST→ROLLBACK→ROLLED_BACK/FAILED（状态名与流转不可更改）
- 灰度 5%→20%→50%→100%，每批观察 24h，成功率 ≥95% 才推进，<95% 立即暂停+告警+人工介入
- Kafka：``hunter.{v}.ota_notify``（平台→车）/ ``hunter.{v}.ota_status``（车→平台）
- Redis：``ota:progress:{task_id}`` Hash（TTL 86400s，第 8 条）
- MinIO：``hunter-ota-packages``（永久保留），预签名上传 1h / 下载 15min（第 7 条）

容器型用例（Kafka/Redis/MinIO）依赖 Docker，不可用时自动 skip（附原因）。
"""
from __future__ import annotations

import hashlib
import time
from typing import Any

import pytest

from tests.support import broker, contracts, flow, messages, thresholds

pytestmark = [pytest.mark.e2e, pytest.mark.l5_case("11.2")]

#: 车辆号取自契约示例（ota_status.schema.json 优先，回退 telemetry）
VEHICLE_ID: str = str(contracts.schema_example("ota_status")["vehicle_id"])


# ---------------------------------------------------------------- 版本与包安全

def test_version_monotonic_increase_and_anti_rollback() -> None:
    """版本号必须单调递增：目标 ≤ 当前 → (False, 6003)，禁止降级/重刷。"""
    ok, err = flow.validate_version_monotonic(current_code=12, target_code=13)
    assert ok and err is None
    blocked, err = flow.validate_version_monotonic(current_code=13, target_code=13)
    assert not blocked and err == 6003
    blocked, err = flow.validate_version_monotonic(current_code=13, target_code=12)
    assert not blocked and err == 6003


def test_package_integrity_then_signature_validation() -> None:
    """先完整性（SHA-256/MD5 → 6001）后签名（RSA-2048 → 6002），顺序不可颠倒。"""
    digest = hashlib.sha256(b"ota-payload").hexdigest()
    ok, err = flow.verify_package(
        actual_sha256=digest, expected_sha256=digest, signature_valid=True
    )
    assert ok and err is None
    ok, err = flow.verify_package(
        actual_sha256="0" * 64, expected_sha256=digest, signature_valid=True
    )
    assert not ok and err == 6001
    ok, err = flow.verify_package(
        actual_sha256=digest,
        expected_sha256=digest,
        signature_valid=False,
        actual_md5="a" * 32,
        expected_md5="b" * 32,
    )
    assert not ok and err == 6001, "MD5 不匹配时必须先报完整性错误（6001），而非签名（6002）"
    ok, err = flow.verify_package(
        actual_sha256=digest, expected_sha256=digest, signature_valid=False
    )
    assert not ok and err == 6002


def test_upgrade_preconditions_gate() -> None:
    """升级门禁逐项拦截（错误码 6003）：电量≥50%、静止(P 档)、网络稳定、存储≥2GB。"""
    ok = flow.UpgradePrecondition(
        battery_soc=80, parked=True, network_stable=True, free_storage_gb=4.0
    )
    assert ok.satisfied and ok.failures() == []
    low_soc = flow.UpgradePrecondition(
        battery_soc=thresholds.OTA_MIN_SOC - 1, parked=True, network_stable=True, free_storage_gb=4.0
    )
    assert not low_soc.satisfied
    assert all(code == 6003 for code, _ in low_soc.failures())
    moving = flow.UpgradePrecondition(
        battery_soc=80, parked=False, network_stable=True, free_storage_gb=4.0
    )
    assert any("静止" in text for _, text in moving.failures())
    low_disk = flow.UpgradePrecondition(
        battery_soc=80, parked=True, network_stable=True,
        free_storage_gb=thresholds.OTA_MIN_FREE_STORAGE_GB - 0.5,
    )
    assert not low_disk.satisfied
    offline = flow.UpgradePrecondition(
        battery_soc=80, parked=True, network_stable=False, free_storage_gb=4.0
    )
    assert any("网络" in text for _, text in offline.failures())

# ---------------------------------------------------------------- 车端状态机

def _walk(states: tuple[str, ...]) -> None:
    """沿状态机逐态推进（advance_ota_state 校验合法性）。"""
    current = "IDLE"
    for target in states[1:]:
        current = flow.advance_ota_state(current, target)
    assert current == states[-1]


def test_onboard_state_machine_happy_path() -> None:
    """正常路径：IDLE→PENDING→DOWNLOAD→INSTALL→TEST→SUCCESS（逐态合法流转）。"""
    _walk(thresholds.OTA_STATE_MACHINE)


def test_onboard_state_machine_rollback_paths() -> None:
    """自检失败路径：TEST→ROLLBACK→ROLLED_BACK / FAILED（终态不可再流转）。"""
    assert flow.advance_ota_state("TEST", "ROLLBACK") == "ROLLBACK"
    assert flow.advance_ota_state("ROLLBACK", "ROLLED_BACK") == "ROLLED_BACK"
    assert flow.advance_ota_state("ROLLBACK", "FAILED") == "FAILED"
    with pytest.raises(AssertionError):
        flow.advance_ota_state("ROLLED_BACK", "PENDING")
    with pytest.raises(AssertionError):
        flow.advance_ota_state("SUCCESS", "PENDING")


def test_onboard_state_machine_rejects_illegal_jumps() -> None:
    """非法跳跃（如 IDLE→TEST、DOWNLOAD→SUCCESS）必须被拒绝。"""
    with pytest.raises(AssertionError):
        flow.advance_ota_state("IDLE", "TEST")
    with pytest.raises(AssertionError):
        flow.advance_ota_state("DOWNLOAD", "SUCCESS")


# ---------------------------------------------------------------- 灰度发布

def test_rollout_plan_matches_contract_batches() -> None:
    """灰度计划 = 5%→20%→50%→100% 四批，每批观察 24h，车辆数逐批不减、末批全量。"""
    vehicles = [f"HUNTER-{i:03d}" for i in range(1, 21)]  # 20 台样例车队
    batches = flow.plan_rollout(vehicles)
    assert [batch.percent for batch in batches] == list(thresholds.OTA_ROLLOUT_BATCHES)
    assert all(batch.observation_hours == thresholds.OTA_OBSERVE_HOURS for batch in batches)
    sizes = [len(batch.target_vehicles) for batch in batches]
    assert sizes == sorted(sizes) and sizes[-1] == len(vehicles)
    assert sizes[0] >= 1


def test_rollout_batch_gate_at_95_percent() -> None:
    """批次成功率门槛：≥95% 推进（succeeded），<95% 立即暂停（paused）。"""
    vehicles = [f"HUNTER-{i:03d}" for i in range(1, 21)]
    passing = flow.plan_rollout(vehicles)[0]
    assert passing.evaluate(success=96, failed=4) is True
    assert passing.status == "succeeded" and passing.success_rate == pytest.approx(0.96)
    exact = flow.plan_rollout(vehicles)[0]
    assert exact.evaluate(success=19, failed=1) is True, "19/20=95% 应恰好通过"
    paused = flow.plan_rollout(vehicles)[0]
    assert paused.evaluate(success=94, failed=6) is False

# ---------------------------------------------------------------- Kafka 链路（容器）

async def test_ota_downlink_and_uplink_roundtrip(kafka_bootstrap: str) -> None:
    """端到端：平台下发 ota_notify → 车端按状态机上报 ota_status → 平台消费回读。"""
    notify_topic = flow.platform_topic_of(VEHICLE_ID, "ota_notify")
    status_topic = flow.platform_topic_of(VEHICLE_ID, "ota_status")
    task_id = "task-l5-e2e"
    notify = messages.ota_notify_message(
        VEHICLE_ID,
        task_id=task_id,
        version_name="v1.2.3",
        version_code=13,
        package_md5=hashlib.md5(b"ota-payload").hexdigest(),
        package_sha256=hashlib.sha256(b"ota-payload").hexdigest(),
        signature="test-signature",
    )
    broker.produce(kafka_bootstrap, notify_topic, VEHICLE_ID, notify)
    received = broker.consume(kafka_bootstrap, notify_topic, max_messages=1, timeout_s=30)
    assert received and received[0]["task_id"] == task_id

    current = "IDLE"
    for index, target in enumerate(("PENDING", "DOWNLOAD", "INSTALL", "TEST", "SUCCESS"), start=1):
        current = flow.advance_ota_state(current, target)
        status = messages.ota_status_message(
            VEHICLE_ID, task_id, status=current, progress=index * 20
        )
        broker.produce(kafka_bootstrap, status_topic, VEHICLE_ID, status)
    statuses = broker.consume(kafka_bootstrap, status_topic, max_messages=5, timeout_s=30)
    assert [s["status"] for s in statuses] == [
        "PENDING", "DOWNLOAD", "INSTALL", "TEST", "SUCCESS"
    ]


async def test_ota_progress_hash_ttl_matches_contract(redis_handle: Any) -> None:
    """ota:progress:{task_id} Hash：写入进度后 TTL = 86400s（第 8 条，任务结束后 1 天）。"""
    redis_module = pytest.importorskip("redis.asyncio", reason="缺少 redis 依赖：pip install redis")
    task_id = "task-l5-progress"
    key = flow.ota_progress_key(task_id)
    client = redis_module.Redis(
        host=redis_handle.host, port=redis_handle.port, decode_responses=True
    )
    try:
        await client.hset(key, mapping={"completed_batches": 1, "current_batch": 2, "paused": "0"})
        await client.expire(key, thresholds.OTA_PROGRESS_TTL_SECONDS)
        ttl = await client.ttl(key)
        assert thresholds.OTA_PROGRESS_TTL_SECONDS - 60 <= ttl <= thresholds.OTA_PROGRESS_TTL_SECONDS
        snapshot = await client.hgetall(key)
        assert snapshot["current_batch"] == "2"
    finally:
        await client.aclose()


# ---------------------------------------------------------------- MinIO 包仓库（容器）

async def test_ota_package_presigned_roundtrip(s3_credentials: Any, minio_container: Any) -> None:
    """OTA 包经预签名 URL 上传/下载：SHA-256 一致；上传 1h / 下载 15min（第 7 条）。"""
    httpx = pytest.importorskip("httpx", reason="缺少 httpx 依赖")
    bucket = "hunter-ota-packages"
    assert contracts.lifecycle_expire_days(bucket) is None, "OTA 包 bucket 必须永久保留"
    key = f"{VEHICLE_ID}/l5-test-{int(time.time())}.pkg"
    payload = b"ota-payload" * 1024
    endpoint = minio_container.http()

    upload_url = broker.presign_url(
        endpoint, bucket, key, method="PUT",
        expires_in=thresholds.PRESIGN_UPLOAD_TTL_SECONDS, credentials=s3_credentials,
    )
    assert broker.presign_expires_seconds(upload_url) == thresholds.PRESIGN_UPLOAD_TTL_SECONDS
    async with httpx.AsyncClient(timeout=30.0) as client:
        put_resp = await client.put(upload_url, content=payload)
        assert put_resp.status_code == 200, f"预签名上传失败：{put_resp.text}"
        download_url = broker.presign_url(
            endpoint, bucket, key, method="GET",
            expires_in=thresholds.PRESIGN_DOWNLOAD_TTL_SECONDS, credentials=s3_credentials,
        )
        get_resp = await client.get(download_url)
        assert get_resp.status_code == 200, f"预签名下载失败：{get_resp.text}"
    assert broker.presign_expires_seconds(download_url) == thresholds.PRESIGN_DOWNLOAD_TTL_SECONDS
    assert hashlib.sha256(get_resp.content).hexdigest() == hashlib.sha256(payload).hexdigest()


def test_rollout_progress_summary() -> None:
    """进度汇总（ota:progress:{task_id} 权威口径）：批次完成数/当前批/暂停标记。"""
    vehicles = [f"HUNTER-{i:03d}" for i in range(1, 11)]
    batches = flow.plan_rollout(vehicles)
    batches[0].evaluate(success=10, failed=0)
    summary = flow.rollout_progress(batches)
    assert summary["total_batches"] == len(thresholds.OTA_ROLLOUT_BATCHES)
    assert summary["completed_batches"] == 1
    assert summary["current_batch"] == 2
    assert summary["paused"] is False


def test_ota_notify_and_status_messages_match_contract() -> None:
    """下发/上报消息契约：ota_notify（平台→车）与 ota_status（车→平台）全字段校验。"""
    task_id = "task-l5-11-2"
    notify = messages.ota_notify_message(
        VEHICLE_ID,
        task_id=task_id,
        version_name="v1.2.3",
        version_code=13,
        package_md5=hashlib.md5(b"ota-payload").hexdigest(),
        package_sha256=hashlib.sha256(b"ota-payload").hexdigest(),
        signature="test-signature",
    )
    contracts.assert_valid_message("ota_notify", notify)
    for state in ("DOWNLOAD", "INSTALL", "TEST", "SUCCESS", "ROLLED_BACK", "FAILED"):
        status = messages.ota_status_message(VEHICLE_ID, task_id, status=state, progress=50)
        contracts.assert_valid_message("ota_status", status)
