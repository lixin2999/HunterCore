"""中间件客户端辅助：Kafka 生产/消费/管理、S3(MinIO) SigV4 签名与 REST 调用。

- Kafka：bootstrap/分区/保留时间全部来自 ``contracts/kafka``（禁止硬编码 Topic 名）
- S3：使用标准库实现 AWS SigV4（query 预签名 + header 签名），
  无需 minio/boto3 SDK 即可完成 Bucket 与对象链路验证（依赖最小化）
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import hmac
import json
import time
import urllib.parse
import uuid
from typing import Any

from tests.support import contracts


class BrokerUnavailableError(RuntimeError):
    """中间件不可用（由夹具转为 skip，不静默通过）。"""


# ---------------------------------------------------------------- Kafka

def _import_confluent() -> Any:
    """惰性导入 confluent_kafka（缺失时给出安装提示）。"""
    try:
        import confluent_kafka
    except ModuleNotFoundError as exc:  # pragma: no cover - 环境相关
        raise BrokerUnavailableError("缺少 confluent-kafka：pip install confluent-kafka") from exc
    return confluent_kafka


def producer_config(bootstrap: str, *, client_id: str = "hunter-l5-test") -> dict[str, Any]:
    """生产者配置：压缩/批量/重试来自契约（测试环境为 PLAINTEXT，仅本机容器）。"""
    return {
        "bootstrap.servers": bootstrap,
        "client.id": client_id,
        "compression.type": "lz4",  # 车端生产者配置（系统关键约束第 4 条）
        "linger.ms": 5,
        "batch.size": 16384,
        "retries": 3,
        "enable.idempotence": False,  # 契约要求 acks=1 场景允许重复，消费端需幂等
    }


def consumer_config(
    bootstrap: str,
    group_id: str,
    *,
    auto_offset_reset: str = "earliest",
) -> dict[str, Any]:
    """消费者配置：服务端消费者使用手动提交（契约：处理成功后提交）。"""
    return {
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "auto.offset.reset": auto_offset_reset,
        "enable.auto.commit": False,
        "enable.partition.eof": False,
    }


def ensure_topics(bootstrap: str, topics: dict[str, dict[str, int]], *, timeout_s: float = 30.0) -> None:
    """按契约创建 Topic（已存在则跳过）。

    ``topics``: ``{topic_name: {"partitions": N, "retention_ms": M}}``，
    全部取自 ``contracts/kafka/topics.yaml``（禁止在测试内硬编码分区数/保留时间）。
    """
    confluent_kafka = _import_confluent()
    admin = confluent_kafka.admin.AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics(timeout=timeout_s).topics)
    pending: dict[str, Any] = {}
    for name, spec in topics.items():
        if name in existing:
            continue
        config = {"retention.ms": str(int(spec["retention_ms"]))} if "retention_ms" in spec else None
        pending[name] = confluent_kafka.admin.NewTopic(
            name, num_partitions=int(spec["partitions"]), replication_factor=1, config=config
        )
    if not pending:
        return
    for name, future in admin.create_topics(list(pending.values())).items():
        try:
            future.result(timeout=timeout_s)
        except Exception as exc:
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise BrokerUnavailableError(f"创建 Topic {name} 失败：{exc}") from exc


def topic_metadata(bootstrap: str, topic: str) -> dict[int, int]:
    """返回 Topic 分区 -> leader id（验证分区数与可用性）。"""
    confluent_kafka = _import_confluent()
    admin = confluent_kafka.admin.AdminClient({"bootstrap.servers": bootstrap})
    metadata = admin.list_topics(timeout=30).topics.get(topic)
    if metadata is None:
        raise BrokerUnavailableError(f"Topic 不存在：{topic}")
    return {partition: meta.leader for partition, meta in metadata.partitions.items()}


def topic_config(bootstrap: str, topic: str, keys: list[str] | None = None) -> dict[str, str]:
    """回读 Topic 级配置（如 ``retention.ms`` / ``cleanup.policy``）。

    未显式设置的配置项由 broker 默认值决定，此时 **不返回该键**（调用方跳过比对），
    避免把测试环境的 broker 默认值误判为契约漂移。
    """
    confluent_kafka = _import_confluent()
    admin = confluent_kafka.admin.AdminClient({"bootstrap.servers": bootstrap})
    futures = admin.describe_configs(
        [confluent_kafka.admin.ConfigResource(confluent_kafka.admin.RESOURCE_TOPIC, topic)]
    )
    config: dict[str, str] = {}
    for future in futures.values():
        described = future.result(timeout=30)
        for name, entry in described.items():
            if keys is None or name in keys:
                config[name] = str(entry.value)
    return config


def produce(
    bootstrap: str,
    topic: str,
    key: str,
    payload: dict[str, Any],
    *,
    timeout_s: float = 20.0,
) -> dict[str, Any]:
    """生产一条消息（key = vehicle_id，保证单车辆有序），同步等待投递结果。"""
    confluent_kafka = _import_confluent()
    producer = confluent_kafka.Producer(producer_config(bootstrap))
    result: dict[str, Any] = {}

    def _on_delivery(err: Any, msg: Any) -> None:
        """投递回调：记录错误/分区/offset（测试断言依据）。"""
        result["error"] = None if err is None else str(err)
        result["partition"] = None if msg is None else msg.partition()
        result["offset"] = None if msg is None else msg.offset()

    from tests.support import messages  # 局部导入避免循环依赖

    producer.produce(topic, key=key.encode("utf-8"), value=messages.encode(payload), callback=_on_delivery)
    remaining = producer.flush(timeout_s)
    if remaining:
        raise BrokerUnavailableError(f"消息未在 {timeout_s}s 内投递完成（剩余 {remaining}）")
    if result.get("error"):
        raise BrokerUnavailableError(f"投递失败：{result['error']}")
    return result


def consume(
    bootstrap: str,
    topic: str,
    *,
    group_id: str | None = None,
    max_messages: int = 1,
    timeout_s: float = 30.0,
    auto_commit: bool = False,
) -> list[dict[str, Any]]:
    """消费消息直到收满 ``max_messages`` 或超时；返回解析后的 JSON 列表。

    默认不自动提交 offset（契约：处理成功后手动提交），由调用方显式 commit。
    """
    confluent_kafka = _import_confluent()
    group = group_id or f"l5-test-{uuid.uuid4().hex[:8]}"
    consumer = confluent_kafka.Consumer(consumer_config(bootstrap, group))
    consumer.subscribe([topic])
    collected: list[dict[str, Any]] = []
    deadline = time.monotonic() + timeout_s
    try:
        while len(collected) < max_messages and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            collected.append(json.loads(msg.value().decode("utf-8")))
            if not auto_commit:
                consumer.commit(msg, asynchronous=False)
    finally:
        consumer.close()
    return collected


# ---------------------------------------------------------------- S3 / MinIO（SigV4，标准库实现）

#: 签名算法与固定项（AWS SigV4）
S3_ALGORITHM = "AWS4-HMAC-SHA256"
S3_TERMINATOR = "aws4_request"
S3_UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD"
#: MinIO 默认区域（与 infra/docker 初始化脚本一致；可用环境变量覆盖）
S3_REGION = "us-east-1"


@dataclasses.dataclass(frozen=True)
class S3Credentials:
    """对象存储访问凭证（测试环境由环境变量注入，禁止硬编码到用例断言）。"""

    access_key: str
    secret_key: str
    region: str = S3_REGION


def _sign(key: bytes, message: str) -> bytes:
    """HMAC-SHA256。"""
    return hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()


def _signing_key(secret_key: str, date_stamp: str, region: str) -> bytes:
    """派生签名密钥（kDate -> kRegion -> kService -> kSigning）。"""
    k_date = _sign(("AWS4" + secret_key).encode("utf-8"), date_stamp)
    k_region = _sign(k_date, region)
    k_service = _sign(k_region, "s3")
    return _sign(k_service, S3_TERMINATOR)


def presign_url(
    endpoint: str,
    bucket: str,
    key: str,
    *,
    method: str = "GET",
    expires_in: int,
    credentials: S3Credentials,
    now: dt.datetime | None = None,
) -> str:
    """生成 SigV4 预签名 URL（query 认证），``expires_in`` 精度为秒。

    预签名有效期由契约规定（上传 1 小时 / 下载 15 分钟），
    测试通过回读 ``X-Amz-Expires`` 校验 TTL 与契约一致。
    """
    moment = now or dt.datetime.now(dt.UTC)
    date_stamp = moment.strftime("%Y%m%d")
    amz_date = moment.strftime("%Y%m%dT%H%M%SZ")
    scope = f"{date_stamp}/{credentials.region}/s3/{S3_TERMINATOR}"
    canonical_uri = "/" + urllib.parse.quote(f"{bucket}/{key}", safe="/")
    params = {
        "X-Amz-Algorithm": S3_ALGORITHM,
        "X-Amz-Credential": f"{credentials.access_key}/{scope}",
        "X-Amz-Date": amz_date,
        "X-Amz-Expires": str(int(expires_in)),
        "X-Amz-SignedHeaders": "host",
    }
    canonical_query = "&".join(
        f"{urllib.parse.quote(name, safe='')}={urllib.parse.quote(value, safe='')}"
        for name, value in sorted(params.items())
    )
    host = urllib.parse.urlparse(endpoint).netloc
    canonical_request = "\n".join(
        [
            method.upper(),
            canonical_uri,
            canonical_query,
            f"host:{host}\n",
            "host",
            S3_UNSIGNED_PAYLOAD,
        ]
    )
    string_to_sign = "\n".join(
        [
            S3_ALGORITHM,
            amz_date,
            scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
        ]
    )
    signature = hmac.new(
        _signing_key(credentials.secret_key, date_stamp, credentials.region),
        string_to_sign.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"{endpoint.rstrip('/')}{canonical_uri}?{canonical_query}&X-Amz-Signature={signature}"


def signed_headers(
    method: str,
    url: str,
    *,
    credentials: S3Credentials,
    payload: bytes = b"",
    content_type: str | None = None,
    now: dt.datetime | None = None,
) -> tuple[dict[str, str], bytes]:
    """生成 header 方式的 SigV4 签名，返回 (headers, body) 供 REST 调用。"""
    moment = now or dt.datetime.now(dt.UTC)
    parsed = urllib.parse.urlparse(url)
    date_stamp = moment.strftime("%Y%m%d")
    amz_date = moment.strftime("%Y%m%dT%H%M%SZ")
    scope = f"{date_stamp}/{credentials.region}/s3/{S3_TERMINATOR}"
    payload_hash = hashlib.sha256(payload).hexdigest()
    headers: dict[str, str] = {
        "host": parsed.netloc,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }
    if content_type:
        headers["content-type"] = content_type
    signed_header_names = ";".join(sorted(headers))
    canonical_headers = "".join(f"{name}:{headers[name]}\n" for name in sorted(headers))
    canonical_query = "&".join(
        f"{urllib.parse.quote(name, safe='')}={urllib.parse.quote(value, safe='')}"
        for name, value in sorted(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
    )
    canonical_request = (
        f"{method.upper()}\n{parsed.path}\n{canonical_query}\n"
        f"{canonical_headers}\n{signed_header_names}\n{payload_hash}"
    )
    string_to_sign = "\n".join(
        [
            S3_ALGORITHM,
            amz_date,
            scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
        ]
    )
    signature = hmac.new(
        _signing_key(credentials.secret_key, date_stamp, credentials.region),
        string_to_sign.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    headers["authorization"] = (
        f"{S3_ALGORITHM} Credential={credentials.access_key}/{scope}, "
        f"SignedHeaders={signed_header_names}, Signature={signature}"
    )
    return headers, payload


LIFECYCLE_XML_TEMPLATE = """<LifecycleConfiguration>
  <Rule>
    <ID>{bucket}-lifecycle</ID>
    <Filter><Prefix></Prefix></Filter>
    <Status>Enabled</Status>
    <Expiration><Days>{days}</Days></Expiration>
  </Rule>
</LifecycleConfiguration>
"""


def lifecycle_xml(bucket: str, days: int) -> bytes:
    """按契约生成 Bucket 生命周期规则 XML（天数来自 object-storage 契约）。"""
    return LIFECYCLE_XML_TEMPLATE.format(bucket=bucket, days=days).encode("utf-8")


def contract_topics() -> dict[str, dict[str, int]]:
    """全部 Topic（平台内部 + 车端模板实例）-> {partitions, retention_ms}，取自契约。"""
    topics: dict[str, dict[str, int]] = {}
    for name, spec in contracts.platform_topics().items():
        topics[name] = {
            "partitions": int(spec["partitions"]),
            "retention_ms": int(spec["retention_ms"]),
        }
    sample_vehicle = str(contracts.schema_example("telemetry")["vehicle_id"])
    for spec in contracts.vehicle_topics().values():
        name = contracts.render_topic(spec["name"], sample_vehicle)
        topics[name] = {
            "partitions": int(spec["partitions"]),
            "retention_ms": int(spec["retention_ms"]),
        }
    broadcast = contracts.broadcast_topic()
    broadcast_spec = contracts.topic_spec(broadcast)
    topics[broadcast] = {
        "partitions": int(broadcast_spec["partitions"]),
        "retention_ms": int(broadcast_spec["retention_ms"]),
    }
    return topics


def presign_expires_seconds(url: str) -> int:
    """从预签名 URL 解析 ``X-Amz-Expires``（校验 TTL 与契约一致）。"""
    query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
    values = query.get("X-Amz-Expires")
    assert values, "预签名 URL 缺少 X-Amz-Expires"
    return int(values[0])
