"""`hunter_flink.job_conn` 的单测（V1.19.8）：Kafka SASL 属性装配 + Redis 连接串组装。

这些函数是**纯 Python、在仓库内真实执行**的（不像三个作业的 ``main()`` 那样 ``pragma: no cover``），
所以本文件的断言就是那道防线的实体：连接参数错了会在 CI 红，而不是在服务器上表现为
「作业 RUNNING 但不出数」。

现场依据（服务器实录）：三作业 RUNNING 后 ``redis-cli GET analytics:ingest_latency`` 返回
``NOAUTH Authentication required.``，而 flink-jm/flink-tm 环境里既无 SASL 也无 Redis 凭据。
"""
from __future__ import annotations

import pytest
from hunter_flink.job_conn import (
    DEFAULT_SCRAM_LOGIN_MODULE,
    apply_kafka_properties,
    kafka_sasl_properties,
    redis_url,
)

_SASL_ENV = {
    "KAFKA_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
    "KAFKA_SASL_MECHANISM": "SCRAM-SHA-512",
    "KAFKA_SASL_USERNAME": "hunter_internal_user",
    "KAFKA_SASL_PASSWORD": "s3cret",
}


def test_plaintext_env_yields_no_sasl_properties() -> None:
    """明文形态（本地/开发）不得下发任何 sasl.* 属性——保持 V1.19.8 前的行为不变。"""
    assert kafka_sasl_properties({}) == {}
    assert kafka_sasl_properties({"KAFKA_SECURITY_PROTOCOL": "PLAINTEXT"}) == {}


def test_sasl_protocol_produces_consumer_and_producer_properties() -> None:
    props = kafka_sasl_properties(_SASL_ENV)
    assert props["security.protocol"] == "SASL_PLAINTEXT"
    assert props["sasl.mechanism"] == "SCRAM-SHA-512"
    jaas = props["sasl.jaas.config"]
    # 连接器是 shaded fat jar：默认必须用 shade 后的 login module，否则运行期 Failed to create login module
    assert jaas.startswith(DEFAULT_SCRAM_LOGIN_MODULE + " required ")
    assert 'username="hunter_internal_user"' in jaas
    assert 'password="s3cret";' in jaas
    assert set(props) == {"security.protocol", "sasl.mechanism", "sasl.jaas.config"}


@pytest.mark.parametrize(
    "env",
    [
        {**_SASL_ENV, "KAFKA_SASL_USERNAME": ""},
        {**_SASL_ENV, "KAFKA_SASL_PASSWORD": ""},
        {k: v for k, v in _SASL_ENV.items() if not k.endswith("PASSWORD")},
    ],
)
def test_sasl_protocol_without_credentials_fails_fast(env: dict[str, str]) -> None:
    """声明 SASL 却没凭据 ⇒ 提交期即 ValueError（绝不允许退化成明文连接后静默空转）。"""
    with pytest.raises(ValueError, match="KAFKA_SASL_USERNAME"):
        kafka_sasl_properties(env)


def test_login_module_is_overridable() -> None:
    """非 shaded classpath（如把连接器放进 /opt/flink/lib 的其它发行）可用环境变量改回原生类名。"""
    native = "org.apache.kafka.common.security.scram.ScramLoginModule"
    props = kafka_sasl_properties({**_SASL_ENV, "KAFKA_SASL_LOGIN_MODULE": native})
    assert props["sasl.jaas.config"].startswith(native + " required ")
    with pytest.raises(ValueError, match="不可为空"):
        kafka_sasl_properties({**_SASL_ENV, "KAFKA_SASL_LOGIN_MODULE": "  "})


def test_password_quoting_keeps_jaas_valid() -> None:
    """口令含双引号/反斜杠不得破坏 JAAS 语法（也是注入面）。"""
    props = kafka_sasl_properties({**_SASL_ENV, "KAFKA_SASL_PASSWORD": 'p"w\\d'})
    assert 'password="p\\"w\\\\d";' in props["sasl.jaas.config"]


def test_apply_kafka_properties_chains_before_build() -> None:
    class _FakeBuilder:
        def __init__(self) -> None:
            self.props: dict[str, str] = {}

        def set_property(self, key: str, value: str) -> _FakeBuilder:
            self.props[key] = value
            return self

        def build(self) -> dict[str, dict[str, str]]:
            return {"props": self.props}

    builder = apply_kafka_properties(_FakeBuilder(), kafka_sasl_properties(_SASL_ENV))
    assert builder.build()["props"]["security.protocol"] == "SASL_PLAINTEXT"
    assert apply_kafka_properties(_FakeBuilder(), {}).props == {}


def test_redis_url_prefers_segments_and_encodes_password() -> None:
    url = redis_url(
        {
            "REDIS_HOST": "redis",
            "REDIS_PORT": "6379",
            "REDIS_PASSWORD": "p@ss:w/rd#1",
            "REDIS_DB": "0",
        }
    )
    assert url == "redis://:p%40ss%3Aw%2Frd%231@redis:6379/0"


def test_redis_url_without_password_has_no_auth_segment() -> None:
    assert redis_url({"REDIS_HOST": "redis"}) == "redis://redis:6379/0"


def test_redis_url_segments_win_over_stale_url() -> None:
    """遗留的无口令 REDIS_URL 不得抢在分段凭据之前（否则现场表现为 NOAUTH 假象）。"""
    url = redis_url({"REDIS_URL": "redis://redis:6379/0", "REDIS_HOST": "redis", "REDIS_PASSWORD": "pw"})
    assert url == "redis://:pw@redis:6379/0"


def test_redis_url_falls_back_to_explicit_url_then_default() -> None:
    assert redis_url({"REDIS_URL": "redis://cache:6380/2"}) == "redis://cache:6380/2"
    assert redis_url({}) == "redis://redis:6379/0"
