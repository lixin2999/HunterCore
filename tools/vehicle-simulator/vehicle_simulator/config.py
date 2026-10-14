"""模拟器配置（CLI 参数 + 环境变量；车端 Kafka 身份 = SCRAM user 为 vehicle_id）。

对齐 contracts/kafka/topics.yaml#defaults：车端↔平台强制 SASL_SSL + SCRAM-SHA-512，
严格 mTLS（broker ssl.client.auth=required 时须出示客户端证书）。本地开发若 broker 为
PLAINTEXT，可用 ``--security-protocol PLAINTEXT`` 覆盖。所有可调项禁止硬编码到业务代码。
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass


@dataclass
class SimConfig:
    """车辆模拟器运行配置。"""

    vehicle_id: str
    bootstrap: str = "localhost:9093"
    # ---------- Kafka 安全 ----------
    security_protocol: str = "SASL_SSL"        # PLAINTEXT / SSL / SASL_PLAINTEXT / SASL_SSL
    sasl_mechanism: str = "SCRAM-SHA-512"
    scram_user: str | None = None              # 默认 = vehicle_id（车端身份约定）
    scram_password: str | None = None
    ssl_cafile: str | None = None
    ssl_certfile: str | None = None            # mTLS 客户端证书
    ssl_keyfile: str | None = None             # mTLS 客户端私钥
    ssl_keypassword: str | None = None
    # ---------- Topic（按 vehicle_id 派生，契约模板） ----------
    consumer_group: str | None = None          # 默认 vehicle-sim-{vehicle_id}
    # ---------- 频率/时长 ----------
    telemetry_hz: float = 10.0
    health_hz: float = 1.0
    duration_s: float = 0.0                    # 0 = 一直运行（Ctrl-C 退出）
    start_soc: int = 80
    # ---------- 视频 ----------
    srs_rtmp: str | None = None                # 基址 rtmp://{ip}:1935/remote_control
    srs_whip: str | None = None                # 基址 http://{ip}:1985/rtc/v1/whip/（无 ffmpeg 回退）
    stream_prefix: str = "hunter"              # 与 RC_SRS_STREAM_PREFIX 对齐（流名 {prefix}_{vid}_{sid}）
    video: bool = True
    video_width: int = 1280
    video_height: int = 720
    video_fps: int = 30
    # ---------- 契约目录（schema 自校验） ----------
    contract_dir: str | None = None
    # ---------- 回执 ----------
    ack_control_frames: bool = True            # 对每个控制帧回投 command_result

    @property
    def effective_scram_user(self) -> str:
        return self.scram_user or self.vehicle_id

    def remote_control_topic(self) -> str:
        return f"hunter.{self.vehicle_id}.remote_control"

    def telemetry_topic(self) -> str:
        return f"hunter.{self.vehicle_id}.telemetry"

    def health_topic(self) -> str:
        return f"hunter.{self.vehicle_id}.health"

    def command_result_topic(self) -> str:
        return f"hunter.{self.vehicle_id}.command_result"

    def stream_name(self, session_id: str) -> str:
        """SRS 流名（与 remote-control srs_stream_name 一致：{prefix}_{vid}_{sid}）。"""
        return f"{self.stream_prefix}_{self.vehicle_id}_{session_id}"

    def rtmp_url_for(self, session_id: str) -> str | None:
        """车端 RTMP 推流完整地址（{base}/{stream}；base 未配置返回 None）。"""
        if not self.srs_rtmp:
            return None
        return f"{self.srs_rtmp.rstrip('/')}/{self.stream_name(session_id)}"

    def whip_url_for(self, session_id: str) -> str | None:
        """WHIP 推流完整地址（base + ?app=remote_control&stream=...；base 未配置返回 None）。"""
        if not self.srs_whip:
            return None
        base = self.srs_whip
        sep = "&" if "?" in base else "?"
        return f"{base}{sep}app=remote_control&stream={self.stream_name(session_id)}"

    def consumer_group_id(self) -> str:
        return self.consumer_group or f"vehicle-sim-{self.vehicle_id}"


def _env(name: str, default: str | None = None) -> str | None:
    val = os.environ.get(name)
    return val if val not in (None, "") else default


def default_contract_dir() -> str:
    """探测 contracts/kafka：从本文件向上查找（工具位于 tools/vehicle-simulator 下）。"""
    here = os.path.dirname(os.path.abspath(__file__))
    for base in (here, os.path.dirname(here), os.path.dirname(os.path.dirname(here))):
        candidate = os.path.join(base, "contracts", "kafka")
        if os.path.isdir(candidate):
            return candidate
    return os.path.join(os.getcwd(), "contracts", "kafka")


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="vehicle-simulator",
        description="HunterCore 内置车辆模拟器（端到端演示/自测远程操控数据链路）",
    )
    p.add_argument("--vehicle-id", required=True, help="车辆 ID（同时作为 Kafka SCRAM 用户名与消息 key）")
    p.add_argument("--bootstrap", default=_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093"), help="Kafka broker（逗号分隔）")
    p.add_argument("--scram-user", default=_env("KAFKA_SASL_USERNAME"), help="SCRAM 用户名（默认 = --vehicle-id）")
    p.add_argument("--scram-pass", default=_env("KAFKA_SASL_PASSWORD"), help="SCRAM 口令")
    p.add_argument(
        "--security-protocol",
        default=_env("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
        choices=["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"],
    )
    p.add_argument("--sasl-mechanism", default=_env("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"))
    p.add_argument("--ssl-cafile", default=_env("KAFKA_SSL_CAFILE"))
    p.add_argument("--ssl-certfile", default=_env("KAFKA_SSL_CERTFILE"))
    p.add_argument("--ssl-keyfile", default=_env("KAFKA_SSL_KEYFILE"))
    p.add_argument("--ssl-keypassword", default=_env("KAFKA_SSL_KEYPASSWORD"))
    p.add_argument("--consumer-group", default=_env("SIM_CONSUMER_GROUP"), help="remote_control 消费组（默认 vehicle-sim-{vid}）")
    p.add_argument("--telemetry-hz", type=float, default=float(_env("SIM_TELEMETRY_HZ", "10")))
    p.add_argument("--health-hz", type=float, default=float(_env("SIM_HEALTH_HZ", "1")))
    p.add_argument("--duration", type=float, default=float(_env("SIM_DURATION", "0")), help="运行秒数（0=一直运行）")
    p.add_argument("--start-soc", type=int, default=int(_env("SIM_START_SOC", "80")))
    p.add_argument("--srs-rtmp", default=_env("SIM_SRS_RTMP"), help="SRS RTMP 基址，如 rtmp://IP:1935/remote_control")
    p.add_argument("--srs-whip", default=_env("SIM_SRS_WHIP"), help="SRS WHIP 基址（无 ffmpeg 回退），如 http://IP:1985/rtc/v1/whip/")
    p.add_argument("--stream-prefix", default=_env("RC_SRS_STREAM_PREFIX", "hunter"))
    p.add_argument("--no-video", action="store_true", help="禁用视频推流（仅数据链路自测）")
    p.add_argument("--no-ack", action="store_true", help="禁用控制帧 command_result 回执")
    p.add_argument("--contract-dir", default=_env("SIM_CONTRACT_DIR"), help="contracts/kafka 目录（schema 自校验）")
    return p


def from_args(argv: list[str] | None = None) -> SimConfig:
    ns = build_arg_parser().parse_args(argv)
    return SimConfig(
        vehicle_id=ns.vehicle_id,
        bootstrap=ns.bootstrap,
        security_protocol=ns.security_protocol,
        sasl_mechanism=ns.sasl_mechanism,
        scram_user=ns.scram_user,
        scram_password=ns.scram_password,
        ssl_cafile=ns.ssl_cafile,
        ssl_certfile=ns.ssl_certfile,
        ssl_keyfile=ns.ssl_keyfile,
        ssl_keypassword=ns.ssl_keypassword,
        consumer_group=ns.consumer_group,
        telemetry_hz=ns.telemetry_hz,
        health_hz=ns.health_hz,
        duration_s=ns.duration,
        start_soc=ns.start_soc,
        srs_rtmp=ns.srs_rtmp,
        srs_whip=ns.srs_whip,
        stream_prefix=ns.stream_prefix,
        video=not ns.no_video,
        contract_dir=ns.contract_dir or default_contract_dir(),
        ack_control_frames=not ns.no_ack,
    )
