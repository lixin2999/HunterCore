"""节点来源地址自学习（V1.24.0）。

契约：``contracts/openapi/scene-service.yaml#x-hunter-simulation-flow.node_dispatch.endpoint_learning``。

需求实质：HunterSim 只能出向连接平台（走 WiFi、地址由 DHCP 分配），而平台与它同网段可回访
其实时地址 ⇒ 平台的下发目标必须**从入站请求里学**，而不是写死在配置里。本模块只做「取值 +
校验」这两件事，且是纯函数（不读全局状态、不碰仓储），因此注册/心跳服务层与单元测试都能直接驱动。

取值规则（逐级降级，任一级取到合法值即停）：
1. ``X-Real-IP``（nginx 已 ``proxy_set_header X-Real-IP $remote_addr``）；
2. ``X-Forwarded-For`` 的**首跳**（最靠近客户端的一跳）；
3. TCP 对端 ``request.client.host``（直连部署时的唯一来源）。
第 1、2 级仅在 ``SIM_NODE_TRUST_FORWARDED_FOR=true`` 时采信 —— 客户端可伪造头，反代不可信时
必须只认传输层对端。非法形态（域名、多值串、空串、端口后缀）一律**丢弃并继续降级**，
全级失败返回 ``None``，由调用方判定为 2001「无法判定节点地址来源」。

分层：本模块不写 Redis、不拼业务错误码；端口/协议由注册请求体声明，基址组装见 :func:`build_base_url`。
"""
from __future__ import annotations

import ipaddress
from dataclasses import dataclass
from typing import Any

from fastapi import Request
from hunter_common.logging import get_logger

from app.config import Settings

logger = get_logger("app.core.client_ip")

#: 地址来源标记（契约 SimNodeData.endpoint_source 枚举，逐字对齐 real-ip|forwarded-for|socket-peer）
SOURCE_REAL_IP = "real-ip"
SOURCE_FORWARDED_FOR = "forwarded-for"
SOURCE_SOCKET_PEER = "socket-peer"

#: 反代注入的来源头（本服务只读，不改写）
REAL_IP_HEADER = "x-real-ip"
FORWARDED_FOR_HEADER = "x-forwarded-for"


@dataclass(frozen=True, slots=True)
class LearnedAddress:
    """学习到的节点地址（不含端口：端口/协议来自注册请求体，见 node_dispatch.endpoint_learning）。"""

    ip: str
    source: str


def normalize_ip(value: Any) -> str | None:
    """字面量 IP 校验并归一（IPv4 点分十进制 / IPv6 压缩形态）；非法返回 ``None``。

    用 ``ipaddress`` 而非正则：域名、``1.2.3.4:8080``、``1.2.3.4, 5.6.7.8`` 这类形态必须一律
    判非，否则会把垃圾值写进登记表并在下发时表现为「不可达」，现场很难反查到「头被污染」。
    """
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return str(ipaddress.ip_address(text))
    except ValueError:
        return None


def in_allowed_networks(ip: str, networks: tuple[str, ...]) -> bool:
    """CIDR 白名单判定（``networks`` 为空 = 不限制）。非法 CIDR 条目被忽略（不放宽、也不误拒）。"""
    if not networks:
        return True
    try:
        address = ipaddress.ip_address(ip)
    except ValueError:
        return False
    matched = False
    for text in networks:
        try:
            network = ipaddress.ip_network(text.strip(), strict=False)
        except ValueError:
            logger.warning("sim_node_allowed_network_invalid", cidr=text)
            continue
        if address.version == network.version and address in network:
            matched = True
            break
    return matched


def learn_node_endpoint(request: Request, settings: Settings) -> LearnedAddress | None:
    """按头优先级自学习来源地址；全部来源不可得（或都不合法）→ ``None``。"""
    candidates: list[tuple[str, str]] = []
    if settings.sim_node_trust_forwarded_for:
        candidates.append((request.headers.get(REAL_IP_HEADER, ""), SOURCE_REAL_IP))
        forwarded = request.headers.get(FORWARDED_FOR_HEADER, "")
        # 首跳 = 最靠近客户端的地址（后续跳数是各级代理，回访它们没有意义）
        candidates.append((forwarded.split(",")[0] if forwarded else "", SOURCE_FORWARDED_FOR))
    client = getattr(request, "client", None)
    candidates.append((getattr(client, "host", "") or "", SOURCE_SOCKET_PEER))

    allowed = settings.sim_node_allowed_network_list
    for raw, source in candidates:
        ip = normalize_ip(raw)
        if ip is None:
            if raw.strip():  # 有值但不合法：留痕，便于现场判断「头被上游改写」
                logger.warning("sim_node_address_candidate_rejected", source=source)
            continue
        if not in_allowed_networks(ip, allowed):
            logger.warning("sim_node_address_not_in_allowed_networks", source=source)
            continue
        return LearnedAddress(ip=ip, source=source)
    return None


def build_base_url(ip: str, port: int, scheme: str) -> str:
    """组装节点 CARLA 管理 API 基址（IPv6 必须加方括号，否则端口分隔符会被当作地址的一部分）。"""
    host = f"[{ip}]" if ":" in ip else ip
    return f"{scheme}://{host}:{port}"


__all__ = [
    "FORWARDED_FOR_HEADER",
    "REAL_IP_HEADER",
    "SOURCE_FORWARDED_FOR",
    "SOURCE_REAL_IP",
    "SOURCE_SOCKET_PEER",
    "LearnedAddress",
    "build_base_url",
    "in_allowed_networks",
    "learn_node_endpoint",
    "normalize_ip",
]
