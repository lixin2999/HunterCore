"""api-gateway 服务配置（pydantic-settings，全部参数来自环境变量/.env）。

非敏感配置经 K8s ConfigMap 注入（infra/k8s/services/api-gateway.yaml），
敏感配置（jwt_secret_key 等，继承自 HunterBaseConfig）经 Secret 注入环境变量。
"""
from __future__ import annotations

from hunter_common.config import HunterBaseConfig


class Settings(HunterBaseConfig):
    """api-gateway 配置；阈值/URL 全部可经环境变量覆盖（禁止硬编码业务参数）。"""

    # 服务标识与默认端口（仍可被环境变量 SERVICE_NAME / API_PORT 覆盖）
    service_name: str = "api-gateway"
    api_port: int = 8080

    # ---------- 转发目标（契约 x-hunter-gateway-routes；strip_prefix=false） ----------
    # 环境变量形如 SCENE_SERVICE_URL；K8s 集群内建议配置为 Service DNS
    scene_service_url: str = "http://localhost:8081"
    data_collector_service_url: str = "http://localhost:8082"
    data_analytics_service_url: str = "http://localhost:8083"
    ota_service_url: str = "http://localhost:8084"
    remote_control_service_url: str = "http://localhost:8085"
    # vehicle-service / user-service 归属待确认（契约 status=pending_confirmation，端口未登记）：
    # 未配置 URL 时对应前缀返回 503 + code=5001（服务不可用），禁止伪造转发
    vehicle_service_url: str | None = None
    user_service_url: str | None = None

    # 转发超时（轻量 API 面向 P95 ≤ 200ms；大文件上传等按需经环境变量调整上限）
    proxy_connect_timeout_seconds: float = 5.0
    proxy_timeout_seconds: float = 30.0
    proxy_max_connections: int = 100

    # 熔断默认策略（契约 circuit_breaker：连续 5 次失败或超时率 > 50% 熔断 30s；阈值待人工确认）
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_timeout_rate: float = 0.5
    circuit_breaker_open_seconds: float = 30.0

    # ---------- 附录 D 限流阈值（不可更改） ----------
    # 全局 10000 QPS 为集群级容量（由 Ingress/Nginx 入口承担；进程内不重复计数）
    rate_limit_global_qps: int = 10000
    rate_limit_per_user_qps: int = 100
    rate_limit_per_ip_qps: int = 200

    # 登录防护（设计文档 14.5 节：登录失败次数限制 + IP 锁定；阈值为实现默认，需人工确认）
    login_rate_limit_per_ip: int = 30      # 登录接口单 IP 每分钟上限
    login_max_failures: int = 5            # 失败锁定阈值（窗口内连续失败）
    login_lock_window_seconds: int = 900   # IP 锁定窗口（15 分钟）

    # JWT 签发方（设计文档 3.2.2 节：iss="hunter-platform"）
    jwt_issuer: str = "hunter-platform"
    password_bcrypt_rounds: int = 12       # bcrypt 轮次（user_svc.users.password_hash）
    # 反向代理后是否信任 X-Forwarded-For（K8s Ingress 覆盖该头时可开启；直连部署建议关闭防伪造）
    trust_forwarded_for: bool = True


settings = Settings()
