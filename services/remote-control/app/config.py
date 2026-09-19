"""remote-control 服务配置（pydantic-settings，全部参数来自环境变量/.env）。

取值来源（单一事实来源，业务代码禁止硬编码阈值）：
- contracts/openapi/remote-control.yaml 的 x-hunter-* 扩展（session-lifecycle /
  control-safety / control-channel / history-archive / observability）；
- 《数据采集与分析系统详细设计文档 V4.0》远程操控章节 + 系统约束第 15 条
  （20Hz / 50ms / 500ms 超时停车 / 2.0 m/s 限速 / 90 天录像保留）。

契约 pending 项对应的配置化处理：
- pending #17：rc_session_start / rc_session_end 指令类型取值经环境变量注入
  （RC_COMMAND_SESSION_START_TYPE / RC_COMMAND_SESSION_END_TYPE），契约定稿后只改配置不改代码；
- pending #10/#12：SRS 媒体面参数（WHIP/WHEP/RTMP）默认空串占位，SRS 集成定稿后回填；
- pending #18：RC_MAX_STEER_RAD 为实现侧保护上限（契约 Kafka schema 未定义 steer 取值域）。
"""
from __future__ import annotations

from hunter_common.config import HunterBaseConfig
from pydantic import model_validator


def _split_csv(raw: str) -> set[str]:
    """逗号分隔配置 → 集合（去空白、去空项）。"""
    return {item.strip() for item in raw.split(",") if item.strip()}


class Settings(HunterBaseConfig):
    """remote-control 配置（服务私有项统一 RC_ 前缀；敏感项经 K8s Secret 注入）。"""

    # ---------- 服务基础 ----------
    service_name: str = "remote-control"
    api_port: int = 8085

    # ---------- MinIO（录像/sidecar 归档；Bucket 名称契约固定不可更改） ----------
    minio_video_bucket: str = "hunter-video"
    minio_region: str = "us-east-1"

    # ---------- 公网入口（WebRTC 信令/控制通道 URL 下发用；网关 TLS 终结） ----------
    rc_public_ws_base_url: str = "wss://hunter-edge.example.com"

    # ---------- 控制安全（x-hunter-control-safety；系统约束第 15 条，不可放宽） ----------
    rc_max_speed_mps: float = 2.0          # 远程操控限速（不可上调 >2.0）
    rc_max_steer_rad: float = 0.5          # 前轮转角保护上限（pending #18 待定稿）
    rc_command_hz: int = 20                # 控制指令频率（固定 20Hz）
    rc_command_interval_ms: int = 50       # 指令间隔（固定 50ms）
    rc_command_ack_timeout_ms: int = 100   # 指令 ACK 超时（≤100ms）
    rc_stop_on_timeout_ms: int = 500       # 超时停车阈值（固定 500ms）
    rc_max_command_rate_hz: int = 25       # 单会话帧率上限（超出丢弃并计数，WS 通道使用）
    rc_session_lock_ttl_s: int = 30        # 会话互斥锁 TTL（契约固定 30s）
    # 会话 Hash 硬 TTL（审查 R7 兜底；redis-keys pending #4 建议 6h）：
    # 副本崩溃/未走 DELETE 时残留会话会永久占用车辆互斥位（后续接管恒 7001）
    rc_session_ttl_s: int = 21600

    # ---------- 会话信令（Kafka command 通道；pending #17 取值域未定稿，经配置注入） ----------
    rc_command_session_start_type: str = "rc_session_start"
    rc_command_session_end_type: str = "rc_session_end"
    rc_session_command_timeout_ms: int = 5000  # 会话信令执行超时（command.schema.json timeout_ms）

    # ---------- 会话心跳（x-hunter-session-heartbeat） ----------
    rc_heartbeat_interval_s: int = 10      # 车辆侧心跳周期（固定 10s，不可放宽）
    rc_heartbeat_degraded_misses: int = 3  # 连续丢失 N 次 → degraded（pending #4 待定稿）
    rc_heartbeat_end_after_s: int = 60     # 无心跳持续 → 平台侧结束会话（pending #4 待定稿）
    # 陈旧会话守护周期（审查 R7）：按 last_heartbeat_at 对账并强制结束失效会话
    rc_session_reaper_interval_s: int = 30

    # ---------- 视频链路（VideoConfig 契约枚举：H.264 硬编 720p@30fps） ----------
    rc_video_width: int = 1280
    rc_video_height: int = 720
    rc_video_codec: str = "h264"
    rc_video_fps: int = 30
    rc_video_bitrate_min_kbps: int = 2048
    rc_video_bitrate_max_kbps: int = 4096
    rc_video_keyframe_interval_s: int = 1
    rc_video_container: str = "mp4"

    # ---------- SRS 媒体面（pending #10/#12：集成定稿前空串占位） ----------
    rc_srs_app: str = "remote_control"
    rc_srs_stream_prefix: str = "hunter"
    rc_srs_rtmp_url: str = ""
    rc_srs_whip_url: str = ""
    rc_srs_whep_url: str = ""

    # ---------- WebRTC ICE（STUN/TURN；TURN 走 REST API 短期凭据） ----------
    rc_stun_urls: str = "stun:stun.hunter-edge.example.com:3478"
    rc_turn_urls: str = ""
    rc_turn_username: str = ""
    rc_turn_shared_secret: str = ""        # 敏感：生产经 K8s Secret 注入
    rc_turn_credential_ttl_s: int = 3600

    # ---------- 历史/归档（x-hunter-history-archive） ----------
    rc_history_retention_days: int = 90    # 录像保留（固定 90 天）
    rc_history_query_max_range_days: int = 31
    rc_presign_get_ttl_s: int = 900        # 下载预签名有效期（契约上限 900s）
    rc_sidecar_schema_version: str = "1"

    # ---------- 会话枚举扫描上限（SCAN 防放大；内部保护值，非契约项） ----------
    rc_sessions_scan_limit: int = 500

    # ---------- RBAC（remote:read / remote:create / remote:execute） ----------
    rc_read_roles: str = "admin,operator,viewer"
    rc_create_roles: str = "admin,operator"
    rc_execute_roles: str = "admin,operator"
    rc_admin_role: str = "admin"

    # ---------- 限流（附录 D：POST /remote/session 单用户 1 QPS） ----------
    rc_session_create_rate_limit_per_min: int = 1
    rate_limit_window_seconds: int = 60

    # ---------- 角色集合属性 ----------
    @property
    def rc_read_role_set(self) -> set[str]:
        """remote:read 允许角色集。"""
        return _split_csv(self.rc_read_roles)

    @property
    def rc_create_role_set(self) -> set[str]:
        """remote:create 允许角色集。"""
        return _split_csv(self.rc_create_roles)

    @property
    def rc_execute_role_set(self) -> set[str]:
        """remote:execute 允许角色集。"""
        return _split_csv(self.rc_execute_roles)

    @property
    def rc_stun_url_list(self) -> list[str]:
        """STUN 服务器地址列表（逗号分隔配置）。"""
        return sorted(_split_csv(self.rc_stun_urls))

    @property
    def rc_turn_url_list(self) -> list[str]:
        """TURN 服务器地址列表（未配置时为空 → 仅下发 STUN）。"""
        return sorted(_split_csv(self.rc_turn_urls))

    @model_validator(mode="after")
    def _validate_contract_invariants(self) -> Settings:
        """校验契约不变量（系统约束第 15 条 + VideoConfig 枚举；配置错误启动即失败）。"""
        if self.rc_command_hz != 20:
            raise ValueError("rc_command_hz 固定为 20（契约 x-hunter-control-channel）")
        if self.rc_command_interval_ms != 50:
            raise ValueError("rc_command_interval_ms 固定为 50（契约 x-hunter-control-channel）")
        if self.rc_stop_on_timeout_ms != 500:
            raise ValueError("rc_stop_on_timeout_ms 固定为 500（系统约束第 15 条）")
        if not 0 < self.rc_max_speed_mps <= 2.0:
            raise ValueError("rc_max_speed_mps 必须满足 0 < v ≤ 2.0（远程操控限速，不可上调）")
        if not 0 < self.rc_command_ack_timeout_ms <= 100:
            raise ValueError("rc_command_ack_timeout_ms 必须满足 0 < t ≤ 100ms")
        if self.rc_session_lock_ttl_s != 30:
            raise ValueError("rc_session_lock_ttl_s 固定为 30s（契约 x-hunter-control-safety）")
        if self.rc_heartbeat_interval_s != 10:
            raise ValueError("rc_heartbeat_interval_s 固定为 10s（契约 x-hunter-session-heartbeat）")
        if self.rc_history_retention_days != 90:
            raise ValueError("rc_history_retention_days 固定为 90 天（录像保留）")
        if not 0 < self.rc_presign_get_ttl_s <= 900:
            raise ValueError("rc_presign_get_ttl_s 必须满足 0 < t ≤ 900s（契约预签名上限）")
        if (self.rc_video_width, self.rc_video_height, self.rc_video_codec,
                self.rc_video_keyframe_interval_s, self.rc_video_container) != (1280, 720, "h264", 1, "mp4"):
            raise ValueError("视频链路固定为 h264/1280x720/关键帧 1s/mp4（系统约束第 15 条）")
        if not 1 <= self.rc_video_fps <= 30:
            raise ValueError("rc_video_fps 必须在 1-30 之间")
        if not 2048 <= self.rc_video_bitrate_min_kbps <= self.rc_video_bitrate_max_kbps <= 4096:
            raise ValueError("码率区间必须满足 2048 ≤ min ≤ max ≤ 4096 kbps")
        return self


settings = Settings()

