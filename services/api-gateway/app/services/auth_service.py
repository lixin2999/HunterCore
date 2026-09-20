"""统一认证业务逻辑（设计文档 3.2.1 登录流程 / 14.1 会话管理 / 14.5 暴力破解防护）。

关键设计（契约依据）：
- 登录失败统一返回 1001（不区分用户名不存在/密码错误/账号禁用，防用户名枚举）
- IP 锁定（14.5 节）：失败计数键 ``rate_limit:{ip}:login-fail``（复用契约键模式
  ``rate_limit:{ip}:{api}``，{api} 槽位=login-fail）；锁定期间返回 429 + Retry-After
- 会话（redis-keys.yaml 第 1 条）：``session:{user_id}`` = Access Token 原样存储，
  TTL 1800s（G-04① 收紧）；登录写入 / 刷新覆盖 / 登出 DEL（撤销语义见 core/auth.py 模块注释）
- MFA(TOTP)：G-23 已在数据库契约增 `users.mfa_enabled` / `users.totp_secret` 字段，
  但 TOTP 签发/校验流程（含密钥加密存储）尚未实现，当前仍按契约
  「账号未启用 MFA 时忽略」处理；启用前需同步实现校验逻辑与前端绑定流程
"""
from __future__ import annotations

from typing import Any, Final
from uuid import UUID

from hunter_common.database.enums import UserStatus
from hunter_common.exceptions import (
    AuthenticationError,
    InvalidParameterError,
    ServiceUnavailableError,
    TokenExpiredError,
)
from hunter_common.logging import get_logger

from app.config import Settings, settings
from app.core.auth import load_session_token, session_key
from app.core.rate_limit import RateLimiter, RateLimitExceeded, rate_limit_key
from app.core.security import (
    create_access_token,
    create_refresh_token,
    decode_access_token_ignoring_expiry,
    decode_refresh_token,
    hash_password,
    verify_password,
)
from app.repositories.user_repository import UserRepository
from app.schemas.auth import (
    ChangePasswordRequest,
    LoginRequest,
    RefreshTokenRequest,
    TokenPair,
    UserProfile,
)

logger = get_logger("app.services.auth_service")

LOGIN_SCOPE: Final[str] = "login"              # rate_limit:{ip}:login（登录频率限制）
LOGIN_FAILURE_SCOPE: Final[str] = "login-fail"  # rate_limit:{ip}:login-fail（失败锁定）

__all__ = ["LOGIN_FAILURE_SCOPE", "LOGIN_SCOPE", "AuthService"]


class AuthService:
    """统一认证服务（登录 / 刷新 / 注销 / 当前用户）。

    ``redis`` 为 RedisManager 或接口兼容对象（鸭子类型，便于单测注入）。
    """

    def __init__(
        self,
        users: UserRepository,
        redis: Any,
        settings: Settings = settings,
    ) -> None:
        self._users = users
        self._redis = redis
        self._settings = settings
        self._limiter = RateLimiter(redis)

    # =================================================================
    # 登录（POST /api/v1/user/login）
    # =================================================================
    async def login(self, payload: LoginRequest, client_ip: str) -> TokenPair:
        """登录流程（契约 login description；设计文档 3.2.1 / 14.5）。"""
        cfg = self._settings
        # 1) IP 级登录频率限制（14.5 暴力破解防护；阈值 env 可覆盖）
        await self._limiter.check(client_ip, LOGIN_SCOPE, cfg.login_rate_limit_per_ip)
        # 2) IP 锁定快速拒绝（14.5）：失败计数已达阈值 → 任何尝试（含正确密码）429
        await self._ensure_not_locked(client_ip)
        # 3) 身份核验：DB 异常 → 503；凭证错误/账号非启用 → 统一 1001（防枚举）
        user = await self._get_user_by_username(payload.username)
        reason = self._check_credentials(user, payload.password)
        if reason is not None:
            await self._register_failure(client_ip)  # 超阈值抛 RateLimitExceeded → 429
            logger.warning(
                "auth_login_failed",
                reason=reason,
                username=payload.username,
                client_ip=client_ip,
            )
            raise AuthenticationError
        # 4) MFA(TOTP)：契约「账号未启用 MFA 时忽略」；TOTP 密文字段未入数据库契约
        #    （见模块 docstring），故此处无校验分支 —— 启用前先扩展契约。
        # 5) 角色/权限（RBAC 五表展开）
        roles = await self._users.get_enabled_role_codes(user.user_id)
        permissions = await self._users.get_permission_codes(user.user_id)
        # 6) 更新 last_login_time（契约 login description 明确要求）
        await self._users.touch_last_login(user.user_id)
        # 7) 签发 Token 对 + 写会话（redis-keys 第 1 条：SET session:{user_id} EX 1800，G-04①）
        pair = await self._issue_pair(
            user_id=str(user.user_id),
            username=user.username,
            real_name=user.real_name,
            roles=roles,
            permissions=permissions,
            must_change_password=bool(getattr(user, "must_change_password", False)),
        )
        await self._reset_failures(client_ip)  # 成功后清零失败计数
        logger.info(
            "auth_login_succeeded",
            user_id=str(user.user_id),
            username=user.username,
            client_ip=client_ip,
        )
        return pair

    # =================================================================
    # 刷新（POST /api/v1/user/refresh）
    # =================================================================
    async def refresh(self, payload: RefreshTokenRequest) -> TokenPair:
        """刷新 Token 对（契约：失效/过期 → 1003 需重新登录；一次一换防重放）。"""
        claims = decode_refresh_token(payload.refresh_token, self._settings)  # → 1003
        user_id = str(claims["sub"])
        paired_access_jti = claims.get("at_jti")
        if not paired_access_jti:
            raise TokenExpiredError  # 缺配对声明（伪造/旧版令牌）
        stored = await load_session_token(self._redis, user_id)  # Redis 挂 → 503
        if not stored:
            # 会话已注销（登出后禁止刷新，需重新登录；契约幂等撤销语义）
            raise TokenExpiredError
        stored_claims = decode_access_token_ignoring_expiry(stored, self._settings)
        if stored_claims.get("jti") != paired_access_jti:
            # 旧 Refresh Token 已随轮换作废（防重放；redis-keys pending #6 选项②）
            raise TokenExpiredError
        user = await self._get_user_by_id(UUID(user_id))
        if user is None or user.status is not UserStatus.ENABLED:
            logger.warning("auth_refresh_user_invalid", user_id=user_id)
            raise AuthenticationError  # 用户已删除/禁用/锁定 → 重新登录（1001）
        roles = await self._users.get_enabled_role_codes(user.user_id)
        permissions = await self._users.get_permission_codes(user.user_id)
        pair = await self._issue_pair(
            user_id=user_id,
            username=user.username,
            real_name=user.real_name,
            roles=roles,
            permissions=permissions,
            must_change_password=bool(getattr(user, "must_change_password", False)),
        )
        logger.info("auth_token_refreshed", user_id=user_id)
        return pair

    # =================================================================
    # 注销（POST /api/v1/user/logout；幂等）
    # =================================================================
    async def logout(self, user_id: str) -> None:
        """注销：DEL 会话（Access/Refresh 因会话撤销即刻失效；重复调用无副作用）。

        契约「Access Token 加入黑名单直至自然过期」由会话撤销实现
        （redis-keys.yaml pending #6 选项②：删除会话 + jti 校验替代黑名单）。
        """
        try:
            await self._redis.delete(session_key(user_id))
        except Exception as exc:
            logger.error("session_delete_failed", user_id=user_id)
            raise ServiceUnavailableError("服务不可用") from exc
        logger.info("auth_logout", user_id=user_id)

    # =================================================================
    # 当前用户（GET /api/v1/user/me）
    # =================================================================
    async def me(self, claims: dict[str, Any]) -> UserProfile:
        """当前用户：JWT 解析结果（sub/username/roles/permissions）+ 用户资料。

        ``real_name`` / ``must_change_password`` 取自 DB（JWT 载荷无该字段）；DB 异常时
        降级为 None/False 并告警（JWT 已严格校验，资料字段仅展示用途，不打断会话
        查询；契约允许 503，这里选择部分降级以保证可用性 —— 需人工确认，见 README 待确认项）。
        """
        user_id = str(claims["sub"])
        real_name: str | None = None
        must_change_password = False
        try:
            flags = await self._users.get_security_flags(UUID(user_id))
            if flags is not None:
                real_name, must_change_password = flags
        except Exception:  # noqa: BLE001 - 展示字段降级（JWT 已严格校验，不打断会话查询）
            logger.warning("me_profile_lookup_degraded", user_id=user_id)
        return UserProfile(
            user_id=UUID(user_id),
            username=str(claims.get("username") or ""),
            real_name=real_name,
            roles=list(claims.get("roles") or []),
            permissions=list(claims.get("permissions") or []),
            must_change_password=must_change_password,
        )

    # =================================================================
    # 修改密码（POST /api/v1/user/change-password；G-06 首登强制改密）
    # =================================================================
    async def change_password(
        self, claims: dict[str, Any], payload: ChangePasswordRequest
    ) -> None:
        """改密：验旧口令（错 → 1001）→ 强度由 schema 保证 → 更新哈希并复位标志。

        新口令与旧口令相同返回 2001（避免无意义轮换假成功）；当前会话不强制撤销
        （契约 change-password description 明确）。
        """
        user_id = str(claims["sub"])
        user = await self._get_user_by_id(UUID(user_id))
        if user is None or user.status is not UserStatus.ENABLED:
            raise AuthenticationError  # 用户已删除/禁用/锁定 → 重新登录（1001）
        if not verify_password(payload.old_password, user.password_hash):
            logger.warning("auth_change_password_bad_old_password", user_id=user_id)
            raise AuthenticationError  # 统一 1001，不区分原因（防枚举/探测）
        if payload.new_password == payload.old_password:
            raise InvalidParameterError("新口令不能与当前口令相同")
        await self._users.change_password(
            user.user_id, hash_password(payload.new_password, rounds=self._settings.password_bcrypt_rounds)
        )
        logger.info("auth_password_changed", user_id=user_id)

    # =================================================================
    # 内部方法
    # =================================================================
    @staticmethod
    def _check_credentials(user: Any, password: str) -> str | None:
        """凭证/状态核验；返回失败原因（None=通过）。统一 1001，不向客户端泄露原因。"""
        if user is None:
            return "unknown_username"
        if not verify_password(password, user.password_hash):
            return "bad_password"
        if user.status is not UserStatus.ENABLED:
            return f"user_{user.status.value}"  # disabled / locked
        return None

    async def _issue_pair(
        self,
        *,
        user_id: str,
        username: str,
        real_name: str | None,
        roles: list[str],
        permissions: list[str],
        must_change_password: bool = False,
    ) -> TokenPair:
        """签发 Access+Refresh 并写会话（会话强依赖：写失败 → 503，不发半截会话）。"""
        cfg = self._settings
        access_token, access_jti = create_access_token(
            user_id=user_id,
            username=username,
            roles=roles,
            permissions=permissions,
            settings=cfg,
        )
        refresh_token, _ = create_refresh_token(
            user_id=user_id, access_jti=access_jti, settings=cfg
        )
        try:
            await self._redis.set(
                session_key(user_id),
                access_token,
                expire_seconds=cfg.jwt_access_token_expire_minutes * 60,  # 30min（redis-keys 第 1 条，G-04①）
            )
        except Exception as exc:
            logger.error("session_write_failed", user_id=user_id)
            raise ServiceUnavailableError("服务不可用") from exc
        return TokenPair(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=cfg.jwt_access_token_expire_minutes * 60,          # 1800（G-04①）
            refresh_expires_in=cfg.jwt_refresh_token_expire_days * 86400,  # 604800
            user=UserProfile(
                user_id=UUID(user_id),
                username=username,
                real_name=real_name,
                roles=roles,
                permissions=permissions,
                must_change_password=must_change_password,
            ),
        )

    async def _ensure_not_locked(self, client_ip: str) -> None:
        """IP 锁定快速拒绝（14.5 节）：失败计数已达阈值 → 429（不做凭证校验）。

        与 :meth:`_register_failure` 同语义（计数 ≥ 阈值即锁定）；读取失败
        fail-open（基础防护由 per-IP 频率限制兜底）。
        """
        try:
            raw = await self._redis.get(rate_limit_key(client_ip, LOGIN_FAILURE_SCOPE))
        except Exception:  # noqa: BLE001 - fail-open：锁定检查非强依赖（per-IP 限流兜底）
            logger.warning("login_lock_check_backend_unavailable_fail_open", client_ip=client_ip)
            return
        if raw and int(raw) >= self._settings.login_max_failures:
            raise RateLimitExceeded(
                limit=self._settings.login_max_failures,
                retry_after=self._settings.login_lock_window_seconds,
            )

    async def _register_failure(self, client_ip: str) -> None:
        """登记登录失败并执行 IP 锁定（14.5 节；阈值 env 覆盖）。

        键 ``rate_limit:{ip}:login-fail``（复用契约键模式）；计数达到阈值即锁定。
        Redis 故障 fail-open（失败计数非强依赖；per-IP 限流已提供基础防护）。
        """
        try:
            count = await self._redis.incr_with_expire(
                rate_limit_key(client_ip, LOGIN_FAILURE_SCOPE),
                self._settings.login_lock_window_seconds,
            )
        except Exception:  # noqa: BLE001 - fail-open：失败计数非强依赖（per-IP 限流兜底）
            logger.warning("login_failure_backend_unavailable_fail_open", client_ip=client_ip)
            return
        if count >= self._settings.login_max_failures:
            raise RateLimitExceeded(
                limit=self._settings.login_max_failures,
                retry_after=self._settings.login_lock_window_seconds,
            )

    async def _reset_failures(self, client_ip: str) -> None:
        """登录成功后清零失败计数（fail-open：清理失败不影响登录结果）。"""
        try:
            await self._redis.delete(rate_limit_key(client_ip, LOGIN_FAILURE_SCOPE))
        except Exception:  # noqa: BLE001 - fail-open：清理失败不影响登录结果
            logger.warning("login_failure_reset_failed", client_ip=client_ip)

    async def _get_user_by_username(self, username: str) -> Any:
        """按用户名查询（DB 异常 → 503 服务不可用）。"""
        try:
            return await self._users.get_by_username(username)
        except Exception as exc:
            logger.error("auth_db_unavailable", username=username)
            raise ServiceUnavailableError("服务不可用") from exc

    async def _get_user_by_id(self, user_id: UUID) -> Any:
        """按 ID 查询（DB 异常 → 503 服务不可用）。"""
        try:
            return await self._users.get_by_id(user_id)
        except Exception as exc:
            logger.error("auth_db_unavailable", user_id=str(user_id))
            raise ServiceUnavailableError("服务不可用") from exc