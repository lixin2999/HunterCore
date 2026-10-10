"""data-analytics 配置测试：写路径 database_url 的 RW 最小权限账号解析。

契约（er.md §3 附注 / db_access.write）：
- 读路径与就绪探针走只读账号（read_only_database_url，不受影响）；
- 写路径（DatabaseSessionManager）优先走专用写账号 ANALYTICS_RW_DB_USER/PASSWORD；
- 未配置 RW 专用凭据时回落共享 POSTGRES_USER（向后兼容，开发/单机形态）。
"""
from __future__ import annotations

from urllib.parse import quote

from app.config import Settings

BASE = {
    "postgres_host": "db",
    "postgres_port": 5432,
    "postgres_user": "hunter",
    "postgres_password": "shared-pw",
    "postgres_db": "hunter_core",
}


def test_database_url_falls_back_to_shared_postgres_user() -> None:
    """未配置 RW 专用账号 → 写路径连接串 = 基类共享账号（向后兼容）。"""
    s = Settings(**BASE, analytics_rw_db_user="", analytics_rw_db_password="")
    assert s.database_url == "postgresql+asyncpg://hunter:shared-pw@db:5432/hunter_core"


def test_database_url_prefers_dedicated_rw_account() -> None:
    """配置 RW 专用账号 → 写路径连接串走 hunter_analytics_rw，口令做 URL 转义。"""
    s = Settings(
        **BASE,
        analytics_rw_db_user="hunter_analytics_rw",
        analytics_rw_db_password="s3cr#t",
    )
    assert s.database_url == (
        f"postgresql+asyncpg://hunter_analytics_rw:{quote('s3cr#t')}@db:5432/hunter_core"
    )


def test_read_only_database_url_independent_of_rw() -> None:
    """只读连接串始终走 ANALYTICS_RO_DB_*，不受 RW 配置影响（审查 Y10）。"""
    s = Settings(
        **BASE,
        analytics_ro_db_user="hunter_analytics_ro",
        analytics_ro_db_password="ro-pw",
        analytics_rw_db_user="hunter_analytics_rw",
        analytics_rw_db_password="rw-pw",
    )
    assert s.read_only_database_url == "postgresql+asyncpg://hunter_analytics_ro:ro-pw@db:5432/hunter_core"
    assert s.database_url != s.read_only_database_url
