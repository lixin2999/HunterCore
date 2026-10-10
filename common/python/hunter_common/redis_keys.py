"""Redis 受控键构造常量（``contracts/database/redis-keys.yaml`` 的代码镜像）。

契约规则（redis-keys.yaml 禁止绕过）：
  - 规则 1：键模式是唯一来源，禁止在任何服务/作业中新增键模式或改写拼写；
  - 规则 2：键值拼接必须经共享库键构造常量，业务代码禁止硬编码键字面量。

本模块把「键模式常量 + 占位符替换」收敛到单一位置，供**写入端**（微服务、Flink 实时作业）
与**读取端**共同引用，杜绝跨端拼写漂移（典型后果：Flink 写 ``analytics:ingest_latency``、
看板读错拼写 → ``ingest_latency_ms_p95`` 恒降级 null）。

设计纪律：纯字符串常量，**不依赖** redis 客户端 / DB / 配置，可被无 redis 运行时的
Flink 作业安全 import（redis-keys.yaml pending #7 落地的 constants_module）。

校验：``common/python/tests/test_storage_contracts.py`` 断言 ``CONTROLLED_PATTERNS``
与 redis-keys.yaml ``keys[].pattern`` 集合完全一致（新增/改名键模式必须先改契约再改本模块）。
"""
from __future__ import annotations

# =====================================================================
# 键模式常量（值 = redis-keys.yaml keys[].pattern 逐字镜像；禁止改写大小写/分隔符）
# =====================================================================

#: 会话 JWT（api-gateway 写读；TTL 1800s，G-04① 收紧）
SESSION = "session:{user_id}"
#: 车辆最新状态读模型 Hash（data-collector 写，多服务只读；不过期）
VEHICLE_STATUS = "vehicle:status:{vehicle_id}"
#: 在线车辆集合 Set（data-collector 写；不过期）
VEHICLE_ONLINE_SET = "vehicle:online:set"
#: 接口限流窗口计数器（各服务写读；TTL 60s）
RATE_LIMIT = "rate_limit:{ip}:{api}"
#: OTA 升级进度热点视图 Hash（ota-service 独占；TTL 86400s）
OTA_PROGRESS = "ota:progress:{task_id}"
#: 远程操控会话唯一事实来源 Hash（remote-control 独占；会话期间不过期）
RC_SESSION = "rc:session:{vehicle_id}"
#: 场景详情缓存 String(JSON)（scene-service 独占；TTL 3600s）
CACHE_SCENE = "cache:scene:{scene_id}"
#: 远程操控互斥锁 String（remote-control 独占；redis-py client.lock 令牌，TTL 30s）
RC_LOCK = "rc:lock:{vehicle_id}"
#: 遥测入库延迟车队级 P95 指标 String(JSON)（data-analytics Flink 作业写、看板只读；TTL 300s）
#: 无占位符的固定键——Flink 写端（ingest_latency_job）与看板读端（IngestLatencyRedisReader）共用本常量。
ANALYTICS_INGEST_LATENCY = "analytics:ingest_latency"

#: 受控键模式全集（test_storage_contracts.py 据此比对 redis-keys.yaml）。
CONTROLLED_PATTERNS: frozenset[str] = frozenset(
    {
        SESSION,
        VEHICLE_STATUS,
        VEHICLE_ONLINE_SET,
        RATE_LIMIT,
        OTA_PROGRESS,
        RC_SESSION,
        CACHE_SCENE,
        RC_LOCK,
        ANALYTICS_INGEST_LATENCY,
    }
)


# =====================================================================
# 键构造函数（占位符替换的唯一入口；实际键 = 模式替换占位符后的字符串）
# =====================================================================


def session_key(user_id: str | int) -> str:
    """``session:{user_id}``。"""
    return SESSION.format(user_id=user_id)


def vehicle_status_key(vehicle_id: str) -> str:
    """``vehicle:status:{vehicle_id}``。"""
    return VEHICLE_STATUS.format(vehicle_id=vehicle_id)


def rate_limit_key(ip: str, api: str) -> str:
    """``rate_limit:{ip}:{api}``。"""
    return RATE_LIMIT.format(ip=ip, api=api)


def ota_progress_key(task_id: str | int) -> str:
    """``ota:progress:{task_id}``。"""
    return OTA_PROGRESS.format(task_id=task_id)


def rc_session_key(vehicle_id: str) -> str:
    """``rc:session:{vehicle_id}``。"""
    return RC_SESSION.format(vehicle_id=vehicle_id)


def cache_scene_key(scene_id: str) -> str:
    """``cache:scene:{scene_id}``。"""
    return CACHE_SCENE.format(scene_id=scene_id)


def rc_lock_key(vehicle_id: str) -> str:
    """``rc:lock:{vehicle_id}``（互斥锁名 = 实际键）。"""
    return RC_LOCK.format(vehicle_id=vehicle_id)


__all__ = [
    "ANALYTICS_INGEST_LATENCY",
    "CACHE_SCENE",
    "CONTROLLED_PATTERNS",
    "OTA_PROGRESS",
    "RATE_LIMIT",
    "RC_LOCK",
    "RC_SESSION",
    "SESSION",
    "VEHICLE_ONLINE_SET",
    "VEHICLE_STATUS",
    "cache_scene_key",
    "ota_progress_key",
    "rate_limit_key",
    "rc_lock_key",
    "rc_session_key",
    "session_key",
    "vehicle_status_key",
]
