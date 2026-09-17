"""PostgreSQL/TimescaleDB 测试访问层（asyncpg，最小依赖）。

职责：
- 提供连接/查询/批量写入原语（表名与列名一律来自 ``contracts``，禁止硬编码）
- 提供 hypertable / 保留策略回读（校验 chunk_time_interval=1 day、retention=90 天）
- 提供异步轮询等待（``wait_until``），用于「Kafka → 入库」链路的时序断言

约束：本模块只在集成/E2E/性能测试中使用（依赖 ``pytest.mark.integration`` 夹具）。
"""
from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Iterable, Sequence
from typing import Any

from tests.support import contracts

#: 遥测时序表（DDL 契约：contracts/database/ddl/05_timeseries.sql）
TELEMETRY_TABLE = ("data_collector", "vehicle_telemetry")
#: 算法指标时序表
METRICS_TABLE = ("data_analytics", "algorithm_metrics")
#: 事件表
EVENTS_TABLE = ("data_collector", "events")


def _import_asyncpg() -> Any:
    """惰性导入 asyncpg（缺失时给出安装提示）。"""
    try:
        import asyncpg
    except ModuleNotFoundError as exc:  # pragma: no cover - 环境相关
        raise RuntimeError("缺少 asyncpg：pip install asyncpg") from exc
    return asyncpg


async def connect(handle: Any) -> Any:
    """建立 asyncpg 连接（句柄为 ``infra.PostgresHandle``，凭证取自夹具）。"""
    asyncpg = _import_asyncpg()
    return await asyncpg.connect(
        host=handle.host,
        port=handle.port,
        user=handle.user,
        password=handle.password,
        database=handle.database,
    )


async def fetch(handle: Any, sql: str, *args: Any) -> list[Any]:
    """执行查询并返回记录列表（连接即用即关）。"""
    connection = await connect(handle)
    try:
        return list(await connection.fetch(sql, *args))
    finally:
        await connection.close()


async def fetchval(handle: Any, sql: str, *args: Any) -> Any:
    """执行查询并返回单值。"""
    connection = await connect(handle)
    try:
        return await connection.fetchval(sql, *args)
    finally:
        await connection.close()


async def fetchrow(handle: Any, sql: str, *args: Any) -> Any:
    """执行查询并返回单行。"""
    connection = await connect(handle)
    try:
        return await connection.fetchrow(sql, *args)
    finally:
        await connection.close()


async def execute(handle: Any, sql: str, *args: Any) -> str:
    """执行语句（DDL/DML）。"""
    connection = await connect(handle)
    try:
        return await connection.execute(sql, *args)
    finally:
        await connection.close()


def telemetry_insert_sql(table: tuple[str, str] = TELEMETRY_TABLE) -> str:
    """生成批量插入语句（列名/列序取自 DDL 契约，幂等 ON CONFLICT DO NOTHING）。"""
    columns = contracts.table_column_names(*table)
    placeholders = ", ".join(f"${index}" for index in range(1, len(columns) + 1))
    pk = ", ".join(contracts.primary_key_columns(*table))
    return (
        f"INSERT INTO {qualified(table)} ({', '.join(columns)}) "
        f"VALUES ({placeholders}) ON CONFLICT ({pk}) DO NOTHING"
    )


def row_values(row: dict[str, Any], table: tuple[str, str] = TELEMETRY_TABLE) -> tuple[Any, ...]:
    """按 DDL 列序把行字典转为参数元组（缺列报错，防止字段名漂移静默通过）。"""
    values: list[Any] = []
    for column in contracts.table_column_names(*table):
        if column not in row:
            raise KeyError(f"行缺少 DDL 契约列 {column!r}（表 {qualified(table)}）")
        values.append(row[column])
    return tuple(values)


async def bulk_insert(
    handle: Any,
    rows: Sequence[dict[str, Any]],
    *,
    table: tuple[str, str] = TELEMETRY_TABLE,
    chunk_size: int = 2000,
) -> float:
    """批量写入（executemany，禁止逐条 commit），返回耗时秒数（≥10000 点/秒 断言依据）。"""
    connection = await connect(handle)
    sql = telemetry_insert_sql(table)
    payload = [row_values(row, table) for row in rows]
    started = time.perf_counter()
    try:
        for offset in range(0, len(payload), chunk_size):
            await connection.executemany(sql, payload[offset : offset + chunk_size])
    finally:
        await connection.close()
    return time.perf_counter() - started


async def count_rows(handle: Any, table: tuple[str, str], where: str = "", *args: Any) -> int:
    """统计行数（``where`` 为契约字面量片段，值一律参数化）。"""
    clause = f" WHERE {where}" if where else ""
    return int(await fetchval(handle, f"SELECT count(*) FROM {qualified(table)}{clause}", *args))


async def table_exists(handle: Any, table: tuple[str, str]) -> bool:
    """表是否存在（校验 DDL 契约已在容器内生效）。"""
    schema, name = table
    return bool(
        await fetchval(
            handle,
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = $1 AND table_name = $2)",
            schema,
            name,
        )
    )


async def table_columns_in_db(handle: Any, table: tuple[str, str]) -> list[str]:
    """回读实际列名（校验实现与 DDL 契约一致）。"""
    schema, name = table
    records = await fetch(
        handle,
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position",
        schema,
        name,
    )
    return [str(record["column_name"]) for record in records]


def qualified(schema_table: tuple[str, str]) -> str:
    """``(schema, table)`` -> ``schema.table``（标识符均为契约字面量）。"""
    schema, table = schema_table
    return f"{schema}.{table}"


async def hypertable_config(handle: Any, table: tuple[str, str]) -> dict[str, Any]:
    """回读 hypertable 配置：chunk_time_interval 与保留策略（对照 DDL 契约）。"""
    dimension = await fetchrow(
        handle,
        "SELECT time_interval FROM timescaledb_information.dimensions "
        "WHERE hypertable_schema = $1 AND hypertable_name = $2",
        *table,
    )
    retention = await fetchrow(
        handle,
        "SELECT config FROM timescaledb_information.jobs "
        "WHERE proc_name = 'policy_retention' AND hypertable_schema = $1 AND hypertable_name = $2",
        *table,
    )
    return {
        "is_hypertable": dimension is not None,
        "chunk_time_interval": None if dimension is None else dimension["time_interval"],
        "retention_config": None if retention is None else dict(retention["config"]),
    }


async def wait_until(
    predicate: Callable[[], Awaitable[bool]],
    *,
    timeout_s: float,
    interval_s: float = 0.2,
) -> bool:
    """轮询等待条件成立（超时返回 False，由调用方断言并给出上下文）。"""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if await predicate():
            return True
        await asyncio.sleep(interval_s)
    return False


def seed_rows(
    count: int,
    vehicle_id: str,
    *,
    base_seq: int = 1,
    start_ts: float | None = None,
) -> list[dict[str, Any]]:
    """构造 ``count`` 条时序行（复用消息工厂 + flow 映射，保证字段与契约一致）。

    时间戳按 10ms 步进，``seq`` 连续（便于丢包检测断言）；供批量写入性能与其他用例共用。
    """
    from tests.support import flow, messages  # 局部导入避免循环依赖

    base = start_ts if start_ts is not None else time.time()
    rows: list[dict[str, Any]] = []
    for index in range(count):
        payload = messages.telemetry_message(
            vehicle_id, seq=base_seq + index, timestamp=round(base + index * 0.01, 3)
        )
        rows.append(flow.telemetry_row(payload))
    return rows


def columns_of(table: tuple[str, str]) -> list[str]:
    """DDL 契约列名（测试构造行时的权威列序）。"""
    return [name for name, _type in contracts.table_columns(*table)]


def missing_columns(row: dict[str, Any], table: tuple[str, str]) -> Iterable[str]:
    """返回行中缺失的 DDL 列（用于断言消息→行映射无遗漏）。"""
    keys = set(row)
    return [column for column in columns_of(table) if column not in keys]