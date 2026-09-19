# hunter_common —— HunterEdge 共享代码库

供所有微服务以 editable 方式安装：`pip install -e common/python`

| 模块 | 职责 |
|------|------|
| `config.HunterBaseConfig` | pydantic-settings 基础配置基类（DB/Redis/Kafka/MinIO/JWT），各服务继承并补充私有配置 |
| `logging` | structlog JSON 日志：`service`/`trace_id`/`vehicle_id` 自动注入，敏感字段（password/token/secret/key）自动脱敏 |
| `exceptions` | `HunterBaseException` 基类 + 预定义错误码（1001–7002）异常体系 + `ErrorCode` 枚举 |
| `responses` | 统一响应模型 `ApiResponse[T]`（code/message/data/request_id/timestamp）+ `success_response`/`error_response` |
| `kafka` | confluent-kafka 2.x 异步封装（**契约驱动**）：`KafkaProducerManager`（单例；按契约 acks 投递、key=vehicle_id 强制、重试+指数退避、网络中断落盘缓冲 1GB 并可重投）/ `KafkaConsumerManager`（手动提交 offset、契约 Schema 校验→DLQ、handler 重试→DLQ、幂等守卫、消费积压指标）；子模块 `contracts`（topics.yaml/consumer-groups.yaml/schemas 运行时加载）/ `messages`（编解码+key 规则）/ `buffer`（磁盘缓冲）/ `idempotency`（幂等守卫）/ `metrics`（`hunter_kafka_*` 指标） |
| `database` | SQLAlchemy 2.0 数据访问层包：`DatabaseSessionManager`（asyncpg 异步引擎 + 会话，连接池 CPU×2+1）· `Base`/`StrEnumType`/混入类 · `enums`（车辆状态/事件类型/OTA 状态机等受控词表）· `BaseRepository`（CRUD + 分页 + 软删除 + 批量写入）· `models`（13 张表 ORM）· `migrations`（Alembic） |
| `redis` | redis-py 异步客户端封装（单例、限流计数 `incr_with_expire`、分布式锁） |

## 使用示例

```python
from hunter_common.config import HunterBaseConfig
from hunter_common.logging import configure_logging, get_logger, set_trace_id
from hunter_common.exceptions import ResourceNotFoundError
from hunter_common.responses import success_response

configure_logging("scene-service", "INFO", json_output=False)
logger = get_logger(__name__)
```

### 数据访问层（ORM + Repository）

```python
from sqlalchemy.ext.asyncio import AsyncSession

from hunter_common.database import BaseRepository, DatabaseSessionManager
from hunter_common.database.enums import SceneStatus
from hunter_common.database.models import Scene


class SceneRepository(BaseRepository[Scene]):
    """业务 Repository 只需声明 model（可选默认排序）。"""

    model = Scene
    default_order_by = ("-create_time",)


async def demo(db: AsyncSession) -> None:
    repo = SceneRepository(db)
    # 分页 + 过滤（非法列名抛 2001；page_size 上限 200）
    page = await repo.paginate(page=1, page_size=20, filters={"status": SceneStatus.DRAFT})
    # 主键查询（缺失抛 3001）；软删除对 Scene 生效，deleted_at 非空记录默认被过滤
    scene = await repo.get_or_raise(page.items[0].scene_id)
    await repo.soft_delete(scene)     # 写 deleted_at
    # 高吞吐批量写入（时序/事件），重复键幂等跳过
    await repo.bulk_create_ignore_conflicts(
        [{"scene_name": "s1", "scene_type": "urban", "creator": scene.creator}],
        conflict_columns=["scene_name"],
    )


manager = DatabaseSessionManager(HunterBaseConfig())
manager.init()          # 引擎/会话工厂（幂等）
# FastAPI: db: AsyncSession = Depends(manager.get_session)
```

受控词表（`hunter_common.database.enums`）与 DDL 的 `CHECK` 约束、Kafka 消息 schema 的 `enum`
三者由测试与 `scripts/verify_data_layer.py` 强制一致，业务代码禁止硬编码字面量。

## 契约一致性测试（仓库根目录执行）

```bash
pytest common/python/tests/test_kafka_contracts.py -q     # Kafka Topic / 消费者组 / 11 个消息 Schema
pytest common/python/tests/test_storage_contracts.py -q   # Redis Key / MinIO Bucket ↔ 服务声明 ↔ 初始化脚本
python scripts/verify_data_layer.py                       # 26 项数据层契约校验（含校验 9 Redis / 校验 10 MinIO）
```

## Kafka 契约驱动生产/消费（用法）

```python
from hunter_common.config import HunterBaseConfig
from hunter_common.kafka import KafkaProducerManager, KafkaConsumerManager, SCHEMA_AUTO
from hunter_common.kafka.contracts import get_contract

settings = HunterBaseConfig(service_name="data-collector")

# --- 生产者：契约驱动（Schema 校验 + key=vehicle_id + 按契约 acks + 失败落盘缓冲）---
producer = KafkaProducerManager.initialize(settings)
result = await producer.publish_payload(
    "sensor_file", payload, contract_required=True
)   # result.status ∈ {"delivered", "buffered"}
await producer.replay_buffered()        # 链路恢复后重投磁盘缓冲
await producer.close()                  # 先尽力重投缓冲，再 flush

# --- 消费者：契约校验（非法消息进 DLQ）+ 手动提交 + 重试 + 幂等 + 积压指标 ---
consumer = KafkaConsumerManager(
    settings,
    group_id="data-collector-telemetry",      # 须登记在 consumer-groups.yaml
    topics=["hunter.*.telemetry"],            # 正则订阅：新车接入无需改配置
    schema_name=SCHEMA_AUTO,                  # 按消息实际 Topic 解析契约 Schema
)
await consumer.run(handler)                   # handler(message, value) 抛异常 → 重试耗尽后 DLQ
consumer.stop()                               # 优雅停机
```

契约目录定位顺序：显式 `KAFKA_CONTRACT_DIR` → 工作目录向上查找 → 包位置向上查找
（`get_contract(required=True)` 在契约缺失时 fail fast，禁止静默跳过校验）。
Kafka 侧配置项（重试/退避/缓冲/幂等/契约目录）见 `hunter_common.config.HunterBaseConfig` 的
`kafka_*` 字段与根目录 `.env.example`。

存储契约同为单一事实来源：`contracts/database/redis-keys.yaml`（Redis Key 类型/TTL/读写方）与
`contracts/database/object-storage.yaml`（Bucket 生命周期/SSE-S3/预签名有效期），各服务契约的
`x-hunter-service.redis_keys` / `minio_buckets` 仅是声明，MinIO 初始化脚本
（`infra/docker/minio/init-buckets.sh`、`infra/k8s/jobs/minio-init-job.yaml`）为运行侧落地。
共享库当前**仅提供 Redis 封装**，MinIO 客户端封装待随存储层实现落地（见 `object-storage.yaml`
pending #7，以及 15 项待确认项 / 8 项阻塞）。

## 约束

- 所有异常必须继承 `HunterBaseException`，错误码使用 `ErrorCode` 预定义值，禁止新增/更改含义
- 日志禁止输出密码、Token、私钥等敏感信息（封装已自动脱敏）
- Kafka 车端链路必须 SASL_SSL + SCRAM-SHA-512，消息 key = vehicle_id
