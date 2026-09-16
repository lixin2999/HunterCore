# hunter_common —— HunterEdge 共享代码库

供所有微服务以 editable 方式安装：`pip install -e common/python`

| 模块 | 职责 |
|------|------|
| `config.HunterBaseConfig` | pydantic-settings 基础配置基类（DB/Redis/Kafka/MinIO/JWT），各服务继承并补充私有配置 |
| `logging` | structlog JSON 日志：`service`/`trace_id`/`vehicle_id` 自动注入，敏感字段（password/token/secret/key）自动脱敏 |
| `exceptions` | `HunterBaseException` 基类 + 预定义错误码（1001–7002）异常体系 + `ErrorCode` 枚举 |
| `responses` | 统一响应模型 `ApiResponse[T]`（code/message/data/request_id/timestamp）+ `success_response`/`error_response` |
| `kafka` | confluent-kafka 2.x 异步封装：`KafkaProducerManager`（单例，非阻塞投递）/ `KafkaConsumerManager`（手动提交 offset，异常进 DLQ），支持 SASL_SSL + SCRAM-SHA-512 |
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

## 约束

- 所有异常必须继承 `HunterBaseException`，错误码使用 `ErrorCode` 预定义值，禁止新增/更改含义
- 日志禁止输出密码、Token、私钥等敏感信息（封装已自动脱敏）
- Kafka 车端链路必须 SASL_SSL + SCRAM-SHA-512，消息 key = vehicle_id
