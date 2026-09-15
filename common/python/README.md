# hunter_common —— HunterEdge 共享代码库

供所有微服务以 editable 方式安装：`pip install -e common/python`

| 模块 | 职责 |
|------|------|
| `config.HunterBaseConfig` | pydantic-settings 基础配置基类（DB/Redis/Kafka/MinIO/JWT），各服务继承并补充私有配置 |
| `logging` | structlog JSON 日志：`service`/`trace_id`/`vehicle_id` 自动注入，敏感字段（password/token/secret/key）自动脱敏 |
| `exceptions` | `HunterBaseException` 基类 + 预定义错误码（1001–7002）异常体系 + `ErrorCode` 枚举 |
| `responses` | 统一响应模型 `ApiResponse[T]`（code/message/data/request_id/timestamp）+ `success_response`/`error_response` |
| `kafka` | confluent-kafka 2.x 异步封装：`KafkaProducerManager`（单例，非阻塞投递）/ `KafkaConsumerManager`（手动提交 offset，异常进 DLQ），支持 SASL_SSL + SCRAM-SHA-512 |
| `database` | SQLAlchemy 2.0 异步引擎 + `async_sessionmaker` 会话管理（asyncpg，连接池 CPU×2+1，FastAPI Depends 注入） |
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

## 约束

- 所有异常必须继承 `HunterBaseException`，错误码使用 `ErrorCode` 预定义值，禁止新增/更改含义
- 日志禁止输出密码、Token、私钥等敏感信息（封装已自动脱敏）
- Kafka 车端链路必须 SASL_SSL + SCRAM-SHA-512，消息 key = vehicle_id
