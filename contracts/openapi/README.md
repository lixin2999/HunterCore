# contracts/openapi — REST API 契约（OpenAPI 3.0）

单一事实来源：所有微服务 REST 接口先在此定义 OpenAPI 3.0 YAML，再进行实现。

计划文件（随各层级开发逐步填充）：

| 文件 | 服务 | 路由前缀 |
|------|------|----------|
| scene-service.yaml | scene-service | `/api/v1/scene` |
| data-collector.yaml | data-collector | `/api/v1/data` |
| data-analytics.yaml | data-analytics | `/api/v1/analytics` |
| ota-service.yaml | ota-service | `/api/v1/ota` |
| remote-control.yaml | remote-control | `/api/v1/remote` |

约定：
- 统一响应格式 `{code, message, data, request_id, timestamp}`，错误码使用预定义值（见 `common/python/hunter_common/exceptions.py`）
- 认证：Bearer JWT（车端另有 X.509 双向 TLS）
