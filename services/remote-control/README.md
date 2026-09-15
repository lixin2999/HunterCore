# remote-control

远程操控：WebRTC 视频流转发、控制指令转发（20Hz）、操作员权限管理、操控会话与录像记录

- 端口：**8085**（环境变量 `API_PORT` 可覆盖）
- 技术栈：Python 3.11+ / FastAPI / pydantic-settings / SQLAlchemy 2.0 (asyncpg)
- 健康探针：`GET /healthz`（存活）、`GET /readyz`（就绪，含 DB/Redis 检查）

## 本地运行

```bash
pip install -e "common/python"     # 仓库根目录：共享库
pip install -e "services/remote-control[dev]"
cd services/remote-control
uvicorn app.main:app --reload --port 8085
```

## 测试

```bash
cd services/remote-control && pytest -q
```

> 骨架层级（L0）：业务逻辑按 `contracts/` 契约逐层实现。
