"""Carla 管理 API 客户端（4.4 节下发流程第 2-5 步：创建仿真实例 / 下发场景配置）。

⚠ 契约待确认项 #2/#9：Carla 管理 API 地址（``CARLA_MANAGEMENT_ENDPOINT``）与各步骤子路径
设计文档未定义 —— 本实现全部经环境变量注入（禁止硬编码），默认子路径见 app/config.py，
人工确认后仅需调整配置，无需改代码。
错误语义：未配置连接地址 / 连接失败 / 超时 / 非 2xx / 响应结构非法 → 5001 服务不可用
（契约 4.4 节第 2 步「Carla 管理 API 不可达/超时返回 5001」）。
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import httpx
from hunter_common.exceptions import ServiceUnavailableError
from hunter_common.logging import get_logger

logger = get_logger("app.repositories.carla")

#: 重试间隔基数（秒；内部节流参数，非业务阈值）
_RETRY_BACKOFF_SECONDS = 0.2
#: 视为「进行中」的实例状态（契约 SimulationStatus 中未终结的取值）
ACTIVE_STATUSES = frozenset({"pending", "running"})
#: 响应体中实例 ID 的可接受键名（⚠ Carla API 未定义，待确认 #9）
_INSTANCE_ID_KEYS = ("sim_instance_id", "instance_id", "id")


@dataclass(frozen=True, slots=True)
class SimInstance:
    """仿真实例快照（4.4 节第 5 步：sim_instance_id + 运行状态）。"""

    instance_id: str
    status: str


class CarlaManagementClient:
    """Carla 管理 API 异步客户端（httpx，连接惰性创建 + 超时/重试 + 统一 5001）。"""

    def __init__(
        self,
        base_url: str,
        *,
        create_path: str,
        query_path: str,
        scenario_path: str,
        timeout_seconds: float,
        max_retries: int,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._create_path = create_path
        self._query_path = query_path
        self._scenario_path = scenario_path
        self._timeout_seconds = timeout_seconds
        self._max_retries = max(0, max_retries)
        self._client: httpx.AsyncClient | None = None
        self._lock = asyncio.Lock()

    @property
    def configured(self) -> bool:
        """是否已配置 Carla 管理 API 地址（未配置时任何调用返回 5001）。"""
        return bool(self._base_url)

    async def _get_client(self) -> httpx.AsyncClient:
        """惰性创建 httpx 客户端（并发安全）。"""
        if self._client is None:
            async with self._lock:
                if self._client is None:
                    self._client = httpx.AsyncClient(
                        timeout=httpx.Timeout(self._timeout_seconds)
                    )
        return self._client

    async def close(self) -> None:
        """释放连接池（应用关闭时调用）。"""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # ---------- 4.4 节流程步骤 ----------

    async def create_instance(
        self, *, scene_id: str, scene_name: str, scene_config: dict[str, Any]
    ) -> SimInstance:
        """② 调用 Carla 管理 API 创建仿真实例（返回 sim_instance_id 与初始状态）。"""
        response = await self._request(
            "POST",
            self._create_path,
            json_body={
                "scene_id": scene_id,
                "scene_name": scene_name,
                "scene_config": scene_config,
            },
        )
        body = self._json_body(response, self._create_path)
        instance_id = self._extract_instance_id(body)
        status = str(body.get("status") or "running") if isinstance(body, dict) else "running"
        logger.info("carla_instance_created", scene_id=scene_id, sim_instance_id=instance_id)
        return SimInstance(instance_id=instance_id, status=status)

    async def find_active_instance(self, scene_id: str) -> SimInstance | None:
        """查询该场景进行中的仿真实例（未找到/响应结构未知返回 None，仅记录告警）。"""
        response = await self._request("GET", self._query_path, params={"scene_id": scene_id})
        body = self._json_body(response, self._query_path)
        entries = body if isinstance(body, list) else body.get("items")
        if not isinstance(entries, list):
            logger.warning("carla_instance_query_shape_unknown", scene_id=scene_id)
            return None
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            status = str(entry.get("status") or "")
            if status not in ACTIVE_STATUSES:
                continue
            for key in _INSTANCE_ID_KEYS:
                value = entry.get(key)
                if value:
                    return SimInstance(instance_id=str(value), status=status)
        return None

    async def submit_scenario(self, instance_id: str, payload: dict[str, Any]) -> None:
        """③④ 下发场景配置 JSON（Carla 侧加载地图、生成参与者、设置环境）。"""
        path = self._scenario_path.format(sim_instance_id=instance_id)
        await self._request("POST", path, json_body=payload)
        logger.info("carla_scenario_submitted", sim_instance_id=instance_id)

    # ---------- 内部：请求与解析 ----------

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> httpx.Response:
        """发送请求（超时 + 有限重试）；任何失败统一映射 5001（依赖服务不可用）。"""
        if not self._base_url:
            raise ServiceUnavailableError(
                "Carla 管理 API 未配置", details={"env": "CARLA_MANAGEMENT_ENDPOINT"}
            )
        url = f"{self._base_url}{path}"
        client = await self._get_client()
        attempts = self._max_retries + 1
        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                response = await client.request(method, url, params=params, json=json_body)
            except httpx.HTTPError as exc:
                last_exc = exc
                logger.warning(
                    "carla_request_failed", path=path, attempt=attempt, error=type(exc).__name__
                )
                if attempt < attempts:
                    await asyncio.sleep(_RETRY_BACKOFF_SECONDS * attempt)
                continue
            if response.status_code >= 400:
                logger.error("carla_error_response", path=path, status_code=response.status_code)
                raise ServiceUnavailableError(
                    f"Carla 管理 API 返回 {response.status_code}", details={"path": path}
                )
            return response
        raise ServiceUnavailableError(
            "Carla 管理 API 不可达", details={"path": path, "attempts": attempts}
        ) from last_exc

    @staticmethod
    def _json_body(response: httpx.Response, path: str) -> Any:
        """解析 JSON 响应体（非法 JSON → 5001）。"""
        try:
            return response.json()
        except ValueError as exc:
            logger.error("carla_response_not_json", path=path)
            raise ServiceUnavailableError(
                "Carla 管理 API 响应非法", details={"path": path}
            ) from exc

    @staticmethod
    def _extract_instance_id(body: Any) -> str:
        """从响应体提取实例 ID（可接受键名见 _INSTANCE_ID_KEYS）。"""
        if isinstance(body, dict):
            for key in _INSTANCE_ID_KEYS:
                value = body.get(key)
                if value:
                    return str(value)
        raise ServiceUnavailableError(
            "Carla 管理 API 未返回仿真实例 ID", details={"keys": list(_INSTANCE_ID_KEYS)}
        )


__all__ = ["ACTIVE_STATUSES", "CarlaManagementClient", "SimInstance"]