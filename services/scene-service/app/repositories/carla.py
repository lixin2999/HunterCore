"""Carla 管理 API 客户端（4.4 节下发流程第 2-5 步：创建仿真实例 / 下发场景配置）。

⚠ 契约待确认项 #2/#9：Carla 管理 API 地址（``CARLA_MANAGEMENT_ENDPOINT``）与各步骤子路径
设计文档未定义 —— 本实现全部经环境变量注入（禁止硬编码），默认子路径见 app/config.py，
人工确认后仅需调整配置，无需改代码。
错误语义（全部 5001 服务不可用，但**文案按成因分流**，见 V1.23.7）：
- 未配置连接地址 → 「Carla 管理 API 未配置」；
- 地址形态非法（缺 scheme / 非 http(s) / 无主机名 / 携带子路径）→ 「Carla 管理 API 地址非法」，
  **在发出任何请求前**判定（契约 4.4 节第 2 步；动机见 validate_management_endpoint）；
- 子路径模板缺 `{sim_instance_id}` 占位符 → 「Carla 管理 API 子路径配置非法」（不得伪装成 5000）；
- 连接失败 / 超时 → 「Carla 管理 API 不可达」/「Carla 管理 API 连接超时」；
- 非 2xx / 响应结构非法 → 「Carla 管理 API 返回 <code>」/「Carla 管理 API 响应非法」；
进度/结果查询（G-20①）中 Carla 返回 404 → 3001 资源不存在（实例不存在或已回收）。
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

import httpx
from hunter_common.exceptions import ResourceNotFoundError, ServiceUnavailableError
from hunter_common.logging import get_logger

logger = get_logger("app.repositories.carla")

#: 重试间隔基数（秒；内部节流参数，非业务阈值）
_RETRY_BACKOFF_SECONDS = 0.2
#: 视为「进行中」的实例状态（契约 SimulationStatus 中未终结的取值）
ACTIVE_STATUSES = frozenset({"pending", "running"})
#: 响应体中实例 ID 的可接受键名（⚠ Carla API 未定义，待确认 #9）
_INSTANCE_ID_KEYS = ("sim_instance_id", "instance_id", "id")
#: 管理 API 基址允许的 scheme（本客户端不携带任何认证/证书，故不接受其它协议猜测）
_ALLOWED_SCHEMES = frozenset({"http", "https"})


def validate_management_endpoint(base_url: str) -> str | None:
    """校验管理 API 基址形态；返回 ``None`` 表示合法，否则返回缺陷描述。

    动机（V1.23.7 实机）：``.env`` 中 ``KEY=   # 例：http://…`` 这一形状的行尾注释会被
    docker compose 当成**值**下发 —— 值非空 ⇒ 绕过「未配置」判定，httpx 随即抛
    ``UnsupportedProtocol``，被兜底文案伪装成「Carla 管理 API 不可达」，现场按网络故障
    多绕一轮。故在构造期判定形态，把「配置本身非法」与「网络不通」彻底分开。

    空值不算形态缺陷（返回 ``None``）：那是「未配置」分支的语义，两者文案必须区分。
    """
    if not base_url.strip():
        return None
    try:
        parts = urlsplit(base_url)
    except ValueError:
        return "URL 无法解析"
    if not parts.scheme:
        return "缺少 scheme（正确形态：http://<host>[:<port>]，子路径另由 CARLA_INSTANCE_*_PATH 配置）"
    if parts.scheme.lower() not in _ALLOWED_SCHEMES:
        return f"scheme 非法：{parts.scheme}（仅支持 http/https）"
    if not parts.hostname:
        return "缺少主机名"
    if parts.path.rstrip("/"):
        return f"基址不得携带子路径：{parts.path}（子路径由 CARLA_INSTANCE_*_PATH 配置）"
    return None


def _transport_failure_message(exc: BaseException | None) -> str:
    """按传输异常类型分流文案：超时 / 协议非法 / 不可达三类不再共用一条。"""
    if isinstance(exc, httpx.TimeoutException):
        return "Carla 管理 API 连接超时"
    if isinstance(exc, httpx.UnsupportedProtocol):
        return "Carla 管理 API 地址协议非法（需 http/https）"
    return "Carla 管理 API 不可达"


@dataclass(frozen=True, slots=True)
class SimInstance:
    """仿真实例快照（4.4 节第 5 步：sim_instance_id + 运行状态）。"""

    instance_id: str
    status: str
    sim_id: str | None = None
    data: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SimInstanceSnapshot:
    """仿真实例完整快照（G-20① 进度/结果查询：额外携带原始响应体供宽松透传）。"""

    instance_id: str
    status: str
    data: dict[str, Any] = field(default_factory=dict)


class CarlaSimIdMismatchError(ServiceUnavailableError):
    """CARLA 已创建实例，但回显的 sim_id 与平台下发不一致。

    携带 ``instance_id`` 供调用方先回收实例再上抛，避免「实例已建、平台不知道」的孤儿实例
    （V1.23.4 修复）。
    """

    def __init__(
        self,
        *,
        instance_id: str,
        scene_id: str,
        platform_sim_id: str,
        carla_sim_id: str,
    ) -> None:
        super().__init__(
            "Carla 返回的 sim_id 与平台下发不一致",
            details={
                "scene_id": scene_id,
                "platform_sim_id": platform_sim_id,
                "carla_sim_id": carla_sim_id,
                "sim_instance_id": instance_id,
            },
        )
        self.instance_id = instance_id


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
        get_path: str = "",
        result_path: str = "",
        pause_path: str = "",
        resume_path: str = "",
        stop_path: str = "",
    ) -> None:
        self._base_url = base_url.rstrip("/")
        # 地址形态门禁（构造期判定，不阻塞启动：场景库 CRUD 不应被 CARLA 配置缺陷带下）
        self._endpoint_problem = validate_management_endpoint(self._base_url)
        if self._endpoint_problem:
            # 不回显原始值（可能含 userinfo/口令），只回显成因
            logger.error(
                "carla_endpoint_invalid",
                env="CARLA_MANAGEMENT_ENDPOINT",
                problem=self._endpoint_problem,
            )
        self._create_path = create_path
        self._query_path = query_path
        self._scenario_path = scenario_path
        self._get_path = get_path
        self._result_path = result_path
        self._pause_path = pause_path
        self._resume_path = resume_path
        self._stop_path = stop_path
        self._timeout_seconds = timeout_seconds
        self._max_retries = max(0, max_retries)
        self._client: httpx.AsyncClient | None = None
        self._lock = asyncio.Lock()

    @property
    def configured(self) -> bool:
        """是否已配置 Carla 管理 API 地址（未配置时任何调用返回 5001）。

        注意：“非空”不等于“可用”；地址形态非法时 ``endpoint_problem`` 携带缺陷描述。
        """
        return bool(self._base_url)

    @property
    def endpoint_problem(self) -> str | None:
        """地址形态缺陷描述（``None`` = 合法或未配置）。"""
        return self._endpoint_problem

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
        self,
        *,
        scene_id: str,
        scene_name: str,
        scene_config: dict[str, Any],
        sim_id: str | None = None,
        mode: str | None = None,
        vehicle_id: str | None = None,
        vil: dict[str, Any] | None = None,
        replay_config: dict[str, Any] | None = None,
        kafka: dict[str, Any] | None = None,
        quality: str | None = None,
    ) -> SimInstance:
        """② 调用 Carla 管理 API 创建仿真实例（返回 sim_instance_id 与初始状态）。

        创建请求携带平台生成的 ``sim_id`` 与动态 Topic 名，保证 CARLA 回传 Topic 与会话一致；
        ``kafka`` 段只传 Topic 名，凭据由 CARLA 运行环境注入（禁止经平台传递）。
        """
        body: dict[str, Any] = {
            "scene_id": scene_id,
            "scene_name": scene_name,
            "scene_config": scene_config,
        }
        optional = {
            "sim_id": sim_id,
            "mode": mode,
            "vehicle_id": vehicle_id,
            "vil": vil,
            "replay_config": replay_config,
            "kafka": kafka,
            "quality": quality,
        }
        body.update({key: value for key, value in optional.items() if value is not None})
        # 创建实例非幂等：超时重试可能产生第二个实例，禁止自动重试（V1.23.4）。
        response = await self._request("POST", self._create_path, json_body=body, retry=False)
        payload = self._json_body(response, self._create_path)
        instance_id = self._extract_instance_id(payload)
        if isinstance(payload, dict):
            status = str(payload.get("status") or "created")
            returned_sim_id = payload.get("sim_id") or payload.get("simulation_id")
            if returned_sim_id and sim_id and str(returned_sim_id) != sim_id:
                # sim_id 是动态 Topic、Kafka key 与 VIL 会话的共同主键；CARLA 回显不一致
                # 会导致消息写到另一套 Topic（平台永远收不到回传），必须 fail fast。
                # 但实例已创建，调用方需要 instance_id 先 stop 再上抛，避免孤儿实例。
                logger.error(
                    "carla_sim_id_mismatch",
                    scene_id=scene_id,
                    platform_sim_id=sim_id,
                    carla_sim_id=str(returned_sim_id),
                    sim_instance_id=instance_id,
                )
                raise CarlaSimIdMismatchError(
                    instance_id=instance_id,
                    scene_id=scene_id,
                    platform_sim_id=sim_id,
                    carla_sim_id=str(returned_sim_id),
                )
            data = dict(payload)
        else:
            status = "created"
            returned_sim_id = None
            data = {}
        logger.info(
            "carla_instance_created",
            scene_id=scene_id,
            sim_instance_id=instance_id,
            sim_id=sim_id,
            mode=mode,
        )
        return SimInstance(
            instance_id=instance_id,
            status=status,
            sim_id=str(returned_sim_id) if returned_sim_id else sim_id,
            data=data,
        )

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
            if status.strip().lower() not in ACTIVE_STATUSES:
                continue
            for key in _INSTANCE_ID_KEYS:
                value = entry.get(key)
                if value:
                    sim_id = entry.get("sim_id") or entry.get("simulation_id")
                    return SimInstance(
                        instance_id=str(value),
                        status=status,
                        sim_id=str(sim_id) if sim_id else None,
                        data=dict(entry),
                    )
        return None

    async def submit_scenario(self, instance_id: str, payload: dict[str, Any]) -> None:
        """③④ 下发场景配置 JSON（Carla 侧加载地图、生成参与者、设置环境）。"""
        path = self._render_path(
            "CARLA_SCENARIO_SUBMIT_PATH", self._scenario_path, instance_id=instance_id
        )
        # 场景下发非幂等（可能重复加载 world），禁止自动重试（V1.23.4）。
        await self._request("POST", path, json_body=payload, retry=False)
        logger.info("carla_scenario_submitted", sim_instance_id=instance_id)

    async def get_instance(self, instance_id: str) -> SimInstanceSnapshot:
        """⑥ 查询仿真实例进度快照（G-20①；Carla 404 → 3001，其余失败 → 5001）。"""
        return await self._fetch_snapshot(
            "CARLA_INSTANCE_GET_PATH", self._get_path, instance_id
        )

    async def get_result(self, instance_id: str) -> SimInstanceSnapshot:
        """⑦ 查询仿真实例结果（G-20①；终态判定由服务层完成，此处仅取快照）。"""
        return await self._fetch_snapshot(
            "CARLA_INSTANCE_RESULT_PATH", self._result_path, instance_id
        )

    async def pause_instance(self, instance_id: str) -> None:
        """暂停仿真实例（POST /instances/{id}/pause）。"""
        await self._control_instance(
            "CARLA_INSTANCE_PAUSE_PATH", self._pause_path, instance_id, "pause"
        )

    async def resume_instance(self, instance_id: str) -> None:
        """恢复仿真实例（POST /instances/{id}/resume）。"""
        await self._control_instance(
            "CARLA_INSTANCE_RESUME_PATH", self._resume_path, instance_id, "resume"
        )

    async def stop_instance(self, instance_id: str) -> None:
        """停止并回收仿真实例（POST /instances/{id}/stop）。"""
        await self._control_instance(
            "CARLA_INSTANCE_STOP_PATH", self._stop_path, instance_id, "stop"
        )

    async def _control_instance(
        self, env_name: str, path_template: str, instance_id: str, action: str
    ) -> None:
        if not path_template:
            raise ServiceUnavailableError(
                "Carla 管理 API 控制路径未配置", details={"action": action, "env": env_name}
            )
        path = self._render_path(env_name, path_template, instance_id=instance_id)
        # pause/resume/stop 为幂等控制语义，可安全重试。
        await self._request("POST", path, json_body={}, retry=True)
        logger.info("carla_instance_control", sim_instance_id=instance_id, action=action)

    async def _fetch_snapshot(
        self, env_name: str, path_template: str, instance_id: str
    ) -> SimInstanceSnapshot:
        """GET 实例子资源并解析快照（响应体非对象 → 5001；404 → 3001）。"""
        path = self._render_path(env_name, path_template, instance_id=instance_id)
        response = await self._request("GET", path, allow_not_found=True)
        if response.status_code == 404:
            raise ResourceNotFoundError(
                f"仿真实例不存在或已回收：{instance_id}",
                details={"sim_instance_id": instance_id},
            )
        body = self._json_body(response, path)
        if not isinstance(body, dict):
            logger.error("carla_instance_snapshot_not_object", path=path)
            raise ServiceUnavailableError(
                "Carla 管理 API 响应非法", details={"path": path}
            )
        status = str(body.get("status") or "created")
        return SimInstanceSnapshot(instance_id=instance_id, status=status, data=body)

    # ---------- 内部：请求与解析 ----------

    @staticmethod
    def _render_path(env_name: str, template: str, *, instance_id: str | None = None) -> str:
        """渲染子路径（V1.23.7）：占位符缺失/非法一律 5001「子路径配置非法」。

        不得让 ``str.format`` 的 ``KeyError``/``IndexError`` 泄到请求层变成 5000，
        否则现场会把“配置写错”当成“服务异常”查。
        """
        if instance_id is None:
            return template
        try:
            return template.format(sim_instance_id=instance_id)
        except (IndexError, KeyError) as exc:
            logger.error("carla_path_template_invalid", env=env_name, template=template)
            raise ServiceUnavailableError(
                "Carla 管理 API 子路径配置非法",
                details={"env": env_name, "template": template, "error": type(exc).__name__},
            ) from exc

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        allow_not_found: bool = False,
        retry: bool | None = None,
    ) -> httpx.Response:
        """发送请求（超时 + 有限重试）；失败统一映射 5001；``allow_not_found`` 时 404 原样返回。

        ``retry``：是否允许自动重试。默认仅 GET/HEAD 重试；创建实例、下发场景等非幂等
        POST 必须显式 ``retry=False``，避免超时重试造成重复实例（V1.23.4）。
        """
        if not self._base_url:
            raise ServiceUnavailableError(
                "Carla 管理 API 未配置", details={"env": "CARLA_MANAGEMENT_ENDPOINT"}
            )
        if self._endpoint_problem:
            raise ServiceUnavailableError(
                "Carla 管理 API 地址非法",
                details={
                    "env": "CARLA_MANAGEMENT_ENDPOINT",
                    "problem": self._endpoint_problem,
                    "path": path,
                },
            )
        url = f"{self._base_url}{path}"
        client = await self._get_client()
        if retry is None:
            retry = method.upper() in ("GET", "HEAD")
        attempts = self._max_retries + 1 if retry else 1
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
                if allow_not_found and response.status_code == 404:
                    return response
                logger.error("carla_error_response", path=path, status_code=response.status_code)
                raise ServiceUnavailableError(
                    f"Carla 管理 API 返回 {response.status_code}", details={"path": path}
                )
            return response
        raise ServiceUnavailableError(
            _transport_failure_message(last_exc),
            details={
                "path": path,
                "attempts": attempts,
                "error": type(last_exc).__name__ if last_exc else None,
            },
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


__all__ = [
    "ACTIVE_STATUSES",
    "CarlaManagementClient",
    "CarlaSimIdMismatchError",
    "SimInstance",
    "SimInstanceSnapshot",
    "validate_management_endpoint",
]