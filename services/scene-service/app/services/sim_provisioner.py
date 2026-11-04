"""仿真端一键开通编排（V1.24.2）：4 步顺序执行 + 逆序回滚 + 下线/轮换/续跑。

契约依据：``contracts/openapi/scene-service.yaml#x-hunter-sim-provisioning``。
实现镜像 vehicle-service ``services/provisioner.py`` 的成熟语义（用户决策：scene 内自持精简版）。

步骤（顺序固定）：
  1. **db**     UPSERT scene_svc.sim_node_registry（初始 provision_status 骨架，各步 pending）
  2. **scram**  SimKafkaAdminOps UPSERT SCRAM-SHA-512 用户（主体 ``sim-{node_id}``，口令服务端生成）
  3. **topics** 创建 ``simulation.{node_id}.result`` + 死信 ``.dlq``（分区/保留期取自契约）
  4. **cert**   openssl 签发双证书：Kafka 客户端（CN=sim-{node_id}）+ 平台访问（CN=sim-platform-{node_id}）

失败处理：
- 任一步异常 → 记 ``state=failed, error=str(exc)[:500]`` → **逆序回滚**已成功的步骤
  （cert → topics → scram；DB 行保留供 retry-provision 续跑）→ 落失败态 → 抛原始异常；
- ``takeover_existing=true``：SCRAM/Topic/证书已存在时接管复用（跳过创建，state="skipped"），
  回滚时也不清理这些外部既有资源；⚠ 接管语义下不重设 SCRAM 口令（避免悄悄失效旧口令）；
- 回滚本身失败仅告警（``rollback_*_failed``），不掩盖原始异常。

状态持久化：``provision_status`` JSONB（结构同车辆侧）：
```
{"state": "pending|in_progress|ready|failed",
 "steps": {"db": {...}, "scram": {...}, "topics": {...}, "cert": {...}}}
```
steps 内允许出现契约外辅助键（``topics``/``cert_issued``），供 retry/deprovision 精确回收
本次真建资源；展平为响应时只取 name/state/error/ts。

⚠ ``scram_password`` 一次性：只在 provision/retry/rotate 响应体出现，不落库、不写日志
（路由日志禁止 echo 响应体）。
"""
from __future__ import annotations

import secrets
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from hunter_common.exceptions import (
    InvalidParameterError,
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
    ServiceUnavailableError,
)
from hunter_common.logging import get_logger

from app.config import Settings
from app.repositories.sim_node_registry import SimNodeRegistryStore
from app.schemas.sim_nodes import SimNodeProvisionStep
from app.services import sim_node_cert_signer as cert_module
from app.services.sim_kafka_admin import SimKafkaAdminOps

logger = get_logger("app.services.sim_provisioner")

StepName = Literal["db", "scram", "topics", "cert"]
StepState = Literal["pending", "in_progress", "ok", "failed", "skipped"]
AggregateState = Literal["pending", "in_progress", "ready", "failed"]

#: 步骤顺序（回滚逆序 = reversed）
STEP_ORDER: tuple[StepName, StepName, StepName, StepName] = ("db", "scram", "topics", "cert")


# =====================================================================
# provision_status JSONB 构造
# =====================================================================
def _now_ts() -> int:
    return int(time.time())


def _new_step(
    name: StepName,
    state: StepState = "pending",
    error: str | None = None,
    ts: int | None = None,
    **extra: Any,
) -> dict[str, Any]:
    step: dict[str, Any] = {
        "name": name,
        "state": state,
        "error": error[:500] if error else None,
        "ts": ts if ts is not None else _now_ts(),
    }
    step.update(extra)
    return step


def initial_provision_status() -> dict[str, Any]:
    """骨架：aggregate=pending，各步 pending（未开通节点详情展示默认态）。"""
    return {
        "state": "pending",
        "steps": {name: _new_step(name, "pending") for name in STEP_ORDER},
    }


def aggregate_state(steps: dict[str, dict[str, Any]]) -> AggregateState:
    """4 步归并汇总态（契约 SimNodeProvisionResult.provision_state）。"""
    states = [step.get("state", "pending") for step in steps.values()]
    if any(s == "failed" for s in states):
        return "failed"
    if all(s in ("ok", "skipped") for s in states):
        return "ready"
    if any(s == "in_progress" for s in states):
        return "in_progress"
    return "pending"


def steps_to_list(provision_status: dict[str, Any] | None) -> list[SimNodeProvisionStep]:
    """JSONB steps → 按 STEP_ORDER 展平的响应列表（辅助键不进入响应）。"""
    if not provision_status:
        return [
            SimNodeProvisionStep(name=name, state="pending")  # type: ignore[arg-type]
            for name in STEP_ORDER
        ]
    steps = provision_status.get("steps") or {}
    out: list[SimNodeProvisionStep] = []
    for name in STEP_ORDER:
        raw = steps.get(name) or {}
        out.append(
            SimNodeProvisionStep(
                name=name,  # type: ignore[arg-type]
                state=raw.get("state", "pending"),  # type: ignore[arg-type]
                error=raw.get("error"),
                ts=raw.get("ts"),
            )
        )
    return out


def aggregate_from_status(provision_status: dict[str, Any] | None) -> AggregateState:
    """登记行 provision_status → 聚合态（无记录 = pending；详情视图用）。"""
    if not provision_status:
        return "pending"
    state = provision_status.get("state")
    if state in ("pending", "in_progress", "ready", "failed"):
        return state  # type: ignore[return-value]
    return aggregate_state(provision_status.get("steps") or {})


# =====================================================================
# SimProvisioner 服务（组合 registry + sim_kafka_admin + cert_signer）
# =====================================================================
@dataclass
class SimProvisionOutcome:
    """provisioning 结果（成功时含一次性口令；失败由异常携带，steps 为最终快照）。"""

    node_id: str
    scram_username: str
    scram_password: str | None
    kafka_cert_sn: str | None = None
    plat_cert_sn: str | None = None
    steps: dict[str, dict[str, Any]] = field(default_factory=dict)
    aggregate: AggregateState = "pending"


class SimProvisioner:
    """编排仿真节点 4 步开通与下线回收（路由层经 SimNodeService 持有，注入到 endpoint）。"""

    def __init__(
        self,
        registry: SimNodeRegistryStore,
        admin: SimKafkaAdminOps | None,
        settings_obj: Settings,
        *,
        cert_module_obj: Any = cert_module,
    ) -> None:
        self._registry = registry
        self._admin = admin
        self._settings = settings_obj
        self._certs = cert_module_obj

    # ---------- 开通 ----------

    def scram_username(self, node_id: str) -> str:
        """Kafka SCRAM 主体（与 Kafka 客户端证书 CN 一致）。"""
        return f"{self._settings.sim_node_scram_prefix}{node_id}"

    async def provision(
        self,
        node_id: str,
        *,
        display_name: str | None = None,
        description: str | None = None,
        labels: dict[str, str] | None = None,
        takeover_existing: bool = False,
    ) -> SimProvisionOutcome:
        """一键开通：4 步顺序 + 失败逆序回滚（口令不回写日志/库）。"""
        if not self._settings.sim_provision_enabled:
            raise InvalidParameterError(
                "仿真端一键开通未启用（SIM_PROVISION_ENABLED=false）",
                details={"node_id": node_id},
            )
        if self._admin is None:
            raise ServiceUnavailableError("Kafka AdminClient 未就绪（sim_kafka_admin=None）")

        steps: dict[str, dict[str, Any]] = {name: _new_step(name, "pending") for name in STEP_ORDER}
        status: dict[str, Any] = {"state": "in_progress", "steps": steps}

        # ---------- 步骤 1: DB（登记行） ----------
        steps["db"] = _new_step("db", "in_progress")
        username = self.scram_username(node_id)
        try:
            exists = await self._registry.exists(node_id)
            if exists and not takeover_existing:
                steps["db"] = _new_step("db", "failed", "node_id 已存在")
                # 该行非本次所建：禁止覆写既有登记行的 provision_status；未创建外部资源，无需回滚
                logger.warning("sim_provision_node_exists", node_id=node_id)
                raise ResourceAlreadyExistsError(
                    "node_id 已存在（接管模式请设 takeover_existing=true）",
                    details={"node_id": node_id},
                )
            await self._registry.upsert(
                node_id,
                provision_status=status,
                scram_username=username,
                **{k: v for k, v in (("display_name", display_name), ("description", description), ("labels", labels)) if v is not None},
            )
            steps["db"] = _new_step("db", "ok")
        except (ResourceAlreadyExistsError, InvalidParameterError):
            raise
        except Exception as exc:
            steps["db"] = _new_step("db", "failed", str(exc))
            await self._persist_status(node_id, steps, failed=True)
            raise ServiceUnavailableError(f"DB 登记行写入失败: {exc}") from exc

        # ---------- 步骤 2: SCRAM ----------
        scram_password: str | None = None
        steps["scram"] = _new_step("scram", "in_progress")
        if not self._settings.sim_scram_enabled:
            steps["scram"] = _new_step("scram", "skipped")
            logger.warning("sim_provision_scram_disabled", node_id=node_id)
        else:
            try:
                scram_password = secrets.token_urlsafe(24)
                await self._admin.upsert_scram_user(username, scram_password)
                steps["scram"] = _new_step("scram", "ok")
            except Exception as exc:
                steps["scram"] = _new_step("scram", "failed", str(exc))
                await self._rollback(node_id, steps, takeover_existing=takeover_existing)
                await self._persist_status(node_id, steps, failed=True)
                if isinstance(exc, ServiceUnavailableError):
                    raise
                raise ServiceUnavailableError(f"SCRAM 用户创建失败: {exc}") from exc

        # ---------- 步骤 3: Topics（节点级回传 + 死信） ----------
        created_topics: list[str] = []
        steps["topics"] = _new_step("topics", "in_progress")
        try:
            created_topics = await self._admin.ensure_node_topics(
                node_id, skip_existing=takeover_existing
            )
            # takeover 下 Topic 全部已存在 → 本次零新建：记 skipped（回滚不清理外部既有资源）
            topic_state: StepState = "skipped" if (takeover_existing and not created_topics) else "ok"
            steps["topics"] = _new_step("topics", topic_state, created=created_topics)
        except Exception as exc:
            steps["topics"] = _new_step("topics", "failed", str(exc), created=created_topics)
            await self._rollback(
                node_id,
                steps,
                takeover_existing=takeover_existing,
                created_topics=created_topics,
            )
            await self._persist_status(node_id, steps, failed=True)
            if isinstance(exc, ServiceUnavailableError):
                raise
            raise ServiceUnavailableError(f"节点级 Topic 创建失败: {exc}") from exc

        # ---------- 步骤 4: Cert（双证书） ----------
        kafka_cert_sn: str | None = None
        plat_cert_sn: str | None = None
        cert_not_after: datetime | None = None
        cert_issued = False
        steps["cert"] = _new_step("cert", "in_progress")
        row = await self._registry.get(node_id)
        if takeover_existing and row is not None and row.kafka_cert_sn:
            # 接管：双证书序列号沿用登记行既有值（不重签、不清目录）
            kafka_cert_sn = row.kafka_cert_sn
            plat_cert_sn = row.plat_cert_sn
            cert_not_after = row.cert_not_after
            steps["cert"] = _new_step("cert", "skipped", cert_issued=False)
        else:
            try:
                kafka_artifacts = await self._certs.aissue_kafka_cert(node_id)
                plat_artifacts = await self._certs.aissue_platform_cert(node_id)
                kafka_cert_sn = kafka_artifacts.serial_hex
                plat_cert_sn = plat_artifacts.serial_hex
                # openssl 签发的 notAfter = 签发时刻 + 有效期（与 -days 同源，不再二次解析证书文件）
                cert_not_after = datetime.now(UTC) + timedelta(
                    days=self._settings.sim_node_cert_validity_days
                )
                cert_issued = True
                steps["cert"] = _new_step("cert", "ok", cert_issued=True)
            except Exception as exc:
                steps["cert"] = _new_step("cert", "failed", str(exc), cert_issued=cert_issued)
                await self._rollback(
                    node_id,
                    steps,
                    takeover_existing=takeover_existing,
                    created_topics=created_topics,
                    issued_cert=cert_issued,
                )
                await self._persist_status(node_id, steps, failed=True)
                if isinstance(exc, ServiceUnavailableError):
                    raise
                raise ServiceUnavailableError(f"证书签发失败: {exc}") from exc

        # ---------- 全步成功 → 落 ready + 开通快照 ----------
        await self._persist_status(
            node_id,
            steps,
            failed=False,
            scram_username=username,
            scram_credential_updated_at=datetime.now(UTC) if scram_password else None,
            cert_not_after=cert_not_after,
            kafka_cert_sn=kafka_cert_sn,
            plat_cert_sn=plat_cert_sn,
        )
        logger.info(
            "sim_provision_succeeded",
            node_id=node_id,
            topics_created=len(created_topics),
            kafka_cert_sn=kafka_cert_sn,
            plat_cert_sn=plat_cert_sn,
        )
        return SimProvisionOutcome(
            node_id=node_id,
            scram_username=username,
            scram_password=scram_password,
            kafka_cert_sn=kafka_cert_sn,
            plat_cert_sn=plat_cert_sn,
            steps=steps,
            aggregate=aggregate_state(steps),
        )

    # ---------- 续跑（先回收本次残留 → 重跑四步） ----------

    async def retry_provision(self, node_id: str) -> SimProvisionOutcome:
        """从失败步续跑（契约 retrySimNodeProvision）：幂等，可多次调用。

        语义等价「先回收本次真建的外部资源（Topic/SCRAM/证书）→ 以既有登记元信息重跑四步」；
        takeover_existing=False —— 回收后按全新开通执行，保证终态各步一致可审计。
        """
        row = await self._registry.get(node_id)
        if row is None:
            raise ResourceNotFoundError(
                f"仿真节点未登记，无法续跑开通：{node_id}",
                details={"node_id": node_id, "hint": "请先 POST /api/v1/scene/sim-nodes"},
            )
        if not self._settings.sim_provision_enabled:
            raise InvalidParameterError(
                "仿真端一键开通未启用（SIM_PROVISION_ENABLED=false）",
                details={"node_id": node_id},
            )
        if self._admin is None:
            raise ServiceUnavailableError("Kafka AdminClient 未就绪（sim_kafka_admin=None）")

        await self._recycle_leftovers(node_id, row.provision_status or {})
        # 清残留后以全新骨架重跑（db 步走 takeover upsert：行已存在）
        return await self.provision(
            node_id,
            display_name=row.display_name,
            description=row.description,
            labels=row.labels or None,
            takeover_existing=True,
        )

    async def _recycle_leftovers(self, node_id: str, status: dict[str, Any]) -> None:
        """回收上次尝试**本次真建**的残留（state=ok 步骤）；不存在均幂等成功，失败仅告警。"""
        steps = status.get("steps") or {}
        # 证书：本次签发过 → 清整个节点证书目录
        if (steps.get("cert") or {}).get("cert_issued"):
            try:
                self._certs.delete_node_cert_dir(node_id)
            except Exception:
                logger.exception("sim_retry_recycle_cert_failed", node_id=node_id)
        # Topic：只删记录在 created 列表里的（本次新建；外部既有不动）
        created = (steps.get("topics") or {}).get("created")
        if isinstance(created, list) and created and self._admin is not None:
            try:
                await self._admin.delete_node_topics([str(t) for t in created])
            except Exception:
                logger.exception("sim_retry_recycle_topics_failed", node_id=node_id, count=len(created))
        # SCRAM：本次创建过（state=ok）→ 删除后由重跑重建
        if (steps.get("scram") or {}).get("state") == "ok" and self._admin is not None:
            try:
                await self._admin.delete_scram_user(self.scram_username(node_id))
            except Exception:
                logger.exception("sim_retry_recycle_scram_failed", node_id=node_id)

    # ---------- 回滚 ----------

    async def _rollback(
        self,
        node_id: str,
        steps: dict[str, dict[str, Any]],
        *,
        takeover_existing: bool,
        created_topics: list[str] | None = None,
        issued_cert: bool = False,
    ) -> None:
        """逆序回收已成功步骤（cert → topics → scram）；失败仅告警不掩盖原始异常。

        - 若某步 state != "ok" 视为未创建，跳过；
        - takeover_existing=true 时，"skipped" 步骤对应的外部资源不动（仅 "ok" = 本次真的新建）；
        - DB 登记行保留（供审计与 retry-provision 续跑），不做 DELETE。
        """
        if issued_cert or steps.get("cert", {}).get("state") == "ok":
            try:
                self._certs.delete_node_cert_dir(node_id)
            except Exception:
                logger.exception("sim_rollback_cert_failed", node_id=node_id)

        topics = created_topics if created_topics is not None else (
            steps.get("topics", {}).get("created") or []
        )
        if topics and self._admin is not None:
            try:
                await self._admin.delete_node_topics(list(topics))
            except Exception:
                logger.exception("sim_rollback_topics_failed", node_id=node_id, count=len(topics))

        if steps.get("scram", {}).get("state") == "ok" and not takeover_existing and self._admin is not None:
            try:
                await self._admin.delete_scram_user(self.scram_username(node_id))
            except Exception:
                logger.exception("sim_rollback_scram_failed", node_id=node_id)

    # ---------- 状态持久化 ----------

    async def _persist_status(
        self,
        node_id: str,
        steps: dict[str, dict[str, Any]],
        *,
        failed: bool,
        **fields: Any,
    ) -> None:
        aggregate: AggregateState = "failed" if failed else aggregate_state(steps)
        payload = {"state": aggregate, "steps": steps}
        try:
            await self._registry.update_provision_status(node_id, payload, **fields)
        except Exception:
            logger.exception("sim_provision_status_persist_failed", node_id=node_id)

    # ---------- 下线：反序回收（证书目录→SCRAM→Topic→登记行→Redis） ----------

    async def deprovision(
        self,
        node_id: str,
        *,
        redis_deleter: Any | None = None,
        purge_topics: bool = True,
    ) -> dict[str, Any]:
        """下线回收。返回摘要（对齐契约 SimNodeDeprovisionResult）。

        ``redis_deleter`` 为 ``SimNodeStore.delete_node`` 协程函数（服务层注入；None 时跳过
        Redis 清理并由调用方自行处理）。外部资源不存在均视为幂等成功。
        """
        summary: dict[str, Any] = {
            "node_id": node_id,
            "cert_dir_removed": False,
            "scram_removed": False,
            "topics_removed": [],
            "registry_removed": False,
            "redis_removed": False,
        }
        row = await self._registry.get(node_id)
        # 1) 证书目录（不存在 → True：与契约 cert_dir_removed 幂等语义一致）
        self._certs.delete_node_cert_dir(node_id)
        summary["cert_dir_removed"] = True
        # 2) SCRAM 用户（行可能已被删过：回退按前缀推导主体名）
        if self._admin is not None and self._settings.sim_scram_enabled:
            username = (row.scram_username if row is not None else None) or self.scram_username(node_id)
            await self._admin.delete_scram_user(username)
            summary["scram_removed"] = True
        # 3) 节点级 Topic（含死信；按契约渲染全集，未建过的幂等跳过）
        if purge_topics and self._admin is not None:
            names = [t["name"] for t in self._admin.render_node_topics(node_id)]
            summary["topics_removed"] = await self._admin.delete_node_topics(names)
        # 4) 登记行
        summary["registry_removed"] = await self._registry.delete(node_id)
        # 5) Redis 登记（服务层注入；离线节点的元数据键一并清）
        if redis_deleter is not None:
            summary["redis_removed"] = bool(await redis_deleter(node_id))
        logger.info("sim_deprovision_succeeded", node_id=node_id, **{k: v for k, v in summary.items() if k != "node_id"})
        return summary

    # ---------- 轮换 SCRAM 口令 ----------

    async def rotate_scram(self, node_id: str) -> tuple[str, datetime]:
        """重置 SCRAM 口令：UPSERT 覆盖旧凭据；返回 (一次性新口令, 更新时间)。"""
        if not self._settings.sim_scram_enabled:
            raise InvalidParameterError(
                "Kafka SCRAM 未启用（SIM_SCRAM_ENABLED=false），无法轮换口令",
                details={"node_id": node_id},
            )
        if self._admin is None:
            raise ServiceUnavailableError("Kafka AdminClient 未就绪（sim_kafka_admin=None）")
        row = await self._registry.get(node_id)
        if row is None:
            raise ResourceNotFoundError(
                f"仿真节点未登记：{node_id}",
                details={"node_id": node_id, "hint": "先完成一键开通（POST /api/v1/scene/sim-nodes）"},
            )
        username = row.scram_username or self.scram_username(node_id)
        new_password = secrets.token_urlsafe(24)
        await self._admin.upsert_scram_user(username, new_password)
        updated_at = datetime.now(UTC)
        # 只回写时间戳与主体名，provision_status 保持原快照
        await self._registry.update_provision_status(
            node_id,
            row.provision_status or initial_provision_status(),
            scram_username=username,
            scram_credential_updated_at=updated_at,
        )
        logger.info("sim_scram_rotated", node_id=node_id, scram_username=username)
        return new_password, updated_at


__all__ = [
    "STEP_ORDER",
    "SimProvisionOutcome",
    "SimProvisioner",
    "aggregate_from_status",
    "aggregate_state",
    "initial_provision_status",
    "steps_to_list",
]
