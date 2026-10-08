"""一键开通编排（provisioner）：4 步顺序执行 + 逆序回滚。

契约依据：contracts/openapi/vehicle-service.yaml `x-hunter-provisioning`。

步骤（顺序固定）：
  1. **db**     INSERT vehicle_svc.vehicles（初始 provision_status 骨架，各步 pending）
  2. **scram**  AdminClient UPSERT SCRAM-SHA-512 用户（username=vehicle_id，口令服务端生成）
  3. **topics** AdminClient 创建 8 个 `hunter.{vehicle_id}.*` Topic
  4. **cert**   openssl 签发每车独立客户端证书（key/cert/p12），落 VEHICLE_CERTS_DIR/{id}/

失败处理：
- 任一步异常 → 记 `state=failed, error=str(exc)[:500]` → **逆序回滚**已成功的步骤 →
  最终落 provision_status = 失败态；抛出原始异常（映射由路由层处理）；
- `takeover_existing=true`：SCRAM/Topic/证书已存在时接管复用（跳过创建，state="skipped"），
  回滚时也不清理这些外部既有资源；
- 回滚本身失败仅告警（`rollback_step_failed`），不掩盖原始异常。

状态持久化：`provision_status` JSONB 结构（与 Vehicle ORM 注释一致）：
```
{
  "state": "pending|in_progress|ready|failed",
  "steps": {
    "db":     {"state": "ok",         "ts": 1724035200},
    "scram":  {"state": "ok",         "ts": 1724035201},
    "topics": {"state": "ok",         "ts": 1724035202},
    "cert":   {"state": "failed", "error": "...", "ts": 1724035203}
  }
}
```

事务边界：步骤 1 的 DB 插入独立事务（失败 → 直接返回，无资源需回收）；
步骤 2/3/4 完成后各自单条 UPDATE provision_status（不使用跨资源分布式事务）；
若中途失败，步骤 1 的 DB 行保留（供审计与再次重跑），仅"下线"操作会删除它。

⚠ 该语义与"步骤 1 失败自动回滚"稍有差异：步骤 1 是 INSERT 且未提交外部资源，
异常天然 rollback（`db.session()` 上下文）；步骤 2 之后失败，DB 行仍在 provision_status
中标记 failed —— 便于运营"从失败步续跑"（MVP 未提供续跑 UI，等价于先下线再重开）。
"""
from __future__ import annotations

import secrets
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal

from hunter_common.exceptions import (
    HunterBaseException,
    ResourceAlreadyExistsError,
    ServiceUnavailableError,
)
from hunter_common.logging import get_logger
from sqlalchemy.exc import IntegrityError

from app.config import settings
from app.repositories.vehicle import VehicleStore
from app.schemas.vehicle import (
    ProvisionStep,
    VehicleCreateRequest,
)
from app.services import cert_signer, kafka_admin

logger = get_logger("app.services.provisioner")

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


def _new_step(name: StepName, state: StepState = "pending", error: str | None = None, ts: int | None = None) -> dict[str, Any]:
    return {"name": name, "state": state, "error": error[:500] if error else None, "ts": ts if ts is not None else _now_ts()}


def initial_provision_status() -> dict[str, Any]:
    """新建车辆时的骨架：aggregate=pending，各步 pending。"""
    return {
        "state": "pending",
        "steps": {name: _new_step(name, "pending") for name in STEP_ORDER},
    }


def aggregate_state(steps: dict[str, dict[str, Any]]) -> AggregateState:
    """4 步归并汇总态（契约 VehicleRow.provision_state）。"""
    states = [step.get("state", "pending") for step in steps.values()]
    if any(s == "failed" for s in states):
        return "failed"
    if all(s in ("ok", "skipped") for s in states):
        return "ready"
    if any(s == "in_progress" for s in states):
        return "in_progress"
    return "pending"


# =====================================================================
# Provisioner 服务（组合 repo + kafka_admin + cert_signer）
# =====================================================================
@dataclass
class ProvisionOutcome:
    """provisioning 结果（成功时含一次性口令；失败时含最终 steps 快照）。"""

    vehicle_id: str
    scram_password: str | None
    device_cert_sn: str | None
    steps: dict[str, dict[str, Any]] = field(default_factory=dict)
    aggregate: AggregateState = "pending"


class Provisioner:
    """编排 4 步 provision 与下线回收（路由层持有单例，注入到 endpoint）。"""

    def __init__(
        self,
        store: VehicleStore,
        admin: kafka_admin.KafkaAdminOps | None,
        settings_obj: type(settings) = settings,  # noqa: B008 - 允许测试注入替代
    ) -> None:
        self._store = store
        self._admin = admin
        self._settings = settings_obj

    # ---------- 开通 ----------
    async def provision(self, req: VehicleCreateRequest) -> ProvisionOutcome:
        """一键开通：4 步顺序 + 失败逆序回滚。"""
        if self._admin is None:
            raise ServiceUnavailableError("Kafka AdminClient 未就绪（kafka_admin=None）")

        steps: dict[str, dict[str, Any]] = {name: _new_step(name, "pending") for name in STEP_ORDER}
        status: dict[str, Any] = {"state": "in_progress", "steps": steps}

        # ---------- 步骤 1: DB ----------
        steps["db"] = _new_step("db", "in_progress")
        try:
            async with self._store.session() as session:
                await self._store.insert_ledger(
                    session,
                    vehicle_id=req.vehicle_id,
                    vehicle_name=req.vehicle_name,
                    model=req.model,
                    firmware_version=req.firmware_version,
                    software_version=req.software_version,
                    fence_json=req.fence_json,
                    description=req.description,
                    provision_status=status,
                )
            steps["db"] = _new_step("db", "ok")
        except IntegrityError as exc:
            # vehicle_id 主键/唯一冲突 → 契约 409/3002（“vehicle_id 已存在”）。
            # 该行非本次所建：禁止 _persist_status 覆写既有台账的 provision_status；未创建外部资源，无需回滚。
            # （takeover_existing 仅作用于 SCRAM/Topic/证书等外部资源，不接管已存在的台账行）
            steps["db"] = _new_step("db", "failed", "vehicle_id 已存在")
            logger.warning("provision_vehicle_exists", vehicle_id=req.vehicle_id)
            raise ResourceAlreadyExistsError(
                "vehicle_id 已存在", details={"vehicle_id": req.vehicle_id}
            ) from exc
        except HunterBaseException as exc:
            # 其它业务异常（含显式 3002 等）：直接映射，无需回滚（未创建外部资源）
            steps["db"] = _new_step("db", "failed", str(exc))
            await self._persist_status(req.vehicle_id, steps, failed=True)
            raise
        except Exception as exc:  # noqa: BLE001 - DB 层未知异常
            steps["db"] = _new_step("db", "failed", str(exc))
            await self._persist_status(req.vehicle_id, steps, failed=True)
            raise ServiceUnavailableError(f"DB 台账写入失败: {exc}") from exc

        # ---------- 步骤 2: SCRAM ----------
        scram_password: str | None = None
        steps["scram"] = _new_step("scram", "in_progress")
        try:
            scram_password = secrets.token_urlsafe(24)
            await self._admin.upsert_scram_user(req.vehicle_id, scram_password)
            steps["scram"] = _new_step("scram", "ok")
        except Exception as exc:  # noqa: BLE001
            steps["scram"] = _new_step("scram", "failed", str(exc))
            await self._rollback(req.vehicle_id, steps, takeover_existing=req.takeover_existing)
            await self._persist_status(req.vehicle_id, steps, failed=True)
            if isinstance(exc, HunterBaseException):
                raise
            raise ServiceUnavailableError(f"SCRAM 用户创建失败: {exc}") from exc

        # ---------- 步骤 3: Topics ----------
        created_topics: list[str] = []
        steps["topics"] = _new_step("topics", "in_progress")
        try:
            created_topics = await self._admin.create_vehicle_topics(
                req.vehicle_id, skip_existing=req.takeover_existing
            )
            steps["topics"] = _new_step("topics", "ok")
        except Exception as exc:  # noqa: BLE001
            steps["topics"] = _new_step("topics", "failed", str(exc))
            # 回滚顺序：先删已建 Topic → 再删 SCRAM → 最后 DB 行
            await self._rollback(
                req.vehicle_id,
                steps,
                takeover_existing=req.takeover_existing,
                created_topics=created_topics,
                scram_password=scram_password,
            )
            await self._persist_status(req.vehicle_id, steps, failed=True)
            if isinstance(exc, HunterBaseException):
                raise
            raise ServiceUnavailableError(f"Topic 创建失败: {exc}") from exc

        # ---------- 步骤 4: Cert ----------
        device_cert_sn: str | None = None
        steps["cert"] = _new_step("cert", "in_progress")
        try:
            artifacts = await cert_signer.issue_client_cert(req.vehicle_id)
            device_cert_sn = artifacts.serial_hex
            steps["cert"] = _new_step("cert", "ok")
        except Exception as exc:  # noqa: BLE001
            steps["cert"] = _new_step("cert", "failed", str(exc))
            await self._rollback(
                req.vehicle_id,
                steps,
                takeover_existing=req.takeover_existing,
                created_topics=created_topics,
                scram_password=scram_password,
                issued_cert=True,
            )
            await self._persist_status(req.vehicle_id, steps, failed=True)
            if isinstance(exc, HunterBaseException):
                raise
            raise ServiceUnavailableError(f"证书签发失败: {exc}") from exc

        # ---------- 全步成功 → 落 ready + device_cert_sn ----------
        await self._persist_status(
            req.vehicle_id, steps, failed=False, device_cert_sn=device_cert_sn
        )
        logger.info(
            "provision_succeeded",
            vehicle_id=req.vehicle_id,
            topics_created=len(created_topics),
            cert_sn=device_cert_sn,
        )
        return ProvisionOutcome(
            vehicle_id=req.vehicle_id,
            scram_password=scram_password,
            device_cert_sn=device_cert_sn,
            steps=steps,
            aggregate=aggregate_state(steps),
        )

    # ---------- 回滚 ----------
    async def _rollback(
        self,
        vehicle_id: str,
        steps: dict[str, dict[str, Any]],
        *,
        takeover_existing: bool,
        created_topics: list[str] | None = None,
        scram_password: str | None = None,
        issued_cert: bool = False,
    ) -> None:
        """逆序回收已成功步骤；失败仅告警不掩盖原始异常。

        - 若某步 state != "ok" 视为未创建，跳过；
        - takeover_existing=true 时，"skipped" 步骤对应的外部资源不动（仅"ok"步骤 = 本次真的新建）；
        - DB 行保留（供审计与后续"下线"回收），不做 DELETE。
        """
        # 逆序：cert → topics → scram → db
        if issued_cert or steps.get("cert", {}).get("state") == "ok":
            try:
                cert_signer.delete_cert_dir(vehicle_id)
            except Exception:  # noqa: BLE001
                logger.exception("rollback_cert_failed", vehicle_id=vehicle_id)

        if created_topics:
            try:
                await self._required_admin().delete_topics(created_topics)
            except Exception:  # noqa: BLE001
                logger.exception("rollback_topics_failed", vehicle_id=vehicle_id, count=len(created_topics))

        if steps.get("scram", {}).get("state") == "ok" and not takeover_existing:
            try:
                await self._required_admin().delete_scram_user(vehicle_id)
            except Exception:  # noqa: BLE001
                logger.exception("rollback_scram_failed", vehicle_id=vehicle_id)

    def _required_admin(self) -> kafka_admin.KafkaAdminOps:
        if self._admin is None:
            raise ServiceUnavailableError("Kafka AdminClient 未就绪")
        return self._admin

    # ---------- 状态持久化 ----------
    async def _persist_status(
        self,
        vehicle_id: str,
        steps: dict[str, dict[str, Any]],
        *,
        failed: bool,
        device_cert_sn: str | None = None,
    ) -> None:
        aggregate: AggregateState = "failed" if failed else aggregate_state(steps)
        payload = {"state": aggregate, "steps": steps}
        try:
            await self._store.update_provision_status(
                vehicle_id, payload, device_cert_sn=device_cert_sn
            )
        except Exception:  # noqa: BLE001 - 状态回写失败不掩盖原始异常
            logger.exception("provision_status_persist_failed", vehicle_id=vehicle_id)

    # ---------- 下线：反序回收 ----------
    async def deprovision(self, vehicle_id: str, *, purge_topics: bool = True) -> dict[str, Any]:
        """下线（DELETE 端点）：证书目录 → SCRAM → Topic（可选）→ DB 行。

        返回清理摘要（供日志与响应 data 展示）。任一步失败视为部分完成，异常向上抛。
        与 provisioning 回滚不同：下线允许"外部资源不存在"视为幂等成功
        （kafka_admin / cert_signer 已内建此语义）。
        """
        summary: dict[str, Any] = {
            "cert_dir_removed": False,
            "scram_removed": False,
            "topics_removed": [],
            "ledger_removed": False,
        }
        # 1) 证书目录
        summary["cert_dir_removed"] = cert_signer.delete_cert_dir(vehicle_id)
        # 2) SCRAM 用户
        if self._admin is not None:
            await self._admin.delete_scram_user(vehicle_id)
            summary["scram_removed"] = True
        # 3) Topic（可选）
        if purge_topics and self._admin is not None:
            names = [t["name"] for t in kafka_admin.render_vehicle_topics(vehicle_id)]
            summary["topics_removed"] = await self._admin.delete_topics(names)
        # 4) DB 行
        summary["ledger_removed"] = await self._store.delete_ledger(vehicle_id)
        logger.info("deprovision_succeeded", vehicle_id=vehicle_id, **summary)
        return summary

    # ---------- 轮换 SCRAM 口令 ----------
    async def rotate_scram(self, vehicle_id: str) -> str:
        """重置 SCRAM 口令：AdminClient UPSERT 覆盖旧凭据；返回新一次性口令。"""
        new_password = secrets.token_urlsafe(24)
        await self._required_admin().upsert_scram_user(vehicle_id, new_password)
        logger.info("scram_rotated", vehicle_id=vehicle_id)
        return new_password

    # ---------- 重签证书 ----------
    async def reissue_cert(self, vehicle_id: str) -> tuple[str, datetime]:
        """重签客户端证书：覆盖 VEHICLE_CERTS_DIR/{id}/；返回 (serial_hex, issued_at_utc)。"""
        artifacts = await cert_signer.issue_client_cert(vehicle_id)
        # 更新 device_cert_sn（唯一索引；新 serial 与旧值不冲突）
        row = await self._store.get(vehicle_id)
        if row is not None:
            current = row.provision_status or initial_provision_status()
            steps = current.get("steps") or {}
            steps["cert"] = _new_step("cert", "ok")
            current["steps"] = steps
            current["state"] = aggregate_state(steps)
            await self._store.update_provision_status(
                vehicle_id, current, device_cert_sn=artifacts.serial_hex
            )
        return artifacts.serial_hex, datetime.now(UTC)


# =====================================================================
# 辅助：从 ORM 行投影到 Pydantic ProvisionStep 列表
# =====================================================================
def steps_to_list(provision_status: dict[str, Any] | None) -> list[ProvisionStep]:
    """把 JSONB steps 展平为顺序列表（供 VehicleRow.provision_steps 展示）。"""
    if not provision_status:
        return [
            ProvisionStep(name=name, state="pending")  # type: ignore[arg-type]
            for name in STEP_ORDER
        ]
    steps = provision_status.get("steps") or {}
    out: list[ProvisionStep] = []
    for name in STEP_ORDER:
        raw = steps.get(name) or {}
        out.append(
            ProvisionStep(
                name=name,  # type: ignore[arg-type]
                state=raw.get("state", "pending"),  # type: ignore[arg-type]
                error=raw.get("error"),
                ts=raw.get("ts"),
            )
        )
    return out


def aggregate_from_row(provision_status: dict[str, Any] | None) -> AggregateState:
    if not provision_status:
        return "pending"
    state = provision_status.get("state")
    if state in ("pending", "in_progress", "ready", "failed"):
        return state  # type: ignore[return-value]
    return aggregate_state(provision_status.get("steps") or {})


__all__ = [
    "ProvisionOutcome",
    "Provisioner",
    "STEP_ORDER",
    "aggregate_from_row",
    "aggregate_state",
    "initial_provision_status",
    "steps_to_list",
]

# 保留 import 供 typing（未使用则清理避免 lint 噪声）
_ = ResourceAlreadyExistsError
