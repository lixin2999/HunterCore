"""命令下发路由（§9.4 POST /api/v1/ingest/commands）。"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends
from hunter_common.responses import success_response

from app.core.dependencies import ExecuteUser, get_cmd_service
from app.schemas.schemas import IssueCmdReq, IssueCmdResp
from app.services.cmd_service import CmdService

router = APIRouter(prefix="/api/v1/ingest/commands", tags=["commands"])


@router.post("", response_model=None)
async def issue_command(
    body: IssueCmdReq,
    _user: ExecuteUser,
    svc: Annotated[CmdService, Depends(get_cmd_service)],
) -> dict:
    """下发车端命令（§9.4）。

    cmd_type 支持：reupload / reparse / cleanup / seal / set_config。
    """
    cmd_type = body.cmd_type
    vehicle_id = body.vehicle_id
    payload = body.payload

    if cmd_type == "reupload":
        cmd_id = await svc.issue_reupload(vehicle_id, payload.get("bag_id", ""))
    elif cmd_type == "reparse":
        cmd_id = await svc.issue_reparse(vehicle_id, payload.get("bag_id", ""))
    elif cmd_type == "cleanup":
        cmd_id = await svc.issue_cleanup(vehicle_id, list(payload.get("bag_ids", [])))
    elif cmd_type == "seal":
        cmd_id = await svc.issue_seal(vehicle_id, payload.get("bag_path", ""))
    else:
        # 通用命令（set_config 等）
        cmd_id = await svc.issue_cmd(vehicle_id, cmd_type, payload)

    return success_response(data={
        "cmd_id": cmd_id,
        "status": "issued",
    }).model_dump()
