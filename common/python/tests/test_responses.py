"""hunter_common.responses 单元测试：统一响应五字段结构必须符合设计文档。"""
from __future__ import annotations

from pydantic import BaseModel

from hunter_common.responses import ApiResponse, error_response, success_response


def test_success_response_default_fields() -> None:
    resp = success_response(data={"k": "v"})
    assert resp.code == 0
    assert resp.message == "success"
    assert resp.data == {"k": "v"}
    assert resp.request_id  # 默认生成 UUID
    assert resp.timestamp > 0


def test_error_response_payload_shape() -> None:
    """统一响应字段名固定：code/message/data/request_id/timestamp。"""
    resp = error_response(3001, "资源不存在", request_id="req-1")
    dumped = resp.model_dump()
    assert set(dumped.keys()) == {"code", "message", "data", "request_id", "timestamp"}
    assert dumped["code"] == 3001
    assert dumped["message"] == "资源不存在"
    assert dumped["data"] is None
    assert dumped["request_id"] == "req-1"


def test_api_response_is_generic() -> None:
    class Item(BaseModel):
        name: str

    resp = ApiResponse[Item](data=Item(name="x"))
    assert resp.data is not None
    assert resp.data.name == "x"
