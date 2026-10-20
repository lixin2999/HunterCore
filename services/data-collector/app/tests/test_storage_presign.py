"""MinioStorage 双 client 行为单测（MINIO_PUBLIC_ENDPOINT 预签名 host 分离）。

契约依据：contracts/openapi/data-collector.yaml x-hunter-file-upload-flow.presign.url_host
- 预签名（presign_put / presign_upload_part / presign_get）必须走 _presign_client
  （endpoint = MINIO_PUBLIC_ENDPOINT，车端可达地址）；
- 服务侧操作（head / 分片会话 / 打标 / 列举 / 流式哈希）必须走内部 client
  （endpoint = MINIO_ENDPOINT，集群内高速地址）；
- 未注入 presign client 时回落内部 client（单机同网段/旧测试兼容）。
"""
from __future__ import annotations

import base64
import contextlib
import hashlib
from typing import Any

import boto3
from botocore.awsrequest import AWSResponse
from botocore.exceptions import ParamValidationError
from botocore.session import get_session
from botocore.validate import ParamValidator

from app.repositories import storage as storage_module
from app.repositories.storage import MinioStorage, _inject_content_md5, _register_content_md5


class _RecordingClient:
    """记录调用的 boto3 客户端替身（按 name 标识归属，URL 可断言走向）。"""

    def __init__(self, name: str) -> None:
        self.name = name
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def __getattr__(self, item: str):  # 动态替身，签名不可枚举
        def _call(*args: Any, **kwargs: Any) -> Any:
            self.calls.append((item, kwargs))
            if item == "generate_presigned_url":
                params = kwargs.get("Params") or {}
                return f"{self.name}://{params.get('Bucket', '')}/{params.get('Key', '')}"
            if item == "create_multipart_upload":
                return {"UploadId": "upload-1"}
            return {}

        return _call

    def methods(self) -> list[str]:
        return [name for name, _ in self.calls]


def test_presign_methods_use_presign_client() -> None:
    internal, presign = _RecordingClient("internal"), _RecordingClient("presign")
    store = MinioStorage(internal_client=internal, presign_client=presign)

    url = store.presign_put("hunter-rosbag", "rosbag/V1/2026-10-17/rosbag/1.ts", "video/mp4")
    assert url.startswith("presign://hunter-rosbag/")
    store.presign_upload_part("hunter-rosbag", "k", "upload-1", 2)
    store.presign_get("hunter-rosbag", "k")

    assert presign.methods().count("generate_presigned_url") == 3
    assert internal.methods() == []  # 服务侧 client 不得收到任何预签名请求


def test_service_side_operations_use_internal_client() -> None:
    internal, presign = _RecordingClient("internal"), _RecordingClient("presign")
    store = MinioStorage(internal_client=internal, presign_client=presign)

    store.create_multipart_upload("hunter-rosbag", "k", "application/octet-stream")
    store.complete_multipart_upload("hunter-rosbag", "k", "upload-1", [{"part_number": 1, "etag": "e"}])
    store.abort_multipart_upload("hunter-rosbag", "k", "upload-1")
    store.put_object_tags("hunter-rosbag", "k", "event")
    store.list_objects("hunter-rosbag", "rosbag/V1/", 100)

    assert "create_multipart_upload" in internal.methods()
    assert "complete_multipart_upload" in internal.methods()
    assert "abort_multipart_upload" in internal.methods()
    assert "put_object_tagging" in internal.methods()
    assert "list_objects_v2" in internal.methods()
    assert presign.calls == []  # 服务侧操作不得走外部 endpoint


def test_single_client_fallback_backward_compatible() -> None:
    """未配置 MINIO_PUBLIC_ENDPOINT（get_storage 两 client 同源）/旧单 client 注入：预签名仍可用。"""
    only = _RecordingClient("only")
    store = MinioStorage(internal_client=only)
    assert store.presign_get("b", "k").startswith("only://b/k")
    assert only.methods().count("generate_presigned_url") == 1


async def test_get_storage_builds_clients_per_endpoint(monkeypatch: Any) -> None:
    """get_storage：内部 client 用 minio_endpoint_url，预签名 client 用 minio_public_endpoint_url。"""
    endpoints: list[str] = []

    def _fake_build(endpoint_url: str) -> Any:
        endpoints.append(endpoint_url)
        return _RecordingClient(endpoint_url)

    monkeypatch.setattr(storage_module, "_build_sync_client", _fake_build)
    monkeypatch.setattr(storage_module.settings, "minio_endpoint", "minio:9000")
    monkeypatch.setattr(storage_module.settings, "minio_secure", False)
    monkeypatch.setattr(storage_module.settings, "minio_public_endpoint", "203.0.113.10:9000")

    store = await storage_module.get_storage()
    assert endpoints == ["http://minio:9000", "http://203.0.113.10:9000"]
    assert store._presign_client.name == "http://203.0.113.10:9000"  # type: ignore[attr-defined]

    # 回落：未配置公网地址时两 client 指向同一内部地址
    endpoints.clear()
    monkeypatch.setattr(storage_module.settings, "minio_public_endpoint", None)
    store2 = await storage_module.get_storage()
    assert endpoints == ["http://minio:9000", "http://minio:9000"]
    assert store2._presign_client.name == "http://minio:9000"  # type: ignore[attr-defined]


class _SdkValidatingClient:
    """按**真实 botocore 服务模型**校验 ``list_objects_v2`` 入参的客户端替身。

    未知入参（如把 v1 的 ``Marker`` 传给 v2）即抛 ``ParamValidationError``——模拟真实
    SDK 客户端行为。V1.21.16 形状门禁：替身不得靠「收得下就算通过」掩盖入参漂移。
    """

    def __init__(self) -> None:
        self.list_calls: list[dict[str, Any]] = []
        self._shape = (
            get_session().get_service_model("s3").operation_model("ListObjectsV2").input_shape
        )

    def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
        report = ParamValidator().validate(kwargs, self._shape)
        if report.has_errors():
            raise ParamValidationError(report=report.generate_report())
        self.list_calls.append(kwargs)
        return {
            "Contents": [
                {"Key": "rosbag/a.bag", "Size": 1, "ETag": '"e1"'},
                {"Key": "rosbag/b.bag", "Size": 2, "ETag": '"e2"'},
            ],
            "IsTruncated": True,  # v2 无 NextMarker → 实现回退末项 Key 作游标
        }


def test_list_objects_v2_cursor_is_accepted_by_real_sdk_model() -> None:
    """回归（V1.21.16）：「加载更多（游标分页）」不得 500。

    实机判据：首页（无游标）正常、带 marker 的第二页恒 500「服务器内部错误」；根因是旧实现
    把 v1 的 ``Marker`` 传给 ``list_objects_v2``（botocore 判未知入参 → ParamValidationError）。
    """
    client = _SdkValidatingClient()
    store = MinioStorage(internal_client=client)

    store.list_objects("hunter-rosbag", "rosbag/", 100)  # 首页：无游标
    # 加载更多：带游标必须被真实 SDK 模型接受（旧实现此处抛 ParamValidationError → 500）
    items, truncated, next_marker = store.list_objects(
        "hunter-rosbag", "rosbag/", 100, marker="rosbag/a.bag"
    )

    assert client.list_calls[-1]["StartAfter"] == "rosbag/a.bag"
    assert "Marker" not in client.list_calls[-1]
    assert truncated is True
    assert next_marker == "rosbag/b.bag"  # v2 无 NextMarker → 回退末项 Key
    assert [item["object_key"] for item in items] == ["rosbag/a.bag", "rosbag/b.bag"]


# ---------------------------------------------------------------------------
# DeleteObjects Content-MD5 补算（V1.21.21：MinIO 要求、新版 botocore 不再自动加）
# ---------------------------------------------------------------------------
def test_inject_content_md5_sets_base64_md5_of_body() -> None:
    """纯函数：base64(MD5(body)) 写入 Content-MD5；str 与 bytes 正文同值。"""
    body = b"<Delete><Object><Key>k</Key></Object></Delete>"
    params: dict[str, Any] = {"body": body, "headers": {}}
    _inject_content_md5(params)
    assert params["headers"]["Content-MD5"] == base64.b64encode(hashlib.md5(body).digest()).decode()

    # 已存在 Content-MD5（任意大小写）不覆盖
    keep = {"Content-MD5": "preset"}
    _inject_content_md5({"body": body, "headers": keep})
    assert keep["Content-MD5"] == "preset"


def test_delete_objects_request_carries_content_md5_real_sdk() -> None:
    """形状门禁（对齐真库）：经 `_register_content_md5` 后，真实 botocore 发出的
    DeleteObjects 请求必须携带与实际正文一致的 Content-MD5。

    内存替身会掩盖此问题（根本不走签名/序列化）⇒ 必须用真 botocore client，
    在 before-send 短路（不发网络）捕获最终请求。复现并封堵实机 MissingContentMD5。
    """
    client = boto3.client(
        "s3",
        endpoint_url="http://127.0.0.1:9",
        aws_access_key_id="x",
        aws_secret_access_key="y",
        region_name="us-east-1",
    )
    _register_content_md5(client)

    captured: dict[str, Any] = {}

    class _Captured(Exception):
        """捕获到最终请求后中断发送（不发网络）。"""

    def _short_circuit(request: Any, **_kwargs: Any) -> AWSResponse:
        captured["md5"] = dict(request.headers).get("Content-MD5")
        captured["body"] = request.body
        raise _Captured

    client.meta.events.register("before-send.s3.DeleteObjects", _short_circuit)
    with contextlib.suppress(_Captured):
        client.delete_objects(Bucket="b", Delete={"Objects": [{"Key": "k1"}, {"Key": "k2"}]})

    md5_header = captured.get("md5")
    body = captured.get("body")
    assert md5_header is not None, "DeleteObjects 请求必须携带 Content-MD5"
    md5_str = md5_header.decode() if isinstance(md5_header, bytes) else str(md5_header)
    body_bytes = body.encode() if isinstance(body, str) else body
    expect = base64.b64encode(hashlib.md5(body_bytes).digest()).decode()
    assert md5_str == expect, "Content-MD5 必须与实际请求体一致（否则 MinIO 返 BadDigest）"
