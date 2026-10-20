"""预签名契约门禁单测（V1.21.19）。

锁死两个曾被真实故障证伪、且极易被"内存替身"掩盖的契约：

1. **协程形状**：aioboto3 的 ``generate_presigned_url`` 是**协程函数**（与同步 boto3 不同）。
   生产代码 ``presign_get`` 必须 ``await`` 它。V1.21.16 起用返回裸 ``str`` 的替身做单测，
   漏 ``await`` 时协程对象被当字符串塞进 Pydantic ``str`` 字段 → 实机 500（V1.21.18 才修复），
   单测全程未捕获。本门禁让替身**对齐真库协程形状**：一旦去掉 ``await``，``presign_get``
   即返回协程对象，断言 ``isinstance(url, str)`` 立刻失败。

2. **预签名 host**：下载 URL 必须走 **public endpoint**（浏览器可达），不能用集群内
   ``minio:9000``。此前 remote-control 单 client 令 host 恒为内部地址 → 能返回链接但点了不播。
"""
from __future__ import annotations

import inspect
from types import SimpleNamespace
from typing import Any

import pytest

from app.repositories.storage import S3VideoArchiveStorage

_INTERNAL_URL = "http://minio:9000"
_PUBLIC_URL = "http://192.168.31.35:9000"


class _Aioboto3ShapedClient:
    """aioboto3 客户端替身：**刻意对齐真库形状**——``generate_presigned_url`` 是协程函数。

    ⚠ 不得改成同步返回裸 str：那正是 V1.21.16→V1.21.18 掩盖漏 await 缺陷的替身写法。
    """

    def __init__(self, endpoint_url: str) -> None:
        self.endpoint_url = endpoint_url
        self.presign_calls: list[dict[str, Any]] = []

    async def generate_presigned_url(
        self, ClientMethod: str, Params: dict[str, Any] | None = None, ExpiresIn: int = 604800, **_: Any
    ) -> str:
        self.presign_calls.append({"ClientMethod": ClientMethod, "ExpiresIn": ExpiresIn})
        return f"{self.endpoint_url}/{Params['Bucket']}/{Params['Key']}?X-Amz-Expires={ExpiresIn}"


class _FakeClientCM:
    """对齐真库：``session.client(...)`` 返回**同步构造的异步上下文管理器**（__aenter__ 为协程）。"""

    def __init__(self, client: _Aioboto3ShapedClient) -> None:
        self._client = client

    async def __aenter__(self) -> _Aioboto3ShapedClient:
        return self._client

    async def __aexit__(self, *exc: object) -> None:
        return None


def _make_settings() -> SimpleNamespace:
    """最小 config：内部/公网 endpoint 刻意不同，便于断言 presign 走哪一个。"""
    return SimpleNamespace(
        minio_endpoint_url=_INTERNAL_URL,
        minio_public_endpoint_url=_PUBLIC_URL,
        minio_access_key="ak",
        minio_secret_key="sk",
    )


@pytest.fixture
def patched_storage(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[S3VideoArchiveStorage, dict[str, _Aioboto3ShapedClient]]:
    """注入按 endpoint 分建的协程形状替身，记录每个 endpoint 创建的 client。"""
    created: dict[str, _Aioboto3ShapedClient] = {}

    def fake_create_client(self: S3VideoArchiveStorage, endpoint_url: str) -> Any:
        # 同步返回异步上下文管理器（与真库 session.client(...) 一致）
        client = _Aioboto3ShapedClient(endpoint_url)
        created[endpoint_url] = client
        return _FakeClientCM(client)

    monkeypatch.setattr(S3VideoArchiveStorage, "_create_client", fake_create_client)
    storage = S3VideoArchiveStorage(_make_settings(), bucket="hunter-video", region="us-east-1")
    return storage, created


async def test_presign_get_awaits_coroutine_and_returns_str(patched_storage) -> None:
    """漏 await 时 presign_get 返回协程对象 → 本断言失败（复现并封堵 V1.21.18 的 500 根因）。"""
    storage, _ = patched_storage
    url = await storage.presign_get("remote-control/HUNTER-001/2026/10/20/s1.mp4", 900)

    assert isinstance(url, str), (
        f"presign_get 返回 {type(url).__name__}：aioboto3.generate_presigned_url 是协程，"
        "必须 await，否则协程对象会被下游按 str 校验拒绝（ValidationError → 500）"
    )
    assert "?X-Amz-Expires=900" in url


async def test_presign_get_uses_public_endpoint_host(patched_storage) -> None:
    """预签名必须走 public endpoint，URL host 为浏览器可达地址（封堵 V1.21.19 不播根因）。"""
    storage, created = patched_storage
    url = await storage.presign_get("remote-control/HUNTER-001/2026/10/20/s1.mp4", 900)

    assert url.startswith(_PUBLIC_URL), f"预签名 host 非公网可达地址：{url}"
    assert _INTERNAL_URL not in url and "minio:9000" not in url
    # 仅创建 presign client，内部 client 不应被 presign 路径触发
    assert list(created) == [_PUBLIC_URL]
    assert storage._client is None


async def test_head_video_still_uses_internal_endpoint(patched_storage) -> None:
    """服务侧操作（head）仍走内部 endpoint，与 presign 双 client 隔离。"""
    storage, created = patched_storage

    def fake_create_client(endpoint_url):  # 实例属性赋值：不会被绑定，无 self
        client = _Aioboto3ShapedClient(endpoint_url)

        async def head_object(Bucket, Key):  # 参数名对齐 botocore（Bucket/Key 大驼峰）
            return {"ContentLength": 123}

        client.head_object = head_object  # type: ignore[method-assign]
        created[endpoint_url] = client
        return _FakeClientCM(client)

    storage._create_client = fake_create_client  # type: ignore[method-assign]
    size = await storage.head_video("remote-control/HUNTER-001/2026/10/20/s1.mp4")

    assert size == 123
    assert list(created) == [_INTERNAL_URL]


def test_fake_shape_mirrors_real_library_coroutine() -> None:
    """替身形状自检：generate_presigned_url 必须是协程函数（对齐真库 aioboto3）。

    若有人把替身改回同步返回裸 str，本用例即失败，防止再次失去对漏 await 的捕获能力。
    """
    assert inspect.iscoroutinefunction(_Aioboto3ShapedClient.generate_presigned_url)
