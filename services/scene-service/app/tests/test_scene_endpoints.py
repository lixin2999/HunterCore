"""场景库端点测试（契约 paths ↔ 实现：正常流程 + 错误流程 + 错误码）。

覆盖：GET/POST /api/v1/scene、GET/PUT/DELETE /api/v1/scene/{scene_id}、
POST .../duplicate|publish、GET .../templates、POST /api/v1/scene/export、POST .../run。
所有响应必须是统一五字段格式（契约 ApiResponse）。
"""
from __future__ import annotations

from uuid import uuid4

import pytest
from httpx import AsyncClient
from hunter_common.database.enums import SceneStatus

from app.config import settings
from app.tests.conftest import ADMIN_HEADERS, VIEWER_HEADERS
from app.tests.fakes import (
    InMemorySceneRepository,
    config_payload,
    create_payload,
    make_scene_model,
)

RESPONSE_FIELDS = {"code", "message", "data", "request_id", "timestamp"}
SCENE_BASE = "/api/v1/scene"


def _assert_envelope(body: dict[str, object], code: int = 0) -> dict[str, object]:
    """统一响应格式校验（五字段 + 错误码）。"""
    assert set(body) == RESPONSE_FIELDS
    assert body["code"] == code
    assert body["request_id"]
    return body


# =====================================================================
# GET /api/v1/scene（listScenes）
# =====================================================================
async def test_list_scenes_returns_paged_envelope(client: AsyncClient, scene_env: object) -> None:
    """列表返回分页数据与统一响应体（默认 create_time DESC）。"""
    repository: InMemorySceneRepository = scene_env.repository  # type: ignore[attr-defined]
    repository.rows = {
        row.scene_id: row
        for row in (
            make_scene_model(scene_name="场景-1"),
            make_scene_model(scene_name="场景-2", scene_type="rainy"),
        )
    }
    resp = await client.get(SCENE_BASE, headers=ADMIN_HEADERS, params={"page": 1, "page_size": 10})
    assert resp.status_code == 200
    data = _assert_envelope(resp.json())["data"]
    assert set(data) == {"items", "total", "page", "page_size"}  # type: ignore[arg-type]
    assert data["total"] == 2 and len(data["items"]) == 2  # type: ignore[index]


async def test_list_scenes_requires_authentication(client: AsyncClient) -> None:
    """缺失网关注入身份头 → 401 + code=1001。"""
    resp = await client.get(SCENE_BASE)
    assert resp.status_code == 401
    _assert_envelope(resp.json(), code=1001)


async def test_list_scenes_forbidden_for_viewer(client: AsyncClient, scene_env: object) -> None:
    """角色不含 scene:read → 403 + code=1002。"""
    resp = await client.get(SCENE_BASE, headers=VIEWER_HEADERS)
    assert resp.status_code == 403
    _assert_envelope(resp.json(), code=1002)


async def test_list_scenes_applies_filters(client: AsyncClient, scene_env: object) -> None:
    """分类/状态/标签/关键字筛选与软删除过滤（契约描述语义）。"""
    repository: InMemorySceneRepository = scene_env.repository  # type: ignore[attr-defined]
    rainy = make_scene_model(scene_name="雨天场景", scene_type="rainy", tags=["env", "rain"])
    published = make_scene_model(
        scene_name="已发布场景", status=SceneStatus.PUBLISHED, tags=["published"]
    )
    deleted = make_scene_model(scene_name="已删除场景")
    deleted.deleted_at = deleted.create_time
    repository.rows = {row.scene_id: row for row in (rainy, published, deleted)}

    resp = await client.get(
        SCENE_BASE,
        headers=ADMIN_HEADERS,
        params={"scene_type": "rainy", "status": "draft", "tags": ["rain"], "keyword": "雨天"},
    )
    data = _assert_envelope(resp.json())["data"]
    assert data["total"] == 1  # type: ignore[index]
    assert data["items"][0]["scene_name"] == "雨天场景"  # type: ignore[index]

    all_resp = await client.get(SCENE_BASE, headers=ADMIN_HEADERS)
    all_data = _assert_envelope(all_resp.json())["data"]
    assert all_data["total"] == 2  # 软删除场景不计入  # type: ignore[index]


async def test_list_scenes_rejects_page_size_over_max(client: AsyncClient, scene_env: object) -> None:
    """page_size 超过契约上限 200 → 422 + code=2001。"""
    resp = await client.get(SCENE_BASE, headers=ADMIN_HEADERS, params={"page_size": 201})
    assert resp.status_code == 422
    _assert_envelope(resp.json(), code=2001)


async def test_list_scenes_returns_5001_when_database_unreachable(
    client: AsyncClient, scene_env: object
) -> None:
    """PostgreSQL 不可达（连接拒绝）→ 503 + code=5001（契约 5001 覆盖依赖不可达）。"""

    async def _raise(**_: object) -> object:
        raise ConnectionRefusedError("数据库连接被拒绝")

    scene_env.repository.list_page = _raise  # type: ignore[attr-defined]
    resp = await client.get(SCENE_BASE, headers=ADMIN_HEADERS)
    assert resp.status_code == 503
    _assert_envelope(resp.json(), code=5001)


async def test_list_scenes_rejects_invalid_sort_field(client: AsyncClient, scene_env: object) -> None:
    """排序字段必须命中白名单 enum → 否则 422。"""
    resp = await client.get(SCENE_BASE, headers=ADMIN_HEADERS, params={"sort": "config_json"})
    assert resp.status_code == 422


# =====================================================================
# POST /api/v1/scene（createScene）
# =====================================================================
async def test_create_scene_returns_draft_with_server_fields(
    client: AsyncClient, scene_env: object
) -> None:
    """创建成功 201：status=draft、creator=网关注入用户、version 默认 1.0.0。"""
    resp = await client.post(SCENE_BASE, headers=ADMIN_HEADERS, json=create_payload())
    assert resp.status_code == 201
    data = _assert_envelope(resp.json())["data"]
    assert data["status"] == "draft"  # type: ignore[index]
    assert data["creator"] == ADMIN_HEADERS["X-User-Id"]  # type: ignore[index]
    assert data["version"] == "1.0.0"  # type: ignore[index]
    assert set(data["config"]) == {  # type: ignore[index]
        "map",
        "ego_vehicle",
        "weather",
        "actors",
        "events",
        "success_criteria",
        "duration",
    }
    assert data["config"]["weather"]["sun_altitude"] == 45.0  # type: ignore[index]


async def test_create_scene_conflict_returns_3002(client: AsyncClient, scene_env: object) -> None:
    """存活场景内重名 → 409 + code=3002。"""
    payload = create_payload("重名场景")
    first = await client.post(SCENE_BASE, headers=ADMIN_HEADERS, json=payload)
    assert first.status_code == 201
    second = await client.post(SCENE_BASE, headers=ADMIN_HEADERS, json=payload)
    assert second.status_code == 409
    _assert_envelope(second.json(), code=3002)


async def test_create_scene_rejects_unknown_field(client: AsyncClient, scene_env: object) -> None:
    """契约 additionalProperties=false → 未声明字段 422 + code=2001。"""
    payload = create_payload()
    payload["status"] = "published"
    resp = await client.post(SCENE_BASE, headers=ADMIN_HEADERS, json=payload)
    assert resp.status_code == 422
    _assert_envelope(resp.json(), code=2001)


async def test_create_scene_rejects_duration_over_limit(
    client: AsyncClient, scene_env: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """场景时长超过配置上限 → 422 + code=2001（上限经环境变量化）。"""
    monkeypatch.setattr(settings, "scene_duration_max_seconds", 60.0)
    payload = create_payload("超长场景", config=config_payload(duration=120.0))
    resp = await client.post(SCENE_BASE, headers=ADMIN_HEADERS, json=payload)
    assert resp.status_code == 422
    _assert_envelope(resp.json(), code=2001)


async def test_create_scene_requires_write_role(client: AsyncClient, scene_env: object) -> None:
    """viewer 无 scene:create 权限 → 403 + code=1002。"""
    resp = await client.post(SCENE_BASE, headers=VIEWER_HEADERS, json=create_payload())
    assert resp.status_code == 403
    _assert_envelope(resp.json(), code=1002)


# =====================================================================
# GET /api/v1/scene/{scene_id}（getScene）
# =====================================================================
async def test_get_scene_caches_detail(client: AsyncClient, scene_env: object) -> None:
    """详情返回完整对象（含 config），并按 cache:scene:{scene_id} 回填缓存。"""
    model = make_scene_model(scene_name="详情场景")
    scene_env.repository.rows[model.scene_id] = model  # type: ignore[attr-defined]
    resp = await client.get(f"{SCENE_BASE}/{model.scene_id}", headers=ADMIN_HEADERS)
    assert resp.status_code == 200
    data = _assert_envelope(resp.json())["data"]
    assert data["scene_id"] == str(model.scene_id)  # type: ignore[index]
    assert scene_env.cache.store[str(model.scene_id)]["scene_name"] == "详情场景"  # type: ignore[attr-defined]

    # 缓存命中：仓储被清空后仍能返回（证明读自缓存）
    scene_env.repository.rows.clear()  # type: ignore[attr-defined]
    cached = await client.get(f"{SCENE_BASE}/{model.scene_id}", headers=ADMIN_HEADERS)
    assert cached.status_code == 200
    assert cached.json()["data"]["scene_name"] == "详情场景"


async def test_get_scene_not_found_returns_3001(client: AsyncClient, scene_env: object) -> None:
    """未知场景 ID → 404 + code=3001。"""
    resp = await client.get(f"{SCENE_BASE}/{uuid4()}", headers=ADMIN_HEADERS)
    assert resp.status_code == 404
    _assert_envelope(resp.json(), code=3001)


async def test_get_scene_rejects_invalid_uuid(client: AsyncClient, scene_env: object) -> None:
    """路径参数非 UUID → 422 + code=2001。"""
    resp = await client.get(f"{SCENE_BASE}/not-a-uuid", headers=ADMIN_HEADERS)
    assert resp.status_code == 422
    _assert_envelope(resp.json(), code=2001)


# =====================================================================
# PUT / DELETE /api/v1/scene/{scene_id}（updateScene / deleteScene）
# =====================================================================
async def test_update_scene_edits_draft_and_invalidates_cache(
    client: AsyncClient, scene_env: object
) -> None:
    """draft 可全量更新；写后失效缓存。"""
    model = make_scene_model(scene_name="待编辑场景")
    scene_env.repository.rows[model.scene_id] = model  # type: ignore[attr-defined]
    scene_env.cache.store[str(model.scene_id)] = {"scene_name": "旧缓存"}  # type: ignore[attr-defined]
    payload = {
        "scene_name": "已编辑场景",
        "scene_type": "curve_driving",
        "description": "更新后",
        "tags": ["edited"],
        "config": config_payload(duration=25.0),
    }
    resp = await client.put(f"{SCENE_BASE}/{model.scene_id}", headers=ADMIN_HEADERS, json=payload)
    assert resp.status_code == 200
    data = _assert_envelope(resp.json())["data"]
    assert data["scene_name"] == "已编辑场景"  # type: ignore[index]
    assert data["config"]["duration"] == 25.0  # type: ignore[index]
    assert str(model.scene_id) in scene_env.cache.invalidated  # type: ignore[attr-defined]
    assert str(model.scene_id) not in scene_env.cache.store  # type: ignore[attr-defined]


async def test_update_scene_state_conflict_returns_3003(
    client: AsyncClient, scene_env: object
) -> None:
    """published 场景不可编辑 → 409 + code=3003。"""
    model = make_scene_model(scene_name="已发布场景", status=SceneStatus.PUBLISHED)
    scene_env.repository.rows[model.scene_id] = model  # type: ignore[attr-defined]
    payload = {
        "scene_name": "改名尝试",
        "scene_type": "curve_driving",
        "tags": [],
        "config": config_payload(),
    }
    resp = await client.put(f"{SCENE_BASE}/{model.scene_id}", headers=ADMIN_HEADERS, json=payload)
    assert resp.status_code == 409
    _assert_envelope(resp.json(), code=3003)


async def test_update_scene_not_found_returns_3001(client: AsyncClient, scene_env: object) -> None:
    """未知场景更新 → 404 + code=3001。"""
    payload = {
        "scene_name": "任意名称",
        "scene_type": "custom",
        "tags": [],
        "config": config_payload(),
    }
    resp = await client.put(f"{SCENE_BASE}/{uuid4()}", headers=ADMIN_HEADERS, json=payload)
    assert resp.status_code == 404
    _assert_envelope(resp.json(), code=3001)


async def test_delete_scene_soft_deletes_draft(client: AsyncClient, scene_env: object) -> None:
    """draft 软删除成功（deleted=true，软删除后详情 404）。"""
    model = make_scene_model(scene_name="待删除场景")
    scene_env.repository.rows[model.scene_id] = model  # type: ignore[attr-defined]
    resp = await client.delete(f"{SCENE_BASE}/{model.scene_id}", headers=ADMIN_HEADERS)
    assert resp.status_code == 200
    data = _assert_envelope(resp.json())["data"]
    assert data["deleted"] is True  # type: ignore[index]
    assert model.deleted_at is not None
    gone = await client.get(f"{SCENE_BASE}/{model.scene_id}", headers=ADMIN_HEADERS)
    assert gone.status_code == 404


async def test_delete_published_scene_returns_3003(client: AsyncClient, scene_env: object) -> None:
    """published 场景需先归档，直接删除 → 409 + code=3003。"""
    model = make_scene_model(scene_name="已发布不可删", status=SceneStatus.PUBLISHED)
    scene_env.repository.rows[model.scene_id] = model  # type: ignore[attr-defined]
    resp = await client.delete(f"{SCENE_BASE}/{model.scene_id}", headers=ADMIN_HEADERS)
    assert resp.status_code == 409
    _assert_envelope(resp.json(), code=3003)


async def test_delete_archived_scene_allowed(client: AsyncClient, scene_env: object) -> None:
    """archived 场景可删除（契约 deletable_states = draft/archived）。"""
    model = make_scene_model(scene_name="已归档场景", status=SceneStatus.ARCHIVED)
    scene_env.repository.rows[model.scene_id] = model  # type: ignore[attr-defined]
    resp = await client.delete(f"{SCENE_BASE}/{model.scene_id}", headers=ADMIN_HEADERS)
    assert resp.status_code == 200
    assert resp.json()["data"]["deleted"] is True


# =====================================================================
# POST /api/v1/scene/{scene_id}/duplicate（duplicateScene）
# =====================================================================
async def test_duplicate_scene_generates_copy_name(client: AsyncClient, scene_env: object) -> None:
    """省略请求体时按 <源名称>-copy 生成名称，副本为草稿且 version 重置。"""
    source = make_scene_model(scene_name="源场景", version="2.3.1")
    scene_env.repository.rows[source.scene_id] = source  # type: ignore[attr-defined]
    resp = await client.post(f"{SCENE_BASE}/{source.scene_id}/duplicate", headers=ADMIN_HEADERS)
    assert resp.status_code == 201
    data = _assert_envelope(resp.json())["data"]
    assert data["scene_name"] == "源场景-copy"  # type: ignore[index]
    assert data["status"] == "draft"  # type: ignore[index]
    assert data["version"] == "1.0.0"  # type: ignore[index]
    assert data["scene_id"] != str(source.scene_id)  # type: ignore[index]

    second = await client.post(f"{SCENE_BASE}/{source.scene_id}/duplicate", headers=ADMIN_HEADERS)
    assert second.json()["data"]["scene_name"] == "源场景-copy-2"


async def test_duplicate_scene_with_conflicting_name_returns_3002(
    client: AsyncClient, scene_env: object
) -> None:
    """显式指定名称且已存在 → 409 + code=3002。"""
    source = make_scene_model(scene_name="源场景-B")
    occupied = make_scene_model(scene_name="占用名称")
    scene_env.repository.rows[source.scene_id] = source  # type: ignore[attr-defined]
    scene_env.repository.rows[occupied.scene_id] = occupied  # type: ignore[attr-defined]
    resp = await client.post(
        f"{SCENE_BASE}/{source.scene_id}/duplicate",
        headers=ADMIN_HEADERS,
        json={"new_scene_name": "占用名称"},
    )
    assert resp.status_code == 409
    _assert_envelope(resp.json(), code=3002)


async def test_duplicate_scene_not_found_returns_3001(
    client: AsyncClient, scene_env: object
) -> None:
    """源场景不存在 → 404 + code=3001。"""
    resp = await client.post(f"{SCENE_BASE}/{uuid4()}/duplicate", headers=ADMIN_HEADERS)
    assert resp.status_code == 404
    _assert_envelope(resp.json(), code=3001)


# =====================================================================
# POST /api/v1/scene/{scene_id}/publish（publishScene）
# =====================================================================
async def test_publish_scene_moves_draft_to_published(
    client: AsyncClient, scene_env: object
) -> None:
    """draft → published；未显式传 version 时版本保持不变（契约待确认 #7）。"""
    model = make_scene_model(scene_name="待发布场景", version="1.2.0")
    scene_env.repository.rows[model.scene_id] = model  # type: ignore[attr-defined]
    resp = await client.post(f"{SCENE_BASE}/{model.scene_id}/publish", headers=ADMIN_HEADERS)
    assert resp.status_code == 200
    data = _assert_envelope(resp.json())["data"]
    assert data["status"] == "published"  # type: ignore[index]
    assert data["version"] == "1.2.0"  # type: ignore[index]


async def test_publish_scene_updates_version_when_given(
    client: AsyncClient, scene_env: object
) -> None:
    """显式传 version（≥ 当前）时更新 scenes.version。"""
    model = make_scene_model(scene_name="版本发布场景", version="1.0.0")
    scene_env.repository.rows[model.scene_id] = model  # type: ignore[attr-defined]
    resp = await client.post(
        f"{SCENE_BASE}/{model.scene_id}/publish",
        headers=ADMIN_HEADERS,
        json={"version": "1.1.0"},
    )
    assert resp.status_code == 200
    assert resp.json()["data"]["version"] == "1.1.0"


async def test_publish_scene_rejects_lower_version(client: AsyncClient, scene_env: object) -> None:
    """发布版本低于当前版本 → 422 + code=2001。"""
    model = make_scene_model(scene_name="版本回退场景", version="2.0.0")
    scene_env.repository.rows[model.scene_id] = model  # type: ignore[attr-defined]
    resp = await client.post(
        f"{SCENE_BASE}/{model.scene_id}/publish",
        headers=ADMIN_HEADERS,
        json={"version": "1.9.9"},
    )
    assert resp.status_code == 422
    _assert_envelope(resp.json(), code=2001)


async def test_publish_scene_state_conflict_returns_3003(
    client: AsyncClient, scene_env: object
) -> None:
    """非 draft 发布 → 409 + code=3003。"""
    model = make_scene_model(scene_name="已发布场景-2", status=SceneStatus.PUBLISHED)
    scene_env.repository.rows[model.scene_id] = model  # type: ignore[attr-defined]
    resp = await client.post(f"{SCENE_BASE}/{model.scene_id}/publish", headers=ADMIN_HEADERS)
    assert resp.status_code == 409
    _assert_envelope(resp.json(), code=3003)


# =====================================================================
# GET /api/v1/scene/templates（listSceneTemplates）
# =====================================================================
async def test_list_templates_returns_builtin_catalog(client: AsyncClient, scene_env: object) -> None:
    """模板清单覆盖 4.2.1 分类体系（17 个叶子场景，实车回放无预置模板）。"""
    resp = await client.get(f"{SCENE_BASE}/templates", headers=ADMIN_HEADERS)
    assert resp.status_code == 200
    data = _assert_envelope(resp.json())["data"]
    assert data["total"] == 17  # type: ignore[index]
    types = {item["scene_type"] for item in data["items"]}  # type: ignore[index]
    assert "real_vehicle_replay" not in types
    first = data["items"][0]  # type: ignore[index]
    assert set(first["config"]) == {  # 4.2.2 结构完整
        "map",
        "ego_vehicle",
        "weather",
        "actors",
        "events",
        "success_criteria",
        "duration",
    }


async def test_list_templates_filters_by_category_and_type(
    client: AsyncClient, scene_env: object
) -> None:
    """category / scene_type 过滤（环境场景 4 个；前车切入 1 个）。"""
    env_resp = await client.get(
        f"{SCENE_BASE}/templates", headers=ADMIN_HEADERS, params={"category": "environment"}
    )
    assert env_resp.json()["data"]["total"] == 4

    cut_in_resp = await client.get(
        f"{SCENE_BASE}/templates", headers=ADMIN_HEADERS, params={"scene_type": "cut_in"}
    )
    cut_in_data = cut_in_resp.json()["data"]
    assert cut_in_data["total"] == 1
    assert cut_in_data["items"][0]["template_id"] == "tpl-cut_in"

    bad_resp = await client.get(
        f"{SCENE_BASE}/templates", headers=ADMIN_HEADERS, params={"category": "real_vehicle"}
    )
    assert bad_resp.status_code == 422  # 实车回放不在模板分类枚举中


# =====================================================================
# POST /api/v1/scene/export（exportScenes）
# =====================================================================
async def test_export_scene_writes_object_and_presigns(
    client: AsyncClient, scene_env: object
) -> None:
    """导出产物落 MinIO hunter-scene-assets，返回对象键与 15 分钟预签名地址。"""
    model = make_scene_model(scene_name="导出场景")
    scene_env.repository.rows[model.scene_id] = model  # type: ignore[attr-defined]
    resp = await client.post(
        f"{SCENE_BASE}/export",
        headers=ADMIN_HEADERS,
        json={"scene_ids": [str(model.scene_id)], "format": "carla_scenariorunner_xml"},
    )
    assert resp.status_code == 200
    data = _assert_envelope(resp.json())["data"]
    expected_name = f"scene-{model.scene_id}-{model.version}.xml"
    assert data["file_name"] == expected_name  # type: ignore[index]
    assert data["object_key"] == f"scenarios/{expected_name}"  # type: ignore[index]
    assert data["expires_in"] == 900  # type: ignore[index]
    assert len(data["sha256"]) == 64  # type: ignore[index]
    assert data["download_url"].startswith("https://minio.test/hunter-scene-assets/")  # type: ignore[index]
    content = scene_env.storage.objects[data["object_key"]]  # type: ignore[attr-defined]
    assert b"<scenarios>" in content and b"<scenario " in content


async def test_export_scene_openscenario_format(client: AsyncClient, scene_env: object) -> None:
    """OpenSCENARIO 1.2 产物写入 x-scenario-version（契约 x-hunter-export）。"""
    model = make_scene_model(scene_name="OSC 导出场景", version="1.4.0")
    scene_env.repository.rows[model.scene_id] = model  # type: ignore[attr-defined]
    resp = await client.post(
        f"{SCENE_BASE}/export",
        headers=ADMIN_HEADERS,
        json={"scene_ids": [str(model.scene_id)], "format": "openscenario_1_2"},
    )
    data = _assert_envelope(resp.json())["data"]
    assert data["file_name"].endswith(".xosc")  # type: ignore[index]
    content = scene_env.storage.objects[data["object_key"]].decode("utf-8")  # type: ignore[attr-defined]
    assert "<OpenSCENARIO" in content
    assert 'x-scenario-version="1.4.0"' in content


async def test_export_scene_missing_returns_3001(client: AsyncClient, scene_env: object) -> None:
    """待导出场景不存在 → 404 + code=3001。"""
    resp = await client.post(
        f"{SCENE_BASE}/export",
        headers=ADMIN_HEADERS,
        json={"scene_ids": [str(uuid4())], "format": "openscenario_1_2"},
    )
    assert resp.status_code == 404
    _assert_envelope(resp.json(), code=3001)


async def test_export_scene_rejects_empty_ids(client: AsyncClient, scene_env: object) -> None:
    """scene_ids 为空 → 422 + code=2001（契约 minItems 1）。"""
    resp = await client.post(
        f"{SCENE_BASE}/export",
        headers=ADMIN_HEADERS,
        json={"scene_ids": [], "format": "openscenario_1_2"},
    )
    assert resp.status_code == 422
    _assert_envelope(resp.json(), code=2001)


async def test_export_scene_minio_failure_returns_5001(
    client: AsyncClient, scene_env: object
) -> None:
    """MinIO 不可用 → 503 + code=5001。"""
    model = make_scene_model(scene_name="导出失败场景")
    scene_env.repository.rows[model.scene_id] = model  # type: ignore[attr-defined]
    scene_env.storage.fail_upload = True  # type: ignore[attr-defined]
    resp = await client.post(
        f"{SCENE_BASE}/export",
        headers=ADMIN_HEADERS,
        json={"scene_ids": [str(model.scene_id)], "format": "openscenario_1_2"},
    )
    assert resp.status_code == 503
    _assert_envelope(resp.json(), code=5001)


# =====================================================================
# POST /api/v1/scene/{scene_id}/run（runScene，4.4 节）
# =====================================================================
async def test_run_scene_publishes_instance_and_overrides(
    client: AsyncClient, scene_env: object
) -> None:
    """published 场景可下发：创建实例 + 下发配置 + 回显生效的参数覆盖。"""
    model = make_scene_model(scene_name="仿真场景", status=SceneStatus.PUBLISHED)
    scene_env.repository.rows[model.scene_id] = model  # type: ignore[attr-defined]
    resp = await client.post(
        f"{SCENE_BASE}/{model.scene_id}/run",
        headers=ADMIN_HEADERS,
        json={"param_overrides": {"ego_vehicle.initial_speed": 2.0, "weather.rain": 60}},
    )
    assert resp.status_code == 200
    data = _assert_envelope(resp.json())["data"]
    assert data["sim_instance_id"] == "sim-0001"  # type: ignore[index]
    assert data["status"] == "running"  # type: ignore[index]
    assert data["param_overrides"] == {  # type: ignore[index]
        "ego_vehicle.initial_speed": 2.0,
        "weather.rain": 60,
    }
    carla = scene_env.carla  # type: ignore[attr-defined]
    assert len(carla.created) == 1
    assert carla.created[0]["scene_config"]["ego_vehicle"]["initial_speed"] == 2.0
    instance_id, submitted = carla.submitted[0]
    assert instance_id == "sim-0001"
    assert submitted["scene_config"]["weather"]["rain"] == 60


async def test_run_scene_requires_published_state(client: AsyncClient, scene_env: object) -> None:
    """draft 场景下发 → 409 + code=3003（仅 published 可下发）。"""
    model = make_scene_model(scene_name="草稿场景")
    scene_env.repository.rows[model.scene_id] = model  # type: ignore[attr-defined]
    resp = await client.post(f"{SCENE_BASE}/{model.scene_id}/run", headers=ADMIN_HEADERS)
    assert resp.status_code == 409
    _assert_envelope(resp.json(), code=3003)


async def test_run_scene_rejects_forbidden_override_path(
    client: AsyncClient, scene_env: object
) -> None:
    """禁止覆盖 map.map_id → 422 + code=2001（契约白名单）。"""
    model = make_scene_model(scene_name="越权覆盖场景", status=SceneStatus.PUBLISHED)
    scene_env.repository.rows[model.scene_id] = model  # type: ignore[attr-defined]
    resp = await client.post(
        f"{SCENE_BASE}/{model.scene_id}/run",
        headers=ADMIN_HEADERS,
        json={"param_overrides": {"map.map_id": "Town02"}},
    )
    assert resp.status_code == 422
    _assert_envelope(resp.json(), code=2001)


async def test_run_scene_rejects_non_whitelisted_override_path(
    client: AsyncClient, scene_env: object
) -> None:
    """白名单外路径（含未定义字段）→ 422 + code=2001。"""
    model = make_scene_model(scene_name="非白名单覆盖场景", status=SceneStatus.PUBLISHED)
    scene_env.repository.rows[model.scene_id] = model  # type: ignore[attr-defined]
    resp = await client.post(
        f"{SCENE_BASE}/{model.scene_id}/run",
        headers=ADMIN_HEADERS,
        json={"param_overrides": {"unexpected.field": 1}},
    )
    assert resp.status_code == 422
    _assert_envelope(resp.json(), code=2001)


async def test_run_scene_carla_unavailable_returns_5001(
    client: AsyncClient, scene_env: object
) -> None:
    """Carla 管理 API 不可达 → 503 + code=5001（4.4 节第 2 步）。"""
    model = make_scene_model(scene_name="Carla 不可达场景", status=SceneStatus.PUBLISHED)
    scene_env.repository.rows[model.scene_id] = model  # type: ignore[attr-defined]
    scene_env.carla.unavailable = True  # type: ignore[attr-defined]
    resp = await client.post(f"{SCENE_BASE}/{model.scene_id}/run", headers=ADMIN_HEADERS)
    assert resp.status_code == 503
    _assert_envelope(resp.json(), code=5001)


async def test_run_scene_reuses_instance_when_parallel_allowed(
    client: AsyncClient, scene_env: object
) -> None:
    """已有进行中实例且 allow_parallel=true → 复用实例，不重复创建。"""
    model = make_scene_model(scene_name="并行仿真场景", status=SceneStatus.PUBLISHED)
    scene_env.repository.rows[model.scene_id] = model  # type: ignore[attr-defined]
    from app.repositories.carla import SimInstance

    scene_env.carla.active[str(model.scene_id)] = SimInstance(  # type: ignore[attr-defined]
        instance_id="sim-existing", status="running"
    )
    conflict = await client.post(f"{SCENE_BASE}/{model.scene_id}/run", headers=ADMIN_HEADERS)
    assert conflict.status_code == 409
    _assert_envelope(conflict.json(), code=3003)

    reused = await client.post(
        f"{SCENE_BASE}/{model.scene_id}/run",
        headers=ADMIN_HEADERS,
        json={"sim_config": {"allow_parallel": True}},
    )
    assert reused.status_code == 200
    assert reused.json()["data"]["sim_instance_id"] == "sim-existing"
    assert scene_env.carla.created == []  # type: ignore[attr-defined]


# =====================================================================
# 未知路径（统一响应体，不得裸 404 文本）
# =====================================================================
async def test_unknown_scene_path_returns_unified_error(client: AsyncClient, scene_env: object) -> None:
    """契约外路径 → 404 + 统一响应体（code=3001/2001）。"""
    resp = await client.get(f"{SCENE_BASE}/unknown/action", headers=ADMIN_HEADERS)
    assert resp.status_code == 404
    body = _assert_envelope(resp.json(), code=resp.json()["code"])
    assert body["code"] in {3001, 2001}