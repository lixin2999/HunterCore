"""服务层单元测试：模板清单 / 导出序列化 / 参数覆盖白名单 / 版本语义。

不依赖 PostgreSQL/Redis/MinIO/Carla（以 app/tests/fakes.py 替身驱动）。
"""
from __future__ import annotations

import hashlib
from uuid import UUID

import pytest
from hunter_common.database.enums import SceneStatus
from hunter_common.exceptions import InvalidParameterError, ResourceStateConflictError

from app.config import settings
from app.repositories.storage import endpoint_url_for
from app.schemas.scene import (
    SCENE_CATEGORY_BY_TYPE,
    SCENE_TYPES_BY_CATEGORY,
    SceneConfig,
    SceneExportFormat,
    SceneExportRequest,
    SceneType,
    SceneUpdateRequest,
)
from app.services.export import SceneExportService
from app.services.scenes import SceneService, parse_version
from app.services.serializers import file_name_for, render_export_document
from app.services.simulation import apply_param_overrides
from app.services.templates import SceneTemplateService
from app.tests.fakes import (
    FakeSceneCache,
    InMemorySceneRepository,
    InMemorySceneStorage,
    make_scene_model,
)


# =====================================================================
# 场景分类体系 ↔ 模板清单（4.2.1 节）
# =====================================================================
def test_category_mapping_covers_all_scene_types() -> None:
    """分类 → 类型映射并集必须等于 SceneType 全集（4.2.1 + 4.5 实车回放）。"""
    grouped = {scene_type for types in SCENE_TYPES_BY_CATEGORY.values() for scene_type in types}
    assert grouped == set(SceneType)
    assert len(SCENE_TYPES_BY_CATEGORY) == 6  # 6 个分类
    assert len(SCENE_CATEGORY_BY_TYPE) == len(set(SceneType))


def test_template_catalog_covers_classification() -> None:
    """模板清单 = 全部叶子场景 - 实车回放（4.5 节无预置模板）。"""
    service = SceneTemplateService(settings)
    data = service.list_templates(category=None, scene_type=None)
    assert data.total == len(set(SceneType)) - 1 == 17
    assert SceneType.REAL_VEHICLE_REPLAY not in {item.scene_type for item in data.items}
    for scene_type in (SceneType.CUT_IN, SceneType.RAINY, SceneType.SENSOR_FAILURE):
        template = service.get_template(scene_type)
        assert template is not None
        assert template.template_id == f"tpl-{scene_type.value}"
    assert service.get_template(SceneType.REAL_VEHICLE_REPLAY) is None


def test_template_configs_are_4_2_2_complete() -> None:
    """每个模板 config 必须是完整 4.2.2 结构（7 字段 + 天气 7 字段）。"""
    service = SceneTemplateService(settings)
    for item in service.list_templates(category=None, scene_type=None).items:
        assert set(item.config.model_dump()) == {
            "map",
            "ego_vehicle",
            "weather",
            "actors",
            "events",
            "success_criteria",
            "duration",
        }
        assert set(item.config.weather.model_dump()) == {
            "cloudiness",
            "rain",
            "wetness",
            "fog",
            "wind",
            "sun_azimuth",
            "sun_altitude",
        }


# =====================================================================
# 派生语义化版本（发布版本门禁）
# =====================================================================
def test_parse_version_semantics() -> None:
    """语义化版本解析与比较（发布版本需 ≥ 当前版本）。"""
    assert parse_version("1.2.3") == (1, 2, 3)
    assert parse_version("1.10.0") > parse_version("1.9.0")
    with pytest.raises(ValueError):
        parse_version("1.2")


# =====================================================================
# 参数覆盖白名单（契约 SceneRunRequest）
# =====================================================================
def _config() -> SceneConfig:
    """构造 4.2.2 结构 config（用于覆盖测试）。"""
    return SceneConfig.model_validate(make_scene_model().config_json)


def test_apply_param_overrides_merges_whitelisted_paths() -> None:
    """白名单路径深合并（嵌套对象 + 顶层标量）。"""
    merged, applied = apply_param_overrides(
        _config(),
        {"ego_vehicle.initial_speed": 2.0, "weather.fog": 30.0, "duration": 45.0},
        settings,
    )
    assert merged.ego_vehicle.initial_speed == 2.0
    assert merged.weather.fog == 30.0
    assert merged.duration == 45.0
    assert applied == {
        "ego_vehicle.initial_speed": 2.0,
        "weather.fog": 30.0,
        "duration": 45.0,
    }


def test_apply_param_overrides_rejects_forbidden_and_unknown_paths() -> None:
    """禁止路径 map.* 与白名单外路径一律 2001。"""
    config = _config()
    with pytest.raises(InvalidParameterError):
        apply_param_overrides(config, {"map.map_id": "Town02"}, settings)
    with pytest.raises(InvalidParameterError):
        apply_param_overrides(config, {"unknown.path": 1}, settings)
    with pytest.raises(InvalidParameterError):
        apply_param_overrides(config, {"actors.9.spawn_point.x": 1.0}, settings)


def test_apply_param_overrides_rejects_invalid_value() -> None:
    """覆盖后结构非法（取值越界）→ 2001，不产生半成品配置。"""
    with pytest.raises(InvalidParameterError):
        apply_param_overrides(_config(), {"ego_vehicle.initial_speed": -1.0}, settings)


# =====================================================================
# 导出序列化（4.3 节）
# =====================================================================
def _scene_service() -> SceneService:
    """构造带内存替身的场景服务。"""
    return SceneService(InMemorySceneRepository(), FakeSceneCache(), settings)


def test_render_export_document_is_deterministic() -> None:
    """同一场景两次渲染字节完全一致（可复现导出，供 SHA-256 校验）。"""
    service = _scene_service()
    scene = service.to_schema(make_scene_model(scene_name="导出确定性"))
    first = render_export_document([scene], SceneExportFormat.CARLA_SCENARIORUNNER_XML)
    second = render_export_document([scene], SceneExportFormat.CARLA_SCENARIORUNNER_XML)
    assert first == second
    assert hashlib.sha256(first).hexdigest() == hashlib.sha256(second).hexdigest()
    text = first.decode("utf-8")
    for token in ("<scenarios>", "<scenario ", "<map ", "<ego_vehicle ", "<weather ", "<duration>"):
        assert token in text


def test_render_export_document_batch_container() -> None:
    """批量导出（>1 场景）在 OSC 中落地为单文件集容器（契约待确认 #6）。"""
    service = _scene_service()
    scenes = [
        service.to_schema(make_scene_model(scene_name="批量-1")),
        service.to_schema(make_scene_model(scene_name="批量-2", scene_type="rainy")),
    ]
    text = render_export_document(scenes, SceneExportFormat.OPENSCENARIO_1_2).decode("utf-8")
    assert "<x-hunter-scenario-set" in text
    assert text.count("<OpenSCENARIO ") == 2
    assert text.count("</OpenSCENARIO>") == 2


def test_export_file_name_follows_contract_naming() -> None:
    """文件名遵循契约命名 scene-{scene_id}-{version}.{ext}。"""
    service = _scene_service()
    scene = service.to_schema(make_scene_model(version="2.0.0"))
    assert file_name_for(scene, SceneExportFormat.OPENSCENARIO_1_2) == (
        f"scene-{scene.scene_id}-2.0.0.xosc"
    )


def test_minio_endpoint_url_normalization() -> None:
    """MinIO 端点规范化：共享配置的 host:port 形式必须补 scheme（boto3 要求）。"""
    assert endpoint_url_for("localhost:9000", secure=False) == "http://localhost:9000"
    assert endpoint_url_for("minio.example.com:9000", secure=True) == "https://minio.example.com:9000"
    assert endpoint_url_for("https://minio.example.com", secure=False) == "https://minio.example.com"


async def test_export_service_rejects_too_many_scenes(monkeypatch: pytest.MonkeyPatch) -> None:
    """导出场景数超过配置上限 → 2001（契约 maxItems 50）。"""
    monkeypatch.setattr(settings, "scene_export_max_scenes", 1)
    export_service = SceneExportService(_scene_service(), InMemorySceneStorage(), settings)
    request = SceneExportRequest.model_validate(
        {
            "scene_ids": [
                "11111111-1111-4111-8111-111111111111",
                "22222222-2222-4222-8222-222222222222",
            ],
            "format": "openscenario_1_2",
        }
    )
    with pytest.raises(InvalidParameterError):
        await export_service.export_scenes(request)


# =====================================================================
# 状态机（4.2 节 lifecycle）：仅 draft 可编辑 / 写操作后缓存失效
# =====================================================================
async def test_service_rejects_update_on_published_scene() -> None:
    """published 场景不可编辑 → 3003。"""
    repository = InMemorySceneRepository()
    service = SceneService(repository, FakeSceneCache(), settings)
    model = make_scene_model(scene_name="状态机场景", status=SceneStatus.PUBLISHED)
    repository.rows[model.scene_id] = model
    payload = SceneUpdateRequest.model_validate(
        {"scene_name": "改名", "scene_type": "custom", "tags": [], "config": model.config_json}
    )
    with pytest.raises(ResourceStateConflictError):
        await service.update_scene(model.scene_id, payload, user_id=str(model.creator))


async def test_service_invalidates_cache_after_publish_and_delete() -> None:
    """写操作（发布 / 删除）后缓存必须失效（Redis Key 契约）。"""
    repository = InMemorySceneRepository()
    cache = FakeSceneCache()
    service = SceneService(repository, cache, settings)
    draft = make_scene_model(scene_name="缓存失效-发布")
    archivable = make_scene_model(scene_name="缓存失效-删除", status=SceneStatus.ARCHIVED)
    repository.rows[draft.scene_id] = draft
    repository.rows[archivable.scene_id] = archivable
    cache.store[str(draft.scene_id)] = {"stale": True}
    cache.store[str(archivable.scene_id)] = {"stale": True}

    await service.publish_scene(draft.scene_id, None)
    await service.delete_scene(archivable.scene_id)
    assert str(draft.scene_id) in cache.invalidated
    assert str(archivable.scene_id) in cache.invalidated
    assert cache.store == {}


async def test_service_data_scope_forces_creator_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    """开启数据权限隔离后，非豁免角色只能查询自有场景（creator 由服务端强制注入）。"""
    monkeypatch.setattr(settings, "scene_scope_by_creator", True)
    repository = InMemorySceneRepository()
    service = SceneService(repository, FakeSceneCache(), settings)
    owned = make_scene_model(scene_name="我的场景")
    other = make_scene_model(
        scene_name="他人场景", creator=UUID("99999999-9999-4999-8999-999999999999")
    )
    repository.rows[owned.scene_id] = owned
    repository.rows[other.scene_id] = other

    data = await service.list_scenes(
        user_id=str(owned.creator),
        roles=frozenset({"operator"}),
        page=1,
        page_size=20,
        scene_type=None,
        status=None,
        tags=None,
        keyword=None,
        creator=None,
        sort="create_time",
        order="desc",
    )
    assert data.total == 1
    assert data.items[0].scene_name == "我的场景"