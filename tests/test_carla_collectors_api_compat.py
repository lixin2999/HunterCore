"""CARLA 采集脚本对不同 **PythonAPI 形态** 的兼容性用例（V1.24.11）。

为什么需要这一组用例
--------------------
`scripts/collect_carla_vehicle_geometry.py` 历来只能人工实机验证，结果连续两轮在部署机首发：

1. V1.24.10 —— 脚本用了 3.11+ 的 ``datetime.UTC``，系统 Python 3.10 崩在 import 行；
2. V1.24.11 —— 脚本写死 ``client.set_client_timeout()``，而部署机上的 carla **没有这个属性**
   （实机读数：现场 wheel ``get_client_version()`` = 0.9.16，超时接口只有 ``set_timeout``），
   实机崩在 ``AttributeError: 'Client' object has no attribute 'set_client_timeout'``。

接口的存在性在不同构建之间并不一致（官方 wheel 与 自行编译的 wheel 可各带一个名字），因此
脚本不预设版本、两个名字都试。本用例用 **carla 替身模块**直接跑脚本的 ``main()``，覆盖
三种形态（新名 / 旧名 / 两者皆无），并要求「必需接口缺失」时给出 ``[error]`` 可读结论而
**不是裸堆栈**。不需要真实 CARLA，也不需要装 carla。

V1.25.0 说明：出生点采集脚本（``collect_carla_spawn_points.py``）已移除——平台不再维护
每个地图的可用出生点，改由仿真端按下发场景自行取有效出生坐标。
"""

from __future__ import annotations

import importlib.util
import sys
import types
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[1]
GEOMETRY_SCRIPT = ROOT / "scripts" / "collect_carla_vehicle_geometry.py"

#: 故意取一个**旧**版本号：若脚本哪天又按版本推断接口（而不实探属性），本组用例必须失败
SERVER_VERSION = "0.9.9"


class _V3:
    def __init__(self, x: float = 0.0, y: float = 0.0, z: float = 0.0) -> None:
        self.x, self.y, self.z = float(x), float(y), float(z)


def _base_cfg(**overrides: Any) -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "server_version": SERVER_VERSION,
        "client_version_str": SERVER_VERSION,
        "client_version_api": True,
        "timeouts": ("set_client_timeout",),
        "reject_float": False,
        "timeout_calls": [],
        "get_world_api": True,
        "blueprint_library_api": True,
        "library_filter_api": True,
        "dimension_api": True,
        "bounding_box_api": True,
    }
    cfg.update(overrides)
    return cfg


def _make_carla_module(cfg: dict[str, Any]) -> types.ModuleType:
    """按 cfg 决定「哪些接口存在」，组装出与真实 wheel 同形的 carla 模块。"""

    def _setter(name: str) -> Any:
        def setter(self: Any, value: Any) -> None:
            if cfg["reject_float"] and isinstance(value, float):
                raise TypeError("timeout must be a timedelta")
            cfg["timeout_calls"].append((name, value))

        return setter

    def blueprint_obj(index: int) -> Any:
        # 挂在实例（SimpleNamespace）上的函数不会自动绑定，故一律零参 lambda；
        # 只有 Client 走 type() 构造，其方法才需要 self。
        ns: dict[str, Any] = {
            "id": f"vehicle.hunter_se.{index}",
            "display_name": "Hunter SE",
        }
        if cfg["dimension_api"]:
            ns["get_dimension"] = lambda: _V3(4.72, 1.93, 1.86)
        if cfg["bounding_box_api"]:
            ns["get_bounding_box"] = lambda: types.SimpleNamespace(
                location=_V3(0.0, 0.0, 0.93), extent=_V3(2.36, 0.965, 0.93)
            )
        return types.SimpleNamespace(**ns)

    def library_obj() -> Any:
        if not cfg["library_filter_api"]:
            return types.SimpleNamespace()
        return types.SimpleNamespace(filter=lambda pattern: [blueprint_obj(1), blueprint_obj(2)])

    def world_obj() -> Any:
        ns: dict[str, Any] = {}
        if cfg["blueprint_library_api"]:
            ns["get_blueprint_library"] = lambda: library_obj()
        return types.SimpleNamespace(**ns)

    client_ns: dict[str, Any] = {
        "get_server_version": lambda self: cfg["server_version"],
        "__init__": lambda self, host, port: None,
    }
    for name in cfg["timeouts"]:
        client_ns[name] = _setter(name)
    if cfg["client_version_api"]:
        client_ns["get_client_version"] = lambda self: cfg["client_version_str"]
    if cfg["get_world_api"]:
        client_ns["get_world"] = lambda self: world_obj()

    module = types.ModuleType("carla")
    module.Client = type("Client", (), client_ns)
    return module


def _run(
    script: Path,
    module_name: str,
    cfg: dict[str, Any],
    argv: list[str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> tuple[int, str, str]:
    """跑脚本 main()。未捕获异常会直接让用例报错——正是「不得留裸堆栈」的判据。"""
    monkeypatch.setitem(sys.modules, "carla", _make_carla_module(cfg))
    spec = importlib.util.spec_from_file_location(module_name, script)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    code = int(module.main(argv))
    captured = capsys.readouterr()
    return code, captured.out, captured.err


# --- 几何表脚本 ------------------------------------------------------------------
#
# V1.25.0：出生点采集脚本已删除（平台不再维护地图可用点），本文件只覆盖几何表采集。


def _geom_argv(tmp_path: Path) -> list[str]:
    return [
        "--host",
        "127.0.0.1",
        "--blueprints",
        "vehicle.*",
        "--model-map",
        "HUNTER_SE=vehicle.hunter_se.1",
        "--out",
        str(tmp_path / "geom.json"),
        "--print",
    ]


def test_geometry_old_api_exports_bounding_box(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    cfg = _base_cfg(timeouts=("set_timeout",))
    code, out, err = _run(GEOMETRY_SCRIPT, "hunter_geom_old", cfg, _geom_argv(tmp_path), monkeypatch, capsys)
    assert code == 0, out + err
    assert "超时接口 set_timeout" in out
    assert '"calibrated": true' in out
    assert '"extent"' in out and '"length_m": 4.72' in out


def test_geometry_deployment_wheel_form_0_9_16_set_timeout_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """部署机实机形态（V1.24.11 回报）：wheel 自称 0.9.16、超时接口只有 ``set_timeout``、
    服务器版本号为 git 提交号。这类构建必须能跑完而不是 AttributeError。"""
    cfg = _base_cfg(
        client_version_str="0.9.16",
        server_version="35e2d5a",
        timeouts=("set_timeout",),
    )
    code, out, err = _run(GEOMETRY_SCRIPT, "hunter_geom_deploy", cfg, _geom_argv(tmp_path), monkeypatch, capsys)
    assert code == 0, out + err
    assert "AttributeError" not in err
    assert cfg["timeout_calls"][0][0] == "set_timeout"


def test_geometry_setter_accepts_timedelta_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """部分构建只收 timedelta：float 抛 TypeError 后必须换类型重试而不是崩掉。"""
    cfg = _base_cfg(timeouts=("set_client_timeout",), reject_float=True)
    code, out, err = _run(GEOMETRY_SCRIPT, "hunter_geom_td", cfg, _geom_argv(tmp_path), monkeypatch, capsys)
    assert code == 0, out + err
    used, value = cfg["timeout_calls"][0]
    assert used == "set_client_timeout"
    assert isinstance(value, timedelta)


def test_geometry_missing_bounding_box_refuses_to_fabricate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """老 wheel 无 ``get_bounding_box`` 时必须拒绝出表，**不得**用 dimension/2 推算值冒充实机包围盒。"""
    cfg = _base_cfg(bounding_box_api=False)
    code, _out, err = _run(GEOMETRY_SCRIPT, "hunter_geom_nobox", cfg, _geom_argv(tmp_path), monkeypatch, capsys)
    assert code == 2
    assert "get_bounding_box" in err
    assert "不会用 dimension/2 推算值冒充" in err
    assert "Traceback" not in err


def test_geometry_world_without_library_api_is_readable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    cfg = _base_cfg(blueprint_library_api=False)
    code, _out, err = _run(GEOMETRY_SCRIPT, "hunter_geom_nolib", cfg, _geom_argv(tmp_path), monkeypatch, capsys)
    assert code == 2
    assert "[error]" in err and "get_blueprint_library" in err
    assert "World" in err


def test_shipped_scripts_do_not_hardcode_the_new_timeout_name() -> None:
    """源码级钉桩：采集脚本不得直接调用 ``client.set_client_timeout(...)``，必须走自适应超时。"""
    source = GEOMETRY_SCRIPT.read_text(encoding="utf-8")
    assert "client.set_client_timeout(" not in source, "几何表采集脚本又写死了新版接口名"
    assert "_apply_client_timeout(client" in source, "几何表采集脚本未走版本自适应超时"


def test_spawn_point_collector_is_gone() -> None:
    """V1.25.0：平台不再维护地图可用出生点，采集脚本与 provider 必须真的不在了。"""
    assert not (ROOT / "scripts" / "collect_carla_spawn_points.py").exists()
    assert not (ROOT / "services" / "scene-service" / "app" / "scene_engine" / "spawn_points.py").exists()
    assert not (ROOT / "services" / "scene-service" / "config" / "carla_spawn_points.json").exists()
