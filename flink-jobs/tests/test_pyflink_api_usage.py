"""三个作业 ``main()`` 的 PyFlink API 用法静态校验（V1.19.7，仓库 CI 无需 PyFlink）。

背景（服务器实录）：Flink 1.18 → 2.1 升级后三个作业提交齐报
``AttributeError: type object 'KafkaOffsetsInitializer' has no attribute 'group_offsets'``——
``main()`` 标 ``pragma: no cover``，API 名字从未被任何单测执行过，改名只能靠实机往返暴露。

两道防线分工（名字集合的单一事实来源是 ``hunter_flink.api_guard``）：

- 本测试（无 pyflink）：作业源码里出现的 pyflink 模块 / 类 / 成员名必须全在白名单内，
  且禁用语（1.x 旧名、从未存在的名字）不出现；
- 镜像内 ``python -m hunter_flink.api_guard``（flink-jobs/Dockerfile 第四道自检）：白名单必须
  与已安装 apache-flink 的实际 API 面一致。

两侧同时成立 ⇒ 作业用到的每个 PyFlink 名字都被验证过。
"""
from __future__ import annotations

import ast
import importlib
from pathlib import Path

import pytest
from hunter_flink import api_guard

JOBS_DIR = Path(__file__).resolve().parents[1] / "hunter_flink"
JOB_FILES = ("detection_job.py", "algorithm_performance_job.py", "ingest_latency_job.py")


def _main_node(path: Path) -> ast.FunctionDef:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "main":
            return node
    raise AssertionError(f"{path.name} 缺少 main() 提交入口")


def _pyflink_imports(main: ast.FunctionDef) -> dict[str, tuple[str, str]]:
    """``main()`` 内 pyflink 延迟导入：局部名 → (模块路径, 类/属性名)。"""
    imported: dict[str, tuple[str, str]] = {}
    for node in ast.walk(main):
        if not isinstance(node, ast.ImportFrom) or not (node.module or "").startswith("pyflink"):
            continue
        module = node.module
        assert module in api_guard.API_WHITELIST, f"{module} 未纳入 API 白名单（先补 api_guard）"
        classes = api_guard.API_WHITELIST[module]
        for alias in node.names:
            assert alias.name in classes, f"{module} 的成员 {alias.name} 未纳入 API 白名单"
            imported[alias.asname or alias.name] = (module, alias.name)
    return imported


def _chain(node: ast.AST) -> tuple[str | None, tuple[str, ...]]:
    """沿 ``Attribute``/``Call`` 链下钻到根 ``Name``，返回 (根名, 链上的属性名)。"""
    attrs: list[str] = []
    cur: ast.AST = node
    while True:
        if isinstance(cur, ast.Call):
            cur = cur.func
        elif isinstance(cur, ast.Attribute):
            attrs.append(cur.attr)
            cur = cur.value
        elif isinstance(cur, ast.Name):
            return cur.id, tuple(attrs)
        else:
            return None, ()


@pytest.mark.parametrize("job_file", JOB_FILES)
def test_job_uses_only_whitelisted_pyflink_members(job_file: str) -> None:
    main = _main_node(JOBS_DIR / job_file)
    roots = _pyflink_imports(main)
    assert roots, f"{job_file} 的 main() 未延迟导入任何 pyflink 名字"

    for node in ast.walk(main):
        if not isinstance(node, ast.Attribute):
            continue
        root, attrs = _chain(node)
        if root not in roots:
            continue  # 非 pyflink 根（env / ds / json / self 等）：链上名字由镜像内自检覆盖
        module, cls = roots[root]
        allowed = api_guard.allowed_members(module, cls)
        for attr in attrs:
            assert attr in allowed, (
                f"{job_file}: {root}.{attr} 不在 {module}.{cls}（含 fluent 后继类）的白名单成员 "
                f"{sorted(allowed)} 内"
            )


@pytest.mark.parametrize("job_file", JOB_FILES)
def test_job_avoids_removed_or_invented_pyflink_names(job_file: str) -> None:
    """1.x 旧名与「凭印象写」的名字在源码里出现即为回归（不依赖白名单是否恰好收录）。"""
    main = _main_node(JOBS_DIR / job_file)
    used = {
        node.attr if isinstance(node, ast.Attribute) else node.id
        for node in ast.walk(main)
        if isinstance(node, (ast.Attribute, ast.Name))
    }
    assert not used & api_guard.FORBIDDEN_IN_JOBS, (
        f"{job_file} 使用了禁用的 PyFlink 名字："
        f"{sorted(used & api_guard.FORBIDDEN_IN_JOBS)}（见 api_guard.FORBIDDEN_IN_JOBS 注释）"
    )


def test_three_jobs_share_the_same_source_sink_shape() -> None:
    """三个作业必须走同一套已验证入口：from_source + sink_to（或 print 收口）。"""
    for job_file in JOB_FILES:
        main = _main_node(JOBS_DIR / job_file)
        text = ast.dump(main)
        assert "from_source" in text, f"{job_file} 未用 from_source（FLIP-27 Source 唯一入口）"
        assert "set_value_only_deserializer" in text, f"{job_file} 缺少值反序列化 schema"
        assert "set_starting_offsets" in text, f"{job_file} 未显式声明起始位点策略"


def test_api_guard_whitelist_is_self_consistent() -> None:
    """白名单自身可导航：每个点路径都有宿主条目、成员不重复、2.x 已删名字都指向白名单内的类。"""
    assert api_guard.API_WHITELIST, "白名单不得为空"
    for module, classes in api_guard.API_WHITELIST.items():
        assert module.startswith("pyflink"), module
        assert classes, module
        for dotted, members in classes.items():
            top = dotted.split(".")[0]
            assert top in classes or dotted in classes, f"{module}.{dotted} 缺宿主条目"
            assert len(set(members)) == len(members), f"{module}.{dotted} 成员重复"
    for module, dotted, member in api_guard.REMOVED_IN_FLINK_21:
        assert dotted in api_guard.API_WHITELIST[module], f"{module}.{dotted} 未列入白名单"
        assert member in api_guard.FORBIDDEN_IN_JOBS, f"{module}.{dotted}.{member} 应同时禁用于作业"


def test_fluent_successors_point_at_whitelisted_classes() -> None:
    """链式展开表的后继类必须已列入白名单（否则 allowed_members 会静默少算成员）。"""
    for key, successors in api_guard.FLUENT_SUCCESSORS.items():
        assert key[1] in api_guard.API_WHITELIST[key[0]], f"链根 {key} 未列入白名单"
        for succ in successors:
            assert succ[1] in api_guard.API_WHITELIST[succ[0]], f"后继类 {succ} 未列入白名单"


def test_api_guard_check_passes_when_pyflink_installed() -> None:
    """装了 pyflink 的环境（作业镜像内）才跑得动；CI 无 pyflink 时跳过，由第四道自检兜底。"""
    pytest.importorskip("pyflink", reason="apache-flink 仅在作业镜像内提供")
    assert api_guard.check() == []


def test_api_guard_is_executable_as_module() -> None:
    """`python -m hunter_flink.api_guard` 必须真正执行 :func:`main`。

    缺 ``if __name__ == "__main__"`` 时 `-m` 只导入模块就退出（返回码 0），Dockerfile
    第四道自检会**静默变为空跑** —— 本用例把“自检存在”本身锁死。
    """
    tree = ast.parse((JOBS_DIR / "api_guard.py").read_text(encoding="utf-8"))
    entries = [
        node
        for node in tree.body
        if isinstance(node, ast.If)
        and isinstance(node.test, ast.Compare)
        and "__main__" in ast.dump(node.test)
    ]
    assert entries, "api_guard.py 缺模块入口（`-m` 执行时不会跑 check）"
    assert any("main" in ast.dump(node) for node in entries), "入口未调用 main()"
    # 镜像内的调用点也必须存在（把 RUN 行改成注释等于拆掉门禁）
    dockerfile = (JOBS_DIR.parent / "Dockerfile").read_text(encoding="utf-8")
    assert "RUN python -m hunter_flink.api_guard" in dockerfile, (
        "flink-jobs/Dockerfile 缺第四道自检 RUN"
    )


def test_api_guard_does_not_pass_silently_without_pyflink() -> None:
    """无 pyflink 的环境下 :func:`check` 必须报告问题（不能返回空列表而让人误判为通过）。"""
    try:
        importlib.import_module("pyflink.datastream.connectors.kafka")
    except ImportError:
        assert api_guard.check(), "未装 pyflink 时 check() 应报 import 失败"
    else:  # pragma: no cover - 装了 pyflink 时由上面那条覆盖
        pytest.skip("本环境已装 apache-flink，由 check_passes 用例覆盖")
