# HunterEdge L5 测试报告（{{VERSION}}）

> 本文件由 `tests/support/report.py` 依据模板自动生成，禁止手工修改结论字段。
> 模板路径：`tests/report-template.md`；生成命令：`python -m tests.support.report`。

## 一、运行环境

| 项 | 值 |
| --- | --- |
| 版本 | {{VERSION}} |
| 执行时间 | {{DATE}} |
| 环境 | {{ENVIRONMENT}} |
| Python | {{PYTHON}} |
| 平台 | {{PLATFORM}} |

## 二、套件汇总

{{SUMMARY_TABLE}}

> 划分依据：`unit`（无外部依赖）/ `integration`（容器化中间件）/ `e2e`（11.1 数据采集 → 11.2 OTA 灰度 → 11.3 远程操控）/ `performance`（第 10 条性能指标）。
> 无 Docker 环境时 `integration`/`e2e(容器型)` 用例会 skip，并在明细表中记录跳过，CI 需在具备 Docker 的 runner 上执行完整套件。

## 三、用例明细

{{CASE_TABLE}}

## 四、性能指标对照（系统关键约束 第 10 条）

{{PERF_TABLE}}

### 阈值来源登记

{{PERF_THRESHOLD_SOURCES}}

## 五、验收结论

**结论：{{CONCLUSION}}**

| 判定项 | 规则 |
| --- | --- |
| 功能用例 | 不允许存在 `fail`（`skip` 需在下方"风险与豁免"中逐条说明） |
| 性能指标 | 任一指标未达标即整体不通过（阈值不可放宽） |
| 契约一致性 | 契约文件为单一事实来源，用例不得内联魔法数字/字段名 |

## 六、风险与豁免

| 编号 | 风险/豁免项 | 影响 | 处理决定 | 责任人 |
| --- | --- | --- | --- | --- |
| R-01 | `tests/support/flow.py` 中的 11.1/11.2/11.3 参考实现基于《系统关键约束》+ `contracts/` 推导，仓库内暂无对应详细设计文档 | 业务流程断言可能与文档最终口径存在差异 | 待详细设计文档补齐后回归核对；本条需人工确认 | 待指派 |

## 七、附件

| 附件 | 路径 |
| --- | --- |
| JSON 结果 | `docs/test-reports/l5-results.json` |
| 服务启动日志 | `.pytest_cache/service-logs/*.log` |
| 测试层自检脚本 | `scripts/verify_test_layer.py` |
