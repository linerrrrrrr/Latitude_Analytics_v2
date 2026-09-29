# b01 日历结果页验收

这是针对“看不出进度、应直接展示维度表”的本轮实现记录。正式使用边界见 [operations README](../02_Futures_Lakehouse/operations/README.md#a01-的四个日历看板)。

- b01 改为结果优先的“日历结果”页：明确的结束状态、生成/待写入/提交数量，以及直接可读的日历行。`up_to_date` 显示已是最新、0 行新增/修订、覆盖水位及日志明确提供的 API 请求数。
- 区分已落盘日历与本批生成结果，默认常用六列，可展开全部 11 个契约字段；按年月和交易日状态筛选，每页 100 行。表名、分区、主键和字段来自权威 `TRADE_CALENDAR_SCHEMA`。
- 只读加载最多 50,000 行的快照，超限报错；查询在独立线程中进行，状态/日志读取不受其阻塞。监控刷新不重复扫描，旧异步结果不会覆盖新的目标湖。
- b01 的 Notebook 是唯一编辑源，增加已验收生成表的批次预览保存；默认 PythonExporter 完整导出同名 `.py`，字节一致，Notebook/Python 均使用 LF。标准独立运行默认不保存预览。worker 仅给 b01 子进程指定本批预览路径；不向其他阶段泄漏。预览写入失败只 warning，不改变业务采集/事务结果。
- 已压缩 b01 重复标题区，保留原文简介。日志独立页签；亮/暗主题和 1100×820 窗口完成视觉检查，小窗口使用滚动确保控件不重叠。预览全部使用明确标注的临时模拟日历。

标准解释器 `E:\anaconda3\envs\latitude\python.exe` 下，97 项相关检查通过：

| 测试 | 数量 |
|---|---:|
| `test_b01_result_table.py` | 10 |
| `test_b01_c01_c02_daily_tail_modes.py` | 11 |
| operations `test_console.py` | 30 |
| `test_a01_calendar_dashboards.py` | 10 |
| `test_operations_progress_events.py` | 10 |
| `test_a00_workbench.py` | 8 |
| `test_operations_single_and_tools.py` | 18 |

测试覆盖零更新、真实临时 Parquet 的筛选与时区显示、只读生成结果、未确认事务、读取失败/空湖、稳定刷新无重复读取、迟到结果隔离、读取线程独立、生成预览成功/失败及 worker 环境隔离。来源 API 使用 mock；未执行真实取数或正式湖写入。编译检查和规范文本定向 `git diff --check` 通过。

[深色零更新结果预览](artifacts/b01_result_table_20260929/dark_noop_detail.png) · [只读生成结果预览](artifacts/b01_result_table_20260929/light_readonly_detail.png)
