# a01/b01—b04 看板验收记录

本记录描述本次实现与离线验收，不新增业务运行规范。正式交互边界见 [operations 说明](../02_Futures_Lakehouse/operations/README.md#a01-的四个日历看板)。

- 在现有 `operations/console.py` 中增加四个日历入口的运行看板；原参数和完整说明保留。单项启动留在该页，多项批次进入通用监控。
- worker 对四张日历表的现有白名单日志补充明确单位的进度解读；原业务 Notebook 和导出脚本均未因这次界面任务改写。b01/b02 安装量与事务确认分开，b03 信息响应与整个请求批次分开，b04 当前分区频率进度与批次进度分开。
- 看板绑定本窗口各入口的批次，隔离准备检查、其他业务、旧事务及异步日志；缺失总量不推算。支持失败位置、独立水位确认、只读及无需更新状态。
- 文本规范同步到 operations AGENTS/README、湖仓 README 及根/湖仓规范索引。

使用 `E:\anaconda3\envs\latitude\python.exe`，相关测试共 76 项通过：

| 测试 | 数量 |
|---|---:|
| 新增 `tests/test_a01_calendar_dashboards.py` | 10 |
| 既有 operations `test_console.py` | 30 |
| 草稿 `test_operations_progress_events.py` | 10 |
| 草稿 `test_operations_single_and_tools.py` | 18 |
| 草稿 `test_a00_workbench.py` | 8 |

新增测试使用临时状态、日志和临时 worker 脚本，覆盖共同事务尚未确认、多个日期范围事务、局部频率计数、信息请求完成量、处理水位失败、页间状态/错误/日志隔离、只读历史和单项参数冻结。既有界面测试仅更新了 b01 默认进入看板的预期。

`scripts/verify_a01_calendar_visual.py` 生成亮/暗主题四页、完整看板及 900×640 小窗口截图，位于 [预览目录](artifacts/a01_calendar_dashboards_20260929)。截图均使用明确标注的测试状态；已检查流程卡片、计数条、日志及滚动可达性。

本次没有启动业务采集、联网请求业务 API、扫描/写入正式湖或执行导出同步。规范文本定向 `git diff --check` 通过；全工作区检查仍会报告既有 PythonExporter 业务导出中的注释行尾空格，本次未改写这些导出。
