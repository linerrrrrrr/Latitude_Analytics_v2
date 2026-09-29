# a00 采集工作台本次验证记录

本文件仅记录本次改动和验证结果，不新增运行规范；正式说明见 `02_Futures_Lakehouse/operations/README.md` 与同目录 `AGENTS.md`。

## 后续四项导航调整

按用户后续要求，左侧 a00 已收敛为 a00_01— a00_04 四项。a00_01 页内切换运行环境与 Windows 控制面辅助检查，并显示辅助脚本的 `operations/runtime/verify_operations_runtime.py` 归属；a00_02 页内切换代码检查、完整检查、导出同步。a00_03/a00_04 各自保留流程职责页。

页内模式在当前窗口内保持，重新读取采集参数后也保留。各模式仍独立绑定运行目录和日志；导航和模式切换不执行任务。五个实际工具的启动均已验证回到正确父页面及模式。

本轮验证：a00 专项 8 项、单环节与工具测试 18 项、既有总控台测试 30 项，共 56 项通过；语法及文档 whitespace 检查通过。明暗主题与 900×640 小窗口预览位于 `00_draft_collection_02/artifacts/a00_four_pages_20260929/`，均为测试状态。本轮未改动采集脚本，也未启动业务采集或正式导出同步。

## 首次实现验证记录

- “采集配置”与原维护工具合并为“采集工作台”；a00 不参加业务批量勾选或日常快捷配置。
- 五个执行看板：Python 运行环境、Windows 控制面、Notebook 代码检查、完整检查、导出同步。
- 两个共用模块流程页：Schema/样例浏览、路径安装与失败恢复。没有独立执行或通用恢复按钮。
- 按实际事件分别显示流程位置、各步进度、本步总量/已处理/剩余、异常、逐项结果、耗时、心跳与增量原始日志。a00 单项启动后留在本页。
- 原事件协议、后台 worker、锁、中断和失败停止边界继续复用。当前没有改造 a01—a04 业务看板。
- 已检查根级规范索引所列文本的相关引用，更新根 AGENTS、湖仓 AGENTS/README、operations AGENTS/README；没有修改业务 Notebook、数据契约或归档证据。

验证均使用 `E:\anaconda3\envs\latitude\python.exe`：

| 范围 | 结果 |
|---|---|
| operations 既有测试 | 53 项通过 |
| 草稿区 `test_operations_*.py` | 28 项通过 |
| 新增 `test_a00_workbench.py` | 7 项通过 |
| 最后一次既有 `test_console.py` 界面回归 | 30 项通过（含在上述 53 项中） |
| Python 运行环境检查 | latitude，核心依赖均存在 |
| 受影响 Python 文件语法编译 | 通过 |
| 文档 Git whitespace 检查 | 通过；仅既有 CRLF 策略提示 |
| Notebook 代码检查 | 19/19 通过，4 项非正文差异提示 |
| Notebook 完整字节检查 | 15/19 通过，4 份现有导出存在非正文差异 |

完整检查差异位于 a01/b01_trade_calendar、a01/b03_futures_contract_calendar、a02/b01_exchange_report_calendar、a04/b01_macro_release_calendar。代码 AST 均一致；本次未修改这些工作流，没有运行正式 `--write` 同步。

新增事件测试只在临时目录的 19 份最小 Notebook 中验证写入/复核，以及缺失依赖、单文件比较失败；不执行正式 Notebook、不调用业务 API、不写湖。

视觉检查覆盖七页明暗主题及 900×640 小窗口。小窗口通过工作页滚动访问结果与日志；切页时立即隐藏待销毁卡片，避免 Qt 延迟销毁造成覆盖残影。

截图与完整/代码检查日志在 `00_draft_collection_02/artifacts/a00_workbench_20260929/`；所有截图中的运行状态均为明确标记的测试状态，不是生产运行结果。
