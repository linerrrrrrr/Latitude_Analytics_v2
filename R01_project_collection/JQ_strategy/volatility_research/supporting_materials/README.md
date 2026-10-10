# 支持附件：一次 IM 日内波动研究

本目录于 2026-09-08 收录原 JQ_strategy/intraday_modeling_research 的完整材料，支持主项目中的方法讨论。通用符号与方法以[主项目 README](../README.md)为准；这里保存该次研究实际采用的定义、参数、输入与结果，不自动同步原目录后续变更。规范路由见[根级 AGENTS.md](../../../../AGENTS.md)。

| 入口 | 内容 |
|---|---|
| [附件研究说明](intraday_modeling_research/README.md) | 原研究范围、目录和复现入口 |
| [已有结果报告](intraday_modeling_research/FINAL_REPORT.md) | 已经运行的 IM 实验结果与限制 |
| [原文符号核对](intraday_modeling_research/literature/notes/07_notation_survey.md) | 逐篇原始符号、尺度和页码证据 |
| [IM 实现与方法推导](intraday_modeling_research/research/03_method_derivations.md) | 本次实现采用的公式、参数与代码字段对应 |
| [归档清单](attachment_manifest.json) | 所有文件的来源/归档 SHA-256，以及三个 Markdown 文档的路径适配记录 |

本次完整收录 153 个文件，约 356 MB，包含文献、笔记、代码、数据快照、Notebook、运行记录和结果。与复制时源文件逐项核对；仅三个 Markdown 文档适配外部链接，其中附件 README 补充归档身份。计算代码、Notebook、数据、结果、PDF 和既有 manifest 保持原字节，本次未执行实验。

附件中的 15 份论文与主项目 literature 中的论文重复，不另计为新文献。历史 manifest 内的来源路径和旧批次摘要仍是原研究的来源证据，本次归档另有清单，不改写历史记录。文档里链接到 JQ_strategy 的 v1/v2 Notebook 是外部对照资料，未收录在本附件内。

2026-09-14 随主项目迁入 JQ_strategy/volatility_research，仅同步文档中的外部导航链接。attachment_manifest.json 保留 2026-09-08 归档时的路径与摘要，是历史快照，不是迁移后文档的当前摘要清单；计算代码、数据、PDF、Notebook 和已有结果未改动。

该研究的样本为 2022-07-25 至 2026-08-28 的 995 个交易日、238,800 条 IM 分钟记录；2025 用于验证选窗，2026 是曾被观察过的回顾评估段。引用结论时应保留这些边界。

原代码按自身文件位置定位输入；若以后交互执行附件的 overview Notebook，工作目录应为 intraday_modeling_research 本身。这里只保存已有入口，不代表本次整理已经重新验证其全部实证结论。
