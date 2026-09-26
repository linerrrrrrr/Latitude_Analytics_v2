# 第 09 项真实小批次往返验收

结论：通过。此记录对应用户实际在聚宽执行的正式 v1 文件，不是模拟来源，也不是前一份准备记录。只完成本次两个合约日，未开始第 10 项。

## 时间与来源

- 聚宽报告初始化／运行开始：2026-09-04T21:49:20.164421+08:00。
- 聚宽文件发布：2026-09-04T21:49:21.276405+08:00。
- 本地首次落盘原始输出：2026-09-04T21:58:53.245925+08:00（文件创建时间）；消息接收时间未提供，不补造。
- 正式入口只校验：21:59:00.287476—21:59:01.202622+08:00。
- 报告／不可变快照／准备计划交叉核对：2026-09-04T21:59:51.587677+08:00。
- 本地正式导入：22:00:43.906214—22:00:45.170703+08:00；成功重导检查：22:00:45.179165—22:00:45.238132+08:00。
- Notebook 代码单元格回查：22:02:47.244524—22:03:06.656594+08:00，独立 latitude 解释器中按顺序执行 10 个非空代码单元格；没有改写 Notebook 保存的输出，也不冒称另一个 Jupyter 内核记录。
- 本地解释器：E:\anaconda3\envs\latitude\python.exe；DuckDB 1.5.4。

## 收到文本、格式转换和代码身份

[raw_output.txt](raw_output.txt) 保留本消息可见的完整聚宽输出，含信封外四条 source 进度和 Markdown 的反斜杠下划线；以 UTF-8、单个末尾 LF 保存。用户指定的 inbox 路径另存 [received_file_path.txt](received_file_path.txt)。不是平台原始 stdout 字节或消息传输元数据。

收到文本的下划线被 Markdown 转义，原信封内部并非可直接解析的 JSON。因此没有把它冒充原样 result.json：[normalized_output.txt](normalized_output.txt) 只将每个反斜杠下划线还原为下划线；[decoded_report.json](decoded_report.json) 是随后解析并格式化的派生报告。raw_output 首次保存后未改写。所有证据摘要见 SHA256SUMS，各文件另有 SHA-256 sidecar。

[准备记录](../prepared_2026-09-04T213454.688871+0800_item_09_v1/analysis.md) 与本次分开。当前本地 Notebook 的原有 20 个单元格源码与准备快照相同，用户新增的末尾空单元格和保存的输出均保留；最终输出 text/plain 的完整代码摘要也与本记录 [joinquant_cell_source.py.txt](joinquant_cell_source.py.txt) 相同：
`195f1ee712974e4493be25d946e88c70d6eee622e6f55570cfe87b6cd03cfff7`。
远端报告没有携带执行源码摘要，不能独立证明聚宽端源码逐字未变；可证实的是本地交付代码、保存的 Notebook 输出一致，并且远端与文件都指向同一个准备计划。这不是来源数字签名。

## 文件与计划完整性

- 正式固定文件：JQ_FINANCIAL_FUTURES_TRANSFER.zip，10,008 字节。
- 聚宽报告、下载原文件、正式导入快照和数据库账本的文件 SHA-256 一致：
  `cc9e1a530ab75ab66f49a10f7acb058b47e824ae0669d18e2f7d00d630df7ec6`。
- 计划 SHA-256：
  `ddd915a9256c5499b04dc01bd1ad8f76c5e547880a03c6cfd9d1379e2429aafc`；与准备 plan.json 的 canonical 摘要一致。
- run_id：`7d96befe-e6ff-4efa-88a0-c9ec71f3d713`。
- 正式导入器验证 DEFLATE 四成员、canonical JSON/JSONL、成员摘要／计数、重建计划、理论键、行情键和值、观察资格与当前正式日历坐标。额外交叉核对 manifest 与用户报告的运行身份、开始时间、计划、发布状态和全部计数。详情：[identity_verification.json](identity_verification.json)、[preflight_result.json](preflight_result.json)。
- 本次先使用正式入口的只校验模式取得不可变快照，以便写库前与人工报告交叉核对；这不是新增日常强制双跑规则。实际提交及成功幂等验收期间，外层只读共享句柄防止用户同时替换此文件；操作完成即释放，后续仍可正常同名覆盖。

## 实际提交和缺失语义

| 合约日 | 日线有效／理论键 | 分钟有效／理论键 | 观察结果 | 入库后本范围待拉取 |
|---|---:|---:|---|---:|
| IF2409.CCFX / 2024-06-28 | 1 / 1 | 240 / 240 | 3 块 complete + passed | 0 |
| TF1303.CCFX / 2012-06-11 | 0 / 1 | 0 / 270 | 3 块 empty + warning | 0 |

四个来源调用全部成功，六个观察块成功；publication_status=complete 说明计划传输完整，并不说明全部理论键有行情。TF 的 271 个键继续属于 data_missing 与 source_confirmed_missing，没有伪造占位行情，也没有删除上游早期 TF 日历；不进入普通无限重拉。

TF 分钟缺段（两端包含）是 2012-06-11 09:16—11:30、13:01—15:15，各 135 键，北京时间；日线缺失键为 2012-06-11。IF 分钟实值分别覆盖 09:31—11:30、13:01—15:00，各 120 行。

首次真实项目数据库已由正式导入器初始化并提交：
`data/warehouse/financial_futures.duckdb`，3,944,448 字节。
四表计数为 ingest_batch=1、fetch_observation=6、futures_daily=1、futures_minute=240。
再次导入返回 already_imported、database_write_performed=false；其 counts 是原批次账本，不表示再次插入。
重导前后及只读回查后的完整数据库 SHA-256 均为：
`71b6b4e972bb0d59bdb410bb5778ebeefa4b5371c782aa92ba89201e3d84ace0`。
详见 [import_acceptance.json](import_acceptance.json)、[import_execution.txt](import_execution.txt)。

## 回查、边界与下一项

[notebook_recheck_result.json](notebook_recheck_result.json) 证明同一验收范围的来源请求数、观察块数均为 0；最终代码是 [说明性空代码](empty_plan_generated_code.py.txt)，没有可执行语句，不调用聚宽也不覆盖文件。IF 不再出现在本范围缺失清单；TF 的 1 日线键和 270 分钟键仍明确可见。

全白名单覆盖宇宙未被验收坐标裁剪。本次 as_of=2026-09-04T22:02:48.472362+08:00 的正常待拉取总量仍为日线 68,652 键、分钟 17,247,540 键；这些只是当次计算快照，不是新的固定生产范围或第 10 项执行授权。

全程未调用本地聚宽 API、未自动登录或下载、未写正式湖；只写本项目库与本条实验记录。正式导入器只清理本次已成功快照与候选库临时名称，inbox 原文件保留且未变；staging 无遗留运行产物。没有失败重试、扩展合约或新增正式脚本。

本次同步清单与状态说明，不改变白名单、Schema、协议、规划算法或导入器。Notebook 只修正阶段说明，用户已保存输出继续保留，因而旧输出仍表示入库前状态；需要查看当前状态时重新从头运行，不要重新复制已消费的旧聚宽代码。第 10 项“初始全量回补并冻结日常操作说明”尚未开始；须由用户下一轮明确推进。没有遗留第 09 项业务阻塞。

文档同步后的附加检查曾把 Notebook 末尾空单元格的 source 错误地限定为 []，实际合法表示是空字符串，因此检查断言失败；随后改为验证拼接后的文本为空并通过，没有修改业务代码、数据或用户空单元格。修订后的检查时间、当前 Notebook 摘要和通过结果见 [document_sync_verification.json](document_sync_verification.json)。该问题不涉及聚宽、导入或缺失回查失败。
