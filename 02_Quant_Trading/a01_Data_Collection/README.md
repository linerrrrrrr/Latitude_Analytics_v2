# 数据采集重建项目

本目录最初按照 `a01_Data_Collection_Rebuild_Blueprint` 从空白目录实现；该蓝图现已冻结为历史实施记录，
与当前规范冲突的旧设计由当前契约覆盖。正式 silver 当前为 7 张日历维度表和 10 张事实表，共 17 张；
所有 Arrow Schema 与中文 metadata 由 `config/data_contracts.py` 统一约束。生意社来源原文进入 raw 层，
不计入 silver 表数。

## 强制执行边界

- 当前实现顺序受
  [重建执行清单](../a01_Data_Collection_Rebuild_Blueprint/08_EXECUTION_CHECKLIST.md)
  约束：代码、消费者和空湖验证全部通过后，才允许编写或运行数据迁移程序。
- 业务 Notebook 是唯一直接编辑的源文件；同名 `.py` 仅由默认 PythonExporter 生成。
- 所有命令都要求操作员显式启动；当前不提供根级 BAT 编排，不创建定时任务或后台恢复。预计超过 10 分钟的当前授权批次必须先通过非正式小样本，随后才可由边界明确且不自动重试的 detached worker 执行；运行时须另开不依赖 LLM 的用户可见 Terminal，持续显示阶段、进度、耗时、心跳和失败。确认 worker 与 monitor 健康后，Codex 必须结束回合，不得靠轮询保持活动。
- 业务入口不设置独立的 API 调用开关：命令启动后直接执行数据采集与契约校验；`--write` 是唯一的
  “是否写入”开关，不带 `--write` 时不得改动数据湖、日历状态或完成水位。
- `.env` 的 `FUTURES_LAKE_ROOT` 指向唯一正式湖根目录，其下按职责分为 `raw`、`silver`、`gold`；业务代码通过
  `settings.futures_lake_root` 引用，`--lake-root` 只作为非正式临时湖覆盖。首次重建前，将旧
  `03_Futures_Database/futures_lake` 在同一磁盘直接移动到
  `04_Old_Projects/futures_lake_pre_rebuild_20260810`；禁止读取旧数据后重新写入归档。
- `.env` 的 `FUTURES_DATA_START_DATE` 是当前稳定采集统一正式起点；变量名为兼容既有期货入口而保留，
  `b04/c01` 也从该日期生成宏观理论历史水位，不另设生产日期范围。
- 运行任何业务入口前必须先验证 `latitude` 环境和 Notebook/Python 双轨同步状态；预检失败时不调用 API。
- API 成功、确认空、可重试错误、永久错误、Schema 错误和质量错误分别处理；结构化事实正式路径
  复读成功前不得回写日历完成状态。生意社 c02 不生成结构化事实，必须在 raw 原文字节及 SHA-256 sidecar
  正式复读并核对一致后才回写日历。

## 自动更新范围与日期参数禁令

正式更新的唯一范围规则是：

```text
上游当前有效格点
    − 下游已经完整落盘的格点
    = 本次自动更新范围
```

“完整落盘”不是“日期不晚于下游最大日期”的同义词。格点必须具有完整主键，并通过权威
Arrow Schema/metadata、表级质量规则和正式路径复读；尾部新增、历史内部空洞以及内容不完整的格点都进入
待补集合。下游没有文件时，已完整格点集合为空，同一差集自然产生全量建表，不另设生产全量日期参数。

完整业务语义由每张表自己的生产者在转换、staging 复读和正式路径复读中证明。消费者信任已经正式提交的
上游表，不复制上游全部表级质检；消费者只检查权威 Schema/metadata 以及自身计算直接依赖的主键、范围和
覆盖边界，并对自己的输出执行完整契约与质量校验。

写入 `FUTURES_LAKE_ROOT` 指向的正式湖时，禁止显式指定起止日期。带日期参数的命令只能：

- 不带 `--write` 做定向采集和校验；或
- 同时用 `--lake-root` 指向解析后明确不同于正式湖的临时/测试湖。

当前 `b01/c01_trade_calendar` 至 `b01/c08_full_minute_quality`、
`b02/c01_exchange_report_calendar` 至 `b02/c03_warehouse_receipt`，以及
`b03/c01_external_market_calendar` 至 `b03/c04_external_index`、`b04/c01_macro_release_calendar` 至 `b04/c03_macro_release` 已完成逐文件迁移；其中 b01/c01 每次自动运行都会
重新取得完整有效区间的 JQData 交易日集合，以同时发现缺失日期和历史交易状态修订；c03 按交易所—年月
流式比较 Session 当前真值；c04 从完整 Session 上游逐月比较全部 `1d`/`1m` 结构格点并保留未变化格点的
下游回写状态；c05 在完整 `1d` 格点上应用共享事实白名单，再以选中格点减去完整日线事实并使用 JQData 自动补缺；
c06 在完整 `1m` 格点上应用同一白名单，再以选中的 Session 减去由正式分钟事实和日历状态共同证明完整的 Session，按品种月分区补缺；c07 按证据指纹自动选择需重跑的疑似休市 Session；c08 则是独立手工全量审计，不属于日常差集更新。b02/c01 不调用外部 API，从完整品种日历展开三类报告格点，自动识别新增、缺口、撤销与政策变化，并保留未变化格点的事实采集状态；b02/c02 对排名和会员类型的共同待办格点只查询一次 JQData `finance.FUT_MEMBER_POSITION_RANK`，将长表透视成两张事实；b02/c03 对仓单 required 格点自动求差，查询 JQData `finance.FUT_WAREHOUSE_RECEIPT` 并保存数量、来源单位与较昨日变化。两类事实入口都在正式复读后回写报告日历。b03/c01 不调用外部 API，从完整自然日历与共享请求实体配置生成现货、境外期货全表和 19 个外部指数的完整理论请求格点，保留无需请求日并继承未变化格点的下游状态；b03/c02 以 `domestic_spot_basis/ALL` required 日期减去 raw 原文与日历共同证明完整的日期，只归档生意社 `response.content` 原始字节及 SHA-256 sidecar，不解析或提取，并对 raw 完整但日历陈旧的日期无 API 修复状态；b03/c03 对 `overseas_futures/ALL` required 日期先分流，正式事实缺失或不完整的日期才查询 JQData `finance.FUT_GLOBAL_DAILY`，正式事实完整但日历状态或原因陈旧的日期则从正式事实复算行数与 OHLC 结论、无 API 提交并复读日历；来源 ID、请求日期、返回上限和非有限值继续严格检查，有限 OHLC 高低关系异常按原值落盘并以 `warning` 留痕；b03/c04 以 `external_index/INDICATOR_ID` required 格点减去正式事实与日历状态共同证明完整的格点，按共享配置逐指标将连续待办段分页请求 Eastmoney `RPT_INDUSTRY_INDEX`，只接纳精确待办日期，严格校验分页计数、来源映射、日期和有限值，并把不再属于当前 required 水位的旧事实无 API 清退；完整指数分类—年月分区复读后才回写成功、确认空或失败状态。

其中 c08 的全量审计严格收缩为 `c04 required 且已完成的理论分钟主键 − c06 正式分钟事实的 contract_code、bar_at 主键`。它不读取行情值、不复核 c06 已承担的主键、Session、范围、数值或 OHLC 门禁，也不调用 c07；只回写日历的实际/缺失计数与检查时间，质量结论、旁证和完成状态逐值继承。

b04/c01 不调用 API，从环境统一起点到北京时间当前日按共享配置生成 8 个 SHIBOR 普通工作日格点和 17 个宏观月末/季末格点，计算版本化可用日，继承政策未变化的事实状态，并以完整 `dataset_name/year/month` 叶分区提交和正式复读。b04/c02 以 `interest_rate` required 系列—日期格点减去正式 SHIBOR 事实与日历状态共同证明完整的格点；正式事实完整但日历陈旧时无 API 修复状态，事实缺口按年月窗口调用 Tushare `pro.shibor`，只接纳精确待办期限，完整事实叶正式复读后才回写成功或确认空。b04/c03 以 `macro_release` required 系列—报告期格点自动求差或无 API 修复日历；事实缺口按报告名和年月严格分页请求 Eastmoney，将来源报告月 1 日归一到项目月末/季末，按共享配置读取 PPI `BASE_SAME` 并把 CPI/PPI 累计指数减 100 转成累计同比百分比，完整事实叶正式复读后才回写日历。至此 18 个正式采集入口均已迁移完成。

## 历史湖迁移工具

历史湖迁移另由草稿工具 `00_draft_collection_02/scripts/migrate_pre_rebuild_futures_lake.py` 管理，支持只读 `--plan`、人工授权后的 `--execute` 和失败恢复 `--recover`。该工具不联网；兼容分钟 Parquet 可只重写 footer，重叠叶分区才合并重写。历史日线新增的昨收、结算及变化字段全部保持 null，历史 `source` 标签按 `config/data_contracts.py` 永久允许列表校验；新采集入口不改变当前来源标签。正式湖迁移前必须完成 staging/正式复读、跨表主键和质量门禁。

## 目录和目标表

| 业务目录 | 入口数 | 目标表/职责 |
|---|---:|---|
| `b01_Futures_Market_Data` | 8 | 交易日历、品种/合约/行情日历、日线、分钟线、定向校对、分钟全量质检 |
| `b02_Futures_Exchange_Reports` | 3 | 报告日历、排名与会员类型持仓、仓单 |
| `b03_External_Market_Data` | 4 | 外部市场日历、生意社原文归档、境外期货、外部指数 |
| `b04_Macro_And_Interest_Rates` | 3 | 宏观发布日历、SHIBOR、宏观发布值 |

当前各表字段、主键、分区、来源列和质量规则以 `config/data_contracts.py` 的可执行 Schema/metadata 为准，
永久文本约束见数据库 `AGENTS.md`；重建蓝图只保留为冻结的历史实施记录，不再承担当前语义治理，
其中任何与当前表集合或生意社职责冲突的旧设计，均由当前 17 表契约与 raw 原文归档政策覆盖。

## Notebook 语义浏览

业务 Notebook 使用 `config/notebook_schema_browser.py` 统一展示当前工作流直接涉及的权威 Arrow Schema。
界面依次展示 Schema 列表、Schema 下拉框、表级 metadata 表、字段目录表、Field 下拉框和单字段完整
metadata 表。字段目录只展示英文字段名、中文字段名、Arrow 类型、结构角色、语义角色和来源系统；结构角色
根据当前 Schema 的主键与分区 metadata 计算。

该界面只读取 `config/data_contracts.py` 中的 Schema 及其 metadata，不定义表名、字段、主键或分区，也不代替
契约验证。当前 `b01/c01` 至 `b01/c08`、`b02/c01` 至 `b02/c03`、`b03/c01` 至 `b03/c04` 和 `b04/c01` 至 `b04/c02` 已接入；其余业务 Notebook 在本次重建期间按
[Arrow 中文 metadata 契约](../a01_Data_Collection_Rebuild_Blueprint/01_METADATA_CONVENTION.md) 接入同一个共享实现；
重建完成后的持续治理以 [数据库 AGENTS.md](../../03_Futures_Database/AGENTS.md) 为准。
展示单元格只在交互式 Notebook 内核中运行，导出的命令行 `.py` 不加载 widgets。

同一单一来源规则也适用于业务执行代码：每个 Notebook 直接导入具名权威 Schema，在模块初始化时从
`table_name`、`primary_key` 和 `partition_columns` metadata 各读取一次，后续路径、分区、唯一性校验和
日志只复用读取结果。不得在业务代码中平行硬编码这些值，不得在每个使用点重复解码，也不得增加
`SCHEMA = ...` 纯改名层。长期约束及标准写法同样以数据库 `AGENTS.md` 为准。

## 双轨同步

```powershell
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b00_sync_notebook_exports.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b00_sync_notebook_exports.py --check
```

同步入口只递归扫描四个正式 `b` 目录中的 `cNN_*.ipynb`，不扫描蓝图、草稿或归档项目。

## 当前运行边界

原 `run_full_rebuild.bat`、`run_daily_incremental.bat` 和 `run_full_minute_quality.bat`
已按用户要求删除。当前只保留各业务目录下的 `cNN_*.py` 命令入口，执行前先运行：

```powershell
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/verify_runtime.py
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b00_sync_notebook_exports.py --check
```

已经迁移的十八个入口写入正式湖时不传湖路径和日期：

```powershell
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b01_Futures_Market_Data/c01_trade_calendar.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b01_Futures_Market_Data/c02_futures_variety_calendar.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b01_Futures_Market_Data/c03_futures_contract_calendar.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b01_Futures_Market_Data/c04_futures_bar_calendar.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b01_Futures_Market_Data/c05_futures_daily.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b01_Futures_Market_Data/c06_futures_minute.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b01_Futures_Market_Data/c07_suspected_session_reconciliation.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b01_Futures_Market_Data/c08_full_minute_quality.py --confirm-full-quality --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b02_Futures_Exchange_Reports/c01_exchange_report_calendar.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b02_Futures_Exchange_Reports/c02_futures_holding_reports.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b02_Futures_Exchange_Reports/c03_warehouse_receipt.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b03_External_Market_Data/c01_external_market_calendar.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b03_External_Market_Data/c02_domestic_spot_basis.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b03_External_Market_Data/c03_overseas_futures.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b03_External_Market_Data/c04_external_index.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b04_Macro_And_Interest_Rates/c01_macro_release_calendar.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b04_Macro_And_Interest_Rates/c02_interest_rate.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b04_Macro_And_Interest_Rates/c03_macro_release.py --write
```

程序会先输出自动差集计划；不带 `--write` 时只采集、比较和校验，不提交。

空湖搭建可以按
[数据流与依赖顺序](../a01_Data_Collection_Rebuild_Blueprint/06_DATA_FLOW_AND_DEPENDENCIES.md)
由操作员逐个调用业务入口。`b01/c01` 至 `b01/c07` 已使用自动差集或证据指纹兼容空湖全量和日常补缺；`b01/c08` 已实现独立全量审计；`b02/c01` 已从上游品种日历自动维护完整报告格点并保留事实采集状态；`b02/c02`、`b02/c03` 已分别以报告日历 required 格点减去事实和日历状态共同证明完整的格点，自动查询 JQData、按完整叶分区提交并回写状态；`b03/c01` 已从完整自然日历和版本化请求实体配置自动维护完整理论请求格点，识别内部缺口、上游撤销和配置变化并保留未变化下游状态；`b03/c02` 已按现货 required 日期减去 raw 原文与日历共同完整的日期，只归档原始响应字节和摘要，并对原文完整但日历陈旧的日期无 API 修复；`b03/c03` 已把境外期货 required 日期分成事实缺口 API 待办与事实完整但日历陈旧的无 API 状态修复集合，后者只从正式事实复算并复读日历；`b03/c04` 已按外部指数 required 指标—日期格点自动补缺，逐指标分页查询 Eastmoney、只接纳精确待办日期，并把上游失效旧事实从原完整叶分区无 API 清退；完整指数分类—年月分区正式复读后才回写外部市场日历；`b04/c01` 已按统一起点、共享系列配置和当前日自动维护完整宏观理论日历并继承未变化事实状态；`b04/c02` 已按上游 SHIBOR required 格点自动补缺或无 API 修复日历，逐月调用 Tushare、只触达精确待办期限并在完整事实叶正式复读后回写状态；`b04/c03` 已按上游宏观 required 格点自动补缺或无 API 修复日历，逐报告严格分页调用 Eastmoney，归一来源日期并按共享配置转换数值，完整事实叶正式复读后才回写状态。至此所有 18 个正式入口均完成分区内保留、主键覆盖和日历状态回写；项目仍不建立跨 17 张稳定 silver 表的根级自动编排入口。

这里的 c08 “独立全量审计”特指分钟主键投影求差；不是对 c06 行情值质量或 c07 旁证的第二次全表复核。

因此，存在 API 待办时，不带 `--write` 运行采集入口仍会访问其规定的上游 API 并完成内存中的转换和校验，只是不提交结果；纯 `b03/c02` 日历状态修复只复读 raw 原文及摘要、不请求生意社，纯 `b03/c03` 日历状态修复不认证或重拉 JQData，纯外部指数失效事实清退计划不创建 Eastmoney 会话，纯 `b04/c02` 日历状态修复不创建或调用 Tushare 客户端，纯 `b04/c03` 日历状态修复不创建 Eastmoney 会话；
不调用 API 的 `b01/c07`、`b01/c08`、`b02/c01`、`b03/c01` 和 `b04/c01` 则完成正式湖只读计算与校验，不带 `--write` 时不提交任何状态。
是否允许 Agent 发起具体生产批次，仍受量化交易目录的人工触发规则约束。

定向校对直接调用 `b01_Futures_Market_Data/c07_suspected_session_reconciliation.py`。默认模式只处理
c06 正式复读为 0 条后形成或证据发生变化的 `is_fetch_required=true AND suspected_closed` Session；
显式日期或合约范围不得写回正式湖。一致结果只形成 `reconciled` 旁证，不确认休市、不取消拉取。分钟全量
校对直接调用 `b01_Futures_Market_Data/c08_full_minute_quality.py` 并显式传入
`--confirm-full-quality`。c08 不提供日期、月份或合约过滤，只处理 required 且已完成的 `1m` Session；分钟事实严格只投影
`contract_code,bar_at`，以向量主键求差生成明细，只回写日历的 `actual_bar_count`、`is_data_missing`、
`missing_bar_count`、`missing_checked_at`、`updated_at`。它不扫描行情值、不重复执行 c06 的质量门禁，也不调用 c07；
c06 质量结论、c07 旁证和拉取完成状态必须逐值继承。不带 `--write` 时只在系统临时目录构建并复读
staging，带 `--write` 时全量替换缺失明细并协调提交所有触达的行情日历叶分区，任一步失败共同回滚。
两者当前也不再由 BAT 包装。

结构化事实供应商明确返回空结果时，通常不伪造业务行；入口会建立 0 行 `schema.parquet`，使该表仍可按
Arrow Schema 和 Hive 分区契约读取。`fact_futures_daily` 是明确例外：它必须为每个已请求的日历格点
保留一行，并用 `has_market_data=false`、空行情度量和日历中的缺失计数表达 JQData 没有有效收盘价。
网络超时和 Schema 错误不得按确认空处理。生意社 raw 链路是另一项明确例外：HTTP 200 的任意正文均作为
一份成功原文归档，不把正文为空或页面结构变化解释为确认空、Schema 错误或质量错误。c06 只有在 JQData 请求成功、分钟事实正式提交且
对应 Session 从正式路径复读仍为 0 条时，才写入 `suspected_closed + inferred` 并交给 c07 定向校对；
后续正式分钟恢复非空时撤销该疑似信号。日线、分钟线或境外期货来源行的 OHLC 都必须是有限数；若有限
OHLC 仅违反 high/low 跨列关系，生产者保留来源原值并正常落盘，不改写也不丢弃，同时继续在对应 1d 格点、
1m Session 或外部市场请求日期的 `warning` 原因中留痕。该类来源异常不计作缺失，也不得被后续全量审计
覆盖成 `passed`；NaN/Inf、负数量及其他原有硬门禁仍保持失败语义。

## 完整日历宇宙与期货事实采集白名单

品种日历入口必须调用 `get_all_securities(["futures"], date=None)` 取得跨全部日期的完整期货合约目录，
再由项目自己按合约代码和上市/退市区间构建日历。处理顺序为：

`JQData 完整期货证券目录 → 固定月份合约 → 排除 8888/9998/9999 合成代码 → dim_futures_variety_calendar`

事实采集白名单不得参与 `dim_futures_variety_calendar`、`dim_futures_contract_calendar`、
`dim_futures_bar_calendar` 或 `dim_futures_exchange_report_calendar` 的行筛选。四张日历表保留完整目录宇宙，
不按交易所白名单过滤；`CCFX` 等完整目录中的固定月份合约也必须保留。`c04_futures_bar_calendar` 本身不读取
或解释白名单：它只生成完整 1d/1m 理论格点。1d 格点先安全初始化为需要采集，1m 新格点先初始化为
未选择；随后由 `c05_futures_daily`、`c06_futures_minute` 分别应用同一事实白名单并回写
`is_fetch_required` 与 `selection_reason`。

白名单唯一来源是共享策略模块
[`config/futures_fact_collection_policy.py`](../../config/futures_fact_collection_policy.py)；业务目录不得复制或维护第二份白名单。模块中的映射直接列出五个交易所、
60 个期货事实采集品种：

| 交易所代码 | 品种（数量） |
|---|---|
| `GFEX` | `LC PD PS PT SI`（5） |
| `XDCE` | `BB BZ EB EG FB I J JM L LG LH PG PP V`（14） |
| `XINE` | `BC EC LU NR SC`（5） |
| `XSGE` | `AD AG AL AO AU BR BU CU FU HC NI OP PB RB RU SN SP SS WR ZN`（20） |
| `XZCE` | `CY FG MA ME PF PL PR PX SA SF SH SM TA TC UR ZC`（16） |

所有 JQData 调用方统一从
[`config/jqdata_connection.py`](../../config/jqdata_connection.py) 导入认证入口。该模块只负责认证及
Windows TUN 环境下的物理出口绑定，不负责决定采集范围、API 调用时机或是否写入；这些业务语义仍保留在
各 Notebook 入口中，`--write` 仍是唯一的“是否写入”开关。

白名单作用边界按接口调用粒度确定：

- `1d` 日线理论格点全部保留。`c05_futures_daily` 在完整格点上应用共享白名单并回写选择状态，只对
  选中格点批量调用 JQData `get_price(frequency="daily")` 与
  `get_extras("futures_sett_price"/"futures_positions")`；未选中格点保留并标为无需采集。
- `1m` 分钟格点由 `c04` 全部保留，`c04` 不判断白名单。`c06_futures_minute` 读取完整 1m 格点后统一应用
  共享白名单，把选择结果写回 `is_fetch_required` 和 `selection_reason`，再调用 JQData `get_price(1m)`；
  白名单由实际分钟采集器控制请求数和分钟额度。被选择 Session 只有在正式分钟事实复读计数与日历完成状态
  共同一致时才属于完整下游格点；每个品种月分区成功后立即回写状态，遇配额边界正常停止。
- 排名、会员类型持仓和仓单的报告格点全部保留；`is_fetch_required` 同时要求品种在白名单内、日期位于
  对应 API 覆盖期且交易所—品种受该 API 支持。白名单外格点写为 `not_required`，事实入口不会消费。
  当前报告日历入口尚未配置额外覆盖排除，因而现阶段该条件等价于“命中白名单”；后续增加覆盖配置时
  必须与白名单取交集，不能删除格点。

某个历史品种在指定交易日没有活跃合约时，该日可以没有对应品种日历行。白名单调整由 `c05`、`c06`
重新评估各自频率的选择状态，不得触发 `c04` 重建理论格点，也不得重写或缩小任何日历历史。白名单缩减
只停止后续采集并把对应格点标为无需采集，不自动删除已经正式落盘的历史日线或分钟事实。

## 2026-08-10 历史单日流程记录

- 旧 `03_Futures_Database/futures_lake` 已同卷直接移动到
  `04_Old_Projects/futures_lake_pre_rebuild_20260810`，未读取或重写旧数据内容。
- 当时的新湖使用 `2026-08-07` 完成交易日历、期货行情、交易所报告、外部市场、宏观利率和分钟全量
  质检链路；该结果只保留为历史记录，不是当前 17 张稳定 silver 表及 raw 原文政策下的生产基线。
- 新湖包含 262,650 行分钟数据；全量质检缺失 0 行。品种日历当天有 57 个活跃品种；
  白名单中的历史郑商所代码 `ME`、`TC` 当天无活跃固定月份合约。
- `fact_futures_member_position_daily`、`fact_macro_release` 和
  `fact_futures_missing_bar` 当天确认空，均以 0 行契约 Dataset 存在。
- 上述记录使用了已经失效的旧语义。任何当前基线都必须按 `config/data_contracts.py` 的 17 张权威 Schema、
  完整目录日历与事实白名单分离、JQData 报告接口及生意社 raw 原文归档政策重新生成，不能沿用该单日湖。

## 数据源与迁移原则

- 日线：JQData `get_price(frequency="daily", panel=False, fq=None, skip_paused=True, fill_paused=False, round=False)`
  提供 OHLC、成交量、元计成交额和昨收；有限 OHLC 的 high/low 跨列异常按来源原值保留，并在 1d 日历
  格点记录 `warning`。`get_extras("futures_sett_price")` 与
  `get_extras("futures_positions")` 提供结算价和持仓量，昨结、两种相对昨结涨跌及持仓变化由同一合约的
  上一有效交易日值派生。
- 分钟：JQData `get_price(frequency="1m", panel=False, fq=None, skip_paused=True)`；有限 OHLC 的 high/low
  跨列异常行仍按原值落盘，实际条数和缺失条数照常按正式事实复读计算，并在所属 Session 记录 `warning`。
- 会员成交持仓排名：JQData `finance.FUT_MEMBER_POSITION_RANK`。c02 对每个交易所—品种—交易日只查询
  一次，按 `rank_type` 将成交量、持买仓和持卖仓长表透视成逐会员排名事实；只有 `member_name` 为明确
  参与者类型汇总标签的行进入会员类型事实，普通会员名称不得用于猜测类型。
- 仓单日报：JQData `finance.FUT_WAREHOUSE_RECEIPT`。c03 对每个仓单 required 格点查询一次，保存逐仓库
  数量、来源 `unit` 和 `warehouse_receipt_number_increase`；确认空响应只回写日历，不生成占位仓库行。
- 外部市场日历：唯一上游维度是 `dim_trade_calendar`，不调用生意社、JQData 或 Eastmoney。国内现货和
  按日期返回整表的 `FUT_GLOBAL_DAILY` 使用请求实体 `ALL`；19 个 Eastmoney 指数使用来源
  `INDICATOR_ID`。请求实体、指数项目代码、中文名、分类和有效期只由
  [`config/external_market_entities.py`](../../config/external_market_entities.py) 定义。
- 生意社国内现货基差原文：c02 每个 API 待办日期只请求一次 `day-{YYYY-MM-DD}.html`，把 HTTP
  `response.content` 原始字节和 SHA-256 sidecar 原样提交到
  `raw/100ppi/domestic_spot_basis/year=YYYY/month=MM/observation_date=YYYY-MM-DD/{response.html,response.sha256}`。
  当前入口不解码、解析或提取正文，也不生产 `fact_domestic_spot_basis_daily`。完整日期必须同时具有原文字节、
  匹配摘要与完整日历状态；原文完整但日历陈旧时从正式 raw 无 API 修复。HTTP 200 的任何内容正式复读成功后
  均回写 `success`、`record_count=1`、`passed`，不因正文为空、页面结构或业务内容改变结论。页面结构监测、
  历史重采、解析、字段提取与结构化事实生产属于未来另行确认的独立项目。旧 silver 事实目录若存在，c02
  不得自动删除、迁移或改写。
- 境外期货：JQData `finance.FUT_GLOBAL_DAILY`。c03 先把 `overseas_futures/ALL` required 日期分成
  事实缺失/不完整的 API 待办与事实完整但日历状态或原因陈旧的无 API 修复集合；只有前者才逐日查询
  一次全表，后者必须从正式事实复算行数和 OHLC 质量结论、提交并正式复读日历，不得重拉。API 显式选择
  `id,code,name,day,open,close,low,high,volume,change_pct,amplitude,pre_close`；
  `day` 必须等于请求 `snapshot_date`，来源 ID 和代码不可空，达到 5000 行返回上限时拒绝提交。查询成功的
  空表只回写确认空，不制造空品种事实行；有限 OHLC 的 high/low 跨列异常按来源原值落盘，并把该请求日期
  记为 `success + warning`，NaN/Inf、负成交量和负振幅仍拒绝提交。
- 外部指数：Eastmoney `RPT_INDUSTRY_INDEX`。c04 从共享配置读取 19 个 `INDICATOR_ID` 及项目代码、
  中文名和分类；每个指标按连续 required 待办段分页，只选择
  `INDICATOR_ID,INDICATOR_VALUE,REPORT_DATE`，冻结并核对 `pages/count`、页长、累计行数、来源 ID、
  日期范围和跨页唯一性，且只接纳精确待办日期。完整分页后每个待办格点只能形成 1 行事实或 0 行确认空；
  已经退出上游 required 集合的旧事实不访问 API，并从原完整叶分区清退；完整指数分类—年月分区经
  staging 与正式路径复读成功后才回写日历。
- 宏观发布日历：c01 不调用 API。25 个系列、来源 API/原列、宏观数值偏移、普通工作日/月末/季末频率及
  `availability-v1.0.0` 可用日规则只由
  [`config/macro_release_entities.py`](../../config/macro_release_entities.py) 定义。理论水位从
  `FUTURES_DATA_START_DATE` 到北京时间当前日；CPI/PPI 使用次月 9 日并向后顺延周末，PMI 使用月末且
  2 月保守使用 3 月 4 日，GDP 使用季末后第 16 日。这些日期是项目可见性规则，不是 API 实际发布日期。
  规则未变时继承事实状态；变更叶分区经 staging 和正式路径逐值复读后提交，旧策略必须无日期整表迁移。
- SHIBOR：c02 只消费宏观发布日历中的 `interest_rate` required 格点；8 个稳定系列与
  `date,on,1w,2w,1m,3m,6m,9m,1y` 来源映射复用同一共享配置。事实存在但日历陈旧时从正式事实
  无 API 修复；事实缺口按 `year/month` 月度窗口调用 Tushare `pro.shibor`，显式选择上述字段，校验
  官方单次 2000 行上限、日期唯一与范围、有限数和 `[-100, 100]` 百分比硬边界，只接纳精确待办期限。
  完整响应中某个期限为 `None`、`pd.NA` 或 `NaN` 时，该格点正式复读 0 行后记为
  `empty_confirmed + warning`；结构、重复、越界、布尔值、无穷值或其他非法数值必须失败。完整事实叶和
  日历叶均经 staging 与正式路径逐值复读后提交。
- 宏观发布事实：c03 只消费宏观发布日历中的 `macro_release` required 系列—报告期格点；正式事实完整但
  日历陈旧时无 API 修复，事实缺口按 Eastmoney CPI/PPI/PMI/GDP 报告名与 `year/month` 窗口分页请求。
  首页冻结 `pages/count`，后续页必须保持元数据、页长和累计行数一致，来源日期在范围内且跨页唯一；只接纳
  精确待办格点。Eastmoney `REPORT_DATE` 使用报告月 1 日编码，月度报告归一到月末，GDP 只允许季度末月份
  并归一到季末；它不是发布日期。PPI 同比读取 `BASE_SAME`；CPI 全国/城市/农村累计与 PPI 累计来源值
  是以 100 为基准的指数，按共享 `source_value_offset=-100.0` 转成累计同比百分比。当前事实值不得为空、
  为布尔值或非有限数；完整分页中某精确系列缺值时，事实正式复读 0 行后才可写
  `empty_confirmed + warning`。完整事实叶与日历叶均经 staging 和正式路径逐值复读后提交。
- `XDCE.A`、`XZCE.AP` 等白名单外品种仍进入完整日历维度，但不补拉其日线、分钟、持仓或仓单事实；
  各事实入口只消费任务日历中 `is_fetch_required=true` 的格点。
- 存量数据优先做同字段迁移和派生转换；只有日历差集、缺列或质量失败部分才进入 API 补拉清单。
- 迁移工具与迁移报告在 CODE-GATE 完成后才允许进入 `00_draft_collection_02`。

## 相关规范

- [项目级规则](../../AGENTS.md)
- [环境变量模板](../../.env.template)
- [量化交易目录规则](../AGENTS.md)
- [数据库字段与分区规则](../../03_Futures_Database/AGENTS.md)
- [本次一次性重建蓝图](../a01_Data_Collection_Rebuild_Blueprint/README.md)
- [本次一次性执行与退出清单](../a01_Data_Collection_Rebuild_Blueprint/08_EXECUTION_CHECKLIST.md)
- [可执行契约](../../config/data_contracts.py)
- [外部市场请求实体配置](../../config/external_market_entities.py)
- [宏观发布系列、来源变换与可用日配置](../../config/macro_release_entities.py)
