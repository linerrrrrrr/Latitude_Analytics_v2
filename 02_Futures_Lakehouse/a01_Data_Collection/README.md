# 正式数据采集项目

本目录维护正式数据采集入口。正式 silver 当前为 7 张日历维度表和 10 张事实表，共 17 张；
当前采集拓扑共有 19 个正式入口，其中 b01/c08 只允许人工显式运行，默认日常 worker 编排其余 18 个阶段。
所有 Arrow Schema 与中文 metadata 由 `config/data_contracts.py` 统一约束。生意社来源原文进入 raw 层，
不计入 silver 表数。

## 强制执行边界

- 业务 Notebook 是唯一直接编辑的源文件；同名 `.py` 仅由默认 PythonExporter 生成。
- 所有命令都要求操作员显式启动；当前不提供根级 BAT 编排，不创建定时任务或后台恢复。预计超过 10 分钟的当前授权批次可以由边界明确且不自动重试的 detached worker 执行；运行时须另开不依赖 LLM 的用户可见 Terminal，持续显示阶段、进度、耗时、心跳和失败。运行时长、正式湖写入和 detached 执行均不要求先跑非正式小样本；样本、测试湖演练及仅因运行时长追加的 dry-run 属于只有用户明确要求才执行的可选检查。确认 worker 与 monitor 健康后，Codex 必须结束回合，不得靠轮询保持活动。
- 正式 worker、monitor、状态文件、单批运行、失败停止、现场保留和人工处置统一由 [operations 规范](../a02_Data_Collection_Operations/AGENTS.md) 与 [operations 操作说明](../a02_Data_Collection_Operations/README.md) 管理；本 README 不另建第二套启动命令或控制面契约。
- Windows 下 monitor 必须用允许 `ReadWrite/Delete` 的共享句柄读取原子 `status.json` 并立即释放；worker 以临时文件、fsync 和原子替换发布状态，只可对替换控制面的 `WinError 5/32` 做短时有界重试。2026-08-21 的 b01-b04 样本曾因 monitor 读取锁与 `os.replace` 冲突而误停 c06，因此禁止用默认 `Get-Content` 持续读取恢复旧实现；状态发布最终失败时必须终止 child 并保留现场，不能把控制面重试扩展成 API 或阶段自动重试。
- 业务入口不设置独立的 API 调用开关：命令启动后直接执行数据采集与契约校验；`--write` 是唯一的
  “是否写入”开关，不带 `--write` 时不得改动数据湖、日历状态或完成水位。
- `.env` 的 `FUTURES_LAKE_ROOT` 指向唯一正式湖根目录，其下按职责分为 `raw`、`silver`、`gold`；业务代码通过
  `settings.futures_lake_root` 引用，`--lake-root` 只作为非正式临时湖覆盖。
- `.env` 的 `FUTURES_DATA_START_DATE` 是当前稳定采集统一正式起点；变量名为兼容既有期货入口而保留，
  `b04/c01` 也从该日期生成宏观理论历史水位，不另设生产日期范围。
- 运行任何业务入口前必须先验证 `latitude` 环境和 Notebook/Python 双轨同步状态；预检失败时不调用 API。
- API 成功、确认空、可重试错误、永久错误、Schema 错误和质量错误分别处理；结构化事实正式路径
  复读成功前不得回写日历完成状态。生意社 c02 不生成结构化事实，必须在 raw 原文字节及 SHA-256 sidecar
  正式复读并核对一致后才回写日历。

## 自动更新范围与日期参数禁令

除下述 b01 日常增量例外外，正式更新范围仍按生产者定义的上游有效格点与下游完整格点求差：

```text
上游当前有效格点
    − 下游已经完整落盘的格点
    = 本次自动更新范围
```

“完整落盘”不是“日期不晚于下游最大日期”的同义词。生产者仍对本批 dirty 完整叶执行业务校验；随后 staging
只复读物理字段、类型、nullable、表名、主键、分区、主键唯一性和行数摘要。描述性中文 metadata 以当前
`config/data_contracts.py` 为权威，但文本差异不触发物理版本升级或历史 Parquet 重写。正式安装成功即是完成凭证，
消费者不再扫描 clean 历史来重复证明同一规则。

来源异常先区分留存与验收：能够按目标表契约无歧义表达的业务质量异常保留来源值进入 silver，并把格点记为已完成的
`warning`；不能无损表达的已接收载荷按该生产者的 raw 证据契约留存，除非已有显式归一化或校准规则，否则不提交 silver。
raw 留存本身不形成 silver 完成状态。尚未建立 raw 证据契约的入口继续执行当前 silver 门禁，后续按生产者逐项迁移。

写入 `FUTURES_LAKE_ROOT` 指向的正式湖时，显式日期仍只允许：

- 不带 `--write` 做定向采集和校验；或
- 同时用 `--lake-root` 指向解析后明确不同于正式湖的临时/测试湖。

`b01/c01`、`b01/c02`、`b01/c04` 的 `--full` 与显式日期互斥；只有 `--full --write` 才允许正式全历史维护。
`b01/c03` 的成对日期与 `--full` 分别定义定向和全历史来源质检范围，配合 `--write` 时只对齐发现的差异。
`b01/c07 --force` 无论是否写入或是否使用正式湖，都必须同时给出日期范围或合约。

b01 的普通 `--write` 是可信历史上的尾部增量：c01 只请求正式最大自然日之后的日期，无新增时不认证、不调用 API；
c02 只展开 c01 新增交易日，无新增时不请求证券目录；c03 只处理 c02 新增交易日，不做全历史合约计数求差，
来源合约缺少有效 `trade_time` 时 warning 后跳过；c04 只生成 c03 尾部新增结构和对应目标叶。c01—c04 原有全历史能力
统一保留在显式 `--full` 慢路径。

c05、c06 每次仍用少量投影向量化重算当前全历史白名单政策，但 API 只处理 `pending` 或从未成功完成的格点。
`is_fetch_completed=true` 是可信完成凭证：即使最近快照为缺失、warning 或零行，日常也不重复拉取。白名单扩大时历史未完成格点
自动回补；缩小时保留事实、完成批次、实际条数、质量和 c07 旁证，只将当前格点改为无需拉取并清零当前缺失状态；再次纳入时
已有完成证据继续生效。事实和日历只按 dirty 叶定向读取与提交，不做日常全事实复读、状态修复、孤儿清退或批末全根复读。
c06 的零行和部分缺失 Session 均写成完成并保留 warning；有限 OHLC 关系异常保留来源原值并记 warning，不覆盖开市证据字段；
重复键、请求/Session 越界、NaN/Inf 和负数量仍阻断当前 silver 提交。

c07 默认只处理本次由 c06 新形成、尚无 c07 旁证的 `suspected_closed` Session，不把规则版本或证据指纹作为日常历史变化探测器；
四项比较全部匹配可写 `evidence_level=reconciled`，但缺失 Session 的 `quality_status` 仍为 warning。c08 只保留为人工全历史审计，
不进入任何默认 worker，必须显式 `--confirm-full-quality --write`。

b02/c01 不调用外部 API，从完整品种日历展开三类报告格点；b02/c01a 只维护已配置成交量榜特殊案例的上期所权威原文、摘要和校准 manifest；b02/c02 串行按交易所—品种—年月批量请求月份待办日，
只读写当前事实和日历完整叶，并由两张正式事实格点计数与两类报告日历状态共同证明完成；b02/c03 只以 `warehouse_receipt` required 且 `is_fetch_completed=false` 的日历快照形成待办，
每个交易所—品种—年月分区直读对应事实叶和日历叶，不扫描 clean 事实历史或在批末复读表根，并在正式事实复读后回写报告日历。b03/c01 从自然日历和共享实体配置生成理论请求格点；c02 只归档生意社原始字节与摘要，
c03/c04 分别采集境外期货与外部指数。b03—b04 仍按各自生产者的完整格点差集和状态契约运行。

其中 c08 的全量审计严格收缩为 `c04 required 且已完成的理论分钟主键 − c06 正式分钟事实的 contract_code、bar_at 主键`。它不读取行情值、不复核 c06 已承担的主键、Session、范围、数值或 OHLC 门禁，也不调用 c07；只回写日历的实际/缺失计数与检查时间，质量结论、旁证和完成状态逐值继承。

c04 信任 c03 正式提交对上游结构的证明；默认按交易所—频率的目标最大交易日严格追加，旧日结构和目标孤儿叶不参与比较，clean 尾部叶不读状态列、不改变 `updated_at`。`--full` 发现结构新增或变化时，仅结构一致的同主键行继承
全部状态，新增或结构变化行采用安全初始状态，退出结构行删除。历史缺口、孤儿叶和 Session 修订发现均属于 `--full`；描述性 metadata
变化不再触发全叶重写，物理字段、类型、nullable、表名、主键或分区不兼容仍在写入前失败。

b04/c01 不调用 API，从环境统一起点到北京时间当前日按共享配置生成 8 个 SHIBOR 普通工作日格点和 17 个宏观月末/季末格点，计算版本化可用日，继承政策未变化的事实状态，并以完整 `dataset_name/year/month` 叶分区提交和正式复读。b04/c02 以 `interest_rate` required 系列—日期格点减去正式 SHIBOR 事实与日历状态共同证明完整的格点；正式事实完整但日历陈旧时无 API 修复状态，事实缺口按年月窗口调用 Tushare `pro.shibor`，只接纳精确待办期限，完整事实叶正式复读后才回写成功或确认空。b04/c03 以 `macro_release` required 系列—报告期格点自动求差或无 API 修复日历；事实缺口按报告名和年月严格分页请求 Eastmoney，将来源报告月 1 日归一到项目月末/季末，按共享配置读取 PPI `BASE_SAME` 并把 CPI/PPI 累计指数减 100 转成累计同比百分比，完整事实叶正式复读后才回写日历。至此 19 个正式采集入口均已迁移完成。

## 目录和目标表

| 业务目录 | 入口数 | 目标表/职责 |
|---|---:|---|
| `b01_Futures_Market_Data` | 8 | 交易日历、品种/合约/行情日历、日线、分钟线、定向校对、分钟全量质检 |
| `b02_Futures_Exchange_Reports` | 4 | 报告日历、排名特殊案例原文校准、排名与会员类型持仓、仓单 |
| `b03_External_Market_Data` | 4 | 外部市场日历、生意社原文归档、境外期货、外部指数 |
| `b04_Macro_And_Interest_Rates` | 3 | 宏观发布日历、SHIBOR、宏观发布值 |

当前各表字段、主键、分区、来源列和质量规则以 `config/data_contracts.py` 的可执行 Schema/metadata 为准，
永久文本约束见数据库 `AGENTS.md`。

## Notebook 开篇 Schema 契约呈现

直接涉及权威 Arrow Schema 的业务 Notebook，应在开篇使用 `config/notebook_schema_browser.py` 统一呈现
当前工作流的 Schema 契约。
界面依次展示 Schema 列表、Schema 下拉框、表级 metadata 表、字段目录表、Field 下拉框和单字段完整
metadata 表。字段目录只展示英文字段名、中文字段名、Arrow 类型、结构角色、语义角色和来源系统；结构角色
根据当前 Schema 的主键与分区 metadata 计算。

该界面只读取 `config/data_contracts.py` 中的 Schema 及其 metadata，不定义表名、字段、主键或分区，也不代替
契约验证。当前 `b01/c01` 至 `b01/c08`、`b02/c01`、`b02/c02`、`b02/c03`、`b03/c01` 至 `b03/c04` 和
`b04/c01` 至 `b04/c03` 已接入同一共享实现；持续治理以
[数据库 AGENTS.md](../../03_Futures_Database/AGENTS.md) 为准。
展示单元格只在交互式 Notebook 内核中运行，导出的命令行 `.py` 不加载 widgets。

同一单一来源规则也适用于业务执行代码：每个 Notebook 直接导入具名权威 Schema，在模块初始化时从
`table_name`、`primary_key` 和 `partition_columns` metadata 各读取一次，后续路径、分区、唯一性校验和
日志只复用读取结果。不得在业务代码中平行硬编码这些值，不得在每个使用点重复解码，也不得增加
`SCHEMA = ...` 纯改名层。长期约束及标准写法同样以数据库 `AGENTS.md` 为准。

采集工作流的变量命名与是否需要改名统一遵循根目录 `AGENTS.md` 的“变量命名与最小改动”。“最小改动”约束无关扩散，
不要求优先保留短名。在当前已触及的连贯工作流内，可以为数据源、业务身份、真实处理角色、契约状态和对象表示成组统一命名。
例如，`requested_calendar_df`、`expected_calendar_df`、`pending_calendar_df`、`validated_calendar_table`、`existing_calendar_dataset`、`replacement_calendar_table`、
`valid_calendar_dates` 和 `expected_calendar_signature_by_date` 中的 `requested/expected/pending/validated/existing/replacement`、`calendar`、`_df/_table/_dataset`、
复数 `dates` 与映射方向 `_by_date` 都各自承担语义，不应仅为缩短名称而移除。对象表示转换只改变表示后缀，不自动改变业务角色；例如
`pandas_to_arrow(new_variety_calendar_df, ...)` 的结果应命名为 `new_variety_calendar_table`，而不是在没有独立来源或职责差异时改成
`incoming_variety_calendar_table`。禁止的是无独立含义的修饰词堆叠、近义角色词漂移和跨无关文件的风格清洗。

## 双轨同步

```powershell
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b00_sync_notebook_exports.py --write
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b00_sync_notebook_exports.py --check
```

同步入口只递归扫描四个正式 `b` 目录中的 `cNN_*.ipynb`，不扫描草稿或归档项目。

## 当前运行边界

原 `run_full_rebuild.bat`、`run_daily_incremental.bat` 和 `run_full_minute_quality.bat`
已按用户要求删除。当前只保留各业务目录下的 `cNN_*.py` 命令入口，执行前先运行：

```powershell
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/verify_runtime.py
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b00_sync_notebook_exports.py --check
```

默认日常执行的 18 个阶段写入正式湖时不传湖路径和日期；其中 b01 默认只执行 c01—c07：

```powershell
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b01_Futures_Market_Data/c01_trade_calendar.py --write
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b01_Futures_Market_Data/c02_futures_variety_calendar.py --write
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b01_Futures_Market_Data/c03_futures_contract_calendar.py --write
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b01_Futures_Market_Data/c04_futures_bar_calendar.py --write
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b01_Futures_Market_Data/c05_futures_daily.py --write
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b01_Futures_Market_Data/c06_futures_minute.py --write
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b01_Futures_Market_Data/c07_suspected_session_reconciliation.py --write
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b02_Futures_Exchange_Reports/c01_exchange_report_calendar.py --write
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b02_Futures_Exchange_Reports/c01a_position_rank_special_case_calibration.py --write
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b02_Futures_Exchange_Reports/c02_futures_holding_reports.py --write
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b02_Futures_Exchange_Reports/c03_warehouse_receipt.py --write
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b03_External_Market_Data/c01_external_market_calendar.py --write
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b03_External_Market_Data/c02_domestic_spot_basis.py --write
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b03_External_Market_Data/c03_overseas_futures.py --write
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b03_External_Market_Data/c04_external_index.py --write
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b04_Macro_And_Interest_Rates/c01_macro_release_calendar.py --write
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b04_Macro_And_Interest_Rates/c02_interest_rate.py --write
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b04_Macro_And_Interest_Rates/c03_macro_release.py --write
```

程序会先输出自动差集计划；不带 `--write` 时只采集、比较和校验，不提交。

`b02/c03` 的可选性能门槛只能成对用于无显式日期的自动正式 `--write`。本次恢复使用：

```powershell
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b02_Futures_Exchange_Reports/c03_warehouse_receipt.py --write --performance-window-size 50 --performance-max-median-seconds 15.4
```

它在首 50 个成功完整提交分区后检查分区总耗时中位数；超过 15.4 秒时在安全提交边界终止，不回滚已成功分区。默认日常入口不启用该门槛，也不提供历史审计参数。

b01 的全历史维护、c07 有界强制校对和 c08 人工全量审计必须显式调用：

```powershell
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b01_Futures_Market_Data/c01_trade_calendar.py --full --write
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b01_Futures_Market_Data/c02_futures_variety_calendar.py --full --write
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b01_Futures_Market_Data/c03_futures_contract_calendar.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD --write
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b01_Futures_Market_Data/c03_futures_contract_calendar.py --full --write
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b01_Futures_Market_Data/c04_futures_bar_calendar.py --full --write
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b01_Futures_Market_Data/c07_suspected_session_reconciliation.py --force --start-date YYYY-MM-DD --end-date YYYY-MM-DD --write
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b01_Futures_Market_Data/c08_full_minute_quality.py --confirm-full-quality --write
```

统一生产 worker 的 18 阶段 manifest 中，b01 固定为 c01—c07，`--skip-optional-quality` 只移除 c07，
不恢复或增加其他 `--groups` 阶段；c08 永不进入 worker。正式启动命令、运行目录和 monitor 用法只在
[operations README](../a02_Data_Collection_Operations/README.md) 维护。

空湖搭建可以按上列顺序由操作员逐个调用业务入口。b01 的空湖默认运行会从统一起点自然完成首次建立；已有正式湖则只做尾部、pending 和白名单变化，
历史修订与损坏检查由显式全历史入口承担。b02/c01 已从上游品种日历自动维护完整报告格点并保留事实采集状态；
b02/c01a 先保证已配置特殊案例的正式 raw 证据可复读，b02/c02、b02/c03 再按各自报告日历待办补事实并回写状态。b03、b04 继续按各自完整理论水位自动补缺或无 API 修复状态。
19 个正式入口仍各自负责自身 raw 或 silver 提交；默认 worker 只编排其中 18 个日常阶段，不把 c08 变成日常前置条件。

这里的 c08 “独立全量审计”特指分钟主键投影求差；不是对 c06 行情值质量或 c07 旁证的第二次全表复核。

因此，存在 API 待办时，不带 `--write` 运行采集入口仍会访问其规定的上游 API 并完成内存中的转换和校验，只是不提交结果；纯 `b03/c02` 日历状态修复只复读 raw 原文及摘要、不请求生意社，纯 `b03/c03` 日历状态修复不认证或重拉 JQData，纯外部指数失效事实清退计划不创建 Eastmoney 会话，纯 `b04/c02` 日历状态修复不创建或调用 Tushare 客户端，纯 `b04/c03` 日历状态修复不创建 Eastmoney 会话；
不调用 API 的 `b01/c04`、`b01/c07`、`b01/c08`、`b02/c01`、`b03/c01` 和 `b04/c01` 则完成正式湖只读计算与校验，不带 `--write` 时不提交任何状态。
是否允许 Agent 发起具体生产批次，仍受期货湖仓目录及 operations 的人工触发规则约束。

定向校对直接调用 `b01_Futures_Market_Data/c07_suspected_session_reconciliation.py`。默认模式只处理
c06 本次成功提交后形成、尚未具有 c07 旁证的 `is_fetch_required=true AND suspected_closed` Session；既有证据不因
规则版本或输入指纹变化而日常重算。`--force` 只允许重做显式日期或合约范围，不存在无界读取或写回模式。一致结果只形成
`reconciled` 旁证，不确认休市、不取消拉取，`quality_status` 仍保持 warning。分钟全量
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
后续正式分钟恢复非空时撤销该疑似信号。日线、分钟线或境外期货来源行的 OHLC 非空值都必须有限；若有限
OHLC 仅违反 high/low 跨列关系，生产者保留来源原值并正常落盘，不改写也不丢弃，同时继续在对应 1d 格点、
1m Session 或外部市场请求日期的 `warning` 原因中留痕。该类来源异常不计作缺失，也不得被后续全量审计
覆盖成 `passed`。日线和分钟线的 NaN/Inf 仍失败；境外期货财务库可空数值的 Pandas NaN 缺失标记归一为
Arrow null且不另记 warning，非空非有限数、负数量及其他原有硬门禁仍保持失败语义。

## 完整日历宇宙与期货事实采集白名单

品种日历入口必须调用 `get_all_securities(["futures"], date=None)` 取得跨全部日期的完整期货合约目录，
再由项目自己按合约代码和上市/退市区间构建日历。处理顺序为：

`JQData 完整期货证券目录 → 固定月份合约 → 排除 8888/9998/9999 合成代码 → dim_futures_variety_calendar`

`c02_futures_variety_calendar` 信任 c01 已正式提交的交易日历业务语义；消费上游时只确认 Dataset 存在且
Schema/metadata 物理兼容，并只投影 `calendar_date`、`is_trading_day`。c02 不再重复检查上游日期主键、
逐自然日连续性或 c01 派生字段；这些失败必须由 c01 自身的提交与复读门禁负责。c02 仍对自己的理论结果、
现有正式表、完整叶分区以及 staging/正式路径逐值一致性承担责任。

`c03_futures_contract_calendar` 同样信任 c02 的正式证明，只检查 Dataset 存在及 Schema/metadata 物理兼容，
不重复校验 c02 主键、空表、`active_contract_count` 或 JQData 交易日成员关系。默认模式只读取可信自动尾部水位之后的 c02 品种日；水位取正式行最大交易日与根 `schema.parquet` 中 `automatic_tail_processed_through` 的较大值，因此整日候选均无有效 `trade_time` 时也会在成功 `--write` 后推进，下次日常 no-op 且不认证。日期参数质检完整日期区间，`--full` 质检当前 c02 全部水位与本地整表；两者均不写自动水位。c03 仍严格校验 JQData 原始响应及自身 dirty 输出，staging 和正式叶只复读物理契约与主键/行数摘要。

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
  API 覆盖期且交易所—品种受支持，并且只表示当前采集义务。`is_fetch_completed` 保存已正式提交的持久完成凭证。
  当前政策排除但从未完成的格点写为 `not_required`；
  已完成格点保留凭证并停止调度，重新纳入时不重复采集。
  当前报告日历入口尚未配置额外覆盖排除，因而现阶段该条件等价于“命中白名单”；后续增加覆盖配置时
  必须与白名单取交集，不能删除格点。

某个历史品种在指定交易日没有活跃合约时，该日可以没有对应品种日历行。白名单调整由 `c05`、`c06`
重新评估各自频率的选择状态，不得触发 `c04` 重建理论格点，也不得重写或缩小任何日历历史。白名单缩减
只停止后续采集并把对应格点标为无需采集，不自动删除已经正式落盘的历史日线或分钟事实。

## 2026-08-10 历史单日流程记录

- 当时位于 `03_Futures_Database/futures_lake` 的重建前旧湖已同卷直接移动到
  `05_Old_Projects/futures_lake_pre_rebuild_20260810`，未读取或重写数据内容。
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
- 会员成交持仓排名：JQData `finance.FUT_MEMBER_POSITION_RANK`。c02 按
  `exchange_code + underlying_code + year + month` 串行归并报告日历待办日，使用
  `finance.run_query(FUT_MEMBER_POSITION_RANK.day.in_(pending_dates))` 批量请求月份待办子集；返回恰好
  5000 行时丢弃截顶响应并按排序日期确定性二分，单日仍触顶则硬失败。来源 `rank` 必须是 `1—20`
  的整数；Top 20 是每个具体合约、每类 `rank_type` 的榜单边界，不是一个品种日的总行数上限。响应按日拆回后，
  按 `rank_type` 将成交量、持买仓和持卖仓长表透视成逐会员排名事实；只有 `member_name` 为明确
  参与者类型汇总标签的行进入会员类型事实，普通会员名称不得用于猜测类型。同一来源业务键出现重复行时，
  只有原始会员标签、排名类别 ID/文字、指标和变化全部一致才按事实粒度合并；逐会员排名取最小名次，
  对应报告日历写为已完成的 `success + warning`。任一来源业务值冲突仍按 Schema 错误停止，不得静默选值。
  对 `config/futures_position_rank_special_cases.py` 明确列出的格点，c01a 先把冻结交易所响应、SHA-256 和校准 manifest 原子归档到
  `raw/shfe/position_rank_special_cases/<case_id>/`。c02 只有在该格点完整 20 行成交量榜逐值命中配置中的已知坏载荷时，才复读正式证据并整榜替换为上期所权威 Top 20；来源已等于权威榜时原样通过，第三种载荷硬失败。持买仓、持卖仓及其他格点不变，报告日历永久保留案例 ID 和原文摘要的 `success + warning`。
  启动时仅以两张正式事实的格点计数和两类报告日历完成状态求差，月份循环只读取、校验和提交当前完整叶，
  不反复扫描事实或日历表根；该入口没有 `--full`，不使用 `run_offset_query` 分页、并发或业务自动重试。
- 仓单日报：JQData `finance.FUT_WAREHOUSE_RECEIPT`。c03 对每个仓单 required 格点查询一次，保存逐仓库
  数量、来源 `unit` 和 `warehouse_receipt_number_increase`；确认空响应只回写日历，不生成占位仓库行。已完成快照不重拉；每个品种月分区只读当前事实叶和日历叶，dirty 叶各执行一次完整业务 validator，staging/正式只复读物理契约、行数和主键摘要，不做 clean 历史或批末全根质检。该优化不减少 API 请求，仍为每个待办日格点一次。
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
  空表只回写确认空，不制造空品种事实行；财务库可空数值列的 `None`、`pd.NA` 和 Pandas `NaN` 缺失标记
  统一归一为 Arrow null且不另记 warning，非空非有限数、负成交量和负振幅仍拒绝提交；有限 OHLC 的
  high/low 跨列异常按来源原值落盘，并把该请求日期记为 `success + warning`。
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

## 相关规范

- [项目级规则与变量命名](../../AGENTS.md)
- [环境变量模板](../../.env.template)
- [期货湖仓生产与运维目录规则](../AGENTS.md)
- [数据采集 operations 规范](../a02_Data_Collection_Operations/AGENTS.md)
- [数据采集 operations 操作说明](../a02_Data_Collection_Operations/README.md)
- [数据库来源异常、字段与分区规则](../../03_Futures_Database/AGENTS.md)
- [可执行契约](../../config/data_contracts.py)
- [外部市场请求实体配置](../../config/external_market_entities.py)
- [宏观发布系列、来源变换与可用日配置](../../config/macro_release_entities.py)
