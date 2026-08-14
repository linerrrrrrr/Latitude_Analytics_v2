# 数据采集重建项目

本目录是按照 `a01_Data_Collection_Rebuild_Blueprint` 从空白目录实现的新采集项目。正式
silver 目标为 7 张日历维度表和 11 张事实表，共 18 张；所有 Arrow Schema 与中文
metadata 由 `config/data_contracts.py` 统一约束。

## 强制执行边界

- 当前实现顺序受
  [重建执行清单](../a01_Data_Collection_Rebuild_Blueprint/08_EXECUTION_CHECKLIST.md)
  约束：代码、消费者和空湖验证全部通过后，才允许编写或运行数据迁移程序。
- 业务 Notebook 是唯一直接编辑的源文件；同名 `.py` 仅由默认 PythonExporter 生成。
- 所有命令都要求操作员显式启动；当前不提供根级 BAT 编排，不创建定时任务或后台恢复。
- 业务入口不设置独立的 API 调用开关：命令启动后直接执行数据采集与契约校验；`--write` 是唯一的
  “是否写入”开关，不带 `--write` 时不得改动数据湖、日历状态或完成水位。
- `.env` 的 `FUTURES_LAKE_ROOT` 指向唯一正式湖根目录，业务代码通过
  `settings.futures_lake_root` 引用；`--lake-root` 只作为非正式临时湖覆盖。首次重建前，将旧
  `03_Futures_Database/futures_lake` 在同一磁盘直接移动到
  `04_Old_Projects/futures_lake_pre_rebuild_20260810`；禁止读取旧数据后重新写入归档。
- 运行任何业务入口前必须先验证 `latitude` 环境和 Notebook/Python 双轨同步状态；预检失败时不调用 API。
- API 成功、确认空、可重试错误、永久错误、Schema 错误和质量错误分别处理；事实正式路径
  复读成功前不得回写日历完成状态。

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

当前 `b01/c01_trade_calendar` 至 `b01/c08_full_minute_quality` 已完成逐文件迁移；其中 c01 每次自动运行都会
重新取得完整有效区间的 JQData 交易日集合，以同时发现缺失日期和历史交易状态修订；c03 按交易所—年月
流式比较 Session 当前真值；c04 从完整 Session 上游逐月比较全部 `1d`/`1m` 结构格点并保留未变化格点的
下游回写状态；c05 在完整 `1d` 格点上应用共享事实白名单，再以选中格点减去完整日线事实并使用 JQData 自动补缺；
c06 在完整 `1m` 格点上应用同一白名单，再以选中的 Session 减去由正式分钟事实和日历状态共同证明完整的 Session，按品种月分区补缺；c07 按证据指纹自动选择需重跑的疑似休市 Session；c08 则是独立手工全量审计，不属于日常差集更新。其他 10 个采集入口仍按
逐脚本审查顺序迁移。本轮文字契约对全链路生效，但不表示其他入口的旧代码已经自动完成对齐。

## 目录和目标表

| 业务目录 | 入口数 | 目标表/职责 |
|---|---:|---|
| `b01_Futures_Market_Data` | 8 | 交易日历、品种/合约/行情日历、日线、分钟线、定向校对、分钟全量质检 |
| `b02_Futures_Exchange_Reports` | 3 | 报告日历、排名与会员类型持仓、仓单 |
| `b03_External_Market_Data` | 4 | 外部市场日历、现货基差、境外期货、外部指数 |
| `b04_Macro_And_Interest_Rates` | 3 | 宏观发布日历、SHIBOR、宏观发布值 |

当前各表字段、主键、分区、来源列和质量规则以 `config/data_contracts.py` 的可执行 Schema/metadata 为准，
永久文本约束见数据库 `AGENTS.md`；重建蓝图四份 metadata 文档只记录本次搭建的设计输入，搭建完成后冻结，
不得继续作为当前语义来源。

## Notebook 语义浏览

业务 Notebook 使用 `config/notebook_schema_browser.py` 统一展示当前工作流直接涉及的权威 Arrow Schema。
界面先展示 Schema 列表，再分别通过紧邻对应 metadata 表的 Schema 和 Field 下拉框查看完整表级与字段级语义。

该界面只读取 `config/data_contracts.py` 中的 Schema 及其 metadata，不定义表名、字段、主键或分区，也不代替
契约验证。当前 `b01/c01` 至 `b01/c08` 已接入；其余业务 Notebook 在本次重建期间按
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

已经迁移的八个 `b01` 入口写入正式湖时不传湖路径和日期：

```powershell
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b01_Futures_Market_Data/c01_trade_calendar.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b01_Futures_Market_Data/c02_futures_variety_calendar.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b01_Futures_Market_Data/c03_futures_contract_calendar.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b01_Futures_Market_Data/c04_futures_bar_calendar.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b01_Futures_Market_Data/c05_futures_daily.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b01_Futures_Market_Data/c06_futures_minute.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b01_Futures_Market_Data/c07_suspected_session_reconciliation.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b01_Futures_Market_Data/c08_full_minute_quality.py --confirm-full-quality --write
```

程序会先输出自动差集计划；不带 `--write` 时只采集、比较和校验，不提交。

空湖搭建可以按
[数据流与依赖顺序](../a01_Data_Collection_Rebuild_Blueprint/06_DATA_FLOW_AND_DEPENDENCIES.md)
由操作员逐个调用业务入口。`c01` 至 `c07` 已使用自动差集或证据指纹兼容空湖全量和日常补缺；`c08` 已实现独立全量审计；多数后续表当前仍以
“触达分区整体替换”方式提交，尚未完成统一迁移。在分区内保留、主键覆盖和日历完成状态回写全部完成前，
不建立跨 18 张表的根级自动编排入口。

因此，不带 `--write` 运行采集入口仍会访问其规定的上游 API 并完成内存中的转换和校验，只是不提交结果；
不调用 API 的 `c07`、`c08` 则完成正式湖只读校验而不回写日历。
是否允许 Agent 发起具体生产批次，仍受量化交易目录的人工触发规则约束。

定向校对直接调用 `b01_Futures_Market_Data/c07_suspected_session_reconciliation.py`。默认模式只处理
c06 正式复读为 0 条后形成或证据发生变化的 `is_fetch_required=true AND suspected_closed` Session；
显式日期或合约范围不得写回正式湖。一致结果只形成 `reconciled` 旁证，不确认休市、不取消拉取。分钟全量
校对直接调用 `b01_Futures_Market_Data/c08_full_minute_quality.py` 并显式传入
`--confirm-full-quality`。c08 不提供日期、月份或合约过滤；不带 `--write` 时只在系统临时目录构建并复读
staging，带 `--write` 时全量替换缺失明细并协调提交所有触达的行情日历叶分区，任一步失败共同回滚。
两者当前也不再由 BAT 包装。

供应商明确返回空结果时，通常不伪造业务行；入口会建立 0 行 `schema.parquet`，使该表仍可按
Arrow Schema 和 Hive 分区契约读取。`fact_futures_daily` 是明确例外：它必须为每个已请求的日历格点
保留一行，并用 `has_market_data=false`、空行情度量和日历中的缺失计数表达 JQData 没有有效收盘价。
网络超时、页面结构异常和 Schema 错误不得按确认空处理。c06 只有在 JQData 请求成功、分钟事实正式提交且
对应 Session 从正式路径复读仍为 0 条时，才写入 `suspected_closed + inferred` 并交给 c07 定向校对；
后续正式分钟恢复非空时撤销该疑似信号。供应商原始分钟行若违反 OHLC 关系，c06 拒收该行且不修写价格，
在对应 Session 的缺失计数和 `warning` 原因中留痕；若 0 条由此产生，不得误判为疑似休市。

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
59 个期货事实采集品种：

| 交易所代码 | 品种（数量） |
|---|---|
| `GFEX` | `LC PD PS PT SI`（5） |
| `XDCE` | `BB BZ EB EG FB I J JM L LG PG PP V`（13） |
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

## 2026-08-10 单日完整流程验收

- 旧 `03_Futures_Database/futures_lake` 已同卷直接移动到
  `04_Old_Projects/futures_lake_pre_rebuild_20260810`，未读取或重写旧数据内容。
- 新湖使用 `2026-08-07` 完成交易日历、期货行情、交易所报告、外部市场、宏观利率和分钟全量
  质检链路；18 张 silver Dataset 均通过精确 Schema/metadata 和主键唯一性检查。
- 新湖包含 262,650 行分钟数据；全量质检缺失 0 行。品种日历当天有 57 个活跃品种；
  白名单中的历史郑商所代码 `ME`、`TC` 当天无活跃固定月份合约。
- `fact_futures_member_position_daily`、`fact_macro_release` 和
  `fact_futures_missing_bar` 当天确认空，均以 0 行契约 Dataset 存在。
- 上述验收使用了“白名单过滤品种日历”的旧语义。完整目录日历与事实白名单分离后，该单日湖必须按
  新的 `1.1.0` 日历契约重新生成，不能作为新语义下的生产基线。

## 数据源与迁移原则

- 日线：JQData `get_price(frequency="daily", panel=False, fq=None, skip_paused=True, fill_paused=False, round=False)`
  提供 OHLC、成交量、元计成交额和昨收；`get_extras("futures_sett_price")` 与
  `get_extras("futures_positions")` 提供结算价和持仓量，昨结、两种相对昨结涨跌及持仓变化由同一合约的
  上一有效交易日值派生。
- 分钟：JQData `get_price(frequency="1m", panel=False, fq=None, skip_paused=True)`。
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
