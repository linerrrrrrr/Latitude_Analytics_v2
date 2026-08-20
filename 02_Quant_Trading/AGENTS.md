# 适用范围与规范地位

- 本文件是 `E:\Latitude_Analytics_v2\02_Quant_Trading` 整棵目录树的目录级 Agent 规则入口，适用于目录本身以及任意层级的当前子目录、只读旧项目和未来新增目录。
- 本文件用于集中定义或索引量化交易目录的全部 Agent 规则；`.ipynb/.py` 双轨及 PythonExporter 只是当前其中一个专题，不代表本文件只负责双轨。
- 覆盖范围包括但不限于新建的 `a01_Data_Collection`、`a02_Feature_Engineering` 及其全部后代目录。两个旧采集项目和 `old_02_Feature_Engineering` 已经移动到根级 `04_Old_Projects`，不再属于本目录树。
- 未来新增量化交易目录级规则时，应直接加入本文件或由本文件建立明确索引，不得另建无法从这里发现的孤立规范。
- 本目录继承根目录 [AGENTS.md](../AGENTS.md) 的全部规则。下级 `AGENTS.md` 或 README 可以增加更严格的要求，但未经用户明确同意，不得豁免、弱化或缩小本文件的覆盖范围。

# 根级旧项目归档路由

- `E:\Latitude_Analytics_v2\04_Old_Projects` 保存重建前采集实现、更早历史采集项目和旧特征工程项目；其递归只读规则以 [04_Old_Projects/AGENTS.md](../04_Old_Projects/AGENTS.md) 为准。
- Agent 只允许读取、搜索、比较和分析其中的内容。禁止在该目录内新增、修改、删除、移动、重命名、格式化或重新导出任何文件，也禁止补齐 `.ipynb/.py` 双轨、修复旧代码、写入缓存、测试输出、数据文件或新的规范文本。
- 下文的双轨、PythonExporter、完成条件和存量迁移规则均不得作为修改该旧项目的依据。需要复用旧项目思路时，只能把必要逻辑迁入当前可写目录，并在当前目录按适用规范实现。
- 不得执行可能在该目录内产生 `__pycache__`、Notebook checkpoint、日志或其他副作用的旧项目代码；只读检查必须使用不会写回该目录的方式。
- 只有用户明确点名该目录中的具体目标并明确授权修改时，才可在该次任务的授权范围内例外处理。笼统的“实现双轨”“同步整个 `02_Quant_Trading`”或一般重构请求不构成修改旧项目的授权。

# `.ipynb` 与 `.py` 双轨强制规则

- `02_Quant_Trading` 下所有项目自研的业务工作流入口都必须同时保留同一目录、同一基名的 `.ipynb` 与 `.py`，例如 `strategy.ipynb` 与 `strategy.py`。
- 业务工作流入口包括数据采集、清洗与加工、因子研究、策略研究、回测、建模、评估、报告和可视化等可独立运行的任务；当前由哪种文件起步不影响双轨要求。只读旧项目不属于本规则的改造范围。
- `.ipynb` 用于交互式分步执行、观察中间结果和记录业务解释；`.py` 用于批量执行、模块导入、测试和调度。两者都是正式交付物，未经用户明确同意不得删除、遗漏或用其中一轨替代另一轨。
- `.ipynb` 是唯一允许直接编辑的源文件；同名 `.py` 必须使用项目标准 `latitude` 环境中的默认 `nbconvert.exporters.PythonExporter` 完整生成，并与导出结果逐字节一致。
- 两轨必须保持核心业务逻辑一致，包括参数与默认值、数据源、字段和 Schema、路径、过滤条件、更新水位及输出语义。Notebook 输出以及 PythonExporter 未导出的元数据可以只存在于 `.ipynb`；Markdown、代码单元格顺序和展示代码若会进入导出结果，就必须同步出现在 `.py` 中。
- 不得直接修改或重新格式化导出的 `.py`；PythonExporter 生成的 shebang、编码声明、`# In[...]` 标记、Markdown 注释、空行与单元格顺序必须原样保留。
- 共享模块必须符合根目录 `AGENTS.md` 的抽象判定：承担项目级不变量或数据契约、独立且实质复杂的操作、危险或不稳定的外部副作用边界，或者由多个独立真实调用方以相同语义复用。普通顺序步骤、单次底层库调用和只为消除少量重复而抽出的代码应保留在当前 Notebook/模块内。同名 `.py` 仍只能是该 Notebook 的完整导出结果。
- silver 业务 Notebook 必须直接使用 `config/data_contracts.py` 中的具名权威 Schema；不得增加 `SCHEMA = ...` 纯改名层。表名、主键和 Hive 分区在模块初始化时从该 Schema metadata 各读取一次并复用，不得平行硬编码，也不得在每个使用点重复解码。永久语义规则及示例以 `03_Futures_Database/AGENTS.md` 为准。

# 标准生成与验证

运行本目录树内任何业务工作流入口前，先使用量化目录统一环境预检入口验证标准
`latitude` 环境及核心 DataFrame 依赖：

```powershell
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/verify_runtime.py
```

`verify_runtime.py` 是验证脚本，不是业务工作流入口，不要求创建同名 Notebook。

重建后的 `a01_Data_Collection` 必须使用同目录的 `b00_sync_notebook_exports.py` 作为双轨生成与检查入口：

```powershell
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b00_sync_notebook_exports.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/b00_sync_notebook_exports.py --check
```

- `--write` 使用默认 PythonExporter 递归扫描四个正式 `b` 目录，重新生成其中 `c01` 起始业务 Notebook 的同名 `.py`，随后执行 Notebook 结构、脚本语法和逐字节检查；同步脚本不执行业务 Notebook 单元格，也不调用 API。
- `--check` 是默认模式，只检查而不写文件，适合交付复核和 CI。
- 该脚本只递归扫描 `b01_Futures_Market_Data`、`b02_Futures_Exchange_Reports`、`b03_External_Market_Data`、`b04_Macro_And_Interest_Rates`，不得扫描根级旧项目归档。修改业务 Notebook 后必须运行 `--write`；交付前必须运行 `--check`。
- `b00_sync_notebook_exports.py` 是运维与验证脚本，不是业务工作流入口，不要求创建同名 Notebook。

`a02_Feature_Engineering` 使用自身目录的 `b00_sync_notebook_exports.py`，只扫描本目录的
`b01` 起始业务 Notebook：

```powershell
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a02_Feature_Engineering/b00_sync_notebook_exports.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a02_Feature_Engineering/b00_sync_notebook_exports.py --check
```

当前主力连续实验的局部输出结构、gold 存放位置、log 双向复权与期限结构边界以
[a02_Feature_Engineering/README.md](a02_Feature_Engineering/README.md) 为准。

以下代码是该脚本及本目录树内其他允许修改业务入口从 `.ipynb` → `.py` 的唯一底层生成方式。不得对只读旧项目执行导出，也不得传入自定义模板、preprocessor 或其他改变导出文本的配置：

```python
from pathlib import Path

import nbformat
from nbconvert.exporters import PythonExporter

notebook_path = Path("path/to/task.ipynb")
script_path = notebook_path.with_suffix(".py")

notebook = nbformat.read(str(notebook_path), as_version=4)
nbformat.validate(notebook)
script_source, _ = PythonExporter().from_notebook_node(notebook)
script_path.write_text(script_source, encoding="utf-8", newline="\n")

if script_path.read_bytes() != script_source.encode("utf-8"):
    raise RuntimeError(".py 与 PythonExporter 导出结果不一致")
```

# Agent 完成条件

- Agent 新增、实质修改、移动或重命名任何业务工作流入口前，必须先查找同名另一轨；若缺失，必须在同一次任务中补齐。
- 所有业务改动必须先落实到 `.ipynb`，随后用默认 PythonExporter 重新生成 `.py`。若任务起点是对 `.py` 的修改，必须先把改动迁回 Notebook，再重新导出；不得保留手改脚本。
- 交付前至少确认两个同名文件均存在，使用 `nbformat.validate()` 校验 Notebook，并逐字节比较 `.py` 与默认 PythonExporter 的输出；比较不一致不得交付。
- 导出后的 `.py` 还必须通过语法检查；能够安全执行时再运行相应验证。不得通过格式化导出的 `.py` 修复问题，必须回到 Notebook 修改并重新导出。
- 存量单轨工作流不要求在无关任务中一次性批量改造，但一旦进入该工作流做实质修改，就必须先补齐双轨再完成任务。
- 查找存量单轨、导出不一致或语法问题时必须排除根级 `04_Old_Projects`；不得对归档项目进行补轨、重导出或修复。

# 生产数据更新的手动触发规则

- `a01_Data_Collection` 的生产数据更新采用“人工启动、程序按状态批量执行”的半自动模式。每一批更新都必须由操作者在本机显式运行命令；脚本内部可以遍历分区、检查配额、记录完成状态并支持断点续拉。
- 采集业务入口不设置独立的 API 调用开关。命令启动即执行采集与契约校验；保留 `--write` 作为唯一的“是否写入”语义，不带 `--write` 时不得提交数据湖或回写状态。
- `.env` 的 `FUTURES_LAKE_ROOT` 是正式期货湖仓根目录的唯一配置来源；来源原文、稳定表和实验性输出分别位于其 `raw`、`silver`、`gold` 子目录。生产入口必须通过 `settings.futures_lake_root` 引用。显式 `--lake-root` 只能作为非正式临时湖覆盖，不能形成第二个正式路径常量。
- 默认生产更新范围必须由程序计算：`上游当前有效格点 − 下游已经完整落盘的格点`。不得只从下游最大日期续写；尾部新增、历史内部空洞及未通过完整性检查的格点都属于待补范围，空库则自然得到全量范围。
- `b03/c04_external_index` 还必须计算正式外部指数事实中已经退出当前 `external_index` required 水位的格点；这些格点不调用 Eastmoney API，而从旧事实自身所在的完整 `index_category/year/month` 叶分区清退。此规则只约束该表，不改变期货事实白名单缩减时保留历史事实的既有政策。
- 上游表的完整业务语义由其生产者在提交链路中验证。消费者必须检查权威 Schema/metadata 和自身直接依赖的主键、范围、覆盖等边界，但不得为了防御性重复而复制上游的来源枚举、派生字段复算或其他完整表级质检；上游语义缺陷应回到上游生产者修复。
- 写入 `FUTURES_LAKE_ROOT` 指向的正式湖时，不得同时接受操作者指定的起止日期或其他手工截断范围。带显式日期的命令只能只读检查，或写入解析后明确不同于正式湖根目录的临时/测试湖。
- `b01/c05`、`b01/c06` 与 `b03/c03` 对来源 OHLC 的硬门禁区分“非有限数”和“有限数之间的 high/low 跨列异常”：NaN/Inf 仍禁止提交；有限的跨列异常不得修写或丢弃，必须按来源原值落盘，并在对应 1d 格点、1m Session 或外部市场请求日期继续记录 `warning`。该 warning 是已经完成且正式复读成功的来源质量异常，不得改写成数据缺失、请求失败或 `passed`。
- `b03/c02_domestic_spot_basis` 的长期职责只是归档生意社 HTTP `response.content` 原始字节及 SHA-256 sidecar，不解析 HTML、不提取字段、不生成结构化现货基差事实。正式目录固定为 `raw/100ppi/domestic_spot_basis/year=YYYY/month=MM/observation_date=YYYY-MM-DD/{response.html,response.sha256}`。待办集合是 `required 日期 −（原文字节与 sidecar 共同完整且外部市场日历状态完整的日期）`；原文完整但日历缺失或陈旧时必须从正式 raw 无 API 修复状态。HTTP 200 的任意正文在原文与摘要正式复读成功后统一记为 `success`、`record_count=1`、`passed`，不得依据正文为空、HTML 结构或业务内容另作判定。页面结构监测、历史重采、解析、字段提取和结构化事实生产属于未来独立项目；旧 `fact_domestic_spot_basis_daily` 若存在，不得由当前入口自动删除、迁移或改写。
- 当前 `b01_Futures_Market_Data` 的 `c01_trade_calendar` 至 `c08_full_minute_quality`、`b02_Futures_Exchange_Reports/c01_exchange_report_calendar` 至 `c03_warehouse_receipt`、`b03/c01_external_market_calendar` 至 `b03/c04_external_index`，以及 `b04/c01_macro_release_calendar` 至 `b04/c03_macro_release` 已完成逐文件迁移；其中 b01/c08 是无日期过滤、无外部 API 的独立手工全量审计，不属于日常自动差集入口；b02/c01 不调用外部 API，只从完整品种日历自动维护三类报告格点和事实采集状态；b02/c02 对共同待办格点只查询一次 JQData `finance.FUT_MEMBER_POSITION_RANK`，同时生成排名与明确参与者类型汇总两张事实；b02/c03 按仓单 required 格点自动求差，查询 JQData `finance.FUT_WAREHOUSE_RECEIPT`，保存数量、来源单位及较昨日变化。两类事实入口都只在事实正式复读后回写相应日历状态。b03/c01 不调用外部 API，从完整自然日历和 `config/external_market_entities.py` 维护现货、境外期货全表与外部指数的完整理论请求格点，规则未变时继承下游状态；b03/c02 按上条 raw 原文与日历的共同完整性自动补缺或无 API 修复状态；b03/c03 对 `overseas_futures/ALL` required 日期先分流：正式事实缺失或不能证明完整的日期才进入 API 待办，每个待办日期只查询一次 JQData `finance.FUT_GLOBAL_DAILY` 全表；正式事实已经完整但日历状态或原因陈旧的日期进入无 API 状态修复集合，必须从正式事实复算行数与有限 OHLC warning、提交并复读日历，不得重拉。来源 ID、请求日、5000 行上限和非有限值继续严格检查，有限 OHLC 跨列异常原值保留并以 `warning` 留痕；b03/c04 按 `external_index/INDICATOR_ID` required 格点自动求差，以共享配置逐指标分页请求 Eastmoney `RPT_INDUSTRY_INDEX`，严格检查分页计数、来源映射、日期和有限值，只接纳精确待办日期，并把不再属于当前 required 水位的旧事实无 API 清退；完整指数分类—年月分区经 staging 与正式路径复读后，才回写成功、确认空或失败状态。b04/c01 不调用 API，从 `FUTURES_DATA_START_DATE` 到北京时间当前日按 `config/macro_release_entities.py` 生成 8 个 SHIBOR 普通工作日格点和 17 个宏观月末/季末格点，逐值计算版本化可用日，继承政策未变化的事实状态，并以完整 `dataset_name/year/month` 叶分区提交和复读；旧 metadata 或旧规则只允许在无日期自动模式中整表迁移。b04/c02 以 `interest_rate` required 系列—日期格点减去正式 SHIBOR 事实与日历状态共同证明完整的格点；正式事实完整但日历陈旧时无 API 修复，事实缺口按年月窗口调用 Tushare `pro.shibor`，只接纳精确待办期限并保留同月未触达事实，完整事实叶正式复读后才回写成功或确认空；单次不得超过 2000 行，非空利率必须有限且位于 `[-100, 100]` 百分比硬边界。b04/c03 以 `macro_release` required 系列—报告期格点自动求差或无 API 修复日历；事实缺口按 Eastmoney 报告名与年月严格分页，只接纳精确待办格点，将来源报告月 1 日归一到项目月末/季末，PPI 同比读取 `BASE_SAME`，CPI/PPI 累计指数按共享偏移减 100 后保存为累计同比百分比；完整事实叶正式复读后才回写成功或确认空。至此 18 个正式采集入口均已完成迁移。
- 禁止由 LLM/Agent 心跳、定时任务、线程唤醒、cron、后台守护程序或其他无人值守机制发起下一批生产更新。达到 API 配额边界后必须正常停止，等待操作者在额度重置后手动再次执行。
- LLM/Agent 不作为常规生产更新执行者，不得仅因历史对话中的“继续”或既有计划而在未来自动恢复拉取；可以检查代码、解释状态和提供人工执行命令。只有用户在当前交互中明确要求执行具体批次时，才可在该次授权范围内运行。
- 分钟线全部完成后的缺失检测和全链路验证同样由操作者手动触发，不得隐式串接到定时任务。

# 默认例外

## 重建前期货 silver 历史迁移

历史湖迁移工具只允许位于 `00_draft_collection_02/scripts/migrate_pre_rebuild_futures_lake.py`，正式归属未获用户接纳前不得移入业务目录。工具只读本地 Parquet，不调用 API；默认 `--plan`，`--execute` 必须在全量预检和复核后人工启动，`--recover` 用于事务失败恢复。`fact_futures_daily` 的历史来源标签 `JQData_get_price_1d_skip_paused` 与 `fact_futures_minute` 的 `JQData_get_price_1m`、`JQData_get_price_1m_skip_paused` 必须永久保留并接受；新采集仍只写当前来源标签。迁移工具、隔离测试与计划输出均以根级 `AGENTS.md`、`03_Futures_Database/AGENTS.md` 和 `config/data_contracts.py` 为准。

- `__init__.py`、配置模块、共享库模块、测试、验证或运维脚本（包括 `a01_Data_Collection/b00_sync_notebook_exports.py` 和 `a02_Feature_Engineering/b00_sync_notebook_exports.py`）、第三方或 vendored 源码、自动生成文件默认不要求创建同名 Notebook。
- 若文件实际承担可独立运行的业务任务，或用户及所属目录规范明确将其列为业务工作流入口，则不得以文件名或所在目录为由套用例外；根级 `04_Old_Projects` 仍服从其独立的递归只读规则。

# 规范索引与同步

- [根目录 AGENTS.md](../AGENTS.md)：项目运行环境、根目录定位和规范路由的项目级强制规则。
- [.env.template](../.env.template)：项目根目录定位代码、当前稳定采集统一正式起点，以及包含 `raw`、`silver`、`gold` 的正式湖仓根路径环境变量权威模板。
- [03_Futures_Database/AGENTS.md](../03_Futures_Database/AGENTS.md)：数据库目录级 Agent 规则，以及字段命名、跨引擎类型与数据湖 Schema 文本规范。
- [a01_Data_Collection/README.md](a01_Data_Collection/README.md)：当前国内期货数据采集链路、Notebook 语义浏览、具体双轨清单及 `b00` 同步入口。
- [a01_Data_Collection_Rebuild_Blueprint/README.md](a01_Data_Collection_Rebuild_Blueprint/README.md)：冻结的一次性历史实施记录；其中与当前 17 表契约或生意社 raw 原文归档政策不一致的旧设计由当前规范和可执行契约覆盖。
- [a01_Data_Collection_Rebuild_Blueprint/08_EXECUTION_CHECKLIST.md](a01_Data_Collection_Rebuild_Blueprint/08_EXECUTION_CHECKLIST.md)：强制先完成全部代码和 CODE-GATE，再编写及执行数据迁移。
- [根级旧项目归档规则](../04_Old_Projects/AGENTS.md)：重建前采集实现、更早历史采集项目与旧特征工程项目的递归只读规则。
- [a02_Feature_Engineering/README.md](a02_Feature_Engineering/README.md)：主力连续合约、log 双向复权、期限结构边界及当前 gold 实验工作流说明。
- [config/futures_fact_collection_policy.py](../config/futures_fact_collection_policy.py)：国内期货日线、分钟线和逐品种交易所报告事实采集白名单的项目级唯一权威来源；所有调用方必须从 `config` 导入，不得在业务目录复制，也不得据此裁剪维度或研究宇宙。
- [config/external_market_entities.py](../config/external_market_entities.py)：外部市场请求实体、Eastmoney 指标 ID—项目代码映射与有效期的项目级唯一权威来源；c01 日历和 c04 事实必须共用，业务目录不得复制。
- [config/macro_release_entities.py](../config/macro_release_entities.py)：b04 宏观发布日历、SHIBOR 与宏观事实共用的 25 个系列、来源列、宏观数值偏移、理论频率及版本化可用日规则唯一权威来源；业务目录不得复制。
- [config/jqdata_connection.py](../config/jqdata_connection.py)：JQData 认证及 Windows TUN 直连处理的共享边界；所有调用方必须从 `config` 导入。
- [config/data_contracts.py](../config/data_contracts.py)：17 张稳定 silver 数据湖 Schema 及 DataFrame 类型转换的可执行契约。
- 修改本文件时，必须同步检查根目录规范索引和受影响子目录的规范；不得形成只在本文件可见的孤立约束。
- 修改数据湖字段、Schema、生产者、读取者、类型转换或相关验证时，必须同时读取并遵循 `03_Futures_Database/AGENTS.md` 与 `config/data_contracts.py`。
