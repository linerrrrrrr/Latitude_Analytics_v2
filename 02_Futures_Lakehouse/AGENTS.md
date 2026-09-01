# 适用范围与规范地位

- 本文件是 `E:\Latitude_Analytics_v2\02_Futures_Lakehouse` 整棵生产与运维目录树的目录级 Agent 规则入口，适用于目录本身以及任意层级的当前和未来子目录。
- 本文件用于集中定义或索引期货湖仓生产与运维目录的全部 Agent 规则；`.ipynb/.py` 双轨及 PythonExporter 是 `a01_Data_Collection` 的业务入口规则，不覆盖纯控制面的 operations 脚本。
- 当前范围包括 `a01_Data_Collection`、`a02_Data_Collection_Operations` 及其全部后代目录。特征工程已经独立迁移到根级 `04_Feature_Engineering`；旧采集和旧特征工程项目位于根级 `05_Old_Projects`，均不属于本目录树。
- 未来新增期货湖仓目录级规则时，应直接加入本文件或由本文件建立明确索引，不得另建无法从这里发现的孤立规范。
- 本目录继承根目录 [AGENTS.md](../AGENTS.md) 的全部规则。下级 `AGENTS.md` 或 README 可以增加更严格的要求，但未经用户明确同意，不得豁免、弱化或缩小本文件的覆盖范围。

# 根级旧项目归档路由

- `E:\Latitude_Analytics_v2\05_Old_Projects` 保存重建前采集实现、更早历史采集项目和旧特征工程项目；其递归只读规则以 [05_Old_Projects/AGENTS.md](../05_Old_Projects/AGENTS.md) 为准。
- Agent 只允许读取、搜索、比较和分析其中的内容。禁止在该目录内新增、修改、删除、移动、重命名、格式化或重新导出任何文件，也禁止补齐 `.ipynb/.py` 双轨、修复旧代码、写入缓存、测试输出、数据文件或新的规范文本。
- 下文的双轨、PythonExporter、完成条件和存量迁移规则均不得作为修改该旧项目的依据。需要复用旧项目思路时，只能把必要逻辑迁入当前可写目录，并在当前目录按适用规范实现。
- 不得执行可能在该目录内产生 `__pycache__`、Notebook checkpoint、日志或其他副作用的旧项目代码；只读检查必须使用不会写回该目录的方式。
- 只有用户明确点名该目录中的具体目标并明确授权修改时，才可在该次任务的授权范围内例外处理。笼统的“实现双轨”“同步整个 `02_Futures_Lakehouse`”或一般重构请求不构成修改旧项目的授权。

# `.ipynb` 与 `.py` 双轨强制规则

- `02_Futures_Lakehouse/a01_Data_Collection` 下所有项目自研的数据采集业务工作流入口都必须同时保留同一目录、同一基名的 `.ipynb` 与 `.py`，例如 `c01_trade_calendar.ipynb` 与 `c01_trade_calendar.py`。
- 业务工作流入口包括数据采集、清洗与加工、因子研究、策略研究、回测、建模、评估、报告和可视化等可独立运行的任务；当前由哪种文件起步不影响双轨要求。只读旧项目不属于本规则的改造范围。
- `.ipynb` 用于交互式分步执行、观察中间结果和记录业务解释；`.py` 用于批量执行、模块导入、测试和调度。两者都是正式交付物，未经用户明确同意不得删除、遗漏或用其中一轨替代另一轨。
- `.ipynb` 是唯一允许直接编辑的源文件；同名 `.py` 必须使用项目标准 `latitude` 环境中的默认 `nbconvert.exporters.PythonExporter` 完整生成，并与导出结果逐字节一致。
- 两轨必须保持核心业务逻辑一致，包括参数与默认值、数据源、字段和 Schema、路径、过滤条件、更新水位及输出语义。Notebook 输出以及 PythonExporter 未导出的元数据可以只存在于 `.ipynb`；Markdown、代码单元格顺序和展示代码若会进入导出结果，就必须同步出现在 `.py` 中。
- 不得直接修改或重新格式化导出的 `.py`；PythonExporter 生成的 shebang、编码声明、`# In[...]` 标记、Markdown 注释、空行与单元格顺序必须原样保留。
- 共享模块必须符合根目录 `AGENTS.md` 的抽象判定：承担项目级不变量或数据契约、独立且实质复杂的操作、危险或不稳定的外部副作用边界，或者由多个独立真实调用方以相同语义复用。普通顺序步骤、单次底层库调用和只为消除少量重复而抽出的代码应保留在当前 Notebook/模块内。同名 `.py` 仍只能是该 Notebook 的完整导出结果。
- silver 业务 Notebook 必须直接使用 `config/data_contracts.py` 中的具名权威 Schema；不得增加 `SCHEMA = ...` 纯改名层。表名、主键和 Hive 分区在模块初始化时从该 Schema metadata 各读取一次并复用，不得平行硬编码，也不得在每个使用点重复解码。变量命名与是否需要改名遵循根目录 `AGENTS.md` 的“变量命名与最小改动”；可以为实际业务身份、来源、处理角色、契约状态和对象表示保留稳定共同词根，并使用 `existing_*`、`expected_*`、`pending_*`、`validated_*` 以及 `*_df`、`*_table`、`*_dataset`、`*_schema` 等有独立语义的修饰词或后缀。DataFrame、Arrow Table 和 Dataset 之间的表示转换本身不改变业务角色，应保留原有角色修饰词与业务词根并只替换表示后缀，例如 `new_variety_calendar_df -> new_variety_calendar_table`；不得用 `incoming` 等近义词制造并不存在的阶段差异。禁止的是无语义堆叠和跨无关代码的风格清洗，不是显式命名本身。

# 标准生成与验证

运行本目录树内任何业务工作流入口前，先使用本目录的统一环境预检入口验证标准
`latitude` 环境及核心 DataFrame 依赖：

```powershell
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/verify_runtime.py
```

`verify_runtime.py` 是验证脚本，不是业务工作流入口，不要求创建同名 Notebook。

重建后的 `a01_Data_Collection` 必须使用同目录的 `b00_sync_notebook_exports.py` 作为双轨生成与检查入口：

```powershell
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b00_sync_notebook_exports.py --write
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/a01_Data_Collection/b00_sync_notebook_exports.py --check
```

- `--write` 使用默认 PythonExporter 递归扫描四个正式 `b` 目录，重新生成其中 `c01` 起始业务 Notebook 的同名 `.py`，随后执行 Notebook 结构、脚本语法和逐字节检查；同步脚本不执行业务 Notebook 单元格，也不调用 API。
- `--check` 是默认模式，只检查而不写文件，适合交付复核和 CI。
- 该脚本只递归扫描 `b01_Futures_Market_Data`、`b02_Futures_Exchange_Reports`、`b03_External_Market_Data`、`b04_Macro_And_Interest_Rates`，不得扫描根级旧项目归档。修改业务 Notebook 后必须运行 `--write`；交付前必须运行 `--check`。
- `b00_sync_notebook_exports.py` 是运维与验证脚本，不是业务工作流入口，不要求创建同名 Notebook。

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
- 查找存量单轨、导出不一致或语法问题时必须排除根级 `05_Old_Projects`；不得对归档项目进行补轨、重导出或修复。

# 生产数据更新的手动触发规则

- `a01_Data_Collection` 的生产数据更新采用“人工启动、程序按状态批量执行”的半自动模式。每一批更新都必须由操作者在本机显式运行命令；脚本内部可以遍历分区、检查配额、记录完成状态并支持断点续拉。
- 采集业务入口不设置独立的 API 调用开关。命令启动即执行采集与契约校验；保留 `--write` 作为唯一的“是否写入”语义，不带 `--write` 时不得提交数据湖或回写状态。
- `.env` 的 `FUTURES_LAKE_ROOT` 是正式期货湖仓根目录的唯一配置来源；来源原文与证据、稳定表和实验性输出分别位于其 `raw`、`silver`、`gold` 子目录。生产入口必须通过 `settings.futures_lake_root` 引用。显式 `--lake-root` 只能作为非正式临时湖覆盖，不能形成第二个正式路径常量。
- 来源异常的 raw 证据留存、silver 验收与持久 `warning` 统一服从 `03_Futures_Database/AGENTS.md`；下文的失败或拒绝提交均指 silver 验收失败，不得据此丢弃已经按生产者 raw 证据契约归档的载荷。某个生产者尚无经确认的 raw 证据契约时，继续保留现有 silver 门禁，不得自行发明归档路径或静默放宽校验。
- 除已明确迁移的 b01 日常链路和 `b02/c03_warehouse_receipt` 外，默认生产更新范围仍由程序计算：`上游当前有效格点 − 下游已经完整落盘的格点`。b01/c01—c04 默认只处理可信正式水位之后的尾部新增，历史内部空洞、删除或修订由显式 `--full` 处理；c05/c06 只以当前白名单和可信 `is_fetch_completed` 形成待办，不从全历史事实重建完成状态。空库仍由同一默认入口自然得到全量范围。
- `b03/c04_external_index` 还必须计算正式外部指数事实中已经退出当前 `external_index` required 水位的格点；这些格点不调用 Eastmoney API，而从旧事实自身所在的完整 `index_category/year/month` 叶分区清退。此规则只约束该表，不改变期货事实白名单缩减时保留历史事实的既有政策。
- 上游表的完整业务语义由其生产者在提交链路中验证；正式提交成功即证明上游主键、水位、覆盖和其他权威业务约束已经成立。消费者必须信任该证明，只确认权威 Schema/metadata 物理兼容，并检查未由上游契约保证、但确属自身计算前提的局部边界；不得防御性重复上游主键、日期连续性、来源枚举、派生字段或其他表级质检。上游语义缺陷应回到上游生产者修复。代码收缩仍按用户确认逐脚本进行，不得据此顺带批量修改其他入口。
- 写入 `FUTURES_LAKE_ROOT` 指向的正式湖时不得用日期或月份截断自动生产水位。b01/c01、c02、c04 的 `--full` 与显式日期互斥，正式全历史维护必须使用 `--full --write`；b01/c03 允许成对日期或 `--full` 来源双向质检并以 `--write` 提交差异；b01/c07 只有带日期范围或合约范围的 `--force --write` 才能定向回写正式旁证。其他入口显式日期只能只读检查，或写入不同于正式湖根目录的测试湖。
- `b01/c03_futures_contract_calendar` 默认只处理 c02 相对正式合约日历新增的交易日，不再用历史 `active_contract_count` 与 distinct `contract_code` 数差异触发日常 API；无有效 `trade_time` 的合约 warning 后跳过并推进日常水位。成对日期与 `--full` 分别检查指定范围和 c02 当前全部水位与本地整表，执行来源拉取和双向比较，`--write` 只提交发现的差异。c03 信任 c02 正式提交语义，仍完整验证自己的 API 响应和 dirty 输出叶。
- `b01/c05`、`b01/c06` 与 `b03/c03` 对来源 OHLC 都区分非空非有限值和有限数之间的 high/low 跨列异常。b01/c05、c06 的 NaN/Inf 仍禁止提交；b03/c03 将财务库可空数值列中的 `None`、`pd.NA` 和 Pandas `NaN` 缺失标记统一归一为 Arrow null，不另记 warning，非空非有限数仍禁止提交。有限的跨列异常不得修写或丢弃，必须按来源原值落盘，并在对应 1d 格点、1m Session 或外部市场请求日期继续记录 `warning`。该 warning 是已经完成的来源质量异常，日常不得重拉或改写成缺失、失败或 `passed`；c06 不得用 OHLC 质量覆盖 schedule 开市证据。
- `b02/c02` 串行按 `exchange_code + underlying_code + year + month` 归并报告日历待办日，以一次 JQData `finance.run_query(FUT_MEMBER_POSITION_RANK.day.in_(pending_dates))` 请求月份待办子集；返回恰好 5000 行时必须丢弃截顶响应并按排序日期确定性二分，单日仍触顶则硬失败。来源 `rank` 必须是 `1—20` 的整数；Top 20 约束作用于每个具体合约的每类榜单，不得误写成一个品种日总共至多 20 行。入口只读取和提交当前事实与日历完整叶，以两张正式事实格点计数和两类报告日历完成状态共同证明完成；不提供 `--full`，也不使用分页、并发或业务自动重试。
- `b02/c02` 对 JQData 会员成交持仓排名的同一来源业务键重复行，只在原始会员标签、排名类别 ID/文字、指标和变化全部一致时按事实粒度合并，逐会员排名取最小名次，并把对应报告日历永久记为已完成的 `success + warning`；任一来源业务值冲突继续按 Schema 错误停止。该 warning 与正式事实计数共同构成可信完成凭证，日常不得因此重拉。
- `b02/c01a_position_rank_special_case_calibration` 固定先于 c02 运行，只处理 `config/futures_position_rank_special_cases.py` 列出的已核实案例。正式 raw 证据不存在时各请求一次冻结的交易所 URL，并在 HTTP、SHA-256、目标合约和完整 Top 20 全部精确匹配后原子提交 `response.dat`、`response.sha256` 与 `calibration.json`；证据存在时只正式复读。c01a 不写 silver、不发现或推断新异常、不自动重试。c02 仅当指定格点的 JQData 成交量榜完整 20 行精确等于冻结坏载荷时，才凭正式证据整组替换为交易所权威 Top 20；已等于权威载荷则原样通过，第三种载荷硬失败，持买仓、持卖仓及未配置格点不变。校准结果在报告日历永久记录含案例 ID 与原文摘要的 `success + warning`，不得因此日常重拉。
- `b03/c02_domestic_spot_basis` 的长期职责只是归档生意社 HTTP `response.content` 原始字节及 SHA-256 sidecar，不解析 HTML、不提取字段、不生成结构化现货基差事实。正式目录固定为 `raw/100ppi/domestic_spot_basis/year=YYYY/month=MM/observation_date=YYYY-MM-DD/{response.html,response.sha256}`。待办集合是 `required 日期 −（原文字节与 sidecar 共同完整且外部市场日历状态完整的日期）`；原文完整但日历缺失或陈旧时必须从正式 raw 无 API 修复状态。HTTP 200 的任意正文在原文与摘要正式复读成功后统一记为 `success`、`record_count=1`、`passed`，不得依据正文为空、HTML 结构或业务内容另作判定。页面结构监测、历史重采、解析、字段提取和结构化事实生产属于未来独立项目；旧 `fact_domestic_spot_basis_daily` 若存在，不得由当前入口自动删除、迁移或改写。
- 当前正式采集拓扑固定为 17 张稳定 silver 表、19 个正式采集入口（包含只能人工显式运行的 b01/c08）和 18 个默认日常阶段。默认 worker 中 b01 固定执行 c01—c07；`--skip-optional-quality` 只跳过 c07，c08 从所有默认 worker manifest 退出，只能人工显式 `--confirm-full-quality --write`。c01—c04 默认尾部增量，历史维护使用 `--full`；c05/c06 每次窄列向量重算当前白名单，只拉取未完成格点；c07 默认只处理新产生且尚未校对的 `suspected_closed` Session，不用输入指纹扫描历史变化。b02—b04 仍按其既有逐表差集、正式复读和状态回写契约运行，本次 b01 改写不改变这些业务入口。
- `b01/c08` 的全量审计边界固定为 `c04 required 且已完成的理论分钟主键 − c06 正式分钟事实的 contract_code、bar_at 主键`。它不读取行情值、不重复检查 c06 的事实质量、不调用 c07；只回写日历的实际/缺失计数与检查时间，并逐值保留 c06 质量结论、c07 旁证及完成状态。
- 禁止由 LLM/Agent 心跳、定时任务、线程唤醒、cron、后台守护程序或其他无人值守机制发起下一批生产更新。达到 API 配额边界后必须正常停止，等待操作者在额度重置后手动再次执行。
- LLM/Agent 不作为常规生产更新执行者，不得仅因历史对话中的“继续”或既有计划而在未来自动恢复拉取；可以检查代码、解释状态和提供人工执行命令。只有用户在当前交互中明确要求执行具体批次时，才可在该次授权范围内运行。
- 分钟线全部完成后的缺失检测和全链路验证同样由操作者手动触发，不得隐式串接到定时任务。
- 根级“长时间任务”规则适用于本目录全部采集、审计与迁移命令。用户当前明确授权的有界批次可以由 detached worker 顺序执行，同时必须开启独立可见且无 LLM 的 Terminal monitor，并在失败或配额停止时终止，不自动恢复。运行时长、正式湖写入和 detached 执行均不要求先跑非正式小样本；样本、测试湖演练及仅因运行时长追加的 dry-run 都是只有用户明确要求才执行的可选检查。该人工启动的单批 worker 不构成定时任务、守护服务或未来运行授权。
- 采集 worker 与 PowerShell monitor 共享 `status.json` 时必须遵守根级 Windows 控制面规则：monitor 以允许删除/替换的共享句柄读取并立即关闭，worker 以 fsync 后原子替换发布状态，仅对替换本身的 `WinError 5/32` 做有界短重试。2026-08-21 的 b01-b04 样本曾因普通 monitor 读取锁阻断原子替换而误停 c06；禁止恢复会重现该冲突的 `Get-Content` 持续读取实现，状态通道最终失败必须主动停止业务 child。

# 默认例外

## 重建前期货 silver 历史迁移

历史湖迁移工具只允许位于 `00_draft_collection_02/scripts/migrate_pre_rebuild_futures_lake.py`，正式归属未获用户接纳前不得移入业务目录。工具只读本地 Parquet，不调用 API；默认 `--plan`，`--execute` 必须在全量预检和复核后人工启动，`--recover` 用于事务失败恢复。`fact_futures_daily` 的历史来源标签 `JQData_get_price_1d_skip_paused` 与 `fact_futures_minute` 的 `JQData_get_price_1m`、`JQData_get_price_1m_skip_paused` 必须永久保留并接受；新采集仍只写当前来源标签。迁移工具、隔离测试与计划输出均以根级 `AGENTS.md`、`03_Futures_Database/AGENTS.md` 和 `config/data_contracts.py` 为准。

- `__init__.py`、配置模块、共享库模块、测试、验证或运维脚本（包括 `a01_Data_Collection/b00_sync_notebook_exports.py` 和 `a02_Data_Collection_Operations` 中不承载业务采集逻辑的 worker、monitor 与状态工具）、第三方或 vendored 源码、自动生成文件默认不要求创建同名 Notebook。
- operations 只负责人工启动、阶段编排、子进程监督、可见监控、状态证据、失败停止、现场保留与人工处置，不得复制或内嵌 19 个正式业务入口的采集、转换、Schema 或提交逻辑；其更严格边界以 [a02_Data_Collection_Operations/AGENTS.md](a02_Data_Collection_Operations/AGENTS.md) 为准。
- 若文件实际承担可独立运行的业务采集任务，或用户及所属目录规范明确将其列为业务工作流入口，则不得以文件名或所在目录为由套用例外；根级 `05_Old_Projects` 仍服从其独立的递归只读规则。

# 规范索引与同步

- [根目录 AGENTS.md](../AGENTS.md)：变量命名与最小改动、项目运行环境、根目录定位和规范路由的项目级强制规则。
- [.env.template](../.env.template)：项目根目录定位代码、当前稳定采集统一正式起点，以及包含 `raw`、`silver`、`gold` 的正式湖仓根路径环境变量权威模板。
- [03_Futures_Database/AGENTS.md](../03_Futures_Database/AGENTS.md)：数据库目录级 Agent 规则，以及来源异常 raw/silver 边界、数据字段命名、跨引擎类型与数据湖 Schema 文本规范。
- [a01_Data_Collection/README.md](a01_Data_Collection/README.md)：19 个正式采集入口（含人工 c08）的采集链路、来源异常留存与验收语义、Notebook 开篇 Schema 契约呈现、具体双轨清单及 `b00` 同步入口。
- [a02_Data_Collection_Operations/AGENTS.md](a02_Data_Collection_Operations/AGENTS.md)：18 个默认日常阶段的人工启动、detached worker、可见 monitor、原子状态发布、失败停止、现场保留与人工核查边界。
- [a02_Data_Collection_Operations/README.md](a02_Data_Collection_Operations/README.md)：正式 worker、monitor 与有界单批运行的操作入口和状态证据说明。
- [根级旧项目归档规则](../05_Old_Projects/AGENTS.md)：重建前采集实现、更早历史采集项目与旧特征工程项目的递归只读规则。
- [独立特征工程 AGENTS.md](../04_Feature_Engineering/AGENTS.md)：下游 silver 消费、双轨与当前结构迁移阻塞规范。
- [独立特征工程 README](../04_Feature_Engineering/README.md)：主力连续合约、log 双向复权、期限结构边界及当前 gold 实验工作流说明。
- [config/futures_fact_collection_policy.py](../config/futures_fact_collection_policy.py)：国内期货日线、分钟线和逐品种交易所报告事实采集白名单的项目级唯一权威来源；所有调用方必须从 `config` 导入，不得在业务目录复制，也不得据此裁剪维度或研究宇宙。
- [config/futures_position_rank_special_cases.py](../config/futures_position_rank_special_cases.py)：已人工核实的成交持仓排名特殊案例、完整坏载荷指纹、交易所原文摘要与完整校准值的项目级唯一配置来源；c01a 与 c02 必须共用，业务目录不得复制。
- [config/external_market_entities.py](../config/external_market_entities.py)：外部市场请求实体、Eastmoney 指标 ID—项目代码映射与有效期的项目级唯一权威来源；c01 日历和 c04 事实必须共用，业务目录不得复制。
- [config/macro_release_entities.py](../config/macro_release_entities.py)：b04 宏观发布日历、SHIBOR 与宏观事实共用的 25 个系列、来源列、宏观数值偏移、理论频率及版本化可用日规则唯一权威来源；业务目录不得复制。
- [config/jqdata_connection.py](../config/jqdata_connection.py)：JQData 认证及 Windows TUN 直连处理的共享边界；所有调用方必须从 `config` 导入。
- [config/data_contracts.py](../config/data_contracts.py)：17 张稳定 silver 数据湖 Schema 及 DataFrame 类型转换的可执行契约。
- 修改本文件时，必须同步检查根目录规范索引和受影响子目录的规范；不得形成只在本文件可见的孤立约束。
- 修改数据湖字段、Schema、生产者、读取者、类型转换或相关验证时，必须同时读取并遵循 `03_Futures_Database/AGENTS.md` 与 `config/data_contracts.py`。
