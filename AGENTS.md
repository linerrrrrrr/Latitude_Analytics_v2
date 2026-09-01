# 规范索引与同步要求

- [AGENTS.md](AGENTS.md)：变量命名与最小改动、项目运行环境、根目录定位与规范路由的项目级强制规则。
- [.env.template](.env.template)：项目根目录定位代码、当前稳定采集统一正式起点，以及包含 `raw`、`silver`、`gold` 的正式湖仓根路径环境变量权威模板。
- [02_Futures_Lakehouse/AGENTS.md](02_Futures_Lakehouse/AGENTS.md)：`E:\Latitude_Analytics_v2\02_Futures_Lakehouse` 整棵生产与运维目录树的目录级 Agent 规则入口，包含采集双轨、PythonExporter、`b00` 同步入口、正式 operations 路由及根级旧项目归档路由。
- [03_Futures_Database/AGENTS.md](03_Futures_Database/AGENTS.md)：`E:\Latitude_Analytics_v2\03_Futures_Database` 整棵目录树的目录级 Agent 规则入口，以及来源异常 raw/silver 边界、正式湖与项目专属研究成果边界、数据字段命名、跨引擎类型、Schema metadata 单一来源与 Notebook 开篇 Schema 契约呈现的永久文本规范。
- [03_Futures_Database/read_futures_lake_demo.ipynb](03_Futures_Database/read_futures_lake_demo.ipynb)：17 张稳定 silver 表的契约化读取示例；每张表必须由独立代码单元格演示；另含不计入 silver 表数的生意社 raw 原文与摘要核对示例。
- [02_Futures_Lakehouse/a01_Data_Collection/README.md](02_Futures_Lakehouse/a01_Data_Collection/README.md)：19 个正式采集入口（含人工 c08）的数据采集链路、来源异常留存与验收的运行语义、双轨同步入口、Schema metadata 运行时读取、Notebook 开篇 Schema 契约呈现、表粒度、主键、分区与更新水位规范。
- [02_Futures_Lakehouse/a02_Data_Collection_Operations/AGENTS.md](02_Futures_Lakehouse/a02_Data_Collection_Operations/AGENTS.md)：18 个默认日常阶段的人工启动、detached worker、可见 monitor、状态发布、失败停止、现场保留与人工核查边界的正式运维规范。
- [02_Futures_Lakehouse/a02_Data_Collection_Operations/README.md](02_Futures_Lakehouse/a02_Data_Collection_Operations/README.md)：正式 worker、monitor 与有界单批运行的操作入口和状态证据说明。
- [04_Feature_Engineering/AGENTS.md](04_Feature_Engineering/AGENTS.md)：独立特征工程项目的双轨、silver 消费与当前结构迁移阻塞规范。
- [04_Feature_Engineering/README.md](04_Feature_Engineering/README.md)：主力连续合约、log 双向复权、期限结构边界及当前 gold 实验工作流说明；当前仅完成结构迁移，不代表业务入口已经恢复运行。
- [01_project_collection/china_futures_market_evolution_reproduction/AGENTS.md](01_project_collection/china_futures_market_evolution_reproduction/AGENTS.md)：中国期货市场演变方法复现项目的只读 silver 消费、方法说明路由与局部执行规则。
- [01_project_collection/china_commodity_futures_intraday_volatility_forecasting_reproduction/AGENTS.md](01_project_collection/china_commodity_futures_intraday_volatility_forecasting_reproduction/AGENTS.md)：中国商品期货日内波动预测方法复现项目的只读 silver 消费、项目专属版本化研究成果、项目内长批次控制、固定研究口径、逐项 Notebook 工作方式与局部执行规则。
- [config/futures_fact_collection_policy.py](config/futures_fact_collection_policy.py)：日线、分钟线和逐品种交易所报告共用的国内期货事实采集白名单唯一权威来源；白名单只含明确列出的交易所—品种，不得把它解释为品种、合约、日历或研究宇宙。
- [config/futures_position_rank_special_cases.py](config/futures_position_rank_special_cases.py)：已经人工核实的成交持仓排名来源特殊案例、完整坏载荷指纹、上期所权威原文摘要与校准值唯一配置来源；配置不调用 API、不决定是否写入。
- [config/external_market_entities.py](config/external_market_entities.py)：外部市场日历与外部指数事实共用的请求实体、Eastmoney 指标映射及有效期唯一权威来源；配置不调用 API、不决定是否写入。
- [config/macro_release_entities.py](config/macro_release_entities.py)：宏观发布日历、SHIBOR 与宏观事实共用的 25 个系列、来源列、宏观数值偏移、理论频率及版本化可用日规则唯一权威来源；配置不调用 API、不决定是否写入。
- [config/jqdata_connection.py](config/jqdata_connection.py)：JQData 认证以及 Windows TUN 物理出口绑定的项目级共享连接边界；业务采集与是否写入仍由各业务入口负责。
- [config/data_contracts.py](config/data_contracts.py)：17 张稳定 silver 数据湖 Schema（7 张日历维度表、10 张事实表）、表名/主键/分区 metadata 单一来源及 Pandas、Polars、Arrow 转换的可执行契约。
- [00_draft_collection_02](00_draft_collection_02)：尚未经用户确认接纳为正式项目代码的脚本、测试、审计与验证工具的统一暂存目录。
- [05_Old_Projects/AGENTS.md](05_Old_Projects/AGENTS.md)：根级旧项目只读归档规则；包含重建前采集实现、更早历史采集项目和旧特征工程项目。
- 修改以上任一规范、模板或可执行契约前，必须检查其余索引项，并在同一次变更中同步所有受影响的描述、示例与代码。
- 新增具有规范作用的文本时，必须将其加入本索引，并在其他相关规范文本中添加反向索引；不得形成无法从本索引发现的孤立规范。
- 若不同规范之间存在冲突，必须先消除冲突再完成任务，不得选择性遵循其中一份。

# 变量命名与最小改动

## 判断顺序

- 变量名首先服务于语义清晰、身份连续和类型可辨识，不以字符最少为目标。在不丢失重要业务身份、处理角色、契约状态或对象表示的前提下，再选择最紧凑的名称。
- “最小改动”主要约束任务范围：不得因为修改一个工作流而顺带扫描、重命名无关模块。它不表示旧短名天然优先，也不禁止在当前已触及的完整工作流内，为消除含混、保持共同词根或显式区分表示而成组改名。
- 不仅判断单个名称“能否看懂”，还要判断它在函数边界、较长流程、日志、异常、调试器和搜索结果中能否仍然独立表意。显式名称能稳定降低跳读或误用风险时，可以重命名已有的通用短名。

## 业务身份、处理角色与对象表示

* 同一业务实体在同一工作流中应保留稳定的业务词根。变量名优先按“角色或状态 + 业务实体 + 表示类型”组织，例如 `expected_calendar_df`、`validated_calendar_table`。
* 对象表示或类型转换本身不改变业务身份和处理角色。DataFrame、Arrow Table、Dataset 之间转换时，默认只改变表示后缀，例如 `new_variety_calendar_df -> new_variety_calendar_table`；不得仅因 `pandas_to_arrow()` 等转换改称 `incoming_*`。
* 只有来源、职责、生命周期或已保证的后置条件确实发生变化时，才新增或替换角色修饰词，例如 `calendar_table -> validated_calendar_table`。不得用近似含义的词替换来伪装阶段变化。
* 跨越具有独立契约的函数或组件边界时，可以按该组件中的处理角色命名，例如调用侧的 `new_calendar_table` 可作为 `commit_partition(incoming_table=...)` 传入。
* 同一实体存在多种表示或作用域较长时，应明确使用 `*_df`、`*_table`、`*_dataset`、`*_schema`、`*_path`、`*_dir` 等后缀。避免 `data`、`result`、`frame`、`checked` 等无法表达业务身份的泛化名称。

## 已确认的项目偏好

- 在 `b01/c01_trade_calendar` 这类同时包含 Pandas、Arrow、Dataset、Schema、多个日历状态和提交阶段的工作流中，`calendar_table`、`new_calendar_df`、`validated_calendar_table`、`existing_calendar_dataset`、`existing_calendar_table`、`existing_calendar_schema`、`requested_calendar_df`、`expected_calendar_df`、`pending_calendar_df`、`valid_calendar_dates`、`incomplete_calendar_dates` 和 `expected_calendar_signature_by_date` 都是合理且应保留的显式命名。不得仅为恢复旧名、缩短名称或减少类型后缀而改回 `table`、`frame`、`checked`、`existing`、`valid_dates` 或 `expected_signature_by_date`。
- 在 `b01/c02_futures_variety_calendar` 中，`pandas_to_arrow()` 只把同一份新生成的品种日历从 Pandas 表示转换为符合权威 Schema 的 Arrow 表示，因此应保持 `new_variety_calendar_df -> new_variety_calendar_table` 的身份连续性。除非 `incoming` 在该工作流中另有可独立说明的来源或职责，否则不得改称 `incoming_variety_calendar_table`。
- 上述例子表达的是项目偏好，不是要求把其他文件中所有短名立即批量替换。新任务只在其授权和实际触及的代码边界内应用这项偏好。

## 改名边界与偏好校准

- 内部函数参数可以为业务身份或对象表示而改名，例如 `table -> calendar_table`、`frame -> new_calendar_df`；必须同时检查并更新函数体、文档、测试和所有关键字参数调用。CLI 选项、配置键、Schema metadata、数据字段和对外稳定 API 仍需优先保持兼容；如果必须改名，要明确处理迁移边界。
- 在一个已触及的函数或工作流中，为了稳定共同词根和成对角色，可以一次同步相关名称；不得把这项授权扩展成跨无关函数、脚本或目录的风格清洗。
- 当用户要求制定命名规范、评审一组系统性改名，或同一处同时存在两种合理风格而用户偏好还不明确时，应优先展示当前代码中的多组具体对比例子并请用户说明偏好，而不是用抽象的“简洁”或“明确”标签替用户选择。例子应覆盖函数参数、业务词根、阶段修饰词、对象表示后缀、集合复数、映射方向、路径和计数命名等边界。
- 偏好校准可以分多轮进行。每一轮都要沿用用户已确认的选择，只询问新的边界或反例；一旦形成可复用的稳定偏好，应在用户授权下回写相应规范，避免后续 Agent 重复猜测或反复询问同一问题。单个局部且低风险的命名决策可以直接依照已确认偏好执行，不必为每个变量停下询问。

# Simplicity, readability, and abstraction rules

## Core principle

Optimize for code that can be understood by reading it from top to bottom.

Prefer explicit implementation, shallow call chains, and local readability over
DRYness, small functions, or additional abstraction.

Do not optimize the codebase for theoretical elegance. Optimize it for
understanding, modification, debugging, and deletion.

Share stable rules, invariants, contracts, and genuinely independent operations.
Do not share ordinary sequential implementation steps merely to remove a small
amount of duplication.

## Prefer direct code

- Prefer direct library calls over project-local wrappers when the library API
  is already clear and stable.
- Do not create a helper merely to wrap:
  - a single library call;
  - a trivial expression;
  - simple object construction;
  - a short transformation;
  - a small validation that is only relevant to one caller.
- Straightforward calls to libraries such as PyArrow, Pandas, Polars, `pathlib`,
  `shutil`, NumPy, and similar dependencies should normally remain visible at
  the point where they are used.

Prefer:

```python
dataset = ds.dataset(
    table_path,
    format="parquet",
    partitioning=partitioning,
)
```

over:

```python
dataset = open_arrow_dataset(table_path, partitioning)
```

when the wrapper adds no meaningful semantics.

## Optimize for linear readability

- A complete operation should normally be understandable by reading its owning
  function sequentially from top to bottom.
- Do not extract sequential implementation steps into helpers merely to shorten
  the parent function.
- Do not create helpers merely to give a block of code a name.
- When a sequential block needs explanation, prefer a short comment over moving
  the block into another function.
- Prefer one longer coherent function over several small functions when the
  smaller functions are only stages of the same workflow.
- Function length is not a reason to refactor by itself.
- A 100-line function implementing one coherent sequential operation may be
  preferable to ten 10-line helpers that require constant navigation.

## Minimize navigation

- Avoid making the reader jump between functions to understand ordinary control
  flow.
- Keep internal call chains shallow.

A normal implementation should preferably look like:

```text
operation
    -> external library
```

or:

```text
operation
    -> one meaningful project abstraction
    -> external library
```

Avoid:

```text
operation
    -> helper
    -> utility
    -> adapter
    -> wrapper
    -> external library
```

- If understanding a helper requires immediately opening its implementation,
  consider inlining it.
- A helper should be worth navigating to.

## Duplication is acceptable

- DRY is not a primary objective.
- Duplication alone is not a reason to refactor.
- Prefer duplicated straightforward implementation over additional indirection.
- Prefer repeating simple library calls in multiple modules over introducing a
  shared wrapper solely to remove repetition.
- It is acceptable for multiple modules to contain similar 5-30 line blocks when
  keeping those blocks local makes each workflow easier to understand.
- Do not extract common code merely because two implementations look similar.
- Abstract semantic duplication, not textual duplication.
- Refactor duplication only when maintaining the duplicated implementations has
  become a concrete problem.

Prefer duplicated implementation over duplicated navigation.

## When abstraction is justified

Create a helper, shared module, class, wrapper, or other abstraction only when it
has a concrete current responsibility.

Good reasons include:

1. It enforces an important project-wide invariant or data contract.
2. It represents a genuinely independent domain or technical operation.
3. It isolates substantial complexity that would obscure the owning workflow.
4. It isolates dangerous or unstable external side effects or APIs.
5. It has multiple independent real callers with the same semantic meaning.
6. It provides a boundary that is independently useful to test, reason about, or
   replace.

Do not create abstractions for hypothetical future reuse. Do not create an
abstraction merely because code could be extracted.

## Helpers

- A helper should represent an independent operation, not merely one step of
  another operation.

Good:

```python
validate_arrow_table(table, schema)
```

when it defines and enforces the project's Arrow data contract.

Potentially good:

```python
dataset_partitions(path, fields)
```

when it performs a complete operation such as discovering logical Hive
partitions from dataset fragments.

Usually unnecessary:

```python
make_hive_partitioning(fields)
```

when it only wraps:

```python
ds.partitioning(pa.schema(fields), flavor="hive")
```

- A helper with only one caller should be treated skeptically.
- If a one-caller helper only contains part of the caller's sequential workflow,
  inline it by default.
- Do not decompose a function into `_prepare_*`, `_build_*`, `_create_*`,
  `_process_*`, `_finalize_*`, or similar helpers unless those operations have
  genuinely independent semantics.

## Shared utilities

- Do not move code into a shared utility module merely because it is reusable.
- Shared utilities should contain stable concepts, not miscellaneous convenient
  snippets.
- Prefer keeping implementation local until there are multiple independent real
  callers and the shared semantic meaning is clear.
- Share rules and invariants; keep ordinary workflow steps local to the workflow
  that owns them.
- Do not create generic `utils.py`, `helpers.py`, `common.py`, manager, provider,
  factory, adapter, registry, service, or similar layers without a concrete
  architectural reason.
- A new file or module is a dependency boundary and therefore has a cost.

## Large-scale structure

Preserve clear large-scale boundaries.

It is appropriate to centralize things such as:

- schemas;
- data contracts;
- domain models;
- configuration;
- project-wide invariants;
- genuinely shared protocols;
- substantial external-system boundaries.

Do not confuse large-scale modularity with fine-grained decomposition. A project
may have clear modules while still keeping the implementation inside each module
direct and linear.

## Data and I/O code

For data pipelines, prefer exposing the actual sequence of operations. Opening
the workflow that owns a file operation should normally show the relevant
PyArrow, `pathlib`, `shutil`, or DataFrame calls directly.

For example, a write workflow may directly show:

```text
validate input
-> create staging path
-> write with PyArrow
-> read written data back
-> validate
-> move old data to backup
-> move staging data into place
-> rollback on failure
-> cleanup
```

Do not automatically turn each stage into a separate helper. The sequence itself
is often the most useful documentation of how the system works.

## Refactoring rules

When modifying existing code:

- Prefer simplifying existing structure over introducing new architecture.
- Prefer removing unnecessary wrappers and indirection.
- Minimize the number of files involved in a change.
- Minimize new dependency edges between modules.
- Preserve existing clear architectural boundaries.
- Do not redesign unrelated code.
- Do not introduce abstractions for possible future requirements.
- Do not split code merely to satisfy an arbitrary function-length target.
- Do not deduplicate straightforward code unless duplication is causing a real
  maintenance problem.

## Decision rule

Before creating any helper, class, wrapper, shared function, or module, ask:

1. Does this abstraction represent something meaningful on its own?
2. Does it remove substantial complexity rather than merely move code elsewhere?
3. Does it enforce an important invariant?
4. Is it independently reused by real callers?
5. Will the caller become easier to understand without immediately opening the
   abstraction?

If the answer to all of these is no, keep the code inline.

## Preferred implementation order

When several designs are valid, prefer them in this order:

1. Direct linear code using the underlying library.
2. Direct linear code with small explanatory comments.
3. A meaningful independent helper where necessary.
4. A shared abstraction with demonstrated reuse or invariant value.
5. Additional architectural layers only when clearly required.

Do not move down this list without a concrete reason.

# 项目运行环境

- 本仓库的标准 Python 环境是名为 `latitude` 的 Conda 环境。
- 在本工作站上，使用 `E:\anaconda3\envs\latitude\python.exe` 运行 Python，或使用 `conda run -n latitude python`。
- 不要根据裸 `python` 或裸 `pip` 的结果判断依赖缺失；它们可能指向 Conda 的 `base` 环境。
- 报告环境或依赖问题前，先输出 `sys.executable`，并使用标准解释器检查相关包。
- 使用标准解释器执行 `python -m pip`，不要使用裸 `pip`。
- 运行 `E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse\verify_runtime.py`，验证当前运行环境及核心 DataFrame 依赖。

# 长时间任务的人工启动、后台执行与可见监控

- 预计运行超过 10 分钟的任务，只能由用户在当前交互中明确授权为一个边界清楚的批次。允许把该批次交给与 Codex 回合解耦的 detached/background worker，但不得据此创建定时任务、常驻守护服务、自动恢复或未来批次授权。
- 预计运行时长、正式湖写入或采用 detached/background worker，均不自动触发非正式小样本、测试湖演练或仅因运行时长追加的 dry-run。此类步骤属于可选检查，只有用户在当前交互中明确要求时才执行；用户要求“可选时不要检查”时必须跳过，不得把未执行样本作为阻塞正式批次的理由。
- 后台 worker 必须同时配有独立、用户可见且不依赖 LLM 的 Terminal 窗口或 pane，持续显示当前阶段、可量化进度、累计耗时、心跳新鲜度和失败信息；仅写日志文件不构成可见监控。
- worker 和 monitor 都不得要求 Codex 回合保持活动。Codex 只做一次有界健康检查；确认 worker、业务子进程、心跳和可见 monitor 均正常后，必须立即结束回合，不得用 sleep、进程轮询或 tail 日志维持 Agent 存活。
- worker 不得自动重试。普通失败、配额停止或监控异常都必须停止后续阶段，保留状态、日志和事务证据，等待用户再次调用 Codex 后再决定如何继续。
- Windows monitor 读取原子状态文件时，必须使用允许 `ReadWrite` 与 `Delete` 的文件共享方式，读取后立即释放句柄；禁止用会阻塞 `os.replace` 的默认独占/非删除共享读取持续轮询状态文件。2026-08-21 的样本运行曾因 monitor 与 worker 对 `status.json` 发生共享冲突，导致业务仍正常时控制面报 `WinError 5` 并停止，此项是据此冻结的强制边界。
- worker 的状态发布必须采用同目录临时文件、flush/fsync、原子替换；只允许对状态文件替换时的 Windows `WinError 5/32` 做短时有界重试。该控制面重试不属于业务重试，不得据此重试 API、阶段或事务。状态发布最终失败时，worker 必须终止当前业务子进程并保留现场，不能留下失去监控的孤儿任务。

# 草稿脚本与测试收纳规则

- 项目根目录不得新建或恢复 `scripts`、`tests` 目录；原有内容统一暂存于 `00_draft_collection_02/scripts` 和 `00_draft_collection_02/tests`。
- 未经用户明确确认正式归属的新脚本、测试、一次性迁移代码、审计工具与验证工具，必须先放入 `00_draft_collection_02`，不得散落在项目根目录或任意业务子目录。
- 将 `00_draft_collection_02` 中的代码提升到正式目录前，必须由用户明确确认目标文件及归属；不得由 Agent 自行认定为正式项目代码。
- 移入草稿区不等于用户已经接受该代码。若既有正式入口仍引用被移动文件，必须同步修正路径以避免断链，并在审计结果中明确列出该依赖，等待用户决定保留、改造或移除。
- 审计发现的其他疑似未获接受脚本，只能先报告清单；未经用户对具体目标授权，不得继续移动、删除或改写。

# 项目根目录定位约定

- `.env.template` 中记录的项目根目录定位约定，是 AI 修改本仓库时必须遵循的权威规范。
- 从子目录运行的代码如果需要先定位项目根目录，再导入 `config.settings.settings`，必须使用下面的标记文件搜索方式。不要引入其他项目根目录定位方法。

```python
import pathlib
import sys

project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()  # 当前工作目录

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        break
else:
    raise RuntimeError("未找到项目根目录")

from config.settings import settings
```

# 正式期货湖仓定位、raw 归档与 silver 自动更新契约

- `.env` 的 `FUTURES_LAKE_ROOT` 是正式期货湖仓根目录的唯一配置来源；来源原文与证据归档、稳定表和实验性输出分别位于其 `raw`、`silver`、`gold` 子目录。生产代码通过 `config.settings.settings.futures_lake_root` 引用，不得另写一份正式路径常量。该路径约定不构成对 gold 表集合、字段、Schema、组织方式或研究方法的统一定义。
- 来源异常的 raw 证据留存、silver 验收和完成状态是不同语义；通用边界以 `03_Futures_Database/AGENTS.md` 的“来源异常留存与 silver 验收”条款为唯一文本权威。raw 留存不等于 silver 通过，目录级规则不得把二者合并。
- 生意社国内现货基差链路长期只归档 HTTP `response.content` 原始字节及其 SHA-256 sidecar，不解析页面、不提取字段，也不生产结构化现货基差事实。正式路径固定为 `raw/100ppi/domestic_spot_basis/year=YYYY/month=MM/observation_date=YYYY-MM-DD/{response.html,response.sha256}`。HTTP 200 的任意响应内容在两文件正式复读并核对摘要后，都回写外部市场日历为 `success`、`record_count=1`、`passed`；不得根据空正文、HTML 结构或业务内容另作质量判断。
- 生意社原文待办集合定义为：`上游 required 日期 −（原文字节与 SHA-256 sidecar 共同完整且外部市场日历状态完整的日期）`。原文归档完整但日历状态缺失或陈旧时，只从正式 raw 复读证据无 API 修复日历；原文缺失或摘要不一致才重新请求。页面结构监测、历史重采、解析、字段提取和结构化事实生产均属于未来另行确认的独立项目。
- `b02/c01a_position_rank_special_case_calibration` 是 c01 与 c02 之间的正式特殊案例证据环节。它只处理 `config/futures_position_rank_special_cases.py` 明确列出的案例：正式 raw 证据缺失时各请求一次冻结的交易所 URL，只有 HTTP 成功、响应 SHA-256、目标合约及完整 Top 20 校准值同时与配置一致才原子提交 `raw/shfe/position_rank_special_cases/<case_id>/{response.dat,response.sha256,calibration.json}`；证据已存在时只复读，不联网。该环节不写 silver、不扫描未配置异常、不自动推断新案例，也不进行业务重试。
- `b02/c02` 只在配置指定格点的 JQData 成交量榜完整 20 行与冻结坏载荷逐值一致时，才要求上述正式 raw 证据并把该榜整组替换为交易所权威 Top 20；来源已等于权威值时原样通过，任何第三种载荷都硬失败。持买仓、持卖仓和其他格点不受影响；完成后的报告日历必须永久保留包含案例 ID 与交易所原文摘要的 `success + warning` 校准证据，日常不得因此重拉。
- `fact_domestic_spot_basis_daily` 不再属于稳定 silver 契约；若旧正式湖中仍存在该表，当前采集入口和契约变更不得自动删除、迁移或改写它，应留待用户另行决定处置。
- 除下述已迁移的 b01 日常增量链路和 `b02/c03_warehouse_receipt` 外，silver 业务表的默认更新集合仍定义为：`上游当前有效格点 − 下游已经完整落盘的格点 = 本次自动更新范围`。空目录只是下游完整格点集合为空的普通情形，必须由同一自动入口自然得到全量建表，不另设一套生产日期范围。
- b01/c01—c04 的默认日常路径只处理可信正式水位之后的尾部新增；c01 以正式最大自然日推进，c02/c03 分别只消费上游新增交易日，c04 只消费 c03 尾部新增结构。历史内部缺口、删除、来源修订和结构修订只由显式 `--full` 慢路径发现；c03 仍保留成对日期质检。c03 日常遇到无有效 `trade_time` 的来源合约只 warning、跳过并推进水位，后续由显式全量审计统计。
- b01/c05—c06 每次仍按当前白名单窄列向量评估全部理论格点，但日常 API 待办只由 `is_fetch_required=true AND is_fetch_completed=false` 形成；正式成功提交后的 `is_fetch_completed`、成功批次、条数和质量是可信快照，不从全历史事实重新证明或修复。白名单扩大自动回补从未完成格点；缩小保留既有事实与完成/质量/c07 证据，只停止未来采集并清零当前缺失；再次纳入的已完成格点不重复拉取。
- 每张 silver 表的生产者仍对转换结果和自己的 dirty 完整叶承担完整业务质量验证。正式提交成功即证明该表的主键、水位、覆盖和其他权威业务约束已经成立；clean 历史和下游消费者必须信任这项证明，不得在日常重新扫描或复算。b01/c01—c07 与 `b02/c03` 已采用 dirty 叶单次业务校验，staging 和正式安装只复读物理字段、类型、nullable、表名、主键、分区及行数/主键摘要；描述性 metadata 差异以当前 `config/data_contracts.py` 为权威，不触发历史 Parquet 重写。尚未采用这一提交策略的入口继续遵守各自现有完整复读契约。
- 写入正式 silver 时，原则上禁止操作者用日期、月份等截断生产水位。b01/c01、c02、c04 的 `--full` 与显式日期互斥，且只有 `--full --write` 允许正式全历史维护；b01/c03 的成对日期或 `--full` 只定义来源双向质检范围，`--write` 仅提交发现的差异；b01/c07 的 `--force --write` 必须带日期范围或合约范围；b01/c08 只能显式 `--confirm-full-quality --write` 人工运行。其他入口的显式范围仍仅可用于只读检查，或写入明确不同于正式湖的临时/测试湖。
- 当前正式采集拓扑固定为 17 张稳定 silver 表、19 个正式采集入口（包含只能人工显式运行的 b01/c08）和 18 个默认日常阶段。默认 worker 中 b01 只执行 c01—c07；`--skip-optional-quality` 只跳过 c07。c08 永远不在任何默认 worker manifest 中。
- 本规则是 silver 数据采集的全项目目标契约；后续生产契约调整按用户确认的入口范围进行。发现个别脚本仍保留与本规则不一致的旧参数语义时，不得据此弱化本规则，也不得未经授权顺带批量修改其他业务脚本。gold 的实验输出契约和更新方式由所属下游工作流局部说明，不得提升为数据库级统一规则。

# 目录级规范路由

- 在 `02_Futures_Lakehouse` 目录树内工作时，必须读取并遵循 [02_Futures_Lakehouse/AGENTS.md](02_Futures_Lakehouse/AGENTS.md)；涉及正式 worker、monitor、状态、失败现场或人工处置时还必须读取 [operations AGENTS.md](02_Futures_Lakehouse/a02_Data_Collection_Operations/AGENTS.md)。
- 在 `04_Feature_Engineering` 目录树内工作时，必须读取并遵循 [04_Feature_Engineering/AGENTS.md](04_Feature_Engineering/AGENTS.md)。该项目当前仅完成结构迁移，已知运行阻塞不得被路径移动或双轨检查掩盖。
- 旧采集和旧特征工程项目位于根级 `05_Old_Projects`，其只读保护以 [05_Old_Projects/AGENTS.md](05_Old_Projects/AGENTS.md) 为准。
- 在 `03_Futures_Database` 目录树内工作，或在任何目录修改数据湖字段、Schema、生产者、读取者、类型转换及相关验证时，必须读取并遵循 [03_Futures_Database/AGENTS.md](03_Futures_Database/AGENTS.md) 和 `config/data_contracts.py`。
- 目录级业务细则只在对应目录的 `AGENTS.md` 中定义；根文件只负责项目级通用规则与规范路由，不复制目录细则。
