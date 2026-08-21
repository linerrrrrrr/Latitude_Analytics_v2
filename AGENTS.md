# 规范索引与同步要求

- [AGENTS.md](AGENTS.md)：项目运行环境、根目录定位与规范路由的项目级强制规则。
- [.env.template](.env.template)：项目根目录定位代码、当前稳定采集统一正式起点，以及包含 `raw`、`silver`、`gold` 的正式湖仓根路径环境变量权威模板。
- [02_Quant_Trading/AGENTS.md](02_Quant_Trading/AGENTS.md)：`E:\Latitude_Analytics_v2\02_Quant_Trading` 整棵目录树的目录级 Agent 规则入口，包含双轨、PythonExporter、`b00` 同步入口及根级旧项目归档路由。
- [03_Futures_Database/AGENTS.md](03_Futures_Database/AGENTS.md)：`E:\Latitude_Analytics_v2\03_Futures_Database` 整棵目录树的目录级 Agent 规则入口，以及字段命名、跨引擎类型、Schema metadata 单一来源与 Notebook 语义浏览的永久文本规范。
- [03_Futures_Database/read_futures_lake_demo.ipynb](03_Futures_Database/read_futures_lake_demo.ipynb)：17 张稳定 silver 表的契约化读取示例；每张表必须由独立代码单元格演示；另含不计入 silver 表数的生意社 raw 原文与摘要核对示例。
- [02_Quant_Trading/a01_Data_Collection/README.md](02_Quant_Trading/a01_Data_Collection/README.md)：数据采集链路、双轨同步入口、Schema metadata 运行时读取、Notebook 语义浏览、表粒度、主键、分区与更新水位规范。
- [02_Quant_Trading/a01_Data_Collection_Rebuild_Blueprint/README.md](02_Quant_Trading/a01_Data_Collection_Rebuild_Blueprint/README.md)：本次删除并重建采集项目的一次性历史实施记录；文件保持冻结，其中与当前 17 表契约或生意社原文归档政策不一致的旧设计均由当前规范和可执行契约覆盖。
- [02_Quant_Trading/a01_Data_Collection_Rebuild_Blueprint/08_EXECUTION_CHECKLIST.md](02_Quant_Trading/a01_Data_Collection_Rebuild_Blueprint/08_EXECUTION_CHECKLIST.md)：本次一次性重建的强制执行顺序与蓝图退出门禁；完成全部代码和代码门禁前禁止编写或执行数据迁移。
- [02_Quant_Trading/a02_Feature_Engineering/README.md](02_Quant_Trading/a02_Feature_Engineering/README.md)：主力连续合约、log 双向复权、期限结构边界及当前 gold 实验工作流说明。
- [config/futures_fact_collection_policy.py](config/futures_fact_collection_policy.py)：日线、分钟线和逐品种交易所报告共用的国内期货事实采集白名单唯一权威来源；白名单只含明确列出的交易所—品种，不得把它解释为品种、合约、日历或研究宇宙。
- [config/external_market_entities.py](config/external_market_entities.py)：外部市场日历与外部指数事实共用的请求实体、Eastmoney 指标映射及有效期唯一权威来源；配置不调用 API、不决定是否写入。
- [config/macro_release_entities.py](config/macro_release_entities.py)：宏观发布日历、SHIBOR 与宏观事实共用的 25 个系列、来源列、宏观数值偏移、理论频率及版本化可用日规则唯一权威来源；配置不调用 API、不决定是否写入。
- [config/jqdata_connection.py](config/jqdata_connection.py)：JQData 认证以及 Windows TUN 物理出口绑定的项目级共享连接边界；业务采集与是否写入仍由各业务入口负责。
- [config/data_contracts.py](config/data_contracts.py)：17 张稳定 silver 数据湖 Schema（7 张日历维度表、10 张事实表）、表名/主键/分区 metadata 单一来源及 Pandas、Polars、Arrow 转换的可执行契约。
- [00_draft_collection_02](00_draft_collection_02)：尚未经用户确认接纳为正式项目代码的脚本、测试、审计与验证工具的统一暂存目录。
- [04_Old_Projects/AGENTS.md](04_Old_Projects/AGENTS.md)：根级旧项目只读归档规则；包含重建前采集实现、更早历史采集项目和旧特征工程项目。
- 修改以上任一规范、模板或可执行契约前，必须检查其余索引项，并在同一次变更中同步所有受影响的描述、示例与代码。
- 新增具有规范作用的文本时，必须将其加入本索引，并在其他相关规范文本中添加反向索引；不得形成无法从本索引发现的孤立规范。
- 若不同规范之间存在冲突，必须先消除冲突再完成任务，不得选择性遵循其中一份。

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
- 运行 `E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading\verify_runtime.py`，验证当前运行环境及核心 DataFrame 依赖。

# 长时间任务的人工启动、后台执行与可见监控

- 预计运行超过 10 分钟的任务，必须先在小范围、非正式目标上完成同一执行路径的样本验证；样本失败时不得启动正式全量任务，并须保留失败现场。
- 正式长任务只能由用户在当前交互中明确授权为一个边界清楚的批次。允许把该批次交给与 Codex 回合解耦的 detached/background worker，但不得据此创建定时任务、常驻守护服务、自动恢复或未来批次授权。
- 后台 worker 必须同时配有独立、用户可见且不依赖 LLM 的 Terminal 窗口或 pane，持续显示当前阶段、可量化进度、累计耗时、心跳新鲜度和失败信息；仅写日志文件不构成可见监控。
- worker 和 monitor 都不得要求 Codex 回合保持活动。Codex 只做一次有界健康检查；确认 worker、业务子进程、心跳和可见 monitor 均正常后，必须立即结束回合，不得用 sleep、进程轮询或 tail 日志维持 Agent 存活。
- worker 不得自动重试。普通失败、配额停止或监控异常都必须停止后续阶段，保留状态、日志和事务证据，等待用户再次调用 Codex 后再决定如何继续。

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

- `.env` 的 `FUTURES_LAKE_ROOT` 是正式期货湖仓根目录的唯一配置来源；来源原文归档、稳定表和实验性输出分别位于其 `raw`、`silver`、`gold` 子目录。生产代码通过 `config.settings.settings.futures_lake_root` 引用，不得另写一份正式路径常量。该路径约定不构成对 gold 表集合、字段、Schema、组织方式或研究方法的统一定义。
- 生意社国内现货基差链路长期只归档 HTTP `response.content` 原始字节及其 SHA-256 sidecar，不解析页面、不提取字段，也不生产结构化现货基差事实。正式路径固定为 `raw/100ppi/domestic_spot_basis/year=YYYY/month=MM/observation_date=YYYY-MM-DD/{response.html,response.sha256}`。HTTP 200 的任意响应内容在两文件正式复读并核对摘要后，都回写外部市场日历为 `success`、`record_count=1`、`passed`；不得根据空正文、HTML 结构或业务内容另作质量判断。
- 生意社原文待办集合定义为：`上游 required 日期 −（原文字节与 SHA-256 sidecar 共同完整且外部市场日历状态完整的日期）`。原文归档完整但日历状态缺失或陈旧时，只从正式 raw 复读证据无 API 修复日历；原文缺失或摘要不一致才重新请求。页面结构监测、历史重采、解析、字段提取和结构化事实生产均属于未来另行确认的独立项目。
- `fact_domestic_spot_basis_daily` 不再属于稳定 silver 契约；若旧正式湖中仍存在该表，当前采集入口和契约变更不得自动删除、迁移或改写它，应留待用户另行决定处置。
- silver 业务表的默认更新集合统一定义为：`上游当前有效格点 − 下游已经完整落盘的格点 = 本次自动更新范围`。空目录只是下游完整格点集合为空的普通情形，必须由同一自动入口自然得到全量建表，不另设一套生产日期范围。
- “已经完整落盘”至少要求主键存在、整行通过权威 Arrow Schema/metadata 与表级质量约束；依赖完成状态的事实格点还必须已经从正式路径复读成功。不得只用下游最大日期判断无缺口。
- 每张 silver 表的生产者负责在转换、staging 复读和正式路径复读时执行该表的完整 Schema/metadata 与表级业务质量约束。下游消费者可以信任已经由生产者正式提交的上游表，不重复执行上游全部业务规则；消费者只校验物理兼容性以及自身计算直接依赖的主键、范围、覆盖等边界条件，并继续对自己的输出承担完整验证责任。
- 写入正式 silver 时，禁止由操作者指定起止日期、月份或其他手工截断范围；程序必须从上游有效水位和下游完成状态自行求差集。显式范围仅可用于只读检查，或在明确不同于正式湖的临时/测试湖中写入。
- 本规则是 silver 数据采集的全项目目标契约；当前代码迁移按用户确认逐表进行。在某个尚未迁移的脚本中发现旧参数语义时，不得据此弱化本规则，也不得未经授权顺带批量修改其他业务脚本。gold 的实验输出契约和更新方式由所属下游工作流局部说明，不得提升为数据库级统一规则。

# 目录级规范路由

- 在 `02_Quant_Trading` 目录树内工作时，必须读取并遵循 [02_Quant_Trading/AGENTS.md](02_Quant_Trading/AGENTS.md)。旧采集和旧特征工程项目已经移入根级 `04_Old_Projects`，其只读保护以 [04_Old_Projects/AGENTS.md](04_Old_Projects/AGENTS.md) 为准。
- 在 `03_Futures_Database` 目录树内工作，或在任何目录修改数据湖字段、Schema、生产者、读取者、类型转换及相关验证时，必须读取并遵循 [03_Futures_Database/AGENTS.md](03_Futures_Database/AGENTS.md) 和 `config/data_contracts.py`。
- 目录级业务细则只在对应目录的 `AGENTS.md` 中定义；根文件只负责项目级通用规则与规范路由，不复制目录细则。
