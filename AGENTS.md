# 规范索引与同步要求

本文件定义全仓通用规则；目录业务细则由下列入口定义。修改索引中的规范、模板或可执行契约前，检查其余索引项，并在同一次变更中同步受影响的说明、示例和代码。新增规范须加入本索引及相关文件的反向索引；规范冲突必须先消除，不得选择性执行。

- [仓库 README](README.md)：分区、正式入口与记录归属。
- [本文件](AGENTS.md)：[写作与读者前提](#写作与读者前提)、命名、实现、环境及执行边界。
- [环境说明](environment/README.md)：v2 重建、v1 回退、验证边界及环境材料收纳；[requirements.txt](environment/requirements.txt) 保存直接依赖与可选 GPU 安装说明；[alipai 说明](environment/alipai/README.md) 定义 SDK 依赖例外、验证入口与批次结果。
- [.gitignore](.gitignore) 与 [.gitattributes](.gitattributes)：Git 收纳、敏感旧文件排除、Notebook/Python 的 LF 和历史快照字节保护。忽略规则不会移除已跟踪的数据。
- [.env.template](.env.template)：根目录定位、统一采集起点和正式湖根路径。
- [采集 AGENTS](02_Market_Data/a01_Collection/AGENTS.md)：采集目录全树规则、b00 支撑脚本、Notebook/Python 双轨与导出同步。
- [采集 README](02_Market_Data/a01_Collection/README.md)：19 个正式入口的来源、粒度、更新水位、写入与验收。
- [采集检查](02_Market_Data/a01_Collection/checks/README.md)：来源质量、API 行为与连接确认、采集代码本地测试及检查记录。
- [湖仓 AGENTS](02_Market_Data/a02_Lake/AGENTS.md)：字段与类型、Schema metadata、raw/silver 边界、研究成果边界及 Notebook 契约展示。
- [数据湖读取 Demo](02_Market_Data/a02_Lake/read_futures_lake_demo.ipynb)：17 张 silver 表逐表独立演示，另含生意社 raw 字节与摘要核对。
- [operations AGENTS](02_Market_Data/a01_Collection/operations/AGENTS.md)：单项/批量授权、18 项日常配置、维护白名单、worker、监控与失败处置。
- [operations README](02_Market_Data/a01_Collection/operations/README.md)：GUI、看板、参数、代码检查、完整导出同步、日志与历史查看。
- [研究 AGENTS](04_Research/AGENTS.md) 与 [README](04_Research/README.md)：方法及派生编号、demo 引用、Notebook 代理、时间分区实验、本地／云端成果边界及 `referance/` 参考资料。
- [研究 Notebook 加载器](04_Research/a00_notebook_loader.py)：按 `export` 标签加载同名 Notebook 定义，不执行未标记 demo，也不复制算法源码。
- [市场演变复现规则](01_project_collection/china_futures_market_evolution_reproduction/AGENTS.md)：只读 silver、方法说明及逐项执行。
- [日内波动预测复现规则](01_project_collection/china_commodity_futures_intraday_volatility_forecasting_reproduction/AGENTS.md)：固定研究口径、Notebook、版本化成果与项目内长批次。
- [波动率研究入口](01_project_collection/JQ_strategy/volatility_research/README.md)、[阅读教程](01_project_collection/JQ_strategy/volatility_research/READING_TUTORIAL.md) 与 [支持附件说明](01_project_collection/JQ_strategy/volatility_research/supporting_materials/README.md)：分别定义通用符号与方法、学习顺序、一次 IM 研究的归档证据和适用边界。
- [金融期货数据 AGENTS](01_project_collection/JQ_strategy/financial_futures_data/AGENTS.md) 与 [README](01_project_collection/JQ_strategy/financial_futures_data/README.md)：人工聚宽传输、缺失检测、项目数据库、正式湖只读及建设状态。
- [金融期货采集政策](01_project_collection/JQ_strategy/financial_futures_data/financial_futures_collection_policy.py)：项目局部中金所白名单、频率与数据就绪时点的可执行唯一来源。
- [国内期货事实政策](config/futures_lakehouse/futures_fact_collection_policy.py)：日线、分钟线及逐品种交易所报告共用的事实采集白名单；不定义品种、合约、日历或研究宇宙。
- [成交持仓特殊案例](config/futures_lakehouse/futures_position_rank_special_cases.py)：已核实案例、完整坏载荷指纹、交易所摘要与校准值的唯一配置。
- [外部市场实体](config/futures_lakehouse/external_market_entities.py)：请求实体、Eastmoney 映射与有效期的唯一配置。
- [宏观发布实体](config/futures_lakehouse/macro_release_entities.py)：宏观日历、SHIBOR 与事实共用的系列、来源列、数值偏移、频率及版本化可用日规则。特殊案例、外部市场与宏观配置均不调用 API，也不决定是否写入。
- [JQData 连接](config/jqdata_connection.py)：共享认证与 Windows TUN 物理出口绑定；业务采集和写入由入口负责。
- [数据契约](config/data_contracts.py)：17 张稳定 silver 表（7 张日历维度表、10 张事实表）的唯一 Schema、表名/主键/分区 metadata 及 Pandas、Polars、Arrow 转换。
- [Schema 浏览器](02_Market_Data/a01_Collection/b00_03_notebook_schema_browser.py)：共用的只读契约展示及显式启用的有界数据/raw 样例；不另定义契约、不调用 API 或写数据。
- [路径事务](02_Market_Data/a01_Collection/b00_04_staged_path_transaction.py)：共用 staging 安装与失败恢复；业务合并、验收和共同回滚范围由环节决定。
- [operations 参考快照清单](02_Market_Data/a01_Collection/operations/referance/snapshot_manifest.json)：重构前只读 ZIP 的逐文件摘要，属于冻结证据。
- [实验草稿区规则](00_draft_collection_01/AGENTS.md)：探索 Notebook、配套输出目录、默认内嵌展示、显式导出及引用同步。
- [草稿区](00_draft_collection_02)：临时文件及尚待用户确认去留、复用或最终归属的材料；处置遵循[草稿区与文件收纳规则](#草稿区与文件收纳规则)。
- [旧项目 AGENTS](05_Old_Projects/AGENTS.md)：历史采集与特征工程项目的递归只读保护。

# 写作与读者前提

不要让读者替作者补全语境。本节适用于本仓库的研究报告、Notebook、技术文档与 AI Agent 规则。

写作不是单纯把作者脑中的信息转成文字。只要文本用于交流，它就隐含了一个读者模型：读者是谁、已经知道什么、从哪里开始阅读、当前能看到什么，以及读这段文字是为了完成什么任务。

很多所谓“文字脱节”，并不是句子语法错误，也不是术语太专业，而是作者调用了并未建立的读者前提。作者知道某件事，于是无意识地把“我知道”写成了“我们都知道”。

## 三个关键概念

- Audience model / 受众模型：文本究竟假定什么样的人正在阅读，而不能只笼统地写“面向工程师”“面向领导”。
- Common ground / 共同基础：作者和读者在当前交流中可以合法视为已经共享的信息。作者自己的项目经历不自动属于共同基础。
- Pragmatic presupposition / 语用预设：一句话在真正表达新信息以前，已经要求读者接受或识别的一组背景条件。

基本约束：当前句子所依赖的前提，只能来自目标读者合理已有的知识、本文此前已经建立的信息、当前可感知环境，或明确指定的外部材料。

## 写作时真正需要维持的是稳定的“读者模型”

避免未经建立的读者前提，并不要求所有文档都从零解释所有知识。专业文档当然可以假定读者已经理解某些领域概念。例如面向量化研究人员的报告，没有必要重新解释什么是收益率、回撤或主力合约。

真正需要明确的是：哪些知识允许被假定为已知，哪些信息必须由当前文本建立。

写作前至少应确定：

- 目标读者是谁：例如熟悉 Python 和期货市场，但第一次接触当前项目；
- 允许预设什么：例如理解收益率、回撤、滚动窗口等通用概念；
- 不得预设什么：例如旧版实现、内部会议、此前聊天记录、项目历史和未公开的命名约定；
- 读者可能从哪里进入：从第一页顺序阅读，还是可能直接打开某个章节、Notebook 或文件；
- 读者要完成什么任务：理解研究结论、复现实验、修改代码，还是执行项目规则。

所谓“面向工程师”“面向研究员”往往仍然过于宽泛。真正能够约束写作的是对已有知识、未知背景、阅读入口和任务目标的明确规定。

## 总约束与逐句检查

写作前先建立稳定的读者模型和语境边界。明确读者已经知道什么、不知道什么，从哪里进入文本，为了什么任务阅读，以及哪些信息可以合理视为共享背景。

全文不得无提示地提高读者的知识水平，也不得把作者本人拥有的项目历史、会议记录、工作过程或当前会话当成读者已经拥有的信息。

逐句检查：

- 这个概念，读者凭什么知道？
- 这个“它、这里、上述、之前、继续”，读者凭什么确定指什么？
- 这条规则依赖的条件是否已经写出来？
- 这项背景属于目标读者合理已有的知识，还是只是作者自己恰好知道？
- 如果拿掉当前聊天记录、会议历史、旧版本和作者本人，这句话是否仍然能够稳定解释？

尤其避免：

> “继续按照原来的方法。”
>
> “和上次一样。”
>
> “这里不再处理。”
>
> “按照现有逻辑即可。”
>
> “具体过程见之前内容。”

这些表达并非绝对不能出现，但其参照对象必须已经被当前文本可靠建立。

更一般地说：不要把作者的工作过程当成读者的理解路径。

研究者可能经历：旧方案 → 讨论 → 修改 → Notebook → 某次运行 → 再修改 → 最终报告。

但外部读者真正需要的通常是：研究对象是什么 → 为什么研究 → 如何研究 → 使用什么证据 → 得到什么结果 → 结论适用于什么范围。

工程项目也是如此。开发者可能经历多次迁移、重构和目录调整，但新进入项目的人或 AI Agent 首先需要的是当前系统的结构、规则和边界，而不是先恢复整个开发历史。

最终目标：每一次省略都有依据，每一次指代都有对象，每一次语境切换都先建立再使用。

这样形成的文档，即使换读者、换入口、换时间甚至换一次 AI 会话，其核心含义仍然能够保持稳定。

## 示例与解释

以下三个示例说明写作规则的应用。示例中的样本数量、文档名、批次时间和目录路径用于解释表达问题，不构成本仓库的数据事实或目录约定。

### Demo 1：研究报告面向外部读者，却写成了内部工作日志

研究报告开篇的反例：

> “本报告承接策略生成研究……”
>
> “分类依据为所有思维导图和60条策略清单。”
>
> “新上传的《固定区间研究》仅保存策略清单与任务预览……”
>
> “本报告实证部分读取此前交付包《回测总控》在2026年9月30日03:43批次保存的统计与曲线……”

这些话对于刚刚完成这项工作的内部人员可能非常明确，但对于第一次拿到正式研究报告的外部读者，存在大量未经建立的预设：

- 什么是“策略生成研究”，报告与它是什么关系？
- “所有思维导图”具体是哪一些？
- 什么叫“新上传”？谁上传给谁？
- “此前交付包”是什么文档体系？
- 为什么读者需要知道 03:43 这个运行批次？
- “单元格未保存输出”是研究结论，还是作者制作报告时遇到的工作过程问题？

根本问题：作者把“报告是怎样被制作出来的”误写成了“读者理解研究对象所需要知道的内容”。

正式报告应该优先告诉读者：本报告研究如何对现有规则策略进行分类，并检验不同类别策略在统一回测条件下是否表现出可解释的收益差异。研究对象共包含60项策略规则；其中45项具有完整可用的回测结果，构成本报告的实证样本。其余策略因回测数据不完整，不参与当前绩效比较。

如果确实存在数据来源限制，可以再写：本报告仅使用已经形成完整回测输出的样本，因此当前结论只适用于上述45项策略。这里保留真正影响结论边界的信息，删除“谁上传了哪个 Notebook、几点跑出的结果”这种生产过程噪声。

给 AI 的约束：撰写正式报告时，默认读者没有参与研究过程。不要使用“此前交付”“新上传”“本次任务”“上一版”“03:43批次”等只有项目内部人员才能理解的过程性表述。先说明研究对象、问题、方法、样本和结论；只有当内部过程会改变证据可靠性或适用范围时，才将其转换为面向读者的数据限制说明。

### Demo 2：Notebook 把项目历史当成当前说明

例如：

> 本 Notebook 参考旧版主力判定与拼接思路……
>
> 期限结构继续使用真实合约原始价格，不使用这里的累计复权因子。

第一次进入 Notebook 的研究人员可能完全理解“主力合约”“期限结构”和“复权”这些专业概念，却仍然不知道：

- “旧版”是什么；
- 旧版究竟采用什么规则；
- “继续”是在延续什么历史；
- “这里的累计复权因子”是哪一个对象。

关键区分：领域知识已知，不等于项目内部对象已经进入当前语篇。

更适合入口文档的写法是：本 Notebook 从真实合约日线行情中构造主力连续序列。交易日 t 的主力合约只能使用 t−1 日及以前已经收盘的数据确定，以避免未来信息。连续序列会生成用于消除换月跳跃的复权价格；期限结构指标则始终使用各真实合约的原始价格，因为该指标需要保留同一时点不同合约之间的真实价差关系。

最后如果需要，才补充：该规则由旧项目方案演化而来。

给 AI 的约束：当前实现必须能够独立说明。“沿用旧版”“与之前一致”“继续采用”等历史关系只能作为补充，不能代替当前规则本身的定义。

### Demo 3：不要把作者所处的情境，当成文本已经提供的信息

路线指引很容易暴露这种问题：

> 前行 200 米，在路口左转，再沿右侧道路前进。

这句话只有在一些没有写出的条件成立时才足够明确：读者正站在作者预想的位置、面对预想的方向，并且是在现场阅读。

普遍结构：作者拥有某个当前情境，于是误以为读者也拥有这个情境。

同样的问题在研究、技术文档和项目规则中更加常见：

> “这里继续使用之前的方法。”
>
> “按照现有方式处理即可。”
>
> “与上次一致，不再调整这一部分。”
>
> “保持当前目录结构。”

写下这些话的人通常知道“这里”“之前”“上次”“当前”分别指什么，因为他刚刚修改过代码、参加过会议，或者正停留在某个目录、Notebook 或对话中。但换一个读者、换一个时间、换一个入口，这些信息就可能消失。

例如在 AGENTS.md 中写：“保持现有方式，不要使用旧方案”。对于刚进入仓库的 AI Agent，这并没有形成一条稳定规则。它仍然需要自行判断：

- “现有方式”具体是什么；
- “旧方案”是哪一套；
- 两者在哪里区分；
- 规则适用于整个项目还是某个目录；
- 什么行为才算违反这条规则。

问题因此不只是“表达不够清楚”，而是文本产生了不必要的语境依赖：只有保留作者当时的工作现场、会话记录或项目历史，文本才能得到稳定解释。研究报告、技术文档和 Agent 规则尤其需要避免这种写法，因为它们往往需要跨越不同的读者、时间、会话和阅读入口继续使用。

因此，应把真正影响理解和执行的条件写进文本，而不是要求读者恢复作者当时所处的情境。

不要只写：继续使用原来的方法。

应直接写清：主力合约判定只使用上一交易日及以前已经收盘的数据。

不要只写：保持现有目录结构。

应直接写清：新增策略统一放入 strategies/；每个策略对应一个 .py 文件，不新增按“人类可读/机器可读”划分的平行目录。

这里涉及的仍然是共同基础（common ground）和指示（deixis）。像“这里”“之前”“当前”“原来”“继续”“同样处理”这样的表达，本身不能完全确定意义，需要借助上下文解释。问题发生在作者把只有自己掌握的上下文，误判成了作者与读者已经共享的上下文。

给 AI 的约束：不要把当前会话、作者的工作过程、会议历史、默认阅读顺序或项目内部经验视为读者天然拥有的信息。使用“这里、上述、之前、继续、仍然、原来、现有方式、同样处理”等依赖语境的表达时，检查其对象是否能够仅凭当前文本确定。若不能，应直接写明对象、条件、规则和适用范围。

# 变量命名与最小改动

## 判断顺序

变量名优先保证语义清楚、身份连续和类型可辨识，再考虑长度。名称应在函数边界、日志、异常、调试器和搜索结果中独立表意；不得为了字符少牺牲业务身份、处理角色或契约状态。

“最小改动”约束任务范围，不保护含混旧名。可以在已触及的完整工作流中成组改名以消除歧义、统一词根和区分表示，不得扩展成无关模块的风格清洗。

## 业务身份、处理角色与对象表示

- 同一实体保持稳定业务词根，优先使用“角色或状态 + 业务实体 + 表示类型”，如 `expected_calendar_df`、`validated_calendar_table`。
- 表示转换不改变业务身份和角色。例如 `new_variety_calendar_df -> new_variety_calendar_table` 只换表示后缀，不因 `pandas_to_arrow()` 改称 `incoming_*`。只有来源、职责、生命周期或已保证的后置条件变化，才改变角色词。
- 跨独立契约边界可以使用组件内的角色名，例如调用侧 `new_calendar_table` 传给 `commit_partition(incoming_table=...)`；不得用近义词伪造阶段变化。
- 多种表示并存或作用域较长时，明确使用 `*_df`、`*_table`、`*_dataset`、`*_schema`、`*_path`、`*_dir`。避免无法表达身份的 `data`、`result`、`frame`、`checked`。

## 已确认的项目偏好

- b01/c01 中的 `calendar_table`、`new_calendar_df`、`validated_calendar_table`、`existing_calendar_dataset`、`existing_calendar_table`、`existing_calendar_schema`、`requested_calendar_df`、`expected_calendar_df`、`pending_calendar_df`、`valid_calendar_dates`、`incomplete_calendar_dates`、`expected_calendar_signature_by_date` 均有独立语义；不得仅为缩短或恢复旧名而改回 `table`、`frame`、`checked`、`existing`、`valid_dates`、`expected_signature_by_date`。
- b01/c02 的 `pandas_to_arrow()` 保持 `new_variety_calendar_df -> new_variety_calendar_table`；除非存在独立可说明的来源或职责，不改称 `incoming_variety_calendar_table`。
- 这些偏好只应用于授权且实际触及的代码，不要求立即改遍其他文件。

## 改名边界与偏好校准

- 内部参数改名须同步函数体、文档、测试及所有关键字调用。CLI、配置键、Schema metadata、字段和稳定 API 优先兼容；必要改名须处理迁移边界。
- 制定命名规范、评审系统性改名，或两种合理风格之间的偏好尚不明确时，用当前代码的具体对比例子向用户校准；覆盖参数、业务词根、角色、表示后缀、集合复数、映射方向、路径和计数，不能只问抽象的“简洁还是明确”。
- 多轮校准沿用已确认选择，只询问新边界；形成可复用偏好后，在用户授权下回写规范。局部低风险命名可依已确认偏好执行，不必逐个询问。

# Simplicity, readability, and abstraction rules

Optimize for top-to-bottom understanding, modification, debugging, and deletion. Prefer explicit code, shallow calls, and local readability over DRYness, short functions, or theoretical elegance.

## Direct implementation

- Keep clear library calls visible at their use site. Do not wrap a single call, trivial expression, simple construction or transformation, or caller-specific validation merely to name it.
- Keep a coherent operation and its ordinary sequential steps together. Use a short comment when explanation is needed; function length alone does not justify splitting. One coherent 100-line function can be clearer than ten navigational steps.
- Prefer `operation -> library`, or `operation -> one meaningful abstraction -> library`. If a helper requires immediately opening its implementation to understand ordinary control flow, consider inlining it.
- Straightforward duplication is acceptable, including similar 5–30 line blocks. Share stable rules and semantic invariants; do not extract steps merely because their text repeats. Deduplicate when maintenance has become a concrete problem.

## When abstraction is justified

A helper, module, class, or shared boundary must have a current responsibility: enforce a contract or invariant; perform an independent domain/technical operation; isolate substantial complexity or dangerous/unstable external effects; serve multiple independent real callers with the same meaning; or provide a boundary independently useful to test, reason about, or replace.

A one-caller helper deserves scrutiny: inline it when it is only a stage of its caller. Do not build chains of `_prepare_*`, `_build_*`, `_process_*`, and `_finalize_*` without independent semantics. For example, a shared `validate_arrow_table(table, schema)` enforces a contract; `make_hive_partitioning(fields)` merely wrapping `ds.partitioning(...)` usually does not.

Shared utilities must hold stable concepts, not miscellaneous convenient snippets. Do not create `utils.py`, `helpers.py`, `common.py`, managers, providers, factories, adapters, registries, or services solely for possible reuse. Preserve useful large-scale boundaries for schemas, domain models, configuration, contracts, protocols, and substantial external-system interactions; each new module and dependency edge still has a cost.

## Data workflows and refactoring

Keep file operations visible in their owning workflow: validate input, stage and write, read back, validate, back up and install, roll back on failure, then clean up. Do not turn each stage into a helper just to shorten the function.

Within authorized changes, prefer removing unnecessary wrappers, minimizing changed files and dependencies, and preserving existing clear boundaries. Do not redesign unrelated code or build for hypothetical future requirements.

Choose direct library code first, then explanatory comments, an independently meaningful helper, a demonstrated shared abstraction, and only then additional layers. Move beyond direct code only for one of the current responsibilities above.

# 项目运行环境

- 开发、采集、研究、Notebook 与 alipai 本地入口统一使用 `latitude_env_v2`。版本、解释器、重建顺序、依赖例外、v1 回退和历史成果验收边界见[环境说明](environment/README.md)。
- 判断依赖问题前先输出 `sys.executable`，再用 v2 解释器运行 `python -m pip` 或检查包；不得依据可能指向 base 的裸 `python` / `pip` 判断缺失。
- 环境检查：`E:\anaconda3\envs\latitude_env_v2\python.exe 02_Market_Data\a01_Collection\b00_01_verify_runtime.py`。

# 长时间任务的人工启动、后台执行与可见监控

- 预计超过 10 分钟的任务须由用户在当前交互中明确授权一个有清楚边界的批次。可使用与 Codex 回合解耦的 detached/background worker；这不授权定时任务、常驻守护、自动恢复或未来批次。
- 时长、正式湖写入或后台执行不自动要求小样本、测试湖或追加 dry-run。此类可选检查只在当前交互明确要求时执行；用户要求跳过时必须跳过，不得以未做样本阻塞正式批次。
- worker 必须配独立、可见且不依赖 LLM 的 Terminal、pane 或所属工作流规定的总控台，持续展示阶段、可量化进度、耗时、心跳新鲜度和失败信息；仅写日志不算监控。
- worker 和 monitor 不得依赖 Codex 回合存活。只做一次有界健康检查，确认 worker、业务子进程、心跳和可见 monitor 正常后立即结束回合；不用 sleep、轮询或 tail 维持 Agent。
- worker 不自动重试。普通失败、配额停止或监控异常须停止后续阶段、保留状态/日志/事务证据，由操作者核查后显式决定新的有界批次。
- Windows monitor 读取原子状态文件须允许 `ReadWrite | Delete` 共享，并立即释放句柄，不能阻塞 `os.replace`。状态发布使用同目录临时文件、flush/fsync 和原子替换；只对该替换的 `WinError 5/32` 短时有界重试，不扩展到 API、业务阶段或事务。发布最终失败须终止业务子进程并保留现场。

# 草稿区与文件收纳规则

`00_draft_collection_02` 是流动的暂存区，收纳临时文件，以及尚未由用户决定去留、复用或最终目录的材料。其中既可能有用完即可丢弃的一次性文件，也可能有需要保存的数据源质检、实验依据或可复用代码；位于草稿区本身不代表材料没有价值。

- 用途完成后，Agent 应说明具体材料的用途、结果、保存或复用价值及建议去向，由用户确认其重要性、是否保留或删除、是否复用及最终收纳目录。一次性验证已经完成，不构成自动删除依据。
- 用户确认删除的材料才可删除；确认保留的证据或复用文件，在最终目录确定后移出草稿区。已有明确授权的具体文件或批次按授权范围执行，无需重复确认；未决定的材料继续暂存，Agent 不自行接纳、删除或安排永久归属。
- 全部待办材料完成处置且没有新增材料时，草稿区应不留文件或子目录。实际工作不断产生新材料，允许暂时非空；不得为了清空目录跳过用户确认，也不得把暂存位置当作最终收纳位置。

- 分区结构见 [README](README.md)：市场数据第二层固定为 `a01_Collection`、`a02_Lake`；GUI 留在采集目录，正式湖根不另加 `futures_lake` 一层。 采集目录的编号按层级递进：`a01_Collection/bNN_业务组/cNN_环节`；与业务组同层的支撑文件使用 `b00_NN`，具体入口见采集规范。
- 已确定所属项目的历史、日志、状态及验收记录归产生它们的项目。采集运维使用 `02_Market_Data/a01_Collection/operations/run_history/`；研究记录归所属研究项目。草稿区不得充当正式工作流长期日志仓库。
- `00_draft_collection_01/` 中的实验工作遵循[实验草稿区规则](00_draft_collection_01/AGENTS.md)，包括 Notebook 与配套输出收纳、默认展示、导出和跨 Notebook 引用；目录业务细则不在根文件重复定义。
- 根目录不得新建或恢复 `scripts/`、`tests/`。尚未确认归属的新脚本、测试、一次性迁移及审计验证工具先放 `00_draft_collection_02`；脚本和测试按需使用其中的 `scripts/`、`tests/`，不预建或永久保留空目录，不散落在业务目录。
- 已接纳的数据源质量与连接检查归 `a01_Collection/checks/`；来源检查与采集代码的本地测试统一归其 `tests/`，来源检查结果归其 `results/`。`operations/` 只收纳 GUI 及直接支撑它的代码、资源，其测试归 `operations/tests/`、运行记录归 `operations/run_history/`。采集维护历史包保存在 `05_Old_Projects/collection_maintenance_20261002.zip`，保留包内原路径和摘要，不从包内导入代码。
- 移入草稿不等于接纳：既有入口引用被移动文件时，须修正路径并报告依赖，待用户决定保留、改造或移除。审计发现其他疑似未接纳脚本时先报告，未获具体目标授权不得移动、删除或改写。

# 项目根目录定位约定

以 [.env.template](.env.template) 的代码为唯一定位方式：从当前工作目录向父目录查找同时包含 `.git`、`.env`、`config/settings.py` 的目录，加入 `sys.path` 后再导入 `config.settings.settings`；找不到则报错，不另建定位实现。

# 正式期货湖仓定位、raw 归档与 silver 自动更新契约

- 正式湖仅由 `.env` 的 `FUTURES_LAKE_ROOT` 配置，代码使用 `config.settings.settings.futures_lake_root`。raw、silver、gold 的目录划分不构成 gold 表集合、Schema 或研究方法的全仓定义。
- 在任何目录修改数据湖生产者、消费者、字段、Schema、转换或验证时，必须遵循[湖仓规则](02_Market_Data/a02_Lake/AGENTS.md)和[可执行契约](config/data_contracts.py)。raw 留证、silver 验收和完成状态不得混同；生产者验证来源及 dirty 输出，消费者和 clean 历史按契约信任正式提交的业务证明。
- 各入口的更新集合、空湖全建、日期/全量写入门禁、来源异常和提交验收以[采集规则](02_Market_Data/a01_Collection/AGENTS.md)、[采集说明](02_Market_Data/a01_Collection/README.md)及湖仓规则为准。其中包括生意社只归档原文、特殊案例精确校准、可信完成状态，以及 c08 必须人工显式选择和确认的例外；不得用通用求差或重试替代这些边界。
- 生产契约调整只按用户确认的入口范围实施。发现脚本保留不一致的旧语义时，不得据此弱化契约，也不得顺带批量修改其他业务脚本。gold 输出由所属工作流定义和验证。

# 目录级规范路由

- 在 `00_draft_collection_01/` 内创建、修改或执行实验，读取[实验草稿区 AGENTS](00_draft_collection_01/AGENTS.md)。
- 在采集目录树内工作，读取[采集 AGENTS](02_Market_Data/a01_Collection/AGENTS.md)；涉及 worker、monitor、状态或失败处置，再读[operations AGENTS](02_Market_Data/a01_Collection/operations/AGENTS.md)。在其他目录涉及湖仓契约，也须遵守上节规则。
- 在 `04_Research` 内工作，读取[研究 AGENTS](04_Research/AGENTS.md)。当前仅建立方法／实验骨架与加载器，具体环节尚未定义；原特征工程留存与已知阻塞见[研究说明](04_Research/README.md)。研究代理与采集完整 PythonExporter 导出分别遵循所属目录规则。其他研究项目遵循上方索引中的项目入口。
- `05_Old_Projects` 遵守[归档只读规则](05_Old_Projects/AGENTS.md)。归档规范中的旧采集路径属于迁移前标识；当前维护入口以根 README 和采集 AGENTS 为准，不改写归档文件。
- 目录业务细则在所属 AGENTS 中定义，根文件只保留全仓规则和必要路由，不复制目录细则。
