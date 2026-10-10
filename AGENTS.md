# 规范索引与同步要求

本文件定义全仓通用规则；目录业务细则由下列入口定义。修改索引中的规范、模板或可执行契约前，检查其余索引项，并在同一次变更中同步受影响的说明、示例和代码。新增规范须加入本索引及相关文件的反向索引；规范冲突必须先消除，不得选择性执行。

- [仓库 README](README.md)：分区、正式入口与记录归属。
- [本文件](AGENTS.md)：[写作与读者前提](#写作与读者前提)、[协作纠错与执行状态](#协作纠错与执行状态)、[层级编号与资源归属](#层级编号与资源归属)、[草稿区与文件收纳规则](#草稿区与文件收纳规则)中的运行输出归属、命名、实现、环境及执行边界。
- [环境说明](environment/README.md)：v2 重建、v1 回退、验证边界及环境材料收纳；[requirements.txt](environment/requirements.txt) 保存直接依赖与可选 GPU 安装说明；[alipai 说明](environment/alipai/README.md) 定义 SDK 依赖例外、验证入口与批次结果。
- [.gitignore](.gitignore) 与 [.gitattributes](.gitattributes)：Git 收纳、研究方法 demo 的跟踪例外、二进制与 Git LFS 属性、敏感旧文件排除、Notebook/Python 的 LF 和历史快照字节保护。忽略规则不会移除已跟踪的数据。
- [.env.template](.env.template)：根目录定位、统一采集起点和正式湖根路径。
- [采集 AGENTS](R02_Market_Data/a01_Collection/AGENTS.md)：采集目录全树规则、b00 支撑脚本、Notebook/Python 双轨与导出同步。
- [采集 README](R02_Market_Data/a01_Collection/README.md)：19 个正式入口的来源、粒度、更新水位、写入与验收。
- [采集检查](R02_Market_Data/a01_Collection/checks/README.md)：来源质量、API 行为与连接确认、采集代码本地测试及检查记录。
- [湖仓 AGENTS](R02_Market_Data/a02_Lake/AGENTS.md)：字段与类型、Schema metadata、raw/silver 边界、研究成果边界及 Notebook 契约展示。
- [数据湖读取 Demo](R02_Market_Data/a02_Lake/read_futures_lake_demo.ipynb)：17 张 silver 表逐表独立演示，另含生意社 raw 字节与摘要核对。
- [数据呈现附带中文](#数据呈现附带中文)：市场数据与 `R04_Research` 的表头、索引及研究派生字段展示规则。
- [operations AGENTS](R02_Market_Data/a01_Collection/operations/AGENTS.md)：单项/批量授权、18 项日常配置、维护白名单、worker、监控与失败处置。
- [operations README](R02_Market_Data/a01_Collection/operations/README.md)：GUI、看板、参数、代码检查、完整导出同步、日志与历史查看。
- [研究 AGENTS](R04_Research/AGENTS.md) 与 [README](R04_Research/README.md)：研究主题 `bNN`、主题内单元 `cNN` 及派生编号、同单元 Notebook／代理／`cNN_Name_DemoData/` 的归属、采样位置与价格执行的分工、可调用的单图交互选择、一个 b 层级内的收益／波动率建模、demo 的品种／范围／参数命名（涉及划分时分别列出训练与测试范围）、Git 收纳与引用、只读旧采样实现、Notebook 代理、[上游契约与研究校验](R04_Research/AGENTS.md#上游契约与研究校验)、实验命名与 scope、本地／云端成果边界及 `referance/` 参考资料。
- [研究 Notebook 加载器](R04_Research/a00_notebook_loader.py)：按 `export` 标签加载同名 Notebook 定义，不执行未标记 demo，也不复制算法源码。
- [研究真实 demo](R04_Research/AGENTS.md#notebook-与代理)：逐环节使用真实上游产物调用计算函数；完整重采样序列的逐点重估、失败保留及实际验收状态与合成边界测试分别记录。
- [环节函数 Notebook 的 demo DLC 流程](R04_Research/AGENTS.md#notebook-dlc-流程)：只约束环节 demo 的 15 格、同一完整输入与不落盘试错、无额外脚本提交、60 秒查询、回收及独立展示；实验层不套用该排列。
- [期限结构与 NSS 校准](R04_Research/README.md#期限结构输入)：`b05_TermStructureModeling` 的 c01 输入尺度与最后有效日自然日末终点、c02 固定 λ 约束拟合及 c03 训练期网格、三种并列参数选择、测试期固定 λ 和独立单张热力图；[共用参数](R04_Research/a01_Methods/b05_TermStructureModeling/c03_NSSCalibration.yaml) 定义 RB、N15 的 σ×成交额完整云 demo 及不落盘小样本，真实状态与执行规则分别见研究说明及 [研究 AGENTS](R04_Research/AGENTS.md#目录与文件)。
- [小波分析](R04_Research/README.md#小波分析)：`b06_WaveletAnalysis` 的 c01 复因果 Gammatone 导数核与离线 Morlet、采样点周期、显式递推状态、逐周期预热和缺失分段；c02 的正式 SST、核导数频率映射、当前点比例／绝对阈值、固定尺度权重、频率区间及排除诊断；c03 的连续序列状态、采样与可得时刻对齐、宽表契约及参数化独立单图；c04 的逐点谱特征、区间宽度权重、圆周相位一致性、未定义原因与独立单图。真实 demo 状态见研究说明，执行规则见 [研究 AGENTS](R04_Research/AGENTS.md#目录与文件)。
- [市场演变复现规则](R01_project_collection/china_futures_market_evolution_reproduction/AGENTS.md)：只读 silver、方法说明及逐项执行。
- [日内波动预测复现规则](R01_project_collection/china_commodity_futures_intraday_volatility_forecasting_reproduction/AGENTS.md)：固定研究口径、Notebook、版本化成果与项目内长批次。
- [波动率研究入口](R01_project_collection/JQ_strategy/volatility_research/README.md)、[阅读教程](R01_project_collection/JQ_strategy/volatility_research/READING_TUTORIAL.md) 与 [支持附件说明](R01_project_collection/JQ_strategy/volatility_research/supporting_materials/README.md)：分别定义通用符号与方法、学习顺序、一次 IM 研究的归档证据和适用边界。
- [金融期货数据 AGENTS](R01_project_collection/JQ_strategy/financial_futures_data/AGENTS.md) 与 [README](R01_project_collection/JQ_strategy/financial_futures_data/README.md)：人工聚宽传输、缺失检测、项目数据库、正式湖只读及建设状态。
- [金融期货采集政策](R01_project_collection/JQ_strategy/financial_futures_data/financial_futures_collection_policy.py)：项目局部中金所白名单、频率与数据就绪时点的可执行唯一来源。
- [国内期货事实政策](config/futures_lakehouse/futures_fact_collection_policy.py)：日线、分钟线及逐品种交易所报告共用的事实采集白名单；不定义品种、合约、日历或研究宇宙。
- [成交持仓特殊案例](config/futures_lakehouse/futures_position_rank_special_cases.py)：已核实案例、完整坏载荷指纹、交易所摘要与校准值的唯一配置。
- [外部市场实体](config/futures_lakehouse/external_market_entities.py)：请求实体、Eastmoney 映射与有效期的唯一配置。
- [宏观发布实体](config/futures_lakehouse/macro_release_entities.py)：宏观日历、SHIBOR 与事实共用的系列、来源列、数值偏移、频率及版本化可用日规则。特殊案例、外部市场与宏观配置均不调用 API，也不决定是否写入。
- [JQData 连接](config/jqdata_connection.py)：共享认证与 Windows TUN 物理出口绑定；业务采集和写入由入口负责。
- [数据契约](config/data_contracts.py)：17 张稳定 silver 表（7 张日历维度表、10 张事实表）的唯一 Schema、表名/主键/分区 metadata 及 Pandas、Polars、Arrow 转换。
- [Schema 浏览器](R02_Market_Data/a01_Collection/b00_03_notebook_schema_browser.py)：共用的只读契约展示及显式启用的有界数据/raw 样例；不另定义契约、不调用 API 或写数据。
- [路径事务](R02_Market_Data/a01_Collection/b00_04_staged_path_transaction.py)：共用 staging 安装与失败恢复；业务合并、验收和共同回滚范围由环节决定。
- [operations 参考快照清单](R02_Market_Data/a01_Collection/operations/referance/snapshot_manifest.json)：重构前只读 ZIP 的逐文件摘要，属于冻结证据。
- [实验草稿区规则](R00_draft_collection_01/AGENTS.md)：探索 Notebook、配套输出目录、默认内嵌展示、显式导出及引用同步。
- [草稿区](R00_draft_collection_02)：临时文件及尚待用户确认去留、复用或最终归属的材料；处置遵循[草稿区与文件收纳规则](#草稿区与文件收纳规则)。
- [旧项目 AGENTS](R05_Old_Projects/AGENTS.md)：历史采集与特征工程项目的递归只读保护。

# 写作与读者前提

本节规定文本内容的取舍、组织与表达，适用于本仓库的研究报告、Notebook、技术文档与 AI Agent 规则。

文本应围绕表达目的和目标读者的阅读任务组织，而不是按编写过程中接触信息的顺序组织。先判断哪些信息需要传达，再判断读者理解这些信息需要什么前提，最后处理结构、指代和措辞。不要让读者替编写者补全语境，也不要让读者承担与阅读任务无关的背景恢复工作。

## 内容选择先于表述完善

写作需要分别回答两个问题：**这项信息是否应该出现在当前文本中？如果应该，读者能否正确理解它？** 前者是内容选择，后者是表述及读者前提。两者不能互相替代。

信息真实、来源明确、解释清楚，说明它可能具备被使用的条件，不说明它应当被使用。某项信息曾参与分析、讨论或决策，也不直接构成写入文本的理由。它在编写过程中发挥过作用，与它在读者的阅读过程中是否发挥作用，是两个不同的判断。

保留内容时，应能说明它在当前文本中的具体作用：定义对象，提出问题，解释方法，支持判断，说明条件，限定结论，帮助理解，指导操作，或满足已经确定的比较、复现、迁移、审计等任务。这里列举的是可能的作用，不是要求每篇文本具备这些内容，也不是要求每段文字只能承担其中一种作用。

“提供背景”“说明来源”“补充说明”只是内容的名目，不能代替保留理由。需要进一步说清：这段背景解释了什么？这个来源支持了什么？这项补充解决了读者的什么问题？如果只能回答“因为查过”“因为讨论过”“因为确实发生过”，还没有建立它与阅读任务之间的关系。

发现一句话依赖未交代的背景时，不要立即开始补背景。应先判断这句话是否必要。必要的内容，应建立理解它所需的前提；不必要的内容，应从文本中删除，而不是通过增加定义、路径、历史介绍或解释，使它变得完整却仍然无关。

内容选择也不能用版面位置代替。放在开头、结尾、脚注、附录或“补充说明”中，都需要与相应的阅读任务有关。附录可以承载不宜打断主线、但有助于核查或复现的细节；它不是无法说明用途的信息的默认收纳处。

**内容有用，不等于必须简短；内容详细，也不等于有用。** 必要的动机、推导、例子、反例、边界和解释应当充分展开。删去这些内容虽然可能缩短篇幅，却会让读者自行补足理解过程。内容取舍的目标是保留足够的信息完成表达任务，而不是把文本压缩到最短。

## 编写语境与阅读语境

本节所称“编写者”，指参与形成文本的主体，可以是一方，也可以是多方。在人机共同写作中，用户与 AI 都可能参与分析、选择和表述；不能把其中一方的工作过程、已有知识或判断，无条件地归属于双方。

**编写语境**包括参与者为形成文本而接触的材料、交换的信息、进行的比较、作出的判断和经历的修改。各参与者掌握的信息可能不同；只有实际共同建立的部分，才能视为他们之间的共同背景。

**阅读语境**包括目标读者的阅读目的、允许预设的知识、进入文本的位置、当前可见的内容，以及已明确指定的参考材料。阅读语境应由文本的使用方式确定，不能直接照搬编写者当时的工作现场。

两者之间必须区分两种关系：**编写者已经知道，不等于读者已经知道；编写者为了形成文本需要知道，不等于读者为了使用文本也需要知道。** 前一种关系决定哪些前提必须建立，后一种关系决定哪些内容应该传达。

这种区分不要求把目标读者设定为固定的第三个人。参与编写的用户也可能是成果的主要读者；同一个人参与讨论、检查修改和独立阅读成果时，任务与可用背景仍可能不同。是否允许调用此前讨论，取决于当前文本的用途，而不只取决于读者是不是同一个人。

协作交流可以说明材料比较、修改理由、待确认事项和执行情况，以便参与者共同决策。将内容写入另一份成果时，应重新依据那份成果的目的组织，不能默认把交流记录整理一下就成为正文。反过来，如果交付物本身就是会议纪要、决策记录或协作过程说明，相应经过便可能属于它需要表达的内容。

需要叙述行为或判断时，应准确区分主体和状态。AI 阅读了某个文件，不代表用户也阅读过；用户提供了某份材料，不代表认可其中全部结论；一方提出的解释，不代表双方已经确认。与阅读任务无关时，不必介绍这些分工；必须介绍时，不使用含混的“作者”“我们”掩盖差异。

## 区分材料来源、论证依据与形成过程

信息是否保留，不能只按它来自哪里、是谁发现的，或是否产生于编写过程分类。同一份材料可能同时提供方法依据、数据证据和旧实现记录；应分别判断其中哪些内容与当前文本有关。

“编写时查阅了材料 A”说明一次查阅行为；“估计量采用材料 A 中的定义”说明方法依据；“某项结论由材料 A 中的数据支持”说明证据关系。它们可能涉及同一份材料，却承担不同的表达任务。不能把查阅清单当作论证，也不能为了删除查阅经过而删去必要引用。

用户交给 AI 的写作规范、旧文件和参考代码，首先是供协作使用的材料。要求 AI 阅读它们，不等于要求把它们全部列入成果的“参考来源”。规范可以约束成果怎样写，却不因此成为成果所讨论对象的方法来源。旧代码可以帮助识别问题，却不因此要求读者先了解旧代码。

说明一项选择时，应直接交代选择的内容、适用条件及其依据。只有当历史比较本身有助于理解或评价这项选择时，才介绍历史。不要用“旧版怎样，所以现在怎样”代替当前规则的定义，也不要因当前规则已经写清，就默认可以附加旧版沿革。

数据来源、方法归属、必要引用、输入依赖、版本身份和复现条件不能与无关的编写经过混为一谈。某个版本号或运行批次是否需要保留，应看它是否用于定位证据、区分结果或复现实验，而不是看它形式上是否“像过程信息”。需要追溯时，应提供准确标识和使用方式；不需要时，不把全部查阅、尝试和运行经过写成读者必须阅读的叙事。

整理过程信息时，不得改变证据状态。尚未执行的验证不能写成已经通过，无法取得的结果不能写成已经观察到，影响结论的缺失和限制不能为了正文整洁而隐藏。可以删除无关的操作经过，但必须保留会影响读者判断的事实、依据与边界。

本节只规定信息是否进入文本及如何组织。**不写入正文，不等于删除源材料、日志或实验记录。** 材料的保存、移动和删除，仍遵循相应的授权与收纳规则；也不因为正文不收录某段经过，就自动为它新建附录、日志或其他文件。

## 三个关键概念

- **Audience model / 受众模型**：文本所设定的目标读者及其阅读条件，包括已有知识、未知背景、阅读入口和任务目标。只写“面向工程师”或“面向领导”，通常不足以约束具体表达。
- **Common ground / 共同基础**：在当前交流或阅读情境中，可以合理视为编写者与目标读者已经共享的信息。某位编写者的项目经历，或多位编写者在讨论中形成的背景，不自动属于它。
- **Pragmatic presupposition / 语用预设**：一句话在表达新信息之前，已经要求读者接受、知道或能够识别的背景条件。写作者必须检查这些条件是否已经成立，不能把自己的知情状态当作依据。

基本约束：当前内容所依赖的前提，只能来自目标读者合理已有的知识、本文已经建立的信息、阅读时确实可感知的环境，或明确指定的外部材料。

引用外部材料时，应明确它是什么、在哪里，以及读者需要借助其中什么内容。给出文件名或链接，只解决了部分定位问题，不自动完成概念定义，也不自动证明这份材料值得读者阅读。

## 建立稳定的读者模型

避免未经建立的读者前提，不要求所有文档从零解释所有知识。专业文档可以预设与目标读者相符的领域知识。例如面向量化研究人员的报告，可以不重新解释收益率、回撤或主力合约；但不能因此假定他们知道某个内部 Notebook、临时变量、历史方案或项目命名约定。

写作前至少应确定：

- **目标读者与任务**：谁会使用文本，是为了理解结论、学习方法、复现实验、修改代码，还是执行规则。
- **允许预设的知识**：哪些领域概念、工具用法和约定可以直接使用，哪些需要在文本中建立。
- **不得自动预设的背景**：未被指定为阅读材料的旧实现、内部会议、此前聊天、项目历史和非公开命名。
- **可能的阅读入口**：读者是否顺序阅读全文，还是会直接进入某个章节、Notebook、函数说明或独立文件。
- **可用的外部参照**：哪些文件、数据、代码或其他材料属于约定的阅读范围，以及如何准确定位。

读者模型应在全文保持稳定。不能前面按初次接触者解释，后面突然要求读者熟悉未介绍的内部对象；也不能在没有说明的情况下，改变“当前”“这里”“这个方法”所依赖的参照范围。

独立可读不等于重复全部背景。可以使用前文定义和明确引用来避免重复，但应让读者知道需要什么前提、到哪里获取，以及返回后如何继续。可能被单独打开的内容，不能把默认顺序阅读当作唯一的理解条件。

## 按理解任务组织，而不是按工作经历排列

编写者可能先看旧代码，再讨论差异，随后修改方法、执行实验，最后回头补充定义。这是工作顺序，不必成为文本顺序。

文本应按读者需要建立的关系组织：先确认讨论对象和问题，再说明理解后续内容必需的定义、条件和依据，使方法、证据与结论能够相互对应。具体顺序由文体和阅读任务决定，不要求所有文本使用同一模板；但不得仅因某项内容最先被查到，就把它放在最前面。

介绍方法时，应让读者首先知道方法是什么、使用什么信息、在什么条件下成立，以及怎样使用或判断结果。介绍系统时，应首先建立当前结构、职责和约束。编写迁移说明或决策记录时，则可以按变更关系或决策过程组织，因为这些过程本身就是阅读对象。

概念、规则和引用对象应在需要时得到定义或可靠定位。不要先使用一串尚未建立的名称，再让读者到后文猜测它们之间的关系。必要时可以先给整体概览，但应区分概览与具体定义，不能把“提到过”当作“已经解释清楚”。

## 总约束与检查顺序

检查应覆盖整篇文本、章节、段落和句子。不能只检查局部措辞，而不检查整段乃至整章是否需要存在。

**先检查内容作用。** 这一部分帮助读者理解或完成什么？删除以后会缺少什么依据、条件、解释或操作信息？它的保留理由是否仅仅是“真实发生过”“编写时用过”或“可以补充背景”？

**再检查组织关系。** 这一部分与前后内容如何连接？它是在解释对象，还是在复述编写者接触对象的经过？是否为了保留一项无关信息，额外引入了更多必须解释的背景？

**然后检查读者前提。** 这个概念，读者凭什么知道？这项背景属于允许预设的知识，还是仅为某位编写者所知？规则所依赖的条件是否已经建立？拿掉未约定提供的聊天记录、会议历史和工作现场后，必要内容是否仍能被理解？

**再检查指代与范围。** “它、这里、上述、之前、继续、仍然、原来、现有方式、同样处理”分别指向什么？参照对象能否由当前文本、约定环境或明确引用稳定确定？一条规则的适用对象与成立条件是否清楚？

**最后检查信息状态与表达。** 谁提出、执行或确认了相关事项？事实、推测、示例和已确认规则是否区分？是否存在不必要的限定词、未定义的内部名称，或用熟悉的措辞掩盖尚未成立的判断？

“继续按照原来的方法”“和上次一样”“这里不再处理”“按照现有逻辑即可”“具体过程见之前内容”等表达，不能独立承担规则定义。它们并非词语层面的禁用项；参照对象已经可靠建立且关系确有必要时，可以使用。反过来，即使所有名称都已解释清楚，无关内容仍不应保留。

## 通用规则与具体示例的边界

从一次错误中提炼规范时，应区分错误发生的载体、对象与真正使其成为错误的条件。一次内容选择错误发生在 Notebook 中，不意味着相应规则只适用于 Notebook。若换成报告、技术说明或规则文件，判断仍然成立，就不应在通用条款中无故加入载体限定。

通用并不意味着删除所有条件。确实只针对某类文本、某种读者或某项任务的要求，应保留相应范围。应当删除的是偶然附着在案例上的条件，而不是决定规则是否成立的条件。

规则应说明可以迁移的判断标准，示例应展示标准如何应用。不要只把某个错误句子改顺，就把修改结果当作新规则；也不要把案例中的技术参数、文件名和业务选择直接提升为全仓要求。

完整示例应提供足够的任务背景，展示不合适的文字，指出错误发生在哪个判断环节，并给出能够体现修正原则的表达。需要时说明：同一项信息在另一种明确任务下为什么可能应该保留。示例本身也应当可读，不要求读者先恢复触发它的聊天。

## 示例与解释

以下示例用于解释写作判断。示例中的样本数量、文档名、批次时间、技术参数和目录路径，不构成本仓库的数据事实、业务规则或目录约定。

### Demo 1：研究报告面向外部读者，却写成了内部工作日志

**任务与读者**

编写一份策略分类研究报告，说明分类方法，并比较不同类别策略在统一回测条件下的表现。目标读者熟悉量化研究，但没有参与报告编写。示例假定已经核实：策略清单包含 60 项规则，其中 45 项具有完整可用的回测结果。

**不合适的表达**

> 本报告承接策略生成研究，分类依据为所有思维导图和 60 条策略清单。新上传的《固定区间研究》仅保存策略清单与任务预览。本报告实证部分读取此前交付包《回测总控》在 2026 年 9 月 30 日 03:43 批次保存的统计与曲线。

**问题分析**

这段话同时存在内容选择和读者前提问题。

“承接”“所有思维导图”“新上传”“此前交付包”调用了尚未建立的背景。读者不知道这些对象的身份与关系，也不知道上传行为与自己的阅读有什么关系。

但即使把文件名、上传经过和批次时间解释完整，仍需回答另一个问题：读者为什么需要先了解报告是怎样制作出来的？当前任务要求说明研究对象、分类方法、实证样本与结论，不是复原材料交接过程。

其中应保留的是会影响证据与结论边界的信息，例如实际纳入比较的样本范围。不能把“某个 Notebook 没保存输出”直接等同于“相关实验没有执行”，也不能用文件处理经过代替已经核实的样本说明。

**合适的表达**

> 本报告研究如何对规则策略进行分类，并比较不同类别策略在统一回测条件下的表现。研究对象包含 60 项策略规则，其中 45 项具有完整可用的回测结果，构成本报告的实证样本；其余策略不参与本次绩效比较。当前实证结论以这 45 项策略为依据。

这里直接建立研究任务和证据范围。具体分类依据、回测条件和结果应在相应位置展开。若复现需要定位某个结果批次，应在结果引用或复现说明中给出准确标识，而不是要求所有读者先阅读一次材料整理经过。

**对应约束**

先判断读者需要知道什么，再判断必要信息的前提是否充分。不能通过解释“新上传”“此前交付”等过程性措辞，代替对相关内容是否应该出现的判断。

### Demo 2：Notebook 把方案形成过程当成方法说明

**任务与协作背景**

用户要求 AI 编写主力合约识别方法的 Notebook，并提供历史主力拼接 Notebook 与 `testing_10.ipynb` 作为参考。AI 阅读并比较两个实现，与用户在聊天中讨论日期处理、换约条件和异常情况，再据此形成方法说明。

用户提供材料、AI 阅读比较、双方讨论选择，是这个示例中的协作过程。目标文本用于说明如何识别主力合约，而不是说明两个旧实现如何演变成新实现。目标读者可以熟悉期货概念，但不必读过参考代码，也不必参与过双方讨论。

**不合适的表达**

> 参考来源：历史主力拼接 Notebook、`testing_10.ipynb`、`AGENTS.md`。
>
> 两份参考实现与正式方案的关系：旧 Notebook 换约后会覆盖当日主力，`testing_10.ipynb` 区分信号日与生效日，因此本方法采用后者的日期处理。两份实现均使用 1.10 倍成交量门槛，本方法予以保留。旧 Notebook 未限制换约方向，`testing_10.ipynb` 只允许向更晚退市的合约切换，本方法采用后一种规则。

**问题分析**

这些比较可以帮助用户检查 AI 是否读懂参考材料，也可以帮助双方决定采用什么方法，因此适合在相应的协作交流中出现。但它们不因具有协作价值，就自动成为方法说明的正文。

第一段把不同用途的材料混列为“参考来源”。写作规范约束文本如何编写，旧代码提供实现参照；二者都不因此自动成为主力识别方法的论证依据。

第二段按“旧实现如何、另一实现如何、现在保留什么”的顺序组织。读者必须先理解两份实现，才能提取当前规则。即使名称明确、路径完整、比较准确，也仍然需要判断这一历史关系是否有助于使用当前方法。

这里的关键错误不是背景交代得不够，而是把方案形成过程当成了读者理解方法的路径。补充旧文件介绍，或者把整章移入附录，都不能自动解决它与阅读任务之间的关系。

同时，“双方讨论过”不意味着双方做过完全相同的工作。若需要报告比较过程，应准确区分谁阅读了材料、谁提出了选择、哪些判断已经确认；但方法正文通常不需要为说明规则而介绍这些分工。

**合适的表达**

下面只展示“正常换约”段落，初始化与异常处理应在各自的位置定义；参数仅用于演示表达方式。

> 正常换约时，在交易日 \(s\) 收盘后，使用该日已完成的成交量信息，确定下一交易日 \(t\) 的主力合约。候选合约须在 \(t\) 日仍具备交易资格，且退市日期晚于当前主力。
>
> 在符合条件的合约中确定挑战者。只有当挑战者在 \(s\) 日的成交量严格超过当前主力的 1.10 倍时，才于 \(t\) 日切换；否则保留当前主力。判定不使用 \(t\) 日结束后才能获得的完整成交量，以避免使用决策时尚未可得的信息。

当前规则、信息时点、比较条件和方法理由都直接陈述，不依赖旧实现曾经怎样处理。挑战者的选择顺序、并列处理、初始化和异常规则，应在完整方法中明确，而不是让读者去旧文件中补齐。

另一个常见表述是：

> 期限结构继续使用真实合约原始价格，不使用这里的累计复权因子。

读者理解“期限结构”和“复权”，不等于知道“继续”延续什么，或“这里的累计复权因子”指哪个对象。必要内容应直接说明为：

> 连续序列使用复权价格处理换月跳跃；期限结构指标使用同一时点各真实合约的原始价格，以保留不同合约之间的真实价差关系。

前一处重点纠正内容选择，后一处重点纠正未建立的参照。两种检查都需要执行，不能相互代替。

**任务改变时如何判断**

如果另一份交付物明确用于比较两种主力识别方法、说明兼容性变化或指导迁移，那么旧规则与新规则之间的差异就属于那份文本的表达对象。此时应保留必要比较，并解释差异会改变什么结果或操作，而不是只写“保留了什么、修改了什么”。

**对应约束**

参考材料参与了方案形成，不构成将其列入正文的充分理由。当前方法必须能够独立说明；历史关系能否保留，由它对阅读任务的作用决定，而不是由它是否放在“补充”位置决定。

### Demo 3：不要把编写者所处的情境，当成文本已经提供的信息

**情境依赖的反例**

> 前行 200 米，在路口左转，再沿右侧道路前进。

这句话只有在读者位于预定起点、朝向预定方向，并能识别相关道路时，才足够明确。现场指引可以借助双方确实共享的环境；脱离现场使用的路线说明，则需要建立相应起点、方向和参照。

问题不在于指示语天然不清楚，而在于编写者假定的环境是否也是读者实际拥有的环境。

**规则文件中的同类问题**

> 保持现有方式，不要使用旧方案。新增内容与上次一致，不再调整目录结构。

对于刚进入仓库的 Agent，这段话没有形成稳定的执行规则。它仍然不知道“现有方式”与“旧方案”分别是什么，如何区分，适用于哪个目录，以及什么行为构成违反。

在这个示例设定的目录约定下，可以直接写：

> 新增策略统一放入 `strategies/`。每个策略对应一个 `.py` 文件；不新增按“人类可读／机器可读”划分的平行目录。

这里给出了可定位的对象、操作规则和限制，不需要恢复此前讨论。路径仅为本例设定，不构成本仓库实际目录要求。

**问题分析与对应约束**

这类内容通常本身需要传达，问题在于表达依赖了未经建立的情境。应补足必要对象、条件、规则和范围，而不是仅把“现有”“原来”换成另一个含混名称。

“这里”“之前”“当前”“原来”“继续”“同样处理”等表达需要借助上下文解释。使用时，检查它们的对象是否已经通过当前文本、约定环境或明确引用建立。领域知识不能替代项目对象的定义；编写者当时看得见的文件、目录和会话，也不能被默认视为读者当前看得见。

## 最终要求

每项保留的内容都有与阅读任务有关的作用，每处必要的前提都有建立依据，每次指代都有可确定的对象，每次语境切换都先建立再使用。

文本不必重演编写者的全部经历，也不应为了摆脱过程叙述而删去必要的依据、解释和限制。应当使目标读者在约定的阅读条件下，得到完成任务所需的信息，而不必额外恢复编写现场。

# 数据呈现附带中文

适用于 `R02_Market_Data` 和 `R04_Research` 中面向读者的数据表预览，包括 Notebook 的 `display`、末行返回的表格、HTML 样例、终端表格和 GUI 数据结果表。

- 英文字段以 `英文字段名（中文含义）` 呈现；作为表格索引的字段名或范围键也须附带中文。已经使用中文的摘要表头可保留。
- 中文仅用于展示副本，例如 `display(preview_df.rename(columns=column_labels))`。计算、连接、过滤、缓存、Schema、Parquet、CSV、JSON、状态协议及原始日志仍使用原字段和原值；状态枚举不因表头展示而改写。
- silver 字段的中文直接读取当前权威 Schema 的 `field_name_zh` metadata，不维护另一份字段翻译。研究派生字段、检查指标和展示专用范围键在所属 Notebook 或入口中定义明确的中文映射，含义不确定时先向用户确认。
- 共用研究标签可以放在独立的 `export` 单元格供下游导入；纯展示定义不标记为 `demo-dependency`，不得为更新表头改变算法、局部 Schema 或既有数据产物身份。
- 历史证据、归档、参考材料及已经落盘的数据不为展示偏好批量改写。图表和纯说明性表格按读者任务使用中文，不机械套用数据字段表头格式。

具体入口和同步边界见[采集规则](R02_Market_Data/a01_Collection/AGENTS.md#共享配置与-notebook-展示归属)、[湖仓展示规则](R02_Market_Data/a02_Lake/AGENTS.md#33-notebook-开篇-schema-契约呈现)、[operations 规则](R02_Market_Data/a01_Collection/operations/AGENTS.md)及[研究规则](R04_Research/AGENTS.md#notebook-与代理)。

# 协作纠错与执行状态

由于言语与事实后果可能脱节，Agent 不能仅凭改变说法，就宣称先前行为及其后果已经改变或消除。尤其不得将后续改口称为“撤回”，暗示已发出的消息、已耗费的用户时间或已造成的影响因此消失。纠错时只准确说明哪里说错了、纠正了什么、实际改了什么，不花费篇幅展示自责或宣称负责。

# 层级编号与资源归属

编号分配给所属层级中职责明确的业务单元。同一单元的 Notebook、Python 文件、配置与附属资源共同归属该编号；编号用于业务归属和浏览排序，实际依赖及执行顺序由调用关系确定。

- 仓库顶层编号分区使用 `RNN_Name`，大写 `R` 标识仓库分区，与内部小写 `a`、`b`、`c` 业务层级区分；保留既有编号和业务词根。`config/`、`environment/` 等按职责命名的共用目录不机械补编号。Python 导入从仓库根开始使用完整包路径，例如 `R02_Market_Data.a01_Collection.b00_04_staged_path_transaction`；不依赖将某个业务子目录另设为模块搜索根。
- `a`、`b`、`c` 等字母表示所属工作流的业务层级。新增独立职责才分配新编号；增加文件格式、数据目录或输出目录不占用下一个编号，资源目录的嵌套也不自动产生下一业务层级。
- 同一编号下的文件及资源保留同一个业务词根。附属资源使用 `<所属单元完整基名>_<资源用途>`，例如 `c01_MainContinuousAdjustment.ipynb`、同名 `.py` 与 `c01_MainContinuousAdjustment_DemoData/`；资源用途后缀不改变其所属单元。
- 同一父级内，不同职责不得借用同一编号；同一职责也不得仅按代码、数据等资源类型各自编号。派生业务单元的编号形式及支撑单元的保留编号由目录规则定义。
- 多个单元共有且归属于父级的材料按用途命名，例如实验的 `Data/`、`Tracking/`。它们不占子级业务单元编号，也不改变既有按职责命名的 `checks/`、`operations/`、`referance/` 等目录。
- 目录规则明确具体业务层级和资源用途，并反向引用本节。历史证据、归档及只读旧项目保留内部编号、原始字节和来源路径标识；外层目录更名须获得对具体受保护目标的明确授权，不能据此改写内部历史文件。当前入口迁移须同步活动引用、说明和 Git 收纳规则，不因路径整理改写已落盘的数据身份或内容。

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
- 环境检查：`E:\anaconda3\envs\latitude_env_v2\python.exe R02_Market_Data\a01_Collection\b00_01_verify_runtime.py`。

# 长时间任务的人工启动、后台执行与可见监控

- `R04_Research` 环节函数 Notebook 的 demo DLC 流程遵循[研究规则](R04_Research/AGENTS.md#notebook-dlc-流程)。已提交作业由 DLC 维持，DLC 控制台满足云端可见监控要求；Notebook 使用普通 Python 每 60 秒查询同一身份，不另设 worker、HTTP 看板、监督心跳或 Agent 定时恢复。以下 worker／monitor 及原子状态规则适用于本地后台业务进程，不要求为 DLC 增加本地监督层。
- 预计超过 10 分钟的任务须由用户在当前交互中明确授权一个有清楚边界的批次。可使用与 Codex 回合解耦的 detached/background worker；这不授权定时任务、常驻守护、自动恢复或未来批次。
- 时长、正式湖写入或后台执行不自动要求小样本、测试湖或追加 dry-run。此类可选检查只在当前交互明确要求时执行；用户要求跳过时必须跳过，不得以未做样本阻塞正式批次。
- worker 必须配独立、可见且不依赖 LLM 的 Terminal、pane 或所属工作流规定的总控台，持续展示阶段、可量化进度、耗时、心跳新鲜度和失败信息；仅写日志不算监控。
- worker 和 monitor 不得依赖 Codex 回合存活。只做一次有界健康检查，确认 worker、业务子进程、心跳和可见 monitor 正常后立即结束回合；不用 sleep、轮询或 tail 维持 Agent。
- worker 不自动重试。普通失败、配额停止或监控异常须停止后续阶段、保留状态/日志/事务证据，由操作者核查后显式决定新的有界批次。
- Windows monitor 读取原子状态文件须允许 `ReadWrite | Delete` 共享，并立即释放句柄，不能阻塞 `os.replace`。状态发布使用同目录临时文件、flush/fsync 和原子替换；只对该替换的 `WinError 5/32` 短时有界重试，不扩展到 API、业务阶段或事务。发布最终失败须终止业务子进程并保留现场。

# 草稿区与文件收纳规则

`R00_draft_collection_02` 是流动的暂存区，收纳临时文件，以及尚未由用户决定去留、复用或最终目录的材料。其中既可能有用完即可丢弃的一次性文件，也可能有需要保存的数据源质检、实验依据或可复用代码；位于草稿区本身不代表材料没有价值。

- 用途完成后，Agent 应说明具体材料的用途、结果、保存或复用价值及建议去向，由用户确认其重要性、是否保留或删除、是否复用及最终收纳目录。一次性验证已经完成，不构成自动删除依据。
- 用户确认删除的材料才可删除；确认保留的证据或复用文件，在最终目录确定后移出草稿区。已有明确授权的具体文件或批次按授权范围执行，无需重复确认；未决定的材料继续暂存，Agent 不自行接纳、删除或安排永久归属。
- 全部待办材料完成处置且没有新增材料时，草稿区应不留文件或子目录。实际工作不断产生新材料，允许暂时非空；不得为了清空目录跳过用户确认，也不得把暂存位置当作最终收纳位置。

- 分区结构见 [README](README.md)：市场数据第二层固定为 `a01_Collection`、`a02_Lake`；GUI 留在采集目录，正式湖根不另加 `futures_lake` 一层。 采集目录的编号按层级递进：`a01_Collection/bNN_业务组/cNN_环节`；与业务组同层的支撑文件使用 `b00_NN`，具体入口见采集规范。
- 已确定所属项目的历史、日志、状态及验收记录归产生它们的项目。采集运维使用 `R02_Market_Data/a01_Collection/operations/run_history/`；研究记录归所属研究项目。草稿区不得充当正式工作流长期日志仓库。
- 运行输出必须显式定位到所属项目或单元，不能由进程当前工作目录决定。仓库根目录禁止生成 `mlruns/`、`mlartifacts/`、`mlflow.db` 等工具默认输出。MLflow 的数据库地址、Experiment 附件位置和恢复 Run 的附件地址须分别明确并在写附件前核对；仅设置 SQLite tracking URI 不能保证附件归属。路径不符时停止并报告，不回退到默认路径，不以 `.gitignore` 隐藏代替修复。研究细则见[研究规则](R04_Research/AGENTS.md#hydramlflow-与-dlc)，环境验证和教程也遵守本条。
- `R00_draft_collection_01/` 中的实验工作遵循[实验草稿区规则](R00_draft_collection_01/AGENTS.md)，包括 Notebook 与配套输出收纳、默认展示、导出和跨 Notebook 引用；目录业务细则不在根文件重复定义。
- 根目录不得新建或恢复 `scripts/`、`tests/`。尚未确认归属的新脚本、测试、一次性迁移及审计验证工具先放 `R00_draft_collection_02`；脚本和测试按需使用其中的 `scripts/`、`tests/`，不预建或永久保留空目录，不散落在业务目录。
- 已接纳的数据源质量与连接检查归 `a01_Collection/checks/`；来源检查与采集代码的本地测试统一归其 `tests/`，来源检查结果归其 `results/`。`operations/` 只收纳 GUI 及直接支撑它的代码、资源，其测试归 `operations/tests/`、运行记录归 `operations/run_history/`。采集维护历史包保存在 `R05_Old_Projects/collection_maintenance_20261002.zip`，保留包内原路径和摘要，不从包内导入代码。
- 移入草稿不等于接纳：既有入口引用被移动文件时，须修正路径并报告依赖，待用户决定保留、改造或移除。审计发现其他疑似未接纳脚本时先报告，未获具体目标授权不得移动、删除或改写。

# 项目根目录定位约定

以 [.env.template](.env.template) 的代码为唯一定位方式：从当前工作目录向父目录查找同时包含 `.git`、`.env`、`config/settings.py` 的目录，加入 `sys.path` 后再导入 `config.settings.settings`；找不到则报错，不另建定位实现。

# 正式期货湖仓定位、raw 归档与 silver 自动更新契约

- 正式湖仅由 `.env` 的 `FUTURES_LAKE_ROOT` 配置，代码使用 `config.settings.settings.futures_lake_root`。raw、silver、gold 的目录划分不构成 gold 表集合、Schema 或研究方法的全仓定义。
- 在任何目录修改数据湖生产者、消费者、字段、Schema、转换或验证时，必须遵循[湖仓规则](R02_Market_Data/a02_Lake/AGENTS.md)和[可执行契约](config/data_contracts.py)。raw 留证、silver 验收和完成状态不得混同；生产者验证来源及 dirty 输出，消费者和 clean 历史按契约信任正式提交的业务证明。
- 各入口的更新集合、空湖全建、日期/全量写入门禁、来源异常和提交验收以[采集规则](R02_Market_Data/a01_Collection/AGENTS.md)、[采集说明](R02_Market_Data/a01_Collection/README.md)及湖仓规则为准。其中包括生意社只归档原文、特殊案例精确校准、可信完成状态，以及 c08 必须人工显式选择和确认的例外；不得用通用求差或重试替代这些边界。
- 生产契约调整只按用户确认的入口范围实施。发现脚本保留不一致的旧语义时，不得据此弱化契约，也不得顺带批量修改其他业务脚本。gold 输出由所属工作流定义和验证。

# 目录级规范路由

- 在 `R00_draft_collection_01/` 内创建、修改或执行实验，读取[实验草稿区 AGENTS](R00_draft_collection_01/AGENTS.md)。
- 在采集目录树内工作，读取[采集 AGENTS](R02_Market_Data/a01_Collection/AGENTS.md)；涉及 worker、monitor、状态或失败处置，再读[operations AGENTS](R02_Market_Data/a01_Collection/operations/AGENTS.md)。在其他目录涉及湖仓契约，也须遵守上节规则。
- 在 `R04_Research` 内工作，读取[研究 AGENTS](R04_Research/AGENTS.md)。主力识别和复权的 CU、RB 全历史 demo 归生产单元的 `cNN_Name_DemoData/`；分界采样在同一 `b03_Sampling/` 内由位置与价格执行两本 Notebook 分工，各自保存 demo，轴及初始单位校准并入位置定义，不保留独立校准入口。收益／波动率建模统一归一个 b 主题，c01 已实现分钟贡献、每日季节曲线及训练期初始回填，c02 已实现分钟去季节、区间汇总及模型输入转换；c03 已实现六组收益模型的 QML 联合拟合、预测和状态更新；c04 已实现 ARMA、ARFIMA、HAR、MEM 的直接观测拟合、预测和状态更新。close 重采样按只读规则保护，采样生成状态见研究说明。首个按日收益／波动率实验保留既定 1,944 项配置、每作业 10 节点和无金额上限。方法完整 demo 的季节 DLC 阶段已成功并回收，观测阶段失败、模型阶段未提交；正式实验尚未完成。环节函数 Notebook 的 demo 已按 15 格重写并通过离线流程检查，新版首作业因云依赖遗漏 polars 而失败；依赖、导出标签和下载顺序已修复，新季节作业 train1v6y4ne9zpv 已提交。用户已明确授权失败排错、重新提交及每 30 分钟唤醒本聊天，目标为 24 个完整方法 demo 产物回收验收，完成后停用定时任务；完整验收尚未完成。该 demo 排列不约束实验层。实际执行和验收状态见研究说明。原特征工程留存与已知阻塞见[研究说明](R04_Research/README.md)。研究代理与采集完整 PythonExporter 导出分别遵循所属目录规则。其他研究项目遵循上方索引中的项目入口。
- `R05_Old_Projects` 遵守[归档只读规则](R05_Old_Projects/AGENTS.md)。归档规范中的旧采集路径属于迁移前标识；当前维护入口以根 README 和采集 AGENTS 为准，不改写归档文件。
- 目录业务细则在所属 AGENTS 中定义，根文件只保留全仓规则和必要路由，不复制目录细则。
