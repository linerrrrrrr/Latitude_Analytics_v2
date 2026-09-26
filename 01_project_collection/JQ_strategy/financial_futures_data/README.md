# 金融期货本地数据工作流

本目录是 `JQ_strategy` 的正式子项目，用于建立以下人工闭环：

```text
正式 silver 日历（只读）
→ 本地缺失检测与聚宽代码生成
→ 用户在聚宽研究 Notebook 手动执行
→ 聚宽固定名单文件
→ 用户手动下载并覆盖本地同名文件
→ 从头运行本地 Notebook：自动验证、入库并删除已消化 ZIP
→ 自动再次检测剩余缺失，最后输出下一批代码／完成说明
```

当前已完成清单第 01—09 项，并进入第 10 项的半自动全白名单回补。每次下载后从头运行 Notebook，自动消化并删除 ZIP、重新检测并输出下一批代码和预计剩余轮数。初始全量尚未完成；不能把来源确认缺失或陈旧上游日历误报为全量更新到最新。

## 目录布局

```text
financial_futures_data/
├── AGENTS.md
├── README.md
├── financial_futures_collection_policy.py  # 项目局部白名单与频率
├── financial_futures_local_contract.py      # 项目库与传输可执行契约
├── financial_futures_fetch_planner.py       # 精确缺失与有界拉取规划
├── plan_financial_futures_fetch.ipynb   # 每轮 Run All：消化删除、规划、代码／完成提示
├── import_financial_futures_file.py     # 固定文件校验与四表事务导入
├── experiment_records/
│   ├── joinquant_fetch_protocol/         # 按实际执行时间保存的聚宽协议实验记录
│   ├── local_fetch_planner/              # 带时间的本地规划器验收证据
│   ├── local_notebook_export/            # 带时间的 Notebook 与模拟导出验收证据
│   ├── local_file_import/                # 带时间的模拟导入及失败/幂等验收证据
│   └── formal_roundtrip/                 # 准备、真实往返及半自动闭环的带时间验证记录
└── data/
    ├── inbox/                           # 聚宽手动下载的固定名单文件
    ├── warehouse/                       # 已创建的 financial_futures.duckdb
    └── staging/                         # 当前导入批次临时产物
```

规划 Notebook 和导入入口均已创建。三个运行时数据目录通过各自 `.gitignore` 保留目录骨架，实际内容不进入 Git。`experiment_records` 与运行时行情数据分开，保存可审计的实验文本并进入版本控制。

每次实际运行使用独立的 `YYYY-MM-DDTHHMMSS.ffffff+0800_round_NN_vN` 目录，时间来自探针报告初始化时刻而不是本地文件修改时间。完整记录包含收到的 `raw_output.txt`、从完整 BEGIN/END 信封原样提取的 `result.json`、对应 SHA-256 sidecar、`analysis.md` 和用户实际执行单元格的非执行源码快照；异常或截断运行即使不能产生 `result.json`，也必须保留原始文本与分析。默认直接在对话中交付单元格，结果返回后再从已完成对话提取源码进入该时间戳记录，不为交付本身额外创建 `.py` 文件。历史记录不得被修正版或重跑覆盖。

## 固定边界

- 正式数据湖只读，本目录的任何流程都不得回写正式 `raw`、`silver` 或 `gold`。
- 项目数据库只服务 `JQ_strategy`，不冒充稳定 silver 数据库。
- 本地流程不直接调用 JQData；聚宽侧代码始终由用户手动复制和执行，文件始终由用户手动下载。
- 每次聚宽运行最终只发布 `JQ_FINANCIAL_FUTURES_TRANSFER.zip`；本地人工下载后覆盖 `data/inbox/` 下的同名文件。探针文件名永远不能进入正式导入入口。
- 暂存文件不构成成功导入；只有 DuckDB 事务、导入账本和提交复读共同确认后，才报告完成。

## 金融期货范围

项目局部白名单固定为中金所 `CCFX` 的 8 个品种：

| 品种代码 | 品种 |
|---|---|
| `IF` | 沪深 300 股指期货 |
| `IH` | 上证 50 股指期货 |
| `IC` | 中证 500 股指期货 |
| `IM` | 中证 1000 股指期货 |
| `TS` | 2 年期国债期货 |
| `TF` | 5 年期国债期货 |
| `T` | 10 年期国债期货 |
| `TL` | 30 年期国债期货 |

只使用正式日历枚举的固定月份合约，不纳入 `8888/9998/9999` 合成代码。白名单以 `financial_futures_collection_policy.py` 为唯一可执行来源，不修改全项目商品事实采集白名单。

2026-09-02 的只读基线审计显示：上游共有 687 个白名单固定月份合约，日历水位截至 2026-08-28，形成 68,654 个日线应有键、137,308 个分钟 Session 和 17,248,050 个分钟应有键。这里的数量只是当前水位证据；后续每次运行都按最新正式上游结构重新计算，不把这些数字写成固定上限。

## 应有数据与缺失语义

- 日线按 `contract_code,trading_date` 判断；每个开市合约日只有一个应有键。
- 分钟线按每条 Session 的北京时间 `(session_start_at, session_end_at]` 展开，以 `contract_code,bar_at` 判断；不能只比较日期首尾或总行数。
- 只有权威证据确认休市的 Session 才免于请求；普通 scheduled、疑似休市和非权威旁证仍属于理论范围。
- 当前交易日的数据到次日 00:01 后才进入可拉取范围；未到时的尾部单列，不作为缺失。
- `data_missing` 表示所有理论键与已入库有效行情的差；`fetch_pending` 进一步扣除已经完整请求但来源明确未返回有效行情的键。Notebook 必须同时展示两者，但正常生成的聚宽代码只处理 `fetch_pending`。
- 请求失败、配额停止、截顶、文件不完整或本地提交失败都不能把缺口变成“来源确认无数据”；这些范围仍待下一次人工拉取。
- 白名单或上游结构缩减不自动删除已有项目数据；新增范围自然形成新的待拉取项。

早期 TF 合约从 2012-06-11 起就存在于本地权威日历中，因此保留在理论范围，不用外部上市日期静默裁剪。第 04 项已经确认 `TF1303.CCFX` 整个聚宽元数据生命周期的日线和分钟均为成功零行，而 `TF1312.CCFX` 首个候选交易日存在真实数据；这种完整空响应后续按来源确认缺失处理。

## 轻量数据库与状态模型

项目库固定为 `data/warehouse/financial_futures.duckdb`。DuckDB 无需启动数据库服务，日线与分钟线都保存在同一文件中；当前标准 `latitude` 环境已验证 DuckDB 1.5.4 的单文件事务、回滚和只读重开行为。第 05 项已经冻结列级 Schema，但数据库仍只允许由第 08 项正式导入入口按需初始化。

第一版只保留四类持久表：

| 表 | 持久内容 | 身份或粒度 |
|---|---|---|
| `futures_daily` | 有效日线行情，不存空响应占位 | `contract_code,trading_date` |
| `futures_minute` | 有效分钟行情，不存空响应占位 | `contract_code,bar_at` |
| `fetch_observation` | 完整成功且无截顶的来源观察及其精确请求覆盖 | 日线为合约日，分钟为合约 Session |
| `ingest_batch` | 文件摘要、聚宽导出运行身份和本地原子提交证据 | 一份具体文件内容 |

理论格点与四个缺失状态不复制入库，而在每次规划时现算：

```text
data_missing = expected_due − 有效行情
source_confirmed_missing =（expected_due ∩ 成功观察覆盖）− 有效行情
fetch_pending = data_missing − source_confirmed_missing
```

成功观察可为 `complete`、`partial_missing` 或 `empty`；后两者仍是完整请求，只是来源少返回或零返回，因此形成持续可见但日常不重拉的缺失。失败、配额停止、中断、截顶或边界不可证明的请求块不形成观察，仍属 `fetch_pending`。一份 manifest 完整的文件可以只提交其中独立成功的块，失败块保持待拉取；manifest 自身不完整则整文件拒绝。

固定文件名不参与幂等判断。导入器按整文件 SHA-256 与内部 `run_id` 识别批次：同一摘要重导不写库，同一 `run_id` 却对应不同摘要时失败。文件内重复业务键一律拒绝；跨批次相同键只有行情业务值完全一致时才可事实层无操作，冲突值不得覆盖。事实、观察和批次账本由一个 DuckDB 事务共同提交或共同回滚。

本地工作流保持单写者：Notebook 的规划格只读，入库格直接调用正式导入函数写项目库；导入与规划不得并发运行。数据库自身不保存约 1,725 万个当前理论分钟键，也不另存 `data_missing` 或 `fetch_pending` 快照。

## 第 05 项：本地行情 Schema 与正式单文件协议

本项已经完成，权威列级定义和逐字段协议位于 [`AGENTS.md`](AGENTS.md) 的两个“第 05 项冻结”章节。第一版刻意保持轻量，只保存聚宽真实小包已经贯通的 `get_price` 字段：

- 日线：`previous_close,open,high,low,close,volume,money,open_interest`，主键是 `contract_code,trading_date`。
- 分钟：`open,high,low,close,volume,money,open_interest`，另存 `trading_date,session_number`，主键是 `contract_code,bar_at`。
- 行情事实只保存有效来源行；`fetch_observation` 保存成功请求的精确 Session 覆盖，`ingest_batch` 保存文件、manifest、运行和事务计数证据。
- v1 不把 `get_extras` 结算价、上一结算价或涨跌派生列混入协议。将来需要时必须显式迁移 Schema 和协议版本。

正式单文件固定为：

```text
data/inbox/JQ_FINANCIAL_FUTURES_TRANSFER.zip
├── manifest.json
├── request_blocks.jsonl
├── futures_daily.jsonl
└── futures_minute.jsonl
```

协议标识为 `jq_financial_futures_transfer`，版本为 `1.0.0`。成员统一采用 canonical UTF-8 JSON；manifest 对三个 JSONL 的字节数、SHA-256 和记录数负责，整 ZIP 摘要由远端发布后输出并由本地不可变快照重新计算。一个实际 `get_price` 来源请求可携带多个连续合约日或 Session 观察块；来源请求任一处失败会使该组全部观察块失败且不形成事实或观察，其他完整来源请求仍可进入 `partial` 文件。协议或 manifest 自身不完整时整文件拒绝。

本地导入前必须先把 inbox 文件复制并原子固定为 `data/staging/<file_sha256>.zip`，所有校验和事务只读取该快照。四表共同提交；同摘要幂等无写入，同 `run_id` 异摘要或业务值冲突整批失败。第 05 项只冻结契约，没有创建数据库、Notebook 或导入器。

## 第 06 项：精确缺失检测与有界拉取规划

本项已经完成，正式实现为 [`financial_futures_fetch_planner.py`](financial_futures_fetch_planner.py)，稳定字段、DDL 与协议标识集中在 [`financial_futures_local_contract.py`](financial_futures_local_contract.py)。规划器只读正式行情日历和已有 DuckDB；数据库尚不存在时直接按空库状态计算，不创建空库或占位文件。

规划器同时计算 `data_missing`、`source_confirmed_missing` 与 `fetch_pending`。日线逐合约日比较；分钟在空库的完整缺段上保留 Session 范围和计数，只有存在事实或成功观察的 Session 才展开理论分钟求内部精确差集，因此能识别漏掉的一根分钟线而不必常驻物化全宇宙分钟键。

一次计划区分两层：观察块是一个合约日或一个 Session，是后续缺失状态的最小证据；来源请求才是一次实际 `get_price` 调用，可合并同合约、同频率且结构连续的观察块。当前每个来源请求最多 38,000 个理论键；正常规划每份传输文件最多选择 250,000 个理论键，并继续受 50,000 个观察块和 64 个来源请求约束。协议兼容硬上限仍为 500,000 个理论键。所有观察、来源请求与完整计划都有 canonical SHA-256 身份。

2026-09-04 使用标准 `latitude` 环境读取真实正式日历的空库验收耗时约 24.6 秒：计算得到 68,654 个日线键、137,308 个分钟 Session 和 17,248,050 个分钟键；数据库文件没有被创建。当前有界计划因“日线优先”和 64 个来源请求上限，选中 5,992 个日线观察块；分钟缺口仍完整出现在覆盖汇总与 137,308 条 Session 缺失范围中，后续批次会继续确定性推进。另用临时合成库验证了完整日线、分钟少一根和整段分钟空缺可分别得到正确状态，精确来源确认缺失合计 121 根。

最终验收记录已按执行时间保存在 [`local_fetch_planner/2026-09-04T023032.039079+0800_item_06_v1`](experiment_records/local_fetch_planner/2026-09-04T023032.039079+0800_item_06_v1)：00:01 就绪边界、相同范围的稳定计划摘要、全未就绪空计划、四表字段顺序、Session 历史编号变化和数据库只读字节不变均通过。另以历史 `as_of` 验证日线与分钟同时进入计划，23 次来源请求携带 1,812 个观察块和 163,684 个理论键（其中分钟 163,080 个），逐分钟边界、键摘要、跨块零重叠及全部批次上限均通过；这些只是本地规划验证，没有调用聚宽或写正式湖。

## 第 07 项：本地 Notebook 与完整聚宽代码

正式入口为 [`plan_financial_futures_fetch.ipynb`](plan_financial_futures_fetch.ipynb)，使用 latitude 内核，每轮运行全部单元格（Run All）。环境格重载模块并清空旧状态；第 1—2 节展示交易日历、bar 日历与项目库契约，第 3 节自动消化 ZIP，第 4—8 节全白名单精确缺失、覆盖、请求范围和剩余轮数，第 9 节定义远端模板，最后第 10 节输出结论或完整聚宽代码。默认使用当前北京时间与全白名单，不再限定两组验收坐标。

最终输出框内 Ctrl+A、Ctrl+C，在聚宽研究 Notebook 重启内核后粘贴到一个单元格执行即可；代码自带压缩后的计划，不需要上传其他脚本。远端模板直接属于本 Notebook，不额外生成一份本地 `.py`。本地运行不会执行字符串里的聚宽请求或文件覆盖。

远端成功后只发布 `JQ_FINANCIAL_FUTURES_TRANSFER.zip`。首个请求或响应验证失败会停止后续调用；有独立成功请求时发布包含完整失败证据的 `partial` 包，没有任何成功请求则保留旧文件且不提示下载。序列化、临时文件校验或原子替换失败也不会把半成品发布为新文件。只有完整 END 标记与下载指令均出现后才手动下载；没有下载指令时，旧同名文件不能当作本次结果。

本项已完成本地干净内核全单元格执行、Python 3.6 语法检查和 19 个模拟场景，包括成功、分钟缺根/空表、OHLC warning 原值保留、API 异常、重复/额外键、空值/Inf/负数量/布尔值、来源警告、首请求失败、序列化失败、原子替换失败、计划篡改、空计划、同名覆盖及受污染全局内置函数。模拟验证不替代旧版聚宽环境实测；随后第 08 项完成导入器，第 09 项另行取得正式协议真实小批次往返证据。

带执行时间的结果、非执行 Notebook 快照和验证源码已归档到 [`2026-09-04T123214.427162+0800_item_07_v1`](experiment_records/local_notebook_export/2026-09-04T123214.427162+0800_item_07_v1/analysis.md)。最终版本完整 Notebook 执行耗时约 59.7 秒，生成 932,915 字符代码，内嵌 64 个来源请求和 5,992 个日线观察块；复制框逐字符复读与计划摘要均通过。数据库没有创建，正式 Notebook 未保存运行输出。

## 第 08 项：固定文件校验与本地入库

用户在 [`本地 Notebook`](plan_financial_futures_fetch.ipynb) 从头运行时，第 3 节自动检查固定文件并复用 [`import_financial_futures_file.py`](import_financial_futures_file.py)。只接受人工覆盖到 `data/inbox/JQ_FINANCIAL_FUTURES_TRANSFER.zip` 的正式文件，不接受 PROBE，不自动下载或调用聚宽。项目库仍是 `data/warehouse/financial_futures.duckdb`，没有新增持久状态表或第二套数据库。

日常只需重复以下流程，不需要 PowerShell、命令行、手工解压或逐轮询问 Agent：

1. 在本地 Notebook 运行全部单元格。若 inbox 有 ZIP，自动校验、入库、复核并删除；若没有，直接检测全白名单日线和分钟数据。
2. 最后一格有待办时，显示预计还需多少轮（包含本轮），并输出本轮完整聚宽代码。复制到聚宽研究 Notebook 执行。
3. 只在收到完整下载指令后，手动下载固定 ZIP、放入本地 inbox，再从第 1 步开始。
4. 没有待办时，最后一格区分“行情全齐”“普通任务完成但来源仍有缺失”“上游日历陈旧”。只有第一种能称为全量覆盖当前就绪范围；同时提示明天北京时间 00:01 后可再运行，上游日历须先同步。

剩余轮数由与本轮完全相同的请求分组、排序和装包上限预排得出，不等于 API 调用数，也不是总行数简单除以上限。它假定当前日历和政策不变、后续请求均完整成功；来源失败、配额停止或新增交易日都会在下一次 Run All 重算。不需要手改日期、批次编号或验收坐标。

2026-09-05 已在独立 latitude Jupyter 内核中连续从头运行两次，验证自动幂等消化、删除 ZIP、无文件继续和完整代码输出；数据库字节不变。14 组导入/结论分支验证及本轮源码证据见 [半自动闭环验收记录](experiment_records/formal_roundtrip/implementation_2026-09-05_semiautomatic_v1/analysis.md)。

每轮开篇和入库前清空旧计划、代码与报告。失败立即停止，后面的代码生成格拒绝使用旧计划，不自动重试。重复下载相同已入库文件时，完整核验后返回 already_imported、零新增，并删除这份已消化 ZIP。

函数默认 `write=False` 仍可用于只校验：不创建或写入数据库，保留摘要快照并返回 `validated_not_imported`。它不是提交前必须额外执行的步骤。模块保留原有 CLI 兼容，但不是用户日常操作的必经入口。

2026-09-04 已将直接入库格接入现有 Notebook，并在独立 latitude Jupyter 内核中取得已入库文件的幂等结果；后续只读复核确认数据库和 ZIP 字节未变。验证驱动自身的变量错误与复核证据均保存在 [Notebook 入库入口记录](experiment_records/formal_roundtrip/recorded_2026-09-04T224703.608803+0800_notebook_import_v1/analysis.md)，不修改前一轮真实往返记录。

每次先从一次打开的 inbox 句柄取得快照，在 staging 写入、flush/fsync、复读，再原子、不覆盖地固定为 `<SHA-256>.zip`；之后只读取这个快照。已存在的同摘要快照必须逐字节相同。本次快照固定后，即使用户又替换 inbox，本批也不会混入新文件的内容。

校验包含 ZIP/CRC/canonical JSON、逐成员摘要与行数、全部成功和失败请求块、观察—来源请求—完整计划三级身份、日线及分钟精确理论键、行情类型/有限值/非负数量、归属和逐块覆盖证据。新文件另只读当前正式日历中本批的合约日期范围，拒绝不存在、权威休市、Session 边界已变化或跨越未计划结构块的坐标，不擅自调整文件。相同 SHA 的历史重导按不可变账本完整复核，不重新把旧批次当作今天的规划结果。

同文件 SHA 重导返回 `already_imported`，数据库无写入；同 `run_id` 配不同文件、文件内重复键或已有行情业务值冲突全部拒绝。跨批次逐值相同的事实保留原首次观察身份，只追加新批次的成功观察。成功空响应和缺根响应形成带 warning 的观察，不填造行情；失败来源组和未执行请求不入观察表。

首次建库在唯一 staging 候选库中完成四表事务和只读复核，再原子、不覆盖地安装到 warehouse；校验或提交前失败不留下正式空库。已有库的批次、观察、日线和分钟同事务提交，任一步失败回滚。错误现场同时记录 `commit_attempted`、`commit_returned` 及回滚失败附注；提交结果不确定或提交后复读/安装失败时，不谎称已回滚，也不删除可能已经提交的证据。成功报告的 `counts` 属于原批次账本，本次是否写库看 `database_write_performed`，幂等重导不会再次插入这些行。

write=True 且数据库最终复核成功后，删除本次已消化 ZIP、清理本次摘要快照和候选临时名；write=False 保留 inbox。删除时先原子移走当前目录项，再核对与已提交快照相同才删除，避免误删入库期间新下载的文件；若内容不同则不覆盖恢复，入口又被占用则两份新文件均保留并停止。占用、删除或恢复失败会留下尚存快照、已移走文件、候选库和带时区错误报告；提交已完成不能声称回滚。报告分别说明 database_state_verified、inbox_deleted 与 rows_inserted_this_invocation。独占锁拒绝并发导入，崩溃残留锁须先核查；历史错误不自动清理。

本地资源门禁为压缩 ZIP 512 MiB、声明展开总量 1 GiB、manifest 8 MiB、单行 JSONL 1 MiB，并继续执行协议中的 64 来源请求、50,000 观察和 500,000 理论键硬上限；正常规划另采用 250,000 理论键内存安全上限。超限停下，不自动拆包。摘要证明完整性与身份，不是来源签名，人工文件还应与聚宽下载报告核对。

本项使用系统临时目录中的模拟行情和数据库验收，正式 inbox、warehouse 均未写入。2026-09-04 13:05:44+08:00 开始的最终验收，76 个场景全部通过，耗时约 102.9 秒：覆盖文件门禁、正常/部分/空响应、同 SHA 数据库字节不变、冲突拒绝、四表回滚、提交后复读失败和锁/快照保护。直接衔接正式缺失规划器时，样本的 121 根来源确认缺失仍可见且待拉取为 0。结果、源码快照和摘要已归档到 [`2026-09-04T130544.909797+0800_item_08_v1`](experiment_records/local_file_import/2026-09-04T130544.909797+0800_item_08_v1/analysis.md)。下一项才是第 09 项正式协议真实小批次端到端验收。

## 第 09 项：真实小批次往返（已完成）

本项历史验收在当时 Notebook 第 3 节显式选择以下两组坐标，日线和分钟都包含；不调整白名单、as_of 或全局批次上限，也不另建取数脚本。此限制已由 2026-09-05 的全白名单半自动流程取代：

| 合约日 | 日线理论键 | 分钟理论键 | 用途 |
|---|---:|---:|---|
| IF2409.CCFX / 2024-06-28 | 1 | 240（上午/下午各 120） | 正常真实行情与交易时段 |
| TF1303.CCFX / 2012-06-11 | 1 | 270（上午/下午各 135） | 早期 TF 成功空响应及来源确认缺失 |

首次空库准备生成 4 个来源请求、6 个观察块、512 个理论键。理论键不是来源必须返回的行情条数；本轮真实返回 IF 日线 1 条、分钟 240 条，TF 日线和分钟均成功零行。所有坐标由正式日历复核，只有仍为 `fetch_pending` 的块会进入代码；导入后本范围无待办，输出空计划，不跳到其他合约。

本项已执行的验收顺序：

1. 本地使用 `latitude` 内核重启并从头运行 Notebook，确认第 7 节本批仅含上述两组坐标；把最后一格的完整代码复制到聚宽研究 Notebook 的干净内核，在一个单元格中执行。
2. 只有出现完整 `JQ_FINANCIAL_FUTURES_EXPORT_V1_BEGIN/END` 与下载指令后，才手动下载正式 `JQ_FINANCIAL_FUTURES_TRANSFER.zip`。返回完整输出与下载文件位置，不能用旧 PROBE 包或没有下载指令时的旧同名文件。
3. 收到文件后核对字节数、SHA-256、运行 ID 和本次计划，再由正式导入器提交这一个小批次；随后核对同 SHA 重导无写入、四表计数和重新计算的精确缺失，全部通过才勾选第 09 项。

`acceptance_contract_dates` 仅保留为显式有界验收参数：不裁剪完整缺失宇宙，未知、重复、未就绪或非开市坐标报错，空范围绝不回退全量。当前正式 Notebook 不传该参数，默认正常全白名单选择；不再停留在已完成的两合约日验收范围。

准备记录归档到 `experiment_records/formal_roundtrip/prepared_..._item_09_vN/`，保存本地准备时间、计划身份、完整生成代码文本和检查结果；聚宽真实运行另建按 `run_started_at` 命名的记录。准备记录不冒充真实来源结果；当前已完成本次真实导入，尚未全量回补。

2026-09-04 21:34:54+08:00 的本地准备已通过完整 Notebook 连贯执行、空范围不回退、非法坐标拒绝、正常计划未变、完整缺失未裁剪及生成代码校验。[本次已执行的完整聚宽代码](experiment_records/formal_roundtrip/prepared_2026-09-04T213454.688871+0800_item_09_v1/joinquant_cell_source.py.txt) 来自 Notebook 最终单元格，仅为非执行源码证据；不要为推进状态重复执行此旧代码。[准备分析与摘要](experiment_records/formal_roundtrip/prepared_2026-09-04T213454.688871+0800_item_09_v1/analysis.md) 与实际结果分开保存。

2026-09-04 21:49:20+08:00 的聚宽真实运行和本地验收见 [真实往返记录](experiment_records/formal_roundtrip/2026-09-04T214920.164421+0800_item_09_v1/analysis.md)。下载 ZIP 为 10,008 字节，摘要与远端报告完全一致；正式库现有 `ingest_batch=1`、`fetch_observation=6`、`futures_daily=1`、`futures_minute=240`。相同 SHA 重导返回 `already_imported`，完整数据库字节未变。

同范围历史回查中，IF 已无缺失；TF 的 1 个日线键和 270 个分钟键仍为 source_confirmed_missing，不能称为行情补齐，但不再普通重拉。两组验收坐标待办为 0 不代表全部白名单没有待办。原 Notebook 和用户输出已在本次半自动改造前归档，当前 Notebook 已清除过期输出并保留用户空单元格，最后结果格在末尾。

## 第 04 项：聚宽取数协议多轮实验

本项已经完成。第 01—05 轮均取得完整实测证据；第 05 轮 v1 的序列化失败也已独立留档，v2 完成真实行情请求、打包、固定名覆盖、人工下载及本地逐字节复核。冻结的环境行为和安全边界见本目录 `AGENTS.md`；第 05 项已经在此证据基础上另行冻结正式 Schema、生产成员结构和正式文件名，没有把探针结构直接提升为生产协议。

| 轮次 | 验证内容 | 当前状态 |
|---|---|---|
| 01 | 环境、API 签名、返回形状、字段、索引、时区、正常 IF 与早期 TF | v4 实际行情结果通过 |
| 02 | Session 起止边界、空响应、早期 TF 和就绪时点的定向复核 | v2 的 18 个 case 全部成功；通过 |
| 03 | 单次容量、资源/配额、异常和静默截顶排除 | v1 大小块逐键逐值一致；通过 |
| 04 | 研究文件写入、同名覆盖、压缩与下载摘要 | 聚宽端与下载原文件逐字节一致；通过 |
| 05 | 一个日线＋分钟线＋请求证据的小包端到端往返 | 聚宽端与下载原文件逐成员、逐键、逐摘要一致；通过 |

第 04 项的探针文件名和成员结构只作为已验证能力证据，不自动成为正式设计。基础行情样本坐标来自本地权威日历：

- 正常样本 `IF2409.CCFX / 2024-06-28`：日线 1 键；分钟上午、下午各 120 键。
- 边界样本 `TF1303.CCFX / 2012-06-11`：本地日历列为日线 1 键；分钟上午、下午各 135 键，但其时间早于通常认知中的国债期货上市时间，专门用于识别聚宽的空响应或证券元数据差异。

2026-09-02 返回的 v1 已按实际报告时间保存为正式项目实验记录：[`result.json`](experiment_records/joinquant_fetch_protocol/2026-09-02T234028.808489+0800_round_01_v1/result.json)、[`raw_output.txt`](experiment_records/joinquant_fetch_protocol/2026-09-02T234028.808489+0800_round_01_v1/raw_output.txt) 和 [`analysis.md`](experiment_records/joinquant_fetch_protocol/2026-09-02T234028.808489+0800_round_01_v1/analysis.md)。旧草稿区仍保留一份数值等价但重新排版过的 JSON，不能作为原始字节证据；上述时间戳目录是唯一 canonical 记录。

该轮已经确认聚宽研究环境为 Python 3.6.7、Pandas 0.23.4、NumPy 1.14.6、`jqdata` 1.33.38、北京时间，且没有 PyArrow；`TF1303.CCFX` 的聚宽证券元数据起始日也确为 2012-06-11。v1 将网站接口误写为 `jqdata.get_price` 等模块属性，因此行情失败只证明调用形式不对，不能解释为接口或数据不可用。END 标记后的四个额外 `1` 已保留在原始文本中，但不属于结构化协议信封。

2026-09-03 返回的 v2 也已经归档：[`result.json`](experiment_records/joinquant_fetch_protocol/2026-09-03T121632.973484+0800_round_01_v2/result.json)、[`raw_output.txt`](experiment_records/joinquant_fetch_protocol/2026-09-03T121632.973484+0800_round_01_v2/raw_output.txt)、[`probe_source.py.txt`](experiment_records/joinquant_fetch_protocol/2026-09-03T121632.973484+0800_round_01_v2/probe_source.py.txt) 和 [`analysis.md`](experiment_records/joinquant_fetch_protocol/2026-09-03T121632.973484+0800_round_01_v2/analysis.md)。v2 证明 `from jqdata import *` 成功，但 `get_price`、`get_extras` 和 `get_query_count` 在平台预注入全局、导入后全局及 `jqdata` 顶层模块三个位置都没有暴露；所有行情 case 都在真正请求前失败，因此尚不能解释为数据或权限问题。

2026-09-03 返回的 v3 已经归档：[`result.json`](experiment_records/joinquant_fetch_protocol/2026-09-03T123009.161758+0800_round_01_v3/result.json)、[`raw_output.txt`](experiment_records/joinquant_fetch_protocol/2026-09-03T123009.161758+0800_round_01_v3/raw_output.txt)、[`probe_source.py.txt`](experiment_records/joinquant_fetch_protocol/2026-09-03T123009.161758+0800_round_01_v3/probe_source.py.txt) 和 [`analysis.md`](experiment_records/joinquant_fetch_protocol/2026-09-03T123009.161758+0800_round_01_v3/analysis.md)。它确认平台研究 API 位于 `jqresearch.api`，该模块明确暴露 `get_price`、`get_extras`、`get_bars`、`get_ticks`、`history` 和 `attribute_history`。本轮也证明 Notebook 内核保留了前序变量，因此不能把其全局导入差异当成干净内核语义；后续改用显式模块导入。

2026-09-03 返回的 v4 已经归档：[`result.json`](experiment_records/joinquant_fetch_protocol/2026-09-03T123547.489279+0800_round_01_v4/result.json)、[`raw_output.txt`](experiment_records/joinquant_fetch_protocol/2026-09-03T123547.489279+0800_round_01_v4/raw_output.txt)、[`probe_source.py.txt`](experiment_records/joinquant_fetch_protocol/2026-09-03T123547.489279+0800_round_01_v4/probe_source.py.txt) 和 [`analysis.md`](experiment_records/joinquant_fetch_protocol/2026-09-03T123547.489279+0800_round_01_v4/analysis.md)。12 个 case 全部成功：单合约日线为日期索引宽表，双合约 `panel=False` 为含 `time,code` 的长表；结算价和持仓附加数据为日期索引、合约列宽表；IF 上午分钟线为 09:31—11:30 的 120 行；日期字符串不能表示整日分钟区间；`TF1303` 首日日线与分钟线都是成功零行。至此第 01 轮通过。

第 02 轮验证上午、下午、午间和整日分钟键，检查共享时间边界切块是否重复；同时请求 `TF1303` 全生命周期、`TF1312` 首个真实行情候选，以及探针运行日前后的 `IF2609` 数据可见性。

第 02 轮 v1 已经运行到最终 JSON 编码，但因 Notebook 全局同名 `all` 遮蔽 Python 内置函数，`all(generator_expression)` 把 generator 留在报告中而失败。失败现场与当次源码保存在 [`analysis.md`](experiment_records/joinquant_fetch_protocol/recorded_2026-09-03T124314.534144+0800_round_02_v1_failed_serialization/analysis.md)。该错误不发生在行情 API 调用栈中，但由于完整报告尚未返回，也不能把任何请求块认定为成功。

恢复单元格只重新计算两个 comparison 字段并打印仍在内存中的 `report`，不会重复调用聚宽 API。v2 已移除所有未限定的 `all(generator)`，并通过 Python 3.6 语法及受污染全局命名空间测试；用户重启内核后执行 v2，取得了完整结果。

第 02 轮 v2 已经归档：[`result.json`](experiment_records/joinquant_fetch_protocol/2026-09-03T125014.056973+0800_round_02_v2/result.json)、[`raw_output.txt`](experiment_records/joinquant_fetch_protocol/2026-09-03T125014.056973+0800_round_02_v2/raw_output.txt)、[`probe_source.py.txt`](experiment_records/joinquant_fetch_protocol/2026-09-03T125014.056973+0800_round_02_v2/probe_source.py.txt) 和 [`analysis.md`](experiment_records/joinquant_fetch_protocol/2026-09-03T125014.056973+0800_round_02_v2/analysis.md)。18 个 case 全部成功：IF 上午与下午各 120 行且并集等于整日 240 行；午间区间为空；共享 10:30 边界的两个请求会重复 10:30；`TF1303` 整个元数据生命周期为成功零行，而 `TF1312` 首日有 1 行日线和 270 行分钟线。探针在交易日 12:50 已能读到当日盘中日线及上午分钟线，但这不证明日线已最终完成，因此继续采用 T+1 00:01 的完成数据边界。至此第 02 轮通过。

第 03 轮 v1 不追逐平台最大极限，而是验证一个足以覆盖未来安全生产块的容量下限：把 `IF2409.CCFX / 2024-01-22—2024-09-20` 的一次分钟请求与九个逐月请求按有序索引和全部字段值精确比较，同时记录 DataFrame 内存、进程内存高水位、耗时、警告，以及无效合约、字段、频率和反向时间区间的异常。只有大块与小块完全一致，才在该测试范围内判定未观察到静默截顶；正式协议仍须逐块按理论键验收。

第 03 轮 v1 已经归档：[`result.json`](experiment_records/joinquant_fetch_protocol/2026-09-03T130126.449968+0800_round_03_v1/result.json)、[`raw_output.txt`](experiment_records/joinquant_fetch_protocol/2026-09-03T130126.449968+0800_round_03_v1/raw_output.txt)、[`probe_source.py.txt`](experiment_records/joinquant_fetch_protocol/2026-09-03T130126.449968+0800_round_03_v1/probe_source.py.txt) 和 [`analysis.md`](experiment_records/joinquant_fetch_protocol/2026-09-03T130126.449968+0800_round_03_v1/analysis.md)。全生命周期请求和九个逐月请求都得到 38,640 行，索引、全部值及 canonical SHA-256 完全相同，最大逐月块为 5,520 行，测试范围内未观察到静默截顶。无效合约、字段、频率分别产生 `ParamsError`、`AssertionError`、`ValueError`；反向区间却成功返回零行，因此正式生成器必须在请求前自行拒绝反向边界。平台没有暴露配额计数器，后续代码必须维护自己的请求块计数。至此第 03 轮通过。

第 04 轮 v1 使用不含真实行情的可压缩探针 payload：先把旧内容写入专用固定名称，再把最新内容写入临时文件、复核摘要并通过 `os.replace` 原子覆盖固定名称；随后复读 ZIP、核对成员 CRC、manifest、payload 摘要及压缩效果。Notebook 报告中的最终文件字节数与 SHA-256 还必须和用户手动下载后的本地原文件一致，才能通过本轮。

第 04 轮 v1 已经归档：[`result.json`](experiment_records/joinquant_fetch_protocol/2026-09-03T130742.344715+0800_round_04_v1/result.json)、[`raw_output.txt`](experiment_records/joinquant_fetch_protocol/2026-09-03T130742.344715+0800_round_04_v1/raw_output.txt)、[`probe_source.py.txt`](experiment_records/joinquant_fetch_protocol/2026-09-03T130742.344715+0800_round_04_v1/probe_source.py.txt)、[`downloaded_file_identity.txt`](experiment_records/joinquant_fetch_protocol/2026-09-03T130742.344715+0800_round_04_v1/downloaded_file_identity.txt) 和 [`analysis.md`](experiment_records/joinquant_fetch_protocol/2026-09-03T130742.344715+0800_round_04_v1/analysis.md)。聚宽端最终 ZIP 和 `D:\Downloads` 下载原文件均为 13,546 字节，SHA-256 均为 `6302a8035b761332ee0dbfb70041995b19a64ec992b4a744d938f60ed6782c65`；本地复读的成员、CRC、manifest、运行身份、payload 字节数及 payload SHA-256 也全部一致。至此第 04 轮通过。

第 05 轮 v1 的失败现场已经归档：[`raw_output.txt`](experiment_records/joinquant_fetch_protocol/recorded_2026-09-03T232807.810966+0800_round_05_v1_failed_serialization/raw_output.txt)、[`probe_source.py.txt`](experiment_records/joinquant_fetch_protocol/recorded_2026-09-03T232807.810966+0800_round_05_v1_failed_serialization/probe_source.py.txt) 和 [`analysis.md`](experiment_records/joinquant_fetch_protocol/recorded_2026-09-03T232807.810966+0800_round_05_v1_failed_serialization/analysis.md)。该次执行没有输出报告初始化时间，目录明确使用本地记录时间；聚宽全局 `sum` 遮蔽内置函数，使 `dict_values` 进入请求观察并在打包前触发 JSON 序列化错误，因此不形成成功请求块或新传输文件证据。

第 05 轮 v2 的完整证据已经归档：[`result.json`](experiment_records/joinquant_fetch_protocol/2026-09-03T233454.394064+0800_round_05_v2/result.json)、[`raw_output.txt`](experiment_records/joinquant_fetch_protocol/2026-09-03T233454.394064+0800_round_05_v2/raw_output.txt)、[`recovery_source.py.txt`](experiment_records/joinquant_fetch_protocol/2026-09-03T233454.394064+0800_round_05_v2/recovery_source.py.txt)、[`probe_source.py.txt`](experiment_records/joinquant_fetch_protocol/2026-09-03T233454.394064+0800_round_05_v2/probe_source.py.txt)、[`downloaded_file_identity.txt`](experiment_records/joinquant_fetch_protocol/2026-09-03T233454.394064+0800_round_05_v2/downloaded_file_identity.txt) 和 [`analysis.md`](experiment_records/joinquant_fetch_protocol/2026-09-03T233454.394064+0800_round_05_v2/analysis.md)。三个请求块均为 `complete`：日线 1 键，上午与下午分钟各 120 键；全部零缺失、零额外、零重复、零空值。聚宽端和本地下载 ZIP 均为 8,437 字节，SHA-256 均为 `e331a7da334a9f62ab5e2708a0675190dcdba84d51dd5c56b0af45081e7516c4`；本地复读的成员顺序、压缩信息、CRC、manifest、请求块、241 条行情记录及全部业务键都一致。至此第 05 轮及清单第 04 项通过。

由这次失败冻结的 Notebook 安全规则是：聚宽全局命名空间一律按可能污染处理，内置函数通过 `py_builtins` 显式调用且禁止星号导入；全部 ZIP 成员先在内存完成 JSON 往返与摘要校验，再接触临时文件；固定文件只经临时文件复读和 `os.replace` 原子覆盖；打包前失败必须保留旧固定文件且不得提示下载；恢复前必须判断行情值是否真正留在内存，没有保留时如实说明并重复有界请求。`In[n]` 恢复只适用于 traceback 已证明编号且内核未重启的现场，并要求每个源码替换片段恰好出现一次及提升探针版本。完整强制条款见本目录 [`AGENTS.md`](AGENTS.md) 的“聚宽 Notebook 代码安全边界”。

## 建设清单

- [x] 01．确定正式子目录及职责边界
- [x] 02．确定金融期货白名单与“应有数据”语义
- [x] 03．确定轻量数据库方案和状态模型
- [x] 04．聚宽取数协议的探索性、多轮实验与冻结
- [x] 05．冻结本地行情 Schema 与单文件传输协议
- [x] 06．实现精确缺失检测与拉取任务规划
- [x] 07．构建本地规划 Notebook 和最终代码生成单元格
- [x] 08．构建本地导入脚本
- [x] 09．完成真实小批次端到端验收
- [ ] 10．执行初始全量回补并冻结日常操作说明（半自动入口与操作说明已完成；真实全量回补待逐批完成）

用户已明确推进全白名单半自动流程，后续每轮下载后直接重新运行 Notebook，不需要 Agent 逐批确认；若真实数据或环境报错，再保留输出和文件进行核查。
