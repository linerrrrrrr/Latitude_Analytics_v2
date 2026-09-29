# 适用范围与规范地位

- 本文件适用于 `01_project_collection/JQ_strategy/financial_futures_data` 整棵目录树。
- 用户已在 2026-09-02 明确确认本目录作为正式项目子目录，用于金融期货的人工聚宽取数规划、固定名单文件接收、本地缺失检测和项目内轻量数据库导入。
- 本目录继承仓库根 `AGENTS.md` 的全部规则。凡涉及正式数据湖读取、Arrow Schema、字段、类型或转换，还必须遵循 `03_Futures_Database/AGENTS.md` 和 `config/data_contracts.py`。

# 规范索引与同步要求

- [仓库根 AGENTS.md](../../../AGENTS.md)：项目级命名、最小改动、运行环境、正式湖定位和规范同步要求。
- [仓库 Python 依赖清单](../../../requirements.txt)：本项目本地数据库所用 DuckDB 的可复现版本来源。
- [数据库 AGENTS.md](../../../03_Futures_Database/AGENTS.md)：正式数据湖读取、字段、类型、Arrow Schema 和项目专属数据边界。
- [稳定 silver 可执行契约](../../../config/data_contracts.py)：正式 17 张 silver 表的唯一 Schema 与 metadata 来源。
- [正式事实采集白名单](../../../config/futures_lakehouse/futures_fact_collection_policy.py)：正式商品期货事实生产白名单；不得作为本子项目金融期货白名单。
- [项目金融期货范围策略](financial_futures_collection_policy.py)：本子项目中金所品种白名单、支持频率、当前聚宽数据就绪时点与有界计划规模的唯一可执行来源。
- [项目库与传输可执行契约](financial_futures_local_contract.py)：第 05 项四表字段、DuckDB DDL、协议稳定标识和 Schema 复核的唯一可执行来源。
- [精确缺失与拉取规划器](financial_futures_fetch_planner.py)：第 06 项正式只读缺失计算、观察块合并、有界来源请求选择和 canonical 计划身份实现。
- [本地工作流 Notebook](plan_financial_futures_fetch.ipynb)：唯一半自动入口；每轮从头运行，第 3 节自动消化并删除固定 ZIP，随后规划，最后第 10 节输出完成说明或下一批聚宽代码及剩余轮数；远端模板归本 Notebook 所有。
- [固定文件导入入口](import_financial_futures_file.py)：第 08 项固定快照、协议与日历坐标核对、四表事务、幂等复核和失败现场实现。
- [聚宽期货参考资料](../references/joinquant/futures_backtest/README.md)：聚宽研究环境、期货接口和人工下载边界的外部资料快照。
- [聚宽取数协议实验记录](experiment_records/joinquant_fetch_protocol)：按实际执行时间留存的逐轮原始输出、结构化结果和分析；它是第 04 项环境行为证据，不是正式取数入口。
- [本地规划器验收记录](experiment_records/local_fetch_planner)：带时区执行时间的本地只读与临时合成库验证结果、非执行源码快照；不是生产配置或取数入口。
- [Notebook 与导出代码验收记录](experiment_records/local_notebook_export)：带时区时间的本地完整 Notebook 执行及模拟来源验证；不是聚宽端实际运行证据。
- [本地导入验收记录](experiment_records/local_file_import)：带时区时间的模拟文件导入、冲突/回滚/幂等及规划器衔接结果和非执行源码快照；不是正式行情导入证据。
- [正式协议往返记录](experiment_records/formal_roundtrip)：第 09 项本地准备与聚宽真实执行分开留存，后续半自动流程改造和验证也在此按时间留档；实现、准备或模拟结果不是来源运行或全量回补完成证据。
- [本目录 README](README.md)：目录布局、人工数据流和分项建设状态。
- 修改本文件或 README 中具有规范作用的内容时，必须同步检查以上材料及仓库根索引，消除冲突后再完成变更。

# 数据与系统边界

- 正式湖只作为只读上游。业务代码必须通过 `config.settings.settings.futures_lake_root` 定位 `silver`，按权威 Schema metadata 构造表路径和 Hive 分区，并直接使用 PyArrow Dataset 读取；不得在本目录保存另一份正式湖路径常量。
- 本目录不得写入、删除、迁移或修复正式湖的 `raw`、`silver`、`gold`、日历、事实或完成状态。发现正式上游异常时停止并报告，不得在项目库中伪装修复正式湖。
- `data/warehouse/` 中的数据库是 `JQ_strategy` 项目专属数据，不属于正式湖，也不自动加入 `config/data_contracts.py` 的稳定 silver 表清单。未来若要提升为全项目正式数据，必须另行取得用户授权并执行完整契约迁移。
- 本项目必须建立职责明确的局部金融期货白名单。不得修改或复用 `config/futures_lakehouse/futures_fact_collection_policy.py` 来暗中扩大正式商品期货事实生产范围，也不得把正式事实白名单解释为品种、合约或日历宇宙。
- 聚宽取数保持人工边界：本地 Notebook 每轮从头运行，先检查固定 inbox ZIP；存在时直接调用正式导入函数校验、提交、复核、删除，再规划全白名单待办；不存在时直接规划。用户只在聚宽研究环境手动执行代码并手动下载覆盖文件，之后再次从头运行本地 Notebook，不需要逐轮询问 Agent 或手动选入库格。Notebook 不通过 PowerShell、shell 或子进程入库，不自动登录聚宽、调用本地 JQData、保存凭据或自动下载文件。

# 金融期货范围与应有格点

- 项目局部白名单的唯一来源是 `financial_futures_collection_policy.py`，当前固定为 `CCFX` 的 `IC、IF、IH、IM、T、TF、TL、TS`。未来新增品种不得仅因上游日历出现而自动纳入，必须先显式修改该配置及本规范。
- 只处理正式 `dim_futures_contract_calendar` 已枚举的固定月份合约；不请求或入库 `8888` 品种指数、`9998`/`9999` 连续合约等合成代码。不得按外部上市日期另行裁掉上游权威日历中的早期合约，早期 TF 可得性留给第 04 项实测并以成功空响应如实记录。
- `dim_futures_bar_calendar` 的结构字段是本项目理论应有格点的直接来源；`dim_futures_variety_calendar` 与 `dim_futures_contract_calendar` 提供品种有效日、合约生命周期和 Session 结构。本项目信任这些正式上游证明，不重新调用 API 或自行向上游水位之后扩展。
- 项目范围不读取 `dim_futures_bar_calendar.is_fetch_required`、`is_fetch_completed`、`selection_reason`、实际计数或质量字段作为自身状态。这些字段属于正式商品事实采集政策，当前排除了 `CCFX`；本项目只复用结构、Session、`schedule_status` 与开休市证据。
- 只有 `schedule_status='confirmed_closed' AND evidence_level='authoritative'` 的格点免于请求。`scheduled`、`suspected_closed` 或只有 `reconciled` 旁证的格点仍属于结构应有范围。
- 日线应有键来自 `bar_frequency='1d'` 的结构行，业务键为 `contract_code,trading_date`；`session_number` 必须为 0，`expected_bar_count` 必须为 1。不得从两条合约 Session 行生成两个日线键。
- 分钟应有键来自 `bar_frequency='1m'` 的 Session 行，按北京时间 `(session_start_at, session_end_at]` 每分钟展开，业务键为 `contract_code,bar_at`，并保留 `trading_date,session_number` 归属；每个 Session 展开数量必须等于 `expected_bar_count`。
- 理论应有格点随正式上游日历水位增长。进入正常拉取计划还必须满足北京时间已经到达该交易日次日 00:01；尚未到达的尾部单列为 `not_yet_fetchable`，不得误报为数据缺失或生成取数代码。第 04 项若以实测修正聚宽就绪时点，必须同步本配置、本规范和 README。
- 白名单缩减或上游结构修订不得自动删除项目库既有事实或成功观察证据；移出当前范围的数据只是不再参与正常缺失与拉取计划。重新纳入时，仍可信的既有事实和成功观察证据继续生效。

# 缺失与拉取待办术语

- `expected_due`：当前白名单、结构开市且已经到达聚宽就绪时点的精确理论键。
- `data_missing`：`expected_due − 已提交有效行情键`。它是覆盖质量事实，包含已经成功请求但聚宽未返回有效行情的键；第 05 项即使选择保存空响应占位记录，占位也不得计作有效行情覆盖。
- `source_confirmed_missing`：完整请求、完整传输和本地成功提交共同证明已经观察过，但仍无有效行情的 `data_missing` 子集。它必须持续可见，不得伪造行情值。
- `fetch_pending`：`data_missing − source_confirmed_missing`。只有该集合进入正常生成的聚宽取数代码；从未尝试、上次失败、配额停止或无法证明完整的截顶/中断范围仍属于本集合。
- 成功的日线空响应、分钟部分响应和分钟零行响应可以形成 `source_confirmed_missing`，并记录实际覆盖与 warning；它们不进入日常无限重拉。未来如需复查，只能由另行确认的显式 repair/recheck 模式处理。
- `data_missing` 与 `fetch_pending` 的底层判断必须按上述精确业务键求差，不能用总行数或最小/最大日期代替。面向人的“缺失名单＋时间范围”只是精确键的聚合展示和请求计划，不得跨合约、频率、交易日 Session 或真实内部缺口错误合并。
- 远端异常、未执行完全部计划、已知/疑似截顶、manifest 或文件摘要不完整，以及本地校验/事务失败，都不得形成成功观察或完成证据。

# 轻量数据库与状态模型

- 项目数据库固定使用 DuckDB，正式路径固定为 `data/warehouse/financial_futures.duckdb`。它是无需独立服务的项目单文件数据库；第一版不得再并列维护 SQLite、外部 Parquet 事实目录或第二个数据库文件。
- 数据库文件尚不在第 03—05 项创建。第 05 项已经冻结列级 Schema；只有第 08 项的正式导入入口才可按该 Schema 初始化数据库，不得单独生成一个空库作为伪完成证据。
- 理论应有格点和 `expected_due`、`data_missing`、`source_confirmed_missing`、`fetch_pending` 都是随当前正式日历、白名单、就绪时点、成功观察和有效事实变化的派生集合，不落成持久状态表。尤其不得把约 1,725 万个当前理论分钟键复制进项目库。
- 第一版数据库只保留四类持久表角色，列级字段与类型由下节第 05 项契约冻结：
  - `futures_daily`：已提交的有效日线行情，业务主键为 `contract_code,trading_date`；不保存空响应占位。
  - `futures_minute`：已提交的有效分钟行情，业务主键为 `contract_code,bar_at`；不保存空响应占位。
  - `fetch_observation`：已经完整请求、完整传输并成功本地提交的不可变来源观察。日线最小观察单元是一个合约交易日，分钟最小观察单元是一个合约 Session；每条观察必须保留足以精确重建当次请求覆盖的边界、预期数量和请求块身份。
  - `ingest_batch`：一个固定名单传输文件内容经过本地完整校验并由单个数据库事务成功提交的批次账本，保存文件内容摘要、聚宽导出运行身份和 manifest 身份。
- 精确缺失不另建持久明细表。当前成功观察覆盖记为 `observed_expected_due = expected_due ∩ 已提交 fetch_observation 的精确覆盖`；随后按精确业务键计算 `source_confirmed_missing = observed_expected_due − 已提交有效行情键`。新增有效事实会自然使相应键退出缺失集合，不得删除或改写原观察历史。
- `fetch_observation` 的覆盖状态只有 `complete`、`partial_missing`、`empty` 三种，三者都表示请求块本身完整成功且没有截顶：实际有效键数分别等于预期数、介于零与预期数之间、等于零。`partial_missing` 和 `empty` 可以产生来源确认缺失，并默认记为 `warning`；行情覆盖状态与 `passed/warning` 质量状态必须分轴表达。
- 请求失败、未执行、异常中断、配额停止、疑似截顶或无法证明请求边界完整的块不得写 `fetch_observation`。一个结构和摘要均完整的传输文件可以声明部分计划失败，并只提交其中可独立证明完整成功的请求块；失败块不导入事实、不形成观察，继续属于 `fetch_pending`。manifest 本身不完整或不能无歧义区分请求块时整文件拒绝。
- 固定传输文件名只负责人工覆盖，绝不能充当批次身份。`ingest_batch` 必须以整文件 SHA-256 幂等去重，并校验聚宽导出 `run_id`：同一文件摘要再次导入是经核验的无写入操作；同一 `run_id` 对应不同文件摘要必须硬失败。
- 单个传输文件内部出现重复行情业务键必须硬失败，不允许 `last-write-wins`，也不因值相同而静默折叠。跨批次遇到已有业务键时，行情业务值逐值一致可保留原事实并视作事实层无操作；任一业务值冲突必须硬失败，后续只能由另行确认的显式 repair 流程处理。
- 行情事实、成功观察和 `ingest_batch` 必须在同一个 DuckDB 事务中协调提交，任一步失败全部回滚；失败事务不得留下批次行或完成证据。导入前还必须把可能正被人工覆盖的同名文件取得不可变快照并核对摘要，具体协议与实现分别在第 05、08 项完成。
- 本地并发固定为单写者：只有正式导入函数负责写数据库；Notebook 的规划格只读，明确标为入库的代码格可以直接调用该函数提交。人工流程不得在导入事务运行时同时执行缺失检测，也不得启动两个导入进程；各入口应短连接、使用后关闭。显式 recheck/repair 如何追加新观察不属于日常状态，留待未来单独授权。

# 第 05 项冻结的本地数据库 Schema

## 共用类型与版本

- 项目库物理 Schema 版本固定为 `1.0.0`。它是项目专属 DuckDB 契约，不加入正式 silver 的 `config/data_contracts.py`；但同名行情字段必须沿用全项目字段语义和跨引擎类型：日期为 `DATE`，有时刻的值为 `TIMESTAMPTZ`，标识符为 `VARCHAR`，行情价格、成交量、持仓量和成交金额为 `DOUBLE`。
- 第 06 项将来源调用身份和计数补齐到尚未生成任何正式文件或数据库实例的 v1 设计中，明确分开来源调用和观察块。后续不得用相同版本静默接纳缺少这些字段的旧结构；Schema 不符必须拒绝。
- 每个 DuckDB 连接必须把 `TimeZone` 设为 `Asia/Shanghai`。`bar_at`、远端运行时间和请求时间以带偏移 RFC 3339 文本进入 `TIMESTAMPTZ`；数据库按绝对时刻比较，`trading_date` 始终是北京时间的交易日，不能从连接的默认时区反推。
- 所有 SHA-256 字段都是 64 位小写十六进制 `VARCHAR`；`run_id` 是标准 UUID 文本；`observation_id = sha256((run_id + "\n" + request_id).encode("utf-8"))`。这些格式必须由导入器验证，不依赖 DuckDB 隐式转换。
- 日线正式来源字段固定为 `open,high,low,close,volume,money,pre_close,open_interest`，入库时只把来源名 `pre_close` 规范为 `previous_close`；分钟正式来源字段固定为 `open,high,low,close,volume,money,open_interest`。第一版不请求、不传输、不入库 `get_extras` 结算价，也不保存上一结算价和各种涨跌派生列；如需增加必须升级协议与数据库 Schema，不能静默加列。
- `open/high/low/close/volume/money/open_interest` 非空且必须为有限数，三种数量值非负；`previous_close` 只允许来源本身缺失时为空，非空时必须有限。有限 OHLC 的高低关系异常按来源原值保留并将请求块记为 `warning`，不得修写；NaN、正负无穷、布尔伪装数值、负数量、类型错误或主键缺失使请求块无资格形成观察。

## 四张持久表

`ingest_batch` 是文件级不可变账本，字段顺序和约束固定如下：

| 字段 | DuckDB 类型 | 可空 | 语义 |
|---|---|---|---|
| `file_sha256` | `VARCHAR` | 否 | 主键；导入时不可变快照的整 ZIP 摘要 |
| `run_id` | `VARCHAR` | 否 | 唯一；聚宽本次导出运行 UUID |
| `plan_sha256` | `VARCHAR` | 否 | 本地生成的完整有序请求计划 canonical SHA-256 |
| `protocol_version` | `VARCHAR` | 否 | 第一版固定 `1.0.0` |
| `generator_version` | `VARCHAR` | 否 | 第一版固定 `financial_futures_joinquant_export_v1` |
| `fixed_file_name` | `VARCHAR` | 否 | 固定 `JQ_FINANCIAL_FUTURES_TRANSFER.zip` |
| `file_bytes` | `BIGINT` | 否 | ZIP 正字节数，必须大于 0 |
| `manifest_sha256` | `VARCHAR` | 否 | `manifest.json` 原字节摘要 |
| `publication_status` | `VARCHAR` | 否 | `complete` 或 `partial` |
| `run_started_at` | `TIMESTAMPTZ` | 否 | 聚宽运行开始时刻 |
| `run_completed_at` | `TIMESTAMPTZ` | 否 | 聚宽完成内存成员构建的时刻，不早于开始时刻 |
| `source_request_count` | `INTEGER` | 否 | 计划来源请求数，正数；每个至多实际调用一次 `get_price` |
| `successful_source_request_count` | `INTEGER` | 否 | 整组成功并可拆成观察的来源请求数，非负 |
| `failed_source_request_count` | `INTEGER` | 否 | 整组失败的来源请求数，非负 |
| `planned_request_count` | `INTEGER` | 否 | 计划观察块数，正数 |
| `successful_request_count` | `INTEGER` | 否 | 位于成功来源请求中且 observation-eligible 的观察块数，非负 |
| `failed_request_count` | `INTEGER` | 否 | 位于失败来源请求中的观察块数，非负 |
| `daily_record_count` | `BIGINT` | 否 | 日线成员记录数，非负 |
| `minute_record_count` | `BIGINT` | 否 | 分钟成员记录数，非负 |
| `daily_inserted_count` | `BIGINT` | 否 | 本事务新插入日线数，非负 |
| `daily_existing_equal_count` | `BIGINT` | 否 | 已存在且业务值逐值一致的日线数，非负 |
| `minute_inserted_count` | `BIGINT` | 否 | 本事务新插入分钟数，非负 |
| `minute_existing_equal_count` | `BIGINT` | 否 | 已存在且业务值逐值一致的分钟数，非负 |
| `manifest_json` | `VARCHAR` | 否 | 已按 canonical 规则复读一致的完整 manifest 文本 |
| `imported_at` | `TIMESTAMPTZ` | 否 | 本地事务提交时间 |

`file_sha256` 是主键，`run_id` 是唯一键。必须分别满足来源请求和观察块的成功数加失败数等于总数，以及两类 `inserted + existing_equal = record_count`；`complete` 要求两个失败数都为零，`partial` 要求至少一个来源请求以及它携带的观察块失败。相同 `file_sha256` 重导只能在完整复核现有账本后无写入返回；相同 `run_id` 配不同文件摘要硬失败。

`fetch_observation` 只保存可独立证明完整请求且成功提交的块，不保存失败块：

| 字段 | DuckDB 类型 | 可空 | 语义 |
|---|---|---|---|
| `observation_id` | `VARCHAR` | 否 | 主键；按本节公式生成 |
| `file_sha256` | `VARCHAR` | 否 | 外键到 `ingest_batch.file_sha256` |
| `source_request_id` | `VARCHAR` | 否 | 实际 `get_price` 调用的稳定身份 |
| `source_request_ordinal` | `INTEGER` | 否 | 来源调用在完整计划中的一基序号，必须大于 0 |
| `request_id` | `VARCHAR` | 否 | 本地计划中该观察块的稳定身份 |
| `request_ordinal` | `INTEGER` | 否 | 观察块在完整计划中的一基序号，必须大于 0 |
| `contract_code` | `VARCHAR` | 否 | 固定月份聚宽合约代码 |
| `bar_frequency` | `VARCHAR` | 否 | `1d` 或 `1m` |
| `trading_date` | `DATE` | 否 | 行情归属交易日 |
| `session_number` | `TINYINT` | 否 | 日线为 0，分钟为正数 |
| `session_start_at` | `TIMESTAMPTZ` | 是 | 日线为空；分钟为理论 Session 左开边界 |
| `session_end_at` | `TIMESTAMPTZ` | 是 | 日线为空；分钟为理论 Session 右闭边界 |
| `expected_key_count` | `INTEGER` | 否 | 日线为 1；分钟等于边界间分钟数 |
| `expected_keys_sha256` | `VARCHAR` | 否 | 理论有序业务键 canonical SHA-256 |
| `actual_key_count` | `INTEGER` | 否 | 来源实际有效键数，范围 `0..expected` |
| `actual_keys_sha256` | `VARCHAR` | 否 | 实际有序业务键 canonical SHA-256，零行也保存空列表摘要 |
| `missing_key_count` | `INTEGER` | 否 | 必须等于 `expected - actual` |
| `coverage_status` | `VARCHAR` | 否 | `complete`、`partial_missing` 或 `empty` |
| `quality_status` | `VARCHAR` | 否 | `passed` 或 `warning`，与覆盖状态分轴 |
| `quality_reason` | `VARCHAR` | 否 | 可读的完整性和来源质量原因 |
| `request_started_at` | `TIMESTAMPTZ` | 否 | 远端所属来源请求开始时刻 |
| `request_completed_at` | `TIMESTAMPTZ` | 否 | 远端所属来源请求结束时刻，不早于开始时刻 |
| `elapsed_seconds` | `DOUBLE` | 否 | 非负有限耗时 |

同一文件内 `request_id` 和 `request_ordinal` 分别唯一；同一 `source_request_id` 必须始终映射同一 `source_request_ordinal`、合约和频率，一个来源请求可携带多个连续观察块。日线必须同时满足 `session_number=0`、两边界为空、`expected_key_count=1`；分钟必须满足正 Session 编号、两边界非空且结束晚于开始、预期数等于 `(session_start_at,session_end_at]` 的分钟数。三种覆盖状态必须分别满足 `actual=expected`、`0<actual<expected`、`actual=0`；后两者强制 `warning`。

`futures_daily` 只保存有效来源行：

| 字段 | DuckDB 类型 | 可空 | 语义 |
|---|---|---|---|
| `contract_code` | `VARCHAR` | 否 | 业务主键 1 |
| `trading_date` | `DATE` | 否 | 业务主键 2 |
| `previous_close` | `DOUBLE` | 是 | 聚宽 `pre_close` 原值 |
| `open` | `DOUBLE` | 否 | 开盘价 |
| `high` | `DOUBLE` | 否 | 最高价 |
| `low` | `DOUBLE` | 否 | 最低价 |
| `close` | `DOUBLE` | 否 | 收盘价 |
| `volume` | `DOUBLE` | 否 | 成交量，手 |
| `money` | `DOUBLE` | 否 | 成交金额，聚宽原值为元 |
| `open_interest` | `DOUBLE` | 否 | 持仓量，手 |
| `first_observation_id` | `VARCHAR` | 否 | 首次提交该业务键的观察外键 |

主键固定为 `contract_code,trading_date`，`first_observation_id` 外键到 `fetch_observation.observation_id`。业务值集合固定为 `previous_close,open,high,low,close,volume,money,open_interest`。

`futures_minute` 只保存有效来源行：

| 字段 | DuckDB 类型 | 可空 | 语义 |
|---|---|---|---|
| `contract_code` | `VARCHAR` | 否 | 业务主键 1 |
| `bar_at` | `TIMESTAMPTZ` | 否 | 业务主键 2；北京时间一分钟 bar 结束时刻 |
| `trading_date` | `DATE` | 否 | 正式日历给出的交易日归属 |
| `session_number` | `TINYINT` | 否 | 正 Session 编号 |
| `open` | `DOUBLE` | 否 | 开盘价 |
| `high` | `DOUBLE` | 否 | 最高价 |
| `low` | `DOUBLE` | 否 | 最低价 |
| `close` | `DOUBLE` | 否 | 收盘价 |
| `volume` | `DOUBLE` | 否 | 成交量，手 |
| `money` | `DOUBLE` | 否 | 成交金额，元 |
| `open_interest` | `DOUBLE` | 否 | 持仓量，手 |
| `first_observation_id` | `VARCHAR` | 否 | 首次提交该业务键的观察外键 |

主键固定为 `contract_code,bar_at`，`first_observation_id` 外键到 `fetch_observation.observation_id`。业务值集合固定为 `trading_date,session_number,open,high,low,close,volume,money,open_interest`；跨批次已有键比较使用 DuckDB null-safe 逐值语义且不比较 `first_observation_id`。日线采用同样规则。任何冲突均使整批回滚，不能更新原事实的观察身份。

# 第 05 项冻结的单文件传输协议

## 文件、编码与版本

- 聚宽和本地 inbox 的正式固定文件名均为 `JQ_FINANCIAL_FUTURES_TRANSFER.zip`，本地固定路径为 `data/inbox/JQ_FINANCIAL_FUTURES_TRANSFER.zip`。探针名 `JQ_FINANCIAL_FUTURES_TRANSFER_PROBE.zip` 永远不属于正式入口。
- 协议标识为 `jq_financial_futures_transfer`，`protocol_version` 固定为 `1.0.0`。ZIP 必须使用 DEFLATE，成员顺序且成员集合精确固定为 `manifest.json`、`request_blocks.jsonl`、`futures_daily.jsonl`、`futures_minute.jsonl`；拒绝额外、重复、加密、目录或非 DEFLATE 成员。
- 所有成员使用 UTF-8、无 BOM。JSON 使用 `ensure_ascii=False,sort_keys=True,separators=(",", ":"),allow_nan=False` 的 compact canonical 字节；`manifest.json` 不带末尾换行。JSONL 每行恰好一个对象并以 LF 结尾，空成员必须是零字节。禁止 NaN/Infinity、注释、空白行和非对象顶层值。
- `manifest.json` 不能包含整 ZIP SHA-256，因为这会形成自引用。远端在原子发布后另行输出固定文件的字节数和 SHA-256；本地以取得的不可变快照重新计算并写入 `ingest_batch.file_sha256`。

## Manifest 和三个 JSONL 成员

`manifest.json` 的顶层键固定为：

```text
format_id, protocol_version, fixed_file_name, generator_version,
planner_version, plan_sha256, run_id, run_started_at, run_completed_at,
market_time_zone, source_api_module, environment, publication_status,
source_request_count, successful_source_request_count, failed_source_request_count,
planned_request_count, successful_request_count, failed_request_count,
daily_record_count, minute_record_count, failed_source_requests, failed_requests, members
```

其中前三个标识分别固定为 `jq_financial_futures_transfer`、`1.0.0`、`JQ_FINANCIAL_FUTURES_TRANSFER.zip`，`generator_version` 第一版固定为 `financial_futures_joinquant_export_v1`，`planner_version` 第一版固定为 `financial_futures_fetch_planner_v1`，`market_time_zone` 固定为 `Asia/Shanghai`，`source_api_module` 固定为 `jqresearch.api`。`environment` 恰含 `python_version,pandas_version,numpy_version,jqdata_version`；`failed_source_requests` 按来源请求序号排列并恰含 `source_request_id,source_request_ordinal,error_type,error_message`，`failed_requests` 按观察块序号排列并恰含 `source_request_id,request_id,error_type,error_message`。`members` 恰好描述三个 JSONL 成员，每项恰含 `bytes,sha256,record_count`。来源请求计数、观察块计数及成员计数必须分别对账；`complete` 表示两个失败数均为零，`partial` 表示至少一个来源请求及其观察块失败但文件结构和其余成功来源请求仍完整。

`request_blocks.jsonl` 必须按 `request_ordinal` 升序包含计划中的每一块，不论成功或失败。每行字段固定为：

```text
observation_id, source_request_id, source_request_ordinal,
request_id, request_ordinal, contract_code, bar_frequency,
trading_date, session_number, session_start_at, session_end_at,
source_frequency, source_fields, source_parameters,
expected, actual, request_status, response_validation_status,
coverage_status, observation_eligible, quality_status, quality_reason,
request_started_at, request_completed_at, elapsed_seconds,
error_type, error_message
```

`source_frequency` 日线为 `daily`、分钟为 `1m`；`source_fields` 必须与本节冻结字段及顺序相同；`source_parameters` 恰含 `skip_paused=true,fq=null,panel=false,fill_paused=false,round=false`，可执行定义为 `financial_futures_local_contract.py` 的 `TRANSFER_SOURCE_PARAMETERS`。`expected` 恰含 `key_count,first_key,last_key,keys_sha256`；可解释的响应中，`actual` 恰含上述四项以及 `duplicate_key_count,missing_key_count,extra_key_count,null_count_by_field`。未执行、API 未返回对象或响应结构/索引无法无歧义解释时，失败块的 `actual` 可为空；成功观察绝不允许为空，不得用伪造的零行统计替代无法解释的响应。日期键编码为 `YYYY-MM-DD`，分钟键编码为带 `+08:00` 的秒精度 RFC 3339 文本。同一来源请求内所有行共享来源请求身份、实际 API 调用起止时间和耗时；返回表按各观察块理论键拆分并分别生成 `actual` 证据。`request_status` 固定为 `success/error/not_executed`，`response_validation_status` 固定为 `passed/failed/not_run`，调用成功与观察是否有效仍分别判断。

只有整个来源请求调用成功、返回结构可验证，而且其每个观察块都满足 `request_status=success`、`response_validation_status=passed`、零重复、零额外、实际键严格升序且未观察到截顶时，该来源请求才成功；其中观察块的 `observation_eligible` 才可为真并使用三种正式覆盖状态。来源请求任何一处失败都使它携带的所有观察块失败：这些行必须 `observation_eligible=false,coverage_status=null,quality_status=failed`，不产生行情成员记录或本地观察，并同时列入 manifest 的来源请求与观察块失败摘要。生成器不做业务自动重试。

`futures_daily.jsonl` 每行字段固定为：

```text
observation_id, contract_code, trading_date,
previous_close, open, high, low, close, volume, money, open_interest
```

`futures_minute.jsonl` 每行字段固定为：

```text
observation_id, contract_code, bar_at, trading_date, session_number,
open, high, low, close, volume, money, open_interest
```

两成员分别按 `contract_code,trading_date` 和 `contract_code,bar_at` 升序。每行必须引用同文件中 observation-eligible 且频率匹配的请求块；每个成功块的成员业务键重新计算后，必须与其 `actual` 的数量、首尾键、摘要和缺失数完全一致。文件内全局重复业务键硬失败；失败或无资格请求块不得残留行情行。

## 发布、快照与本地接纳

- 聚宽端继续执行“全部成员先在内存 canonical 编码并复读 → 构建 ZIP 内存字节并完整复读 → 同目录临时文件写入、flush/fsync、复读 → `os.replace` 固定名 → 固定名复读”的顺序。只有最后一次复读全部通过才打印下载指令；运行中存在失败请求块时可以发布 `partial` 文件，但打包或协议自身失败时必须保留旧固定文件且不提示下载。
- 本地导入不得直接解析用户可能正在覆盖的 inbox 文件。第 08 项入口必须先以唯一临时名把一次打开得到的字节流复制到 `data/staging/`，flush/fsync 后以该副本的 SHA-256 原子命名为 `<file_sha256>.zip`；后续 ZIP、manifest、JSONL 和数据库验证只读取该不可变快照。已存在同名摘要快照时必须逐字节一致，否则失败。
- 接纳顺序固定为：ZIP 容器与成员门禁 → canonical JSON/JSONL 往返 → manifest 摘要和计数 → 请求计划身份与请求块状态 → 理论键重建 → 行情成员类型、键、值和逐块摘要 → 库内既有批次/`run_id`/业务键冲突 → 单事务写入四表 → 只读重开复核 → 删除已消化文件。提交前失败不得留下完成观察或批次账本；提交后复读或清理失败必须报告已提交/待核查，不得谎称回滚或成功结束。
- 相同文件摘要经完整复核后是无写入幂等返回。write=True 时，新文件成功或幂等完成且数据库最终复核通过后，删除本次已消化 ZIP，并清理本次 staging 快照；write=False 不删除 inbox。验证、事务或清理失败停止并保留尚存现场，不得把快照当作已提交来源。各步骤均无业务自动重试。

# 第 08 项冻结的固定文件导入

- 唯一导入实现为 `import_financial_futures_file.py` 中的 `import_financial_futures_file()`，使用 latitude。正式用户入口是 Notebook 第 3 节自动入库检查，发现固定文件即直接调用 write=True，不要求命令行或手工解压。没有文件是正常状态，继续规划。函数默认 write=False 只校验且保留 inbox，不要求提交前额外运行一次；原有 CLI 仅保留兼容，不接受任意输入文件或数据库路径。
- 快照只从同一个 inbox 打开句柄复制，写入临时文件并 flush/fsync；同句柄复读摘要与前后文件状态检测原地覆盖。复读临时副本后，以同卷原子、不覆盖的硬链接方式发布摘要名称并移除自己的临时名；已存在的摘要快照必须逐字节相同。后续 inbox 被人工替换不改变本次导入对象。禁止解析可变 inbox、解压执行文件内容或把同名旧文件当作新批次。
- 本地资源门禁为 ZIP 最大 512 MiB、声明展开总量最大 1 GiB、manifest 最大 8 MiB、JSONL 单行最大 1 MiB；另复核策略文件定义的来源请求、观察块和理论键上限。门禁失败保留现场，不自动拆包或重试，也不是平台容量承诺。JSONL 按行读取并验证 canonical 字节、CRC、摘要和计数；不把 ZIP 路径解压到文件系统。
- 导入器从全部请求块重建观察、来源请求与完整计划三级身份，包含失败和未执行块。新文件还只读正式行情日历中本批合约/日期范围，验证请求坐标、Session 边界、理论数量、权威休市状态及来源组的结构连续性；不重跑缺失规划、不重新证明上游业务主键，也不要求计划仍等于现在的下一批。若日历已修订而不再匹配，保留文件并停止，不能擅改传输证据。已提交同摘要文件完整复核原账本、观察、事实和首次观察引用，不因上游后续修订而修改或删除历史证据。
- `staging/import.lock` 采用独占创建实现人工单写者门禁，竞争进程不得删除别人的锁。正常结束或可捕获异常只释放自己的锁；进程崩溃残留锁必须先人工确认无导入进程，再另行处置，不自动删除、自动重启或重试。该锁不替代“不要同时运行规划 Notebook”的人工边界。
- 新库首先在 staging 唯一候选路径建四表并完成单事务，关闭连接、只读重开复核且确认无 WAL 后，再以原子、不覆盖的硬链接安装到固定 warehouse 路径并只读复核。已有库在同一事务中重新检查冲突，依次提交批次、观察、日线和分钟；任何提交前异常都整体回滚。相同业务值保留原 `first_observation_id`，同文件幂等必须包括完整账本与逐值复核，不能只查到 SHA 就跳过。
- 报告与错误现场必须区分提交前失败和提交后复读/安装/删除失败：记录 `commit_attempted`、`commit_returned`，回滚失败保留原异常及附注。提交已返回时不能声称回滚或删除账本。只有数据库最终复核和要求的清理完成才成功返回。失败保留尚存摘要快照、候选库、已移走文件和带时区时间的 `error_*.json`，历史错误不自动清理。`counts` 是原批次账本计数；本次新增由 `rows_inserted_this_invocation` 表示，幂等为零；`database_write_performed`、`database_state_verified`、`inbox_deleted` 分别表示实际写库、最终复核和已消化文件删除状态。
- 删除前先将当前 inbox 目录项原子移到 staging 的唯一 `.consumed_inbox_*.zip`，复读 SHA-256，仅与本次已提交快照一致才删除；不得先 hash(inbox) 后 unlink(inbox)。不同内容用不覆盖硬链接恢复到空闲 inbox 后报错；入口又被新文件占用时两份均保留。占用、删除或恢复失败均停止，不能误删新下载文件。Notebook 入库后再次检查入口，仍有文件则停止，不生成下一批。
- SHA-256 和计划身份验证提供完整性与去重，不是来源数字签名；人工 inbox 只应接收用户本次聚宽生成并核对下载报告的文件。第 08 项只以隔离临时文件与模拟行情验收，不创建正式数据库，不自动执行第 09 项真实往返。

# 第 06 项冻结的精确缺失与有界规划

- 正式实现固定为 `financial_futures_fetch_planner.py` 的 `build_financial_futures_fetch_plan()`。它是只读领域操作：从正式 `dim_futures_bar_calendar` 和已有项目库计算精确缺失，并读取 `dim_trade_calendar` 区分节假日与陈旧上游水位；项目库不存在时按空状态计算但不得创建数据库、目录或占位文件。
- 第 09 项增加显式 `acceptance_contract_dates` 参数，只限制本次请求选择，不裁剪覆盖汇总、缺失明细、理论宇宙或来源确认缺失。默认 `None` 保持正常算法；坐标元组仅允许 `(合约字符串, datetime.date)`，重复、未知、未就绪或不在当前开市结构的坐标报错，空元组生成空计划。范围已无待办也返回空计划，绝不退回全范围或重新拉取已观察缺失。候选合并仍由本规划器负责，范围外块中断来源组连续性；不得在 Notebook 另写计划身份或缺失算法。
- `as_of` 必须带时区并转换为北京时间；当天达到 00:01 后最新 due 日为前一自然日，否则为前二自然日。规划器按当前白名单和权威结构现场派生 `expected_due` 与 `not_yet_fetchable`，不得把基线行数、结束日或规划结果持久化为新的权威状态。
- 日线按 `contract_code,trading_date` 精确比较。分钟在整段事实与观察覆盖均为零时保留 Session 级计数和范围，不为此物化全宇宙约 1,725 万个键；只有存在事实或观察覆盖的 Session 才展开理论分钟键求精确内部差集。输出的 `minute_missing_ranges_df` 只把同一合约、交易日、Session、缺失状态内真正连续的分钟合并。
- 只有 `fetch_pending` 键可以触发正常待拉取。最小观察块仍为一个合约日或一个合约 Session；若当前 Session 因上游边界修订而只新增部分待拉取键，实际请求仍覆盖该完整 Session，已有值按跨批次一致性规则核对，不另拆持久观察粒度。没有任何待拉取键的块不因来源确认缺失而重拉。一个来源请求只合并同一合约、同一频率、在权威结构中连续且都待拉取的观察块，不跨越无待拉取键、尚未就绪或权威休市的块。
- 来源请求按日线优先、各频率内最新交易日优先、再按合约和结构序号确定稳定顺序。单来源请求最多 38,000 个理论键；正常规划单传输文件最多选择 250,000 个理论键，并继续受 50,000 个观察块和 64 个实际来源请求约束。协议兼容硬上限保留为 500,000 个理论键，以便既有成功批次仍可复核。2026-09-05 的真实回补中，264,000 个分钟键成功发布，而 499,800 个分钟键在全部来源请求成功后使聚宽 1 GiB 内核重启，因此正常规划不得再贴近协议硬上限。候选超过文件剩余容量时跳过并继续选择能完整容纳的后续候选，不允许切开单个观察块。
- 每个观察块的 `request_id` 由其合约、频率、交易日、Session 边界、理论键数及理论键摘要组成的 canonical 对象生成；每个来源请求的 `source_request_id` 由其合约、频率、来源起止边界、字段、参数及有序观察块身份生成。完整 `plan_payload` 含协议、生成器、规划器、时区和有序来源请求，`plan_sha256` 是其 canonical UTF-8 JSON 的 SHA-256；相同输入状态与 `as_of` 必须生成相同计划身份。
- 规划结果必须同时返回覆盖汇总、尚未就绪块、全部缺失块、分钟精确缺失范围、当前有界观察块、当前实际来源请求、完整计划 payload 和摘要。第 07 项 Notebook 只能展示和消费这些结果，不得另写第二套缺失算法或批量边界。

# 第 07 项冻结的 Notebook 与聚宽代码生成

- `plan_financial_futures_fetch.ipynb` 是唯一正式本地规划 Notebook，使用 `latitude` 内核，按标记文件定位仓库根并直接导入第 06 项规划器。开篇通过 `02_Futures_Lakehouse/a00_03_notebook_schema_browser.py` 的 `display_schema_metadata()` 展示 `TRADE_CALENDAR_SCHEMA` 与 `FUTURES_BAR_CALENDAR_SCHEMA`，项目 DuckDB 四表则只读展示 `financial_futures_local_contract.py` 的列定义；不得伪造新的 Arrow silver 契约或为了展示创建数据库。
- Notebook 采用单次 Run All 的线性流程：开篇环境与第 1—2 节契约，第 3 节自动消化 ZIP，第 4—8 节全白名单缺失、覆盖与分批预估，第 9 节定义远端模板，最后第 10 节输出结论或完整聚宽代码。默认 as_of=None、acceptance_contract_dates=None；首次没有 ZIP、历史回补和日常增量共用此入口。合约首尾日期只是包围范围，预览过滤不改变规划范围。
- 聚宽导出模板直接保存在 Notebook 文本单元格，不为交付再生成独立 `.py`。最后第 10 节重新核对内存计划 SHA-256，内嵌压缩计划与协议常量，只在本地编译、不执行远端代码；非空计划提供可全选文本框及纯文本 MIME，空计划不调用 API 或覆盖文件。
- 第 3 节只调用正式导入函数并展示带时间结果，不复制解包、事务或缺失算法；每轮开篇重载模块并清空旧计划、代码、报告及规划就绪标志。导入失败或未经过规划时最后一格拒绝生成；成功后同一次 Run All 自动继续规划。修改流程前归档原 Notebook 及用户输出，交付 Notebook 清除过期输出但保留用户空单元格，最后代码格始终位于末尾。
- 剩余轮数按同一来源请求排序及有界装包算法预排当前全部候选，第一包用于本轮代码；不是总键数简单除以上限，也不是 API 调用数。最后一格明确包含本轮，以及本轮成功后预计余下轮数；估计以日历、政策不变且未来每个请求完整成功为前提，每轮自动重算。成功空返回仍按既有来源确认缺失规则推进，不无限重拉。
- 只有全白名单 data_missing=0 且上游日历已跟上当前就绪边界，才显示行情全量覆盖。无待办但有来源确认缺失时明确是可拉取任务完成、行情仍缺失；上游陈旧时不能称为最新。水位检查只读 TRADE_CALENDAR_SCHEMA 的自然日/交易日与白名单各频率 bar 结构最大日期，不重验上游已保证的历史质量；两份权威 Schema 均在 Notebook 开篇展示。最后给出明天北京时间策略就绪时刻（当前 00:01）后可再次运行，并提示上游日历须先同步。
- 远端代码必须在任何请求或文件操作前核对完整计划、来源请求、观察块三级身份，白名单、固定月份合约、字段、参数、日期时区、就绪时点、源边界、跨请求零重叠和全部批次上限。使用 Python 3.6 兼容语法，所有内置函数显式经 `py_builtins` 调用；不得依赖本地项目模块、PyArrow、账号信息或 Notebook 前序变量。
- 非空来源响应必须为精确来源列顺序、无时区 `DatetimeIndex` 的 DataFrame，索引归入实测北京时间语义。实测空响应为 Float64Index，因此空表不因索引类型被误拒绝，但仍须通过字段、调用状态和容量验证。调用中捕获到的任何来源警告均使整个来源请求失败，不把疑似截顶当成 `partial_missing`；有限 OHLC 关系异常则保留原值并标记 `warning`，不属于调用失败。
- 遇到首个 API 异常或响应验证失败时，整组观察块失败并丢弃该组事实，后续来源请求全部记为 `NotExecuted`，不得自动重试。已有独立成功来源请求时可发布完整可校验的 `partial` 包；一个成功来源请求也没有时直接报错，保留旧固定文件且不打印下载指令。手动中断、打包或序列化失败同样不得发布半成品。
- JSONL 复读按内存字节流逐行对账，不另建立一份全量解码行情列表。全部成员和 ZIP 在内存完成复读后才创建同目录唯一临时文件，写入并 flush/fsync、复读，再原子替换固定文件并最终复读；只能清理本次自己创建的临时文件。报告必须带运行时间、运行 ID、计划摘要、成功/失败计数、固定文件字节数和 SHA-256，只有最终复读成功才输出下载指令。
- 本项本地 Notebook 全单元格执行与模拟来源验证不能替代聚宽真实环境验收。第 08 项只构建导入器，第 09 项再进行正式协议真实小批次往返；当前 Notebook 不自动发起该往返，也不把模拟包放进项目 inbox 或 warehouse。

# 第 09 项真实小批次验收（已完成）

- 第 09 项历史验收曾显式设置 `acceptance_contract_dates=(("IF2409.CCFX", date(2024,6,28)), ("TF1303.CCFX", date(2012,6,11)))`。坐标仅用于该次验收，不是当前正常默认；正式日历始终决定 Session、理论数量与就绪性。
- 首次空库准备时，该范围为 4 个来源请求、6 个观察块、512 个理论键（2 日线、510 分钟）。2026-09-04T21:49:20.164421+08:00 的真实运行返回 IF 日线 1 条、分钟 240 条；TF 日线与分钟均成功零行。此结果来自本轮报告及文件，不是根据早期探针预造或裁掉 TF。
- 用户在本地 `latitude` 内核重启并按顺序运行 Notebook，核对本批范围后，把最终完整代码复制到聚宽干净内核的一个单元格手动执行。出现完整 `JQ_FINANCIAL_FUTURES_EXPORT_V1_BEGIN/END` 和下载指令后，手动下载固定正式 ZIP 并返回文件位置与完整输出；打包失败或没有下载指令时不取旧同名文件。
- 收到本轮真实报告和文件后，先按报告时间新建独立记录，保存原始输出、结构化报告、实际执行代码身份和下载文件字节/摘要核对结果；与本地准备计划身份不符时停下核查。然后由正式导入器对本批执行 `--write`，复核同 SHA 重导无写入、四表计数和来源观察覆盖，再重新运行同验收范围的缺失检测。只有传输、导入、幂等和精确缺失证据均闭合后才勾选第 09 项。
- `experiment_records/formal_roundtrip/prepared_YYYY-MM-DDTHHMMSS.ffffff+0800_item_09_vN/` 留存带本地准备时间的计划、生成代码非执行文本、准备验证和源码摘要，不冒充聚宽已执行；收到真实报告后另建以实际 `run_started_at` 命名的目录，沿用原始文本不变、SHA sidecar、分析与来源时间缺失标记规则。不能覆盖准备记录或旧轮次。
- 本项已完成真实文件摘要／计划核对、正式四表提交、同 SHA 重导无写入、逐块观察与精确缺失回查。证据见 [本次真实往返记录](experiment_records/formal_roundtrip/2026-09-04T214920.164421+0800_item_09_v1/analysis.md)。首库现有 1 个批次、6 个观察、1 条日线和 240 条分钟；TF 的 1 个日线键和 270 个分钟键仍为来源确认缺失，两组验收坐标的待拉取均为 0。
- 用户于 2026-09-05 明确要求全白名单半自动闭环，已授权取消 Notebook 的两合约日验收限制，进入第 10 项。每轮从头运行自动入库、删除和规划；全量历史尚需用户逐批在聚宽执行与下载。不得把实现或代码生成当作全量回补完成，不需要逐轮再取得 Agent 推进确认。

# 聚宽取数协议实验门禁

- 建设清单第 04 项必须是实际跨多轮的聚宽研究环境实验，不得只根据文档、现有 `jqdatasdk` 生产代码或 Agent 推断直接冻结。聚宽官方材料只形成实验假设；用户当前研究 Notebook 返回的完整输出才是环境行为证据。
- 该实验保持人工边界：Agent 生成小样本探针，用户复制到聚宽 Python 3 研究 Notebook 手动执行并返回结果。探针不得读取账号信息、输出凭据、自动登录、联网下载到本地或修改本地数据库。
- 第 04 项按以下顺序逐轮通过，后一轮代码必须依据前一轮实测修订：
  1. 环境、`jqdata` 可调用签名、单/多合约返回形状、日线/分钟字段、索引与时区。
  2. 分钟请求起止边界、Session 归属、早期 TF 可得性、空响应及当前数据就绪时点。
  3. 单次请求容量、资源或配额表现、异常类型，以及如何排除静默截顶。
  4. 研究目录直接写文件、同名覆盖、压缩能力、下载后字节摘要一致性。
  5. 一个含日线、分钟线和完整请求块证据的真实小包往返；只验证协议，不提前实施第 05 项的正式 Schema。
- 每轮必须使用有版本号的 BEGIN/END 标记输出机器可复制结果，并记录请求坐标、返回类型、字段、行数、首尾键、重复键、空值和异常类型。不得要求用户凭截图或手工转述猜测关键语义。
- 每次用户实际执行探针后，必须在 `experiment_records/joinquant_fetch_protocol/` 下新建独立目录。正常名称固定为 `YYYY-MM-DDTHHMMSS.ffffff+0800_round_NN_vN`：时间取探针报告初始化时的含时区时间，`round_NN` 表示逻辑实验轮次，`vN` 表示探针代码版本；重跑同一版本仍以新时间目录区分。历史 `executed_at` 字段按报告初始化时间解释，后续探针应使用语义更明确的 `run_started_at`。时间缺失、无时区或与报告时区矛盾时只记录异常，不得人工补造；未形成报告时间的失败使用 `recorded_YYYY-MM-DDTHHMMSS.ffffff+0800_round_NN_vN_failed_REASON`，明确表示目录时间只是本地记录时间。
- 记录目录已存在时必须停止，不得覆盖。`raw_output.txt` 首次落盘用户返回的可见文本并保留 BEGIN/END 外的异常输出；只有信封完整且 JSON 可解析时才从信封内原样提取 `result.json`。两份证据首次落盘后不可修改，并分别保存 SHA-256 sidecar；分析修订不得回写或重排原始结果。
- `analysis.md` 必须分别记录报告初始化时间、本地记录时间、可获得时的消息接收时间、完整性、摘要、已确认事项、未决事项、门禁判定和下一探针改动。它只是可修订的实验解释，生产代码不得扫描本目录取得规则；五轮闭合后仍须把获接纳结论同步到本文件、README、策略配置和正式实现。
- 每条记录必须关联执行时的探针版本和源码 SHA-256，并把用户实际执行的单元格源码作为不可执行的 `probe_source.py.txt` 证据快照及摘要一并保存；若历史运行缺少源码快照，必须如实记录可复现性缺口。默认交付方式是在对话中直接提供完整 Notebook 单元格，用户返回结果后再从已完成对话逐字提取该源码进入对应时间戳记录；不要求为了交付或执行预先创建一份独立 `.py` 文件。保存源码文本证据不改变代码归属。
- 只有确有独立本地调试或复用需要时才创建探索性 `.py` 文件；此类尚未接纳的文件仍只能位于根级 `00_draft_collection_02/scripts`，必须在实验记录中注明实际路径，且已经执行的版本不得被下一版覆盖。历史第 01 轮 v2 保留为 `probe_joinquant_financial_futures_round_01.py`，从 v3 起采用版本化文件名。是否存在独立草稿文件不改变探针的有界读取、人工聚宽执行和证据留存要求。
- `partial_missing` 指请求块完整成功、确认没有截顶但来源返回数少于理论数；它与配额停止、异常中断或疑似截顶不同。只有第 03 项定义的前一种情况可以形成成功观察，实验报告和后续协议不得混用。
- 任一轮出现环境差异、异常或反例时，先保留原始结构化输出并修订下一轮探针，不得为得到预期答案而静默删样本、换边界或放宽完成条件。只有五轮证据均闭合，才可同步本文件、README 和受影响配置并勾选第 04 项。

# 聚宽 Notebook 代码安全边界

- 聚宽研究 Notebook 的全局命名空间视为受平台预注入和既有单元格污染，不能假定 Python 内置名称仍指向 `builtins`。实测已经发生 `all` 与 `sum` 被遮蔽；后续探针、恢复单元格和正式生成代码必须显式 `import builtins as py_builtins`，所有内置函数调用统一写成 `py_builtins.<name>`，不得只修补已知的两个名称，也不得使用 `from jqdata import *` 等星号导入扩大污染面。API 参数名如 `round=False` 不属于函数调用，不受此条改名。
- 所有 manifest、请求块和行情成员必须先在内存中完成严格 JSON 编码、解码复读、键数量和摘要校验，再允许打开临时输出文件。生成或序列化在此之前失败时，必须保留既有固定名文件原字节，不得把旧文件描述成当前运行产物，也不得提示用户下载或导入旧文件。
- 固定名文件只能采用“内存构建 → 同目录临时文件完整写入并 flush/fsync → 临时文件复读核验 → `os.replace` 原子覆盖 → 固定文件再次复读核验”的顺序。任一步失败都不得把半成品提升为固定名文件；遗留临时文件只可有界清理并如实报告。
- 探针失败后的恢复必须先根据实际控制流判断 API 是否执行、各响应行是否已经保存在可复用对象中、固定文件是否已经替换，不能仅凭 traceback 所在行推断“不需要重拉”。没有保留完整行情值时，即使请求已经发生，也必须明确说明恢复将重复哪些有界请求。
- 依赖 IPython `In[n]` 的短恢复单元格只允许在同一未重启内核、原输入编号已经由 traceback 明确证明时使用；必须逐项验证待替换源码片段恰好出现一次、提升探针版本并保留原失败记录。内核已重启、输入编号不确定或替换计数不符时，停止恢复并重新交付完整修正版单元格，不得猜测历史输入。

# 第 04 项冻结的聚宽环境行为

- 聚宽研究环境的行情入口固定通过显式 `import jqresearch.api as jq_api` 后调用 `jq_api.get_price`；`jqdata.get_price`、`from jqdata import *` 和对 Notebook 预注入全局的依赖均不可用。当前实测环境为 Python 3.6.7、Pandas 0.23.4、NumPy 1.14.6，且没有 PyArrow，因此聚宽侧传输实现只能依赖该环境已有的标准库、Pandas 和 NumPy。
- 单合约 `get_price(..., panel=False)` 返回以无时区 `DatetimeIndex` 为索引的宽表；多合约日线返回含 `time,code` 的长表。正式生成器优先按单合约请求块处理，并必须把聚宽无时区索引按已知市场时区 `Asia/Shanghai` 显式编码，不能把无时区解释为 UTC。
- `1m` 请求的 `start_date` 与 `end_date` 都包含在返回边界内。对本项目日历定义的 `(session_start_at, session_end_at]`，请求起点必须直接使用首个理论分钟键，即 `session_start_at + 1 minute`，终点使用 `session_end_at`；相邻块不得共享边界时间戳。日期字符串不能代表整日分钟请求，反向区间必须在请求前由本地代码拒绝，因为聚宽会把它静默处理为成功零行。
- 每个请求块必须携带理论键数量、首尾键和有序键摘要，并用实际有序键逐项验证零重复、零额外及缺失集合。成功零行和成功部分返回分别是 `empty` 与 `partial_missing`；API 异常、结构异常和无法排除截顶的响应不是成功观察，不得仅按总行数、首尾日期或调用成功认定完成。
- `TF1303.CCFX` 的证券元数据生命周期为 2012-06-11 至 2013-03-08，但该完整生命周期的日线和分钟请求都成功返回零行；`TF1312.CCFX` 首个候选交易日则有真实日线和分钟数据。因此本地权威日历中的早期合约不能被预先裁掉，完整空响应必须作为来源确认缺失保存，而不是伪造行情或无限日常重拉。
- `IF2409.CCFX` 全生命周期单次分钟请求实测 38,640 行，与九个逐月请求的有序索引、全部字段值和 canonical SHA-256 完全一致；这只证明该范围内未观察到静默截顶，不构成平台最大容量承诺。聚宽没有暴露可用配额计数器，正式代码必须分别计数实际来源请求和观察块，并对每个观察块继续执行理论键校验。
- 当日 12:50 已可读取当日盘中日线和上午分钟，但这不能证明当日日线最终完成；正常缺失规划继续以北京时间 T+1 00:01 为完整数据就绪边界。该结论已经同步到 `financial_futures_collection_policy.py`。
- 聚宽环境支持 DEFLATE ZIP、同目录临时文件、flush/fsync 和 `os.replace`。人工下载后的文件字节与聚宽端完全一致；固定文件名只负责人工覆盖，批次身份必须使用整文件 SHA-256 与 `run_id`。第 05 轮还证明一个 ZIP 可以同时携带 manifest、逐请求块证据、日线 JSONL 和分钟 JSONL，并在两端逐成员复读，但这些探针成员名、行情字段和 `JQ_FINANCIAL_FUTURES_TRANSFER_PROBE.zip` 都不是正式 Schema 或正式文件名。

# 文件归属

- `plan_financial_futures_fetch.ipynb` 是唯一正式半自动 Notebook：第 3 节自动入库并删除已消化 ZIP，之后全白名单规划，最后第 10 节输出完成说明或下一批代码与预计剩余轮数；远端模板归它所有。
- `import_financial_futures_file.py` 是唯一正式本地导入实现；Notebook 第 3 节直接调用并显式 write=True，复核后删除已消化 ZIP；默认只校验，原有 CLI 保留兼容。
- `financial_futures_collection_policy.py` 是已经生效的项目局部白名单与频率配置；业务代码不得复制第二份同义列表。
- `financial_futures_local_contract.py` 是已经生效的项目库与人工传输可执行契约；规划 Notebook 与导入器必须直接复用，不得复制第二份字段、DDL 或稳定标识。
- `financial_futures_fetch_planner.py` 是已经生效的第 06 项只读规划实现；它不是为交付 Notebook 单元格而额外保存的临时脚本，后续正式 Notebook 直接导入该领域操作。
- `experiment_records/joinquant_fetch_protocol/` 只保存逐轮聚宽探针的不可变收到文本、可解析结构化结果、摘要和非权威分析，不保存项目行情、数据库状态或正式可执行入口。
- `experiment_records/local_fetch_planner/` 只保存本地规划器验收的带时区时间结果和非执行源码快照；临时合成数据库使用系统临时目录并在验收后清理，不进入记录目录。
- `experiment_records/local_notebook_export/` 只保存第 07 项本地 Notebook 和模拟导出验证的带时区时间结果、摘要与非执行验证源码；不把模拟行情、临时 ZIP 或运行输出写入正式 Notebook、inbox 或 warehouse。
- `experiment_records/local_file_import/` 只保存第 08 项本地导入器隔离验收的带时区时间结果、摘要和非执行源码；测试用 ZIP、DuckDB 和失败注入现场只存在于系统临时目录并在验收后清理。
- `experiment_records/formal_roundtrip/` 保存第 09 项带时间的准备记录和随后由用户返回的真实执行/下载/导入证据；两阶段必须明确分开。源码文本为复现证据，不是额外正式 `.py` 入口。
- `data/inbox/` 只接收用户从聚宽手动下载并覆盖的 `JQ_FINANCIAL_FUTURES_TRANSFER.zip`；其格式只服从本文件第 05 项协议。
- `data/warehouse/` 只保存固定的 `financial_futures.duckdb` 及 DuckDB 自身必要文件；已在第 09 项由正式导入器创建并提交首个真实批次。
- `data/staging/` 只保存当前导入批次的临时解包和校验产物，不是已提交数据或持久完成证据的来源。
- `data/` 下运行时内容不得提交到 Git；目录中的 `.gitignore` 只用于保留明确的目录骨架。
- 未经用户对具体文件及正式归属另行确认，探索性脚本、测试、审计和验证工具仍应进入仓库根 `00_draft_collection_02`，不得在本目录自行增加 `scripts/`、`tests/` 或通用工具层。

# 当前冻结范围

- 当前已经完成建设清单第 01—09 项：正式目录边界；金融期货白名单与精确缺失语义；单文件 DuckDB 与状态模型；五轮聚宽环境实验；四张表列级 Schema、正式固定文件及单文件协议；精确缺失检测与有界规划；本地规划 Notebook 与完整聚宽代码生成；本地固定文件校验与事务导入；真实小批次端到端验收。
- 第 08 项复用既有契约完成隔离模拟验收，没有创建正式项目数据库，也没有实际调用聚宽。第 09 项继续服从人工聚宽执行和人工文件下载边界；不得把本地模拟文件放进正式 inbox 或当作真实行情。
- 第 09 项于 2026-09-04 完成真实往返；第 10 项已进入半自动历史回补与日常闭环。操作说明和实现已改为每轮 Run All，但初始全量回补未完成，须以实际逐批下载、提交及最新日历下精确缺失回查为证，不提前勾选完成。
