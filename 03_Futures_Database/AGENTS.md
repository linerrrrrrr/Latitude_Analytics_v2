# 适用范围与规范地位

- 本文件是 `E:\Latitude_Analytics_v2\03_Futures_Database` 整棵目录树的目录级 Agent 规则入口，适用于目录本身以及任意层级的当前和未来子目录。
- 本文件同时是全项目数据字段命名、跨引擎类型和数据湖 Schema 行为约束的文本权威来源；修改其他目录中的数据生产者、读取者、转换代码或验证脚本时，只要涉及这些事项，也必须先读取并遵循本文件。项目级变量命名与是否需要改名统一服从根目录 `AGENTS.md`。
- 17 张稳定 silver 表（7 张日历维度表、10 张事实表）的可执行 Schema 以 `config/data_contracts.py` 为唯一权威来源；本文件负责规定 Agent 的修改流程、数据字段命名、类型映射和同步责任，两者不得冲突。来源原文与证据归档属于 raw 层，不计入 silver 表数；实验性 gold 输出由所属下游工作流局部定义，不进入数据库级表清单或统一 Schema。
- 下级 `AGENTS.md` 或 README 可以补充更具体的目录规则，但未经用户明确同意，不得豁免、弱化或覆盖本文件的数据契约要求。

# 数据字段命名与类型规范

本项目以 **Arrow/Parquet schema 作为落盘数据的唯一类型契约**。Pandas、Polars、NumPy
只是计算层；写入 Parquet 前必须转换并通过 Arrow schema 校验。

## 0. 规范索引与同步要求

- [根目录 AGENTS.md](../AGENTS.md)：变量命名与最小改动、项目运行环境、根目录定位和规范路由的项目级强制规则。
- [02_Futures_Lakehouse/AGENTS.md](../02_Futures_Lakehouse/AGENTS.md)：期货湖仓生产与运维目录树的目录级 Agent 规则入口，包含采集双轨、PythonExporter、`a00_02_sync_notebook_exports.py` 同步入口、operations 及根级旧项目归档路由。
- [.env.template](../.env.template)：项目根目录定位代码、当前稳定采集统一正式起点，以及包含 `raw`、`silver`、`gold` 的正式湖仓根路径环境变量权威模板。
- [数据采集链路 README](../02_Futures_Lakehouse/README.md)：19 个正式采集入口（含人工 b08）的来源异常留存与验收语义、双轨同步入口、Notebook 开篇 Schema 契约呈现、表粒度、主键、分区和更新水位规范。
- [数据采集 operations AGENTS.md](../02_Futures_Lakehouse/operations/AGENTS.md)：19 个正式环节的人工选择、18 项日常快捷选择、detached worker、可见总控台、状态发布、失败停止、现场保留与人工核查规范。
- [数据采集 operations README](../02_Futures_Lakehouse/operations/README.md)：正式 worker、monitor 与状态证据的操作说明。
- [旧项目归档规则](../05_Old_Projects/AGENTS.md)：重建前采集实现、更早历史采集项目和旧特征工程项目的只读保护。
- [特征工程 AGENTS.md](../04_Feature_Engineering/AGENTS.md)：独立特征工程项目的双轨、silver 消费与当前结构迁移阻塞规范。
- [特征工程 README](../04_Feature_Engineering/README.md)：主力连续合约、log 双向复权、期限结构边界及当前 gold 实验工作流说明；当前结构迁移不代表入口已恢复运行。
- [中国期货市场演变方法复现项目](../01_project_collection/china_futures_market_evolution_reproduction/AGENTS.md)：正式 silver 的只读研究消费者及固定复现口径路由。
- [中国商品期货日内波动预测方法复现项目](../01_project_collection/china_commodity_futures_intraday_volatility_forecasting_reproduction/AGENTS.md)：正式 silver 的只读研究消费者，以及项目专属版本化研究成果、项目内长批次控制、固定研究口径与逐项 Notebook 实施规范。
- [JQ_strategy 金融期货数据规则](../01_project_collection/JQ_strategy/financial_futures_data/AGENTS.md)：正式 silver 日历只读消费、人工聚宽文件传输和项目专属轻量数据库边界。
- [JQ_strategy 金融期货数据说明](../01_project_collection/JQ_strategy/financial_futures_data/README.md)：人工取数闭环、正式文件路由及逐项建设状态。
- [JQ_strategy 金融期货范围策略](../01_project_collection/JQ_strategy/financial_futures_data/financial_futures_collection_policy.py)：项目局部中金所金融期货白名单、支持频率与当前聚宽数据就绪时点，不改变正式商品事实生产政策。
- [国内期货事实采集白名单](../config/futures_lakehouse/futures_fact_collection_policy.py)：日线、分钟线和逐品种交易所报告共用的交易所—品种白名单唯一权威来源。
- [成交持仓排名特殊案例配置](../config/futures_lakehouse/futures_position_rank_special_cases.py)：已人工核实的特殊案例、完整坏载荷指纹、交易所原文摘要与完整校准值唯一配置来源。
- [外部市场请求实体配置](../config/futures_lakehouse/external_market_entities.py)：外部市场日历与外部指数事实共用的请求实体、Eastmoney 指标映射及有效期唯一权威来源。
- [宏观发布系列与可用日配置](../config/futures_lakehouse/macro_release_entities.py)：宏观发布日历、SHIBOR 与宏观事实共用的 25 个系列、来源列、宏观数值偏移、理论频率及版本化可用日规则唯一权威来源。
- [JQData 共享连接边界](../config/jqdata_connection.py)：JQData 认证与 Windows TUN 物理出口绑定的项目级实现；不承载业务采集或写入语义。
- [config/data_contracts.py](../config/data_contracts.py)：17 张稳定 silver Schema 及 Pandas、Polars、Arrow 转换的可执行契约。
- [Schema 展示实现](../02_Futures_Lakehouse/a00_03_notebook_schema_browser.py)：采集与研究 Notebook 共用的 Schema 只读浏览、关键内容选择和完整内容折叠实现。
- [read_futures_lake_demo.ipynb](read_futures_lake_demo.ipynb)：17 张稳定 silver 表的契约化读取演示，以及不计入 silver 表数的生意社 raw 原文与摘要核对示例。
- 修改本规范时，必须同步检查以上索引项；数据字段命名、类型、Schema 或转换入口发生变化时，相关文本和代码必须在同一次变更中更新。

## 1. 数据字段命名

- 数据字段一律使用英文 `snake_case`，不使用缩写拼音。
- 表示单个业务值的字段使用单数；字段名应直接表达业务含义，不依赖调用方上下文猜测。
- 日期、时间戳和一天内时间的字段语义不可混用：`trade_date` 永远不包含时分秒，`event_datetime` 必须包含日期和时刻，表示事件发生或更新时间的数据库字段优先使用 `*_at`。
- Python 变量命名及是否需要改名统一遵循根目录 `AGENTS.md` 的“变量命名与最小改动”。本文件不强制所有变量统一增加对象类型后缀；但当业务身份、数据表示、来源、处理阶段或契约后置条件有助于理解时，允许并鼓励使用稳定业务词根、真实角色修饰词及 `*_df`、`*_table`、`*_dataset`、`*_schema` 等显式表示后缀。`pandas_to_arrow()`、`arrow_to_pandas()` 等转换只改变对象表示并执行相应 Schema 契约，不自动改变业务身份、来源或生命周期；同一对象应保留角色修饰词和业务词根，只替换表示后缀，例如 `new_variety_calendar_df -> new_variety_calendar_table`，不得无真实语义地改成 `incoming_variety_calendar_table`。改名只在当前任务已触及的连贯代码边界内保持一致，不扩展为无关模块的批量风格清洗。

## 2. 跨引擎类型契约

下表与 `config/data_contracts.py` 的 `ARROW_TYPE_MAPPINGS` 是同一份规范的文本形式和
可执行形式，不存在“面向人”与“面向 AI”的区别。除参数化的 `decimal128(p,s)` 外，
每一行都必须在映射字典中具有对应项；两者必须在同一次变更中逐项同步。

| 业务含义 | Parquet / Arrow（落盘） | Pandas | Polars | NumPy |
|---|---|---|---|---|
| 自然日 | `date32[day]` | `date32[day][pyarrow]` | `Date` | `datetime64[D]` |
| UTC 时间戳 | `timestamp[us, tz=UTC]` | `timestamp[us, tz=UTC][pyarrow]` | `Datetime(time_unit='us', time_zone='UTC')` | `datetime64[us]`（NumPy 丢失时区，值必须先转 UTC） |
| 本地时间戳 | `timestamp[us, tz=Asia/Shanghai]` | `timestamp[us, tz=Asia/Shanghai][pyarrow]` | `Datetime(time_unit='us', time_zone='Asia/Shanghai')` | 不允许直接表示；先转 UTC |
| 一天内时间 | `time64[us]` | `time64[us][pyarrow]` | `Time` | `timedelta64[us]`（自午夜起微秒数） |
| 标识符、代码 | `string` | `string[pyarrow]` | `String` | `str_` / Unicode；禁止定长字节串 `S` |
| 布尔值 | `bool` | `bool[pyarrow]` | `Boolean` | `bool_` |
| 小整数/枚举 | `int8` | `int8[pyarrow]` | `Int8` | `int8` |
| 小整数/枚举 | `int16` | `int16[pyarrow]` | `Int16` | `int16` |
| 普通整数 | `int32` | `int32[pyarrow]` | `Int32` | `int32` |
| 普通整数 | `int64` | `int64[pyarrow]` | `Int64` | `int64` |
| 明确允许降精度的模型特征 | `float32` | `float[pyarrow]` | `Float32` | `float32` |
| 行情价格、成交量、持仓量、成交金额、连续指标 | `float64` | `double[pyarrow]` | `Float64` | `float64` |
| 要求十进制定点精度的金额 | `decimal128(p,s)` | `decimal128(p,s)[pyarrow]` | `Decimal(precision=p, scale=s)` | 无等价类型；不得用 NumPy 做最终金额存储 |

补充规则：

- 证券代码、合约代码、交易所代码、日期键即使只包含数字，也一律存 `string`，避免丢失前导零。
- `float32` 仅用于明确允许降低精度的模型特征；行情和统计计算默认 `float64`。
- 当前行情事实表的价格、成交量、持仓量和成交金额依照可执行 Schema 使用 `float64`；不得依赖二进制浮点精确相等。
- 金额或结算价若明确要求十进制定点精度，必须先将可执行 Schema 迁移为 `decimal128`，再写入相应数据。
- 所有时间戳必须声明时区。跨市场事件时间统一存 UTC；交易所本地规则可存明确的 IANA 时区。
- 缺失值落盘统一为 Arrow null。可空整数/布尔值不得通过 `float64 + NaN` 伪装。
- NumPy 不作为数据库交换格式。含 null、时区或 decimal 的数据应保留在 Arrow/Pandas/Polars 中。

Pandas 与 Polars 不得为同一张表分别维护独立类型定义。silver 的读取和转换必须显式传入
`config/data_contracts.py` 中的同一份 Arrow Schema；gold 实验则显式传入所属工作流的局部
Arrow Schema：

```python
calendar_df = read_dataset(
    table_path,
    partitioning,
    TRADE_CALENDAR_SCHEMA,
)
```

```python
calendar_pl_df = read_dataset_polars(
    table_path,
    partitioning,
    TRADE_CALENDAR_SCHEMA,
)
```

读取者必须在当前工作流直接使用 `pyarrow.dataset.dataset(...)`，再调用
`validate_arrow_table()` 与 `arrow_to_pandas()` / `arrow_to_polars()`。禁止恢复已归档的
`c00_lakehouse.py` 薄封装，也禁止依赖 Pandas 或 Polars 推断落盘类型。

## 3. 当前稳定 silver Schema 与转换入口

17 张稳定 silver 表（7 张日历维度表、10 张事实表）的权威定义均位于 `config/data_contracts.py`：

| 数据集 | Arrow Schema |
|---|---|
| `dim_trade_calendar` | `TRADE_CALENDAR_SCHEMA` |
| `dim_futures_variety_calendar` | `FUTURES_VARIETY_CALENDAR_SCHEMA` |
| `dim_futures_contract_calendar` | `FUTURES_CONTRACT_CALENDAR_SCHEMA` |
| `dim_futures_bar_calendar` | `FUTURES_BAR_CALENDAR_SCHEMA` |
| `fact_futures_missing_bar` | `FUTURES_MISSING_BAR_SCHEMA` |
| `fact_futures_daily` | `FUTURES_DAILY_SCHEMA` |
| `fact_futures_minute` | `FUTURES_MINUTE_SCHEMA` |
| `dim_futures_exchange_report_calendar` | `FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA` |
| `fact_futures_position_rank_daily` | `FUTURES_POSITION_RANK_DAILY_SCHEMA` |
| `fact_futures_member_position_daily` | `FUTURES_MEMBER_POSITION_DAILY_SCHEMA` |
| `fact_futures_warehouse_receipt_daily` | `FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA` |
| `dim_external_market_calendar` | `EXTERNAL_MARKET_CALENDAR_SCHEMA` |
| `fact_overseas_futures_daily` | `OVERSEAS_FUTURES_DAILY_SCHEMA` |
| `fact_external_index_daily` | `EXTERNAL_INDEX_DAILY_SCHEMA` |
| `dim_macro_release_calendar` | `MACRO_RELEASE_CALENDAR_SCHEMA` |
| `fact_interest_rate_daily` | `INTEREST_RATE_DAILY_SCHEMA` |
| `fact_macro_release` | `MACRO_RELEASE_SCHEMA` |

`fact_domestic_spot_basis_daily` 不再属于稳定 silver 契约，也不得继续出现在
`config/data_contracts.py` 或 silver 读取 Demo 中。旧正式湖若仍有该目录，当前 b02、契约同步和读取 Demo
不得自动删除、迁移、覆盖或把它误认为已完成 raw 归档；其处置必须等待用户另行决定。

生意社国内现货基差链路长期只保存来源原文，不生产结构化 silver 事实。每个 required 日期的 HTTP
`response.content` 原始字节与 SHA-256 sidecar 固定归档到
`raw/100ppi/domestic_spot_basis/year=YYYY/month=MM/observation_date=YYYY-MM-DD/{response.html,response.sha256}`。
raw 文件不套用 Arrow Schema、Hive Dataset 或 silver metadata；但写入链路仍必须在 staging 和正式路径逐字节
复读 `response.html`，并核对 `response.sha256` 与实际摘要一致。

### 3.1 稳定 silver metadata 永久契约

稳定 silver Schema 的 metadata 键使用英文 `snake_case`，值使用中文 UTF-8；PyArrow 中的键和值都必须
编码为 `bytes`。metadata 不仅要存在于内存 Schema，还必须写入 Parquet，并在 fragment、Dataset 和正式
路径重开后保持可读。

每张稳定 silver Schema 必须包含以下表级 metadata 键：

- `table_name`、`table_name_zh`、`description_zh`、`content_zh`；
- `field_list`、`grain_zh`、`primary_key`、`partition_columns`；
- `role_zh`、`calendar_role_zh`、`dimension_dependencies`；
- `source_systems_zh`、`source_apis_zh`、`source_columns_zh`；
- `update_mode_zh`、`quality_rules_zh`、`schema_version`、`metadata_language`。

每个字段必须包含以下字段级 metadata 键：

- `field_name_zh`、`description_zh`；
- `source_system_zh`、`source_api`、`source_column`、`transformation_zh`；
- `unit_zh`、`nullable_reason_zh`、`semantic_role_zh`；
- `enum_values_zh`、`quality_rules_zh`。

`field_list`、`primary_key` 和 `partition_columns` 均使用英文逗号分隔字段名，不插入说明文字；
`field_list` 必须与实际 Schema 字段顺序完全一致，`primary_key` 中的字段必须真实存在，
`partition_columns` 必须按 Hive 目录层级顺序排列。`metadata_language` 固定为 `zh-CN`；
`schema_version` 使用语义化版本，并在字段、类型、nullable 或持久值语义发生变化时同步调整。只校正
`description_zh`、`update_mode_zh`、`quality_rules_zh` 等描述性文字或运行策略说明，且字段与持久值语义不变时，
不得为此升级物理版本或重写历史 Parquet。

`role_zh` 只能按真实职责填写 `日历维度表` 或 `事实表`，不得恢复含混的“状态表”；日历表的
`calendar_role_zh` 说明其枚举的预期格点，事实表说明预期格点由哪个直接上游日历规定。
`dimension_dependencies` 只列直接依赖的 `dim_*` 表，不把 API、库、配置、事实表或质检回写列为维度。
`update_mode_zh` 必须准确记录所属入口的当前生产水位。a01/b01—b04 默认只推进尾部新增，历史维护由显式
`--full` 完成；b05/b06 按当前白名单和可信 `is_fetch_completed` 形成待办；其他入口继续服从
“上游当前有效格点 − 下游已经完整落盘的格点”。metadata 必须说明空湖如何自然全建、显式全历史或定向
维护边界，以及 `--write` 只提交当前计划。`quality_rules_zh` 必须覆盖表级主键、完整性、范围、对账和所属
提交链路实际执行的复读摘要边界。

不可空字段的 `nullable_reason_zh` 写明不可为空；可空字段必须写出允许为空的具体业务条件。
枚举字段的 `enum_values_zh` 列出允许值及含义，非枚举字段明确标为非枚举；字段级
`quality_rules_zh` 写明适用的非空、唯一、范围、时区、精度或跨列关系。

来源 metadata 必须记录实际来源。API 原始字段写明真实 API 与原列名；DataFrame 索引字段明确标为索引；
派生字段使用实际输入列并说明转换，不得伪造 API 原列；常量、运行状态和审计字段标为系统生成；多源证据字段
列全直接证据来源。表级 `field_list` 只用于快速阅读和顺序校验，不能替代逐字段 metadata。

稳定 silver 的物理 Schema 或持久值语义变更必须在同一次变更中同步生产者、消费者、读取 Demo、验证、
现有 Parquet 数据及本规范；纯描述性 metadata 校正同步当前代码与规范即可，不迁移历史文件。验收至少覆盖字段名、顺序、Arrow 类型、nullable、全部强制 metadata 键、UTF-8 解码、
`field_list` 一致性、fragment physical schema 兼容性以及 Dataset 重开后的 metadata round-trip。

### 3.2 Schema metadata 的单一来源与运行时读取

稳定 silver 表的物理表名、业务主键和 Hive 分区顺序只在对应权威 Schema 的 `table_name`、
`primary_key` 和 `partition_columns` metadata 中定义。`config/data_contracts.py`、配置模块、生产者、
消费者和 Notebook 均不得再用字符串或列表平行定义同一组值。

业务模块导入具名权威 Schema 后，在模块初始化时直接读取这三项 metadata 各一次，并在后续路径构造、
Hive partitioning、唯一性校验、排序和日志中复用读取结果。不得在每个使用点重复解码，也不得在
`config/data_contracts.py` 中导出一套派生的表名、主键或分区常量。例如：

```python
from config.data_contracts import FUTURES_VARIETY_CALENDAR_SCHEMA

TABLE_NAME = FUTURES_VARIETY_CALENDAR_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")
PARTITION_COLUMNS = FUTURES_VARIETY_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")
PRIMARY_KEY = FUTURES_VARIETY_CALENDAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")
```

这些模块级变量只是同一份 Schema metadata 的运行时读取结果，不是新的契约来源。必填 metadata 缺失时应
直接失败，不得用默认表名、默认主键或默认分区掩盖契约错误。业务代码直接使用具名 Schema 常量，不再增加
`SCHEMA = FUTURES_VARIETY_CALENDAR_SCHEMA` 这类纯改名层。

如果某项业务算法确实使用不同于表级主键的匹配列、分组列或比较列，应按具体用途命名，例如
`SESSION_MATCH_COLUMNS`，并在当前工作流内说明其算法语义；不得把这类局部列集合命名为 `PRIMARY_KEY`，
也不得回写为表级 Schema metadata。不得仅为隐藏三次 metadata 解码而创建 accessor、adapter、数据类或
其他薄封装；项目级 metadata 完整性验证则可以作为独立的数据契约校验操作集中实现。

正式湖仓的共享业务配置集中在 `config/futures_lakehouse/`，生产者与研究消费者均可直接导入。
事实采集白名单、外部实体和宏观系列说明当前维护范围及解释口径；已核实特殊案例说明校准依据。
这些配置不能替代正式数据与日历完成状态对实际可用范围的描述，也不能代替研究项目自己的样本选择。
例如期货事实白名单缩小时保留既有历史事实，当前白名单不等于湖中全部可用数据。
配置目录迁移只更新导入与规范链接；`config/data_contracts.py` 的既有 metadata 中旧配置路径保留为
已经落盘的来源说明标识，对应模块现统一位于 `config/futures_lakehouse/`。不得仅为整理代码路径改写
Schema metadata 或历史 Parquet，以免影响仍执行精确 metadata 比较的入口。
湖仓目录扁平化同样不改写既有 Schema metadata 的来源标识：其中旧业务组 b01—b04 对应当前 a01—a04，旧步骤 cNN 对应当前各组的 bNN；实际代码位置与执行顺序以 [湖仓 README](../02_Futures_Lakehouse/README.md) 为准。该映射仅解释历史来源，不定义第二份表契约。

### 3.3 Notebook 开篇 Schema 契约呈现

直接涉及权威 Arrow Schema 的业务 Notebook，应在开篇呈现当前工作流的 Schema 契约。该呈现是权威
Schema 的只读投影，不是第二份语义来源。Notebook 使用 `02_Futures_Lakehouse/a00_03_notebook_schema_browser.py` 中的同一
展示实现，传入当前工作流直接涉及的具名权威 Schema，并按上游依赖在前、当前产出在后的顺序排列。
采集 Notebook 另外显式传入 `lake_root=settings.futures_lake_root` 启用只读数据样例；其他调用方不传路径时仍只浏览契约。
导入路径在现有项目根定位的命中分支中加入 `candidate_root / "02_Futures_Lakehouse"`，随后直接
`from a00_03_notebook_schema_browser import display_schema_metadata`；具体写法见 [湖仓目录规则](../02_Futures_Lakehouse/AGENTS.md#共享配置与-notebook-展示归属)。

默认界面展示三块表格：Schema 列表、所选表的用途与粒度说明、关键字段目录。宽窗口中后两块并排，窄窗口上下排列。
Schema 列表保留原有六列横向总览：英文表名、中文表名、字段数、主键、Hive 分区、Schema 版本；
中英文表名分别成列，不合并或折叠该总览。表选择框紧邻所选表说明；Schema 列表、所选表说明及数据样例三个区块上方使用相同分隔线。
必须同时考虑这三块表格和按需查看的单字段详情的阅读负担，不得只裁剪 metadata 行数或列数。
约 2/5 只是减少冗余的参考，不是各表固定配额；不用凑足行数，不要求用户逐表指定取舍。

表级默认按职责选择 2—3 项权威 metadata，用“用途”“一行代表”“何时可用”等中文阅读标签呈现，
保留理解业务必需的 Session、空行情、报告期与可用日期等边界。主键、分区、来源、版本和长质量规则可在完整
表说明中展开。逐表选择及 Notebook 覆盖关系见
[采集链路 README](../02_Futures_Lakehouse/README.md#notebook-开篇-schema-契约呈现)。
选择配置只保存权威 Schema 引用、metadata 键及业务重点字段名，不复制或改写任何契约值，不按字符数截断说明。

默认字段目录只展示主键和逐表选定的业务重点字段，列为“字段”“含义”，并明确显示已展示数与总数。
主键集合只能从当前 Schema metadata 读取；业务重点选择不是另一份 Schema 或主键定义。完整字段目录点击展开，
保留所有字段及英文字段名、中文字段名、Arrow 类型、结构角色、语义角色、来源系统，长目录可滚动。
结构角色仍从当前 Schema 的 `primary_key` 和 `partition_columns` 计算。
字段详情入口默认折叠，不自动选择第一个字段，也不显示详情表；展开后可从包含所有字段的下拉框中选择查看含义、类型、
有效单位、取值与空值条件、枚举含义或生成方法，使用中文标签，省略无意义单位和完全重复的补充说明。
全部原始表级、字段级 metadata 均必须可展开和收起；切换表时清空字段选择与详情，切换字段时完整详情恢复折叠。

数据样例跟随所选 Schema，每次只读一个已有叶分区，默认显示至多 10 行、主键及逐表选定的关键字段（最多 8 列）。
标题与刷新同排；其下筛选项按网格对齐、标签置于输入框上方；行数与字段开关另排一行，结果提示紧邻样例表。
“行数”独立选择 10、20、50、100；“显示全部字段”只控制列，保留所选行数。表格在固定高度内滚动，不加载全表。分区候选逐层读取目录，年份和月份默认选择已有的较新值；
品种、合约、指标候选只来自所选分区的有界窄列读取，并允许手动输入其他值。日期以“全部日期 / 指定日期”切换；指定日期时显示可选取或输入日期的日历控件，全部日期仅取消当前分区内的日期筛选，不扩大读取范围；明细表仍默认聚焦一个已有日期。候选不完整及有界读取未命中时
必须明确说明，不能把局部样例或未命中解释为全表覆盖或全表无数据。当前筛选的至多 100 行可在内存中复用；缩小行数或切回已有字段不再读盘，刷新和切换分区必须清除缓存并重读；样例内部按主键排序不代表最新记录。
Schema 和 metadata 始终来自 `config/data_contracts.py`；目标表或分区不存在、没有 Parquet 文件、零行时显示空表头与
尚未生成数据状态，不创建目录或示例行；筛选无结果与权限、损坏文件、物理 Schema 不兼容等读取错误分别呈现。
样例读取只核对打开文件的物理兼容性，不重复生产者的完整业务质量验证，不强求历史描述性 metadata 与当前文本一致。
raw 环节只显示指定归档文件的存在状态、大小及已保存的 SHA-256 文本，不解析响应正文，不把文件存在等同于验收通过。

展示代码不得硬编码 silver 表名、主键、分区、字段说明或 Schema 版本，不得访问 API 或产生写入副作用；
未显式传入湖仓路径的 Schema 浏览不得读取湖仓。数据样例不得从表根递归扫描完整历史，也不得先全量加载再截取。
展示不能代替 Schema/metadata 完整性和 Parquet round-trip 校验。展示
单元格只在交互式 Notebook 内核中运行；深浅色主题、重复执行释放旧 widgets、控件与表格视觉分组均属于
统一实现的验收要求。

统一转换入口：

| 方向 | 函数 |
|---|---|
| Arrow 类型 → Pandas/Polars 类型 | `ARROW_TYPE_MAPPINGS` / `resolve_type_mapping()` |
| Arrow Schema → Pandas 逐字段类型 | `pandas_dtypes()` |
| Arrow Schema → Polars 逐字段类型 | `polars_dtypes()` |
| Arrow 校验/安全转换 | `validate_arrow_table()` |
| Pandas → Arrow | `pandas_to_arrow()` |
| Polars → Arrow | `polars_to_arrow()` |
| Arrow → Pandas | `arrow_to_pandas()` |
| Arrow → Polars | `arrow_to_polars()` |
| 带契约的空 Pandas/Polars 表 | `empty_pandas()` / `empty_polars()` |

任何 silver 生产写入都必须通过相应 Schema；不得直接调用 `DataFrame.to_parquet()`，也不得
在其他模块重复定义同一张 silver 表的 Schema。字段新增、删除或类型变更必须先修改可执行契约，
再同步迁移生产者、读取者、验证脚本、已有 Parquet 数据以及本规范。gold 的实验输出 Schema
保留在拥有该实验的工作流内，可随研究迭代，不加入本节清单，也不要求同步数据库标准 Demo。

`dim_futures_bar_calendar` 与 `fact_futures_missing_bar` 必须把 `bar_frequency` 作为第一层
Hive 分区；其他表严格使用各 Schema 的 `partition_columns` metadata 顺序。a01/b01—b07 与 `a02/b03` 的 dirty 完整叶只
执行一次完整业务 validator，staging 与正式安装只检查物理字段、类型、nullable、表名、主键、分区和
行数/主键摘要；运行时允许描述性 metadata 与历史文件不同，并以当前 `config/data_contracts.py` 为说明权威。
其他入口继续按各自既有 staging/正式复读契约执行。分钟线分区增量写入仍必须先通过
`pandas_to_arrow()` 或 `polars_to_arrow()`，禁止由某批 DataFrame 的推断类型暗中改变数据库类型。

`dim_futures_bar_calendar` 中 `evidence_level='reconciled'` 的记录只表达日线—分钟重聚合旁证；
只有 `evidence_level='authoritative'` 才允许 `schedule_status='confirmed_closed'` 并免除拉取。
不得把工作日休市间隔或行情一致性推断直接等同于 Session 已确认关闭。

`dim_futures_variety_calendar` 和 `dim_futures_contract_calendar` 必须来自
`get_all_securities(["futures"], date=None)` 的完整固定月份合约目录，事实采集白名单不得裁剪维度行。
`dim_futures_bar_calendar` 与 `dim_futures_exchange_report_calendar` 同样保留完整理论格点。`b04` 不读取或
解释事实采集白名单，其内容只由 `config/futures_lakehouse/futures_fact_collection_policy.py` 定义。`b05` 和 `b06` 分别在
完整 `1d`、`1m` 理论格点上窄列向量应用同一政策，通过 `is_fetch_required`、`selection_reason` 回写政策变化，
且不得删除理论格点。白名单扩大自动回补从未完成历史格点；缩减只停止后续事实采集并清零当前缺失，
不得删除事实或清除成功批次、实际条数、质量与 b07 证据；再次纳入的已完成格点不得重复拉取。
逐品种交易所报告的同一白名单与覆盖规则由报告日历生产者应用；具体报告还必须同时满足 API 覆盖期及
交易所—品种支持范围。

`dim_external_market_calendar` 必须保留上游有效自然日与当前请求实体配置形成的完整理论格点：国内现货和
按日返回整表的 JQData `FUT_GLOBAL_DAILY` 使用 `entity_code=ALL`，Eastmoney 外部指数使用来源
`INDICATOR_ID`。工作日、交易日和配置有效期只通过 `is_fetch_required` 与中文原因表达，不得删除无需请求日；
请求实体、指数项目代码、中文名、分类和有效期只由 `config/futures_lakehouse/external_market_entities.py` 定义。

`domestic_spot_basis/ALL` 的下游完成证据是正式 raw 原文字节、匹配的 SHA-256 sidecar 与外部市场日历状态
三者共同完整，不再依赖任何结构化事实表。生产待办集合固定为
`required 日期 −（原文字节与 sidecar 共同完整且日历状态完整的日期）`。原文归档完整但日历状态缺失或陈旧
时，b02 必须从正式 raw 复读证据并无 API 修复日历；原文缺失或摘要不一致时才重新请求。HTTP 200 的任意
`response.content` 在正式归档和摘要核对成功后，日历统一写为 `fetch_status=success`、`record_count=1`、
`quality_status=passed`；正文为空、HTML 结构变化和业务内容均不得改变该结论。网络错误或非 200 响应仍不得
伪装成成功归档。页面结构监测、历史重采、解析、字段提取和结构化事实生产均属于未来另行确认的独立项目。

`fact_external_index_daily` 除正向 API 待办外，还必须计算“正式事实格点 − 当前 `external_index`
required 格点”的无 API 清退范围。清退格点按旧事实自身的 `index_category/year/month` 定位原完整叶分区，
保留分区内其他事实；staging 与正式路径均逐值复读，正式计数确认为 0 后才算清退完成。此规则不覆盖上文
“期货事实白名单缩减只停止后续采集、不自动删除历史事实”的专门政策。

`dim_macro_release_calendar` 没有直接上游维度表，也不得为了生成理论格点调用 Tushare 或 Eastmoney。
它必须从 `FUTURES_DATA_START_DATE` 到北京时间当前日，按 `config/futures_lakehouse/macro_release_entities.py` 的 25 个
稳定系列、理论频率和版本化可用日规则生成完整水位：SHIBOR 只生成普通工作日，CPI/PPI/PMI 生成月末，
GDP 生成季末；是否 required 只由项目可用日是否已到决定。政策字段逐值不变时继承 b02/b03 回写状态，
新增、内部缺口、系列撤销、可用日到达和规则变化都由完整表比较识别。普通写入替换完整
`dataset_name/year/month` 叶分区。当前契约版本的既有状态信任正式提交证明，配置政策变化由生成时逐格点比较处理；
只有物理兼容但 `schema_version` 不同的历史才执行一次当前规则兼容识别，旧版本写入迁移必须使用无日期自动模式，
不能通过当前规则时不继承旧状态。生成结果执行一次完整业务验收；staging 一次物化后在内存逐叶核对，正式整表复读一次，
均保留物理结构、表身份、契约版本、行数和完整内容摘要验收。纯描述性 metadata 差异不形成迁移或历史重写，
b02/b03 消费本日历时采用同一兼容边界，新写文件仍携带当前完整 metadata。该入口本身不得把项目规则可用日描述成
API 实际发布日期，也不得擅自删除两张下游事实表的历史行。

`fact_interest_rate_daily` 只消费上述日历中 `dataset_name=interest_rate` 且 required 的系列—日期格点。
完成集合必须由正式事实和日历状态共同证明；正式事实完整但日历状态陈旧的格点从正式事实无 API 修复，
事实缺口才按 `year/month` 月度窗口调用 Tushare `pro.shibor`。8 个系列及
`date,on,1w,2w,1m,3m,6m,9m,1y` 来源映射只从 `config/futures_lakehouse/macro_release_entities.py` 读取；范围响应中只接纳
精确待办期限，不得覆盖已经完整的同月格点。请求日期必须唯一且位于范围内，单次不得超过官方 2000 行；
非空利率必须为有限数、保留百分比年利率原单位并位于 `[-100, 100]` 硬边界。完整响应中某精确期限没有
有效值时（Tushare DataFrame 中的 `None`、`pd.NA` 或 `NaN`），事实正式复读 0 行后才可记
`empty_confirmed + warning`；缺列、重复、越界、布尔值、无穷值、其他非数值或越界利率必须失败。每个
`year/month` 完整事实叶和对应 `interest_rate/year/month` 完整日历叶均须在
staging 与正式路径通过物理字段、类型、nullable、表名、主键、分区、契约版本及逐值复读，事实正式复读成功后才能推进日历完成状态。
当前版本正式历史信任生产者业务证明；来源转换结果和待提交的 dirty 完整叶分别执行业务验收一次。
纯描述性 metadata 差异使用当前契约，不触发历史重写；物理和身份兼容的旧事实版本仍须在无日期写入模式中
按当前规则完整验收后迁移。启动只进行一次事实计数与日历完成状态对账，主循环复用事实和日历分区映射，
每月只合并、更新和正式复读当前叶；修复后及批末复用已验收的提交证据，不再重读和复验全历史。

`fact_macro_release` 只消费上述日历中 `dataset_name=macro_release` 且 required 的系列—报告期格点。
完成集合必须由正式事实和日历状态共同证明；正式事实完整但日历状态陈旧时从正式事实无 API 修复，
事实缺口才按 Eastmoney 报告名与 `year/month` 窗口分页请求。17 个系列的报告名、原列、单位和数值偏移
只从 `config/futures_lakehouse/macro_release_entities.py` 读取；PPI 同比必须使用 `BASE_SAME`，CPI 全国/城市/农村累计与
PPI 累计来源是以 100 为基准的累计指数，必须按共享 `source_value_offset=-100.0` 转成累计同比百分比。
Eastmoney `REPORT_DATE` 使用报告月 1 日编码：CPI/PPI/PMI 归一到该月最后一个自然日，GDP 只允许
3/6/9/12 月并归一到季末；它不是发布日期。每个报告请求必须冻结首页 `pages/count`，核对后续页元数据、
页长、累计行数、日期范围和跨页来源日期唯一性，只接纳精确待办格点。当前生产事实值不得为空、为布尔值
或非有限数；完整分页中某个精确系列缺值时，事实正式复读 0 行后才可写
`empty_confirmed + warning`。每个 `year/month` 完整事实叶和对应 `macro_release/year/month` 日历叶均须
在 staging 与正式路径通过物理字段、类型、nullable、表名、主键、分区、契约版本及逐值复读，事实正式复读成功后才能推进日历完成状态。
当前版本历史信任生产者业务证明；来源输出与每次 dirty 完整叶分别执行业务验收一次。描述性 metadata 差异不重写历史，
兼容旧事实版本仍限无日期写入时验收并迁移。启动一次共同对账后，只处理当前叶并继承同月已提交结果，修复后和批末不重读全历史。

### 正式湖根路径、raw 归档与 silver 写入水位

- `.env` 的 `FUTURES_LAKE_ROOT` 定义唯一正式湖根目录；来源原文与证据归档、稳定表和实验性输出分别位于 `settings.futures_lake_root / "raw"`、`settings.futures_lake_root / "silver"`、`settings.futures_lake_root / "gold"`。生产者、消费者和读取示例不得各自硬编码 `03_Futures_Database/futures_lake`。raw 的专门归档契约按本文件执行；这一目录约定不定义 gold 的表集合或 Schema。

#### 来源异常留存与 silver 验收

- 质量异常不等于采集失败。来源响应能够按目标表粒度、主键、字段类型和持久值语义无歧义表达时，生产者必须保留来源值提交 silver，并在对应格点记录持久 `warning`；该完成证据不得因质量状态不是 `passed` 而触发日常重拉。
- 已取得的响应若不完整或不能无损表达为唯一契约事实，必须先按该生产者的 raw 证据契约留存完整接收载荷、请求坐标、采集时间、摘要和失败原因。只有具有显式且可追溯的归一化或校准规则才可继续提交 silver，否则必须阻断；raw 留存不等于 silver 通过。
- raw 证据的路径、编码、原子提交、正式复读和保留边界必须在对应生产者迁移时一并确认并同步代码、metadata 与测试。尚未具备该契约的生产者继续执行现有 silver 门禁、失败留痕及已经冻结的显式归一化，不得自行发明通用 raw 路径、新增静默取舍或提前放宽校验。下文的失败、硬失败和拒绝提交均指 silver 验收结果。

- 除 a01 已迁移日常链路和 `a02/b03_warehouse_receipt` 外，每张 silver 表的生产待更新集合仍从契约化格点求差：`上游当前有效格点 − 下游已经完整落盘的格点`。a01/b01—b04 默认只处理可信水位后的尾部新增，历史内部缺口、删除与修订由 `--full` 发现；b05/b06 只以当前白名单和 `is_fetch_completed=false` 形成事实待办。空湖仍由同一默认入口自然全建。
- 下游消费者必须信任生产者正式提交的上游表：正式提交已经证明上游主键、水位、覆盖和完整表级业务质量。消费者仍须精确检查上游 Schema/metadata 的物理兼容性，但不得重新扫描或复算上游已经保证的主键唯一性、日期连续性、范围覆盖和派生质量；只验证未写入上游契约、但确属自身计算前提的局部边界。消费者对自己的输出继续承担完整契约与质量校验；代码收缩按用户确认逐脚本实施，不自动扩大为批量重构。
- 正式 silver 写入命令不得用日期区间截断自动生产水位。a01/b01、b02、b04 只有 `--full --write` 可正式维护全历史，且 `--full` 与显式日期互斥；b03 允许成对日期或 `--full` 定义来源质检范围；b07 只有带日期或合约边界的 `--force --write` 可正式定向旁证。其他入口显式日期只允许只读检查或写入不同于正式湖的测试湖。
- `dim_futures_contract_calendar` 默认只处理可信品种日历相对正式表新增的交易日，不再按历史 `active_contract_count` 与 distinct `contract_code` 数差异触发日常 API；无有效 Session 规则的合约 warning 跳过并推进日常水位。成对日期与 `--full` 分别检查指定范围和当前上游全部水位与本地整表，包含来源拉取和双向比较，排除 `updated_at`；`--write` 只提交发现的差异。b03 信任 b02 已提交语义，仍完整验证自己的 API 响应和 dirty 输出叶。
- 空正式表和小规模缺口使用同一规则：空表的已完整格点集合为空，差集自然等于上游全量。下游表必须先读取已由上游生产者正式提交的水位，不能生成或提交超过上游的格点。
- `fact_futures_daily`、`fact_futures_minute` 与 `fact_overseas_futures_daily` 的来源 OHLC 非空值必须有限；有限数之间的 high/low 跨列关系异常属于必须原值保留的来源质量证据，不得修写或丢弃。生产者仍须把对应格点标为已完成并记录 `warning`；该异常不改变事实计数、不得被日常重拉或缺失审计覆盖为 `passed`，b06 也不得覆盖 schedule 开市证据。日线和分钟线的 NaN/Inf 继续硬失败；境外期货生产者把财务库可空数值列的 `None`、`pd.NA` 和 Pandas `NaN` 缺失标记归一为 Arrow null且不另记 warning，非空非有限数继续硬失败。负数量、重复键、请求范围和 Session 越界仍按各表既有门禁处理。
- `fact_futures_position_rank_daily` 与 `fact_futures_member_position_daily` 的 a02/b02 生产者串行按 `exchange_code + underlying_code + year + month` 归并 required 待办日，以 JQData `finance.run_query(FUT_MEMBER_POSITION_RANK.day.in_(pending_dates))` 请求月份待办子集。恰好返回 5000 行表示可能截顶，必须丢弃该响应并按排序日期确定性二分；单日仍触顶时硬失败。来源 `rank` 必须是 `1—20` 的整数，且 Top 20 是具体合约每类榜单的边界，不是品种日总行数上限。两张正式事实的格点计数与 `position_rank`、`member_position` 两类报告日历完成状态共同证明格点完整；生产循环只读取、验证和替换目标 `exchange_code/underlying_code/year/month` 事实完整叶及 `dataset_name/exchange_code/year/month` 日历完整叶，不扫描或提交无关表根。该入口不提供 `--full`，不使用 `run_offset_query` 分页、并发或业务自动重试。
- 交易所报告日历的 `is_fetch_required` 表示当前采集义务，`is_fetch_completed` 是持久完成凭证。政策排除只停止调度并清零当前缺失，不清除完成批次、计数和质量；重新纳入的已完成格点不得重复采集。
- `fact_futures_position_rank_daily` 与 `fact_futures_member_position_daily` 的生产者遇到同一来源业务键重复行时，只有原始会员标签、排名类别 ID/文字、指标和变化全部一致才允许按权威事实粒度合并；逐会员排名取最小名次，并把对应报告日历永久记为已完成的 `success + warning`。任一来源业务值冲突继续硬失败；已完成 warning 格点不得因质量状态不是 `passed` 而进入日常重拉。
- `a02/b01a_position_rank_special_case_calibration` 固定先于 b02，且只处理 `config/futures_lakehouse/futures_position_rank_special_cases.py` 显式列出的案例。正式 raw 证据缺失时各请求一次冻结的交易所 URL，只有 HTTP、响应 SHA-256、目标合约和完整 Top 20 同时精确匹配才原子提交；已有证据只复读。b02 只在指定格点的 JQData 成交量榜完整 20 行精确命中冻结坏载荷时，才依据该正式证据整组校准为交易所权威 Top 20；权威载荷原样通过，第三种载荷硬失败，持买仓、持卖仓和其他格点不变。报告日历必须永久保留含案例 ID 和原文摘要的 `success + warning`；该完成凭证不得触发重拉。
- `fact_futures_warehouse_receipt_daily` 的 `a02/b03` 生产者只以 `warehouse_receipt` 报告日历中 required 且 `is_fetch_completed=false` 的格点作默认待办，信任已完成快照，不扫描 clean 事实历史或在批末复读表根。每个待办日请求一次 API；事实与日历 dirty 叶通过校验和正式安装检查后，才推进日历状态。
- `a01/b08` 只以 b04 required 且已完成的理论分钟主键减去 b06 正式分钟事实投影的 `contract_code,bar_at` 主键。它信任 b06 正式提交的事实主键、范围、数值与 OHLC 质量，不读取行情值、不调用 b07；只回写实际/缺失计数与检查时间，既有质量结论、旁证及完成状态必须逐值保留。缺失明细与触达的完整日历叶继续协调提交、共同回滚。
- `a01/b07` 默认只处理 b06 新写入 `fact_futures_minute:formal_empty_session` 且尚无 b07 校对结果的 `suspected_closed` Session；不得计算历史输入指纹探测事实变化。四项比较全部匹配可写 `evidence_level=reconciled`，但分钟缺失的 `quality_status` 必须保持 `warning`。`--force` 只有带日期或合约范围才能写正式湖。
- operations 总控台的日常快捷选择为 18 个阶段，其中 a01 为 b01—b07；操作者可以单独选择任意正式环节并配置其原 CLI 参数。b08 不进入日常快捷选择，必须人工单独选择并显式设置 `--confirm-full-quality`，提交时另选 `--write`；允许这个经人工确认的显式批次交给 detached worker。总控台不改变各业务入口的写入范围和数据契约。

gold 用于下游数据组织、特征探索和复权方法探索，不由本数据库规范统一定义表名、字段、Schema、
分区或更新方式。某个实验需要落盘时，由所属工作流局部定义并校验其当前输出结构；不得把该实验
快照加入 `config/data_contracts.py`、本节正式表清单或标准 silver 读取 Demo。

`01_project_collection/china_commodity_futures_intraday_volatility_forecasting_reproduction/data/item09/<run_id>/`
是该方法复现项目在正式湖仓之外局部定义的版本化、经验证 research artifact，不是 `raw`、`silver`、
`gold` 或普通缓存。它不得加入稳定 silver Schema、生产水位或标准读取 Demo；其扁平 checkpoint 文件、指纹、原子 Parquet 提交、
manifest、`_SUCCESS`、不可变提交和下游固定 run 规则只由对应项目 AGENTS 与 README 约束。本数据库规范仍只要求
该项目对正式 `silver` 保持只读，并对落盘 Parquet 使用明确的局部 Arrow Schema、`float64` 统计值和 Arrow null。

## 4. 数据湖读取 Demo

### 重建前历史来源标签

`fact_futures_daily` 的当前生产来源标签为 `JQData_get_price_daily_get_extras_raw`，历史迁移还必须接受 `JQData_get_price_1d_skip_paused`；历史行缺少当前生产新增的昨收、结算及变化来源字段时全部保留为 null，不得由相邻行推算。`fact_futures_minute` 的当前生产来源标签为 `JQData_get_price_1m_skip_paused_fq_none`，历史迁移还必须接受 `JQData_get_price_1m` 与 `JQData_get_price_1m_skip_paused`。来源标签是历史事实证据，迁移不得伪造为当前标签。对应一次性迁移工具和隔离测试位于根级 `00_draft_collection_02/scripts` 与 `00_draft_collection_02/tests`；`--execute` 必须先通过预检并经人工复核，旧湖与事务备份只可在全量正式复读通过后清理。

- `read_futures_lake_demo.ipynb` 是 17 张稳定 silver 表的标准读取演示；必须逐一使用本节列出的
  权威 Schema 常量，且不得恢复已被 `dim_futures_bar_calendar` 替代的旧状态表。
- 每张表必须由独立代码单元格演示；单个演示单元格不得同时读取多张表。公共导入、项目根目录定位、
  分区定义和展示函数可以放在单独的初始化单元格。
- Demo 必须使用项目规定的根目录标记文件搜索方式、`latitude_env_v2` 环境、
  `config/data_contracts.py` 中的权威 Schema 及统一 Arrow 转换入口；禁止直接依赖
  Pandas 或 Polars 推断落盘类型。
- 每个示例直接用 PyArrow Dataset 读取，并通过相应 Arrow Schema 校验和转换。分钟线示例必须先按
  交易所、品种、年和月执行分区过滤，禁止为了演示而把整张分钟表载入内存。
- Demo 另设独立 raw 示例，按日期读取生意社 `response.html` 与 `response.sha256`、重新计算 SHA-256 并
  校验一致；该示例不得解析 HTML、展示提取字段或把 raw 归档计入 17 张 silver 表。
- silver 表名、字段、分区、Schema 或读取/转换入口发生变化时，必须在同一次变更中更新并验证该 Demo。
