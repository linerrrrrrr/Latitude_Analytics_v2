# 适用范围与规范地位

- 本文件是 `E:\Latitude_Analytics_v2\03_Futures_Database` 整棵目录树的目录级 Agent 规则入口，适用于目录本身以及任意层级的当前和未来子目录。
- 本文件同时是全项目字段命名、跨引擎类型和数据湖 Schema 行为约束的文本权威来源；修改其他目录中的数据生产者、读取者、转换代码或验证脚本时，只要涉及这些事项，也必须先读取并遵循本文件。
- 17 张稳定 silver 表（7 张日历维度表、10 张事实表）的可执行 Schema 以 `config/data_contracts.py` 为唯一权威来源；本文件负责规定 Agent 的修改流程、命名、类型映射和同步责任，两者不得冲突。来源原文归档属于 raw 层，不计入 silver 表数；实验性 gold 输出由所属下游工作流局部定义，不进入数据库级表清单或统一 Schema。
- 下级 `AGENTS.md` 或 README 可以补充更具体的目录规则，但未经用户明确同意，不得豁免、弱化或覆盖本文件的数据契约要求。

# 数据字段、变量命名与类型规范

本项目以 **Arrow/Parquet schema 作为落盘数据的唯一类型契约**。Pandas、Polars、NumPy
只是计算层；写入 Parquet 前必须转换并通过 Arrow schema 校验。

## 0. 规范索引与同步要求

- [根目录 AGENTS.md](../AGENTS.md)：项目运行环境、根目录定位和规范路由的项目级强制规则。
- [02_Quant_Trading/AGENTS.md](../02_Quant_Trading/AGENTS.md)：量化交易整棵目录树的目录级 Agent 规则入口，包含双轨、PythonExporter、`b00` 同步入口及根级旧项目归档路由。
- [.env.template](../.env.template)：项目根目录定位代码、当前稳定采集统一正式起点，以及包含 `raw`、`silver`、`gold` 的正式湖仓根路径环境变量权威模板。
- [数据采集链路 README](../02_Quant_Trading/a01_Data_Collection/README.md)：双轨同步入口、Notebook 语义浏览、表粒度、主键、分区和更新水位规范。
- [数据采集系统重建蓝图](../02_Quant_Trading/a01_Data_Collection_Rebuild_Blueprint/README.md)：冻结的一次性历史实施记录；其中与当前 17 表契约或生意社 raw 原文归档政策不一致的旧设计由本规范和当前可执行契约覆盖。
- [数据采集系统重建执行清单](../02_Quant_Trading/a01_Data_Collection_Rebuild_Blueprint/08_EXECUTION_CHECKLIST.md)：本次一次性重建的强制代码完成门禁和蓝图退出条件；门禁通过前禁止编写或执行数据迁移。
- [旧项目归档规则](../04_Old_Projects/AGENTS.md)：重建前采集实现、更早历史采集项目和旧特征工程项目的只读保护。
- [特征工程 README](../02_Quant_Trading/a02_Feature_Engineering/README.md)：主力连续合约、log 双向复权、期限结构边界及当前 gold 实验工作流说明。
- [国内期货事实采集白名单](../config/futures_fact_collection_policy.py)：日线、分钟线和逐品种交易所报告共用的交易所—品种白名单唯一权威来源。
- [外部市场请求实体配置](../config/external_market_entities.py)：外部市场日历与外部指数事实共用的请求实体、Eastmoney 指标映射及有效期唯一权威来源。
- [宏观发布系列与可用日配置](../config/macro_release_entities.py)：宏观发布日历、SHIBOR 与宏观事实共用的 25 个系列、来源列、宏观数值偏移、理论频率及版本化可用日规则唯一权威来源。
- [JQData 共享连接边界](../config/jqdata_connection.py)：JQData 认证与 Windows TUN 物理出口绑定的项目级实现；不承载业务采集或写入语义。
- [config/data_contracts.py](../config/data_contracts.py)：17 张稳定 silver Schema 及 Pandas、Polars、Arrow 转换的可执行契约。
- [read_futures_lake_demo.ipynb](read_futures_lake_demo.ipynb)：17 张稳定 silver 表的契约化读取演示，以及不计入 silver 表数的生意社 raw 原文与摘要核对示例。
- 修改本规范时，必须同步检查以上索引项；字段、类型、Schema 或转换入口发生变化时，相关文本和代码必须在同一次变更中更新。

## 1. 通用命名

- 一律使用英文 `snake_case`，字段名和变量名不使用缩写拼音。
- 单数表示单个对象，复数表示集合，例如 `trade_date`、`trading_dates`。
- 字段名表达业务含义；变量名可增加对象类型后缀。
- 禁止用 `data`、`value`、`temp`、`result` 表示长期存在的变量。

### Python 变量后缀

| 对象 | 命名格式 | 示例 |
|---|---|---|
| `datetime.date` | `*_date` | `start_date`, `calendar_date` |
| `datetime.datetime` | `*_datetime` 或业务明确的 `*_at` | `as_of_datetime`, `updated_at` |
| `datetime.time` | `*_time` | `cutoff_time` |
| `datetime.timedelta` | `*_duration` | `lookback_duration` |
| 字符串 | `*_text`；业务代码/名称可直接按语义命名 | `raw_date_text`, `symbol`, `exchange_code` |
| `Path` | `*_path`；目录使用 `*_dir` / `*_root` | `table_path`, `lake_root` |
| Pandas DataFrame | `*_df` | `calendar_df` |
| Polars DataFrame/LazyFrame | `*_pl_df` / `*_lazy_df` | `calendar_pl_df` |
| NumPy ndarray | `*_array` | `returns_array` |
| Arrow Table | `*_table` | `calendar_table` |
| 布尔值 | `is_*`, `has_*`, `can_*`, `should_*` | `is_trading_day` |
| 数量 | `*_count` | `row_count` |

`date`、`datetime`、`time` 不可混用：`trade_date` 永远不包含时分秒；`event_datetime`
必须包含日期和时刻；表示事件发生/更新时间的数据库字段优先使用 `*_at`。

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
`config/data_contracts.py` 或 silver 读取 Demo 中。旧正式湖若仍有该目录，当前 c02、契约同步和读取 Demo
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
`schema_version` 使用语义化版本，并在字段、类型、nullable 或业务语义发生变化时同步调整。

`role_zh` 只能按真实职责填写 `日历维度表` 或 `事实表`，不得恢复含混的“状态表”；日历表的
`calendar_role_zh` 说明其枚举的预期格点，事实表说明预期格点由哪个直接上游日历规定。
`dimension_dependencies` 只列直接依赖的 `dim_*` 表，不把 API、库、配置、事实表或质检回写列为维度。
`update_mode_zh` 必须服从“上游当前有效格点 − 下游已经完整落盘的格点”，并说明空湖如何由同一规则自然
得到全量；不得把人工日期范围写成正式生产更新方式。`quality_rules_zh` 必须覆盖表级主键、完整性、范围、
对账和正式路径复读边界。

不可空字段的 `nullable_reason_zh` 写明不可为空；可空字段必须写出允许为空的具体业务条件。
枚举字段的 `enum_values_zh` 列出允许值及含义，非枚举字段明确标为非枚举；字段级
`quality_rules_zh` 写明适用的非空、唯一、范围、时区、精度或跨列关系。

来源 metadata 必须记录实际来源。API 原始字段写明真实 API 与原列名；DataFrame 索引字段明确标为索引；
派生字段使用实际输入列并说明转换，不得伪造 API 原列；常量、运行状态和审计字段标为系统生成；多源证据字段
列全直接证据来源。表级 `field_list` 只用于快速阅读和顺序校验，不能替代逐字段 metadata。

稳定 silver Schema 变更必须在同一次变更中同步生产者、消费者、读取 Demo、验证、现有 Parquet 数据及本
规范。验收至少覆盖字段名、顺序、Arrow 类型、nullable、全部强制 metadata 键、UTF-8 解码、
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

### 3.3 Notebook Schema 语义浏览

Notebook 语义浏览界面是权威 Schema 的只读投影，不是第二份语义来源。所有业务 Notebook 使用
`config/notebook_schema_browser.py` 中的同一展示实现，只传入当前工作流直接涉及的具名权威 Schema，
并按上游依赖在前、当前产出在后的顺序排列。

界面固定按以下顺序展示：Schema 列表、Schema 下拉框、表级 metadata 表、字段目录表、Field 下拉框、
单字段完整 metadata 表。Schema 下拉框必须紧邻表级 metadata 表，Field 下拉框必须紧邻单字段完整
metadata 表；切换 Schema 时必须同步刷新字段目录和字段选项。

字段目录表只展示英文字段名、中文字段名、Arrow 类型、结构角色、语义角色和来源系统。结构角色根据当前
Schema 表级 `primary_key` 和 `partition_columns` metadata 计算，不建立另一份定义；其余字段级 metadata
保留在单字段完整 metadata 表中。展示代码不得硬编码表名、主键、分区、字段说明或 Schema 版本，不得访问
API、读取数据湖、产生写入副作用，也不得代替 Schema/metadata 完整性和 Parquet round-trip 校验。展示
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
Hive 分区；其他表严格使用各 Schema 的 `partition_columns` metadata 顺序。所有 staging 和正式路径
复读都必须再次执行 Schema/metadata 校验；分钟线分区增量写入也必须先通过
`pandas_to_arrow()` 或 `polars_to_arrow()`，禁止由某批 DataFrame 的推断类型暗中改变数据库类型。

`dim_futures_bar_calendar` 中 `evidence_level='reconciled'` 的记录只表达日线—分钟重聚合旁证；
只有 `evidence_level='authoritative'` 才允许 `schedule_status='confirmed_closed'` 并免除拉取。
不得把工作日休市间隔或行情一致性推断直接等同于 Session 已确认关闭。

`dim_futures_variety_calendar` 和 `dim_futures_contract_calendar` 必须来自
`get_all_securities(["futures"], date=None)` 的完整固定月份合约目录，事实采集白名单不得裁剪维度行。
`dim_futures_bar_calendar` 与 `dim_futures_exchange_report_calendar` 同样保留完整理论格点。`c04` 不读取或
解释事实采集白名单，其内容只由 `config/futures_fact_collection_policy.py` 定义。`c05` 和 `c06` 分别在
完整 `1d`、`1m` 理论格点上应用同一政策，通过 `is_fetch_required`、`selection_reason` 回写选择结果，且
不得删除理论格点。白名单缩减只停止后续事实采集，不得由采集器自动删除已经正式落盘的历史事实。
逐品种交易所报告的同一白名单与覆盖规则由报告日历生产者应用；具体报告还必须同时满足 API 覆盖期及
交易所—品种支持范围。

`dim_external_market_calendar` 必须保留上游有效自然日与当前请求实体配置形成的完整理论格点：国内现货和
按日返回整表的 JQData `FUT_GLOBAL_DAILY` 使用 `entity_code=ALL`，Eastmoney 外部指数使用来源
`INDICATOR_ID`。工作日、交易日和配置有效期只通过 `is_fetch_required` 与中文原因表达，不得删除无需请求日；
请求实体、指数项目代码、中文名、分类和有效期只由 `config/external_market_entities.py` 定义。

`domestic_spot_basis/ALL` 的下游完成证据是正式 raw 原文字节、匹配的 SHA-256 sidecar 与外部市场日历状态
三者共同完整，不再依赖任何结构化事实表。生产待办集合固定为
`required 日期 −（原文字节与 sidecar 共同完整且日历状态完整的日期）`。原文归档完整但日历状态缺失或陈旧
时，c02 必须从正式 raw 复读证据并无 API 修复日历；原文缺失或摘要不一致时才重新请求。HTTP 200 的任意
`response.content` 在正式归档和摘要核对成功后，日历统一写为 `fetch_status=success`、`record_count=1`、
`quality_status=passed`；正文为空、HTML 结构变化和业务内容均不得改变该结论。网络错误或非 200 响应仍不得
伪装成成功归档。页面结构监测、历史重采、解析、字段提取和结构化事实生产均属于未来另行确认的独立项目。

`fact_external_index_daily` 除正向 API 待办外，还必须计算“正式事实格点 − 当前 `external_index`
required 格点”的无 API 清退范围。清退格点按旧事实自身的 `index_category/year/month` 定位原完整叶分区，
保留分区内其他事实；staging 与正式路径均逐值复读，正式计数确认为 0 后才算清退完成。此规则不覆盖上文
“期货事实白名单缩减只停止后续采集、不自动删除历史事实”的专门政策。

`dim_macro_release_calendar` 没有直接上游维度表，也不得为了生成理论格点调用 Tushare 或 Eastmoney。
它必须从 `FUTURES_DATA_START_DATE` 到北京时间当前日，按 `config/macro_release_entities.py` 的 25 个
稳定系列、理论频率和版本化可用日规则生成完整水位：SHIBOR 只生成普通工作日，CPI/PPI/PMI 生成月末，
GDP 生成季末；是否 required 只由项目可用日是否已到决定。政策字段逐值不变时继承 c02/c03 回写状态，
新增、内部缺口、系列撤销、可用日到达和规则变化都由完整表比较识别。普通写入替换完整
`dataset_name/year/month` 叶分区，旧 metadata 或旧策略只能在无日期自动模式中整根迁移；staging 与
正式路径均须通过精确 Schema/metadata、完整叶和完整表逐值复读。该入口本身不得把项目规则可用日描述成
API 实际发布日期，也不得擅自删除两张下游事实表的历史行。

`fact_interest_rate_daily` 只消费上述日历中 `dataset_name=interest_rate` 且 required 的系列—日期格点。
完成集合必须由正式事实和日历状态共同证明；正式事实完整但日历状态陈旧的格点从正式事实无 API 修复，
事实缺口才按 `year/month` 月度窗口调用 Tushare `pro.shibor`。8 个系列及
`date,on,1w,2w,1m,3m,6m,9m,1y` 来源映射只从 `config/macro_release_entities.py` 读取；范围响应中只接纳
精确待办期限，不得覆盖已经完整的同月格点。请求日期必须唯一且位于范围内，单次不得超过官方 2000 行；
非空利率必须为有限数、保留百分比年利率原单位并位于 `[-100, 100]` 硬边界。完整响应中某精确期限没有
有效值时（Tushare DataFrame 中的 `None`、`pd.NA` 或 `NaN`），事实正式复读 0 行后才可记
`empty_confirmed + warning`；缺列、重复、越界、布尔值、无穷值、其他非数值或越界利率必须失败。每个
`year/month` 完整事实叶和对应 `interest_rate/year/month` 完整日历叶均须在
staging 与正式路径通过精确 Schema/metadata 和逐值复读，事实正式复读成功后才能推进日历完成状态。

`fact_macro_release` 只消费上述日历中 `dataset_name=macro_release` 且 required 的系列—报告期格点。
完成集合必须由正式事实和日历状态共同证明；正式事实完整但日历状态陈旧时从正式事实无 API 修复，
事实缺口才按 Eastmoney 报告名与 `year/month` 窗口分页请求。17 个系列的报告名、原列、单位和数值偏移
只从 `config/macro_release_entities.py` 读取；PPI 同比必须使用 `BASE_SAME`，CPI 全国/城市/农村累计与
PPI 累计来源是以 100 为基准的累计指数，必须按共享 `source_value_offset=-100.0` 转成累计同比百分比。
Eastmoney `REPORT_DATE` 使用报告月 1 日编码：CPI/PPI/PMI 归一到该月最后一个自然日，GDP 只允许
3/6/9/12 月并归一到季末；它不是发布日期。每个报告请求必须冻结首页 `pages/count`，核对后续页元数据、
页长、累计行数、日期范围和跨页来源日期唯一性，只接纳精确待办格点。当前生产事实值不得为空、为布尔值
或非有限数；完整分页中某个精确系列缺值时，事实正式复读 0 行后才可写
`empty_confirmed + warning`。每个 `year/month` 完整事实叶和对应 `macro_release/year/month` 日历叶均须
在 staging 与正式路径通过精确 Schema/metadata 和逐值复读，事实正式复读成功后才能推进日历完成状态。

### 正式湖根路径、raw 归档与 silver 写入水位

- `.env` 的 `FUTURES_LAKE_ROOT` 定义唯一正式湖根目录；来源原文归档、稳定表和实验性输出分别位于 `settings.futures_lake_root / "raw"`、`settings.futures_lake_root / "silver"`、`settings.futures_lake_root / "gold"`。生产者、消费者和读取示例不得各自硬编码 `03_Futures_Database/futures_lake`。raw 的专门归档契约按本文件执行；这一目录约定不定义 gold 的表集合或 Schema。
- 每张 silver 表的生产待更新集合必须从契约化格点求差：`上游当前有效格点 − 下游已经完整落盘的格点`。目标表自身的完整性由其生产者通过 Schema/metadata、主键和表级质量校验证明，不能把“日期不晚于最大日期”直接等同于完整。
- 下游消费者可以信任已由生产者正式提交的上游表，不重复执行上游的完整表级业务质检。消费者仍必须精确检查上游 Schema/metadata，并验证自身计算直接依赖的主键唯一性、请求范围、日期覆盖或其他边界条件；消费者对自己的输出继续承担完整契约与质量校验。
- 正式 silver 写入命令不得由操作者指定日期区间。显式日期只允许只读检查，或写入解析后与 `settings.futures_lake_root` 不同的临时/测试湖。
- 空正式表和小规模缺口使用同一规则：空表的已完整格点集合为空，差集自然等于上游全量。下游表必须先读取已由上游生产者正式提交的水位，不能生成或提交超过上游的格点。
- `fact_futures_daily`、`fact_futures_minute` 与 `fact_overseas_futures_daily` 的来源 OHLC 非空值必须有限；有限数之间的 high/low 跨列关系异常属于必须原值保留的来源质量证据，不得修写或丢弃。生产者在事实 staging 与正式路径复读成功后，仍须把对应 1d 格点、1m Session 或外部市场请求日期标为已完成并记录 `warning`；该异常不改变正式事实计数，也不得被缺失审计覆盖为 `passed`。NaN/Inf、负数量及各表其他硬门禁不因本规则放宽。
- 当前代码已完成 `b01` 的 `c01` 至 `c08`、`b02/c01_exchange_report_calendar` 至 `b02/c03_warehouse_receipt`、`b03/c01_external_market_calendar` 至 `b03/c04_external_index`，以及 `b04/c01_macro_release_calendar` 至 `b04/c03_macro_release` 的迁移；b01/c08 全量重建缺失明细并与触达的行情日历叶分区协调提交、共同回滚，b02/c01 从完整品种日历自动维护三类报告格点、保留未变化的事实采集状态并按完整叶分区提交，b02/c02 从 JQData `finance.FUT_MEMBER_POSITION_RANK` 的一次格点查询同时生成排名与明确参与者类型汇总两张事实，b02/c03 从 JQData `finance.FUT_WAREHOUSE_RECEIPT` 保存逐仓库数量、来源单位和较昨日变化；两类事实入口都在事实正式复读后回写相应日历状态。b03/c01 从完整自然日历与版本化请求实体配置自动维护三类外部市场完整理论格点、继承未变化的下游状态并按完整叶分区提交；b03/c02 只把生意社 `response.content` 与 SHA-256 sidecar 归档到正式 raw，以原文与日历共同完整性自动补缺，并对原文完整但日历陈旧的日期无 API 修复状态；b03/c03 对 `overseas_futures/ALL` required 日期先分流，只有正式事实缺失或不能证明完整的日期才逐日查询 JQData `FUT_GLOBAL_DAILY` 全表；正式事实已经完整但日历状态或原因陈旧的日期必须从正式事实复算行数与 OHLC 质量结论、提交并正式复读日历，不得重拉 API。来源记录 ID、请求日、返回上限和非有限值继续严格校验，有限 OHLC 跨列关系异常原值保留并以 `warning` 留痕；b03/c04 以 `external_index/INDICATOR_ID` required 格点减去正式事实与日历状态共同证明完整的格点，按共享配置逐指标将连续待办段分页请求 Eastmoney `RPT_INDUSTRY_INDEX`，只接纳精确待办日期，严格校验分页计数、来源映射、日期和有限值，并把不再属于当前 required 水位的旧事实无 API 清退；完整指数分类—年月叶分区经 staging 与正式路径逐值复读后才回写日历。b04/c01 从环境统一起点与当前日自动生成 25 个版本化理论系列格点，继承未变化事实状态，并以完整宏观日历叶分区提交和正式复读；b04/c02 自动求差 SHIBOR required 格点，对完整事实执行无 API 日历修复，对事实缺口按月调用 Tushare，只接纳精确待办期限并在完整事实叶复读后回写日历；b04/c03 自动求差宏观 required 格点或从正式事实无 API 修复日历，对事实缺口按 Eastmoney 报告名与年月严格分页，将来源月初编码归一到项目月末/季末，并按共享配置修正 PPI 同比原列和 CPI/PPI 累计同比偏移，完整事实叶复读后才回写日历。至此 18 个正式采集入口均已完成迁移。

gold 用于下游数据组织、特征探索和复权方法探索，不由本数据库规范统一定义表名、字段、Schema、
分区或更新方式。某个实验需要落盘时，由所属工作流局部定义并校验其当前输出结构；不得把该实验
快照加入 `config/data_contracts.py`、本节正式表清单或标准 silver 读取 Demo。

## 4. 数据湖读取 Demo

### 重建前历史来源标签

`fact_futures_daily` 的当前生产来源标签为 `JQData_get_price_daily_get_extras_raw`，历史迁移还必须接受 `JQData_get_price_1d_skip_paused`；历史行缺少当前生产新增的昨收、结算及变化来源字段时全部保留为 null，不得由相邻行推算。`fact_futures_minute` 的当前生产来源标签为 `JQData_get_price_1m_skip_paused_fq_none`，历史迁移还必须接受 `JQData_get_price_1m` 与 `JQData_get_price_1m_skip_paused`。来源标签是历史事实证据，迁移不得伪造为当前标签。对应一次性迁移工具和隔离测试位于根级 `00_draft_collection_02/scripts` 与 `00_draft_collection_02/tests`；`--execute` 必须先通过预检并经人工复核，旧湖与事务备份只可在全量正式复读通过后清理。

- `read_futures_lake_demo.ipynb` 是 17 张稳定 silver 表的标准读取演示；必须逐一使用本节列出的
  权威 Schema 常量，且不得恢复已被 `dim_futures_bar_calendar` 替代的旧状态表。
- 每张表必须由独立代码单元格演示；单个演示单元格不得同时读取多张表。公共导入、项目根目录定位、
  分区定义和展示函数可以放在单独的初始化单元格。
- Demo 必须使用项目规定的根目录标记文件搜索方式、`latitude` 环境、
  `config/data_contracts.py` 中的权威 Schema 及统一 Arrow 转换入口；禁止直接依赖
  Pandas 或 Polars 推断落盘类型。
- 每个示例直接用 PyArrow Dataset 读取，并通过相应 Arrow Schema 校验和转换。分钟线示例必须先按
  交易所、品种、年和月执行分区过滤，禁止为了演示而把整张分钟表载入内存。
- Demo 另设独立 raw 示例，按日期读取生意社 `response.html` 与 `response.sha256`、重新计算 SHA-256 并
  校验一致；该示例不得解析 HTML、展示提取字段或把 raw 归档计入 17 张 silver 表。
- silver 表名、字段、分区、Schema 或读取/转换入口发生变化时，必须在同一次变更中更新并验证该 Demo。
