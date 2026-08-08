# 适用范围与规范地位

- 本文件是 `E:\Latitude_Analytics_v2\03_Futures_Database` 整棵目录树的目录级 Agent 规则入口，适用于目录本身以及任意层级的当前和未来子目录。
- 本文件同时是全项目字段命名、跨引擎类型和数据湖 Schema 行为约束的文本权威来源；修改其他目录中的数据生产者、读取者、转换代码或验证脚本时，只要涉及这些事项，也必须先读取并遵循本文件。
- 数据湖各表的可执行 Schema 仍以 `config/data_contracts.py` 为唯一权威来源；本文件负责规定 Agent 的修改流程、命名、类型映射和同步责任，两者不得冲突。
- 下级 `AGENTS.md` 或 README 可以补充更具体的目录规则，但未经用户明确同意，不得豁免、弱化或覆盖本文件的数据契约要求。

# 数据字段、变量命名与类型规范

本项目以 **Arrow/Parquet schema 作为落盘数据的唯一类型契约**。Pandas、Polars、NumPy
只是计算层；写入 Parquet 前必须转换并通过 Arrow schema 校验。

## 0. 规范索引与同步要求

- [根目录 AGENTS.md](../AGENTS.md)：项目运行环境、根目录定位和规范路由的项目级强制规则。
- [02_Quant_Trading/AGENTS.md](../02_Quant_Trading/AGENTS.md)：量化交易整棵目录树的目录级 Agent 规则入口，包含双轨、PythonExporter、`c00` 同步入口及旧项目只读规定。
- [.env.template](../.env.template)：项目根目录定位代码的权威模板。
- [数据采集链路 README](../02_Quant_Trading/a01_Data_Collection/README.md)：双轨同步入口、表粒度、主键、分区和更新水位规范。
- [特征工程 README](../02_Quant_Trading/a02_Feature_Engineering/README.md)：主力连续合约、log 双向复权、期限结构边界及 gold 派生表规范。
- [config/data_contracts.py](../config/data_contracts.py)：数据湖 Schema 及 Pandas、Polars、Arrow 转换的可执行契约。
- [read_futures_lake_demo.ipynb](read_futures_lake_demo.ipynb)：八张 silver 表与两张 gold 派生表的契约化读取演示。
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

Pandas 与 Polars 不得分别维护独立类型定义。读取和转换必须显式传入
`config/data_contracts.py` 中的同一份 Arrow Schema：

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

两个读取入口均位于 `02_Quant_Trading/a01_Data_Collection/c00_lakehouse.py`，并在返回
DataFrame 前执行 Arrow Schema 校验。禁止直接读取后依赖 Pandas 或 Polars 推断类型。

## 3. 当前数据湖 Schema 与转换入口

所有表的权威定义均位于 `config/data_contracts.py`：

| 数据集 | Arrow Schema |
|---|---|
| `dim_trade_calendar` | `TRADE_CALENDAR_SCHEMA` |
| `dim_futures_variety_calendar` | `FUTURES_VARIETY_CALENDAR_SCHEMA` |
| `dim_futures_contract_calendar` | `FUTURES_CONTRACT_CALENDAR_SCHEMA` |
| `dim_futures_session_schedule_signal` | `FUTURES_SESSION_SCHEDULE_SIGNAL_SCHEMA` |
| `fact_futures_fetch_status` | `FUTURES_FETCH_STATUS_SCHEMA` |
| `fact_futures_missing_bar` | `FUTURES_MISSING_BAR_SCHEMA` |
| `fact_futures_daily` | `FUTURES_DAILY_SCHEMA` |
| `fact_futures_minute` | `FUTURES_MINUTE_SCHEMA` |
| `fact_futures_main_contract_daily` | `FUTURES_MAIN_CONTRACT_DAILY_SCHEMA` |
| `fact_futures_main_continuous_daily` | `FUTURES_MAIN_CONTINUOUS_DAILY_SCHEMA` |

统一转换入口：

| 方向 | 函数 |
|---|---|
| 数据集名称 → Arrow Schema | `DATASET_SCHEMAS` |
| Arrow 类型 → Pandas/Polars 类型 | `ARROW_TYPE_MAPPINGS` / `resolve_type_mapping()` |
| Arrow Schema → Pandas 逐字段类型 | `pandas_dtypes()` |
| Arrow Schema → Polars 逐字段类型 | `polars_dtypes()` |
| Arrow 校验/安全转换 | `validate_arrow_table()` |
| Pandas → Arrow | `pandas_to_arrow()` |
| Polars → Arrow | `polars_to_arrow()` |
| Arrow → Pandas | `arrow_to_pandas()` |
| Arrow → Polars | `arrow_to_polars()` |
| 带契约的空 Pandas/Polars 表 | `empty_pandas()` / `empty_polars()` |

任何生产写入都必须通过相应 Schema；不得直接调用 `DataFrame.to_parquet()`，也不得
在其他模块重复定义同一张表的 Schema。字段新增、删除或类型变更必须先修改可执行契约，
再同步迁移生产者、读取者、验证脚本、已有 Parquet 数据以及本规范。

`fact_futures_fetch_status` 与 `fact_futures_missing_bar` 必须把 `bar_frequency` 作为第一层
Hive 分区；后续分区字段分别遵循采集 README 中的表级契约。`replace_dataset()` 在最终写入前
再次执行 Schema 校验；分钟线分区增量写入也必须先通过
`pandas_to_arrow()` 或 `polars_to_arrow()`，禁止由某批 DataFrame 的推断类型暗中改变数据库类型。

`dim_futures_session_schedule_signal` 中 `evidence_level='inferred'` 的记录只表达日历信号，
其 `is_fetch_exempt` 必须为假；只有 `evidence_level='authoritative'` 的记录才允许设置
`is_fetch_exempt=True` 并影响拉取要求。不得把工作日休市间隔的推断直接等同于 Session 已确认关闭。

两张主力连续合约派生表位于 `futures_lake/gold`，均按
`exchange_code / underlying_code / year / month` 分区。其主力选择、换月锚点、log 前后复权和
期限结构不复权边界以 `02_Quant_Trading/a02_Feature_Engineering/README.md` 为准。gold 派生表与
silver 输入表一样，生产写入必须先通过 `config/data_contracts.py` 的对应 Arrow Schema。

## 4. 数据湖读取 Demo

- `read_futures_lake_demo.ipynb` 是当前十张数据湖表的标准读取演示，必须覆盖
  `dim_trade_calendar`、`dim_futures_variety_calendar`、
  `dim_futures_contract_calendar`、`dim_futures_session_schedule_signal`、
  `fact_futures_fetch_status`、
  `fact_futures_missing_bar`、`fact_futures_daily`、`fact_futures_minute`、
  `fact_futures_main_contract_daily` 和 `fact_futures_main_continuous_daily`。
- 每张表必须由独立代码单元格演示；单个演示单元格不得同时读取多张表。公共导入、项目根目录定位、
  分区定义和展示函数可以放在单独的初始化单元格。
- Demo 必须使用项目规定的根目录标记文件搜索方式、`latitude` 环境、
  `config/data_contracts.py` 中的权威 Schema 及统一 Arrow 转换入口；禁止直接依赖
  Pandas 或 Polars 推断落盘类型。
- 维表和日线示例使用 `c00_lakehouse.py` 的统一读取入口。分钟线示例必须先按交易所、品种、
  年和月执行分区过滤，再通过相应 Arrow Schema 校验并转换，禁止为了演示而把整张分钟表载入内存。
- gold 示例必须从 `futures_lake/gold` 读取，并先按交易所、品种、年和月过滤；不得把 gold 路径
  混写为 silver 表路径。
- 表名、字段、分区、Schema 或读取/转换入口发生变化时，必须在同一次变更中更新并验证该 Demo。
