# 数据字段、变量命名与类型规范

本项目以 **Arrow/Parquet schema 作为落盘数据的唯一类型契约**。Pandas、Polars、NumPy
只是计算层；写入 Parquet 前必须转换并通过 Arrow schema 校验。

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

| 业务含义 | Parquet / Arrow（落盘） | Pandas | Polars | NumPy |
|---|---|---|---|---|
| 自然日 | `date32` | `date32[day][pyarrow]` | `Date` | `datetime64[D]` |
| UTC 时间戳 | `timestamp[us, UTC]` | `timestamp[us, tz=UTC][pyarrow]` | `Datetime(us, UTC)` | `datetime64[us]`（NumPy 丢失时区，值必须先转 UTC） |
| 本地时间戳 | `timestamp[us, Asia/Shanghai]` | 对应 Arrow dtype | `Datetime(us, Asia/Shanghai)` | 不允许直接表示；先转 UTC |
| 一天内时间 | `time64[us]` | `time64[us][pyarrow]` | `Time` | `timedelta64[us]`（自午夜起微秒数） |
| 标识符、代码 | `string` | `string[pyarrow]` | `String` | `str_` / Unicode；禁止定长字节串 `S` |
| 布尔值 | `bool` | `bool[pyarrow]` | `Boolean` | `bool_` |
| 小整数/枚举 | `int8` / `int16` | 对应 Arrow dtype | `Int8` / `Int16` | `int8` / `int16` |
| 普通整数 | `int32` / `int64` | 对应 Arrow dtype | `Int32` / `Int64` | `int32` / `int64` |
| 价格、金额 | `decimal128(p,s)` | Arrow decimal dtype | `Decimal(p,s)` | 无等价类型；不得用 NumPy 做最终金额存储 |
| 连续指标/收益率 | `float64` | `float64[pyarrow]` | `Float64` | `float64` |

补充规则：

- 证券代码、合约代码、交易所代码、日期键即使只包含数字，也一律存 `string`，避免丢失前导零。
- `float32` 仅用于明确允许降低精度的模型特征；行情和统计计算默认 `float64`。
- 金额/结算价若需要十进制定点精度，使用 `decimal128`，禁止依赖二进制浮点精确相等。
- 所有时间戳必须声明时区。跨市场事件时间统一存 UTC；交易所本地规则可存明确的 IANA 时区。
- 缺失值落盘统一为 Arrow null。可空整数/布尔值不得通过 `float64 + NaN` 伪装。
- NumPy 不作为数据库交换格式。含 null、时区或 decimal 的数据应保留在 Arrow/Pandas/Polars 中。

Pandas 读取时使用 Arrow dtype：

```python
calendar_df = pd.read_parquet(table_path, dtype_backend="pyarrow")
```

Polars 会按 Parquet schema 原生读取：

```python
calendar_pl_df = pl.read_parquet(table_path)
```

## 3. 当前交易日历 schema

权威定义位于 `config/data_contracts.py` 的 `TRADE_CALENDAR_SCHEMA`。任何生产写入都必须通过
该 schema；字段新增或类型变更必须先修改契约，再迁移已有 Parquet，禁止由某次 DataFrame
的推断类型暗中改变数据库类型。
