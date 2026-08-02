"""供 Parquet 写入端与 DataFrame 读取端共用的标准数据契约。

相关规范：
- ``AGENTS.md``：项目运行环境、根目录定位与规范路由的项目级强制规则；
- ``02_Quant_Trading/AGENTS.md``：整个量化交易目录树的目录级 Agent 规则入口；
- ``.env.template``：项目根目录定位代码的权威模板；
- ``03_Futures_Database/AGENTS.md``：数据库目录级 Agent 规则及字段与跨引擎类型规范；
- ``02_Quant_Trading/a01_Data_Collection/README.md``：采集链路与表级规范。

修改 Schema、``ARROW_TYPE_MAPPINGS`` 或转换入口时，必须在同一次变更中同步检查并
更新以上规范；文本映射与可执行映射表达的是同一份约束。
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
import polars as pl
import pyarrow as pa


# Parquet/Arrow 是唯一存储契约，Pandas 与 Polars 都必须遵循同一份 Schema。
TRADE_CALENDAR_SCHEMA = pa.schema(
    [
        pa.field("calendar_date", pa.date32(), nullable=False),
        pa.field("date_key", pa.string(), nullable=False),
        pa.field("is_trading_day", pa.bool_(), nullable=False),
        pa.field("weekday", pa.int8(), nullable=False),
        pa.field("is_weekend", pa.bool_(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("calendar_name", pa.string(), nullable=False),
        pa.field("calendar_timezone", pa.string(), nullable=False),
        pa.field("effective_after", pa.time64("us"), nullable=False),
        pa.field("updated_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("year", pa.int16(), nullable=False),
    ]
)

FUTURES_CONTRACT_CALENDAR_SCHEMA = pa.schema(
    [
        pa.field("contract_code", pa.string(), nullable=False),
        pa.field("exchange_code", pa.string(), nullable=False),
        pa.field("underlying_code", pa.string(), nullable=False),
        pa.field("trading_date", pa.date32(), nullable=False),
        pa.field("list_date", pa.date32(), nullable=False),
        pa.field("delist_date", pa.date32(), nullable=False),
        pa.field("contract_multiplier", pa.float64()),
        pa.field("tick_size", pa.float64()),
        pa.field("rule_effective_date", pa.date32(), nullable=False),
        pa.field("rule_expiry_date", pa.date32(), nullable=False),
        pa.field("session_number", pa.int8(), nullable=False),
        pa.field("session_text", pa.string(), nullable=False),
        pa.field("session_start_at", pa.timestamp("us", tz="Asia/Shanghai"), nullable=False),
        pa.field("session_end_at", pa.timestamp("us", tz="Asia/Shanghai"), nullable=False),
        pa.field("is_night_session", pa.bool_(), nullable=False),
        pa.field("spans_midnight", pa.bool_(), nullable=False),
        pa.field("minute_count", pa.int16(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("updated_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("year", pa.int16(), nullable=False),
        pa.field("month", pa.int8(), nullable=False),
    ]
)

FUTURES_SESSION_SCHEDULE_SIGNAL_SCHEMA = pa.schema(
    [
        pa.field("exchange_code", pa.string(), nullable=False),
        pa.field("trading_date", pa.date32(), nullable=False),
        pa.field("session_text", pa.string(), nullable=False),
        pa.field("session_start_at", pa.timestamp("us", tz="Asia/Shanghai"), nullable=False),
        pa.field("session_end_at", pa.timestamp("us", tz="Asia/Shanghai"), nullable=False),
        pa.field("is_night_session", pa.bool_(), nullable=False),
        pa.field("schedule_status", pa.string(), nullable=False),
        pa.field("signal_reason", pa.string(), nullable=False),
        pa.field("evidence_level", pa.string(), nullable=False),
        pa.field("evidence_source", pa.string(), nullable=False),
        pa.field("is_fetch_exempt", pa.bool_(), nullable=False),
        pa.field("updated_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("year", pa.int16(), nullable=False),
        pa.field("month", pa.int8(), nullable=False),
    ]
)

FUTURES_FETCH_STATUS_SCHEMA = pa.schema(
    [
        pa.field("bar_frequency", pa.string(), nullable=False),
        pa.field("contract_code", pa.string(), nullable=False),
        pa.field("exchange_code", pa.string(), nullable=False),
        pa.field("underlying_code", pa.string(), nullable=False),
        pa.field("trading_date", pa.date32(), nullable=False),
        pa.field("session_number", pa.int8(), nullable=False),
        pa.field("session_start_at", pa.timestamp("us", tz="Asia/Shanghai")),
        pa.field("session_end_at", pa.timestamp("us", tz="Asia/Shanghai")),
        pa.field("is_fetch_required", pa.bool_(), nullable=False),
        pa.field("is_fetch_completed", pa.bool_(), nullable=False),
        pa.field("is_data_missing", pa.bool_(), nullable=False),
        pa.field("expected_bar_count", pa.int32(), nullable=False),
        pa.field("actual_bar_count", pa.int32(), nullable=False),
        pa.field("missing_bar_count", pa.int32(), nullable=False),
        pa.field("selection_reason", pa.string(), nullable=False),
        pa.field("fetch_run_id", pa.string()),
        pa.field("fetch_completed_at", pa.timestamp("us", tz="UTC")),
        pa.field("missing_checked_at", pa.timestamp("us", tz="UTC")),
        pa.field("updated_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("year", pa.int16(), nullable=False),
        pa.field("month", pa.int8(), nullable=False),
    ]
)

FUTURES_MISSING_BAR_SCHEMA = pa.schema(
    [
        pa.field("bar_frequency", pa.string(), nullable=False),
        pa.field("contract_code", pa.string(), nullable=False),
        pa.field("exchange_code", pa.string(), nullable=False),
        pa.field("underlying_code", pa.string(), nullable=False),
        pa.field("trading_date", pa.date32(), nullable=False),
        pa.field("session_number", pa.int8(), nullable=False),
        pa.field("expected_bar_at", pa.timestamp("us", tz="Asia/Shanghai"), nullable=False),
        pa.field("detected_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("year", pa.int16(), nullable=False),
        pa.field("month", pa.int8(), nullable=False),
    ]
)

FUTURES_VARIETY_CALENDAR_SCHEMA = pa.schema(
    [
        pa.field("underlying_code", pa.string(), nullable=False),
        pa.field("exchange_code", pa.string(), nullable=False),
        pa.field("trading_date", pa.date32(), nullable=False),
        pa.field("active_contract_count", pa.int16(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("updated_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("year", pa.int16(), nullable=False),
        pa.field("month", pa.int8(), nullable=False),
    ]
)

FUTURES_DAILY_SCHEMA = pa.schema(
    [
        pa.field("contract_code", pa.string(), nullable=False),
        pa.field("exchange_code", pa.string(), nullable=False),
        pa.field("underlying_code", pa.string(), nullable=False),
        pa.field("trading_date", pa.date32(), nullable=False),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("volume", pa.float64()),
        pa.field("money", pa.float64()),
        pa.field("open_interest", pa.float64()),
        pa.field("has_market_data", pa.bool_(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("updated_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("year", pa.int16(), nullable=False),
        pa.field("month", pa.int8(), nullable=False),
    ]
)

FUTURES_MINUTE_SCHEMA = pa.schema(
    [
        pa.field("contract_code", pa.string(), nullable=False),
        pa.field("exchange_code", pa.string(), nullable=False),
        pa.field("underlying_code", pa.string(), nullable=False),
        pa.field("trading_date", pa.date32(), nullable=False),
        pa.field("session_number", pa.int8(), nullable=False),
        pa.field("bar_at", pa.timestamp("us", tz="Asia/Shanghai"), nullable=False),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("volume", pa.float64()),
        pa.field("money", pa.float64()),
        pa.field("open_interest", pa.float64()),
        pa.field("source", pa.string(), nullable=False),
        pa.field("updated_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("year", pa.int16(), nullable=False),
        pa.field("month", pa.int8(), nullable=False),
    ]
)


DATASET_SCHEMAS = {
    "dim_trade_calendar": TRADE_CALENDAR_SCHEMA,
    "dim_futures_variety_calendar": FUTURES_VARIETY_CALENDAR_SCHEMA,
    "dim_futures_contract_calendar": FUTURES_CONTRACT_CALENDAR_SCHEMA,
    "dim_futures_session_schedule_signal": FUTURES_SESSION_SCHEDULE_SIGNAL_SCHEMA,
    "fact_futures_fetch_status": FUTURES_FETCH_STATUS_SCHEMA,
    "fact_futures_missing_bar": FUTURES_MISSING_BAR_SCHEMA,
    "fact_futures_daily": FUTURES_DAILY_SCHEMA,
    "fact_futures_minute": FUTURES_MINUTE_SCHEMA,
}


@dataclass(frozen=True)
class DataFrameTypeMapping:
    """单个 Arrow 类型对应的 Pandas 与 Polars 类型。"""

    pandas_dtype: pd.ArrowDtype
    polars_dtype: pl.DataType | type[pl.DataType]


def _type_mapping(
    arrow_type: pa.DataType,
    polars_dtype: pl.DataType | type[pl.DataType],
) -> DataFrameTypeMapping:
    return DataFrameTypeMapping(pd.ArrowDtype(arrow_type), polars_dtype)


# 该可执行映射必须与 03_Futures_Database/AGENTS.md 的“跨引擎类型契约”表同步更新。
ARROW_TYPE_MAPPINGS: dict[pa.DataType, DataFrameTypeMapping] = {
    pa.date32(): _type_mapping(pa.date32(), pl.Date),
    pa.timestamp("us", tz="UTC"): _type_mapping(
        pa.timestamp("us", tz="UTC"),
        pl.Datetime("us", "UTC"),
    ),
    pa.timestamp("us", tz="Asia/Shanghai"): _type_mapping(
        pa.timestamp("us", tz="Asia/Shanghai"),
        pl.Datetime("us", "Asia/Shanghai"),
    ),
    pa.time64("us"): _type_mapping(pa.time64("us"), pl.Time),
    pa.string(): _type_mapping(pa.string(), pl.String),
    pa.bool_(): _type_mapping(pa.bool_(), pl.Boolean),
    pa.int8(): _type_mapping(pa.int8(), pl.Int8),
    pa.int16(): _type_mapping(pa.int16(), pl.Int16),
    pa.int32(): _type_mapping(pa.int32(), pl.Int32),
    pa.int64(): _type_mapping(pa.int64(), pl.Int64),
    pa.float32(): _type_mapping(pa.float32(), pl.Float32),
    pa.float64(): _type_mapping(pa.float64(), pl.Float64),
}


def resolve_type_mapping(arrow_type: pa.DataType) -> DataFrameTypeMapping:
    """返回 Arrow 类型对应的 Pandas/Polars 类型，未登记类型直接报错。"""
    mapping = ARROW_TYPE_MAPPINGS.get(arrow_type)
    if mapping is not None:
        return mapping
    if pa.types.is_decimal128(arrow_type):
        return _type_mapping(
            arrow_type,
            pl.Decimal(precision=arrow_type.precision, scale=arrow_type.scale),
        )
    raise TypeError(
        f"Arrow 类型 {arrow_type} 尚未登记 Pandas/Polars 映射；"
        "请同步更新 ARROW_TYPE_MAPPINGS 与 03_Futures_Database/AGENTS.md。"
    )


def pandas_dtypes(schema: pa.Schema) -> dict[str, pd.ArrowDtype]:
    """从 Arrow Schema 派生逐字段 Pandas dtype。"""
    return {
        field.name: resolve_type_mapping(field.type).pandas_dtype
        for field in schema
    }


def polars_dtypes(
    schema: pa.Schema,
) -> dict[str, pl.DataType | type[pl.DataType]]:
    """从 Arrow Schema 派生逐字段 Polars dtype。"""
    return {
        field.name: resolve_type_mapping(field.type).polars_dtype
        for field in schema
    }


def _validate_columns(actual_columns: list[str], schema: pa.Schema) -> None:
    """校验列名及其顺序是否与 Schema 完全一致。"""
    expected_columns = schema.names
    if actual_columns != expected_columns:
        raise ValueError(
            "数据列与 Schema 不一致。"
            f"期望 {expected_columns}，实际 {actual_columns}。"
        )


def validate_arrow_table(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """按 Schema 安全转换并校验 Arrow Table。"""
    if not isinstance(table, pa.Table):
        raise TypeError(f"期望 pyarrow.Table，实际为 {type(table).__name__}。")

    pandas_dtypes(schema)
    polars_dtypes(schema)
    _validate_columns(table.column_names, schema)
    typed_table = table.cast(schema, safe=True)
    for field in schema:
        if not field.nullable and typed_table[field.name].null_count:
            raise ValueError(f"非空字段 {field.name!r} 包含空值。")
    return typed_table


def pandas_to_arrow(dataframe: pd.DataFrame, schema: pa.Schema) -> pa.Table:
    """校验 Pandas DataFrame，并转换为符合 Schema 的 Arrow Table。"""
    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError(f"期望 pandas.DataFrame，实际为 {type(dataframe).__name__}。")
    _validate_columns(dataframe.columns.tolist(), schema)
    table = pa.Table.from_pandas(
        dataframe,
        schema=schema,
        preserve_index=False,
        safe=True,
    )
    return validate_arrow_table(table, schema)


def polars_to_arrow(dataframe: pl.DataFrame, schema: pa.Schema) -> pa.Table:
    """校验 Polars DataFrame，并转换为符合 Schema 的 Arrow Table。"""
    if not isinstance(dataframe, pl.DataFrame):
        raise TypeError(f"期望 polars.DataFrame，实际为 {type(dataframe).__name__}。")
    _validate_columns(dataframe.columns, schema)
    return validate_arrow_table(dataframe.to_arrow(), schema)


def arrow_to_pandas(table: pa.Table, schema: pa.Schema) -> pd.DataFrame:
    """将 Arrow Table 转换为使用 Arrow 扩展类型的 Pandas DataFrame。"""
    typed_table = validate_arrow_table(table, schema)
    dataframe = typed_table.to_pandas(types_mapper=pd.ArrowDtype)
    expected_dtypes = pandas_dtypes(schema)
    actual_dtypes = dataframe.dtypes.to_dict()
    if actual_dtypes != expected_dtypes:
        raise TypeError(
            "Pandas 类型与 Arrow 映射不一致。"
            f"期望 {expected_dtypes}，实际 {actual_dtypes}。"
        )
    return dataframe


def arrow_to_polars(table: pa.Table, schema: pa.Schema) -> pl.DataFrame:
    """将 Arrow Table 转换为符合 Schema 的 Polars DataFrame。"""
    typed_table = validate_arrow_table(table, schema)
    dataframe = pl.from_arrow(typed_table, rechunk=True)
    expected_dtypes = polars_dtypes(schema)
    actual_dtypes = dict(dataframe.schema)
    if actual_dtypes != expected_dtypes:
        raise TypeError(
            "Polars 类型与 Arrow 映射不一致。"
            f"期望 {expected_dtypes}，实际 {actual_dtypes}。"
        )
    return dataframe


def empty_pandas(schema: pa.Schema) -> pd.DataFrame:
    """创建具有完整 Arrow 扩展类型的空 Pandas DataFrame。"""
    empty_table = pa.Table.from_batches([], schema=schema)
    return arrow_to_pandas(empty_table, schema)


def empty_polars(schema: pa.Schema) -> pl.DataFrame:
    """创建符合 Schema 的空 Polars DataFrame。"""
    empty_table = pa.Table.from_batches([], schema=schema)
    return arrow_to_polars(empty_table, schema)
