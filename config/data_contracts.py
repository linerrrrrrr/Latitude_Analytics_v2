"""Canonical schemas shared by Parquet writers and dataframe readers."""

from __future__ import annotations

import pyarrow as pa


# Parquet/Arrow is the storage contract. Pandas and Polars must conform to it.
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


def pandas_to_arrow(dataframe, schema: pa.Schema) -> pa.Table:
    """Validate column names, order and values, then create a typed Arrow table."""
    expected_columns = schema.names
    actual_columns = dataframe.columns.tolist()
    if actual_columns != expected_columns:
        raise ValueError(
            f"Schema columns do not match. Expected {expected_columns}, got {actual_columns}."
        )
    return pa.Table.from_pandas(dataframe, schema=schema, preserve_index=False, safe=True)
