"""Create or incrementally update domestic futures daily bars."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import click
import pandas as pd
import pyarrow as pa

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from config.data_contracts import FUTURES_DAILY_SCHEMA, pandas_to_arrow  # noqa: E402
from config.settings import settings  # noqa: E402
from lakehouse import hive_partitioning, read_dataset, replace_dataset  # noqa: E402

LAKE_ROOT = PROJECT_ROOT / "03_Futures_Database" / "futures_lake" / "silver"
CONTRACT_CALENDAR_PATH = LAKE_ROOT / "dim_futures_contract_calendar"
TABLE_PATH = LAKE_ROOT / "fact_futures_daily"
PARTITIONING = hive_partitioning(
    [pa.field("exchange_code", pa.string()), pa.field("year", pa.int16()), pa.field("month", pa.int8())]
)
PRICE_FIELDS = ["open", "high", "low", "close", "volume", "money", "open_interest"]


def contract_day_keys(contract_calendar_path: Path) -> pd.DataFrame:
    contract_df = read_dataset(
        contract_calendar_path,
        PARTITIONING,
        ["contract_code", "exchange_code", "underlying_code", "trading_date"],
    )
    return contract_df.drop_duplicates(
        ["contract_code", "trading_date"]
    ).sort_values(["trading_date", "contract_code"]).reset_index(drop=True)


def fetch_daily(keys_df: pd.DataFrame, batch_size: int = 50) -> pd.DataFrame:
    import jqdatasdk

    frames = []
    contract_codes = sorted(keys_df["contract_code"].unique())
    start_date = keys_df["trading_date"].min()
    end_date = keys_df["trading_date"].max()
    for offset in range(0, len(contract_codes), batch_size):
        batch = contract_codes[offset : offset + batch_size]
        raw_df = jqdatasdk.get_price(
            batch,
            start_date=start_date,
            end_date=end_date,
            frequency="daily",
            fields=PRICE_FIELDS,
            skip_paused=False,
            fq=None,
            panel=False,
        )
        if raw_df.empty:
            continue
        if "time" not in raw_df.columns:
            raw_df = raw_df.rename_axis("time").reset_index()
        if "code" not in raw_df.columns:
            raw_df["code"] = batch[0]
        raw_df = raw_df.rename(columns={"code": "contract_code"})
        raw_df["trading_date"] = pd.to_datetime(raw_df["time"]).dt.date
        frames.append(raw_df[["contract_code", "trading_date", *PRICE_FIELDS]])
    if not frames:
        return pd.DataFrame(columns=["contract_code", "trading_date", *PRICE_FIELDS])
    return pd.concat(frames, ignore_index=True).drop_duplicates(
        ["contract_code", "trading_date"], keep="last"
    )


def update_futures_daily(
    contract_calendar_path: Path = CONTRACT_CALENDAR_PATH,
    table_path: Path = TABLE_PATH,
    full_refresh: bool = False,
) -> pd.DataFrame:
    keys_df = contract_day_keys(contract_calendar_path)
    existing_df = pd.DataFrame() if full_refresh else read_dataset(table_path, PARTITIONING)
    last_date = existing_df["trading_date"].max() if not existing_df.empty else None
    pending_keys_df = keys_df[
        keys_df["trading_date"].map(lambda value: last_date is None or value > last_date)
    ]
    if pending_keys_df.empty:
        return existing_df

    import jqdatasdk

    jqdatasdk.auth(settings.jqdata_id, settings.jqdata_secret)
    price_df = fetch_daily(pending_keys_df)
    daily_df = pending_keys_df.merge(
        price_df, on=["contract_code", "trading_date"], how="left", validate="one_to_one"
    )
    daily_df["has_market_data"] = daily_df["close"].notna()
    daily_df["source"] = "JQData_get_price_1d"
    daily_df["updated_at"] = datetime.now(timezone.utc).replace(microsecond=0)
    daily_df["year"] = daily_df["trading_date"].map(lambda value: value.year)
    daily_df["month"] = daily_df["trading_date"].map(lambda value: value.month)
    daily_df = daily_df[FUTURES_DAILY_SCHEMA.names]

    combined_df = pd.concat([existing_df, daily_df], ignore_index=True)
    combined_df = combined_df.drop_duplicates(
        ["contract_code", "trading_date"], keep="last"
    ).sort_values(["trading_date", "exchange_code", "underlying_code", "contract_code"])
    combined_df = combined_df[FUTURES_DAILY_SCHEMA.names].reset_index(drop=True)
    replace_dataset(
        pandas_to_arrow(combined_df, FUTURES_DAILY_SCHEMA),
        table_path,
        PARTITIONING,
    )
    return combined_df


@click.command()
@click.option(
    "--contract-calendar-path",
    type=click.Path(path_type=Path),
    default=CONTRACT_CALENDAR_PATH,
)
@click.option("--table-path", type=click.Path(path_type=Path), default=TABLE_PATH)
@click.option("--full-refresh", is_flag=True)
def main(contract_calendar_path: Path, table_path: Path, full_refresh: bool) -> None:
    """Update only through the upstream contract-calendar watermark."""
    result_df = update_futures_daily(contract_calendar_path, table_path, full_refresh)
    click.echo(f"row_count: {len(result_df)}")
    click.echo(f"last_date: {result_df['trading_date'].max()}")


if __name__ == "__main__":
    main()
