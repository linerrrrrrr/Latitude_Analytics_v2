"""Create or incrementally update session-aligned futures minute bars."""

from __future__ import annotations

import shutil
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import click
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from config.data_contracts import FUTURES_MINUTE_SCHEMA, pandas_to_arrow  # noqa: E402
from config.settings import settings  # noqa: E402
from lakehouse import hive_partitioning, read_dataset  # noqa: E402

LAKE_ROOT = PROJECT_ROOT / "03_Futures_Database" / "futures_lake" / "silver"
CONTRACT_CALENDAR_PATH = LAKE_ROOT / "dim_futures_contract_calendar"
TABLE_PATH = LAKE_ROOT / "fact_futures_minute"
CONTRACT_PARTITIONING = hive_partitioning(
    [pa.field("exchange_code", pa.string()), pa.field("year", pa.int16()), pa.field("month", pa.int8())]
)
MINUTE_PARTITIONING = hive_partitioning(
    [
        pa.field("exchange_code", pa.string()),
        pa.field("underlying_code", pa.string()),
        pa.field("year", pa.int16()),
        pa.field("month", pa.int8()),
    ]
)
PRICE_FIELDS = ["open", "high", "low", "close", "volume", "money", "open_interest"]
CHINA_TZ = "Asia/Shanghai"


def select_varieties(session_df: pd.DataFrame, sample_per_exchange: int) -> pd.DataFrame:
    varieties_df = (
        session_df[["exchange_code", "underlying_code"]]
        .drop_duplicates()
        .sort_values(["exchange_code", "underlying_code"])
    )
    sampled_df = varieties_df.groupby("exchange_code").head(sample_per_exchange)
    required_df = varieties_df[varieties_df["underlying_code"].isin(["RB", "CU"])]
    return (
        pd.concat([sampled_df, required_df])
        .drop_duplicates()
        .sort_values(["exchange_code", "underlying_code"])
    )


def fetch_partition_bars(session_df: pd.DataFrame) -> pd.DataFrame:
    import jqdatasdk

    contract_codes = sorted(session_df["contract_code"].unique())
    raw_df = jqdatasdk.get_price(
        contract_codes,
        start_date=session_df["session_start_at"].min().tz_localize(None),
        end_date=session_df["session_end_at"].max().tz_localize(None),
        frequency="1m",
        fields=PRICE_FIELDS,
        skip_paused=True,
        fq=None,
        panel=False,
    )
    if raw_df.empty:
        return pd.DataFrame(columns=FUTURES_MINUTE_SCHEMA.names)
    if "time" not in raw_df.columns:
        raw_df = raw_df.rename_axis("time").reset_index()
    if "code" not in raw_df.columns:
        raw_df["code"] = contract_codes[0]
    raw_df = raw_df.rename(columns={"code": "contract_code", "time": "bar_at"})
    raw_df["bar_at"] = pd.to_datetime(raw_df["bar_at"]).dt.tz_localize(CHINA_TZ)

    frames = []
    for contract_code, contract_bars_df in raw_df.groupby("contract_code"):
        rules_df = session_df[session_df["contract_code"] == contract_code].sort_values(
            "session_end_at"
        ).copy()
        for column in ["session_start_at", "session_end_at"]:
            rules_df[column] = pd.Series(
                pd.DatetimeIndex(rules_df[column].array).as_unit("ns"),
                index=rules_df.index,
            )
        contract_bars_df = contract_bars_df.sort_values("bar_at")
        aligned_df = pd.merge_asof(
            contract_bars_df,
            rules_df[
                ["session_start_at", "session_end_at", "trading_date", "session_number"]
            ],
            left_on="bar_at",
            right_on="session_end_at",
            direction="forward",
        )
        aligned_df = aligned_df[
            (aligned_df["bar_at"] > aligned_df["session_start_at"])
            & (aligned_df["bar_at"] <= aligned_df["session_end_at"])
        ]
        frames.append(aligned_df)

    if not frames:
        return pd.DataFrame(columns=FUTURES_MINUTE_SCHEMA.names)
    minute_df = pd.concat(frames, ignore_index=True)
    minute_df["exchange_code"] = session_df["exchange_code"].iloc[0]
    minute_df["underlying_code"] = session_df["underlying_code"].iloc[0]
    minute_df["source"] = "JQData_get_price_1m"
    minute_df["updated_at"] = datetime.now(timezone.utc).replace(microsecond=0)
    minute_df["year"] = minute_df["trading_date"].map(lambda value: value.year)
    minute_df["month"] = minute_df["trading_date"].map(lambda value: value.month)
    return minute_df[FUTURES_MINUTE_SCHEMA.names].drop_duplicates(
        ["contract_code", "bar_at"], keep="last"
    )


def existing_watermarks(table_path: Path) -> dict[tuple[str, str, int, int], object]:
    if not table_path.exists():
        return {}
    existing_df = read_dataset(
        table_path,
        MINUTE_PARTITIONING,
        ["exchange_code", "underlying_code", "year", "month", "trading_date"],
    )
    return (
        existing_df.groupby(["exchange_code", "underlying_code", "year", "month"])[
            "trading_date"
        ]
        .max()
        .to_dict()
    )


def update_futures_minute(
    contract_calendar_path: Path = CONTRACT_CALENDAR_PATH,
    table_path: Path = TABLE_PATH,
    sample_per_exchange: int = 1,
    full_refresh: bool = False,
) -> tuple[int, pd.DataFrame]:
    if full_refresh and table_path.exists():
        shutil.rmtree(table_path)

    session_df = read_dataset(contract_calendar_path, CONTRACT_PARTITIONING)
    selected_df = select_varieties(session_df, sample_per_exchange)
    session_df = session_df.merge(
        selected_df, on=["exchange_code", "underlying_code"], how="inner"
    )
    watermarks = existing_watermarks(table_path)
    partition_columns = ["exchange_code", "underlying_code", "year", "month"]
    row_count = 0
    pending_partitions = [
        (partition, partition_df)
        for partition, partition_df in session_df.groupby(partition_columns, sort=True)
        if watermarks.get(partition) != partition_df["trading_date"].max()
    ]
    if not pending_partitions:
        return 0, selected_df

    import jqdatasdk

    jqdatasdk.auth(settings.jqdata_id, settings.jqdata_secret)
    for partition, partition_df in pending_partitions:
        minute_df = fetch_partition_bars(partition_df)
        if minute_df.empty:
            click.echo(f"empty: {partition}")
            continue
        table_path.mkdir(parents=True, exist_ok=True)
        ds.write_dataset(
            pandas_to_arrow(minute_df, FUTURES_MINUTE_SCHEMA),
            table_path,
            format="parquet",
            partitioning=MINUTE_PARTITIONING,
            basename_template=f"part-{uuid.uuid4().hex}-{{i}}.parquet",
            existing_data_behavior="delete_matching",
        )
        row_count += len(minute_df)
        click.echo(f"written: {partition}, rows={len(minute_df)}")
    return row_count, selected_df


@click.command()
@click.option(
    "--contract-calendar-path",
    type=click.Path(path_type=Path),
    default=CONTRACT_CALENDAR_PATH,
)
@click.option("--table-path", type=click.Path(path_type=Path), default=TABLE_PATH)
@click.option("--sample-per-exchange", type=click.IntRange(1), default=1, show_default=True)
@click.option("--full-refresh", is_flag=True)
def main(
    contract_calendar_path: Path,
    table_path: Path,
    sample_per_exchange: int,
    full_refresh: bool,
) -> None:
    """Update sampled varieties; RB and CU are always included."""
    row_count, selected_df = update_futures_minute(
        contract_calendar_path, table_path, sample_per_exchange, full_refresh
    )
    click.echo(selected_df.to_string(index=False))
    click.echo(f"written_rows: {row_count}")
    click.echo(f"table_path: {table_path}")


if __name__ == "__main__":
    main()
