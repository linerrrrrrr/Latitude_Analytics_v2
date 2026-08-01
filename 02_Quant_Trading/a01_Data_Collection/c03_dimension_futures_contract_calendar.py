"""Create or incrementally update contract-day trading sessions."""

from __future__ import annotations

import pathlib
import re
import sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import click
import pandas as pd
import pyarrow as pa

project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()  # 当前工作目录

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        break
else:
    raise RuntimeError("未找到项目根目录")

PROJECT_ROOT = candidate_root

from config.data_contracts import FUTURES_CONTRACT_CALENDAR_SCHEMA, pandas_to_arrow  # noqa: E402
from config.settings import settings  # noqa: E402
from lakehouse import hive_partitioning, read_dataset, replace_dataset  # noqa: E402

LAKE_ROOT = PROJECT_ROOT / "03_Futures_Database" / "futures_lake" / "silver"
VARIETY_PATH = LAKE_ROOT / "dim_futures_variety_calendar"
TABLE_PATH = LAKE_ROOT / "dim_futures_contract_calendar"
PARTITIONING = hive_partitioning(
    [pa.field("exchange_code", pa.string()), pa.field("year", pa.int16()), pa.field("month", pa.int8())]
)
VARIETY_PARTITIONING = PARTITIONING
CHINA_TZ = "Asia/Shanghai"


def fixed_contract(code: str) -> bool:
    stem = code.rsplit(".", 1)[0]
    return bool(re.fullmatch(r"[A-Za-z]+[0-9]+", stem)) and not stem.endswith(("8888", "9999"))


def underlying(code: str) -> str:
    return re.match(r"[A-Za-z]+", code).group().upper()


def active_rule(rules: list[list[str]], contract: str, trading_date: date) -> list[str]:
    matches = [
        rule for rule in rules
        if date.fromisoformat(rule[0]) <= trading_date <= date.fromisoformat(rule[1])
    ]
    if len(matches) != 1:
        raise ValueError(f"Expected one trade-time rule: {contract} {trading_date}: {matches}")
    return matches[0]


def update_contract_calendar(
    variety_path: Path = VARIETY_PATH,
    table_path: Path = TABLE_PATH,
    full_refresh: bool = False,
) -> pd.DataFrame:
    import jqdatasdk

    variety_df = read_dataset(variety_path, VARIETY_PARTITIONING)
    existing_df = pd.DataFrame() if full_refresh else read_dataset(table_path, PARTITIONING)
    last_date = existing_df["trading_date"].max() if not existing_df.empty else None
    pending_variety_df = variety_df[
        variety_df["trading_date"].map(lambda value: last_date is None or value > last_date)
    ]
    if pending_variety_df.empty:
        return existing_df

    jqdatasdk.auth(settings.jqdata_id, settings.jqdata_secret)
    securities_df = jqdatasdk.get_all_securities(["futures"]).rename_axis("contract_code").reset_index()
    securities_df = securities_df[securities_df["contract_code"].map(fixed_contract)].copy()
    securities_df["underlying_code"] = securities_df["contract_code"].map(underlying)
    securities_df["exchange_code"] = securities_df["contract_code"].str.rsplit(".", n=1).str[-1]
    needed_pairs = set(zip(pending_variety_df["underlying_code"], pending_variety_df["exchange_code"]))
    pending_start = pending_variety_df["trading_date"].min()
    pending_end = pending_variety_df["trading_date"].max()
    securities_df = securities_df[
        securities_df.apply(lambda row: (row["underlying_code"], row["exchange_code"]) in needed_pairs, axis=1)
        & (securities_df["start_date"].dt.date <= pending_end)
        & (securities_df["end_date"].dt.date >= pending_start)
    ]
    codes = securities_df["contract_code"].tolist()
    info = {}
    for offset in range(0, len(codes), 200):
        info.update(
            jqdatasdk.get_futures_info(
                codes[offset : offset + 200],
                fields=["contract_multiplier", "tick_size", "trade_time"],
            )
        )

    unique_dates = sorted(variety_df["trading_date"].unique())
    previous_dates = {
        current: previous
        for previous, current in zip(
            unique_dates[:-1],
            unique_dates[1:],
        )
    }
    first_date = unique_dates[0]
    prior_dates = jqdatasdk.get_trade_days(end_date=first_date, count=2)
    previous_dates[first_date] = pd.Timestamp(prior_dates[0]).date()
    updated_at = datetime.now(timezone.utc).replace(microsecond=0)
    timezone_info = pd.Timestamp.now(tz=CHINA_TZ).tz
    rows = []
    for variety in pending_variety_df.itertuples(index=False):
        previous_date = previous_dates.get(variety.trading_date)
        if previous_date is None:
            continue
        contracts_df = securities_df[
            (securities_df["underlying_code"] == variety.underlying_code)
            & (securities_df["exchange_code"] == variety.exchange_code)
            & (securities_df["start_date"].dt.date <= variety.trading_date)
            & (securities_df["end_date"].dt.date >= variety.trading_date)
        ]
        for contract in contracts_df.itertuples(index=False):
            contract_info = info.get(contract.contract_code)
            if not contract_info:
                raise ValueError(f"get_futures_info returned no data: {contract.contract_code}")
            rule = active_rule(contract_info["trade_time"], contract.contract_code, variety.trading_date)
            for number, session_text in enumerate(rule[2:], 1):
                start_text, end_text = session_text.split("~", 1)
                start_time, end_time = time.fromisoformat(start_text), time.fromisoformat(end_text)
                is_night = start_time >= time(20) or start_time < time(6)
                start_date = previous_date if is_night else variety.trading_date
                end_date = start_date + timedelta(days=1) if end_time <= start_time else start_date
                start_at = datetime.combine(start_date, start_time, timezone_info)
                end_at = datetime.combine(end_date, end_time, timezone_info)
                rows.append(
                    {
                        "contract_code": contract.contract_code,
                        "exchange_code": variety.exchange_code,
                        "underlying_code": variety.underlying_code,
                        "trading_date": variety.trading_date,
                        "list_date": contract.start_date.date(),
                        "delist_date": contract.end_date.date(),
                        "contract_multiplier": contract_info.get("contract_multiplier"),
                        "tick_size": contract_info.get("tick_size"),
                        "rule_effective_date": date.fromisoformat(rule[0]),
                        "rule_expiry_date": date.fromisoformat(rule[1]),
                        "session_number": number,
                        "session_text": session_text,
                        "session_start_at": start_at,
                        "session_end_at": end_at,
                        "is_night_session": is_night,
                        "spans_midnight": start_date != end_date,
                        "minute_count": int((end_at - start_at).total_seconds() // 60),
                        "source": "JQData_get_futures_info",
                        "updated_at": updated_at,
                        "year": variety.trading_date.year,
                        "month": variety.trading_date.month,
                    }
                )

    new_df = pd.DataFrame(rows, columns=FUTURES_CONTRACT_CALENDAR_SCHEMA.names)
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    combined_df = combined_df.drop_duplicates(
        ["contract_code", "trading_date", "session_number"], keep="last"
    ).sort_values(["trading_date", "exchange_code", "underlying_code", "contract_code", "session_number"])
    combined_df = combined_df[FUTURES_CONTRACT_CALENDAR_SCHEMA.names].reset_index(drop=True)
    replace_dataset(
        pandas_to_arrow(combined_df, FUTURES_CONTRACT_CALENDAR_SCHEMA),
        table_path,
        PARTITIONING,
    )
    return combined_df


@click.command()
@click.option("--variety-path", type=click.Path(path_type=Path), default=VARIETY_PATH)
@click.option("--table-path", type=click.Path(path_type=Path), default=TABLE_PATH)
@click.option("--full-refresh", is_flag=True)
def main(variety_path: Path, table_path: Path, full_refresh: bool) -> None:
    """Update only through the upstream variety-calendar watermark."""
    result_df = update_contract_calendar(variety_path, table_path, full_refresh)
    click.echo(f"row_count: {len(result_df)}")
    click.echo(f"last_date: {result_df['trading_date'].max()}")


if __name__ == "__main__":
    main()
