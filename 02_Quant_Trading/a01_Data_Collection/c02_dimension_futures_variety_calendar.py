#!/usr/bin/env python
# coding: utf-8

# # c02_dimension_futures_variety_calendar
# 
# 创建或增量更新国内期货品种日历。
# 
# 本 Notebook 是该业务工作流的唯一可编辑源文件；同名 `.py` 由项目标准 `latitude` 环境中的默认 PythonExporter 完整生成。

# In[ ]:


"""创建或增量更新国内期货品种日历。"""

from __future__ import annotations

import pathlib
import re
import sys
from datetime import date, datetime, timezone
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

from config.data_contracts import (  # noqa: E402
    FUTURES_VARIETY_CALENDAR_SCHEMA,
    TRADE_CALENDAR_SCHEMA,
    empty_pandas,
    pandas_to_arrow,
)
from config.settings import settings  # noqa: E402
from c00_jqdata_connection import authenticate_jqdata  # noqa: E402
from c00_lakehouse import hive_partitioning, read_dataset, replace_dataset  # noqa: E402

LAKE_ROOT = PROJECT_ROOT / "03_Futures_Database" / "futures_lake" / "silver"
CALENDAR_PATH = LAKE_ROOT / "dim_trade_calendar"
TABLE_PATH = LAKE_ROOT / "dim_futures_variety_calendar"
PARTITIONING = hive_partitioning(
    [pa.field("exchange_code", pa.string()), pa.field("year", pa.int16()), pa.field("month", pa.int8())]
)
TRADE_CALENDAR_PARTITIONING = hive_partitioning([pa.field("year", pa.int16())])
SUPPORTED_EXCHANGES = {"XDCE", "XSGE", "XZCE", "XINE", "GFEX"}


# In[ ]:


def fixed_contract(code: str) -> bool:
    stem = code.rsplit(".", 1)[0]
    return bool(re.fullmatch(r"[A-Za-z]+[0-9]+", stem)) and not stem.endswith(("8888", "9999"))


# In[ ]:


def underlying(code: str) -> str:
    return re.match(r"[A-Za-z]+", code).group().upper()


# In[ ]:


def update_variety_calendar(
    calendar_path: Path = CALENDAR_PATH,
    table_path: Path = TABLE_PATH,
    full_refresh: bool = False,
) -> pd.DataFrame:
    calendar_df = read_dataset(
        calendar_path,
        TRADE_CALENDAR_PARTITIONING,
        TRADE_CALENDAR_SCHEMA,
    )
    trading_dates = sorted(calendar_df.loc[calendar_df["is_trading_day"], "calendar_date"].tolist())
    existing_df = (
        empty_pandas(FUTURES_VARIETY_CALENDAR_SCHEMA)
        if full_refresh
        else read_dataset(table_path, PARTITIONING, FUTURES_VARIETY_CALENDAR_SCHEMA)
    )
    last_date = existing_df["trading_date"].max() if not existing_df.empty else None
    pending_dates = [value for value in trading_dates if last_date is None or value > last_date]
    if not pending_dates:
        return existing_df

    jqdata = authenticate_jqdata(settings.jqdata_id, settings.jqdata_secret)
    securities_df = jqdata.get_all_securities(["futures"]).rename_axis("contract_code").reset_index()
    securities_df = securities_df[securities_df["contract_code"].map(fixed_contract)].copy()
    securities_df["underlying_code"] = securities_df["contract_code"].map(underlying)
    securities_df["exchange_code"] = securities_df["contract_code"].str.rsplit(".", n=1).str[-1]
    securities_df = securities_df[
        securities_df["exchange_code"].isin(SUPPORTED_EXCHANGES)
    ]
    updated_at = datetime.now(timezone.utc).replace(microsecond=0)
    frames = []
    for trading_date in pending_dates:
        active_df = securities_df[
            (securities_df["start_date"].dt.date <= trading_date)
            & (securities_df["end_date"].dt.date >= trading_date)
        ]
        daily_df = (
            active_df.groupby(["underlying_code", "exchange_code"])
            .size()
            .rename("active_contract_count")
            .reset_index()
        )
        daily_df["trading_date"] = trading_date
        daily_df["active_contract_count"] = daily_df["active_contract_count"].astype("int16")
        daily_df["source"] = "JQData"
        daily_df["updated_at"] = updated_at
        daily_df["year"] = trading_date.year
        daily_df["month"] = trading_date.month
        frames.append(daily_df[FUTURES_VARIETY_CALENDAR_SCHEMA.names])

    combined_df = pd.concat([existing_df, *frames], ignore_index=True)
    combined_df = combined_df.drop_duplicates(
        ["underlying_code", "exchange_code", "trading_date"], keep="last"
    ).sort_values(["trading_date", "exchange_code", "underlying_code"])
    combined_df = combined_df[FUTURES_VARIETY_CALENDAR_SCHEMA.names].reset_index(drop=True)
    replace_dataset(
        pandas_to_arrow(combined_df, FUTURES_VARIETY_CALENDAR_SCHEMA),
        table_path,
        PARTITIONING,
        FUTURES_VARIETY_CALENDAR_SCHEMA,
    )
    return combined_df


# In[ ]:


@click.command()
@click.option("--calendar-path", type=click.Path(path_type=Path), default=CALENDAR_PATH)
@click.option("--table-path", type=click.Path(path_type=Path), default=TABLE_PATH)
@click.option("--full-refresh", is_flag=True)
def main(calendar_path: Path, table_path: Path, full_refresh: bool) -> None:
    """仅更新至上游交易日历的数据水位。"""
    result_df = update_variety_calendar(calendar_path, table_path, full_refresh)
    click.echo(f"row_count: {len(result_df)}")
    click.echo(f"last_date: {result_df['trading_date'].max()}")


# In[ ]:


def running_in_ipykernel() -> bool:
    try:
        from ipykernel.kernelapp import IPKernelApp
    except ImportError:
        return False

    return IPKernelApp.initialized()


if __name__ == "__main__":
    in_kernel = running_in_ipykernel()
    main(
        args=[] if in_kernel else None,
        standalone_mode=not in_kernel,
    )

