#!/usr/bin/env python
# coding: utf-8

# # c01_dimension_trade_calendar
# 
# 创建或增量更新中国市场交易日历。
# 
# 本 Notebook 是该业务工作流的唯一可编辑源文件；同名 `.py` 由项目标准 `latitude` 环境中的默认 PythonExporter 完整生成。

# In[ ]:


"""创建或增量更新中国市场交易日历。"""

from __future__ import annotations

import pathlib
import sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import click
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds


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
    TRADE_CALENDAR_SCHEMA,
    arrow_to_pandas,
    empty_pandas,
    pandas_to_arrow,
)
from config.settings import settings  # noqa: E402
from c00_jqdata_connection import authenticate_jqdata  # noqa: E402
from c00_lakehouse import replace_dataset  # noqa: E402


CHINA_TIMEZONE = timezone(timedelta(hours=8), name="Asia/Shanghai")
START_DATE = date(2010, 1, 1)
CUTOFF_TIME = time(20, 0)
TABLE_PATH = (
    PROJECT_ROOT
    / "03_Futures_Database"
    / "futures_lake"
    / "silver"
    / "dim_trade_calendar"
)
YEAR_PARTITIONING = ds.partitioning(
    pa.schema([pa.field("year", pa.int16(), nullable=False)]),
    flavor="hive",
)


# In[ ]:


def calendar_end_date(as_of_datetime: datetime | None = None) -> date:
    """北京时间 20:00 后使用当天，否则使用前一天。"""
    current_datetime = as_of_datetime or datetime.now(CHINA_TIMEZONE)
    if current_datetime.tzinfo is None:
        current_datetime = current_datetime.replace(tzinfo=CHINA_TIMEZONE)
    current_datetime = current_datetime.astimezone(CHINA_TIMEZONE)

    if current_datetime.time() >= CUTOFF_TIME:
        return current_datetime.date()
    return current_datetime.date() - timedelta(days=1)


# In[ ]:


def fetch_trading_dates(start_date: date, end_date: date) -> set[date]:
    """从 JQData 获取官方交易日期。"""
    jqdata = authenticate_jqdata(settings.jqdata_id, settings.jqdata_secret)
    jq_dates = jqdata.get_trade_days(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
    )
    return {pd.Timestamp(jq_date).date() for jq_date in jq_dates}


# In[ ]:


def build_calendar_df(
    start_date: date,
    end_date: date,
    trading_dates: set[date],
) -> pd.DataFrame:
    """为日历范围内的每个自然日生成一行数据。"""
    updated_at = datetime.now(timezone.utc).replace(microsecond=0)
    calendar_dates = pd.date_range(start_date, end_date, freq="D").date

    return pd.DataFrame(
        {
            "calendar_date": calendar_dates,
            "date_key": [calendar_date.strftime("%Y%m%d") for calendar_date in calendar_dates],
            "is_trading_day": [calendar_date in trading_dates for calendar_date in calendar_dates],
            "weekday": [calendar_date.isoweekday() for calendar_date in calendar_dates],
            "is_weekend": [calendar_date.isoweekday() >= 6 for calendar_date in calendar_dates],
            "source": "JQData",
            "calendar_name": "CN_MARKET",
            "calendar_timezone": "Asia/Shanghai",
            "effective_after": CUTOFF_TIME,
            "updated_at": updated_at,
            "year": [calendar_date.year for calendar_date in calendar_dates],
        },
        columns=TRADE_CALENDAR_SCHEMA.names,
    )


# In[ ]:


def read_calendar_df(table_path: Path) -> pd.DataFrame:
    if not table_path.exists():
        return empty_pandas(TRADE_CALENDAR_SCHEMA)
    table = ds.dataset(
        table_path,
        format="parquet",
        partitioning=YEAR_PARTITIONING,
    ).to_table()
    return arrow_to_pandas(table, TRADE_CALENDAR_SCHEMA)


# In[ ]:


def write_calendar_df(calendar_df: pd.DataFrame, table_path: Path) -> None:
    calendar_df = calendar_df.copy()
    calendar_df["calendar_date"] = pd.to_datetime(calendar_df["calendar_date"]).dt.date
    calendar_df["date_key"] = calendar_df["date_key"].astype(str)
    calendar_df["effective_after"] = calendar_df["effective_after"].map(
        lambda clock_time: (
            time.fromisoformat(clock_time) if isinstance(clock_time, str) else clock_time
        )
    )
    calendar_df["updated_at"] = pd.to_datetime(calendar_df["updated_at"], utc=True)
    calendar_df["weekday"] = calendar_df["weekday"].astype("int8")
    calendar_df["year"] = calendar_df["year"].astype("int16")

    calendar_table = pandas_to_arrow(calendar_df, TRADE_CALENDAR_SCHEMA)
    replace_dataset(
        calendar_table,
        table_path,
        YEAR_PARTITIONING,
        TRADE_CALENDAR_SCHEMA,
    )


# In[ ]:


def update_trade_calendar(
    table_path: Path = TABLE_PATH,
    start_date: date | None = None,
    end_date: date | None = None,
    full_refresh: bool = False,
    as_of_datetime: datetime | None = None,
) -> pd.DataFrame:
    """更新数据集并返回完整交易日历。"""
    target_end_date = end_date or calendar_end_date(as_of_datetime)
    target_start_date = start_date or START_DATE
    existing_df = read_calendar_df(table_path)

    if full_refresh or existing_df.empty:
        fetch_start_date = target_start_date
        existing_df = empty_pandas(TRADE_CALENDAR_SCHEMA)
    else:
        last_date = existing_df["calendar_date"].max()
        if last_date >= target_end_date:
            return existing_df
        fetch_start_date = last_date + timedelta(days=1)

    trading_dates = fetch_trading_dates(fetch_start_date, target_end_date)
    new_df = build_calendar_df(fetch_start_date, target_end_date, trading_dates)
    calendar_df = pd.concat([existing_df, new_df], ignore_index=True)
    calendar_df = calendar_df.drop_duplicates("calendar_date", keep="last")
    calendar_df = calendar_df.sort_values("calendar_date").reset_index(drop=True)
    write_calendar_df(calendar_df, table_path)
    return calendar_df


# In[ ]:


@click.command()
@click.option("--table-path", type=click.Path(path_type=Path), default=TABLE_PATH, show_default=True)
@click.option("--start-date", type=click.DateTime(["%Y-%m-%d"]), default=str(START_DATE), show_default=True)
@click.option("--end-date", type=click.DateTime(["%Y-%m-%d"]))
@click.option("--full-refresh", is_flag=True)
def main(
    table_path: Path,
    start_date: datetime | None,
    end_date: datetime | None,
    full_refresh: bool,
) -> None:
    """创建或增量更新中国市场交易日历。"""
    calendar_df = update_trade_calendar(
        table_path=table_path,
        start_date=start_date.date() if start_date else None,
        end_date=end_date.date() if end_date else None,
        full_refresh=full_refresh,
    )
    click.echo(f"table_path: {table_path}")
    click.echo(f"row_count: {len(calendar_df)}")
    click.echo(f"last_date: {calendar_df['calendar_date'].max()}")


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


# In[ ]:




