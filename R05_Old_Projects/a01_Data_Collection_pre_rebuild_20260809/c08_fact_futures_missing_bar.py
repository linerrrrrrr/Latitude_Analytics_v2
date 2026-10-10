#!/usr/bin/env python
# coding: utf-8

# # c08_fact_futures_missing_bar
# 
# 只检查 `is_fetch_required=True` 且 `is_fetch_completed=True` 的状态：日线记录缺失交易日，分钟线记录精确到 Session 内具体分钟的缺失明细。API 异常不写入本表。
# 
# 本 Notebook 是该业务工作流的唯一可编辑源文件；同名 `.py` 由项目标准 `latitude` 环境中的默认 PythonExporter 完整生成。

# In[ ]:


"""检测期货日线/分钟线事实表中的真实缺失。"""

from __future__ import annotations

import pathlib
import shutil
import sys
import uuid
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
WORKFLOW_DIR = PROJECT_ROOT / "02_Quant_Trading" / "a01_Data_Collection"
sys.path.insert(0, str(WORKFLOW_DIR))

from config.data_contracts import (  # noqa: E402
    FUTURES_DAILY_SCHEMA,
    FUTURES_FETCH_STATUS_SCHEMA,
    FUTURES_MINUTE_SCHEMA,
    FUTURES_MISSING_BAR_SCHEMA,
    empty_pandas,
    pandas_to_arrow,
)
from c00_futures_fetch_control import (  # noqa: E402
    MISSING_PARTITIONING,
    STATUS_PARTITIONING,
    detect_missing_bars,
    read_status_partition,
    write_missing_dataset,
)
from c00_lakehouse import (  # noqa: E402
    dataset_partitions,
    hive_partitioning,
    swap_staged_dataset,
    validate_dataset_streaming,
)

LAKE_ROOT = PROJECT_ROOT / "03_Futures_Database" / "futures_lake" / "silver"
STATUS_PATH = LAKE_ROOT / "fact_futures_fetch_status"
DAILY_PATH = LAKE_ROOT / "fact_futures_daily"
MINUTE_PATH = LAKE_ROOT / "fact_futures_minute"
TABLE_PATH = LAKE_ROOT / "fact_futures_missing_bar"
DAILY_PARTITIONING = hive_partitioning([pa.field("exchange_code", pa.string()), pa.field("year", pa.int16()), pa.field("month", pa.int8())])
MINUTE_PARTITIONING = hive_partitioning([pa.field("exchange_code", pa.string()), pa.field("underlying_code", pa.string()), pa.field("year", pa.int16()), pa.field("month", pa.int8())])


# In[ ]:


def _partition_filter(exchange_code: str, year: int, month: int) -> ds.Expression:
    return (
        (ds.field("exchange_code") == exchange_code)
        & (ds.field("year") == year)
        & (ds.field("month") == month)
    )


def read_fact_partition(
    bar_frequency: str,
    exchange_code: str,
    year: int,
    month: int,
    daily_path: Path,
    minute_path: Path,
) -> pd.DataFrame:
    table_path = daily_path if bar_frequency == "1d" else minute_path
    schema = FUTURES_DAILY_SCHEMA if bar_frequency == "1d" else FUTURES_MINUTE_SCHEMA
    partitioning = DAILY_PARTITIONING if bar_frequency == "1d" else MINUTE_PARTITIONING
    if not table_path.exists():
        return empty_pandas(schema)
    dataset = ds.dataset(table_path, format="parquet", partitioning=partitioning)
    return dataset.to_table(
        columns=schema.names,
        filter=_partition_filter(exchange_code, year, month),
    ).to_pandas()


# In[ ]:


def update_missing_bars(
    status_path: Path = STATUS_PATH,
    daily_path: Path = DAILY_PATH,
    minute_path: Path = MINUTE_PATH,
    table_path: Path = TABLE_PATH,
) -> tuple[int, int]:
    """流式重建缺失明细，并用同一次检测结果更新主状态表。"""
    partitions = dataset_partitions(
        status_path, ["bar_frequency", "exchange_code", "year", "month"]
    )
    if not partitions:
        raise ValueError(f"拉取状态表没有可用分区：{status_path}")
    status_staging_path = status_path.with_name(f".{status_path.name}.{uuid.uuid4().hex}.tmp")
    missing_staging_path = table_path.with_name(f".{table_path.name}.{uuid.uuid4().hex}.tmp")
    status_row_count = 0
    missing_row_count = 0
    try:
        for partition in partitions:
            bar_frequency = str(partition["bar_frequency"])
            exchange_code = str(partition["exchange_code"])
            year = int(partition["year"])
            month = int(partition["month"])
            status_df = read_status_partition(
                status_path, bar_frequency, exchange_code, year, month
            )
            should_check = status_df["is_fetch_required"].astype(bool) & status_df["is_fetch_completed"].astype(bool)
            if should_check.any():
                fact_df = read_fact_partition(
                    bar_frequency, exchange_code, year, month, daily_path, minute_path
                )
                status_df, missing_df = detect_missing_bars(status_df, fact_df)
                if not missing_df.empty:
                    missing_row_count += write_missing_dataset([missing_df], missing_staging_path)
            status_row_count += len(status_df)
            ds.write_dataset(
                pandas_to_arrow(status_df, FUTURES_FETCH_STATUS_SCHEMA),
                status_staging_path,
                format="parquet",
                partitioning=STATUS_PARTITIONING,
                basename_template=f"part-{uuid.uuid4().hex}-{{i}}.parquet",
                existing_data_behavior="overwrite_or_ignore",
            )
        if missing_row_count == 0:
            write_missing_dataset([], missing_staging_path)
        validate_dataset_streaming(
            missing_staging_path, MISSING_PARTITIONING, FUTURES_MISSING_BAR_SCHEMA, missing_row_count
        )
        validate_dataset_streaming(
            status_staging_path, STATUS_PARTITIONING, FUTURES_FETCH_STATUS_SCHEMA, status_row_count
        )
        swap_staged_dataset(missing_staging_path, table_path)
        swap_staged_dataset(status_staging_path, status_path)
    finally:
        for staging_path in [status_staging_path, missing_staging_path]:
            if staging_path.exists():
                shutil.rmtree(staging_path)
    return status_row_count, missing_row_count


# In[ ]:


@click.command()
@click.option("--status-path", type=click.Path(path_type=Path), default=STATUS_PATH)
@click.option("--daily-path", type=click.Path(path_type=Path), default=DAILY_PATH)
@click.option("--minute-path", type=click.Path(path_type=Path), default=MINUTE_PATH)
@click.option("--table-path", type=click.Path(path_type=Path), default=TABLE_PATH)
def main(status_path: Path, daily_path: Path, minute_path: Path, table_path: Path) -> None:
    """重建精确缺失明细并更新状态。"""
    status_row_count, missing_row_count = update_missing_bars(
        status_path, daily_path, minute_path, table_path
    )
    click.echo(f"status_row_count: {status_row_count}")
    click.echo(f"missing_row_count: {missing_row_count}")
    click.echo(f"table_path: {table_path}")


# In[ ]:


def running_in_ipykernel() -> bool:
    try:
        from ipykernel.kernelapp import IPKernelApp
    except ImportError:
        return False

    return IPKernelApp.initialized()


if __name__ == "__main__":
    in_kernel = running_in_ipykernel()
    main(args=[] if in_kernel else None, standalone_mode=not in_kernel)

