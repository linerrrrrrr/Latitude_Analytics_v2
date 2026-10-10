#!/usr/bin/env python
# coding: utf-8

# # c06_dimension_futures_session_schedule_signal
# 
# 把交易日之间出现工作日休市记录为 `suspected_closed` 夜盘信号。该信号是推断证据，不直接豁免拉取；只有后续写入的 `authoritative + is_fetch_exempt=True` 记录才能影响拉取要求。
# 
# 本 Notebook 是该业务工作流的唯一可编辑源文件；同名 `.py` 由项目标准 `latitude` 环境中的默认 PythonExporter 完整生成。

# In[ ]:


"""建立期货 Session 日历开闭市信号维表。"""

from __future__ import annotations

import pathlib
import sys
from pathlib import Path

import click
import pandas as pd

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

from c00_futures_fetch_control import rebuild_session_schedule_signals  # noqa: E402

LAKE_ROOT = PROJECT_ROOT / "03_Futures_Database" / "futures_lake" / "silver"
TRADE_CALENDAR_PATH = LAKE_ROOT / "dim_trade_calendar"
CONTRACT_CALENDAR_PATH = LAKE_ROOT / "dim_futures_contract_calendar"
TABLE_PATH = LAKE_ROOT / "dim_futures_session_schedule_signal"


# In[ ]:


def update_session_schedule_signals(
    trade_calendar_path: Path = TRADE_CALENDAR_PATH,
    contract_calendar_path: Path = CONTRACT_CALENDAR_PATH,
    table_path: Path = TABLE_PATH,
) -> pd.DataFrame:
    """重建推断信号，并保留既有 authoritative 记录。"""
    return rebuild_session_schedule_signals(
        trade_calendar_path, contract_calendar_path, table_path
    )


# In[ ]:


@click.command()
@click.option("--trade-calendar-path", type=click.Path(path_type=Path), default=TRADE_CALENDAR_PATH)
@click.option("--contract-calendar-path", type=click.Path(path_type=Path), default=CONTRACT_CALENDAR_PATH)
@click.option("--table-path", type=click.Path(path_type=Path), default=TABLE_PATH)
def main(trade_calendar_path: Path, contract_calendar_path: Path, table_path: Path) -> None:
    """重建 Session 日历信号维表。"""
    signal_df = update_session_schedule_signals(
        trade_calendar_path, contract_calendar_path, table_path
    )
    click.echo(f"row_count: {len(signal_df)}")
    click.echo(f"suspected_count: {(signal_df['schedule_status'] == 'suspected_closed').sum()}")
    click.echo(f"fetch_exempt_count: {signal_df['is_fetch_exempt'].sum()}")
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

