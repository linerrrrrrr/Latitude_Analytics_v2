#!/usr/bin/env python
# coding: utf-8

# # c03_dimension_futures_contract_calendar
# 
# 依据固定合约上市/退市候选区间与 `get_futures_info` 历史交易时段规则，按交易所、年、月流式重建合约 Session 日历。候选区间内没有有效 `trade_time` 的合约日不生成 Session，并单独计数。正式目录只在暂存数据集完整复读校验后替换。
# 
# 本 Notebook 是该业务工作流的唯一可编辑源文件；同名 `.py` 由项目标准 `latitude` 环境中的默认 PythonExporter 完整生成。

# In[ ]:


"""创建合约交易日的交易 Session 日历。"""

from __future__ import annotations

import pathlib
import sys
from pathlib import Path

import click

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

from config.settings import settings  # noqa: E402
from c00_futures_contract_calendar import (  # noqa: E402
    ContractCalendarBuildSummary,
    active_rule,
    fixed_contract,
    rebuild_contract_calendar,
    underlying,
)
from c00_jqdata_connection import authenticate_jqdata  # noqa: E402

LAKE_ROOT = PROJECT_ROOT / "03_Futures_Database" / "futures_lake" / "silver"
VARIETY_PATH = LAKE_ROOT / "dim_futures_variety_calendar"
TABLE_PATH = LAKE_ROOT / "dim_futures_contract_calendar"


# In[ ]:


def update_contract_calendar(
    variety_path: Path = VARIETY_PATH,
    table_path: Path = TABLE_PATH,
    full_refresh: bool = False,
) -> ContractCalendarBuildSummary:
    """确定性重建全量；保留 full_refresh 参数兼容既有调度命令。"""
    del full_refresh
    jqdata = authenticate_jqdata(settings.jqdata_id, settings.jqdata_secret)
    return rebuild_contract_calendar(variety_path, table_path, jqdata)


# In[ ]:


@click.command()
@click.option("--variety-path", type=click.Path(path_type=Path), default=VARIETY_PATH)
@click.option("--table-path", type=click.Path(path_type=Path), default=TABLE_PATH)
@click.option("--full-refresh", is_flag=True)
def main(variety_path: Path, table_path: Path, full_refresh: bool) -> None:
    """按分区流式全量重建合约 Session 日历。"""
    summary = update_contract_calendar(variety_path, table_path, full_refresh)
    click.echo(f"row_count: {summary.row_count}")
    click.echo(f"contract_count: {summary.contract_count}")
    click.echo(f"no_trade_time_contract_days: {summary.no_trade_time_contract_day_count}")
    click.echo(f"first_date: {summary.first_date}")
    click.echo(f"last_date: {summary.last_date}")
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

