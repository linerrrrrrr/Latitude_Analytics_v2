#!/usr/bin/env python
# coding: utf-8

# # c04_fact_futures_daily
# 
# 只遍历 `fact_futures_fetch_status` 中应拉取的日线合约日。请求按交易所/年份和预期返回行数分批，使用 `skip_paused=True`，单批目标不超过 90,000 行，并保留 5,000,000 行日配额。完成标记只在事实数据写入及复读校验成功后更新。
# 
# 本 Notebook 是该业务工作流的唯一可编辑源文件；同名 `.py` 由项目标准 `latitude` 环境中的默认 PythonExporter 完整生成。

# In[ ]:


"""创建或增量更新国内期货日线数据。"""

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
from c00_futures_daily_fetch import (  # noqa: E402
    DEFAULT_QUOTA_RESERVE,
    DailyFetchSummary,
    update_futures_daily as execute_daily_fetch,
)
from c00_jqdata_connection import authenticate_jqdata  # noqa: E402

LAKE_ROOT = PROJECT_ROOT / "03_Futures_Database" / "futures_lake" / "silver"
STATUS_PATH = LAKE_ROOT / "fact_futures_fetch_status"
TABLE_PATH = LAKE_ROOT / "fact_futures_daily"


# In[ ]:


def update_futures_daily(
    status_path: Path = STATUS_PATH,
    table_path: Path = TABLE_PATH,
    full_refresh: bool = False,
    dry_run: bool = False,
    quota_reserve: int = DEFAULT_QUOTA_RESERVE,
) -> DailyFetchSummary:
    """以状态表为唯一任务清单，预览或执行日线拉取。"""
    jqdata = None
    if not dry_run:
        jqdata = authenticate_jqdata(settings.jqdata_id, settings.jqdata_secret)
    return execute_daily_fetch(
        status_path, table_path, jqdata, full_refresh, dry_run, quota_reserve
    )


# In[ ]:


@click.command()
@click.option("--status-path", type=click.Path(path_type=Path), default=STATUS_PATH)
@click.option("--table-path", type=click.Path(path_type=Path), default=TABLE_PATH)
@click.option("--full-refresh", is_flag=True)
@click.option("--dry-run", is_flag=True)
@click.option("--quota-reserve", type=click.IntRange(0), default=DEFAULT_QUOTA_RESERVE, show_default=True)
def main(
    status_path: Path,
    table_path: Path,
    full_refresh: bool,
    dry_run: bool,
    quota_reserve: int,
) -> None:
    """预览或执行状态驱动的日线拉取。"""
    summary = update_futures_daily(
        status_path, table_path, full_refresh, dry_run, quota_reserve
    )
    click.echo(f"fetch_run_id: {summary.fetch_run_id}")
    click.echo(f"expected_rows: {summary.expected_row_count}")
    click.echo(f"request_count: {summary.request_count}")
    click.echo(f"returned_rows: {summary.returned_row_count}")
    click.echo(f"written_rows: {summary.written_row_count}")
    click.echo(f"completed_rows: {summary.completed_row_count}")
    click.echo(f"dry_run: {summary.is_dry_run}")
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

