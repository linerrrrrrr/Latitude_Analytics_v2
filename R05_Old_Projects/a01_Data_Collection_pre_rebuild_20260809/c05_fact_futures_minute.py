#!/usr/bin/env python
# coding: utf-8

# # c05_fact_futures_minute
# 
# 按已确认的 59 个分钟品种和状态表中的应拉取 Session，逐交易所/品种/年/月执行。先使用 `--dry-run` 复核 Session、分区和理论行数；正式执行在完整分区边界检查配额、写入事实表并登记完成状态。
# 
# 本 Notebook 是该业务工作流的唯一可编辑源文件；同名 `.py` 由项目标准 `latitude` 环境中的默认 PythonExporter 完整生成。

# In[ ]:


"""创建或增量更新按交易 Session 对齐的期货分钟线。"""

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
from c00_futures_minute_fetch import (  # noqa: E402
    DEFAULT_QUOTA_RESERVE,
    MinuteFetchSummary,
    fetch_partition_bars,
    update_futures_minute as execute_minute_fetch,
)
from c00_futures_universe import select_minute_varieties  # noqa: E402
from c00_jqdata_connection import authenticate_jqdata  # noqa: E402

LAKE_ROOT = PROJECT_ROOT / "03_Futures_Database" / "futures_lake" / "silver"
STATUS_PATH = LAKE_ROOT / "fact_futures_fetch_status"
TABLE_PATH = LAKE_ROOT / "fact_futures_minute"


# In[ ]:


def update_futures_minute(
    status_path: Path = STATUS_PATH,
    table_path: Path = TABLE_PATH,
    full_refresh: bool = False,
    dry_run: bool = False,
    quota_reserve: int = DEFAULT_QUOTA_RESERVE,
    targets: tuple[str, ...] = (),
) -> MinuteFetchSummary:
    """预览或执行状态驱动的分钟线拉取。"""
    jqdata = None
    if not dry_run:
        jqdata = authenticate_jqdata(settings.jqdata_id, settings.jqdata_secret)
    return execute_minute_fetch(
        status_path, table_path, jqdata, full_refresh, dry_run, quota_reserve, targets
    )


# In[ ]:


@click.command()
@click.option("--status-path", type=click.Path(path_type=Path), default=STATUS_PATH)
@click.option("--table-path", type=click.Path(path_type=Path), default=TABLE_PATH)
@click.option("--full-refresh", is_flag=True)
@click.option("--dry-run", is_flag=True)
@click.option("--quota-reserve", type=click.IntRange(0), default=DEFAULT_QUOTA_RESERVE, show_default=True)
@click.option("--target", "targets", multiple=True, metavar="EXCHANGE.UNDERLYING")
def main(
    status_path: Path,
    table_path: Path,
    full_refresh: bool,
    dry_run: bool,
    quota_reserve: int,
    targets: tuple[str, ...],
) -> None:
    """预览或执行分钟线拉取；配额不足时在完整分区边界正常停止。"""
    summary = update_futures_minute(
        status_path, table_path, full_refresh, dry_run, quota_reserve, targets
    )
    click.echo(summary.selected_varieties.to_string(index=False))
    click.echo(f"fetch_run_id: {summary.fetch_run_id}")
    click.echo(f"session_count: {summary.session_count}")
    click.echo(f"partition_count: {summary.partition_count}")
    click.echo(f"expected_rows: {summary.expected_row_count}")
    click.echo(f"written_rows: {summary.written_row_count}")
    click.echo(f"completed_sessions: {summary.completed_session_count}")
    click.echo(f"completed_partitions: {summary.completed_partition_count}")
    click.echo(f"stop_reason: {summary.stop_reason}")
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

