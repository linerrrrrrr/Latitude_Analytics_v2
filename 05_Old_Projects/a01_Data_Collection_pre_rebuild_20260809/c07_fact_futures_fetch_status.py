#!/usr/bin/env python
# coding: utf-8

# # c07_fact_futures_fetch_status
# 
# 由品种日历、合约 Session 日历和独立的 Session 日历信号维表生成日线/分钟线拉取计划。推断信号不豁免拉取；只有 `authoritative + is_fetch_exempt=True` 才能影响 `is_fetch_required`。`bar_frequency` 是第一层 Hive 分区。
# 
# 本 Notebook 是该业务工作流的唯一可编辑源文件；同名 `.py` 由项目标准 `latitude` 环境中的默认 PythonExporter 完整生成。

# In[ ]:


"""建立期货日线/分钟线拉取状态表。"""

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

from c00_futures_fetch_control import rebuild_fetch_status  # noqa: E402

LAKE_ROOT = PROJECT_ROOT / "03_Futures_Database" / "futures_lake" / "silver"
VARIETY_CALENDAR_PATH = LAKE_ROOT / "dim_futures_variety_calendar"
CONTRACT_CALENDAR_PATH = LAKE_ROOT / "dim_futures_contract_calendar"
SCHEDULE_SIGNAL_PATH = LAKE_ROOT / "dim_futures_session_schedule_signal"
TABLE_PATH = LAKE_ROOT / "fact_futures_fetch_status"


# In[ ]:


def update_fetch_status(
    variety_calendar_path: Path = VARIETY_CALENDAR_PATH,
    contract_calendar_path: Path = CONTRACT_CALENDAR_PATH,
    schedule_signal_path: Path = SCHEDULE_SIGNAL_PATH,
    table_path: Path = TABLE_PATH,
) -> tuple[int, int]:
    """流式全量重建拉取要求，同时保留仍有效的完成和缺失状态。"""
    return rebuild_fetch_status(
        variety_calendar_path, contract_calendar_path, schedule_signal_path, table_path
    )


# In[ ]:


@click.command()
@click.option("--variety-calendar-path", type=click.Path(path_type=Path), default=VARIETY_CALENDAR_PATH)
@click.option("--contract-calendar-path", type=click.Path(path_type=Path), default=CONTRACT_CALENDAR_PATH)
@click.option("--schedule-signal-path", type=click.Path(path_type=Path), default=SCHEDULE_SIGNAL_PATH)
@click.option("--table-path", type=click.Path(path_type=Path), default=TABLE_PATH)
def main(
    variety_calendar_path: Path,
    contract_calendar_path: Path,
    schedule_signal_path: Path,
    table_path: Path,
) -> None:
    """重建日线/分钟线应拉取状态。"""
    row_count, required_count = update_fetch_status(
        variety_calendar_path, contract_calendar_path, schedule_signal_path, table_path
    )
    click.echo(f"row_count: {row_count}")
    click.echo(f"required_count: {required_count}")
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

