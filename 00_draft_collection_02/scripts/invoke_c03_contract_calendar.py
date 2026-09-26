"""显式调用导出的 c03 Click 入口，保留原始 CLI 参数。"""

from __future__ import annotations

import pathlib
import runpy


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
ENTRYPOINT_PATH = (
    PROJECT_ROOT
    / "02_Futures_Lakehouse"
    / "a01_Data_Collection"
    / "b01_Futures_Market_Data"
    / "c03_futures_contract_calendar.py"
)


if __name__ == "__main__":
    runpy.run_path(str(ENTRYPOINT_PATH))["main"]()
