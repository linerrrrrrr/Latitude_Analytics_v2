"""按拉取状态校验分钟事实表与精确缺失明细。"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
WORKFLOW_DIR = PROJECT_ROOT / "02_Futures_Lakehouse"
sys.path.insert(0, str(WORKFLOW_DIR))

from c00_futures_fetch_control import (  # noqa: E402
    MISSING_PARTITIONING,
    STATUS_PARTITIONING,
)
from c00_lakehouse import hive_partitioning  # noqa: E402


LAKE_ROOT = PROJECT_ROOT / "03_Futures_Database" / "futures_lake" / "silver"
MINUTE_PARTITIONING = hive_partitioning(
    [
        pa.field("exchange_code", pa.string()),
        pa.field("underlying_code", pa.string()),
        pa.field("year", pa.int16()),
        pa.field("month", pa.int8()),
    ]
)


def main() -> None:
    status_dataset = ds.dataset(
        LAKE_ROOT / "fact_futures_fetch_status",
        format="parquet",
        partitioning=STATUS_PARTITIONING,
    )
    minute_required_filter = (
        (ds.field("bar_frequency") == "1m")
        & (ds.field("is_fetch_required") == True)  # noqa: E712
    )
    status_df = status_dataset.to_table(
        columns=[
            "exchange_code",
            "underlying_code",
            "is_fetch_completed",
            "is_data_missing",
            "expected_bar_count",
            "actual_bar_count",
            "missing_bar_count",
        ],
        filter=minute_required_filter,
    ).to_pandas()
    coverage_df = status_df.groupby(
        ["exchange_code", "underlying_code"], as_index=False
    ).agg(
        session_count=("expected_bar_count", "size"),
        completed_session_count=("is_fetch_completed", "sum"),
        expected_minutes=("expected_bar_count", "sum"),
        actual_minutes=("actual_bar_count", "sum"),
        missing_minutes=("missing_bar_count", "sum"),
    )

    minute_dataset = ds.dataset(
        LAKE_ROOT / "fact_futures_minute",
        format="parquet",
        partitioning=MINUTE_PARTITIONING,
    )
    fact_rows = minute_dataset.count_rows()
    fact_duplicate_count = minute_dataset.to_table(
        columns=["contract_code", "bar_at"]
    ).to_pandas().duplicated(["contract_code", "bar_at"]).sum()

    missing_dataset = ds.dataset(
        LAKE_ROOT / "fact_futures_missing_bar",
        format="parquet",
        partitioning=MISSING_PARTITIONING,
    )
    missing_detail_count = missing_dataset.count_rows(
        filter=ds.field("bar_frequency") == "1m"
    )
    checked_actual_count = int(
        status_df.loc[status_df["is_fetch_completed"], "actual_bar_count"].sum()
    )
    checked_missing_count = int(
        status_df.loc[status_df["is_fetch_completed"], "missing_bar_count"].sum()
    )

    if fact_duplicate_count:
        raise AssertionError(f"分钟事实表存在 {fact_duplicate_count} 个重复主键")
    if checked_missing_count != missing_detail_count:
        raise AssertionError("分钟缺失状态与精确缺失明细数量不一致")
    if checked_actual_count > fact_rows:
        raise AssertionError("状态表实际分钟数大于分钟事实表总行数")

    print(coverage_df.to_string(index=False))
    print(f"selected_variety_count={len(coverage_df)}")
    print(f"fact_row_count={fact_rows}")
    print(f"checked_actual_minutes={checked_actual_count}")
    print(f"missing_detail_count={missing_detail_count}")
    print("verification_ok=True")


if __name__ == "__main__":
    main()
