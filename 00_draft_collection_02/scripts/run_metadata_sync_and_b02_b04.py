"""先同步当前 silver Parquet metadata，再继续 b02-b04 正式补缺。

这是经用户授权的单批恢复包装器。它复用现有 worker 的状态、心跳与
可见 monitor 门禁，不运行 b01，也不自动重试 metadata 事务或业务阶段。
"""

from __future__ import annotations

import argparse
import pathlib

import run_b01_b04_full_update as worker
import run_b01_b04_production_update as production


METADATA_SYNC_PATH = pathlib.Path(__file__).with_name(
    "sync_current_silver_parquet_metadata.py"
)
EXPECTED_DOWNSTREAM_STAGE_NAMES = (
    "b02/c01_exchange_report_calendar",
    "b02/c01a_position_rank_special_case_calibration",
    "b02/c02_futures_holding_reports",
    "b02/c03_warehouse_receipt",
    "b03/c01_external_market_calendar",
    "b03/c02_domestic_spot_basis",
    "b03/c03_overseas_futures",
    "b03/c04_external_index",
    "b04/c01_macro_release_calendar",
    "b04/c02_interest_rate",
    "b04/c03_macro_release",
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-root", type=pathlib.Path, required=True)
    args = parser.parse_args()

    downstream_stages = tuple(
        stage
        for stage in production.PRODUCTION_STAGES
        if stage[0].split("/", maxsplit=1)[0] in {"b02", "b03", "b04"}
    )
    downstream_stage_names = tuple(
        stage_name
        for stage_name, _entrypoint_path, _supports_dates in downstream_stages
    )
    if downstream_stage_names != EXPECTED_DOWNSTREAM_STAGE_NAMES:
        raise RuntimeError(
            "B02-B04 生产入口清单已经变化；必须人工复核恢复批次后再运行。"
        )

    run_root = args.run_root.resolve()
    metadata_transaction_root = run_root / "metadata_transaction"
    recovery_stages = [
        (
            "metadata/sync_current_silver_parquet_metadata",
            METADATA_SYNC_PATH,
            False,
            (
                "--transaction-root",
                str(metadata_transaction_root),
            ),
        ),
    ]
    for stage_name, entrypoint_path, supports_dates in downstream_stages:
        recovery_stages.append((
            stage_name,
            production.INVOKER_PATH,
            supports_dates,
            (
                "--entrypoint-path",
                str(entrypoint_path),
            ),
        ))

    worker.PROGRESS_PREFIXES = (
        *worker.PROGRESS_PREFIXES,
        "metadata_plan:",
        "metadata_progress:",
    )
    worker.STAGES = tuple(recovery_stages)
    return worker.run_job(
        "formal",
        run_root,
        None,
        None,
        require_monitor=True,
    )


if __name__ == "__main__":
    raise SystemExit(main())
