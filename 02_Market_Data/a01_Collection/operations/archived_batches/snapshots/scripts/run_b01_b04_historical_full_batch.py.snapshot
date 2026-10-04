"""执行本次固定的 b01 历史维护与 b01-b04 正式补缺批次。"""

from __future__ import annotations

import argparse
import pathlib

import run_b01_b04_full_update as worker
import run_b01_b04_production_update as production


EXPECTED_STAGE_NAMES = (
    "b01/c01_trade_calendar",
    "b01/c02_futures_variety_calendar",
    "b01/c03_futures_contract_calendar",
    "b01/c04_futures_bar_calendar",
    "b01/c05_futures_daily",
    "b01/c06_futures_minute",
    "b01/c07_suspected_session_reconciliation",
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
FULL_B01_CALENDAR_STAGE_NAMES = frozenset(EXPECTED_STAGE_NAMES[:4])

production_stage_names = tuple(
    stage_name for stage_name, _entrypoint_path, _supports_dates
    in production.PRODUCTION_STAGES
)
if production_stage_names != EXPECTED_STAGE_NAMES:
    raise RuntimeError(
        "生产入口清单已经变化；必须人工复核本次固定全量批次后再运行。"
    )

full_batch_stages = []
for stage_name, entrypoint_path, _supports_dates in production.PRODUCTION_STAGES:
    extra_arguments = [
        "--entrypoint-path",
        str(entrypoint_path),
    ]
    if stage_name in FULL_B01_CALENDAR_STAGE_NAMES:
        extra_arguments.append("--full")
    full_batch_stages.append((
        stage_name,
        production.INVOKER_PATH,
        False,
        tuple(extra_arguments),
    ))
FULL_BATCH_STAGES = tuple(full_batch_stages)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-root", type=pathlib.Path, required=True)
    args = parser.parse_args()

    worker.STAGES = FULL_BATCH_STAGES
    return worker.run_job(
        "formal",
        args.run_root,
        None,
        None,
        require_monitor=True,
    )


if __name__ == "__main__":
    raise SystemExit(main())
