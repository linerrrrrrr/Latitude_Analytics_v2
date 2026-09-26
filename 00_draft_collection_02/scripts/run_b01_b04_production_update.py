"""执行选定采集组的生产入口；a01/b07 是唯一可选日常质检。"""

from __future__ import annotations

import argparse
import pathlib

import run_b01_b04_full_update as worker


INVOKER_PATH = pathlib.Path(__file__).with_name(
    "invoke_exported_click_entrypoint.py"
)

PRODUCTION_STAGES = (
    ("a01/b01_trade_calendar", worker.B01_ROOT / "b01_trade_calendar.py", True),
    (
        "a01/b02_futures_variety_calendar",
        worker.B01_ROOT / "b02_futures_variety_calendar.py",
        True,
    ),
    (
        "a01/b03_futures_contract_calendar",
        worker.B01_ROOT / "b03_futures_contract_calendar.py",
        True,
    ),
    (
        "a01/b04_futures_bar_calendar",
        worker.B01_ROOT / "b04_futures_bar_calendar.py",
        True,
    ),
    ("a01/b05_futures_daily", worker.B01_ROOT / "b05_futures_daily.py", True),
    ("a01/b06_futures_minute", worker.B01_ROOT / "b06_futures_minute.py", True),
    (
        "a01/b07_suspected_session_reconciliation",
        worker.B01_ROOT / "b07_suspected_session_reconciliation.py",
        True,
    ),
    (
        "a02/b01_exchange_report_calendar",
        worker.B02_ROOT / "b01_exchange_report_calendar.py",
        True,
    ),
    (
        "a02/b01a_position_rank_special_case_calibration",
        worker.B02_ROOT / "b01a_position_rank_special_case_calibration.py",
        False,
    ),
    (
        "a02/b02_futures_holding_reports",
        worker.B02_ROOT / "b02_futures_holding_reports.py",
        True,
    ),
    (
        "a02/b03_warehouse_receipt",
        worker.B02_ROOT / "b03_warehouse_receipt.py",
        True,
    ),
    (
        "a03/b01_external_market_calendar",
        worker.B03_ROOT / "b01_external_market_calendar.py",
        True,
    ),
    (
        "a03/b02_domestic_spot_basis",
        worker.B03_ROOT / "b02_domestic_spot_basis.py",
        True,
    ),
    (
        "a03/b03_overseas_futures",
        worker.B03_ROOT / "b03_overseas_futures.py",
        True,
    ),
    (
        "a03/b04_external_index",
        worker.B03_ROOT / "b04_external_index.py",
        True,
    ),
    (
        "a04/b01_macro_release_calendar",
        worker.B04_ROOT / "b01_macro_release_calendar.py",
        True,
    ),
    ("a04/b02_interest_rate", worker.B04_ROOT / "b02_interest_rate.py", True),
    ("a04/b03_macro_release", worker.B04_ROOT / "b03_macro_release.py", True),
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("sample", "formal"), required=True)
    parser.add_argument("--run-root", type=pathlib.Path, required=True)
    parser.add_argument("--sample-start")
    parser.add_argument("--sample-end")
    parser.add_argument("--require-monitor", action="store_true")
    parser.add_argument("--skip-optional-quality", action="store_true")
    parser.add_argument("--warehouse-performance-window-size", type=int)
    parser.add_argument("--warehouse-performance-max-median-seconds", type=float)
    parser.add_argument(
        "--groups",
        nargs="+",
        choices=("a01", "a02", "a03", "a04"),
        default=("a01", "a02", "a03", "a04"),
    )
    args = parser.parse_args()

    has_warehouse_performance_gate = (
        args.warehouse_performance_window_size is not None
    )
    if has_warehouse_performance_gate != (
        args.warehouse_performance_max_median_seconds is not None
    ):
        parser.error(
            "--warehouse-performance-window-size 与 "
            "--warehouse-performance-max-median-seconds 必须同时提供。"
        )
    if has_warehouse_performance_gate and args.mode != "formal":
        parser.error("仓单性能门槛只允许用于 formal 模式。")
    if has_warehouse_performance_gate and (
        args.warehouse_performance_window_size <= 0
        or args.warehouse_performance_max_median_seconds <= 0
    ):
        parser.error("仓单性能窗口和中位耗时上限必须大于零。")

    selected_groups = set(args.groups)
    selected_stages = tuple(
        stage
        for stage in PRODUCTION_STAGES
        if stage[0].split("/", maxsplit=1)[0] in selected_groups
    )
    if args.skip_optional_quality:
        selected_stages = tuple(
            stage
            for stage in selected_stages
            if stage[0] != "a01/b07_suspected_session_reconciliation"
        )

    worker_stages = []
    for stage_name, entrypoint_path, supports_dates in selected_stages:
        extra_arguments = [
            "--entrypoint-path",
            str(entrypoint_path),
        ]
        if (
            stage_name == "a02/b03_warehouse_receipt"
            and has_warehouse_performance_gate
        ):
            extra_arguments.extend([
                "--performance-window-size",
                str(args.warehouse_performance_window_size),
                "--performance-max-median-seconds",
                str(args.warehouse_performance_max_median_seconds),
            ])
        worker_stages.append((
            stage_name,
            INVOKER_PATH,
            supports_dates,
            tuple(extra_arguments),
        ))
    worker.STAGES = tuple(worker_stages)

    return worker.run_job(
        args.mode,
        args.run_root,
        args.sample_start,
        args.sample_end,
        require_monitor=args.require_monitor,
    )


if __name__ == "__main__":
    raise SystemExit(main())
