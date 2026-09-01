"""Formal daily collection CLI for the fixed 18-stage b01-b04 manifest."""

from __future__ import annotations

import argparse
import math
import pathlib
import sys


project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        PROJECT_ROOT = candidate_root
        break
else:
    raise RuntimeError("未找到项目根目录")


OPERATIONS_ROOT = (
    PROJECT_ROOT
    / "02_Futures_Lakehouse"
    / "a02_Data_Collection_Operations"
)
sys.path.insert(0, str(OPERATIONS_ROOT))

from background_worker import StageSpec, run_batch  # noqa: E402


COLLECTION_ROOT = (
    PROJECT_ROOT / "02_Futures_Lakehouse" / "a01_Data_Collection"
)
B01_ROOT = COLLECTION_ROOT / "b01_Futures_Market_Data"
B02_ROOT = COLLECTION_ROOT / "b02_Futures_Exchange_Reports"
B03_ROOT = COLLECTION_ROOT / "b03_External_Market_Data"
B04_ROOT = COLLECTION_ROOT / "b04_Macro_And_Interest_Rates"
INVOKER_PATH = OPERATIONS_ROOT / "invoke_exported_click_entrypoint.py"
GROUP_NAMES = ("b01", "b02", "b03", "b04")
OPTIONAL_QUALITY_STAGE = "b01/c07_suspected_session_reconciliation"
WAREHOUSE_STAGE = "b02/c03_warehouse_receipt"


DAILY_ENTRYPOINTS = (
    ("b01/c01_trade_calendar", B01_ROOT / "c01_trade_calendar.py"),
    (
        "b01/c02_futures_variety_calendar",
        B01_ROOT / "c02_futures_variety_calendar.py",
    ),
    (
        "b01/c03_futures_contract_calendar",
        B01_ROOT / "c03_futures_contract_calendar.py",
    ),
    (
        "b01/c04_futures_bar_calendar",
        B01_ROOT / "c04_futures_bar_calendar.py",
    ),
    ("b01/c05_futures_daily", B01_ROOT / "c05_futures_daily.py"),
    ("b01/c06_futures_minute", B01_ROOT / "c06_futures_minute.py"),
    (
        OPTIONAL_QUALITY_STAGE,
        B01_ROOT / "c07_suspected_session_reconciliation.py",
    ),
    (
        "b02/c01_exchange_report_calendar",
        B02_ROOT / "c01_exchange_report_calendar.py",
    ),
    (
        "b02/c01a_position_rank_special_case_calibration",
        B02_ROOT / "c01a_position_rank_special_case_calibration.py",
    ),
    (
        "b02/c02_futures_holding_reports",
        B02_ROOT / "c02_futures_holding_reports.py",
    ),
    (WAREHOUSE_STAGE, B02_ROOT / "c03_warehouse_receipt.py"),
    (
        "b03/c01_external_market_calendar",
        B03_ROOT / "c01_external_market_calendar.py",
    ),
    (
        "b03/c02_domestic_spot_basis",
        B03_ROOT / "c02_domestic_spot_basis.py",
    ),
    (
        "b03/c03_overseas_futures",
        B03_ROOT / "c03_overseas_futures.py",
    ),
    ("b03/c04_external_index", B03_ROOT / "c04_external_index.py"),
    (
        "b04/c01_macro_release_calendar",
        B04_ROOT / "c01_macro_release_calendar.py",
    ),
    ("b04/c02_interest_rate", B04_ROOT / "c02_interest_rate.py"),
    ("b04/c03_macro_release", B04_ROOT / "c03_macro_release.py"),
)


def build_daily_stages(
    *,
    groups: tuple[str, ...] = GROUP_NAMES,
    skip_optional_quality: bool = False,
    warehouse_performance_window_size: int | None = None,
    warehouse_performance_max_median_seconds: float | None = None,
) -> tuple[StageSpec, ...]:
    if not isinstance(groups, tuple) or not groups:
        raise ValueError("groups 必须是非空不可变 tuple。")
    unknown_groups = set(groups) - set(GROUP_NAMES)
    if unknown_groups:
        raise ValueError(f"未知采集组：{sorted(unknown_groups)}")
    if len(groups) != len(set(groups)):
        raise ValueError("groups 不得重复。")

    has_performance_window = warehouse_performance_window_size is not None
    has_performance_limit = (
        warehouse_performance_max_median_seconds is not None
    )
    if has_performance_window != has_performance_limit:
        raise ValueError(
            "两个 warehouse 性能参数必须同时提供或同时省略。"
        )
    if has_performance_window and (
        warehouse_performance_window_size <= 0
        or warehouse_performance_max_median_seconds <= 0
        or not math.isfinite(warehouse_performance_max_median_seconds)
    ):
        raise ValueError("warehouse 性能窗口和中位耗时上限必须是有限正数。")
    if has_performance_window and "b02" not in groups:
        raise ValueError("未选择 b02 时不得提供 warehouse 性能参数。")

    selected_groups = set(groups)
    stages: list[StageSpec] = []
    for stage_name, collection_entrypoint in DAILY_ENTRYPOINTS:
        group_name = stage_name.split("/", maxsplit=1)[0]
        if group_name not in selected_groups:
            continue
        if skip_optional_quality and stage_name == OPTIONAL_QUALITY_STAGE:
            continue

        arguments = ["--entrypoint-path", str(collection_entrypoint)]
        if stage_name == WAREHOUSE_STAGE and has_performance_window:
            arguments.extend(
                [
                    "--performance-window-size",
                    str(warehouse_performance_window_size),
                    "--performance-max-median-seconds",
                    str(warehouse_performance_max_median_seconds),
                ]
            )
        arguments.append("--write")
        stages.append(
            StageSpec(
                name=stage_name,
                entrypoint_path=INVOKER_PATH,
                arguments=tuple(arguments),
            )
        )
    return tuple(stages)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="顺序执行显式选择的正式日常采集组。"
    )
    parser.add_argument("--run-root", type=pathlib.Path, required=True)
    parser.add_argument(
        "--groups",
        nargs="+",
        choices=GROUP_NAMES,
        default=GROUP_NAMES,
    )
    parser.add_argument("--skip-optional-quality", action="store_true")
    parser.add_argument("--warehouse-performance-window-size", type=int)
    parser.add_argument(
        "--warehouse-performance-max-median-seconds",
        type=float,
    )
    arguments = parser.parse_args(argv)

    try:
        arguments.stages = build_daily_stages(
            groups=tuple(arguments.groups),
            skip_optional_quality=arguments.skip_optional_quality,
            warehouse_performance_window_size=(
                arguments.warehouse_performance_window_size
            ),
            warehouse_performance_max_median_seconds=(
                arguments.warehouse_performance_max_median_seconds
            ),
        )
    except (TypeError, ValueError) as error:
        parser.error(str(error))
    return arguments


def main(argv: list[str] | None = None) -> int:
    arguments = parse_args(argv)
    return run_batch(
        operation_name="daily_collection_update",
        run_root=arguments.run_root,
        stages=arguments.stages,
    )


if __name__ == "__main__":
    raise SystemExit(main())
