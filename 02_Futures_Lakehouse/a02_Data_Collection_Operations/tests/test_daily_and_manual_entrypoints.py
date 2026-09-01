from __future__ import annotations

import importlib.util
import itertools
import io
import pathlib
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from unittest import mock


OPERATIONS_ROOT = pathlib.Path(__file__).resolve().parents[1]
PROJECT_ROOT = OPERATIONS_ROOT.parents[1]
sys.path.insert(0, str(OPERATIONS_ROOT))

import invoke_exported_click_entrypoint as invoker  # noqa: E402
import run_daily_update as daily  # noqa: E402


APPROVED_DAILY_STAGE_ENTRYPOINTS = (
    (
        "b01/c01_trade_calendar",
        "b01_Futures_Market_Data/c01_trade_calendar.py",
    ),
    (
        "b01/c02_futures_variety_calendar",
        "b01_Futures_Market_Data/c02_futures_variety_calendar.py",
    ),
    (
        "b01/c03_futures_contract_calendar",
        "b01_Futures_Market_Data/c03_futures_contract_calendar.py",
    ),
    (
        "b01/c04_futures_bar_calendar",
        "b01_Futures_Market_Data/c04_futures_bar_calendar.py",
    ),
    (
        "b01/c05_futures_daily",
        "b01_Futures_Market_Data/c05_futures_daily.py",
    ),
    (
        "b01/c06_futures_minute",
        "b01_Futures_Market_Data/c06_futures_minute.py",
    ),
    (
        "b01/c07_suspected_session_reconciliation",
        "b01_Futures_Market_Data/c07_suspected_session_reconciliation.py",
    ),
    (
        "b02/c01_exchange_report_calendar",
        "b02_Futures_Exchange_Reports/c01_exchange_report_calendar.py",
    ),
    (
        "b02/c01a_position_rank_special_case_calibration",
        "b02_Futures_Exchange_Reports/c01a_position_rank_special_case_calibration.py",
    ),
    (
        "b02/c02_futures_holding_reports",
        "b02_Futures_Exchange_Reports/c02_futures_holding_reports.py",
    ),
    (
        "b02/c03_warehouse_receipt",
        "b02_Futures_Exchange_Reports/c03_warehouse_receipt.py",
    ),
    (
        "b03/c01_external_market_calendar",
        "b03_External_Market_Data/c01_external_market_calendar.py",
    ),
    (
        "b03/c02_domestic_spot_basis",
        "b03_External_Market_Data/c02_domestic_spot_basis.py",
    ),
    (
        "b03/c03_overseas_futures",
        "b03_External_Market_Data/c03_overseas_futures.py",
    ),
    (
        "b03/c04_external_index",
        "b03_External_Market_Data/c04_external_index.py",
    ),
    (
        "b04/c01_macro_release_calendar",
        "b04_Macro_And_Interest_Rates/c01_macro_release_calendar.py",
    ),
    (
        "b04/c02_interest_rate",
        "b04_Macro_And_Interest_Rates/c02_interest_rate.py",
    ),
    (
        "b04/c03_macro_release",
        "b04_Macro_And_Interest_Rates/c03_macro_release.py",
    ),
)
APPROVED_DAILY_STAGE_NAMES = tuple(
    stage_name
    for stage_name, _ in APPROVED_DAILY_STAGE_ENTRYPOINTS
)
APPROVED_GROUP_COUNTS = {"b01": 7, "b02": 4, "b03": 4, "b04": 3}


def load_manual_module():
    module_path = (
        OPERATIONS_ROOT
        / "b02_Manual_Maintenance"
        / "run_c03_contract_calendar_full_update.py"
    )
    spec = importlib.util.spec_from_file_location("manual_c03_full", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class DailyAndManualEntrypointTests(unittest.TestCase):
    def test_default_daily_manifest_has_exact_18_stage_order(self) -> None:
        stages = daily.build_daily_stages()
        self.assertEqual(len(stages), 18)
        self.assertEqual(
            [stage.name for stage in stages],
            list(APPROVED_DAILY_STAGE_NAMES),
        )
        self.assertFalse(any("c08" in stage.name.lower() for stage in stages))
        for stage in stages:
            self.assertEqual(stage.entrypoint_path, daily.INVOKER_PATH)
            self.assertEqual(stage.arguments[0], "--entrypoint-path")
            self.assertEqual(stage.arguments[-1], "--write")
        for stage, (
            expected_stage_name,
            expected_entrypoint_relative_path,
        ) in zip(stages, APPROVED_DAILY_STAGE_ENTRYPOINTS, strict=True):
            self.assertEqual(stage.name, expected_stage_name)
            self.assertEqual(
                pathlib.Path(stage.arguments[1]),
                daily.COLLECTION_ROOT / expected_entrypoint_relative_path,
            )

    def test_groups_and_optional_quality_only_change_explicit_selection(self) -> None:
        for group_count in range(1, len(daily.GROUP_NAMES) + 1):
            for selected_groups in itertools.combinations(
                daily.GROUP_NAMES,
                group_count,
            ):
                with self.subTest(selected_groups=selected_groups):
                    stages = daily.build_daily_stages(groups=selected_groups)
                    expected_names = [
                        stage_name
                        for stage_name in APPROVED_DAILY_STAGE_NAMES
                        if stage_name.split("/", maxsplit=1)[0]
                        in selected_groups
                    ]
                    self.assertEqual(
                        [stage.name for stage in stages],
                        expected_names,
                    )
                    self.assertEqual(
                        len(stages),
                        sum(
                            APPROVED_GROUP_COUNTS[group]
                            for group in selected_groups
                        ),
                    )
                    self.assertEqual(
                        {
                            stage.name.split("/", maxsplit=1)[0]
                            for stage in stages
                        },
                        set(selected_groups),
                    )
        skipped_stages = daily.build_daily_stages(skip_optional_quality=True)
        self.assertEqual(len(skipped_stages), 17)
        self.assertNotIn(
            daily.OPTIONAL_QUALITY_STAGE,
            [stage.name for stage in skipped_stages],
        )
        self.assertEqual(
            [
                name
                for name in APPROVED_DAILY_STAGE_NAMES
                if name != daily.OPTIONAL_QUALITY_STAGE
            ],
            [stage.name for stage in skipped_stages],
        )

    def test_warehouse_performance_arguments_are_paired_and_local(self) -> None:
        stages = daily.build_daily_stages(
            warehouse_performance_window_size=50,
            warehouse_performance_max_median_seconds=15.4,
        )
        warehouse_stage = next(
            stage for stage in stages if stage.name == daily.WAREHOUSE_STAGE
        )
        self.assertEqual(
            warehouse_stage.arguments[-5:],
            (
                "--performance-window-size",
                "50",
                "--performance-max-median-seconds",
                "15.4",
                "--write",
            ),
        )
        for stage in stages:
            if stage.name != daily.WAREHOUSE_STAGE:
                self.assertNotIn("--performance-window-size", stage.arguments)

        invalid_cases = (
            {"warehouse_performance_window_size": 50},
            {"warehouse_performance_max_median_seconds": 15.4},
            {
                "warehouse_performance_window_size": 0,
                "warehouse_performance_max_median_seconds": 15.4,
            },
            {
                "groups": ("b01",),
                "warehouse_performance_window_size": 50,
                "warehouse_performance_max_median_seconds": 15.4,
            },
            {
                "warehouse_performance_window_size": 50,
                "warehouse_performance_max_median_seconds": float("nan"),
            },
            {
                "warehouse_performance_window_size": 50,
                "warehouse_performance_max_median_seconds": float("inf"),
            },
            {
                "warehouse_performance_window_size": 50,
                "warehouse_performance_max_median_seconds": float("-inf"),
            },
        )
        for keyword_arguments in invalid_cases:
            with self.subTest(keyword_arguments=keyword_arguments):
                with self.assertRaises(ValueError):
                    daily.build_daily_stages(**keyword_arguments)

        for non_finite_text in ("nan", "inf", "-inf"):
            with self.subTest(cli_value=non_finite_text):
                cli_error = io.StringIO()
                with redirect_stderr(cli_error), self.assertRaises(
                    SystemExit
                ) as rejected:
                    daily.parse_args(
                        [
                            "--run-root",
                            str(daily.OPERATIONS_ROOT / "run_history" / "x"),
                            "--warehouse-performance-window-size",
                            "50",
                            (
                                "--warehouse-performance-max-median-seconds="
                                + non_finite_text
                            ),
                        ]
                    )
                self.assertEqual(rejected.exception.code, 2)
                self.assertIn("必须是有限正数", cli_error.getvalue())

    def test_daily_cli_requires_run_root_and_exposes_no_draft_controls(self) -> None:
        missing_error = io.StringIO()
        with redirect_stderr(missing_error), self.assertRaises(
            SystemExit
        ) as missing_run_root:
            daily.parse_args([])
        self.assertEqual(missing_run_root.exception.code, 2)
        self.assertIn("--run-root", missing_error.getvalue())

        forbidden_options = {
            "--mode": "formal",
            "--sample-start": "2026-08-01",
            "--sample-end": "2026-08-02",
            "--sample-lake-root": "E:/test-lake",
            "--test-lake": "E:/test-lake",
            "--lake-root": "E:/test-lake",
            "--start-date": "2026-08-01",
            "--end-date": "2026-08-02",
            "--start-stage": "2",
            "--require-monitor": None,
        }
        for forbidden_option, example_value in forbidden_options.items():
            with self.subTest(option=forbidden_option):
                argv = [
                    "--run-root",
                    str(daily.OPERATIONS_ROOT / "run_history" / "x"),
                    forbidden_option,
                ]
                if example_value is not None:
                    argv.append(example_value)
                forbidden_error = io.StringIO()
                with redirect_stderr(forbidden_error), self.assertRaises(
                    SystemExit
                ) as rejected:
                    daily.parse_args(
                        argv
                    )
                self.assertEqual(rejected.exception.code, 2)
                self.assertIn(
                    f"unrecognized arguments: {forbidden_option}",
                    forbidden_error.getvalue(),
                )

    def test_daily_main_delegates_only_explicit_immutable_manifest(self) -> None:
        run_root = daily.OPERATIONS_ROOT / "run_history" / "unit-main"
        with mock.patch.object(daily, "run_batch", return_value=7) as run_batch:
            exit_code = daily.main(
                [
                    "--run-root",
                    str(run_root),
                    "--groups",
                    "b04",
                ]
            )
        self.assertEqual(exit_code, 7)
        keyword_arguments = run_batch.call_args.kwargs
        self.assertEqual(
            keyword_arguments["operation_name"],
            "daily_collection_update",
        )
        self.assertEqual(keyword_arguments["run_root"], run_root)
        self.assertIsInstance(keyword_arguments["stages"], tuple)
        self.assertEqual(len(keyword_arguments["stages"]), 3)

    def test_generic_invoker_restricts_collection_tree_and_c08(self) -> None:
        valid_entrypoint = (
            invoker.COLLECTION_ROOT
            / "b01_Futures_Market_Data"
            / "c01_trade_calendar.py"
        )
        argv = ["invoker.py", "--entrypoint-path", str(valid_entrypoint), "--write"]
        self.assertEqual(invoker.selected_entrypoint(argv), valid_entrypoint)
        self.assertEqual(argv, ["invoker.py", "--write"])

        outside_argv = [
            "invoker.py",
            "--entrypoint-path",
            str(OPERATIONS_ROOT / "background_worker.py"),
        ]
        with self.assertRaises(ValueError):
            invoker.selected_entrypoint(outside_argv)

        c08_entrypoint = (
            invoker.COLLECTION_ROOT
            / "b01_Futures_Market_Data"
            / "c08_full_minute_quality.py"
        )
        with self.assertRaisesRegex(ValueError, "c08"):
            invoker.selected_entrypoint(
                ["invoker.py", "--entrypoint-path", str(c08_entrypoint)]
            )

    def test_manual_category_has_only_formal_c03_full_operation(self) -> None:
        manual = load_manual_module()
        self.assertEqual(len(manual.MANUAL_STAGES), 1)
        stage = manual.MANUAL_STAGES[0]
        self.assertEqual(
            stage.name,
            "b01/c03_futures_contract_calendar_full",
        )
        self.assertEqual(
            stage.entrypoint_path,
            OPERATIONS_ROOT / "invoke_exported_click_entrypoint.py",
        )
        self.assertEqual(stage.arguments[-2:], ("--full", "--write"))
        self.assertNotIn("--start-date", stage.arguments)
        self.assertNotIn("--end-date", stage.arguments)

        manual_error = io.StringIO()
        with redirect_stderr(manual_error), self.assertRaises(
            SystemExit
        ) as missing_run_root:
            manual.parse_args([])
        self.assertEqual(missing_run_root.exception.code, 2)

        run_root = OPERATIONS_ROOT / "run_history" / "unit-manual"
        with mock.patch.object(manual, "run_batch", return_value=9) as run_batch:
            self.assertEqual(
                manual.main(["--run-root", str(run_root)]),
                9,
            )
        self.assertEqual(
            run_batch.call_args.kwargs["operation_name"],
            "c03_contract_calendar_full_maintenance",
        )


if __name__ == "__main__":
    unittest.main()
