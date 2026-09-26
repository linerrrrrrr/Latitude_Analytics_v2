from __future__ import annotations

import ast
import pathlib
import tempfile
import types
import unittest
from datetime import date, datetime, timedelta, timezone
from unittest import mock

import nbformat
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from click.testing import CliRunner
from nbconvert.exporters import PythonExporter


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
NOTEBOOK_PATH = (
    PROJECT_ROOT
    / "02_Futures_Lakehouse"
    / "a02_Futures_Exchange_Reports"
    / "b03_warehouse_receipt.ipynb"
)


def load_notebook_module() -> tuple[types.ModuleType, str]:
    notebook = nbformat.read(NOTEBOOK_PATH, as_version=4)
    nbformat.validate(notebook)
    source, _ = PythonExporter().from_notebook_node(notebook)
    compile(source, str(NOTEBOOK_PATH), "exec")

    module = types.ModuleType("b03_warehouse_receipt_test_module")
    module.__file__ = str(NOTEBOOK_PATH.with_suffix(".py"))
    exec(compile(source, module.__file__, "exec"), module.__dict__)
    return module, source


MODULE, EXPORTED_SOURCE = load_notebook_module()


def exported_function_source(function_name: str) -> str:
    syntax_tree = ast.parse(EXPORTED_SOURCE)
    for node in syntax_tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            source = ast.get_source_segment(EXPORTED_SOURCE, node)
            if source is None:
                raise AssertionError(f"无法提取导出函数源码：{function_name}")
            return source
    raise AssertionError(f"Notebook 导出中缺少函数：{function_name}")


def calendar_frame(
    trading_date: date,
    *,
    exchange_code: str = "XDCE",
    underlying_code: str = "EB",
    is_fetch_completed: bool = False,
    fetch_result_status: str = "pending",
    actual_record_count: int = 0,
    quality_status: str = "pending",
) -> pd.DataFrame:
    checked_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    is_empty = fetch_result_status == "empty_confirmed"
    row = {
        "dataset_name": "warehouse_receipt",
        "exchange_code": exchange_code,
        "underlying_code": underlying_code,
        "trading_date": trading_date,
        "is_fetch_required": True,
        "requirement_reason": "当前交易所品种交易日需要仓单日报。",
        "is_fetch_completed": is_fetch_completed,
        "fetch_result_status": fetch_result_status,
        "is_data_missing": is_empty,
        "expected_record_count": 1,
        "actual_record_count": actual_record_count,
        "quality_status": quality_status,
        "quality_reason": (
            "等待采集仓单日报。"
            if not is_fetch_completed
            else "已形成可信完成快照。"
        ),
        "fetch_run_id": "completed-run" if is_fetch_completed else None,
        "fetch_completed_at": checked_at if is_fetch_completed else None,
        "quality_checked_at": (
            checked_at
            if quality_status in {"passed", "warning", "failed"}
            else None
        ),
        "updated_at": checked_at,
        "year": trading_date.year,
        "month": trading_date.month,
    }
    table = MODULE.pandas_to_arrow(
        pd.DataFrame([row]),
        MODULE.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
    )
    return MODULE.arrow_to_pandas(
        table,
        MODULE.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
    )


def warehouse_frame(
    trading_date: date,
    *,
    exchange_code: str = "XDCE",
    underlying_code: str = "EB",
    warehouse_name: str = "大连一号库",
    quantity: float = 12.0,
    unit: str | None = "吨",
) -> pd.DataFrame:
    row = {
        "trading_date": trading_date,
        "exchange_code": exchange_code,
        "underlying_code": underlying_code,
        "warehouse_name": warehouse_name,
        "warehouse_receipt_number": quantity,
        "warehouse_receipt_unit": unit,
        "warehouse_receipt_number_change": 1.0,
        "source": MODULE.SOURCE,
        "updated_at": datetime.now(timezone.utc) - timedelta(seconds=1),
        "year": trading_date.year,
        "month": trading_date.month,
    }
    table = MODULE.pandas_to_arrow(
        pd.DataFrame([row]),
        MODULE.FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
    )
    return MODULE.arrow_to_pandas(
        table,
        MODULE.FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
    )


def valid_raw_response(
    trading_date: date,
    *,
    exchange: str = "DCE",
    underlying_code: str = "EB",
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "day": trading_date,
                "exchange": exchange,
                "underlying_code": underlying_code,
                "warehouse_name": "大连一号库",
                "warehouse_receipt_number": 12.0,
                "unit": "吨",
                "warehouse_receipt_number_increase": 1.0,
            }
        ]
    )


def write_exact_dataset(
    table_path: pathlib.Path,
    frame: pd.DataFrame,
    schema: pa.Schema,
    partition_columns: list[str],
    partitioning: ds.Partitioning,
) -> None:
    table_path.mkdir(parents=True, exist_ok=True)
    file_schema = pa.schema(
        [field for field in schema if field.name not in partition_columns],
        metadata=schema.metadata,
    )
    pq.write_table(
        pa.Table.from_batches([], schema=file_schema),
        table_path / "schema.parquet",
    )
    if frame.empty:
        return
    ds.write_dataset(
        MODULE.pandas_to_arrow(frame.loc[:, schema.names], schema),
        table_path,
        format="parquet",
        partitioning=partitioning,
        existing_data_behavior="delete_matching",
        basename_template="part-{i}.parquet",
    )


def invoke_mocked_performance_batch(
    partition_count: int,
    clock_step: float,
) -> tuple[object, mock.Mock, mock.Mock]:
    calendar_leaves: dict[tuple[object, ...], pd.DataFrame] = {}
    planning_frames = []
    for offset in range(partition_count):
        year = 2020 + offset // 12
        month = 1 + offset % 12
        trading_date = date(year, month, 1)
        current_calendar_df = calendar_frame(trading_date)
        planning_frames.append(
            current_calendar_df.loc[:, MODULE.CALENDAR_PLANNING_COLUMNS]
        )
        calendar_leaves[("warehouse_receipt", "XDCE", year, month)] = (
            current_calendar_df
        )

    planning_df = pd.concat(planning_frames, ignore_index=True)
    planning_table = pa.Table.from_pandas(planning_df, preserve_index=False)
    planning_dataset = mock.Mock()
    planning_dataset.to_table.return_value = planning_table

    def read_leaf(
        table_path: pathlib.Path,
        schema: pa.Schema,
        partition_columns: list[str],
        partitioning: ds.Partitioning,
        partition_key: tuple[object, ...],
        label: str,
    ) -> pd.DataFrame:
        del table_path, partition_columns, partitioning, label
        if schema is MODULE.FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA:
            return MODULE.empty_pandas(
                MODULE.FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA
            )
        return calendar_leaves[partition_key].copy()

    clock_value = 0.0

    def perf_counter() -> float:
        nonlocal clock_value
        clock_value += clock_step
        return clock_value

    fact_commit = mock.Mock(return_value=0)
    calendar_commit = mock.Mock(return_value=1)
    with tempfile.TemporaryDirectory() as temporary_directory:
        formal_root = pathlib.Path(temporary_directory) / "formal-lake"
        test_settings = types.SimpleNamespace(
            futures_lake_root=formal_root,
            jqdata_id="test-id",
            jqdata_secret="test-secret",
        )
        with (
            mock.patch.object(MODULE, "settings", test_settings),
            mock.patch.object(
                MODULE,
                "open_planning_calendar_dataset",
                return_value=planning_dataset,
            ),
            mock.patch.object(MODULE, "validate_table_marker", return_value=False),
            mock.patch.object(MODULE, "authenticate_jqdata", return_value=object()),
            mock.patch.object(
                MODULE,
                "query_warehouse_grid",
                return_value=pd.DataFrame(columns=MODULE.JQDATA_FIELDS),
            ),
            mock.patch.object(MODULE, "read_partition_leaf", side_effect=read_leaf),
            mock.patch.object(MODULE, "commit_complete_partition", fact_commit),
            mock.patch.object(MODULE, "commit_calendar_partitions", calendar_commit),
            mock.patch.object(MODULE.time, "perf_counter", side_effect=perf_counter),
        ):
            result = CliRunner().invoke(
                MODULE.main,
                [
                    "--lake-root",
                    str(formal_root),
                    "--performance-window-size",
                    "50",
                    "--performance-max-median-seconds",
                    "15.4",
                    "--write",
                ],
            )
    return result, fact_commit, calendar_commit


class WarehouseReceiptTests(unittest.TestCase):
    def test_notebook_is_valid_and_export_loads_the_tested_module(self) -> None:
        self.assertIn("def normalize_warehouse_response", EXPORTED_SOURCE)
        self.assertIn("def performance_gate_result", EXPORTED_SOURCE)
        self.assertGreaterEqual(EXPORTED_SOURCE.count("# In["), 10)

    def test_normalize_accepts_exact_grid_and_preserves_missing_unit_warning_input(
        self,
    ) -> None:
        trading_date = date(2024, 3, 1)
        grid = {
            "exchange_code": "XDCE",
            "underlying_code": "EB",
            "trading_date": trading_date,
        }
        raw_df = valid_raw_response(trading_date)
        raw_df.loc[0, "unit"] = None

        normalized_df = MODULE.normalize_warehouse_response(
            raw_df,
            grid,
            datetime.now(timezone.utc) - timedelta(seconds=1),
        )

        self.assertEqual(len(normalized_df), 1)
        self.assertEqual(normalized_df.loc[0, "exchange_code"], "XDCE")
        self.assertEqual(normalized_df.loc[0, "underlying_code"], "EB")
        self.assertTrue(pd.isna(normalized_df.loc[0, "warehouse_receipt_unit"]))

    def test_normalize_preserves_zero_row_zero_column_empty_response(self) -> None:
        trading_date = date(2024, 3, 1)
        normalized_df = MODULE.normalize_warehouse_response(
            pd.DataFrame(),
            {
                "exchange_code": "XDCE",
                "underlying_code": "EB",
                "trading_date": trading_date,
            },
            datetime.now(timezone.utc) - timedelta(seconds=1),
        )

        self.assertTrue(normalized_df.empty)
        self.assertEqual(
            list(normalized_df.columns),
            MODULE.FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA.names,
        )

    def test_default_pending_plan_trusts_completed_calendar_snapshot(self) -> None:
        completed_date = date(2024, 3, 1)
        empty_warning_date = date(2024, 3, 4)
        pending_date = date(2024, 3, 5)
        planning_df = pd.concat(
            [
                calendar_frame(
                    completed_date,
                    is_fetch_completed=True,
                    fetch_result_status="success",
                    # 故意使用与任何事实行数无关的值，证明规划不重证 clean 历史。
                    actual_record_count=999,
                    quality_status="passed",
                ),
                calendar_frame(
                    empty_warning_date,
                    is_fetch_completed=True,
                    fetch_result_status="empty_confirmed",
                    actual_record_count=0,
                    quality_status="warning",
                ),
                calendar_frame(pending_date),
            ],
            ignore_index=True,
        )

        pending_df, complete_count = MODULE.pending_report_grids(planning_df)

        self.assertEqual(complete_count, 2)
        self.assertEqual(pending_df["trading_date"].tolist(), [pending_date])

    def test_calendar_validator_accepts_completed_grid_outside_current_policy(self) -> None:
        historical_df = calendar_frame(
            date(2024, 3, 1),
            is_fetch_completed=True,
            fetch_result_status="success",
            actual_record_count=1,
            quality_status="passed",
        )
        historical_df.loc[0, "is_fetch_required"] = False
        historical_df.loc[0, "requirement_reason"] = (
            "当前政策排除，保留历史完成凭证。"
        )
        historical_df.loc[0, "is_data_missing"] = False

        validated_df = MODULE.validate_calendar_frame(historical_df, "测试")
        self.assertFalse(validated_df.loc[0, "is_fetch_required"])
        self.assertTrue(validated_df.loc[0, "is_fetch_completed"])

        invalid_df = historical_df.copy()
        invalid_df.loc[0, "is_fetch_completed"] = False
        with self.assertRaisesRegex(ValueError, "not_required"):
            MODULE.validate_calendar_frame(invalid_df, "测试")

    def test_authoritative_metadata_matches_default_snapshot_plan(self) -> None:
        update_mode = MODULE.FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA.metadata[
            b"update_mode_zh"
        ].decode("utf-8")
        self.assertIn("required 且 is_fetch_completed=false", update_mode)
        self.assertIn("不扫描 clean 事实历史", update_mode)

    def test_leaf_reader_never_enumerates_or_opens_table_root(self) -> None:
        march_date = date(2024, 3, 1)
        april_date = date(2024, 4, 1)
        partition_key = ("XDCE", "EB", 2024, 3)
        with tempfile.TemporaryDirectory() as temporary_directory:
            table_path = pathlib.Path(temporary_directory) / MODULE.TABLE_NAME
            write_exact_dataset(
                table_path,
                pd.concat(
                    [warehouse_frame(march_date), warehouse_frame(april_date)],
                    ignore_index=True,
                ),
                MODULE.FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
                MODULE.PARTITION_COLUMNS,
                MODULE.FACT_PARTITIONING,
            )
            expected_leaf_path = (
                table_path
                / MODULE.partition_relative_path(
                    MODULE.PARTITION_COLUMNS,
                    partition_key,
                )
            ).resolve()
            original_dataset = MODULE.ds.dataset
            with mock.patch.object(
                MODULE.ds,
                "dataset",
                wraps=original_dataset,
            ) as dataset_mock:
                read_df = MODULE.read_partition_leaf(
                    table_path,
                    MODULE.FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
                    MODULE.PARTITION_COLUMNS,
                    MODULE.FACT_PARTITIONING,
                    partition_key,
                    "测试仓单事实",
                )

        self.assertEqual(read_df["trading_date"].tolist(), [march_date])
        opened_paths = [
            pathlib.Path(call.args[0]).resolve()
            for call in dataset_mock.call_args_list
        ]
        self.assertEqual(opened_paths, [expected_leaf_path])
        leaf_reader_source = exported_function_source("read_partition_leaf")
        self.assertIn('leaf_path.glob("*.parquet")', leaf_reader_source)
        self.assertNotIn(".rglob(", leaf_reader_source)

    def test_dirty_fact_validator_runs_once_and_commit_uses_pk_summaries(
        self,
    ) -> None:
        trading_date = date(2024, 3, 1)
        partition_key = ("XDCE", "EB", 2024, 3)
        existing_df = MODULE.empty_pandas(
            MODULE.FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA
        )
        incoming_df = warehouse_frame(trading_date)
        original_validator = MODULE.validate_warehouse_frame
        original_summary_reader = MODULE.read_leaf_primary_key_summary

        with tempfile.TemporaryDirectory() as temporary_directory, mock.patch.object(
            MODULE,
            "validate_warehouse_frame",
            wraps=original_validator,
        ) as validator_mock, mock.patch.object(
            MODULE,
            "read_leaf_primary_key_summary",
            wraps=original_summary_reader,
        ) as summary_mock:
            validated_df = MODULE.full_fact_partition(
                existing_df,
                incoming_df,
                {trading_date},
            )
            committed_count = MODULE.commit_complete_partition(
                validated_df,
                pathlib.Path(temporary_directory),
                partition_key,
            )

        self.assertEqual(committed_count, 1)
        self.assertEqual(validator_mock.call_count, 1)
        self.assertEqual(
            validator_mock.call_args.args[1],
            "合并后的完整仓单分区",
        )
        self.assertEqual(summary_mock.call_count, 2)
        summary_labels = [call.args[-1] for call in summary_mock.call_args_list]
        self.assertEqual(summary_labels, ["仓单 staging", "正式仓单事实"])

    def test_dirty_calendar_validator_runs_once_and_commit_does_not_repeat(
        self,
    ) -> None:
        trading_date = date(2024, 3, 1)
        pending_df = calendar_frame(trading_date)
        grid_key = ("XDCE", "EB", trading_date)
        partition_key = ("warehouse_receipt", "XDCE", 2024, 3)
        original_validator = MODULE.validate_calendar_frame
        original_summary_reader = MODULE.read_leaf_primary_key_summary

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            calendar_path = lake_root / "silver" / MODULE.CALENDAR_TABLE_NAME
            write_exact_dataset(
                calendar_path,
                pending_df,
                MODULE.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
                MODULE.CALENDAR_PARTITION_COLUMNS,
                MODULE.CALENDAR_PARTITIONING,
            )
            with mock.patch.object(
                MODULE,
                "validate_calendar_frame",
                wraps=original_validator,
            ) as validator_mock, mock.patch.object(
                MODULE,
                "read_leaf_primary_key_summary",
                wraps=original_summary_reader,
            ) as summary_mock:
                validated_df = MODULE.apply_calendar_completion(
                    pending_df,
                    {grid_key: (1, 0)},
                    "test-run",
                    datetime.now(timezone.utc) - timedelta(milliseconds=1),
                )
                committed_count = MODULE.commit_calendar_partitions(
                    validated_df,
                    {grid_key},
                    lake_root,
                )

        self.assertEqual(committed_count, 1)
        self.assertEqual(validator_mock.call_count, 1)
        self.assertEqual(
            validator_mock.call_args.args[1],
            "仓单完成状态回写后的",
        )
        self.assertEqual(summary_mock.call_count, 2)
        summary_labels = [call.args[-1] for call in summary_mock.call_args_list]
        self.assertEqual(summary_labels, ["报告日历 staging", "正式报告日历"])

    def test_fact_second_move_failure_restores_previous_leaf(self) -> None:
        trading_date = date(2024, 3, 1)
        partition_key = ("XDCE", "EB", 2024, 3)

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            old_df = MODULE.full_fact_partition(
                MODULE.empty_pandas(
                    MODULE.FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA
                ),
                warehouse_frame(trading_date, quantity=12.0),
                {trading_date},
            )
            MODULE.commit_complete_partition(old_df, lake_root, partition_key)

            replacement_df = MODULE.full_fact_partition(
                old_df,
                warehouse_frame(trading_date, quantity=99.0),
                {trading_date},
            )
            original_move = MODULE.shutil.move
            move_count = 0

            def fail_staging_install(source: str, destination: str) -> str:
                nonlocal move_count
                move_count += 1
                if move_count == 2:
                    raise OSError("injected staging install failure")
                return original_move(source, destination)

            with mock.patch.object(
                MODULE.shutil,
                "move",
                side_effect=fail_staging_install,
            ):
                with self.assertRaisesRegex(OSError, "staging install"):
                    MODULE.commit_complete_partition(
                        replacement_df,
                        lake_root,
                        partition_key,
                    )

            restored_df = MODULE.read_partition_leaf(
                lake_root / "silver" / MODULE.TABLE_NAME,
                MODULE.FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
                MODULE.PARTITION_COLUMNS,
                MODULE.FACT_PARTITIONING,
                partition_key,
                "回滚后的仓单事实",
            )
            recovery_paths = [
                path
                for path in (lake_root / "silver").iterdir()
                if path.name.startswith(f".{MODULE.TABLE_NAME}.")
            ]

        self.assertEqual(move_count, 3)
        self.assertEqual(restored_df["warehouse_receipt_number"].tolist(), [12.0])
        self.assertEqual(recovery_paths, [])

    def test_normalize_rejects_source_shape_grid_and_value_violations(self) -> None:
        trading_date = date(2024, 3, 1)
        grid = {
            "exchange_code": "XDCE",
            "underlying_code": "EB",
            "trading_date": trading_date,
        }
        valid_df = valid_raw_response(trading_date)

        invalid_cases: list[tuple[str, object, str]] = []
        invalid_cases.append(("non_dataframe", object(), "未返回 DataFrame"))
        invalid_cases.append(
            (
                "missing_column",
                valid_df.drop(columns=["warehouse_name"]),
                "缺列",
            )
        )
        invalid_cases.append(
            (
                "wrong_date",
                valid_raw_response(date(2024, 3, 4)),
                "请求交易日之外",
            )
        )
        invalid_cases.append(
            (
                "wrong_exchange",
                valid_raw_response(trading_date, exchange="SHFE"),
                "交易所与待办日历不一致",
            )
        )
        invalid_cases.append(
            (
                "wrong_underlying",
                valid_raw_response(trading_date, underlying_code="EG"),
                "品种与待办日历不一致",
            )
        )
        negative_df = valid_df.copy()
        negative_df.loc[0, "warehouse_receipt_number"] = -1.0
        invalid_cases.append(("negative_quantity", negative_df, "有限且非负"))
        infinite_change_df = valid_df.copy()
        infinite_change_df.loc[0, "warehouse_receipt_number_increase"] = np.inf
        invalid_cases.append(("infinite_change", infinite_change_df, "变化必须为有限数"))
        limit_df = pd.concat([valid_df] * 5000, ignore_index=True)
        invalid_cases.append(("query_limit", limit_df, "run_query 上限"))

        for label, raw_value, message in invalid_cases:
            with self.subTest(label=label), self.assertRaisesRegex(
                (TypeError, ValueError),
                message,
            ):
                MODULE.normalize_warehouse_response(
                    raw_value,
                    grid,
                    datetime.now(timezone.utc) - timedelta(seconds=1),
                )

    def test_performance_gate_uses_only_first_complete_window(self) -> None:
        self.assertIsNone(
            MODULE.performance_gate_result([1.0] * 49, 50, 15.4)
        )
        self.assertEqual(
            MODULE.performance_gate_result([10.0] * 50 + [1000.0], 50, 15.4),
            (True, 10.0),
        )
        self.assertEqual(
            MODULE.performance_gate_result([15.5] * 50, 50, 15.4),
            (False, 15.5),
        )

    def test_performance_gate_passes_after_50_commits_and_continues(self) -> None:
        result, fact_commit, calendar_commit = invoke_mocked_performance_batch(
            partition_count=51,
            clock_step=0.1,
        )

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("performance_gate_passed: samples=50", result.output)
        self.assertIn("performance_samples=51", result.output)
        self.assertEqual(fact_commit.call_count, 51)
        self.assertEqual(calendar_commit.call_count, 51)

    def test_performance_gate_failure_stops_on_50th_safe_commit_boundary(
        self,
    ) -> None:
        result, fact_commit, calendar_commit = invoke_mocked_performance_batch(
            partition_count=51,
            clock_step=10.0,
        )

        self.assertEqual(result.exit_code, 1, result.output)
        self.assertIn("performance_gate_failed: samples=50", result.output)
        self.assertIn("partition_start: 50/51", result.output)
        self.assertNotIn("partition_start: 51/51", result.output)
        self.assertEqual(fact_commit.call_count, 50)
        self.assertEqual(calendar_commit.call_count, 50)

    def test_performance_options_must_be_paired_before_lake_io(self) -> None:
        lake_sentinel = mock.Mock(side_effect=AssertionError("不得读取数据湖"))
        with mock.patch.object(
            MODULE,
            "open_planning_calendar_dataset",
            lake_sentinel,
        ):
            size_only = CliRunner().invoke(
                MODULE.main,
                ["--performance-window-size", "50"],
            )
            threshold_only = CliRunner().invoke(
                MODULE.main,
                ["--performance-max-median-seconds", "15.4"],
            )

        self.assertEqual(size_only.exit_code, 2, size_only.output)
        self.assertEqual(threshold_only.exit_code, 2, threshold_only.output)
        lake_sentinel.assert_not_called()

    def test_performance_options_require_automatic_formal_write_before_lake_io(
        self,
    ) -> None:
        lake_sentinel = mock.Mock(side_effect=AssertionError("不得读取数据湖"))
        shared_options = [
            "--performance-window-size",
            "50",
            "--performance-max-median-seconds",
            "15.4",
        ]
        with mock.patch.object(
            MODULE,
            "open_planning_calendar_dataset",
            lake_sentinel,
        ):
            no_write = CliRunner().invoke(MODULE.main, shared_options)
            explicit_dates = CliRunner().invoke(
                MODULE.main,
                [
                    *shared_options,
                    "--write",
                    "--start-date",
                    "2024-03-01",
                    "--end-date",
                    "2024-03-01",
                ],
            )
            with tempfile.TemporaryDirectory() as temporary_directory:
                non_formal = CliRunner().invoke(
                    MODULE.main,
                    [
                        *shared_options,
                        "--lake-root",
                        str(pathlib.Path(temporary_directory) / "lake"),
                        "--write",
                    ],
                )

        self.assertEqual(no_write.exit_code, 2, no_write.output)
        self.assertEqual(explicit_dates.exit_code, 2, explicit_dates.output)
        self.assertEqual(non_formal.exit_code, 2, non_formal.output)
        self.assertIn("性能门槛只允许", no_write.output)
        self.assertIn("显式指定日期时禁止写入", explicit_dates.output)
        self.assertIn("性能门槛只允许", non_formal.output)
        lake_sentinel.assert_not_called()


if __name__ == "__main__":
    unittest.main()
