"""验证品种日历由 date=None 完整合约目录构建及 c02 的上游消费边界。

本测试只验证 c02 行为，不重新定义 c01 的完整表级契约。
"""

from __future__ import annotations

import io
import json
import pathlib
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import date, datetime, time, timezone
from unittest import mock

import click
from click.testing import CliRunner
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds


project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()
for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        break
else:
    raise RuntimeError("未找到项目根目录")

PROJECT_ROOT = candidate_root
COLLECTION_DIR = PROJECT_ROOT / "R02_Market_Data/a01_Collection"
MARKET_WORKFLOW_DIR = COLLECTION_DIR / "b01_Futures_Market_Data"
sys.path.insert(0, str(COLLECTION_DIR))
sys.path.insert(0, str(MARKET_WORKFLOW_DIR))

from config.data_contracts import (  # noqa: E402
    FUTURES_VARIETY_CALENDAR_SCHEMA,
    TRADE_CALENDAR_SCHEMA,
    pandas_to_arrow,
)
import c02_futures_variety_calendar as variety_calendar  # noqa: E402
from config import jqdata_connection  # noqa: E402


TRADING_DATE = date(2024, 1, 3)
UPDATED_AT = datetime(2026, 8, 10, tzinfo=timezone.utc)


class FakeJQData:
    def __init__(self) -> None:
        self.calls: list[tuple[list[str], object]] = []

    def get_all_securities(
        self,
        types: list[str],
        *,
        date: object,
    ) -> pd.DataFrame:
        self.calls.append((types, date))
        codes = [
            "RB2405.XSGE",
            "A2405.XDCE",
            "AP2405.XZCE",
            "IF2403.CCFX",
            "RB8888.XSGE",
            "A9998.XDCE",
            "AP9999.XZCE",
            "RB.XSGE",
        ]
        return pd.DataFrame(
            {
                "start_date": [pd.Timestamp("2023-01-01").date()] * len(codes),
                "end_date": [pd.Timestamp("2024-05-15").date()] * len(codes),
            },
            index=pd.Index(codes, name=None),
        )


class FuturesVarietyCalendarCompleteCatalogTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        self.lake_root = pathlib.Path(temporary_directory.name)
        silver_root = self.lake_root / "silver"
        # 这些未被 c02 使用的 c01 专属字段刻意采用非正式值，用来确认消费者
        # 只确认物理 Schema，并信任生产者已经正式提交的主键、水位和业务语义。
        trade_frame = pd.DataFrame(
            [
                {
                    "calendar_date": TRADING_DATE,
                    "date_key": "20240103",
                    "is_trading_day": True,
                    "weekday": 3,
                    "is_weekend": False,
                    "source": "test_fixture",
                    "calendar_name": "China Futures",
                    "calendar_timezone": "Asia/Shanghai",
                    "effective_after": time(15, 0),
                    "updated_at": UPDATED_AT,
                    "year": 2024,
                }
            ],
            columns=TRADE_CALENDAR_SCHEMA.names,
        )
        trade_partitioning = ds.partitioning(
            pa.schema([TRADE_CALENDAR_SCHEMA.field("year")]),
            flavor="hive",
        )
        ds.write_dataset(
            pandas_to_arrow(trade_frame, TRADE_CALENDAR_SCHEMA),
            silver_root / "dim_trade_calendar",
            format="parquet",
            partitioning=trade_partitioning,
        )

    def test_complete_catalog_build_and_commit_do_not_use_fact_whitelist(self) -> None:
        fake_jqdata = FakeJQData()
        output = io.StringIO()
        with (
            redirect_stdout(output),
            mock.patch.object(
                jqdata_connection,
                "authenticate_jqdata",
                return_value=fake_jqdata,
            ),
        ):
            frame = variety_calendar.collect(
                self.lake_root,
                TRADING_DATE,
                TRADING_DATE,
            )

        self.assertEqual(fake_jqdata.calls, [(["futures"], None)])
        self.assertIn("phase=collect; status=started", output.getvalue())
        self.assertIn("phase=expand; completed=1; total=1", output.getvalue())
        self.assertIn("phase=collect; status=completed; rows=4", output.getvalue())
        expected_pairs = {
            ("XSGE", "RB"),
            ("XDCE", "A"),
            ("XZCE", "AP"),
            ("CCFX", "IF"),
        }
        self.assertEqual(
            set(
                frame[["exchange_code", "underlying_code"]].itertuples(
                    index=False,
                    name=None,
                )
            ),
            expected_pairs,
        )
        self.assertTrue(frame["active_contract_count"].eq(1).all())

        with redirect_stdout(output):
            committed_rows = variety_calendar.commit_partitions(
                frame,
                self.lake_root,
                TRADING_DATE,
                TRADING_DATE,
            )
        self.assertEqual(committed_rows, 4)
        self.assertIn("phase=commit; status=started", output.getvalue())
        self.assertIn("completed=4; total=4; rows=1", output.getvalue())
        self.assertIn(
            "committed: table=dim_futures_variety_calendar; status=completed; rows=4; partitions=4",
            output.getvalue(),
        )
        partitioning = ds.partitioning(
            pa.schema(
                [
                    FUTURES_VARIETY_CALENDAR_SCHEMA.field(name)
                    for name in ["exchange_code", "year", "month"]
                ]
            ),
            flavor="hive",
        )
        committed = ds.dataset(
            self.lake_root / "silver" / "dim_futures_variety_calendar",
            format="parquet",
            partitioning=partitioning,
        ).to_table(columns=FUTURES_VARIETY_CALENDAR_SCHEMA.names).to_pandas()
        self.assertEqual(
            set(
                committed[["exchange_code", "underlying_code"]].itertuples(
                    index=False,
                    name=None,
                )
            ),
            expected_pairs,
        )

    def test_collect_does_not_recheck_upstream_natural_date_coverage(self) -> None:
        fake_jqdata = FakeJQData()
        with mock.patch.object(
            jqdata_connection,
            "authenticate_jqdata",
            return_value=fake_jqdata,
        ):
            frame = variety_calendar.collect(
                self.lake_root,
                date(2024, 1, 2),
                date(2024, 1, 4),
            )

        # 测试湖只含 1 月 3 日；c02 直接消费该正式上游结果，不重建并比较逐日列表。
        self.assertEqual(fake_jqdata.calls, [(["futures"], None)])
        self.assertEqual(set(frame["trading_date"]), {TRADING_DATE})
        self.assertEqual(len(frame), 4)

    def test_empty_upstream_range_finishes_without_authentication(self) -> None:
        output = io.StringIO()
        with (
            redirect_stdout(output),
            mock.patch.object(jqdata_connection, "authenticate_jqdata") as authenticate,
        ):
            frame = variety_calendar.collect(
                self.lake_root, date(2024, 1, 6), date(2024, 1, 7),
            )

        authenticate.assert_not_called()
        self.assertTrue(frame.empty)
        self.assertEqual(list(frame.columns), FUTURES_VARIETY_CALENDAR_SCHEMA.names)
        self.assertIn(
            "phase=collect; status=completed; rows=0; trading_dates=0; api_calls=0",
            output.getvalue(),
        )

    def test_source_failure_propagates_without_collection_success(self) -> None:
        source_error = RuntimeError("injected catalog failure")
        fake_jqdata = mock.Mock()
        fake_jqdata.get_all_securities.side_effect = source_error
        output = io.StringIO()
        with (
            redirect_stdout(output),
            mock.patch.object(
                jqdata_connection, "authenticate_jqdata", return_value=fake_jqdata,
            ),
            self.assertRaises(RuntimeError) as raised,
        ):
            variety_calendar.collect(self.lake_root, TRADING_DATE, TRADING_DATE)

        self.assertIs(raised.exception, source_error)
        fake_jqdata.get_all_securities.assert_called_once_with(["futures"], date=None)
        self.assertIn("phase=contract_catalog; status=started", output.getvalue())
        self.assertIn("phase=collect; status=failed; failed_phase=contract_catalog", output.getvalue())
        self.assertNotIn("phase=collect; status=completed", output.getvalue())
        self.assertFalse((self.lake_root / "silver" / variety_calendar.TABLE_NAME).exists())

    def test_catalog_validation_failure_reports_its_own_phase(self) -> None:
        fake_jqdata = mock.Mock()
        fake_jqdata.get_all_securities.return_value = pd.DataFrame(
            {"start_date": [TRADING_DATE]}, index=["RB2405.XSGE"],
        )
        output = io.StringIO()
        with (
            redirect_stdout(output),
            mock.patch.object(jqdata_connection, "authenticate_jqdata", return_value=fake_jqdata),
            self.assertRaisesRegex(ValueError, "缺少列"),
        ):
            variety_calendar.collect(self.lake_root, TRADING_DATE, TRADING_DATE)

        self.assertIn("phase=collect; status=failed; failed_phase=catalog_validation", output.getvalue())
        self.assertNotIn("phase=collect; status=completed", output.getvalue())

    def test_empty_result_conversion_failure_does_not_report_success(self) -> None:
        conversion_error = RuntimeError("injected empty table conversion failure")
        output = io.StringIO()
        with (
            redirect_stdout(output),
            mock.patch.object(variety_calendar, "pa", wraps=pa) as arrow,
            mock.patch.object(jqdata_connection, "authenticate_jqdata") as authenticate,
            self.assertRaises(RuntimeError) as raised,
        ):
            arrow.Table.from_batches.side_effect = conversion_error
            variety_calendar.collect(self.lake_root, date(2024, 1, 6), date(2024, 1, 7))

        self.assertIs(raised.exception, conversion_error)
        authenticate.assert_not_called()
        self.assertIn("phase=collect; status=failed; failed_phase=contract_conversion", output.getvalue())
        self.assertNotIn("phase=collect; status=completed", output.getvalue())

    def test_explicit_readonly_run_reports_completion_without_commit(self) -> None:
        with (
            mock.patch.object(variety_calendar, "collect", return_value=pd.DataFrame()) as collect,
            mock.patch.object(variety_calendar, "commit_partitions") as commit,
        ):
            result = CliRunner().invoke(variety_calendar.main, [
                "--lake-root", str(self.lake_root),
                "--start-date", "2024-01-03", "--end-date", "2024-01-03",
            ])

        self.assertEqual(result.exit_code, 0, result.output)
        collect.assert_called_once_with(self.lake_root.resolve(), TRADING_DATE, TRADING_DATE)
        commit.assert_not_called()
        self.assertIn("phase=run; status=completed; mode=explicit; write=false; rows=0", result.output)
        self.assertNotIn("committed:", result.output)

    def test_full_comparison_repairs_missing_and_changed_keys_and_rejects_excess_keys(self) -> None:
        with (
            redirect_stdout(io.StringIO()),
            mock.patch.object(jqdata_connection, "authenticate_jqdata", return_value=FakeJQData()),
        ):
            expected_frame = variety_calendar.collect(self.lake_root, TRADING_DATE, TRADING_DATE)
            variety_calendar.commit_partitions(
                expected_frame.iloc[1:].copy(), self.lake_root, TRADING_DATE, TRADING_DATE,
            )
        expected_frame.loc[2, "active_contract_count"] = 2
        arguments = ["--lake-root", str(self.lake_root), "--full", "--write"]
        with mock.patch.object(variety_calendar, "collect", return_value=expected_frame):
            result = CliRunner().invoke(variety_calendar.main, arguments)

        self.assertEqual(result.exit_code, 0, f"{result.output}\n{result.exception!r}")
        self.assertIn("missing_or_incomplete_grid_count=2", result.output)
        self.assertIn("phase=run; status=completed; mode=full; write=true; rows=4; ranges=1", result.output)
        partitioning = ds.partitioning(
            pa.schema([FUTURES_VARIETY_CALENDAR_SCHEMA.field(name) for name in variety_calendar.PARTITION_COLUMNS]),
            flavor="hive",
        )
        committed_frame = ds.dataset(
            self.lake_root / "silver" / variety_calendar.TABLE_NAME,
            format="parquet", partitioning=partitioning,
        ).to_table(columns=FUTURES_VARIETY_CALENDAR_SCHEMA.names).to_pandas(types_mapper=pd.ArrowDtype)
        pd.testing.assert_frame_equal(
            expected_frame.sort_values(variety_calendar.PRIMARY_KEY).reset_index(drop=True),
            committed_frame.sort_values(variety_calendar.PRIMARY_KEY).reset_index(drop=True),
        )

        with (
            mock.patch.object(variety_calendar, "collect", return_value=expected_frame),
            mock.patch.object(variety_calendar, "commit_partitions") as commit,
        ):
            current_result = CliRunner().invoke(variety_calendar.main, arguments)
        self.assertEqual(current_result.exit_code, 0, current_result.output)
        self.assertIn("outcome=up_to_date; rows=0", current_result.output)
        commit.assert_not_called()

        with (
            mock.patch.object(variety_calendar, "collect", return_value=expected_frame.iloc[:-1].copy()),
            mock.patch.object(variety_calendar, "commit_partitions") as commit,
        ):
            rejected_result = CliRunner().invoke(variety_calendar.main, arguments)
        self.assertIsInstance(rejected_result.exception, ValueError)
        self.assertIn("不属于上游有效格点", str(rejected_result.exception))
        self.assertNotIn("phase=run; status=completed", rejected_result.output)
        commit.assert_not_called()

    def test_execution_cell_distinguishes_notebook_script_and_module_import(self) -> None:
        notebook = json.loads((MARKET_WORKFLOW_DIR / "c02_futures_variety_calendar.ipynb").read_text(encoding="utf-8"))
        entry_source = next(
            "".join(cell["source"])
            for cell in notebook["cells"]
            if cell["cell_type"] == "code" and "notebook_args =" in "".join(cell["source"])
        )
        for execution_mode in ("notebook", "script", "import"):
            with self.subTest(mode=execution_mode):
                callback = mock.Mock()
                command = click.Command("b02", params=variety_calendar.main.params, callback=callback)
                namespace = {"sys": sys, "main": command, "__name__": "__main__"}
                arguments = ["ipykernel_launcher.py", "-f", "connection.json"]
                if execution_mode != "notebook":
                    namespace["__file__"] = str(MARKET_WORKFLOW_DIR / "c02_futures_variety_calendar.py")
                if execution_mode == "import":
                    namespace["__name__"] = "c02_futures_variety_calendar"
                elif execution_mode == "script":
                    arguments = [namespace["__file__"], "--full"]
                with mock.patch.dict(sys.modules, {"ipykernel": mock.Mock()}), mock.patch.object(sys, "argv", arguments):
                    if execution_mode == "script":
                        with self.assertRaises(SystemExit) as exit_result:
                            exec(compile(entry_source, "<b02 execution cell>", "exec"), namespace)
                        self.assertEqual(exit_result.exception.code, 0)
                    else:
                        exec(compile(entry_source, "<b02 execution cell>", "exec"), namespace)
                if execution_mode == "import":
                    callback.assert_not_called()
                else:
                    callback.assert_called_once()
                    self.assertFalse(callback.call_args.kwargs["write"])
                    if execution_mode == "notebook":
                        self.assertEqual(callback.call_args.kwargs["start_date"], datetime(2026, 8, 1))
                        self.assertEqual(callback.call_args.kwargs["end_date"], datetime(2026, 8, 15))
                    else:
                        self.assertTrue(callback.call_args.kwargs["full_refresh"])

    def test_multimonth_replacement_preserves_outer_rows_and_clears_empty_leaves(self) -> None:
        old_rows = [
            (date(2024, 1, 2), "XSGE", "RB"),
            (date(2024, 1, 3), "XSGE", "RB"),
            (date(2024, 1, 3), "XDCE", "A"),
            (date(2024, 2, 1), "XSGE", "RB"),
            (date(2024, 2, 1), "XDCE", "A"),
            (date(2024, 2, 6), "XSGE", "RB"),
        ]
        old_frame = pd.DataFrame([
            {
                "trading_date": trading_date,
                "exchange_code": exchange_code,
                "underlying_code": underlying_code,
                "active_contract_count": 1,
                "source": "JQData_get_all_securities+dim_trade_calendar",
                "updated_at": UPDATED_AT,
                "year": trading_date.year,
                "month": trading_date.month,
            }
            for trading_date, exchange_code, underlying_code in old_rows
        ], columns=FUTURES_VARIETY_CALENDAR_SCHEMA.names)
        partitioning = ds.partitioning(
            pa.schema([
                FUTURES_VARIETY_CALENDAR_SCHEMA.field(name)
                for name in variety_calendar.PARTITION_COLUMNS
            ]),
            flavor="hive",
        )
        table_path = self.lake_root / "silver" / variety_calendar.TABLE_NAME
        ds.write_dataset(
            pandas_to_arrow(old_frame, FUTURES_VARIETY_CALENDAR_SCHEMA),
            table_path, format="parquet", partitioning=partitioning,
        )
        incoming_frame = old_frame.loc[[1, 3]].copy()
        incoming_frame["active_contract_count"] = 2
        output = io.StringIO()
        with redirect_stdout(output):
            committed_rows = variety_calendar.commit_partitions(
                incoming_frame, self.lake_root, date(2024, 1, 3), date(2024, 2, 1),
            )

        self.assertEqual(committed_rows, 2)
        committed_frame = ds.dataset(
            table_path, format="parquet", partitioning=partitioning,
        ).to_table().to_pandas()
        self.assertEqual(
            set(committed_frame[["trading_date", "active_contract_count"]].itertuples(index=False, name=None)),
            {(date(2024, 1, 2), 1), (date(2024, 1, 3), 2), (date(2024, 2, 1), 2), (date(2024, 2, 6), 1)},
        )
        self.assertEqual(set(committed_frame["exchange_code"]), {"XSGE"})
        partition_logs = [line for line in output.getvalue().splitlines() if line.startswith("partition_committed:")]
        self.assertEqual(len(partition_logs), 4)
        self.assertEqual(sum("rows=0;" in line for line in partition_logs), 2)
        self.assertEqual(sum("rows=2;" in line for line in partition_logs), 2)
        self.assertIn("replacement_rows=4; partitions=4", output.getvalue())

        empty_frame = incoming_frame.iloc[0:0].copy()
        for target_lake_root in (self.lake_root, self.lake_root / "initially_empty"):
            with self.subTest(lake_root=target_lake_root), redirect_stdout(io.StringIO()):
                self.assertEqual(
                    variety_calendar.commit_partitions(
                        empty_frame, target_lake_root, date(2024, 1, 1), date(2024, 2, 29),
                    ),
                    0,
                )
                empty_table_path = target_lake_root / "silver" / variety_calendar.TABLE_NAME
                self.assertTrue((empty_table_path / "schema.parquet").is_file())
                empty_table = ds.dataset(
                    empty_table_path, format="parquet", partitioning=partitioning,
                ).to_table(columns=FUTURES_VARIETY_CALENDAR_SCHEMA.names)
                self.assertEqual(len(empty_table), 0)
                self.assertTrue(empty_table.schema.equals(FUTURES_VARIETY_CALENDAR_SCHEMA, check_metadata=True))

    def test_second_leaf_failure_restores_all_old_leaves_without_commit_success(self) -> None:
        with (
            redirect_stdout(io.StringIO()),
            mock.patch.object(
                jqdata_connection, "authenticate_jqdata", return_value=FakeJQData(),
            ),
        ):
            old_frame = variety_calendar.collect(self.lake_root, TRADING_DATE, TRADING_DATE)
            variety_calendar.commit_partitions(old_frame, self.lake_root, TRADING_DATE, TRADING_DATE)
        replacement_frame = old_frame.copy()
        replacement_frame["active_contract_count"] = 2
        original_validate = variety_calendar.validate_compatible_dataset_schema
        formal_readback_partitions = set()

        def reject_second_formal_leaf(actual_schema, expected_schema, context):
            if context.startswith("正式分区"):
                formal_readback_partitions.add(context.split(" fragment ", 1)[0])
                if len(formal_readback_partitions) == 2:
                    raise TypeError("injected second leaf failure")
            return original_validate(actual_schema, expected_schema, context)

        output = io.StringIO()
        with (
            redirect_stdout(output),
            mock.patch.object(
                variety_calendar, "validate_compatible_dataset_schema", side_effect=reject_second_formal_leaf,
            ),
            self.assertRaisesRegex(RuntimeError, "分区提交失败"),
        ):
            variety_calendar.commit_partitions(replacement_frame, self.lake_root, TRADING_DATE, TRADING_DATE)

        partitioning = ds.partitioning(
            pa.schema([
                FUTURES_VARIETY_CALENDAR_SCHEMA.field(name)
                for name in variety_calendar.PARTITION_COLUMNS
            ]),
            flavor="hive",
        )
        restored_frame = ds.dataset(
            self.lake_root / "silver" / variety_calendar.TABLE_NAME,
            format="parquet", partitioning=partitioning,
        ).to_table(columns=FUTURES_VARIETY_CALENDAR_SCHEMA.names).to_pandas(types_mapper=pd.ArrowDtype)
        pd.testing.assert_frame_equal(
            old_frame.sort_values(variety_calendar.PRIMARY_KEY).reset_index(drop=True),
            restored_frame.sort_values(variety_calendar.PRIMARY_KEY).reset_index(drop=True),
        )
        self.assertEqual(len(formal_readback_partitions), 2)
        self.assertIn("phase=rollback; status=completed; partitions=2", output.getvalue())
        self.assertNotIn("committed: table=dim_futures_variety_calendar; status=completed", output.getvalue())


if __name__ == "__main__":
    unittest.main()
