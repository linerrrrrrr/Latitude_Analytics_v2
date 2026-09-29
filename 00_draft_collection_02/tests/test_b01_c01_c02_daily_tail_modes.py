"""验证 c01/c02 默认尾部水位与显式 --full 模式。"""

from __future__ import annotations

import importlib.util
import io
import pathlib
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import date, datetime, time, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from click.testing import CliRunner


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
WORKFLOW_DIR = (
    PROJECT_ROOT
    / "02_Futures_Lakehouse"
    / "a01_Futures_Market_Data"
)


def load_workflow(module_name: str):
    module_path = WORKFLOW_DIR / f"{module_name}.py"
    module_spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    return module


c01 = load_workflow("b01_trade_calendar")
c02 = load_workflow("b02_futures_variety_calendar")
c03 = load_workflow("b03_futures_contract_calendar")


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2024, 1, 3, 21, 0, tzinfo=tz)


def trade_calendar_df(start_date: date, end_date: date) -> pd.DataFrame:
    calendar_dates = list(pd.date_range(start_date, end_date, freq="D").date)
    frame = pd.DataFrame({"calendar_date": calendar_dates})
    frame["date_key"] = frame["calendar_date"].map(lambda value: value.strftime("%Y%m%d"))
    frame["is_trading_day"] = True
    frame["weekday"] = frame["calendar_date"].map(lambda value: value.weekday() + 1)
    frame["is_weekend"] = frame["weekday"].isin([6, 7])
    frame["source"] = "JQData_get_trade_days"
    frame["calendar_name"] = "CN_FUTURES_MARKET"
    frame["calendar_timezone"] = "Asia/Shanghai"
    frame["effective_after"] = time(20, 0)
    frame["updated_at"] = datetime(2024, 1, 3, tzinfo=timezone.utc)
    frame["year"] = pd.to_datetime(frame["calendar_date"]).dt.year.astype("int16")
    return frame.loc[:, c01.TRADE_CALENDAR_SCHEMA.names]


def variety_calendar_df(trading_date: date) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "underlying_code": "RB",
                "exchange_code": "XSGE",
                "trading_date": trading_date,
                "active_contract_count": 1,
                "source": "JQData_get_all_securities+dim_trade_calendar",
                "updated_at": datetime(2024, 1, 3, tzinfo=timezone.utc),
                "year": trading_date.year,
                "month": trading_date.month,
            }
        ],
        columns=c02.FUTURES_VARIETY_CALENDAR_SCHEMA.names,
    )


def write_trade_calendar(lake_root: pathlib.Path, frame: pd.DataFrame) -> None:
    ds.write_dataset(
        c01.pandas_to_arrow(frame, c01.TRADE_CALENDAR_SCHEMA),
        lake_root / "silver" / c01.TABLE_NAME,
        format="parquet",
        partitioning=ds.partitioning(
            pa.schema([c01.TRADE_CALENDAR_SCHEMA.field("year")]),
            flavor="hive",
        ),
    )


def write_variety_calendar(lake_root: pathlib.Path, frame: pd.DataFrame) -> None:
    ds.write_dataset(
        c02.pandas_to_arrow(frame, c02.FUTURES_VARIETY_CALENDAR_SCHEMA),
        lake_root / "silver" / c02.TABLE_NAME,
        format="parquet",
        partitioning=ds.partitioning(
            pa.schema(
                [
                    c02.FUTURES_VARIETY_CALENDAR_SCHEMA.field(name)
                    for name in c02.PARTITION_COLUMNS
                ]
            ),
            flavor="hive",
        ),
    )


class DailyTailModeTest(unittest.TestCase):
    def test_c01_collect_reports_progress_without_main_and_propagates_source_failure(self) -> None:
        for source_fails in (False, True):
            with self.subTest(source_fails=source_fails):
                jqdata_client = Mock()
                if source_fails:
                    jqdata_client.get_trade_days.side_effect = RuntimeError("source unavailable")
                else:
                    jqdata_client.get_trade_days.return_value = [date(2024, 1, 2)]
                output = io.StringIO()
                with (
                    patch.object(c01, "settings", SimpleNamespace(jqdata_id="test", jqdata_secret="test")),
                    patch("config.jqdata_connection.authenticate_jqdata", return_value=jqdata_client),
                    redirect_stdout(output),
                ):
                    if source_fails:
                        with self.assertRaisesRegex(RuntimeError, "source unavailable"):
                            c01.collect(date(2023, 12, 31), date(2024, 1, 2))
                    else:
                        calendar_df = c01.collect(date(2023, 12, 31), date(2024, 1, 2))
                        self.assertEqual(calendar_df.calendar_date.tolist(), [
                            date(2023, 12, 31), date(2024, 1, 1), date(2024, 1, 2),
                        ])
                        self.assertEqual(calendar_df.is_trading_day.tolist(), [False, False, True])
                jqdata_client.get_trade_days.assert_called_once_with(
                    start_date=date(2023, 12, 31), end_date=date(2024, 1, 2),
                )
                self.assertEqual(output.getvalue().count("phase=collect; status=started"), 1)
                self.assertEqual(
                    output.getvalue().count("phase=collect; status=completed"),
                    0 if source_fails else 1,
                )
                self.assertIn("request_batch:", output.getvalue())

    def test_c01_full_commits_only_differences_across_years_without_revalidating_subset(self) -> None:
        expected_calendar_df = trade_calendar_df(date(2023, 12, 31), date(2024, 1, 3))
        existing_calendar_df = expected_calendar_df.iloc[[0, 1, 3]].copy()
        existing_calendar_df.loc[0, "is_trading_day"] = False
        expected_calendar_df["updated_at"] = pd.Timestamp("2024-01-03T12:00:00Z")
        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            write_trade_calendar(lake_root, existing_calendar_df)
            with (
                patch.object(c01, "settings", SimpleNamespace(
                    futures_lake_root=lake_root, futures_data_start_date=date(2023, 12, 31),
                )),
                patch.object(c01, "datetime", FixedDateTime),
                patch.object(c01, "collect", return_value=expected_calendar_df),
                patch.object(c01, "validate_calendar_table", wraps=c01.validate_calendar_table) as validate,
            ):
                result = CliRunner().invoke(c01.main, ["--full", "--write"])
            self.assertEqual(result.exit_code, 0, result.output)
            self.assertEqual(validate.call_count, 3)
            calendar_dataset = ds.dataset(
                lake_root / "silver" / c01.TABLE_NAME,
                format="parquet",
                partitioning=ds.partitioning(pa.schema([c01.TRADE_CALENDAR_SCHEMA.field("year")]), flavor="hive"),
            )
            committed_calendar_df = calendar_dataset.to_table(
                columns=c01.TRADE_CALENDAR_SCHEMA.names,
            ).to_pandas().sort_values("calendar_date").reset_index(drop=True)
        unchanged_dates = [date(2024, 1, 1), date(2024, 1, 3)]
        expected_calendar_df.loc[
            expected_calendar_df.calendar_date.isin(unchanged_dates), "updated_at",
        ] = pd.Timestamp("2024-01-03T00:00:00Z")
        expected_calendar_df = c01.pandas_to_arrow(
            expected_calendar_df, c01.TRADE_CALENDAR_SCHEMA,
        ).to_pandas()
        pd.testing.assert_frame_equal(committed_calendar_df, expected_calendar_df)
        partition_logs = [line for line in result.output.splitlines() if line.startswith("partition_committed:")]
        self.assertEqual(len(partition_logs), 2)
        self.assertIn("completed=1; total=2", partition_logs[0])
        self.assertIn("completed=2; total=2", partition_logs[1])
        commit_logs = [line for line in result.output.splitlines() if line.startswith("committed:")]
        self.assertEqual(len(commit_logs), 1)
        self.assertIn("rows=2; partitions=2", commit_logs[0])

    def test_descriptive_metadata_changes_are_compatible(self) -> None:
        schema_cases = (
            (c01, c01.TRADE_CALENDAR_SCHEMA),
            (c02, c02.FUTURES_VARIETY_CALENDAR_SCHEMA),
            (c03, c03.FUTURES_CONTRACT_CALENDAR_SCHEMA),
        )
        for workflow, expected_schema in schema_cases:
            with self.subTest(table=expected_schema.metadata[b"table_name"]):
                changed_fields = [
                    field.with_metadata({b"description_zh": b"current-description"})
                    for field in expected_schema
                ]
                changed_metadata = dict(expected_schema.metadata)
                changed_metadata[b"description_zh"] = b"current-table-description"
                actual_schema = pa.schema(changed_fields, metadata=changed_metadata)
                workflow.validate_compatible_dataset_schema(
                    actual_schema,
                    expected_schema,
                    "测试 ",
                )

    def test_physical_routing_metadata_change_is_rejected(self) -> None:
        changed_metadata = dict(c01.TRADE_CALENDAR_SCHEMA.metadata)
        changed_metadata[b"primary_key"] = b"date_key"
        actual_schema = c01.TRADE_CALENDAR_SCHEMA.with_metadata(changed_metadata)
        with self.assertRaisesRegex(TypeError, "primary_key"):
            c01.validate_compatible_dataset_schema(
                actual_schema,
                c01.TRADE_CALENDAR_SCHEMA,
                "测试 ",
            )

    def test_c01_no_tail_does_not_collect(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            write_trade_calendar(
                lake_root,
                trade_calendar_df(date(2024, 1, 1), date(2024, 1, 3)),
            )
            settings = SimpleNamespace(
                futures_lake_root=(lake_root / "formal").resolve(),
                futures_data_start_date=date(2024, 1, 1),
            )
            with (
                patch.object(c01, "settings", settings),
                patch.object(c01, "datetime", FixedDateTime),
                patch.object(c01, "collect") as collect,
            ):
                result = CliRunner().invoke(
                    c01.main,
                    ["--lake-root", str(lake_root), "--write"],
                )

        self.assertEqual(result.exit_code, 0, f"{result.output}\n{result.exception!r}")
        collect.assert_not_called()
        self.assertIn("api_calls=0", result.output)

    def test_c01_full_is_explicit_and_dates_are_mutually_exclusive(self) -> None:
        settings = SimpleNamespace(
            futures_lake_root=pathlib.Path("unused-formal").resolve(),
            futures_data_start_date=date(2024, 1, 1),
        )
        expected_frame = trade_calendar_df(date(2024, 1, 1), date(2024, 1, 3))
        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            with (
                patch.object(c01, "settings", settings),
                patch.object(c01, "datetime", FixedDateTime),
                patch.object(c01, "collect", return_value=expected_frame) as collect,
            ):
                result = CliRunner().invoke(
                    c01.main,
                    ["--lake-root", str(lake_root), "--full"],
                )
        self.assertEqual(result.exit_code, 0, f"{result.output}\n{result.exception!r}")
        collect.assert_called_once_with(date(2024, 1, 1), date(2024, 1, 3))
        self.assertIn("Full-history", result.output)

        invalid = CliRunner().invoke(
            c01.main,
            [
                "--full",
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-01-02",
            ],
        )
        self.assertEqual(invalid.exit_code, 2)

    def test_c01_commit_validates_dirty_year_once_without_formal_root_revalidation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            write_trade_calendar(
                lake_root,
                trade_calendar_df(date(2024, 1, 1), date(2024, 1, 2)),
            )
            pending_frame = trade_calendar_df(date(2024, 1, 3), date(2024, 1, 3))
            with patch.object(
                c01,
                "validate_calendar_table",
                wraps=c01.validate_calendar_table,
            ) as validate:
                committed_rows = c01.commit_partitions(pending_frame, lake_root)
            committed_dataset = ds.dataset(
                lake_root / "silver" / c01.TABLE_NAME,
                format="parquet",
                partitioning=ds.partitioning(
                    pa.schema([c01.TRADE_CALENDAR_SCHEMA.field("year")]),
                    flavor="hive",
                ),
            )
            committed_dates = sorted(committed_dataset.to_table(
                columns=["calendar_date"]
            ).column("calendar_date").to_pylist())

        self.assertEqual(committed_rows, 1)
        self.assertEqual(
            committed_dates,
            [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
        )
        # 输入响应一次、合并后的完整 dirty 年分区一次；staging/正式不再调用业务 validator。
        self.assertEqual(validate.call_count, 2)

    def test_c01_formal_leaf_physical_reread_failure_rolls_back(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            old_frame = trade_calendar_df(date(2024, 1, 1), date(2024, 1, 2))
            write_trade_calendar(lake_root, old_frame)
            pending_frame = trade_calendar_df(date(2024, 1, 3), date(2024, 1, 3))
            original_validate = c01.validate_compatible_dataset_schema

            def reject_formal_leaf(actual_schema, expected_schema, context):
                if context.startswith("正式分区"):
                    raise TypeError("injected formal physical failure")
                return original_validate(actual_schema, expected_schema, context)

            with (
                patch.object(
                    c01,
                    "validate_compatible_dataset_schema",
                    side_effect=reject_formal_leaf,
                ),
                self.assertRaisesRegex(TypeError, "formal physical failure"),
            ):
                c01.commit_partitions(pending_frame, lake_root)

            committed_dataset = ds.dataset(
                lake_root / "silver" / c01.TABLE_NAME,
                format="parquet",
                partitioning=ds.partitioning(
                    pa.schema([c01.TRADE_CALENDAR_SCHEMA.field("year")]),
                    flavor="hive",
                ),
            )
            committed_dates = sorted(
                committed_dataset.to_table(columns=["calendar_date"])
                .column("calendar_date")
                .to_pylist()
            )

        self.assertEqual(committed_dates, [date(2024, 1, 1), date(2024, 1, 2)])

    def test_c02_no_new_c01_trading_day_does_not_collect_catalog(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            write_trade_calendar(lake_root, trade_calendar_df(date(2024, 1, 2), date(2024, 1, 2)))
            write_variety_calendar(lake_root, variety_calendar_df(date(2024, 1, 2)))
            settings = SimpleNamespace(futures_lake_root=(lake_root / "formal").resolve())
            with (
                patch.object(c02, "settings", settings),
                patch.object(c02, "collect") as collect,
            ):
                result = CliRunner().invoke(
                    c02.main,
                    ["--lake-root", str(lake_root), "--write"],
                )

        self.assertEqual(result.exit_code, 0, f"{result.output}\n{result.exception!r}")
        collect.assert_not_called()
        self.assertIn("api_calls=0", result.output)

    def test_c02_collects_and_commits_only_new_tail_dates(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            write_trade_calendar(lake_root, trade_calendar_df(date(2024, 1, 2), date(2024, 1, 3)))
            write_variety_calendar(lake_root, variety_calendar_df(date(2024, 1, 2)))
            pending_frame = variety_calendar_df(date(2024, 1, 3))
            settings = SimpleNamespace(futures_lake_root=(lake_root / "formal").resolve())
            with (
                patch.object(c02, "settings", settings),
                patch.object(c02, "collect", return_value=pending_frame) as collect,
                patch.object(c02, "commit_partitions", return_value=1) as commit,
            ):
                result = CliRunner().invoke(
                    c02.main,
                    ["--lake-root", str(lake_root), "--write"],
                )

        self.assertEqual(result.exit_code, 0, f"{result.output}\n{result.exception!r}")
        collect.assert_called_once_with(
            lake_root.resolve(), date(2024, 1, 3), date(2024, 1, 3),
            selected_trading_dates=[date(2024, 1, 3)],
        )
        commit.assert_called_once()
        self.assertIn("new_trading_date_count=1", result.output)

    def test_c02_formal_leaf_physical_reread_failure_restores_old_leaf(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            old_frame = variety_calendar_df(date(2024, 1, 2))
            write_variety_calendar(lake_root, old_frame)
            replacement_frame = old_frame.copy()
            replacement_frame["active_contract_count"] = 2
            original_validate = c02.validate_compatible_dataset_schema

            def reject_formal_leaf(actual_schema, expected_schema, context):
                if context.startswith("正式分区"):
                    raise TypeError("injected formal physical failure")
                return original_validate(actual_schema, expected_schema, context)

            with (
                patch.object(
                    c02,
                    "validate_compatible_dataset_schema",
                    side_effect=reject_formal_leaf,
                ),
                self.assertRaisesRegex(RuntimeError, "分区提交失败"),
            ):
                c02.commit_partitions(
                    replacement_frame,
                    lake_root,
                    date(2024, 1, 2),
                    date(2024, 1, 2),
                )

            committed_dataset = ds.dataset(
                lake_root / "silver" / c02.TABLE_NAME,
                format="parquet",
                partitioning=ds.partitioning(
                    pa.schema(
                        [
                            c02.FUTURES_VARIETY_CALENDAR_SCHEMA.field(name)
                            for name in c02.PARTITION_COLUMNS
                        ]
                    ),
                    flavor="hive",
                ),
            )
            committed_count = committed_dataset.to_table(
                columns=["active_contract_count"]
            ).column("active_contract_count").to_pylist()

        self.assertEqual(committed_count, [1])


if __name__ == "__main__":
    unittest.main()
