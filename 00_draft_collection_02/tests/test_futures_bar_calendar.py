"""验证 c04 行情日历的结构快路径、状态继承与多文件叶读取。"""

from __future__ import annotations

import pathlib
import tempfile
import types
import unittest
from datetime import date, datetime, timezone
from unittest import mock

import nbformat
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
    / "a01_Futures_Market_Data"
    / "b04_futures_bar_calendar.ipynb"
)


def load_notebook_module() -> types.ModuleType:
    notebook = nbformat.read(NOTEBOOK_PATH, as_version=4)
    nbformat.validate(notebook)
    source, _ = PythonExporter().from_notebook_node(notebook)
    module = types.ModuleType("test_b04_futures_bar_calendar_notebook")
    module.__file__ = str(NOTEBOOK_PATH.with_suffix(".py"))
    exec(compile(source, module.__file__, "exec"), module.__dict__)
    return module


class FuturesBarCalendarTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.module = load_notebook_module()

    def setUp(self) -> None:
        self.updated_at = datetime(2026, 8, 23, 1, 2, tzinfo=timezone.utc)

    def contract_frame(
        self,
        contract_codes: tuple[str, ...] = (
            "RB2405.XSGE",
            "HC2405.XSGE",
        ),
        *,
        session_count: int = 1,
        trading_date_value: date = date(2024, 1, 3),
    ) -> pd.DataFrame:
        session_specs = (
            (
                "09:00~09:03",
                pd.Timestamp(
                    f"{trading_date_value.isoformat()} 09:00",
                    tz="Asia/Shanghai",
                ),
                pd.Timestamp(
                    f"{trading_date_value.isoformat()} 09:03",
                    tz="Asia/Shanghai",
                ),
                3,
            ),
            (
                "10:30~10:32",
                pd.Timestamp(
                    f"{trading_date_value.isoformat()} 10:30",
                    tz="Asia/Shanghai",
                ),
                pd.Timestamp(
                    f"{trading_date_value.isoformat()} 10:32",
                    tz="Asia/Shanghai",
                ),
                2,
            ),
        )
        rows = []
        for contract_code in contract_codes:
            underlying_code = contract_code.split("2", maxsplit=1)[0]
            for session_number, session_spec in enumerate(
                session_specs[:session_count],
                start=1,
            ):
                session_text, session_start_at, session_end_at, minute_count = (
                    session_spec
                )
                rows.append(
                    {
                        "contract_code": contract_code,
                        "exchange_code": "XSGE",
                        "underlying_code": underlying_code,
                        "trading_date": trading_date_value,
                        "list_date": date(2023, 1, 1),
                        "delist_date": date(2024, 5, 15),
                        "contract_multiplier": 10.0,
                        "tick_size": 1.0,
                        "rule_effective_date": date(2020, 1, 1),
                        "rule_expiry_date": date(2030, 1, 1),
                        "session_number": session_number,
                        "session_text": session_text,
                        "session_start_at": session_start_at,
                        "session_end_at": session_end_at,
                        "is_night_session": False,
                        "spans_midnight": False,
                        "minute_count": minute_count,
                        "source": "test_fixture",
                        "updated_at": self.updated_at,
                        "year": trading_date_value.year,
                        "month": trading_date_value.month,
                    }
                )
        return pd.DataFrame(
            rows,
            columns=self.module.FUTURES_CONTRACT_CALENDAR_SCHEMA.names,
        )

    def state_table_for_contract(
        self,
        frame: pd.DataFrame,
        contract_code: str,
    ):
        selected_df = frame.loc[
            frame["contract_code"].eq(contract_code),
            self.module.FUTURES_BAR_CALENDAR_SCHEMA.names,
        ]
        return self.module.pandas_to_arrow(
            selected_df,
            self.module.FUTURES_BAR_CALENDAR_SCHEMA,
        ).select(self.module.STATE_COLUMNS)

    @staticmethod
    def calendar_table_with_schema_version(
        calendar_table: pa.Table,
        schema_version: str,
    ) -> pa.Table:
        metadata = dict(calendar_table.schema.metadata or {})
        metadata[b"schema_version"] = schema_version.encode("utf-8")
        return calendar_table.replace_schema_metadata(metadata)

    def test_build_structural_partitions_projects_daily_and_minute_grids(self) -> None:
        contract_df = self.contract_frame(session_count=2)
        projected_contract_df = contract_df.loc[
            :, self.module.UPSTREAM_STRUCTURE_COLUMNS
        ]

        with (
            mock.patch.object(
                self.module,
                "validate_contract_input",
                side_effect=AssertionError("结构快路径不得执行完整上游校验"),
            ),
            mock.patch.object(
                self.module,
                "validate_contract_structure",
                side_effect=AssertionError("正式 c03 投影不得重复证明业务规则"),
            ),
        ):
            partitions = self.module.build_structural_partitions(
                projected_contract_df
            )

        daily_df = partitions["1d"]
        minute_df = partitions["1m"]
        self.assertEqual(list(daily_df.columns), self.module.STRUCTURAL_COLUMNS)
        self.assertEqual(list(minute_df.columns), self.module.STRUCTURAL_COLUMNS)
        self.assertEqual(len(daily_df), 2)
        self.assertEqual(len(minute_df), 4)
        self.assertTrue(daily_df["session_number"].eq(0).all())
        self.assertTrue(daily_df["expected_bar_count"].eq(1).all())
        self.assertTrue(
            daily_df[
                [
                    "session_text",
                    "session_start_at",
                    "session_end_at",
                    "is_night_session",
                ]
            ].isna().all(axis=None)
        )
        self.assertEqual(
            minute_df["expected_bar_count"].astype(int).tolist(),
            [3, 2, 3, 2],
        )
        self.assertEqual(
            minute_df["session_number"].astype(int).tolist(),
            [1, 2, 1, 2],
        )

    def test_build_fresh_partitions_preserves_public_initial_state_semantics(self) -> None:
        partitions = self.module.build_fresh_partitions(
            self.contract_frame(session_count=2),
            self.updated_at,
        )

        daily_df = partitions["1d"]
        minute_df = partitions["1m"]
        self.assertEqual(list(daily_df.columns), self.module.FUTURES_BAR_CALENDAR_SCHEMA.names)
        self.assertEqual(list(minute_df.columns), self.module.FUTURES_BAR_CALENDAR_SCHEMA.names)
        self.assertEqual(len(daily_df), 2)
        self.assertEqual(len(minute_df), 4)
        self.assertTrue(daily_df["is_fetch_required"].all())
        self.assertFalse(minute_df["is_fetch_required"].any())
        self.assertFalse(daily_df["is_fetch_completed"].any())
        self.assertFalse(minute_df["is_fetch_completed"].any())
        self.assertTrue(daily_df["quality_status"].eq("pending").all())
        self.assertTrue(minute_df["quality_status"].eq("pending").all())
        self.assertTrue(daily_df["updated_at"].eq(self.updated_at).all())
        self.assertTrue(minute_df["updated_at"].eq(self.updated_at).all())

    def test_empty_upstream_returns_typed_empty_structural_and_full_partitions(
        self,
    ) -> None:
        empty_contract_df = self.module.empty_pandas(
            self.module.FUTURES_CONTRACT_CALENDAR_SCHEMA
        )

        structural_partitions = self.module.build_structural_partitions(
            empty_contract_df.loc[:, self.module.UPSTREAM_STRUCTURE_COLUMNS]
        )
        full_partitions = self.module.build_fresh_partitions(
            empty_contract_df,
            self.updated_at,
        )

        for frequency in ("1d", "1m"):
            self.assertTrue(structural_partitions[frequency].empty)
            self.assertEqual(
                list(structural_partitions[frequency].columns),
                self.module.STRUCTURAL_COLUMNS,
            )
            self.assertTrue(full_partitions[frequency].empty)
            self.assertEqual(
                list(full_partitions[frequency].columns),
                self.module.FUTURES_BAR_CALENDAR_SCHEMA.names,
            )

    def test_structural_clean_comparison_is_null_safe_and_skips_full_state_path(
        self,
    ) -> None:
        expected_df = self.module.build_structural_partitions(
            self.contract_frame(
                (
                    "RB2405.XSGE",
                    "HC2405.XSGE",
                    "CU2405.XSGE",
                )
            ).loc[:, self.module.UPSTREAM_STRUCTURE_COLUMNS]
        )["1d"]
        existing_df = expected_df.sample(frac=1, random_state=13).reset_index(drop=True)

        with (
            mock.patch.object(
                self.module,
                "validate_bar_calendar_frame",
                side_effect=AssertionError("clean 结构比较不得读取完整状态"),
            ),
            mock.patch.object(
                self.module,
                "merge_preserving_state",
                side_effect=AssertionError("clean 结构比较不得合并状态"),
            ),
        ):
            audit = self.module.assess_structural_partition(
                expected_df,
                existing_df,
                None,
            )

        self.assertEqual(
            audit,
            {
                "expected_row_count": 3,
                "complete_count": 3,
                "missing_count": 0,
                "changed_count": 0,
                "extra_count": 0,
                "quality_error": None,
                "is_dirty": False,
            },
        )

    def test_clean_main_reads_only_structure_and_skips_every_slow_path(self) -> None:
        contract_df = self.contract_frame(session_count=2)
        fresh_partitions = self.module.build_fresh_partitions(
            contract_df,
            self.updated_at,
        )
        existing_calendar_df = pd.concat(
            [fresh_partitions["1d"], fresh_partitions["1m"]],
            ignore_index=True,
        )
        existing_calendar_df.loc[
            existing_calendar_df.index[0], "quality_reason"
        ] = "结构未变时应信任的既有状态说明。"

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            silver_root = lake_root / "silver"
            upstream_path = silver_root / self.module.UPSTREAM_TABLE_NAME
            target_path = silver_root / self.module.TABLE_NAME
            ds.write_dataset(
                self.module.pandas_to_arrow(
                    contract_df,
                    self.module.FUTURES_CONTRACT_CALENDAR_SCHEMA,
                ),
                upstream_path,
                format="parquet",
                partitioning=self.module.UPSTREAM_PARTITIONING,
            )
            ds.write_dataset(
                self.module.pandas_to_arrow(
                    existing_calendar_df,
                    self.module.FUTURES_BAR_CALENDAR_SCHEMA,
                ),
                target_path,
                format="parquet",
                partitioning=self.module.HIVE_PARTITIONING,
            )

            read_schema_names = []
            original_read_partition = self.module.read_partition

            def record_projected_read(
                dataset,
                schema,
                partition_columns,
                partition_key,
                start_date=None,
                end_date=None,
            ):
                read_schema_names.append(list(schema.names))
                return original_read_partition(
                    dataset,
                    schema,
                    partition_columns,
                    partition_key,
                    start_date,
                    end_date,
                )

            with (
                mock.patch.object(
                    self.module,
                    "read_partition",
                    side_effect=record_projected_read,
                ),
                mock.patch.object(
                    self.module,
                    "build_fresh_partitions",
                    side_effect=AssertionError("clean 规划不得构造完整状态表"),
                ),
                mock.patch.object(
                    self.module,
                    "merge_preserving_state",
                    side_effect=AssertionError("clean 规划不得合并状态"),
                ),
                mock.patch.object(
                    self.module,
                    "read_existing_partition_leaf",
                    side_effect=AssertionError("clean 规划不得读取正式完整叶"),
                ),
                mock.patch.object(
                    self.module,
                    "commit_partition",
                    side_effect=AssertionError("clean 规划不得提交"),
                ),
            ):
                result = CliRunner().invoke(
                    self.module.main,
                    ("--lake-root", str(lake_root)),
                )

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("up_to_date:", result.output)
        self.assertIn("clean_partition_count=2", result.output)
        self.assertEqual(
            read_schema_names,
            [
                self.module.STRUCTURAL_COLUMNS,
                self.module.STRUCTURAL_COLUMNS,
                self.module.UPSTREAM_STRUCTURE_COLUMNS,
            ],
        )

    def test_empty_target_main_write_trusts_c03_and_builds_both_frequencies(
        self,
    ) -> None:
        contract_df = self.contract_frame(session_count=2)

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            silver_root = lake_root / "silver"
            target_path = silver_root / self.module.TABLE_NAME
            open_dataset_labels = []
            original_open_contract_dataset = self.module.open_contract_dataset
            original_calendar_validator = (
                self.module.validate_bar_calendar_frame
            )

            def record_open_contract_dataset(*args, **kwargs):
                open_dataset_labels.append(args[4])
                return original_open_contract_dataset(*args, **kwargs)

            ds.write_dataset(
                self.module.pandas_to_arrow(
                    contract_df,
                    self.module.FUTURES_CONTRACT_CALENDAR_SCHEMA,
                ),
                silver_root / self.module.UPSTREAM_TABLE_NAME,
                format="parquet",
                partitioning=self.module.UPSTREAM_PARTITIONING,
            )

            with (
                mock.patch.object(
                    self.module,
                    "validate_contract_input",
                    side_effect=AssertionError("正式 c03 不得执行完整输入校验"),
                ),
                mock.patch.object(
                    self.module,
                    "validate_contract_structure",
                    side_effect=AssertionError("正式 c03 不得重复证明业务规则"),
                ),
                mock.patch.object(
                    self.module,
                    "open_contract_dataset",
                    side_effect=record_open_contract_dataset,
                ),
                mock.patch.object(
                    self.module,
                    "validate_bar_calendar_frame",
                    wraps=original_calendar_validator,
                ) as calendar_validator_mock,
            ):
                result = CliRunner().invoke(
                    self.module.main,
                    ("--lake-root", str(lake_root), "--write"),
                )

            daily_df = self.module.read_existing_partition_leaf(
                target_path,
                ("1d", "XSGE", 2024, 1),
            )
            minute_df = self.module.read_existing_partition_leaf(
                target_path,
                ("1m", "XSGE", 2024, 1),
            )

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("partitions=2", result.output)
        self.assertEqual(len(daily_df), 2)
        self.assertEqual(len(minute_df), 4)
        self.assertTrue(daily_df["is_fetch_required"].all())
        self.assertFalse(minute_df["is_fetch_required"].any())
        self.assertEqual(
            [
                call.args[1]
                for call in calendar_validator_mock.call_args_list
            ],
            [
                "提交前 dirty 完整分区",
                "提交前 dirty 完整分区",
            ],
        )
        self.assertEqual(
            open_dataset_labels,
            [
                "上游合约 Session 日历",
                "现有行情日历",
            ],
        )

    def test_automatic_run_keeps_future_target_only_orphan_leaves(self) -> None:
        upstream_contract_df = self.contract_frame(("RB2405.XSGE",))
        current_partitions = self.module.build_fresh_partitions(
            upstream_contract_df,
            self.updated_at,
        )
        future_orphan_partitions = self.module.build_fresh_partitions(
            self.contract_frame(
                ("RB2406.XSGE",),
                trading_date_value=date(2024, 2, 2),
            ),
            self.updated_at,
        )
        existing_calendar_df = pd.concat(
            [
                current_partitions["1d"],
                current_partitions["1m"],
                future_orphan_partitions["1d"],
                future_orphan_partitions["1m"],
            ],
            ignore_index=True,
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            silver_root = lake_root / "silver"
            target_path = silver_root / self.module.TABLE_NAME
            ds.write_dataset(
                self.module.pandas_to_arrow(
                    upstream_contract_df,
                    self.module.FUTURES_CONTRACT_CALENDAR_SCHEMA,
                ),
                silver_root / self.module.UPSTREAM_TABLE_NAME,
                format="parquet",
                partitioning=self.module.UPSTREAM_PARTITIONING,
            )
            ds.write_dataset(
                self.module.pandas_to_arrow(
                    existing_calendar_df,
                    self.module.FUTURES_BAR_CALENDAR_SCHEMA,
                ),
                target_path,
                format="parquet",
                partitioning=self.module.HIVE_PARTITIONING,
            )

            with mock.patch.object(
                self.module,
                "commit_partition",
                side_effect=AssertionError("默认尾部不得清退未来孤儿叶"),
            ) as commit_mock:
                result = CliRunner().invoke(
                    self.module.main,
                    ("--lake-root", str(lake_root), "--write"),
                )

            future_daily_df = self.module.read_existing_partition_leaf(
                target_path,
                ("1d", "XSGE", 2024, 2),
            )
            future_minute_df = self.module.read_existing_partition_leaf(
                target_path,
                ("1m", "XSGE", 2024, 2),
            )

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("up_to_date:", result.output)
        self.assertIn("evaluated_partition_count=0", result.output)
        commit_mock.assert_not_called()
        self.assertEqual(
            set(future_daily_df["contract_code"]),
            {"RB2406.XSGE"},
        )
        self.assertEqual(
            set(future_minute_df["contract_code"]),
            {"RB2406.XSGE"},
        )

    def test_automatic_run_ignores_old_structure_change_in_common_tail_month(
        self,
    ) -> None:
        upstream_contract_df = self.contract_frame(("RB2405.XSGE",))
        existing_by_frequency = self.module.build_fresh_partitions(
            upstream_contract_df,
            self.updated_at,
        )
        existing_by_frequency["1d"].loc[:, "expected_bar_count"] = 2
        existing_calendar_df = pd.concat(
            [
                existing_by_frequency["1d"],
                existing_by_frequency["1m"],
            ],
            ignore_index=True,
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            silver_root = lake_root / "silver"
            target_path = silver_root / self.module.TABLE_NAME
            ds.write_dataset(
                self.module.pandas_to_arrow(
                    upstream_contract_df,
                    self.module.FUTURES_CONTRACT_CALENDAR_SCHEMA,
                ),
                silver_root / self.module.UPSTREAM_TABLE_NAME,
                format="parquet",
                partitioning=self.module.UPSTREAM_PARTITIONING,
            )
            ds.write_dataset(
                self.module.pandas_to_arrow(
                    existing_calendar_df,
                    self.module.FUTURES_BAR_CALENDAR_SCHEMA,
                ),
                target_path,
                format="parquet",
                partitioning=self.module.HIVE_PARTITIONING,
            )

            with mock.patch.object(
                self.module,
                "commit_partition",
                side_effect=AssertionError("默认尾部不得修订旧结构"),
            ) as commit_mock:
                result = CliRunner().invoke(
                    self.module.main,
                    ("--lake-root", str(lake_root), "--write"),
                )

            unchanged_daily_df = self.module.read_existing_partition_leaf(
                target_path,
                ("1d", "XSGE", 2024, 1),
            )

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("up_to_date:", result.output)
        self.assertIn("changed_grid_count=0", result.output)
        commit_mock.assert_not_called()
        self.assertEqual(
            unchanged_daily_df["expected_bar_count"].astype(int).tolist(),
            [2],
        )

    def test_automatic_run_appends_next_trading_date_in_same_month(self) -> None:
        old_contract_df = self.contract_frame(
            ("RB2405.XSGE",),
            trading_date_value=date(2024, 1, 3),
        )
        new_contract_df = self.contract_frame(
            ("RB2405.XSGE",),
            trading_date_value=date(2024, 1, 4),
        )
        upstream_contract_df = pd.concat(
            [old_contract_df, new_contract_df],
            ignore_index=True,
        )
        old_by_frequency = self.module.build_fresh_partitions(
            old_contract_df,
            self.updated_at,
        )
        existing_calendar_df = pd.concat(
            [old_by_frequency["1d"], old_by_frequency["1m"]],
            ignore_index=True,
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            silver_root = lake_root / "silver"
            target_path = silver_root / self.module.TABLE_NAME
            ds.write_dataset(
                self.module.pandas_to_arrow(
                    upstream_contract_df,
                    self.module.FUTURES_CONTRACT_CALENDAR_SCHEMA,
                ),
                silver_root / self.module.UPSTREAM_TABLE_NAME,
                format="parquet",
                partitioning=self.module.UPSTREAM_PARTITIONING,
            )
            ds.write_dataset(
                self.module.pandas_to_arrow(
                    existing_calendar_df,
                    self.module.FUTURES_BAR_CALENDAR_SCHEMA,
                ),
                target_path,
                format="parquet",
                partitioning=self.module.HIVE_PARTITIONING,
            )

            result = CliRunner().invoke(
                self.module.main,
                ("--lake-root", str(lake_root), "--write"),
            )

            actual_by_frequency = {
                frequency: self.module.read_existing_partition_leaf(
                    target_path,
                    (frequency, "XSGE", 2024, 1),
                )
                for frequency in ("1d", "1m")
            }

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("missing_grid_count=2", result.output)
        self.assertIn("partitions=2", result.output)
        for frequency in ("1d", "1m"):
            actual_df = actual_by_frequency[frequency]
            self.assertEqual(
                set(actual_df["trading_date"]),
                {date(2024, 1, 3), date(2024, 1, 4)},
            )
            old_rows = actual_df.loc[
                actual_df["trading_date"].eq(date(2024, 1, 3))
            ]
            self.assertTrue(old_rows["updated_at"].eq(self.updated_at).all())

    def test_automatic_run_builds_all_history_for_missing_frequency(self) -> None:
        january_contract_df = self.contract_frame(
            ("RB2405.XSGE",),
            trading_date_value=date(2024, 1, 3),
        )
        february_contract_df = self.contract_frame(
            ("RB2405.XSGE",),
            trading_date_value=date(2024, 2, 2),
        )
        upstream_contract_df = pd.concat(
            [january_contract_df, february_contract_df],
            ignore_index=True,
        )
        existing_daily_df = pd.concat(
            [
                self.module.build_fresh_partitions(
                    january_contract_df,
                    self.updated_at,
                )["1d"],
                self.module.build_fresh_partitions(
                    february_contract_df,
                    self.updated_at,
                )["1d"],
            ],
            ignore_index=True,
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            silver_root = lake_root / "silver"
            target_path = silver_root / self.module.TABLE_NAME
            ds.write_dataset(
                self.module.pandas_to_arrow(
                    upstream_contract_df,
                    self.module.FUTURES_CONTRACT_CALENDAR_SCHEMA,
                ),
                silver_root / self.module.UPSTREAM_TABLE_NAME,
                format="parquet",
                partitioning=self.module.UPSTREAM_PARTITIONING,
            )
            ds.write_dataset(
                self.module.pandas_to_arrow(
                    existing_daily_df,
                    self.module.FUTURES_BAR_CALENDAR_SCHEMA,
                ),
                target_path,
                format="parquet",
                partitioning=self.module.HIVE_PARTITIONING,
            )

            result = CliRunner().invoke(
                self.module.main,
                ("--lake-root", str(lake_root), "--write"),
            )

            january_minute_df = self.module.read_existing_partition_leaf(
                target_path,
                ("1m", "XSGE", 2024, 1),
            )
            february_minute_df = self.module.read_existing_partition_leaf(
                target_path,
                ("1m", "XSGE", 2024, 2),
            )

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("missing_grid_count=2", result.output)
        self.assertIn("partitions=2", result.output)
        self.assertEqual(
            set(january_minute_df["trading_date"]),
            {date(2024, 1, 3)},
        )
        self.assertEqual(
            set(february_minute_df["trading_date"]),
            {date(2024, 2, 2)},
        )

    def test_main_plans_internal_gap_missing_frequency_and_orphan_leaves(self) -> None:
        january_contract_df = self.contract_frame(
            (
                "RB2405.XSGE",
                "HC2405.XSGE",
                "CU2405.XSGE",
            )
        )
        february_contract_df = self.contract_frame(
            ("RB2405.XSGE",),
            trading_date_value=date(2024, 2, 2),
        )
        upstream_df = pd.concat(
            [january_contract_df, february_contract_df],
            ignore_index=True,
        )

        january_partitions = self.module.build_fresh_partitions(
            january_contract_df,
            self.updated_at,
        )
        january_daily_with_internal_gap = january_partitions["1d"].loc[
            ~january_partitions["1d"]["contract_code"].eq("HC2405.XSGE")
        ]
        february_daily_df = self.module.build_fresh_partitions(
            february_contract_df,
            self.updated_at,
        )["1d"]
        orphan_partitions = self.module.build_fresh_partitions(
            self.contract_frame(
                ("RB2312.XSGE",),
                trading_date_value=date(2023, 12, 29),
            ),
            self.updated_at,
        )
        existing_calendar_df = pd.concat(
            [
                january_daily_with_internal_gap,
                january_partitions["1m"],
                february_daily_df,
                orphan_partitions["1d"],
                orphan_partitions["1m"],
            ],
            ignore_index=True,
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            silver_root = lake_root / "silver"
            ds.write_dataset(
                self.module.pandas_to_arrow(
                    upstream_df,
                    self.module.FUTURES_CONTRACT_CALENDAR_SCHEMA,
                ),
                silver_root / self.module.UPSTREAM_TABLE_NAME,
                format="parquet",
                partitioning=self.module.UPSTREAM_PARTITIONING,
            )
            ds.write_dataset(
                self.module.pandas_to_arrow(
                    existing_calendar_df,
                    self.module.FUTURES_BAR_CALENDAR_SCHEMA,
                ),
                silver_root / self.module.TABLE_NAME,
                format="parquet",
                partitioning=self.module.HIVE_PARTITIONING,
            )

            result = CliRunner().invoke(
                self.module.main,
                ("--lake-root", str(lake_root), "--full"),
            )

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("upstream_grid_count=8", result.output)
        self.assertIn("complete_grid_count=6", result.output)
        self.assertIn("missing_grid_count=2", result.output)
        self.assertIn("changed_grid_count=0", result.output)
        self.assertIn("extra_grid_count=2", result.output)
        self.assertIn("touched_partition_count=4", result.output)
        self.assertIn("clean_partition_count=2", result.output)
        self.assertIn("evaluated_partition_count=6", result.output)

    def test_structural_comparison_counts_missing_changed_extra_and_complete(
        self,
    ) -> None:
        expected_codes = (
            "RB2405.XSGE",
            "HC2405.XSGE",
            "CU2405.XSGE",
            "AL2405.XSGE",
        )
        expected_df = self.module.build_structural_partitions(
            self.contract_frame(expected_codes).loc[
                :, self.module.UPSTREAM_STRUCTURE_COLUMNS
            ]
        )["1d"]
        extra_df = self.module.build_structural_partitions(
            self.contract_frame(("ZN2405.XSGE",)).loc[
                :, self.module.UPSTREAM_STRUCTURE_COLUMNS
            ]
        )["1d"]

        existing_df = expected_df.loc[
            ~expected_df["contract_code"].eq("RB2405.XSGE")
        ].copy()
        existing_df.loc[
            existing_df["contract_code"].eq("HC2405.XSGE"),
            "underlying_code",
        ] = "HC_CHANGED"
        existing_df = pd.concat([existing_df, extra_df], ignore_index=True)

        audit = self.module.assess_structural_partition(
            expected_df,
            existing_df,
            None,
        )

        self.assertEqual(audit["expected_row_count"], 4)
        self.assertEqual(audit["complete_count"], 2)
        self.assertEqual(audit["missing_count"], 1)
        self.assertEqual(audit["changed_count"], 1)
        self.assertEqual(audit["extra_count"], 1)
        self.assertIsNone(audit["quality_error"])
        self.assertTrue(audit["is_dirty"])

    def test_structural_comparison_marks_duplicate_primary_keys_dirty(self) -> None:
        expected_df = self.module.build_structural_partitions(
            self.contract_frame(("RB2405.XSGE",)).loc[
                :, self.module.UPSTREAM_STRUCTURE_COLUMNS
            ]
        )["1d"]
        duplicated_df = pd.concat([expected_df, expected_df], ignore_index=True)

        audit = self.module.assess_structural_partition(
            expected_df,
            duplicated_df,
            None,
        )

        self.assertEqual(audit["expected_row_count"], 1)
        self.assertEqual(audit["complete_count"], 0)
        self.assertEqual(audit["missing_count"], 1)
        self.assertEqual(audit["extra_count"], 2)
        self.assertIsNotNone(audit["quality_error"])
        self.assertTrue(audit["is_dirty"])

    def test_forced_quality_error_marks_all_common_rows_changed(self) -> None:
        expected_df = self.module.build_structural_partitions(
            self.contract_frame().loc[:, self.module.UPSTREAM_STRUCTURE_COLUMNS]
        )["1d"]

        audit = self.module.assess_structural_partition(
            expected_df,
            expected_df.copy(),
            "调用方强制结构质量失败。",
        )

        self.assertEqual(audit["expected_row_count"], 2)
        self.assertEqual(audit["complete_count"], 0)
        self.assertEqual(audit["missing_count"], 0)
        self.assertEqual(audit["changed_count"], 2)
        self.assertEqual(audit["extra_count"], 0)
        self.assertIsNotNone(audit["quality_error"])
        self.assertTrue(audit["is_dirty"])

    def test_open_contract_dataset_accepts_mixed_descriptive_metadata(self) -> None:
        daily_df = self.module.build_fresh_partitions(
            self.contract_frame(),
            self.updated_at,
        )["1d"]
        physical_calendar_table = self.module.pandas_to_arrow(
            daily_df,
            self.module.FUTURES_BAR_CALENDAR_SCHEMA,
        ).drop(self.module.PARTITION_COLUMNS)
        old_physical_calendar_table = self.calendar_table_with_schema_version(
            physical_calendar_table.slice(1, 1),
            "1.3.9",
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            target_path = pathlib.Path(temporary_directory) / self.module.TABLE_NAME
            leaf_path = target_path / self.module.partition_relative_path(
                ("1d", "XSGE", 2024, 1)
            )
            leaf_path.mkdir(parents=True)
            pq.write_table(
                physical_calendar_table.slice(0, 1),
                leaf_path / "part-0-current.parquet",
            )
            pq.write_table(
                old_physical_calendar_table,
                leaf_path / "part-1-old.parquet",
            )

            dataset = self.module.open_contract_dataset(
                target_path,
                self.module.HIVE_PARTITIONING,
                self.module.FUTURES_BAR_CALENDAR_SCHEMA,
                self.module.PARTITION_COLUMNS,
                "混合 metadata 测试表",
                required=True,
            )
            row_count = dataset.count_rows()

        self.assertIsNotNone(dataset)
        self.assertEqual(row_count, 2)

    def test_automatic_run_ignores_descriptive_metadata_without_rewrite(
        self,
    ) -> None:
        contract_df = self.contract_frame(session_count=2)
        expected_by_frequency = self.module.build_fresh_partitions(
            contract_df,
            self.updated_at,
        )
        existing_calendar_df = pd.concat(
            [expected_by_frequency["1d"], expected_by_frequency["1m"]],
            ignore_index=True,
        )
        old_calendar_table = self.calendar_table_with_schema_version(
            self.module.pandas_to_arrow(
                existing_calendar_df,
                self.module.FUTURES_BAR_CALENDAR_SCHEMA,
            ),
            "1.3.9",
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            silver_root = lake_root / "silver"
            target_path = silver_root / self.module.TABLE_NAME
            ds.write_dataset(
                self.module.pandas_to_arrow(
                    contract_df,
                    self.module.FUTURES_CONTRACT_CALENDAR_SCHEMA,
                ),
                silver_root / self.module.UPSTREAM_TABLE_NAME,
                format="parquet",
                partitioning=self.module.UPSTREAM_PARTITIONING,
            )
            ds.write_dataset(
                old_calendar_table,
                target_path,
                format="parquet",
                partitioning=self.module.HIVE_PARTITIONING,
            )

            with mock.patch.object(
                self.module,
                "commit_partition",
                side_effect=AssertionError("描述性 metadata 不得触发提交"),
            ) as commit_mock:
                result = CliRunner().invoke(
                    self.module.main,
                    ("--lake-root", str(lake_root), "--write"),
                )

            actual_by_frequency = {
                frequency: self.module.read_existing_partition_leaf(
                    target_path,
                    (frequency, "XSGE", 2024, 1),
                )
                for frequency in ("1d", "1m")
            }

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("up_to_date:", result.output)
        commit_mock.assert_not_called()
        for frequency in ("1d", "1m"):
            self.assertTrue(
                self.module.pandas_to_arrow(
                    actual_by_frequency[frequency],
                    self.module.FUTURES_BAR_CALENDAR_SCHEMA,
                ).equals(
                    self.module.pandas_to_arrow(
                        expected_by_frequency[frequency],
                        self.module.FUTURES_BAR_CALENDAR_SCHEMA,
                    )
                )
            )

    def test_explicit_write_accepts_descriptive_metadata_without_commit(
        self,
    ) -> None:
        contract_df = self.contract_frame(("RB2405.XSGE",))
        existing_by_frequency = self.module.build_fresh_partitions(
            contract_df,
            self.updated_at,
        )
        existing_calendar_df = pd.concat(
            [
                existing_by_frequency["1d"],
                existing_by_frequency["1m"],
            ],
            ignore_index=True,
        )
        old_calendar_table = self.calendar_table_with_schema_version(
            self.module.pandas_to_arrow(
                existing_calendar_df,
                self.module.FUTURES_BAR_CALENDAR_SCHEMA,
            ),
            "1.3.9",
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            silver_root = lake_root / "silver"
            ds.write_dataset(
                self.module.pandas_to_arrow(
                    contract_df,
                    self.module.FUTURES_CONTRACT_CALENDAR_SCHEMA,
                ),
                silver_root / self.module.UPSTREAM_TABLE_NAME,
                format="parquet",
                partitioning=self.module.UPSTREAM_PARTITIONING,
            )
            ds.write_dataset(
                old_calendar_table,
                silver_root / self.module.TABLE_NAME,
                format="parquet",
                partitioning=self.module.HIVE_PARTITIONING,
            )

            with mock.patch.object(
                self.module,
                "commit_partition",
                side_effect=AssertionError("描述性 metadata 不得触发提交"),
            ) as commit_mock:
                result = CliRunner().invoke(
                    self.module.main,
                    (
                        "--lake-root",
                        str(lake_root),
                        "--start-date",
                        "2024-01-03",
                        "--end-date",
                        "2024-01-03",
                        "--write",
                    ),
                )

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("up_to_date:", result.output)
        commit_mock.assert_not_called()

    def test_merge_preserves_every_state_column_and_resets_changed_structure(
        self,
    ) -> None:
        fresh_df = self.module.build_fresh_partitions(
            self.contract_frame(),
            self.updated_at,
        )["1m"]
        existing_df = fresh_df.copy()
        completed_at = datetime(2026, 8, 23, 1, 5, tzinfo=timezone.utc)
        existing_df.loc[:, "schedule_status"] = "suspected_closed"
        existing_df.loc[:, "schedule_signal_reason"] = "c07 定向校对旁证。"
        existing_df.loc[:, "evidence_level"] = "reconciled"
        existing_df.loc[:, "evidence_source"] = "c07_test_fixture"
        existing_df.loc[:, "is_fetch_required"] = True
        existing_df.loc[:, "selection_reason"] = "c06 白名单评估要求采集。"
        existing_df.loc[:, "is_fetch_completed"] = True
        existing_df.loc[:, "actual_bar_count"] = 2
        existing_df.loc[:, "is_data_missing"] = True
        existing_df.loc[:, "missing_bar_count"] = 1
        existing_df.loc[:, "fetch_run_id"] = "c06-completed-run"
        existing_df.loc[:, "fetch_completed_at"] = completed_at
        existing_df.loc[:, "missing_checked_at"] = completed_at
        existing_df.loc[:, "quality_status"] = "warning"
        existing_df.loc[:, "quality_reason"] = "c08 发现一根分钟线缺失。"
        for column, value in {
            "daily_open": 10.0,
            "daily_high": 12.0,
            "daily_low": 9.0,
            "daily_close": 11.0,
            "daily_volume": 100.0,
            "daily_money": 1000.0,
            "daily_open_interest": 500.0,
            "aggregated_open": 10.0,
            "aggregated_high": 12.0,
            "aggregated_low": 9.0,
            "aggregated_close": 11.0,
            "aggregated_volume": 90.0,
            "aggregated_money": 900.0,
            "aggregated_open_interest": 490.0,
        }.items():
            existing_df.loc[:, column] = value
        existing_df.loc[:, "ohlc_matches_daily"] = True
        existing_df.loc[:, "volume_matches_daily"] = False
        existing_df.loc[:, "money_matches_daily"] = False
        existing_df.loc[:, "open_interest_matches_daily"] = False
        existing_df.loc[:, "quality_checked_at"] = completed_at
        existing_df.loc[:, "updated_at"] = completed_at
        existing_df = self.module.validate_bar_calendar_frame(
            existing_df,
            "测试状态丰富的现有分区",
        )

        changed_fresh_df = fresh_df.copy()
        changed_fresh_df.loc[
            changed_fresh_df["contract_code"].eq("HC2405.XSGE"),
            "underlying_code",
        ] = "HC_CHANGED"
        merged_df = self.module.merge_preserving_state(
            changed_fresh_df,
            existing_df,
        )

        self.assertTrue(
            self.state_table_for_contract(merged_df, "RB2405.XSGE").equals(
                self.state_table_for_contract(existing_df, "RB2405.XSGE")
            )
        )
        self.assertTrue(
            self.state_table_for_contract(merged_df, "HC2405.XSGE").equals(
                self.state_table_for_contract(changed_fresh_df, "HC2405.XSGE")
            )
        )
        reset_row = merged_df.loc[
            merged_df["contract_code"].eq("HC2405.XSGE")
        ].iloc[0]
        self.assertFalse(reset_row["is_fetch_completed"])
        self.assertEqual(reset_row["quality_status"], "pending")
        self.assertEqual(reset_row["evidence_level"], "contract_rule")
        self.assertTrue(pd.isna(reset_row["missing_checked_at"]))

    def test_dirty_main_fails_closed_when_existing_state_is_invalid(self) -> None:
        upstream_contract_df = self.contract_frame(
            ("RB2405.XSGE", "HC2405.XSGE")
        )
        existing_daily_df = self.module.build_fresh_partitions(
            self.contract_frame(("RB2405.XSGE",)),
            self.updated_at,
        )["1d"]
        existing_daily_df.loc[:, "quality_status"] = "passed"

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            silver_root = lake_root / "silver"
            target_path = silver_root / self.module.TABLE_NAME
            ds.write_dataset(
                self.module.pandas_to_arrow(
                    upstream_contract_df,
                    self.module.FUTURES_CONTRACT_CALENDAR_SCHEMA,
                ),
                silver_root / self.module.UPSTREAM_TABLE_NAME,
                format="parquet",
                partitioning=self.module.UPSTREAM_PARTITIONING,
            )
            ds.write_dataset(
                self.module.pandas_to_arrow(
                    existing_daily_df,
                    self.module.FUTURES_BAR_CALENDAR_SCHEMA,
                ),
                target_path,
                format="parquet",
                partitioning=self.module.HIVE_PARTITIONING,
            )

            with mock.patch.object(
                self.module.ds,
                "write_dataset",
                side_effect=AssertionError("非法既有状态不得写 staging"),
            ) as staging_write_mock:
                result = CliRunner().invoke(
                    self.module.main,
                    (
                        "--lake-root",
                        str(lake_root),
                        "--full",
                        "--write",
                    ),
                )

            unchanged_daily_df = self.module.read_existing_partition_leaf(
                target_path,
                ("1d", "XSGE", 2024, 1),
            )

        self.assertNotEqual(result.exit_code, 0)
        self.assertIsInstance(result.exception, ValueError)
        self.assertIn("非 pending 质量状态缺少检查时间", str(result.exception))
        staging_write_mock.assert_not_called()
        self.assertTrue(
            self.module.pandas_to_arrow(
                unchanged_daily_df,
                self.module.FUTURES_BAR_CALENDAR_SCHEMA,
            ).equals(
                self.module.pandas_to_arrow(
                    existing_daily_df,
                    self.module.FUTURES_BAR_CALENDAR_SCHEMA,
                )
            )
        )

    def test_multi_file_leaf_is_discovered_once_and_read_completely(self) -> None:
        daily_df = self.module.build_fresh_partitions(
            self.contract_frame(),
            self.updated_at,
        )["1d"]
        with tempfile.TemporaryDirectory() as temporary_directory:
            table_path = pathlib.Path(temporary_directory) / self.module.TABLE_NAME
            ds.write_dataset(
                self.module.pandas_to_arrow(
                    daily_df,
                    self.module.FUTURES_BAR_CALENDAR_SCHEMA,
                ),
                table_path,
                format="parquet",
                partitioning=self.module.HIVE_PARTITIONING,
                basename_template="part-{i}.parquet",
                max_rows_per_file=1,
                max_rows_per_group=1,
            )

            parquet_files = list(table_path.rglob("*.parquet"))
            self.assertEqual(len(parquet_files), 2)
            partition_key = ("1d", "XSGE", 2024, 1)
            self.assertEqual(
                self.module.partition_keys_from_files(
                    table_path,
                    self.module.PARTITION_COLUMNS,
                ),
                {partition_key},
            )
            with mock.patch.object(
                self.module,
                "open_contract_dataset",
                side_effect=AssertionError("精确叶读取不得重开根 Dataset"),
            ) as open_dataset_mock:
                read_df = self.module.read_existing_partition_leaf(
                    table_path,
                    partition_key,
                )
            open_dataset_mock.assert_not_called()

        expected_table = self.module.pandas_to_arrow(
            daily_df.sort_values(self.module.PRIMARY_KEY).reset_index(drop=True),
            self.module.FUTURES_BAR_CALENDAR_SCHEMA,
        )
        actual_table = self.module.pandas_to_arrow(
            read_df.sort_values(self.module.PRIMARY_KEY).reset_index(drop=True),
            self.module.FUTURES_BAR_CALENDAR_SCHEMA,
        )
        self.assertTrue(actual_table.equals(expected_table))

    def test_commit_uses_short_paths_and_rereads_only_replaced_formal_leaf(
        self,
    ) -> None:
        old_daily_df = self.module.build_fresh_partitions(
            self.contract_frame(),
            self.updated_at,
        )["1d"]
        incoming_daily_df = self.module.build_fresh_partitions(
            self.contract_frame(("RB2405.XSGE",)),
            datetime(2026, 8, 23, 1, 10, tzinfo=timezone.utc),
        )["1d"]
        partition_key = ("1d", "XSGE", 2024, 1)
        fixed_run_id = "0123456789ab"

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            silver_root = lake_root / "silver"
            target_path = silver_root / self.module.TABLE_NAME
            ds.write_dataset(
                self.module.pandas_to_arrow(
                    old_daily_df,
                    self.module.FUTURES_BAR_CALENDAR_SCHEMA,
                ),
                target_path,
                format="parquet",
                partitioning=self.module.HIVE_PARTITIONING,
            )
            original_dataset = ds.dataset
            with (
                mock.patch.object(
                    self.module.uuid,
                    "uuid4",
                    return_value=types.SimpleNamespace(
                        hex=f"{fixed_run_id}ffff"
                    ),
                ),
                mock.patch.object(
                    self.module,
                    "open_contract_dataset",
                    side_effect=AssertionError("叶提交不得重开根 Dataset"),
                ) as open_dataset_mock,
                mock.patch.object(
                    self.module.ds,
                    "dataset",
                    wraps=original_dataset,
                ) as leaf_dataset_mock,
            ):
                committed_count = self.module.commit_partition(
                    incoming_daily_df,
                    lake_root,
                    partition_key,
                )

            self.assertEqual(committed_count, 1)
            open_dataset_mock.assert_not_called()
            dataset_paths = [
                pathlib.Path(call.args[0]).resolve()
                for call in leaf_dataset_mock.call_args_list
            ]
            formal_leaf_path = (
                target_path
                / self.module.partition_relative_path(partition_key)
            ).resolve()
            self.assertIn(formal_leaf_path, dataset_paths)
            self.assertNotIn(target_path.resolve(), dataset_paths)
            committed_df = self.module.read_existing_partition_leaf(
                target_path,
                partition_key,
            )
            for prefix in (".c04s-", ".c04b-", ".c04q-"):
                self.assertFalse(
                    (silver_root / f"{prefix}{fixed_run_id}").exists()
                )

        expected_table = self.module.pandas_to_arrow(
            incoming_daily_df,
            self.module.FUTURES_BAR_CALENDAR_SCHEMA,
        )
        committed_table = self.module.pandas_to_arrow(
            committed_df,
            self.module.FUTURES_BAR_CALENDAR_SCHEMA,
        )
        self.assertTrue(committed_table.equals(expected_table))

    def test_formal_leaf_reread_failure_restores_old_leaf(self) -> None:
        old_daily_df = self.module.build_fresh_partitions(
            self.contract_frame(),
            self.updated_at,
        )["1d"]
        incoming_daily_df = self.module.build_fresh_partitions(
            self.contract_frame(("RB2405.XSGE",)),
            datetime(2026, 8, 23, 1, 10, tzinfo=timezone.utc),
        )["1d"]
        partition_key = ("1d", "XSGE", 2024, 1)
        fixed_run_id = "fedcba987654"

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            silver_root = lake_root / "silver"
            target_path = silver_root / self.module.TABLE_NAME
            formal_leaf_path = (
                target_path
                / self.module.partition_relative_path(partition_key)
            ).resolve()
            ds.write_dataset(
                self.module.pandas_to_arrow(
                    old_daily_df,
                    self.module.FUTURES_BAR_CALENDAR_SCHEMA,
                ),
                target_path,
                format="parquet",
                partitioning=self.module.HIVE_PARTITIONING,
            )

            original_read_partition = self.module.read_partition
            formal_read_attempts = 0

            def fail_formal_leaf_read(
                dataset,
                schema,
                partition_columns,
                selected_partition_key,
                start_date=None,
                end_date=None,
            ):
                nonlocal formal_read_attempts
                dataset_files = [
                    pathlib.Path(path).resolve()
                    for path in dataset.files
                ]
                if any(
                    file_path.is_relative_to(formal_leaf_path)
                    for file_path in dataset_files
                ):
                    formal_read_attempts += 1
                    raise ValueError("模拟正式叶复读失败")
                return original_read_partition(
                    dataset,
                    schema,
                    partition_columns,
                    selected_partition_key,
                    start_date,
                    end_date,
                )

            with (
                mock.patch.object(
                    self.module.uuid,
                    "uuid4",
                    return_value=types.SimpleNamespace(
                        hex=f"{fixed_run_id}ffff"
                    ),
                ),
                mock.patch.object(
                    self.module,
                    "open_contract_dataset",
                    side_effect=AssertionError("失败回滚也不得重开根 Dataset"),
                ),
                mock.patch.object(
                    self.module,
                    "read_partition",
                    side_effect=fail_formal_leaf_read,
                ),
            ):
                with self.assertRaises(RuntimeError):
                    self.module.commit_partition(
                        incoming_daily_df,
                        lake_root,
                        partition_key,
                    )

            self.assertEqual(formal_read_attempts, 1)
            restored_df = self.module.read_existing_partition_leaf(
                target_path,
                partition_key,
            )
            quarantine_path = silver_root / f".c04q-{fixed_run_id}"
            self.assertTrue(quarantine_path.is_dir())
            self.assertTrue(any(quarantine_path.rglob("*.parquet")))
            self.assertFalse((silver_root / f".c04s-{fixed_run_id}").exists())
            self.assertFalse((silver_root / f".c04b-{fixed_run_id}").exists())

        expected_old_table = self.module.pandas_to_arrow(
            old_daily_df,
            self.module.FUTURES_BAR_CALENDAR_SCHEMA,
        )
        restored_table = self.module.pandas_to_arrow(
            restored_df,
            self.module.FUTURES_BAR_CALENDAR_SCHEMA,
        )
        self.assertTrue(restored_table.equals(expected_old_table))

    def test_explicit_non_formal_replacement_preserves_rows_outside_range(
        self,
    ) -> None:
        existing_daily_frames = []
        for trading_date_value in (
            date(2024, 1, 2),
            date(2024, 1, 3),
            date(2024, 1, 4),
        ):
            existing_daily_frames.append(
                self.module.build_fresh_partitions(
                    self.contract_frame(
                        ("RB2405.XSGE",),
                        trading_date_value=trading_date_value,
                    ),
                    self.updated_at,
                )["1d"]
            )
        existing_daily_df = pd.concat(
            existing_daily_frames,
            ignore_index=True,
        )
        incoming_daily_df = existing_daily_frames[1].copy()
        incoming_daily_df.loc[:, "underlying_code"] = "RB_REVISED"
        incoming_daily_df.loc[:, "updated_at"] = datetime(
            2026, 8, 23, 2, 0, tzinfo=timezone.utc
        )
        partition_key = ("1d", "XSGE", 2024, 1)

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            target_path = (
                lake_root / "silver" / self.module.TABLE_NAME
            )
            ds.write_dataset(
                self.module.pandas_to_arrow(
                    existing_daily_df,
                    self.module.FUTURES_BAR_CALENDAR_SCHEMA,
                ),
                target_path,
                format="parquet",
                partitioning=self.module.HIVE_PARTITIONING,
            )

            committed_count = self.module.commit_partition(
                incoming_daily_df,
                lake_root,
                partition_key,
                date(2024, 1, 3),
                date(2024, 1, 3),
            )
            committed_df = self.module.read_existing_partition_leaf(
                target_path,
                partition_key,
            )

        self.assertEqual(committed_count, 1)
        self.assertEqual(
            committed_df["trading_date"].tolist(),
            [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)],
        )
        self.assertEqual(
            committed_df.loc[
                committed_df["trading_date"].eq(date(2024, 1, 3)),
                "underlying_code",
            ].item(),
            "RB_REVISED",
        )
        outside_range_df = committed_df.loc[
            ~committed_df["trading_date"].eq(date(2024, 1, 3))
        ].reset_index(drop=True)
        expected_outside_range_df = existing_daily_df.loc[
            ~existing_daily_df["trading_date"].eq(date(2024, 1, 3))
        ].reset_index(drop=True)
        self.assertTrue(
            self.module.pandas_to_arrow(
                outside_range_df,
                self.module.FUTURES_BAR_CALENDAR_SCHEMA,
            ).equals(
                self.module.pandas_to_arrow(
                    expected_outside_range_df,
                    self.module.FUTURES_BAR_CALENDAR_SCHEMA,
                )
            )
        )

    def test_main_rejects_physically_incompatible_target_before_planning(self) -> None:
        contract_df = self.contract_frame(("RB2405.XSGE",))
        calendar_df = self.module.build_fresh_partitions(
            contract_df,
            self.updated_at,
        )["1d"]
        incompatible_table = self.module.pandas_to_arrow(
            calendar_df,
            self.module.FUTURES_BAR_CALENDAR_SCHEMA,
        ).drop(["quality_reason"])

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            silver_root = lake_root / "silver"
            ds.write_dataset(
                self.module.pandas_to_arrow(
                    contract_df,
                    self.module.FUTURES_CONTRACT_CALENDAR_SCHEMA,
                ),
                silver_root / self.module.UPSTREAM_TABLE_NAME,
                format="parquet",
                partitioning=self.module.UPSTREAM_PARTITIONING,
            )
            ds.write_dataset(
                incompatible_table,
                silver_root / self.module.TABLE_NAME,
                format="parquet",
                partitioning=self.module.HIVE_PARTITIONING,
            )

            with mock.patch.object(
                self.module,
                "build_structural_partitions",
                side_effect=AssertionError("物理契约失败后不得进入规划"),
            ) as planning_mock:
                result = CliRunner().invoke(
                    self.module.main,
                    ("--lake-root", str(lake_root)),
                )

        self.assertNotEqual(result.exit_code, 0)
        self.assertIsInstance(result.exception, TypeError)
        planning_mock.assert_not_called()

    def test_staging_reread_failure_leaves_old_leaf_untouched(self) -> None:
        old_daily_df = self.module.build_fresh_partitions(
            self.contract_frame(),
            self.updated_at,
        )["1d"]
        incoming_daily_df = self.module.build_fresh_partitions(
            self.contract_frame(("RB2405.XSGE",)),
            datetime(2026, 8, 23, 2, 5, tzinfo=timezone.utc),
        )["1d"]
        partition_key = ("1d", "XSGE", 2024, 1)
        fixed_run_id = "111122223333"

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            silver_root = lake_root / "silver"
            target_path = silver_root / self.module.TABLE_NAME
            ds.write_dataset(
                self.module.pandas_to_arrow(
                    old_daily_df,
                    self.module.FUTURES_BAR_CALENDAR_SCHEMA,
                ),
                target_path,
                format="parquet",
                partitioning=self.module.HIVE_PARTITIONING,
            )
            original_read_partition = self.module.read_partition

            def fail_staging_read(
                dataset,
                schema,
                partition_columns,
                selected_partition_key,
                start_date=None,
                end_date=None,
            ):
                if any(".c04s-" in str(path) for path in dataset.files):
                    raise ValueError("模拟 staging 复读失败")
                return original_read_partition(
                    dataset,
                    schema,
                    partition_columns,
                    selected_partition_key,
                    start_date,
                    end_date,
                )

            with (
                mock.patch.object(
                    self.module.uuid,
                    "uuid4",
                    return_value=types.SimpleNamespace(
                        hex=f"{fixed_run_id}ffff"
                    ),
                ),
                mock.patch.object(
                    self.module,
                    "read_partition",
                    side_effect=fail_staging_read,
                ),
            ):
                with self.assertRaisesRegex(ValueError, "staging"):
                    self.module.commit_partition(
                        incoming_daily_df,
                        lake_root,
                        partition_key,
                    )

            restored_df = self.module.read_existing_partition_leaf(
                target_path,
                partition_key,
            )
            for prefix in (".c04s-", ".c04b-", ".c04q-"):
                self.assertFalse(
                    (silver_root / f"{prefix}{fixed_run_id}").exists()
                )

        self.assertTrue(
            self.module.pandas_to_arrow(
                restored_df,
                self.module.FUTURES_BAR_CALENDAR_SCHEMA,
            ).equals(
                self.module.pandas_to_arrow(
                    old_daily_df,
                    self.module.FUTURES_BAR_CALENDAR_SCHEMA,
                )
            )
        )

    def test_first_move_failure_does_not_displace_old_leaf(self) -> None:
        old_daily_df = self.module.build_fresh_partitions(
            self.contract_frame(),
            self.updated_at,
        )["1d"]
        incoming_daily_df = self.module.build_fresh_partitions(
            self.contract_frame(("RB2405.XSGE",)),
            datetime(2026, 8, 23, 2, 10, tzinfo=timezone.utc),
        )["1d"]
        partition_key = ("1d", "XSGE", 2024, 1)
        fixed_run_id = "444455556666"

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            silver_root = lake_root / "silver"
            target_path = silver_root / self.module.TABLE_NAME
            ds.write_dataset(
                self.module.pandas_to_arrow(
                    old_daily_df,
                    self.module.FUTURES_BAR_CALENDAR_SCHEMA,
                ),
                target_path,
                format="parquet",
                partitioning=self.module.HIVE_PARTITIONING,
            )
            original_move = self.module.shutil.move
            move_call_count = 0

            def fail_first_move(source, destination):
                nonlocal move_call_count
                move_call_count += 1
                if move_call_count == 1:
                    raise OSError("模拟旧叶移入备份失败")
                return original_move(source, destination)

            with (
                mock.patch.object(
                    self.module.uuid,
                    "uuid4",
                    return_value=types.SimpleNamespace(
                        hex=f"{fixed_run_id}ffff"
                    ),
                ),
                mock.patch.object(
                    self.module.shutil,
                    "move",
                    side_effect=fail_first_move,
                ),
            ):
                with self.assertRaises(OSError):
                    self.module.commit_partition(
                        incoming_daily_df,
                        lake_root,
                        partition_key,
                    )

            restored_df = self.module.read_existing_partition_leaf(
                target_path,
                partition_key,
            )

        self.assertEqual(move_call_count, 1)
        self.assertTrue(
            self.module.pandas_to_arrow(
                restored_df,
                self.module.FUTURES_BAR_CALENDAR_SCHEMA,
            ).equals(
                self.module.pandas_to_arrow(
                    old_daily_df,
                    self.module.FUTURES_BAR_CALENDAR_SCHEMA,
                )
            )
        )

    def test_second_move_failure_restores_old_leaf_from_backup(self) -> None:
        old_daily_df = self.module.build_fresh_partitions(
            self.contract_frame(),
            self.updated_at,
        )["1d"]
        incoming_daily_df = self.module.build_fresh_partitions(
            self.contract_frame(("RB2405.XSGE",)),
            datetime(2026, 8, 23, 2, 15, tzinfo=timezone.utc),
        )["1d"]
        partition_key = ("1d", "XSGE", 2024, 1)
        fixed_run_id = "777788889999"

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            silver_root = lake_root / "silver"
            target_path = silver_root / self.module.TABLE_NAME
            ds.write_dataset(
                self.module.pandas_to_arrow(
                    old_daily_df,
                    self.module.FUTURES_BAR_CALENDAR_SCHEMA,
                ),
                target_path,
                format="parquet",
                partitioning=self.module.HIVE_PARTITIONING,
            )
            original_move = self.module.shutil.move
            move_call_count = 0

            def fail_second_move(source, destination):
                nonlocal move_call_count
                move_call_count += 1
                if move_call_count == 2:
                    raise OSError("模拟新叶从 staging 安装失败")
                return original_move(source, destination)

            with (
                mock.patch.object(
                    self.module.uuid,
                    "uuid4",
                    return_value=types.SimpleNamespace(
                        hex=f"{fixed_run_id}ffff"
                    ),
                ),
                mock.patch.object(
                    self.module.shutil,
                    "move",
                    side_effect=fail_second_move,
                ),
            ):
                with self.assertRaisesRegex(OSError, "staging"):
                    self.module.commit_partition(
                        incoming_daily_df,
                        lake_root,
                        partition_key,
                    )

            restored_df = self.module.read_existing_partition_leaf(
                target_path,
                partition_key,
            )
            for prefix in (".c04s-", ".c04b-", ".c04q-"):
                self.assertFalse(
                    (silver_root / f"{prefix}{fixed_run_id}").exists()
                )

        self.assertEqual(move_call_count, 3)
        self.assertTrue(
            self.module.pandas_to_arrow(
                restored_df,
                self.module.FUTURES_BAR_CALENDAR_SCHEMA,
            ).equals(
                self.module.pandas_to_arrow(
                    old_daily_df,
                    self.module.FUTURES_BAR_CALENDAR_SCHEMA,
                )
            )
        )


if __name__ == "__main__":
    unittest.main()
