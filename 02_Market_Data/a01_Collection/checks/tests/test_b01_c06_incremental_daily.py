"""c06 日常窄列规划、可信完成凭证与定向提交回归。"""

from __future__ import annotations

import pathlib
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from unittest import mock

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from click.testing import CliRunner


project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()
for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        PROJECT_ROOT = candidate_root
        sys.path.insert(0, str(candidate_root))
        break
else:
    raise RuntimeError("未找到项目根目录")
WORKFLOW_ROOT = (
    PROJECT_ROOT
    / "02_Market_Data/a01_Collection"
    / "b01_Futures_Market_Data"
)
for import_path in (PROJECT_ROOT, WORKFLOW_ROOT):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

import c06_futures_minute as minute  # noqa: E402
from config.data_contracts import (  # noqa: E402
    FUTURES_BAR_CALENDAR_SCHEMA,
    FUTURES_MINUTE_SCHEMA,
    arrow_to_pandas,
    pandas_to_arrow,
)


TRADING_DATE = pd.Timestamp("2026-08-03").date()
SESSION_START = pd.Timestamp("2026-08-03 09:00", tz="Asia/Shanghai")
SESSION_END = pd.Timestamp("2026-08-03 09:01", tz="Asia/Shanghai")
CHECKED_AT = datetime(2026, 8, 23, tzinfo=timezone.utc)


def calendar_row(
    *,
    completed: bool = True,
    contract_code: str = "RB2609.XSGE",
    session_start_at: pd.Timestamp = SESSION_START,
    session_end_at: pd.Timestamp = SESSION_END,
    expected_bar_count: int = 1,
    is_fetch_required: bool = True,
) -> dict[str, object]:
    row = {
        "bar_frequency": "1m",
        "contract_code": contract_code,
        "exchange_code": "XSGE",
        "underlying_code": "RB",
        "trading_date": TRADING_DATE,
        "session_number": 1,
        "session_text": (
            f"{session_start_at:%H:%M}-{session_end_at:%H:%M}"
        ),
        "session_start_at": session_start_at,
        "session_end_at": session_end_at,
        "is_night_session": False,
        "schedule_status": "scheduled",
        "schedule_signal_reason": "合约规则计划开市。",
        "evidence_level": "contract_rule",
        "evidence_source": "contract-rule-v1",
        "is_fetch_required": is_fetch_required,
        "expected_bar_count": expected_bar_count,
        "selection_reason": (
            minute.SELECTED_REASON
            if is_fetch_required
            else minute.EXCLUDED_REASON
        ),
        "is_fetch_completed": completed,
        "actual_bar_count": expected_bar_count if completed else 0,
        "is_data_missing": False,
        "missing_bar_count": 0,
        "fetch_run_id": "original-run" if completed else None,
        "fetch_completed_at": CHECKED_AT if completed else None,
        "missing_checked_at": CHECKED_AT if completed else None,
        "quality_status": "passed" if completed else "pending",
        "quality_reason": "既有成功质量结论。" if completed else "等待采集。",
        "quality_checked_at": CHECKED_AT if completed else None,
        "updated_at": CHECKED_AT,
        "year": 2026,
        "month": 8,
    }
    for column in minute.EVIDENCE_COLUMNS:
        row[column] = None
    return row


def minute_row(
    *,
    invalid_ohlc: bool = False,
    contract_code: str = "RB2609.XSGE",
    bar_at: pd.Timestamp = SESSION_END,
) -> dict[str, object]:
    return {
        "contract_code": contract_code,
        "exchange_code": "XSGE",
        "underlying_code": "RB",
        "trading_date": TRADING_DATE,
        "session_number": 1,
        "bar_at": bar_at,
        "open": 100.0,
        "high": 99.0 if invalid_ohlc else 101.0,
        "low": 98.0,
        "close": 99.0,
        "volume": 1.0,
        "money": 100.0,
        "open_interest": 2.0,
        "source": minute.SOURCE_NAME,
        "updated_at": CHECKED_AT,
        "year": 2026,
        "month": 8,
    }


def write_partitioned(
    frame: pd.DataFrame,
    table_path: pathlib.Path,
    schema: pa.Schema,
    partition_columns: list[str],
) -> None:
    partitioning = ds.partitioning(
        pa.schema([schema.field(name) for name in partition_columns]),
        flavor="hive",
    )
    ds.write_dataset(
        pandas_to_arrow(frame.loc[:, schema.names], schema),
        table_path,
        format="parquet",
        partitioning=partitioning,
    )


def schema_with_legacy_descriptions(schema: pa.Schema) -> pa.Schema:
    schema_metadata = dict(schema.metadata or {})
    schema_metadata[b"description"] = b"legacy table description"
    fields = []
    for field in schema:
        field_metadata = dict(field.metadata or {})
        field_metadata[b"description"] = b"legacy field description"
        fields.append(pa.field(
            field.name,
            field.type,
            nullable=field.nullable,
            metadata=field_metadata,
        ))
    return pa.schema(fields, metadata=schema_metadata)


class C06IncrementalDailyTest(unittest.TestCase):
    def temporary_lake(self) -> tuple[tempfile.TemporaryDirectory, pathlib.Path]:
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        return temporary_directory, pathlib.Path(temporary_directory.name)

    def test_policy_contraction_and_reexpansion_preserve_completion_evidence(self) -> None:
        _, lake_root = self.temporary_lake()
        calendar_df = pd.DataFrame([calendar_row()]).loc[
            :,
            FUTURES_BAR_CALENDAR_SCHEMA.names,
        ]
        write_partitioned(
            calendar_df,
            lake_root / "silver" / minute.CALENDAR_TABLE_NAME,
            FUTURES_BAR_CALENDAR_SCHEMA,
            minute.CALENDAR_PARTITION_COLUMNS,
        )
        policy_df = calendar_df.loc[:, minute.CALENDAR_PRIMARY_KEY].copy()
        policy_df["_desired_required"] = False
        result = minute.commit_calendar_updates(
            {("1m", "XSGE", 2026, 8): policy_df},
            {},
            lake_root,
            "unused-run",
            CHECKED_AT,
        )
        self.assertEqual(result, (1, 0, 1))
        contracted_df = minute.read_complete_partition(
            lake_root / "silver" / minute.CALENDAR_TABLE_NAME,
            FUTURES_BAR_CALENDAR_SCHEMA,
            minute.CALENDAR_PARTITION_COLUMNS,
            ("1m", "XSGE", 2026, 8),
        )
        contracted_row = contracted_df.iloc[0]
        self.assertFalse(contracted_row["is_fetch_required"])
        self.assertTrue(contracted_row["is_fetch_completed"])
        self.assertEqual(contracted_row["fetch_run_id"], "original-run")
        self.assertEqual(contracted_row["quality_status"], "passed")
        self.assertEqual(contracted_row["evidence_source"], "contract-rule-v1")

        policy_df["_desired_required"] = True
        minute.commit_calendar_updates(
            {("1m", "XSGE", 2026, 8): policy_df},
            {},
            lake_root,
            "unused-run",
            CHECKED_AT,
        )
        expanded_df = minute.read_complete_partition(
            lake_root / "silver" / minute.CALENDAR_TABLE_NAME,
            FUTURES_BAR_CALENDAR_SCHEMA,
            minute.CALENDAR_PARTITION_COLUMNS,
            ("1m", "XSGE", 2026, 8),
        )
        expanded_row = expanded_df.iloc[0]
        self.assertTrue(expanded_row["is_fetch_required"])
        self.assertTrue(expanded_row["is_fetch_completed"])
        self.assertEqual(expanded_row["fetch_run_id"], "original-run")

    def test_zero_and_partial_completion_share_one_calendar_commit(self) -> None:
        _, lake_root = self.temporary_lake()
        partial_end = pd.Timestamp(
            "2026-08-03 09:02", tz="Asia/Shanghai"
        )
        zero_row = calendar_row(
            completed=False,
            contract_code="RB2609.XSGE",
            is_fetch_required=False,
        )
        partial_row = calendar_row(
            completed=False,
            contract_code="RB2610.XSGE",
            session_end_at=partial_end,
            expected_bar_count=2,
            is_fetch_required=False,
        )
        calendar_df = pd.DataFrame([zero_row, partial_row]).loc[
            :, FUTURES_BAR_CALENDAR_SCHEMA.names
        ]
        write_partitioned(
            calendar_df,
            lake_root / "silver" / minute.CALENDAR_TABLE_NAME,
            FUTURES_BAR_CALENDAR_SCHEMA,
            minute.CALENDAR_PARTITION_COLUMNS,
        )
        fact_df = pd.DataFrame([
            minute_row(
                contract_code="RB2610.XSGE",
                bar_at=SESSION_END,
            )
        ]).loc[:, FUTURES_MINUTE_SCHEMA.names]
        completion_updates = minute.build_calendar_completion_updates(
            calendar_df,
            fact_df,
            {},
        )
        policy_df = calendar_df.loc[:, minute.CALENDAR_PRIMARY_KEY].copy()
        policy_df["_desired_required"] = True

        with mock.patch.object(
            minute,
            "commit_complete_partition",
            wraps=minute.commit_complete_partition,
        ) as commit_partition:
            result = minute.commit_calendar_updates(
                {("1m", "XSGE", 2026, 8): policy_df},
                completion_updates,
                lake_root,
                "new-run",
                CHECKED_AT,
            )
        self.assertEqual(result, (2, 2, 1))
        self.assertEqual(commit_partition.call_count, 1)

        committed_df = minute.read_complete_partition(
            lake_root / "silver" / minute.CALENDAR_TABLE_NAME,
            FUTURES_BAR_CALENDAR_SCHEMA,
            minute.CALENDAR_PARTITION_COLUMNS,
            ("1m", "XSGE", 2026, 8),
        ).set_index("contract_code")
        zero = committed_df.loc["RB2609.XSGE"]
        self.assertTrue(zero["is_fetch_completed"])
        self.assertEqual(zero["actual_bar_count"], 0)
        self.assertEqual(zero["missing_bar_count"], 1)
        self.assertEqual(zero["quality_status"], "warning")
        self.assertEqual(zero["schedule_status"], "suspected_closed")
        partial = committed_df.loc["RB2610.XSGE"]
        self.assertTrue(partial["is_fetch_completed"])
        self.assertEqual(partial["actual_bar_count"], 1)
        self.assertEqual(partial["missing_bar_count"], 1)
        self.assertEqual(partial["quality_status"], "warning")

    def test_out_of_session_api_row_is_hard_failure(self) -> None:
        sessions_df = pd.DataFrame([calendar_row(completed=False)])

        class FakeJQData:
            @staticmethod
            def get_price(*args: object, **kwargs: object) -> pd.DataFrame:
                return pd.DataFrame(
                    {
                        "open": [1.0],
                        "high": [1.0],
                        "low": [1.0],
                        "close": [1.0],
                        "volume": [1.0],
                        "money": [1.0],
                        "open_interest": [1.0],
                    },
                    index=pd.DatetimeIndex(
                        ["2026-08-03 09:02"],
                        name="time",
                    ),
                )

        with self.assertRaisesRegex(ValueError, "越出请求 Session"):
            minute.collect_partition(FakeJQData(), sessions_df, CHECKED_AT)

    def test_collect_partition_accepts_us_sessions_and_ns_api_bars(self) -> None:
        calendar_df = pd.DataFrame([
            calendar_row(completed=False)
        ]).loc[:, FUTURES_BAR_CALENDAR_SCHEMA.names]
        sessions_df = arrow_to_pandas(
            pandas_to_arrow(calendar_df, FUTURES_BAR_CALENDAR_SCHEMA),
            FUTURES_BAR_CALENDAR_SCHEMA,
        )
        session_start_at_index = pd.DatetimeIndex(
            pd.to_datetime(sessions_df["session_start_at"])
        )
        api_bar_at_index = pd.DatetimeIndex(
            ["2026-08-03 09:01:00"],
            dtype="datetime64[ns]",
            name="time",
        )
        self.assertEqual(session_start_at_index.unit, "us")
        self.assertEqual(api_bar_at_index.unit, "ns")

        fake_jqdata = mock.Mock()
        fake_jqdata.get_price.return_value = pd.DataFrame(
            {
                "open": [1.0],
                "high": [1.0],
                "low": [1.0],
                "close": [1.0],
                "volume": [1.0],
                "money": [1.0],
                "open_interest": [1.0],
            },
            index=api_bar_at_index,
        )

        collected_df, returned_rows, invalid_session_counts = (
            minute.collect_partition(
                fake_jqdata,
                sessions_df,
                CHECKED_AT,
            )
        )

        fake_jqdata.get_price.assert_called_once()
        self.assertEqual(returned_rows, 1)
        self.assertEqual(invalid_session_counts, {})
        self.assertEqual(len(collected_df), 1)
        self.assertEqual(collected_df["session_number"].iloc[0], 1)
        self.assertEqual(collected_df["bar_at"].iloc[0], SESSION_END)
        self.assertEqual(
            collected_df["bar_at"].dtype,
            pd.ArrowDtype(FUTURES_MINUTE_SCHEMA.field("bar_at").type),
        )

    def test_off_grid_api_row_is_hard_failure_before_fact_commit(self) -> None:
        sessions_df = pd.DataFrame([calendar_row(completed=False)])

        class FakeJQData:
            @staticmethod
            def get_price(*args: object, **kwargs: object) -> pd.DataFrame:
                return pd.DataFrame(
                    {
                        "open": [1.0],
                        "high": [1.0],
                        "low": [1.0],
                        "close": [1.0],
                        "volume": [1.0],
                        "money": [1.0],
                        "open_interest": [1.0],
                    },
                    index=pd.DatetimeIndex(
                        ["2026-08-03 09:00:30"],
                        name="time",
                    ),
                )

        with self.assertRaisesRegex(ValueError, "理论分钟格点"):
            minute.collect_partition(FakeJQData(), sessions_df, CHECKED_AT)

    def test_invalid_ohlc_warning_does_not_overwrite_schedule_evidence(self) -> None:
        _, lake_root = self.temporary_lake()
        pending_calendar_df = pd.DataFrame([calendar_row(completed=False)]).loc[
            :,
            FUTURES_BAR_CALENDAR_SCHEMA.names,
        ]
        write_partitioned(
            pending_calendar_df,
            lake_root / "silver" / minute.CALENDAR_TABLE_NAME,
            FUTURES_BAR_CALENDAR_SCHEMA,
            minute.CALENDAR_PARTITION_COLUMNS,
        )
        fact_df = pd.DataFrame([minute_row(invalid_ohlc=True)]).loc[
            :,
            FUTURES_MINUTE_SCHEMA.names,
        ]
        completion_updates = minute.build_calendar_completion_updates(
            pending_calendar_df,
            fact_df,
            {("RB2609.XSGE", TRADING_DATE, 1): 1},
        )
        updated = minute.commit_calendar_updates(
            {},
            completion_updates,
            lake_root,
            "new-run",
            CHECKED_AT,
        )
        self.assertEqual(updated, (0, 1, 1))
        committed_df = minute.read_complete_partition(
            lake_root / "silver" / minute.CALENDAR_TABLE_NAME,
            FUTURES_BAR_CALENDAR_SCHEMA,
            minute.CALENDAR_PARTITION_COLUMNS,
            ("1m", "XSGE", 2026, 8),
        )
        row = committed_df.iloc[0]
        self.assertEqual(row["quality_status"], "warning")
        self.assertEqual(row["evidence_source"], "contract-rule-v1")
        self.assertEqual(row["evidence_level"], "contract_rule")

    def test_clean_default_main_never_reads_fact_leaf_or_authenticates(self) -> None:
        _, lake_root = self.temporary_lake()
        completed_calendar_df = pd.DataFrame([calendar_row()]).loc[
            :,
            FUTURES_BAR_CALENDAR_SCHEMA.names,
        ]
        write_partitioned(
            completed_calendar_df,
            lake_root / "silver" / minute.CALENDAR_TABLE_NAME,
            FUTURES_BAR_CALENDAR_SCHEMA,
            minute.CALENDAR_PARTITION_COLUMNS,
        )
        original_reader = minute.read_complete_partition

        def guarded_reader(*args: object, **kwargs: object) -> pd.DataFrame:
            if args[1] is FUTURES_MINUTE_SCHEMA:
                raise AssertionError("clean 日常路径不应读取事实叶")
            return original_reader(*args, **kwargs)

        with mock.patch.object(
            minute,
            "read_complete_partition",
            side_effect=guarded_reader,
        ):
            result = CliRunner().invoke(
                minute.main,
                ["--lake-root", str(lake_root)],
            )
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("up_to_date", result.output)

    def test_descriptive_metadata_is_compatible_but_physical_type_is_not(self) -> None:
        _, lake_root = self.temporary_lake()
        fact_df = pd.DataFrame([minute_row()]).loc[
            :, FUTURES_MINUTE_SCHEMA.names
        ]
        legacy_schema = schema_with_legacy_descriptions(
            FUTURES_MINUTE_SCHEMA
        )
        current_table = pandas_to_arrow(fact_df, FUTURES_MINUTE_SCHEMA)
        legacy_table = pa.Table.from_arrays(
            current_table.columns,
            schema=legacy_schema,
        )
        legacy_path = lake_root / "legacy-minute"
        ds.write_dataset(
            legacy_table,
            legacy_path,
            format="parquet",
            partitioning=minute.HIVE_PARTITIONING,
        )
        opened_dataset = minute.open_contract_dataset(
            legacy_path,
            minute.HIVE_PARTITIONING,
            FUTURES_MINUTE_SCHEMA,
            "legacy minute",
            required=True,
        )
        self.assertIsNotNone(opened_dataset)

        incompatible_fields = [
            pa.field(
                field.name,
                pa.float32() if field.name == "close" else field.type,
                nullable=field.nullable,
                metadata=field.metadata,
            )
            for field in FUTURES_MINUTE_SCHEMA
        ]
        incompatible_schema = pa.schema(
            incompatible_fields,
            metadata=FUTURES_MINUTE_SCHEMA.metadata,
        )
        incompatible_table = current_table.cast(
            incompatible_schema,
            safe=True,
        )
        incompatible_path = lake_root / "incompatible-minute"
        ds.write_dataset(
            incompatible_table,
            incompatible_path,
            format="parquet",
            partitioning=minute.HIVE_PARTITIONING,
        )
        with self.assertRaisesRegex(TypeError, "物理字段"):
            minute.open_contract_dataset(
                incompatible_path,
                minute.HIVE_PARTITIONING,
                FUTURES_MINUTE_SCHEMA,
                "incompatible minute",
                required=True,
            )

    def test_formal_leaf_reread_failure_restores_previous_fact_leaf(self) -> None:
        _, lake_root = self.temporary_lake()
        partition_key = ("XSGE", "RB", 2026, 8)
        original_df = pd.DataFrame([minute_row()]).loc[
            :,
            FUTURES_MINUTE_SCHEMA.names,
        ]
        minute.commit_complete_partition(
            original_df,
            lake_root,
            minute.TABLE_NAME,
            FUTURES_MINUTE_SCHEMA,
            minute.PARTITION_COLUMNS,
            minute.HIVE_PARTITIONING,
            partition_key,
            minute.validate_minute_frame,
        )

        changed_row = minute_row()
        changed_row["high"] = 102.0
        changed_df = pd.DataFrame([changed_row]).loc[
            :,
            FUTURES_MINUTE_SCHEMA.names,
        ]
        original_read_schema = minute.pq.read_schema

        def failing_formal_read_schema(path, *args, **kwargs):
            parquet_path = pathlib.Path(path)
            if (
                minute.TABLE_NAME in parquet_path.parts
                and not any(part.startswith(".s-") for part in parquet_path.parts)
                and parquet_path.name != "schema.parquet"
            ):
                raise OSError("injected formal reread failure")
            return original_read_schema(path, *args, **kwargs)

        with mock.patch.object(
            minute.pq,
            "read_schema",
            side_effect=failing_formal_read_schema,
        ):
            with self.assertRaises(Exception):
                minute.commit_complete_partition(
                    changed_df,
                    lake_root,
                    minute.TABLE_NAME,
                    FUTURES_MINUTE_SCHEMA,
                    minute.PARTITION_COLUMNS,
                    minute.HIVE_PARTITIONING,
                    partition_key,
                    minute.validate_minute_frame,
                )

        restored_df = minute.read_complete_partition(
            lake_root / "silver" / minute.TABLE_NAME,
            FUTURES_MINUTE_SCHEMA,
            minute.PARTITION_COLUMNS,
            partition_key,
        )
        self.assertEqual(restored_df.iloc[0]["high"], 101.0)


if __name__ == "__main__":
    unittest.main()
