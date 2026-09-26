from __future__ import annotations

import importlib.util
import pathlib
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import pyarrow as pa
from click.testing import CliRunner


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
BUSINESS_DIR = (
    PROJECT_ROOT
    / "02_Futures_Lakehouse"
    / "a01_Futures_Market_Data"
)
if str(BUSINESS_DIR) not in sys.path:
    sys.path.insert(0, str(BUSINESS_DIR))


def load_module(name: str, path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法导入 {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


C04 = load_module("c04_incremental_mode_test", BUSINESS_DIR / "b04_futures_bar_calendar.py")
C07 = load_module(
    "c07_incremental_mode_test",
    BUSINESS_DIR / "b07_suspected_session_reconciliation.py",
)
FIXTURES = load_module(
    "c07_incremental_mode_fixtures",
    pathlib.Path(__file__).with_name("test_b01_ohlc_retention.py"),
)


class EmptyCalendarDataset:
    def __init__(self, filters: list[str]):
        self.filters = filters

    def get_fragments(self, *, filter):
        return iter(())

    def to_table(self, *, columns, filter):
        self.filters.append(str(filter))
        if columns != C07.FUTURES_BAR_CALENDAR_SCHEMA.names:
            raise AssertionError("空候选扫描应保持权威列顺序。")
        return pa.Table.from_batches([], schema=C07.FUTURES_BAR_CALENDAR_SCHEMA)


class IncrementalModeTests(unittest.TestCase):
    def test_c04_c07_runtime_schema_ignores_only_descriptive_metadata(
        self,
    ) -> None:
        for module in (C04, C07):
            expected_schema = module.FUTURES_BAR_CALENDAR_SCHEMA
            descriptive_fields = []
            for field in expected_schema:
                field_metadata = dict(field.metadata or {})
                field_metadata[b"description_zh"] = b"legacy-description"
                descriptive_fields.append(field.with_metadata(field_metadata))
            descriptive_metadata = dict(expected_schema.metadata or {})
            descriptive_metadata[b"schema_version"] = b"legacy-version"
            descriptive_schema = pa.schema(
                descriptive_fields,
                metadata=descriptive_metadata,
            )

            with self.subTest(module=module.__name__, change="descriptive"):
                self.assertFalse(
                    descriptive_schema.equals(
                        expected_schema,
                        check_metadata=True,
                    )
                )
                self.assertTrue(
                    module.physically_and_identity_compatible(
                        descriptive_schema,
                        expected_schema,
                    )
                )

            identity_metadata = dict(descriptive_schema.metadata or {})
            identity_metadata[b"primary_key"] = b"wrong_primary_key"
            with self.subTest(module=module.__name__, change="identity"):
                self.assertFalse(
                    module.physically_and_identity_compatible(
                        descriptive_schema.with_metadata(identity_metadata),
                        expected_schema,
                    )
                )

            first_field = descriptive_schema.field(0)
            incompatible_fields = list(descriptive_schema)
            incompatible_fields[0] = pa.field(
                first_field.name,
                first_field.type,
                nullable=not first_field.nullable,
                metadata=first_field.metadata,
            )
            with self.subTest(module=module.__name__, change="nullable"):
                self.assertFalse(
                    module.physically_and_identity_compatible(
                        pa.schema(
                            incompatible_fields,
                            metadata=descriptive_schema.metadata,
                        ),
                        expected_schema,
                    )
                )

    def test_c04_default_tail_ignores_internal_gap_but_includes_new_tail(self) -> None:
        upstream = {
            ("XSGE", 2024, month)
            for month in (1, 2, 3, 4)
        }
        target = {
            ("1d", "XSGE", 2024, month)
            for month in (1, 2, 3)
        } | {
            # 1m 的 2 月是内部历史缺口，默认尾部不得把它加入日常计划。
            ("1m", "XSGE", 2024, month)
            for month in (1, 3)
        }

        self.assertEqual(
            C04.default_tail_partition_keys(upstream, target),
            {
                ("1d", "XSGE", 2024, 3),
                ("1d", "XSGE", 2024, 4),
                ("1m", "XSGE", 2024, 3),
                ("1m", "XSGE", 2024, 4),
            },
        )

    def test_c04_full_and_dates_are_mutually_exclusive_before_read(self) -> None:
        with mock.patch.object(
            C04,
            "open_contract_dataset",
            side_effect=AssertionError("CLI 门禁后才允许读取数据集。"),
        ) as open_mock:
            result = CliRunner().invoke(
                C04.main,
                (
                    "--full",
                    "--start-date",
                    "2024-01-01",
                    "--end-date",
                    "2024-01-31",
                ),
            )

        self.assertEqual(result.exit_code, 2)
        self.assertIn("--full 与显式日期范围互斥", result.output)
        open_mock.assert_not_called()

    def test_c07_default_filter_excludes_existing_evidence(self) -> None:
        filters: list[str] = []
        fake_dataset = EmptyCalendarDataset(filters)
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            C07,
            "open_exact_dataset",
            return_value=fake_dataset,
        ):
            result = CliRunner().invoke(
                C07.main,
                ("--lake-root", directory),
            )

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(len(filters), 1)
        self.assertIn("evidence_source", filters[0])
        self.assertIn("formal_empty_session", filters[0])

    def test_c07_unbounded_force_cannot_write_formal_lake(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            formal_root = pathlib.Path(directory).resolve()
            with (
                mock.patch.object(
                    C07,
                    "settings",
                    SimpleNamespace(futures_lake_root=formal_root),
                ),
                mock.patch.object(
                    C07,
                    "open_exact_dataset",
                    side_effect=AssertionError("CLI 门禁后才允许读取数据集。"),
                ) as open_mock,
            ):
                result = CliRunner().invoke(
                    C07.main,
                    (
                        "--lake-root",
                        str(formal_root),
                        "--force",
                        "--write",
                    ),
                )

        self.assertEqual(result.exit_code, 2)
        self.assertIn("必须同时提供日期范围或 --contract-code", result.output)
        open_mock.assert_not_called()

    def test_c07_unbounded_force_is_rejected_for_readonly_and_nonformal_write(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            nonformal_root = pathlib.Path(directory).resolve()
            formal_root = (nonformal_root / "unused-formal").resolve()
            for extra_arguments in ((), ("--write",)):
                with self.subTest(extra_arguments=extra_arguments):
                    with (
                        mock.patch.object(
                            C07,
                            "settings",
                            SimpleNamespace(futures_lake_root=formal_root),
                        ),
                        mock.patch.object(
                            C07,
                            "open_exact_dataset",
                            side_effect=AssertionError(
                                "CLI 门禁后才允许读取数据集。"
                            ),
                        ) as open_mock,
                    ):
                        result = CliRunner().invoke(
                            C07.main,
                            (
                                "--lake-root",
                                str(nonformal_root),
                                "--force",
                                *extra_arguments,
                            ),
                        )

                    self.assertEqual(result.exit_code, 2)
                    self.assertIn(
                        "必须同时提供日期范围或 --contract-code",
                        result.output,
                    )
                    open_mock.assert_not_called()

    def test_c07_bounded_force_can_reach_formal_candidate_scan(self) -> None:
        filters: list[str] = []
        fake_dataset = EmptyCalendarDataset(filters)
        with tempfile.TemporaryDirectory() as directory:
            formal_root = pathlib.Path(directory).resolve()
            with (
                mock.patch.object(
                    C07,
                    "settings",
                    SimpleNamespace(futures_lake_root=formal_root),
                ),
                mock.patch.object(
                    C07,
                    "open_exact_dataset",
                    return_value=fake_dataset,
                ),
            ):
                result = CliRunner().invoke(
                    C07.main,
                    (
                        "--lake-root",
                        str(formal_root),
                        "--contract-code",
                        FIXTURES.CONTRACT_CODE,
                        "--force",
                        "--write",
                    ),
                )

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(len(filters), 1)
        self.assertIn("contract_code", filters[0])
        self.assertNotIn("evidence_source", filters[0])

    def test_c07_formal_scoped_write_requires_force(self) -> None:
        scoped_arguments = (
            (
                "--start-date",
                "2026-08-03",
                "--end-date",
                "2026-08-03",
            ),
            ("--contract-code", FIXTURES.CONTRACT_CODE),
        )
        with tempfile.TemporaryDirectory() as directory:
            formal_root = pathlib.Path(directory).resolve()
            for scope in scoped_arguments:
                with self.subTest(scope=scope), (
                    mock.patch.object(
                        C07,
                        "settings",
                        SimpleNamespace(futures_lake_root=formal_root),
                    )
                ), mock.patch.object(
                    C07,
                    "open_exact_dataset",
                    side_effect=AssertionError(
                        "CLI 门禁后才允许读取数据集。"
                    ),
                ) as open_mock:
                    result = CliRunner().invoke(
                        C07.main,
                        (
                            "--lake-root",
                            str(formal_root),
                            *scope,
                            "--write",
                        ),
                    )

                self.assertEqual(result.exit_code, 2, result.output)
                self.assertIn("必须同时使用 --force", result.output)
                open_mock.assert_not_called()

    def test_c04_commit_validates_dirty_complete_leaf_once(self) -> None:
        calendar_df = pd.DataFrame([
            FIXTURES.calendar_row(
                bar_frequency="1m",
                quality_status="passed",
                quality_reason="隔离测试完整。",
                evidence_source="fact_futures_minute:formal_nonempty_session",
            )
        ]).loc[:, C04.FUTURES_BAR_CALENDAR_SCHEMA.names]
        partition_key = ("1m", "XSGE", 2026, 8)

        with tempfile.TemporaryDirectory() as directory:
            original_validator = C04.validate_bar_calendar_frame
            with mock.patch.object(
                C04,
                "validate_bar_calendar_frame",
                wraps=original_validator,
            ) as validator_mock:
                committed_rows = C04.commit_partition(
                    calendar_df,
                    pathlib.Path(directory),
                    partition_key,
                )

        self.assertEqual(committed_rows, 1)
        self.assertEqual(validator_mock.call_count, 1)
        self.assertEqual(
            validator_mock.call_args.args[1],
            "提交前 dirty 完整分区",
        )

    def test_c07_commit_rereads_only_primary_keys_from_formal_leaf(self) -> None:
        calendar_df = pd.DataFrame([
            FIXTURES.calendar_row(
                bar_frequency="1m",
                quality_status="passed",
                quality_reason="隔离测试完整。",
                evidence_source="fact_futures_minute:formal_nonempty_session",
            )
        ]).loc[:, C07.FUTURES_BAR_CALENDAR_SCHEMA.names]
        partition_key = ("1m", "XSGE", 2026, 8)

        with tempfile.TemporaryDirectory() as directory:
            lake_root = pathlib.Path(directory)
            target_path = (
                lake_root / "silver" / C07.CALENDAR_TABLE_NAME
            ).resolve()
            original_dataset = C07.ds.dataset
            original_validator = C07.validate_calendar_frame
            with (
                mock.patch.object(
                    C07.ds,
                    "dataset",
                    wraps=original_dataset,
                ) as dataset_mock,
                mock.patch.object(
                    C07,
                    "validate_calendar_frame",
                    wraps=original_validator,
                ) as validator_mock,
            ):
                committed_rows = C07.commit_calendar_partitions(
                    {partition_key: calendar_df},
                    lake_root,
                )

        self.assertEqual(committed_rows, 1)
        opened_paths = [
            pathlib.Path(call.args[0]).resolve()
            for call in dataset_mock.call_args_list
        ]
        self.assertNotIn(target_path, opened_paths)
        self.assertTrue(
            any(path.is_relative_to(target_path) for path in opened_paths)
        )
        validation_contexts = [
            call.args[1]
            for call in validator_mock.call_args_list
        ]
        self.assertEqual(
            validation_contexts,
            ["待提交完整日历分区"],
        )

    def test_c07_reconciled_missing_session_remains_warning(self) -> None:
        candidate = FIXTURES.calendar_row(
            bar_frequency="1m",
            quality_status="warning",
            quality_reason="0 行响应等待定向旁证。",
            evidence_source="fact_futures_minute:formal_empty_session",
        )
        candidate.update({
            "schedule_status": "suspected_closed",
            "schedule_signal_reason": "正式分钟事实为 0 行，疑似休市。",
            "actual_bar_count": 0,
            "is_data_missing": True,
            "missing_bar_count": 1,
        })

        retained = FIXTURES.calendar_row(
            bar_frequency="1m",
            quality_status="passed",
            quality_reason="其他 Session 分钟事实完整。",
            evidence_source="fact_futures_minute:formal_nonempty_session",
        )
        retained.update({
            "session_number": 2,
            "session_text": "09:01-09:03",
            "session_start_at": pd.Timestamp(
                "2026-08-03 09:01:00", tz="Asia/Shanghai"
            ),
            "session_end_at": pd.Timestamp(
                "2026-08-03 09:03:00", tz="Asia/Shanghai"
            ),
            "expected_bar_count": 2,
            "actual_bar_count": 2,
        })
        calendar_df = pd.DataFrame([candidate, retained]).loc[
            :, C07.FUTURES_BAR_CALENDAR_SCHEMA.names
        ]

        first_contract = FIXTURES.contract_row()
        second_contract = FIXTURES.contract_row()
        second_contract.update({
            "session_number": 2,
            "session_text": "09:01-09:03",
            "session_start_at": retained["session_start_at"],
            "session_end_at": retained["session_end_at"],
            "minute_count": 2,
        })
        contract_df = pd.DataFrame([first_contract, second_contract]).loc[
            :, C07.FUTURES_CONTRACT_CALENDAR_SCHEMA.names
        ]

        minute_rows = []
        for bar_at, values in [
            (
                "2026-08-03 09:02:00",
                {"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0,
                 "volume": 10.0, "money": 1_000.0, "open_interest": 19.0},
            ),
            (
                "2026-08-03 09:03:00",
                {"open": 100.0, "high": 102.0, "low": 98.0, "close": 101.0,
                 "volume": 10.0, "money": 1_000.0, "open_interest": 20.0},
            ),
        ]:
            minute_row = FIXTURES.minute_row()
            minute_row.update(values)
            minute_row.update({
                "session_number": 2,
                "bar_at": pd.Timestamp(bar_at, tz="Asia/Shanghai"),
            })
            minute_rows.append(minute_row)
        minute_df = pd.DataFrame(minute_rows).loc[
            :, C07.FUTURES_MINUTE_SCHEMA.names
        ]

        daily_row = FIXTURES.daily_row(invalid_ohlc=False)
        daily_row.update({
            "high": 102.0,
            "low": 98.0,
            "close": 101.0,
            "volume": 20.0,
            "money": 2_000.0,
            "open_interest": 20.0,
        })
        daily_df = pd.DataFrame([daily_row]).loc[
            :, C07.FUTURES_DAILY_SCHEMA.names
        ]

        with mock.patch.object(
            C07,
            "validate_calendar_frame",
            side_effect=AssertionError(
                "校对阶段不得重复完整业务校验"
            ),
        ):
            _, changed_df, unchanged_count = C07.reconcile_partition(
                calendar_df,
                {("1m", FIXTURES.CONTRACT_CODE, FIXTURES.TRADING_DATE, 1)},
                contract_df,
                daily_df,
                minute_df,
                FIXTURES.CHECKED_AT,
            )

        self.assertEqual(unchanged_count, 0)
        self.assertEqual(changed_df.iloc[0]["evidence_level"], "reconciled")
        self.assertEqual(changed_df.iloc[0]["quality_status"], "warning")
        self.assertIn("仍缺失", changed_df.iloc[0]["quality_reason"])


if __name__ == "__main__":
    unittest.main()
