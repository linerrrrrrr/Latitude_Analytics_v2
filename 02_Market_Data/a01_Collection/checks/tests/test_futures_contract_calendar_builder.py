"""验证当前 c03 合约 Session 日历生成和分区提交边界。"""

from __future__ import annotations

import importlib.util
import pathlib
import tempfile
import unittest
from contextlib import ExitStack
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from click.testing import CliRunner


project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()
for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        project_root = candidate_root
        break
else:
    raise RuntimeError("未找到项目根目录")

module_path = (
    project_root
    / "02_Market_Data/a01_Collection"
    / "b01_Futures_Market_Data"
    / "c03_futures_contract_calendar.py"
)
module_spec = importlib.util.spec_from_file_location(
    "c03_futures_contract_calendar",
    module_path,
)
contract_calendar = importlib.util.module_from_spec(module_spec)
module_spec.loader.exec_module(contract_calendar)


class FakeJQData:
    """只实现 c03 当前来源边界，并记录真实发生的 API 调用。"""

    def __init__(
        self,
        securities_df: pd.DataFrame,
        *,
        all_securities_result: object | None = None,
        forbid_trade_day_range_query: bool = False,
    ) -> None:
        self.securities_df = securities_df.copy()
        self.all_securities_result = all_securities_result
        self.forbid_trade_day_range_query = forbid_trade_day_range_query
        self.get_all_securities_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.get_futures_info_calls: list[tuple[str, ...]] = []
        self.get_trade_days_calls: list[dict[str, object]] = []

    def get_all_securities(self, *args: object, **kwargs: object) -> object:
        self.get_all_securities_calls.append((args, kwargs))
        if self.all_securities_result is not None:
            return self.all_securities_result
        return self.securities_df.set_index("contract_code").loc[
            :, ["start_date", "end_date"]
        ]

    def get_futures_info(
        self,
        contract_codes: list[str],
        *,
        fields: list[str],
    ) -> dict[str, dict[str, object]]:
        self.get_futures_info_calls.append(tuple(contract_codes))
        records_by_code = self.securities_df.set_index("contract_code").to_dict("index")
        return {
            contract_code: {
                field: records_by_code[contract_code][field]
                for field in fields
            }
            for contract_code in contract_codes
        }

    def get_trade_days(self, *args: object, **kwargs: object) -> list[date]:
        if args:
            raise AssertionError("测试替身要求 get_trade_days 使用关键字参数。")
        self.get_trade_days_calls.append(dict(kwargs))

        if kwargs.get("count") == 2:
            first_date = pd.Timestamp(kwargs["end_date"]).date()
            previous_date = {
                date(2024, 1, 2): date(2023, 12, 29),
                date(2024, 1, 3): date(2024, 1, 2),
            }[first_date]
            return [previous_date, first_date]

        if self.forbid_trade_day_range_query:
            raise AssertionError("c03 不得调用 JQData 重新证明可信 c02 的交易日成员关系。")

        available_dates = [
            date(2023, 12, 29),
            date(2024, 1, 2),
            date(2024, 1, 3),
        ]
        start_date = pd.Timestamp(kwargs["start_date"]).date()
        end_date = pd.Timestamp(kwargs["end_date"]).date()
        return [
            trading_date
            for trading_date in available_dates
            if start_date <= trading_date <= end_date
        ]


class ContractCalendarBuilderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.updated_at = datetime(2026, 8, 12, tzinfo=timezone.utc)
        self.securities_df = pd.DataFrame(
            [
                {
                    "contract_code": "RB2405.XSGE",
                    "start_date": date(2023, 1, 1),
                    "end_date": date(2024, 5, 15),
                    "underlying_code": "RB",
                    "delivery_code": "2405",
                    "exchange_code": "XSGE",
                    "contract_multiplier": 10.0,
                    "tick_size": 1.0,
                    "trade_time": [
                        [
                            "2020-01-01",
                            "2030-01-01",
                            "21:00~02:30",
                            "09:00~10:15",
                        ]
                    ],
                },
                {
                    "contract_code": "RB2410.XSGE",
                    "start_date": date(2023, 1, 1),
                    "end_date": date(2024, 10, 15),
                    "underlying_code": "RB",
                    "delivery_code": "2410",
                    "exchange_code": "XSGE",
                    "contract_multiplier": None,
                    "tick_size": None,
                    "trade_time": [],
                },
            ]
        )

        self.valid_securities_df = self.securities_df.iloc[[0]].reset_index(drop=True)
        second_valid_contract = self.securities_df.iloc[0].to_dict()
        second_valid_contract.update(
            {
                "contract_code": "RB2410.XSGE",
                "delivery_code": "2410",
                "end_date": date(2024, 10, 15),
                "contract_multiplier": 10.0,
                "tick_size": 1.0,
                "trade_time": [["2020-01-01", "2030-01-01", "09:00~10:15"]],
            }
        )
        self.two_valid_securities_df = pd.concat(
            [self.valid_securities_df, pd.DataFrame([second_valid_contract])],
            ignore_index=True,
        )

        extra_contract = self.securities_df.iloc[0].to_dict()
        extra_contract.update(
            {
                "contract_code": "RB2501.XSGE",
                "delivery_code": "2501",
                "end_date": date(2025, 1, 15),
                "contract_multiplier": 10.0,
                "tick_size": 1.0,
                "trade_time": [["2020-01-01", "2030-01-01", "09:00~10:15"]],
            }
        )
        self.extra_securities_df = pd.concat(
            [self.valid_securities_df, pd.DataFrame([extra_contract])],
            ignore_index=True,
        )

    def variety_frame(
        self,
        trading_dates: list[date],
        *,
        active_contract_count: int = 2,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "underlying_code": "RB",
                    "exchange_code": "XSGE",
                    "trading_date": trading_date,
                    "active_contract_count": active_contract_count,
                    "source": "test",
                    "updated_at": self.updated_at,
                    "year": trading_date.year,
                    "month": trading_date.month,
                }
                for trading_date in trading_dates
            ]
        )

    @staticmethod
    def previous_dates() -> dict[date, date]:
        return {
            date(2024, 1, 2): date(2023, 12, 29),
            date(2024, 1, 3): date(2024, 1, 2),
        }

    def build_frame_with_securities(
        self,
        trading_dates: list[date],
        securities_df: pd.DataFrame,
        *,
        active_contract_count: int | None = None,
    ) -> tuple[pd.DataFrame, dict[str, object]]:
        if active_contract_count is None:
            active_contract_count = len(securities_df)
        return contract_calendar.build_contract_calendar_partition(
            self.variety_frame(
                trading_dates,
                active_contract_count=active_contract_count,
            ),
            {("XSGE", "RB"): securities_df},
            self.previous_dates(),
            self.updated_at,
        )

    def build_frame(self, trading_dates: list[date]) -> tuple[pd.DataFrame, dict]:
        return self.build_frame_with_securities(
            trading_dates,
            self.securities_df,
            active_contract_count=2,
        )

    def write_variety_calendar(
        self,
        lake_root: pathlib.Path,
        trading_dates: list[date],
        *,
        active_contract_count: int = 1,
        duplicate_rows: bool = False,
    ) -> None:
        variety_df = self.variety_frame(
            trading_dates,
            active_contract_count=active_contract_count,
        )
        if duplicate_rows:
            variety_df = pd.concat([variety_df, variety_df], ignore_index=True)
        variety_table = contract_calendar.pandas_to_arrow(
            variety_df.loc[
                :, contract_calendar.FUTURES_VARIETY_CALENDAR_SCHEMA.names
            ],
            contract_calendar.FUTURES_VARIETY_CALENDAR_SCHEMA,
        )
        upstream_path = (
            lake_root / "silver" / contract_calendar.UPSTREAM_TABLE_NAME
        )
        ds.write_dataset(
            variety_table,
            upstream_path,
            format="parquet",
            partitioning=contract_calendar.UPSTREAM_PARTITIONING,
            existing_data_behavior="delete_matching",
            basename_template="part-{i}.parquet",
        )

    @staticmethod
    def write_empty_variety_calendar(lake_root: pathlib.Path) -> None:
        upstream_schema = contract_calendar.FUTURES_VARIETY_CALENDAR_SCHEMA
        upstream_partition_columns = upstream_schema.metadata[
            b"partition_columns"
        ].decode("utf-8").split(",")
        upstream_path = (
            lake_root / "silver" / contract_calendar.UPSTREAM_TABLE_NAME
        )
        upstream_path.mkdir(parents=True)
        file_schema = pa.schema(
            [
                upstream_schema.field(name)
                for name in upstream_schema.names
                if name not in upstream_partition_columns
            ],
            metadata=upstream_schema.metadata,
        )
        pq.write_table(
            pa.Table.from_batches([], schema=file_schema),
            upstream_path / "schema.parquet",
        )

    def write_contract_calendar(
        self,
        lake_root: pathlib.Path,
        contract_calendar_df: pd.DataFrame,
    ) -> None:
        for partition_key, partition_df in contract_calendar_df.groupby(
            contract_calendar.PARTITION_COLUMNS,
            sort=True,
        ):
            contract_calendar.commit_partition(
                partition_df.reset_index(drop=True),
                lake_root,
                tuple(partition_key),
            )

    @staticmethod
    def read_contract_calendar(lake_root: pathlib.Path) -> pd.DataFrame:
        dataset = ds.dataset(
            lake_root / "silver" / contract_calendar.TABLE_NAME,
            format="parquet",
            partitioning=contract_calendar.HIVE_PARTITIONING,
        )
        return contract_calendar.arrow_to_pandas(
            dataset.to_table(
                columns=contract_calendar.FUTURES_CONTRACT_CALENDAR_SCHEMA.names
            ),
            contract_calendar.FUTURES_CONTRACT_CALENDAR_SCHEMA,
        )

    def invoke_with_jqdata(
        self,
        lake_root: pathlib.Path,
        arguments: list[str],
        jqdata: FakeJQData,
        *,
        formal: bool = False,
    ) -> tuple[object, object]:
        formal_lake_root = (
            lake_root.resolve()
            if formal
            else (lake_root / "unused-formal-lake").resolve()
        )
        settings = SimpleNamespace(
            futures_lake_root=formal_lake_root,
            jqdata_id="test-id",
            jqdata_secret="test-secret",
        )
        cli_arguments = list(arguments)
        if not formal:
            cli_arguments = ["--lake-root", str(lake_root), *cli_arguments]

        with ExitStack() as stack:
            authenticate = stack.enter_context(
                patch(
                    "config.jqdata_connection.authenticate_jqdata",
                    return_value=jqdata,
                )
            )
            stack.enter_context(patch.object(contract_calendar, "settings", settings))
            if hasattr(contract_calendar, "authenticate_jqdata"):
                authenticate = stack.enter_context(
                    patch.object(
                        contract_calendar,
                        "authenticate_jqdata",
                        return_value=jqdata,
                    )
                )
            result = CliRunner().invoke(contract_calendar.main, cli_arguments)
        return result, authenticate

    def test_build_expands_sessions_and_audits_missing_rule(self) -> None:
        frame, audit = self.build_frame([date(2024, 1, 2)])

        self.assertEqual(len(frame), 2)
        self.assertEqual(audit["candidate_contract_day_count"], 2)
        self.assertEqual(audit["no_valid_rule_contract_day_count"], 1)
        self.assertEqual(
            frame.iloc[0]["session_start_at"],
            pd.Timestamp("2023-12-29 21:00", tz="Asia/Shanghai"),
        )
        self.assertEqual(
            frame.iloc[0]["session_end_at"],
            pd.Timestamp("2023-12-30 02:30", tz="Asia/Shanghai"),
        )
        self.assertEqual(frame.iloc[0]["minute_count"], 330)
        self.assertTrue(frame.iloc[0]["is_night_session"])
        self.assertFalse(frame.iloc[1]["is_night_session"])

    def test_build_trusts_upstream_active_contract_count(self) -> None:
        frame, audit = self.build_frame_with_securities(
            [date(2024, 1, 2)],
            self.valid_securities_df,
            active_contract_count=99,
        )

        self.assertEqual(set(frame["contract_code"]), {"RB2405.XSGE"})
        self.assertEqual(audit["candidate_contract_day_count"], 1)

    def test_contract_output_validation_still_rejects_invalid_session(self) -> None:
        frame, _ = self.build_frame_with_securities(
            [date(2024, 1, 2)],
            self.valid_securities_df,
        )
        frame.loc[frame.index[0], "minute_count"] += 1

        with self.assertRaisesRegex(ValueError, "minute_count"):
            contract_calendar.validate_contract_calendar_frame(frame, "测试输出")

    def test_cli_requires_paired_dates_and_exclusive_full_mode(self) -> None:
        runner = CliRunner()
        help_result = runner.invoke(contract_calendar.main, ["--help"])
        self.assertEqual(help_result.exit_code, 0)
        self.assertIn("--full", help_result.output)

        for arguments in (
            ["--start-date", "2024-01-02"],
            ["--end-date", "2024-01-02"],
            [
                "--full",
                "--start-date",
                "2024-01-02",
                "--end-date",
                "2024-01-03",
            ],
        ):
            with self.subTest(arguments=arguments):
                result = runner.invoke(contract_calendar.main, arguments)
                self.assertEqual(result.exit_code, 2)

    def test_default_complete_grid_makes_zero_api_calls(self) -> None:
        frame, _ = self.build_frame_with_securities(
            [date(2024, 1, 2)],
            self.valid_securities_df,
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            self.write_variety_calendar(lake_root, [date(2024, 1, 2)])
            self.write_contract_calendar(lake_root, frame)
            jqdata = FakeJQData(self.valid_securities_df)
            result, authenticate = self.invoke_with_jqdata(lake_root, [], jqdata)

        self.assertEqual(result.exit_code, 0, result.output)
        authenticate.assert_not_called()
        self.assertEqual(jqdata.get_all_securities_calls, [])
        self.assertIn("up_to_date", result.output)

    def test_default_appends_only_c02_dates_after_target_watermark(self) -> None:
        existing_df, _ = self.build_frame_with_securities(
            [date(2024, 1, 2)],
            self.valid_securities_df,
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            self.write_variety_calendar(
                lake_root,
                [date(2024, 1, 2), date(2024, 1, 3)],
            )
            self.write_contract_calendar(lake_root, existing_df)
            jqdata = FakeJQData(self.valid_securities_df)
            result, authenticate = self.invoke_with_jqdata(
                lake_root,
                ["--write"],
                jqdata,
            )
            committed_df = self.read_contract_calendar(lake_root)

        self.assertEqual(result.exit_code, 0, result.output)
        authenticate.assert_called_once()
        self.assertEqual(len(jqdata.get_all_securities_calls), 1)
        self.assertEqual(
            set(committed_df["trading_date"]),
            {date(2024, 1, 2), date(2024, 1, 3)},
        )
        self.assertIn("requested_variety_date_count=1", result.output)

    def test_default_all_invalid_trade_time_persists_marker_then_noops(self) -> None:
        invalid_securities_df = self.securities_df.iloc[[1]].reset_index(drop=True)

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            self.write_variety_calendar(lake_root, [date(2024, 1, 2)])

            first_jqdata = FakeJQData(invalid_securities_df)
            first_result, first_authenticate = self.invoke_with_jqdata(
                lake_root,
                ["--write"],
                first_jqdata,
            )

            marker_path = (
                lake_root
                / "silver"
                / contract_calendar.TABLE_NAME
                / "schema.parquet"
            )
            marker_metadata = pq.read_schema(marker_path).metadata or {}

            second_jqdata = FakeJQData(invalid_securities_df)
            second_result, second_authenticate = self.invoke_with_jqdata(
                lake_root,
                [],
                second_jqdata,
            )

        self.assertEqual(first_result.exit_code, 0, first_result.output)
        first_authenticate.assert_called_once()
        self.assertEqual(len(first_jqdata.get_all_securities_calls), 1)
        self.assertIn("warning: no_valid_trade_time_samples", first_result.output)
        self.assertIn("watermark_committed", first_result.output)
        self.assertEqual(
            marker_metadata[
                contract_calendar.AUTOMATIC_TAIL_PROCESSED_THROUGH_KEY
            ],
            b"2024-01-02",
        )

        self.assertEqual(second_result.exit_code, 0, second_result.output)
        second_authenticate.assert_not_called()
        self.assertEqual(second_jqdata.get_all_securities_calls, [])
        self.assertEqual(second_jqdata.get_futures_info_calls, [])
        self.assertIn("up_to_date", second_result.output)
        self.assertIn("latest_trading_date=2024-01-02", second_result.output)

    def test_default_ignores_historical_contract_count_gap_and_full_audits_it(
        self,
    ) -> None:
        existing_df, _ = self.build_frame_with_securities(
            [date(2024, 1, 2)],
            self.valid_securities_df,
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            self.write_variety_calendar(
                lake_root,
                [date(2024, 1, 2)],
                active_contract_count=2,
            )
            self.write_contract_calendar(lake_root, existing_df)
            jqdata = FakeJQData(self.securities_df)
            with patch.object(contract_calendar, "commit_partition") as commit:
                result, authenticate = self.invoke_with_jqdata(
                    lake_root,
                    ["--write"],
                    jqdata,
                )

        self.assertEqual(result.exit_code, 0, result.output)
        authenticate.assert_not_called()
        self.assertEqual(jqdata.get_all_securities_calls, [])
        commit.assert_not_called()
        self.assertIn("up_to_date", result.output)

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            self.write_variety_calendar(
                lake_root,
                [date(2024, 1, 2)],
                active_contract_count=2,
            )
            self.write_contract_calendar(lake_root, existing_df)
            jqdata = FakeJQData(self.securities_df)
            with patch.object(contract_calendar, "commit_partition") as commit:
                result, authenticate = self.invoke_with_jqdata(
                    lake_root,
                    ["--full", "--write"],
                    jqdata,
                )

        self.assertEqual(result.exit_code, 0, result.output)
        authenticate.assert_called_once()
        self.assertEqual(len(jqdata.get_all_securities_calls), 1)
        commit.assert_not_called()
        self.assertIn("warning: no_valid_trade_time_samples", result.output)

    def test_default_does_not_remove_local_grid_outside_current_upstream(self) -> None:
        existing_df, _ = self.build_frame_with_securities(
            [date(2024, 1, 2), date(2024, 1, 3)],
            self.valid_securities_df,
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            self.write_variety_calendar(lake_root, [date(2024, 1, 3)])
            self.write_contract_calendar(lake_root, existing_df)
            jqdata = FakeJQData(self.valid_securities_df)
            result, authenticate = self.invoke_with_jqdata(
                lake_root,
                ["--write"],
                jqdata,
            )
            committed_df = self.read_contract_calendar(lake_root)

        self.assertEqual(result.exit_code, 0, result.output)
        authenticate.assert_not_called()
        self.assertEqual(
            set(committed_df["trading_date"]),
            {date(2024, 1, 2), date(2024, 1, 3)},
        )

    def test_default_does_not_revalidate_trusted_upstream_primary_key(self) -> None:
        existing_df, _ = self.build_frame_with_securities(
            [date(2024, 1, 2)],
            self.valid_securities_df,
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            self.write_variety_calendar(
                lake_root,
                [date(2024, 1, 2)],
                duplicate_rows=True,
            )
            self.write_contract_calendar(lake_root, existing_df)
            jqdata = FakeJQData(self.valid_securities_df)
            result, authenticate = self.invoke_with_jqdata(lake_root, [], jqdata)

        self.assertEqual(result.exit_code, 0, result.output)
        authenticate.assert_not_called()

    def test_explicit_partition_replace_preserves_other_dates(self) -> None:
        frame, _ = self.build_frame([date(2024, 1, 2), date(2024, 1, 3)])

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            contract_calendar.commit_partition(
                frame,
                lake_root,
                ("XSGE", 2024, 1),
            )

            replacement_df = frame.loc[
                frame["trading_date"].eq(date(2024, 1, 3))
            ].copy()
            replacement_df["tick_size"] = 2.0
            contract_calendar.commit_partition(
                replacement_df,
                lake_root,
                ("XSGE", 2024, 1),
                replace_start_date=date(2024, 1, 3),
                replace_end_date=date(2024, 1, 3),
            )

            dataset = ds.dataset(
                lake_root / "silver" / contract_calendar.TABLE_NAME,
                format="parquet",
                partitioning=contract_calendar.HIVE_PARTITIONING,
            )
            checked_df = contract_calendar.arrow_to_pandas(
                dataset.to_table(
                    columns=(
                        contract_calendar.FUTURES_CONTRACT_CALENDAR_SCHEMA.names
                    )
                ),
                contract_calendar.FUTURES_CONTRACT_CALENDAR_SCHEMA,
            )

        self.assertEqual(len(checked_df), 4)
        self.assertTrue(
            checked_df.loc[
                checked_df["trading_date"].eq(date(2024, 1, 2)),
                "tick_size",
            ].eq(1.0).all()
        )
        self.assertTrue(
            checked_df.loc[
                checked_df["trading_date"].eq(date(2024, 1, 3)),
                "tick_size",
            ].eq(2.0).all()
        )

    def test_exact_variety_date_replace_preserves_other_variety_on_same_date(
        self,
    ) -> None:
        rb_df, _ = self.build_frame_with_securities(
            [date(2024, 1, 3)],
            self.valid_securities_df,
        )
        hc_security = self.valid_securities_df.iloc[0].to_dict()
        hc_security.update(
            {
                "contract_code": "HC2405.XSGE",
                "underlying_code": "HC",
            }
        )
        hc_variety_df = self.variety_frame(
            [date(2024, 1, 3)],
            active_contract_count=1,
        )
        hc_variety_df["underlying_code"] = "HC"
        hc_df, _ = contract_calendar.build_contract_calendar_partition(
            hc_variety_df,
            {("XSGE", "HC"): pd.DataFrame([hc_security])},
            self.previous_dates(),
            self.updated_at,
        )
        existing_df = pd.concat([rb_df, hc_df], ignore_index=True)
        replacement_rb_df = rb_df.copy()
        replacement_rb_df["tick_size"] = 2.0

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            self.write_contract_calendar(lake_root, existing_df)
            contract_calendar.commit_partition(
                replacement_rb_df,
                lake_root,
                ("XSGE", 2024, 1),
                replacement_variety_date_keys={
                    ("XSGE", "RB", date(2024, 1, 3))
                },
            )
            committed_df = self.read_contract_calendar(lake_root)

        self.assertEqual(set(committed_df["underlying_code"]), {"HC", "RB"})
        self.assertTrue(
            committed_df.loc[
                committed_df["underlying_code"].eq("RB"),
                "tick_size",
            ].eq(2.0).all()
        )
        self.assertTrue(
            committed_df.loc[
                committed_df["underlying_code"].eq("HC"),
                "tick_size",
            ].eq(1.0).all()
        )

    def test_date_refresh_applies_add_change_delete_and_preserves_outside_range(
        self,
    ) -> None:
        outside_df, _ = self.build_frame_with_securities(
            [date(2024, 1, 2)],
            self.valid_securities_df,
        )
        in_range_df, _ = self.build_frame_with_securities(
            [date(2024, 1, 3)],
            self.extra_securities_df,
        )
        in_range_df.loc[
            in_range_df["contract_code"].eq("RB2405.XSGE"),
            "tick_size",
        ] = 2.0
        existing_df = pd.concat([outside_df, in_range_df], ignore_index=True)

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            self.write_variety_calendar(
                lake_root,
                [date(2024, 1, 3)],
                active_contract_count=2,
            )
            self.write_contract_calendar(lake_root, existing_df)
            jqdata = FakeJQData(self.two_valid_securities_df)
            result, authenticate = self.invoke_with_jqdata(
                lake_root,
                [
                    "--start-date",
                    "2024-01-03",
                    "--end-date",
                    "2024-01-03",
                    "--write",
                ],
                jqdata,
            )
            committed_df = self.read_contract_calendar(lake_root)

        self.assertEqual(result.exit_code, 0, result.output)
        authenticate.assert_called_once()
        outside_codes = set(
            committed_df.loc[
                committed_df["trading_date"].eq(date(2024, 1, 2)),
                "contract_code",
            ]
        )
        refreshed_df = committed_df.loc[
            committed_df["trading_date"].eq(date(2024, 1, 3))
        ]
        self.assertEqual(outside_codes, {"RB2405.XSGE"})
        self.assertEqual(
            set(refreshed_df["contract_code"]),
            {"RB2405.XSGE", "RB2410.XSGE"},
        )
        self.assertNotIn("RB2501.XSGE", set(refreshed_df["contract_code"]))
        self.assertTrue(
            refreshed_df.loc[
                refreshed_df["contract_code"].eq("RB2405.XSGE"),
                "tick_size",
            ].eq(1.0).all()
        )

    def test_date_refresh_without_write_still_pulls_and_plans(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            self.write_variety_calendar(lake_root, [date(2024, 1, 2)])
            jqdata = FakeJQData(self.valid_securities_df)
            with patch.object(contract_calendar, "commit_partition") as commit:
                result, authenticate = self.invoke_with_jqdata(
                    lake_root,
                    [
                        "--start-date",
                        "2024-01-02",
                        "--end-date",
                        "2024-01-02",
                    ],
                    jqdata,
                )
            target_path = lake_root / "silver" / contract_calendar.TABLE_NAME
            target_exists = target_path.exists()

        self.assertEqual(result.exit_code, 0, result.output)
        authenticate.assert_called_once()
        commit.assert_not_called()
        self.assertFalse(target_exists)
        self.assertIn("explicit_plan", result.output)

    def test_date_refresh_outside_c02_waterline_removes_only_requested_local_rows(
        self,
    ) -> None:
        existing_df, _ = self.build_frame_with_securities(
            [date(2024, 1, 2), date(2024, 1, 3)],
            self.valid_securities_df,
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            self.write_variety_calendar(lake_root, [date(2024, 1, 3)])
            self.write_contract_calendar(lake_root, existing_df)
            jqdata = FakeJQData(self.valid_securities_df)
            result, authenticate = self.invoke_with_jqdata(
                lake_root,
                [
                    "--start-date",
                    "2024-01-02",
                    "--end-date",
                    "2024-01-02",
                    "--write",
                ],
                jqdata,
            )
            committed_df = self.read_contract_calendar(lake_root)

        self.assertEqual(result.exit_code, 0, result.output)
        authenticate.assert_not_called()
        self.assertEqual(set(committed_df["trading_date"]), {date(2024, 1, 3)})

    def test_full_refresh_removes_local_rows_outside_current_upstream(self) -> None:
        existing_df, _ = self.build_frame_with_securities(
            [date(2024, 1, 2), date(2024, 1, 3)],
            self.valid_securities_df,
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            self.write_variety_calendar(lake_root, [date(2024, 1, 3)])
            self.write_contract_calendar(lake_root, existing_df)
            jqdata = FakeJQData(self.valid_securities_df)
            result, authenticate = self.invoke_with_jqdata(
                lake_root,
                ["--full", "--write"],
                jqdata,
            )
            committed_df = self.read_contract_calendar(lake_root)

        self.assertEqual(result.exit_code, 0, result.output)
        authenticate.assert_called_once()
        self.assertEqual(set(committed_df["trading_date"]), {date(2024, 1, 3)})

    def test_full_refresh_with_empty_trusted_c02_removes_all_local_rows(self) -> None:
        existing_df, _ = self.build_frame_with_securities(
            [date(2024, 1, 2)],
            self.valid_securities_df,
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            self.write_empty_variety_calendar(lake_root)
            self.write_contract_calendar(lake_root, existing_df)
            jqdata = FakeJQData(self.valid_securities_df)
            result, authenticate = self.invoke_with_jqdata(
                lake_root,
                ["--full", "--write"],
                jqdata,
            )
            committed_df = self.read_contract_calendar(lake_root)

        self.assertEqual(result.exit_code, 0, result.output)
        authenticate.assert_not_called()
        self.assertTrue(committed_df.empty)

    def test_refresh_modes_ignore_updated_at_and_do_not_rewrite_equal_business_rows(
        self,
    ) -> None:
        existing_df, _ = self.build_frame_with_securities(
            [date(2024, 1, 3)],
            self.valid_securities_df,
        )

        for mode_arguments in (
            [
                "--start-date",
                "2024-01-03",
                "--end-date",
                "2024-01-03",
                "--write",
            ],
            ["--full", "--write"],
        ):
            with self.subTest(mode_arguments=mode_arguments):
                with tempfile.TemporaryDirectory() as temporary_directory:
                    lake_root = pathlib.Path(temporary_directory)
                    self.write_variety_calendar(lake_root, [date(2024, 1, 3)])
                    self.write_contract_calendar(lake_root, existing_df)
                    jqdata = FakeJQData(self.valid_securities_df)
                    with patch.object(contract_calendar, "commit_partition") as commit:
                        result, authenticate = self.invoke_with_jqdata(
                            lake_root,
                            mode_arguments,
                            jqdata,
                        )

                self.assertEqual(result.exit_code, 0, result.output)
                authenticate.assert_called_once()
                commit.assert_not_called()

    def test_only_earliest_boundary_uses_jqdata_trade_days(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            self.write_variety_calendar(
                lake_root,
                [date(2024, 1, 2), date(2024, 1, 3)],
            )
            jqdata = FakeJQData(
                self.valid_securities_df,
                forbid_trade_day_range_query=True,
            )
            result, authenticate = self.invoke_with_jqdata(
                lake_root,
                ["--full"],
                jqdata,
            )

        self.assertEqual(result.exit_code, 0, result.output)
        authenticate.assert_called_once()
        self.assertEqual(len(jqdata.get_trade_days_calls), 1)
        boundary_call = jqdata.get_trade_days_calls[0]
        self.assertEqual(boundary_call.get("count"), 2)
        self.assertEqual(
            pd.Timestamp(boundary_call["end_date"]).date(),
            date(2024, 1, 2),
        )
        self.assertNotIn("start_date", boundary_call)

    def test_invalid_jqdata_response_type_is_still_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            self.write_variety_calendar(lake_root, [date(2024, 1, 2)])
            jqdata = FakeJQData(
                self.valid_securities_df,
                all_securities_result=[],
            )
            result, _ = self.invoke_with_jqdata(
                lake_root,
                ["--full"],
                jqdata,
            )

        self.assertEqual(result.exit_code, 1)
        self.assertIsInstance(result.exception, TypeError)

    def test_empty_full_replacement_keeps_schema_marker(self) -> None:
        frame, _ = self.build_frame([date(2024, 1, 2)])

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            contract_calendar.commit_partition(
                frame,
                lake_root,
                ("XSGE", 2024, 1),
            )
            contract_calendar.commit_partition(
                contract_calendar.empty_pandas(
                    contract_calendar.FUTURES_CONTRACT_CALENDAR_SCHEMA
                ),
                lake_root,
                ("XSGE", 2024, 1),
            )

            table_path = lake_root / "silver" / contract_calendar.TABLE_NAME
            dataset = ds.dataset(
                table_path,
                format="parquet",
                partitioning=contract_calendar.HIVE_PARTITIONING,
            )
            rebuilt_schema = pa.schema(
                [
                    dataset.schema.field(name)
                    for name in (
                        contract_calendar.FUTURES_CONTRACT_CALENDAR_SCHEMA.names
                    )
                ],
                metadata=dataset.schema.metadata,
            )
            marker_exists = (table_path / "schema.parquet").is_file()
            row_count = dataset.count_rows()

        self.assertTrue(marker_exists)
        self.assertEqual(row_count, 0)
        self.assertTrue(
            rebuilt_schema.equals(
                contract_calendar.FUTURES_CONTRACT_CALENDAR_SCHEMA,
                check_metadata=True,
            )
        )

    def test_formal_leaf_physical_reread_failure_restores_old_leaf(self) -> None:
        old_frame, _ = self.build_frame_with_securities(
            [date(2024, 1, 2)],
            self.valid_securities_df,
        )
        replacement_frame = old_frame.copy()
        replacement_frame["tick_size"] = 2.0

        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            self.write_contract_calendar(lake_root, old_frame)
            original_validate = (
                contract_calendar.validate_compatible_dataset_schema
            )

            def reject_formal_leaf(actual_schema, expected_schema, context):
                if context.startswith("正式分区"):
                    raise TypeError("injected formal physical failure")
                return original_validate(actual_schema, expected_schema, context)

            with (
                patch.object(
                    contract_calendar,
                    "validate_compatible_dataset_schema",
                    side_effect=reject_formal_leaf,
                ),
                self.assertRaisesRegex(RuntimeError, "分区提交失败"),
            ):
                contract_calendar.commit_partition(
                    replacement_frame,
                    lake_root,
                    ("XSGE", 2024, 1),
                )
            committed_df = self.read_contract_calendar(lake_root)

        self.assertTrue(committed_df["tick_size"].eq(1.0).all())

    def test_c03_allows_explicit_date_write_to_formal_lake(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            self.write_variety_calendar(lake_root, [date(2024, 1, 2)])
            jqdata = FakeJQData(self.valid_securities_df)
            result, authenticate = self.invoke_with_jqdata(
                lake_root,
                [
                    "--start-date",
                    "2024-01-02",
                    "--end-date",
                    "2024-01-02",
                    "--write",
                ],
                jqdata,
                formal=True,
            )
            committed_df = self.read_contract_calendar(lake_root)

        self.assertEqual(result.exit_code, 0, result.output)
        authenticate.assert_called_once()
        self.assertEqual(set(committed_df["trading_date"]), {date(2024, 1, 2)})


if __name__ == "__main__":
    unittest.main()
