"""验证完整期货日历与期货事实采集白名单相互独立。"""

from __future__ import annotations

import pathlib
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from unittest import mock

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
COLLECTION_DIR = PROJECT_ROOT / "02_Market_Data/a01_Collection"
MARKET_WORKFLOW_DIR = COLLECTION_DIR / "b01_Futures_Market_Data"
REPORT_WORKFLOW_DIR = COLLECTION_DIR / "b02_Futures_Exchange_Reports"
sys.path.insert(0, str(COLLECTION_DIR))
sys.path.insert(0, str(MARKET_WORKFLOW_DIR))
sys.path.insert(0, str(REPORT_WORKFLOW_DIR))

from config.data_contracts import (  # noqa: E402
    FUTURES_CONTRACT_CALENDAR_SCHEMA,
    FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
    FUTURES_VARIETY_CALENDAR_SCHEMA,
    empty_pandas,
    pandas_to_arrow,
)
from config.futures_lakehouse.futures_fact_collection_policy import (  # noqa: E402
    FUTURES_FACT_VARIETIES_BY_EXCHANGE,
    FUTURES_FACT_VARIETY_PAIRS,
)
import c01_exchange_report_calendar as exchange_report_calendar  # noqa: E402
import c04_futures_bar_calendar as futures_bar_calendar  # noqa: E402
import c05_futures_daily as futures_daily  # noqa: E402
import c06_futures_minute as futures_minute  # noqa: E402


TRADING_DATE = date(2024, 1, 3)
UPDATED_AT = datetime(2026, 8, 10, tzinfo=timezone.utc)
CONTRACTS = (
    ("RB2405.XSGE", "XSGE", "RB"),
    ("A2405.XDCE", "XDCE", "A"),
    ("IF2403.CCFX", "CCFX", "IF"),
)


class FuturesFactCollectionPolicyTest(unittest.TestCase):
    def test_policy_contains_exactly_60_unique_pairs_on_five_exchanges(self) -> None:
        configured_count = sum(
            len(underlying_codes)
            for underlying_codes in FUTURES_FACT_VARIETIES_BY_EXCHANGE.values()
        )

        self.assertEqual(
            set(FUTURES_FACT_VARIETIES_BY_EXCHANGE),
            {"GFEX", "XDCE", "XINE", "XSGE", "XZCE"},
        )
        self.assertEqual(configured_count, 60)
        self.assertEqual(len(FUTURES_FACT_VARIETY_PAIRS), 60)
        self.assertIn(("XSGE", "RB"), FUTURES_FACT_VARIETY_PAIRS)
        self.assertIn(("XDCE", "LH"), FUTURES_FACT_VARIETY_PAIRS)
        self.assertNotIn(("XDCE", "A"), FUTURES_FACT_VARIETY_PAIRS)
        self.assertNotIn(("CCFX", "IF"), FUTURES_FACT_VARIETY_PAIRS)


class CalendarFactSelectionTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        self.lake_root = pathlib.Path(temporary_directory.name)
        silver_root = self.lake_root / "silver"

        contract_rows = []
        for contract_code, exchange_code, underlying_code in CONTRACTS:
            contract_rows.append(
                {
                    "contract_code": contract_code,
                    "exchange_code": exchange_code,
                    "underlying_code": underlying_code,
                    "trading_date": TRADING_DATE,
                    "list_date": date(2023, 1, 1),
                    "delist_date": date(2024, 5, 15),
                    "contract_multiplier": 10.0,
                    "tick_size": 1.0,
                    "rule_effective_date": date(2020, 1, 1),
                    "rule_expiry_date": date(2030, 1, 1),
                    "session_number": 1,
                    "session_text": "09:00~09:03",
                    "session_start_at": pd.Timestamp(
                        "2024-01-03 09:00", tz="Asia/Shanghai"
                    ),
                    "session_end_at": pd.Timestamp(
                        "2024-01-03 09:03", tz="Asia/Shanghai"
                    ),
                    "is_night_session": False,
                    "spans_midnight": False,
                    "minute_count": 3,
                    "source": "test_fixture",
                    "updated_at": UPDATED_AT,
                    "year": 2024,
                    "month": 1,
                }
            )
        contract_df = pd.DataFrame(
            contract_rows,
            columns=FUTURES_CONTRACT_CALENDAR_SCHEMA.names,
        )
        self.contract_df = contract_df
        contract_partitioning = ds.partitioning(
            pa.schema(
                [
                    FUTURES_CONTRACT_CALENDAR_SCHEMA.field(name)
                    for name in ["exchange_code", "year", "month"]
                ]
            ),
            flavor="hive",
        )
        ds.write_dataset(
            pandas_to_arrow(contract_df, FUTURES_CONTRACT_CALENDAR_SCHEMA),
            silver_root / "dim_futures_contract_calendar",
            format="parquet",
            partitioning=contract_partitioning,
        )

        variety_rows = [
            {
                "underlying_code": underlying_code,
                "exchange_code": exchange_code,
                "trading_date": TRADING_DATE,
                "active_contract_count": 1,
                "source": "test_fixture",
                "updated_at": UPDATED_AT,
                "year": 2024,
                "month": 1,
            }
            for _, exchange_code, underlying_code in CONTRACTS
        ]
        variety_df = pd.DataFrame(
            variety_rows,
            columns=FUTURES_VARIETY_CALENDAR_SCHEMA.names,
        )
        self.variety_df = variety_df
        variety_partitioning = ds.partitioning(
            pa.schema(
                [
                    FUTURES_VARIETY_CALENDAR_SCHEMA.field(name)
                    for name in ["exchange_code", "year", "month"]
                ]
            ),
            flavor="hive",
        )
        ds.write_dataset(
            pandas_to_arrow(variety_df, FUTURES_VARIETY_CALENDAR_SCHEMA),
            silver_root / "dim_futures_variety_calendar",
            format="parquet",
            partitioning=variety_partitioning,
        )

    def test_bar_calendar_keeps_all_grids_before_fact_policy_evaluation(self) -> None:
        fresh_partitions = futures_bar_calendar.build_fresh_partitions(
            self.contract_df,
            UPDATED_AT,
        )
        calendar_df = pd.concat(fresh_partitions.values(), ignore_index=True)

        expected_grid_keys = {
            (bar_frequency, contract_code)
            for bar_frequency in ("1d", "1m")
            for contract_code, _, _ in CONTRACTS
        }
        self.assertEqual(
            set(
                calendar_df[["bar_frequency", "contract_code"]].itertuples(
                    index=False,
                    name=None,
                )
            ),
            expected_grid_keys,
        )

        daily_df = calendar_df.loc[calendar_df["bar_frequency"].eq("1d")]
        minute_df = calendar_df.loc[calendar_df["bar_frequency"].eq("1m")]
        self.assertEqual(len(daily_df), 3)
        self.assertTrue(daily_df["is_fetch_required"].all())
        self.assertEqual(len(minute_df), 3)
        self.assertFalse(minute_df["is_fetch_required"].any())

    def test_daily_and_minute_fact_policies_select_only_whitelisted_pairs(self) -> None:
        fresh_partitions = futures_bar_calendar.build_fresh_partitions(
            self.contract_df,
            UPDATED_AT,
        )
        calendar_df = pd.concat(fresh_partitions.values(), ignore_index=True)
        daily_policy_changes_df, daily_pending_df, _ = (
            futures_daily.plan_daily_policy(
                calendar_df.loc[calendar_df["bar_frequency"].eq("1d")],
                TRADING_DATE,
                TRADING_DATE,
            )
        )
        minute_policy_frames = []
        for exchange_code, minute_group_df in calendar_df.loc[
            calendar_df["bar_frequency"].eq("1m")
        ].groupby("exchange_code", sort=True):
            planning_table = pandas_to_arrow(
                minute_group_df.loc[
                    :, futures_minute.MINUTE_PLANNING_COLUMNS
                ],
                futures_minute.MINUTE_PLANNING_SCHEMA,
            )
            desired_required, _, _, _ = futures_minute.minute_policy_plan(
                planning_table,
                exchange_code,
            )
            planned_group_df = minute_group_df.copy()
            planned_group_df["desired_is_fetch_required"] = (
                desired_required.to_numpy(zero_copy_only=False)
            )
            minute_policy_frames.append(planned_group_df)

        self.assertEqual(
            set(
                daily_pending_df.loc[
                    daily_pending_df["desired_is_fetch_required"],
                    "contract_code",
                ]
            ),
            {"RB2405.XSGE"},
        )
        self.assertEqual(
            set(
                daily_policy_changes_df.loc[
                    ~daily_policy_changes_df["desired_is_fetch_required"],
                    "contract_code",
                ]
            ),
            {"A2405.XDCE", "IF2403.CCFX"},
        )
        minute_policy_df = pd.concat(
            minute_policy_frames,
            ignore_index=True,
        )
        self.assertEqual(
            set(
                minute_policy_df.loc[
                    minute_policy_df["desired_is_fetch_required"],
                    "contract_code",
                ]
            ),
            {"RB2405.XSGE"},
        )
        self.assertEqual(
            set(
                minute_policy_df.loc[
                    ~minute_policy_df["desired_is_fetch_required"],
                    "contract_code",
                ]
            ),
            {"A2405.XDCE", "IF2403.CCFX"},
        )

    def test_minute_collection_preserves_invalid_vendor_ohlc_without_rewriting(self) -> None:
        fresh_partitions = futures_bar_calendar.build_fresh_partitions(
            self.contract_df,
            UPDATED_AT,
        )
        sessions_df = fresh_partitions["1m"].loc[
            fresh_partitions["1m"]["contract_code"].eq("RB2405.XSGE")
        ]

        class FakeJQData:
            @staticmethod
            def get_price(*args, **kwargs):
                return pd.DataFrame(
                    {
                        "open": [100.0, 101.0],
                        "high": [102.0, 101.0],
                        "low": [102.0, 101.0],
                        "close": [102.0, 101.0],
                        "volume": [1.0, 0.0],
                        "money": [102.0, 0.0],
                        "open_interest": [10.0, 10.0],
                    },
                    index=pd.DatetimeIndex(
                        ["2024-01-03 09:01", "2024-01-03 09:02"],
                        name="time",
                    ),
                )

        collected_df, returned_rows, invalid_counts = (
            futures_minute.collect_partition(
                FakeJQData(),
                sessions_df,
                UPDATED_AT,
            )
        )

        self.assertEqual(returned_rows, 2)
        self.assertEqual(len(collected_df), 2)
        self.assertEqual(sum(invalid_counts.values()), 1)
        invalid_row = collected_df.sort_values("bar_at").iloc[0]
        self.assertEqual(
            tuple(
                float(invalid_row[column])
                for column in ["open", "high", "low", "close"]
            ),
            (100.0, 102.0, 102.0, 102.0),
        )

    def test_exchange_report_calendar_keeps_all_three_dataset_grids(self) -> None:
        calendar_df = exchange_report_calendar.build_expected_calendar(
            self.variety_df,
            empty_pandas(FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA),
            UPDATED_AT,
        )

        expected_grid_keys = {
            (dataset_name, exchange_code, underlying_code)
            for dataset_name in exchange_report_calendar.DATASET_NAMES
            for _, exchange_code, underlying_code in CONTRACTS
        }
        self.assertEqual(
            set(
                calendar_df[
                    ["dataset_name", "exchange_code", "underlying_code"]
                ].itertuples(index=False, name=None)
            ),
            expected_grid_keys,
        )
        self.assertEqual(len(calendar_df), 9)

        required_df = calendar_df.loc[calendar_df["is_fetch_required"]]
        self.assertEqual(
            set(required_df["underlying_code"]),
            {"RB"},
        )
        self.assertTrue(required_df["fetch_result_status"].eq("pending").all())

        not_required_df = calendar_df.loc[~calendar_df["is_fetch_required"]]
        self.assertEqual(
            set(
                not_required_df[
                    ["exchange_code", "underlying_code"]
                ].itertuples(index=False, name=None)
            ),
            {("XDCE", "A"), ("CCFX", "IF")},
        )
        self.assertTrue(
            not_required_df["fetch_result_status"].eq("not_required").all()
        )

    def test_exchange_report_policy_changes_preserve_completed_evidence(self) -> None:
        initial_df = exchange_report_calendar.build_expected_calendar(
            self.variety_df,
            empty_pandas(FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA),
            UPDATED_AT,
        )
        completed_mask = (
            initial_df["dataset_name"].eq("warehouse_receipt")
            & initial_df["exchange_code"].eq("XSGE")
            & initial_df["underlying_code"].eq("RB")
        )
        initial_df.loc[completed_mask, "is_fetch_completed"] = True
        initial_df.loc[completed_mask, "fetch_result_status"] = "empty_confirmed"
        initial_df.loc[completed_mask, "is_data_missing"] = True
        initial_df.loc[completed_mask, "expected_record_count"] = 1
        initial_df.loc[completed_mask, "actual_record_count"] = 0
        initial_df.loc[completed_mask, "quality_status"] = "warning"
        initial_df.loc[completed_mask, "quality_reason"] = "正式复读确认该报告为空。"
        initial_df.loc[completed_mask, "fetch_run_id"] = "completed-run"
        initial_df.loc[completed_mask, "fetch_completed_at"] = UPDATED_AT
        initial_df.loc[completed_mask, "quality_checked_at"] = UPDATED_AT
        initial_df = exchange_report_calendar.validate_report_calendar_table(
            pandas_to_arrow(
                initial_df.loc[:, FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names],
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
            ),
            "测试初始",
        )

        contracted_policy = set(FUTURES_FACT_VARIETY_PAIRS) - {("XSGE", "RB")}
        with mock.patch.object(
            exchange_report_calendar,
            "FUTURES_FACT_VARIETY_PAIRS",
            contracted_policy,
        ):
            contracted_df = exchange_report_calendar.build_expected_calendar(
                self.variety_df,
                initial_df,
                UPDATED_AT + timedelta(days=1),
            )

        contracted_completed = contracted_df.loc[completed_mask].iloc[0]
        self.assertFalse(contracted_completed["is_fetch_required"])
        self.assertTrue(contracted_completed["is_fetch_completed"])
        self.assertEqual(
            contracted_completed["fetch_result_status"],
            "empty_confirmed",
        )
        self.assertFalse(contracted_completed["is_data_missing"])
        self.assertEqual(contracted_completed["fetch_run_id"], "completed-run")
        self.assertEqual(contracted_completed["expected_record_count"], 1)
        self.assertEqual(contracted_completed["actual_record_count"], 0)
        self.assertEqual(contracted_completed["quality_status"], "warning")
        self.assertEqual(
            contracted_completed["updated_at"],
            UPDATED_AT + timedelta(days=1),
        )

        contracted_pending = contracted_df.loc[
            contracted_df["dataset_name"].eq("position_rank")
            & contracted_df["exchange_code"].eq("XSGE")
            & contracted_df["underlying_code"].eq("RB")
        ].iloc[0]
        self.assertFalse(contracted_pending["is_fetch_required"])
        self.assertFalse(contracted_pending["is_fetch_completed"])
        self.assertEqual(contracted_pending["fetch_result_status"], "not_required")

        reexpanded_df = exchange_report_calendar.build_expected_calendar(
            self.variety_df,
            contracted_df,
            UPDATED_AT + timedelta(days=2),
        )
        reexpanded_completed = reexpanded_df.loc[completed_mask].iloc[0]
        self.assertTrue(reexpanded_completed["is_fetch_required"])
        self.assertTrue(reexpanded_completed["is_fetch_completed"])
        self.assertTrue(reexpanded_completed["is_data_missing"])
        self.assertEqual(reexpanded_completed["fetch_run_id"], "completed-run")
        self.assertEqual(
            reexpanded_completed["updated_at"],
            UPDATED_AT + timedelta(days=2),
        )

        reexpanded_pending = reexpanded_df.loc[
            reexpanded_df["dataset_name"].eq("position_rank")
            & reexpanded_df["exchange_code"].eq("XSGE")
            & reexpanded_df["underlying_code"].eq("RB")
        ].iloc[0]
        self.assertTrue(reexpanded_pending["is_fetch_required"])
        self.assertFalse(reexpanded_pending["is_fetch_completed"])
        self.assertEqual(reexpanded_pending["fetch_result_status"], "pending")

    def test_exchange_report_schema_accepts_descriptive_metadata_drift(self) -> None:
        completed_metadata = (
            FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.field("is_fetch_completed")
            .metadata[b"transformation_zh"]
            .decode("utf-8")
        )
        table_quality = FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.metadata[
            b"quality_rules_zh"
        ].decode("utf-8")
        self.assertIn("不因当前 is_fetch_required 变为 false 而清除", completed_metadata)
        self.assertIn(
            "is_fetch_required=false、is_fetch_completed=true",
            table_quality,
        )

        historical_metadata = dict(
            FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.metadata or {}
        )
        historical_metadata[b"update_mode_zh"] = "历史更新说明。".encode("utf-8")
        historical_fields = list(FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA)
        first_field_metadata = dict(historical_fields[0].metadata or {})
        first_field_metadata[b"description_zh"] = "历史字段说明。".encode("utf-8")
        historical_fields[0] = historical_fields[0].with_metadata(
            first_field_metadata
        )
        historical_schema = pa.schema(
            historical_fields,
            metadata=historical_metadata,
        )

        self.assertTrue(
            exchange_report_calendar.physically_and_identity_compatible(
                historical_schema,
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
            )
        )


if __name__ == "__main__":
    unittest.main()
