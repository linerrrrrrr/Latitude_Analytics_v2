"""隔离验证 b01/b03 对有限 OHLC 异常的保留、留痕和非有限数门禁。"""

from __future__ import annotations

import pathlib
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from unittest import mock

import pandas as pd
import numpy as np
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
WORKFLOW_DIR = (
    PROJECT_ROOT
    / "R02_Market_Data/a01_Collection"
    / "b01_Futures_Market_Data"
)
EXTERNAL_WORKFLOW_DIR = (
    PROJECT_ROOT
    / "R02_Market_Data/a01_Collection"
    / "b03_External_Market_Data"
)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(WORKFLOW_DIR) not in sys.path:
    sys.path.insert(0, str(WORKFLOW_DIR))
if str(EXTERNAL_WORKFLOW_DIR) not in sys.path:
    sys.path.insert(0, str(EXTERNAL_WORKFLOW_DIR))

import c05_futures_daily as daily  # noqa: E402
import c06_futures_minute as minute  # noqa: E402
import c07_suspected_session_reconciliation as reconciliation  # noqa: E402
import c08_full_minute_quality as full_quality  # noqa: E402
import c03_overseas_futures as overseas  # noqa: E402
from config.data_contracts import (  # noqa: E402
    FUTURES_BAR_CALENDAR_SCHEMA,
    FUTURES_CONTRACT_CALENDAR_SCHEMA,
    FUTURES_DAILY_SCHEMA,
    FUTURES_MINUTE_SCHEMA,
    OVERSEAS_FUTURES_DAILY_SCHEMA,
    arrow_to_pandas,
    pandas_to_arrow,
)


TRADING_DATE = date(2026, 8, 3)
SESSION_START = pd.Timestamp("2026-08-03 09:00:00", tz="Asia/Shanghai")
SESSION_END = pd.Timestamp("2026-08-03 09:01:00", tz="Asia/Shanghai")
CHECKED_AT = datetime.now(timezone.utc) - timedelta(seconds=1)
MINUTE_OHLC_WARNING_REASON = (
    "JQData 分钟请求已完成；其中 1 条供应商原始 OHLC 跨列关系异常，"
    "原值已保留。"
)
CONTRACT_CODE = "RB2609.XSGE"


def daily_row(*, invalid_ohlc: bool = True) -> dict[str, object]:
    """构造一条派生字段自洽的日线事实；异常仅来自 high < open。"""
    return {
        "contract_code": CONTRACT_CODE,
        "exchange_code": "XSGE",
        "underlying_code": "RB",
        "trading_date": TRADING_DATE,
        "previous_close": None,
        "previous_settlement": None,
        "open": 100.0,
        "high": 99.0 if invalid_ohlc else 101.0,
        "low": 98.0,
        "close": 99.0,
        "settlement": 99.5,
        "close_change_from_previous_settlement": None,
        "settlement_change_from_previous_settlement": None,
        "volume": 10.0,
        "money": 1_000.0,
        "open_interest": 20.0,
        "open_interest_change": None,
        "has_market_data": True,
        "source": daily.SOURCE_NAME,
        "updated_at": CHECKED_AT,
        "year": 2026,
        "month": 8,
    }


def minute_row() -> dict[str, object]:
    """构造一条有限但 high < open 的供应商原始分钟事实。"""
    return {
        "contract_code": CONTRACT_CODE,
        "exchange_code": "XSGE",
        "underlying_code": "RB",
        "trading_date": TRADING_DATE,
        "session_number": 1,
        "bar_at": SESSION_END,
        "open": 100.0,
        "high": 99.0,
        "low": 98.0,
        "close": 99.0,
        "volume": 10.0,
        "money": 1_000.0,
        "open_interest": 20.0,
        "source": minute.SOURCE_NAME,
        "updated_at": CHECKED_AT,
        "year": 2026,
        "month": 8,
    }


def calendar_row(
    *,
    bar_frequency: str,
    quality_status: str,
    quality_reason: str,
    evidence_source: str,
) -> dict[str, object]:
    """构造通过完整日历结构校验的一条 1d 或 1m 格点。"""
    is_daily = bar_frequency == "1d"
    row = {
        "bar_frequency": bar_frequency,
        "contract_code": CONTRACT_CODE,
        "exchange_code": "XSGE",
        "underlying_code": "RB",
        "trading_date": TRADING_DATE,
        "session_number": 0 if is_daily else 1,
        "session_text": None if is_daily else "09:00-09:01",
        "session_start_at": None if is_daily else SESSION_START,
        "session_end_at": None if is_daily else SESSION_END,
        "is_night_session": None if is_daily else False,
        "schedule_status": "scheduled",
        "schedule_signal_reason": "合约交易规则计划开市。",
        "evidence_level": "inferred",
        "evidence_source": evidence_source,
        "is_fetch_required": True,
        "expected_bar_count": 1,
        "selection_reason": "命中事实采集白名单。",
        "is_fetch_completed": True,
        "actual_bar_count": 1,
        "is_data_missing": False,
        "missing_bar_count": 0,
        "fetch_run_id": "isolated-test",
        "fetch_completed_at": CHECKED_AT,
        "missing_checked_at": CHECKED_AT,
        "quality_status": quality_status,
        "quality_reason": quality_reason,
        "quality_checked_at": CHECKED_AT,
        "updated_at": CHECKED_AT,
        "year": 2026,
        "month": 8,
    }
    for column in minute.EVIDENCE_COLUMNS:
        row[column] = None
    return row


def contract_row() -> dict[str, object]:
    """构造 c08 直接依赖的一条合约 Session 与 tick_size 记录。"""
    return {
        "contract_code": CONTRACT_CODE,
        "exchange_code": "XSGE",
        "underlying_code": "RB",
        "trading_date": TRADING_DATE,
        "list_date": date(2026, 1, 1),
        "delist_date": date(2026, 9, 30),
        "contract_multiplier": 10.0,
        "tick_size": 1.0,
        "rule_effective_date": date(2026, 1, 1),
        "rule_expiry_date": date(2026, 9, 30),
        "session_number": 1,
        "session_text": "09:00-09:01",
        "session_start_at": SESSION_START,
        "session_end_at": SESSION_END,
        "is_night_session": False,
        "spans_midnight": False,
        "minute_count": 1,
        "source": "isolated_test",
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
    """在临时湖写一份带权威 Schema metadata 的完整叶分区。"""
    partitioning = ds.partitioning(
        pa.schema([schema.field(name) for name in partition_columns]),
        flavor="hive",
    )
    ds.write_dataset(
        pandas_to_arrow(frame.loc[:, schema.names], schema),
        table_path,
        format="parquet",
        partitioning=partitioning,
        existing_data_behavior="delete_matching",
        basename_template="part-{i}.parquet",
    )


class FakeJQData:
    """只返回一条有限 OHLC 跨列异常分钟，不访问外部服务。"""

    def get_price(self, *args: object, **kwargs: object) -> pd.DataFrame:
        del args, kwargs
        row = minute_row()
        return pd.DataFrame(
            [{"time": row["bar_at"], "code": row["contract_code"], **{
                name: row[name] for name in minute.PRICE_FIELDS
            }}]
        )


class B01OHLCRetentionTest(unittest.TestCase):
    def test_full_quality_digest_ignores_only_null_slot_physical_bits(self) -> None:
        schema = pa.schema(
            [pa.field("row_id", pa.int32()), pa.field("flag", pa.bool_())],
            metadata={b"table": b"digest_test"},
        )
        hidden_true = pd.arrays.BooleanArray(
            np.array([True, False]),
            np.array([True, False]),
        )
        hidden_false = pd.arrays.BooleanArray(
            np.array([False, False]),
            np.array([True, False]),
        )
        left = pd.DataFrame({"row_id": [1, 2], "flag": hidden_true})
        right = pd.DataFrame({"row_id": [1, 2], "flag": hidden_false})
        left_table = pa.Table.from_pandas(
            left,
            schema=schema,
            preserve_index=False,
        )
        right_table = pa.Table.from_pandas(
            right,
            schema=schema,
            preserve_index=False,
        )

        self.assertEqual(
            full_quality.table_digest(left_table, schema, ["row_id"]),
            full_quality.table_digest(right_table, schema, ["row_id"]),
        )

        different_null_position = pd.DataFrame({
            "row_id": [1, 2],
            "flag": pd.array([False, None], dtype="boolean"),
        })
        self.assertNotEqual(
            full_quality.table_digest(left_table, schema, ["row_id"]),
            full_quality.table_digest(
                pa.Table.from_pandas(
                    different_null_position,
                    schema=schema,
                    preserve_index=False,
                ),
                schema,
                ["row_id"],
            ),
        )

    def test_full_quality_digest_canonicalizes_nan_payload_and_signed_zero(
        self,
    ) -> None:
        schema = pa.schema(
            [pa.field("row_id", pa.int32()), pa.field("value", pa.float64())],
            metadata={b"table": b"float_digest_test"},
        )
        nan_left = np.array([0x7FF8000000000001], dtype=np.uint64).view(
            np.float64
        )[0]
        nan_right = np.array([0x7FF80000000000FF], dtype=np.uint64).view(
            np.float64
        )[0]
        left = pd.DataFrame({"row_id": [1, 2], "value": [nan_left, -0.0]})
        right = pd.DataFrame({"row_id": [1, 2], "value": [nan_right, 0.0]})
        left_table = pa.Table.from_pandas(
            left,
            schema=schema,
            preserve_index=False,
        )
        right_table = pa.Table.from_pandas(
            right,
            schema=schema,
            preserve_index=False,
        )
        self.assertEqual(
            full_quality.table_digest(left_table, schema, ["row_id"]),
            full_quality.table_digest(right_table, schema, ["row_id"]),
        )

        changed = right.copy()
        changed.loc[1, "value"] = 1.0
        self.assertNotEqual(
            full_quality.table_digest(left_table, schema, ["row_id"]),
            full_quality.table_digest(
                pa.Table.from_pandas(
                    changed,
                    schema=schema,
                    preserve_index=False,
                ),
                schema,
                ["row_id"],
            ),
        )

    def test_full_quality_accepts_legacy_descriptive_metadata(self) -> None:
        schema_metadata = dict(FUTURES_BAR_CALENDAR_SCHEMA.metadata or {})
        schema_metadata[b"description_zh"] = b"legacy description"
        legacy_fields = []
        for field in FUTURES_BAR_CALENDAR_SCHEMA:
            field_metadata = dict(field.metadata or {})
            field_metadata[b"description_zh"] = b"legacy field description"
            legacy_fields.append(
                pa.field(
                    field.name,
                    field.type,
                    nullable=field.nullable,
                    metadata=field_metadata,
                )
            )
        legacy_schema = pa.schema(
            legacy_fields,
            metadata=schema_metadata,
        )
        self.assertTrue(
            full_quality.physically_and_identity_compatible(
                legacy_schema,
                FUTURES_BAR_CALENDAR_SCHEMA,
            )
        )

    def test_daily_invalid_ohlc_is_retained_and_new_completion_warns(
        self,
    ) -> None:
        fact_df = pd.DataFrame([daily_row()]).loc[:, FUTURES_DAILY_SCHEMA.names]

        checked_df = daily.validate_daily_frame(fact_df, "隔离日线")
        checked_row = pandas_to_arrow(
            checked_df,
            FUTURES_DAILY_SCHEMA,
        ).to_pylist()[0]
        self.assertEqual(len(checked_df), 1)
        self.assertEqual(checked_row["open"], 100.0)
        self.assertEqual(checked_row["high"], 99.0)

        calendar_df = pd.DataFrame([
            calendar_row(
                bar_frequency="1d",
                quality_status="pending",
                quality_reason="等待事实提交。",
                evidence_source="dim_futures_contract_calendar",
            )
        ]).loc[:, FUTURES_BAR_CALENDAR_SCHEMA.names]
        completed_df = daily.apply_completion_to_calendar_leaf(
            calendar_df,
            checked_df,
            "test-run",
            CHECKED_AT,
        )
        completed_row = completed_df.iloc[0]
        self.assertTrue(completed_row["is_fetch_completed"])
        self.assertEqual(completed_row["actual_bar_count"], 1)
        self.assertEqual(completed_row["missing_bar_count"], 0)
        self.assertEqual(completed_row["quality_status"], "warning")
        self.assertIn("原值已保留", completed_row["quality_reason"])

    def test_daily_nonfinite_and_negative_values_are_still_rejected(self) -> None:
        for nonfinite_value in [float("nan"), float("inf")]:
            nonfinite = daily_row()
            nonfinite["open"] = nonfinite_value
            with self.subTest(nonfinite_value=nonfinite_value):
                with self.assertRaisesRegex(ValueError, "非有限"):
                    daily.validate_daily_frame(
                        pd.DataFrame([nonfinite]).loc[
                            :, FUTURES_DAILY_SCHEMA.names
                        ],
                        "隔离日线",
                    )

        negative = daily_row()
        negative["volume"] = -1.0
        with self.assertRaisesRegex(ValueError, "不得为负"):
            daily.validate_daily_frame(
                pd.DataFrame([negative]).loc[:, FUTURES_DAILY_SCHEMA.names],
                "隔离日线",
            )

        raw_price_df = pd.DataFrame([
            {
                "time": TRADING_DATE,
                "code": CONTRACT_CODE,
                "open": 100.0,
                "high": 99.0,
                "low": 98.0,
                "close": 99.0,
                "volume": 10.0,
                "money": 1_000.0,
                "pre_close": None,
            }
        ])
        normalized_price_df = daily.normalize_price_response(
            raw_price_df,
            [CONTRACT_CODE],
        )
        self.assertIs(normalized_price_df.iloc[0]["pre_close"], pd.NA)
        self.assertEqual(normalized_price_df.iloc[0]["open"], 100.0)

        for nonfinite_value in [float("nan"), float("inf")]:
            nonfinite_raw_df = raw_price_df.copy()
            nonfinite_raw_df.loc[0, "open"] = nonfinite_value
            with self.subTest(raw_nonfinite_value=nonfinite_value):
                with self.assertRaisesRegex(ValueError, "非有限"):
                    daily.normalize_price_response(
                        nonfinite_raw_df,
                        [CONTRACT_CODE],
                    )

        raw_extra_df = pd.DataFrame(
            {CONTRACT_CODE: [None]},
            index=pd.DatetimeIndex([TRADING_DATE]),
            dtype="object",
        )
        normalized_extra_df = daily.normalize_extra_response(
            raw_extra_df,
            [CONTRACT_CODE],
            "settlement",
        )
        self.assertIs(normalized_extra_df.iloc[0]["settlement"], pd.NA)

        # JQData get_extras 的浮点 NaN 是 nullable 扩展值的缺失占位。
        nan_extra_df = raw_extra_df.copy()
        nan_extra_df.iloc[0, 0] = float("nan")
        normalized_nan_df = daily.normalize_extra_response(
            nan_extra_df,
            [CONTRACT_CODE],
            "settlement",
        )
        self.assertIs(normalized_nan_df.iloc[0]["settlement"], pd.NA)

        infinite_extra_df = raw_extra_df.copy()
        infinite_extra_df.iloc[0, 0] = float("inf")
        with self.assertRaisesRegex(ValueError, "非有限"):
            daily.normalize_extra_response(
                infinite_extra_df,
                [CONTRACT_CODE],
                "settlement",
            )

    def test_minute_collection_preserves_invalid_row_and_counts_it(self) -> None:
        sessions_df = pd.DataFrame([
            calendar_row(
                bar_frequency="1m",
                quality_status="pending",
                quality_reason="等待事实提交。",
                evidence_source="dim_futures_contract_calendar",
            )
        ]).loc[:, FUTURES_BAR_CALENDAR_SCHEMA.names]

        collected_df, returned_rows, invalid_counts = minute.collect_partition(
            FakeJQData(),
            sessions_df,
            CHECKED_AT,
        )
        collected_row = pandas_to_arrow(
            collected_df,
            FUTURES_MINUTE_SCHEMA,
        ).to_pylist()[0]

        self.assertEqual(returned_rows, 1)
        self.assertEqual(len(collected_df), 1)
        self.assertEqual(collected_row["open"], 100.0)
        self.assertEqual(collected_row["high"], 99.0)
        self.assertEqual(
            invalid_counts[(CONTRACT_CODE, TRADING_DATE, 1)],
            1,
        )

    def test_minute_invalid_fact_completion_receipt_is_trusted(self) -> None:
        fact_df = pd.DataFrame([minute_row()]).loc[:, FUTURES_MINUTE_SCHEMA.names]
        checked_df = minute.validate_minute_frame(fact_df, "隔离分钟")
        self.assertEqual(len(checked_df), 1)

        warning_calendar_df = pd.DataFrame([
            calendar_row(
                bar_frequency="1m",
                quality_status="warning",
                quality_reason=MINUTE_OHLC_WARNING_REASON,
                evidence_source="fact_futures_minute:invalid_ohlc",
            )
        ]).loc[:, FUTURES_BAR_CALENDAR_SCHEMA.names]
        self.assertTrue(warning_calendar_df.iloc[0]["is_fetch_completed"])

        passed_calendar_df = warning_calendar_df.copy()
        passed_calendar_df.loc[0, "quality_status"] = "passed"
        passed_calendar_df.loc[0, "quality_reason"] = "错误地通过。"
        self.assertTrue(passed_calendar_df.iloc[0]["is_fetch_completed"])

        for nonfinite_value in [float("nan"), float("inf")]:
            nonfinite = minute_row()
            nonfinite["close"] = nonfinite_value
            with self.subTest(nonfinite_value=nonfinite_value):
                with self.assertRaisesRegex(ValueError, "非有限"):
                    minute.validate_minute_frame(
                        pd.DataFrame([nonfinite]).loc[
                            :, FUTURES_MINUTE_SCHEMA.names
                        ],
                        "隔离分钟",
                    )

        negative = minute_row()
        negative["open_interest"] = -1.0
        with self.assertRaisesRegex(ValueError, "不得为负"):
            minute.validate_minute_frame(
                pd.DataFrame([negative]).loc[:, FUTURES_MINUTE_SCHEMA.names],
                "隔离分钟",
            )

        raw_minute_df = pd.DataFrame([
            {
                "time": SESSION_END,
                "code": CONTRACT_CODE,
                "open": 100.0,
                "high": 99.0,
                "low": 98.0,
                "close": 99.0,
                "volume": 10.0,
                "money": 1_000.0,
                "open_interest": 20.0,
            }
        ])
        normalized_minute_df = minute.normalize_minute_response(
            raw_minute_df,
            CONTRACT_CODE,
        )
        self.assertEqual(normalized_minute_df.iloc[0]["open"], 100.0)

        for nonfinite_value in [float("nan"), float("inf")]:
            nonfinite_raw_df = raw_minute_df.copy()
            nonfinite_raw_df.loc[0, "close"] = nonfinite_value
            with self.subTest(raw_nonfinite_value=nonfinite_value):
                with self.assertRaisesRegex(ValueError, "非有限"):
                    minute.normalize_minute_response(
                        nonfinite_raw_df,
                        CONTRACT_CODE,
                    )

    def test_overseas_invalid_ohlc_is_retained_and_api_nan_becomes_null(
        self,
    ) -> None:
        raw_df = pd.DataFrame([
            {
                "id": "known-sm-anomaly",
                "code": "SM",
                "name": "CBOT-黄豆粉",
                "day": date(2026, 7, 17),
                "open": 326.2,
                "close": 322.5,
                "low": 322.0,
                "high": 325.0,
                "volume": 12.0,
                "change_pct": -1.0,
                "amplitude": 2.0,
                "pre_close": None,
            }
        ])
        snapshot_date = date(2026, 7, 17)

        normalized = overseas.normalize_overseas_futures_response(
            raw_df,
            snapshot_date,
            CHECKED_AT,
        )
        normalized = normalized.loc[:, OVERSEAS_FUTURES_DAILY_SCHEMA.names]
        row = normalized.iloc[0]
        self.assertEqual(
            tuple(float(row[column]) for column in ["open", "high", "low", "close"]),
            (326.2, 325.0, 322.0, 322.5),
        )

        warning_map = overseas.ohlc_relation_warning_map(normalized)
        self.assertIn(snapshot_date, warning_map)
        self.assertIn("open=326.2 高于 high=325.0", warning_map[snapshot_date])

        calendar_row = {
            "is_fetch_required": True,
            "is_fetch_completed": True,
            "actual_record_count": 1,
            "fetch_run_id": "isolated-test",
            "fetch_completed_at": CHECKED_AT,
            "quality_checked_at": CHECKED_AT,
            "fetch_result_status": "success",
            "is_data_missing": False,
            "quality_status": "warning",
            "quality_reason": (
                "JQData FUT_GLOBAL_DAILY 响应已转换并从正式境外期货事实复读 1 行。 "
                + warning_map[snapshot_date]
            ),
        }
        self.assertTrue(
            overseas.calendar_grid_is_complete(
                calendar_row,
                1,
                warning_map[snapshot_date],
            )
        )
        calendar_row["quality_status"] = "passed"
        calendar_row["quality_reason"] = "错误地通过。"
        self.assertFalse(
            overseas.calendar_grid_is_complete(
                calendar_row,
                1,
                warning_map[snapshot_date],
            )
        )

        missing_df = raw_df.copy()
        missing_df.loc[0, "change_pct"] = float("nan")
        missing_df.loc[0, "amplitude"] = float("nan")
        missing_df.loc[0, "pre_close"] = 0.0
        normalized_missing = overseas.normalize_overseas_futures_response(
            missing_df,
            snapshot_date,
            CHECKED_AT,
        )
        normalized_missing_row = overseas.pandas_to_arrow(
            normalized_missing,
            OVERSEAS_FUTURES_DAILY_SCHEMA,
        ).to_pylist()[0]
        self.assertIsNone(normalized_missing_row["change_pct"])
        self.assertIsNone(normalized_missing_row["amplitude"])
        self.assertEqual(normalized_missing_row["previous_close"], 0.0)
        self.assertEqual(
            overseas.ohlc_relation_warning_map(normalized_missing),
            warning_map,
        )

        for field_name, nonfinite_value in [
            ("open", float("inf")),
            ("change_pct", float("-inf")),
            ("amplitude", float("inf")),
        ]:
            nonfinite_df = raw_df.copy()
            nonfinite_df.loc[0, field_name] = nonfinite_value
            with self.subTest(field_name=field_name):
                with self.assertRaisesRegex(ValueError, "有限数"):
                    overseas.normalize_overseas_futures_response(
                        nonfinite_df,
                        snapshot_date,
                        CHECKED_AT,
                    )

        nonnumeric_df = raw_df.copy()
        nonnumeric_df["change_pct"] = nonnumeric_df["change_pct"].astype("object")
        nonnumeric_df.loc[0, "change_pct"] = "NaN"
        with self.assertRaisesRegex(ValueError, "有限数"):
            overseas.normalize_overseas_futures_response(
                nonnumeric_df,
                snapshot_date,
                CHECKED_AT,
            )

        residual_nan = normalized.astype({"change_pct": "float64"})
        residual_nan.loc[0, "change_pct"] = float("nan")
        with self.assertRaisesRegex(ValueError, "有限数"):
            overseas.validate_overseas_futures_frame(
                residual_nan,
                "隔离境外期货事实",
            )

    def test_reconciliation_cannot_pass_with_invalid_retained_minute(self) -> None:
        # 当前疑似 Session 为 0 行；另一个 Session 有两行且总聚合与日线完全一致。
        # 第一根分钟的 high < open，若忽略单行异常，旧逻辑会错误升级为 passed。
        candidate = calendar_row(
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

        retained = calendar_row(
            bar_frequency="1m",
            quality_status="warning",
            quality_reason="包含一条原始 OHLC 跨列异常。",
            evidence_source="fact_futures_minute:invalid_ohlc",
        )
        retained.update({
            "session_number": 2,
            "session_text": "09:01-09:03",
            "session_start_at": SESSION_END,
            "session_end_at": pd.Timestamp(
                "2026-08-03 09:03:00",
                tz="Asia/Shanghai",
            ),
            "expected_bar_count": 2,
            "actual_bar_count": 2,
        })
        calendar_df = pd.DataFrame([candidate, retained]).loc[
            :, FUTURES_BAR_CALENDAR_SCHEMA.names
        ]

        first_contract = contract_row()
        second_contract = contract_row()
        second_contract.update({
            "session_number": 2,
            "session_text": "09:01-09:03",
            "session_start_at": SESSION_END,
            "session_end_at": pd.Timestamp(
                "2026-08-03 09:03:00",
                tz="Asia/Shanghai",
            ),
            "minute_count": 2,
        })
        contract_df = pd.DataFrame([first_contract, second_contract]).loc[
            :, FUTURES_CONTRACT_CALENDAR_SCHEMA.names
        ]

        first_minute = minute_row()
        first_minute.update({
            "session_number": 2,
            "bar_at": pd.Timestamp(
                "2026-08-03 09:02:00",
                tz="Asia/Shanghai",
            ),
            "open_interest": 19.0,
        })
        second_minute = minute_row()
        second_minute.update({
            "session_number": 2,
            "bar_at": pd.Timestamp(
                "2026-08-03 09:03:00",
                tz="Asia/Shanghai",
            ),
            "open": 99.0,
            "high": 101.0,
            "low": 97.0,
            "close": 100.0,
        })
        minute_df = pd.DataFrame([first_minute, second_minute]).loc[
            :, FUTURES_MINUTE_SCHEMA.names
        ]

        comparison_daily = daily_row(invalid_ohlc=False)
        comparison_daily.update({
            "high": 101.0,
            "low": 97.0,
            "close": 100.0,
            "settlement": 100.0,
            "volume": 20.0,
            "money": 2_000.0,
        })
        daily_df = pd.DataFrame([comparison_daily]).loc[
            :, FUTURES_DAILY_SCHEMA.names
        ]

        candidate_key = ("1m", CONTRACT_CODE, TRADING_DATE, 1)
        _, changed_df, _ = reconciliation.reconcile_partition(
            calendar_df,
            {candidate_key},
            contract_df,
            daily_df,
            minute_df,
            CHECKED_AT,
        )

        self.assertEqual(len(changed_df), 1)
        self.assertEqual(changed_df.iloc[0]["quality_status"], "warning")
        self.assertEqual(changed_df.iloc[0]["evidence_level"], "inferred")
        self.assertIn(
            "原始 OHLC 跨列关系异常",
            changed_df.iloc[0]["quality_reason"],
        )

    def test_full_audit_preserves_ohlc_warning_without_reading_value_tables(self) -> None:
        with tempfile.TemporaryDirectory(prefix="b01-ohlc-retention-") as tmp:
            lake_root = pathlib.Path(tmp)
            silver_root = lake_root / "silver"

            calendar_df = pd.DataFrame([
                calendar_row(
                    bar_frequency="1m",
                    quality_status="warning",
                    quality_reason=MINUTE_OHLC_WARNING_REASON,
                    evidence_source="fact_futures_minute:invalid_ohlc",
                )
            ]).loc[:, FUTURES_BAR_CALENDAR_SCHEMA.names]
            minute_df = pd.DataFrame([minute_row()]).loc[
                :, FUTURES_MINUTE_SCHEMA.names
            ]
            write_partitioned(
                calendar_df,
                silver_root / full_quality.CALENDAR_TABLE_NAME,
                FUTURES_BAR_CALENDAR_SCHEMA,
                full_quality.CALENDAR_PARTITION_COLUMNS,
            )
            write_partitioned(
                minute_df,
                silver_root / full_quality.MINUTE_TABLE_NAME,
                FUTURES_MINUTE_SCHEMA,
                full_quality.MINUTE_PARTITION_COLUMNS,
            )

            missing_staging = lake_root / "missing-staging"
            calendar_staging = lake_root / "calendar-staging"
            result = full_quality.build_full_audit_staging(
                lake_root,
                missing_staging,
                calendar_staging,
                CHECKED_AT,
            )

            self.assertEqual(result["total_actual"], 1)
            self.assertEqual(result["total_missing"], 0)
            self.assertNotIn("total_invalid_ohlc", result)
            self.assertNotIn("anomalous_sessions", result)
            self.assertNotIn("reconciled_candidates", result)

            missing_dataset = ds.dataset(
                missing_staging,
                format="parquet",
                partitioning=full_quality.MISSING_PARTITIONING,
            )
            self.assertEqual(missing_dataset.count_rows(), 0)

            staged_calendar = ds.dataset(
                calendar_staging,
                format="parquet",
                partitioning=full_quality.CALENDAR_PARTITIONING,
            )
            staged_calendar_df = arrow_to_pandas(
                staged_calendar.to_table(
                    columns=FUTURES_BAR_CALENDAR_SCHEMA.names
                ),
                FUTURES_BAR_CALENDAR_SCHEMA,
            )
            self.assertEqual(staged_calendar_df.loc[0, "actual_bar_count"], 1)
            self.assertEqual(staged_calendar_df.loc[0, "missing_bar_count"], 0)
            self.assertEqual(staged_calendar_df.loc[0, "quality_status"], "warning")
            self.assertEqual(
                staged_calendar_df.loc[0, "quality_reason"],
                MINUTE_OHLC_WARNING_REASON,
            )
            self.assertEqual(
                staged_calendar_df.loc[0, "evidence_source"],
                "fact_futures_minute:invalid_ohlc",
            )

            formal_minute_dataset = ds.dataset(
                silver_root / full_quality.MINUTE_TABLE_NAME,
                format="parquet",
                partitioning=full_quality.MINUTE_PARTITIONING,
            )
            formal_row = formal_minute_dataset.to_table(
                columns=FUTURES_MINUTE_SCHEMA.names
            ).to_pylist()[0]
            self.assertEqual(formal_row["open"], 100.0)
            self.assertEqual(formal_row["high"], 99.0)

    def test_full_audit_projects_only_keys_and_finds_exact_missing_minute(self) -> None:
        with tempfile.TemporaryDirectory(prefix="b01-c08-key-difference-") as tmp:
            lake_root = pathlib.Path(tmp)
            silver_root = lake_root / "silver"
            session_start = pd.Timestamp(
                "2026-08-03 10:00:00",
                tz="Asia/Shanghai",
            )
            session_end = pd.Timestamp(
                "2026-08-03 10:04:00",
                tz="Asia/Shanghai",
            )
            warning_reason = "c06 已记录有限 OHLC 跨列异常。"
            calendar = calendar_row(
                bar_frequency="1m",
                quality_status="warning",
                quality_reason=warning_reason,
                evidence_source="c07:daily-minute-evidence",
            )
            calendar.update({
                "session_text": "10:00-10:04",
                "session_start_at": session_start,
                "session_end_at": session_end,
                "expected_bar_count": 4,
                "actual_bar_count": 3,
                "is_data_missing": True,
                "missing_bar_count": 1,
            })
            calendar_df = pd.DataFrame([calendar]).loc[
                :, FUTURES_BAR_CALENDAR_SCHEMA.names
            ]

            minute_rows = []
            for minute_number in (1, 2, 4):
                row = minute_row()
                row["bar_at"] = session_start + pd.Timedelta(
                    minutes=minute_number
                )
                minute_rows.append(row)
            minute_df = pd.DataFrame(minute_rows).loc[
                :, FUTURES_MINUTE_SCHEMA.names
            ]
            write_partitioned(
                calendar_df,
                silver_root / full_quality.CALENDAR_TABLE_NAME,
                FUTURES_BAR_CALENDAR_SCHEMA,
                full_quality.CALENDAR_PARTITION_COLUMNS,
            )
            write_partitioned(
                minute_df,
                silver_root / full_quality.MINUTE_TABLE_NAME,
                FUTURES_MINUTE_SCHEMA,
                full_quality.MINUTE_PARTITION_COLUMNS,
            )

            projected_columns: list[list[str]] = []
            original_open = full_quality.open_exact_dataset

            class MinuteProjection:
                def __init__(self, dataset: ds.Dataset) -> None:
                    self.dataset = dataset

                def __getattr__(self, name: str) -> object:
                    return getattr(self.dataset, name)

                def to_table(self, *args: object, **kwargs: object) -> pa.Table:
                    columns = kwargs.get("columns")
                    projected_columns.append(list(columns))
                    return self.dataset.to_table(*args, **kwargs)

            def tracked_open(*args: object, **kwargs: object) -> ds.Dataset:
                dataset = original_open(*args, **kwargs)
                label = args[4] if len(args) > 4 else kwargs.get("label")
                if label == "一分钟行情事实":
                    return MinuteProjection(dataset)  # type: ignore[return-value]
                return dataset

            missing_staging = lake_root / "missing-staging"
            calendar_staging = lake_root / "calendar-staging"
            with mock.patch.object(
                full_quality,
                "open_exact_dataset",
                side_effect=tracked_open,
            ):
                result = full_quality.build_full_audit_staging(
                    lake_root,
                    missing_staging,
                    calendar_staging,
                    CHECKED_AT,
                )

            self.assertEqual(result["total_sessions"], 1)
            self.assertEqual(result["total_expected"], 4)
            self.assertEqual(result["total_actual"], 3)
            self.assertEqual(result["total_missing"], 1)
            self.assertEqual(
                projected_columns,
                [full_quality.MINUTE_KEY_COLUMNS],
            )
            self.assertFalse(
                (silver_root / "fact_futures_daily").exists()
            )
            self.assertFalse(
                (silver_root / "dim_futures_contract_calendar").exists()
            )

            missing_df = arrow_to_pandas(
                ds.dataset(
                    missing_staging,
                    format="parquet",
                    partitioning=full_quality.MISSING_PARTITIONING,
                ).to_table(columns=full_quality.FUTURES_MISSING_BAR_SCHEMA.names),
                full_quality.FUTURES_MISSING_BAR_SCHEMA,
            )
            self.assertEqual(len(missing_df), 1)
            self.assertEqual(
                missing_df.loc[0, "expected_bar_at"],
                session_start + pd.Timedelta(minutes=3),
            )

            staged_calendar_df = arrow_to_pandas(
                ds.dataset(
                    calendar_staging,
                    format="parquet",
                    partitioning=full_quality.CALENDAR_PARTITIONING,
                ).to_table(columns=FUTURES_BAR_CALENDAR_SCHEMA.names),
                FUTURES_BAR_CALENDAR_SCHEMA,
            )
            self.assertEqual(staged_calendar_df.loc[0, "actual_bar_count"], 3)
            self.assertEqual(staged_calendar_df.loc[0, "missing_bar_count"], 1)
            self.assertTrue(staged_calendar_df.loc[0, "is_data_missing"])
            for field in (
                "quality_status",
                "quality_reason",
                "quality_checked_at",
                "schedule_status",
                "evidence_level",
                "evidence_source",
                "fetch_run_id",
                "is_fetch_completed",
                "fetch_completed_at",
            ):
                self.assertEqual(
                    staged_calendar_df.loc[0, field],
                    calendar_df.loc[0, field],
                )

    def test_full_audit_rejects_incomplete_required_session_before_staging(self) -> None:
        with tempfile.TemporaryDirectory(prefix="b01-c08-not-ready-") as tmp:
            lake_root = pathlib.Path(tmp)
            silver_root = lake_root / "silver"
            calendar = calendar_row(
                bar_frequency="1m",
                quality_status="pending",
                quality_reason="等待 c06 正式提交。",
                evidence_source="calendar",
            )
            calendar["is_fetch_completed"] = False
            calendar_df = pd.DataFrame([calendar]).loc[
                :, FUTURES_BAR_CALENDAR_SCHEMA.names
            ]
            minute_df = pd.DataFrame([minute_row()]).loc[
                :, FUTURES_MINUTE_SCHEMA.names
            ]
            write_partitioned(
                calendar_df,
                silver_root / full_quality.CALENDAR_TABLE_NAME,
                FUTURES_BAR_CALENDAR_SCHEMA,
                full_quality.CALENDAR_PARTITION_COLUMNS,
            )
            write_partitioned(
                minute_df,
                silver_root / full_quality.MINUTE_TABLE_NAME,
                FUTURES_MINUTE_SCHEMA,
                full_quality.MINUTE_PARTITION_COLUMNS,
            )

            missing_staging = lake_root / "missing-staging"
            calendar_staging = lake_root / "calendar-staging"
            with self.assertRaisesRegex(ValueError, "全部 required Session"):
                full_quality.build_full_audit_staging(
                    lake_root,
                    missing_staging,
                    calendar_staging,
                    CHECKED_AT,
                )
            self.assertFalse(missing_staging.exists())
            self.assertFalse(calendar_staging.exists())

    def test_full_audit_cli_cleans_missing_and_calendar_staging_failures(self) -> None:
        for creates_missing in (True, False):
            with self.subTest(creates_missing=creates_missing):
                with tempfile.TemporaryDirectory(
                    prefix="b01-c08-staging-failure-"
                ) as tmp:
                    lake_root = pathlib.Path(tmp)
                    silver_root = lake_root / "silver"
                    calendar_df = pd.DataFrame([
                        calendar_row(
                            bar_frequency="1m",
                            quality_status="passed",
                            quality_reason="c06 正式复读通过。",
                            evidence_source="calendar",
                        )
                    ]).loc[:, FUTURES_BAR_CALENDAR_SCHEMA.names]
                    fact = minute_row()
                    if creates_missing:
                        # c08 信任 c06，不防御性检查这个理论范围外事实键。
                        fact["bar_at"] = SESSION_END + pd.Timedelta(minutes=1)
                    minute_df = pd.DataFrame([fact]).loc[
                        :, FUTURES_MINUTE_SCHEMA.names
                    ]
                    write_partitioned(
                        calendar_df,
                        silver_root / full_quality.CALENDAR_TABLE_NAME,
                        FUTURES_BAR_CALENDAR_SCHEMA,
                        full_quality.CALENDAR_PARTITION_COLUMNS,
                    )
                    write_partitioned(
                        minute_df,
                        silver_root / full_quality.MINUTE_TABLE_NAME,
                        FUTURES_MINUTE_SCHEMA,
                        full_quality.MINUTE_PARTITION_COLUMNS,
                    )

                    with mock.patch.object(
                        full_quality.ds,
                        "write_dataset",
                        side_effect=RuntimeError("injected staging failure"),
                    ):
                        result = CliRunner().invoke(
                            full_quality.main,
                            [
                                "--lake-root",
                                str(lake_root),
                                "--confirm-full-quality",
                                "--write",
                            ],
                        )
                    self.assertNotEqual(result.exit_code, 0)
                    self.assertIsInstance(result.exception, RuntimeError)
                    self.assertEqual(
                        list(silver_root.glob(".b08-s-*")),
                        [],
                    )

    def test_full_audit_formal_read_failure_rolls_back_both_tables(self) -> None:
        with tempfile.TemporaryDirectory(prefix="b01-c08-rollback-") as tmp:
            lake_root = pathlib.Path(tmp)
            silver_root = lake_root / "silver"
            calendar_df = pd.DataFrame([
                calendar_row(
                    bar_frequency="1m",
                    quality_status="passed",
                    quality_reason="c06 正式复读通过。",
                    evidence_source="calendar",
                )
            ]).loc[:, FUTURES_BAR_CALENDAR_SCHEMA.names]
            fact = minute_row()
            fact["bar_at"] = SESSION_END + pd.Timedelta(minutes=1)
            minute_df = pd.DataFrame([fact]).loc[
                :, FUTURES_MINUTE_SCHEMA.names
            ]
            write_partitioned(
                calendar_df,
                silver_root / full_quality.CALENDAR_TABLE_NAME,
                FUTURES_BAR_CALENDAR_SCHEMA,
                full_quality.CALENDAR_PARTITION_COLUMNS,
            )
            write_partitioned(
                minute_df,
                silver_root / full_quality.MINUTE_TABLE_NAME,
                FUTURES_MINUTE_SCHEMA,
                full_quality.MINUTE_PARTITION_COLUMNS,
            )

            original_calendar_table = ds.dataset(
                silver_root / full_quality.CALENDAR_TABLE_NAME,
                format="parquet",
                partitioning=full_quality.CALENDAR_PARTITIONING,
            ).to_table(columns=FUTURES_BAR_CALENDAR_SCHEMA.names)
            original_calendar_digest = full_quality.table_digest(
                original_calendar_table,
                FUTURES_BAR_CALENDAR_SCHEMA,
                full_quality.CALENDAR_PRIMARY_KEY,
            )

            run_id = "rollbacktest1234567890"
            staging_path = silver_root / f".b08-s-{run_id[:12]}"
            missing_staging = staging_path / full_quality.MISSING_TABLE_NAME
            calendar_staging = staging_path / full_quality.CALENDAR_TABLE_NAME
            audit_result = full_quality.build_full_audit_staging(
                lake_root,
                missing_staging,
                calendar_staging,
                CHECKED_AT,
            )
            original_open = full_quality.open_exact_dataset

            def fail_formal_calendar(
                *args: object,
                **kwargs: object,
            ) -> ds.Dataset:
                label = args[4] if len(args) > 4 else kwargs.get("label")
                if label == "正式行情日历":
                    raise RuntimeError("injected formal reread failure")
                return original_open(*args, **kwargs)

            with mock.patch.object(
                full_quality,
                "open_exact_dataset",
                side_effect=fail_formal_calendar,
            ):
                with self.assertRaisesRegex(RuntimeError, "旧目标已恢复") as caught:
                    full_quality.commit_full_audit(
                        lake_root,
                        staging_path,
                        audit_result,
                        run_id,
                    )
                self.assertIn("injected formal reread failure", str(caught.exception.__cause__))

            restored_calendar_table = ds.dataset(
                silver_root / full_quality.CALENDAR_TABLE_NAME,
                format="parquet",
                partitioning=full_quality.CALENDAR_PARTITIONING,
            ).to_table(columns=FUTURES_BAR_CALENDAR_SCHEMA.names)
            self.assertEqual(
                full_quality.table_digest(
                    restored_calendar_table,
                    FUTURES_BAR_CALENDAR_SCHEMA,
                    full_quality.CALENDAR_PRIMARY_KEY,
                ),
                original_calendar_digest,
            )
            self.assertFalse(
                (silver_root / full_quality.MISSING_TABLE_NAME).exists()
            )
            self.assertEqual(list(silver_root.glob(".b08-s-*")), [])
            self.assertEqual(list(silver_root.glob(".b08-b-*")), [])
            self.assertTrue(list(silver_root.glob(".b08-f-*")))


if __name__ == "__main__":
    unittest.main()
