from __future__ import annotations

import importlib.util
import pathlib
import sys
import tempfile
from datetime import date, datetime, timezone
from unittest import mock

import pyarrow as pa
import pandas as pd
from click.testing import CliRunner

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from config.data_contracts import arrow_to_pandas


MODULE_PATH = (
    PROJECT_ROOT
    / "02_Futures_Lakehouse"
    / "a01_Futures_Market_Data"
    / "b05_futures_daily.py"
)
SPEC = importlib.util.spec_from_file_location("c05_daily_incremental", MODULE_PATH)
c05 = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(c05)


CHECKED_AT = datetime(2026, 8, 23, tzinfo=timezone.utc)


def planning_row(
    *,
    exchange_code: str,
    underlying_code: str,
    is_fetch_required: bool,
    is_fetch_completed: bool,
    actual_bar_count: int,
    is_data_missing: bool,
    missing_bar_count: int,
) -> dict[str, object]:
    return {
        "bar_frequency": "1d",
        "contract_code": f"{underlying_code}2601.{exchange_code}",
        "trading_date": date(2026, 8, 21),
        "session_number": 0,
        "exchange_code": exchange_code,
        "underlying_code": underlying_code,
        "schedule_status": "scheduled",
        "is_fetch_required": is_fetch_required,
        "is_fetch_completed": is_fetch_completed,
        "fetch_run_id": "daily-existing" if is_fetch_completed else None,
        "fetch_completed_at": CHECKED_AT if is_fetch_completed else None,
        "actual_bar_count": actual_bar_count,
        "is_data_missing": is_data_missing,
        "missing_bar_count": missing_bar_count,
        "missing_checked_at": CHECKED_AT if is_fetch_completed else None,
        "year": 2026,
        "month": 8,
    }


def planning_frame(rows: list[dict[str, object]]):
    table = pa.Table.from_pylist(rows, schema=c05.CALENDAR_PLANNING_SCHEMA)
    return arrow_to_pandas(table, c05.CALENDAR_PLANNING_SCHEMA)


def full_calendar_frame(
    *,
    exchange_code: str,
    underlying_code: str,
    is_fetch_required: bool,
):
    row = {
        "bar_frequency": "1d",
        "contract_code": f"{underlying_code}2601.{exchange_code}",
        "exchange_code": exchange_code,
        "underlying_code": underlying_code,
        "trading_date": date(2026, 8, 21),
        "session_number": 0,
        "session_text": None,
        "session_start_at": None,
        "session_end_at": None,
        "is_night_session": None,
        "schedule_status": "scheduled",
        "schedule_signal_reason": "测试计划开市。",
        "evidence_level": "contract_rule",
        "evidence_source": "test",
        "is_fetch_required": is_fetch_required,
        "expected_bar_count": 1,
        "selection_reason": "旧政策说明。",
        "is_fetch_completed": True,
        "actual_bar_count": 0,
        "is_data_missing": is_fetch_required,
        "missing_bar_count": 1 if is_fetch_required else 0,
        "fetch_run_id": "daily-existing",
        "fetch_completed_at": CHECKED_AT,
        "missing_checked_at": CHECKED_AT,
        "quality_status": "warning",
        "quality_reason": "正式空结果。",
        "daily_open": 123.0,
        "daily_high": None,
        "daily_low": None,
        "daily_close": None,
        "daily_volume": None,
        "daily_money": None,
        "daily_open_interest": None,
        "aggregated_open": None,
        "aggregated_high": None,
        "aggregated_low": None,
        "aggregated_close": None,
        "aggregated_volume": None,
        "aggregated_money": None,
        "aggregated_open_interest": None,
        "ohlc_matches_daily": None,
        "volume_matches_daily": None,
        "money_matches_daily": None,
        "open_interest_matches_daily": None,
        "quality_checked_at": CHECKED_AT,
        "updated_at": CHECKED_AT,
        "year": 2026,
        "month": 8,
    }
    table = pa.Table.from_pylist(
        [row],
        schema=c05.FUTURES_BAR_CALENDAR_SCHEMA,
    )
    return arrow_to_pandas(table, c05.FUTURES_BAR_CALENDAR_SCHEMA)


def fact_frame(*, high: float = 101.0, volume: float = 10.0):
    row = {
        "contract_code": "BB2601.XDCE",
        "exchange_code": "XDCE",
        "underlying_code": "BB",
        "trading_date": date(2026, 8, 21),
        "previous_close": None,
        "previous_settlement": None,
        "open": 100.0,
        "high": high,
        "low": 99.0,
        "close": 100.5,
        "settlement": 100.2,
        "close_change_from_previous_settlement": None,
        "settlement_change_from_previous_settlement": None,
        "volume": volume,
        "money": 1000.0,
        "open_interest": 20.0,
        "open_interest_change": None,
        "has_market_data": True,
        "source": c05.SOURCE_NAME,
        "updated_at": CHECKED_AT,
        "year": 2026,
        "month": 8,
    }
    table = pa.Table.from_pylist([row], schema=c05.FUTURES_DAILY_SCHEMA)
    return arrow_to_pandas(table, c05.FUTURES_DAILY_SCHEMA)


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


def test_completed_zero_warning_is_not_requested_again() -> None:
    assert "fetch_run_id" not in c05.CALENDAR_PLANNING_COLUMNS
    assert "fetch_completed_at" not in c05.CALENDAR_PLANNING_COLUMNS
    frame = planning_frame([
        planning_row(
            exchange_code="XDCE",
            underlying_code="BB",
            is_fetch_required=True,
            is_fetch_completed=True,
            actual_bar_count=0,
            is_data_missing=True,
            missing_bar_count=1,
        )
    ])

    _, pending_df, completed_df = c05.plan_daily_policy(frame, None, None)

    assert pending_df.empty
    assert len(completed_df) == 1


def test_quota_is_checked_per_batch_and_stops_after_completed_work() -> None:
    pending_df = planning_frame([
        planning_row(
            exchange_code="XDCE",
            underlying_code="BB",
            is_fetch_required=True,
            is_fetch_completed=False,
            actual_bar_count=0,
            is_data_missing=False,
            missing_bar_count=0,
        ),
        {
            **planning_row(
                exchange_code="XDCE",
                underlying_code="BB",
                is_fetch_required=True,
                is_fetch_completed=False,
                actual_bar_count=0,
                is_data_missing=False,
                missing_bar_count=0,
            ),
            "contract_code": "BB2602.XDCE",
        },
    ])

    class FakeCalendarDataset:
        def get_fragments(self, **_kwargs):
            return iter([object()])

        def to_table(self, **_kwargs):
            return pa.Table.from_pandas(
                pending_df,
                schema=c05.CALENDAR_PLANNING_SCHEMA,
                preserve_index=False,
            )

    batches = []
    for index in range(2):
        batch_pending_df = pending_df.iloc[[index]].reset_index(drop=True)
        batches.append({
            "exchange_code": "XDCE",
            "year": 2026,
            "contract_codes": [batch_pending_df.iloc[0]["contract_code"]],
            "pending_df": batch_pending_df,
            "request_start": date(2026, 8, 1),
            "request_end": date(2026, 8, 21),
            "estimated_values": 1,
        })

    with tempfile.TemporaryDirectory() as temporary_directory:
        with (
            mock.patch.object(
                c05,
                "open_contract_dataset",
                return_value=FakeCalendarDataset(),
            ),
            mock.patch.object(
                c05,
                "plan_daily_policy",
                return_value=(
                    pending_df.iloc[0:0].copy(),
                    pending_df,
                    pending_df.iloc[0:0].copy(),
                ),
            ),
            mock.patch.object(c05, "request_batches", return_value=batches),
            mock.patch.object(c05, "quota_spare", side_effect=[1, 0]),
            mock.patch.object(
                c05,
                "collect_batch",
                return_value=(fact_frame(), 7),
            ) as collect_batch_mock,
            mock.patch(
                "config.jqdata_connection.authenticate_jqdata",
                return_value=object(),
            ),
        ):
            result = CliRunner().invoke(
                c05.main,
                [
                    "--lake-root",
                    temporary_directory,
                    "--quota-reserve",
                    "0",
                ],
            )

    assert result.exit_code == 0, result.output
    assert collect_batch_mock.call_count == 1
    assert "request_batch: batch=1/2" in result.output
    assert "quota_stop: batch=2/2" in result.output
    assert "completed_batches=1" in result.output
    assert "remaining_pending=1" in result.output
    assert "requests=3" in result.output


def test_required_unfinished_grid_is_pending() -> None:
    frame = planning_frame([
        planning_row(
            exchange_code="XDCE",
            underlying_code="BB",
            is_fetch_required=True,
            is_fetch_completed=False,
            actual_bar_count=0,
            is_data_missing=False,
            missing_bar_count=0,
        )
    ])

    _, pending_df, completed_df = c05.plan_daily_policy(frame, None, None)

    assert len(pending_df) == 1
    assert completed_df.empty


def test_authoritative_closed_grid_is_not_reselected() -> None:
    row = planning_row(
        exchange_code="XDCE",
        underlying_code="BB",
        is_fetch_required=False,
        is_fetch_completed=True,
        actual_bar_count=0,
        is_data_missing=False,
        missing_bar_count=0,
    )
    row["schedule_status"] = "confirmed_closed"
    frame = planning_frame([row])

    dirty_df, pending_df, completed_df = c05.plan_daily_policy(
        frame,
        None,
        None,
    )

    assert dirty_df.empty
    assert pending_df.empty
    assert completed_df.empty


def test_policy_shrink_preserves_completion_quality_and_c07_evidence() -> None:
    calendar_df = full_calendar_frame(
        exchange_code="XTEST",
        underlying_code="ZZ",
        is_fetch_required=True,
    )
    narrow_df = planning_frame([
        planning_row(
            exchange_code="XTEST",
            underlying_code="ZZ",
            is_fetch_required=True,
            is_fetch_completed=True,
            actual_bar_count=0,
            is_data_missing=True,
            missing_bar_count=1,
        )
    ])
    dirty_df, pending_df, _ = c05.plan_daily_policy(narrow_df, None, None)

    updated_df = c05.apply_policy_to_calendar_leaf(
        calendar_df,
        dirty_df,
        CHECKED_AT,
    )

    row = updated_df.iloc[0]
    assert pending_df.empty
    assert not row["is_fetch_required"]
    assert not row["is_data_missing"]
    assert row["missing_bar_count"] == 0
    assert row["is_fetch_completed"]
    assert row["fetch_run_id"] == "daily-existing"
    assert row["actual_bar_count"] == 0
    assert row["quality_status"] == "warning"
    assert row["daily_open"] == 123.0


def test_policy_reinclude_uses_completion_evidence_without_api() -> None:
    calendar_df = full_calendar_frame(
        exchange_code="XDCE",
        underlying_code="BB",
        is_fetch_required=False,
    )
    narrow_df = planning_frame([
        planning_row(
            exchange_code="XDCE",
            underlying_code="BB",
            is_fetch_required=False,
            is_fetch_completed=True,
            actual_bar_count=0,
            is_data_missing=False,
            missing_bar_count=0,
        )
    ])
    dirty_df, pending_df, completed_df = c05.plan_daily_policy(
        narrow_df,
        None,
        None,
    )

    updated_df = c05.apply_policy_to_calendar_leaf(
        calendar_df,
        dirty_df,
        CHECKED_AT,
    )

    row = updated_df.iloc[0]
    assert pending_df.empty
    assert len(completed_df) == 1
    assert row["is_fetch_required"]
    assert row["is_data_missing"]
    assert row["missing_bar_count"] == 1
    assert row["is_fetch_completed"]
    assert row["quality_status"] == "warning"
    assert row["daily_open"] == 123.0


def test_finite_ohlc_relationship_warning_is_accepted() -> None:
    # high 低于 close 是有限跨列异常；事实值必须保留，不能硬失败。
    checked_df = c05.validate_daily_frame(
        fact_frame(high=100.0),
        "测试事实 ",
    )
    assert checked_df.iloc[0]["high"] == 100.0
    assert c05.invalid_ohlc_mask(checked_df).iloc[0]


def test_negative_quantity_and_nan_are_hard_failures() -> None:
    try:
        c05.validate_daily_frame(fact_frame(volume=-1.0), "测试事实 ")
    except ValueError as error:
        assert "volume 不得为负" in str(error)
    else:
        raise AssertionError("负成交量未被拒绝。")

    nan_df = fact_frame()
    nan_df["open"] = nan_df["open"].astype(object)
    nan_df.at[0, "open"] = float("nan")
    try:
        c05.validate_daily_frame(nan_df, "测试事实 ")
    except ValueError as error:
        assert "open 包含非有限数" in str(error)
    else:
        raise AssertionError("NaN 未被拒绝。")


def test_nonconsecutive_pending_rows_defer_predecessor_checks() -> None:
    first_df = fact_frame()
    first_df.loc[0, "trading_date"] = date(2026, 8, 19)
    first_df.loc[0, "settlement"] = 100.0
    first_df.loc[0, "open_interest"] = 10.0

    second_df = fact_frame()
    second_df.loc[0, "trading_date"] = date(2026, 8, 21)
    second_df.loc[0, "previous_settlement"] = 150.0
    second_df.loc[0, "close"] = 152.0
    second_df.loc[0, "settlement"] = 151.0
    second_df.loc[0, "close_change_from_previous_settlement"] = 2.0
    second_df.loc[0, "settlement_change_from_previous_settlement"] = 1.0
    second_df.loc[0, "open_interest"] = 30.0
    second_df.loc[0, "open_interest_change"] = 5.0
    pending_df = pd.concat([first_df, second_df], ignore_index=True)

    checked_df = c05.validate_daily_frame(
        pending_df,
        "非连续 pending ",
        validate_predecessor_relationships=False,
    )
    assert len(checked_df) == 2

    try:
        c05.validate_daily_frame(pending_df, "错误完整叶 ")
    except ValueError as error:
        assert "previous_settlement 与前序事实不一致" in str(error)
    else:
        raise AssertionError("完整叶的前序事实不一致未被拒绝。")


def test_api_history_rejects_extra_contract_and_negative_quantity() -> None:
    extra_df = pd.DataFrame(
        {
            "BB2601.XDCE": [100.0],
            "RB2609.XSGE": [200.0],
        },
        index=[date(2026, 8, 1)],
    )
    try:
        c05.normalize_extra_response(
            extra_df,
            ["BB2601.XDCE"],
            "settlement",
        )
    except ValueError as error:
        assert "返回未请求合约" in str(error)
    else:
        raise AssertionError("get_extras 额外合约列未被拒绝。")

    negative_price_df = pd.DataFrame({
        "time": [pd.Timestamp("2026-08-01")],
        "code": ["BB2601.XDCE"],
        "open": [100.0],
        "high": [101.0],
        "low": [99.0],
        "close": [100.5],
        "volume": [-1.0],
        "money": [1000.0],
        "pre_close": [100.0],
    })
    try:
        c05.normalize_price_response(
            negative_price_df,
            ["BB2601.XDCE"],
        )
    except ValueError as error:
        assert "volume 不得为负" in str(error)
    else:
        raise AssertionError("回看窗口中的负成交量未被拒绝。")


def test_completion_update_is_vectorized_and_validates_once() -> None:
    calendar_df = full_calendar_frame(
        exchange_code="XDCE",
        underlying_code="BB",
        is_fetch_required=True,
    )
    updated_df = c05.apply_completion_to_calendar_leaf(
        calendar_df,
        fact_frame(high=100.0),
        "daily-new",
        CHECKED_AT,
    )
    checked_df = c05.validate_calendar_state_frame(
        updated_df,
        "测试日历叶 ",
    )
    row = checked_df.iloc[0]
    assert row["is_fetch_completed"]
    assert row["actual_bar_count"] == 1
    assert not row["is_data_missing"]
    assert row["missing_bar_count"] == 0
    assert row["fetch_run_id"] == "daily-new"
    assert row["quality_status"] == "warning"


def test_api_date_outside_request_range_is_hard_failure() -> None:
    pending_df = planning_frame([
        planning_row(
            exchange_code="XDCE",
            underlying_code="BB",
            is_fetch_required=True,
            is_fetch_completed=False,
            actual_bar_count=0,
            is_data_missing=False,
            missing_bar_count=0,
        )
    ])

    class FakeJQData:
        @staticmethod
        def get_price(*args, **kwargs):
            return pd.DataFrame({
                "time": [date(2026, 8, 22)],
                "code": ["BB2601.XDCE"],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
                "volume": [1.0],
                "money": [100.0],
                "pre_close": [100.0],
            })

        @staticmethod
        def get_extras(*args, **kwargs):
            return pd.DataFrame()

    batch = {
        "contract_codes": ["BB2601.XDCE"],
        "request_start": date(2026, 8, 1),
        "request_end": date(2026, 8, 21),
        "pending_df": pending_df,
    }
    try:
        c05.collect_batch(FakeJQData(), batch, CHECKED_AT)
    except ValueError as error:
        assert "返回日期越出请求范围" in str(error)
    else:
        raise AssertionError("越界来源日期未被拒绝。")


def test_legacy_descriptive_metadata_is_accepted_without_migration() -> None:
    legacy_schema = schema_with_legacy_descriptions(
        c05.FUTURES_DAILY_SCHEMA
    )
    assert not legacy_schema.equals(
        c05.FUTURES_DAILY_SCHEMA,
        check_metadata=True,
    )
    assert c05.physically_and_identity_compatible(
        legacy_schema,
        c05.FUTURES_DAILY_SCHEMA,
    )

    for identity_key in c05.SCHEMA_IDENTITY_METADATA_KEYS:
        incompatible_metadata = dict(legacy_schema.metadata or {})
        incompatible_metadata[identity_key] = b"incompatible"
        assert not c05.physically_and_identity_compatible(
            legacy_schema.with_metadata(incompatible_metadata),
            c05.FUTURES_DAILY_SCHEMA,
        )
    renamed_fields = list(legacy_schema)
    renamed_fields[0] = pa.field(
        "renamed_contract_code",
        renamed_fields[0].type,
        nullable=renamed_fields[0].nullable,
        metadata=renamed_fields[0].metadata,
    )
    assert not c05.physically_and_identity_compatible(
        pa.schema(renamed_fields, metadata=legacy_schema.metadata),
        c05.FUTURES_DAILY_SCHEMA,
    )
    changed_type_fields = list(legacy_schema)
    changed_type_fields[0] = pa.field(
        changed_type_fields[0].name,
        pa.int64(),
        nullable=changed_type_fields[0].nullable,
        metadata=changed_type_fields[0].metadata,
    )
    assert not c05.physically_and_identity_compatible(
        pa.schema(changed_type_fields, metadata=legacy_schema.metadata),
        c05.FUTURES_DAILY_SCHEMA,
    )
    changed_nullable_fields = list(legacy_schema)
    changed_nullable_fields[0] = pa.field(
        changed_nullable_fields[0].name,
        changed_nullable_fields[0].type,
        nullable=not changed_nullable_fields[0].nullable,
        metadata=changed_nullable_fields[0].metadata,
    )
    assert not c05.physically_and_identity_compatible(
        pa.schema(changed_nullable_fields, metadata=legacy_schema.metadata),
        c05.FUTURES_DAILY_SCHEMA,
    )

    with tempfile.TemporaryDirectory() as temporary_dir:
        lake_root = pathlib.Path(temporary_dir)
        table_path = lake_root / "silver" / c05.TABLE_NAME
        legacy_table = pa.Table.from_pandas(
            fact_frame(),
            schema=legacy_schema,
            preserve_index=False,
        )
        c05.ds.write_dataset(
            legacy_table,
            table_path,
            format="parquet",
            partitioning=c05.HIVE_PARTITIONING,
            basename_template="part-{i}.parquet",
        )
        legacy_marker_schema = c05.parquet_file_schema(
            legacy_schema,
            c05.PARTITION_COLUMNS,
        )
        c05.pq.write_table(
            pa.Table.from_batches([], schema=legacy_marker_schema),
            table_path / "schema.parquet",
        )
        marker_before = (table_path / "schema.parquet").read_bytes()

        opened_dataset = c05.open_contract_dataset(
            table_path,
            c05.HIVE_PARTITIONING,
            c05.FUTURES_DAILY_SCHEMA,
            "旧描述 metadata 测试表",
            required=True,
        )
        assert opened_dataset is not None
        old_leaf_df = c05.read_partition_leaf(
            table_path,
            c05.FUTURES_DAILY_SCHEMA,
            c05.PARTITION_COLUMNS,
            c05.HIVE_PARTITIONING,
            ("XDCE", "BB", 2026, 8),
        )
        assert old_leaf_df.iloc[0]["high"] == 101.0

        fact_spec = leaf_specs(
            fact_frame(high=102.0),
            full_calendar_frame(
                exchange_code="XDCE",
                underlying_code="BB",
                is_fetch_required=True,
            ),
        )[0]
        assert c05.commit_validated_leaf_group(
            [fact_spec],
            lake_root,
        ) == 1
        assert (table_path / "schema.parquet").read_bytes() == marker_before
        current_leaf_df = c05.read_partition_leaf(
            table_path,
            c05.FUTURES_DAILY_SCHEMA,
            c05.PARTITION_COLUMNS,
            c05.HIVE_PARTITIONING,
            ("XDCE", "BB", 2026, 8),
        )
        assert current_leaf_df.iloc[0]["high"] == 102.0


def leaf_specs(fact_df, calendar_df):
    return [
        {
            "table_name": c05.TABLE_NAME,
            "schema": c05.FUTURES_DAILY_SCHEMA,
            "partition_columns": c05.PARTITION_COLUMNS,
            "partitioning": c05.HIVE_PARTITIONING,
            "primary_key": c05.PRIMARY_KEY,
            "partition_key": ("XDCE", "BB", 2026, 8),
            "frame": c05.validate_daily_frame(fact_df, "测试事实叶 "),
        },
        {
            "table_name": c05.CALENDAR_TABLE_NAME,
            "schema": c05.FUTURES_BAR_CALENDAR_SCHEMA,
            "partition_columns": c05.CALENDAR_PARTITION_COLUMNS,
            "partitioning": c05.CALENDAR_PARTITIONING,
            "primary_key": c05.CALENDAR_PRIMARY_KEY,
            "partition_key": ("1d", "XDCE", 2026, 8),
            "frame": c05.validate_calendar_state_frame(
                calendar_df,
                "测试日历叶 ",
            ),
        },
    ]


def test_coordinated_leaf_install_rolls_back_both_tables() -> None:
    calendar_df = full_calendar_frame(
        exchange_code="XDCE",
        underlying_code="BB",
        is_fetch_required=True,
    )
    with tempfile.TemporaryDirectory() as temporary_dir:
        lake_root = pathlib.Path(temporary_dir)
        c05.commit_validated_leaf_group(
            leaf_specs(fact_frame(high=101.0), calendar_df),
            lake_root,
        )

        transaction_module = sys.modules[c05.StagedPathTransaction.__module__]
        original_move = transaction_module.os.replace

        def failing_move(source, destination, *args, **kwargs):
            if (
                ".c05s-" in str(source)
                and c05.CALENDAR_TABLE_NAME in str(destination)
            ):
                raise OSError("injected calendar install failure")
            return original_move(source, destination, *args, **kwargs)

        transaction_module.os.replace = failing_move
        try:
            try:
                c05.commit_validated_leaf_group(
                    leaf_specs(fact_frame(high=102.0), calendar_df),
                    lake_root,
                )
            except RuntimeError as error:
                assert "旧目标已恢复" in str(error)
                assert isinstance(error.__cause__, OSError)
            else:
                raise AssertionError("注入的协调安装失败未触发。")
        finally:
            transaction_module.os.replace = original_move

        fact_path = lake_root / "silver" / c05.TABLE_NAME
        restored_fact_df = c05.read_partition_leaf(
            fact_path,
            c05.FUTURES_DAILY_SCHEMA,
            c05.PARTITION_COLUMNS,
            c05.HIVE_PARTITIONING,
            ("XDCE", "BB", 2026, 8),
        )
        calendar_path = lake_root / "silver" / c05.CALENDAR_TABLE_NAME
        restored_calendar_df = c05.read_partition_leaf(
            calendar_path,
            c05.FUTURES_BAR_CALENDAR_SCHEMA,
            c05.CALENDAR_PARTITION_COLUMNS,
            c05.CALENDAR_PARTITIONING,
            ("1d", "XDCE", 2026, 8),
        )
        assert restored_fact_df.iloc[0]["high"] == 101.0
        assert restored_calendar_df.iloc[0]["fetch_run_id"] == "daily-existing"


def test_formal_leaf_reread_failure_rolls_back_both_tables() -> None:
    calendar_df = full_calendar_frame(
        exchange_code="XDCE",
        underlying_code="BB",
        is_fetch_required=True,
    )
    with tempfile.TemporaryDirectory() as temporary_dir:
        lake_root = pathlib.Path(temporary_dir)
        c05.commit_validated_leaf_group(
            leaf_specs(fact_frame(high=101.0), calendar_df),
            lake_root,
        )
        silver_root = (lake_root / "silver").resolve()
        original_dataset = c05.ds.dataset

        def failing_formal_dataset(source, *args, **kwargs):
            source_path = pathlib.Path(source).resolve()
            if source_path.is_relative_to(silver_root):
                relative_parts = source_path.relative_to(silver_root).parts
                if relative_parts and not relative_parts[0].startswith(".c05"):
                    raise OSError("injected formal reread failure")
            return original_dataset(source, *args, **kwargs)

        c05.ds.dataset = failing_formal_dataset
        try:
            try:
                c05.commit_validated_leaf_group(
                    leaf_specs(fact_frame(high=103.0), calendar_df),
                    lake_root,
                )
            except RuntimeError as error:
                assert "旧目标已恢复" in str(error)
                assert isinstance(error.__cause__, OSError)
            else:
                raise AssertionError("注入的正式叶复读失败未触发。")
        finally:
            c05.ds.dataset = original_dataset

        restored_fact_df = c05.read_partition_leaf(
            lake_root / "silver" / c05.TABLE_NAME,
            c05.FUTURES_DAILY_SCHEMA,
            c05.PARTITION_COLUMNS,
            c05.HIVE_PARTITIONING,
            ("XDCE", "BB", 2026, 8),
        )
        restored_calendar_df = c05.read_partition_leaf(
            lake_root / "silver" / c05.CALENDAR_TABLE_NAME,
            c05.FUTURES_BAR_CALENDAR_SCHEMA,
            c05.CALENDAR_PARTITION_COLUMNS,
            c05.CALENDAR_PARTITIONING,
            ("1d", "XDCE", 2026, 8),
        )
        assert restored_fact_df.iloc[0]["high"] == 101.0
        assert restored_calendar_df.iloc[0]["fetch_run_id"] == "daily-existing"


if __name__ == "__main__":
    test_completed_zero_warning_is_not_requested_again()
    test_quota_is_checked_per_batch_and_stops_after_completed_work()
    test_required_unfinished_grid_is_pending()
    test_authoritative_closed_grid_is_not_reselected()
    test_policy_shrink_preserves_completion_quality_and_c07_evidence()
    test_policy_reinclude_uses_completion_evidence_without_api()
    test_finite_ohlc_relationship_warning_is_accepted()
    test_negative_quantity_and_nan_are_hard_failures()
    test_nonconsecutive_pending_rows_defer_predecessor_checks()
    test_api_history_rejects_extra_contract_and_negative_quantity()
    test_completion_update_is_vectorized_and_validates_once()
    test_api_date_outside_request_range_is_hard_failure()
    test_legacy_descriptive_metadata_is_accepted_without_migration()
    test_coordinated_leaf_install_rolls_back_both_tables()
    test_formal_leaf_reread_failure_rolls_back_both_tables()
