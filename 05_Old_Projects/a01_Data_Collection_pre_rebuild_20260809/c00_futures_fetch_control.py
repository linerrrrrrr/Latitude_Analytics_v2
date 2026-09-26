"""期货日线/分钟线拉取计划、完成状态和缺失检测的共享逻辑。"""

from __future__ import annotations

import shutil
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from config.data_contracts import (
    FUTURES_CONTRACT_CALENDAR_SCHEMA,
    FUTURES_DAILY_SCHEMA,
    FUTURES_FETCH_STATUS_SCHEMA,
    FUTURES_MINUTE_SCHEMA,
    FUTURES_MISSING_BAR_SCHEMA,
    FUTURES_SESSION_SCHEDULE_SIGNAL_SCHEMA,
    FUTURES_VARIETY_CALENDAR_SCHEMA,
    TRADE_CALENDAR_SCHEMA,
    empty_pandas,
    pandas_to_arrow,
)
from c00_futures_universe import selection_reason_map
from c00_lakehouse import (
    dataset_partitions,
    hive_partitioning,
    read_dataset,
    replace_dataset,
    replace_partition,
    swap_staged_dataset,
    validate_dataset_streaming,
)


BAR_FREQUENCIES = ("1d", "1m")
STATUS_KEY = ["bar_frequency", "contract_code", "trading_date", "session_number"]
SCHEDULE_SIGNAL_KEY = [
    "exchange_code",
    "trading_date",
    "session_start_at",
    "session_end_at",
]
STATUS_PARTITION_FIELDS = ["bar_frequency", "exchange_code", "year", "month"]
MISSING_KEY = [
    "bar_frequency",
    "contract_code",
    "trading_date",
    "session_number",
    "expected_bar_at",
]
MISSING_PARTITION_FIELDS = [
    "bar_frequency",
    "exchange_code",
    "underlying_code",
    "year",
    "month",
]
STATUS_PARTITIONING = hive_partitioning(
    [
        pa.field("bar_frequency", pa.string()),
        pa.field("exchange_code", pa.string()),
        pa.field("year", pa.int16()),
        pa.field("month", pa.int8()),
    ]
)
SCHEDULE_SIGNAL_PARTITIONING = hive_partitioning(
    [
        pa.field("exchange_code", pa.string()),
        pa.field("year", pa.int16()),
        pa.field("month", pa.int8()),
    ]
)
MISSING_PARTITIONING = hive_partitioning(
    [
        pa.field("bar_frequency", pa.string()),
        pa.field("exchange_code", pa.string()),
        pa.field("underlying_code", pa.string()),
        pa.field("year", pa.int16()),
        pa.field("month", pa.int8()),
    ]
)
EXCHANGE_MONTH_PARTITIONING = hive_partitioning(
    [
        pa.field("exchange_code", pa.string()),
        pa.field("year", pa.int16()),
        pa.field("month", pa.int8()),
    ]
)
TRADE_YEAR_PARTITIONING = hive_partitioning([pa.field("year", pa.int16())])
CHINA_TZ = "Asia/Shanghai"


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def derive_weekday_non_trading_gap_dates(calendar_df: pd.DataFrame) -> pd.DataFrame:
    """找出前一交易日之后出现过工作日休市的下一交易日，仅生成信号。"""
    if calendar_df.empty:
        return pd.DataFrame(columns=["trading_date", "signal_reason"])

    calendar_rows = sorted(
        (row.calendar_date, bool(row.is_trading_day), int(row.weekday))
        for row in calendar_df.itertuples(index=False)
    )
    rows: list[dict[str, object]] = []
    previous_trading_date: date | None = None
    has_weekday_holiday = False
    for calendar_date, is_trading_day, weekday in calendar_rows:
        if not is_trading_day:
            has_weekday_holiday = has_weekday_holiday or weekday <= 5
            continue
        if has_weekday_holiday:
            rows.append(
                {
                    "trading_date": calendar_date,
                    "signal_reason": (
                        "weekday_holiday_before_first_trading_day"
                        if previous_trading_date is None
                        else "weekday_holiday_between_trading_days"
                    ),
                }
            )
        previous_trading_date = calendar_date
        has_weekday_holiday = False
    return pd.DataFrame(rows, columns=["trading_date", "signal_reason"])


def build_session_schedule_signals(
    calendar_df: pd.DataFrame,
    contract_calendar_df: pd.DataFrame,
    existing_signal_df: pd.DataFrame | None = None,
    updated_at: datetime | None = None,
) -> pd.DataFrame:
    """把工作日休市间隔记录为疑似夜盘关闭信号，不直接豁免拉取。"""
    updated_at = updated_at or utc_now()
    gap_df = derive_weekday_non_trading_gap_dates(calendar_df)
    if gap_df.empty or contract_calendar_df.empty:
        inferred_df = empty_pandas(FUTURES_SESSION_SCHEDULE_SIGNAL_SCHEMA)
    else:
        inferred_df = contract_calendar_df[
            contract_calendar_df["is_night_session"].astype(bool)
        ].copy()
        inferred_df = inferred_df.merge(
            gap_df, on="trading_date", how="inner", validate="many_to_one"
        )
        inferred_df = inferred_df.sort_values(SCHEDULE_SIGNAL_KEY + ["session_text"])
        inferred_df = inferred_df.drop_duplicates(SCHEDULE_SIGNAL_KEY, keep="first")
        inferred_df["schedule_status"] = "suspected_closed"
        inferred_df["evidence_level"] = "inferred"
        inferred_df["evidence_source"] = "derived_trade_calendar_contract_calendar"
        inferred_df["is_fetch_exempt"] = False
        inferred_df["updated_at"] = updated_at
        inferred_df["year"] = inferred_df["trading_date"].map(lambda value: value.year)
        inferred_df["month"] = inferred_df["trading_date"].map(lambda value: value.month)
        inferred_df = inferred_df[FUTURES_SESSION_SCHEDULE_SIGNAL_SCHEMA.names]

    if existing_signal_df is None or existing_signal_df.empty:
        result_df = inferred_df
    else:
        authoritative_df = existing_signal_df[
            existing_signal_df["evidence_level"] == "authoritative"
        ].copy()
        if authoritative_df.empty:
            result_df = inferred_df
        else:
            authoritative_keys = set(
                map(tuple, authoritative_df[SCHEDULE_SIGNAL_KEY].to_numpy())
            )
            inferred_keys = inferred_df[SCHEDULE_SIGNAL_KEY].apply(tuple, axis=1)
            inferred_df = inferred_df[~inferred_keys.isin(authoritative_keys)]
            result_df = pd.concat([authoritative_df, inferred_df], ignore_index=True)

    if result_df.empty:
        return empty_pandas(FUTURES_SESSION_SCHEDULE_SIGNAL_SCHEMA)
    if result_df.duplicated(SCHEDULE_SIGNAL_KEY).any():
        raise ValueError("Session 日历信号主键重复。")
    invalid_inferred_exemption = (
        (result_df["evidence_level"] == "inferred")
        & result_df["is_fetch_exempt"].astype(bool)
    )
    if invalid_inferred_exemption.any():
        raise ValueError("推断信号不得设置 is_fetch_exempt=True。")
    return result_df[FUTURES_SESSION_SCHEDULE_SIGNAL_SCHEMA.names].sort_values(
        SCHEDULE_SIGNAL_KEY
    ).reset_index(drop=True)


def rebuild_session_schedule_signals(
    trade_calendar_path: Path,
    contract_calendar_path: Path,
    signal_path: Path,
) -> pd.DataFrame:
    """重建推断信号并保留既有 authoritative 记录。"""
    calendar_df = read_dataset(
        trade_calendar_path,
        TRADE_YEAR_PARTITIONING,
        TRADE_CALENDAR_SCHEMA,
        ["calendar_date", "is_trading_day", "weekday"],
    )
    gap_df = derive_weekday_non_trading_gap_dates(calendar_df)
    contract_columns = [
        "exchange_code",
        "trading_date",
        "session_text",
        "session_start_at",
        "session_end_at",
        "is_night_session",
        "year",
        "month",
    ]
    if gap_df.empty:
        contract_df = pd.DataFrame(columns=contract_columns)
    else:
        contract_dataset = ds.dataset(
            contract_calendar_path,
            format="parquet",
            partitioning=EXCHANGE_MONTH_PARTITIONING,
        )
        contract_table = contract_dataset.to_table(
            columns=contract_columns,
            filter=(
                (ds.field("is_night_session") == True)  # noqa: E712
                & ds.field("trading_date").isin(gap_df["trading_date"].tolist())
            ),
        )
        contract_df = contract_table.to_pandas()
    existing_df = (
        read_dataset(
            signal_path,
            SCHEDULE_SIGNAL_PARTITIONING,
            FUTURES_SESSION_SCHEDULE_SIGNAL_SCHEMA,
        )
        if signal_path.exists()
        else empty_pandas(FUTURES_SESSION_SCHEDULE_SIGNAL_SCHEMA)
    )
    signal_df = build_session_schedule_signals(
        calendar_df, contract_df, existing_df
    )
    replace_dataset(
        pandas_to_arrow(signal_df, FUTURES_SESSION_SCHEDULE_SIGNAL_SCHEMA),
        signal_path,
        SCHEDULE_SIGNAL_PARTITIONING,
        FUTURES_SESSION_SCHEDULE_SIGNAL_SCHEMA,
    )
    return signal_df


def _initial_state(row_count: int) -> dict[str, object]:
    return {
        "is_fetch_completed": [False] * row_count,
        "is_data_missing": [False] * row_count,
        "actual_bar_count": [0] * row_count,
        "missing_bar_count": [0] * row_count,
        "fetch_run_id": [None] * row_count,
        "fetch_completed_at": [None] * row_count,
        "missing_checked_at": [None] * row_count,
    }


def _preserve_existing_state(
    status_df: pd.DataFrame,
    existing_status_df: pd.DataFrame | None,
) -> pd.DataFrame:
    if existing_status_df is None or existing_status_df.empty:
        return status_df

    state_columns = [
        *STATUS_KEY,
        "is_fetch_required",
        "is_fetch_completed",
        "is_data_missing",
        "expected_bar_count",
        "actual_bar_count",
        "missing_bar_count",
        "fetch_run_id",
        "fetch_completed_at",
        "missing_checked_at",
    ]
    old_df = existing_status_df[state_columns].copy()
    old_df = old_df.rename(
        columns={column: f"old_{column}" for column in state_columns if column not in STATUS_KEY}
    )
    merged_df = status_df.merge(old_df, on=STATUS_KEY, how="left", validate="one_to_one")
    same_requirement = (
        merged_df["is_fetch_required"]
        & merged_df["old_is_fetch_required"].fillna(False).astype(bool)
        & (
            merged_df["expected_bar_count"]
            == merged_df["old_expected_bar_count"].fillna(-1)
        )
    )
    for column in [
        "is_fetch_completed",
        "is_data_missing",
        "actual_bar_count",
        "missing_bar_count",
        "fetch_run_id",
        "fetch_completed_at",
        "missing_checked_at",
    ]:
        old_column = f"old_{column}"
        merged_df.loc[same_requirement, column] = merged_df.loc[
            same_requirement, old_column
        ].to_numpy()
    return merged_df[FUTURES_FETCH_STATUS_SCHEMA.names]


def build_fetch_requirements(
    contract_calendar_df: pd.DataFrame,
    minute_selection_reasons: dict[tuple[str, str], str],
    fetch_exempt_session_keys: set[tuple[object, ...]],
    existing_status_df: pd.DataFrame | None = None,
    updated_at: datetime | None = None,
) -> pd.DataFrame:
    """为一个或多个合约日历分区生成日线和分钟线拉取要求。"""
    if contract_calendar_df.empty:
        return empty_pandas(FUTURES_FETCH_STATUS_SCHEMA)
    updated_at = updated_at or utc_now()

    daily_df = contract_calendar_df[
        ["contract_code", "exchange_code", "underlying_code", "trading_date", "year", "month"]
    ].drop_duplicates(["contract_code", "trading_date"])
    daily_df = daily_df.copy()
    daily_df.insert(0, "bar_frequency", "1d")
    daily_df["session_number"] = 0
    daily_df["session_start_at"] = None
    daily_df["session_end_at"] = None
    daily_df["is_fetch_required"] = True
    daily_df["expected_bar_count"] = 1
    daily_df["selection_reason"] = "all_fixed_commodity_futures"
    for column, values in _initial_state(len(daily_df)).items():
        daily_df[column] = values
    daily_df["updated_at"] = updated_at

    minute_columns = [
        "contract_code",
        "exchange_code",
        "underlying_code",
        "trading_date",
        "session_number",
        "session_start_at",
        "session_end_at",
        "is_night_session",
        "minute_count",
        "year",
        "month",
    ]
    minute_df = contract_calendar_df[minute_columns].copy()
    minute_df.insert(0, "bar_frequency", "1m")
    pair_series = pd.Series(
        list(zip(minute_df["exchange_code"], minute_df["underlying_code"])),
        index=minute_df.index,
    )
    minute_df["selection_reason"] = pair_series.map(minute_selection_reasons)
    is_selected = minute_df["selection_reason"].notna()
    session_key_series = pd.Series(
        list(
            zip(
                minute_df["exchange_code"],
                minute_df["trading_date"],
                minute_df["session_start_at"].map(pd.Timestamp),
                minute_df["session_end_at"].map(pd.Timestamp),
                strict=True,
            )
        ),
        index=minute_df.index,
    )
    is_fetch_exempt = session_key_series.isin(fetch_exempt_session_keys)
    minute_df.loc[~is_selected, "selection_reason"] = "not_selected"
    minute_df["is_fetch_required"] = is_selected & ~is_fetch_exempt
    minute_df["expected_bar_count"] = minute_df["minute_count"].where(
        minute_df["is_fetch_required"], 0
    )
    minute_df = minute_df.drop(columns=["is_night_session", "minute_count"])
    for column, values in _initial_state(len(minute_df)).items():
        minute_df[column] = values
    minute_df["updated_at"] = updated_at

    status_df = pd.concat([daily_df, minute_df], ignore_index=True)
    status_df = status_df[FUTURES_FETCH_STATUS_SCHEMA.names]
    if status_df.duplicated(STATUS_KEY).any():
        duplicates = status_df.loc[status_df.duplicated(STATUS_KEY, keep=False), STATUS_KEY]
        raise ValueError(f"拉取状态主键重复：\n{duplicates.head().to_string(index=False)}")
    status_df = _preserve_existing_state(status_df, existing_status_df)
    return status_df.sort_values(STATUS_KEY).reset_index(drop=True)


def _dataset_filter(values: dict[str, object]) -> ds.Expression:
    expression: ds.Expression | None = None
    for field, value in values.items():
        condition = ds.field(field) == value
        expression = condition if expression is None else expression & condition
    if expression is None:
        raise ValueError("数据集过滤条件不能为空。")
    return expression


def read_status_partition(
    status_path: Path,
    bar_frequency: str,
    exchange_code: str,
    year: int,
    month: int,
) -> pd.DataFrame:
    if not status_path.exists():
        return empty_pandas(FUTURES_FETCH_STATUS_SCHEMA)
    dataset = ds.dataset(status_path, format="parquet", partitioning=STATUS_PARTITIONING)
    table = dataset.to_table(
        columns=FUTURES_FETCH_STATUS_SCHEMA.names,
        filter=_dataset_filter(
            {
                "bar_frequency": bar_frequency,
                "exchange_code": exchange_code,
                "year": int(year),
                "month": int(month),
            }
        ),
    )
    return table.to_pandas()


def rebuild_fetch_status(
    variety_calendar_path: Path,
    contract_calendar_path: Path,
    schedule_signal_path: Path,
    status_path: Path,
) -> tuple[int, int]:
    """按交易所年月流式全量重建状态表，并保留仍有效的完成/缺失状态。"""
    variety_df = read_dataset(
        variety_calendar_path,
        EXCHANGE_MONTH_PARTITIONING,
        FUTURES_VARIETY_CALENDAR_SCHEMA,
        ["exchange_code", "underlying_code"],
    ).drop_duplicates()
    minute_reasons = selection_reason_map(variety_df)
    schedule_signal_df = read_dataset(
        schedule_signal_path,
        SCHEDULE_SIGNAL_PARTITIONING,
        FUTURES_SESSION_SCHEDULE_SIGNAL_SCHEMA,
    )
    invalid_exempt_df = schedule_signal_df[
        schedule_signal_df["is_fetch_exempt"].astype(bool)
        & (schedule_signal_df["evidence_level"] != "authoritative")
    ]
    if not invalid_exempt_df.empty:
        raise ValueError("只有 authoritative Session 信号可以豁免拉取。")
    exempt_df = schedule_signal_df[
        schedule_signal_df["is_fetch_exempt"].astype(bool)
    ]
    fetch_exempt_session_keys = {
        (
            row.exchange_code,
            row.trading_date,
            pd.Timestamp(row.session_start_at),
            pd.Timestamp(row.session_end_at),
        )
        for row in exempt_df.itertuples(index=False)
    }

    contract_dataset = ds.dataset(
        contract_calendar_path,
        format="parquet",
        partitioning=EXCHANGE_MONTH_PARTITIONING,
    )
    partitions = dataset_partitions(
        contract_calendar_path, ["exchange_code", "year", "month"]
    )
    if not partitions:
        raise ValueError(f"合约日历没有可用分区：{contract_calendar_path}")

    staging_path = status_path.with_name(f".{status_path.name}.{uuid.uuid4().hex}.tmp")
    updated_at = utc_now()
    row_count = 0
    required_count = 0
    contract_columns = [
        "contract_code",
        "exchange_code",
        "underlying_code",
        "trading_date",
        "session_number",
        "session_start_at",
        "session_end_at",
        "is_night_session",
        "minute_count",
        "year",
        "month",
    ]
    try:
        for partition in partitions:
            contract_table = contract_dataset.to_table(
                columns=contract_columns,
                filter=_dataset_filter(partition),
            )
            contract_df = contract_table.to_pandas()
            existing_frames = [
                read_status_partition(
                    status_path,
                    frequency,
                    str(partition["exchange_code"]),
                    int(partition["year"]),
                    int(partition["month"]),
                )
                for frequency in BAR_FREQUENCIES
            ]
            existing_df = pd.concat(existing_frames, ignore_index=True)
            status_df = build_fetch_requirements(
                contract_df,
                minute_reasons,
                fetch_exempt_session_keys,
                existing_df,
                updated_at,
            )
            row_count += len(status_df)
            required_count += int(status_df["is_fetch_required"].sum())
            ds.write_dataset(
                pandas_to_arrow(status_df, FUTURES_FETCH_STATUS_SCHEMA),
                staging_path,
                format="parquet",
                partitioning=STATUS_PARTITIONING,
                basename_template=f"part-{uuid.uuid4().hex}-{{i}}.parquet",
                existing_data_behavior="overwrite_or_ignore",
            )
        validate_dataset_streaming(
            staging_path,
            STATUS_PARTITIONING,
            FUTURES_FETCH_STATUS_SCHEMA,
            expected_row_count=row_count,
        )
        swap_staged_dataset(staging_path, status_path)
    finally:
        if staging_path.exists():
            shutil.rmtree(staging_path)
    return row_count, required_count


def fetch_status_rows(
    status_path: Path,
    bar_frequency: str,
    include_completed: bool = False,
) -> pd.DataFrame:
    """读取需要拉取的状态键；默认只返回尚未完成的行。"""
    if bar_frequency not in BAR_FREQUENCIES:
        raise ValueError(f"不支持的 bar_frequency：{bar_frequency}")
    if not status_path.exists():
        raise FileNotFoundError(f"拉取状态表不存在，请先运行 c07：{status_path}")
    expression = (
        (ds.field("bar_frequency") == bar_frequency)
        & (ds.field("is_fetch_required") == True)  # noqa: E712
    )
    if not include_completed:
        expression = expression & (ds.field("is_fetch_completed") == False)  # noqa: E712
    columns = [
        "bar_frequency",
        "contract_code",
        "exchange_code",
        "underlying_code",
        "trading_date",
        "session_number",
        "session_start_at",
        "session_end_at",
        "expected_bar_count",
        "selection_reason",
        "year",
        "month",
    ]
    dataset = ds.dataset(status_path, format="parquet", partitioning=STATUS_PARTITIONING)
    return dataset.to_table(columns=columns, filter=expression).to_pandas()


def mark_fetch_completed(
    status_path: Path,
    bar_frequency: str,
    completed_keys_df: pd.DataFrame,
    fetch_run_id: str,
    completed_at: datetime | None = None,
) -> int:
    """在事实分区写入并校验成功后，把对应状态键标为已完成。"""
    if completed_keys_df.empty:
        return 0
    completed_at = completed_at or utc_now()
    key_columns = ["contract_code", "trading_date", "session_number"]
    required_columns = {*key_columns, "exchange_code", "year", "month"}
    missing_columns = required_columns - set(completed_keys_df.columns)
    if missing_columns:
        raise ValueError(f"完成键缺少字段：{sorted(missing_columns)}")

    keys_df = completed_keys_df[list(required_columns)].drop_duplicates().copy()
    updated_count = 0
    for partition, partition_keys_df in keys_df.groupby(
        ["exchange_code", "year", "month"], sort=True
    ):
        exchange_code, year, month = partition
        status_df = read_status_partition(
            status_path, bar_frequency, str(exchange_code), int(year), int(month)
        )
        marker_df = partition_keys_df[key_columns].drop_duplicates().copy()
        marker_df["_completed_in_run"] = True
        status_df = status_df.merge(
            marker_df, on=key_columns, how="left", validate="one_to_one"
        )
        mask = status_df["_completed_in_run"].fillna(False) & status_df[
            "is_fetch_required"
        ].astype(bool)
        updated_count += int(mask.sum())
        status_df.loc[mask, "is_fetch_completed"] = True
        status_df.loc[mask, "is_data_missing"] = False
        status_df.loc[mask, "actual_bar_count"] = 0
        status_df.loc[mask, "missing_bar_count"] = 0
        status_df.loc[mask, "fetch_run_id"] = fetch_run_id
        status_df.loc[mask, "fetch_completed_at"] = completed_at
        status_df.loc[mask, "missing_checked_at"] = None
        status_df.loc[mask, "updated_at"] = completed_at
        status_df = status_df.drop(columns="_completed_in_run")
        replace_partition(
            pandas_to_arrow(status_df[FUTURES_FETCH_STATUS_SCHEMA.names], FUTURES_FETCH_STATUS_SCHEMA),
            status_path,
            STATUS_PARTITIONING,
            STATUS_PARTITION_FIELDS,
            FUTURES_FETCH_STATUS_SCHEMA,
        )
    if updated_count != len(keys_df):
        raise ValueError(
            f"完成状态更新数量不一致：完成键 {len(keys_df)}，实际更新 {updated_count}。"
        )
    return updated_count


def detect_missing_bars(
    status_df: pd.DataFrame,
    fact_df: pd.DataFrame,
    detected_at: datetime | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """只把已完成且应拉取的状态与事实表比较，返回状态更新和精确缺失明细。"""
    if status_df.empty:
        return status_df, empty_pandas(FUTURES_MISSING_BAR_SCHEMA)
    frequencies = set(status_df["bar_frequency"])
    if len(frequencies) != 1:
        raise ValueError(f"一次缺失检测只能包含一个频率：{frequencies}")
    bar_frequency = frequencies.pop()
    detected_at = detected_at or utc_now()
    working_df = status_df.copy()
    check_mask = working_df["is_fetch_required"].astype(bool) & working_df[
        "is_fetch_completed"
    ].astype(bool)
    check_df = working_df.loc[check_mask].copy()
    missing_rows: list[dict[str, object]] = []

    if bar_frequency == "1d":
        actual_df = fact_df[["contract_code", "trading_date", "has_market_data"]].copy()
        actual_df = actual_df.drop_duplicates(["contract_code", "trading_date"], keep="last")
        actual_df["actual_bar_count"] = actual_df["has_market_data"].fillna(False).astype(int)
        check_df = check_df.drop(columns="actual_bar_count").merge(
            actual_df[["contract_code", "trading_date", "actual_bar_count"]],
            on=["contract_code", "trading_date"],
            how="left",
            validate="one_to_one",
        )
        check_df["actual_bar_count"] = check_df["actual_bar_count"].fillna(0).astype(int)
        check_df["missing_bar_count"] = (
            check_df["expected_bar_count"] - check_df["actual_bar_count"]
        ).clip(lower=0)
        for row in check_df[check_df["missing_bar_count"] > 0].itertuples(index=False):
            expected_at = pd.Timestamp(row.trading_date).tz_localize(CHINA_TZ)
            missing_rows.append(
                {
                    "bar_frequency": "1d",
                    "contract_code": row.contract_code,
                    "exchange_code": row.exchange_code,
                    "underlying_code": row.underlying_code,
                    "trading_date": row.trading_date,
                    "session_number": 0,
                    "expected_bar_at": expected_at,
                    "detected_at": detected_at,
                    "year": row.year,
                    "month": row.month,
                }
            )
    elif bar_frequency == "1m":
        key_columns = ["contract_code", "trading_date", "session_number"]
        actual_df = fact_df[[*key_columns, "bar_at"]].drop_duplicates(
            ["contract_code", "bar_at"], keep="last"
        )
        counts_df = actual_df.groupby(key_columns, as_index=False).size().rename(
            columns={"size": "actual_bar_count"}
        )
        check_df = check_df.drop(columns="actual_bar_count").merge(
            counts_df, on=key_columns, how="left", validate="one_to_one"
        )
        check_df["actual_bar_count"] = check_df["actual_bar_count"].fillna(0).astype(int)
        check_df["missing_bar_count"] = (
            check_df["expected_bar_count"] - check_df["actual_bar_count"]
        ).clip(lower=0)
        actual_groups = {
            key: set(group_df["bar_at"].map(pd.Timestamp))
            for key, group_df in actual_df.groupby(key_columns, sort=False)
        }
        for row in check_df[check_df["missing_bar_count"] > 0].itertuples(index=False):
            key = (row.contract_code, row.trading_date, row.session_number)
            actual_times = actual_groups.get(key, set())
            expected_times = pd.date_range(
                pd.Timestamp(row.session_start_at) + timedelta(minutes=1),
                pd.Timestamp(row.session_end_at),
                freq="1min",
            )
            for expected_at in expected_times:
                if expected_at in actual_times:
                    continue
                missing_rows.append(
                    {
                        "bar_frequency": "1m",
                        "contract_code": row.contract_code,
                        "exchange_code": row.exchange_code,
                        "underlying_code": row.underlying_code,
                        "trading_date": row.trading_date,
                        "session_number": row.session_number,
                        "expected_bar_at": expected_at,
                        "detected_at": detected_at,
                        "year": row.year,
                        "month": row.month,
                    }
                )
    else:
        raise ValueError(f"不支持的 bar_frequency：{bar_frequency}")

    metrics_df = check_df[
        [*STATUS_KEY, "actual_bar_count", "missing_bar_count"]
    ].copy()
    working_df = working_df.drop(columns=["actual_bar_count", "missing_bar_count"]).merge(
        metrics_df,
        on=STATUS_KEY,
        how="left",
        validate="one_to_one",
    )
    working_df["actual_bar_count"] = working_df["actual_bar_count"].fillna(0).astype(int)
    working_df["missing_bar_count"] = working_df["missing_bar_count"].fillna(0).astype(int)
    checked_keys = set(map(tuple, check_df[STATUS_KEY].to_numpy()))
    checked_mask = working_df[STATUS_KEY].apply(tuple, axis=1).isin(checked_keys)
    working_df.loc[checked_mask, "is_data_missing"] = (
        working_df.loc[checked_mask, "missing_bar_count"] > 0
    )
    working_df.loc[checked_mask, "missing_checked_at"] = detected_at
    working_df.loc[checked_mask, "updated_at"] = detected_at
    missing_df = pd.DataFrame(missing_rows, columns=FUTURES_MISSING_BAR_SCHEMA.names)
    if missing_df.empty:
        missing_df = empty_pandas(FUTURES_MISSING_BAR_SCHEMA)
    elif missing_df.duplicated(MISSING_KEY).any():
        raise ValueError("缺失分钟明细主键重复。")
    return (
        working_df[FUTURES_FETCH_STATUS_SCHEMA.names].sort_values(STATUS_KEY).reset_index(drop=True),
        missing_df[FUTURES_MISSING_BAR_SCHEMA.names].sort_values(MISSING_KEY).reset_index(drop=True),
    )


def write_missing_dataset(
    missing_frames: list[pd.DataFrame],
    staging_path: Path,
) -> int:
    """把缺失明细写入暂存目录；零缺失时也创建可正常读取的空数据集。"""
    row_count = 0
    for missing_df in missing_frames:
        if missing_df.empty:
            continue
        row_count += len(missing_df)
        ds.write_dataset(
            pandas_to_arrow(missing_df, FUTURES_MISSING_BAR_SCHEMA),
            staging_path,
            format="parquet",
            partitioning=MISSING_PARTITIONING,
            basename_template=f"part-{uuid.uuid4().hex}-{{i}}.parquet",
            existing_data_behavior="overwrite_or_ignore",
        )
    if row_count == 0:
        staging_path.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pandas_to_arrow(empty_pandas(FUTURES_MISSING_BAR_SCHEMA), FUTURES_MISSING_BAR_SCHEMA),
            staging_path / "part-0.parquet",
        )
    return row_count
