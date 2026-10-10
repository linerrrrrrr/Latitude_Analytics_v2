"""从正式日历与项目库构造金融期货精确缺失和有界拉取计划。"""

from __future__ import annotations

import datetime
import hashlib
import json
import pathlib
import sys
from collections import defaultdict
from dataclasses import dataclass
from zoneinfo import ZoneInfo

import duckdb
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

from config.data_contracts import (  # noqa: E402
    FUTURES_BAR_CALENDAR_SCHEMA,
    TRADE_CALENDAR_SCHEMA,
    arrow_to_pandas,
)
from config.settings import settings  # noqa: E402

from financial_futures_collection_policy import (  # noqa: E402
    FINANCIAL_FUTURES_BAR_FREQUENCIES,
    FINANCIAL_FUTURES_EXCHANGE_CODE,
    FINANCIAL_FUTURES_UNDERLYING_CODES,
    JOINQUANT_COMPLETED_DATA_READY_TIME,
    JOINQUANT_MARKET_TIME_ZONE,
    JOINQUANT_MAX_EXPECTED_KEYS_PER_SOURCE_REQUEST,
    JOINQUANT_MAX_EXPECTED_KEYS_PER_TRANSFER,
    JOINQUANT_MAX_OBSERVATIONS_PER_TRANSFER,
    JOINQUANT_MAX_PLANNED_EXPECTED_KEYS_PER_TRANSFER,
    JOINQUANT_MAX_SOURCE_REQUESTS_PER_TRANSFER,
)
from financial_futures_local_contract import (  # noqa: E402
    JOINQUANT_DAILY_SOURCE_FIELDS,
    JOINQUANT_MINUTE_SOURCE_FIELDS,
    TRANSFER_FORMAT_ID,
    TRANSFER_GENERATOR_VERSION,
    TRANSFER_MARKET_TIME_ZONE,
    TRANSFER_PROTOCOL_VERSION,
    TRANSFER_SOURCE_PARAMETERS,
    validate_database_schema,
)


FETCH_PLANNER_VERSION = "financial_futures_fetch_planner_v1"
TRADE_CALENDAR_TABLE_NAME = TRADE_CALENDAR_SCHEMA.metadata[b"table_name"].decode("utf-8")
TRADE_CALENDAR_PARTITION_COLUMNS = TRADE_CALENDAR_SCHEMA.metadata[b"partition_columns"].decode("utf-8").split(",")


@dataclass
class FinancialFuturesFetchPlan:
    """一次只读规划的完整结果；DataFrame 均为当前计算快照。"""

    as_of: datetime.datetime
    latest_due_trading_date: datetime.date
    database_exists: bool
    coverage_summary_df: pd.DataFrame
    not_yet_fetchable_blocks_df: pd.DataFrame
    missing_blocks_df: pd.DataFrame
    minute_missing_ranges_df: pd.DataFrame
    planned_observations_df: pd.DataFrame
    planned_source_requests_df: pd.DataFrame
    plan_payload: dict[str, object]
    plan_sha256: str
    remaining_transfer_count: int
    remaining_source_request_count: int
    transfer_schedule_df: pd.DataFrame
    calendar_coverage_df: pd.DataFrame
    calendar_is_current: bool
    trade_calendar_through: datetime.date | None
    latest_due_market_trading_date: datetime.date | None
    next_run_at: datetime.datetime


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _market_timestamp(value: object) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize(JOINQUANT_MARKET_TIME_ZONE)
    return timestamp.tz_convert(JOINQUANT_MARKET_TIME_ZONE)


def _timestamp_text(value: object) -> str:
    return _market_timestamp(value).to_pydatetime().isoformat(timespec="seconds")


def build_financial_futures_fetch_plan(
    *,
    as_of: datetime.datetime | None = None,
    database_path: pathlib.Path | None = None,
    acceptance_contract_dates: tuple[tuple[str, datetime.date], ...] | None = None,
) -> FinancialFuturesFetchPlan:
    """只读计算全部缺失状态；显式验收坐标只限制请求选择，不裁剪缺失宇宙。"""

    acceptance_coordinates = None
    if acceptance_contract_dates is not None:
        if not isinstance(acceptance_contract_dates, tuple):
            raise TypeError("acceptance_contract_dates 必须是坐标元组或 None。")
        acceptance_coordinates = set()
        for coordinate in acceptance_contract_dates:
            if (not isinstance(coordinate, tuple) or len(coordinate) != 2
                    or type(coordinate[0]) is not str
                    or type(coordinate[1]) is not datetime.date):
                raise TypeError("每个验收坐标必须为 (合约字符串, datetime.date)，不能使用日期字符串或 datetime。")
            if coordinate in acceptance_coordinates:
                raise ValueError(f"验收坐标重复：{coordinate}")
            acceptance_coordinates.add(coordinate)

    market_zone = ZoneInfo(JOINQUANT_MARKET_TIME_ZONE)
    if as_of is None:
        as_of = datetime.datetime.now(tz=market_zone)
    elif as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("as_of 必须是带时区时间，不能把无时区值猜成北京时间。")
    else:
        as_of = as_of.astimezone(market_zone)

    ready_time_today = datetime.datetime.combine(
        as_of.date(),
        JOINQUANT_COMPLETED_DATA_READY_TIME,
        tzinfo=market_zone,
    )
    due_day_offset = 1 if as_of >= ready_time_today else 2
    latest_due_trading_date = as_of.date() - datetime.timedelta(
        days=due_day_offset
    )

    calendar_table_name = FUTURES_BAR_CALENDAR_SCHEMA.metadata[
        b"table_name"
    ].decode("utf-8")
    calendar_path = settings.futures_lake_root / "silver" / calendar_table_name
    if not calendar_path.is_dir():
        raise FileNotFoundError(f"正式行情日历不存在：{calendar_path}")

    partition_columns = FUTURES_BAR_CALENDAR_SCHEMA.metadata[
        b"partition_columns"
    ].decode("utf-8").split(",")
    calendar_dataset = ds.dataset(
        calendar_path,
        format="parquet",
        partitioning=ds.partitioning(
            pa.schema(
                [FUTURES_BAR_CALENDAR_SCHEMA.field(name) for name in partition_columns]
            ),
            flavor="hive",
        ),
    )
    calendar_filter = (
        (ds.field("exchange_code") == FINANCIAL_FUTURES_EXCHANGE_CODE)
        & ds.field("underlying_code").isin(
            sorted(FINANCIAL_FUTURES_UNDERLYING_CODES)
        )
        & ds.field("bar_frequency").isin(
            list(FINANCIAL_FUTURES_BAR_FREQUENCIES)
        )
    )
    calendar_table = calendar_dataset.to_table(filter=calendar_filter).select(
        FUTURES_BAR_CALENDAR_SCHEMA.names
    )
    calendar_df = arrow_to_pandas(
        calendar_table,
        FUTURES_BAR_CALENDAR_SCHEMA,
    )

    block_columns = [
        "bar_frequency",
        "contract_code",
        "exchange_code",
        "underlying_code",
        "trading_date",
        "session_number",
        "session_start_at",
        "session_end_at",
        "schedule_status",
        "evidence_level",
        "expected_bar_count",
    ]
    structure_blocks_df = calendar_df.loc[:, block_columns].copy()
    frequency_rank = {"1d": 0, "1m": 1}
    structure_blocks_df["frequency_rank"] = structure_blocks_df[
        "bar_frequency"
    ].map(frequency_rank)
    structure_blocks_df = structure_blocks_df.sort_values(
        [
            "frequency_rank",
            "contract_code",
            "trading_date",
            "session_number",
        ],
        kind="stable",
        ignore_index=True,
    )
    structure_blocks_df["structure_ordinal"] = (
        structure_blocks_df.groupby(
            ["bar_frequency", "contract_code"],
            sort=False,
            observed=True,
        ).cumcount()
    )

    authoritative_closed = (
        structure_blocks_df["schedule_status"].eq("confirmed_closed")
        & structure_blocks_df["evidence_level"].eq("authoritative")
    )
    structurally_expected_df = structure_blocks_df.loc[
        ~authoritative_closed
    ].copy()
    due_mask = structurally_expected_df["trading_date"].le(
        latest_due_trading_date
    )
    expected_due_df = structurally_expected_df.loc[due_mask].copy()
    not_yet_fetchable_blocks_df = structurally_expected_df.loc[
        ~due_mask,
        block_columns,
    ].copy()
    expected_due_df = expected_due_df.reset_index(drop=True)
    expected_due_df.insert(0, "block_id", expected_due_df.index.astype("int64"))

    # 只读最上游自然日日历，区分周末/节假日与上游尚未更新，不能把旧日历当成最新。
    trade_calendar_dataset = ds.dataset(
        settings.futures_lake_root / "silver" / TRADE_CALENDAR_TABLE_NAME,
        format="parquet",
        partitioning=ds.partitioning(
            pa.schema([TRADE_CALENDAR_SCHEMA.field(name) for name in TRADE_CALENDAR_PARTITION_COLUMNS]),
            flavor="hive",
        ),
    )
    trade_calendar_df = arrow_to_pandas(
        trade_calendar_dataset.to_table(filter=ds.field("calendar_date") <= latest_due_trading_date)
        .select(TRADE_CALENDAR_SCHEMA.names),
        TRADE_CALENDAR_SCHEMA,
    )
    trade_calendar_through = (
        pd.Timestamp(trade_calendar_df["calendar_date"].max()).date()
        if len(trade_calendar_df) else None
    )
    due_market_dates = trade_calendar_df.loc[trade_calendar_df["is_trading_day"], "calendar_date"]
    latest_due_market_trading_date = pd.Timestamp(due_market_dates.max()).date() if len(due_market_dates) else None
    calendar_coverage_df = (
        structure_blocks_df.loc[structure_blocks_df["trading_date"].le(latest_due_trading_date)]
        .groupby(["bar_frequency", "underlying_code"], observed=True)["trading_date"].max()
        .reindex(pd.MultiIndex.from_product(
            [FINANCIAL_FUTURES_BAR_FREQUENCIES, sorted(FINANCIAL_FUTURES_UNDERLYING_CODES)],
            names=["bar_frequency", "underlying_code"],
        )).rename("latest_calendar_trading_date").reset_index()
    )
    calendar_coverage_df["is_current"] = [
        latest_due_market_trading_date is not None and not pd.isna(value)
        and pd.Timestamp(value).date() >= latest_due_market_trading_date
        for value in calendar_coverage_df["latest_calendar_trading_date"]
    ]
    calendar_is_current = bool(
        trade_calendar_through == latest_due_trading_date
        and calendar_coverage_df["is_current"].all()
    )
    next_run_at = datetime.datetime.combine(
        as_of.date() + datetime.timedelta(days=1), JOINQUANT_COMPLETED_DATA_READY_TIME,
        tzinfo=market_zone,
    )

    if acceptance_coordinates is not None:
        due_coordinates = set(zip(
            expected_due_df["contract_code"], expected_due_df["trading_date"]
        ))
        unknown_coordinates = acceptance_coordinates - due_coordinates
        if unknown_coordinates:
            raise ValueError(
                "验收坐标不在当前白名单已就绪开市结构中，不能自动换样本或退回全量："
                f"{sorted(unknown_coordinates)}"
            )

    database_path = (
        pathlib.Path(database_path).resolve()
        if database_path is not None
        else pathlib.Path(__file__).resolve().parent
        / "data"
        / "warehouse"
        / "financial_futures.duckdb"
    )
    database_exists = database_path.is_file()
    if database_path.exists() and not database_exists:
        raise RuntimeError(f"项目数据库路径存在但不是文件：{database_path}")

    expected_due_df["actual_key_count"] = 0
    observations_df = pd.DataFrame(
        columns=[
            "contract_code",
            "bar_frequency",
            "trading_date",
            "session_number",
            "session_start_at",
            "session_end_at",
        ]
    )
    database_connection: duckdb.DuckDBPyConnection | None = None

    try:
        if database_exists:
            database_connection = duckdb.connect(
                str(database_path),
                read_only=True,
            )
            database_connection.execute(
                f"SET TimeZone='{JOINQUANT_MARKET_TIME_ZONE}'"
            )
            validate_database_schema(database_connection)
            database_connection.register(
                "expected_due_input",
                pa.Table.from_pandas(
                    expected_due_df.loc[
                        :,
                        [
                            "block_id",
                            "bar_frequency",
                            "contract_code",
                            "trading_date",
                            "session_number",
                            "session_start_at",
                            "session_end_at",
                        ],
                    ],
                    preserve_index=False,
                ),
            )

            daily_counts_df = database_connection.execute(
                """
                SELECT expected.block_id, count(fact.contract_code) AS actual_key_count
                FROM expected_due_input AS expected
                LEFT JOIN futures_daily AS fact
                  ON fact.contract_code = expected.contract_code
                 AND fact.trading_date = expected.trading_date
                WHERE expected.bar_frequency = '1d'
                GROUP BY expected.block_id
                """
            ).fetchdf()
            minute_counts_df = database_connection.execute(
                """
                SELECT expected.block_id, count(fact.bar_at) AS actual_key_count
                FROM expected_due_input AS expected
                LEFT JOIN futures_minute AS fact
                  ON fact.contract_code = expected.contract_code
                 AND fact.bar_at > expected.session_start_at
                 AND fact.bar_at <= expected.session_end_at
                WHERE expected.bar_frequency = '1m'
                GROUP BY expected.block_id
                """
            ).fetchdf()
            actual_counts_df = pd.concat(
                [daily_counts_df, minute_counts_df],
                ignore_index=True,
            )
            actual_count_by_block = dict(
                zip(
                    actual_counts_df["block_id"].astype("int64"),
                    actual_counts_df["actual_key_count"].astype("int64"),
                    strict=True,
                )
            )
            expected_due_df["actual_key_count"] = expected_due_df[
                "block_id"
            ].map(actual_count_by_block).fillna(0).astype("int64")

            observations_df = database_connection.execute(
                """
                SELECT
                    contract_code,
                    bar_frequency,
                    trading_date,
                    session_number,
                    session_start_at,
                    session_end_at
                FROM fetch_observation
                """
            ).fetchdf()

        expected_due_df["expected_key_count"] = expected_due_df[
            "expected_bar_count"
        ].astype("int64")
        if (
            expected_due_df["actual_key_count"]
            > expected_due_df["expected_key_count"]
        ).any():
            invalid_blocks = expected_due_df.loc[
                expected_due_df["actual_key_count"]
                > expected_due_df["expected_key_count"],
                [
                    "contract_code",
                    "bar_frequency",
                    "trading_date",
                    "session_number",
                    "expected_key_count",
                    "actual_key_count",
                ],
            ]
            raise RuntimeError(
                "项目库在当前理论块内的行情数超过理论数：\n"
                + invalid_blocks.head(20).to_string(index=False)
            )

        daily_observed_keys: set[tuple[str, datetime.date]] = set()
        minute_observed_intervals: dict[
            tuple[str, datetime.date],
            list[tuple[pd.Timestamp, pd.Timestamp]],
        ] = defaultdict(list)
        for observation in observations_df.itertuples(index=False):
            observation_date = pd.Timestamp(observation.trading_date).date()
            if observation.bar_frequency == "1d":
                daily_observed_keys.add(
                    (str(observation.contract_code), observation_date)
                )
            elif observation.bar_frequency == "1m":
                minute_observed_intervals[
                    (str(observation.contract_code), observation_date)
                ].append(
                    (
                        _market_timestamp(observation.session_start_at),
                        _market_timestamp(observation.session_end_at),
                    )
                )

        for observation_key, intervals in minute_observed_intervals.items():
            merged_intervals: list[tuple[pd.Timestamp, pd.Timestamp]] = []
            for interval_start, interval_end in sorted(intervals):
                if interval_end <= interval_start:
                    raise RuntimeError(
                        f"项目库观察区间反向或为空：{observation_key} "
                        f"({interval_start}, {interval_end}]"
                    )
                if (
                    merged_intervals
                    and interval_start <= merged_intervals[-1][1]
                ):
                    previous_start, previous_end = merged_intervals[-1]
                    merged_intervals[-1] = (
                        previous_start,
                        max(previous_end, interval_end),
                    )
                else:
                    merged_intervals.append((interval_start, interval_end))
            minute_observed_intervals[observation_key] = merged_intervals

        observed_expected_key_counts: list[int] = []
        partial_coverage_block_ids: set[int] = set()
        for row in expected_due_df.itertuples(index=False):
            if row.bar_frequency == "1d":
                observed_count = int(
                    (str(row.contract_code), pd.Timestamp(row.trading_date).date())
                    in daily_observed_keys
                )
            else:
                current_start = _market_timestamp(row.session_start_at)
                current_end = _market_timestamp(row.session_end_at)
                observed_count = 0
                for observed_start, observed_end in minute_observed_intervals.get(
                    (
                        str(row.contract_code),
                        pd.Timestamp(row.trading_date).date(),
                    ),
                    [],
                ):
                    intersection_start = max(current_start, observed_start)
                    intersection_end = min(current_end, observed_end)
                    if intersection_end > intersection_start:
                        observed_count += int(
                            (intersection_end - intersection_start)
                            / pd.Timedelta(minutes=1)
                        )
                observed_count = min(observed_count, int(row.expected_key_count))
                if 0 < observed_count < int(row.expected_key_count):
                    partial_coverage_block_ids.add(int(row.block_id))
            observed_expected_key_counts.append(observed_count)
        expected_due_df["observed_expected_key_count"] = (
            observed_expected_key_counts
        )

        expected_due_df["data_missing_key_count"] = (
            expected_due_df["expected_key_count"]
            - expected_due_df["actual_key_count"]
        )

        detail_block_ids = set(
            expected_due_df.loc[
                expected_due_df["bar_frequency"].eq("1m")
                & expected_due_df["data_missing_key_count"].gt(0)
                & expected_due_df["actual_key_count"].gt(0),
                "block_id",
            ].astype("int64")
        ) | partial_coverage_block_ids
        actual_minute_keys_by_block: dict[int, set[int]] = defaultdict(set)
        if detail_block_ids:
            if database_connection is None:
                raise RuntimeError("不存在项目数据库时不应出现需复核的分钟事实键。")
            detail_blocks_df = expected_due_df.loc[
                expected_due_df["block_id"].isin(detail_block_ids),
                [
                    "block_id",
                    "contract_code",
                    "trading_date",
                    "session_number",
                    "session_start_at",
                    "session_end_at",
                ],
            ]
            database_connection.register(
                "detail_blocks_input",
                pa.Table.from_pandas(detail_blocks_df, preserve_index=False),
            )
            actual_minute_keys_df = database_connection.execute(
                """
                SELECT expected.block_id, fact.bar_at
                FROM detail_blocks_input AS expected
                JOIN futures_minute AS fact
                  ON fact.contract_code = expected.contract_code
                 AND fact.bar_at > expected.session_start_at
                 AND fact.bar_at <= expected.session_end_at
                ORDER BY expected.block_id, fact.bar_at
                """
            ).fetchdf()
            for fact_key in actual_minute_keys_df.itertuples(index=False):
                actual_minute_keys_by_block[int(fact_key.block_id)].add(
                    _market_timestamp(fact_key.bar_at).value
                )

        source_confirmed_missing_key_counts = [0] * len(expected_due_df)
        fetch_pending_key_counts = [0] * len(expected_due_df)
        minute_missing_range_records: list[dict[str, object]] = []

        for row in expected_due_df.itertuples(index=False):
            missing_count = int(row.data_missing_key_count)
            if missing_count == 0:
                continue

            observed_count = int(row.observed_expected_key_count)
            if row.bar_frequency == "1d":
                confirmed_count = missing_count if observed_count == 1 else 0
                pending_count = missing_count - confirmed_count
            else:
                current_start = _market_timestamp(row.session_start_at)
                current_end = _market_timestamp(row.session_end_at)
                intervals = minute_observed_intervals.get(
                    (
                        str(row.contract_code),
                        pd.Timestamp(row.trading_date).date(),
                    ),
                    [],
                )
                actual_keys = actual_minute_keys_by_block.get(
                    int(row.block_id),
                    set(),
                )

                if int(row.actual_key_count) == 0 and observed_count in {
                    0,
                    int(row.expected_key_count),
                }:
                    missing_state = (
                        "source_confirmed_missing"
                        if observed_count == int(row.expected_key_count)
                        else "fetch_pending"
                    )
                    minute_missing_range_records.append(
                        {
                            "contract_code": str(row.contract_code),
                            "trading_date": pd.Timestamp(row.trading_date).date(),
                            "session_number": int(row.session_number),
                            "range_start_at": current_start
                            + pd.Timedelta(minutes=1),
                            "range_end_at": current_end,
                            "missing_key_count": missing_count,
                            "missing_state": missing_state,
                        }
                    )
                    confirmed_count = missing_count if observed_count else 0
                    pending_count = missing_count - confirmed_count
                else:
                    confirmed_count = 0
                    pending_count = 0
                    active_range: dict[str, object] | None = None
                    previous_missing_key: pd.Timestamp | None = None
                    expected_keys = pd.date_range(
                        start=current_start + pd.Timedelta(minutes=1),
                        end=current_end,
                        freq="min",
                    )
                    for expected_key in expected_keys:
                        if expected_key.value in actual_keys:
                            if active_range is not None:
                                minute_missing_range_records.append(active_range)
                                active_range = None
                            previous_missing_key = None
                            continue
                        is_observed = any(
                            expected_key > observed_start
                            and expected_key <= observed_end
                            for observed_start, observed_end in intervals
                        )
                        missing_state = (
                            "source_confirmed_missing"
                            if is_observed
                            else "fetch_pending"
                        )
                        if is_observed:
                            confirmed_count += 1
                        else:
                            pending_count += 1
                        if (
                            active_range is None
                            or active_range["missing_state"] != missing_state
                            or previous_missing_key is None
                            or expected_key
                            != previous_missing_key + pd.Timedelta(minutes=1)
                        ):
                            if active_range is not None:
                                minute_missing_range_records.append(active_range)
                            active_range = {
                                "contract_code": str(row.contract_code),
                                "trading_date": pd.Timestamp(
                                    row.trading_date
                                ).date(),
                                "session_number": int(row.session_number),
                                "range_start_at": expected_key,
                                "range_end_at": expected_key,
                                "missing_key_count": 1,
                                "missing_state": missing_state,
                            }
                        else:
                            active_range["range_end_at"] = expected_key
                            active_range["missing_key_count"] = int(
                                active_range["missing_key_count"]
                            ) + 1
                        previous_missing_key = expected_key
                    if active_range is not None:
                        minute_missing_range_records.append(active_range)

            source_confirmed_missing_key_counts[int(row.block_id)] = (
                confirmed_count
            )
            fetch_pending_key_counts[int(row.block_id)] = pending_count

        expected_due_df["source_confirmed_missing_key_count"] = (
            source_confirmed_missing_key_counts
        )
        expected_due_df["fetch_pending_key_count"] = fetch_pending_key_counts

        count_columns = [
            "actual_key_count",
            "expected_key_count",
            "observed_expected_key_count",
            "data_missing_key_count",
            "source_confirmed_missing_key_count",
            "fetch_pending_key_count",
        ]
        expected_due_df[count_columns] = expected_due_df[count_columns].astype(
            "int64"
        )
        if not (
            expected_due_df["source_confirmed_missing_key_count"]
            + expected_due_df["fetch_pending_key_count"]
        ).equals(expected_due_df["data_missing_key_count"]):
            raise RuntimeError("来源确认缺失与待拉取数量未能精确分解数据缺失。")

        expected_due_df["missing_state"] = "complete"
        expected_due_df.loc[
            expected_due_df["data_missing_key_count"].gt(0)
            & expected_due_df["fetch_pending_key_count"].eq(0),
            "missing_state",
        ] = "source_confirmed_missing"
        expected_due_df.loc[
            expected_due_df["fetch_pending_key_count"].gt(0)
            & expected_due_df["source_confirmed_missing_key_count"].eq(0),
            "missing_state",
        ] = "fetch_pending"
        expected_due_df.loc[
            expected_due_df["fetch_pending_key_count"].gt(0)
            & expected_due_df["source_confirmed_missing_key_count"].gt(0),
            "missing_state",
        ] = "mixed_missing"

        missing_blocks_df = expected_due_df.loc[
            expected_due_df["data_missing_key_count"].gt(0),
            [
                "bar_frequency",
                "contract_code",
                "underlying_code",
                "trading_date",
                "session_number",
                "session_start_at",
                "session_end_at",
                "expected_key_count",
                "actual_key_count",
                "observed_expected_key_count",
                "data_missing_key_count",
                "source_confirmed_missing_key_count",
                "fetch_pending_key_count",
                "missing_state",
            ],
        ].copy()
        minute_missing_ranges_df = pd.DataFrame.from_records(
            minute_missing_range_records,
            columns=[
                "contract_code",
                "trading_date",
                "session_number",
                "range_start_at",
                "range_end_at",
                "missing_key_count",
                "missing_state",
            ],
        )

        source_request_candidates: list[dict[str, object]] = []
        for (_, _), contract_blocks_df in expected_due_df.groupby(
            ["bar_frequency", "contract_code"],
            sort=True,
            observed=True,
        ):
            contract_blocks_df = contract_blocks_df.sort_values(
                "structure_ordinal",
                kind="stable",
            )
            current_block_ids: list[int] = []
            current_expected_key_count = 0
            previous_structure_ordinal: int | None = None

            for block in contract_blocks_df.itertuples(index=False):
                is_pending = int(block.fetch_pending_key_count) > 0
                if acceptance_coordinates is not None:
                    is_pending = is_pending and (
                        str(block.contract_code), pd.Timestamp(block.trading_date).date()
                    ) in acceptance_coordinates
                is_adjacent = (
                    previous_structure_ordinal is not None
                    and int(block.structure_ordinal)
                    == previous_structure_ordinal + 1
                )
                exceeds_source_limit = (
                    current_expected_key_count + int(block.expected_key_count)
                    > JOINQUANT_MAX_EXPECTED_KEYS_PER_SOURCE_REQUEST
                )
                if current_block_ids and (
                    not is_pending or not is_adjacent or exceeds_source_limit
                ):
                    source_request_candidates.append(
                        {
                            "block_ids": tuple(current_block_ids),
                            "expected_key_count": current_expected_key_count,
                        }
                    )
                    current_block_ids = []
                    current_expected_key_count = 0

                if is_pending:
                    if (
                        int(block.expected_key_count)
                        > JOINQUANT_MAX_EXPECTED_KEYS_PER_SOURCE_REQUEST
                    ):
                        raise RuntimeError(
                            "单个观察块已经超过来源请求理论键上限："
                            f"{block.contract_code} {block.trading_date} "
                            f"session={block.session_number}"
                        )
                    current_block_ids.append(int(block.block_id))
                    current_expected_key_count += int(block.expected_key_count)
                previous_structure_ordinal = int(block.structure_ordinal)

            if current_block_ids:
                source_request_candidates.append(
                    {
                        "block_ids": tuple(current_block_ids),
                        "expected_key_count": current_expected_key_count,
                    }
                )

        block_by_id_df = expected_due_df.set_index("block_id", drop=False)
        for candidate in source_request_candidates:
            candidate_blocks_df = block_by_id_df.loc[
                list(candidate["block_ids"])
            ]
            candidate["bar_frequency"] = str(
                candidate_blocks_df.iloc[0]["bar_frequency"]
            )
            candidate["contract_code"] = str(
                candidate_blocks_df.iloc[0]["contract_code"]
            )
            candidate["first_trading_date"] = pd.Timestamp(
                candidate_blocks_df.iloc[0]["trading_date"]
            ).date()
            candidate["last_trading_date"] = pd.Timestamp(
                candidate_blocks_df.iloc[-1]["trading_date"]
            ).date()
            candidate["first_structure_ordinal"] = int(
                candidate_blocks_df.iloc[0]["structure_ordinal"]
            )

        source_request_candidates.sort(
            key=lambda candidate: (
                frequency_rank[str(candidate["bar_frequency"])],
                -candidate["last_trading_date"].toordinal(),
                str(candidate["contract_code"]),
                -int(candidate["first_structure_ordinal"]),
            )
        )

        # 使用同一装包过程排完当前全部候选；第一包用于实际代码，后续包仅计数。
        # 这是当前快照、未来请求均成功时的预计轮数；失败或新增日期后每轮重新计算。
        remaining_candidates = list(source_request_candidates)
        selected_candidates: list[dict[str, object]] = []
        selected_expected_key_count = 0
        selected_observation_count = 0
        transfer_schedule_records = []
        while remaining_candidates:
            batch_candidates = []
            deferred_candidates = []
            batch_key_count = 0
            batch_observation_count = 0
            for candidate in remaining_candidates:
                observation_count = len(candidate["block_ids"])
                if (len(batch_candidates) >= JOINQUANT_MAX_SOURCE_REQUESTS_PER_TRANSFER
                        or batch_key_count + int(candidate["expected_key_count"])
                        > JOINQUANT_MAX_PLANNED_EXPECTED_KEYS_PER_TRANSFER
                        or batch_observation_count + observation_count > JOINQUANT_MAX_OBSERVATIONS_PER_TRANSFER):
                    deferred_candidates.append(candidate)
                    continue
                batch_candidates.append(candidate)
                batch_key_count += int(candidate["expected_key_count"])
                batch_observation_count += observation_count
            if not batch_candidates:
                raise RuntimeError("有待拉取块，但没有任何来源请求能进入当前有界文件。")
            if not transfer_schedule_records:
                selected_candidates = batch_candidates
                selected_expected_key_count = batch_key_count
                selected_observation_count = batch_observation_count
            transfer_schedule_records.append({
                "transfer_number": len(transfer_schedule_records) + 1,
                "source_request_count": len(batch_candidates),
                "observation_count": batch_observation_count,
                "expected_key_count": batch_key_count,
            })
            remaining_candidates = deferred_candidates
        transfer_schedule_df = pd.DataFrame.from_records(transfer_schedule_records, columns=[
            "transfer_number", "source_request_count", "observation_count", "expected_key_count",
        ])

        source_request_payloads: list[dict[str, object]] = []
        planned_observation_records: list[dict[str, object]] = []
        planned_source_request_records: list[dict[str, object]] = []
        request_ordinal = 0

        for source_request_ordinal, candidate in enumerate(
            selected_candidates,
            start=1,
        ):
            candidate_blocks_df = block_by_id_df.loc[
                list(candidate["block_ids"])
            ].sort_values(
                ["trading_date", "session_number"],
                kind="stable",
            )
            observation_payloads: list[dict[str, object]] = []
            for block in candidate_blocks_df.itertuples(index=False):
                request_ordinal += 1
                trading_date = pd.Timestamp(block.trading_date).date()
                if block.bar_frequency == "1d":
                    session_start_at = None
                    session_end_at = None
                    expected_keys = [trading_date.isoformat()]
                else:
                    session_start_at = _timestamp_text(block.session_start_at)
                    session_end_at = _timestamp_text(block.session_end_at)
                    expected_keys = [
                        _timestamp_text(expected_key)
                        for expected_key in pd.date_range(
                            start=_market_timestamp(block.session_start_at)
                            + pd.Timedelta(minutes=1),
                            end=_market_timestamp(block.session_end_at),
                            freq="min",
                        )
                    ]
                expected_keys_sha256 = _sha256(expected_keys)
                request_identity = {
                    "bar_frequency": str(block.bar_frequency),
                    "contract_code": str(block.contract_code),
                    "expected_key_count": int(block.expected_key_count),
                    "expected_keys_sha256": expected_keys_sha256,
                    "session_end_at": session_end_at,
                    "session_number": int(block.session_number),
                    "session_start_at": session_start_at,
                    "trading_date": trading_date.isoformat(),
                }
                request_id = _sha256(request_identity)
                observation_payload = {
                    "request_id": request_id,
                    "request_ordinal": request_ordinal,
                    **request_identity,
                }
                observation_payloads.append(observation_payload)

            first_observation = observation_payloads[0]
            last_observation = observation_payloads[-1]
            if candidate["bar_frequency"] == "1d":
                source_frequency = "daily"
                source_fields = list(JOINQUANT_DAILY_SOURCE_FIELDS)
                source_start_at = first_observation["trading_date"]
                source_end_at = last_observation["trading_date"]
            else:
                source_frequency = "1m"
                source_fields = list(JOINQUANT_MINUTE_SOURCE_FIELDS)
                first_block = candidate_blocks_df.iloc[0]
                source_start_at = _timestamp_text(
                    _market_timestamp(first_block["session_start_at"])
                    + pd.Timedelta(minutes=1)
                )
                source_end_at = str(last_observation["session_end_at"])

            source_parameters = dict(TRANSFER_SOURCE_PARAMETERS)
            source_request_identity = {
                "bar_frequency": str(candidate["bar_frequency"]),
                "contract_code": str(candidate["contract_code"]),
                "observation_request_ids": [
                    observation["request_id"]
                    for observation in observation_payloads
                ],
                "source_end_at": source_end_at,
                "source_fields": source_fields,
                "source_frequency": source_frequency,
                "source_parameters": source_parameters,
                "source_start_at": source_start_at,
            }
            source_request_id = _sha256(source_request_identity)
            source_request_payload = {
                "source_request_id": source_request_id,
                "source_request_ordinal": source_request_ordinal,
                **source_request_identity,
                "observations": observation_payloads,
            }
            source_request_payloads.append(source_request_payload)
            planned_source_request_records.append(
                {
                    "source_request_id": source_request_id,
                    "source_request_ordinal": source_request_ordinal,
                    "contract_code": str(candidate["contract_code"]),
                    "bar_frequency": str(candidate["bar_frequency"]),
                    "source_frequency": source_frequency,
                    "source_start_at": source_start_at,
                    "source_end_at": source_end_at,
                    "observation_count": len(observation_payloads),
                    "expected_key_count": int(candidate["expected_key_count"]),
                }
            )
            for observation_payload in observation_payloads:
                planned_observation_records.append(
                    {
                        "source_request_id": source_request_id,
                        "source_request_ordinal": source_request_ordinal,
                        **observation_payload,
                    }
                )

        plan_payload: dict[str, object] = {
            "format_id": TRANSFER_FORMAT_ID,
            "protocol_version": TRANSFER_PROTOCOL_VERSION,
            "generator_version": TRANSFER_GENERATOR_VERSION,
            "planner_version": FETCH_PLANNER_VERSION,
            "market_time_zone": TRANSFER_MARKET_TIME_ZONE,
            "source_requests": source_request_payloads,
        }
        plan_sha256 = _sha256(plan_payload)
        planned_observations_df = pd.DataFrame.from_records(
            planned_observation_records,
            columns=[
                "source_request_id",
                "source_request_ordinal",
                "request_id",
                "request_ordinal",
                "bar_frequency",
                "contract_code",
                "expected_key_count",
                "expected_keys_sha256",
                "session_end_at",
                "session_number",
                "session_start_at",
                "trading_date",
            ],
        )
        planned_source_requests_df = pd.DataFrame.from_records(
            planned_source_request_records,
            columns=[
                "source_request_id",
                "source_request_ordinal",
                "contract_code",
                "bar_frequency",
                "source_frequency",
                "source_start_at",
                "source_end_at",
                "observation_count",
                "expected_key_count",
            ],
        )

        planned_key_count_by_frequency = defaultdict(int)
        planned_observation_count_by_frequency = defaultdict(int)
        for record in planned_observation_records:
            planned_key_count_by_frequency[record["bar_frequency"]] += int(
                record["expected_key_count"]
            )
            planned_observation_count_by_frequency[
                record["bar_frequency"]
            ] += 1

        summary_records: list[dict[str, object]] = []
        for bar_frequency in FINANCIAL_FUTURES_BAR_FREQUENCIES:
            frequency_blocks_df = expected_due_df.loc[
                expected_due_df["bar_frequency"].eq(bar_frequency)
            ]
            summary_records.append(
                {
                    "bar_frequency": bar_frequency,
                    "expected_due_block_count": len(frequency_blocks_df),
                    "expected_due_key_count": int(
                        frequency_blocks_df["expected_key_count"].sum()
                    ),
                    "present_key_count": int(
                        frequency_blocks_df["actual_key_count"].sum()
                    ),
                    "data_missing_key_count": int(
                        frequency_blocks_df["data_missing_key_count"].sum()
                    ),
                    "source_confirmed_missing_key_count": int(
                        frequency_blocks_df[
                            "source_confirmed_missing_key_count"
                        ].sum()
                    ),
                    "fetch_pending_key_count": int(
                        frequency_blocks_df["fetch_pending_key_count"].sum()
                    ),
                    "not_yet_fetchable_block_count": int(
                        not_yet_fetchable_blocks_df["bar_frequency"]
                        .eq(bar_frequency)
                        .sum()
                    ),
                    "planned_observation_count": int(
                        planned_observation_count_by_frequency[bar_frequency]
                    ),
                    "planned_expected_key_count": int(
                        planned_key_count_by_frequency[bar_frequency]
                    ),
                }
            )
        coverage_summary_df = pd.DataFrame.from_records(summary_records)

        if len(selected_candidates) > JOINQUANT_MAX_SOURCE_REQUESTS_PER_TRANSFER:
            raise RuntimeError("当前计划超过单文件来源请求数上限。")
        if selected_observation_count > JOINQUANT_MAX_OBSERVATIONS_PER_TRANSFER:
            raise RuntimeError("当前计划超过单文件观察块上限。")
        if selected_expected_key_count > JOINQUANT_MAX_PLANNED_EXPECTED_KEYS_PER_TRANSFER:
            raise RuntimeError("当前计划超过正常规划的内存安全理论键上限。")
        if selected_expected_key_count > JOINQUANT_MAX_EXPECTED_KEYS_PER_TRANSFER:
            raise RuntimeError("当前计划超过传输协议理论键硬上限。")
        if any(
            int(candidate["expected_key_count"])
            > JOINQUANT_MAX_EXPECTED_KEYS_PER_SOURCE_REQUEST
            for candidate in selected_candidates
        ):
            raise RuntimeError("当前计划存在超过理论键上限的来源请求。")

        return FinancialFuturesFetchPlan(
            as_of=as_of,
            latest_due_trading_date=latest_due_trading_date,
            database_exists=database_exists,
            coverage_summary_df=coverage_summary_df,
            not_yet_fetchable_blocks_df=not_yet_fetchable_blocks_df,
            missing_blocks_df=missing_blocks_df,
            minute_missing_ranges_df=minute_missing_ranges_df,
            planned_observations_df=planned_observations_df,
            planned_source_requests_df=planned_source_requests_df,
            plan_payload=plan_payload,
            plan_sha256=plan_sha256,
            remaining_transfer_count=len(transfer_schedule_records),
            remaining_source_request_count=len(source_request_candidates),
            transfer_schedule_df=transfer_schedule_df,
            calendar_coverage_df=calendar_coverage_df,
            calendar_is_current=calendar_is_current,
            trade_calendar_through=trade_calendar_through,
            latest_due_market_trading_date=latest_due_market_trading_date,
            next_run_at=next_run_at,
        )
    finally:
        if database_connection is not None:
            database_connection.close()
