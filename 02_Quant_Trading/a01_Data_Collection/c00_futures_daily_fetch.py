"""日线行情的限额感知、分区拉取与安全写入逻辑。"""

from __future__ import annotations

import shutil
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

from config.data_contracts import FUTURES_DAILY_SCHEMA, pandas_to_arrow
from c00_futures_fetch_control import fetch_status_rows, mark_fetch_completed
from c00_lakehouse import (
    hive_partitioning,
    replace_partition,
    swap_staged_dataset,
    validate_dataset_streaming,
)


DAILY_PARTITION_FIELDS = ["exchange_code", "year", "month"]
DAILY_PARTITIONING = hive_partitioning(
    [
        pa.field("exchange_code", pa.string()),
        pa.field("year", pa.int16()),
        pa.field("month", pa.int8()),
    ]
)
PRICE_FIELDS = ["open", "high", "low", "close", "volume", "money", "open_interest"]
STATUS_KEY_COLUMNS = [
    "contract_code",
    "exchange_code",
    "underlying_code",
    "trading_date",
    "session_number",
    "year",
    "month",
]
DEFAULT_MAX_REQUEST_ROWS = 90_000
DEFAULT_MAX_CONTRACTS = 200
DEFAULT_QUOTA_RESERVE = 5_000_000


@dataclass(frozen=True)
class DailyFetchSummary:
    fetch_run_id: str
    expected_row_count: int
    request_count: int
    returned_row_count: int
    written_row_count: int
    completed_row_count: int
    is_dry_run: bool


def request_code_batches(
    keys_df: pd.DataFrame,
    max_request_rows: int = DEFAULT_MAX_REQUEST_ROWS,
    max_contracts: int = DEFAULT_MAX_CONTRACTS,
) -> list[list[str]]:
    """按每个合约的预期活跃交易日数构造保守请求批次。"""
    if max_request_rows <= 0 or max_contracts <= 0:
        raise ValueError("请求行数和合约数上限必须为正数。")
    expected_counts = (
        keys_df.groupby("contract_code").size().sort_index().astype(int).to_dict()
    )
    batches: list[list[str]] = []
    current_batch: list[str] = []
    current_rows = 0
    for contract_code, expected_count in expected_counts.items():
        if expected_count > max_request_rows:
            raise ValueError(
                f"单个合约 {contract_code} 的预期行数 {expected_count} 超过请求上限。"
            )
        should_flush = current_batch and (
            current_rows + expected_count > max_request_rows
            or len(current_batch) >= max_contracts
        )
        if should_flush:
            batches.append(current_batch)
            current_batch = []
            current_rows = 0
        current_batch.append(contract_code)
        current_rows += expected_count
    if current_batch:
        batches.append(current_batch)
    return batches


def planned_request_count(keys_df: pd.DataFrame) -> int:
    return sum(
        len(request_code_batches(group_df))
        for _, group_df in keys_df.groupby(["exchange_code", "year"], sort=True)
    )


def _quota_spare(jqdata: ModuleType) -> int | None:
    get_query_count = getattr(jqdata, "get_query_count", None)
    if get_query_count is None:
        return None
    quota = get_query_count()
    if isinstance(quota, dict) and quota.get("spare") is not None:
        return int(quota["spare"])
    return None


def _ensure_quota(
    jqdata: ModuleType,
    expected_rows: int,
    quota_reserve: int,
) -> None:
    spare = _quota_spare(jqdata)
    if spare is not None and spare - expected_rows < quota_reserve:
        raise RuntimeError(
            "JQData 日配额不足："
            f"剩余 {spare}，本批预期 {expected_rows}，要求保留 {quota_reserve}。"
        )


def fetch_daily_group(
    keys_df: pd.DataFrame,
    jqdata: ModuleType,
    quota_reserve: int = DEFAULT_QUOTA_RESERVE,
) -> tuple[pd.DataFrame, int, int]:
    """拉取单一交易所/年份，返回规范化行情、请求数与 API 返回行数。"""
    frames: list[pd.DataFrame] = []
    request_count = 0
    returned_row_count = 0
    for batch in request_code_batches(keys_df):
        batch_keys_df = keys_df[keys_df["contract_code"].isin(batch)]
        _ensure_quota(jqdata, len(batch_keys_df), quota_reserve)
        raw_df = jqdata.get_price(
            batch,
            start_date=batch_keys_df["trading_date"].min(),
            end_date=batch_keys_df["trading_date"].max(),
            frequency="daily",
            fields=PRICE_FIELDS,
            skip_paused=True,
            fq=None,
            panel=False,
        )
        request_count += 1
        returned_row_count += len(raw_df)
        if len(raw_df) >= 1_000_000:
            raise RuntimeError(f"单次 get_price 返回 {len(raw_df)} 行，达到接口安全边界。")
        if raw_df.empty:
            continue
        if "time" not in raw_df.columns:
            raw_df = raw_df.rename_axis("time").reset_index()
        if "code" not in raw_df.columns:
            if len(batch) != 1:
                raise ValueError("多合约 get_price 返回缺少 code 字段。")
            raw_df["code"] = batch[0]
        raw_df = raw_df.rename(columns={"code": "contract_code"})
        raw_df["trading_date"] = pd.to_datetime(raw_df["time"]).dt.date
        raw_df = raw_df[["contract_code", "trading_date", *PRICE_FIELDS]]
        expected_keys_df = batch_keys_df[["contract_code", "trading_date"]].drop_duplicates()
        raw_df = raw_df.merge(
            expected_keys_df,
            on=["contract_code", "trading_date"],
            how="inner",
            validate="many_to_one",
        )
        frames.append(raw_df)
    if not frames:
        return (
            pd.DataFrame(columns=["contract_code", "trading_date", *PRICE_FIELDS]),
            request_count,
            returned_row_count,
        )
    price_df = pd.concat(frames, ignore_index=True)
    return (
        price_df.drop_duplicates(["contract_code", "trading_date"], keep="last"),
        request_count,
        returned_row_count,
    )


def build_daily_rows(
    keys_df: pd.DataFrame,
    price_df: pd.DataFrame,
    updated_at: datetime,
) -> pd.DataFrame:
    daily_df = keys_df[
        ["contract_code", "exchange_code", "underlying_code", "trading_date"]
    ].merge(
        price_df,
        on=["contract_code", "trading_date"],
        how="left",
        validate="one_to_one",
    )
    daily_df["has_market_data"] = daily_df["close"].notna()
    daily_df["source"] = "JQData_get_price_1d_skip_paused"
    daily_df["updated_at"] = updated_at
    daily_df["year"] = daily_df["trading_date"].map(lambda value: value.year)
    daily_df["month"] = daily_df["trading_date"].map(lambda value: value.month)
    return daily_df[FUTURES_DAILY_SCHEMA.names].sort_values(
        ["trading_date", "exchange_code", "underlying_code", "contract_code"]
    ).reset_index(drop=True)


def _read_daily_partition(
    table_path: Path,
    exchange_code: str,
    year: int,
    month: int,
) -> pd.DataFrame:
    if not table_path.exists():
        return pd.DataFrame(columns=FUTURES_DAILY_SCHEMA.names)
    dataset = ds.dataset(table_path, format="parquet", partitioning=DAILY_PARTITIONING)
    table = dataset.to_table(
        columns=FUTURES_DAILY_SCHEMA.names,
        filter=(
            (ds.field("exchange_code") == exchange_code)
            & (ds.field("year") == int(year))
            & (ds.field("month") == int(month))
        ),
    )
    return table.to_pandas()


def update_futures_daily(
    status_path: Path,
    table_path: Path,
    jqdata: ModuleType | None,
    full_refresh: bool = False,
    dry_run: bool = False,
    quota_reserve: int = DEFAULT_QUOTA_RESERVE,
) -> DailyFetchSummary:
    """以状态表为唯一任务清单拉取日线；全量模式使用旁路数据集。"""
    keys_df = fetch_status_rows(
        status_path, "1d", include_completed=full_refresh
    )
    fetch_run_id = f"daily-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    request_count = planned_request_count(keys_df) if not keys_df.empty else 0
    if dry_run or keys_df.empty:
        return DailyFetchSummary(
            fetch_run_id=fetch_run_id,
            expected_row_count=len(keys_df),
            request_count=request_count,
            returned_row_count=0,
            written_row_count=0,
            completed_row_count=0,
            is_dry_run=dry_run,
        )
    if jqdata is None:
        raise ValueError("正式拉取必须提供已认证的 JQData 模块。")

    updated_at = datetime.now(timezone.utc).replace(microsecond=0)
    actual_request_count = 0
    returned_row_count = 0
    written_row_count = 0
    staging_path = table_path.with_name(f".{table_path.name}.{uuid.uuid4().hex}.tmp")
    try:
        for _, group_df in keys_df.groupby(["exchange_code", "year"], sort=True):
            price_df, group_request_count, group_returned_count = fetch_daily_group(
                group_df, jqdata, quota_reserve
            )
            daily_df = build_daily_rows(group_df, price_df, updated_at)
            actual_request_count += group_request_count
            returned_row_count += group_returned_count
            written_row_count += len(daily_df)
            if full_refresh:
                ds.write_dataset(
                    pandas_to_arrow(daily_df, FUTURES_DAILY_SCHEMA),
                    staging_path,
                    format="parquet",
                    partitioning=DAILY_PARTITIONING,
                    basename_template=f"part-{uuid.uuid4().hex}-{{i}}.parquet",
                    existing_data_behavior="overwrite_or_ignore",
                )
            else:
                for partition, new_partition_df in daily_df.groupby(
                    DAILY_PARTITION_FIELDS, sort=True
                ):
                    exchange_code, year, month = partition
                    existing_df = _read_daily_partition(
                        table_path, str(exchange_code), int(year), int(month)
                    )
                    combined_df = pd.concat([existing_df, new_partition_df], ignore_index=True)
                    combined_df = combined_df.drop_duplicates(
                        ["contract_code", "trading_date"], keep="last"
                    )[FUTURES_DAILY_SCHEMA.names]
                    replace_partition(
                        pandas_to_arrow(combined_df, FUTURES_DAILY_SCHEMA),
                        table_path,
                        DAILY_PARTITIONING,
                        DAILY_PARTITION_FIELDS,
                        FUTURES_DAILY_SCHEMA,
                    )
            if not full_refresh:
                mark_fetch_completed(
                    status_path,
                    "1d",
                    group_df[STATUS_KEY_COLUMNS],
                    fetch_run_id,
                    updated_at,
                )

        if full_refresh:
            validate_dataset_streaming(
                staging_path,
                DAILY_PARTITIONING,
                FUTURES_DAILY_SCHEMA,
                expected_row_count=len(keys_df),
            )
            swap_staged_dataset(staging_path, table_path)
            completed_row_count = mark_fetch_completed(
                status_path,
                "1d",
                keys_df[STATUS_KEY_COLUMNS],
                fetch_run_id,
                updated_at,
            )
        else:
            completed_row_count = len(keys_df)
    finally:
        if staging_path.exists():
            shutil.rmtree(staging_path)

    return DailyFetchSummary(
        fetch_run_id=fetch_run_id,
        expected_row_count=len(keys_df),
        request_count=actual_request_count,
        returned_row_count=returned_row_count,
        written_row_count=written_row_count,
        completed_row_count=completed_row_count,
        is_dry_run=False,
    )

