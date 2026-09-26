"""分钟行情按状态 Session 与品种年月分区拉取的共享逻辑。"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType

import pandas as pd
import pyarrow as pa

from config.data_contracts import FUTURES_MINUTE_SCHEMA, pandas_to_arrow
from c00_futures_fetch_control import fetch_status_rows, mark_fetch_completed
from c00_futures_universe import select_minute_varieties
from c00_lakehouse import hive_partitioning, replace_partition


MINUTE_PARTITION_FIELDS = ["exchange_code", "underlying_code", "year", "month"]
MINUTE_PARTITIONING = hive_partitioning(
    [
        pa.field("exchange_code", pa.string()),
        pa.field("underlying_code", pa.string()),
        pa.field("year", pa.int16()),
        pa.field("month", pa.int8()),
    ]
)
PRICE_FIELDS = ["open", "high", "low", "close", "volume", "money", "open_interest"]
CHINA_TZ = "Asia/Shanghai"
CHINA_NS_DTYPE = pd.DatetimeTZDtype(unit="ns", tz=CHINA_TZ)
DEFAULT_QUOTA_RESERVE = 5_000_000
MAX_REQUEST_EXPECTED_ROWS = 900_000


@dataclass(frozen=True)
class MinuteFetchSummary:
    fetch_run_id: str
    session_count: int
    partition_count: int
    expected_row_count: int
    written_row_count: int
    completed_session_count: int
    completed_partition_count: int
    selected_varieties: pd.DataFrame
    is_dry_run: bool
    stop_reason: str | None


class MinuteQuotaInsufficientError(RuntimeError):
    """当前日配额不足以完整执行下一个原子分区。"""


def filter_target_sessions(
    session_df: pd.DataFrame,
    targets: tuple[str, ...] = (),
) -> pd.DataFrame:
    """按显式的 `EXCHANGE.UNDERLYING` 目标缩小本次人工拉取范围。"""
    if not targets or session_df.empty:
        return session_df
    target_keys: set[tuple[str, str]] = set()
    for target in targets:
        parts = target.upper().split(".", maxsplit=1)
        if len(parts) != 2 or not all(parts):
            raise ValueError(
                f"分钟目标格式错误：{target!r}；应使用 EXCHANGE.UNDERLYING。"
            )
        target_keys.add((parts[0], parts[1]))
    row_keys = pd.MultiIndex.from_frame(
        session_df[["exchange_code", "underlying_code"]]
    )
    selected_df = session_df[row_keys.isin(target_keys)].copy()
    selected_keys = set(
        selected_df[["exchange_code", "underlying_code"]]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    )
    unknown_targets = target_keys - selected_keys
    if unknown_targets:
        raise ValueError(
            "目标在当前未完成且应拉取的分钟状态中不存在："
            f"{sorted(unknown_targets)}"
        )
    return selected_df


def _quota_spare(jqdata: ModuleType) -> int | None:
    get_query_count = getattr(jqdata, "get_query_count", None)
    if get_query_count is None:
        return None
    quota = get_query_count()
    if isinstance(quota, dict) and quota.get("spare") is not None:
        return int(quota["spare"])
    return None


def fetch_partition_bars(
    session_df: pd.DataFrame,
    jqdata: ModuleType,
    quota_reserve: int = DEFAULT_QUOTA_RESERVE,
) -> pd.DataFrame:
    expected_rows = int(session_df["expected_bar_count"].sum())
    if expected_rows > MAX_REQUEST_EXPECTED_ROWS:
        raise ValueError(
            "单次分钟请求的理论行数超过安全边界："
            f"预期 {expected_rows}，边界 {MAX_REQUEST_EXPECTED_ROWS}。"
        )
    spare = _quota_spare(jqdata)
    if spare is not None and spare - expected_rows < quota_reserve:
        raise MinuteQuotaInsufficientError(
            "JQData 日配额不足："
            f"剩余 {spare}，本分区预期 {expected_rows}，要求保留 {quota_reserve}。"
        )
    contract_codes = sorted(session_df["contract_code"].unique())
    raw_df = jqdata.get_price(
        contract_codes,
        start_date=pd.Timestamp(session_df["session_start_at"].min()).tz_localize(None),
        end_date=pd.Timestamp(session_df["session_end_at"].max()).tz_localize(None),
        frequency="1m",
        fields=PRICE_FIELDS,
        skip_paused=True,
        fq=None,
        panel=False,
    )
    if len(raw_df) >= 1_000_000:
        raise RuntimeError(f"单次分钟 get_price 返回 {len(raw_df)} 行，达到接口安全边界。")
    if raw_df.empty:
        return pd.DataFrame(columns=FUTURES_MINUTE_SCHEMA.names)
    if "time" not in raw_df.columns:
        raw_df = raw_df.rename_axis("time").reset_index()
    if "code" not in raw_df.columns:
        if len(contract_codes) != 1:
            raise ValueError("多合约分钟 get_price 返回缺少 code 字段。")
        raw_df["code"] = contract_codes[0]
    raw_df = raw_df.rename(columns={"code": "contract_code", "time": "bar_at"})
    raw_df["bar_at"] = pd.to_datetime(raw_df["bar_at"])
    if raw_df["bar_at"].dt.tz is None:
        raw_df["bar_at"] = raw_df["bar_at"].dt.tz_localize(CHINA_TZ)
    else:
        raw_df["bar_at"] = raw_df["bar_at"].dt.tz_convert(CHINA_TZ)
    raw_df["bar_at"] = raw_df["bar_at"].astype(CHINA_NS_DTYPE)

    frames: list[pd.DataFrame] = []
    for contract_code, contract_bars_df in raw_df.groupby("contract_code"):
        rules_df = session_df[session_df["contract_code"] == contract_code].sort_values(
            "session_end_at"
        ).copy()
        for column in ["session_start_at", "session_end_at"]:
            rules_df[column] = (
                pd.to_datetime(rules_df[column], utc=True)
                .dt.tz_convert(CHINA_TZ)
                .astype(CHINA_NS_DTYPE)
            )
        contract_bars_df = contract_bars_df.sort_values("bar_at")
        aligned_df = pd.merge_asof(
            contract_bars_df,
            rules_df[
                ["session_start_at", "session_end_at", "trading_date", "session_number"]
            ],
            left_on="bar_at",
            right_on="session_end_at",
            direction="forward",
        )
        aligned_df = aligned_df[
            (aligned_df["bar_at"] > aligned_df["session_start_at"])
            & (aligned_df["bar_at"] <= aligned_df["session_end_at"])
        ]
        frames.append(aligned_df)
    if not frames:
        return pd.DataFrame(columns=FUTURES_MINUTE_SCHEMA.names)

    minute_df = pd.concat(frames, ignore_index=True)
    minute_df["exchange_code"] = session_df["exchange_code"].iloc[0]
    minute_df["underlying_code"] = session_df["underlying_code"].iloc[0]
    minute_df["source"] = "JQData_get_price_1m_skip_paused"
    minute_df["updated_at"] = datetime.now(timezone.utc).replace(microsecond=0)
    minute_df["year"] = minute_df["trading_date"].map(lambda value: value.year)
    minute_df["month"] = minute_df["trading_date"].map(lambda value: value.month)
    return minute_df[FUTURES_MINUTE_SCHEMA.names].drop_duplicates(
        ["contract_code", "bar_at"], keep="last"
    ).sort_values(["contract_code", "bar_at"]).reset_index(drop=True)


def update_futures_minute(
    status_path: Path,
    table_path: Path,
    jqdata: ModuleType | None,
    full_refresh: bool = False,
    dry_run: bool = False,
    quota_reserve: int = DEFAULT_QUOTA_RESERVE,
    targets: tuple[str, ...] = (),
) -> MinuteFetchSummary:
    """逐品种年月分区执行分钟拉取；每个成功分区立即记录 Session 完成状态。"""
    session_df = fetch_status_rows(status_path, "1m", include_completed=full_refresh)
    session_df = filter_target_sessions(session_df, targets)
    selected_input_df = session_df[["exchange_code", "underlying_code"]].drop_duplicates()
    selected_df = (
        select_minute_varieties(selected_input_df)
        if not selected_input_df.empty
        else pd.DataFrame(columns=["exchange_code", "underlying_code", "selection_reason"])
    )
    partition_count = session_df.groupby(MINUTE_PARTITION_FIELDS).ngroups
    expected_row_count = int(session_df["expected_bar_count"].sum())
    fetch_run_id = f"minute-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    if dry_run or session_df.empty:
        return MinuteFetchSummary(
            fetch_run_id=fetch_run_id,
            session_count=len(session_df),
            partition_count=partition_count,
            expected_row_count=expected_row_count,
            written_row_count=0,
            completed_session_count=0,
            completed_partition_count=0,
            selected_varieties=selected_df,
            is_dry_run=dry_run,
            stop_reason=None,
        )
    if jqdata is None:
        raise ValueError("正式拉取必须提供已认证的 JQData 模块。")

    written_row_count = 0
    completed_session_count = 0
    completed_partition_count = 0
    stop_reason: str | None = None
    for partition, partition_df in session_df.groupby(MINUTE_PARTITION_FIELDS, sort=True):
        try:
            minute_df = fetch_partition_bars(partition_df, jqdata, quota_reserve)
        except MinuteQuotaInsufficientError as error:
            stop_reason = str(error)
            break
        partition_values = dict(zip(MINUTE_PARTITION_FIELDS, partition, strict=True))
        replace_partition(
            pandas_to_arrow(minute_df, FUTURES_MINUTE_SCHEMA),
            table_path,
            MINUTE_PARTITIONING,
            MINUTE_PARTITION_FIELDS,
            FUTURES_MINUTE_SCHEMA,
            partition_values=partition_values,
        )
        written_row_count += len(minute_df)
        completed_session_count += mark_fetch_completed(
            status_path,
            "1m",
            partition_df[
                [
                    "contract_code",
                    "exchange_code",
                    "trading_date",
                    "session_number",
                    "year",
                    "month",
                ]
            ],
            fetch_run_id,
        )
        completed_partition_count += 1
    return MinuteFetchSummary(
        fetch_run_id=fetch_run_id,
        session_count=len(session_df),
        partition_count=partition_count,
        expected_row_count=expected_row_count,
        written_row_count=written_row_count,
        completed_session_count=completed_session_count,
        completed_partition_count=completed_partition_count,
        selected_varieties=selected_df,
        is_dry_run=False,
        stop_reason=stop_reason,
    )
