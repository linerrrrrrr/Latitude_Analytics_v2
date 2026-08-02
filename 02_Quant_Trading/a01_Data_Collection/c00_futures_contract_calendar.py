"""合约 Session 日历的分区流式构建逻辑。"""

from __future__ import annotations

import re
import shutil
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from types import ModuleType
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

from config.data_contracts import (
    FUTURES_CONTRACT_CALENDAR_SCHEMA,
    FUTURES_VARIETY_CALENDAR_SCHEMA,
    pandas_to_arrow,
)
from c00_lakehouse import (
    hive_partitioning,
    read_dataset,
    swap_staged_dataset,
    validate_dataset_streaming,
)


PARTITIONING = hive_partitioning(
    [
        pa.field("exchange_code", pa.string()),
        pa.field("year", pa.int16()),
        pa.field("month", pa.int8()),
    ]
)
CHINA_TZ = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True)
class ContractCalendarBuildSummary:
    row_count: int
    contract_count: int
    no_trade_time_contract_day_count: int
    first_date: date
    last_date: date


def fixed_contract(code: str) -> bool:
    stem = code.rsplit(".", 1)[0]
    return bool(re.fullmatch(r"[A-Za-z]+[0-9]+", stem)) and not stem.endswith(
        ("8888", "9999")
    )


def underlying(code: str) -> str:
    match = re.match(r"[A-Za-z]+", code)
    if match is None:
        raise ValueError(f"无法识别合约品种：{code}")
    return match.group().upper()


def active_rule(
    rules: list[list[str]], contract: str, trading_date: date
) -> list[str] | None:
    matches = [
        rule
        for rule in rules
        if date.fromisoformat(rule[0]) <= trading_date <= date.fromisoformat(rule[1])
    ]
    if not matches:
        return None
    if len(matches) > 1:
        raise ValueError(
            f"Expected one trade-time rule: {contract} {trading_date}: {matches}"
        )
    return matches[0]


def _previous_trading_dates(
    trading_dates: list[date],
    jqdata: ModuleType,
) -> dict[date, date]:
    previous_dates = {
        current: previous
        for previous, current in zip(trading_dates[:-1], trading_dates[1:], strict=True)
    }
    first_date = trading_dates[0]
    prior_dates = jqdata.get_trade_days(end_date=first_date, count=2)
    if len(prior_dates) != 2:
        raise ValueError(f"无法确定首个交易日 {first_date} 的前一交易日。")
    previous_dates[first_date] = pd.Timestamp(prior_dates[0]).date()
    return previous_dates


def rebuild_contract_calendar(
    variety_path: Path,
    table_path: Path,
    jqdata: ModuleType,
) -> ContractCalendarBuildSummary:
    """以交易所/年/月为工作单元重建全量合约 Session 日历。"""
    variety_df = read_dataset(
        variety_path,
        PARTITIONING,
        FUTURES_VARIETY_CALENDAR_SCHEMA,
    )
    if variety_df.empty:
        raise ValueError(f"品种日历为空：{variety_path}")

    securities_df = (
        jqdata.get_all_securities(["futures"])
        .rename_axis("contract_code")
        .reset_index()
    )
    securities_df = securities_df[
        securities_df["contract_code"].map(fixed_contract)
    ].copy()
    securities_df["underlying_code"] = securities_df["contract_code"].map(underlying)
    securities_df["exchange_code"] = securities_df["contract_code"].str.rsplit(
        ".", n=1
    ).str[-1]
    securities_df["list_date"] = securities_df["start_date"].dt.date
    securities_df["delist_date"] = securities_df["end_date"].dt.date
    needed_pairs = set(
        zip(variety_df["underlying_code"], variety_df["exchange_code"], strict=False)
    )
    first_date = min(variety_df["trading_date"])
    last_date = max(variety_df["trading_date"])
    pair_mask = pd.Series(
        [
            (underlying_code, exchange_code) in needed_pairs
            for underlying_code, exchange_code in zip(
                securities_df["underlying_code"],
                securities_df["exchange_code"],
                strict=True,
            )
        ],
        index=securities_df.index,
    )
    securities_df = securities_df[
        pair_mask
        & (securities_df["list_date"] <= last_date)
        & (securities_df["delist_date"] >= first_date)
    ].copy()
    securities_by_pair = {
        pair: pair_df.sort_values("contract_code").reset_index(drop=True)
        for pair, pair_df in securities_df.groupby(
            ["underlying_code", "exchange_code"], sort=False
        )
    }

    contract_codes = sorted(securities_df["contract_code"].tolist())
    futures_info: dict[str, dict[str, object]] = {}
    for offset in range(0, len(contract_codes), 200):
        futures_info.update(
            jqdata.get_futures_info(
                contract_codes[offset : offset + 200],
                fields=["contract_multiplier", "tick_size", "trade_time"],
            )
        )
    missing_info = sorted(set(contract_codes) - set(futures_info))
    if missing_info:
        raise ValueError(f"get_futures_info 缺少 {len(missing_info)} 个合约：{missing_info[:10]}")

    trading_dates = sorted(set(variety_df["trading_date"]))
    previous_dates = _previous_trading_dates(trading_dates, jqdata)
    updated_at = datetime.now(timezone.utc).replace(microsecond=0)
    staging_path = table_path.with_name(f".{table_path.name}.{uuid.uuid4().hex}.tmp")
    row_count = 0
    written_contracts: set[str] = set()
    no_trade_time_contract_day_count = 0
    try:
        for partition, partition_df in variety_df.groupby(
            ["exchange_code", "year", "month"], sort=True
        ):
            rows: list[dict[str, object]] = []
            for variety in partition_df.sort_values(
                ["trading_date", "underlying_code"]
            ).itertuples(index=False):
                pair = (variety.underlying_code, variety.exchange_code)
                pair_securities_df = securities_by_pair.get(pair)
                if pair_securities_df is None:
                    raise ValueError(f"品种日历存在但没有固定合约元数据：{pair}")
                active_df = pair_securities_df[
                    (pair_securities_df["list_date"] <= variety.trading_date)
                    & (pair_securities_df["delist_date"] >= variety.trading_date)
                ]
                for contract in active_df.itertuples(index=False):
                    contract_info = futures_info[contract.contract_code]
                    rule = active_rule(
                        contract_info["trade_time"],
                        contract.contract_code,
                        variety.trading_date,
                    )
                    if rule is None:
                        no_trade_time_contract_day_count += 1
                        continue
                    for session_number, session_text in enumerate(rule[2:], 1):
                        start_text, end_text = session_text.split("~", 1)
                        start_time = time.fromisoformat(start_text)
                        end_time = time.fromisoformat(end_text)
                        is_night = start_time >= time(20) or start_time < time(6)
                        session_date = (
                            previous_dates[variety.trading_date]
                            if is_night
                            else variety.trading_date
                        )
                        session_end_date = (
                            session_date + timedelta(days=1)
                            if end_time <= start_time
                            else session_date
                        )
                        start_at = datetime.combine(session_date, start_time, CHINA_TZ)
                        end_at = datetime.combine(session_end_date, end_time, CHINA_TZ)
                        rows.append(
                            {
                                "contract_code": contract.contract_code,
                                "exchange_code": variety.exchange_code,
                                "underlying_code": variety.underlying_code,
                                "trading_date": variety.trading_date,
                                "list_date": contract.list_date,
                                "delist_date": contract.delist_date,
                                "contract_multiplier": contract_info.get("contract_multiplier"),
                                "tick_size": contract_info.get("tick_size"),
                                "rule_effective_date": date.fromisoformat(rule[0]),
                                "rule_expiry_date": date.fromisoformat(rule[1]),
                                "session_number": session_number,
                                "session_text": session_text,
                                "session_start_at": start_at,
                                "session_end_at": end_at,
                                "is_night_session": is_night,
                                "spans_midnight": session_date != session_end_date,
                                "minute_count": int((end_at - start_at).total_seconds() // 60),
                                "source": "JQData_get_all_securities_get_futures_info",
                                "updated_at": updated_at,
                                "year": variety.trading_date.year,
                                "month": variety.trading_date.month,
                            }
                        )
                        written_contracts.add(contract.contract_code)
            partition_result_df = pd.DataFrame(
                rows, columns=FUTURES_CONTRACT_CALENDAR_SCHEMA.names
            )
            key = ["contract_code", "trading_date", "session_number"]
            if partition_result_df.empty:
                raise ValueError(f"合约日历分区为空：{partition}")
            if partition_result_df.duplicated(key).any():
                raise ValueError(f"合约日历分区主键重复：{partition}")
            row_count += len(partition_result_df)
            ds.write_dataset(
                pandas_to_arrow(partition_result_df, FUTURES_CONTRACT_CALENDAR_SCHEMA),
                staging_path,
                format="parquet",
                partitioning=PARTITIONING,
                basename_template=f"part-{uuid.uuid4().hex}-{{i}}.parquet",
                existing_data_behavior="overwrite_or_ignore",
            )

        validate_dataset_streaming(
            staging_path,
            PARTITIONING,
            FUTURES_CONTRACT_CALENDAR_SCHEMA,
            expected_row_count=row_count,
        )
        swap_staged_dataset(staging_path, table_path)
    finally:
        if staging_path.exists():
            shutil.rmtree(staging_path)
    return ContractCalendarBuildSummary(
        row_count=row_count,
        contract_count=len(written_contracts),
        no_trade_time_contract_day_count=no_trade_time_contract_day_count,
        first_date=first_date,
        last_date=last_date,
    )
