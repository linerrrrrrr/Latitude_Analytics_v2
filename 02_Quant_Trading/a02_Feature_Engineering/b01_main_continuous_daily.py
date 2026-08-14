#!/usr/bin/env python
# coding: utf-8

# # b01 主力连续合约（日线）
# 
# 本 Notebook 参考旧版主力判定与拼接思路，使用当前 silver 数据湖重新实现。主力选择只使用上一交易日已收盘信息；输出写入 gold 层，并同时保留原始价格、原始 log 价格、前复权 log 价格与后复权 log 价格。
# 
# 期限结构继续使用真实合约原始价格，不使用这里的累计复权因子。

# In[ ]:


from __future__ import annotations

import importlib
import math
import pathlib
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import click
import ipywidgets as widgets
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from IPython.display import display

project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()  # 当前工作目录

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        break
else:
    raise RuntimeError("未找到项目根目录")

PROJECT_ROOT = candidate_root
lakehouse = importlib.import_module("02_Quant_Trading.a01_Data_Collection.c00_lakehouse")

from config.data_contracts import (  # noqa: E402
    FUTURES_CONTRACT_CALENDAR_SCHEMA,
    FUTURES_DAILY_SCHEMA,
    FUTURES_VARIETY_CALENDAR_SCHEMA,
    arrow_to_pandas,
    empty_pandas,
    pandas_to_arrow,
)
from config.settings import settings  # noqa: E402


# 当前主力连续实验的局部输出结构；gold 不属于全项目统一数据契约。
MAIN_CONTRACT_OUTPUT_SCHEMA = pa.schema(
    [
        pa.field("exchange_code", pa.string(), nullable=False),
        pa.field("underlying_code", pa.string(), nullable=False),
        pa.field("trading_date", pa.date32(), nullable=False),
        pa.field("main_contract_code", pa.string(), nullable=False),
        pa.field("signal_trading_date", pa.date32(), nullable=False),
        pa.field("continuity_segment", pa.int32(), nullable=False),
        pa.field("previous_main_contract_code", pa.string(), nullable=True),
        pa.field("challenger_contract_code", pa.string(), nullable=True),
        pa.field("selection_metric", pa.string(), nullable=False),
        pa.field("main_contract_signal_volume", pa.float64(), nullable=True),
        pa.field("incumbent_signal_volume", pa.float64(), nullable=True),
        pa.field("challenger_signal_volume", pa.float64(), nullable=True),
        pa.field("roll_trigger_ratio", pa.float64(), nullable=False),
        pa.field("is_roll", pa.bool_(), nullable=False),
        pa.field("selection_reason", pa.string(), nullable=False),
        pa.field("roll_anchor_date", pa.date32(), nullable=True),
        pa.field("previous_contract_anchor_close", pa.float64(), nullable=True),
        pa.field("main_contract_anchor_close", pa.float64(), nullable=True),
        pa.field("roll_log_gap", pa.float64(), nullable=True),
        pa.field("source", pa.string(), nullable=False),
        pa.field("updated_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("year", pa.int16(), nullable=False),
        pa.field("month", pa.int8(), nullable=False),
    ]
)

MAIN_CONTINUOUS_OUTPUT_SCHEMA = pa.schema(
    [
        pa.field("exchange_code", pa.string(), nullable=False),
        pa.field("underlying_code", pa.string(), nullable=False),
        pa.field("trading_date", pa.date32(), nullable=False),
        pa.field("main_contract_code", pa.string(), nullable=False),
        pa.field("signal_trading_date", pa.date32(), nullable=False),
        pa.field("continuity_segment", pa.int32(), nullable=False),
        pa.field("is_roll", pa.bool_(), nullable=False),
        pa.field("roll_log_gap", pa.float64(), nullable=True),
        pa.field("open", pa.float64(), nullable=True),
        pa.field("high", pa.float64(), nullable=True),
        pa.field("low", pa.float64(), nullable=True),
        pa.field("close", pa.float64(), nullable=True),
        pa.field("volume", pa.float64(), nullable=True),
        pa.field("money", pa.float64(), nullable=True),
        pa.field("open_interest", pa.float64(), nullable=True),
        pa.field("has_market_data", pa.bool_(), nullable=False),
        pa.field("log_open", pa.float64(), nullable=True),
        pa.field("log_high", pa.float64(), nullable=True),
        pa.field("log_low", pa.float64(), nullable=True),
        pa.field("log_close", pa.float64(), nullable=True),
        pa.field("forward_log_adjustment", pa.float64(), nullable=False),
        pa.field("backward_log_adjustment", pa.float64(), nullable=False),
        pa.field("forward_adjusted_log_open", pa.float64(), nullable=True),
        pa.field("forward_adjusted_log_high", pa.float64(), nullable=True),
        pa.field("forward_adjusted_log_low", pa.float64(), nullable=True),
        pa.field("forward_adjusted_log_close", pa.float64(), nullable=True),
        pa.field("backward_adjusted_log_open", pa.float64(), nullable=True),
        pa.field("backward_adjusted_log_high", pa.float64(), nullable=True),
        pa.field("backward_adjusted_log_low", pa.float64(), nullable=True),
        pa.field("backward_adjusted_log_close", pa.float64(), nullable=True),
        pa.field("source", pa.string(), nullable=False),
        pa.field("updated_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("year", pa.int16(), nullable=False),
        pa.field("month", pa.int8(), nullable=False),
    ]
)


# ## 1. 数据位置与分区
# 
# 输入是 silver 层的品种日历、合约日历和真实合约日线；输出是 gold 层的主力映射与连续日线。正式数据不写入代码目录。

# In[ ]:


SILVER_ROOT = settings.futures_lake_root / "silver"
GOLD_ROOT = settings.futures_lake_root / "gold"

VARIETY_CALENDAR_PATH = SILVER_ROOT / "dim_futures_variety_calendar"
CONTRACT_CALENDAR_PATH = SILVER_ROOT / "dim_futures_contract_calendar"
DAILY_PATH = SILVER_ROOT / "fact_futures_daily"
MAIN_CONTRACT_PATH = GOLD_ROOT / "fact_futures_main_contract_daily"
MAIN_CONTINUOUS_PATH = GOLD_ROOT / "fact_futures_main_continuous_daily"

SILVER_PARTITIONING = lakehouse.hive_partitioning(
    [
        pa.field("exchange_code", pa.string()),
        pa.field("year", pa.int16()),
        pa.field("month", pa.int8()),
    ]
)
GOLD_PARTITIONING = lakehouse.hive_partitioning(
    [
        pa.field("exchange_code", pa.string()),
        pa.field("underlying_code", pa.string()),
        pa.field("year", pa.int16()),
        pa.field("month", pa.int8()),
    ]
)
GOLD_PARTITION_FIELDS = ["exchange_code", "underlying_code", "year", "month"]


# ## 2. 契约化读取
# 
# 筛选参数用于 Notebook 研究和命令行预览。带筛选的结果不能覆盖正式 gold 全表。

# In[ ]:


def build_filter_expression(
    exchange_code: str,
    underlying_code: str,
) -> ds.Expression:
    conditions: list[ds.Expression] = [
        ds.field("exchange_code") == exchange_code,
        ds.field("underlying_code") == underlying_code,
    ]
    expression = conditions[0]
    for condition in conditions[1:]:
        expression = expression & condition
    return expression


def load_inputs(
    exchange_code: str,
    underlying_code: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    for table_path in [VARIETY_CALENDAR_PATH, CONTRACT_CALENDAR_PATH, DAILY_PATH]:
        if not table_path.exists():
            raise FileNotFoundError(f"输入数据集不存在：{table_path}")

    filter_expression = build_filter_expression(exchange_code, underlying_code)
    variety_calendar_df = lakehouse.read_dataset(
        VARIETY_CALENDAR_PATH,
        SILVER_PARTITIONING,
        FUTURES_VARIETY_CALENDAR_SCHEMA,
        ["exchange_code", "underlying_code", "trading_date", "year", "month"],
        filter_expression,
    )
    contract_calendar_df = lakehouse.read_dataset(
        CONTRACT_CALENDAR_PATH,
        SILVER_PARTITIONING,
        FUTURES_CONTRACT_CALENDAR_SCHEMA,
        [
            "contract_code",
            "exchange_code",
            "underlying_code",
            "trading_date",
            "delist_date",
            "session_number",
            "year",
            "month",
        ],
        filter_expression,
    )
    daily_df = lakehouse.read_dataset(
        DAILY_PATH,
        SILVER_PARTITIONING,
        FUTURES_DAILY_SCHEMA,
        [
            "contract_code",
            "exchange_code",
            "underlying_code",
            "trading_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "money",
            "open_interest",
            "has_market_data",
            "year",
            "month",
        ],
        filter_expression,
    )
    return variety_calendar_df, contract_calendar_df, daily_df


# ## 3. 无未来信息的主力映射
# 
# 信号日成交量决定下一交易日主力。换月只向退市日更晚的合约发生；换月 log-gap 使用不晚于信号日的最近共同有效收盘价。

# In[ ]:


def build_ranked_candidates_by_date(
    candidate_history_df: pd.DataFrame,
) -> dict[date, list[dict[str, object]]]:
    ranked_df = candidate_history_df.loc[
        candidate_history_df["has_market_data"].fillna(False)
        & candidate_history_df["volume"].notna()
        & (candidate_history_df["volume"] > 0)
    ].copy()
    if ranked_df.empty:
        return {}
    ranked_df["open_interest_rank"] = pd.to_numeric(
        ranked_df["open_interest"], errors="coerce"
    ).fillna(-math.inf)
    ranked_df = ranked_df.sort_values(
        ["trading_date", "volume", "open_interest_rank", "delist_date", "contract_code"],
        ascending=[True, False, False, True, True],
        kind="mergesort",
    ).drop(columns="open_interest_rank")
    return {
        trading_date: group_df.to_dict("records")
        for trading_date, group_df in ranked_df.groupby(
            "trading_date", sort=False, observed=True
        )
    }


def find_roll_anchor(
    price_history_df: pd.DataFrame,
    previous_contract_code: str,
    main_contract_code: str,
    signal_trading_date: date,
) -> tuple[date, float, float, float]:
    pair_df = price_history_df.loc[
        (price_history_df["trading_date"] <= signal_trading_date)
        & price_history_df["contract_code"].isin(
            [previous_contract_code, main_contract_code]
        )
        & price_history_df["has_market_data"].fillna(False)
        & price_history_df["close"].notna()
        & (price_history_df["close"] > 0),
        ["trading_date", "contract_code", "close"],
    ].drop_duplicates(["trading_date", "contract_code"], keep="last")
    close_matrix_df = pair_df.pivot(
        index="trading_date", columns="contract_code", values="close"
    )
    required_codes = [previous_contract_code, main_contract_code]
    if any(code not in close_matrix_df.columns for code in required_codes):
        raise ValueError(
            f"换月缺少共同有效收盘价：{previous_contract_code} -> {main_contract_code}，"
            f"signal_trading_date={signal_trading_date}"
        )
    overlap_df = close_matrix_df[required_codes].dropna()
    if overlap_df.empty:
        raise ValueError(
            f"换月缺少共同有效收盘价：{previous_contract_code} -> {main_contract_code}，"
            f"signal_trading_date={signal_trading_date}"
        )
    roll_anchor_date = overlap_df.index.max()
    previous_close = float(overlap_df.loc[roll_anchor_date, previous_contract_code])
    main_close = float(overlap_df.loc[roll_anchor_date, main_contract_code])
    roll_log_gap = math.log(main_close) - math.log(previous_close)
    return roll_anchor_date, previous_close, main_close, roll_log_gap


def build_main_contract_daily(
    variety_calendar_df: pd.DataFrame,
    contract_calendar_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    roll_trigger_ratio: float = 1.10,
    updated_at: datetime | None = None,
) -> pd.DataFrame:
    if roll_trigger_ratio <= 1:
        raise ValueError("roll_trigger_ratio 必须大于 1。")
    if variety_calendar_df.empty:
        return empty_pandas(MAIN_CONTRACT_OUTPUT_SCHEMA)

    updated_at = updated_at or datetime.now(timezone.utc)
    group_keys = ["exchange_code", "underlying_code"]
    key_columns = [*group_keys, "trading_date"]
    variety_dates_df = (
        variety_calendar_df[key_columns]
        .drop_duplicates()
        .sort_values([*group_keys, "trading_date"])
    )
    contract_dates_df = (
        contract_calendar_df[
            ["contract_code", *key_columns, "delist_date", "session_number"]
        ]
        .sort_values([*key_columns, "contract_code", "session_number"])
        .drop_duplicates([*key_columns, "contract_code"], keep="first")
        .drop(columns="session_number")
    )
    daily_work_df = daily_df[
        [
            "contract_code",
            *key_columns,
            "close",
            "volume",
            "open_interest",
            "has_market_data",
        ]
    ].drop_duplicates([*key_columns, "contract_code"], keep="last")
    candidate_history_df = daily_work_df.merge(
        contract_dates_df,
        how="left",
        on=["contract_code", *key_columns],
        validate="one_to_one",
    )

    rows: list[dict[str, object]] = []
    for (exchange_code, underlying_code), date_group_df in variety_dates_df.groupby(
        group_keys, sort=True, observed=True
    ):
        trading_dates = sorted(date_group_df["trading_date"].tolist())
        history_df = candidate_history_df.loc[
            (candidate_history_df["exchange_code"] == exchange_code)
            & (candidate_history_df["underlying_code"] == underlying_code)
        ].copy()
        group_contract_dates_df = contract_dates_df.loc[
            (contract_dates_df["exchange_code"] == exchange_code)
            & (contract_dates_df["underlying_code"] == underlying_code)
        ].copy()
        delist_lookup = (
            group_contract_dates_df
            .groupby("contract_code", observed=True)["delist_date"]
            .max()
            .to_dict()
        )
        active_codes_by_date = {
            trading_date: set(group_df["contract_code"].astype(str))
            for trading_date, group_df in group_contract_dates_df.groupby(
                "trading_date", observed=True
            )
        }
        ranked_candidates_by_date = build_ranked_candidates_by_date(history_df)
        main_contract_code: str | None = None
        continuity_segment = 0

        for signal_index, signal_trading_date in enumerate(trading_dates[:-1]):
            trading_date = trading_dates[signal_index + 1]
            ranked_candidates = ranked_candidates_by_date.get(signal_trading_date, [])
            target_active_codes = active_codes_by_date.get(trading_date, set())
            eligible_candidates = [
                candidate
                for candidate in ranked_candidates
                if str(candidate["contract_code"]) in target_active_codes
            ]

            previous_main_contract_code = main_contract_code
            incumbent_signal_volume: float | None = None
            challenger_contract_code: str | None = None
            challenger_signal_volume: float | None = None

            if main_contract_code is None:
                if not eligible_candidates:
                    continue
                continuity_segment += 1
                main_contract_code = str(eligible_candidates[0]["contract_code"])
                selection_reason = "bootstrap_previous_day_volume_leader"
                later_candidates = [
                    candidate
                    for candidate in eligible_candidates
                    if str(candidate["contract_code"]) != main_contract_code
                ]
                if later_candidates:
                    challenger_contract_code = str(later_candidates[0]["contract_code"])
                    challenger_signal_volume = float(later_candidates[0]["volume"])
            else:
                incumbent_candidate = next(
                    (
                        candidate
                        for candidate in ranked_candidates
                        if str(candidate["contract_code"]) == main_contract_code
                    ),
                    None,
                )
                if incumbent_candidate is not None:
                    incumbent_signal_volume = float(incumbent_candidate["volume"])

                incumbent_delist_date = delist_lookup.get(main_contract_code)
                challenger_candidates = [
                    candidate
                    for candidate in eligible_candidates
                    if str(candidate["contract_code"]) != main_contract_code
                    and (
                        incumbent_delist_date is None
                        or (
                            pd.notna(candidate["delist_date"])
                            and candidate["delist_date"] > incumbent_delist_date
                        )
                    )
                ]
                if challenger_candidates:
                    challenger_contract_code = str(
                        challenger_candidates[0]["contract_code"]
                    )
                    challenger_signal_volume = float(
                        challenger_candidates[0]["volume"]
                    )

                if main_contract_code not in target_active_codes:
                    if challenger_contract_code is None:
                        main_contract_code = None
                        continue
                    main_contract_code = challenger_contract_code
                    selection_reason = "incumbent_not_active_next_trading_date"
                elif incumbent_signal_volume is None:
                    if challenger_contract_code is None:
                        selection_reason = "no_valid_volume_keep_incumbent"
                    else:
                        main_contract_code = challenger_contract_code
                        selection_reason = "incumbent_market_data_missing"
                elif (
                    challenger_signal_volume is not None
                    and challenger_signal_volume
                    > incumbent_signal_volume * roll_trigger_ratio
                ):
                    main_contract_code = challenger_contract_code
                    selection_reason = "challenger_volume_threshold"
                else:
                    selection_reason = "incumbent_retained"

            selected_candidate = next(
                (
                    candidate
                    for candidate in ranked_candidates
                    if str(candidate["contract_code"]) == main_contract_code
                ),
                None,
            )
            main_contract_signal_volume = (
                None
                if selected_candidate is None
                else float(selected_candidate["volume"])
            )
            is_roll = (
                previous_main_contract_code is not None
                and main_contract_code != previous_main_contract_code
            )
            roll_anchor_date = None
            previous_contract_anchor_close = None
            main_contract_anchor_close = None
            roll_log_gap = None
            if is_roll:
                (
                    roll_anchor_date,
                    previous_contract_anchor_close,
                    main_contract_anchor_close,
                    roll_log_gap,
                ) = find_roll_anchor(
                    history_df,
                    previous_main_contract_code,
                    main_contract_code,
                    signal_trading_date,
                )

            rows.append(
                {
                    "exchange_code": str(exchange_code),
                    "underlying_code": str(underlying_code),
                    "trading_date": trading_date,
                    "main_contract_code": main_contract_code,
                    "signal_trading_date": signal_trading_date,
                    "continuity_segment": continuity_segment,
                    "previous_main_contract_code": previous_main_contract_code,
                    "challenger_contract_code": challenger_contract_code,
                    "selection_metric": "volume",
                    "main_contract_signal_volume": main_contract_signal_volume,
                    "incumbent_signal_volume": incumbent_signal_volume,
                    "challenger_signal_volume": challenger_signal_volume,
                    "roll_trigger_ratio": float(roll_trigger_ratio),
                    "is_roll": is_roll,
                    "selection_reason": selection_reason,
                    "roll_anchor_date": roll_anchor_date,
                    "previous_contract_anchor_close": previous_contract_anchor_close,
                    "main_contract_anchor_close": main_contract_anchor_close,
                    "roll_log_gap": roll_log_gap,
                    "source": "fact_futures_daily+dim_futures_contract_calendar",
                    "updated_at": updated_at,
                    "year": trading_date.year,
                    "month": trading_date.month,
                }
            )

    if not rows:
        return empty_pandas(MAIN_CONTRACT_OUTPUT_SCHEMA)
    mapping_df = pd.DataFrame(rows, columns=MAIN_CONTRACT_OUTPUT_SCHEMA.names)
    duplicate_mask = mapping_df.duplicated(key_columns, keep=False)
    if duplicate_mask.any():
        raise ValueError(
            "主力映射主键重复："
            f"{mapping_df.loc[duplicate_mask, key_columns].head().to_dict('records')}"
        )
    return arrow_to_pandas(
        pandas_to_arrow(mapping_df, MAIN_CONTRACT_OUTPUT_SCHEMA),
        MAIN_CONTRACT_OUTPUT_SCHEMA,
    )


# ## 4. log 前、后复权连续日线
# 
# 前复权以最新端为锚，历史价格加上未来换月 gap；后复权以最早端为锚，未来价格减去已经发生的换月 gap。两者在 log 空间相差一个常数。

# In[ ]:


def build_main_continuous_daily(
    mapping_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    updated_at: datetime | None = None,
) -> pd.DataFrame:
    if mapping_df.empty:
        return empty_pandas(MAIN_CONTINUOUS_OUTPUT_SCHEMA)
    updated_at = updated_at or datetime.now(timezone.utc)
    join_keys = ["exchange_code", "underlying_code", "trading_date"]
    selected_daily_df = daily_df[
        [
            "contract_code",
            *join_keys,
            "open",
            "high",
            "low",
            "close",
            "volume",
            "money",
            "open_interest",
            "has_market_data",
        ]
    ].rename(columns={"contract_code": "main_contract_code"})
    selected_daily_df = selected_daily_df.drop_duplicates(
        [*join_keys, "main_contract_code"], keep="last"
    )
    continuous_df = mapping_df[
        [
            *join_keys,
            "main_contract_code",
            "signal_trading_date",
            "continuity_segment",
            "is_roll",
            "roll_log_gap",
        ]
    ].merge(
        selected_daily_df,
        how="left",
        on=[*join_keys, "main_contract_code"],
        validate="one_to_one",
    )
    continuous_df["has_market_data"] = (
        continuous_df["has_market_data"].fillna(False).astype(bool)
    )

    price_columns = ["open", "high", "low", "close"]
    invalid_price_mask = continuous_df["has_market_data"] & (
        continuous_df[price_columns].isna().any(axis=1)
        | (continuous_df[price_columns] <= 0).any(axis=1)
    )
    if invalid_price_mask.any():
        invalid_rows = continuous_df.loc[
            invalid_price_mask,
            [*join_keys, "main_contract_code", *price_columns],
        ].head()
        raise ValueError(
            "有效主力行情存在非正或缺失 OHLC，无法取 log："
            f"{invalid_rows.to_dict('records')}"
        )

    for price_column in price_columns:
        continuous_df[f"log_{price_column}"] = np.log(
            pd.to_numeric(continuous_df[price_column], errors="coerce")
        )

    continuous_df["forward_log_adjustment"] = 0.0
    continuous_df["backward_log_adjustment"] = 0.0
    group_keys = ["exchange_code", "underlying_code"]
    adjustment_group_keys = [*group_keys, "continuity_segment"]
    continuous_df = continuous_df.sort_values([*adjustment_group_keys, "trading_date"]).reset_index(
        drop=True
    )
    for _, group_df in continuous_df.groupby(
        adjustment_group_keys, sort=True, observed=True
    ):
        group_index = group_df.index
        roll_gap_array = pd.to_numeric(
            group_df["roll_log_gap"], errors="coerce"
        ).fillna(0.0).to_numpy(dtype=float)
        cumulative_gap_array = np.cumsum(roll_gap_array)
        continuous_df.loc[group_index, "backward_log_adjustment"] = (
            -cumulative_gap_array
        )
        continuous_df.loc[group_index, "forward_log_adjustment"] = (
            roll_gap_array.sum() - cumulative_gap_array
        )

    for price_column in price_columns:
        log_column = f"log_{price_column}"
        continuous_df[f"forward_adjusted_log_{price_column}"] = (
            continuous_df[log_column] + continuous_df["forward_log_adjustment"]
        )
        continuous_df[f"backward_adjusted_log_{price_column}"] = (
            continuous_df[log_column] + continuous_df["backward_log_adjustment"]
        )

    continuous_df["source"] = "fact_futures_daily+fact_futures_main_contract_daily"
    continuous_df["updated_at"] = updated_at
    continuous_df["year"] = continuous_df["trading_date"].map(lambda value: value.year)
    continuous_df["month"] = continuous_df["trading_date"].map(lambda value: value.month)
    continuous_df = continuous_df[MAIN_CONTINUOUS_OUTPUT_SCHEMA.names]
    return arrow_to_pandas(
        pandas_to_arrow(continuous_df, MAIN_CONTINUOUS_OUTPUT_SCHEMA),
        MAIN_CONTINUOUS_OUTPUT_SCHEMA,
    )


# ## 5. 流程入口
# 
# 计算完成后先通过 Arrow Schema 转换。每次运行必须指定一个交易所和一个品种；显式传入 `--write` 时只替换该品种的年月分区。

# In[ ]:


def replace_target_partitions(
    mapping_df: pd.DataFrame,
    continuous_df: pd.DataFrame,
    variety_calendar_df: pd.DataFrame,
) -> int:
    partition_values = sorted(
        {
            (
                str(row.exchange_code),
                str(row.underlying_code),
                int(row.year),
                int(row.month),
            )
            for row in variety_calendar_df[
                ["exchange_code", "underlying_code", "year", "month"]
            ].drop_duplicates().itertuples(index=False)
        }
    )
    for exchange_code, underlying_code, year, month in partition_values:
        mapping_partition_df = mapping_df.loc[
            (mapping_df["exchange_code"] == exchange_code)
            & (mapping_df["underlying_code"] == underlying_code)
            & (mapping_df["year"] == year)
            & (mapping_df["month"] == month)
        ]
        continuous_partition_df = continuous_df.loc[
            (continuous_df["exchange_code"] == exchange_code)
            & (continuous_df["underlying_code"] == underlying_code)
            & (continuous_df["year"] == year)
            & (continuous_df["month"] == month)
        ]
        values = {
            "exchange_code": exchange_code,
            "underlying_code": underlying_code,
            "year": year,
            "month": month,
        }
        lakehouse.replace_partition(
            pandas_to_arrow(
                mapping_partition_df, MAIN_CONTRACT_OUTPUT_SCHEMA
            ),
            MAIN_CONTRACT_PATH,
            GOLD_PARTITIONING,
            GOLD_PARTITION_FIELDS,
            MAIN_CONTRACT_OUTPUT_SCHEMA,
            values,
        )
        lakehouse.replace_partition(
            pandas_to_arrow(
                continuous_partition_df, MAIN_CONTINUOUS_OUTPUT_SCHEMA
            ),
            MAIN_CONTINUOUS_PATH,
            GOLD_PARTITIONING,
            GOLD_PARTITION_FIELDS,
            MAIN_CONTINUOUS_OUTPUT_SCHEMA,
            values,
        )
    return len(partition_values)


def run_pipeline(
    exchange_code: str,
    underlying_code: str,
    start_date: date | None = None,
    end_date: date | None = None,
    roll_trigger_ratio: float = 1.10,
    write_dataset: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if write_dataset and (start_date is not None or end_date is not None):
        raise ValueError("写入目标品种时不得截断日期；必须重算该品种完整历史。")

    variety_calendar_df, contract_calendar_df, daily_df = load_inputs(
        exchange_code, underlying_code
    )
    updated_at = datetime.now(timezone.utc)
    mapping_df = build_main_contract_daily(
        variety_calendar_df,
        contract_calendar_df,
        daily_df,
        roll_trigger_ratio,
        updated_at,
    )
    continuous_df = build_main_continuous_daily(mapping_df, daily_df, updated_at)

    pandas_to_arrow(mapping_df, MAIN_CONTRACT_OUTPUT_SCHEMA)
    pandas_to_arrow(continuous_df, MAIN_CONTINUOUS_OUTPUT_SCHEMA)
    if write_dataset:
        replace_target_partitions(
            mapping_df,
            continuous_df,
            variety_calendar_df,
        )
    if start_date is not None:
        mapping_df = mapping_df.loc[mapping_df["trading_date"] >= start_date]
        continuous_df = continuous_df.loc[
            continuous_df["trading_date"] >= start_date
        ]
    if end_date is not None:
        mapping_df = mapping_df.loc[mapping_df["trading_date"] <= end_date]
        continuous_df = continuous_df.loc[
            continuous_df["trading_date"] <= end_date
        ]
    return mapping_df, continuous_df


def parse_optional_date(value: str | None) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise click.BadParameter("日期必须使用 YYYY-MM-DD 格式。") from error


@click.command()
@click.option("--start-date", type=str)
@click.option("--end-date", type=str)
@click.option("--exchange-code", type=str, required=True)
@click.option("--underlying-code", type=str, required=True)
@click.option("--roll-trigger-ratio", type=click.FloatRange(min=1.0, min_open=True), default=1.10, show_default=True)
@click.option("--write", "write_dataset", is_flag=True, help="重算目标品种完整历史并替换其 gold 年月分区。")
def main(
    start_date: str | None,
    end_date: str | None,
    exchange_code: str | None,
    underlying_code: str | None,
    roll_trigger_ratio: float,
    write_dataset: bool,
) -> None:
    mapping_df, continuous_df = run_pipeline(
        exchange_code,
        underlying_code,
        parse_optional_date(start_date),
        parse_optional_date(end_date),
        roll_trigger_ratio,
        write_dataset,
    )
    click.echo(f"mapping_rows: {len(mapping_df)}")
    click.echo(f"continuous_rows: {len(continuous_df)}")
    click.echo(f"roll_count: {int(mapping_df['is_roll'].sum()) if len(mapping_df) else 0}")
    click.echo(f"write_dataset: {write_dataset}")
    click.echo(f"main_contract_path: {MAIN_CONTRACT_PATH}")
    click.echo(f"main_continuous_path: {MAIN_CONTINUOUS_PATH}")


def running_in_ipykernel() -> bool:
    try:
        from ipykernel.kernelapp import IPKernelApp
    except ImportError:
        return False
    return IPKernelApp.initialized()


if __name__ == "__main__" and not running_in_ipykernel():
    main()


# ## 6. Notebook 交互预览
# 
# 控件只允许选择一个交易所和一个品种，并且只触发只读计算。日期控件裁剪展示区间；主力状态和前复权仍基于该品种完整历史计算，避免区间起点错误初始化。交互区不提供生产写入按钮。

# In[ ]:


def build_interactive_preview() -> widgets.VBox:
    if not VARIETY_CALENDAR_PATH.exists():
        raise FileNotFoundError(f"输入数据集不存在：{VARIETY_CALENDAR_PATH}")
    variety_options_df = (
        ds.dataset(
            VARIETY_CALENDAR_PATH,
            format="parquet",
            partitioning=SILVER_PARTITIONING,
        )
        .to_table(columns=["exchange_code", "underlying_code"])
        .to_pandas()
        .drop_duplicates()
    )
    exchange_options = sorted(variety_options_df["exchange_code"].unique().tolist())
    if not exchange_options:
        raise ValueError("完整品种日历没有可选交易所。")

    def underlying_options(exchange_code: str) -> list[str]:
        return sorted(
            variety_options_df.loc[
                variety_options_df["exchange_code"] == exchange_code, "underlying_code"
            ].unique().tolist()
        )

    default_exchange = "XSGE" if "XSGE" in exchange_options else exchange_options[0]
    exchange_widget = widgets.Dropdown(
        options=exchange_options, value=default_exchange, description="交易所"
    )
    initial_underlyings = underlying_options(exchange_widget.value)
    default_underlying = "CU" if "CU" in initial_underlyings else initial_underlyings[0]
    underlying_widget = widgets.Combobox(
        options=initial_underlyings,
        value=default_underlying,
        description="品种",
        ensure_option=False,
        placeholder="可输入日线品种代码",
    )
    start_date_widget = widgets.DatePicker(
        description="起始日", value=date(2024, 1, 1)
    )
    end_date_widget = widgets.DatePicker(
        description="结束日", value=date(2024, 12, 31)
    )
    ratio_widget = widgets.FloatSlider(
        description="换月阈值",
        value=1.10,
        min=1.01,
        max=2.00,
        step=0.01,
        readout_format=".2f",
        continuous_update=False,
    )
    run_button = widgets.Button(
        description="只读计算", button_style="primary", icon="play"
    )
    clear_button = widgets.Button(description="清空输出", icon="trash")
    output_widget = widgets.Output()

    def handle_exchange_change(change: dict[str, object]) -> None:
        options = underlying_options(str(change["new"]))
        underlying_widget.options = options
        if options:
            underlying_widget.value = options[0]

    def handle_run(_: widgets.Button) -> None:
        output_widget.clear_output(wait=True)
        with output_widget:
            start_date = start_date_widget.value
            end_date = end_date_widget.value
            if start_date is not None and end_date is not None and start_date > end_date:
                print("起始日不能晚于结束日。")
                return
            try:
                mapping_preview_df, continuous_preview_df = run_pipeline(
                    exchange_code=str(exchange_widget.value),
                    underlying_code=str(underlying_widget.value).strip().upper(),
                    start_date=start_date,
                    end_date=end_date,
                    roll_trigger_ratio=float(ratio_widget.value),
                    write_dataset=False,
                )
            except Exception as error:
                print(f"{type(error).__name__}: {error}")
                return

            summary_df = pd.DataFrame(
                [
                    {
                        "exchange_code": exchange_widget.value,
                        "underlying_code": underlying_widget.value,
                        "row_count": len(continuous_preview_df),
                        "roll_count": int(mapping_preview_df["is_roll"].sum()),
                        "continuity_segment_count": int(
                            continuous_preview_df["continuity_segment"].nunique()
                        ),
                    }
                ]
            )
            display(summary_df)
            roll_columns = [
                "trading_date",
                "previous_main_contract_code",
                "main_contract_code",
                "roll_anchor_date",
                "roll_log_gap",
                "selection_reason",
            ]
            display(mapping_preview_df.loc[mapping_preview_df["is_roll"], roll_columns])
            if continuous_preview_df.empty:
                print("所选展示区间没有可用连续行情。")
                return

            plot_df = continuous_preview_df.sort_values("trading_date")
            figure, axis = plt.subplots(figsize=(14, 6))
            axis.plot(plot_df["trading_date"], plot_df["log_close"], label="raw log close", alpha=0.55)
            axis.plot(plot_df["trading_date"], plot_df["forward_adjusted_log_close"], label="forward adjusted")
            axis.plot(plot_df["trading_date"], plot_df["backward_adjusted_log_close"], label="backward adjusted")
            axis.set_title(
                f"{exchange_widget.value}.{underlying_widget.value} main continuous log close"
            )
            axis.set_xlabel("trading_date")
            axis.set_ylabel("log price")
            axis.grid(alpha=0.25)
            axis.legend()
            figure.autofmt_xdate()
            plt.show()

    exchange_widget.observe(handle_exchange_change, names="value")
    run_button.on_click(handle_run)
    clear_button.on_click(lambda _: output_widget.clear_output())
    controls = widgets.VBox(
        [
            widgets.HBox([exchange_widget, underlying_widget]),
            widgets.HBox([start_date_widget, end_date_widget]),
            ratio_widget,
            widgets.HBox([run_button, clear_button]),
            output_widget,
        ]
    )
    return controls


if running_in_ipykernel():
    display(build_interactive_preview())

