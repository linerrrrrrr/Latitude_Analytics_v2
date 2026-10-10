"""期货日线与分钟线品种宇宙的单一配置入口。"""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd


DAILY_UNIVERSE_POLICY = "all_fixed_commodity_futures"

# 分钟线第一阶段只配置业务选择，不在这里写日期或合约判断。日期和合约由上游
# dim_futures_variety_calendar / dim_futures_contract_calendar 决定。
MINUTE_VARIETIES_BY_REASON: dict[str, dict[str, tuple[str, ...]]] = {
    "required": {
        "XSGE": ("CU", "RB"),
    },
    "energy": {
        "XDCE": ("BZ", "EB", "EG", "J", "JM", "PG"),
        "XINE": ("LU", "SC"),
        "XSGE": ("BU", "FU"),
        "XZCE": ("FG", "MA", "ME", "TA", "TC", "ZC"),
    },
    "metal": {
        "GFEX": ("LC", "PT", "SI"),
        "XDCE": ("I",),
        "XINE": ("BC",),
        "XSGE": (
            "AD", "AG", "AL", "AO", "AU", "BR", "NI", "OP", "PB",
            "SN", "SP", "SS", "WR", "ZN",
        ),
        "XZCE": ("SF", "SH", "SM"),
    },
    "industrial": {
        "GFEX": ("PD", "PS"),
        "XDCE": ("BB", "FB", "L", "LG", "PP", "V"),
        "XINE": ("EC", "NR"),
        "XSGE": ("HC", "RU"),
        "XZCE": ("CY", "PF", "PL", "PR", "PX", "SA", "UR"),
    },
}

EXCHANGE_FALLBACK_PRIORITY: dict[str, tuple[str, ...]] = {
    "GFEX": ("SI",),
    "XDCE": ("I",),
    "XINE": ("SC",),
    "XSGE": ("RB",),
    "XZCE": ("TA",),
}


def _policy_rows() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for selection_reason, exchange_map in MINUTE_VARIETIES_BY_REASON.items():
        for exchange_code, underlying_codes in exchange_map.items():
            for underlying_code in underlying_codes:
                pair = (exchange_code, underlying_code)
                if pair in seen:
                    raise ValueError(f"分钟线品种配置重复：{pair}")
                seen.add(pair)
                rows.append(
                    {
                        "exchange_code": exchange_code,
                        "underlying_code": underlying_code,
                        "selection_reason": selection_reason,
                    }
                )
    return rows


MINUTE_POLICY_DF = pd.DataFrame(_policy_rows())
if len(MINUTE_POLICY_DF) != 59:
    raise ValueError(f"确认的分钟线品种应为 59 个，实际为 {len(MINUTE_POLICY_DF)} 个。")


def normalize_available_varieties(available_df: pd.DataFrame) -> pd.DataFrame:
    """规范化上游可用品种，只保留交易所与品种代码。"""
    required_columns = {"exchange_code", "underlying_code"}
    missing_columns = required_columns - set(available_df.columns)
    if missing_columns:
        raise ValueError(f"可用品种缺少字段：{sorted(missing_columns)}")
    normalized_df = available_df[["exchange_code", "underlying_code"]].copy()
    normalized_df["exchange_code"] = normalized_df["exchange_code"].astype(str).str.upper()
    normalized_df["underlying_code"] = normalized_df["underlying_code"].astype(str).str.upper()
    return normalized_df.drop_duplicates().sort_values(
        ["exchange_code", "underlying_code"]
    ).reset_index(drop=True)


def select_daily_varieties(available_df: pd.DataFrame) -> pd.DataFrame:
    """日线选择全部上游可用的固定商品期货品种。"""
    selected_df = normalize_available_varieties(available_df)
    selected_df["selection_reason"] = DAILY_UNIVERSE_POLICY
    return selected_df


def _first_available(
    exchange_code: str,
    available_codes: Iterable[str],
) -> str:
    available_set = set(available_codes)
    for underlying_code in EXCHANGE_FALLBACK_PRIORITY.get(exchange_code, ()):
        if underlying_code in available_set:
            return underlying_code
    return sorted(available_set)[0]


def select_minute_varieties(available_df: pd.DataFrame) -> pd.DataFrame:
    """应用已确认的 59 品种规则，并保证每个可用交易所至少选择一个品种。"""
    available_df = normalize_available_varieties(available_df)
    if available_df.empty:
        return pd.DataFrame(
            columns=["exchange_code", "underlying_code", "selection_reason"]
        )

    selected_df = available_df.merge(
        MINUTE_POLICY_DF,
        on=["exchange_code", "underlying_code"],
        how="inner",
        validate="one_to_one",
    )
    rows = [selected_df]
    selected_exchanges = set(selected_df["exchange_code"])
    for exchange_code, exchange_df in available_df.groupby("exchange_code", sort=True):
        if exchange_code in selected_exchanges:
            continue
        fallback_code = _first_available(exchange_code, exchange_df["underlying_code"])
        rows.append(
            pd.DataFrame(
                [
                    {
                        "exchange_code": exchange_code,
                        "underlying_code": fallback_code,
                        "selection_reason": "exchange_fallback",
                    }
                ]
            )
        )

    result_df = pd.concat(rows, ignore_index=True)
    return result_df.drop_duplicates(
        ["exchange_code", "underlying_code"], keep="first"
    ).sort_values(["exchange_code", "underlying_code"]).reset_index(drop=True)


def selection_reason_map(available_df: pd.DataFrame) -> dict[tuple[str, str], str]:
    """返回便于状态表逐行映射的 ``(交易所, 品种) -> 原因``。"""
    selected_df = select_minute_varieties(available_df)
    return {
        (row.exchange_code, row.underlying_code): row.selection_reason
        for row in selected_df.itertuples(index=False)
    }

