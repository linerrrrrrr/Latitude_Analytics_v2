"""实验 JQData 期货日 K 与夜盘、日盘分钟 Session 的边界对应关系。"""

from __future__ import annotations

import pathlib
import sys
from datetime import date, datetime, time, timedelta

import numpy as np
import pandas as pd


project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        project_root = candidate_root
        break
else:
    raise RuntimeError("未找到项目根目录")

from config.settings import settings  # noqa: E402


START_DATE = date(2025, 2, 10)
END_DATE = date(2025, 2, 14)
SYMBOLS = {
    "RB": "螺纹钢（约 23:00 结束夜盘）",
    "CU": "沪铜（约 01:00 结束夜盘）",
    "AU": "沪金（约 02:30 结束夜盘）",
}
PRICE_FIELDS = ["open", "high", "low", "close"]
MARKET_FIELDS = PRICE_FIELDS + ["volume", "money", "open_interest"]
OUTPUT_DIR = pathlib.Path(__file__).resolve().parent / "output"


def main() -> None:
    from config.jqdata_connection import authenticate_jqdata

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    jq = authenticate_jqdata(settings.jqdata_id, settings.jqdata_secret)

    try:
        extended_trade_days = [
            pd.Timestamp(value).date()
            for value in jq.get_trade_days(
                start_date=START_DATE - timedelta(days=15),
                end_date=END_DATE,
            )
        ]
        target_trade_days = [
            value for value in extended_trade_days if START_DATE <= value <= END_DATE
        ]
        if len(target_trade_days) != 5:
            raise ValueError(
                f"实验周应包含 5 个交易日，实际为 {target_trade_days}。"
            )

        previous_trade_day = {}
        for trading_date in target_trade_days:
            earlier = [value for value in extended_trade_days if value < trading_date]
            if not earlier:
                raise ValueError(f"无法取得 {trading_date} 的上一交易日。")
            previous_trade_day[trading_date] = max(earlier)

        contract_rows = []
        daily_frames = []
        minute_frames = []

        for symbol, description in SYMBOLS.items():
            dominant = jq.get_dominant_future(
                symbol,
                START_DATE.isoformat(),
                END_DATE.isoformat(),
            )
            if isinstance(dominant, str):
                dominant_series = pd.Series(
                    [dominant] * len(target_trade_days),
                    index=pd.Index(target_trade_days, name="trading_date"),
                    dtype="string",
                )
            elif isinstance(dominant, pd.DataFrame):
                if dominant.shape[1] != 1:
                    raise ValueError(
                        f"{symbol} get_dominant_future 返回多列：{list(dominant)}"
                    )
                dominant_series = dominant.iloc[:, 0]
            else:
                dominant_series = pd.Series(dominant)

            dominant_series = dominant_series.dropna().astype("string")
            contracts = sorted(dominant_series.unique().tolist())
            if len(contracts) != 1:
                raise ValueError(
                    f"{symbol} 在实验周发生主力切换，不能用同一合约比较：{contracts}"
                )
            contract_code = contracts[0]
            contract_rows.append(
                {
                    "underlying_code": symbol,
                    "description": description,
                    "contract_code": contract_code,
                    "week_start": START_DATE,
                    "week_end": END_DATE,
                }
            )

            daily_raw = jq.get_price(
                contract_code,
                start_date=START_DATE,
                end_date=END_DATE,
                frequency="daily",
                fields=MARKET_FIELDS,
                skip_paused=False,
                fq=None,
                panel=False,
            )
            if daily_raw is None or daily_raw.empty:
                raise RuntimeError(f"{contract_code} 未返回日 K。")
            missing_daily = set(MARKET_FIELDS) - set(daily_raw.columns)
            if missing_daily:
                raise ValueError(
                    f"{contract_code} 日 K 缺列：{sorted(missing_daily)}"
                )
            daily = daily_raw.reset_index()
            daily_time_column = "time" if "time" in daily else daily.columns[0]
            daily = daily.rename(columns={daily_time_column: "daily_bar_at"})
            daily["daily_bar_at"] = pd.to_datetime(daily["daily_bar_at"])
            daily["trading_date"] = daily["daily_bar_at"].dt.date
            daily["underlying_code"] = symbol
            daily["contract_code"] = contract_code
            daily = daily.loc[daily["trading_date"].isin(target_trade_days)].copy()
            if daily["trading_date"].duplicated().any() or len(daily) != 5:
                raise ValueError(
                    f"{contract_code} 日 K 应为 5 行且交易日唯一，实际 {len(daily)} 行。"
                )
            daily_frames.append(
                daily[
                    [
                        "underlying_code",
                        "contract_code",
                        "daily_bar_at",
                        "trading_date",
                        *MARKET_FIELDS,
                    ]
                ]
            )

            first_previous = previous_trade_day[target_trade_days[0]]
            minute_raw = jq.get_price(
                contract_code,
                start_date=datetime.combine(first_previous, time(15, 0)),
                end_date=datetime.combine(target_trade_days[-1], time(15, 0)),
                frequency="1m",
                fields=MARKET_FIELDS,
                skip_paused=False,
                fq=None,
                panel=False,
            )
            if minute_raw is None or minute_raw.empty:
                raise RuntimeError(f"{contract_code} 未返回分钟 K。")
            missing_minute = set(MARKET_FIELDS) - set(minute_raw.columns)
            if missing_minute:
                raise ValueError(
                    f"{contract_code} 分钟 K 缺列：{sorted(missing_minute)}"
                )
            minute = minute_raw.reset_index()
            minute_time_column = "time" if "time" in minute else minute.columns[0]
            minute = minute.rename(columns={minute_time_column: "bar_at"})
            minute["bar_at"] = pd.to_datetime(minute["bar_at"])
            minute = minute.loc[
                (minute["bar_at"] > datetime.combine(first_previous, time(15, 0)))
                & (
                    minute["bar_at"]
                    <= datetime.combine(target_trade_days[-1], time(15, 0))
                )
            ].copy()
            minute["underlying_code"] = symbol
            minute["contract_code"] = contract_code

            local_times = minute["bar_at"].dt.time
            evening = local_times >= time(20, 0)
            early_morning = local_times <= time(3, 0)
            daytime = (local_times >= time(8, 30)) & (local_times <= time(16, 0))
            minute["session_type"] = "other"
            minute.loc[evening | early_morning, "session_type"] = "night"
            minute.loc[daytime, "session_type"] = "day"
            minute["session_anchor_date"] = minute["bar_at"].dt.date
            minute.loc[early_morning, "session_anchor_date"] = minute.loc[
                early_morning, "bar_at"
            ].dt.date - timedelta(days=1)
            minute["session_id"] = (
                minute["session_type"].astype("string")
                + "_"
                + minute["session_anchor_date"].astype("string")
            )
            unexpected = minute.loc[minute["session_type"].eq("other")]
            if not unexpected.empty:
                raise ValueError(
                    f"{contract_code} 出现未分类分钟："
                    f"{unexpected['bar_at'].head().astype(str).tolist()}"
                )
            minute_frames.append(
                minute[
                    [
                        "underlying_code",
                        "contract_code",
                        "bar_at",
                        "session_type",
                        "session_anchor_date",
                        "session_id",
                        *MARKET_FIELDS,
                    ]
                ]
            )
    finally:
        jq.logout()

    contracts = pd.DataFrame(contract_rows)
    daily_bars = pd.concat(daily_frames, ignore_index=True)
    minute_bars = pd.concat(minute_frames, ignore_index=True)
    minute_bars = minute_bars.sort_values(
        ["underlying_code", "contract_code", "bar_at"]
    ).reset_index(drop=True)

    session_rows = []
    for keys, session in minute_bars.groupby(
        [
            "underlying_code",
            "contract_code",
            "session_type",
            "session_anchor_date",
            "session_id",
        ],
        sort=True,
    ):
        session = session.sort_values("bar_at")
        session_rows.append(
            {
                "underlying_code": keys[0],
                "contract_code": keys[1],
                "session_type": keys[2],
                "session_anchor_date": keys[3],
                "session_id": keys[4],
                "first_bar_at": session.iloc[0]["bar_at"],
                "last_bar_at": session.iloc[-1]["bar_at"],
                "first_bar_open": session.iloc[0]["open"],
                "last_bar_close": session.iloc[-1]["close"],
                "session_high": session["high"].max(),
                "session_low": session["low"].min(),
                "session_volume": session["volume"].sum(),
                "minute_count": len(session),
            }
        )
    session_boundaries = pd.DataFrame(session_rows)

    comparison_rows = []
    for daily_row in daily_bars.itertuples(index=False):
        symbol_minutes = minute_bars.loc[
            minute_bars["contract_code"].eq(daily_row.contract_code)
        ].copy()
        previous_date = previous_trade_day[daily_row.trading_date]
        previous_cutoff = pd.Timestamp(datetime.combine(previous_date, time(15, 0)))
        current_cutoff = pd.Timestamp(
            datetime.combine(daily_row.trading_date, time(15, 0))
        )
        cycle = symbol_minutes.loc[
            (symbol_minutes["bar_at"] > previous_cutoff)
            & (symbol_minutes["bar_at"] <= current_cutoff)
        ].sort_values("bar_at")
        day_only = symbol_minutes.loc[
            symbol_minutes["bar_at"].dt.date.eq(daily_row.trading_date)
            & symbol_minutes["session_type"].eq("day")
        ].sort_values("bar_at")
        night_only = cycle.loc[cycle["session_type"].eq("night")]

        if cycle.empty or day_only.empty or night_only.empty:
            raise ValueError(
                f"{daily_row.contract_code} {daily_row.trading_date} "
                "缺少完整的夜盘或日盘分钟。"
            )

        cycle_open = cycle.iloc[0]["open"]
        cycle_high = cycle["high"].max()
        cycle_low = cycle["low"].min()
        cycle_close = cycle.iloc[-1]["close"]
        cycle_volume = cycle["volume"].sum()
        day_open = day_only.iloc[0]["open"]
        day_high = day_only["high"].max()
        day_low = day_only["low"].min()
        day_close = day_only.iloc[-1]["close"]
        day_volume = day_only["volume"].sum()
        night_open = night_only.iloc[0]["open"]
        night_close = night_only.iloc[-1]["close"]

        daily_values = np.array(
            [
                daily_row.open,
                daily_row.high,
                daily_row.low,
                daily_row.close,
                daily_row.volume,
            ],
            dtype="float64",
        )
        cycle_values = np.array(
            [cycle_open, cycle_high, cycle_low, cycle_close, cycle_volume],
            dtype="float64",
        )
        day_values = np.array(
            [day_open, day_high, day_low, day_close, day_volume],
            dtype="float64",
        )

        comparison_rows.append(
            {
                "underlying_code": daily_row.underlying_code,
                "contract_code": daily_row.contract_code,
                "trading_date": daily_row.trading_date,
                "previous_trading_date": previous_date,
                "daily_open": daily_row.open,
                "daily_high": daily_row.high,
                "daily_low": daily_row.low,
                "daily_close": daily_row.close,
                "daily_volume": daily_row.volume,
                "cycle_first_bar_at": cycle.iloc[0]["bar_at"],
                "cycle_first_session": cycle.iloc[0]["session_id"],
                "cycle_last_bar_at": cycle.iloc[-1]["bar_at"],
                "cycle_last_session": cycle.iloc[-1]["session_id"],
                "night_first_bar_at": night_only.iloc[0]["bar_at"],
                "night_last_bar_at": night_only.iloc[-1]["bar_at"],
                "night_open": night_open,
                "night_close": night_close,
                "day_first_bar_at": day_only.iloc[0]["bar_at"],
                "day_last_bar_at": day_only.iloc[-1]["bar_at"],
                "day_open": day_open,
                "day_close": day_close,
                "cycle_open": cycle_open,
                "cycle_high": cycle_high,
                "cycle_low": cycle_low,
                "cycle_close": cycle_close,
                "cycle_volume": cycle_volume,
                "day_only_high": day_high,
                "day_only_low": day_low,
                "day_only_volume": day_volume,
                "daily_open_matches_night_open": bool(
                    np.isclose(daily_row.open, night_open)
                ),
                "daily_open_matches_day_open": bool(
                    np.isclose(daily_row.open, day_open)
                ),
                "daily_close_matches_night_close": bool(
                    np.isclose(daily_row.close, night_close)
                ),
                "daily_close_matches_day_close": bool(
                    np.isclose(daily_row.close, day_close)
                ),
                "cycle_ohlcv_matches_daily": bool(
                    np.allclose(daily_values, cycle_values)
                ),
                "day_only_ohlcv_matches_daily": bool(
                    np.allclose(daily_values, day_values)
                ),
                "cycle_minute_count": len(cycle),
                "night_minute_count": len(night_only),
                "day_minute_count": len(day_only),
            }
        )

    comparisons = pd.DataFrame(comparison_rows).sort_values(
        ["underlying_code", "trading_date"]
    )

    contracts.to_csv(OUTPUT_DIR / "selected_contracts.csv", index=False, encoding="utf-8-sig")
    daily_bars.to_csv(OUTPUT_DIR / "daily_bars.csv", index=False, encoding="utf-8-sig")
    minute_bars.to_csv(OUTPUT_DIR / "minute_bars.csv", index=False, encoding="utf-8-sig")
    session_boundaries.to_csv(
        OUTPUT_DIR / "session_boundaries.csv", index=False, encoding="utf-8-sig"
    )
    comparisons.to_csv(
        OUTPUT_DIR / "daily_session_comparison.csv", index=False, encoding="utf-8-sig"
    )

    report_lines = [
        "# 期货夜盘归属实验结果",
        "",
        f"- 实验周：{START_DATE} 至 {END_DATE}",
        "- 数据源：JQData `get_price` 的同一固定月份合约日 K 与 1 分钟 K",
        "- 周期候选：上一交易日日盘收盘后至当前交易日日盘收盘",
        "- 对照候选：当前自然日仅日盘分钟",
        "",
        "## 合约与汇总",
        "",
        "| 品种 | 固定合约 | 日 K 数 | 日开=夜开 | 日开=日开 | 日收=夜收 | 日收=日收 | 完整周期 OHLCV 一致 | 仅日盘 OHLCV 一致 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for contract in contracts.itertuples(index=False):
        selected = comparisons.loc[
            comparisons["underlying_code"].eq(contract.underlying_code)
        ]
        report_lines.append(
            "| "
            f"{contract.underlying_code} | {contract.contract_code} | {len(selected)} | "
            f"{int(selected['daily_open_matches_night_open'].sum())}/{len(selected)} | "
            f"{int(selected['daily_open_matches_day_open'].sum())}/{len(selected)} | "
            f"{int(selected['daily_close_matches_night_close'].sum())}/{len(selected)} | "
            f"{int(selected['daily_close_matches_day_close'].sum())}/{len(selected)} | "
            f"{int(selected['cycle_ohlcv_matches_daily'].sum())}/{len(selected)} | "
            f"{int(selected['day_only_ohlcv_matches_daily'].sum())}/{len(selected)} |"
        )

    report_lines.extend(
        [
            "",
            "## 逐交易日边界",
            "",
            "| 品种 | 交易日 | 日 K 开盘 | 夜盘首分钟及开盘 | 日盘首分钟及开盘 | 日 K 收盘 | 夜盘末分钟及收盘 | 日盘末分钟及收盘 | 完整周期一致 |",
            "|---|---|---:|---|---|---:|---|---|---|",
        ]
    )
    for row in comparisons.itertuples(index=False):
        report_lines.append(
            "| "
            f"{row.underlying_code} | {row.trading_date} | {row.daily_open} | "
            f"{row.night_first_bar_at} / {row.night_open} | "
            f"{row.day_first_bar_at} / {row.day_open} | {row.daily_close} | "
            f"{row.night_last_bar_at} / {row.night_close} | "
            f"{row.day_last_bar_at} / {row.day_close} | "
            f"{'是' if row.cycle_ohlcv_matches_daily else '否'} |"
        )

    all_cycle_match = comparisons["cycle_ohlcv_matches_daily"].all()
    all_day_only_mismatch = (~comparisons["day_only_ohlcv_matches_daily"]).all()
    all_open_night = comparisons["daily_open_matches_night_open"].all()
    all_close_day = comparisons["daily_close_matches_day_close"].all()
    report_lines.extend(
        [
            "",
            "## 仅陈述本次样本结果",
            "",
            f"- 15 根日 K 的完整周期 OHLCV 是否全部与分钟聚合一致：{'是' if all_cycle_match else '否'}。",
            f"- 15 根日 K 的仅日盘 OHLCV 是否全部不一致：{'是' if all_day_only_mismatch else '否'}。",
            f"- 15 根日 K 的开盘是否全部等于候选夜盘首分钟开盘：{'是' if all_open_night else '否'}。",
            f"- 15 根日 K 的收盘是否全部等于当日日盘末分钟收盘：{'是' if all_close_day else '否'}。",
            "- 本报告不把样本结果上升为全历史规则；节假日、临时停盘和交易规则变更需要另行实验。",
            "",
        ]
    )
    (OUTPUT_DIR / "experiment_report.md").write_text(
        "\n".join(report_lines), encoding="utf-8"
    )

    print(f"python: {sys.executable}")
    print(f"output: {OUTPUT_DIR}")
    print(contracts.to_string(index=False))
    print()
    print(
        comparisons[
            [
                "underlying_code",
                "contract_code",
                "trading_date",
                "cycle_first_bar_at",
                "cycle_last_bar_at",
                "daily_open_matches_night_open",
                "daily_close_matches_day_close",
                "cycle_ohlcv_matches_daily",
                "day_only_ohlcv_matches_daily",
            ]
        ].to_string(index=False)
    )


if __name__ == "__main__":
    main()
