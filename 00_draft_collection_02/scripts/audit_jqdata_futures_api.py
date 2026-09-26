"""Live, non-destructive audit of the vendored JQData futures APIs."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "01_project_collection" / "jqdatasdk"))
sys.path.insert(0, str(ROOT))

from config.settings import settings  # noqa: E402
import jqdatasdk as jq  # noqa: E402


def describe(value):
    result = {"type": type(value).__name__}
    if isinstance(value, (pd.DataFrame, pd.Series)):
        result["shape"] = list(value.shape)
        result["empty"] = bool(value.empty)
        result["columns"] = list(value.columns) if isinstance(value, pd.DataFrame) else None
        result["dtypes"] = (
            {str(k): str(v) for k, v in value.dtypes.items()}
            if isinstance(value, pd.DataFrame) else str(value.dtype)
        )
        result["index_type"] = type(value.index).__name__
        result["index_names"] = list(value.index.names)
    elif isinstance(value, np.ndarray):
        result["shape"] = list(value.shape)
        result["dtype_names"] = list(value.dtype.names or [])
    elif isinstance(value, dict):
        result["len"] = len(value)
        result["keys_sample"] = list(value)[:5]
        if value:
            first_value = next(iter(value.values()))
            result["value_type"] = type(first_value).__name__
            if isinstance(first_value, dict):
                result["value_keys"] = list(first_value)
                result["value_types"] = {
                    str(k): type(v).__name__ for k, v in first_value.items()
                }
    elif isinstance(value, (list, tuple)):
        result["len"] = len(value)
        result["sample"] = list(value)[:5]
    elif isinstance(value, str):
        result["value"] = value
    elif value is None:
        result["value"] = None
    elif hasattr(value, "to_dict"):
        result["fields"] = {
            str(k): {"type": type(v).__name__, "value": str(v)}
            for k, v in value.to_dict().items()
        }
    elif type(value).__name__ == "Security":
        result["fields"] = {
            name: {"type": type(getattr(value, name, None)).__name__,
                   "value": str(getattr(value, name, None))}
            for name in ("code", "display_name", "name", "start_date",
                         "end_date", "type", "parent")
        }
    return result


def main():
    results = {}

    def check(name, fn):
        try:
            results[name] = {"status": "ok", **describe(fn())}
        except Exception as exc:
            results[name] = {
                "status": "error",
                "error_type": type(exc).__name__,
                "error": str(exc),
            }

    jq.auth(settings.jqdata_id, settings.jqdata_secret)
    try:
        check("get_all_securities", lambda: jq.get_all_securities(["futures"], "2024-05-06"))
        check("get_all_securities2_exchange", lambda: jq.get_all_securities2(
            ["futures"], "2024-05-06", ["XSGE", "XINE"]
        ))
        check("get_security_info", lambda: jq.get_security_info("AG2406.XSGE", "2024-05-06"))
        check("get_security_info_invalid", lambda: jq.get_security_info("NOT_A_FUTURE"))
        check("get_security_info2", lambda: jq.get_security_info2("AG2406.XSGE", "2024-05-06"))
        check("normalize_code_contract", lambda: jq.normalize_code("AG2406"))
        check("normalize_code_continuous", lambda: jq.normalize_code("AG9999"))
        check("get_future_contracts_upper", lambda: jq.get_future_contracts("AG", "2024-05-06"))
        check("get_future_contracts_lower", lambda: jq.get_future_contracts("ag", "2024-05-06"))
        check("get_future_contracts_invalid", lambda: jq.get_future_contracts("NOTREAL", "2024-05-06"))
        check("get_dominant_future_scalar", lambda: jq.get_dominant_future("AG", "2024-05-06"))
        check("get_dominant_future_invalid_scalar", lambda: jq.get_dominant_future("X", "2024-05-06"))
        check("get_dominant_future_range", lambda: jq.get_dominant_future(
            "AG", "2024-05-06", "2024-05-10"
        ))
        check("get_dominant_future_multi_scalar", lambda: jq.get_dominant_future(
            ["AG", "PX", "X"], "2024-05-06"
        ))
        check("get_dominant_future_multi_range", lambda: jq.get_dominant_future(
            ["AG", "RR"], "2019-08-16", "2019-08-20"
        ))
        check("get_dominant_future_all_scalar", lambda: jq.get_dominant_future(
            None, "2024-05-06"
        ))
        check("get_dominant_future_all_range", lambda: jq.get_dominant_future(
            None, "2024-05-06", "2024-05-10"
        ))
        check("get_price_daily_single", lambda: jq.get_price(
            "AG2406.XSGE", "2024-05-06", "2024-05-10", fq=None
        ))
        check("get_price_minute_single", lambda: jq.get_price(
            "AG2406.XSGE", "2024-05-06 21:00:00", "2024-05-06 21:05:00",
            frequency="1m", fq=None
        ))
        check("get_price_multi_panel_false", lambda: jq.get_price(
            ["AG2406.XSGE", "RB2410.XSGE"], "2024-05-06", "2024-05-10",
            fields=["close", "volume", "open_interest"], panel=False, fq=None
        ))
        check("get_price_no_data", lambda: jq.get_price(
            "AG2406.XSGE", "2000-01-01", "2000-01-03", fq=None
        ))
        check("get_price_dominant_continuous", lambda: jq.get_price(
            "AG8888.XSGE", "2024-05-06", "2024-05-10",
            fields=["close", "volume", "open_interest"], fq=None
        ))
        check("get_price_index_continuous", lambda: jq.get_price(
            "AG9999.XSGE", "2024-05-06", "2024-05-10",
            fields=["close", "volume", "open_interest"], fq=None
        ))
        check("get_extras_settlement_df", lambda: jq.get_extras(
            "futures_sett_price", "AG2406.XSGE", "2024-05-06", "2024-05-10"
        ))
        check("get_extras_positions_df", lambda: jq.get_extras(
            "futures_positions", "AG2406.XSGE", "2024-05-06", "2024-05-10"
        ))
        check("get_extras_settlement_dict", lambda: jq.get_extras(
            "futures_sett_price", ["AG2406.XSGE"], "2024-05-06", "2024-05-10", df=False
        ))
        check("get_ticks_df", lambda: jq.get_ticks(
            "AG2406.XSGE", end_dt="2024-05-07", count=5
        ))
        check("get_ticks_array", lambda: jq.get_ticks(
            "AG2406.XSGE", end_dt="2024-05-07", count=5, df=False
        ))
        check("get_ticks_no_data", lambda: jq.get_ticks(
            "AG2406.XSGE", "2000-01-01", "2000-01-02"
        ))
        check("get_bars_df", lambda: jq.get_bars(
            "AG2406.XSGE", count=5, unit="1d", end_dt="2024-05-10",
            fields=["date", "close", "volume", "open_interest"]
        ))
        check("get_bars_array", lambda: jq.get_bars(
            "AG2406.XSGE", count=5, unit="1d", end_dt="2024-05-10",
            fields=["date", "close"], df=False
        ))
        check("get_bars_multi_df", lambda: jq.get_bars(
            ["AG2406.XSGE", "RB2410.XSGE"], count=2, unit="1d",
            end_dt="2024-05-10"
        ))
        check("get_bars_no_data", lambda: jq.get_bars(
            "AG2406.XSGE", count=2, unit="1d", end_dt="2000-01-03"
        ))
        check("get_futures_info_single", lambda: jq.get_futures_info("AG2406.XSGE"))
        check("get_futures_info_multi", lambda: jq.get_futures_info(
            ["AG2406.XSGE", "RB2410.XSGE"]
        ))
        check("get_futures_info_invalid", lambda: jq.get_futures_info("NOT_A_FUTURE"))
        check("get_futures_info_partial_fields", lambda: jq.get_futures_info(
            "AG2406.XSGE", fields=["tick_size"]
        ))
        check("get_order_future_bar_daily", lambda: jq.get_order_future_bar(
            "AG", "1", "2024-05-06", "2024-05-10", unit="1d"
        ))
        check("get_order_future_bar_minute", lambda: jq.get_order_future_bar(
            "AG", "1", "2024-05-06 21:00:00", "2024-05-06 21:05:00",
            unit="1m", fields=["code", "date", "close", "volume"]
        ))
        check("get_order_future_bar_invalid_symbol", lambda: jq.get_order_future_bar(
            "NOTREAL", "1", "2024-05-06", "2024-05-10", unit="1d"
        ))
        check("get_current_tick_weekend", lambda: jq.get_current_tick("AG2608.XSGE"))
        check("get_today_tick_period_weekend", lambda: jq.get_today_tick_period("AG2608.XSGE"))
    finally:
        jq.logout()

    print(json.dumps(results, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
