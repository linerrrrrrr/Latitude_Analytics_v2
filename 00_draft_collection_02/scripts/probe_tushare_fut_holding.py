from __future__ import annotations

from datetime import date
import pathlib
import sys

import tushare as ts

project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        break
else:
    raise RuntimeError("未找到项目根目录")

from config.settings import settings


TRADE_DATE = date(2026, 8, 3).strftime("%Y%m%d")
PROBES = [
    ("A", ["DCE"]),
    ("SC", ["SHFE", "INE"]),
    ("SI", ["GFEX", "GFE"]),
    ("RB", ["SHFE"]),
    ("SR", ["CZCE"]),
]


def main() -> None:
    pro = ts.pro_api(settings.tushare_token)
    for symbol, exchanges in PROBES:
        variants: list[tuple[str, str | None]] = [("symbol_only", None)]
        variants.extend((f"exchange={exchange}", exchange) for exchange in exchanges)
        for label, exchange in variants:
            arguments = {"trade_date": TRADE_DATE, "symbol": symbol}
            arguments["fields"] = (
                "trade_date,symbol,broker,vol,vol_chg,long_hld,long_chg,"
                "short_hld,short_chg,exchange"
            )
            if exchange is not None:
                arguments["exchange"] = exchange
            frame = pro.fut_holding(**arguments)
            exchange_values = (
                sorted(frame["exchange"].dropna().astype(str).unique().tolist())
                if frame is not None and "exchange" in frame
                else []
            )
            summary_labels = (
                sorted(
                    value
                    for value in frame["broker"].dropna().astype(str).unique()
                    if "会员" in value or "参与者" in value
                )
                if frame is not None and "broker" in frame
                else []
            )
            print(
                f"symbol={symbol} variant={label} rows={0 if frame is None else len(frame)} "
                f"response_exchanges={exchange_values} summary_labels={summary_labels}"
            )


if __name__ == "__main__":
    main()
