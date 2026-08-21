"""国内期货事实采集使用的交易所—品种白名单。

本模块是白名单的项目级唯一来源。
它只选择日线、分钟线和逐品种交易所报告事实，不定义或裁剪品种、合约及行情日历宇宙。
"""

from __future__ import annotations


FUTURES_FACT_VARIETIES_BY_EXCHANGE = {
    "GFEX": frozenset({"LC", "PD", "PS", "PT", "SI"}),
    "XDCE": frozenset(
        {
            "BB",
            "BZ",
            "EB",
            "EG",
            "FB",
            "I",
            "J",
            "JM",
            "L",
            "LG",
            "LH",
            "PG",
            "PP",
            "V",
        }
    ),
    "XINE": frozenset({"BC", "EC", "LU", "NR", "SC"}),
    "XSGE": frozenset(
        {
            "AD",
            "AG",
            "AL",
            "AO",
            "AU",
            "BR",
            "BU",
            "CU",
            "FU",
            "HC",
            "NI",
            "OP",
            "PB",
            "RB",
            "RU",
            "SN",
            "SP",
            "SS",
            "WR",
            "ZN",
        }
    ),
    "XZCE": frozenset(
        {
            "CY",
            "FG",
            "MA",
            "ME",
            "PF",
            "PL",
            "PR",
            "PX",
            "SA",
            "SF",
            "SH",
            "SM",
            "TA",
            "TC",
            "UR",
            "ZC",
        }
    ),
}

FUTURES_FACT_VARIETY_PAIRS = frozenset(
    (exchange_code, underlying_code)
    for exchange_code, underlying_codes in FUTURES_FACT_VARIETIES_BY_EXCHANGE.items()
    for underlying_code in underlying_codes
)

if len(FUTURES_FACT_VARIETY_PAIRS) != 60:
    raise RuntimeError(
        "国内期货事实采集白名单应包含 60 个交易所—品种组合，"
        f"实际为 {len(FUTURES_FACT_VARIETY_PAIRS)} 个。"
    )
