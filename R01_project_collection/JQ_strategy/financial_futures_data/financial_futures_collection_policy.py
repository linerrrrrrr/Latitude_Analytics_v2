"""JQ_strategy 金融期货人工取数的项目局部范围策略。

本模块是本子项目金融期货白名单、当前支持频率、数据就绪时点与有界计划
规模的唯一可执行来源。它不调用 API、不读取数据湖、不定义数据库 Schema，
也不修改全项目正式事实采集白名单。
"""

from __future__ import annotations

from datetime import time


FINANCIAL_FUTURES_EXCHANGE_CODE = "CCFX"

FINANCIAL_FUTURES_UNDERLYING_CODES = frozenset(
    {
        "IC",
        "IF",
        "IH",
        "IM",
        "T",
        "TF",
        "TL",
        "TS",
    }
)

FINANCIAL_FUTURES_BAR_FREQUENCIES = ("1d", "1m")

# 第 04 项实测到交易日 12:50 已可见盘中数据，但不能证明当日日线已经最终完成。
# 因此继续采用聚宽材料给出的 T+1 00:01，作为完整日数据进入正常拉取计划的保守边界。
JOINQUANT_MARKET_TIME_ZONE = "Asia/Shanghai"
JOINQUANT_COMPLETED_DATA_READY_TIME = time(hour=0, minute=1)

# 第 03 轮实测的单合约分钟请求为 38,640 行。正式来源请求保守限制在
# 38,000 个理论键以内；一次人工传输还同时受总理论键、观察块和来源请求数约束。
# 2026-09-05 的真实回补中，264,000 个分钟键成功发布，而 499,800 个分钟键
# 在全部来源请求成功后触发聚宽 1 GiB 内核重启。协议硬上限继续保留 500,000，
# 以兼容已经提交的文件；正常规划采用更小的内存安全上限，不生成贴近硬上限的批次。
JOINQUANT_MAX_EXPECTED_KEYS_PER_SOURCE_REQUEST = 38_000
JOINQUANT_MAX_EXPECTED_KEYS_PER_TRANSFER = 500_000
JOINQUANT_MAX_PLANNED_EXPECTED_KEYS_PER_TRANSFER = 250_000
JOINQUANT_MAX_OBSERVATIONS_PER_TRANSFER = 50_000
JOINQUANT_MAX_SOURCE_REQUESTS_PER_TRANSFER = 64


if len(FINANCIAL_FUTURES_UNDERLYING_CODES) != 8:
    raise RuntimeError(
        "JQ_strategy 金融期货白名单应包含 8 个中金所品种，"
        f"实际为 {len(FINANCIAL_FUTURES_UNDERLYING_CODES)} 个。"
    )

if set(FINANCIAL_FUTURES_BAR_FREQUENCIES) != {"1d", "1m"}:
    raise RuntimeError("JQ_strategy 金融期货频率必须且只能为 1d 与 1m。")

if not (
    0 < JOINQUANT_MAX_EXPECTED_KEYS_PER_SOURCE_REQUEST
    <= JOINQUANT_MAX_PLANNED_EXPECTED_KEYS_PER_TRANSFER
    <= JOINQUANT_MAX_EXPECTED_KEYS_PER_TRANSFER
):
    raise RuntimeError(
        "聚宽单来源请求、正常规划和单文件协议上限必须为正且依次不减。"
    )

if JOINQUANT_MAX_OBSERVATIONS_PER_TRANSFER <= 0:
    raise RuntimeError("聚宽单文件观察块上限必须为正。")

if JOINQUANT_MAX_SOURCE_REQUESTS_PER_TRANSFER <= 0:
    raise RuntimeError("聚宽单文件来源请求数上限必须为正。")
