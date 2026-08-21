#!/usr/bin/env python
# coding: utf-8

# # c05_futures_daily
# 
# 目标表：`fact_futures_daily`。
# 
# 本入口以 `dim_futures_bar_calendar` 的完整日线理论格点为上游，先应用共享期货事实采集白名单，再自动计算：
# 
# ```text
# 白名单选中的上游有效格点
#     − 正式日线事实中已经完整落盘的格点
#     = 本次自动更新范围
# ```
# 
# 日线统一来自 JQData：`get_price(frequency="daily")` 提供价格、成交量、元计成交额与昨收；
# `get_extras("futures_sett_price")` 和 `get_extras("futures_positions")` 提供结算价与持仓量。
# 昨结、两种相对昨结涨跌和持仓变化由同一合约的上一有效交易日值派生。有限数之间的原始 OHLC
# 跨列关系异常不会被修写或删除：事实保留供应商原值，对应 1d 日历格点以 `warning` 留痕。
# NaN/Inf 和负数量仍拒绝落盘。白名单只改变事实选择状态，不删除理论格点；白名单缩减也不自动删除既有历史日线事实。
# 
# 命令启动即执行采集与内存校验；`--write` 只决定是否写入数据湖和回写日历状态。

# ## 项目定位与依赖
# 
# 先按项目统一标记定位根目录，再导入配置、权威 Arrow Schema 和共享期货事实采集白名单。根目录定位必须在配置导入之前完成，
# 这样从仓库根目录或任意业务子目录执行，读取的都仍是同一份 `.env` 和数据契约。
# 

# In[ ]:


from __future__ import annotations

# 标准库负责数值校验、路径管理、原子替换和运行批次标识。
import math
import pathlib
import shutil
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from types import ModuleType
from typing import Callable


# 只使用项目统一规定的三个标记定位根目录，保证从仓库任意子目录运行都一致。
project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()  # 当前工作目录

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        project_root = candidate_root
        break
else:
    raise RuntimeError("未找到项目根目录")


# 第三方库分别承担 CLI、表格转换和 Arrow/Parquet 数据集读写。
import click
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

# 业务代码只引用中央契约，不在 Notebook 中复制字段定义或正式湖路径。
from config.data_contracts import (
    FUTURES_BAR_CALENDAR_SCHEMA,
    FUTURES_DAILY_SCHEMA,
    arrow_to_pandas,
    empty_pandas,
    pandas_to_arrow,
)
# 配置在根目录定位完成后导入，避免把正式湖路径或凭据复制到 Notebook。
from config.settings import settings
from config.futures_fact_collection_policy import FUTURES_FACT_VARIETY_PAIRS


# ## Schema 契约交互浏览

# In[ ]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from config.notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        FUTURES_BAR_CALENDAR_SCHEMA,
        FUTURES_DAILY_SCHEMA,
    ])


# ## 表、主键、分区与 JQData 常量
# 
# 这一块只声明稳定边界，不执行 I/O。事实主键用于自动求差；Hive 分区用于完整叶分区提交；
# JQData 字段列表同时约束请求参数和响应校验。正式湖路径不在这里另写常量。
# 

# In[ ]:


# 下游事实与上游行情日历分别使用各自的权威 Arrow Schema。
SCHEMA = FUTURES_DAILY_SCHEMA  # 国内期货合约日线事实的权威 Arrow Schema。
UPSTREAM_SCHEMA = FUTURES_BAR_CALENDAR_SCHEMA  # 上游行情拉取与质检日历的权威 Schema。

# 表名、主键和 Hive 分区是自动求差与完整分区提交的稳定边界。
TABLE_NAME = "fact_futures_daily"  # 国内期货合约日线行情事实表。
UPSTREAM_TABLE_NAME = "dim_futures_bar_calendar"  # 上游期货行情拉取与质检日历表。

PRIMARY_KEY = [  # 唯一标识一个固定月份合约的交易日日线。
    "contract_code",  # JQData 标准固定月份合约代码。
    "trading_date",  # 日线归属的期货交易日。
]
UPSTREAM_PRIMARY_KEY = [  # 唯一标识上游日线或分钟 Session 格点。
    "bar_frequency",  # 行情频率；本入口只消费 1d。
    "contract_code",  # 固定月份合约代码。
    "trading_date",  # 行情归属交易日。
    "session_number",  # 日线格点固定为 0。
]

PARTITION_COLUMNS = [  # 日线事实的 Hive 叶分区层级。
    "exchange_code",  # 合约所属交易所。
    "underlying_code",  # 合约所属期货品种。
    "year",  # 交易年份。
    "month",  # 交易月份。
]
UPSTREAM_PARTITION_COLUMNS = [  # 上游行情日历的 Hive 叶分区层级。
    "bar_frequency",  # 1d 或 1m；本入口读取 1d。
    "exchange_code",  # 交易所代码。
    "year",  # 交易年份。
    "month",  # 交易月份。
]

# JQData 原始字段与最终事实度量分开列出；缺失占位时后者必须全部置空。
PRICE_FIELDS = [  # 传给 JQData get_price(daily) 的原始行情字段。
    "open",  # 开盘价。
    "high",  # 最高价。
    "low",  # 最低价。
    "close",  # 收盘价。
    "volume",  # 成交量，单位为手。
    "money",  # 成交额，JQData 返回单位为元。
    "pre_close",  # JQData 返回的昨收盘价。
]
MARKET_VALUE_COLUMNS = [  # 无有效行情时必须整体置空的事实度量。
    "previous_close",  # 上一有效交易日收盘价。
    "previous_settlement",  # 上一有效交易日结算价。
    "open",  # 当日开盘价。
    "high",  # 当日最高价。
    "low",  # 当日最低价。
    "close",  # 当日收盘价。
    "settlement",  # 当日结算价。
    "close_change_from_previous_settlement",  # 收盘价相对昨结算价的涨跌额。
    "settlement_change_from_previous_settlement",  # 结算价相对昨结算价的涨跌额。
    "volume",  # 当日成交量，单位为手。
    "money",  # 当日成交额，单位为元。
    "open_interest",  # 当日收盘持仓量，单位为手。
    "open_interest_change",  # 相对上一有效交易日的持仓变化。
]

SOURCE_NAME = "JQData_get_price_daily_get_extras_raw"
LEGACY_SOURCE_NAMES = {"JQData_get_price_1d_skip_paused"}
LEGACY_NULL_EXTRA_FIELDS = [
    "previous_close",
    "previous_settlement",
    "settlement",
    "close_change_from_previous_settlement",
    "settlement_change_from_previous_settlement",
    "open_interest_change",
]
SELECTED_REASON = "命中国内期货事实采集白名单；日线事实需要采集。"
EXCLUDED_REASON = (
    "未命中国内期货事实采集白名单；保留理论格点但不采集日线事实，"
    "既有历史日线事实不自动删除。"
)

# 批大小同时受合约数和保守返回量估计约束；配额保留量可由 CLI 调整。
MAX_CONTRACTS_PER_REQUEST = 100
MAX_ESTIMATED_VALUES_PER_BATCH = 90_000
LOOKBACK_CALENDAR_DAYS = 45
DEFAULT_QUOTA_RESERVE = 5_000_000

# 下列枚举属于 c04 行情日历契约；c05 回写完整分区时必须继续维护它们。
SCHEDULE_STATUSES = {
    "scheduled",
    "suspected_closed",
    "confirmed_closed",
}
EVIDENCE_LEVELS = {
    "contract_rule",
    "inferred",
    "reconciled",
    "authoritative",
}
QUALITY_STATUSES = {
    "pending",
    "passed",
    "warning",
    "failed",
    "not_applicable",
}

# 显式声明 Hive 分区字段类型，避免目录字符串推断改变契约类型。
HIVE_PARTITIONING = ds.partitioning(
    pa.schema([SCHEMA.field(name) for name in PARTITION_COLUMNS]),
    flavor="hive",
)
UPSTREAM_PARTITIONING = ds.partitioning(
    pa.schema(
        [UPSTREAM_SCHEMA.field(name) for name in UPSTREAM_PARTITION_COLUMNS]
    ),
    flavor="hive",
)


# ## 上游有效日线格点校验
# 
# `c04` 已对行情日历承担完整生产者质检。`c05` 作为消费者只检查自己直接依赖的边界：
# 必须是 `1d` 格点、每个合约—交易日只有一行，并且分区和合约代码一致；是否需要拉取由 c05 随后应用白名单决定。
# 

# In[ ]:


def validate_upstream_frame(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    # 消费者只确认 c05 直接依赖的 1d 主键、范围和结构边界，
    # 不在这里复制 c04 已经承担的完整来源与派生规则。
    # Arrow 转换先固定字段类型和列顺序，再进入集合与逐行检查。
    table = pandas_to_arrow(
        frame.loc[:, UPSTREAM_SCHEMA.names],
        UPSTREAM_SCHEMA,
    )
    checked_df = arrow_to_pandas(table, UPSTREAM_SCHEMA)

    # 自动差集以该主键为集合元素，重复键会让完成状态失去唯一含义。
    if checked_df.duplicated(UPSTREAM_PRIMARY_KEY).any():
        raise ValueError(f"{context}主键不唯一。")

    # 转成 Python 标量后逐行检查，日期和可空字段的含义更直观。
    for row in table.to_pylist():
        if row["bar_frequency"] != "1d":
            raise ValueError(f"{context}只能包含 1d 格点。")
        if row["session_number"] != 0:
            raise ValueError(f"{context}日线 session_number 必须为 0。")
        if row["expected_bar_count"] != 1:
            raise ValueError(f"{context}日线 expected_bar_count 必须为 1。")
        # 日线格点不对应具体 Session；Session 专属字段只能由 1m 行使用。
        session_values = [
            row["session_text"],
            row["session_start_at"],
            row["session_end_at"],
            row["is_night_session"],
        ]
        if any(value is not None for value in session_values):
            raise ValueError(f"{context}日线 Session 专属字段必须为空。")

        # 代码后缀和日期分区必须能由主键本身复算，避免写入错误叶分区。
        if not row["contract_code"].endswith(f".{row['exchange_code']}"):
            raise ValueError(f"{context}交易所与合约代码后缀不一致。")
        if (
            row["year"] != row["trading_date"].year
            or row["month"] != row["trading_date"].month
        ):
            raise ValueError(f"{context}year/month 与 trading_date 不一致。")

    return checked_df.sort_values(UPSTREAM_PRIMARY_KEY).reset_index(drop=True)


# ## 日线事实完整性校验
# 
# 一行只有同时满足 Arrow Schema/metadata、主键、来源、有限数、非负度量、缺失占位和派生值规则，
# 才属于“已经完整落盘”。有限数之间的 OHLC 跨列异常不再否定事实完整性，而由日历 `warning` 留痕；
# 校验仍用于现有事实求差、转换结果、staging 和正式路径复读。
# 

# In[ ]:


def values_close(left: float, right: float) -> bool:
    # 只吸收浮点减法的微小舍入误差，不放宽真实业务差异。
    return math.isclose(left, right, rel_tol=1e-10, abs_tol=1e-8)


def has_invalid_ohlc_relationship(row: dict[str, object]) -> bool:
    # 这里只识别有限原始价格之间的跨列关系；非有限数由事实 validator 单独拒绝。
    prices = [row["open"], row["close"]]

    comparable_for_high = [value for value in prices if value is not None]
    if row["low"] is not None:
        comparable_for_high.append(row["low"])
    high_is_invalid = (
        row["high"] is not None
        and bool(comparable_for_high)
        and row["high"] < max(comparable_for_high)
    )

    comparable_for_low = [value for value in prices if value is not None]
    if row["high"] is not None:
        comparable_for_low.append(row["high"])
    low_is_invalid = (
        row["low"] is not None
        and bool(comparable_for_low)
        and row["low"] > min(comparable_for_low)
    )

    return bool(high_is_invalid or low_is_invalid)


# In[ ]:


# 浮点派生值来自减法，比较时允许极小的二进制舍入误差。
def validate_daily_frame(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    # 必须在 Arrow 转换前检查来源数值；否则 Pandas NaN 会被 Arrow 静默转成 null。
    # 真正的 Python None / pd.NA 仍按 Schema 的 nullable 语义处理，NaN/Inf 则明确拒绝。
    source_frame = frame.loc[:, SCHEMA.names]
    for column in MARKET_VALUE_COLUMNS:
        for value in source_frame[column]:
            if value is None or value is pd.NA:
                continue
            try:
                is_finite = math.isfinite(float(value))
            except (TypeError, ValueError):
                # 非数值类型交给紧随其后的权威 Arrow 类型转换给出契约错误。
                continue
            if not is_finite:
                raise ValueError(f"{context}{column} 包含非有限数。")

    # 通过 Arrow 契约统一列顺序、类型、可空性和 metadata，再检查其余业务规则。
    table = pandas_to_arrow(source_frame, SCHEMA)
    checked_df = arrow_to_pandas(table, SCHEMA)

    # 一份正式事实对每个合约—交易日只能给出一个确定结论。
    if checked_df.duplicated(PRIMARY_KEY).any():
        raise ValueError(f"{context}主键不唯一。")
    if checked_df.empty:
        return checked_df

    # 跨日派生检查依赖稳定顺序，因此先按合约和交易日排序。
    rows = sorted(
        table.to_pylist(),
        key=lambda row: (row["contract_code"], row["trading_date"]),
    )
    # 映射始终保存同一合约最近一个非空值；占位日不会截断派生序列。
    previous_settlement_by_contract: dict[str, float] = {}
    previous_position_by_contract: dict[str, float] = {}

    for row in rows:
        contract_code = row["contract_code"]
        contract_symbol = contract_code.split(".", maxsplit=1)[0]
        derived_underlying = "".join(
            character
            for character in contract_symbol
            if character.isalpha()
        ).upper()

        # 第一组规则只校验身份与分区，不涉及行情值本身。
        if not contract_code.endswith(f".{row['exchange_code']}"):
            raise ValueError(f"{context}交易所与合约代码后缀不一致。")
        if row["underlying_code"] != derived_underlying:
            raise ValueError(f"{context}品种代码无法由合约代码复算。")
        if (
            row["year"] != row["trading_date"].year
            or row["month"] != row["trading_date"].month
        ):
            raise ValueError(f"{context}year/month 与 trading_date 不一致。")
        if row["source"] not in {SOURCE_NAME, *LEGACY_SOURCE_NAMES}:
            raise ValueError(f"{context}source 与 JQData 日线契约不一致。")
        legacy_without_extras = (
            row["source"] in LEGACY_SOURCE_NAMES
            and all(row[name] is None for name in LEGACY_NULL_EXTRA_FIELDS)
        )

        # 所有非空行情度量必须是有限数；NaN/Inf 不得进入正式事实。
        for column in MARKET_VALUE_COLUMNS:
            value = row[column]
            if value is not None and not math.isfinite(float(value)):
                raise ValueError(f"{context}{column} 包含非有限数。")

        # 无有效收盘价代表这次请求得到明确空结果，而不是删除上游格点。
        if row["has_market_data"]:
            if row["close"] is None:
                raise ValueError(f"{context}有效行情行必须具有 close。")
        elif any(row[column] is not None for column in MARKET_VALUE_COLUMNS):
            raise ValueError(f"{context}缺失占位行的行情度量必须全部为空。")

        # 数量和金额可以为空或为零，但正式保存的非空值不得为负。
        if row["volume"] is not None and row["volume"] < 0:
            raise ValueError(f"{context}volume 不得为负。")
        if row["money"] is not None and row["money"] < 0:
            raise ValueError(f"{context}money 不得为负。")
        if row["open_interest"] is not None and row["open_interest"] < 0:
            raise ValueError(f"{context}open_interest 不得为负。")

        # 有限 OHLC 跨列异常保留供应商原值；状态派生会把对应日历格点标为 warning。
        # 因而这里不修写价格，也不把该关系异常误当作事实未完整落盘。

        # 两种涨跌都以昨结为共同基准，必须能由保存的原始价格复算。
        previous_settlement = row["previous_settlement"]
        close_value = row["close"]
        settlement = row["settlement"]

        if (
            not legacy_without_extras
            and previous_settlement is not None
            and close_value is not None
        ):
            expected_change = close_value - previous_settlement
            actual_change = row["close_change_from_previous_settlement"]
            if actual_change is None or not values_close(
                actual_change,
                expected_change,
            ):
                raise ValueError(f"{context}收盘价相对昨结涨跌额无法复算。")
        elif (
            not legacy_without_extras
            and row["close_change_from_previous_settlement"] is not None
        ):
            raise ValueError(f"{context}收盘涨跌缺少必要输入。")

        if (
            not legacy_without_extras
            and previous_settlement is not None
            and settlement is not None
        ):
            expected_change = settlement - previous_settlement
            actual_change = row[
                "settlement_change_from_previous_settlement"
            ]
            if actual_change is None or not values_close(
                actual_change,
                expected_change,
            ):
                raise ValueError(f"{context}结算价相对昨结涨跌额无法复算。")
        elif (
            not legacy_without_extras
            and row["settlement_change_from_previous_settlement"] is not None
        ):
            raise ValueError(f"{context}结算涨跌缺少必要输入。")

        # 接着核对当前行保存的昨结是否等于本表前一条有效结算价。
        known_previous_settlement = previous_settlement_by_contract.get(
            contract_code
        )
        if (
            not legacy_without_extras
            and known_previous_settlement is not None
            and previous_settlement is not None
            and not values_close(
                previous_settlement,
                known_previous_settlement,
            )
        ):
            raise ValueError(f"{context}previous_settlement 与前序事实不一致。")

        # 持仓变化采用相同的前序有效值规则，不能直接与自然日前一日比较。
        known_previous_position = previous_position_by_contract.get(contract_code)
        if (
            not legacy_without_extras
            and known_previous_position is not None
            and row["open_interest"] is not None
        ):
            expected_change = row["open_interest"] - known_previous_position
            actual_change = row["open_interest_change"]
            if actual_change is None or not values_close(
                actual_change,
                expected_change,
            ):
                raise ValueError(f"{context}持仓变化无法由前序事实复算。")

        # 最后推进合约级状态；空值不覆盖最近一次可用的结算价或持仓量。
        if settlement is not None:
            previous_settlement_by_contract[contract_code] = settlement
        if row["open_interest"] is not None:
            previous_position_by_contract[contract_code] = row["open_interest"]

    return checked_df.sort_values(PRIMARY_KEY).reset_index(drop=True)


def daily_calendar_result(
    fact_row: dict[str, object],
) -> tuple[int, int, str, str]:
    # 日历的实际/缺失计数按物理事实是否存在计算；OHLC 异常不伪装成缺失。
    if not fact_row["has_market_data"]:
        return (
            0,
            1,
            "warning",
            "JQData 请求完成但没有有效收盘价；事实以缺失占位行正式提交。",
        )

    if has_invalid_ohlc_relationship(fact_row):
        return (
            1,
            0,
            "warning",
            "JQData 日线事实已正式提交并复读；供应商原始 OHLC 跨列关系异常，原值已保留。",
        )

    return (
        1,
        0,
        "passed",
        "JQData 日线事实已正式提交并复读，实际条数为 1。",
    )


# ## 行情日历状态完整性校验
# 
# 状态回写会替换 `dim_futures_bar_calendar` 的完整叶分区，因此不能只检查被修改的三两个字段。
# 这一块维护日线/分钟共存、完成审计、缺失计数和质量时间等整张日历表的不变量。
# 

# In[ ]:


def validate_calendar_state_frame(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    # c05 会改写 c04 表的完整叶分区，因此提交前后必须维护该表全部状态不变量。
    table = pandas_to_arrow(
        frame.loc[:, UPSTREAM_SCHEMA.names],
        UPSTREAM_SCHEMA,
    )
    checked_df = arrow_to_pandas(table, UPSTREAM_SCHEMA)

    if checked_df.duplicated(UPSTREAM_PRIMARY_KEY).any():
        raise ValueError(f"{context}主键不唯一。")

    # 完整分区同时包含 1d 和 1m 语义，逐行维护 c04 已建立的不变量。
    for row in table.to_pylist():
        if row["bar_frequency"] not in {"1d", "1m"}:
            raise ValueError(f"{context}bar_frequency 不在允许枚举中。")
        if row["schedule_status"] not in SCHEDULE_STATUSES:
            raise ValueError(f"{context}schedule_status 不在允许枚举中。")
        if row["evidence_level"] not in EVIDENCE_LEVELS:
            raise ValueError(f"{context}evidence_level 不在允许枚举中。")
        if row["quality_status"] not in QUALITY_STATUSES:
            raise ValueError(f"{context}quality_status 不在允许枚举中。")

        # 这些说明字段承担可审计语义，不能用空字符串绕过状态解释。
        text_fields = [
            "schedule_signal_reason",
            "evidence_source",
            "selection_reason",
            "quality_reason",
        ]
        if any(not str(row[name]).strip() for name in text_fields):
            raise ValueError(f"{context}状态说明字段不得为空。")

        # 代码后缀和日期分区必须能由主键本身复算，避免写入错误叶分区。
        if not row["contract_code"].endswith(f".{row['exchange_code']}"):
            raise ValueError(f"{context}交易所与合约代码后缀不一致。")
        if (
            row["year"] != row["trading_date"].year
            or row["month"] != row["trading_date"].month
        ):
            raise ValueError(f"{context}year/month 与 trading_date 不一致。")
        if row["expected_bar_count"] <= 0:
            raise ValueError(f"{context}expected_bar_count 必须大于 0。")
        if row["actual_bar_count"] < 0 or row["missing_bar_count"] < 0:
            raise ValueError(f"{context}实际与缺失条数不得为负。")

        # 1d 使用合约—交易日粒度；Session 专属字段只属于 1m 行。
        # 日线与分钟线的结构规则在这里分支，但仍由同一完整分区提交。
        if row["bar_frequency"] == "1d":
            session_values = [
                row["session_text"],
                row["session_start_at"],
                row["session_end_at"],
                row["is_night_session"],
            ]
            if row["session_number"] != 0:
                raise ValueError(f"{context}日线 session_number 必须为 0。")
            if any(value is not None for value in session_values):
                raise ValueError(f"{context}日线 Session 专属字段必须为空。")
            if row["expected_bar_count"] != 1:
                raise ValueError(f"{context}日线理论条数必须为 1。")
        else:
            if row["session_number"] <= 0:
                raise ValueError(f"{context}分钟 session_number 必须大于 0。")
            if row["session_start_at"] >= row["session_end_at"]:
                raise ValueError(f"{context}分钟 Session 起点必须早于终点。")

        # 权威休市是唯一可以取消既有拉取要求的日历结论。
        if row["schedule_status"] == "confirmed_closed":
            if row["evidence_level"] != "authoritative":
                raise ValueError(f"{context}确认休市必须具有权威证据。")
            if row["is_fetch_required"]:
                raise ValueError(f"{context}确认休市不得继续要求拉取。")

        # 完成标志必须同时具有可审计的运行批次和正式复读完成时间。
        # 完成状态必须和批次、完成时间共同出现，避免只有布尔值而无法审计。
        if row["is_fetch_completed"]:
            if not row["fetch_run_id"] or row["fetch_completed_at"] is None:
                raise ValueError(f"{context}完成状态缺少批次或完成时间。")
        elif row["fetch_completed_at"] is not None:
            raise ValueError(f"{context}未完成格点不得具有完成时间。")

        # 缺失结论必须由期望条数和正式事实实际条数复算。
        if row["missing_checked_at"] is None:
            if row["is_data_missing"] or row["missing_bar_count"] != 0:
                raise ValueError(f"{context}未经检查不得记录缺失。")
        else:
            expected_missing = (
                max(row["expected_bar_count"] - row["actual_bar_count"], 0)
                if row["is_fetch_required"]
                else 0
            )
            if row["missing_bar_count"] != expected_missing:
                raise ValueError(f"{context}missing_bar_count 无法复算。")
            if row["is_data_missing"] != (expected_missing > 0):
                raise ValueError(f"{context}is_data_missing 与缺失条数不一致。")

        # 非 pending 质检结论必须带检查时间，保持状态和审计时间同步。
        if (
            row["quality_status"] != "pending"
            and row["quality_checked_at"] is None
        ):
            raise ValueError(f"{context}非 pending 状态缺少质检时间。")

    return checked_df.sort_values(UPSTREAM_PRIMARY_KEY).reset_index(drop=True)


# ## 契约化读取数据集
# 
# 精确 metadata 用于判定事实是否已经完整。旧事实若字段名、类型和可空性兼容但 metadata 过期，
# 可以读取后通过重写修复；上游日历则必须直接符合当前权威契约。
# 

# In[ ]:


# 将目录分区字段与 Parquet 文件字段重新组合成可和权威契约比较的完整 Schema。
def reconstructed_schema(
    dataset: ds.Dataset,
    schema: pa.Schema,
) -> pa.Schema:
    try:
        fields = [dataset.schema.field(name) for name in schema.names]
    except KeyError as error:
        raise TypeError("数据集缺少契约字段。") from error

    return pa.schema(fields, metadata=dataset.schema.metadata)


# In[ ]:


# metadata 版本升级时只允许字段名、类型和可空性完全兼容。
def physically_compatible(
    actual_schema: pa.Schema,
    expected_schema: pa.Schema,
) -> bool:
    if actual_schema.names != expected_schema.names:
        return False

    return all(
        actual_field.type == expected_field.type
        and actual_field.nullable == expected_field.nullable
        for actual_field, expected_field in zip(
            actual_schema,
            expected_schema,
            strict=True,
        )
    )


# In[ ]:


# 统一打开契约化数据集；required 控制空湖是否允许，metadata 升级只对本表旧事实开放。
def open_contract_dataset(
    table_path: pathlib.Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    label: str,
    *,
    required: bool,
    allow_metadata_upgrade: bool = False,
) -> tuple[ds.Dataset | None, str | None]:
    # 目录存在但没有 Parquet 文件，与下游表尚未创建具有相同含义。
    parquet_files = (
        list(table_path.rglob("*.parquet"))
        if table_path.is_dir()
        else []
    )
    if not parquet_files:
        if required:
            raise FileNotFoundError(f"{label}不存在：{table_path}")
        return None, None

    dataset = ds.dataset(
        table_path,
        format="parquet",
        partitioning=partitioning,
    )
    actual_schema = reconstructed_schema(dataset, schema)

    # 正常情况要求精确 metadata；仅目标事实允许识别物理兼容的旧 metadata 并重建。
    if actual_schema.equals(schema, check_metadata=True):
        return dataset, None
    if allow_metadata_upgrade and physically_compatible(actual_schema, schema):
        return dataset, "metadata 与当前契约不一致。"

    raise TypeError(f"{label} Schema/metadata 与契约不一致。")


# In[ ]:


# 只读取契约列，并立即转换成契约化 Pandas 表，避免后续步骤携带额外列。
def read_dataset_frame(
    dataset: ds.Dataset,
    schema: pa.Schema,
    filter_expression: ds.Expression | None = None,
) -> pd.DataFrame:
    table = dataset.to_table(
        columns=schema.names,
        filter=filter_expression,
    )
    return arrow_to_pandas(table, schema)


# ## 日期视图、键集合与状态修复
# 
# 显式日期只用于检查或独立测试湖，不改变自动生产模式的完整上游范围。
# 事实键已经完整、但日历完成状态滞后时，单独形成状态修复集合，避免重复请求 JQData。
# 

# In[ ]:


def date_limited_frame(
    frame: pd.DataFrame,
    date_column: str,
    start_date: date | None,
    end_date: date | None,
) -> pd.DataFrame:
    # 显式日期只形成检查视图；自动生产模式始终保留完整上游范围。
    if start_date is None:
        return frame.reset_index(drop=True)

    selected = frame.loc[
        frame[date_column].map(
            lambda value: start_date <= value <= end_date
        )
    ]
    return selected.reset_index(drop=True)


# In[ ]:


# 集合中的元素始终是不可变元组，便于直接执行上游减下游。
def key_set(
    frame: pd.DataFrame,
    columns: list[str],
) -> set[tuple[object, ...]]:
    return set(frame[columns].itertuples(index=False, name=None))


# In[ ]:


# 从原表取回集合命中的完整行；空集合保留原 Schema 而不临时造列。
def rows_for_keys(
    frame: pd.DataFrame,
    columns: list[str],
    selected_keys: set[tuple[object, ...]],
) -> pd.DataFrame:
    if not selected_keys:
        return frame.iloc[0:0].copy()

    row_keys = frame[columns].apply(tuple, axis=1)
    return frame.loc[row_keys.isin(selected_keys)].reset_index(drop=True)


# In[ ]:


def state_repair_keys(
    upstream_df: pd.DataFrame,
    fact_df: pd.DataFrame,
) -> set[tuple[object, ...]]:
    if fact_df.empty:
        return set()

    # 事实已经完整时，只比较它应对应的日历完成、缺失和质检状态。
    # 状态滞后不会触发第二次 API 请求。
    fact_rows = {
        (row["contract_code"], row["trading_date"]): row
        for row in pandas_to_arrow(fact_df, SCHEMA).to_pylist()
    }
    repairs = set()

    for row in pandas_to_arrow(upstream_df, UPSTREAM_SCHEMA).to_pylist():
        key = (row["contract_code"], row["trading_date"])
        fact_row = fact_rows.get(key)
        if fact_row is None:
            continue

        # 空占位和有限 OHLC 关系异常都形成 warning，但只有空占位计入缺失。
        (
            actual_count,
            missing_count,
            expected_quality,
            expected_reason,
        ) = daily_calendar_result(fact_row)

        state_is_current = (
            row["is_fetch_completed"]
            and row["actual_bar_count"] == actual_count
            and row["is_data_missing"] == (missing_count > 0)
            and row["missing_bar_count"] == missing_count
            and bool(row["fetch_run_id"])
            and row["fetch_completed_at"] is not None
            and row["missing_checked_at"] is not None
            and row["quality_status"] == expected_quality
            and row["quality_reason"] == expected_reason
            and row["quality_checked_at"] is not None
        )
        if not state_is_current:
            repairs.add(key)

    return repairs


# ## 日线事实白名单评估与选择状态
# 
# c05 在完整 `1d` 理论日历上应用权威期货事实白名单。白名单只改变事实选择和状态，不删除理论格点；白名单缩减也不自动删除既有历史日线事实。
# 

# In[ ]:


def apply_daily_policy(
    calendar_df: pd.DataFrame,
    start_date: date | None,
    end_date: date | None,
    updated_at: datetime,
) -> tuple[pd.DataFrame, set[tuple[object, ...]]]:
    calendar_df = validate_upstream_frame(calendar_df, "日线政策评估前日历")
    changed_keys = set()

    for index, row in calendar_df.iterrows():
        if start_date is not None and not (
            start_date <= row["trading_date"] <= end_date
        ):
            continue
        if row["schedule_status"] == "confirmed_closed":
            continue

        should_fetch = (
            row["exchange_code"],
            row["underlying_code"],
        ) in FUTURES_FACT_VARIETY_PAIRS
        desired_reason = SELECTED_REASON if should_fetch else EXCLUDED_REASON
        state_changed = row["is_fetch_required"] != should_fetch
        reason_changed = row["selection_reason"] != desired_reason
        excluded_state_stale = (
            not should_fetch
            and (
                row["is_fetch_completed"]
                or row["actual_bar_count"] != 0
                or row["missing_bar_count"] != 0
                or row["quality_status"] != "not_applicable"
            )
        )
        if not (state_changed or reason_changed or excluded_state_stale):
            continue

        calendar_df.at[index, "is_fetch_required"] = should_fetch
        calendar_df.at[index, "selection_reason"] = desired_reason
        if state_changed or not should_fetch:
            calendar_df.at[index, "is_fetch_completed"] = False
            calendar_df.at[index, "actual_bar_count"] = 0
            calendar_df.at[index, "is_data_missing"] = False
            calendar_df.at[index, "missing_bar_count"] = 0
            calendar_df.at[index, "fetch_run_id"] = None
            calendar_df.at[index, "fetch_completed_at"] = None
            calendar_df.at[index, "missing_checked_at"] = (
                None if should_fetch else updated_at
            )
            calendar_df.at[index, "quality_status"] = (
                "pending" if should_fetch else "not_applicable"
            )
            calendar_df.at[index, "quality_reason"] = (
                "日线事实采集政策已选择该格点，等待事实提交与复读。"
                if should_fetch
                else EXCLUDED_REASON
            )
            calendar_df.at[index, "quality_checked_at"] = (
                None if should_fetch else updated_at
            )

        calendar_df.at[index, "updated_at"] = updated_at
        changed_keys.add(
            tuple(calendar_df.at[index, name] for name in UPSTREAM_PRIMARY_KEY)
        )

    return (
        validate_calendar_state_frame(calendar_df, "日线政策评估后日历"),
        changed_keys,
    )


def commit_daily_policy_partitions(
    calendar_df: pd.DataFrame,
    changed_keys: set[tuple[object, ...]],
    lake_root: pathlib.Path,
) -> int:
    if not changed_keys:
        return 0

    key_series = calendar_df[UPSTREAM_PRIMARY_KEY].apply(tuple, axis=1)
    changed_df = calendar_df.loc[key_series.isin(changed_keys)]
    partition_keys = set(
        changed_df[UPSTREAM_PARTITION_COLUMNS].itertuples(
            index=False,
            name=None,
        )
    )
    for partition_key in sorted(partition_keys):
        mask = pd.Series(True, index=calendar_df.index)
        for column, value in zip(
            UPSTREAM_PARTITION_COLUMNS,
            partition_key,
            strict=True,
        ):
            mask &= calendar_df[column].eq(value)
        complete_df = validate_calendar_state_frame(
            calendar_df.loc[mask, UPSTREAM_SCHEMA.names],
            "日线政策更新后的完整日历分区",
        )
        commit_complete_partition(
            complete_df,
            lake_root,
            UPSTREAM_TABLE_NAME,
            UPSTREAM_SCHEMA,
            UPSTREAM_PARTITION_COLUMNS,
            UPSTREAM_PARTITIONING,
            partition_key,
            validate_calendar_state_frame,
        )
    return len(changed_keys)


# ## JQData 请求分批与配额预检
# 
# 待办先按交易所和年份分组，再同时受每批合约数与估计返回量限制。请求范围向前回看 45 个自然日，
# 用于取得本批首个待办日所需的上一有效结算价和持仓量；回看日期不会成为输出键。
# 

# In[ ]:


def request_batches(
    pending_df: pd.DataFrame,
) -> list[dict[str, object]]:
    batches = []

    # 先按交易所和年份隔离请求，便于定位错误，也控制最长日期跨度。
    for (exchange_code, year), group_df in pending_df.groupby(
        ["exchange_code", "year"],
        sort=True,
    ):
        contract_codes = sorted(group_df["contract_code"].unique().tolist())
        current_codes: list[str] = []

        # 逐个试放合约；超过任一安全边界时先落定上一批。
        for contract_code in contract_codes:
            tentative_codes = [*current_codes, contract_code]
            tentative_df = group_df.loc[
                group_df["contract_code"].isin(tentative_codes)
            ]
            request_start = (
                tentative_df["trading_date"].min()
                - timedelta(days=LOOKBACK_CALENDAR_DAYS)
            )
            request_end = tentative_df["trading_date"].max()
            natural_days = (request_end - request_start).days + 1
            estimated_values = natural_days * len(tentative_codes) * 3

            # 任一安全边界达到就结束当前批；单合约即使跨度大也必须能够执行。
            should_flush = current_codes and (
                len(tentative_codes) > MAX_CONTRACTS_PER_REQUEST
                or estimated_values > MAX_ESTIMATED_VALUES_PER_BATCH
            )
            if should_flush:
                # 当前合约留给下一批，已经累计的合约先形成一个完整请求计划。
                selected_df = group_df.loc[
                    group_df["contract_code"].isin(current_codes)
                ].reset_index(drop=True)
                start_value = (
                    selected_df["trading_date"].min()
                    - timedelta(days=LOOKBACK_CALENDAR_DAYS)
                )
                end_value = selected_df["trading_date"].max()
                batches.append(
                    {
                        "exchange_code": exchange_code,
                        "year": int(year),
                        "contract_codes": current_codes,
                        "pending_df": selected_df,
                        "request_start": start_value,
                        "request_end": end_value,
                        "estimated_values": (
                            (end_value - start_value).days + 1
                        )
                        * len(current_codes)
                        * 3,
                    }
                )
                current_codes = []

            current_codes.append(contract_code)

        if current_codes:
            # 分组末尾仍未触发阈值的合约组成最后一批。
            selected_df = group_df.loc[
                group_df["contract_code"].isin(current_codes)
            ].reset_index(drop=True)
            start_value = (
                selected_df["trading_date"].min()
                - timedelta(days=LOOKBACK_CALENDAR_DAYS)
            )
            end_value = selected_df["trading_date"].max()
            batches.append(
                {
                    "exchange_code": exchange_code,
                    "year": int(year),
                    "contract_codes": current_codes,
                    "pending_df": selected_df,
                    "request_start": start_value,
                    "request_end": end_value,
                    "estimated_values": (
                        (end_value - start_value).days + 1
                    )
                    * len(current_codes)
                    * 3,
                }
            )

    return batches


# In[ ]:


def quota_spare(jqdata: ModuleType) -> int | None:
    get_query_count = getattr(jqdata, "get_query_count", None)
    if get_query_count is None:
        return None

    # 旧版 SDK 可能没有配额查询；此时不凭空假定一个剩余额度。
    # 只读取 spare；total 不能代表当前仍可消费的返回量。
    quota = get_query_count()
    if isinstance(quota, dict) and quota.get("spare") is not None:
        return int(quota["spare"])
    return None


# ## JQData 响应归一化
# 
# `get_price(panel=False)` 返回长表，而 `get_extras` 返回日期×合约宽表。
# 这里先把三类返回统一成 `contract_code + trading_date` 长表，并拒绝缺列、额外合约和重复主键。
# 

# In[ ]:


def normalize_price_response(
    raw_df: pd.DataFrame,
    contract_codes: list[str],
) -> pd.DataFrame:
    columns = ["contract_code", "trading_date", *PRICE_FIELDS]
    if not isinstance(raw_df, pd.DataFrame):
        raise TypeError("schema_error: get_price 未返回 DataFrame。")
    # 空 DataFrame 是合法的明确空响应，保留固定列供日历左连接生成占位行。
    if raw_df.empty:
        return pd.DataFrame(columns=columns)

    # 单合约响应通常把时间放在索引，多合约 panel=False 则显式返回 code/time。
    normalized_df = raw_df.copy()
    if "time" not in normalized_df.columns:
        normalized_df = normalized_df.rename_axis("time").reset_index()
    if "time" not in normalized_df.columns:
        normalized_df = normalized_df.rename(
            columns={normalized_df.columns[0]: "time"}
        )

    if "code" not in normalized_df.columns:
        if len(contract_codes) != 1:
            raise ValueError("schema_error: 多合约 get_price 缺少 code。")
        normalized_df["code"] = contract_codes[0]

    # 请求字段必须全部返回；缺列是响应契约错误，不应被当作无行情。
    required_columns = {"time", "code", *PRICE_FIELDS}
    missing_columns = required_columns - set(normalized_df.columns)
    if missing_columns:
        raise ValueError(
            f"schema_error: get_price 缺列 {sorted(missing_columns)}"
        )

    normalized_df = normalized_df.loc[
        :, ["code", "time", *PRICE_FIELDS]
    ].rename(columns={"code": "contract_code"})
    normalized_df["contract_code"] = normalized_df["contract_code"].astype(str)
    normalized_df["trading_date"] = pd.to_datetime(
        normalized_df["time"],
        errors="raise",
    ).dt.date

    unexpected_codes = set(normalized_df["contract_code"]) - set(contract_codes)
    if unexpected_codes:
        raise ValueError(
            f"schema_error: get_price 返回未请求合约 {sorted(unexpected_codes)}"
        )

    # 在数值转换前区分真正的 None/pd.NA 与供应商返回的 NaN/Inf。
    # 前者转换为 Pandas nullable 浮点，后者是来源异常，必须立即失败。
    for column in PRICE_FIELDS:
        for value in normalized_df[column]:
            if value is None or value is pd.NA:
                continue
            try:
                is_finite = math.isfinite(float(value))
            except (TypeError, ValueError):
                # 非数值文本由下面 errors=raise 的转换给出明确契约错误。
                continue
            if not is_finite:
                raise ValueError(
                    f"schema_error: get_price {column} 含非有限数。"
                )
        normalized_df[column] = pd.to_numeric(
            normalized_df[column],
            errors="raise",
        ).astype("Float64")

    normalized_df = normalized_df.loc[:, columns]
    if normalized_df.duplicated(PRIMARY_KEY).any():
        raise ValueError("schema_error: get_price 返回重复合约日。")
    return normalized_df


# In[ ]:


def normalize_extra_response(
    raw_df: pd.DataFrame,
    contract_codes: list[str],
    value_column: str,
) -> pd.DataFrame:
    columns = ["contract_code", "trading_date", value_column]
    if not isinstance(raw_df, pd.DataFrame):
        raise TypeError(f"schema_error: {value_column} 未返回 DataFrame。")
    # 空 DataFrame 是合法的明确空响应，保留固定列供日历左连接生成占位行。
    if raw_df.empty:
        return pd.DataFrame(columns=columns)

    # get_extras 返回“日期 × 合约”的宽表；请求的每个合约都必须有一列。
    missing_codes = set(contract_codes) - set(map(str, raw_df.columns))
    if missing_codes:
        raise ValueError(
            f"schema_error: {value_column} 缺少合约列 {sorted(missing_codes)}"
        )

    normalized_df = raw_df.copy()
    normalized_df.columns = [str(column) for column in normalized_df.columns]
    # 转成长表后，三类响应都统一为 contract_code + trading_date 主键。
    normalized_df.index.name = "trading_date"
    normalized_df = normalized_df.reset_index().melt(
        id_vars="trading_date",
        value_vars=contract_codes,
        var_name="contract_code",
        value_name=value_column,
    )
    normalized_df["trading_date"] = pd.to_datetime(
        normalized_df["trading_date"],
        errors="raise",
    ).dt.date
    # get_extras 的宽表用浮点 NaN 表示某个合约日没有扩展值。
    # 该字段本身允许为空，所以 None、pd.NA 和 NaN 都归一为 Arrow null；Inf 仍是来源异常。
    for value in normalized_df[value_column]:
        if value is None or value is pd.NA or pd.isna(value):
            continue
        try:
            is_finite = math.isfinite(float(value))
        except (TypeError, ValueError):
            # 非数值文本由下面 errors=raise 的转换给出明确契约错误。
            continue
        if not is_finite:
            raise ValueError(
                f"schema_error: {value_column} 含非有限数。"
            )
    normalized_df[value_column] = pd.to_numeric(
        normalized_df[value_column],
        errors="raise",
    ).astype("Float64")

    if normalized_df.duplicated(PRIMARY_KEY).any():
        raise ValueError(f"schema_error: {value_column} 返回重复合约日。")
    return normalized_df.loc[:, columns]


# ## 构造一批日线事实
# 
# 每批固定调用一次日线和两次扩展字段接口。派生字段先在包含回看日期的序列上计算，
# 最后再由待办日历左连接裁回输出范围；API 无有效收盘价时保留一条全空行情占位事实。
# 

# In[ ]:


def collect_batch(
    jqdata: ModuleType,
    batch: dict[str, object],
    updated_at: datetime,
) -> tuple[pd.DataFrame, int]:
    # 批次字典已经由 request_batches 固化；这里不重新扩大日期或合约范围。
    contract_codes = batch["contract_codes"]
    request_start = batch["request_start"]
    request_end = batch["request_end"]
    pending_df = batch["pending_df"]

    # 一批固定执行一次日线和两次扩展字段请求；任一 None 都视为可重试失败。
    raw_price_df = jqdata.get_price(
        contract_codes,
        start_date=request_start,
        end_date=request_end,
        frequency="daily",
        fields=PRICE_FIELDS,
        skip_paused=True,
        fq=None,
        panel=False,
        fill_paused=False,
        round=False,
    )
    if raw_price_df is None:
        raise RuntimeError("retryable_error: get_price 返回 None。")

    raw_settlement_df = jqdata.get_extras(
        "futures_sett_price",
        contract_codes,
        start_date=request_start,
        end_date=request_end,
        df=True,
    )
    if raw_settlement_df is None:
        raise RuntimeError("retryable_error: futures_sett_price 返回 None。")

    raw_position_df = jqdata.get_extras(
        "futures_positions",
        contract_codes,
        start_date=request_start,
        end_date=request_end,
        df=True,
    )
    if raw_position_df is None:
        raise RuntimeError("retryable_error: futures_positions 返回 None。")

    # 三类响应先各自标准化，再按合约—日期一对一合并。
    price_df = normalize_price_response(raw_price_df, contract_codes)
    settlement_df = normalize_extra_response(
        raw_settlement_df,
        contract_codes,
        "settlement",
    )
    position_df = normalize_extra_response(
        raw_position_df,
        contract_codes,
        "open_interest",
    )

    # 结算价和持仓量允许某一侧缺值，所以使用外连接保留完整扩展字段日期轴。
    extras_df = settlement_df.merge(
        position_df,
        on=PRIMARY_KEY,
        how="outer",
        validate="one_to_one",
    ).sort_values(PRIMARY_KEY)

    # 先在包含 45 天回看窗口的完整序列上派生，再裁回真正待办键。
    # 因此本批首个待办日也能取得请求窗口内的上一有效值。
    if extras_df.empty:
        # 明确创建派生列，使全空批次仍具有稳定的合并结构。
        extras_df["previous_settlement"] = pd.Series(dtype="float64")
        extras_df["open_interest_change"] = pd.Series(dtype="float64")
    else:
        extras_df["previous_settlement"] = extras_df.groupby(
            "contract_code",
            sort=False,
        )["settlement"].transform(lambda values: values.ffill().shift())
        previous_position = extras_df.groupby(
            "contract_code",
            sort=False,
        )["open_interest"].transform(lambda values: values.ffill().shift())
        extras_df["open_interest_change"] = (
            extras_df["open_interest"] - previous_position
        )

    # 三类 API 结果统一到同一合约—日期轴；真正输出范围仍由下方日历左表决定。
    api_df = price_df.merge(
        extras_df,
        on=PRIMARY_KEY,
        how="outer",
        validate="one_to_one",
    )

    base_columns = [
        "contract_code",
        "exchange_code",
        "underlying_code",
        "trading_date",
        "year",
        "month",
    ]
    # 日历是输出键的唯一驱动方：API 多返回的回看日期不会写入，少返回则形成占位。
    daily_df = pending_df.loc[:, base_columns].merge(
        api_df,
        on=PRIMARY_KEY,
        how="left",
        validate="one_to_one",
    )

    daily_df = daily_df.rename(columns={"pre_close": "previous_close"})
    daily_df["close_change_from_previous_settlement"] = (
        daily_df["close"] - daily_df["previous_settlement"]
    )
    daily_df["settlement_change_from_previous_settlement"] = (
        daily_df["settlement"] - daily_df["previous_settlement"]
    )
    daily_df["has_market_data"] = daily_df["close"].notna()
    daily_df["source"] = SOURCE_NAME
    daily_df["updated_at"] = updated_at

    # close 是“有行情”的最小判据；确认空结果必须清空全部行情度量，避免半行数据。
    missing_mask = ~daily_df["has_market_data"]
    daily_df[MARKET_VALUE_COLUMNS] = daily_df[
        MARKET_VALUE_COLUMNS
    ].astype("Float64")
    daily_df.loc[missing_mask, MARKET_VALUE_COLUMNS] = pd.NA

    # 返回调用方之前执行完整事实质检，未通过的数据不会进入 staging。
    checked_df = validate_daily_frame(
        daily_df.loc[:, SCHEMA.names],
        "JQData 转换结果",
    )

    # 配额审计按实际返回的日线行和两类非空扩展值统计。
    returned_value_count = (
        len(price_df)
        + int(settlement_df["settlement"].notna().sum())
        + int(position_df["open_interest"].notna().sum())
    )
    return checked_df, returned_value_count


# ## Parquet 物理结构与叶分区表达式
# 
# Hive 分区字段由目录名承载，不应重复出现在数据文件 Schema 中。
# 这里分别校验单文件物理契约，并构造只命中一个完整叶分区的 Arrow 过滤表达式。
# 

# In[ ]:


def parquet_file_schema(
    schema: pa.Schema,
    partition_columns: list[str],
) -> pa.Schema:
    # Hive 分区值位于目录名中，数据文件只保存其余字段及完整 metadata。
    return pa.schema(
        [
            schema.field(name)
            for name in schema.names
            if name not in partition_columns
        ],
        metadata=schema.metadata,
    )


# In[ ]:


def validate_output_parquet_files(
    table_path: pathlib.Path,
    schema: pa.Schema,
    partition_columns: list[str],
) -> None:
    # Hive 分区列位于目录名中，单个 Parquet 文件只保存非分区字段。
    expected_schema = parquet_file_schema(schema, partition_columns)
    parquet_files = list(table_path.rglob("*.parquet"))

    if not parquet_files:
        raise FileNotFoundError(f"数据集没有 Parquet 文件：{table_path}")

    for parquet_path in parquet_files:
        actual_schema = pq.read_schema(parquet_path)
        if not actual_schema.equals(expected_schema, check_metadata=True):
            raise TypeError(
                f"Parquet 文件 Schema/metadata 与契约不一致：{parquet_path}"
            )


# In[ ]:


def partition_expression(
    partition_columns: list[str],
    partition_key: tuple[object, ...],
) -> ds.Expression:
    # 用精确叶分区表达式限制 staging 与正式路径复读范围。
    expression = None

    for column, value in zip(
        partition_columns,
        partition_key,
        strict=True,
    ):
        condition = ds.field(column) == value
        expression = condition if expression is None else expression & condition

    if expression is None:
        raise ValueError("分区键不得为空。")
    return expression


# ## 完整叶分区的原子提交
# 
# 事实和日历都遵循同一危险写入不变量：
# 
# ```text
# 写 staging → 复读 staging → 备份旧分区 → 替换 → 正式复读 → 成功后清理备份
# ```
# 
# 失败时优先恢复旧分区；无法安全丢弃的新分区进入同级隔离目录，不覆盖唯一恢复副本。
# 

# In[ ]:


def commit_complete_partition(
    frame: pd.DataFrame,
    lake_root: pathlib.Path,
    table_name: str,
    schema: pa.Schema,
    partition_columns: list[str],
    partitioning: ds.Partitioning,
    partition_key: tuple[object, ...],
    validate_frame: Callable[[pd.DataFrame, str], pd.DataFrame],
) -> pd.DataFrame:
    # 调用方必须交付一个完整叶分区；空分区代表明确删除该叶分区。
    complete_df = validate_frame(frame, "待提交完整分区")
    if not complete_df.empty:
        actual_partition_keys = set(
            complete_df[partition_columns].itertuples(
                index=False,
                name=None,
            )
        )
        if actual_partition_keys != {partition_key}:
            raise ValueError("待提交数据越出指定 Hive 分区。")

    # 先锁定最终 Arrow 内容；后续 staging 与正式复读都必须逐值等于它。
    complete_table = pandas_to_arrow(complete_df, schema)

    silver_root = lake_root.resolve() / "silver"
    target_path = silver_root / table_name

    # staging、backup、quarantine 均位于同一 silver 根目录，便于同盘移动与失败恢复。
    run_id = uuid.uuid4().hex
    staging_path = silver_root / f".{table_name}.staging-{run_id}"
    backup_path = silver_root / f".{table_name}.backup-{run_id}"
    quarantine_path = silver_root / f".{table_name}.failed-{run_id}"

    silver_root.mkdir(parents=True, exist_ok=True)
    for managed_path in (
        target_path,
        staging_path,
        backup_path,
        quarantine_path,
    ):
        if not managed_path.resolve().is_relative_to(silver_root):
            raise ValueError(f"数据集路径越出 silver 根目录：{managed_path}")

    # 叶分区路径完全由已校验的分区键构造，不接受调用方传入任意相对路径。
    relative_path = pathlib.Path(
        *[
            f"{name}={value}"
            for name, value in zip(
                partition_columns,
                partition_key,
                strict=True,
            )
        ]
    )
    staging_path.mkdir(parents=True, exist_ok=False)

    # 第一阶段只写 staging 并复读；此时正式分区完全不受影响。
    try:
        if len(complete_table):
            ds.write_dataset(
                complete_table,
                staging_path,
                format="parquet",
                partitioning=partitioning,
                existing_data_behavior="delete_matching",
                basename_template="part-{i}.parquet",
            )

            # staging 写完立即按同一分区规则重开，验证目录分区字段可以正确重建。
            staged_dataset = ds.dataset(
                staging_path,
                format="parquet",
                partitioning=partitioning,
            )
            staged_schema = reconstructed_schema(staged_dataset, schema)
            if not staged_schema.equals(schema, check_metadata=True):
                raise TypeError("staging Schema/metadata 与契约不一致。")

            staged_table = staged_dataset.to_table(
                columns=schema.names,
                filter=partition_expression(
                    partition_columns,
                    partition_key,
                ),
            )
            staged_df = validate_frame(
                arrow_to_pandas(staged_table, schema),
                "staging 完整分区",
            )
            if not pandas_to_arrow(staged_df, schema).equals(complete_table):
                raise ValueError("staging 分区内容检查失败。")
    except Exception:
        shutil.rmtree(staging_path, ignore_errors=True)
        raise

    source_path = staging_path / relative_path
    destination_path = target_path / relative_path
    saved_path = backup_path / relative_path
    marker_path = target_path / "schema.parquet"
    saved_marker_path = backup_path / "schema.parquet"

    marker_created = False
    marker_replaced = False
    commit_succeeded = False

    backup_path.mkdir(parents=True, exist_ok=False)
    target_path.mkdir(parents=True, exist_ok=True)
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    saved_path.parent.mkdir(parents=True, exist_ok=True)

    # 第二阶段先保存旧分区，再移动经过验证的新分区，最后从正式路径复读。
    try:
        if destination_path.exists():
            shutil.move(str(destination_path), str(saved_path))

        if source_path.is_dir():
            shutil.move(str(source_path), str(destination_path))
        elif len(complete_table):
            raise FileNotFoundError(f"staging 缺少 {relative_path}。")

        # schema.parquet 是零行契约标记；旧 metadata 只能通过替换该标记升级。
        expected_file_schema = parquet_file_schema(schema, partition_columns)
        if marker_path.exists():
            marker_schema = pq.read_schema(marker_path)
            if not marker_schema.equals(
                expected_file_schema,
                check_metadata=True,
            ):
                marker_metadata = pq.read_metadata(marker_path)
                if marker_metadata.num_rows:
                    raise ValueError("schema.parquet 必须是 0 行契约标记。")

                shutil.copy2(marker_path, saved_marker_path)
                pq.write_table(
                    pa.Table.from_batches([], schema=expected_file_schema),
                    marker_path,
                )
                marker_replaced = True

        data_files = [
            path
            for path in target_path.rglob("*.parquet")
            if path.name != "schema.parquet"
        ]
        if not data_files and not marker_path.exists():
            pq.write_table(
                pa.Table.from_batches([], schema=expected_file_schema),
                marker_path,
            )
            marker_created = True

        if destination_path.exists():
            validate_output_parquet_files(
                destination_path,
                schema,
                partition_columns,
            )

        # 文件移动完成不等于提交成功，必须从正式表根目录重新发现并复读。
        committed_dataset = ds.dataset(
            target_path,
            format="parquet",
            partitioning=partitioning,
        )
        committed_schema = reconstructed_schema(committed_dataset, schema)
        if not physically_compatible(committed_schema, schema):
            raise TypeError("正式数据集物理 Schema 与契约不兼容。")

        committed_table = committed_dataset.to_table(
            columns=schema.names,
            filter=partition_expression(
                partition_columns,
                partition_key,
            ),
        )
        committed_df = validate_frame(
            arrow_to_pandas(committed_table, schema),
            "正式复读完整分区",
        )
        if not pandas_to_arrow(committed_df, schema).equals(complete_table):
            raise ValueError("正式复读分区内容与 staging 不一致。")

        commit_succeeded = True
    # 提交失败优先恢复旧分区；无法安全丢弃的新分区进入隔离目录。
    except Exception as commit_error:
        if marker_created and marker_path.exists():
            marker_path.unlink()
        if marker_replaced:
            if marker_path.exists():
                marker_path.unlink()
            if saved_marker_path.exists():
                shutil.move(str(saved_marker_path), str(marker_path))

        # rollback_errors 单独记录恢复故障，避免原始提交错误掩盖唯一副本的位置。
        rollback_errors = []
        try:
            if destination_path.exists():
                isolated_path = quarantine_path / relative_path
                isolated_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(destination_path), str(isolated_path))

            if saved_path.exists():
                destination_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(saved_path), str(destination_path))
        except Exception as rollback_error:
            rollback_errors.append(
                f"{type(rollback_error).__name__}: {rollback_error}"
            )

        if rollback_errors:
            raise RuntimeError(
                f"分区提交失败且回滚不完整；恢复副本保留在 {backup_path}；"
                f"回滚错误：{rollback_errors}"
            ) from commit_error

        if quarantine_path.exists() and any(quarantine_path.rglob("*")):
            raise RuntimeError(
                f"分区提交失败；旧分区已恢复，新分区隔离在 {quarantine_path}。"
            ) from commit_error
        raise
    finally:
        # staging 永远可删除；backup 只有在提交成功或已确认没有恢复数据时才清理。
        shutil.rmtree(staging_path, ignore_errors=True)

        backup_has_data = (
            backup_path.exists()
            and any(backup_path.rglob("*.parquet"))
        )
        if commit_succeeded or not backup_has_data:
            shutil.rmtree(backup_path, ignore_errors=True)

        if quarantine_path.exists() and not any(quarantine_path.rglob("*")):
            shutil.rmtree(quarantine_path, ignore_errors=True)

    return committed_df


# ## 合并并提交事实分区
# 
# 自动模式只保留仍属于当前上游、且本轮开始时已经通过完整质检的旧事实，再合并本次结果；
# 因此尾部新增、内部空洞、质量失败和上游已删除的额外旧行都由同一入口处理。
# 显式日期写独立测试湖时则保留同一品种月分区内范围外的旧行。
# 

# In[ ]:


def commit_fact_partitions(
    collected_df: pd.DataFrame,
    existing_fact_df: pd.DataFrame,
    retained_fact_keys: set[tuple[object, ...]],
    theoretical_fact_keys: set[tuple[object, ...]],
    automatic_mode: bool,
    lake_root: pathlib.Path,
) -> pd.DataFrame:
    collected_df = validate_daily_frame(collected_df, "本次完整采集结果")

    # 触达分区包括新事实所在分区，也包括自动模式下应被清理的额外旧事实分区。
    incoming_partition_keys = key_set(collected_df, PARTITION_COLUMNS)
    existing_extra_df = existing_fact_df.iloc[0:0].copy()
    if automatic_mode and not existing_fact_df.empty:
        existing_keys = existing_fact_df[PRIMARY_KEY].apply(tuple, axis=1)
        existing_extra_df = existing_fact_df.loc[
            ~existing_keys.isin(theoretical_fact_keys)
        ]
    dirty_partition_keys = (
        incoming_partition_keys
        | key_set(existing_extra_df, PARTITION_COLUMNS)
    )

    committed_new_frames = []

    # 每个品种月叶分区独立提交；任一分区失败不会把未触达分区卷入替换。
    for partition_key in sorted(dirty_partition_keys):
        partition_mask = pd.Series(True, index=existing_fact_df.index)
        for column, value in zip(
            PARTITION_COLUMNS,
            partition_key,
            strict=True,
        ):
            partition_mask &= existing_fact_df[column].eq(value)
        existing_partition_df = existing_fact_df.loc[partition_mask].copy()

        incoming_mask = pd.Series(True, index=collected_df.index)
        for column, value in zip(
            PARTITION_COLUMNS,
            partition_key,
            strict=True,
        ):
            incoming_mask &= collected_df[column].eq(value)
        incoming_df = collected_df.loc[incoming_mask].copy()
        incoming_keys = key_set(incoming_df, PRIMARY_KEY)

        # 自动模式只信任本轮开始时已完整且仍属于当前上游的旧键。
        # 显式测试湖模式则保留日期视图之外的同分区旧行。
        if automatic_mode:
            # 正式自动模式保留仍属理论格点且通过本轮保留规则的旧键。
            retained_mask = existing_partition_df[PRIMARY_KEY].apply(
                tuple,
                axis=1,
            ).map(
                lambda key: (
                    key in retained_fact_keys
                    and key in theoretical_fact_keys
                    and key not in incoming_keys
                )
            )
        else:
            # 显式测试模式只覆盖本次请求键，保留同一月中日期范围外的旧事实。
            retained_mask = ~existing_partition_df[PRIMARY_KEY].apply(
                tuple,
                axis=1,
            ).isin(incoming_keys)

        retained_df = existing_partition_df.loc[
            retained_mask,
            SCHEMA.names,
        ]
        desired_df = pd.concat(
            [retained_df, incoming_df.loc[:, SCHEMA.names]],
            ignore_index=True,
        )
        desired_df = validate_daily_frame(
            desired_df,
            "合并后事实完整分区",
        )

        # 传入的是完整目标分区，而不是仅含增量的新行。
        committed_df = commit_complete_partition(
            desired_df,
            lake_root,
            TABLE_NAME,
            SCHEMA,
            PARTITION_COLUMNS,
            HIVE_PARTITIONING,
            partition_key,
            validate_daily_frame,
        )
        committed_new_frames.append(
            rows_for_keys(
                committed_df,
                PRIMARY_KEY,
                incoming_keys,
            )
        )

    if not committed_new_frames:
        return empty_pandas(SCHEMA)
    return validate_daily_frame(
        pd.concat(committed_new_frames, ignore_index=True),
        "正式复读的本次事实行",
    )


# ## 事实正式复读后回写日历状态
# 
# 只有已从正式事实路径复读成功的键可以进入本步骤。正常行情实际条数为 1、质量通过；
# 有限 OHLC 关系异常仍按实际条数 1 落盘，但质量为警告；确认空结果实际条数为 0、缺失条数为 1。
# 若状态提交失败，下次运行会从完整事实识别并只修复状态。
# 

# In[ ]:


def update_calendar_states(
    fact_df: pd.DataFrame,
    lake_root: pathlib.Path,
    fetch_run_id: str,
    completed_at: datetime,
) -> int:
    fact_df = validate_daily_frame(fact_df, "状态回写事实输入")
    if fact_df.empty:
        return 0

    silver_root = lake_root.resolve() / "silver"
    calendar_path = silver_root / UPSTREAM_TABLE_NAME
    updated_count = 0

    # 日历按频率—交易所—年月分区；一次只重写事实命中的 1d 叶分区。
    for partition_values, partition_fact_df in fact_df.groupby(
        ["exchange_code", "year", "month"],
        sort=True,
    ):
        exchange_code, year, month = partition_values
        partition_key = ("1d", exchange_code, int(year), int(month))

        # 每次重新打开正式日历，确保前一分区提交后的目录状态已经可见。
        calendar_dataset, _ = open_contract_dataset(
            calendar_path,
            UPSTREAM_PARTITIONING,
            UPSTREAM_SCHEMA,
            "行情日历",
            required=True,
        )
        calendar_table = calendar_dataset.to_table(
            columns=UPSTREAM_SCHEMA.names,
            filter=partition_expression(
                UPSTREAM_PARTITION_COLUMNS,
                partition_key,
            ),
        )
        calendar_df = validate_calendar_state_frame(
            arrow_to_pandas(calendar_table, UPSTREAM_SCHEMA),
            "状态回写前日历完整分区",
        )

        # 事实先转为 Arrow/Python 标量字典，避免 Pandas 可空标量影响布尔判断。
        fact_rows = {
            (row["contract_code"], row["trading_date"]): row
            for row in pandas_to_arrow(
                partition_fact_df.loc[:, SCHEMA.names],
                SCHEMA,
            ).to_pylist()
        }
        matched_keys = set()

        # 事实已经从正式路径复读；现在才允许把完成、缺失和质检状态写回日历。
        # 只修改事实命中的 1d 行；同一完整分区中的分钟状态原样保留。
        for index, row in calendar_df.iterrows():
            key = (row["contract_code"], row["trading_date"])
            fact_row = fact_rows.get(key)
            if fact_row is None:
                continue
            if row["bar_frequency"] != "1d" or row["session_number"] != 0:
                raise ValueError("状态回写命中了非日线格点。")
            if not row["is_fetch_required"]:
                raise ValueError("状态回写命中了当前无需拉取的格点。")

            # 同一派生规则覆盖正常事实、OHLC 原值异常和确认空占位，避免状态修复与首次回写分叉。
            (
                actual_count,
                missing_count,
                quality_status,
                quality_reason,
            ) = daily_calendar_result(fact_row)

            calendar_df.at[index, "is_fetch_completed"] = True
            calendar_df.at[index, "actual_bar_count"] = actual_count
            calendar_df.at[index, "is_data_missing"] = missing_count > 0
            calendar_df.at[index, "missing_bar_count"] = missing_count
            calendar_df.at[index, "fetch_run_id"] = fetch_run_id
            calendar_df.at[index, "fetch_completed_at"] = completed_at
            calendar_df.at[index, "missing_checked_at"] = completed_at
            calendar_df.at[index, "quality_status"] = quality_status
            calendar_df.at[index, "quality_reason"] = quality_reason
            calendar_df.at[index, "quality_checked_at"] = completed_at
            calendar_df.at[index, "updated_at"] = completed_at
            matched_keys.add(key)

        # 任一事实无法回到上游日历都属于依赖破坏，禁止提交不完整状态分区。
        missing_calendar_keys = set(fact_rows) - matched_keys
        if missing_calendar_keys:
            raise ValueError(
                f"事实行无法匹配日线日历：{sorted(missing_calendar_keys)[:5]}"
            )

        # 提交前再次验证整个叶分区，包含本次未修改的分钟行。
        desired_df = validate_calendar_state_frame(
            calendar_df,
            "状态回写后日历完整分区",
        )
        commit_complete_partition(
            desired_df,
            lake_root,
            UPSTREAM_TABLE_NAME,
            UPSTREAM_SCHEMA,
            UPSTREAM_PARTITION_COLUMNS,
            UPSTREAM_PARTITIONING,
            partition_key,
            validate_calendar_state_frame,
        )
        updated_count += len(fact_rows)

    return updated_count


# ## CLI：计划、采集、提交与最终复读
# 
# - 不传日期：对完整上游自动求差；空湖自然得到全量。
# - 传日期：只形成显式检查范围；仅在独立非正式测试湖才允许同时 `--write`。
# - 不传 `--write`：仍认证、调用 JQData、转换并完成内存质检，但不写事实、不回写状态。
# - 不存在独立 API 执行开关；`--write` 只表达是否写入。
# 
# `main` 保持线性执行，内部用编号注释标出七个阶段，避免为普通顺序步骤增加跳转层级。
# 

# In[ ]:


@click.command()
@click.option("--lake-root", type=click.Path(path_type=pathlib.Path))
@click.option("--start-date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--end-date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option(
    "--quota-reserve",
    type=click.IntRange(min=0),
    default=DEFAULT_QUOTA_RESERVE,
    show_default=True,
)
@click.option("--write", is_flag=True)
def main(
    lake_root: pathlib.Path | None,
    start_date: datetime | None,
    end_date: datetime | None,
    quota_reserve: int,
    write: bool,
) -> None:
    # 1. 解析运行模式，并在任何湖读取、认证或 API 调用前执行正式湖写入门禁。
    # 1a. 正式路径只来自 settings；CLI 覆盖路径只是一次性的非正式测试目标。
    formal_lake_root = settings.futures_lake_root
    resolved_lake_root = (lake_root or formal_lake_root).resolve()

    # 1b. 日期参数必须成对，并且日期 + --write + 正式湖这个组合绝不允许继续。
    has_explicit_dates = start_date is not None or end_date is not None
    if (start_date is None) != (end_date is None):
        raise click.UsageError("--start-date 与 --end-date 必须同时提供。")
    if has_explicit_dates and write and resolved_lake_root == formal_lake_root:
        raise click.UsageError(
            "显式指定日期时禁止写入 FUTURES_LAKE_ROOT 指向的正式湖；"
            "请移除日期参数使用自动补缺，或改用非正式测试湖。"
        )

    # 1c. Click 解析为 datetime，业务主键统一降为 date 后再比较。
    requested_start_date = start_date.date() if start_date else None
    requested_end_date = end_date.date() if end_date else None
    if (
        requested_start_date is not None
        and requested_start_date > requested_end_date
    ):
        raise click.BadParameter("起始日期不得晚于结束日期。")

    # 2. 读取完整 1d 上游格点和现有事实；显式日期仅裁剪本次检查视图。
    # 2a. 先形成两个唯一表路径，后续读取和写入不再重新拼接正式路径。
    silver_root = resolved_lake_root / "silver"
    upstream_path = silver_root / UPSTREAM_TABLE_NAME
    target_path = silver_root / TABLE_NAME

    # 2b. 上游是必需依赖；没有 c04 日历时 c05 不允许自行猜测日期或合约范围。
    upstream_dataset, _ = open_contract_dataset(
        upstream_path,
        UPSTREAM_PARTITIONING,
        UPSTREAM_SCHEMA,
        "上游行情日历",
        required=True,
    )
    run_updated_at = datetime.now(timezone.utc)
    calendar_daily_df = validate_upstream_frame(
        read_dataset_frame(
            upstream_dataset,
            UPSTREAM_SCHEMA,
            ds.field("bar_frequency") == "1d",
        ),
        "上游完整日线理论格点",
    )
    policy_calendar_df, policy_changed_keys = apply_daily_policy(
        calendar_daily_df,
        requested_start_date,
        requested_end_date,
        run_updated_at,
    )
    theoretical_df = date_limited_frame(
        policy_calendar_df,
        "trading_date",
        requested_start_date,
        requested_end_date,
    )
    upstream_df = validate_upstream_frame(
        theoretical_df.loc[
            theoretical_df["is_fetch_required"],
            UPSTREAM_SCHEMA.names,
        ],
        "白名单选择后的日线事实格点",
    )

    # 2c. 下游允许不存在；空目录与未建表都自然得到空的完整格点集合。
    target_dataset, target_contract_error = open_contract_dataset(
        target_path,
        HIVE_PARTITIONING,
        SCHEMA,
        "现有日线事实",
        required=False,
        allow_metadata_upgrade=True,
    )
    existing_fact_df = (
        read_dataset_frame(target_dataset, SCHEMA)
        if target_dataset is not None
        else empty_pandas(SCHEMA)
    )
    existing_fact_view_df = date_limited_frame(
        existing_fact_df,
        "trading_date",
        requested_start_date,
        requested_end_date,
    )

    # 事实只有同时通过契约与完整业务质检，主键才算“已经完整落盘”。
    # 物理兼容但 metadata 过期时保留读取能力，并把全部上游键重新纳入修复。
    target_quality_error = None
    try:
        checked_existing_df = validate_daily_frame(
            existing_fact_df,
            "现有日线事实",
        )
        checked_existing_view_df = date_limited_frame(
            checked_existing_df,
            "trading_date",
            requested_start_date,
            requested_end_date,
        )
    except (TypeError, ValueError) as error:
        target_quality_error = str(error)
        checked_existing_df = existing_fact_df
        checked_existing_view_df = existing_fact_view_df

    # 3. 精确求差：待采集、上游之外的额外事实，以及只需回写状态的三类键分开处理。
    # 3a. 集合公式的两侧都使用完全相同的事实主键。
    theoretical_keys = key_set(theoretical_df, PRIMARY_KEY)
    upstream_keys = key_set(upstream_df, PRIMARY_KEY)
    existing_view_keys = key_set(checked_existing_view_df, PRIMARY_KEY)

    if target_contract_error is None and target_quality_error is None:
        complete_fact_keys = upstream_keys & existing_view_keys
    else:
        complete_fact_keys = set()

    # 3b. pending 是 API 待办；extra 是自动模式需清理；repair 只修复状态。
    pending_keys = upstream_keys - complete_fact_keys
    extra_fact_keys = existing_view_keys - theoretical_keys
    retained_fact_keys = complete_fact_keys
    if target_contract_error is None and target_quality_error is None:
        retained_fact_keys |= existing_view_keys & (
            theoretical_keys - upstream_keys
        )

    pending_df = rows_for_keys(
        upstream_df,
        PRIMARY_KEY,
        pending_keys,
    )
    complete_fact_df = rows_for_keys(
        checked_existing_view_df,
        PRIMARY_KEY,
        complete_fact_keys,
    )
    repair_keys = state_repair_keys(upstream_df, complete_fact_df)
    repair_fact_df = rows_for_keys(
        complete_fact_df,
        PRIMARY_KEY,
        repair_keys,
    )

    # 4. 在认证前完成请求分批和配额需求估计，让停止条件不产生部分写入。
    batches = request_batches(pending_df)
    estimated_values = sum(
        int(batch["estimated_values"])
        for batch in batches
    )
    mode = "explicit" if has_explicit_dates else "automatic"
    plan_name = "explicit_plan" if has_explicit_dates else "auto_plan"

    # 4a. 计划先完整输出，操作者能在认证和调用 API 前看到本次影响范围。
    click.echo(
        f"{plan_name}: table={TABLE_NAME}; "
        f"theoretical_grid_count={len(theoretical_keys)}; "
        f"upstream_grid_count={len(upstream_keys)}; "
        f"complete_grid_count={len(complete_fact_keys)}; "
        f"pending_grid_count={len(pending_keys)}; "
        f"state_repair_count={len(repair_keys)}; "
        f"extra_fact_count={len(extra_fact_keys)}; "
        f"policy_changed_count={len(policy_changed_keys)}; "
        f"request_batch_count={len(batches)}; "
        f"estimated_values={estimated_values}; "
        f"contract_error={target_contract_error}; "
        f"quality_error={target_quality_error}"
    )

    policy_committed_count = 0
    if write and policy_changed_keys:
        policy_committed_count = commit_daily_policy_partitions(
            policy_calendar_df,
            policy_changed_keys,
            resolved_lake_root,
        )
        click.echo(
            "policy_committed: "
            f"table={UPSTREAM_TABLE_NAME}; "
            f"updated_grid_count={policy_committed_count}"
        )

    # 4b. 三类工作都为空才是真正最新；只有事实完整但状态滞后不能提前返回。
    if not pending_keys and not repair_keys and not extra_fact_keys:
        if policy_changed_keys and not write:
            click.echo(
                "policy_preview: "
                f"table={UPSTREAM_TABLE_NAME}; "
                f"changed_grid_count={len(policy_changed_keys)}"
            )
        else:
            click.echo(f"up_to_date: table={TABLE_NAME}; mode={mode}")
        return

    # 5. 仅当存在事实缺口时认证并调用 JQData；纯状态修复不会消耗 API 配额。
    # 5a. 所有批次共享同一运行时间与批次号，便于事实和状态交叉审计。
    collected_frames = []
    returned_value_count = 0
    fetch_run_id = (
        f"daily-{run_updated_at:%Y%m%dT%H%M%SZ}-"
        f"{uuid.uuid4().hex[:8]}"
    )

    if pending_keys:
        from config.jqdata_connection import authenticate_jqdata

        # 5b. 凭据只在确有待办时读取并认证，不打印到计划或异常信息。
        jqdata = authenticate_jqdata(
            settings.jqdata_id,
            settings.jqdata_secret,
        )

        # 5c. 配额不足时整批停止；此检查发生在第一项行情请求和任何写入之前。
        spare = quota_spare(jqdata)
        if spare is not None and spare - estimated_values < quota_reserve:
            raise click.ClickException(
                "quota_stop: "
                f"spare={spare}; estimated={estimated_values}; "
                f"reserve={quota_reserve}；未调用行情接口，未写事实或状态。"
            )

        # 5d. 批次逐个完成请求、转换和内存质检；湖仓提交仍统一留到全部批次之后。
        for batch_number, batch in enumerate(batches, start=1):
            click.echo(
                "request_batch: "
                f"batch={batch_number}/{len(batches)}; "
                f"exchange={batch['exchange_code']}; "
                f"year={batch['year']}; "
                f"contracts={len(batch['contract_codes'])}; "
                f"pending={len(batch['pending_df'])}; "
                f"start={batch['request_start']}; "
                f"end={batch['request_end']}"
            )
            batch_df, batch_returned_count = collect_batch(
                jqdata,
                batch,
                run_updated_at,
            )
            collected_frames.append(batch_df)
            returned_value_count += batch_returned_count

    # 5e. 合并所有批次后再做一次全局主键和跨日派生检查。
    collected_df = (
        validate_daily_frame(
            pd.concat(collected_frames, ignore_index=True),
            "本次全部 JQData 结果",
        )
        if collected_frames
        else empty_pandas(SCHEMA)
    )
    confirmed_empty_count = int(
        collected_df["has_market_data"].eq(False).sum()
    )
    invalid_ohlc_count = sum(
        has_invalid_ohlc_relationship(row)
        for row in pandas_to_arrow(collected_df, SCHEMA).to_pylist()
        if row["has_market_data"]
    )

    click.echo(
        "api_result: "
        f"requests={len(batches) * 3}; "
        f"returned_values={returned_value_count}; "
        f"fact_rows={len(collected_df)}; "
        f"confirmed_empty={confirmed_empty_count}; "
        f"invalid_ohlc_rows={invalid_ohlc_count}"
    )

    # 不带 --write 仍完成真实采集和内存质检，到这里为止不创建任何湖仓文件。
    if not write:
        return

    # 6. 先提交并正式复读事实，再把相同事实键回写到行情日历。
    # 6a. 事实是状态的证据源，所以先完成事实分区的正式提交与复读。
    committed_new_df = commit_fact_partitions(
        collected_df,
        checked_existing_df,
        retained_fact_keys,
        theoretical_keys,
        not has_explicit_dates,
        resolved_lake_root,
    )

    # 6b. 状态输入由新提交事实和无需重拉的修复事实合并，主键仍保持唯一。
    status_input_df = validate_daily_frame(
        pd.concat(
            [repair_fact_df, committed_new_df],
            ignore_index=True,
        ).drop_duplicates(PRIMARY_KEY, keep="last"),
        "待回写状态的正式事实行",
    )
    updated_state_count = update_calendar_states(
        status_input_df,
        resolved_lake_root,
        fetch_run_id,
        run_updated_at,
    )

    # 7. 最后从两个正式表执行全表复读，确认本批提交没有破坏其他分区。
    # 7a. 事实表执行全表契约与业务复读，并逐文件确认物理 Schema。
    committed_dataset, _ = open_contract_dataset(
        target_path,
        HIVE_PARTITIONING,
        SCHEMA,
        "提交后的日线事实",
        required=True,
    )
    validate_daily_frame(
        read_dataset_frame(committed_dataset, SCHEMA),
        "提交后的完整日线事实",
    )
    validate_output_parquet_files(
        target_path,
        SCHEMA,
        PARTITION_COLUMNS,
    )

    # 7b. 日历也执行全表复读，确认本轮状态提交没有破坏未触达格点。
    calendar_dataset, _ = open_contract_dataset(
        upstream_path,
        UPSTREAM_PARTITIONING,
        UPSTREAM_SCHEMA,
        "状态回写后的行情日历",
        required=True,
    )
    validate_calendar_state_frame(
        read_dataset_frame(calendar_dataset, UPSTREAM_SCHEMA),
        "状态回写后的完整行情日历",
    )

    # 7c. 最终摘要区分正式自动补缺和显式非正式测试写入。
    commit_mode = (
        "explicit_non_formal"
        if has_explicit_dates
        else "automatic_gap_fill"
    )
    click.echo(
        f"committed: mode={commit_mode}; "
        f"fact_rows={len(committed_new_df)}; "
        f"state_rows={updated_state_count}; "
        f"removed_extra_rows={len(extra_fact_keys) if not has_explicit_dates else 0}; "
        "remaining_pending=0"
    )


if __name__ == "__main__":
    main()

