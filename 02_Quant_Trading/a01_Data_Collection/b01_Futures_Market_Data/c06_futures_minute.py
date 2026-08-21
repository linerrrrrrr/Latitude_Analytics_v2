#!/usr/bin/env python
# coding: utf-8

# # c06_futures_minute
# 
# 目标表：`fact_futures_minute`。
# 
# 本入口先对 `dim_futures_bar_calendar` 的完整 `1m` 理论格点应用共享期货事实采集政策，
# 再自动计算：
# 
# ```text
# 政策选中的上游有效 Session
#     − 已由正式分钟事实复读和日历完成状态共同证明完整的 Session
#     = 本次分钟自动更新范围
# ```
# 
# 分钟线统一来自 JQData `get_price(frequency="1m")`，时间戳采用
# `(session_start_at, session_end_at]` 的 bar 结束时刻语义。API 没有返回的分钟不会被填充；
# 实际条数、缺失条数和完成状态在事实正式复读后写回行情日历。有限数之间的供应商原始 OHLC
# 跨列关系异常行保留原值并允许落盘，同时继续计数并把 Session 标为 `warning`；它不伪装成缺失或空响应。
# NaN/Inf 和负数量仍拒绝落盘，真实 0 行响应仍按既有规则形成疑似休市。
# 
# 白名单缩减只停止后续采集并更新日历选择状态，不自动删除已经正式落盘的历史分钟事实。
# 命令启动即执行政策评估、真实采集和内存质检；`--write` 只决定是否提交事实和状态。
# 

# ## 项目定位、依赖与权威契约
# 
# 先使用项目统一标记定位根目录，再导入 `.env` 配置、中央 Arrow Schema 和共享白名单。
# 正式湖路径和凭据不在 Notebook 内复制。
# 

# In[ ]:


from __future__ import annotations

# 标准库负责数值检查、路径管理、原子替换、时间和运行批次标识。
import math
import pathlib
import shutil
import sys
import uuid
from datetime import date, datetime, timezone
from types import ModuleType
from typing import Callable


project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()  # 当前工作目录

# 只使用项目统一规定的三个标记定位根目录。
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

# 所有字段、类型、metadata 和转换都来自中央可执行契约。
from config.data_contracts import (
    FUTURES_BAR_CALENDAR_SCHEMA,
    FUTURES_MINUTE_SCHEMA,
    arrow_to_pandas,
    empty_pandas,
    pandas_to_arrow,
)
from config.settings import settings


# 白名单来自根级配置；它只控制期货事实，不定义日历宇宙。
from config.futures_fact_collection_policy import FUTURES_FACT_VARIETY_PAIRS


# ## Schema 契约交互浏览

# In[ ]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from config.notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        FUTURES_BAR_CALENDAR_SCHEMA,
        FUTURES_MINUTE_SCHEMA,
    ])


# ## 表名、主键、分区与稳定常量
# 
# 这一单元格只声明 c06 的稳定边界，不执行任何 I/O。事实分区与日历分区不同，
# 状态回写时必须分别按各自的完整叶分区提交。
# 

# In[ ]:


SCHEMA = FUTURES_MINUTE_SCHEMA  # 国内期货一分钟行情事实的权威 Arrow Schema。
CALENDAR_SCHEMA = FUTURES_BAR_CALENDAR_SCHEMA  # 行情拉取与质检日历的权威 Schema。
# 复用的完整日历校验函数沿用 c05 的上游命名；这里显式绑定到同一权威契约。
UPSTREAM_SCHEMA = CALENDAR_SCHEMA  # 状态回写所使用的上游/目标日历 Schema。

TABLE_NAME = "fact_futures_minute"  # 国内期货合约一分钟行情事实表。
CALENDAR_TABLE_NAME = "dim_futures_bar_calendar"  # 需回写完成与疑似休市状态的行情日历表。

PRIMARY_KEY = [  # 唯一标识某合约的一根一分钟 bar。
    "contract_code",  # JQData 标准固定月份合约代码。
    "bar_at",  # Asia/Shanghai 时区的一分钟 bar 结束时刻。
]
SESSION_KEY = [  # 汇总分钟条数和回写状态所用的 Session 业务键。
    "contract_code",  # 固定月份合约代码。
    "trading_date",  # 分钟 bar 归属的期货交易日。
    "session_number",  # 同一合约日内的 Session 顺序号。
]
CALENDAR_PRIMARY_KEY = [  # 唯一标识行情日历中的一个格点。
    "bar_frequency",  # 行情频率；本入口处理 1m。
    "contract_code",  # 固定月份合约代码。
    "trading_date",  # 行情归属交易日。
    "session_number",  # 分钟 Session 顺序号。
]
UPSTREAM_PRIMARY_KEY = CALENDAR_PRIMARY_KEY  # 完整日历校验沿用同一四列主键。

PARTITION_COLUMNS = [  # 分钟事实的 Hive 叶分区层级。
    "exchange_code",  # 合约所属交易所。
    "underlying_code",  # 合约所属期货品种。
    "year",  # bar 所属交易年份。
    "month",  # bar 所属交易月份。
]
CALENDAR_PARTITION_COLUMNS = [  # 行情日历状态回写的 Hive 叶分区层级。
    "bar_frequency",  # 行情频率；本入口回写 1m 分区。
    "exchange_code",  # 交易所代码。
    "year",  # 交易年份。
    "month",  # 交易月份。
]

PRICE_FIELDS = [  # JQData 一分钟响应中参与行情质量检查的字段。
    "open",  # 分钟开盘价。
    "high",  # 分钟最高价。
    "low",  # 分钟最低价。
    "close",  # 分钟收盘价。
    "volume",  # 分钟成交量，单位为手。
    "money",  # 分钟成交额，单位为元。
    "open_interest",  # 分钟结束时持仓量，单位为手。
]
SOURCE_NAME = "JQData_get_price_1m_skip_paused_fq_none"
LEGACY_SOURCE_NAMES = {"JQData_get_price_1m", "JQData_get_price_1m_skip_paused"}
DEFAULT_QUOTA_RESERVE = 5_000_000

SELECTED_REASON = (
    "命中国内期货事实采集白名单；一分钟事实需要采集。"
)
EXCLUDED_REASON = (
    "未命中国内期货事实采集白名单；"
    "保留理论格点但不采集一分钟事实，既有历史分钟事实不自动删除。"
)

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

EVIDENCE_COLUMNS = [  # c06 产生新分钟状态时必须清空的旧 c07 校对证据。
    "daily_open",  # JQData 日线开盘价旁证。
    "daily_high",  # JQData 日线最高价旁证。
    "daily_low",  # JQData 日线最低价旁证。
    "daily_close",  # JQData 日线收盘价旁证。
    "daily_volume",  # JQData 日线成交量旁证。
    "daily_money",  # JQData 日线成交额旁证。
    "daily_open_interest",  # JQData 日线收盘持仓量旁证。
    "aggregated_open",  # 其他完整 Session 重聚合开盘价。
    "aggregated_high",  # 其他完整 Session 重聚合最高价。
    "aggregated_low",  # 其他完整 Session 重聚合最低价。
    "aggregated_close",  # 其他完整 Session 重聚合收盘价。
    "aggregated_volume",  # 其他完整 Session 重聚合成交量。
    "aggregated_money",  # 其他完整 Session 重聚合成交额。
    "aggregated_open_interest",  # 其他完整 Session 末条有效持仓量。
    "ohlc_matches_daily",  # 重聚合 OHLC 是否均匹配日线。
    "volume_matches_daily",  # 重聚合成交量是否匹配日线。
    "money_matches_daily",  # 重聚合成交额是否匹配日线。
    "open_interest_matches_daily",  # 重聚合末持仓是否匹配日线。
]

# 显式类型的 Hive 分区避免目录字符串推断改变契约。
HIVE_PARTITIONING = ds.partitioning(
    pa.schema([SCHEMA.field(name) for name in PARTITION_COLUMNS]),
    flavor="hive",
)
CALENDAR_PARTITIONING = ds.partitioning(
    pa.schema(
        [CALENDAR_SCHEMA.field(name) for name in CALENDAR_PARTITION_COLUMNS]
    ),
    flavor="hive",
)


# ## 行情日历完整分区校验
# 
# c06 会修改 `is_fetch_required`、`selection_reason` 和分钟完成状态，但提交单位是完整日历叶分区。
# 因此提交前后必须继续维护日线/分钟结构、审计时间、缺失计数和质量状态等整表不变量。
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


# ## 分钟事实完整性与 Session 边界
# 
# 通用事实质检负责主键、来源、分区、有限数和非负数量。有限 OHLC 跨列异常保留原值，
# 由独立异常计数和日历 `warning` 留痕；当调用方同时提供 Session 时，再验证每条 bar 确实属于
# 对应 `(start,end]`，且继承了正确交易日和 Session 编号。
# 

# In[ ]:


def invalid_ohlc_mask(frame: pd.DataFrame) -> pd.Series:
    # 空表也返回同索引布尔序列，便于调用方直接筛选和计数。
    if frame.empty:
        return pd.Series(False, index=frame.index, dtype=bool)

    # 只比较实际存在的有限价格；NaN/Inf 仍由事实 validator 单独拒绝。
    comparable_max = frame[["open", "close", "low"]].max(
        axis=1,
        skipna=True,
    )
    comparable_min = frame[["open", "close", "high"]].min(
        axis=1,
        skipna=True,
    )
    invalid_mask = (
        frame["high"].notna()
        & comparable_max.notna()
        & frame["high"].lt(comparable_max)
    ) | (
        frame["low"].notna()
        & comparable_min.notna()
        & frame["low"].gt(comparable_min)
    )
    return invalid_mask.fillna(False).astype(bool)


def minute_session_quality(
    expected_count: int,
    actual_count: int,
    invalid_count: int,
) -> tuple[int, str, str]:
    # actual_count 包含保留的 OHLC 异常行；只有物理时点未出现才属于缺失。
    if actual_count > expected_count:
        raise ValueError("分钟实际条数超过 Session 理论条数。")
    if invalid_count < 0 or invalid_count > actual_count:
        raise ValueError("OHLC 异常条数无法由 Session 正式事实复算。")

    missing_count = expected_count - actual_count
    has_warning = missing_count > 0 or invalid_count > 0
    if not has_warning:
        return (
            0,
            "passed",
            "JQData 分钟事实已正式提交并复读，实际条数等于理论条数。",
        )

    quality_reason = (
        "JQData 分钟请求已完成并正式复读；"
        f"理论 {expected_count} 条，实际 {actual_count} 条，"
        f"缺失 {missing_count} 条。"
    )
    if invalid_count:
        quality_reason += (
            f"其中 {invalid_count} 条供应商原始 OHLC 跨列关系异常，原值已保留。"
        )
    return missing_count, "warning", quality_reason


def validate_minute_frame(
    frame: pd.DataFrame,
    context: str,
    sessions_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    # 必须在 Arrow 转换前检查来源数值；否则 Pandas NaN 会被 Arrow 静默转成 null。
    # 真正的 Python None / pd.NA 仍按 Schema 的 nullable 语义处理，NaN/Inf 则明确拒绝。
    source_frame = frame.loc[:, SCHEMA.names]
    for column in PRICE_FIELDS:
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

    # Arrow 转换固定列顺序、类型、可空性和 metadata。
    table = pandas_to_arrow(source_frame, SCHEMA)
    checked_df = arrow_to_pandas(table, SCHEMA)

    if checked_df.duplicated(PRIMARY_KEY).any():
        raise ValueError(f"{context}主键不唯一。")
    if checked_df.empty:
        return checked_df

    session_rows = None
    if sessions_df is not None:
        session_rows = {
            (
                row["contract_code"],
                row["trading_date"],
                row["session_number"],
            ): row
            for row in pandas_to_arrow(
                sessions_df.loc[:, CALENDAR_SCHEMA.names],
                CALENDAR_SCHEMA,
            ).to_pylist()
        }

    for row in table.to_pylist():
        # 身份、分区和来源必须能由主键及对应 Session 复算。
        if not row["contract_code"].endswith(f".{row['exchange_code']}"):
            raise ValueError(f"{context}交易所与合约代码后缀不一致。")
        contract_symbol = row["contract_code"].split(".", maxsplit=1)[0]
        underlying_code = "".join(
            character
            for character in contract_symbol
            if character.isalpha()
        ).upper()
        if row["underlying_code"] != underlying_code:
            raise ValueError(f"{context}品种代码无法由合约代码复算。")
        if (
            row["year"] != row["trading_date"].year
            or row["month"] != row["trading_date"].month
        ):
            raise ValueError(f"{context}year/month 与 trading_date 不一致。")
        if row["session_number"] <= 0:
            raise ValueError(f"{context}session_number 必须大于 0。")
        if row["source"] not in {SOURCE_NAME, *LEGACY_SOURCE_NAMES}:
            raise ValueError(f"{context}source 与 JQData 分钟契约不一致。")

        # API 非空数值必须有限；NaN 和 Inf 不得进入正式事实。
        for column in PRICE_FIELDS:
            value = row[column]
            if value is not None and not math.isfinite(float(value)):
                raise ValueError(f"{context}{column} 包含非有限数。")

        # 数量和金额允许为空或为零，但非空值不得为负。
        for column in ["volume", "money", "open_interest"]:
            value = row[column]
            if value is not None and value < 0:
                raise ValueError(f"{context}{column} 不得为负。")

        # 有限 OHLC 跨列异常不会在这里被拒收或修写；采集、完成判定和 c08 会继续检测并留痕。

        if session_rows is not None:
            key = (
                row["contract_code"],
                row["trading_date"],
                row["session_number"],
            )
            session = session_rows.get(key)
            if session is None:
                raise ValueError(f"{context}分钟行不属于待办 Session。")
            if (
                row["exchange_code"] != session["exchange_code"]
                or row["underlying_code"] != session["underlying_code"]
            ):
                raise ValueError(f"{context}分钟行未继承 Session 业务属性。")
            if not (
                session["session_start_at"]
                < row["bar_at"]
                <= session["session_end_at"]
            ):
                raise ValueError(f"{context}bar_at 越出 (Session 开始,结束]。")

    return checked_df.sort_values(PRIMARY_KEY).reset_index(drop=True)


# In[ ]:


def rows_for_sessions(
    frame: pd.DataFrame,
    sessions_df: pd.DataFrame,
) -> pd.DataFrame:
    # Session 为空时返回保留事实 Schema 的空表。
    if sessions_df.empty or frame.empty:
        return frame.iloc[0:0].copy()

    selected_frames = []
    for session in pandas_to_arrow(
        sessions_df.loc[:, CALENDAR_SCHEMA.names],
        CALENDAR_SCHEMA,
    ).to_pylist():
        mask = (
            frame["contract_code"].eq(session["contract_code"])
            & frame["trading_date"].eq(session["trading_date"])
            & frame["session_number"].eq(session["session_number"])
            & frame["bar_at"].gt(session["session_start_at"])
            & frame["bar_at"].le(session["session_end_at"])
        )
        selected_frames.append(frame.loc[mask, SCHEMA.names])

    selected_df = pd.concat(selected_frames, ignore_index=True)
    # Session 不应重叠；若同一 bar 命中两次，这里按主键明确失败。
    return validate_minute_frame(
        selected_df,
        "Session 选取结果",
        sessions_df,
    )


# ## 契约化数据集读取与分区发现
# 
# 上游日历必须精确符合权威 Schema/metadata。分钟事实允许识别“物理字段兼容但 metadata 旧”的历史表，
# 但这些 Session 不属于完整下游集合，触达后必须以当前契约重写。
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


# In[ ]:


def cast_partition_value(column: str, raw_value: str) -> object:
    # year/month 在 Arrow 契约中是整数，其余分区字段保留字符串。
    if column in {"year", "month"}:
        return int(raw_value)
    return raw_value


def partition_keys_from_files(
    table_path: pathlib.Path,
    partition_columns: list[str],
) -> set[tuple[object, ...]]:
    # 直接从文件路径发现已有叶分区，schema.parquet 不代表业务分区。
    keys = set()
    if not table_path.is_dir():
        return keys

    for parquet_path in table_path.rglob("*.parquet"):
        if parquet_path.name == "schema.parquet":
            continue

        relative_parts = parquet_path.relative_to(table_path).parts
        if len(relative_parts) != len(partition_columns) + 1:
            raise ValueError(f"数据集包含非法分区文件：{parquet_path}")

        values = []
        for column, directory_name in zip(
            partition_columns,
            relative_parts[:-1],
            strict=True,
        ):
            prefix = f"{column}="
            if not directory_name.startswith(prefix):
                raise ValueError(f"数据集包含非法分区目录：{parquet_path}")
            values.append(
                cast_partition_value(column, directory_name[len(prefix):])
            )
        keys.add(tuple(values))

    return keys


# ## 白名单政策评估与选择状态
# 
# c06 是分钟事实白名单的应用入口。政策变化只更新完整日历中的选择状态：
# 
# - 新命中白名单：设为需要采集，并从未完成状态开始；
# - 不再命中：设为无需采集和 `not_applicable`，但不删除日历格点或既有分钟事实；
# - 已由权威证据确认休市：保留权威结论，c06 不覆盖。
# 

# In[ ]:


def reset_minute_execution_state(
    calendar_df: pd.DataFrame,
    index: int,
    quality_status: str,
    quality_reason: str,
    checked_at: datetime | None,
) -> None:
    # 政策边界变化后，旧完成/缺失/校对证据不再代表当前调度决定。
    calendar_df.at[index, "is_fetch_completed"] = False
    calendar_df.at[index, "actual_bar_count"] = 0
    calendar_df.at[index, "is_data_missing"] = False
    calendar_df.at[index, "missing_bar_count"] = 0
    calendar_df.at[index, "fetch_run_id"] = None
    calendar_df.at[index, "fetch_completed_at"] = None
    calendar_df.at[index, "missing_checked_at"] = checked_at
    calendar_df.at[index, "quality_status"] = quality_status
    calendar_df.at[index, "quality_reason"] = quality_reason
    calendar_df.at[index, "quality_checked_at"] = checked_at

    # 日线—分钟定向校对证据也必须随政策重置。
    for column in EVIDENCE_COLUMNS:
        calendar_df.at[index, column] = None


# In[ ]:


def apply_minute_policy(
    calendar_df: pd.DataFrame,
    start_date: date | None,
    end_date: date | None,
    updated_at: datetime,
) -> tuple[pd.DataFrame, set[tuple[object, ...]]]:
    calendar_df = validate_calendar_state_frame(
        calendar_df,
        "政策评估前行情日历",
    )
    changed_keys = set()

    for index, row in calendar_df.iterrows():
        # c06 只评估本次视图中的 1m 格点，日线行永远不受分钟白名单影响。
        if row["bar_frequency"] != "1m":
            continue
        if start_date is not None and not (
            start_date <= row["trading_date"] <= end_date
        ):
            continue

        # 权威休市是白名单之后的进一步业务结论，不由 c06 反向覆盖。
        if row["schedule_status"] == "confirmed_closed":
            continue

        pair = (row["exchange_code"], row["underlying_code"])
        should_fetch = pair in FUTURES_FACT_VARIETY_PAIRS
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

        if should_fetch:
            if state_changed:
                # 新进入白名单的 Session 从 pending 开始，等待正式事实提交。
                reset_minute_execution_state(
                    calendar_df,
                    index,
                    "pending",
                    "分钟采集政策已选择该格点，等待事实提交与复读。",
                    None,
                )
            # 仅说明文字标准化时保留已经正式复读的完成状态，不重复请求 API。
        else:
            # 排除只影响未来调度；日历格点和既有事实均保留。
            reset_minute_execution_state(
                calendar_df,
                index,
                "not_applicable",
                EXCLUDED_REASON,
                updated_at,
            )

        calendar_df.at[index, "updated_at"] = updated_at
        changed_keys.add(
            tuple(calendar_df.at[index, name] for name in CALENDAR_PRIMARY_KEY)
        )

    return (
        validate_calendar_state_frame(calendar_df, "政策评估后行情日历"),
        changed_keys,
    )


# ## Session 完成判定与自动更新计划
# 
# 分钟事实没有人为补齐的占位行。一个 Session 只有同时满足以下条件才属于完整下游格点：
# 
# 1. 日历已经记录请求完成和批次时间；
# 2. 正式事实分区通过当前 Schema 与完整分钟质检；
# 3. 正式复读行数与日历 `actual_bar_count` 一致；
# 4. 缺失数和质量状态能由期望数减实际数复算。
# 

# In[ ]:


def session_key_set(frame: pd.DataFrame) -> set[tuple[object, ...]]:
    return set(frame[SESSION_KEY].itertuples(index=False, name=None))


def rows_for_session_keys(
    frame: pd.DataFrame,
    selected_keys: set[tuple[object, ...]],
) -> pd.DataFrame:
    if not selected_keys:
        return frame.iloc[0:0].copy()
    keys = frame[SESSION_KEY].apply(tuple, axis=1)
    return frame.loc[keys.isin(selected_keys)].reset_index(drop=True)


# In[ ]:


def complete_session_keys(
    sessions_df: pd.DataFrame,
    fact_df: pd.DataFrame,
    target_contract_error: str | None,
) -> set[tuple[object, ...]]:
    if target_contract_error is not None:
        # metadata 过期时正式事实尚未满足当前完整落盘定义。
        return set()

    counts = {}
    invalid_counts = {}
    if not fact_df.empty:
        counts = (
            fact_df.groupby(SESSION_KEY, dropna=False)
            .size()
            .to_dict()
        )
        invalid_fact_df = fact_df.loc[invalid_ohlc_mask(fact_df)]
        if not invalid_fact_df.empty:
            invalid_counts = (
                invalid_fact_df.groupby(SESSION_KEY, dropna=False)
                .size()
                .to_dict()
            )

    complete_keys = set()
    for row in pandas_to_arrow(
        sessions_df.loc[:, CALENDAR_SCHEMA.names],
        CALENDAR_SCHEMA,
    ).to_pylist():
        key = (
            row["contract_code"],
            row["trading_date"],
            row["session_number"],
        )
        actual_count = int(counts.get(key, 0))
        invalid_count = int(invalid_counts.get(key, 0))
        try:
            expected_missing, expected_quality, _ = minute_session_quality(
                int(row["expected_bar_count"]),
                actual_count,
                invalid_count,
            )
        except ValueError:
            # 条数超界或异常计数无法复算时，当前状态不得掩盖事实问题。
            continue

        # 空响应形成的疑似休市状态可以被 c07 的旁证质检进一步更新。
        # 这类行即使 quality_status 已由 warning 升为 passed，仍属于已完成请求；
        # 反之，只要正式事实后来变为非空，就必须重新进入本入口以撤销疑似休市。
        if actual_count == 0:
            quality_state_is_current = (
                row["schedule_status"] == "suspected_closed"
                and row["quality_status"] in {"passed", "warning"}
                and row["evidence_level"] in {"inferred", "reconciled"}
            )
        elif invalid_count > 0:
            # 异常行已经正式存在，因此 Session 完成状态必须明确保留 warning 证据。
            quality_state_is_current = (
                row["schedule_status"] == "scheduled"
                and row["quality_status"] == "warning"
                and row["evidence_source"]
                == "fact_futures_minute:invalid_ohlc"
            )
        elif actual_count > 0 and row["schedule_status"] == "suspected_closed":
            quality_state_is_current = False
        else:
            quality_state_is_current = row["quality_status"] == expected_quality

        state_is_current = (
            row["is_fetch_completed"]
            and bool(row["fetch_run_id"])
            and row["fetch_completed_at"] is not None
            and row["actual_bar_count"] == actual_count
            and row["missing_checked_at"] is not None
            and row["missing_bar_count"] == expected_missing
            and row["is_data_missing"] == (expected_missing > 0)
            and quality_state_is_current
            and row["quality_checked_at"] is not None
        )
        if state_is_current:
            complete_keys.add(key)

    return complete_keys


# ## JQData 配额与请求批次
# 
# 每个事实叶分区在第一项行情请求前，用其中全部待办 Session 的理论分钟数检查真实剩余额度。
# 分区内部按“合约—交易日”请求，既减少 API 次数，也把请求时间限制在该合约日全部 Session 的首尾之间。
# 

# In[ ]:


def quota_spare(jqdata: ModuleType) -> int | None:
    get_query_count = getattr(jqdata, "get_query_count", None)
    if get_query_count is None:
        return None

    # total 不是当前可消费量，只使用 spare。
    quota = get_query_count()
    if isinstance(quota, dict) and quota.get("spare") is not None:
        return int(quota["spare"])
    return None


def request_batches(sessions_df: pd.DataFrame) -> list[pd.DataFrame]:
    batches = []
    for _, batch_df in sessions_df.groupby(
        ["contract_code", "trading_date"],
        sort=True,
    ):
        batches.append(
            batch_df.sort_values("session_number").reset_index(drop=True)
        )
    return batches


# ## JQData 分钟响应归一化与 Session 对齐
# 
# JQData 单合约响应可能把时间放在索引中。这里统一为长表、严格转换数值，
# 再由待办 Session 区间筛选实际输出；休盘时间或请求边界之外的返回不会进入事实。
# 

# In[ ]:


def normalize_minute_response(
    raw_df: pd.DataFrame,
    contract_code: str,
) -> pd.DataFrame:
    columns = ["contract_code", "bar_at", *PRICE_FIELDS]
    if not isinstance(raw_df, pd.DataFrame):
        raise TypeError("schema_error: get_price 未返回 DataFrame。")
    if raw_df.empty:
        return pd.DataFrame(columns=columns)

    normalized_df = raw_df.copy()
    if "time" not in normalized_df.columns:
        normalized_df = normalized_df.rename_axis("time").reset_index()
    if "time" not in normalized_df.columns:
        normalized_df = normalized_df.rename(
            columns={normalized_df.columns[0]: "time"}
        )

    if "code" not in normalized_df.columns:
        normalized_df["code"] = contract_code

    required_columns = {"time", "code", *PRICE_FIELDS}
    missing_columns = required_columns - set(normalized_df.columns)
    if missing_columns:
        raise ValueError(
            f"schema_error: get_price 缺列 {sorted(missing_columns)}"
        )

    normalized_df = normalized_df.loc[
        :, ["code", "time", *PRICE_FIELDS]
    ].rename(columns={"code": "contract_code", "time": "bar_at"})
    normalized_df["contract_code"] = normalized_df["contract_code"].astype(str)
    if set(normalized_df["contract_code"]) - {contract_code}:
        raise ValueError("schema_error: get_price 返回未请求合约。")

    bar_at = pd.to_datetime(normalized_df["bar_at"], errors="raise")
    if bar_at.dt.tz is None:
        bar_at = bar_at.dt.tz_localize("Asia/Shanghai")
    else:
        bar_at = bar_at.dt.tz_convert("Asia/Shanghai")
    normalized_df["bar_at"] = bar_at

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
        raise ValueError("schema_error: get_price 返回重复分钟主键。")
    return normalized_df


# In[ ]:


def collect_partition(
    jqdata: ModuleType,
    sessions_df: pd.DataFrame,
    updated_at: datetime,
) -> tuple[pd.DataFrame, int, dict[tuple[object, ...], int]]:
    collected_frames = []
    returned_rows = 0
    invalid_session_counts = {}

    for batch_number, batch_df in enumerate(
        request_batches(sessions_df),
        start=1,
    ):
        contract_code = str(batch_df["contract_code"].iloc[0])
        request_start = pd.Timestamp(batch_df["session_start_at"].min())
        request_end = pd.Timestamp(batch_df["session_end_at"].max())

        click.echo(
            "request_batch: "
            f"batch={batch_number}/{len(request_batches(sessions_df))}; "
            f"contract={contract_code}; "
            f"trading_date={batch_df['trading_date'].iloc[0]}; "
            f"sessions={len(batch_df)}; "
            f"start={request_start}; end={request_end}"
        )

        # JQData 接受无时区本地时间；业务时区含义仍由 Session 契约保存。
        raw_df = jqdata.get_price(
            contract_code,
            start_date=request_start.tz_localize(None),
            end_date=request_end.tz_localize(None),
            frequency="1m",
            fields=PRICE_FIELDS,
            skip_paused=True,
            fq=None,
            panel=False,
        )
        if raw_df is None:
            raise RuntimeError(
                f"retryable_error: get_price 返回 None：{contract_code}"
            )

        normalized_df = normalize_minute_response(raw_df, contract_code)
        returned_rows += len(normalized_df)

        # 日历是输出键唯一驱动方；请求跨越休盘时间也不会写入区间外 bar。
        for session in pandas_to_arrow(
            batch_df.loc[:, CALENDAR_SCHEMA.names],
            CALENDAR_SCHEMA,
        ).to_pylist():
            mask = (
                normalized_df["contract_code"].eq(session["contract_code"])
                & normalized_df["bar_at"].gt(session["session_start_at"])
                & normalized_df["bar_at"].le(session["session_end_at"])
            )
            selected_df = normalized_df.loc[mask].copy()
            if selected_df.empty:
                continue

            # 供应商偶发返回有限 OHLC 跨列关系异常。原值仍进入事实表，
            # 同时按 Session 计数并输出逐行审计；异常不被修写，也不伪装成缺失。
            invalid_ohlc = invalid_ohlc_mask(selected_df)
            if invalid_ohlc.any():
                session_key = (
                    session["contract_code"],
                    session["trading_date"],
                    session["session_number"],
                )
                invalid_df = selected_df.loc[invalid_ohlc]
                invalid_session_counts[session_key] = (
                    invalid_session_counts.get(session_key, 0) + len(invalid_df)
                )
                for invalid_row in invalid_df.itertuples(index=False):
                    click.echo(
                        "preserved_invalid_ohlc_bar: "
                        f"contract={invalid_row.contract_code}; "
                        f"bar_at={invalid_row.bar_at}; "
                        f"ohlc={(invalid_row.open, invalid_row.high, invalid_row.low, invalid_row.close)}"
                    )
            selected_df["exchange_code"] = session["exchange_code"]
            selected_df["underlying_code"] = session["underlying_code"]
            selected_df["trading_date"] = session["trading_date"]
            selected_df["session_number"] = session["session_number"]
            selected_df["source"] = SOURCE_NAME
            selected_df["updated_at"] = updated_at
            selected_df["year"] = session["year"]
            selected_df["month"] = session["month"]
            collected_frames.append(selected_df.loc[:, SCHEMA.names])

    collected_df = (
        pd.concat(collected_frames, ignore_index=True)
        if collected_frames
        else empty_pandas(SCHEMA)
    )
    return (
        validate_minute_frame(
            collected_df,
            "JQData 分钟转换结果",
            sessions_df,
        ),
        returned_rows,
        invalid_session_counts,
    )


# ## 完整叶分区原子提交
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
        else:
            # 零行也是一次完整 API 结论；在叶分区写零行文件，使空 Session 可被正式复读证明。
            empty_partition_path = staging_path / relative_path
            empty_partition_path.mkdir(parents=True, exist_ok=False)
            pq.write_table(
                pa.Table.from_batches(
                    [],
                    schema=parquet_file_schema(schema, partition_columns),
                ),
                empty_partition_path / "part-0.parquet",
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


# ## 完整分区读取、事实合并与正式复读
# 
# 待办 Session 重新请求时，先从目标分区删除这些 Session 的旧分钟，再合并本次 API 实际返回；
# 同一品种月内未触达 Session 和白名单外历史分钟原样保留。
# 

# In[ ]:


def read_fact_partition(
    dataset: ds.Dataset | None,
    partition_key: tuple[object, ...],
    partition_keys: set[tuple[object, ...]],
) -> pd.DataFrame:
    if dataset is None or partition_key not in partition_keys:
        return empty_pandas(SCHEMA)

    table = dataset.to_table(
        columns=SCHEMA.names,
        filter=partition_expression(PARTITION_COLUMNS, partition_key),
    )
    return validate_minute_frame(
        arrow_to_pandas(table, SCHEMA),
        "现有分钟事实完整分区",
    )


def fact_partition_contract_error(
    table_path: pathlib.Path,
    partition_key: tuple[object, ...],
    dataset_contract_error: str | None,
) -> str | None:
    # 完成判定按叶分区检查文件 metadata，避免白名单外旧分区让所有已迁移分区反复重拉。
    relative_path = pathlib.Path(
        *[
            f"{name}={value}"
            for name, value in zip(
                PARTITION_COLUMNS,
                partition_key,
                strict=True,
            )
        ]
    )
    partition_path = table_path / relative_path
    expected_schema = parquet_file_schema(SCHEMA, PARTITION_COLUMNS)
    data_files = list(partition_path.rglob("*.parquet"))

    if data_files:
        for parquet_path in data_files:
            actual_schema = pq.read_schema(parquet_path)
            if not actual_schema.equals(expected_schema, check_metadata=True):
                return f"叶分区文件 metadata 过期：{parquet_path}"
        return None

    # API 确认空的 Session 没有事实文件，由精确零行 marker 证明表契约已建立。
    marker_path = table_path / "schema.parquet"
    if marker_path.exists():
        marker_schema = pq.read_schema(marker_path)
        marker_metadata = pq.read_metadata(marker_path)
        if (
            marker_schema.equals(expected_schema, check_metadata=True)
            and marker_metadata.num_rows == 0
        ):
            return None

    # 有其他精确事实文件且全表契约精确时，当前无文件分区也是合法空分区。
    if dataset_contract_error is None and table_path.is_dir():
        if any(table_path.rglob("*.parquet")):
            return None
    return "分钟事实尚未建立当前契约，或空分区缺少精确契约标记。"


# In[ ]:


def commit_fact_partition(
    collected_df: pd.DataFrame,
    existing_df: pd.DataFrame,
    pending_sessions_df: pd.DataFrame,
    partition_key: tuple[object, ...],
    lake_root: pathlib.Path,
) -> pd.DataFrame:
    collected_df = validate_minute_frame(
        collected_df,
        "本次分钟采集结果",
        pending_sessions_df,
    )
    existing_df = validate_minute_frame(existing_df, "合并前分钟完整分区")

    # 只删除真正待办 Session 的旧分钟；未触达和白名单外历史数据继续保留。
    stale_df = rows_for_sessions(existing_df, pending_sessions_df)
    stale_keys = set(stale_df[PRIMARY_KEY].itertuples(index=False, name=None))
    existing_keys = existing_df[PRIMARY_KEY].apply(tuple, axis=1)
    retained_df = existing_df.loc[~existing_keys.isin(stale_keys), SCHEMA.names]

    desired_df = validate_minute_frame(
        pd.concat(
            [retained_df, collected_df.loc[:, SCHEMA.names]],
            ignore_index=True,
        ),
        "合并后分钟完整分区",
    )
    committed_df = commit_complete_partition(
        desired_df,
        lake_root,
        TABLE_NAME,
        SCHEMA,
        PARTITION_COLUMNS,
        HIVE_PARTITIONING,
        partition_key,
        validate_minute_frame,
    )

    # 从正式路径复读后，只返回本次待办 Session 的最终事实行。
    committed_pending_df = rows_for_sessions(
        committed_df,
        pending_sessions_df,
    )
    committed_pending_df = validate_minute_frame(
        committed_pending_df,
        "正式复读的本次分钟事实",
        pending_sessions_df,
    )

    expected_table = pandas_to_arrow(
        collected_df.sort_values(PRIMARY_KEY).loc[:, SCHEMA.names],
        SCHEMA,
    )
    actual_table = pandas_to_arrow(
        committed_pending_df.sort_values(PRIMARY_KEY).loc[:, SCHEMA.names],
        SCHEMA,
    )
    if not actual_table.equals(expected_table):
        raise ValueError("正式复读的待办 Session 与本次 JQData 结果不一致。")
    return committed_pending_df


# ## 政策提交与事实完成状态回写
# 
# 政策选择可以单独提交；事实完成状态只能在对应分钟分区正式复读后更新。
# 同一日历分区中的日线和其他品种状态必须完整保留。
# 

# In[ ]:


def commit_calendar_policy_partitions(
    calendar_df: pd.DataFrame,
    changed_keys: set[tuple[object, ...]],
    lake_root: pathlib.Path,
) -> int:
    if not changed_keys:
        return 0

    key_series = calendar_df[CALENDAR_PRIMARY_KEY].apply(tuple, axis=1)
    changed_df = calendar_df.loc[key_series.isin(changed_keys)]
    partition_keys = set(
        changed_df[CALENDAR_PARTITION_COLUMNS].itertuples(
            index=False,
            name=None,
        )
    )

    for partition_key in sorted(partition_keys):
        mask = pd.Series(True, index=calendar_df.index)
        for column, value in zip(
            CALENDAR_PARTITION_COLUMNS,
            partition_key,
            strict=True,
        ):
            mask &= calendar_df[column].eq(value)

        complete_df = validate_calendar_state_frame(
            calendar_df.loc[mask, CALENDAR_SCHEMA.names],
            "政策更新后的日历完整分区",
        )
        commit_complete_partition(
            complete_df,
            lake_root,
            CALENDAR_TABLE_NAME,
            CALENDAR_SCHEMA,
            CALENDAR_PARTITION_COLUMNS,
            CALENDAR_PARTITIONING,
            partition_key,
            validate_calendar_state_frame,
        )

    return len(changed_keys)


# In[ ]:


def update_calendar_completion(
    sessions_df: pd.DataFrame,
    formal_fact_df: pd.DataFrame,
    lake_root: pathlib.Path,
    fetch_run_id: str,
    completed_at: datetime,
    invalid_session_counts: dict[tuple[object, ...], int],
) -> int:
    formal_fact_df = validate_minute_frame(
        formal_fact_df,
        "状态回写事实输入",
        sessions_df,
    )
    if sessions_df.empty:
        return 0

    silver_root = lake_root.resolve() / "silver"
    calendar_path = silver_root / CALENDAR_TABLE_NAME
    counts = (
        formal_fact_df.groupby(SESSION_KEY, dropna=False).size().to_dict()
        if not formal_fact_df.empty
        else {}
    )
    formal_invalid_df = formal_fact_df.loc[
        invalid_ohlc_mask(formal_fact_df)
    ]
    formal_invalid_counts = (
        formal_invalid_df.groupby(SESSION_KEY, dropna=False).size().to_dict()
        if not formal_invalid_df.empty
        else {}
    )
    reported_invalid_counts = {
        key: int(value)
        for key, value in invalid_session_counts.items()
        if int(value) > 0
    }
    if formal_invalid_counts != reported_invalid_counts:
        raise ValueError(
            "正式事实中的 OHLC 异常计数与本次响应审计不一致。"
        )
    updated_count = 0

    for partition_values, partition_sessions_df in sessions_df.groupby(
        ["exchange_code", "year", "month"],
        sort=True,
    ):
        exchange_code, year, month = partition_values
        partition_key = ("1m", exchange_code, int(year), int(month))

        # 每次重开正式日历，确保前一个品种的状态提交已经可见。
        calendar_dataset, _ = open_contract_dataset(
            calendar_path,
            CALENDAR_PARTITIONING,
            CALENDAR_SCHEMA,
            "行情日历",
            required=True,
        )
        table = calendar_dataset.to_table(
            columns=CALENDAR_SCHEMA.names,
            filter=partition_expression(
                CALENDAR_PARTITION_COLUMNS,
                partition_key,
            ),
        )
        calendar_df = validate_calendar_state_frame(
            arrow_to_pandas(table, CALENDAR_SCHEMA),
            "状态回写前日历完整分区",
        )

        session_rows = {
            (
                row["contract_code"],
                row["trading_date"],
                row["session_number"],
            ): row
            for row in pandas_to_arrow(
                partition_sessions_df.loc[:, CALENDAR_SCHEMA.names],
                CALENDAR_SCHEMA,
            ).to_pylist()
        }
        matched_keys = set()

        for index, row in calendar_df.iterrows():
            key = (
                row["contract_code"],
                row["trading_date"],
                row["session_number"],
            )
            source_session = session_rows.get(key)
            if source_session is None:
                continue
            if row["bar_frequency"] != "1m" or not row["is_fetch_required"]:
                raise ValueError("状态回写命中了非待采集分钟 Session。")

            actual_count = int(counts.get(key, 0))
            invalid_count = int(formal_invalid_counts.get(key, 0))
            expected_count = int(row["expected_bar_count"])
            (
                missing_count,
                quality_status,
                quality_reason,
            ) = minute_session_quality(
                expected_count,
                actual_count,
                invalid_count,
            )

            # 成功请求并经正式路径复读后，0 条事实是疑似休市信号，
            # 不是确认休市。它不会取消 is_fetch_required，后续只交给 c07 做旁证校对。
            if actual_count == 0:
                calendar_df.at[index, "schedule_status"] = "suspected_closed"
                calendar_df.at[index, "schedule_signal_reason"] = (
                    "JQData 分钟请求成功且正式复读为 0 条，"
                    "Session 疑似休市，等待 c07 定向校对。"
                )
                calendar_df.at[index, "evidence_level"] = "inferred"
                calendar_df.at[index, "evidence_source"] = (
                    "fact_futures_minute:formal_empty_session"
                )
            elif invalid_count > 0:
                # 异常行作为正式事实保留，因此保持计划开市并显式记录异常证据。
                calendar_df.at[index, "schedule_status"] = "scheduled"
                calendar_df.at[index, "schedule_signal_reason"] = (
                    "JQData 返回分钟行，但供应商原始 OHLC 跨列关系异常；"
                    "原值已保留并保持计划开市。"
                )
                calendar_df.at[index, "evidence_level"] = "inferred"
                calendar_df.at[index, "evidence_source"] = (
                    "fact_futures_minute:invalid_ohlc"
                )
            elif row["schedule_status"] == "suspected_closed":
                # 正式事实恢复非空时，撤销由空响应产生的疑似休市结论。
                calendar_df.at[index, "schedule_status"] = "scheduled"
                calendar_df.at[index, "schedule_signal_reason"] = (
                    "分钟事实正式复读非空，取消由空响应形成的疑似休市信号。"
                )
                calendar_df.at[index, "evidence_level"] = "inferred"
                calendar_df.at[index, "evidence_source"] = (
                    "fact_futures_minute:formal_nonempty_session"
                )

            # c06 每次形成新的分钟事实状态时，都清空旧 c07 旁证，
            # 避免把旧日线或旧分钟聚合结论带入下一轮自动校对。
            for evidence_column in EVIDENCE_COLUMNS:
                calendar_df.at[index, evidence_column] = None

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

        missing_keys = set(session_rows) - matched_keys
        if missing_keys:
            raise ValueError(
                f"待回写 Session 无法匹配正式日历：{sorted(missing_keys)[:5]}"
            )

        complete_df = validate_calendar_state_frame(
            calendar_df,
            "状态回写后日历完整分区",
        )
        commit_complete_partition(
            complete_df,
            lake_root,
            CALENDAR_TABLE_NAME,
            CALENDAR_SCHEMA,
            CALENDAR_PARTITION_COLUMNS,
            CALENDAR_PARTITIONING,
            partition_key,
            validate_calendar_state_frame,
        )
        updated_count += len(session_rows)

    return updated_count


# ## CLI：政策、计划、配额、分区采集与状态回写
# 
# - 不传日期：评估完整 1m 日历并自动补缺；空湖自然得到全量事实计划。
# - 传日期：只处理该日期视图；仅在明确不同于正式湖的测试湖才允许同时 `--write`。
# - 不传 `--write`：仍认证、调用 JQData 并完成内存质检，但不提交政策、事实或状态。
# - 配额在每个事实分区前检查；不足时正常停止，不等待、不自动恢复。
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
    # 1. 正式湖路径只来自 settings；日期写入门禁发生在任何读取和认证之前。
    formal_lake_root = settings.futures_lake_root.resolve()
    resolved_lake_root = (lake_root or formal_lake_root).resolve()

    has_explicit_dates = start_date is not None or end_date is not None
    if (start_date is None) != (end_date is None):
        raise click.UsageError("--start-date 与 --end-date 必须同时提供。")
    if has_explicit_dates and write and resolved_lake_root == formal_lake_root:
        raise click.UsageError(
            "显式指定日期时禁止写入 FUTURES_LAKE_ROOT 指向的正式湖；"
            "请移除日期参数使用自动补缺，或改用非正式测试湖。"
        )

    requested_start_date = start_date.date() if start_date else None
    requested_end_date = end_date.date() if end_date else None
    if (
        requested_start_date is not None
        and requested_start_date > requested_end_date
    ):
        raise click.BadParameter("起始日期不得晚于结束日期。")

    silver_root = resolved_lake_root / "silver"
    calendar_path = silver_root / CALENDAR_TABLE_NAME
    target_path = silver_root / TABLE_NAME

    # 2. c04 日历是必需上游；c06 不自行猜测合约、交易日或 Session。
    calendar_dataset, _ = open_contract_dataset(
        calendar_path,
        CALENDAR_PARTITIONING,
        CALENDAR_SCHEMA,
        "上游行情日历",
        required=True,
    )
    calendar_df = validate_calendar_state_frame(
        read_dataset_frame(calendar_dataset, CALENDAR_SCHEMA),
        "上游完整行情日历",
    )

    run_updated_at = datetime.now(timezone.utc)
    fetch_run_id = (
        f"minute-{run_updated_at:%Y%m%dT%H%M%SZ}-"
        f"{uuid.uuid4().hex[:8]}"
    )

    # 3. 在内存中对完整理论格点应用政策；显式日期只限制本次视图。
    policy_calendar_df, policy_changed_keys = apply_minute_policy(
        calendar_df,
        requested_start_date,
        requested_end_date,
        run_updated_at,
    )
    selected_mask = (
        policy_calendar_df["bar_frequency"].eq("1m")
        & policy_calendar_df["is_fetch_required"].eq(True)
    )
    if requested_start_date is not None:
        selected_mask &= policy_calendar_df["trading_date"].ge(
            requested_start_date
        )
        selected_mask &= policy_calendar_df["trading_date"].le(
            requested_end_date
        )
    selected_sessions_df = policy_calendar_df.loc[
        selected_mask,
        CALENDAR_SCHEMA.names,
    ].reset_index(drop=True)

    # 4. 目标事实允许空湖；metadata 旧时触达 Session 全部重新进入待办。
    target_dataset, target_contract_error = open_contract_dataset(
        target_path,
        HIVE_PARTITIONING,
        SCHEMA,
        "现有分钟事实",
        required=False,
        allow_metadata_upgrade=True,
    )
    target_partition_keys = partition_keys_from_files(
        target_path,
        PARTITION_COLUMNS,
    )

    plans = []
    complete_count = 0
    pending_count = 0
    expected_pending_rows = 0

    session_groups = selected_sessions_df.groupby(
        PARTITION_COLUMNS,
        sort=True,
    )
    partition_total = session_groups.ngroups
    for partition_number, (partition_values, sessions_df) in enumerate(
        session_groups,
        start=1,
    ):
        partition_key = tuple(partition_values)
        if partition_number == 1 or partition_number % 25 == 0:
            click.echo(
                "planning_progress: "
                f"table={TABLE_NAME}; "
                f"partitions={partition_number}/{partition_total}; "
                f"key={partition_key}"
            )
        existing_df = read_fact_partition(
            target_dataset,
            partition_key,
            target_partition_keys,
        )
        partition_fact_df = rows_for_sessions(existing_df, sessions_df)
        partition_fact_df = validate_minute_frame(
            partition_fact_df,
            "正式事实中的当前上游 Session",
            sessions_df,
        )

        partition_contract_error = fact_partition_contract_error(
            target_path,
            partition_key,
            target_contract_error,
        )
        complete_keys = complete_session_keys(
            sessions_df,
            partition_fact_df,
            partition_contract_error,
        )
        pending_keys = session_key_set(sessions_df) - complete_keys
        pending_sessions_df = rows_for_session_keys(sessions_df, pending_keys)

        complete_count += len(complete_keys)
        pending_count += len(pending_keys)
        expected_pending_rows += int(
            pending_sessions_df["expected_bar_count"].sum()
        )
        if pending_keys:
            plans.append(
                {
                    "partition_key": partition_key,
                    "pending_sessions_df": pending_sessions_df,
                    "expected_rows": int(
                        pending_sessions_df["expected_bar_count"].sum()
                    ),
                }
            )

    mode = "explicit" if has_explicit_dates else "automatic"
    plan_name = "explicit_plan" if has_explicit_dates else "auto_plan"
    click.echo(
        f"{plan_name}: table={TABLE_NAME}; "
        f"policy_changed={len(policy_changed_keys)}; "
        f"selected_sessions={len(selected_sessions_df)}; "
        f"complete_sessions={complete_count}; "
        f"pending_sessions={pending_count}; "
        f"pending_partitions={len(plans)}; "
        f"expected_pending_rows={expected_pending_rows}; "
        f"contract_error={target_contract_error}"
    )

    # 5. --write 时先保存 c06 的政策决定；不带 --write 时只保留内存视图。
    if write and policy_changed_keys:
        committed_policy_count = commit_calendar_policy_partitions(
            policy_calendar_df,
            policy_changed_keys,
            resolved_lake_root,
        )
        click.echo(f"policy_committed: rows={committed_policy_count}")

    if not plans:
        click.echo(f"up_to_date: table={TABLE_NAME}; mode={mode}")
        return

    # 6. 仅确有事实待办时认证；凭据不写日志。
    from config.jqdata_connection import authenticate_jqdata

    jqdata = authenticate_jqdata(
        settings.jqdata_id,
        settings.jqdata_secret,
    )

    completed_partitions = 0
    completed_sessions = 0
    committed_rows = 0
    returned_rows = 0
    invalid_ohlc_rows = 0

    # 7. 每个事实叶分区独立完成配额检查、采集、提交、复读和状态回写。
    for partition_number, plan in enumerate(plans, start=1):
        partition_key = plan["partition_key"]
        expected_rows = int(plan["expected_rows"])

        spare = quota_spare(jqdata)
        if spare is not None and spare - expected_rows < quota_reserve:
            click.echo(
                "quota_stop: "
                f"partition={partition_number}/{len(plans)}; "
                f"key={partition_key}; spare={spare}; "
                f"expected={expected_rows}; reserve={quota_reserve}; "
                f"completed_partitions={completed_partitions}; "
                "未请求或写入当前分区。"
            )
            return

        click.echo(
            "partition_start: "
            f"partition={partition_number}/{len(plans)}; "
            f"key={partition_key}; "
            f"sessions={len(plan['pending_sessions_df'])}; "
            f"expected_rows={expected_rows}"
        )
        (
            collected_df,
            partition_returned_rows,
            invalid_session_counts,
        ) = collect_partition(
            jqdata,
            plan["pending_sessions_df"],
            run_updated_at,
        )
        returned_rows += partition_returned_rows
        partition_invalid_rows = sum(invalid_session_counts.values())
        invalid_ohlc_rows += partition_invalid_rows

        if not write:
            continue

        # 只在即将提交当前分区时保留其既有事实，避免全量计划长期缓存全部分钟。
        existing_df = read_fact_partition(
            target_dataset,
            partition_key,
            target_partition_keys,
        )
        formal_fact_df = commit_fact_partition(
            collected_df,
            existing_df,
            plan["pending_sessions_df"],
            partition_key,
            resolved_lake_root,
        )
        state_rows = update_calendar_completion(
            plan["pending_sessions_df"],
            formal_fact_df,
            resolved_lake_root,
            fetch_run_id,
            run_updated_at,
            invalid_session_counts,
        )

        completed_partitions += 1
        completed_sessions += state_rows
        committed_rows += len(formal_fact_df)
        click.echo(
            "partition_committed: "
            f"key={partition_key}; fact_rows={len(formal_fact_df)}; "
            f"state_rows={state_rows}; "
            f"invalid_ohlc_rows={partition_invalid_rows}"
        )

    if not write:
        click.echo(
            f"api_success: partitions={len(plans)}; "
            f"sessions={pending_count}; returned_rows={returned_rows}; "
            f"invalid_ohlc_rows={invalid_ohlc_rows}; "
            "write=False"
        )
        return

    # 8. 最终摘要不扫描全量分钟事实；每个触达分区已经单独正式复读。
    click.echo(
        "committed: mode=automatic_gap_fill" if not has_explicit_dates
        else "committed: mode=explicit_non_formal"
    )
    click.echo(
        f"completed_partitions={completed_partitions}; "
        f"completed_sessions={completed_sessions}; "
        f"fact_rows={committed_rows}; returned_rows={returned_rows}; "
        f"invalid_ohlc_rows={invalid_ohlc_rows}; "
        "remaining_current_plan=0"
    )


if __name__ == "__main__":
    main()

