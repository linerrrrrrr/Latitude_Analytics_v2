#!/usr/bin/env python
# coding: utf-8

# # c05_futures_daily
# 
# 目标表：`fact_futures_daily`。
# 
# 日常入口只投影 `dim_futures_bar_calendar` 的日线主键、白名单身份、当前 required 与完成证据，
# 向量化评估全历史白名单。API 待办严格等于“当前 required 且从未完成”的格点；已经完成的
# 缺失占位、warning、零条结果都不重新请求。白名单缩小时保留既有事实、完成批次、实际条数、
# 质量结论和 c07 证据；仅在 required 从真切换为假时清零当前缺失状态。以后重新纳入时，已有完成
# 证据仍然阻止重复拉取。
# 
# 日线统一来自 JQData：`get_price(frequency="daily")` 提供价格、成交量、元计成交额与昨收；
# `get_extras("futures_sett_price")` 和 `get_extras("futures_positions")` 提供结算价与持仓量。
# 有限 OHLC 跨列异常保留供应商原值并写 `warning`；重复键、响应越界、NaN/Inf 和负数量仍硬失败。
# 
# 命令启动即执行待办采集与内存校验；`--write` 只决定是否提交事实和日历状态。

# ## 项目定位与依赖
# 
# 先按项目统一标记定位根目录，再导入配置、权威 Arrow Schema 和共享期货事实采集白名单。根目录定位必须在配置导入之前完成，
# 这样从仓库根目录或任意业务子目录执行，读取的都仍是同一份 `.env` 和数据契约。
# 

# In[ ]:


from __future__ import annotations

# 标准库负责数值校验、路径管理、原子替换和运行批次标识。
import math
import numpy as np
import pathlib
import shutil
import sys
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from types import ModuleType


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
import pyarrow.compute as pc
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


# ## Schema 契约呈现

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


# 表名、主键和 Hive 分区只从具名权威 Schema metadata 读取一次。
TABLE_NAME = FUTURES_DAILY_SCHEMA.metadata[b"table_name"].decode("utf-8")
CALENDAR_TABLE_NAME = FUTURES_BAR_CALENDAR_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")
PRIMARY_KEY = FUTURES_DAILY_SCHEMA.metadata[b"primary_key"].decode(
    "utf-8"
).split(",")
CALENDAR_PRIMARY_KEY = FUTURES_BAR_CALENDAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")
PARTITION_COLUMNS = FUTURES_DAILY_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")
CALENDAR_PARTITION_COLUMNS = FUTURES_BAR_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")

# 日常规划只读取决定白名单变化和 API 待办所需的窄列。
CALENDAR_PLANNING_COLUMNS = [
    *CALENDAR_PRIMARY_KEY,
    "exchange_code",
    "underlying_code",
    "schedule_status",
    "is_fetch_required",
    "is_fetch_completed",
    "year",
    "month",
]
CALENDAR_PLANNING_SCHEMA = pa.schema([
    FUTURES_BAR_CALENDAR_SCHEMA.field(name)
    for name in CALENDAR_PLANNING_COLUMNS
])

PRICE_FIELDS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "money",
    "pre_close",
]
MARKET_VALUE_COLUMNS = [
    "previous_close",
    "previous_settlement",
    "open",
    "high",
    "low",
    "close",
    "settlement",
    "close_change_from_previous_settlement",
    "settlement_change_from_previous_settlement",
    "volume",
    "money",
    "open_interest",
    "open_interest_change",
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
    "未命中国内期货事实采集白名单；保留理论格点和既有事实，"
    "当前不要求采集日线事实。"
)

MAX_CONTRACTS_PER_REQUEST = 100
MAX_ESTIMATED_VALUES_PER_BATCH = 90_000
LOOKBACK_CALENDAR_DAYS = 45
DEFAULT_QUOTA_RESERVE = 5_000_000

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

HIVE_PARTITIONING = ds.partitioning(
    pa.schema([
        FUTURES_DAILY_SCHEMA.field(name)
        for name in PARTITION_COLUMNS
    ]),
    flavor="hive",
)
CALENDAR_PARTITIONING = ds.partitioning(
    pa.schema([
        FUTURES_BAR_CALENDAR_SCHEMA.field(name)
        for name in CALENDAR_PARTITION_COLUMNS
    ]),
    flavor="hive",
)


# ## 上游有效日线格点校验
# 
# `c04` 已对行情日历承担完整生产者质检。`c05` 作为消费者只检查自己直接依赖的边界：
# 必须是 `1d` 格点、每个合约—交易日只有一行，并且分区和合约代码一致；是否需要拉取由 c05 随后应用白名单决定。
# 

# ## 日线事实完整性校验
# 
# 一行只有同时满足 Arrow Schema/metadata、主键、来源、有限数、非负度量、缺失占位和派生值规则，
# 才属于“已经完整落盘”。有限数之间的 OHLC 跨列异常不再否定事实完整性，而由日历 `warning` 留痕；
# 校验仍用于现有事实求差、转换结果、staging 和正式路径复读。
# 

# In[ ]:


def close_mask(
    left: pd.Series,
    right: pd.Series,
) -> pd.Series:
    left_values = left.to_numpy(dtype="float64", na_value=np.nan)
    right_values = right.to_numpy(dtype="float64", na_value=np.nan)
    return pd.Series(
        np.isclose(
            left_values,
            right_values,
            rtol=1e-10,
            atol=1e-8,
            equal_nan=False,
        ),
        index=left.index,
    )


def invalid_ohlc_mask(frame: pd.DataFrame) -> pd.Series:
    comparable_high = frame[["open", "close", "low"]].max(
        axis=1,
        skipna=True,
    )
    comparable_low = frame[["open", "close", "high"]].min(
        axis=1,
        skipna=True,
    )
    high_invalid = (
        frame["high"].notna()
        & comparable_high.notna()
        & frame["high"].lt(comparable_high)
    )
    low_invalid = (
        frame["low"].notna()
        & comparable_low.notna()
        & frame["low"].gt(comparable_low)
    )
    return (high_invalid | low_invalid).astype(bool)


# In[ ]:


def validate_daily_frame(
    frame: pd.DataFrame,
    context: str,
    *,
    validate_predecessor_relationships: bool = True,
) -> pd.DataFrame:
    source_df = frame.loc[:, FUTURES_DAILY_SCHEMA.names].copy()

    # Arrow 会把 Pandas NaN 转成 null，因此先在原输入中拒绝 NaN/Inf。
    for column in MARKET_VALUE_COLUMNS:
        explicit_nan = np.fromiter(
            (
                isinstance(value, (float, np.floating))
                and np.isnan(value)
                for value in source_df[column].array
            ),
            dtype=bool,
            count=len(source_df),
        )
        if explicit_nan.any():
            raise ValueError(f"{context}{column} 包含非有限数。")
        original_non_null = source_df[column].notna()
        numeric_values = pd.to_numeric(
            source_df[column], errors="coerce"
        ).to_numpy(dtype="float64", na_value=np.nan)
        if (original_non_null.to_numpy() & ~np.isfinite(numeric_values)).any():
            raise ValueError(f"{context}{column} 包含非有限数。")

    table = pandas_to_arrow(source_df, FUTURES_DAILY_SCHEMA)
    checked_df = arrow_to_pandas(table, FUTURES_DAILY_SCHEMA)
    checked_df = checked_df.sort_values(PRIMARY_KEY).reset_index(drop=True)
    if checked_df.duplicated(PRIMARY_KEY).any():
        raise ValueError(f"{context}主键不唯一。")
    if checked_df.empty:
        return checked_df

    contract_codes = checked_df["contract_code"].astype("string")
    contract_symbols = contract_codes.str.split(".", n=1, regex=False).str[0]
    contract_exchanges = contract_codes.str.rsplit(".", n=1).str[-1]
    derived_underlying = contract_symbols.str.extract(
        r"^([A-Za-z]+)", expand=False
    ).str.upper()
    if (
        ~contract_codes.str.contains(".", regex=False)
        | contract_exchanges.ne(
            checked_df["exchange_code"].astype("string")
        )
    ).any():
        raise ValueError(f"{context}交易所与合约代码后缀不一致。")
    if checked_df["underlying_code"].astype("string").ne(
        derived_underlying
    ).any():
        raise ValueError(f"{context}品种代码无法由合约代码复算。")
    if (
        checked_df["year"].ne(checked_df["trading_date"].dt.year).any()
        or checked_df["month"].ne(
            checked_df["trading_date"].dt.month
        ).any()
    ):
        raise ValueError(f"{context}year/month 与 trading_date 不一致。")
    if not checked_df["source"].isin(
        {SOURCE_NAME, *LEGACY_SOURCE_NAMES}
    ).all():
        raise ValueError(f"{context}source 与 JQData 日线契约不一致。")

    legacy_without_extras = checked_df["source"].isin(
        LEGACY_SOURCE_NAMES
    ) & checked_df[LEGACY_NULL_EXTRA_FIELDS].isna().all(axis=1)
    has_market_data = checked_df["has_market_data"].astype(bool)
    if (has_market_data & checked_df["close"].isna()).any():
        raise ValueError(f"{context}有效行情行必须具有 close。")
    if (
        ~has_market_data
        & checked_df[MARKET_VALUE_COLUMNS].notna().any(axis=1)
    ).any():
        raise ValueError(f"{context}缺失占位行的行情度量必须全部为空。")
    for column in ["volume", "money", "open_interest"]:
        if checked_df[column].dropna().lt(0).any():
            raise ValueError(f"{context}{column} 不得为负。")

    previous_settlement = checked_df["previous_settlement"]
    close_value = checked_df["close"]
    settlement = checked_df["settlement"]

    close_change_inputs = (
        ~legacy_without_extras
        & previous_settlement.notna()
        & close_value.notna()
    )
    expected_close_change = close_value - previous_settlement
    actual_close_change = checked_df[
        "close_change_from_previous_settlement"
    ]
    if (
        close_change_inputs
        & (
            actual_close_change.isna()
            | ~close_mask(actual_close_change, expected_close_change)
        )
    ).any():
        raise ValueError(f"{context}收盘价相对昨结涨跌额无法复算。")
    if (
        ~legacy_without_extras
        & ~close_change_inputs
        & actual_close_change.notna()
    ).any():
        raise ValueError(f"{context}收盘涨跌缺少必要输入。")

    settlement_change_inputs = (
        ~legacy_without_extras
        & previous_settlement.notna()
        & settlement.notna()
    )
    expected_settlement_change = settlement - previous_settlement
    actual_settlement_change = checked_df[
        "settlement_change_from_previous_settlement"
    ]
    if (
        settlement_change_inputs
        & (
            actual_settlement_change.isna()
            | ~close_mask(
                actual_settlement_change,
                expected_settlement_change,
            )
        )
    ).any():
        raise ValueError(f"{context}结算价相对昨结涨跌额无法复算。")
    if (
        ~legacy_without_extras
        & ~settlement_change_inputs
        & actual_settlement_change.notna()
    ).any():
        raise ValueError(f"{context}结算涨跌缺少必要输入。")

    # 只有合并后的 dirty 完整叶具有可比较的本地前序事实。API pending
    # 子集可能跨过已经完成的日期，不能把上一 pending 行误作上一交易日。
    if validate_predecessor_relationships:
        known_previous_settlement = checked_df.groupby(
            "contract_code", sort=False
        )["settlement"].transform(lambda series: series.ffill().shift())
        comparable_previous_settlement = (
            ~legacy_without_extras
            & known_previous_settlement.notna()
            & previous_settlement.notna()
        )
        if (
            comparable_previous_settlement
            & ~close_mask(previous_settlement, known_previous_settlement)
        ).any():
            raise ValueError(
                f"{context}previous_settlement 与前序事实不一致。"
            )

        known_previous_position = checked_df.groupby(
            "contract_code", sort=False
        )["open_interest"].transform(lambda series: series.ffill().shift())
        position_change_inputs = (
            ~legacy_without_extras
            & known_previous_position.notna()
            & checked_df["open_interest"].notna()
        )
        expected_position_change = (
            checked_df["open_interest"] - known_previous_position
        )
        actual_position_change = checked_df["open_interest_change"]
        if (
            position_change_inputs
            & (
                actual_position_change.isna()
                | ~close_mask(
                    actual_position_change,
                    expected_position_change,
                )
            )
        ).any():
            raise ValueError(f"{context}持仓变化无法由前序事实复算。")

    return checked_df


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
    table = pandas_to_arrow(
        frame.loc[:, FUTURES_BAR_CALENDAR_SCHEMA.names],
        FUTURES_BAR_CALENDAR_SCHEMA,
    )
    checked_df = arrow_to_pandas(table, FUTURES_BAR_CALENDAR_SCHEMA)
    checked_df = checked_df.sort_values(CALENDAR_PRIMARY_KEY).reset_index(
        drop=True
    )
    if checked_df.duplicated(CALENDAR_PRIMARY_KEY).any():
        raise ValueError(f"{context}主键不唯一。")
    if checked_df.empty:
        return checked_df

    enum_rules = {
        "schedule_status": SCHEDULE_STATUSES,
        "evidence_level": EVIDENCE_LEVELS,
        "quality_status": QUALITY_STATUSES,
    }
    for column, allowed_values in enum_rules.items():
        if not checked_df[column].isin(allowed_values).all():
            raise ValueError(f"{context}{column} 不在允许枚举中。")

    text_fields = [
        "schedule_signal_reason",
        "evidence_source",
        "selection_reason",
        "quality_reason",
    ]
    if checked_df[text_fields].apply(
        lambda series: series.str.strip().eq("")
    ).to_numpy().any():
        raise ValueError(f"{context}状态说明字段不得为空。")

    contract_codes = checked_df["contract_code"].astype("string")
    contract_exchanges = contract_codes.str.rsplit(".", n=1).str[-1]
    if (
        ~contract_codes.str.contains(".", regex=False)
        | contract_exchanges.ne(
            checked_df["exchange_code"].astype("string")
        )
    ).any():
        raise ValueError(f"{context}交易所与合约代码后缀不一致。")
    if (
        checked_df["year"].ne(checked_df["trading_date"].dt.year).any()
        or checked_df["month"].ne(
            checked_df["trading_date"].dt.month
        ).any()
    ):
        raise ValueError(f"{context}year/month 与 trading_date 不一致。")
    if checked_df["expected_bar_count"].le(0).any():
        raise ValueError(f"{context}expected_bar_count 必须大于 0。")
    if (
        checked_df["actual_bar_count"].lt(0).any()
        or checked_df["missing_bar_count"].lt(0).any()
    ):
        raise ValueError(f"{context}实际与缺失条数不得为负。")

    daily_df = checked_df.loc[checked_df["bar_frequency"].eq("1d")]
    minute_df = checked_df.loc[checked_df["bar_frequency"].eq("1m")]
    session_columns = [
        "session_text",
        "session_start_at",
        "session_end_at",
        "is_night_session",
    ]
    if (
        daily_df["session_number"].ne(0).any()
        or daily_df[session_columns].notna().to_numpy().any()
        or daily_df["expected_bar_count"].ne(1).any()
    ):
        raise ValueError(f"{context}日线 Session 结构不合法。")
    if not minute_df.empty:
        duration_seconds = (
            minute_df["session_end_at"] - minute_df["session_start_at"]
        ).dt.total_seconds().to_numpy(
            dtype="float64", na_value=float("nan")
        )
        if (
            minute_df["session_number"].le(0).any()
            or minute_df[session_columns].isna().to_numpy().any()
            or pd.isna(duration_seconds).any()
            or (duration_seconds <= 0).any()
            or (duration_seconds % 60 != 0).any()
            or minute_df["expected_bar_count"].ne(
                duration_seconds // 60
            ).any()
        ):
            raise ValueError(f"{context}分钟 Session 结构不合法。")

    confirmed_closed = checked_df["schedule_status"].eq(
        "confirmed_closed"
    )
    if (
        confirmed_closed
        & (
            checked_df["evidence_level"].ne("authoritative")
            | checked_df["is_fetch_required"]
        )
    ).any():
        raise ValueError(f"{context}确认休市必须具有权威证据且不得拉取。")

    completed = checked_df["is_fetch_completed"]
    missing_run_id = (
        checked_df["fetch_run_id"].isna()
        | checked_df["fetch_run_id"].eq("")
    )
    if (completed & (
        missing_run_id | checked_df["fetch_completed_at"].isna()
    )).any():
        raise ValueError(f"{context}完成状态缺少运行批次或完成时间。")
    if (~completed & checked_df["fetch_completed_at"].notna()).any():
        raise ValueError(f"{context}未完成格点不得具有完成时间。")

    unchecked = checked_df["missing_checked_at"].isna()
    if (
        unchecked
        & (
            checked_df["is_data_missing"]
            | checked_df["missing_bar_count"].ne(0)
        )
    ).any():
        raise ValueError(f"{context}未经检查不得记录缺失。")
    expected_missing = (
        checked_df["expected_bar_count"] - checked_df["actual_bar_count"]
    ).clip(lower=0).where(checked_df["is_fetch_required"], 0)
    checked_missing = ~unchecked
    if (
        checked_missing
        & checked_df["missing_bar_count"].ne(expected_missing)
    ).any():
        raise ValueError(f"{context}missing_bar_count 无法复算。")
    if (
        checked_missing
        & checked_df["is_data_missing"].ne(expected_missing.gt(0))
    ).any():
        raise ValueError(f"{context}is_data_missing 与缺失条数不一致。")
    if (
        checked_df["quality_status"].ne("pending")
        & checked_df["quality_checked_at"].isna()
    ).any():
        raise ValueError(f"{context}非 pending 状态缺少检查时间。")
    return checked_df


# ## 契约化 Dataset 与精确叶读取
# 
# 根 Dataset 只用于窄列规划和 Schema/metadata 物理边界；事实与日历写入前只打开实际 dirty 叶。

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


SCHEMA_IDENTITY_METADATA_KEYS = [
    b"table_name",
    b"primary_key",
    b"partition_columns",
]


def physically_and_identity_compatible(
    actual_schema: pa.Schema,
    expected_schema: pa.Schema,
) -> bool:
    """忽略描述 metadata；硬检查物理字段和稳定表身份。"""
    if actual_schema.names != expected_schema.names:
        return False
    if any(
        actual_field.type != expected_field.type
        or actual_field.nullable != expected_field.nullable
        for actual_field, expected_field in zip(
            actual_schema,
            expected_schema,
            strict=True,
        )
    ):
        return False
    actual_metadata = actual_schema.metadata or {}
    expected_metadata = expected_schema.metadata or {}
    return all(
        actual_metadata.get(key) == expected_metadata.get(key)
        and expected_metadata.get(key) is not None
        for key in SCHEMA_IDENTITY_METADATA_KEYS
    )


# In[ ]:


def open_contract_dataset(
    table_path: pathlib.Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    label: str,
    *,
    required: bool,
) -> ds.Dataset | None:
    first_parquet = (
        next(table_path.rglob("*.parquet"), None)
        if table_path.is_dir()
        else None
    )
    if first_parquet is None:
        if required:
            raise FileNotFoundError(f"{label}不存在：{table_path}")
        return None

    dataset = ds.dataset(
        table_path,
        format="parquet",
        partitioning=partitioning,
    )
    actual_schema = reconstructed_schema(dataset, schema)
    if not physically_and_identity_compatible(actual_schema, schema):
        raise TypeError(f"{label} 物理结构或表身份与契约不一致。")

    expected_file_schema = parquet_file_schema(
        schema,
        partitioning.schema.names,
    )
    for fragment in dataset.get_fragments():
        if not physically_and_identity_compatible(
            fragment.physical_schema,
            expected_file_schema,
        ):
            raise TypeError(
                f"{label} Parquet fragment 物理结构或表身份与契约不一致："
                f"{fragment.path}"
            )
    return dataset


# In[ ]:





# ## 窄列白名单规划
# 
# 日常规划对全历史 1d 格点只读取主键、白名单身份、当前 required 和完成证据。
# 中文原因、质量详情、c07 证据和事实表均不参与 API 待办判定。

# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# ## 白名单变化与 API 待办
# 
# 白名单变化只在布尔决定真正改变时产生 dirty 日历行；从纳入切换为排除时同步清零当前缺失状态。
# 说明文字本身变化不触发全历史同步。完成证据一旦成立，无论实际条数或质量状态如何都不重新拉取。

# In[ ]:


def daily_policy_plan(
    planning_table: pa.Table,
) -> tuple[pa.Table, pa.Array, pa.Array, pa.Array]:
    """用 Arrow compute 形成 dirty、pending 和 completed 精确掩码。"""
    planned_table = planning_table.cast(
        CALENDAR_PLANNING_SCHEMA, safe=True
    ).sort_by([(name, "ascending") for name in CALENDAR_PRIMARY_KEY])
    if bool(
        pc.any(pc.not_equal(planned_table["bar_frequency"], "1d")).as_py()
    ):
        raise ValueError("日线窄列规划只能包含 1d 格点。")
    if bool(
        pc.any(pc.not_equal(planned_table["session_number"], 0)).as_py()
    ):
        raise ValueError("日线窄列规划 session_number 必须为 0。")

    desired_required = pa.array(
        np.zeros(len(planned_table), dtype=bool)
    )
    selected_underlyings_by_exchange: dict[str, list[str]] = {}
    for exchange_code, underlying_code in FUTURES_FACT_VARIETY_PAIRS:
        selected_underlyings_by_exchange.setdefault(
            exchange_code, []
        ).append(underlying_code)
    for exchange_code, underlying_codes in sorted(
        selected_underlyings_by_exchange.items()
    ):
        exchange_mask = pc.equal(
            planned_table["exchange_code"], exchange_code
        )
        underlying_mask = pc.is_in(
            planned_table["underlying_code"],
            value_set=pa.array(
                sorted(underlying_codes),
                type=planned_table["underlying_code"].type,
            ),
        )
        desired_required = pc.or_(
            desired_required,
            pc.and_(exchange_mask, underlying_mask),
        )
    confirmed_closed = pc.equal(
        planned_table["schedule_status"], "confirmed_closed"
    )
    desired_required = pc.and_(
        desired_required, pc.invert(confirmed_closed)
    )
    policy_changed = pc.not_equal(
        planned_table["is_fetch_required"], desired_required
    )
    completion_evidence = planned_table["is_fetch_completed"]
    dirty_mask = policy_changed
    pending_mask = pc.and_(
        desired_required, pc.invert(completion_evidence)
    )
    completed_mask = pc.and_(desired_required, completion_evidence)
    desired_reason = pc.if_else(
        confirmed_closed,
        "权威证据确认休市；当前无需拉取日线事实。",
        pc.if_else(desired_required, SELECTED_REASON, EXCLUDED_REASON),
    )
    planned_table = planned_table.append_column(
        "desired_is_fetch_required", desired_required
    ).append_column("desired_selection_reason", desired_reason)
    planned_table = planned_table.append_column(
        "policy_changed", policy_changed
    )
    return planned_table, dirty_mask, pending_mask, completed_mask


def plan_daily_policy(
    planning_df: pd.DataFrame,
    start_date: date | None,
    end_date: date | None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    planning_table = pandas_to_arrow(
        planning_df.loc[:, CALENDAR_PLANNING_COLUMNS],
        CALENDAR_PLANNING_SCHEMA,
    )
    if start_date is not None:
        date_mask = pc.and_(
            pc.greater_equal(planning_table["trading_date"], start_date),
            pc.less_equal(planning_table["trading_date"], end_date),
        )
        planning_table = planning_table.filter(date_mask)
    planned_table, dirty_mask, pending_mask, completed_mask = (
        daily_policy_plan(planning_table)
    )
    return tuple(
        planned_table.filter(mask).to_pandas(types_mapper=pd.ArrowDtype)
        for mask in (dirty_mask, pending_mask, completed_mask)
    )


def apply_policy_to_calendar_leaf(
    calendar_df: pd.DataFrame,
    dirty_policy_df: pd.DataFrame,
    updated_at: datetime,
) -> pd.DataFrame:
    if dirty_policy_df.empty:
        return calendar_df.copy()

    desired_df = calendar_df.copy()
    calendar_index = pd.MultiIndex.from_frame(
        desired_df[CALENDAR_PRIMARY_KEY]
    )
    update_df = dirty_policy_df.set_index(CALENDAR_PRIMARY_KEY)
    if update_df.index.has_duplicates:
        raise ValueError("白名单 dirty 规划主键不唯一。")
    missing_keys = update_df.index.difference(calendar_index)
    if len(missing_keys):
        raise ValueError(
            f"白名单 dirty 格点无法匹配日历叶：{missing_keys[:5].tolist()}"
        )

    matched = calendar_index.isin(update_df.index)
    matched_index = calendar_index[matched]
    desired_required = update_df.loc[
        matched_index, "desired_is_fetch_required"
    ].to_numpy(dtype=bool)
    policy_changed = update_df.loc[
        matched_index, "policy_changed"
    ].to_numpy(dtype=bool)
    matched_rows = desired_df.index[matched]

    desired_df.loc[
        matched_rows, "is_fetch_required"
    ] = desired_required
    reason_rows = matched_rows[policy_changed]
    reason_values = update_df.loc[
        matched_index[policy_changed],
        "desired_selection_reason",
    ].to_numpy()
    desired_df.loc[reason_rows, "selection_reason"] = reason_values

    required_rows = matched_rows[desired_required]
    required_completed = (
        desired_df.loc[required_rows, "is_fetch_completed"].astype(bool)
        & desired_df.loc[required_rows, "missing_checked_at"].notna()
    )
    required_missing = (
        desired_df.loc[required_rows, "expected_bar_count"].astype("int64")
        - desired_df.loc[required_rows, "actual_bar_count"].astype("int64")
    ).clip(lower=0).where(required_completed, 0)
    desired_df.loc[required_rows, "missing_bar_count"] = (
        required_missing.to_numpy()
    )
    desired_df.loc[required_rows, "is_data_missing"] = (
        required_missing.gt(0).to_numpy()
    )

    excluded_rows = matched_rows[~desired_required]
    desired_df.loc[excluded_rows, "is_data_missing"] = False
    desired_df.loc[excluded_rows, "missing_bar_count"] = 0
    desired_df.loc[matched_rows, "updated_at"] = updated_at
    return desired_df


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
    for column in ["volume", "money"]:
        if normalized_df[column].dropna().lt(0).any():
            raise ValueError(
                f"schema_error: get_price {column} 不得为负。"
            )

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
    response_codes = set(map(str, raw_df.columns))
    missing_codes = set(contract_codes) - response_codes
    if missing_codes:
        raise ValueError(
            f"schema_error: {value_column} 缺少合约列 {sorted(missing_codes)}"
        )
    unexpected_codes = response_codes - set(contract_codes)
    if unexpected_codes:
        raise ValueError(
            f"schema_error: {value_column} 返回未请求合约 "
            f"{sorted(unexpected_codes)}"
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
    if (
        value_column == "open_interest"
        and normalized_df[value_column].dropna().lt(0).any()
    ):
        raise ValueError(
            "schema_error: open_interest 不得为负。"
        )

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

    # 所有来源日期都必须落在本次明确请求窗口内；越界响应是来源契约错误。
    for response_name, response_df in [
        ("get_price", price_df),
        ("futures_sett_price", settlement_df),
        ("futures_positions", position_df),
    ]:
        if (
            response_df["trading_date"].lt(request_start).any()
            or response_df["trading_date"].gt(request_end).any()
        ):
            raise ValueError(
                f"schema_error: {response_name} 返回日期越出请求范围。"
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

    # API 子集先执行逐行和请求边界门禁；跨日前序关系留到 dirty 完整叶。
    checked_df = validate_daily_frame(
        daily_df.loc[:, FUTURES_DAILY_SCHEMA.names],
        "JQData 转换结果",
        validate_predecessor_relationships=False,
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
    return pa.schema(
        [
            schema.field(name)
            for name in schema.names
            if name not in partition_columns
        ],
        metadata=schema.metadata,
    )


def partition_relative_path(
    partition_columns: list[str],
    partition_key: tuple[object, ...],
) -> pathlib.Path:
    return pathlib.Path(*[
        f"{name}={value}"
        for name, value in zip(
            partition_columns,
            partition_key,
            strict=True,
        )
    ])


def read_partition_leaf(
    table_path: pathlib.Path,
    schema: pa.Schema,
    partition_columns: list[str],
    partitioning: ds.Partitioning,
    partition_key: tuple[object, ...],
) -> pd.DataFrame:
    relative_path = partition_relative_path(
        partition_columns,
        partition_key,
    )
    leaf_path = table_path / relative_path
    if not leaf_path.is_dir() or not next(leaf_path.glob("*.parquet"), None):
        return empty_pandas(schema)

    leaf_dataset = ds.dataset(
        leaf_path,
        format="parquet",
        partitioning=partitioning,
        partition_base_dir=str(table_path),
    )
    expected_file_schema = parquet_file_schema(schema, partition_columns)
    for fragment in leaf_dataset.get_fragments():
        if not physically_and_identity_compatible(
            fragment.physical_schema,
            expected_file_schema,
        ):
            raise TypeError(
                f"正式叶 {partition_key} 包含不兼容 Parquet fragment："
                f"{fragment.path}"
            )
    actual_schema = reconstructed_schema(leaf_dataset, schema)
    if not physically_and_identity_compatible(actual_schema, schema):
        raise TypeError(
            f"正式叶 {partition_key} 物理结构或表身份与契约不一致。"
        )
    leaf_table = leaf_dataset.to_table(columns=schema.names)
    return arrow_to_pandas(leaf_table, schema)


# In[ ]:





# In[ ]:





# ## 轻量 staging 与协调安装
# 
# 调用方对每个 dirty 完整叶只执行一次业务校验；staging 仅检查物理 Schema、表身份、主键和行数。
# 事实叶与对应 c04 日历叶在同一进程内共同备份、安装和回滚，安装后不再扫描根 Dataset 或重复 validator。

# In[ ]:


def commit_validated_leaf_group(
    partition_specs: list[dict[str, object]],
    lake_root: pathlib.Path,
) -> int:
    if not partition_specs:
        return 0

    silver_root = lake_root.resolve() / "silver"
    run_id = uuid.uuid4().hex[:12]
    staging_root = silver_root / f".c05s-{run_id}"
    backup_root = silver_root / f".c05b-{run_id}"
    quarantine_root = silver_root / f".c05q-{run_id}"
    for path in (staging_root, backup_root, quarantine_root):
        if not path.resolve().is_relative_to(silver_root):
            raise ValueError(f"事务路径越出 silver 根目录：{path}")

    prepared_specs = []
    destinations = set()
    for spec in partition_specs:
        frame = spec["frame"]
        schema = spec["schema"]
        partition_columns = spec["partition_columns"]
        partition_key = spec["partition_key"]
        table_name = spec["table_name"]
        if frame.empty:
            raise ValueError("日常增量提交不得写空叶分区。")
        actual_keys = set(
            frame[partition_columns].itertuples(index=False, name=None)
        )
        if actual_keys != {partition_key}:
            raise ValueError("待提交数据越出指定 Hive 分区。")
        complete_table = pandas_to_arrow(frame, schema)
        relative_path = partition_relative_path(
            partition_columns,
            partition_key,
        )
        destination_key = (table_name, relative_path.as_posix())
        if destination_key in destinations:
            raise ValueError(f"事务包含重复目标叶：{destination_key}")
        destinations.add(destination_key)
        prepared_specs.append({
            **spec,
            "complete_table": complete_table,
            "relative_path": relative_path,
        })

    silver_root.mkdir(parents=True, exist_ok=True)
    checked_tables = set()
    for spec in prepared_specs:
        table_name = spec["table_name"]
        if table_name in checked_tables:
            continue
        checked_tables.add(table_name)
        target_table_path = silver_root / table_name
        marker_path = target_table_path / "schema.parquet"
        expected_marker_schema = parquet_file_schema(
            spec["schema"],
            spec["partition_columns"],
        )
        if marker_path.exists():
            marker_metadata = pq.read_metadata(marker_path)
            if marker_metadata.num_rows:
                raise ValueError("schema.parquet 必须是 0 行契约标记。")
            if not physically_and_identity_compatible(
                pq.read_schema(marker_path),
                expected_marker_schema,
            ):
                raise TypeError(
                    f"{table_name} schema.parquet 与权威契约不一致。"
                )
        elif target_table_path.is_dir() and next(
            target_table_path.rglob("*.parquet"),
            None,
        ) is not None:
            raise FileNotFoundError(
                f"{table_name} 已有数据但缺少 schema.parquet。"
            )

    staging_root.mkdir(parents=True, exist_ok=False)
    try:
        for spec in prepared_specs:
            staging_table_path = staging_root / spec["table_name"]
            ds.write_dataset(
                spec["complete_table"],
                staging_table_path,
                format="parquet",
                partitioning=spec["partitioning"],
                existing_data_behavior="delete_matching",
                basename_template="part-{i}.parquet",
            )

        # staging 只核对物理 Schema、主键唯一和行数，不重复业务 validator。
        for spec in prepared_specs:
            staging_table_path = staging_root / spec["table_name"]
            staging_leaf_path = staging_table_path / spec["relative_path"]
            parquet_files = list(staging_leaf_path.glob("*.parquet"))
            if not parquet_files:
                raise FileNotFoundError(
                    f"staging 缺少叶分区：{staging_leaf_path}"
                )
            expected_file_schema = parquet_file_schema(
                spec["schema"],
                spec["partition_columns"],
            )
            for parquet_path in parquet_files:
                if not physically_and_identity_compatible(
                    pq.read_schema(parquet_path),
                    expected_file_schema,
                ):
                    raise TypeError(
                        f"staging 文件 Schema 不一致：{parquet_path}"
                    )
            staged_dataset = ds.dataset(
                staging_leaf_path,
                format="parquet",
                partitioning=spec["partitioning"],
                partition_base_dir=str(staging_table_path),
            )
            staged_key_table = staged_dataset.to_table(
                columns=spec["primary_key"]
            )
            if len(staged_key_table) != len(spec["complete_table"]):
                raise ValueError("staging 行数与待提交完整叶不一致。")
            if staged_key_table.to_pandas().duplicated(
                spec["primary_key"]
            ).any():
                raise ValueError("staging 主键不唯一。")
    except Exception:
        shutil.rmtree(staging_root, ignore_errors=True)
        raise

    backup_root.mkdir(parents=True, exist_ok=False)
    moved_specs = []
    created_markers = []
    commit_succeeded = False
    try:
        # 同一日历叶涉及的事实叶与日历叶作为一个进程内协调事务安装。
        for spec in prepared_specs:
            table_name = spec["table_name"]
            target_table_path = silver_root / table_name
            destination_path = target_table_path / spec["relative_path"]
            source_path = (
                staging_root / table_name / spec["relative_path"]
            )
            saved_path = (
                backup_root / table_name / spec["relative_path"]
            )
            target_table_path.mkdir(parents=True, exist_ok=True)
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            saved_path.parent.mkdir(parents=True, exist_ok=True)
            if destination_path.exists():
                shutil.move(str(destination_path), str(saved_path))
            moved_specs.append((spec, destination_path, saved_path))
            shutil.move(str(source_path), str(destination_path))

            marker_path = target_table_path / "schema.parquet"
            if not marker_path.exists():
                pq.write_table(
                    pa.Table.from_batches([], schema=parquet_file_schema(
                        spec["schema"],
                        spec["partition_columns"],
                    )),
                    marker_path,
                )
                created_markers.append(marker_path)

        # 正式安装后只精确复读触达叶的物理契约、主键和行数摘要。
        for spec, destination_path, _ in moved_specs:
            target_table_path = silver_root / spec["table_name"]
            parquet_files = list(destination_path.glob("*.parquet"))
            if not parquet_files:
                raise FileNotFoundError(
                    f"正式叶缺少 Parquet 文件：{destination_path}"
                )
            expected_file_schema = parquet_file_schema(
                spec["schema"],
                spec["partition_columns"],
            )
            for parquet_path in parquet_files:
                if not physically_and_identity_compatible(
                    pq.read_schema(parquet_path),
                    expected_file_schema,
                ):
                    raise TypeError(
                        f"正式叶文件 Schema 不一致：{parquet_path}"
                    )
            formal_dataset = ds.dataset(
                destination_path,
                format="parquet",
                partitioning=spec["partitioning"],
                partition_base_dir=str(target_table_path),
            )
            formal_key_table = formal_dataset.to_table(
                columns=spec["primary_key"]
            )
            if len(formal_key_table) != len(spec["complete_table"]):
                raise ValueError("正式叶行数与 staging 不一致。")
            if formal_key_table.to_pandas().duplicated(
                spec["primary_key"]
            ).any():
                raise ValueError("正式叶主键不唯一。")
        commit_succeeded = True
    except Exception as commit_error:
        rollback_errors = []
        for marker_path in reversed(created_markers):
            try:
                marker_path.unlink(missing_ok=True)
            except Exception as rollback_error:
                rollback_errors.append(str(rollback_error))
        for spec, destination_path, saved_path in reversed(moved_specs):
            try:
                if destination_path.exists():
                    isolated_path = (
                        quarantine_root
                        / spec["table_name"]
                        / spec["relative_path"]
                    )
                    isolated_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(destination_path), str(isolated_path))
                if saved_path.exists():
                    destination_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(saved_path), str(destination_path))
            except Exception as rollback_error:
                rollback_errors.append(str(rollback_error))
        if rollback_errors:
            raise RuntimeError(
                f"协调提交失败且回滚不完整；备份位于 {backup_root}；"
                f"错误：{rollback_errors}"
            ) from commit_error
        if quarantine_root.exists() and any(quarantine_root.rglob("*")):
            raise RuntimeError(
                f"协调提交失败；旧事实与日历叶均已恢复，"
                f"新叶隔离在 {quarantine_root}。"
            ) from commit_error
        raise
    finally:
        shutil.rmtree(staging_root, ignore_errors=True)
        backup_has_data = (
            backup_root.exists()
            and next(backup_root.rglob("*.parquet"), None) is not None
        )
        if commit_succeeded or not backup_has_data:
            shutil.rmtree(backup_root, ignore_errors=True)
        if quarantine_root.exists() and not any(
            quarantine_root.rglob("*")
        ):
            shutil.rmtree(quarantine_root, ignore_errors=True)

    return sum(len(spec["complete_table"]) for spec in prepared_specs)


# ## 生成对应 c04 完整叶
# 
# 政策变化和本批完成状态在同一次定向叶读取中合并；未触达行及完成/run/actual/quality/c07 证据逐值保留。

# In[ ]:


def prepare_fact_leaf_specs(
    collected_df: pd.DataFrame,
    lake_root: pathlib.Path,
) -> list[dict[str, object]]:
    if collected_df.empty:
        return []
    fact_path = lake_root.resolve() / "silver" / TABLE_NAME
    specs = []
    for partition_values, incoming_df in collected_df.groupby(
        PARTITION_COLUMNS,
        sort=True,
    ):
        partition_key = tuple(partition_values)
        existing_df = read_partition_leaf(
            fact_path,
            FUTURES_DAILY_SCHEMA,
            PARTITION_COLUMNS,
            HIVE_PARTITIONING,
            partition_key,
        )
        incoming_keys = pd.MultiIndex.from_frame(incoming_df[PRIMARY_KEY])
        existing_keys = pd.MultiIndex.from_frame(existing_df[PRIMARY_KEY])
        retained_df = existing_df.loc[
            ~existing_keys.isin(incoming_keys),
            FUTURES_DAILY_SCHEMA.names,
        ]
        desired_df = validate_daily_frame(
            pd.concat(
                [
                    retained_df,
                    incoming_df.loc[:, FUTURES_DAILY_SCHEMA.names],
                ],
                ignore_index=True,
            ),
            f"dirty 事实叶 {partition_key} ",
        )
        specs.append({
            "table_name": TABLE_NAME,
            "schema": FUTURES_DAILY_SCHEMA,
            "partition_columns": PARTITION_COLUMNS,
            "partitioning": HIVE_PARTITIONING,
            "primary_key": PRIMARY_KEY,
            "partition_key": partition_key,
            "frame": desired_df,
        })
    return specs


# ## 本批事实完成状态定向回写
# 
# 只有 API 响应门禁和 dirty 完整事实叶业务校验通过的键可以进入本步骤。正常行情实际条数为 1、质量通过；
# 有限 OHLC 关系异常仍按实际条数 1 落盘，但质量为警告；确认空结果实际条数为 0、缺失条数为 1。
# 若事实已提交但状态未写回，下次仍按 pending 重新请求并确定性覆盖；日常不扫描完整事实修复状态。
# 

# In[ ]:


def apply_completion_to_calendar_leaf(
    calendar_df: pd.DataFrame,
    fact_df: pd.DataFrame,
    fetch_run_id: str,
    completed_at: datetime,
) -> pd.DataFrame:
    if fact_df.empty:
        return calendar_df.copy()

    desired_df = calendar_df.copy()
    fact_status_df = fact_df.loc[
        :, [*PRIMARY_KEY, "has_market_data", "open", "high", "low", "close"]
    ].copy()
    invalid_mask = invalid_ohlc_mask(fact_df)
    has_market_data = fact_df["has_market_data"].astype(bool)
    fact_status_df["actual_bar_count"] = has_market_data.astype("int32")
    fact_status_df["missing_bar_count"] = (~has_market_data).astype("int32")
    fact_status_df["quality_status"] = np.where(
        ~has_market_data | invalid_mask,
        "warning",
        "passed",
    )
    fact_status_df["quality_reason"] = np.select(
        [
            ~has_market_data,
            invalid_mask,
        ],
        [
            "JQData 请求完成但没有有效收盘价；事实以缺失占位行正式提交。",
            "JQData 日线事实已正式提交；供应商原始 OHLC 跨列关系异常，原值已保留。",
        ],
        default="JQData 日线事实已正式提交，实际条数为 1。",
    )
    fact_status_df = fact_status_df.set_index(PRIMARY_KEY)

    calendar_index = pd.MultiIndex.from_frame(desired_df[PRIMARY_KEY])
    if fact_status_df.index.has_duplicates:
        raise ValueError("完成状态输入事实主键不唯一。")
    missing_keys = fact_status_df.index.difference(calendar_index)
    if len(missing_keys):
        raise ValueError(
            f"事实行无法匹配日线日历：{missing_keys[:5].tolist()}"
        )
    matched = calendar_index.isin(fact_status_df.index)
    matched_rows = desired_df.index[matched]
    matched_index = calendar_index[matched]
    if not desired_df.loc[matched_rows, "is_fetch_required"].all():
        raise ValueError("完成状态回写命中了当前无需拉取的格点。")

    status_values = fact_status_df.loc[matched_index]
    desired_df.loc[matched_rows, "is_fetch_completed"] = True
    desired_df.loc[matched_rows, "actual_bar_count"] = status_values[
        "actual_bar_count"
    ].to_numpy()
    desired_df.loc[matched_rows, "is_data_missing"] = status_values[
        "missing_bar_count"
    ].gt(0).to_numpy()
    desired_df.loc[matched_rows, "missing_bar_count"] = status_values[
        "missing_bar_count"
    ].to_numpy()
    desired_df.loc[matched_rows, "fetch_run_id"] = fetch_run_id
    desired_df.loc[matched_rows, "fetch_completed_at"] = completed_at
    desired_df.loc[matched_rows, "missing_checked_at"] = completed_at
    desired_df.loc[matched_rows, "quality_status"] = status_values[
        "quality_status"
    ].to_numpy()
    desired_df.loc[matched_rows, "quality_reason"] = status_values[
        "quality_reason"
    ].to_numpy()
    desired_df.loc[matched_rows, "quality_checked_at"] = completed_at
    desired_df.loc[matched_rows, "updated_at"] = completed_at
    return desired_df


def prepare_calendar_leaf_spec(
    policy_df: pd.DataFrame,
    fact_df: pd.DataFrame,
    lake_root: pathlib.Path,
    partition_key: tuple[object, ...],
    fetch_run_id: str,
    completed_at: datetime,
) -> dict[str, object]:
    calendar_path = lake_root.resolve() / "silver" / CALENDAR_TABLE_NAME
    existing_df = read_partition_leaf(
        calendar_path,
        FUTURES_BAR_CALENDAR_SCHEMA,
        CALENDAR_PARTITION_COLUMNS,
        CALENDAR_PARTITIONING,
        partition_key,
    )
    if existing_df.empty:
        raise FileNotFoundError(f"c04 日历叶不存在：{partition_key}")
    desired_df = apply_policy_to_calendar_leaf(
        existing_df,
        policy_df,
        completed_at,
    )
    desired_df = apply_completion_to_calendar_leaf(
        desired_df,
        fact_df,
        fetch_run_id,
        completed_at,
    )
    validated_df = validate_calendar_state_frame(
        desired_df,
        f"dirty 日历叶 {partition_key} ",
    )
    return {
        "table_name": CALENDAR_TABLE_NAME,
        "schema": FUTURES_BAR_CALENDAR_SCHEMA,
        "partition_columns": CALENDAR_PARTITION_COLUMNS,
        "partitioning": CALENDAR_PARTITIONING,
        "primary_key": CALENDAR_PRIMARY_KEY,
        "partition_key": partition_key,
        "frame": validated_df,
    }


# ## CLI：窄列规划、未完成采集和定向协调提交
# 
# - 日常全历史只扫描 c04 的规划窄列，不读取全量事实。
# - API 只处理 required 且没有完整完成证据的格点。
# - 写入只复读 dirty 事实叶及对应 c04 叶；不做 extra 清退、状态修复或批末全根复读。

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

    run_updated_at = datetime.now(timezone.utc)
    fetch_run_id = (
        f"daily-{run_updated_at:%Y%m%dT%H%M%SZ}-"
        f"{uuid.uuid4().hex[:8]}"
    )
    silver_root = resolved_lake_root / "silver"
    calendar_path = silver_root / CALENDAR_TABLE_NAME

    # 全历史日常规划只读取决定政策和完成状态的窄列，并用 Arrow filter 裁剪频率/显式日期。
    planning_started_at = time.perf_counter()
    calendar_dataset = open_contract_dataset(
        calendar_path,
        CALENDAR_PARTITIONING,
        FUTURES_BAR_CALENDAR_SCHEMA,
        "上游行情日历",
        required=True,
    )
    calendar_filter = ds.field("bar_frequency") == "1d"
    if requested_start_date is not None:
        calendar_filter &= ds.field("trading_date") >= requested_start_date
        calendar_filter &= ds.field("trading_date") <= requested_end_date
    calendar_file_count = sum(
        1 for _ in calendar_dataset.get_fragments(filter=calendar_filter)
    )
    click.echo(
        "planning_start: "
        f"table={TABLE_NAME}; calendar_files={calendar_file_count}; "
        f"planning_columns={len(CALENDAR_PLANNING_COLUMNS)}"
    )
    planning_table = calendar_dataset.to_table(
        columns=CALENDAR_PLANNING_COLUMNS,
        filter=calendar_filter,
    )
    planned_table, dirty_mask, pending_mask, completed_mask = (
        daily_policy_plan(planning_table)
    )
    dirty_policy_df = planned_table.filter(dirty_mask).to_pandas(
        types_mapper=pd.ArrowDtype
    )
    pending_df = planned_table.filter(pending_mask).to_pandas(
        types_mapper=pd.ArrowDtype
    )
    completed_required_count = int(
        pc.sum(pc.cast(completed_mask, pa.int64())).as_py() or 0
    )
    batches = request_batches(pending_df)
    estimated_values = sum(
        int(batch["estimated_values"])
        for batch in batches
    )
    mode = "explicit" if has_explicit_dates else "automatic"
    plan_name = "explicit_plan" if has_explicit_dates else "auto_plan"
    click.echo(
        f"{plan_name}: table={TABLE_NAME}; "
        f"planned_grid_count={len(planned_table)}; "
        f"policy_dirty_count={len(dirty_policy_df)}; "
        f"completed_required_count={completed_required_count}; "
        f"pending_grid_count={len(pending_df)}; "
        f"request_batch_count={len(batches)}; "
        f"estimated_values={estimated_values}; "
        f"planning_seconds={time.perf_counter() - planning_started_at:.3f}"
    )

    collected_frames = []
    returned_value_count = 0
    completed_batch_count = 0
    completed_pending_count = 0
    quota_stop_message = None
    if batches:
        from config.jqdata_connection import authenticate_jqdata

        jqdata = authenticate_jqdata(
            settings.jqdata_id,
            settings.jqdata_secret,
        )
        for batch_number, batch in enumerate(batches, start=1):
            batch_estimated_values = int(batch["estimated_values"])
            spare = quota_spare(jqdata)
            if (
                spare is not None
                and spare - batch_estimated_values < quota_reserve
            ):
                remaining_pending_count = (
                    len(pending_df) - completed_pending_count
                )
                quota_stop_message = (
                    "quota_stop: "
                    f"batch={batch_number}/{len(batches)}; spare={spare}; "
                    f"expected={batch_estimated_values}; "
                    f"reserve={quota_reserve}; "
                    f"completed_batches={completed_batch_count}; "
                    f"remaining_pending={remaining_pending_count}"
                )
                break
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
            completed_batch_count += 1
            completed_pending_count += len(batch["pending_df"])

    remaining_pending_count = len(pending_df) - completed_pending_count

    collected_df = (
        validate_daily_frame(
            pd.concat(collected_frames, ignore_index=True),
            "本次全部 JQData 结果",
            validate_predecessor_relationships=False,
        )
        if collected_frames
        else empty_pandas(FUTURES_DAILY_SCHEMA)
    )
    invalid_ohlc_count = int(invalid_ohlc_mask(collected_df).sum())
    confirmed_empty_count = int(
        collected_df["has_market_data"].eq(False).sum()
    )
    click.echo(
        "api_result: "
        f"requests={completed_batch_count * 3}; "
        f"returned_values={returned_value_count}; "
        f"fact_rows={len(collected_df)}; "
        f"confirmed_empty={confirmed_empty_count}; "
        f"invalid_ohlc_rows={invalid_ohlc_count}"
    )

    if not write:
        if (
            quota_stop_message is None
            and dirty_policy_df.empty
            and collected_df.empty
        ):
            click.echo(f"up_to_date: table={TABLE_NAME}; mode={mode}")
        else:
            click.echo(
                "preview_complete: "
                f"policy_rows={len(dirty_policy_df)}; "
                f"fact_rows={len(collected_df)}; write=False"
            )
        if quota_stop_message is not None:
            click.echo(quota_stop_message)
        return
    if dirty_policy_df.empty and collected_df.empty:
        if quota_stop_message is None:
            click.echo(f"up_to_date: table={TABLE_NAME}; mode={mode}")
        else:
            click.echo(quota_stop_message)
        return

    dirty_calendar_keys = set(
        dirty_policy_df[["exchange_code", "year", "month"]].itertuples(
            index=False,
            name=None,
        )
    )
    dirty_calendar_keys |= set(
        collected_df[["exchange_code", "year", "month"]].itertuples(
            index=False,
            name=None,
        )
    )

    committed_fact_rows = 0
    committed_calendar_rows = 0
    committed_leaf_groups = 0
    for base_key in sorted(dirty_calendar_keys):
        exchange_code, year, month = base_key
        policy_mask = (
            dirty_policy_df["exchange_code"].eq(exchange_code)
            & dirty_policy_df["year"].eq(year)
            & dirty_policy_df["month"].eq(month)
        )
        fact_mask = (
            collected_df["exchange_code"].eq(exchange_code)
            & collected_df["year"].eq(year)
            & collected_df["month"].eq(month)
        )
        leaf_policy_df = dirty_policy_df.loc[policy_mask].reset_index(
            drop=True
        )
        leaf_fact_df = collected_df.loc[fact_mask].reset_index(drop=True)

        fact_specs = prepare_fact_leaf_specs(
            leaf_fact_df,
            resolved_lake_root,
        )
        calendar_partition_key = (
            "1d",
            exchange_code,
            int(year),
            int(month),
        )
        calendar_spec = prepare_calendar_leaf_spec(
            leaf_policy_df,
            leaf_fact_df,
            resolved_lake_root,
            calendar_partition_key,
            fetch_run_id,
            run_updated_at,
        )
        commit_validated_leaf_group(
            [*fact_specs, calendar_spec],
            resolved_lake_root,
        )
        committed_fact_rows += len(leaf_fact_df)
        committed_calendar_rows += len(calendar_spec["frame"])
        committed_leaf_groups += 1
        click.echo(
            "leaf_group_committed: "
            f"calendar_partition={calendar_partition_key}; "
            f"fact_leaves={len(fact_specs)}; "
            f"fact_rows={len(leaf_fact_df)}; "
            f"policy_rows={len(leaf_policy_df)}"
        )

    click.echo(
        f"committed: mode={mode}; "
        f"leaf_groups={committed_leaf_groups}; "
        f"fact_rows={committed_fact_rows}; "
        f"calendar_leaf_rows={committed_calendar_rows}; "
        f"remaining_current_plan={remaining_pending_count}"
    )
    if quota_stop_message is not None:
        click.echo(quota_stop_message)


if __name__ == "__main__":
    main()

