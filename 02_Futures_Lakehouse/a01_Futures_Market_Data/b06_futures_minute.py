#!/usr/bin/env python
# coding: utf-8

# # b06_futures_minute
# 
# 目标表：`fact_futures_minute`。
# 
# 默认日常路径按 b04 的 `1m` 叶分区窄列扫描当前白名单、选择状态和可信完成凭证，
# 只请求 `pending` 或从未完成的 Session。白名单扩大自动回补，缩小只停止未来请求，
# 保留既有事实、成功批次、质量与 b07 旁证。
# 
# 分钟线来自 JQData `get_price(frequency="1m")`，时间戳采用
# `(session_start_at, session_end_at]`。零行与部分缺失都形成已完成 warning；有限 OHLC
# 关系异常保留原值并记录 warning，但不覆盖开市证据。重复、越界、NaN/Inf 和负数量硬失败。

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
import time
import time
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
        sys.path.insert(0, str(candidate_root / "02_Futures_Lakehouse"))
        project_root = candidate_root
        break
else:
    raise RuntimeError("未找到项目根目录")


# 第三方库分别承担 CLI、表格转换和 Arrow/Parquet 数据集读写。
import click
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
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
from config.futures_lakehouse.futures_fact_collection_policy import FUTURES_FACT_VARIETY_PAIRS


# ## Schema 契约呈现

# In[ ]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        FUTURES_BAR_CALENDAR_SCHEMA,
        FUTURES_MINUTE_SCHEMA,
    ], lake_root=settings.futures_lake_root)


# ## 表名、主键、分区与稳定常量
# 
# 这一单元格只声明 b06 的稳定边界，不执行任何 I/O。事实分区与日历分区不同，
# 状态回写时必须分别按各自的完整叶分区提交。
# 

# In[ ]:


TABLE_NAME = FUTURES_MINUTE_SCHEMA.metadata[b"table_name"].decode("utf-8")
CALENDAR_TABLE_NAME = FUTURES_BAR_CALENDAR_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")
PRIMARY_KEY = FUTURES_MINUTE_SCHEMA.metadata[b"primary_key"].decode("utf-8").split(",")
SESSION_KEY = [  # 汇总分钟条数和回写状态所用的 Session 业务键。
    "contract_code",  # 固定月份合约代码。
    "trading_date",  # 分钟 bar 归属的期货交易日。
    "session_number",  # 同一合约日内的 Session 顺序号。
]
CALENDAR_PRIMARY_KEY = FUTURES_BAR_CALENDAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")
PARTITION_COLUMNS = FUTURES_MINUTE_SCHEMA.metadata[b"partition_columns"].decode(
    "utf-8"
).split(",")
CALENDAR_PARTITION_COLUMNS = FUTURES_BAR_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")

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

EVIDENCE_COLUMNS = [  # b06 产生新分钟状态时必须清空的旧 b07 校对证据。
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
    pa.schema([FUTURES_MINUTE_SCHEMA.field(name) for name in PARTITION_COLUMNS]),
    flavor="hive",
)
CALENDAR_PARTITIONING = ds.partitioning(
    pa.schema(
        [
            FUTURES_BAR_CALENDAR_SCHEMA.field(name)
            for name in CALENDAR_PARTITION_COLUMNS
        ]
    ),
    flavor="hive",
)

MINUTE_PLANNING_COLUMNS = [
    "bar_frequency",
    "contract_code",
    "exchange_code",
    "underlying_code",
    "trading_date",
    "session_number",
    "session_start_at",
    "session_end_at",
    "schedule_status",
    "is_fetch_required",
    "expected_bar_count",
    "is_fetch_completed",
    "year",
    "month",
]
MINUTE_PLANNING_SCHEMA = pa.schema([
    FUTURES_BAR_CALENDAR_SCHEMA.field(name)
    for name in MINUTE_PLANNING_COLUMNS
])


# ## 行情日历完整分区校验
# 
# b06 会修改 `is_fetch_required`、`selection_reason` 和分钟完成状态，但提交单位是完整日历叶分区。
# 因此提交前后必须继续维护日线/分钟结构、审计时间、缺失计数和质量状态等整表不变量。
# 

# In[ ]:


def validate_calendar_state_frame(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    calendar_table = pandas_to_arrow(
        frame.loc[:, FUTURES_BAR_CALENDAR_SCHEMA.names],
        FUTURES_BAR_CALENDAR_SCHEMA,
    )
    checked_df = arrow_to_pandas(
        calendar_table,
        FUTURES_BAR_CALENDAR_SCHEMA,
    )

    if checked_df.duplicated(CALENDAR_PRIMARY_KEY).any():
        raise ValueError(f"{context}主键不唯一。")
    if not checked_df["bar_frequency"].isin({"1d", "1m"}).all():
        raise ValueError(f"{context}bar_frequency 不在允许枚举中。")
    if not checked_df["schedule_status"].isin(SCHEDULE_STATUSES).all():
        raise ValueError(f"{context}schedule_status 不在允许枚举中。")
    if not checked_df["evidence_level"].isin(EVIDENCE_LEVELS).all():
        raise ValueError(f"{context}evidence_level 不在允许枚举中。")
    if not checked_df["quality_status"].isin(QUALITY_STATUSES).all():
        raise ValueError(f"{context}quality_status 不在允许枚举中。")

    text_fields = [
        "schedule_signal_reason",
        "evidence_source",
        "selection_reason",
        "quality_reason",
    ]
    empty_text = pd.Series(False, index=checked_df.index)
    for column in text_fields:
        text = checked_df[column].astype("string")
        empty_text |= text.isna() | text.str.strip().eq("")
    if empty_text.any():
        raise ValueError(f"{context}状态说明字段不得为空。")

    contract_exchange = checked_df["contract_code"].astype("string").str.rsplit(
        ".",
        n=1,
    ).str[-1]
    if not contract_exchange.eq(checked_df["exchange_code"]).all():
        raise ValueError(f"{context}交易所与合约代码后缀不一致。")
    trading_date = pd.to_datetime(checked_df["trading_date"])
    if (
        ~checked_df["year"].eq(trading_date.dt.year)
        | ~checked_df["month"].eq(trading_date.dt.month)
    ).any():
        raise ValueError(f"{context}year/month 与 trading_date 不一致。")
    if checked_df["expected_bar_count"].le(0).any():
        raise ValueError(f"{context}expected_bar_count 必须大于 0。")
    if (
        checked_df["actual_bar_count"].lt(0)
        | checked_df["missing_bar_count"].lt(0)
    ).any():
        raise ValueError(f"{context}实际与缺失条数不得为负。")

    daily_mask = checked_df["bar_frequency"].eq("1d")
    minute_mask = ~daily_mask
    session_columns = [
        "session_text",
        "session_start_at",
        "session_end_at",
        "is_night_session",
    ]
    if (
        ~checked_df.loc[daily_mask, "session_number"].eq(0)
    ).any() or checked_df.loc[daily_mask, session_columns].notna().any(axis=None):
        raise ValueError(f"{context}日线 Session 结构不合法。")
    if (~checked_df.loc[daily_mask, "expected_bar_count"].eq(1)).any():
        raise ValueError(f"{context}日线理论条数必须为 1。")
    if checked_df.loc[minute_mask, "session_number"].le(0).any():
        raise ValueError(f"{context}分钟 session_number 必须大于 0。")
    if (
        checked_df.loc[minute_mask, "session_start_at"]
        >= checked_df.loc[minute_mask, "session_end_at"]
    ).any():
        raise ValueError(f"{context}分钟 Session 起点必须早于终点。")

    confirmed_mask = checked_df["schedule_status"].eq("confirmed_closed")
    if (
        ~checked_df.loc[confirmed_mask, "evidence_level"].eq("authoritative")
    ).any() or checked_df.loc[confirmed_mask, "is_fetch_required"].any():
        raise ValueError(f"{context}确认休市状态缺少权威证据或仍要求拉取。")

    completed_mask = checked_df["is_fetch_completed"].eq(True)
    if (
        checked_df.loc[completed_mask, "fetch_run_id"].isna()
        | checked_df.loc[completed_mask, "fetch_run_id"]
        .astype("string")
        .str.strip()
        .eq("")
        | checked_df.loc[completed_mask, "fetch_completed_at"].isna()
    ).any():
        raise ValueError(f"{context}完成状态缺少批次或完成时间。")
    if checked_df.loc[~completed_mask, "fetch_completed_at"].notna().any():
        raise ValueError(f"{context}未完成格点不得具有完成时间。")

    checked_missing_mask = checked_df["missing_checked_at"].notna()
    if (
        checked_df.loc[~checked_missing_mask, "is_data_missing"].any()
        or checked_df.loc[~checked_missing_mask, "missing_bar_count"].ne(0).any()
    ):
        raise ValueError(f"{context}未经检查不得记录缺失。")
    expected_missing = (
        checked_df["expected_bar_count"] - checked_df["actual_bar_count"]
    ).clip(lower=0)
    expected_missing = expected_missing.where(
        checked_df["is_fetch_required"],
        0,
    )
    if (
        checked_df.loc[checked_missing_mask, "missing_bar_count"]
        .ne(expected_missing.loc[checked_missing_mask])
        .any()
        or checked_df.loc[checked_missing_mask, "is_data_missing"]
        .ne(expected_missing.loc[checked_missing_mask].gt(0))
        .any()
    ):
        raise ValueError(f"{context}缺失状态无法由当前格点复算。")

    non_pending_mask = ~checked_df["quality_status"].eq("pending")
    if checked_df.loc[non_pending_mask, "quality_checked_at"].isna().any():
        raise ValueError(f"{context}非 pending 状态缺少质检时间。")

    comparison_columns = [
        "ohlc_matches_daily",
        "volume_matches_daily",
        "money_matches_daily",
        "open_interest_matches_daily",
    ]
    comparison_not_null = checked_df[comparison_columns].notna()
    if (comparison_not_null.any(axis=1) != comparison_not_null.all(axis=1)).any():
        raise ValueError(f"{context}b07 四项比较必须同时为空或同时非空。")
    reconciled_mask = checked_df["evidence_level"].eq("reconciled")
    if (
        ~comparison_not_null.loc[reconciled_mask].all(axis=1)
        | ~checked_df.loc[reconciled_mask, comparison_columns].eq(True).all(axis=1)
    ).any():
        raise ValueError(f"{context}reconciled 必须具有四项匹配证据。")

    return checked_df.sort_values(CALENDAR_PRIMARY_KEY).reset_index(drop=True)


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


def validate_minute_frame(
    frame: pd.DataFrame,
    context: str,
    sessions_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    # 必须在 Arrow 转换前检查来源数值；否则 Pandas NaN 会被 Arrow 静默转成 null。
    # 真正的 Python None / pd.NA 仍按 Schema 的 nullable 语义处理，NaN/Inf 则明确拒绝。
    source_frame = frame.loc[:, FUTURES_MINUTE_SCHEMA.names]
    for column in PRICE_FIELDS:
        source_series = source_frame[column]
        if source_series.dtype == object:
            source_values = source_series.to_numpy(dtype=object, copy=False)
            explicit_null_mask = np.fromiter(
                (value is None or value is pd.NA for value in source_values),
                dtype=bool,
                count=len(source_values),
            )
            source_array = pa.array(
                source_values,
                mask=explicit_null_mask,
                type=pa.float64(),
                from_pandas=False,
            )
        else:
            source_array = pa.array(
                source_series,
                type=pa.float64(),
                from_pandas=False,
            )
        populated_values = source_array.drop_null().to_numpy(
            zero_copy_only=False
        )
        if (~np.isfinite(populated_values)).any():
            raise ValueError(f"{context}{column} 包含非有限数。")

    # Arrow 转换固定列顺序、类型、可空性和 metadata。
    minute_table = pandas_to_arrow(source_frame, FUTURES_MINUTE_SCHEMA)
    checked_df = arrow_to_pandas(minute_table, FUTURES_MINUTE_SCHEMA)

    if checked_df.duplicated(PRIMARY_KEY).any():
        raise ValueError(f"{context}主键不唯一。")
    if checked_df.empty:
        return checked_df

    contract_parts = checked_df["contract_code"].astype("string").str.split(
        ".",
        n=1,
        expand=True,
    )
    if not contract_parts[1].eq(checked_df["exchange_code"]).all():
        raise ValueError(f"{context}交易所与合约代码后缀不一致。")
    expected_underlying = contract_parts[0].str.extract(
        r"^([A-Za-z]+)",
        expand=False,
    ).str.upper()
    if not expected_underlying.eq(checked_df["underlying_code"]).all():
        raise ValueError(f"{context}品种代码无法由合约代码复算。")
    trading_date = pd.to_datetime(checked_df["trading_date"])
    if (
        ~checked_df["year"].eq(trading_date.dt.year)
        | ~checked_df["month"].eq(trading_date.dt.month)
    ).any():
        raise ValueError(f"{context}year/month 与 trading_date 不一致。")
    if checked_df["session_number"].le(0).any():
        raise ValueError(f"{context}session_number 必须大于 0。")
    if not checked_df["source"].isin({SOURCE_NAME, *LEGACY_SOURCE_NAMES}).all():
        raise ValueError(f"{context}source 与 JQData 分钟契约不一致。")
    if checked_df[["volume", "money", "open_interest"]].lt(0).any(axis=None):
        raise ValueError(f"{context}数量和金额不得为负。")

    if sessions_df is not None and not checked_df.empty:
        session_columns = [
            *SESSION_KEY,
            "exchange_code",
            "underlying_code",
            "session_start_at",
            "session_end_at",
            "expected_bar_count",
        ]
        session_boundaries_df = sessions_df.loc[:, session_columns].copy()
        if session_boundaries_df.duplicated(SESSION_KEY).any():
            raise ValueError(f"{context}待办 Session 主键不唯一。")
        matched_df = checked_df.merge(
            session_boundaries_df,
            on=SESSION_KEY,
            how="left",
            suffixes=("", "_session"),
            validate="many_to_one",
            indicator=True,
        )
        if matched_df["_merge"].ne("both").any():
            raise ValueError(f"{context}分钟行不属于待办 Session。")
        if (
            ~matched_df["exchange_code"].eq(matched_df["exchange_code_session"])
            | ~matched_df["underlying_code"].eq(
                matched_df["underlying_code_session"]
            )
        ).any():
            raise ValueError(f"{context}分钟行未继承 Session 业务属性。")
        if (
            ~matched_df["bar_at"].gt(matched_df["session_start_at"])
            | ~matched_df["bar_at"].le(matched_df["session_end_at"])
        ).any():
            raise ValueError(f"{context}bar_at 越出 (Session 开始,结束]。")
        elapsed_seconds = (
            matched_df["bar_at"] - matched_df["session_start_at"]
        ).dt.total_seconds().to_numpy(dtype="float64")
        minute_number = elapsed_seconds / 60
        expected_bar_count = matched_df["expected_bar_count"].to_numpy(
            dtype="int64"
        )
        if (
            (np.remainder(elapsed_seconds, 60) != 0)
            | (minute_number > expected_bar_count)
        ).any():
            raise ValueError(
                f"{context}bar_at 不是 Session 内的理论分钟格点。"
            )
        actual_counts = matched_df.groupby(
            SESSION_KEY, dropna=False
        ).size()
        expected_counts = session_boundaries_df.set_index(
            SESSION_KEY
        )["expected_bar_count"]
        if actual_counts.gt(expected_counts.reindex(actual_counts.index)).any():
            raise ValueError(
                f"{context}Session 实际分钟条数超过理论条数。"
            )

    return checked_df.sort_values(PRIMARY_KEY).reset_index(drop=True)


# In[ ]:


def rows_for_sessions(
    frame: pd.DataFrame,
    sessions_df: pd.DataFrame,
) -> pd.DataFrame:
    # Session 为空时返回保留事实 Schema 的空表。
    if sessions_df.empty or frame.empty:
        return frame.iloc[0:0].copy()

    session_boundaries_df = sessions_df.loc[
        :,
        [*SESSION_KEY, "session_start_at", "session_end_at"],
    ]
    selected_df = frame.merge(
        session_boundaries_df,
        on=SESSION_KEY,
        how="inner",
        validate="many_to_one",
    )
    selected_df = selected_df.loc[
        selected_df["bar_at"].gt(selected_df["session_start_at"])
        & selected_df["bar_at"].le(selected_df["session_end_at"]),
        FUTURES_MINUTE_SCHEMA.names,
    ].reset_index(drop=True)
    if selected_df.duplicated(PRIMARY_KEY).any():
        raise ValueError("Session 选取结果主键不唯一。")
    return selected_df.sort_values(PRIMARY_KEY).reset_index(drop=True)


# ## 契约化数据集读取与分区发现
# 
# 上游日历与分钟事实都严格检查物理字段、类型、nullable 及表名、主键、分区身份 metadata。
# 描述性 metadata 以当前权威契约为准，但文本差异不触发历史重写，也不改变既有完成凭证。
# 

# In[ ]:


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


def schema_identity_compatible(
    actual_schema: pa.Schema,
    expected_schema: pa.Schema,
) -> bool:
    actual_metadata = actual_schema.metadata or {}
    expected_metadata = expected_schema.metadata or {}
    identity_keys = (b"table_name", b"primary_key", b"partition_columns")
    return all(
        expected_metadata.get(key) is not None
        and actual_metadata.get(key) == expected_metadata[key]
        for key in identity_keys
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
    # 目录存在但没有 Parquet 文件，与下游表尚未创建具有相同含义。
    first_parquet_file = (
        next(table_path.rglob("*.parquet"), None)
        if table_path.is_dir()
        else None
    )
    if first_parquet_file is None:
        if required:
            raise FileNotFoundError(f"{label}不存在：{table_path}")
        return None

    dataset = ds.dataset(
        table_path,
        format="parquet",
        partitioning=partitioning,
    )
    actual_schema = reconstructed_schema(dataset, schema)

    if not physically_compatible(actual_schema, schema):
        raise TypeError(f"{label}物理字段、类型或 nullable 与契约不一致。")
    if not schema_identity_compatible(actual_schema, schema):
        raise TypeError(f"{label}表名、主键或分区 metadata 与契约不一致。")

    expected_file_schema = parquet_file_schema(
        schema,
        partitioning.schema.names,
    )
    for fragment in dataset.get_fragments():
        fragment_schema = fragment.physical_schema
        if not physically_compatible(fragment_schema, expected_file_schema):
            raise TypeError(
                f"{label} Parquet fragment 物理字段、类型或 nullable 不兼容："
                f"{fragment.path}"
            )
        if not schema_identity_compatible(fragment_schema, expected_file_schema):
            raise TypeError(
                f"{label} Parquet fragment 表名、主键或分区 metadata 不兼容："
                f"{fragment.path}"
            )

    # 描述性 metadata 只影响当前代码中的权威说明，不触发历史 Parquet 重写。
    return dataset


# In[ ]:


def cast_partition_value(column: str, raw_value: str) -> object:
    # year/month 在 Arrow 契约中是整数，其余分区字段保留字符串。
    if column in {"year", "month"}:
        return int(raw_value)
    return raw_value


def partition_keys_from_dataset(
    dataset: ds.Dataset,
    table_path: pathlib.Path,
    partition_columns: list[str],
) -> set[tuple[object, ...]]:
    keys = set()
    resolved_table_path = table_path.resolve()
    for file_name in dataset.files:
        parquet_path = pathlib.Path(file_name).resolve()
        if parquet_path.name == "schema.parquet":
            continue
        relative_parts = parquet_path.relative_to(resolved_table_path).parts
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


def exact_partition_path(
    table_path: pathlib.Path,
    partition_columns: list[str],
    partition_key: tuple[object, ...],
) -> pathlib.Path:
    return table_path.joinpath(
        *[
            f"{column}={value}"
            for column, value in zip(
                partition_columns,
                partition_key,
                strict=True,
            )
        ]
    )


def read_complete_partition(
    table_path: pathlib.Path,
    schema: pa.Schema,
    partition_columns: list[str],
    partition_key: tuple[object, ...],
) -> pd.DataFrame:
    partition_path = exact_partition_path(
        table_path,
        partition_columns,
        partition_key,
    )
    parquet_files = list(partition_path.rglob("*.parquet"))
    if not parquet_files:
        return empty_pandas(schema)

    expected_file_schema = parquet_file_schema(schema, partition_columns)
    for parquet_path in parquet_files:
        actual_file_schema = pq.read_schema(parquet_path)
        if not physically_compatible(actual_file_schema, expected_file_schema):
            raise TypeError(f"叶分区物理 Schema 不兼容：{parquet_path}")
        if not schema_identity_compatible(actual_file_schema, expected_file_schema):
            raise TypeError(f"叶分区表身份 metadata 不兼容：{parquet_path}")

    file_table = ds.dataset(partition_path, format="parquet").to_table(
        columns=expected_file_schema.names,
    )
    partition_value_by_column = dict(
        zip(partition_columns, partition_key, strict=True)
    )
    logical_arrays = []
    for field in schema:
        if field.name in partition_value_by_column:
            logical_arrays.append(
                pa.chunked_array(
                    [
                        pa.array(
                            [partition_value_by_column[field.name]] * len(file_table),
                            type=field.type,
                        )
                    ],
                    type=field.type,
                )
            )
        else:
            logical_arrays.append(file_table[field.name].cast(field.type, safe=True))
    logical_table = pa.Table.from_arrays(logical_arrays, schema=schema)
    return arrow_to_pandas(logical_table, schema)


# ## 白名单窄列规划与选择状态
# 日常按完整政策扫描窄列，但只在白名单纳入/排除发生变化时定向读取并替换对应完整 b04 叶。
# 排除不会清除成功完成证据，再次纳入时已完成格点不会重复请求。

# In[ ]:


def minute_policy_plan(
    planning_table: pa.Table,
    exchange_code: str,
) -> tuple[pa.Array, pa.Array, pa.Array, pa.Array]:
    """用 Arrow compute 形成 required、dirty、pending、completed 掩码。"""
    planned_table = planning_table.cast(
        MINUTE_PLANNING_SCHEMA, safe=True
    )
    selected_underlyings = sorted(
        underlying_code
        for policy_exchange, underlying_code in FUTURES_FACT_VARIETY_PAIRS
        if policy_exchange == exchange_code
    )
    if selected_underlyings:
        desired_required = pc.is_in(
            planned_table["underlying_code"],
            value_set=pa.array(
                selected_underlyings,
                type=planned_table["underlying_code"].type,
            ),
        )
    else:
        desired_required = pa.array(
            np.zeros(len(planned_table), dtype=bool)
        )
    desired_required = pc.and_(
        desired_required,
        pc.invert(
            pc.equal(
                planned_table["schedule_status"], "confirmed_closed"
            )
        ),
    )
    completed_mask = pc.and_(
        desired_required, planned_table["is_fetch_completed"]
    )
    pending_mask = pc.and_(
        desired_required,
        pc.invert(planned_table["is_fetch_completed"]),
    )
    policy_changed_mask = pc.not_equal(
        planned_table["is_fetch_required"], desired_required
    )
    return (
        desired_required,
        policy_changed_mask,
        pending_mask,
        completed_mask,
    )


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

    batches = request_batches(sessions_df)
    for batch_number, batch_df in enumerate(batches, start=1):
        contract_code = str(batch_df["contract_code"].iloc[0])
        request_start = pd.Timestamp(batch_df["session_start_at"].min())
        request_end = pd.Timestamp(batch_df["session_end_at"].max())

        click.echo(
            "request_batch: "
            f"batch={batch_number}/{len(batches)}; "
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

        if normalized_df.empty:
            continue
        session_intervals = pd.IntervalIndex.from_arrays(
            pd.DatetimeIndex(pd.to_datetime(batch_df["session_start_at"])),
            pd.DatetimeIndex(pd.to_datetime(batch_df["session_end_at"])),
            closed="right",
        )
        # Pandas 3.0 要求 IntervalIndex 与目标时间戳的单位完全一致。
        # 严格转换到 Session 单位，禁止把无法无损表示的亚微秒值静默截断。
        bar_at_index = pd.DatetimeIndex(
            pd.to_datetime(normalized_df["bar_at"])
        ).as_unit(
            session_intervals.dtype.subtype.unit,
            round_ok=False,
        )
        matched_session_positions = session_intervals.get_indexer(
            bar_at_index
        )
        if (matched_session_positions < 0).any():
            unexpected_rows = normalized_df.loc[
                matched_session_positions < 0,
                ["contract_code", "bar_at"],
            ]
            raise ValueError(
                "schema_error: get_price 返回越出请求 Session 边界的分钟："
                f"{unexpected_rows.head(5).to_dict(orient='records')}"
            )

        matched_sessions_df = batch_df.iloc[
            matched_session_positions
        ].reset_index(drop=True)
        selected_df = normalized_df.reset_index(drop=True).copy()
        for column in [
            "exchange_code",
            "underlying_code",
            "trading_date",
            "session_number",
            "year",
            "month",
        ]:
            selected_df[column] = matched_sessions_df[column].array
        selected_df["source"] = SOURCE_NAME
        selected_df["updated_at"] = updated_at

        invalid_ohlc = invalid_ohlc_mask(selected_df)
        if invalid_ohlc.any():
            invalid_df = selected_df.loc[invalid_ohlc]
            grouped_invalid_counts = invalid_df.groupby(
                SESSION_KEY,
                dropna=False,
            ).size()
            for session_key, invalid_count in grouped_invalid_counts.items():
                invalid_session_counts[tuple(session_key)] = (
                    invalid_session_counts.get(tuple(session_key), 0)
                    + int(invalid_count)
                )
            for invalid_row in invalid_df.itertuples(index=False):
                click.echo(
                    "preserved_invalid_ohlc_bar: "
                    f"contract={invalid_row.contract_code}; "
                    f"bar_at={invalid_row.bar_at}; "
                    f"ohlc={(invalid_row.open, invalid_row.high, invalid_row.low, invalid_row.close)}"
                )
        collected_frames.append(
            selected_df.loc[:, FUTURES_MINUTE_SCHEMA.names]
        )

    collected_df = (
        pd.concat(collected_frames, ignore_index=True)
        if collected_frames
        else empty_pandas(FUTURES_MINUTE_SCHEMA)
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


# ## 完整叶原子提交
# 
# dirty 完整叶执行一次业务 validator；staging 与安装后的正式叶只复读物理 Schema、
# 主键和行数摘要，并保留同进程 backup、隔离和失败回滚。

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
    """校验一次 dirty 叶，并以轻量 staging/正式摘要完成可回滚替换。"""
    complete_df = validate_frame(frame, "待提交完整分区")
    if not complete_df.empty:
        actual_partition_keys = set(
            complete_df[partition_columns].itertuples(index=False, name=None)
        )
        if actual_partition_keys != {partition_key}:
            raise ValueError("待提交数据越出指定 Hive 分区。")
    complete_table = pandas_to_arrow(complete_df, schema)

    silver_root = lake_root.resolve() / "silver"
    target_path = silver_root / table_name
    run_id = uuid.uuid4().hex[:12]
    staging_path = silver_root / f".s-{run_id}"
    backup_path = silver_root / f".b-{run_id}"
    quarantine_path = silver_root / f".q-{run_id}"
    silver_root.mkdir(parents=True, exist_ok=True)
    for managed_path in (target_path, staging_path, backup_path, quarantine_path):
        if not managed_path.resolve().is_relative_to(silver_root):
            raise ValueError(f"数据集路径越出 silver 根目录：{managed_path}")

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
    expected_file_schema = parquet_file_schema(schema, partition_columns)
    if table_name == TABLE_NAME:
        primary_key = PRIMARY_KEY
    elif table_name == CALENDAR_TABLE_NAME:
        primary_key = CALENDAR_PRIMARY_KEY
    else:
        raise ValueError(f"b06 不支持提交未知表：{table_name}")
    non_partition_primary_key = [
        name for name in primary_key if name not in partition_columns
    ]
    staging_path.mkdir(parents=True, exist_ok=False)

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
            empty_partition_path = staging_path / relative_path
            empty_partition_path.mkdir(parents=True, exist_ok=False)
            pq.write_table(
                pa.Table.from_batches([], schema=expected_file_schema),
                empty_partition_path / "part-0.parquet",
            )

        staged_leaf_path = staging_path / relative_path
        staged_files = list(staged_leaf_path.rglob("*.parquet"))
        if not staged_files:
            raise FileNotFoundError(f"staging 缺少 {relative_path}。")
        staged_row_count = 0
        staged_key_frames = []
        for parquet_path in staged_files:
            staged_schema = pq.read_schema(parquet_path)
            if not physically_compatible(staged_schema, expected_file_schema):
                raise TypeError("staging 物理 Schema 与契约不兼容。")
            if not schema_identity_compatible(staged_schema, expected_file_schema):
                raise TypeError("staging 表身份 metadata 与契约不兼容。")
            metadata = pq.read_metadata(parquet_path)
            staged_row_count += metadata.num_rows
            if non_partition_primary_key and metadata.num_rows:
                staged_key_frames.append(
                    pq.read_table(
                        parquet_path,
                        columns=non_partition_primary_key,
                    ).to_pandas(types_mapper=pd.ArrowDtype)
                )
        if staged_row_count != len(complete_table):
            raise ValueError("staging 行数摘要与待提交分区不一致。")
        if staged_key_frames:
            staged_keys_df = pd.concat(staged_key_frames, ignore_index=True)
            if staged_keys_df.duplicated(non_partition_primary_key).any():
                raise ValueError("staging 主键不唯一。")
    except Exception:
        shutil.rmtree(staging_path, ignore_errors=True)
        raise

    source_path = staging_path / relative_path
    destination_path = target_path / relative_path
    saved_path = backup_path / relative_path
    marker_path = target_path / "schema.parquet"
    marker_created = False
    commit_succeeded = False
    backup_path.mkdir(parents=True, exist_ok=False)
    target_path.mkdir(parents=True, exist_ok=True)
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    saved_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        if destination_path.exists():
            shutil.move(str(destination_path), str(saved_path))
        shutil.move(str(source_path), str(destination_path))

        if marker_path.exists():
            marker_metadata = pq.read_metadata(marker_path)
            marker_schema = pq.read_schema(marker_path)
            if marker_metadata.num_rows:
                raise ValueError("schema.parquet 必须是 0 行契约标记。")
            if not physically_compatible(marker_schema, expected_file_schema):
                raise TypeError("schema.parquet 物理 Schema 与契约不兼容。")
            if not schema_identity_compatible(marker_schema, expected_file_schema):
                raise TypeError("schema.parquet 表身份 metadata 与契约不兼容。")
        else:
            pq.write_table(
                pa.Table.from_batches([], schema=expected_file_schema),
                marker_path,
            )
            marker_created = True

        formal_files = list(destination_path.rglob("*.parquet"))
        if not formal_files:
            raise FileNotFoundError(f"正式叶缺少 Parquet 文件：{destination_path}")
        formal_row_count = 0
        formal_key_frames = []
        for parquet_path in formal_files:
            formal_schema = pq.read_schema(parquet_path)
            if not physically_compatible(formal_schema, expected_file_schema):
                raise TypeError("正式叶物理 Schema 与契约不兼容。")
            if not schema_identity_compatible(formal_schema, expected_file_schema):
                raise TypeError("正式叶表身份 metadata 与契约不兼容。")
            formal_metadata = pq.read_metadata(parquet_path)
            formal_row_count += formal_metadata.num_rows
            if non_partition_primary_key and formal_metadata.num_rows:
                formal_key_frames.append(
                    pq.read_table(
                        parquet_path,
                        columns=non_partition_primary_key,
                    ).to_pandas(types_mapper=pd.ArrowDtype)
                )
        if formal_row_count != len(complete_table):
            raise ValueError("正式叶行数摘要与 staging 不一致。")
        if formal_key_frames:
            formal_keys_df = pd.concat(formal_key_frames, ignore_index=True)
            if formal_keys_df.duplicated(non_partition_primary_key).any():
                raise ValueError("正式叶主键不唯一。")
        commit_succeeded = True
    except Exception as commit_error:
        rollback_errors = []
        if marker_created and marker_path.exists():
            try:
                marker_path.unlink()
            except Exception as marker_rollback_error:
                rollback_errors.append(
                    f"{type(marker_rollback_error).__name__}: "
                    f"{marker_rollback_error}"
                )
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
        shutil.rmtree(staging_path, ignore_errors=True)
        backup_has_data = backup_path.exists() and any(
            backup_path.rglob("*.parquet")
        )
        if commit_succeeded or not backup_has_data:
            shutil.rmtree(backup_path, ignore_errors=True)
        if quarantine_path.exists() and not any(quarantine_path.rglob("*")):
            shutil.rmtree(quarantine_path, ignore_errors=True)

    return complete_df


# ## 精确叶读取与事实合并
# 
# 提交前只读取当前 dirty 事实叶，替换本批 Session 后保留同叶其他历史事实；
# 不读取完整事实根，也不自动清退 extra/orphan。

# In[ ]:


def commit_fact_partition(
    collected_df: pd.DataFrame,
    existing_df: pd.DataFrame,
    pending_sessions_df: pd.DataFrame,
    partition_key: tuple[object, ...],
    lake_root: pathlib.Path,
) -> pd.DataFrame:
    # 只删除真正待办 Session 的旧分钟；未触达和白名单外历史数据继续保留。
    stale_df = rows_for_sessions(existing_df, pending_sessions_df)
    stale_index = pd.MultiIndex.from_frame(stale_df[PRIMARY_KEY])
    existing_index = pd.MultiIndex.from_frame(existing_df[PRIMARY_KEY])
    retained_df = existing_df.loc[
        ~existing_index.isin(stale_index),
        FUTURES_MINUTE_SCHEMA.names,
    ]

    desired_df = pd.concat(
        [
            retained_df,
            collected_df.loc[:, FUTURES_MINUTE_SCHEMA.names],
        ],
        ignore_index=True,
    )
    committed_df = commit_complete_partition(
        desired_df,
        lake_root,
        TABLE_NAME,
        FUTURES_MINUTE_SCHEMA,
        PARTITION_COLUMNS,
        HIVE_PARTITIONING,
        partition_key,
        validate_minute_frame,
    )

    # 正式安装只复读物理 Schema、主键与行数摘要；业务内容已经在 dirty 叶校验一次。
    return collected_df.sort_values(PRIMARY_KEY).reset_index(drop=True)


# ## 定向政策提交与完成状态回写
# 
# 每个 dirty b04 叶都从正式路径重新精确读取，向量更新本批键，并保留未触达状态与证据。

# In[ ]:


def build_calendar_completion_updates(
    sessions_df: pd.DataFrame,
    committed_requested_df: pd.DataFrame,
    invalid_session_counts: dict[tuple[object, ...], int],
) -> dict[tuple[object, ...], pd.DataFrame]:
    """把成功事实分区压成按 b04 叶组织的 Session 摘要。"""
    if sessions_df.empty:
        return {}

    fact_counts = (
        committed_requested_df.groupby(SESSION_KEY, dropna=False).size()
        if not committed_requested_df.empty
        else pd.Series(dtype="int64")
    )
    invalid_df = committed_requested_df.loc[
        invalid_ohlc_mask(committed_requested_df)
    ]
    invalid_counts = (
        invalid_df.groupby(SESSION_KEY, dropna=False).size()
        if not invalid_df.empty
        else pd.Series(dtype="int64")
    )
    reported_invalid_counts = {
        key: int(value)
        for key, value in invalid_session_counts.items()
        if int(value) > 0
    }
    if invalid_counts.to_dict() != reported_invalid_counts:
        raise ValueError(
            "已提交事实中的 OHLC 异常计数与本次响应审计不一致。"
        )

    session_summary_df = sessions_df.loc[
        :, SESSION_KEY
    ].drop_duplicates().copy()
    session_index = pd.MultiIndex.from_frame(
        session_summary_df[SESSION_KEY]
    )
    session_summary_df["_actual_count"] = fact_counts.reindex(
        session_index, fill_value=0
    ).to_numpy(dtype="int64")
    session_summary_df["_invalid_count"] = invalid_counts.reindex(
        session_index, fill_value=0
    ).to_numpy(dtype="int64")

    updates_by_partition = {}
    for partition_values, partition_sessions_df in sessions_df.groupby(
        ["exchange_code", "year", "month"], sort=True
    ):
        exchange_code, year, month = partition_values
        partition_key = (
            "1m", exchange_code, int(year), int(month)
        )
        partition_keys_df = partition_sessions_df.loc[
            :, SESSION_KEY
        ].drop_duplicates()
        updates_by_partition[partition_key] = partition_keys_df.merge(
            session_summary_df,
            on=SESSION_KEY,
            how="left",
            validate="one_to_one",
        )
    return updates_by_partition


def commit_calendar_updates(
    policy_updates_by_partition: dict[tuple[object, ...], pd.DataFrame],
    completion_updates_by_partition: dict[
        tuple[object, ...], pd.DataFrame
    ],
    lake_root: pathlib.Path,
    fetch_run_id: str,
    completed_at: datetime,
) -> tuple[int, int, int]:
    """每个 dirty b04 叶一次读取、一次完整校验和一次提交。"""
    calendar_path = lake_root.resolve() / "silver" / CALENDAR_TABLE_NAME
    policy_row_count = 0
    completion_row_count = 0
    committed_partition_count = 0
    partition_keys = sorted(
        set(policy_updates_by_partition)
        | set(completion_updates_by_partition)
    )
    for partition_key in partition_keys:
        calendar_df = read_complete_partition(
            calendar_path,
            FUTURES_BAR_CALENDAR_SCHEMA,
            CALENDAR_PARTITION_COLUMNS,
            partition_key,
        )
        if calendar_df.empty:
            raise FileNotFoundError(f"日历回写目标叶不存在：{partition_key}")
        desired_df = calendar_df

        policy_updates_df = policy_updates_by_partition.get(partition_key)
        if policy_updates_df is not None and not policy_updates_df.empty:
            desired_df = desired_df.merge(
                policy_updates_df,
                on=CALENDAR_PRIMARY_KEY,
                how="left",
                validate="one_to_one",
            )
            update_mask = desired_df["_desired_required"].notna()
            if int(update_mask.sum()) != len(policy_updates_df):
                raise ValueError(
                    f"政策回写主键无法完整匹配：{partition_key}"
                )
            desired_required = desired_df["_desired_required"].fillna(
                desired_df["is_fetch_required"]
            ).astype(bool)
            expansion_mask = update_mask & desired_required
            contraction_mask = update_mask & ~desired_required
            desired_df.loc[update_mask, "is_fetch_required"] = (
                desired_required.loc[update_mask]
            )
            desired_df.loc[expansion_mask, "selection_reason"] = (
                SELECTED_REASON
            )
            desired_df.loc[contraction_mask, "selection_reason"] = (
                EXCLUDED_REASON
            )
            desired_df.loc[contraction_mask, "is_data_missing"] = False
            desired_df.loc[contraction_mask, "missing_bar_count"] = 0
            incomplete_expansion_mask = (
                expansion_mask
                & ~desired_df["is_fetch_completed"].eq(True)
            )
            completed_expansion_mask = (
                expansion_mask & ~incomplete_expansion_mask
            )
            completed_expansion_missing = (
                desired_df["expected_bar_count"]
                - desired_df["actual_bar_count"]
            ).clip(lower=0)
            desired_df.loc[
                completed_expansion_mask, "missing_bar_count"
            ] = completed_expansion_missing.loc[completed_expansion_mask]
            desired_df.loc[
                completed_expansion_mask, "is_data_missing"
            ] = completed_expansion_missing.loc[
                completed_expansion_mask
            ].gt(0)
            desired_df.loc[
                incomplete_expansion_mask, "missing_checked_at"
            ] = None
            desired_df.loc[
                incomplete_expansion_mask, "quality_status"
            ] = "pending"
            desired_df.loc[
                incomplete_expansion_mask, "quality_reason"
            ] = "分钟采集政策已选择该格点，等待事实提交与复读。"
            desired_df.loc[
                incomplete_expansion_mask, "quality_checked_at"
            ] = None
            desired_df.loc[update_mask, "updated_at"] = completed_at
            desired_df = desired_df.drop(columns=["_desired_required"])
            policy_row_count += len(policy_updates_df)

        completion_updates_df = completion_updates_by_partition.get(
            partition_key
        )
        if (
            completion_updates_df is not None
            and not completion_updates_df.empty
        ):
            if completion_updates_df.duplicated(SESSION_KEY).any():
                raise ValueError(
                    f"完成摘要 Session 主键不唯一：{partition_key}"
                )
            desired_df = desired_df.merge(
                completion_updates_df,
                on=SESSION_KEY,
                how="left",
                validate="one_to_one",
            )
            requested_mask = desired_df["_actual_count"].notna()
            if int(requested_mask.sum()) != len(completion_updates_df):
                raise ValueError(
                    f"待回写 Session 无法完整匹配正式日历：{partition_key}"
                )
            if (
                ~desired_df.loc[requested_mask, "bar_frequency"].eq("1m")
                | ~desired_df.loc[requested_mask, "is_fetch_required"].eq(True)
            ).any():
                raise ValueError("状态回写命中了非待采集分钟 Session。")

            actual_count = desired_df["_actual_count"].fillna(0).astype("int64")
            invalid_count = desired_df["_invalid_count"].fillna(0).astype("int64")
            expected_count = desired_df["expected_bar_count"].astype("int64")
            if (
                actual_count.loc[requested_mask].gt(
                    expected_count.loc[requested_mask]
                ).any()
                or invalid_count.loc[requested_mask].gt(
                    actual_count.loc[requested_mask]
                ).any()
            ):
                raise ValueError(
                    "分钟实际条数或 OHLC 异常条数越出理论边界。"
                )
            missing_count = expected_count - actual_count
            warning_mask = requested_mask & (
                missing_count.gt(0) | invalid_count.gt(0)
            )
            passed_mask = requested_mask & ~warning_mask
            zero_mask = requested_mask & actual_count.eq(0)
            recovered_mask = (
                requested_mask
                & actual_count.gt(0)
                & desired_df["schedule_status"].eq("suspected_closed")
            )
            desired_df.loc[zero_mask, "schedule_status"] = "suspected_closed"
            desired_df.loc[zero_mask, "schedule_signal_reason"] = (
                "JQData 分钟请求成功且正式安装为 0 条，Session 疑似休市，"
                "等待 b07 定向校对。"
            )
            desired_df.loc[zero_mask, "evidence_level"] = "inferred"
            desired_df.loc[zero_mask, "evidence_source"] = (
                "fact_futures_minute:formal_empty_session"
            )
            desired_df.loc[recovered_mask, "schedule_status"] = "scheduled"
            desired_df.loc[recovered_mask, "schedule_signal_reason"] = (
                "分钟事实正式安装非空，取消由空响应形成的疑似休市信号。"
            )
            desired_df.loc[recovered_mask, "evidence_level"] = "inferred"
            desired_df.loc[recovered_mask, "evidence_source"] = (
                "fact_futures_minute:formal_nonempty_session"
            )
            for evidence_column in EVIDENCE_COLUMNS:
                desired_df.loc[requested_mask, evidence_column] = None
            desired_df.loc[requested_mask, "is_fetch_completed"] = True
            desired_df.loc[requested_mask, "actual_bar_count"] = (
                actual_count.loc[requested_mask]
            )
            desired_df.loc[requested_mask, "is_data_missing"] = (
                missing_count.loc[requested_mask].gt(0)
            )
            desired_df.loc[requested_mask, "missing_bar_count"] = (
                missing_count.loc[requested_mask]
            )
            desired_df.loc[requested_mask, "fetch_run_id"] = fetch_run_id
            desired_df.loc[requested_mask, "fetch_completed_at"] = completed_at
            desired_df.loc[requested_mask, "missing_checked_at"] = completed_at
            desired_df.loc[passed_mask, "quality_status"] = "passed"
            desired_df.loc[warning_mask, "quality_status"] = "warning"
            quality_reason = (
                "JQData 分钟请求已完成并正式安装；理论 "
                + expected_count.astype("string")
                + " 条，实际 "
                + actual_count.astype("string")
                + " 条，缺失 "
                + missing_count.astype("string")
                + " 条。"
            )
            invalid_reason = (
                "其中 "
                + invalid_count.astype("string")
                + " 条供应商原始 OHLC 跨列关系异常，原值已保留。"
            )
            quality_reason = quality_reason + invalid_reason.where(
                invalid_count.gt(0), ""
            )
            desired_df.loc[requested_mask, "quality_reason"] = (
                quality_reason.loc[requested_mask]
            )
            desired_df.loc[requested_mask, "quality_checked_at"] = completed_at
            desired_df.loc[requested_mask, "updated_at"] = completed_at
            desired_df = desired_df.drop(
                columns=["_actual_count", "_invalid_count"]
            )
            completion_row_count += len(completion_updates_df)

        complete_df = desired_df.loc[
            :, FUTURES_BAR_CALENDAR_SCHEMA.names
        ]
        commit_complete_partition(
            complete_df,
            lake_root,
            CALENDAR_TABLE_NAME,
            FUTURES_BAR_CALENDAR_SCHEMA,
            CALENDAR_PARTITION_COLUMNS,
            CALENDAR_PARTITIONING,
            partition_key,
            validate_calendar_state_frame,
        )
        committed_partition_count += 1
    return policy_row_count, completion_row_count, committed_partition_count


# ## CLI：日常窄列规划、配额与定向提交
# 
# - 不传日期：按当前白名单扫描窄列，以 `is_fetch_completed` 直接求 pending。
# - 显式日期：只用于检查或非正式测试湖写入。
# - 无 pending 时不认证、不调用 API。
# - 配额不足时停止当前批次，不等待、不自动恢复。

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

    silver_root = resolved_lake_root / "silver"
    calendar_path = silver_root / CALENDAR_TABLE_NAME
    target_path = silver_root / TABLE_NAME
    planning_started_at = time.perf_counter()

    # 根 Dataset 各打开一次，只做物理契约门禁；日常规划不读取任何事实行。
    calendar_dataset = open_contract_dataset(
        calendar_path,
        CALENDAR_PARTITIONING,
        FUTURES_BAR_CALENDAR_SCHEMA,
        "上游行情日历",
        required=True,
    )
    open_contract_dataset(
        target_path,
        HIVE_PARTITIONING,
        FUTURES_MINUTE_SCHEMA,
        "现有分钟事实",
        required=False,
    )
    calendar_partition_keys = sorted(
        partition_key
        for partition_key in partition_keys_from_dataset(
            calendar_dataset,
            calendar_path,
            CALENDAR_PARTITION_COLUMNS,
        )
        if partition_key[0] == "1m"
    )
    run_updated_at = datetime.now(timezone.utc)
    fetch_run_id = (
        f"minute-{run_updated_at:%Y%m%dT%H%M%SZ}-"
        f"{uuid.uuid4().hex[:8]}"
    )

    policy_updates_by_partition: dict[
        tuple[object, ...],
        pd.DataFrame,
    ] = {}
    pending_frames_by_fact_partition: dict[
        tuple[object, ...],
        list[pd.DataFrame],
    ] = {}
    scanned_rows = 0
    selected_count = 0
    complete_count = 0
    pending_count = 0
    expected_pending_rows = 0
    click.echo(
        "planning_start: "
        f"table={TABLE_NAME}; calendar_partitions={len(calendar_partition_keys)}; "
        f"calendar_files={len(calendar_dataset.files)}"
    )

    for partition_number, calendar_partition_key in enumerate(
        calendar_partition_keys,
        start=1,
    ):
        calendar_filter = partition_expression(
            CALENDAR_PARTITION_COLUMNS, calendar_partition_key
        )
        if requested_start_date is not None:
            calendar_filter &= (
                ds.field("trading_date") >= requested_start_date
            ) & (ds.field("trading_date") <= requested_end_date)
        planning_table = calendar_dataset.to_table(
            columns=MINUTE_PLANNING_COLUMNS,
            filter=calendar_filter,
        ).cast(MINUTE_PLANNING_SCHEMA, safe=True)
        scanned_rows += len(planning_table)
        exchange_code = str(calendar_partition_key[1])
        (
            desired_required,
            policy_changed_mask,
            pending_mask,
            completed_mask,
        ) = minute_policy_plan(
            planning_table, exchange_code
        )

        selected_count += int(
            pc.sum(pc.cast(desired_required, pa.int64())).as_py() or 0
        )
        complete_count += int(
            pc.sum(pc.cast(completed_mask, pa.int64())).as_py() or 0
        )
        pending_count += int(
            pc.sum(pc.cast(pending_mask, pa.int64())).as_py() or 0
        )
        if bool(pc.any(policy_changed_mask).as_py()):
            policy_updates_df = planning_table.select(
                CALENDAR_PRIMARY_KEY
            ).filter(policy_changed_mask).to_pandas(
                types_mapper=pd.ArrowDtype
            )
            policy_updates_df["_desired_required"] = pc.filter(
                desired_required, policy_changed_mask
            ).to_numpy(zero_copy_only=False)
            policy_updates_by_partition[calendar_partition_key] = (
                policy_updates_df.reset_index(drop=True)
            )

        pending_table = planning_table.filter(pending_mask)
        pending_sessions_df = pending_table.to_pandas(
            types_mapper=pd.ArrowDtype
        )
        expected_pending_rows += int(
            pc.sum(pending_table["expected_bar_count"]).as_py() or 0
        )
        for partition_values, partition_sessions_df in pending_sessions_df.groupby(
            PARTITION_COLUMNS,
            sort=True,
        ):
            fact_partition_key = tuple(partition_values)
            pending_frames_by_fact_partition.setdefault(
                fact_partition_key,
                [],
            ).append(partition_sessions_df.reset_index(drop=True))

        if (
            partition_number == 1
            or partition_number % 25 == 0
            or partition_number == len(calendar_partition_keys)
        ):
            click.echo(
                "planning_progress: "
                f"table={TABLE_NAME}; "
                f"partitions={partition_number}/{len(calendar_partition_keys)}; "
                f"key={calendar_partition_key}; rows={scanned_rows}; "
                f"pending={pending_count}; "
                f"elapsed_seconds={time.perf_counter() - planning_started_at:.3f}"
            )

    plans = []
    for partition_key, partition_frames in sorted(
        pending_frames_by_fact_partition.items()
    ):
        pending_sessions_df = pd.concat(partition_frames, ignore_index=True)
        plans.append(
            {
                "partition_key": partition_key,
                "pending_sessions_df": pending_sessions_df,
                "expected_rows": int(
                    pending_sessions_df["expected_bar_count"].sum()
                ),
            }
        )
    policy_changed_count = sum(
        len(policy_updates_df)
        for policy_updates_df in policy_updates_by_partition.values()
    )
    mode = "explicit" if has_explicit_dates else "automatic"
    plan_name = "explicit_plan" if has_explicit_dates else "auto_plan"
    click.echo(
        f"{plan_name}: table={TABLE_NAME}; "
        f"policy_changed={policy_changed_count}; "
        f"selected_sessions={selected_count}; "
        f"complete_sessions={complete_count}; "
        f"pending_sessions={pending_count}; "
        f"pending_partitions={len(plans)}; "
        f"expected_pending_rows={expected_pending_rows}; "
        f"planning_seconds={time.perf_counter() - planning_started_at:.3f}"
    )

    if not plans:
        if write and policy_updates_by_partition:
            calendar_commit_started_at = time.perf_counter()
            policy_rows, state_rows, calendar_partitions = (
                commit_calendar_updates(
                    policy_updates_by_partition,
                    {},
                    resolved_lake_root,
                    fetch_run_id,
                    run_updated_at,
                )
            )
            click.echo(
                "calendar_committed: "
                f"policy_rows={policy_rows}; state_rows={state_rows}; "
                f"partitions={calendar_partitions}; "
                f"elapsed_seconds="
                f"{time.perf_counter() - calendar_commit_started_at:.3f}"
            )
        click.echo(f"up_to_date: table={TABLE_NAME}; mode={mode}")
        return

    from config.jqdata_connection import authenticate_jqdata

    jqdata = authenticate_jqdata(settings.jqdata_id, settings.jqdata_secret)
    processed_partitions = 0
    completed_partitions = 0
    committed_rows = 0
    returned_rows = 0
    invalid_ohlc_rows = 0
    completed_session_count = 0
    quota_stop_message = None
    completion_update_frames_by_partition: dict[
        tuple[object, ...], list[pd.DataFrame]
    ] = {}
    for partition_number, plan in enumerate(plans, start=1):
        partition_key = plan["partition_key"]
        expected_rows = int(plan["expected_rows"])
        spare = quota_spare(jqdata)
        if spare is not None and spare - expected_rows < quota_reserve:
            quota_stop_message = (
                "quota_stop: "
                f"partition={partition_number}/{len(plans)}; "
                f"key={partition_key}; spare={spare}; "
                f"expected={expected_rows}; reserve={quota_reserve}; "
                f"completed_partitions={completed_partitions}; "
                "未请求或写入当前分区。"
            )
            break

        click.echo(
            "partition_start: "
            f"partition={partition_number}/{len(plans)}; "
            f"key={partition_key}; "
            f"sessions={len(plan['pending_sessions_df'])}; "
            f"expected_rows={expected_rows}"
        )
        collected_df, partition_returned_rows, invalid_session_counts = (
            collect_partition(
                jqdata,
                plan["pending_sessions_df"],
                run_updated_at,
            )
        )
        returned_rows += partition_returned_rows
        processed_partitions += 1
        partition_invalid_rows = sum(invalid_session_counts.values())
        invalid_ohlc_rows += partition_invalid_rows
        if not write:
            continue

        existing_df = read_complete_partition(
            target_path,
            FUTURES_MINUTE_SCHEMA,
            PARTITION_COLUMNS,
            partition_key,
        )
        committed_requested_df = commit_fact_partition(
            collected_df,
            existing_df,
            plan["pending_sessions_df"],
            partition_key,
            resolved_lake_root,
        )
        partition_completion_updates = build_calendar_completion_updates(
            plan["pending_sessions_df"],
            committed_requested_df,
            invalid_session_counts,
        )
        for calendar_partition_key, completion_updates_df in (
            partition_completion_updates.items()
        ):
            completion_update_frames_by_partition.setdefault(
                calendar_partition_key, []
            ).append(completion_updates_df)
        completed_partitions += 1
        completed_session_count += len(plan["pending_sessions_df"])
        committed_rows += len(committed_requested_df)
        click.echo(
            "fact_partition_committed: "
            f"key={partition_key}; "
            f"fact_rows={len(committed_requested_df)}; "
            f"pending_calendar_rows={len(plan['pending_sessions_df'])}; "
            f"invalid_ohlc_rows={partition_invalid_rows}"
        )

    if not write:
        click.echo(
            f"api_success: partitions={processed_partitions}; "
            f"planned_partitions={len(plans)}; "
            f"sessions={pending_count}; returned_rows={returned_rows}; "
            f"invalid_ohlc_rows={invalid_ohlc_rows}; write=False"
        )
        if quota_stop_message is not None:
            click.echo(quota_stop_message)
        return

    completion_updates_by_partition = {
        partition_key: pd.concat(update_frames, ignore_index=True)
        for partition_key, update_frames in (
            completion_update_frames_by_partition.items()
        )
    }
    calendar_commit_started_at = time.perf_counter()
    policy_rows, completed_sessions, calendar_partitions = (
        commit_calendar_updates(
            policy_updates_by_partition,
            completion_updates_by_partition,
            resolved_lake_root,
            fetch_run_id,
            run_updated_at,
        )
    )
    click.echo(
        "calendar_committed: "
        f"policy_rows={policy_rows}; state_rows={completed_sessions}; "
        f"partitions={calendar_partitions}; "
        f"elapsed_seconds="
        f"{time.perf_counter() - calendar_commit_started_at:.3f}"
    )
    if completed_sessions != completed_session_count:
        raise RuntimeError("日历完成摘要行数与已提交 Session 数不一致。")
    if quota_stop_message is not None:
        click.echo(quota_stop_message)
    click.echo(
        "committed: mode=automatic_gap_fill"
        if not has_explicit_dates
        else "committed: mode=explicit_non_formal"
    )
    click.echo(
        f"completed_partitions={completed_partitions}; "
        f"completed_sessions={completed_sessions}; "
        f"fact_rows={committed_rows}; returned_rows={returned_rows}; "
        f"invalid_ohlc_rows={invalid_ohlc_rows}; "
        f"remaining_current_plan={pending_count - completed_sessions}"
    )


if __name__ == "__main__":
    main()

