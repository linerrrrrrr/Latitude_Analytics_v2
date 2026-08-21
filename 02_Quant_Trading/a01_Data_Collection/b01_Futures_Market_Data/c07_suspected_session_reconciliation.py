#!/usr/bin/env python
# coding: utf-8

# # c07 疑似休市 Session 定向校对
# 
# 目标表：`dim_futures_bar_calendar`。
# 
# 本入口只处理已经由 c06 形成的 `is_fetch_required=true AND schedule_status=suspected_closed`
# 分钟 Session。它排除当前疑似 Session，使用其余完整 Session 的一分钟事实重聚合日线，
# 再与 JQData 日线事实比较。
# 
# 一致结果只把证据升级为 `reconciled`：它是强旁证，不是交易所权威休市公告。因此本入口绝不
# 写出 `confirmed_closed`，也绝不把既有 `is_fetch_required=true` 改为 `false`。
# 

# ## 运行与写入契约
# 
# - 不指定日期或合约时，程序自动扫描新增或证据发生变化的疑似 Session。
# - `--write` 只控制是否把校对证据写回湖仓；不带该参数时仍完成读取、聚合和内存质检。
# - 显式日期或 `--contract-code` 只允许只读检查，或写入明确不同于正式湖的临时/测试湖。
# - 回写以完整 `bar_frequency/exchange_code/year/month` 叶分区为单位，保留同月未触达行。
# 

# In[ ]:


from __future__ import annotations

import hashlib
import pathlib
import shutil
import sys
import uuid
from datetime import datetime, timezone
from typing import Callable

# Notebook 可以从项目任意子目录启动；根目录定位方法由 .env.template 统一规定。
project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        break
else:
    raise RuntimeError("未找到项目根目录")

import click
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from config.data_contracts import (
    FUTURES_BAR_CALENDAR_SCHEMA,
    FUTURES_CONTRACT_CALENDAR_SCHEMA,
    FUTURES_DAILY_SCHEMA,
    FUTURES_MINUTE_SCHEMA,
    arrow_to_pandas,
    pandas_to_arrow,
)
from config.settings import settings


# ## Schema 契约交互浏览

# In[ ]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from config.notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        FUTURES_CONTRACT_CALENDAR_SCHEMA,
        FUTURES_BAR_CALENDAR_SCHEMA,
        FUTURES_DAILY_SCHEMA,
        FUTURES_MINUTE_SCHEMA,
    ])


# ## 表名、主键、分区与比较容差
# 
# 四张输入/输出表都直接引用 `config.data_contracts` 的权威 Arrow Schema。
# `EVIDENCE_RULE_VERSION` 是校对语义版本；比较规则变化时必须同步递增，使旧证据自动重新进入待办。
# 

# In[ ]:


CALENDAR_SCHEMA = FUTURES_BAR_CALENDAR_SCHEMA  # 校对结果回写的行情日历 Schema。
CONTRACT_SCHEMA = FUTURES_CONTRACT_CALENDAR_SCHEMA  # 提供 Session 与 tick_size 的合约日历 Schema。
DAILY_SCHEMA = FUTURES_DAILY_SCHEMA  # 提供独立日线对照值的事实 Schema。
MINUTE_SCHEMA = FUTURES_MINUTE_SCHEMA  # 提供其他 Session 分钟事实的 Schema。

CALENDAR_TABLE_NAME = "dim_futures_bar_calendar"  # 校对证据和质量状态的回写表。
CONTRACT_TABLE_NAME = "dim_futures_contract_calendar"  # 合约 Session 及交易规则维度表。
DAILY_TABLE_NAME = "fact_futures_daily"  # JQData 合约日线事实表。
MINUTE_TABLE_NAME = "fact_futures_minute"  # JQData 合约一分钟行情事实表。

CALENDAR_PRIMARY_KEY = [  # 唯一标识一个日线或分钟 Session 行情格点。
    "bar_frequency",  # 行情频率；候选必须为 1m。
    "contract_code",  # 固定月份合约代码。
    "trading_date",  # 行情归属交易日。
    "session_number",  # 分钟 Session 顺序号。
]
CONTRACT_PRIMARY_KEY = [  # 唯一标识合约日历中的一段 Session。
    "contract_code",  # 固定月份合约代码。
    "trading_date",  # Session 归属交易日。
    "session_number",  # 同一合约日内的 Session 顺序号。
]
DAILY_PRIMARY_KEY = [  # 唯一标识一条合约日线事实。
    "contract_code",  # 固定月份合约代码。
    "trading_date",  # 日线归属交易日。
]
MINUTE_PRIMARY_KEY = [  # 唯一标识一根合约分钟 bar。
    "contract_code",  # 固定月份合约代码。
    "bar_at",  # Asia/Shanghai 时区的分钟结束时刻。
]

CALENDAR_PARTITION_COLUMNS = [  # 行情日历的 Hive 叶分区层级。
    "bar_frequency",  # 行情频率；本入口只触达 1m。
    "exchange_code",  # 交易所代码。
    "year",  # 交易年份。
    "month",  # 交易月份。
]
CONTRACT_PARTITION_COLUMNS = [  # 合约 Session 日历的 Hive 叶分区。
    "exchange_code",  # 交易所代码。
    "year",  # 交易年份。
    "month",  # 交易月份。
]
FACT_PARTITION_COLUMNS = [  # 日线和分钟事实共同使用的 Hive 叶分区。
    "exchange_code",  # 合约所属交易所。
    "underlying_code",  # 合约所属期货品种。
    "year",  # 交易年份。
    "month",  # 交易月份。
]

CALENDAR_PARTITIONING = ds.partitioning(
    pa.schema(
        [
            CALENDAR_SCHEMA.field(name)
            for name in CALENDAR_PARTITION_COLUMNS
        ]
    ),
    flavor="hive",
)
CONTRACT_PARTITIONING = ds.partitioning(
    pa.schema(
        [
            CONTRACT_SCHEMA.field(name)
            for name in CONTRACT_PARTITION_COLUMNS
        ]
    ),
    flavor="hive",
)
DAILY_PARTITIONING = ds.partitioning(
    pa.schema(
        [DAILY_SCHEMA.field(name) for name in FACT_PARTITION_COLUMNS]
    ),
    flavor="hive",
)
MINUTE_PARTITIONING = ds.partitioning(
    pa.schema(
        [MINUTE_SCHEMA.field(name) for name in FACT_PARTITION_COLUMNS]
    ),
    flavor="hive",
)

EVIDENCE_RULE_VERSION = "c07-v2"
PRICE_TOLERANCE_FACTOR = 0.5
VOLUME_ATOL = 1e-6
MONEY_RTOL = 1e-6
MONEY_ATOL = 0.01
OPEN_INTEREST_ATOL = 1e-6

PRICE_METRICS = [  # 逐项使用 tick_size/2 容差比较的价格度量。
    "open",  # 开盘价。
    "high",  # 最高价。
    "low",  # 最低价。
    "close",  # 收盘价。
]
QUANTITY_METRICS = [  # 使用各自契约容差比较的数量/金额度量。
    "volume",  # 成交量，单位为手。
    "money",  # 成交额，单位为元。
    "open_interest",  # 收盘或末条有效持仓量，单位为手。
]
EVIDENCE_METRICS = [  # 日线原值与分钟重聚合值共同保存的七类度量。
    *PRICE_METRICS,  # 开、高、低、收四类价格。
    *QUANTITY_METRICS,  # 成交量、成交额、持仓量。
]
EVIDENCE_COLUMNS = [  # c07 写回行情日历的原值和四类比较结论。
    *[f"daily_{name}" for name in EVIDENCE_METRICS],  # JQData 日线原值旁证。
    *[f"aggregated_{name}" for name in EVIDENCE_METRICS],  # 其他完整 Session 的重聚合值。
    "ohlc_matches_daily",  # 四项价格是否全部在容差内匹配。
    "volume_matches_daily",  # 成交量是否匹配。
    "money_matches_daily",  # 成交额是否匹配。
    "open_interest_matches_daily",  # 末持仓量是否匹配。
]

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


# ## 输出日历的完整契约校验
# 
# c07 是 `dim_futures_bar_calendar` 的状态生产者。即使只修改一行证据，也必须在 staging 和正式
# 路径复读时校验整个叶分区，防止局部回写破坏主键、分区、Session 或完成状态不变量。
# 

# In[ ]:


def validate_calendar_frame(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    # Arrow 转换先固定列顺序、类型、nullable 和全部中文 metadata。
    table = pandas_to_arrow(
        frame.loc[:, CALENDAR_SCHEMA.names],
        CALENDAR_SCHEMA,
    )
    checked_df = arrow_to_pandas(table, CALENDAR_SCHEMA)

    if checked_df.duplicated(CALENDAR_PRIMARY_KEY).any():
        raise ValueError(f"{context}主键不唯一。")

    for row in table.to_pylist():
        # 状态枚举与中文说明是可审计契约，不能用空字符串占位。
        if row["bar_frequency"] not in {"1d", "1m"}:
            raise ValueError(f"{context}bar_frequency 不在允许枚举中。")
        if row["schedule_status"] not in SCHEDULE_STATUSES:
            raise ValueError(f"{context}schedule_status 不在允许枚举中。")
        if row["evidence_level"] not in EVIDENCE_LEVELS:
            raise ValueError(f"{context}evidence_level 不在允许枚举中。")
        if row["quality_status"] not in QUALITY_STATUSES:
            raise ValueError(f"{context}quality_status 不在允许枚举中。")

        text_fields = [
            "schedule_signal_reason",
            "evidence_source",
            "selection_reason",
            "quality_reason",
        ]
        if any(not str(row[name]).strip() for name in text_fields):
            raise ValueError(f"{context}状态说明字段不得为空。")

        # 合约后缀和日期必须能复算 Hive 分区，避免写入错误叶目录。
        if not row["contract_code"].endswith(
            f".{row['exchange_code']}"
        ):
            raise ValueError(f"{context}交易所与合约代码后缀不一致。")
        if (
            row["year"] != row["trading_date"].year
            or row["month"] != row["trading_date"].month
        ):
            raise ValueError(
                f"{context}year/month 与 trading_date 不一致。"
            )
        if row["expected_bar_count"] <= 0:
            raise ValueError(
                f"{context}expected_bar_count 必须大于 0。"
            )
        if row["actual_bar_count"] < 0 or row["missing_bar_count"] < 0:
            raise ValueError(f"{context}实际与缺失条数不得为负。")

        # 日线只有保留编号 0；分钟线必须具有完整 Session 边界。
        if row["bar_frequency"] == "1d":
            session_values = [
                row["session_text"],
                row["session_start_at"],
                row["session_end_at"],
                row["is_night_session"],
            ]
            if row["session_number"] != 0:
                raise ValueError(
                    f"{context}日线 session_number 必须为 0。"
                )
            if any(value is not None for value in session_values):
                raise ValueError(
                    f"{context}日线 Session 专属字段必须为空。"
                )
            if row["expected_bar_count"] != 1:
                raise ValueError(f"{context}日线理论条数必须为 1。")
        else:
            if row["session_number"] <= 0:
                raise ValueError(
                    f"{context}分钟 session_number 必须大于 0。"
                )
            if row["session_start_at"] >= row["session_end_at"]:
                raise ValueError(
                    f"{context}分钟 Session 起点必须早于终点。"
                )

        # 只有权威证据可以确认休市并免除拉取。
        if row["schedule_status"] == "confirmed_closed":
            if row["evidence_level"] != "authoritative":
                raise ValueError(f"{context}确认休市必须具有权威证据。")
            if row["is_fetch_required"]:
                raise ValueError(f"{context}确认休市不得继续要求拉取。")

        # 完成状态、运行批次和正式复读时间必须共同出现。
        if row["is_fetch_completed"]:
            if not row["fetch_run_id"] or row["fetch_completed_at"] is None:
                raise ValueError(f"{context}完成状态缺少批次或完成时间。")
        elif row["fetch_completed_at"] is not None:
            raise ValueError(f"{context}未完成格点不得具有完成时间。")

        # 缺失状态由理论条数和正式事实实际条数直接复算。
        if row["missing_checked_at"] is None:
            if row["is_data_missing"] or row["missing_bar_count"] != 0:
                raise ValueError(f"{context}未经检查不得记录缺失。")
        else:
            expected_missing = (
                max(
                    row["expected_bar_count"]
                    - row["actual_bar_count"],
                    0,
                )
                if row["is_fetch_required"]
                else 0
            )
            if row["missing_bar_count"] != expected_missing:
                raise ValueError(
                    f"{context}missing_bar_count 无法复算。"
                )
            if row["is_data_missing"] != (expected_missing > 0):
                raise ValueError(
                    f"{context}is_data_missing 与缺失条数不一致。"
                )

        if (
            row["quality_status"] != "pending"
            and row["quality_checked_at"] is None
        ):
            raise ValueError(f"{context}非 pending 状态缺少质检时间。")

        # reconciled 只能表达当前疑似 Session 的全部比较均一致。
        if row["evidence_level"] == "reconciled":
            comparison_columns = [
                "ohlc_matches_daily",
                "volume_matches_daily",
                "money_matches_daily",
                "open_interest_matches_daily",
            ]
            if row["schedule_status"] != "suspected_closed":
                raise ValueError(
                    f"{context}reconciled 只能用于疑似休市 Session。"
                )
            if not all(row[name] is True for name in comparison_columns):
                raise ValueError(
                    f"{context}reconciled 必须具有四类一致旁证。"
                )

    return checked_df.sort_values(
        CALENDAR_PRIMARY_KEY
    ).reset_index(drop=True)


# ## 契约化读取与分区表达式
# 
# 读取者只检查上游的精确 Schema/metadata、主键和本次计算直接依赖的分区边界；上游完整业务
# 质检仍由各自生产者负责。候选扫描只返回疑似格点，随后按叶分区定向读取相关事实。
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


def open_exact_dataset(
    table_path: pathlib.Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    label: str,
) -> ds.Dataset:
    parquet_files = (
        list(table_path.rglob("*.parquet"))
        if table_path.is_dir()
        else []
    )
    if not parquet_files:
        raise FileNotFoundError(f"{label}不存在：{table_path}")

    dataset = ds.dataset(
        table_path,
        format="parquet",
        partitioning=partitioning,
    )
    actual_schema = reconstructed_schema(dataset, schema)
    if not actual_schema.equals(schema, check_metadata=True):
        raise TypeError(f"{label} Schema/metadata 与契约不一致。")

    return dataset


def partition_expression(
    partition_columns: list[str],
    partition_key: tuple[object, ...],
) -> ds.Expression:
    expression = None

    for column, value in zip(
        partition_columns,
        partition_key,
        strict=True,
    ):
        condition = ds.field(column) == value
        expression = (
            condition
            if expression is None
            else expression & condition
        )

    if expression is None:
        raise ValueError("分区键不得为空。")
    return expression


# ## 单个日历叶分区的定向校对
# 
# 输入指纹只包含 c06 完成状态、同日其他 Session、合约 `tick_size`、JQData 日线及一分钟事实。
# 有限 OHLC 跨列异常的原值可以存在于事实表，但不能作为把疑似休市升级为 `passed` 的可靠旁证。
# c07 自己写入的证据列不进入指纹，因此来源没有变化时不会反复更新时间和重写分区。
# 

# In[ ]:


def invalid_ohlc_mask(frame: pd.DataFrame) -> pd.Series:
    # c07 不拒收上游事实，只识别不能用于通过旁证的有限 OHLC 跨列异常。
    if frame.empty:
        return pd.Series(False, index=frame.index, dtype=bool)

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


def reconcile_partition(
                calendar_df: pd.DataFrame,
                candidate_keys: set[tuple[object, ...]],
                contract_df: pd.DataFrame,
                daily_df: pd.DataFrame,
                minute_df: pd.DataFrame,
                checked_at: datetime,
            ) -> tuple[pd.DataFrame, pd.DataFrame, int]:
                updated_df = calendar_df.copy()
                changed_keys: set[tuple[object, ...]] = set()
                unchanged_evidence_count = 0

                key_series = updated_df[CALENDAR_PRIMARY_KEY].apply(tuple, axis=1)
                candidate_indices = updated_df.index[key_series.isin(candidate_keys)]

                for index in candidate_indices:
                    row = updated_df.loc[index]
                    contract_code = row["contract_code"]
                    trading_date = row["trading_date"]
                    session_number = row["session_number"]

                    # 同日 Session 状态决定哪些分钟可以作为“保留 Session”旁证。
                    same_day_calendar_df = updated_df.loc[
                        updated_df["bar_frequency"].eq("1m")
                        & updated_df["contract_code"].eq(contract_code)
                        & updated_df["trading_date"].eq(trading_date)
                    ].copy()
                    other_calendar_df = same_day_calendar_df.loc[
                        same_day_calendar_df["session_number"].ne(session_number)
                    ].copy()

                    same_day_contract_df = contract_df.loc[
                        contract_df["contract_code"].eq(contract_code)
                        & contract_df["trading_date"].eq(trading_date)
                    ].copy()
                    same_day_daily_df = daily_df.loc[
                        daily_df["contract_code"].eq(contract_code)
                        & daily_df["trading_date"].eq(trading_date)
                    ].copy()
                    same_day_minute_df = minute_df.loc[
                        minute_df["contract_code"].eq(contract_code)
                        & minute_df["trading_date"].eq(trading_date)
                    ].copy()

                    # 规则版本和所有直接证据共同组成稳定指纹。
                    digest = hashlib.sha256()
                    digest.update(EVIDENCE_RULE_VERSION.encode("utf-8"))

                    fingerprint_inputs = [
                        (
                            "suspected_session",
                            updated_df.loc[[index]],
                            [
                                "contract_code",
                                "trading_date",
                                "session_number",
                                "schedule_status",
                                "schedule_signal_reason",
                                "is_fetch_required",
                                "expected_bar_count",
                                "is_fetch_completed",
                                "actual_bar_count",
                                "is_data_missing",
                                "missing_bar_count",
                                "fetch_run_id",
                                "fetch_completed_at",
                                "missing_checked_at",
                            ],
                            CALENDAR_PRIMARY_KEY,
                        ),
                        (
                            "other_sessions",
                            other_calendar_df,
                            [
                                "contract_code",
                                "trading_date",
                                "session_number",
                                "schedule_status",
                                "is_fetch_required",
                                "expected_bar_count",
                                "is_fetch_completed",
                                "actual_bar_count",
                                "is_data_missing",
                                "missing_bar_count",
                                "fetch_run_id",
                                "fetch_completed_at",
                                "missing_checked_at",
                            ],
                            ["contract_code", "trading_date", "session_number"],
                        ),
                        (
                            "contract_tick_size",
                            same_day_contract_df,
                            [
                                "contract_code",
                                "trading_date",
                                "session_number",
                                "tick_size",
                                "updated_at",
                            ],
                            CONTRACT_PRIMARY_KEY,
                        ),
                        (
                            "daily_fact",
                            same_day_daily_df,
                            [
                                "contract_code",
                                "trading_date",
                                "open",
                                "high",
                                "low",
                                "close",
                                "volume",
                                "money",
                                "open_interest",
                                "has_market_data",
                                "updated_at",
                            ],
                            DAILY_PRIMARY_KEY,
                        ),
                        (
                            "minute_fact",
                            same_day_minute_df,
                            [
                                "contract_code",
                                "trading_date",
                                "session_number",
                                "bar_at",
                                "open",
                                "high",
                                "low",
                                "close",
                                "volume",
                                "money",
                                "open_interest",
                                "updated_at",
                            ],
                            MINUTE_PRIMARY_KEY,
                        ),
                    ]

                    for label, source_df, columns, sort_columns in fingerprint_inputs:
                        fingerprint_df = source_df.sort_values(sort_columns).loc[
                            :, columns
                        ].copy()
                        for column in columns:
                            fingerprint_df[column] = (
                                fingerprint_df[column]
                                .astype("string")
                                .fillna("<NULL>")
                            )

                        digest.update(label.encode("utf-8"))
                        digest.update(
                            fingerprint_df.to_csv(
                                index=False,
                                lineterminator="\n",
                            ).encode("utf-8")
                        )

                    evidence_source = (
                        "c07:daily_vs_other_sessions:"
                        f"rule={EVIDENCE_RULE_VERSION};sha256={digest.hexdigest()}"
                    )
                    if row["evidence_source"] == evidence_source:
                        unchanged_evidence_count += 1
                        continue

                    # 每轮先清空旧旁证，避免证据不足时保留上一轮的比较布尔值。
                    for evidence_column in EVIDENCE_COLUMNS:
                        updated_df.at[index, evidence_column] = None

                    issues: list[str] = []

                    # c07 只接受 c06 已完成、正式复读为 0 条的当前疑似 Session。
                    if not row["is_fetch_completed"]:
                        issues.append("当前疑似 Session 尚未完成正式事实复读")
                    if row["actual_bar_count"] != 0:
                        issues.append("当前疑似 Session 的正式实际条数不是 0")
                    if not row["is_data_missing"]:
                        issues.append("当前疑似 Session 未记录缺失状态")
                    if row["missing_bar_count"] != row["expected_bar_count"]:
                        issues.append("当前疑似 Session 的缺失条数无法由理论条数复算")

                    # tick_size 在同一合约日的各 Session 中必须唯一且为正。
                    tick_sizes = pd.to_numeric(
                        same_day_contract_df["tick_size"],
                        errors="coerce",
                    ).dropna().astype(float).unique()
                    tick_size = None
                    if (
                        len(tick_sizes) == 1
                        and np.isfinite(tick_sizes[0])
                        and tick_sizes[0] > 0
                    ):
                        tick_size = float(tick_sizes[0])
                    else:
                        issues.append("当日 tick_size 缺失、不唯一或非正")

                    # 日线必须唯一且明确具有市场数据。
                    daily_row = None
                    if len(same_day_daily_df) != 1:
                        issues.append("JQData 日线事实缺失或不唯一")
                    elif not bool(same_day_daily_df.iloc[0]["has_market_data"]):
                        issues.append("JQData 日线没有有效市场数据")
                        daily_row = same_day_daily_df.iloc[0]
                    else:
                        daily_row = same_day_daily_df.iloc[0]

                    # 权威确认休市的其他 Session 可以排除；另一个疑似 Session 会使旁证不可归因。
                    uncertain_other_df = other_calendar_df.loc[
                        other_calendar_df["schedule_status"].eq(
                            "suspected_closed"
                        )
                    ]
                    if not uncertain_other_df.empty:
                        issues.append("同一合约日还存在其他疑似休市 Session")

                    retained_calendar_df = other_calendar_df.loc[
                        other_calendar_df["schedule_status"].eq("scheduled")
                        & other_calendar_df["is_fetch_required"].eq(True)
                    ].copy()
                    non_authoritative_other_df = other_calendar_df.loc[
                        other_calendar_df["schedule_status"].ne(
                            "confirmed_closed"
                        )
                    ]
                    if retained_calendar_df.empty:
                        issues.append("没有可用于重聚合的其他计划开市 Session")
                    if len(retained_calendar_df) != len(non_authoritative_other_df):
                        issues.append("其他非权威休市 Session 未全部处于需拉取计划状态")

                    retained_session_numbers = set(
                        retained_calendar_df["session_number"].tolist()
                    )
                    retained_minute_df = same_day_minute_df.loc[
                        same_day_minute_df["session_number"].isin(
                            retained_session_numbers
                        )
                    ].sort_values("bar_at")
                    invalid_retained_count = int(
                        invalid_ohlc_mask(retained_minute_df).sum()
                    )
                    if invalid_retained_count:
                        issues.append(
                            "其他完整 Session 包含 "
                            f"{invalid_retained_count} 条原始 OHLC 跨列关系异常"
                        )

                    # 每个保留 Session 必须由日历状态和正式分钟计数共同证明完整。
                    minute_counts = (
                        retained_minute_df.groupby("session_number", dropna=False)
                        .size()
                        .to_dict()
                    )
                    for retained_row in retained_calendar_df.to_dict(
                        orient="records"
                    ):
                        retained_number = retained_row["session_number"]
                        fact_count = int(minute_counts.get(retained_number, 0))
                        if (
                            not retained_row["is_fetch_completed"]
                            or retained_row["actual_bar_count"]
                            != retained_row["expected_bar_count"]
                            or retained_row["missing_bar_count"] != 0
                            or retained_row["is_data_missing"]
                            or fact_count != retained_row["expected_bar_count"]
                        ):
                            issues.append(
                                f"其他 Session {retained_number} 未被正式状态与事实共同证明完整"
                            )

                    # 有多少证据就保存多少原值；比较结论只有在全部必要证据完备时才生成。
                    daily_values = {name: None for name in EVIDENCE_METRICS}
                    if daily_row is not None:
                        for name in EVIDENCE_METRICS:
                            raw_value = daily_row[name]
                            if pd.notna(raw_value):
                                normalized_value = float(raw_value)
                                if not np.isfinite(normalized_value):
                                    raise ValueError(
                                        f"日线直接依赖字段 {name} 包含非有限数。"
                                    )
                                daily_values[name] = normalized_value
                        if invalid_ohlc_mask(same_day_daily_df).any():
                            issues.append(
                                "JQData 日线事实包含原始 OHLC 跨列关系异常"
                            )

                    aggregated_values = {
                        name: None for name in EVIDENCE_METRICS
                    }
                    if not retained_minute_df.empty:
                        open_interest_series = retained_minute_df[
                            "open_interest"
                        ].dropna()
                        aggregated_raw = {
                            "open": retained_minute_df.iloc[0]["open"],
                            "high": retained_minute_df["high"].max(),
                            "low": retained_minute_df["low"].min(),
                            "close": retained_minute_df.iloc[-1]["close"],
                            "volume": retained_minute_df["volume"].sum(
                                min_count=1
                            ),
                            "money": retained_minute_df["money"].sum(
                                min_count=1
                            ),
                            "open_interest": (
                                open_interest_series.iloc[-1]
                                if not open_interest_series.empty
                                else None
                            ),
                        }
                        for name, raw_value in aggregated_raw.items():
                            if pd.notna(raw_value):
                                normalized_value = float(raw_value)
                                if not np.isfinite(normalized_value):
                                    raise ValueError(
                                        f"分钟直接依赖字段 {name} 包含非有限数。"
                                    )
                                aggregated_values[name] = normalized_value

                    for name in EVIDENCE_METRICS:
                        updated_df.at[index, f"daily_{name}"] = daily_values[name]
                        updated_df.at[index, f"aggregated_{name}"] = (
                            aggregated_values[name]
                        )

                    missing_evidence = [
                        name
                        for name in EVIDENCE_METRICS
                        if daily_values[name] is None
                        or aggregated_values[name] is None
                    ]
                    if missing_evidence:
                        issues.append(
                            "必要日线或分钟证据缺值："
                            + ",".join(missing_evidence)
                        )

                    if issues:
                        # 证据不足或结构不完整时不猜测比较结果，也不升级证据等级。
                        updated_df.at[index, "evidence_level"] = "inferred"
                        updated_df.at[index, "quality_status"] = "warning"
                        updated_df.at[index, "quality_reason"] = (
                            "疑似休市定向校对证据不足："
                            + "；".join(dict.fromkeys(issues))
                            + "。仍保持疑似休市及拉取要求。"
                        )
                    else:
                        price_atol = tick_size * PRICE_TOLERANCE_FACTOR
                        ohlc_matches = all(
                            np.isclose(
                                daily_values[name],
                                aggregated_values[name],
                                rtol=0.0,
                                atol=price_atol,
                            )
                            for name in PRICE_METRICS
                        )
                        volume_matches = bool(
                            np.isclose(
                                daily_values["volume"],
                                aggregated_values["volume"],
                                rtol=0.0,
                                atol=VOLUME_ATOL,
                            )
                        )
                        money_matches = bool(
                            np.isclose(
                                daily_values["money"],
                                aggregated_values["money"],
                                rtol=MONEY_RTOL,
                                atol=MONEY_ATOL,
                            )
                        )
                        open_interest_matches = bool(
                            np.isclose(
                                daily_values["open_interest"],
                                aggregated_values["open_interest"],
                                rtol=0.0,
                                atol=OPEN_INTEREST_ATOL,
                            )
                        )

                        updated_df.at[index, "ohlc_matches_daily"] = bool(
                            ohlc_matches
                        )
                        updated_df.at[index, "volume_matches_daily"] = (
                            volume_matches
                        )
                        updated_df.at[index, "money_matches_daily"] = money_matches
                        updated_df.at[index, "open_interest_matches_daily"] = (
                            open_interest_matches
                        )

                        comparison_results = {
                            "OHLC": bool(ohlc_matches),
                            "volume": volume_matches,
                            "money": money_matches,
                            "open_interest": open_interest_matches,
                        }
                        mismatched_names = [
                            name
                            for name, is_matched in comparison_results.items()
                            if not is_matched
                        ]

                        if not mismatched_names:
                            updated_df.at[index, "evidence_level"] = "reconciled"
                            updated_df.at[index, "quality_status"] = "passed"
                            updated_df.at[index, "quality_reason"] = (
                                "排除当前疑似 Session 后，其他完整 Session 重聚合与 "
                                "JQData 日线一致；OHLC 容差为 tick_size/2，成交量和持仓量 "
                                "绝对容差为 1e-6，成交额相对容差为 1e-6 且绝对容差为 "
                                "0.01。该结论仅为 reconciled 旁证，不确认休市且不取消拉取。"
                            )
                        else:
                            updated_df.at[index, "evidence_level"] = "inferred"
                            updated_df.at[index, "quality_status"] = "warning"
                            updated_df.at[index, "quality_reason"] = (
                                "排除当前疑似 Session 后，其他完整 Session 重聚合与 "
                                "JQData 日线存在不一致："
                                + ",".join(mismatched_names)
                                + "。不能据此确认休市，仍保持拉取要求。"
                            )

                    # 本入口只写证据、质量和审计时间；调度状态与拉取要求保持原值。
                    updated_df.at[index, "evidence_source"] = evidence_source
                    updated_df.at[index, "quality_checked_at"] = checked_at
                    updated_df.at[index, "updated_at"] = checked_at
                    changed_keys.add(
                        tuple(row[name] for name in CALENDAR_PRIMARY_KEY)
                    )

                checked_df = validate_calendar_frame(
                    updated_df,
                    "定向校对后的完整日历分区",
                )
                if changed_keys:
                    checked_key_series = checked_df[CALENDAR_PRIMARY_KEY].apply(
                        tuple,
                        axis=1,
                    )
                    changed_df = checked_df.loc[
                        checked_key_series.isin(changed_keys)
                    ].copy()
                else:
                    changed_df = checked_df.iloc[0:0].copy()

                return checked_df, changed_df, unchanged_evidence_count


# ## 完整叶分区的 staging、正式复读与回滚
# 
# 调用方交付的是每个触达叶分区的全部行，而不是日期窗口。全部分区先写入同一 staging 并逐分区
# 复读，之后才替换正式目录；任何正式复读失败都会恢复本批次已经移动的旧分区。
# 

# In[ ]:


def commit_calendar_partitions(
    partition_frames: dict[tuple[object, ...], pd.DataFrame],
    lake_root: pathlib.Path,
) -> int:
    if not partition_frames:
        return 0

    checked_frames: dict[tuple[object, ...], pd.DataFrame] = {}
    expected_tables: dict[tuple[object, ...], pa.Table] = {}

    for partition_key, frame in partition_frames.items():
        checked_df = validate_calendar_frame(
            frame,
            "待提交完整日历分区",
        )
        actual_partition_keys = set(
            checked_df[CALENDAR_PARTITION_COLUMNS].itertuples(
                index=False,
                name=None,
            )
        )
        if actual_partition_keys != {partition_key}:
            raise ValueError("待提交日历数据越出指定 Hive 分区。")

        checked_frames[partition_key] = checked_df
        expected_tables[partition_key] = pandas_to_arrow(
            checked_df,
            CALENDAR_SCHEMA,
        )

    combined_df = pd.concat(
        list(checked_frames.values()),
        ignore_index=True,
    )
    combined_table = pandas_to_arrow(combined_df, CALENDAR_SCHEMA)

    silver_root = lake_root.resolve() / "silver"
    target_path = silver_root / CALENDAR_TABLE_NAME
    run_id = uuid.uuid4().hex
    staging_path = silver_root / (
        f".{CALENDAR_TABLE_NAME}.staging-{run_id}"
    )
    backup_path = silver_root / (
        f".{CALENDAR_TABLE_NAME}.backup-{run_id}"
    )
    quarantine_path = silver_root / (
        f".{CALENDAR_TABLE_NAME}.failed-{run_id}"
    )

    silver_root.mkdir(parents=True, exist_ok=True)
    for managed_path in (
        target_path,
        staging_path,
        backup_path,
        quarantine_path,
    ):
        if not managed_path.resolve().is_relative_to(silver_root):
            raise ValueError(
                f"数据集路径越出 silver 根目录：{managed_path}"
            )

    # 第一阶段：写 staging，并用正式分区规则逐叶复读完整内容。
    staging_path.mkdir(parents=True, exist_ok=False)
    try:
        ds.write_dataset(
            combined_table,
            staging_path,
            format="parquet",
            partitioning=CALENDAR_PARTITIONING,
            existing_data_behavior="delete_matching",
            basename_template="part-{i}.parquet",
        )

        expected_file_schema = pa.schema(
            [
                field
                for field in CALENDAR_SCHEMA
                if field.name not in CALENDAR_PARTITION_COLUMNS
            ],
            metadata=CALENDAR_SCHEMA.metadata,
        )
        for parquet_path in staging_path.rglob("*.parquet"):
            actual_file_schema = pq.read_schema(parquet_path)
            if not actual_file_schema.equals(
                expected_file_schema,
                check_metadata=True,
            ):
                raise TypeError(
                    "staging Parquet 文件 Schema/metadata 与契约不一致："
                    f"{parquet_path}"
                )

        staged_dataset = ds.dataset(
            staging_path,
            format="parquet",
            partitioning=CALENDAR_PARTITIONING,
        )
        if not reconstructed_schema(
            staged_dataset,
            CALENDAR_SCHEMA,
        ).equals(CALENDAR_SCHEMA, check_metadata=True):
            raise TypeError(
                "staging 数据集 Schema/metadata 与契约不一致。"
            )

        for partition_key, expected_table in expected_tables.items():
            staged_table = staged_dataset.to_table(
                columns=CALENDAR_SCHEMA.names,
                filter=partition_expression(
                    CALENDAR_PARTITION_COLUMNS,
                    partition_key,
                ),
            )
            staged_df = validate_calendar_frame(
                arrow_to_pandas(staged_table, CALENDAR_SCHEMA),
                "staging 完整日历分区",
            )
            if not pandas_to_arrow(
                staged_df,
                CALENDAR_SCHEMA,
            ).equals(expected_table):
                raise ValueError("staging 日历分区内容检查失败。")
    except Exception:
        shutil.rmtree(staging_path, ignore_errors=True)
        raise

    moved_partitions: list[
        tuple[pathlib.Path, pathlib.Path, pathlib.Path]
    ] = []
    cleanup_recovery_paths = True
    # 第二阶段：保存旧叶分区，移动已验证分区，再从正式根路径逐叶复读。
    try:
        backup_path.mkdir(parents=True, exist_ok=False)
        target_path.mkdir(parents=True, exist_ok=True)

        for partition_key in sorted(expected_tables):
            relative_path = pathlib.Path(
                *[
                    f"{name}={value}"
                    for name, value in zip(
                        CALENDAR_PARTITION_COLUMNS,
                        partition_key,
                        strict=True,
                    )
                ]
            )
            source_path = staging_path / relative_path
            destination_path = target_path / relative_path
            saved_path = backup_path / relative_path

            if not source_path.is_dir():
                raise FileNotFoundError(
                    f"staging 缺少日历叶分区：{relative_path}"
                )

            destination_path.parent.mkdir(
                parents=True,
                exist_ok=True,
            )
            saved_path.parent.mkdir(parents=True, exist_ok=True)
            if destination_path.exists():
                shutil.move(str(destination_path), str(saved_path))

            # 旧分区保存完成后立即登记；若新分区移动失败，except 仍能恢复旧目录。
            moved_partitions.append(
                (destination_path, saved_path, relative_path)
            )
            shutil.move(str(source_path), str(destination_path))

        committed_dataset = ds.dataset(
            target_path,
            format="parquet",
            partitioning=CALENDAR_PARTITIONING,
        )
        if not reconstructed_schema(
            committed_dataset,
            CALENDAR_SCHEMA,
        ).equals(CALENDAR_SCHEMA, check_metadata=True):
            raise TypeError(
                "正式日历数据集 Schema/metadata 与契约不一致。"
            )

        for partition_key, expected_table in expected_tables.items():
            committed_table = committed_dataset.to_table(
                columns=CALENDAR_SCHEMA.names,
                filter=partition_expression(
                    CALENDAR_PARTITION_COLUMNS,
                    partition_key,
                ),
            )
            committed_df = validate_calendar_frame(
                arrow_to_pandas(committed_table, CALENDAR_SCHEMA),
                "正式路径复读日历分区",
            )
            if not pandas_to_arrow(
                committed_df,
                CALENDAR_SCHEMA,
            ).equals(expected_table):
                raise ValueError("正式日历分区内容检查失败。")
    except Exception:
        # 已移动的新分区先隔离，再把本批次备份逐一恢复到原位置。
        try:
            for destination_path, saved_path, relative_path in reversed(
                moved_partitions
            ):
                if destination_path.exists():
                    failed_path = quarantine_path / relative_path
                    failed_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(destination_path), str(failed_path))
                if saved_path.exists():
                    destination_path.parent.mkdir(
                        parents=True,
                        exist_ok=True,
                    )
                    shutil.move(str(saved_path), str(destination_path))
        except Exception as rollback_error:
            # 回滚本身失败时保留 backup/quarantine，禁止 finally 再删除恢复依据。
            cleanup_recovery_paths = False
            raise RuntimeError(
                "日历分区提交失败且自动回滚未完成；"
                f"请保留并检查 {backup_path} 与 {quarantine_path}。"
            ) from rollback_error
        raise
    finally:
        # 三个路径均由本函数生成并已验证位于 silver 根目录内。
        shutil.rmtree(staging_path, ignore_errors=True)
        if cleanup_recovery_paths:
            shutil.rmtree(backup_path, ignore_errors=True)
            shutil.rmtree(quarantine_path, ignore_errors=True)

    return len(combined_table)


# ## CLI：自动候选、显式范围门禁与是否写入
# 
# 正式生产模式不接收手工范围。`--start-date/--end-date` 必须成对出现，`--contract-code` 可重复；
# 只要存在任一显式范围，正式湖的 `--write` 就会在任何数据读取之前拒绝。
# 

# In[ ]:


@click.command()
@click.option("--lake-root", type=click.Path(path_type=pathlib.Path))
@click.option("--start-date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--end-date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--contract-code", multiple=True)
@click.option("--write", is_flag=True)
def main(
    lake_root: pathlib.Path | None,
    start_date: datetime | None,
    end_date: datetime | None,
    contract_code: tuple[str, ...],
    write: bool,
) -> None:
    # 1. 正式路径只来自 settings；显式范围门禁先于任何数据集读取。
    formal_lake_root = settings.futures_lake_root.resolve()
    resolved_lake_root = (lake_root or formal_lake_root).resolve()

    if (start_date is None) != (end_date is None):
        raise click.UsageError(
            "--start-date 与 --end-date 必须同时提供。"
        )

    requested_start_date = (
        start_date.date() if start_date is not None else None
    )
    requested_end_date = (
        end_date.date() if end_date is not None else None
    )
    if (
        requested_start_date is not None
        and requested_start_date > requested_end_date
    ):
        raise click.BadParameter("起始日期不得晚于结束日期。")

    requested_contract_codes = tuple(
        dict.fromkeys(
            value.strip().upper()
            for value in contract_code
            if value.strip()
        )
    )
    has_explicit_scope = (
        requested_start_date is not None
        or bool(requested_contract_codes)
    )
    if (
        has_explicit_scope
        and write
        and resolved_lake_root == formal_lake_root
    ):
        raise click.UsageError(
            "显式日期或合约范围禁止写入 FUTURES_LAKE_ROOT 指向的正式湖；"
            "请移除范围参数使用自动候选，或改用非正式临时/测试湖。"
        )

    silver_root = resolved_lake_root / "silver"
    calendar_path = silver_root / CALENDAR_TABLE_NAME
    contract_path = silver_root / CONTRACT_TABLE_NAME
    daily_path = silver_root / DAILY_TABLE_NAME
    minute_path = silver_root / MINUTE_TABLE_NAME

    # 2. 先只打开行情日历并扫描需拉取的疑似 Session。
    calendar_dataset = open_exact_dataset(
        calendar_path,
        CALENDAR_PARTITIONING,
        CALENDAR_SCHEMA,
        "行情日历",
    )
    candidate_filter = (
        (ds.field("bar_frequency") == "1m")
        & (ds.field("is_fetch_required") == True)
        & (ds.field("schedule_status") == "suspected_closed")
    )
    if requested_start_date is not None:
        candidate_filter = (
            candidate_filter
            & (ds.field("trading_date") >= requested_start_date)
            & (ds.field("trading_date") <= requested_end_date)
        )
    if requested_contract_codes:
        candidate_filter = candidate_filter & ds.field(
            "contract_code"
        ).isin(list(requested_contract_codes))

    candidate_table = calendar_dataset.to_table(
        columns=CALENDAR_SCHEMA.names,
        filter=candidate_filter,
    )
    candidate_df = arrow_to_pandas(
        candidate_table,
        CALENDAR_SCHEMA,
    )
    if candidate_df.duplicated(CALENDAR_PRIMARY_KEY).any():
        raise ValueError("疑似休市候选主键不唯一。")

    if candidate_df.empty:
        click.echo(
            "selected_candidates=0; changed_candidates=0; "
            "committed_partitions=0"
        )
        return

    # 3. 有候选时再打开三张直接依赖表；全部要求精确 Schema/metadata。
    contract_dataset = open_exact_dataset(
        contract_path,
        CONTRACT_PARTITIONING,
        CONTRACT_SCHEMA,
        "合约 Session 日历",
    )
    daily_dataset = open_exact_dataset(
        daily_path,
        DAILY_PARTITIONING,
        DAILY_SCHEMA,
        "JQData 日线事实",
    )
    minute_dataset = open_exact_dataset(
        minute_path,
        MINUTE_PARTITIONING,
        MINUTE_SCHEMA,
        "JQData 分钟事实",
    )

    checked_at = datetime.now(timezone.utc)
    partition_frames: dict[
        tuple[object, ...], pd.DataFrame
    ] = {}
    changed_rows: list[pd.DataFrame] = []
    unchanged_evidence_count = 0

    calendar_partition_keys = sorted(
        set(
            candidate_df[
                CALENDAR_PARTITION_COLUMNS
            ].itertuples(index=False, name=None)
        )
    )

    # 4. 每个日历叶分区只读取对应合约月和候选品种月事实。
    for calendar_partition_key in calendar_partition_keys:
        calendar_partition_table = calendar_dataset.to_table(
            columns=CALENDAR_SCHEMA.names,
            filter=partition_expression(
                CALENDAR_PARTITION_COLUMNS,
                calendar_partition_key,
            ),
        )
        calendar_partition_df = validate_calendar_frame(
            arrow_to_pandas(
                calendar_partition_table,
                CALENDAR_SCHEMA,
            ),
            "校对前完整日历分区",
        )

        partition_candidate_mask = pd.Series(
            True,
            index=candidate_df.index,
        )
        for column, value in zip(
            CALENDAR_PARTITION_COLUMNS,
            calendar_partition_key,
            strict=True,
        ):
            partition_candidate_mask &= candidate_df[column].eq(value)
        partition_candidate_df = candidate_df.loc[
            partition_candidate_mask
        ]
        candidate_keys = set(
            partition_candidate_df[CALENDAR_PRIMARY_KEY].itertuples(
                index=False,
                name=None,
            )
        )

        _, exchange_code, year, month = calendar_partition_key
        contract_partition_key = (exchange_code, year, month)
        contract_table = contract_dataset.to_table(
            columns=CONTRACT_SCHEMA.names,
            filter=partition_expression(
                CONTRACT_PARTITION_COLUMNS,
                contract_partition_key,
            ),
        )
        contract_df = arrow_to_pandas(
            contract_table,
            CONTRACT_SCHEMA,
        )
        if contract_df.duplicated(CONTRACT_PRIMARY_KEY).any():
            raise ValueError("合约 Session 日历分区主键不唯一。")

        # 同一个交易所月可能出现多个候选品种，逐品种读取事实叶分区后合并。
        daily_tables: list[pa.Table] = []
        minute_tables: list[pa.Table] = []
        underlying_codes = sorted(
            set(partition_candidate_df["underlying_code"].tolist())
        )
        for underlying_code in underlying_codes:
            fact_partition_key = (
                exchange_code,
                underlying_code,
                year,
                month,
            )
            fact_filter = partition_expression(
                FACT_PARTITION_COLUMNS,
                fact_partition_key,
            )
            daily_tables.append(
                daily_dataset.to_table(
                    columns=DAILY_SCHEMA.names,
                    filter=fact_filter,
                )
            )
            minute_tables.append(
                minute_dataset.to_table(
                    columns=MINUTE_SCHEMA.names,
                    filter=fact_filter,
                )
            )

        daily_df = arrow_to_pandas(
            pa.concat_tables(daily_tables),
            DAILY_SCHEMA,
        )
        minute_df = arrow_to_pandas(
            pa.concat_tables(minute_tables),
            MINUTE_SCHEMA,
        )
        if daily_df.duplicated(DAILY_PRIMARY_KEY).any():
            raise ValueError("定向读取的日线事实主键不唯一。")
        if minute_df.duplicated(MINUTE_PRIMARY_KEY).any():
            raise ValueError("定向读取的分钟事实主键不唯一。")

        reconciled_df, changed_df, unchanged_count = (
            reconcile_partition(
                calendar_partition_df,
                candidate_keys,
                contract_df,
                daily_df,
                minute_df,
                checked_at,
            )
        )
        unchanged_evidence_count += unchanged_count
        if not changed_df.empty:
            partition_frames[calendar_partition_key] = reconciled_df
            changed_rows.append(changed_df)

    if changed_rows:
        changed_df = pd.concat(changed_rows, ignore_index=True)
    else:
        changed_df = arrow_to_pandas(
            pa.Table.from_batches([], schema=CALENDAR_SCHEMA),
            CALENDAR_SCHEMA,
        )

    passed_count = int(
        changed_df["quality_status"].eq("passed").sum()
    )
    warning_count = int(
        changed_df["quality_status"].eq("warning").sum()
    )
    click.echo(
        f"selected_candidates={len(candidate_df)}; "
        f"changed_candidates={len(changed_df)}; "
        f"unchanged_evidence={unchanged_evidence_count}; "
        f"passed={passed_count}; warning={warning_count}"
    )

    if not changed_df.empty:
        preview_columns = [
            "contract_code",
            "trading_date",
            "session_number",
            "evidence_level",
            "quality_status",
            "quality_reason",
        ]
        click.echo(
            changed_df.loc[:, preview_columns]
            .head(20)
            .to_string(index=False)
        )

    # 5. 不带 --write 到此结束；写入时提交的是完整叶分区，不是候选子集。
    if not write:
        click.echo("write=false; committed_partitions=0")
        return

    committed_rows = commit_calendar_partitions(
        partition_frames,
        resolved_lake_root,
    )
    click.echo(
        f"write=true; committed_partitions={len(partition_frames)}; "
        f"committed_calendar_rows={committed_rows}"
    )


if __name__ == "__main__":
    main()

