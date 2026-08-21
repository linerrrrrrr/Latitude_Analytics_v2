#!/usr/bin/env python
# coding: utf-8

# # c04_futures_bar_calendar
# 
# 目标表：dim_futures_bar_calendar。
# 
# 本表只从完整的 dim_futures_contract_calendar 建立全部 1d 合约日格点和 1m Session 格点，并保存后续事实提交、缺失检查与定向校对回写的状态。
# 
# 昂贵分钟事实白名单不属于 c04：新建或结构变化的 1m 格点先保持未选择状态，由 c06_futures_minute 统一评估并更新 is_fetch_required 与 selection_reason。c04 不导入白名单，也不会按白名单删除或重建格点。
# 
# Notebook 是唯一业务源；同名 Python 文件由默认 PythonExporter 生成。

# ## 环境、契约与分区
# 
# 正式湖根目录只来自 .env 的 FUTURES_LAKE_ROOT。显式日期只允许只读检查，或者配合 --write 写入解析后明确不同于正式湖的临时测试湖。

# In[ ]:


from __future__ import annotations

import pathlib
import shutil
import sys
import uuid
from datetime import date, datetime, timezone


# 从任意子目录启动时，先按项目统一标记定位根目录。
project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        project_root = candidate_root
        break
else:
    raise RuntimeError("未找到项目根目录")


import click
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

# Schema、类型转换和校验入口全部取自根级可执行契约。
from config.data_contracts import (
    FUTURES_BAR_CALENDAR_SCHEMA,
    FUTURES_CONTRACT_CALENDAR_SCHEMA,
    arrow_to_pandas,
    empty_pandas,
    pandas_to_arrow,
)
from config.settings import settings


# 常量集中描述本表、上游表、主键、分区和允许的状态枚举。
SCHEMA = FUTURES_BAR_CALENDAR_SCHEMA  # 行情拉取与质检日历的权威 Arrow Schema。
UPSTREAM_SCHEMA = FUTURES_CONTRACT_CALENDAR_SCHEMA  # 上游合约 Session 日历的权威 Schema。

TABLE_NAME = "dim_futures_bar_calendar"  # 期货行情拉取与质检日历维度表。
UPSTREAM_TABLE_NAME = "dim_futures_contract_calendar"  # 上游固定月份合约 Session 日历表。

PRIMARY_KEY = [  # 唯一标识一个日线格点或一分钟 Session 格点。
    "bar_frequency",  # 行情频率：1d 日线或 1m 一分钟线。
    "contract_code",  # JQData 标准固定月份合约代码。
    "trading_date",  # 行情归属的期货交易日。
    "session_number",  # 日线固定为 0；分钟为 Session 顺序号。
]
UPSTREAM_PRIMARY_KEY = [  # 唯一标识上游的一段合约 Session。
    "contract_code",  # 固定月份合约代码。
    "trading_date",  # Session 归属交易日。
    "session_number",  # 同一合约日内的 Session 顺序号。
]

PARTITION_COLUMNS = [  # 本表 Hive 叶分区；先按频率隔离日线与分钟线。
    "bar_frequency",  # 行情频率分区。
    "exchange_code",  # 交易所代码。
    "year",  # 交易年份。
    "month",  # 交易月份。
]
UPSTREAM_PARTITION_COLUMNS = [  # 上游合约日历的 Hive 叶分区。
    "exchange_code",  # 交易所代码。
    "year",  # 交易年份。
    "month",  # 交易月份。
]

STRUCTURAL_COLUMNS = [  # 由上游日历决定；结构变化时相应格点必须重建。
    *PRIMARY_KEY,  # 本表频率—合约—交易日—Session 主键。
    "exchange_code",  # 合约所属交易所。
    "underlying_code",  # 合约所属期货品种。
    "session_text",  # 上游原始 Session 文本；日线为空。
    "session_start_at",  # Session 开始时刻；日线为空。
    "session_end_at",  # Session 结束时刻；日线为空。
    "is_night_session",  # 是否夜盘 Session；日线为空。
    "expected_bar_count",  # 日线为 1；分钟为 Session 理论分钟数。
    "year",  # 交易年份分区值。
    "month",  # 交易月份分区值。
]
STATE_COLUMNS = [  # 由 c05～c08 回写且结构未变化时应保留的状态/证据字段。
    name
    for name in SCHEMA.names
    if name not in STRUCTURAL_COLUMNS
]

BAR_FREQUENCIES = {"1d", "1m"}
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
    pa.schema([SCHEMA.field(name) for name in PARTITION_COLUMNS]),
    flavor="hive",
)
UPSTREAM_PARTITIONING = ds.partitioning(
    pa.schema([UPSTREAM_SCHEMA.field(name) for name in UPSTREAM_PARTITION_COLUMNS]),
    flavor="hive",
)


# ## Schema 契约交互浏览

# In[ ]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from config.notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        FUTURES_CONTRACT_CALENDAR_SCHEMA,
        FUTURES_BAR_CALENDAR_SCHEMA,
    ])


# ## 上游边界与本表质量校验
# 
# c03 负责证明合约 Session 日历的完整业务语义。c04 精确检查上游 Schema/metadata、主键和自己直接依赖的 Session 边界；对自己的输出则执行完整表级校验。

# In[ ]:


# c04 信任 c03 已提交的业务语义，只校验本计算直接依赖的边界。
def validate_contract_input(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    table = pandas_to_arrow(frame.loc[:, UPSTREAM_SCHEMA.names], UPSTREAM_SCHEMA)
    checked_df = arrow_to_pandas(table, UPSTREAM_SCHEMA)

    if checked_df.duplicated(UPSTREAM_PRIMARY_KEY).any():
        raise ValueError(f"{context}主键不唯一。")
    if checked_df.empty:
        return checked_df

    # Session 时间、分钟数和代码派生列必须可直接用于生成行情格点。
    for row in table.to_pylist():
        if row["session_number"] <= 0:
            raise ValueError(f"{context}session_number 必须大于 0。")
        if row["session_start_at"] >= row["session_end_at"]:
            raise ValueError(f"{context}Session 起点必须严格早于终点。")

        duration_seconds = (
            row["session_end_at"] - row["session_start_at"]
        ).total_seconds()
        if duration_seconds % 60:
            raise ValueError(f"{context}Session 时间差不是整分钟。")
        if row["minute_count"] != int(duration_seconds // 60):
            raise ValueError(f"{context}minute_count 与 Session 时间差不一致。")
        if row["minute_count"] <= 0:
            raise ValueError(f"{context}minute_count 必须大于 0。")

        if not row["contract_code"].endswith(f".{row['exchange_code']}"):
            raise ValueError(f"{context}exchange_code 与合约代码后缀不一致。")
        if (
            row["year"] != row["trading_date"].year
            or row["month"] != row["trading_date"].month
        ):
            raise ValueError(f"{context}year/month 与 trading_date 不一致。")

    # 同一合约日内的 Session 编号必须连续，直接属性也必须一致。
    for group_key, group_df in checked_df.groupby(
        ["contract_code", "trading_date"],
        sort=False,
    ):
        session_numbers = sorted(
            int(value)
            for value in group_df["session_number"]
        )
        if session_numbers != list(range(1, len(session_numbers) + 1)):
            raise ValueError(f"{context}{group_key} 的 Session 编号不连续。")

        direct_columns = [
            "exchange_code",
            "underlying_code",
            "year",
            "month",
        ]
        if any(group_df[name].nunique(dropna=False) != 1 for name in direct_columns):
            raise ValueError(f"{context}{group_key} 的合约日属性不一致。")

    return checked_df.sort_values(UPSTREAM_PRIMARY_KEY).reset_index(drop=True)



# 对 c04 输出承担完整表级校验，包括结构、状态和质检计数关系。
def validate_bar_calendar_frame(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    table = pandas_to_arrow(frame.loc[:, SCHEMA.names], SCHEMA)
    checked_df = arrow_to_pandas(table, SCHEMA)

    if checked_df.duplicated(PRIMARY_KEY).any():
        raise ValueError(f"{context}主键不唯一。")
    if checked_df.empty:
        return checked_df

    rows = table.to_pylist()
    # 逐行检查频率、枚举、日线/分钟专属字段以及完成状态证据。
    for row in rows:
        frequency = row["bar_frequency"]
        if frequency not in BAR_FREQUENCIES:
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
            raise ValueError(f"{context}中文状态说明字段不得为空。")

        if not row["contract_code"].endswith(f".{row['exchange_code']}"):
            raise ValueError(f"{context}exchange_code 与合约代码后缀不一致。")
        if (
            row["year"] != row["trading_date"].year
            or row["month"] != row["trading_date"].month
        ):
            raise ValueError(f"{context}year/month 与 trading_date 不一致。")
        if row["expected_bar_count"] <= 0:
            raise ValueError(f"{context}expected_bar_count 必须大于 0。")
        if row["actual_bar_count"] < 0 or row["missing_bar_count"] < 0:
            raise ValueError(f"{context}实际与缺失条数不得为负。")

        # 日线格点没有 Session 时间；分钟格点必须保留完整 Session。
        if frequency == "1d":
            daily_session_values = [
                row["session_text"],
                row["session_start_at"],
                row["session_end_at"],
                row["is_night_session"],
            ]
            if row["session_number"] != 0:
                raise ValueError(f"{context}日线 session_number 必须为 0。")
            if any(value is not None for value in daily_session_values):
                raise ValueError(f"{context}日线 Session 专属字段必须为空。")
            if row["expected_bar_count"] != 1:
                raise ValueError(f"{context}日线 expected_bar_count 必须为 1。")
        else:
            minute_session_values = [
                row["session_text"],
                row["session_start_at"],
                row["session_end_at"],
                row["is_night_session"],
            ]
            if row["session_number"] <= 0:
                raise ValueError(f"{context}分钟 session_number 必须大于 0。")
            if any(value is None for value in minute_session_values):
                raise ValueError(f"{context}分钟 Session 字段不得为空。")
            if row["session_start_at"] >= row["session_end_at"]:
                raise ValueError(f"{context}分钟 Session 起点必须早于终点。")

            duration_seconds = (
                row["session_end_at"] - row["session_start_at"]
            ).total_seconds()
            if duration_seconds % 60:
                raise ValueError(f"{context}分钟 Session 时间差不是整分钟。")
            if row["expected_bar_count"] != int(duration_seconds // 60):
                raise ValueError(f"{context}分钟理论条数与 Session 不一致。")

        # 只有权威证据可以确认休市并免除事实拉取。
        if row["schedule_status"] == "confirmed_closed":
            if row["evidence_level"] != "authoritative":
                raise ValueError(f"{context}确认休市必须具有权威证据。")
            if row["is_fetch_required"]:
                raise ValueError(f"{context}确认休市格点不得继续要求拉取。")

        if row["is_fetch_completed"]:
            if not row["fetch_run_id"] or row["fetch_completed_at"] is None:
                raise ValueError(f"{context}完成状态缺少运行批次或完成时间。")
        elif row["fetch_completed_at"] is not None:
            raise ValueError(f"{context}未完成格点不得具有完成时间。")

        if row["missing_checked_at"] is None:
            if row["is_data_missing"] or row["missing_bar_count"] != 0:
                raise ValueError(f"{context}未经缺失检查不得记录缺失。")
        else:
            expected_missing = (
                max(row["expected_bar_count"] - row["actual_bar_count"], 0)
                if row["is_fetch_required"]
                else 0
            )
            if row["missing_bar_count"] != expected_missing:
                raise ValueError(f"{context}missing_bar_count 无法由条数复算。")
            if row["is_data_missing"] != (expected_missing > 0):
                raise ValueError(f"{context}is_data_missing 与缺失条数不一致。")

        if (
            row["quality_status"] != "pending"
            and row["quality_checked_at"] is None
        ):
            raise ValueError(f"{context}非 pending 质量状态缺少检查时间。")

    # 同一合约日的分钟 Session 编号仍须保持连续。
    minute_df = checked_df.loc[
        checked_df["bar_frequency"].eq("1m")
    ]
    for group_key, group_df in minute_df.groupby(
        ["contract_code", "trading_date"],
        sort=False,
    ):
        session_numbers = sorted(
            int(value)
            for value in group_df["session_number"]
        )
        if session_numbers != list(range(1, len(session_numbers) + 1)):
            raise ValueError(f"{context}{group_key} 的分钟 Session 编号不连续。")

    return checked_df.sort_values(PRIMARY_KEY).reset_index(drop=True)


# 将契约行转成主键字典，供分区差异比较使用。
def rows_by_key(
    frame: pd.DataFrame,
    columns: list[str],
) -> dict[tuple[object, ...], tuple[object, ...]]:
    table = pandas_to_arrow(frame.loc[:, SCHEMA.names], SCHEMA)
    result = {}

    for row in table.to_pylist():
        key = tuple(row[name] for name in PRIMARY_KEY)
        result[key] = tuple(row[name] for name in columns)

    return result


# ## 契约化分区读取
# 
# 自动模式逐交易所—年月读取上游，不把完整历史一次性载入内存。目标表只允许 metadata 旧而字段物理兼容的情况进入自动重写；字段、类型或 nullable 不兼容时立即失败。

# In[ ]:


# Dataset 会混入 Hive 分区字段；按契约顺序重建完整 Schema。
def reconstructed_schema(
    dataset: ds.Dataset,
    schema: pa.Schema,
) -> pa.Schema:
    try:
        fields = [dataset.schema.field(name) for name in schema.names]
    except KeyError as error:
        raise TypeError("数据集缺少契约字段。") from error

    return pa.schema(fields, metadata=dataset.schema.metadata)


# metadata 升级时仍要求字段名、类型和可空性完全兼容。
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


# 统一打开上游或下游 Dataset，并显式处理空目录与 metadata 升级。
def open_contract_dataset(
    table_path: pathlib.Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    label: str,
    *,
    required: bool,
    allow_metadata_upgrade: bool = False,
) -> tuple[ds.Dataset | None, str | None]:
    parquet_files = list(table_path.rglob("*.parquet")) if table_path.is_dir() else []
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

    if actual_schema.equals(schema, check_metadata=True):
        return dataset, None
    if allow_metadata_upgrade and physically_compatible(actual_schema, schema):
        return dataset, "现有数据集 metadata 与当前契约不一致，需要重写触达分区。"

    raise TypeError(f"{label} Schema/metadata 与契约不一致。")


def cast_partition_value(column: str, raw_value: str) -> object:
    if column in {"year", "month"}:
        return int(raw_value)
    return raw_value


# 从实际 Parquet 文件路径发现完整 Hive 分区键，不能只看目录名。
def partition_keys_from_files(
    table_path: pathlib.Path,
    partition_columns: list[str],
) -> set[tuple[object, ...]]:
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

            raw_value = directory_name[len(prefix):]
            values.append(cast_partition_value(column, raw_value))

        keys.add(tuple(values))

    return keys


def partition_filter(
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
        expression = condition if expression is None else expression & condition

    if expression is None:
        raise ValueError("分区键不得为空。")
    return expression


# 按完整分区键读取；显式日期只在读取后进一步裁剪行。
def read_partition(
    dataset: ds.Dataset,
    schema: pa.Schema,
    partition_columns: list[str],
    partition_key: tuple[object, ...],
    start_date: date | None = None,
    end_date: date | None = None,
) -> pd.DataFrame:
    expression = partition_filter(partition_columns, partition_key)

    if start_date is not None:
        expression &= ds.field("trading_date") >= start_date
        expression &= ds.field("trading_date") <= end_date

    table = dataset.to_table(
        columns=schema.names,
        filter=expression,
    )
    return arrow_to_pandas(table, schema)


# ## 构造完整理论格点
# 
# 日线按合约日去重并使用保留编号 0；分钟逐 Session 原样保留。
# 
# c04 对新的 1m 格点只写入安全的“等待 c06 评估”初始状态。白名单选择以及由此产生的 is_fetch_required、selection_reason 变化由 c06 负责。

# In[ ]:


# 新格点只初始化调度状态，不冒充事实已经完成。
def initial_state(
    frequency: str,
    updated_at: datetime,
) -> dict[str, object]:
    # 日线覆盖全部固定月份合约；分钟等待 c06 应用共享白名单。
    if frequency == "1d":
        is_fetch_required = True
        selection_reason = (
            "完整固定月份合约日线格点；等待 c05_futures_daily 采集。"
        )
        quality_reason = "等待日线事实提交与复读质检。"
    else:
        is_fetch_required = False
        selection_reason = (
            "分钟事实采集范围由 c06_futures_minute 维护；"
            "当前新建或结构变化格点等待其评估。"
        )
        quality_reason = "等待分钟采集政策评估及后续事实质检。"

    return {
        "schedule_status": "scheduled",
        "schedule_signal_reason": (
            "上游合约 Session 规则有效，结构阶段按计划开市初始化。"
        ),
        "evidence_level": "contract_rule",
        "evidence_source": "dim_futures_contract_calendar",
        "is_fetch_required": is_fetch_required,
        "selection_reason": selection_reason,
        "is_fetch_completed": False,
        "actual_bar_count": 0,
        "is_data_missing": False,
        "missing_bar_count": 0,
        "fetch_run_id": None,
        "fetch_completed_at": None,
        "missing_checked_at": None,
        "quality_status": "pending",
        "quality_reason": quality_reason,
        "daily_open": None,
        "daily_high": None,
        "daily_low": None,
        "daily_close": None,
        "daily_volume": None,
        "daily_money": None,
        "daily_open_interest": None,
        "aggregated_open": None,
        "aggregated_high": None,
        "aggregated_low": None,
        "aggregated_close": None,
        "aggregated_volume": None,
        "aggregated_money": None,
        "aggregated_open_interest": None,
        "ohlc_matches_daily": None,
        "volume_matches_daily": None,
        "money_matches_daily": None,
        "open_interest_matches_daily": None,
        "quality_checked_at": None,
        "updated_at": updated_at,
    }


# 从同一份合约 Session 上游同时生成完整 1d 与 1m 理论格点。
def build_fresh_partitions(
    contract_frame: pd.DataFrame,
    updated_at: datetime,
) -> dict[str, pd.DataFrame]:
    contracts_df = validate_contract_input(
        contract_frame,
        "上游合约日历分区",
    )

    minute_rows = []
    # 分钟表逐 Session 保留；理论条数直接继承 c03 的 minute_count。
    for row in pandas_to_arrow(
        contracts_df.loc[:, UPSTREAM_SCHEMA.names],
        UPSTREAM_SCHEMA,
    ).to_pylist():
        minute_row = {
            "bar_frequency": "1m",
            "contract_code": row["contract_code"],
            "exchange_code": row["exchange_code"],
            "underlying_code": row["underlying_code"],
            "trading_date": row["trading_date"],
            "session_number": row["session_number"],
            "session_text": row["session_text"],
            "session_start_at": row["session_start_at"],
            "session_end_at": row["session_end_at"],
            "is_night_session": row["is_night_session"],
            "expected_bar_count": row["minute_count"],
            "year": row["year"],
            "month": row["month"],
        }
        minute_row.update(initial_state("1m", updated_at))
        minute_rows.append(minute_row)

    minute_df = pd.DataFrame(minute_rows, columns=SCHEMA.names)
    if minute_df.empty:
        minute_df = empty_pandas(SCHEMA)
    minute_df = validate_bar_calendar_frame(
        minute_df,
        "新生成分钟分区",
    )

    # 日线按合约—交易日折叠为一行，不保留任一 Session 编号。
    daily_rows = []

    daily_source_df = contracts_df.drop_duplicates(
        ["contract_code", "trading_date"],
        keep="first",
    )
    for row in pandas_to_arrow(
        daily_source_df.loc[:, UPSTREAM_SCHEMA.names],
        UPSTREAM_SCHEMA,
    ).to_pylist():
        daily_row = {
            "bar_frequency": "1d",
            "contract_code": row["contract_code"],
            "exchange_code": row["exchange_code"],
            "underlying_code": row["underlying_code"],
            "trading_date": row["trading_date"],
            "session_number": 0,
            "session_text": None,
            "session_start_at": None,
            "session_end_at": None,
            "is_night_session": None,
            "expected_bar_count": 1,
            "year": row["year"],
            "month": row["month"],
        }
        daily_row.update(initial_state("1d", updated_at))
        daily_rows.append(daily_row)

    daily_df = pd.DataFrame(daily_rows, columns=SCHEMA.names)
    if daily_df.empty:
        daily_df = empty_pandas(SCHEMA)
    daily_df = validate_bar_calendar_frame(
        daily_df,
        "新生成日线分区",
    )

    return {
        "1d": daily_df,
        "1m": minute_df,
    }


# ## 自动差集与状态继承
# 
# 上游结构未变化时，完整保留 c05—c08 已回写的调度、选择、完成和质检证据；新增、删除或结构变化时只重置受影响格点。比较不会因为 updated_at 自身发生伪变化。

# In[ ]:


# 结构未变化时完整继承下游事实与质检回写状态。
def merge_preserving_state(
    fresh_frame: pd.DataFrame,
    existing_frame: pd.DataFrame,
) -> pd.DataFrame:
    fresh_df = validate_bar_calendar_frame(fresh_frame, "当前上游结果")
    existing_df = validate_bar_calendar_frame(existing_frame, "现有下游分区")

    existing_rows = {
        tuple(row[name] for name in PRIMARY_KEY): row
        for row in pandas_to_arrow(existing_df, SCHEMA).to_pylist()
    }

    merged_rows = []
    # 新增或结构变化的格点保留安全初始状态，旧状态不得错位继承。
    for fresh_row in pandas_to_arrow(fresh_df, SCHEMA).to_pylist():
        key = tuple(fresh_row[name] for name in PRIMARY_KEY)
        existing_row = existing_rows.get(key)

        if existing_row is not None and all(
            fresh_row[name] == existing_row[name]
            for name in STRUCTURAL_COLUMNS
        ):
            for name in STATE_COLUMNS:
                fresh_row[name] = existing_row[name]

        merged_rows.append(fresh_row)

    merged_df = pd.DataFrame(merged_rows, columns=SCHEMA.names)
    if merged_df.empty:
        merged_df = empty_pandas(SCHEMA)

    return validate_bar_calendar_frame(
        merged_df,
        "状态继承后的目标分区",
    )


# 比较期望分区与现有分区，汇总新增、删除、修订和质量错误。
def assess_partition(
    fresh_frame: pd.DataFrame,
    existing_frame: pd.DataFrame,
    forced_quality_error: str | None,
) -> tuple[pd.DataFrame, dict[str, object]]:
    business_quality_error = None

    try:
        checked_existing_df = validate_bar_calendar_frame(
            existing_frame,
            "现有下游分区",
        )
    except (TypeError, ValueError) as error:
        checked_existing_df = empty_pandas(SCHEMA)
        business_quality_error = f"{type(error).__name__}: {error}"

    quality_messages = [
        message
        for message in [forced_quality_error, business_quality_error]
        if message is not None
    ]
    existing_quality_error = "；".join(quality_messages) or None

    if business_quality_error is None:
        desired_df = merge_preserving_state(
            fresh_frame,
            checked_existing_df,
        )
    else:
        desired_df = validate_bar_calendar_frame(
            fresh_frame,
            "质量失败后的重建分区",
        )

    desired_rows = rows_by_key(desired_df, SCHEMA.names)
    existing_rows = rows_by_key(checked_existing_df, SCHEMA.names)

    desired_keys = set(desired_rows)
    existing_keys = set(existing_rows)

    missing_keys = desired_keys - existing_keys
    extra_keys = existing_keys - desired_keys
    changed_keys = {
        key
        for key in desired_keys & existing_keys
        if desired_rows[key] != existing_rows[key]
    }

    if existing_quality_error is not None:
        changed_keys = desired_keys & existing_keys
        extra_count = (
            len(existing_frame)
            if business_quality_error is not None
            else len(extra_keys)
        )
        complete_count = 0
    else:
        extra_count = len(extra_keys)
        complete_count = len(desired_df) - len(missing_keys) - len(changed_keys)

    audit = {
        "expected_row_count": len(desired_df),
        "complete_count": complete_count,
        "missing_count": len(missing_keys),
        "changed_count": len(changed_keys),
        "extra_count": extra_count,
        "quality_error": existing_quality_error,
    }
    audit["is_dirty"] = bool(
        audit["missing_count"]
        or audit["changed_count"]
        or audit["extra_count"]
        or audit["quality_error"]
    )

    return desired_df, audit


# ## 单分区暂存、复读、替换与回滚
# 
# 自动模式提交完整叶分区。显式日期只用于非正式测试湖，提交前会保留同一叶分区中日期范围外的旧行，避免定向验证删除同月历史。
# 
# 如果只是 metadata 版本升级，允许同一运行中逐分区迁移；每个新叶分区都必须使用当前精确 Schema/metadata，全部计划完成后再执行全表文件级契约检查。

# In[ ]:


def parquet_file_schema() -> pa.Schema:
    return pa.schema(
        [
            SCHEMA.field(name)
            for name in SCHEMA.names
            if name not in PARTITION_COLUMNS
        ],
        metadata=SCHEMA.metadata,
    )


# 每个物理 Parquet 文件都必须带有去除 Hive 分区列后的精确文件 Schema。
def validate_output_parquet_files(table_path: pathlib.Path) -> None:
    expected_schema = parquet_file_schema()
    parquet_files = list(table_path.rglob("*.parquet"))

    if not parquet_files:
        raise FileNotFoundError(f"数据集没有 Parquet 文件：{table_path}")

    for parquet_path in parquet_files:
        actual_schema = pq.read_schema(parquet_path)
        if not actual_schema.equals(expected_schema, check_metadata=True):
            raise TypeError(
                f"Parquet 文件 Schema/metadata 与契约不一致：{parquet_path}"
            )


def partition_relative_path(
    partition_key: tuple[object, ...],
) -> pathlib.Path:
    return pathlib.Path(
        *[
            f"{name}={value}"
            for name, value in zip(
                PARTITION_COLUMNS,
                partition_key,
                strict=True,
            )
        ]
    )


def commit_partition(
    frame: pd.DataFrame,
    lake_root: pathlib.Path,
    partition_key: tuple[object, ...],
    replace_start_date: date | None = None,
    replace_end_date: date | None = None,
) -> int:
    if (replace_start_date is None) != (replace_end_date is None):
        raise ValueError("替换起止日期必须同时提供。")
    if replace_start_date is not None and replace_start_date > replace_end_date:
        raise ValueError("替换起始日期不得晚于结束日期。")

    # 提交输入必须完全属于目标频率—交易所—年月分区。
    incoming_df = validate_bar_calendar_frame(frame, "待提交分区")
    if not incoming_df.empty:
        incoming_keys = set(
            incoming_df[PARTITION_COLUMNS].itertuples(
                index=False,
                name=None,
            )
        )
        if incoming_keys != {partition_key}:
            raise ValueError("待提交数据越出指定 Hive 分区。")


        if replace_start_date is not None:
            outside_range = incoming_df["trading_date"].map(
                lambda value: (
                    value < replace_start_date
                    or value > replace_end_date
                )
            )
            if outside_range.any():
                raise ValueError("待提交数据越出显式替换日期范围。")

    silver_root = lake_root.resolve() / "silver"
    target_path = silver_root / TABLE_NAME

    run_id = uuid.uuid4().hex
    staging_path = silver_root / f".{TABLE_NAME}.staging-{run_id}"
    backup_path = silver_root / f".{TABLE_NAME}.backup-{run_id}"
    quarantine_path = silver_root / f".{TABLE_NAME}.failed-{run_id}"

    silver_root.mkdir(parents=True, exist_ok=True)
    for managed_path in (
        target_path,
        staging_path,
        backup_path,
        quarantine_path,
    ):
        if not managed_path.resolve().is_relative_to(silver_root):
            raise ValueError(f"数据集路径越出 silver 根目录：{managed_path}")

    target_dataset, _ = open_contract_dataset(
        target_path,
        HIVE_PARTITIONING,
        SCHEMA,
        "现有行情日历",
        required=False,
        allow_metadata_upgrade=True,
    )

    existing_df = empty_pandas(SCHEMA)
    # 自动模式替换完整分区；显式非正式模式只替换指定日期范围。
    if target_dataset is not None:
        existing_df = read_partition(
            target_dataset,
            SCHEMA,
            PARTITION_COLUMNS,
            partition_key,
        )
        existing_df = validate_bar_calendar_frame(
            existing_df,
            "提交前现有完整分区",
        )

    if replace_start_date is None:
        complete_partition_df = incoming_df
    else:
        retained_df = existing_df.loc[
            (existing_df["trading_date"] < replace_start_date)
            | (existing_df["trading_date"] > replace_end_date),
            SCHEMA.names,
        ]
        complete_partition_df = pd.concat(
            [retained_df, incoming_df],
            ignore_index=True,
        )

    complete_partition_df = validate_bar_calendar_frame(
        complete_partition_df,
        "合并后完整分区",
    )
    complete_partition_table = pandas_to_arrow(
        complete_partition_df,
        SCHEMA,
    )

    relative_path = partition_relative_path(partition_key)
    staging_path.mkdir(parents=True, exist_ok=False)

    # 先写 staging 并逐文件、逐表复读校验，再接触正式目标。
    try:
        if len(complete_partition_table):
            ds.write_dataset(
                complete_partition_table,
                staging_path,
                format="parquet",
                partitioning=HIVE_PARTITIONING,
                existing_data_behavior="delete_matching",
                basename_template="part-{i}.parquet",
            )

            staged_dataset = ds.dataset(
                staging_path,
                format="parquet",
                partitioning=HIVE_PARTITIONING,
            )
            staged_schema = reconstructed_schema(staged_dataset, SCHEMA)
            if not staged_schema.equals(SCHEMA, check_metadata=True):
                raise TypeError("staging Schema/metadata 与契约不一致。")

            staged_df = read_partition(
                staged_dataset,
                SCHEMA,
                PARTITION_COLUMNS,
                partition_key,
            )
            staged_df = validate_bar_calendar_frame(
                staged_df,
                "staging 完整分区",
            )
            if not pandas_to_arrow(staged_df, SCHEMA).equals(
                complete_partition_table
            ):
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
    # 正式替换使用备份和隔离目录，异常时恢复原分区。
    commit_succeeded = False

    backup_path.mkdir(parents=True, exist_ok=False)
    target_path.mkdir(parents=True, exist_ok=True)
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    saved_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        if destination_path.exists():
            shutil.move(str(destination_path), str(saved_path))

        if source_path.is_dir():
            shutil.move(str(source_path), str(destination_path))
        elif len(complete_partition_table):
            raise FileNotFoundError(f"staging 缺少 {relative_path}。")

        expected_file_schema = parquet_file_schema()
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
            validate_output_parquet_files(destination_path)

        committed_dataset = ds.dataset(
            target_path,
            format="parquet",
            partitioning=HIVE_PARTITIONING,
        )
        committed_schema = reconstructed_schema(committed_dataset, SCHEMA)
        if not physically_compatible(committed_schema, SCHEMA):
            raise TypeError("正式数据集物理 Schema 与契约不兼容。")

        committed_df = read_partition(
            committed_dataset,
            SCHEMA,
            PARTITION_COLUMNS,
            partition_key,
        )
        committed_df = validate_bar_calendar_frame(
            committed_df,
            "正式复读完整分区",
        )
        if not pandas_to_arrow(committed_df, SCHEMA).equals(
            complete_partition_table
        ):
            raise ValueError("正式复读分区内容与 staging 不一致。")

        commit_succeeded = True
    except Exception as commit_error:
        if marker_created and marker_path.exists():
            marker_path.unlink()
        if marker_replaced:
            if marker_path.exists():
                marker_path.unlink()
            if saved_marker_path.exists():
                shutil.move(str(saved_marker_path), str(marker_path))

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
                f"分区提交失败；旧分区已恢复，"
                f"新分区隔离在 {quarantine_path}。"
            ) from commit_error
        raise
    finally:
        shutil.rmtree(staging_path, ignore_errors=True)

        backup_has_data = (
            backup_path.exists()
            and any(backup_path.rglob("*.parquet"))
        )
        if commit_succeeded or not backup_has_data:
            shutil.rmtree(backup_path, ignore_errors=True)

        if quarantine_path.exists() and not any(quarantine_path.rglob("*")):
            shutil.rmtree(quarantine_path, ignore_errors=True)

    return len(incoming_df)


# ## 命令入口与自动计划
# 
# 无日期模式逐月比较完整上游格点与下游完整分区；空湖自然得到全量计划。无 --write 时只读取、构造、比较和校验，不创建 staging、不修改日历状态。

# In[ ]:


def month_is_relevant(
    partition_key: tuple[str, int, int],
    start_date: date,
    end_date: date,
) -> bool:
    _, year, month = partition_key
    return (
        (start_date.year, start_date.month)
        <= (year, month)
        <= (end_date.year, end_date.month)
    )


def read_upstream_for_base_key(
    upstream_dataset: ds.Dataset,
    upstream_partition_keys: set[tuple[object, ...]],
    base_key: tuple[str, int, int],
    start_date: date | None,
    end_date: date | None,
) -> pd.DataFrame:
    if base_key not in upstream_partition_keys:
        return empty_pandas(UPSTREAM_SCHEMA)

    upstream_df = read_partition(
        upstream_dataset,
        UPSTREAM_SCHEMA,
        UPSTREAM_PARTITION_COLUMNS,
        base_key,
        start_date,
        end_date,
    )
    return validate_contract_input(
        upstream_df,
        f"上游分区 {base_key} ",
    )


def read_existing_for_partition(
    target_dataset: ds.Dataset | None,
    target_partition_keys: set[tuple[object, ...]],
    partition_key: tuple[object, ...],
    start_date: date | None,
    end_date: date | None,
) -> pd.DataFrame:
    if target_dataset is None or partition_key not in target_partition_keys:
        return empty_pandas(SCHEMA)

    return read_partition(
        target_dataset,
        SCHEMA,
        PARTITION_COLUMNS,
        partition_key,
        start_date,
        end_date,
    )


@click.command()
@click.option("--lake-root", type=click.Path(path_type=pathlib.Path))
@click.option("--start-date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--end-date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--write", is_flag=True)
def main(
    lake_root: pathlib.Path | None,
    start_date: datetime | None,
    end_date: datetime | None,
    write: bool,
) -> None:
    # 首先解析湖路径和显式日期，并执行正式写入门禁。
    formal_lake_root = settings.futures_lake_root
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

    # 打开 c03 上游和现有 c04 下游，只接受契约兼容的数据集。
    silver_root = resolved_lake_root / "silver"
    upstream_path = silver_root / UPSTREAM_TABLE_NAME
    target_path = silver_root / TABLE_NAME

    upstream_dataset, _ = open_contract_dataset(
        upstream_path,
        UPSTREAM_PARTITIONING,
        UPSTREAM_SCHEMA,
        "上游合约 Session 日历",
        required=True,
    )
    target_dataset, target_contract_error = open_contract_dataset(
        target_path,
        HIVE_PARTITIONING,
        SCHEMA,
        "现有行情日历",
        required=False,
        allow_metadata_upgrade=True,
    )

    upstream_partition_keys = partition_keys_from_files(
        upstream_path,
        UPSTREAM_PARTITION_COLUMNS,
    )
    target_partition_keys = partition_keys_from_files(
        target_path,
        PARTITION_COLUMNS,
    )

    invalid_frequencies = {
        key[0]
        for key in target_partition_keys
        if key[0] not in BAR_FREQUENCIES
    }
    if invalid_frequencies:
        raise ValueError(
            f"现有行情日历包含非法频率分区：{sorted(invalid_frequencies)}"
        )

    upstream_base_keys = set(upstream_partition_keys)
    target_base_keys = {
        (exchange_code, year, month)
        for _, exchange_code, year, month in target_partition_keys
    }

    if has_explicit_dates:
        upstream_base_keys = {
            key
            for key in upstream_base_keys
            if month_is_relevant(
                key,
                requested_start_date,
                requested_end_date,
            )
        }
        target_base_keys = {
            key
            for key in target_base_keys
            if month_is_relevant(
                key,
                requested_start_date,
                requested_end_date,
            )
        }

    base_keys = sorted(upstream_base_keys | target_base_keys)
    run_updated_at = datetime.now(timezone.utc)

    dirty_partition_plans = []
    upstream_grid_count = 0
    complete_grid_count = 0
    missing_grid_count = 0
    changed_grid_count = 0
    extra_grid_count = 0

    # 按交易所—年月读取一次上游，再分别规划 1d 和 1m 分区。
    for base_number, base_key in enumerate(base_keys, start=1):
        if base_number == 1 or base_number % 25 == 0:
            click.echo(
                "planning_progress: "
                f"table={TABLE_NAME}; "
                f"base_partitions={base_number}/{len(base_keys)}; "
                f"key={base_key}"
            )
        upstream_df = read_upstream_for_base_key(
            upstream_dataset,

            upstream_partition_keys,
            base_key,
            requested_start_date,
            requested_end_date,
        )
        fresh_by_frequency = build_fresh_partitions(
            upstream_df,
            run_updated_at,
        )

        for frequency in sorted(BAR_FREQUENCIES):
            partition_key = (frequency, *base_key)
            existing_df = read_existing_for_partition(
                target_dataset,
                target_partition_keys,
                partition_key,
                requested_start_date,
                requested_end_date,
            )
            _, audit = assess_partition(
                fresh_by_frequency[frequency],
                existing_df,
                target_contract_error,
            )

            upstream_grid_count += int(audit["expected_row_count"])
            complete_grid_count += int(audit["complete_count"])
            missing_grid_count += int(audit["missing_count"])
            changed_grid_count += int(audit["changed_count"])
            extra_grid_count += int(audit["extra_count"])

            # 只有业务格点或质量状态真正变化时才加入写入计划。
            if audit["is_dirty"]:
                dirty_partition_plans.append(
                    {
                        "partition_key": partition_key,
                        **audit,
                    }
                )

    mode = "explicit" if has_explicit_dates else "automatic"
    plan_name = "explicit_plan" if has_explicit_dates else "auto_plan"

    click.echo(
        f"{plan_name}: table={TABLE_NAME}; "
        f"upstream_grid_count={upstream_grid_count}; "
        f"complete_grid_count={complete_grid_count}; "
        f"missing_grid_count={missing_grid_count}; "
        f"changed_grid_count={changed_grid_count}; "
        f"extra_grid_count={extra_grid_count}; "
        f"touched_partition_count={len(dirty_partition_plans)}"
    )

    for plan in dirty_partition_plans:
        click.echo(
            "partition_plan: "
            f"partition={plan['partition_key']}; "
            f"expected_rows={plan['expected_row_count']}; "
            f"missing={plan['missing_count']}; "
            f"changed={plan['changed_count']}; "
            f"extra={plan['extra_count']}; "
            f"quality_error={plan['quality_error']}"
        )

    if not dirty_partition_plans:
        click.echo(f"up_to_date: table={TABLE_NAME}; mode={mode}")
        return
    # 不带 --write 时输出完整计划，但不创建 staging 或修改状态。
    if not write:
        return

    planned_partition_keys = {
        plan["partition_key"]
        for plan in dirty_partition_plans
    }
    planned_base_keys = sorted(
        {
            (exchange_code, year, month)
            for _, exchange_code, year, month in planned_partition_keys
        }
    )

    committed_row_count = 0
    # 同一基础分区重新读取上游并构建两种频率，逐分区原子提交。
    for base_key in planned_base_keys:
        upstream_df = read_upstream_for_base_key(
            upstream_dataset,
            upstream_partition_keys,
            base_key,
            requested_start_date,
            requested_end_date,
        )
        fresh_by_frequency = build_fresh_partitions(
            upstream_df,
            run_updated_at,
        )

        for frequency in sorted(BAR_FREQUENCIES):
            partition_key = (frequency, *base_key)
            if partition_key not in planned_partition_keys:
                continue

            existing_df = read_existing_for_partition(
                target_dataset,
                target_partition_keys,
                partition_key,
                requested_start_date,
                requested_end_date,
            )
            desired_df, _ = assess_partition(
                fresh_by_frequency[frequency],
                existing_df,
                target_contract_error,
            )
            committed_row_count += commit_partition(
                desired_df,
                resolved_lake_root,
                partition_key,
                requested_start_date,
                requested_end_date,
            )

    if target_contract_error is not None:
        validate_output_parquet_files(target_path)

    open_contract_dataset(
        target_path,
        HIVE_PARTITIONING,
        SCHEMA,
        "提交后的行情日历",
        required=True,
    )

    commit_mode = (
        "explicit_non_formal"
        if has_explicit_dates
        else "automatic_gap_fill"
    )
    click.echo(
        f"committed: mode={commit_mode}; "
        f"rows={committed_row_count}; "
        f"partitions={len(dirty_partition_plans)}; "
        "remaining_pending=0"
    )


if __name__ == "__main__":
    main()

