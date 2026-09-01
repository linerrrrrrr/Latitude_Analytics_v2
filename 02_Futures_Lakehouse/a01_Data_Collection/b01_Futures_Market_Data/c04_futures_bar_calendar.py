#!/usr/bin/env python
# coding: utf-8

# # c04_futures_bar_calendar
# 
# 目标表：dim_futures_bar_calendar。
# 
# 本表只从完整的 dim_futures_contract_calendar 建立全部 1d 合约日格点和 1m Session 格点，并保存后续事实提交、缺失检查与定向校对回写的状态。
# 
# 默认只比较 c03 尾部新增及其当前目标叶；--full 才执行全历史结构维护。两种模式都只投影 c03 包含 3 个上游主键在内的 12 列，向量生成并精确比较 c04 包含 4 个本表主键在内的 13 个结构字段；clean 叶信任 c05—c08 的正式状态提交证明，只有 dirty 叶才复读完整状态并执行完整校验。
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
import time
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


# 表名、主键和 Hive 分区只从权威 Schema metadata 读取。
TABLE_NAME = FUTURES_BAR_CALENDAR_SCHEMA.metadata[b"table_name"].decode("utf-8")
UPSTREAM_TABLE_NAME = FUTURES_CONTRACT_CALENDAR_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")
PRIMARY_KEY = FUTURES_BAR_CALENDAR_SCHEMA.metadata[b"primary_key"].decode(
    "utf-8"
).split(",")
UPSTREAM_PRIMARY_KEY = FUTURES_CONTRACT_CALENDAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")
PARTITION_COLUMNS = FUTURES_BAR_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")
UPSTREAM_PARTITION_COLUMNS = FUTURES_CONTRACT_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")

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
    for name in FUTURES_BAR_CALENDAR_SCHEMA.names
    if name not in STRUCTURAL_COLUMNS
]
STRUCTURAL_NON_KEY_COLUMNS = [
    name for name in STRUCTURAL_COLUMNS if name not in PRIMARY_KEY
]
UPSTREAM_STRUCTURE_COLUMNS = [
    *UPSTREAM_PRIMARY_KEY,
    "exchange_code",
    "underlying_code",
    "session_text",
    "session_start_at",
    "session_end_at",
    "is_night_session",
    "minute_count",
    "year",
    "month",
]
STRUCTURAL_SCHEMA = pa.schema([
    FUTURES_BAR_CALENDAR_SCHEMA.field(name) for name in STRUCTURAL_COLUMNS
])
UPSTREAM_STRUCTURE_SCHEMA = pa.schema([
    FUTURES_CONTRACT_CALENDAR_SCHEMA.field(name)
    for name in UPSTREAM_STRUCTURE_COLUMNS
])

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
    pa.schema([
        FUTURES_BAR_CALENDAR_SCHEMA.field(name)
        for name in PARTITION_COLUMNS
    ]),
    flavor="hive",
)
UPSTREAM_PARTITIONING = ds.partitioning(
    pa.schema([
        FUTURES_CONTRACT_CALENDAR_SCHEMA.field(name)
        for name in UPSTREAM_PARTITION_COLUMNS
    ]),
    flavor="hive",
)


# ## Schema 契约呈现

# In[ ]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from config.notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        FUTURES_CONTRACT_CALENDAR_SCHEMA,
        FUTURES_BAR_CALENDAR_SCHEMA,
    ])


# ## 上游边界与本表质量校验
# 
# c03 正式提交负责证明合约 Session 日历的主键、Session 连续性和派生字段等完整业务语义。c04 启动时只确认上游 Dataset 的物理结构和稳定表身份兼容，并读取构造所需投影；c04 对每个 dirty 完整目标叶在 staging 前执行一次全部表级校验。

# In[ ]:


# 兼容公开 helper 和迁移工具的任意 DataFrame 输入；正式 c03 Dataset 路径不重复调用。
def validate_contract_structure(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    table = pandas_to_arrow(
        frame.loc[:, UPSTREAM_STRUCTURE_COLUMNS],
        UPSTREAM_STRUCTURE_SCHEMA,
    )
    checked_df = arrow_to_pandas(table, UPSTREAM_STRUCTURE_SCHEMA)
    checked_df = checked_df.sort_values(UPSTREAM_PRIMARY_KEY).reset_index(drop=True)

    if checked_df.duplicated(UPSTREAM_PRIMARY_KEY).any():
        raise ValueError(f"{context}主键不唯一。")
    if checked_df.empty:
        return checked_df

    duration_seconds = (
        checked_df["session_end_at"] - checked_df["session_start_at"]
    ).dt.total_seconds().to_numpy(dtype="float64", na_value=float("nan"))
    if checked_df["session_number"].le(0).any():
        raise ValueError(f"{context}session_number 必须大于 0。")
    if pd.isna(duration_seconds).any() or (duration_seconds <= 0).any():
        raise ValueError(f"{context}Session 起点必须严格早于终点。")
    if (duration_seconds % 60 != 0).any():
        raise ValueError(f"{context}Session 时间差不是整分钟。")
    if checked_df["minute_count"].ne(duration_seconds // 60).any():
        raise ValueError(f"{context}minute_count 与 Session 时间差不一致。")
    if checked_df["minute_count"].le(0).any():
        raise ValueError(f"{context}minute_count 必须大于 0。")

    contract_codes = checked_df["contract_code"].astype("string")
    contract_exchanges = contract_codes.str.rsplit(
        ".", n=1
    ).str[-1]
    if (
        ~contract_codes.str.contains(".", regex=False)
        | contract_exchanges.ne(checked_df["exchange_code"].astype("string"))
    ).any():
        raise ValueError(f"{context}exchange_code 与合约代码后缀不一致。")
    if (
        checked_df["year"].ne(checked_df["trading_date"].dt.year).any()
        or checked_df["month"].ne(checked_df["trading_date"].dt.month).any()
    ):
        raise ValueError(f"{context}year/month 与 trading_date 不一致。")

    group_columns = ["contract_code", "trading_date"]
    expected_session_numbers = checked_df.groupby(
        group_columns, sort=False
    ).cumcount().add(1)
    if checked_df["session_number"].ne(expected_session_numbers).any():
        raise ValueError(f"{context}同一合约日的 Session 编号不连续。")

    direct_columns = ["exchange_code", "underlying_code", "year", "month"]
    inconsistent_groups = checked_df.groupby(
        group_columns, sort=False
    )[direct_columns].nunique(dropna=False).gt(1).any(axis=1)
    if inconsistent_groups.any():
        raise ValueError(f"{context}同一合约日的直接属性不一致。")
    return checked_df


def validate_contract_input(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    table = pandas_to_arrow(
        frame.loc[:, FUTURES_CONTRACT_CALENDAR_SCHEMA.names],
        FUTURES_CONTRACT_CALENDAR_SCHEMA,
    )
    checked_df = arrow_to_pandas(table, FUTURES_CONTRACT_CALENDAR_SCHEMA)
    validate_contract_structure(checked_df, context)
    return checked_df.sort_values(UPSTREAM_PRIMARY_KEY).reset_index(drop=True)


# 结构快路径不读取 c05～c08 的状态字段。
def validate_structural_frame(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    table = pandas_to_arrow(frame.loc[:, STRUCTURAL_COLUMNS], STRUCTURAL_SCHEMA)
    checked_df = arrow_to_pandas(table, STRUCTURAL_SCHEMA)
    checked_df = checked_df.sort_values(PRIMARY_KEY).reset_index(drop=True)

    if checked_df.duplicated(PRIMARY_KEY).any():
        raise ValueError(f"{context}主键不唯一。")
    if checked_df.empty:
        return checked_df
    if not checked_df["bar_frequency"].isin(BAR_FREQUENCIES).all():
        raise ValueError(f"{context}bar_frequency 不在允许枚举中。")

    contract_codes = checked_df["contract_code"].astype("string")
    contract_exchanges = contract_codes.str.rsplit(
        ".", n=1
    ).str[-1]
    if (
        ~contract_codes.str.contains(".", regex=False)
        | contract_exchanges.ne(checked_df["exchange_code"].astype("string"))
    ).any():
        raise ValueError(f"{context}exchange_code 与合约代码后缀不一致。")
    if (
        checked_df["year"].ne(checked_df["trading_date"].dt.year).any()
        or checked_df["month"].ne(checked_df["trading_date"].dt.month).any()
    ):
        raise ValueError(f"{context}year/month 与 trading_date 不一致。")
    if checked_df["expected_bar_count"].le(0).any():
        raise ValueError(f"{context}expected_bar_count 必须大于 0。")

    daily_df = checked_df.loc[checked_df["bar_frequency"].eq("1d")]
    minute_df = checked_df.loc[checked_df["bar_frequency"].eq("1m")]
    session_columns = [
        "session_text", "session_start_at", "session_end_at", "is_night_session"
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
        expected_numbers = minute_df.groupby(
            ["contract_code", "trading_date"], sort=False
        ).cumcount().add(1)
        if minute_df["session_number"].ne(expected_numbers).any():
            raise ValueError(f"{context}分钟 Session 编号不连续。")
    return checked_df


# dirty 完整输出在 staging 前承担唯一一次完整状态校验；staging 与正式安装后只复核契约、主键和行数摘要。
def validate_bar_calendar_frame(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    table = pandas_to_arrow(
        frame.loc[:, FUTURES_BAR_CALENDAR_SCHEMA.names],
        FUTURES_BAR_CALENDAR_SCHEMA,
    )
    checked_df = arrow_to_pandas(table, FUTURES_BAR_CALENDAR_SCHEMA)
    checked_df = checked_df.sort_values(PRIMARY_KEY).reset_index(drop=True)
    validate_structural_frame(checked_df, context)
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
        raise ValueError(f"{context}中文状态说明字段不得为空。")
    if (
        checked_df["actual_bar_count"].lt(0).any()
        or checked_df["missing_bar_count"].lt(0).any()
    ):
        raise ValueError(f"{context}实际与缺失条数不得为负。")

    confirmed_closed = checked_df["schedule_status"].eq("confirmed_closed")
    if (
        confirmed_closed
        & (
            checked_df["evidence_level"].ne("authoritative")
            | checked_df["is_fetch_required"]
        )
    ).any():
        raise ValueError(f"{context}确认休市必须具有权威证据且不得继续拉取。")

    completed = checked_df["is_fetch_completed"]
    missing_run_id = (
        checked_df["fetch_run_id"].isna()
        | checked_df["fetch_run_id"].eq("")
    )
    if (completed & (missing_run_id | checked_df["fetch_completed_at"].isna())).any():
        raise ValueError(f"{context}完成状态缺少运行批次或完成时间。")
    if (~completed & checked_df["fetch_completed_at"].notna()).any():
        raise ValueError(f"{context}未完成格点不得具有完成时间。")

    unchecked = checked_df["missing_checked_at"].isna()
    if (
        unchecked
        & (checked_df["is_data_missing"] | checked_df["missing_bar_count"].ne(0))
    ).any():
        raise ValueError(f"{context}未经缺失检查不得记录缺失。")
    expected_missing = (
        checked_df["expected_bar_count"] - checked_df["actual_bar_count"]
    ).clip(lower=0).where(checked_df["is_fetch_required"], 0)
    checked_missing = ~unchecked
    if (
        checked_missing
        & checked_df["missing_bar_count"].ne(expected_missing)
    ).any():
        raise ValueError(f"{context}missing_bar_count 无法由条数复算。")
    if (
        checked_missing
        & checked_df["is_data_missing"].ne(expected_missing.gt(0))
    ).any():
        raise ValueError(f"{context}is_data_missing 与缺失条数不一致。")
    if (
        checked_df["quality_status"].ne("pending")
        & checked_df["quality_checked_at"].isna()
    ).any():
        raise ValueError(f"{context}非 pending 质量状态缺少检查时间。")
    return checked_df


def assess_structural_partition(
    expected_frame: pd.DataFrame,
    existing_frame: pd.DataFrame,
    forced_quality_error: str | None,
) -> dict[str, object]:
    expected_df = validate_structural_frame(expected_frame, "当前上游结构")
    business_quality_error = None
    try:
        existing_df = validate_structural_frame(existing_frame, "现有下游结构")
    except (TypeError, ValueError) as error:
        existing_row_count = len(existing_frame)
        existing_df = empty_pandas(STRUCTURAL_SCHEMA)
        business_quality_error = f"{type(error).__name__}: {error}"
    else:
        existing_row_count = len(existing_df)

    quality_messages = [
        message
        for message in [forced_quality_error, business_quality_error]
        if message is not None
    ]
    quality_error = "；".join(quality_messages) or None
    expected_table = pandas_to_arrow(expected_df, STRUCTURAL_SCHEMA)
    existing_table = pandas_to_arrow(existing_df, STRUCTURAL_SCHEMA)

    if expected_table.equals(existing_table) and quality_error is None:
        return {
            "expected_row_count": len(expected_df),
            "complete_count": len(expected_df),
            "missing_count": 0,
            "changed_count": 0,
            "extra_count": 0,
            "quality_error": None,
            "is_dirty": False,
        }

    joined_df = expected_df.merge(
        existing_df,
        how="outer",
        on=PRIMARY_KEY,
        suffixes=("__expected", "__existing"),
        indicator=True,
        validate="one_to_one",
    )
    missing_count = int(joined_df["_merge"].eq("left_only").sum())
    extra_count = int(joined_df["_merge"].eq("right_only").sum())
    common_df = joined_df.loc[joined_df["_merge"].eq("both")]
    same_structure = pd.Series(True, index=common_df.index, dtype=bool)
    for column in STRUCTURAL_NON_KEY_COLUMNS:
        expected_values = common_df[f"{column}__expected"]
        existing_values = common_df[f"{column}__existing"]
        same_values = expected_values.eq(existing_values) | (
            expected_values.isna() & existing_values.isna()
        )
        same_structure &= same_values.fillna(False).astype(bool)
    changed_count = int((~same_structure).sum())

    if forced_quality_error is not None and business_quality_error is None:
        changed_count = len(common_df)
    if business_quality_error is not None:
        extra_count = existing_row_count
    complete_count = (
        0
        if quality_error is not None
        else len(expected_df) - missing_count - changed_count
    )
    audit = {
        "expected_row_count": len(expected_df),
        "complete_count": complete_count,
        "missing_count": missing_count,
        "changed_count": changed_count,
        "extra_count": extra_count,
        "quality_error": quality_error,
    }
    audit["is_dirty"] = bool(
        missing_count or changed_count or extra_count or quality_error
    )
    return audit


# ## 契约化分区读取
# 
# 启动时只打开一次上下游根 Dataset；自动模式逐交易所—年月投影结构字段，不把完整历史或状态列一次性载入内存。既有湖只要求字段、类型、nullable 和稳定表身份兼容；中文描述等非身份 metadata 不触发历史重写，真正的物理或身份不兼容立即失败。

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


# 统一打开上游或下游 Dataset；既有湖不比较描述性 metadata。
def open_contract_dataset(
    table_path: pathlib.Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    partition_columns: list[str],
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
    expected_file_schema = pa.schema(
        [
            schema.field(name)
            for name in schema.names
            if name not in partition_columns
        ],
        metadata=schema.metadata,
    )
    for fragment in dataset.get_fragments():
        if not physically_and_identity_compatible(
            fragment.physical_schema, expected_file_schema
        ):
            raise TypeError(
                f"{label} fragment 物理结构或表身份与契约不一致："
                f"{fragment.path}"
            )
    return dataset


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


# 正式路径信任 c03 已提交语义，只转换 12 列投影并验证生成的 13 列 c04 结构。
def build_structural_partitions(
    upstream_contract_structure_df: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    upstream_contract_structure_table = pandas_to_arrow(
        upstream_contract_structure_df.loc[:, UPSTREAM_STRUCTURE_COLUMNS],
        UPSTREAM_STRUCTURE_SCHEMA,
    )
    upstream_contract_structure_df = arrow_to_pandas(
        upstream_contract_structure_table, UPSTREAM_STRUCTURE_SCHEMA
    )
    upstream_contract_structure_df = upstream_contract_structure_df.sort_values(
        UPSTREAM_PRIMARY_KEY
    ).reset_index(drop=True)

    minute_bar_structure_df = upstream_contract_structure_df.rename(
        columns={"minute_count": "expected_bar_count"}
    ).copy()
    minute_bar_structure_df["bar_frequency"] = "1m"
    minute_bar_structure_df = validate_structural_frame(
        minute_bar_structure_df.loc[:, STRUCTURAL_COLUMNS],
        "新生成分钟结构分区",
    )

    daily_bar_structure_df = upstream_contract_structure_df.drop_duplicates(
        ["contract_code", "trading_date"],
        keep="first",
    ).copy()
    daily_bar_structure_df["bar_frequency"] = "1d"
    daily_bar_structure_df["session_number"] = 0
    daily_bar_structure_df["session_text"] = None
    daily_bar_structure_df["session_start_at"] = None
    daily_bar_structure_df["session_end_at"] = None
    daily_bar_structure_df["is_night_session"] = None
    daily_bar_structure_df["expected_bar_count"] = 1
    daily_bar_structure_df = validate_structural_frame(
        daily_bar_structure_df.loc[:, STRUCTURAL_COLUMNS],
        "新生成日线结构分区",
    )
    return {
        "1d": daily_bar_structure_df,
        "1m": minute_bar_structure_df,
    }


# 脏分区慢路径补齐安全初始状态，并继续保留公开 helper 语义。
def build_fresh_partitions(
    contract_frame: pd.DataFrame,
    updated_at: datetime,
) -> dict[str, pd.DataFrame]:
    contracts_df = validate_contract_input(
        contract_frame,
        "上游合约日历分区",
    )
    structural_by_frequency = build_structural_partitions(contracts_df)
    fresh_by_frequency = {}

    for frequency, structural_df in structural_by_frequency.items():
        fresh_df = structural_df.copy()
        for column, value in initial_state(frequency, updated_at).items():
            fresh_df[column] = value
        fresh_by_frequency[frequency] = validate_bar_calendar_frame(
            fresh_df.loc[:, FUTURES_BAR_CALENDAR_SCHEMA.names],
            f"新生成 {frequency} 完整分区",
        )

    return fresh_by_frequency


# ## 自动差集与状态继承
# 
# 上游结构未变化时，完整保留 c05—c08 已回写的调度、选择、完成和质检证据；新增、删除或结构变化时只重置受影响格点。比较不会因为 updated_at 自身发生伪变化。状态合并只固定 Schema；dirty 完整叶在 staging 前统一执行一次完整业务校验，失败必须停止提交。

# In[ ]:


# 结构未变化时向量化继承下游事实与质检回写状态。
def merge_preserving_state(
    fresh_frame: pd.DataFrame,
    existing_frame: pd.DataFrame,
) -> pd.DataFrame:
    fresh_df = arrow_to_pandas(
        pandas_to_arrow(
            fresh_frame.loc[:, FUTURES_BAR_CALENDAR_SCHEMA.names],
            FUTURES_BAR_CALENDAR_SCHEMA,
        ),
        FUTURES_BAR_CALENDAR_SCHEMA,
    )
    existing_df = arrow_to_pandas(
        pandas_to_arrow(
            existing_frame.loc[:, FUTURES_BAR_CALENDAR_SCHEMA.names],
            FUTURES_BAR_CALENDAR_SCHEMA,
        ),
        FUTURES_BAR_CALENDAR_SCHEMA,
    )

    merged_df = fresh_df.merge(
        existing_df,
        how="left",
        on=PRIMARY_KEY,
        suffixes=("", "__existing"),
        indicator=True,
        validate="one_to_one",
    )
    same_structure = merged_df["_merge"].eq("both")
    for column in STRUCTURAL_NON_KEY_COLUMNS:
        fresh_values = merged_df[column]
        existing_values = merged_df[f"{column}__existing"]
        same_values = fresh_values.eq(existing_values) | (
            fresh_values.isna() & existing_values.isna()
        )
        same_structure &= same_values.fillna(False).astype(bool)

    # 新增或结构变化行保留 fresh 安全状态；只覆盖结构完全相同的行。
    for column in STATE_COLUMNS:
        merged_df[column] = merged_df[column].where(
            ~same_structure,
            merged_df[f"{column}__existing"],
        )

    merged_table = pandas_to_arrow(
        merged_df.loc[:, FUTURES_BAR_CALENDAR_SCHEMA.names],
        FUTURES_BAR_CALENDAR_SCHEMA,
    )
    return arrow_to_pandas(
        merged_table, FUTURES_BAR_CALENDAR_SCHEMA
    ).sort_values(PRIMARY_KEY).reset_index(drop=True)


# 比较期望分区与现有分区，汇总新增、删除、修订和质量错误。
def assess_partition(
    fresh_frame: pd.DataFrame,
    existing_frame: pd.DataFrame,
    forced_quality_error: str | None,
) -> tuple[pd.DataFrame, dict[str, object]]:
    # 状态合并阶段只固定 Schema；完整业务规则由提交前唯一 validator 负责。
    checked_existing_df = arrow_to_pandas(
        pandas_to_arrow(
            existing_frame.loc[:, FUTURES_BAR_CALENDAR_SCHEMA.names],
            FUTURES_BAR_CALENDAR_SCHEMA,
        ),
        FUTURES_BAR_CALENDAR_SCHEMA,
    )
    desired_df = merge_preserving_state(
        fresh_frame,
        checked_existing_df,
    )

    audit = assess_structural_partition(
        desired_df.loc[:, STRUCTURAL_COLUMNS],
        checked_existing_df.loc[:, STRUCTURAL_COLUMNS],
        forced_quality_error,
    )
    return desired_df, audit


# ## 单分区暂存、复读、替换与回滚
# 
# 自动模式提交完整叶分区。显式日期只用于非正式测试湖，提交前会保留同一叶分区中日期范围外的旧行，避免定向验证删除同月历史。
# 
# 每个 dirty 完整叶只在写 staging 前执行一次完整业务校验。staging 必须使用当前完整 Schema/metadata，并只复核主键唯一性和行数摘要；正式安装后同样只复核刚触达叶的物理结构、稳定表身份、主键唯一性和行数。既有叶的描述性 metadata 不触发维护写入。

# In[ ]:


def parquet_file_schema() -> pa.Schema:
    return pa.schema(
        [
            FUTURES_BAR_CALENDAR_SCHEMA.field(name)
            for name in FUTURES_BAR_CALENDAR_SCHEMA.names
            if name not in PARTITION_COLUMNS
        ],
        metadata=FUTURES_BAR_CALENDAR_SCHEMA.metadata,
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


# dirty 慢路径只精确打开当前正式叶，避免重新扫描或打开根 Dataset。
def read_existing_partition_leaf(
    target_path: pathlib.Path,
    partition_key: tuple[object, ...],
) -> pd.DataFrame:
    leaf_path = target_path / partition_relative_path(partition_key)
    parquet_files = list(leaf_path.glob("*.parquet"))
    if not parquet_files:
        return empty_pandas(FUTURES_BAR_CALENDAR_SCHEMA)

    leaf_dataset = ds.dataset(
        leaf_path,
        format="parquet",
        partitioning=HIVE_PARTITIONING,
        partition_base_dir=str(target_path),
    )
    leaf_schema = reconstructed_schema(
        leaf_dataset, FUTURES_BAR_CALENDAR_SCHEMA
    )
    if not physically_and_identity_compatible(
        leaf_schema, FUTURES_BAR_CALENDAR_SCHEMA
    ):
        raise TypeError(
            f"正式叶 {partition_key} 物理结构或表身份与契约不兼容。"
        )
    return read_partition(
        leaf_dataset,
        FUTURES_BAR_CALENDAR_SCHEMA,
        PARTITION_COLUMNS,
        partition_key,
    )


def commit_partition(
    frame: pd.DataFrame,
    lake_root: pathlib.Path,
    partition_key: tuple[object, ...],
    replace_start_date: date | None = None,
    replace_end_date: date | None = None,
    existing_partition_frame: pd.DataFrame | None = None,
) -> int:
    if (replace_start_date is None) != (replace_end_date is None):
        raise ValueError("替换起止日期必须同时提供。")
    if replace_start_date is not None and replace_start_date > replace_end_date:
        raise ValueError("替换起始日期不得晚于结束日期。")

    # 先固定输入 Schema；完整业务规则只在合并得到最终叶后校验一次。
    incoming_table = pandas_to_arrow(
        frame.loc[:, FUTURES_BAR_CALENDAR_SCHEMA.names],
        FUTURES_BAR_CALENDAR_SCHEMA,
    )
    incoming_df = arrow_to_pandas(
        incoming_table, FUTURES_BAR_CALENDAR_SCHEMA
    )
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
            outside_range = (
                incoming_df["trading_date"].lt(replace_start_date)
                | incoming_df["trading_date"].gt(replace_end_date)
            )
            if outside_range.any():
                raise ValueError("待提交数据越出显式替换日期范围。")

    silver_root = lake_root.resolve() / "silver"
    target_path = silver_root / TABLE_NAME

    run_id = uuid.uuid4().hex[:12]
    staging_path = silver_root / f".c04s-{run_id}"
    backup_path = silver_root / f".c04b-{run_id}"
    quarantine_path = silver_root / f".c04q-{run_id}"

    silver_root.mkdir(parents=True, exist_ok=True)
    for managed_path in (
        target_path,
        staging_path,
        backup_path,
        quarantine_path,
    ):
        if not managed_path.resolve().is_relative_to(silver_root):
            raise ValueError(f"数据集路径越出 silver 根目录：{managed_path}")

    if replace_start_date is None:
        complete_partition_df = incoming_df
    else:
        if existing_partition_frame is None:
            existing_partition_frame = read_existing_partition_leaf(
                target_path, partition_key
            )
        existing_table = pandas_to_arrow(
            existing_partition_frame.loc[
                :, FUTURES_BAR_CALENDAR_SCHEMA.names
            ],
            FUTURES_BAR_CALENDAR_SCHEMA,
        )
        existing_df = arrow_to_pandas(
            existing_table, FUTURES_BAR_CALENDAR_SCHEMA
        )
        retained_df = existing_df.loc[
            (existing_df["trading_date"] < replace_start_date)
            | (existing_df["trading_date"] > replace_end_date),
            FUTURES_BAR_CALENDAR_SCHEMA.names,
        ]
        complete_partition_df = pd.concat(
            [retained_df, incoming_df],
            ignore_index=True,
        )

    complete_partition_df = validate_bar_calendar_frame(
        complete_partition_df,
        "提交前 dirty 完整分区",
    )
    complete_partition_table = pandas_to_arrow(
        complete_partition_df,
        FUTURES_BAR_CALENDAR_SCHEMA,
    )
    primary_key_schema = pa.schema(
        [
            FUTURES_BAR_CALENDAR_SCHEMA.field(name)
            for name in PRIMARY_KEY
        ],
        metadata=FUTURES_BAR_CALENDAR_SCHEMA.metadata,
    )

    relative_path = partition_relative_path(partition_key)
    staging_path.mkdir(parents=True, exist_ok=False)

    # 先写 staging 并复核完整文件契约、主键和行数摘要，再接触正式目标。
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

            source_path = staging_path / relative_path
            validate_output_parquet_files(source_path)
            staged_dataset = ds.dataset(
                source_path,
                format="parquet",
                partitioning=HIVE_PARTITIONING,
                partition_base_dir=str(staging_path),
            )
            staged_schema = reconstructed_schema(
                staged_dataset, FUTURES_BAR_CALENDAR_SCHEMA
            )
            if not staged_schema.equals(
                FUTURES_BAR_CALENDAR_SCHEMA, check_metadata=True
            ):
                raise TypeError("staging Schema/metadata 与契约不一致。")

            staged_primary_key_df = read_partition(
                staged_dataset,
                primary_key_schema,
                PARTITION_COLUMNS,
                partition_key,
            )
            if staged_primary_key_df.duplicated(PRIMARY_KEY).any():
                raise ValueError("staging 分区主键不唯一。")
            if len(staged_primary_key_df) != len(complete_partition_table):
                raise ValueError("staging 分区行数摘要不一致。")
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
    old_partition_saved = False
    new_partition_installed = False
    # 正式替换使用备份和隔离目录，异常时恢复原分区。
    commit_succeeded = False

    backup_path.mkdir(parents=True, exist_ok=False)
    target_path.mkdir(parents=True, exist_ok=True)
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    saved_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        if destination_path.exists():
            shutil.move(str(destination_path), str(saved_path))
            old_partition_saved = True

        if source_path.is_dir():
            shutil.move(str(source_path), str(destination_path))
            new_partition_installed = True
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
                marker_replaced = True
                pq.write_table(
                    pa.Table.from_batches([], schema=expected_file_schema),
                    marker_path,
                )

        if not marker_path.exists():
            marker_created = True
            pq.write_table(
                pa.Table.from_batches([], schema=expected_file_schema),
                marker_path,
            )

        if destination_path.is_dir():
            committed_dataset = ds.dataset(
                destination_path,
                format="parquet",
                partitioning=HIVE_PARTITIONING,
                partition_base_dir=str(target_path),
            )
            committed_schema = reconstructed_schema(
                committed_dataset, FUTURES_BAR_CALENDAR_SCHEMA
            )
            if not physically_and_identity_compatible(
                committed_schema, FUTURES_BAR_CALENDAR_SCHEMA
            ):
                raise TypeError("正式叶物理结构或表身份与契约不一致。")
            committed_primary_key_df = read_partition(
                committed_dataset,
                primary_key_schema,
                PARTITION_COLUMNS,
                partition_key,
            )
        else:
            committed_primary_key_df = empty_pandas(primary_key_schema)
        if committed_primary_key_df.duplicated(PRIMARY_KEY).any():
            raise ValueError("正式叶主键不唯一。")
        if len(committed_primary_key_df) != len(complete_partition_table):
            raise ValueError("正式叶行数与 staging 摘要不一致。")

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
            if (
                (old_partition_saved or new_partition_installed)
                and destination_path.exists()
            ):
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


# ## 命令入口、尾部计划与显式全量维护
# 
# 默认模式读取每个交易所、频率的目标尾叶以确定最大 trading_date，只比较并追加严格晚于该水位的上游结构；同日或旧日变化、目标孤儿叶只由 --full 处理。目标缺少某一频率时，该频率自然从全部上游结构建立。clean 叶不读取状态列、不改变 updated_at；无 --write 时不创建 staging、不修改湖文件或日历状态。

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


def default_tail_partition_keys(
    upstream_partition_keys: set[tuple[object, ...]],
    target_partition_keys: set[tuple[object, ...]],
) -> set[tuple[object, ...]]:
    # 每个频率—交易所只选目标尾月及随后上游月份。内部缺口、
    # 目标独有孤儿叶和旧月修订留给 --full。空目标频率自然选全部上游。
    upstream_months_by_exchange: dict[str, set[tuple[int, int]]] = {}
    for exchange_code, year, month in upstream_partition_keys:
        upstream_months_by_exchange.setdefault(exchange_code, set()).add(
            (year, month)
        )

    target_months_by_frequency_exchange: dict[
        tuple[str, str], set[tuple[int, int]]
    ] = {}
    for frequency, exchange_code, year, month in target_partition_keys:
        target_months_by_frequency_exchange.setdefault(
            (frequency, exchange_code), set()
        ).add((year, month))

    exchange_codes = set(upstream_months_by_exchange)
    selected_partition_keys: set[tuple[object, ...]] = set()
    for frequency in sorted(BAR_FREQUENCIES):
        for exchange_code in sorted(exchange_codes):
            upstream_months = upstream_months_by_exchange.get(
                exchange_code, set()
            )
            target_months = target_months_by_frequency_exchange.get(
                (frequency, exchange_code), set()
            )
            if not upstream_months:
                continue
            if not target_months:
                selected_months = upstream_months
            else:
                tail_boundary = max(target_months)
                selected_months = {
                    value
                    for value in upstream_months
                    if value >= tail_boundary
                }
            selected_partition_keys.update(
                (frequency, exchange_code, year, month)
                for year, month in selected_months
            )
    return selected_partition_keys


@click.command()
@click.option("--lake-root", type=click.Path(path_type=pathlib.Path))
@click.option("--start-date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--end-date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--full", is_flag=True)
@click.option("--write", is_flag=True)
def main(
    lake_root: pathlib.Path | None,
    start_date: datetime | None,
    end_date: datetime | None,
    full: bool,
    write: bool,
) -> None:
    run_started_at = time.perf_counter()
    # 首先解析湖路径和显式日期，并执行正式写入门禁。
    formal_lake_root = settings.futures_lake_root
    resolved_lake_root = (lake_root or formal_lake_root).resolve()

    has_explicit_dates = start_date is not None or end_date is not None
    if (start_date is None) != (end_date is None):
        raise click.UsageError("--start-date 与 --end-date 必须同时提供。")
    if full and has_explicit_dates:
        raise click.UsageError("--full 与显式日期范围互斥。")
    if has_explicit_dates and write and resolved_lake_root == formal_lake_root:
        raise click.UsageError(
            "显式指定日期时禁止写入 FUTURES_LAKE_ROOT 指向的正式湖；"
            "请移除日期参数使用自动补缺，或改用非正式测试湖。"
        )

    requested_start_date = start_date.date() if start_date else None
    requested_end_date = end_date.date() if end_date else None
    automatic_tail_mode = not full and not has_explicit_dates
    if (
        requested_start_date is not None
        and requested_start_date > requested_end_date
    ):
        raise click.BadParameter("起始日期不得晚于结束日期。")

    # 根 Dataset 各打开一次；规划循环只做列投影和分区过滤。
    discovery_started_at = time.perf_counter()
    silver_root = resolved_lake_root / "silver"
    upstream_path = silver_root / UPSTREAM_TABLE_NAME
    target_path = silver_root / TABLE_NAME

    upstream_dataset = open_contract_dataset(
        upstream_path,
        UPSTREAM_PARTITIONING,
        FUTURES_CONTRACT_CALENDAR_SCHEMA,
        UPSTREAM_PARTITION_COLUMNS,
        "上游合约 Session 日历",
        required=True,
    )
    target_dataset = open_contract_dataset(
        target_path,
        HIVE_PARTITIONING,
        FUTURES_BAR_CALENDAR_SCHEMA,
        PARTITION_COLUMNS,
        "现有行情日历",
        required=False,
    )

    upstream_partition_keys = partition_keys_from_files(
        upstream_path,
        UPSTREAM_PARTITION_COLUMNS,
    )
    target_partition_keys = partition_keys_from_files(
        target_path,
        PARTITION_COLUMNS,
    )
    upstream_file_count = sum(
        pathlib.Path(path).name != "schema.parquet"
        for path in upstream_dataset.files
    )
    target_file_count = (
        sum(
            pathlib.Path(path).name != "schema.parquet"
            for path in target_dataset.files
        )
        if target_dataset is not None
        else 0
    )
    discovery_elapsed = time.perf_counter() - discovery_started_at

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
    automatic_tail_watermark_by_frequency_exchange: dict[
        tuple[str, str], date
    ] = {}
    target_tail_structure_row_count = 0
    if automatic_tail_mode and target_dataset is not None:
        target_partition_keys_by_frequency_exchange: dict[
            tuple[str, str], list[tuple[object, ...]]
        ] = {}
        for partition_key in target_partition_keys:
            frequency, exchange_code, _, _ = partition_key
            target_partition_keys_by_frequency_exchange.setdefault(
                (frequency, exchange_code), []
            ).append(partition_key)
        for frequency_exchange, partition_keys in sorted(
            target_partition_keys_by_frequency_exchange.items()
        ):
            tail_partition_key = max(
                partition_keys, key=lambda value: value[2:]
            )
            target_tail_structure_df = read_partition(
                target_dataset,
                STRUCTURAL_SCHEMA,
                PARTITION_COLUMNS,
                tail_partition_key,
            )
            target_tail_structure_row_count += len(
                target_tail_structure_df
            )
            if not target_tail_structure_df.empty:
                automatic_tail_watermark_by_frequency_exchange[
                    frequency_exchange
                ] = max(target_tail_structure_df["trading_date"])

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

    if full or has_explicit_dates:
        base_keys = sorted(upstream_base_keys | target_base_keys)
        candidate_partition_keys = {
            (frequency, *base_key)
            for base_key in base_keys
            for frequency in BAR_FREQUENCIES
        }
    else:
        candidate_partition_keys = default_tail_partition_keys(
            upstream_partition_keys, target_partition_keys
        )
        base_keys = sorted({
            (exchange_code, year, month)
            for _, exchange_code, year, month in candidate_partition_keys
        })
    run_updated_at = datetime.now(timezone.utc)

    planning_started_at = time.perf_counter()
    dirty_partition_plans = []
    evaluated_partition_count = 0
    clean_partition_count = 0
    upstream_source_row_count = 0
    target_structure_row_count = target_tail_structure_row_count
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
        if base_key in upstream_partition_keys:
            upstream_contract_structure_df = read_partition(
                upstream_dataset,
                UPSTREAM_STRUCTURE_SCHEMA,
                UPSTREAM_PARTITION_COLUMNS,
                base_key,
                requested_start_date,
                requested_end_date,
            )
        else:
            upstream_contract_structure_df = empty_pandas(
                UPSTREAM_STRUCTURE_SCHEMA
            )
        upstream_source_row_count += len(upstream_contract_structure_df)
        expected_bar_structure_df_by_frequency = build_structural_partitions(
            upstream_contract_structure_df
        )

        for frequency in sorted(BAR_FREQUENCIES):
            partition_key = (frequency, *base_key)
            if partition_key not in candidate_partition_keys:
                continue
            expected_bar_structure_df = (
                expected_bar_structure_df_by_frequency[frequency]
            )
            if automatic_tail_mode:
                exchange_code = str(base_key[0])
                automatic_tail_watermark = (
                    automatic_tail_watermark_by_frequency_exchange.get(
                        (frequency, exchange_code)
                    )
                )
                if automatic_tail_watermark is not None:
                    expected_bar_structure_df = (
                        expected_bar_structure_df.loc[
                            expected_bar_structure_df["trading_date"].gt(
                                automatic_tail_watermark
                            )
                        ].reset_index(drop=True)
                    )
                existing_bar_structure_df = empty_pandas(
                    STRUCTURAL_SCHEMA
                )
            elif (
                target_dataset is not None
                and partition_key in target_partition_keys
            ):
                existing_bar_structure_df = read_partition(
                    target_dataset,
                    STRUCTURAL_SCHEMA,
                    PARTITION_COLUMNS,
                    partition_key,
                    requested_start_date,
                    requested_end_date,
                )
            else:
                existing_bar_structure_df = empty_pandas(STRUCTURAL_SCHEMA)
            target_structure_row_count += len(existing_bar_structure_df)
            structural_audit = assess_structural_partition(
                expected_bar_structure_df,
                existing_bar_structure_df,
                None,
            )

            evaluated_partition_count += 1
            upstream_grid_count += int(structural_audit["expected_row_count"])
            complete_grid_count += int(structural_audit["complete_count"])
            missing_grid_count += int(structural_audit["missing_count"])
            changed_grid_count += int(structural_audit["changed_count"])
            extra_grid_count += int(structural_audit["extra_count"])

            # 只有业务格点或质量状态真正变化时才加入写入计划。
            if structural_audit["is_dirty"]:
                dirty_partition_plans.append(
                    {
                        "partition_key": partition_key,
                        **structural_audit,
                    }
                )
            else:
                clean_partition_count += 1

    structure_planning_elapsed = time.perf_counter() - planning_started_at
    total_planning_elapsed = time.perf_counter() - run_started_at

    if has_explicit_dates:
        mode = "explicit"
        plan_name = "explicit_plan"
    elif full:
        mode = "full"
        plan_name = "full_plan"
    else:
        mode = "automatic_tail"
        plan_name = "auto_plan"

    click.echo(
        f"{plan_name}: table={TABLE_NAME}; "
        f"upstream_grid_count={upstream_grid_count}; "
        f"complete_grid_count={complete_grid_count}; "
        f"missing_grid_count={missing_grid_count}; "
        f"changed_grid_count={changed_grid_count}; "
        f"extra_grid_count={extra_grid_count}; "
        f"touched_partition_count={len(dirty_partition_plans)}; "
        f"clean_partition_count={clean_partition_count}; "
        f"evaluated_partition_count={evaluated_partition_count}"
    )
    click.echo(
        f"planning_timing: table={TABLE_NAME}; "
        f"upstream_files={upstream_file_count}; "
        f"target_files={target_file_count}; "
        f"upstream_rows={upstream_source_row_count}; "
        f"target_rows={target_structure_row_count}; "
        f"discovery_seconds={discovery_elapsed:.3f}; "
        f"structure_planning_seconds={structure_planning_elapsed:.3f}; "
        f"total_seconds={total_planning_elapsed:.3f}"
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
    committed_partition_count = 0
    commit_elapsed_total = 0.0
    # 同一基础分区重新投影正式 c03 结构，逐分区构造完整 c04 目标并原子提交。
    for base_key in planned_base_keys:
        if base_key in upstream_partition_keys:
            upstream_contract_structure_df = read_partition(
                upstream_dataset,
                UPSTREAM_STRUCTURE_SCHEMA,
                UPSTREAM_PARTITION_COLUMNS,
                base_key,
                requested_start_date,
                requested_end_date,
            )
        else:
            upstream_contract_structure_df = empty_pandas(
                UPSTREAM_STRUCTURE_SCHEMA
            )
        expected_bar_structure_df_by_frequency = build_structural_partitions(
            upstream_contract_structure_df
        )
        fresh_bar_calendar_df_by_frequency = {}
        for frequency in sorted(BAR_FREQUENCIES):
            fresh_bar_calendar_df = (
                expected_bar_structure_df_by_frequency[frequency].copy()
            )
            if automatic_tail_mode:
                exchange_code = str(base_key[0])
                automatic_tail_watermark = (
                    automatic_tail_watermark_by_frequency_exchange.get(
                        (frequency, exchange_code)
                    )
                )
                if automatic_tail_watermark is not None:
                    fresh_bar_calendar_df = (
                        fresh_bar_calendar_df.loc[
                            fresh_bar_calendar_df["trading_date"].gt(
                                automatic_tail_watermark
                            )
                        ].reset_index(drop=True)
                    )
            for column, value in initial_state(
                frequency, run_updated_at
            ).items():
                fresh_bar_calendar_df[column] = value
            fresh_bar_calendar_df_by_frequency[frequency] = (
                fresh_bar_calendar_df.loc[
                    :, FUTURES_BAR_CALENDAR_SCHEMA.names
                ].copy()
            )

        for frequency in sorted(BAR_FREQUENCIES):
            partition_key = (frequency, *base_key)
            if partition_key not in planned_partition_keys:
                continue

            # 规划后可能有 c05～c08 更新状态；提交前精确复读当前完整叶。
            existing_complete_bar_calendar_df = (
                read_existing_partition_leaf(target_path, partition_key)
            )
            if automatic_tail_mode:
                exchange_code = str(base_key[0])
                automatic_tail_watermark = (
                    automatic_tail_watermark_by_frequency_exchange.get(
                        (frequency, exchange_code)
                    )
                )
                if automatic_tail_watermark is None:
                    existing_bar_calendar_df = empty_pandas(
                        FUTURES_BAR_CALENDAR_SCHEMA
                    )
                else:
                    existing_bar_calendar_df = (
                        existing_complete_bar_calendar_df.loc[
                            existing_complete_bar_calendar_df["trading_date"].gt(
                                automatic_tail_watermark
                            ),
                            FUTURES_BAR_CALENDAR_SCHEMA.names,
                        ]
                    )
            elif requested_start_date is None:
                existing_bar_calendar_df = (
                    existing_complete_bar_calendar_df
                )
            else:
                existing_bar_calendar_df = (
                    existing_complete_bar_calendar_df.loc[
                        existing_complete_bar_calendar_df["trading_date"].ge(
                            requested_start_date
                        )
                        & existing_complete_bar_calendar_df["trading_date"].le(
                            requested_end_date
                        ),
                        FUTURES_BAR_CALENDAR_SCHEMA.names,
                    ]
                )
            (
                desired_bar_calendar_df,
                current_structural_audit,
            ) = assess_partition(
                fresh_bar_calendar_df_by_frequency[frequency],
                existing_bar_calendar_df,
                None,
            )
            if not current_structural_audit["is_dirty"]:
                click.echo(
                    f"commit_skipped_clean: partition={partition_key}"
                )
                continue
            if automatic_tail_mode:
                desired_primary_key_index = pd.MultiIndex.from_frame(
                    desired_bar_calendar_df[PRIMARY_KEY]
                )
                existing_primary_key_index = pd.MultiIndex.from_frame(
                    existing_complete_bar_calendar_df[PRIMARY_KEY]
                )
                retained_existing_bar_calendar_df = (
                    existing_complete_bar_calendar_df.loc[
                        ~existing_primary_key_index.isin(
                            desired_primary_key_index
                        ),
                        FUTURES_BAR_CALENDAR_SCHEMA.names,
                    ]
                )
                partition_commit_df = pd.concat(
                    [
                        retained_existing_bar_calendar_df,
                        desired_bar_calendar_df,
                    ],
                    ignore_index=True,
                )
            else:
                partition_commit_df = desired_bar_calendar_df
            commit_started_at = time.perf_counter()
            committed_partition_rows = commit_partition(
                partition_commit_df,
                resolved_lake_root,
                partition_key,
                requested_start_date,
                requested_end_date,
                existing_partition_frame=existing_complete_bar_calendar_df,
            )
            committed_row_count += committed_partition_rows
            committed_partition_count += 1
            commit_elapsed = time.perf_counter() - commit_started_at
            commit_elapsed_total += commit_elapsed
            click.echo(
                "commit_timing: "
                f"partition={partition_key}; "
                f"rows={committed_partition_rows}; "
                f"seconds={commit_elapsed:.3f}"
            )

    if has_explicit_dates:
        commit_mode = "explicit_non_formal"
    elif full:
        commit_mode = "full_history"
    else:
        commit_mode = "automatic_tail"
    click.echo(
        f"committed: mode={commit_mode}; "
        f"rows={committed_row_count}; "
        f"partitions={committed_partition_count}; "
        f"commit_seconds={commit_elapsed_total:.3f}; "
        "remaining_pending=0"
    )


if __name__ == "__main__":
    main()

