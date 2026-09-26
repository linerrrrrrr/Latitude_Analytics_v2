#!/usr/bin/env python
# coding: utf-8

# # b03 JQData 期货仓单日报
# 
# 目标表：`fact_futures_warehouse_receipt_daily`。
# 
# 本入口只消费 `dim_futures_exchange_report_calendar` 中 `dataset_name=warehouse_receipt`
# 且 `is_fetch_required=true` 的格点。每个交易所—品种—交易日只查询一次 JQData
# `finance.FUT_WAREHOUSE_RECEIPT`，保存逐仓库数量、来源单位及较昨日变化。

# ## 自动更新与写入边界
# 
# 自动模式遵循：
# 
# `is_fetch_required=true 且 is_fetch_completed=false 的仓单格点 = 本次自动更新范围`
# 
# 空事实表只是下游完整格点集合为空。`--write` 只表示是否写入；显式日期只允许只读检查，
# 或写入与 `.env` 正式湖不同的临时/测试湖。事实叶正式摘要复读成功前不得把仓单日历标记完成。

# ## 初始化与权威 Schema

# In[ ]:


from __future__ import annotations

import hashlib
import math
import pathlib
import shutil
import statistics
import sys
import time
import uuid
from datetime import date, datetime, timezone
from types import ModuleType

# Notebook 可以从项目任意子目录启动；根目录定位方法由 .env.template 统一规定。
project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        sys.path.insert(0, str(candidate_root / "02_Futures_Lakehouse"))
        project_root = candidate_root
        break
else:
    raise RuntimeError("未找到项目根目录")

import click
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from config.data_contracts import (
    FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
    FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
    arrow_to_pandas,
    empty_pandas,
    pandas_to_arrow,
)
from config.jqdata_connection import authenticate_jqdata
from config.settings import settings


# ## Schema 契约呈现
# 
# 本节只在交互式 Notebook 内核中呈现只读 Schema 契约。依赖顺序为报告日历、仓单事实；
# 展示不会读取数据湖、认证 JQData 或产生写入。

# In[ ]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
        FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
    ], lake_root=settings.futures_lake_root)


# ## 表名、主键、Hive 分区与来源字段

# In[ ]:


# 两张表的物理契约只从权威 Schema metadata 各读取一次。
CALENDAR_TABLE_NAME = FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # 期货交易所报告采集日历维度表。
CALENDAR_PRIMARY_KEY = FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")  # 报告类型—交易所—品种—交易日格点。
CALENDAR_PARTITION_COLUMNS = FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 报告日历 Hive 叶分区顺序。

TABLE_NAME = FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # 期货逐仓库日频仓单事实表。
PRIMARY_KEY = FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")  # 日期—交易所—品种—仓库业务主键。
PARTITION_COLUMNS = FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 仓单事实 Hive 叶分区顺序。

GRID_COLUMNS = [
    "exchange_code",  # 项目/JQData 标准交易所代码。
    "underlying_code",  # 期货品种代码。
    "trading_date",  # 仓单报告归属交易日。
]
CALENDAR_PLANNING_COLUMNS = [
    *GRID_COLUMNS,
    "is_fetch_completed",
    "year",
    "month",
]
SCHEMA_IDENTITY_METADATA_KEYS = [
    b"table_name",
    b"primary_key",
    b"partition_columns",
]

# 显式选择业务转换和来源复核需要的列；不依赖整表隐式返回顺序。
JQDATA_FIELDS = [
    "day",  # 来源报告日期。
    "exchange",  # 来源交易所代码，仅用于复核日历。
    "underlying_code",  # 来源品种代码。
    "warehouse_name",  # 仓库或统计地点名称。
    "warehouse_receipt_number",  # 当日仓单数量。
    "unit",  # 来源计量单位。
    "warehouse_receipt_number_increase",  # 相对昨日增减。
]
SOURCE = "JQData_finance_FUT_WAREHOUSE_RECEIPT"

# 响应可能使用项目后缀或交易所常用简称；输出始终继承上游项目代码。
JQDATA_RESPONSE_EXCHANGES = {
    "CCFX": {"CCFX", "CFFEX"},
    "XDCE": {"XDCE", "DCE"},
    "XZCE": {"XZCE", "CZCE", "ZCE"},
    "XSGE": {"XSGE", "SHFE"},
    "XINE": {"XINE", "INE", "SHFE"},
    "GFEX": {"GFEX"},
}

CALENDAR_PARTITIONING = ds.partitioning(
    pa.schema([
        FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.field(name)
        for name in CALENDAR_PARTITION_COLUMNS
    ]),
    flavor="hive",
)
FACT_PARTITIONING = ds.partitioning(
    pa.schema([
        FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA.field(name)
        for name in PARTITION_COLUMNS
    ]),
    flavor="hive",
)


# ## 契约化读取与表级质检
# 
# 报告日历已经由 b01 正式提交，规划阶段只投影仓单完成快照；不重新证明 clean 历史。
# 每个 dirty 事实叶与日历叶各完整验证一次，staging 与正式路径只复读物理契约和主键摘要。

# In[ ]:


def reconstructed_schema(
    dataset: ds.Dataset,
    schema: pa.Schema,
) -> pa.Schema:
    # Hive 分区列由目录补回；按权威字段顺序重建后再比较整表 metadata。
    try:
        fields = [dataset.schema.field(name) for name in schema.names]
    except KeyError as error:
        raise TypeError("数据集缺少权威 Schema 字段。") from error

    return pa.schema(fields, metadata=dataset.schema.metadata)


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


def physically_and_identity_compatible(
    actual_schema: pa.Schema,
    expected_schema: pa.Schema,
) -> bool:
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


def validate_table_marker(
    table_path: pathlib.Path,
    schema: pa.Schema,
    partition_columns: list[str],
    label: str,
    *,
    required: bool,
) -> bool:
    marker_path = table_path / "schema.parquet"
    if not marker_path.is_file():
        has_existing_content = (
            table_path.is_dir()
            and next(table_path.iterdir(), None) is not None
        )
        if required or has_existing_content:
            raise FileNotFoundError(f"{label}缺少根级 schema.parquet：{table_path}")
        return False

    marker_metadata = pq.read_metadata(marker_path)
    expected_file_schema = parquet_file_schema(schema, partition_columns)
    if (
        marker_metadata.num_rows != 0
        or not physically_and_identity_compatible(
            pq.read_schema(marker_path),
            expected_file_schema,
        )
    ):
        raise TypeError(f"{label}根级 schema.parquet 与权威物理契约不一致。")
    return True


def open_planning_calendar_dataset(
    calendar_path: pathlib.Path,
) -> ds.Dataset:
    validate_table_marker(
        calendar_path,
        FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
        CALENDAR_PARTITION_COLUMNS,
        "正式报告日历",
        required=True,
    )
    calendar_dataset = ds.dataset(
        calendar_path,
        format="parquet",
        partitioning=CALENDAR_PARTITIONING,
    )
    if not physically_and_identity_compatible(
        reconstructed_schema(
            calendar_dataset,
            FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
        ),
        FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
    ):
        raise TypeError("正式报告日历物理结构或表身份与权威契约不一致。")

    expected_file_schema = parquet_file_schema(
        FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
        CALENDAR_PARTITION_COLUMNS,
    )
    for fragment in calendar_dataset.get_fragments():
        if not physically_and_identity_compatible(
            fragment.physical_schema,
            expected_file_schema,
        ):
            raise TypeError(
                "正式报告日历包含不兼容 Parquet fragment："
                f"{fragment.path}"
            )
    return calendar_dataset


def validate_calendar_frame(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    checked = pandas_to_arrow(
        frame.loc[:, FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names],
        FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
    )
    normalized = arrow_to_pandas(
        checked,
        FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
    )
    if normalized.duplicated(CALENDAR_PRIMARY_KEY).any():
        raise ValueError(f"{context}报告日历主键不唯一。")

    fetch_statuses = {
        "pending",
        "success",
        "empty_confirmed",
        "retryable_error",
        "permanent_error",
        "not_required",
    }
    quality_statuses = {
        "pending",
        "passed",
        "warning",
        "failed",
        "not_applicable",
    }
    now_utc = datetime.now(timezone.utc)

    for row in checked.to_pylist():
        if row["dataset_name"] != "warehouse_receipt":
            raise ValueError(f"{context}包含非仓单报告日历行。")
        if row["fetch_result_status"] not in fetch_statuses:
            raise ValueError(f"{context}采集结果状态不在允许枚举中。")
        if row["quality_status"] not in quality_statuses:
            raise ValueError(f"{context}质量状态不在允许枚举中。")
        if not str(row["requirement_reason"]).strip() or not str(row["quality_reason"]).strip():
            raise ValueError(f"{context}报告日历中文原因不得为空。")
        if row["trading_date"].year != row["year"] or row["trading_date"].month != row["month"]:
            raise ValueError(f"{context}报告日历年月分区与交易日不一致。")
        if row["expected_record_count"] < 0 or row["actual_record_count"] < 0:
            raise ValueError(f"{context}报告日历记录数不得为负。")

        if not row["is_fetch_required"]:
            if row["is_data_missing"]:
                raise ValueError(f"{context}当前无需采集格点不得标记数据缺失。")
            if not row["is_fetch_completed"]:
                if row["fetch_result_status"] != "not_required":
                    raise ValueError(f"{context}无完成凭证的免采集格点必须为 not_required。")
                if row["expected_record_count"] != 0 or row["actual_record_count"] != 0:
                    raise ValueError(f"{context}无完成凭证的免采集格点记录数必须为 0。")
                if row["quality_status"] != "not_applicable":
                    raise ValueError(f"{context}无完成凭证的免采集格点必须为 not_applicable。")
                if any(
                    row[name] is not None
                    for name in [
                        "fetch_run_id",
                        "fetch_completed_at",
                        "quality_checked_at",
                    ]
                ):
                    raise ValueError(f"{context}无完成凭证的免采集格点不得保留运行审计值。")
        elif row["fetch_result_status"] == "not_required":
            raise ValueError(f"{context}需采集格点不得标为 not_required。")

        completed_status = row["fetch_result_status"] in {"success", "empty_confirmed"}
        if row["is_fetch_completed"] != completed_status:
            raise ValueError(f"{context}完成布尔值与采集结果状态不一致。")
        if row["is_fetch_completed"]:
            if not row["fetch_run_id"] or row["fetch_completed_at"] is None:
                raise ValueError(f"{context}完成格点缺少批次或完成时间。")
        elif row["fetch_completed_at"] is not None:
            raise ValueError(f"{context}未完成格点不得具有完成时间。")

        if row["is_data_missing"] and (
            not row["is_fetch_required"]
            or row["fetch_result_status"] != "empty_confirmed"
            or row["actual_record_count"] != 0
        ):
            raise ValueError(f"{context}缺失状态不能由当前结果复算。")
        if row["quality_status"] in {"passed", "warning", "failed"} and row["quality_checked_at"] is None:
            raise ValueError(f"{context}已形成质检结论但缺少质检时间。")
        if row["updated_at"] > now_utc:
            raise ValueError(f"{context}updated_at 不得晚于当前 UTC 时间。")

    return normalized.sort_values(CALENDAR_PRIMARY_KEY).reset_index(drop=True)


def validate_warehouse_frame(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    checked = pandas_to_arrow(
        frame.loc[:, FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA.names],
        FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
    )
    normalized = arrow_to_pandas(
        checked,
        FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
    )
    if normalized.duplicated(PRIMARY_KEY).any():
        raise ValueError(f"{context}仓单事实主键不唯一。")

    now_utc = datetime.now(timezone.utc)
    for row in checked.to_pylist():
        warehouse_name = str(row["warehouse_name"]).strip()
        if not warehouse_name or warehouse_name.lower() in {"nan", "none"}:
            raise ValueError(f"{context}仓库名称不得为空。")
        if row["source"] != SOURCE:
            raise ValueError(f"{context}仓单事实来源不一致。")
        if row["trading_date"].year != row["year"] or row["trading_date"].month != row["month"]:
            raise ValueError(f"{context}仓单年月分区与交易日不一致。")
        if not math.isfinite(row["warehouse_receipt_number"]) or row["warehouse_receipt_number"] < 0:
            raise ValueError(f"{context}仓单数量必须为有限非负数。")

        unit = row["warehouse_receipt_unit"]
        if unit is not None and not str(unit).strip():
            raise ValueError(f"{context}非空仓单单位不得为空白。")
        change = row["warehouse_receipt_number_change"]
        if change is not None and not math.isfinite(change):
            raise ValueError(f"{context}仓单数量变化必须为有限数。")
        if row["updated_at"] > now_utc:
            raise ValueError(f"{context}仓单 updated_at 不得晚于当前 UTC 时间。")

    return normalized.sort_values(PRIMARY_KEY).reset_index(drop=True)


def partition_relative_path(
    partition_columns: list[str],
    partition_key: tuple[object, ...],
) -> pathlib.Path:
    if len(partition_columns) != len(partition_key):
        raise ValueError("分区键数量与权威分区列不一致。")
    return pathlib.Path(*[
        f"{column}={value}"
        for column, value in zip(
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
    label: str,
) -> pd.DataFrame:
    leaf_path = table_path / partition_relative_path(
        partition_columns,
        partition_key,
    )
    if not leaf_path.is_dir():
        return empty_pandas(schema)
    parquet_files = list(leaf_path.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"{label}叶目录不含 Parquet 文件：{leaf_path}")

    expected_file_schema = parquet_file_schema(schema, partition_columns)
    for parquet_path in parquet_files:
        if not physically_and_identity_compatible(
            pq.read_schema(parquet_path),
            expected_file_schema,
        ):
            raise TypeError(f"{label}叶包含不兼容文件：{parquet_path}")

    leaf_dataset = ds.dataset(
        leaf_path,
        format="parquet",
        partitioning=partitioning,
        partition_base_dir=str(table_path),
    )
    if not physically_and_identity_compatible(
        reconstructed_schema(leaf_dataset, schema),
        schema,
    ):
        raise TypeError(f"{label}叶物理结构或表身份与权威契约不一致。")
    leaf_table = leaf_dataset.to_table(columns=schema.names)
    if len(leaf_table):
        actual_partition_keys = set(
            leaf_table.select(partition_columns)
            .to_pandas()
            .itertuples(index=False, name=None)
        )
        if actual_partition_keys != {partition_key}:
            raise ValueError(f"{label}叶内容越出指定 Hive 分区。")
    return arrow_to_pandas(leaf_table, schema)


def primary_key_summary(
    table: pa.Table,
    primary_key: list[str],
    label: str,
) -> tuple[int, str]:
    primary_key_table = table.select(primary_key)
    primary_key_df = primary_key_table.to_pandas()
    if primary_key_df.duplicated(primary_key).any():
        raise ValueError(f"{label}主键不唯一。")
    sorted_primary_key_table = primary_key_table.sort_by([
        (column, "ascending")
        for column in primary_key
    ]).replace_schema_metadata(None)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, sorted_primary_key_table.schema) as writer:
        writer.write_table(sorted_primary_key_table)
    digest = hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()
    return len(primary_key_table), digest


def read_leaf_primary_key_summary(
    table_path: pathlib.Path,
    schema: pa.Schema,
    primary_key: list[str],
    partition_columns: list[str],
    partitioning: ds.Partitioning,
    partition_key: tuple[object, ...],
    label: str,
) -> tuple[int, str]:
    leaf_path = table_path / partition_relative_path(
        partition_columns,
        partition_key,
    )
    if not leaf_path.exists():
        empty_table = pa.Table.from_batches([], schema=schema)
        return primary_key_summary(empty_table, primary_key, label)
    parquet_files = list(leaf_path.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"{label}叶目录不含 Parquet 文件：{leaf_path}")

    expected_file_schema = parquet_file_schema(schema, partition_columns)
    for parquet_path in parquet_files:
        if not physically_and_identity_compatible(
            pq.read_schema(parquet_path),
            expected_file_schema,
        ):
            raise TypeError(f"{label}叶包含不兼容文件：{parquet_path}")
    leaf_dataset = ds.dataset(
        leaf_path,
        format="parquet",
        partitioning=partitioning,
        partition_base_dir=str(table_path),
    )
    if not physically_and_identity_compatible(
        reconstructed_schema(leaf_dataset, schema),
        schema,
    ):
        raise TypeError(f"{label}叶物理结构或表身份与权威契约不一致。")
    summary_columns = list(dict.fromkeys([
        *primary_key,
        *partition_columns,
    ]))
    summary_table = leaf_dataset.to_table(columns=summary_columns)
    if len(summary_table):
        actual_partition_keys = set(
            summary_table.select(partition_columns)
            .to_pandas()
            .itertuples(index=False, name=None)
        )
        if actual_partition_keys != {partition_key}:
            raise ValueError(f"{label}叶主键越出指定 Hive 分区。")
    return primary_key_summary(
        summary_table.select(primary_key),
        primary_key,
        label,
    )


# ## JQData 响应归一化
# 
# 来源日期、交易所和品种必须与待办日历完全一致；不能把越界响应跳过后冒充空数据。
# 仓单数量不可空且非负，单位原样保留，较昨日变化允许为空并保留正负号。

# In[ ]:


def normalize_warehouse_response(
    raw_df: pd.DataFrame,
    grid: dict[str, object],
    updated_at: datetime,
) -> pd.DataFrame:
    if not isinstance(raw_df, pd.DataFrame):
        raise TypeError("schema_error: JQData 仓单查询未返回 DataFrame。")
    if raw_df.empty:
        return empty_pandas(FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA)

    missing_columns = set(JQDATA_FIELDS) - set(raw_df.columns)
    if missing_columns:
        raise ValueError(f"schema_error: JQData 仓单表缺列 {sorted(missing_columns)}。")
    if len(raw_df) >= 5000:
        raise ValueError("schema_error: 单格点响应达到 run_query 上限，不能证明结果完整。")

    response_df = raw_df.loc[:, JQDATA_FIELDS].copy()
    response_df["day"] = pd.to_datetime(response_df["day"], errors="coerce").dt.date
    if response_df["day"].isna().any() or not response_df["day"].eq(grid["trading_date"]).all():
        raise ValueError("schema_error: JQData 仓单响应包含请求交易日之外的行。")

    response_underlying = response_df["underlying_code"].astype(str).str.strip().str.upper()
    if not response_underlying.eq(grid["underlying_code"]).all():
        raise ValueError("schema_error: JQData 仓单响应品种与待办日历不一致。")
    response_exchange_values = response_df["exchange"]
    response_exchange_codes = (
        response_exchange_values.astype(str).str.strip().str.upper()
    )
    allowed_exchanges = JQDATA_RESPONSE_EXCHANGES.get(
        grid["exchange_code"],
        {grid["exchange_code"]},
    )
    if (
        response_exchange_values.isna().any()
        or response_exchange_codes.eq("").any()
        or not response_exchange_codes.isin(allowed_exchanges).all()
    ):
        raise ValueError(
            "schema_error: JQData 仓单响应交易所与待办日历不一致；"
            f"actual={sorted(set(response_exchange_codes))}。"
        )

    rows = []
    for source_row in response_df.to_dict("records"):
        raw_warehouse_name = source_row["warehouse_name"]
        if pd.isna(raw_warehouse_name):
            raise ValueError("schema_error: JQData warehouse_name 为空。")
        warehouse_name = str(raw_warehouse_name).strip()
        if not warehouse_name or warehouse_name.lower() in {"nan", "none"}:
            raise ValueError("schema_error: JQData warehouse_name 为空。")

        quantity = pd.to_numeric(
            source_row["warehouse_receipt_number"],
            errors="coerce",
        )
        if pd.isna(quantity) or not math.isfinite(float(quantity)) or quantity < 0:
            raise ValueError("schema_error: JQData 仓单数量必须非空、有限且非负。")

        raw_unit = source_row["unit"]
        if pd.isna(raw_unit):
            unit = None
        else:
            unit_text = str(raw_unit).strip()
            unit = unit_text if unit_text and unit_text.lower() not in {"nan", "none"} else None

        raw_change = source_row["warehouse_receipt_number_increase"]
        if pd.isna(raw_change):
            quantity_change = None
        else:
            numeric_change = pd.to_numeric(raw_change, errors="coerce")
            if pd.isna(numeric_change) or not math.isfinite(float(numeric_change)):
                raise ValueError("schema_error: JQData 仓单数量变化必须为有限数。")
            quantity_change = float(numeric_change)

        rows.append({
            "trading_date": grid["trading_date"],
            "exchange_code": grid["exchange_code"],
            "underlying_code": grid["underlying_code"],
            "warehouse_name": warehouse_name,
            "warehouse_receipt_number": float(quantity),
            "warehouse_receipt_unit": unit,
            "warehouse_receipt_number_change": quantity_change,
            "source": SOURCE,
            "updated_at": updated_at,
            "year": grid["trading_date"].year,
            "month": grid["trading_date"].month,
        })

    frame = pd.DataFrame(
        rows,
        columns=FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA.names,
    )
    warehouse_table = pandas_to_arrow(
        frame,
        FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
    )
    return arrow_to_pandas(
        warehouse_table,
        FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
    )


# ## 完整格点与自动待办
# 
# 日常规划信任正式提交形成的 `is_fetch_completed` 快照，只投影读取 required 仓单日历。
# clean 历史不再由事实计数重复证明；每次写入前仍从当前日历叶复核待办身份。

# In[ ]:


def grid_count_map(frame: pd.DataFrame) -> dict[tuple[object, ...], int]:
    if frame.empty:
        return {}
    counts = frame.groupby(GRID_COLUMNS, dropna=False).size()
    return {tuple(key): int(value) for key, value in counts.items()}


def pending_report_grids(
    calendar_planning_df: pd.DataFrame,
) -> tuple[pd.DataFrame, int]:
    completed_mask = calendar_planning_df["is_fetch_completed"].eq(True)
    complete_count = int(completed_mask.sum())
    pending_df = calendar_planning_df.loc[
        ~completed_mask,
        [*GRID_COLUMNS, "year", "month"],
    ].copy()
    if not pending_df.empty:
        pending_df = pending_df.sort_values([
            "exchange_code",
            "underlying_code",
            "year",
            "month",
            "trading_date",
        ]).reset_index(drop=True)
    return pending_df, complete_count


def full_fact_partition(
    existing_df: pd.DataFrame,
    incoming_df: pd.DataFrame,
    touched_dates: set[date],
) -> pd.DataFrame:
    retained_df = existing_df.loc[
        ~existing_df["trading_date"].isin(touched_dates),
        FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA.names,
    ]
    complete_df = pd.concat([retained_df, incoming_df], ignore_index=True)
    if complete_df.empty:
        complete_df = empty_pandas(FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA)
    return validate_warehouse_frame(complete_df, "合并后的完整仓单分区")


def ensure_pending_calendar_grids(
    calendar_partition_df: pd.DataFrame,
    grid_records: list[dict[str, object]],
) -> None:
    for grid in grid_records:
        grid_mask = calendar_partition_df["dataset_name"].eq("warehouse_receipt")
        for column in GRID_COLUMNS:
            grid_mask &= calendar_partition_df[column].eq(grid[column])
        calendar_grid_df = calendar_partition_df.loc[grid_mask]
        if len(calendar_grid_df) != 1:
            raise ValueError(f"报告日历叶缺少唯一仓单格点：{grid}。")
        calendar_grid = calendar_grid_df.iloc[0]
        if (
            not bool(calendar_grid["is_fetch_required"])
            or bool(calendar_grid["is_fetch_completed"])
        ):
            raise ValueError(f"仓单格点提交前已不再属于未完成待办：{grid}。")


def performance_gate_result(
    partition_seconds: list[float],
    window_size: int,
    max_median_seconds: float,
) -> tuple[bool, float] | None:
    if len(partition_seconds) < window_size:
        return None
    median_seconds = float(statistics.median(
        partition_seconds[:window_size]
    ))
    return median_seconds <= max_median_seconds, median_seconds


# ## 完整叶分区 staging、正式复读与回滚
# 
# 调用方对 dirty 完整事实叶只执行一次业务 validator。staging 与正式安装只精确复读物理 Schema、
# 分区、行数、主键唯一性与主键摘要；失败恢复旧叶。空响应允许删除目标叶并保留根级契约标记。

# In[ ]:


def commit_complete_partition(
    validated_fact_partition_df: pd.DataFrame,
    lake_root: pathlib.Path,
    partition_key: tuple[object, ...],
) -> int:
    complete_df = validated_fact_partition_df.loc[
        :,
        FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA.names,
    ]
    if not complete_df.empty:
        actual_keys = set(
            complete_df[PARTITION_COLUMNS].itertuples(index=False, name=None)
        )
        if actual_keys != {partition_key}:
            raise ValueError("待提交仓单内容越出指定 Hive 叶分区。")
    complete_table = pandas_to_arrow(
        complete_df.loc[:, FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA.names],
        FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
    )
    expected_summary = primary_key_summary(
        complete_table,
        PRIMARY_KEY,
        "待提交完整仓单分区",
    )

    silver_root = lake_root.resolve() / "silver"
    target_path = silver_root / TABLE_NAME
    validate_table_marker(
        target_path,
        FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
        PARTITION_COLUMNS,
        "正式仓单事实",
        required=False,
    )
    run_id = uuid.uuid4().hex
    staging_path = silver_root / f".{TABLE_NAME}.staging-{run_id}"
    backup_path = silver_root / f".{TABLE_NAME}.backup-{run_id}"
    quarantine_path = silver_root / f".{TABLE_NAME}.failed-{run_id}"

    for managed_path in [target_path, staging_path, backup_path, quarantine_path]:
        if not managed_path.resolve().is_relative_to(silver_root):
            raise ValueError(f"数据集路径越出 silver 根目录：{managed_path}")
    silver_root.mkdir(parents=True, exist_ok=True)
    try:
        staging_path.mkdir(parents=True, exist_ok=False)

        file_schema = pa.schema(
            [
                field
                for field in FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA
                if field.name not in PARTITION_COLUMNS
            ],
            metadata=FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA.metadata,
        )
        pq.write_table(
            pa.Table.from_batches([], schema=file_schema),
            staging_path / "schema.parquet",
        )
        validate_table_marker(
            staging_path,
            FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
            PARTITION_COLUMNS,
            "仓单 staging",
            required=True,
        )
        if len(complete_table):
            ds.write_dataset(
                complete_table,
                staging_path,
                format="parquet",
                partitioning=FACT_PARTITIONING,
                existing_data_behavior="delete_matching",
                basename_template="part-{i}.parquet",
            )

        staged_summary = read_leaf_primary_key_summary(
            staging_path,
            FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
            PRIMARY_KEY,
            PARTITION_COLUMNS,
            FACT_PARTITIONING,
            partition_key,
            "仓单 staging",
        )
        if staged_summary != expected_summary:
            raise ValueError("仓单 staging 行数或主键摘要检查失败。")

    except Exception:
        shutil.rmtree(staging_path, ignore_errors=True)
        raise

    relative_path = pathlib.Path(*[
        f"{column}={value}"
        for column, value in zip(PARTITION_COLUMNS, partition_key, strict=True)
    ])
    source_path = staging_path / relative_path
    destination_path = target_path / relative_path
    saved_path = backup_path / relative_path
    target_marker_path = target_path / "schema.parquet"
    staging_marker_path = staging_path / "schema.parquet"
    target_had_partition = destination_path.exists()
    marker_created = False
    cleanup_recovery_paths = True

    try:
        target_path.mkdir(parents=True, exist_ok=True)
        if not target_marker_path.exists():
            shutil.move(str(staging_marker_path), str(target_marker_path))
            marker_created = True

        if target_had_partition:
            saved_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(destination_path), str(saved_path))
        if len(complete_table):
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source_path), str(destination_path))

        validate_table_marker(
            target_path,
            FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
            PARTITION_COLUMNS,
            "正式仓单事实",
            required=True,
        )
        committed_summary = read_leaf_primary_key_summary(
            target_path,
            FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
            PRIMARY_KEY,
            PARTITION_COLUMNS,
            FACT_PARTITIONING,
            partition_key,
            "正式仓单事实",
        )
        if committed_summary != expected_summary:
            raise ValueError("正式仓单行数或主键摘要检查失败。")
    except Exception as commit_error:
        rollback_errors = []
        try:
            if destination_path.exists():
                failed_path = quarantine_path / relative_path
                failed_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(destination_path), str(failed_path))
            if target_had_partition and saved_path.exists():
                destination_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(saved_path), str(destination_path))
            if marker_created and target_marker_path.exists():
                target_marker_path.unlink()
            if target_path.is_dir() and next(target_path.rglob("*.parquet"), None) is None:
                shutil.rmtree(target_path)
        except Exception as rollback_error:
            rollback_errors.append(str(rollback_error))

        if rollback_errors:
            cleanup_recovery_paths = False
            raise RuntimeError(
                f"{TABLE_NAME} 提交失败且回滚未完成；"
                f"请检查 {backup_path} 与 {quarantine_path}。"
            ) from commit_error
        raise
    finally:
        if cleanup_recovery_paths:
            shutil.rmtree(staging_path, ignore_errors=True)
            shutil.rmtree(backup_path, ignore_errors=True)
            shutil.rmtree(quarantine_path, ignore_errors=True)

    return len(complete_table)


# ## 报告日历完成、失败状态与协调提交
# 
# 事实叶正式物理复读后才写 `success` 或 `empty_confirmed`。日历 dirty 叶只完整验证一次，
# 单位缺失保留 warning；请求或 Schema 错误只回写当前日历叶并停止，供下次自动求差继续处理。

# In[ ]:


def apply_calendar_completion(
    calendar_partition_df: pd.DataFrame,
    grid_results: dict[tuple[object, ...], tuple[int, int]],
    fetch_run_id: str,
    completed_at: datetime,
) -> pd.DataFrame:
    updated_df = calendar_partition_df.copy()
    updated_grid_keys = set()

    for index, row in updated_df.iterrows():
        if row["dataset_name"] != "warehouse_receipt":
            continue
        grid_key = tuple(row[column] for column in GRID_COLUMNS)
        result = grid_results.get(grid_key)
        if result is None:
            continue
        updated_grid_keys.add(grid_key)

        actual_count, missing_unit_count = result
        has_rows = actual_count > 0
        has_unit_warning = missing_unit_count > 0
        updated_df.at[index, "is_fetch_completed"] = True
        updated_df.at[index, "fetch_result_status"] = "success" if has_rows else "empty_confirmed"
        updated_df.at[index, "is_data_missing"] = not has_rows
        updated_df.at[index, "expected_record_count"] = 1
        updated_df.at[index, "actual_record_count"] = actual_count
        updated_df.at[index, "quality_status"] = (
            "warning"
            if not has_rows or has_unit_warning
            else "passed"
        )
        if not has_rows:
            quality_reason = "JQData 查询成功但没有仓单记录，已按确认空记录。"
        elif has_unit_warning:
            quality_reason = (
                f"正式仓单事实复读 {actual_count} 行，其中 {missing_unit_count} 行来源未提供计量单位。"
            )
        else:
            quality_reason = f"JQData 仓单响应已转换并从正式事实表复读 {actual_count} 行。"
        updated_df.at[index, "quality_reason"] = quality_reason
        updated_df.at[index, "fetch_run_id"] = fetch_run_id
        updated_df.at[index, "fetch_completed_at"] = completed_at
        updated_df.at[index, "quality_checked_at"] = completed_at
        updated_df.at[index, "updated_at"] = completed_at

    if updated_grid_keys != set(grid_results):
        missing_grid_keys = sorted(set(grid_results) - updated_grid_keys)
        raise ValueError(f"报告日历叶缺少待完成格点：{missing_grid_keys}。")
    return validate_calendar_frame(updated_df, "仓单完成状态回写后的")


def apply_calendar_failure(
    calendar_partition_df: pd.DataFrame,
    grid_key: tuple[object, ...],
    fetch_status: str,
    failure_reason: str,
    fetch_run_id: str,
    failed_at: datetime,
    current_fact_count: int,
) -> pd.DataFrame:
    if fetch_status not in {"retryable_error", "permanent_error"}:
        raise ValueError("失败状态不在允许枚举中。")
    updated_df = calendar_partition_df.copy()
    updated = False

    for index, row in updated_df.iterrows():
        row_grid_key = tuple(row[column] for column in GRID_COLUMNS)
        if row["dataset_name"] != "warehouse_receipt" or row_grid_key != grid_key:
            continue
        updated = True
        updated_df.at[index, "is_fetch_completed"] = False
        updated_df.at[index, "fetch_result_status"] = fetch_status
        updated_df.at[index, "is_data_missing"] = False
        updated_df.at[index, "expected_record_count"] = 1
        updated_df.at[index, "actual_record_count"] = current_fact_count
        updated_df.at[index, "quality_status"] = "failed"
        updated_df.at[index, "quality_reason"] = failure_reason
        updated_df.at[index, "fetch_run_id"] = fetch_run_id
        updated_df.at[index, "fetch_completed_at"] = None
        updated_df.at[index, "quality_checked_at"] = failed_at
        updated_df.at[index, "updated_at"] = failed_at

    if not updated:
        raise ValueError(f"报告日历叶缺少待记录失败的格点：{grid_key}。")
    return validate_calendar_frame(updated_df, "仓单失败状态回写后的")


def commit_calendar_partitions(
    calendar_df: pd.DataFrame,
    touched_grid_keys: set[tuple[object, ...]],
    lake_root: pathlib.Path,
) -> int:
    if not touched_grid_keys:
        return 0

    touched_mask = (
        calendar_df["dataset_name"].eq("warehouse_receipt")
        & calendar_df[GRID_COLUMNS].apply(tuple, axis=1).isin(touched_grid_keys)
    )
    touched_df = calendar_df.loc[touched_mask]
    partition_keys = set(
        touched_df[CALENDAR_PARTITION_COLUMNS].itertuples(index=False, name=None)
    )

    for partition_key in sorted(partition_keys):
        partition_mask = pd.Series(True, index=calendar_df.index)
        for column, value in zip(CALENDAR_PARTITION_COLUMNS, partition_key, strict=True):
            partition_mask &= calendar_df[column].eq(value)
        complete_df = calendar_df.loc[
            partition_mask,
            FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names,
        ]

        # 日历和事实使用同一套完整叶分区提交语义，但保持各自独立 Schema。
        complete_table = pandas_to_arrow(
            complete_df,
            FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
        )
        expected_summary = primary_key_summary(
            complete_table,
            CALENDAR_PRIMARY_KEY,
            "待提交完整报告日历分区",
        )
        silver_root = lake_root.resolve() / "silver"
        target_path = silver_root / CALENDAR_TABLE_NAME
        validate_table_marker(
            target_path,
            FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
            CALENDAR_PARTITION_COLUMNS,
            "正式报告日历",
            required=True,
        )
        run_id = uuid.uuid4().hex
        staging_path = silver_root / f".{CALENDAR_TABLE_NAME}.staging-{run_id}"
        backup_path = silver_root / f".{CALENDAR_TABLE_NAME}.backup-{run_id}"
        quarantine_path = silver_root / f".{CALENDAR_TABLE_NAME}.failed-{run_id}"
        relative_path = pathlib.Path(*[
            f"{column}={value}"
            for column, value in zip(
                CALENDAR_PARTITION_COLUMNS,
                partition_key,
                strict=True,
            )
        ])

        for managed_path in [target_path, staging_path, backup_path, quarantine_path]:
            if not managed_path.resolve().is_relative_to(silver_root):
                raise ValueError(f"数据集路径越出 silver 根目录：{managed_path}")
        try:
            staging_path.mkdir(parents=True, exist_ok=False)
            file_schema = pa.schema(
                [
                    field
                    for field in FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA
                    if field.name not in CALENDAR_PARTITION_COLUMNS
                ],
                metadata=FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.metadata,
            )
            pq.write_table(
                pa.Table.from_batches([], schema=file_schema),
                staging_path / "schema.parquet",
            )
            validate_table_marker(
                staging_path,
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
                CALENDAR_PARTITION_COLUMNS,
                "报告日历 staging",
                required=True,
            )
            ds.write_dataset(
                complete_table,
                staging_path,
                format="parquet",
                partitioning=CALENDAR_PARTITIONING,
                existing_data_behavior="delete_matching",
                basename_template="part-{i}.parquet",
            )
            staged_summary = read_leaf_primary_key_summary(
                staging_path,
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
                CALENDAR_PRIMARY_KEY,
                CALENDAR_PARTITION_COLUMNS,
                CALENDAR_PARTITIONING,
                partition_key,
                "报告日历 staging",
            )
            if staged_summary != expected_summary:
                raise ValueError("报告日历 staging 行数或主键摘要检查失败。")

        except Exception:
            shutil.rmtree(staging_path, ignore_errors=True)
            raise

        source_path = staging_path / relative_path
        destination_path = target_path / relative_path
        saved_path = backup_path / relative_path
        target_had_partition = destination_path.exists()
        cleanup_recovery_paths = True

        try:
            if target_had_partition:
                saved_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(destination_path), str(saved_path))
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source_path), str(destination_path))

            validate_table_marker(
                target_path,
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
                CALENDAR_PARTITION_COLUMNS,
                "正式报告日历",
                required=True,
            )
            committed_summary = read_leaf_primary_key_summary(
                target_path,
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
                CALENDAR_PRIMARY_KEY,
                CALENDAR_PARTITION_COLUMNS,
                CALENDAR_PARTITIONING,
                partition_key,
                "正式报告日历",
            )
            if committed_summary != expected_summary:
                raise ValueError("正式报告日历行数或主键摘要检查失败。")
        except Exception as commit_error:
            rollback_errors = []
            try:
                if destination_path.exists():
                    failed_path = quarantine_path / relative_path
                    failed_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(destination_path), str(failed_path))
                if target_had_partition and saved_path.exists():
                    destination_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(saved_path), str(destination_path))
            except Exception as rollback_error:
                rollback_errors.append(str(rollback_error))

            if rollback_errors:
                cleanup_recovery_paths = False
                raise RuntimeError(
                    "报告日历提交失败且回滚未完成；"
                    f"请检查 {backup_path} 与 {quarantine_path}。"
                ) from commit_error
            raise
        finally:
            if cleanup_recovery_paths:
                shutil.rmtree(staging_path, ignore_errors=True)
                shutil.rmtree(backup_path, ignore_errors=True)
                shutil.rmtree(quarantine_path, ignore_errors=True)

    return len(touched_df)


# ## JQData 查询边界
# 
# 每个待办格点只执行一次显式字段查询。连接和 Windows TUN 出口由共享连接模块负责；业务入口只负责
# 构造 `FUT_WAREHOUSE_RECEIPT` 查询，并区分永久权限/表错误与可重试连接错误。

# In[ ]:


def query_warehouse_grid(
    jqdata: ModuleType,
    grid: dict[str, object],
) -> pd.DataFrame:
    table = jqdata.finance.FUT_WAREHOUSE_RECEIPT
    query_object = jqdata.query(*[
        getattr(table, field_name)
        for field_name in JQDATA_FIELDS
    ]).filter(
        table.day == grid["trading_date"],
        table.underlying_code == grid["underlying_code"],
    )

    try:
        raw_df = jqdata.finance.run_query(query_object)
    except Exception as error:
        message = str(error)
        permanent_markers = ["无权限", "permission", "no table", "不存在"]
        error_type = (
            "permanent_error"
            if any(marker.lower() in message.lower() for marker in permanent_markers)
            else "retryable_error"
        )
        raise RuntimeError(
            f"{error_type}: JQData FUT_WAREHOUSE_RECEIPT 查询失败；grid={grid}。"
        ) from error

    if raw_df is None:
        raise RuntimeError(
            f"retryable_error: JQData 仓单查询返回 None；grid={grid}。"
        )
    return raw_df


# ## CLI：自动差集、分区进度与是否写入
# 
# 默认从正式报告日历的未完成快照形成待办，并按事实叶顺序处理；每个成功分区输出 API、事实、日历和总耗时。
# 性能窗口与中位耗时上限必须成对提供且只用于自动正式写入；门槛失败只在完整提交边界停止。

# In[ ]:


@click.command()
@click.option("--lake-root", type=click.Path(path_type=pathlib.Path))
@click.option("--start-date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--end-date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option(
    "--performance-window-size",
    type=click.IntRange(min=1),
)
@click.option(
    "--performance-max-median-seconds",
    type=click.FloatRange(min=0.0, min_open=True),
)
@click.option("--write", is_flag=True)
def main(
    lake_root: pathlib.Path | None,
    start_date: datetime | None,
    end_date: datetime | None,
    performance_window_size: int | None,
    performance_max_median_seconds: float | None,
    write: bool,
) -> None:
    formal_lake_root = settings.futures_lake_root.resolve()
    resolved_lake_root = (lake_root or formal_lake_root).resolve()
    has_explicit_dates = start_date is not None or end_date is not None

    if (start_date is None) != (end_date is None):
        raise click.UsageError("--start-date 与 --end-date 必须同时提供。")
    has_performance_gate = performance_window_size is not None
    if has_performance_gate != (performance_max_median_seconds is not None):
        raise click.UsageError(
            "--performance-window-size 与 "
            "--performance-max-median-seconds 必须同时提供。"
        )
    if has_explicit_dates and write and resolved_lake_root == formal_lake_root:
        raise click.UsageError(
            "显式指定日期时禁止写入 FUTURES_LAKE_ROOT 指向的正式湖；"
            "请移除日期参数使用自动更新，或改用非正式测试湖。"
        )
    if has_performance_gate and (
        not write
        or has_explicit_dates
        or resolved_lake_root != formal_lake_root
    ):
        raise click.UsageError(
            "性能门槛只允许用于无显式日期的正式湖 --write 自动更新。"
        )

    requested_start = start_date.date() if start_date is not None else None
    requested_end = end_date.date() if end_date is not None else None
    if requested_start is not None and requested_start > requested_end:
        raise click.BadParameter("起始日期不得晚于结束日期。")

    silver_root = resolved_lake_root / "silver"
    calendar_path = silver_root / CALENDAR_TABLE_NAME
    fact_path = silver_root / TABLE_NAME

    planning_started_at = time.perf_counter()
    calendar_dataset = open_planning_calendar_dataset(calendar_path)
    calendar_filter = (
        (ds.field("dataset_name") == "warehouse_receipt")
        & (ds.field("is_fetch_required") == True)
    )
    if requested_start is not None:
        calendar_filter &= ds.field("trading_date") >= pa.scalar(
            requested_start,
            type=pa.date32(),
        )
        calendar_filter &= ds.field("trading_date") <= pa.scalar(
            requested_end,
            type=pa.date32(),
        )
    calendar_planning_df = calendar_dataset.to_table(
        columns=CALENDAR_PLANNING_COLUMNS,
        filter=calendar_filter,
    ).to_pandas()
    validate_table_marker(
        fact_path,
        FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
        PARTITION_COLUMNS,
        "正式仓单事实",
        required=False,
    )

    pending_df, complete_count = pending_report_grids(calendar_planning_df)
    mode = "explicit" if has_explicit_dates else "automatic"
    click.echo(
        f"table={TABLE_NAME}; mode={mode}; "
        f"lake_root={resolved_lake_root}; write={str(write).lower()}"
    )
    click.echo(
        "reconciliation_plan: "
        f"complete_grid_count={complete_count}; "
        f"pending_grid_count={len(pending_df)}; "
        f"planning_seconds={time.perf_counter() - planning_started_at:.3f}"
    )
    if pending_df.empty:
        click.echo("仓单事实与报告日历状态已经完整一致。")
        return

    # 只有确实存在 API 待办时才认证，纯完整性检查不会消耗供应商连接。
    jqdata = authenticate_jqdata(settings.jqdata_id, settings.jqdata_secret)
    partition_groups = pending_df.groupby(
        ["exchange_code", "underlying_code", "year", "month"],
        sort=True,
    )
    partition_count = partition_groups.ngroups
    click.echo(f"pending_partitions={partition_count}")

    total_rows = 0
    processed_grid_count = 0
    successful_partition_seconds: list[float] = []

    for group_number, (raw_partition_key, group_df) in enumerate(
        partition_groups,
        start=1,
    ):
        partition_key = tuple(raw_partition_key)
        partition_started_at = time.perf_counter()
        updated_at = datetime.now(timezone.utc)
        batch_id = uuid.uuid4().hex
        frames = []

        click.echo(
            f"partition_start: {group_number}/{partition_count}; "
            f"key={partition_key}; grids={len(group_df)}"
        )
        group_records = group_df[GRID_COLUMNS].to_dict("records")
        calendar_partition_values = {
            "dataset_name": "warehouse_receipt",
            "exchange_code": partition_key[0],
            "year": partition_key[2],
            "month": partition_key[3],
        }
        calendar_partition_key = tuple(
            calendar_partition_values[column]
            for column in CALENDAR_PARTITION_COLUMNS
        )
        api_started_at = time.perf_counter()
        for grid in group_records:
            grid_key = tuple(grid[column] for column in GRID_COLUMNS)
            try:
                raw_df = query_warehouse_grid(jqdata, grid)
                grid_df = normalize_warehouse_response(raw_df, grid, updated_at)
            except Exception as error:
                message = str(error)
                if write:
                    fetch_status = (
                        "retryable_error"
                        if message.startswith("retryable_error:")
                        else "permanent_error"
                    )
                    existing_fact_partition_df = read_partition_leaf(
                        fact_path,
                        FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
                        PARTITION_COLUMNS,
                        FACT_PARTITIONING,
                        partition_key,
                        "正式仓单事实",
                    )
                    calendar_partition_df = read_partition_leaf(
                        calendar_path,
                        FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
                        CALENDAR_PARTITION_COLUMNS,
                        CALENDAR_PARTITIONING,
                        calendar_partition_key,
                        "正式报告日历",
                    )
                    ensure_pending_calendar_grids(
                        calendar_partition_df,
                        [grid],
                    )
                    current_fact_count = grid_count_map(
                        existing_fact_partition_df
                    ).get(grid_key, 0)
                    failed_calendar_partition_df = apply_calendar_failure(
                        calendar_partition_df,
                        grid_key,
                        fetch_status,
                        f"仓单采集失败：{message}",
                        batch_id,
                        datetime.now(timezone.utc),
                        current_fact_count,
                    )
                    commit_calendar_partitions(
                        failed_calendar_partition_df,
                        {grid_key},
                        resolved_lake_root,
                    )
                raise click.ClickException(message) from error
            frames.append(grid_df)
        api_seconds = time.perf_counter() - api_started_at

        if any(not frame.empty for frame in frames):
            incoming_table = pandas_to_arrow(
                pd.concat(frames, ignore_index=True),
                FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
            )
            incoming_df = arrow_to_pandas(
                incoming_table,
                FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
            )
        else:
            incoming_df = empty_pandas(FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA)
        requested_grid_keys = {
            tuple(grid[column] for column in GRID_COLUMNS)
            for grid in group_records
        }
        incoming_grid_keys = (
            set(incoming_df[GRID_COLUMNS].itertuples(index=False, name=None))
            if not incoming_df.empty
            else set()
        )
        if not incoming_grid_keys.issubset(requested_grid_keys):
            raise ValueError("JQData 转换结果包含当前分区待办之外的格点。")
        click.echo(
            f"api_success: key={partition_key}; rows={len(incoming_df)}"
        )
        if not write:
            total_rows += len(incoming_df)
            processed_grid_count += len(group_df)
            partition_seconds = time.perf_counter() - partition_started_at
            click.echo(
                f"partition_checked: key={partition_key}; grids={len(group_df)}"
            )
            click.echo(
                f"partition_timing: key={partition_key}; grids={len(group_df)}; "
                f"api_seconds={api_seconds:.3f}; fact_seconds=0.000; "
                f"calendar_seconds=0.000; partition_seconds={partition_seconds:.3f}"
            )
            continue

        fact_started_at = time.perf_counter()
        existing_fact_partition_df = read_partition_leaf(
            fact_path,
            FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
            PARTITION_COLUMNS,
            FACT_PARTITIONING,
            partition_key,
            "正式仓单事实",
        )
        calendar_partition_df = read_partition_leaf(
            calendar_path,
            FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
            CALENDAR_PARTITION_COLUMNS,
            CALENDAR_PARTITIONING,
            calendar_partition_key,
            "正式报告日历",
        )
        ensure_pending_calendar_grids(calendar_partition_df, group_records)

        touched_dates = set(group_df["trading_date"].tolist())
        complete_df = full_fact_partition(
            existing_fact_partition_df,
            incoming_df,
            touched_dates,
        )
        commit_complete_partition(
            complete_df,
            resolved_lake_root,
            partition_key,
        )
        fact_seconds = time.perf_counter() - fact_started_at

        partition_counts = grid_count_map(complete_df)
        if complete_df.empty:
            missing_unit_counts = {}
        else:
            missing_unit_series = (
                complete_df.loc[
                    complete_df["warehouse_receipt_unit"].isna()
                ]
                .groupby(GRID_COLUMNS, dropna=False)
                .size()
            )
            missing_unit_counts = {
                tuple(key): int(value)
                for key, value in missing_unit_series.items()
            }
        grid_results = {
            tuple(grid[column] for column in GRID_COLUMNS): (
                partition_counts.get(
                    tuple(grid[column] for column in GRID_COLUMNS),
                    0,
                ),
                missing_unit_counts.get(
                    tuple(grid[column] for column in GRID_COLUMNS),
                    0,
                ),
            )
            for grid in group_df[GRID_COLUMNS].to_dict("records")
        }
        calendar_started_at = time.perf_counter()
        completed_at = datetime.now(timezone.utc)
        completed_calendar_partition_df = apply_calendar_completion(
            calendar_partition_df,
            grid_results,
            batch_id,
            completed_at,
        )
        calendar_rows = commit_calendar_partitions(
            completed_calendar_partition_df,
            set(grid_results),
            resolved_lake_root,
        )
        calendar_seconds = time.perf_counter() - calendar_started_at

        total_rows += len(incoming_df)
        processed_grid_count += len(group_df)
        partition_seconds = time.perf_counter() - partition_started_at
        click.echo(
            f"partition_committed: key={partition_key}; "
            f"calendar_rows={calendar_rows}; grids={len(group_df)}"
        )
        click.echo(
            f"partition_timing: key={partition_key}; grids={len(group_df)}; "
            f"api_seconds={api_seconds:.3f}; fact_seconds={fact_seconds:.3f}; "
            f"calendar_seconds={calendar_seconds:.3f}; "
            f"partition_seconds={partition_seconds:.3f}"
        )
        successful_partition_seconds.append(partition_seconds)
        if (
            has_performance_gate
            and len(successful_partition_seconds) == performance_window_size
        ):
            gate_result = performance_gate_result(
                successful_partition_seconds,
                performance_window_size,
                performance_max_median_seconds,
            )
            if gate_result is None:
                raise RuntimeError("性能门槛样本计数状态不一致。")
            gate_passed, median_seconds = gate_result
            if not gate_passed:
                message = (
                    "performance_gate_failed: "
                    f"samples={performance_window_size}; "
                    f"median_seconds={median_seconds:.3f}; "
                    f"max_median_seconds={performance_max_median_seconds:.3f}"
                )
                click.echo(message)
                raise click.ClickException(message)
            click.echo(
                "performance_gate_passed: "
                f"samples={performance_window_size}; "
                f"median_seconds={median_seconds:.3f}; "
                f"max_median_seconds={performance_max_median_seconds:.3f}"
            )

    click.echo(
        f"finished: grids={processed_grid_count}; rows={total_rows}; "
        f"write={str(write).lower()}; "
        f"performance_samples={len(successful_partition_seconds)}"
    )


# ## Notebook 与脚本运行入口

# In[ ]:


if "ipykernel" in sys.modules:
    # Notebook 默认执行正式湖自动 dry-run；带写入的测试必须显式使用非正式湖。
    main.main(
        args=[],
        prog_name="b03_warehouse_receipt",
        standalone_mode=False,
    )
elif __name__ == "__main__":
    main()

