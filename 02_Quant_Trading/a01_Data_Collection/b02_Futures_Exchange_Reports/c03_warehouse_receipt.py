#!/usr/bin/env python
# coding: utf-8

# # c03 JQData 期货仓单日报
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
# `上游当前 required 仓单格点 − 事实与日历状态共同证明完整的格点 = 本次自动更新范围`
# 
# 空事实表只是下游完整格点集合为空。`--write` 只表示是否写入；显式日期只允许只读检查，
# 或写入与 `.env` 正式湖不同的临时/测试湖。事实正式复读成功前不得把仓单日历标记完成。

# ## 初始化与权威 Schema

# In[ ]:


from __future__ import annotations

import math
import pathlib
import shutil
import sys
import uuid
from collections.abc import Callable
from datetime import date, datetime, timezone
from types import ModuleType

# Notebook 可以从项目任意子目录启动；根目录定位方法由 .env.template 统一规定。
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

from config.data_contracts import (
    FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
    FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
    arrow_to_pandas,
    empty_pandas,
    pandas_to_arrow,
)
from config.jqdata_connection import authenticate_jqdata
from config.settings import settings


# ## Schema 契约交互浏览
# 
# 只在交互式 Notebook 内核中展示只读语义浏览界面。依赖顺序为报告日历、仓单事实；
# 展示不会读取数据湖、认证 JQData 或产生写入。

# In[ ]:


if "ipykernel" in sys.modules:
    from config.notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
        FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
    ])


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
# 报告日历已经由 c01 生成，本入口只检查自身直接依赖和将要回写的状态关系；仓单事实则完整检查
# Schema/metadata、主键、日期、来源、仓库名称、数量、单位、变化值和分区。

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


def open_exact_dataset(
    table_path: pathlib.Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    label: str,
) -> ds.Dataset:
    # 正式输入、staging 与提交后输出都执行同一精确物理契约检查。
    parquet_files = list(table_path.rglob("*.parquet")) if table_path.is_dir() else []
    if not parquet_files:
        raise FileNotFoundError(f"{label}不存在：{table_path}")

    dataset = ds.dataset(
        table_path,
        format="parquet",
        partitioning=partitioning,
    )
    if not reconstructed_schema(dataset, schema).equals(
        schema,
        check_metadata=True,
    ):
        raise TypeError(f"{label} Schema/metadata 与权威契约不一致。")

    return dataset


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
        if row["dataset_name"] not in {
            "position_rank",
            "member_position",
            "warehouse_receipt",
        }:
            raise ValueError(f"{context}报告数据集名称不在权威枚举中。")
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
            if row["fetch_result_status"] != "not_required" or row["is_fetch_completed"]:
                raise ValueError(f"{context}无需采集格点的执行状态不一致。")
            if row["quality_status"] != "not_applicable" or row["is_data_missing"]:
                raise ValueError(f"{context}无需采集格点的质量状态不一致。")
            if row["expected_record_count"] != 0 or row["actual_record_count"] != 0:
                raise ValueError(f"{context}无需采集格点的记录数必须为零。")
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


def read_optional_fact(table_path: pathlib.Path) -> pd.DataFrame:
    if not table_path.is_dir() or next(table_path.rglob("*.parquet"), None) is None:
        return empty_pandas(FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA)

    dataset = open_exact_dataset(
        table_path,
        FACT_PARTITIONING,
        FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
        "正式仓单事实",
    )
    table = dataset.to_table(columns=FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA.names)
    return validate_warehouse_frame(
        arrow_to_pandas(table, FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA),
        "正式路径读取的",
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
    response_exchanges = set(
        response_df["exchange"].dropna().astype(str).str.strip().str.upper()
    )
    allowed_exchanges = JQDATA_RESPONSE_EXCHANGES.get(
        grid["exchange_code"],
        {grid["exchange_code"]},
    )
    if not response_exchanges or not response_exchanges.issubset(allowed_exchanges):
        raise ValueError(
            "schema_error: JQData 仓单响应交易所与待办日历不一致；"
            f"actual={sorted(response_exchanges)}。"
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
    return validate_warehouse_frame(frame, "JQData 转换后的")


# ## 完整格点与自动待办
# 
# 仓单明细行数无法事先精确预测，因此事实目录中“有几行”不能单独证明一次可变长度响应完整。
# 只有报告日历完成状态、正式事实复读计数和状态审计字段共同一致，格点才从自动待办中扣除。

# In[ ]:


def grid_count_map(frame: pd.DataFrame) -> dict[tuple[object, ...], int]:
    if frame.empty:
        return {}
    counts = frame.groupby(GRID_COLUMNS, dropna=False).size()
    return {tuple(key): int(value) for key, value in counts.items()}


def calendar_grid_is_complete(
    row: dict[str, object],
    actual_fact_count: int,
) -> bool:
    if not row["is_fetch_required"] or not row["is_fetch_completed"]:
        return False
    if row["actual_record_count"] != actual_fact_count:
        return False
    if not row["fetch_run_id"] or row["fetch_completed_at"] is None or row["quality_checked_at"] is None:
        return False

    if actual_fact_count > 0:
        return (
            row["fetch_result_status"] == "success"
            and not row["is_data_missing"]
            and row["expected_record_count"] == 1
            and row["quality_status"] in {"passed", "warning"}
        )
    return (
        row["fetch_result_status"] == "empty_confirmed"
        and row["is_data_missing"]
        and row["expected_record_count"] == 1
        and row["quality_status"] == "warning"
    )


def pending_report_grids(
    calendar_df: pd.DataFrame,
    fact_df: pd.DataFrame,
    start_date: date | None,
    end_date: date | None,
) -> tuple[pd.DataFrame, int]:
    relevant_mask = (
        calendar_df["dataset_name"].eq("warehouse_receipt")
        & calendar_df["is_fetch_required"].eq(True)
    )
    if start_date is not None:
        relevant_mask &= calendar_df["trading_date"].ge(start_date)
        relevant_mask &= calendar_df["trading_date"].le(end_date)
    relevant_df = calendar_df.loc[relevant_mask].copy()

    calendar_rows = pandas_to_arrow(
        relevant_df.loc[:, FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names],
        FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
    ).to_pylist()
    fact_counts = grid_count_map(fact_df)
    pending_rows = []
    complete_count = 0

    for row in calendar_rows:
        grid_key = tuple(row[column] for column in GRID_COLUMNS)
        if calendar_grid_is_complete(row, fact_counts.get(grid_key, 0)):
            complete_count += 1
            continue
        pending_rows.append(dict(zip(GRID_COLUMNS, grid_key, strict=True)))

    pending_df = pd.DataFrame(pending_rows, columns=GRID_COLUMNS)
    if not pending_df.empty:
        pending_df["year"] = pending_df["trading_date"].map(lambda value: value.year)
        pending_df["month"] = pending_df["trading_date"].map(lambda value: value.month)
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
    partition_key: tuple[object, ...],
) -> pd.DataFrame:
    partition_mask = pd.Series(True, index=existing_df.index)
    for column, value in zip(PARTITION_COLUMNS, partition_key, strict=True):
        partition_mask &= existing_df[column].eq(value)
    existing_partition_df = existing_df.loc[
        partition_mask,
        FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA.names,
    ]
    retained_df = existing_partition_df.loc[
        ~existing_partition_df["trading_date"].isin(touched_dates),
        FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA.names,
    ]
    complete_df = pd.concat([retained_df, incoming_df], ignore_index=True)
    if complete_df.empty:
        complete_df = empty_pandas(FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA)
    return validate_warehouse_frame(complete_df, "合并后的完整仓单分区")


# ## 完整叶分区 staging、正式复读与回滚
# 
# 每个事实叶分区都按“完整叶分区”提交：先写系统 staging 并复读，再备份旧叶目录、替换、
# 正式复读；失败时恢复旧分区。空响应同样可以用根级 `schema.parquet` 建立 0 行契约表。

# In[ ]:


def partition_expression(
    partition_key: tuple[object, ...],
) -> ds.Expression:
    expression = None
    for column, value in zip(PARTITION_COLUMNS, partition_key, strict=True):
        condition = ds.field(column) == value
        expression = condition if expression is None else expression & condition
    if expression is None:
        raise ValueError("分区键不得为空。")
    return expression


def commit_complete_partition(
    frame: pd.DataFrame,
    lake_root: pathlib.Path,
    partition_key: tuple[object, ...],
) -> pd.DataFrame:
    complete_df = validate_warehouse_frame(frame, "待提交完整仓单分区")
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

    silver_root = lake_root.resolve() / "silver"
    target_path = silver_root / TABLE_NAME
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
        if len(complete_table):
            ds.write_dataset(
                complete_table,
                staging_path,
                format="parquet",
                partitioning=FACT_PARTITIONING,
                existing_data_behavior="delete_matching",
                basename_template="part-{i}.parquet",
            )

        staged_dataset = open_exact_dataset(
            staging_path,
            FACT_PARTITIONING,
            FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
            "仓单 staging",
        )
        staged_table = staged_dataset.to_table(
            columns=FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA.names,
            filter=partition_expression(partition_key),
        )
        staged_df = validate_warehouse_frame(
            arrow_to_pandas(staged_table, FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA),
            "staging 完整仓单分区",
        )
        if not pandas_to_arrow(
            staged_df,
            FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
        ).equals(complete_table):
            raise ValueError("仓单 staging 完整分区内容检查失败。")

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

        committed_dataset = open_exact_dataset(
            target_path,
            FACT_PARTITIONING,
            FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
            "正式仓单事实",
        )
        committed_table = committed_dataset.to_table(
            columns=FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA.names,
            filter=partition_expression(partition_key),
        )
        committed_df = validate_warehouse_frame(
            arrow_to_pandas(
                committed_table,
                FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
            ),
            "正式路径复读的完整仓单分区",
        )
        if not pandas_to_arrow(
            committed_df,
            FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
        ).equals(complete_table):
            raise ValueError("正式仓单完整分区内容检查失败。")
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

    return committed_df


# ## 报告日历完成、失败状态与协调提交
# 
# 事实正式复读后才写 `success` 或 `empty_confirmed`。单位缺失属于可落盘但需留痕的警告；
# 请求或 Schema 错误保持未完成，并在带 `--write` 时记录失败类型，供下次自动求差继续处理。

# In[ ]:


def apply_calendar_completion(
    calendar_df: pd.DataFrame,
    grid_results: dict[tuple[object, ...], tuple[int, int]],
    fetch_run_id: str,
    completed_at: datetime,
) -> pd.DataFrame:
    updated_df = calendar_df.copy()

    for index, row in updated_df.iterrows():
        if row["dataset_name"] != "warehouse_receipt":
            continue
        grid_key = tuple(row[column] for column in GRID_COLUMNS)
        result = grid_results.get(grid_key)
        if result is None:
            continue

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

    return validate_calendar_frame(updated_df, "仓单完成状态回写后的")


def apply_calendar_failure(
    calendar_df: pd.DataFrame,
    grid_key: tuple[object, ...],
    fetch_status: str,
    failure_reason: str,
    fetch_run_id: str,
    failed_at: datetime,
    current_fact_count: int,
) -> pd.DataFrame:
    if fetch_status not in {"retryable_error", "permanent_error"}:
        raise ValueError("失败状态不在允许枚举中。")
    updated_df = calendar_df.copy()

    for index, row in updated_df.iterrows():
        row_grid_key = tuple(row[column] for column in GRID_COLUMNS)
        if row["dataset_name"] != "warehouse_receipt" or row_grid_key != grid_key:
            continue
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
        complete_df = validate_calendar_frame(
            calendar_df.loc[
                partition_mask,
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names,
            ],
            "待提交的完整报告日历分区",
        )

        # 日历和事实使用同一套完整叶分区提交语义，但保持各自独立 Schema。
        complete_table = pandas_to_arrow(
            complete_df,
            FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
        )
        silver_root = lake_root.resolve() / "silver"
        target_path = silver_root / CALENDAR_TABLE_NAME
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
            ds.write_dataset(
                complete_table,
                staging_path,
                format="parquet",
                partitioning=CALENDAR_PARTITIONING,
                existing_data_behavior="delete_matching",
                basename_template="part-{i}.parquet",
            )
            staged_dataset = open_exact_dataset(
                staging_path,
                CALENDAR_PARTITIONING,
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
                "报告日历 staging",
            )
            staged_table = staged_dataset.to_table(
                columns=FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names,
                filter=(
                    (ds.field("dataset_name") == partition_key[0])
                    & (ds.field("exchange_code") == partition_key[1])
                    & (ds.field("year") == partition_key[2])
                    & (ds.field("month") == partition_key[3])
                ),
            )
            staged_df = validate_calendar_frame(
                arrow_to_pandas(
                    staged_table,
                    FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
                ),
                "staging 完整报告日历分区",
            )
            if not pandas_to_arrow(
                staged_df,
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
            ).equals(complete_table):
                raise ValueError("报告日历 staging 内容检查失败。")

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

            committed_dataset = open_exact_dataset(
                target_path,
                CALENDAR_PARTITIONING,
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
                "正式报告日历",
            )
            committed_table = committed_dataset.to_table(
                columns=FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names,
                filter=(
                    (ds.field("dataset_name") == partition_key[0])
                    & (ds.field("exchange_code") == partition_key[1])
                    & (ds.field("year") == partition_key[2])
                    & (ds.field("month") == partition_key[3])
                ),
            )
            committed_df = validate_calendar_frame(
                arrow_to_pandas(
                    committed_table,
                    FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
                ),
                "正式路径复读的完整报告日历分区",
            )
            if not pandas_to_arrow(
                committed_df,
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
            ).equals(complete_table):
                raise ValueError("正式报告日历分区内容检查失败。")
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
# 默认从正式报告日历自动寻找全部缺口。待办按事实叶分区顺序处理；带 `--write` 时每完成一个品种月份，
# 立即提交仓单事实并回写日历，因此后续人工重启会从剩余格点继续。

# In[ ]:


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
    formal_lake_root = settings.futures_lake_root.resolve()
    resolved_lake_root = (lake_root or formal_lake_root).resolve()
    has_explicit_dates = start_date is not None or end_date is not None

    if (start_date is None) != (end_date is None):
        raise click.UsageError("--start-date 与 --end-date 必须同时提供。")
    if has_explicit_dates and write and resolved_lake_root == formal_lake_root:
        raise click.UsageError(
            "显式指定日期时禁止写入 FUTURES_LAKE_ROOT 指向的正式湖；"
            "请移除日期参数使用自动更新，或改用非正式测试湖。"
        )

    requested_start = start_date.date() if start_date is not None else None
    requested_end = end_date.date() if end_date is not None else None
    if requested_start is not None and requested_start > requested_end:
        raise click.BadParameter("起始日期不得晚于结束日期。")

    silver_root = resolved_lake_root / "silver"
    calendar_path = silver_root / CALENDAR_TABLE_NAME
    fact_path = silver_root / TABLE_NAME

    calendar_dataset = open_exact_dataset(
        calendar_path,
        CALENDAR_PARTITIONING,
        FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
        "正式报告日历",
    )
    calendar_df = validate_calendar_frame(
        arrow_to_pandas(
            calendar_dataset.to_table(
                columns=FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names
            ),
            FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
        ),
        "正式路径读取的",
    )
    fact_df = read_optional_fact(fact_path)

    pending_df, complete_count = pending_report_grids(
        calendar_df,
        fact_df,
        requested_start,
        requested_end,
    )
    mode = "explicit" if has_explicit_dates else "automatic"
    click.echo(
        f"table={TABLE_NAME}; mode={mode}; "
        f"lake_root={resolved_lake_root}; write={str(write).lower()}"
    )
    click.echo(
        "reconciliation_plan: "
        f"complete_grid_count={complete_count}; "
        f"pending_grid_count={len(pending_df)}"
    )
    if pending_df.empty:
        click.echo("仓单事实与报告日历状态已经完整一致。")
        return

    # 只有确实存在 API 待办时才认证，纯完整性检查不会消耗供应商连接。
    jqdata = authenticate_jqdata(settings.jqdata_id, settings.jqdata_secret)
    partition_groups = list(
        pending_df.groupby(
            ["exchange_code", "underlying_code", "year", "month"],
            sort=True,
        )
    )
    click.echo(f"pending_partitions={len(partition_groups)}")

    total_rows = 0
    processed_grid_count = 0

    for group_number, (raw_partition_key, group_df) in enumerate(
        partition_groups,
        start=1,
    ):
        partition_key = tuple(raw_partition_key)
        updated_at = datetime.now(timezone.utc)
        batch_id = uuid.uuid4().hex
        frames = []

        click.echo(
            f"partition_start: {group_number}/{len(partition_groups)}; "
            f"key={partition_key}; grids={len(group_df)}"
        )
        for grid in group_df[GRID_COLUMNS].to_dict("records"):
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
                    current_fact_count = grid_count_map(fact_df).get(grid_key, 0)
                    calendar_df = apply_calendar_failure(
                        calendar_df,
                        grid_key,
                        fetch_status,
                        f"仓单采集失败：{message}",
                        batch_id,
                        datetime.now(timezone.utc),
                        current_fact_count,
                    )
                    commit_calendar_partitions(
                        calendar_df,
                        {grid_key},
                        resolved_lake_root,
                    )
                raise click.ClickException(message) from error
            frames.append(grid_df)

        incoming_df = (
            validate_warehouse_frame(
                pd.concat(frames, ignore_index=True),
                "本分区 JQData 汇总后的",
            )
            if any(not frame.empty for frame in frames)
            else empty_pandas(FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA)
        )
        click.echo(
            f"api_success: key={partition_key}; rows={len(incoming_df)}"
        )
        if not write:
            total_rows += len(incoming_df)
            processed_grid_count += len(group_df)
            continue

        touched_dates = set(group_df["trading_date"].tolist())
        complete_df = full_fact_partition(
            fact_df,
            incoming_df,
            touched_dates,
            partition_key,
        )
        committed_partition_df = commit_complete_partition(
            complete_df,
            resolved_lake_root,
            partition_key,
        )

        partition_counts = grid_count_map(committed_partition_df)
        if committed_partition_df.empty:
            missing_unit_counts = {}
        else:
            missing_unit_series = (
                committed_partition_df.loc[
                    committed_partition_df["warehouse_receipt_unit"].isna()
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
        completed_at = datetime.now(timezone.utc)
        calendar_df = apply_calendar_completion(
            calendar_df,
            grid_results,
            batch_id,
            completed_at,
        )
        calendar_rows = commit_calendar_partitions(
            calendar_df,
            set(grid_results),
            resolved_lake_root,
        )

        # 后续分区继续使用本次已经提交的正式仓单内容。
        fact_df = pd.concat([
            fact_df.loc[
                ~(
                    fact_df[PARTITION_COLUMNS]
                    .apply(tuple, axis=1)
                    .isin({partition_key})
                )
            ],
            committed_partition_df,
        ], ignore_index=True)

        total_rows += len(incoming_df)
        processed_grid_count += len(group_df)
        click.echo(
            f"partition_committed: key={partition_key}; "
            f"calendar_rows={calendar_rows}; grids={len(group_df)}"
        )

    if write:
        # 最终复读事实与日历，并确认本次选择范围内已经没有缺口。
        final_calendar_dataset = open_exact_dataset(
            calendar_path,
            CALENDAR_PARTITIONING,
            FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
            "最终正式报告日历",
        )
        final_calendar_df = validate_calendar_frame(
            arrow_to_pandas(
                final_calendar_dataset.to_table(
                    columns=FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names
                ),
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
            ),
            "最终正式",
        )
        final_fact_df = read_optional_fact(fact_path)
        remaining_df, _ = pending_report_grids(
            final_calendar_df,
            final_fact_df,
            requested_start,
            requested_end,
        )
        if not remaining_df.empty:
            raise RuntimeError("仓单提交后仍存在本次选择范围内的未完成格点。")

    click.echo(
        f"finished: grids={processed_grid_count}; rows={total_rows}; "
        f"write={str(write).lower()}"
    )


# ## Notebook 与脚本运行入口

# In[ ]:


if "ipykernel" in sys.modules:
    # Notebook 默认执行正式湖自动 dry-run；带写入的测试必须显式使用非正式湖。
    main.main(
        args=[],
        prog_name="c03_warehouse_receipt",
        standalone_mode=False,
    )
elif __name__ == "__main__":
    main()

