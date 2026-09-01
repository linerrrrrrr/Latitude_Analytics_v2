#!/usr/bin/env python
# coding: utf-8

# # c01_macro_release_calendar
# 
# 目标表：`dim_macro_release_calendar`（利率观测与宏观发布日历维度表）。
# 
# 本入口不调用 API。它从 `.env` 的统一正式起点到北京时间当前日，按共享配置生成：
# 
# - 8 个 SHIBOR 系列的普通工作日观测格点；
# - CPI、PPI、PMI 的月末报告期格点；
# - GDP 的季末报告期格点；
# - 每个格点的版本化项目可用日与事实采集状态。
# 
# 规则未变化的既有行完整继承 c02/c03 回写状态；新增、内部缺口、系列撤销、
# 可用日到达或规则版本变化都由完整理论表与正式表比较自动识别。
# 

# ## 自动更新与写入边界
# 
# - 默认生产范围由程序生成，不接受人工截断作为正式更新方式。
# - `--write` 是唯一写入语义；不带时只计算和校验计划。
# - 显式日期必须成对出现，只能用于只读检查或非正式测试湖写入。
# - 完整 `dataset_name/year/month` 叶分区先写 staging、逐值复读，再替换正式叶分区。
# - 旧 metadata 物理兼容时整根升级；任一步失败都恢复旧正式内容。
# 

# In[ ]:


from __future__ import annotations

import hashlib
import pathlib
import shutil
import sys
import uuid
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo


# 从任意子目录运行时，先按项目唯一约定定位根目录。
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
    MACRO_RELEASE_CALENDAR_SCHEMA,
    arrow_to_pandas,
    empty_pandas,
    pandas_to_arrow,
    validate_arrow_table,
)
from config.macro_release_entities import (
    AVAILABILITY_RULE_VERSION,
    MACRO_RELEASE_CONFIG_VERSION,
    MACRO_RELEASE_SERIES,
    MACRO_RELEASE_SERIES_BY_KEY,
    MacroReleaseSeries,
    expected_available_date as calculate_expected_available_date,
    is_valid_report_date,
)
from config.settings import settings


# ## Schema 契约呈现
# 
# 本表没有直接上游维度表，因此只展示当前权威 Schema。浏览界面不读取湖仓、
# 不调用 API，也不定义第二份表名、字段或分区语义。
# 

# In[ ]:


if "ipykernel" in sys.modules:
    from config.notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        MACRO_RELEASE_CALENDAR_SCHEMA,
    ])


# ## 表名、主键、Hive 分区与状态边界
# 
# 表名、主键和分区只从权威 Schema metadata 解码一次。共享配置负责 25 个系列、
# 来源列、理论频率和可用日规则；当前 Notebook 不复制这些列表。
# 

# In[ ]:


# 表名、主键和 Hive 分区是完整理论比较与完整叶提交的稳定边界。
TABLE_NAME = MACRO_RELEASE_CALENDAR_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # 利率观测与宏观发布日历表。
PRIMARY_KEY = MACRO_RELEASE_CALENDAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")  # 数据集类型—系列—报告/观测日唯一标识。
PARTITION_COLUMNS = MACRO_RELEASE_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 数据集类型—年—月完整叶分区。

POLICY_COLUMNS = [
    "expected_available_date",  # 项目规则推定的最早可用日。
    "is_fetch_required",  # 当前是否已经进入事实采集水位。
    "requirement_reason",  # 配置和可用日规则的版本化中文说明。
]
STATE_COLUMNS = [
    "is_fetch_completed",  # 事实请求、提交和正式复读是否完成。
    "fetch_result_status",  # 最近一次事实采集结果。
    "is_data_missing",  # 已确认对应事实格点为空。
    "actual_record_count",  # 对应正式事实复读行数，只允许 0 或 1。
    "quality_status",  # 格点综合质检状态。
    "quality_reason",  # 质检结论中文说明。
    "fetch_run_id",  # 最近一次事实采集批次号。
    "fetch_completed_at",  # 最近一次事实完成时间。
    "quality_checked_at",  # 最近一次事实质检时间。
    "updated_at",  # 本行政策或状态最后变化时间。
]

FETCH_RESULT_STATUSES = {
    "pending",
    "success",
    "empty_confirmed",
    "retryable_error",
    "permanent_error",
    "not_required",
}
QUALITY_STATUSES = {
    "pending",
    "passed",
    "warning",
    "failed",
    "not_applicable",
}

CALENDAR_PARTITIONING = ds.partitioning(
    pa.schema([
        MACRO_RELEASE_CALENDAR_SCHEMA.field(name)
        for name in PARTITION_COLUMNS
    ]),
    flavor="hive",
)


def requirement_reason_text(
    series: MacroReleaseSeries,
    available_date: date,
    is_fetch_required: bool,
) -> str:
    # 原因文本包含配置与规则版本；任一版本变化都会显式重置下游状态。
    prefix = (
        f"宏观系列配置 v{MACRO_RELEASE_CONFIG_VERSION}；"
        f"可用日规则 {AVAILABILITY_RULE_VERSION}；"
        f"{series.series_name_zh}。"
    )
    if is_fetch_required:
        return (
            f"{prefix}项目规则可用日 {available_date.isoformat()} 已到，"
            "进入事实采集水位。"
        )
    return (
        f"{prefix}项目规则可用日 {available_date.isoformat()} 尚未到，"
        "保留理论格点但不请求。"
    )


# ## 契约化读取与 metadata 兼容边界
# 
# 正式、staging 和提交后路径必须逐 fragment 精确匹配当前 metadata。只有读取旧表准备
# 整根迁移时，才允许 metadata 过期但字段名、顺序、类型和 nullable 完全兼容。
# 

# In[ ]:


def reconstructed_schema(
    dataset: ds.Dataset,
    schema: pa.Schema,
) -> pa.Schema:
    # Hive 分区列由目录补回；按权威顺序重建后再比较整表 metadata。
    try:
        fields = [dataset.schema.field(name) for name in schema.names]
    except KeyError as error:
        raise TypeError("数据集缺少权威 Schema 字段。") from error

    return pa.schema(fields, metadata=dataset.schema.metadata)


def physical_schema_matches(
    actual_schema: pa.Schema,
    expected_schema: pa.Schema,
) -> bool:
    # metadata 迁移不得掩盖字段、类型或 nullable 的物理破坏。
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


def dataset_has_exact_schema_metadata(
    dataset: ds.Dataset,
    schema: pa.Schema,
) -> bool:
    if not reconstructed_schema(dataset, schema).equals(
        schema,
        check_metadata=True,
    ):
        return False

    expected_file_schema = pa.schema(
        [
            field
            for field in schema
            if field.name not in PARTITION_COLUMNS
        ],
        metadata=schema.metadata,
    )
    return all(
        fragment.physical_schema.equals(
            expected_file_schema,
            check_metadata=True,
        )
        for fragment in dataset.get_fragments()
    )


def open_compatible_dataset(
    table_path: pathlib.Path,
    label: str,
) -> tuple[ds.Dataset, bool]:
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
        partitioning=CALENDAR_PARTITIONING,
    )
    if not physical_schema_matches(
        reconstructed_schema(
            dataset,
            MACRO_RELEASE_CALENDAR_SCHEMA,
        ),
        MACRO_RELEASE_CALENDAR_SCHEMA,
    ):
        raise TypeError(
            f"{label}物理字段、顺序、类型或 nullable 与权威契约不兼容。"
        )

    expected_file_schema = pa.schema([
        field
        for field in MACRO_RELEASE_CALENDAR_SCHEMA
        if field.name not in PARTITION_COLUMNS
    ])
    for fragment in dataset.get_fragments():
        if not physical_schema_matches(
            pa.schema(list(fragment.physical_schema)),
            expected_file_schema,
        ):
            raise TypeError(
                f"{label}存在物理结构不兼容的 fragment：{fragment.path}"
            )

    is_exact = dataset_has_exact_schema_metadata(
        dataset,
        MACRO_RELEASE_CALENDAR_SCHEMA,
    )
    return dataset, is_exact


def open_exact_dataset(
    table_path: pathlib.Path,
    label: str,
) -> ds.Dataset:
    dataset, is_exact = open_compatible_dataset(table_path, label)
    if not is_exact:
        raise TypeError(f"{label} Schema/metadata 与权威契约不一致。")
    return dataset


def partition_expression(
    partition_key: tuple[object, ...],
) -> ds.Expression:
    expression = None
    for column, value in zip(
        PARTITION_COLUMNS,
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
        raise ValueError("宏观发布日历分区键不得为空。")
    return expression


# ## 表级业务校验
# 
# 严格模式检查当前 25 个系列、频率、可用日、调度状态、事实计数和审计字段。
# `allow_legacy_policy=True` 只用于读取物理兼容旧表：允许旧系列/规则及旧的
# `not_required` 初始化状态进入自动全表迁移，绝不用于新期望、staging 或正式复读。
# 

# In[ ]:


def validate_macro_calendar_table(
    table: pa.Table,
    context: str,
    *,
    allow_legacy_policy: bool = False,
) -> pd.DataFrame:
    checked = validate_arrow_table(
        table,
        MACRO_RELEASE_CALENDAR_SCHEMA,
    )
    frame = arrow_to_pandas(
        checked,
        MACRO_RELEASE_CALENDAR_SCHEMA,
    )

    if frame.duplicated(PRIMARY_KEY).any():
        raise ValueError(f"{context}宏观发布日历主键不唯一。")

    now_utc = datetime.now(timezone.utc)
    for row in checked.to_pylist():
        if row["dataset_name"] not in {"interest_rate", "macro_release"}:
            raise ValueError(f"{context}dataset_name 不在权威枚举中。")
        if row["fetch_result_status"] not in FETCH_RESULT_STATUSES:
            raise ValueError(f"{context}fetch_result_status 不在允许枚举中。")
        if row["quality_status"] not in QUALITY_STATUSES:
            raise ValueError(f"{context}quality_status 不在允许枚举中。")
        if not str(row["requirement_reason"]).strip():
            raise ValueError(f"{context}requirement_reason 不得为空。")
        if not str(row["quality_reason"]).strip():
            raise ValueError(f"{context}quality_reason 不得为空。")
        if (
            row["report_date"].year != row["year"]
            or row["report_date"].month != row["month"]
        ):
            raise ValueError(f"{context}year/month 与报告/观测日期不一致。")
        if row["actual_record_count"] not in {0, 1}:
            raise ValueError(f"{context}正式事实复读计数只允许 0 或 1。")
        if row["expected_available_date"] < row["report_date"]:
            raise ValueError(f"{context}项目可用日不得早于报告/观测日期。")
        if row["updated_at"] > now_utc:
            raise ValueError(f"{context}updated_at 不得晚于当前 UTC 时间。")

        for timestamp_name in [
            "fetch_completed_at",
            "quality_checked_at",
        ]:
            timestamp_value = row[timestamp_name]
            if timestamp_value is not None and timestamp_value > now_utc:
                raise ValueError(
                    f"{context}{timestamp_name} 不得晚于当前 UTC 时间。"
                )

        completed_status = row["fetch_result_status"] in {
            "success",
            "empty_confirmed",
        }
        if row["is_fetch_completed"] != completed_status:
            raise ValueError(f"{context}完成布尔值与采集结果状态不一致。")
        if row["is_fetch_completed"]:
            if (
                not row["fetch_run_id"]
                or row["fetch_completed_at"] is None
                or row["quality_checked_at"] is None
            ):
                raise ValueError(f"{context}完成格点缺少批次或审计时间。")
        elif row["fetch_completed_at"] is not None:
            raise ValueError(f"{context}未完成格点不得具有完成时间。")

        if row["fetch_result_status"] == "success":
            if row["actual_record_count"] != 1 or row["is_data_missing"]:
                raise ValueError(f"{context}success 必须正式复读 1 行且不得缺失。")
        elif row["fetch_result_status"] == "empty_confirmed":
            if row["actual_record_count"] != 0 or not row["is_data_missing"]:
                raise ValueError(f"{context}确认空必须为 0 行且标记缺失。")
        else:
            if row["actual_record_count"] != 0 or row["is_data_missing"]:
                raise ValueError(f"{context}未完成状态不得具有事实行或缺失结论。")

        if not allow_legacy_policy:
            key = (row["dataset_name"], row["series_code"])
            series = MACRO_RELEASE_SERIES_BY_KEY.get(key)
            if series is None:
                raise ValueError(f"{context}系列未命中共享宏观配置：{key}")
            if not is_valid_report_date(series, row["report_date"]):
                raise ValueError(
                    f"{context}{row['series_code']} 的报告/观测日不符合理论频率。"
                )

            expected_date = calculate_expected_available_date(
                series,
                row["report_date"],
            )
            if row["expected_available_date"] != expected_date:
                raise ValueError(
                    f"{context}{row['series_code']} 的项目可用日不符合版本化规则。"
                )
            expected_reason = requirement_reason_text(
                series,
                expected_date,
                row["is_fetch_required"],
            )
            if row["requirement_reason"] != expected_reason:
                raise ValueError(
                    f"{context}{row['series_code']} 的调度原因与当前版本不一致。"
                )

            status = row["fetch_result_status"]
            if not row["is_fetch_required"]:
                if (
                    status != "not_required"
                    or row["is_fetch_completed"]
                    or row["quality_status"] != "not_applicable"
                    or any(
                        row[name] is not None
                        for name in [
                            "fetch_run_id",
                            "fetch_completed_at",
                            "quality_checked_at",
                        ]
                    )
                ):
                    raise ValueError(
                        f"{context}尚未到可用日格点的状态或审计值不自洽。"
                    )
            elif status == "not_required":
                raise ValueError(f"{context}已到可用日格点不得标为 not_required。")
            elif status == "pending":
                if (
                    row["quality_status"] != "pending"
                    or row["fetch_run_id"] is not None
                    or row["quality_checked_at"] is not None
                ):
                    raise ValueError(f"{context}pending 格点状态不自洽。")
            elif status == "success":
                if row["quality_status"] not in {"passed", "warning"}:
                    raise ValueError(f"{context}success 格点必须 passed 或 warning。")
            elif status == "empty_confirmed":
                if row["quality_status"] != "warning":
                    raise ValueError(f"{context}确认空格点必须记录 warning。")
            elif (
                row["quality_status"] != "failed"
                or not row["fetch_run_id"]
                or row["quality_checked_at"] is None
            ):
                raise ValueError(f"{context}失败格点缺少 failed 结论或审计值。")

    return frame.sort_values(PRIMARY_KEY).reset_index(drop=True)


# ## 生成完整理论格点并继承事实状态
# 
# 统一起点与当前可见日只限定理论报告/观测日期，不冒充 API 覆盖范围。
# `expected_available_date` 已到时才进入 required 水位。只有三个政策字段逐值不变时，
# 才继承事实生产者回写的完整状态和原 `updated_at`。
# 

# In[ ]:


def build_expected_calendar(
    start_date: date,
    end_date: date,
    existing_df: pd.DataFrame,
    visible_date: date,
    updated_at: datetime,
) -> pd.DataFrame:
    existing_rows_by_key = {
        tuple(row[name] for name in PRIMARY_KEY): row
        for row in pandas_to_arrow(
            existing_df.loc[:, MACRO_RELEASE_CALENDAR_SCHEMA.names],
            MACRO_RELEASE_CALENDAR_SCHEMA,
        ).to_pylist()
    }

    expected_rows = []
    if start_date <= end_date:
        for series in MACRO_RELEASE_SERIES:
            if series.frequency == "business_day":
                report_dates = pd.bdate_range(start_date, end_date).date
            elif series.frequency == "month_end":
                report_dates = pd.date_range(
                    start_date,
                    end_date,
                    freq="ME",
                ).date
            else:
                report_dates = pd.date_range(
                    start_date,
                    end_date,
                    freq="QE-DEC",
                ).date

            for report_date in report_dates:
                available_date = calculate_expected_available_date(
                    series,
                    report_date,
                )
                is_fetch_required = available_date <= visible_date

                if is_fetch_required:
                    fetch_result_status = "pending"
                    quality_status = "pending"
                    quality_reason = (
                        "已进入事实采集水位，等待对应事实生产者提交并正式复读。"
                    )
                else:
                    fetch_result_status = "not_required"
                    quality_status = "not_applicable"
                    quality_reason = (
                        "项目规则可用日尚未到；保留理论格点但当前不请求。"
                    )

                row = {
                    "dataset_name": series.dataset_name,
                    "series_code": series.series_code,
                    "report_date": report_date,
                    "expected_available_date": available_date,
                    "is_fetch_required": is_fetch_required,
                    "requirement_reason": requirement_reason_text(
                        series,
                        available_date,
                        is_fetch_required,
                    ),
                    "is_fetch_completed": False,
                    "fetch_result_status": fetch_result_status,
                    "is_data_missing": False,
                    "actual_record_count": 0,
                    "quality_status": quality_status,
                    "quality_reason": quality_reason,
                    "fetch_run_id": None,
                    "fetch_completed_at": None,
                    "quality_checked_at": None,
                    "updated_at": updated_at,
                    "year": report_date.year,
                    "month": report_date.month,
                }

                key = tuple(row[name] for name in PRIMARY_KEY)
                existing_row = existing_rows_by_key.get(key)
                if existing_row is not None and all(
                    existing_row[name] == row[name]
                    for name in POLICY_COLUMNS
                ):
                    for name in STATE_COLUMNS:
                        row[name] = existing_row[name]

                expected_rows.append(row)

    if expected_rows:
        expected_df = pd.DataFrame(
            expected_rows,
            columns=MACRO_RELEASE_CALENDAR_SCHEMA.names,
        )
    else:
        expected_df = empty_pandas(MACRO_RELEASE_CALENDAR_SCHEMA)

    return validate_macro_calendar_table(
        pandas_to_arrow(
            expected_df.loc[:, MACRO_RELEASE_CALENDAR_SCHEMA.names],
            MACRO_RELEASE_CALENDAR_SCHEMA,
        ),
        "期望",
    )


# ## 完整内容比较与变更分区
# 
# 按权威字段顺序和主键排序生成 Arrow IPC 摘要。比较的是完整叶内容，
# 因此能够同时发现新增、内部缺口、规则变化、状态回写和已撤销系列。
# 

# In[ ]:


def table_digest(frame: pd.DataFrame) -> str:
    ordered_df = frame.sort_values(PRIMARY_KEY).reset_index(drop=True)
    source_table = pandas_to_arrow(
        ordered_df.loc[:, MACRO_RELEASE_CALENDAR_SCHEMA.names],
        MACRO_RELEASE_CALENDAR_SCHEMA,
    )
    stable_table = pa.Table.from_pylist(
        source_table.to_pylist(),
        schema=MACRO_RELEASE_CALENDAR_SCHEMA,
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(
        sink,
        MACRO_RELEASE_CALENDAR_SCHEMA,
    ) as writer:
        writer.write_table(stable_table)
    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()


def changed_partition_keys(
    expected_df: pd.DataFrame,
    existing_df: pd.DataFrame,
) -> list[tuple[object, ...]]:
    expected_keys = set(
        expected_df[PARTITION_COLUMNS].itertuples(
            index=False,
            name=None,
        )
    )
    existing_keys = set(
        existing_df[PARTITION_COLUMNS].itertuples(
            index=False,
            name=None,
        )
    )
    changed_keys = []

    for partition_key in sorted(expected_keys | existing_keys):
        expected_mask = pd.Series(True, index=expected_df.index)
        existing_mask = pd.Series(True, index=existing_df.index)
        for column, value in zip(
            PARTITION_COLUMNS,
            partition_key,
            strict=True,
        ):
            expected_mask &= expected_df[column].eq(value)
            existing_mask &= existing_df[column].eq(value)

        expected_partition_df = expected_df.loc[expected_mask]
        existing_partition_df = existing_df.loc[existing_mask]
        expected_digest = (
            table_digest(expected_partition_df)
            if not expected_partition_df.empty
            else None
        )
        existing_digest = (
            table_digest(existing_partition_df)
            if not existing_partition_df.empty
            else None
        )
        if expected_digest != existing_digest:
            changed_keys.append(partition_key)

    return changed_keys


# ## 完整叶 staging、正式复读与失败回滚
# 
# 普通更新只替换变化叶分区；新表、空表或 metadata 迁移执行整根交换。
# 旧内容移走和新内容安装分别记录状态，第一步移动失败时绝不碰仍在原位的正式表。
# 

# In[ ]:


def commit_partitions(
    expected_df: pd.DataFrame,
    partition_keys: list[tuple[object, ...]],
    lake_root: pathlib.Path,
    *,
    force_full_swap: bool = False,
) -> int:
    if not partition_keys and not force_full_swap:
        return 0

    expected_df = validate_macro_calendar_table(
        pandas_to_arrow(
            expected_df.loc[:, MACRO_RELEASE_CALENDAR_SCHEMA.names],
            MACRO_RELEASE_CALENDAR_SCHEMA,
        ),
        "待提交完整",
    )
    expected_digest = table_digest(expected_df)

    silver_root = lake_root.resolve() / "silver"
    target_path = silver_root / TABLE_NAME
    run_id = uuid.uuid4().hex
    staging_path = silver_root / f".{TABLE_NAME}.staging-{run_id}"
    backup_path = silver_root / f".{TABLE_NAME}.backup-{run_id}"
    quarantine_path = silver_root / f".{TABLE_NAME}.failed-{run_id}"

    for managed_path in [
        target_path,
        staging_path,
        backup_path,
        quarantine_path,
    ]:
        if not managed_path.resolve().is_relative_to(silver_root):
            raise ValueError(f"数据集路径越出 silver 根目录：{managed_path}")

    silver_root.mkdir(parents=True, exist_ok=True)
    try:
        staging_path.mkdir(parents=True, exist_ok=False)
        file_schema = pa.schema(
            [
                field
                for field in MACRO_RELEASE_CALENDAR_SCHEMA
                if field.name not in PARTITION_COLUMNS
            ],
            metadata=MACRO_RELEASE_CALENDAR_SCHEMA.metadata,
        )
        pq.write_table(
            pa.Table.from_batches([], schema=file_schema),
            staging_path / "schema.parquet",
        )

        changed_row_mask = pd.Series(False, index=expected_df.index)
        for partition_key in partition_keys:
            partition_mask = pd.Series(True, index=expected_df.index)
            for column, value in zip(
                PARTITION_COLUMNS,
                partition_key,
                strict=True,
            ):
                partition_mask &= expected_df[column].eq(value)
            changed_row_mask |= partition_mask

        changed_rows_df = (
            expected_df
            if force_full_swap
            else expected_df.loc[changed_row_mask]
        )
        if not changed_rows_df.empty:
            ds.write_dataset(
                pandas_to_arrow(
                    changed_rows_df.loc[
                        :, MACRO_RELEASE_CALENDAR_SCHEMA.names
                    ],
                    MACRO_RELEASE_CALENDAR_SCHEMA,
                ),
                staging_path,
                format="parquet",
                partitioning=CALENDAR_PARTITIONING,
                existing_data_behavior="delete_matching",
                basename_template="part-{i}.parquet",
            )

        staged_dataset = open_exact_dataset(
            staging_path,
            "宏观发布日历 staging",
        )
        staged_df = validate_macro_calendar_table(
            staged_dataset.to_table(
                columns=MACRO_RELEASE_CALENDAR_SCHEMA.names
            ),
            "staging ",
        )
        if len(staged_df) != len(changed_rows_df):
            raise ValueError("staging 触达行数与完整分区计划不一致。")
        if force_full_swap and table_digest(staged_df) != expected_digest:
            raise ValueError("metadata 迁移 staging 与完整期望表不一致。")

        for partition_key in partition_keys:
            expected_mask = pd.Series(True, index=expected_df.index)
            for column, value in zip(
                PARTITION_COLUMNS,
                partition_key,
                strict=True,
            ):
                expected_mask &= expected_df[column].eq(value)
            expected_partition_df = expected_df.loc[expected_mask]
            staged_partition_df = validate_macro_calendar_table(
                staged_dataset.to_table(
                    columns=MACRO_RELEASE_CALENDAR_SCHEMA.names,
                    filter=partition_expression(partition_key),
                ),
                "staging 叶分区",
            )
            if len(staged_partition_df) != len(expected_partition_df):
                raise ValueError("staging 叶分区行数与期望不一致。")
            if (
                not expected_partition_df.empty
                and table_digest(staged_partition_df)
                != table_digest(expected_partition_df)
            ):
                raise ValueError("staging 叶分区内容与期望不一致。")
    except Exception:
        shutil.rmtree(staging_path, ignore_errors=True)
        raise

    target_had_existing = target_path.exists()
    full_swap = (
        force_full_swap
        or expected_df.empty
        or not target_had_existing
    )
    moved_partitions = []
    old_target_moved = False
    new_target_installed = False
    cleanup_recovery_paths = True

    try:
        if full_swap:
            if target_had_existing:
                shutil.move(str(target_path), str(backup_path))
                old_target_moved = True
            shutil.move(str(staging_path), str(target_path))
            new_target_installed = True
        else:
            backup_path.mkdir(parents=True, exist_ok=False)
            for partition_key in partition_keys:
                relative_path = pathlib.Path(*[
                    f"{name}={value}"
                    for name, value in zip(
                        PARTITION_COLUMNS,
                        partition_key,
                        strict=True,
                    )
                ])
                source_path = staging_path / relative_path
                destination_path = target_path / relative_path
                saved_path = backup_path / relative_path

                expected_mask = pd.Series(True, index=expected_df.index)
                for column, value in zip(
                    PARTITION_COLUMNS,
                    partition_key,
                    strict=True,
                ):
                    expected_mask &= expected_df[column].eq(value)
                should_exist = bool(expected_mask.any())
                if should_exist != source_path.is_dir():
                    raise FileNotFoundError(
                        "staging 叶分区存在性与期望不一致："
                        f"{relative_path}"
                    )

                had_existing = destination_path.exists()
                if had_existing:
                    saved_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(destination_path), str(saved_path))

                # 在第二次 move 前保存回滚事实，避免新分区安装失败后丢旧叶。
                moved_partitions.append((
                    destination_path,
                    saved_path,
                    relative_path,
                    had_existing,
                ))
                if should_exist:
                    destination_path.parent.mkdir(
                        parents=True,
                        exist_ok=True,
                    )
                    shutil.move(str(source_path), str(destination_path))

        committed_dataset = open_exact_dataset(
            target_path,
            "正式宏观发布日历",
        )
        committed_df = validate_macro_calendar_table(
            committed_dataset.to_table(
                columns=MACRO_RELEASE_CALENDAR_SCHEMA.names
            ),
            "正式路径复读的",
        )
        if len(committed_df) != len(expected_df):
            raise ValueError("正式宏观发布日历总行数与期望不一致。")
        if table_digest(committed_df) != expected_digest:
            raise ValueError("正式宏观发布日历内容与期望不一致。")
    except Exception as commit_error:
        rollback_errors = []
        try:
            if full_swap:
                if (
                    new_target_installed
                    or old_target_moved
                    or not target_had_existing
                ) and target_path.exists():
                    shutil.move(str(target_path), str(quarantine_path))
                if old_target_moved and backup_path.exists():
                    shutil.move(str(backup_path), str(target_path))
            else:
                for (
                    destination_path,
                    saved_path,
                    relative_path,
                    had_existing,
                ) in reversed(moved_partitions):
                    if destination_path.exists():
                        failed_path = quarantine_path / relative_path
                        failed_path.parent.mkdir(
                            parents=True,
                            exist_ok=True,
                        )
                        shutil.move(
                            str(destination_path),
                            str(failed_path),
                        )
                    if had_existing and saved_path.exists():
                        destination_path.parent.mkdir(
                            parents=True,
                            exist_ok=True,
                        )
                        shutil.move(
                            str(saved_path),
                            str(destination_path),
                        )
        except Exception as rollback_error:
            rollback_errors.append(str(rollback_error))

        if rollback_errors:
            cleanup_recovery_paths = False
            raise RuntimeError(
                "宏观发布日历提交失败且自动回滚未完成；"
                f"请检查 {backup_path} 与 {quarantine_path}。"
            ) from commit_error
        raise
    finally:
        if cleanup_recovery_paths:
            shutil.rmtree(staging_path, ignore_errors=True)
            shutil.rmtree(backup_path, ignore_errors=True)
            shutil.rmtree(quarantine_path, ignore_errors=True)

    return len(changed_rows_df)


# ## CLI：自动理论水位、完整比较与是否写入
# 
# 默认使用 `.env` 的起点和正式湖。显式范围只改变测试/只读作用域；旧策略行必须通过
# 无日期自动模式完成全表迁移，避免把范围外旧语义重新提交为当前契约。
# 

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

    requested_start_date = (
        start_date.date()
        if start_date is not None
        else None
    )
    requested_end_date = (
        end_date.date()
        if end_date is not None
        else None
    )
    if (
        requested_start_date is not None
        and requested_start_date > requested_end_date
    ):
        raise click.BadParameter("起始日期不得晚于结束日期。")

    policy_start_date = settings.futures_data_start_date
    visible_date = datetime.now(ZoneInfo("Asia/Shanghai")).date()
    if policy_start_date > visible_date:
        raise ValueError(
            "FUTURES_DATA_START_DATE 不得晚于北京时间当前日期。"
        )

    generation_start_date = max(
        policy_start_date,
        requested_start_date or policy_start_date,
    )
    generation_end_date = min(
        visible_date,
        requested_end_date or visible_date,
    )

    silver_root = resolved_lake_root / "silver"
    target_path = silver_root / TABLE_NAME
    target_dataset_exists = (
        target_path.is_dir()
        and next(target_path.rglob("*.parquet"), None) is not None
    )
    metadata_upgrade_required = False
    existing_policy_is_current = True

    if target_dataset_exists:
        existing_dataset, schema_is_exact = open_compatible_dataset(
            target_path,
            "现有正式宏观发布日历",
        )
        metadata_upgrade_required = not schema_is_exact
        existing_table = existing_dataset.to_table(
            columns=MACRO_RELEASE_CALENDAR_SCHEMA.names
        )
        existing_df = validate_macro_calendar_table(
            existing_table,
            "现有正式",
            allow_legacy_policy=True,
        )
        try:
            validate_macro_calendar_table(
                existing_table,
                "现有正式当前策略",
            )
        except ValueError:
            existing_policy_is_current = False
    else:
        existing_df = empty_pandas(MACRO_RELEASE_CALENDAR_SCHEMA)

    if has_explicit_dates and not existing_policy_is_current:
        raise click.UsageError(
            "现有宏观发布日历包含旧系列、可用日规则或旧状态语义；"
            "请先移除日期参数执行一次完整自动迁移。"
        )
    if not existing_policy_is_current and not metadata_upgrade_required:
        raise ValueError(
            "当前 metadata 下的宏观发布日历违反当前策略或状态契约；"
            "不得把正式数据损坏静默当作旧契约迁移。"
        )

    if has_explicit_dates:
        in_scope_mask = (
            existing_df["report_date"].ge(requested_start_date)
            & existing_df["report_date"].le(requested_end_date)
        )
        scoped_existing_df = existing_df.loc[in_scope_mask].copy()
        outside_scope_df = existing_df.loc[~in_scope_mask].copy()
    else:
        scoped_existing_df = existing_df
        outside_scope_df = empty_pandas(
            MACRO_RELEASE_CALENDAR_SCHEMA
        )

    # 只有当前策略严格有效的行才允许继承事实状态；旧 metadata/旧策略整表重置。
    inheritance_existing_df = (
        scoped_existing_df
        if existing_policy_is_current
        else empty_pandas(MACRO_RELEASE_CALENDAR_SCHEMA)
    )

    expected_scope_df = build_expected_calendar(
        generation_start_date,
        generation_end_date,
        inheritance_existing_df,
        visible_date,
        datetime.now(timezone.utc),
    )

    if has_explicit_dates:
        expected_full_df = pd.concat(
            [outside_scope_df, expected_scope_df],
            ignore_index=True,
        )
        expected_full_df = validate_macro_calendar_table(
            pandas_to_arrow(
                expected_full_df.loc[
                    :, MACRO_RELEASE_CALENDAR_SCHEMA.names
                ],
                MACRO_RELEASE_CALENDAR_SCHEMA,
            ),
            "显式范围合并后的完整",
        )
    else:
        expected_full_df = expected_scope_df

    existing_rows_by_key = {
        tuple(row[name] for name in PRIMARY_KEY): row
        for row in pandas_to_arrow(
            existing_df.loc[:, MACRO_RELEASE_CALENDAR_SCHEMA.names],
            MACRO_RELEASE_CALENDAR_SCHEMA,
        ).to_pylist()
    }
    expected_rows_by_key = {
        tuple(row[name] for name in PRIMARY_KEY): row
        for row in pandas_to_arrow(
            expected_full_df.loc[
                :, MACRO_RELEASE_CALENDAR_SCHEMA.names
            ],
            MACRO_RELEASE_CALENDAR_SCHEMA,
        ).to_pylist()
    }
    existing_keys = set(existing_rows_by_key)
    expected_keys = set(expected_rows_by_key)
    new_grid_count = len(expected_keys - existing_keys)
    retired_grid_count = len(existing_keys - expected_keys)
    policy_reset_count = sum(
        any(
            existing_rows_by_key[key][name]
            != expected_rows_by_key[key][name]
            for name in POLICY_COLUMNS
        )
        for key in existing_keys & expected_keys
    )

    partition_keys = changed_partition_keys(
        expected_full_df,
        existing_df,
    )
    if metadata_upgrade_required:
        partition_keys = sorted(
            set(
                expected_full_df[PARTITION_COLUMNS].itertuples(
                    index=False,
                    name=None,
                )
            )
            | set(
                existing_df[PARTITION_COLUMNS].itertuples(
                    index=False,
                    name=None,
                )
            )
        )

    mode = "explicit" if has_explicit_dates else "automatic"
    click.echo(
        f"table={TABLE_NAME}; mode={mode}; "
        f"lake_root={resolved_lake_root}; write={str(write).lower()}"
    )
    click.echo(
        "reconciliation_plan: "
        f"expected_rows={len(expected_full_df)}; "
        f"existing_rows={len(existing_df)}; "
        f"new_grids={new_grid_count}; "
        f"retired_grids={retired_grid_count}; "
        f"policy_resets={policy_reset_count}; "
        f"changed_partitions={len(partition_keys)}; "
        f"initial_dataset_required={str(not target_dataset_exists).lower()}; "
        f"metadata_upgrade_required={str(metadata_upgrade_required).lower()}"
    )

    if (
        not partition_keys
        and not metadata_upgrade_required
        and target_dataset_exists
    ):
        click.echo("宏观发布日历已经与当前理论水位和下游状态完整一致。")
        return

    if write:
        full_root_swap_used = (
            metadata_upgrade_required
            or not target_dataset_exists
            or expected_full_df.empty
        )
        committed_rows = commit_partitions(
            expected_full_df,
            partition_keys,
            resolved_lake_root,
            force_full_swap=(
                metadata_upgrade_required or not target_dataset_exists
            ),
        )
        click.echo(
            f"committed: rows={committed_rows}; "
            f"full_root_swap={str(full_root_swap_used).lower()}"
        )
    else:
        click.echo("dry_run: 未写入数据湖。")


# ## Notebook 与脚本运行入口
# 
# Notebook 默认执行正式湖只读计划；命令行只有显式 `--write` 才提交。
# 

# In[ ]:


if "ipykernel" in sys.modules:
    main.main(
        args=[],
        prog_name="c01_macro_release_calendar",
        standalone_mode=False,
    )
elif __name__ == "__main__":
    main()

