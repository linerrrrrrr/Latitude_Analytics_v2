#!/usr/bin/env python
# coding: utf-8

# # c01 期货交易所报告采集日历
# 
# 目标表：`dim_futures_exchange_report_calendar`。
# 
# 本入口不调用外部 API。它读取完整的期货品种交易日历，并为每个品种交易日展开
# `position_rank`、`member_position`、`warehouse_receipt` 三类报告格点。共享期货事实采集白名单
# 和当前数据集覆盖规则只决定 `is_fetch_required`，不能删除白名单外的日历格点。已形成的
# `is_fetch_completed` 是持久完成凭证；当前政策排除不清除它，重新纳入时也不重复采集。

# ## 自动更新与写入边界
# 
# 自动模式遵循：
# 
# `上游当前有效报告格点 − 下游已经完整落盘且规则一致的格点 = 本次自动更新范围`
# 
# 空湖、尾部新增、历史内部缺口、上游格点撤销及白名单或覆盖规则变化均由同一入口处理。
# `--write` 只表达是否提交；显式日期只允许只读检查，或写入与正式湖不同的临时/测试湖。

# ## 初始化与权威 Schema

# In[ ]:


from __future__ import annotations

import hashlib
import pathlib
import shutil
import sys
import uuid
from datetime import date, datetime, timezone

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
    FUTURES_VARIETY_CALENDAR_SCHEMA,
    arrow_to_pandas,
    empty_pandas,
    pandas_to_arrow,
    validate_arrow_table,
)
from config.futures_fact_collection_policy import FUTURES_FACT_VARIETY_PAIRS
from config.settings import settings


# ## Schema 契约呈现
# 
# 本节只在交互式 Notebook 内核中呈现只读 Schema 契约。上游品种日历在前，当前报告日历在后；
# 展示实现不会读取数据湖、调用 API 或产生写入。

# In[ ]:


if "ipykernel" in sys.modules:
    from config.notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        FUTURES_VARIETY_CALENDAR_SCHEMA,
        FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
    ])


# ## 表名、主键、Hive 分区与状态列

# In[ ]:


# 三项物理契约只从权威 Schema metadata 读取一次，后续路径、排序和校验统一复用。
TABLE_NAME = FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # 期货交易所报告采集日历维度表。
PRIMARY_KEY = FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")  # 唯一标识一类报告的交易所—品种—交易日格点。
PARTITION_COLUMNS = FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 正式表的 Hive 叶分区顺序。

UPSTREAM_TABLE_NAME = FUTURES_VARIETY_CALENDAR_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # 完整期货品种交易日历维度表。
UPSTREAM_PRIMARY_KEY = FUTURES_VARIETY_CALENDAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")  # 唯一标识交易所—品种—交易日格点。
UPSTREAM_PARTITION_COLUMNS = FUTURES_VARIETY_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 上游品种日历的 Hive 叶分区顺序。

# 报告类型枚举同样读取字段 metadata，避免在业务入口维护第二份枚举定义。
DATASET_NAMES = tuple(
    FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.field("dataset_name")
    .metadata[b"enum_values_zh"]
    .decode("utf-8")
    .split("、")
)

POLICY_COLUMNS = [
    "is_fetch_required",  # 当前格点是否必须进入事实采集。
    "requirement_reason",  # 当前选择或排除规则的中文说明。
]
STATE_COLUMNS = [
    "is_fetch_completed",  # 事实提交及正式复读形成的持久完成凭证。
    "fetch_result_status",  # 最近一次采集结果状态。
    "is_data_missing",  # 应有报告但确认空响应时的缺失状态。
    "expected_record_count",  # 当前能够明确确认的最小记录数。
    "actual_record_count",  # 相应事实表正式复读的实际记录数。
    "quality_status",  # 本格点综合质量状态。
    "quality_reason",  # 本格点质量结论的中文说明。
    "fetch_run_id",  # 最近一次事实采集批次标识。
    "fetch_completed_at",  # 事实提交和正式复读完成时间。
    "quality_checked_at",  # 最近一次质量检查时间。
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
SCHEMA_IDENTITY_METADATA_KEYS = [
    b"table_name",
    b"primary_key",
    b"partition_columns",
]

REPORT_PARTITIONING = ds.partitioning(
    pa.schema([
        FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.field(name)
        for name in PARTITION_COLUMNS
    ]),
    flavor="hive",
)
UPSTREAM_PARTITIONING = ds.partitioning(
    pa.schema([
        FUTURES_VARIETY_CALENDAR_SCHEMA.field(name)
        for name in UPSTREAM_PARTITION_COLUMNS
    ]),
    flavor="hive",
)


# ## 契约化读取与表级业务校验
# 
# 上游已经由其生产者正式验证，本入口只检查物理 Schema、表身份、直接依赖的主键和分区边界；
# 描述性 metadata 以当前权威契约为准，不要求重写既有 Parquet。
# 当前报告日历是本入口的直接产物，因此还要检查枚举、选择状态、完成状态、计数、时间和中文原因。

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


def open_exact_dataset(
    table_path: pathlib.Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    label: str,
) -> ds.Dataset:
    # 正式输入、staging 与提交后输出都检查物理契约和表身份。
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
    if not physically_and_identity_compatible(
        reconstructed_schema(dataset, schema),
        schema,
    ):
        raise TypeError(f"{label}物理结构或表身份与权威契约不一致。")

    return dataset


def partition_expression(
    partition_key: tuple[object, ...],
) -> ds.Expression:
    # 有序分区键直接转换为 PyArrow 过滤表达式，不拼接查询字符串。
    expression = None

    for column, value in zip(
        PARTITION_COLUMNS,
        partition_key,
        strict=True,
    ):
        condition = ds.field(column) == value
        expression = condition if expression is None else expression & condition

    if expression is None:
        raise ValueError("报告日历分区键不得为空。")

    return expression


def validate_upstream_table(
    table: pa.Table,
    context: str,
) -> pd.DataFrame:
    # 消费者只验证本次展开直接依赖的上游键与分区边界。
    checked = validate_arrow_table(
        table.cast(FUTURES_VARIETY_CALENDAR_SCHEMA, safe=True),
        FUTURES_VARIETY_CALENDAR_SCHEMA,
    )
    frame = arrow_to_pandas(checked, FUTURES_VARIETY_CALENDAR_SCHEMA)

    if frame.duplicated(UPSTREAM_PRIMARY_KEY).any():
        raise ValueError(f"{context}品种日历主键不唯一。")

    for row in checked.to_pylist():
        if (
            row["trading_date"].year != row["year"]
            or row["trading_date"].month != row["month"]
        ):
            raise ValueError(f"{context}品种日历 year/month 与交易日不一致。")

    return frame.sort_values(UPSTREAM_PRIMARY_KEY).reset_index(drop=True)


def validate_report_calendar_table(
    table: pa.Table,
    context: str,
) -> pd.DataFrame:
    # Arrow 转换先固定列顺序、类型、nullable 以及全部中文 metadata。
    checked = validate_arrow_table(
        table.cast(FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA, safe=True),
        FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
    )
    frame = arrow_to_pandas(
        checked,
        FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
    )

    if frame.duplicated(PRIMARY_KEY).any():
        raise ValueError(f"{context}报告日历主键不唯一。")

    now_utc = datetime.now(timezone.utc)
    allowed_dataset_names = set(DATASET_NAMES)

    for row in checked.to_pylist():
        # 枚举、中文说明和日期分区都是日历生产者的完整责任。
        if row["dataset_name"] not in allowed_dataset_names:
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
            row["trading_date"].year != row["year"]
            or row["trading_date"].month != row["month"]
        ):
            raise ValueError(f"{context}year/month 与 trading_date 不一致。")

        # 记录数不能为负；日历只保存格点汇总，不伪造事实明细。
        if row["expected_record_count"] < 0 or row["actual_record_count"] < 0:
            raise ValueError(f"{context}理论或实际记录数不得为负。")

        # 当前无需采集时清除当前缺失；没有历史完成凭证的格点使用未采集状态。
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

        # 完成状态只允许成功或确认空，并且必须同时具备批次和正式复读时间。
        is_completed_status = row["fetch_result_status"] in {
            "success",
            "empty_confirmed",
        }
        if row["is_fetch_completed"] != is_completed_status:
            raise ValueError(f"{context}完成布尔值与采集结果状态不一致。")
        if row["is_fetch_completed"]:
            if not row["fetch_run_id"] or row["fetch_completed_at"] is None:
                raise ValueError(f"{context}完成格点缺少批次或完成时间。")
        elif row["fetch_completed_at"] is not None:
            raise ValueError(f"{context}未完成格点不得具有完成时间。")

        # 缺失只能来自需要采集且已经确认空的响应，不能把请求错误冒充空数据。
        if row["is_data_missing"] and (
            not row["is_fetch_required"]
            or row["fetch_result_status"] != "empty_confirmed"
            or row["actual_record_count"] != 0
        ):
            raise ValueError(f"{context}缺失状态不能由当前结果复算。")

        if (
            row["quality_status"] in {"passed", "warning", "failed"}
            and row["quality_checked_at"] is None
        ):
            raise ValueError(f"{context}已形成质检结论但缺少质检时间。")
        if row["updated_at"] > now_utc:
            raise ValueError(f"{context}updated_at 不得晚于当前 UTC 时间。")

    return frame.sort_values(PRIMARY_KEY).reset_index(drop=True)


# ## 从完整品种日历生成期望报告格点
# 
# 当前三类报告没有额外覆盖排除，因此选择结果暂时等价于共享事实白名单。政策变化只更新当前
# `is_fetch_required` 与原因：已有完成凭证持久保留，未完成状态仅在采集义务改变时重置。

# In[ ]:


def build_expected_calendar(
    upstream_df: pd.DataFrame,
    existing_df: pd.DataFrame,
    updated_at: datetime,
) -> pd.DataFrame:
    # 现有行已经通过本表完整校验，可以按权威主键安全索引。
    existing_rows_by_key = {
        tuple(row[name] for name in PRIMARY_KEY): row
        for row in pandas_to_arrow(
            existing_df.loc[:, FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names],
            FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
        ).to_pylist()
    }

    expected_rows = []
    upstream_rows = pandas_to_arrow(
        upstream_df.loc[:, FUTURES_VARIETY_CALENDAR_SCHEMA.names],
        FUTURES_VARIETY_CALENDAR_SCHEMA,
    ).to_pylist()

    for upstream_row in upstream_rows:
        pair = (
            upstream_row["exchange_code"],
            upstream_row["underlying_code"],
        )
        is_fetch_required = pair in FUTURES_FACT_VARIETY_PAIRS

        for dataset_name in DATASET_NAMES:
            # 当前没有额外数据集覆盖排除；白名单外行仍完整保留在日历中。
            if is_fetch_required:
                requirement_reason = (
                    "品种位于期货事实采集白名单；"
                    f"{dataset_name} 当前未配置额外 API 覆盖或交易所支持排除。"
                )
                fetch_result_status = "pending"
                quality_status = "pending"
                quality_reason = "等待相应事实采集器处理。"
            else:
                requirement_reason = (
                    "品种不在期货事实采集白名单；"
                    f"保留 {dataset_name} 报告日历格点但不采集事实。"
                )
                fetch_result_status = "not_required"
                quality_status = "not_applicable"
                quality_reason = (
                    "本格点仅保留完整报告日历，不进入期货事实采集。"
                )

            row = {
                "dataset_name": dataset_name,
                "exchange_code": upstream_row["exchange_code"],
                "underlying_code": upstream_row["underlying_code"],
                "trading_date": upstream_row["trading_date"],
                "is_fetch_required": is_fetch_required,
                "requirement_reason": requirement_reason,
                "is_fetch_completed": False,
                "fetch_result_status": fetch_result_status,
                "is_data_missing": False,
                "expected_record_count": 0,
                "actual_record_count": 0,
                "quality_status": quality_status,
                "quality_reason": quality_reason,
                "fetch_run_id": None,
                "fetch_completed_at": None,
                "quality_checked_at": None,
                "updated_at": updated_at,
                "year": upstream_row["trading_date"].year,
                "month": upstream_row["trading_date"].month,
            }

            key = tuple(row[name] for name in PRIMARY_KEY)
            existing_row = existing_rows_by_key.get(key)

            if existing_row is not None:
                policy_unchanged = all(
                    existing_row[name] == row[name]
                    for name in POLICY_COLUMNS
                )
                preserve_execution_state = (
                    existing_row["is_fetch_required"]
                    == row["is_fetch_required"]
                    or existing_row["is_fetch_completed"]
                )
                if preserve_execution_state:
                    for name in STATE_COLUMNS:
                        row[name] = existing_row[name]
                    if not row["is_fetch_required"]:
                        row["is_data_missing"] = False
                    elif row["fetch_result_status"] == "empty_confirmed":
                        row["is_data_missing"] = True
                if policy_unchanged:
                    row["updated_at"] = existing_row["updated_at"]

            expected_rows.append(row)

    if expected_rows:
        candidate_df = pd.DataFrame(
            expected_rows,
            columns=FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names,
        )
    else:
        candidate_df = empty_pandas(
            FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA
        )

    return validate_report_calendar_table(
        pandas_to_arrow(
            candidate_df.loc[:, FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names],
            FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
        ),
        "期望",
    )


# ## 内容摘要与自动差集计划
# 
# `updated_at` 只在新建或规则变化时更新；未变化格点已经保留旧值。因此完整行摘要可以直接区分新增、
# 内部缺口、状态变化、政策变化和上游撤销。叶分区摘要用于确定最小提交范围。

# In[ ]:


def table_digest(frame: pd.DataFrame) -> str:
    # Arrow IPC 固定权威列顺序、类型、metadata 和主键排序后再生成摘要。
    ordered_df = frame.sort_values(PRIMARY_KEY).reset_index(drop=True)
    source_table = pandas_to_arrow(
        ordered_df.loc[:, FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names],
        FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
    )
    # 从 Python 标量按权威 Schema 重建缓冲区，消除不同 fragment 的切片布局差异。
    table = pa.Table.from_pylist(
        source_table.to_pylist(),
        schema=FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
    )
    sink = pa.BufferOutputStream()

    with pa.ipc.new_stream(sink, FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA) as writer:
        writer.write_table(table)

    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()


def changed_partition_keys(
    expected_df: pd.DataFrame,
    existing_df: pd.DataFrame,
) -> list[tuple[object, ...]]:
    # 比较完整叶分区；空期望分区也保留在结果中，用于删除上游已撤销的旧分区。
    expected_keys = set(
        expected_df[PARTITION_COLUMNS].itertuples(index=False, name=None)
    )
    existing_keys = set(
        existing_df[PARTITION_COLUMNS].itertuples(index=False, name=None)
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


# ## 完整叶分区 staging、正式复读与失败回滚
# 
# 每个触达叶分区都写入该分区的完整期望内容，而不是只写本次日期切片。上游撤销导致的空分区会删除
# 对应旧叶目录。staging 和正式路径均复读完整 Schema/metadata、业务规则和全表内容摘要。

# In[ ]:


def commit_partitions(
    expected_df: pd.DataFrame,
    partition_keys: list[tuple[object, ...]],
    lake_root: pathlib.Path,
) -> int:
    if not partition_keys:
        return 0

    # 提交前先验证完整期望表，保证删除旧行也来自合法的上游当前真值。
    expected_df = validate_report_calendar_table(
        pandas_to_arrow(
            expected_df.loc[:, FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names],
            FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
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

    # 所有移动路径必须位于本次明确指定的 silver 根目录。
    for managed_path in (
        target_path,
        staging_path,
        backup_path,
        quarantine_path,
    ):
        if not managed_path.resolve().is_relative_to(silver_root):
            raise ValueError(f"数据集路径越出 silver 根目录：{managed_path}")

    silver_root.mkdir(parents=True, exist_ok=True)
    try:
        staging_path.mkdir(parents=True, exist_ok=False)

        # 根级 0 行 Schema marker 使“全部触达分区均待删除”仍可完成 staging 复读。
        file_schema = pa.schema(
            [
                field
                for field in FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA
                if field.name not in PARTITION_COLUMNS
            ],
            metadata=FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.metadata,
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

        changed_rows_df = expected_df.loc[changed_row_mask]
        if not changed_rows_df.empty:
            ds.write_dataset(
                pandas_to_arrow(
                    changed_rows_df.loc[:, FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names],
                    FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
                ),
                staging_path,
                format="parquet",
                partitioning=REPORT_PARTITIONING,
                existing_data_behavior="delete_matching",
                basename_template="part-{i}.parquet",
            )

        # staging 根路径与每个非空触达叶分区都必须能够精确复读。
        staged_dataset = open_exact_dataset(
            staging_path,
            REPORT_PARTITIONING,
            FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
            "报告日历 staging",
        )
        staged_df = validate_report_calendar_table(
            staged_dataset.to_table(
                columns=FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names
            ),
            "staging ",
        )
        if len(staged_df) != len(changed_rows_df):
            raise ValueError("staging 触达行数与完整分区计划不一致。")

        for partition_key in partition_keys:
            expected_mask = pd.Series(True, index=expected_df.index)
            for column, value in zip(
                PARTITION_COLUMNS,
                partition_key,
                strict=True,
            ):
                expected_mask &= expected_df[column].eq(value)
            expected_partition_df = expected_df.loc[expected_mask]
            staged_partition_table = staged_dataset.to_table(
                columns=FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names,
                filter=partition_expression(partition_key),
            )
            staged_partition_df = validate_report_calendar_table(
                staged_partition_table,
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
    moved_partitions = []
    full_swap = expected_df.empty
    cleanup_recovery_paths = True

    try:
        if full_swap:
            # 上游当前有效集合为空时，全表替换为可读的 0 行契约数据集。
            if target_had_existing:
                shutil.move(str(target_path), str(backup_path))
            shutil.move(str(staging_path), str(target_path))
        else:
            backup_path.mkdir(parents=True, exist_ok=False)
            target_path.mkdir(parents=True, exist_ok=True)

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
                        f"staging 叶分区存在性与期望不一致：{relative_path}"
                    )

                destination_path.parent.mkdir(parents=True, exist_ok=True)
                saved_path.parent.mkdir(parents=True, exist_ok=True)
                had_existing = destination_path.exists()

                if had_existing:
                    shutil.move(str(destination_path), str(saved_path))

                # 在移动新分区前记录回滚信息，保证第二次 move 失败时仍能恢复旧分区。
                moved_partitions.append(
                    (destination_path, saved_path, relative_path, had_existing)
                )
                if should_exist:
                    shutil.move(str(source_path), str(destination_path))

        # 正式路径必须与完整期望表逐行一致，不能只验证本次新增行。
        committed_dataset = open_exact_dataset(
            target_path,
            REPORT_PARTITIONING,
            FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
            "正式报告日历",
        )
        committed_df = validate_report_calendar_table(
            committed_dataset.to_table(
                columns=FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names
            ),
            "正式路径复读的",
        )
        if len(committed_df) != len(expected_df):
            raise ValueError("正式报告日历总行数与完整期望表不一致。")
        if table_digest(committed_df) != expected_digest:
            raise ValueError("正式报告日历内容与完整期望表不一致。")
    except Exception as commit_error:
        rollback_errors = []

        try:
            if full_swap:
                if target_path.exists():
                    shutil.move(str(target_path), str(quarantine_path))
                if target_had_existing and backup_path.exists():
                    shutil.move(str(backup_path), str(target_path))
            else:
                # 多叶分区按提交逆序恢复，避免留下新旧混合批次。
                for (
                    destination_path,
                    saved_path,
                    relative_path,
                    had_existing,
                ) in reversed(moved_partitions):
                    if destination_path.exists():
                        failed_path = quarantine_path / relative_path
                        failed_path.parent.mkdir(parents=True, exist_ok=True)
                        shutil.move(str(destination_path), str(failed_path))
                    if had_existing and saved_path.exists():
                        destination_path.parent.mkdir(
                            parents=True,
                            exist_ok=True,
                        )
                        shutil.move(str(saved_path), str(destination_path))

                if (
                    not target_had_existing
                    and target_path.is_dir()
                    and next(target_path.rglob("*.parquet"), None) is None
                ):
                    shutil.rmtree(target_path)
        except Exception as rollback_error:
            rollback_errors.append(str(rollback_error))

        if rollback_errors:
            cleanup_recovery_paths = False
            raise RuntimeError(
                "报告日历提交失败且自动回滚未完成；"
                f"请保留并检查 {backup_path} 与 {quarantine_path}。"
            ) from commit_error
        raise
    finally:
        # 回滚本身失败时保留 staging、backup 和 failed，避免清除恢复依据。
        if cleanup_recovery_paths:
            shutil.rmtree(staging_path, ignore_errors=True)
            shutil.rmtree(backup_path, ignore_errors=True)
            shutil.rmtree(quarantine_path, ignore_errors=True)

    return len(changed_rows_df)


# ## CLI：自动水位、显式检查与是否写入
# 
# 不带日期时读取上游全部当前有效格点并自动求差。显式日期模式仍会保留同月范围外旧行，但不得写入
# `.env` 指向的正式湖。由于本入口不调用 API，不带 `--write` 时只读取和验证数据湖。

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

    requested_start_date = start_date.date() if start_date is not None else None
    requested_end_date = end_date.date() if end_date is not None else None
    if (
        requested_start_date is not None
        and requested_start_date > requested_end_date
    ):
        raise click.BadParameter("起始日期不得晚于结束日期。")

    silver_root = resolved_lake_root / "silver"
    upstream_path = silver_root / UPSTREAM_TABLE_NAME
    target_path = silver_root / TABLE_NAME

    # 上游日历是本表有效水位的唯一业务来源；本入口不请求外部 API。
    upstream_dataset = open_exact_dataset(
        upstream_path,
        UPSTREAM_PARTITIONING,
        FUTURES_VARIETY_CALENDAR_SCHEMA,
        "正式品种日历",
    )
    upstream_filter = None
    if has_explicit_dates:
        upstream_filter = (
            (ds.field("trading_date") >= requested_start_date)
            & (ds.field("trading_date") <= requested_end_date)
        )
    upstream_df = validate_upstream_table(
        upstream_dataset.to_table(
            columns=FUTURES_VARIETY_CALENDAR_SCHEMA.names,
            filter=upstream_filter,
        ),
        "正式路径读取的",
    )

    # 目标不存在只是现有完整格点集合为空，后续同一逻辑自然生成全量候选。
    if target_path.is_dir() and next(target_path.rglob("*.parquet"), None):
        existing_dataset = open_exact_dataset(
            target_path,
            REPORT_PARTITIONING,
            FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
            "现有正式报告日历",
        )
        existing_df = validate_report_calendar_table(
            existing_dataset.to_table(
                columns=FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names
            ),
            "现有正式",
        )
    else:
        existing_df = empty_pandas(
            FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA
        )

    if has_explicit_dates:
        in_scope_mask = (
            existing_df["trading_date"].ge(requested_start_date)
            & existing_df["trading_date"].le(requested_end_date)
        )
        scoped_existing_df = existing_df.loc[in_scope_mask].copy()
        outside_scope_df = existing_df.loc[~in_scope_mask].copy()
    else:
        scoped_existing_df = existing_df
        outside_scope_df = empty_pandas(
            FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA
        )

    updated_at = datetime.now(timezone.utc)
    expected_scope_df = build_expected_calendar(
        upstream_df,
        scoped_existing_df,
        updated_at,
    )

    # 显式范围只替换范围内当前真值；同一月范围外旧行进入完整叶分区并原样保留。
    if has_explicit_dates:
        expected_full_df = pd.concat(
            [outside_scope_df, expected_scope_df],
            ignore_index=True,
        )
        expected_full_df = validate_report_calendar_table(
            pandas_to_arrow(
                expected_full_df.loc[
                    :, FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names
                ],
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
            ),
            "显式范围合并后的完整",
        )
    else:
        expected_full_df = expected_scope_df

    partition_keys = changed_partition_keys(
        expected_full_df,
        existing_df,
    )

    existing_rows_by_key = {
        tuple(row[name] for name in PRIMARY_KEY): row
        for row in pandas_to_arrow(
            scoped_existing_df.loc[
                :, FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names
            ],
            FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
        ).to_pylist()
    }
    expected_rows_by_key = {
        tuple(row[name] for name in PRIMARY_KEY): row
        for row in pandas_to_arrow(
            expected_scope_df.loc[
                :, FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names
            ],
            FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
        ).to_pylist()
    }
    complete_grid_count = sum(
        existing_rows_by_key.get(key) == row
        for key, row in expected_rows_by_key.items()
    )
    removed_grid_count = len(
        set(existing_rows_by_key) - set(expected_rows_by_key)
    )

    run_mode = "explicit" if has_explicit_dates else "automatic"
    click.echo(
        f"table={TABLE_NAME}; mode={run_mode}; "
        f"lake_root={resolved_lake_root}; write={str(write).lower()}"
    )
    click.echo(
        "reconciliation_plan: "
        f"upstream_variety_grids={len(upstream_df)}; "
        f"valid_report_grids={len(expected_scope_df)}; "
        f"complete_report_grids={complete_grid_count}; "
        f"missing_or_revised_grids="
        f"{len(expected_scope_df) - complete_grid_count}; "
        f"removed_grids={removed_grid_count}; "
        f"changed_partitions={len(partition_keys)}"
    )

    if not partition_keys:
        click.echo("报告日历已经与上游和当前事实采集政策一致。")
        return

    if write:
        committed_rows = commit_partitions(
            expected_full_df,
            partition_keys,
            resolved_lake_root,
        )
        click.echo(
            f"committed_rows={committed_rows}; "
            f"committed_partitions={len(partition_keys)}"
        )
    else:
        click.echo(
            f"dry_run_rows={len(expected_scope_df)}; "
            f"planned_partitions={len(partition_keys)}"
        )


# ## Notebook 与脚本运行入口

# In[ ]:


if "ipykernel" in sys.modules:
    # Notebook 默认执行正式湖只读自动计划；需要测试写入时显式改为非正式湖参数。
    main.main(
        args=[],
        prog_name="c01_exchange_report_calendar",
        standalone_mode=False,
    )
elif __name__ == "__main__":
    main()

