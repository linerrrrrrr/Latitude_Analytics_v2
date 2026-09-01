#!/usr/bin/env python
# coding: utf-8

# # c01 外部市场数据采集日历
# 
# 目标表：`dim_external_market_calendar`。
# 
# 本入口不调用任何外部 API。它只读取完整的 `dim_trade_calendar`，并结合
# `config/external_market_entities.py` 的版本化请求实体配置，生成生意社原始页面归档、
# JQData 境外期货全表和 Eastmoney 外部指数的完整理论请求格点。

# ## 自动更新与写入边界
# 
# 自动模式遵循：
# 
# `上游当前有效自然日—请求实体格点 − 已经完整落盘且选择语义未变化的日历格点 = 本次自动更新范围`
# 
# 周末、非中国交易日或配置有效期外的理论格点仍保留，并以 `is_fetch_required=false`
# 表达当前无需请求。未变化格点必须继承下游 raw 归档或事实生产者已经回写的完成、错误和质检状态。
# 
# `--write` 只表示是否提交。显式日期只允许只读检查，或者写入与 `.env` 正式湖不同的临时湖。

# ## 初始化与权威 Schema

# In[ ]:


from __future__ import annotations

import hashlib
import pathlib
import shutil
import sys
import uuid
from datetime import date, datetime, timedelta, timezone

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
    EXTERNAL_MARKET_CALENDAR_SCHEMA,
    TRADE_CALENDAR_SCHEMA,
    arrow_to_pandas,
    empty_pandas,
    pandas_to_arrow,
    validate_arrow_table,
)
from config.external_market_entities import (
    DOMESTIC_SPOT_BASIS_ENTITY_CODE,
    EXTERNAL_INDEX_ENTITIES,
    EXTERNAL_MARKET_ENTITY_CONFIG_VERSION,
    OVERSEAS_FUTURES_ENTITY_CODE,
)
from config.settings import settings


# ## Schema 契约呈现
# 
# 本节只在交互式 Notebook 内核中呈现只读 Schema 契约。上游中国自然日历在前，
# 当前外部市场请求日历在后；展示不会读取湖仓、请求网络或产生写入。

# In[ ]:


if "ipykernel" in sys.modules:
    from config.notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        TRADE_CALENDAR_SCHEMA,
        EXTERNAL_MARKET_CALENDAR_SCHEMA,
    ])


# ## 表名、主键、Hive 分区与请求实体

# In[ ]:


# 表名、主键和 Hive 分区只从权威 Schema metadata 读取一次。
TABLE_NAME = EXTERNAL_MARKET_CALENDAR_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # 外部市场数据采集日历维度表。
PRIMARY_KEY = EXTERNAL_MARKET_CALENDAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")  # 数据集—请求实体—观测日期格点。
PARTITION_COLUMNS = EXTERNAL_MARKET_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 外部市场日历 Hive 叶分区顺序。

UPSTREAM_TABLE_NAME = TRADE_CALENDAR_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # 中国期货交易日历维度表，也是本表唯一上游维度。
UPSTREAM_PRIMARY_KEY = TRADE_CALENDAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")  # 唯一标识一个中国自然日。
UPSTREAM_PARTITION_COLUMNS = TRADE_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 上游自然日历 Hive 分区顺序。

# 数据集枚举来自字段 metadata；指数请求实体来自项目级共享配置。
DATASET_NAMES = tuple(
    EXTERNAL_MARKET_CALENDAR_SCHEMA.field("dataset_name")
    .metadata[b"enum_values_zh"]
    .decode("utf-8")
    .split("、")
)
CONFIGURED_INDEX_IDS = {
    entity.source_indicator_id
    for entity in EXTERNAL_INDEX_ENTITIES
}

POLICY_COLUMNS = [
    "is_fetch_required",  # 当前理论格点是否应由相应下游产物入口请求。
    "requirement_reason",  # 当前工作日、交易日、有效期和配置版本说明。
]
STATE_COLUMNS = [
    "is_fetch_completed",  # 请求、下游产物提交及正式复读是否完成。
    "fetch_result_status",  # 最近一次下游产物采集结果。
    "is_data_missing",  # 允许确认空的事实源是否判为应有而缺失。
    "actual_record_count",  # 正式下游产物复读数量；生意社 raw 成功固定为 1。
    "quality_status",  # 格点综合质量状态。
    "quality_reason",  # 格点质量结论的中文说明。
    "fetch_run_id",  # 最近一次下游产物采集批次号。
    "fetch_completed_at",  # 最近一次下游产物完成时间。
    "quality_checked_at",  # 最近一次质量检查时间。
    "updated_at",  # 本行任一业务状态最后变化时间。
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
        EXTERNAL_MARKET_CALENDAR_SCHEMA.field(name)
        for name in PARTITION_COLUMNS
    ]),
    flavor="hive",
)
UPSTREAM_PARTITIONING = ds.partitioning(
    pa.schema([
        TRADE_CALENDAR_SCHEMA.field(name)
        for name in UPSTREAM_PARTITION_COLUMNS
    ]),
    flavor="hive",
)


# ## 契约化读取与表级业务校验
# 
# 上游日历已经由其生产者承担完整业务验证，本入口只检查精确 Schema/metadata 和当前展开直接依赖的
# 日期、周几、交易日与分区边界。当前日历则由本入口完整检查请求实体、选择状态、完成状态与审计时间。

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


def physical_schema_matches(
    actual_schema: pa.Schema,
    expected_schema: pa.Schema,
) -> bool:
    # metadata 升级只允许字段名、顺序、类型和 nullable 完全不变。
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
    partition_columns: list[str],
) -> bool:
    # Dataset 汇总 Schema 与每个 Parquet fragment 都必须携带当前 metadata。
    if not reconstructed_schema(dataset, schema).equals(
        schema,
        check_metadata=True,
    ):
        return False

    expected_file_schema = pa.schema(
        [field for field in schema if field.name not in partition_columns],
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
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    partition_columns: list[str],
    label: str,
) -> tuple[ds.Dataset, bool]:
    # 读取 metadata 过期表前，先逐 fragment 证明物理结构仍可无损迁移。
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
    expected_names = set(schema.names)
    if (
        len(dataset.schema.names) != len(schema.names)
        or set(dataset.schema.names) != expected_names
        or not physical_schema_matches(
            reconstructed_schema(dataset, schema),
            schema,
        )
    ):
        raise TypeError(f"{label}物理字段、类型或 nullable 与权威契约不兼容。")

    expected_file_schema = pa.schema([
        field for field in schema if field.name not in partition_columns
    ])
    for fragment in dataset.get_fragments():
        if not physical_schema_matches(
            pa.schema(list(fragment.physical_schema)),
            expected_file_schema,
        ):
            raise TypeError(
                f"{label}存在物理结构不兼容的 Parquet fragment：{fragment.path}"
            )

    is_exact = dataset_has_exact_schema_metadata(
        dataset,
        schema,
        partition_columns,
    )
    return dataset, is_exact


def open_exact_dataset(
    table_path: pathlib.Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    label: str,
) -> ds.Dataset:
    # staging、上游与提交后输出必须逐 fragment 精确匹配当前 metadata。
    dataset, is_exact = open_compatible_dataset(
        table_path,
        partitioning,
        schema,
        (
            PARTITION_COLUMNS
            if schema is EXTERNAL_MARKET_CALENDAR_SCHEMA
            else UPSTREAM_PARTITION_COLUMNS
        ),
        label,
    )
    if not is_exact:
        raise TypeError(f"{label} Schema/metadata 与权威契约不一致。")

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
        raise ValueError("外部市场日历分区键不得为空。")

    return expression


def validate_upstream_table(
    table: pa.Table,
    context: str,
) -> pd.DataFrame:
    # 消费者只验证本次理论格点展开直接依赖的上游字段关系。
    checked = validate_arrow_table(table, TRADE_CALENDAR_SCHEMA)
    frame = arrow_to_pandas(checked, TRADE_CALENDAR_SCHEMA)

    if frame.duplicated(UPSTREAM_PRIMARY_KEY).any():
        raise ValueError(f"{context}自然日历主键不唯一。")

    ordered_dates = []
    for row in checked.to_pylist():
        ordered_dates.append(row["calendar_date"])

        if row["calendar_date"].year != row["year"]:
            raise ValueError(f"{context}自然日历 year 与 calendar_date 不一致。")
        if row["weekday"] != row["calendar_date"].isoweekday():
            raise ValueError(f"{context}自然日历 weekday 与 calendar_date 不一致。")
        if row["is_weekend"] != (row["weekday"] >= 6):
            raise ValueError(f"{context}自然日历周末标记与 weekday 不一致。")

    ordered_dates.sort()
    if any(
        right - left != timedelta(days=1)
        for left, right in zip(ordered_dates, ordered_dates[1:], strict=False)
    ):
        raise ValueError(f"{context}自然日历在当前读取范围内不连续。")

    return frame.sort_values(UPSTREAM_PRIMARY_KEY).reset_index(drop=True)


def validate_external_calendar_table(
    table: pa.Table,
    context: str,
    *,
    allow_legacy_domestic_state: bool = False,
) -> pd.DataFrame:
    # Arrow 转换固定字段顺序、类型、nullable 和全部中文 metadata。
    checked = validate_arrow_table(table, EXTERNAL_MARKET_CALENDAR_SCHEMA)
    frame = arrow_to_pandas(checked, EXTERNAL_MARKET_CALENDAR_SCHEMA)

    if frame.duplicated(PRIMARY_KEY).any():
        raise ValueError(f"{context}外部市场日历主键不唯一。")

    now_utc = datetime.now(timezone.utc)
    allowed_dataset_names = set(DATASET_NAMES)

    for row in checked.to_pylist():
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
            row["observation_date"].year != row["year"]
            or row["observation_date"].month != row["month"]
        ):
            raise ValueError(f"{context}year/month 与 observation_date 不一致。")
        if row["actual_record_count"] < 0:
            raise ValueError(f"{context}实际记录数不得为负。")

        # 请求实体必须与下游原始页面归档或事实 API 的实际调用粒度一致。
        if row["dataset_name"] == "domestic_spot_basis":
            if row["entity_code"] != DOMESTIC_SPOT_BASIS_ENTITY_CODE:
                raise ValueError(f"{context}生意社原始页面请求实体必须为 ALL。")
            if (
                not allow_legacy_domestic_state
                and row["fetch_result_status"] == "empty_confirmed"
            ):
                raise ValueError(f"{context}生意社原始页面归档禁止 empty_confirmed。")
        elif row["dataset_name"] == "overseas_futures":
            if row["entity_code"] != OVERSEAS_FUTURES_ENTITY_CODE:
                raise ValueError(f"{context}境外期货请求实体必须为 ALL。")
        elif row["entity_code"] not in CONFIGURED_INDEX_IDS:
            raise ValueError(f"{context}外部指数请求实体未命中项目配置。")

        # 无需请求的理论格点仍保留，但执行、计数与审计状态必须完全自洽。
        if not row["is_fetch_required"]:
            if row["fetch_result_status"] != "not_required":
                raise ValueError(f"{context}无需请求格点必须为 not_required。")
            if row["is_fetch_completed"] or row["is_data_missing"]:
                raise ValueError(f"{context}无需请求格点不得标记完成或数据缺失。")
            if row["actual_record_count"] != 0:
                raise ValueError(f"{context}无需请求格点的记录数必须为 0。")
            if row["quality_status"] != "not_applicable":
                raise ValueError(f"{context}无需请求格点必须为 not_applicable。")
            if any(
                row[name] is not None
                for name in [
                    "fetch_run_id",
                    "fetch_completed_at",
                    "quality_checked_at",
                ]
            ):
                raise ValueError(f"{context}无需请求格点不得保留下游产物运行审计值。")
        elif row["fetch_result_status"] == "not_required":
            raise ValueError(f"{context}需请求格点不得标为 not_required。")

        # 只有下游产物已经正式提交并复读，或允许确认空的事实源明确确认空时才完成。
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

        if row["fetch_result_status"] == "success":
            if row["actual_record_count"] <= 0 or row["is_data_missing"]:
                raise ValueError(f"{context}success 必须有正式下游产物且不得标记缺失。")
        if row["fetch_result_status"] == "empty_confirmed" and row["actual_record_count"] != 0:
            raise ValueError(f"{context}empty_confirmed 的正式事实计数必须为 0。")
        if row["is_data_missing"] and row["fetch_result_status"] != "empty_confirmed":
            raise ValueError(f"{context}数据缺失只能来自确认空响应。")

        # 生意社只归档原始响应：正式文件字节数与 SHA-256 复读一致后固定完成一项。
        if (
            not allow_legacy_domestic_state
            and row["dataset_name"] == "domestic_spot_basis"
            and row["is_fetch_completed"]
            and (
                row["fetch_result_status"] != "success"
                or row["actual_record_count"] != 1
                or row["is_data_missing"]
                or row["quality_status"] != "passed"
            )
        ):
            raise ValueError(
                f"{context}生意社完成格点必须为 success + passed、产物数 1 且不缺失。"
            )

        if (
            row["quality_status"] in {"passed", "warning", "failed"}
            and row["quality_checked_at"] is None
        ):
            raise ValueError(f"{context}已形成质检结论但缺少质检时间。")
        if row["updated_at"] > now_utc:
            raise ValueError(f"{context}updated_at 不得晚于当前 UTC 时间。")

    return frame.sort_values(PRIMARY_KEY).reset_index(drop=True)


# ## 从完整自然日历生成理论请求格点
# 
# 生意社原始页面按中国交易日请求并整页归档；`FUT_GLOBAL_DAILY` 按普通工作日整表请求；每个 Eastmoney 指数按
# `INDICATOR_ID` 和配置有效期在普通工作日请求。周末和非交易日不删除，而是保留为 `not_required`。
# 
# 只有 `is_fetch_required` 和带配置版本的 `requirement_reason` 均未改变时，才继承原下游产物采集状态。

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
            existing_df.loc[:, EXTERNAL_MARKET_CALENDAR_SCHEMA.names],
            EXTERNAL_MARKET_CALENDAR_SCHEMA,
        ).to_pylist()
    }

    expected_rows = []
    upstream_rows = pandas_to_arrow(
        upstream_df.loc[:, TRADE_CALENDAR_SCHEMA.names],
        TRADE_CALENDAR_SCHEMA,
    ).to_pylist()

    for upstream_row in upstream_rows:
        observation_date = upstream_row["calendar_date"]
        weekday = upstream_row["weekday"]
        is_weekday = weekday <= 5

        request_entities = [
            (
                "domestic_spot_basis",
                DOMESTIC_SPOT_BASIS_ENTITY_CODE,
                bool(upstream_row["is_trading_day"]),
                "中国期货交易日，按日期请求并原样归档生意社整页响应，不解析 HTML。"
                if upstream_row["is_trading_day"]
                else "非中国期货交易日，保留理论格点但不请求生意社页面。",
            ),
            (
                "overseas_futures",
                OVERSEAS_FUTURES_ENTITY_CODE,
                is_weekday,
                "普通工作日，按日期请求 JQData FUT_GLOBAL_DAILY 整张表。"
                if is_weekday
                else "周末，保留理论格点但不请求 JQData FUT_GLOBAL_DAILY。",
            ),
        ]

        for entity in EXTERNAL_INDEX_ENTITIES:
            in_active_period = (
                (entity.active_from is None or observation_date >= entity.active_from)
                and (entity.active_to is None or observation_date <= entity.active_to)
            )
            is_fetch_required = is_weekday and in_active_period

            if not is_weekday:
                reason = "周末，保留理论格点但不请求 Eastmoney 指数。"
            elif not in_active_period:
                reason = "不在该指数配置有效期内，保留理论格点但不请求。"
            else:
                reason = (
                    f"普通工作日，按 INDICATOR_ID={entity.source_indicator_id} "
                    "请求 Eastmoney 指数。"
                )

            request_entities.append((
                "external_index",
                entity.source_indicator_id,
                is_fetch_required,
                reason,
            ))

        for (
            dataset_name,
            entity_code,
            is_fetch_required,
            selection_reason,
        ) in request_entities:
            requirement_reason = (
                f"外部市场请求实体配置 v{EXTERNAL_MARKET_ENTITY_CONFIG_VERSION}；"
                f"{selection_reason}"
            )

            if is_fetch_required:
                fetch_result_status = "pending"
                quality_status = "pending"
                quality_reason = "等待相应 raw 归档或事实生产者请求、提交并正式复读。"
            else:
                fetch_result_status = "not_required"
                quality_status = "not_applicable"
                quality_reason = "当前规则无需请求；保留完整理论日历格点。"

            row = {
                "dataset_name": dataset_name,
                "entity_code": entity_code,
                "observation_date": observation_date,
                "is_fetch_required": is_fetch_required,
                "requirement_reason": requirement_reason,
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
                "year": observation_date.year,
                "month": observation_date.month,
            }

            key = tuple(row[name] for name in PRIMARY_KEY)
            existing_row = existing_rows_by_key.get(key)

            # 选择语义未变化时，下游产物状态和原 updated_at 必须逐字段继承。
            if existing_row is not None and all(
                existing_row[name] == row[name]
                for name in POLICY_COLUMNS
            ):
                for name in STATE_COLUMNS:
                    row[name] = existing_row[name]

            expected_rows.append(row)

    if expected_rows:
        candidate_df = pd.DataFrame(
            expected_rows,
            columns=EXTERNAL_MARKET_CALENDAR_SCHEMA.names,
        )
    else:
        candidate_df = empty_pandas(EXTERNAL_MARKET_CALENDAR_SCHEMA)

    return validate_external_calendar_table(
        pandas_to_arrow(
            candidate_df.loc[:, EXTERNAL_MARKET_CALENDAR_SCHEMA.names],
            EXTERNAL_MARKET_CALENDAR_SCHEMA,
        ),
        "期望",
    )


# ## 内容摘要与自动差集计划
# 
# `updated_at` 只在新增格点或选择语义变化时更新；未变化格点已经继承旧状态。因此按完整权威行生成的
# 摘要可以直接识别新增、历史缺口、配置变化、下游状态回写和上游撤销，并把变化收敛到完整 Hive 叶分区。

# In[ ]:


def table_digest(frame: pd.DataFrame) -> str:
    # Arrow IPC 固定权威列顺序、类型、metadata 和主键排序后再生成摘要。
    ordered_df = frame.sort_values(PRIMARY_KEY).reset_index(drop=True)
    source_table = pandas_to_arrow(
        ordered_df.loc[:, EXTERNAL_MARKET_CALENDAR_SCHEMA.names],
        EXTERNAL_MARKET_CALENDAR_SCHEMA,
    )
    # 从 Python 标量按权威 Schema 重建缓冲区，消除不同 fragment 的切片布局差异。
    table = pa.Table.from_pylist(
        source_table.to_pylist(),
        schema=EXTERNAL_MARKET_CALENDAR_SCHEMA,
    )
    sink = pa.BufferOutputStream()

    with pa.ipc.new_stream(sink, EXTERNAL_MARKET_CALENDAR_SCHEMA) as writer:
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
# 每个触达的 `dataset_name/year/month` 叶分区都写入该分区的完整期望内容。即使显式检查只覆盖月内少数日期，
# 同月范围外的现有行也会进入完整分区快照，不会因 `delete_matching` 丢失。staging 和正式路径均执行精确
# Schema/metadata、业务规则、行数与内容摘要复读；提交失败时恢复全部已触达旧分区。
# 
# 若现有表仅 metadata 过期而字段、顺序、类型与 nullable 完全兼容，则先用当前规则生成完整
# `expected_full_df`，再执行全表 staging、逐值复读和整根 swap。该路径不允许只升级部分叶分区；
# 正式复读失败时整根恢复旧表。

# In[ ]:


def commit_partitions(
    expected_df: pd.DataFrame,
    partition_keys: list[tuple[object, ...]],
    lake_root: pathlib.Path,
    force_full_swap: bool = False,
) -> int:
    if not partition_keys and not force_full_swap:
        return 0

    # 提交前先验证完整期望表，保证删除旧行也来自合法的上游当前真值。
    expected_df = validate_external_calendar_table(
        pandas_to_arrow(
            expected_df.loc[:, EXTERNAL_MARKET_CALENDAR_SCHEMA.names],
            EXTERNAL_MARKET_CALENDAR_SCHEMA,
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
                for field in EXTERNAL_MARKET_CALENDAR_SCHEMA
                if field.name not in PARTITION_COLUMNS
            ],
            metadata=EXTERNAL_MARKET_CALENDAR_SCHEMA.metadata,
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

        # metadata 过期时 staging 必须包含完整 expected_full_df，禁止混合新旧叶。
        changed_rows_df = (
            expected_df
            if force_full_swap
            else expected_df.loc[changed_row_mask]
        )
        if not changed_rows_df.empty:
            ds.write_dataset(
                pandas_to_arrow(
                    changed_rows_df.loc[:, EXTERNAL_MARKET_CALENDAR_SCHEMA.names],
                    EXTERNAL_MARKET_CALENDAR_SCHEMA,
                ),
                staging_path,
                format="parquet",
                partitioning=CALENDAR_PARTITIONING,
                existing_data_behavior="delete_matching",
                basename_template="part-{i}.parquet",
            )

        # staging 根路径与每个非空触达叶分区都必须能够精确复读。
        staged_dataset = open_exact_dataset(
            staging_path,
            CALENDAR_PARTITIONING,
            EXTERNAL_MARKET_CALENDAR_SCHEMA,
            "外部市场日历 staging",
        )
        staged_df = validate_external_calendar_table(
            staged_dataset.to_table(
                columns=EXTERNAL_MARKET_CALENDAR_SCHEMA.names
            ),
            "staging ",
        )
        if len(staged_df) != len(changed_rows_df):
            raise ValueError("staging 触达行数与完整分区计划不一致。")
        if (
            force_full_swap
            and table_digest(staged_df) != expected_digest
        ):
            raise ValueError("metadata 升级 staging 与完整期望表逐值不一致。")

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
                columns=EXTERNAL_MARKET_CALENDAR_SCHEMA.names,
                filter=partition_expression(partition_key),
            )
            staged_partition_df = validate_external_calendar_table(
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
    full_swap = force_full_swap or expected_df.empty
    cleanup_recovery_paths = True
    old_target_moved = False
    new_target_installed = False

    try:
        if full_swap:
            # 空上游或 metadata 升级都以整根 swap 提交，避免部分叶混合契约。
            if target_had_existing:
                shutil.move(str(target_path), str(backup_path))
                old_target_moved = True
            shutil.move(str(staging_path), str(target_path))
            new_target_installed = True
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
            CALENDAR_PARTITIONING,
            EXTERNAL_MARKET_CALENDAR_SCHEMA,
            "正式外部市场日历",
        )
        committed_df = validate_external_calendar_table(
            committed_dataset.to_table(
                columns=EXTERNAL_MARKET_CALENDAR_SCHEMA.names
            ),
            "正式路径复读的",
        )
        if len(committed_df) != len(expected_df):
            raise ValueError("正式外部市场日历总行数与完整期望表不一致。")
        if table_digest(committed_df) != expected_digest:
            raise ValueError("正式外部市场日历内容与完整期望表不一致。")
    except Exception as commit_error:
        rollback_errors = []

        try:
            if full_swap:
                # 旧根第一步移动失败时两个标志都为 False，绝不能再碰仍在原位的正式根。
                if (
                    new_target_installed
                    or old_target_moved
                    or not target_had_existing
                ) and target_path.exists():
                    shutil.move(str(target_path), str(quarantine_path))
                if old_target_moved and backup_path.exists():
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
                "外部市场日历提交失败且自动回滚未完成；"
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
# 不带日期时消费上游全部当前有效自然日并自动求差。显式范围只改写范围内当前真值，并把同月范围外旧行
# 带入完整分区；它不得写入 `.env` 指向的正式湖。本入口没有外部 API，不带 `--write` 时只读湖仓并输出计划。

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

    # 中国自然日历是本表有效水位的唯一上游维度；这里不会请求任何外部数据源。
    upstream_dataset = open_exact_dataset(
        upstream_path,
        UPSTREAM_PARTITIONING,
        TRADE_CALENDAR_SCHEMA,
        "正式中国自然日历",
    )
    upstream_filter = ds.field("calendar_date") >= settings.futures_data_start_date
    if has_explicit_dates:
        upstream_filter = (
            upstream_filter
            & (ds.field("calendar_date") >= requested_start_date)
            & (ds.field("calendar_date") <= requested_end_date)
        )

    upstream_df = validate_upstream_table(
        upstream_dataset.to_table(
            columns=TRADE_CALENDAR_SCHEMA.names,
            filter=upstream_filter,
        ),
        "正式路径读取的",
    )

    # 目标不存在只是现有完整格点集合为空；旧 metadata 仅在物理完全兼容时迁移。
    metadata_upgrade_required = False
    if target_path.is_dir() and next(target_path.rglob("*.parquet"), None):
        existing_dataset, existing_schema_is_exact = open_compatible_dataset(
            target_path,
            CALENDAR_PARTITIONING,
            EXTERNAL_MARKET_CALENDAR_SCHEMA,
            PARTITION_COLUMNS,
            "现有正式外部市场日历",
        )
        metadata_upgrade_required = not existing_schema_is_exact
        existing_df = validate_external_calendar_table(
            existing_dataset.to_table(
                columns=EXTERNAL_MARKET_CALENDAR_SCHEMA.names
            ),
            "现有正式",
            # 这里只为迁移读取旧结构化基差状态；新期望表仍由严格 1.3 规则校验。
            allow_legacy_domestic_state=True,
        )
    else:
        existing_df = empty_pandas(EXTERNAL_MARKET_CALENDAR_SCHEMA)

    # 显式范围只更新范围内格点；同一叶分区中范围外旧行原样带入完整快照。
    if has_explicit_dates:
        in_scope_mask = (
            existing_df["observation_date"].ge(requested_start_date)
            & existing_df["observation_date"].le(requested_end_date)
        )
        scoped_existing_df = existing_df.loc[in_scope_mask].copy()
        outside_scope_df = existing_df.loc[~in_scope_mask].copy()
    else:
        scoped_existing_df = existing_df
        outside_scope_df = empty_pandas(EXTERNAL_MARKET_CALENDAR_SCHEMA)

    expected_scope_df = build_expected_calendar(
        upstream_df,
        scoped_existing_df,
        datetime.now(timezone.utc),
    )

    if has_explicit_dates:
        expected_full_df = pd.concat(
            [outside_scope_df, expected_scope_df],
            ignore_index=True,
        )
        expected_full_df = validate_external_calendar_table(
            pandas_to_arrow(
                expected_full_df.loc[:, EXTERNAL_MARKET_CALENDAR_SCHEMA.names],
                EXTERNAL_MARKET_CALENDAR_SCHEMA,
            ),
            "显式范围合并后的完整",
        )
    else:
        expected_full_df = expected_scope_df

    partition_keys = changed_partition_keys(expected_full_df, existing_df)
    if metadata_upgrade_required:
        # metadata 升级必须触达完整表并整根替换，不能只重写内容变化的叶分区。
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

    existing_rows_by_key = {
        tuple(row[name] for name in PRIMARY_KEY): row
        for row in pandas_to_arrow(
            scoped_existing_df.loc[:, EXTERNAL_MARKET_CALENDAR_SCHEMA.names],
            EXTERNAL_MARKET_CALENDAR_SCHEMA,
        ).to_pylist()
    }
    expected_rows_by_key = {
        tuple(row[name] for name in PRIMARY_KEY): row
        for row in pandas_to_arrow(
            expected_scope_df.loc[:, EXTERNAL_MARKET_CALENDAR_SCHEMA.names],
            EXTERNAL_MARKET_CALENDAR_SCHEMA,
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
        f"upstream_calendar_dates={len(upstream_df)}; "
        f"valid_request_grids={len(expected_scope_df)}; "
        f"complete_calendar_grids={complete_grid_count}; "
        f"missing_or_revised_grids={len(expected_scope_df) - complete_grid_count}; "
        f"removed_grids={removed_grid_count}; "
        f"metadata_upgrade_required={str(metadata_upgrade_required).lower()}; "
        f"changed_partitions={len(partition_keys)}"
    )

    if not partition_keys and not metadata_upgrade_required:
        click.echo("外部市场日历已经与上游自然日历和当前请求实体配置一致。")
        return

    if write:
        committed_rows = commit_partitions(
            expected_full_df,
            partition_keys,
            resolved_lake_root,
            force_full_swap=metadata_upgrade_required,
        )
        click.echo(
            f"committed_rows={committed_rows}; "
            f"committed_partitions={len(partition_keys)}; "
            f"full_root_swap={str(metadata_upgrade_required).lower()}"
        )
    else:
        click.echo(
            f"dry_run_rows={len(expected_scope_df)}; "
            f"planned_partitions={len(partition_keys)}; "
            f"full_root_swap={str(metadata_upgrade_required).lower()}"
        )


# ## Notebook 与脚本运行入口

# In[ ]:


if "ipykernel" in sys.modules:
    # Notebook 默认执行正式湖只读自动计划；测试写入必须显式改用非正式湖路径。
    main.main(
        args=[],
        prog_name="c01_external_market_calendar",
        standalone_mode=False,
    )
elif __name__ == "__main__":
    main()

