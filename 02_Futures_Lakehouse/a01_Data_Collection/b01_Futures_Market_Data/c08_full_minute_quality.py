#!/usr/bin/env python
# coding: utf-8

# # c08 分钟主键缺失审计
# 
# 目标表：\`fact_futures_missing_bar\`。本入口只执行：
# 
# \`c04 required 理论分钟主键 − c06 正式分钟事实主键 = 缺失分钟主键\`
# 
# c06 已负责分钟事实的主键、Session 归属、时间范围、非有限值、负数量和有限 OHLC 关系异常留痕。
# c08 信任这些生产者契约，只投影 \`contract_code\` 与 \`bar_at\`，不读取行情值，也不调用 c07。

# ## 运行与提交边界
# 
# - 不接受日期、月份、合约等裁剪参数；一次运行覆盖全部 required 且已完成的 1m Session。
# - \`--confirm-full-quality\` 是人工全量确认；\`--write\` 是唯一写入语义。
# - 缺失明细全表和触达的完整日历叶作为一个协调批次提交并共同回滚。
# - 日历只更新实际条数、缺失标志、缺失条数、缺失检查时间和更新时间；OHLC warning、质量原因及 c07 旁证全部保留。

# In[ ]:


from __future__ import annotations

import hashlib
import pathlib
import shutil
import sys
import tempfile
import uuid
from datetime import datetime, timezone

# Notebook 可从项目任意子目录启动；根目录定位方法由 .env.template 统一规定。
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
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from config.data_contracts import (
    FUTURES_BAR_CALENDAR_SCHEMA,
    FUTURES_MINUTE_SCHEMA,
    FUTURES_MISSING_BAR_SCHEMA,
    arrow_to_pandas,
    pandas_to_arrow,
    polars_to_arrow,
    validate_arrow_table,
)
from config.settings import settings


# ## Schema 契约呈现

# In[ ]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from config.notebook_schema_browser import display_schema_metadata

    # 只展示本入口的直接上游和当前产出，不再引入 c07 的日线或合约旁证。
    display_schema_metadata([
        FUTURES_BAR_CALENDAR_SCHEMA,
        FUTURES_MINUTE_SCHEMA,
        FUTURES_MISSING_BAR_SCHEMA,
    ])


# ## 表名、主键、算法键与 Hive 分区
# 
# 稳定表名、主键和分区只从权威 Schema metadata 解码一次。分钟事实的两列投影是本地算法键，
# 不是第二套表级主键定义。

# In[ ]:


MISSING_TABLE_NAME = FUTURES_MISSING_BAR_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # 国内期货缺失 bar 明细事实表。
MISSING_PRIMARY_KEY = FUTURES_MISSING_BAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")  # 缺失分钟明细的正式业务主键。
MISSING_PARTITION_COLUMNS = FUTURES_MISSING_BAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 缺失明细的 Hive 叶分区。

CALENDAR_TABLE_NAME = FUTURES_BAR_CALENDAR_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # 行情拉取与质检日历。
CALENDAR_PRIMARY_KEY = FUTURES_BAR_CALENDAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")  # 唯一标识一个 1d 或 1m 日历格点。
CALENDAR_PARTITION_COLUMNS = FUTURES_BAR_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 行情日历的 Hive 叶分区。

MINUTE_TABLE_NAME = FUTURES_MINUTE_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # c06 正式提交的一分钟行情事实表。
MINUTE_PARTITION_COLUMNS = FUTURES_MINUTE_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 分钟事实的品种月 Hive 叶分区。

# 集合差只需要事实表级主键本身；交易日和 Session 属性从理论格点继承。
MINUTE_KEY_COLUMNS = [
    "contract_code",  # 固定月份合约代码。
    "bar_at",  # 实际一分钟 bar 的 Asia/Shanghai 结束时刻。
]
SESSION_MATCH_COLUMNS = [
    "contract_code",  # 固定月份合约代码。
    "trading_date",  # Session 归属的期货交易日。
    "session_number",  # 同一合约日内的 Session 顺序号。
]

MISSING_PARTITIONING = ds.partitioning(
    pa.schema([
        FUTURES_MISSING_BAR_SCHEMA.field(name)
        for name in MISSING_PARTITION_COLUMNS
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
MINUTE_PARTITIONING = ds.partitioning(
    pa.schema([
        FUTURES_MINUTE_SCHEMA.field(name)
        for name in MINUTE_PARTITION_COLUMNS
    ]),
    flavor="hive",
)


# ## 精确数据集读取、分区发现和内容摘要
# 
# 上游只检查物理字段、稳定表身份及本次直接依赖的完成边界；描述 metadata 以当前契约为权威但不阻塞历史读取。c08 自己的 staging 与正式输出
# 继续执行逐 fragment 契约检查和确定性内容摘要。

# In[ ]:


def reconstructed_schema(
    dataset: ds.Dataset,
    schema: pa.Schema,
) -> pa.Schema:
    # Hive 分区列由目录补回；按权威列顺序重建完整 Dataset Schema。
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


def parquet_file_schema(
    schema: pa.Schema,
    partition_columns: list[str],
) -> pa.Schema:
    # 单个 Parquet 文件不重复保存 Hive 目录字段。
    partition_set = set(partition_columns)
    return pa.schema(
        [field for field in schema if field.name not in partition_set],
        metadata=schema.metadata,
    )


def open_exact_dataset(
    table_path: pathlib.Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    partition_columns: list[str],
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
    if not physically_and_identity_compatible(
        reconstructed_schema(dataset, schema),
        schema,
    ):
        raise TypeError(f"{label} Dataset 物理结构或表身份与契约不一致。")

    expected_file_schema = parquet_file_schema(
        schema,
        partition_columns,
    )
    for fragment in dataset.get_fragments():
        if not physically_and_identity_compatible(
            fragment.physical_schema,
            expected_file_schema,
        ):
            raise TypeError(
                f"{label} fragment 物理结构或表身份与契约不一致："
                f"{fragment.path}"
            )

    return dataset


def partition_expression(
    partition_columns: list[str],
    partition_key: tuple[object, ...],
) -> ds.Expression:
    # 直接构造 Arrow 表达式，不拼接字符串查询条件。
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


def discover_partition_keys(
    table_path: pathlib.Path,
    partition_columns: list[str],
) -> set[tuple[object, ...]]:
    # 根级 schema.parquet 只证明零行契约，不属于业务叶分区。
    partition_keys = set()
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
            values.append(
                int(raw_value)
                if column in {"year", "month"}
                else raw_value
            )
        partition_keys.add(tuple(values))

    return partition_keys


def table_digest(
    table: pa.Table,
    schema: pa.Schema,
    sort_columns: list[str],
) -> str:
    # 摘要覆盖权威类型、metadata、列顺序及确定性行顺序。
    typed_table = validate_arrow_table(table, schema)
    sort_indices = pc.sort_indices(
        typed_table,
        sort_keys=[(name, "ascending") for name in sort_columns],
    )
    sorted_table = typed_table.take(sort_indices).combine_chunks()

    # null 槽位的数据 buffer 不是逻辑值；先记录有效位，再用类型固定值填充。
    canonical_columns = []
    validity_masks = []
    for field, column in zip(schema, sorted_table.columns, strict=True):
        array = column.combine_chunks()
        validity_masks.append(
            array.is_valid().to_numpy(zero_copy_only=False).tobytes()
        )
        if array.null_count:
            if pa.types.is_boolean(field.type):
                fill_value = False
            elif pa.types.is_string(field.type):
                fill_value = ""
            else:
                fill_value = 0
            array = array.fill_null(pa.scalar(fill_value, type=field.type))

        if pa.types.is_floating(field.type):
            # NaN payload 与 -0.0 的物理位模式不属于业务值语义。
            array = pc.if_else(
                pc.is_nan(array),
                pa.scalar(float("nan"), type=field.type),
                array,
            )
            array = pc.if_else(
                pc.equal(array, pa.scalar(0.0, type=field.type)),
                pa.scalar(0.0, type=field.type),
                array,
            )
        canonical_columns.append(array)

    canonical_table = pa.Table.from_arrays(
        canonical_columns,
        schema=schema,
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        writer.write_table(canonical_table)

    digest = hashlib.sha256(sink.getvalue().to_pybytes())
    for validity_mask in validity_masks:
        digest.update(len(validity_mask).to_bytes(8, byteorder="little"))
        digest.update(validity_mask)
    return digest.hexdigest()


# ## c08 直接产出的质量门禁
# 
# c08 不复核分钟事实业务质量。它只对自己生成的缺失表和自己修改的日历字段承担验证责任。

# In[ ]:


def validate_missing_output(
    table: pa.Table,
    partition_key: tuple[object, ...],
    context: str,
) -> pa.Table:
    checked_table = validate_arrow_table(
        table,
        FUTURES_MISSING_BAR_SCHEMA,
    )
    if checked_table.num_rows == 0:
        return checked_table

    checked_frame = pl.from_arrow(checked_table)
    if checked_frame.select(MISSING_PRIMARY_KEY).is_duplicated().any():
        raise ValueError(f"{context}缺失明细主键不唯一。")

    actual_partition_keys = set(
        checked_frame.select(MISSING_PARTITION_COLUMNS).unique().rows()
    )
    if actual_partition_keys != {partition_key}:
        raise ValueError(f"{context}缺失明细越出目标 Hive 分区。")

    return checked_table


def validate_calendar_audit_output(
    frame: pd.DataFrame,
    partition_key: tuple[object, ...],
    detected_at: datetime,
    context: str,
) -> pa.Table:
    checked_table = pandas_to_arrow(
        frame.loc[:, FUTURES_BAR_CALENDAR_SCHEMA.names],
        FUTURES_BAR_CALENDAR_SCHEMA,
    )
    checked_frame = arrow_to_pandas(
        checked_table,
        FUTURES_BAR_CALENDAR_SCHEMA,
    )

    actual_partition_keys = set(
        checked_frame[CALENDAR_PARTITION_COLUMNS].itertuples(
            index=False,
            name=None,
        )
    )
    if actual_partition_keys != {partition_key}:
        raise ValueError(f"{context}行情日历越出目标 Hive 分区。")

    selected = checked_frame.loc[
        checked_frame["bar_frequency"].eq("1m")
        & checked_frame["is_fetch_required"].eq(True)
    ]
    if selected.empty:
        return checked_table
    if not selected["is_fetch_completed"].all():
        raise ValueError(f"{context}required Session 尚未完成 c06。")

    expected = selected["expected_bar_count"].astype("int64")
    actual = selected["actual_bar_count"].astype("int64")
    missing = selected["missing_bar_count"].astype("int64")
    if (actual < 0).any() or (missing < 0).any():
        raise ValueError(f"{context}实际或缺失条数为负。")
    if not (actual + missing).eq(expected).all():
        raise ValueError(f"{context}实际与缺失条数不能还原理论条数。")
    if not selected["is_data_missing"].eq(missing.gt(0)).all():
        raise ValueError(f"{context}缺失标志与缺失条数不一致。")

    detected_timestamp = pd.Timestamp(detected_at)
    if not selected["missing_checked_at"].eq(detected_timestamp).all():
        raise ValueError(f"{context}缺失检查时间未统一更新。")
    if not selected["updated_at"].eq(detected_timestamp).all():
        raise ValueError(f"{context}更新时间未统一更新。")

    return checked_table


# ## 分区流式键投影与向量求差
# 
# 外层按 1m 交易所月保留完整日历叶，内层按品种月只读取两列分钟主键。理论时点使用
# Polars 原生 \`datetime_ranges + explode + anti join\` 生成，不再逐 Session 构造 Python 集合。

# In[ ]:


def build_full_audit_staging(
    lake_root: pathlib.Path,
    missing_staging_path: pathlib.Path,
    calendar_staging_path: pathlib.Path,
    detected_at: datetime,
) -> dict[str, object]:
    silver_root = lake_root.resolve() / "silver"
    calendar_path = silver_root / CALENDAR_TABLE_NAME
    minute_path = silver_root / MINUTE_TABLE_NAME

    # 上游必须是各生产者已经正式提交的精确契约数据集。
    calendar_dataset = open_exact_dataset(
        calendar_path,
        CALENDAR_PARTITIONING,
        FUTURES_BAR_CALENDAR_SCHEMA,
        CALENDAR_PARTITION_COLUMNS,
        "行情日历",
    )
    minute_dataset = open_exact_dataset(
        minute_path,
        MINUTE_PARTITIONING,
        FUTURES_MINUTE_SCHEMA,
        MINUTE_PARTITION_COLUMNS,
        "一分钟行情事实",
    )

    calendar_partition_keys = sorted(
        key
        for key in discover_partition_keys(
            calendar_path,
            CALENDAR_PARTITION_COLUMNS,
        )
        if key[0] == "1m"
    )

    # 先在任何 staging I/O 之前完成全局就绪门禁，避免把未采集 Session 误报为缺失。
    readiness_table = calendar_dataset.to_table(
        columns=[
            *CALENDAR_PRIMARY_KEY,
            "is_fetch_required",
            "is_fetch_completed",
        ],
        filter=(
            ds.field("bar_frequency") == "1m"
        ) & ds.field("is_fetch_required"),
    )
    readiness_df = readiness_table.to_pandas()
    if (
        not readiness_df.empty
        and not readiness_df["is_fetch_completed"].all()
    ):
        pending_examples = readiness_df.loc[
            readiness_df["is_fetch_completed"].ne(True),
            CALENDAR_PRIMARY_KEY,
        ].head(5)
        raise ValueError(
            "分钟缺失审计只能在全部 required Session 完成 c06 后运行；"
            f"示例：{pending_examples.to_dict(orient='records')}"
        )

    missing_staging_path.mkdir(parents=True, exist_ok=False)
    calendar_staging_path.mkdir(parents=True, exist_ok=False)

    # 即使本轮没有缺失，也写根级零行 marker，使正式表仍具有精确契约。
    missing_file_schema = parquet_file_schema(
        FUTURES_MISSING_BAR_SCHEMA,
        MISSING_PARTITION_COLUMNS,
    )
    pq.write_table(
        pa.Table.from_batches([], schema=missing_file_schema),
        missing_staging_path / "schema.parquet",
    )

    calendar_digests: dict[tuple[object, ...], str] = {}
    missing_digests: dict[tuple[object, ...], str] = {}
    total_sessions = 0
    total_expected = 0
    total_actual = 0
    total_missing = 0

    for calendar_partition_key in calendar_partition_keys:
        calendar_table = calendar_dataset.to_table(
            columns=FUTURES_BAR_CALENDAR_SCHEMA.names,
            filter=partition_expression(
                CALENDAR_PARTITION_COLUMNS,
                calendar_partition_key,
            ),
        )
        calendar_df = arrow_to_pandas(
            calendar_table,
            FUTURES_BAR_CALENDAR_SCHEMA,
        )

        selected_df = calendar_df.loc[
            calendar_df["bar_frequency"].eq("1m")
            & calendar_df["is_fetch_required"].eq(True)
        ].copy()
        if selected_df.empty:
            continue
        if not selected_df["is_fetch_completed"].all():
            pending_examples = selected_df.loc[
                selected_df["is_fetch_completed"].ne(True),
                CALENDAR_PRIMARY_KEY,
            ].head(5)
            raise ValueError(
                "分钟缺失审计只能在全部 required Session 完成 c06 后运行；"
                f"示例：{pending_examples.to_dict(orient='records')}"
            )

        _, exchange_code, year, month = calendar_partition_key

        # 日历完整叶只建立一次索引，后续按品种批量回写五个缺失字段。
        for underlying_code, selected_underlying_df in selected_df.groupby(
            "underlying_code",
            sort=True,
        ):
            minute_partition_key = (
                exchange_code,
                underlying_code,
                year,
                month,
            )
            minute_table = minute_dataset.to_table(
                # 事实侧严格只读取正式主键两列；Hive 分区列来自路径过滤。
                columns=MINUTE_KEY_COLUMNS,
                filter=partition_expression(
                    MINUTE_PARTITION_COLUMNS,
                    minute_partition_key,
                ),
            )
            actual_keys = (
                pl.from_arrow(minute_table)
                .rename({"bar_at": "expected_bar_at"})
                .select(["contract_code", "expected_bar_at"])
            )

            session_input = (
                selected_underlying_df.reset_index(
                    names="_calendar_index"
                )
                .loc[:, [
                    "_calendar_index",
                    "contract_code",
                    "exchange_code",
                    "underlying_code",
                    "trading_date",
                    "session_number",
                    "session_start_at",
                    "session_end_at",
                    "expected_bar_count",
                    "year",
                    "month",
                ]]
            )
            sessions = pl.from_pandas(session_input, include_index=False)

            # bar_at 采用分钟结束时刻语义，理论区间固定为 (start, end]。
            expected_points = (
                sessions.with_columns(
                    pl.datetime_ranges(
                        pl.col("session_start_at")
                        + pl.duration(minutes=1),
                        pl.col("session_end_at"),
                        interval="1m",
                        closed="both",
                    ).alias("expected_bar_at")
                )
                .explode("expected_bar_at")
                .with_columns(
                    pl.col("expected_bar_at").cast(
                        pl.Datetime("us", "Asia/Shanghai")
                    )
                )
            )
            expected_count = int(
                selected_underlying_df["expected_bar_count"].sum()
            )
            if expected_points.height != expected_count:
                raise ValueError(
                    f"{minute_partition_key} 理论时点展开条数与 c04 不一致。"
                )

            # c06 保证事实键唯一且属于理论 Session；c08 只做存在性反连接。
            missing_points = expected_points.join(
                actual_keys,
                on=["contract_code", "expected_bar_at"],
                how="anti",
            )
            missing_counts = {
                int(index): int(count)
                for index, count in (
                    missing_points.group_by("_calendar_index")
                    .len()
                    .select(["_calendar_index", "len"])
                    .iter_rows()
                )
            }

            calendar_indices = selected_underlying_df.index.to_list()
            expected_counts = (
                selected_underlying_df["expected_bar_count"]
                .astype("int64")
                .to_list()
            )
            missing_values = [
                missing_counts.get(int(index), 0)
                for index in calendar_indices
            ]
            actual_values = [
                expected_value - missing_value
                for expected_value, missing_value in zip(
                    expected_counts,
                    missing_values,
                    strict=True,
                )
            ]

            calendar_df.loc[
                calendar_indices,
                "actual_bar_count",
            ] = actual_values
            calendar_df.loc[
                calendar_indices,
                "is_data_missing",
            ] = [value > 0 for value in missing_values]
            calendar_df.loc[
                calendar_indices,
                "missing_bar_count",
            ] = missing_values
            calendar_df.loc[
                calendar_indices,
                "missing_checked_at",
            ] = detected_at
            calendar_df.loc[
                calendar_indices,
                "updated_at",
            ] = detected_at

            missing_partition_key = (
                "1m",
                exchange_code,
                underlying_code,
                year,
                month,
            )
            if missing_points.height:
                missing_output = (
                    missing_points.with_columns([
                        pl.lit("1m").alias("bar_frequency"),
                        pl.lit(detected_at).alias("detected_at"),
                    ])
                    .select(FUTURES_MISSING_BAR_SCHEMA.names)
                    .sort(MISSING_PRIMARY_KEY)
                )
                missing_arrow = polars_to_arrow(
                    missing_output,
                    FUTURES_MISSING_BAR_SCHEMA,
                )
                missing_arrow = validate_missing_output(
                    missing_arrow,
                    missing_partition_key,
                    "待写入 staging 的",
                )
                ds.write_dataset(
                    missing_arrow,
                    missing_staging_path,
                    format="parquet",
                    partitioning=MISSING_PARTITIONING,
                    existing_data_behavior="delete_matching",
                    basename_template="part-{i}.parquet",
                )

                staged_missing_dataset = ds.dataset(
                    missing_staging_path,
                    format="parquet",
                    partitioning=MISSING_PARTITIONING,
                )
                staged_missing_table = staged_missing_dataset.to_table(
                    columns=FUTURES_MISSING_BAR_SCHEMA.names,
                    filter=partition_expression(
                        MISSING_PARTITION_COLUMNS,
                        missing_partition_key,
                    ),
                )
                staged_missing_table = validate_missing_output(
                    staged_missing_table,
                    missing_partition_key,
                    "staging 复读的",
                )
                expected_digest = table_digest(
                    missing_arrow,
                    FUTURES_MISSING_BAR_SCHEMA,
                    MISSING_PRIMARY_KEY,
                )
                actual_digest = table_digest(
                    staged_missing_table,
                    FUTURES_MISSING_BAR_SCHEMA,
                    MISSING_PRIMARY_KEY,
                )
                if actual_digest != expected_digest:
                    raise ValueError(
                        "staging 缺失明细内容检查失败："
                        f"partition={missing_partition_key}"
                    )
                missing_digests[missing_partition_key] = expected_digest

            partition_missing = missing_points.height
            partition_actual = expected_count - partition_missing
            total_sessions += len(selected_underlying_df)
            total_expected += expected_count
            total_actual += partition_actual
            total_missing += partition_missing

        # 只在全部品种求差完成后提交本交易所月的完整日历叶。
        calendar_arrow = validate_calendar_audit_output(
            calendar_df,
            calendar_partition_key,
            detected_at,
            "待写入 staging 的",
        )
        ds.write_dataset(
            calendar_arrow,
            calendar_staging_path,
            format="parquet",
            partitioning=CALENDAR_PARTITIONING,
            existing_data_behavior="delete_matching",
            basename_template="part-{i}.parquet",
        )
        staged_calendar_dataset = ds.dataset(
            calendar_staging_path,
            format="parquet",
            partitioning=CALENDAR_PARTITIONING,
        )
        staged_calendar_table = staged_calendar_dataset.to_table(
            columns=FUTURES_BAR_CALENDAR_SCHEMA.names,
            filter=partition_expression(
                CALENDAR_PARTITION_COLUMNS,
                calendar_partition_key,
            ),
        )
        staged_calendar_table = validate_calendar_audit_output(
            arrow_to_pandas(
                staged_calendar_table,
                FUTURES_BAR_CALENDAR_SCHEMA,
            ),
            calendar_partition_key,
            detected_at,
            "staging 复读的",
        )
        expected_digest = table_digest(
            calendar_arrow,
            FUTURES_BAR_CALENDAR_SCHEMA,
            CALENDAR_PRIMARY_KEY,
        )
        actual_digest = table_digest(
            staged_calendar_table,
            FUTURES_BAR_CALENDAR_SCHEMA,
            CALENDAR_PRIMARY_KEY,
        )
        if actual_digest != expected_digest:
            raise ValueError(
                "staging 行情日历内容检查失败："
                f"partition={calendar_partition_key}"
            )
        calendar_digests[calendar_partition_key] = expected_digest

    staged_missing_dataset = open_exact_dataset(
        missing_staging_path,
        MISSING_PARTITIONING,
        FUTURES_MISSING_BAR_SCHEMA,
        MISSING_PARTITION_COLUMNS,
        "staging 缺失明细",
    )
    if staged_missing_dataset.count_rows() != total_missing:
        raise ValueError("staging 缺失明细总行数与主键求差汇总不一致。")

    if calendar_digests:
        open_exact_dataset(
            calendar_staging_path,
            CALENDAR_PARTITIONING,
            FUTURES_BAR_CALENDAR_SCHEMA,
            CALENDAR_PARTITION_COLUMNS,
            "staging 行情日历",
        )

    return {
        "calendar_digests": calendar_digests,
        "missing_digests": missing_digests,
        "total_sessions": total_sessions,
        "total_expected": total_expected,
        "total_actual": total_actual,
        "total_missing": total_missing,
    }


# ## 两表协调提交、正式复读与失败回滚
# 
# 缺失表采用全根替换，日历采用完整 1m 交易所月叶替换。任一正式复读失败时，两张表共同回滚。

# In[ ]:


def commit_full_audit(
    lake_root: pathlib.Path,
    missing_staging_path: pathlib.Path,
    calendar_staging_path: pathlib.Path,
    audit_result: dict[str, object],
    run_id: str,
) -> tuple[int, int]:
    silver_root = lake_root.resolve() / "silver"
    missing_target_path = silver_root / MISSING_TABLE_NAME
    calendar_target_path = silver_root / CALENDAR_TABLE_NAME
    short_run_id = run_id[:12]

    missing_backup_path = silver_root / f".c08-m-b-{short_run_id}"
    missing_quarantine_path = silver_root / f".c08-m-f-{short_run_id}"
    calendar_backup_path = silver_root / f".c08-c-b-{short_run_id}"
    calendar_quarantine_path = silver_root / f".c08-c-f-{short_run_id}"

    managed_paths = [
        missing_target_path,
        calendar_target_path,
        missing_staging_path,
        calendar_staging_path,
        missing_backup_path,
        missing_quarantine_path,
        calendar_backup_path,
        calendar_quarantine_path,
    ]
    for managed_path in managed_paths:
        if not managed_path.resolve().is_relative_to(silver_root):
            raise ValueError(
                f"数据集路径越出 silver 根目录：{managed_path}"
            )

    calendar_digests = audit_result["calendar_digests"]
    missing_digests = audit_result["missing_digests"]
    total_missing = int(audit_result["total_missing"])
    missing_had_existing = missing_target_path.exists()
    moved_calendar_partitions = []
    missing_moved = False
    cleanup_recovery_paths = True

    try:
        # 先替换缺失全表，再逐叶替换行情日历；备份一直保留到两表正式复读完成。
        if missing_had_existing:
            shutil.move(
                str(missing_target_path),
                str(missing_backup_path),
            )
        missing_moved = True
        shutil.move(
            str(missing_staging_path),
            str(missing_target_path),
        )

        calendar_backup_path.mkdir(parents=True, exist_ok=False)
        calendar_target_path.mkdir(parents=True, exist_ok=True)
        for partition_key in sorted(calendar_digests):
            relative_path = pathlib.Path(*[
                f"{name}={value}"
                for name, value in zip(
                    CALENDAR_PARTITION_COLUMNS,
                    partition_key,
                    strict=True,
                )
            ])
            source_path = calendar_staging_path / relative_path
            destination_path = calendar_target_path / relative_path
            saved_path = calendar_backup_path / relative_path
            if not source_path.is_dir():
                raise FileNotFoundError(
                    f"staging 缺少日历叶分区：{relative_path}"
                )

            destination_path.parent.mkdir(parents=True, exist_ok=True)
            saved_path.parent.mkdir(parents=True, exist_ok=True)
            had_existing = destination_path.exists()
            if had_existing:
                shutil.move(str(destination_path), str(saved_path))
            moved_calendar_partitions.append(
                (
                    destination_path,
                    saved_path,
                    relative_path,
                    had_existing,
                )
            )
            shutil.move(str(source_path), str(destination_path))

        committed_missing_dataset = open_exact_dataset(
            missing_target_path,
            MISSING_PARTITIONING,
            FUTURES_MISSING_BAR_SCHEMA,
            MISSING_PARTITION_COLUMNS,
            "正式缺失明细",
        )
        if committed_missing_dataset.count_rows() != total_missing:
            raise ValueError("正式缺失明细总行数与主键求差汇总不一致。")
        if discover_partition_keys(
            missing_target_path,
            MISSING_PARTITION_COLUMNS,
        ) != set(missing_digests):
            raise ValueError("正式缺失明细分区集合与 staging 不一致。")

        for partition_key, expected_digest in missing_digests.items():
            table = committed_missing_dataset.to_table(
                columns=FUTURES_MISSING_BAR_SCHEMA.names,
                filter=partition_expression(
                    MISSING_PARTITION_COLUMNS,
                    partition_key,
                ),
            )
            table = validate_missing_output(
                table,
                partition_key,
                "正式复读的",
            )
            if table_digest(
                table,
                FUTURES_MISSING_BAR_SCHEMA,
                MISSING_PRIMARY_KEY,
            ) != expected_digest:
                raise ValueError("正式缺失明细分区内容检查失败。")

        committed_calendar_dataset = open_exact_dataset(
            calendar_target_path,
            CALENDAR_PARTITIONING,
            FUTURES_BAR_CALENDAR_SCHEMA,
            CALENDAR_PARTITION_COLUMNS,
            "正式行情日历",
        )
        for partition_key, expected_digest in calendar_digests.items():
            table = committed_calendar_dataset.to_table(
                columns=FUTURES_BAR_CALENDAR_SCHEMA.names,
                filter=partition_expression(
                    CALENDAR_PARTITION_COLUMNS,
                    partition_key,
                ),
            )
            if table_digest(
                table,
                FUTURES_BAR_CALENDAR_SCHEMA,
                CALENDAR_PRIMARY_KEY,
            ) != expected_digest:
                raise ValueError("正式行情日历分区内容检查失败。")
    except Exception:
        try:
            # 逆序恢复日历叶，避免部分提交形成混合批次。
            for (
                destination_path,
                saved_path,
                relative_path,
                had_existing,
            ) in reversed(moved_calendar_partitions):
                if destination_path.exists():
                    failed_path = calendar_quarantine_path / relative_path
                    failed_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(destination_path), str(failed_path))
                if had_existing and saved_path.exists():
                    destination_path.parent.mkdir(
                        parents=True,
                        exist_ok=True,
                    )
                    shutil.move(str(saved_path), str(destination_path))

            if missing_moved and missing_target_path.exists():
                shutil.move(
                    str(missing_target_path),
                    str(missing_quarantine_path),
                )
            if missing_had_existing and missing_backup_path.exists():
                shutil.move(
                    str(missing_backup_path),
                    str(missing_target_path),
                )
        except Exception as rollback_error:
            cleanup_recovery_paths = False
            raise RuntimeError(
                "分钟主键缺失审计提交失败且自动回滚未完成；"
                "请保留并检查本批 backup/failed 路径。"
            ) from rollback_error
        raise
    finally:
        # 回滚本身失败时保留全部恢复依据；正常完成或完整回滚才清理。
        if cleanup_recovery_paths:
            shutil.rmtree(missing_staging_path, ignore_errors=True)
            shutil.rmtree(calendar_staging_path, ignore_errors=True)
            shutil.rmtree(missing_backup_path, ignore_errors=True)
            shutil.rmtree(missing_quarantine_path, ignore_errors=True)
            shutil.rmtree(calendar_backup_path, ignore_errors=True)
            shutil.rmtree(calendar_quarantine_path, ignore_errors=True)

    return total_missing, len(calendar_digests)


# ## CLI：人工确认、全量范围与是否写入
# 
# 正式湖默认来自环境配置。本入口不提供日期范围，也不访问任何外部 API。

# In[ ]:


@click.command()
@click.option("--lake-root", type=click.Path(path_type=pathlib.Path))
@click.option("--confirm-full-quality", is_flag=True, required=True)
@click.option("--write", is_flag=True)
def main(
    lake_root: pathlib.Path | None,
    confirm_full_quality: bool,
    write: bool,
) -> None:
    if not confirm_full_quality:
        raise click.UsageError(
            "分钟主键缺失审计必须显式传入 --confirm-full-quality。"
        )

    formal_lake_root = settings.futures_lake_root.resolve()
    resolved_lake_root = (lake_root or formal_lake_root).resolve()
    run_id = uuid.uuid4().hex
    detected_at = datetime.now(timezone.utc)

    temporary_directory = None
    if write:
        silver_root = resolved_lake_root / "silver"
        silver_root.mkdir(parents=True, exist_ok=True)
        short_run_id = run_id[:12]
        missing_staging_path = silver_root / f".c08-m-s-{short_run_id}"
        calendar_staging_path = silver_root / f".c08-c-s-{short_run_id}"
    else:
        temporary_directory = tempfile.TemporaryDirectory(
            prefix="latitude-c08-missing-"
        )
        temporary_root = pathlib.Path(temporary_directory.name)
        missing_staging_path = temporary_root / MISSING_TABLE_NAME
        calendar_staging_path = temporary_root / CALENDAR_TABLE_NAME

    click.echo(
        "missing_audit_scope=all_required_completed_1m_sessions; "
        f"lake_root={resolved_lake_root}; write={str(write).lower()}; "
        "minute_columns=contract_code,bar_at"
    )

    commit_attempted = False
    try:
        audit_result = build_full_audit_staging(
            resolved_lake_root,
            missing_staging_path,
            calendar_staging_path,
            detected_at,
        )

        click.echo(
            "missing_audit_total: "
            f"sessions={audit_result['total_sessions']}; "
            f"expected_keys={audit_result['total_expected']}; "
            f"actual_keys={audit_result['total_actual']}; "
            f"missing_keys={audit_result['total_missing']}; "
            f"calendar_partitions={len(audit_result['calendar_digests'])}"
        )

        if not write:
            click.echo(
                "write=false; committed_missing_rows=0; "
                "committed_calendar_partitions=0"
            )
            return

        commit_attempted = True
        committed_missing_rows, committed_calendar_partitions = (
            commit_full_audit(
                resolved_lake_root,
                missing_staging_path,
                calendar_staging_path,
                audit_result,
                run_id,
            )
        )
        click.echo(
            "write=true; "
            f"committed_missing_rows={committed_missing_rows}; "
            f"committed_calendar_partitions={committed_calendar_partitions}"
        )
    finally:
        if temporary_directory is not None:
            temporary_directory.cleanup()
        elif write and not commit_attempted:
            shutil.rmtree(missing_staging_path, ignore_errors=True)
            shutil.rmtree(calendar_staging_path, ignore_errors=True)


if __name__ == "__main__":
    main()

