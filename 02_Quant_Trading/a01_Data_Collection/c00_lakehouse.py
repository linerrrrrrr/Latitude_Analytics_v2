"""日历采集链路共用的轻量级 Arrow/Hive 辅助函数。"""

from __future__ import annotations

import shutil
import uuid
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from config.data_contracts import arrow_to_pandas, arrow_to_polars, validate_arrow_table


def hive_partitioning(fields: list[pa.Field]) -> ds.Partitioning:
    return ds.partitioning(pa.schema(fields), flavor="hive")


def _select_schema(schema: pa.Schema, columns: list[str] | None) -> pa.Schema:
    """按指定列及顺序从完整 Schema 派生读取 Schema。"""
    if columns is None:
        return schema
    missing_columns = [column for column in columns if column not in schema.names]
    if missing_columns:
        raise ValueError(f"Schema 中不存在以下列：{missing_columns}。")
    return pa.schema([schema.field(column) for column in columns], metadata=schema.metadata)


def read_arrow_dataset(
    table_path: Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    columns: list[str] | None = None,
    filter_expression: ds.Expression | None = None,
) -> pa.Table:
    """读取数据集，并按完整或派生 Schema 返回 Arrow Table。"""
    selected_schema = _select_schema(schema, columns)
    if not table_path.exists():
        return pa.Table.from_batches([], schema=selected_schema)
    table = ds.dataset(
        table_path, format="parquet", partitioning=partitioning
    ).to_table(columns=selected_schema.names, filter=filter_expression)
    return validate_arrow_table(table, selected_schema)


def dataset_partitions(
    table_path: Path,
    partition_fields: list[str],
) -> list[dict[str, Any]]:
    """从数据集 fragment 的 Hive 表达式中提取已有分区。"""
    if not table_path.exists():
        return []
    dataset = ds.dataset(table_path, format="parquet", partitioning="hive")
    partitions = {
        tuple(ds.get_partition_keys(fragment.partition_expression).get(field)
              for field in partition_fields)
        for fragment in dataset.get_fragments()
    }
    return [
        dict(zip(partition_fields, values, strict=True))
        for values in sorted(partitions)
        if all(value is not None for value in values)
    ]


def validate_dataset_streaming(
    table_path: Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    expected_row_count: int | None = None,
) -> int:
    """分批复读整个 Parquet 数据集，避免为校验一次性物化全表。"""
    dataset = ds.dataset(
        table_path,
        format="parquet",
        partitioning=partitioning,
    )
    row_count = 0
    for record_batch in dataset.to_batches(columns=schema.names, batch_size=131_072):
        batch_table = pa.Table.from_batches([record_batch], schema=record_batch.schema)
        validate_arrow_table(batch_table, schema)
        row_count += record_batch.num_rows
    if expected_row_count is not None and row_count != expected_row_count:
        raise ValueError(
            "Parquet 写后复读行数不一致："
            f"期望 {expected_row_count}，实际 {row_count}。"
        )
    return row_count


def swap_staged_dataset(staging_path: Path, table_path: Path) -> None:
    """用已校验的临时目录替换正式目录，失败时恢复旧目录。"""
    if not staging_path.exists():
        raise FileNotFoundError(f"临时数据集不存在：{staging_path}")
    table_path.parent.mkdir(parents=True, exist_ok=True)
    backup_path = table_path.with_name(f".{table_path.name}.{uuid.uuid4().hex}.bak")
    moved_old = False
    try:
        if table_path.exists():
            shutil.move(str(table_path), str(backup_path))
            moved_old = True
        shutil.move(str(staging_path), str(table_path))
    except Exception:
        if table_path.exists() and moved_old:
            shutil.rmtree(table_path)
        if moved_old and backup_path.exists():
            shutil.move(str(backup_path), str(table_path))
        raise
    else:
        if backup_path.exists():
            shutil.rmtree(backup_path)


def replace_partition(
    table: pa.Table,
    table_path: Path,
    partitioning: ds.Partitioning,
    partition_fields: list[str],
    schema: pa.Schema,
    partition_values: dict[str, object] | None = None,
) -> None:
    """先在旁路目录写好单个 Hive 叶分区，再安全替换该叶目录。"""
    table = validate_arrow_table(table, schema)
    resolved_partition_values: list[str] = []
    if table.num_rows == 0:
        if partition_values is None:
            raise ValueError("空表替换必须显式提供 partition_values。")
        missing_fields = set(partition_fields) - set(partition_values)
        if missing_fields:
            raise ValueError(f"空表替换缺少分区值：{sorted(missing_fields)}")
        resolved_partition_values = [str(partition_values[field]) for field in partition_fields]
    else:
        for field in partition_fields:
            values = table[field].unique().to_pylist()
            if len(values) != 1:
                raise ValueError(f"字段 {field!r} 不是单一分区值：{values}")
            resolved_partition_values.append(str(values[0]))
        if partition_values is not None:
            supplied_values = [str(partition_values[field]) for field in partition_fields]
            if supplied_values != resolved_partition_values:
                raise ValueError(
                    "显式分区值与表内分区值不一致："
                    f"{supplied_values} != {resolved_partition_values}"
                )

    table_path.parent.mkdir(parents=True, exist_ok=True)
    staging_root = table_path.with_name(
        f".{table_path.name}.partition.{uuid.uuid4().hex}.tmp"
    )
    relative_leaf = Path(
        *[
            f"{field}={value}"
            for field, value in zip(
                partition_fields, resolved_partition_values, strict=True
            )
        ]
    )
    target_leaf = table_path / relative_leaf
    staging_leaf = staging_root / relative_leaf
    backup_leaf = target_leaf.with_name(f".{target_leaf.name}.{uuid.uuid4().hex}.bak")
    try:
        if table.num_rows:
            ds.write_dataset(
                table,
                staging_root,
                format="parquet",
                partitioning=partitioning,
                basename_template="part-{i}.parquet",
            )
        else:
            staging_leaf.mkdir(parents=True, exist_ok=True)
            payload_fields = [
                field for field in schema.names if field not in partition_fields
            ]
            pq.write_table(table.select(payload_fields), staging_leaf / "part-0.parquet")
        if not staging_leaf.exists():
            raise RuntimeError(f"未生成预期临时分区：{staging_leaf}")
        target_leaf.parent.mkdir(parents=True, exist_ok=True)
        moved_old = False
        try:
            if target_leaf.exists():
                shutil.move(str(target_leaf), str(backup_leaf))
                moved_old = True
            shutil.move(str(staging_leaf), str(target_leaf))
        except Exception:
            if target_leaf.exists() and moved_old:
                shutil.rmtree(target_leaf)
            if moved_old and backup_leaf.exists():
                shutil.move(str(backup_leaf), str(target_leaf))
            raise
        else:
            if backup_leaf.exists():
                shutil.rmtree(backup_leaf)
    finally:
        if staging_root.exists():
            shutil.rmtree(staging_root)


def read_dataset(
    table_path: Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    columns: list[str] | None = None,
    filter_expression: ds.Expression | None = None,
) -> pd.DataFrame:
    """读取并返回具有 Arrow 扩展类型的 Pandas DataFrame。"""
    table = read_arrow_dataset(
        table_path, partitioning, schema, columns, filter_expression
    )
    return arrow_to_pandas(table, table.schema)


def read_dataset_polars(
    table_path: Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    columns: list[str] | None = None,
    filter_expression: ds.Expression | None = None,
) -> pl.DataFrame:
    """读取并返回符合相同 Arrow Schema 的 Polars DataFrame。"""
    table = read_arrow_dataset(
        table_path, partitioning, schema, columns, filter_expression
    )
    return arrow_to_polars(table, table.schema)


def replace_dataset(
    table: pa.Table,
    table_path: Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
) -> None:
    """在目标旁写入、复读校验，再以可恢复方式替换正式数据集。"""
    table = validate_arrow_table(table, schema)
    table_path.parent.mkdir(parents=True, exist_ok=True)
    staging_path = table_path.with_name(f".{table_path.name}.{uuid.uuid4().hex}.tmp")
    try:
        if table.num_rows:
            ds.write_dataset(
                table,
                staging_path,
                format="parquet",
                partitioning=partitioning,
                basename_template="part-{i}.parquet",
            )
        else:
            staging_path.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, staging_path / "part-0.parquet")
        validate_dataset_streaming(
            staging_path,
            partitioning,
            schema,
            expected_row_count=table.num_rows,
        )
        swap_staged_dataset(staging_path, table_path)
    finally:
        if staging_path.exists():
            shutil.rmtree(staging_path)
