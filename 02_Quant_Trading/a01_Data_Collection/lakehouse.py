"""Small Arrow/Hive helpers shared by the calendar pipeline."""

from __future__ import annotations

import shutil
import uuid
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds


def hive_partitioning(fields: list[pa.Field]) -> ds.Partitioning:
    return ds.partitioning(pa.schema(fields), flavor="hive")


def read_dataset(
    table_path: Path,
    partitioning: ds.Partitioning,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    if not table_path.exists():
        return pd.DataFrame()
    table = ds.dataset(
        table_path, format="parquet", partitioning=partitioning
    ).to_table(columns=columns)
    return table.to_pandas(types_mapper=pd.ArrowDtype)


def replace_dataset(
    table: pa.Table,
    table_path: Path,
    partitioning: ds.Partitioning,
) -> None:
    """Write a complete dataset beside the target, then replace it without backup."""
    table_path.parent.mkdir(parents=True, exist_ok=True)
    staging_path = table_path.with_name(f".{table_path.name}.{uuid.uuid4().hex}.tmp")
    ds.write_dataset(
        table,
        staging_path,
        format="parquet",
        partitioning=partitioning,
        basename_template="part-{i}.parquet",
    )
    if table_path.exists():
        shutil.rmtree(table_path)
    shutil.move(str(staging_path), str(table_path))



