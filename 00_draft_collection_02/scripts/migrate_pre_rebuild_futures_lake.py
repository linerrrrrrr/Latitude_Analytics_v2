"""把重建前 futures silver 湖离线迁移到当前 17 表契约。

这是一次性草稿工具，不是日常生产入口。它有两个互斥模式：

* ``--plan`` 只读取目录和 Parquet footer，绝不创建目录或修改文件；
* ``--execute`` 在同一 E: 卷建立候选湖，完整复读后交换七张表根目录。

分钟事实是本次迁移的绝大部分。对于不和当前湖重叠的旧分钟叶分区，
工具只重建 Parquet footer 中的 Arrow Schema/metadata，再用同卷 rename 移动；
不会重新压缩 5 亿多行分钟值。其他表体量远小于分钟事实，按当前 Schema
逐叶重写，以完成字段补充、分区调整、来源规范化和当前行优先去重。
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import pathlib
import shutil
import struct
import subprocess
import sys
import tempfile
import time
import traceback
import uuid
from datetime import datetime, timezone
from typing import Iterable

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq


PROJECT_MARKERS = [".git", ".env", "config/settings.py"]
CURRENT_PATH = pathlib.Path.cwd().resolve()
for CANDIDATE_ROOT in [CURRENT_PATH, *CURRENT_PATH.parents]:
    if all((CANDIDATE_ROOT / marker).exists() for marker in PROJECT_MARKERS):
        PROJECT_ROOT = CANDIDATE_ROOT
        sys.path.insert(0, str(PROJECT_ROOT))
        break
else:
    raise RuntimeError("未找到项目根目录")

from config.data_contracts import (  # noqa: E402
    FUTURES_BAR_CALENDAR_SCHEMA,
    FUTURES_CONTRACT_CALENDAR_SCHEMA,
    FUTURES_DAILY_SCHEMA,
    FUTURES_MINUTE_SCHEMA,
    FUTURES_MISSING_BAR_SCHEMA,
    FUTURES_VARIETY_CALENDAR_SCHEMA,
    TRADE_CALENDAR_SCHEMA,
    arrow_to_pandas,
    pandas_to_arrow,
)
from config.futures_lakehouse.futures_fact_collection_policy import (  # noqa: E402
    FUTURES_FACT_VARIETY_PAIRS,
)
from config.settings import settings  # noqa: E402


DEFAULT_OLD_SILVER = (
    PROJECT_ROOT
    / "05_Old_Projects"
    / "futures_lake_pre_rebuild_20260810"
    / "silver"
)

AFFECTED_TABLES = [
    "dim_trade_calendar",
    "dim_futures_variety_calendar",
    "dim_futures_contract_calendar",
    "dim_futures_bar_calendar",
    "fact_futures_daily",
    "fact_futures_minute",
    "fact_futures_missing_bar",
]

TABLE_SCHEMAS = {
    "dim_trade_calendar": TRADE_CALENDAR_SCHEMA,
    "dim_futures_variety_calendar": FUTURES_VARIETY_CALENDAR_SCHEMA,
    "dim_futures_contract_calendar": FUTURES_CONTRACT_CALENDAR_SCHEMA,
    "dim_futures_bar_calendar": FUTURES_BAR_CALENDAR_SCHEMA,
    "fact_futures_daily": FUTURES_DAILY_SCHEMA,
    "fact_futures_minute": FUTURES_MINUTE_SCHEMA,
    "fact_futures_missing_bar": FUTURES_MISSING_BAR_SCHEMA,
}

POST_C08_EXACT_ROWS = {
    "dim_trade_calendar": 6069,
    "dim_futures_variety_calendar": 204883,
    "dim_futures_contract_calendar": 7451291,
    "dim_futures_bar_calendar": 9506158,
    "fact_futures_daily": 2054247,
    "fact_futures_minute": 517390417,
    "fact_futures_missing_bar": 8951963,
}
POST_C08_MISSING_PRESERVED = ".missing-bar-before-resume"
POST_C08_CHECKPOINT = "post-c08-swap-checkpoint.json"

SOURCE_NORMALIZATION = {
    "dim_trade_calendar": "JQData_get_trade_days",
    "dim_futures_variety_calendar": (
        "JQData_get_all_securities+dim_trade_calendar"
    ),
    "dim_futures_contract_calendar": (
        "JQData_get_all_securities+get_futures_info"
    ),
}


def load_current_module(filename: str):
    """只加载当前生产实现；绝不 import 或执行旧归档代码。"""
    path = (
        PROJECT_ROOT
        / "02_Futures_Lakehouse"
        / "a01_Futures_Market_Data"
        / filename
    )
    module_name = f"legacy_migration_{path.stem}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载当前生产模块：{path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


C01 = load_current_module("b01_trade_calendar.py")
C03 = load_current_module("b03_futures_contract_calendar.py")
C04 = load_current_module("b04_futures_bar_calendar.py")
C05 = load_current_module("b05_futures_daily.py")
C06 = load_current_module("b06_futures_minute.py")
C07 = load_current_module("b07_suspected_session_reconciliation.py")
C08 = load_current_module("b08_full_minute_quality.py")


def schema_metadata_list(schema: pa.Schema, key: bytes) -> list[str]:
    value = schema.metadata.get(key) if schema.metadata else None
    if not value:
        raise ValueError(f"Schema 缺少 metadata：{key!r}")
    return value.decode("utf-8").split(",")


def partition_columns(schema: pa.Schema) -> list[str]:
    return schema_metadata_list(schema, b"partition_columns")


def primary_key(schema: pa.Schema) -> list[str]:
    return schema_metadata_list(schema, b"primary_key")


def file_schema(schema: pa.Schema) -> pa.Schema:
    partitions = set(partition_columns(schema))
    return pa.schema(
        [field for field in schema if field.name not in partitions],
        metadata=schema.metadata,
    )


def hive_partitioning(schema: pa.Schema) -> ds.Partitioning:
    return ds.partitioning(
        pa.schema([schema.field(name) for name in partition_columns(schema)]),
        flavor="hive",
    )


def parse_leaf_key(
    table_root: pathlib.Path,
    leaf: pathlib.Path,
    columns: list[str],
) -> tuple[object, ...]:
    values: dict[str, str] = {}
    for part in leaf.relative_to(table_root).parts:
        if "=" in part:
            name, value = part.split("=", 1)
            values[name] = value
    result: list[object] = []
    for name in columns:
        raw = values[name]
        if name in {"year", "month", "session_number"}:
            result.append(int(raw))
        else:
            result.append(raw)
    return tuple(result)


def leaf_directories(
    table_root: pathlib.Path,
    columns: list[str],
) -> dict[tuple[object, ...], pathlib.Path]:
    if not table_root.is_dir():
        return {}
    leaves: dict[tuple[object, ...], pathlib.Path] = {}
    for parquet_path in table_root.rglob("*.parquet"):
        if parquet_path.parent == table_root:
            continue
        key = parse_leaf_key(table_root, parquet_path.parent, columns)
        existing = leaves.get(key)
        if existing is not None and existing != parquet_path.parent:
            raise ValueError(f"分区键映射到多个目录：{key}")
        leaves[key] = parquet_path.parent
    return leaves


def read_leaf(
    table_root: pathlib.Path,
    leaf: pathlib.Path,
    schema: pa.Schema,
) -> pd.DataFrame:
    tables = [
        pq.ParquetFile(path).read()
        for path in sorted(leaf.glob("*.parquet"))
    ]
    if not tables:
        return arrow_to_pandas(
            pa.Table.from_batches([], schema=schema),
            schema,
        )
    table = pa.concat_tables(tables, promote_options="default")
    key = parse_leaf_key(table_root, leaf, partition_columns(schema))
    # Arrow nullable 类型保留 pd.NA；默认 NumPy 转换会把真实 Parquet null
    # 变成 NaN，进而被当前生产者正确的“拒绝来源 NaN”门禁误判。
    frame = table.to_pandas(types_mapper=pd.ArrowDtype)
    for name, value in zip(partition_columns(schema), key, strict=True):
        frame[name] = value
    return frame


def replace_leaf(
    table_root: pathlib.Path,
    key: tuple[object, ...],
    frame: pd.DataFrame,
    schema: pa.Schema,
    run_id: str,
) -> pathlib.Path:
    columns = partition_columns(schema)
    relative = pathlib.Path(
        *(f"{name}={value}" for name, value in zip(columns, key, strict=True))
    )
    target = table_root / relative
    staging_root = table_root.parent / f".{table_root.name}.leaf-{run_id}"
    if staging_root.exists():
        shutil.rmtree(staging_root)
    staging_root.mkdir(parents=True)

    table = pandas_to_arrow(frame.loc[:, schema.names], schema)
    ds.write_dataset(
        table,
        staging_root,
        format="parquet",
        partitioning=hive_partitioning(schema),
        existing_data_behavior="delete_matching",
        basename_template="part-{i}.parquet",
    )
    staged_leaf = staging_root / relative
    if not staged_leaf.is_dir():
        raise RuntimeError(f"staging 未生成预期叶分区：{staged_leaf}")

    read_back = read_leaf(staging_root, staged_leaf, schema)
    expected = pandas_to_arrow(
        frame.sort_values(primary_key(schema)).reset_index(drop=True),
        schema,
    )
    actual = pandas_to_arrow(
        read_back.loc[:, schema.names]
        .sort_values(primary_key(schema))
        .reset_index(drop=True),
        schema,
    )
    if not actual.equals(expected):
        raise ValueError(f"staging 逐值复读失败：{table_root.name} {key}")

    displaced = table_root.parent / f".{table_root.name}.old-leaf-{run_id}"
    if displaced.exists():
        shutil.rmtree(displaced)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        target.rename(displaced)
    try:
        staged_leaf.rename(target)
    except BaseException:
        if displaced.exists() and not target.exists():
            displaced.rename(target)
        raise
    finally:
        if staging_root.exists():
            shutil.rmtree(staging_root)
    if displaced.exists():
        shutil.rmtree(displaced)
    return target


def write_schema_marker(table_root: pathlib.Path, schema: pa.Schema) -> None:
    table_root.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_batches([], schema=file_schema(schema)),
        table_root / "schema.parquet",
    )


def append_event(event_path: pathlib.Path, payload: dict[str, object]) -> None:
    payload = {
        "at": datetime.now(timezone.utc).isoformat(),
        **payload,
    }
    with event_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def parquet_footer(path: pathlib.Path) -> tuple[int, bytes]:
    size = path.stat().st_size
    with path.open("rb") as handle:
        handle.seek(-8, os.SEEK_END)
        tail = handle.read(8)
        if len(tail) != 8 or tail[4:] != b"PAR1":
            raise ValueError(f"不是完整 Parquet 文件：{path}")
        footer_length = struct.unpack("<I", tail[:4])[0]
        footer_start = size - 8 - footer_length
        if footer_start < 4:
            raise ValueError(f"Parquet footer 边界非法：{path}")
        handle.seek(footer_start)
        return footer_start, handle.read(size - footer_start)


def build_replacement_footer(path: pathlib.Path, schema: pa.Schema) -> bytes:
    original_metadata = pq.ParquetFile(path).metadata
    with tempfile.TemporaryDirectory(prefix="latitude-footer-") as directory:
        seed = pathlib.Path(directory) / "seed.parquet"
        rebuilt = pathlib.Path(directory) / "rebuilt.parquet"
        pq.write_metadata(file_schema(schema), seed)
        metadata = pq.ParquetFile(seed).metadata
        metadata.append_row_groups(original_metadata)
        metadata.write_metadata_file(rebuilt)
        replacement = rebuilt.read_bytes()
    if replacement[:4] != b"PAR1" or replacement[-4:] != b"PAR1":
        raise ValueError("重建的 Parquet footer 容器不完整。")
    return replacement[4:]


def journal_footer(
    journal_handle,
    relative_path: str,
    footer_start: int,
    old_footer: bytes,
) -> None:
    encoded_path = relative_path.encode("utf-8")
    journal_handle.write(struct.pack("<I", len(encoded_path)))
    journal_handle.write(encoded_path)
    journal_handle.write(struct.pack("<Q", footer_start))
    journal_handle.write(struct.pack("<Q", len(old_footer)))
    journal_handle.write(old_footer)
    journal_handle.flush()
    os.fsync(journal_handle.fileno())


def read_footer_journal(
    journal_path: pathlib.Path,
) -> list[tuple[str, int, bytes]]:
    records = []
    if not journal_path.is_file():
        return records
    with journal_path.open("rb") as handle:
        while True:
            prefix = handle.read(4)
            if not prefix:
                break
            if len(prefix) != 4:
                raise ValueError("footer journal 尾部不完整。")
            path_length = struct.unpack("<I", prefix)[0]
            relative_path = handle.read(path_length).decode("utf-8")
            footer_start = struct.unpack("<Q", handle.read(8))[0]
            footer_length = struct.unpack("<Q", handle.read(8))[0]
            footer = handle.read(footer_length)
            if len(footer) != footer_length:
                raise ValueError("footer journal 记录不完整。")
            records.append((relative_path, footer_start, footer))
    return records


def patch_parquet_footer(
    path: pathlib.Path,
    schema: pa.Schema,
    journal_handle=None,
    journal_relative_path: str | None = None,
) -> None:
    old_rows = pq.ParquetFile(path).metadata.num_rows
    footer_start, old_footer = parquet_footer(path)
    replacement = build_replacement_footer(path, schema)
    if journal_handle is not None:
        if journal_relative_path is None:
            raise ValueError("写 footer journal 时必须提供相对路径。")
        journal_footer(
            journal_handle,
            journal_relative_path,
            footer_start,
            old_footer,
        )
    with path.open("r+b") as handle:
        handle.truncate(footer_start)
        handle.seek(footer_start)
        handle.write(replacement)
        handle.flush()
        os.fsync(handle.fileno())
    parquet = pq.ParquetFile(path)
    if parquet.metadata.num_rows != old_rows:
        raise ValueError(f"footer 更新改变了行数：{path}")
    if not pq.read_schema(path).equals(file_schema(schema), check_metadata=True):
        raise TypeError(f"footer 更新后 Schema/metadata 不精确：{path}")


def restore_footer_journal(
    old_minute_root: pathlib.Path,
    candidate_minute_root: pathlib.Path,
    journal_path: pathlib.Path,
) -> None:
    for relative_path, footer_start, old_footer in reversed(
        read_footer_journal(journal_path)
    ):
        relative = pathlib.Path(relative_path)
        candidates = [old_minute_root / relative, candidate_minute_root / relative]
        existing = [path for path in candidates if path.is_file()]
        if len(existing) != 1:
            raise RuntimeError(
                f"恢复 footer 时无法唯一定位文件：{relative_path} -> {existing}"
            )
        path = existing[0]
        with path.open("r+b") as handle:
            handle.truncate(footer_start)
            handle.seek(footer_start)
            handle.write(old_footer)
            handle.flush()
            os.fsync(handle.fileno())


def copy_tree(source: pathlib.Path, target: pathlib.Path) -> None:
    if not source.is_dir():
        return
    for source_path in source.rglob("*"):
        relative = source_path.relative_to(source)
        target_path = target / relative
        if source_path.is_dir():
            target_path.mkdir(parents=True, exist_ok=True)
        else:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_path, target_path)


def validate_small_table(
    table_name: str,
    frame: pd.DataFrame,
) -> pd.DataFrame:
    schema = TABLE_SCHEMAS[table_name]
    frame = frame.loc[:, schema.names]
    if table_name == "dim_trade_calendar":
        checked = C01.validate_calendar_table(
            pandas_to_arrow(frame, schema),
            require_contiguous=False,
        )
        return arrow_to_pandas(checked, schema)
    if table_name == "dim_futures_variety_calendar":
        checked = arrow_to_pandas(pandas_to_arrow(frame, schema), schema)
        if checked.duplicated(primary_key(schema)).any():
            raise ValueError("品种日历主键不唯一。")
        if (checked["active_contract_count"] <= 0).any():
            raise ValueError("品种日历 active_contract_count 必须大于 0。")
        if not checked["source"].eq(SOURCE_NORMALIZATION[table_name]).all():
            raise ValueError("品种日历来源未规范化。")
        return checked
    if table_name == "dim_futures_contract_calendar":
        return C03.validate_contract_calendar_frame(frame, "迁移合约日历")
    if table_name == "fact_futures_daily":
        return C05.validate_daily_frame(frame, "迁移日线事实")
    raise ValueError(f"没有表级校验器：{table_name}")


def migrate_rewritten_table(
    table_name: str,
    old_silver: pathlib.Path,
    formal_silver: pathlib.Path,
    candidate_silver: pathlib.Path,
    run_id: str,
    event_path: pathlib.Path,
) -> dict[str, int]:
    schema = TABLE_SCHEMAS[table_name]
    columns = partition_columns(schema)
    old_root = old_silver / table_name
    current_root = formal_silver / table_name
    target_root = candidate_silver / table_name
    old_leaves = leaf_directories(old_root, columns)
    current_leaves = leaf_directories(current_root, columns)
    target_root.mkdir(parents=True, exist_ok=False)
    rows = 0

    for key in sorted(set(old_leaves) | set(current_leaves)):
        frames = []
        if key in old_leaves:
            old_frame = read_leaf(old_root, old_leaves[key], schema)
            if table_name == "fact_futures_daily":
                for name in schema.names:
                    if name not in old_frame.columns:
                        old_frame[name] = None
            if table_name in SOURCE_NORMALIZATION:
                old_frame["source"] = SOURCE_NORMALIZATION[table_name]
            if table_name == "dim_trade_calendar":
                old_frame["calendar_name"] = "CN_FUTURES_MARKET"
            frames.append(old_frame.loc[:, schema.names])
        if key in current_leaves:
            current_frame = read_leaf(current_root, current_leaves[key], schema)
            if table_name in SOURCE_NORMALIZATION:
                current_frame["source"] = SOURCE_NORMALIZATION[table_name]
            if table_name == "dim_trade_calendar":
                current_frame["calendar_name"] = "CN_FUTURES_MARKET"
            frames.append(current_frame.loc[:, schema.names])

        merged = pd.concat(frames, ignore_index=True)
        merged = merged.drop_duplicates(primary_key(schema), keep="last")
        merged = merged.sort_values(primary_key(schema)).reset_index(drop=True)
        checked = validate_small_table(table_name, merged)
        replace_leaf(target_root, key, checked, schema, run_id)
        rows += len(checked)
        append_event(
            event_path,
            {
                "action": "rewritten_leaf",
                "table": table_name,
                "partition": list(key),
                "rows": len(checked),
            },
        )

    write_schema_marker(target_root, schema)
    return {"rows": rows, "partitions": len(set(old_leaves) | set(current_leaves))}


def migrate_daily_table(
    old_silver: pathlib.Path,
    formal_silver: pathlib.Path,
    candidate_silver: pathlib.Path,
    run_id: str,
    event_path: pathlib.Path,
) -> dict[str, int]:
    """把旧三层日线分区改写为当前含 underlying_code 的四层分区。"""
    table_name = "fact_futures_daily"
    schema = FUTURES_DAILY_SCHEMA
    old_root = old_silver / table_name
    current_root = formal_silver / table_name
    target_root = candidate_silver / table_name
    target_root.mkdir(parents=True, exist_ok=False)
    old_columns = ["exchange_code", "year", "month"]
    old_leaves = leaf_directories(old_root, old_columns)
    current_leaves = leaf_directories(
        current_root,
        partition_columns(schema),
    )
    processed: set[tuple[object, ...]] = set()
    rows = 0

    for base_key, old_leaf in sorted(old_leaves.items()):
        tables = [
            pq.ParquetFile(path).read()
            for path in sorted(old_leaf.glob("*.parquet"))
        ]
        old_frame = pa.concat_tables(
            tables,
            promote_options="default",
        ).to_pandas(types_mapper=pd.ArrowDtype)
        for name, value in zip(old_columns, base_key, strict=True):
            old_frame[name] = value
        for name in schema.names:
            if name not in old_frame.columns:
                old_frame[name] = None

        for underlying_code, underlying_frame in old_frame.groupby(
            "underlying_code",
            sort=True,
        ):
            key = (base_key[0], str(underlying_code), base_key[1], base_key[2])
            frames = [underlying_frame.loc[:, schema.names]]
            if key in current_leaves:
                frames.append(
                    read_leaf(current_root, current_leaves[key], schema).loc[
                        :, schema.names
                    ]
                )
            merged = pd.concat(frames, ignore_index=True)
            merged = merged.drop_duplicates(primary_key(schema), keep="last")
            merged = merged.sort_values(primary_key(schema)).reset_index(drop=True)
            checked = validate_small_table(table_name, merged)
            replace_leaf(target_root, key, checked, schema, run_id)
            processed.add(key)
            rows += len(checked)
            append_event(
                event_path,
                {
                    "action": "rewritten_daily_leaf",
                    "partition": list(key),
                    "rows": len(checked),
                },
            )

    for key in sorted(set(current_leaves) - processed):
        frame = read_leaf(current_root, current_leaves[key], schema)
        checked = validate_small_table(table_name, frame)
        replace_leaf(target_root, key, checked, schema, run_id)
        rows += len(checked)
        processed.add(key)
        append_event(
            event_path,
            {
                "action": "rewritten_current_only_daily_leaf",
                "partition": list(key),
                "rows": len(checked),
            },
        )

    write_schema_marker(target_root, schema)
    return {"rows": rows, "partitions": len(processed)}


def merge_minute_table(
    old_silver: pathlib.Path,
    formal_silver: pathlib.Path,
    candidate_silver: pathlib.Path,
    transaction_root: pathlib.Path,
    run_id: str,
    event_path: pathlib.Path,
) -> dict[str, int]:
    schema = FUTURES_MINUTE_SCHEMA
    columns = partition_columns(schema)
    old_root = old_silver / "fact_futures_minute"
    current_root = formal_silver / "fact_futures_minute"
    target_root = candidate_silver / "fact_futures_minute"
    copy_tree(current_root, target_root)

    # 当前湖很小，但 metadata 可能早于 1.5.0；在候选副本上统一 footer。
    for path in sorted(target_root.rglob("*.parquet")):
        patch_parquet_footer(path, schema)

    old_leaves = leaf_directories(old_root, columns)
    current_leaves = leaf_directories(current_root, columns)
    overlap = sorted(set(old_leaves) & set(current_leaves))
    non_overlap = sorted(set(old_leaves) - set(current_leaves))
    footer_journal_path = transaction_root / "minute_footer_journal.bin"
    moved_leaves = 0
    patched_files = 0

    with footer_journal_path.open("ab") as journal_handle:
        for key in non_overlap:
            source_leaf = old_leaves[key]
            relative_leaf = source_leaf.relative_to(old_root)
            for path in sorted(source_leaf.glob("*.parquet")):
                relative_file = path.relative_to(old_root).as_posix()
                patch_parquet_footer(
                    path,
                    schema,
                    journal_handle,
                    relative_file,
                )
                patched_files += 1
                append_event(
                    event_path,
                    {
                        "action": "patched_old_minute_footer",
                        "relative_path": relative_file,
                    },
                )
            target_leaf = target_root / relative_leaf
            target_leaf.parent.mkdir(parents=True, exist_ok=True)
            if target_leaf.exists():
                raise FileExistsError(f"非重叠分钟目标已存在：{target_leaf}")
            source_leaf.rename(target_leaf)
            moved_leaves += 1
            append_event(
                event_path,
                {
                    "action": "moved_old_minute_leaf",
                    "relative_path": relative_leaf.as_posix(),
                },
            )

    # 重叠叶只占当前 2026-08 小窗口；这里允许一次正常重写并让当前 PK 优先。
    for key in overlap:
        old_frame = read_leaf(old_root, old_leaves[key], schema)
        current_frame = read_leaf(current_root, current_leaves[key], schema)
        merged = pd.concat([old_frame, current_frame], ignore_index=True)
        merged = merged.drop_duplicates(primary_key(schema), keep="last")
        merged = C06.validate_minute_frame(merged, "重叠分钟叶迁移")
        replace_leaf(target_root, key, merged, schema, run_id)
        append_event(
            event_path,
            {
                "action": "rewritten_overlap_minute_leaf",
                "partition": list(key),
                "rows": len(merged),
            },
        )

    write_schema_marker(target_root, schema)
    return {
        "non_overlap_moved": moved_leaves,
        "overlap_rewritten": len(overlap),
        "old_files_footer_patched": patched_files,
    }


def run_current_entry(
    filename: str,
    lake_root: pathlib.Path,
    arguments: list[str],
    log_path: pathlib.Path,
) -> None:
    script = (
        PROJECT_ROOT
        / "02_Futures_Lakehouse"
        / "a01_Futures_Market_Data"
        / filename
    )
    command = [
        str(pathlib.Path(sys.executable)),
        str(script),
        "--lake-root",
        str(lake_root),
        *arguments,
    ]
    with log_path.open("w", encoding="utf-8") as log_handle:
        completed = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            check=False,
        )
    if completed.returncode != 0:
        tail = log_path.read_text(encoding="utf-8", errors="replace")[-8000:]
        raise RuntimeError(
            f"当前入口执行失败：{filename} code={completed.returncode}\n{tail}"
        )


def old_status_frame(
    old_status_root: pathlib.Path,
    frequency: str,
    exchange_code: str,
    year: int,
    month: int,
) -> pd.DataFrame:
    leaf = (
        old_status_root
        / f"bar_frequency={frequency}"
        / f"exchange_code={exchange_code}"
        / f"year={year}"
        / f"month={month}"
    )
    frames = []
    for path in sorted(leaf.glob("*.parquet")):
        frame = pq.ParquetFile(path).read().to_pandas()
        frame["bar_frequency"] = frequency
        frame["exchange_code"] = exchange_code
        frame["year"] = year
        frame["month"] = month
        frames.append(frame)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def reconcile_calendar_from_facts(
    old_silver: pathlib.Path,
    candidate_lake_root: pathlib.Path,
    run_id: str,
    event_path: pathlib.Path,
) -> dict[str, int]:
    silver = candidate_lake_root / "silver"
    calendar_root = silver / "dim_futures_bar_calendar"
    daily_root = silver / "fact_futures_daily"
    minute_root = silver / "fact_futures_minute"
    status_root = old_silver / "fact_futures_fetch_status"
    calendar_schema = FUTURES_BAR_CALENDAR_SCHEMA
    calendar_leaves = leaf_directories(
        calendar_root,
        partition_columns(calendar_schema),
    )
    daily_dataset = ds.dataset(
        daily_root,
        format="parquet",
        partitioning=hive_partitioning(FUTURES_DAILY_SCHEMA),
    )
    minute_dataset = ds.dataset(
        minute_root,
        format="parquet",
        partitioning=hive_partitioning(FUTURES_MINUTE_SCHEMA),
    )
    now = datetime.now(timezone.utc)
    incomplete_examples: list[dict[str, object]] = []
    required_count = 0
    completed_count = 0

    for key in sorted(calendar_leaves):
        frequency, exchange_code, year, month = key
        frame = read_leaf(calendar_root, calendar_leaves[key], calendar_schema)
        frame = C04.validate_bar_calendar_frame(frame, "离线状态回写前行情日历")
        original = frame.copy()
        pair_required = pd.Series(
            [
                (exchange, underlying) in FUTURES_FACT_VARIETY_PAIRS
                for exchange, underlying in zip(
                    frame["exchange_code"],
                    frame["underlying_code"],
                    strict=True,
                )
            ],
            index=frame.index,
        )
        authoritative_closed = (
            frame["schedule_status"].eq("confirmed_closed")
            & frame["evidence_level"].eq("authoritative")
        ).fillna(False).astype(bool)
        required = (pair_required & ~authoritative_closed).fillna(False).astype(bool)
        frame["is_fetch_required"] = required
        selected_reason = (
            C05.SELECTED_REASON if frequency == "1d" else C06.SELECTED_REASON
        )
        excluded_reason = (
            C05.EXCLUDED_REASON if frequency == "1d" else C06.EXCLUDED_REASON
        )
        frame.loc[required, "selection_reason"] = selected_reason
        frame.loc[~required & ~authoritative_closed, "selection_reason"] = excluded_reason

        fact_filter = (
            (ds.field("exchange_code") == exchange_code)
            & (ds.field("year") == year)
            & (ds.field("month") == month)
        )
        if frequency == "1d":
            fact_table = daily_dataset.to_table(
                columns=[
                    "contract_code",
                    "trading_date",
                    "has_market_data",
                    "open",
                    "high",
                    "low",
                    "close",
                ],
                filter=fact_filter,
            )
            fact_df = fact_table.to_pandas()
            if fact_df.duplicated(["contract_code", "trading_date"]).any():
                raise ValueError(f"日线事实主键不唯一：{key}")
            fact_df["session_number"] = 0
            fact_df["fact_exists"] = True
            fact_df["recomputed_actual"] = fact_df["has_market_data"].astype(int)
            non_null = fact_df[["open", "high", "low", "close"]].notna()
            fact_df["invalid_ohlc"] = (
                (non_null["high"] & non_null["open"] & (fact_df["high"] < fact_df["open"]))
                | (non_null["high"] & non_null["close"] & (fact_df["high"] < fact_df["close"]))
                | (non_null["low"] & non_null["open"] & (fact_df["low"] > fact_df["open"]))
                | (non_null["low"] & non_null["close"] & (fact_df["low"] > fact_df["close"]))
                | (non_null["high"] & non_null["low"] & (fact_df["high"] < fact_df["low"]))
            )
            facts = fact_df[
                [
                    "contract_code",
                    "trading_date",
                    "session_number",
                    "fact_exists",
                    "recomputed_actual",
                    "invalid_ohlc",
                    "has_market_data",
                ]
            ]
        else:
            minute_table = minute_dataset.to_table(
                columns=["contract_code", "trading_date", "session_number"],
                filter=fact_filter,
            )
            minute_df = minute_table.to_pandas()
            facts = (
                minute_df.groupby(
                    ["contract_code", "trading_date", "session_number"],
                    as_index=False,
                    dropna=False,
                )
                .size()
                .rename(columns={"size": "recomputed_actual"})
            )
            facts["fact_exists"] = facts["recomputed_actual"].gt(0)

        join_key = ["contract_code", "trading_date", "session_number"]
        frame = frame.merge(facts, on=join_key, how="left", validate="one_to_one")
        frame["recomputed_actual"] = frame["recomputed_actual"].fillna(0).astype(int)
        frame["fact_exists"] = frame["fact_exists"].fillna(False).astype(bool)

        status = old_status_frame(
            status_root,
            frequency,
            exchange_code,
            year,
            month,
        )
        if not status.empty:
            if status.duplicated(join_key).any():
                raise ValueError(f"旧 fetch status 主键不唯一：{key}")
            status = status[
                join_key
                + [
                    "is_fetch_completed",
                    "expected_bar_count",
                    "actual_bar_count",
                    "fetch_run_id",
                    "fetch_completed_at",
                ]
            ].rename(
                columns={
                    "is_fetch_completed": "old_completed",
                    "expected_bar_count": "old_expected",
                    "actual_bar_count": "old_actual",
                    "fetch_run_id": "old_run_id",
                    "fetch_completed_at": "old_completed_at",
                }
            )
            frame = frame.merge(status, on=join_key, how="left", validate="one_to_one")
        else:
            frame["old_completed"] = False
            frame["old_expected"] = pd.NA
            frame["old_actual"] = pd.NA
            frame["old_run_id"] = None
            frame["old_completed_at"] = None

        old_completed = frame["old_completed"].astype("boolean").fillna(False)
        old_expected = pd.to_numeric(
            frame["old_expected"],
            errors="coerce",
        ).astype("Int64")
        old_actual = pd.to_numeric(
            frame["old_actual"],
            errors="coerce",
        ).astype("Int64")
        old_consistent = (
            old_completed
            & old_expected.eq(frame["expected_bar_count"].astype("Int64")).fillna(False)
            & old_actual.eq(frame["recomputed_actual"].astype("Int64")).fillna(False)
            & frame["old_run_id"].notna()
            & frame["old_completed_at"].notna()
        ).fillna(False).astype(bool)
        existing_consistent = (
            original["is_fetch_completed"].astype(bool)
            & original["actual_bar_count"].eq(frame["recomputed_actual"].to_numpy())
            & original["fetch_run_id"].notna()
            & original["fetch_completed_at"].notna()
        )
        if frequency == "1d":
            completion_evidence = frame["fact_exists"] | old_consistent | existing_consistent
        else:
            completion_evidence = (
                frame["recomputed_actual"].gt(0)
                | old_consistent
                | existing_consistent
            )
        complete = (required & completion_evidence).fillna(False).astype(bool)

        # 非 required 格点只保留理论结构和历史事实，不携带旧执行完成语义。
        non_required = ~required & ~authoritative_closed
        frame.loc[non_required, "is_fetch_completed"] = False
        frame.loc[non_required, "actual_bar_count"] = 0
        frame.loc[non_required, "is_data_missing"] = False
        frame.loc[non_required, "missing_bar_count"] = 0
        frame.loc[non_required, "fetch_run_id"] = None
        frame.loc[non_required, "fetch_completed_at"] = None
        frame.loc[non_required, "missing_checked_at"] = now
        frame.loc[non_required, "quality_status"] = "not_applicable"
        frame.loc[non_required, "quality_reason"] = excluded_reason
        frame.loc[non_required, "quality_checked_at"] = now

        pending = required & ~complete
        frame.loc[pending, "is_fetch_completed"] = False
        frame.loc[pending, "actual_bar_count"] = frame.loc[pending, "recomputed_actual"]
        frame.loc[pending, "is_data_missing"] = False
        frame.loc[pending, "missing_bar_count"] = 0
        frame.loc[pending, "fetch_run_id"] = None
        frame.loc[pending, "fetch_completed_at"] = None
        frame.loc[pending, "missing_checked_at"] = None
        frame.loc[pending, "quality_status"] = "pending"
        frame.loc[pending, "quality_reason"] = "迁移后尚无可复核的完成证据。"
        frame.loc[pending, "quality_checked_at"] = None

        missing = (frame["expected_bar_count"] - frame["recomputed_actual"]).clip(lower=0)
        frame.loc[complete, "is_fetch_completed"] = True
        frame.loc[complete, "actual_bar_count"] = frame.loc[complete, "recomputed_actual"]
        frame.loc[complete, "missing_bar_count"] = missing.loc[complete]
        frame.loc[complete, "is_data_missing"] = missing.loc[complete].gt(0)
        chosen_run_id = frame["old_run_id"].where(old_consistent)
        chosen_run_id = chosen_run_id.where(
            chosen_run_id.notna(),
            original["fetch_run_id"],
        ).fillna(f"legacy-migration-{run_id}")
        chosen_completed_at = frame["old_completed_at"].where(old_consistent)
        chosen_completed_at = chosen_completed_at.where(
            chosen_completed_at.notna(),
            original["fetch_completed_at"],
        ).fillna(now)
        frame.loc[complete, "fetch_run_id"] = chosen_run_id.loc[complete]
        frame.loc[complete, "fetch_completed_at"] = chosen_completed_at.loc[complete]
        frame.loc[complete, "missing_checked_at"] = now
        frame.loc[complete, "quality_checked_at"] = now

        if frequency == "1d":
            has_market_data = frame["has_market_data"].astype("boolean").fillna(False)
            invalid_ohlc = frame["invalid_ohlc"].astype("boolean").fillna(False)
            no_market = complete & ~has_market_data.astype(bool)
            invalid = complete & invalid_ohlc.astype(bool)
            passed = complete & ~no_market & ~invalid
            frame.loc[no_market, "quality_status"] = "warning"
            frame.loc[no_market, "quality_reason"] = (
                "旧日线请求已完成但没有有效收盘价；缺失占位事实已按当前契约复读。"
            )
            frame.loc[invalid, "quality_status"] = "warning"
            frame.loc[invalid, "quality_reason"] = (
                "历史日线事实已按当前契约复读；供应商 OHLC 跨列关系异常，原值保留。"
            )
            frame.loc[passed, "quality_status"] = "passed"
            frame.loc[passed, "quality_reason"] = (
                "历史日线事实已迁移、正式候选路径复读且实际条数为 1。"
            )
        else:
            zero = complete & frame["recomputed_actual"].eq(0)
            nonzero = complete & ~zero
            frame.loc[zero, "schedule_status"] = "suspected_closed"
            frame.loc[zero, "schedule_signal_reason"] = (
                "旧采集完成证据与迁移后事实复读均为 0 条；仅作为疑似休市推断。"
            )
            frame.loc[zero, "evidence_level"] = "inferred"
            frame.loc[zero, "evidence_source"] = (
                "legacy_fact_futures_fetch_status:zero_count_replayed"
            )
            frame.loc[zero, "quality_status"] = "warning"
            frame.loc[zero, "quality_reason"] = (
                "旧分钟请求完成记录经候选事实复读确认实际为 0 条；等待 c07/c08 旁证。"
            )
            frame.loc[nonzero & missing.gt(0), "quality_status"] = "warning"
            frame.loc[nonzero & missing.gt(0), "quality_reason"] = (
                "历史分钟事实已迁移并复读，但实际条数少于理论条数；等待 c08 全量重建缺失明细。"
            )
            frame.loc[nonzero & missing.eq(0), "quality_status"] = "passed"
            frame.loc[nonzero & missing.eq(0), "quality_reason"] = (
                "历史分钟事实已迁移并复读，实际条数等于理论条数；等待 c08 全量逐时点复核。"
            )

        frame["updated_at"] = now
        drop_columns = [
            "recomputed_actual",
            "fact_exists",
            "old_completed",
            "old_expected",
            "old_actual",
            "old_run_id",
            "old_completed_at",
            "invalid_ohlc",
            "has_market_data",
        ]
        frame = frame.drop(columns=[name for name in drop_columns if name in frame.columns])
        frame = C04.validate_bar_calendar_frame(
            frame.loc[:, calendar_schema.names],
            "离线状态回写后行情日历",
        )
        replace_leaf(calendar_root, key, frame, calendar_schema, run_id)
        required_count += int(required.sum())
        completed_count += int(complete.sum())
        if pending.any() and len(incomplete_examples) < 20:
            incomplete_examples.extend(
                frame.loc[pending.to_numpy(), [
                    "bar_frequency",
                    "contract_code",
                    "trading_date",
                    "session_number",
                ]]
                .head(20 - len(incomplete_examples))
                .astype(str)
                .to_dict(orient="records")
            )
        append_event(
            event_path,
            {
                "action": "reconciled_calendar_leaf",
                "partition": list(key),
                "required": int(required.sum()),
                "completed": int(complete.sum()),
            },
        )

    if incomplete_examples:
        raise RuntimeError(
            "禁止联网且存在 required 无完成证据格点；迁移回滚。示例："
            + json.dumps(incomplete_examples, ensure_ascii=False)
        )
    return {"required": required_count, "completed": completed_count}


def validate_exact_dataset(table_root: pathlib.Path, schema: pa.Schema) -> int:
    dataset = ds.dataset(
        table_root,
        format="parquet",
        partitioning=hive_partitioning(schema),
    )
    reconstructed = pa.schema(
        [dataset.schema.field(name) for name in schema.names],
        metadata=dataset.schema.metadata,
    )
    if not reconstructed.equals(schema, check_metadata=True):
        raise TypeError(f"数据集 Schema/metadata 不精确：{table_root}")
    return dataset.count_rows()


def validate_all_minute_leaves(
    candidate_silver: pathlib.Path,
    event_path: pathlib.Path,
) -> int:
    root = candidate_silver / "fact_futures_minute"
    leaves = leaf_directories(root, partition_columns(FUTURES_MINUTE_SCHEMA))
    total = 0
    for key in sorted(leaves):
        frame = read_leaf(root, leaves[key], FUTURES_MINUTE_SCHEMA)
        checked = C06.validate_minute_frame(frame, f"分钟事实全量叶 {key}")
        if len(checked) != len(frame):
            raise ValueError(f"分钟叶校验改变行数：{key}")
        total += len(frame)
    append_event(
        event_path,
        {"action": "validated_all_minute_values", "rows": total},
    )
    return total


def validate_all_minute_leaves_vectorized(
    candidate_silver: pathlib.Path,
    event_path: pathlib.Path,
    *,
    progress_action: str = "vector_minute_validation_progress",
    completion_action: str = "validated_all_minute_values_vectorized",
) -> dict[str, int]:
    """全量扫描分钟值，但不把 5.17 亿行转换为 Python 字典。"""
    root = candidate_silver / "fact_futures_minute"
    schema = FUTURES_MINUTE_SCHEMA
    expected_file_schema = file_schema(schema)
    leaves = leaf_directories(root, partition_columns(schema))
    total_rows = 0
    invalid_ohlc_rows = 0

    for leaf_number, key in enumerate(sorted(leaves), start=1):
        exchange_code, underlying_code, year, month = key
        paths = sorted(leaves[key].glob("*.parquet"))
        for path in paths:
            if not pq.read_schema(path).equals(
                expected_file_schema,
                check_metadata=True,
            ):
                raise TypeError(f"分钟文件 Schema/metadata 不精确：{path}")
        table = pa.concat_tables(
            [pq.ParquetFile(path).read() for path in paths],
            promote_options="default",
        )
        total_rows += table.num_rows

        # 叶内事实主键必须唯一；每个候选叶当前只含一个或少量文件。
        key_frame = table.select(["contract_code", "bar_at"]).to_pandas(
            types_mapper=pd.ArrowDtype
        )
        if key_frame.duplicated(["contract_code", "bar_at"]).any():
            raise ValueError(f"分钟事实叶主键不唯一：{key}")

        # 合约身份只需检查叶内唯一代码，不逐行构造 Python dict。
        contract_codes = pc.unique(table["contract_code"]).to_pylist()
        for contract_code in contract_codes:
            if not contract_code.endswith(f".{exchange_code}"):
                raise ValueError(f"分钟事实合约后缀与分区不一致：{key} {contract_code}")
            symbol = contract_code.split(".", maxsplit=1)[0]
            derived_underlying = "".join(
                character for character in symbol if character.isalpha()
            ).upper()
            if derived_underlying != underlying_code:
                raise ValueError(f"分钟事实品种与分区不一致：{key} {contract_code}")

        if pc.any(pc.less_equal(table["session_number"], 0)).as_py():
            raise ValueError(f"分钟事实 session_number 非正：{key}")
        if pc.any(pc.not_equal(pc.year(table["trading_date"]), year)).as_py():
            raise ValueError(f"分钟事实 year 与 trading_date 不一致：{key}")
        if pc.any(pc.not_equal(pc.month(table["trading_date"]), month)).as_py():
            raise ValueError(f"分钟事实 month 与 trading_date 不一致：{key}")

        allowed_sources = pa.array(
            [C06.SOURCE_NAME, *sorted(C06.LEGACY_SOURCE_NAMES)],
            type=pa.string(),
        )
        source_allowed = pc.is_in(table["source"], value_set=allowed_sources)
        if pc.any(pc.invert(source_allowed)).as_py():
            raise ValueError(f"分钟事实包含非法来源标签：{key}")

        for column in C06.PRICE_FIELDS:
            finite_or_null = pc.or_(
                pc.is_null(table[column]),
                pc.is_finite(table[column]),
            )
            if pc.any(pc.invert(finite_or_null)).as_py():
                raise ValueError(f"分钟事实 {column} 包含 NaN/Inf：{key}")
        for column in ["volume", "money", "open_interest"]:
            negative = pc.fill_null(pc.less(table[column], 0), False)
            if pc.any(negative).as_py():
                raise ValueError(f"分钟事实 {column} 为负：{key}")

        high_invalid = pc.or_kleene(
            pc.less(table["high"], table["open"]),
            pc.or_kleene(
                pc.less(table["high"], table["close"]),
                pc.less(table["high"], table["low"]),
            ),
        )
        low_invalid = pc.or_kleene(
            pc.greater(table["low"], table["open"]),
            pc.greater(table["low"], table["close"]),
        )
        invalid_mask = pc.fill_null(
            pc.or_kleene(high_invalid, low_invalid),
            False,
        )
        invalid_ohlc_rows += int(pc.sum(pc.cast(invalid_mask, pa.int64())).as_py())

        if leaf_number % 250 == 0:
            append_event(
                event_path,
                {
                    "action": progress_action,
                    "leaves": leaf_number,
                    "rows": total_rows,
                    "invalid_ohlc_rows": invalid_ohlc_rows,
                },
            )

    append_event(
        event_path,
        {
            "action": completion_action,
            "leaves": len(leaves),
            "rows": total_rows,
            "invalid_ohlc_rows": invalid_ohlc_rows,
        },
    )
    return {
        "leaves": len(leaves),
        "rows": total_rows,
        "invalid_ohlc_rows": invalid_ohlc_rows,
    }


def build_fresh_bar_partitions_vectorized(
    contract_frame: pd.DataFrame,
    updated_at: datetime,
) -> dict[str, pd.DataFrame]:
    """与 c04.build_fresh_partitions 等价，但不把每行转成 Python dict。"""
    contract = arrow_to_pandas(
        pandas_to_arrow(
            contract_frame.loc[:, FUTURES_CONTRACT_CALENDAR_SCHEMA.names],
            FUTURES_CONTRACT_CALENDAR_SCHEMA,
        ),
        FUTURES_CONTRACT_CALENDAR_SCHEMA,
    )
    upstream_pk = primary_key(FUTURES_CONTRACT_CALENDAR_SCHEMA)
    if contract.duplicated(upstream_pk).any():
        raise ValueError("向量 c04 上游合约日历主键不唯一。")
    if not contract.empty:
        if contract["session_number"].le(0).any():
            raise ValueError("向量 c04 上游 session_number 非正。")
        duration_minutes = (
            contract["session_end_at"] - contract["session_start_at"]
        ).dt.total_seconds().astype("float64") / 60
        if duration_minutes.le(0).any() or (duration_minutes % 1).ne(0).any():
            raise ValueError("向量 c04 上游 Session 时间差非法。")
        if not contract["minute_count"].eq(duration_minutes.astype(int)).all():
            raise ValueError("向量 c04 上游 minute_count 与 Session 不一致。")

        contract_codes = contract["contract_code"].astype("string")
        code_parts = contract_codes.str.rsplit(".", n=1)
        if not code_parts.str[-1].eq(contract["exchange_code"].astype("string")).all():
            raise ValueError("向量 c04 上游合约后缀与交易所不一致。")
        symbols = code_parts.str[0]
        derived_underlying = symbols.str.replace(
            r"[^A-Za-z]",
            "",
            regex=True,
        ).str.upper()
        if not derived_underlying.eq(contract["underlying_code"].astype("string")).all():
            raise ValueError("向量 c04 上游合约代码与品种不一致。")
        trading_dates = pd.to_datetime(contract["trading_date"])
        if not contract["year"].eq(trading_dates.dt.year).all():
            raise ValueError("向量 c04 上游 year 与 trading_date 不一致。")
        if not contract["month"].eq(trading_dates.dt.month).all():
            raise ValueError("向量 c04 上游 month 与 trading_date 不一致。")

        groups = contract.groupby(
            ["contract_code", "trading_date"],
            dropna=False,
        )
        session_groups = groups["session_number"].agg(
            ["count", "nunique", "min", "max"],
        )
        if not (
            session_groups["count"].eq(session_groups["nunique"])
            & session_groups["min"].eq(1)
            & session_groups["max"].eq(session_groups["count"])
        ).all():
            raise ValueError("向量 c04 上游 Session 编号不连续。")
        direct_columns = ["exchange_code", "underlying_code", "year", "month"]
        direct_counts = groups[direct_columns].nunique(dropna=False)
        if direct_counts.ne(1).any().any():
            raise ValueError("向量 c04 上游同一合约日直接属性不一致。")
    schema = FUTURES_BAR_CALENDAR_SCHEMA

    minute = pd.DataFrame(index=contract.index)
    minute["bar_frequency"] = "1m"
    for name in [
        "contract_code",
        "exchange_code",
        "underlying_code",
        "trading_date",
        "session_number",
        "session_text",
        "session_start_at",
        "session_end_at",
        "is_night_session",
        "year",
        "month",
    ]:
        minute[name] = contract[name]
    minute["expected_bar_count"] = contract["minute_count"]
    for name, value in C04.initial_state("1m", updated_at).items():
        minute[name] = value
    minute = arrow_to_pandas(
        pandas_to_arrow(minute.loc[:, schema.names], schema),
        schema,
    )

    daily_source = contract.drop_duplicates(
        ["contract_code", "trading_date"],
        keep="first",
    ).reset_index(drop=True)
    daily = pd.DataFrame(index=daily_source.index)
    daily["bar_frequency"] = "1d"
    for name in [
        "contract_code",
        "exchange_code",
        "underlying_code",
        "trading_date",
        "year",
        "month",
    ]:
        daily[name] = daily_source[name]
    daily["session_number"] = 0
    daily["session_text"] = None
    daily["session_start_at"] = None
    daily["session_end_at"] = None
    daily["is_night_session"] = None
    daily["expected_bar_count"] = 1
    for name, value in C04.initial_state("1d", updated_at).items():
        daily[name] = value
    daily = arrow_to_pandas(
        pandas_to_arrow(daily.loc[:, schema.names], schema),
        schema,
    )
    return {"1d": daily, "1m": minute}


def validate_bar_calendar_vectorized(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    """覆盖 c04 全部行级规则；c07/c08 仍负责后续跨表 Session 门禁。"""
    schema = FUTURES_BAR_CALENDAR_SCHEMA
    checked = arrow_to_pandas(
        pandas_to_arrow(frame.loc[:, schema.names], schema),
        schema,
    )
    pk = primary_key(schema)
    if checked.duplicated(pk).any():
        raise ValueError(f"{context}主键不唯一。")
    if checked.empty:
        return checked
    if not checked["bar_frequency"].isin(C04.BAR_FREQUENCIES).all():
        raise ValueError(f"{context}bar_frequency 非法。")
    if not checked["schedule_status"].isin(C04.SCHEDULE_STATUSES).all():
        raise ValueError(f"{context}schedule_status 非法。")
    if not checked["evidence_level"].isin(C04.EVIDENCE_LEVELS).all():
        raise ValueError(f"{context}evidence_level 非法。")
    if not checked["quality_status"].isin(C04.QUALITY_STATUSES).all():
        raise ValueError(f"{context}quality_status 非法。")
    for name in [
        "schedule_signal_reason",
        "evidence_source",
        "selection_reason",
        "quality_reason",
    ]:
        if checked[name].astype("string").str.strip().eq("").any():
            raise ValueError(f"{context}{name} 为空。")

    contract_codes = checked["contract_code"].astype("string")
    code_parts = contract_codes.str.rsplit(".", n=1)
    if not code_parts.str[-1].eq(checked["exchange_code"].astype("string")).all():
        raise ValueError(f"{context}合约后缀与交易所不一致。")
    derived_underlying = code_parts.str[0].str.replace(
        r"[^A-Za-z]",
        "",
        regex=True,
    ).str.upper()
    if not derived_underlying.eq(checked["underlying_code"].astype("string")).all():
        raise ValueError(f"{context}合约代码与品种不一致。")
    trading_dates = pd.to_datetime(checked["trading_date"])
    if not checked["year"].eq(trading_dates.dt.year).all():
        raise ValueError(f"{context}year 与 trading_date 不一致。")
    if not checked["month"].eq(trading_dates.dt.month).all():
        raise ValueError(f"{context}month 与 trading_date 不一致。")
    if checked["expected_bar_count"].le(0).any():
        raise ValueError(f"{context}expected_bar_count 非正。")
    if checked[["actual_bar_count", "missing_bar_count"]].lt(0).any().any():
        raise ValueError(f"{context}实际或缺失条数为负。")

    daily = checked["bar_frequency"].eq("1d")
    minute = ~daily
    if not checked.loc[daily, "session_number"].eq(0).all():
        raise ValueError(f"{context}日线 session_number 不为0。")
    if checked.loc[daily, [
        "session_text",
        "session_start_at",
        "session_end_at",
        "is_night_session",
    ]].notna().any().any():
        raise ValueError(f"{context}日线包含 Session 专属值。")
    if not checked.loc[daily, "expected_bar_count"].eq(1).all():
        raise ValueError(f"{context}日线 expected_bar_count 不为1。")
    if checked.loc[minute, "session_number"].le(0).any():
        raise ValueError(f"{context}分钟 session_number 非正。")
    minute_fields = [
        "session_text",
        "session_start_at",
        "session_end_at",
        "is_night_session",
    ]
    if checked.loc[minute, minute_fields].isna().any().any():
        raise ValueError(f"{context}分钟 Session 字段为空。")
    minute_frame = checked.loc[minute]
    duration_minutes = (
        minute_frame["session_end_at"] - minute_frame["session_start_at"]
    ).dt.total_seconds().astype("float64") / 60
    if duration_minutes.le(0).any() or (duration_minutes % 1).ne(0).any():
        raise ValueError(f"{context}分钟 Session 时间差非法。")
    if not minute_frame["expected_bar_count"].eq(duration_minutes.astype(int)).all():
        raise ValueError(f"{context}分钟理论条数与 Session 不一致。")
    session_groups = minute_frame.groupby(
        ["contract_code", "trading_date"],
        dropna=False,
    )["session_number"].agg(["count", "nunique", "min", "max"])
    if not (
        session_groups["count"].eq(session_groups["nunique"])
        & session_groups["min"].eq(1)
        & session_groups["max"].eq(session_groups["count"])
    ).all():
        raise ValueError(f"{context}分钟 Session 编号不连续。")

    confirmed = checked["schedule_status"].eq("confirmed_closed")
    if (
        ~checked.loc[confirmed, "evidence_level"].eq("authoritative")
    ).any() or checked.loc[confirmed, "is_fetch_required"].any():
        raise ValueError(f"{context}确认休市缺少权威证据或仍需拉取。")
    completed = checked["is_fetch_completed"]
    if checked.loc[completed, "fetch_run_id"].isna().any():
        raise ValueError(f"{context}完成状态缺少批次。")
    if checked.loc[completed, "fetch_run_id"].astype("string").str.strip().eq("").any():
        raise ValueError(f"{context}完成状态批次为空。")
    if checked.loc[completed, "fetch_completed_at"].isna().any():
        raise ValueError(f"{context}完成状态缺少完成时间。")
    if checked.loc[~completed, "fetch_completed_at"].notna().any():
        raise ValueError(f"{context}未完成格点具有完成时间。")

    checked_missing = checked["missing_checked_at"].notna()
    unchecked = ~checked_missing
    if checked.loc[unchecked, "is_data_missing"].any() or checked.loc[
        unchecked, "missing_bar_count"
    ].ne(0).any():
        raise ValueError(f"{context}未经检查却记录缺失。")
    expected_missing = (
        checked["expected_bar_count"] - checked["actual_bar_count"]
    ).clip(lower=0).where(checked["is_fetch_required"], 0)
    if not checked.loc[checked_missing, "missing_bar_count"].eq(
        expected_missing.loc[checked_missing]
    ).all():
        raise ValueError(f"{context}missing_bar_count 无法复算。")
    if not checked.loc[checked_missing, "is_data_missing"].eq(
        expected_missing.loc[checked_missing].gt(0)
    ).all():
        raise ValueError(f"{context}is_data_missing 无法复算。")
    quality_done = ~checked["quality_status"].eq("pending")
    if checked.loc[quality_done, "quality_checked_at"].isna().any():
        raise ValueError(f"{context}非 pending 质量状态缺检查时间。")
    return checked.sort_values(pk).reset_index(drop=True)


def assert_vector_c04_equivalence(
    candidate_silver: pathlib.Path,
    event_path: pathlib.Path,
) -> None:
    contract_root = candidate_silver / "dim_futures_contract_calendar"
    samples = [
        ("GFEX", 2023, 10),
        ("XSGE", 2020, 1),
        ("CCFX", 2026, 8),
    ]
    checked_at = datetime(2026, 8, 18, 0, 0, tzinfo=timezone.utc)
    evidence = []
    for key in samples:
        leaf = contract_root / f"exchange_code={key[0]}" / f"year={key[1]}" / f"month={key[2]}"
        contract = read_leaf(contract_root, leaf, FUTURES_CONTRACT_CALENDAR_SCHEMA)
        original = C04.build_fresh_partitions(contract, checked_at)
        vector = build_fresh_bar_partitions_vectorized(contract, checked_at)
        for frequency in ["1d", "1m"]:
            original_table = pandas_to_arrow(
                original[frequency].sort_values(C04.PRIMARY_KEY).reset_index(drop=True),
                FUTURES_BAR_CALENDAR_SCHEMA,
            )
            vector_checked = C04.validate_bar_calendar_frame(
                vector[frequency],
                f"向量 c04 样本 {key} {frequency}",
            )
            vector_checked = validate_bar_calendar_vectorized(
                vector_checked,
                f"向量 c04 自身门禁样本 {key} {frequency}",
            )
            vector_table = pandas_to_arrow(
                vector_checked.sort_values(C04.PRIMARY_KEY).reset_index(drop=True),
                FUTURES_BAR_CALENDAR_SCHEMA,
            )
            if not vector_table.equals(original_table):
                raise ValueError(f"向量 c04 与原实现不等价：{key} {frequency}")
        evidence.append({
            "partition": list(key),
            "contract_rows": len(contract),
            "night_rows": int(contract["is_night_session"].sum()),
            "spans_midnight_rows": int(contract["spans_midnight"].sum()),
            "max_sessions": int(
                contract.groupby(["contract_code", "trading_date"]).size().max()
            ),
            "current_overlap": key[1:] == (2026, 8),
        })
    append_event(
        event_path,
        {"action": "vector_c04_sample_equivalence_passed", "samples": evidence},
    )


def build_bar_calendar_vectorized(
    candidate_silver: pathlib.Path,
    run_id: str,
    event_path: pathlib.Path,
) -> dict[str, int]:
    contract_root = candidate_silver / "dim_futures_contract_calendar"
    calendar_root = candidate_silver / "dim_futures_bar_calendar"
    contract_leaves = leaf_directories(
        contract_root,
        partition_columns(FUTURES_CONTRACT_CALENDAR_SCHEMA),
    )
    existing_leaves = leaf_directories(
        calendar_root,
        partition_columns(FUTURES_BAR_CALENDAR_SCHEMA),
    )
    target_keys: set[tuple[object, ...]] = set()
    total_rows = 0
    updated_at = datetime.now(timezone.utc)

    for number, (base_key, contract_leaf) in enumerate(
        sorted(contract_leaves.items()),
        start=1,
    ):
        contract = read_leaf(
            contract_root,
            contract_leaf,
            FUTURES_CONTRACT_CALENDAR_SCHEMA,
        )
        fresh = build_fresh_bar_partitions_vectorized(contract, updated_at)
        for frequency in ["1d", "1m"]:
            key = (frequency, *base_key)
            target_keys.add(key)
            desired = fresh[frequency]
            if key in existing_leaves:
                existing = read_leaf(
                    calendar_root,
                    existing_leaves[key],
                    FUTURES_BAR_CALENDAR_SCHEMA,
                )
                # 现行函数只对结构逐字段完全一致的主键复制全部 mutable 状态。
                desired = C04.merge_preserving_state(desired, existing)
            desired = validate_bar_calendar_vectorized(
                desired,
                f"向量 c04 完整分区 {key}",
            )
            replace_leaf(
                calendar_root,
                key,
                desired,
                FUTURES_BAR_CALENDAR_SCHEMA,
                run_id,
            )
            total_rows += len(desired)
        if number % 50 == 0:
            append_event(
                event_path,
                {
                    "action": "vector_c04_progress",
                    "base_partitions": number,
                    "rows": total_rows,
                },
            )

    # 当前表中不再属于上游的额外叶不得残留。
    for key, leaf in existing_leaves.items():
        if key not in target_keys and leaf.exists():
            shutil.rmtree(leaf)
    write_schema_marker(calendar_root, FUTURES_BAR_CALENDAR_SCHEMA)
    exact_rows = validate_exact_dataset(calendar_root, FUTURES_BAR_CALENDAR_SCHEMA)
    if exact_rows != total_rows:
        raise ValueError("向量 c04 根级行数与逐叶汇总不一致。")
    append_event(
        event_path,
        {
            "action": "vector_c04_completed",
            "base_partitions": len(contract_leaves),
            "rows": total_rows,
        },
    )
    return {"base_partitions": len(contract_leaves), "rows": total_rows}


def file_identity_manifest(root: pathlib.Path) -> dict[str, tuple[int, int]]:
    result = {}
    for path in sorted(root.rglob("*.parquet")):
        stat = path.stat()
        result[path.relative_to(root).as_posix()] = (stat.st_size, stat.st_ino)
    return result


def resume_candidate_manifest(
    candidate_silver: pathlib.Path,
) -> dict[str, list[dict[str, object]]]:
    """记录候选 Parquet 的文件身份和 footer 摘要，不重扫分钟数据页。"""
    manifest: dict[str, list[dict[str, object]]] = {}
    for table_name in AFFECTED_TABLES:
        if table_name == "fact_futures_missing_bar":
            continue
        table_root = candidate_silver / table_name
        if not table_root.is_dir():
            raise FileNotFoundError(f"恢复候选缺少表根：{table_root}")
        files = []
        for path in sorted(table_root.rglob("*.parquet")):
            footer_start, footer = parquet_footer(path)
            stat = path.stat()
            files.append({
                "path": path.relative_to(candidate_silver).as_posix(),
                "size": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
                "inode": stat.st_ino,
                "footer_start": footer_start,
                "footer_sha256": hashlib.sha256(footer).hexdigest(),
            })
        manifest[table_name] = files
    return manifest


def prove_ready_for_c07(
    transaction_root: pathlib.Path,
    old_silver: pathlib.Path,
    formal_lake_root: pathlib.Path,
) -> dict[str, object]:
    """严格证明 c04 与离线回写完整；证据不足时在任何恢复写入前失败。"""
    run_id = transaction_root.name.removeprefix(".legacy-migration-")
    candidate_silver = transaction_root / "candidate_lake" / "silver"
    formal_silver = formal_lake_root / "silver"
    event_path = transaction_root / "events.jsonl"
    journal_path = transaction_root / "minute_footer_journal.bin"
    checkpoint_path = transaction_root / "resume-ready-for-c07.json"

    if not event_path.is_file() or not journal_path.is_file():
        raise FileNotFoundError("恢复事务缺少 events 或 minute footer journal。")
    if any(formal_silver.glob(".*.pre-migration-*")):
        raise RuntimeError("恢复证明发现正式 root backup。")
    if any(path.name.startswith(".") for path in candidate_silver.iterdir()):
        raise RuntimeError("恢复证明发现候选 staging/backup 隐藏目录。")
    if (candidate_silver / "fact_futures_missing_bar").exists():
        raise RuntimeError("恢复证明发现 c08 产物，阶段边界不再是 c07 之前。")

    raw_events = event_path.read_bytes()
    if not raw_events.endswith(b"\n"):
        raise ValueError("events.jsonl 尾部不完整。")
    raw_lines = raw_events.splitlines(keepends=True)
    events = []
    for number, raw_line in enumerate(raw_lines, start=1):
        try:
            event = json.loads(raw_line)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError(f"events.jsonl 第 {number} 行损坏。") from error
        if not isinstance(event, dict) or not isinstance(event.get("action"), str):
            raise ValueError(f"events.jsonl 第 {number} 行缺少 action。")
        events.append(event)

    checkpoint_events = [
        (index, event)
        for index, event in enumerate(events)
        if event["action"] == "resume_ready_for_c07_checkpoint"
    ]
    if len(checkpoint_events) > 1:
        raise RuntimeError("恢复 checkpoint 事件重复。")
    if checkpoint_events:
        checkpoint_index, checkpoint_event = checkpoint_events[0]
        if checkpoint_index != len(events) - 1:
            raise RuntimeError("恢复 checkpoint 之后已有其他事件，拒绝覆盖阶段。")
        core_events = events[:-1]
        core_event_bytes = b"".join(raw_lines[:-1])
    else:
        checkpoint_event = None
        core_events = events
        core_event_bytes = raw_events

    forbidden_actions = {
        "transaction_resumed_vectorized",
        "transaction_resumed_phase_aware",
        "c07_completed",
        "c08_completed",
        "candidate_fully_validated",
        "formal_to_backup",
        "candidate_to_formal",
        "formal_reread_completed",
        "old_lake_and_backups_removed",
    }
    present_forbidden = sorted(
        {event["action"] for event in core_events} & forbidden_actions
    )
    if present_forbidden:
        raise RuntimeError(f"恢复事务已越过 c07 前边界：{present_forbidden}")

    started = [
        (index, event)
        for index, event in enumerate(core_events)
        if event["action"] == "transaction_started"
    ]
    if len(started) != 1 or started[0][0] != 0 or started[0][1].get("run_id") != run_id:
        raise RuntimeError("transaction_started 事件不唯一、非首行或 run_id 不匹配。")

    patched = [
        (index, event)
        for index, event in enumerate(core_events)
        if event["action"] == "patched_old_minute_footer"
    ]
    moved = [
        (index, event)
        for index, event in enumerate(core_events)
        if event["action"] == "moved_old_minute_leaf"
    ]
    overlap = [
        (index, event)
        for index, event in enumerate(core_events)
        if event["action"] == "rewritten_overlap_minute_leaf"
    ]
    patched_paths = [str(event.get("relative_path")) for _, event in patched]
    journal = read_footer_journal(journal_path)
    journal_paths = [relative_path for relative_path, _, _ in journal]
    if len(patched) != 6439 or len(set(patched_paths)) != 6439:
        raise RuntimeError("分钟 footer 事件不是 6439 条唯一文件。")
    if journal_paths != patched_paths or len(set(journal_paths)) != 6439:
        raise RuntimeError("分钟 footer journal 与事件顺序/集合不精确一致。")
    moved_paths = [str(event.get("relative_path")) for _, event in moved]
    if len(moved) != 6439 or len(set(moved_paths)) != 6439:
        raise RuntimeError("分钟 move 事件不是 6439 条唯一叶。")
    overlap_keys = [tuple(event.get("partition", [])) for _, event in overlap]
    if len(overlap) != 57 or len(set(overlap_keys)) != 57:
        raise RuntimeError("分钟 overlap 事件不是 57 条唯一叶。")
    candidate_minute_root = candidate_silver / "fact_futures_minute"
    old_minute_root = old_silver / "fact_futures_minute"
    for relative_path in journal_paths:
        relative = pathlib.Path(relative_path)
        if not (candidate_minute_root / relative).is_file():
            raise RuntimeError(f"journal 文件不在候选 minute：{relative_path}")
        if (old_minute_root / relative).exists():
            raise RuntimeError(f"journal 文件同时残留旧 minute：{relative_path}")

    minute_events = [
        (index, event)
        for index, event in enumerate(core_events)
        if event["action"] == "validated_all_minute_values_vectorized"
    ]
    if len(minute_events) != 1:
        raise RuntimeError("分钟向量全量门禁事件不唯一。")
    minute_index, minute_event = minute_events[0]
    if minute_index <= max(index for index, _ in [*patched, *moved, *overlap]):
        raise RuntimeError("分钟向量门禁发生在 merge 完成之前。")
    minute_leaves = leaf_directories(
        candidate_minute_root,
        partition_columns(FUTURES_MINUTE_SCHEMA),
    )
    minute_rows = validate_exact_dataset(candidate_minute_root, FUTURES_MINUTE_SCHEMA)
    minute_summary = {
        "leaves": len(minute_leaves),
        "rows": minute_rows,
        "invalid_ohlc_rows": int(minute_event.get("invalid_ohlc_rows", -1)),
    }
    expected_minute_summary = {"leaves": 6496, "rows": 517390417, "invalid_ohlc_rows": 413}
    if minute_summary != expected_minute_summary:
        raise RuntimeError(
            f"候选 minute 与唯一向量门禁不一致：{minute_summary}"
        )
    if any(int(minute_event.get(name, -1)) != value for name, value in expected_minute_summary.items()):
        raise RuntimeError("分钟向量门禁事件载荷不匹配。")
    if len(list(candidate_minute_root.rglob("*.parquet"))) != 6497:
        raise RuntimeError("候选 minute 文件数不为 6497。")

    contract_leaves = leaf_directories(
        candidate_silver / "dim_futures_contract_calendar",
        partition_columns(FUTURES_CONTRACT_CALENDAR_SCHEMA),
    )
    calendar_root = candidate_silver / "dim_futures_bar_calendar"
    calendar_leaves = leaf_directories(
        calendar_root,
        partition_columns(FUTURES_BAR_CALENDAR_SCHEMA),
    )
    expected_calendar_keys = {
        (frequency, *base_key)
        for base_key in contract_leaves
        for frequency in ("1d", "1m")
    }
    if len(contract_leaves) != 748 or set(calendar_leaves) != expected_calendar_keys:
        raise RuntimeError("候选 c04 叶集合不等于 748 个合约叶乘 1d/1m。")

    vector_events = [
        (index, event)
        for index, event in enumerate(core_events)
        if event["action"] == "vector_c04_completed"
    ]
    c04_events = [
        (index, event)
        for index, event in enumerate(core_events)
        if event["action"] == "c04_completed_vectorized"
    ]
    sample_events = [
        (index, event)
        for index, event in enumerate(core_events)
        if event["action"] == "vector_c04_sample_equivalence_passed"
    ]
    if len(vector_events) != 1 or len(c04_events) != 1 or len(sample_events) != 1:
        raise RuntimeError("c04 等价/完成事件不唯一。")
    vector_index, vector_event = vector_events[0]
    c04_index, _ = c04_events[0]
    if not (minute_index < sample_events[0][0] < vector_index < c04_index):
        raise RuntimeError("minute/c04 阶段事件顺序非法。")
    if int(vector_event.get("base_partitions", -1)) != 748:
        raise RuntimeError("c04 完成事件 base_partitions 不为 748。")

    reconciled = [
        (index, event)
        for index, event in enumerate(core_events)
        if event["action"] == "reconciled_calendar_leaf"
    ]
    reconciled_keys = [tuple(event.get("partition", [])) for _, event in reconciled]
    if len(reconciled) != 1496 or len(set(reconciled_keys)) != 1496:
        raise RuntimeError("离线回写事件不是 1496 条唯一叶。")
    if set(reconciled_keys) != expected_calendar_keys:
        raise RuntimeError("离线回写事件叶集合与候选 c04 叶集合不一致。")
    if any(index <= c04_index for index, _ in reconciled):
        raise RuntimeError("离线回写事件早于 c04 完成。")
    if any(int(event.get("required", -1)) != int(event.get("completed", -2)) for _, event in reconciled):
        raise RuntimeError("离线回写存在 required 未完成叶。")
    if core_events[-1]["action"] != "reconciled_calendar_leaf":
        raise RuntimeError("c07 前最后核心事件不是离线回写完成叶。")
    allowed_after_c04 = {"reconciled_calendar_leaf"}
    unexpected_after_c04 = sorted({
        event["action"]
        for event in core_events[c04_index + 1:]
        if event["action"] not in allowed_after_c04
    })
    if unexpected_after_c04:
        raise RuntimeError(f"c04 后出现非离线回写事件：{unexpected_after_c04}")

    total_calendar_rows = 0
    required_rows = 0
    completed_required_rows = 0
    reconciled_by_key = {
        tuple(event["partition"]): event for _, event in reconciled
    }
    for key in sorted(calendar_leaves):
        frame = read_leaf(calendar_root, calendar_leaves[key], FUTURES_BAR_CALENDAR_SCHEMA)
        checked = validate_bar_calendar_vectorized(frame, f"恢复证明 c04 叶 {key} ")
        required = checked["is_fetch_required"].astype(bool)
        completed = checked["is_fetch_completed"].astype(bool)
        event = reconciled_by_key[key]
        leaf_required = int(required.sum())
        leaf_completed = int((required & completed).sum())
        if leaf_required != int(event["required"]) or leaf_completed != int(event["completed"]):
            raise RuntimeError(f"离线回写事件载荷与候选叶不一致：{key}")
        if leaf_completed != leaf_required:
            raise RuntimeError(f"候选 c04 仍有 required 未完成：{key}")
        total_calendar_rows += len(checked)
        required_rows += leaf_required
        completed_required_rows += leaf_completed
    exact_calendar_rows = validate_exact_dataset(calendar_root, FUTURES_BAR_CALENDAR_SCHEMA)
    if total_calendar_rows != exact_calendar_rows or total_calendar_rows != int(vector_event.get("rows", -1)):
        raise RuntimeError("候选 c04 逐叶、根级与完成事件行数不一致。")

    candidate_manifest = resume_candidate_manifest(candidate_silver)
    proof = {
        "checkpoint_version": 1,
        "run_id": run_id,
        "resume_from": "c07",
        "core_event_count": len(core_events),
        "core_events_sha256": hashlib.sha256(core_event_bytes).hexdigest(),
        "journal_records": len(journal),
        "journal_size": journal_path.stat().st_size,
        "journal_sha256": hashlib.sha256(journal_path.read_bytes()).hexdigest(),
        "minute_validation": minute_summary,
        "bar_calendar": {
            "base_partitions": len(contract_leaves),
            "leaves": len(calendar_leaves),
            "rows": total_calendar_rows,
            "required": required_rows,
            "completed_required": completed_required_rows,
        },
        "candidate_manifest": candidate_manifest,
    }
    encoded = json.dumps(proof, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")

    if checkpoint_path.exists():
        if checkpoint_path.read_bytes() != encoded:
            raise RuntimeError("持久恢复 checkpoint 与当前严格证明不一致。")
    else:
        temporary_path = transaction_root / f".resume-ready-for-c07-{uuid.uuid4().hex}.tmp"
        with temporary_path.open("xb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, checkpoint_path)

    checkpoint_sha256 = hashlib.sha256(encoded).hexdigest()
    if checkpoint_event is None:
        append_event(event_path, {
            "action": "resume_ready_for_c07_checkpoint",
            "run_id": run_id,
            "manifest": checkpoint_path.name,
            "manifest_sha256": checkpoint_sha256,
            "core_event_count": len(core_events),
        })
    elif (
        checkpoint_event.get("run_id") != run_id
        or checkpoint_event.get("manifest") != checkpoint_path.name
        or checkpoint_event.get("manifest_sha256") != checkpoint_sha256
        or int(checkpoint_event.get("core_event_count", -1)) != len(core_events)
    ):
        raise RuntimeError("恢复 checkpoint 事件与 manifest 不一致。")
    return proof


def prove_c07_retained_after_rollback(
    transaction_root: pathlib.Path,
    old_silver: pathlib.Path,
    formal_lake_root: pathlib.Path,
) -> dict[str, object]:
    """证明 c08 启动即失败后的回滚完整，且 c07 候选提交仍可信。"""
    candidate_silver = transaction_root / "candidate_lake" / "silver"
    formal_silver = formal_lake_root / "silver"
    event_path = transaction_root / "events.jsonl"
    journal_path = transaction_root / "minute_footer_journal.bin"
    c07_log_path = transaction_root / "c07.log"

    if any(formal_silver.glob(".*.pre-migration-*")):
        raise RuntimeError("c08 失败恢复发现正式 root backup。")
    if any(path.name.startswith(".") for path in candidate_silver.iterdir()):
        raise RuntimeError("c08 失败恢复发现候选 staging/backup 残留。")
    if (candidate_silver / "fact_futures_missing_bar").exists():
        raise RuntimeError("c08 失败恢复发现候选 missing 表残留。")

    raw_events = event_path.read_bytes()
    if not raw_events.endswith(b"\n"):
        raise ValueError("events.jsonl 尾部不完整。")
    events = [json.loads(line) for line in raw_events.splitlines()]
    actions = [event.get("action") for event in events]
    if actions.count("c07_completed") != 1:
        raise RuntimeError("c07 完成事件不唯一。")
    c07_index = actions.index("c07_completed")
    allowed_retry_actions = {
        "transaction_resumed_phase_aware",
        "reapplied_minute_after_c08_rollback_progress",
        "reapplied_minute_vector_validation_progress",
        "reapplied_minute_vector_validation_completed",
        "minute_reapplied_after_c08_rollback_completed",
    }
    unexpected_retry_actions = sorted({
        str(event.get("action"))
        for event in events[c07_index + 1:]
        if event.get("action") not in allowed_retry_actions
    })
    if unexpected_retry_actions:
        raise RuntimeError(f"c07 后出现未知重试事件：{unexpected_retry_actions}")
    for event in events[c07_index + 1:]:
        if event.get("action") == "transaction_resumed_phase_aware" and event.get(
            "resume_from"
        ) != "minute_reapply_then_c08":
            raise RuntimeError("c07 后的重试启动事件阶段不匹配。")
    forbidden = {
        "c08_completed",
        "candidate_fully_validated",
        "formal_to_backup",
        "candidate_to_formal",
        "formal_reread_completed",
        "old_lake_and_backups_removed",
    }
    if set(actions) & forbidden:
        raise RuntimeError("c08 失败事务已出现更晚阶段事件。")

    c07_log = c07_log_path.read_bytes()
    expected_summary = (
        "selected_candidates=51520; changed_candidates=51520; "
        "unchanged_evidence=0; passed=26490; warning=25030"
    )
    if expected_summary.encode("ascii") not in c07_log:
        raise RuntimeError("c07 日志缺少预期完整汇总。")
    if b"write=true; committed_partitions=300; committed_calendar_rows=3340612" not in c07_log:
        raise RuntimeError("c07 日志缺少候选正式提交汇总。")

    journal = read_footer_journal(journal_path)
    journal_paths = [relative_path for relative_path, _, _ in journal]
    if len(journal) != 6439 or len(set(journal_paths)) != 6439:
        raise RuntimeError("c08 失败恢复 journal 不是 6439 条唯一文件。")
    old_minute_root = old_silver / "fact_futures_minute"
    candidate_minute_root = candidate_silver / "fact_futures_minute"
    for relative_path, footer_start, old_footer in journal:
        relative = pathlib.Path(relative_path)
        old_path = old_minute_root / relative
        candidate_path = candidate_minute_root / relative
        if not old_path.is_file() or candidate_path.exists():
            raise RuntimeError(f"c08 回滚后的 minute 文件位置不唯一：{relative_path}")
        current_footer_start, current_footer = parquet_footer(old_path)
        if current_footer_start != footer_start or current_footer != old_footer:
            raise RuntimeError(f"c08 回滚后的旧 minute footer 未精确恢复：{relative_path}")
    if len(list(old_minute_root.rglob("*.parquet"))) != 6496:
        raise RuntimeError("c08 回滚后旧 minute 文件数不为 6496。")
    if len(list(candidate_minute_root.rglob("*.parquet"))) != 58:
        raise RuntimeError("c08 回滚后候选 minute 文件数不为 58。")

    calendar_root = candidate_silver / "dim_futures_bar_calendar"
    calendar_leaves = leaf_directories(
        calendar_root,
        partition_columns(FUTURES_BAR_CALENDAR_SCHEMA),
    )
    contract_leaves = leaf_directories(
        candidate_silver / "dim_futures_contract_calendar",
        partition_columns(FUTURES_CONTRACT_CALENDAR_SCHEMA),
    )
    expected_calendar_keys = {
        (frequency, *base_key)
        for base_key in contract_leaves
        for frequency in ("1d", "1m")
    }
    if len(contract_leaves) != 748 or set(calendar_leaves) != expected_calendar_keys:
        raise RuntimeError("保留的 c07 候选叶集合不完整。")
    total_rows = 0
    for key in sorted(calendar_leaves):
        frame = read_leaf(calendar_root, calendar_leaves[key], FUTURES_BAR_CALENDAR_SCHEMA)
        checked = C07.validate_calendar_frame(frame, f"c08 失败后保留 c07 叶 {key} ")
        total_rows += len(checked)
    if total_rows != 9506158 or validate_exact_dataset(
        calendar_root,
        FUTURES_BAR_CALENDAR_SCHEMA,
    ) != total_rows:
        raise RuntimeError("保留的 c07 候选逐叶/根级行数不精确。")
    return {
        "resume_from": "minute_reapply_then_c08",
        "journal_records": len(journal),
        "old_minute_files": 6496,
        "candidate_minute_files": 58,
        "bar_calendar_leaves": len(calendar_leaves),
        "bar_calendar_rows": total_rows,
        "c07_selected": 51520,
        "c07_passed": 26490,
        "c07_warning": 25030,
    }


def move_reapplied_minute_leaf(
    old_leaf: pathlib.Path,
    candidate_leaf: pathlib.Path,
) -> None:
    """只用精确空目标目录替换一个 journal 已证明的旧 minute 叶。"""
    if not old_leaf.is_dir():
        if candidate_leaf.is_dir() and not old_leaf.exists():
            return
        raise RuntimeError(f"重应用 minute 旧叶不存在：{old_leaf}")
    if candidate_leaf.exists():
        if not candidate_leaf.is_dir() or any(candidate_leaf.iterdir()):
            raise RuntimeError(
                f"重应用 minute 目标叶不是精确空目录：{candidate_leaf}"
            )
        candidate_leaf.rmdir()
    candidate_leaf.parent.mkdir(parents=True, exist_ok=True)
    old_leaf.rename(candidate_leaf)


def reapply_minute_after_c08_rollback(
    transaction_root: pathlib.Path,
    old_silver: pathlib.Path,
) -> dict[str, int]:
    """按原 journal 幂等重应用 footer patch 与同盘 move，再重跑全量门禁。"""
    candidate_silver = transaction_root / "candidate_lake" / "silver"
    old_minute_root = old_silver / "fact_futures_minute"
    candidate_minute_root = candidate_silver / "fact_futures_minute"
    event_path = transaction_root / "events.jsonl"
    journal = read_footer_journal(transaction_root / "minute_footer_journal.bin")
    leaf_pairs: dict[pathlib.Path, pathlib.Path] = {}
    for relative_path, footer_start, old_footer in journal:
        relative = pathlib.Path(relative_path)
        old_path = old_minute_root / relative
        candidate_path = candidate_minute_root / relative
        if old_path.is_file() and not candidate_path.exists():
            current_footer_start, current_footer = parquet_footer(old_path)
            if current_footer_start != footer_start or current_footer != old_footer:
                raise RuntimeError(f"重应用前旧 footer 与 journal 不一致：{relative_path}")
            patch_parquet_footer(old_path, FUTURES_MINUTE_SCHEMA)
        elif candidate_path.is_file() and not old_path.exists():
            if not pq.read_schema(candidate_path).equals(
                file_schema(FUTURES_MINUTE_SCHEMA),
                check_metadata=True,
            ):
                raise RuntimeError(f"已重应用候选 footer 不精确：{relative_path}")
        else:
            raise RuntimeError(f"重应用 minute 文件无法唯一定位：{relative_path}")
        leaf_pairs[old_path.parent] = candidate_path.parent

    if len(leaf_pairs) != 6439:
        raise RuntimeError("重应用 journal 未形成 6439 个唯一叶。")
    for number, (old_leaf, candidate_leaf) in enumerate(
        sorted(leaf_pairs.items(), key=lambda item: item[0].as_posix()),
        start=1,
    ):
        move_reapplied_minute_leaf(old_leaf, candidate_leaf)
        if number % 250 == 0:
            append_event(event_path, {
                "action": "reapplied_minute_after_c08_rollback_progress",
                "leaves": number,
            })

    validation = validate_all_minute_leaves_vectorized(
        candidate_silver,
        event_path,
        progress_action="reapplied_minute_vector_validation_progress",
        completion_action="reapplied_minute_vector_validation_completed",
    )
    expected = {"leaves": 6496, "rows": 517390417, "invalid_ohlc_rows": 413}
    if validation != expected:
        raise RuntimeError(f"重应用 minute 全量门禁不匹配：{validation}")
    append_event(event_path, {
        "action": "minute_reapplied_after_c08_rollback_completed",
        **validation,
    })
    return validation


def rollback_old_minute_moves(
    old_minute_root: pathlib.Path,
    candidate_minute_root: pathlib.Path,
    formal_minute_root: pathlib.Path,
    journal_path: pathlib.Path,
) -> None:
    # root swap 若已发生，候选分钟根可能暂时位于正式路径。
    source_root = (
        candidate_minute_root
        if candidate_minute_root.is_dir()
        else formal_minute_root
    )
    old_minute_root.mkdir(parents=True, exist_ok=True)
    for relative_path, _, _ in reversed(read_footer_journal(journal_path)):
        relative = pathlib.Path(relative_path)
        source_file = source_root / relative
        target_file = old_minute_root / relative
        if source_file.is_file() and not target_file.exists():
            target_file.parent.mkdir(parents=True, exist_ok=True)
            source_file.rename(target_file)
    restore_footer_journal(
        old_minute_root,
        source_root,
        journal_path,
    )


def inventory(root: pathlib.Path) -> dict[str, dict[str, int]]:
    result = {}
    if not root.is_dir():
        return result
    for table_root in sorted(path for path in root.iterdir() if path.is_dir()):
        files = list(table_root.rglob("*.parquet"))
        result[table_root.name] = {
            "files": len(files),
            "rows": sum(pq.ParquetFile(path).metadata.num_rows for path in files),
            "bytes": sum(path.stat().st_size for path in files),
        }
    return result


def plan(old_silver: pathlib.Path, formal_lake_root: pathlib.Path) -> dict[str, object]:
    formal_silver = formal_lake_root / "silver"
    if old_silver.resolve() == formal_silver.resolve():
        raise ValueError("旧湖与正式湖不得相同。")
    if old_silver.drive.lower() != formal_silver.drive.lower():
        raise ValueError("低 IO rename 要求旧湖与正式湖位于同一卷。")
    required_old = {
        "dim_trade_calendar",
        "dim_futures_variety_calendar",
        "dim_futures_contract_calendar",
        "dim_futures_session_schedule_signal",
        "fact_futures_daily",
        "fact_futures_fetch_status",
        "fact_futures_minute",
        "fact_futures_missing_bar",
    }
    missing = sorted(name for name in required_old if not (old_silver / name).is_dir())
    if missing:
        raise FileNotFoundError(f"旧湖缺少预期表：{missing}")
    old_minute = leaf_directories(
        old_silver / "fact_futures_minute",
        partition_columns(FUTURES_MINUTE_SCHEMA),
    )
    current_minute = leaf_directories(
        formal_silver / "fact_futures_minute",
        partition_columns(FUTURES_MINUTE_SCHEMA),
    )
    return {
        "mode": "read_only_plan",
        "old_silver": str(old_silver.resolve()),
        "formal_lake_root": str(formal_lake_root.resolve()),
        "same_volume": True,
        "old_inventory": inventory(old_silver),
        "formal_inventory": inventory(formal_silver),
        "minute_non_overlap_leaves": len(set(old_minute) - set(current_minute)),
        "minute_overlap_leaves": len(set(old_minute) & set(current_minute)),
        "strategy": {
            "dimensions_daily": "current-PK-first leaf rewrite",
            "minute_non_overlap": "footer-only metadata patch + same-volume rename",
            "minute_overlap": "current-PK-first leaf rewrite",
            "bar_calendar": "current c04 theoretical grid + offline fact/status reconciliation",
            "missing": "discard old derived rows; rebuild with current c08",
            "commit": "seven table-root swaps with rollback backups",
        },
    }


def execute(old_silver: pathlib.Path, formal_lake_root: pathlib.Path) -> dict[str, object]:
    preflight = plan(old_silver, formal_lake_root)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    formal_silver = formal_lake_root / "silver"
    transaction_root = formal_lake_root.parent / f".legacy-migration-{run_id}"
    candidate_lake = transaction_root / "candidate_lake"
    candidate_silver = candidate_lake / "silver"
    event_path = transaction_root / "events.jsonl"
    journal_path = transaction_root / "minute_footer_journal.bin"
    report_path = (
        PROJECT_ROOT
        / "00_draft_collection_02"
        / "migration_reports"
        / f"legacy-futures-lake-{run_id}.json"
    )
    backups: dict[str, pathlib.Path] = {}
    swapped: list[str] = []
    result: dict[str, object] = {
        "run_id": run_id,
        "preflight": preflight,
        "started_at": datetime.now(timezone.utc).isoformat(),
    }
    transaction_root.mkdir(parents=True, exist_ok=False)
    candidate_silver.mkdir(parents=True, exist_ok=False)
    append_event(event_path, {"action": "transaction_started", "run_id": run_id})

    try:
        rewritten = {}
        for table_name in [
            "dim_trade_calendar",
            "dim_futures_variety_calendar",
            "dim_futures_contract_calendar",
        ]:
            rewritten[table_name] = migrate_rewritten_table(
                table_name,
                old_silver,
                formal_silver,
                candidate_silver,
                run_id,
                event_path,
            )
        rewritten["fact_futures_daily"] = migrate_daily_table(
            old_silver,
            formal_silver,
            candidate_silver,
            run_id,
            event_path,
        )
        result["rewritten"] = rewritten

        result["minute_merge"] = merge_minute_table(
            old_silver,
            formal_silver,
            candidate_silver,
            transaction_root,
            run_id,
            event_path,
        )
        result["minute_validation"] = validate_all_minute_leaves_vectorized(
            candidate_silver,
            event_path,
        )

        # c04 从已迁移的完整 c03 上游生成新理论格点；当前 2026-08 状态自动优先继承。
        copy_tree(
            formal_silver / "dim_futures_bar_calendar",
            candidate_silver / "dim_futures_bar_calendar",
        )
        assert_vector_c04_equivalence(candidate_silver, event_path)
        result["bar_calendar_vectorized"] = build_bar_calendar_vectorized(
            candidate_silver,
            run_id,
            event_path,
        )
        append_event(event_path, {"action": "c04_completed_vectorized"})

        result["calendar_reconciliation"] = reconcile_calendar_from_facts(
            old_silver,
            candidate_lake,
            run_id,
            event_path,
        )

        # 当前 c07 先复核疑似休市，c08 再逐 Session 展开理论时点并全量重建 missing。
        run_current_entry(
            "b07_suspected_session_reconciliation.py",
            candidate_lake,
            ["--write"],
            transaction_root / "c07.log",
        )
        append_event(event_path, {"action": "c07_completed"})
        run_current_entry(
            "b08_full_minute_quality.py",
            candidate_lake,
            ["--confirm-full-quality", "--write"],
            transaction_root / "c08.log",
        )
        append_event(event_path, {"action": "c08_completed"})

        exact_rows = {}
        for table_name in AFFECTED_TABLES:
            exact_rows[table_name] = validate_exact_dataset(
                candidate_silver / table_name,
                TABLE_SCHEMAS[table_name],
            )
        result["candidate_exact_rows"] = exact_rows
        identity_before = {
            table_name: file_identity_manifest(candidate_silver / table_name)
            for table_name in AFFECTED_TABLES
        }
        append_event(event_path, {"action": "candidate_fully_validated"})

        # 所有候选验证通过后才进入很短的正式 root swap 窗口。
        for table_name in AFFECTED_TABLES:
            formal_table = formal_silver / table_name
            candidate_table = candidate_silver / table_name
            backup = formal_silver / f".{table_name}.pre-migration-{run_id}"
            if backup.exists():
                raise FileExistsError(f"事务备份已存在：{backup}")
            if formal_table.exists():
                formal_table.rename(backup)
                backups[table_name] = backup
                append_event(event_path, {"action": "formal_to_backup", "table": table_name})
            try:
                candidate_table.rename(formal_table)
            except BaseException:
                if backup.exists() and not formal_table.exists():
                    backup.rename(formal_table)
                    backups.pop(table_name, None)
                raise
            swapped.append(table_name)
            append_event(event_path, {"action": "candidate_to_formal", "table": table_name})

        # 正式路径不再做第三次全值扫描；精确 Schema、行数与 inode/size 证明就是已验证候选。
        formal_rows = {}
        for table_name in AFFECTED_TABLES:
            formal_rows[table_name] = validate_exact_dataset(
                formal_silver / table_name,
                TABLE_SCHEMAS[table_name],
            )
            if formal_rows[table_name] != exact_rows[table_name]:
                raise ValueError(f"正式 root swap 后行数漂移：{table_name}")
            if file_identity_manifest(formal_silver / table_name) != identity_before[table_name]:
                raise ValueError(f"正式 root swap 后文件身份漂移：{table_name}")
        result["formal_exact_rows"] = formal_rows
        append_event(event_path, {"action": "formal_reread_completed"})

        result["completed_at"] = datetime.now(timezone.utc).isoformat()
        result["old_lake_removed"] = True
        result["transaction_backups_removed"] = True
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(result, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )

        # 到这里正式湖已通过验证；按用户授权精确清理旧湖和事务备份。
        for backup in backups.values():
            shutil.rmtree(backup)
        backups.clear()
        shutil.rmtree(old_silver)
        append_event(event_path, {"action": "old_lake_and_backups_removed"})
        result["report_path"] = str(report_path)
        return result
    except BaseException as error:
        rollback_errors = []
        # 先逆序撤销 root swap。
        for table_name in reversed(swapped):
            formal_table = formal_silver / table_name
            candidate_table = candidate_silver / table_name
            backup = backups.get(table_name)
            try:
                if formal_table.exists():
                    candidate_table.parent.mkdir(parents=True, exist_ok=True)
                    formal_table.rename(candidate_table)
                if backup is not None and backup.exists():
                    backup.rename(formal_table)
            except BaseException as rollback_error:
                rollback_errors.append(
                    f"root {table_name}: {type(rollback_error).__name__}: {rollback_error}"
                )
        # 再把已移动的旧分钟文件放回并恢复原 footer。
        try:
            rollback_old_minute_moves(
                old_silver / "fact_futures_minute",
                candidate_silver / "fact_futures_minute",
                formal_silver / "fact_futures_minute",
                journal_path,
            )
        except BaseException as rollback_error:
            rollback_errors.append(
                f"minute: {type(rollback_error).__name__}: {rollback_error}"
            )
        failure = {
            **result,
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "error": f"{type(error).__name__}: {error}",
            "traceback": traceback.format_exc(),
            "rollback_errors": rollback_errors,
            "transaction_root_retained": str(transaction_root),
        }
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(failure, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        if rollback_errors:
            raise RuntimeError(
                "迁移失败且回滚不完整；现场已保留："
                f"{transaction_root}; errors={rollback_errors}"
            ) from error
        # 回滚完整也保留事务日志与候选现场，便于定位；旧湖和正式湖均已恢复。
        raise


def json_file_identity_manifest(root: pathlib.Path) -> dict[str, list[int]]:
    return {
        relative: [int(size), int(file_id)]
        for relative, (size, file_id) in file_identity_manifest(root).items()
    }


def strong_file_manifest(root: pathlib.Path) -> dict[str, list[object]]:
    result: dict[str, list[object]] = {}
    for path in sorted(root.rglob("*.parquet")):
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
        result[path.relative_to(root).as_posix()] = [
            int(path.stat().st_size),
            digest.hexdigest(),
        ]
    return result


def write_fsync_json(path: pathlib.Path, payload: dict[str, object]) -> None:
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    envelope = {
        "sha256": hashlib.sha256(canonical).hexdigest(),
        "payload": payload,
    }
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex[:12]}.tmp")
    with temporary.open("x", encoding="utf-8") as handle:
        json.dump(envelope, handle, ensure_ascii=False, indent=2, default=str)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def read_fsync_json(path: pathlib.Path) -> dict[str, object]:
    envelope = json.loads(path.read_text(encoding="utf-8"))
    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        raise TypeError(f"checkpoint payload 非对象：{path}")
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    if envelope.get("sha256") != hashlib.sha256(canonical).hexdigest():
        raise ValueError(f"checkpoint 摘要不匹配：{path}")
    return payload


def write_plain_fsync_json(path: pathlib.Path, payload: dict[str, object]) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex[:12]}.tmp")
    with temporary.open("x", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, default=str)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def assert_identity_manifest(
    root: pathlib.Path,
    expected: dict[str, list[int]],
    label: str,
) -> None:
    if not root.is_dir():
        raise FileNotFoundError(f"{label}不存在：{root}")
    actual = json_file_identity_manifest(root)
    if actual != expected:
        raise RuntimeError(f"{label}文件身份漂移：{root}")


def rename_directory_with_retry(
    source: pathlib.Path,
    target: pathlib.Path,
    event_path: pathlib.Path,
    action: str,
    table_name: str,
    identity_guard,
) -> None:
    delays = (0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 30.0)
    for attempt in range(1, len(delays) + 2):
        if not source.is_dir() or target.exists():
            raise RuntimeError(
                f"rename 前状态不唯一：source={source.is_dir()} target={target.exists()} "
                f"{source} -> {target}"
            )
        identity_guard()
        try:
            source.rename(target)
            return
        except PermissionError as error:
            winerror = getattr(error, "winerror", None)
            if os.name != "nt" or winerror not in {5, 32} or attempt > len(delays):
                raise
            if not source.is_dir() or target.exists():
                raise RuntimeError(
                    f"rename 失败后状态漂移：{source} -> {target}"
                ) from error
            append_event(event_path, {
                "action": "windows_rename_retry",
                "operation": action,
                "table": table_name,
                "attempt": attempt,
                "winerror": winerror,
                "delay_seconds": delays[attempt - 1],
            })
            time.sleep(delays[attempt - 1])


def process_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        import ctypes

        process_query_limited_information = 0x1000
        handle = ctypes.windll.kernel32.OpenProcess(
            process_query_limited_information,
            False,
            pid,
        )
        if not handle:
            # Access denied means a process exists but cannot be queried; stop safely.
            return ctypes.get_last_error() == 5
        try:
            exit_code = ctypes.c_ulong()
            if not ctypes.windll.kernel32.GetExitCodeProcess(
                handle,
                ctypes.byref(exit_code),
            ):
                return True
            return exit_code.value == 259
        finally:
            ctypes.windll.kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def acquire_post_c08_lock(
    lock_path: pathlib.Path,
    event_path: pathlib.Path,
    run_id: str,
) -> None:
    while True:
        try:
            lock_fd = os.open(lock_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
            break
        except FileExistsError as error:
            try:
                existing = json.loads(lock_path.read_text(encoding="utf-8"))
                pids = [
                    int(existing.get(name, 0) or 0)
                    for name in ("pid", "child_pid")
                ]
            except (OSError, ValueError, TypeError) as parse_error:
                raise RuntimeError(
                    f"恢复 PID 锁无法证明为 stale：{lock_path}"
                ) from parse_error
            if existing.get("run_id") != run_id or any(
                process_is_running(pid) for pid in pids
            ):
                raise RuntimeError(
                    f"恢复 PID 锁仍活跃或 run_id 不匹配：{lock_path}"
                ) from error
            lock_path.unlink()
            append_event(event_path, {
                "action": "post_c08_stale_pid_lock_removed",
                "pids": pids,
            })
    with os.fdopen(lock_fd, "w", encoding="utf-8") as handle:
        json.dump({"pid": os.getpid(), "run_id": run_id}, handle)
        handle.flush()
        os.fsync(handle.fileno())


def prove_post_c08_outputs(
    transaction_root: pathlib.Path,
    old_silver: pathlib.Path,
    formal_lake_root: pathlib.Path,
) -> dict[str, object]:
    candidate_silver = transaction_root / "candidate_lake" / "silver"
    formal_silver = formal_lake_root / "silver"
    event_path = transaction_root / "events.jsonl"
    report_path = (
        PROJECT_ROOT
        / "00_draft_collection_02"
        / "migration_reports"
        / f"legacy-futures-lake-{transaction_root.name.removeprefix('.legacy-migration-')}.json"
    )
    preserved_missing = transaction_root / POST_C08_MISSING_PRESERVED

    if any(formal_silver.glob(".*.pre-migration-*")) or any(
        formal_silver.glob(".lm-b-*")
    ):
        raise RuntimeError("post-c08 恢复前正式 root 存在 backup。")
    if not preserved_missing.is_dir():
        raise FileNotFoundError("缺少 c08 保全 missing root。")

    raw_events = event_path.read_bytes()
    if not raw_events.endswith(b"\n"):
        raise ValueError("events.jsonl 尾部不完整。")
    events = [json.loads(line) for line in raw_events.splitlines()]
    actions = [event.get("action") for event in events]
    required_counts = {
        "c08_completed": 1,
        "candidate_fully_validated": 1,
        "formal_to_backup": 6,
        "candidate_to_formal": 5,
        "formal_reread_completed": 0,
        "old_lake_and_backups_removed": 0,
    }
    for action, expected_count in required_counts.items():
        if actions.count(action) != expected_count:
            raise RuntimeError(
                f"post-c08 核心事件计数不精确：{action}={actions.count(action)}"
            )
    root_swap_events = [
        event
        for event in events
        if event.get("action") in {"formal_to_backup", "candidate_to_formal"}
    ]
    if not root_swap_events or root_swap_events[-1].get("action") != "formal_to_backup" or (
        root_swap_events[-1].get("table") != "fact_futures_minute"
    ):
        raise RuntimeError("post-c08 事件尾部不是 minute formal_to_backup。")

    original_report_path = transaction_root / "post-c08-original-failure.json"
    report = json.loads(
        (original_report_path if original_report_path.exists() else report_path).read_text(
            encoding="utf-8"
        )
    )
    if report.get("candidate_exact_rows") != POST_C08_EXACT_ROWS:
        raise RuntimeError("失败报告的候选七表精确行数不匹配。")
    if report.get("rollback_errors") != []:
        raise RuntimeError("失败报告显示回滚不完整。")
    if "fact_futures_minute" not in str(report.get("error", "")) or (
        "WinError 5" not in str(report.get("error", ""))
    ):
        raise RuntimeError("失败报告不是已知 minute root WinError 5。")

    for table_name in AFFECTED_TABLES[:5]:
        rows = validate_exact_dataset(
            candidate_silver / table_name,
            TABLE_SCHEMAS[table_name],
        )
        if rows != POST_C08_EXACT_ROWS[table_name]:
            raise RuntimeError(f"post-c08 候选表行数漂移：{table_name}={rows}")

    journal = read_footer_journal(transaction_root / "minute_footer_journal.bin")
    journal_paths = [relative for relative, _, _ in journal]
    if len(journal) != 6439 or len(set(journal_paths)) != 6439:
        raise RuntimeError("post-c08 minute journal 不是 6439 条唯一文件。")
    old_minute = old_silver / "fact_futures_minute"
    candidate_minute = candidate_silver / "fact_futures_minute"
    old_locations = 0
    candidate_locations = 0
    for relative_path, footer_start, old_footer in journal:
        relative = pathlib.Path(relative_path)
        old_path = old_minute / relative
        candidate_path = candidate_minute / relative
        if old_path.is_file() and not candidate_path.exists():
            current_start, current_footer = parquet_footer(old_path)
            if current_start != footer_start or current_footer != old_footer:
                raise RuntimeError(f"旧 minute footer 不精确：{relative_path}")
            old_locations += 1
        elif candidate_path.is_file() and not old_path.exists():
            if not pq.read_schema(candidate_path).equals(
                file_schema(FUTURES_MINUTE_SCHEMA),
                check_metadata=True,
            ):
                raise RuntimeError(f"候选 minute footer 不精确：{relative_path}")
            candidate_locations += 1
        else:
            raise RuntimeError(f"minute journal 文件位置不唯一：{relative_path}")
    if old_locations + candidate_locations != 6439:
        raise RuntimeError("minute journal 文件集合不完整。")

    missing_rows = validate_exact_dataset(
        preserved_missing,
        FUTURES_MISSING_BAR_SCHEMA,
    )
    if missing_rows != POST_C08_EXACT_ROWS["fact_futures_missing_bar"]:
        raise RuntimeError("保全 missing root 精确行数不匹配。")

    c08_lines = (transaction_root / "c08.log").read_text(
        encoding="utf-8"
    ).splitlines()
    try:
        summary_start = c08_lines.index("partition_summary:") + 2
    except ValueError as error:
        raise RuntimeError("c08.log 缺少 partition_summary。") from error
    logged_missing: dict[tuple[object, ...], int] = {}
    for line in c08_lines[summary_start:]:
        if line.startswith("full_quality_total:"):
            break
        parts = line.split()
        if len(parts) != 10:
            raise RuntimeError(f"c08.log 分区汇总行无法解析：{line}")
        exchange_code, underlying_code, year, month = parts[:4]
        missing_count = int(parts[7])
        if missing_count:
            logged_missing[(
                "1m",
                exchange_code,
                underlying_code,
                int(year),
                int(month),
            )] = missing_count

    artifact_missing: dict[tuple[object, ...], int] = {}
    for path in preserved_missing.rglob("*.parquet"):
        if path.name == "schema.parquet":
            continue
        parts = path.relative_to(preserved_missing).parts[:-1]
        if len(parts) != 5:
            raise RuntimeError(f"保全 missing 非法叶路径：{path}")
        values = {
            part.split("=", 1)[0]: part.split("=", 1)[1]
            for part in parts
        }
        key = (
            values["bar_frequency"],
            values["exchange_code"],
            values["underlying_code"],
            int(values["year"]),
            int(values["month"]),
        )
        if key in artifact_missing:
            raise RuntimeError(f"保全 missing 叶有多个文件：{key}")
        artifact_missing[key] = int(pq.ParquetFile(path).metadata.num_rows)
    if artifact_missing != logged_missing:
        raise RuntimeError("保全 missing 的逐叶行数与 c08.log 不一致。")

    calendar_dataset = ds.dataset(
        candidate_silver / "dim_futures_bar_calendar",
        format="parquet",
        partitioning=hive_partitioning(FUTURES_BAR_CALENDAR_SCHEMA),
    )
    scanner = calendar_dataset.scanner(
        columns=[
            "expected_bar_count",
            "actual_bar_count",
            "missing_bar_count",
        ],
        filter=(ds.field("bar_frequency") == "1m")
        & (ds.field("is_fetch_required") == True),  # noqa: E712
        batch_size=131072,
    )
    calendar_totals = {
        "sessions": 0,
        "expected": 0,
        "actual": 0,
        "missing": 0,
    }
    for batch in scanner.to_batches():
        calendar_totals["sessions"] += batch.num_rows
        for source_name, target_name in [
            ("expected_bar_count", "expected"),
            ("actual_bar_count", "actual"),
            ("missing_bar_count", "missing"),
        ]:
            calendar_totals[target_name] += int(
                pc.sum(batch.column(source_name)).as_py() or 0
            )
    expected_totals = {
        "sessions": 5580486,
        "expected": 524099085,
        "actual": 515147122,
        "missing": 8951963,
    }
    if calendar_totals != expected_totals:
        raise RuntimeError(
            f"保留的 c08 行情日历汇总不精确：{calendar_totals}"
        )
    return {
        "journal_records": len(journal),
        "journal_old_locations": old_locations,
        "journal_candidate_locations": candidate_locations,
        "missing_rows": missing_rows,
        "missing_partitions": len(artifact_missing),
        "calendar_totals": calendar_totals,
    }


def prepare_post_c08_swap(
    transaction_root: pathlib.Path,
    old_silver: pathlib.Path,
    formal_lake_root: pathlib.Path,
) -> dict[str, object]:
    candidate_silver = transaction_root / "candidate_lake" / "silver"
    formal_silver = formal_lake_root / "silver"
    event_path = transaction_root / "events.jsonl"
    checkpoint_path = transaction_root / POST_C08_CHECKPOINT
    preserved_missing = transaction_root / POST_C08_MISSING_PRESERVED
    proof = prove_post_c08_outputs(
        transaction_root,
        old_silver,
        formal_lake_root,
    )
    append_event(event_path, {"action": "post_c08_proof_completed", **proof})

    minute_validation = reapply_minute_after_c08_rollback(
        transaction_root,
        old_silver,
    )
    candidate_missing = candidate_silver / "fact_futures_missing_bar"
    missing_copy = transaction_root / ".p8-m-copy"
    expected_missing_manifest = strong_file_manifest(preserved_missing)
    if candidate_missing.exists():
        if strong_file_manifest(candidate_missing) != expected_missing_manifest:
            raise RuntimeError("现有候选 missing 与 c08 保全副本不一致。")
    else:
        if missing_copy.exists():
            if strong_file_manifest(missing_copy) != expected_missing_manifest:
                raise RuntimeError("中断遗留 missing copy 与保全副本不一致。")
        else:
            shutil.copytree(preserved_missing, missing_copy, copy_function=shutil.copy2)
        rename_directory_with_retry(
            missing_copy,
            candidate_missing,
            event_path,
            "preserved_missing_to_candidate",
            "fact_futures_missing_bar",
            lambda: (
                None
                if strong_file_manifest(missing_copy) == expected_missing_manifest
                else (_ for _ in ()).throw(
                    RuntimeError("missing copy 在 rename 前漂移。")
                )
            ),
        )
    if strong_file_manifest(candidate_missing) != expected_missing_manifest:
        raise RuntimeError("候选 missing 恢复后与保全副本不一致。")

    exact_rows = {
        table_name: validate_exact_dataset(
            candidate_silver / table_name,
            TABLE_SCHEMAS[table_name],
        )
        for table_name in AFFECTED_TABLES
    }
    if exact_rows != POST_C08_EXACT_ROWS:
        raise RuntimeError(f"post-c08 候选七表行数不精确：{exact_rows}")
    candidate_manifests = {
        table_name: json_file_identity_manifest(candidate_silver / table_name)
        for table_name in AFFECTED_TABLES
    }
    # 原 formal 是本次迁移要被替换并可回滚的旧 baseline；它不必先满足
    # 新契约。这里只要求七个根都存在、Parquet 可读，并记录精确文件身份。
    formal_rows = {}
    for table_name in AFFECTED_TABLES:
        formal_table = formal_silver / table_name
        formal_files = sorted(formal_table.rglob("*.parquet"))
        if not formal_files:
            raise FileNotFoundError(f"原 formal 表缺少 Parquet：{formal_table}")
        formal_rows[table_name] = sum(
            int(pq.ParquetFile(path).metadata.num_rows)
            for path in formal_files
        )
    formal_manifests = {
        table_name: json_file_identity_manifest(formal_silver / table_name)
        for table_name in AFFECTED_TABLES
    }
    payload: dict[str, object] = {
        "version": 1,
        "stage": "ready",
        "run_id": transaction_root.name.removeprefix(".legacy-migration-"),
        "prepared_at": datetime.now(timezone.utc).isoformat(),
        "proof": proof,
        "minute_validation": minute_validation,
        "candidate_exact_rows": exact_rows,
        "candidate_manifests": candidate_manifests,
        "formal_rows_before": formal_rows,
        "formal_manifests_before": formal_manifests,
        "missing_strong_manifest": expected_missing_manifest,
    }
    write_fsync_json(checkpoint_path, payload)
    checkpoint_sha = hashlib.sha256(checkpoint_path.read_bytes()).hexdigest()
    append_event(event_path, {
        "action": "post_c08_swap_ready",
        "checkpoint": checkpoint_path.name,
        "checkpoint_sha256": checkpoint_sha,
    })
    return payload


def run_post_c08_prepare_child(
    transaction_root: pathlib.Path,
    old_silver: pathlib.Path,
    formal_lake_root: pathlib.Path,
) -> None:
    log_path = transaction_root / "post-c08-prepare.log"
    command = [
        str(pathlib.Path(sys.executable)),
        str(pathlib.Path(__file__).resolve()),
        "--_prepare-post-c08",
        str(transaction_root),
        "--old-silver",
        str(old_silver),
        "--lake-root",
        str(formal_lake_root),
    ]
    with log_path.open("w", encoding="utf-8") as handle:
        child = subprocess.Popen(
            command,
            cwd=PROJECT_ROOT,
            stdin=subprocess.DEVNULL,
            stdout=handle,
            stderr=subprocess.STDOUT,
        )
        write_plain_fsync_json(
            transaction_root / "resume.pid",
            {
                "pid": os.getpid(),
                "child_pid": child.pid,
                "run_id": transaction_root.name.removeprefix(".legacy-migration-"),
            },
        )
        returncode = child.wait()
    write_plain_fsync_json(
        transaction_root / "resume.pid",
        {
            "pid": os.getpid(),
            "run_id": transaction_root.name.removeprefix(".legacy-migration-"),
        },
    )
    if returncode != 0:
        tail = log_path.read_text(encoding="utf-8", errors="replace")[-12000:]
        raise RuntimeError(
            f"post-c08 准备子进程失败 code={returncode}\n{tail}"
        )


def verify_post_c08_formal(
    transaction_root: pathlib.Path,
    formal_lake_root: pathlib.Path,
) -> dict[str, int]:
    checkpoint = read_fsync_json(transaction_root / POST_C08_CHECKPOINT)
    formal_silver = formal_lake_root / "silver"
    expected_rows = checkpoint["candidate_exact_rows"]
    expected_manifests = checkpoint["candidate_manifests"]
    rows: dict[str, int] = {}
    for table_name in AFFECTED_TABLES:
        rows[table_name] = validate_exact_dataset(
            formal_silver / table_name,
            TABLE_SCHEMAS[table_name],
        )
        if rows[table_name] != int(expected_rows[table_name]):
            raise RuntimeError(f"正式复读行数漂移：{table_name}")
        assert_identity_manifest(
            formal_silver / table_name,
            expected_manifests[table_name],
            f"正式复读 {table_name}",
        )
    return rows


def run_post_c08_formal_verify_child(
    transaction_root: pathlib.Path,
    formal_lake_root: pathlib.Path,
) -> None:
    log_path = transaction_root / "post-c08-formal-verify.log"
    command = [
        str(pathlib.Path(sys.executable)),
        str(pathlib.Path(__file__).resolve()),
        "--_verify-post-c08-formal",
        str(transaction_root),
        "--lake-root",
        str(formal_lake_root),
    ]
    with log_path.open("w", encoding="utf-8") as handle:
        child = subprocess.Popen(
            command,
            cwd=PROJECT_ROOT,
            stdin=subprocess.DEVNULL,
            stdout=handle,
            stderr=subprocess.STDOUT,
        )
        write_plain_fsync_json(
            transaction_root / "resume.pid",
            {
                "pid": os.getpid(),
                "child_pid": child.pid,
                "run_id": transaction_root.name.removeprefix(".legacy-migration-"),
            },
        )
        returncode = child.wait()
    write_plain_fsync_json(
        transaction_root / "resume.pid",
        {
            "pid": os.getpid(),
            "run_id": transaction_root.name.removeprefix(".legacy-migration-"),
        },
    )
    if returncode != 0:
        tail = log_path.read_text(encoding="utf-8", errors="replace")[-12000:]
        raise RuntimeError(
            f"post-c08 正式复读子进程失败 code={returncode}\n{tail}"
        )


def post_c08_backup_path(
    formal_silver: pathlib.Path,
    table_name: str,
    run_id: str,
) -> pathlib.Path:
    return formal_silver / (
        f".lm-b-{AFFECTED_TABLES.index(table_name)}-{run_id[-8:]}"
    )


def post_c08_table_state(
    candidate: pathlib.Path,
    formal: pathlib.Path,
    backup: pathlib.Path,
    candidate_manifest: dict[str, list[int]],
    formal_manifest: dict[str, list[int]],
    allow_removed_backup: bool,
) -> str:
    candidate_matches = (
        candidate.is_dir()
        and json_file_identity_manifest(candidate) == candidate_manifest
    )
    formal_is_candidate = (
        formal.is_dir()
        and json_file_identity_manifest(formal) == candidate_manifest
    )
    formal_is_original = (
        formal.is_dir()
        and json_file_identity_manifest(formal) == formal_manifest
    )
    backup_is_original = (
        backup.is_dir()
        and json_file_identity_manifest(backup) == formal_manifest
    )
    if candidate_matches and formal_is_original and not backup.exists():
        return "unswapped"
    if candidate_matches and not formal.exists() and backup_is_original:
        return "backed_up"
    if not candidate.exists() and formal_is_candidate and backup_is_original:
        return "swapped"
    if (
        allow_removed_backup
        and not candidate.exists()
        and formal_is_candidate
        and not backup.exists()
    ):
        return "swapped_backup_removed"
    raise RuntimeError(
        f"root swap 状态无法唯一证明：candidate={candidate} formal={formal} backup={backup}"
    )


def resume_post_c08_transaction(
    transaction_root: pathlib.Path,
    old_silver: pathlib.Path,
    formal_lake_root: pathlib.Path,
) -> dict[str, object]:
    transaction_root = transaction_root.resolve()
    run_id = transaction_root.name.removeprefix(".legacy-migration-")
    candidate_silver = transaction_root / "candidate_lake" / "silver"
    formal_silver = formal_lake_root / "silver"
    event_path = transaction_root / "events.jsonl"
    checkpoint_path = transaction_root / POST_C08_CHECKPOINT
    report_path = (
        PROJECT_ROOT
        / "00_draft_collection_02"
        / "migration_reports"
        / f"legacy-futures-lake-{run_id}.json"
    )
    lock_path = transaction_root / "resume.pid"
    acquire_post_c08_lock(lock_path, event_path, run_id)

    original_report_path = transaction_root / "post-c08-original-failure.json"
    if not original_report_path.exists():
        original_report = report_path.read_bytes()
        with original_report_path.open("xb") as handle:
            handle.write(original_report)
            handle.flush()
            os.fsync(handle.fileno())

    result: dict[str, object] = {
        "run_id": run_id,
        "resumed_at": datetime.now(timezone.utc).isoformat(),
        "resume_from": "post_c08_swap_only",
    }
    try:
        if checkpoint_path.exists():
            checkpoint = read_fsync_json(checkpoint_path)
            if checkpoint.get("stage") == "rolled_back":
                run_post_c08_prepare_child(
                    transaction_root,
                    old_silver,
                    formal_lake_root,
                )
                checkpoint = read_fsync_json(checkpoint_path)
        else:
            run_post_c08_prepare_child(
                transaction_root,
                old_silver,
                formal_lake_root,
            )
            checkpoint = read_fsync_json(checkpoint_path)
        if checkpoint.get("run_id") != run_id:
            raise RuntimeError("post-c08 checkpoint run_id 不匹配。")
        if checkpoint.get("stage") not in {"ready", "formal_verified"}:
            raise RuntimeError("post-c08 checkpoint 阶段非法。")

        candidate_manifests = checkpoint["candidate_manifests"]
        formal_manifests = checkpoint["formal_manifests_before"]
        append_event(event_path, {
            "action": "post_c08_swap_only_started",
            "stage": checkpoint["stage"],
        })
        allow_removed = checkpoint["stage"] == "formal_verified"

        if checkpoint["stage"] == "ready":
            for table_name in AFFECTED_TABLES:
                candidate = candidate_silver / table_name
                formal = formal_silver / table_name
                backup = post_c08_backup_path(formal_silver, table_name, run_id)
                state = post_c08_table_state(
                    candidate,
                    formal,
                    backup,
                    candidate_manifests[table_name],
                    formal_manifests[table_name],
                    False,
                )
                if state == "unswapped":
                    rename_directory_with_retry(
                        formal,
                        backup,
                        event_path,
                        "formal_to_backup",
                        table_name,
                        lambda path=formal, manifest=formal_manifests[table_name]: (
                            assert_identity_manifest(
                                path,
                                manifest,
                                f"swap 前正式 {table_name}",
                            )
                        ),
                    )
                    append_event(event_path, {
                        "action": "post_c08_formal_to_backup",
                        "table": table_name,
                    })
                    state = "backed_up"
                if state == "backed_up":
                    rename_directory_with_retry(
                        candidate,
                        formal,
                        event_path,
                        "candidate_to_formal",
                        table_name,
                        lambda path=candidate, manifest=candidate_manifests[table_name], backup_path=backup, backup_manifest=formal_manifests[table_name]: (
                            assert_identity_manifest(
                                path,
                                manifest,
                                f"swap 前候选 {table_name}",
                            ),
                            assert_identity_manifest(
                                backup_path,
                                backup_manifest,
                                f"swap 前 backup {table_name}",
                            ),
                        ),
                    )
                    append_event(event_path, {
                        "action": "post_c08_candidate_to_formal",
                        "table": table_name,
                    })
                final_state = post_c08_table_state(
                    candidate,
                    formal,
                    backup,
                    candidate_manifests[table_name],
                    formal_manifests[table_name],
                    False,
                )
                if final_state != "swapped":
                    raise RuntimeError(f"swap 后状态不为 swapped：{table_name}")

            run_post_c08_formal_verify_child(transaction_root, formal_lake_root)
            formal_rows = {
                table_name: int(checkpoint["candidate_exact_rows"][table_name])
                for table_name in AFFECTED_TABLES
            }
            result["formal_exact_rows"] = formal_rows
            append_event(event_path, {
                "action": "post_c08_formal_reread_completed",
                "rows": formal_rows,
            })
            checkpoint = {**checkpoint, "stage": "formal_verified"}
            write_fsync_json(checkpoint_path, checkpoint)
            allow_removed = True

        for table_name in AFFECTED_TABLES:
            candidate = candidate_silver / table_name
            formal = formal_silver / table_name
            backup = post_c08_backup_path(formal_silver, table_name, run_id)
            state = post_c08_table_state(
                candidate,
                formal,
                backup,
                candidate_manifests[table_name],
                formal_manifests[table_name],
                allow_removed,
            )
            if state == "swapped":
                shutil.rmtree(backup)
            elif state != "swapped_backup_removed":
                raise RuntimeError(f"正式复读后 root 状态非法：{table_name}")
        append_event(event_path, {"action": "post_c08_backups_removed"})

        if old_silver.exists():
            shutil.rmtree(old_silver)
        append_event(event_path, {"action": "post_c08_old_silver_removed"})
        result.update({
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "candidate_exact_rows": checkpoint["candidate_exact_rows"],
            "formal_exact_rows": checkpoint["candidate_exact_rows"],
            "old_lake_removed": True,
            "transaction_backups_removed": True,
            "report_path": str(report_path),
        })
        report_path.parent.mkdir(parents=True, exist_ok=True)
        write_plain_fsync_json(report_path, result)
        lock_path.unlink(missing_ok=True)
        shutil.rmtree(transaction_root)
        return result
    except BaseException as error:
        rollback_errors: list[str] = []
        checkpoint = (
            read_fsync_json(checkpoint_path)
            if checkpoint_path.exists()
            else None
        )
        if checkpoint is not None and checkpoint.get("stage") == "ready":
            candidate_manifests = checkpoint["candidate_manifests"]
            formal_manifests = checkpoint["formal_manifests_before"]
            for table_name in reversed(AFFECTED_TABLES):
                candidate = candidate_silver / table_name
                formal = formal_silver / table_name
                backup = post_c08_backup_path(formal_silver, table_name, run_id)
                try:
                    state = post_c08_table_state(
                        candidate,
                        formal,
                        backup,
                        candidate_manifests[table_name],
                        formal_manifests[table_name],
                        False,
                    )
                    if state == "swapped":
                        rename_directory_with_retry(
                            formal,
                            candidate,
                            event_path,
                            "rollback_formal_to_candidate",
                            table_name,
                            lambda path=formal, manifest=candidate_manifests[table_name]: assert_identity_manifest(
                                path,
                                manifest,
                                f"回滚候选 {table_name}",
                            ),
                        )
                        state = "backed_up"
                    if state == "backed_up":
                        rename_directory_with_retry(
                            backup,
                            formal,
                            event_path,
                            "rollback_backup_to_formal",
                            table_name,
                            lambda path=backup, manifest=formal_manifests[table_name]: assert_identity_manifest(
                                path,
                                manifest,
                                f"回滚正式 {table_name}",
                            ),
                        )
                except BaseException as rollback_error:
                    rollback_errors.append(
                        f"root {table_name}: {type(rollback_error).__name__}: {rollback_error}"
                    )
            try:
                rollback_old_minute_moves(
                    old_silver / "fact_futures_minute",
                    candidate_silver / "fact_futures_minute",
                    formal_silver / "fact_futures_minute",
                    transaction_root / "minute_footer_journal.bin",
                )
            except BaseException as rollback_error:
                rollback_errors.append(
                    f"minute: {type(rollback_error).__name__}: {rollback_error}"
                )
            if not rollback_errors:
                write_fsync_json(
                    checkpoint_path,
                    {**checkpoint, "stage": "rolled_back"},
                )
        elif checkpoint is None:
            try:
                rollback_old_minute_moves(
                    old_silver / "fact_futures_minute",
                    candidate_silver / "fact_futures_minute",
                    formal_silver / "fact_futures_minute",
                    transaction_root / "minute_footer_journal.bin",
                )
            except BaseException as rollback_error:
                rollback_errors.append(
                    f"minute: {type(rollback_error).__name__}: {rollback_error}"
                )
        failure = {
            **result,
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "error": f"{type(error).__name__}: {error}",
            "traceback": traceback.format_exc(),
            "rollback_errors": rollback_errors,
            "transaction_root_retained": str(transaction_root),
        }
        report_path.parent.mkdir(parents=True, exist_ok=True)
        write_plain_fsync_json(report_path, failure)
        lock_path.unlink(missing_ok=True)
        if rollback_errors:
            raise RuntimeError(
                f"post-c08 恢复失败且回滚不完整：{rollback_errors}"
            ) from error
        raise


def resume_transaction(
    transaction_root: pathlib.Path,
    old_silver: pathlib.Path,
    formal_lake_root: pathlib.Path,
) -> dict[str, object]:
    """从 minute merge 已完成且正式 root 尚未交换的安全检查点继续。"""
    transaction_root = transaction_root.resolve()
    expected_parent = formal_lake_root.resolve().parent
    if transaction_root.parent != expected_parent:
        raise ValueError("恢复事务不在正式湖同级目录。")
    if not transaction_root.name.startswith(".legacy-migration-"):
        raise ValueError("恢复事务目录名不符合约定。")
    run_id = transaction_root.name.removeprefix(".legacy-migration-")
    candidate_lake = transaction_root / "candidate_lake"
    candidate_silver = candidate_lake / "silver"
    formal_silver = formal_lake_root / "silver"
    event_path = transaction_root / "events.jsonl"
    journal_path = transaction_root / "minute_footer_journal.bin"
    report_path = (
        PROJECT_ROOT
        / "00_draft_collection_02"
        / "migration_reports"
        / f"legacy-futures-lake-{run_id}.json"
    )

    existing_events = [
        json.loads(line)
        for line in event_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    existing_actions = [event.get("action") for event in existing_events]
    if "c08_completed" in existing_actions:
        return resume_post_c08_transaction(
            transaction_root,
            old_silver,
            formal_lake_root,
        )
    if "c07_completed" in existing_actions:
        checkpoint = prove_c07_retained_after_rollback(
            transaction_root,
            old_silver,
            formal_lake_root,
        )
        resume_from = "minute_reapply_then_c08"
    else:
        checkpoint = prove_ready_for_c07(
            transaction_root,
            old_silver,
            formal_lake_root,
        )
        resume_from = "c07"

    result: dict[str, object] = {
        "run_id": run_id,
        "resumed_at": datetime.now(timezone.utc).isoformat(),
        "resume_checkpoint": checkpoint,
        "resume_from": resume_from,
    }
    backups: dict[str, pathlib.Path] = {}
    swapped: list[str] = []
    lock_path = transaction_root / "resume.pid"
    try:
        lock_fd = os.open(lock_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
    except FileExistsError as error:
        raise RuntimeError(f"恢复 PID 锁已存在，拒绝并发继续：{lock_path}") from error
    with os.fdopen(lock_fd, "w", encoding="utf-8") as lock_handle:
        lock_handle.write(json.dumps({
            "pid": os.getpid(),
            "run_id": run_id,
            "started_at": datetime.now(timezone.utc).isoformat(),
        }, ensure_ascii=False))
        lock_handle.flush()
        os.fsync(lock_handle.fileno())
    append_event(event_path, {
        "action": "transaction_resumed_phase_aware",
        "run_id": run_id,
        "resume_from": resume_from,
        "pid": os.getpid(),
    })

    try:
        if resume_from == "c07":
            run_current_entry(
                "b07_suspected_session_reconciliation.py",
                candidate_lake,
                ["--write"],
                transaction_root / "c07.log",
            )
            append_event(event_path, {"action": "c07_completed"})
        else:
            result["minute_reapply"] = reapply_minute_after_c08_rollback(
                transaction_root,
                old_silver,
            )
        run_current_entry(
            "b08_full_minute_quality.py",
            candidate_lake,
            ["--confirm-full-quality", "--write"],
            transaction_root / "c08.log",
        )
        append_event(event_path, {"action": "c08_completed"})

        exact_rows = {}
        for table_name in AFFECTED_TABLES:
            exact_rows[table_name] = validate_exact_dataset(
                candidate_silver / table_name,
                TABLE_SCHEMAS[table_name],
            )
        result["candidate_exact_rows"] = exact_rows
        identity_before = {
            table_name: file_identity_manifest(candidate_silver / table_name)
            for table_name in AFFECTED_TABLES
        }
        append_event(event_path, {"action": "candidate_fully_validated"})

        for table_name in AFFECTED_TABLES:
            formal_table = formal_silver / table_name
            candidate_table = candidate_silver / table_name
            backup = formal_silver / f".{table_name}.pre-migration-{run_id}"
            if formal_table.exists():
                formal_table.rename(backup)
                backups[table_name] = backup
                append_event(event_path, {"action": "formal_to_backup", "table": table_name})
            try:
                candidate_table.rename(formal_table)
            except BaseException:
                if backup.exists() and not formal_table.exists():
                    backup.rename(formal_table)
                    backups.pop(table_name, None)
                raise
            swapped.append(table_name)
            append_event(event_path, {"action": "candidate_to_formal", "table": table_name})

        formal_rows = {}
        for table_name in AFFECTED_TABLES:
            formal_rows[table_name] = validate_exact_dataset(
                formal_silver / table_name,
                TABLE_SCHEMAS[table_name],
            )
            if formal_rows[table_name] != exact_rows[table_name]:
                raise ValueError(f"正式 root swap 后行数漂移：{table_name}")
            if file_identity_manifest(formal_silver / table_name) != identity_before[table_name]:
                raise ValueError(f"正式 root swap 后文件身份漂移：{table_name}")
        result["formal_exact_rows"] = formal_rows
        append_event(event_path, {"action": "formal_reread_completed"})

        # 成功后先清理可回滚副本，再删除用户明确授权的整个旧湖根。
        for backup in backups.values():
            shutil.rmtree(backup)
        backups.clear()
        shutil.rmtree(old_silver)
        append_event(event_path, {"action": "old_lake_and_backups_removed"})
        result.update({
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "old_lake_removed": True,
            "transaction_backups_removed": True,
            "report_path": str(report_path),
        })
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(result, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        shutil.rmtree(transaction_root)
        return result
    except BaseException as error:
        rollback_errors = []
        for table_name in reversed(swapped):
            formal_table = formal_silver / table_name
            candidate_table = candidate_silver / table_name
            backup = backups.get(table_name)
            try:
                if formal_table.exists():
                    candidate_table.parent.mkdir(parents=True, exist_ok=True)
                    formal_table.rename(candidate_table)
                if backup is not None and backup.exists():
                    backup.rename(formal_table)
            except BaseException as rollback_error:
                rollback_errors.append(
                    f"root {table_name}: {type(rollback_error).__name__}: {rollback_error}"
                )
        try:
            rollback_old_minute_moves(
                old_silver / "fact_futures_minute",
                candidate_silver / "fact_futures_minute",
                formal_silver / "fact_futures_minute",
                journal_path,
            )
        except BaseException as rollback_error:
            rollback_errors.append(
                f"minute: {type(rollback_error).__name__}: {rollback_error}"
            )
        failure = {
            **result,
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "error": f"{type(error).__name__}: {error}",
            "traceback": traceback.format_exc(),
            "rollback_errors": rollback_errors,
            "transaction_root_retained": str(transaction_root),
        }
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(failure, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        if rollback_errors:
            lock_path.unlink(missing_ok=True)
            raise RuntimeError(
                "恢复迁移失败且回滚不完整；现场已保留："
                f"{transaction_root}; errors={rollback_errors}"
            ) from error
        lock_path.unlink(missing_ok=True)
        raise


def main() -> None:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan", action="store_true")
    mode.add_argument("--execute", action="store_true")
    mode.add_argument("--resume-transaction", type=pathlib.Path)
    mode.add_argument(
        "--_prepare-post-c08",
        type=pathlib.Path,
        help=argparse.SUPPRESS,
    )
    mode.add_argument(
        "--_verify-post-c08-formal",
        type=pathlib.Path,
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--old-silver", type=pathlib.Path, default=DEFAULT_OLD_SILVER)
    parser.add_argument("--lake-root", type=pathlib.Path)
    args = parser.parse_args()

    formal_lake_root = (args.lake_root or settings.futures_lake_root).resolve()
    old_silver = args.old_silver.resolve()
    if formal_lake_root != settings.futures_lake_root.resolve():
        raise ValueError("真实迁移只接受 .env/settings 指向的正式湖根。")

    if args._prepare_post_c08 is not None:
        output = prepare_post_c08_swap(
            args._prepare_post_c08.resolve(),
            old_silver,
            formal_lake_root,
        )
    elif args._verify_post_c08_formal is not None:
        output = verify_post_c08_formal(
            args._verify_post_c08_formal.resolve(),
            formal_lake_root,
        )
    elif args.plan:
        output = plan(old_silver, formal_lake_root)
    elif args.resume_transaction is not None:
        output = resume_transaction(
            args.resume_transaction,
            old_silver,
            formal_lake_root,
        )
    else:
        output = execute(old_silver, formal_lake_root)
    print(json.dumps(output, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
