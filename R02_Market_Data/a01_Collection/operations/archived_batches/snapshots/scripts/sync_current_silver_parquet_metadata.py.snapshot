"""一次性同步六张 B01 silver 表的 Parquet footer metadata。

默认只读输出计划；``--write`` 在独立事务目录构造、验证完整 candidate 后，
协调交换六张正式表；``--recover`` 只恢复未完成事务，不调用 API，也不继续业务阶段。
"""

from __future__ import annotations

import argparse
import base64
import dataclasses
import hashlib
import json
import os
import pathlib
import shutil
import struct
import sys
import tempfile
import time
import uuid
from datetime import date, datetime, time as datetime_time, timezone
from decimal import Decimal
from typing import Any, Callable, Iterable

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq


project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        PROJECT_ROOT = candidate_root
        break
else:
    raise RuntimeError("未找到项目根目录")


from config.data_contracts import (  # noqa: E402
    FUTURES_BAR_CALENDAR_SCHEMA,
    FUTURES_CONTRACT_CALENDAR_SCHEMA,
    FUTURES_DAILY_SCHEMA,
    FUTURES_MINUTE_SCHEMA,
    FUTURES_VARIETY_CALENDAR_SCHEMA,
    TRADE_CALENDAR_SCHEMA,
)
from config.settings import settings  # noqa: E402


TOOL_VERSION = 1
ALLOWED_TRANSACTION_ROOT = (
    PROJECT_ROOT / "00_draft_collection_02" / "run_status"
)
STATUS_REPLACE_ATTEMPTS = 50
STATUS_REPLACE_RETRY_SECONDS = 0.1
COPY_BUFFER_SIZE = 8 * 1024 * 1024
WINDOWS_LEGACY_PATH_LIMIT = 260

AUTHORITATIVE_SCHEMAS = (
    TRADE_CALENDAR_SCHEMA,
    FUTURES_VARIETY_CALENDAR_SCHEMA,
    FUTURES_CONTRACT_CALENDAR_SCHEMA,
    FUTURES_BAR_CALENDAR_SCHEMA,
    FUTURES_DAILY_SCHEMA,
    FUTURES_MINUTE_SCHEMA,
)

# 这是用户复核过的本次事务基线，不是第二份业务契约。
EXPECTED_BASELINE = {
    "dim_trade_calendar": {"total": 18, "exact": 1, "metadata_drift": 17},
    "dim_futures_variety_calendar": {
        "total": 945,
        "exact": 0,
        "metadata_drift": 945,
    },
    "dim_futures_contract_calendar": {
        "total": 945,
        "exact": 0,
        "metadata_drift": 945,
    },
    "dim_futures_bar_calendar": {
        "total": 1889,
        "exact": 72,
        "metadata_drift": 1817,
    },
    "fact_futures_daily": {
        "total": 10214,
        "exact": 0,
        "metadata_drift": 10214,
    },
    "fact_futures_minute": {
        "total": 6565,
        "exact": 125,
        "metadata_drift": 6440,
    },
}


class PhysicalSchemaError(TypeError):
    """Parquet 物理字段、顺序、类型或 nullable 与权威契约不一致。"""


class BaselineChangedError(RuntimeError):
    """正式湖不再符合本次已复核的固定基线。"""


@dataclasses.dataclass(frozen=True)
class ParquetInspection:
    path: pathlib.Path
    status: str
    size: int
    mtime_ns: int
    footer_start: int
    footer_sha256: str
    row_group_metadata_sha256: str
    num_rows: int
    num_columns: int
    num_row_groups: int
    format_version: str


@dataclasses.dataclass(frozen=True)
class CandidateParquetResult:
    changed: bool
    source: ParquetInspection
    candidate: ParquetInspection
    source_data_sha256: str
    candidate_data_sha256: str


@dataclasses.dataclass(frozen=True)
class TableTransaction:
    table_name: str
    formal_path: pathlib.Path
    candidate_path: pathlib.Path
    backup_path: pathlib.Path
    restore_path: pathlib.Path | None = None

    @property
    def rollback_path(self) -> pathlib.Path:
        if self.restore_path is not None:
            return self.restore_path
        return self.backup_path.parent / f".{self.table_name}.rollback"


def utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat()


def schema_metadata_text(schema: pa.Schema, key: bytes) -> str:
    value = schema.metadata.get(key) if schema.metadata else None
    if not value:
        raise ValueError(f"Schema 缺少 metadata：{key!r}")
    return value.decode("utf-8")


def schema_table_name(schema: pa.Schema) -> str:
    return schema_metadata_text(schema, b"table_name")


def schema_partition_columns(schema: pa.Schema) -> list[str]:
    return schema_metadata_text(schema, b"partition_columns").split(",")


SCHEMAS_BY_TABLE = {
    schema_table_name(schema): schema for schema in AUTHORITATIVE_SCHEMAS
}
if tuple(SCHEMAS_BY_TABLE) != tuple(EXPECTED_BASELINE):
    raise RuntimeError("六张权威 Schema 的固定顺序或表名已经变化。")


def expected_file_schema(
    schema: pa.Schema,
    partition_columns: Iterable[str] | None = None,
) -> pa.Schema:
    """返回不含 Hive 目录字段、但保留完整权威 metadata 的文件 Schema。"""

    columns = set(
        schema_partition_columns(schema)
        if partition_columns is None
        else partition_columns
    )
    missing = columns.difference(schema.names)
    if missing:
        raise ValueError(f"分区字段不在 Schema 中：{sorted(missing)}")
    return pa.schema(
        [field for field in schema if field.name not in columns],
        metadata=schema.metadata,
    )


def authoritative_hive_partitioning(schema: pa.Schema) -> ds.Partitioning:
    return ds.partitioning(
        pa.schema([
            schema.field(name) for name in schema_partition_columns(schema)
        ]),
        flavor="hive",
    )


def _json_default(value: object) -> object:
    if isinstance(value, bytes):
        return {"__bytes_hex__": value.hex()}
    if isinstance(value, (date, datetime, datetime_time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "tolist"):
        return value.tolist()
    return repr(value)


def _canonical_sha256(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _row_group_metadata_sha256(metadata: pq.FileMetaData) -> str:
    description = metadata.to_dict()
    return _canonical_sha256({
        "num_columns": description["num_columns"],
        "num_rows": description["num_rows"],
        "num_row_groups": description["num_row_groups"],
        "format_version": description["format_version"],
        "row_groups": description["row_groups"],
    })


def parquet_footer(path: pathlib.Path) -> tuple[int, bytes]:
    size = path.stat().st_size
    if size < 12:
        raise ValueError(f"Parquet 文件过短：{path}")
    with path.open("rb") as handle:
        if handle.read(4) != b"PAR1":
            raise ValueError(f"Parquet 首部 magic 损坏：{path}")
        handle.seek(-8, os.SEEK_END)
        trailer = handle.read(8)
        if len(trailer) != 8 or trailer[4:] != b"PAR1":
            raise ValueError(f"Parquet 尾部 magic 或 trailer 损坏：{path}")
        footer_length = struct.unpack("<I", trailer[:4])[0]
        footer_start = size - 8 - footer_length
        if footer_start < 4 or footer_start >= size - 8:
            raise ValueError(f"Parquet footer 边界非法：{path}")
        handle.seek(footer_start)
        footer = handle.read(size - footer_start)
    if len(footer) != size - footer_start:
        raise ValueError(f"Parquet footer 被截断：{path}")
    return footer_start, footer


_EXPECTED_PARQUET_METADATA_CACHE: dict[bytes, dict[bytes, bytes]] = {}


def expected_parquet_key_value_metadata(
    expected_schema: pa.Schema,
) -> dict[bytes, bytes]:
    serialized_schema = expected_schema.serialize().to_pybytes()
    cached = _EXPECTED_PARQUET_METADATA_CACHE.get(serialized_schema)
    if cached is not None:
        return cached

    sink = pa.BufferOutputStream()
    pq.write_metadata(expected_schema, sink)
    metadata_file = sink.getvalue()
    generated = dict(
        pq.ParquetFile(pa.BufferReader(metadata_file)).metadata.metadata or {}
    )
    if b"ARROW:schema" not in generated:
        raise RuntimeError("PyArrow 未生成 ARROW:schema metadata。")
    _EXPECTED_PARQUET_METADATA_CACHE[serialized_schema] = generated
    return generated


def inspect_parquet_file(
    path: pathlib.Path,
    expected_schema: pa.Schema,
) -> ParquetInspection:
    """区分 exact 与纯 metadata 漂移，并拒绝所有物理不兼容/损坏。"""

    path = pathlib.Path(path)
    footer_start, footer = parquet_footer(path)
    try:
        parquet = pq.ParquetFile(path)
        actual_schema = pq.read_schema(path)
    except Exception as error:
        raise ValueError(f"PyArrow 无法打开 Parquet：{path}: {error}") from error

    if not actual_schema.equals(expected_schema, check_metadata=False):
        raise PhysicalSchemaError(
            "Parquet 字段名、顺序、类型或 nullable 与权威文件 Schema 不一致："
            f"{path}\nactual={actual_schema}\nexpected={expected_schema}"
        )

    actual_key_values = dict(parquet.metadata.metadata or {})
    expected_key_values = expected_parquet_key_value_metadata(expected_schema)
    is_exact = (
        actual_schema.equals(expected_schema, check_metadata=True)
        and actual_key_values == expected_key_values
    )
    stat = path.stat()
    metadata = parquet.metadata
    return ParquetInspection(
        path=path,
        status="exact" if is_exact else "metadata_drift",
        size=stat.st_size,
        mtime_ns=stat.st_mtime_ns,
        footer_start=footer_start,
        footer_sha256=hashlib.sha256(footer).hexdigest(),
        row_group_metadata_sha256=_row_group_metadata_sha256(metadata),
        num_rows=metadata.num_rows,
        num_columns=metadata.num_columns,
        num_row_groups=metadata.num_row_groups,
        format_version=metadata.format_version,
    )


def _assert_row_groups_unchanged(
    source_path: pathlib.Path,
    candidate_path: pathlib.Path,
) -> None:
    source = pq.ParquetFile(source_path)
    candidate = pq.ParquetFile(candidate_path)
    source_metadata = source.metadata
    candidate_metadata = candidate.metadata

    file_attributes = (
        "num_rows",
        "num_columns",
        "num_row_groups",
        "format_version",
    )
    for attribute in file_attributes:
        if getattr(source_metadata, attribute) != getattr(
            candidate_metadata, attribute
        ):
            raise ValueError(
                f"footer 同步改变了文件属性 {attribute}：{candidate_path}"
            )
    if not source.schema.equals(candidate.schema):
        raise ValueError(f"footer 同步改变了 Parquet 物理 Schema：{candidate_path}")

    for row_group_index in range(source_metadata.num_row_groups):
        source_row_group = source_metadata.row_group(row_group_index)
        candidate_row_group = candidate_metadata.row_group(row_group_index)
        if not source_row_group.equals(candidate_row_group):
            for column_index in range(source_row_group.num_columns):
                if not source_row_group.column(column_index).equals(
                    candidate_row_group.column(column_index)
                ):
                    raise ValueError(
                        "footer 同步改变了列块 metadata："
                        f"{candidate_path}; row_group={row_group_index}; "
                        f"column={column_index}"
                    )
            raise ValueError(
                "footer 同步改变了 row group metadata："
                f"{candidate_path}; row_group={row_group_index}"
            )


def build_replacement_footer(
    path: pathlib.Path,
    expected_schema: pa.Schema,
) -> bytes:
    """用权威 Arrow Schema 与原 row groups 生成 footer+trailer。"""

    original_metadata = pq.ParquetFile(path).metadata
    with tempfile.TemporaryDirectory(prefix="latitude-footer-") as directory:
        seed_path = pathlib.Path(directory) / "seed.parquet"
        rebuilt_path = pathlib.Path(directory) / "rebuilt.parquet"
        pq.write_metadata(
            expected_schema,
            seed_path,
            version=original_metadata.format_version,
        )
        rebuilt_metadata = pq.ParquetFile(seed_path).metadata
        rebuilt_metadata.append_row_groups(original_metadata)
        rebuilt_metadata.write_metadata_file(rebuilt_path)
        replacement_container = rebuilt_path.read_bytes()

    if (
        replacement_container[:4] != b"PAR1"
        or replacement_container[-4:] != b"PAR1"
    ):
        raise ValueError("重建的 Parquet footer 容器不完整。")
    return replacement_container[4:]


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(COPY_BUFFER_SIZE):
            digest.update(block)
    return digest.hexdigest()


def sha256_prefix(path: pathlib.Path, length: int) -> str:
    digest = hashlib.sha256()
    remaining = length
    with path.open("rb") as handle:
        while remaining:
            block = handle.read(min(COPY_BUFFER_SIZE, remaining))
            if not block:
                raise ValueError(f"文件数据区提前结束：{path}")
            digest.update(block)
            remaining -= len(block)
    return digest.hexdigest()


def write_candidate_parquet(
    source_path: pathlib.Path,
    candidate_path: pathlib.Path,
    expected_schema: pa.Schema,
) -> CandidateParquetResult:
    """写一个候选文件；源文件始终只读，exact 文件逐字节复制。"""

    source_path = pathlib.Path(source_path)
    candidate_path = pathlib.Path(candidate_path)
    if candidate_path.exists():
        raise FileExistsError(f"候选文件已经存在：{candidate_path}")
    candidate_path.parent.mkdir(parents=True, exist_ok=True)

    before_stat = source_path.stat()
    source_inspection = inspect_parquet_file(source_path, expected_schema)
    if source_inspection.status == "exact":
        shutil.copy2(source_path, candidate_path)
        source_data_sha256 = sha256_prefix(
            source_path, source_inspection.footer_start
        )
    else:
        replacement_footer = build_replacement_footer(
            source_path, expected_schema
        )
        source_digest = hashlib.sha256()
        remaining = source_inspection.footer_start
        with source_path.open("rb") as source_handle, candidate_path.open(
            "xb"
        ) as candidate_handle:
            while remaining:
                block = source_handle.read(min(COPY_BUFFER_SIZE, remaining))
                if not block:
                    raise ValueError(f"源文件数据区提前结束：{source_path}")
                source_digest.update(block)
                candidate_handle.write(block)
                remaining -= len(block)
            candidate_handle.write(replacement_footer)
            candidate_handle.flush()
            os.fsync(candidate_handle.fileno())
        shutil.copystat(source_path, candidate_path)
        source_data_sha256 = source_digest.hexdigest()

    after_stat = source_path.stat()
    if (
        before_stat.st_size != after_stat.st_size
        or before_stat.st_mtime_ns != after_stat.st_mtime_ns
    ):
        raise RuntimeError(f"构造 candidate 时源文件发生变化：{source_path}")
    source_after = inspect_parquet_file(source_path, expected_schema)
    if source_after != source_inspection:
        raise RuntimeError(f"构造 candidate 时源 footer 发生变化：{source_path}")

    candidate_inspection = inspect_parquet_file(candidate_path, expected_schema)
    if candidate_inspection.status != "exact":
        raise TypeError(f"候选文件 metadata 仍不精确：{candidate_path}")
    if candidate_inspection.footer_start != source_inspection.footer_start:
        raise ValueError(f"候选文件数据区边界发生变化：{candidate_path}")

    candidate_data_sha256 = sha256_prefix(
        candidate_path, candidate_inspection.footer_start
    )
    if candidate_data_sha256 != source_data_sha256:
        raise ValueError(f"候选文件数据区 SHA-256 发生变化：{candidate_path}")
    _assert_row_groups_unchanged(source_path, candidate_path)

    if source_inspection.status == "exact":
        if sha256_file(source_path) != sha256_file(candidate_path):
            raise ValueError(f"exact 文件复制后字节不一致：{candidate_path}")

    return CandidateParquetResult(
        changed=source_inspection.status == "metadata_drift",
        source=source_inspection,
        candidate=candidate_inspection,
        source_data_sha256=source_data_sha256,
        candidate_data_sha256=candidate_data_sha256,
    )


def atomic_write_json(path: pathlib.Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary_path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.flush()
        os.fsync(handle.fileno())
    for attempt in range(1, STATUS_REPLACE_ATTEMPTS + 1):
        try:
            os.replace(temporary_path, path)
            return
        except PermissionError as error:
            if (
                getattr(error, "winerror", None) not in {5, 32}
                or attempt == STATUS_REPLACE_ATTEMPTS
            ):
                raise
            time.sleep(STATUS_REPLACE_RETRY_SECONDS)


def append_event(path: pathlib.Path, event: str, **details: object) -> None:
    payload = {"at": utc_now_text(), "event": event, **details}
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _record_from_inspection(
    relative_path: str,
    inspection: ParquetInspection,
) -> dict[str, object]:
    return {
        "relative_path": relative_path,
        "kind": "parquet",
        "size": inspection.size,
        "mtime_ns": inspection.mtime_ns,
        "status": inspection.status,
        "footer_start": inspection.footer_start,
        "footer_sha256": inspection.footer_sha256,
        "row_group_metadata_sha256": inspection.row_group_metadata_sha256,
        "num_rows": inspection.num_rows,
        "num_columns": inspection.num_columns,
        "num_row_groups": inspection.num_row_groups,
        "format_version": inspection.format_version,
    }


def scan_table(
    table_root: pathlib.Path,
    schema: pa.Schema,
) -> dict[str, object]:
    if not table_root.is_dir():
        raise FileNotFoundError(f"正式表根目录不存在：{table_root}")

    file_schema = expected_file_schema(schema)
    directories: list[str] = []
    records: list[dict[str, object]] = []
    counts = {
        "total": 0,
        "exact": 0,
        "metadata_drift": 0,
        "physical_bad": 0,
        "non_parquet": 0,
        "rows": 0,
        "bytes": 0,
    }

    for path in sorted(table_root.rglob("*")):
        if path.is_symlink():
            raise ValueError(f"表根目录内禁止符号链接：{path}")
        relative_path = path.relative_to(table_root).as_posix()
        if path.is_dir():
            directories.append(relative_path)
            continue
        if not path.is_file():
            raise ValueError(f"表根目录内存在未知对象：{path}")

        stat = path.stat()
        counts["bytes"] += stat.st_size
        if path.suffix.lower() != ".parquet":
            counts["non_parquet"] += 1
            records.append({
                "relative_path": relative_path,
                "kind": "non_parquet",
                "size": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
                "sha256": sha256_file(path),
            })
            continue

        counts["total"] += 1
        try:
            inspection = inspect_parquet_file(path, file_schema)
        except Exception as error:
            counts["physical_bad"] += 1
            records.append({
                "relative_path": relative_path,
                "kind": "parquet",
                "size": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
                "status": "physical_bad",
                "error": f"{type(error).__name__}: {error}",
            })
            continue

        counts[inspection.status] += 1
        counts["rows"] += inspection.num_rows
        records.append(_record_from_inspection(relative_path, inspection))

    return {
        "table_name": schema_table_name(schema),
        "formal_path": str(table_root.resolve()),
        "schema_serialized_base64": base64.b64encode(
            schema.serialize().to_pybytes()
        ).decode("ascii"),
        "schema_sha256": hashlib.sha256(
            schema.serialize().to_pybytes()
        ).hexdigest(),
        "directories": directories,
        "files": records,
        "counts": counts,
    }


def scan_formal_tables() -> dict[str, object]:
    silver_root = (settings.futures_lake_root / "silver").resolve()
    tables = {}
    for table_index, (table_name, schema) in enumerate(
        SCHEMAS_BY_TABLE.items(), start=1
    ):
        tables[table_name] = scan_table(silver_root / table_name, schema)
        counts = tables[table_name]["counts"]
        print(
            "metadata_progress: "
            f"phase=preflight_scan; tables={table_index}/6; "
            f"table={table_name}; files={counts['total']}",
            flush=True,
        )
    return {
        "tool_version": TOOL_VERSION,
        "created_at": utc_now_text(),
        "project_root": str(PROJECT_ROOT),
        "lake_root": str(settings.futures_lake_root),
        "silver_root": str(silver_root),
        "tables": tables,
    }


def print_plan(manifest: dict[str, object]) -> None:
    total = exact = drift = physical_bad = rows = byte_count = 0
    for table_name in SCHEMAS_BY_TABLE:
        counts = manifest["tables"][table_name]["counts"]
        print(
            "metadata_plan: "
            f"table={table_name}; total={counts['total']}; "
            f"exact={counts['exact']}; drift={counts['metadata_drift']}; "
            f"physical_bad={counts['physical_bad']}; rows={counts['rows']}; "
            f"bytes={counts['bytes']}",
            flush=True,
        )
        total += counts["total"]
        exact += counts["exact"]
        drift += counts["metadata_drift"]
        physical_bad += counts["physical_bad"]
        rows += counts["rows"]
        byte_count += counts["bytes"]
    print(
        "metadata_plan: "
        f"tables=6; total={total}; exact={exact}; drift={drift}; "
        f"physical_bad={physical_bad}; rows={rows}; bytes={byte_count}",
        flush=True,
    )


def assert_expected_baseline(manifest: dict[str, object]) -> None:
    differences: list[str] = []
    for table_name, expected in EXPECTED_BASELINE.items():
        counts = manifest["tables"][table_name]["counts"]
        actual = {
            "total": counts["total"],
            "exact": counts["exact"],
            "metadata_drift": counts["metadata_drift"],
        }
        if actual != expected:
            differences.append(
                f"{table_name}: expected={expected}, actual={actual}"
            )
        if counts["physical_bad"] != 0:
            differences.append(
                f"{table_name}: physical_bad={counts['physical_bad']}"
            )
    if differences:
        raise BaselineChangedError(
            "正式湖不再符合已复核基线，未执行任何写入：\n"
            + "\n".join(differences)
        )


def assert_transaction_path_lengths(
    transaction_root: pathlib.Path,
    manifest: dict[str, object],
) -> None:
    """在 Windows 实际写候选前拒绝会越过传统 Win32 边界的路径。"""

    if os.name != "nt":
        return
    transaction_root = transaction_root.resolve()
    longest_path: pathlib.Path | None = None
    longest_length = 0
    for generated_directory in ("candidate", "backup", "rollback"):
        for table_name in SCHEMAS_BY_TABLE:
            table_manifest = manifest["tables"][table_name]
            for record in table_manifest["files"]:
                generated_path = (
                    transaction_root
                    / generated_directory
                    / table_name
                    / pathlib.Path(record["relative_path"])
                )
                path_length = len(str(generated_path))
                if path_length > longest_length:
                    longest_path = generated_path
                    longest_length = path_length
    if longest_length >= WINDOWS_LEGACY_PATH_LIMIT:
        raise OSError(
            "事务路径过长，当前 Windows 文件 API 无法安全构造 candidate："
            f"length={longest_length}, limit<{WINDOWS_LEGACY_PATH_LIMIT}, "
            f"path={longest_path}。请使用更短的 run root。"
        )


def _manifest_records_by_path(
    table_manifest: dict[str, object],
) -> dict[str, dict[str, object]]:
    return {
        record["relative_path"]: record
        for record in table_manifest["files"]
    }


def assert_root_matches_frozen_manifest(
    root: pathlib.Path,
    table_manifest: dict[str, object],
    schema: pa.Schema,
) -> None:
    if not root.is_dir():
        raise FileNotFoundError(f"表根目录不存在：{root}")
    actual_directories = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_dir()
    }
    expected_directories = set(table_manifest["directories"])
    if actual_directories != expected_directories:
        raise RuntimeError(f"目录集合在冻结后变化：{root}")

    expected_records = _manifest_records_by_path(table_manifest)
    actual_files = {
        path.relative_to(root).as_posix(): path
        for path in root.rglob("*")
        if path.is_file()
    }
    if set(actual_files) != set(expected_records):
        raise RuntimeError(f"文件集合在冻结后变化：{root}")

    file_schema = expected_file_schema(schema)
    for relative_path, record in expected_records.items():
        path = actual_files[relative_path]
        stat = path.stat()
        if (
            stat.st_size != record["size"]
            or stat.st_mtime_ns != record["mtime_ns"]
        ):
            raise RuntimeError(f"文件大小或 mtime 在冻结后变化：{path}")
        if record["kind"] == "non_parquet":
            if sha256_file(path) != record["sha256"]:
                raise RuntimeError(f"非 Parquet 文件在冻结后变化：{path}")
            continue
        inspection = inspect_parquet_file(path, file_schema)
        comparable = (
            inspection.status,
            inspection.footer_start,
            inspection.footer_sha256,
            inspection.row_group_metadata_sha256,
            inspection.num_rows,
            inspection.num_columns,
            inspection.num_row_groups,
            inspection.format_version,
        )
        frozen = (
            record["status"],
            record["footer_start"],
            record["footer_sha256"],
            record["row_group_metadata_sha256"],
            record["num_rows"],
            record["num_columns"],
            record["num_row_groups"],
            record["format_version"],
        )
        if comparable != frozen:
            raise RuntimeError(f"Parquet footer 在冻结后变化：{path}")


def build_all_candidates(
    manifest: dict[str, object],
    candidate_parent: pathlib.Path,
) -> dict[str, object]:
    evidence: dict[str, object] = {"tables": {}}
    total_files = sum(
        table_manifest["counts"]["total"]
        + table_manifest["counts"]["non_parquet"]
        for table_manifest in manifest["tables"].values()
    )
    completed = 0

    for table_name, schema in SCHEMAS_BY_TABLE.items():
        table_manifest = manifest["tables"][table_name]
        formal_root = pathlib.Path(table_manifest["formal_path"])
        candidate_root = candidate_parent / table_name
        if candidate_root.exists():
            raise FileExistsError(f"候选表根已经存在：{candidate_root}")
        candidate_root.mkdir(parents=True)
        for relative_directory in table_manifest["directories"]:
            (candidate_root / relative_directory).mkdir(parents=True, exist_ok=True)

        table_evidence = []
        file_schema = expected_file_schema(schema)
        for record in table_manifest["files"]:
            relative_path = pathlib.Path(record["relative_path"])
            source_path = formal_root / relative_path
            candidate_path = candidate_root / relative_path
            candidate_path.parent.mkdir(parents=True, exist_ok=True)

            if record["kind"] == "non_parquet":
                shutil.copy2(source_path, candidate_path)
                source_hash = record["sha256"]
                candidate_hash = sha256_file(candidate_path)
                if candidate_hash != source_hash:
                    raise ValueError(
                        f"非 Parquet 文件复制后不一致：{candidate_path}"
                    )
                table_evidence.append({
                    "relative_path": record["relative_path"],
                    "kind": "non_parquet",
                    "sha256": source_hash,
                })
            else:
                result = write_candidate_parquet(
                    source_path, candidate_path, file_schema
                )
                table_evidence.append({
                    "relative_path": record["relative_path"],
                    "kind": "parquet",
                    "changed": result.changed,
                    "data_sha256": result.source_data_sha256,
                    "row_group_metadata_sha256": (
                        result.source.row_group_metadata_sha256
                    ),
                    "num_rows": result.source.num_rows,
                    "num_row_groups": result.source.num_row_groups,
                })

            completed += 1
            if completed == total_files or completed % 100 == 0:
                print(
                    "metadata_progress: "
                    f"phase=build_candidate; files={completed}/{total_files}; "
                    f"table={table_name}",
                    flush=True,
                )
        evidence["tables"][table_name] = {"files": table_evidence}
    return evidence


def _validate_dataset_root(
    root: pathlib.Path,
    schema: pa.Schema,
    expected_parquet_paths: set[str],
    expected_rows: int,
) -> None:
    dataset = ds.dataset(
        root,
        format="parquet",
        partitioning=authoritative_hive_partitioning(schema),
        schema=schema,
        exclude_invalid_files=True,
    )
    if not dataset.schema.equals(schema, check_metadata=True):
        raise TypeError(f"Dataset Schema/metadata 不精确：{root}")

    fragments = list(dataset.get_fragments())
    actual_paths: set[str] = set()
    target_file_schema = expected_file_schema(schema)
    for fragment in fragments:
        fragment_path = pathlib.Path(fragment.path)
        if not fragment_path.is_absolute():
            fragment_path = root / fragment_path
        relative_path = fragment_path.resolve().relative_to(
            root.resolve()
        ).as_posix()
        actual_paths.add(relative_path)
        if not fragment.physical_schema.equals(
            target_file_schema, check_metadata=True
        ):
            raise TypeError(f"Fragment Schema/metadata 不精确：{fragment_path}")
    if actual_paths != expected_parquet_paths:
        raise RuntimeError(f"Dataset fragment 集合不一致：{root}")
    if dataset.count_rows() != expected_rows:
        raise RuntimeError(f"Dataset 总行数发生变化：{root}")


def validate_candidate_table(
    source_root: pathlib.Path,
    candidate_root: pathlib.Path,
    table_manifest: dict[str, object],
    schema: pa.Schema,
) -> dict[str, object]:
    expected_records = _manifest_records_by_path(table_manifest)
    actual_files = {
        path.relative_to(candidate_root).as_posix(): path
        for path in candidate_root.rglob("*")
        if path.is_file()
    }
    actual_directories = {
        path.relative_to(candidate_root).as_posix()
        for path in candidate_root.rglob("*")
        if path.is_dir()
    }
    if set(actual_files) != set(expected_records):
        raise RuntimeError(f"candidate 文件集合不一致：{candidate_root}")
    if actual_directories != set(table_manifest["directories"]):
        raise RuntimeError(f"candidate 目录集合不一致：{candidate_root}")

    target_file_schema = expected_file_schema(schema)
    validated = 0
    changed = 0
    for relative_path, record in expected_records.items():
        source_path = source_root / relative_path
        candidate_path = actual_files[relative_path]
        if record["kind"] == "non_parquet":
            source_hash = sha256_file(source_path)
            candidate_hash = sha256_file(candidate_path)
            if source_hash != record["sha256"] or candidate_hash != source_hash:
                raise ValueError(
                    f"candidate 非 Parquet 文件不一致：{candidate_path}"
                )
            continue

        source_inspection = inspect_parquet_file(
            source_path, target_file_schema
        )
        candidate_inspection = inspect_parquet_file(
            candidate_path, target_file_schema
        )
        if candidate_inspection.status != "exact":
            raise TypeError(f"candidate metadata 不精确：{candidate_path}")
        if (
            source_inspection.footer_start
            != candidate_inspection.footer_start
            or source_inspection.row_group_metadata_sha256
            != candidate_inspection.row_group_metadata_sha256
            or source_inspection.num_rows != candidate_inspection.num_rows
            or source_inspection.num_row_groups
            != candidate_inspection.num_row_groups
        ):
            raise ValueError(
                f"candidate 数据边界或 row group metadata 改变：{candidate_path}"
            )
        _assert_row_groups_unchanged(source_path, candidate_path)
        if source_inspection.status == "exact":
            if sha256_file(source_path) != sha256_file(candidate_path):
                raise ValueError(f"exact candidate 字节不一致：{candidate_path}")
        else:
            source_hash = sha256_prefix(
                source_path, source_inspection.footer_start
            )
            candidate_hash = sha256_prefix(
                candidate_path, candidate_inspection.footer_start
            )
            if source_hash != candidate_hash:
                raise ValueError(
                    f"candidate 数据区 SHA-256 改变：{candidate_path}"
                )
            changed += 1
        validated += 1
        if validated % 100 == 0:
            print(
                "metadata_progress: "
                f"phase=validate_candidate; files={validated}/"
                f"{table_manifest['counts']['total']}; "
                f"table={table_manifest['table_name']}",
                flush=True,
            )

    parquet_paths = {
        relative_path
        for relative_path, record in expected_records.items()
        if record["kind"] == "parquet"
    }
    _validate_dataset_root(
        candidate_root,
        schema,
        parquet_paths,
        table_manifest["counts"]["rows"],
    )
    return {
        "table_name": table_manifest["table_name"],
        "validated_parquet_files": validated,
        "changed_parquet_files": changed,
        "rows": table_manifest["counts"]["rows"],
    }


def _remove_generated_tree(path: pathlib.Path, allowed_root: pathlib.Path) -> None:
    resolved_path = path.resolve()
    resolved_allowed = allowed_root.resolve()
    if not resolved_path.is_relative_to(resolved_allowed):
        raise ValueError(f"拒绝清理事务目录之外的路径：{resolved_path}")
    if resolved_path == resolved_allowed:
        raise ValueError(f"拒绝清理整个事务目录：{resolved_path}")
    if path.exists():
        shutil.rmtree(path)


def _clone_tree_for_rollback(source: pathlib.Path, target: pathlib.Path) -> None:
    if target.exists():
        raise FileExistsError(f"rollback clone 已存在：{target}")
    target.mkdir(parents=True)
    for source_path in sorted(source.rglob("*")):
        relative_path = source_path.relative_to(source)
        target_path = target / relative_path
        if source_path.is_symlink():
            raise ValueError(f"backup 内禁止符号链接：{source_path}")
        if source_path.is_dir():
            target_path.mkdir(parents=True, exist_ok=True)
            continue
        target_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.link(source_path, target_path)
        except OSError:
            shutil.copy2(source_path, target_path)


def _rollback_active_exchange(
    transactions: list[TableTransaction],
    backed_up: set[str],
    installed: set[str],
    on_event: Callable[[str, TableTransaction], None] | None,
) -> list[str]:
    errors: list[str] = []
    for transaction in reversed(transactions):
        if transaction.table_name not in backed_up:
            continue
        try:
            if transaction.table_name in installed:
                if transaction.candidate_path.exists():
                    raise RuntimeError(
                        f"回滚时 candidate 路径意外存在："
                        f"{transaction.candidate_path}"
                    )
                transaction.formal_path.replace(transaction.candidate_path)
            # clone 构造中断时目标目录可能只是部分树；回滚时始终从完整
            # backup 重新建立，不能把部分 clone 安装回正式路径。
            if transaction.rollback_path.exists():
                _remove_generated_tree(
                    transaction.rollback_path,
                    transaction.rollback_path.parent,
                )
            _clone_tree_for_rollback(
                transaction.backup_path, transaction.rollback_path
            )
            if transaction.formal_path.exists():
                raise RuntimeError(
                    f"回滚前正式路径仍存在：{transaction.formal_path}"
                )
            transaction.rollback_path.replace(transaction.formal_path)
            if on_event is not None:
                on_event("rolled_back", transaction)
        except Exception as error:
            errors.append(
                f"{transaction.table_name}: {type(error).__name__}: {error}"
            )
    return errors


def coordinated_exchange(
    transactions: Iterable[TableTransaction],
    validate_before_exchange: Callable[[TableTransaction], None] | None = None,
    validate_formal: Callable[[TableTransaction], None] | None = None,
    on_event: Callable[[str, TableTransaction], None] | None = None,
) -> None:
    """交换全部表；任一 rename/正式复读失败时逆序恢复并保留 backup。"""

    transaction_list = list(transactions)
    backed_up: set[str] = set()
    installed: set[str] = set()
    try:
        for transaction in transaction_list:
            if validate_before_exchange is not None:
                validate_before_exchange(transaction)
            if not transaction.formal_path.is_dir():
                raise FileNotFoundError(
                    f"正式表根不存在：{transaction.formal_path}"
                )
            if not transaction.candidate_path.is_dir():
                raise FileNotFoundError(
                    f"candidate 表根不存在：{transaction.candidate_path}"
                )
            if transaction.backup_path.exists():
                raise FileExistsError(
                    f"backup 表根已经存在：{transaction.backup_path}"
                )
            if transaction.rollback_path.exists():
                raise FileExistsError(
                    f"rollback clone 已经存在：{transaction.rollback_path}"
                )
            transaction.backup_path.parent.mkdir(parents=True, exist_ok=True)
            transaction.rollback_path.parent.mkdir(parents=True, exist_ok=True)

            transaction.formal_path.replace(transaction.backup_path)
            backed_up.add(transaction.table_name)
            if on_event is not None:
                on_event("formal_moved_to_backup", transaction)

            _clone_tree_for_rollback(
                transaction.backup_path, transaction.rollback_path
            )
            transaction.candidate_path.replace(transaction.formal_path)
            installed.add(transaction.table_name)
            if on_event is not None:
                on_event("candidate_installed", transaction)

        if validate_formal is not None:
            for transaction in transaction_list:
                validate_formal(transaction)
                if on_event is not None:
                    on_event("formal_table_validated", transaction)
    except BaseException as original_error:
        rollback_errors = _rollback_active_exchange(
            transaction_list, backed_up, installed, on_event
        )
        if rollback_errors:
            raise RuntimeError(
                "协调交换失败且自动回滚不完整：\n"
                + "\n".join(rollback_errors)
            ) from original_error
        raise
    else:
        for transaction in transaction_list:
            if transaction.rollback_path.exists():
                _remove_generated_tree(
                    transaction.rollback_path,
                    transaction.rollback_path.parent,
                )


def _transaction_paths(
    transaction_root: pathlib.Path,
    manifest: dict[str, object],
) -> list[TableTransaction]:
    return [
        TableTransaction(
            table_name=table_name,
            formal_path=pathlib.Path(
                manifest["tables"][table_name]["formal_path"]
            ),
            candidate_path=transaction_root / "candidate" / table_name,
            backup_path=transaction_root / "backup" / table_name,
            restore_path=transaction_root / "rollback" / table_name,
        )
        for table_name in SCHEMAS_BY_TABLE
    ]


def _validate_transaction_root(transaction_root: pathlib.Path) -> pathlib.Path:
    resolved = transaction_root.resolve()
    allowed = ALLOWED_TRANSACTION_ROOT.resolve()
    if not resolved.is_relative_to(allowed) or resolved == allowed:
        raise ValueError(f"事务目录必须位于 {allowed} 的子目录中。")
    if resolved == settings.futures_lake_root.resolve():
        raise ValueError("事务目录不得等于正式湖根目录。")
    return resolved


def execute_transaction(transaction_root: pathlib.Path) -> int:
    transaction_root = _validate_transaction_root(transaction_root)
    if transaction_root.exists():
        raise FileExistsError(
            f"事务目录已经存在，禁止覆盖或自动重试：{transaction_root}"
        )
    transaction_root.mkdir(parents=True)
    state_path = transaction_root / "state.json"
    event_path = transaction_root / "events.jsonl"
    manifest_path = transaction_root / "frozen_manifest.json"
    validation_path = transaction_root / "validation.json"
    state: dict[str, object] = {
        "tool_version": TOOL_VERSION,
        "phase": "preflight_scanning",
        "created_at": utc_now_text(),
        "updated_at": utc_now_text(),
        "error": None,
        "tables": {
            table_name: {"exchange_state": "not_started"}
            for table_name in SCHEMAS_BY_TABLE
        },
    }
    atomic_write_json(state_path, state)
    append_event(event_path, "transaction_created")

    swaps_started = False
    try:
        manifest = scan_formal_tables()
        atomic_write_json(manifest_path, manifest)
        print_plan(manifest)
        assert_expected_baseline(manifest)
        assert_transaction_path_lengths(transaction_root, manifest)
        state.update({"phase": "preflight_frozen", "updated_at": utc_now_text()})
        atomic_write_json(state_path, state)
        append_event(event_path, "preflight_frozen")

        for table_name, schema in SCHEMAS_BY_TABLE.items():
            assert_root_matches_frozen_manifest(
                pathlib.Path(manifest["tables"][table_name]["formal_path"]),
                manifest["tables"][table_name],
                schema,
            )

        build_evidence = build_all_candidates(
            manifest, transaction_root / "candidate"
        )
        state.update({"phase": "candidate_built", "updated_at": utc_now_text()})
        atomic_write_json(state_path, state)
        append_event(event_path, "candidate_built")

        validation = {
            "created_at": utc_now_text(),
            "build_evidence": build_evidence,
            "tables": {},
        }
        for table_name, schema in SCHEMAS_BY_TABLE.items():
            table_manifest = manifest["tables"][table_name]
            validation["tables"][table_name] = validate_candidate_table(
                pathlib.Path(table_manifest["formal_path"]),
                transaction_root / "candidate" / table_name,
                table_manifest,
                schema,
            )
        atomic_write_json(validation_path, validation)
        state.update({
            "phase": "candidate_validated",
            "updated_at": utc_now_text(),
        })
        atomic_write_json(state_path, state)
        append_event(event_path, "candidate_validated")

        # 全部 candidate 验证结束后，再次确认六张正式表未发生并发变化。
        for table_name, schema in SCHEMAS_BY_TABLE.items():
            assert_root_matches_frozen_manifest(
                pathlib.Path(manifest["tables"][table_name]["formal_path"]),
                manifest["tables"][table_name],
                schema,
            )
        state.update({"phase": "ready_to_swap", "updated_at": utc_now_text()})
        atomic_write_json(state_path, state)
        append_event(event_path, "ready_to_swap")

        transactions = _transaction_paths(transaction_root, manifest)

        def record_exchange_event(
            event: str, transaction: TableTransaction
        ) -> None:
            state["tables"][transaction.table_name]["exchange_state"] = event
            state["updated_at"] = utc_now_text()
            atomic_write_json(state_path, state)
            append_event(
                event_path, event, table_name=transaction.table_name
            )
            print(
                "metadata_progress: "
                f"phase={event}; table={transaction.table_name}",
                flush=True,
            )

        def validate_formal(transaction: TableTransaction) -> None:
            schema = SCHEMAS_BY_TABLE[transaction.table_name]
            table_manifest = manifest["tables"][transaction.table_name]
            validate_candidate_table(
                transaction.backup_path,
                transaction.formal_path,
                table_manifest,
                schema,
            )

        def validate_before_exchange(transaction: TableTransaction) -> None:
            schema = SCHEMAS_BY_TABLE[transaction.table_name]
            table_manifest = manifest["tables"][transaction.table_name]
            assert_root_matches_frozen_manifest(
                transaction.formal_path,
                table_manifest,
                schema,
            )

        swaps_started = True
        coordinated_exchange(
            transactions,
            validate_before_exchange=validate_before_exchange,
            validate_formal=validate_formal,
            on_event=record_exchange_event,
        )
        state.update({"phase": "formal_verified", "updated_at": utc_now_text()})
        atomic_write_json(state_path, state)
        append_event(event_path, "formal_verified")

        state.update({
            "phase": "complete",
            "completed_at": utc_now_text(),
            "updated_at": utc_now_text(),
        })
        atomic_write_json(state_path, state)
        append_event(event_path, "transaction_complete")
        print(
            "metadata_progress: phase=complete; tables=6; "
            "backup_retained=true",
            flush=True,
        )
        return 0
    except BaseException as error:
        state.update({
            "phase": "failed_after_swap" if swaps_started else "failed_before_swap",
            "updated_at": utc_now_text(),
            "error": f"{type(error).__name__}: {error}",
        })
        try:
            atomic_write_json(state_path, state)
            append_event(
                event_path,
                "transaction_failed",
                error=state["error"],
            )
        except Exception:
            pass
        raise


def _load_json(path: pathlib.Path) -> dict[str, object]:
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise TypeError(f"JSON 根对象必须是 object：{path}")
    return value


def _root_matches_manifest_without_raising(
    root: pathlib.Path,
    table_manifest: dict[str, object],
    schema: pa.Schema,
) -> bool:
    try:
        assert_root_matches_frozen_manifest(root, table_manifest, schema)
    except Exception:
        return False
    return True


def recover_transaction(transaction_root: pathlib.Path) -> int:
    """恢复未完成事务到迁移前六表；完整事务只验证、不清理 backup。"""

    transaction_root = _validate_transaction_root(transaction_root)
    state_path = transaction_root / "state.json"
    manifest_path = transaction_root / "frozen_manifest.json"
    event_path = transaction_root / "events.jsonl"
    if not state_path.is_file():
        raise FileNotFoundError(f"事务状态不存在：{state_path}")
    state = _load_json(state_path)
    phase = state.get("phase")

    if phase == "complete":
        print(
            "metadata_progress: phase=recover_noop; transaction=complete",
            flush=True,
        )
        return 0

    if phase == "formal_verified":
        manifest = _load_json(manifest_path)
        for table_name, schema in SCHEMAS_BY_TABLE.items():
            table_manifest = manifest["tables"][table_name]
            formal_root = pathlib.Path(table_manifest["formal_path"])
            backup_root = transaction_root / "backup" / table_name
            if not backup_root.is_dir():
                raise FileNotFoundError(
                    f"formal_verified 事务缺少保留 backup：{backup_root}"
                )
            expected_paths = {
                record["relative_path"]
                for record in table_manifest["files"]
                if record["kind"] == "parquet"
            }
            _validate_dataset_root(
                formal_root,
                schema,
                expected_paths,
                table_manifest["counts"]["rows"],
            )
        state.update({
            "phase": "complete",
            "completed_at": utc_now_text(),
            "recovered_at": utc_now_text(),
            "updated_at": utc_now_text(),
        })
        atomic_write_json(state_path, state)
        append_event(event_path, "recover_completed_formal_verified")
        print(
            "metadata_progress: phase=recover_complete; "
            "source_checkpoint=formal_verified",
            flush=True,
        )
        return 0

    if not manifest_path.is_file():
        # 预检尚未冻结时不可能发生正式目录交换。
        state.update({
            "phase": "recovered_no_formal_changes",
            "recovered_at": utc_now_text(),
            "updated_at": utc_now_text(),
        })
        atomic_write_json(state_path, state)
        append_event(event_path, "recovered_no_formal_changes")
        return 0

    manifest = _load_json(manifest_path)
    transactions = _transaction_paths(transaction_root, manifest)
    recovery_errors: list[str] = []
    for transaction in reversed(transactions):
        schema = SCHEMAS_BY_TABLE[transaction.table_name]
        table_manifest = manifest["tables"][transaction.table_name]
        try:
            if not transaction.backup_path.exists():
                if not _root_matches_manifest_without_raising(
                    transaction.formal_path, table_manifest, schema
                ):
                    raise RuntimeError(
                        "backup 不存在且正式表不是冻结的原表，无法安全恢复。"
                    )
                continue

            if not _root_matches_manifest_without_raising(
                transaction.backup_path, table_manifest, schema
            ):
                raise RuntimeError("事务 backup 与冻结原表不一致。")

            formal_is_original = _root_matches_manifest_without_raising(
                transaction.formal_path, table_manifest, schema
            )
            if formal_is_original:
                continue

            if transaction.formal_path.exists():
                if transaction.candidate_path.exists():
                    failed_parent = transaction_root / "failed_installed"
                    failed_parent.mkdir(parents=True, exist_ok=True)
                    preserved = failed_parent / (
                        f"{transaction.table_name}-{uuid.uuid4().hex[:8]}"
                    )
                    transaction.formal_path.replace(preserved)
                else:
                    transaction.formal_path.replace(
                        transaction.candidate_path
                    )

            if transaction.rollback_path.exists():
                _remove_generated_tree(
                    transaction.rollback_path,
                    transaction_root,
                )
            _clone_tree_for_rollback(
                transaction.backup_path, transaction.rollback_path
            )
            transaction.rollback_path.replace(transaction.formal_path)
            assert_root_matches_frozen_manifest(
                transaction.formal_path, table_manifest, schema
            )
            append_event(
                event_path,
                "recover_table_rolled_back",
                table_name=transaction.table_name,
            )
        except Exception as error:
            recovery_errors.append(
                f"{transaction.table_name}: {type(error).__name__}: {error}"
            )

    if recovery_errors:
        state.update({
            "phase": "recovery_failed",
            "updated_at": utc_now_text(),
            "recovery_errors": recovery_errors,
        })
        atomic_write_json(state_path, state)
        append_event(
            event_path, "recovery_failed", errors=recovery_errors
        )
        raise RuntimeError(
            "事务恢复不完整：\n" + "\n".join(recovery_errors)
        )

    state.update({
        "phase": "recovered_rolled_back",
        "recovered_at": utc_now_text(),
        "updated_at": utc_now_text(),
        "recovery_errors": [],
    })
    atomic_write_json(state_path, state)
    append_event(event_path, "transaction_recovered_rolled_back")
    print(
        "metadata_progress: phase=recovered_rolled_back; tables=6; "
        "backup_retained=true",
        flush=True,
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument("--write", action="store_true")
    actions.add_argument("--recover", action="store_true")
    parser.add_argument("--transaction-root", type=pathlib.Path)
    args = parser.parse_args()
    if (args.write or args.recover) and args.transaction_root is None:
        parser.error("--write/--recover 必须同时提供 --transaction-root")
    if not (args.write or args.recover) and args.transaction_root is not None:
        parser.error("只读计划不使用 --transaction-root")
    return args


def main() -> int:
    args = parse_args()
    if args.recover:
        return recover_transaction(args.transaction_root)
    if args.write:
        return execute_transaction(args.transaction_root)

    manifest = scan_formal_tables()
    print_plan(manifest)
    assert_expected_baseline(manifest)
    print("metadata_plan: baseline=matched; action=read_only", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
