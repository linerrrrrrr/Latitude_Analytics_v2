from __future__ import annotations

import base64
import hashlib
import importlib.util
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPT_PATH = (
    PROJECT_ROOT
    / "00_draft_collection_02"
    / "scripts"
    / "sync_current_silver_parquet_metadata.py"
)
SPEC = importlib.util.spec_from_file_location(
    "sync_current_silver_parquet_metadata",
    SCRIPT_PATH,
)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"无法加载模块：{SCRIPT_PATH}")
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


EXPECTED_SCHEMA = pa.schema(
    [
        pa.field(
            "record_id",
            pa.int64(),
            nullable=False,
            metadata={
                b"field_name_zh": "记录标识".encode("utf-8"),
                b"description_zh": "唯一记录标识。".encode("utf-8"),
            },
        ),
        pa.field(
            "category",
            pa.string(),
            nullable=False,
            metadata={
                b"field_name_zh": "分类".encode("utf-8"),
                b"description_zh": "记录分类。".encode("utf-8"),
            },
        ),
        pa.field(
            "amount",
            pa.float64(),
            nullable=True,
            metadata={
                b"field_name_zh": "数值".encode("utf-8"),
                b"description_zh": "允许为空的测试数值。".encode("utf-8"),
            },
        ),
        pa.field(
            "year",
            pa.int16(),
            nullable=False,
            metadata={
                b"field_name_zh": "年份".encode("utf-8"),
                b"description_zh": "Hive 分区年份。".encode("utf-8"),
            },
        ),
    ],
    metadata={
        b"table_name": b"test_metadata_sync",
        b"primary_key": b"record_id",
        b"partition_columns": b"year",
        b"description_zh": "当前表级说明。".encode("utf-8"),
        b"update_mode_zh": "当前更新说明。".encode("utf-8"),
        b"schema_version": b"1.2.0",
    },
)


def field_metadata_drift_schema(schema: pa.Schema) -> pa.Schema:
    fields = []
    for field in schema:
        field_metadata = dict(field.metadata or {})
        field_metadata[b"description_zh"] = "历史字段说明。".encode("utf-8")
        fields.append(field.with_metadata(field_metadata))
    return pa.schema(fields, metadata=schema.metadata)


def table_metadata_drift_schema(schema: pa.Schema) -> pa.Schema:
    table_metadata = dict(schema.metadata or {})
    table_metadata[b"description_zh"] = "历史表级说明。".encode("utf-8")
    table_metadata[b"update_mode_zh"] = "历史更新说明。".encode("utf-8")
    table_metadata[b"schema_version"] = b"1.1.0"
    return schema.with_metadata(table_metadata)


def metadata_drift_schema(schema: pa.Schema) -> pa.Schema:
    return table_metadata_drift_schema(field_metadata_drift_schema(schema))


def fixture_table(schema: pa.Schema) -> pa.Table:
    return pa.Table.from_pylist(
        [
            {"record_id": 1, "category": "alpha", "amount": 1.25},
            {"record_id": 2, "category": "alpha", "amount": None},
            {"record_id": 3, "category": "beta", "amount": -3.5},
            {"record_id": 4, "category": "gamma", "amount": 8.0},
            {"record_id": 5, "category": "gamma", "amount": 13.75},
        ],
        schema=schema,
    )


def write_fixture(path: pathlib.Path, schema: pa.Schema) -> None:
    pq.write_table(
        fixture_table(schema),
        path,
        row_group_size=2,
        compression="snappy",
        use_dictionary=["category"],
        write_statistics=True,
        version="2.6",
        data_page_version="2.0",
    )


def parquet_data_region_sha256(path: pathlib.Path) -> str:
    payload = path.read_bytes()
    if len(payload) < 12 or payload[:4] != b"PAR1" or payload[-4:] != b"PAR1":
        raise ValueError("测试 Parquet 文件 magic 不完整。")
    footer_length = int.from_bytes(payload[-8:-4], byteorder="little")
    footer_start = len(payload) - 8 - footer_length
    if footer_start < 4:
        raise ValueError("测试 Parquet footer 边界非法。")
    return hashlib.sha256(payload[:footer_start]).hexdigest()


def statistics_signature(statistics: object | None) -> tuple[object, ...] | None:
    if statistics is None:
        return None
    return (
        statistics.has_min_max,
        statistics.min if statistics.has_min_max else None,
        statistics.max if statistics.has_min_max else None,
        statistics.null_count,
        statistics.distinct_count,
        statistics.num_values,
        statistics.physical_type,
    )


def parquet_physical_signature(path: pathlib.Path) -> tuple[object, ...]:
    metadata = pq.ParquetFile(path).metadata
    row_groups = []
    for row_group_index in range(metadata.num_row_groups):
        row_group = metadata.row_group(row_group_index)
        columns = []
        for column_index in range(row_group.num_columns):
            column = row_group.column(column_index)
            columns.append(
                (
                    column.path_in_schema,
                    column.physical_type,
                    column.num_values,
                    column.compression,
                    tuple(column.encodings),
                    column.has_dictionary_page,
                    column.dictionary_page_offset,
                    column.data_page_offset,
                    column.file_offset,
                    column.total_compressed_size,
                    column.total_uncompressed_size,
                    statistics_signature(column.statistics),
                )
            )
        row_groups.append(
            (
                row_group.num_rows,
                row_group.total_byte_size,
                tuple(columns),
            )
        )
    return (
        metadata.format_version,
        metadata.created_by,
        metadata.num_rows,
        metadata.num_row_groups,
        metadata.num_columns,
        tuple(row_groups),
    )


class CurrentSilverParquetMetadataSyncTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary_directory = tempfile.TemporaryDirectory(
            prefix="current-silver-metadata-sync-"
        )
        self.addCleanup(temporary_directory.cleanup)
        self.root = pathlib.Path(temporary_directory.name)
        self.expected_file_schema = MODULE.expected_file_schema(
            EXPECTED_SCHEMA,
            ["year"],
        )

    def transaction_path_manifest(
        self,
        relative_path: pathlib.Path,
    ) -> dict[str, object]:
        return {
            "tables": {
                table_name: {
                    "formal_path": str(self.root / "formal" / table_name),
                    "files": (
                        [{"relative_path": relative_path.as_posix()}]
                        if table_name == "fact_futures_minute"
                        else []
                    ),
                }
                for table_name in MODULE.SCHEMAS_BY_TABLE
            }
        }

    def transaction_root_for_candidate_length(
        self,
        relative_path: pathlib.Path,
        candidate_length: int,
    ) -> pathlib.Path:
        table_name = "fact_futures_minute"
        unpadded_root = self.root / "t"
        unpadded_candidate = (
            unpadded_root / "candidate" / table_name / relative_path
        )
        padding_length = candidate_length - len(str(unpadded_candidate))
        if padding_length < 0:
            raise ValueError("测试临时根路径过长，无法构造目标边界。")
        transaction_root = self.root / f"t{'x' * padding_length}"
        actual_candidate = (
            transaction_root / "candidate" / table_name / relative_path
        )
        if len(str(actual_candidate)) != candidate_length:
            raise AssertionError("未精确构造 candidate 路径长度。")
        return transaction_root

    def test_table_and_field_metadata_drift_are_independently_detected(self) -> None:
        drift_schemas = {
            "table": table_metadata_drift_schema(self.expected_file_schema),
            "field": field_metadata_drift_schema(self.expected_file_schema),
        }
        for metadata_level, drift_schema in drift_schemas.items():
            with self.subTest(metadata_level=metadata_level):
                source_path = self.root / f"{metadata_level}-metadata.parquet"
                write_fixture(source_path, drift_schema)
                self.assertEqual(
                    MODULE.inspect_parquet_file(
                        source_path,
                        self.expected_file_schema,
                    ).status,
                    "metadata_drift",
                )

    def test_transaction_path_length_gate_allows_short_and_rejects_260(
        self,
    ) -> None:
        relative_path = pathlib.Path(
            "exchange_code=XDCE",
            "underlying_code=A",
            "year=2024",
            "month=1",
            "part-6c062f62080a44579f21c596c5d542d1-0.parquet",
        )
        manifest = self.transaction_path_manifest(relative_path)
        short_transaction_root = self.root / "m"
        short_lengths = [
            len(
                str(
                    short_transaction_root
                    / tree_name
                    / "fact_futures_minute"
                    / relative_path
                )
            )
            for tree_name in ["candidate", "backup", "rollback"]
        ]
        self.assertLess(max(short_lengths), 260)
        self.assertIsNone(
            MODULE.assert_transaction_path_lengths(
                short_transaction_root,
                manifest,
            )
        )

        boundary_transaction_root = self.transaction_root_for_candidate_length(
            relative_path,
            260,
        )
        with self.assertRaisesRegex(OSError, "260|MAX_PATH|路径"):
            MODULE.assert_transaction_path_lengths(
                boundary_transaction_root,
                manifest,
            )

    def test_transaction_path_gate_runs_before_candidate_build(self) -> None:
        relative_path = pathlib.Path(
            "exchange_code=XDCE",
            "underlying_code=A",
            "year=2024",
            "month=1",
            "part-6c062f62080a44579f21c596c5d542d1-0.parquet",
        )
        manifest = self.transaction_path_manifest(relative_path)
        transaction_root = self.transaction_root_for_candidate_length(
            relative_path,
            260,
        )

        with (
            mock.patch.object(
                MODULE,
                "ALLOWED_TRANSACTION_ROOT",
                self.root,
            ),
            mock.patch.object(
                MODULE,
                "scan_formal_tables",
                return_value=manifest,
            ),
            mock.patch.object(MODULE, "print_plan"),
            mock.patch.object(MODULE, "assert_expected_baseline"),
            mock.patch.object(MODULE, "assert_root_matches_frozen_manifest"),
            mock.patch.object(MODULE, "build_all_candidates") as build_candidates,
        ):
            with self.assertRaisesRegex(OSError, "260|MAX_PATH|路径"):
                MODULE.execute_transaction(transaction_root)

        build_candidates.assert_not_called()
        self.assertFalse((transaction_root / "candidate").exists())

    def test_table_and_field_metadata_drift_updates_footer_and_arrow_schema(self) -> None:
        source_path = self.root / "source.parquet"
        candidate_path = self.root / "candidate.parquet"
        stale_schema = metadata_drift_schema(self.expected_file_schema)
        write_fixture(source_path, stale_schema)

        before_source_bytes = source_path.read_bytes()
        before_table = pq.read_table(source_path)
        before_data_hash = parquet_data_region_sha256(source_path)
        before_physical_signature = parquet_physical_signature(source_path)

        inspection = MODULE.inspect_parquet_file(
            source_path,
            self.expected_file_schema,
        )
        self.assertEqual(inspection.status, "metadata_drift")

        candidate_result = MODULE.write_candidate_parquet(
            source_path,
            candidate_path,
            self.expected_file_schema,
        )

        self.assertTrue(candidate_result.changed)
        self.assertEqual(
            candidate_result.source_data_sha256,
            candidate_result.candidate_data_sha256,
        )
        self.assertEqual(source_path.read_bytes(), before_source_bytes)
        self.assertTrue(
            pq.read_schema(candidate_path).equals(
                self.expected_file_schema,
                check_metadata=True,
            )
        )
        arrow_schema_metadata = pq.ParquetFile(candidate_path).metadata.metadata
        self.assertEqual(
            arrow_schema_metadata[b"ARROW:schema"],
            base64.b64encode(self.expected_file_schema.serialize().to_pybytes()),
        )
        self.assertEqual(
            parquet_data_region_sha256(candidate_path),
            before_data_hash,
        )
        self.assertEqual(
            parquet_physical_signature(candidate_path),
            before_physical_signature,
        )
        self.assertTrue(pq.read_table(candidate_path).equals(before_table))
        self.assertEqual(
            MODULE.inspect_parquet_file(
                candidate_path,
                self.expected_file_schema,
            ).status,
            "exact",
        )

    def test_exact_file_is_classified_without_touching_source(self) -> None:
        source_path = self.root / "exact.parquet"
        write_fixture(source_path, self.expected_file_schema)
        before_bytes = source_path.read_bytes()
        before_stat = source_path.stat()

        inspection = MODULE.inspect_parquet_file(
            source_path,
            self.expected_file_schema,
        )

        self.assertEqual(inspection.status, "exact")
        self.assertEqual(source_path.read_bytes(), before_bytes)
        self.assertEqual(source_path.stat().st_mtime_ns, before_stat.st_mtime_ns)

        copied_path = self.root / "copied-exact.parquet"
        candidate_result = MODULE.write_candidate_parquet(
            source_path,
            copied_path,
            self.expected_file_schema,
        )
        self.assertFalse(candidate_result.changed)
        self.assertEqual(copied_path.read_bytes(), before_bytes)
        self.assertEqual(
            MODULE.inspect_parquet_file(
                copied_path,
                self.expected_file_schema,
            ).status,
            "exact",
        )

    def test_physical_field_order_type_and_nullable_drift_are_rejected(self) -> None:
        changed_schemas = {}

        reversed_fields = list(reversed(self.expected_file_schema))
        changed_schemas["order"] = pa.schema(
            reversed_fields,
            metadata=self.expected_file_schema.metadata,
        )

        type_fields = list(self.expected_file_schema)
        type_fields[0] = pa.field(
            type_fields[0].name,
            pa.int32(),
            nullable=type_fields[0].nullable,
            metadata=type_fields[0].metadata,
        )
        changed_schemas["type"] = pa.schema(
            type_fields,
            metadata=self.expected_file_schema.metadata,
        )

        nullable_fields = list(self.expected_file_schema)
        nullable_fields[0] = nullable_fields[0].with_nullable(True)
        changed_schemas["nullable"] = pa.schema(
            nullable_fields,
            metadata=self.expected_file_schema.metadata,
        )

        for difference, changed_schema in changed_schemas.items():
            with self.subTest(difference=difference):
                source_path = self.root / f"incompatible-{difference}.parquet"
                write_fixture(source_path, changed_schema)
                before_bytes = source_path.read_bytes()

                with self.assertRaises((TypeError, ValueError)):
                    MODULE.inspect_parquet_file(
                        source_path,
                        self.expected_file_schema,
                    )

                self.assertEqual(source_path.read_bytes(), before_bytes)

    def test_truncated_footer_and_damaged_magic_are_rejected(self) -> None:
        valid_path = self.root / "valid.parquet"
        write_fixture(valid_path, self.expected_file_schema)
        valid_payload = valid_path.read_bytes()
        footer_length = int.from_bytes(valid_payload[-8:-4], byteorder="little")
        footer_start = len(valid_payload) - 8 - footer_length

        corrupted_payloads = {
            "truncated_footer": valid_payload[: footer_start + 8] + valid_payload[-8:],
            "header_magic": b"BAD!" + valid_payload[4:],
            "trailer_magic": valid_payload[:-4] + b"BAD!",
        }
        for corruption, payload in corrupted_payloads.items():
            with self.subTest(corruption=corruption):
                corrupted_path = self.root / f"{corruption}.parquet"
                corrupted_path.write_bytes(payload)
                before_bytes = corrupted_path.read_bytes()

                with self.assertRaises((OSError, RuntimeError, TypeError, ValueError)):
                    MODULE.inspect_parquet_file(
                        corrupted_path,
                        self.expected_file_schema,
                    )

                self.assertEqual(corrupted_path.read_bytes(), before_bytes)

    def test_candidate_exchange_failure_restores_every_formal_table(self) -> None:
        formal_root = self.root / "formal" / "silver"
        candidate_root = self.root / "candidate" / "silver"
        backup_root = self.root / "backup" / "silver"
        table_names = [f"table_{index}" for index in range(6)]
        transactions = []

        for table_name in table_names:
            formal_table = formal_root / table_name
            candidate_table = candidate_root / table_name
            backup_table = backup_root / table_name
            formal_table.mkdir(parents=True)
            candidate_table.mkdir(parents=True)
            (formal_table / "origin.bin").write_bytes(
                f"formal-{table_name}".encode("utf-8")
            )
            (candidate_table / "origin.bin").write_bytes(
                f"candidate-{table_name}".encode("utf-8")
            )
            transactions.append(
                MODULE.TableTransaction(
                    table_name=table_name,
                    formal_path=formal_table,
                    candidate_path=candidate_table,
                    backup_path=backup_table,
                )
            )

        before_formal = {
            table_name: (formal_root / table_name / "origin.bin").read_bytes()
            for table_name in table_names
        }
        original_replace = pathlib.Path.replace
        candidate_install_count = 0

        def fail_second_candidate_install(
            source_path: pathlib.Path,
            destination_path: pathlib.Path,
        ) -> pathlib.Path:
            nonlocal candidate_install_count
            if source_path.parent == candidate_root:
                candidate_install_count += 1
                if candidate_install_count == 2:
                    raise OSError("injected candidate exchange failure")
            return original_replace(source_path, destination_path)

        with mock.patch.object(
            pathlib.Path,
            "replace",
            fail_second_candidate_install,
        ):
            with self.assertRaisesRegex(
                OSError,
                "injected candidate exchange failure",
            ):
                MODULE.coordinated_exchange(transactions)

        for table_index, table_name in enumerate(table_names):
            self.assertEqual(
                (formal_root / table_name / "origin.bin").read_bytes(),
                before_formal[table_name],
            )
            self.assertEqual(
                (candidate_root / table_name / "origin.bin").read_bytes(),
                f"candidate-{table_name}".encode("utf-8"),
            )
            transaction = transactions[table_index]
            self.assertFalse(transaction.rollback_path.exists())
            if table_index < 2:
                self.assertEqual(
                    (transaction.backup_path / "origin.bin").read_bytes(),
                    before_formal[table_name],
                )
            else:
                self.assertFalse(transaction.backup_path.exists())

    def test_formal_reread_failure_after_six_installs_rolls_back_every_table(
        self,
    ) -> None:
        formal_root = self.root / "formal" / "silver"
        candidate_root = self.root / "candidate" / "silver"
        backup_root = self.root / "backup" / "silver"
        table_names = [f"table_{index}" for index in range(6)]
        transactions = []

        for table_name in table_names:
            formal_table = formal_root / table_name
            candidate_table = candidate_root / table_name
            backup_table = backup_root / table_name
            formal_table.mkdir(parents=True)
            candidate_table.mkdir(parents=True)
            (formal_table / "origin.bin").write_bytes(
                f"formal-{table_name}".encode("utf-8")
            )
            (candidate_table / "origin.bin").write_bytes(
                f"candidate-{table_name}".encode("utf-8")
            )
            transactions.append(
                MODULE.TableTransaction(
                    table_name=table_name,
                    formal_path=formal_table,
                    candidate_path=candidate_table,
                    backup_path=backup_table,
                )
            )

        before_formal = {
            table_name: (formal_root / table_name / "origin.bin").read_bytes()
            for table_name in table_names
        }
        validated_tables = []
        injected_error = RuntimeError("injected formal reread failure")

        def fail_fourth_formal_reread(
            transaction: MODULE.TableTransaction,
        ) -> None:
            validated_tables.append(transaction.table_name)
            if transaction.table_name == "table_3":
                raise injected_error

        with self.assertRaises(RuntimeError) as raised:
            MODULE.coordinated_exchange(
                transactions,
                validate_formal=fail_fourth_formal_reread,
            )

        self.assertIs(raised.exception, injected_error)
        self.assertEqual(validated_tables, table_names[:4])
        for table_index, table_name in enumerate(table_names):
            transaction = transactions[table_index]
            self.assertEqual(
                (transaction.formal_path / "origin.bin").read_bytes(),
                before_formal[table_name],
            )
            self.assertEqual(
                (transaction.candidate_path / "origin.bin").read_bytes(),
                f"candidate-{table_name}".encode("utf-8"),
            )
            self.assertEqual(
                (transaction.backup_path / "origin.bin").read_bytes(),
                before_formal[table_name],
            )
            self.assertFalse(transaction.rollback_path.exists())


if __name__ == "__main__":
    unittest.main()
