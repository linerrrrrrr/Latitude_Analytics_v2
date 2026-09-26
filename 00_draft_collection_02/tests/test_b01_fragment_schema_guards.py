from __future__ import annotations

import importlib.util
import pathlib
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
BUSINESS_DIR = (
    PROJECT_ROOT
    / "02_Futures_Lakehouse"
    / "a01_Futures_Market_Data"
)


def load_module(name: str, path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载测试模块：{path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


C05 = load_module("c05_fragment_schema_guards", BUSINESS_DIR / "b05_futures_daily.py")
C06 = load_module("c06_fragment_schema_guards", BUSINESS_DIR / "b06_futures_minute.py")
C07 = load_module(
    "c07_fragment_schema_guards",
    BUSINESS_DIR / "b07_suspected_session_reconciliation.py",
)


CALENDAR_SCHEMA = C05.FUTURES_BAR_CALENDAR_SCHEMA
PARTITION_COLUMNS = C05.CALENDAR_PARTITION_COLUMNS
SELECTED_PARTITION_KEY = ("1m", "CCFX", 2026, 8)
OTHER_PARTITION_KEY = ("1m", "XSGE", 2026, 8)
CORRUPTIONS = (
    "type",
    "nullable",
    "extra_column",
    "table_name",
    "primary_key",
    "partition_columns",
)


def parquet_file_schema(schema: pa.Schema) -> pa.Schema:
    return pa.schema(
        [field for field in schema if field.name not in PARTITION_COLUMNS],
        metadata=schema.metadata,
    )


def corrupt_schema(expected_schema: pa.Schema, corruption: str) -> pa.Schema:
    fields = list(expected_schema)
    metadata = dict(expected_schema.metadata or {})
    contract_code_index = expected_schema.get_field_index("contract_code")
    contract_code_field = fields[contract_code_index]
    if corruption == "type":
        fields[contract_code_index] = pa.field(
            contract_code_field.name,
            pa.large_string(),
            nullable=contract_code_field.nullable,
            metadata=contract_code_field.metadata,
        )
    elif corruption == "nullable":
        fields[contract_code_index] = pa.field(
            contract_code_field.name,
            contract_code_field.type,
            nullable=not contract_code_field.nullable,
            metadata=contract_code_field.metadata,
        )
    elif corruption == "extra_column":
        fields.append(pa.field("unexpected", pa.int8(), nullable=True))
    elif corruption in {"table_name", "primary_key", "partition_columns"}:
        metadata[corruption.encode("utf-8")] = b"incompatible"
    else:
        raise AssertionError(f"未知测试破坏类型：{corruption}")
    return pa.schema(fields, metadata=metadata)


def partition_path(
    table_path: pathlib.Path,
    partition_key: tuple[object, ...],
) -> pathlib.Path:
    return table_path.joinpath(
        *[
            f"{column}={value}"
            for column, value in zip(
                PARTITION_COLUMNS,
                partition_key,
                strict=True,
            )
        ]
    )


def write_fragment(
    table_path: pathlib.Path,
    partition_key: tuple[object, ...],
    file_name: str,
    file_schema: pa.Schema,
) -> None:
    leaf_path = partition_path(table_path, partition_key)
    leaf_path.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_batches([], schema=file_schema),
        leaf_path / file_name,
    )


class FragmentSchemaGuardTests(unittest.TestCase):
    def test_c05_root_and_dirty_leaf_reject_every_bad_fragment(self) -> None:
        expected_schema = parquet_file_schema(CALENDAR_SCHEMA)
        for corruption in CORRUPTIONS:
            with self.subTest(corruption=corruption), tempfile.TemporaryDirectory() as directory:
                table_path = pathlib.Path(directory)
                write_fragment(
                    table_path,
                    SELECTED_PARTITION_KEY,
                    "part-0-good.parquet",
                    expected_schema,
                )
                write_fragment(
                    table_path,
                    SELECTED_PARTITION_KEY,
                    "part-9-bad.parquet",
                    corrupt_schema(expected_schema, corruption),
                )

                with self.assertRaisesRegex(TypeError, "fragment"):
                    C05.open_contract_dataset(
                        table_path,
                        C05.CALENDAR_PARTITIONING,
                        CALENDAR_SCHEMA,
                        "混合行情日历",
                        required=True,
                    )
                with self.assertRaisesRegex(TypeError, "fragment"):
                    C05.read_partition_leaf(
                        table_path,
                        CALENDAR_SCHEMA,
                        PARTITION_COLUMNS,
                        C05.CALENDAR_PARTITIONING,
                        SELECTED_PARTITION_KEY,
                    )

    def test_c06_root_rejects_every_bad_fragment_and_leaf_guard_remains_strict(self) -> None:
        expected_schema = parquet_file_schema(CALENDAR_SCHEMA)
        for corruption in CORRUPTIONS:
            with self.subTest(corruption=corruption), tempfile.TemporaryDirectory() as directory:
                table_path = pathlib.Path(directory)
                write_fragment(
                    table_path,
                    SELECTED_PARTITION_KEY,
                    "part-0-good.parquet",
                    expected_schema,
                )
                write_fragment(
                    table_path,
                    SELECTED_PARTITION_KEY,
                    "part-9-bad.parquet",
                    corrupt_schema(expected_schema, corruption),
                )

                with self.assertRaisesRegex(TypeError, "fragment"):
                    C06.open_contract_dataset(
                        table_path,
                        C06.CALENDAR_PARTITIONING,
                        CALENDAR_SCHEMA,
                        "混合行情日历",
                        required=True,
                    )
                with self.assertRaises(TypeError):
                    C06.read_complete_partition(
                        table_path,
                        CALENDAR_SCHEMA,
                        PARTITION_COLUMNS,
                        SELECTED_PARTITION_KEY,
                    )

    def test_c07_checks_only_fragments_selected_for_the_immediate_read(self) -> None:
        expected_schema = parquet_file_schema(CALENDAR_SCHEMA)
        selected_filter = C07.partition_expression(
            PARTITION_COLUMNS,
            SELECTED_PARTITION_KEY,
        )
        other_filter = C07.partition_expression(
            PARTITION_COLUMNS,
            OTHER_PARTITION_KEY,
        )
        for corruption in CORRUPTIONS:
            with self.subTest(corruption=corruption), tempfile.TemporaryDirectory() as directory:
                table_path = pathlib.Path(directory)
                write_fragment(
                    table_path,
                    SELECTED_PARTITION_KEY,
                    "part-0-good.parquet",
                    expected_schema,
                )
                write_fragment(
                    table_path,
                    OTHER_PARTITION_KEY,
                    "part-9-bad.parquet",
                    corrupt_schema(expected_schema, corruption),
                )
                dataset = C07.open_exact_dataset(
                    table_path,
                    C07.CALENDAR_PARTITIONING,
                    CALENDAR_SCHEMA,
                    "混合行情日历",
                )

                C07.validate_read_fragments(
                    dataset,
                    selected_filter,
                    CALENDAR_SCHEMA,
                    PARTITION_COLUMNS,
                    "选中行情日历叶",
                )
                with self.assertRaisesRegex(TypeError, "fragment"):
                    C07.validate_read_fragments(
                        dataset,
                        other_filter,
                        CALENDAR_SCHEMA,
                        PARTITION_COLUMNS,
                        "另一行情日历叶",
                    )

    def test_c07_rejects_an_incompatible_root_schema_marker(self) -> None:
        expected_schema = parquet_file_schema(CALENDAR_SCHEMA)
        with tempfile.TemporaryDirectory() as directory:
            table_path = pathlib.Path(directory)
            write_fragment(
                table_path,
                SELECTED_PARTITION_KEY,
                "part-0-good.parquet",
                expected_schema,
            )
            pq.write_table(
                pa.Table.from_batches(
                    [],
                    schema=corrupt_schema(expected_schema, "primary_key"),
                ),
                table_path / "schema.parquet",
            )

            with self.assertRaisesRegex(TypeError, "schema.parquet"):
                C07.open_exact_dataset(
                    table_path,
                    C07.CALENDAR_PARTITIONING,
                    CALENDAR_SCHEMA,
                    "行情日历",
                )


if __name__ == "__main__":
    unittest.main()
