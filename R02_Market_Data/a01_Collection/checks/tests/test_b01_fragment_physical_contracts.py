"""b01/c01-b04 根 Dataset 必须逐 fragment 拒绝物理契约漂移。"""

from __future__ import annotations

import importlib.util
import pathlib
import sys
import tempfile
import unittest

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq


project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()
for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        PROJECT_ROOT = candidate_root
        sys.path.insert(0, str(candidate_root))
        break
else:
    raise RuntimeError("未找到项目根目录")
B01_ROOT = (
    PROJECT_ROOT
    / "R02_Market_Data/a01_Collection"
    / "b01_Futures_Market_Data"
)


def load_workflow(stem: str):
    path = B01_ROOT / f"{stem}.py"
    specification = importlib.util.spec_from_file_location(
        f"fragment_contract_{stem}", path
    )
    if specification is None or specification.loader is None:
        raise RuntimeError(f"无法加载 {path}。")
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


C01 = load_workflow("c01_trade_calendar")
C02 = load_workflow("c02_futures_variety_calendar")
C03 = load_workflow("c03_futures_contract_calendar")
C04 = load_workflow("c04_futures_bar_calendar")


CASES = (
    (
        C01,
        C01.TRADE_CALENDAR_SCHEMA,
        C01.PARTITION_COLUMNS,
        "is_trading_day",
        ({"year": 2024}, {"year": 2025}),
    ),
    (
        C02,
        C02.FUTURES_VARIETY_CALENDAR_SCHEMA,
        C02.PARTITION_COLUMNS,
        "active_contract_count",
        (
            {"exchange_code": "XSGE", "year": 2024, "month": 1},
            {"exchange_code": "XSGE", "year": 2024, "month": 2},
        ),
    ),
    (
        C03,
        C03.FUTURES_CONTRACT_CALENDAR_SCHEMA,
        C03.PARTITION_COLUMNS,
        "minute_count",
        (
            {"exchange_code": "XSGE", "year": 2024, "month": 1},
            {"exchange_code": "XSGE", "year": 2024, "month": 2},
        ),
    ),
    (
        C04,
        C04.FUTURES_BAR_CALENDAR_SCHEMA,
        C04.PARTITION_COLUMNS,
        "expected_bar_count",
        (
            {
                "bar_frequency": "1d",
                "exchange_code": "XSGE",
                "year": 2024,
                "month": 1,
            },
            {
                "bar_frequency": "1d",
                "exchange_code": "XSGE",
                "year": 2024,
                "month": 2,
            },
        ),
    ),
)


def physical_file_schema(
    schema: pa.Schema,
    partition_columns: list[str],
) -> pa.Schema:
    return pa.schema(
        [
            schema.field(name)
            for name in schema.names
            if name not in partition_columns
        ],
        metadata=schema.metadata,
    )


def mutated_schema(
    schema: pa.Schema,
    field_name: str,
    mutation: str,
) -> pa.Schema:
    fields = list(schema)
    field_index = schema.get_field_index(field_name)
    field = fields[field_index]
    metadata = dict(schema.metadata or {})

    if mutation == "field_name":
        fields[field_index] = pa.field(
            f"{field.name}_wrong",
            field.type,
            nullable=field.nullable,
            metadata=field.metadata,
        )
    elif mutation == "field_type":
        fields[field_index] = pa.field(
            field.name,
            pa.int64(),
            nullable=field.nullable,
            metadata=field.metadata,
        )
    elif mutation == "nullable":
        fields[field_index] = pa.field(
            field.name,
            field.type,
            nullable=not field.nullable,
            metadata=field.metadata,
        )
    elif mutation in {
        "table_name_identity",
        "primary_key_identity",
        "partition_columns_identity",
    }:
        identity_key = mutation.removesuffix("_identity").encode("utf-8")
        metadata[identity_key] = b"wrong_identity"
    else:
        raise ValueError(f"未知 mutation：{mutation}")

    return pa.schema(fields, metadata=metadata)


def write_fragment(
    root: pathlib.Path,
    partition_columns: list[str],
    partition_values: dict[str, object],
    schema: pa.Schema,
) -> None:
    leaf = root
    for column in partition_columns:
        leaf /= f"{column}={partition_values[column]}"
    leaf.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_batches([], schema=schema),
        leaf / "part.parquet",
    )


class FragmentPhysicalContractTests(unittest.TestCase):
    def test_later_fragment_physical_or_identity_drift_is_rejected(self) -> None:
        for (
            module,
            schema,
            partition_columns,
            field_name,
            partition_values,
        ) in CASES:
            for mutation in (
                "field_name",
                "field_type",
                "nullable",
                "table_name_identity",
                "primary_key_identity",
                "partition_columns_identity",
            ):
                with self.subTest(
                    table=schema.metadata[b"table_name"],
                    mutation=mutation,
                ), tempfile.TemporaryDirectory() as directory:
                    root = pathlib.Path(directory)
                    expected_file_schema = physical_file_schema(
                        schema, partition_columns
                    )
                    write_fragment(
                        root,
                        partition_columns,
                        partition_values[0],
                        expected_file_schema,
                    )
                    write_fragment(
                        root,
                        partition_columns,
                        partition_values[1],
                        mutated_schema(
                            expected_file_schema,
                            field_name,
                            mutation,
                        ),
                    )
                    partitioning = ds.partitioning(
                        pa.schema(
                            [schema.field(name) for name in partition_columns]
                        ),
                        flavor="hive",
                    )
                    dataset = ds.dataset(
                        root,
                        format="parquet",
                        partitioning=partitioning,
                    )

                    # 首 fragment 正确时 Dataset 逻辑 Schema 仍可以看似正常；
                    # 新门禁必须由后续 fragment 物理 Schema 发现漂移。
                    logical_schema = pa.schema(
                        [dataset.schema.field(name) for name in schema.names],
                        metadata=dataset.schema.metadata,
                    )
                    if module is C04:
                        self.assertTrue(
                            module.physically_and_identity_compatible(
                                logical_schema, schema
                            )
                        )
                        with self.assertRaises(TypeError):
                            module.open_contract_dataset(
                                root,
                                partitioning,
                                schema,
                                partition_columns,
                                "混合物理契约测试",
                                required=True,
                            )
                    else:
                        module.validate_compatible_dataset_schema(
                            logical_schema, schema, "逻辑 Schema 测试 "
                        )
                        with self.assertRaises(TypeError):
                            module.validate_dataset_fragment_schemas(
                                dataset,
                                schema,
                                partition_columns,
                                "混合物理契约测试 ",
                            )

    def test_descriptive_metadata_drift_remains_compatible(self) -> None:
        for (
            module,
            schema,
            partition_columns,
            _,
            partition_values,
        ) in CASES:
            with self.subTest(table=schema.metadata[b"table_name"]), tempfile.TemporaryDirectory() as directory:
                root = pathlib.Path(directory)
                expected_file_schema = physical_file_schema(
                    schema, partition_columns
                )
                descriptive_fields = []
                for field in expected_file_schema:
                    field_metadata = dict(field.metadata or {})
                    field_metadata[b"description_zh"] = b"legacy description"
                    descriptive_fields.append(field.with_metadata(field_metadata))
                descriptive_metadata = dict(
                    expected_file_schema.metadata or {}
                )
                descriptive_metadata[b"schema_version"] = b"legacy-version"
                descriptive_schema = pa.schema(
                    descriptive_fields,
                    metadata=descriptive_metadata,
                )
                for values in partition_values:
                    write_fragment(
                        root,
                        partition_columns,
                        values,
                        descriptive_schema,
                    )
                partitioning = ds.partitioning(
                    pa.schema(
                        [schema.field(name) for name in partition_columns]
                    ),
                    flavor="hive",
                )
                if module is C04:
                    self.assertIsNotNone(
                        module.open_contract_dataset(
                            root,
                            partitioning,
                            schema,
                            partition_columns,
                            "描述 metadata 测试",
                            required=True,
                        )
                    )
                else:
                    dataset = ds.dataset(
                        root,
                        format="parquet",
                        partitioning=partitioning,
                    )
                    module.validate_dataset_fragment_schemas(
                        dataset,
                        schema,
                        partition_columns,
                        "描述 metadata 测试 ",
                    )


if __name__ == "__main__":
    unittest.main()
