"""验证 Pandas、Polars 与 Arrow 共用同一份数据契约。"""

from __future__ import annotations

import pathlib
import sys
import unittest
from datetime import datetime, timezone

import pandas as pd
import polars as pl
import pyarrow as pa

project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()  # 当前工作目录

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        break
else:
    raise RuntimeError("未找到项目根目录")

PROJECT_ROOT = candidate_root

from config.data_contracts import (
    ARROW_TYPE_MAPPINGS,
    arrow_to_pandas,
    arrow_to_polars,
    empty_pandas,
    empty_polars,
    pandas_to_arrow,
    polars_to_arrow,
    resolve_type_mapping,
    validate_arrow_table,
)


TEST_SCHEMA = pa.schema(
    [
        pa.field("identifier", pa.int8(), nullable=False),
        pa.field("label", pa.string(), nullable=False),
        pa.field("observed_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("value", pa.float64()),
    ]
)


def valid_arrow_table() -> pa.Table:
    return pa.Table.from_pydict(
        {
            "identifier": [1, 2],
            "label": ["甲", "乙"],
            "observed_at": [
                datetime(2026, 8, 1, 0, 0, tzinfo=timezone.utc),
                datetime(2026, 8, 1, 0, 1, tzinfo=timezone.utc),
            ],
            "value": [1.5, None],
        },
        schema=TEST_SCHEMA,
    )


class DataContractsTest(unittest.TestCase):
    def test_executable_mapping_covers_documented_non_parameterized_types(self) -> None:
        expected_polars_types = {
            pa.date32(): pl.Date,
            pa.timestamp("us", tz="UTC"): pl.Datetime("us", "UTC"),
            pa.timestamp("us", tz="Asia/Shanghai"): pl.Datetime(
                "us", "Asia/Shanghai"
            ),
            pa.time64("us"): pl.Time,
            pa.string(): pl.String,
            pa.bool_(): pl.Boolean,
            pa.int8(): pl.Int8,
            pa.int16(): pl.Int16,
            pa.int32(): pl.Int32,
            pa.int64(): pl.Int64,
            pa.float32(): pl.Float32,
            pa.float64(): pl.Float64,
        }

        self.assertEqual(set(ARROW_TYPE_MAPPINGS), set(expected_polars_types))
        for arrow_type, polars_type in expected_polars_types.items():
            with self.subTest(arrow_type=str(arrow_type)):
                mapping = ARROW_TYPE_MAPPINGS[arrow_type]
                self.assertEqual(mapping.pandas_dtype, pd.ArrowDtype(arrow_type))
                self.assertEqual(mapping.polars_dtype, polars_type)

    def test_documented_mapping_contains_every_executable_engine_type(self) -> None:
        database_agents_path = PROJECT_ROOT / "03_Futures_Database" / "AGENTS.md"
        database_rules = database_agents_path.read_text(encoding="utf-8")

        for arrow_type, mapping in ARROW_TYPE_MAPPINGS.items():
            with self.subTest(arrow_type=str(arrow_type)):
                self.assertIn(f"`{mapping.pandas_dtype}`", database_rules)
                self.assertIn(f"`{mapping.polars_dtype}`", database_rules)

    def test_decimal_mapping_preserves_precision_and_scale(self) -> None:
        arrow_type = pa.decimal128(18, 4)
        mapping = resolve_type_mapping(arrow_type)

        self.assertEqual(mapping.pandas_dtype, pd.ArrowDtype(arrow_type))
        self.assertEqual(mapping.polars_dtype, pl.Decimal(18, 4))

    def test_unregistered_arrow_type_is_rejected(self) -> None:
        with self.assertRaisesRegex(TypeError, "尚未登记 Pandas/Polars 映射"):
            resolve_type_mapping(pa.duration("us"))

    def test_pandas_round_trip_uses_arrow_dtypes(self) -> None:
        dataframe = arrow_to_pandas(valid_arrow_table(), TEST_SCHEMA)

        self.assertEqual(str(dataframe.dtypes["identifier"]), "int8[pyarrow]")
        self.assertEqual(str(dataframe.dtypes["observed_at"]), "timestamp[us, tz=UTC][pyarrow]")
        self.assertEqual(pandas_to_arrow(dataframe, TEST_SCHEMA).schema, TEST_SCHEMA)

    def test_polars_round_trip_uses_schema_dtypes(self) -> None:
        dataframe = arrow_to_polars(valid_arrow_table(), TEST_SCHEMA)

        self.assertEqual(dataframe.schema["identifier"], pl.Int8)
        self.assertEqual(dataframe.schema["label"], pl.String)
        self.assertEqual(dataframe.schema["observed_at"], pl.Datetime("us", "UTC"))
        self.assertEqual(polars_to_arrow(dataframe, TEST_SCHEMA).schema, TEST_SCHEMA)

    def test_pandas_and_polars_both_reject_wrong_column_order(self) -> None:
        pandas_df = arrow_to_pandas(valid_arrow_table(), TEST_SCHEMA)
        polars_df = arrow_to_polars(valid_arrow_table(), TEST_SCHEMA)
        reversed_columns = list(reversed(TEST_SCHEMA.names))

        with self.assertRaisesRegex(ValueError, "数据列与 Schema 不一致"):
            pandas_to_arrow(pandas_df[reversed_columns], TEST_SCHEMA)
        with self.assertRaisesRegex(ValueError, "数据列与 Schema 不一致"):
            polars_to_arrow(polars_df.select(reversed_columns), TEST_SCHEMA)

    def test_dataframe_converters_reject_the_other_dataframe_type(self) -> None:
        pandas_df = arrow_to_pandas(valid_arrow_table(), TEST_SCHEMA)
        polars_df = arrow_to_polars(valid_arrow_table(), TEST_SCHEMA)

        with self.assertRaisesRegex(TypeError, "pandas.DataFrame"):
            pandas_to_arrow(polars_df, TEST_SCHEMA)
        with self.assertRaisesRegex(TypeError, "polars.DataFrame"):
            polars_to_arrow(pandas_df, TEST_SCHEMA)

    def test_arrow_validation_rejects_null_in_required_field(self) -> None:
        table = pa.table(
            {
                "identifier": pa.array([1, None], type=pa.int8()),
                "label": ["甲", "乙"],
                "observed_at": pa.array(
                    [
                        datetime(2026, 8, 1, 0, 0, tzinfo=timezone.utc),
                        datetime(2026, 8, 1, 0, 1, tzinfo=timezone.utc),
                    ],
                    type=pa.timestamp("us", tz="UTC"),
                ),
                "value": pa.array([1.5, None], type=pa.float64()),
            }
        )

        with self.assertRaises((ValueError, pa.ArrowInvalid)):
            validate_arrow_table(table, TEST_SCHEMA)

    def test_arrow_validation_rejects_unsafe_cast(self) -> None:
        table = valid_arrow_table().set_column(
            0,
            "identifier",
            pa.array([127, 128], type=pa.int64()),
        )

        with self.assertRaises(pa.ArrowInvalid):
            validate_arrow_table(table, TEST_SCHEMA)

    def test_empty_frames_keep_the_contract(self) -> None:
        pandas_df = empty_pandas(TEST_SCHEMA)
        polars_df = empty_polars(TEST_SCHEMA)

        self.assertEqual(pandas_to_arrow(pandas_df, TEST_SCHEMA).schema, TEST_SCHEMA)
        self.assertEqual(polars_to_arrow(polars_df, TEST_SCHEMA).schema, TEST_SCHEMA)


if __name__ == "__main__":
    unittest.main()
