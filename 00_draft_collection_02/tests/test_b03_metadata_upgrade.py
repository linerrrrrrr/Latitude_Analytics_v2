"""隔离验证外部日历 metadata 兼容与境外期货描述差异不重写历史。"""

from __future__ import annotations

import hashlib
import json
import pathlib
import sys
import tempfile
import types
import unittest
from datetime import date, datetime, time, timedelta, timezone
from unittest import mock

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from click.testing import CliRunner


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
B03_ROOT = (
    PROJECT_ROOT
    / "02_Futures_Lakehouse"
    / "a03_External_Market_Data"
)
C01_NOTEBOOK_PATH = B03_ROOT / "b01_external_market_calendar.ipynb"
C03_NOTEBOOK_PATH = B03_ROOT / "b03_overseas_futures.ipynb"
C01_SKIPPED_CELL_IDS = {"30256ca1", "5dc7bd54"}
C03_SKIPPED_CELL_IDS = {"b03-c03-05", "b03-c03-21"}


def load_notebook_module(
    notebook_path: pathlib.Path,
    skipped_cell_ids: set[str],
    module_name: str,
) -> types.ModuleType:
    """直接执行 Notebook 权威代码单元格，不依赖尚待统一导出的 .py。"""
    notebook = json.loads(notebook_path.read_text(encoding="utf-8"))
    source = "\n\n".join(
        "".join(cell["source"])
        for cell in notebook["cells"]
        if cell["cell_type"] == "code"
        and cell["id"] not in skipped_cell_ids
    )
    module = types.ModuleType(module_name)
    module.__file__ = str(notebook_path)
    exec(compile(source, str(notebook_path), "exec"), module.__dict__)
    return module


def schema_with_stale_metadata(schema: pa.Schema) -> pa.Schema:
    """仅降低 metadata 版本，字段、顺序、类型与 nullable 保持完全兼容。"""
    metadata = dict(schema.metadata or {})
    metadata[b"schema_version"] = b"0.0.0-test"
    return pa.schema(list(schema), metadata=metadata)


def write_partitioned_table(
    module: types.ModuleType,
    frame: pd.DataFrame,
    current_schema: pa.Schema,
    partition_columns: list[str],
    table_path: pathlib.Path,
    *,
    write_schema: pa.Schema | None = None,
) -> None:
    """按生产 Hive 结构写入当前或 metadata 过期的隔离数据集。"""
    actual_schema = write_schema or current_schema
    current_table = module.pandas_to_arrow(
        frame.loc[:, current_schema.names],
        current_schema,
    )
    table = pa.Table.from_pylist(current_table.to_pylist(), schema=actual_schema)

    table_path.mkdir(parents=True)
    file_schema = pa.schema(
        [field for field in actual_schema if field.name not in partition_columns],
        metadata=actual_schema.metadata,
    )
    pq.write_table(
        pa.Table.from_batches([], schema=file_schema),
        table_path / "schema.parquet",
    )
    partitioning = ds.partitioning(
        pa.schema([actual_schema.field(name) for name in partition_columns]),
        flavor="hive",
    )
    if len(table):
        ds.write_dataset(
            table,
            table_path,
            format="parquet",
            partitioning=partitioning,
            existing_data_behavior="delete_matching",
            basename_template="part-{i}.parquet",
        )


def parquet_hashes(table_path: pathlib.Path) -> dict[str, str]:
    """记录逐文件摘要，用来证明交换失败后旧根逐字节恢复。"""
    return {
        str(path.relative_to(table_path)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(table_path.rglob("*.parquet"))
    }


def assert_no_recovery_paths(
    test_case: unittest.TestCase,
    silver_root: pathlib.Path,
    table_name: str,
) -> None:
    for suffix in ["staging", "backup", "failed"]:
        test_case.assertFalse(
            list(silver_root.glob(f".{table_name}.{suffix}-*")),
            msg=f"残留恢复路径：{suffix}",
        )


def build_trade_calendar_frame(module: types.ModuleType) -> pd.DataFrame:
    updated_at = datetime.now(timezone.utc) - timedelta(seconds=5)
    rows = []
    for calendar_date, is_trading_day in [
        (date(2026, 7, 31), True),
        (date(2026, 8, 1), False),
    ]:
        weekday = calendar_date.isoweekday()
        rows.append({
            "calendar_date": calendar_date,
            "date_key": calendar_date.strftime("%Y%m%d"),
            "is_trading_day": is_trading_day,
            "weekday": weekday,
            "is_weekend": weekday >= 6,
            "source": "JQData_get_trade_days",
            "calendar_name": "CN_FUTURES_MARKET",
            "calendar_timezone": "Asia/Shanghai",
            "effective_after": time(20, 0),
            "updated_at": updated_at,
            "year": calendar_date.year,
        })
    return module.arrow_to_pandas(
        module.pandas_to_arrow(
            pd.DataFrame(rows).loc[:, module.TRADE_CALENDAR_SCHEMA.names],
            module.TRADE_CALENDAR_SCHEMA,
        ),
        module.TRADE_CALENDAR_SCHEMA,
    )


def build_legacy_domestic_migration_frames(
    module: types.ModuleType,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """构造物理兼容、但仍携带旧结构化基差完成语义的三种历史状态。"""
    checked_at = datetime.now(timezone.utc) - timedelta(seconds=5)
    upstream_rows = []
    legacy_rows = []
    legacy_states = [
        (date(2026, 8, 10), "success", False, 8, "passed"),
        (date(2026, 8, 11), "success", False, 6, "warning"),
        (date(2026, 8, 12), "empty_confirmed", True, 0, "passed"),
    ]

    for observation_date, status, is_missing, record_count, quality in legacy_states:
        weekday = observation_date.isoweekday()
        upstream_rows.append({
            "calendar_date": observation_date,
            "date_key": observation_date.strftime("%Y%m%d"),
            "is_trading_day": True,
            "weekday": weekday,
            "is_weekend": weekday >= 6,
            "source": "JQData_get_trade_days",
            "calendar_name": "CN_FUTURES_MARKET",
            "calendar_timezone": "Asia/Shanghai",
            "effective_after": time(20, 0),
            "updated_at": checked_at,
            "year": observation_date.year,
        })
        legacy_rows.append({
            "dataset_name": "domestic_spot_basis",
            "entity_code": "ALL",
            "observation_date": observation_date,
            "is_fetch_required": True,
            "requirement_reason": (
                "外部市场请求实体配置 v1.0.0；"
                "中国期货交易日，按日期请求并解析生意社现货基差页面。"
            ),
            "is_fetch_completed": True,
            "fetch_result_status": status,
            "is_data_missing": is_missing,
            "actual_record_count": record_count,
            "quality_status": quality,
            "quality_reason": "旧结构化现货基差事实已经提交并复读。",
            "fetch_run_id": "legacy-structured-fact",
            "fetch_completed_at": checked_at,
            "quality_checked_at": checked_at,
            "updated_at": checked_at,
            "year": observation_date.year,
            "month": observation_date.month,
        })

    upstream_df = module.arrow_to_pandas(
        module.pandas_to_arrow(
            pd.DataFrame(upstream_rows).loc[:, module.TRADE_CALENDAR_SCHEMA.names],
            module.TRADE_CALENDAR_SCHEMA,
        ),
        module.TRADE_CALENDAR_SCHEMA,
    )
    legacy_df = module.arrow_to_pandas(
        module.pandas_to_arrow(
            pd.DataFrame(legacy_rows).loc[
                :, module.EXTERNAL_MARKET_CALENDAR_SCHEMA.names
            ],
            module.EXTERNAL_MARKET_CALENDAR_SCHEMA,
        ),
        module.EXTERNAL_MARKET_CALENDAR_SCHEMA,
    )
    return upstream_df, legacy_df


def build_overseas_fact_frame(module: types.ModuleType) -> pd.DataFrame:
    updated_at = datetime.now(timezone.utc) - timedelta(seconds=5)
    frames = []
    for source_id, instrument_code, snapshot_date, opening_price in [
        ("july-id", "JULY", date(2026, 7, 31), 100.0),
        ("august-id", "AUGUST", date(2026, 8, 3), 200.0),
    ]:
        raw_df = pd.DataFrame([{
            "id": source_id,
            "code": instrument_code,
            "name": f"{instrument_code}境外期货",
            "day": snapshot_date,
            "open": opening_price,
            "close": opening_price + 1.0,
            "low": opening_price - 1.0,
            "high": opening_price + 2.0,
            "volume": 10.0,
            "change_pct": 1.0,
            "amplitude": 3.0,
            "pre_close": opening_price,
        }])
        frames.append(module.normalize_overseas_futures_response(
            raw_df,
            snapshot_date,
            updated_at,
        ))
    return module.validate_overseas_futures_frame(
        pd.concat(frames, ignore_index=True),
        "隔离测试",
    )


def build_completed_overseas_calendar(
    module: types.ModuleType,
    fact_df: pd.DataFrame,
) -> pd.DataFrame:
    completed_at = datetime.now(timezone.utc) - timedelta(seconds=2)
    rows = []
    for snapshot_date, actual_count in module.grid_count_map(fact_df).items():
        status, is_missing, quality, reason = module.calendar_completion_result(
            actual_count,
            None,
        )
        rows.append({
            "dataset_name": module.DATASET_NAME,
            "entity_code": module.ENTITY_CODE,
            "observation_date": snapshot_date,
            "is_fetch_required": True,
            "requirement_reason": "隔离测试 required 格点",
            "is_fetch_completed": True,
            "fetch_result_status": status,
            "is_data_missing": is_missing,
            "actual_record_count": actual_count,
            "quality_status": quality,
            "quality_reason": reason,
            "fetch_run_id": "already-complete",
            "fetch_completed_at": completed_at,
            "quality_checked_at": completed_at,
            "updated_at": completed_at,
            "year": snapshot_date.year,
            "month": snapshot_date.month,
        })
    return module.arrow_to_pandas(
        module.pandas_to_arrow(pd.DataFrame(rows), module.EXTERNAL_MARKET_CALENDAR_SCHEMA),
        module.EXTERNAL_MARKET_CALENDAR_SCHEMA,
    )


class ExternalCalendarMetadataUpgradeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.module = load_notebook_module(
            C01_NOTEBOOK_PATH,
            C01_SKIPPED_CELL_IDS,
            "test_b03_c01_metadata_notebook",
        )
        cls.upstream_df = build_trade_calendar_frame(cls.module)
        cls.expected_df = cls.module.build_expected_calendar(
            cls.upstream_df,
            cls.module.empty_pandas(cls.module.EXTERNAL_MARKET_CALENDAR_SCHEMA),
            datetime.now(timezone.utc) - timedelta(seconds=4),
        )

    def prepare_lake(self, lake_root: pathlib.Path) -> tuple[pathlib.Path, pathlib.Path]:
        silver_root = lake_root / "silver"
        upstream_path = silver_root / self.module.UPSTREAM_TABLE_NAME
        target_path = silver_root / self.module.TABLE_NAME
        write_partitioned_table(
            self.module,
            self.upstream_df,
            self.module.TRADE_CALENDAR_SCHEMA,
            self.module.UPSTREAM_PARTITION_COLUMNS,
            upstream_path,
        )
        write_partitioned_table(
            self.module,
            self.expected_df,
            self.module.EXTERNAL_MARKET_CALENDAR_SCHEMA,
            self.module.PARTITION_COLUMNS,
            target_path,
            write_schema=schema_with_stale_metadata(
                self.module.EXTERNAL_MARKET_CALENDAR_SCHEMA
            ),
        )
        return silver_root, target_path

    def test_cli_upgrades_multiple_partitions_with_full_root_swap(self) -> None:
        with tempfile.TemporaryDirectory(prefix="b03-c01-metadata-") as directory:
            lake_root = pathlib.Path(directory)
            silver_root, target_path = self.prepare_lake(lake_root)

            result = CliRunner().invoke(
                self.module.main,
                (
                    "--lake-root",
                    str(lake_root),
                    "--start-date",
                    "2026-07-31",
                    "--end-date",
                    "2026-08-01",
                    "--write",
                ),
            )

            self.assertEqual(
                result.exit_code,
                0,
                msg=f"output={result.output!r}; exception={result.exception!r}",
            )
            self.assertIn("metadata_upgrade_required=true", result.output)
            self.assertIn("full_root_swap=true", result.output)
            upgraded = self.module.open_exact_dataset(
                target_path,
                self.module.CALENDAR_PARTITIONING,
                self.module.EXTERNAL_MARKET_CALENDAR_SCHEMA,
                "升级后的隔离日历",
            )
            upgraded_df = self.module.validate_external_calendar_table(
                upgraded.to_table(
                    columns=self.module.EXTERNAL_MARKET_CALENDAR_SCHEMA.names
                ),
                "升级后的隔离",
            )
            self.assertEqual(
                self.module.table_digest(self.module.pandas_to_arrow(upgraded_df, self.module.EXTERNAL_MARKET_CALENDAR_SCHEMA)),
                self.module.table_digest(self.module.pandas_to_arrow(self.expected_df, self.module.EXTERNAL_MARKET_CALENDAR_SCHEMA)),
            )
            self.assertGreaterEqual(len(list(target_path.glob("*/*/*"))), 2)
            self.assertFalse(list(silver_root.glob(".a03-b01-*")))

    def test_cli_migrates_legacy_domestic_states_then_enforces_raw_contract(self) -> None:
        upstream_df, legacy_df = build_legacy_domestic_migration_frames(self.module)

        # 严格生产校验继续拒绝旧语义；宽容入口只允许 main 读取历史 existing。
        with self.assertRaises(ValueError):
            self.module.validate_external_calendar_table(
                self.module.pandas_to_arrow(
                    legacy_df,
                    self.module.EXTERNAL_MARKET_CALENDAR_SCHEMA,
                ),
                "严格生产输出",
            )

        with tempfile.TemporaryDirectory(prefix="b03-c01-legacy-domestic-") as directory:
            lake_root = pathlib.Path(directory)
            silver_root = lake_root / "silver"
            upstream_path = silver_root / self.module.UPSTREAM_TABLE_NAME
            target_path = silver_root / self.module.TABLE_NAME
            write_partitioned_table(
                self.module,
                upstream_df,
                self.module.TRADE_CALENDAR_SCHEMA,
                self.module.UPSTREAM_PARTITION_COLUMNS,
                upstream_path,
            )
            write_partitioned_table(
                self.module,
                legacy_df,
                self.module.EXTERNAL_MARKET_CALENDAR_SCHEMA,
                self.module.PARTITION_COLUMNS,
                target_path,
                write_schema=schema_with_stale_metadata(
                    self.module.EXTERNAL_MARKET_CALENDAR_SCHEMA
                ),
            )

            result = CliRunner().invoke(
                self.module.main,
                (
                    "--lake-root",
                    str(lake_root),
                    "--start-date",
                    "2026-08-10",
                    "--end-date",
                    "2026-08-12",
                    "--write",
                ),
            )
            self.assertEqual(
                result.exit_code,
                0,
                msg=f"output={result.output!r}; exception={result.exception!r}",
            )

            migrated_dataset = self.module.open_exact_dataset(
                target_path,
                self.module.CALENDAR_PARTITIONING,
                self.module.EXTERNAL_MARKET_CALENDAR_SCHEMA,
                "旧状态迁移后的隔离日历",
            )
            migrated_df = self.module.validate_external_calendar_table(
                migrated_dataset.to_table(
                    columns=self.module.EXTERNAL_MARKET_CALENDAR_SCHEMA.names
                ),
                "旧状态迁移后的严格生产输出",
            )
            domestic_df = migrated_df.loc[
                migrated_df["dataset_name"].eq("domestic_spot_basis")
                & migrated_df["is_fetch_required"]
            ].sort_values("observation_date")

            self.assertEqual(len(domestic_df), 3)
            self.assertEqual(set(domestic_df["fetch_result_status"]), {"pending"})
            self.assertEqual(set(domestic_df["quality_status"]), {"pending"})
            self.assertEqual(set(domestic_df["actual_record_count"]), {0})
            self.assertFalse(domestic_df["is_fetch_completed"].any())
            self.assertFalse(domestic_df["is_data_missing"].any())
            self.assertTrue(
                domestic_df["requirement_reason"].str.contains("原样归档").all()
            )
            self.assertFalse(list(silver_root.glob(".a03-b01-*")))

    def test_full_root_swap_failure_restores_old_tree_byte_for_byte(self) -> None:
        with tempfile.TemporaryDirectory(prefix="b03-c01-rollback-") as directory:
            lake_root = pathlib.Path(directory)
            silver_root, target_path = self.prepare_lake(lake_root)
            before_hashes = parquet_hashes(target_path)
            partition_keys = sorted(set(
                self.expected_df[self.module.PARTITION_COLUMNS].itertuples(
                    index=False,
                    name=None,
                )
            ))
            transaction_module = sys.modules[self.module.StagedPathTransaction.__module__]
            real_move = transaction_module.os.replace

            def fail_new_root_move(source: str, destination: str) -> str:
                if pathlib.Path(source).name.startswith(".a03-b01-s-") and pathlib.Path(destination) == target_path:
                    raise RuntimeError("injected full-root swap failure")
                return real_move(source, destination)

            with mock.patch.object(
                transaction_module.os,
                "replace",
                side_effect=fail_new_root_move,
            ):
                with self.assertRaisesRegex(RuntimeError, "injected"):
                    self.module.commit_partitions(
                        self.expected_df,
                        partition_keys,
                        lake_root,
                        force_full_swap=True,
                    )

            self.assertEqual(parquet_hashes(target_path), before_hashes)
            _, is_exact = self.module.open_compatible_dataset(
                target_path,
                self.module.CALENDAR_PARTITIONING,
                self.module.EXTERNAL_MARKET_CALENDAR_SCHEMA,
                self.module.PARTITION_COLUMNS,
                "回滚后的隔离日历",
            )
            self.assertFalse(is_exact)
            self.assertFalse(list(silver_root.glob(".a03-b01-*")))

    def test_first_old_root_move_failure_never_touches_formal_target(self) -> None:
        with tempfile.TemporaryDirectory(prefix="b03-c01-first-move-") as directory:
            lake_root = pathlib.Path(directory)
            silver_root, target_path = self.prepare_lake(lake_root)
            before_hashes = parquet_hashes(target_path)
            partition_keys = sorted(set(
                self.expected_df[self.module.PARTITION_COLUMNS].itertuples(
                    index=False,
                    name=None,
                )
            ))
            transaction_module = sys.modules[self.module.StagedPathTransaction.__module__]
            real_move = transaction_module.os.replace

            def fail_old_root_move(source: str, destination: str) -> str:
                if (
                    pathlib.Path(source) == target_path
                    and pathlib.Path(destination).parent.name.startswith(".a03-b01-b-")
                ):
                    raise RuntimeError("injected first old-root move failure")
                return real_move(source, destination)

            with mock.patch.object(
                transaction_module.os,
                "replace",
                side_effect=fail_old_root_move,
            ):
                with self.assertRaisesRegex(RuntimeError, "injected first"):
                    self.module.commit_partitions(
                        self.expected_df,
                        partition_keys,
                        lake_root,
                        force_full_swap=True,
                    )

            self.assertTrue(target_path.is_dir())
            self.assertEqual(parquet_hashes(target_path), before_hashes)
            _, is_exact = self.module.open_compatible_dataset(
                target_path,
                self.module.CALENDAR_PARTITIONING,
                self.module.EXTERNAL_MARKET_CALENDAR_SCHEMA,
                self.module.PARTITION_COLUMNS,
                "首次移动失败后的隔离日历",
            )
            self.assertFalse(is_exact)
            self.assertFalse(list(silver_root.glob(".a03-b01-*")))


class OverseasFactMetadataUpgradeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.module = load_notebook_module(
            C03_NOTEBOOK_PATH,
            C03_SKIPPED_CELL_IDS,
            "test_b03_c03_metadata_notebook",
        )
        cls.fact_df = build_overseas_fact_frame(cls.module)
        cls.calendar_df = build_completed_overseas_calendar(cls.module, cls.fact_df)

    def prepare_lake(self, lake_root: pathlib.Path) -> tuple[pathlib.Path, pathlib.Path]:
        silver_root = lake_root / "silver"
        calendar_path = silver_root / self.module.CALENDAR_TABLE_NAME
        fact_path = silver_root / self.module.TABLE_NAME
        write_partitioned_table(
            self.module,
            self.calendar_df,
            self.module.EXTERNAL_MARKET_CALENDAR_SCHEMA,
            self.module.CALENDAR_PARTITION_COLUMNS,
            calendar_path,
        )
        write_partitioned_table(
            self.module,
            self.fact_df,
            self.module.OVERSEAS_FUTURES_DAILY_SCHEMA,
            self.module.PARTITION_COLUMNS,
            fact_path,
            write_schema=schema_with_stale_metadata(
                self.module.OVERSEAS_FUTURES_DAILY_SCHEMA
            ),
        )
        return silver_root, fact_path

    def test_description_drift_is_read_without_api_or_any_rewrite(self) -> None:
        with tempfile.TemporaryDirectory(prefix="overseas-description-") as directory:
            lake_root = pathlib.Path(directory)
            silver_root, fact_path = self.prepare_lake(lake_root)
            before = parquet_hashes(silver_root)
            with mock.patch.object(self.module, "authenticate_jqdata", side_effect=AssertionError("no API")) as authentication:
                result = CliRunner().invoke(self.module.main, ["--lake-root", str(lake_root), "--write"])
            self.assertEqual(result.exit_code, 0, result.output)
            authentication.assert_not_called()
            self.assertIn("outcome=up_to_date", result.output)
            self.assertEqual(before, parquet_hashes(silver_root))
            self.assertEqual(len(self.module.read_optional_fact(fact_path)), 2)
            assert_no_recovery_paths(self, silver_root, self.module.TABLE_NAME)

    def test_identity_metadata_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="overseas-identity-") as directory:
            lake_root = pathlib.Path(directory)
            _, fact_path = self.prepare_lake(lake_root)
            leaf_file = next(fact_path.glob("year=*/month=*/*.parquet"))
            table = pq.ParquetFile(leaf_file).read()
            metadata = dict(table.schema.metadata)
            metadata[b"table_name"] = b"wrong_table"
            pq.write_table(table.replace_schema_metadata(metadata), leaf_file)
            with self.assertRaisesRegex(TypeError, "表身份 metadata"):
                self.module.read_optional_fact(fact_path)

    def test_physical_nullability_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="overseas-nullability-") as directory:
            lake_root = pathlib.Path(directory)
            _, fact_path = self.prepare_lake(lake_root)
            leaf_file = next(fact_path.glob("year=*/month=*/*.parquet"))
            table = pq.ParquetFile(leaf_file).read()
            fields = [pa.field(field.name, field.type, nullable=True, metadata=field.metadata)
                      if field.name == "instrument_code" else field for field in table.schema]
            pq.write_table(table.cast(pa.schema(fields, metadata=table.schema.metadata)), leaf_file)
            with self.assertRaisesRegex(TypeError, "物理字段、类型或 nullable"):
                self.module.read_optional_fact(fact_path)


if __name__ == "__main__":
    unittest.main()
