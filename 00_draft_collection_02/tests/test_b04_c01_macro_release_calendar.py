from __future__ import annotations

import pathlib
import tempfile
import types
import unittest
from datetime import date, datetime, timedelta, timezone
from unittest import mock

import nbformat
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from click.testing import CliRunner
from nbconvert.exporters import PythonExporter


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
NOTEBOOK_PATH = (
    PROJECT_ROOT
    / "02_Futures_Lakehouse"
    / "a04_Macro_And_Interest_Rates"
    / "b01_macro_release_calendar.ipynb"
)


def load_notebook_module() -> types.ModuleType:
    notebook = nbformat.read(NOTEBOOK_PATH, as_version=4)
    nbformat.validate(notebook)
    source, _ = PythonExporter().from_notebook_node(notebook)
    module = types.ModuleType("test_b04_b01_macro_release_calendar_notebook")
    module.__file__ = str(NOTEBOOK_PATH.with_suffix(".py"))
    exec(compile(source, module.__file__, "exec"), module.__dict__)
    return module


def stale_metadata_schema(schema: pa.Schema) -> pa.Schema:
    metadata = dict(schema.metadata or {})
    metadata[b"schema_version"] = b"1.0.0"
    return pa.schema(list(schema), metadata=metadata)


def write_partitioned_calendar(
    module: types.ModuleType,
    frame: pd.DataFrame,
    target_path: pathlib.Path,
    *,
    write_schema: pa.Schema | None = None,
) -> None:
    schema = write_schema or module.MACRO_RELEASE_CALENDAR_SCHEMA
    target_path.mkdir(parents=True, exist_ok=True)
    file_schema = pa.schema(
        [
            field
            for field in schema
            if field.name not in module.PARTITION_COLUMNS
        ],
        metadata=schema.metadata,
    )
    pq.write_table(
        pa.Table.from_batches([], schema=file_schema),
        target_path / "schema.parquet",
    )
    if frame.empty:
        return

    current_table = module.pandas_to_arrow(
        frame.loc[:, module.MACRO_RELEASE_CALENDAR_SCHEMA.names],
        module.MACRO_RELEASE_CALENDAR_SCHEMA,
    )
    table = pa.Table.from_pylist(current_table.to_pylist(), schema=schema)
    ds.write_dataset(
        table,
        target_path,
        format="parquet",
        partitioning=module.CALENDAR_PARTITIONING,
        existing_data_behavior="delete_matching",
        basename_template="part-{i}.parquet",
    )


def recovery_paths(
    silver_root: pathlib.Path,
    table_name: str,
) -> list[pathlib.Path]:
    return sorted(
        path
        for path in silver_root.iterdir()
        if path.is_dir()
        and path.name.startswith(f".{table_name}.")
        and any(
            marker in path.name
            for marker in (".staging-", ".backup-", ".failed-")
        )
    )


class MacroReleaseCalendarTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.module = load_notebook_module()

    def build_frame(
        self,
        start_date: date = date(2026, 7, 17),
        end_date: date = date(2026, 8, 17),
        visible_date: date = date(2026, 8, 17),
    ) -> pd.DataFrame:
        return self.module.build_expected_calendar(
            start_date,
            end_date,
            self.module.empty_pandas(
                self.module.MACRO_RELEASE_CALENDAR_SCHEMA
            ),
            visible_date,
            datetime.now(timezone.utc) - timedelta(seconds=5),
        )

    def test_versioned_availability_rules(self) -> None:
        series = self.module.MACRO_RELEASE_SERIES_BY_KEY
        self.assertEqual(
            self.module.calculate_expected_available_date(
                series[("interest_rate", "SHIBOR_ON")],
                date(2026, 8, 17),
            ),
            date(2026, 8, 17),
        )
        self.assertEqual(
            self.module.calculate_expected_available_date(
                series[("macro_release", "CPI_NATIONAL_YOY")],
                date(2026, 7, 31),
            ),
            date(2026, 8, 10),
        )
        self.assertEqual(
            self.module.calculate_expected_available_date(
                series[("macro_release", "PMI_MANUFACTURING")],
                date(2026, 2, 28),
            ),
            date(2026, 3, 4),
        )
        self.assertEqual(
            self.module.calculate_expected_available_date(
                series[("macro_release", "GDP_YOY")],
                date(2026, 6, 30),
            ),
            date(2026, 7, 16),
        )

    def test_builds_frequency_grids_and_not_required_states(self) -> None:
        frame = self.module.build_expected_calendar(
            date(2026, 7, 1),
            date(2026, 7, 31),
            self.module.empty_pandas(
                self.module.MACRO_RELEASE_CALENDAR_SCHEMA
            ),
            date(2026, 8, 5),
            datetime.now(timezone.utc) - timedelta(seconds=5),
        )
        self.assertEqual(len(frame), 197)

        cpi = frame.loc[frame["series_code"].eq("CPI_NATIONAL_YOY")].iloc[0]
        self.assertFalse(cpi["is_fetch_required"])
        self.assertEqual(cpi["fetch_result_status"], "not_required")
        self.assertEqual(cpi["quality_status"], "not_applicable")

        pmi = frame.loc[frame["series_code"].eq("PMI_MANUFACTURING")].iloc[0]
        self.assertTrue(pmi["is_fetch_required"])
        self.assertEqual(pmi["fetch_result_status"], "pending")
        self.assertEqual(pmi["quality_status"], "pending")

        shibor_dates = frame.loc[
            frame["dataset_name"].eq("interest_rate"),
            "report_date",
        ]
        self.assertTrue(all(value.weekday() < 5 for value in shibor_dates))

    def test_unchanged_policy_inherits_fact_state_and_changed_policy_resets(self) -> None:
        base = self.module.build_expected_calendar(
            date(2026, 8, 17),
            date(2026, 8, 17),
            self.module.empty_pandas(
                self.module.MACRO_RELEASE_CALENDAR_SCHEMA
            ),
            date(2026, 8, 17),
            datetime.now(timezone.utc) - timedelta(seconds=10),
        )
        completed_at = datetime.now(timezone.utc) - timedelta(seconds=5)
        mask = base["series_code"].eq("SHIBOR_ON")
        base.loc[mask, "is_fetch_completed"] = True
        base.loc[mask, "fetch_result_status"] = "success"
        base.loc[mask, "actual_record_count"] = 1
        base.loc[mask, "quality_status"] = "passed"
        base.loc[mask, "quality_reason"] = "隔离测试正式事实已复读 1 行。"
        base.loc[mask, "fetch_run_id"] = "fact-complete"
        base.loc[mask, "fetch_completed_at"] = completed_at
        base.loc[mask, "quality_checked_at"] = completed_at
        base.loc[mask, "updated_at"] = completed_at
        base = self.module.validate_macro_calendar_table(
            self.module.pandas_to_arrow(
                base,
                self.module.MACRO_RELEASE_CALENDAR_SCHEMA,
            ),
            "隔离测试完成状态",
        )

        inherited = self.module.build_expected_calendar(
            date(2026, 8, 17),
            date(2026, 8, 17),
            base,
            date(2026, 8, 17),
            datetime.now(timezone.utc),
        )
        inherited_row = inherited.loc[
            inherited["series_code"].eq("SHIBOR_ON")
        ].iloc[0]
        self.assertEqual(inherited_row["fetch_result_status"], "success")
        self.assertEqual(inherited_row["actual_record_count"], 1)

        legacy = base.copy()
        legacy.loc[mask, "requirement_reason"] = "旧版未版本化规则。"
        reset = self.module.build_expected_calendar(
            date(2026, 8, 17),
            date(2026, 8, 17),
            legacy,
            date(2026, 8, 17),
            datetime.now(timezone.utc),
        )
        reset_row = reset.loc[reset["series_code"].eq("SHIBOR_ON")].iloc[0]
        self.assertEqual(reset_row["fetch_result_status"], "pending")
        self.assertEqual(reset_row["actual_record_count"], 0)
        self.assertTrue(pd.isna(reset_row["fetch_run_id"]))

    def test_explicit_formal_write_gate_precedes_lake_io(self) -> None:
        formal_root = pathlib.Path("E:/formal-macro-lake").resolve()
        fake_settings = types.SimpleNamespace(
            futures_lake_root=formal_root,
            futures_data_start_date=date(2026, 7, 17),
        )
        with (
            mock.patch.object(self.module, "settings", fake_settings),
            mock.patch.object(
                self.module,
                "open_compatible_dataset",
                side_effect=AssertionError("日期门禁后不应读取湖"),
            ) as open_mock,
        ):
            result = CliRunner().invoke(
                self.module.main,
                (
                    "--start-date",
                    "2026-08-01",
                    "--end-date",
                    "2026-08-02",
                    "--write",
                ),
            )
        self.assertEqual(result.exit_code, 2)
        self.assertIn("显式指定日期时禁止写入", result.output)
        open_mock.assert_not_called()

    def test_empty_lake_build_and_second_run_are_idempotent(self) -> None:
        with tempfile.TemporaryDirectory(prefix="b04-c01-empty-") as directory:
            lake_root = pathlib.Path(directory)
            fake_settings = types.SimpleNamespace(
                futures_lake_root=lake_root,
                futures_data_start_date=date(2026, 7, 17),
            )
            with mock.patch.object(self.module, "settings", fake_settings):
                first = CliRunner().invoke(self.module.main, ("--write",))
                second = CliRunner().invoke(self.module.main, ("--write",))

            self.assertEqual(
                first.exit_code,
                0,
                msg=f"output={first.output!r}; exception={first.exception!r}",
            )
            self.assertEqual(
                second.exit_code,
                0,
                msg=f"output={second.output!r}; exception={second.exception!r}",
            )
            self.assertIn("已经与当前理论水位", second.output)

            target_path = lake_root / "silver" / self.module.TABLE_NAME
            dataset = self.module.open_exact_dataset(
                target_path,
                "隔离空湖建表结果",
            )
            frame = self.module.validate_macro_calendar_table(
                dataset.to_table(
                    columns=self.module.MACRO_RELEASE_CALENDAR_SCHEMA.names
                ),
                "隔离空湖建表结果",
            )
            self.assertFalse(frame.empty)
            self.assertFalse(
                recovery_paths(lake_root / "silver", self.module.TABLE_NAME)
            )

    def test_empty_theoretical_scope_creates_zero_row_contract_dataset(self) -> None:
        with tempfile.TemporaryDirectory(prefix="b04-c01-zero-") as directory:
            base_path = pathlib.Path(directory)
            formal_root = base_path / "formal"
            test_root = base_path / "test"
            fake_settings = types.SimpleNamespace(
                futures_lake_root=formal_root,
                futures_data_start_date=date(2026, 7, 17),
            )
            with mock.patch.object(self.module, "settings", fake_settings):
                result = CliRunner().invoke(
                    self.module.main,
                    (
                        "--lake-root",
                        str(test_root),
                        "--start-date",
                        "2026-07-01",
                        "--end-date",
                        "2026-07-02",
                        "--write",
                    ),
                )

            self.assertEqual(
                result.exit_code,
                0,
                msg=f"output={result.output!r}; exception={result.exception!r}",
            )
            self.assertIn("initial_dataset_required=true", result.output)
            target_path = test_root / "silver" / self.module.TABLE_NAME
            dataset = self.module.open_exact_dataset(
                target_path,
                "0 行宏观发布日历",
            )
            frame = self.module.validate_macro_calendar_table(
                dataset.to_table(
                    columns=self.module.MACRO_RELEASE_CALENDAR_SCHEMA.names
                ),
                "0 行宏观发布日历",
            )
            self.assertTrue(frame.empty)
            self.assertFalse(
                recovery_paths(test_root / "silver", self.module.TABLE_NAME)
            )

    def test_legacy_policy_and_metadata_are_migrated_by_automatic_run(self) -> None:
        current = self.build_frame()
        legacy = current.loc[
            current["series_code"].eq("CPI_NATIONAL_YOY")
            & current["report_date"].eq(date(2026, 7, 31))
        ].copy()
        self.assertEqual(len(legacy), 1)
        legacy.loc[:, "is_fetch_required"] = False
        legacy.loc[:, "requirement_reason"] = "旧 availability-v1.0.0 规则。"
        legacy.loc[:, "fetch_result_status"] = "pending"
        legacy.loc[:, "quality_status"] = "pending"
        legacy.loc[:, "quality_reason"] = "等待采集。"

        with tempfile.TemporaryDirectory(prefix="b04-c01-legacy-") as directory:
            lake_root = pathlib.Path(directory)
            target_path = lake_root / "silver" / self.module.TABLE_NAME
            write_partitioned_calendar(
                self.module,
                legacy,
                target_path,
                write_schema=stale_metadata_schema(
                    self.module.MACRO_RELEASE_CALENDAR_SCHEMA
                ),
            )
            fake_settings = types.SimpleNamespace(
                futures_lake_root=lake_root,
                futures_data_start_date=date(2026, 7, 17),
            )
            with mock.patch.object(self.module, "settings", fake_settings):
                result = CliRunner().invoke(self.module.main, ("--write",))

            self.assertEqual(
                result.exit_code,
                0,
                msg=f"output={result.output!r}; exception={result.exception!r}",
            )
            self.assertIn("metadata_upgrade_required=true", result.output)
            migrated_dataset = self.module.open_exact_dataset(
                target_path,
                "迁移后的宏观发布日历",
            )
            migrated_df = self.module.validate_macro_calendar_table(
                migrated_dataset.to_table(
                    columns=self.module.MACRO_RELEASE_CALENDAR_SCHEMA.names
                ),
                "迁移后的宏观发布日历",
            )
            migrated_row = migrated_df.loc[
                migrated_df["series_code"].eq("CPI_NATIONAL_YOY")
                & migrated_df["report_date"].eq(date(2026, 7, 31))
            ].iloc[0]
            self.assertTrue(migrated_row["is_fetch_required"])
            self.assertEqual(migrated_row["fetch_result_status"], "pending")
            self.assertFalse(
                recovery_paths(lake_root / "silver", self.module.TABLE_NAME)
            )

    def test_current_metadata_policy_damage_is_not_silently_migrated(self) -> None:
        current = self.build_frame()
        damaged = current.loc[
            current["series_code"].eq("CPI_NATIONAL_YOY")
            & current["report_date"].eq(date(2026, 7, 31))
        ].copy()
        damaged.loc[:, "requirement_reason"] = "当前 metadata 下的损坏策略。"

        with tempfile.TemporaryDirectory(prefix="b04-c01-damaged-") as directory:
            lake_root = pathlib.Path(directory)
            target_path = lake_root / "silver" / self.module.TABLE_NAME
            write_partitioned_calendar(
                self.module,
                damaged,
                target_path,
            )
            fake_settings = types.SimpleNamespace(
                futures_lake_root=lake_root,
                futures_data_start_date=date(2026, 7, 17),
            )
            with mock.patch.object(self.module, "settings", fake_settings):
                result = CliRunner().invoke(self.module.main, ("--write",))

            self.assertEqual(result.exit_code, 1)
            self.assertIsInstance(result.exception, ValueError)
            self.assertIn("不得把正式数据损坏静默当作旧契约迁移", str(result.exception))
            persisted_dataset = self.module.open_exact_dataset(
                target_path,
                "损坏策略拒绝后的原正式表",
            )
            persisted_df = self.module.validate_macro_calendar_table(
                persisted_dataset.to_table(
                    columns=self.module.MACRO_RELEASE_CALENDAR_SCHEMA.names
                ),
                "损坏策略拒绝后的原正式表",
                allow_legacy_policy=True,
            )
            self.assertEqual(
                persisted_df.iloc[0]["requirement_reason"],
                "当前 metadata 下的损坏策略。",
            )
            self.assertFalse(
                recovery_paths(lake_root / "silver", self.module.TABLE_NAME)
            )

    def test_full_swap_first_move_failure_keeps_old_root(self) -> None:
        old_frame = self.build_frame()
        changed_frame = old_frame.copy()
        changed_frame.loc[changed_frame.index[0], "quality_reason"] = (
            "隔离测试的新质量说明。"
        )

        with tempfile.TemporaryDirectory(prefix="b04-c01-root-rollback-") as directory:
            lake_root = pathlib.Path(directory)
            silver_root = lake_root / "silver"
            target_path = silver_root / self.module.TABLE_NAME
            write_partitioned_calendar(
                self.module,
                old_frame,
                target_path,
            )
            old_digest = self.module.table_digest(old_frame)
            real_move = self.module.shutil.move

            def fail_old_root_move(source: str, destination: str) -> str:
                if pathlib.Path(source) == target_path:
                    raise OSError("injected old-root move failure")
                return real_move(source, destination)

            with mock.patch.object(
                self.module.shutil,
                "move",
                side_effect=fail_old_root_move,
            ):
                with self.assertRaisesRegex(OSError, "injected"):
                    self.module.commit_partitions(
                        changed_frame,
                        self.module.changed_partition_keys(
                            changed_frame,
                            old_frame,
                        ),
                        lake_root,
                        force_full_swap=True,
                    )

            restored_dataset = self.module.open_exact_dataset(
                target_path,
                "首步失败后的旧正式根",
            )
            restored_df = self.module.validate_macro_calendar_table(
                restored_dataset.to_table(
                    columns=self.module.MACRO_RELEASE_CALENDAR_SCHEMA.names
                ),
                "首步失败后的旧正式根",
            )
            self.assertEqual(self.module.table_digest(restored_df), old_digest)
            self.assertFalse(recovery_paths(silver_root, self.module.TABLE_NAME))

    def test_partial_leaf_second_move_failure_restores_old_leaf(self) -> None:
        old_frame = self.build_frame()
        changed_frame = old_frame.copy()
        changed_frame.loc[changed_frame.index[0], "quality_reason"] = (
            "隔离测试的叶分区新质量说明。"
        )
        partition_keys = self.module.changed_partition_keys(
            changed_frame,
            old_frame,
        )
        self.assertEqual(len(partition_keys), 1)

        with tempfile.TemporaryDirectory(prefix="b04-c01-leaf-rollback-") as directory:
            lake_root = pathlib.Path(directory)
            silver_root = lake_root / "silver"
            target_path = silver_root / self.module.TABLE_NAME
            write_partitioned_calendar(
                self.module,
                old_frame,
                target_path,
            )
            old_digest = self.module.table_digest(old_frame)
            real_move = self.module.shutil.move
            injected = False

            def fail_staging_leaf_move(source: str, destination: str) -> str:
                nonlocal injected
                source_path = pathlib.Path(source)
                destination_path = pathlib.Path(destination)
                if (
                    not injected
                    and ".staging-" in source_path.as_posix()
                    and self.module.TABLE_NAME in destination_path.as_posix()
                ):
                    injected = True
                    raise OSError("injected staging-leaf move failure")
                return real_move(source, destination)

            with mock.patch.object(
                self.module.shutil,
                "move",
                side_effect=fail_staging_leaf_move,
            ):
                with self.assertRaisesRegex(OSError, "injected"):
                    self.module.commit_partitions(
                        changed_frame,
                        partition_keys,
                        lake_root,
                    )

            restored_dataset = self.module.open_exact_dataset(
                target_path,
                "第二步失败后的旧正式叶",
            )
            restored_df = self.module.validate_macro_calendar_table(
                restored_dataset.to_table(
                    columns=self.module.MACRO_RELEASE_CALENDAR_SCHEMA.names
                ),
                "第二步失败后的旧正式叶",
            )
            self.assertEqual(self.module.table_digest(restored_df), old_digest)
            self.assertFalse(recovery_paths(silver_root, self.module.TABLE_NAME))


if __name__ == "__main__":
    unittest.main()
