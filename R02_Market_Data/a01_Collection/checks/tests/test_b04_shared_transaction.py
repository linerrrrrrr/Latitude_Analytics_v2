"""b04 共享事务集成：临时文件、主动注入安装/验收失败，不假设正式文件已损坏。"""

import contextlib
import hashlib
import importlib.util
import io
import pathlib
import sys
import tempfile
import unittest
from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock, patch

import nbformat
import pandas as pd
from click.testing import CliRunner


project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()
for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        ROOT = candidate_root
        sys.path.insert(0, str(candidate_root))
        break
else:
    raise RuntimeError("未找到项目根目录")
NOTEBOOK = ROOT / "R02_Market_Data/a01_Collection/b01_Futures_Market_Data/c04_futures_bar_calendar.ipynb"
spec = importlib.util.spec_from_file_location(
    "b04_transaction_fixtures", pathlib.Path(__file__).with_name("test_futures_bar_calendar.py")
)
fixtures = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixtures)
b04 = fixtures.load_notebook_module()
transaction_module = sys.modules[b04.StagedPathTransaction.__module__]


class B04SharedTransactionTest(unittest.TestCase):
    def setUp(self):
        self.capture = io.StringIO()
        redirect = contextlib.redirect_stdout(self.capture)
        redirect.__enter__()
        self.addCleanup(redirect.__exit__, None, None, None)
        temporary = tempfile.TemporaryDirectory(prefix="b04-transaction-")
        self.addCleanup(temporary.cleanup)
        self.root = pathlib.Path(temporary.name)
        self.silver = self.root / "silver"
        self.target = self.silver / b04.TABLE_NAME
        self.key = ("1d", "XSGE", 2024, 1)
        self.relative = b04.partition_relative_path(self.key)
        self.leaf = self.target / self.relative
        self.marker = self.target / "schema.parquet"
        self.fixture = fixtures.FuturesBarCalendarTests()
        self.fixture.module = b04
        self.fixture.setUp()
        self.frame = b04.build_fresh_partitions(
            self.fixture.contract_frame(), self.fixture.updated_at
        )["1d"]
        self.replacement = self.frame.iloc[:1].copy()
        self.empty = self.frame.iloc[:0]

    def hashes(self, path=None):
        path = path or self.target
        return {str(p.relative_to(path)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in path.rglob("*.parquet")}

    def seed(self, marker="stale"):
        b04.ds.write_dataset(
            b04.pandas_to_arrow(self.frame, b04.FUTURES_BAR_CALENDAR_SCHEMA),
            self.target, format="parquet", partitioning=b04.HIVE_PARTITIONING,
            existing_data_behavior="delete_matching",
        )
        if marker is not None:
            schema = b04.PARQUET_FILE_SCHEMA
            if marker == "stale":
                schema = schema.with_metadata({**schema.metadata, b"schema_version": b"old-description"})
            b04.pq.write_table(b04.pa.Table.from_batches([], schema=schema), self.marker)
        return self.hashes()

    def assert_clean(self):
        self.assertFalse(list(self.silver.glob(".c04s-*")))
        self.assertFalse(list(self.silver.glob(".c04b-*")))

    def reject_formal(self, dataset, *args, **kwargs):
        if any(pathlib.Path(path).resolve().is_relative_to(self.target.resolve()) for path in dataset.files):
            raise ValueError("injected formal rejection")
        return self.original_read(dataset, *args, **kwargs)

    def test_success_replaces_stale_marker_and_matching_marker_is_untouched(self):
        self.seed()
        before_marker = self.marker.read_bytes()
        self.assertEqual(b04.commit_partition(self.replacement, self.root, self.key), 1)
        self.assertNotEqual(self.marker.read_bytes(), before_marker)
        self.assertTrue(b04.pq.read_schema(self.marker).equals(b04.PARQUET_FILE_SCHEMA, check_metadata=True))
        self.assertEqual(b04.pq.read_metadata(self.marker).num_rows, 0)
        matching_bytes = self.marker.read_bytes()
        original_replace = transaction_module.os.replace
        with patch.object(transaction_module.os, "replace", wraps=original_replace) as replacements:
            b04.commit_partition(self.frame, self.root, self.key)
        self.assertEqual(self.marker.read_bytes(), matching_bytes)
        self.assertTrue(all(self.marker.resolve() not in call.args for call in replacements.call_args_list))
        self.assert_clean()

    def test_formal_failure_restores_old_leaf_and_old_marker_bytes(self):
        before = self.seed()
        self.original_read = b04.read_partition
        with patch.object(b04, "read_partition", side_effect=self.reject_formal):
            with self.assertRaisesRegex(RuntimeError, "旧目标已恢复") as caught:
                b04.commit_partition(self.replacement, self.root, self.key)
        self.assertIsInstance(caught.exception.__cause__, ValueError)
        self.assertEqual(self.hashes(), before)
        quarantines = list(self.silver.glob(".c04q-*"))
        self.assertEqual(len(quarantines), 1)
        self.assertTrue((quarantines[0] / self.relative).is_dir())
        self.assertFalse((quarantines[0] / "schema.parquet").exists())
        self.assertNotIn("partition_committed:", self.capture.getvalue())
        self.assert_clean()

    def test_new_marker_readback_failure_removes_marker_and_restores_old_leaf(self):
        before = self.seed(marker=None)
        original_parquet_file = b04.pq.ParquetFile

        def reject_marker(path, *args, **kwargs):
            if pathlib.Path(path) == self.marker:
                raise OSError("injected marker readback failure")
            return original_parquet_file(path, *args, **kwargs)

        with patch.object(b04.pq, "ParquetFile", side_effect=reject_marker):
            with self.assertRaises(RuntimeError):
                b04.commit_partition(self.replacement, self.root, self.key)
        self.assertEqual(self.hashes(), before)
        self.assertFalse(self.marker.exists())
        self.assert_clean()

    def test_marker_backup_or_install_failure_restores_leaf_and_marker(self):
        # Both failure positions occur after the current leaf has been installed.
        for operation in ("backup", "install"):
            with self.subTest(operation=operation):
                before = self.seed()
                original_replace = transaction_module.os.replace

                def reject_marker(source, target):
                    if (operation == "backup" and source == self.marker.resolve()) or (
                        operation == "install" and source.name == "schema.parquet"
                        and any(part.startswith(".c04s-") for part in source.parts)
                    ):
                        raise PermissionError("injected marker move failure")
                    return original_replace(source, target)

                with patch.object(transaction_module.os, "replace", side_effect=reject_marker):
                    with self.assertRaises(RuntimeError):
                        b04.commit_partition(self.replacement, self.root, self.key)
                self.assertEqual(self.hashes(), before)
                self.assert_clean()

    def test_marker_staging_write_failure_restores_already_installed_leaf(self):
        before = self.seed()
        original_write = b04.pq.write_table

        def reject_marker(table, path, *args, **kwargs):
            if pathlib.Path(path).name == "schema.parquet":
                raise OSError("injected marker staging failure")
            return original_write(table, path, *args, **kwargs)

        with patch.object(b04.pq, "write_table", side_effect=reject_marker):
            with self.assertRaises(RuntimeError):
                b04.commit_partition(self.replacement, self.root, self.key)
        self.assertEqual(self.hashes(), before)
        self.assert_clean()

    def test_empty_output_deletes_leaf_and_keeps_zero_row_marker(self):
        self.seed(marker="matching")
        before_marker = self.marker.read_bytes()
        self.assertEqual(b04.commit_partition(self.empty, self.root, self.key), 0)
        self.assertFalse(self.leaf.exists())
        self.assertEqual(self.marker.read_bytes(), before_marker)
        self.assert_clean()

    def test_empty_output_marker_failure_restores_deleted_leaf(self):
        before = self.seed(marker=None)
        with patch.object(b04.pq, "ParquetFile", side_effect=OSError("marker rejected")):
            with self.assertRaisesRegex(OSError, "marker rejected"):
                b04.commit_partition(self.empty, self.root, self.key)
        self.assertEqual(self.hashes(), before)
        self.assert_clean()

    def test_marker_restore_failure_keeps_backup_and_still_restores_leaf(self):
        before = self.seed()
        before_marker = self.marker.read_bytes()
        original_replace = transaction_module.os.replace
        self.original_read = b04.read_partition

        def reject_restore(source, target):
            if source.name == "schema.parquet" and any(part.startswith(".c04b-") for part in source.parts):
                raise PermissionError("injected marker recovery failure")
            return original_replace(source, target)

        with patch.object(b04, "read_partition", side_effect=self.reject_formal), patch.object(
            transaction_module.os, "replace", side_effect=reject_restore
        ):
            with self.assertRaisesRegex(RuntimeError, "回滚不完整"):
                b04.commit_partition(self.replacement, self.root, self.key)
        self.assertEqual(self.hashes(), {key: value for key, value in before.items() if key != "schema.parquet"})
        backups = list(self.silver.glob(".c04b-*"))
        self.assertEqual(len(backups), 1)
        self.assertEqual((backups[0] / "schema.parquet").read_bytes(), before_marker)
        self.assertFalse(list(self.silver.glob(".c04s-*")))
        self.assertIn("phase=rollback; status=failed", self.capture.getvalue())
        self.assertNotIn("partition_committed:", self.capture.getvalue())

    def test_missing_staging_leaf_is_not_interpreted_as_deletion(self):
        before = self.seed(marker="matching")
        original_replace = b04.StagedPathTransaction.replace

        def replace_with_missing_source(transaction, *, target_path, staged_path, **kwargs):
            if target_path == self.leaf:
                staged_path = transaction.staging_dir / "missing"
            return original_replace(transaction, target_path=target_path, staged_path=staged_path, **kwargs)

        with patch.object(b04.StagedPathTransaction, "replace", new=replace_with_missing_source):
            with self.assertRaises(FileNotFoundError):
                b04.commit_partition(self.replacement, self.root, self.key)
        self.assertEqual(self.hashes(), before)
        self.assert_clean()

    def test_second_leaf_failure_preserves_first_and_stops_later_leaves(self):
        upstream_df = pd.concat([
            self.fixture.contract_frame(trading_date_value=date(2024, month, 3))
            for month in (1, 2)
        ], ignore_index=True)
        b04.ds.write_dataset(
            b04.pandas_to_arrow(upstream_df, b04.FUTURES_CONTRACT_CALENDAR_SCHEMA),
            self.silver / b04.UPSTREAM_TABLE_NAME,
            format="parquet", partitioning=b04.UPSTREAM_PARTITIONING,
        )
        original_read = b04.read_partition

        def reject_second(dataset, schema, columns, key, *args, **kwargs):
            if key[0] == "1m" and any(pathlib.Path(path).resolve().is_relative_to(self.target.resolve()) for path in dataset.files):
                raise OSError("second leaf rejected")
            return original_read(dataset, schema, columns, key, *args, **kwargs)

        with patch.object(b04, "read_partition", side_effect=reject_second):
            result = CliRunner().invoke(b04.main, ["--lake-root", str(self.root), "--write"])
        self.assertNotEqual(result.exit_code, 0, result.output)
        self.assertTrue(self.leaf.is_dir())
        self.assertFalse((self.target / b04.partition_relative_path(("1m", "XSGE", 2024, 1))).exists())
        self.assertFalse((self.target / b04.partition_relative_path(("1d", "XSGE", 2024, 2))).exists())
        self.assertTrue(self.marker.exists())
        self.assertEqual(result.output.count("partition_committed:"), 1)
        self.assertNotIn("phase=run; status=completed", result.output)
        self.assert_clean()


class B04EntryCellTest(unittest.TestCase):
    def test_notebook_script_and_import_routes(self):
        notebook = nbformat.read(NOTEBOOK, as_version=4)
        source = next(cell.source for cell in notebook.cells
                      if cell.cell_type == "code" and "notebook_args =" in cell.source)
        cases = [
            ({"ipykernel": object()}, {}, "__main__", "notebook"),
            ({}, {"__file__": str(NOTEBOOK.with_suffix('.py'))}, "__main__", "script"),
            ({}, {"__file__": str(NOTEBOOK.with_suffix('.py'))}, "imported_b04", "import"),
            ({"ipykernel": object()}, {"__file__": str(NOTEBOOK.with_suffix('.py'))}, "imported_b04", "import"),
        ]
        for modules, extras, name, expected in cases:
            with self.subTest(expected=expected, modules=bool(modules)):
                command = Mock()
                namespace = {"__name__": name, "main": command,
                             "sys": SimpleNamespace(modules=modules, argv=["ipykernel_launcher.py", "-f", "connection.json"]), **extras}
                exec(compile(source, str(NOTEBOOK), "exec"), namespace)
                if expected == "notebook":
                    command.assert_not_called()
                    command.main.assert_called_once_with(
                        args=["--start-date", "2026-08-01", "--end-date", "2026-08-15"],
                        prog_name="c04_futures_bar_calendar", standalone_mode=False,
                    )
                elif expected == "script":
                    command.assert_called_once_with()
                    command.main.assert_not_called()
                else:
                    self.assertEqual(command.mock_calls, [])


if __name__ == "__main__":
    unittest.main()
