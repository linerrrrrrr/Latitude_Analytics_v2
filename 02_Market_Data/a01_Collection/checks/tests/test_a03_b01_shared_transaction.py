"""外部市场日历整批事务与入口；临时 Parquet + 主动注入的移动/验收异常。"""
import contextlib
import importlib.util
import io
import pathlib
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

import nbformat
from click.testing import CliRunner

import test_b03_metadata_upgrade as fixtures

project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()
for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        ROOT = candidate_root
        sys.path.insert(0, str(candidate_root))
        break
else:
    raise RuntimeError("未找到项目根目录")
SOURCE = ROOT/'02_Market_Data/a01_Collection/b03_External_Market_Data/c01_external_market_calendar.py'
spec = importlib.util.spec_from_file_location('external_calendar_transaction_test', SOURCE)
calendar = importlib.util.module_from_spec(spec)
spec.loader.exec_module(calendar)
transaction_module = sys.modules[calendar.StagedPathTransaction.__module__]
NOW = datetime.now(timezone.utc) - timedelta(seconds=5)


class ExternalCalendarTransactionTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='ext-tx-')
        self.addCleanup(temporary.cleanup)
        self.root = pathlib.Path(temporary.name).resolve()
        self.silver = self.root/'silver'
        self.target = self.silver/calendar.TABLE_NAME
        self.staging = self.silver/'.a03-b01-s-test'
        self.backup = self.silver/'.a03-b01-b-test'
        self.quarantine = self.silver/'.a03-b01-f-test'
        self.output = io.StringIO()
        capture = contextlib.redirect_stdout(self.output)
        capture.__enter__()
        self.addCleanup(capture.__exit__, None, None, None)
        fixed_id = patch.object(calendar.uuid, 'uuid4', return_value=SimpleNamespace(hex='test'))
        fixed_id.start()
        self.addCleanup(fixed_id.stop)
        upstream = fixtures.build_trade_calendar_frame(calendar)
        rows = []
        for month in (7, 8, 9):
            row = upstream.iloc[0].to_dict()
            day = date(2026, month, 1)
            row.update(calendar_date=day, date_key=day.strftime('%Y%m%d'),
                       weekday=day.isoweekday(), is_weekend=day.isoweekday() >= 6,
                       is_trading_day=day.isoweekday() < 6, updated_at=NOW)
            rows.append(row)
        self.upstream = calendar.arrow_to_pandas(
            calendar.pa.Table.from_pylist(rows, schema=calendar.TRADE_CALENDAR_SCHEMA), calendar.TRADE_CALENDAR_SCHEMA)
        self.empty = calendar.empty_pandas(calendar.EXTERNAL_MARKET_CALENDAR_SCHEMA)
        self.expected = calendar.build_expected_calendar(self.upstream, self.empty, NOW)
        self.output.seek(0)
        self.output.truncate()

    def seed(self, frame=None, *, stale=False):
        fixtures.write_partitioned_table(calendar, self.expected if frame is None else frame,
            calendar.EXTERNAL_MARKET_CALENDAR_SCHEMA, calendar.PARTITION_COLUMNS, self.target,
            write_schema=fixtures.schema_with_stale_metadata(calendar.EXTERNAL_MARKET_CALENDAR_SCHEMA) if stale else None)

    def read(self):
        table = calendar.ds.dataset(self.target, format='parquet', partitioning=calendar.CALENDAR_PARTITIONING).to_table(
            columns=calendar.EXTERNAL_MARKET_CALENDAR_SCHEMA.names)
        return calendar.arrow_to_pandas(table, calendar.EXTERNAL_MARKET_CALENDAR_SCHEMA)

    def stored_bytes(self):
        return {str(p.relative_to(self.target)): p.read_bytes() for p in self.target.rglob('*.parquet')}

    def leaf(self, month, dataset='domestic_spot_basis'):
        return self.target/f'dataset_name={dataset}/year=2026/month={month}'

    def commit(self, expected=None, existing=None, *, full=False):
        expected = self.expected if expected is None else expected
        existing = self.empty if existing is None else existing
        return calendar.commit_partitions(expected, calendar.changed_partition_keys(expected, existing), self.root, force_full_swap=full)

    def fail_formal(self):
        original = calendar.open_exact_dataset
        def opened(*args, **kwargs):
            if args[3] == '正式外部市场日历':
                raise ValueError('injected formal readback failure')
            return original(*args, **kwargs)
        return patch.object(calendar, 'open_exact_dataset', side_effect=opened)

    def assert_recovered(self, previous, *, quarantine=True):
        self.assertEqual(self.stored_bytes(), previous)
        self.assertFalse(self.staging.exists())
        self.assertFalse(self.backup.exists())
        self.assertEqual(self.quarantine.exists(), quarantine)
        self.assertIn('phase=rollback; status=completed', self.output.getvalue())
        self.assertNotIn('persisted=true', self.output.getvalue())

    def test_empty_lake_success_and_no_changes(self):
        self.assertEqual(self.commit(), len(self.expected))
        self.assertEqual(calendar.table_digest(calendar.pandas_to_arrow(self.read(), calendar.EXTERNAL_MARKET_CALENDAR_SCHEMA)), calendar.table_digest(calendar.pandas_to_arrow(self.expected, calendar.EXTERNAL_MARKET_CALENDAR_SCHEMA)))
        self.assertFalse(list(self.silver.glob('.a03-b01-*')))
        before = self.stored_bytes()
        self.assertEqual(self.commit(existing=self.expected), 0)
        self.assertEqual(before, self.stored_bytes())

    def test_replace_delete_preserve_clean_leaf_and_marker(self):
        self.seed()
        before = self.stored_bytes()
        updated = self.expected.loc[self.expected.month.ne(8)].copy()
        updated.loc[updated.month.eq(7), 'updated_at'] = NOW + timedelta(seconds=1)
        self.assertEqual(self.commit(updated, self.expected), int(updated.month.eq(7).sum()))
        self.assertEqual(calendar.table_digest(calendar.pandas_to_arrow(self.read(), calendar.EXTERNAL_MARKET_CALENDAR_SCHEMA)), calendar.table_digest(calendar.pandas_to_arrow(updated, calendar.EXTERNAL_MARKET_CALENDAR_SCHEMA)))
        for name, content in before.items():
            if 'month=9' in name or name == 'schema.parquet':
                self.assertEqual(self.stored_bytes()[name], content)
        self.assertFalse(self.leaf(8).exists())
        self.assertFalse(list(self.silver.glob('.a03-b01-*')))

    def test_full_empty_replaces_root_with_readable_marker(self):
        self.seed()
        self.assertEqual(self.commit(self.empty, self.expected), 0)
        self.assertTrue(self.read().empty)
        self.assertEqual([p.name for p in self.target.rglob('*.parquet')], ['schema.parquet'])

    def test_staging_failure_leaves_original_untouched(self):
        self.seed()
        before = self.stored_bytes()
        with patch.object(calendar.pq, 'write_table', side_effect=OSError('injected staging failure')):
            with self.assertRaisesRegex(OSError, 'injected staging'):
                self.commit()
        self.assertEqual(self.stored_bytes(), before)
        self.assertFalse(list(self.silver.glob('.a03-b01-*')))

    def test_transaction_entry_failure_cleans_staging(self):
        self.seed()
        before = self.stored_bytes()
        with patch.object(calendar.StagedPathTransaction, '__enter__', side_effect=OSError('injected entry failure')):
            with self.assertRaisesRegex(OSError, 'injected entry'):
                self.commit()
        self.assertEqual(self.stored_bytes(), before)
        self.assertFalse(list(self.silver.glob('.a03-b01-*')))

    def test_later_leaf_install_failure_restores_whole_batch(self):
        self.seed()
        before = self.stored_bytes()
        original = transaction_module.os.replace
        failure = OSError('injected later install failure')
        def moved(source, target):
            if pathlib.Path(source).is_relative_to(self.staging) and pathlib.Path(target) == self.leaf(8):
                raise failure
            return original(source, target)
        with patch.object(transaction_module.os, 'replace', side_effect=moved):
            with self.assertRaises(RuntimeError) as caught:
                self.commit()
        self.assertIs(caught.exception.__cause__, failure)
        self.assert_recovered(before)

    def test_formal_failure_restores_deleted_replaced_and_new_leaves(self):
        old = self.expected.loc[self.expected.month.ne(9)].copy()
        self.seed(old)
        before = self.stored_bytes()
        updated = self.expected.loc[self.expected.month.ne(7)].copy()
        updated.loc[updated.month.eq(8), 'updated_at'] = NOW + timedelta(seconds=1)
        with self.fail_formal():
            with self.assertRaises(RuntimeError) as caught:
                self.commit(updated, old)
        self.assertIsInstance(caught.exception.__cause__, ValueError)
        self.assert_recovered(before)
        self.assertFalse(self.leaf(9).exists())

    def test_full_empty_formal_failure_restores_old_root(self):
        self.seed()
        before = self.stored_bytes()
        with self.fail_formal():
            with self.assertRaises(RuntimeError):
                self.commit(self.empty, self.expected)
        self.assert_recovered(before)
        self.assertTrue((self.quarantine/calendar.TABLE_NAME/'schema.parquet').exists())

    def test_metadata_migration_formal_failure_restores_old_bytes(self):
        self.seed(stale=True)
        before = self.stored_bytes()
        with self.fail_formal():
            with self.assertRaises(RuntimeError):
                self.commit(full=True)
        self.assert_recovered(before)

    def test_empty_lake_failure_restores_target_absence(self):
        with self.fail_formal():
            with self.assertRaises(RuntimeError):
                self.commit()
        self.assert_recovered({})
        self.assertFalse(self.target.exists())

    def test_restore_failure_keeps_backup_and_continues_other_leaves(self):
        self.seed()
        before = self.stored_bytes()
        original = transaction_module.os.replace
        failed_leaf = self.leaf(8)
        attempted = []
        def moved(source, target):
            if pathlib.Path(source).is_relative_to(self.backup):
                attempted.append(pathlib.Path(target))
                if pathlib.Path(target) == failed_leaf:
                    raise PermissionError('injected restore failure')
            return original(source, target)
        with self.fail_formal(), patch.object(transaction_module.os, 'replace', side_effect=moved):
            with self.assertRaisesRegex(RuntimeError, '回滚不完整') as caught:
                self.commit()
        self.assertIsInstance(caught.exception.__cause__, ValueError)
        self.assertEqual(len(attempted), 9)
        self.assertIn(self.leaf(7), attempted[attempted.index(failed_leaf)+1:])
        for name, content in before.items():
            base = self.backup/calendar.TABLE_NAME if name.startswith(str(failed_leaf.relative_to(self.target))) else self.target
            self.assertEqual((base/name).read_bytes(), content)
        self.assertFalse(self.staging.exists())
        self.assertTrue(self.quarantine.exists())
        self.assertNotIn('persisted=true', self.output.getvalue())

    def test_cli_does_not_report_commit_success_when_formal_readback_fails(self):
        fixtures.write_partitioned_table(calendar, fixtures.build_trade_calendar_frame(calendar), calendar.TRADE_CALENDAR_SCHEMA,
            calendar.UPSTREAM_PARTITION_COLUMNS, self.silver/calendar.UPSTREAM_TABLE_NAME)
        with self.fail_formal():
            result = CliRunner().invoke(calendar.main, ['--lake-root', str(self.root), '--write'])
        self.assertNotEqual(result.exit_code, 0)
        self.assertNotIn('partition_committed:', result.output)
        self.assertNotIn('persisted=true', result.output)
        self.assertIn('phase=rollback; status=completed', result.output)
        self.assertFalse(self.target.exists())

    def test_formal_explicit_date_write_rejected_before_any_read(self):
        with patch.object(calendar, 'settings', SimpleNamespace(futures_lake_root=self.root)), patch.object(calendar, 'open_exact_dataset') as opened:
            result = CliRunner().invoke(calendar.main, ['--lake-root', str(self.root), '--start-date', '2026-07-01', '--end-date', '2026-07-01', '--write'])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn('禁止写入', result.output)
        opened.assert_not_called()

    def test_entry_distinguishes_notebook_import_and_script(self):
        notebook = nbformat.read(SOURCE.with_suffix('.ipynb'), as_version=4)
        entry = next(cell.source for cell in notebook.cells if cell.id == '5dc7bd54')
        for kernel, has_file, name, expected in (
            (True, False, '__main__', 'notebook'), (True, True, 'imported', 'none'),
            (False, True, 'imported', 'none'), (False, True, '__main__', 'script'),
            (True, True, '__main__', 'script'),
        ):
            with self.subTest(kernel=kernel, has_file=has_file, name=name):
                command = Mock()
                namespace = {'sys': SimpleNamespace(modules={'ipykernel': object()} if kernel else {}), 'main': command, '__name__': name}
                if has_file:
                    namespace['__file__'] = str(SOURCE)
                exec(entry, namespace)
                if expected == 'notebook':
                    command.main.assert_called_once_with(args=[], prog_name='c01_external_market_calendar', standalone_mode=False)
                    command.assert_not_called()
                elif expected == 'script':
                    command.assert_called_once_with()
                    command.main.assert_not_called()
                else:
                    command.assert_not_called()
                    command.main.assert_not_called()


if __name__ == '__main__':
    unittest.main()
