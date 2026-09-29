"""raw 日期和日历叶分别恢复；仅使用临时湖、假 HTTP 与明确注入的异常。"""
import contextlib
import io
import pathlib
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

import nbformat
import pandas as pd
from click.testing import CliRunner

import test_b03_c02_raw_staging_lifecycle as fixtures

calendar = fixtures.MODULE
transaction_module = sys.modules[calendar.StagedPathTransaction.__module__]
NOW = datetime.now(timezone.utc) - timedelta(seconds=1)


class RawCalendarTransactionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='raw-tx-')
        self.addCleanup(temporary.cleanup)
        self.root = pathlib.Path(temporary.name).resolve()
        self.raw = self.root/calendar.RAW_RELATIVE_ROOT
        self.silver = self.root/'silver'
        self.target = self.silver/calendar.CALENDAR_TABLE_NAME
        self.day = date(2026, 7, 1)
        self.output = io.StringIO()
        capture = contextlib.redirect_stdout(self.output)
        capture.__enter__()
        self.addCleanup(capture.__exit__, None, None, None)
        batch_id = patch.object(calendar.uuid, 'uuid4', return_value=SimpleNamespace(hex='test'))
        batch_id.start()
        self.addCleanup(batch_id.stop)

    def stored(self, root):
        return {p.relative_to(root).as_posix(): p.read_bytes() for p in root.rglob('*') if p.is_file()}

    def seed(self):
        rows = pd.concat([fixtures.pending_calendar_frame(date(2026, month, 1)) for month in (7, 8, 9)], ignore_index=True)
        fixtures.write_exact_calendar(self.root, rows)
        return rows

    def completed(self, rows, days):
        evidence = {day: {'byte_count': 3, 'sha256': 'a'*64} for day in days}
        return calendar.apply_calendar_completion(rows, evidence, 'new-run', NOW)

    def leaf(self, month):
        return self.target/f'dataset_name={calendar.DATASET_NAME}/year=2026/month={month}'

    def read(self):
        return calendar.arrow_to_pandas(calendar.ds.dataset(self.target, format='parquet', partitioning=calendar.CALENDAR_PARTITIONING).to_table(columns=calendar.EXTERNAL_MARKET_CALENDAR_SCHEMA.names), calendar.EXTERNAL_MARKET_CALENDAR_SCHEMA).sort_values('observation_date')

    def fail_raw_formal(self):
        original = calendar.inspect_raw_leaf
        target = calendar.raw_leaf_path(self.raw, self.day)
        def inspect(path):
            if path == target:
                raise ValueError('injected raw formal failure')
            return original(path)
        return patch.object(calendar, 'inspect_raw_leaf', side_effect=inspect)

    def fail_calendar_formal(self, ordinal=1):
        original = calendar.open_exact_dataset
        calls = 0
        def open_dataset(*args, **kwargs):
            nonlocal calls
            if args[3] == '正式外部市场日历':
                calls += 1
                if calls == ordinal:
                    raise ValueError('injected calendar formal failure')
            return original(*args, **kwargs)
        return patch.object(calendar, 'open_exact_dataset', side_effect=open_dataset)

    def test_raw_success_replaces_one_date_and_preserves_other_dates(self):
        other = date(2026, 7, 2)
        calendar.commit_raw_response(self.raw, self.day, b'old')
        calendar.commit_raw_response(self.raw, other, b'other')
        other_before = self.stored(calendar.raw_leaf_path(self.raw, other))
        evidence = calendar.commit_raw_response(self.raw, self.day, b'\x00\xffnew')
        self.assertEqual(evidence, calendar.inspect_raw_leaf(calendar.raw_leaf_path(self.raw, self.day)))
        self.assertEqual(other_before, self.stored(calendar.raw_leaf_path(self.raw, other)))
        self.assertEqual(list(self.raw.glob('.*-test')), [])

    def test_raw_install_failure_restores_old_bytes(self):
        calendar.commit_raw_response(self.raw, self.day, b'old')
        leaf = calendar.raw_leaf_path(self.raw, self.day)
        before = self.stored(leaf)
        original = transaction_module.os.replace
        def replace(source, destination):
            if pathlib.Path(source) == self.raw/'.staging-test':
                raise OSError('injected raw install failure')
            return original(source, destination)
        with patch.object(transaction_module.os, 'replace', side_effect=replace):
            with self.assertRaisesRegex(OSError, 'raw install failure'):
                calendar.commit_raw_response(self.raw, self.day, b'new')
        self.assertEqual(before, self.stored(leaf))
        self.assertEqual(list(self.raw.glob('.*-test')), [])

    def test_raw_formal_failure_restores_old_and_keeps_new_evidence(self):
        calendar.commit_raw_response(self.raw, self.day, b'old')
        leaf = calendar.raw_leaf_path(self.raw, self.day)
        before = self.stored(leaf)
        with self.fail_raw_formal(), self.assertRaises(RuntimeError) as error:
            calendar.commit_raw_response(self.raw, self.day, b'new')
        self.assertIsInstance(error.exception.__cause__, ValueError)
        self.assertEqual(before, self.stored(leaf))
        quarantined = self.raw/'.quarantine-test'/leaf.relative_to(self.raw)
        self.assertEqual((quarantined/'response.html').read_bytes(), b'new')
        self.assertFalse((self.raw/'.backup-test').exists())
        self.assertFalse((self.raw/'.staging-test').exists())

    def test_new_raw_formal_failure_restores_absence(self):
        with self.fail_raw_formal(), self.assertRaises(RuntimeError):
            calendar.commit_raw_response(self.raw, self.day, b'new')
        self.assertFalse(calendar.raw_leaf_path(self.raw, self.day).exists())
        self.assertTrue(list((self.raw/'.quarantine-test').rglob('response.html')))
        pending, repair, count = calendar.plan_raw_grids(fixtures.pending_calendar_frame(self.day), self.raw, None, None)
        self.assertEqual(len(pending), 1)
        self.assertTrue(repair.empty)
        self.assertEqual(count, 0)

    def test_raw_restore_failure_keeps_old_backup_and_new_quarantine(self):
        calendar.commit_raw_response(self.raw, self.day, b'old')
        leaf = calendar.raw_leaf_path(self.raw, self.day)
        backup = self.raw/'.backup-test'/leaf.relative_to(self.raw)
        original = transaction_module.os.replace
        def replace(source, destination):
            if pathlib.Path(source) == backup:
                raise OSError('injected restore failure')
            return original(source, destination)
        with self.fail_raw_formal(), patch.object(transaction_module.os, 'replace', side_effect=replace):
            with self.assertRaisesRegex(RuntimeError, '回滚不完整') as error:
                calendar.commit_raw_response(self.raw, self.day, b'new')
        self.assertIsInstance(error.exception.__cause__, ValueError)
        self.assertEqual((backup/'response.html').read_bytes(), b'old')
        self.assertEqual((self.raw/'.quarantine-test'/leaf.relative_to(self.raw)/'response.html').read_bytes(), b'new')
        self.assertFalse((self.raw/'.staging-test').exists())

    def test_raw_staging_failure_does_not_touch_formal(self):
        calendar.commit_raw_response(self.raw, self.day, b'old')
        before = self.stored(calendar.raw_leaf_path(self.raw, self.day))
        with patch.object(calendar, 'inspect_raw_leaf', return_value=None), patch.object(calendar, 'StagedPathTransaction') as transaction:
            with self.assertRaisesRegex(ValueError, 'staging'):
                calendar.commit_raw_response(self.raw, self.day, b'new')
            transaction.assert_not_called()
        self.assertEqual(before, self.stored(calendar.raw_leaf_path(self.raw, self.day)))
        self.assertFalse((self.raw/'.staging-test').exists())

    def test_transaction_enter_failure_cleans_raw_staging(self):
        calendar.commit_raw_response(self.raw, self.day, b'old')
        before = self.stored(calendar.raw_leaf_path(self.raw, self.day))
        with patch.object(calendar.StagedPathTransaction, '__enter__', side_effect=OSError('injected enter failure')):
            with self.assertRaisesRegex(OSError, 'enter failure'):
                calendar.commit_raw_response(self.raw, self.day, b'new')
        self.assertEqual(before, self.stored(calendar.raw_leaf_path(self.raw, self.day)))
        self.assertFalse((self.raw/'.staging-test').exists())

    def test_calendar_success_keeps_untouched_leaf_and_root_marker(self):
        rows = self.seed()
        untouched = self.stored(self.leaf(9))
        marker = (self.target/'schema.parquet').read_bytes()
        days = {self.day, date(2026, 8, 1)}
        updated = self.completed(rows, days)
        self.assertEqual(calendar.commit_calendar_partitions(updated, days, self.root), 2)
        self.assertEqual(untouched, self.stored(self.leaf(9)))
        self.assertEqual(marker, (self.target/'schema.parquet').read_bytes())
        self.assertEqual(self.read()['is_fetch_completed'].tolist(), [True, True, False])
        self.assertEqual(list(self.silver.glob('.*-test')), [])

    def test_second_calendar_leaf_failure_preserves_first_success(self):
        rows = self.seed()
        before_second = self.stored(self.leaf(8))
        before_third = self.stored(self.leaf(9))
        days = {self.day, date(2026, 8, 1)}
        updated = self.completed(rows, days)
        with self.fail_calendar_formal(2), self.assertRaises(RuntimeError) as error:
            calendar.commit_calendar_partitions(updated, days, self.root)
        self.assertIsInstance(error.exception.__cause__, ValueError)
        self.assertEqual(before_second, self.stored(self.leaf(8)))
        self.assertEqual(before_third, self.stored(self.leaf(9)))
        self.assertEqual(self.read()['is_fetch_completed'].tolist(), [True, False, False])
        self.assertTrue(list(self.silver.glob('.*.failed-test')))
        self.assertFalse(list(self.silver.glob('.*.backup-test')))
        self.assertFalse(list(self.silver.glob('.*.staging-test')))

    def test_calendar_first_backup_failure_keeps_old_leaf(self):
        rows = self.seed()
        before = self.stored(self.target)
        original = transaction_module.os.replace
        def replace(source, destination):
            if pathlib.Path(source) == self.leaf(7):
                raise OSError('injected calendar backup failure')
            return original(source, destination)
        with patch.object(transaction_module.os, 'replace', side_effect=replace):
            with self.assertRaisesRegex(OSError, 'calendar backup failure'):
                calendar.commit_calendar_partitions(self.completed(rows, {self.day}), {self.day}, self.root)
        self.assertEqual(before, self.stored(self.target))
        self.assertEqual(list(self.silver.glob('.*-test')), [])

    def test_calendar_install_failure_restores_old_leaf(self):
        rows = self.seed()
        before = self.stored(self.target)
        original = transaction_module.os.replace
        def replace(source, destination):
            if '.staging-test' in str(source):
                raise OSError('injected calendar install failure')
            return original(source, destination)
        with patch.object(transaction_module.os, 'replace', side_effect=replace):
            with self.assertRaisesRegex(OSError, 'calendar install failure'):
                calendar.commit_calendar_partitions(self.completed(rows, {self.day}), {self.day}, self.root)
        self.assertEqual(before, self.stored(self.target))
        self.assertEqual(list(self.silver.glob('.*-test')), [])

    def test_calendar_enter_failure_cleans_staging(self):
        rows = self.seed()
        before = self.stored(self.target)
        with patch.object(calendar.StagedPathTransaction, '__enter__', side_effect=OSError('injected enter failure')):
            with self.assertRaisesRegex(OSError, 'enter failure'):
                calendar.commit_calendar_partitions(self.completed(rows, {self.day}), {self.day}, self.root)
        self.assertEqual(before, self.stored(self.target))
        self.assertEqual(list(self.silver.glob('.*-test')), [])

    def test_empty_touched_dates_do_not_open_transaction_or_create_lake(self):
        with patch.object(calendar, 'StagedPathTransaction') as transaction:
            self.assertEqual(calendar.commit_calendar_partitions(pd.DataFrame(), set(), self.root), 0)
            transaction.assert_not_called()
        self.assertEqual(list(self.root.iterdir()), [])

    def test_calendar_failure_keeps_raw_then_next_run_repairs_without_http(self):
        fixtures.write_exact_calendar(self.root, fixtures.pending_calendar_frame(self.day))
        before = self.stored(self.target)
        session = fixtures.FakeSession(fixtures.FakeResponse(200, b'\x00\xffsource'))
        with patch.object(calendar, 'create_http_session', return_value=session), self.fail_calendar_formal(2):
            with self.assertRaises(RuntimeError):
                calendar.main.callback(lake_root=self.root, start_date=None, end_date=None, write=True)
        self.assertTrue(session.closed)
        self.assertEqual(session.get_count, 1)
        self.assertEqual(before, self.stored(self.target))
        raw_before = self.stored(calendar.raw_leaf_path(self.raw, self.day))
        self.assertEqual(raw_before['response.html'], b'\x00\xffsource')
        # A real rerun uses a fresh transaction id; retained failure evidence is not overwritten.
        with patch.object(calendar.uuid, 'uuid4', return_value=SimpleNamespace(hex='next')):
            with patch.object(calendar, 'create_http_session', side_effect=AssertionError('repair must not request HTTP')):
                calendar.main.callback(lake_root=self.root, start_date=None, end_date=None, write=True)
        self.assertTrue(self.read().iloc[0]['is_fetch_completed'])
        self.assertEqual(raw_before, self.stored(calendar.raw_leaf_path(self.raw, self.day)))

    def test_readonly_fetch_does_not_modify_lake(self):
        self.seed()
        before = self.stored(self.root)
        session = fixtures.FakeSession(fixtures.FakeResponse(200, b''))
        with patch.object(calendar, 'create_http_session', return_value=session):
            result = CliRunner().invoke(calendar.main, ['--lake-root', str(self.root)])
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(session.get_count, 3)
        self.assertTrue(session.closed)
        self.assertEqual(before, self.stored(self.root))

    def test_http_failure_body_remains_hidden_if_calendar_commit_fails(self):
        fixtures.write_exact_calendar(self.root, fixtures.pending_calendar_frame(self.day))
        before = self.stored(self.target)
        session = fixtures.FakeSession(fixtures.FakeResponse(404, b'not-found'))
        with patch.object(calendar, 'create_http_session', return_value=session), self.fail_calendar_formal(2):
            with self.assertRaises(RuntimeError):
                calendar.main.callback(lake_root=self.root, start_date=None, end_date=None, write=True)
        self.assertEqual(before, self.stored(self.target))
        self.assertFalse(calendar.raw_leaf_path(self.raw, self.day).exists())
        body = next((self.raw/'.failed-test').rglob('response.html'))
        self.assertEqual(body.read_bytes(), b'not-found')
        self.assertTrue(session.closed)

    def test_formal_explicit_date_write_fails_before_io(self):
        with patch.object(calendar, 'settings', SimpleNamespace(futures_lake_root=self.root)), patch.object(calendar, 'open_exact_dataset') as opened, patch.object(calendar, 'create_http_session') as session:
            result = CliRunner().invoke(calendar.main, ['--start-date', '2026-07-01', '--end-date', '2026-07-01', '--write'])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn('禁止写入', result.output)
        opened.assert_not_called()
        session.assert_not_called()

    def test_entry_distinguishes_notebook_script_and_import(self):
        notebook = nbformat.read(fixtures.NOTEBOOK_PATH, as_version=4)
        entry = next(cell.source for cell in notebook.cells if cell.id == 'e3f69e92')
        for kernel, has_file, name, expected in (
            (True, False, '__main__', 'notebook'), (True, True, 'imported', 'none'),
            (False, True, 'imported', 'none'), (False, True, '__main__', 'script'),
            (True, True, '__main__', 'script'),
        ):
            with self.subTest(kernel=kernel, has_file=has_file, name=name):
                command = Mock()
                namespace = {'sys': SimpleNamespace(modules={'ipykernel': object()} if kernel else {}), 'main': command, '__name__': name}
                if has_file:
                    namespace['__file__'] = str(fixtures.NOTEBOOK_PATH.with_suffix('.py'))
                exec(entry, namespace)
                if expected == 'notebook':
                    command.main.assert_called_once_with(args=[], prog_name='b02_domestic_spot_basis', standalone_mode=False)
                    command.assert_not_called()
                elif expected == 'script':
                    command.assert_called_once_with()
                    command.main.assert_not_called()
                else:
                    command.assert_not_called()
                    command.main.assert_not_called()


if __name__ == '__main__':
    unittest.main()
