"""仓单当前叶事务和执行入口；只使用临时目录和主动故障注入。"""
import ast
import contextlib
import hashlib
import io
import json
import pathlib
import tempfile
import unittest
from datetime import date
from unittest import mock

import pandas as pd

import test_b02_warehouse_receipt as fixtures
import b00_04_staged_path_transaction as transactions

h = fixtures.MODULE
DAY = date(2024, 3, 1)


def hashes(root):
    return {p.relative_to(root).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*.parquet')}


class LeafChecks:
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='warehouse-transaction-test-')
        self.addCleanup(self.temporary.cleanup)
        self.lake = pathlib.Path(self.temporary.name)
        self.silver = self.lake/'silver'
        self.output = io.StringIO()
        self.capture = contextlib.redirect_stdout(self.output)
        self.capture.__enter__()
        self.addCleanup(self.capture.__exit__, None, None, None)
        if self.calendar:
            self.schema = h.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA
            self.columns, self.partitioning = h.CALENDAR_PARTITION_COLUMNS, h.CALENDAR_PARTITIONING
            self.name, self.key = h.CALENDAR_TABLE_NAME, ('warehouse_receipt','XDCE',2024,3)
            self.old = fixtures.calendar_frame(DAY)
            self.new = fixtures.calendar_frame(DAY, is_fetch_completed=True, fetch_result_status='success', actual_record_count=1, quality_status='passed')
            self.label = '正式报告日历'
        else:
            self.schema = h.FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA
            self.columns, self.partitioning = h.PARTITION_COLUMNS, h.FACT_PARTITIONING
            self.name, self.key = h.TABLE_NAME, ('XDCE','EB',2024,3)
            self.old = fixtures.warehouse_frame(DAY, quantity=12)
            self.new = fixtures.warehouse_frame(DAY, quantity=99)
            self.label = '正式仓单事实'
        self.target = self.silver/self.name
        self.relative = h.partition_relative_path(self.columns, self.key)
        self.leaf = self.target/self.relative
        self.marker = self.target/'schema.parquet'

    def seed(self):
        fixtures.write_exact_dataset(self.target, self.old, self.schema, self.columns, self.partitioning)
        return hashes(self.target)

    def commit(self, frame=None):
        frame = self.new if frame is None else frame
        if self.calendar:
            return h.commit_calendar_partitions(frame, {('XDCE','EB',DAY)}, self.lake)
        return h.commit_complete_partition(frame, self.lake, self.key)

    def paths(self, kind):
        return list(self.silver.glob(f'.{self.name}.{kind}-*'))

    def failed(self):
        self.assertNotIn('partition_committed:', self.output.getvalue())
        self.assertNotIn('phase=calendar_state; status=completed;', self.output.getvalue())
        self.assertFalse(self.paths('staging'))

    def reject_formal(self):
        original = h.read_leaf_primary_key_summary
        def read(*args, **kwargs):
            value = original(*args, **kwargs)
            if args[-1] == self.label:
                raise ValueError('injected formal rejection')
            return value
        return mock.patch.object(h, 'read_leaf_primary_key_summary', side_effect=read)

    def test_success_preserves_unrelated_leaf_and_existing_marker(self):
        self.seed()
        other = self.old.copy()
        other['month'] = 4
        other['trading_date'] = date(2024,4,1)
        fixtures.write_exact_dataset(self.target, other, self.schema, self.columns, self.partitioning)
        before = hashes(self.target)
        self.assertEqual(self.commit(), 1)
        after = hashes(self.target)
        for name, digest in before.items():
            if not name.startswith(self.relative.as_posix()+'/'):
                self.assertEqual(after[name], digest)
        self.assertNotEqual(before[self.relative.as_posix()+'/part-0.parquet'], after[self.relative.as_posix()+'/part-0.parquet'])
        self.assertFalse(list(self.silver.glob('.*')))
        self.assertEqual(self.output.getvalue().count('partition_committed:'), 1)

    def test_staging_rejection_leaves_formal_untouched(self):
        before = self.seed()
        with mock.patch.object(h, 'read_leaf_primary_key_summary', side_effect=ValueError('injected staging rejection')):
            with self.assertRaisesRegex(ValueError, 'staging rejection'):
                self.commit()
        self.assertEqual(hashes(self.target), before)
        self.assertFalse(list(self.silver.glob('.*')))
        self.failed()

    def test_transaction_entry_failure_cleans_staging(self):
        before = self.seed()
        with mock.patch.object(h.StagedPathTransaction, '__enter__', side_effect=OSError('injected enter')):
            with self.assertRaisesRegex(OSError, 'injected enter'):
                self.commit()
        self.assertEqual(hashes(self.target), before)
        self.assertFalse(list(self.silver.glob('.*')))
        self.failed()

    def test_first_backup_failure_keeps_original_leaf(self):
        before = self.seed()
        original = transactions.os.replace
        def move(source, target):
            if pathlib.Path(source) == self.leaf:
                raise PermissionError('injected backup failure')
            return original(source, target)
        with mock.patch.object(transactions.os, 'replace', side_effect=move):
            with self.assertRaisesRegex(PermissionError, 'backup failure'):
                self.commit()
        self.assertEqual(hashes(self.target), before)
        self.assertFalse(list(self.silver.glob('.*')))
        self.failed()

    def test_install_failure_restores_old_leaf(self):
        before = self.seed()
        original = transactions.os.replace
        def move(source, target):
            if pathlib.Path(target) == self.leaf and '.staging-' in str(source):
                raise OSError('injected install failure')
            return original(source, target)
        with mock.patch.object(transactions.os, 'replace', side_effect=move):
            with self.assertRaisesRegex(OSError, 'install failure'):
                self.commit()
        self.assertEqual(hashes(self.target), before)
        self.assertFalse(list(self.silver.glob('.*')))
        self.failed()

    def test_formal_rejection_restores_old_and_keeps_new_evidence(self):
        before = self.seed()
        with self.reject_formal():
            with self.assertRaises(RuntimeError) as caught:
                self.commit()
        self.assertIsInstance(caught.exception.__cause__, ValueError)
        self.assertEqual(hashes(self.target), before)
        failed = self.paths('failed')
        self.assertEqual(len(failed), 1)
        self.assertTrue((failed[0]/self.relative/'part-0.parquet').is_file())
        self.assertFalse(self.paths('backup'))
        self.failed()

    def test_restore_failure_keeps_old_backup_and_new_evidence(self):
        before = self.seed()
        original = transactions.os.replace
        def move(source, target):
            if pathlib.Path(target) == self.leaf and '.backup-' in str(source):
                raise PermissionError('injected restore failure')
            return original(source, target)
        with self.reject_formal(), mock.patch.object(transactions.os, 'replace', side_effect=move):
            with self.assertRaisesRegex(RuntimeError, '回滚不完整'):
                self.commit()
        backups = self.paths('backup')
        self.assertEqual(len(backups), 1)
        name = self.relative.as_posix()+'/part-0.parquet'
        self.assertEqual(hashes(backups[0])[name], before[name])
        self.assertEqual(len(self.paths('failed')), 1)
        self.assertIn('phase=rollback; status=failed;', self.output.getvalue())
        self.failed()


class FactTests(LeafChecks, unittest.TestCase):
    calendar = False

    def test_empty_replacement_explicitly_deletes_old_leaf(self):
        self.seed()
        marker = self.marker.read_bytes()
        self.assertEqual(self.commit(h.empty_pandas(self.schema)), 0)
        self.assertFalse(self.leaf.exists())
        self.assertEqual(self.marker.read_bytes(), marker)
        self.assertFalse(list(self.silver.glob('.*')))

    def test_empty_replacement_rejection_restores_old_leaf(self):
        before = self.seed()
        with self.reject_formal():
            with self.assertRaisesRegex(ValueError, 'formal rejection'):
                self.commit(h.empty_pandas(self.schema))
        self.assertEqual(hashes(self.target), before)
        self.assertFalse(list(self.silver.glob('.*')))
        self.failed()

    def test_new_table_marker_or_leaf_install_failure_restores_absence(self):
        original = transactions.os.replace
        for failed_target in (self.marker, self.leaf):
            with self.subTest(target=failed_target):
                def move(source, target):
                    if pathlib.Path(target) == failed_target:
                        raise OSError('injected new table install')
                    return original(source, target)
                with mock.patch.object(transactions.os, 'replace', side_effect=move):
                    with self.assertRaisesRegex(OSError, 'new table install'):
                        self.commit()
                self.assertFalse(self.target.exists())
                self.assertFalse(list(self.silver.glob('.*')))
                self.failed()

    def test_new_table_formal_failure_removes_marker_preserves_failed_leaf(self):
        with self.reject_formal():
            with self.assertRaises(RuntimeError):
                self.commit()
        self.assertFalse(self.target.exists())
        self.assertEqual(len(self.paths('failed')), 1)
        self.assertFalse(self.paths('backup'))
        self.failed()

    def test_new_empty_table_formal_failure_removes_marker(self):
        with self.reject_formal():
            with self.assertRaisesRegex(ValueError, 'formal rejection'):
                self.commit(h.empty_pandas(self.schema))
        self.assertFalse(self.target.exists())
        self.assertFalse(list(self.silver.glob('.*')))
        self.failed()

    def test_new_leaf_quarantine_failure_does_not_block_marker_removal(self):
        original = transactions.os.replace
        def move(source, target):
            if pathlib.Path(source) == self.leaf and '.failed-' in str(target):
                raise PermissionError('injected quarantine failure')
            return original(source, target)
        with self.reject_formal(), mock.patch.object(transactions.os, 'replace', side_effect=move):
            with self.assertRaisesRegex(RuntimeError, '回滚不完整'):
                self.commit()
        self.assertFalse(self.marker.exists())
        self.assertTrue((self.leaf/'part-0.parquet').is_file())
        self.assertEqual(len(self.paths('backup')), 1)
        self.failed()


class CalendarTests(LeafChecks, unittest.TestCase):
    calendar = True

    def test_later_calendar_leaf_failure_preserves_earlier_commit(self):
        self.seed()
        second_day = date(2024,4,1)
        old_second = fixtures.calendar_frame(second_day)
        fixtures.write_exact_dataset(self.target, old_second, self.schema, self.columns, self.partitioning)
        old_hashes = hashes(self.target)
        new_second = fixtures.calendar_frame(second_day, is_fetch_completed=True, fetch_result_status='success', actual_record_count=1, quality_status='passed')
        original = h.read_leaf_primary_key_summary
        def read(*args, **kwargs):
            value = original(*args, **kwargs)
            if args[-1] == self.label and args[-2][-1] == 4:
                raise ValueError('injected second calendar rejection')
            return value
        with mock.patch.object(h, 'read_leaf_primary_key_summary', side_effect=read):
            with self.assertRaises(RuntimeError):
                h.commit_calendar_partitions(pd.concat([self.new,new_second], ignore_index=True), {('XDCE','EB',DAY),('XDCE','EB',second_day)}, self.lake)
        after = hashes(self.target)
        for name, value in old_hashes.items():
            if name.startswith(self.relative.as_posix()+'/'):
                self.assertNotEqual(after[name], value)
            else:
                self.assertEqual(after[name], value)
        self.assertEqual(self.output.getvalue().count('partition_committed:'), 1)
        self.assertNotIn('phase=calendar_state; status=completed;', self.output.getvalue())
        self.assertEqual(len(self.paths('failed')), 1)
        self.assertFalse(self.paths('staging'))
        self.assertFalse(self.paths('backup'))


class EntryTests(unittest.TestCase):
    def test_entry_routes_notebook_script_and_import(self):
        notebook = json.loads(fixtures.NOTEBOOK_PATH.read_text(encoding='utf8'))
        entry = next(c for c in notebook['cells'] if c['id'] == 'd4e12ca7')
        code = compile(''.join(entry['source']), 'entry_cell', 'exec')
        for kernel, has_file, name, route in (
            (True, False, '__main__', 'notebook'),
            (True, True, 'imported_module', 'none'),
            (False, True, 'imported_module', 'none'),
            (False, True, '__main__', 'script'),
            (True, True, '__main__', 'script'),
        ):
            with self.subTest(kernel=kernel, has_file=has_file, name=name):
                main = mock.Mock()
                namespace = {'sys':mock.Mock(modules={'ipykernel':object()} if kernel else {}), 'main':main, '__name__':name}
                if has_file:
                    namespace['__file__'] = 'module.py'
                exec(code, namespace)
                if route == 'notebook':
                    main.main.assert_called_once_with(args=[], prog_name='c03_warehouse_receipt', standalone_mode=False)
                    main.assert_not_called()
                elif route == 'script':
                    main.assert_called_once_with()
                    main.main.assert_not_called()
                else:
                    self.assertEqual(main.mock_calls, [])

    def test_terminal_cell_is_comment_only(self):
        notebook = json.loads(fixtures.NOTEBOOK_PATH.read_text(encoding='utf8'))
        cell = notebook['cells'][-1]
        self.assertEqual(cell['id'], 'a02-b03-terminal-command')
        self.assertEqual(ast.parse(''.join(cell['source'])).body, [])
        self.assertIn('c03_warehouse_receipt.py --write', ''.join(cell['source']))
        self.assertEqual(cell['outputs'], [])
        self.assertIsNone(cell['execution_count'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
