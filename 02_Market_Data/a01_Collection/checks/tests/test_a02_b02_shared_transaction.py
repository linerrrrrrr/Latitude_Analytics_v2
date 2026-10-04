"""b02/c02 单叶事务的真实文件移动故障注入；仅使用临时湖。"""
from __future__ import annotations

import ast
import contextlib
import io
import json
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

import pandas as pd

import test_b02_futures_holding_reports as fixtures
import b00_04_staged_path_transaction as transactions

h = fixtures.holding_reports
KEY = ('XDCE', 'BB', 2014, 4)
RELATIVE = pathlib.Path('exchange_code=XDCE/underlying_code=BB/year=2014/month=4')


class LeafTransactionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='a02-b02-transaction-test-')
        self.addCleanup(self.temporary.cleanup)
        self.lake = pathlib.Path(self.temporary.name)
        self.silver = self.lake/'silver'
        self.target = self.silver/h.POSITION_TABLE_NAME
        self.leaf = self.target/RELATIVE
        self.marker = self.target/'schema.parquet'
        self.output = io.StringIO()
        self.redirect = contextlib.redirect_stdout(self.output)
        self.redirect.__enter__()
        self.addCleanup(self.redirect.__exit__, None, None, None)
        self.old, self.member = fixtures.normalized_fact_frames('BB', fixtures.GRID['trading_date'], 6, 20)
        self.new = self.old.copy()
        self.new['volume'] = 9.0
        self.empty = h.empty_pandas(h.FUTURES_POSITION_RANK_DAILY_SCHEMA)

    def commit(self, frame=None):
        return h.commit_complete_partition(
            self.new if frame is None else frame, self.lake, h.POSITION_TABLE_NAME,
            h.FUTURES_POSITION_RANK_DAILY_SCHEMA, h.POSITION_PARTITION_COLUMNS,
            h.POSITION_PARTITIONING, KEY, h.validate_position_frame,
        )

    def seed(self):
        self.commit(self.old)
        self.output.seek(0)
        self.output.truncate()
        return fixtures.parquet_hashes(self.target)

    def recovery_paths(self, kind):
        return list(self.silver.glob(f'.{h.POSITION_TABLE_NAME}.{kind}-*'))

    def assert_failed(self):
        self.assertNotIn('partition_committed:', self.output.getvalue())
        self.assertIn('phase=commit; status=failed;', self.output.getvalue())
        self.assertFalse(self.recovery_paths('staging'))

    def reject_formal(self):
        original = h.read_complete_partition
        def read(*args, **kwargs):
            result = original(*args, **kwargs)
            if args[-1] == f'正式 {h.POSITION_TABLE_NAME}':
                raise ValueError('injected formal rejection')
            return result
        return mock.patch.object(h, 'read_complete_partition', side_effect=read)

    def test_three_table_kinds_commit_and_reread(self):
        calendar = pd.DataFrame([fixtures.calendar_row('position_rank', 'BB', fixtures.GRID['trading_date'], completed=False)])
        cases = (
            (self.new, h.POSITION_TABLE_NAME, h.FUTURES_POSITION_RANK_DAILY_SCHEMA, h.POSITION_PARTITION_COLUMNS, h.POSITION_PARTITIONING, KEY, h.validate_position_frame),
            (self.member, h.MEMBER_TABLE_NAME, h.FUTURES_MEMBER_POSITION_DAILY_SCHEMA, h.MEMBER_PARTITION_COLUMNS, h.MEMBER_PARTITIONING, KEY, h.validate_member_frame),
            (calendar, h.CALENDAR_TABLE_NAME, h.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA, h.CALENDAR_PARTITION_COLUMNS, h.CALENDAR_PARTITIONING, ('position_rank', 'XDCE', 2014, 4), h.validate_calendar_frame),
        )
        for frame, name, schema, columns, partitioning, key, validator in cases:
            with self.subTest(table=name):
                result = h.commit_complete_partition(frame, self.lake, name, schema, columns, partitioning, key, validator)
                pd.testing.assert_frame_equal(result, validator(frame, 'expected'))
                self.assertEqual(h.pq.ParquetFile(self.silver/name/'schema.parquet').metadata.num_rows, 0)
        self.assertEqual(self.output.getvalue().count('partition_committed:'), 3)
        self.assertFalse(list(self.silver.glob('.*')))

    def test_replacement_and_explicit_empty_preserve_unrelated_files(self):
        self.seed()
        unrelated = self.target/'exchange_code=XDCE/underlying_code=A/year=2014/month=4'
        unrelated.mkdir(parents=True)
        (unrelated/'evidence.bin').write_bytes(b'unrelated leaf')
        marker = self.marker.read_bytes()
        self.commit()
        self.assertEqual(self.marker.read_bytes(), marker)
        self.assertEqual((unrelated/'evidence.bin').read_bytes(), b'unrelated leaf')
        result = self.commit(self.empty)
        self.assertTrue(result.empty)
        self.assertFalse(self.leaf.exists())
        self.assertEqual(self.marker.read_bytes(), marker)
        self.assertEqual((unrelated/'evidence.bin').read_bytes(), b'unrelated leaf')
        self.assertFalse(list(self.silver.glob('.*')))

    def test_empty_new_table_keeps_readable_zero_marker(self):
        result = self.commit(self.empty)
        self.assertTrue(result.empty)
        self.assertTrue(self.marker.exists())
        self.assertFalse(self.leaf.exists())
        self.assertEqual(h.open_exact_dataset(self.target, h.POSITION_PARTITIONING, h.FUTURES_POSITION_RANK_DAILY_SCHEMA, 'empty').count_rows(), 0)

    def test_staging_write_failure_preserves_old_data(self):
        before = self.seed()
        with mock.patch.object(h.pq, 'write_table', side_effect=OSError('injected staging write')):
            with self.assertRaisesRegex(OSError, 'staging write'):
                self.commit()
        self.assertEqual(fixtures.parquet_hashes(self.target), before)
        self.assert_failed()

    def test_staging_validation_failure_preserves_old_data(self):
        before = self.seed()
        with mock.patch.object(h, 'read_complete_partition', side_effect=ValueError('injected staging verify')):
            with self.assertRaisesRegex(ValueError, 'staging verify'):
                self.commit()
        self.assertEqual(fixtures.parquet_hashes(self.target), before)
        self.assertFalse(list(self.silver.glob('.*')))
        self.assert_failed()

    def test_transaction_entry_failure_cleans_staging(self):
        before = self.seed()
        with mock.patch.object(h.StagedPathTransaction, '__enter__', side_effect=OSError('injected enter')):
            with self.assertRaisesRegex(OSError, 'injected enter'):
                self.commit()
        self.assertEqual(fixtures.parquet_hashes(self.target), before)
        self.assertFalse(list(self.silver.glob('.*')))
        self.assert_failed()

    def test_first_backup_failure_does_not_remove_original(self):
        before = self.seed()
        original = transactions.os.replace
        error = PermissionError('injected first backup failure')
        def move(source, target):
            if pathlib.Path(source) == self.leaf:
                raise error
            return original(source, target)
        with mock.patch.object(transactions.os, 'replace', side_effect=move):
            with self.assertRaises(PermissionError) as caught:
                self.commit()
        self.assertIs(caught.exception, error)
        self.assertEqual(fixtures.parquet_hashes(self.target), before)
        self.assertFalse(list(self.silver.glob('.*')))
        self.assert_failed()

    def test_install_failure_after_backup_restores_old_leaf(self):
        before = self.seed()
        original = transactions.os.replace
        def move(source, target):
            if pathlib.Path(target) == self.leaf and '.staging-' in str(source):
                raise OSError('injected install failure')
            return original(source, target)
        with mock.patch.object(transactions.os, 'replace', side_effect=move):
            with self.assertRaisesRegex(OSError, 'install failure'):
                self.commit()
        self.assertEqual(fixtures.parquet_hashes(self.target), before)
        self.assertFalse(list(self.silver.glob('.*')))
        self.assert_failed()

    def test_new_marker_install_failure_restores_absence(self):
        original = transactions.os.replace
        def move(source, target):
            if pathlib.Path(target) == self.marker:
                raise OSError('injected marker install')
            return original(source, target)
        with mock.patch.object(transactions.os, 'replace', side_effect=move):
            with self.assertRaisesRegex(OSError, 'marker install'):
                self.commit()
        self.assertFalse(self.target.exists())
        self.assertFalse(list(self.silver.glob('.*')))
        self.assert_failed()

    def test_new_leaf_install_failure_removes_new_marker(self):
        original = transactions.os.replace
        def move(source, target):
            if pathlib.Path(target) == self.leaf:
                raise OSError('injected leaf install')
            return original(source, target)
        with mock.patch.object(transactions.os, 'replace', side_effect=move):
            with self.assertRaisesRegex(OSError, 'leaf install'):
                self.commit()
        self.assertFalse(self.target.exists())
        self.assertFalse(list(self.silver.glob('.*')))
        self.assert_failed()

    def test_formal_rejection_restores_old_and_keeps_failed_new_leaf(self):
        before = self.seed()
        with self.reject_formal():
            with self.assertRaises(RuntimeError) as caught:
                self.commit()
        self.assertIsInstance(caught.exception.__cause__, ValueError)
        self.assertEqual(fixtures.parquet_hashes(self.target), before)
        failed = self.recovery_paths('failed')
        self.assertEqual(len(failed), 1)
        self.assertTrue((failed[0]/RELATIVE/'part-0.parquet').exists())
        self.assertFalse(self.recovery_paths('backup'))
        self.assert_failed()

    def test_empty_replacement_rejection_restores_deleted_old_leaf(self):
        before = self.seed()
        with self.reject_formal():
            with self.assertRaisesRegex(ValueError, 'formal rejection'):
                self.commit(self.empty)
        self.assertEqual(fixtures.parquet_hashes(self.target), before)
        self.assertFalse(list(self.silver.glob('.*')))
        self.assert_failed()

    def test_new_table_formal_rejection_removes_marker_and_keeps_failed_leaf(self):
        with self.reject_formal():
            with self.assertRaises(RuntimeError):
                self.commit()
        self.assertFalse(self.target.exists())
        self.assertEqual(len(self.recovery_paths('failed')), 1)
        self.assertFalse(self.recovery_paths('backup'))
        self.assert_failed()

    def test_new_empty_table_formal_rejection_removes_marker(self):
        with self.reject_formal():
            with self.assertRaisesRegex(ValueError, 'formal rejection'):
                self.commit(self.empty)
        self.assertFalse(self.target.exists())
        self.assertFalse(list(self.silver.glob('.*')))
        self.assert_failed()

    def test_restore_failure_keeps_old_backup_and_new_evidence(self):
        before = self.seed()
        original = transactions.os.replace
        def move(source, target):
            if pathlib.Path(target) == self.leaf and '.backup-' in str(source):
                raise PermissionError('injected restore failure')
            return original(source, target)
        with self.reject_formal(), mock.patch.object(transactions.os, 'replace', side_effect=move):
            with self.assertRaisesRegex(RuntimeError, '回滚不完整') as caught:
                self.commit()
        self.assertIsInstance(caught.exception.__cause__, ValueError)
        backups = self.recovery_paths('backup')
        self.assertEqual(len(backups), 1)
        self.assertEqual(fixtures.parquet_hashes(backups[0])[str(RELATIVE.as_posix())+'/part-0.parquet'], before[str(RELATIVE.as_posix())+'/part-0.parquet'])
        self.assertEqual(len(self.recovery_paths('failed')), 1)
        self.assertIn('phase=rollback; status=failed;', self.output.getvalue())
        self.assert_failed()


class EntryTests(unittest.TestCase):
    def test_entry_routes_notebook_script_and_imports(self):
        notebook = json.loads(fixtures.MODULE_PATH.with_suffix('.ipynb').read_text(encoding='utf8'))
        cell = next(c for c in notebook['cells'] if c['id'] == 'abaf342b')
        code = compile(''.join(cell['source']), 'entry_cell', 'exec')
        for kernel, has_file, name, route in (
            (True, False, '__main__', 'notebook'),
            (True, True, 'imported_module', 'none'),
            (False, True, 'imported_module', 'none'),
            (False, True, '__main__', 'script'),
            (True, True, '__main__', 'script'),
        ):
            with self.subTest(kernel=kernel, has_file=has_file, name=name):
                main = mock.Mock()
                namespace = {'sys': mock.Mock(modules={'ipykernel': object()} if kernel else {}), 'main': main, '__name__': name}
                if has_file:
                    namespace['__file__'] = 'module.py'
                exec(code, namespace)
                if route == 'notebook':
                    main.main.assert_called_once_with(args=[], prog_name='c02_futures_holding_reports', standalone_mode=False)
                    main.assert_not_called()
                elif route == 'script':
                    main.assert_called_once_with()
                    main.main.assert_not_called()
                else:
                    self.assertEqual(main.mock_calls, [])

    def test_terminal_cell_has_comments_only(self):
        notebook = json.loads(fixtures.MODULE_PATH.with_suffix('.ipynb').read_text(encoding='utf8'))
        terminal = notebook['cells'][-1]
        self.assertEqual(terminal['id'], 'a02-b02-terminal-command')
        source = ''.join(terminal['source'])
        self.assertEqual(ast.parse(source).body, [])
        self.assertIn('c02_futures_holding_reports.py --write', source)
        self.assertIsNone(terminal['execution_count'])
        self.assertEqual(terminal['outputs'], [])


if __name__ == '__main__':
    unittest.main(verbosity=2)
