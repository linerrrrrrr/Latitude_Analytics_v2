"""b07 整批事务及执行入口：临时 Parquet 和明确 I/O 故障注入，无 API/正式湖访问。"""

import contextlib
import io
import pathlib
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import nbformat
import pandas as pd

import test_b01_ohlc_retention as fixture

b07 = fixture.reconciliation
transaction_module = sys.modules[b07.StagedPathTransaction.__module__]
NOTEBOOK = pathlib.Path(b07.__file__).with_suffix('.ipynb')


class B07SharedTransactionTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='b07-transaction-')
        self.addCleanup(temporary.cleanup)
        self.root = pathlib.Path(temporary.name).resolve()
        self.silver = self.root / 'silver'
        self.target = self.silver / b07.CALENDAR_TABLE_NAME
        self.output = io.StringIO()
        capture = contextlib.redirect_stdout(self.output)
        capture.__enter__()
        self.addCleanup(capture.__exit__, None, None, None)

    def frames(self, months=(8, 9), reason='old evidence'):
        frames = {}
        for month in months:
            row = fixture.calendar_row(
                bar_frequency='1m', quality_status='passed', quality_reason=reason,
                evidence_source='fact_futures_minute:formal_nonempty_session',
            )
            delta = pd.DateOffset(months=month - 8)
            row.update(month=month, trading_date=(pd.Timestamp(row['trading_date']) + delta).date())
            row['session_start_at'] += delta
            row['session_end_at'] += delta
            frames[('1m', 'XSGE', 2026, month)] = pd.DataFrame([row]).loc[:, b07.FUTURES_BAR_CALENDAR_SCHEMA.names]
        return frames

    def leaf(self, month):
        return self.target / f'bar_frequency=1m/exchange_code=XSGE/year=2026/month={month}'

    def seed(self, months=(8, 9)):
        for frame in self.frames(months).values():
            fixture.write_partitioned(frame, self.target, b07.FUTURES_BAR_CALENDAR_SCHEMA,
                                      b07.CALENDAR_PARTITION_COLUMNS)

    def stored_bytes(self):
        return {str(path.relative_to(self.target)): path.read_bytes() for path in self.target.rglob('*.parquet')}

    def clear_output(self):
        self.output.seek(0)
        self.output.truncate()

    def assert_no_success(self):
        output = self.output.getvalue()
        self.assertNotIn('committed:', output)
        self.assertNotIn('persisted=true', output)
        self.assertNotIn('phase=evidence_state;', output)
        self.assertIn('phase=commit; status=failed', output)

    def assert_recovered(self, original, isolated_count):
        self.assertEqual(self.stored_bytes(), original)
        self.assertFalse(list(self.silver.glob('.*.staging-*')))
        self.assertFalse(list(self.silver.glob('.*.backup-*')))
        self.assertEqual(len(list(self.silver.glob('.*.failed-*/**/*.parquet'))), isolated_count)
        self.assertIn('phase=rollback; status=completed', self.output.getvalue())
        self.assert_no_success()

    def test_success_keeps_marker_and_clean_leaf_and_reports_only_after_whole_batch(self):
        self.seed((7, 8, 9))
        marker = self.target / 'schema.parquet'
        b07.pq.write_table(b07.pa.Table.from_batches([], schema=b07.CALENDAR_FILE_SCHEMA), marker)
        before = self.stored_bytes()
        self.clear_output()
        original = transaction_module.os.replace
        with patch.object(transaction_module.os, 'replace', wraps=original) as moves:
            count = b07.commit_calendar_partitions(self.frames(reason='new evidence'), self.root)
        self.assertEqual(count, 2)
        after = self.stored_bytes()
        for name, content in before.items():
            if 'month=7' in name or name == 'schema.parquet':
                self.assertEqual(after[name], content)
        for month in (8, 9):
            leaf_table = b07.ds.dataset(self.leaf(month), format='parquet', partitioning=b07.CALENDAR_PARTITIONING,
                                        partition_base_dir=str(self.target)).to_table()
            self.assertEqual(leaf_table.column('quality_reason').to_pylist(), ['new evidence'])
        self.assertTrue(all(marker.resolve() not in call.args for call in moves.call_args_list))
        output = self.output.getvalue()
        self.assertEqual(output.count('phase=formal_readback; status=completed'), 2)
        self.assertLess(output.rindex('phase=formal_readback; status=completed'), output.index('committed:'))
        self.assertIn('batch_state=pending', output)
        self.assertIn('date_watermark=none', output)
        self.assertFalse(list(self.silver.glob('.*')))

    def test_empty_mapping_does_not_create_paths_or_enter_transaction(self):
        with patch.object(b07, 'StagedPathTransaction') as transaction:
            self.assertEqual(b07.commit_calendar_partitions({}, self.root), 0)
        transaction.assert_not_called()
        self.assertFalse(self.silver.exists())
        self.assertIn('outcome=no_partitions', self.output.getvalue())

    def test_success_without_marker_does_not_create_marker(self):
        self.assertEqual(b07.commit_calendar_partitions(self.frames(), self.root), 2)
        self.assertFalse((self.target / 'schema.parquet').exists())
        self.assertEqual(len(list(self.target.rglob('*.parquet'))), 2)

    def test_staging_and_transaction_enter_failures_do_not_touch_formal_data(self):
        self.seed()
        before = self.stored_bytes()
        for phase in ('write', 'readback', 'enter'):
            with self.subTest(phase=phase):
                self.clear_output()
                owner, method = {'write': (b07.ds, 'write_dataset'), 'readback': (b07.pq, 'read_schema'),
                                 'enter': (b07.StagedPathTransaction, '__enter__')}[phase]
                with patch.object(owner, method, side_effect=OSError('injected ' + phase)):
                    with self.assertRaisesRegex(OSError, 'injected ' + phase):
                        b07.commit_calendar_partitions(self.frames(reason='new evidence'), self.root)
                self.assertEqual(self.stored_bytes(), before)
                self.assertFalse(list(self.silver.glob('.*')))
                self.assert_no_success()

    def test_first_backup_failure_preserves_old_leaf(self):
        self.seed()
        before = self.stored_bytes()
        original = transaction_module.os.replace

        def fail(source, target):
            if '.backup-' in str(target):
                raise PermissionError('injected first backup failure')
            return original(source, target)

        with patch.object(transaction_module.os, 'replace', side_effect=fail):
            with self.assertRaisesRegex(PermissionError, 'first backup failure'):
                b07.commit_calendar_partitions(self.frames(reason='new evidence'), self.root)
        self.assert_recovered(before, 0)

    def test_first_install_failure_restores_saved_old_leaf(self):
        self.seed()
        before = self.stored_bytes()
        original = transaction_module.os.replace

        def fail(source, target):
            if '.staging-' in str(source):
                raise OSError('injected first install failure')
            return original(source, target)

        with patch.object(transaction_module.os, 'replace', side_effect=fail):
            with self.assertRaisesRegex(OSError, 'first install failure'):
                b07.commit_calendar_partitions(self.frames(reason='new evidence'), self.root)
        self.assert_recovered(before, 0)

    def test_second_install_failure_restores_first_verified_leaf_too(self):
        self.seed()
        before = self.stored_bytes()
        original = transaction_module.os.replace
        error = OSError('injected second install failure')

        def fail(source, target):
            if '.staging-' in str(source) and target == self.leaf(9):
                raise error
            return original(source, target)

        with patch.object(transaction_module.os, 'replace', side_effect=fail):
            with self.assertRaisesRegex(RuntimeError, '旧目标已恢复') as caught:
                b07.commit_calendar_partitions(self.frames(reason='new evidence'), self.root)
        self.assertIs(caught.exception.__cause__, error)
        self.assert_recovered(before, 1)
        self.assertIn('verified_partitions=1/2', self.output.getvalue())

    def test_second_formal_failure_restores_entire_batch_including_new_leaf_case(self):
        for old_months in ((8, 9), (8,)):
            with self.subTest(old_months=old_months), tempfile.TemporaryDirectory(prefix='b07-batch-') as directory:
                self.root = pathlib.Path(directory).resolve()
                self.silver = self.root / 'silver'
                self.target = self.silver / b07.CALENDAR_TABLE_NAME
                self.seed(old_months)
                before = self.stored_bytes()
                self.clear_output()
                original = b07.ds.dataset
                error = OSError('injected second formal readback failure')

                def fail(path, *args, **kwargs):
                    if pathlib.Path(path).resolve() == self.leaf(9):
                        raise error
                    return original(path, *args, **kwargs)

                with patch.object(b07.ds, 'dataset', side_effect=fail):
                    with self.assertRaisesRegex(RuntimeError, '旧目标已恢复') as caught:
                        b07.commit_calendar_partitions(self.frames(reason='new evidence'), self.root)
                self.assertIs(caught.exception.__cause__, error)
                self.assert_recovered(before, 2)
                self.assertEqual(self.leaf(9).exists(), 9 in old_months)
                self.assertIn('verified_partitions=1/2', self.output.getvalue())

    def test_restore_failure_keeps_evidence_and_continues_restoring_earlier_leaf(self):
        self.seed()
        before = self.stored_bytes()
        old_late = next(self.leaf(9).glob('*.parquet')).read_bytes()
        original_read, original_move = b07.ds.dataset, transaction_module.os.replace
        error = OSError('injected second formal readback failure')

        def fail_read(path, *args, **kwargs):
            if pathlib.Path(path).resolve() == self.leaf(9):
                raise error
            return original_read(path, *args, **kwargs)

        def fail_restore(source, target):
            if '.backup-' in str(source) and target == self.leaf(9):
                raise PermissionError('injected late leaf restore failure')
            return original_move(source, target)

        with patch.object(b07.ds, 'dataset', side_effect=fail_read), patch.object(transaction_module.os, 'replace', side_effect=fail_restore):
            with self.assertRaisesRegex(RuntimeError, '回滚不完整') as caught:
                b07.commit_calendar_partitions(self.frames(reason='new evidence'), self.root)
        self.assertIs(caught.exception.__cause__, error)
        after = self.stored_bytes()
        self.assertEqual(after, {name: value for name, value in before.items() if 'month=8' in name})
        backup_files = list(self.silver.glob('.*.backup-*/**/*.parquet'))
        self.assertEqual(len(backup_files), 1)
        self.assertEqual(backup_files[0].read_bytes(), old_late)
        self.assertEqual(len(list(self.silver.glob('.*.failed-*/**/*.parquet'))), 2)
        self.assertFalse(list(self.silver.glob('.*.staging-*')))
        self.assertIn('phase=rollback; status=failed', self.output.getvalue())
        self.assert_no_success()


class B07EntryCellTest(unittest.TestCase):
    def test_notebook_script_and_import_routes(self):
        notebook = nbformat.read(NOTEBOOK, as_version=4)
        source = next(cell.source for cell in notebook.cells if cell.id == 'b07-entry')
        cases = [
            ({'ipykernel': object()}, {}, '__main__', 'notebook'),
            ({}, {'__file__': str(NOTEBOOK.with_suffix('.py'))}, '__main__', 'script'),
            ({}, {'__file__': str(NOTEBOOK.with_suffix('.py'))}, 'imported_b07', 'import'),
            ({'ipykernel': object()}, {'__file__': str(NOTEBOOK.with_suffix('.py'))}, 'imported_b07', 'import'),
        ]
        for modules, extras, name, expected in cases:
            with self.subTest(expected=expected, kernel=bool(modules)):
                command = Mock()
                namespace = {'__name__': name, 'main': command,
                             'sys': SimpleNamespace(modules=modules, argv=['ipykernel_launcher.py', '-f', 'connection.json']), **extras}
                exec(compile(source, str(NOTEBOOK), 'exec'), namespace)
                if expected == 'notebook':
                    command.assert_not_called()
                    command.main.assert_called_once_with(
                        args=['--start-date', '2026-08-01', '--end-date', '2026-08-15'],
                        prog_name='c07_suspected_session_reconciliation', standalone_mode=False,
                    )
                elif expected == 'script':
                    command.assert_called_once_with()
                    command.main.assert_not_called()
                else:
                    self.assertEqual(command.mock_calls, [])


if __name__ == '__main__':
    unittest.main()
