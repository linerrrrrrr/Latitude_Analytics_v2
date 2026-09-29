"""b08 整根＋多叶共享事务与执行入口；仅使用临时 Parquet。"""
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
from click.testing import CliRunner

import test_b01_ohlc_retention as fixture

b08 = fixture.full_quality
transaction_module = sys.modules[b08.StagedPathTransaction.__module__]


class B08SharedTransactionTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='b08-transaction-')
        self.addCleanup(temporary.cleanup)
        self.root = pathlib.Path(temporary.name).resolve()
        self.silver = self.root / 'silver'
        self.calendar = self.silver / b08.CALENDAR_TABLE_NAME
        self.missing = self.silver / b08.MISSING_TABLE_NAME
        self.staging = self.silver / '.b08-s-test'
        self.backup = self.silver / '.b08-b-test'
        self.quarantine = self.silver / '.b08-f-test'
        self.output = io.StringIO()
        capture = contextlib.redirect_stdout(self.output)
        capture.__enter__()
        self.addCleanup(capture.__exit__, None, None, None)

    def seed(self, scenario='missing', existing_missing=True):
        calendar_rows, minute_rows = [], []
        for month in (7, 8, 9):
            row = fixture.calendar_row(bar_frequency='1m', quality_status='warning',
                                       quality_reason='保留来源 OHLC warning', evidence_source='c07:old_evidence')
            start = fixture.SESSION_START + pd.DateOffset(months=month - 8)
            required = month != 7 and scenario != 'no_required'
            numbers = (1, 2, 3, 4) if scenario == 'zero_missing' else (1, 2, 4)
            row.update(month=month, trading_date=start.date(), session_start_at=start,
                       session_end_at=start + pd.Timedelta(minutes=4), session_text='09:00-09:04',
                       expected_bar_count=4, actual_bar_count=len(numbers), is_fetch_required=required,
                       is_data_missing=required and len(numbers) < 4, missing_bar_count=4-len(numbers) if required else 0)
            calendar_rows.append(row)
            minute_rows.extend(dict(fixture.minute_row(), month=month, trading_date=start.date(),
                                    bar_at=start + pd.Timedelta(minutes=n)) for n in numbers)
        calendar_rows.append(fixture.calendar_row(bar_frequency='1d', quality_status='passed',
                                                  quality_reason='保留日线', evidence_source='fact_futures_daily'))
        for rows, schema, partitions in (
            (calendar_rows, b08.FUTURES_BAR_CALENDAR_SCHEMA, b08.CALENDAR_PARTITION_COLUMNS),
            (minute_rows, b08.FUTURES_MINUTE_SCHEMA, b08.MINUTE_PARTITION_COLUMNS),
        ):
            fixture.write_partitioned(pd.DataFrame(rows), self.silver/schema.metadata[b'table_name'].decode(), schema, partitions)
        b08.pq.write_table(b08.pa.Table.from_batches([], schema=b08.parquet_file_schema(
            b08.FUTURES_BAR_CALENDAR_SCHEMA, b08.CALENDAR_PARTITION_COLUMNS)), self.calendar/'schema.parquet')
        if existing_missing:
            row = calendar_rows[0]
            stale = {name: row[name] for name in ('bar_frequency', 'contract_code', 'exchange_code',
                                                 'underlying_code', 'trading_date', 'session_number', 'year', 'month')}
            stale.update(expected_bar_at=row['session_start_at'] + pd.Timedelta(minutes=3), detected_at=fixture.CHECKED_AT)
            fixture.write_partitioned(pd.DataFrame([stale]), self.missing, b08.FUTURES_MISSING_BAR_SCHEMA,
                                      b08.MISSING_PARTITION_COLUMNS)

    def leaf(self, month):
        return self.calendar / f'bar_frequency=1m/exchange_code=XSGE/year=2026/month={month}'

    def stored_bytes(self):
        return {str(p.relative_to(self.silver)): p.read_bytes()
                for name in (b08.CALENDAR_TABLE_NAME, b08.MINUTE_TABLE_NAME, b08.MISSING_TABLE_NAME)
                for p in (self.silver/name).rglob('*.parquet')}

    def prepare(self):
        audit = b08.build_full_audit_staging(self.root, self.staging/b08.MISSING_TABLE_NAME,
                                            self.staging/b08.CALENDAR_TABLE_NAME, fixture.CHECKED_AT)
        self.output.seek(0)
        self.output.truncate()
        return audit

    def commit(self, audit):
        return b08.commit_full_audit(self.root, self.staging, audit, 'test')

    def assert_no_success(self):
        self.assertNotIn('committed:', self.output.getvalue())
        self.assertNotIn('persisted=true', self.output.getvalue())
        self.assertIn('phase=commit; status=failed', self.output.getvalue())

    def assert_recovered(self, previous, quarantine=True):
        self.assertEqual(self.stored_bytes(), previous)
        self.assertFalse(self.staging.exists())
        self.assertFalse(self.backup.exists())
        self.assertEqual(self.quarantine.exists(), quarantine)
        self.assertIn('phase=rollback; status=completed', self.output.getvalue())
        self.assert_no_success()

    def test_success_is_one_transaction_and_preserves_clean_data_and_marker(self):
        self.seed()
        before = self.stored_bytes()
        audit = self.prepare()
        original = transaction_module.os.replace
        with patch.object(transaction_module.os, 'replace', wraps=original) as moves:
            self.assertEqual(self.commit(audit), (2, 2))
        installed = [pathlib.Path(call.args[1]) for call in moves.call_args_list
                     if pathlib.Path(call.args[0]).is_relative_to(self.staging)]
        self.assertEqual(installed, [self.missing, self.leaf(8), self.leaf(9)])
        after = self.stored_bytes()
        for name, content in before.items():
            if name.startswith(b08.MINUTE_TABLE_NAME) or name.startswith(b08.CALENDAR_TABLE_NAME) and (
                'month=7' in name or 'bar_frequency=1d' in name or name.endswith('schema.parquet')
            ):
                self.assertEqual(after[name], content)
        self.assertEqual(b08.discover_partition_keys(self.missing, b08.MISSING_PARTITION_COLUMNS), set(audit['missing_digests']))
        output = self.output.getvalue()
        self.assertLess(output.rindex('phase=calendar_formal_readback; status=completed'), output.index('committed:'))
        self.assertIn('date_watermark=none', output)
        self.assertFalse(list(self.silver.glob('.b08-*')))

    def test_zero_missing_and_no_required_replace_stale_missing_snapshot(self):
        for scenario in ('zero_missing', 'no_required'):
            with self.subTest(scenario=scenario):
                self.seed(scenario)
                original_calendar = {p: value for p, value in self.stored_bytes().items() if p.startswith(b08.CALENDAR_TABLE_NAME)}
                audit = self.prepare()
                self.assertEqual(self.commit(audit), (0, 0 if scenario == 'no_required' else 2))
                self.assertEqual([p.name for p in self.missing.rglob('*.parquet')], ['schema.parquet'])
                if scenario == 'no_required':
                    self.assertEqual({p: value for p, value in self.stored_bytes().items() if p.startswith(b08.CALENDAR_TABLE_NAME)}, original_calendar)
                self.assertFalse(list(self.silver.glob('.b08-*')))

    def test_transaction_enter_failure_cleans_staging_without_touching_data(self):
        self.seed()
        before = self.stored_bytes()
        audit = self.prepare()
        with patch.object(b08.StagedPathTransaction, '__enter__', side_effect=OSError('injected enter failure')):
            with self.assertRaisesRegex(OSError, 'injected enter failure'):
                self.commit(audit)
        self.assertEqual(self.stored_bytes(), before)
        self.assertFalse(list(self.silver.glob('.b08-*')))
        self.assert_no_success()

    def test_first_backup_failure_leaves_old_table_in_place(self):
        self.seed()
        before = self.stored_bytes()
        audit = self.prepare()
        original = transaction_module.os.replace
        failure = PermissionError('injected first backup failure')

        def move(source, target):
            if pathlib.Path(source) == self.missing:
                raise failure
            return original(source, target)

        with patch.object(transaction_module.os, 'replace', side_effect=move):
            with self.assertRaises(PermissionError) as caught:
                self.commit(audit)
        self.assertIs(caught.exception, failure)
        self.assert_recovered(before, quarantine=False)

    def test_missing_install_failure_restores_backup(self):
        self.seed()
        before = self.stored_bytes()
        audit = self.prepare()
        original = transaction_module.os.replace
        failure = OSError('injected missing install failure')

        def move(source, target):
            if pathlib.Path(source) == self.staging/b08.MISSING_TABLE_NAME:
                raise failure
            return original(source, target)

        with patch.object(transaction_module.os, 'replace', side_effect=move):
            with self.assertRaises(OSError) as caught:
                self.commit(audit)
        self.assertIs(caught.exception, failure)
        self.assert_recovered(before, quarantine=False)

    def test_second_calendar_install_failure_restores_entire_batch(self):
        self.seed()
        before = self.stored_bytes()
        audit = self.prepare()
        original = transaction_module.os.replace
        failure = OSError('injected second calendar install failure')

        def move(source, target):
            if pathlib.Path(source).is_relative_to(self.staging) and pathlib.Path(target) == self.leaf(9):
                raise failure
            return original(source, target)

        with patch.object(transaction_module.os, 'replace', side_effect=move):
            with self.assertRaises(RuntimeError) as caught:
                self.commit(audit)
        self.assertIs(caught.exception.__cause__, failure)
        self.assert_recovered(before)
        self.assertTrue((self.quarantine/b08.MISSING_TABLE_NAME).exists())
        self.assertTrue((self.quarantine/self.leaf(8).relative_to(self.silver)).exists())

    def test_formal_failure_restores_two_tables_and_absent_missing_root(self):
        for existing_missing in (True, False):
            for label in ('正式缺失明细', '正式行情日历'):
                with self.subTest(existing_missing=existing_missing, label=label):
                    # 每轮使用独立目标和事务路径，避免上一轮隔离证据影响本轮。
                    with tempfile.TemporaryDirectory(prefix='b08-recovery-case-') as directory:
                        root = pathlib.Path(directory).resolve()
                        self.root, self.silver = root, root/'silver'
                        self.calendar, self.missing = self.silver/b08.CALENDAR_TABLE_NAME, self.silver/b08.MISSING_TABLE_NAME
                        self.staging, self.backup, self.quarantine = (self.silver/f'.b08-{kind}-test' for kind in ('s', 'b', 'f'))
                        self.seed(existing_missing=existing_missing)
                        before = self.stored_bytes()
                        audit = self.prepare()
                        original = b08.open_exact_dataset
                        failure = OSError('injected formal failure')

                        def open_dataset(*args, **kwargs):
                            if args[4] == label:
                                raise failure
                            return original(*args, **kwargs)

                        with patch.object(b08, 'open_exact_dataset', side_effect=open_dataset):
                            with self.assertRaises(RuntimeError) as caught:
                                self.commit(audit)
                        self.assertIs(caught.exception.__cause__, failure)
                        self.assert_recovered(before)
                        self.assertEqual(self.missing.exists(), existing_missing)

    def test_failed_calendar_restore_does_not_block_other_leaf_or_missing_root(self):
        self.seed()
        before = self.stored_bytes()
        audit = self.prepare()
        original_move, original_open = transaction_module.os.replace, b08.open_exact_dataset
        failure = OSError('injected formal failure')

        def open_dataset(*args, **kwargs):
            if args[4] == '正式行情日历':
                raise failure
            return original_open(*args, **kwargs)

        def move(source, target):
            if pathlib.Path(source).is_relative_to(self.backup) and pathlib.Path(target) == self.leaf(9):
                raise PermissionError('injected restore failure')
            return original_move(source, target)

        with patch.object(b08, 'open_exact_dataset', side_effect=open_dataset), patch.object(transaction_module.os, 'replace', side_effect=move):
            with self.assertRaisesRegex(RuntimeError, '回滚不完整') as caught:
                self.commit(audit)
        self.assertIs(caught.exception.__cause__, failure)
        self.assertIn('injected restore failure', str(caught.exception))
        self.assertTrue(self.backup.exists())
        self.assertTrue(self.quarantine.exists())
        self.assertFalse(self.staging.exists())
        after = self.stored_bytes()
        for name, value in before.items():
            if name.startswith(b08.CALENDAR_TABLE_NAME) and 'month=9' in name:
                self.assertNotIn(name, after)
                self.assertEqual((self.backup/name).read_bytes(), value)
            else:
                self.assertEqual(after[name], value)
        self.assert_no_success()

    def test_entry_routes_notebook_script_and_import(self):
        notebook = nbformat.read(pathlib.Path(b08.__file__).with_suffix('.ipynb'), as_version=4)
        source = next(c.source for c in notebook.cells if c.id == 'b08-entry')
        for mode in ('notebook', 'script', 'import'):
            with self.subTest(mode=mode):
                command = Mock()
                namespace = {'sys': SimpleNamespace(modules={'ipykernel': object()}, argv=['kernel', '-f', 'connection.json']),
                             'main': command, '__name__': '__main__' if mode != 'import' else 'imported_b08'}
                if mode != 'notebook':
                    namespace['__file__'] = b08.__file__
                exec(compile(source, '<entry>', 'exec'), namespace)
                if mode == 'notebook':
                    command.main.assert_called_once_with(args=['--confirm-full-quality'], prog_name='b08_full_minute_quality', standalone_mode=False)
                    command.assert_not_called()
                elif mode == 'script':
                    command.assert_called_once_with()
                    command.main.assert_not_called()
                else:
                    command.assert_not_called()
                    command.main.assert_not_called()

    def test_cli_confirmation_and_no_date_filter_remain(self):
        with patch.object(b08, 'build_full_audit_staging') as build:
            for args in ([], ['--write'], ['--confirm-full-quality', '--start-date', '2026-08-01']):
                result = CliRunner().invoke(b08.main, args)
                self.assertEqual(result.exit_code, 2, result.output)
            build.assert_not_called()


if __name__ == '__main__':
    unittest.main()
