"""b02/c01 共享事务与入口；仅使用临时 Parquet 和受控文件系统异常。"""
import contextlib
import importlib.util
import io
import pathlib
import sys
import tempfile
import unittest
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

import nbformat
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
SOURCE = ROOT / '02_Market_Data/a01_Collection/b02_Futures_Exchange_Reports/c01_exchange_report_calendar.py'
spec = importlib.util.spec_from_file_location('report_calendar_transaction_test', SOURCE)
calendar = importlib.util.module_from_spec(spec)
spec.loader.exec_module(calendar)
transaction_module = sys.modules[calendar.StagedPathTransaction.__module__]
NOW = datetime(2026, 9, 1, tzinfo=timezone.utc)


class ReportCalendarTransactionTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='report-tx-')
        self.addCleanup(temporary.cleanup)
        self.root = pathlib.Path(temporary.name).resolve()
        self.silver = self.root / 'silver'
        self.target = self.silver / calendar.TABLE_NAME
        self.staging = self.silver / '.a02-b01-s-test'
        self.backup = self.silver / '.a02-b01-b-test'
        self.quarantine = self.silver / '.a02-b01-f-test'
        self.output = io.StringIO()
        capture = contextlib.redirect_stdout(self.output)
        capture.__enter__()
        self.addCleanup(capture.__exit__, None, None, None)
        fixed_id = patch.object(calendar.uuid, 'uuid4', return_value=SimpleNamespace(hex='test'))
        fixed_id.start()
        self.addCleanup(fixed_id.stop)
        rows = [dict(exchange_code='XSGE', underlying_code='RB', trading_date=date(2024, month, 3),
                     active_contract_count=1, source='fixture', updated_at=NOW, year=2024, month=month)
                for month in (1, 2, 3)]
        self.upstream = calendar.arrow_to_pandas(
            calendar.pa.Table.from_pylist(rows, schema=calendar.FUTURES_VARIETY_CALENDAR_SCHEMA),
            calendar.FUTURES_VARIETY_CALENDAR_SCHEMA,
        )
        self.empty = calendar.empty_pandas(calendar.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA)
        self.expected = calendar.build_expected_calendar(self.upstream, self.empty, NOW)
        self.output.seek(0)
        self.output.truncate()

    def table(self, frame):
        return calendar.pandas_to_arrow(frame, calendar.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA)

    def seed(self, frame=None):
        calendar.ds.write_dataset(self.table(self.expected if frame is None else frame), self.target,
                                  format='parquet', partitioning=calendar.REPORT_PARTITIONING)
        file_schema = calendar.pa.schema(
            [field for field in calendar.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA if field.name not in calendar.PARTITION_COLUMNS],
            metadata=calendar.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.metadata,
        )
        calendar.pq.write_table(calendar.pa.Table.from_batches([], schema=file_schema), self.target / 'schema.parquet')

    def read(self):
        return calendar.ds.dataset(self.target, format='parquet', partitioning=calendar.REPORT_PARTITIONING).to_table(
            columns=calendar.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names,
        )

    def stored_bytes(self):
        return {str(p.relative_to(self.target)): p.read_bytes() for p in self.target.rglob('*.parquet')}

    def leaf(self, month, dataset='member_position'):
        return self.target / f'dataset_name={dataset}/exchange_code=XSGE/year=2024/month={month}'

    def commit(self, expected=None, existing=None):
        expected = self.expected if expected is None else expected
        existing = self.empty if existing is None else existing
        keys = calendar.changed_partition_keys(expected, existing)
        return calendar.commit_partitions(expected, keys, self.root)

    def clear_log(self):
        self.output.seek(0)
        self.output.truncate()

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

    def fail_formal(self):
        original = calendar.open_exact_dataset
        def open_dataset(*args, **kwargs):
            if args[3] == '正式报告日历':
                raise ValueError('injected formal readback failure')
            return original(*args, **kwargs)
        return patch.object(calendar, 'open_exact_dataset', side_effect=open_dataset)

    def test_empty_lake_success_and_no_changes(self):
        self.assertEqual(self.commit(), 9)
        self.assertEqual(calendar.table_digest(self.read()), calendar.table_digest(self.table(self.expected)))
        self.assertFalse(list(self.silver.glob('.a02-b01-*')))
        text = self.output.getvalue()
        self.assertLess(text.index('phase=formal_readback; status=completed'), text.index('committed:'))
        self.assertIn('date_watermark=none', text)
        before = self.stored_bytes()
        self.clear_log()
        self.assertEqual(self.commit(existing=self.expected), 0)
        self.assertEqual(self.stored_bytes(), before)
        self.assertNotIn('committed:', self.output.getvalue())
        self.assertFalse(list(self.silver.glob('.a02-b01-*')))

    def test_replacement_and_deletion_preserve_clean_leaf_and_marker(self):
        self.seed()
        before = self.stored_bytes()
        updated = self.expected.loc[self.expected.month.ne(2)].copy()
        updated.loc[updated.month.eq(1), 'updated_at'] = NOW.replace(day=2)
        original = transaction_module.os.replace
        with patch.object(transaction_module.os, 'replace', wraps=original) as moves:
            self.assertEqual(self.commit(updated, self.expected), 3)
        self.assertEqual(calendar.table_digest(self.read()), calendar.table_digest(self.table(updated)))
        for name, content in before.items():
            if 'month=3/' in name or name == 'schema.parquet':
                self.assertEqual(self.stored_bytes()[name], content)
        self.assertFalse(self.leaf(2).exists())
        self.assertIn('action=delete', self.output.getvalue())
        self.assertFalse(list(self.silver.glob('.a02-b01-*')))
        self.assertEqual(sum(pathlib.Path(call.args[0]).is_relative_to(self.staging) for call in moves.call_args_list), 3)

    def test_full_empty_replaces_root_with_readable_marker(self):
        self.seed()
        self.assertEqual(self.commit(self.empty, self.expected), 0)
        self.assertEqual(self.read().num_rows, 0)
        self.assertEqual([p.name for p in self.target.rglob('*.parquet')], ['schema.parquet'])
        self.assertFalse(list(self.silver.glob('.a02-b01-*')))

    def test_staging_failure_leaves_original_untouched(self):
        self.seed()
        previous = self.stored_bytes()
        with patch.object(calendar.pq, 'write_table', side_effect=OSError('injected staging write failure')):
            with self.assertRaisesRegex(OSError, 'injected staging'):
                self.commit()
        self.assertEqual(self.stored_bytes(), previous)
        self.assertFalse(list(self.silver.glob('.a02-b01-*')))
        self.assert_no_success()

    def test_transaction_entry_failure_cleans_staging(self):
        self.seed()
        previous = self.stored_bytes()
        with patch.object(calendar.StagedPathTransaction, '__enter__', side_effect=OSError('injected entry failure')):
            with self.assertRaisesRegex(OSError, 'injected entry'):
                self.commit()
        self.assertEqual(self.stored_bytes(), previous)
        self.assertFalse(list(self.silver.glob('.a02-b01-*')))
        self.assert_no_success()

    def test_first_backup_failure_keeps_old_root_in_place(self):
        self.seed()
        previous = self.stored_bytes()
        original = transaction_module.os.replace
        failure = PermissionError('injected backup failure')
        def move(source, target):
            if pathlib.Path(source) == self.target:
                raise failure
            return original(source, target)
        with patch.object(transaction_module.os, 'replace', side_effect=move):
            with self.assertRaises(PermissionError) as caught:
                self.commit(self.empty, self.expected)
        self.assertIs(caught.exception, failure)
        self.assert_recovered(previous, quarantine=False)

    def test_full_root_install_failure_restores_old_root(self):
        self.seed()
        previous = self.stored_bytes()
        original = transaction_module.os.replace
        failure = OSError('injected root install failure')
        def move(source, target):
            if pathlib.Path(source) == self.staging:
                raise failure
            return original(source, target)
        with patch.object(transaction_module.os, 'replace', side_effect=move):
            with self.assertRaises(OSError) as caught:
                self.commit(self.empty, self.expected)
        self.assertIs(caught.exception, failure)
        self.assert_recovered(previous, quarantine=False)

    def test_later_leaf_failure_restores_entire_batch(self):
        self.seed()
        previous = self.stored_bytes()
        original = transaction_module.os.replace
        failure = OSError('injected later install failure')
        def move(source, target):
            if pathlib.Path(source).is_relative_to(self.staging) and pathlib.Path(target) == self.leaf(2):
                raise failure
            return original(source, target)
        with patch.object(transaction_module.os, 'replace', side_effect=move):
            with self.assertRaises(RuntimeError) as caught:
                self.commit()
        self.assertIs(caught.exception.__cause__, failure)
        self.assert_recovered(previous)

    def test_formal_failure_restores_deleted_replaced_and_new_leaves(self):
        # 原有 1、2 月；新批删除 1 月、替换 2 月、新增 3 月。
        previous_frame = self.expected.loc[self.expected.month.ne(3)].copy()
        self.seed(previous_frame)
        previous = self.stored_bytes()
        updated = self.expected.loc[self.expected.month.ne(1)].copy()
        updated.loc[updated.month.eq(2), 'updated_at'] = NOW.replace(day=2)
        with self.fail_formal():
            with self.assertRaises(RuntimeError) as caught:
                self.commit(updated, previous_frame)
        self.assertIsInstance(caught.exception.__cause__, ValueError)
        self.assert_recovered(previous)
        self.assertFalse(self.leaf(3).exists())

    def test_full_empty_formal_failure_restores_old_root(self):
        self.seed()
        previous = self.stored_bytes()
        with self.fail_formal():
            with self.assertRaises(RuntimeError):
                self.commit(self.empty, self.expected)
        self.assert_recovered(previous)
        self.assertTrue((self.quarantine/calendar.TABLE_NAME/'schema.parquet').exists())

    def test_empty_lake_failure_restores_target_absence(self):
        with self.fail_formal():
            with self.assertRaises(RuntimeError):
                self.commit()
        self.assert_recovered({})
        self.assertFalse(self.target.exists())

    def test_one_restore_failure_retains_backup_and_restores_other_leaves(self):
        self.seed()
        previous = self.stored_bytes()
        original = transaction_module.os.replace
        failed_leaf = self.leaf(2)
        attempted = []
        def move(source, target):
            if pathlib.Path(source).is_relative_to(self.backup):
                attempted.append(pathlib.Path(target))
                if pathlib.Path(target) == failed_leaf:
                    raise PermissionError('injected restore failure')
            return original(source, target)
        with self.fail_formal(), patch.object(transaction_module.os, 'replace', side_effect=move):
            with self.assertRaisesRegex(RuntimeError, '回滚不完整') as caught:
                self.commit()
        self.assertIsInstance(caught.exception.__cause__, ValueError)
        self.assertEqual(len(attempted), 9)
        self.assertIn(self.leaf(1), attempted[attempted.index(failed_leaf)+1:])
        for name, content in previous.items():
            if name.startswith(str(failed_leaf.relative_to(self.target))):
                self.assertEqual((self.backup/calendar.TABLE_NAME/name).read_bytes(), content)
            else:
                self.assertEqual((self.target/name).read_bytes(), content)
        self.assertFalse(self.staging.exists())
        self.assertTrue(self.quarantine.exists())
        self.assert_no_success()

    def test_formal_content_mismatch_rolls_back_even_with_same_row_count(self):
        self.seed()
        previous = self.stored_bytes()
        original = calendar.open_exact_dataset
        class AlteredDataset:
            def __init__(self, dataset):
                self.dataset = dataset
            def to_table(self, *args, **kwargs):
                table = self.dataset.to_table(*args, **kwargs)
                i = table.schema.get_field_index('requirement_reason')
                values = table.column(i).to_pylist()
                values[0] = 'injected different content'
                return table.set_column(i, table.schema.field(i), calendar.pa.array(values, type=table.schema.field(i).type))
        def altered(*args, **kwargs):
            dataset = original(*args, **kwargs)
            return AlteredDataset(dataset) if args[3] == '正式报告日历' else dataset
        with patch.object(calendar, 'open_exact_dataset', side_effect=altered):
            with self.assertRaises(RuntimeError) as caught:
                self.commit()
        self.assertIn('内容与完整期望表不一致', str(caught.exception.__cause__))
        self.assert_recovered(previous)

    def test_formal_explicit_write_rejected_before_any_read(self):
        for extra in ([], ['--lake-root', str(self.root)]):
            with patch.object(calendar, 'settings', SimpleNamespace(futures_lake_root=self.root)), patch.object(calendar, 'open_exact_dataset') as read:
                result = CliRunner().invoke(calendar.main, [*extra, '--start-date', '2024-01-03', '--end-date', '2024-01-03', '--write'])
            self.assertNotEqual(result.exit_code, 0)
            self.assertIn('禁止写入', result.output)
            read.assert_not_called()

    def test_entry_distinguishes_notebook_import_and_script(self):
        notebook = nbformat.read(SOURCE.with_suffix('.ipynb'), as_version=4)
        entry = next(cell.source for cell in notebook.cells if cell.id == '03fe6466')
        for kernel, has_file, name, expected in (
            (True, False, '__main__', 'notebook'),
            (True, True, 'imported', 'none'),
            (False, True, 'imported', 'none'),
            (False, True, '__main__', 'script'),
            (True, True, '__main__', 'script'),
        ):
            with self.subTest(kernel=kernel, has_file=has_file, name=name):
                command = Mock()
                namespace = {'sys': SimpleNamespace(modules={'ipykernel': object()} if kernel else {}), 'main': command, '__name__': name}
                if has_file:
                    namespace['__file__'] = str(SOURCE)
                exec(entry, namespace)
                if expected == 'notebook':
                    command.main.assert_called_once_with(args=[], prog_name='c01_exchange_report_calendar', standalone_mode=False)
                    command.assert_not_called()
                elif expected == 'script':
                    command.assert_called_once_with()
                    command.main.assert_not_called()
                else:
                    command.assert_not_called()
                    command.main.assert_not_called()


if __name__ == '__main__':
    unittest.main()
