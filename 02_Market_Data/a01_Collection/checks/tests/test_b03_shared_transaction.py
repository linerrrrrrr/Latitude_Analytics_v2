"""b03 接入共享事务的真实路径集成检查：模拟来源，所有写入均在临时目录。

安装与验收异常为人工注入，用来检验恢复边界，不假设现存文件已损坏。
"""

import contextlib
import hashlib
import importlib.util
import io
import pathlib
import sys
import tempfile
import unittest
from datetime import date
from unittest.mock import Mock, patch

import nbformat
import pandas as pd

project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()
for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        ROOT = candidate_root
        sys.path.insert(0, str(candidate_root))
        break
else:
    raise RuntimeError("未找到项目根目录")
spec = importlib.util.spec_from_file_location(
    'b03_fixtures', pathlib.Path(__file__).with_name('test_futures_contract_calendar_builder.py')
)
fixtures = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixtures)
b03 = fixtures.contract_calendar
transaction_module = sys.modules[b03.StagedPathTransaction.__module__]


class B03SharedTransactionTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.ContractCalendarBuilderTest()
        self.fixture.setUp()
        self.capture = io.StringIO()
        redirect = contextlib.redirect_stdout(self.capture)
        redirect.__enter__()
        self.addCleanup(redirect.__exit__, None, None, None)
        temporary = tempfile.TemporaryDirectory(prefix='b03-transaction-integration-')
        self.addCleanup(temporary.cleanup)
        self.root = pathlib.Path(temporary.name)
        self.table = self.root / 'silver' / b03.TABLE_NAME
        self.key = ('XSGE', 2024, 1)
        self.relative = pathlib.Path('exchange_code=XSGE/year=2024/month=1')
        self.leaf = self.table / self.relative
        self.frame, _ = self.fixture.build_frame_with_securities(
            [date(2024, 1, 2)], self.fixture.valid_securities_df
        )
        self.replacement = self.frame.copy()
        self.replacement['tick_size'] = 2.0
        self.empty = b03.empty_pandas(b03.FUTURES_CONTRACT_CALENDAR_SCHEMA)

    def hashes(self, path=None):
        path = self.table if path is None else path
        return {str(p.relative_to(path)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in path.rglob('*.parquet')}

    def seed(self):
        b03.commit_partition(self.frame, self.root, self.key)
        self.capture.seek(0)
        self.capture.truncate()
        return self.hashes()

    def assert_clean(self):
        self.assertFalse(list((self.root / 'silver').glob('.c03s-*')))
        self.assertFalse(list((self.root / 'silver').glob('.c03b-*')))

    def reject_formal(self, actual, expected, context):
        if context.startswith('正式分区'):
            raise TypeError('injected formal validation failure')
        return self.original_validate(actual, expected, context)

    def test_staging_failure_keeps_formal_leaf_without_install(self):
        before = self.seed()
        with patch.object(b03.ds, 'write_dataset', side_effect=OSError('staging write')), patch.object(
            transaction_module.os, 'replace', wraps=transaction_module.os.replace
        ) as replace:
            with self.assertRaisesRegex(OSError, 'staging write'):
                b03.commit_partition(self.replacement, self.root, self.key)
        replace.assert_not_called()
        self.assertEqual(self.hashes(), before)
        self.assert_clean()

    def test_first_backup_failure_keeps_original_and_does_not_quarantine_it(self):
        before = self.seed()
        with patch.object(transaction_module.os, 'replace', side_effect=PermissionError('backup locked')) as replace:
            with self.assertRaisesRegex(PermissionError, 'backup locked'):
                b03.commit_partition(self.replacement, self.root, self.key)
        replace.assert_called_once()
        self.assertEqual(self.hashes(), before)
        self.assertFalse(list((self.root / 'silver').glob('.c03q-*')))
        self.assert_clean()

    def test_install_failure_restores_old_leaf(self):
        before = self.seed()
        original_replace = transaction_module.os.replace

        def fail_install(source, target):
            if any(part.startswith('.c03s-') for part in source.parts):
                raise PermissionError('install locked')
            return original_replace(source, target)

        with patch.object(transaction_module.os, 'replace', side_effect=fail_install):
            with self.assertRaisesRegex(PermissionError, 'install locked'):
                b03.commit_partition(self.replacement, self.root, self.key)
        self.assertEqual(self.hashes(), before)
        self.assert_clean()

    def test_formal_rejection_restores_old_and_quarantines_new_leaf(self):
        before = self.seed()
        self.original_validate = b03.validate_compatible_dataset_schema
        with patch.object(b03, 'validate_compatible_dataset_schema', side_effect=self.reject_formal):
            with self.assertRaisesRegex(RuntimeError, '旧目标已恢复') as caught:
                b03.commit_partition(self.replacement, self.root, self.key)
        self.assertIsInstance(caught.exception.__cause__, TypeError)
        self.assertEqual(self.hashes(), before)
        quarantines = list((self.root / 'silver').glob('.c03q-*'))
        self.assertEqual(len(quarantines), 1)
        failed = b03.ds.dataset(quarantines[0] / self.relative, format='parquet').to_table()
        self.assertTrue(failed['tick_size'].to_pandas().eq(2.0).all())
        self.assertNotIn('partition_committed:', self.capture.getvalue())
        self.assert_clean()

    def test_new_leaf_rejection_removes_formal_leaf_and_preserves_quarantine(self):
        self.original_validate = b03.validate_compatible_dataset_schema
        with patch.object(b03, 'validate_compatible_dataset_schema', side_effect=self.reject_formal):
            with self.assertRaisesRegex(RuntimeError, '分区提交失败'):
                b03.commit_partition(self.frame, self.root, self.key)
        self.assertFalse(self.leaf.exists())
        self.assertEqual(len(list((self.root / 'silver').glob('.c03q-*'))), 1)
        self.assert_clean()

    def test_marker_rejection_restores_deleted_leaf_and_removes_new_marker(self):
        before = self.seed()
        original_validate = b03.validate_compatible_dataset_schema

        def reject_marker(actual, expected, context):
            if context.startswith('正式空表 marker'):
                raise TypeError('injected marker rejection')
            return original_validate(actual, expected, context)

        with patch.object(b03, 'validate_compatible_dataset_schema', side_effect=reject_marker):
            with self.assertRaisesRegex(TypeError, 'marker rejection'):
                b03.commit_partition(self.empty, self.root, self.key)
        self.assertEqual(self.hashes(), before)
        self.assertFalse((self.table / 'schema.parquet').exists())
        self.assertFalse(list((self.root / 'silver').glob('.c03q-*')))
        self.assert_clean()

    def test_marker_write_failure_restores_deleted_leaf(self):
        before = self.seed()
        with patch.object(b03.pq, 'write_table', side_effect=OSError('marker write')):
            with self.assertRaisesRegex(OSError, 'marker write'):
                b03.commit_partition(self.empty, self.root, self.key)
        self.assertEqual(self.hashes(), before)
        self.assertFalse((self.table / 'schema.parquet').exists())
        self.assert_clean()

    def test_existing_watermark_marker_is_preserved_during_leaf_deletion(self):
        self.seed()
        b03.write_automatic_tail_processed_through(self.root, date(2024, 1, 2))
        marker = self.table / 'schema.parquet'
        before = marker.read_bytes()
        b03.commit_partition(self.empty, self.root, self.key)
        self.assertFalse(self.leaf.exists())
        self.assertEqual(marker.read_bytes(), before)
        self.assert_clean()

    def test_rollback_failure_preserves_backup_evidence_and_no_success_event(self):
        self.seed()
        before = self.hashes(self.leaf)
        self.original_validate = b03.validate_compatible_dataset_schema
        original_replace = transaction_module.os.replace

        def fail_restore(source, target):
            if any(part.startswith('.c03b-') for part in source.parts):
                raise PermissionError('restore locked')
            return original_replace(source, target)

        with patch.object(b03, 'validate_compatible_dataset_schema', side_effect=self.reject_formal), patch.object(
            transaction_module.os, 'replace', side_effect=fail_restore
        ):
            with self.assertRaisesRegex(RuntimeError, '回滚不完整'):
                b03.commit_partition(self.replacement, self.root, self.key)
        backups = list((self.root / 'silver').glob('.c03b-*'))
        self.assertEqual(len(backups), 1)
        self.assertEqual(self.hashes(backups[0] / self.relative), before)
        self.assertFalse(list((self.root / 'silver').glob('.c03s-*')))
        self.assertNotIn('partition_committed:', self.capture.getvalue())
        self.assertIn('phase=rollback; status=failed', self.capture.getvalue())

    def test_second_leaf_failure_keeps_first_success_and_stops_watermark(self):
        catalogue = self.fixture.valid_securities_df.copy()
        second = catalogue.copy()
        second['contract_code'] = 'M2405.XDCE'
        second['underlying_code'] = 'M'
        second['exchange_code'] = 'XDCE'
        catalogue = pd.concat([catalogue, second], ignore_index=True)
        variety = self.fixture.variety_frame([date(2024, 1, 2), date(2024, 1, 3)])
        other_variety = variety.copy()
        other_variety['underlying_code'] = 'M'
        other_variety['exchange_code'] = 'XDCE'
        variety = pd.concat([variety, other_variety], ignore_index=True)
        b03.ds.write_dataset(
            b03.pandas_to_arrow(variety, b03.FUTURES_VARIETY_CALENDAR_SCHEMA),
            self.root / 'silver' / b03.UPSTREAM_TABLE_NAME,
            format='parquet', partitioning=b03.UPSTREAM_PARTITIONING,
        )
        # 从已完成 1 月 2 日开始；本次真实进入两叶自动尾部提交。
        for code in ('XDCE', 'XSGE'):
            old = self.frame.copy()
            if code == 'XDCE':
                old['exchange_code'] = code
                old['underlying_code'] = 'M'
                old['contract_code'] = 'M2405.XDCE'
            b03.commit_partition(old, self.root, (code, 2024, 1))
        before_second = self.hashes(self.leaf)
        b03.write_automatic_tail_processed_through(self.root, date(2024, 1, 2))
        marker = self.table / 'schema.parquet'
        before_marker = marker.read_bytes()
        original_validate = b03.validate_compatible_dataset_schema

        def reject_second(actual, expected, context):
            if context.startswith('正式分区 exchange_code=XSGE'):
                raise TypeError('second leaf rejection')
            return original_validate(actual, expected, context)

        with patch.object(b03, 'validate_compatible_dataset_schema', side_effect=reject_second), patch.object(
            b03, 'write_automatic_tail_processed_through', wraps=b03.write_automatic_tail_processed_through
        ) as watermark:
            result, _ = self.fixture.invoke_with_jqdata(self.root, ['--write'], fixtures.FakeJQData(catalogue))
        self.assertNotEqual(result.exit_code, 0, result.output)
        watermark.assert_not_called()
        self.assertEqual(self.hashes(self.leaf), before_second)
        first = b03.ds.dataset(self.table / 'exchange_code=XDCE/year=2024/month=1', format='parquet').to_table()
        self.assertEqual(set(first['trading_date'].to_pylist()), {date(2024, 1, 2), date(2024, 1, 3)})
        self.assertEqual(marker.read_bytes(), before_marker)
        self.assertEqual(result.output.count('partition_committed:'), 1)
        self.assertNotIn('phase=run; status=completed', result.output)
        self.assert_clean()


class B03EntryCellTest(unittest.TestCase):
    def test_notebook_uses_explicit_readonly_args_script_uses_cli_and_import_is_idle(self):
        notebook = nbformat.read(ROOT / '02_Market_Data/a01_Collection/b01_Futures_Market_Data/c03_futures_contract_calendar.ipynb', as_version=4)
        source = next(cell.source for cell in notebook.cells if cell.id == 'b03-entry')
        for kernel, has_file, name, expected in (
            (True, False, '__main__', 'notebook'),
            (False, True, '__main__', 'script'),
            (True, True, '__main__', 'script'),
            (False, True, 'imported_b03', 'idle'),
            (True, True, 'imported_b03', 'idle'),
        ):
            command = Mock()
            namespace = {'__name__': name, 'main': command, 'sys': sys}
            if has_file:
                namespace['__file__'] = 'c03_futures_contract_calendar.py'
            with patch.dict(sys.modules):
                if kernel:
                    sys.modules['ipykernel'] = Mock()
                else:
                    sys.modules.pop('ipykernel', None)
                with patch.object(sys, 'argv', ['ipykernel_launcher.py', '-f', 'connection.json']):
                    exec(compile(source, '<b03-entry>', 'exec'), namespace)
            if expected == 'notebook':
                command.assert_not_called()
                command.main.assert_called_once_with(
                    args=['--start-date', '2026-08-01', '--end-date', '2026-08-15'],
                    prog_name='c03_futures_contract_calendar', standalone_mode=False,
                )
            elif expected == 'script':
                command.assert_called_once_with()
                command.main.assert_not_called()
            else:
                command.assert_not_called()
                command.main.assert_not_called()


if __name__ == '__main__':
    unittest.main()
