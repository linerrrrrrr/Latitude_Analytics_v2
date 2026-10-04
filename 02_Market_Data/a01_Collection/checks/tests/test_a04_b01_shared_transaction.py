"""宏观日历共享事务接入的真实 Parquet/路径故障检查；仅用临时目录。"""
import contextlib
import hashlib
import io
import pathlib
import sys
import tempfile
import types
import unittest
from datetime import date, datetime, timezone
from unittest.mock import patch

import nbformat

import test_b04_c01_macro_release_calendar as fixtures


def file_hashes(root):
    return {p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in root.rglob('*') if p.is_file()}


class MacroCalendarTransactionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = fixtures.load_notebook_module()
        cls.transaction_module = sys.modules[cls.module.StagedPathTransaction.__module__]
        with contextlib.redirect_stdout(io.StringIO()):
            cls.frame = cls.module.build_expected_calendar(
                date(2026, 7, 17), date(2026, 8, 31),
                cls.module.empty_pandas(cls.module.MACRO_RELEASE_CALENDAR_SCHEMA),
                date(2026, 9, 17), datetime(2026, 9, 17, tzinfo=timezone.utc),
            )

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='a04-txn-')
        self.addCleanup(temporary.cleanup)
        self.lake = pathlib.Path(temporary.name)
        self.target = self.lake / 'silver' / self.module.TABLE_NAME
        self.output = io.StringIO()
        capture = contextlib.redirect_stdout(self.output)
        capture.__enter__()
        self.addCleanup(capture.__exit__, None, None, None)
        fixtures.write_partitioned_calendar(self.module, self.frame, self.target)
        self.original = file_hashes(self.target)
        self.changed = self.frame.copy()
        self.changed.loc[self.changed['dataset_name'].eq('interest_rate'), 'quality_reason'] = '新提交内容'
        self.keys = self.module.changed_partition_keys(self.changed, self.frame)
        self.assertEqual(len(self.keys), 2)
        self.real_replace = self.transaction_module.os.replace

    def commit(self, frame=None, keys=None, **kwargs):
        return self.module.commit_partitions(
            self.changed if frame is None else frame,
            self.keys if keys is None else keys, self.lake, **kwargs,
        )

    def recovery(self, kind):
        return list((self.lake / 'silver').glob(f'.{self.module.TABLE_NAME}.{kind}-*'))

    def assert_failed(self, *, restored=True, quarantined=False, backup=False):
        if restored:
            self.assertEqual(self.original, file_hashes(self.target))
        self.assertFalse(self.recovery('staging'))
        self.assertEqual(bool(self.recovery('backup')), backup)
        self.assertEqual(bool(self.recovery('failed')), quarantined)
        self.assertNotIn('calendar_state=committed', self.output.getvalue())

    def test_replace_delete_and_clean_leaf_together(self):
        expected = self.changed.loc[~(self.changed['dataset_name'].eq('macro_release') & self.changed['month'].eq(7))].copy()
        keys = self.module.changed_partition_keys(expected, self.frame)
        self.assertEqual(len(keys), 3)
        clean = self.target / 'dataset_name=macro_release/year=2026/month=8'
        clean_hashes = file_hashes(clean)
        rows = self.commit(expected, keys)
        self.assertEqual(rows, int(expected['dataset_name'].eq('interest_rate').sum()))
        self.assertEqual(clean_hashes, file_hashes(clean))
        self.assertFalse((self.target / 'dataset_name=macro_release/year=2026/month=7').exists())
        dataset = self.module.open_exact_dataset(self.target, '事务成功检查')
        self.assertEqual(self.module.table_digest(dataset.to_table(columns=self.module.MACRO_RELEASE_CALENDAR_SCHEMA.names)),
                         self.module.table_digest(self.module.pandas_to_arrow(expected, self.module.MACRO_RELEASE_CALENDAR_SCHEMA)))
        self.assertEqual(self.original['schema.parquet'], file_hashes(self.target)['schema.parquet'])
        self.assertFalse(fixtures.recovery_paths(self.lake / 'silver', self.module.TABLE_NAME))
        self.assertEqual(self.output.getvalue().count('calendar_state=committed'), 1)

    def test_later_leaf_install_failure_restores_all_dirty_leaves(self):
        installs = []

        def replacing(source, target):
            if '.staging-' in str(source):
                installs.append(source)
                if len(installs) == 2:
                    raise OSError('injected later leaf install')
            return self.real_replace(source, target)

        with patch.object(self.transaction_module.os, 'replace', side_effect=replacing):
            with self.assertRaisesRegex(RuntimeError, '旧目标已恢复') as caught:
                self.commit()
        self.assertIsInstance(caught.exception.__cause__, OSError)
        self.assertEqual(len(installs), 2)
        self.assert_failed(quarantined=True)
        self.assertIn('phase=rollback; status=completed', self.output.getvalue())

    def test_formal_value_mismatch_restores_replacements_and_deletion(self):
        expected = self.changed.loc[~(self.changed['dataset_name'].eq('macro_release') & self.changed['month'].eq(7))].copy()
        keys = self.module.changed_partition_keys(expected, self.frame)
        real_open = self.module.open_exact_dataset

        class WrongValues:
            def __init__(self, dataset):
                self.dataset = dataset

            def to_table(inner, **kwargs):
                table = inner.dataset.to_table(**kwargs)
                index = table.column_names.index('quality_reason')
                values = table.column(index).to_pylist()
                values[0] = '注入复读值不一致'
                return table.set_column(index, table.schema.field(index), self.module.pa.array(values))

        def opening(path, label):
            dataset = real_open(path, label)
            return WrongValues(dataset) if path == self.target else dataset

        with patch.object(self.module, 'open_exact_dataset', side_effect=opening):
            with self.assertRaises(RuntimeError) as caught:
                self.commit(expected, keys)
        self.assertIsInstance(caught.exception.__cause__, ValueError)
        self.assertIn('内容与期望不一致', str(caught.exception.__cause__))
        self.assert_failed(quarantined=True)

    def test_one_restore_failure_does_not_stop_other_restores(self):
        failed_target = self.target / 'dataset_name=interest_rate/year=2026/month=8'
        restored_targets = []

        def replacing(source, target):
            if '.backup-' in str(source):
                if pathlib.Path(target) == failed_target:
                    raise OSError('injected restore failure')
                restored_targets.append(pathlib.Path(target))
            return self.real_replace(source, target)

        real_open = self.module.open_exact_dataset

        def opening(path, label):
            if path == self.target:
                raise ValueError('injected formal rejection')
            return real_open(path, label)

        with patch.object(self.transaction_module.os, 'replace', side_effect=replacing), patch.object(self.module, 'open_exact_dataset', side_effect=opening):
            with self.assertRaisesRegex(RuntimeError, '回滚不完整'):
                self.commit()
        earlier = self.target / 'dataset_name=interest_rate/year=2026/month=7'
        self.assertIn(earlier, restored_targets)
        self.assertFalse(failed_target.exists())
        prefix = 'dataset_name=interest_rate/year=2026/month=7/'
        self.assertEqual({k[len(prefix):]: v for k, v in self.original.items() if k.startswith(prefix)}, file_hashes(earlier))
        self.assert_failed(restored=False, quarantined=True, backup=True)
        backup_leaf = self.recovery('backup')[0] / failed_target.relative_to(self.target)
        prefix = failed_target.relative_to(self.target).as_posix() + '/'
        self.assertEqual({k[len(prefix):]: v for k, v in self.original.items() if k.startswith(prefix)}, file_hashes(backup_leaf))

    def test_full_swap_formal_failure_restores_old_root(self):
        real_open = self.module.open_exact_dataset

        def opening(path, label):
            if path == self.target:
                raise ValueError('injected root rejection')
            return real_open(path, label)

        with patch.object(self.module, 'open_exact_dataset', side_effect=opening):
            with self.assertRaises(RuntimeError):
                self.commit(force_full_swap=True)
        self.assert_failed(quarantined=True)
        self.assertTrue((self.recovery('failed')[0] / '_root' / 'schema.parquet').exists())

    def test_first_creation_formal_failure_restores_absence(self):
        self.lake = self.lake / 'new_lake'
        self.target = self.lake / 'silver' / self.module.TABLE_NAME
        real_open = self.module.open_exact_dataset

        def opening(path, label):
            if path == self.target:
                raise ValueError('injected first creation rejection')
            return real_open(path, label)

        with patch.object(self.module, 'open_exact_dataset', side_effect=opening):
            with self.assertRaises(RuntimeError):
                self.commit(keys=self.module.changed_partition_keys(self.changed, self.module.empty_pandas(self.module.MACRO_RELEASE_CALENDAR_SCHEMA)))
        self.assertFalse(self.target.exists())
        self.assert_failed(restored=False, quarantined=True)

    def test_staging_write_and_read_failures_do_not_touch_formal(self):
        with patch.object(self.module.ds, 'write_dataset', side_effect=OSError('injected write')):
            with self.assertRaisesRegex(OSError, 'injected write'):
                self.commit()
        self.assert_failed()
        with patch.object(self.module, 'open_exact_dataset', side_effect=ValueError('injected staging read')):
            with self.assertRaisesRegex(ValueError, 'injected staging read'):
                self.commit()
        self.assert_failed()

    def test_transaction_enter_failure_cleans_staging_only(self):
        with patch.object(self.module.StagedPathTransaction, '__enter__', side_effect=OSError('injected enter')):
            with self.assertRaisesRegex(OSError, 'injected enter'):
                self.commit()
        self.assert_failed()

    def test_zero_expected_root_stays_readable(self):
        empty = self.module.empty_pandas(self.module.MACRO_RELEASE_CALENDAR_SCHEMA)
        keys = self.module.changed_partition_keys(empty, self.frame)
        self.assertEqual(self.commit(empty, keys), 0)
        dataset = self.module.open_exact_dataset(self.target, '零行正式表')
        self.assertEqual(dataset.to_table().num_rows, 0)
        self.assertEqual(list(file_hashes(self.target)), ['schema.parquet'])

    def test_no_changes_never_enters_transaction(self):
        with patch.object(self.module.StagedPathTransaction, '__enter__', side_effect=AssertionError('不应进入事务')):
            self.assertEqual(self.commit(self.frame, []), 0)
        self.assertEqual(self.original, file_hashes(self.target))
        self.assertNotIn('persisted=true', self.output.getvalue())

    def test_execution_cell_notebook_script_and_import_modes(self):
        notebook = nbformat.read(fixtures.NOTEBOOK_PATH, 4)
        entry = next(cell.source for cell in notebook.cells if cell.id == '4a5b544e')
        for kernel, is_file, name, expected in (
            (True, False, '__main__', 'notebook'),
            (False, True, '__main__', 'script'),
            (True, True, '__main__', 'script'),
            (True, True, 'imported', 'none'),
            (False, True, 'imported', 'none'),
        ):
            with self.subTest(kernel=kernel, is_file=is_file, name=name):
                calls = []

                class Command:
                    def main(inner, **kwargs):
                        calls.append(('notebook', kwargs))

                    def __call__(inner):
                        calls.append(('script', {}))

                namespace = {'sys': types.SimpleNamespace(modules={'ipykernel': object()} if kernel else {}), '__name__': name, 'main': Command()}
                if is_file:
                    namespace['__file__'] = 'calendar.py'
                exec(compile(entry, '<entry>', 'exec'), namespace)
                self.assertEqual([c[0] for c in calls], [] if expected == 'none' else [expected])
                if expected == 'notebook':
                    self.assertEqual(calls[0][1], {'args': [], 'prog_name': 'c01_macro_release_calendar', 'standalone_mode': False})


if __name__ == '__main__':
    unittest.main()
