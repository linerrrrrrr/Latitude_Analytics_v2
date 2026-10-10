"""宏观发布 共享事务接入检查：临时 Parquet、受控文件系统故障，无真实 API。"""
import contextlib
import hashlib
import io
import pathlib
import shutil
import sys
import tempfile
import types
import unittest
from datetime import date
from unittest.mock import patch

import nbformat
import pandas as pd
from click.testing import CliRunner

import test_b04_c03_macro_release as fixtures

M = fixtures.C03
TRANSACTION = sys.modules[M.StagedPathTransaction.__module__]


def hashes(root):
    return {p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in root.rglob('*') if p.is_file()}


class MacroReleaseSharedTransactionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='macro-txn-')
        self.addCleanup(temporary.cleanup)
        self.lake = pathlib.Path(temporary.name)
        self.output = io.StringIO()
        capture = contextlib.redirect_stdout(self.output)
        capture.__enter__()
        self.addCleanup(capture.__exit__, None, None, None)
        self.first, self.second = date(2026, 7, 31), date(2026, 8, 31)
        built = CliRunner().invoke(fixtures.C01.main, [
            '--lake-root', str(self.lake), '--start-date', self.first.isoformat(),
            '--end-date', self.second.isoformat(), '--write',
        ])
        self.assertEqual(built.exit_code, 0, built.output)
        self.fact_path = self.lake / 'silver' / M.TABLE_NAME
        self.calendar_path = self.lake / 'silver' / M.CALENDAR_TABLE_NAME
        self.calendar_df = M.read_macro_calendar(self.calendar_path)
        frames = []
        self.counts = {}
        for report_name in M.SERIES_BY_REPORT:
            pending = self.calendar_df.loc[
                self.calendar_df['report_date'].eq(self.first)
                & self.calendar_df['series_code'].isin([s.series_code for s in M.SERIES_BY_REPORT[report_name]])
            ]
            if pending.empty:
                continue
            frame, counts = M.normalize_macro_release_response(
                [fixtures.source_row(report_name, self.first.replace(day=1))], report_name,
                pending, self.first.replace(day=1), self.first, M.datetime.now(M.timezone.utc),
            )
            frames.append(frame)
            self.counts.update(counts)
        self.fact_df = pd.concat(frames, ignore_index=True)
        M.commit_complete_fact_partition(self.fact_df, self.lake, (2026, 7))
        self.before = hashes(self.lake)
        self.real_replace = TRANSACTION.os.replace
        self.real_open = M.open_exact_dataset
        self.output.seek(0)
        self.output.truncate()

    def invoke_commit(self, kind):
        if kind == 'upgrade':
            return M.upgrade_fact_metadata(self.fact_df, self.lake)
        if kind == 'fact':
            changed = self.fact_df.copy()
            changed.loc[:, 'value'] = 99.0
            return M.commit_complete_fact_partition(changed, self.lake, (2026, 7))
        changed = self.calendar_df.copy()
        changed.loc[:, 'quality_reason'] = '本次状态回写'
        return M.commit_calendar_partition(changed, self.lake, (M.DATASET_NAME, 2026, 7))

    def recovery(self, kind, table=M.TABLE_NAME):
        return list((self.lake / 'silver').glob(f'.{table}.{kind}-*'))

    def assert_old_formal_files(self):
        current = hashes(self.lake)
        # 失败证据不是正式表内容；所有之前存在的文件均逐字恢复。
        self.assertEqual(self.before, {k: current[k] for k in self.before})

    def clear_recovery(self):
        for p in (self.lake / 'silver').glob('.*'):
            self.assertTrue(p.resolve().is_relative_to(self.lake.resolve()))
            if p.is_dir():
                shutil.rmtree(p)
        self.output.seek(0)
        self.output.truncate()

    def test_formal_value_mismatch_restores_three_transaction_boundaries(self):
        for kind in ('fact', 'calendar', 'upgrade'):
            with self.subTest(kind=kind):
                target = self.calendar_path if kind == 'calendar' else self.fact_path

                class WrongValues:
                    def __init__(self, dataset):
                        self.dataset = dataset

                    def to_table(inner, **kwargs):
                        table = inner.dataset.to_table(**kwargs)
                        name = 'quality_reason' if kind == 'calendar' else 'value'
                        index = table.column_names.index(name)
                        values = table.column(index).to_pylist()
                        values[0] = '注入不同内容' if kind == 'calendar' else 98.0
                        return table.set_column(index, table.schema.field(index), M.pa.array(values))

                def opening(path, *args, **kwargs):
                    dataset = self.real_open(path, *args, **kwargs)
                    # calendar 的提交前契约检查不物化，只有正式复读使用代理表。
                    return WrongValues(dataset) if path.is_relative_to(target) else dataset

                with patch.object(M, 'open_exact_dataset', side_effect=opening):
                    with self.assertRaisesRegex(RuntimeError, '旧目标已恢复') as caught:
                        self.invoke_commit(kind)
                self.assertIsInstance(caught.exception.__cause__, ValueError)
                self.assert_old_formal_files()
                table = M.CALENDAR_TABLE_NAME if kind == 'calendar' else M.TABLE_NAME
                self.assertFalse(self.recovery('staging', table))
                self.assertFalse(self.recovery('backup', table))
                self.assertTrue(hashes(self.recovery('failed', table)[0]))
                if kind == 'upgrade':
                    self.assertTrue((self.recovery('failed')[0] / '_root').is_dir())
                self.assertNotIn('committed:', self.output.getvalue())
                self.clear_recovery()

    def test_staging_rejection_keeps_all_formal_files_and_cleans_staging(self):
        for kind in ('fact', 'calendar', 'upgrade'):
            with self.subTest(kind=kind):
                def opening(path, *args, **kwargs):
                    if '.staging-' in str(path):
                        raise ValueError('injected staging rejection')
                    return self.real_open(path, *args, **kwargs)

                with patch.object(M, 'open_exact_dataset', side_effect=opening), patch.object(
                    M.StagedPathTransaction, '__enter__', side_effect=AssertionError('must not install'),
                ) as entering:
                    with self.assertRaisesRegex(ValueError, 'staging rejection'):
                        self.invoke_commit(kind)
                entering.assert_not_called()
                self.assertEqual(self.before, hashes(self.lake))
                self.assertFalse(list((self.lake / 'silver').glob('.*')))
                self.assertNotIn('committed:', self.output.getvalue())

    def test_transaction_entry_collision_preserves_existing_evidence(self):
        for kind in ('fact', 'calendar', 'upgrade'):
            with self.subTest(kind=kind):
                table = M.CALENDAR_TABLE_NAME if kind == 'calendar' else M.TABLE_NAME
                collision = self.lake / 'silver' / f'.{table}.backup-fixed'
                collision.mkdir()
                (collision / 'evidence').write_bytes(b'existing evidence')
                with patch.object(M.uuid, 'uuid4', return_value=types.SimpleNamespace(hex='fixed')):
                    with self.assertRaises(FileExistsError):
                        self.invoke_commit(kind)
                self.assert_old_formal_files()
                self.assertEqual((collision / 'evidence').read_bytes(), b'existing evidence')
                self.assertFalse(self.recovery('staging', table))
                self.assertNotIn('committed:', self.output.getvalue())
                self.clear_recovery()

    def test_upgrade_first_backup_and_install_failure_restore_exact_old_root(self):
        for phase in ('backup', 'install'):
            with self.subTest(phase=phase):
                def replacing(source, target):
                    if (phase == 'backup' and pathlib.Path(source) == self.fact_path
                            or phase == 'install' and '.staging-' in str(source)):
                        raise OSError('injected root move')
                    return self.real_replace(source, target)

                with patch.object(TRANSACTION.os, 'replace', side_effect=replacing):
                    with self.assertRaisesRegex(OSError, 'root move'):
                        self.invoke_commit('upgrade')
                self.assertEqual(self.before, hashes(self.lake))
                self.assertFalse(self.recovery('staging'))
                self.assertFalse(self.recovery('backup'))
                self.assertNotIn('committed:', self.output.getvalue())

    def test_new_marker_is_removed_without_deleting_unrelated_files(self):
        shutil.rmtree(self.fact_path)
        self.fact_path.mkdir()
        sentinel = self.fact_path / 'operator-note.txt'
        sentinel.write_bytes(b'keep')

        def opening(path, *args, **kwargs):
            if path.is_relative_to(self.fact_path):
                raise ValueError('injected formal rejection')
            return self.real_open(path, *args, **kwargs)

        with patch.object(M, 'open_exact_dataset', side_effect=opening):
            with self.assertRaisesRegex(RuntimeError, '旧目标已恢复'):
                self.invoke_commit('fact')
        self.assertEqual(sentinel.read_bytes(), b'keep')
        self.assertFalse(list(self.fact_path.rglob('*.parquet')))
        failed = self.recovery('failed')[0]
        self.assertFalse((failed / 'schema.parquet').exists())
        self.assertTrue(list(failed.rglob('*.parquet')))
        self.assertFalse(self.recovery('backup'))
        self.assertFalse(self.recovery('staging'))

    def test_incomplete_leaf_recovery_still_removes_new_marker(self):
        # 正式叶存在但根标记缺失：本次新建标记，恢复失败不能阻止撤销标记。
        (self.fact_path / 'schema.parquet').unlink()
        old_leaf = hashes(self.fact_path / 'year=2026/month=7')

        def opening(path, *args, **kwargs):
            if path.is_relative_to(self.fact_path):
                raise ValueError('injected formal rejection')
            return self.real_open(path, *args, **kwargs)

        def replacing(source, target):
            if '.backup-' in str(source):
                raise OSError('injected restore failure')
            return self.real_replace(source, target)

        with patch.object(M, 'open_exact_dataset', side_effect=opening), patch.object(
            TRANSACTION.os, 'replace', side_effect=replacing,
        ):
            with self.assertRaisesRegex(RuntimeError, '回滚不完整') as caught:
                self.invoke_commit('fact')
        self.assertIsInstance(caught.exception.__cause__, ValueError)
        self.assertEqual(old_leaf, hashes(self.recovery('backup')[0] / 'year=2026/month=7'))
        self.assertTrue(self.recovery('failed'))
        self.assertFalse((self.fact_path / 'schema.parquet').exists())
        self.assertFalse(self.recovery('staging'))
        self.assertNotIn('committed:', self.output.getvalue())

    def test_empty_fact_deletion_is_restored_on_formal_rejection(self):
        def opening(path, *args, **kwargs):
            if path.is_relative_to(self.fact_path):
                raise ValueError('injected deletion rejection')
            return self.real_open(path, *args, **kwargs)

        with patch.object(M, 'open_exact_dataset', side_effect=opening):
            with self.assertRaisesRegex(ValueError, 'deletion rejection'):
                M.commit_complete_fact_partition(M.empty_pandas(M.MACRO_RELEASE_SCHEMA), self.lake, (2026, 7))
        self.assertEqual(self.before, hashes(self.lake))
        self.assertFalse(self.recovery('failed'))
        self.assertFalse(self.recovery('staging'))

    def test_later_calendar_failure_keeps_facts_then_repairs_without_api(self):
        # July 无 API 修复；August 同月三个报告的最后一个日历安装失败。
        client = fixtures.FakeEastmoneySession(fixtures.all_report_rows(self.second.replace(day=1)))
        def replacing(source, target):
            if f'.{M.CALENDAR_TABLE_NAME}.staging-' in str(source) and str(target).endswith('month=8'):
                table = M.ds.dataset(source, format='parquet').to_table()
                if all(table['is_fetch_completed'].to_pylist()):
                    raise OSError('injected last August calendar install')
            return self.real_replace(source, target)
        with patch.object(TRANSACTION.os, 'replace', side_effect=replacing):
            failed = fixtures.run_c03(self.lake, None, client)
        self.assertIsInstance(failed.exception, OSError, failed.output)
        self.assertEqual(len(client.calls), 3)
        self.assertTrue(client.closed)
        calendar = M.read_macro_calendar(self.calendar_path)
        self.assertTrue(calendar.loc[calendar['month'].eq(7), 'is_fetch_completed'].all())
        self.assertEqual(int(calendar.loc[calendar['month'].eq(8), 'is_fetch_completed'].sum()), 11)
        facts, _ = M.read_optional_fact(self.fact_path)
        self.assertEqual(len(facts), 26)
        fact_files = hashes(self.fact_path)
        repaired = fixtures.run_c03(self.lake, None, None)
        self.assertEqual(repaired.exit_code, 0, repaired.output)
        self.assertEqual(fact_files, hashes(self.fact_path))
        self.assertTrue(M.read_macro_calendar(self.calendar_path)['is_fetch_completed'].all())

    def test_boolean_null_and_infinite_dirty_values_rejected_before_staging(self):
        for value in (True, None, float('inf')):
            with self.subTest(value=value):
                changed = self.fact_df.copy()
                changed['value'] = changed['value'].astype(object)
                changed.loc[changed.index[0], 'value'] = value
                with patch.object(M, 'write_fact_staging', side_effect=AssertionError('must not stage')) as writing:
                    with self.assertRaises(ValueError):
                        M.commit_complete_fact_partition(changed, self.lake, (2026, 7))
                writing.assert_not_called()
                self.assertEqual(self.before, hashes(self.lake))

    def test_description_metadata_does_not_rewrite_completed_facts(self):
        session = fixtures.FakeEastmoneySession(fixtures.all_report_rows(self.second.replace(day=1)))
        completed = fixtures.run_c03(self.lake, None, session)
        self.assertEqual(completed.exit_code, 0, completed.output)
        for path in self.fact_path.rglob('*.parquet'):
            table = M.pq.ParquetFile(path).read()
            metadata = dict(table.schema.metadata)
            metadata[b'description'] = b'older descriptive text'
            M.pq.write_table(table.replace_schema_metadata(metadata), path)
        before = hashes(self.lake)
        with patch.object(M, 'upgrade_fact_metadata', side_effect=AssertionError('no description migration')):
            result = fixtures.run_c03(self.lake, None, None)
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(before, hashes(self.lake))

    def test_missing_upstream_leaf_is_not_created(self):
        leaf = self.calendar_path / 'dataset_name=macro_release/year=2026/month=7'
        shutil.rmtree(leaf)
        remaining = hashes(self.lake)
        with self.assertRaisesRegex(FileNotFoundError, '缺少待回写完整叶'):
            self.invoke_commit('calendar')
        self.assertEqual(remaining, hashes(self.lake))
        self.assertFalse(leaf.exists())
        self.assertFalse(self.recovery('staging', M.CALENDAR_TABLE_NAME))


class MacroReleaseEntryTests(unittest.TestCase):
    def test_notebook_script_and_import_entry_branches(self):
        notebook = nbformat.read(fixtures.C03_NOTEBOOK_PATH, 4)
        source = next(c.source for c in notebook.cells if c.id == '14766fcd')
        for kernel, has_file, name, expected in (
            (True, False, '__main__', 'notebook'),
            (True, True, '__main__', 'script'),
            (False, True, '__main__', 'script'),
            (True, True, 'imported', None),
            (False, True, 'imported', None),
        ):
            with self.subTest(kernel=kernel, has_file=has_file, name=name):
                calls = []

                class Command:
                    def __call__(self):
                        calls.append(('script', {}))

                    def main(self, **kwargs):
                        calls.append(('notebook', kwargs))

                namespace = {'__name__': name, 'sys': types.SimpleNamespace(
                    modules={'ipykernel': object()} if kernel else {},
                    argv=['ipykernel_launcher.py', '-f', 'connection.json']), 'main': Command()}
                if has_file:
                    namespace['__file__'] = str(fixtures.C03_PATH)
                exec(compile(source, '<entry cell>', 'exec'), namespace)
                self.assertEqual([item[0] for item in calls], [] if expected is None else [expected])
                if expected == 'notebook':
                    self.assertEqual(calls[0][1], {'args': [], 'prog_name': 'c03_macro_release', 'standalone_mode': False})


if __name__ == '__main__':
    unittest.main()
