"""b05 共享事务集成；临时 Parquet、显式 I/O 故障注入，不访问正式湖或 API。"""

import contextlib
import hashlib
import importlib.util
import io
import pathlib
import sys
import tempfile
import unittest
from datetime import date
from types import SimpleNamespace
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
NOTEBOOK = ROOT / 'R02_Market_Data/a01_Collection/b01_Futures_Market_Data/c05_futures_daily.ipynb'
spec = importlib.util.spec_from_file_location(
    'b05_transaction_fixtures', pathlib.Path(__file__).with_name('test_c05_daily_incremental_planning.py')
)
fixtures = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixtures)
b05 = fixtures.c05
transaction_module = sys.modules[b05.StagedPathTransaction.__module__]


class B05SharedTransactionTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='b05-transaction-')
        self.addCleanup(temporary.cleanup)
        self.root = pathlib.Path(temporary.name)
        self.silver = self.root / 'silver'
        self.output = io.StringIO()
        redirect = contextlib.redirect_stdout(self.output)
        redirect.__enter__()
        self.addCleanup(redirect.__exit__, None, None, None)

    def specs(self, high=101.0, month=8):
        facts, calendars, specs = [], [], []
        for variety in ('BB', 'A'):
            fact = fixtures.fact_frame(high=high)
            fact['underlying_code'] = variety
            fact['contract_code'] = f'{variety}2601.XDCE'
            fact['trading_date'] = date(2026, month, 21)
            fact['month'] = month
            calendar = fixtures.full_calendar_frame(
                exchange_code='XDCE', underlying_code=variety, is_fetch_required=True
            )
            calendar['trading_date'] = date(2026, month, 21)
            calendar['month'] = month
            pair = fixtures.leaf_specs(fact, calendar)
            pair[0]['partition_key'] = ('XDCE', variety, 2026, month)
            specs.append(pair[0])
            facts.append(pair[0]['frame'])
            calendars.append(pair[1]['frame'])
        calendar_spec = pair[1]
        calendar_spec['partition_key'] = ('1d', 'XDCE', 2026, month)
        calendar_spec['frame'] = b05.validate_calendar_state_frame(
            b05.apply_completion_to_calendar_leaf(
                pd.concat(calendars, ignore_index=True), pd.concat(facts, ignore_index=True),
                f'batch-{month}-{high}', fixtures.CHECKED_AT,
            ), '测试协调日历 ',
        )
        return [*specs, calendar_spec]

    def hashes(self):
        return {
            str(path.relative_to(self.silver)): hashlib.sha256(path.read_bytes()).hexdigest()
            for table in (b05.TABLE_NAME, b05.CALENDAR_TABLE_NAME)
            for path in (self.silver / table).rglob('*.parquet')
        }

    def seed(self):
        b05.commit_validated_leaf_group(self.specs(), self.root)
        self.output.seek(0)
        self.output.truncate()
        return self.hashes()

    def assert_recovered(self, expected):
        self.assertEqual(self.hashes(), expected)
        self.assertFalse(list(self.silver.glob('.c05s-*')))
        self.assertFalse(list(self.silver.glob('.c05b-*')))
        self.assertIn('phase=rollback; status=completed', self.output.getvalue())
        self.assertNotIn('persisted=true', self.output.getvalue())
        self.assertNotIn('partition_committed:', self.output.getvalue())

    def test_success_installs_two_fact_leaves_and_one_calendar_then_reports_completion(self):
        self.assertEqual(b05.commit_validated_leaf_group(self.specs(), self.root), 4)
        markers = {table: (self.silver / table / 'schema.parquet').read_bytes()
                   for table in (b05.TABLE_NAME, b05.CALENDAR_TABLE_NAME)}
        original = transaction_module.os.replace
        with patch.object(transaction_module.os, 'replace', wraps=original) as moves:
            self.assertEqual(b05.commit_validated_leaf_group(self.specs(high=102.0), self.root), 4)
        for table, marker in markers.items():
            marker_path = self.silver / table / 'schema.parquet'
            self.assertEqual(marker_path.read_bytes(), marker)
            self.assertEqual(b05.pq.read_metadata(marker_path).num_rows, 0)
            self.assertTrue(all(marker_path.resolve() not in call.args for call in moves.call_args_list))
        for leaf_spec in self.specs(high=102.0):
            actual = b05.read_partition_leaf(
                self.silver / leaf_spec['table_name'], leaf_spec['schema'], leaf_spec['partition_columns'],
                leaf_spec['partitioning'], leaf_spec['partition_key'],
            )
            pd.testing.assert_frame_equal(actual, leaf_spec['frame'])
        output = self.output.getvalue()
        self.assertLess(output.rindex('phase=formal_readback; status=running'), output.rindex('persisted=true'))
        self.assertFalse(list(self.silver.glob('.c05[bsq]-*')))

    def test_first_backup_failure_keeps_every_old_file_untouched(self):
        before = self.seed()
        original = transaction_module.os.replace

        def fail(source, target):
            if any(part.startswith('.c05b-') for part in target.parts):
                raise PermissionError('injected first backup failure')
            return original(source, target)

        with patch.object(transaction_module.os, 'replace', side_effect=fail):
            with self.assertRaisesRegex(PermissionError, 'first backup failure'):
                b05.commit_validated_leaf_group(self.specs(high=102.0), self.root)
        self.assert_recovered(before)
        self.assertFalse(list(self.silver.glob('.c05q-*')))

    def test_second_fact_install_failure_restores_entire_group(self):
        before = self.seed()
        original = transaction_module.os.replace

        def fail(source, target):
            if '.c05s-' in str(source) and 'underlying_code=A' in target.parts:
                raise OSError('injected second fact install failure')
            return original(source, target)

        with patch.object(transaction_module.os, 'replace', side_effect=fail):
            with self.assertRaisesRegex(RuntimeError, '旧目标已恢复') as error:
                b05.commit_validated_leaf_group(self.specs(high=102.0), self.root)
        self.assertIsInstance(error.exception.__cause__, OSError)
        self.assert_recovered(before)
        self.assertEqual(len(list(self.silver.glob('.c05q-*/**/*.parquet'))), 1)

    def test_new_marker_write_install_and_readback_failures_restore_all_new_paths(self):
        for phase in ('write', 'install', 'readback'):
            with self.subTest(phase=phase), tempfile.TemporaryDirectory(prefix='b05-marker-') as temporary:
                self.root = pathlib.Path(temporary)
                self.silver = self.root / 'silver'
                self.output.seek(0)
                self.output.truncate()
                module, method = {'write': (b05.pq, 'write_table'),
                                  'install': (transaction_module.os, 'replace'),
                                  'readback': (b05.pq, 'ParquetFile')}[phase]
                original = getattr(module, method)

                def fail(*args, **kwargs):
                    path = pathlib.Path(args[1] if phase in ('write', 'install') else args[0])
                    if path.name == 'schema.parquet' and b05.CALENDAR_TABLE_NAME in path.parts:
                        raise OSError(f'injected marker {phase} failure')
                    return original(*args, **kwargs)

                with patch.object(module, method, side_effect=fail):
                    with self.assertRaisesRegex(RuntimeError, '旧目标已恢复') as error:
                        b05.commit_validated_leaf_group(self.specs(), self.root)
                self.assertIsInstance(error.exception.__cause__, OSError)
                self.assert_recovered({})
                isolated = list(self.silver.glob('.c05q-*/**/*.parquet'))
                self.assertEqual(len(isolated), 3)
                self.assertTrue(all(path.name != 'schema.parquet' for path in isolated))

    def test_formal_calendar_readback_failure_restores_three_leaves_and_keeps_prior_group(self):
        before = self.seed()
        self.assertEqual(b05.commit_validated_leaf_group(self.specs(month=7), self.root), 4)
        before = self.hashes()
        self.output.seek(0)
        self.output.truncate()
        original = b05.ds.dataset
        calendar_root = self.silver / b05.CALENDAR_TABLE_NAME

        def fail(source, *args, **kwargs):
            if pathlib.Path(source).is_relative_to(calendar_root):
                raise OSError('injected calendar formal readback failure')
            return original(source, *args, **kwargs)

        with patch.object(b05.ds, 'dataset', side_effect=fail):
            with self.assertRaisesRegex(RuntimeError, '旧目标已恢复'):
                b05.commit_validated_leaf_group(self.specs(high=102.0), self.root)
        self.assert_recovered(before)
        self.assertEqual(len(list(self.silver.glob('.c05q-*/**/*.parquet'))), 3)


class B05EntryCellTest(unittest.TestCase):
    def test_notebook_script_and_import_routes(self):
        notebook = nbformat.read(NOTEBOOK, as_version=4)
        source = next(cell.source for cell in notebook.cells if cell.id == 'b05-entry')
        cases = [
            ({'ipykernel': object()}, {}, '__main__', 'notebook'),
            ({}, {'__file__': str(NOTEBOOK.with_suffix('.py'))}, '__main__', 'script'),
            ({}, {'__file__': str(NOTEBOOK.with_suffix('.py'))}, 'imported_b05', 'import'),
            ({'ipykernel': object()}, {'__file__': str(NOTEBOOK.with_suffix('.py'))}, 'imported_b05', 'import'),
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
                        prog_name='c05_futures_daily', standalone_mode=False,
                    )
                elif expected == 'script':
                    command.assert_called_once_with()
                    command.main.assert_not_called()
                else:
                    self.assertEqual(command.mock_calls, [])


if __name__ == '__main__':
    unittest.main()
