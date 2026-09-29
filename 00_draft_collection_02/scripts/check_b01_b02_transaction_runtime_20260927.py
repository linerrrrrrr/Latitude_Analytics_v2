"""有界检查 b01/b02 抽象后的语义、耗时和日志；正式湖只读。"""

from __future__ import annotations

import ast
import hashlib
import importlib.util
import io
import json
import pathlib
import shutil
import statistics
import sys
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from click.testing import CliRunner

PROJECT = pathlib.Path(__file__).resolve().parents[2]
DRAFT = PROJECT / '00_draft_collection_02'
OUTPUT = DRAFT / 'staged_path_transaction_refactor/runtime_check_20260927'
OUTPUT.mkdir(exist_ok=True)
sys.path[:0] = [str(PROJECT / '02_Futures_Lakehouse'),
               str(PROJECT / '02_Futures_Lakehouse/a01_Futures_Market_Data'),
               str(DRAFT / 'tests'), str(PROJECT)]

from config.settings import settings
from config.data_contracts import pandas_to_arrow
import b01_trade_calendar as b01
import b02_futures_variety_calendar as b02
import a00_04_staged_path_transaction as transaction_module


class TimedResult(unittest.TextTestResult):
    def startTest(self, test):
        self.test_started = time.perf_counter()
        super().startTest(test)

    def stopTest(self, test):
        test_runs.append({'id': test.id(), 'elapsed_s': time.perf_counter() - self.test_started})
        super().stopTest(test)


def snapshot(root):
    return {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in root.rglob('*.parquet')}


class TimedLog:
    encoding = 'utf-8'
    errors = 'strict'

    def __init__(self, stream):
        self.stream = stream
        self.pending_text = ''
        self.events = []

    def write(self, value):
        self.stream.write(value)
        self.pending_text += value
        while '\n' in self.pending_text:
            line, self.pending_text = self.pending_text.split('\n', 1)
            self.events.append((time.perf_counter(), line))
            self.stream.flush()
        return len(value)

    def flush(self):
        self.stream.flush()


def load_baseline(stem):
    baseline = pathlib.Path(json.loads((DRAFT / 'staged_path_transaction_refactor/baseline.json').read_text(encoding='utf-8'))['root'])
    path = baseline / '02_Futures_Lakehouse/a01_Futures_Market_Data' / f'{stem}.py'
    spec = importlib.util.spec_from_file_location(f'before_{stem}', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def read_frame(path, schema, partitioning):
    return ds.dataset(path, format='parquet', partitioning=partitioning).to_table(
        columns=schema.names,
    ).to_pandas()


def fields(line):
    return dict(part.strip().split('=', 1) for part in line.split(': ', 1)[1].split(';') if '=' in part)


started = time.perf_counter()
results = {'python': sys.executable, 'repetitions': 3, 'benchmark': [], 'cli': [], 'inventory': []}
assert pathlib.Path(transaction_module.__file__).resolve() == PROJECT / '02_Futures_Lakehouse/a00_04_staged_path_transaction.py'
print('phase=regression; tests=44', flush=True)
import test_b01_c01_c02_daily_tail_modes as daily
import test_futures_variety_calendar_complete_catalog as catalog
import test_b01_fragment_physical_contracts as physical
import test_b01_b02_commit_semantics as regression
import test_staged_path_transaction as transaction_tests

physical.CASES = [case for case in physical.CASES if case[0] in (physical.C01, physical.C02)]
test_runs = []
suite = unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromModule(module)
                          for module in (daily, catalog, physical, regression, transaction_tests))
with (OUTPUT / 'regression.log').open('w', encoding='utf-8', newline='\n') as log, redirect_stdout(log):
    regression_started = time.perf_counter()
    test_result = unittest.TextTestRunner(stream=log, verbosity=2, resultclass=TimedResult).run(suite)
    results['regression_elapsed_s'] = time.perf_counter() - regression_started
results['tests'] = test_runs
results['regression_ok'] = test_result.wasSuccessful()
assert test_result.wasSuccessful(), '回归失败，详见 regression.log'
print(f'phase=regression; status=passed; elapsed_s={results["regression_elapsed_s"]:.3f}', flush=True)

worker_source = (PROJECT / '02_Futures_Lakehouse/operations/background_worker.py').read_text(encoding='utf-8')
progress_prefixes = next(ast.literal_eval(node.value) for node in ast.parse(worker_source).body
                        if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name)
                        and target.id == 'DEFAULT_PROGRESS_PREFIXES' for target in node.targets))
formal_snapshots = {}
workflows = []
for module, schema, date_field in ((b01, b01.TRADE_CALENDAR_SCHEMA, 'calendar_date'),
                                  (b02, b02.FUTURES_VARIETY_CALENDAR_SCHEMA, 'trading_date')):
    source = settings.futures_lake_root / 'silver' / module.TABLE_NAME
    formal_snapshots[source] = snapshot(source)
    partitioning = ds.partitioning(pa.schema([schema.field(name) for name in module.PARTITION_COLUMNS]), flavor='hive')
    frame = read_frame(source, schema, partitioning)
    last_date = max(frame[date_field])
    tail = frame.loc[frame[date_field].eq(last_date)].copy()
    info = {'table': module.TABLE_NAME, 'rows': len(frame), 'files': len(formal_snapshots[source]),
            'actual_partitions': len(frame[module.PARTITION_COLUMNS].drop_duplicates()),
            'bytes': sum(path.stat().st_size for path in source.rglob('*.parquet')),
            'min_date': str(min(frame[date_field])), 'max_date': str(last_date),
            'tail_rows': len(tail), 'tail_partitions': len(tail[module.PARTITION_COLUMNS].drop_duplicates())}
    results['inventory'].append(info)
    workflows.append((module, load_baseline(module.__name__), schema, date_field, partitioning, source, frame, tail))

# 每次仅复制正式文件到本轮独有测试目录；不硬链接，不修改正式路径。
with tempfile.TemporaryDirectory(prefix='b01-b02-runtime-', dir=DRAFT) as directory:
    base = pathlib.Path(directory).resolve()
    assert base.is_relative_to(DRAFT.resolve())
    full_seed = base / 'full_seed'
    tail_seed = base / 'tail_seed'
    for current, old, schema, date_field, partitioning, source, frame, tail in workflows:
        shutil.copytree(source, full_seed / 'silver' / current.TABLE_NAME)
        shutil.copytree(source, tail_seed / 'silver' / current.TABLE_NAME)
        # 在测试副本中移除最新一天，模拟前一日已经成功提交的正常日常状态。
        touched_keys = tail[current.PARTITION_COLUMNS].drop_duplicates()
        for key in touched_keys.itertuples(index=False, name=None):
            mask = frame[date_field].ne(max(frame[date_field]))
            for column, value in zip(current.PARTITION_COLUMNS, key, strict=True):
                mask &= frame[column].eq(value)
            retained = frame.loc[mask]
            assert not retained.empty, '本批尾部情景要求已有同月/同年旧行'
            ds.write_dataset(pandas_to_arrow(retained, schema), tail_seed / 'silver' / current.TABLE_NAME,
                             format='parquet', partitioning=partitioning,
                             existing_data_behavior='delete_matching', basename_template='part-{i}.parquet')

    for current, old, schema, date_field, partitioning, source, frame, tail in workflows:
        for scenario, incoming in (('tail_one_day', tail), ('empty_lake_full_build', frame)):
            for repetition in range(3):
                order = (('before', old), ('current', current)) if repetition % 2 == 0 else (('current', current), ('before', old))
                for version, module in order:
                    if time.perf_counter() - started > 300:
                        raise TimeoutError('达到本地检查 300 秒边界；停止后续测量。')
                    run_name = f'{module.TABLE_NAME}-{scenario}-{version}-{repetition + 1}'
                    print(f'phase=benchmark; run={run_name}; status=started', flush=True)
                    with tempfile.TemporaryDirectory(prefix='run-', dir=base) as run_directory:
                        lake_root = pathlib.Path(run_directory)
                        if scenario == 'tail_one_day':
                            shutil.copytree(tail_seed / 'silver' / module.TABLE_NAME,
                                            lake_root / 'silver' / module.TABLE_NAME)
                        before_files = snapshot(lake_root / 'silver' / module.TABLE_NAME)
                        arguments = () if current is b01 else (min(incoming[date_field]), max(incoming[date_field]))
                        with (OUTPUT / f'{run_name}.log').open('w', encoding='utf-8', newline='\n') as stream:
                            log = TimedLog(stream)
                            commit_started = time.perf_counter()
                            with redirect_stdout(log):
                                returned_rows = module.commit_partitions(incoming.copy(), lake_root, *arguments)
                            elapsed = time.perf_counter() - commit_started
                        assert returned_rows == len(incoming)
                        after = read_frame(lake_root / 'silver' / module.TABLE_NAME, schema, partitioning)
                        pd.testing.assert_frame_equal(frame.sort_values(module.PRIMARY_KEY).reset_index(drop=True),
                                                      after.sort_values(module.PRIMARY_KEY).reset_index(drop=True))
                        if scenario == 'tail_one_day':
                            changed_leaf_paths = {str(pathlib.Path(*[f'{column}={value}' for column, value in zip(module.PARTITION_COLUMNS, key, strict=True)]))
                                                  for key in tail[module.PARTITION_COLUMNS].drop_duplicates().itertuples(index=False, name=None)}
                            after_files = snapshot(lake_root / 'silver' / module.TABLE_NAME)
                            for file, digest in before_files.items():
                                if str(pathlib.Path(file).parent) not in changed_leaf_paths:
                                    assert after_files[file] == digest, file
                        events = [(at, line) for at, line in log.events if line.startswith(progress_prefixes)]
                        committed = [fields(line) for _, line in events if line.startswith('partition_committed:')]
                        total = int(fields(next(line for _, line in events if line.startswith('committed:')))['partitions'])
                        assert len(committed) == total
                        assert [int(event['completed']) for event in committed] == list(range(1, total + 1))
                        assert events[-1][1].startswith('committed:') and 'status=completed' in events[-1][1]
                        assert not any((lake_root / 'silver').glob('.c0*'))
                        times = [commit_started, *(at for at, _ in events), commit_started + elapsed]
                        record = {'table': current.TABLE_NAME, 'scenario': scenario, 'version': version,
                                  'repetition': repetition + 1, 'elapsed_s': elapsed, 'rows': len(incoming),
                                  'planned_partitions': total, 'progress_events': len(events),
                                  'max_progress_gap_s': max(right - left for left, right in zip(times, times[1:])),
                                  'content_equal': True, 'untouched_files_equal': scenario == 'tail_one_day', 'cleanup_ok': True}
                        results['benchmark'].append(record)
                    print(f'phase=benchmark; run={run_name}; status=passed; elapsed_s={elapsed:.3f}; partitions={total}', flush=True)

    as_of = max(workflows[0][6]['calendar_date'])

    class SnapshotDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(as_of.year, as_of.month, as_of.day, 21, tzinfo=tz)

    for current, old, *_ in workflows:
        for version, module in (('before', old), ('current', current)):
            for repetition in range(3):
                cli_started = time.perf_counter()
                with patch.object(module, 'datetime', SnapshotDateTime), patch(
                    'config.jqdata_connection.authenticate_jqdata', side_effect=AssertionError('无待办不应认证或联网'),
                ) as authenticate:
                    result = CliRunner().invoke(module.main, ['--lake-root', str(full_seed)])
                elapsed = time.perf_counter() - cli_started
                assert result.exit_code == 0, result.output
                authenticate.assert_not_called()
                assert 'up_to_date:' in result.output
                results['cli'].append({'table': current.TABLE_NAME, 'scenario': 'no_pending_as_of_snapshot',
                                       'version': version, 'repetition': repetition + 1, 'elapsed_s': elapsed,
                                       'authentication_calls': 0, 'exit_code': result.exit_code})
                (OUTPUT / f'{current.TABLE_NAME}-no_pending-{version}-{repetition+1}.log').write_text(result.output, encoding='utf-8')

for source, before in formal_snapshots.items():
    assert snapshot(source) == before, f'正式数据发生变化：{source}'
results['formal_files_unchanged'] = True
results['total_elapsed_s'] = time.perf_counter() - started
(OUTPUT / 'results.json').write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
for records in (results['benchmark'], results['cli']):
    for table, scenario in sorted({(record['table'], record['scenario']) for record in records}):
        summary = {'table': table, 'scenario': scenario}
        for version in ('before', 'current'):
            values = [record['elapsed_s'] for record in records if (record['table'], record['scenario'], record['version']) == (table, scenario, version)]
            summary[version] = {'median_s': statistics.median(values), 'min_s': min(values), 'max_s': max(values)}
        print(json.dumps(summary, ensure_ascii=False), flush=True)
print(f'check_ok: True; total_elapsed_s={results["total_elapsed_s"]:.3f}; results={OUTPUT / "results.json"}', flush=True)
