"""b07 函数日志验证：直接调用、原版本差分及临时湖回滚故障注入。"""
from __future__ import annotations

import contextlib
import importlib.util
import io
import pathlib
import runpy
import sys
import tempfile
from unittest import mock

import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[2]
snapshot = pathlib.Path(sys.argv[1])
spec = importlib.util.spec_from_file_location('b07_before_function_logs', snapshot / 'b07_suspected_session_reconciliation.py')
before = importlib.util.module_from_spec(spec)
spec.loader.exec_module(before)
with contextlib.redirect_stdout(io.StringIO()):
    smoke = runpy.run_path(str(ROOT / '00_draft_collection_02/scripts/check_b07_docs_logs_20260927.py'))
after = smoke['b07']
fixtures = smoke['fixtures']
runner = smoke['runner']
tables = smoke['tables']
calendar_df, contract_df, daily_df, minute_df = [pd.DataFrame(rows).loc[:, schema.names] for rows, schema, _ in tables]
candidate_keys = {tuple(calendar_df.iloc[0][name] for name in after.CALENDAR_PRIMARY_KEY)}
checks = ['five_cli_scenarios']


def capture(function, *args, **kwargs):
    with contextlib.redirect_stdout(io.StringIO()) as output:
        result = function(*args, **kwargs)
    return result, output.getvalue()


def capture_error(function, *args):
    try:
        with contextlib.redirect_stdout(io.StringIO()) as output:
            function(*args)
    except Exception as error:
        return error, output.getvalue()
    raise AssertionError('expected injected failure')


for scenario in ('matched', 'mismatched', 'empty_candidates', 'aggregate_overflow'):
    scenario_daily = daily_df.copy()
    scenario_calendar = calendar_df.copy()
    scenario_contract = contract_df.copy()
    scenario_minute = minute_df.copy()
    keys = set() if scenario == 'empty_candidates' else candidate_keys
    if scenario == 'mismatched':
        scenario_daily.loc[:, 'volume'] += 1.0
    if scenario == 'aggregate_overflow':
        scenario_daily.loc[:, 'money'] = 1e308
        scenario_minute.loc[:, 'money'] = 1e308
        extra_minute = scenario_minute.copy()
        extra_minute.loc[:, 'bar_at'] += pd.Timedelta(minutes=1)
        scenario_minute = pd.concat([scenario_minute, extra_minute], ignore_index=True)
        scenario_calendar.loc[1, ['expected_bar_count', 'actual_bar_count']] = 2
        scenario_calendar.loc[1, 'session_end_at'] += pd.Timedelta(minutes=1)
        scenario_calendar.loc[1, 'session_text'] = '09:01-09:03'
        scenario_contract.loc[1, 'session_end_at'] += pd.Timedelta(minutes=1)
        scenario_contract.loc[1, 'minute_count'] = 2
        scenario_contract.loc[1, 'session_text'] = '09:01-09:03'
    args = (scenario_calendar, keys, scenario_contract, scenario_daily, scenario_minute, fixtures.CHECKED_AT)
    if scenario == 'aggregate_overflow':
        old_error, _ = capture_error(before.reconcile_partition, *args)
        new_error, output = capture_error(after.reconcile_partition, *args)
        assert type(old_error) is type(new_error) and str(old_error) == str(new_error)
        assert 'function=reconcile_partition; phase=reconcile; status=failed' in output
        assert 'processed_candidates=0' in output and 'candidate=' in output
        assert 'phase=reconcile; status=completed' not in output
    else:
        old_result, _ = capture(before.reconcile_partition, *args)
        new_result, output = capture(after.reconcile_partition, *args)
        for old_frame, new_frame in zip(old_result[:2], new_result[:2]):
            pd.testing.assert_frame_equal(old_frame, new_frame)
        assert old_result[2] == new_result[2]
        assert 'function=reconcile_partition; phase=reconcile; status=started' in output
        assert 'function=reconcile_partition; phase=reconcile; status=completed' in output
        assert 'persisted=true' not in output
    checks.append('generation_' + scenario)

with tempfile.TemporaryDirectory(prefix='b07-direct-read-') as directory:
    missing_path = pathlib.Path(directory) / after.CALENDAR_TABLE_NAME
    error, output = capture_error(after.open_exact_dataset, missing_path, after.CALENDAR_PARTITIONING, after.FUTURES_BAR_CALENDAR_SCHEMA, 'missing temporary table')
    assert isinstance(error, FileNotFoundError)
    assert 'phase=dataset_open; status=failed' in output and 'failed_phase=discovery' in output
    assert 'status=completed' not in output
    result, output = capture(after.commit_calendar_partitions, {}, pathlib.Path(directory))
    assert result == 0 and 'outcome=no_partitions' in output and 'phase=evidence_state' not in output
checks.extend(['read_failure', 'empty_commit'])

base_row = smoke['retained'].copy()
partition_frames = {}
for month in (8, 9):
    row = base_row.copy()
    delta = pd.DateOffset(months=month - 8)
    row.update(month=month, trading_date=(pd.Timestamp(row['trading_date']) + delta).date())
    row['session_start_at'] += delta
    row['session_end_at'] += delta
    partition_frames[('1m', 'XSGE', 2026, month)] = pd.DataFrame([row]).loc[:, after.FUTURES_BAR_CALENDAR_SCHEMA.names]
pending_frames = {key: frame.assign(quality_reason='new temporary evidence') for key, frame in partition_frames.items()}

for scenario in ('success', 'formal_readback_failure', 'rollback_failure'):
    with tempfile.TemporaryDirectory(prefix='b07-batch-log-') as directory:
        lake_root = pathlib.Path(directory).resolve()
        capture(before.commit_calendar_partitions, partition_frames, lake_root)
        original_files = {str(p.relative_to(lake_root)): p.read_bytes() for p in lake_root.rglob('*.parquet')}
        target = lake_root / 'silver' / after.CALENDAR_TABLE_NAME
        late_leaf = target / 'bar_frequency=1m/exchange_code=XSGE/year=2026/month=9'
        original_dataset = after.ds.dataset
        transaction_module = sys.modules[after.StagedPathTransaction.__module__]
        original_move = transaction_module.os.replace
        injected_read_error = OSError('injected second formal leaf readback failure')
        injected_rollback_error = PermissionError('injected restore failure')

        def open_dataset(path, *args, **kwargs):
            if scenario != 'success' and pathlib.Path(path).resolve() == late_leaf:
                raise injected_read_error
            return original_dataset(path, *args, **kwargs)

        def move_path(source, destination, *args, **kwargs):
            assert pathlib.Path(source).resolve().is_relative_to(lake_root)
            assert pathlib.Path(destination).resolve().is_relative_to(lake_root)
            if scenario == 'rollback_failure' and '.backup-' in str(source):
                raise injected_rollback_error
            return original_move(source, destination, *args, **kwargs)

        with mock.patch.object(after.ds, 'dataset', side_effect=open_dataset), mock.patch.object(transaction_module.os, 'replace', side_effect=move_path):
            if scenario == 'success':
                result, output = capture(after.commit_calendar_partitions, pending_frames, lake_root)
                assert result == 2
                committed_line = next(line for line in output.splitlines() if line.startswith('committed:'))
                state_line = next(line for line in output.splitlines() if 'phase=evidence_state;' in line)
                assert 'function=commit_calendar_partitions' in committed_line
                assert 'persisted=true' in state_line and 'date_watermark=none' in state_line
                assert output.rindex('phase=formal_readback; status=completed') < output.index(committed_line) < output.index(state_line)
                assert not list((lake_root / 'silver').glob('.*'))
            else:
                error, output = capture_error(after.commit_calendar_partitions, pending_frames, lake_root)
                assert 'verified_partitions=1/2' in output and 'batch_state=pending' in output
                assert 'phase=commit; status=failed' in output
                assert 'committed:' not in output and 'phase=evidence_state; status=completed' not in output
                if scenario == 'formal_readback_failure':
                    assert isinstance(error, RuntimeError) and error.__cause__ is injected_read_error
                    assert 'phase=rollback; status=completed; partitions=2' in output
                    current_files = {str(p.relative_to(lake_root)): p.read_bytes() for p in target.rglob('*.parquet')}
                    assert original_files == current_files
                    assert not list((lake_root / 'silver').glob('.*.backup-*'))
                    assert not list((lake_root / 'silver').glob('.*.staging-*'))
                    assert len(list((lake_root / 'silver').glob('.*.failed-*/**/*.parquet'))) == 2
                else:
                    assert isinstance(error, RuntimeError) and error.__cause__ is injected_read_error
                    assert 'phase=rollback; status=failed' in output and '回滚不完整' in str(error)
                    assert list((lake_root / 'silver').glob('.*.backup-*'))
                    assert list((lake_root / 'silver').glob('.*.failed-*'))
        checks.append('commit_' + scenario)

# CLI 仍有整批汇总；函数级生成和提交起止不得残留在 main。
output = smoke['written'].output
assert 'function=main; phase=reconcile; status=started' not in output
assert 'function=main; phase=commit; status=started' not in output
assert 'committed: table=dim_futures_bar_calendar; function=main' not in output
assert output.count('function=commit_calendar_partitions; phase=commit; status=started') == 1
assert output.count('function=commit_calendar_partitions; phase=commit; status=completed') == 1
checks.append('main_function_ownership')
print('Passed:', ', '.join(checks))
