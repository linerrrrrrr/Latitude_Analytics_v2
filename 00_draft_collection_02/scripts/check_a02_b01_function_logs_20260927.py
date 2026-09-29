"""a02/b01 函数日志：原版本差分、临时湖与受控异常检查，不访问正式湖或 API。"""
import contextlib
import hashlib
import importlib.util
import io
import itertools
import json
import pathlib
import sys
import tempfile
import time
from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from click.testing import CliRunner

ROOT = pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT = pathlib.Path(sys.argv[1])
SOURCE = ROOT / '02_Futures_Lakehouse/a02_Futures_Exchange_Reports/b01_exchange_report_calendar.py'


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


before = load('report_before_logs', SNAPSHOT / SOURCE.name)
after = load('report_after_logs', SOURCE)
CHECKED_AT = datetime(2026, 9, 1, tzinfo=timezone.utc)


class FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return CHECKED_AT


before.datetime = after.datetime = FrozenDatetime
checks = []
outputs = {}
started = time.perf_counter()


def capture(name, function, *args):
    with contextlib.redirect_stdout(io.StringIO()) as output:
        result = function(*args)
    outputs[name] = output.getvalue()
    return result, output.getvalue()


def failure(name, function, *args):
    try:
        with contextlib.redirect_stdout(io.StringIO()) as output:
            function(*args)
    except Exception as error:
        outputs[name] = output.getvalue()
        return error, output.getvalue()
    raise AssertionError(f'{name}: expected controlled failure')


def report_table(frame):
    return after.pandas_to_arrow(frame, after.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA)


def write_report(lake, frame):
    ds.write_dataset(report_table(frame), lake / 'silver' / after.TABLE_NAME,
                     format='parquet', partitioning=after.REPORT_PARTITIONING)


def read_report(lake):
    return ds.dataset(lake / 'silver' / after.TABLE_NAME, format='parquet',
                      partitioning=after.REPORT_PARTITIONING).to_table(
        columns=after.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names,
    ).sort_by([(name, 'ascending') for name in after.PRIMARY_KEY])


def file_hashes(path):
    return {str(p.relative_to(path)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in path.rglob('*') if p.is_file()}


rows = [dict(underlying_code=code, exchange_code=exchange, trading_date=day,
             active_contract_count=1, source='test_fixture', updated_at=CHECKED_AT,
             year=day.year, month=day.month)
        for exchange, code in [('XSGE', 'RB'), ('CCFX', 'IF')]
        for day in [date(2024, 1, 3), date(2024, 2, 1)]]
upstream_table = pa.Table.from_pylist(rows, schema=after.FUTURES_VARIETY_CALENDAR_SCHEMA)
upstream_df = after.arrow_to_pandas(upstream_table, after.FUTURES_VARIETY_CALENDAR_SCHEMA).sort_values(after.UPSTREAM_PRIMARY_KEY).reset_index(drop=True)
before_upstream_df = (
    before.validate_upstream_table(upstream_table, 'fixture')
    if hasattr(before, 'validate_upstream_table')
    else before.arrow_to_pandas(upstream_table, before.FUTURES_VARIETY_CALENDAR_SCHEMA).sort_values(before.UPSTREAM_PRIMARY_KEY).reset_index(drop=True)
)
pd.testing.assert_frame_equal(upstream_df, before_upstream_df)
checks.append('trusted_upstream_conversion_same_result')
empty = after.empty_pandas(after.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA)
expected, log = capture('generate', after.build_expected_calendar, upstream_df, empty, CHECKED_AT)
pd.testing.assert_frame_equal(expected, before.build_expected_calendar(upstream_df, empty, CHECKED_AT))
assert 'function=build_expected_calendar; phase=generate; status=completed' in log
assert 'generated_rows=12; persisted=false' in log
checks.append('generate_same_result')
empty_result, log = capture('empty_generation', after.build_expected_calendar, upstream_df.iloc[:0], empty, CHECKED_AT)
assert empty_result.empty and 'generated_rows=0' in log
checks.append('empty_generation_reports_completion')
validated, log = capture('report_validation', after.validate_report_calendar_table, report_table(expected), 'fixture')
pd.testing.assert_frame_equal(validated, before.validate_report_calendar_table(report_table(expected), 'fixture'))
checks.append('report_validation_same_result')
invalid_table = pa.concat_tables([report_table(expected), report_table(expected).slice(0, 1)])
old_error, _ = failure('old_invalid', before.validate_report_calendar_table, invalid_table, 'fixture')
new_error, log = failure('new_invalid', after.validate_report_calendar_table, invalid_table, 'fixture')
assert type(old_error) is type(new_error) and str(old_error) == str(new_error)
assert 'failed_phase=primary_key' in log and 'phase=validate; status=completed' not in log
checks.append('validator_failure_preserves_exception')
keys, log = capture('difference', after.changed_partition_keys, expected, empty)
assert keys == before.changed_partition_keys(expected, empty)
assert 'compared_partitions=12/12; changed_partitions=12' in log
checks.append('partition_difference_same_result')
completed = expected.copy()
mask = completed.exchange_code.eq('XSGE')
completed.loc[mask, ['is_fetch_completed', 'fetch_result_status', 'actual_record_count', 'quality_status', 'quality_reason', 'fetch_run_id', 'fetch_completed_at', 'quality_checked_at']] = [True, 'success', 20, 'warning', '已确认的来源质量证据。', 'fixture-run', CHECKED_AT, CHECKED_AT]
with patch.object(after, 'FUTURES_FACT_VARIETY_PAIRS', frozenset()), patch.object(before, 'FUTURES_FACT_VARIETY_PAIRS', frozenset()):
    old = before.build_expected_calendar(upstream_df, completed, CHECKED_AT)
    new, _ = capture('policy_inheritance', after.build_expected_calendar, upstream_df, completed, CHECKED_AT)
pd.testing.assert_frame_equal(old, new)
assert new.loc[mask, 'quality_status'].eq('warning').all()
checks.append('completed_quality_evidence_preserved')

# 有界构造 1001 个品种日，仅验证生成循环的限频计数；时间源受控推进，不代表生产耗时。
progress_rows = [dict(rows[0], trading_date=date(1990, 1, 1) + timedelta(days=i),
                      year=(date(1990, 1, 1) + timedelta(days=i)).year,
                      month=(date(1990, 1, 1) + timedelta(days=i)).month)
                 for i in range(1001)]
progress_table = pa.Table.from_pylist(progress_rows, schema=after.FUTURES_VARIETY_CALENDAR_SCHEMA)
progress_df = after.arrow_to_pandas(progress_table, after.FUTURES_VARIETY_CALENDAR_SCHEMA)
with patch.object(after.time, 'perf_counter', side_effect=itertools.count(0, 3)):
    _, log = capture('generation_progress', after.build_expected_calendar, progress_df.iloc[:1001], empty, CHECKED_AT)
assert 'processed_upstream=1000/1001; generated_rows=3000' in log
checks.append('bounded_loop_progress_counts')

with tempfile.TemporaryDirectory(prefix='a02-b01-function-logs-') as temp:
    base = pathlib.Path(temp)
    lake = base / 'current'
    old_lake = base / 'previous'
    upstream_path = lake / 'silver' / after.UPSTREAM_TABLE_NAME
    ds.write_dataset(upstream_table, upstream_path, format='parquet', partitioning=after.UPSTREAM_PARTITIONING)
    dataset, log = capture('dataset_open', after.open_exact_dataset, upstream_path, after.UPSTREAM_PARTITIONING, after.FUTURES_VARIETY_CALENDAR_SCHEMA, 'fixture')
    assert 'materialized=false' in log and dataset.to_table().num_rows == 4
    error, log = failure('missing_input', after.open_exact_dataset, base / 'absent', after.UPSTREAM_PARTITIONING, after.FUTURES_VARIETY_CALENDAR_SCHEMA, 'fixture')
    assert isinstance(error, FileNotFoundError) and 'failed_phase=discovery' in log
    checks.append('dataset_open_and_failure_logs')
    count, log = capture('commit', after.commit_partitions, expected, keys, lake)
    old_count = before.commit_partitions(expected, keys, old_lake)
    assert count == old_count == 12 and read_report(lake).equals(read_report(old_lake), check_metadata=True)
    assert log.index('phase=formal_readback; status=completed') < log.index('committed:') < log.index('phase=calendar_state; status=completed')
    assert 'date_watermark=none' in log and 'function=main' not in log
    checks.append('direct_commit_same_result_and_state_order')
    count, log = capture('empty_commit', after.commit_partitions, expected, [], lake)
    assert count == 0 and 'calendar_state=unchanged' in log and 'persisted=true' not in log
    checks.append('empty_commit_no_persisted_state')
    # 正式复读故障只注入 Dataset 打开边界；不依赖已删除的重复业务校验。
    before_hashes = file_hashes(lake / 'silver' / after.TABLE_NAME)
    real_open = after.open_exact_dataset
    def reject_formal(*args, **kwargs):
        if args[3] == '正式报告日历':
            raise ValueError('controlled formal readback failure')
        return real_open(*args, **kwargs)
    with patch.object(after, 'open_exact_dataset', side_effect=reject_formal):
        error, log = failure('formal_rollback', after.commit_partitions, expected, keys, lake)
    assert type(error) is RuntimeError and type(error.__cause__) is ValueError
    assert 'phase=rollback; status=completed' in log
    assert 'phase=commit; status=failed; failed_phase=formal_readback' in log
    assert 'phase=calendar_state; status=completed' not in log and 'committed:' not in log
    assert file_hashes(lake / 'silver' / after.TABLE_NAME) == before_hashes
    checks.append('formal_failure_restores_original_bytes')
    with patch.object(after.pq, 'write_table', side_effect=OSError('controlled staging failure')):
        error, log = failure('staging_failure', after.commit_partitions, expected, keys, lake)
    assert type(error) is OSError and 'failed_phase=staging_prepare' in log and 'committed:' not in log
    assert file_hashes(lake / 'silver' / after.TABLE_NAME) == before_hashes
    checks.append('staging_failure_no_success_log')
    transaction_module = sys.modules[after.StagedPathTransaction.__module__]
    real_move = transaction_module.os.replace
    def reject_restore(src, dst):
        if any(part.startswith('.a02-b01-b-') for part in pathlib.Path(src).parts):
            raise OSError('controlled restore failure')
        return real_move(src, dst)
    recovery_lake = base / 'recovery'
    write_report(recovery_lake, expected)
    with patch.object(after, 'open_exact_dataset', side_effect=reject_formal), patch.object(transaction_module.os, 'replace', side_effect=reject_restore):
        error, log = failure('recovery_failure', after.commit_partitions, expected, keys, recovery_lake)
    assert type(error) is RuntimeError and type(error.__cause__) is ValueError
    assert 'phase=rollback; status=failed' in log and 'committed:' not in log
    assert list((recovery_lake / 'silver').glob('.a02-b01-b-*'))
    checks.append('recovery_failure_retains_cause_and_evidence')
    remaining = expected.loc[expected.exchange_code.eq('CCFX')].copy()
    delete_keys, _ = capture('delete_keys', after.changed_partition_keys, remaining, expected)
    _, log = capture('delete_leaves', after.commit_partitions, remaining, delete_keys, lake)
    assert read_report(lake).num_rows == 6 and 'action=delete' in log
    checks.append('leaf_deletion_logs')
    clear_keys, _ = capture('clear_keys', after.changed_partition_keys, empty, remaining)
    _, log = capture('clear_table', after.commit_partitions, empty, clear_keys, lake)
    assert read_report(lake).num_rows == 0 and 'full_swap=true' in log and 'persisted=true' in log
    checks.append('full_empty_replacement_logs')
    runner = CliRunner()
    for name, args, outcome in [
        ('cli_read_only', [], 'read_only'),
        ('cli_commit', ['--write'], 'committed'),
        ('cli_no_changes', ['--write'], 'no_changes'),
        ('cli_explicit', ['--start-date', '2024-01-03', '--end-date', '2024-01-03'], 'no_changes'),
    ]:
        result = runner.invoke(after.main, ['--lake-root', str(lake), *args])
        outputs[name] = result.output
        assert result.exit_code == 0, (name, result.exception, result.output)
        assert f'outcome={outcome}' in result.output
        assert 'function=main; phase=generate;' not in result.output
        assert 'function=main; phase=commit; status=completed' not in result.output
    checks.append('four_cli_branches_without_duplicate_owner_logs')
    missing = runner.invoke(after.main, ['--lake-root', str(base / 'missing')])
    assert missing.exit_code != 0 and 'function=main; phase=run; status=failed' in missing.output
    assert 'function=main; phase=run; status=completed' not in missing.output
    checks.append('run_failure_has_no_success_footer')

for name, log in outputs.items():
    (SNAPSHOT / f'{name}.log').write_text(log, encoding='utf-8')
hashes = json.loads((SNAPSHOT / 'hashes.json').read_text(encoding='utf-8'))
changed = [name for name, value in hashes.items() if hashlib.sha256((ROOT / name).read_bytes()).hexdigest() != value]
allowed = {str(SOURCE.relative_to(ROOT)), str(SOURCE.with_suffix('.ipynb').relative_to(ROOT))}
allowed.update(str(pathlib.Path(name)) for name in ('AGENTS.md', '02_Futures_Lakehouse/AGENTS.md', '02_Futures_Lakehouse/README.md'))
assert set(changed) <= allowed, changed
report = dict(checks_passed=len(checks), checks=checks, runtime_s=round(time.perf_counter() - started, 3), changed_files=changed)
(SNAPSHOT / 'verification.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
print(json.dumps(report, ensure_ascii=False, indent=2))
