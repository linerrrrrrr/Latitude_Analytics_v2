"""b08 日志归属、前后结果/I/O 差分、临时湖提交及明确故障注入。"""

import contextlib
import importlib.util
import inspect
import io
import json
import pathlib
import sys
import tempfile
from collections import Counter
from unittest import mock

import pandas as pd
from click.testing import CliRunner

ROOT = pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT = pathlib.Path(sys.argv[1])
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a01_Futures_Market_Data/b08_full_minute_quality.py')
sys.path.insert(0, str(ROOT / '00_draft_collection_02/tests'))
import test_b01_ohlc_retention as fixture

after = fixture.full_quality
transaction_module = sys.modules[after.StagedPathTransaction.__module__]
spec = importlib.util.spec_from_file_location('b08_before_function_logs', SNAPSHOT / RELATIVE)
before = importlib.util.module_from_spec(spec)
spec.loader.exec_module(before)
checks = []
profiles = {}


def commit_snapshot(module, root, missing_staging, calendar_staging, audit, run_id):
    # 兼容旧快照的双路径签名；当前入口统一使用一个 staging 根。
    if 'staging_path' in inspect.signature(module.commit_full_audit).parameters:
        return module.commit_full_audit(root, missing_staging.parent, audit, run_id)
    return module.commit_full_audit(root, missing_staging, calendar_staging, audit, run_id)


def capture(function, *args):
    with contextlib.redirect_stdout(io.StringIO()) as output:
        result = function(*args)
    return result, output.getvalue()


def capture_error(function, *args):
    try:
        with contextlib.redirect_stdout(io.StringIO()) as output:
            function(*args)
    except Exception as error:
        return error, output.getvalue()
    raise AssertionError('expected injected failure')


def seed(root, scenario='missing'):
    calendar_rows, minute_rows = [], []
    for month in (7, 8, 9):
        row = fixture.calendar_row(bar_frequency='1m', quality_status='warning',
                                   quality_reason='来源 OHLC warning 原样保留。',
                                   evidence_source='fact_futures_minute:formal_nonempty_session')
        delta = pd.DateOffset(months=month - 8)
        start = fixture.SESSION_START + delta
        required = month != 7 and scenario != 'no_required'
        numbers = (1, 2, 3, 4) if scenario == 'zero_missing' else (1, 2, 4)
        row.update(month=month, trading_date=start.date(), session_start_at=start,
                   session_end_at=start+pd.Timedelta(minutes=4), session_text='09:00-09:04',
                   expected_bar_count=4, actual_bar_count=len(numbers), is_fetch_required=required,
                   is_data_missing=required and len(numbers)<4, missing_bar_count=4-len(numbers) if required else 0)
        if scenario == 'incomplete' and month == 9:
            row.update(is_fetch_completed=False, actual_bar_count=0, is_data_missing=True,
                       missing_bar_count=4, fetch_run_id=None, fetch_completed_at=None,
                       missing_checked_at=None, quality_checked_at=None, quality_status='pending')
        calendar_rows.append(row)
        for minute in numbers:
            minute_rows.append(dict(fixture.minute_row(), month=month, trading_date=start.date(),
                                    bar_at=start+pd.Timedelta(minutes=minute)))
    calendar_rows.append(fixture.calendar_row(bar_frequency='1d', quality_status='passed',
                                             quality_reason='日线已完成。', evidence_source='fact_futures_daily'))
    for rows, schema, partitions in (
        (calendar_rows, after.FUTURES_BAR_CALENDAR_SCHEMA, after.CALENDAR_PARTITION_COLUMNS),
        (minute_rows, after.FUTURES_MINUTE_SCHEMA, after.MINUTE_PARTITION_COLUMNS),
    ):
        fixture.write_partitioned(pd.DataFrame(rows), root/'silver'/schema.metadata[b'table_name'].decode(), schema, partitions)


def formal_files(root):
    return {str(p.relative_to(root)): p.read_bytes()
            for table in (after.CALENDAR_TABLE_NAME, after.MISSING_TABLE_NAME, after.MINUTE_TABLE_NAME)
            for p in (root/'silver'/table).rglob('*.parquet')}


for scenario in ('missing', 'zero_missing', 'no_required'):
    outcomes = []
    for version, module in (('before', before), ('after', after)):
        with tempfile.TemporaryDirectory(prefix='b08-log-diff-') as directory:
            root = pathlib.Path(directory).resolve()
            seed(root, scenario)
            staging = root/'silver'/'.b08-s-diff'
            missing_staging = staging/module.MISSING_TABLE_NAME
            calendar_staging = staging/module.CALENDAR_TABLE_NAME
            counts = Counter()
            original_dataset = module.ds.dataset

            class DatasetProxy:
                def __init__(self, dataset):
                    self.dataset = dataset

                def __getattr__(self, name):
                    return getattr(self.dataset, name)

                def to_table(self, *args, **kwargs):
                    counts['to_table'] += 1
                    return self.dataset.to_table(*args, **kwargs)

                def count_rows(self, *args, **kwargs):
                    counts['count_rows'] += 1
                    return self.dataset.count_rows(*args, **kwargs)

                def get_fragments(self, *args, **kwargs):
                    counts['get_fragments'] += 1
                    return self.dataset.get_fragments(*args, **kwargs)

            def open_dataset(*args, **kwargs):
                counts['dataset_opens'] += 1
                return DatasetProxy(original_dataset(*args, **kwargs))

            with mock.patch.object(module.ds, 'dataset', side_effect=open_dataset):
                audit, output = capture(module.build_full_audit_staging, root, missing_staging, calendar_staging, fixture.CHECKED_AT)
                committed, commit_output = capture(commit_snapshot, module, root, missing_staging, calendar_staging, audit, 'comparison')
            logical_tables = {}
            for table_name, schema, partitioning in (
                (module.CALENDAR_TABLE_NAME, module.FUTURES_BAR_CALENDAR_SCHEMA, module.CALENDAR_PARTITIONING),
                (module.MISSING_TABLE_NAME, module.FUTURES_MISSING_BAR_SCHEMA, module.MISSING_PARTITIONING),
            ):
                table = original_dataset(root/'silver'/table_name, format='parquet', partitioning=partitioning).to_table(columns=schema.names)
                digest, _ = capture(module.table_digest, table, schema, schema.metadata[b'primary_key'].decode().split(','))
                logical_tables[table_name] = digest
            outcomes.append((audit, committed, logical_tables, counts))
            if version == 'after':
                assert f'function=build_full_audit_staging; phase=build_audit; status=started' in output
                assert 'missing_audit_total: table=fact_futures_missing_bar; function=build_full_audit_staging;' in output
                assert 'persisted=true' not in output
                assert 'function=discover_partition_keys; phase=partition_discovery; status=completed' in output
                assert 'materialized=false' in output and 'reason=no_required_sessions' in output
                assert 'function=commit_full_audit; phase=commit; status=completed' in commit_output
                assert 'phase=audit_state; status=completed' in commit_output and 'date_watermark=none' in commit_output
                assert commit_output.rindex('phase=calendar_formal_readback; status=completed') < commit_output.index('committed:') < commit_output.index('phase=audit_state;')
                if scenario == 'missing':
                    assert 'processed_groups=2' in output and 'total_sessions=2; total_missing=2' in output
                    assert 'checked_partitions=2/2' in commit_output
            profiles.setdefault(scenario, {})[version] = dict(counts)
    assert outcomes[0] == outcomes[1], (scenario, outcomes)
    checks.append('identical_results_and_io_' + scenario)

with tempfile.TemporaryDirectory(prefix='b08-log-read-error-') as directory:
    error, output = capture_error(after.open_exact_dataset, pathlib.Path(directory)/'absent',
                                  after.CALENDAR_PARTITIONING, after.FUTURES_BAR_CALENDAR_SCHEMA,
                                  after.CALENDAR_PARTITION_COLUMNS, 'temporary missing input')
    assert isinstance(error, FileNotFoundError) and 'failed_phase=discovery' in output
    assert 'phase=dataset_open; status=completed' not in output
checks.append('direct_reader_failure')

for scenario in ('incomplete', 'staging_failure', 'formal_failure', 'rollback_failure'):
    with tempfile.TemporaryDirectory(prefix='b08-log-failure-') as directory:
        root = pathlib.Path(directory).resolve()
        seed(root, scenario)
        missing_target = root/'silver'/after.MISSING_TABLE_NAME
        missing_target.mkdir()
        after.pq.write_table(after.pa.Table.from_batches([], schema=after.parquet_file_schema(after.FUTURES_MISSING_BAR_SCHEMA, after.MISSING_PARTITION_COLUMNS)), missing_target/'schema.parquet')
        previous = formal_files(root)
        runner = CliRunner()
        original_move = transaction_module.os.replace
        original_write = after.ds.write_dataset
        read_error = OSError('injected second calendar formal read failure')
        restore_error = PermissionError('injected calendar restore failure')

        class FormalDatasetProxy:
            def __init__(self, dataset):
                self.dataset = dataset

            def __getattr__(self, name):
                return getattr(self.dataset, name)

            def to_table(self, *args, **kwargs):
                if '(month == 9)' in str(kwargs.get('filter')):
                    raise read_error
                return self.dataset.to_table(*args, **kwargs)

        original_open = after.open_exact_dataset

        def open_exact(*args, **kwargs):
            dataset = original_open(*args, **kwargs)
            if scenario in ('formal_failure', 'rollback_failure') and args[4] == '正式行情日历':
                return FormalDatasetProxy(dataset)
            return dataset

        def move(source, destination, *args, **kwargs):
            if scenario == 'rollback_failure' and '.b08-b-' in str(source) and pathlib.Path(source).name == 'month=9':
                raise restore_error
            return original_move(source, destination, *args, **kwargs)

        def write(*args, **kwargs):
            if scenario == 'staging_failure':
                raise OSError('injected staging write failure')
            return original_write(*args, **kwargs)

        with mock.patch.object(after, 'open_exact_dataset', side_effect=open_exact), mock.patch.object(transaction_module.os, 'replace', side_effect=move), mock.patch.object(after.ds, 'write_dataset', side_effect=write):
            result = runner.invoke(after.main, ['--lake-root', str(root), '--confirm-full-quality', '--write'])
        assert result.exit_code != 0, result.output
        assert 'persisted=true' not in result.output and 'committed:' not in result.output and 'Missing audit run ended' not in result.output
        if scenario in ('incomplete', 'staging_failure'):
            assert formal_files(root) == previous
            assert 'function=build_full_audit_staging; phase=build_audit; status=failed' in result.output
            assert 'phase=build_audit; status=completed' not in result.output
            assert not list((root/'silver').glob('.b08-s-*'))
            assert ('failed_phase=readiness' if scenario == 'incomplete' else 'failed_phase=missing_staging_write') in result.output
        elif scenario == 'formal_failure':
            assert isinstance(result.exception, RuntimeError) and result.exception.__cause__ is read_error
            assert formal_files(root) == previous
            assert 'checked_partitions=1/2' in result.output
            assert 'phase=rollback; status=completed' in result.output
            assert not list((root/'silver').glob('.b08-s-*'))
            assert not list((root/'silver').glob('.b08-b-*'))
            assert list((root/'silver').glob('.b08-f-*'))
        else:
            assert isinstance(result.exception, RuntimeError) and result.exception.__cause__ is read_error and str(restore_error) in str(result.exception)
            assert 'phase=rollback; status=failed' in result.output
            assert list((root/'silver').glob('.b08-b-*')) and list((root/'silver').glob('.b08-f-*'))
            assert not list((root/'silver').glob('.b08-s-*'))
        checks.append(scenario)

with tempfile.TemporaryDirectory(prefix='b08-log-cli-') as directory:
    root = pathlib.Path(directory).resolve()
    seed(root)
    previous = formal_files(root)
    runner = CliRunner()
    for write in (False, True):
        result = runner.invoke(after.main, ['--lake-root', str(root), '--confirm-full-quality', *(['--write'] if write else [])])
        assert result.exit_code == 0, (result.output, result.exception)
        output = result.output
        assert output.count('function=build_full_audit_staging; phase=build_audit; status=started') == 1
        assert output.count('function=build_full_audit_staging; phase=build_audit; status=completed') == 1
        assert 'function=main; phase=build_audit;' not in output
        assert 'function=main; phase=commit; status=started' not in output
        assert 'committed: table=fact_futures_missing_bar; function=main' not in output
        assert output.splitlines().count('='*88) == 4
        if write:
            assert output.count('function=commit_full_audit; phase=commit; status=completed') == 1
            assert 'phase=audit_state; status=completed' in output
            (SNAPSHOT/'example-function-run.log').write_text(output, encoding='utf-8')
        else:
            assert formal_files(root) == previous and 'persisted=true' not in output
            assert 'outcome=read_only' in output
    checks.append('cli_log_ownership_and_readonly')

print(json.dumps({'passed': checks, 'io_profiles': profiles, 'formal_lake_used': False}, ensure_ascii=False, indent=2))
