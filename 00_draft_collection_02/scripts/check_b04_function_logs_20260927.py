"""b04 函数日志的隔离验证；模拟业务夹具和主动注入失败，不访问正式湖。"""

import contextlib
import hashlib
import importlib.util
import io
import json
import pathlib
import sys
import tempfile
from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock, patch

from click.testing import CliRunner

ROOT = pathlib.Path(__file__).resolve().parents[2]
OUT = pathlib.Path(sys.argv[1])
OUT.mkdir(parents=True, exist_ok=True)


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fixtures = load('b04_log_fixtures', ROOT / '00_draft_collection_02/tests/test_futures_bar_calendar.py')
fixtures.FuturesBarCalendarTests.setUpClass()
fixture = fixtures.FuturesBarCalendarTests()
fixture.setUp()
m = fixture.module
checks = []
contract_df = fixture.contract_frame(session_count=2)


def save(name, output):
    (OUT / f'{name}.log').write_text(output, encoding='utf-8')
    checks.append(name)


def hashes(path):
    return {str(p.relative_to(path)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in path.rglob('*.parquet')}


with tempfile.TemporaryDirectory(prefix='b04-function-logs-') as temporary:
    lake = pathlib.Path(temporary)
    upstream = lake / 'silver' / m.UPSTREAM_TABLE_NAME
    target = lake / 'silver' / m.TABLE_NAME
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        assert m.open_contract_dataset(target, m.HIVE_PARTITIONING, m.FUTURES_BAR_CALENDAR_SCHEMA, m.PARTITION_COLUMNS, 'target', required=False) is None
        try:
            m.open_contract_dataset(upstream, m.UPSTREAM_PARTITIONING, m.FUTURES_CONTRACT_CALENDAR_SCHEMA, m.UPSTREAM_PARTITION_COLUMNS, 'upstream', required=True)
        except FileNotFoundError:
            pass
        else:
            raise AssertionError('missing required upstream was accepted')
        assert m.partition_keys_from_files(target, m.PARTITION_COLUMNS) == set()
        assert m.read_existing_partition_leaf(target, ('1d', 'XSGE', 2024, 1)).empty
    assert 'outcome=absent_optional' in output.getvalue()
    assert 'phase=dataset_open; status=failed' in output.getvalue()
    assert 'outcome=absent_leaf; rows=0' in output.getvalue()
    save('missing_optional_and_required', output.getvalue())

    m.ds.write_dataset(m.pandas_to_arrow(contract_df, m.FUTURES_CONTRACT_CALENDAR_SCHEMA), upstream,
                       format='parquet', partitioning=m.UPSTREAM_PARTITIONING)
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        dataset = m.open_contract_dataset(upstream, m.UPSTREAM_PARTITIONING, m.FUTURES_CONTRACT_CALENDAR_SCHEMA, m.UPSTREAM_PARTITION_COLUMNS, 'upstream', required=True)
        assert m.partition_keys_from_files(upstream, m.UPSTREAM_PARTITION_COLUMNS) == {('XSGE', 2024, 1)}
        read_df = m.read_partition(dataset, m.UPSTREAM_STRUCTURE_SCHEMA, m.UPSTREAM_PARTITION_COLUMNS, ('XSGE', 2024, 1))
        empty_df = m.read_partition(dataset, m.UPSTREAM_STRUCTURE_SCHEMA, m.UPSTREAM_PARTITION_COLUMNS, ('XSGE', 2024, 1), date(2024, 1, 4), date(2024, 1, 4))
    assert len(read_df) == 4 and empty_df.empty
    assert 'outcome=ready; checked_fragments=1' in output.getvalue()
    assert output.getvalue().count('phase=read; status=completed') == 2
    save('dataset_discovery_and_projected_reads', output.getvalue())

    error = OSError('injected read failure')
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        try:
            m.read_partition(SimpleNamespace(to_table=Mock(side_effect=error)), m.STRUCTURAL_SCHEMA, m.PARTITION_COLUMNS, ('1m', 'XSGE', 2024, 1))
        except OSError as actual:
            assert actual is error
        else:
            raise AssertionError('read exception was swallowed')
    assert 'failed_phase=scan' in output.getvalue()
    assert 'phase=read; status=completed' not in output.getvalue()
    save('read_failure', output.getvalue())

    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        structures = m.build_structural_partitions(read_df)
        fresh = m.build_fresh_partitions(contract_df, fixture.updated_at)
        empty_structures = m.build_structural_partitions(read_df.iloc[:0])
    assert len(structures['1d']) == 2 and len(structures['1m']) == 4
    assert all(frame.empty for frame in empty_structures.values())
    assert output.getvalue().count('phase=build_structure; status=completed') == 3
    assert output.getvalue().count('phase=build_fresh; status=completed') == 1
    assert 'daily_rows=0; minute_rows=0' in output.getvalue()
    save('direct_generation_and_empty_input', output.getvalue())

    error = ValueError('injected generated-structure rejection')
    output = io.StringIO()
    with contextlib.redirect_stdout(output), patch.object(m, 'validate_structural_frame', side_effect=error):
        try:
            m.build_structural_partitions(read_df)
        except ValueError as actual:
            assert actual is error
        else:
            raise AssertionError('generation exception was swallowed')
    assert 'failed_phase=minute_structure' in output.getvalue()
    assert 'phase=build_structure; status=completed' not in output.getvalue()
    save('generation_failure', output.getvalue())

    key = ('1d', 'XSGE', 2024, 1)
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        assert m.commit_partition(fresh['1d'], lake, key) == 2
        assert len(m.read_existing_partition_leaf(target, key)) == 2
    assert output.getvalue().count('partition_start:') == 1
    assert output.getvalue().count('partition_committed:') == 1
    assert 'phase=schema_marker; status=started' in output.getvalue()
    save('direct_commit_and_leaf_read', output.getvalue())

    before = hashes(target)
    original_read = m.read_partition
    original_echo = m.click.echo
    error = TypeError('injected formal readback failure')

    def fail_formal(dataset, *args, **kwargs):
        if any(pathlib.Path(path).resolve().is_relative_to(target.resolve()) for path in dataset.files):
            raise error
        return original_read(dataset, *args, **kwargs)

    def verify_failure_after_recovery(message, *args, **kwargs):
        if 'function=commit_partition; phase=commit; status=failed' in str(message):
            assert hashes(target) == before
            assert not list((lake / 'silver').glob('.c04s-*'))
        return original_echo(message, *args, **kwargs)

    changed = fresh['1d'].copy()
    changed['quality_reason'] = '本次替换夹具；恢复后旧值必须完整保留。'
    output = io.StringIO()
    with contextlib.redirect_stdout(output), patch.object(m, 'read_partition', side_effect=fail_formal), patch.object(m.click, 'echo', side_effect=verify_failure_after_recovery):
        try:
            m.commit_partition(changed, lake, key)
        except RuntimeError as actual:
            assert actual.__cause__ is error
        else:
            raise AssertionError('formal failure was swallowed')
    assert hashes(target) == before
    assert 'failed_phase=formal_readback' in output.getvalue()
    assert 'phase=rollback; status=completed' in output.getvalue()
    assert 'partition_committed:' not in output.getvalue()
    save('commit_failure_logged_after_recovery', output.getvalue())

    # 目标只有 1d，自动模式将补齐缺少的 1m；单叶事件只来自提交函数。
    result = CliRunner().invoke(m.main, ['--lake-root', str(lake), '--write'])
    assert result.exit_code == 0, result.output
    assert result.output.count('partition_start:') == result.output.count('partition_committed:') == 1
    assert 'phase=commit_batch; status=running' in result.output
    assert result.output.count('phase=watermark; status=completed') == 1
    assert 'watermark=2024-01-03' in result.output
    save('cli_commit_ownership_and_watermark', result.output)

    result = CliRunner().invoke(m.main, ['--lake-root', str(lake)])
    assert result.exit_code == 0, result.output
    assert 'watermark_groups=2' in result.output
    assert 'phase=run; status=completed' in result.output and 'outcome=up_to_date' in result.output
    assert 'partition_committed:' not in result.output
    save('watermark_two_frequencies_no_changes', result.output)

    for label, options in [('full', ['--full']), ('explicit', ['--start-date', '2024-01-03', '--end-date', '2024-01-03'])]:
        result = CliRunner().invoke(m.main, ['--lake-root', str(lake), *options])
        assert result.exit_code == 0, result.output
        assert 'phase=watermark; status=skipped' in result.output
        assert 'phase=watermark; status=completed' not in result.output
        save(f'{label}_skips_automatic_watermark', result.output)

    with patch.object(m, 'read_partition', side_effect=OSError('injected watermark read failure')):
        result = CliRunner().invoke(m.main, ['--lake-root', str(lake)])
    assert result.exit_code != 0
    assert 'phase=watermark; status=failed' in result.output
    assert 'phase=watermark; status=completed' not in result.output
    assert 'phase=run; status=completed' not in result.output
    save('watermark_failure', result.output)

summary = {'checks': checks, 'real_api_calls': 0, 'formal_lake_writes': 0}
(OUT / 'results.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
print(json.dumps(summary, ensure_ascii=False))
