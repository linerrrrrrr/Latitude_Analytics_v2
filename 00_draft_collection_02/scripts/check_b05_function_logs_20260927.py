"""有界检查 b05 函数日志；模拟 API、临时 Parquet 与主动注入异常。"""

import contextlib
import importlib.util
import io
import json
import pathlib
import sys
import tempfile
from datetime import date
from unittest import mock

import pandas as pd
from click.testing import CliRunner

ROOT = pathlib.Path(__file__).resolve().parents[2]
OUT = pathlib.Path(sys.argv[1])
OUT.mkdir(parents=True, exist_ok=True)
sys.path.insert(0, str(ROOT))
spec = importlib.util.spec_from_file_location('daily_log_fixtures', ROOT / '00_draft_collection_02/tests/test_c05_daily_incremental_planning.py')
fixtures = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixtures)
m = fixtures.c05
checks = []


@contextlib.contextmanager
def capture(name):
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        yield output
    (OUT / f'{name}.log').write_text(output.getvalue(), encoding='utf-8')
    checks.append(name)


class FakeJQData:
    def __init__(self, *, empty=False, fail_at=None):
        self.empty = empty
        self.fail_at = fail_at
        self.calls = []
        self.error = OSError('injected API failure')

    def get_price(self, *args, **kwargs):
        self.calls.append('get_price')
        if self.fail_at == 'get_price':
            raise self.error
        if self.empty:
            return pd.DataFrame()
        return pd.DataFrame({
            'time': [date(2026, 8, 21)], 'code': ['BB2601.XDCE'],
            'open': [100.0], 'high': [101.0], 'low': [99.0], 'close': [100.5],
            'volume': [10.0], 'money': [1000.0], 'pre_close': [100.0],
        })

    def get_extras(self, name, *args, **kwargs):
        self.calls.append(name)
        if self.fail_at == name:
            raise self.error
        if self.empty:
            return pd.DataFrame()
        value = 100.2 if name == 'futures_sett_price' else 20.0
        return pd.DataFrame({'BB2601.XDCE': [value]}, index=pd.DatetimeIndex(['2026-08-21']))


pending_df = fixtures.planning_frame([fixtures.planning_row(
    exchange_code='XDCE', underlying_code='BB', is_fetch_required=True,
    is_fetch_completed=False, actual_bar_count=0, is_data_missing=False, missing_bar_count=0,
)])
# 独立 collect_batch 调用只提供函数原本需要的四个键，不依赖入口附加字段。
batch = {'contract_codes': ['BB2601.XDCE'], 'request_start': date(2026, 8, 1),
         'request_end': date(2026, 8, 21), 'pending_df': pending_df}

for empty in (False, True):
    jqdata = FakeJQData(empty=empty)
    with capture('collect_empty' if empty else 'collect_success') as log:
        fact_df, returned_count = m.collect_batch(jqdata, batch, fixtures.CHECKED_AT)
        assert len(fact_df) == 1
        assert bool(fact_df.iloc[0]['has_market_data']) != empty
        assert returned_count == (0 if empty else 3)
        assert jqdata.calls == ['get_price', 'futures_sett_price', 'futures_positions']
        text = log.getvalue()
        assert text.count('function=collect_batch; phase=api_request; status=started') == 3
        assert text.count('function=collect_batch; phase=api_request; status=completed') == 3
        assert text.count('api_success:') == 1 and 'phase=collect; status=completed' in text
        assert 'persisted=true' not in text

for api_index, api in enumerate(('get_price', 'futures_sett_price', 'futures_positions'), 1):
    jqdata = FakeJQData(fail_at=api)
    with capture(f'api_failure_{api}') as log:
        try:
            m.collect_batch(jqdata, batch, fixtures.CHECKED_AT)
        except OSError as error:
            assert error is jqdata.error
        else:
            raise AssertionError('API exception was swallowed')
        assert len(jqdata.calls) == api_index
        assert f'failed_phase={api};' in log.getvalue()
        assert 'api_success:' not in log.getvalue()

with capture('policy_and_request_plan') as log:
    dirty, pending, completed = m.plan_daily_policy(pending_df, None, None)
    batches = m.request_batches(pending)
    assert len(batches) == 1 and completed.empty and dirty.empty
    assert m.request_batches(pending.iloc[:0]) == []
    assert 'completion_source=is_fetch_completed' in log.getvalue()
    assert 'contracts=1/1' in log.getvalue()
    assert 'pending=0; batches=0' in log.getvalue()

calendar_df = fixtures.full_calendar_frame(exchange_code='XDCE', underlying_code='BB', is_fetch_required=True)
with tempfile.TemporaryDirectory(prefix='b05-function-logs-') as temporary:
    lake = pathlib.Path(temporary)
    fact_path = lake / 'silver' / m.TABLE_NAME
    calendar_path = lake / 'silver' / m.CALENDAR_TABLE_NAME
    with capture('absent_and_empty') as log:
        assert m.open_contract_dataset(fact_path, m.HIVE_PARTITIONING, m.FUTURES_DAILY_SCHEMA, 'fact', required=False) is None
        try:
            m.open_contract_dataset(calendar_path, m.CALENDAR_PARTITIONING, m.FUTURES_BAR_CALENDAR_SCHEMA, 'calendar', required=True)
        except FileNotFoundError:
            pass
        else:
            raise AssertionError('missing upstream was accepted')
        assert m.read_partition_leaf(fact_path, m.FUTURES_DAILY_SCHEMA, m.PARTITION_COLUMNS, m.HIVE_PARTITIONING, ('XDCE', 'BB', 2026, 8)).empty
        assert m.prepare_fact_leaf_specs(m.empty_pandas(m.FUTURES_DAILY_SCHEMA), lake) == []
        assert m.commit_validated_leaf_group([], lake) == 0
        assert 'outcome=absent_optional' in log.getvalue()
        assert 'phase=dataset_open; status=failed' in log.getvalue()
        assert 'outcome=absent_leaf; rows=0' in log.getvalue()
        assert 'persisted=true' not in log.getvalue()

    with capture('seed_and_direct_reads') as log:
        m.commit_validated_leaf_group(fixtures.leaf_specs(fixtures.fact_frame(), calendar_df), lake)
        dataset = m.open_contract_dataset(calendar_path, m.CALENDAR_PARTITIONING, m.FUTURES_BAR_CALENDAR_SCHEMA, 'calendar', required=True)
        assert dataset is not None
        leaf = m.read_partition_leaf(fact_path, m.FUTURES_DAILY_SCHEMA, m.PARTITION_COLUMNS, m.HIVE_PARTITIONING, ('XDCE', 'BB', 2026, 8))
        assert len(leaf) == 1
        assert 'outcome=ready; checked_fragments=2' in log.getvalue()
        assert 'outcome=read; rows=1' in log.getvalue()
        assert log.getvalue().count('persisted=true') == 1

    with capture('prepare_without_persistence') as log:
        fact_specs = m.prepare_fact_leaf_specs(fixtures.fact_frame(high=102.0), lake)
        calendar_spec = m.prepare_calendar_leaf_spec(dirty, fixtures.fact_frame(high=102.0), lake, ('1d', 'XDCE', 2026, 8), 'daily-log-check', fixtures.CHECKED_AT)
        assert len(fact_specs) == 1 and calendar_spec['frame'].iloc[0]['fetch_run_id'] == 'daily-log-check'
        assert 'outcome=prepared; updated_rows=1; persisted=false' in log.getvalue()
        assert 'persisted=true' not in log.getvalue()
        # 只准备内存结果时，真实临时磁盘上的旧完成批次保持不变。
        old_calendar = m.read_partition_leaf(calendar_path, m.FUTURES_BAR_CALENDAR_SCHEMA, m.CALENDAR_PARTITION_COLUMNS, m.CALENDAR_PARTITIONING, ('1d', 'XDCE', 2026, 8))
        assert old_calendar.iloc[0]['fetch_run_id'] == 'daily-existing'

    with capture('commit_completion_after_readback') as log:
        m.commit_validated_leaf_group([*fact_specs, calendar_spec], lake)
        text = log.getvalue()
        assert text.count('persisted=true') == 1
        assert text.rindex('phase=formal_readback; status=running') < text.index('persisted=true')
        assert text.count('partition_committed:') == 1

    with capture('cli_dispatch_and_summary') as log:
        # 临时日历沿用已完成状态，入口不应认证、请求或提交。
        with mock.patch('config.jqdata_connection.authenticate_jqdata', side_effect=AssertionError('unexpected API')):
            result = CliRunner().invoke(m.main, ['--lake-root', str(lake), '--write'])
        print(result.output)
        assert result.exit_code == 0, (result.output, result.exception)
        assert 'function=main; phase=run; status=completed; outcome=up_to_date' in result.output
        assert 'partition_start:' not in result.output

# 复用已有真实临时文件回滚测试，并额外检查失败时的日志归属。
for name, test, phase in (
    ('install_failure_rolls_back', fixtures.test_coordinated_leaf_install_rolls_back_both_tables, 'install'),
    ('formal_readback_failure_rolls_back', fixtures.test_formal_leaf_reread_failure_rolls_back_both_tables, 'formal_readback'),
):
    with capture(name) as log:
        test()
        text = log.getvalue()
        failure_part = text.split('phase=commit; status=failed;', 1)[1]
        assert f'failed_phase={phase};' in failure_part
        assert 'phase=rollback; status=completed;' in failure_part
        assert 'phase=commit_group; status=failed;' in failure_part
        assert 'persisted=true' not in failure_part
        assert 'partition_committed:' not in failure_part

(OUT / 'checks.json').write_text(json.dumps(checks, ensure_ascii=False, indent=2), encoding='utf-8')
print(f'function_log_checks_passed: {len(checks)}; no real API or formal lake access')
print('\n'.join(checks))
