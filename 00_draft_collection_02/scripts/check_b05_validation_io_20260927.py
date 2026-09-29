"""b05 局部收缩的差分检查：固定夹具、模拟来源、临时 Parquet，不访问正式湖。"""

import contextlib
import importlib.util
import io
import json
import pathlib
import sys
import tempfile
import uuid
from datetime import date
from types import SimpleNamespace
from unittest import mock

import pandas as pd
from click.testing import CliRunner

ROOT = pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT = pathlib.Path(sys.argv[1])
OUT = SNAPSHOT / 'validation-io-checks'
OUT.mkdir(parents=True, exist_ok=True)
sys.path.insert(0, str(ROOT))


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fixtures = load('b05_io_fixtures', ROOT / '00_draft_collection_02/tests/test_c05_daily_incremental_planning.py')
old = load('b05_before_reduction', SNAPSHOT / 'b05_futures_daily.py')
new = fixtures.c05
rows = []
for month, code, completed in [(5, 'BB2610.XDCE', False), (6, 'BB2611.XDCE', False), (7, 'BB2612.XDCE', True)]:
    row = fixtures.full_calendar_frame(exchange_code='XDCE', underlying_code='BB', is_fetch_required=False).iloc[0].to_dict()
    row.update(contract_code=code, trading_date=date(2026, month, 22), month=month)
    if not completed:
        row.update(is_fetch_required=True, is_fetch_completed=False, fetch_run_id=None,
                   fetch_completed_at=None, missing_checked_at=None, quality_status='pending',
                   quality_reason='等待日线采集。', quality_checked_at=None, daily_open=None)
    rows.append(row)
calendar_df = new.arrow_to_pandas(new.pa.Table.from_pylist(rows, schema=new.FUTURES_BAR_CALENDAR_SCHEMA), new.FUTURES_BAR_CALENDAR_SCHEMA)
pending_rows = rows[:2]


class FakeJQData:
    def __init__(self, *, quota_stop=False, duplicate_price=False, duplicate_extras=False):
        self.quota_stop = quota_stop
        self.duplicate_price = duplicate_price
        self.duplicate_extras = duplicate_extras
        self.calls = []

    def get_query_count(self):
        return {'spare': 0 if self.quota_stop else 10_000_000}

    def get_price(self, codes, *, start_date, end_date, **kwargs):
        self.calls.append('get_price')
        records = []
        for row in pending_rows:
            if row['contract_code'] in codes and start_date <= row['trading_date'] <= end_date:
                records.append({'code': row['contract_code'], 'time': row['trading_date'],
                                'open': 100.0, 'high': 101.0, 'low': 99.0, 'close': 100.5,
                                'volume': 10.0, 'money': 1000.0, 'pre_close': 100.0})
        if self.duplicate_price:
            records.append(records[0].copy())
        return pd.DataFrame(records)

    def get_extras(self, name, codes, *, start_date, end_date, **kwargs):
        self.calls.append(name)
        selected = [row for row in pending_rows if row['contract_code'] in codes and start_date <= row['trading_date'] <= end_date]
        dates = sorted({row['trading_date'] for row in selected})
        result = pd.DataFrame(index=pd.DatetimeIndex(dates), columns=codes, dtype='float64')
        for row in selected:
            result.loc[pd.Timestamp(row['trading_date']), row['contract_code']] = 100.2 if name == 'futures_sett_price' else 20.0
        if self.duplicate_extras:
            result = pd.concat([result, result.iloc[:1]])
        return result


def run_cli(module, mode):
    with tempfile.TemporaryDirectory(prefix='b05-io-diff-') as temporary:
        lake = pathlib.Path(temporary)
        calendar_path = lake / 'silver' / module.CALENDAR_TABLE_NAME
        module.ds.write_dataset(module.pandas_to_arrow(calendar_df, module.FUTURES_BAR_CALENDAR_SCHEMA),
                                calendar_path, format='parquet', partitioning=module.CALENDAR_PARTITIONING)
        module.pq.write_table(module.pa.Table.from_batches([], schema=module.parquet_file_schema(
            module.FUTURES_BAR_CALENDAR_SCHEMA, module.CALENDAR_PARTITION_COLUMNS)), calendar_path / 'schema.parquet')
        dataset_paths = []
        original_dataset = module.ds.dataset

        def dataset(source, *args, **kwargs):
            path = pathlib.Path(source)
            relative = path.relative_to(lake / 'silver').as_posix()
            dataset_paths.append(relative)
            return original_dataset(source, *args, **kwargs)

        provider = FakeJQData(quota_stop=mode == 'quota_stop')
        with (
            mock.patch('config.jqdata_connection.authenticate_jqdata', return_value=provider),
            mock.patch.object(module, 'datetime', SimpleNamespace(now=lambda *_: fixtures.CHECKED_AT)),
            mock.patch.object(module.uuid, 'uuid4', return_value=uuid.UUID(int=123)),
            mock.patch.object(module.ds, 'dataset', side_effect=dataset),
            mock.patch.object(module, 'validate_daily_frame', wraps=module.validate_daily_frame) as fact_validation,
            mock.patch.object(module, 'validate_calendar_state_frame', wraps=module.validate_calendar_state_frame) as calendar_validation,
            mock.patch.object(module, 'pandas_to_arrow', wraps=module.pandas_to_arrow) as to_arrow,
            mock.patch.object(module, 'arrow_to_pandas', wraps=module.arrow_to_pandas) as to_pandas,
            mock.patch.object(module, 'parquet_file_schema', wraps=module.parquet_file_schema) as file_schema,
            mock.patch.object(module, 'timedelta', wraps=module.timedelta) as lookback,
        ):
            args = ['--lake-root', str(lake), '--quota-reserve', '0']
            if mode != 'readonly':
                args.append('--write')
            result = CliRunner().invoke(module.main, args)
            assert result.exit_code == 0, (module.__name__, mode, result.output, result.exception)
            counts = dict(fact_validations=fact_validation.call_count,
                          calendar_validations=calendar_validation.call_count,
                          pandas_to_arrow=to_arrow.call_count, arrow_to_pandas=to_pandas.call_count,
                          file_schema_builds=file_schema.call_count, lookback_builds=lookback.call_count)
        (OUT / f'{module.__name__}-{mode}.log').write_text(result.output, encoding='utf-8')
        root_opens = [name for name in dataset_paths if '/' not in name]
        assert root_opens == [module.CALENDAR_TABLE_NAME], dataset_paths
        outputs = {}
        for name, schema, partitioning, key in [
            (module.TABLE_NAME, module.FUTURES_DAILY_SCHEMA, module.HIVE_PARTITIONING, module.PRIMARY_KEY),
            (module.CALENDAR_TABLE_NAME, module.FUTURES_BAR_CALENDAR_SCHEMA, module.CALENDAR_PARTITIONING, module.CALENDAR_PRIMARY_KEY),
        ]:
            table_path = lake / 'silver' / name
            if table_path.exists():
                table = original_dataset(table_path, format='parquet', partitioning=partitioning).to_table(columns=schema.names)
                outputs[name] = module.arrow_to_pandas(table, schema).sort_values(key).reset_index(drop=True)
            else:
                outputs[name] = module.empty_pandas(schema)
        return outputs, counts, dataset_paths, provider.calls


report = []
for mode in ['write', 'readonly', 'quota_stop']:
    before_output, before_count, before_paths, before_api = run_cli(old, mode)
    after_output, after_count, after_paths, after_api = run_cli(new, mode)
    for name in before_output:
        pd.testing.assert_frame_equal(before_output[name], after_output[name], check_exact=True)
    assert before_paths == after_paths
    assert before_api == after_api
    assert after_count['calendar_validations'] == before_count['calendar_validations']
    reduction = 0 if mode == 'quota_stop' else 1
    for name in ['fact_validations', 'pandas_to_arrow', 'arrow_to_pandas']:
        assert before_count[name] - after_count[name] == reduction, (mode, name, before_count, after_count)
    assert after_count['lookback_builds'] == 1
    if mode == 'write':
        assert len(after_output[new.TABLE_NAME]) == 2
        assert after_count['fact_validations'] == 3  # 一批来源 + 两个 dirty 完整事实叶。
        assert after_count['calendar_validations'] == 3  # 包含一个只改政策的叶。
        assert after_count['file_schema_builds'] < before_count['file_schema_builds']
        assert after_output[new.CALENDAR_TABLE_NAME]['is_fetch_completed'].all()
    if mode == 'quota_stop':
        assert after_output[new.TABLE_NAME].empty
        assert before_api == []
        assert after_output[new.CALENDAR_TABLE_NAME]['is_fetch_completed'].tolist() == [False, False, True]
    report.append({'scenario': mode, 'before': before_count, 'after': after_count,
                   'dataset_open_count': len(after_paths), 'table_root_opens': 1, 'api_calls': len(after_api),
                   'output_exactly_equal': True})

with contextlib.redirect_stdout(io.StringIO()):
    _, pending_df, _ = new.plan_daily_policy(calendar_df, None, None)
    before_batches = old.request_batches(pending_df)
    after_batches = new.request_batches(pending_df)
assert len(before_batches) == len(after_batches)
for left, right in zip(before_batches, after_batches, strict=True):
    assert {k: v for k, v in left.items() if k != 'pending_df'} == {k: v for k, v in right.items() if k != 'pending_df'}
    pd.testing.assert_frame_equal(left['pending_df'], right['pending_df'])

# 分批阈值强制触发拆分，证明每个 pending 键只归属一个批次。
with contextlib.redirect_stdout(io.StringIO()), mock.patch.object(new, 'MAX_CONTRACTS_PER_REQUEST', 1):
    split_batches = new.request_batches(pending_df)
assert len(split_batches) == 2
split_keys = pd.concat([batch['pending_df'] for batch in split_batches])[new.PRIMARY_KEY]
assert not split_keys.duplicated().any() and len(split_keys) == len(pending_df)

batch = after_batches[0]
for field in ['duplicate_price', 'duplicate_extras']:
    messages = []
    for module in [old, new]:
        with contextlib.redirect_stdout(io.StringIO()):
            try:
                module.collect_batch(FakeJQData(**{field: True}), batch, fixtures.CHECKED_AT)
            except ValueError as error:
                messages.append(str(error))
            else:
                raise AssertionError(f'{field} was accepted')
    assert messages[0] == messages[1] and '重复' in messages[0], messages

# 单合约索引时间与多合约显式时间两种来源格式仍保持原结果。
raw = FakeJQData().get_price(batch['contract_codes'], start_date=batch['request_start'], end_date=batch['request_end'])
for response, codes in [(raw, batch['contract_codes']), (raw.iloc[:1].drop(columns='code').set_index('time'), batch['contract_codes'][:1])]:
    pd.testing.assert_frame_equal(old.normalize_price_response(response, codes), new.normalize_price_response(response, codes))
fact_df = fixtures.fact_frame()
original_fact_df = fact_df.copy(deep=True)
pd.testing.assert_frame_equal(old.validate_daily_frame(fact_df, ''), new.validate_daily_frame(fact_df, ''), check_exact=True)
pd.testing.assert_frame_equal(fact_df, original_fact_df, check_exact=True)

(OUT / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
print(json.dumps(report, ensure_ascii=False, indent=2))
print('Also passed: batch range equivalence/disjointness, duplicate-source rejection, response time formats, validator input unchanged.')
