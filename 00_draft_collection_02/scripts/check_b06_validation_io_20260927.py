"""b06 局部收缩差分：模拟 API 与临时 Parquet，不访问正式湖。"""

import contextlib
import importlib.util
import io
import json
import pathlib
import sys
import tempfile
from datetime import datetime
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import pyarrow as pa
from click.testing import CliRunner

ROOT = pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT = pathlib.Path(sys.argv[1])
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a01_Futures_Market_Data/b06_futures_minute.py')
sys.path.insert(0, str(ROOT / '00_draft_collection_02/tests'))
import test_b01_c06_incremental_daily as fixture


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


before = load('b06_before_validation_io', SNAPSHOT / RELATIVE)
after = fixture.minute
checks = []


def calendar_row(code, day, *, completed=False, required=True, expected=2):
    start = pd.Timestamp(f'{day} 09:00', tz='Asia/Shanghai')
    row = fixture.calendar_row(
        contract_code=code, completed=completed, is_fetch_required=required,
        session_start_at=start, session_end_at=start + pd.Timedelta(minutes=expected),
        expected_bar_count=expected,
    )
    row.update(trading_date=start.date(), exchange_code=code.split('.')[1],
               underlying_code=''.join(c for c in code.split('.')[0] if c.isalpha()),
               year=start.year, month=start.month)
    return row


pending_rows = [
    calendar_row('RB2609.XSGE', '2026-08-03'),
    calendar_row('RB2610.XSGE', '2026-08-03'),
    calendar_row('CU2609.XSGE', '2026-08-03'),
    calendar_row('RB2610.XSGE', '2026-09-01'),
]
calendar_rows = pending_rows + [
    calendar_row('RB2611.XSGE', '2026-08-03', completed=True),
    calendar_row('A2609.XDCE', '2026-08-03', completed=True),
]


class FakeJQData:
    def __init__(self, mode='normal'):
        self.mode = mode
        self.calls = []
        self.quota_calls = 0

    def get_query_count(self):
        self.quota_calls += 1
        if self.mode == 'quota_zero' or (self.mode == 'quota_after_first' and self.quota_calls > 1):
            return {'spare': 0}
        return {'spare': 10_000_000}

    def get_price(self, code, *, start_date, end_date, **kwargs):
        self.calls.append((code, str(start_date), str(end_date), kwargs))
        if self.mode == 'api_error':
            raise OSError('fake API error')
        if self.mode == 'none':
            return None
        records = []
        # RB2609 为成功空响应；RB2610 为有限 OHLC 异常且部分缺失。
        count = 0 if code == 'RB2609.XSGE' else (1 if code == 'RB2610.XSGE' else 2)
        if self.mode == 'empty':
            count = 0
        for number in range(1, count + 1):
            records.append(dict(time=start_date + pd.Timedelta(minutes=number), code=code,
                                open=100.0, high=99.0 if code == 'RB2610.XSGE' else 101.0,
                                low=98.0, close=99.0, volume=1.0, money=100.0, open_interest=2.0))
        return pd.DataFrame(records, columns=['time', 'code', *after.PRICE_FIELDS])


class FixedDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return fixture.CHECKED_AT


def run_cli(module, mode, write=True, explicit=False):
    with tempfile.TemporaryDirectory(prefix='b06-io-diff-') as temporary:
        lake = pathlib.Path(temporary)
        source_rows = calendar_rows
        if mode == 'clean':
            source_rows = [calendar_row('RB2611.XSGE', '2026-08-03', completed=True)]
        elif mode == 'policy_only':
            source_rows = [calendar_rows[-1]]
        calendar_df = pd.DataFrame(source_rows).loc[:, module.FUTURES_BAR_CALENDAR_SCHEMA.names]
        fixture.write_partitioned(calendar_df, lake/'silver'/module.CALENDAR_TABLE_NAME,
                                  module.FUTURES_BAR_CALENDAR_SCHEMA, module.CALENDAR_PARTITION_COLUMNS)
        old_rows = []
        for row in source_rows:
            for number in range(1, row['expected_bar_count'] + 1):
                fact = fixture.minute_row(contract_code=row['contract_code'],
                                          bar_at=row['session_start_at'] + pd.Timedelta(minutes=number))
                fact.update({key: row[key] for key in ['trading_date','exchange_code','underlying_code','year','month']})
                old_rows.append(fact)
        fixture.write_partitioned(pd.DataFrame(old_rows).loc[:, module.FUTURES_MINUTE_SCHEMA.names],
                                  lake/'silver'/module.TABLE_NAME, module.FUTURES_MINUTE_SCHEMA, module.PARTITION_COLUMNS)

        dataset_opens, scans, validations = [], [], []
        original_dataset = module.ds.dataset

        class TracedDataset:
            def __init__(self, dataset, path):
                self.dataset, self.path = dataset, path

            def __getattr__(self, name):
                return getattr(self.dataset, name)

            def to_table(self, *args, **kwargs):
                scans.append((self.path, kwargs.get('columns'), str(kwargs.get('filter'))))
                return self.dataset.to_table(*args, **kwargs)

        def traced_dataset(path, *args, **kwargs):
            relative = str(pathlib.Path(path).relative_to(lake))
            dataset_opens.append(relative)
            return TracedDataset(original_dataset(path, *args, **kwargs), relative)

        real_minute_validator = module.validate_minute_frame
        real_calendar_validator = module.validate_calendar_state_frame

        def minute_validator(frame, context, *args):
            validations.append(('minute', context, len(frame)))
            return real_minute_validator(frame, context, *args)

        def calendar_validator(frame, context):
            validations.append(('calendar', context, len(frame)))
            return real_calendar_validator(frame, context)

        fake = FakeJQData(mode)
        generated_ids = iter(SimpleNamespace(hex=f'{i:032x}') for i in range(1,100))
        args = ['--lake-root', str(lake), '--quota-reserve', '0']
        if write:
            args.append('--write')
        if explicit:
            args += ['--start-date', '2026-08-03', '--end-date', '2026-08-03']
        with mock.patch('config.jqdata_connection.authenticate_jqdata', return_value=fake) as authenticate, \
             mock.patch.object(module, 'datetime', FixedDatetime), \
             mock.patch.object(module, 'uuid', SimpleNamespace(uuid4=lambda: next(generated_ids))), \
             mock.patch.object(module.ds, 'dataset', side_effect=traced_dataset), \
             mock.patch.object(module, 'validate_minute_frame', side_effect=minute_validator), \
             mock.patch.object(module, 'validate_calendar_state_frame', side_effect=calendar_validator):
            result = CliRunner().invoke(module.main, args)

        expected_failure = mode in ('api_error', 'none')
        assert (result.exit_code != 0) == expected_failure, (mode, result.output, result.exception)
        for table_name in (module.CALENDAR_TABLE_NAME, module.TABLE_NAME):
            assert dataset_opens.count(str(pathlib.Path('silver')/table_name)) == 1, dataset_opens
        for path, columns, expression in scans:
            if path == str(pathlib.Path('silver')/module.CALENDAR_TABLE_NAME):
                assert columns == module.MINUTE_PLANNING_COLUMNS
                assert all(name in expression for name in module.CALENDAR_PARTITION_COLUMNS)
            else:
                assert 'year=' in path and 'month=' in path, (path, columns)
        tables = {}
        for table_name, schema, partitioning, keys in (
            (module.TABLE_NAME, module.FUTURES_MINUTE_SCHEMA, module.HIVE_PARTITIONING, module.PRIMARY_KEY),
            (module.CALENDAR_TABLE_NAME, module.FUTURES_BAR_CALENDAR_SCHEMA, module.CALENDAR_PARTITIONING, module.CALENDAR_PRIMARY_KEY),
        ):
            dataset = original_dataset(lake/'silver'/table_name, format='parquet', partitioning=partitioning)
            tables[table_name] = module.arrow_to_pandas(dataset.to_table(columns=schema.names), schema).sort_values(keys).reset_index(drop=True)
        return dict(tables=tables, calls=fake.calls, quota=fake.quota_calls,
                    exception=type(result.exception).__name__ if result.exception else None,
                    dataset_opens=dataset_opens, scans=scans, validations=validations,
                    authenticated=authenticate.call_count, output=result.output)


for mode, write, explicit in [
    ('normal',False,False), ('normal',True,False), ('normal',True,True),
    ('empty',True,False), ('quota_zero',True,False), ('quota_after_first',True,False),
    ('api_error',True,False), ('none',True,False), ('clean',True,False), ('policy_only',True,False),
]:
    old = run_cli(before, mode, write, explicit)
    new = run_cli(after, mode, write, explicit)
    for name in old['tables']:
        pd.testing.assert_frame_equal(old['tables'][name], new['tables'][name])
    for name in ('calls','quota','exception','dataset_opens','scans','validations','authenticated'):
        assert old[name] == new[name], (mode, name, old[name], new[name])
    scenario = f'cli_{mode}_write_{write}_explicit_{explicit}'
    (SNAPSHOT/f'{scenario}.log').write_text(new['output'], encoding='utf-8')
    checks.append(dict(scenario=scenario, api_calls=len(new['calls']),
                       dataset_opens=new['dataset_opens'], scans=new['scans'], validations=new['validations']))

# 一个输入覆盖多个日历叶，并使用非连续索引，证明摘要不依赖整表 merge 或重算 OHLC。
sessions = after.arrow_to_pandas(after.pandas_to_arrow(
    pd.DataFrame(pending_rows).loc[:,after.FUTURES_BAR_CALENDAR_SCHEMA.names],
    after.FUTURES_BAR_CALENDAR_SCHEMA), after.FUTURES_BAR_CALENDAR_SCHEMA)
sessions.index = [7,20,35,50]
with contextlib.redirect_stdout(io.StringIO()):
    facts, _, invalid_counts = after.collect_partition(FakeJQData(), sessions, fixture.CHECKED_AT)
    with mock.patch.object(before, 'invalid_ohlc_mask', wraps=before.invalid_ohlc_mask) as old_mask:
        old_updates = before.build_calendar_completion_updates(sessions, facts, invalid_counts)
    with mock.patch.object(after, 'invalid_ohlc_mask', side_effect=AssertionError('redundant OHLC scan')), \
         mock.patch.object(pd.DataFrame, 'merge', side_effect=AssertionError('whole summary merge')):
        new_updates = after.build_calendar_completion_updates(sessions, facts, invalid_counts)
assert old_updates.keys() == new_updates.keys()
for key in old_updates:
    pd.testing.assert_frame_equal(old_updates[key], new_updates[key])
checks.append(dict(scenario='summary_multiple_leaves', partitions=len(new_updates),
                   old_ohlc_scans=old_mask.call_count, new_ohlc_scans=0, new_summary_merges=0))

# 来源异常属于真实接口边界；删减不得使这些异常进入事实输出。
for mode in ('nan','inf','negative','duplicate','foreign_contract','off_grid','sub_microsecond','null'):
    outcomes = []
    for module in (before, after):
        raw = pd.DataFrame({field:[100.0] for field in module.PRICE_FIELDS},
                           index=pd.DatetimeIndex(['2026-08-03 09:01:00'], dtype='datetime64[ns]',name='time'))
        if mode in ('nan','inf'):
            raw.loc[:, 'open'] = float(mode)
        elif mode == 'negative':
            raw.loc[:, 'volume'] = -1.0
        elif mode == 'duplicate':
            raw = pd.concat([raw,raw])
        elif mode == 'foreign_contract':
            raw['code'] = 'CU2609.XSGE'
        elif mode == 'off_grid':
            raw.index = pd.DatetimeIndex(['2026-08-03 09:00:30'],name='time')
        elif mode == 'sub_microsecond':
            raw.index = pd.DatetimeIndex(['2026-08-03 09:01:00.000000001'],name='time')
        elif mode == 'null':
            raw['open'] = pd.Series([None],index=raw.index,dtype=object)
        api = mock.Mock()
        api.get_price.return_value = raw
        with contextlib.redirect_stdout(io.StringIO()):
            try:
                actual, _, _ = module.collect_partition(api,sessions.iloc[:1],fixture.CHECKED_AT)
            except Exception as error:
                outcomes.append((type(error).__name__, str(error)))
            else:
                assert mode == 'null'
                outcomes.append(actual)
        api.get_price.assert_called_once()
    if mode == 'null':
        pd.testing.assert_frame_equal(*outcomes)
        assert outcomes[0]['open'].isna().all()
    else:
        assert outcomes[0] == outcomes[1] and not isinstance(outcomes[0],pd.DataFrame), (mode,outcomes)
    checks.append(dict(scenario=f'source_{mode}', matched=True))

(SNAPSHOT/'validation-io-checks.json').write_text(json.dumps(checks,ensure_ascii=False,indent=2),encoding='utf-8')
print(f'passed={len(checks)}; report={SNAPSHOT / "validation-io-checks.json"}')
