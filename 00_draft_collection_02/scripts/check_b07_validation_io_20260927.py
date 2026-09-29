"""b07 7—9 项差分：可信正式类型输入、分区 I/O 和转换计数；不触及正式湖。"""
from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import pathlib
import runpy
import sys
import tempfile
from datetime import datetime
from unittest import mock

import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT = pathlib.Path(sys.argv[1])
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a01_Futures_Market_Data/b07_suspected_session_reconciliation.py')
spec = importlib.util.spec_from_file_location('b07_before_validation_io', SNAPSHOT / RELATIVE)
before = importlib.util.module_from_spec(spec)
spec.loader.exec_module(before)
with contextlib.redirect_stdout(io.StringIO()):
    fixture = runpy.run_path(str(ROOT / '00_draft_collection_02/scripts/check_b07_docs_logs_20260927.py'))
after = fixture['b07']
data_fixtures = fixture['fixtures']
schemas = [spec[1] for spec in fixture['tables']]
original_frames = [pd.DataFrame(rows).loc[:, schema.names] for rows, schema, _ in fixture['tables']]


def typed_frames(frames):
    return [after.arrow_to_pandas(after.pandas_to_arrow(df.loc[:, schema.names], schema), schema) for df, schema in zip(frames, schemas)]


def reconcile(module, frames, keys=None):
    if keys is None:
        keys = set(frames[0].loc[frames[0]['schedule_status'].eq('suspected_closed'), module.CALENDAR_PRIMARY_KEY].itertuples(index=False, name=None))
    with contextlib.redirect_stdout(io.StringIO()):
        return module.reconcile_partition(*[frames[0], keys, *frames[1:], data_fixtures.CHECKED_AT])


scenarios = []
for scenario in ('match', 'volume_mismatch', 'daily_absent', 'contract_absent', 'tick_null', 'daily_ohlc_warning', 'minute_ohlc_warning', 'daily_money_missing', 'other_incomplete', 'two_suspected'):
    frames = typed_frames(original_frames)
    calendar, contract, daily, minute = frames
    if scenario == 'volume_mismatch':
        daily.loc[0, 'volume'] += 1.0
    elif scenario == 'daily_absent':
        frames[2] = daily.iloc[:0]
    elif scenario == 'contract_absent':
        frames[1] = contract.iloc[:0]
    elif scenario == 'tick_null':
        contract.loc[:, 'tick_size'] = None
    elif scenario == 'daily_ohlc_warning':
        daily.loc[0, 'high'] = 99.0
    elif scenario == 'minute_ohlc_warning':
        minute.loc[0, 'high'] = 99.0
    elif scenario == 'daily_money_missing':
        daily.loc[0, 'money'] = None
    elif scenario == 'other_incomplete':
        calendar.loc[1, 'is_fetch_completed'] = False
        calendar.loc[1, 'actual_bar_count'] = 0
        calendar.loc[1, 'is_data_missing'] = True
        calendar.loc[1, 'missing_bar_count'] = 1
        calendar.loc[1, 'fetch_run_id'] = None
        calendar.loc[1, 'fetch_completed_at'] = None
        frames[3] = minute.iloc[:0]
    elif scenario == 'two_suspected':
        calendar.loc[1, 'schedule_status'] = 'suspected_closed'
        calendar.loc[1, 'actual_bar_count'] = 0
        calendar.loc[1, 'is_data_missing'] = True
        calendar.loc[1, 'missing_bar_count'] = 1
        frames[3] = minute.iloc[:0]
    frames = typed_frames(frames)
    old, new = reconcile(before, frames), reconcile(after, frames)
    for old_df, new_df in zip(old[:2], new[:2]):
        pd.testing.assert_frame_equal(old_df, new_df)
    assert old[2] == new[2]
    scenarios.append(scenario)

# 同叶包含多个合约日、未选中行以及 1d 行，验证位置索引不把局部位置误作全表位置。
multi_frames = [[] for _ in schemas]
for code, days in [('RB2609.XSGE', 0), ('RB2610.XSGE', 1), ('RB2611.XSGE', 2)]:
    for index, frame in enumerate(original_frames):
        expanded = frame.copy()
        expanded.loc[:, 'contract_code'] = code
        expanded.loc[:, 'trading_date'] = expanded['trading_date'].map(lambda day: (pd.Timestamp(day) + pd.Timedelta(days=days)).date())
        for column in ('session_start_at', 'session_end_at', 'bar_at'):
            if column in expanded:
                expanded.loc[:, column] += pd.Timedelta(days=days)
        multi_frames[index].append(expanded)
multi_frames = [pd.concat(pieces, ignore_index=True) for pieces in multi_frames]
unselected_daily = data_fixtures.calendar_row(bar_frequency='1d', quality_status='passed', quality_reason='unchanged', evidence_source='fact_futures_daily:formal_nonempty')
multi_frames[0] = pd.concat([pd.DataFrame([unselected_daily]), multi_frames[0]], ignore_index=True)
multi_frames = typed_frames(multi_frames)
keys = set(multi_frames[0].loc[multi_frames[0]['schedule_status'].eq('suspected_closed'), after.CALENDAR_PRIMARY_KEY].itertuples(index=False, name=None))
keys = {key for key in keys if 'RB2611.XSGE' not in key}
old, new = reconcile(before, multi_frames, keys), reconcile(after, multi_frames, keys)
for a, b in zip(old[:2], new[:2]):
    pd.testing.assert_frame_equal(a, b)
assert len(new[1]) == 2 and len(new[0]) == 7
scenarios.append('multiple_contract_days_and_unselected_rows')

conversion_counts = {}
for name, module in [('before', before), ('after', after)]:
    with tempfile.TemporaryDirectory(prefix='b07-conversion-count-') as directory:
        frames = {('1m', 'XSGE', 2026, 8): typed_frames(original_frames)[0]}
        with mock.patch.object(module, 'pandas_to_arrow', wraps=module.pandas_to_arrow) as p2a, mock.patch.object(module, 'arrow_to_pandas', wraps=module.arrow_to_pandas) as a2p, mock.patch.object(module, 'validate_calendar_frame', wraps=module.validate_calendar_frame) as validate, contextlib.redirect_stdout(io.StringIO()):
            assert module.commit_calendar_partitions(frames, pathlib.Path(directory)) == 2
        conversion_counts[name] = {'pandas_to_arrow': p2a.call_count, 'arrow_to_pandas': a2p.call_count, 'business_validator': validate.call_count}
assert conversion_counts['before'] == {'pandas_to_arrow': 3, 'arrow_to_pandas': 1, 'business_validator': 1}
assert conversion_counts['after'] == {'pandas_to_arrow': 2, 'arrow_to_pandas': 0, 'business_validator': 1}


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return data_fixtures.CHECKED_AT


outputs = {}
io_evidence = {}
for name, module in [('before', before), ('after', after)]:
    with tempfile.TemporaryDirectory(prefix='b07-multipart-') as directory:
        lake_root = pathlib.Path(directory).resolve()
        silver = lake_root / 'silver'
        for rows, schema, partitions in fixture['tables']:
            frames = []
            for month in (8, 9):
                frame = pd.DataFrame(rows).loc[:, schema.names]
                delta = pd.DateOffset(months=month - 8)
                frame.loc[:, 'month'] = month
                frame.loc[:, 'trading_date'] = frame['trading_date'].map(lambda day: (pd.Timestamp(day) + delta).date())
                for column in ('session_start_at', 'session_end_at', 'bar_at'):
                    if column in frame:
                        frame.loc[:, column] += delta
                frames.append(frame)
            data_fixtures.write_partitioned(pd.concat(frames, ignore_index=True), silver / schema.metadata[b'table_name'].decode(), schema, partitions)
        original_dataset = module.ds.dataset
        reads, opens = [], []

        class DatasetProbe:
            def __init__(self, path, dataset):
                self.path, self.dataset = pathlib.Path(path).resolve(), dataset

            def __getattr__(self, key):
                return getattr(self.dataset, key)

            def to_table(self, *args, **kwargs):
                reads.append((self.path, tuple(kwargs.get('columns', ())), str(kwargs.get('filter'))))
                return self.dataset.to_table(*args, **kwargs)

        def tracked_dataset(path, *args, **kwargs):
            opens.append(pathlib.Path(path).resolve())
            return DatasetProbe(path, original_dataset(path, *args, **kwargs))

        with mock.patch.object(module.ds, 'dataset', side_effect=tracked_dataset), mock.patch.object(module, 'datetime', FixedDateTime):
            result = fixture['runner'].invoke(module.main, ['--lake-root', str(lake_root), '--write'])
        assert result.exit_code == 0, result.output
        assert 'selected_candidates=2;' in result.output and 'committed_calendar_rows=4' in result.output
        for table_name in (module.CALENDAR_TABLE_NAME, module.CONTRACT_TABLE_NAME, module.DAILY_TABLE_NAME, module.MINUTE_TABLE_NAME):
            assert opens.count(silver / table_name) == 1, (name, table_name, opens)
        for path, columns, expression in reads:
            if path in [silver / table for table in (module.CALENDAR_TABLE_NAME, module.CONTRACT_TABLE_NAME, module.DAILY_TABLE_NAME, module.MINUTE_TABLE_NAME)]:
                assert expression != 'None'
            elif '.staging-' in str(path):
                assert expression != 'None' and list(columns) == module.CALENDAR_PRIMARY_KEY
            else:
                assert path.is_relative_to(silver / module.CALENDAR_TABLE_NAME)
                assert list(columns) == module.CALENDAR_PRIMARY_KEY
        io_evidence[name] = {'root_opens': 4, 'table_reads': len(reads), 'formal_leaf_reads': sum(path.is_relative_to(silver / module.CALENDAR_TABLE_NAME) and path != silver / module.CALENDAR_TABLE_NAME for path, _, _ in reads)}
        table = original_dataset(silver / module.CALENDAR_TABLE_NAME, format='parquet', partitioning=module.CALENDAR_PARTITIONING).to_table(columns=module.FUTURES_BAR_CALENDAR_SCHEMA.names)
        outputs[name] = module.arrow_to_pandas(table, module.FUTURES_BAR_CALENDAR_SCHEMA).sort_values(module.CALENDAR_PRIMARY_KEY).reset_index(drop=True)
pd.testing.assert_frame_equal(outputs['before'], outputs['after'])
assert io_evidence['before'] == io_evidence['after']
print(json.dumps({'generation_scenarios': scenarios, 'single_leaf_commit_conversion_counts': conversion_counts, 'two_partition_cli_io': io_evidence, 'formal_lake_used': False}, ensure_ascii=False, indent=2))
