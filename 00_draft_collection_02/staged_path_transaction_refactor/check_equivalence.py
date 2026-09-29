"""把相同本地输入交给抽象前快照和候选，逐文件比较提交输出。"""

import hashlib
import importlib.util
import io
import json
import pathlib
import sys
import tempfile
from contextlib import redirect_stdout
from datetime import date

import pandas as pd

DRAFT = pathlib.Path(__file__).resolve().parent
PROJECT = DRAFT.parents[1]
BASELINE = pathlib.Path(json.loads((DRAFT / 'baseline.json').read_text(encoding='utf-8'))['root'])
WORKFLOW = pathlib.Path('02_Futures_Lakehouse/a01_Futures_Market_Data')
sys.path[:0] = [str(DRAFT), str(PROJECT / '00_draft_collection_02/tests')]
from test_b01_c01_c02_daily_tail_modes import trade_calendar_df, variety_calendar_df


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def parquet_bytes(root):
    return {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in root.rglob('*.parquet')}


def varieties(*dates):
    return pd.concat([variety_calendar_df(value) for value in dates], ignore_index=True)


scenarios_checked = 0
for stem in ('b01_trade_calendar', 'b02_futures_variety_calendar'):
    old = load(BASELINE / WORKFLOW / f'{stem}.py', f'baseline_{stem}')
    new = load(DRAFT / f'{stem}.py', f'candidate_{stem}')
    if stem.startswith('b01'):
        seed = trade_calendar_df(date(2023, 12, 30), date(2024, 1, 3))
        revised = seed.iloc[[1, 3]].copy()
        revised['is_trading_day'] = False
        cases = [
            ('empty', None, seed.iloc[0:0], ()),
            ('initial_two_years', None, seed, ()),
            ('tail', seed, trade_calendar_df(date(2024, 1, 4), date(2024, 1, 5)), ()),
            ('revisions_keep_other_dates', seed, revised, ()),
        ]
        seed_args = ()
    else:
        seed = varieties(date(2023, 12, 29), date(2024, 1, 2), date(2024, 2, 1), date(2024, 3, 1))
        other_exchange = seed.copy()
        other_exchange['exchange_code'] = 'XDCE'
        other_exchange['underlying_code'] = 'A'
        seed = pd.concat([seed, other_exchange], ignore_index=True)
        revised = varieties(date(2024, 1, 2), date(2024, 2, 1))
        revised['active_contract_count'] = 3
        seed_args = (date(2023, 12, 29), date(2024, 3, 1))
        cases = [
            ('initial_empty_marker', None, seed.iloc[0:0], seed_args),
            ('initial_multiple_exchanges_and_months', None, seed, seed_args),
            ('range_replacement_keep_outer_rows', seed, revised, (date(2024, 1, 1), date(2024, 2, 29))),
            ('delete_range_keep_other_months', seed, seed.iloc[0:0], (date(2024, 1, 1), date(2024, 2, 29))),
            ('clear_table_create_marker', seed, seed.iloc[0:0], seed_args),
            ('tail_new_month', seed, varieties(date(2024, 4, 1)), (date(2024, 4, 1), date(2024, 4, 1))),
        ]
    for case_name, existing, incoming, arguments in cases:
        with tempfile.TemporaryDirectory() as directory, redirect_stdout(io.StringIO()):
            roots = [pathlib.Path(directory) / 'before', pathlib.Path(directory) / 'after']
            outputs = []
            for module, root in zip((old, new), roots):
                if existing is not None:
                    old.commit_partitions(existing.copy(), root, *seed_args)
                returned_rows = module.commit_partitions(incoming.copy(), root, *arguments)
                outputs.append((returned_rows, parquet_bytes(root / 'silver' / module.TABLE_NAME)))
            assert outputs[0] == outputs[1], (stem, case_name, outputs)
        scenarios_checked += 1
        print(f'equivalent: {stem}/{case_name}')
print(f'equivalence_ok: {scenarios_checked} scenarios; return values and all Parquet bytes identical')
