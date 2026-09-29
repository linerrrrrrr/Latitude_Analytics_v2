"""以前后同一两个月场景记录真实调用次数及主循环输入范围。"""
import contextlib
import importlib.util
import json
import pathlib
import sys
import tempfile
from collections import Counter
from datetime import date
from unittest.mock import patch

import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT = pathlib.Path(sys.argv[1])
REL = pathlib.Path('02_Futures_Lakehouse/a04_Macro_And_Interest_Rates/b02_interest_rate.py')


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fixtures = load('shibor_cost_fixtures', ROOT / '00_draft_collection_02/tests/test_b04_c02_interest_rate.py')
before = load('shibor_cost_before', SNAPSHOT / REL)
after = fixtures.C02
report = {}
first, second = date(2026, 7, 31), date(2026, 8, 3)


class Client:
    def shibor(self, **kwargs):
        return pd.concat([fixtures.shibor_response(value) for value in (first, second)
                          if kwargs['start_date'] <= value.strftime('%Y%m%d') <= kwargs['end_date']], ignore_index=True)


for label, module in (('before', before), ('after', after)):
    with tempfile.TemporaryDirectory(prefix='shibor-cost-') as directory:
        lake = pathlib.Path(directory)
        fixtures.build_calendar(lake, first, second)
        calls = Counter()
        leaf_inputs = {'full_fact_partition': [], 'apply_calendar_completion': []}
        commit_root_opens = []
        def recording(name, real):
            def invoke(*args, **kwargs):
                calls[name] += 1
                if name in leaf_inputs:
                    leaf_inputs[name].append(len(args[0]))
                if name == 'open_exact_dataset' and args[4] in ('正式 SHIBOR 事实', '回写后的正式宏观发布日历', '待回写的正式宏观发布日历'):
                    if args[0] in (lake / 'silver' / module.TABLE_NAME, lake / 'silver' / module.CALENDAR_TABLE_NAME):
                        commit_root_opens.append(str(args[0]))
                return real(*args, **kwargs)
            return invoke
        with contextlib.ExitStack() as stack:
            for name in ('read_interest_calendar', 'read_optional_fact', 'plan_interest_rate_grids',
                         'validate_interest_calendar_table', 'full_fact_partition', 'apply_calendar_completion',
                         'open_exact_dataset', 'validate_interest_rate_frame' if label == 'before' else 'validate_interest_rate_table'):
                stack.enter_context(patch.object(module, name, side_effect=recording(name, getattr(module, name))))
            stack.enter_context(patch.object(module, 'create_tushare_client', return_value=Client()))
            result = fixtures.CliRunner().invoke(module.main, ['--lake-root', str(lake), '--write'])
        assert result.exit_code == 0, result.output
        report[label] = {'calls': dict(calls), 'leaf_input_rows': leaf_inputs, 'commit_table_root_opens': len(commit_root_opens)}
        (SNAPSHOT / f'costs_{label}.log').write_text(result.output, encoding='utf8')
assert report['after']['commit_table_root_opens'] == 0
for name in ('read_interest_calendar', 'read_optional_fact', 'plan_interest_rate_grids'):
    assert report['after']['calls'][name] == 1
assert report['after']['calls']['validate_interest_calendar_table'] == 2
assert report['after']['calls']['validate_interest_rate_table'] == 4
(SNAPSHOT / 'validation_costs.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf8')
print(json.dumps(report, ensure_ascii=False, indent=2))
