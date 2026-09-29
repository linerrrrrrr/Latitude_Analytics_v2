"""对照本轮快照检查 b03 校验次数、当前叶 I/O 与正式结果等价性。"""

import contextlib
import hashlib
import importlib.util
import io
import json
import pathlib
import sys
import tempfile
from datetime import date, datetime
from unittest.mock import patch

import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT = pathlib.Path(sys.argv[1])
OUT = SNAPSHOT / 'validation_io_checks'
OUT.mkdir(parents=True, exist_ok=True)


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


tests = load('b03_fixture', ROOT / '00_draft_collection_02/tests/test_futures_contract_calendar_builder.py')
new = tests.contract_calendar
old = load('b03_before_validation_io', SNAPSHOT / 'b03_futures_contract_calendar.py')
contracts = sys.modules['config.data_contracts']
fixture = tests.ContractCalendarBuilderTest()
fixture.setUp()
summary = {}
logs = io.StringIO()


class FixedDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return fixture.updated_at.astimezone(tz) if tz is not None else fixture.updated_at.replace(tzinfo=None)


def files(lake_root, module):
    table_root = lake_root / 'silver' / module.TABLE_NAME
    return {str(p.relative_to(table_root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in table_root.rglob('*.parquet')}


with contextlib.redirect_stdout(logs):
    built, _ = fixture.build_frame_with_securities([date(2024, 1, 2), date(2024, 1, 3)], fixture.valid_securities_df)

validator_counts = []
validated_frames = []
for module in (old, new):
    with patch.object(contracts, 'validate_arrow_table', wraps=contracts.validate_arrow_table) as cast:
        validated_frames.append(module.validate_contract_calendar_frame(built, 'count'))
        validator_counts.append(cast.call_count)
assert validator_counts == [2, 1], validator_counts
pd.testing.assert_frame_equal(*validated_frames)
summary['arrow_checks_per_business_validator_before_after'] = validator_counts

mapping_cases = []
for nullable in (False, True):
    frame = built.copy()
    if nullable:
        frame['tick_size'] = None
        frame['contract_multiplier'] = pd.NA
    frame = new.validate_contract_calendar_frame(frame, 'mapping')
    expected = old.business_rows_by_key(frame)
    with patch.object(new, 'pandas_to_arrow', side_effect=AssertionError('comparison must not convert to Arrow')):
        actual = new.business_rows_by_key(frame)
    assert actual == expected
    changed = frame.copy()
    changed['tick_size'] = 2.0
    changed = new.validate_contract_calendar_frame(changed, 'changed mapping')
    assert old.business_rows_by_key(changed) == new.business_rows_by_key(changed)
    assert actual != new.business_rows_by_key(changed)
    mapping_cases.append('nullable' if nullable else 'non_null')
empty = new.empty_pandas(new.FUTURES_CONTRACT_CALENDAR_SCHEMA)
assert old.business_rows_by_key(empty) == new.business_rows_by_key(empty) == {}
summary['mapping_equivalence'] = [*mapping_cases, 'empty']

# 不增加生产日期范围；两日固定夹具用于三种局部替换和清空路径的前后对照。
with tempfile.TemporaryDirectory(prefix='b03-equivalence-') as temporary:
    base = pathlib.Path(temporary)
    old_root, new_root = base / 'before', base / 'after'
    scenarios = []
    validator_call_counts = []
    initial = built.copy()
    replacement = built.loc[built['trading_date'].eq(date(2024, 1, 2))].copy()
    replacement['tick_size'] = 2.0
    exact_replacement = replacement.copy()
    exact_replacement['tick_size'] = 3.0
    steps = [
        ('initial', initial, {}),
        ('explicit_range', replacement, {'replace_start_date':date(2024, 1, 2), 'replace_end_date':date(2024, 1, 2)}),
        ('exact_variety_date', exact_replacement, {'replacement_variety_date_keys':{('XSGE', 'RB', date(2024, 1, 2))}}),
        ('full_leaf', replacement, {}),
        ('empty_leaf', empty, {}),
        ('empty_again_with_marker', empty, {}),
    ]
    for name, frame, kwargs in steps:
        counts, returned = [], []
        for module, lake_root in ((old, old_root), (new, new_root)):
            with contextlib.redirect_stdout(logs), patch.object(module, 'validate_contract_calendar_frame', wraps=module.validate_contract_calendar_frame) as validate:
                returned.append(module.commit_partition(frame, lake_root, ('XSGE', 2024, 1), **kwargs))
            counts.append([call.args[1] for call in validate.call_args_list])
        assert returned[0] == returned[1]
        assert files(old_root, old) == files(new_root, new), name
        assert counts == [['待提交分区', '合并后完整分区'], ['合并后完整分区']], (name, counts)
        scenarios.append(name)
        validator_call_counts.append([len(c) for c in counts])
    for module, lake_root in ((old, old_root), (new, new_root)):
        with contextlib.redirect_stdout(logs):
            module.write_automatic_tail_processed_through(lake_root, date(2024, 1, 3))
    assert files(old_root, old) == files(new_root, new)
    scenarios.append('watermark')
    summary['byte_identical_commit_scenarios'] = scenarios
    summary['business_validators_per_commit_before_after'] = validator_call_counts

# 两个正常 Hive 叶的显式 full 修订：观测主流程与提交时实际打开的路径。
io_results = []
with tempfile.TemporaryDirectory(prefix='b03-root-scan-count-') as temporary:
    base = pathlib.Path(temporary)
    for module, label in ((old, 'before'), (new, 'after')):
        lake_root = base / label
        variety = fixture.variety_frame([date(2024, 1, 2)])
        second_variety = variety.copy()
        second_variety['underlying_code'] = 'M'
        second_variety['exchange_code'] = 'XDCE'
        variety = pd.concat([variety, second_variety], ignore_index=True)
        module.ds.write_dataset(
            module.pandas_to_arrow(variety, module.FUTURES_VARIETY_CALENDAR_SCHEMA),
            lake_root / 'silver' / module.UPSTREAM_TABLE_NAME,
            format='parquet', partitioning=module.UPSTREAM_PARTITIONING,
        )
        catalogue = fixture.valid_securities_df.copy()
        second_contract = catalogue.copy()
        second_contract['contract_code'] = 'M2405.XDCE'
        second_contract['underlying_code'] = 'M'
        second_contract['exchange_code'] = 'XDCE'
        catalogue = pd.concat([catalogue, second_contract], ignore_index=True)
        with patch.object(tests, 'contract_calendar', module), patch.object(module, 'datetime', FixedDatetime):
            first, _ = fixture.invoke_with_jqdata(lake_root, ['--write'], tests.FakeJQData(catalogue))
        assert first.exit_code == 0, first.output
        catalogue['tick_size'] = 2.0
        formal_root = (lake_root / 'silver' / module.TABLE_NAME).resolve()
        opened, fragment_checks = [], []
        original_dataset = module.ds.dataset
        original_fragments = module.validate_dataset_fragment_schemas

        def open_dataset(source, *args, **kwargs):
            if isinstance(source, (str, pathlib.Path)):
                opened.append(pathlib.Path(source).resolve())
            return original_dataset(source, *args, **kwargs)

        def inspect_fragments(dataset, schema, partition_columns, context):
            fragment_checks.append((context, tuple(pathlib.Path(f.path).resolve() for f in dataset.get_fragments())))
            return original_fragments(dataset, schema, partition_columns, context)

        with patch.object(tests, 'contract_calendar', module), patch.object(module, 'datetime', FixedDatetime), patch.object(module.ds, 'dataset', side_effect=open_dataset), patch.object(module, 'validate_dataset_fragment_schemas', side_effect=inspect_fragments):
            result, _ = fixture.invoke_with_jqdata(lake_root, ['--full', '--write'], tests.FakeJQData(catalogue))
        assert result.exit_code == 0, result.output
        root_open_count = sum(p == formal_root for p in opened)
        root_fragment_passes = [c for c, fs in fragment_checks if c == '现有合约日历 ' or c == '现有正式数据集 ']
        leaf_fragment_passes = [(c, fs) for c, fs in fragment_checks if c == '现有正式分区 ']
        for _, paths in leaf_fragment_passes:
            assert len({p.parent for p in paths}) == 1
        io_results.append({'version': label, 'formal_root_opens':root_open_count, 'whole_table_fragment_checks':len(root_fragment_passes), 'exact_leaf_checks':len(leaf_fragment_passes)})
        (OUT / f'{label}_two_leaf_full.log').write_text(result.output, encoding='utf-8')
    assert files(base / 'before', old) == files(base / 'after', new)
assert io_results == [
    {'version':'before', 'formal_root_opens':3, 'whole_table_fragment_checks':3, 'exact_leaf_checks':0},
    {'version':'after', 'formal_root_opens':1, 'whole_table_fragment_checks':1, 'exact_leaf_checks':2},
], io_results
summary['two_leaf_io_counts'] = io_results

# 直接提交仍由完整合并叶拒绝无效业务值；当前请求范围之外的行不被覆盖。
with tempfile.TemporaryDirectory(prefix='b03-dirty-validation-') as temporary:
    lake_root = pathlib.Path(temporary)
    with contextlib.redirect_stdout(logs):
        new.commit_partition(built, lake_root, ('XSGE', 2024, 1))
    original_files = files(lake_root, new)
    invalid = built.copy()
    invalid['minute_count'] = 1
    with contextlib.redirect_stdout(logs):
        try:
            new.commit_partition(invalid, lake_root, ('XSGE', 2024, 1))
        except ValueError as error:
            assert '合并后完整分区' in str(error)
        else:
            raise AssertionError('dirty leaf business validation was lost')
    assert files(lake_root, new) == original_files
summary['invalid_dirty_leaf_rejected_before_install'] = True

(OUT / 'results.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
(OUT / 'direct_calls.log').write_text(logs.getvalue(), encoding='utf-8')
print(json.dumps(summary, ensure_ascii=False))
