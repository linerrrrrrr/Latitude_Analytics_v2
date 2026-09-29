"""a04/b01 校验和 I/O 收缩：旧新结果、实际读表次数和下游兼容检查。"""
import ast
import contextlib
import hashlib
import importlib.util
import io
import json
import pathlib
import sys
import tempfile
import tokenize
from collections import Counter
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

import nbformat
from click.testing import CliRunner
from nbconvert.exporters import PythonExporter

ROOT = pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT = pathlib.Path(sys.argv[1])
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a04_Macro_And_Interest_Rates/b01_macro_release_calendar.ipynb')
helper = ast.parse((ROOT / '00_draft_collection_02/scripts/check_a04_b01_function_logs_20260928.py').read_text(encoding='utf8'))
definitions = [node for node in helper.body if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in ('load', 'FrozenDatetime', 'prepare', 'invoke')]
exec(compile(ast.Module(body=definitions, type_ignores=[]), '<existing isolated fixtures>', 'exec'))
before = load('macro_before_validation_io', SNAPSHOT / RELATIVE.with_suffix('.py'))
after = load('macro_after_validation_io', ROOT / RELATIVE.with_suffix('.py'))
fixtures = load('macro_validation_io_fixtures', ROOT / '00_draft_collection_02/tests/test_b04_c01_macro_release_calendar.py')
NOW = datetime(2026, 8, 17, 12, tzinfo=timezone.utc)
with contextlib.redirect_stdout(io.StringIO()):
    base_frame = after.build_expected_calendar(date(2026, 7, 17), date(2026, 8, 17), after.empty_pandas(after.MACRO_RELEASE_CALENDAR_SCHEMA), date(2026, 8, 17), NOW)
TRACE_NAMES = ('open_compatible_dataset', 'open_exact_dataset', 'validate_macro_calendar_table', 'build_expected_calendar', 'table_digest', 'changed_partition_keys', 'commit_partitions', 'pandas_to_arrow', 'arrow_to_pandas')

def measured(module, lake_root, scenario):
    materializations = []
    date_calls = Counter()
    original_open = module.open_compatible_dataset
    original_dates, original_business_dates = module.pd.date_range, module.pd.bdate_range

    class DatasetRead:
        def __init__(self, dataset, path):
            self.dataset, self.path = dataset, path

        def __getattr__(self, name):
            return getattr(self.dataset, name)

        def to_table(self, *args, **kwargs):
            materializations.append({'path': str(self.path.relative_to(lake_root)), 'filtered': kwargs.get('filter') is not None})
            return self.dataset.to_table(*args, **kwargs)

    def opening(path, label):
        dataset, current = original_open(path, label)
        return DatasetRead(dataset, path), current

    def dates(*args, **kwargs):
        date_calls['date_range'] += 1
        return original_dates(*args, **kwargs)

    def business_dates(*args, **kwargs):
        date_calls['bdate_range'] += 1
        return original_business_dates(*args, **kwargs)

    with patch.object(module, 'open_compatible_dataset', side_effect=opening), patch.object(module.pd, 'date_range', side_effect=dates), patch.object(module.pd, 'bdate_range', side_effect=business_dates):
        result, calls, files = invoke(module, lake_root, scenario)
    counts = Counter(calls)
    return result, calls, files, dict(counts), materializations, dict(date_calls)

scenarios = ['readonly_empty', 'first_write', 'unchanged', 'internal_gap', 'explicit_scope', 'legacy_metadata', 'first_backup_failure', 'leaf_install_failure', 'formal_rejection', 'rollback_failure']
results = []
with tempfile.TemporaryDirectory(prefix='a04-b01-validation-diff-') as directory:
    temporary = pathlib.Path(directory)
    for scenario in scenarios:
        runs = []
        for name, module in [('before', before), ('after', after)]:
            lake_root = temporary / scenario / name
            lake_root.mkdir(parents=True)
            prepare(lake_root, scenario)
            runs.append(measured(module, lake_root, scenario))
        old, new = runs
        assert old[0].exit_code == new[0].exit_code, (scenario, new[0].exception)
        assert type(old[0].exception) is type(new[0].exception), scenario
        assert old[2] == new[2], scenario
        owning_calls = {'build_expected_calendar', 'changed_partition_keys', 'commit_partitions'}
        assert [call for call in old[1] if call in owning_calls] == [call for call in new[1] if call in owning_calls]
        assert new[3]['validate_macro_calendar_table'] == (2 if scenario in ('legacy_metadata', 'first_backup_failure') else 1), (scenario, new[3])
        assert new[5] == {'bdate_range': 1, 'date_range': 2}
        assert sum('.staging-' in read['path'] for read in new[4]) <= 1
        assert not any(read['filtered'] for read in new[4])
        commits = [line for line in new[0].output.splitlines() if line.startswith('committed:') and 'status=completed' in line]
        assert bool(commits) == (scenario in ('first_write', 'internal_gap', 'legacy_metadata'))
        (SNAPSHOT / (scenario + '_after.log')).write_text(new[0].output, encoding='utf8')
        results.append({'scenario': scenario, 'exit_code': new[0].exit_code, 'files_identical': True, 'before_calls': old[3], 'after_calls': new[3], 'before_reads': old[4], 'after_reads': new[4], 'before_dates': old[5], 'after_dates': new[5]})

downstream = [load('interest_calendar_consumer_check', ROOT / RELATIVE.parent / 'b02_interest_rate.py'), load('macro_calendar_consumer_check', ROOT / RELATIVE.parent / 'b03_macro_release.py')]
schema = after.MACRO_RELEASE_CALENDAR_SCHEMA
description_metadata = dict(schema.metadata)
description_metadata[b'description_zh'] = '先前说明文字'.encode('utf8')
fields = list(schema)
field_metadata = dict(fields[1].metadata)
field_metadata[b'description_zh'] = '先前字段说明'.encode('utf8')
fields[1] = fields[1].with_metadata(field_metadata)
descriptive_schema = after.pa.schema(fields, metadata=description_metadata)
compatibility_checks = []

def file_hashes(root):
    return {file.relative_to(root).as_posix(): hashlib.sha256(file.read_bytes()).hexdigest() for file in root.rglob('*.parquet')}

with tempfile.TemporaryDirectory(prefix='a04-macro-calendar-compat-') as directory, contextlib.redirect_stdout(io.StringIO()):
    lake_root = pathlib.Path(directory)
    target_path = lake_root / 'silver' / after.TABLE_NAME
    fixtures.write_partitioned_calendar(after, base_frame, target_path, write_schema=descriptive_schema)
    previous_files = file_hashes(lake_root)
    outcome, calls, files, counts, reads, date_calls = measured(after, lake_root, 'unchanged')
    assert outcome.exit_code == 0, outcome.exception
    assert 'metadata_upgrade_required=false' in outcome.output
    assert previous_files == file_hashes(lake_root)
    assert 'commit_partitions' not in calls
    for module, read_name, dataset_name in zip(downstream, ('read_interest_calendar', 'read_macro_calendar'), ('interest_rate', 'macro_release'), strict=True):
        frame = getattr(module, read_name)(target_path)
        assert len(frame) == int(base_frame['dataset_name'].eq(dataset_name).sum())
        module.commit_calendar_partition(frame, lake_root, (dataset_name, 2026, 7))
    compatibility_checks.append('description_only_no_b01_rewrite_and_b02_b03_read_write_compatible')

    # 每个 fragment 都必须符合身份和当前契约版本；描述容忍不能变成任意 metadata 容忍。
    for bad_key, value in ((b'primary_key', b'wrong_key'), (b'schema_version', b'1.0.0')):
        bad_root = lake_root / bad_key.decode()
        bad_target = bad_root / 'silver' / after.TABLE_NAME
        fixtures.write_partitioned_calendar(after, base_frame, bad_target)
        fragment = next(bad_target.rglob('part-*.parquet'))
        table = after.pq.ParquetFile(fragment).read()
        metadata = dict(table.schema.metadata)
        metadata[bad_key] = value
        after.pq.write_table(table.replace_schema_metadata(metadata), fragment)
        if bad_key == b'primary_key':
            try:
                after.open_compatible_dataset(bad_target, 'mixed identity')
            except TypeError:
                pass
            else:
                raise AssertionError('wrong fragment identity accepted')
        else:
            assert after.open_compatible_dataset(bad_target, 'legacy version')[1] is False
        for module, read_name in zip(downstream, ('read_interest_calendar', 'read_macro_calendar'), strict=True):
            try:
                getattr(module, read_name)(bad_target)
            except TypeError:
                pass
            else:
                raise AssertionError('consumer accepted incompatible calendar')
        compatibility_checks.append(bad_key.decode() + '_fragment_guard')

    # 日期范围不能把旧版本迁移变成局部正式语义；非正式湖也需先完整自动迁移。
    legacy_lake = lake_root / 'schema_version'
    old_files = file_hashes(legacy_lake)
    args = ['--lake-root', str(legacy_lake), '--start-date', '2026-08-01', '--end-date', '2026-08-10', '--write']
    with patch.object(after, 'settings', SimpleNamespace(futures_lake_root=lake_root / 'not_used', futures_data_start_date=date(2026, 7, 17))), patch.object(after, 'datetime', FrozenDatetime):
        outcome = CliRunner().invoke(after.main, args)
    assert outcome.exit_code == 2
    assert old_files == file_hashes(legacy_lake)
    compatibility_checks.append('legacy_explicit_write_rejected_without_mutation')

    # 保留新生成数据的业务门禁。
    duplicate = after.pd.concat([base_frame, base_frame.iloc[:1]], ignore_index=True)
    try:
        after.validate_macro_calendar_table(after.pandas_to_arrow(duplicate, schema), 'duplicate generated rows')
    except ValueError as error:
        assert '主键不唯一' in str(error)
    else:
        raise AssertionError('duplicate generated rows accepted')
    compatibility_checks.append('new_output_primary_key_validation_retained')

    # 主键和行数相同但内容不同，正式摘要仍拒绝并恢复旧文件。
    value_lake = lake_root / 'value_readback'
    target = value_lake / 'silver' / after.TABLE_NAME
    old_frame = base_frame.drop(base_frame.index[0]).reset_index(drop=True)
    fixtures.write_partitioned_calendar(after, old_frame, target)
    prior_files = file_hashes(value_lake)
    real_open = after.open_exact_dataset

    class WrongContent:
        def __init__(self, dataset):
            self.dataset = dataset

        def to_table(self, **kwargs):
            table = self.dataset.to_table(**kwargs)
            index = table.column_names.index('quality_reason')
            values = table.column(index).to_pylist()
            values[0] = 'injected readback difference'
            return table.set_column(index, table.schema.field(index), after.pa.array(values, type=after.pa.string()))

    def reading(path, label):
        dataset = real_open(path, label)
        return WrongContent(dataset) if label == '正式宏观发布日历' else dataset

    with patch.object(after, 'open_exact_dataset', side_effect=reading):
        try:
            after.commit_partitions(base_frame, after.changed_partition_keys(base_frame, old_frame), value_lake)
        except ValueError as error:
            assert '内容与期望不一致' in str(error)
        else:
            raise AssertionError('readback content difference accepted')
    assert prior_files == file_hashes(value_lake)
    compatibility_checks.append('same_keys_and_count_different_value_rejected_and_rolled_back')

exports = {}
for name in ('b01_macro_release_calendar', 'b02_interest_rate', 'b03_macro_release'):
    relative = RELATIVE.parent / (name + '.ipynb')
    old_nb, new_nb = nbformat.read(SNAPSHOT / relative, 4), nbformat.read(ROOT / relative, 4)
    nbformat.validate(new_nb)
    assert old_nb.metadata == new_nb.metadata
    new_cells = {cell.id: cell for cell in new_nb.cells}
    removed = set(json.loads((SNAPSHOT / 'change_scope.json').read_text())['removed_cell_ids']) if name.startswith('b01_') else set()
    for old in old_nb.cells:
        if old.id not in removed:
            assert {key: value for key, value in old.items() if key != 'source'} == {key: value for key, value in new_cells[old.id].items() if key != 'source'}
    generated, _ = PythonExporter().from_notebook_node(new_nb)
    assert (ROOT / relative.with_suffix('.py')).read_bytes() == generated.encode('utf8')
    ast.parse(generated)
    if not name.startswith('b01_'):
        code = lambda nb: '\n\n'.join(cell.source for cell in nb.cells if cell.cell_type == 'code')
        functions = lambda nb: {node.name: ast.dump(node) for node in ast.parse(code(nb)).body if isinstance(node, (ast.FunctionDef, ast.ClassDef))}
        old_functions, new_functions = functions(old_nb), functions(new_nb)
        assert [name for name in old_functions if old_functions[name] != new_functions[name]] == ['dataset_has_exact_schema_metadata']
    exports[name] = hashlib.sha256(generated.encode('utf8')).hexdigest()

tree = ast.parse((ROOT / RELATIVE.with_suffix('.py')).read_text(encoding='utf8'))
commit = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'commit_partitions')
for node in ast.walk(commit):
    if isinstance(node, (ast.For, ast.While)):
        assert not any(isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute) and call.func.attr == 'to_table' for call in ast.walk(node))
rendered = json.loads((SNAPSHOT / 'flowcharts/render_results.json').read_text())
assert len(rendered) == 15 and not any(item['clipped'] for item in rendered)
report = {'comparisons': results, 'additional_checks': compatibility_checks, 'exports': exports, 'formal_lake_io': False, 'api_calls': 0, 'flowcharts': 15}
(SNAPSHOT / 'validation_io_verification.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf8')
print(json.dumps({'comparison_scenarios': len(results), 'additional_checks': compatibility_checks, 'first_write_costs': next(item for item in results if item['scenario'] == 'first_write'), 'exports': exports, 'formal_lake_io': False, 'api_calls': 0}, ensure_ascii=False, indent=2))
