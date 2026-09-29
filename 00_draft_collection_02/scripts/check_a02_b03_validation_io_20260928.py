"""a02/b03 校验与 I/O 收缩差分验证；仅模拟来源及临时目录，不运行正式采集。"""
import ast
import contextlib
import copy
import hashlib
import importlib.util
import io
import itertools
import json
import pathlib
import shutil
import sys
import tempfile
import tokenize
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest import mock

import nbformat
import pandas as pd
from click.testing import CliRunner
from nbconvert.exporters import PythonExporter

ROOT = pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT = pathlib.Path(sys.argv[1])
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a02_Futures_Exchange_Reports/b03_warehouse_receipt.ipynb')
old_notebook = json.loads((SNAPSHOT/RELATIVE).read_text(encoding='utf8'))
new_notebook = json.loads((ROOT/RELATIVE).read_text(encoding='utf8'))
assert {k:v for k,v in old_notebook.items() if k != 'cells'} == {k:v for k,v in new_notebook.items() if k != 'cells'}
assert [c['id'] for c in old_notebook['cells']] == [c['id'] for c in new_notebook['cells'][:len(old_notebook['cells'])]]
changed_cells = []
for old, new in zip(old_notebook['cells'], new_notebook['cells']):
    assert {k:v for k,v in old.items() if k != 'source'} == {k:v for k,v in new.items() if k != 'source'}
    if old['cell_type'] == 'code':
        def comments(cell):
            return [t.string for t in tokenize.generate_tokens(io.StringIO(''.join(cell['source'])).readline) if t.type == tokenize.COMMENT]
        assert comments(old) == comments(new), old['id']
        if old['source'] != new['source']:
            changed_cells.append(old['id'])
assert changed_cells == ['8e8b39eb', 'a02-b03-leaf-read', 'df69af57', 'a02-b03-calendar-commit', 'f4cfe66c'], changed_cells
old_tree = ast.parse((SNAPSHOT/RELATIVE.with_suffix('.py')).read_text(encoding='utf8'))
new_tree = ast.parse((ROOT/RELATIVE.with_suffix('.py')).read_text(encoding='utf8'))
functions = lambda tree: {n.name:ast.dump(n) for n in tree.body if isinstance(n,ast.FunctionDef)}
old_functions,new_functions = functions(old_tree),functions(new_tree)
assert old_functions.keys() == new_functions.keys()
assert [n for n in old_functions if old_functions[n] != new_functions[n]] == ['validate_table_marker', 'primary_key_summary', 'read_leaf_primary_key_summary', 'commit_complete_partition', 'commit_calendar_partitions', 'main']
notebook = nbformat.read(ROOT/RELATIVE, as_version=4)
nbformat.validate(notebook)
exported, _ = PythonExporter().from_notebook_node(notebook)
assert exported.encode('utf8') == (ROOT/RELATIVE.with_suffix('.py')).read_bytes()
compile(exported, str(RELATIVE), 'exec')
assert sum('```mermaid' in ''.join(c['source']) for c in new_notebook['cells']) == 18
for i,cell in enumerate(new_notebook['cells']):
    if cell['cell_type'] == 'code':
        assert '```mermaid' in ''.join(new_notebook['cells'][i-1]['source']), cell['id']

sys.path.insert(0, str(ROOT/'00_draft_collection_02/tests'))
import test_b02_warehouse_receipt as fixtures
after = fixtures.MODULE
spec = importlib.util.spec_from_file_location('warehouse_before_docs_logs', SNAPSHOT/RELATIVE.with_suffix('.py'))
before = importlib.util.module_from_spec(spec)
spec.loader.exec_module(before)
NOW = datetime.now(timezone.utc)
DAY = date(2024, 3, 1)
scenarios = []


class FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW


def hashes(lake):
    return {p.relative_to(lake).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in lake.rglob('*.parquet')}


def invoke(module, lake, scenario, *, write=False):
    jqdata = mock.MagicMock()
    response = fixtures.valid_raw_response(DAY)
    if scenario == 'empty':
        response = pd.DataFrame()
    if scenario == 'missing_unit':
        response['unit'] = None
    if scenario == 'query_failure':
        jqdata.finance.run_query.side_effect = PermissionError('无权限')
    else:
        if scenario == 'normalize_failure':
            response['warehouse_receipt_number'] = -1.0
        jqdata.finance.run_query.return_value = response
    calls = []
    def traced(name, function):
        def call(*args, **kwargs):
            calls.append(name)
            return function(*args, **kwargs)
        return call
    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(module, 'datetime', FrozenDatetime))
        stack.enter_context(mock.patch.object(module.uuid, 'uuid4', return_value=SimpleNamespace(hex='fixed_run')))
        auth = stack.enter_context(mock.patch.object(module, 'authenticate_jqdata', return_value=jqdata))
        if scenario == 'calendar_failure':
            stack.enter_context(mock.patch.object(module, 'commit_calendar_partitions', side_effect=ValueError('injected calendar failure')))
        for name in ('validate_table_marker', 'open_planning_calendar_dataset', 'pending_report_grids',
                     'query_warehouse_grid', 'normalize_warehouse_response', 'read_partition_leaf',
                     'read_leaf_primary_key_summary', 'validate_warehouse_frame', 'validate_calendar_frame',
                     'full_fact_partition', 'commit_complete_partition', 'apply_calendar_completion',
                     'apply_calendar_failure', 'commit_calendar_partitions'):
            stack.enter_context(mock.patch.object(module, name, side_effect=traced(name, getattr(module, name))))
        result = CliRunner().invoke(module.main, ['--lake-root', str(lake), *(['--write'] if write else [])], standalone_mode=False)
    result.business_call_trace = calls
    return result, auth.call_count, jqdata.finance.run_query.call_count


with tempfile.TemporaryDirectory(prefix='a02-b03-log-check-') as directory:
    temporary = pathlib.Path(directory)
    seed = temporary/'seed'
    calendar = fixtures.calendar_frame(DAY)
    calendar['updated_at'] = NOW
    fixtures.write_exact_dataset(seed/'silver'/after.CALENDAR_TABLE_NAME, calendar, after.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA, after.CALENDAR_PARTITION_COLUMNS, after.CALENDAR_PARTITIONING)
    for scenario, write in (('readonly', False), ('committed', True), ('empty', True), ('missing_unit', True), ('query_failure', True), ('normalize_failure', True), ('calendar_failure', True)):
        pairs = []
        for module, label in ((before, 'before'), (after, 'after')):
            lake = temporary/f'{scenario}-{label}'
            shutil.copytree(seed, lake)
            result, auth_count, api_count = invoke(module, lake, scenario, write=write)
            assert auth_count == api_count == 1
            pairs.append((result, hashes(lake)))
            (SNAPSHOT/f'{scenario}_{label}.log').write_text(result.output, encoding='utf8')
        old_result, old_hashes = pairs[0]
        result, new_hashes = pairs[1]
        assert old_result.exit_code == result.exit_code
        assert old_hashes == new_hashes, scenario
        assert [n for n in old_result.business_call_trace if n != 'validate_table_marker'] == [n for n in result.business_call_trace if n != 'validate_table_marker'], scenario
        assert result.business_call_trace.count('validate_table_marker') <= old_result.business_call_trace.count('validate_table_marker')
        assert type(old_result.exception) is type(result.exception), scenario
        if result.exception is not None:
            assert str(old_result.exception) == str(result.exception)
            assert type(old_result.exception.__cause__) is type(result.exception.__cause__)
            assert str(old_result.exception.__cause__) == str(result.exception.__cause__)
            assert 'phase=run; status=failed;' in result.output
            assert 'phase=run; status=completed;' not in result.output
            assert result.output.count('partition_committed:') == 1
        else:
            assert result.exit_code == 0, result.output
            assert result.output.count('='*88) == 3
            assert 'phase=run; status=completed;' in result.output
        if scenario == 'readonly':
            assert new_hashes == hashes(seed)
            assert 'partition_checked: function=main; phase=partition; status=completed; outcome=read_only;' in result.output
            assert 'persisted=false' in result.output and 'partition_committed:' not in result.output
        elif scenario in ('committed', 'empty', 'missing_unit'):
            assert result.output.count('partition_committed:') == 2
            assert 'function=commit_complete_partition; phase=commit; status=completed;' in result.output
            assert 'function=commit_calendar_partitions; phase=calendar_state; status=completed;' in result.output
            assert 'partition_committed: function=main' not in result.output
            assert result.output.index('api_success:') < result.output.index('partition_committed:') < result.output.index('finished:')
        elif scenario in ('query_failure', 'normalize_failure'):
            expected_phase = 'query' if scenario == 'query_failure' else 'normalize'
            assert f'failed_phase={expected_phase};' in result.output
            failed_calendar = after.read_partition_leaf(lake/'silver'/after.CALENDAR_TABLE_NAME, after.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA, after.CALENDAR_PARTITION_COLUMNS, after.CALENDAR_PARTITIONING, ('warehouse_receipt','XDCE',2024,3), 'failure check')
            assert not bool(failed_calendar.iloc[0]['is_fetch_completed'])
            assert failed_calendar.iloc[0]['quality_status'] == 'failed'
            assert 'function=apply_calendar_failure; phase=generate_calendar_failure; status=completed;' in result.output
            assert 'fetch_completed=false' in result.output
            assert 'function=commit_calendar_partitions; phase=calendar_state; status=completed;' in result.output
            assert 'fetch_completed=true' not in result.output
        else:
            assert 'failed_phase=commit_calendar;' in result.output
            assert (lake/'silver'/after.TABLE_NAME).exists()
        scenarios.append({'scenario':scenario, 'same_parquet_bytes':True, 'same_exception_chain':True, 'same_business_trace_except_reduced_marker_checks':True, 'api_calls_per_run':api_count})

    lake = temporary/'committed-after'
    initial = hashes(lake)
    with mock.patch.object(after, 'authenticate_jqdata', side_effect=AssertionError('无待办不应认证')):
        result = CliRunner().invoke(after.main, ['--lake-root', str(lake), '--write'])
    assert result.exit_code == 0 and 'basis=calendar_completion_snapshot' in result.output
    assert 'outcome=up_to_date' in result.output and 'partition_start:' not in result.output
    assert initial == hashes(lake)
    scenarios.append({'scenario':'up_to_date', 'no_authentication_or_write':True})


    with mock.patch.object(after, 'open_planning_calendar_dataset', side_effect=AssertionError('参数错误不应读湖')):
        result = CliRunner().invoke(after.main, ['--start-date', '2024-03-01'])
    assert result.exit_code == 2
    assert 'failed_phase=arguments;' in result.output and 'phase=run; status=completed;' not in result.output
    scenarios.append({'scenario':'invalid_dates', 'no_lake_io':True})



    for rejected_label in ('正式仓单事实', '正式报告日历'):
        lake = temporary/('reject-fact' if rejected_label == '正式仓单事实' else 'reject-calendar')
        shutil.copytree(seed, lake)
        original = after.read_leaf_primary_key_summary
        def read(*args, **kwargs):
            value = original(*args, **kwargs)
            if args[-1] == rejected_label:
                raise ValueError('injected formal rejection')
            return value
        with mock.patch.object(after, 'read_leaf_primary_key_summary', side_effect=read):
            result, auth_count, api_count = invoke(after, lake, 'committed', write=True)
        (SNAPSHOT/(lake.name+'.log')).write_text(result.output, encoding='utf8')
        assert result.exit_code != 0 and isinstance(result.exception, RuntimeError)
        assert isinstance(result.exception.__cause__, ValueError)
        assert result.output.count('partition_committed:') == (0 if rejected_label == '正式仓单事实' else 1)
        assert 'phase=calendar_state; status=completed;' not in result.output
        assert 'partition_timing:' not in result.output and 'phase=run; status=completed;' not in result.output
        assert hashes(lake/'silver'/after.CALENDAR_TABLE_NAME) == hashes(seed/'silver'/after.CALENDAR_TABLE_NAME)
        if rejected_label == '正式报告日历':
            assert hashes(lake/'silver'/after.TABLE_NAME) == hashes(temporary/'committed-after'/'silver'/after.TABLE_NAME)
        else:
            assert not (lake/'silver'/after.TABLE_NAME).exists()
        assert list((lake/'silver').glob('.*.failed-*'))
        assert not list((lake/'silver').glob('.*.staging-*'))
        assert not list((lake/'silver').glob('.*.backup-*'))
        scenarios.append({'scenario':lake.name, 'prior_commits_preserved':True, 'calendar_uncompleted':True, 'no_false_success':True})

hashes_before = json.loads((SNAPSHOT/'hashes.json').read_text(encoding='utf8'))
changed = [p for p,d in hashes_before.items() if hashlib.sha256((ROOT/p).read_bytes()).hexdigest() != d]
assert set(changed) == {RELATIVE.as_posix(), RELATIVE.with_suffix('.py').as_posix()}, changed
report = {
    'scenarios':scenarios, 'scenario_count':len(scenarios), 'changed_old_code_cells':changed_cells,
    'unchanged_functions':len(old_functions)-6, 'original_comments_ids_metadata_outputs_execution_counts_preserved':True,
    'default_export_exact':True, 'diagrams':18, 'changed_production_and_specification_files':changed,
    'shared_module_and_contracts_unchanged':True, 'real_api_calls':0, 'formal_lake_writes':0,
    'assumptions':'单个写入者、同一文件系统；模拟 API 的非空、空响应、缺失单位、查询和转换失败；在临时湖中注入移动与验收失败，不假设正式文件损坏，不验证进程被杀或并发写入。性能参数与计时边界保留，未测量真实 API 批次耗时。',
}
(SNAPSHOT/'final_verification.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf8')
print(json.dumps(report, ensure_ascii=False, indent=2))
