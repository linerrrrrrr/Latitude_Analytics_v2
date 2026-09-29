"""a02/b03 函数日志差分验证；仅模拟来源及临时目录，不运行正式采集。"""
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


logging_names = set()

class ReplaceReturnName(ast.NodeTransformer):
    def __init__(self, name, value): self.name, self.value = name, value
    def visit_Name(self, node):
        return copy.deepcopy(self.value) if node.id == self.name and isinstance(node.ctx, ast.Load) else node


class BusinessTree(ast.NodeTransformer):
    def visit_Expr(self, node):
        if isinstance(node.value,ast.Call) and ast.unparse(node.value.func)=='click.echo': return None
        return self.generic_visit(node)
    def visit_Assign(self, node):
        if all(isinstance(t,ast.Name) and (t.id.startswith('log_') or t.id in logging_names) for t in node.targets): return None
        return self.generic_visit(node)
    def visit_AugAssign(self, node):
        if isinstance(node.target,ast.Name) and node.target.id.startswith('log_'): return None
        return self.generic_visit(node)
    def visit_If(self, node):
        if any(isinstance(n,ast.Name) and n.id=='log_last_progress_at' for n in ast.walk(node.test)): return None
        return self.generic_visit(node)
    def visit_For(self, node):
        if all(n.id.startswith('log_') for n in ast.walk(node.target) if isinstance(n,ast.Name)): return None
        return self.generic_visit(node)
    def visit_Try(self, node):
        if len(node.handlers)==1 and node.handlers[0].name=='log_error':
            assert isinstance(node.handlers[0].body[-1],ast.Raise) and node.handlers[0].body[-1].exc is None
            assert not node.orelse and not node.finalbody
            return self.generic_visit(node).body
        return self.generic_visit(node)
    def generic_visit(self,node):
        node=super().generic_visit(node)
        for field,value in ast.iter_fields(node):
            if not isinstance(value,list) or not value or not all(isinstance(x,ast.stmt) for x in value): continue
            result=[]
            for stmt in value:
                if isinstance(stmt,ast.Return):
                    while result and isinstance(result[-1],ast.Assign) and len(result[-1].targets)==1 and isinstance(result[-1].targets[0],ast.Name):
                        previous=result[-1];name=previous.targets[0].id
                        if not any(isinstance(n,ast.Name) and n.id==name for n in ast.walk(stmt)):break
                        result.pop();stmt=ReplaceReturnName(name,previous.value).visit(stmt)
                result.append(stmt)
            setattr(node,field,result)
        return node


StripLogs = BusinessTree


def code(notebook):
    return '\n\n'.join(''.join(c['source']) for c in notebook['cells'] if c['cell_type'] == 'code')


old_source, new_source = code(old_notebook), code(new_notebook)
assert ast.dump(StripLogs().visit(ast.parse(old_source))) == ast.dump(StripLogs().visit(ast.parse(new_source))), '业务 AST 改变'
comments = lambda source: [t.string for t in tokenize.generate_tokens(io.StringIO(source).readline) if t.type == tokenize.COMMENT]
assert comments(old_source) == comments(new_source)
old_cells = {c['id']:c for c in old_notebook['cells']}
new_cells = {c['id']:c for c in new_notebook['cells']}
assert [c['id'] for c in new_notebook['cells'] if c['id'] in old_cells] == list(old_cells)
for cell_id, old in old_cells.items():
    assert {k:v for k,v in old.items() if k != 'source'} == {k:v for k,v in new_cells[cell_id].items() if k != 'source'}, cell_id
assert {k:v for k,v in old_notebook.items() if k != 'cells'} == {k:v for k,v in new_notebook.items() if k != 'cells'}
assert old_cells['d4e12ca7'] == new_cells['d4e12ca7']
flow_count = sum('```mermaid' in ''.join(c['source']) for c in new_notebook['cells'])
assert flow_count == 17
for index, cell in enumerate(new_notebook['cells']):
    if cell['cell_type'] == 'code':
        assert '```mermaid' in ''.join(new_notebook['cells'][index-1]['source']), cell['id']
notebook = nbformat.read(ROOT/RELATIVE, as_version=4)
nbformat.validate(notebook)
exported, _ = PythonExporter().from_notebook_node(notebook)
assert exported.encode('utf8') == (ROOT/RELATIVE.with_suffix('.py')).read_bytes()
compile(exported, str(RELATIVE), 'exec')

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
    for scenario, write in (('readonly', False), ('committed', True), ('missing_unit', True), ('query_failure', True), ('normalize_failure', True), ('calendar_failure', True)):
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
        assert old_result.business_call_trace == result.business_call_trace, scenario
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
        elif scenario in ('committed', 'missing_unit'):
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
        scenarios.append({'scenario':scenario, 'same_parquet_bytes':True, 'same_exception_chain':True, 'same_business_call_trace':True, 'api_calls_per_run':api_count})

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

for step, should_pass in ((0.1, True), (10.0, False)):
    result, fact_commit, calendar_commit = fixtures.invoke_mocked_performance_batch(51, step)
    expected_count = 51 if should_pass else 50
    assert fact_commit.call_count == calendar_commit.call_count == expected_count
    if should_pass:
        assert result.exit_code == 0 and 'performance_gate_passed: samples=50' in result.output
        assert 'phase=run; status=completed;' in result.output
    else:
        assert result.exit_code == 1 and 'performance_gate_failed: samples=50' in result.output
        assert 'failed_phase=performance_gate;' in result.output
        assert 'partition_start: 51/51' not in result.output and 'finished:' not in result.output
    (SNAPSHOT/f'performance_{should_pass}.log').write_text(result.output, encoding='utf8')
    scenarios.append({'scenario':'performance_pass' if should_pass else 'performance_stop', 'committed_partitions':expected_count})

# 进度检查沿原有循环推进；这里只放大模拟载荷，不请求供应商。
response = pd.concat([fixtures.valid_raw_response(DAY)]*2001, ignore_index=True)
response['warehouse_name'] = [f'进度测试仓库-{index}' for index in range(2001)]
grid = {'exchange_code':'XDCE', 'underlying_code':'EB', 'trading_date':DAY}
with contextlib.redirect_stdout(io.StringIO()):
    expected = before.normalize_warehouse_response(response, grid, NOW)
with contextlib.redirect_stdout(io.StringIO()) as output, mock.patch.object(after.time, 'perf_counter', side_effect=itertools.count(0, 3)):
    actual = after.normalize_warehouse_response(response, grid, NOW)
pd.testing.assert_frame_equal(actual, expected)
assert 'visited_rows=1000/2001' in output.getvalue() and 'visited_rows=2000/2001' in output.getvalue()
assert 'function=normalize_warehouse_response; phase=generate_fact; status=completed;' in output.getvalue()
(SNAPSHOT/'row_progress.log').write_text(output.getvalue(), encoding='utf8')
scenarios.append({'scenario':'row_progress', 'synthetic_rows':2001, 'same_dataframe':True})

with tempfile.TemporaryDirectory(prefix='a02-b03-file-progress-') as directory:
    table_path = pathlib.Path(directory)/after.TABLE_NAME
    schema = after.FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA
    columns, partitioning = after.PARTITION_COLUMNS, after.FACT_PARTITIONING
    key = ('XDCE','EB',2024,3)
    leaf = table_path/'exchange_code=XDCE/underlying_code=EB/year=2024/month=3'
    fixtures.write_exact_dataset(table_path, fixtures.warehouse_frame(DAY), schema, columns, partitioning)
    for index in (1, 2):
        frame = fixtures.warehouse_frame(DAY, warehouse_name=f'其他测试仓库-{index}')
        table = after.pandas_to_arrow(frame, schema).select([name for name in schema.names if name not in columns])
        after.pq.write_table(table, leaf/f'part-{index}.parquet')
    for function_name in ('read_partition_leaf', 'read_leaf_primary_key_summary'):
        arguments = (table_path, schema, *([after.PRIMARY_KEY] if function_name.endswith('summary') else []), columns, partitioning, key, 'progress check')
        with contextlib.redirect_stdout(io.StringIO()):
            expected = getattr(before, function_name)(*arguments)
        original_read_schema = after.pq.read_schema
        with contextlib.redirect_stdout(io.StringIO()) as output, mock.patch.object(after.time, 'perf_counter', side_effect=itertools.count(0, 3)), mock.patch.object(after.pq, 'read_schema', wraps=original_read_schema) as reads:
            actual = getattr(after, function_name)(*arguments)
        assert reads.call_count == 3
        assert 'checked_files=3/3' in output.getvalue()
        if function_name.endswith('summary'):
            assert actual == expected
        else:
            pd.testing.assert_frame_equal(actual, expected)
        (SNAPSHOT/(function_name+'_progress.log')).write_text(output.getvalue(), encoding='utf8')
        scenarios.append({'scenario':function_name+'_progress', 'files':3, 'physical_schema_reads':3, 'same_result':True})

    # 正式验收失败只记录恢复结果，不误报事实叶提交成功。
    lake = pathlib.Path(directory)/'recovery'
    frame = fixtures.warehouse_frame(DAY)
    with contextlib.redirect_stdout(io.StringIO()):
        after.commit_complete_partition(frame, lake, key)
    old_hashes = hashes(lake)
    original_summary = after.read_leaf_primary_key_summary
    def reject_formal(*args, **kwargs):
        summary = original_summary(*args, **kwargs)
        if args[-1] == '正式仓单事实':
            raise ValueError('injected formal rejection')
        return summary
    with contextlib.redirect_stdout(io.StringIO()) as output, mock.patch.object(after, 'read_leaf_primary_key_summary', side_effect=reject_formal):
        try:
            after.commit_complete_partition(frame, lake, key)
        except ValueError as error:
            assert str(error) == 'injected formal rejection'
        else:
            raise AssertionError('Expected formal rejection')
    assert hashes(lake) == old_hashes
    assert 'phase=rollback; status=completed;' in output.getvalue()
    assert 'partition_committed:' not in output.getvalue()
    assert 'failed_phase=formal_verify' in output.getvalue()
    (SNAPSHOT/'formal_rejection.log').write_text(output.getvalue(), encoding='utf8')
    scenarios.append({'scenario':'formal_rejection', 'old_leaf_bytes_restored':True, 'no_false_commit_success':True})

hashes_before = json.loads((SNAPSHOT/'hashes.json').read_text(encoding='utf8'))
changed = [p for p,digest in hashes_before.items() if hashlib.sha256((ROOT/p).read_bytes()).hexdigest() != digest]
assert set(changed) == {RELATIVE.as_posix(), RELATIVE.with_suffix('.py').as_posix()}, changed
report = {
    'business_ast_unchanged_after_removing_logs':True,
    'original_comments_ids_metadata_outputs_execution_counts_preserved':True,
    'execution_entry_unchanged':True,
    'default_export_byte_exact':True,
    'flowchart_count':flow_count,
    'log_scenarios':scenarios, 'log_scenario_count':len(scenarios),
    'changed_production_files':changed,
    'real_api_calls':0, 'formal_lake_writes':0,
    'assumptions':'明确构造的来源响应、临时湖与固定业务时间/批次；查询和日历提交失败为受控注入。性能测试使用模拟计时验证窗口和停止位置，不代表真实来源速度。',
}
(SNAPSHOT/'verification.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf8')
print(json.dumps(report, ensure_ascii=False, indent=2))
