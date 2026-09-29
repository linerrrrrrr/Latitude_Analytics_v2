"""a02/b03 文档与日志差分验证；仅模拟来源及临时目录，不运行正式采集。"""
import ast
import contextlib
import copy
import hashlib
import importlib.util
import io
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


class StripLogs(ast.NodeTransformer):
    def visit_Expr(self, node):
        if isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Attribute) and isinstance(node.value.func.value, ast.Name) and node.value.func.value.id == 'click' and node.value.func.attr == 'echo':
            return None
        return self.generic_visit(node)

    def visit_Assign(self, node):
        if all(isinstance(t, ast.Name) and t.id.startswith('log_') for t in node.targets):
            return None
        return self.generic_visit(node)

    def visit_Try(self, node):
        if len(node.handlers) == 1 and node.handlers[0].name == 'log_error':
            assert isinstance(node.handlers[0].body[-1], ast.Raise) and node.handlers[0].body[-1].exc is None
            assert not node.orelse and not node.finalbody
            return self.generic_visit(node).body
        return self.generic_visit(node)


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
    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(module, 'datetime', FrozenDatetime))
        stack.enter_context(mock.patch.object(module.uuid, 'uuid4', return_value=SimpleNamespace(hex='fixed_run')))
        auth = stack.enter_context(mock.patch.object(module, 'authenticate_jqdata', return_value=jqdata))
        if scenario == 'calendar_failure':
            stack.enter_context(mock.patch.object(module, 'commit_calendar_partitions', side_effect=ValueError('injected calendar failure')))
        result = CliRunner().invoke(module.main, ['--lake-root', str(lake), *(['--write'] if write else [])], standalone_mode=False)
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
        assert type(old_result.exception) is type(result.exception), scenario
        if result.exception is not None:
            assert str(old_result.exception) == str(result.exception)
            assert type(old_result.exception.__cause__) is type(result.exception.__cause__)
            assert str(old_result.exception.__cause__) == str(result.exception.__cause__)
            assert 'phase=run; status=failed;' in result.output
            assert 'phase=run; status=completed;' not in result.output and 'partition_committed:' not in result.output
        else:
            assert result.exit_code == 0, result.output
            assert result.output.count('='*88) == 3
            assert 'phase=run; status=completed;' in result.output
        if scenario == 'readonly':
            assert new_hashes == hashes(seed)
            assert 'partition_checked: function=main; phase=partition; status=completed; outcome=read_only;' in result.output
            assert 'persisted=false' in result.output and 'partition_committed:' not in result.output
        elif scenario in ('committed', 'missing_unit'):
            assert 'partition_committed: function=main; phase=commit; status=completed; persisted=true;' in result.output
            assert result.output.index('api_success:') < result.output.index('partition_committed:') < result.output.index('finished:')
        elif scenario in ('query_failure', 'normalize_failure'):
            expected_phase = 'query' if scenario == 'query_failure' else 'normalize'
            assert f'failed_phase={expected_phase};' in result.output
            failed_calendar = after.read_partition_leaf(lake/'silver'/after.CALENDAR_TABLE_NAME, after.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA, after.CALENDAR_PARTITION_COLUMNS, after.CALENDAR_PARTITIONING, ('warehouse_receipt','XDCE',2024,3), 'failure check')
            assert not bool(failed_calendar.iloc[0]['is_fetch_completed'])
            assert failed_calendar.iloc[0]['quality_status'] == 'failed'
        else:
            assert 'failed_phase=commit_calendar;' in result.output
            assert (lake/'silver'/after.TABLE_NAME).exists()
        scenarios.append({'scenario':scenario, 'same_parquet_bytes':True, 'same_exception_chain':True, 'api_calls_per_run':api_count})

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
