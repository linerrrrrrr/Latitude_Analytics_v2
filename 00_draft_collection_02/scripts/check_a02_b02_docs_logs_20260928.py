"""文档/日志改动的业务 AST、Notebook 结构和临时湖日志分支核对。"""
import ast
import copy
import hashlib
import io
import json
import pathlib
import sys
import tempfile
import tokenize
from unittest import mock

import nbformat
import pandas as pd
from click.testing import CliRunner
from nbconvert.exporters import PythonExporter

ROOT = pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT = pathlib.Path(sys.argv[1])
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a02_Futures_Exchange_Reports/b02_futures_holding_reports.ipynb')
before = json.loads((SNAPSHOT / RELATIVE).read_text(encoding='utf8'))
after = json.loads((ROOT / RELATIVE).read_text(encoding='utf8'))


def code(notebook):
    return '\n\n'.join(''.join(c['source']) for c in notebook['cells'] if c['cell_type'] == 'code')


class StripLogs(ast.NodeTransformer):
    def visit_Expr(self, node):
        if isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Attribute):
            if isinstance(node.value.func.value, ast.Name) and node.value.func.value.id == 'click' and node.value.func.attr == 'echo':
                return None
        return self.generic_visit(node)

    def visit_Assign(self, node):
        if all(isinstance(t, ast.Name) and t.id.startswith('log_') for t in node.targets):
            return None
        return self.generic_visit(node)

    def visit_Try(self, node):
        if len(node.handlers) == 1 and node.handlers[0].name == 'log_error':
            assert isinstance(node.handlers[0].body[-1], ast.Raise)
            assert node.handlers[0].body[-1].exc is None and not node.orelse and not node.finalbody
            visited = self.generic_visit(node)
            return visited.body
        return self.generic_visit(node)


old_source, new_source = code(before), code(after)
assert ast.dump(StripLogs().visit(ast.parse(old_source))) == ast.dump(StripLogs().visit(ast.parse(new_source))), '除日志之外的业务 AST 改变'
comments = lambda source: [t.string for t in tokenize.generate_tokens(io.StringIO(source).readline) if t.type == tokenize.COMMENT]
assert comments(old_source) == comments(new_source)
before_cells = {c['id']: c for c in before['cells']}
after_cells = {c['id']: c for c in after['cells']}
assert [c['id'] for c in after['cells'] if c['id'] in before_cells] == list(before_cells)
for cell_id, old in before_cells.items():
    assert {k: v for k, v in old.items() if k != 'source'} == {k: v for k, v in after_cells[cell_id].items() if k != 'source'}, cell_id
assert {k: v for k, v in before.items() if k != 'cells'} == {k: v for k, v in after.items() if k != 'cells'}
assert before_cells['abaf342b'] == after_cells['abaf342b'], '第 11 项入口必须尚未改动'
flow_count = sum('```mermaid' in ''.join(c['source']) for c in after['cells'])
assert flow_count == 17
for index, cell in enumerate(after['cells']):
    if cell['cell_type'] == 'code':
        assert '```mermaid' in ''.join(after['cells'][index - 1]['source']), cell['id']
notebook = nbformat.read(ROOT / RELATIVE, as_version=4)
nbformat.validate(notebook)
exported, _ = PythonExporter().from_notebook_node(notebook)
assert exported.encode('utf8') == (ROOT / RELATIVE.with_suffix('.py')).read_bytes()
compile(exported, str(RELATIVE), 'exec')

sys.path.insert(0, str(ROOT / '00_draft_collection_02/tests'))
import test_b02_futures_holding_reports as fixtures
module = fixtures.holding_reports
fixture = fixtures.LeafIsolationTests()
scenarios = []
with tempfile.TemporaryDirectory(prefix='a02-b02-logs-') as temp_dir:
    lake_root = pathlib.Path(temp_dir)
    for dataset in module.DATASET_NAMES:
        fixture.commit_calendar_leaf(lake_root, dataset, [fixtures.calendar_row(dataset, 'BB', fixtures.GRID['trading_date'], completed=False)], 2014, 4)
    baseline_hashes = fixtures.parquet_hashes(lake_root)
    response = pd.DataFrame([fixtures.source_row(), fixtures.source_row(member_name='期货公司', rank=1)])
    runner = CliRunner()

    with mock.patch.object(module, 'authenticate_jqdata', return_value=fixtures.query_jqdata(response)) as auth:
        result = runner.invoke(module.main, ['--lake-root', str(lake_root)])
    assert result.exit_code == 0, result.output
    assert auth.call_count == 1
    assert result.output.count('=' * 88) == 3
    assert 'partition_checked: function=main; phase=partition; status=completed; outcome=read_only; persisted=false;' in result.output
    assert 'partition_committed:' not in result.output
    assert 'phase=run; status=completed; outcome=read_only;' in result.output
    assert fixtures.parquet_hashes(lake_root) == baseline_hashes
    (SNAPSHOT / 'log_readonly.txt').write_text(result.output, encoding='utf8')
    scenarios.append('只读待办：模拟请求一次，验收日志明确 persisted=false，文件逐字节未改')

    with mock.patch.object(module, 'authenticate_jqdata', return_value=fixtures.query_jqdata(PermissionError('无权限'))):
        result = runner.invoke(module.main, ['--lake-root', str(lake_root), '--write'])
    assert result.exit_code != 0 and isinstance(result.exception, RuntimeError)
    assert isinstance(result.exception.__cause__, PermissionError)
    assert 'phase=run; status=failed; failed_phase=query;' in result.output
    assert 'phase=run; status=completed;' not in result.output
    assert 'partition_committed:' not in result.output
    assert fixtures.parquet_hashes(lake_root) == baseline_hashes
    (SNAPSHOT / 'log_query_failure.txt').write_text(result.output, encoding='utf8')
    scenarios.append('模拟权限失败：失败阶段为 query，原异常链保留，无成功收尾或写入')

    with mock.patch.object(module, 'authenticate_jqdata', return_value=fixtures.query_jqdata(response)):
        result = runner.invoke(module.main, ['--lake-root', str(lake_root), '--write'])
    assert result.exit_code == 0, result.output
    assert 'partition_committed: function=main; phase=commit; status=completed; persisted=true;' in result.output
    assert result.output.index('api_success:') < result.output.index('partition_committed:') < result.output.index('finished:')
    assert 'phase=run; status=completed; outcome=committed;' in result.output
    (SNAPSHOT / 'log_committed.txt').write_text(result.output, encoding='utf8')
    scenarios.append('临时湖提交：正式复读后才报告 persisted=true，随后才输出运行完成')
    committed_hashes = fixtures.parquet_hashes(lake_root)

    with mock.patch.object(module, 'authenticate_jqdata', side_effect=AssertionError('无待办不能认证')):
        result = runner.invoke(module.main, ['--lake-root', str(lake_root)])
    assert result.exit_code == 0, result.output
    assert 'up_to_date:' in result.output and 'phase=run; status=completed; outcome=up_to_date;' in result.output
    assert 'partition_start:' not in result.output
    assert fixtures.parquet_hashes(lake_root) == committed_hashes
    scenarios.append('无待办：不认证或请求，正常完整收尾，无文件变化')

    result = runner.invoke(module.main, ['--lake-root', str(lake_root), '--start-date', '2014-04-28'])
    assert result.exit_code == 2
    assert '必须同时提供' in result.output
    assert 'phase=run; status=failed; failed_phase=arguments;' in result.output
    assert 'phase=run; status=completed;' not in result.output
    scenarios.append('非法成对日期：保持 Click 参数错误，并报告 arguments 阶段失败')

hashes = json.loads((SNAPSHOT / 'hashes.json').read_text(encoding='utf8'))
changed = [p for p, digest in hashes.items() if hashlib.sha256((ROOT / p).read_bytes()).hexdigest() != digest]
assert set(changed) == {RELATIVE.as_posix(), RELATIVE.with_suffix('.py').as_posix()}, changed
report = {'business_ast_unchanged_after_removing_logging': True,
          'original_comments_ids_metadata_outputs_execution_counts_preserved': True,
          'entry_unchanged': True, 'default_export_byte_exact': True,
          'flowcharts': flow_count, 'log_scenarios': scenarios,
          'changed_production_files': changed,
          'real_api_calls': 0, 'formal_lake_writes': 0}
(SNAPSHOT / 'verification.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf8')
print(json.dumps(report, ensure_ascii=False, indent=2))
