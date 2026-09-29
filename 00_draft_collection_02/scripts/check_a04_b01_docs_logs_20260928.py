"""核对 a04/b01 呈现与日志改动；不访问正式湖或网络。"""
import ast
import contextlib
import copy
import hashlib
import importlib.util
import io
import json
import pathlib
import sys
import tempfile
import tokenize
from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

import nbformat
from click.testing import CliRunner
from nbconvert.exporters import PythonExporter

ROOT = pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT = pathlib.Path(sys.argv[1])
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a04_Macro_And_Interest_Rates/b01_macro_release_calendar.ipynb')
before = nbformat.read(SNAPSHOT / RELATIVE, 4)
after = nbformat.read(ROOT / RELATIVE, 4)
nbformat.validate(after)

def code(notebook):
    return '\n\n'.join(cell.source for cell in notebook.cells if cell.cell_type == 'code')

def comments(source):
    return [token.string for token in tokenize.generate_tokens(io.StringIO(source).readline) if token.type == tokenize.COMMENT]

class WithoutLogs(ast.NodeTransformer):
    def visit_Import(self, node):
        return None if [item.name for item in node.names] == ['time'] else node

    def visit_Assign(self, node):
        if all(isinstance(target, ast.Name) and target.id.startswith('log_') for target in node.targets):
            return None
        return self.generic_visit(node)

    def visit_Expr(self, node):
        if isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Attribute):
            function = node.value.func
            if isinstance(function.value, ast.Name) and function.value.id == 'click' and function.attr == 'echo':
                return None
        return self.generic_visit(node)

    def visit_Try(self, node):
        # 仅展开本轮 main 增加的日志 try；保留原旧策略识别与提交恢复。
        is_log_wrapper = len(node.handlers) == 1 and node.handlers[0].name == 'error' and bool(node.finalbody)
        node = self.generic_visit(node)
        return node.body if is_log_wrapper else node

    def visit_If(self, node):
        node = self.generic_visit(node)
        # 原只读分支和新只读分支都仅含日志。
        return node if node.body else None

before_source, after_source = code(before), code(after)
assert comments(before_source) == comments(after_source), '原代码注释发生改变'
assert ast.dump(WithoutLogs().visit(ast.parse(before_source))) == ast.dump(WithoutLogs().visit(ast.parse(after_source))), '剔除日志后业务 AST 不一致'
assert before.metadata == after.metadata
after_cells = {cell.id: cell for cell in after.cells}
for old in before.cells:
    assert {key: value for key, value in old.items() if key != 'source'} == {key: value for key, value in after_cells[old.id].items() if key != 'source'}
for index, cell in enumerate(after.cells):
    if cell.cell_type == 'code':
        assert index and '```mermaid' in after.cells[index - 1].source
assert after_cells['4a5b544e'].source == next(cell.source for cell in before.cells if cell.id == '4a5b544e')

generated, _ = PythonExporter().from_notebook_node(after)
script_path = (ROOT / RELATIVE).with_suffix('.py')
assert script_path.read_bytes() == generated.encode('utf8')
assert b'\r\n' not in script_path.read_bytes()
ast.parse(generated)
render_results = json.loads((SNAPSHOT / 'flowcharts/render_results.json').read_text(encoding='utf8'))
assert len(render_results) == 17 and not any(item['clipped'] for item in render_results)

spec = importlib.util.spec_from_file_location('a04_b01_docs_logs_check', script_path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
runner = CliRunner()
with tempfile.TemporaryDirectory(prefix='a04-b01-log-check-') as directory:
    lake_root = pathlib.Path(directory)
    settings = SimpleNamespace(futures_lake_root=lake_root, futures_data_start_date=date(2026, 9, 1))
    with patch.object(module, 'settings', settings):
        readonly = runner.invoke(module.main, [])
        assert readonly.exit_code == 0, readonly.exception
        assert 'dry_run:' in readonly.output and 'persisted=true' not in readonly.output
        assert readonly.output.splitlines()[0] == '=' * 80
        assert readonly.output.splitlines()[-1] == '=' * 80
        assert 'phase=run; status=completed' in readonly.output
        assert not list(lake_root.iterdir())
        injected = OSError('controlled commit exception')
        with patch.object(module, 'commit_partitions', side_effect=injected):
            failed = runner.invoke(module.main, ['--write'])
        assert failed.exception is injected
        assert 'phase=commit; status=failed; error=OSError' in failed.output
        assert 'committed:' not in failed.output and 'phase=run; status=completed' not in failed.output
        assert failed.output.splitlines()[-1] == '=' * 80
        with patch.object(module, 'open_compatible_dataset', side_effect=AssertionError('must not read')):
            invalid = runner.invoke(module.main, ['--start-date', '2026-09-01'])
        assert invalid.exit_code == 2
        assert 'phase=parameters; status=failed; error=UsageError' in invalid.output
        assert not list(lake_root.iterdir())

before_functions = {node.name: ast.dump(node) for node in ast.parse(before_source).body if isinstance(node, ast.FunctionDef)}
after_functions = {node.name: ast.dump(node) for node in ast.parse(after_source).body if isinstance(node, ast.FunctionDef)}
unchanged_functions = [name for name in before_functions if before_functions[name] == after_functions[name]]
assert set(before_functions) - set(unchanged_functions) == {'main'}
report = {
    'business_ast_unchanged_after_removing_logs': True,
    'unchanged_non_main_functions': unchanged_functions,
    'original_comments_and_cell_state_preserved': True,
    'entry_unchanged': True,
    'flowcharts_rendered': len(render_results),
    'clipped_labels': 0,
    'export_exact_and_syntax_valid': True,
    'log_checks': ['readonly', 'commit_exception_propagates', 'parameter_gate'],
    'formal_lake_io': False,
    'api_calls': False,
    'sha256': hashlib.sha256(script_path.read_bytes()).hexdigest(),
}
(SNAPSHOT / 'docs_logs_verification.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf8')
print(json.dumps(report, ensure_ascii=False, indent=2))
